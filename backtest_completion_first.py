#!/usr/bin/env python3
"""
Completion-First Strategy Backtester
===================================
Proxy backtest for the Rust `completion_first` strategy using the existing
`btc5m.db` replay dataset.

Important limitation:
  The live strategy uses recent trade alternation and OFI. The current replay DB
  does not contain venue trade ticks / OFI snapshots, so this script uses
  L1-book-change proxies instead. It is suitable for offline screening and
  parameter ranking, not for claiming live-equivalent edge.
"""

import argparse
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

PAIR_ARB_NET_EPS = 0.001


@dataclass
class Config:
    pair_target: float = 0.98
    bid_size: float = 2.0
    max_net_diff: float = 5.0
    tick_size: float = 0.01
    score_threshold: float = 0.58
    completion_ttl_secs: float = 30.0
    reentry_cooldown_secs: float = 10.0
    trade_recency_secs: float = 8.0
    seed_size_mult: float = 0.50
    repair_size_mult: float = 1.00
    score_pair_band_bonus: float = 0.03
    timeout_pair_band_bonus: float = 0.02
    max_seed_spread_ticks: float = 4.0
    hedge_debounce_secs: float = 0.10
    requote_interval_sec: float = 2.0
    fill_model: str = "conservative"


@dataclass
class Inventory:
    yes_qty: float = 0.0
    yes_avg_cost: float = 0.0
    no_qty: float = 0.0
    no_avg_cost: float = 0.0
    net_diff: float = 0.0

    def update_after_fill(self, side: str, qty: float, price: float):
        if side == "YES":
            new_total = self.yes_qty + qty
            if new_total > 0:
                self.yes_avg_cost = (
                    self.yes_qty * self.yes_avg_cost + qty * price
                ) / new_total
            self.yes_qty = new_total
        else:
            new_total = self.no_qty + qty
            if new_total > 0:
                self.no_avg_cost = (
                    self.no_qty * self.no_avg_cost + qty * price
                ) / new_total
            self.no_qty = new_total
        self.net_diff = self.yes_qty - self.no_qty

    def paired_qty(self) -> float:
        return min(self.yes_qty, self.no_qty)

    def residual_qty(self) -> float:
        return abs(self.net_diff)

    def dominant_side(self) -> Optional[str]:
        if self.net_diff > PAIR_ARB_NET_EPS:
            return "YES"
        if self.net_diff < -PAIR_ARB_NET_EPS:
            return "NO"
        return None


@dataclass
class ActiveOrders:
    yes_bid: float = 0.0
    yes_size: float = 0.0
    no_bid: float = 0.0
    no_size: float = 0.0

    def clear(self):
        self.yes_bid = 0.0
        self.yes_size = 0.0
        self.no_bid = 0.0
        self.no_size = 0.0


@dataclass
class StrategyState:
    active_since: Optional[float] = None
    last_flattened_at: Optional[float] = None
    last_seed_at: Optional[float] = None
    last_taker_repair_at: Optional[float] = None
    first_leg_ts: Optional[float] = None
    pair_completed_ts: Optional[float] = None


@dataclass
class ActivityState:
    last_yes_activity_ts: Optional[float] = None
    last_no_activity_ts: Optional[float] = None
    last_activity_side: Optional[str] = None
    last_activity_ts: Optional[float] = None
    alternation_streak: int = 0


@dataclass
class ScoreBreakdown:
    score: float
    both_live: bool
    two_sided_recent: bool
    alternating_recent: bool
    spread_ok: bool
    book_centered: bool
    healthy_flow: bool
    balanced_heat: bool
    early_or_tail_window: bool
    toxic_penalty: bool
    any_side_stale: bool


def safe_price(price: float, tick: float) -> float:
    p = round(price / tick) * tick
    if p < tick:
        return 0.0
    return min(p, 0.99)


def check_fill(our_bid: float, market_ask: float, model: str = "conservative") -> bool:
    if our_bid <= 0.0 or market_ask is None or market_ask <= 0.0:
        return False
    if model == "conservative":
        return market_ask <= our_bid
    return our_bid >= market_ask


def dynamic_pair_target(cfg: Config, score: float, allow_score_bonus: bool, timed_out: bool) -> float:
    target = max(cfg.pair_target, 0.0)
    if allow_score_bonus and score > cfg.score_threshold:
        unlocked = (score - cfg.score_threshold) / max(1.0 - cfg.score_threshold, 1e-9)
        target += max(0.0, min(1.0, unlocked)) * cfg.score_pair_band_bonus
    if timed_out:
        target += cfg.timeout_pair_band_bonus
    return max(0.05, min(0.999, target))


def update_activity(prev_tick, tick, state: ActivityState, cfg: Config):
    if prev_tick is None:
        return

    yes_move = abs((tick["ask_up"] or 0.0) - (prev_tick["ask_up"] or 0.0)) + abs(
        (tick["bid_up"] or 0.0) - (prev_tick["bid_up"] or 0.0)
    )
    no_move = abs((tick["ask_down"] or 0.0) - (prev_tick["ask_down"] or 0.0)) + abs(
        (tick["bid_down"] or 0.0) - (prev_tick["bid_down"] or 0.0)
    )
    ts = float(tick["ts"])

    active_sides = []
    if yes_move >= cfg.tick_size - 1e-9:
        state.last_yes_activity_ts = ts
        active_sides.append(("YES", yes_move))
    if no_move >= cfg.tick_size - 1e-9:
        state.last_no_activity_ts = ts
        active_sides.append(("NO", no_move))
    if not active_sides:
        return

    dominant_side = sorted(active_sides, key=lambda item: item[1], reverse=True)[0][0]
    if state.last_activity_side is not None:
        if state.last_activity_side != dominant_side:
            state.alternation_streak = min(state.alternation_streak + 1, 8)
        else:
            state.alternation_streak = 0
    state.last_activity_side = dominant_side
    state.last_activity_ts = ts


def completion_score_breakdown(cfg: Config, tick, state: ActivityState, total_window_sec: float) -> ScoreBreakdown:
    ts = float(tick["ts"])
    trade_window = cfg.trade_recency_secs

    yes_recent = (
        state.last_yes_activity_ts is not None and ts - state.last_yes_activity_ts <= trade_window
    )
    no_recent = (
        state.last_no_activity_ts is not None and ts - state.last_no_activity_ts <= trade_window
    )
    two_sided_recent = yes_recent and no_recent
    alternating_recent = (
        state.last_activity_ts is not None
        and ts - state.last_activity_ts <= trade_window
        and state.alternation_streak >= 1
    )

    yes_bid = tick["bid_up"] or 0.0
    yes_ask = tick["ask_up"] or 0.0
    no_bid = tick["bid_down"] or 0.0
    no_ask = tick["ask_down"] or 0.0
    both_live = yes_bid > 0 and yes_ask > 0 and no_bid > 0 and no_ask > 0

    yes_spread_ticks = ((yes_ask - yes_bid) / cfg.tick_size) if both_live else math.inf
    no_spread_ticks = ((no_ask - no_bid) / cfg.tick_size) if both_live else math.inf
    spread_ok = (
        yes_spread_ticks <= cfg.max_seed_spread_ticks + 1e-9
        and no_spread_ticks <= cfg.max_seed_spread_ticks + 1e-9
    )

    mid_yes = (yes_bid + yes_ask) / 2.0 if both_live else 0.0
    mid_no = (no_bid + no_ask) / 2.0 if both_live else 0.0
    book_centered = both_live and abs((mid_yes + mid_no) - 1.0) <= 0.08

    quote_age_up = float(tick["quote_age_up"] or 0.0)
    quote_age_down = float(tick["quote_age_down"] or 0.0)
    ws_fresh = int(tick["ws_fresh"] or 0) == 1
    is_stale = int(tick["is_stale"] or 0) == 1
    state_conf = float(tick["state_confidence"] or 0.0)
    any_side_stale = is_stale or quote_age_up > trade_window or quote_age_down > trade_window
    healthy_flow = ws_fresh and not is_stale and state_conf >= 0.7 and quote_age_up <= 2.0 and quote_age_down <= 2.0
    toxic_penalty = is_stale or not ws_fresh or state_conf < 0.5

    ask_depth_up = float(tick["ask_depth_L1_up"] or 0.0)
    ask_depth_down = float(tick["ask_depth_L1_down"] or 0.0)
    min_depth = min(ask_depth_up, ask_depth_down)
    max_depth = max(ask_depth_up, ask_depth_down, 1e-9)
    balanced_heat = min_depth > 0.0 and (max_depth / min_depth) <= 4.0

    remaining_sec = float(tick["remaining_sec"] or 0.0)
    elapsed = max(0.0, total_window_sec - remaining_sec)
    early_window = 5.0 <= elapsed <= 75.0
    tail_window = remaining_sec <= 45.0

    score = 0.0
    if two_sided_recent:
        score += 0.30
    if alternating_recent:
        score += 0.20
    if healthy_flow:
        score += 0.18
    if spread_ok:
        score += 0.14
    if book_centered:
        score += 0.10
    if balanced_heat:
        score += 0.08
    if early_window or tail_window:
        score += 0.08
    if not both_live:
        score -= 0.15
    if toxic_penalty:
        score -= 0.18
    if any_side_stale:
        score -= 0.20

    return ScoreBreakdown(
        score=max(0.0, min(1.0, score)),
        both_live=both_live,
        two_sided_recent=two_sided_recent,
        alternating_recent=alternating_recent,
        spread_ok=spread_ok,
        book_centered=book_centered,
        healthy_flow=healthy_flow,
        balanced_heat=balanced_heat,
        early_or_tail_window=(early_window or tail_window),
        toxic_penalty=toxic_penalty,
        any_side_stale=any_side_stale,
    )


def compute_quotes(
    cfg: Config,
    inv: Inventory,
    state: StrategyState,
    tick,
    score: ScoreBreakdown,
):
    orders = ActiveOrders()
    yes_bid = tick["bid_up"] or 0.0
    yes_ask = tick["ask_up"] or 0.0
    no_bid = tick["bid_down"] or 0.0
    no_ask = tick["ask_down"] or 0.0
    ts = float(tick["ts"])

    if inv.residual_qty() <= PAIR_ARB_NET_EPS:
        if (
            state.last_flattened_at is not None
            and ts - state.last_flattened_at < cfg.reentry_cooldown_secs
        ):
            return orders, "cooldown"
        if score.score + 1e-9 < cfg.score_threshold:
            return orders, "score_gate"

        seed_size = max(cfg.bid_size * cfg.seed_size_mult, 0.25)
        dynamic_target = dynamic_pair_target(cfg, score.score, False, False)
        mid_yes = (yes_bid + yes_ask) / 2.0
        mid_no = (no_bid + no_ask) / 2.0
        raw_yes = mid_yes
        raw_no = mid_no
        if raw_yes + raw_no > dynamic_target:
            overflow = raw_yes + raw_no - dynamic_target
            raw_yes -= overflow / 2.0
            raw_no -= overflow / 2.0
        orders.yes_bid = safe_price(min(raw_yes, yes_ask - cfg.tick_size), cfg.tick_size)
        orders.no_bid = safe_price(min(raw_no, no_ask - cfg.tick_size), cfg.tick_size)
        orders.yes_size = seed_size if orders.yes_bid > 0.0 else 0.0
        orders.no_size = seed_size if orders.no_bid > 0.0 else 0.0
        return orders, "seed"

    dominant = inv.dominant_side()
    if dominant is None:
        return orders, "flat"

    repair_side = "NO" if dominant == "YES" else "YES"
    completion_timed_out = (
        state.active_since is not None and ts - state.active_since >= cfg.completion_ttl_secs
    )
    held_avg_cost = inv.yes_avg_cost if dominant == "YES" else inv.no_avg_cost
    ceiling = dynamic_pair_target(cfg, score.score, True, completion_timed_out) - max(held_avg_cost, 0.0)
    ceiling = safe_price(ceiling, cfg.tick_size)
    if ceiling <= cfg.tick_size + 1e-9:
        return orders, "ceiling_blocked"

    if completion_timed_out:
        return orders, "taker_repair"

    repair_size = min(inv.residual_qty(), max(cfg.bid_size * cfg.repair_size_mult, 0.25))
    repair_bid = no_bid if repair_side == "NO" else yes_bid
    target_price = min(repair_bid + cfg.tick_size, ceiling) if repair_bid > 0.0 else ceiling
    bid = safe_price(target_price, cfg.tick_size)
    if repair_side == "YES":
        orders.yes_bid = bid
        orders.yes_size = repair_size if bid > 0.0 else 0.0
    else:
        orders.no_bid = bid
        orders.no_size = repair_size if bid > 0.0 else 0.0
    return orders, "maker_repair"


def compute_settlement(inv: Inventory, outcome: str) -> dict:
    paired = inv.paired_qty()
    res_yes = inv.yes_qty - paired
    res_no = inv.no_qty - paired
    pair_cost = inv.yes_avg_cost + inv.no_avg_cost if paired > 0 else 0.0
    paired_pnl = paired * max(0.0, 1.0 - pair_cost) if paired > 0 else 0.0

    if outcome == "UP":
        res_pnl = res_yes * (1.0 - inv.yes_avg_cost) - res_no * inv.no_avg_cost
    elif outcome == "DOWN":
        res_pnl = res_no * (1.0 - inv.no_avg_cost) - res_yes * inv.yes_avg_cost
    else:
        res_pnl = 0.0

    return {
        "paired_qty": paired,
        "pair_cost": pair_cost,
        "paired_pnl": paired_pnl,
        "residual_yes": res_yes,
        "residual_no": res_no,
        "residual_pnl": res_pnl,
        "total_pnl": paired_pnl + res_pnl,
    }


def run_backtest(db_path: str, cfg: Config, limit: int = 0, verbose: bool = False):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = """
        SELECT sr.condition_id, sr.outcome
        FROM settlement_records sr
        WHERE sr.outcome IN ('UP', 'DOWN')
        ORDER BY sr.ts_end
    """
    if limit > 0:
        query += f" LIMIT {limit}"
    cur.execute(query)
    settlements = cur.fetchall()

    print(
        f"📊 Backtesting completion_first on {len(settlements)} windows "
        f"(pair_target={cfg.pair_target:.3f}, bid={cfg.bid_size:.2f}, "
        f"score_threshold={cfg.score_threshold:.2f}, ttl={cfg.completion_ttl_secs:.0f}s)"
    )
    print("=" * 100)

    total_pnl = 0.0
    total_paired_pnl = 0.0
    total_residual_pnl = 0.0
    total_fills = 0
    total_pairs = 0.0
    windows = 0
    perfect_pairs = 0
    residual_windows = 0
    pair_within_30s = 0
    seeded_windows = 0
    taker_repairs = 0
    score_gate_skips = 0
    cooldown_skips = 0
    seed_scores = []
    pnls = []

    for sett in settlements:
        cid = sett["condition_id"]
        cur.execute(
            """
            SELECT ts, remaining_sec, ask_up, bid_up, ask_down, bid_down,
                   ask_depth_L1_up, ask_depth_L1_down, ws_fresh, quote_age_up,
                   quote_age_down, state_confidence, is_stale
            FROM market_ticks
            WHERE condition_id = ?
            ORDER BY ts ASC
            """,
            (cid,),
        )
        ticks = cur.fetchall()
        if len(ticks) < 10:
            continue

        inv = Inventory()
        orders = ActiveOrders()
        strat = StrategyState()
        activity = ActivityState()
        last_quote_ts = 0.0
        prev_tick = None
        total_window_sec = float(ticks[0]["remaining_sec"] or 300.0)
        filled_sides = []
        window_paired_within_30s = False

        for tick in ticks:
            ts = float(tick["ts"])
            yes_ask = tick["ask_up"] or 0.0
            no_ask = tick["ask_down"] or 0.0
            if yes_ask <= 0.0 or no_ask <= 0.0 or (tick["bid_up"] or 0.0) <= 0.0 or (tick["bid_down"] or 0.0) <= 0.0:
                prev_tick = tick
                continue

            update_activity(prev_tick, tick, activity, cfg)
            breakdown = completion_score_breakdown(cfg, tick, activity, total_window_sec)

            filled_this_tick = False
            prev_abs_net = abs(inv.net_diff)
            prev_paired = inv.paired_qty()

            if orders.yes_bid > 0.0 and check_fill(orders.yes_bid, yes_ask, cfg.fill_model):
                inv.update_after_fill("YES", orders.yes_size, orders.yes_bid)
                total_fills += 1
                filled_sides.append(("YES", orders.yes_bid, orders.yes_size, tick["remaining_sec"]))
                orders.yes_bid = 0.0
                orders.yes_size = 0.0
                filled_this_tick = True

            if orders.no_bid > 0.0 and check_fill(orders.no_bid, no_ask, cfg.fill_model):
                inv.update_after_fill("NO", orders.no_size, orders.no_bid)
                total_fills += 1
                filled_sides.append(("NO", orders.no_bid, orders.no_size, tick["remaining_sec"]))
                orders.no_bid = 0.0
                orders.no_size = 0.0
                filled_this_tick = True

            curr_abs_net = abs(inv.net_diff)
            curr_paired = inv.paired_qty()
            if prev_abs_net <= PAIR_ARB_NET_EPS and curr_abs_net > PAIR_ARB_NET_EPS:
                strat.active_since = ts
                strat.last_seed_at = ts
                strat.first_leg_ts = ts
                strat.pair_completed_ts = None
            elif prev_abs_net > PAIR_ARB_NET_EPS and curr_abs_net <= PAIR_ARB_NET_EPS:
                strat.active_since = None
                strat.last_flattened_at = ts
                if (
                    strat.first_leg_ts is not None
                    and not window_paired_within_30s
                    and ts - strat.first_leg_ts <= 30.0
                ):
                    window_paired_within_30s = True
                    strat.pair_completed_ts = ts
            elif prev_paired <= PAIR_ARB_NET_EPS and curr_paired > PAIR_ARB_NET_EPS:
                if (
                    strat.first_leg_ts is not None
                    and not window_paired_within_30s
                    and ts - strat.first_leg_ts <= 30.0
                ):
                    window_paired_within_30s = True
                strat.pair_completed_ts = ts

            reason = None
            if filled_this_tick or ts - last_quote_ts >= cfg.requote_interval_sec:
                orders, reason = compute_quotes(cfg, inv, strat, tick, breakdown)
                last_quote_ts = ts
                if reason == "seed":
                    seeded_windows += 1
                    seed_scores.append(breakdown.score)
                elif reason == "score_gate":
                    score_gate_skips += 1
                elif reason == "cooldown":
                    cooldown_skips += 1
                elif reason == "taker_repair":
                    repair_side = "NO" if inv.dominant_side() == "YES" else "YES"
                    repair_ask = no_ask if repair_side == "NO" else yes_ask
                    held_avg = inv.yes_avg_cost if inv.dominant_side() == "YES" else inv.no_avg_cost
                    limit_price = dynamic_pair_target(cfg, breakdown.score, True, True) - max(held_avg, 0.0)
                    if (
                        strat.last_taker_repair_at is None
                        or ts - strat.last_taker_repair_at >= cfg.hedge_debounce_secs
                    ):
                        strat.last_taker_repair_at = ts
                        if repair_ask > 0.0 and repair_ask <= limit_price + 1e-9:
                            qty = inv.residual_qty()
                            if repair_side == "YES":
                                inv.update_after_fill("YES", qty, repair_ask)
                            else:
                                inv.update_after_fill("NO", qty, repair_ask)
                            total_fills += 1
                            taker_repairs += 1
                            filled_sides.append((repair_side, repair_ask, qty, tick["remaining_sec"]))
                            if (
                                strat.first_leg_ts is not None
                                and not window_paired_within_30s
                                and ts - strat.first_leg_ts <= 30.0
                            ):
                                window_paired_within_30s = True
                            strat.active_since = None
                            strat.last_flattened_at = ts
                            strat.pair_completed_ts = ts
                            orders.clear()

            prev_tick = tick

        result = compute_settlement(inv, sett["outcome"])
        windows += 1
        if window_paired_within_30s:
            pair_within_30s += 1
        total_pnl += result["total_pnl"]
        total_paired_pnl += result["paired_pnl"]
        total_residual_pnl += result["residual_pnl"]
        total_pairs += result["paired_qty"]
        pnls.append(result["total_pnl"])

        residual = result["residual_yes"] > 0.01 or result["residual_no"] > 0.01
        if residual:
            residual_windows += 1
        else:
            perfect_pairs += 1

        if verbose and (filled_sides or residual):
            dt = datetime.fromtimestamp(float(ticks[0]["ts"]), tz=timezone.utc).strftime("%m-%d %H:%M")
            fill_str = " ".join([f"{side[0]}@{price:.2f}" for side, price, _, _ in filled_sides[:6]])
            print(
                f"  [{dt}] fills={len(filled_sides):2d} paired={result['paired_qty']:5.1f} "
                f"pair_cost={result['pair_cost']:.3f} res_Y={result['residual_yes']:.1f} "
                f"res_N={result['residual_no']:.1f} total={result['total_pnl']:+.3f} | {fill_str}"
            )

    print("\n" + "=" * 100)
    print("📈 COMPLETION_FIRST BACKTEST RESULTS")
    print("=" * 100)
    print(f"  Windows tested:        {windows:,}")
    print(f"  Total fills:           {total_fills:,}")
    print(f"  Total paired qty:      {total_pairs:,.1f}")
    print(f"  Perfect pair windows:  {perfect_pairs} ({100 * perfect_pairs / max(windows, 1):.1f}%)")
    print(f"  Windows with residual: {residual_windows} ({100 * residual_windows / max(windows, 1):.1f}%)")
    print(f"  Pair within 30s:       {pair_within_30s} ({100 * pair_within_30s / max(windows, 1):.1f}%)")
    print(f"  Taker repairs hit:     {taker_repairs}")
    print(f"  Seed emissions:        {seeded_windows}")
    print(f"  Skip score gate:       {score_gate_skips}")
    print(f"  Skip cooldown:         {cooldown_skips}")
    if seed_scores:
        avg_seed_score = sum(seed_scores) / len(seed_scores)
        print(f"  Avg seed score:        {avg_seed_score:.3f}")
    print()
    print(f"  💰 Paired P&L:         {total_paired_pnl:+,.2f} USDC")
    print(f"  💀 Residual P&L:       {total_residual_pnl:+,.2f} USDC")
    print(f"  📊 Total P&L:          {total_pnl:+,.2f} USDC")
    print(f"  📊 Avg P&L/window:     {total_pnl / max(windows, 1):+.4f} USDC")
    if total_pairs > 0:
        print(f"  📊 Avg profit/pair:    {total_paired_pnl / total_pairs:.4f}")

    if pnls:
        sorted_pnls = sorted(pnls)
        n = len(sorted_pnls)
        print("\n  P&L Distribution:")
        print(f"    Min:     {sorted_pnls[0]:+.3f}")
        print(f"    p5:      {sorted_pnls[int(n * 0.05)]:+.3f}")
        print(f"    p25:     {sorted_pnls[int(n * 0.25)]:+.3f}")
        print(f"    Median:  {sorted_pnls[int(n * 0.50)]:+.3f}")
        print(f"    p75:     {sorted_pnls[int(n * 0.75)]:+.3f}")
        print(f"    p95:     {sorted_pnls[int(n * 0.95)]:+.3f}")
        print(f"    Max:     {sorted_pnls[-1]:+.3f}")

    conn.close()
    return total_pnl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Completion-First Strategy Backtester")
    parser.add_argument("--db", default="/Users/hot/web3Scientist/poly_trans_research/btc5m.db")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--pair-target", type=float, default=0.98)
    parser.add_argument("--bid-size", type=float, default=2.0)
    parser.add_argument("--max-net-diff", type=float, default=5.0)
    parser.add_argument("--score-threshold", type=float, default=0.58)
    parser.add_argument("--ttl", type=float, default=30.0, help="completion_ttl_secs")
    parser.add_argument("--cooldown", type=float, default=10.0, help="reentry_cooldown_secs")
    parser.add_argument("--trade-recency", type=float, default=8.0)
    parser.add_argument("--seed-mult", type=float, default=0.50)
    parser.add_argument("--repair-mult", type=float, default=1.00)
    parser.add_argument("--score-band-bonus", type=float, default=0.03)
    parser.add_argument("--timeout-band-bonus", type=float, default=0.02)
    parser.add_argument("--max-seed-spread-ticks", type=float, default=4.0)
    parser.add_argument("--fill-model", choices=["conservative", "aggressive"], default="conservative")
    args = parser.parse_args()

    cfg = Config(
        pair_target=args.pair_target,
        bid_size=args.bid_size,
        max_net_diff=args.max_net_diff,
        score_threshold=args.score_threshold,
        completion_ttl_secs=args.ttl,
        reentry_cooldown_secs=args.cooldown,
        trade_recency_secs=args.trade_recency,
        seed_size_mult=args.seed_mult,
        repair_size_mult=args.repair_mult,
        score_pair_band_bonus=args.score_band_bonus,
        timeout_pair_band_bonus=args.timeout_band_bonus,
        max_seed_spread_ticks=args.max_seed_spread_ticks,
        fill_model=args.fill_model,
    )

    run_backtest(args.db, cfg, limit=args.limit, verbose=args.verbose)
