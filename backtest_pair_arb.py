#!/usr/bin/env python3
"""
Pair-Arb Strategy Backtester
============================
Faithful reimplementation of the production pair_arb.rs logic in Python,
driven by tick-level replay from btc5m.db.

Core parameters from .env (v1.0.0-stable):
  pair_target  = 0.97
  bid_size     = 5.0
  max_net_diff = 5.0
  tier_1_mult  = 0.50
  tier_2_mult  = 0.15
  tick_size    = 0.01
  risk_open_cutoff_secs = 240
  pair_cost_safety_margin = 0.02

Fill model (Conservative):
  Our YES bid filled when market ask_up <= our_bid (someone sells into us)
  Our NO  bid filled when market ask_down <= our_bid
"""

import sqlite3
import argparse
import math
import sys
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

# ============================================================
# Constants (matching pair_arb.rs exactly)
# ============================================================
PAIR_ARB_NET_EPS = 0.001
TIER_1_NET_DIFF = 5.0
TIER_2_NET_DIFF = 10.0
RISK_INCR_TIER_1_NET_DIFF = 3.5
RISK_INCR_TIER_2_NET_DIFF = 8.0
EARLY_SKEW_MULT = 0.35

# ============================================================
# Config
# ============================================================
@dataclass
class Config:
    pair_target: float = 0.97
    bid_size: float = 5.0
    max_net_diff: float = 5.0
    tick_size: float = 0.01
    tier_1_mult: float = 0.50
    tier_2_mult: float = 0.15
    risk_open_cutoff_secs: float = 240.0
    pair_cost_safety_margin: float = 0.02
    as_skew_factor: float = 0.06
    initial_balance: float = 50.0  # generous for backtesting
    fill_model: str = "conservative"  # "conservative" or "aggressive"

# ============================================================
# Inventory State
# ============================================================
@dataclass
class Inventory:
    yes_qty: float = 0.0
    yes_avg_cost: float = 0.0
    no_qty: float = 0.0
    no_avg_cost: float = 0.0
    net_diff: float = 0.0  # yes_qty - no_qty

    def update_after_fill(self, side: str, qty: float, price: float):
        if side == "YES":
            new_total = self.yes_qty + qty
            if new_total > 0:
                self.yes_avg_cost = (self.yes_qty * self.yes_avg_cost + qty * price) / new_total
            self.yes_qty = new_total
            self.net_diff = self.yes_qty - self.no_qty
        else:
            new_total = self.no_qty + qty
            if new_total > 0:
                self.no_avg_cost = (self.no_qty * self.no_avg_cost + qty * price) / new_total
            self.no_qty = new_total
            self.net_diff = self.yes_qty - self.no_qty

# ============================================================
# Core Strategy Logic (matching pair_arb.rs)
# ============================================================

def risk_effect(inv: Inventory, side: str, size: float) -> str:
    """PairArbRiskEffect: RiskIncreasing or PairingOrReducing"""
    if side == "YES":
        projected = abs(inv.net_diff + size)
    else:
        projected = abs(inv.net_diff - size)
    if projected <= abs(inv.net_diff) + PAIR_ARB_NET_EPS:
        return "pairing"
    return "risk_increasing"


def candidate_size(inv: Inventory, side: str, bid_size: float) -> float:
    """candidate_size_for_side"""
    if bid_size <= PAIR_ARB_NET_EPS:
        return max(bid_size, 0.0)
    dominant = None
    if inv.net_diff > PAIR_ARB_NET_EPS:
        dominant = "YES"
    elif inv.net_diff < -PAIR_ARB_NET_EPS:
        dominant = "NO"
    pairing_side = None
    if dominant == "YES":
        pairing_side = "NO"
    elif dominant == "NO":
        pairing_side = "YES"
    if pairing_side == side:
        d = max(abs(inv.net_diff), 0.0)
        sized = math.floor(d * 100.0) / 100.0
        if sized >= 0.01:
            return sized
    return bid_size


def tier_cap_price(inv: Inventory, side: str, re: str,
                   tier_1_mult: float, tier_2_mult: float) -> Optional[float]:
    """tier_cap_price_for_candidate"""
    if re != "risk_increasing":
        return None
    current_abs = abs(inv.net_diff)
    if current_abs + PAIR_ARB_NET_EPS >= RISK_INCR_TIER_2_NET_DIFF:
        mult = tier_2_mult
    elif current_abs + PAIR_ARB_NET_EPS >= RISK_INCR_TIER_1_NET_DIFF:
        mult = tier_1_mult
    else:
        return None
    if side == "YES" and inv.yes_qty > 1e-9 and inv.yes_avg_cost > 0:
        return inv.yes_avg_cost * mult
    elif side == "NO" and inv.no_qty > 1e-9 and inv.no_avg_cost > 0:
        return inv.no_avg_cost * mult
    return None


def vwap_ceiling(pair_target: float, safety_margin: float,
                 opp_avg: float, same_qty: float, same_avg: float,
                 bid_size: float) -> float:
    """vwap_ceiling"""
    guarded = max(pair_target - safety_margin, 0.0)
    legacy = guarded - opp_avg
    if same_qty <= PAIR_ARB_NET_EPS or bid_size <= PAIR_ARB_NET_EPS:
        return legacy
    numerator = (guarded - opp_avg) * (same_qty + bid_size) - same_qty * same_avg
    ceiling = numerator / bid_size
    if math.isfinite(ceiling):
        return ceiling
    return legacy


def safe_price(price: float, tick: float = 0.01) -> float:
    """Round to tick, clamp to [tick, 0.99]"""
    p = round(price / tick) * tick
    if p < tick:
        return 0.0
    return min(p, 0.99)


def compute_quotes(cfg: Config, inv: Inventory,
                   ask_up: float, bid_up: float,
                   ask_down: float, bid_down: float,
                   remaining_sec: float,
                   total_window_sec: float = 300.0):
    """
    Returns dict with 'yes_bid', 'yes_size', 'no_bid', 'no_size'.
    Values are 0 if suppressed.
    """
    mid_yes = (bid_up + ask_up) / 2.0
    mid_no = (bid_down + ask_down) / 2.0

    # Base pricing
    excess = max(0.0, (mid_yes + mid_no) - cfg.pair_target)
    skew = (inv.net_diff / cfg.max_net_diff if cfg.max_net_diff > 0 else 0.0)
    skew = max(-1.0, min(1.0, skew))
    abs_nd = abs(inv.net_diff)

    # Time decay factor (simplified — production uses a more complex version)
    elapsed = total_window_sec - remaining_sec
    time_decay = 1.0 + (elapsed / total_window_sec) * 0.6  # ramp from 1.0 to 1.6

    # Effective skew factor
    if abs_nd < TIER_1_NET_DIFF:
        eff_skew_fac = cfg.as_skew_factor * EARLY_SKEW_MULT
    elif abs_nd < TIER_2_NET_DIFF:
        ramp = (abs_nd - TIER_1_NET_DIFF) / (TIER_2_NET_DIFF - TIER_1_NET_DIFF)
        eff_skew_fac = cfg.as_skew_factor * (EARLY_SKEW_MULT + (1.0 - EARLY_SKEW_MULT) * ramp) * time_decay
    else:
        eff_skew_fac = cfg.as_skew_factor * time_decay

    skew_shift = skew * eff_skew_fac

    raw_yes = mid_yes - (excess / 2.0) - skew_shift
    raw_no = mid_no - (excess / 2.0) + skew_shift

    if raw_yes + raw_no > cfg.pair_target:
        overflow = (raw_yes + raw_no) - cfg.pair_target
        raw_yes -= overflow / 2.0
        raw_no -= overflow / 2.0

    # Candidate sizes
    y_size = candidate_size(inv, "YES", cfg.bid_size)
    n_size = candidate_size(inv, "NO", cfg.bid_size)
    y_re = risk_effect(inv, "YES", y_size)
    n_re = risk_effect(inv, "NO", n_size)

    # Tier cap
    y_cap = tier_cap_price(inv, "YES", y_re, cfg.tier_1_mult, cfg.tier_2_mult)
    if y_cap is not None:
        raw_yes = min(raw_yes, y_cap)
    n_cap = tier_cap_price(inv, "NO", n_re, cfg.tier_1_mult, cfg.tier_2_mult)
    if n_cap is not None:
        raw_no = min(raw_no, n_cap)

    # VWAP ceiling
    eff_margin = 0.0 if abs(inv.net_diff) < PAIR_ARB_NET_EPS else cfg.pair_cost_safety_margin
    disable_yes = False
    disable_no = False

    if inv.no_qty > 1e-9 and inv.no_avg_cost > 0:
        y_ceil = vwap_ceiling(cfg.pair_target, eff_margin, inv.no_avg_cost,
                              inv.yes_qty, inv.yes_avg_cost, y_size)
        raw_yes = min(raw_yes, y_ceil)
        if y_ceil <= cfg.tick_size + 1e-9:
            disable_yes = True

    if inv.yes_qty > 1e-9 and inv.yes_avg_cost > 0:
        n_ceil = vwap_ceiling(cfg.pair_target, eff_margin, inv.yes_avg_cost,
                              inv.no_qty, inv.no_avg_cost, n_size)
        raw_no = min(raw_no, n_ceil)
        if n_ceil <= cfg.tick_size + 1e-9:
            disable_no = True

    # Post-only safety (risk-increasing only)
    if ask_up > 0 and y_re == "risk_increasing":
        raw_yes = min(raw_yes, ask_up - cfg.tick_size)
    if ask_down > 0 and n_re == "risk_increasing":
        raw_no = min(raw_no, ask_down - cfg.tick_size)

    bid_yes = 0.0 if disable_yes else safe_price(raw_yes, cfg.tick_size)
    bid_no = 0.0 if disable_no else safe_price(raw_no, cfg.tick_size)

    # Inventory gate (max_net_diff)
    if bid_yes > 0 and y_re == "risk_increasing":
        projected = abs(inv.net_diff + y_size)
        if projected > cfg.max_net_diff + PAIR_ARB_NET_EPS:
            bid_yes = 0.0

    if bid_no > 0 and n_re == "risk_increasing":
        projected = abs(inv.net_diff - n_size)
        if projected > cfg.max_net_diff + PAIR_ARB_NET_EPS:
            bid_no = 0.0

    # Risk open cutoff
    if remaining_sec <= cfg.risk_open_cutoff_secs:
        if y_re == "risk_increasing":
            bid_yes = 0.0
        if n_re == "risk_increasing":
            bid_no = 0.0

    return {
        "yes_bid": bid_yes, "yes_size": y_size,
        "no_bid": bid_no, "no_size": n_size,
    }

# ============================================================
# Fill Simulation
# ============================================================

def check_fill(our_bid: float, market_ask: float, model: str = "conservative") -> bool:
    """
    Conservative: filled when market ask drops to or below our bid
    (someone is selling at or below our price)
    """
    if our_bid <= 0:
        return False
    if market_ask is None or market_ask <= 0:
        return False
    if model == "conservative":
        return market_ask <= our_bid
    else:  # aggressive: filled when bid >= market bid (we're at top of book)
        return our_bid >= market_ask


# ============================================================
# Pairing & P&L
# ============================================================

def compute_paired_pnl(inv: Inventory) -> dict:
    """Compute paired quantity and locked P&L"""
    paired = min(inv.yes_qty, inv.no_qty)
    if paired < 0.01:
        return {"paired_qty": 0, "pair_cost": 0, "locked_pnl": 0}
    pair_cost = inv.yes_avg_cost + inv.no_avg_cost
    locked_pnl = paired * (1.0 - pair_cost)
    return {
        "paired_qty": paired,
        "pair_cost": pair_cost,
        "locked_pnl": locked_pnl,
    }


def compute_settlement(inv: Inventory, outcome: str) -> dict:
    """Settle residual at end of window"""
    paired = min(inv.yes_qty, inv.no_qty)
    res_yes = inv.yes_qty - paired
    res_no = inv.no_qty - paired

    pair_cost = inv.yes_avg_cost + inv.no_avg_cost if paired > 0 else 0
    paired_pnl = paired * max(0, 1.0 - pair_cost) if paired > 0 else 0

    if outcome == "UP":
        # YES wins: YES tokens worth 1.0, NO tokens worth 0
        res_pnl = res_yes * (1.0 - inv.yes_avg_cost) - res_no * inv.no_avg_cost
    elif outcome == "DOWN":
        # NO wins: NO tokens worth 1.0, YES tokens worth 0
        res_pnl = res_no * (1.0 - inv.no_avg_cost) - res_yes * inv.yes_avg_cost
    else:
        res_pnl = 0.0  # unclear outcome

    return {
        "paired_qty": paired,
        "pair_cost": pair_cost,
        "paired_pnl": paired_pnl,
        "residual_yes": res_yes,
        "residual_no": res_no,
        "residual_pnl": res_pnl,
        "total_pnl": paired_pnl + res_pnl,
        "outcome": outcome,
    }


# ============================================================
# Main Backtest Engine
# ============================================================

def run_backtest(db_path: str, cfg: Config, limit: int = 0, verbose: bool = False):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Get all conditions with settlement
    query = """
        SELECT sr.condition_id, sr.outcome, sr.btc_open, sr.btc_close_binance
        FROM settlement_records sr
        WHERE sr.outcome IN ('UP', 'DOWN')
        ORDER BY sr.ts_end
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    c.execute(query)
    settlements = c.fetchall()
    print(f"📊 Backtesting {len(settlements)} windows (model={cfg.fill_model}, "
          f"max_net={cfg.max_net_diff}, pair_target={cfg.pair_target}, "
          f"bid={cfg.bid_size}, tier1={cfg.tier_1_mult}, tier2={cfg.tier_2_mult})")
    print("=" * 100)

    # Aggregate stats
    total_paired_pnl = 0.0
    total_residual_pnl = 0.0
    total_pnl = 0.0
    total_fills = 0
    total_pairs = 0.0
    n_windows = 0
    n_with_residual = 0
    n_perfect_pair = 0
    residual_win = 0
    residual_lose = 0
    window_pnls = []

    for sett in settlements:
        cid = sett["condition_id"]
        outcome = sett["outcome"]

        # Load ticks for this condition
        c.execute("""
            SELECT ts, remaining_sec, btc_open, btc_price,
                   ask_up, bid_up, ask_down, bid_down
            FROM market_ticks
            WHERE condition_id = ?
            ORDER BY ts ASC
        """, (cid,))
        ticks = c.fetchall()
        if len(ticks) < 10:
            continue

        # Initialize per-window state
        inv = Inventory()
        fills = []
        window_total_sec = ticks[0]["remaining_sec"] if ticks[0]["remaining_sec"] else 300.0

        # Active orders (simplified: one YES bid, one NO bid at a time)
        active_yes_bid = 0.0
        active_yes_size = 0.0
        active_no_bid = 0.0
        active_no_size = 0.0
        last_quote_ts = 0

        for tick in ticks:
            ts = tick["ts"]
            rem = tick["remaining_sec"] if tick["remaining_sec"] is not None else 0
            ask_up = tick["ask_up"] if tick["ask_up"] else 0
            bid_up = tick["bid_up"] if tick["bid_up"] else 0
            ask_down = tick["ask_down"] if tick["ask_down"] else 0
            bid_down = tick["bid_down"] if tick["bid_down"] else 0

            if ask_up <= 0 or bid_up <= 0 or ask_down <= 0 or bid_down <= 0:
                continue

            # 1) Check fills on active orders
            if active_yes_bid > 0 and check_fill(active_yes_bid, ask_up, cfg.fill_model):
                inv.update_after_fill("YES", active_yes_size, active_yes_bid)
                fills.append(("YES", active_yes_bid, active_yes_size, rem))
                active_yes_bid = 0.0
                active_yes_size = 0.0
                last_quote_ts = 0  # force requote

            if active_no_bid > 0 and check_fill(active_no_bid, ask_down, cfg.fill_model):
                inv.update_after_fill("NO", active_no_size, active_no_bid)
                fills.append(("NO", active_no_bid, active_no_size, rem))
                active_no_bid = 0.0
                active_no_size = 0.0
                last_quote_ts = 0

            # 2) Requote every ~2 seconds or after fill
            if ts - last_quote_ts >= 2:
                q = compute_quotes(cfg, inv, ask_up, bid_up, ask_down, bid_down,
                                   rem, window_total_sec)
                active_yes_bid = q["yes_bid"]
                active_yes_size = q["yes_size"]
                active_no_bid = q["no_bid"]
                active_no_size = q["no_size"]
                last_quote_ts = ts

        # 3) Settlement
        result = compute_settlement(inv, outcome)
        n_windows += 1
        total_paired_pnl += result["paired_pnl"]
        total_residual_pnl += result["residual_pnl"]
        total_pnl += result["total_pnl"]
        total_fills += len(fills)
        total_pairs += result["paired_qty"]
        window_pnls.append(result["total_pnl"])

        has_residual = (result["residual_yes"] > 0.01 or result["residual_no"] > 0.01)
        if has_residual:
            n_with_residual += 1
            if result["residual_pnl"] > 0:
                residual_win += 1
            else:
                residual_lose += 1
        else:
            n_perfect_pair += 1

        if verbose and (len(fills) > 0 or has_residual):
            dt = datetime.fromtimestamp(ticks[0]["ts"], tz=timezone.utc).strftime("%m-%d %H:%M")
            fill_str = " ".join([f"{f[0][0]}@{f[1]:.2f}" for f in fills[:6]])
            print(f"  [{dt}] fills={len(fills):2d} paired={result['paired_qty']:5.1f} "
                  f"pair_cost={result['pair_cost']:.3f} "
                  f"res_Y={result['residual_yes']:.1f} res_N={result['residual_no']:.1f} "
                  f"outcome={outcome:4s} "
                  f"paired_pnl={result['paired_pnl']:+.3f} "
                  f"res_pnl={result['residual_pnl']:+.3f} "
                  f"total={result['total_pnl']:+.3f} | {fill_str}")

    # Summary
    print("\n" + "=" * 100)
    print("📈 BACKTEST RESULTS")
    print("=" * 100)
    print(f"  Windows tested:        {n_windows:,}")
    print(f"  Total fills:           {total_fills:,}")
    print(f"  Total paired qty:      {total_pairs:,.1f}")
    print(f"  Perfect pair windows:  {n_perfect_pair} ({100*n_perfect_pair/max(n_windows,1):.1f}%)")
    print(f"  Windows with residual: {n_with_residual} ({100*n_with_residual/max(n_windows,1):.1f}%)")
    if n_with_residual > 0:
        print(f"    Residual wins:       {residual_win} ({100*residual_win/max(n_with_residual,1):.1f}%)")
        print(f"    Residual losses:     {residual_lose} ({100*residual_lose/max(n_with_residual,1):.1f}%)")
    print()
    print(f"  💰 Paired P&L:         {total_paired_pnl:+,.2f} USDC")
    print(f"  💀 Residual P&L:       {total_residual_pnl:+,.2f} USDC")
    print(f"  📊 Total P&L:          {total_pnl:+,.2f} USDC")
    print(f"  📊 Avg P&L/window:     {total_pnl/max(n_windows,1):+.4f} USDC")
    if total_pairs > 0:
        print(f"  📊 Avg pair cost:      {(total_paired_pnl/total_pairs):.4f} profit/pair")

    # Distribution analysis
    if window_pnls:
        sorted_pnls = sorted(window_pnls)
        n = len(sorted_pnls)
        print(f"\n  P&L Distribution:")
        print(f"    Min:     {sorted_pnls[0]:+.3f}")
        print(f"    p5:      {sorted_pnls[int(n*0.05)]:+.3f}")
        print(f"    p25:     {sorted_pnls[int(n*0.25)]:+.3f}")
        print(f"    Median:  {sorted_pnls[int(n*0.50)]:+.3f}")
        print(f"    p75:     {sorted_pnls[int(n*0.75)]:+.3f}")
        print(f"    p95:     {sorted_pnls[int(n*0.95)]:+.3f}")
        print(f"    Max:     {sorted_pnls[-1]:+.3f}")

        win_count = sum(1 for p in window_pnls if p > 0)
        lose_count = sum(1 for p in window_pnls if p < 0)
        zero_count = sum(1 for p in window_pnls if abs(p) < 0.001)
        print(f"\n    Win/Loss/Zero: {win_count}/{lose_count}/{zero_count}")
        if win_count > 0:
            avg_win = sum(p for p in window_pnls if p > 0) / win_count
            print(f"    Avg Win:   {avg_win:+.3f}")
        if lose_count > 0:
            avg_lose = sum(p for p in window_pnls if p < 0) / lose_count
            print(f"    Avg Loss:  {avg_lose:+.3f}")

    conn.close()
    return total_pnl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pair-Arb Strategy Backtester")
    parser.add_argument("--db", default="/Users/hot/web3Scientist/poly_trans_research/btc5m.db")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of windows (0=all)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--max-net-diff", type=float, default=5.0)
    parser.add_argument("--pair-target", type=float, default=0.97)
    parser.add_argument("--bid-size", type=float, default=5.0)
    parser.add_argument("--tier1", type=float, default=0.50)
    parser.add_argument("--tier2", type=float, default=0.15)
    parser.add_argument("--cutoff", type=float, default=240.0,
                        help="risk_open_cutoff_secs")
    parser.add_argument("--fill-model", choices=["conservative", "aggressive"],
                        default="conservative")
    parser.add_argument("--balance", type=float, default=50.0,
                        help="Initial USDC balance (large for unconstrained test)")
    args = parser.parse_args()

    cfg = Config(
        max_net_diff=args.max_net_diff,
        pair_target=args.pair_target,
        bid_size=args.bid_size,
        tier_1_mult=args.tier1,
        tier_2_mult=args.tier2,
        risk_open_cutoff_secs=args.cutoff,
        initial_balance=args.balance,
        fill_model=args.fill_model,
    )

    run_backtest(args.db, cfg, limit=args.limit, verbose=args.verbose)
