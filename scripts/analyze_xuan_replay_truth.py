#!/usr/bin/env python3
"""Read-only xuan BTC 5m replay truth analyzer.

This script intentionally uses xuan_activity.usdc_size / size as execution cost.
The public xuan_trades.price field is a display price and systematically
understates realized cost in the current replay dataset.

Direction semantics are taken from replay-side normalized fields:
xuan_activity.outcome_side and settlement_records.winner_side. The analyzer does
not map Up/Down text labels to YES/NO.
"""

from __future__ import annotations

import argparse
import collections
import datetime
import json
import math
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_REPLAY_ROOT = Path("/Users/hot/web3Scientist/poly_trans_research/data/replay")
DEFAULT_DAYS = ("2026-04-27", "2026-04-28", "2026-04-29", "2026-04-30", "2026-05-01")
DEFAULT_TRUSTED_START_MS = 1_777_274_700_000
DEFAULT_OUTAGE_START_MS = 1_777_374_000_000
DEFAULT_OUTAGE_END_MS = 1_777_377_600_000
DEFAULT_CAPS = (0.99, 1.0, 1.005, 1.01, 1.02, 1.03, 1.04, 1.05, 1.08, 1.10, 1.15)
SIDES = ("YES", "NO")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--replay-root", default=str(DEFAULT_REPLAY_ROOT))
    p.add_argument("--days", nargs="+", default=list(DEFAULT_DAYS))
    p.add_argument("--trusted-start-ms", type=int, default=DEFAULT_TRUSTED_START_MS)
    p.add_argument("--min-start-ms", type=int, help="Optional market start_ms lower bound.")
    p.add_argument("--max-start-ms", type=int, help="Optional market start_ms upper bound.")
    p.add_argument("--outage-start-ms", type=int, default=DEFAULT_OUTAGE_START_MS)
    p.add_argument("--outage-end-ms", type=int, default=DEFAULT_OUTAGE_END_MS)
    p.add_argument("--output-json", help="Optional output path; stdout is always written.")
    return p.parse_args()


def open_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.execute("pragma query_only=ON")
    conn.row_factory = sqlite3.Row
    return conn


def q(value: Any, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    rank = (len(values) - 1) * pct / 100.0
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return values[lo]
    return values[lo] * (hi - rank) + values[hi] * (rank - lo)


def summary(values: list[float | None]) -> dict[str, float | int | None]:
    xs = [float(v) for v in values if v is not None]
    if not xs:
        return {"n": 0}
    return {
        "n": len(xs),
        "min": min(xs),
        "p10": percentile(xs, 10.0),
        "p25": percentile(xs, 25.0),
        "p50": percentile(xs, 50.0),
        "p75": percentile(xs, 75.0),
        "p90": percentile(xs, 90.0),
        "p95": percentile(xs, 95.0),
        "max": max(xs),
        "mean": sum(xs) / len(xs),
        "sum": sum(xs),
    }


def normalized_side(value: Any) -> str | None:
    side = str(value or "").upper()
    if side in SIDES:
        return side
    return None


def opposite_side(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def utc_day(ms: int) -> str:
    return datetime.datetime.utcfromtimestamp(ms / 1000.0).date().isoformat()


def load_markets(args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    markets: dict[str, dict[str, Any]] = {}
    root = Path(args.replay_root)
    for day in args.days:
        db = root / day / "crypto_5m.sqlite"
        conn = open_ro(db)
        try:
            for row in conn.execute(
                """
                select condition_id, slug, symbol, interval_sec, start_ms, end_ms
                from market_meta
                where symbol='BTC' and interval_sec=300 and end_ms>=?
                """,
                (args.trusted_start_ms,),
            ):
                if row["start_ms"] < args.outage_end_ms and row["end_ms"] > args.outage_start_ms:
                    continue
                if args.min_start_ms is not None and row["start_ms"] < args.min_start_ms:
                    continue
                if args.max_start_ms is not None and row["start_ms"] > args.max_start_ms:
                    continue
                markets.setdefault(row["condition_id"], dict(row))
        finally:
            conn.close()
    return markets


def load_settlements(args: argparse.Namespace, markets: dict[str, dict[str, Any]]) -> None:
    root = Path(args.replay_root)
    for day in args.days:
        db = root / day / "crypto_5m.sqlite"
        conn = open_ro(db)
        try:
            for row in conn.execute(
                """
                select condition_id, winner_side, winner_token_id, resolution_source, settle_ms
                from settlement_records
                """
            ):
                market = markets.get(row["condition_id"])
                if market is None:
                    continue
                winner_side = normalized_side(row["winner_side"])
                if winner_side is None:
                    continue
                market["winner_side"] = winner_side
                market["winner_token_id"] = row["winner_token_id"]
                market["resolution_source"] = row["resolution_source"]
                market["settle_ms"] = row["settle_ms"]
        finally:
            conn.close()


def load_deduped_activity(args: argparse.Namespace, markets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    root = Path(args.replay_root)
    for day in args.days:
        db = root / day / "crypto_5m.sqlite"
        conn = open_ro(db)
        try:
            for row in conn.execute(
                """
                select condition_id, slug, activity_ts_ms, activity_type, outcome,
                       outcome_side, side, price, size, usdc_size, asset, tx_hash,
                       poll_ts_ms
                from xuan_activity
                where condition_id is not null and slug like 'btc-updown-5m%'
                """
            ):
                if row["condition_id"] not in markets:
                    continue
                key = (
                    row["condition_id"],
                    row["tx_hash"] or "",
                    row["activity_ts_ms"],
                    row["activity_type"] or "",
                    row["outcome"] or "",
                    row["outcome_side"] or "",
                    row["side"] or "",
                    q(row["price"]),
                    q(row["size"]),
                    q(row["usdc_size"]),
                    row["asset"] or "",
                )
                rec = dict(row)
                rec["_source_day"] = day
                old = rows_by_key.get(key)
                if old is None or (rec.get("poll_ts_ms") or 10**20) < (old.get("poll_ts_ms") or 10**20):
                    rows_by_key[key] = rec
        finally:
            conn.close()
    return list(rows_by_key.values())


def trade_value(row: dict[str, Any]) -> float:
    size = float(row.get("size") or 0.0)
    usdc = float(row.get("usdc_size") or 0.0)
    price = float(row.get("price") or 0.0)
    return usdc if usdc > 0.0 else size * price


def exec_price(row: dict[str, Any]) -> float:
    size = float(row.get("size") or 0.0)
    usdc = float(row.get("usdc_size") or 0.0)
    price = float(row.get("price") or 0.0)
    if size > 0.0 and usdc > 0.0:
        return usdc / size
    return price


def build_truth(markets: dict[str, dict[str, Any]], activity: list[dict[str, Any]]) -> dict[str, Any]:
    trades: list[dict[str, Any]] = []
    cash = collections.defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "merge": 0.0, "redeem": 0.0})
    price_gap: list[float] = []
    missing_outcome_side = 0

    for row in activity:
        cid = row["condition_id"]
        typ = str(row.get("activity_type") or "").upper()
        side = str(row.get("side") or "").upper()
        value = trade_value(row)
        size = float(row.get("size") or 0.0)

        if typ == "TRADE" and side == "BUY":
            outcome_side = normalized_side(row.get("outcome_side"))
            if outcome_side is None:
                missing_outcome_side += 1
                continue
            rec = dict(row)
            rec["outcome_side"] = outcome_side
            rec["exec_price"] = exec_price(row)
            trades.append(rec)
            cash[cid]["buy"] += value
            display_price = float(row.get("price") or 0.0)
            if display_price > 0.0 and size > 0.0:
                price_gap.append(rec["exec_price"] / display_price - 1.0)
        elif typ == "TRADE" and side == "SELL":
            cash[cid]["sell"] += value
        elif typ == "MERGE":
            cash[cid]["merge"] += value if value > 0.0 else size
        elif typ == "REDEEM":
            cash[cid]["redeem"] += value if value > 0.0 else size

    by_market: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for row in trades:
        by_market[row["condition_id"]].append(row)
    for rows in by_market.values():
        rows.sort(key=lambda r: (r.get("activity_ts_ms") or 0, r.get("tx_hash") or "", r.get("outcome_side") or ""))

    market_rows: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    clips = collections.defaultdict(list)
    clip_prices = collections.defaultdict(list)

    for cid, rows in by_market.items():
        market = markets[cid]
        totals = {side: {"qty": 0.0, "cost": 0.0} for side in SIDES}
        open_legs = {side: [] for side in SIDES}
        buy_idx = 0

        for row in rows:
            outcome_side = normalized_side(row.get("outcome_side"))
            if outcome_side is None:
                continue
            qty = float(row.get("size") or 0.0)
            price = float(row["exec_price"])
            value = trade_value(row)
            ts = int(row.get("activity_ts_ms") or 0)
            totals[outcome_side]["qty"] += qty
            totals[outcome_side]["cost"] += value
            buy_idx += 1
            if buy_idx <= 15:
                clips[buy_idx].append(qty)
                clip_prices[buy_idx].append(price)

            opposite = opposite_side(outcome_side)
            remaining = qty
            while remaining > 1e-9 and open_legs[opposite]:
                leg = open_legs[opposite][0]
                take = min(remaining, leg["qty"])
                pair_cost = leg["price"] + price
                episodes.append(
                    {
                        "condition_id": cid,
                        "slug": market["slug"],
                        "qty": take,
                        "pair_cost": pair_cost,
                        "pnl": (1.0 - pair_cost) * take,
                        "delay_s": (ts - leg["ts"]) / 1000.0,
                        "close_rel_s": (ts - market["start_ms"]) / 1000.0,
                        "second_ts": ts,
                    }
                )
                leg["qty"] -= take
                remaining -= take
                if leg["qty"] <= 1e-9:
                    open_legs[opposite].pop(0)
            if remaining > 1e-9:
                open_legs[outcome_side].append({"qty": remaining, "price": price, "ts": ts})

        yes_qty = totals["YES"]["qty"]
        no_qty = totals["NO"]["qty"]
        pair_qty = min(yes_qty, no_qty)
        residual_qty = abs(yes_qty - no_qty)
        residual_side = None
        if yes_qty > no_qty + 1e-9:
            residual_side = "YES"
        elif no_qty > yes_qty + 1e-9:
            residual_side = "NO"

        pair_cost = None
        locked_pnl = None
        if pair_qty > 1e-9 and yes_qty > 0.0 and no_qty > 0.0:
            pair_cost = totals["YES"]["cost"] / yes_qty + totals["NO"]["cost"] / no_qty
            locked_pnl = (1.0 - pair_cost) * pair_qty

        buy_cost = totals["YES"]["cost"] + totals["NO"]["cost"]
        winner_side = normalized_side(market.get("winner_side"))
        residual_winner_qty = residual_qty if residual_side is not None and residual_side == winner_side else 0.0
        residual_loser_qty = (
            residual_qty if residual_side is not None and winner_side is not None and residual_side != winner_side else 0.0
        )
        economic_final_value = pair_qty + residual_winner_qty if winner_side is not None else None
        economic_final_pnl = economic_final_value - buy_cost if economic_final_value is not None else None
        cf = cash[cid]
        cash_pnl = cf["sell"] + cf["merge"] + cf["redeem"] - cf["buy"]
        market_rows.append(
            {
                "condition_id": cid,
                "slug": market["slug"],
                "start_ms": market["start_ms"],
                "day": utc_day(int(market["start_ms"])),
                "winner_side": winner_side,
                "yes_qty": yes_qty,
                "no_qty": no_qty,
                "buy_cost": buy_cost,
                "pair_qty": pair_qty,
                "residual_qty": residual_qty,
                "residual_side": residual_side,
                "residual_winner_qty": residual_winner_qty,
                "residual_loser_qty": residual_loser_qty,
                "pair_cost": pair_cost,
                "locked_pnl": locked_pnl,
                "economic_final_value": economic_final_value,
                "economic_final_pnl": economic_final_pnl,
                "economic_final_roi": economic_final_pnl / buy_cost if economic_final_pnl is not None and buy_cost > 0.0 else None,
                "cash_vs_economic_gap": cash_pnl - economic_final_pnl if economic_final_pnl is not None else None,
                "cash_pnl": cash_pnl,
                "cash_roi": cash_pnl / cf["buy"] if cf["buy"] > 0.0 else None,
            }
        )

    episodes.sort(key=lambda row: (row["second_ts"], row["slug"]))
    total_episode_qty = sum(row["qty"] for row in episodes)
    settled_markets = [row for row in market_rows if row["winner_side"] is not None]
    by_day: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for row in market_rows:
        by_day[row["day"]].append(row)

    def market_group_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
        buy_cost = sum(row["buy_cost"] for row in rows if row["economic_final_pnl"] is not None)
        economic_pnl = sum(row["economic_final_pnl"] or 0.0 for row in rows)
        cash_buy = sum(cash[row["condition_id"]]["buy"] for row in rows)
        cash_pnl = sum(row["cash_pnl"] for row in rows)
        return {
            "markets": len(rows),
            "buy_cost": buy_cost,
            "economic_final_pnl": economic_pnl,
            "economic_final_roi": economic_pnl / buy_cost if buy_cost > 0.0 else None,
            "cash_buy": cash_buy,
            "cash_pnl": cash_pnl,
            "cash_roi": cash_pnl / cash_buy if cash_buy > 0.0 else None,
            "pair_cost": summary([row["pair_cost"] for row in rows]),
            "residual_qty": summary([row["residual_qty"] for row in rows]),
            "residual_winner_qty_sum": sum(row["residual_winner_qty"] for row in rows),
            "residual_loser_qty_sum": sum(row["residual_loser_qty"] for row in rows),
        }

    return {
        "universe": {
            "valid_btc_5m_markets": len(markets),
            "settled_btc_5m_markets": sum(1 for market in markets.values() if normalized_side(market.get("winner_side")) is not None),
            "xuan_activity_rows_unique": len(activity),
            "xuan_trade_markets": len(by_market),
            "xuan_trade_rows": len(trades),
            "xuan_buy_rows_missing_outcome_side": missing_outcome_side,
        },
        "price_gap_exec_over_display": summary(price_gap),
        "market": {
            "pair_cost": summary([row["pair_cost"] for row in market_rows]),
            "residual_qty": summary([row["residual_qty"] for row in market_rows]),
            "residual_winner_qty": summary([row["residual_winner_qty"] for row in settled_markets]),
            "residual_loser_qty": summary([row["residual_loser_qty"] for row in settled_markets]),
            "residual_side_winner_ratio": (
                sum(1 for row in settled_markets if row["residual_qty"] > 1.0 and row["residual_side"] == row["winner_side"])
                / max(sum(1 for row in settled_markets if row["residual_qty"] > 1.0), 1)
            ),
            "clean_ratio_residual_le_1": sum(1 for row in market_rows if row["residual_qty"] <= 1.0) / len(market_rows)
            if market_rows
            else None,
            "locked_pnl_sum_proxy": sum(row["locked_pnl"] or 0.0 for row in market_rows),
            "economic_final": {
                "value": sum(row["economic_final_value"] or 0.0 for row in market_rows),
                "buy_cost": sum(row["buy_cost"] for row in settled_markets),
                "pnl": sum(row["economic_final_pnl"] or 0.0 for row in settled_markets),
                "roi": (
                    sum(row["economic_final_pnl"] or 0.0 for row in settled_markets)
                    / sum(row["buy_cost"] for row in settled_markets)
                ),
                "pnl_dist": summary([row["economic_final_pnl"] for row in settled_markets]),
                "roi_dist": summary([row["economic_final_roi"] for row in settled_markets]),
            },
            "cash": {
                "buy": sum(cash[cid]["buy"] for cid in by_market),
                "merge": sum(cash[cid]["merge"] for cid in by_market),
                "redeem": sum(cash[cid]["redeem"] for cid in by_market),
                "sell": sum(cash[cid]["sell"] for cid in by_market),
                "pnl": sum(row["cash_pnl"] for row in market_rows),
            },
            "cash_pnl": summary([row["cash_pnl"] for row in market_rows]),
            "cash_roi": summary([row["cash_roi"] for row in market_rows]),
            "cash_vs_economic_gap": summary([row["cash_vs_economic_gap"] for row in settled_markets]),
        },
        "per_day": {day: market_group_summary(rows) for day, rows in sorted(by_day.items())},
        "episode": {
            "episodes": len(episodes),
            "qty": summary([row["qty"] for row in episodes]),
            "pair_cost": summary([row["pair_cost"] for row in episodes]),
            "delay_s": summary([row["delay_s"] for row in episodes]),
            "delay_le_30_ratio": sum(1 for row in episodes if row["delay_s"] <= 30.0) / len(episodes) if episodes else None,
            "delay_le_60_ratio": sum(1 for row in episodes if row["delay_s"] <= 60.0) / len(episodes) if episodes else None,
            "pnl_sum_proxy": sum(row["pnl"] for row in episodes),
        },
        "cap_frontier": cap_frontier(episodes, total_episode_qty),
        "per_market_funded_frontier_50pct_reserve_0p5c": funded_frontier(episodes, total_episode_qty),
        "clip": {
            "size": summary([float(row.get("size") or 0.0) for row in trades]),
            "by_trade_index": {
                str(idx): {"qty": summary(clips[idx]), "exec_price": summary(clip_prices[idx])}
                for idx in range(1, 16)
                if clips[idx]
            },
        },
        "examples": {
            "worst_economic_markets": sorted(
                [
                    {
                        key: row[key]
                        for key in (
                            "slug",
                            "day",
                            "winner_side",
                            "pair_cost",
                            "pair_qty",
                            "residual_qty",
                            "residual_side",
                            "residual_winner_qty",
                            "residual_loser_qty",
                            "economic_final_pnl",
                            "cash_pnl",
                        )
                    }
                    for row in settled_markets
                ],
                key=lambda row: row["economic_final_pnl"],
            )[:12],
            "largest_loser_residual_markets": sorted(
                [
                    {
                        key: row[key]
                        for key in (
                            "slug",
                            "day",
                            "winner_side",
                            "pair_cost",
                            "pair_qty",
                            "residual_qty",
                            "residual_side",
                            "residual_winner_qty",
                            "residual_loser_qty",
                            "economic_final_pnl",
                        )
                    }
                    for row in settled_markets
                ],
                key=lambda row: row["residual_loser_qty"],
                reverse=True,
            )[:12],
        },
    }


def cap_frontier(episodes: list[dict[str, Any]], total_qty: float) -> list[dict[str, Any]]:
    rows = []
    for cap in DEFAULT_CAPS:
        accepted = [row for row in episodes if row["pair_cost"] <= cap]
        qty = sum(row["qty"] for row in accepted)
        pnl = sum(row["pnl"] for row in accepted)
        neg_spend = sum(-row["pnl"] for row in accepted if row["pnl"] < 0.0)
        rows.append(
            {
                "cap": cap,
                "qty_ratio": qty / total_qty if total_qty > 0.0 else 0.0,
                "pnl": pnl,
                "pnl_per_pair": pnl / qty if qty > 0.0 else 0.0,
                "neg_spend": neg_spend,
                "episodes": len(accepted),
            }
        )
    return rows


def funded_frontier(episodes: list[dict[str, Any]], total_qty: float) -> list[dict[str, Any]]:
    by_market: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for row in episodes:
        by_market[row["condition_id"]].append(row)

    out = []
    for hard_cap in (1.01, 1.02, 1.03, 1.04, 1.05, 1.08, 1.10, 1.15):
        accepted = []
        rejected_qty = 0.0
        for rows in by_market.values():
            surplus = 0.0
            accepted_qty = 0.0
            for row in rows:
                pnl = row["pnl"]
                if row["pair_cost"] <= 1.0:
                    accepted.append(row)
                    surplus += pnl
                    accepted_qty += row["qty"]
                    continue
                available = max(0.0, surplus - 0.005 * accepted_qty) * 0.50
                if row["pair_cost"] <= hard_cap and -pnl <= available + 1e-9:
                    accepted.append(row)
                    surplus += pnl
                    accepted_qty += row["qty"]
                else:
                    rejected_qty += row["qty"]
        qty = sum(row["qty"] for row in accepted)
        pnl = sum(row["pnl"] for row in accepted)
        out.append(
            {
                "hard_cap": hard_cap,
                "qty_ratio": qty / total_qty if total_qty > 0.0 else 0.0,
                "pnl": pnl,
                "pnl_per_pair": pnl / qty if qty > 0.0 else 0.0,
                "neg_spend": sum(-row["pnl"] for row in accepted if row["pnl"] < 0.0),
                "rejected_qty": rejected_qty,
                "episodes": len(accepted),
            }
        )
    return out


def main() -> None:
    args = parse_args()
    markets = load_markets(args)
    load_settlements(args, markets)
    activity = load_deduped_activity(args, markets)
    payload = {
        "data": {
            "replay_root": str(Path(args.replay_root)),
            "days": args.days,
            "trusted_start_ms": args.trusted_start_ms,
            "min_start_ms": args.min_start_ms,
            "max_start_ms": args.max_start_ms,
            "excluded_outage_ms": [args.outage_start_ms, args.outage_end_ms],
        },
        **build_truth(markets, activity),
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_json:
        Path(args.output_json).write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
