#!/usr/bin/env python3
"""Read-only xuan BTC 5m replay truth analyzer.

This script intentionally uses xuan_activity.usdc_size / size as execution cost.
The public xuan_trades.price field is a display price and systematically
understates realized cost in the current replay dataset.
"""

from __future__ import annotations

import argparse
import collections
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--replay-root", default=str(DEFAULT_REPLAY_ROOT))
    p.add_argument("--days", nargs="+", default=list(DEFAULT_DAYS))
    p.add_argument("--trusted-start-ms", type=int, default=DEFAULT_TRUSTED_START_MS)
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
                markets.setdefault(row["condition_id"], dict(row))
        finally:
            conn.close()
    return markets


def load_deduped_activity(args: argparse.Namespace, markets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    root = Path(args.replay_root)
    for day in args.days:
        db = root / day / "crypto_5m.sqlite"
        conn = open_ro(db)
        try:
            for row in conn.execute(
                """
                select condition_id, slug, activity_ts_ms, activity_type, outcome, side,
                       price, size, usdc_size, asset, tx_hash, poll_ts_ms
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

    for row in activity:
        cid = row["condition_id"]
        typ = str(row.get("activity_type") or "").upper()
        side = str(row.get("side") or "").upper()
        value = trade_value(row)
        size = float(row.get("size") or 0.0)

        if typ == "TRADE" and side == "BUY":
            rec = dict(row)
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
        rows.sort(key=lambda r: (r.get("activity_ts_ms") or 0, r.get("tx_hash") or "", r.get("outcome") or ""))

    market_rows: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    clips = collections.defaultdict(list)
    clip_prices = collections.defaultdict(list)

    for cid, rows in by_market.items():
        market = markets[cid]
        totals = {outcome: {"qty": 0.0, "cost": 0.0} for outcome in ("Up", "Down")}
        open_legs = {"Up": [], "Down": []}
        buy_idx = 0

        for row in rows:
            outcome = row.get("outcome") or ""
            if outcome not in totals:
                continue
            qty = float(row.get("size") or 0.0)
            price = float(row["exec_price"])
            value = trade_value(row)
            ts = int(row.get("activity_ts_ms") or 0)
            totals[outcome]["qty"] += qty
            totals[outcome]["cost"] += value
            buy_idx += 1
            if buy_idx <= 15:
                clips[buy_idx].append(qty)
                clip_prices[buy_idx].append(price)

            opposite = "Down" if outcome == "Up" else "Up"
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
                open_legs[outcome].append({"qty": remaining, "price": price, "ts": ts})

        pair_qty = min(totals["Up"]["qty"], totals["Down"]["qty"])
        residual_qty = abs(totals["Up"]["qty"] - totals["Down"]["qty"])
        pair_cost = None
        locked_pnl = None
        if pair_qty > 1e-9 and totals["Up"]["qty"] > 0.0 and totals["Down"]["qty"] > 0.0:
            pair_cost = totals["Up"]["cost"] / totals["Up"]["qty"] + totals["Down"]["cost"] / totals["Down"]["qty"]
            locked_pnl = (1.0 - pair_cost) * pair_qty
        cf = cash[cid]
        cash_pnl = cf["sell"] + cf["merge"] + cf["redeem"] - cf["buy"]
        market_rows.append(
            {
                "condition_id": cid,
                "slug": market["slug"],
                "pair_qty": pair_qty,
                "residual_qty": residual_qty,
                "pair_cost": pair_cost,
                "locked_pnl": locked_pnl,
                "cash_pnl": cash_pnl,
                "cash_roi": cash_pnl / cf["buy"] if cf["buy"] > 0.0 else None,
            }
        )

    episodes.sort(key=lambda row: (row["second_ts"], row["slug"]))
    total_episode_qty = sum(row["qty"] for row in episodes)

    return {
        "universe": {
            "valid_btc_5m_markets": len(markets),
            "xuan_activity_rows_unique": len(activity),
            "xuan_trade_markets": len(by_market),
            "xuan_trade_rows": len(trades),
        },
        "price_gap_exec_over_display": summary(price_gap),
        "market": {
            "pair_cost": summary([row["pair_cost"] for row in market_rows]),
            "residual_qty": summary([row["residual_qty"] for row in market_rows]),
            "clean_ratio_residual_le_1": sum(1 for row in market_rows if row["residual_qty"] <= 1.0) / len(market_rows)
            if market_rows
            else None,
            "locked_pnl_sum_proxy": sum(row["locked_pnl"] or 0.0 for row in market_rows),
            "cash": {
                "buy": sum(cash[cid]["buy"] for cid in by_market),
                "merge": sum(cash[cid]["merge"] for cid in by_market),
                "redeem": sum(cash[cid]["redeem"] for cid in by_market),
                "sell": sum(cash[cid]["sell"] for cid in by_market),
                "pnl": sum(row["cash_pnl"] for row in market_rows),
            },
            "cash_pnl": summary([row["cash_pnl"] for row in market_rows]),
            "cash_roi": summary([row["cash_roi"] for row in market_rows]),
        },
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
    activity = load_deduped_activity(args, markets)
    payload = {
        "data": {
            "replay_root": str(Path(args.replay_root)),
            "days": args.days,
            "trusted_start_ms": args.trusted_start_ms,
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
