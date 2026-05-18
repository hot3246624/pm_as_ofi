#!/usr/bin/env python3
"""Strict-L1 l1-cap sweep for the xuan taker-BUY pair-quality candidate.

This is a parity audit script for the shadow/replay gap. It intentionally uses
the same replay data and pair-quality rules as the clip sweep, but evaluates L1
with exact latest recv_ms <= trade_ts_ms instead of the legacy per-second L1
lookup.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any


sys.path.insert(0, "/tmp")
from xuan_clip_size_sweep_strict_l1 import (  # noqa: E402
    TRUSTED_START_MS,
    add_trade,
    completion_scan,
    finalize_metric,
    high_side,
    latest_sweep,
    load_l1_strict_index,
    load_l2_asks,
    load_markets_official,
    load_trigger_trades,
    metric_template,
    other,
    ro_connect,
    strict_book_at,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-root", type=Path, default=Path("/mnt/poly-replay"))
    parser.add_argument("--days", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--clip", type=float, default=60.0)
    parser.add_argument("--l1-caps", default="0.97,0.98,0.99,1.0,1.01,1.02,1.03,1.05")
    parser.add_argument("--min-trade-price", type=float, default=0.45)
    parser.add_argument("--max-trade-price", type=float, default=0.75)
    parser.add_argument("--min-trade-size", type=float, default=10.0)
    parser.add_argument("--max-trade-size", type=float, default=160.0)
    parser.add_argument("--min-offset-s", type=int, default=0)
    parser.add_argument("--max-offset-s", type=int, default=240)
    parser.add_argument("--completion-s", type=int, default=30)
    parser.add_argument("--max-l2-age-ms", type=int, default=750)
    parser.add_argument("--completion-ceiling", type=float, default=0.98)
    parser.add_argument("--max-entries", type=int, default=6)
    parser.add_argument("--max-residuals", type=int, default=1)
    return parser.parse_args()


def ceiling_prefix(value: float) -> str:
    return f"ceil_{str(value).replace('.', '_')}"


def run_day(day: str, caps: list[float], args: argparse.Namespace) -> dict[str, Any]:
    db_path = args.replay_root / day / "crypto_5m.sqlite"
    t0 = time.time()
    metrics = {cap: metric_template(args.clip) for cap in caps}
    with ro_connect(db_path) as conn:
        markets = load_markets_official(conn)
        print(f"{day}: markets={len(markets)} strict_l1_cap_sweep", flush=True)
        for idx, market in enumerate(markets, start=1):
            condition_id = str(market["condition_id"])
            trades = load_trigger_trades(conn, condition_id, market, args)
            if not trades:
                continue
            start_ms = int(market["start_ms"])
            end_ms = int(market["end_ms"])
            l1_index = load_l1_strict_index(conn, condition_id, max(start_ms, TRUSTED_START_MS) - 2_000, end_ms)
            l2_cache: dict[str, tuple[list[int], list[list[tuple[float, float]]]]] = {}
            states = {cap: {"entries": 0, "residuals": 0, "active_until": -1} for cap in caps}
            for trade in trades:
                ts_ms = int(trade["trade_ts_ms"])
                side = str(trade["market_side"])
                book = strict_book_at(l1_index, ts_ms)
                if book is None:
                    for metric in metrics.values():
                        metric["gates"]["no_l1"] += 1
                    continue
                high = high_side(book)
                if high is None or side != high:
                    for metric in metrics.values():
                        metric["gates"]["not_high"] += 1
                    continue
                opp = other(side)
                opp_ask = book[opp]["ask"]
                if opp_ask is None:
                    for metric in metrics.values():
                        metric["gates"]["no_opp_l1"] += 1
                    continue
                if side not in l2_cache:
                    l2_cache[side] = load_l2_asks(
                        conn,
                        condition_id,
                        side,
                        max(start_ms, TRUSTED_START_MS),
                        min(end_ms, start_ms + (args.max_offset_s + args.completion_s) * 1000),
                    )
                if opp not in l2_cache:
                    l2_cache[opp] = load_l2_asks(
                        conn,
                        condition_id,
                        opp,
                        max(start_ms, TRUSTED_START_MS),
                        min(end_ms, start_ms + (args.max_offset_s + args.completion_s) * 1000),
                    )
                first_times, first_books = l2_cache[side]
                opp_times, opp_books = l2_cache[opp]
                first_vwap, _age_ms, _worst_px, _filled = latest_sweep(
                    first_times,
                    first_books,
                    ts_ms,
                    args.clip,
                    args.max_l2_age_ms,
                )
                if first_vwap is None:
                    for metric in metrics.values():
                        metric["gates"]["first_l2_depth_or_age"] += 1
                    continue
                if not (0.50 <= first_vwap <= 0.80):
                    for metric in metrics.values():
                        metric["gates"]["vwap"] += 1
                    continue

                completion: dict[str, Any] | None = None
                first_is_winner = side == str(market["winner_side"])
                immediate_pair = first_vwap + float(opp_ask)
                for cap in caps:
                    metric = metrics[cap]
                    state = states[cap]
                    if state["active_until"] >= ts_ms:
                        metric["gates"]["busy"] += 1
                        continue
                    if state["entries"] >= args.max_entries:
                        metric["gates"]["max_entries"] += 1
                        continue
                    if immediate_pair > cap + 1e-9:
                        metric["gates"]["l1"] += 1
                        continue
                    if completion is None:
                        completion = completion_scan(
                            opp_times,
                            opp_books,
                            ts_ms,
                            min(end_ms, ts_ms + args.completion_s * 1000),
                            first_vwap,
                            args.clip,
                        )
                    prefix = ceiling_prefix(args.completion_ceiling)
                    hit = bool(completion[f"{prefix}_hit"])
                    if not hit and state["residuals"] >= args.max_residuals:
                        metric["gates"]["max_residuals"] += 1
                        continue
                    if hit:
                        pair_cost = float(completion[f"{prefix}_pair_cost"])
                        delay_s = float(completion[f"{prefix}_delay_s"])
                        state["active_until"] = ts_ms + int(delay_s * 1000)
                    else:
                        pair_cost = None
                        delay_s = None
                        state["residuals"] += 1
                        state["active_until"] = ts_ms + args.completion_s * 1000
                    state["entries"] += 1
                    add_trade(
                        metric,
                        slug=str(market["slug"]),
                        ts_ms=ts_ms,
                        first_vwap=first_vwap,
                        hit=hit,
                        pair_cost=pair_cost,
                        delay_s=delay_s,
                        first_is_winner=first_is_winner,
                    )
            if idx % 25 == 0:
                print(f"{day}: processed={idx}/{len(markets)} elapsed={time.time() - t0:.1f}s", flush=True)

    return {
        "day": day,
        "elapsed_sec": round(time.time() - t0, 3),
        "market_total": len(markets),
        "summaries": {str(cap): finalize_with_cap(metrics[cap], len(markets), cap) for cap in caps},
    }


def finalize_with_cap(metric: dict[str, Any], market_total: int, cap: float) -> dict[str, Any]:
    out = finalize_metric(metric, market_total)
    out["l1_cap"] = cap
    return out


def combine_days(day_results: list[dict[str, Any]], caps: list[float], clip: float) -> dict[str, Any]:
    market_total = sum(int(result["market_total"]) for result in day_results)
    combined = []
    for cap in caps:
        merged = metric_template(clip)
        key = str(cap)
        for result in day_results:
            summary = result["summaries"][key]
            merged["rows"] += summary["rows"]
            merged["closed"] += round(summary["rows"] * (summary["closed_rate"] or 0.0) / 100.0)
            merged["residuals"] += summary["residuals"]
            merged["pnl"] += summary["pnl"]
            merged["stress_100bps"] += summary["stress_100bps"]
            merged["worst_residual_pnl"] += summary["worst_residual_pnl"]
            merged["worst_residual_100bps"] += summary["worst_residual_100bps"]
            merged["first_cost"] += summary["first_cost"]
            merged["residual_winners"] += summary["residual_winners"]
            merged["residual_losers"] += summary["residual_losers"]
            merged["pnl_seq"].extend(summary["pnl_seq"])
            merged["markets"].update(f"{result['day']}:{i}" for i in range(summary["markets"]))
            merged["gates"].update(summary["gates"])
        combined.append(finalize_with_cap(merged, market_total, cap))
    return {
        "scope": "BTC 5m pair-quality strict-L1 l1-cap sweep, official settled only, read-only /mnt/poly-replay",
        "days": [result["day"] for result in day_results],
        "market_total": market_total,
        "clip": clip,
        "summary": combined,
        "daily": {
            result["day"]: {
                cap: {k: v for k, v in summary.items() if k != "pnl_seq"}
                for cap, summary in result["summaries"].items()
            }
            for result in day_results
        },
    }


def main() -> int:
    args = parse_args()
    days = [part.strip() for part in args.days.split(",") if part.strip()]
    caps = [float(part.strip()) for part in args.l1_caps.split(",") if part.strip()]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    day_results = []
    for day in days:
        day_path = args.output_dir / f"day_{day}.json"
        if day_path.exists() and not args.force:
            print(f"{day}: reuse {day_path}", flush=True)
            result = json.loads(day_path.read_text(encoding="utf-8"))
        else:
            result = run_day(day, caps, args)
            write_json(day_path, result)
            print(f"{day}: wrote {day_path} elapsed={result['elapsed_sec']}s", flush=True)
        day_results.append(result)
        write_json(args.output_dir / "combined_partial.json", combine_days(day_results, caps, args.clip))
    combined = combine_days(day_results, caps, args.clip)
    write_json(args.output_dir / "combined.json", combined)
    print(json.dumps({k: v for k, v in combined.items() if k != "daily"}, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
