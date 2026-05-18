#!/usr/bin/env python3
"""Replay clip-size sweep for the xuan taker-BUY pair-quality candidate.

This script is intended to run on the research EC2 host. It reads the published
replay SQLite files read-only and writes only to a caller-provided /tmp output
directory. It checkpoints one JSON file per day so a disconnected SSH session
does not lose completed work.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


sys.path.insert(0, "/tmp")
from build_taker_buy_signal_candidate_cache_compat import (  # noqa: E402
    OUTAGE_END_MS,
    OUTAGE_START_MS,
    TRUSTED_START_MS,
    book_at,
    completion_scan,
    high_side,
    latest_sweep,
    load_trigger_trades,
    load_day_trigger_trades,
    load_l1_by_second,
    load_l2_asks,
    other,
    ro_connect,
)


DEFAULT_DAYS = (
    "2026-04-27",
    "2026-04-28",
    "2026-04-29",
    "2026-04-30",
    "2026-05-01",
)
DEFAULT_CLIPS = (10.0, 20.0, 30.0, 45.0, 60.0, 75.0, 90.0, 120.0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-root", type=Path, default=Path("/mnt/poly-replay"))
    parser.add_argument("--days", default=",".join(DEFAULT_DAYS))
    parser.add_argument("--clips", default=",".join(str(int(c)) for c in DEFAULT_CLIPS))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--min-trade-price", type=float, default=0.45)
    parser.add_argument("--max-trade-price", type=float, default=0.75)
    parser.add_argument("--min-trade-size", type=float, default=10.0)
    parser.add_argument("--max-trade-size", type=float, default=160.0)
    parser.add_argument("--min-offset-s", type=int, default=0)
    parser.add_argument("--max-offset-s", type=int, default=240)
    parser.add_argument("--completion-s", type=int, default=30)
    parser.add_argument("--max-l2-age-ms", type=int, default=750)
    parser.add_argument("--l1-cap", type=float, default=0.98)
    parser.add_argument("--completion-ceiling", type=float, default=0.98)
    parser.add_argument("--max-entries", type=int, default=6)
    parser.add_argument("--max-residuals", type=int, default=2)
    parser.add_argument(
        "--l1-mode",
        choices=("second", "strict"),
        default="second",
        help="Use legacy per-second L1 lookup or strict latest recv_ms <= trade_ts_ms.",
    )
    parser.add_argument(
        "--trade-load-mode",
        choices=("day", "market"),
        default="day",
        help="Use day-level trigger scan or per-market indexed trigger queries.",
    )
    return parser.parse_args()


def load_l1_strict_index(
    conn: sqlite3.Connection,
    condition_id: str,
    start_ms: int,
    end_ms: int,
) -> tuple[list[int], list[dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT recv_ms, yes_bid_px, yes_ask_px, no_bid_px, no_ask_px,
               yes_bid_sz, yes_ask_sz, no_bid_sz, no_ask_sz
        FROM md_book_l1
        WHERE condition_id = ? AND recv_ms >= ? AND recv_ms <= ?
        ORDER BY recv_ms, capture_seq
        """,
        (condition_id, start_ms, end_ms),
    )
    times: list[int] = []
    books: list[dict[str, Any]] = []
    for row in rows:
        times.append(int(row["recv_ms"]))
        books.append(
            {
                "recv_ms": int(row["recv_ms"]),
                "YES": {
                    "bid": row["yes_bid_px"],
                    "ask": row["yes_ask_px"],
                    "bid_sz": row["yes_bid_sz"],
                    "ask_sz": row["yes_ask_sz"],
                },
                "NO": {
                    "bid": row["no_bid_px"],
                    "ask": row["no_ask_px"],
                    "bid_sz": row["no_bid_sz"],
                    "ask_sz": row["no_ask_sz"],
                },
            }
        )
    return times, books


def strict_book_at(l1_index: tuple[list[int], list[dict[str, Any]]], ts_ms: int) -> dict[str, Any] | None:
    times, books = l1_index
    idx = bisect.bisect_right(times, ts_ms) - 1
    if idx < 0:
        return None
    return books[idx]


def overlaps(a0: int, a1: int, b0: int, b1: int) -> bool:
    return a0 < b1 and b0 < a1


def load_markets_official(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT m.condition_id, m.slug, m.start_ms, m.end_ms, s.winner_side
        FROM market_meta m
        JOIN settlement_records s ON s.condition_id = m.condition_id
        WHERE m.symbol = 'BTC'
          AND m.interval_sec = 300
          AND upper(coalesce(s.winner_side, '')) IN ('YES', 'NO')
          AND coalesce(s.resolution_source, '') != 'inferred'
        ORDER BY m.start_ms
        """
    ).fetchall()
    out = []
    for row in rows:
        start_ms = int(row["start_ms"])
        end_ms = int(row["end_ms"])
        if end_ms <= TRUSTED_START_MS:
            continue
        if overlaps(start_ms, end_ms, OUTAGE_START_MS, OUTAGE_END_MS):
            continue
        out.append(row)
    return out


def metric_template(clip: float) -> dict[str, Any]:
    return {
        "clip": clip,
        "rows": 0,
        "closed": 0,
        "residuals": 0,
        "pnl": 0.0,
        "stress_100bps": 0.0,
        "worst_residual_pnl": 0.0,
        "worst_residual_100bps": 0.0,
        "first_cost": 0.0,
        "markets": set(),
        "residual_winners": 0,
        "residual_losers": 0,
        "completion_delays": [],
        "pair_costs": [],
        "first_vwaps": [],
        "pnl_seq": [],
        "gates": Counter(),
    }


def add_trade(
    metric: dict[str, Any],
    *,
    slug: str,
    ts_ms: int,
    first_vwap: float,
    hit: bool,
    pair_cost: float | None,
    delay_s: float | None,
    first_is_winner: bool,
) -> None:
    clip = float(metric["clip"])
    first_cost = clip * first_vwap
    if hit:
        assert pair_cost is not None
        pnl = clip * (1.0 - pair_cost)
        stress = pnl - 0.02 * clip
        worst = pnl
        worst_stress = stress
        closed = 1
        residual = 0
        if delay_s is not None:
            metric["completion_delays"].append(delay_s)
        metric["pair_costs"].append(pair_cost)
    else:
        if first_is_winner:
            pnl = clip * (1.0 - first_vwap)
            metric["residual_winners"] += 1
        else:
            pnl = -first_cost
            metric["residual_losers"] += 1
        stress = pnl - 0.01 * clip
        worst = -first_cost
        worst_stress = worst - 0.01 * clip
        closed = 0
        residual = 1

    metric["rows"] += 1
    metric["closed"] += closed
    metric["residuals"] += residual
    metric["pnl"] += pnl
    metric["stress_100bps"] += stress
    metric["worst_residual_pnl"] += worst
    metric["worst_residual_100bps"] += worst_stress
    metric["first_cost"] += first_cost
    metric["markets"].add(slug)
    metric["first_vwaps"].append(first_vwap)
    metric["pnl_seq"].append([ts_ms, pnl, stress, worst, worst_stress])


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    idx = min(len(xs) - 1, max(0, int(round((len(xs) - 1) * p))))
    return xs[idx]


def pct(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return 100.0 * numerator / denominator


def drawdown_from_seq(seq: list[list[float]], value_index: int) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for item in sorted(seq, key=lambda x: int(x[0])):
        equity += float(item[value_index])
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return max_drawdown


def finalize_metric(metric: dict[str, Any], market_total: int) -> dict[str, Any]:
    rows = int(metric["rows"])
    pnl_seq = list(metric["pnl_seq"])
    return {
        "clip": metric["clip"],
        "rows": rows,
        "markets": len(metric["markets"]),
        "market_total": market_total,
        "coverage_pct": pct(len(metric["markets"]), market_total),
        "pnl": metric["pnl"],
        "stress_100bps": metric["stress_100bps"],
        "worst_residual_pnl": metric["worst_residual_pnl"],
        "worst_residual_100bps": metric["worst_residual_100bps"],
        "closed_rate": pct(metric["closed"], rows),
        "residual_rate": pct(metric["residuals"], rows),
        "residuals": metric["residuals"],
        "residual_winners": metric["residual_winners"],
        "residual_losers": metric["residual_losers"],
        "first_cost": metric["first_cost"],
        "roi_first_cost_pct": pct(metric["pnl"], metric["first_cost"]),
        "stress_roi_first_cost_pct": pct(metric["stress_100bps"], metric["first_cost"]),
        "worst_stress_roi_first_cost_pct": pct(metric["worst_residual_100bps"], metric["first_cost"]),
        "avg_pnl_per_row": None if not rows else metric["pnl"] / rows,
        "avg_first_cost_per_row": None if not rows else metric["first_cost"] / rows,
        "max_drawdown": drawdown_from_seq(pnl_seq, 1),
        "stress_max_drawdown": drawdown_from_seq(pnl_seq, 2),
        "worst_residual_max_drawdown": drawdown_from_seq(pnl_seq, 3),
        "worst_residual_100bps_max_drawdown": drawdown_from_seq(pnl_seq, 4),
        "pair_cost_p50": percentile(metric["pair_costs"], 0.50),
        "pair_cost_p95": percentile(metric["pair_costs"], 0.95),
        "completion_delay_p50": percentile(metric["completion_delays"], 0.50),
        "completion_delay_p95": percentile(metric["completion_delays"], 0.95),
        "first_vwap_p50": percentile(metric["first_vwaps"], 0.50),
        "first_vwap_p95": percentile(metric["first_vwaps"], 0.95),
        "gates": dict(metric["gates"]),
        "pnl_seq": pnl_seq,
    }


def strip_sequences(summary: dict[str, Any]) -> dict[str, Any]:
    out = dict(summary)
    out.pop("pnl_seq", None)
    return out


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def run_day(day: str, clips: list[float], args: argparse.Namespace) -> dict[str, Any]:
    db_path = args.replay_root / day / "crypto_5m.sqlite"
    t0 = time.time()
    metrics = {clip: metric_template(clip) for clip in clips}
    with ro_connect(db_path) as conn:
        markets = load_markets_official(conn)
        trades_by_condition = load_day_trigger_trades(conn, args) if args.trade_load_mode == "day" else {}
        print(
            f"{day}: markets={len(markets)} trigger_markets={len(trades_by_condition)}",
            flush=True,
        )
        for idx, market in enumerate(markets, start=1):
            condition_id = str(market["condition_id"])
            slug = str(market["slug"])
            trades = (
                trades_by_condition.get(condition_id, [])
                if args.trade_load_mode == "day"
                else load_trigger_trades(conn, condition_id, market, args)
            )
            if not trades:
                continue
            start_ms = int(market["start_ms"])
            end_ms = int(market["end_ms"])
            if args.l1_mode == "strict":
                l1_books = load_l1_strict_index(
                    conn,
                    condition_id,
                    max(start_ms, TRUSTED_START_MS) - 2_000,
                    end_ms,
                )
            else:
                l1_books = load_l1_by_second(
                    conn,
                    condition_id,
                    max(start_ms, TRUSTED_START_MS) - 2_000,
                    end_ms,
                )
            l2_cache: dict[str, tuple[list[int], list[list[tuple[float, float]]]]] = {}
            states = {clip: {"entries": 0, "residuals": 0, "active_until": -1} for clip in clips}
            for trade in trades:
                ts_ms = int(trade["trade_ts_ms"])
                side = str(trade["market_side"])
                book = strict_book_at(l1_books, ts_ms) if args.l1_mode == "strict" else book_at(l1_books, ts_ms)
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
                first_is_winner = side == str(market["winner_side"])
                for clip in clips:
                    metric = metrics[clip]
                    state = states[clip]
                    if state["active_until"] >= ts_ms:
                        metric["gates"]["busy"] += 1
                        continue
                    if state["entries"] >= args.max_entries:
                        metric["gates"]["max_entries"] += 1
                        continue
                    first_vwap, _age_ms, _worst_px, _filled = latest_sweep(
                        first_times,
                        first_books,
                        ts_ms,
                        clip,
                        args.max_l2_age_ms,
                    )
                    if first_vwap is None:
                        metric["gates"]["first_l2_depth_or_age"] += 1
                        continue
                    if not (0.50 <= first_vwap <= 0.80):
                        metric["gates"]["vwap"] += 1
                        continue
                    if first_vwap + float(opp_ask) > args.l1_cap + 1e-9:
                        metric["gates"]["l1"] += 1
                        continue
                    completion = completion_scan(
                        opp_times,
                        opp_books,
                        ts_ms,
                        min(end_ms, ts_ms + args.completion_s * 1000),
                        first_vwap,
                        clip,
                    )
                    completion_prefix = f"ceil_{str(args.completion_ceiling).replace('.', '_')}"
                    hit = bool(completion[f"{completion_prefix}_hit"])
                    if not hit and state["residuals"] >= args.max_residuals:
                        metric["gates"]["max_residuals"] += 1
                        continue
                    if hit:
                        pair_cost = float(completion[f"{completion_prefix}_pair_cost"])
                        delay_s = float(completion[f"{completion_prefix}_delay_s"])
                        state["active_until"] = ts_ms + int(delay_s * 1000)
                    else:
                        pair_cost = None
                        delay_s = None
                        state["residuals"] += 1
                        state["active_until"] = ts_ms + args.completion_s * 1000
                    state["entries"] += 1
                    add_trade(
                        metric,
                        slug=slug,
                        ts_ms=ts_ms,
                        first_vwap=first_vwap,
                        hit=hit,
                        pair_cost=pair_cost,
                        delay_s=delay_s,
                        first_is_winner=first_is_winner,
                    )
            if idx % 25 == 0:
                print(f"{day}: processed={idx}/{len(markets)} elapsed={time.time() - t0:.1f}s", flush=True)

    summaries = {str(clip): finalize_metric(metrics[clip], len(markets)) for clip in clips}
    return {
        "day": day,
        "elapsed_sec": round(time.time() - t0, 3),
        "market_total": len(markets),
        "clips": clips,
        "summaries": summaries,
    }


def combine_days(day_results: list[dict[str, Any]], clips: list[float]) -> dict[str, Any]:
    market_total = sum(int(result["market_total"]) for result in day_results)
    combined = []
    for clip in clips:
        key = str(clip)
        merged = metric_template(clip)
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
            merged["completion_delays"].extend(summary["completion_delay_p50"] for _ in [])
            merged["pnl_seq"].extend(summary["pnl_seq"])
            merged["markets"].update(f"{result['day']}:{i}" for i in range(summary["markets"]))
            merged["gates"].update(summary["gates"])
        combined.append(finalize_metric(merged, market_total))
    return {
        "scope": "BTC 5m pair-quality v3 clip-size sweep, official settled only, read-only /mnt/poly-replay",
        "days": [result["day"] for result in day_results],
        "market_total": market_total,
        "clips": clips,
        "summary": combined,
        "daily": {result["day"]: {k: strip_sequences(v) for k, v in result["summaries"].items()} for result in day_results},
    }


def main() -> int:
    args = parse_args()
    days = [part.strip() for part in args.days.split(",") if part.strip()]
    clips = [float(part.strip()) for part in args.clips.split(",") if part.strip()]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    day_results = []
    for day in days:
        day_path = args.output_dir / f"day_{day}.json"
        if day_path.exists() and not args.force:
            print(f"{day}: reuse {day_path}", flush=True)
            day_results.append(json.loads(day_path.read_text(encoding="utf-8")))
            continue
        result = run_day(day, clips, args)
        write_json(day_path, result)
        print(f"{day}: wrote {day_path} elapsed={result['elapsed_sec']}s", flush=True)
        day_results.append(result)
        combined = combine_days(day_results, clips)
        write_json(args.output_dir / "combined_partial.json", combined)
    combined = combine_days(day_results, clips)
    write_json(args.output_dir / "combined.json", combined)
    print(json.dumps({k: v for k, v in combined.items() if k != "daily"}, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
