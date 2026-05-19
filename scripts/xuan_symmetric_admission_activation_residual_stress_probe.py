#!/usr/bin/env python3
"""Past-only activation stress probe for symmetric admission.

This verifier tests whether requiring recent opposite-side observable strict
flow before placing a maker quote can reduce unmatched first-leg exposure. It
uses only manifest-discovered local strict/cache data and does not touch
raw/replay/collector or services.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import xuan_symmetric_admission_residual_stress_probe as base


ARTIFACT = "xuan_symmetric_admission_activation_residual_stress_probe"


def parse_int_list(raw: str) -> list[int]:
    return [int(item.strip()) for item in raw.split(",") if item.strip()]


def expire_history(history: dict[str, list[int]], cutoff: int) -> None:
    for side in ("YES", "NO"):
        history[side] = [ts for ts in history[side] if ts >= cutoff]


def run_probe(
    rows: list[tuple[Any, ...]],
    edge: float,
    window_s: float,
    leak_rate: float,
    activation_window_s: float,
    min_opp_count: int,
    args: argparse.Namespace,
) -> dict[str, Any]:
    pending: dict[str, dict[str, list[tuple[int, float, float, str, str]]]] = {}
    histories: dict[str, dict[str, list[int]]] = {}
    counters: dict[str, float] = defaultdict(float)
    daily: dict[str, dict[str, float]] = {}
    active_markets: set[str] = set()
    window_ms = int(window_s * 1000)
    activation_window_ms = int(activation_window_s * 1000)

    for day, condition_id, ts_ms, side, public_px, public_size, winner_side, _pair_now in rows:
        px = float(public_px)
        size = float(public_size or 0.0)
        if not math.isfinite(px) or size <= 0:
            continue
        seed_px = max(float(args.min_tick), px - edge)
        seed_qty = min(float(args.clip), size * float(args.fill_haircut))
        if seed_qty <= 0:
            continue

        condition = str(condition_id)
        day_text = str(day)
        side_text = str(side)
        winner_text = str(winner_side or "")
        opp = "NO" if side_text == "YES" else "YES"
        book = pending.setdefault(condition, {"YES": [], "NO": []})
        history = histories.setdefault(condition, {"YES": [], "NO": []})
        base.expire_pending(book, int(ts_ms) - window_ms, leak_rate, daily, counters)
        expire_history(history, int(ts_ms) - activation_window_ms)

        counters["candidate_rows_seen"] += 1
        recent_opp_count = len(history[opp])
        active = recent_opp_count >= min_opp_count
        if not active:
            counters["activation_skipped"] += 1
            base.add_daily(daily, day_text, "activation_skipped", 1.0)
            history[side_text].append(int(ts_ms))
            continue

        counters["activation_passed"] += 1
        left = seed_qty
        while left > 1e-9 and book[opp]:
            opp_ts, opp_px, opp_qty, opp_day, _opp_winner = book[opp][0]
            qty = min(left, opp_qty)
            cost = seed_px + opp_px
            counters["pair_qty"] += qty
            counters["pair_cost"] += qty * cost
            counters["pair_gross_pnl"] += qty * (1.0 - cost)
            counters["pair_fee"] += qty * cost * float(args.pair_fee_rate)
            counters["pair_actions"] += 1
            active_markets.add(condition)
            base.add_daily(daily, day_text, "pair_actions", 1.0)
            base.add_daily(daily, day_text, "pair_qty", qty)
            base.add_daily(daily, day_text, "pair_gross_pnl", qty * (1.0 - cost))
            base.add_daily(daily, day_text, "pair_fee", qty * cost * float(args.pair_fee_rate))
            left -= qty
            opp_qty -= qty
            if opp_qty <= 1e-9:
                book[opp].pop(0)
            else:
                book[opp][0] = (opp_ts, opp_px, opp_qty, opp_day, _opp_winner)
        if left > 1e-9:
            book[side_text].append((int(ts_ms), seed_px, left, day_text, winner_text))
            counters["pending_opened_qty"] += left
        history[side_text].append(int(ts_ms))

    for book in pending.values():
        base.expire_pending(book, 10**18, leak_rate, daily, counters)

    pair_qty = counters["pair_qty"]
    residual_qty = counters["residual_qty"]
    gross_buy_qty = pair_qty * 2.0 + residual_qty
    residual_fee = counters["residual_cost"] * float(args.residual_fee_rate)
    worst_total = (
        counters["pair_gross_pnl"]
        - counters["pair_fee"]
        - counters["residual_cost"]
        - residual_fee
    )
    actual_total = (
        counters["pair_gross_pnl"]
        - counters["pair_fee"]
        + counters["actual_residual_settle"]
        - counters["residual_cost"]
        - residual_fee
    )

    daily_rows = []
    for day, item in sorted(daily.items()):
        pair_net = item.get("pair_gross_pnl", 0.0) - item.get("pair_fee", 0.0)
        residual_cost = item.get("residual_cost", 0.0)
        residual_fee_day = residual_cost * float(args.residual_fee_rate)
        worst = pair_net - residual_cost - residual_fee_day
        actual = pair_net + item.get("actual_residual_settle", 0.0) - residual_cost - residual_fee_day
        daily_rows.append(
            {
                "day": day,
                "pair_actions": int(item.get("pair_actions", 0.0)),
                "pair_qty": round(item.get("pair_qty", 0.0), 6),
                "residual_qty": round(item.get("residual_qty", 0.0), 6),
                "activation_skipped": int(item.get("activation_skipped", 0.0)),
                "worst_net_fee_after": round(worst, 6),
                "actual_settle_net_fee_after": round(actual, 6),
            }
        )

    return {
        "edge": edge,
        "window_s": window_s,
        "leak_rate": leak_rate,
        "activation_window_s": activation_window_s,
        "min_opp_count": min_opp_count,
        "candidate_rows_seen": int(counters["candidate_rows_seen"]),
        "activation_passed": int(counters["activation_passed"]),
        "activation_skipped": int(counters["activation_skipped"]),
        "activation_rate": round(
            counters["activation_passed"] / counters["candidate_rows_seen"]
            if counters["candidate_rows_seen"]
            else 0.0,
            6,
        ),
        "pair_actions": int(counters["pair_actions"]),
        "active_markets": len(active_markets),
        "pair_qty": round(pair_qty, 6),
        "pending_opened_qty": round(counters["pending_opened_qty"], 6),
        "residual_qty": round(residual_qty, 6),
        "residual_cost": round(counters["residual_cost"], 6),
        "residual_qty_rate": round(residual_qty / gross_buy_qty, 6) if gross_buy_qty else 0.0,
        "weighted_pair_cost": round(counters["pair_cost"] / pair_qty, 6) if pair_qty else None,
        "pair_gross_pnl": round(counters["pair_gross_pnl"], 6),
        "pair_fee": round(counters["pair_fee"], 6),
        "residual_fee": round(residual_fee, 6),
        "worst_net_fee_after": round(worst_total, 6),
        "actual_settle_net_fee_after": round(actual_total, 6),
        "worst_day_worst_net_fee_after": round(
            min((row["worst_net_fee_after"] for row in daily_rows), default=0.0),
            6,
        ),
        "worst_day_actual_settle_net_fee_after": round(
            min((row["actual_settle_net_fee_after"] for row in daily_rows), default=0.0),
            6,
        ),
        "daily": daily_rows,
    }


def qualified_rows(results: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    by_key: dict[tuple[float, float, float, int], dict[str, dict[str, Any]]] = {}
    for row in results:
        key = (
            float(row["edge"]),
            float(row["leak_rate"]),
            float(row["activation_window_s"]),
            int(row["min_opp_count"]),
        )
        by_key.setdefault(key, {})[str(row["bucket"])] = row
    out: list[dict[str, Any]] = []
    for (edge, leak_rate, activation_window_s, min_opp_count), buckets in sorted(by_key.items()):
        covered = buckets.get("covered")
        holdout = buckets.get("holdout")
        if not covered or not holdout:
            continue
        if int(covered["pair_actions"]) < args.min_covered_pair_actions:
            continue
        if int(holdout["pair_actions"]) < args.min_holdout_pair_actions:
            continue
        if float(covered["residual_qty_rate"]) > args.max_residual_qty_rate:
            continue
        if float(holdout["residual_qty_rate"]) > args.max_residual_qty_rate:
            continue
        if float(covered["worst_net_fee_after"]) <= 0 or float(holdout["worst_net_fee_after"]) <= 0:
            continue
        if (
            float(covered["worst_day_worst_net_fee_after"]) <= 0
            or float(holdout["worst_day_worst_net_fee_after"]) <= 0
        ):
            continue
        out.append(
            {
                "edge": edge,
                "leak_rate": leak_rate,
                "activation_window_s": activation_window_s,
                "min_opp_count": min_opp_count,
                "covered_pair_actions": int(covered["pair_actions"]),
                "holdout_pair_actions": int(holdout["pair_actions"]),
                "covered_residual_qty_rate": covered["residual_qty_rate"],
                "holdout_residual_qty_rate": holdout["residual_qty_rate"],
                "covered_worst_net_fee_after": covered["worst_net_fee_after"],
                "holdout_worst_net_fee_after": holdout["worst_net_fee_after"],
                "covered_worst_day": covered["worst_day_worst_net_fee_after"],
                "holdout_worst_day": holdout["worst_day_worst_net_fee_after"],
            }
        )
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=str(base.DEFAULT_ROOT))
    parser.add_argument("--output-dir")
    parser.add_argument("--edges", default="0.06,0.065,0.07")
    parser.add_argument("--leak-rates", default="0.005,0.01")
    parser.add_argument("--window-s", type=float, default=10.0)
    parser.add_argument("--activation-window-s", default="10,20,30")
    parser.add_argument("--min-opp-counts", default="1,2")
    parser.add_argument("--clip", type=float, default=5.0)
    parser.add_argument("--fill-haircut", type=float, default=0.25)
    parser.add_argument("--pair-fee-rate", type=float, default=0.0283)
    parser.add_argument("--residual-fee-rate", type=float, default=0.0283)
    parser.add_argument("--price-min", type=float, default=0.05)
    parser.add_argument("--price-max", type=float, default=0.90)
    parser.add_argument("--offset-min-s", type=float, default=0.0)
    parser.add_argument("--offset-max-s", type=float, default=300.0)
    parser.add_argument("--strict-l1-pair-cap", type=float, default=1.02)
    parser.add_argument("--min-tick", type=float, default=0.01)
    parser.add_argument("--max-residual-qty-rate", type=float, default=0.05)
    parser.add_argument("--min-covered-pair-actions", type=int, default=1000)
    parser.add_argument("--min-holdout-pair-actions", type=int, default=200)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.perf_counter()
    data_root = Path(args.data_root).expanduser().resolve()
    strict_root = data_root / "backtest_cache" / "taker_buy_signal_core_v2_strict_l1"
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{base.utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    declarations: dict[str, Any] = {}
    for bucket in ("covered", "holdout"):
        inputs = base.discover_strict_inputs(strict_root, bucket)
        rows, source_rows = base.load_rows(inputs, args)
        declarations[bucket] = {
            "labels": [str(item["label"]) for item in inputs],
            "days": sorted({day for item in inputs for day in item["days"]}),
            "source_row_count": source_rows,
            "filtered_row_count": len(rows),
        }
        for edge in base.parse_float_list(args.edges):
            for leak_rate in base.parse_float_list(args.leak_rates):
                for activation_window_s in base.parse_float_list(args.activation_window_s):
                    for min_opp_count in parse_int_list(args.min_opp_counts):
                        results.append(
                            {
                                "bucket": bucket,
                                **run_probe(
                                    rows,
                                    edge,
                                    float(args.window_s),
                                    leak_rate,
                                    activation_window_s,
                                    min_opp_count,
                                    args,
                                ),
                            }
                        )

    qualified = qualified_rows(results, args)
    status = "UNKNOWN_ACTIVATION_REPAIR_LEAD" if qualified else "DISCARD_ACTIVATION_DOES_NOT_FIX_LEAKAGE"
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": status,
        "hypothesis": "past-only opposite-side activation can reduce unmatched first-leg exposure enough to tolerate leakage",
        "data_root": str(data_root),
        "dataset_type": "strict_cache_symmetric_admission_activation_residual_stress_probe",
        "labels": {bucket: item["labels"] for bucket, item in declarations.items()},
        "days": {bucket: item["days"] for bucket, item in declarations.items()},
        "market_prefix": "btc-updown-5m-",
        "assets": ["BTC"],
        "row_count": sum(int(item["source_row_count"]) for item in declarations.values()),
        "filtered_row_count": sum(int(item["filtered_row_count"]) for item in declarations.values()),
        "excluded_20260514_20260515": True,
        "public_account_execution_truth_v1_included": False,
        "raw_replay_scanned": False,
        "collector_scanned": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "can_support_strategy_promotion": False,
        "assumption": "candidate is quoted only if opposite-side strict flow was observed in the past activation window; expired unmatched quotes leak at leak_rate",
        "config": {
            "edges": base.parse_float_list(args.edges),
            "leak_rates": base.parse_float_list(args.leak_rates),
            "window_s": args.window_s,
            "activation_window_s": base.parse_float_list(args.activation_window_s),
            "min_opp_counts": parse_int_list(args.min_opp_counts),
            "clip": args.clip,
            "fill_haircut": args.fill_haircut,
            "pair_fee_rate": args.pair_fee_rate,
            "residual_fee_rate": args.residual_fee_rate,
            "strict_l1_pair_cap": args.strict_l1_pair_cap,
            "max_residual_qty_rate": args.max_residual_qty_rate,
        },
        "qualified": qualified,
        "next_action": (
            "if UNKNOWN, review implementability and source truth; if DISCARD, do not use past-only activation as leakage repair"
        ),
        "results": results,
        "elapsed_s": round(time.perf_counter() - started, 3),
        "outputs": {"results_jsonl": "results.jsonl"},
    }
    base.write_json(output_dir / "manifest.json", manifest)
    (output_dir / "results.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in results))
    print(output_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
