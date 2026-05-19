#!/usr/bin/env python3
"""Stress symmetric admission with imperfect cancel / first-leg leak.

The earlier symmetric-admission probe pairs only when the opposite-side strict
candidate appears in a short window, so residual is zero by construction. This
follow-up keeps the same candidate-stable admission rule, but treats expired
unpaired first-leg quotes as partially filled residual inventory. It is
research-only and reads only local strict/cache manifests.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import duckdb
except ModuleNotFoundError as exc:  # pragma: no cover - operational guard
    raise SystemExit("duckdb is required. Run with `uv run --with duckdb python ...`.") from exc


ARTIFACT = "xuan_symmetric_admission_residual_stress_probe"
DEFAULT_ROOT = Path(os.environ.get("POLY_BT_ROOT", "/Users/hot/web3Scientist/poly_backtest_data"))
BLOCKED_DAYS = {"2026-05-14", "2026-05-15"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_float_list(raw: str) -> list[float]:
    return [float(item.strip()) for item in raw.split(",") if item.strip()]


def label_days(label: str, manifest: dict[str, Any]) -> list[str]:
    days = manifest.get("days")
    if days:
        return [str(day) for day in days]
    if "_" not in label:
        return [f"{label[:4]}-{label[4:6]}-{label[6:8]}"]
    start, end = label.split("_", 1)
    return [f"{start[:4]}-{start[4:6]}-{start[6:8]}", f"{end[:4]}-{end[4:6]}-{end[6:8]}"]


def discover_strict_inputs(strict_root: Path, bucket: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for manifest_path in sorted(strict_root.glob("*/CACHE_MANIFEST.json")):
        label = manifest_path.parent.name
        manifest = read_json(manifest_path)
        days = label_days(label, manifest)
        if any(day in BLOCKED_DAYS for day in days):
            continue
        if bucket == "covered" and not all("2026-05-02" <= day <= "2026-05-13" for day in days):
            continue
        if bucket == "holdout" and not any(day in {"2026-05-16", "2026-05-17", "2026-05-18"} for day in days):
            continue
        db_path = manifest_path.parent / str(manifest.get("outputs", {}).get("duckdb", "cache.duckdb"))
        if db_path.is_file():
            out.append({"label": label, "days": days, "duckdb": db_path})
    return out


def load_rows(inputs: list[dict[str, Any]], args: argparse.Namespace) -> tuple[list[tuple[Any, ...]], int]:
    rows: list[tuple[Any, ...]] = []
    source_rows = 0
    for item in inputs:
        con = duckdb.connect(str(item["duckdb"]), read_only=True)
        source_rows += int(con.execute("SELECT COUNT(*) FROM taker_buy_signal_candidates").fetchone()[0])
        rows.extend(
            con.execute(
                """
                SELECT
                  day,
                  condition_id,
                  trigger_ts_ms,
                  first_side,
                  public_trade_price,
                  public_trade_size,
                  winner_side,
                  strict_l1_immediate_pair
                FROM taker_buy_signal_candidates
                WHERE first_side IN ('YES', 'NO')
                  AND public_trade_price BETWEEN ? AND ?
                  AND offset_s BETWEEN ? AND ?
                  AND strict_l1_immediate_pair <= ?
                ORDER BY condition_id, trigger_ts_ms
                """,
                [
                    float(args.price_min),
                    float(args.price_max),
                    float(args.offset_min_s),
                    float(args.offset_max_s),
                    float(args.strict_l1_pair_cap),
                ],
            ).fetchall()
        )
        con.close()
    return rows, source_rows


def add_daily(daily: dict[str, dict[str, float]], day: str, key: str, value: float) -> None:
    daily.setdefault(day, defaultdict(float))[key] += value


def expire_pending(
    book: dict[str, list[tuple[int, float, float, str, str]]],
    cutoff: int,
    leak_rate: float,
    daily: dict[str, dict[str, float]],
    counters: dict[str, float],
) -> None:
    for side in ("YES", "NO"):
        keep: list[tuple[int, float, float, str, str]] = []
        for ts_ms, px, qty, day, winner_side in book[side]:
            if ts_ms >= cutoff:
                keep.append((ts_ms, px, qty, day, winner_side))
                continue
            leaked_qty = qty * leak_rate
            if leaked_qty > 1e-12:
                leaked_cost = leaked_qty * px
                counters["residual_qty"] += leaked_qty
                counters["residual_cost"] += leaked_cost
                counters["residual_actions"] += 1
                if side == winner_side:
                    counters["actual_residual_settle"] += leaked_qty
                    add_daily(daily, day, "actual_residual_settle", leaked_qty)
                add_daily(daily, day, "residual_qty", leaked_qty)
                add_daily(daily, day, "residual_cost", leaked_cost)
        book[side] = keep


def run_probe(
    rows: list[tuple[Any, ...]],
    edge: float,
    window_s: float,
    leak_rate: float,
    args: argparse.Namespace,
) -> dict[str, Any]:
    pending: dict[str, dict[str, list[tuple[int, float, float, str, str]]]] = {}
    counters: dict[str, float] = defaultdict(float)
    daily: dict[str, dict[str, float]] = {}
    active_markets: set[str] = set()
    window_ms = int(window_s * 1000)

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
        expire_pending(book, int(ts_ms) - window_ms, leak_rate, daily, counters)

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
            add_daily(daily, day_text, "pair_actions", 1.0)
            add_daily(daily, day_text, "pair_qty", qty)
            add_daily(daily, day_text, "pair_gross_pnl", qty * (1.0 - cost))
            add_daily(daily, day_text, "pair_fee", qty * cost * float(args.pair_fee_rate))
            left -= qty
            opp_qty -= qty
            if opp_qty <= 1e-9:
                book[opp].pop(0)
            else:
                book[opp][0] = (opp_ts, opp_px, opp_qty, opp_day, _opp_winner)
        if left > 1e-9:
            book[side_text].append((int(ts_ms), seed_px, left, day_text, winner_text))

    for book in pending.values():
        expire_pending(book, 10**18, leak_rate, daily, counters)

    pair_qty = counters["pair_qty"]
    residual_qty = counters["residual_qty"]
    gross_buy_qty = pair_qty * 2.0 + residual_qty
    residual_fee = counters["residual_cost"] * float(args.residual_fee_rate)
    net_fee_after_worst = (
        counters["pair_gross_pnl"]
        - counters["pair_fee"]
        - counters["residual_cost"]
        - residual_fee
    )
    actual_settle_pnl = (
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
                "residual_cost": round(residual_cost, 6),
                "worst_net_fee_after": round(worst, 6),
                "actual_settle_net_fee_after": round(actual, 6),
            }
        )

    return {
        "edge": edge,
        "window_s": window_s,
        "leak_rate": leak_rate,
        "pair_actions": int(counters["pair_actions"]),
        "residual_actions": int(counters["residual_actions"]),
        "active_markets": len(active_markets),
        "day_count": len(daily_rows),
        "pair_qty": round(pair_qty, 6),
        "residual_qty": round(residual_qty, 6),
        "residual_cost": round(counters["residual_cost"], 6),
        "residual_qty_rate": round(residual_qty / gross_buy_qty, 6) if gross_buy_qty else 0.0,
        "weighted_pair_cost": round(counters["pair_cost"] / pair_qty, 6) if pair_qty else None,
        "pair_gross_pnl": round(counters["pair_gross_pnl"], 6),
        "pair_fee": round(counters["pair_fee"], 6),
        "residual_fee": round(residual_fee, 6),
        "worst_net_fee_after": round(net_fee_after_worst, 6),
        "actual_settle_net_fee_after": round(actual_settle_pnl, 6),
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=str(DEFAULT_ROOT))
    parser.add_argument("--output-dir")
    parser.add_argument("--edges", default="0.055,0.06,0.065")
    parser.add_argument("--leak-rates", default="0.01,0.03,0.05")
    parser.add_argument("--window-s", type=float, default=10.0)
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


def qualified_rows(results: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    by_key: dict[tuple[float, float], dict[str, dict[str, Any]]] = {}
    for row in results:
        key = (float(row["edge"]), float(row["leak_rate"]))
        by_key.setdefault(key, {})[str(row["bucket"])] = row
    out: list[dict[str, Any]] = []
    for (edge, leak_rate), buckets in sorted(by_key.items()):
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


def main() -> int:
    args = parse_args()
    started = time.perf_counter()
    data_root = Path(args.data_root).expanduser().resolve()
    strict_root = data_root / "backtest_cache" / "taker_buy_signal_core_v2_strict_l1"
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)

    declarations: dict[str, Any] = {}
    results: list[dict[str, Any]] = []
    for bucket in ("covered", "holdout"):
        inputs = discover_strict_inputs(strict_root, bucket)
        rows, source_rows = load_rows(inputs, args)
        declarations[bucket] = {
            "labels": [str(item["label"]) for item in inputs],
            "days": sorted({day for item in inputs for day in item["days"]}),
            "source_row_count": source_rows,
            "filtered_row_count": len(rows),
        }
        for edge in parse_float_list(args.edges):
            for leak_rate in parse_float_list(args.leak_rates):
                results.append(
                    {
                        "bucket": bucket,
                        **run_probe(rows, edge, float(args.window_s), leak_rate, args),
                    }
                )

    qualified = qualified_rows(results, args)
    if qualified:
        status = "UNKNOWN_RESIDUAL_TOLERANT_MECHANISM_CLUE"
        next_action = "implementability review for maker cancel/fill leakage and queue evidence"
    else:
        status = "DISCARD_RESIDUAL_FRAGILE"
        next_action = "do not shadow until maker-fill leakage can be proven below the failing threshold"

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": status,
        "data_root": str(data_root),
        "dataset_type": "strict_cache_symmetric_admission_residual_stress_probe",
        "labels": {bucket: item["labels"] for bucket, item in declarations.items()},
        "days": {bucket: item["days"] for bucket, item in declarations.items()},
        "market_prefix": "btc-updown-5m-",
        "assets": ["BTC"],
        "row_count": sum(int(item["source_row_count"]) for item in declarations.values()),
        "filtered_row_count": sum(int(item["filtered_row_count"]) for item in declarations.values()),
        "excluded_20260514_20260515": True,
        "public_account_execution_truth_v1_included": False,
        "public_audit_coverage_note": "covered bucket uses 2026-05-02..13 where public audit exists; holdout 05-16/17/18 lacks public audit truth",
        "raw_replay_scanned": False,
        "collector_scanned": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "can_support_strategy_promotion": False,
        "assumption": "unpaired expired maker quotes leak into residual at leak_rate; residual worst case loses full cost",
        "config": {
            "edges": parse_float_list(args.edges),
            "leak_rates": parse_float_list(args.leak_rates),
            "window_s": args.window_s,
            "clip": args.clip,
            "fill_haircut": args.fill_haircut,
            "pair_fee_rate": args.pair_fee_rate,
            "residual_fee_rate": args.residual_fee_rate,
            "price_min": args.price_min,
            "price_max": args.price_max,
            "offset_min_s": args.offset_min_s,
            "offset_max_s": args.offset_max_s,
            "strict_l1_pair_cap": args.strict_l1_pair_cap,
            "max_residual_qty_rate": args.max_residual_qty_rate,
        },
        "qualified": qualified,
        "next_action": next_action,
        "results": results,
        "elapsed_s": round(time.perf_counter() - started, 3),
        "outputs": {"results_jsonl": "results.jsonl"},
    }
    write_json(output_dir / "manifest.json", manifest)
    (output_dir / "results.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in results))
    print(output_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
