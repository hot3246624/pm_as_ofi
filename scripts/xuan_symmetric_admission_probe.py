#!/usr/bin/env python3
"""Probe a candidate-stable symmetric admission mechanism on strict cache data.

The probe is deliberately small: it reads manifest-discovered strict/cache
DuckDB files, pairs only when the opposite-side trigger appears within a short
past window, and emits a research-only manifest. It does not read raw/replay or
control any service.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import duckdb
except ModuleNotFoundError as exc:  # pragma: no cover - operational guard
    raise SystemExit("duckdb is required. Run with `uv run --with duckdb python ...`.") from exc


ARTIFACT = "xuan_symmetric_admission_probe"
DEFAULT_ROOT = Path(os.environ.get("POLY_BT_ROOT", "/Users/hot/web3Scientist/poly_backtest_data"))
BLOCKED_DAYS = {"2026-05-14", "2026-05-15"}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_float_list(text: str) -> list[float]:
    return [float(part.strip()) for part in text.split(",") if part.strip()]


def label_days(label: str, manifest: dict[str, Any]) -> list[str]:
    days = manifest.get("days")
    if days:
        return [str(day) for day in days]
    if "_" not in label:
        return [f"{label[:4]}-{label[4:6]}-{label[6:8]}"]
    start, end = label.split("_", 1)
    # Keep this fallback intentionally simple; all current local manifests
    # include explicit days.
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


def run_probe(rows: list[tuple[Any, ...]], edge: float, window_s: float, args: argparse.Namespace) -> dict[str, Any]:
    pending: dict[str, dict[str, list[tuple[int, float, float, str]]]] = {}
    pair_qty = 0.0
    pair_cost = 0.0
    gross_pnl = 0.0
    fee = 0.0
    pair_actions = 0
    active_markets: set[str] = set()
    daily: dict[str, dict[str, float]] = {}
    for day, condition_id, ts_ms, side, public_px, public_size, _pair_now in rows:
        px = float(public_px)
        size = float(public_size or 0.0)
        if not math.isfinite(px) or size <= 0:
            continue
        seed_px = max(float(args.min_tick), px - edge)
        seed_qty = min(float(args.clip), size * float(args.fill_haircut))
        if seed_qty <= 0:
            continue
        condition = str(condition_id)
        side_text = str(side)
        opp = "NO" if side_text == "YES" else "YES"
        book = pending.setdefault(condition, {"YES": [], "NO": []})
        cutoff = int(ts_ms) - int(window_s * 1000)
        book["YES"] = [lot for lot in book["YES"] if lot[0] >= cutoff]
        book["NO"] = [lot for lot in book["NO"] if lot[0] >= cutoff]
        left = seed_qty
        while left > 1e-9 and book[opp]:
            opp_ts, opp_px, opp_qty, _opp_day = book[opp][0]
            qty = min(left, opp_qty)
            cost = seed_px + opp_px
            pair_qty += qty
            pair_cost += qty * cost
            gross_pnl += qty * (1.0 - cost)
            fee += qty * cost * float(args.fee_rate)
            pair_actions += 1
            active_markets.add(condition)
            day_item = daily.setdefault(str(day), {"pair_actions": 0.0, "pair_qty": 0.0, "gross": 0.0, "fee": 0.0})
            day_item["pair_actions"] += 1
            day_item["pair_qty"] += qty
            day_item["gross"] += qty * (1.0 - cost)
            day_item["fee"] += qty * cost * float(args.fee_rate)
            left -= qty
            opp_qty -= qty
            if opp_qty <= 1e-9:
                book[opp].pop(0)
            else:
                book[opp][0] = (opp_ts, opp_px, opp_qty, _opp_day)
        if left > 1e-9:
            book[side_text].append((int(ts_ms), seed_px, left, str(day)))

    daily_rows = []
    for day, item in sorted(daily.items()):
        daily_rows.append(
            {
                "day": day,
                "pair_actions": int(item["pair_actions"]),
                "pair_qty": round(item["pair_qty"], 6),
                "net_fee_after": round(item["gross"] - item["fee"], 6),
            }
        )
    net = gross_pnl - fee
    return {
        "edge": edge,
        "window_s": window_s,
        "pair_actions": pair_actions,
        "active_markets": len(active_markets),
        "day_count": len(daily),
        "pair_qty": round(pair_qty, 6),
        "weighted_pair_cost": round(pair_cost / pair_qty, 6) if pair_qty else None,
        "gross_pnl": round(gross_pnl, 6),
        "fee": round(fee, 6),
        "net_fee_after": round(net, 6),
        "worst_day_net_fee_after": round(min((row["net_fee_after"] for row in daily_rows), default=0.0), 6),
        "residual_qty_rate": 0.0,
        "daily": daily_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=str(DEFAULT_ROOT))
    parser.add_argument("--output-dir")
    parser.add_argument("--edges", default="0.055,0.07,0.08")
    parser.add_argument("--window-s", type=float, default=15.0)
    parser.add_argument("--clip", type=float, default=5.0)
    parser.add_argument("--fill-haircut", type=float, default=0.25)
    parser.add_argument("--fee-rate", type=float, default=0.0283)
    parser.add_argument("--price-min", type=float, default=0.05)
    parser.add_argument("--price-max", type=float, default=0.90)
    parser.add_argument("--offset-min-s", type=float, default=0.0)
    parser.add_argument("--offset-max-s", type=float, default=300.0)
    parser.add_argument("--strict-l1-pair-cap", type=float, default=1.02)
    parser.add_argument("--min-tick", type=float, default=0.01)
    parser.add_argument("--min-covered-pair-actions", type=int, default=1000)
    parser.add_argument("--min-holdout-pair-actions", type=int, default=200)
    return parser.parse_args()


def qualified_edge_rows(results: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    by_edge: dict[float, dict[str, dict[str, Any]]] = {}
    for row in results:
        by_edge.setdefault(float(row["edge"]), {})[str(row["bucket"])] = row
    qualified: list[dict[str, Any]] = []
    for edge, buckets in sorted(by_edge.items()):
        covered = buckets.get("covered")
        holdout = buckets.get("holdout")
        if not covered or not holdout:
            continue
        if int(covered["pair_actions"]) < int(args.min_covered_pair_actions):
            continue
        if int(holdout["pair_actions"]) < int(args.min_holdout_pair_actions):
            continue
        if float(covered["net_fee_after"]) <= 0 or float(holdout["net_fee_after"]) <= 0:
            continue
        if float(covered["worst_day_net_fee_after"]) <= 0 or float(holdout["worst_day_net_fee_after"]) <= 0:
            continue
        qualified.append(
            {
                "edge": edge,
                "covered_pair_actions": int(covered["pair_actions"]),
                "holdout_pair_actions": int(holdout["pair_actions"]),
                "covered_net_fee_after": covered["net_fee_after"],
                "holdout_net_fee_after": holdout["net_fee_after"],
                "covered_worst_day_net_fee_after": covered["worst_day_net_fee_after"],
                "holdout_worst_day_net_fee_after": holdout["worst_day_net_fee_after"],
            }
        )
    return qualified


def main() -> int:
    args = parse_args()
    started = time.perf_counter()
    data_root = Path(args.data_root).expanduser().resolve()
    strict_root = data_root / "backtest_cache" / "taker_buy_signal_core_v2_strict_l1"
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    declarations: dict[str, Any] = {}
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
            results.append({"bucket": bucket, **run_probe(rows, edge, float(args.window_s), args)})

    qualified_edges = qualified_edge_rows(results, args)
    status = "UNKNOWN_PROMISING_MECHANISM_CLUE" if qualified_edges else "DISCARD"
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "data_root": str(data_root),
        "dataset_type": "strict_cache_symmetric_admission_probe",
        "market_prefix": "btc-updown-5m-",
        "assets": ["BTC"],
        "row_count": sum(int(item["source_row_count"]) for item in declarations.values()),
        "filtered_row_count": sum(int(item["filtered_row_count"]) for item in declarations.values()),
        "labels": {bucket: item["labels"] for bucket, item in declarations.items()},
        "days": {bucket: item["days"] for bucket, item in declarations.items()},
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
        "status": status,
        "qualified_edges": qualified_edges,
        "assumption": "passive maker fills at max(0.01, public_trade_price-edge); no queue/private truth",
        "config": {
            "edges": parse_float_list(args.edges),
            "window_s": args.window_s,
            "clip": args.clip,
            "fill_haircut": args.fill_haircut,
            "fee_rate": args.fee_rate,
            "price_min": args.price_min,
            "price_max": args.price_max,
            "offset_min_s": args.offset_min_s,
            "offset_max_s": args.offset_max_s,
            "strict_l1_pair_cap": args.strict_l1_pair_cap,
            "min_covered_pair_actions": args.min_covered_pair_actions,
            "min_holdout_pair_actions": args.min_holdout_pair_actions,
        },
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
