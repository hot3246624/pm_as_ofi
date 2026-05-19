#!/usr/bin/env python3
"""Probe side-cost-aware trigger filters under candidate-stable D+ metrics."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.xuan_b27_dplus_candidate_stable_feature_probe import (  # noqa: E402
    run_feature_rows_for_pair,
)
from scripts.xuan_b27_dplus_compliant_metrics_runner import (  # noqa: E402
    DEFAULT_COMPLETION_ROOT,
    DEFAULT_PUBLIC_AUDIT_DB,
    DEFAULT_STRICT_ROOT,
    build_data_declaration,
    discover_inputs,
    parse_csv_floats,
    public_audit_summary,
    read_json,
    safe_adapter_join,
    safe_float,
    safe_preflight,
    summarize_rows,
    write_json,
)


ARTIFACT = "xuan_b27_dplus_candidate_stable_side_cost_probe"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_config(text: str) -> tuple[float, float]:
    first_text, cap_text = text.split(":", 1)
    return float(first_text), float(cap_text)


def parse_configs(text: str) -> list[tuple[float, float]]:
    return [parse_config(part.strip()) for part in text.split(",") if part.strip()]


def immediate_pair_cost(row: dict[str, Any]) -> float | None:
    value = row.get("strict_l1_immediate_pair") or row.get("l1_immediate_pair")
    if value is not None:
        return float(value)
    first_price = row.get("first_price")
    opp_l1_ask = row.get("opp_l1_ask")
    if first_price is None or opp_l1_ask is None:
        return None
    return float(first_price) + float(opp_l1_ask)


def row_passes(
    row: dict[str, Any],
    immediate_pair_cap: float,
    public_trade_size_max: float,
) -> bool:
    pair_cost = immediate_pair_cost(row)
    public_trade_size = row.get("public_trade_size")
    if pair_cost is None or pair_cost > immediate_pair_cap:
        return False
    if public_trade_size_max > 0:
        return public_trade_size is not None and float(public_trade_size) <= public_trade_size_max
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-preflight", required=True)
    parser.add_argument("--adapter-join-probe", required=True)
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--output-dir")
    parser.add_argument("--clip", type=int, default=10)
    parser.add_argument("--configs", default="0.45:0.98")
    parser.add_argument("--immediate-pair-caps", default="1.02,1.05,1.10")
    parser.add_argument("--public-trade-size-maxes", default="0,100,125,150,200")
    parser.add_argument("--completion-window-s", type=float, default=30.0)
    parser.add_argument("--fee-rate", type=float, default=0.0283)
    parser.add_argument("--max-candidates", type=int, default=1000)
    parser.add_argument("--min-qualified-candidates", type=int, default=100)
    parser.add_argument("--max-residual-rate", type=float, default=0.06)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    label = utc_label()
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    preflight = read_json(Path(args.input_preflight))
    adapter_join = read_json(Path(args.adapter_join_probe))
    strict_inputs = discover_inputs(Path(args.strict_root), "CACHE_MANIFEST.json", "cache.duckdb")
    completion_inputs = discover_inputs(Path(args.completion_root), "EVENT_STORE_MANIFEST.json", "event_store.duckdb")
    public_audit = public_audit_summary(Path(args.public_audit_db))
    input_safe = (
        safe_preflight(preflight)
        and safe_adapter_join(adapter_join)
        and bool(strict_inputs)
        and bool(completion_inputs)
        and public_audit.get("ready") is True
    )
    all_days = sorted(
        set().union(*(set(item["days"]) for item in strict_inputs), *(set(item["days"]) for item in completion_inputs))
        if strict_inputs and completion_inputs
        else set()
    )
    data_declaration = build_data_declaration(
        strict_inputs,
        completion_inputs,
        public_audit,
        int(adapter_join.get("row_count") or 0),
        all_days,
    )
    runs: list[dict[str, Any]] = []
    base_runs: list[dict[str, Any]] = []
    if input_safe:
        for first_max_price, pair_cap in parse_configs(args.configs):
            base_rows: list[dict[str, Any]] = []
            for strict_item in strict_inputs:
                strict_days = set(strict_item["days"])
                for completion_item in completion_inputs:
                    common_days = sorted(strict_days & set(completion_item["days"]))
                    if not common_days:
                        continue
                    base_rows.extend(
                        run_feature_rows_for_pair(
                            Path(strict_item["duckdb"]),
                            Path(completion_item["duckdb"]),
                            common_days,
                            args.clip,
                            pair_cap,
                            args.completion_window_s,
                            first_max_price,
                            args.max_candidates,
                        )
                    )
            base_config = {
                "clip": args.clip,
                "first_max_price": first_max_price,
                "pair_cap": pair_cap,
                "completion_window_s": args.completion_window_s,
                "fee_rate": args.fee_rate,
                "max_candidates_per_label_pair": args.max_candidates,
                "candidate_model": "strict_first_leg_then_candidate_stable_future_opposite_completion_event",
            }
            base_runs.append({"config": base_config, "base_metrics": summarize_rows(base_rows, args.fee_rate)})
            for immediate_pair_cap in parse_csv_floats(args.immediate_pair_caps):
                for public_trade_size_max in parse_csv_floats(args.public_trade_size_maxes):
                    rows = [
                        row
                        for row in base_rows
                        if row_passes(row, immediate_pair_cap, public_trade_size_max)
                    ]
                    if not rows:
                        continue
                    runs.append(
                        {
                            "kind": "candidate_stable_side_cost_run",
                            "config": {
                                **base_config,
                                "immediate_pair_cap": immediate_pair_cap,
                                "public_trade_size_max": public_trade_size_max,
                                "public_trade_size_filter_enabled": public_trade_size_max > 0,
                            },
                            "metrics": summarize_rows(rows, args.fee_rate),
                        }
                    )
    nonzero = [row for row in runs if row["metrics"]["candidate_count"] > 0]
    positive = [row for row in nonzero if safe_float(row["metrics"]["fee_worst_case_pnl"]) > 0]
    qualified = [
        row
        for row in positive
        if row["metrics"]["candidate_count"] >= args.min_qualified_candidates
        and row["metrics"]["qty_residual_rate"] <= args.max_residual_rate
        and safe_float(row["metrics"]["worst_day_fee_worst_case_pnl"]) >= 0
    ]
    best = max(
        nonzero,
        key=lambda row: (
            safe_float(row["metrics"]["fee_worst_case_pnl"]),
            row["metrics"]["candidate_count"],
            -safe_float(row["metrics"]["qty_residual_rate"]),
        ),
        default=None,
    )
    top_rows = sorted(
        nonzero,
        key=lambda row: (
            safe_float(row["metrics"]["fee_worst_case_pnl"]),
            row["metrics"]["candidate_count"],
            -safe_float(row["metrics"]["qty_residual_rate"]),
        ),
        reverse=True,
    )
    metrics_path = output_dir / "side_cost_runs.jsonl"
    metrics_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in top_rows))
    status = (
        "BLOCKED_SIDE_COST_PROBE_INPUT_GAP"
        if not input_safe
        else "KEEP_QUALIFIED_SIDE_COST_CANDIDATE"
        if qualified
        else "DISCARD_NO_QUALIFIED_SIDE_COST_FILTER"
    )
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_candidate_stable_side_cost_probe",
        "status": status,
        "hypothesis": (
            "requiring low trigger-time immediate opposite-side pair cost can reduce residual and cost enough "
            "to preserve sample and fee-worst metrics"
        ),
        "input_safe": input_safe,
        "input_preflight_path": str(Path(args.input_preflight)),
        "input_preflight_status": preflight.get("status"),
        "adapter_join_probe_path": str(Path(args.adapter_join_probe)),
        "adapter_join_probe_status": adapter_join.get("status"),
        "backtest_data_declaration": data_declaration,
        "data_root": data_declaration["data_root"],
        "dataset_type": data_declaration["dataset_type"],
        "labels": data_declaration["labels"],
        "days": data_declaration["days"],
        "market_prefix": data_declaration["market_prefix"],
        "assets": data_declaration["assets"],
        "row_count": data_declaration["row_count"],
        "excluded_20260514_20260515": data_declaration["excluded_20260514_20260515"],
        "contains_20260518": data_declaration["contains_20260518"],
        "includes_public_account_execution_truth_v1": data_declaration[
            "includes_public_account_execution_truth_v1"
        ],
        "public_account_truth_level": data_declaration["public_account_truth_level"],
        "can_support_strategy_promotion": False,
        "requires_compliant_backtest_dataset_for_promotion": True,
        "base_runs": base_runs,
        "run_count": len(runs),
        "nonzero_run_count": len(nonzero),
        "positive_run_count": len(positive),
        "qualified_run_count": len(qualified),
        "min_qualified_candidates": args.min_qualified_candidates,
        "max_residual_rate": args.max_residual_rate,
        "best_run": best,
        "side_cost_runs_jsonl": str(metrics_path),
        "strict_inputs": strict_inputs,
        "completion_inputs": completion_inputs,
        "public_audit": {k: v for k, v in public_audit.items() if k != "_keys"},
        "raw_replay_scanned": False,
        "duckdb_tables_read": True,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
        "next_action": (
            "parameterize and retest the qualified side-cost filter"
            if qualified
            else "kill trigger-time side-cost filter as a shadow candidate; search a different signal family"
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0 if input_safe else 2


if __name__ == "__main__":
    raise SystemExit(main())
