#!/usr/bin/env python3
"""Probe trigger-time feature buckets under candidate-stable D+ metrics.

This is a local, read-only xuan_b27_dplus research probe.  It keeps every
strict first-leg candidate in the denominator, counts no-window completions as
residual, and asks whether any trigger-time feature bucket can rescue
fee-worst/residual metrics.  Forward-looking cache labels are reported only as
diagnostics and are not treated as deployable selectors.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.xuan_b27_dplus_compliant_metrics_runner import (  # noqa: E402
    ADAPTER_JOIN_ARTIFACT,
    COMPLETION_TABLE,
    DEFAULT_COMPLETION_ROOT,
    DEFAULT_PUBLIC_AUDIT_DB,
    DEFAULT_STRICT_ROOT,
    PREFLIGHT_ARTIFACT,
    STRICT_TABLE,
    build_data_declaration,
    discover_inputs,
    public_audit_summary,
    read_json,
    safe_adapter_join,
    safe_float,
    safe_preflight,
    sql_list,
    sql_string,
    summarize_rows,
    table_columns,
    write_json,
)


ARTIFACT = "xuan_b27_dplus_candidate_stable_feature_probe"
DEPLOYABLE_FEATURES = [
    "offset_s",
    "first_price",
    "public_trade_size",
    "first_l2_age_ms",
    "strict_l1_age_ms",
    "opp_l1_ask",
    "first_side",
    "side_alignment",
    "strict_side_alignment",
]
DIAGNOSTIC_ONLY_FEATURES = ["min_pair_cost_30s"]


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_config(text: str) -> tuple[float, float]:
    first_text, cap_text = text.split(":", 1)
    return float(first_text), float(cap_text)


def parse_configs(text: str) -> list[tuple[float, float]]:
    return [parse_config(part.strip()) for part in text.split(",") if part.strip()]


def bucket_numeric(value: Any, cuts: list[float]) -> str:
    if value is None:
        return "null"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    for cut in cuts:
        if number <= cut:
            return f"<= {cut:g}"
    return f"> {cuts[-1]:g}"


def run_feature_rows_for_pair(
    strict_db: Path,
    completion_db: Path,
    common_days: list[str],
    clip: int,
    pair_cap: float,
    completion_window_s: float,
    first_max_price: float,
    max_candidates: int,
) -> list[dict[str, Any]]:
    buy_full_col = f"buy_full_{clip}"
    buy_vwap_col = f"buy_vwap_{clip}"
    buy_filled_col = f"buy_filled_{clip}"
    buy_worst_col = f"buy_worst_px_{clip}"
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"attach {sql_string(str(strict_db))} as strict_db (read_only)")
        con.execute(f"attach {sql_string(str(completion_db))} as completion_db (read_only)")
        strict_cols = table_columns(con, f"strict_db.{STRICT_TABLE}")
        completion_cols = table_columns(con, f"completion_db.{COMPLETION_TABLE}")
        required_strict = {
            "day",
            "slug",
            "condition_id",
            "trigger_ts_ms",
            "offset_s",
            "first_side",
            "side_alignment",
            "strict_side_alignment",
            "public_trade_size",
            "first_l2_age_ms",
            "strict_l1_age_ms",
            "public_trade_price",
            "first_l2_vwap",
            "first_l2_filled",
            "opp_l1_ask",
            "l1_immediate_pair",
            "strict_l1_immediate_pair",
            "min_pair_cost_30s",
        }
        required_completion = {
            "day",
            "condition_id",
            "ts_ms",
            "side",
            buy_full_col,
            buy_vwap_col,
            buy_filled_col,
            buy_worst_col,
        }
        missing = sorted((required_strict - strict_cols) | (required_completion - completion_cols))
        if missing:
            raise RuntimeError(f"missing required columns: {missing}")
        limit_clause = f"limit {int(max_candidates)}" if max_candidates > 0 else ""
        days_sql = sql_list(common_days)
        query = f"""
        with strict_candidates as (
            select
                row_number() over (order by day, condition_id, trigger_ts_ms, first_side) as candidate_id,
                day,
                slug,
                condition_id,
                trigger_ts_ms,
                cast(offset_s as double) as offset_s,
                first_side,
                side_alignment,
                strict_side_alignment,
                case when first_side = 'YES' then 'NO' else 'YES' end as completion_side,
                cast(first_l2_vwap as double) as first_price,
                {clip}.0 as first_qty,
                cast(public_trade_price as double) as public_trade_price,
                cast(public_trade_size as double) as public_trade_size,
                cast(first_l2_age_ms as double) as first_l2_age_ms,
                cast(strict_l1_age_ms as double) as strict_l1_age_ms,
                cast(opp_l1_ask as double) as opp_l1_ask,
                cast(l1_immediate_pair as double) as l1_immediate_pair,
                cast(strict_l1_immediate_pair as double) as strict_l1_immediate_pair,
                cast(min_pair_cost_30s as double) as min_pair_cost_30s
            from strict_db.{STRICT_TABLE}
            where day in ({days_sql})
              and first_side in ('YES', 'NO')
              and first_l2_vwap is not null
              and first_l2_vwap > 0
              and first_l2_vwap <= {first_max_price}
              and coalesce(first_l2_filled, 0) >= {clip}
            order by day, condition_id, trigger_ts_ms, first_side
            {limit_clause}
        ),
        completion_candidates as (
            select
                day,
                condition_id,
                ts_ms,
                side,
                cast({buy_vwap_col} as double) as completion_price,
                cast({buy_filled_col} as double) as completion_qty,
                cast({buy_worst_col} as double) as completion_worst_price
            from completion_db.{COMPLETION_TABLE}
            where day in ({days_sql})
              and side in ('YES', 'NO')
              and {buy_full_col} = true
              and {buy_vwap_col} is not null
              and {buy_vwap_col} > 0
              and {buy_filled_col} >= {clip}
        ),
        joined as (
            select
                sc.*,
                cc.ts_ms as completion_ts_ms,
                cc.completion_price,
                cc.completion_qty,
                cc.completion_worst_price,
                row_number() over (
                    partition by sc.candidate_id
                    order by cc.ts_ms asc nulls last
                ) as completion_rank
            from strict_candidates sc
            left join completion_candidates cc
              on cc.day = sc.day
             and cc.condition_id = sc.condition_id
             and cc.side = sc.completion_side
             and cc.ts_ms >= sc.trigger_ts_ms
             and cc.ts_ms <= sc.trigger_ts_ms + cast({completion_window_s * 1000.0} as bigint)
             and sc.first_price + cc.completion_price <= {pair_cap}
        )
        select * exclude(completion_rank)
        from joined
        where completion_rank = 1
        """
        columns = [str(item[0]) for item in con.execute(query + " limit 0").description]
        return [dict(zip(columns, row)) for row in con.execute(query).fetchall()]
    finally:
        con.close()


def feature_bucketters() -> dict[str, Callable[[dict[str, Any]], str]]:
    return {
        "offset_s": lambda row: bucket_numeric(row.get("offset_s"), [60, 120, 180, 240]),
        "first_price": lambda row: bucket_numeric(row.get("first_price"), [0.42, 0.44, 0.46, 0.48, 0.50]),
        "public_trade_size": lambda row: bucket_numeric(row.get("public_trade_size"), [5, 10, 25, 50, 100]),
        "first_l2_age_ms": lambda row: bucket_numeric(row.get("first_l2_age_ms"), [100, 250, 500, 1000, 2000]),
        "strict_l1_age_ms": lambda row: bucket_numeric(row.get("strict_l1_age_ms"), [100, 250, 500, 1000, 2000]),
        "opp_l1_ask": lambda row: bucket_numeric(row.get("opp_l1_ask"), [0.45, 0.50, 0.55, 0.60, 0.70]),
        "first_side": lambda row: str(row.get("first_side")),
        "side_alignment": lambda row: str(row.get("side_alignment")),
        "strict_side_alignment": lambda row: str(row.get("strict_side_alignment")),
        "min_pair_cost_30s": lambda row: bucket_numeric(row.get("min_pair_cost_30s"), [0.90, 0.92, 0.94, 0.96, 0.98, 1.00]),
    }


def summarize_feature_buckets(
    rows: list[dict[str, Any]],
    fee_rate: float,
    min_bucket_candidates: int,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    bucketters = feature_bucketters()
    for feature, bucketter in bucketters.items():
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(bucketter(row), []).append(row)
        for bucket, bucket_rows in sorted(grouped.items()):
            if len(bucket_rows) < min_bucket_candidates:
                continue
            metrics = summarize_rows(bucket_rows, fee_rate)
            summaries.append(
                {
                    "feature": feature,
                    "bucket": bucket,
                    "deployable_feature": feature in DEPLOYABLE_FEATURES,
                    "diagnostic_only": feature in DIAGNOSTIC_ONLY_FEATURES,
                    "metrics": metrics,
                }
            )
    return sorted(
        summaries,
        key=lambda item: (
            item["deployable_feature"],
            safe_float(item["metrics"]["fee_worst_case_pnl"]),
            -safe_float(item["metrics"]["qty_residual_rate"]),
        ),
        reverse=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-preflight", required=True)
    parser.add_argument("--adapter-join-probe", required=True)
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--output-dir")
    parser.add_argument("--clip", type=int, default=10)
    parser.add_argument("--configs", default="0.45:0.98,0.50:1.00")
    parser.add_argument("--completion-window-s", type=float, default=30.0)
    parser.add_argument("--fee-rate", type=float, default=0.0283)
    parser.add_argument("--max-candidates", type=int, default=1000)
    parser.add_argument("--min-bucket-candidates", type=int, default=50)
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
    run_summaries: list[dict[str, Any]] = []
    deployable_positive: list[dict[str, Any]] = []
    diagnostic_positive: list[dict[str, Any]] = []
    if input_safe:
        for first_max_price, pair_cap in parse_configs(args.configs):
            rows: list[dict[str, Any]] = []
            for strict_item in strict_inputs:
                strict_days = set(strict_item["days"])
                for completion_item in completion_inputs:
                    common_days = sorted(strict_days & set(completion_item["days"]))
                    if not common_days:
                        continue
                    rows.extend(
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
            base_metrics = summarize_rows(rows, args.fee_rate)
            buckets = summarize_feature_buckets(rows, args.fee_rate, args.min_bucket_candidates)
            config = {
                "clip": args.clip,
                "first_max_price": first_max_price,
                "pair_cap": pair_cap,
                "completion_window_s": args.completion_window_s,
                "fee_rate": args.fee_rate,
                "max_candidates_per_label_pair": args.max_candidates,
                "min_bucket_candidates": args.min_bucket_candidates,
                "candidate_model": "strict_first_leg_then_candidate_stable_future_opposite_completion_event",
            }
            run_summary = {
                "kind": "candidate_stable_feature_probe_run",
                "run": len(run_summaries) + 1,
                "config": config,
                "base_metrics": base_metrics,
                "top_buckets": buckets[:30],
            }
            run_summaries.append(run_summary)
            for item in buckets:
                row = {"config": config, **item}
                if safe_float(item["metrics"]["fee_worst_case_pnl"]) > 0:
                    if item["deployable_feature"]:
                        deployable_positive.append(row)
                    else:
                        diagnostic_positive.append(row)
    metrics_path = output_dir / "feature_buckets.jsonl"
    metrics_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in run_summaries))
    best_deployable = max(
        deployable_positive,
        key=lambda row: (
            safe_float(row["metrics"]["fee_worst_case_pnl"]),
            -safe_float(row["metrics"]["qty_residual_rate"]),
        ),
        default=None,
    )
    best_diagnostic = max(
        diagnostic_positive,
        key=lambda row: (
            safe_float(row["metrics"]["fee_worst_case_pnl"]),
            -safe_float(row["metrics"]["qty_residual_rate"]),
        ),
        default=None,
    )
    status = (
        "BLOCKED_FEATURE_PROBE_INPUT_GAP"
        if not input_safe
        else "KEEP_DEPLOYABLE_FEATURE_BUCKET_CANDIDATE"
        if best_deployable
        else "DIAGNOSTIC_ONLY_FORWARD_LABEL_BUCKET_POSITIVE"
        if best_diagnostic
        else "DISCARD_NO_DEPLOYABLE_FEATURE_BUCKET"
    )
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_candidate_stable_trigger_feature_probe",
        "status": status,
        "hypothesis": (
            "a trigger-time feature bucket can reduce residual/cost enough to produce fee-worst positive "
            "candidate-stable strict-first metrics"
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
        "deployable_features": DEPLOYABLE_FEATURES,
        "diagnostic_only_forward_label_features": DIAGNOSTIC_ONLY_FEATURES,
        "run_count": len(run_summaries),
        "deployable_positive_bucket_count": len(deployable_positive),
        "diagnostic_positive_bucket_count": len(diagnostic_positive),
        "best_deployable_bucket": best_deployable,
        "best_diagnostic_bucket": best_diagnostic,
        "feature_buckets_jsonl": str(metrics_path),
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
            "promote the deployable bucket to a bounded runner parameter probe"
            if best_deployable
            else "do not use forward-looking min_pair_cost_30s as a selector; search richer trigger-time interactions or new signal family"
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0 if input_safe else 2


if __name__ == "__main__":
    raise SystemExit(main())
