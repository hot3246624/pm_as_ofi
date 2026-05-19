#!/usr/bin/env python3
"""Compare legacy ASOF window filtering with candidate-stable completion semantics.

This is a local, read-only probe for xuan_b27_dplus compliant metrics.  The
existing metrics runner applies the completion window after the ASOF join.  If
the nearest future completion is outside the window, that strict first-leg row
is filtered out instead of counted as residual.  This probe keeps the same input
scope and compares that behavior with a candidate-stable join that counts such
rows as residual.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.xuan_b27_dplus_compliant_metrics_runner import (  # noqa: E402
    ADAPTER_JOIN_ARTIFACT,
    COMPLETION_TABLE,
    DEFAULT_COMPLETION_ROOT,
    DEFAULT_POLY_BT_ROOT,
    DEFAULT_PUBLIC_AUDIT_DB,
    DEFAULT_STRICT_ROOT,
    PREFLIGHT_ARTIFACT,
    STRICT_TABLE,
    build_data_declaration,
    discover_inputs,
    parse_csv_floats,
    public_audit_summary,
    read_json,
    run_config_for_pair,
    safe_adapter_join,
    safe_float,
    safe_preflight,
    sql_list,
    sql_string,
    summarize_rows,
    table_columns,
    write_json,
)


ARTIFACT = "xuan_b27_dplus_compliant_metrics_window_semantics_probe"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run_config_for_pair_candidate_stable(
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
            "first_side",
            "first_l2_vwap",
            "first_l2_filled",
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
        window_ms = completion_window_s * 1000.0
        query = f"""
        with strict_candidates as (
            select
                row_number() over (order by day, condition_id, trigger_ts_ms, first_side) as candidate_id,
                day,
                slug,
                condition_id,
                trigger_ts_ms,
                first_side,
                case when first_side = 'YES' then 'NO' else 'YES' end as completion_side,
                cast(first_l2_vwap as double) as first_price,
                {clip}.0 as first_qty
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
                sc.candidate_id,
                sc.day,
                sc.slug,
                sc.condition_id,
                sc.trigger_ts_ms,
                sc.first_side,
                sc.completion_side,
                sc.first_price,
                sc.first_qty,
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
             and cc.ts_ms <= sc.trigger_ts_ms + cast({window_ms} as bigint)
             and sc.first_price + cc.completion_price <= {pair_cap}
        )
        select
            candidate_id,
            day,
            slug,
            condition_id,
            trigger_ts_ms,
            first_side,
            completion_side,
            first_price,
            first_qty,
            completion_ts_ms,
            completion_price,
            completion_qty,
            completion_worst_price
        from joined
        where completion_rank = 1
        """
        columns = [str(item[0]) for item in con.execute(query + " limit 0").description]
        return [dict(zip(columns, row)) for row in con.execute(query).fetchall()]
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-preflight", required=True)
    parser.add_argument("--adapter-join-probe", required=True)
    parser.add_argument("--strict-root", default=str(DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--output-dir")
    parser.add_argument("--clip", type=int, default=10)
    parser.add_argument("--pair-caps", default="0.90,0.92,0.94,0.96,0.98,1.00")
    parser.add_argument("--first-max-prices", default="0.45,0.46,0.50")
    parser.add_argument("--completion-window-s", type=float, default=30.0)
    parser.add_argument("--fee-rate", type=float, default=0.0283)
    parser.add_argument("--max-candidates", type=int, default=1000)
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
    rows_by_config: list[dict[str, Any]] = []
    if input_safe:
        for first_max_price in parse_csv_floats(args.first_max_prices):
            for pair_cap in parse_csv_floats(args.pair_caps):
                legacy_rows: list[dict[str, Any]] = []
                stable_rows: list[dict[str, Any]] = []
                for strict_item in strict_inputs:
                    strict_days = set(strict_item["days"])
                    for completion_item in completion_inputs:
                        common_days = sorted(strict_days & set(completion_item["days"]))
                        if not common_days:
                            continue
                        legacy_rows.extend(
                            run_config_for_pair(
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
                        stable_rows.extend(
                            run_config_for_pair_candidate_stable(
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
                legacy_metrics = summarize_rows(legacy_rows, args.fee_rate)
                stable_metrics = summarize_rows(stable_rows, args.fee_rate)
                rows_by_config.append(
                    {
                        "kind": "window_semantics_compare_run",
                        "run": len(rows_by_config) + 1,
                        "config": {
                            "clip": args.clip,
                            "pair_cap": pair_cap,
                            "first_max_price": first_max_price,
                            "completion_window_s": args.completion_window_s,
                            "fee_rate": args.fee_rate,
                            "max_candidates_per_label_pair": args.max_candidates,
                        },
                        "legacy_post_asof_filter_metrics": legacy_metrics,
                        "candidate_stable_window_metrics": stable_metrics,
                        "delta": {
                            "candidate_count": stable_metrics["candidate_count"] - legacy_metrics["candidate_count"],
                            "unpaired_candidate_count": stable_metrics["unpaired_candidate_count"]
                            - legacy_metrics["unpaired_candidate_count"],
                            "fee_worst_case_pnl": round(
                                safe_float(stable_metrics["fee_worst_case_pnl"])
                                - safe_float(legacy_metrics["fee_worst_case_pnl"]),
                                6,
                            ),
                            "stress100_worst_pnl": round(
                                safe_float(stable_metrics["stress100_worst_pnl"])
                                - safe_float(legacy_metrics["stress100_worst_pnl"]),
                                6,
                            ),
                        },
                    }
                )
    metrics_path = output_dir / "metrics.jsonl"
    metrics_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows_by_config))
    legacy_positive_fixed_nonpositive = [
        row
        for row in rows_by_config
        if safe_float(row["legacy_post_asof_filter_metrics"]["fee_worst_case_pnl"]) > 0
        and safe_float(row["candidate_stable_window_metrics"]["fee_worst_case_pnl"]) <= 0
    ]
    fixed_positive = [
        row for row in rows_by_config if safe_float(row["candidate_stable_window_metrics"]["fee_worst_case_pnl"]) > 0
    ]
    best_fixed = max(
        rows_by_config,
        key=lambda row: safe_float(row["candidate_stable_window_metrics"]["fee_worst_case_pnl"]),
        default=None,
    )
    status = (
        "BLOCKED_WINDOW_SEMANTICS_INPUT_GAP"
        if not input_safe
        else "FAIL_LEGACY_WINDOW_FILTER_FALSE_POSITIVE_RISK"
        if legacy_positive_fixed_nonpositive
        else "PASS_WINDOW_SEMANTICS_FIXED_POSITIVE_REMAINS"
        if fixed_positive
        else "DISCARD_NO_FIXED_SEMANTICS_POSITIVE_CONFIG"
    )
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_declared_cache_store_window_semantics_probe",
        "status": status,
        "hypothesis": (
            "post-ASOF completion-window filtering can drop first-leg candidates instead of counting them as residual, "
            "creating false positive fee-worst metrics"
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
        "run_count": len(rows_by_config),
        "legacy_positive_fixed_nonpositive_count": len(legacy_positive_fixed_nonpositive),
        "fixed_positive_fee_worst_case_run_count": len(fixed_positive),
        "best_fixed_run": best_fixed,
        "metrics_jsonl": str(metrics_path),
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
            "patch compliant metrics runner to use candidate-stable window semantics before further cap/filter sweeps"
            if legacy_positive_fixed_nonpositive
            else "continue feature segmentation on candidate-stable metrics"
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0 if input_safe else 2


if __name__ == "__main__":
    raise SystemExit(main())
