#!/usr/bin/env python3
"""Check whether allowed local fields can support no-order same-window parity.

This is a local-only data-adapter readiness verifier. It reads only candidate
pipeline schemas/manifests and pulled no-order summary/audit JSON. It does not
read raw/replay/collector stores, full completion stores, events JSONL, sockets,
SSH, or remote state.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_same_window_source_field_readiness_verifier"
DEFAULT_CANDIDATE_BASE_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "local_20260502_20260518_paircap102"
)
DEFAULT_CANDIDATE_REGISTRY = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
    "20260502_20260518_publicfull_v2/candidate_registry.parquet"
)
DEFAULT_GATE_PARITY = Path(
    "xuan_research_artifacts/xuan_b27_dplus_no_order_gate_parity_verifier_"
    "20260520T212620Z/manifest.json"
)
DEFAULT_STATUS_FREEZE = Path(
    "xuan_research_artifacts/xuan_b27_dplus_status_hygiene_fill_to_balance_freeze_"
    "20260520T214753Z/manifest.json"
)
DEFAULT_DIAGNOSTIC_SHADOW = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_fill_to_balance_diagnostic_driver_"
    "20260520T191701Z"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    resolved = str(path.resolve())
    return not any(fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def duckdb_columns(candidate_base_db: Path, table: str) -> dict[str, str]:
    con = duckdb.connect(str(candidate_base_db), read_only=True)
    try:
        rows = con.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_schema = 'main' and table_name = ?
            order by ordinal_position
            """,
            [table],
        ).fetchall()
    finally:
        con.close()
    return {str(name): str(dtype) for name, dtype in rows}


def parquet_columns(path: Path) -> dict[str, str]:
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(f"describe select * from read_parquet('{path.as_posix()}')").fetchall()
    finally:
        con.close()
    return {str(name): str(dtype) for name, dtype, *_rest in rows}


def nested_keys(value: Any, prefix: str = "") -> set[str]:
    out: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            child_key = f"{prefix}.{key}" if prefix else str(key)
            out.add(child_key)
            out.update(nested_keys(child, child_key))
    elif isinstance(value, list):
        for item in value[:5]:
            out.update(nested_keys(item, prefix))
    return out


def load_summaries(shadow_dir: Path) -> tuple[dict[str, Any], dict[str, Any], list[Path]]:
    aggregate = read_json(shadow_dir / "remote" / "output" / "aggregate_report.json")
    audit = read_json(shadow_dir / "audit_manifest.json")
    summaries = sorted((shadow_dir / "remote" / "output").glob("*.summary.json"))
    return aggregate, audit, summaries


def field_entry(
    *,
    field: str,
    required_for: str,
    candidate_base: dict[str, str],
    candidate_registry: dict[str, str],
    shadow_keys: set[str],
    acceptable_sources: list[str],
    missing_reason: str,
    sufficient: bool = False,
) -> dict[str, Any]:
    sources: list[str] = []
    if field in candidate_base:
        sources.append("candidate_base")
    if field in candidate_registry:
        sources.append("candidate_registry")
    if field in shadow_keys:
        sources.append("pulled_shadow_summary")
    present = bool(sources)
    return {
        "field": field,
        "required_for": required_for,
        "present": present,
        "sources": sources,
        "acceptable_sources": acceptable_sources,
        "sufficient_for_same_window_adapter": bool(sufficient and present),
        "missing_or_insufficient_reason": None if sufficient and present else missing_reason,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument("--candidate-registry", type=Path, default=DEFAULT_CANDIDATE_REGISTRY)
    parser.add_argument("--diagnostic-shadow-artifact", type=Path, default=DEFAULT_DIAGNOSTIC_SHADOW)
    parser.add_argument("--gate-parity-manifest", type=Path, default=DEFAULT_GATE_PARITY)
    parser.add_argument("--status-freeze-manifest", type=Path, default=DEFAULT_STATUS_FREEZE)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    candidate_manifest_path = args.candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_base_db = args.candidate_base_dir / "candidate_base.duckdb"
    aggregate_path = args.diagnostic_shadow_artifact / "remote" / "output" / "aggregate_report.json"
    audit_path = args.diagnostic_shadow_artifact / "audit_manifest.json"

    required_paths = [
        candidate_manifest_path,
        candidate_base_db,
        args.candidate_registry,
        aggregate_path,
        audit_path,
        args.gate_parity_manifest,
        args.status_freeze_manifest,
    ]
    missing = [str(path) for path in required_paths if not path.exists()]
    unsafe = [str(path) for path in required_paths if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": label,
            "lane": "data_adapter",
            "decision_label": "BLOCKED",
            "status": "BLOCKED_SOURCE_FIELD_READINESS_INPUT_UNAVAILABLE",
            "missing_paths": missing,
            "unsafe_paths": unsafe,
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps({"status": manifest["status"], "manifest": str(output_dir / "manifest.json")}, indent=2))
        return 2

    candidate_manifest = read_json(candidate_manifest_path)
    gate_parity = read_json(args.gate_parity_manifest)
    status_freeze = read_json(args.status_freeze_manifest)
    aggregate, audit, summaries = load_summaries(args.diagnostic_shadow_artifact)
    first_summary = read_json(summaries[0]) if summaries else {}
    candidate_base_cols = duckdb_columns(candidate_base_db, "candidate_base")
    candidate_registry_cols = parquet_columns(args.candidate_registry)
    shadow_keys = nested_keys(aggregate) | nested_keys(audit) | nested_keys(first_summary)

    present_same_window_days = set(candidate_manifest.get("days") or [])
    same_window_day = "2026-05-20"
    same_window_present = same_window_day in present_same_window_days

    matrix = [
        field_entry(
            field="condition_id",
            required_for="market identity join",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["candidate_base", "candidate_registry", "pulled_shadow_summary"],
            missing_reason="market identity is present but not sufficient without same-window source ordering",
            sufficient=True,
        ),
        field_entry(
            field="slug",
            required_for="slug/window aggregation",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["candidate_base", "candidate_registry", "pulled_shadow_summary"],
            missing_reason="slug is present but not sufficient without same-window source ordering",
            sufficient=True,
        ),
        field_entry(
            field="ts_ms",
            required_for="candidate event ordering",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["candidate_base", "candidate_registry", "pulled_shadow_summary"],
            missing_reason="timestamp is present historically but cannot reconstruct 2026-05-20 live interleaving",
            sufficient=same_window_present,
        ),
        field_entry(
            field="public_trade_row_id",
            required_for="public trade source link",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["candidate_base"],
            missing_reason="historical public_trade_row_id exists, but no same-window 2026-05-20 candidate_base is available",
            sufficient=same_window_present,
        ),
        field_entry(
            field="strict_l1_row_id",
            required_for="book snapshot source link",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["candidate_base"],
            missing_reason="historical strict_l1_row_id exists, but no same-window 2026-05-20 candidate_base is available",
            sufficient=same_window_present,
        ),
        field_entry(
            field="source_sequence_id",
            required_for="book/trade total ordering parity",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["same-window candidate registry", "default-off runner summary diagnostics"],
            missing_reason="not present in candidate_base/candidate_registry or pulled summary in a usable per-candidate matrix",
        ),
        field_entry(
            field="market_md_source_sequence_id",
            required_for="runner event source ordering parity",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["default-off runner summary diagnostics"],
            missing_reason="runner events can emit it, but pulled summary/aggregate do not expose per-candidate ordering",
        ),
        field_entry(
            field="quote_intent_id",
            required_for="seed to fill/pair/residual source link",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["default-off runner summary diagnostics"],
            missing_reason="available only in limited top_high_cost_pair_sources or events, not summary-wide candidate/residual links",
        ),
        field_entry(
            field="source_order_id",
            required_for="pair/residual source link",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["default-off runner summary diagnostics"],
            missing_reason="available only in limited high-cost pair contexts or events, not all pair/residual lots",
        ),
        field_entry(
            field="first_bid_touch_ms",
            required_for="pending-order touch timing",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["default-off runner summary diagnostics"],
            missing_reason="summary has aggregate fill/touch metrics, but no per-candidate touch timing buckets or source links",
        ),
        field_entry(
            field="first_trade_touch_ms",
            required_for="pending-order trade-through timing",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["default-off runner summary diagnostics"],
            missing_reason="summary has aggregate fill/touch metrics, but no per-candidate trade-through timing buckets or source links",
        ),
        field_entry(
            field="queue_credit",
            required_for="queue-supported fill parity",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["default-off runner summary diagnostics"],
            missing_reason="summary has queue_supported_fills totals but no per-candidate queue-credit accumulation path",
        ),
        field_entry(
            field="late_repair_fill_to_balance_diagnostics",
            required_for="fill-to-balance cap/block attribution",
            candidate_base=candidate_base_cols,
            candidate_registry=candidate_registry_cols,
            shadow_keys=shadow_keys,
            acceptable_sources=["pulled_shadow_summary"],
            missing_reason="diagnostic buckets are present but not enough to reconstruct ordering or source links",
            sufficient=True,
        ),
    ]

    insufficient = [row for row in matrix if not row["sufficient_for_same_window_adapter"]]
    required_missing = [
        row["field"]
        for row in insufficient
        if row["field"]
        in {
            "ts_ms",
            "public_trade_row_id",
            "strict_l1_row_id",
            "source_sequence_id",
            "market_md_source_sequence_id",
            "quote_intent_id",
            "source_order_id",
            "first_bid_touch_ms",
            "first_trade_touch_ms",
            "queue_credit",
        }
    ]
    if required_missing:
        decision = "UNKNOWN"
        status = "UNKNOWN_SAME_WINDOW_SOURCE_FIELDS_INSUFFICIENT"
    else:
        decision = "KEEP"
        status = "KEEP_SOURCE_FIELD_ADAPTER_READY"

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "data_adapter",
        "decision_label": decision,
        "status": status,
        "scope": {
            "local_only": True,
            "candidate_base_schema_read_only": True,
            "candidate_registry_schema_read_only": True,
            "pulled_summary_json_read_only": True,
            "ssh_used": False,
            "shared_ingress_connected": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_collector_scanned": False,
            "full_completion_store_scanned": False,
            "runner_or_trading_paths_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_base_db),
            "candidate_registry": str(args.candidate_registry),
            "diagnostic_shadow_aggregate": str(aggregate_path),
            "diagnostic_shadow_audit": str(audit_path),
            "summary_json_count": len(summaries),
            "gate_parity_manifest": str(args.gate_parity_manifest),
            "status_freeze_manifest": str(args.status_freeze_manifest),
        },
        "coverage": {
            "candidate_base_days": sorted(present_same_window_days),
            "same_window_day_required": same_window_day,
            "same_window_day_present": same_window_present,
            "candidate_base_row_count": candidate_manifest.get("row_count"),
            "candidate_base_candidate_reason_counts": candidate_manifest.get("candidate_reason_counts"),
            "diagnostic_shadow_metrics": aggregate.get("metrics", {}),
            "diagnostic_shadow_decision": audit.get("decision_label"),
            "prior_gate_parity_status": gate_parity.get("status"),
            "freeze_status": status_freeze.get("status"),
        },
        "schema_inventory": {
            "candidate_base_columns": candidate_base_cols,
            "candidate_registry_columns": candidate_registry_cols,
            "shadow_summary_top_level_keys": sorted(k for k in shadow_keys if "." not in k),
            "shadow_event_lite_keys": sorted((aggregate.get("event_lite") or {}).keys()),
            "f2b_diagnostic_keys": sorted(
                ((aggregate.get("event_lite") or {}).get("late_repair_fill_to_balance_diagnostics") or {}).keys()
            ),
        },
        "field_availability_matrix": matrix,
        "adapter_readiness": {
            "can_build_same_window_no_order_parity_adapter_from_allowed_local_data": decision == "KEEP",
            "can_build_historical_candidate_pipeline_adapter": True,
            "historical_adapter_is_same_window_live_evidence": False,
            "default_off_instrumentation_path_exists": True,
            "instrumentation_path": [
                "Keep fill-to-balance90 frozen for shadow/canary.",
                "If a future local KEEP requires transfer evidence, add default-off summary buckets for source_sequence_id/order/quote_intent/touch/fill timing joins and smoke locally before any shadow.",
                "Do not retroactively pull events JSONL for prior shadows.",
            ],
        },
        "exact_missing_fields": [
            "2026-05-20 same-window candidate_base/candidate_registry rows",
            "per-candidate live source_sequence_id ordering across book and trade events",
            "summary-wide quote_intent_id/source_order_id links from candidates to fills, pair actions, and residual lots",
            "per-candidate pending-order queue_credit accumulation",
            "per-candidate first_bid_touch_ms and first_trade_touch_ms timing buckets tied to quote_intent/source_order",
            "true shared-ingress book/trade interleaving for the diagnostic shadow window",
        ],
        "research_ranking": {
            "label": status,
            "decision": decision,
            "economic_quality_inferred": False,
            "notes": [
                "This verifier evaluates data adapter readiness, not strategy economics.",
                "Local historical fill-to-balance positives remain research-only.",
                "The exact diagnostic and sample-scale fill-to-balance shadows remain discarded for promotion risk budget.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_DATA_ADAPTER_READINESS_ONLY_NOT_PROMOTION_EVIDENCE",
            "notes": [
                "No shadow/canary can use this artifact as promotion evidence.",
                "No further fill-to-balance90 shadow is allowed until same-window source fields exist and a local verifier returns KEEP.",
            ],
        },
        "next_executable_action": (
            "Do not run EC2 shadow/canary. Keep fill-to-balance90 frozen. Either implement a local-only default-off "
            "source-link diagnostic smoke/spec, or switch to the explicitly requested public Polymarket profile review "
            "using public web/API only and no private truth."
            if decision == "UNKNOWN"
            else "Implement the default-off same-window source-field adapter locally and smoke it without EC2."
        ),
        "deployable": False,
        "can_support_strategy_promotion": False,
        "g2_canary_ready": False,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "status": status,
                "decision_label": decision,
                "manifest": str(output_dir / "manifest.json"),
                "same_window_day_present": same_window_present,
                "missing_field_count": len(required_missing),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
