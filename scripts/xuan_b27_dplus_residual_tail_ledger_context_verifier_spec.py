#!/usr/bin/env python3
"""Review D+ residual-tail ledger-context verifier implementability.

This is a local-only schema/spec reader. It inspects candidate_base and
candidate-pipeline result schemas plus the residual-tail exemplar selector
contract, then emits a verifier spec or exact missing producer fields. It does
not read events JSONL, replay/raw stores, sockets, SSH, or live paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_CANDIDATE_BASE_DB = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "local_20260502_20260518_paircap102/candidate_base.duckdb"
)
DEFAULT_RESULT_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
    "20260502_20260518_publicfull_v2"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
)

CANDIDATE_BASE_REQUIRED = (
    "candidate_row_id",
    "source_label",
    "day",
    "condition_id",
    "slug",
    "ts_ms",
    "ts_iso",
    "offset_s",
    "side",
    "opposite_side",
    "winner_side",
    "candidate_reason",
    "public_trade_price",
    "public_trade_size",
    "public_trade_row_id",
    "public_trade_recv_ms",
    "l1_pair_ask",
    "l1_pair_available_qty",
)

EXISTING_ACTION_REQUIRED = (
    "action_id",
    "candidate_row_id",
    "day",
    "condition_id",
    "slug",
    "ts_ms",
    "offset_s",
    "side",
    "public_trade_price",
    "public_trade_size",
    "l1_pair_ask",
    "seed_px",
    "seed_qty",
    "seed_cost",
    "pair_qty_after_seed",
    "pair_actions_after_seed",
    "pair_cost_wavg_after_seed",
    "inventory_yes_qty_after",
    "inventory_no_qty_after",
    "inventory_yes_cost_after",
    "inventory_no_cost_after",
    "blocked_by",
    "decision_scope",
)

EXISTING_RESIDUAL_REQUIRED = (
    "source_seed_action_id",
    "candidate_row_id",
    "day",
    "condition_id",
    "slug",
    "side",
    "qty",
    "px",
    "cost",
    "payout",
    "pnl",
    "age_s",
)

VERIFIER_REQUIRED_PRODUCER_FIELDS = (
    "source_seed_action_id",
    "source_seed_candidate_row_id",
    "source_sequence_id",
    "quote_intent_id",
    "source_order_id",
    "day",
    "condition_id",
    "slug",
    "side",
    "offset_s",
    "source_risk_direction",
    "trigger_px",
    "trigger_size",
    "trigger_ts_ms",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_ms",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def duckdb_columns(con: duckdb.DuckDBPyConnection, query: str) -> dict[str, str]:
    rows = con.execute(f"describe {query}").fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def candidate_base_columns(candidate_base_db: Path) -> dict[str, str]:
    con = duckdb.connect(str(candidate_base_db), read_only=True)
    try:
        rows = con.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_name = 'candidate_base'
            order by ordinal_position
            """
        ).fetchall()
        return {str(name): str(dtype) for name, dtype in rows}
    finally:
        con.close()


def parquet_columns(path: Path) -> dict[str, str]:
    con = duckdb.connect()
    try:
        return duckdb_columns(con, f"select * from read_parquet('{path}')")
    finally:
        con.close()


def missing(required: tuple[str, ...], columns: dict[str, str]) -> list[str]:
    return [field for field in required if field not in columns]


def selector_contract(selector_manifest: dict[str, Any]) -> dict[str, Any]:
    implementation = selector_manifest.get("implementation") if isinstance(selector_manifest.get("implementation"), dict) else {}
    guardrails = selector_manifest.get("guardrails") if isinstance(selector_manifest.get("guardrails"), dict) else {}
    return {
        "selected_target_when_signal_is_strong": implementation.get("selected_target_when_signal_is_strong"),
        "thresholds": implementation.get("thresholds", {}),
        "frozen_mechanisms_not_allowed": guardrails.get("frozen_mechanisms_not_allowed", []),
    }


def review(
    *,
    candidate_base_db: Path,
    result_dir: Path,
    selector_manifest_path: Path,
) -> dict[str, Any]:
    paths = {
        "candidate_base_db": candidate_base_db,
        "result_dir": result_dir,
        "actions_parquet": result_dir / "actions.parquet",
        "residual_lots_parquet": result_dir / "residual_lots.parquet",
        "selector_manifest": selector_manifest_path,
    }
    paths_safe = {key: path_safe(path) for key, path in paths.items()}
    candidate_columns = candidate_base_columns(candidate_base_db)
    action_columns = parquet_columns(paths["actions_parquet"])
    residual_columns = parquet_columns(paths["residual_lots_parquet"])
    selector_manifest = load_json(selector_manifest_path)

    candidate_missing = missing(CANDIDATE_BASE_REQUIRED, candidate_columns)
    action_missing = missing(EXISTING_ACTION_REQUIRED, action_columns)
    residual_missing = missing(EXISTING_RESIDUAL_REQUIRED, residual_columns)
    existing_output_columns = set(action_columns) | {f"residual_lots.{field}" for field in residual_columns}
    producer_missing = [
        field
        for field in VERIFIER_REQUIRED_PRODUCER_FIELDS
        if field not in action_columns and field not in candidate_columns and f"residual_lots.{field}" not in existing_output_columns
    ]
    derived_in_state_machine = [
        "source_risk_direction",
        "pre_seed_same_qty",
        "pre_seed_opp_qty",
        "pre_seed_same_cost",
        "pre_seed_opp_cost",
        "ledger_proxy_before",
        "ledger_proxy_after",
        "source_pair_qty",
        "source_pair_cost",
        "source_pair_pnl",
        "source_residual_qty",
        "source_residual_cost",
        "source_residual_age_ms",
    ]
    externally_named_fields_missing_locally = [
        "source_sequence_id",
        "quote_intent_id",
        "source_order_id",
    ]
    producer_export_fields_needed = sorted(set(producer_missing) | set(derived_in_state_machine) | set(externally_named_fields_missing_locally))
    base_schema_ok = not candidate_missing and not action_missing and not residual_missing and all(paths_safe.values())

    decision = "KEEP" if base_schema_ok else "UNKNOWN"
    label = (
        "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_SPEC_READY"
        if base_schema_ok
        else "UNKNOWN_RESIDUAL_TAIL_LEDGER_CONTEXT_SCHEMA_GAP"
    )
    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_residual_tail_ledger_context_verifier_spec",
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": "local_implementability_review_verifier_spec",
        "inputs": {key: str(path) for key, path in paths.items()},
        "path_safety": paths_safe,
        "selector_contract": selector_contract(selector_manifest),
        "schema_checks": {
            "candidate_base_required_present": not candidate_missing,
            "actions_required_present": not action_missing,
            "residual_lots_required_present": not residual_missing,
            "candidate_base_missing": candidate_missing,
            "actions_missing": action_missing,
            "residual_lots_missing": residual_missing,
            "candidate_base_columns_used": list(CANDIDATE_BASE_REQUIRED),
            "actions_columns_used": list(EXISTING_ACTION_REQUIRED),
            "residual_lots_columns_used": list(EXISTING_RESIDUAL_REQUIRED),
        },
        "verifier_spec": {
            "name": "residual_tail_ledger_context_verifier_v1",
            "lane": "causal_verifier",
            "status": "SPEC_READY_PRODUCER_EXPORT_NEEDED" if base_schema_ok else "SCHEMA_GAP",
            "purpose": "Use residual-tail exemplar source contexts to test a sample-preserving non-price ledger/context rule locally before any further remote run.",
            "must_start_from": [
                "future approved residual-tail exemplar shadow score.json",
                "candidate_base DuckDB schema above",
                "candidate-pipeline state-machine replay over allowed days only",
            ],
            "must_not_start_from": [
                "replay_store_v2 broad strategy search",
                "events JSONL",
                "raw/replay/collector/full completion-store scan",
                "summary-only removal",
                "static side/offset deletion",
                "price cap family",
            ],
            "producer_export_needed": {
                "name": "selected_seed_ledger_context_export_v1",
                "reason": "Current candidate-pipeline outputs can identify actions and residual lots, but do not export the pre-seed ledger/context and source-pair/source-residual attribution needed by the selector target.",
                "fields": producer_export_fields_needed,
                "fields_computable_in_state_machine": derived_in_state_machine,
                "fields_missing_from_current_local_outputs": producer_missing,
                "external_shadow_identifier_fields_missing_from_candidate_base": externally_named_fields_missing_locally,
            },
            "future_rule_shape": [
                "Use exemplar-selected side+offset+risk_direction only as context.",
                "Condition candidate actions by pre_seed same/opp qty/cost and ledger_proxy_before/after movement.",
                "Require source_pair_qty/source_pair_pnl preservation while reducing source_residual_qty/source_residual_cost.",
                "Reject any variant that blocks repair_or_pairing_improving seeds without a local retention proof.",
            ],
            "future_verifier_pass_criteria": {
                "seed_action_retention_vs_late_repair90_min": 0.75,
                "pair_qty_retention_vs_late_repair90_min": 0.85,
                "residual_qty_rate_must_improve": True,
                "residual_cost_rate_must_improve": True,
                "stress100_worst_pnl_must_not_decline_materially": True,
                "fee_after_pnl_must_remain_positive": True,
                "promotion_gate_passed": False,
                "private_truth_ready": False,
                "deployable": False,
            },
        },
        "guardrails": {
            "summary_only_removal_recommended": False,
            "remote_run_approved": False,
            "promotion_or_canary_claim_allowed": False,
            "frozen_mechanisms_not_allowed": [
                "risk_increasing_pair_fill_cap_v1",
                "summary_only_removal",
                "static_side_offset_cutoff",
                "source_public_price_cap",
                "public_trade_px_cap",
                "residual_cooldown",
                "admission_cap",
                "fill_to_balance90_shadow",
                "portfolio_ledger_shadow",
            ],
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": "Spec is ready if schema inputs are present, but a producer export is still needed before a true local verifier can score the mechanism.",
        },
        "promotion_gate": {
            "passed": False,
            "status": "VERIFIER_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "ssh_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "Implement default-off selected_seed_ledger_context_export_v1 in the local candidate-pipeline state-machine, "
            "then smoke it; do not start remote shadow/canary from this spec alone."
            if base_schema_ok
            else "Fix exact schema gaps before implementing the verifier producer export."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-db", type=Path, default=DEFAULT_CANDIDATE_BASE_DB)
    parser.add_argument("--result-dir", type=Path, default=DEFAULT_RESULT_DIR)
    parser.add_argument(
        "--selector-manifest",
        type=Path,
        default=Path(
            "xuan_research_artifacts/"
            "xuan_b27_dplus_residual_tail_exemplar_mechanism_selector_implementability_20260522T071500Z/"
            "manifest.json"
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = review(
        candidate_base_db=args.candidate_base_db,
        result_dir=args.result_dir,
        selector_manifest_path=args.selector_manifest,
    )
    write_json(args.output, report)
    print(args.output)
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
