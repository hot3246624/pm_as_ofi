#!/usr/bin/env python3
"""Specify the denominator replay scorer for observable pre-action rule mining.

This local-only spec defines how a frozen pre-action predicate may consume the
allowlisted feature-join outputs to build a no-order denominator summary and an
input manifest for the rule miner. It is not a live runner and does not inspect
events JSONL or raw/replay stores.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec"
CONTRACT_NAME = "observable_pre_action_rule_miner_denominator_replay_scorer_spec_v1"
DEFAULT_FEATURE_JOIN_SCORER_MANIFEST = (
    Path("xuan_research_artifacts")
    / "xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z"
    / "manifest.json"
)

ALLOWED_LIVE_FEATURE_FAMILIES = (
    "status_before_action",
    "block_reason",
    "reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "side",
    "offset_bucket",
    "pre_seed_same_qty_bucket",
    "pre_seed_opp_qty_bucket",
    "pre_seed_open_qty_bucket",
    "pre_seed_deficit_qty_bucket",
    "candidate_qty_bucket",
    "source_risk_direction",
)

FORBIDDEN_LIVE_FEATURE_FAMILIES = (
    "source_pair",
    "source_residual",
    "realized_pair_cost",
    "settlement",
    "redeem",
    "future_label",
    "private_truth",
    "public_profile_single_day_profit",
    "static_side_offset_price_cap",
    "dplus_failed_family_marker",
)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def feature_join_scorer_blockers(path: Path) -> list[str]:
    if not path.exists():
        return ["feature_join_output_scorer_manifest_missing"]
    manifest = load_json(path)
    blockers = []
    if manifest.get("decision_label") != "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY":
        blockers.append("feature_join_output_scorer_not_ready")
    if manifest.get("strategy_evidence") is not False:
        blockers.append("feature_join_strategy_evidence_not_false")
    if manifest.get("private_truth_ready") is not False:
        blockers.append("feature_join_private_truth_ready_not_false")
    if manifest.get("deployable") is not False:
        blockers.append("feature_join_deployable_not_false")
    promotion_gate = manifest.get("promotion_gate") or {}
    if promotion_gate.get("passed") is not False:
        blockers.append("feature_join_promotion_gate_not_false")
    safety = manifest.get("safety") or {}
    for key in ("events_jsonl_read", "events_jsonl_pulled", "raw_replay_or_full_store_scan", "orders_sent"):
        if safety.get(key) is not False:
            blockers.append(f"feature_join_safety_{key}_not_false")
    return blockers


def contract(feature_join_scorer_manifest: Path, blockers: list[str]) -> dict[str, Any]:
    ready = not blockers
    return {
        "artifact": ARTIFACT,
        "contract_name": CONTRACT_NAME,
        "decision": "KEEP" if ready else "UNKNOWN",
        "decision_label": (
            "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_SCORER_SPEC_READY"
            if ready
            else "UNKNOWN_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_SCORER_SPEC_FAIL_CLOSED"
        ),
        "blockers": blockers,
        "feature_join_output_scorer_manifest": str(feature_join_scorer_manifest),
        "current_real_input_status": "UNKNOWN_DENOMINATOR_REPLAY_SCORER_NOT_IMPLEMENTED",
        "future_scorer_target": {
            "name": "observable_pre_action_rule_miner_denominator_replay_scorer_v1",
            "inputs": [
                "observable_pre_action_candidate_rows.jsonl",
                "observable_pre_action_source_link_summary.json",
                "observable_pre_action_feature_join_manifest.json",
                "frozen_observable_pre_action_rule_manifest.json",
                "same_window_aggregate_summary.json",
            ],
            "outputs": [
                "observable_pre_action_no_order_denominator_summary.json",
                "observable_pre_action_rule_miner_input_manifest.json",
            ],
        },
        "frozen_rule_manifest_contract": {
            "schema_version": "frozen_observable_pre_action_rule_manifest_v1",
            "required_fields": [
                "frozen_rule_id",
                "rule_version",
                "created_from_train_manifest",
                "holdout_manifest",
                "predicate_expression",
                "predicate_feature_names",
                "allowed_live_feature_families",
                "forbidden_live_feature_families",
                "train_label_fields",
                "holdout_label_fields",
                "same_window_scope",
            ],
            "allowed_live_feature_families": list(ALLOWED_LIVE_FEATURE_FAMILIES),
            "forbidden_live_feature_families": list(FORBIDDEN_LIVE_FEATURE_FAMILIES),
            "label_separation": {
                "post_action_labels_allowed_for_train_holdout_scoring": True,
                "post_action_labels_allowed_in_live_predicate": False,
                "realized_pair_cost_allowed_in_live_predicate": False,
            },
        },
        "denominator_summary_contract": {
            "schema_version": "observable_pre_action_no_order_denominator_summary_v1",
            "required_fields": [
                "frozen_rule_id",
                "same_window_id",
                "same_window_denominator_count",
                "rule_match_count",
                "admitted_count",
                "blocked_count",
                "source_sequence_coverage",
                "quote_intent_presence_rate",
                "source_order_presence_rate",
                "aggregate_parity",
                "field_contract",
            ],
            "minimums": {
                "same_window_denominator_count": 100,
                "rule_match_count": 20,
                "source_sequence_coverage": 0.95,
            },
            "aggregate_parity_requirements": [
                "row_count_equals_source_summary_row_count",
                "admitted_plus_blocked_equals_denominator_count",
                "source_presence_counts_rebuild_from_rows",
            ],
        },
        "input_manifest_contract": {
            "schema_version": "observable_pre_action_rule_miner_input_v1",
            "required_paths": [
                "candidate_rows_path",
                "source_link_summary_path",
                "no_order_denominator_summary_path",
            ],
            "required_gates": [
                "train_discovery_passed",
                "holdout_stability_passed",
                "same_window_denominator_reproduced",
            ],
        },
        "fail_closed_conditions": [
            "missing_feature_join_rows",
            "missing_source_link_summary",
            "missing_feature_join_manifest",
            "missing_frozen_rule_id",
            "predicate_uses_forbidden_live_feature_family",
            "post_action_label_used_in_live_predicate",
            "realized_pair_cost_used_as_live_criteria",
            "denominator_sample_too_small",
            "rule_match_sample_too_small",
            "source_sequence_coverage_below_min",
            "aggregate_parity_failed",
            "private_or_deployable_or_promotion_claim_present",
        ],
        "research_ranking": "DENOMINATOR_REPLAY_SCORER_SPEC_READY_NO_STRATEGY_EVIDENCE" if ready else "SPEC_INPUT_FAIL_CLOSED",
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "denominator_replay_spec_only",
        },
        "safety": {
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scan": False,
        },
        "next_executable_action": (
            "Implement observable_pre_action_rule_miner_denominator_replay_fixture_scorer_v1 local-only: "
            "use synthetic frozen-rule and feature-join fixtures to produce denominator summary and input manifest, "
            "with fail-closed checks before any no-order diagnostic."
            if ready
            else "Provide a KEEP feature-join output scorer manifest before implementing denominator replay."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-join-scorer-manifest", type=Path, default=DEFAULT_FEATURE_JOIN_SCORER_MANIFEST)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    blockers = feature_join_scorer_blockers(args.feature_join_scorer_manifest)
    manifest = contract(args.feature_join_scorer_manifest, blockers)
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
