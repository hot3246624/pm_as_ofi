#!/usr/bin/env python3
"""Spec marker-by-reason/source coverage for D+ no-order summaries.

This is a local-only implementability/spec reader. It inspects the committed
runner/scorer source and produces a field contract; it does not read events
JSONL, raw/replay stores, sockets, SSH, or live order paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_RUNNER_TOKENS = (
    "def record_source_opportunity_marker",
    "marker_key = source_opportunity_marker_key",
    "status_reason = f\"{status}|{reason}\"",
    "quote_intent_id",
    "source_order_id",
    "source_sequence_id",
    "activation_opp_age_bucket_by_status_reason",
)

CURRENT_LIMITATION_TOKENS = (
    "micro_deficit_marker_count_by_status_reason",
    "source_sequence_presence_by_status_reason",
)

REQUIRED_SCORER_TOKENS = (
    "reason_join_limitation",
    "status_reason-level coverage",
    "UNKNOWN_SOURCE_OPPORTUNITY_MARKER_SUMMARY_INPUTS_INSUFFICIENT",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_text(path: Path) -> str:
    return path.read_text()


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def token_checks(text: str, tokens: tuple[str, ...]) -> dict[str, bool]:
    return {token: token in text for token in tokens}


def build_spec(args: argparse.Namespace) -> dict[str, Any]:
    runner_text = read_text(args.runner)
    scorer_text = read_text(args.scorer)
    runner_checks = token_checks(runner_text, REQUIRED_RUNNER_TOKENS)
    limitation_checks = token_checks(runner_text, CURRENT_LIMITATION_TOKENS)
    scorer_checks = token_checks(scorer_text, REQUIRED_SCORER_TOKENS)
    missing_runner = [token for token, ok in runner_checks.items() if not ok]
    missing_scorer = [token for token, ok in scorer_checks.items() if not ok]
    current_status_reason_only_present = all(limitation_checks.values())

    field_contract = {
        "schema_version": "source_opportunity_marker_reason_source_coverage_v1",
        "parent_summary": "event_lite.source_opportunity_marker_summary",
        "recommended_flag": "--source-opportunity-marker-reason-source-event-lite-summary",
        "requires": [
            "--event-lite-summary",
            "--source-opportunity-marker-event-lite-summary"
        ],
        "exact_join_key": "status|reason|side|offset_bucket|source_risk_direction|open_qty_bucket|deficit_bucket",
        "new_count_fields": [
            "transition_count_by_status_reason_side_offset_risk_open_deficit",
            "micro_deficit_marker_count_by_status_reason_side_offset_risk_open_deficit"
        ],
        "new_quantity_sum_fields": [
            "candidate_qty_sum_by_status_reason_side_offset_risk_open_deficit",
            "base_qty_sum_by_status_reason_side_offset_risk_open_deficit",
            "target_room_sum_by_status_reason_side_offset_risk_open_deficit",
            "room_cost_sum_by_status_reason_side_offset_risk_open_deficit",
            "imbalance_room_sum_by_status_reason_side_offset_risk_open_deficit"
        ],
        "new_bucket_fields": [
            "candidate_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
            "base_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
            "target_room_bucket_by_status_reason_side_offset_risk_open_deficit",
            "room_cost_bucket_by_status_reason_side_offset_risk_open_deficit",
            "imbalance_room_bucket_by_status_reason_side_offset_risk_open_deficit",
            "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
            "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_deficit",
            "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_deficit",
            "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_deficit",
            "opposite_seen_by_status_reason_side_offset_risk_open_deficit",
            "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_deficit",
            "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit",
            "source_order_presence_by_status_reason_side_offset_risk_open_deficit",
            "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit"
        ],
        "explicitly_excluded": [
            "raw events JSONL",
            "raw quote_intent_id/source_order_id/source_sequence_id values",
            "source_pair_qty/source_pair_cost/source_pair_pnl",
            "source_residual_qty/source_residual_cost/source_residual_age_ms",
            "private own order/fill/inventory truth",
            "guard enablement or trading behavior change"
        ],
        "purpose": (
            "Explain whether the frozen marker disappears because it is absent from the same-window "
            "opportunity surface or because it is concentrated in a specific blocked/admitted reason "
            "with missing source coverage."
        )
    }

    blockers: list[str] = []
    if missing_runner:
        blockers.append("runner_record_source_opportunity_marker_contract_missing")
    if missing_scorer:
        blockers.append("existing_scorer_limitation_contract_missing")
    if not current_status_reason_only_present:
        blockers.append("current_status_reason_only_fields_not_detected")

    decision = "KEEP" if not blockers else "UNKNOWN"
    label = (
        "KEEP_SOURCE_OPPORTUNITY_MARKER_REASON_SOURCE_COVERAGE_SPEC_READY"
        if decision == "KEEP"
        else "UNKNOWN_SOURCE_OPPORTUNITY_MARKER_REASON_SOURCE_COVERAGE_SPEC_GAP"
    )
    return {
        "artifact": "xuan_b27_dplus_source_opportunity_marker_reason_source_coverage_spec",
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only_spec": True,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected_or_modified": False,
            "orders_cancels_redeems_sent": False
        },
        "inputs": {
            "runner": str(args.runner),
            "scorer": str(args.scorer)
        },
        "checks": {
            "runner_required_tokens": runner_checks,
            "current_limitation_tokens": limitation_checks,
            "scorer_required_tokens": scorer_checks
        },
        "blockers": blockers,
        "field_contract": field_contract,
        "implementation_policy": {
            "default_off": True,
            "existing_behavior_unchanged_when_disabled": True,
            "aggregate_merge_required": True,
            "smoke_required": [
                "default-off absence",
                "enabled exact status_reason_marker key count",
                "enabled admitted and blocked source coverage presence histograms",
                "aggregate parity",
                "CLI requires --event-lite-summary and --source-opportunity-marker-event-lite-summary",
                "post-action outcome labels excluded"
            ]
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "This is a local field-contract spec for marker-denominator transfer-gap diagnostics. "
                "It is not strategy economics, private truth, deployable evidence, canary evidence, or promotion."
            )
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "FIELD_SPEC_ONLY_NOT_PROMOTION_EVIDENCE"
        },
        "next_executable_action": (
            "Implement the default-off reason/source coverage summary and smoke locally, or wait for exact "
            "main-thread approval of late_repair90_micro_deficit_marker_reproduction_no_order_review if this "
            "extra precision is not needed before the diagnostic run."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, default=Path("tools/xuan_dplus_passive_passive_shadow_runner.py"))
    parser.add_argument("--scorer", type=Path, default=Path("scripts/xuan_b27_dplus_source_opportunity_marker_summary_scorer.py"))
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_spec(args)
    write_json(args.output, manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["decision"] == "KEEP" else 3


if __name__ == "__main__":
    raise SystemExit(main())
