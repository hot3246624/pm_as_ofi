#!/usr/bin/env python3
"""Spec blocker-aware ledger-before context diagnostics for D+ no-order summaries.

This local-only review consumes already-pulled/committed completion, gap, scorer
and runner source artifacts. It does not read events JSONL, raw/replay stores,
collector stores, sockets, SSH, shared-ingress, or live trading state.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_COMPLETION = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_shadow_review_ledger_delta_tiny_deficit_completion_20260523T205739Z/"
    "manifest.json"
)
DEFAULT_GAP_AUDIT = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_ledger_delta_tiny_deficit_gap_audit_20260523T205739Z/"
    "manifest.json"
)
DEFAULT_LEDGER_BEFORE_DELTA_SCORE = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_shadow_review_ledger_delta_tiny_deficit_driver_20260523T195139Z/"
    "local_review_20260523T205739Z/ledger_before_delta/manifest.json"
)
DEFAULT_RUNNER = Path("tools/xuan_dplus_passive_passive_shadow_runner.py")
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/replay",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if abs(denominator) > 1e-12 else 0.0


def reason_from_status_reason_marker(key: str) -> str:
    parts = key.split("|", 2)
    return parts[1] if len(parts) >= 2 else "unknown"


def sum_reason_rows(rows: list[dict[str, Any]], reasons: set[str]) -> float:
    total = 0.0
    for row in rows:
        reason = reason_from_status_reason_marker(str(row.get("status_reason_marker_key") or ""))
        if reason in reasons:
            total += as_float(row.get("transition_count"))
    return total


def top_rows(rows: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: as_float(row.get("transition_count")), reverse=True)[:limit]


def token_present(text: str, token: str) -> bool:
    return token in text


def build_contract() -> dict[str, Any]:
    exact_key = (
        "status|reason|side|offset_bucket|source_risk_direction|open_qty_bucket|"
        "deficit_bucket|ledger_before_bucket"
    )
    return {
        "schema_version": "source_opportunity_ledger_before_blocker_context_v1",
        "parent_summary": "event_lite.source_opportunity_marker_summary",
        "recommended_flag": "--source-opportunity-ledger-before-blocker-context-event-lite-summary",
        "requires": [
            "--event-lite-summary",
            "--source-opportunity-marker-event-lite-summary",
            "--source-opportunity-marker-reason-source-event-lite-summary",
            "--source-opportunity-ledger-before-delta-marker-event-lite-summary",
        ],
        "exact_join_key": exact_key,
        "purpose": (
            "Explain whether the offset_90_120 tiny-deficit ledger-before mass is a legal pre-action "
            "context or only target/cooldown/static-cap blocked mass. The fields are diagnostics only."
        ),
        "new_count_fields": [
            "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_before",
        ],
        "new_quantity_sum_fields": [
            "target_room_sum_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "room_cost_sum_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "imbalance_room_sum_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "candidate_qty_sum_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "base_qty_sum_by_status_reason_side_offset_risk_open_deficit_ledger_before",
        ],
        "new_bucket_fields": [
            "target_room_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "room_cost_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "imbalance_room_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "cooldown_remaining_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "opposite_seen_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "quote_intent_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "source_order_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before",
            "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before",
        ],
        "explicitly_excluded": [
            "raw events JSONL",
            "raw quote_intent_id/source_order_id/source_sequence_id values",
            "source_pair/source_residual outcome labels as live criteria",
            "private own order/fill/inventory truth",
            "target/cooldown/static side/offset deletion rules",
            "guard enablement or trading behavior change",
        ],
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    paths = [args.completion_manifest, args.gap_manifest, args.ledger_before_delta_score, args.runner]
    unsafe = [str(path) for path in paths if not path_safe(path)]
    missing = [str(path) for path in paths if not path.exists()]
    completion = read_json(args.completion_manifest) if args.completion_manifest.exists() else {}
    gap = read_json(args.gap_manifest) if args.gap_manifest.exists() else {}
    score = read_json(args.ledger_before_delta_score) if args.ledger_before_delta_score.exists() else {}
    runner_text = args.runner.read_text() if args.runner.exists() else ""

    before = score.get("ledger_before_marker_denominator")
    if not isinstance(before, dict):
        before = {}
    delta = score.get("ledger_delta_marker_denominator")
    if not isinstance(delta, dict):
        delta = {}
    before_reason = score.get("ledger_before_reason_source_coverage")
    before_rows = before_reason.get("matching_marker_rows", []) if isinstance(before_reason, dict) else []
    if not isinstance(before_rows, list):
        before_rows = []

    before_total = as_float(before.get("marker_total"))
    before_blocked = as_float(before.get("blocked_marker_count"))
    before_admitted = as_float(before.get("admitted_marker_count"))
    delta_total = as_float(delta.get("marker_total"))
    target_cooldown_count = sum_reason_rows(before_rows, {"target", "cooldown"})
    target_count = sum_reason_rows(before_rows, {"target"})
    cooldown_count = sum_reason_rows(before_rows, {"cooldown"})
    target_cooldown_share = safe_ratio(target_cooldown_count, before_total)

    existing_generic_fields = {
        "generic_target_room_by_exact_marker_without_ledger": token_present(
            runner_text, "target_room_bucket_by_status_reason_side_offset_risk_open_deficit"
        ),
        "ledger_before_reason_count": token_present(
            runner_text, "transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_before"
        ),
        "ledger_before_reason_source_presence": token_present(
            runner_text, "source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before"
        ),
        "cooldown_block_before_target_qty": token_present(runner_text, 'block_seed("cooldown")'),
    }
    missing_exact_fields = [
        field
        for field in build_contract()["new_bucket_fields"] + build_contract()["new_quantity_sum_fields"]
        if field not in runner_text
    ]

    blockers: list[str] = []
    cautions: list[str] = []
    if unsafe:
        blockers.append("unsafe_input_path")
    if missing:
        blockers.append("missing_input_artifact")
    if before_total < args.min_before_marker_total:
        blockers.append("before_marker_mass_below_threshold")
    if target_cooldown_share < args.min_target_cooldown_share:
        blockers.append("before_marker_mass_not_concentrated_in_target_cooldown")
    if delta_total >= args.max_delta_marker_total + 1e-12:
        cautions.append("selected_delta_marker_not_thin")
    gap_blockers = gap.get("blockers") if isinstance(gap.get("blockers"), list) else []
    if "no_train_holdout_stable_candidate_with_nonzero_before_or_delta_marker" not in gap_blockers:
        cautions.append("gap_audit_did_not_report_expected_no_safe_family_blocker")
    if not existing_generic_fields["ledger_before_reason_count"]:
        blockers.append("runner_lacks_ledger_before_reason_count")
    if not existing_generic_fields["generic_target_room_by_exact_marker_without_ledger"]:
        blockers.append("runner_lacks_generic_blocker_room_context")
    if not missing_exact_fields:
        cautions.append("exact_ledger_before_blocker_context_fields_already_present")

    legal_assessment = {
        "is_new_vs_current_outputs": bool(missing_exact_fields),
        "is_diagnostic_only": True,
        "changes_trading_behavior": False,
        "uses_future_labels_as_live_criteria": False,
        "is_target_or_cooldown_deletion_rule": False,
        "is_threshold_or_cap_sweep": False,
        "requires_private_truth": False,
        "requires_shadow_or_ssh_now": False,
    }
    decision = "KEEP" if not blockers and legal_assessment["is_new_vs_current_outputs"] else "UNKNOWN"
    decision_label = (
        "KEEP_LEDGER_BEFORE_BLOCKER_CONTEXT_SPEC_READY"
        if decision == "KEEP"
        else "UNKNOWN_LEDGER_BEFORE_BLOCKER_CONTEXT_SPEC_NOT_ACTIONABLE"
    )

    return {
        "artifact": "xuan_b27_dplus_ledger_before_blocker_context_spec_v1",
        "created_utc": utc_now(),
        "decision": decision,
        "decision_label": decision_label,
        "scope": {
            "local_only_spec": True,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected_or_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "completion_manifest": str(args.completion_manifest),
            "gap_manifest": str(args.gap_manifest),
            "ledger_before_delta_score": str(args.ledger_before_delta_score),
            "runner": str(args.runner),
        },
        "unsafe_input_paths": unsafe,
        "missing_input_artifacts": missing,
        "source_decisions": {
            "completion_decision_label": completion.get("decision_label"),
            "gap_decision_label": gap.get("decision_label"),
            "ledger_before_delta_score_label": score.get("decision_label"),
        },
        "observed_before_marker_mass": {
            "marker_total": before_total,
            "admitted_marker_count": before_admitted,
            "blocked_marker_count": before_blocked,
            "target_count": target_count,
            "cooldown_count": cooldown_count,
            "target_cooldown_count": target_cooldown_count,
            "target_cooldown_share": round(target_cooldown_share, 8),
            "top_matching_rows": top_rows(before_rows),
        },
        "observed_delta_marker": {
            "marker_total": delta_total,
            "admitted_marker_count": as_float(delta.get("admitted_marker_count")),
            "blocked_marker_count": as_float(delta.get("blocked_marker_count")),
            "matched_marker_rows": delta.get("matched_marker_rows") if isinstance(delta.get("matched_marker_rows"), list) else [],
        },
        "existing_runner_surface": existing_generic_fields,
        "missing_exact_ledger_before_blocker_fields": missing_exact_fields,
        "field_contract": build_contract(),
        "legal_assessment": legal_assessment,
        "blockers": blockers,
        "cautions": cautions,
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "interpretation": (
                "This spec is only a default-off diagnostic contract for blocker-aware attribution. "
                "It is not a trading rule, private truth, deployable evidence, canary evidence, or promotion."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "FIELD_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": (
            "Implement the default-off ledger-before blocker context summary plus smoke locally, or return "
            "UNKNOWN if this diagnostic precision is not useful enough to justify another instrumented run. "
            "Do not start SSH/shadow/canary from this spec."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--completion-manifest", type=Path, default=DEFAULT_COMPLETION)
    parser.add_argument("--gap-manifest", type=Path, default=DEFAULT_GAP_AUDIT)
    parser.add_argument("--ledger-before-delta-score", type=Path, default=DEFAULT_LEDGER_BEFORE_DELTA_SCORE)
    parser.add_argument("--runner", type=Path, default=DEFAULT_RUNNER)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-before-marker-total", type=float, default=50.0)
    parser.add_argument("--min-target-cooldown-share", type=float, default=0.75)
    parser.add_argument("--max-delta-marker-total", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output, manifest)
    print(json.dumps({"decision": manifest["decision"], "decision_label": manifest["decision_label"], "output": str(args.output)}, sort_keys=True))
    return 0 if manifest["decision"] == "KEEP" else 3


if __name__ == "__main__":
    raise SystemExit(main())
