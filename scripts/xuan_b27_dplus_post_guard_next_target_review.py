#!/usr/bin/env python3
"""Review next target after the portfolio-negative risk guard discard.

This is local-only status hygiene. It consumes the latest completion manifest
and the local guard verifier manifest, then decides whether the remaining
intersection/exemplar evidence supports one narrower action-level local
mechanism. It does not read events JSONL, raw/replay stores, sockets, SSH, or
live trading state.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def get_intersection_groups(completion: dict[str, Any]) -> list[dict[str, Any]]:
    scorer = completion.get("scorers", {}).get("portfolio_ledger_source_link_intersection", {})
    metrics = scorer.get("intersection_metrics", {}) if isinstance(scorer, dict) else {}
    groups = metrics.get("top_intersection_groups", []) if isinstance(metrics, dict) else []
    return [row for row in groups if isinstance(row, dict)]


def review(completion_path: Path, guard_path: Path, completion: dict[str, Any], guard: dict[str, Any]) -> dict[str, Any]:
    unsafe = [str(path) for path in (completion_path, guard_path) if not path_safe(path)]
    groups = get_intersection_groups(completion)
    top_group = groups[0] if groups else {}
    guard_ranking = guard.get("research_ranking", {}) if isinstance(guard.get("research_ranking"), dict) else {}
    guard_delta = guard.get("delta_vs_control", {}) if isinstance(guard.get("delta_vs_control"), dict) else {}
    residual_tail = completion.get("scorers", {}).get("residual_tail_exemplar", {})
    residual_metrics = residual_tail.get("exemplar_metrics", {}) if isinstance(residual_tail, dict) else {}

    blockers = []
    if unsafe:
        blockers.append("unsafe_input_path")
    if completion.get("decision") not in {"UNKNOWN", "KEEP"}:
        blockers.append("completion_manifest_unexpected_decision")
    if guard.get("decision") != "DISCARD":
        blockers.append("guard_discard_manifest_missing")
    if not groups:
        blockers.append("intersection_groups_missing")

    top_group_key = str(top_group.get("source_side_offset_risk_direction_ledger") or "")
    top_group_pair_qty = as_float(top_group.get("source_pair_qty"))
    top_group_share = as_float(top_group.get("source_residual_cost_share"))
    broad_guard_seed_retention = as_float(guard_ranking.get("seed_action_retention"))
    broad_guard_pair_retention = as_float(guard_ranking.get("pair_qty_retention"))
    broad_guard_residual_cost_reduction = as_float(guard_ranking.get("residual_cost_rate_reduction"))

    rejected_candidates = [
        {
            "candidate": "top_intersection_bucket_guard",
            "decision": "DISCARD",
            "reason": (
                "The strongest group is expressed as side|offset|risk_direction|ledger bucket. Turning it into a "
                "rule would be a static side/offset deletion layered on the already-discarded broad ledger guard."
            ),
            "evidence": {
                "top_group": top_group_key,
                "top_group_residual_cost_share": top_group_share,
                "top_group_source_pair_qty": top_group_pair_qty,
            },
        },
        {
            "candidate": "source_pair_zero_residual_tail_guard",
            "decision": "DISCARD",
            "reason": (
                "source_pair_qty=0 and source_residual_cost are post-action/future attribution fields. They can label "
                "training examples but cannot be used as live pre-action decision criteria."
            ),
            "evidence": {
                "top_groups_have_source_pair_qty_zero": all(as_float(row.get("source_pair_qty")) == 0.0 for row in groups[:5]),
            },
        },
        {
            "candidate": "narrower_negative_ledger_risk_threshold",
            "decision": "UNKNOWN",
            "reason": (
                "The broad online guard failed because it blocked too much pair/carry flow. Current manifests do not "
                "contain per-seed pre-action ledger proxy distributions joined to source_pair/source_residual labels, "
                "so a tighter threshold would be an ungrounded parameter sweep."
            ),
            "evidence": {
                "broad_guard_seed_action_retention": broad_guard_seed_retention,
                "broad_guard_pair_qty_retention": broad_guard_pair_retention,
                "broad_guard_residual_cost_rate_reduction": broad_guard_residual_cost_reduction,
            },
        },
    ]

    exact_missing = [
        "per-seed pre-action ledger_proxy_before/after distribution for all admitted and blocked candidates",
        "per-seed source_pair_qty/source_pair_cost/source_pair_pnl/source_residual_qty/source_residual_cost labels for all selected seeds, not only top residual exemplars",
        "same-window source_sequence_id/quote_intent_id/source_order_id joined to local candidate_row_id for candidate-pipeline replay",
        "runner-observed pending/opposite order availability and queue/close opportunity at the source action",
        "action-level follow-up close/rescue linkage after residual-tail seed admission",
        "a pre-action separator that predicts zero-pair residual-tail cases without using future source_pair/source_residual attribution",
    ]
    if as_float(residual_metrics.get("required_field_coverage")) < 1.0:
        exact_missing.append("residual-tail exemplar required field coverage below 100%")
    if as_float(residual_metrics.get("source_sequence_coverage")) < 1.0:
        exact_missing.append("residual-tail exemplar source_sequence coverage below 100%")

    decision = "UNKNOWN"
    decision_label = "UNKNOWN_POST_GUARD_NO_SAFE_NARROW_ACTION_LEVEL_MECHANISM"
    selected_next_target = {
        "name": None,
        "reason": (
            "The remaining signal is diagnostic and concentrated, but every currently expressible rule either repeats "
            "the discarded broad guard, becomes static side/offset deletion, or relies on future attribution."
        ),
    }
    next_action = (
        "Implement local-only implementability review for candidate_seed_outcome_separator_export_v1: a default-off "
        "state-machine/export surface that emits per-seed pre-action ledger/context fields plus post-action pair/residual "
        "labels for offline separator discovery; do not start SSH/shadow/canary."
    )

    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_post_guard_next_target_review",
        "created_utc": utc_label(),
        "lane": "status_hygiene",
        "decision": decision,
        "decision_label": decision_label,
        "scope": "local_only_post_guard_next_target_selection",
        "inputs": {
            "completion_manifest": str(completion_path),
            "guard_verifier_manifest": str(guard_path),
        },
        "source_evidence": {
            "completion_decision_label": completion.get("decision_label"),
            "intersection_status": completion.get("scorers", {})
            .get("portfolio_ledger_source_link_intersection", {})
            .get("status"),
            "residual_tail_status": residual_tail.get("status") if isinstance(residual_tail, dict) else None,
            "guard_decision_label": guard.get("decision_label"),
            "top_intersection_groups": groups[:8],
        },
        "broad_guard_failure_summary": {
            "blockers": guard_ranking.get("blockers"),
            "seed_action_retention": broad_guard_seed_retention,
            "pair_qty_retention": broad_guard_pair_retention,
            "residual_qty_rate_reduction": guard_ranking.get("residual_qty_rate_reduction"),
            "residual_cost_rate_reduction": broad_guard_residual_cost_reduction,
            "stress100_worst_pnl_delta": (guard_delta.get("stress100_worst_pnl") or {}).get("absolute_delta"),
            "worst_day_fee_after_pnl_delta": (guard_delta.get("worst_day_fee_after_pnl") or {}).get("absolute_delta"),
        },
        "candidate_reviews": rejected_candidates,
        "blockers": blockers,
        "exact_missing_fields_or_capabilities": exact_missing,
        "selected_next_target": selected_next_target,
        "rejected_next_directions": [
            "broad portfolio-negative risk guard",
            "dynamic_condition_ledger_risk_state_gate",
            "risk_increasing_pair_fill_cap_v1",
            "ledger_tail_first_pair_priority_v1",
            "exact dust-opposite sizer",
            "price/public-L1 caps",
            "summary-only removal",
            "static side/offset deletion",
            "cooldown/admission cap",
            "fill-to-balance or portfolio-ledger shadow revival",
            "broad replay search",
            "shadow/canary/private truth/promotion claims",
        ],
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "interpretation": (
                "The public-profile/portfolio/source-link lead remains active, but the current same-run diagnostics "
                "do not yet expose a safe, pre-action, sample-preserving local mechanism after the broad guard discard."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "STATUS_HYGIENE_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "ssh_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": next_action,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--completion-manifest", type=Path, required=True)
    parser.add_argument("--guard-verifier-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = review(
        args.completion_manifest,
        args.guard_verifier_manifest,
        load_json(args.completion_manifest),
        load_json(args.guard_verifier_manifest),
    )
    write_json(args.output, output)
    print(json.dumps({"decision": output["decision"], "decision_label": output["decision_label"], "output": str(args.output)}))
    return 0 if output["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
