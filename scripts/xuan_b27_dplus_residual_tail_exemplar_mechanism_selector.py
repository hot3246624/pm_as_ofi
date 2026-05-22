#!/usr/bin/env python3
"""Select a local D+ residual-tail next target from exemplar scorer output.

This script consumes only ``xuan_b27_dplus_residual_tail_exemplar_shadow_scorer``
score.json. It does not start a runner, read events JSONL, inspect replay/raw
stores, connect sockets, or propose promotion.
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
)
FROZEN_MECHANISMS = (
    "risk_increasing_pair_fill_cap_v1",
    "summary_only_removal",
    "static_side_offset_cutoff",
    "source_public_price_cap",
    "public_trade_px_cap",
    "residual_cooldown",
    "admission_cap",
    "fill_to_balance90_shadow",
    "portfolio_ledger_shadow",
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


def path_is_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def sorted_groups(score: dict[str, Any]) -> list[dict[str, Any]]:
    rankings = score.get("rankings") if isinstance(score.get("rankings"), dict) else {}
    groups = rankings.get("top_source_side_offset_risk_direction_groups")
    if not isinstance(groups, list):
        return []
    rows = [row for row in groups if isinstance(row, dict)]
    rows.sort(
        key=lambda row: (
            -as_float(row.get("source_residual_cost")),
            -as_float(row.get("source_residual_qty")),
            -as_float(row.get("source_pair_qty")),
            str(row.get("source_side_offset_risk_direction", "")),
        )
    )
    return rows


def scorer_ready(score: dict[str, Any]) -> tuple[bool, list[str]]:
    checks = score.get("checks") if isinstance(score.get("checks"), dict) else {}
    failures: list[str] = []
    required_checks = (
        "paths_safe",
        "report_shape_ok",
        "summary_paths_present",
        "aggregate_exemplars_subset_of_summaries",
        "required_field_coverage_ok",
        "source_sequence_coverage_ok",
    )
    for name in required_checks:
        if checks.get(name) is not True:
            failures.append(f"score.checks.{name}")
    if score.get("promotion_gate", {}).get("passed") is True:
        failures.append("promotion_gate_unexpectedly_passed")
    if score.get("side_effects", {}).get("events_jsonl_read") is True:
        failures.append("score_side_effects_events_jsonl_read")
    return not failures, failures


def select(score_path: Path, score: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    safe_path = path_is_safe(score_path)
    ready, ready_failures = scorer_ready(score)
    groups = sorted_groups(score)
    top = groups[0] if groups else {}
    top_residual_share = as_float(top.get("source_residual_cost_share"))
    top_pair_share = as_float(top.get("source_pair_qty_share"))
    exemplar_metrics = score.get("exemplar_metrics") if isinstance(score.get("exemplar_metrics"), dict) else {}
    top1_share = as_float(exemplar_metrics.get("top1_residual_cost_share"))
    field_coverage = as_float(exemplar_metrics.get("required_field_coverage"))
    source_sequence_coverage = as_float(exemplar_metrics.get("source_sequence_coverage"))
    risk_budget_passed = score.get("checks", {}).get("metric_budgets_ok") is True
    source_link_ok = score.get("checks", {}).get("source_link_scorer_ok") is True
    no_order_safety_ok = score.get("checks", {}).get("no_order_safety_ok") is True
    concentrated = top_residual_share >= args.min_top_group_residual_cost_share or top1_share >= args.min_top_exemplar_residual_cost_share
    sample_preserving_context_available = top_pair_share >= args.min_pair_qty_context_share
    blockers: list[str] = []
    if not safe_path:
        blockers.append("unsafe_score_path")
    blockers.extend(ready_failures)
    if not groups:
        blockers.append("no_source_side_offset_risk_direction_groups")
    if field_coverage < 1.0:
        blockers.append("required_exemplar_field_coverage_below_1")
    if source_sequence_coverage < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_threshold")
    if not concentrated:
        blockers.append("residual_tail_not_concentrated_enough")
    if not sample_preserving_context_available:
        blockers.append("top_tail_group_has_insufficient_pair_context_for_sample_preserving_rule")

    if safe_path and ready and groups and concentrated and sample_preserving_context_available:
        decision = "KEEP"
        label = "KEEP_RESIDUAL_TAIL_EXEMPLAR_LEDGER_CONTEXT_VERIFIER_SELECTED"
        selected_next_target = {
            "name": "residual_tail_ledger_context_verifier_v1",
            "lane": "causal_verifier",
            "type": "local_only_non_price_context_verifier_spec",
            "starting_signal": {
                "top_source_side_offset_risk_direction": top.get("source_side_offset_risk_direction"),
                "top_residual_cost_share": top_residual_share,
                "top_pair_qty_share": top_pair_share,
                "top1_exemplar_residual_cost_share": top1_share,
            },
            "mechanism_shape_allowed": [
                "Use pre_seed same/opp qty/cost, ledger_proxy_before/after movement, source_pair attribution, and source_residual attribution as explanatory features.",
                "Require any future rule to preserve sample and pair contribution before it can become a local verifier.",
                "Treat side/offset/risk_direction as diagnostic context, not as a standalone deletion rule.",
            ],
            "mechanism_shape_disallowed": list(FROZEN_MECHANISMS),
            "future_local_verifier_minimum_contract": {
                "must_start_from": "selected residual-tail exemplar score plus allowed candidate_base/candidate_registry/state-machine fields",
                "must_not_start_from": "replay_store_v2 broad strategy search or summary-only removal",
                "must_report": [
                    "seed_action_retention",
                    "pair_qty_retention",
                    "fee_after_pnl",
                    "residual_qty_rate_delta",
                    "residual_cost_rate_delta",
                    "stress100_worst_pnl_delta",
                    "research_ranking",
                    "promotion_gate"
                ],
                "promotion_gate_passed": False,
                "private_truth_ready": False,
                "deployable": False
            },
        }
        next_action = (
            "Implement a local-only residual_tail_ledger_context_verifier_v1 spec once a real approved exemplar shadow "
            "score confirms the same concentration; do not start remote/canary from selector output alone."
        )
    elif safe_path and ready and groups:
        decision = "UNKNOWN"
        label = "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_MECHANISM_SIGNAL_INSUFFICIENT"
        selected_next_target = {
            "name": None,
            "reason": "exemplar fields are ready, but concentration or pair-context guardrail is insufficient for a sample-preserving local mechanism",
            "top_source_side_offset_risk_direction": top.get("source_side_offset_risk_direction"),
            "top_residual_cost_share": top_residual_share,
            "top_pair_qty_share": top_pair_share,
        }
        next_action = "Wait for a real approved exemplar shadow score or gather a stronger local-only diagnostic; do not create a bucket deletion rule."
    else:
        decision = "UNKNOWN"
        label = "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SELECTOR_INPUT_GAP"
        selected_next_target = {
            "name": None,
            "reason": "score is missing required readiness fields or safe paths",
        }
        next_action = "Fix score input gaps before selecting any mechanism."

    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_residual_tail_exemplar_mechanism_selector",
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": "local_no_network_residual_tail_exemplar_next_target_selection",
        "input_score": str(score_path),
        "checks": {
            "score_path_safe": safe_path,
            "scorer_ready": ready,
            "no_order_safety_ok": no_order_safety_ok,
            "source_link_scorer_ok": source_link_ok,
            "metric_budgets_ok": risk_budget_passed,
            "required_field_coverage": field_coverage,
            "source_sequence_coverage": source_sequence_coverage,
            "top_tail_concentrated": concentrated,
            "sample_preserving_pair_context_available": sample_preserving_context_available,
        },
        "thresholds": {
            "min_top_group_residual_cost_share": args.min_top_group_residual_cost_share,
            "min_top_exemplar_residual_cost_share": args.min_top_exemplar_residual_cost_share,
            "min_pair_qty_context_share": args.min_pair_qty_context_share,
            "min_source_sequence_coverage": args.min_source_sequence_coverage,
        },
        "blockers": blockers,
        "top_groups": groups[:10],
        "selected_next_target": selected_next_target,
        "guardrails": {
            "summary_only_removal_recommended": False,
            "frozen_mechanisms_not_allowed": list(FROZEN_MECHANISMS),
            "remote_run_approved": False,
            "interpretation": (
                "This selector can choose a local verifier target only. It must not be treated as permission to start "
                "a shadow, canary, live run, order, cancel, or redeem."
            ),
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": "Mechanism selection is research-only. Economic score and promotion gate remain separate.",
        },
        "promotion_gate": {
            "passed": False,
            "status": "MECHANISM_SELECTOR_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "shadow_runner_started": False,
            "network_started": False,
            "ssh_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": next_action,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--score", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-top-group-residual-cost-share", type=float, default=0.35)
    parser.add_argument("--min-top-exemplar-residual-cost-share", type=float, default=0.35)
    parser.add_argument("--min-pair-qty-context-share", type=float, default=0.10)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = select(args.score, load_json(args.score), args)
    write_json(args.output, report)
    print(args.output)
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
