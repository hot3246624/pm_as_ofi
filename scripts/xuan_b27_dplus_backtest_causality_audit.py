#!/usr/bin/env python3
"""Audit D+ backtest-to-shadow causality and evidence boundaries.

This local-only audit reads committed research manifests plus selected local
source files. It does not read events JSONL, raw/replay stores, collector
stores, sockets, SSH, shared-ingress, or live trading state.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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

DEFAULT_MANIFESTS = (
    "xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_export_implementability_20260522T182613Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_micro_deficit_repair_guard_verifier_20260522T185614Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_micro_deficit_source_truth_readiness_20260522T192614Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_passive_passive_shadow_runner_micro_deficit_guard_implementability_20260522T205614Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_micro_deficit_completion_20260523T022553Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_post_micro_deficit_residual_tail_next_target_review_20260523T031823Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_complete_set_post_sizer_gap_review_20260522T110542Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_portfolio_negative_risk_increasing_source_guard_verifier_20260522T172513Z/manifest.json",
    "xuan_research_artifacts/xuan_b27_dplus_public_profile_next_lane_review_20260522T145643Z/manifest.json",
)

DEFAULT_SOURCE_FILES = (
    "scripts/xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py",
    "tools/xuan_dplus_passive_passive_shadow_runner.py",
    "scripts/xuan_b27_dplus_micro_deficit_repair_guard_verifier.py",
    "scripts/xuan_b27_dplus_micro_deficit_no_order_summary_scorer.py",
    "scripts/xuan_b27_dplus_post_micro_deficit_residual_tail_next_target_review.py",
    "scripts/xuan_b27_dplus_shadow_trading_acceptance.py",
    "docs/research/xuan/DPLUS_SCORING_CONTRACT_ZH.md",
)

POST_ACTION_LABEL_FIELDS = (
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "source_residual_age_ms",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)

PRE_ACTION_FIELDS = (
    "day",
    "condition_id",
    "slug",
    "ts_ms",
    "side",
    "offset_s",
    "trigger_px",
    "trigger_size",
    "seed_px",
    "seed_qty",
    "seed_cost",
    "official_fee_rate",
    "official_fee",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
)

RUNTIME_JOIN_FIELDS = (
    "quote_intent_id",
    "source_order_id",
    "source_sequence_id",
    "market_md_source_sequence_id",
    "trigger_source_sequence_id",
    "micro_deficit_repair_guard_candidate",
    "micro_deficit_repair_deficit",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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


def as_int(value: Any, default: int = 0) -> int:
    return int(as_float(value, float(default)))


def get_path(obj: dict[str, Any], path: tuple[str, ...], default: Any = None) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def manifest_summary(path: Path, data: dict[str, Any]) -> dict[str, Any]:
    research_ranking = data.get("research_ranking") if isinstance(data.get("research_ranking"), dict) else {}
    promotion_gate = data.get("promotion_gate") if isinstance(data.get("promotion_gate"), dict) else {}
    if "promotion_gate_passed" in data and "passed" not in promotion_gate:
        promotion_gate = {**promotion_gate, "passed": data.get("promotion_gate_passed")}
    return {
        "path": str(path),
        "artifact": data.get("artifact"),
        "decision": data.get("decision"),
        "decision_label": data.get("decision_label")
        or research_ranking.get("label")
        or data.get("label"),
        "research_ranking_decision": research_ranking.get("decision"),
        "research_ranking_label": research_ranking.get("label"),
        "promotion_gate_passed": bool(promotion_gate.get("passed", False)),
        "private_truth_ready": bool(promotion_gate.get("private_truth_ready", data.get("private_truth_ready", False))),
        "deployable": bool(promotion_gate.get("deployable", data.get("deployable", False))),
    }


def source_scan(path: Path) -> dict[str, Any]:
    text = path.read_text()
    lines = text.splitlines()
    post_action_hits: list[dict[str, Any]] = []
    policy_hits: list[dict[str, Any]] = []
    live_forbidden_conditions: list[dict[str, Any]] = []
    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()
        if any(field in line for field in POST_ACTION_LABEL_FIELDS):
            post_action_hits.append({"line": idx, "text": stripped[:180]})
        if "post_action" in line or "must not be used as live" in line or "outcome_labels_are_post_action" in line:
            policy_hits.append({"line": idx, "text": stripped[:180]})
        if path.name == "xuan_dplus_passive_passive_shadow_runner.py" and re.search(
            r"\bif\b.*source_(pair|residual)_", line
        ):
            live_forbidden_conditions.append({"line": idx, "text": stripped[:180]})
    return {
        "path": str(path),
        "line_count": len(lines),
        "post_action_label_hit_count": len(post_action_hits),
        "post_action_label_examples": post_action_hits[:8],
        "policy_hit_count": len(policy_hits),
        "policy_examples": policy_hits[:8],
        "live_forbidden_condition_count": len(live_forbidden_conditions),
        "live_forbidden_condition_examples": live_forbidden_conditions[:8],
        "contains_residual_tail_sort_key": "_residual_tail_sort_key" in text
        and "source_residual_cost" in text,
        "contains_micro_deficit_marker": "micro_deficit_repair_guard_candidate" in text,
        "contains_research_promotion_split": "research_ranking" in text and "promotion_gate" in text,
    }


def evidence_metrics(manifests: dict[str, dict[str, Any]]) -> dict[str, Any]:
    def find(fragment: str) -> dict[str, Any]:
        for path, data in manifests.items():
            artifact = str(data.get("artifact") or "")
            decision_label = str(data.get("decision_label") or "")
            if fragment in path or fragment in artifact or fragment in decision_label:
                return data
        return {}

    micro_local = find("micro_deficit_repair_guard_verifier")
    micro_shadow = find("shadow_review_micro_deficit_completion")
    post_review = find("post_micro_deficit_residual_tail_next_target_review")
    f2b_or_complete = find("complete_set_post_sizer_gap_review")
    portfolio_guard = find("portfolio_negative_risk_increasing_source_guard_verifier")

    local_rank = micro_local.get("research_ranking") if isinstance(micro_local.get("research_ranking"), dict) else {}
    shadow_metrics = micro_shadow.get("aggregate_metrics") if isinstance(micro_shadow.get("aggregate_metrics"), dict) else {}
    micro_scorer = get_path(micro_shadow, ("scorer_results", "micro_deficit_summary"), {})
    if not micro_scorer:
        micro_scorer = get_path(micro_shadow, ("scorer_results", "micro_deficit_summary_scorer"), {})
    micro_scorer_metrics = micro_scorer.get("metrics") if isinstance(micro_scorer, dict) and isinstance(micro_scorer.get("metrics"), dict) else {}
    residual_scorer = get_path(micro_shadow, ("scorer_results", "residual_tail_exemplar"), {})
    if not residual_scorer:
        residual_scorer = get_path(micro_shadow, ("scorer_results", "residual_tail_exemplar_scorer"), {})
    residual_metrics = residual_scorer.get("exemplar_metrics") if isinstance(residual_scorer, dict) and isinstance(residual_scorer.get("exemplar_metrics"), dict) else {}
    normalized_risk_budget = get_path(
        micro_shadow,
        ("scorer_results", "shadow_acceptance", "promotion_gate", "normalized_risk_budget"),
        {},
    )
    post_method = post_review.get("backtest_methodology_risk") if isinstance(post_review.get("backtest_methodology_risk"), dict) else {}

    return {
        "micro_deficit_local_verifier": {
            "decision_label": local_rank.get("label") or micro_local.get("decision_label"),
            "seed_action_retention": local_rank.get("seed_action_retention"),
            "pair_qty_retention": local_rank.get("pair_qty_retention"),
            "residual_qty_rate_reduction": local_rank.get("residual_qty_rate_reduction"),
            "residual_cost_rate_reduction": local_rank.get("residual_cost_rate_reduction"),
            "fee_after_pnl_delta": local_rank.get("fee_after_pnl_delta"),
            "stress100_worst_pnl_delta": local_rank.get("stress100_worst_pnl_delta"),
        },
        "micro_deficit_no_order_shadow": {
            "decision_label": micro_shadow.get("decision_label"),
            "candidates": shadow_metrics.get("candidates"),
            "fee_adjusted_pair_pnl_proxy": shadow_metrics.get("fee_adjusted_pair_pnl_proxy"),
            "net_pair_cost_p90": shadow_metrics.get("net_pair_cost_proxy_p90")
            or shadow_metrics.get("net_pair_cost_p90"),
            "residual_qty_share_of_filled": shadow_metrics.get("residual_qty_share_of_filled"),
            "residual_cost_share_of_filled_cost": shadow_metrics.get("residual_cost_share_of_filled_cost"),
            "pair_tail_loss_share_of_pair_pnl": shadow_metrics.get("pair_tail_loss_share_of_pair_pnl")
            or (
                normalized_risk_budget.get("pair_tail_loss_share_of_pair_pnl")
                if isinstance(normalized_risk_budget, dict)
                else None
            ),
            "strict_micro_deficit_exemplar_count": micro_scorer_metrics.get("micro_deficit_exemplar_count"),
            "runner_micro_deficit_candidates": shadow_metrics.get("micro_deficit_repair_guard_candidates"),
        },
        "residual_tail_no_order_exemplars": {
            "exemplar_count": residual_metrics.get("exemplar_count"),
            "required_field_coverage": residual_metrics.get("required_field_coverage"),
            "source_sequence_coverage": residual_metrics.get("source_sequence_coverage"),
            "quote_intent_coverage": residual_metrics.get("quote_intent_coverage"),
            "source_order_coverage": residual_metrics.get("source_order_coverage"),
        },
        "post_micro_deficit_review": {
            "decision_label": post_review.get("decision_label"),
            "methodology_risk": post_method,
            "missing_fields_or_capabilities": post_review.get("exact_missing_fields_or_capabilities"),
        },
        "complete_set_gap": {
            "decision_label": f2b_or_complete.get("decision_label"),
            "missing_fields_or_capabilities": f2b_or_complete.get("exact_missing_fields_or_capabilities"),
        },
        "portfolio_negative_guard": {
            "decision_label": portfolio_guard.get("decision_label"),
            "research_ranking": portfolio_guard.get("research_ranking"),
        },
    }


def build_checks(
    *,
    manifest_records: list[dict[str, Any]],
    source_records: list[dict[str, Any]],
    metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    live_forbidden_count = sum(as_int(row.get("live_forbidden_condition_count")) for row in source_records)
    any_promotion_passed = any(row["promotion_gate_passed"] for row in manifest_records)
    any_private_truth = any(row["private_truth_ready"] for row in manifest_records)
    any_deployable = any(row["deployable"] for row in manifest_records)

    micro_shadow = metrics["micro_deficit_no_order_shadow"]
    local = metrics["micro_deficit_local_verifier"]
    marker_reproduced = as_float(micro_shadow.get("strict_micro_deficit_exemplar_count")) > 0.0 or as_float(
        micro_shadow.get("runner_micro_deficit_candidates")
    ) > 0.0
    local_large_effect = (
        as_float(local.get("residual_qty_rate_reduction")) >= 0.20
        and as_float(local.get("residual_cost_rate_reduction")) >= 0.20
    )
    shadow_budget_ok = (
        as_float(micro_shadow.get("candidates")) >= 100.0
        and as_float(micro_shadow.get("net_pair_cost_p90")) <= 1.0
        and as_float(micro_shadow.get("residual_qty_share_of_filled")) <= 0.15
        and as_float(micro_shadow.get("residual_cost_share_of_filled_cost")) <= 0.20
        and as_float(micro_shadow.get("pair_tail_loss_share_of_pair_pnl")) <= 0.05
    )

    post_action_sorters = [
        row["path"] for row in source_records if row.get("contains_residual_tail_sort_key") or row.get("post_action_label_hit_count")
    ]
    policy_present = any(as_int(row.get("policy_hit_count")) > 0 for row in source_records)
    split_present = any(bool(row.get("contains_research_promotion_split")) for row in source_records) and not any_promotion_passed

    return [
        {
            "id": "live_rule_future_label_guard",
            "severity": "CRITICAL",
            "status": "PASS" if live_forbidden_count == 0 else "FAIL",
            "finding": (
                "No local live/no-order rule condition was found that directly gates on source_pair/source_residual labels."
                if live_forbidden_count == 0
                else "A live rule appears to branch on post-action source_pair/source_residual labels."
            ),
            "evidence": {"live_forbidden_condition_count": live_forbidden_count},
        },
        {
            "id": "selector_outcome_label_usage",
            "severity": "HIGH",
            "status": "FAIL_RESEARCH_DOWNGRADE_REQUIRED" if post_action_sorters else "PASS",
            "finding": (
                "Several target selectors/export reviews sort or score with source_pair/source_residual outcome labels. "
                "That is acceptable for offline discovery only, but any resulting backtest effect must be downgraded until "
                "a frozen pre-action predicate passes holdout and no-order marker reproduction."
            ),
            "evidence": {"paths_with_post_action_label_usage": post_action_sorters[:12]},
        },
        {
            "id": "train_holdout_freeze",
            "severity": "HIGH",
            "status": "MISSING",
            "finding": (
                "The audited manifests do not prove a train/holdout workflow where a predicate is discovered on one day set "
                "and evaluated unchanged on a disjoint day set. This explains why strong local separators can fail fresh "
                "same-window no-order review."
            ),
            "evidence": {"required": "discover_on_train_days_then_evaluate_frozen_on_holdout_days"},
        },
        {
            "id": "state_machine_to_no_order_reproduction",
            "severity": "HIGH",
            "status": "FAIL_REPRODUCTION"
            if local_large_effect and not marker_reproduced
            else ("FAIL_RISK_BUDGET" if local_large_effect and not shadow_budget_ok else "PASS_OR_NOT_APPLICABLE"),
            "finding": (
                "The micro-deficit guard had large historical residual reductions, but the approved no-order window had "
                "zero strict micro-deficit markers/candidates and failed sample/risk budgets. This is the strongest evidence "
                "that the state-machine fill/opportunity model is not sufficient as promotion evidence."
            ),
            "evidence": {
                "local_large_effect": local_large_effect,
                "no_order_marker_reproduced": marker_reproduced,
                "no_order_budget_ok": shadow_budget_ok,
                "local": local,
                "no_order": micro_shadow,
            },
        },
        {
            "id": "field_policy_present",
            "severity": "MEDIUM",
            "status": "PASS" if policy_present else "MISSING",
            "finding": (
                "The separator export already marks source_pair/source_residual labels as post-action and forbids live use. "
                "The next gap is enforcement at target-selection time."
            ),
            "evidence": {"policy_hit_present": policy_present},
        },
        {
            "id": "research_ranking_promotion_gate_split",
            "severity": "MEDIUM",
            "status": "PASS" if split_present and not (any_private_truth or any_deployable) else "FAIL",
            "finding": (
                "Audited manifests preserve research_ranking and promotion_gate separation; none claim private truth, "
                "deployable readiness, or promotion pass."
            ),
            "evidence": {
                "any_promotion_gate_passed": any_promotion_passed,
                "any_private_truth_ready": any_private_truth,
                "any_deployable": any_deployable,
            },
        },
    ]


def classify_mechanisms(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "mechanism": "candidate_seed_outcome_separator_export_v1",
            "classification": "KEEP_TOOLING_RESEARCH_ONLY_WITH_LEAKAGE_GUARD",
            "reason": (
                "It exposes exactly the fields needed for offline separator discovery, but source_pair/source_residual "
                "columns are outcome labels. It must feed a leakage-aware selector, not a live rule directly."
            ),
        },
        {
            "mechanism": "micro_deficit_repair_guard_v1",
            "classification": "DOWNGRADE_LOCAL_KEEP_TO_RESEARCH_LEAD_PENDING_REPRODUCTION",
            "reason": (
                "The local verifier used only pre-action rule fields once selected, but the target was found with "
                "post-action residual labels and the approved no-order review produced zero strict markers. Do not enable "
                "or sweep thresholds from this evidence."
            ),
            "evidence": {
                "local": metrics["micro_deficit_local_verifier"],
                "no_order": metrics["micro_deficit_no_order_shadow"],
            },
        },
        {
            "mechanism": "complete_set_pair_cost_and_public_profile_leads",
            "classification": "KEEP_PUBLIC_PROXY_INSPIRATION_ONLY",
            "reason": (
                "Public profile and complete-set context are useful for hypotheses, but they are not private order/fill "
                "truth and several local complete-set buffer/cap mechanisms lacked a safe sample-preserving pre-action rule."
            ),
        },
        {
            "mechanism": "portfolio_negative_risk_guard_family",
            "classification": "DISCARD_EXACT_BROAD_GUARD_KEEP_DIAGNOSTIC_LEAD",
            "reason": (
                "The exact broad guard collapsed sample/pair retention and worsened residual cost rate. Narrowing it from "
                "tail exemplars would require admitted/non-tail denominators and frozen holdout validation."
            ),
        },
        {
            "mechanism": "fill_to_balance90_and_price_cap_families",
            "classification": "FROZEN_OR_DISCARDED_FOR_TRANSFER_FAILURE",
            "reason": (
                "These lines either did not reproduce in no-order windows or became price/cap/static deletion families. "
                "They should not be revived without new pre-action fields."
            ),
        },
    ]


def recommendations() -> list[dict[str, Any]]:
    return [
        {
            "gate": "field_registry_enforcement",
            "action": (
                "Every exported field must be tagged as pre_action, runtime_marker, post_action_label, source_truth_ref, "
                "or public_proxy before a selector can nominate a mechanism."
            ),
        },
        {
            "gate": "train_holdout_freeze",
            "action": (
                "Discovery may use post-action labels only on train days. The proposed live predicate must then be frozen "
                "and scored on disjoint holdout days without reselecting thresholds or groups."
            ),
        },
        {
            "gate": "no_order_marker_reproduction",
            "action": (
                "Before another exact shadow question is proposed, the runner summary must expose the proposed marker and "
                "a fresh no-order window must reproduce enough marker volume and source-link coverage."
            ),
        },
        {
            "gate": "denominator_requirement",
            "action": (
                "Residual-tail exemplar groups must be joined to all admitted/non-tail candidates for the same window. "
                "Tail-only concentration cannot justify side/offset/open-qty deletion."
            ),
        },
        {
            "gate": "promotion_block",
            "action": (
                "Even after a holdout KEEP, promotion_gate remains false until no-order risk budget passes and later private "
                "order/fill/inventory truth exists. research_ranking must stay separate."
            ),
        },
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--input-manifest", type=Path, action="append", default=[])
    parser.add_argument("--source-file", type=Path, action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    manifest_paths = args.input_manifest or [root / rel for rel in DEFAULT_MANIFESTS]
    source_paths = args.source_file or [root / rel for rel in DEFAULT_SOURCE_FILES]
    manifest_paths = [path if path.is_absolute() else root / path for path in manifest_paths]
    source_paths = [path if path.is_absolute() else root / path for path in source_paths]
    output_path = args.output if args.output.is_absolute() else root / args.output

    unsafe_paths = [str(path) for path in [*manifest_paths, *source_paths, output_path] if not path_safe(path)]
    manifests: dict[str, dict[str, Any]] = {}
    missing_manifests: list[str] = []
    for path in manifest_paths:
        if not path.exists():
            missing_manifests.append(str(path))
            continue
        manifests[str(path)] = load_json(path)

    missing_sources: list[str] = []
    source_records: list[dict[str, Any]] = []
    for path in source_paths:
        if not path.exists():
            missing_sources.append(str(path))
            continue
        source_records.append(source_scan(path))

    manifest_records = [manifest_summary(Path(path), data) for path, data in manifests.items()]
    metrics = evidence_metrics(manifests)
    checks = build_checks(manifest_records=manifest_records, source_records=source_records, metrics=metrics)
    high_failures = [
        check for check in checks if check["severity"] in {"CRITICAL", "HIGH"} and check["status"] not in {"PASS"}
    ]
    live_future_label_fail = any(
        check["id"] == "live_rule_future_label_guard" and check["status"] == "FAIL" for check in checks
    )
    if unsafe_paths or live_future_label_fail:
        decision = "BLOCKED"
        decision_label = "BLOCKED_BACKTEST_CAUSALITY_AUDIT_UNSAFE_OR_LIVE_LEAKAGE"
    else:
        decision = "KEEP"
        decision_label = "KEEP_BACKTEST_CAUSALITY_AUDIT_READY_RESEARCH_EVIDENCE_DOWNGRADED"

    manifest = {
        "artifact": "xuan_b27_dplus_backtest_causality_audit_v1",
        "schema_version": "backtest_causality_audit_v1",
        "created_utc": utc_now(),
        "decision": decision,
        "decision_label": decision_label,
        "scope": {
            "local_only": True,
            "read_events_jsonl": False,
            "read_raw_replay_or_collector": False,
            "used_ssh_or_shadow": False,
            "modified_shared_ingress_or_live": False,
            "sent_orders_cancels_redeems": False,
        },
        "inputs": {
            "root": str(root),
            "manifest_count": len(manifest_records),
            "source_file_count": len(source_records),
            "missing_manifests": missing_manifests,
            "missing_sources": missing_sources,
            "unsafe_paths": unsafe_paths,
        },
        "field_registry": {
            "pre_action_or_state_machine_context": list(PRE_ACTION_FIELDS),
            "runtime_no_order_marker_or_join": list(RUNTIME_JOIN_FIELDS),
            "post_action_outcome_labels_not_live_criteria": list(POST_ACTION_LABEL_FIELDS),
            "policy": (
                "Post-action fields may rank offline hypotheses, but a deployable rule may use only pre-action/context "
                "fields and must pass frozen holdout plus no-order/private-truth gates."
            ),
        },
        "manifest_summaries": manifest_records,
        "source_static_scan": source_records,
        "evidence_metrics": metrics,
        "checks": checks,
        "mechanism_classification": classify_mechanisms(metrics),
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "high_severity_failures": [
                {"id": check["id"], "status": check["status"], "finding": check["finding"]}
                for check in high_failures
            ],
            "interpretation": (
                "The D+ research phase is not a total failure, but several strong local backtest leads must be downgraded "
                "because target discovery used post-action labels, no train/holdout freeze is proven, and the latest "
                "no-order window did not reproduce the micro-deficit marker. The correct response is to add leakage and "
                "holdout gates before selecting more mechanisms."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "METHODOLOGY_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "recommendations": recommendations(),
        "next_executable_action": (
            "Implement candidate_separator_train_holdout_audit_v1 locally: discover predicates on train days from "
            "candidate_seed_outcome_separator_export_v1, freeze one pre-action predicate, evaluate unchanged on holdout "
            "days, and require no-order marker reproduction before any new shadow question."
        ),
    }
    write_json(output_path, manifest)
    print(json.dumps({"decision": decision, "decision_label": decision_label, "output": str(output_path)}, sort_keys=True))
    return 0 if decision != "BLOCKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
