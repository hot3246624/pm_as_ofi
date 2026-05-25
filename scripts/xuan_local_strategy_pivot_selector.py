#!/usr/bin/env python3
"""Select the next local-only strategy research lane after symmetric activation.

This selector is intentionally conservative. It consumes only current-worktree
manifests and uses them to freeze or discard exhausted lanes. It may nominate a
new local-only spec/scorer target, but it never starts SSH, shadows, services,
live/canary activity, or broad raw/replay scans.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_local_strategy_pivot_selector"
CONTRACT_NAME = "xuan_local_strategy_pivot_selector_v1"

DEFAULT_PUBLIC_PROFILE_HANDOFF = Path(
    "xuan_research_artifacts/"
    "xuan_public_profile_input_blocked_handoff_20260524T125716Z/"
    "manifest.json"
)
DEFAULT_SYMMETRIC_PROJECTED_TAIL_COMPLETION = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_shadow_review_edge0075_projected_tail_completion_20260525T170159Z/"
    "manifest.json"
)
DEFAULT_CLOSED_CYCLE_COMPLETION = Path(
    "xuan_research_artifacts/"
    "xuan_shadow_review_closed_cycle_marker_completion_20260524T041958Z/"
    "manifest.json"
)
DEFAULT_SEPARATOR_STRICT = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_candidate_separator_train_holdout_audit_strict_no_order_20260523T111500Z/"
    "manifest.json"
)
DEFAULT_SOURCE_OPPORTUNITY_GAP = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_source_opportunity_gap_audit_strict_20260523T111500Z/"
    "manifest.json"
)
DEFAULT_COMPLETE_SET_GAP = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_complete_set_post_sizer_gap_review_20260522T110542Z/"
    "manifest.json"
)
DEFAULT_RESIDUAL_TAIL_COMPLETION = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_shadow_review_residual_tail_exemplar_completion_20260522T092012Z/"
    "manifest.json"
)
DEFAULT_DYNAMIC_RISK_FREEZE = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_status_hygiene_dynamic_risk_state_freeze_20260521T043551Z/"
    "manifest.json"
)

FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/replay",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def path_has_forbidden_fragment(path: Path) -> bool:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    return any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    if path_has_forbidden_fragment(path):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return None, reason
    try:
        value = read_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(value, dict):
        return None, "not_json_object"
    return value, None


def label_of(manifest: dict[str, Any] | None) -> str:
    if not isinstance(manifest, dict):
        return ""
    return str(manifest.get("decision_label") or manifest.get("status") or manifest.get("decision") or "")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_bool(value: Any) -> bool:
    return value is True


def summarize_family(
    name: str,
    manifest: dict[str, Any] | None,
    error: str | None,
    *,
    status: str,
    reason: str,
    evidence_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    if isinstance(manifest, dict):
        for key in evidence_keys:
            if key in manifest:
                evidence[key] = manifest[key]
    return {
        "name": name,
        "status": "UNAVAILABLE" if error else status,
        "decision_label": label_of(manifest),
        "load_error": error,
        "reason": error or reason,
        "strategy_evidence": False,
        "promotion_gate_passed": bool(
            isinstance(manifest, dict)
            and isinstance(manifest.get("promotion_gate"), dict)
            and manifest["promotion_gate"].get("passed") is True
        ),
        "evidence": evidence,
    }


def public_profile_family(manifest: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    label = label_of(manifest)
    blocked = label == "UNKNOWN_PUBLIC_PROFILE_INPUT_BLOCKED_SAFE_72H_SOURCE_REQUIRED"
    return summarize_family(
        "public_profile_ce25_b55_mimicability",
        manifest,
        error,
        status="DISCARD_OR_FREEZE_INPUT_BLOCKED" if blocked else "UNKNOWN_PUBLIC_PROFILE_STATE",
        reason=(
            "current worktree has no safe 72h public-profile manifest; single-day public-profile "
            "filters are frozen as feature priors only"
        )
        if blocked
        else "public-profile handoff is not in the expected input-blocked state",
        evidence_keys=("blocked_summary", "research_ranking"),
    )


def symmetric_family(manifest: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    label = label_of(manifest)
    discarded = label.startswith("DISCARD_SYMMETRIC_ACTIVATION_EDGE0075_PROJECTED_TAIL")
    return summarize_family(
        "symmetric_activation_edge0075_window20",
        manifest,
        error,
        status="DISCARD_BROAD_RESIDUAL_AND_TAIL_RISK" if discarded else "UNKNOWN_SYMMETRIC_STATE",
        reason=(
            "latest projected-tail no-order diagnostic failed p90/residual/pair-tail budgets, and "
            "projected low-residual buckets carried the largest residual cost"
        )
        if discarded
        else "latest symmetric activation completion is not the expected DISCARD state",
        evidence_keys=("research_ranking", "promotion_gate", "projected_residual_tail_attribution"),
    )


def closed_cycle_family(manifest: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    label = label_of(manifest)
    failed = label == "UNKNOWN_CLOSED_CYCLE_MARKER_NO_ORDER_REVIEW_DENOMINATOR_PRESENT_RISK_BUDGET_FAIL"
    return summarize_family(
        "closed_cycle_marker",
        manifest,
        error,
        status="DISCARD_OR_PAUSE_RISK_BUDGET_FAIL" if failed else "UNKNOWN_CLOSED_CYCLE_STATE",
        reason=(
            "same-window denominator reproduced, but sample/risk budget failed and risk-adjusted proxy was negative"
        )
        if failed
        else "closed-cycle completion is not in the expected risk-budget-fail state",
        evidence_keys=("metrics", "interpretation", "research_ranking"),
    )


def separator_family(
    strict_manifest: dict[str, Any] | None,
    strict_error: str | None,
    gap_manifest: dict[str, Any] | None,
    gap_error: str | None,
) -> dict[str, Any]:
    strict_label = label_of(strict_manifest)
    gap_label = label_of(gap_manifest)
    failed = strict_label == "UNKNOWN_SEPARATOR_TRAIN_HOLDOUT_OFFLINE_STABLE_NO_ORDER_REPRODUCTION_FAILED"
    gap_ready = gap_label == "KEEP_SOURCE_OPPORTUNITY_GAP_AUDIT_READY_MARKER_REPRODUCTION_GAP_NAMED"
    exact_gap = (
        gap_manifest.get("exact_gap_classification", [])
        if isinstance(gap_manifest, dict)
        else []
    )
    return {
        "name": "candidate_separator_micro_deficit_source_opportunity",
        "status": "DISCARD_OLD_MARKER_REPRODUCTION_FAILED" if failed and gap_ready else "UNKNOWN_SEPARATOR_STATE",
        "decision_label": strict_label,
        "gap_decision_label": gap_label,
        "load_error": strict_error or gap_error,
        "reason": (
            "offline train/holdout predicate was stable, but no-order marker denominator failed; gap audit "
            "requires a genuinely different pre-action signal family instead of threshold/cap sweeps"
        )
        if failed and gap_ready
        else "separator/gap manifests do not jointly establish the expected reproduction failure",
        "exact_gap_classification": exact_gap,
        "strategy_evidence": False,
        "promotion_gate_passed": False,
        "evidence": {
            "strict_research_ranking": strict_manifest.get("research_ranking") if isinstance(strict_manifest, dict) else None,
            "no_order_marker_availability": gap_manifest.get("no_order_marker_availability") if isinstance(gap_manifest, dict) else None,
        },
    }


def complete_set_family(manifest: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    label = label_of(manifest)
    failed = label == "UNKNOWN_COMPLETE_SET_POST_SIZER_NO_SAFE_LOCAL_MECHANISM"
    return summarize_family(
        "complete_set_dust_opp_sizer",
        manifest,
        error,
        status="DISCARD_NO_SAFE_LOCAL_MECHANISM" if failed else "UNKNOWN_COMPLETE_SET_STATE",
        reason=(
            "exact dust-opposite late repair sizer did not improve residual enough; stronger variants collapse "
            "into forbidden qty caps/static deletion or require missing action-level linkage"
        )
        if failed
        else "complete-set post-sizer gap review not in expected state",
        evidence_keys=("discarded_exact_sizer", "exact_missing_fields_or_capabilities", "rejected_next_directions"),
    )


def residual_tail_family(manifest: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    label = label_of(manifest)
    insufficient = label == "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF_SIGNAL_INSUFFICIENT"
    return summarize_family(
        "residual_tail_exemplar",
        manifest,
        error,
        status="UNKNOWN_SIGNAL_TOO_BROAD" if insufficient else "UNKNOWN_RESIDUAL_TAIL_STATE",
        reason=(
            "tail exemplar scorer was ready, but residual tail was not concentrated enough and pair-context "
            "guardrails were insufficient for a sample-preserving mechanism"
        )
        if insufficient
        else "residual-tail completion not in expected signal-insufficient state",
        evidence_keys=("shadow_trading_acceptance", "residual_tail_exemplar_score", "mechanism_selector"),
    )


def dynamic_family(manifest: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    label = label_of(manifest)
    frozen = label == "KEEP"
    return summarize_family(
        "dynamic_condition_ledger_and_prior_dplus_families",
        manifest,
        error,
        status="FROZEN_MIXED_OR_FAILED_PRIOR_FAMILIES" if frozen else "UNKNOWN_DYNAMIC_FAMILY_STATE",
        reason=(
            "fill-to-balance, portfolio-ledger, and dynamic condition ledger lines are frozen/mixed; they are "
            "not eligible as automatic strategy candidates"
        )
        if frozen
        else "dynamic/frozen-family manifest unavailable or not in expected freeze state",
        evidence_keys=("frozen_or_blocked_families", "selected_next_spec"),
    )


def source_gap_supports_rule_miner(gap_manifest: dict[str, Any] | None) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    if not isinstance(gap_manifest, dict):
        return False, ["source_opportunity_gap_manifest_missing"]
    exact_gap = gap_manifest.get("exact_gap_classification")
    exact_gap = exact_gap if isinstance(exact_gap, list) else []
    required_gap = "remaining_gap_requires_new_pre_action_signal_family_not_threshold_sweep"
    if required_gap not in exact_gap:
        blockers.append("gap_does_not_require_new_pre_action_signal_family")
    marker = gap_manifest.get("no_order_marker_availability")
    marker = marker if isinstance(marker, dict) else {}
    if not as_bool(marker.get("reason_source_coverage_available")):
        blockers.append("reason_source_coverage_unavailable")
    source_link = marker.get("source_link_presence_by_status")
    if not isinstance(source_link, dict):
        blockers.append("source_link_presence_summary_missing")
    if as_float(marker.get("source_opportunity_marker_total")) != 0.0:
        blockers.append("old_source_opportunity_marker_not_zero_unexpectedly")
    return not blockers, blockers


def select_next_lane(families: dict[str, dict[str, Any]], gap_manifest: dict[str, Any] | None) -> tuple[dict[str, Any] | None, list[str]]:
    supports_miner, miner_blockers = source_gap_supports_rule_miner(gap_manifest)
    hard_blockers: list[str] = []
    if families["symmetric_activation"]["status"] != "DISCARD_BROAD_RESIDUAL_AND_TAIL_RISK":
        hard_blockers.append("symmetric_activation_not_confirmed_discarded")
    if families["public_profile"]["status"] != "DISCARD_OR_FREEZE_INPUT_BLOCKED":
        hard_blockers.append("public_profile_not_confirmed_blocked")
    if families["candidate_separator"]["status"] != "DISCARD_OLD_MARKER_REPRODUCTION_FAILED":
        hard_blockers.append("old_separator_reproduction_failure_not_confirmed")
    if not supports_miner:
        hard_blockers.extend(miner_blockers)
    if hard_blockers:
        return None, hard_blockers
    selected = {
        "lane": "observable_pre_action_rule_miner_spec",
        "target": "observable_pre_action_rule_miner_spec_v1",
        "type": "local_only_spec_and_fixture_scorer",
        "why_this_is_new": [
            "it starts from the confirmed transfer gap instead of reviving the old micro-deficit/source-opportunity marker",
            "candidate predicates must be frozen from observable pre-action fields before holdout/no-order denominator checks",
            "same-window no-order denominator reproduction is required before any future economic diagnostic proposal",
        ],
        "required_source_inputs": [
            "current-worktree candidate_seed_outcome_separator.csv or equivalent materialized safe feature export",
            "current-worktree source-link/source-opportunity summary manifests with reason/source presence",
            "train/holdout day split declared inside the manifest",
            "no-order denominator reproduction manifest for the exact frozen predicate",
        ],
        "allowed_pre_action_feature_families": [
            "source_sequence/quote_intent/source_order presence before action",
            "admit/block reason and status buckets",
            "pre_seed same/opposite qty and cost buckets",
            "candidate qty bucket and time-to-expiry/offset bucket",
            "source risk direction and open/deficit bucket",
            "activation status only when observed before action",
        ],
        "forbidden_shortcuts": [
            "single-day public-profile filters",
            "realized pair_cost as a live criterion",
            "future source_pair/source_residual labels as live fields",
            "pure Polymarket price cap or static side/offset/timeframe deletion",
            "reviving micro-deficit, fill-to-balance, portfolio-ledger, closed-cycle, or symmetric-activation failed families",
            "starting SSH/shadow/canary/live/local agg/shared WS from selector output",
            "raw/replay/full-store scans",
        ],
        "next_local_acceptance_gates": {
            "spec_names_exact_input_contract": True,
            "fixture_good_path_passes": True,
            "missing_no_order_denominator_fails_closed": True,
            "missing_pre_action_source_fields_fails_closed": True,
            "promotion_gate_passed": False,
            "private_truth_ready": False,
            "deployable": False,
        },
    }
    return selected, []


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    public_profile, public_error = safe_load(args.public_profile_handoff, root)
    symmetric, symmetric_error = safe_load(args.symmetric_projected_tail_completion, root)
    closed_cycle, closed_cycle_error = safe_load(args.closed_cycle_completion, root)
    separator_strict, separator_error = safe_load(args.separator_strict_manifest, root)
    source_gap, source_gap_error = safe_load(args.source_opportunity_gap_manifest, root)
    complete_set, complete_set_error = safe_load(args.complete_set_gap_manifest, root)
    residual_tail, residual_tail_error = safe_load(args.residual_tail_completion, root)
    dynamic_freeze, dynamic_error = safe_load(args.dynamic_risk_freeze, root)

    families = {
        "public_profile": public_profile_family(public_profile, public_error),
        "symmetric_activation": symmetric_family(symmetric, symmetric_error),
        "closed_cycle": closed_cycle_family(closed_cycle, closed_cycle_error),
        "candidate_separator": separator_family(separator_strict, separator_error, source_gap, source_gap_error),
        "complete_set": complete_set_family(complete_set, complete_set_error),
        "residual_tail": residual_tail_family(residual_tail, residual_tail_error),
        "dynamic_and_frozen_dplus": dynamic_family(dynamic_freeze, dynamic_error),
    }
    selected, blockers = select_next_lane(families, source_gap)
    decision = "KEEP" if selected else "UNKNOWN"
    decision_label = (
        "KEEP_LOCAL_STRATEGY_PIVOT_OBSERVABLE_PRE_ACTION_RULE_MINER_SELECTED"
        if selected
        else "UNKNOWN_LOCAL_STRATEGY_PIVOT_NO_LEGAL_NEXT_LANE"
    )
    next_action = (
        "Implement observable_pre_action_rule_miner_spec_v1 local-only: define the exact safe input manifest, "
        "train/holdout/no-order-denominator gates, fixture scorer, and fail-closed behavior; do not start no-order diagnostics."
        if selected
        else "Stop automatic strategy advancement until a safe current-worktree source can support a new observable pre-action mechanism."
    )
    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "lane": "local_strategy_pivot_selector",
        "source_inputs": {
            "public_profile_handoff": str(args.public_profile_handoff),
            "public_profile_error": public_error,
            "symmetric_projected_tail_completion": str(args.symmetric_projected_tail_completion),
            "symmetric_error": symmetric_error,
            "closed_cycle_completion": str(args.closed_cycle_completion),
            "closed_cycle_error": closed_cycle_error,
            "separator_strict_manifest": str(args.separator_strict_manifest),
            "separator_error": separator_error,
            "source_opportunity_gap_manifest": str(args.source_opportunity_gap_manifest),
            "source_opportunity_gap_error": source_gap_error,
            "complete_set_gap_manifest": str(args.complete_set_gap_manifest),
            "complete_set_error": complete_set_error,
            "residual_tail_completion": str(args.residual_tail_completion),
            "residual_tail_error": residual_tail_error,
            "dynamic_risk_freeze": str(args.dynamic_risk_freeze),
            "dynamic_error": dynamic_error,
        },
        "family_dispositions": families,
        "selected_next_lane": selected,
        "blockers": blockers,
        "next_executable_action": next_action,
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "selected_lane": selected.get("lane") if selected else None,
            "old_public_profile_line_active": False,
            "old_symmetric_activation_line_active": False,
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "All prior active strategy families remain proxy/research-only. The selected lane is a local "
                "spec/scorer target for discovering new observable pre-action predicates, not a deployable strategy."
            )
            if selected
            else "No legal next mechanism was selected from current-worktree evidence.",
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "LOCAL_PIVOT_SELECTOR_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "scope": {
            "current_worktree_only": True,
            "local_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "orders_cancels_redeems_sent": False,
            "trading_behavior_changed": False,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-profile-handoff", type=Path, default=DEFAULT_PUBLIC_PROFILE_HANDOFF)
    parser.add_argument(
        "--symmetric-projected-tail-completion",
        type=Path,
        default=DEFAULT_SYMMETRIC_PROJECTED_TAIL_COMPLETION,
    )
    parser.add_argument("--closed-cycle-completion", type=Path, default=DEFAULT_CLOSED_CYCLE_COMPLETION)
    parser.add_argument("--separator-strict-manifest", type=Path, default=DEFAULT_SEPARATOR_STRICT)
    parser.add_argument("--source-opportunity-gap-manifest", type=Path, default=DEFAULT_SOURCE_OPPORTUNITY_GAP)
    parser.add_argument("--complete-set-gap-manifest", type=Path, default=DEFAULT_COMPLETE_SET_GAP)
    parser.add_argument("--residual-tail-completion", type=Path, default=DEFAULT_RESIDUAL_TAIL_COMPLETION)
    parser.add_argument("--dynamic-risk-freeze", type=Path, default=DEFAULT_DYNAMIC_RISK_FREEZE)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
