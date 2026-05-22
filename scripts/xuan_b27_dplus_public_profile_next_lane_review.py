#!/usr/bin/env python3
"""Review whether public-profile leads expose a new safe local D+ lane.

This is a local-only status-hygiene reader. It consumes prior manifests and
does not fetch network data, read events JSONL, inspect raw/replay stores, or
touch live/order/broker/shared-ingress paths.
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
    "raw/replay",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path | None) -> bool:
    if path is None:
        return True
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


def profile_rows(public_profile_manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows = public_profile_manifest.get("profiles_reviewed")
    return rows if isinstance(rows, list) else []


def profile_label(row: dict[str, Any]) -> str:
    for key in ("profile_classification", "classification"):
        value = row.get(key)
        if isinstance(value, dict) and value.get("profile_label"):
            return str(value["profile_label"])
    return ""


def lane_report(
    *,
    public_profile_manifest: dict[str, Any],
    pairing_manifest: dict[str, Any],
    intersection_manifest: dict[str, Any],
    post_sizer_gap_manifest: dict[str, Any],
) -> dict[str, Any]:
    profiles = profile_rows(public_profile_manifest)
    positive_profiles = [
        row
        for row in profiles
        if profile_label(row).startswith("STRONG_PUBLIC_PROXY_POSITIVE")
    ]
    negative_controls = [
        row
        for row in profiles
        if profile_label(row).startswith("NEGATIVE_CONTROL")
    ]
    best_delta = pairing_manifest.get("best_rule_delta_vs_control")
    best_delta = best_delta if isinstance(best_delta, dict) else {}
    residual_qty_reduction = as_float(best_delta.get("residual_qty_rate_reduction"))
    residual_cost_reduction = as_float(best_delta.get("residual_cost_rate_reduction"))
    seed_retention = as_float(best_delta.get("seed_action_retention"))
    pair_retention = as_float(best_delta.get("pair_qty_retention"))
    stress_delta = as_float((best_delta.get("stress100_worst_pnl") or {}).get("absolute_delta"))
    profile_lead_active = bool(positive_profiles) and residual_qty_reduction >= 0.20 and residual_cost_reduction >= 0.20

    intersection_missing = intersection_manifest.get("missing_files_or_fields")
    intersection_missing = intersection_missing if isinstance(intersection_missing, list) else []
    complete_set_gap_status = str(post_sizer_gap_manifest.get("decision_label") or post_sizer_gap_manifest.get("status"))

    candidate_lanes = [
        {
            "lane": "public_profile_complete_set_price_or_cost_gate",
            "decision": "DISCARD",
            "reason": "Would become price/cost cap family or static complete-set edge threshold; this family is frozen without a new non-price mechanism.",
        },
        {
            "lane": "public_profile_two_sided_condition_filter",
            "decision": "DISCARD",
            "reason": "Would become summary-only condition removal/static side-offset deletion because public profiles lack pre-action live queue state.",
        },
        {
            "lane": "complete_set_buffer_or_target_sizer",
            "decision": "DISCARD",
            "reason": (
                "Exact dust-opp +1 sizer was already discarded; post-sizer review says smaller buffers break "
                "sample/order-minimum proxy and larger buffers are ineffective."
            ),
        },
        {
            "lane": "portfolio_risk_adjusted_nonnegative_intersection",
            "decision": "UNKNOWN",
            "reason": (
                "This is still the best research lead, but current pullbacks do not contain portfolio-ledger, "
                "source-link-transition, and residual-tail-exemplar surfaces in the same run."
            ),
            "missing": intersection_missing,
        },
    ]

    missing_capabilities = [
        "same-window source_sequence_id / quote_intent_id / source_order_id joined to portfolio ledger context",
        "runner-observed pending/opposite order availability at the source action",
        "action-level post-seed close/rescue linkage after late dust-opposite seeds",
        "one no-order shadow output containing portfolio_ledger_diagnostics + source_link_transition_diagnostics + source_link_residual_tail_exemplars",
        "pre-action separator between residual-reducing and profitable-paired cases that does not use future source_pair/source_residual attribution",
    ]

    if profile_lead_active and intersection_missing:
        decision_label = "UNKNOWN_PUBLIC_PROFILE_LEAD_ACTIVE_BUT_NO_NEW_LOCAL_LANE"
        decision = "UNKNOWN"
        next_action = (
            "Wait for exact main-thread approval before running late_repair90_portfolio_ledger_source_link_exemplar_no_order_review; "
            "that diagnostic must enable portfolio-ledger, source-link-transition, and residual-tail-exemplar summaries together."
        )
    elif profile_lead_active:
        decision_label = "KEEP_PUBLIC_PROFILE_INTERSECTION_READY_FOR_LOCAL_VERIFIER_SELECTION"
        decision = "KEEP"
        next_action = (
            "Use the complete intersection scorer output to nominate one local-only causal verifier target; "
            "do not convert public-profile buckets directly into trading rules."
        )
    else:
        decision_label = "UNKNOWN_PUBLIC_PROFILE_SIGNAL_OR_LOCAL_REDUCTION_INSUFFICIENT"
        decision = "UNKNOWN"
        next_action = "No new local lane; gather stronger public/proxy or same-window action-level evidence."

    return {
        "artifact": "xuan_b27_dplus_public_profile_next_lane_review",
        "created_utc": utc_now(),
        "schema_version": "public_profile_next_lane_review_v1",
        "decision": decision,
        "decision_label": decision_label,
        "scope": "local_only_status_hygiene",
        "public_profile_summary": {
            "profiles_reviewed": len(profiles),
            "positive_proxy_profile_count": len(positive_profiles),
            "negative_control_profile_count": len(negative_controls),
            "best_positive_wallet": public_profile_manifest.get("cross_profile_summary", {}).get("best_positive_profile_wallet"),
            "profile_lead_active": profile_lead_active,
        },
        "local_pairing_verifier_summary": {
            "best_rule": pairing_manifest.get("best_rule"),
            "seed_action_retention": round(seed_retention, 6),
            "pair_qty_retention": round(pair_retention, 6),
            "residual_qty_rate_reduction": round(residual_qty_reduction, 6),
            "residual_cost_rate_reduction": round(residual_cost_reduction, 6),
            "stress100_worst_pnl_delta": round(stress_delta, 6),
            "promotion_gate_passed": bool(pairing_manifest.get("promotion_gate", {}).get("passed")),
        },
        "current_intersection_status": {
            "decision_label": intersection_manifest.get("decision_label"),
            "status": intersection_manifest.get("status"),
            "missing_files_or_fields": intersection_missing,
        },
        "complete_set_post_sizer_status": {
            "decision_label": complete_set_gap_status,
            "exact_sizer_reusable": False,
        },
        "candidate_lane_reviews": candidate_lanes,
        "missing_fields_or_capabilities_for_new_local_lane": missing_capabilities,
        "guardrails": {
            "no_remote_without_exact_approval": True,
            "no_events_jsonl_read": True,
            "no_raw_replay_or_full_store_scan": True,
            "public_profiles_are_proxy_only": True,
            "no_price_or_cap_family": True,
            "no_static_side_offset_deletion": True,
            "no_summary_only_removal": True,
            "no_cooldown_or_admission_cap": True,
            "no_fill_to_balance_revival": True,
            "no_portfolio_ledger_trading_rule_from_diagnostics": True,
            "no_canary_or_promotion_claim": True,
        },
        "research_ranking": {
            "decision": "PUBLIC_PROFILE_COMPLETE_SET_LEDGER_LEAD_REMAINS_ACTIVE"
            if profile_lead_active
            else "PUBLIC_PROFILE_SIGNAL_INSUFFICIENT",
            "interpretation": (
                "The strong public profile supports condition-level complete-set/paired-inventory structure, "
                "but it does not add a new deployable local control without same-window action-level diagnostics."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "canary_ready": False,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": next_action,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-profile-manifest", type=Path, required=True)
    parser.add_argument("--pairing-verifier-manifest", type=Path, required=True)
    parser.add_argument("--intersection-manifest", type=Path, required=True)
    parser.add_argument("--post-sizer-gap-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    paths = [
        args.public_profile_manifest,
        args.pairing_verifier_manifest,
        args.intersection_manifest,
        args.post_sizer_gap_manifest,
        args.output_dir,
    ]
    unsafe = [str(path) for path in paths if not path_safe(path)]
    if unsafe:
        raise SystemExit(f"refusing forbidden paths: {unsafe}")

    report = lane_report(
        public_profile_manifest=load_json(args.public_profile_manifest),
        pairing_manifest=load_json(args.pairing_verifier_manifest),
        intersection_manifest=load_json(args.intersection_manifest),
        post_sizer_gap_manifest=load_json(args.post_sizer_gap_manifest),
    )
    write_json(args.output_dir / "manifest.json", report)
    print(json.dumps({"decision": report["decision"], "decision_label": report["decision_label"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
