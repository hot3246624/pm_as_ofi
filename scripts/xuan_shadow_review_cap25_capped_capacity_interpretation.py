#!/usr/bin/env python3
"""Interpret capped cap25 density readiness versus capacity blockers.

This local-only artifact reconciles the source-blip-aware density pass with the
still-blocked residual/capacity gates. It does not authorize remote runs,
capacity expansion, deployment, live orders, or private-truth claims.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


RUN_TAG = "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z"
DEFAULT_PACKET = Path(".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_research_packet_20260526T1845Z.json")
DEFAULT_DENSITY_GATE = Path(".tmp_xuan/scorecards/xuan_shadow_review_source_blip_aware_density_gate_20260526T1840Z.json")
DEFAULT_STAGE_GATE = Path(f".tmp_xuan/scorecards/no_order_{RUN_TAG}_capacity_stage_public_benchmark_gate.json")
DEFAULT_RESIDUAL_TAIL = Path(".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_residual_tail_gap_20260526T1910Z.json")
DEFAULT_RESIDUAL_ZERO = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_residual_zero_stress_capacity_plan_20260526T1915Z.json"
)
DEFAULT_PUBLIC_04B6 = Path(".tmp_xuan/scorecards/xuan_public_04b6_current_proxy_review_20260526T1802Z.json")
DEFAULT_PRIVATE_TRUTH = Path(".tmp_xuan/scorecards/xuan_shadow_review_private_truth_request_20260526T0824Z.json")
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_capacity_interpretation_20260526T1915Z.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    "xuan_shadow_review_cap25_capped_capacity_interpretation_20260526T1915Z/"
    "CAP25_CAPPED_CAPACITY_INTERPRETATION.md"
)


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def gap_passed(residual_tail: dict[str, Any], name: str) -> bool:
    return body(body(residual_tail, "residual_gaps"), name).get("passed") is True


def gap_value(residual_tail: dict[str, Any], name: str, field: str) -> float | None:
    return fnum(body(body(residual_tail, "residual_gaps"), name).get(field))


def first_slug_by_cost(residual_tail: dict[str, Any]) -> dict[str, Any]:
    rows = listify(body(residual_tail, "residual_tail_diagnosis").get("top_slug_by_cost"))
    for row in rows:
        if isinstance(row, dict):
            return row
    return {}


def build(args: argparse.Namespace) -> dict[str, Any]:
    packet = load_json(args.cap25_research_packet_scorecard)
    density_gate = load_json(args.source_blip_density_gate_scorecard)
    stage_gate = load_json(args.capacity_stage_gate_scorecard)
    residual_tail = load_json(args.residual_tail_gap_scorecard)
    residual_zero = load_json(args.residual_zero_stress_scorecard)
    public_04b6 = load_json(args.public_04b6_scorecard)
    private_truth = load_json(args.private_truth_scorecard)

    packet_decision = body(packet, "decision")
    density_decision = body(density_gate, "decision")
    stage_decision = body(stage_gate, "decision")
    stage_metrics = body(stage_gate, "metrics")
    tail_decision = body(residual_tail, "decision")
    tail_metrics = body(residual_tail, "current_stage_metrics")
    tail_interp = body(residual_tail, "interpretation")
    tail_lots = body(residual_tail, "residual_tail_diagnosis")
    zero_decision = body(residual_zero, "decision")
    zero_stress = body(residual_zero, "current_residual_zero_stress")
    public_pair = body(public_04b6, "paired_5m_15m_false")
    private_decision = body(private_truth, "decision")

    source_blip_density_ready = (
        is_keep(density_gate)
        and density_decision.get("source_blip_aware_density_pass") is True
        and density_decision.get("cap25_research_review_ready") is True
        and not listify(density_decision.get("hard_blockers"))
    )
    packet_ready = is_keep(packet) and packet_decision.get("cap25_capped_research_packet_ready") is True
    current_stage_gate_pass = stage_decision.get("capacity_stage_gate_pass") is True
    residual_qty_hard_pass = gap_passed(residual_tail, "residual_qty_share_hard")
    public_residual_hard_pass = gap_passed(residual_tail, "public_residual_rate_hard")
    residual_cost_hard_pass = gap_passed(residual_tail, "residual_cost_share_hard")
    residual_cost_to_pair_pass = gap_passed(residual_tail, "residual_cost_to_pair_qty_hard")
    zero_stress_pass = zero_decision.get("residual_zero_current_stage_pass") is True
    economics_source_clean = tail_interp.get("economics_source_safety_otherwise_clean") is True
    mature_material_tail = tail_interp.get("residual_tail_is_not_young_or_tiny_noise") is True

    essential_inputs_ready = (
        packet_ready
        and source_blip_density_ready
        and bool(stage_metrics)
        and is_keep(residual_tail)
        and is_keep(residual_zero)
        and is_keep(public_04b6)
    )

    capacity_blockers = [str(item) for item in listify(tail_decision.get("capacity_expansion_blockers")) if item]
    residual_tail_acceptable_for_research = (
        essential_inputs_ready
        and economics_source_clean
        and residual_qty_hard_pass
        and public_residual_hard_pass
        and not bool(packet_decision.get("promotion_ready"))
    )
    residual_tail_blocks_new_cap25_sample = (
        not current_stage_gate_pass
        or not residual_cost_hard_pass
        or not residual_cost_to_pair_pass
        or not zero_stress_pass
        or bool(capacity_blockers)
    )

    cost_reduction_for_cost_share = gap_value(residual_tail, "residual_cost_share_hard", "required_reduction")
    cost_reduction_for_cost_to_pair = gap_value(
        residual_tail, "residual_cost_to_pair_qty_hard", "required_reduction"
    )
    cost_reduction_for_zero_stress = fnum(
        zero_stress.get("required_residual_cost_reduction_for_zero_stress")
    )
    qty_reduction_for_target = gap_value(residual_tail, "residual_qty_share_target", "required_reduction")
    needed_cost_reduction = max(
        [
            value
            for value in [
                cost_reduction_for_cost_share,
                cost_reduction_for_cost_to_pair,
                cost_reduction_for_zero_stress,
            ]
            if value is not None
        ]
        or [0.0]
    )

    hard_blockers = [] if essential_inputs_ready else ["cap25_capacity_interpretation_inputs_missing_or_not_keep"]
    status_value = (
        "KEEP_SHADOW_REVIEW_CAP25_CAPPED_CAPACITY_INTERPRETATION_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_CAP25_CAPPED_CAPACITY_INTERPRETATION_INPUT_BLOCKED_LOCAL_ONLY"
    )

    xuan_residual_cost_share = fnum(stage_metrics.get("residual_cost_share"))
    xuan_residual_qty_share = fnum(stage_metrics.get("residual_qty_share"))
    public_residual_cost_share = fnum(public_pair.get("residual_cost_share"))
    public_residual_qty_share = fnum(public_pair.get("residual_qty_share"))

    return rounded(
        {
            "artifact": "xuan_shadow_review_cap25_capped_capacity_interpretation",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_cap25_capped_capacity_interpretation.py",
            "status": status_value,
            "inputs": {
                "cap25_research_packet_scorecard": str(args.cap25_research_packet_scorecard),
                "source_blip_density_gate_scorecard": str(args.source_blip_density_gate_scorecard),
                "capacity_stage_gate_scorecard": str(args.capacity_stage_gate_scorecard),
                "residual_tail_gap_scorecard": str(args.residual_tail_gap_scorecard),
                "residual_zero_stress_scorecard": str(args.residual_zero_stress_scorecard),
                "public_04b6_scorecard": str(args.public_04b6_scorecard),
                "private_truth_scorecard": str(args.private_truth_scorecard),
            },
            "source_statuses": {
                "cap25_research_packet": status(packet),
                "source_blip_density_gate": status(density_gate),
                "capacity_stage_gate": status(stage_gate),
                "residual_tail_gap": status(residual_tail),
                "residual_zero_stress": status(residual_zero),
                "public_04b6": status(public_04b6),
                "private_truth": status(private_truth),
            },
            "decision": {
                "cap25_capacity_interpretation_ready": not hard_blockers,
                "cap25_research_review_ready": residual_tail_acceptable_for_research,
                "source_blip_aware_density_ready": source_blip_density_ready,
                "runner_density_gate_pass": bool(density_decision.get("runner_density_gate_pass")),
                "capacity_current_stage_gate_pass": current_stage_gate_pass,
                "residual_tail_acceptable_for_research_review": residual_tail_acceptable_for_research,
                "residual_tail_blocks_new_bounded_cap25_sample": residual_tail_blocks_new_cap25_sample,
                "residual_tail_blocks_capacity_expansion": residual_tail_blocks_new_cap25_sample,
                "residual_zero_current_stage_pass": zero_stress_pass,
                "private_truth_ready": private_decision.get("private_truth_ready") is True,
                "bounded_cap25_remote_rationale_ready": False,
                "cap75_remote_rationale_ready": False,
                "future_remote_allowed_by_this_interpretation": False,
                "future_remote_requires_new_local_mitigation_rationale": True,
                "same_profile_repeat_allowed": False,
                "paper_shadow_only": True,
                "research_only": True,
                "promotion_ready": False,
                "shadow_review_ready": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "hard_blockers": hard_blockers,
                "capacity_expansion_blockers": capacity_blockers,
                "next_action": "build_local_tail_mitigation_hypothesis_or_size_control_before_any_new_remote",
            },
            "density_vs_capacity_reconciliation": {
                "density_gate_scope": "runtime completion, fill/rescue density, and source-blip contamination review",
                "density_gate_read": "source-blip-aware cap25 research density is ready, but runner density remains non-promotion",
                "capacity_gate_scope": "residual cost exposure, residual cost per paired qty, and residual-zero downside",
                "why_capacity_still_fails": [
                    "residual_cost_share_hard_failed",
                    "residual_cost_to_pair_qty_hard_failed",
                    "residual_zero_stress_negative",
                    "mature_material_tail_not_young_tiny_noise",
                ],
                "key_boundary": "density readiness makes the capped cap25 run reviewable; it does not make residual capacity safe.",
            },
            "current_cap25_metrics": {
                "accepted_actions": tail_metrics.get("accepted_actions"),
                "queue_supported_fills": tail_metrics.get("queue_supported_fills"),
                "strict_rescue_closes": tail_metrics.get("strict_rescue_closes"),
                "pair_pnl": tail_metrics.get("pair_pnl"),
                "pair_qty": tail_metrics.get("pair_qty"),
                "residual_qty": tail_metrics.get("residual_qty"),
                "residual_cost": tail_metrics.get("residual_cost"),
                "residual_qty_share": tail_metrics.get("residual_qty_share"),
                "residual_cost_share": tail_metrics.get("residual_cost_share"),
                "residual_cost_to_pair_qty": tail_metrics.get("residual_cost_to_pair_qty"),
                "actual_pair_cost_after_fee": stage_metrics.get("actual_pair_cost_after_fee"),
                "roi_on_total_cash_spend_after_fee": stage_metrics.get("roi_on_total_cash_spend_after_fee"),
                "worst_case_pair_pnl_if_residual_zero": zero_stress.get(
                    "worst_case_pair_pnl_if_residual_zero"
                ),
            },
            "residual_gate_gaps": {
                "residual_qty_share_hard_pass": residual_qty_hard_pass,
                "public_residual_rate_hard_pass": public_residual_hard_pass,
                "residual_cost_share_hard_pass": residual_cost_hard_pass,
                "residual_cost_to_pair_qty_hard_pass": residual_cost_to_pair_pass,
                "residual_cost_share_required_reduction": cost_reduction_for_cost_share,
                "residual_cost_to_pair_qty_required_reduction": cost_reduction_for_cost_to_pair,
                "residual_zero_required_reduction": cost_reduction_for_zero_stress,
                "residual_qty_target_required_reduction": qty_reduction_for_target,
                "required_residual_cost_reduction_before_capacity_rationale": needed_cost_reduction,
            },
            "residual_tail_diagnosis": {
                "primary_blocker": tail_decision.get("residual_tail_primary_blocker"),
                "hardest_residual_gap": tail_decision.get("hardest_residual_gap"),
                "mature_material_tail": mature_material_tail,
                "mature_material_cost": tail_lots.get("mature_material_cost"),
                "mature_material_cost_share": tail_lots.get("mature_material_cost_share"),
                "mature_material_qty": tail_lots.get("mature_material_qty"),
                "mature_material_qty_share": tail_lots.get("mature_material_qty_share"),
                "top_two_lot_cost": tail_lots.get("top_two_lot_cost"),
                "top_two_lot_cost_share": tail_lots.get("top_two_lot_cost_share"),
                "top_slug_by_cost": first_slug_by_cost(residual_tail),
            },
            "public_04b6_proxy_context": {
                "public_proxy_only_not_private_truth": True,
                "xuan_actual_pair_cost_after_fee": stage_metrics.get("actual_pair_cost_after_fee"),
                "public_04b6_weighted_pair_cost": public_pair.get("weighted_pair_cost"),
                "xuan_residual_qty_share": xuan_residual_qty_share,
                "public_04b6_residual_qty_share": public_residual_qty_share,
                "xuan_residual_cost_share": xuan_residual_cost_share,
                "public_04b6_residual_cost_share": public_residual_cost_share,
                "xuan_pair_cost_better_than_04b6_public_proxy": (
                    fnum(stage_metrics.get("actual_pair_cost_after_fee"), 9.0)
                    < fnum(public_pair.get("weighted_pair_cost"), -1.0)
                ),
                "xuan_residual_cost_share_worse_than_04b6_public_proxy": (
                    xuan_residual_cost_share is not None
                    and public_residual_cost_share is not None
                    and xuan_residual_cost_share > public_residual_cost_share
                ),
                "read": "04b6 reinforces maker-heavy low-residual targets; it is not private truth or a hard gate.",
            },
            "required_before_future_remote": [
                "produce a local tail mitigation or size-control rationale that covers residual_cost_to_pair_qty",
                "reduce or bound residual cost by at least the required_residual_cost_reduction_before_capacity_rationale",
                "make residual-zero current-stage stress nonnegative or explicitly bounded by a size-control proof",
                "keep source-blip absorption outside runner source admission and economic gates",
                "do not repeat the same capped profile as a remote justification",
                "run manifest builder/verifier and active runner conflict checks on the future heartbeat",
                "future run, if separately justified, remains one bounded PM_DRY_RUN no-order cap25 run with duration_s<=1800",
            ],
            "interpretation": {
                "research_review_read": "cap25_capped_runtime_is_reviewable_with_explicit_residual_caveat",
                "remote_read": "no_new_remote_from_this_artifact",
                "capacity_read": "current_stage_capacity_gate_failed_due_residual_cost_tail",
                "cap75_read": "cap75_150_300_blocked",
                "private_truth_read": "historical_shadow_no_order_not_private_truth",
                "live_read": "not_deployable_not_live_authorized",
            },
        }
    )


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    metrics = card["current_cap25_metrics"]
    gaps = card["residual_gate_gaps"]
    tail = card["residual_tail_diagnosis"]
    top_slug = tail.get("top_slug_by_cost") if isinstance(tail.get("top_slug_by_cost"), dict) else {}
    public = card["public_04b6_proxy_context"]
    lines = [
        "# Xuan Cap25 Capped Capacity Interpretation",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- cap25_research_review_ready: `{decision['cap25_research_review_ready']}`",
        f"- source_blip_aware_density_ready: `{decision['source_blip_aware_density_ready']}`",
        f"- capacity_current_stage_gate_pass: `{decision['capacity_current_stage_gate_pass']}`",
        f"- residual_tail_acceptable_for_research_review: `{decision['residual_tail_acceptable_for_research_review']}`",
        f"- residual_tail_blocks_new_bounded_cap25_sample: `{decision['residual_tail_blocks_new_bounded_cap25_sample']}`",
        f"- residual_zero_current_stage_pass: `{decision['residual_zero_current_stage_pass']}`",
        f"- bounded_cap25_remote_rationale_ready: `{decision['bounded_cap25_remote_rationale_ready']}`",
        f"- cap75_remote_rationale_ready: `{decision['cap75_remote_rationale_ready']}`",
        f"- deployable/live_orders_allowed: `{decision['deployable']}` / `{decision['live_orders_allowed']}`",
        f"- capacity_expansion_blockers: `{', '.join(decision['capacity_expansion_blockers']) or 'none'}`",
        "",
        "## Why Density Passes But Capacity Fails",
        "",
        "- Source-blip-aware density only says the clean completed run is reviewable without accepted/rescue contamination.",
        "- Capacity still fails because residual cost exposure remains too large for cap25 expansion evidence.",
        f"- residual_cost_share: `{metrics['residual_cost_share']}`",
        f"- residual_cost_to_pair_qty: `{metrics['residual_cost_to_pair_qty']}`",
        f"- residual-zero stress PnL: `{metrics['worst_case_pair_pnl_if_residual_zero']}`",
        "",
        "## Residual Gap",
        "",
        f"- residual qty hard pass: `{gaps['residual_qty_share_hard_pass']}`",
        f"- public residual hard pass: `{gaps['public_residual_rate_hard_pass']}`",
        f"- residual cost hard pass: `{gaps['residual_cost_share_hard_pass']}`",
        f"- residual cost-to-pair hard pass: `{gaps['residual_cost_to_pair_qty_hard_pass']}`",
        f"- required residual cost reduction before capacity rationale: `{gaps['required_residual_cost_reduction_before_capacity_rationale']}`",
        f"- required residual qty reduction to target: `{gaps['residual_qty_target_required_reduction']}`",
        "",
        "## Tail Diagnosis",
        "",
        f"- primary blocker: `{tail['primary_blocker']}`",
        f"- hardest residual gap: `{tail['hardest_residual_gap']}`",
        f"- mature material cost/share: `{tail['mature_material_cost']}` / `{tail['mature_material_cost_share']}`",
        f"- top two lot cost/share: `{tail['top_two_lot_cost']}` / `{tail['top_two_lot_cost_share']}`",
        f"- top slug by cost: `{top_slug.get('slug')}` cost `{top_slug.get('cost')}` share `{top_slug.get('cost_share')}`",
        "",
        "## 04b6 Public Proxy Context",
        "",
        f"- xuan pair cost after fee: `{public['xuan_actual_pair_cost_after_fee']}`",
        f"- 04b6 public weighted pair cost: `{public['public_04b6_weighted_pair_cost']}`",
        f"- xuan residual qty/cost share: `{public['xuan_residual_qty_share']}` / `{public['xuan_residual_cost_share']}`",
        f"- 04b6 public residual qty/cost share: `{public['public_04b6_residual_qty_share']}` / `{public['public_04b6_residual_cost_share']}`",
        "",
        "## Required Before Future Remote",
        "",
    ]
    lines.extend(f"- {item}" for item in card["required_before_future_remote"])
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Local-only interpretation; no deploy, no live orders, no cap75/150/300.",
            "- This artifact does not authorize a new remote sample.",
            "- Historical shadow/no-order evidence remains replay/source/public-proxy only, not private truth.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cap25-research-packet-scorecard", type=Path, default=DEFAULT_PACKET)
    parser.add_argument("--source-blip-density-gate-scorecard", type=Path, default=DEFAULT_DENSITY_GATE)
    parser.add_argument("--capacity-stage-gate-scorecard", type=Path, default=DEFAULT_STAGE_GATE)
    parser.add_argument("--residual-tail-gap-scorecard", type=Path, default=DEFAULT_RESIDUAL_TAIL)
    parser.add_argument("--residual-zero-stress-scorecard", type=Path, default=DEFAULT_RESIDUAL_ZERO)
    parser.add_argument("--public-04b6-scorecard", type=Path, default=DEFAULT_PUBLIC_04B6)
    parser.add_argument("--private-truth-scorecard", type=Path, default=DEFAULT_PRIVATE_TRUTH)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown(card) + "\n", encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
