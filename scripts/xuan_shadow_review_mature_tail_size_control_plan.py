#!/usr/bin/env python3
"""Plan mature residual-tail size control from capped cap25 evidence.

This local-only planner turns the residual cost-to-pair blocker into concrete
tail-lot reduction targets. It does not authorize remote runs, deployment,
live orders, capacity expansion, or private-truth claims.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_CAPACITY_INTERPRETATION = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_capacity_interpretation_20260526T1915Z.json"
)
DEFAULT_RESIDUAL_TAIL = Path(".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_residual_tail_gap_20260526T1910Z.json")
DEFAULT_RESIDUAL_ZERO = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_residual_zero_stress_capacity_plan_20260526T1915Z.json"
)
DEFAULT_PROFILE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_profile_capped_diagnostics_20260526T1750Z.json"
)
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_size_control_plan_20260526T1938Z.json")
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_mature_tail_size_control_plan_20260526T1938Z/"
    "MATURE_TAIL_SIZE_CONTROL_PLAN.md"
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


def residual_gap(residual_tail: dict[str, Any], name: str) -> dict[str, Any]:
    return body(body(residual_tail, "residual_gaps"), name)


def top_lots(residual_tail: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [row for row in listify(body(residual_tail, "residual_tail_diagnosis").get("top_lots")) if isinstance(row, dict)]
    return sorted(rows, key=lambda row: (-(fnum(row.get("cost"), 0.0) or 0.0), str(row.get("slug") or "")))


def select_cut_lots(lots: list[dict[str, Any]], required_cost_reduction: float) -> tuple[list[dict[str, Any]], float, float]:
    selected: list[dict[str, Any]] = []
    cut_cost = 0.0
    cut_qty = 0.0
    for lot in lots:
        selected.append(lot)
        cut_cost += fnum(lot.get("cost"), 0.0) or 0.0
        cut_qty += fnum(lot.get("qty"), 0.0) or 0.0
        if cut_cost + 1e-12 >= required_cost_reduction:
            break
    return selected, cut_cost, cut_qty


def counterfactual_after_cut(
    current: dict[str, Any],
    residual_tail: dict[str, Any],
    cut_cost: float,
    cut_qty: float,
) -> dict[str, Any]:
    filled_cost = fnum(current.get("filled_cost"), 0.0) or 0.0
    filled_qty = fnum(current.get("filled_qty"), 0.0) or 0.0
    pair_qty = fnum(current.get("pair_qty"), 0.0) or 0.0
    pair_pnl = fnum(current.get("pair_pnl"), 0.0) or 0.0
    residual_cost = fnum(current.get("residual_cost"), 0.0) or 0.0
    residual_qty = fnum(current.get("residual_qty"), 0.0) or 0.0

    after_residual_cost = max(0.0, residual_cost - cut_cost)
    after_residual_qty = max(0.0, residual_qty - cut_qty)
    after_filled_cost = max(0.0, filled_cost - cut_cost)
    after_filled_qty = max(0.0, filled_qty - cut_qty)
    after_cost_share = after_residual_cost / after_filled_cost if after_filled_cost > 0 else None
    after_qty_share = after_residual_qty / after_filled_qty if after_filled_qty > 0 else None
    after_cost_to_pair_qty = after_residual_cost / pair_qty if pair_qty > 0 else None
    after_zero_stress = pair_pnl - after_residual_cost

    cost_share_threshold = fnum(residual_gap(residual_tail, "residual_cost_share_hard").get("threshold"))
    cost_to_pair_threshold = fnum(residual_gap(residual_tail, "residual_cost_to_pair_qty_hard").get("threshold"))
    qty_share_threshold = fnum(residual_gap(residual_tail, "residual_qty_share_hard").get("threshold"))
    public_hard_threshold = fnum(residual_gap(residual_tail, "public_residual_rate_hard").get("threshold"))

    return {
        "cut_cost": cut_cost,
        "cut_qty": cut_qty,
        "after_residual_cost": after_residual_cost,
        "after_residual_qty": after_residual_qty,
        "after_filled_cost": after_filled_cost,
        "after_filled_qty": after_filled_qty,
        "after_residual_cost_share": after_cost_share,
        "after_residual_qty_share": after_qty_share,
        "after_residual_cost_to_pair_qty": after_cost_to_pair_qty,
        "after_worst_case_pair_pnl_if_residual_zero": after_zero_stress,
        "residual_cost_share_hard_pass": (
            cost_share_threshold is not None and after_cost_share is not None and after_cost_share <= cost_share_threshold
        ),
        "residual_cost_to_pair_qty_hard_pass": (
            cost_to_pair_threshold is not None
            and after_cost_to_pair_qty is not None
            and after_cost_to_pair_qty <= cost_to_pair_threshold
        ),
        "residual_qty_share_hard_pass": (
            qty_share_threshold is not None and after_qty_share is not None and after_qty_share <= qty_share_threshold
        ),
        "public_residual_rate_hard_pass": (
            public_hard_threshold is not None and after_qty_share is not None and after_qty_share <= public_hard_threshold
        ),
        "residual_zero_stress_pass": after_zero_stress >= 0.0,
    }


def selected_lot_rows(lots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for lot in lots:
        rows.append(
            {
                "slug": lot.get("slug"),
                "side": lot.get("side"),
                "quote_intent_id": lot.get("quote_intent_id"),
                "source_order_id": lot.get("source_order_id"),
                "qty": fnum(lot.get("qty")),
                "px": fnum(lot.get("px")),
                "cost": fnum(lot.get("cost")),
                "residual_class": lot.get("residual_class"),
                "age_ms_at_run_end": fnum(lot.get("age_ms_at_run_end")),
            }
        )
    return rows


def build(args: argparse.Namespace) -> dict[str, Any]:
    interpretation = load_json(args.capacity_interpretation_scorecard)
    residual_tail = load_json(args.residual_tail_gap_scorecard)
    residual_zero = load_json(args.residual_zero_stress_scorecard)
    profile = load_json(args.current_profile_scorecard)

    interp_decision = body(interpretation, "decision")
    interp_gaps = body(interpretation, "residual_gate_gaps")
    tail_decision = body(residual_tail, "decision")
    tail_metrics = body(residual_tail, "current_stage_metrics")
    zero_stress = body(residual_zero, "current_residual_zero_stress")
    profile_body = body(profile, "profile")

    required_capacity_reduction = fnum(
        interp_gaps.get("required_residual_cost_reduction_before_capacity_rationale"), 0.0
    ) or 0.0
    required_zero_reduction = fnum(zero_stress.get("required_residual_cost_reduction_for_zero_stress"), 0.0) or 0.0
    required_cost_reduction = max(required_capacity_reduction, required_zero_reduction)
    lots = top_lots(residual_tail)
    selected, selected_cost, selected_qty = select_cut_lots(lots, required_cost_reduction)
    after = counterfactual_after_cut(tail_metrics, residual_tail, selected_cost, selected_qty)

    selected_covers_gap = selected_cost + 1e-12 >= required_cost_reduction
    counterfactual_capacity_pass = (
        after["residual_cost_share_hard_pass"]
        and after["residual_cost_to_pair_qty_hard_pass"]
        and after["residual_qty_share_hard_pass"]
        and after["public_residual_rate_hard_pass"]
        and after["residual_zero_stress_pass"]
    )
    mature_tail = body(residual_tail, "interpretation").get("residual_tail_is_not_young_or_tiny_noise") is True
    essential_inputs_ready = (
        is_keep(interpretation)
        and is_keep(residual_tail)
        and is_keep(residual_zero)
        and is_keep(profile)
        and interp_decision.get("residual_tail_blocks_new_bounded_cap25_sample") is True
        and bool(lots)
    )
    hard_blockers = [] if essential_inputs_ready else ["mature_tail_size_control_inputs_missing_or_not_blocked"]
    if essential_inputs_ready and not selected_covers_gap:
        hard_blockers.append("top_residual_lots_do_not_cover_required_reduction")
    if essential_inputs_ready and not mature_tail:
        hard_blockers.append("residual_tail_not_mature_material")
    status_value = (
        "KEEP_SHADOW_REVIEW_MATURE_TAIL_SIZE_CONTROL_PLAN_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_MATURE_TAIL_SIZE_CONTROL_PLAN_INPUT_BLOCKED_LOCAL_ONLY"
    )

    residual_cost_ceiling = fnum(
        residual_gap(residual_tail, "residual_cost_to_pair_qty_hard").get("max_allowed_value")
    )
    top_slug_soft_ceiling = residual_cost_ceiling / 2.0 if residual_cost_ceiling is not None else None
    min_selected_cost = min((fnum(lot.get("cost"), 0.0) or 0.0 for lot in selected), default=None)

    return rounded(
        {
            "artifact": "xuan_shadow_review_mature_tail_size_control_plan",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_mature_tail_size_control_plan.py",
            "status": status_value,
            "inputs": {
                "capacity_interpretation_scorecard": str(args.capacity_interpretation_scorecard),
                "residual_tail_gap_scorecard": str(args.residual_tail_gap_scorecard),
                "residual_zero_stress_scorecard": str(args.residual_zero_stress_scorecard),
                "current_profile_scorecard": str(args.current_profile_scorecard),
            },
            "source_statuses": {
                "capacity_interpretation": status(interpretation),
                "residual_tail_gap": status(residual_tail),
                "residual_zero_stress": status(residual_zero),
                "current_profile": status(profile),
            },
            "decision": {
                "mature_tail_size_control_plan_ready": status_value.startswith("KEEP"),
                "local_size_control_rationale_ready": status_value.startswith("KEEP") and selected_covers_gap,
                "counterfactual_selected_lots_cover_required_reduction": selected_covers_gap,
                "counterfactual_after_selected_cut_passes_capacity_and_zero_stress": counterfactual_capacity_pass,
                "candidate_profile_ready": False,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_plan": False,
                "future_remote_requires_followup_profile_and_manifest": True,
                "same_profile_repeat_allowed": False,
                "cap75_remote_rationale_ready": False,
                "capacity_expansion_allowed": False,
                "paper_shadow_only": True,
                "research_only": True,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "hard_blockers": hard_blockers,
                "next_action": "build_local_profile_or_scorer_for_large_mature_tail_lot_control_then_manifest_verify",
            },
            "required_reduction": {
                "required_residual_cost_reduction_before_capacity_rationale": required_capacity_reduction,
                "required_residual_cost_reduction_for_zero_stress": required_zero_reduction,
                "binding_required_residual_cost_reduction": required_cost_reduction,
                "residual_cost_ceiling_from_cost_to_pair_qty_gate": residual_cost_ceiling,
                "residual_cost_ceiling_from_zero_stress": fnum(zero_stress.get("max_residual_cost_for_nonnegative_stress")),
            },
            "selected_tail_cut": {
                "minimum_lots_to_cut_or_pair_complete": len(selected),
                "selected_lot_cost": selected_cost,
                "selected_lot_qty": selected_qty,
                "selected_lot_cost_share_of_current_residual_cost": (
                    selected_cost / (fnum(tail_metrics.get("residual_cost"), 0.0) or 1.0)
                ),
                "selected_lots": selected_lot_rows(selected),
            },
            "counterfactual_after_selected_cut": after,
            "current_profile_context": {
                "profile_family": profile_body.get("profile_family"),
                "tail_mitigation_hypothesis": profile_body.get("tail_mitigation_hypothesis"),
                "target_qty": profile_body.get("target_qty"),
                "fill_haircut": profile_body.get("fill_haircut"),
                "risk_seed_pair_completion_required_above_net_cap": profile_body.get(
                    "risk_seed_pair_completion_required_above_net_cap"
                ),
                "risk_seed_pair_completion_min_qty": profile_body.get("risk_seed_pair_completion_min_qty"),
                "pair_completion_net_cap": profile_body.get("pair_completion_net_cap"),
                "pair_completion_min_pair_pnl_after": profile_body.get("pair_completion_min_pair_pnl_after"),
                "strict_rescue_surplus_net_cap": profile_body.get("strict_rescue_surplus_net_cap"),
                "surplus_budget_max_abs_unpaired_cost": profile_body.get("surplus_budget_max_abs_unpaired_cost"),
                "risk_seed_cancel_on_closeability_net_cap": profile_body.get(
                    "risk_seed_cancel_on_closeability_net_cap"
                ),
            },
            "candidate_size_control_hypothesis": {
                "hypothesis_name": "cap25_large_mature_tail_lot_size_control",
                "runner_profile_ready": False,
                "requires_followup_local_profile_or_scorer": True,
                "block_or_pair_complete_lots_cost_gte": min_selected_cost,
                "per_window_total_residual_cost_ceiling": residual_cost_ceiling,
                "per_top_slug_residual_cost_soft_ceiling": top_slug_soft_ceiling,
                "large_unpaired_lot_cost_soft_ceiling": top_slug_soft_ceiling,
                "preserve_pair_completion_net_cap_lte": profile_body.get("pair_completion_net_cap"),
                "preserve_strict_rescue_surplus_net_cap_lte": profile_body.get("strict_rescue_surplus_net_cap"),
                "do_not_enable_closeability_cancel": True,
                "do_not_relax_source_quality": True,
                "do_not_relax_economic_pair_cost_gates": True,
                "do_not_repeat_same_capped_profile": True,
                "rationale": (
                    "Top two mature NO residual lots alone cover the binding residual cost reduction; "
                    "next local profile/scorer should cap or force immediate pair completion for this tail shape."
                ),
            },
            "required_before_future_remote": [
                "convert this hypothesis into a concrete local profile or scorer with runner-supported controls",
                "prove the profile would cut or pair-complete at least the selected mature residual lots without weakening source gates",
                "prove the counterfactual residual cost-to-pair, residual cost share, and residual-zero gates pass together",
                "prove fill/rescue density is not starved by the new tail-size control",
                "keep strict_rescue_surplus_net_cap <= 1.02 and do not enable closeability-cancel",
                "build a fresh manifest and verifier on a future heartbeat before any bounded cap25 PM_DRY_RUN",
                "do not run cap75/150/300 until cap25 residual/capacity/private-truth gates are jointly clear",
            ],
            "interpretation": {
                "mature_tail_size_control_read": "top_two_mature_residual_lots_are_sufficient_counterfactual_cut",
                "profile_read": "hypothesis_ready_but_not_runner_profile_or_remote_authorization",
                "remote_read": "no_remote_from_this_plan",
                "capacity_read": "capacity_blocker_has_concrete_local_cut_target",
                "live_read": "not_deployable_not_live_authorized",
            },
        }
    )


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    required = card["required_reduction"]
    selected = card["selected_tail_cut"]
    after = card["counterfactual_after_selected_cut"]
    hypothesis = card["candidate_size_control_hypothesis"]
    lines = [
        "# Xuan Mature Tail Size-Control Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- mature_tail_size_control_plan_ready: `{decision['mature_tail_size_control_plan_ready']}`",
        f"- local_size_control_rationale_ready: `{decision['local_size_control_rationale_ready']}`",
        f"- counterfactual_selected_lots_cover_required_reduction: `{decision['counterfactual_selected_lots_cover_required_reduction']}`",
        f"- counterfactual_after_selected_cut_passes_capacity_and_zero_stress: `{decision['counterfactual_after_selected_cut_passes_capacity_and_zero_stress']}`",
        f"- candidate_profile_ready: `{decision['candidate_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{decision['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_plan: `{decision['future_remote_allowed_by_this_plan']}`",
        f"- cap75_remote_rationale_ready: `{decision['cap75_remote_rationale_ready']}`",
        "",
        "## Binding Reduction",
        "",
        f"- required capacity residual cost reduction: `{required['required_residual_cost_reduction_before_capacity_rationale']}`",
        f"- required zero-stress residual cost reduction: `{required['required_residual_cost_reduction_for_zero_stress']}`",
        f"- binding required residual cost reduction: `{required['binding_required_residual_cost_reduction']}`",
        f"- residual cost ceiling from cost-to-pair gate: `{required['residual_cost_ceiling_from_cost_to_pair_qty_gate']}`",
        "",
        "## Selected Tail Cut",
        "",
        f"- minimum lots to cut or pair-complete: `{selected['minimum_lots_to_cut_or_pair_complete']}`",
        f"- selected lot cost/qty: `{selected['selected_lot_cost']}` / `{selected['selected_lot_qty']}`",
        f"- selected cost share of current residual cost: `{selected['selected_lot_cost_share_of_current_residual_cost']}`",
        "",
        "| Slug | Side | Quote | Qty | Px | Cost | Class |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for lot in selected["selected_lots"]:
        lines.append(
            f"| `{lot['slug']}` | `{lot['side']}` | `{lot['quote_intent_id']}` | "
            f"`{lot['qty']}` | `{lot['px']}` | `{lot['cost']}` | `{lot['residual_class']}` |"
        )
    lines.extend(
        [
            "",
            "## Counterfactual After Selected Cut",
            "",
            f"- after residual cost/qty: `{after['after_residual_cost']}` / `{after['after_residual_qty']}`",
            f"- after residual cost share: `{after['after_residual_cost_share']}`",
            f"- after residual cost-to-pair qty: `{after['after_residual_cost_to_pair_qty']}`",
            f"- after residual-zero stress PnL: `{after['after_worst_case_pair_pnl_if_residual_zero']}`",
            f"- residual cost share pass: `{after['residual_cost_share_hard_pass']}`",
            f"- residual cost-to-pair pass: `{after['residual_cost_to_pair_qty_hard_pass']}`",
            f"- residual-zero stress pass: `{after['residual_zero_stress_pass']}`",
            "",
            "## Candidate Hypothesis",
            "",
            f"- hypothesis_name: `{hypothesis['hypothesis_name']}`",
            f"- runner_profile_ready: `{hypothesis['runner_profile_ready']}`",
            f"- block_or_pair_complete_lots_cost_gte: `{hypothesis['block_or_pair_complete_lots_cost_gte']}`",
            f"- per_window_total_residual_cost_ceiling: `{hypothesis['per_window_total_residual_cost_ceiling']}`",
            f"- per_top_slug_residual_cost_soft_ceiling: `{hypothesis['per_top_slug_residual_cost_soft_ceiling']}`",
            f"- do_not_enable_closeability_cancel: `{hypothesis['do_not_enable_closeability_cancel']}`",
            "",
            "## Required Before Future Remote",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in card["required_before_future_remote"])
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Local-only size-control plan; no deploy, no live orders, no remote run.",
            "- This is not a runner-ready profile yet.",
            "- Do not run cap75/150/300 from this artifact.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capacity-interpretation-scorecard", type=Path, default=DEFAULT_CAPACITY_INTERPRETATION)
    parser.add_argument("--residual-tail-gap-scorecard", type=Path, default=DEFAULT_RESIDUAL_TAIL)
    parser.add_argument("--residual-zero-stress-scorecard", type=Path, default=DEFAULT_RESIDUAL_ZERO)
    parser.add_argument("--current-profile-scorecard", type=Path, default=DEFAULT_PROFILE)
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
