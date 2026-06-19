#!/usr/bin/env python3
"""Plan residual-zero stress evidence before capacity expansion.

This local-only helper explains the residual-zero stress caveat using current
cap25 evidence. It does not launch remote jobs, approve deploy, approve live
orders, or relax source/economic gates.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def status(card: dict[str, Any]) -> str | None:
    raw = card.get("status")
    return str(raw) if raw else None


def is_keep(card: dict[str, Any]) -> bool:
    return (status(card) or "").startswith("KEEP")


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def stage_factor(stage: dict[str, Any], current_round_notional: float | None) -> float | None:
    stage_notional = fnum(stage.get("round_redeem_notional"))
    if stage_notional is None or not current_round_notional:
        return None
    return stage_notional / current_round_notional


def current_stage_plan(capacity_plan: dict[str, Any], current_stage: str) -> dict[str, Any]:
    for stage in listify(capacity_plan.get("capacity_ladder")):
        if isinstance(stage, dict) and stage.get("stage") == current_stage:
            return stage
    for stage in listify(capacity_plan.get("capacity_ladder")):
        if isinstance(stage, dict):
            return stage
    return {}


def projected_stress(
    capacity_plan: dict[str, Any],
    current_stage: str,
    pair_pnl: float,
    residual_cost: float,
    worst_case: float,
) -> list[dict[str, Any]]:
    current_stage_body = current_stage_plan(capacity_plan, current_stage)
    current_notional = fnum(current_stage_body.get("round_redeem_notional"))
    rows: list[dict[str, Any]] = []
    for stage in listify(capacity_plan.get("capacity_ladder")):
        if not isinstance(stage, dict):
            continue
        factor = stage_factor(stage, current_notional)
        projected_pair_pnl = pair_pnl * factor if factor is not None else None
        projected_residual_cost = residual_cost * factor if factor is not None else None
        projected_worst = worst_case * factor if factor is not None else None
        rows.append(
            {
                "stage": stage.get("stage"),
                "round_redeem_notional": stage.get("round_redeem_notional"),
                "scale_factor_vs_current": factor,
                "linear_projection_only": True,
                "projected_pair_pnl": projected_pair_pnl,
                "projected_residual_cost": projected_residual_cost,
                "projected_worst_case_pair_pnl_if_residual_zero": projected_worst,
                "residual_zero_pass_under_projection": projected_worst is not None and projected_worst >= 0.0,
            }
        )
    return rows


def build_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    stress = card["current_residual_zero_stress"]
    lines = [
        "# Xuan Shadow Review Residual-Zero Stress Capacity Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- residual_zero_stress_plan_ready: `{decision['residual_zero_stress_plan_ready']}`",
        f"- residual_zero_current_stage_pass: `{decision['residual_zero_current_stage_pass']}`",
        f"- residual_zero_capacity_remote_rationale_ready: `{decision['residual_zero_capacity_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_plan: `{decision['future_remote_allowed_by_this_plan']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        f"- next_action: `{decision['next_action']}`",
        "",
        "## Current Stress",
        "",
        f"- pair PnL: `{stress['pair_pnl']}`",
        f"- residual cost: `{stress['residual_cost']}`",
        f"- worst case PnL if residual zero: `{stress['worst_case_pair_pnl_if_residual_zero']}`",
        f"- max residual cost for nonnegative stress: `{stress['max_residual_cost_for_nonnegative_stress']}`",
        f"- required residual cost reduction for zero stress: `{stress['required_residual_cost_reduction_for_zero_stress']}`",
        f"- required residual cost reduction share: `{stress['required_residual_cost_reduction_share']}`",
        f"- required pair PnL increase for zero stress: `{stress['required_pair_pnl_increase_for_zero_stress']}`",
        "",
        "## Capacity Projection",
        "",
        "| Stage | Factor | Projected pair PnL | Projected residual cost | Projected zero-stress PnL | Pass |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in card["capacity_linear_stress_projection"]:
        lines.append(
            f"| `{row['stage']}` | `{row['scale_factor_vs_current']}` | `{row['projected_pair_pnl']}` | "
            f"`{row['projected_residual_cost']}` | `{row['projected_worst_case_pair_pnl_if_residual_zero']}` | "
            f"`{row['residual_zero_pass_under_projection']}` |"
        )
    lines.extend(
        [
            "",
            "## Residual Tail Link",
            "",
            f"- residual tail primary blocker: `{card['residual_tail_context']['residual_tail_primary_blocker']}`",
            f"- hardest residual gate: `{card['residual_tail_context']['hardest_residual_gap']}`",
            f"- zero-stress reduction is weaker than capacity residual gate: `{card['residual_tail_context']['zero_stress_reduction_weaker_than_capacity_gate']}`",
            f"- residual cost after capacity hard gate: `{card['residual_tail_context']['residual_cost_after_cost_to_pair_qty_gate']}`",
            f"- stress PnL after capacity hard gate: `{card['residual_tail_context']['stress_pnl_after_cost_to_pair_qty_gate']}`",
            "",
            "## Required Before Future Remote",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in decision["required_before_future_remote"])
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- This is a local stress plan, not remote authorization.",
            "- Linear capacity projection is a stress illustration only, not runtime capacity evidence.",
            "- Do not run cap75 while cap25 residual-zero and residual capacity gates remain blocked.",
            "- Do not deploy, restart, send live orders, or mutate shared services.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    residual_tail = load_json(args.residual_tail_gap_scorecard)
    capital = load_json(args.capital_roi_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    capacity_plan = load_json(args.capacity_ladder_evidence_plan_scorecard)
    caveat_gap = load_json(args.caveat_gap_scorecard)

    runtime_metrics = body(runtime, "metrics")
    capital_aggregate = body(capital, "aggregate")
    capital_round = body(capital_aggregate, "round_roi")
    capital_totals = body(capital_aggregate, "totals")
    tail_decision = body(residual_tail, "decision")
    tail_gaps = body(residual_tail, "residual_gaps")
    caveat_decision = body(caveat_gap, "decision")

    pair_pnl = fnum(runtime_metrics.get("pair_pnl"), fnum(capital_totals.get("pair_pnl"), 0.0)) or 0.0
    residual_cost = fnum(runtime_metrics.get("residual_cost"), fnum(capital_totals.get("residual_cost"), 0.0)) or 0.0
    residual_qty = fnum(runtime_metrics.get("residual_qty"), fnum(capital_totals.get("residual_qty"), 0.0)) or 0.0
    pair_qty = fnum(capital_totals.get("pair_qty"), 0.0) or 0.0
    worst_case = fnum(capital_round.get("worst_case_pair_pnl_if_residual_zero"), pair_pnl - residual_cost)
    if worst_case is None:
        worst_case = pair_pnl - residual_cost
    required_reduction = max(0.0, -worst_case)
    required_pnl_increase = max(0.0, residual_cost - pair_pnl)
    max_residual_cost_for_zero = pair_pnl
    residual_cost_to_pair_pnl = residual_cost / pair_pnl if pair_pnl > 0 else None
    reduction_share = required_reduction / residual_cost if residual_cost > 0 else None
    pnl_increase_share = required_pnl_increase / pair_pnl if pair_pnl > 0 else None

    cost_to_pair_qty_gap = body(tail_gaps, "residual_cost_to_pair_qty_hard")
    cost_to_pair_qty_allowed = fnum(cost_to_pair_qty_gap.get("max_allowed_value"))
    cost_to_pair_qty_required_reduction = fnum(cost_to_pair_qty_gap.get("required_reduction"), 0.0) or 0.0
    stress_pnl_after_cost_to_pair_qty_gate = (
        pair_pnl - cost_to_pair_qty_allowed if cost_to_pair_qty_allowed is not None else None
    )
    zero_weaker_than_capacity_gate = cost_to_pair_qty_required_reduction >= required_reduction

    residual_zero_pass = worst_case >= args.min_residual_zero_stress_pnl
    capacity_projection = projected_stress(
        capacity_plan=capacity_plan,
        current_stage=args.current_stage,
        pair_pnl=pair_pnl,
        residual_cost=residual_cost,
        worst_case=worst_case,
    )
    capacity_expansion_blockers = listify(tail_decision.get("capacity_expansion_blockers"))
    essential_inputs_ready = bool(runtime_metrics) and bool(capital_round) and is_keep(residual_tail)
    status_value = (
        "KEEP_SHADOW_REVIEW_RESIDUAL_ZERO_STRESS_CAPACITY_PLAN_READY_LOCAL_ONLY"
        if essential_inputs_ready
        else "UNKNOWN_SHADOW_REVIEW_RESIDUAL_ZERO_STRESS_CAPACITY_PLAN_INPUT_BLOCKED_LOCAL_ONLY"
    )
    required_before_future_remote = [
        "current cap25 residual-zero stress must be nonnegative or bounded by a local size-control proof",
        "current cap25 residual/public residual gates must be resolved or explicitly bounded",
        "local tail mitigation target must cover both residual-zero reduction and residual_cost_to_pair_qty gate",
        "capacity projection must be replaced by fresh bounded runtime evidence before cap75/cap150/cap300 promotion",
        "manifest builder and verifier must KEEP on the future heartbeat",
        "future run, if separately justified, must be one bounded PM_DRY_RUN no-order run with duration_s<=1800",
    ]

    return rounded(
        {
            "artifact": "xuan_shadow_review_residual_zero_stress_capacity_plan",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_residual_zero_stress_capacity_plan.py",
            "status": status_value,
            "inputs": {
                "residual_tail_gap_scorecard": args.residual_tail_gap_scorecard,
                "capital_roi_scorecard": args.capital_roi_scorecard,
                "runtime_summary_scorecard": args.runtime_summary_scorecard,
                "capacity_ladder_evidence_plan_scorecard": args.capacity_ladder_evidence_plan_scorecard,
                "caveat_gap_scorecard": args.caveat_gap_scorecard,
            },
            "source_statuses": {
                "residual_tail_gap": status(residual_tail),
                "capital_roi": status(capital),
                "runtime_summary": status(runtime),
                "capacity_ladder_evidence_plan": status(capacity_plan),
                "caveat_gap": status(caveat_gap),
            },
            "decision": {
                "residual_zero_stress_plan_ready": status_value.startswith("KEEP"),
                "research_shadow_review_ready": caveat_decision.get("research_shadow_review_ready") is True,
                "paper_shadow_only": True,
                "research_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "remote_runner_allowed": False,
                "future_remote_allowed_by_this_plan": False,
                "residual_zero_current_stage_pass": residual_zero_pass,
                "residual_zero_capacity_remote_rationale_ready": False,
                "capacity_expansion_blockers": capacity_expansion_blockers,
                "hard_blockers": [] if essential_inputs_ready else ["residual_zero_stress_inputs_missing"],
                "required_before_future_remote": required_before_future_remote,
                "next_action": "build_local_tail_mitigation_hypothesis_or_size_control_before_any_capacity_remote",
            },
            "current_residual_zero_stress": {
                "pair_pnl": pair_pnl,
                "residual_cost": residual_cost,
                "residual_qty": residual_qty,
                "pair_qty": pair_qty,
                "worst_case_pair_pnl_if_residual_zero": worst_case,
                "min_residual_zero_stress_pnl": args.min_residual_zero_stress_pnl,
                "max_residual_cost_for_nonnegative_stress": max_residual_cost_for_zero,
                "required_residual_cost_reduction_for_zero_stress": required_reduction,
                "required_residual_cost_reduction_share": reduction_share,
                "required_pair_pnl_increase_for_zero_stress": required_pnl_increase,
                "required_pair_pnl_increase_share": pnl_increase_share,
                "residual_cost_to_pair_pnl": residual_cost_to_pair_pnl,
                "residual_zero_pass": residual_zero_pass,
            },
            "residual_tail_context": {
                "residual_tail_primary_blocker": tail_decision.get("residual_tail_primary_blocker"),
                "hardest_residual_gap": tail_decision.get("hardest_residual_gap"),
                "residual_cost_to_pair_qty_gate_required_reduction": cost_to_pair_qty_required_reduction,
                "residual_cost_after_cost_to_pair_qty_gate": cost_to_pair_qty_allowed,
                "stress_pnl_after_cost_to_pair_qty_gate": stress_pnl_after_cost_to_pair_qty_gate,
                "zero_stress_reduction_weaker_than_capacity_gate": zero_weaker_than_capacity_gate,
            },
            "capacity_linear_stress_projection": capacity_projection,
            "interpretation": {
                "current_cap25_zero_stress_negative": not residual_zero_pass,
                "cap75_remote_not_justified": True,
                "linear_projection_is_not_runtime_capacity_evidence": True,
                "capacity_gate_stricter_than_zero_stress": zero_weaker_than_capacity_gate,
                "do_not_relax_surplus_or_source_gates": True,
                "do_not_run_remote_from_this_plan": True,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--residual-tail-gap-scorecard", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--runtime-summary-scorecard", required=True)
    parser.add_argument("--capacity-ladder-evidence-plan-scorecard", required=True)
    parser.add_argument("--caveat-gap-scorecard", required=True)
    parser.add_argument("--current-stage", default="cap_25")
    parser.add_argument("--min-residual-zero-stress-pnl", type=float, default=0.0)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    scorecard = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(build_markdown(scorecard) + "\n")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if scorecard["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
