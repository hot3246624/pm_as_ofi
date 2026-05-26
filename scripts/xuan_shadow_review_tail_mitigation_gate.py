#!/usr/bin/env python3
"""Gate residual-tail mitigation before any future capacity remote.

This local-only helper compresses the residual-tail and residual-zero findings
into concrete pass/fail targets for the next decision. It does not approve
remote execution, deployment, live orders, or strategy-parameter relaxation.
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


def gap_required(gaps: dict[str, Any], name: str) -> float:
    return fnum(body(gaps, name).get("required_reduction"), 0.0) or 0.0


def gap_allowed(gaps: dict[str, Any], name: str) -> float | None:
    return fnum(body(gaps, name).get("max_allowed_value"))


def build_mitigation_levers(args: argparse.Namespace, current: dict[str, Any], targets: dict[str, Any]) -> list[dict[str, Any]]:
    current_surplus_abs = fnum(current.get("surplus_budget_max_abs_unpaired_cost"))
    target_cost = fnum(targets.get("max_residual_cost_for_remote_hard"))
    return [
        {
            "name": "local_tail_reduction_hypothesis",
            "classification": "required_next_local_work",
            "remote_ready_now": False,
            "why": "A future run needs a local hypothesis that can reduce mature-material residual cost and qty to the hard targets.",
            "minimum_target": {
                "residual_cost_lte": target_cost,
                "residual_qty_lte": targets.get("max_residual_qty_for_remote_hard"),
            },
        },
        {
            "name": "surplus_budget_tightening",
            "classification": "possible_but_unproven",
            "remote_ready_now": False,
            "why": "A lower unpaired-cost budget could bound tail cost, but no local scorer has proven it preserves fills and strict rescues.",
            "current_surplus_budget_max_abs_unpaired_cost": current_surplus_abs,
            "theoretical_budget_ceiling_for_cost_gate": target_cost,
        },
        {
            "name": "imbalance_qty_cap_tightening",
            "classification": "possible_but_unproven",
            "remote_ready_now": False,
            "why": "Lower imbalance could cap residual qty, but no local evidence shows it avoids starving the cap25 fill/rescue pattern.",
            "minimum_qty_target": targets.get("max_residual_qty_for_remote_hard"),
        },
        {
            "name": "raise_pair_pnl_without_tail_reduction",
            "classification": "insufficient",
            "remote_ready_now": False,
            "why": "Raising PnL can clear residual-zero stress, but the stricter residual_cost_to_pair_qty gate still requires tail-cost reduction.",
            "required_pair_pnl_increase_for_zero_stress": targets.get("required_pair_pnl_increase_for_zero_stress"),
        },
        {
            "name": "widen_strict_rescue_surplus_net_cap_above_1_02",
            "classification": "rejected",
            "remote_ready_now": False,
            "why": "This can hide residual by relaxing economics; it is not allowed without a separate local proof and would weaken the current bridge interpretation.",
            "max_allowed_without_new_local_proof": args.max_strict_rescue_surplus_net_cap,
        },
        {
            "name": "re_enable_closeability_cancel_guard",
            "classification": "rejected_for_mainline",
            "remote_ready_now": False,
            "why": "Prior closeability-cancel evidence starved fills; it remains disallowed unless a new local scorer proves it fixes residual tail without starving fills.",
        },
    ]


def build_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    targets = card["mitigation_targets"]
    lines = [
        "# Xuan Shadow Review Tail Mitigation Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- mitigation_gate_ready: `{decision['mitigation_gate_ready']}`",
        f"- future_remote_allowed_by_this_gate: `{decision['future_remote_allowed_by_this_gate']}`",
        f"- mitigation_remote_rationale_ready: `{decision['mitigation_remote_rationale_ready']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        f"- next_action: `{decision['next_action']}`",
        "",
        "## Minimum Targets Before Any Future Remote",
        "",
        f"- residual_cost_lte: `{targets['max_residual_cost_for_remote_hard']}`",
        f"- residual_qty_lte: `{targets['max_residual_qty_for_remote_hard']}`",
        f"- residual cost reduction needed: `{targets['required_residual_cost_reduction_for_remote_hard']}`",
        f"- residual qty reduction needed: `{targets['required_residual_qty_reduction_for_remote_hard']}`",
        f"- residual-zero reduction needed: `{targets['required_residual_cost_reduction_for_zero_stress']}`",
        "",
        "## Stronger Review Target",
        "",
        f"- public target residual_qty_lte: `{targets['max_residual_qty_for_public_target']}`",
        f"- public target qty reduction needed: `{targets['required_residual_qty_reduction_for_public_target']}`",
        "",
        "## Lever Gate",
        "",
        "| Lever | Classification | Remote-ready now |",
        "| --- | --- | --- |",
    ]
    for lever in card["mitigation_levers"]:
        lines.append(f"| `{lever['name']}` | `{lever['classification']}` | `{lever['remote_ready_now']}` |")
    lines.extend(
        [
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
            "- This is a decision gate, not remote authorization.",
            "- Do not add another dry-run until a local mitigation hypothesis satisfies this gate.",
            "- Do not widen economics or re-enable closeability cancel to hide residual tail.",
            "- Do not deploy, restart, send live orders, or mutate shared services.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    residual_tail = load_json(args.residual_tail_gap_scorecard)
    residual_zero = load_json(args.residual_zero_stress_scorecard)
    capacity_plan = load_json(args.capacity_ladder_evidence_plan_scorecard)

    tail_decision = body(residual_tail, "decision")
    tail_current = body(residual_tail, "current_stage_metrics")
    tail_gaps = body(residual_tail, "residual_gaps")
    zero_decision = body(residual_zero, "decision")
    zero_stress = body(residual_zero, "current_residual_zero_stress")
    zero_tail_context = body(residual_zero, "residual_tail_context")
    capacity_decision = body(capacity_plan, "decision")
    current_stage_plan = next(
        (
            stage
            for stage in listify(capacity_plan.get("capacity_ladder"))
            if isinstance(stage, dict) and stage.get("stage") == capacity_decision.get("current_stage")
        ),
        {},
    )
    current_profile = body(current_stage_plan, "profile_overrides")

    required_cost_remote = max(
        gap_required(tail_gaps, "residual_cost_share_hard"),
        gap_required(tail_gaps, "residual_cost_to_pair_qty_hard"),
        fnum(zero_stress.get("required_residual_cost_reduction_for_zero_stress"), 0.0) or 0.0,
    )
    required_qty_remote = max(
        gap_required(tail_gaps, "residual_qty_share_hard"),
        gap_required(tail_gaps, "public_residual_rate_hard"),
    )
    required_qty_public_target = max(
        required_qty_remote,
        gap_required(tail_gaps, "residual_qty_share_target"),
        gap_required(tail_gaps, "public_residual_rate_target"),
    )
    current_residual_cost = fnum(tail_current.get("residual_cost"), 0.0) or 0.0
    current_residual_qty = fnum(tail_current.get("residual_qty"), 0.0) or 0.0
    max_cost_for_remote = min(
        value
        for value in [
            gap_allowed(tail_gaps, "residual_cost_share_hard"),
            gap_allowed(tail_gaps, "residual_cost_to_pair_qty_hard"),
            fnum(zero_stress.get("max_residual_cost_for_nonnegative_stress")),
        ]
        if value is not None
    )
    max_qty_for_remote = min(
        value
        for value in [
            gap_allowed(tail_gaps, "residual_qty_share_hard"),
            gap_allowed(tail_gaps, "public_residual_rate_hard"),
        ]
        if value is not None
    )
    max_qty_for_public_target = min(
        value
        for value in [
            gap_allowed(tail_gaps, "residual_qty_share_target"),
            gap_allowed(tail_gaps, "public_residual_rate_target"),
        ]
        if value is not None
    )
    targets = rounded(
        {
            "current_residual_cost": current_residual_cost,
            "current_residual_qty": current_residual_qty,
            "max_residual_cost_for_remote_hard": max_cost_for_remote,
            "max_residual_qty_for_remote_hard": max_qty_for_remote,
            "max_residual_qty_for_public_target": max_qty_for_public_target,
            "required_residual_cost_reduction_for_remote_hard": required_cost_remote,
            "required_residual_cost_reduction_share_for_remote_hard": required_cost_remote / current_residual_cost
            if current_residual_cost > 0
            else None,
            "required_residual_qty_reduction_for_remote_hard": required_qty_remote,
            "required_residual_qty_reduction_share_for_remote_hard": required_qty_remote / current_residual_qty
            if current_residual_qty > 0
            else None,
            "required_residual_qty_reduction_for_public_target": required_qty_public_target,
            "required_residual_qty_reduction_share_for_public_target": required_qty_public_target
            / current_residual_qty
            if current_residual_qty > 0
            else None,
            "required_residual_cost_reduction_for_zero_stress": zero_stress.get(
                "required_residual_cost_reduction_for_zero_stress"
            ),
            "required_pair_pnl_increase_for_zero_stress": zero_stress.get(
                "required_pair_pnl_increase_for_zero_stress"
            ),
            "residual_cost_after_cost_to_pair_qty_gate": zero_tail_context.get(
                "residual_cost_after_cost_to_pair_qty_gate"
            ),
            "stress_pnl_after_cost_to_pair_qty_gate": zero_tail_context.get(
                "stress_pnl_after_cost_to_pair_qty_gate"
            ),
        }
    )

    essential_inputs_ready = is_keep(residual_tail) and is_keep(residual_zero) and is_keep(capacity_plan)
    remote_rationale_ready = (
        False
        and tail_decision.get("current_stage_gate_pass") is True
        and zero_decision.get("residual_zero_current_stage_pass") is True
    )
    status_value = (
        "KEEP_SHADOW_REVIEW_TAIL_MITIGATION_GATE_READY_LOCAL_ONLY"
        if essential_inputs_ready
        else "UNKNOWN_SHADOW_REVIEW_TAIL_MITIGATION_GATE_INPUT_BLOCKED_LOCAL_ONLY"
    )
    hard_blockers = [] if essential_inputs_ready else ["tail_mitigation_gate_inputs_missing"]
    required_before_future_remote = [
        "produce a local mitigation hypothesis that targets mature-material residual tail",
        "show residual_cost <= hard target and residual_qty <= hard target in local gate terms",
        "show residual-zero stress is nonnegative or explicitly bounded by size controls",
        "prove the mitigation does not reduce accepted>=25, fills>=20, strict_rescue_closes>=7, or pair_pnl>=0",
        "keep strict_rescue_surplus_net_cap <= 1.02 unless a separate local scorer proves safety",
        "keep closeability cancel guard disabled unless a new local scorer proves no fill starvation",
        "manifest builder and verifier must KEEP on any future heartbeat before one bounded PM_DRY_RUN",
    ]

    return rounded(
        {
            "artifact": "xuan_shadow_review_tail_mitigation_gate",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_tail_mitigation_gate.py",
            "status": status_value,
            "inputs": {
                "residual_tail_gap_scorecard": args.residual_tail_gap_scorecard,
                "residual_zero_stress_scorecard": args.residual_zero_stress_scorecard,
                "capacity_ladder_evidence_plan_scorecard": args.capacity_ladder_evidence_plan_scorecard,
            },
            "source_statuses": {
                "residual_tail_gap": status(residual_tail),
                "residual_zero_stress": status(residual_zero),
                "capacity_ladder_evidence_plan": status(capacity_plan),
            },
            "decision": {
                "mitigation_gate_ready": status_value.startswith("KEEP"),
                "research_shadow_review_ready": True,
                "paper_shadow_only": True,
                "research_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "remote_runner_allowed": False,
                "future_remote_allowed_by_this_gate": False,
                "mitigation_remote_rationale_ready": remote_rationale_ready,
                "hard_blockers": hard_blockers,
                "required_before_future_remote": required_before_future_remote,
                "next_action": "build_local_mature_material_tail_mitigation_hypothesis_then_gate_again",
            },
            "mitigation_targets": targets,
            "current_profile_context": {
                "target_qty": current_profile.get("target_qty"),
                "imbalance_qty_cap": current_profile.get("imbalance_qty_cap"),
                "surplus_budget_max_abs_unpaired_cost": current_profile.get(
                    "surplus_budget_max_abs_unpaired_cost"
                ),
                "strict_rescue_surplus_net_cap": current_profile.get("strict_rescue_surplus_net_cap"),
                "risk_seed_pending_opp_credit": current_profile.get("risk_seed_pending_opp_credit"),
            },
            "mitigation_levers": build_mitigation_levers(args, current_profile, targets),
            "interpretation": {
                "minimum_remote_target_is_cost_to_pair_qty_gate": tail_decision.get("hardest_residual_gap")
                == "residual_cost_to_pair_qty_hard",
                "zero_stress_is_required_but_not_sufficient": zero_tail_context.get(
                    "zero_stress_reduction_weaker_than_capacity_gate"
                )
                is True,
                "cap75_remote_blocked_until_local_mitigation_gate_passes": True,
                "do_not_create_more_explanatory_artifacts_without_decision_change": True,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--residual-tail-gap-scorecard", required=True)
    parser.add_argument("--residual-zero-stress-scorecard", required=True)
    parser.add_argument("--capacity-ladder-evidence-plan-scorecard", required=True)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
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
