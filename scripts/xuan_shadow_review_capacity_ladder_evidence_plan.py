#!/usr/bin/env python3
"""Interpret capacity-ladder evidence after bridge-aware shadow review.

This local-only helper separates a capacity ladder plan from capacity evidence.
It reads the current cap25 runtime, public benchmark gate, and bridge evidence,
then decides whether a future bounded dry-run has a separate rationale. It does
not launch remote jobs, approve deploy, approve live orders, or relax residual
and source gates.
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
    if value is None or value == "":
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
    raw_status = status(card) or ""
    return raw_status.startswith("KEEP")


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def find_stage(capacity_plan: dict[str, Any], stage_name: str) -> dict[str, Any]:
    for stage in listify(capacity_plan.get("ladder")):
        if isinstance(stage, dict) and stage.get("stage") == stage_name:
            return stage
    return {}


def next_stage_name(capacity_plan: dict[str, Any], current_stage: str) -> str | None:
    stages = [stage.get("stage") for stage in listify(capacity_plan.get("ladder")) if isinstance(stage, dict)]
    if current_stage not in stages:
        return stages[0] if stages else None
    idx = stages.index(current_stage)
    return stages[idx + 1] if idx + 1 < len(stages) else None


def stage_summary(stage: dict[str, Any]) -> dict[str, Any]:
    return {
        "stage": stage.get("stage"),
        "round_redeem_notional": stage.get("round_redeem_notional"),
        "duration_s": stage.get("duration_s"),
        "profile_overrides": {
            key: body(stage, "profile_overrides").get(key)
            for key in [
                "target_qty",
                "max_seed_qty",
                "max_open_cost",
                "imbalance_qty_cap",
                "surplus_budget_max_abs_unpaired_cost",
                "strict_rescue_surplus_net_cap",
                "strict_rescue_min_pair_pnl_after",
                "risk_seed_pending_opp_credit",
            ]
        },
        "pass_gates": body(stage, "pass_gates"),
    }


def hard_gate_map(stage_gate: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for gate in listify(stage_gate.get("hard_gates")):
        if isinstance(gate, dict) and gate.get("name"):
            out[str(gate["name"])] = gate
    return out


def capacity_blocker_interpretation(blockers: list[str]) -> dict[str, Any]:
    residual = [
        name
        for name in blockers
        if name
        in {
            "max_residual_cost_share",
            "max_residual_qty_share",
            "max_residual_cost_to_pair_qty",
            "public_review_residual_rate_hard",
            "public_target_residual_rate",
        }
    ]
    source = [name for name in blockers if "source" in name or "l1_age" in name]
    economics = [name for name in blockers if "pnl" in name or "roi" in name or "edge" in name]
    safety = [
        name
        for name in blockers
        if name
        in {
            "pm_dry_run",
            "orders_sent",
            "deploy_or_restart",
            "shared_service_mutation",
            "remote_repo_mutation",
        }
    ]
    return {
        "residual_blockers": residual,
        "source_blockers": source,
        "economics_blockers": economics,
        "safety_blockers": safety,
        "residual_is_primary_blocker": bool(residual) and not source and not economics and not safety,
    }


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    current = card["current_stage_evidence"]
    next_stage = card["next_stage_plan"]
    lines = [
        "# Xuan Shadow Review Capacity Ladder Evidence Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- capacity_ladder_evidence_plan_ready: `{decision['capacity_ladder_evidence_plan_ready']}`",
        f"- current_stage: `{decision['current_stage']}`",
        f"- current_stage_gate_pass: `{decision['current_stage_gate_pass']}`",
        f"- next_stage: `{decision['next_stage']}`",
        f"- future_bounded_dry_run_rationale_ready: `{decision['future_bounded_dry_run_rationale_ready']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) or 'none'}`",
        "",
        "## Current Stage Evidence",
        "",
        f"- runtime status: `{card['source_statuses']['runtime_summary']}`",
        f"- stage gate status: `{card['source_statuses']['current_stage_public_benchmark_gate']}`",
        f"- accepted actions: `{current['accepted_actions']}`",
        f"- fills: `{current['queue_supported_fills']}`",
        f"- strict rescue closes: `{current['strict_rescue_closes']}`",
        f"- pair PnL: `{current['pair_pnl']}`",
        f"- residual qty/cost share: `{current['residual_qty_share']}` / `{current['residual_cost_share']}`",
        f"- residual cost to pair qty: `{current['residual_cost_to_pair_qty']}`",
        "",
        "## Next Stage",
        "",
        f"- stage: `{next_stage.get('stage')}`",
        f"- round redeem notional: `{next_stage.get('round_redeem_notional')}`",
        f"- duration_s: `{next_stage.get('duration_s')}`",
        f"- profile overrides: `{json.dumps(next_stage.get('profile_overrides'), sort_keys=True)}`",
        "",
        "## Required Before Future Remote",
        "",
    ]
    lines.extend(f"- {item}" for item in decision["required_before_future_remote"])
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- This is a local evidence plan, not remote authorization.",
            "- Do not expand capacity while the current stage residual/public residual gate is blocked.",
            "- Do not deploy, restart, send live orders, or mutate shared services.",
            "- Keep cap25/cap75/cap150/cap300 evidence separate from live capacity approval.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    caveat_gap = load_json(args.caveat_gap_scorecard)
    approval = load_json(args.approval_gate_scorecard)
    capacity_plan = load_json(args.capacity_plan_scorecard)
    stage_gate = load_json(args.current_stage_public_benchmark_gate_scorecard)
    runtime = load_json(args.current_runtime_summary_scorecard)
    capital = load_json(args.capital_roi_scorecard)
    surplus = load_json(args.surplus_bridge_scorecard)
    private_truth = load_json(args.private_truth_request_scorecard)

    caveat_decision = body(caveat_gap, "decision")
    approval_decision = body(approval, "decision")
    runtime_metrics = body(runtime, "metrics")
    capital_aggregate = body(capital, "aggregate")
    capital_round_roi = body(capital_aggregate, "round_roi")
    surplus_aggregate = body(surplus, "aggregate")
    private_truth_decision = body(private_truth, "decision")
    stage_gate_decision = body(stage_gate, "decision")
    stage_gate_metrics = body(stage_gate, "metrics")
    current_stage = args.current_stage
    next_stage = next_stage_name(capacity_plan, current_stage)
    next_stage_body = find_stage(capacity_plan, next_stage or "")
    current_stage_gate_pass = stage_gate_decision.get("capacity_stage_gate_pass") is True
    stage_blockers = [str(item) for item in listify(stage_gate_decision.get("hard_blockers")) if item]
    blocker_groups = capacity_blocker_interpretation(stage_blockers)
    caveats = [str(item) for item in listify(caveat_decision.get("caveats")) if item]
    has_capacity_caveat = "projection_is_linear_capacity_hypothesis_not_runtime_capacity_evidence" in caveats

    hard_blockers: list[str] = []
    if not is_keep(caveat_gap):
        hard_blockers.append("caveat_gap_plan_not_keep")
    if not is_keep(approval):
        hard_blockers.append("approval_gate_not_keep")
    if not is_keep(capacity_plan):
        hard_blockers.append("capacity_ladder_plan_not_keep")
    if status(runtime) != "KEEP_THIRD_WINDOW_RUNTIME_SUMMARY_SAFE_RESEARCH_ONLY":
        hard_blockers.append("current_runtime_summary_not_safe_keep")
    if not has_capacity_caveat:
        hard_blockers.append("capacity_projection_caveat_missing")
    if not current_stage_gate_pass:
        hard_blockers.append("current_stage_public_benchmark_gate_not_keep")

    required_before_future_remote = [
        "current_stage_public_benchmark_gate must KEEP for residual, source, safety, and economics",
        "manifest builder and verifier must KEEP for the next capacity stage",
        "runner conflict check must be clean on the future heartbeat",
        "PM_DRY_RUN=1, orders_sent=false, duration_s<=1800, no shared mutation",
        "remote launch must use the manifest builder remote_launch_command_template",
    ]
    if blocker_groups["residual_blockers"]:
        required_before_future_remote.insert(
            0,
            "resolve or explicitly bound current-stage residual blockers before cap expansion",
        )
    if private_truth_decision.get("private_truth_ready") is not True:
        required_before_future_remote.append(
            "private truth is not required for a research-only capacity dry-run, but remains a live/deploy blocker"
        )

    future_rationale_ready = (
        not hard_blockers
        and current_stage_gate_pass
        and next_stage is not None
        and approval_decision.get("shadow_review_approval_ready") is True
    )
    status_value = (
        "KEEP_SHADOW_REVIEW_CAPACITY_LADDER_EVIDENCE_PLAN_READY_LOCAL_ONLY"
        if is_keep(caveat_gap) and is_keep(approval) and is_keep(capacity_plan) and status(runtime)
        else "UNKNOWN_SHADOW_REVIEW_CAPACITY_LADDER_EVIDENCE_PLAN_INPUT_BLOCKED_LOCAL_ONLY"
    )
    return rounded(
        {
            "artifact": "xuan_shadow_review_capacity_ladder_evidence_plan",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_capacity_ladder_evidence_plan.py",
            "status": status_value,
            "inputs": {
                "caveat_gap_scorecard": args.caveat_gap_scorecard,
                "approval_gate_scorecard": args.approval_gate_scorecard,
                "capacity_plan_scorecard": args.capacity_plan_scorecard,
                "current_stage_public_benchmark_gate_scorecard": args.current_stage_public_benchmark_gate_scorecard,
                "current_runtime_summary_scorecard": args.current_runtime_summary_scorecard,
                "capital_roi_scorecard": args.capital_roi_scorecard,
                "surplus_bridge_scorecard": args.surplus_bridge_scorecard,
                "private_truth_request_scorecard": args.private_truth_request_scorecard,
            },
            "source_statuses": {
                "caveat_gap": status(caveat_gap),
                "approval_gate": status(approval),
                "capacity_plan": status(capacity_plan),
                "current_stage_public_benchmark_gate": status(stage_gate),
                "runtime_summary": status(runtime),
                "capital_roi": status(capital),
                "surplus_bridge": status(surplus),
                "private_truth_request": status(private_truth),
            },
            "decision": {
                "capacity_ladder_evidence_plan_ready": status_value.startswith("KEEP"),
                "research_shadow_review_ready": approval_decision.get("shadow_review_approval_ready") is True,
                "current_stage": current_stage,
                "current_stage_gate_pass": current_stage_gate_pass,
                "current_stage_gate_blockers": stage_blockers,
                "current_stage_blocker_groups": blocker_groups,
                "next_stage": next_stage,
                "future_bounded_dry_run_rationale_ready": future_rationale_ready,
                "future_remote_allowed_by_this_plan": False,
                "remote_runner_allowed": False,
                "research_only": True,
                "paper_shadow_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "hard_blockers": hard_blockers,
                "required_before_future_remote": required_before_future_remote,
                "next_action": "work_residual_capacity_blockers_locally_before_cap75_remote"
                if not future_rationale_ready
                else "prepare_cap75_manifest_verifier_before_future_bounded_remote_decision",
            },
            "current_stage_evidence": {
                "accepted_actions": runtime_metrics.get("accepted_actions"),
                "queue_supported_fills": runtime_metrics.get("queue_supported_fills"),
                "strict_rescue_closes": runtime_metrics.get("strict_rescue_closes"),
                "pair_pnl": runtime_metrics.get("pair_pnl"),
                "roi_on_filled_cost": runtime_metrics.get("roi_on_filled_cost"),
                "residual_qty_share": runtime_metrics.get("residual_qty_share"),
                "residual_cost_share": runtime_metrics.get("residual_cost_share"),
                "residual_cost_to_pair_qty": capital_round_roi.get("residual_cost_to_pair_qty"),
                "actual_pair_cost_after_fee": stage_gate_metrics.get("actual_pair_cost_after_fee"),
                "accepted_l1_age_ms_max": runtime_metrics.get("accepted_l1_age_ms_max"),
                "rescue_l1_age_ms_max": runtime_metrics.get("rescue_l1_age_ms_max"),
                "strict_rescue_source_blocks": runtime_metrics.get("strict_rescue_source_blocks"),
            },
            "bridge_evidence": {
                "accepted_actions": surplus_aggregate.get("accepted_actions"),
                "queue_supported_fills": surplus_aggregate.get("queue_supported_fills"),
                "strict_rescue_closes": surplus_aggregate.get("strict_rescue_closes"),
                "pair_pnl": surplus_aggregate.get("pair_pnl"),
                "roi_on_filled_cost": surplus_aggregate.get("roi_on_filled_cost"),
                "residual_qty_share": surplus_aggregate.get("residual_qty_share"),
                "residual_cost_share": surplus_aggregate.get("residual_cost_share"),
                "bridge_eligible_window_count": surplus_aggregate.get("bridge_eligible_window_count"),
            },
            "current_stage_public_gate": {
                "status": status(stage_gate),
                "hard_blockers": stage_blockers,
                "soft_warnings": stage_gate_decision.get("soft_warnings"),
                "hard_gates": listify(stage_gate.get("hard_gates")),
                "soft_review_gates": listify(stage_gate.get("soft_review_gates")),
            },
            "capacity_ladder": [stage_summary(stage) for stage in listify(capacity_plan.get("ladder")) if isinstance(stage, dict)],
            "next_stage_plan": stage_summary(next_stage_body),
            "guardrails": {
                "no_remote_launched": True,
                "no_deploy_or_restart": True,
                "no_live_orders": True,
                "no_shared_service_mutation": True,
                "no_strategy_economics_relaxed": True,
                "private_truth_remains_live_deploy_blocker": private_truth_decision.get("private_truth_ready") is not True,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--caveat-gap-scorecard", required=True)
    parser.add_argument("--approval-gate-scorecard", required=True)
    parser.add_argument("--capacity-plan-scorecard", required=True)
    parser.add_argument("--current-stage-public-benchmark-gate-scorecard", required=True)
    parser.add_argument("--current-runtime-summary-scorecard", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--surplus-bridge-scorecard", required=True)
    parser.add_argument("--private-truth-request-scorecard", required=True)
    parser.add_argument("--current-stage", default="cap_25")
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
        markdown_path.write_text(markdown(scorecard) + "\n")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if scorecard["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
