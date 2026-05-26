#!/usr/bin/env python3
"""Plan a post-midpoint seed-risk reduction profile.

The 0945Z midpoint run was incomplete and showed weak density under the
residual guard. Its event diagnostics point to risk-increasing seed admissions
and surplus-budget pressure, so the next local profile should reduce risky seed
admissions instead of relaxing rescue economics. This helper emits a local-only
plan and never launches a remote job or authorizes deploy/live execution.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_RERUN_GATE = Path(".tmp_xuan/scorecards/xuan_shadow_review_midpoint_rerun_readiness_gate_20260526T1412Z.json")
DEFAULT_EVENT_DIAGNOSTICS = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-density-midpoint-20260526T0945Z_event_diagnostics.json"
)
DEFAULT_RESIDUAL_PLAN = Path(".tmp_xuan/scorecards/xuan_shadow_review_tail_density_residual_rebalance_plan_20260526T1704Z.json")
DEFAULT_TRANSFER_PLAN = Path(".tmp_xuan/scorecards/xuan_public_04b6_residual_transfer_plan_20260526T1338Z.json")
DEFAULT_CAPACITY_PLAN = Path(".tmp_xuan/scorecards/xuan_shadow_review_capacity_ladder_evidence_plan_20260526T0855Z.json")
DEFAULT_SCORECARD_ROOT = Path(".tmp_xuan/scorecards")
DEFAULT_ARTIFACT_ROOT = Path(".tmp_xuan/local_verifier_artifacts")


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


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


def stat(card: dict[str, Any], key: str, field: str) -> float | None:
    raw = card.get(key)
    if not isinstance(raw, dict):
        return None
    return fnum(raw.get(field))


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    p = card["next_profile_hypothesis"]["profile_overrides"]
    diag = card["diagnostics"]
    lines = [
        "# Xuan Post-Midpoint Seed-Risk Reduction Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- seed_risk_reduction_plan_ready: `{d['seed_risk_reduction_plan_ready']}`",
        f"- future_remote_allowed_by_this_plan: `{d['future_remote_allowed_by_this_plan']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- next_action: `{d['next_action']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Proposed Profile Overrides",
        "",
        f"- risk_seed_pair_completion_required_above_net_cap: `{p['risk_seed_pair_completion_required_above_net_cap']}`",
        f"- risk_seed_pair_completion_min_qty: `{p['risk_seed_pair_completion_min_qty']}`",
        f"- pair_completion_net_cap: `{p['pair_completion_net_cap']}`",
        f"- risk_seed_pending_opp_credit: `{p['risk_seed_pending_opp_credit']}`",
        f"- activation_window_s: `{p['activation_window_s']}`",
        f"- late_repair_after_s: `{p['late_repair_after_s']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{p['surplus_budget_max_abs_unpaired_cost']}`",
        f"- strict_rescue_surplus_net_cap: `{p['strict_rescue_surplus_net_cap']}`",
        "",
        "## Diagnostics",
        "",
        f"- risk_increasing_seed_true_share: `{diag['risk_increasing_seed_true_share']}`",
        f"- candidate_closeability_net_pair_cost_p50: `{diag['candidate_closeability_net_pair_cost_p50']}`",
        f"- candidate_closeability_net_pair_cost_p90: `{diag['candidate_closeability_net_pair_cost_p90']}`",
        f"- surplus_budget_projected_unpaired_cost_p50: `{diag['surplus_budget_projected_unpaired_cost_p50']}`",
        "",
        "## Guardrails",
        "",
        "- Local planning only; no remote is launched.",
        "- Keep cap25; do not expand to cap75 while residual gates are blocked.",
        "- Do not relax surplus budget or strict rescue surplus net cap.",
        "- Do not enable closeability cancel on the soft-mainline evidence lane.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    rerun_gate = load_json(args.rerun_gate_scorecard)
    events = load_json(args.event_diagnostics_scorecard)
    residual_plan = load_json(args.residual_plan_scorecard)
    transfer_plan = load_json(args.transfer_plan_scorecard)
    capacity_plan = load_json(args.capacity_plan_scorecard)

    gate_decision = body(rerun_gate, "decision")
    residual_hypothesis = body(residual_plan, "next_profile_hypothesis")
    residual_overrides = body(residual_hypothesis, "profile_overrides")
    risk_counts = events.get("candidate_risk_increasing_seed_values")
    risk_counts = risk_counts if isinstance(risk_counts, dict) else {}
    risk_true = fnum(risk_counts.get("true"), 0.0) or 0.0
    risk_false = fnum(risk_counts.get("false"), 0.0) or 0.0
    risk_total = risk_true + risk_false
    risk_true_share = risk_true / risk_total if risk_total else None
    net_p50 = stat(events, "candidate_closeability_net_pair_cost", "p50")
    net_p90 = stat(events, "candidate_closeability_net_pair_cost", "p90")
    surplus_p50 = stat(events, "surplus_budget_projected_unpaired_cost", "p50")

    required_above_net_cap = args.required_above_net_cap
    if required_above_net_cap is None:
        required_above_net_cap = net_p50 if net_p50 is not None else args.default_required_above_net_cap
    required_above_net_cap = min(args.max_required_above_net_cap, max(args.min_required_above_net_cap, required_above_net_cap))

    profile_overrides = {
        **residual_overrides,
        "profile_family": "residual_guard",
        "tail_mitigation_hypothesis": "cap25_post_midpoint_seed_risk_reduction_pair_completion_guard",
        "risk_seed_cancel_on_closeability_net_cap": None,
        "risk_seed_pair_completion_required_above_net_cap": required_above_net_cap,
        "risk_seed_pair_completion_min_qty": args.risk_seed_pair_completion_min_qty,
        "pair_completion_net_cap": args.pair_completion_net_cap,
        "pair_completion_min_pair_pnl_after": args.pair_completion_min_pair_pnl_after,
        "risk_seed_pending_opp_credit": args.risk_seed_pending_opp_credit,
        "activation_window_s": args.activation_window_s,
        "late_repair_after_s": args.late_repair_after_s,
        "strict_rescue_surplus_net_cap": min(fnum(residual_overrides.get("strict_rescue_surplus_net_cap"), 1.02) or 1.02, 1.02),
        "surplus_budget_max_abs_unpaired_cost": min(
            fnum(residual_overrides.get("surplus_budget_max_abs_unpaired_cost"), 1.44) or 1.44,
            args.max_surplus_budget_abs_unpaired_cost,
        ),
    }

    hard_blockers: list[str] = []
    if not gate_decision.get("profile_redesign_required"):
        hard_blockers.append("rerun_gate_does_not_require_profile_redesign")
    if gate_decision.get("same_profile_rerun_ready"):
        hard_blockers.append("same_profile_already_rerun_ready")
    if not is_keep(residual_plan):
        hard_blockers.append("residual_rebalance_plan_not_keep")
    if not is_keep(transfer_plan):
        hard_blockers.append("04b6_residual_transfer_plan_not_keep")
    if not is_keep(capacity_plan):
        hard_blockers.append("capacity_ladder_plan_not_keep")
    if risk_true_share is None or risk_true_share < args.min_problem_risk_seed_share:
        hard_blockers.append("risk_increasing_seed_share_not_material")
    if profile_overrides["risk_seed_cancel_on_closeability_net_cap"] is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")
    if fnum(profile_overrides.get("strict_rescue_surplus_net_cap"), 9.0) > 1.02:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if fnum(profile_overrides.get("surplus_budget_max_abs_unpaired_cost"), 9.0) > args.max_surplus_budget_abs_unpaired_cost:
        hard_blockers.append("surplus_budget_relaxed")
    if fnum(profile_overrides.get("pair_completion_net_cap"), 9.0) > args.max_pair_completion_net_cap:
        hard_blockers.append("pair_completion_net_cap_too_loose")

    plan_ready = not hard_blockers
    status_value = (
        "KEEP_SHADOW_REVIEW_POST_MIDPOINT_SEED_RISK_REDUCTION_PLAN_READY_LOCAL_ONLY"
        if plan_ready
        else "UNKNOWN_SHADOW_REVIEW_POST_MIDPOINT_SEED_RISK_REDUCTION_PLAN_BLOCKED_LOCAL_ONLY"
    )
    return rounded(
        {
            "artifact": "xuan_shadow_review_post_midpoint_seed_risk_reduction_plan",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_post_midpoint_seed_risk_reduction_plan.py",
            "status": status_value,
            "inputs": {
                "rerun_gate_scorecard": str(args.rerun_gate_scorecard),
                "event_diagnostics_scorecard": str(args.event_diagnostics_scorecard),
                "residual_plan_scorecard": str(args.residual_plan_scorecard),
                "transfer_plan_scorecard": str(args.transfer_plan_scorecard),
                "capacity_plan_scorecard": str(args.capacity_plan_scorecard),
            },
            "source_statuses": {
                "rerun_gate": status(rerun_gate),
                "residual_plan": status(residual_plan),
                "transfer_plan": status(transfer_plan),
                "capacity_plan": status(capacity_plan),
            },
            "diagnostics": {
                "risk_increasing_seed_true": risk_true,
                "risk_increasing_seed_false": risk_false,
                "risk_increasing_seed_true_share": risk_true_share,
                "candidate_closeability_net_pair_cost_p50": net_p50,
                "candidate_closeability_net_pair_cost_p90": net_p90,
                "surplus_budget_projected_unpaired_cost_p50": surplus_p50,
                "risk_seed_pair_completion_required_above_net_cap_basis": "candidate_closeability_net_pair_cost_p50",
            },
            "decision": {
                "seed_risk_reduction_plan_ready": plan_ready,
                "future_remote_rationale_candidate": plan_ready,
                "future_remote_allowed_by_this_plan": False,
                "remote_runner_allowed": False,
                "cap75_remote_rationale_ready": False,
                "deployable": False,
                "live_orders_allowed": False,
                "research_only": True,
                "paper_shadow_only": True,
                "hard_blockers": hard_blockers,
                "required_before_future_remote": [
                    "materialize this profile from the tail mitigation base profile",
                    "run runner scaffold smoke for pair-completion-required and pending-opp-credit guards",
                    "run manifest builder and verifier after profile materialization",
                    "confirm active remote runner conflict is clear before any bounded PM_DRY_RUN",
                    "keep cap25 until residual_cost_to_pair_qty and density clear together",
                ],
                "next_action": "materialize_seed_risk_reduction_profile_and_run_manifest_verifier"
                if plan_ready
                else "stay_local_until_seed_risk_reduction_inputs_are_clean",
            },
            "next_profile_hypothesis": {
                "candidate": "cap25_post_midpoint_seed_risk_reduction_pair_completion_guard",
                "profile_overrides": profile_overrides,
                "rationale": [
                    "0945Z midpoint cannot be rerun as-is because it exited 137 and had weak density.",
                    "Risk-increasing seed share was high, so the redesign should reduce risky seed admissions.",
                    "Require pair-completion evidence above the midpoint closeability-cost median instead of relaxing surplus caps.",
                    "Set pending opposite credit to zero so pending intent does not count as filled pair protection.",
                    "Keep residual budget and strict rescue surplus cap unchanged to avoid reopening the residual blocker.",
                ],
            },
            "safety": {
                "remote_runner_allowed": False,
                "deployable": False,
                "live_trading_allowed": False,
                "orders_sent": False,
                "shared_service_mutation": False,
                "remote_repo_mutation": False,
                "collector_rebuild": False,
                "cron_loop": False,
            },
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rerun-gate-scorecard", type=Path, default=DEFAULT_RERUN_GATE)
    parser.add_argument("--event-diagnostics-scorecard", type=Path, default=DEFAULT_EVENT_DIAGNOSTICS)
    parser.add_argument("--residual-plan-scorecard", type=Path, default=DEFAULT_RESIDUAL_PLAN)
    parser.add_argument("--transfer-plan-scorecard", type=Path, default=DEFAULT_TRANSFER_PLAN)
    parser.add_argument("--capacity-plan-scorecard", type=Path, default=DEFAULT_CAPACITY_PLAN)
    parser.add_argument("--required-above-net-cap", type=float, default=None)
    parser.add_argument("--default-required-above-net-cap", type=float, default=0.975)
    parser.add_argument("--min-required-above-net-cap", type=float, default=0.9725)
    parser.add_argument("--max-required-above-net-cap", type=float, default=0.98)
    parser.add_argument("--risk-seed-pair-completion-min-qty", type=float, default=1.25)
    parser.add_argument("--pair-completion-net-cap", type=float, default=0.98)
    parser.add_argument("--max-pair-completion-net-cap", type=float, default=0.98)
    parser.add_argument("--pair-completion-min-pair-pnl-after", type=float, default=0.0)
    parser.add_argument("--risk-seed-pending-opp-credit", type=float, default=0.0)
    parser.add_argument("--activation-window-s", type=float, default=10.0)
    parser.add_argument("--late-repair-after-s", type=float, default=60.0)
    parser.add_argument("--max-surplus-budget-abs-unpaired-cost", type=float, default=1.44)
    parser.add_argument("--min-problem-risk-seed-share", type=float, default=0.5)
    parser.add_argument("--scorecard-root", type=Path, default=DEFAULT_SCORECARD_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--stamp", default=time.strftime("%Y%m%dT%H%MZ", time.gmtime()))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scorecard = build(args)
    args.scorecard_root.mkdir(parents=True, exist_ok=True)
    artifact_dir = args.artifact_root / f"xuan_shadow_review_post_midpoint_seed_risk_reduction_plan_{args.stamp}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    scorecard_path = args.scorecard_root / f"xuan_shadow_review_post_midpoint_seed_risk_reduction_plan_{args.stamp}.json"
    markdown_path = artifact_dir / "POST_MIDPOINT_SEED_RISK_REDUCTION_PLAN.md"
    scorecard["scorecard_path"] = str(scorecard_path)
    scorecard["markdown_path"] = str(markdown_path)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(markdown(scorecard) + "\n", encoding="utf-8")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if scorecard["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
