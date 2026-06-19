#!/usr/bin/env python3
"""Plan the next density-preserving pair-completion profile.

The seed-risk-reduction remote showed the desired residual direction but
overfiltered cap25 density. This local-only planner converts that result into a
bounded next-profile hypothesis. It never launches a remote job, authorizes
capacity expansion, deploys, restarts services, or sends orders.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_seed_risk_reduction_result_20260526T1510Z.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_post_midpoint_seed_risk_reduction_profile_20260526T1442Z.json")
DEFAULT_CAPACITY = Path(".tmp_xuan/scorecards/xuan_shadow_review_capacity_ladder_evidence_plan_20260526T0855Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_plan_20260526T1602Z.json")
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_density_preserving_pair_completion_plan_20260526T1602Z/PAIR_COMPLETION_PLAN.md"
)


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


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def midpoint(a: float | None, b: float | None, fallback: float) -> float:
    if a is None or b is None:
        return fallback
    return (a + b) / 2.0


def gap(actual: float | None, target: float | None) -> float | None:
    if actual is None or target is None:
        return None
    return max(0.0, target - actual)


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    g = card["diagnostics"]["density_gaps"]
    p = card["next_profile_hypothesis"]["primary_profile_overrides"]
    lines = [
        "# Xuan Density-Preserving Pair-Completion Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- plan_ready: `{d['plan_ready']}`",
        f"- same_profile_repeat_allowed: `{d['same_profile_repeat_allowed']}`",
        f"- future_remote_allowed_by_this_plan: `{d['future_remote_allowed_by_this_plan']}`",
        f"- future_remote_requires_separate_rationale: `{d['future_remote_requires_separate_rationale']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Density Gaps",
        "",
        f"- accepted_actions shortfall: `{g['accepted_actions']}`",
        f"- queue_supported_fills shortfall: `{g['queue_supported_fills']}`",
        f"- strict_rescue_or_salvage_rows shortfall: `{g['strict_rescue_or_salvage_rows']}`",
        f"- pair_completion_block_ratio_to_accepted: `{card['diagnostics']['guard_pressure']['pair_completion_block_ratio_to_accepted']}`",
        "",
        "## Primary Profile Overrides",
        "",
        f"- risk_seed_pair_completion_required_above_net_cap: `{p['risk_seed_pair_completion_required_above_net_cap']}`",
        f"- risk_seed_pair_completion_min_qty: `{p['risk_seed_pair_completion_min_qty']}`",
        f"- pair_completion_net_cap: `{p['pair_completion_net_cap']}`",
        f"- pair_completion_min_pair_pnl_after: `{p['pair_completion_min_pair_pnl_after']}`",
        f"- risk_seed_pending_opp_credit: `{p['risk_seed_pending_opp_credit']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{p['surplus_budget_max_abs_unpaired_cost']}`",
        f"- strict_rescue_surplus_net_cap: `{p['strict_rescue_surplus_net_cap']}`",
        "",
        "## Interpretation",
        "",
        "- Do not repeat the 1510Z profile: it timed out and did not preserve cap25 density.",
        "- The next local hypothesis should make pair completion less binary while keeping surplus and source gates intact.",
        "- This artifact does not authorize a new remote; manifest/smoke evidence and a separate rationale are still required.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    result = load_json(args.result_scorecard)
    profile = load_json(args.profile_scorecard)
    capacity = load_json(args.capacity_plan_scorecard)

    decision = body(result, "decision")
    observed = body(result, "observed")
    events = body(result, "event_diagnostics")
    prior_profile = body(profile, "profile")
    thresholds = body(observed, "density_thresholds")

    accepted = fnum(observed.get("accepted_actions"), 0.0) or 0.0
    fills = fnum(observed.get("queue_supported_fills"), 0.0) or 0.0
    rescues = fnum(observed.get("strict_rescue_or_salvage_rows"), 0.0) or 0.0
    min_accepted = fnum(thresholds.get("min_accepted_actions"), 20.0)
    min_fills = fnum(thresholds.get("min_fills"), 18.0)
    min_rescues = fnum(thresholds.get("min_rescue_closes"), 7.0)
    pair_blocks = fnum(events.get("pair_completion_blocks"), 0.0) or 0.0
    surplus_blocks = fnum(events.get("surplus_budget_blocks"), 0.0) or 0.0
    strict_cost_p50 = fnum(events.get("strict_rescue_net_pair_cost_p50"))
    strict_cost_p90 = fnum(events.get("strict_rescue_net_pair_cost_p90"))
    trigger_cap = midpoint(strict_cost_p50, strict_cost_p90, args.default_required_above_net_cap)
    trigger_cap = min(args.max_required_above_net_cap, max(args.min_required_above_net_cap, trigger_cap))

    base_surplus_budget = fnum(prior_profile.get("surplus_budget_max_abs_unpaired_cost"), 1.44) or 1.44
    base_rescue_surplus_cap = fnum(prior_profile.get("strict_rescue_surplus_net_cap"), 1.02) or 1.02
    primary = {
        **prior_profile,
        "tail_mitigation_hypothesis": "cap25_density_preserving_pair_completion_guard",
        "risk_seed_cancel_on_closeability_net_cap": None,
        "risk_seed_pair_completion_required_above_net_cap": trigger_cap,
        "risk_seed_pair_completion_min_qty": fnum(
            prior_profile.get("risk_seed_pair_completion_min_qty"), args.risk_seed_pair_completion_min_qty
        ),
        "pair_completion_net_cap": args.pair_completion_net_cap,
        "pair_completion_min_pair_pnl_after": args.pair_completion_min_pair_pnl_after,
        "risk_seed_pending_opp_credit": 0.0,
        "surplus_budget_max_abs_unpaired_cost": min(base_surplus_budget, args.max_surplus_budget_abs_unpaired_cost),
        "strict_rescue_surplus_net_cap": min(base_rescue_surplus_cap, args.max_strict_rescue_surplus_net_cap),
    }
    residual_strict_variant = {
        "risk_seed_pair_completion_required_above_net_cap": primary["risk_seed_pair_completion_required_above_net_cap"],
        "pair_completion_net_cap": fnum(prior_profile.get("pair_completion_net_cap"), 0.98),
        "pair_completion_min_pair_pnl_after": args.pair_completion_min_pair_pnl_after,
        "risk_seed_pending_opp_credit": 0.0,
        "purpose": "if local residual proxy regresses under primary pair_completion_net_cap",
    }
    density_recovery_variant = {
        "risk_seed_pair_completion_required_above_net_cap": strict_cost_p90,
        "pair_completion_net_cap": args.pair_completion_net_cap,
        "pair_completion_min_pair_pnl_after": args.pair_completion_min_pair_pnl_after,
        "risk_seed_pending_opp_credit": 0.0,
        "purpose": "local-only fallback if primary still starves cap25 density",
    }

    hard_blockers: list[str] = []
    if not status(result).startswith("BLOCKED_SHADOW_REVIEW_SEED_RISK_REDUCTION_RESULT"):
        hard_blockers.append("seed_risk_reduction_result_not_blocked_as_expected")
    if not bool(decision.get("pair_completion_overfiltered")):
        hard_blockers.append("pair_completion_overfilter_signal_missing")
    if not bool(decision.get("residual_preflight_zero")):
        hard_blockers.append("residual_zero_signal_missing")
    if bool(decision.get("future_remote_allowed_by_this_result")):
        hard_blockers.append("prior_result_already_allows_remote")
    if not is_keep(profile):
        hard_blockers.append("source_profile_not_keep")
    if not is_keep(capacity):
        hard_blockers.append("capacity_plan_not_keep")
    if fnum(primary.get("pair_completion_net_cap"), 9.0) > args.max_pair_completion_net_cap:
        hard_blockers.append("pair_completion_net_cap_too_loose")
    if fnum(primary.get("surplus_budget_max_abs_unpaired_cost"), 9.0) > args.max_surplus_budget_abs_unpaired_cost:
        hard_blockers.append("surplus_budget_relaxed")
    if fnum(primary.get("strict_rescue_surplus_net_cap"), 9.0) > args.max_strict_rescue_surplus_net_cap:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if primary.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")

    plan_ready = not hard_blockers
    card = {
        "artifact": "xuan_shadow_review_density_preserving_pair_completion_plan",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_density_preserving_pair_completion_plan.py",
        "status": (
            "KEEP_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_PLAN_READY_LOCAL_ONLY"
            if plan_ready
            else "UNKNOWN_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_PLAN_BLOCKED_LOCAL_ONLY"
        ),
        "inputs": {
            "result_scorecard": str(args.result_scorecard),
            "profile_scorecard": str(args.profile_scorecard),
            "capacity_plan_scorecard": str(args.capacity_plan_scorecard),
        },
        "source_statuses": {
            "result": status(result),
            "profile": status(profile),
            "capacity_plan": status(capacity),
        },
        "decision": {
            "plan_ready": plan_ready,
            "same_profile_repeat_allowed": False,
            "future_remote_allowed_by_this_plan": False,
            "future_remote_requires_separate_rationale": True,
            "cap75_remote_rationale_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "hard_blockers": hard_blockers,
            "required_before_future_remote": [
                "materialize the primary profile locally",
                "run runner scaffold smoke for pair-completion and pending-opp-credit guards",
                "run manifest builder and manifest verifier",
                "confirm no active xuan no-order runner conflict",
                "keep capacity at cap25 until density and residual gates clear together",
            ],
            "next_action": "materialize_primary_profile_and_run_local_smoke_manifest"
            if plan_ready
            else "stay_local_until_pair_completion_plan_inputs_are_clean",
        },
        "diagnostics": {
            "density_gaps": {
                "accepted_actions": gap(accepted, min_accepted),
                "queue_supported_fills": gap(fills, min_fills),
                "strict_rescue_or_salvage_rows": gap(rescues, min_rescues),
            },
            "guard_pressure": {
                "pair_completion_blocks": pair_blocks,
                "surplus_budget_blocks": surplus_blocks,
                "pair_completion_block_ratio_to_accepted": pair_blocks / accepted if accepted else None,
                "strict_rescue_net_pair_cost_p50": strict_cost_p50,
                "strict_rescue_net_pair_cost_p90": strict_cost_p90,
                "selected_required_above_net_cap": trigger_cap,
            },
            "residual_signal": {
                "residual_preflight_zero": bool(decision.get("residual_preflight_zero")),
                "residual_qty_share": fnum(observed.get("residual_qty_share")),
                "residual_cost_share": fnum(observed.get("residual_cost_share")),
            },
        },
        "next_profile_hypothesis": {
            "candidate": "cap25_density_preserving_pair_completion_guard",
            "primary_profile_overrides": primary,
            "local_only_variants": {
                "residual_strict": residual_strict_variant,
                "density_recovery": density_recovery_variant,
            },
            "rationale": [
                "1510Z result showed residual preflight zero but density just below cap25 thresholds.",
                "Pair-completion blocks were high relative to accepted actions, so the next profile should reduce binary pair-completion pressure.",
                "Move the risk-seed pair-completion trigger from p50 toward the midpoint of p50 and p90 instead of relaxing source or surplus gates.",
                "Use a 0.99 pair-completion net cap with a positive pair-completion PnL floor to recover density without treating >1.0 pair cost as acceptable.",
                "Keep pending opposite credit at zero and keep strict rescue surplus/residual budgets capped.",
            ],
        },
        "safety": {
            "orders_sent_allowed": False,
            "remote_repo_mutation_allowed": False,
            "shared_service_mutation_allowed": False,
            "collector_rebuild_allowed": False,
            "cap75_or_higher_allowed": False,
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-scorecard", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--capacity-plan-scorecard", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--min-required-above-net-cap", type=float, default=0.985)
    parser.add_argument("--max-required-above-net-cap", type=float, default=1.01)
    parser.add_argument("--default-required-above-net-cap", type=float, default=0.99)
    parser.add_argument("--risk-seed-pair-completion-min-qty", type=float, default=1.25)
    parser.add_argument("--pair-completion-net-cap", type=float, default=0.99)
    parser.add_argument("--max-pair-completion-net-cap", type=float, default=0.99)
    parser.add_argument("--pair-completion-min-pair-pnl-after", type=float, default=0.01)
    parser.add_argument("--max-surplus-budget-abs-unpaired-cost", type=float, default=1.44)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = args.scorecard_json.expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = args.markdown.expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown(card) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
