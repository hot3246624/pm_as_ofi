#!/usr/bin/env python3
"""Pre-authorize the density-preserving pair-completion cap25 rationale.

This local-only gate checks whether the 1510Z blocked run, the redesigned
cap25 profile, scaffold smoke, and staging manifest verifier jointly justify a
future bounded PM_DRY_RUN. It does not launch a remote job, authorize deploys,
restart services, mutate shared state, or send orders.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_seed_risk_reduction_result_20260526T1510Z.json")
DEFAULT_PLAN = Path(".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_plan_20260526T1602Z.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_profile_20260526T1610Z.json")
DEFAULT_MANIFEST = Path(".tmp_xuan/scorecards/no_order_soft_mainline_density_preserving_pair_completion_manifest_20260526T1614Z.json")
DEFAULT_MANIFEST_VERIFIER = Path(
    ".tmp_xuan/scorecards/no_order_soft_mainline_density_preserving_pair_completion_manifest_verifier_20260526T1614Z.json"
)
DEFAULT_SMOKE = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_no_order_runner_scaffold_smoke_density_preserving_pair_completion_20260526T1614Z/smoke_report.json"
)
DEFAULT_CAPACITY = Path(".tmp_xuan/scorecards/xuan_shadow_review_capacity_ladder_evidence_plan_20260526T0855Z.json")
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_pre_authorization_gate_20260526T1650Z.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_density_preserving_pair_completion_pre_authorization_gate_20260526T1650Z/PRE_AUTHORIZATION_GATE.md"
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


def smoke_passed(smoke: dict[str, Any]) -> bool:
    if smoke.get("status") != "PASS":
        return False
    if smoke.get("remote_runner_started") is not False:
        return False
    if smoke.get("raw_replay_collector_scanned") is not False:
        return False
    for check in listify(smoke.get("checks")):
        if not isinstance(check, dict) or check.get("status") != "PASS":
            return False
    return True


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    o = card["prior_result_observed"]
    p = card["profile_excerpt"]
    lines = [
        "# Xuan Density-Preserving Pair-Completion Pre-Authorization Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- pre_authorization_local_gate_pass: `{d['pre_authorization_local_gate_pass']}`",
        f"- bounded_pm_dry_run_rationale_ready: `{d['bounded_pm_dry_run_rationale_ready']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- same_profile_repeat_allowed: `{d['same_profile_repeat_allowed']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Prior Result",
        "",
        f"- status: `{card['source_statuses']['seed_risk_reduction_result']}`",
        f"- run_exit_code: `{o['run_exit_code']}`",
        f"- accepted/fills/rescues: `{o['accepted_actions']}` / `{o['queue_supported_fills']}` / `{o['strict_rescue_or_salvage_rows']}`",
        f"- residual qty/cost share: `{o['residual_qty_share']}` / `{o['residual_cost_share']}`",
        f"- pair_completion_blocks: `{o['pair_completion_blocks']}`",
        "",
        "## Candidate Profile",
        "",
        f"- target_qty: `{p['target_qty']}`",
        f"- risk_seed_pair_completion_required_above_net_cap: `{p['risk_seed_pair_completion_required_above_net_cap']}`",
        f"- pair_completion_net_cap: `{p['pair_completion_net_cap']}`",
        f"- pair_completion_min_pair_pnl_after: `{p['pair_completion_min_pair_pnl_after']}`",
        f"- risk_seed_pending_opp_credit: `{p['risk_seed_pending_opp_credit']}`",
        "",
        "## Interpretation",
        "",
        "- This gate can justify a future bounded cap25 PM_DRY_RUN, but it does not authorize live trading or cap75 expansion.",
        "- The rationale is narrow: retest the density-preserving pair-completion redesign because 1510Z improved residual preflight while overfiltering density.",
        "- Any actual remote launch still must use the verified manifest template, pass active-runner conflict checks, and remain PM_DRY_RUN=1.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    result = load_json(args.result_scorecard)
    plan = load_json(args.plan_scorecard)
    profile = load_json(args.profile_scorecard)
    manifest = load_json(args.manifest_scorecard)
    manifest_verifier = load_json(args.manifest_verifier_scorecard)
    smoke = load_json(args.scaffold_smoke_report)
    capacity = load_json(args.capacity_plan_scorecard)

    result_decision = body(result, "decision")
    result_observed = body(result, "observed")
    result_events = body(result, "event_diagnostics")
    plan_decision = body(plan, "decision")
    profile_decision = body(profile, "decision")
    profile_body = body(profile, "profile")
    manifest_decision = body(manifest, "decision")
    verifier_decision = body(manifest_verifier, "decision")
    capacity_decision = body(capacity, "decision")

    density_shortfall_small = (
        (fnum(result_observed.get("accepted_actions"), 0.0) or 0.0) >= args.min_prior_accepted_actions
        and (fnum(result_observed.get("queue_supported_fills"), 0.0) or 0.0) >= args.min_prior_fills
        and (fnum(result_observed.get("strict_rescue_or_salvage_rows"), 0.0) or 0.0)
        >= args.min_prior_rescue_rows
    )
    residual_preflight_zero = bool(result_decision.get("residual_preflight_zero"))
    pair_completion_overfiltered = bool(result_decision.get("pair_completion_overfiltered"))
    pair_blocks = fnum(result_events.get("pair_completion_blocks"), 0.0) or 0.0
    accepted = fnum(result_observed.get("accepted_actions"), 0.0) or 0.0
    pair_block_ratio = pair_blocks / accepted if accepted else None

    hard_blockers: list[str] = []
    if not status(result).startswith("BLOCKED_SHADOW_REVIEW_SEED_RISK_REDUCTION_RESULT"):
        hard_blockers.append("prior_result_not_expected_blocked_seed_risk_reduction")
    if result_decision.get("future_remote_allowed_by_this_result") is not False:
        hard_blockers.append("prior_result_remote_allowed_not_false")
    if not pair_completion_overfiltered:
        hard_blockers.append("pair_completion_overfilter_signal_missing")
    if not residual_preflight_zero:
        hard_blockers.append("residual_preflight_zero_signal_missing")
    if not density_shortfall_small:
        hard_blockers.append("prior_density_shortfall_not_small_enough")
    if pair_block_ratio is None or pair_block_ratio < args.min_pair_completion_block_ratio:
        hard_blockers.append("pair_completion_block_ratio_not_material")
    if not is_keep(plan):
        hard_blockers.append("pair_completion_plan_not_keep")
    if plan_decision.get("future_remote_allowed_by_this_plan") is not False:
        hard_blockers.append("plan_remote_allowed_not_false")
    if not is_keep(profile):
        hard_blockers.append("profile_not_keep")
    if profile_decision.get("future_remote_allowed_by_this_profile") is not False:
        hard_blockers.append("profile_remote_allowed_not_false")
    if not status(manifest).startswith("THIRD_WINDOW_REMOTE_STAGING_MANIFEST_READY_LOCAL_ONLY"):
        hard_blockers.append("manifest_not_ready")
    if manifest_decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_remote_allowed_not_false")
    if not is_keep(manifest_verifier):
        hard_blockers.append("manifest_verifier_not_keep")
    if verifier_decision.get("local_manifest_verified") is not True:
        hard_blockers.append("manifest_verifier_not_verified")
    if verifier_decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_verifier_remote_allowed_not_false")
    if not smoke_passed(smoke):
        hard_blockers.append("scaffold_smoke_not_pass")
    if not is_keep(capacity):
        hard_blockers.append("capacity_plan_not_keep")
    if capacity_decision.get("cap75_remote_rationale_ready") is True:
        hard_blockers.append("capacity_plan_cap75_rationale_unexpected")
    if fnum(profile_body.get("target_qty"), 0.0) != args.expected_target_qty:
        hard_blockers.append("profile_not_cap25")
    if profile_body.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")
    if fnum(profile_body.get("risk_seed_pending_opp_credit"), 9.0) != 0.0:
        hard_blockers.append("pending_opp_credit_not_zero")
    if fnum(profile_body.get("pair_completion_net_cap"), 9.0) > args.max_pair_completion_net_cap:
        hard_blockers.append("pair_completion_net_cap_too_loose")
    if fnum(profile_body.get("pair_completion_min_pair_pnl_after"), 0.0) < args.min_pair_completion_pnl_after:
        hard_blockers.append("pair_completion_pnl_floor_too_low")
    if fnum(profile_body.get("surplus_budget_max_abs_unpaired_cost"), 9.0) > args.max_surplus_budget_abs_unpaired_cost:
        hard_blockers.append("surplus_budget_relaxed")
    if fnum(profile_body.get("strict_rescue_surplus_net_cap"), 9.0) > args.max_strict_rescue_surplus_net_cap:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")

    gate_pass = not hard_blockers
    bounded_rationale_ready = gate_pass
    status_value = (
        "KEEP_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_PRE_AUTHORIZATION_GATE_PASS_LOCAL_ONLY"
        if gate_pass
        else "BLOCKED_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_PRE_AUTHORIZATION_GATE_LOCAL_ONLY"
    )
    card = {
        "artifact": "xuan_shadow_review_density_preserving_pair_completion_pre_authorization_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_density_preserving_pair_completion_pre_authorization_gate.py",
        "status": status_value,
        "inputs": {
            "result_scorecard": str(args.result_scorecard),
            "plan_scorecard": str(args.plan_scorecard),
            "profile_scorecard": str(args.profile_scorecard),
            "manifest_scorecard": str(args.manifest_scorecard),
            "manifest_verifier_scorecard": str(args.manifest_verifier_scorecard),
            "scaffold_smoke_report": str(args.scaffold_smoke_report),
            "capacity_plan_scorecard": str(args.capacity_plan_scorecard),
        },
        "source_statuses": {
            "seed_risk_reduction_result": status(result),
            "pair_completion_plan": status(plan),
            "pair_completion_profile": status(profile),
            "manifest": status(manifest),
            "manifest_verifier": status(manifest_verifier),
            "scaffold_smoke": str(smoke.get("status") or ""),
            "capacity_plan": status(capacity),
        },
        "decision": {
            "pre_authorization_local_gate_pass": gate_pass,
            "bounded_pm_dry_run_rationale_ready": bounded_rationale_ready,
            "future_remote_allowed_by_this_gate": False,
            "future_remote_requires_active_runner_conflict_check": True,
            "same_profile_repeat_allowed": False,
            "cap25_profile_rationale_ready": bounded_rationale_ready,
            "cap75_remote_rationale_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "hard_blockers": hard_blockers,
            "required_before_remote_launch": [
                "confirm no active xuan no-order runner conflict",
                "use manifest remote_launch_command_template exactly",
                "keep PM_DRY_RUN=1 and duration_s <= 1800",
                "stage only files listed in the verified manifest",
                "run as pmofi and prepare only the xuan output dir",
            ],
            "next_action": "active_runner_conflict_check_then_at_most_one_bounded_cap25_pm_dry_run"
            if bounded_rationale_ready
            else "stay_local_until_pre_authorization_blockers_clear",
        },
        "prior_result_observed": {
            "run_exit_code": fnum(result_observed.get("run_exit_code")),
            "accepted_actions": accepted,
            "queue_supported_fills": fnum(result_observed.get("queue_supported_fills")),
            "strict_rescue_or_salvage_rows": fnum(result_observed.get("strict_rescue_or_salvage_rows")),
            "fill_rate": fnum(result_observed.get("fill_rate")),
            "rescue_per_candidate": fnum(result_observed.get("rescue_per_candidate")),
            "rescue_per_fill": fnum(result_observed.get("rescue_per_fill")),
            "residual_qty_share": fnum(result_observed.get("residual_qty_share")),
            "residual_cost_share": fnum(result_observed.get("residual_cost_share")),
            "pair_completion_blocks": pair_blocks,
            "pair_completion_block_ratio_to_accepted": pair_block_ratio,
            "source_blocks": fnum(result_observed.get("source_blocks")),
            "bad_event_json": fnum(result_observed.get("bad_event_json")),
        },
        "profile_excerpt": {
            "target_qty": fnum(profile_body.get("target_qty")),
            "risk_seed_pair_completion_required_above_net_cap": fnum(
                profile_body.get("risk_seed_pair_completion_required_above_net_cap")
            ),
            "risk_seed_pair_completion_min_qty": fnum(profile_body.get("risk_seed_pair_completion_min_qty")),
            "pair_completion_net_cap": fnum(profile_body.get("pair_completion_net_cap")),
            "pair_completion_min_pair_pnl_after": fnum(profile_body.get("pair_completion_min_pair_pnl_after")),
            "risk_seed_pending_opp_credit": fnum(profile_body.get("risk_seed_pending_opp_credit")),
            "surplus_budget_max_abs_unpaired_cost": fnum(profile_body.get("surplus_budget_max_abs_unpaired_cost")),
            "strict_rescue_surplus_net_cap": fnum(profile_body.get("strict_rescue_surplus_net_cap")),
        },
        "rationale": [
            "1510Z showed residual preflight zero, so the residual direction is worth retesting.",
            "1510Z was blocked by timeout and weak density, so it is not promotion evidence and the same profile must not repeat.",
            "Pair-completion blocks were material relative to accepted actions, supporting a density-preserving pair-completion redesign.",
            "The 1614Z candidate remains cap25, keeps pending opposite credit at zero, keeps pair-completion cost below 1.0, and does not relax source or surplus gates.",
            "Manifest verifier and scaffold smoke passed locally; this justifies only a future bounded PM_DRY_RUN, not live trading or cap75 expansion.",
        ],
        "safety": {
            "orders_sent_allowed": False,
            "remote_repo_mutation_allowed": False,
            "shared_service_mutation_allowed": False,
            "collector_rebuild_allowed": False,
            "deploy_or_restart_allowed": False,
            "cap75_or_higher_allowed": False,
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-scorecard", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--plan-scorecard", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--manifest-scorecard", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--manifest-verifier-scorecard", type=Path, default=DEFAULT_MANIFEST_VERIFIER)
    parser.add_argument("--scaffold-smoke-report", type=Path, default=DEFAULT_SMOKE)
    parser.add_argument("--capacity-plan-scorecard", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--expected-target-qty", type=float, default=25.0)
    parser.add_argument("--min-prior-accepted-actions", type=float, default=18.0)
    parser.add_argument("--min-prior-fills", type=float, default=15.0)
    parser.add_argument("--min-prior-rescue-rows", type=float, default=5.0)
    parser.add_argument("--min-pair-completion-block-ratio", type=float, default=3.0)
    parser.add_argument("--max-pair-completion-net-cap", type=float, default=0.99)
    parser.add_argument("--min-pair-completion-pnl-after", type=float, default=0.01)
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
