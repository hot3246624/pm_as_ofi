#!/usr/bin/env python3
"""Pre-authorize the mature-tail cap25 profile locally.

This gate checks that the mature-tail runner-control profile, fresh manifest,
manifest verifier, and scaffold smoke jointly justify a future bounded cap25
PM_DRY_RUN. It is local-only: it does not launch remote jobs, authorize deploys,
restart services, mutate shared state, or send orders.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_PROFILE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json"
)
DEFAULT_MANIFEST = Path(
    ".tmp_xuan/scorecards/no_order_soft_mainline_mature_tail_surplus_budget_130_manifest_20260526T2110Z.json"
)
DEFAULT_MANIFEST_VERIFIER = Path(
    ".tmp_xuan/scorecards/no_order_soft_mainline_mature_tail_surplus_budget_130_manifest_verifier_20260526T2110Z.json"
)
DEFAULT_SMOKE = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    "xuan_no_order_runner_scaffold_smoke_mature_tail_surplus_budget_130_20260526T2110Z/"
    "smoke_report.json"
)
DEFAULT_CAPACITY = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_capacity_interpretation_20260526T1915Z.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_pre_authorization_gate_20260526T2110Z.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    "xuan_shadow_review_mature_tail_pre_authorization_gate_20260526T2110Z/"
    "PRE_AUTHORIZATION_GATE.md"
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


def profile_excerpt(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_qty": fnum(profile.get("target_qty")),
        "duration_s": fnum(profile.get("duration_s")),
        "tail_mitigation_hypothesis": profile.get("tail_mitigation_hypothesis"),
        "surplus_budget_max_abs_unpaired_cost": fnum(profile.get("surplus_budget_max_abs_unpaired_cost")),
        "pair_completion_net_cap": fnum(profile.get("pair_completion_net_cap")),
        "pair_completion_min_pair_pnl_after": fnum(profile.get("pair_completion_min_pair_pnl_after")),
        "risk_seed_pair_completion_required_above_net_cap": fnum(
            profile.get("risk_seed_pair_completion_required_above_net_cap")
        ),
        "risk_seed_pair_completion_min_qty": fnum(profile.get("risk_seed_pair_completion_min_qty")),
        "risk_seed_pending_opp_credit": fnum(profile.get("risk_seed_pending_opp_credit")),
        "strict_rescue_surplus_net_cap": fnum(profile.get("strict_rescue_surplus_net_cap")),
        "rescue_block_diagnostics_max_per_slug": fnum(profile.get("rescue_block_diagnostics_max_per_slug")),
        "risk_seed_cancel_on_closeability_net_cap": profile.get("risk_seed_cancel_on_closeability_net_cap"),
    }


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    p = card["profile_excerpt"]
    m = card["manifest_checks"]
    r = card["runner_control_evidence"]
    lines = [
        "# Xuan Mature Tail Pre-Authorization Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- pre_authorization_local_gate_pass: `{d['pre_authorization_local_gate_pass']}`",
        f"- bounded_pm_dry_run_rationale_ready: `{d['bounded_pm_dry_run_rationale_ready']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        f"- future_remote_requires_new_wake: `{d['future_remote_requires_new_wake']}`",
        f"- future_remote_requires_active_runner_conflict_check: `{d['future_remote_requires_active_runner_conflict_check']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Candidate Profile",
        "",
        f"- tail_mitigation_hypothesis: `{p['tail_mitigation_hypothesis']}`",
        f"- target_qty/duration_s: `{p['target_qty']}` / `{p['duration_s']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{p['surplus_budget_max_abs_unpaired_cost']}`",
        f"- pair_completion_net_cap: `{p['pair_completion_net_cap']}`",
        f"- strict_rescue_surplus_net_cap: `{p['strict_rescue_surplus_net_cap']}`",
        f"- rescue_block_diagnostics_max_per_slug: `{p['rescue_block_diagnostics_max_per_slug']}`",
        f"- risk_seed_cancel_on_closeability_net_cap: `{p['risk_seed_cancel_on_closeability_net_cap']}`",
        "",
        "## Runner-Control Evidence",
        "",
        f"- residual_gates_pass_under_local_proxy: `{r['residual_gates_pass_under_local_proxy']}`",
        f"- density_lower_bound_pass: `{r['density_lower_bound_pass']}`",
        f"- accepted/fills lower bound: `{r['accepted_after_lower_bound']}` / `{r['fills_after_lower_bound']}`",
        f"- fill-rate lower bound: `{r['fill_rate_after_lower_bound']}`",
        f"- after residual cost share: `{r['after_residual_cost_share']}`",
        f"- after residual cost-to-pair qty: `{r['after_residual_cost_to_pair_qty']}`",
        f"- stressed zero-stress PnL: `{r['after_zero_stress_after_non_residual_blocked_slug_pnl_subtract']}`",
        "",
        "## Manifest Checks",
        "",
        f"- profile_source_field: `{m['profile_source_field']}`",
        f"- manifest_verified: `{m['manifest_verified']}`",
        f"- scaffold_smoke_pass: `{m['scaffold_smoke_pass']}`",
        f"- remote command has PM_DRY_RUN: `{m['remote_command_has_pm_dry_run']}`",
        f"- remote launch captures exit/stdout/stderr/pid: `{m['remote_launch_captures_child']}`",
        f"- remote command surplus budget matches profile: `{m['remote_command_surplus_budget_matches_profile']}`",
        "",
        "## Boundary",
        "",
        "- This gate prepares a future cap25 PM_DRY_RUN rationale only; it does not authorize a remote in this wake.",
        "- A future launch still requires active-runner conflict check, one-remote-per-wake availability, and exact manifest template use.",
        "- No cap75/150/300, no deploy, no live orders, and no private-truth claim.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    profile_card = load_json(args.profile_scorecard)
    manifest = load_json(args.manifest_scorecard)
    verifier = load_json(args.manifest_verifier_scorecard)
    smoke = load_json(args.scaffold_smoke_report)
    capacity = load_json(args.capacity_interpretation_scorecard)

    profile = body(profile_card, "candidate_profile")
    profile_decision = body(profile_card, "decision")
    residual_proxy = body(profile_card, "residual_proxy_after_control")
    density = body(profile_card, "density_lower_bound")
    manifest_decision = body(manifest, "decision")
    manifest_safety = body(manifest, "safety")
    verifier_decision = body(verifier, "decision")
    template_checks = body(verifier, "template_checks")
    capacity_decision = body(capacity, "decision")

    hard_blockers: list[str] = []
    if not is_keep(profile_card):
        hard_blockers.append("runner_control_profile_not_keep")
    if profile_decision.get("candidate_profile_ready") is not True:
        hard_blockers.append("runner_control_candidate_profile_not_ready")
    if profile_decision.get("selected_tail_cut_or_pair_complete") is not True:
        hard_blockers.append("selected_tail_not_covered")
    if profile_decision.get("residual_gates_pass_under_local_proxy") is not True:
        hard_blockers.append("residual_proxy_not_passed")
    if profile_decision.get("density_lower_bound_pass") is not True:
        hard_blockers.append("density_lower_bound_not_passed")
    if profile_decision.get("source_economic_gates_preserved") is not True:
        hard_blockers.append("source_or_economic_gate_not_preserved")
    if profile_decision.get("future_remote_allowed_by_this_profile") is not False:
        hard_blockers.append("profile_remote_allowed_not_false")
    if fnum(profile.get("duration_s"), 0.0) > args.max_duration_s:
        hard_blockers.append("profile_duration_exceeds_1800")
    if fnum(profile.get("target_qty"), 0.0) != args.expected_target_qty:
        hard_blockers.append("profile_not_cap25")
    if fnum(profile.get("surplus_budget_max_abs_unpaired_cost"), 9.0) != args.expected_surplus_budget:
        hard_blockers.append("surplus_budget_not_expected_mature_tail_value")
    if fnum(profile.get("pair_completion_net_cap"), 9.0) > args.max_pair_completion_net_cap:
        hard_blockers.append("pair_completion_net_cap_too_loose")
    if fnum(profile.get("strict_rescue_surplus_net_cap"), 9.0) > args.max_strict_rescue_surplus_net_cap:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if profile.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_enabled")
    if fnum(profile.get("risk_seed_pending_opp_credit"), 9.0) != 0.0:
        hard_blockers.append("pending_opp_credit_not_zero")
    for key in (
        "source_quality_require_l1_source",
        "source_quality_require_l2_source",
        "source_quality_require_trade_source",
        "strict_rescue_require_book_source",
        "strict_rescue_require_l2_source",
    ):
        if profile.get(key) is not True:
            hard_blockers.append(f"{key}_not_true")

    if not status(manifest).startswith("THIRD_WINDOW_REMOTE_STAGING_MANIFEST_READY_LOCAL_ONLY"):
        hard_blockers.append("manifest_not_ready")
    if manifest.get("profile_source_field") != "candidate_profile":
        hard_blockers.append("manifest_not_built_from_candidate_profile")
    if manifest_decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_remote_allowed_not_false")
    if manifest_safety.get("pm_dry_run_required") is not True:
        hard_blockers.append("manifest_pm_dry_run_not_required")
    if manifest_safety.get("orders_sent_allowed") is not False:
        hard_blockers.append("manifest_orders_allowed_not_false")
    if body(manifest, "profile").get("surplus_budget_max_abs_unpaired_cost") != args.expected_surplus_budget:
        hard_blockers.append("manifest_profile_surplus_budget_mismatch")
    if not is_keep(verifier):
        hard_blockers.append("manifest_verifier_not_keep")
    if verifier_decision.get("local_manifest_verified") is not True:
        hard_blockers.append("manifest_not_verified")
    if verifier_decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_verifier_remote_allowed_not_false")
    if not smoke_passed(smoke):
        hard_blockers.append("scaffold_smoke_not_pass")
    if capacity_decision.get("cap75_remote_rationale_ready") is True:
        hard_blockers.append("capacity_cap75_rationale_unexpected")
    if capacity_decision.get("private_truth_ready") is True:
        hard_blockers.append("private_truth_unexpected")

    remote_command = str(manifest.get("remote_command_template") or "")
    remote_launch = str(manifest.get("remote_launch_command_template") or "")
    remote_command_surplus_ok = (
        "--surplus-budget-max-abs-unpaired-cost" in remote_command
        and f" {args.expected_surplus_budget}" in remote_command
    )
    remote_launch_captures_child = all(
        token in remote_launch
        for token in ("run_exit_code.txt", "run_stdout.log", "run_stderr.log", "remote_wrapper_pid.txt")
    )
    if "PM_DRY_RUN=1" not in remote_command:
        hard_blockers.append("remote_command_missing_pm_dry_run")
    if not remote_launch_captures_child:
        hard_blockers.append("remote_launch_missing_child_capture")
    if not remote_command_surplus_ok:
        hard_blockers.append("remote_command_surplus_budget_mismatch")
    if template_checks.get("remote_command_duration_s") != args.max_duration_s:
        hard_blockers.append("remote_command_duration_not_standard")
    if template_checks.get("remote_command_rescue_diagnostics_max_per_slug") != str(
        int(fnum(profile.get("rescue_block_diagnostics_max_per_slug"), 0.0) or 0)
    ):
        hard_blockers.append("remote_command_diagnostics_cap_mismatch")

    gate_pass = not hard_blockers
    status_value = (
        "KEEP_SHADOW_REVIEW_MATURE_TAIL_PRE_AUTHORIZATION_GATE_PASS_LOCAL_ONLY"
        if gate_pass
        else "BLOCKED_SHADOW_REVIEW_MATURE_TAIL_PRE_AUTHORIZATION_GATE_LOCAL_ONLY"
    )
    card = {
        "artifact": "xuan_shadow_review_mature_tail_pre_authorization_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_mature_tail_pre_authorization_gate.py",
        "status": status_value,
        "inputs": {
            "profile_scorecard": str(args.profile_scorecard),
            "manifest_scorecard": str(args.manifest_scorecard),
            "manifest_verifier_scorecard": str(args.manifest_verifier_scorecard),
            "scaffold_smoke_report": str(args.scaffold_smoke_report),
            "capacity_interpretation_scorecard": str(args.capacity_interpretation_scorecard),
        },
        "source_statuses": {
            "runner_control_profile": status(profile_card),
            "manifest": status(manifest),
            "manifest_verifier": status(verifier),
            "scaffold_smoke": str(smoke.get("status") or ""),
            "capacity_interpretation": status(capacity),
        },
        "decision": {
            "pre_authorization_local_gate_pass": gate_pass,
            "bounded_pm_dry_run_rationale_ready": gate_pass,
            "cap25_profile_rationale_ready": gate_pass,
            "future_remote_allowed_by_this_gate": False,
            "future_remote_requires_new_wake": True,
            "future_remote_requires_active_runner_conflict_check": True,
            "same_old_capped_profile_repeat_allowed": False,
            "same_mature_tail_profile_repeat_without_new_result_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "paper_shadow_only": True,
            "research_only": True,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "hard_blockers": hard_blockers,
            "required_before_remote_launch": [
                "confirm no active xuan no-order runner conflict",
                "use manifest remote_launch_command_template exactly",
                "keep PM_DRY_RUN=1 and duration_s <= 1800",
                "stage only files listed in the verified manifest",
                "run as pmofi and prepare only the xuan output dir",
                "do not run cap75/150/300",
            ],
            "next_action": (
                "future_wake_may_consider_one_bounded_cap25_pm_dry_run_after_active_runner_conflict_check"
                if gate_pass
                else "stay_local_until_mature_tail_pre_authorization_blockers_clear"
            ),
        },
        "profile_excerpt": profile_excerpt(profile),
        "runner_control_evidence": {
            "residual_gates_pass_under_local_proxy": profile_decision.get(
                "residual_gates_pass_under_local_proxy"
            ),
            "density_lower_bound_pass": profile_decision.get("density_lower_bound_pass"),
            "accepted_after_lower_bound": density.get("accepted_after_lower_bound"),
            "fills_after_lower_bound": density.get("fills_after_lower_bound"),
            "fill_rate_after_lower_bound": density.get("fill_rate_after_lower_bound"),
            "after_residual_cost_share": residual_proxy.get("after_residual_cost_share"),
            "after_residual_cost_to_pair_qty": residual_proxy.get("after_residual_cost_to_pair_qty"),
            "after_residual_zero_stress_pnl": residual_proxy.get("after_residual_zero_stress_pnl"),
            "after_zero_stress_after_non_residual_blocked_slug_pnl_subtract": residual_proxy.get(
                "after_zero_stress_after_non_residual_blocked_slug_pnl_subtract"
            ),
        },
        "manifest_checks": {
            "profile_source_field": manifest.get("profile_source_field"),
            "manifest_verified": verifier_decision.get("local_manifest_verified"),
            "scaffold_smoke_pass": smoke_passed(smoke),
            "remote_command_has_pm_dry_run": "PM_DRY_RUN=1" in remote_command,
            "remote_launch_captures_child": remote_launch_captures_child,
            "remote_command_surplus_budget_matches_profile": remote_command_surplus_ok,
            "remote_command_duration_s": template_checks.get("remote_command_duration_s"),
            "remote_command_timeout_s": template_checks.get("remote_command_timeout_s"),
            "remote_command_rescue_diagnostics_max_per_slug": template_checks.get(
                "remote_command_rescue_diagnostics_max_per_slug"
            ),
            "remote_launch_command_has_socket_readable_run_user": template_checks.get(
                "remote_launch_command_has_socket_readable_run_user"
            ),
            "remote_launch_command_prepares_run_user_output_dir": template_checks.get(
                "remote_launch_command_prepares_run_user_output_dir"
            ),
        },
        "interpretation": {
            "preauth_read": "future_cap25_bounded_pm_dry_run_rationale_ready_local_only"
            if gate_pass
            else "preauthorization_blocked",
            "remote_read": "no_remote_from_this_gate_current_wake",
            "capacity_read": "cap75_still_blocked",
            "live_read": "not_deployable_not_live_authorized",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--manifest-scorecard", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--manifest-verifier-scorecard", type=Path, default=DEFAULT_MANIFEST_VERIFIER)
    parser.add_argument("--scaffold-smoke-report", type=Path, default=DEFAULT_SMOKE)
    parser.add_argument("--capacity-interpretation-scorecard", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--expected-target-qty", type=float, default=25.0)
    parser.add_argument("--max-duration-s", type=float, default=1800.0)
    parser.add_argument("--expected-surplus-budget", type=float, default=1.30)
    parser.add_argument("--max-pair-completion-net-cap", type=float, default=0.99)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
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
