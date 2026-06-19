#!/usr/bin/env python3
"""Pre-authorize the capped-diagnostics cap25 retry rationale.

This local-only gate checks whether the 1654Z exit-137 result plus the capped
diagnostics runner/manifest path justify a future bounded cap25 PM_DRY_RUN. It
does not launch remote jobs, authorize deploys, restart services, mutate shared
state, or send orders.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_RESULT = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_result_20260526T1654Z.json"
)
DEFAULT_PROFILE = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_profile_capped_diagnostics_20260526T1750Z.json"
)
DEFAULT_MANIFEST = Path(
    ".tmp_xuan/scorecards/no_order_soft_mainline_density_preserving_pair_completion_capped_manifest_20260526T1750Z.json"
)
DEFAULT_MANIFEST_VERIFIER = Path(
    ".tmp_xuan/scorecards/no_order_soft_mainline_density_preserving_pair_completion_capped_manifest_verifier_20260526T1750Z.json"
)
DEFAULT_SMOKE = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_no_order_runner_scaffold_smoke_capped_diagnostics_20260526T1750Z/smoke_report.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_capped_diagnostics_pre_authorization_gate_20260526T1758Z.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_capped_diagnostics_pre_authorization_gate_20260526T1758Z/PRE_AUTHORIZATION_GATE.md"
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


def smoke_passed(smoke: dict[str, Any]) -> bool:
    if smoke.get("status") != "PASS":
        return False
    if smoke.get("remote_runner_started") is not False:
        return False
    if smoke.get("raw_replay_collector_scanned") is not False:
        return False
    for check in smoke.get("checks") or []:
        if not isinstance(check, dict) or check.get("status") != "PASS":
            return False
    return True


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    o = card["result_observed"]
    p = card["profile_excerpt"]
    lines = [
        "# Xuan Capped-Diagnostics Pre-Authorization Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- pre_authorization_local_gate_pass: `{d['pre_authorization_local_gate_pass']}`",
        f"- bounded_pm_dry_run_rationale_ready: `{d['bounded_pm_dry_run_rationale_ready']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        f"- future_remote_requires_new_wake: `{d['future_remote_requires_new_wake']}`",
        f"- same_uncapped_profile_repeat_allowed: `{d['same_uncapped_profile_repeat_allowed']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Result Signal",
        "",
        f"- run_exit_code: `{o['run_exit_code']}`",
        f"- accepted/fills/rescues: `{o['accepted_actions']}` / `{o['queue_supported_fills']}` / `{o['strict_rescue_or_salvage_rows']}`",
        f"- rescue_per_candidate/rescue_per_fill: `{o['rescue_per_candidate']}` / `{o['rescue_per_fill']}`",
        f"- residual qty/cost share: `{o['residual_qty_share']}` / `{o['residual_cost_share']}`",
        f"- source_blocks/bad_json: `{o['source_blocks']}` / `{o['bad_event_json']}`",
        "",
        "## Capped Candidate",
        "",
        f"- target_qty: `{p['target_qty']}`",
        f"- rescue_block_diagnostics_max_per_slug: `{p['rescue_block_diagnostics_max_per_slug']}`",
        f"- pair_completion_net_cap: `{p['pair_completion_net_cap']}`",
        f"- pair_completion_min_pair_pnl_after: `{p['pair_completion_min_pair_pnl_after']}`",
        "",
        "## Interpretation",
        "",
        "- This gate only prepares a future cap25 PM_DRY_RUN rationale; it does not authorize a remote in the current wake.",
        "- The rationale is runtime-volume specific: retry only with capped rescue-block diagnostics and the same source/economic guardrails.",
        "- Cap75/150/300 remains blocked until cap25 runtime, density, and residual gates clear together.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    result = load_json(args.result_scorecard)
    profile = load_json(args.profile_scorecard)
    manifest = load_json(args.manifest_scorecard)
    verifier = load_json(args.manifest_verifier_scorecard)
    smoke = load_json(args.scaffold_smoke_report)

    result_decision = body(result, "decision")
    result_observed = body(result, "observed")
    result_interpretation = body(result, "interpretation")
    profile_body = body(profile, "profile")
    verifier_template = body(verifier, "template_checks")

    cap = fnum(profile_body.get("rescue_block_diagnostics_max_per_slug"))
    hard_blockers: list[str] = []
    if not status(result).startswith(
        "BLOCKED_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_RESULT_EXIT_137"
    ):
        hard_blockers.append("result_not_expected_exit_137_blocked")
    if result_decision.get("future_remote_allowed_by_this_result") is not False:
        hard_blockers.append("result_remote_allowed_not_false")
    if result_decision.get("wrapper_killed") is not True:
        hard_blockers.append("result_wrapper_killed_signal_missing")
    if result_decision.get("aggregate_missing") is not True:
        hard_blockers.append("result_aggregate_missing_signal_missing")
    if result_decision.get("density_recovered_partially") is not True:
        hard_blockers.append("density_partial_recovery_signal_missing")
    if result_decision.get("pair_completion_pressure_reduced") is not True:
        hard_blockers.append("pair_completion_pressure_reduction_missing")
    if result_decision.get("residual_preflight_zero") is not True:
        hard_blockers.append("residual_preflight_zero_missing")
    if result_decision.get("source_review_clean") is not True:
        hard_blockers.append("source_review_not_clean")
    if fnum(result_observed.get("strict_rescue_or_salvage_rows"), 0.0) < args.min_partial_rescue_rows:
        hard_blockers.append("partial_rescue_rows_too_low_for_retry")
    if not is_keep(profile):
        hard_blockers.append("capped_profile_not_keep")
    if fnum(profile_body.get("target_qty"), 0.0) != args.expected_target_qty:
        hard_blockers.append("profile_not_cap25")
    if cap is None or cap <= 0:
        hard_blockers.append("diagnostics_cap_missing")
    elif cap > args.max_rescue_block_diagnostics_per_slug:
        hard_blockers.append("diagnostics_cap_too_high")
    if profile_body.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")
    if fnum(profile_body.get("risk_seed_pending_opp_credit"), 9.0) != 0.0:
        hard_blockers.append("pending_opp_credit_not_zero")
    if not status(manifest).startswith("THIRD_WINDOW_REMOTE_STAGING_MANIFEST_READY_LOCAL_ONLY"):
        hard_blockers.append("capped_manifest_not_ready")
    if body(manifest, "decision").get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_remote_allowed_not_false")
    if not is_keep(verifier):
        hard_blockers.append("capped_manifest_verifier_not_keep")
    if body(verifier, "decision").get("local_manifest_verified") is not True:
        hard_blockers.append("manifest_not_verified")
    if verifier_template.get("remote_command_rescue_diagnostics_max_per_slug") != str(int(cap or 0)):
        hard_blockers.append("verifier_cap_mismatch")
    if not smoke_passed(smoke):
        hard_blockers.append("scaffold_smoke_not_pass")

    gate_pass = not hard_blockers
    card = {
        "artifact": "xuan_shadow_review_capped_diagnostics_pre_authorization_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_capped_diagnostics_pre_authorization_gate.py",
        "status": (
            "KEEP_SHADOW_REVIEW_CAPPED_DIAGNOSTICS_PRE_AUTHORIZATION_GATE_READY_LOCAL_ONLY"
            if gate_pass
            else "BLOCKED_SHADOW_REVIEW_CAPPED_DIAGNOSTICS_PRE_AUTHORIZATION_GATE_LOCAL_ONLY"
        ),
        "inputs": {
            "result_scorecard": str(args.result_scorecard),
            "profile_scorecard": str(args.profile_scorecard),
            "manifest_scorecard": str(args.manifest_scorecard),
            "manifest_verifier_scorecard": str(args.manifest_verifier_scorecard),
            "scaffold_smoke_report": str(args.scaffold_smoke_report),
        },
        "source_statuses": {
            "density_preserving_result": status(result),
            "capped_profile": status(profile),
            "capped_manifest": status(manifest),
            "capped_manifest_verifier": status(verifier),
            "scaffold_smoke": str(smoke.get("status") or ""),
        },
        "decision": {
            "pre_authorization_local_gate_pass": gate_pass,
            "bounded_pm_dry_run_rationale_ready": gate_pass,
            "runtime_volume_blocker_locally_addressed": gate_pass,
            "future_remote_allowed_by_this_gate": False,
            "future_remote_requires_new_wake": True,
            "future_remote_requires_active_runner_conflict_check": True,
            "same_uncapped_profile_repeat_allowed": False,
            "same_capped_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "hard_blockers": hard_blockers,
            "next_action": (
                "future_wake_may_consider_one_bounded_cap25_pm_dry_run_after_active_runner_conflict_check"
                if gate_pass
                else "stay_local_until_capped_diagnostics_pre_authorization_blockers_clear"
            ),
        },
        "result_observed": {
            key: result_observed.get(key)
            for key in (
                "run_exit_code",
                "accepted_actions",
                "queue_supported_fills",
                "strict_rescue_or_salvage_rows",
                "rescue_per_candidate",
                "rescue_per_fill",
                "residual_qty_share",
                "residual_cost_share",
                "source_blocks",
                "bad_event_json",
                "pair_completion_blocks",
                "surplus_budget_blocks",
            )
        },
        "profile_excerpt": {
            key: profile_body.get(key)
            for key in (
                "target_qty",
                "rescue_block_diagnostics_max_per_slug",
                "risk_seed_pair_completion_required_above_net_cap",
                "risk_seed_pair_completion_min_qty",
                "pair_completion_net_cap",
                "pair_completion_min_pair_pnl_after",
                "risk_seed_pending_opp_credit",
                "strict_rescue_surplus_net_cap",
            )
        },
        "interpretation": {
            "result_runtime_signal": result_interpretation.get("runtime_signal"),
            "result_density_signal": result_interpretation.get("density_signal"),
            "do_not_run_remote_this_wake": True,
            "do_not_expand_capacity": True,
            "do_not_repeat_uncapped_profile": True,
            "do_not_weaken_source_or_economic_gates": True,
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-scorecard", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--manifest-scorecard", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--manifest-verifier-scorecard", type=Path, default=DEFAULT_MANIFEST_VERIFIER)
    parser.add_argument("--scaffold-smoke-report", type=Path, default=DEFAULT_SMOKE)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--expected-target-qty", type=float, default=25.0)
    parser.add_argument("--min-partial-rescue-rows", type=float, default=6.0)
    parser.add_argument("--max-rescue-block-diagnostics-per-slug", type=float, default=10000.0)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = args.scorecard_json.expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = args.markdown.expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown(card) + "\n", encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
