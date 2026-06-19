#!/usr/bin/env python3
"""Pre-authorize old-system tail-mark snapshot evidence collection locally.

This gate verifies the tail-mark snapshot profile, manifest, verifier, and
scaffold smoke as a local-only chain. It prepares a future bounded cap25
PM_DRY_RUN rationale for evidence collection only. It does not launch remote
jobs, deploy, restart services, mutate shared state, send orders, or claim
private truth.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0338Z"

DEFAULT_PROFILE = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_tail_mark_snapshot_profile_{STAMP}.json"
)
DEFAULT_MANIFEST = Path(f".tmp_xuan/scorecards/no_order_soft_mainline_tail_mark_snapshot_manifest_{STAMP}.json")
DEFAULT_VERIFIER = Path(
    f".tmp_xuan/scorecards/no_order_soft_mainline_tail_mark_snapshot_manifest_verifier_{STAMP}.json"
)
DEFAULT_SMOKE = Path(
    f".tmp_xuan/local_verifier_artifacts/xuan_no_order_runner_scaffold_smoke_tail_mark_snapshot_{STAMP}/"
    "smoke_report.json"
)
DEFAULT_LOT_FRONTIER = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier_20260527T0325Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_tail_mark_snapshot_pre_authorization_gate_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    f".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_old_system_tail_mark_snapshot_pre_authorization_gate_{STAMP}/"
    "PRE_AUTHORIZATION_GATE.md"
)


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


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
    return all(isinstance(check, dict) and check.get("status") == "PASS" for check in listify(smoke.get("checks")))


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    p = card["profile_excerpt"]
    m = card["manifest_checks"]
    f = card["frontier_context"]
    lines = [
        "# Old-System Tail Mark Snapshot Pre-Authorization Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- pre_authorization_local_gate_pass: `{d['pre_authorization_local_gate_pass']}`",
        f"- bounded_pm_dry_run_rationale_ready: `{d['bounded_pm_dry_run_rationale_ready']}`",
        f"- tail_mark_snapshot_evidence_rationale_ready: `{d['tail_mark_snapshot_evidence_rationale_ready']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        f"- future_remote_requires_new_wake: `{d['future_remote_requires_new_wake']}`",
        f"- future_remote_requires_active_runner_conflict_check: `{d['future_remote_requires_active_runner_conflict_check']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Profile",
        "",
        f"- target_qty/duration_s: `{p['target_qty']}` / `{p['duration_s']}`",
        f"- profile_family: `{p['profile_family']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{p['surplus_budget_max_abs_unpaired_cost']}`",
        f"- write_tail_mark_snapshots: `{p['write_tail_mark_snapshots']}`",
        f"- tail mark cadence: `{p['tail_mark_snapshot_min_age_s']}` / `{p['tail_mark_snapshot_max_per_lot']}` / `{p['tail_mark_snapshot_min_interval_s']}`",
        "",
        "## Manifest",
        "",
        f"- manifest_verified: `{m['manifest_verified']}`",
        f"- scaffold_smoke_pass: `{m['scaffold_smoke_pass']}`",
        f"- remote command has PM_DRY_RUN: `{m['remote_command_has_pm_dry_run']}`",
        f"- remote command has tail mark snapshots: `{m['remote_command_has_tail_mark_snapshots']}`",
        f"- remote launch captures child: `{m['remote_launch_captures_child']}`",
        "",
        "## Frontier Context",
        "",
        f"- prior lot frontier: `{f['lot_frontier_status']}`",
        f"- mature recovery ready: `{f['observed_mature_recovery_evidence_ready']}`",
        f"- early recovery on total cost: `{f['early_mark_recovery_rate_on_total_cost']}`",
        f"- mature recovery on total cost: `{f['mature_mark_recovery_rate_on_total_cost']}`",
        "",
        "## Boundary",
        "",
        "- This gate supports only a future cap25 evidence-collection PM_DRY_RUN.",
        "- It is not a deploy, live, cap75, or private-truth authorization.",
        "- The run must wait for a future wake and an active-runner conflict check.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    profile_card = load_json(args.profile_scorecard)
    manifest = load_json(args.manifest_scorecard)
    verifier = load_json(args.manifest_verifier_scorecard)
    smoke = load_json(args.scaffold_smoke_report)
    lot_frontier = load_json(args.lot_frontier_scorecard)

    profile = body(profile_card, "candidate_profile")
    profile_decision = body(profile_card, "decision")
    manifest_decision = body(manifest, "decision")
    manifest_safety = body(manifest, "safety")
    verifier_decision = body(verifier, "decision")
    template_checks = body(verifier, "template_checks")
    lot_decision = body(lot_frontier, "decision")
    batch_observed = body(lot_frontier, "batch_observed")
    early_mark = body(batch_observed, "early_mark_proxy_aggregate_no_fee")
    mature_mark = body(batch_observed, "mature_actionable_mark_aggregate_no_fee")

    hard_blockers: list[str] = []
    if not is_keep(profile_card):
        hard_blockers.append("tail_mark_profile_not_keep")
    if profile_decision.get("tail_mark_snapshot_profile_ready") is not True:
        hard_blockers.append("tail_mark_profile_not_ready")
    if profile_decision.get("behavior_profile_unchanged_except_instrumentation") is not True:
        hard_blockers.append("profile_changes_behavior_controls")
    if profile_decision.get("future_remote_allowed_by_this_profile") is not False:
        hard_blockers.append("profile_remote_allowed_not_false")
    if profile.get("write_tail_mark_snapshots") is not True:
        hard_blockers.append("tail_mark_snapshots_not_enabled_in_profile")
    if fnum(profile.get("duration_s"), 0.0) > args.max_duration_s:
        hard_blockers.append("profile_duration_exceeds_1800")
    if fnum(profile.get("target_qty"), 0.0) != args.expected_target_qty:
        hard_blockers.append("profile_not_cap25")
    if profile.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_enabled")
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
    if body(manifest, "profile").get("write_tail_mark_snapshots") is not True:
        hard_blockers.append("manifest_profile_tail_mark_not_true")
    if not is_keep(verifier):
        hard_blockers.append("manifest_verifier_not_keep")
    if verifier_decision.get("local_manifest_verified") is not True:
        hard_blockers.append("manifest_not_verified")
    if verifier_decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_verifier_remote_allowed_not_false")
    if template_checks.get("remote_command_has_pm_dry_run") is not True:
        hard_blockers.append("remote_command_missing_pm_dry_run")
    if template_checks.get("remote_command_has_tail_mark_snapshots") is not True:
        hard_blockers.append("remote_command_missing_tail_mark_snapshots")
    if template_checks.get("remote_command_duration_s") != args.max_duration_s:
        hard_blockers.append("remote_command_duration_not_1800")
    if template_checks.get("remote_command_timeout_s") != args.max_duration_s + args.timeout_buffer_s:
        hard_blockers.append("remote_command_timeout_not_2100")
    if template_checks.get("remote_launch_command_has_socket_readable_run_user") is not True:
        hard_blockers.append("remote_launch_not_socket_readable_user")
    if template_checks.get("remote_launch_command_prepares_run_user_output_dir") is not True:
        hard_blockers.append("remote_launch_output_dir_not_scoped_to_run_user")
    if not all(
        template_checks.get(key) is True
        for key in (
            "remote_launch_command_has_run_exit_capture",
            "remote_launch_command_has_run_stdout_capture",
            "remote_launch_command_has_run_stderr_capture",
            "remote_launch_command_has_wrapper_pid",
            "remote_launch_command_has_stdin_detach",
        )
    ):
        hard_blockers.append("remote_launch_child_capture_incomplete")
    if not smoke_passed(smoke):
        hard_blockers.append("scaffold_smoke_not_pass")
    if "MATURE_MARK_EVIDENCE_GAP" not in status(lot_frontier):
        hard_blockers.append("lot_frontier_not_mature_mark_gap")
    if lot_decision.get("observed_mature_recovery_evidence_ready") is not False:
        hard_blockers.append("lot_frontier_unexpectedly_mature_recovery_ready")

    ready = not hard_blockers
    card = {
        "artifact": "xuan_shadow_review_old_system_tail_mark_snapshot_pre_authorization_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_old_system_tail_mark_snapshot_pre_authorization_gate.py",
        "status": (
            "KEEP_SHADOW_REVIEW_OLD_SYSTEM_TAIL_MARK_SNAPSHOT_PRE_AUTHORIZATION_GATE_PASS_LOCAL_ONLY"
            if ready
            else "BLOCKED_SHADOW_REVIEW_OLD_SYSTEM_TAIL_MARK_SNAPSHOT_PRE_AUTHORIZATION_GATE_LOCAL_ONLY"
        ),
        "inputs": {
            "profile_scorecard": str(args.profile_scorecard),
            "manifest_scorecard": str(args.manifest_scorecard),
            "manifest_verifier_scorecard": str(args.manifest_verifier_scorecard),
            "scaffold_smoke_report": str(args.scaffold_smoke_report),
            "lot_frontier_scorecard": str(args.lot_frontier_scorecard),
            "use_backtest_v1_candidate_audit_pack": False,
        },
        "input_statuses": {
            "profile": status(profile_card),
            "manifest": status(manifest),
            "manifest_verifier": status(verifier),
            "scaffold_smoke": str(smoke.get("status") or ""),
            "lot_frontier": status(lot_frontier),
        },
        "profile_excerpt": rounded(
            {
                "target_qty": fnum(profile.get("target_qty")),
                "duration_s": fnum(profile.get("duration_s")),
                "profile_family": profile.get("profile_family"),
                "tail_mitigation_hypothesis": profile.get("tail_mitigation_hypothesis"),
                "surplus_budget_max_abs_unpaired_cost": fnum(profile.get("surplus_budget_max_abs_unpaired_cost")),
                "pair_completion_net_cap": fnum(profile.get("pair_completion_net_cap")),
                "strict_rescue_surplus_net_cap": fnum(profile.get("strict_rescue_surplus_net_cap")),
                "write_tail_mark_snapshots": profile.get("write_tail_mark_snapshots"),
                "tail_mark_snapshot_min_age_s": fnum(profile.get("tail_mark_snapshot_min_age_s")),
                "tail_mark_snapshot_max_per_lot": fnum(profile.get("tail_mark_snapshot_max_per_lot")),
                "tail_mark_snapshot_min_interval_s": fnum(profile.get("tail_mark_snapshot_min_interval_s")),
            }
        ),
        "frontier_context": rounded(
            {
                "lot_frontier_status": status(lot_frontier),
                "observed_mature_recovery_evidence_ready": lot_decision.get(
                    "observed_mature_recovery_evidence_ready"
                ),
                "early_mark_recovery_rate_on_total_cost": early_mark.get(
                    "mark_recovery_rate_no_fee_on_total_cost"
                ),
                "mature_mark_recovery_rate_on_total_cost": mature_mark.get(
                    "mark_recovery_rate_no_fee_on_total_cost"
                ),
                "frontier_blockers": listify(lot_decision.get("hard_blockers")),
            }
        ),
        "manifest_checks": {
            "manifest_verified": verifier_decision.get("local_manifest_verified") is True,
            "scaffold_smoke_pass": smoke_passed(smoke),
            "remote_command_has_pm_dry_run": template_checks.get("remote_command_has_pm_dry_run") is True,
            "remote_command_has_tail_mark_snapshots": template_checks.get(
                "remote_command_has_tail_mark_snapshots"
            )
            is True,
            "remote_command_tail_mark_snapshot_min_age_s": template_checks.get(
                "remote_command_tail_mark_snapshot_min_age_s"
            ),
            "remote_command_tail_mark_snapshot_max_per_lot": template_checks.get(
                "remote_command_tail_mark_snapshot_max_per_lot"
            ),
            "remote_command_tail_mark_snapshot_min_interval_s": template_checks.get(
                "remote_command_tail_mark_snapshot_min_interval_s"
            ),
            "remote_launch_captures_child": all(
                template_checks.get(key) is True
                for key in (
                    "remote_launch_command_has_run_exit_capture",
                    "remote_launch_command_has_run_stdout_capture",
                    "remote_launch_command_has_run_stderr_capture",
                    "remote_launch_command_has_wrapper_pid",
                    "remote_launch_command_has_stdin_detach",
                )
            ),
        },
        "decision": {
            "pre_authorization_local_gate_pass": ready,
            "bounded_pm_dry_run_rationale_ready": ready,
            "tail_mark_snapshot_evidence_rationale_ready": ready,
            "future_remote_allowed_by_this_gate": False,
            "future_remote_requires_new_wake": True,
            "future_remote_requires_active_runner_conflict_check": True,
            "remote_runner_allowed": False,
            "cap75_remote_rationale_ready": False,
            "deployable": False,
            "live_orders_allowed": False,
            "private_truth_ready": False,
            "paper_shadow_only": True,
            "research_only": True,
            "hard_blockers": sorted(set(hard_blockers)),
            "next_action": (
                "future_wake_may_run_exactly_one_bounded_cap25_tail_mark_snapshot_PM_DRY_RUN_after_active_runner_check"
                if ready
                else "fix_tail_mark_snapshot_pre_authorization_blockers_before_any_remote"
            ),
        },
    }
    return rounded(card)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--manifest-scorecard", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--manifest-verifier-scorecard", type=Path, default=DEFAULT_VERIFIER)
    parser.add_argument("--scaffold-smoke-report", type=Path, default=DEFAULT_SMOKE)
    parser.add_argument("--lot-frontier-scorecard", type=Path, default=DEFAULT_LOT_FRONTIER)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--expected-target-qty", type=float, default=25.0)
    parser.add_argument("--max-duration-s", type=int, default=1800)
    parser.add_argument("--timeout-buffer-s", type=int, default=300)
    args = parser.parse_args()
    card = build(args)

    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(card) + "\n", encoding="utf-8")

    print(json.dumps(card, indent=2, sort_keys=True))
    if card["decision"]["hard_blockers"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
