#!/usr/bin/env python3
"""Verify a local third-window staging manifest before any authorized use.

This script is local-only. It checks that the manifest still matches the
current runner/scorer files and that the command templates preserve the
dry-run/no-order boundaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shlex
import time
from pathlib import Path
from typing import Any


PROHIBITED_REMOTE_TOKENS = [
    "git pull",
    "git checkout",
    "git reset",
    "systemctl",
    "service ",
    "supervisorctl",
    "docker restart",
    "collector",
    "raw_replay",
]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def option_value(command: str, option: str) -> str | None:
    try:
        parts = shlex.split(command)
    except ValueError:
        return None
    for idx, part in enumerate(parts):
        if part == option and idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def option_matches(command: str, option: str, expected: Any) -> bool:
    actual = option_value(command, option)
    if actual is None:
        return False
    return actual == str(expected)


def command_parts(command: str) -> list[str]:
    try:
        return shlex.split(command)
    except ValueError:
        return []


def parse_duration_seconds(raw: str | None) -> int | None:
    if not raw:
        return None
    text = raw.strip().lower()
    multiplier = 1
    if text.endswith("s"):
        text = text[:-1]
    elif text.endswith("m"):
        text = text[:-1]
        multiplier = 60
    elif text.endswith("h"):
        text = text[:-1]
        multiplier = 3600
    try:
        return int(float(text) * multiplier)
    except ValueError:
        return None


def verify(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = Path(args.manifest_scorecard).expanduser().resolve()
    manifest = read_json(manifest_path)
    cwd = Path.cwd()
    hard_blockers: list[str] = []
    warnings: list[str] = []
    file_checks: list[dict[str, Any]] = []

    for entry in manifest.get("files_to_stage", []):
        rel = entry.get("path")
        path = (cwd / rel).resolve()
        check: dict[str, Any] = {
            "path": rel,
            "expected_sha256": entry.get("sha256"),
            "expected_bytes": entry.get("bytes"),
            "exists": path.exists(),
        }
        if not path.exists():
            check["status"] = "MISSING"
            hard_blockers.append(f"missing_file:{rel}")
        else:
            actual_sha = sha256_file(path)
            actual_bytes = path.stat().st_size
            check.update({"actual_sha256": actual_sha, "actual_bytes": actual_bytes})
            if actual_sha != entry.get("sha256") or actual_bytes != entry.get("bytes"):
                check["status"] = "DRIFT"
                hard_blockers.append(f"file_hash_or_size_drift:{rel}")
            else:
                check["status"] = "PASS"
        file_checks.append(check)

    decision = manifest.get("decision", {})
    safety = manifest.get("safety", {})
    remote_cmd = manifest.get("remote_command_template", "")
    remote_launch_cmd = manifest.get("remote_launch_command_template", "")
    postrun_cmd = manifest.get("postrun_bundle_command_template", "")
    profile = manifest.get("profile", {})
    bounded_policy = manifest.get("bounded_remote_run_policy", {})
    fair_price_admission = manifest.get("fair_price_admission", {})
    parts = command_parts(remote_cmd)
    duration_value = option_value(remote_cmd, "--duration-s")
    duration_s = parse_duration_seconds(duration_value)
    timeout_s = parse_duration_seconds(parts[1] if len(parts) >= 2 and parts[0] == "timeout" else None)

    if decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_remote_runner_allowed_not_false")
    if decision.get("requires_explicit_user_authorization") is not True:
        hard_blockers.append("manifest_missing_explicit_authorization_requirement")
    if safety.get("orders_sent_allowed") is not False:
        hard_blockers.append("orders_sent_allowed_not_false")
    if safety.get("pm_dry_run_required") is not True:
        hard_blockers.append("pm_dry_run_required_not_true")
    if "PM_DRY_RUN=1" not in remote_cmd:
        hard_blockers.append("remote_command_missing_pm_dry_run")
    if not remote_launch_cmd:
        hard_blockers.append("remote_launch_command_missing")
    else:
        if "run_exit_code.txt" not in remote_launch_cmd:
            hard_blockers.append("remote_launch_command_missing_run_exit_capture")
        if "run_stdout.log" not in remote_launch_cmd:
            hard_blockers.append("remote_launch_command_missing_run_stdout_capture")
        if "run_stderr.log" not in remote_launch_cmd:
            hard_blockers.append("remote_launch_command_missing_run_stderr_capture")
        if "remote_wrapper_pid.txt" not in remote_launch_cmd:
            hard_blockers.append("remote_launch_command_missing_wrapper_pid")
        if "timeout" not in remote_launch_cmd or "PM_DRY_RUN=1" not in remote_launch_cmd:
            hard_blockers.append("remote_launch_command_missing_bounded_dry_run")
        if "< /dev/null" not in remote_launch_cmd:
            hard_blockers.append("remote_launch_command_missing_stdin_detach")
        if "&& { (" not in remote_launch_cmd or "; }" not in remote_launch_cmd:
            hard_blockers.append("remote_launch_command_missing_foreground_pid_group")
    if duration_s is None:
        hard_blockers.append("remote_command_missing_duration_s")
    elif duration_s > args.max_dry_run_duration_s:
        hard_blockers.append("remote_command_duration_exceeds_30m")
    if len(parts) < 3 or parts[0] != "timeout" or parts[2] != "env":
        hard_blockers.append("remote_command_missing_timeout_env_wrapper")
    if timeout_s is None:
        hard_blockers.append("remote_command_timeout_missing_or_unparseable")
    elif duration_s is not None and timeout_s < duration_s + args.min_timeout_buffer_s:
        hard_blockers.append("remote_command_timeout_too_short")
    if bounded_policy.get("max_dry_run_duration_s") not in (args.max_dry_run_duration_s, None):
        hard_blockers.append("manifest_bounded_policy_max_duration_mismatch")
    for env_name in ("PM_DRY_RUN", "PM_SHARED_INGRESS_ROLE", "PM_INSTANCE_ID"):
        if f"'{env_name}=" in remote_cmd or f'"{env_name}=' in remote_cmd:
            hard_blockers.append(f"remote_command_quoted_env_assignment:{env_name}")
    if "--write-normalized-lifecycle" not in remote_cmd:
        hard_blockers.append("remote_command_missing_normalized_lifecycle")
    if "--write-rescue-block-diagnostics" not in remote_cmd:
        hard_blockers.append("remote_command_missing_rescue_diagnostics")
    if "--allow-concurrent-shared-ingress-readers" not in remote_cmd:
        hard_blockers.append("remote_command_missing_concurrent_reader_evidence_flag")
    if fair_price_admission.get("enabled") is True:
        remote_admission = fair_price_admission.get("remote_path_template")
        max_pair_cost = fair_price_admission.get("max_pair_cost")
        min_seconds_to_close = fair_price_admission.get("min_seconds_to_close")
        max_seconds_to_close = fair_price_admission.get("max_seconds_to_close")
        if not remote_admission:
            hard_blockers.append("fair_price_admission_missing_remote_path")
        if not option_matches(remote_cmd, "--fair-price-admission-jsonl", remote_admission):
            hard_blockers.append("remote_command_fair_price_admission_path_mismatch")
        if max_pair_cost is not None and not option_matches(remote_cmd, "--fair-price-max-pair-cost", max_pair_cost):
            hard_blockers.append("remote_command_fair_price_max_pair_cost_mismatch")
        if min_seconds_to_close is not None and not option_matches(
            remote_cmd, "--fair-price-min-seconds-to-close", min_seconds_to_close
        ):
            hard_blockers.append("remote_command_fair_price_min_seconds_to_close_mismatch")
        if max_seconds_to_close is not None and not option_matches(
            remote_cmd, "--fair-price-max-seconds-to-close", max_seconds_to_close
        ):
            hard_blockers.append("remote_command_fair_price_max_seconds_to_close_mismatch")
    if not option_matches(remote_cmd, "--risk-seed-closeability-soft-net-cap", profile.get("soft_cap", 0.98)):
        hard_blockers.append("remote_command_soft_cap_does_not_match_profile")
    if not option_matches(remote_cmd, "--risk-seed-closeability-debt-budget", profile.get("debt_budget", 1.0)):
        hard_blockers.append("remote_command_debt_budget_does_not_match_profile")
    if not option_matches(remote_cmd, "--risk-seed-pending-opp-credit", profile.get("risk_seed_pending_opp_credit", 1.0)):
        hard_blockers.append("remote_command_pending_opp_credit_does_not_match_profile")
    cancel_closeability_cap = profile.get("risk_seed_cancel_on_closeability_net_cap")
    if cancel_closeability_cap is not None and not option_matches(
        remote_cmd,
        "--risk-seed-cancel-on-closeability-net-cap",
        cancel_closeability_cap,
    ):
        hard_blockers.append("remote_command_closeability_cancel_cap_does_not_match_profile")
    if profile.get("pair_completion_net_cap") is not None and not option_matches(
        remote_cmd, "--pair-completion-net-cap", profile.get("pair_completion_net_cap")
    ):
        hard_blockers.append("remote_command_pair_completion_net_cap_does_not_match_profile")
    if profile.get("pair_completion_min_pair_pnl_after") is not None and not option_matches(
        remote_cmd,
        "--pair-completion-min-pair-pnl-after",
        profile.get("pair_completion_min_pair_pnl_after"),
    ):
        hard_blockers.append("remote_command_pair_completion_pair_pnl_floor_does_not_match_profile")
    if not option_matches(remote_cmd, "--salvage-net-cap", profile.get("strict_rescue_salvage_net_cap", 0.95)):
        hard_blockers.append("remote_command_salvage_net_cap_does_not_match_profile")
    if profile.get("max_salvage_qty") is not None and not option_matches(
        remote_cmd, "--max-salvage-qty", profile.get("max_salvage_qty")
    ):
        hard_blockers.append("remote_command_max_salvage_qty_does_not_match_profile")
    if profile.get("strict_rescue_surplus_net_cap") is not None and not option_matches(
        remote_cmd, "--strict-rescue-surplus-net-cap", profile.get("strict_rescue_surplus_net_cap")
    ):
        hard_blockers.append("remote_command_strict_rescue_surplus_net_cap_does_not_match_profile")
    if profile.get("strict_rescue_min_pair_pnl_after") is not None and not option_matches(
        remote_cmd,
        "--strict-rescue-min-pair-pnl-after",
        profile.get("strict_rescue_min_pair_pnl_after"),
    ):
        hard_blockers.append("remote_command_strict_rescue_pair_pnl_floor_does_not_match_profile")
    if not option_matches(remote_cmd, "--imbalance-qty-cap", profile.get("imbalance_qty_cap", 2.0)):
        hard_blockers.append("remote_command_imbalance_qty_cap_does_not_match_profile")
    if profile.get("activation_mode") and not option_matches(remote_cmd, "--activation-mode", profile.get("activation_mode")):
        hard_blockers.append("remote_command_activation_mode_does_not_match_profile")
    if profile.get("activation_window_s") is not None and not option_matches(remote_cmd, "--activation-window-s", profile.get("activation_window_s")):
        hard_blockers.append("remote_command_activation_window_does_not_match_profile")
    if profile.get("late_repair_after_s") is not None and not option_matches(remote_cmd, "--late-repair-after-s", profile.get("late_repair_after_s")):
        hard_blockers.append("remote_command_late_repair_after_does_not_match_profile")
    if "xuan_no_order_third_window_postrun_bundle.py" not in postrun_cmd:
        hard_blockers.append("postrun_command_missing_bundle_script")
    for token in PROHIBITED_REMOTE_TOKENS:
        if token in remote_cmd:
            hard_blockers.append(f"remote_command_contains_prohibited_token:{token}")
        if token in remote_launch_cmd:
            hard_blockers.append(f"remote_launch_command_contains_prohibited_token:{token}")
    if profile.get("duration_s") != args.max_dry_run_duration_s:
        warnings.append("profile_duration_not_standard_30m")
    if profile.get("round_offsets") != "0,1,2,3,4,5,6,7,8,9,10,11,12":
        warnings.append("profile_offsets_not_comparable_set")

    status = (
        "KEEP_THIRD_WINDOW_MANIFEST_VERIFIER_PASS_LOCAL_ONLY"
        if not hard_blockers
        else "BLOCKED_THIRD_WINDOW_MANIFEST_VERIFIER_DRIFT_OR_SAFETY"
    )
    return {
        "status": status,
        "manifest_scorecard": str(manifest_path),
        "hard_blockers": sorted(set(hard_blockers)),
        "warnings": sorted(set(warnings)),
        "file_checks": file_checks,
        "template_checks": {
            "remote_command_has_pm_dry_run": "PM_DRY_RUN=1" in remote_cmd,
            "remote_command_duration_s": duration_s,
            "remote_command_timeout_s": timeout_s,
            "remote_command_has_timeout_env_wrapper": len(parts) >= 3 and parts[0] == "timeout" and parts[2] == "env",
            "remote_command_has_unquoted_env_assignments": not any(
                f"'{env_name}=" in remote_cmd or f'"{env_name}=' in remote_cmd
                for env_name in ("PM_DRY_RUN", "PM_SHARED_INGRESS_ROLE", "PM_INSTANCE_ID")
            ),
            "remote_command_has_normalized_lifecycle": "--write-normalized-lifecycle" in remote_cmd,
            "remote_command_has_rescue_diagnostics": "--write-rescue-block-diagnostics" in remote_cmd,
            "remote_command_has_concurrent_reader_evidence_flag": "--allow-concurrent-shared-ingress-readers" in remote_cmd,
            "remote_command_has_closeability_cancel_guard": "--risk-seed-cancel-on-closeability-net-cap" in remote_cmd,
            "fair_price_admission_enabled": fair_price_admission.get("enabled") is True,
            "remote_command_has_fair_price_admission": "--fair-price-admission-jsonl" in remote_cmd,
            "postrun_command_has_bundle_script": "xuan_no_order_third_window_postrun_bundle.py" in postrun_cmd,
            "remote_launch_command_has_run_exit_capture": "run_exit_code.txt" in remote_launch_cmd,
            "remote_launch_command_has_run_stdout_capture": "run_stdout.log" in remote_launch_cmd,
            "remote_launch_command_has_run_stderr_capture": "run_stderr.log" in remote_launch_cmd,
            "remote_launch_command_has_wrapper_pid": "remote_wrapper_pid.txt" in remote_launch_cmd,
            "remote_launch_command_has_stdin_detach": "< /dev/null" in remote_launch_cmd,
            "remote_launch_command_has_foreground_pid_group": "&& { (" in remote_launch_cmd and "; }" in remote_launch_cmd,
        },
        "decision": {
            "local_manifest_verified": not hard_blockers,
            "remote_runner_allowed": False,
            "deployable": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--max-dry-run-duration-s", type=int, default=1800)
    parser.add_argument("--min-timeout-buffer-s", type=int, default=300)
    args = parser.parse_args()

    started = time.time()
    result = verify(args)
    scorecard = {
        "artifact": "xuan_no_order_third_window_manifest_verifier",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_third_window_manifest_verifier.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))
    if result["hard_blockers"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
