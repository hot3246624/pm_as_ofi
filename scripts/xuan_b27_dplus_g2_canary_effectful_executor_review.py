#!/usr/bin/env python3
"""Review the G2 canary executor implementation without running it.

This is a local, no-network review gate. It distinguishes the current
refusal-first executor preflight from a future reviewed effectful executor.
The current implementation should be reported as blocked, not passed.
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_g2_canary_effectful_executor_review"
PASS_STATUS = "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION"
BLOCKED_STATUS = "BLOCKED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
FAIL_STATUS = "FAIL_EFFECTFUL_EXECUTOR_REVIEW_INPUTS"
EXPECTED_PREFLIGHT_ARTIFACT = "xuan_b27_dplus_g2_canary_executor"
EXPECTED_PREFLIGHT_SCOPE = "local_no_network_g2_canary_executor_preflight"
EXPECTED_REFUSAL_STATUS = "REFUSED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
EXPECTED_CONTRACT_STATUS = "READY_FOR_REVIEWED_G2_CANARY_EXECUTOR_IMPLEMENTATION"
REQUIRED_PLAN_SECTIONS = (
    "remote_run_root_plan",
    "payload_sync_plan",
    "remote_build_plan",
    "shared_ingress_preflight_plan",
    "auth_injection_plan",
    "bounded_canary_process_plan",
    "wait_stop_supervision_plan",
    "artifact_pullback_review_plan",
)
REQUIRED_EFFECTFUL_PHASES = {
    "create_isolated_remote_run_root_under_xuan_research_runs",
    "sync_allowlisted_source_files_only",
    "build_declared_remote_targets_only",
    "run_readonly_shared_ingress_preflight_before_canary",
    "inject_auth_vars_ephemerally_only",
    "start_single_bounded_g2_canary_process",
    "wait_or_stop_at_duration_or_round_limit",
    "pull_artifacts_to_xuan_research_artifacts",
}
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/replay",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def is_under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def safe_artifact_path(root: Path, path: Path) -> bool:
    text = str(path)
    if any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS):
        return False
    return is_under(path, root / "xuan_research_artifacts")


def latest_executor_preflight(root: Path) -> Path | None:
    matches = [
        Path(p)
        for p in glob.glob(
            str(
                root
                / "xuan_research_artifacts"
                / "xuan_b27_dplus_g2_canary_executor_smoke_*"
                / "valid_preflight"
                / "manifest.json"
            )
        )
    ]
    matches = [
        path
        for path in matches
        if read_json(path).get("artifact") == EXPECTED_PREFLIGHT_ARTIFACT
    ]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime_ns)


def all_false(value: Any) -> bool:
    if isinstance(value, dict):
        return all(all_false(item) for item in value.values())
    if isinstance(value, list):
        return all(all_false(item) for item in value)
    return value is False


def side_effects_all_false(preflight: dict[str, Any]) -> bool:
    if not all_false(preflight.get("side_effects") or {}):
        return False
    for section in REQUIRED_PLAN_SECTIONS:
        plan = preflight.get(section) or {}
        if not all_false(plan.get("planned_side_effects") or {}):
            return False
    for item in preflight.get("phase_plan") or []:
        if item.get("side_effects_performed") is not False:
            return False
    return True


def plan_statuses(preflight: dict[str, Any]) -> dict[str, Any]:
    return {
        section: (preflight.get(section) or {}).get("status")
        for section in REQUIRED_PLAN_SECTIONS
    }


def phase_coverage(preflight: dict[str, Any]) -> dict[str, Any]:
    phases = {
        item.get("phase")
        for item in preflight.get("phase_plan") or []
        if isinstance(item, dict)
    }
    missing = sorted(REQUIRED_EFFECTFUL_PHASES - phases)
    return {
        "required_phase_count": len(REQUIRED_EFFECTFUL_PHASES),
        "observed_phase_count": len(phases),
        "missing_effectful_phases": missing,
        "complete": not missing,
    }


def review_failures(preflight: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if preflight.get("artifact") != EXPECTED_PREFLIGHT_ARTIFACT:
        failures.append("wrong_preflight_artifact")
    if preflight.get("scope") != EXPECTED_PREFLIGHT_SCOPE:
        failures.append("wrong_preflight_scope")
    if preflight.get("executor_preflight_implemented") is not True:
        failures.append("executor_preflight_not_implemented")
    if preflight.get("canary_run_authorized") is not False:
        failures.append("canary_run_authorized_in_review_input")
    for field in (
        "orders_sent",
        "cancels_sent",
        "redeems_sent",
        "auth_network_started",
        "started_canary",
    ):
        if preflight.get(field) is not False:
            failures.append(f"{field}_not_false")
    if (preflight.get("executor_contract") or {}).get("status") != EXPECTED_CONTRACT_STATUS:
        failures.append("executor_contract_not_ready")
    for section, status in plan_statuses(preflight).items():
        if status != "PASS":
            failures.append(f"{section}_not_pass")
    coverage = phase_coverage(preflight)
    if coverage["missing_effectful_phases"]:
        failures.append("effectful_phase_plan_incomplete")
    if not side_effects_all_false(preflight):
        failures.append("review_input_side_effects_not_false")
    return failures


def build_manifest(
    *,
    out_dir: Path,
    label: str,
    preflight_path: Path | None,
    preflight: dict[str, Any],
    status: str,
    review_passed: bool,
    fixture_review: bool,
    failures: list[str],
) -> dict[str, Any]:
    implemented = preflight.get("effectful_executor_implemented") is True
    side_effects = {
        "ssh_started": False,
        "network_started": False,
        "sync_started": False,
        "build_started": False,
        "shared_ingress_preflight_started": False,
        "auth_network_started": False,
        "started_canary": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "broker_modified": False,
        "service_control_called": False,
        "remote_files_written": False,
        "local_run_artifacts_written": False,
    }
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_g2_canary_effectful_executor_review",
        "review_passed": review_passed,
        "fixture_review": fixture_review,
        "effectful_executor_implemented": implemented,
        "reviewed_effectful_executor_implementation": review_passed and implemented,
        "executor_preflight_manifest": str(preflight_path) if preflight_path else None,
        "executor_preflight_status": preflight.get("status"),
        "executor_preflight_implemented": preflight.get("executor_preflight_implemented"),
        "executor_contract_status": (preflight.get("executor_contract") or {}).get("status"),
        "canary_run_authorized": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "plan_section_statuses": plan_statuses(preflight),
        "effectful_phase_plan_coverage": phase_coverage(preflight),
        "review_input_side_effects_all_false": side_effects_all_false(preflight),
        "failures": failures,
        "side_effects": side_effects,
        "output_dir": str(out_dir),
        "next_gate": (
            "exact G2 canary approval envelope may reference this review"
            if review_passed
            else "implement and review the effectful executor before any G2 canary execution"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--executor-preflight-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--allow-fixture-pass",
        action="store_true",
        help="Allow a fixture preflight with fixture_effectful_executor_implementation=true to PASS.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"

    preflight_path = args.executor_preflight_manifest or latest_executor_preflight(root)
    if not preflight_path:
        manifest = build_manifest(
            out_dir=out_dir,
            label=label,
            preflight_path=None,
            preflight={},
            status=FAIL_STATUS,
            review_passed=False,
            fixture_review=False,
            failures=["executor_preflight_manifest_missing"],
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 64
    if not safe_artifact_path(root, preflight_path):
        manifest = build_manifest(
            out_dir=out_dir,
            label=label,
            preflight_path=preflight_path,
            preflight={},
            status=FAIL_STATUS,
            review_passed=False,
            fixture_review=False,
            failures=["executor_preflight_manifest_path_not_safe"],
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 64
    if not preflight_path.exists():
        manifest = build_manifest(
            out_dir=out_dir,
            label=label,
            preflight_path=preflight_path,
            preflight={},
            status=FAIL_STATUS,
            review_passed=False,
            fixture_review=False,
            failures=["executor_preflight_manifest_not_found"],
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 64

    preflight = read_json(preflight_path)
    if "_read_error" in preflight:
        manifest = build_manifest(
            out_dir=out_dir,
            label=label,
            preflight_path=preflight_path,
            preflight=preflight,
            status=FAIL_STATUS,
            review_passed=False,
            fixture_review=False,
            failures=["executor_preflight_manifest_decode_error"],
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 65

    failures = review_failures(preflight)
    fixture_pass_allowed = (
        args.allow_fixture_pass
        and preflight.get("fixture_effectful_executor_implementation") is True
        and preflight.get("effectful_executor_implemented") is True
    )
    real_pass_allowed = (
        preflight.get("effectful_executor_implemented") is True
        and preflight.get("executor_reviewed") is True
        and preflight.get("fixture_effectful_executor_implementation") is not True
    )

    if not failures and (fixture_pass_allowed or real_pass_allowed):
        manifest = build_manifest(
            out_dir=out_dir,
            label=label,
            preflight_path=preflight_path,
            preflight=preflight,
            status=PASS_STATUS,
            review_passed=True,
            fixture_review=fixture_pass_allowed,
            failures=[],
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 0

    if not failures and preflight.get("status") == EXPECTED_REFUSAL_STATUS:
        manifest = build_manifest(
            out_dir=out_dir,
            label=label,
            preflight_path=preflight_path,
            preflight=preflight,
            status=BLOCKED_STATUS,
            review_passed=False,
            fixture_review=False,
            failures=["effectful_executor_not_implemented"],
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 75

    if not failures:
        failures.append("effectful_executor_review_not_passable")
    manifest = build_manifest(
        out_dir=out_dir,
        label=label,
        preflight_path=preflight_path,
        preflight=preflight,
        status=FAIL_STATUS,
        review_passed=False,
        fixture_review=False,
        failures=failures,
    )
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 66


if __name__ == "__main__":
    raise SystemExit(main())
