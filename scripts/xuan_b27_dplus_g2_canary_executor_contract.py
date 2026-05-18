#!/usr/bin/env python3
"""Create a local, no-network contract for a future G2 canary executor.

This is not an executor. It records the mandatory phases and fail-closed
requirements a separately reviewed executor must satisfy before any exact run.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def first_line(path: Path) -> str:
    with path.open("rb") as fh:
        return fh.readline(256).decode("utf-8", errors="replace").rstrip("\r\n")


def latest_manifest(root: Path, pattern: str, artifact: str | None = None) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json"))]
    if artifact:
        matches = [p for p in matches if read_json(p).get("artifact") == artifact]
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime_ns)


def summarize(path: Path | None, value: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {"path": str(path) if path else None, "exists": bool(path and path.exists())}
    for field in fields:
        out[field] = value.get(field)
    return out


def payload_drift(root: Path, payload: dict[str, Any]) -> list[dict[str, Any]]:
    drift: list[dict[str, Any]] = []
    for record in payload.get("payload_records") or []:
        rel = record.get("path")
        if not isinstance(rel, str) or rel.startswith("/") or ".." in Path(rel).parts:
            drift.append({"path": rel, "reason": "invalid_relative_path"})
            continue
        path = root / rel
        if not path.exists():
            drift.append({"path": rel, "reason": "missing_current_file"})
            continue
        if not path.is_file():
            drift.append({"path": rel, "reason": "current_path_not_file"})
            continue
        line = first_line(path)
        current = {
            "size_bytes": path.stat().st_size,
            "sha256": sha256(path),
            "mode_octal": f"{path.stat().st_mode & 0o777:o}",
            "is_executable": bool(path.stat().st_mode & 0o111),
            "has_shebang": line.startswith("#!"),
        }
        for field, value in current.items():
            if record.get(field) != value:
                drift.append(
                    {
                        "path": rel,
                        "reason": f"{field}_mismatch",
                        "recorded": record.get(field),
                        "current": value,
                    }
                )
                break
    return drift


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_g2_canary_executor_contract_{label}"

    bundle_path = latest_manifest(
        root,
        "xuan_b27_dplus_local_status_bundle_*",
        artifact="xuan_b27_dplus_local_status_bundle",
    )
    canary_readiness_path = latest_manifest(
        root,
        "xuan_b27_dplus_canary_readiness_plan_smoke_*",
        artifact="xuan_b27_dplus_canary_readiness_plan_smoke",
    )
    launch_plan_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_launch_plan_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_launch_plan_smoke",
    )
    approval_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_approval_envelope_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_approval_envelope_smoke",
    )
    review_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_review_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_review_smoke",
    )
    launcher_refusal_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_launcher_refusal_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_launcher_refusal_smoke",
    )
    payload_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke",
    )

    bundle = read_json(bundle_path)
    canary_readiness = read_json(canary_readiness_path)
    launch_plan = read_json(launch_plan_path)
    approval = read_json(approval_path)
    review = read_json(review_path)
    launcher_refusal = read_json(launcher_refusal_path)
    payload_smoke = read_json(payload_smoke_path)
    payload_manifest = read_json(
        Path(payload_smoke.get("payload_manifest"))
        if isinstance(payload_smoke.get("payload_manifest"), str)
        else None
    )
    payload_drift_rows = payload_drift(root, payload_manifest)
    readiness_plan = read_json(
        Path(canary_readiness.get("plan_manifest"))
        if isinstance(canary_readiness.get("plan_manifest"), str)
        else None
    )
    readiness_status = readiness_plan.get("status")
    bundle_canary_status = bundle.get("canary_readiness_status")
    canary_readiness_fields = readiness_plan.get("canary_readiness") or {}
    canary_gate_status_acceptable = readiness_status in {
        "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL",
        "CANARY_NOT_READY_SHADOW_DRY_RUN_STRATEGY_ACCEPTANCE_MISSING",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_MISSING",
    } and bundle_canary_status in {
        "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL",
        "CANARY_NOT_READY_SHADOW_DRY_RUN_STRATEGY_ACCEPTANCE_MISSING",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_MISSING",
    }
    plumbing_ready_for_executor_implementation = (
        canary_readiness_fields.get("pre_canary_plumbing_ready") is True
        or readiness_status == "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL"
    )

    inputs_ok = (
        bundle.get("ec2_readonly_user_ws_status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
        and canary_readiness.get("status") == "PASS"
        and canary_gate_status_acceptable
        and plumbing_ready_for_executor_implementation
        and launch_plan.get("status") == "PASS"
        and approval.get("status") == "PASS"
        and review.get("status") == "PASS"
        and launcher_refusal.get("status") == "PASS"
        and payload_smoke.get("status") == "PASS"
        and payload_manifest.get("status") == "PASS"
        and payload_manifest.get("missing_required_files") == []
        and payload_manifest.get("missing_executable_files") == []
        and payload_manifest.get("forbidden_path_hits") == []
        and payload_manifest.get("duplicate_paths") == []
        and payload_drift_rows == []
    )
    status = "READY_FOR_REVIEWED_G2_CANARY_EXECUTOR_IMPLEMENTATION" if inputs_ok else "BLOCKED_G2_CANARY_EXECUTOR_CONTRACT_INPUTS"

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_g2_canary_executor_contract",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_g2_canary_executor_contract",
        "inputs_ok": inputs_ok,
        "executor_implemented": False,
        "canary_run_authorized": False,
        "inputs": {
            "status_bundle": summarize(
                bundle_path,
                bundle,
                ("ec2_readonly_user_ws_status", "canary_readiness_status", "canary_next_gate"),
            ),
            "canary_readiness_plan_smoke": summarize(
                canary_readiness_path,
                canary_readiness,
                ("status", "scope", "orders_sent", "auth_network_started", "plan_manifest"),
            ),
            "canary_readiness_plan": {
                "status": readiness_status,
                "pre_canary_plumbing_ready": canary_readiness_fields.get("pre_canary_plumbing_ready"),
                "rust_shadow_strategy_acceptance_ready": canary_readiness_fields.get(
                    "rust_shadow_strategy_acceptance_ready"
                ),
                "ready_for_explicit_g2_canary_approval": canary_readiness_fields.get(
                    "ready_for_explicit_g2_canary_approval"
                ),
                "requires_rust_shadow_strategy_acceptance": canary_readiness_fields.get(
                    "requires_rust_shadow_strategy_acceptance"
                ),
            },
            "launch_plan_smoke": summarize(
                launch_plan_path,
                launch_plan,
                ("status", "scope", "orders_sent", "auth_network_started", "started_canary", "plan_manifest"),
            ),
            "approval_envelope_smoke": summarize(
                approval_path,
                approval,
                ("status", "scope", "orders_sent", "auth_network_started", "started_canary", "verifier"),
            ),
            "post_run_review_smoke": summarize(
                review_path,
                review,
                ("status", "scope", "orders_sent", "auth_network_started", "started_canary", "summarizer"),
            ),
            "launcher_refusal_smoke": summarize(
                launcher_refusal_path,
                launcher_refusal,
                ("status", "scope", "orders_sent", "auth_network_started", "started_canary", "launcher"),
            ),
            "payload_manifest_smoke": summarize(
                payload_smoke_path,
                payload_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "started_canary", "payload_manifest"),
            ),
            "payload_manifest": summarize(
                Path(payload_smoke.get("payload_manifest"))
                if isinstance(payload_smoke.get("payload_manifest"), str)
                else None,
                payload_manifest,
                (
                    "status",
                    "scope",
                    "payload_file_count",
                    "missing_required_files",
                    "missing_executable_files",
                    "forbidden_path_hits",
                    "duplicate_paths",
                ),
            ),
            "payload_current_check": {
                "status": "PASS" if not payload_drift_rows else "FAIL",
                "drift_count": len(payload_drift_rows),
                "drift_sample": payload_drift_rows[:20],
            },
        },
        "required_executor_phases": [
            "load_structured_approval_envelope",
            "validate_current_local_readiness_artifacts",
            "load_reviewed_payload_allowlist_manifest",
            "create_isolated_remote_run_root_under_xuan_research_runs",
            "sync_allowlisted_source_files_only",
            "build_declared_remote_targets_only",
            "run_readonly_shared_ingress_preflight_before_canary",
            "inject_auth_vars_ephemerally_only",
            "start_single_bounded_g2_canary_process",
            "wait_or_stop_at_duration_or_round_limit",
            "pull_artifacts_to_xuan_research_artifacts",
            "run_local_post_run_review_and_secret_sentinel_scan",
            "write_local_acceptance_review",
        ],
        "mandatory_abort_conditions": [
            "approval_envelope_missing_or_invalid",
            "local_readiness_not_ready_for_explicit_g2_canary_approval",
            "rust_shadow_strategy_acceptance_missing",
            "payload_allowlist_missing_or_invalid",
            "payload_allowlist_drift_detected",
            "remote_root_outside_xuan_research_runs",
            "shared_ingress_preflight_not_ok",
            "source_truth_unknown_or_failed",
            "risk_cap_or_stop_on_unknown_violation",
            "unexpected_taker_fill",
            "recorder_decode_error_or_critical_drop",
            "secret_sentinel_leak",
            "any_forbidden_side_effect_detected",
        ],
        "allowed_side_effects_after_exact_executor_approval": {
            "create_isolated_remote_worktree": True,
            "sync_allowlisted_source_files": True,
            "build_remote_binary": True,
            "start_single_bounded_canary": True,
            "capped_post_only_orders": True,
            "bounded_own_order_cancels": True,
        },
        "forbidden_side_effects_even_after_exact_executor_approval": {
            "systemd_or_service_control": True,
            "broker_start_stop_repair": True,
            "shared_ingress_modification": True,
            "remote_env_file_write": True,
            "raw_replay_scan_or_write": True,
            "redeem_or_claim": True,
            "unbounded_live_trading": True,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "synced_code": False,
            "built_remote_binary": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
            "service_control_used": False,
            "shared_ingress_modified": False,
            "env_files_written": False,
        },
        "next_gate": "implement and review an executor that satisfies this contract before any exact G2 canary run",
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if inputs_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
