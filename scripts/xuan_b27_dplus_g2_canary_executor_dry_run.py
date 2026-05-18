#!/usr/bin/env python3
"""Local dry-run skeleton for a future xuan_b27_dplus G2 canary executor.

This is not a network executor. It validates the exact approval envelope and
latest executor contract, materializes the future phase plan, and refuses
before any SSH/sync/build/run/order path.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from xuan_b27_dplus_g2_canary_approval_envelope import summarize as summarize_approval


EXPECTED_CONTRACT_STATUS = "READY_FOR_REVIEWED_G2_CANARY_EXECUTOR_IMPLEMENTATION"
EXPECTED_PAYLOAD_STATUS = "PASS"
REMOTE_PREFIX = "/home/ubuntu/xuan_research_runs/xuan_research_"


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


def latest_executor_contract(root: Path) -> tuple[Path | None, dict[str, Any]]:
    direct_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_contract_*",
        artifact="xuan_b27_dplus_g2_canary_executor_contract",
    )
    if direct_path:
        return direct_path, read_json(direct_path)
    smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_contract_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_contract_smoke",
    )
    smoke = read_json(smoke_path)
    contract_manifest = smoke.get("contract_manifest")
    nested_path = Path(contract_manifest) if isinstance(contract_manifest, str) else None
    return nested_path, read_json(nested_path)


def latest_payload_manifest(
    root: Path,
    override: Path | None = None,
) -> tuple[Path | None, dict[str, Any], Path | None, dict[str, Any]]:
    if override:
        return override, read_json(override), None, {}
    direct_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_payload_manifest_*",
        artifact="xuan_b27_dplus_g2_canary_executor_payload_manifest",
    )
    if direct_path:
        return direct_path, read_json(direct_path), None, {}
    smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke",
    )
    smoke = read_json(smoke_path)
    payload_manifest = smoke.get("payload_manifest")
    nested_path = Path(payload_manifest) if isinstance(payload_manifest, str) else None
    return nested_path, read_json(nested_path), smoke_path, smoke


def side_effects_false() -> dict[str, bool]:
    return {
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
    }


def base_manifest(out_dir: Path, label: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_g2_canary_executor_dry_run",
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_g2_canary_executor_dry_run",
        "executor_implemented": False,
        "dry_run_only": True,
        "canary_run_authorized": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": side_effects_false(),
        "output_dir": str(out_dir),
    }


def remote_path_ok(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(REMOTE_PREFIX)


def approval_run_paths(summary: dict[str, Any]) -> dict[str, Any]:
    envelope_path = Path(summary.get("approval_envelope")) if isinstance(summary.get("approval_envelope"), str) else None
    envelope = read_json(envelope_path)
    run = envelope.get("run") or {}
    return {
        "remote_worktree": run.get("remote_worktree"),
        "remote_run_dir": run.get("remote_run_dir"),
        "remote_worktree_isolated": remote_path_ok(run.get("remote_worktree")),
        "remote_run_dir_isolated": remote_path_ok(run.get("remote_run_dir")),
    }


def build_phase_plan(contract: dict[str, Any]) -> list[dict[str, Any]]:
    phases = contract.get("required_executor_phases") or []
    effectful = {
        "create_isolated_remote_run_root_under_xuan_research_runs",
        "sync_allowlisted_source_files_only",
        "build_declared_remote_targets_only",
        "run_readonly_shared_ingress_preflight_before_canary",
        "inject_auth_vars_ephemerally_only",
        "start_single_bounded_g2_canary_process",
        "wait_or_stop_at_duration_or_round_limit",
        "pull_artifacts_to_xuan_research_artifacts",
    }
    out = []
    for phase in phases:
        out.append(
            {
                "phase": phase,
                "dry_run_status": "VALIDATED_CONTRACT_ONLY"
                if phase not in effectful
                else "REFUSED_BEFORE_SIDE_EFFECT",
                "side_effects_performed": False,
            }
        )
    return out


def payload_allowlist_ok(payload: dict[str, Any]) -> bool:
    side_effects = payload.get("side_effects") or {}
    return (
        payload.get("status") == EXPECTED_PAYLOAD_STATUS
        and payload.get("scope") == "local_no_network_g2_canary_executor_payload_manifest"
        and isinstance(payload.get("payload_file_count"), int)
        and payload.get("payload_file_count", 0) >= 40
        and payload.get("missing_required_files") == []
        and payload.get("missing_executable_files") == []
        and payload.get("forbidden_path_hits") == []
        and payload.get("duplicate_paths") == []
        and payload.get("requires_reviewed_executor_implementation") is True
        and payload.get("requires_exact_g2_canary_approval_before_sync") is True
        and payload.get("orders_sent") is False
        and payload.get("auth_network_started") is False
        and payload.get("started_canary") is False
        and all(value is False for value in side_effects.values())
    )


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
    parser.add_argument("--approval-envelope", type=Path)
    parser.add_argument("--payload-manifest", type=Path)
    parser.add_argument("--approved-g2-canary-sync-and-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_g2_canary_executor_dry_run_{label}"
    manifest = base_manifest(out_dir, label)

    if not args.approved_g2_canary_sync_and_run:
        manifest.update(
            {
                "status": "REFUSED_NO_EXPLICIT_G2_CANARY_SYNC_AND_RUN_APPROVAL",
                "reason": "missing --approved-g2-canary-sync-and-run",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 64

    if not args.approval_envelope:
        manifest.update(
            {
                "status": "REFUSED_MISSING_APPROVAL_ENVELOPE",
                "reason": "missing --approval-envelope",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 64

    approval_summary = summarize_approval(args.approval_envelope)
    manifest["approval_envelope"] = str(args.approval_envelope)
    manifest["approval_summary"] = approval_summary
    manifest["approval_run_paths"] = approval_run_paths(approval_summary)
    if approval_summary.get("status") != "PASS_EXACT_G2_CANARY_APPROVAL_ENVELOPE":
        manifest.update(
            {
                "status": "REFUSED_INVALID_APPROVAL_ENVELOPE",
                "reason": "approval envelope did not pass exact G2 canary checks",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 65

    contract_path, contract = latest_executor_contract(root)
    manifest["executor_contract"] = {
        "path": str(contract_path) if contract_path else None,
        "exists": bool(contract_path and contract_path.exists()),
        "status": contract.get("status"),
        "scope": contract.get("scope"),
        "inputs_ok": contract.get("inputs_ok"),
        "executor_implemented": contract.get("executor_implemented"),
        "canary_run_authorized": contract.get("canary_run_authorized"),
    }
    if (
        contract.get("status") != EXPECTED_CONTRACT_STATUS
        or contract.get("inputs_ok") is not True
        or contract.get("executor_implemented") is not False
        or contract.get("canary_run_authorized") is not False
    ):
        manifest.update(
            {
                "status": "REFUSED_EXECUTOR_CONTRACT_NOT_READY",
                "reason": "latest executor contract is missing or not ready",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 66

    payload_path, payload, payload_smoke_path, payload_smoke = latest_payload_manifest(root, args.payload_manifest)
    payload_drift_rows = payload_drift(root, payload)
    manifest["payload_allowlist"] = {
        "path": str(payload_path) if payload_path else None,
        "exists": bool(payload_path and payload_path.exists()),
        "status": payload.get("status"),
        "scope": payload.get("scope"),
        "payload_file_count": payload.get("payload_file_count"),
        "missing_required_files": payload.get("missing_required_files"),
        "missing_executable_files": payload.get("missing_executable_files"),
        "forbidden_path_hits": payload.get("forbidden_path_hits"),
        "duplicate_paths": payload.get("duplicate_paths"),
        "requires_reviewed_executor_implementation": payload.get("requires_reviewed_executor_implementation"),
        "requires_exact_g2_canary_approval_before_sync": payload.get(
            "requires_exact_g2_canary_approval_before_sync"
        ),
        "source_smoke_path": str(payload_smoke_path) if payload_smoke_path else None,
        "source_smoke_status": payload_smoke.get("status"),
        "override_path": str(args.payload_manifest) if args.payload_manifest else None,
    }
    manifest["payload_current_check"] = {
        "status": "PASS" if not payload_drift_rows else "FAIL",
        "drift_count": len(payload_drift_rows),
        "drift_sample": payload_drift_rows[:20],
    }
    if not payload_allowlist_ok(payload):
        manifest.update(
            {
                "status": "REFUSED_PAYLOAD_ALLOWLIST_NOT_READY",
                "reason": "latest executor payload allowlist manifest is missing or not safe",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 67
    if payload_drift_rows:
        manifest.update(
            {
                "status": "REFUSED_PAYLOAD_ALLOWLIST_DRIFT",
                "reason": "payload allowlist manifest does not match current local files",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 68

    manifest.update(
        {
            "status": "REFUSED_DRY_RUN_ONLY_EXECUTOR",
            "reason": "validated approval envelope, executor contract, and current payload allowlist, but this skeleton is local dry-run only",
            "phase_plan": build_phase_plan(contract),
            "mandatory_abort_conditions": contract.get("mandatory_abort_conditions") or [],
            "allowed_side_effects_after_exact_executor_approval": contract.get(
                "allowed_side_effects_after_exact_executor_approval"
            )
            or {},
            "forbidden_side_effects_even_after_exact_executor_approval": contract.get(
                "forbidden_side_effects_even_after_exact_executor_approval"
            )
            or {},
            "next_gate": "replace this dry-run skeleton with a reviewed executor implementation that preserves all contract gates",
        }
    )
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 75


if __name__ == "__main__":
    raise SystemExit(main())
