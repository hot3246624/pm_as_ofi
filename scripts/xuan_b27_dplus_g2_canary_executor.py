#!/usr/bin/env python3
"""Refusal-first local preflight for a future xuan_b27_dplus G2 executor.

This is the reviewed executor entrypoint shell, not the effectful executor.
It validates approval, contract, payload allowlist, payload drift, remote run
root layout, payload-to-worktree sync mapping, declared remote build plan,
read-only shared-ingress preflight plan, and ephemeral auth injection plan,
then refuses before any remote or trading side effect because later phases
remain unimplemented.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from xuan_b27_dplus_g2_canary_approval_envelope import summarize as summarize_approval
from xuan_b27_dplus_g2_canary_executor_dry_run import (
    EXPECTED_CONTRACT_STATUS,
    approval_run_paths,
    latest_executor_contract,
    latest_payload_manifest,
    payload_allowlist_ok,
    payload_drift,
    read_json,
    side_effects_false,
)


EFFECTFUL_PHASES = {
    "create_isolated_remote_run_root_under_xuan_research_runs",
    "sync_allowlisted_source_files_only",
    "build_declared_remote_targets_only",
    "run_readonly_shared_ingress_preflight_before_canary",
    "inject_auth_vars_ephemerally_only",
    "start_single_bounded_g2_canary_process",
    "wait_or_stop_at_duration_or_round_limit",
    "pull_artifacts_to_xuan_research_artifacts",
}
REMOTE_RUNS_ROOT = PurePosixPath("/home/ubuntu/xuan_research_runs")
REMOTE_RUN_ROOT_PREFIX = "xuan_research_"
REMOTE_RUN_DIR_PREFIX = "g2_canary_run_"
DECLARED_REMOTE_BUILD_TARGETS = ("polymarket_v2",)
REQUIRED_SHARED_INGRESS_ROOT = "/srv/pm_as_ofi/shared-ingress-main"
SHARED_INGRESS_PREFLIGHT_SCRIPT = "scripts/xuan_b27_dplus_shared_ingress_preflight.py"
G2_CANARY_SUMMARIZER_SCRIPT = "scripts/xuan_b27_dplus_summarize_g2_canary_run.py"
SECRET_AUTH_CLASSES = ("polymarket_api_credentials", "polymarket_private_key")
CANARY_BINARY_RELATIVE = PurePosixPath("target/debug/polymarket_v2")
CANARY_STOP_KILL_AFTER_SECONDS = 30


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def base_manifest(out_dir: Path, label: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_g2_canary_executor",
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_g2_canary_executor_preflight",
        "executor_preflight_implemented": True,
        "effectful_executor_implemented": False,
        "canary_run_authorized": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": side_effects_false(),
        "output_dir": str(out_dir),
    }


def build_executor_phase_plan(contract: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for phase in contract.get("required_executor_phases") or []:
        effectful = phase in EFFECTFUL_PHASES
        out.append(
            {
                "phase": phase,
                "effectful": effectful,
                "executor_status": "REFUSED_EFFECTFUL_IMPLEMENTATION_MISSING"
                if effectful
                else "VALIDATED_PRECONDITION",
                "side_effects_performed": False,
            }
        )
    return out


def remote_run_root_plan(envelope: dict[str, Any]) -> dict[str, Any]:
    run = envelope.get("run") or {}
    remote_worktree = run.get("remote_worktree")
    remote_run_dir = run.get("remote_run_dir")
    failures: list[str] = []
    worktree_path: PurePosixPath | None = None
    run_dir_path: PurePosixPath | None = None
    run_root: PurePosixPath | None = None

    if not isinstance(remote_worktree, str):
        failures.append("remote_worktree_missing")
    else:
        worktree_path = PurePosixPath(remote_worktree)
        if not worktree_path.is_absolute():
            failures.append("remote_worktree_not_absolute")
        if ".." in worktree_path.parts:
            failures.append("remote_worktree_parent_escape")
        if worktree_path.name != "worktree":
            failures.append("remote_worktree_leaf_not_worktree")
        run_root = worktree_path.parent

    if not isinstance(remote_run_dir, str):
        failures.append("remote_run_dir_missing")
    else:
        run_dir_path = PurePosixPath(remote_run_dir)
        if not run_dir_path.is_absolute():
            failures.append("remote_run_dir_not_absolute")
        if ".." in run_dir_path.parts:
            failures.append("remote_run_dir_parent_escape")
        if not run_dir_path.name.startswith(REMOTE_RUN_DIR_PREFIX):
            failures.append("remote_run_dir_leaf_not_g2_canary_run")

    if run_root:
        if run_root.parent != REMOTE_RUNS_ROOT:
            failures.append("remote_run_root_parent_not_xuan_research_runs")
        if not run_root.name.startswith(REMOTE_RUN_ROOT_PREFIX):
            failures.append("remote_run_root_leaf_not_xuan_research")
    if run_root and run_dir_path and run_dir_path.parent != run_root:
        failures.append("remote_worktree_and_run_dir_not_same_root")

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "remote_runs_root": str(REMOTE_RUNS_ROOT),
        "remote_run_root": str(run_root) if run_root else None,
        "remote_worktree": str(worktree_path) if worktree_path else remote_worktree,
        "remote_run_dir": str(run_dir_path) if run_dir_path else remote_run_dir,
        "remote_worktree_leaf": worktree_path.name if worktree_path else None,
        "remote_run_dir_leaf": run_dir_path.name if run_dir_path else None,
        "planned_side_effects": {
            "remote_directory_created": False,
            "ssh_started": False,
            "network_started": False,
        },
    }


def apply_remote_root_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "create_isolated_remote_run_root_under_xuan_research_runs":
            updated["executor_status"] = "PLANNED_NO_NETWORK_REMOTE_ROOT_CREATION"
            updated["remote_run_root_plan_status"] = plan.get("status")
            updated["remote_directory_created"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


def payload_sync_plan(envelope: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    run = envelope.get("run") or {}
    remote_worktree = run.get("remote_worktree")
    failures: list[str] = []
    mappings: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    total_size_bytes = 0

    if not isinstance(remote_worktree, str):
        return {
            "status": "FAIL",
            "failures": ["remote_worktree_missing"],
            "planned_side_effects": {
                "sync_archive_created": False,
                "ssh_started": False,
                "network_started": False,
                "remote_files_written": False,
            },
        }

    worktree_path = PurePosixPath(remote_worktree)
    for record in payload.get("payload_records") or []:
        rel = record.get("path")
        if not isinstance(rel, str) or not rel:
            failures.append("payload_path_missing")
            continue
        rel_path = PurePosixPath(rel)
        if rel_path.is_absolute() or ".." in rel_path.parts:
            failures.append(f"payload_path_escape:{rel}")
            continue
        remote_path = worktree_path / rel_path
        if remote_path.parts[: len(worktree_path.parts)] != worktree_path.parts:
            failures.append(f"remote_payload_destination_escape:{rel}")
            continue
        if not isinstance(record.get("sha256"), str) or not isinstance(record.get("size_bytes"), int):
            failures.append(f"payload_metadata_missing:{rel}")
            continue
        category = record.get("category") if isinstance(record.get("category"), str) else "unknown"
        category_counts[category] = category_counts.get(category, 0) + 1
        total_size_bytes += int(record.get("size_bytes") or 0)
        mappings.append(
            {
                "source_path": rel,
                "remote_path": str(remote_path),
                "category": category,
                "size_bytes": record.get("size_bytes"),
                "sha256": record.get("sha256"),
                "is_executable": record.get("is_executable") is True,
            }
        )

    if not mappings:
        failures.append("payload_mapping_empty")

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "remote_worktree": str(worktree_path),
        "payload_file_count": len(mappings),
        "payload_total_size_bytes": total_size_bytes,
        "category_counts": category_counts,
        "mapping_sample": mappings[:20],
        "all_destinations_under_remote_worktree": not failures,
        "planned_side_effects": {
            "sync_archive_created": False,
            "ssh_started": False,
            "network_started": False,
            "remote_files_written": False,
        },
    }


def apply_payload_sync_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "sync_allowlisted_source_files_only":
            updated["executor_status"] = "PLANNED_NO_NETWORK_ALLOWLISTED_PAYLOAD_SYNC"
            updated["payload_sync_plan_status"] = plan.get("status")
            updated["payload_file_count"] = plan.get("payload_file_count")
            updated["sync_archive_created"] = False
            updated["remote_files_written"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


def remote_build_plan(envelope: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    run = envelope.get("run") or {}
    remote_worktree = run.get("remote_worktree")
    failures: list[str] = []
    commands: list[dict[str, Any]] = []
    payload_paths = {record.get("path") for record in payload.get("payload_records") or []}

    if not isinstance(remote_worktree, str):
        return {
            "status": "FAIL",
            "failures": ["remote_worktree_missing"],
            "planned_side_effects": {
                "cargo_invoked": False,
                "ssh_started": False,
                "network_started": False,
                "remote_files_written": False,
            },
        }

    worktree_path = PurePosixPath(remote_worktree)
    if "Cargo.toml" not in payload_paths or "Cargo.lock" not in payload_paths:
        failures.append("cargo_manifest_or_lock_missing_from_payload")

    for target in DECLARED_REMOTE_BUILD_TARGETS:
        bin_source = f"src/bin/{target}.rs"
        if bin_source not in payload_paths:
            failures.append(f"build_target_source_missing:{target}")
            continue
        commands.append(
            {
                "target": target,
                "remote_cwd": str(worktree_path),
                "argv": ["cargo", "build", "--locked", "--bin", target],
                "requires_network_for_dependencies": False,
                "uses_env_file": False,
                "writes_target_dir": True,
                "executed": False,
            }
        )

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "remote_worktree": str(worktree_path),
        "declared_build_targets": list(DECLARED_REMOTE_BUILD_TARGETS),
        "build_commands": commands,
        "build_command_count": len(commands),
        "all_build_cwds_under_remote_worktree": all(
            command.get("remote_cwd") == str(worktree_path) for command in commands
        ),
        "planned_side_effects": {
            "cargo_invoked": False,
            "ssh_started": False,
            "network_started": False,
            "remote_files_written": False,
            "target_dir_written": False,
        },
    }


def apply_remote_build_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "build_declared_remote_targets_only":
            updated["executor_status"] = "PLANNED_NO_NETWORK_DECLARED_REMOTE_BUILD"
            updated["remote_build_plan_status"] = plan.get("status")
            updated["declared_build_targets"] = plan.get("declared_build_targets")
            updated["cargo_invoked"] = False
            updated["remote_files_written"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


def shared_ingress_preflight_plan(envelope: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    run = envelope.get("run") or {}
    remote_worktree = run.get("remote_worktree")
    remote_run_dir = run.get("remote_run_dir")
    role = run.get("shared_ingress_role")
    root = run.get("shared_ingress_root")
    payload_paths = {record.get("path") for record in payload.get("payload_records") or []}
    failures: list[str] = []

    if role != "client":
        failures.append("shared_ingress_role_not_client")
    if root != REQUIRED_SHARED_INGRESS_ROOT:
        failures.append("shared_ingress_root_not_allowlisted")
    if SHARED_INGRESS_PREFLIGHT_SCRIPT not in payload_paths:
        failures.append("shared_ingress_preflight_script_missing_from_payload")

    if not isinstance(remote_worktree, str):
        failures.append("remote_worktree_missing")
        worktree_path = None
    else:
        worktree_path = PurePosixPath(remote_worktree)
    if not isinstance(remote_run_dir, str):
        failures.append("remote_run_dir_missing")
        output_dir = None
    else:
        output_dir = str(PurePosixPath(remote_run_dir) / "shared_ingress_preflight")

    argv = None
    if worktree_path is not None and output_dir is not None:
        argv = [
            "python3",
            str(worktree_path / SHARED_INGRESS_PREFLIGHT_SCRIPT),
            "--root",
            REQUIRED_SHARED_INGRESS_ROOT,
            "--output-dir",
            output_dir,
        ]

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "shared_ingress_role": role,
        "shared_ingress_root": root,
        "required_shared_ingress_root": REQUIRED_SHARED_INGRESS_ROOT,
        "preflight_script": SHARED_INGRESS_PREFLIGHT_SCRIPT,
        "remote_worktree": str(worktree_path) if worktree_path else remote_worktree,
        "remote_output_dir": output_dir,
        "argv": argv,
        "expected_success_status": "OK",
        "expected_unavailable_rc": 69,
        "read_only_manifest_probe": True,
        "planned_side_effects": {
            "preflight_executed": False,
            "ssh_started": False,
            "network_started": False,
            "broker_started": False,
            "broker_stopped": False,
            "broker_repaired": False,
            "connected_to_broker": False,
            "modified_shared_ingress": False,
            "remote_files_written": False,
        },
    }


def apply_shared_ingress_preflight_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "run_readonly_shared_ingress_preflight_before_canary":
            updated["executor_status"] = "PLANNED_NO_NETWORK_SHARED_INGRESS_PREFLIGHT"
            updated["shared_ingress_preflight_plan_status"] = plan.get("status")
            updated["shared_ingress_root"] = plan.get("shared_ingress_root")
            updated["preflight_executed"] = False
            updated["broker_modified"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


def non_secret_canary_env(envelope: dict[str, Any]) -> dict[str, str]:
    run = envelope.get("run") or {}
    risk = envelope.get("risk") or {}
    return {
        "PM_STRATEGY": "xuan_b27_dplus",
        "PM_XUAN_B27_DPLUS_MODE": "canary",
        "PM_XUAN_B27_DPLUS_EXPLICIT_CANARY_APPROVAL": "true",
        "PM_XUAN_B27_DPLUS_RUNTIME_WIRING_ENABLED": "true",
        "PM_XUAN_B27_DPLUS_OMS_ADAPTER_ENABLED": "true",
        "PM_XUAN_B27_DPLUS_MARKET_SLUG": str(run.get("market_slug") or ""),
        "PM_XUAN_B27_DPLUS_TARGET_QTY": str(risk.get("target_qty") or ""),
        "PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC": str(risk.get("max_open_cost_usdc") or ""),
        "PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC": str(risk.get("max_strategy_exposure_usdc") or ""),
        "PM_XUAN_B27_DPLUS_MAX_ACTIVE_MARKETS": str(risk.get("max_active_markets") or ""),
        "PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS": str(risk.get("max_live_orders") or ""),
        "PM_XUAN_B27_DPLUS_POST_ONLY": str(risk.get("post_only")).lower(),
        "PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER": str(risk.get("allow_passive_taker")).lower(),
        "PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN": str(risk.get("stop_on_unknown")).lower(),
        "PM_SHARED_INGRESS_ROLE": str(run.get("shared_ingress_role") or ""),
        "PM_SHARED_INGRESS_ROOT": str(run.get("shared_ingress_root") or ""),
    }


def auth_injection_plan(envelope: dict[str, Any]) -> dict[str, Any]:
    env = non_secret_canary_env(envelope)
    failures: list[str] = []
    expected = {
        "PM_STRATEGY": "xuan_b27_dplus",
        "PM_XUAN_B27_DPLUS_MODE": "canary",
        "PM_XUAN_B27_DPLUS_EXPLICIT_CANARY_APPROVAL": "true",
        "PM_XUAN_B27_DPLUS_RUNTIME_WIRING_ENABLED": "true",
        "PM_XUAN_B27_DPLUS_OMS_ADAPTER_ENABLED": "true",
        "PM_XUAN_B27_DPLUS_MARKET_SLUG": "btc-updown-5m",
        "PM_XUAN_B27_DPLUS_TARGET_QTY": "5",
        "PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC": "50",
        "PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC": "100",
        "PM_XUAN_B27_DPLUS_MAX_ACTIVE_MARKETS": "1",
        "PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS": "2",
        "PM_XUAN_B27_DPLUS_POST_ONLY": "true",
        "PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER": "false",
        "PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN": "true",
        "PM_SHARED_INGRESS_ROLE": "client",
        "PM_SHARED_INGRESS_ROOT": REQUIRED_SHARED_INGRESS_ROOT,
    }
    for key, value in expected.items():
        if env.get(key) != value:
            failures.append(f"non_secret_env_mismatch:{key}")

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "non_secret_env": env,
        "non_secret_env_count": len(env),
        "allowed_secret_auth_classes": list(SECRET_AUTH_CLASSES),
        "secret_values_recorded": False,
        "secret_key_names_recorded": False,
        "credential_derivation_performed": False,
        "injection_method": "ephemeral_process_environment_only",
        "remote_env_file_written": False,
        "remote_shell_history_written": False,
        "stdin_secret_payload_materialized": False,
        "planned_side_effects": {
            "auth_env_injected": False,
            "derived_credentials": False,
            "printed_secret_values": False,
            "remote_env_file_written": False,
            "ssh_started": False,
            "network_started": False,
        },
    }


def apply_auth_injection_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "inject_auth_vars_ephemerally_only":
            updated["executor_status"] = "PLANNED_NO_NETWORK_EPHEMERAL_AUTH_INJECTION"
            updated["auth_injection_plan_status"] = plan.get("status")
            updated["secret_values_recorded"] = False
            updated["remote_env_file_written"] = False
            updated["auth_env_injected"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


def bounded_canary_process_plan(envelope: dict[str, Any], auth_plan: dict[str, Any]) -> dict[str, Any]:
    run = envelope.get("run") or {}
    risk = envelope.get("risk") or {}
    remote_worktree = run.get("remote_worktree")
    remote_run_dir = run.get("remote_run_dir")
    failures: list[str] = []

    if auth_plan.get("status") != "PASS":
        failures.append("auth_injection_plan_not_pass")
    if run.get("run_class") != "G2_READ_WRITE_CANARY_SMOKE":
        failures.append("run_class_not_g2_read_write_canary_smoke")
    if run.get("market_slug") != "btc-updown-5m":
        failures.append("market_slug_not_btc_updown_5m")
    if run.get("duration_seconds") != 1800:
        failures.append("duration_seconds_not_1800")
    if run.get("max_rounds") != 6:
        failures.append("max_rounds_not_6")
    if risk.get("post_only") is not True:
        failures.append("post_only_not_true")
    if risk.get("allow_passive_taker") is not False:
        failures.append("allow_passive_taker_not_false")
    if risk.get("stop_on_unknown") is not True:
        failures.append("stop_on_unknown_not_true")
    if risk.get("max_live_orders") != 2:
        failures.append("max_live_orders_not_2")
    if risk.get("max_open_cost_usdc") != 50:
        failures.append("max_open_cost_usdc_not_50")
    if risk.get("max_strategy_exposure_usdc") != 100:
        failures.append("max_strategy_exposure_usdc_not_100")
    if risk.get("max_active_markets") != 1:
        failures.append("max_active_markets_not_1")

    if not isinstance(remote_worktree, str):
        failures.append("remote_worktree_missing")
        worktree_path = None
    else:
        worktree_path = PurePosixPath(remote_worktree)
    if not isinstance(remote_run_dir, str):
        failures.append("remote_run_dir_missing")
        run_dir_path = None
    else:
        run_dir_path = PurePosixPath(remote_run_dir)

    binary_path = str(worktree_path / CANARY_BINARY_RELATIVE) if worktree_path else None
    log_root = str(run_dir_path / "logs") if run_dir_path else None
    run_manifest_path = str(run_dir_path / "g2_canary_run_manifest.json") if run_dir_path else None
    stdout_path = str(run_dir_path / "logs" / "g2_canary_stdout.log") if run_dir_path else None
    stderr_path = str(run_dir_path / "logs" / "g2_canary_stderr.log") if run_dir_path else None

    process_env_overlay = dict(auth_plan.get("non_secret_env") or {})
    if log_root:
        process_env_overlay["PM_LOG_ROOT"] = log_root
    process_env_overlay["POLYMARKET_MARKET_SLUG"] = str(run.get("market_slug") or "")
    process_env_overlay["RUST_LOG"] = "info"

    expected_overlay = {
        "PM_XUAN_B27_DPLUS_MODE": "canary",
        "PM_XUAN_B27_DPLUS_MARKET_SLUG": "btc-updown-5m",
        "POLYMARKET_MARKET_SLUG": "btc-updown-5m",
        "PM_SHARED_INGRESS_ROOT": REQUIRED_SHARED_INGRESS_ROOT,
        "PM_SHARED_INGRESS_ROLE": "client",
    }
    for key, value in expected_overlay.items():
        if process_env_overlay.get(key) != value:
            failures.append(f"process_env_overlay_mismatch:{key}")

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "remote_worktree": str(worktree_path) if worktree_path else remote_worktree,
        "remote_run_dir": str(run_dir_path) if run_dir_path else remote_run_dir,
        "binary": binary_path,
        "argv": [binary_path] if binary_path else [],
        "remote_log_root": log_root,
        "run_manifest_path": run_manifest_path,
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
        "process_env_overlay": process_env_overlay,
        "non_secret_env_count": len(process_env_overlay),
        "allowed_secret_auth_classes": list(SECRET_AUTH_CLASSES),
        "secret_values_recorded": False,
        "secret_key_names_recorded": False,
        "duration_seconds": run.get("duration_seconds"),
        "max_rounds": run.get("max_rounds"),
        "supervisor": {
            "mode": "single_process_bounded_duration_and_rounds",
            "graceful_signal": "TERM",
            "kill_after_seconds": CANARY_STOP_KILL_AFTER_SECONDS,
            "duration_seconds": run.get("duration_seconds"),
            "max_rounds": run.get("max_rounds"),
        },
        "stop_conditions": {
            "stop_on_unknown": risk.get("stop_on_unknown") is True,
            "stop_on_source_truth_unknown_or_failed": True,
            "stop_on_risk_cap_violation": True,
            "stop_on_unexpected_taker_fill": True,
            "stop_on_recorder_decode_or_critical_drop": True,
            "stop_on_broker_preflight_failure": True,
            "stop_on_duration_seconds": run.get("duration_seconds") == 1800,
            "stop_on_max_rounds": run.get("max_rounds") == 6,
        },
        "planned_side_effects": {
            "process_started": False,
            "ssh_started": False,
            "network_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "stdout_opened": False,
            "stderr_opened": False,
            "remote_files_written": False,
        },
    }


def apply_bounded_canary_process_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "start_single_bounded_g2_canary_process":
            updated["executor_status"] = "PLANNED_NO_NETWORK_BOUNDED_CANARY_PROCESS"
            updated["bounded_canary_process_plan_status"] = plan.get("status")
            updated["duration_seconds"] = plan.get("duration_seconds")
            updated["max_rounds"] = plan.get("max_rounds")
            updated["process_started"] = False
            updated["orders_sent"] = False
            updated["cancels_sent"] = False
            updated["redeems_sent"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


def wait_stop_supervision_plan(envelope: dict[str, Any], process_plan: dict[str, Any]) -> dict[str, Any]:
    run = envelope.get("run") or {}
    failures: list[str] = []
    stop_conditions = process_plan.get("stop_conditions") or {}
    supervisor = process_plan.get("supervisor") or {}

    if process_plan.get("status") != "PASS":
        failures.append("bounded_canary_process_plan_not_pass")
    if process_plan.get("duration_seconds") != 1800 or run.get("duration_seconds") != 1800:
        failures.append("duration_seconds_not_1800")
    if process_plan.get("max_rounds") != 6 or run.get("max_rounds") != 6:
        failures.append("max_rounds_not_6")
    if supervisor.get("kill_after_seconds") != CANARY_STOP_KILL_AFTER_SECONDS:
        failures.append("kill_after_seconds_not_30")
    for required in (
        "stop_on_unknown",
        "stop_on_source_truth_unknown_or_failed",
        "stop_on_risk_cap_violation",
        "stop_on_unexpected_taker_fill",
        "stop_on_recorder_decode_or_critical_drop",
        "stop_on_broker_preflight_failure",
        "stop_on_duration_seconds",
        "stop_on_max_rounds",
    ):
        if stop_conditions.get(required) is not True:
            failures.append(f"missing_stop_condition:{required}")
    for required_path in ("run_manifest_path", "stdout_path", "stderr_path", "remote_log_root"):
        if not process_plan.get(required_path):
            failures.append(f"missing_monitor_path:{required_path}")

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "mode": "single_process_duration_or_round_limit_supervision",
        "duration_seconds": process_plan.get("duration_seconds"),
        "max_rounds": process_plan.get("max_rounds"),
        "poll_interval_seconds": 5,
        "graceful_signal": supervisor.get("graceful_signal"),
        "kill_after_seconds": supervisor.get("kill_after_seconds"),
        "monitor_paths": {
            "run_manifest_path": process_plan.get("run_manifest_path"),
            "stdout_path": process_plan.get("stdout_path"),
            "stderr_path": process_plan.get("stderr_path"),
            "remote_log_root": process_plan.get("remote_log_root"),
        },
        "stop_triggers": {
            "duration_seconds": process_plan.get("duration_seconds") == 1800,
            "max_rounds": process_plan.get("max_rounds") == 6,
            "process_exit": True,
            "source_truth_unknown_or_failed": True,
            "risk_cap_violation": True,
            "unexpected_taker_fill": True,
            "recorder_decode_or_critical_drop": True,
            "broker_preflight_failure": True,
        },
        "termination_sequence": [
            "observe_process_exit_or_stop_trigger",
            "send_graceful_term_if_still_running",
            "wait_kill_after_seconds",
            "send_kill_if_still_running",
            "record_exit_status_for_post_run_review",
        ],
        "planned_side_effects": {
            "wait_started": False,
            "process_polled": False,
            "signal_sent": False,
            "kill_sent": False,
            "process_reaped": False,
            "stdout_read": False,
            "stderr_read": False,
            "remote_files_read": False,
            "remote_files_written": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "ssh_started": False,
            "network_started": False,
        },
    }


def apply_wait_stop_supervision_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "wait_or_stop_at_duration_or_round_limit":
            updated["executor_status"] = "PLANNED_NO_NETWORK_WAIT_STOP_SUPERVISION"
            updated["wait_stop_supervision_plan_status"] = plan.get("status")
            updated["duration_seconds"] = plan.get("duration_seconds")
            updated["max_rounds"] = plan.get("max_rounds")
            updated["poll_interval_seconds"] = plan.get("poll_interval_seconds")
            updated["signal_sent"] = False
            updated["kill_sent"] = False
            updated["process_reaped"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


def artifact_pullback_review_plan(
    envelope: dict[str, Any],
    process_plan: dict[str, Any],
    supervision_plan: dict[str, Any],
    workspace_root: Path,
) -> dict[str, Any]:
    run = envelope.get("run") or {}
    post_run_review = envelope.get("post_run_review") or {}
    remote_run_dir = run.get("remote_run_dir")
    failures: list[str] = []

    if process_plan.get("status") != "PASS":
        failures.append("bounded_canary_process_plan_not_pass")
    if supervision_plan.get("status") != "PASS":
        failures.append("wait_stop_supervision_plan_not_pass")
    if post_run_review.get("summarizer") != G2_CANARY_SUMMARIZER_SCRIPT:
        failures.append("post_run_summarizer_not_allowlisted")
    if post_run_review.get("require_check_acceptance") is not True:
        failures.append("post_run_check_acceptance_not_required")
    if post_run_review.get("require_secret_sentinel_scan") is not True:
        failures.append("post_run_secret_sentinel_scan_not_required")

    if not isinstance(remote_run_dir, str):
        failures.append("remote_run_dir_missing")
        remote_run_path = None
        local_artifact_dir = None
    else:
        remote_run_path = PurePosixPath(remote_run_dir)
        if not remote_run_path.is_absolute() or ".." in remote_run_path.parts:
            failures.append("remote_run_dir_not_safe_absolute")
        run_suffix = remote_run_path.name.removeprefix(REMOTE_RUN_DIR_PREFIX)
        local_artifact_dir = (
            workspace_root
            / "xuan_research_artifacts"
            / f"xuan_b27_dplus_g2_canary_artifacts_{run_suffix}"
        )
        try:
            local_artifact_dir.relative_to(workspace_root / "xuan_research_artifacts")
        except ValueError:
            failures.append("local_artifact_dir_not_under_xuan_research_artifacts")

    local_summary_path = local_artifact_dir / "g2_canary_run_summary.json" if local_artifact_dir else None
    local_acceptance_review_path = (
        local_artifact_dir / "local_acceptance_review.json" if local_artifact_dir else None
    )
    expected_remote_paths = []
    if remote_run_path is not None:
        expected_remote_paths = [
            str(remote_run_path / "g2_canary_run_manifest.json"),
            str(remote_run_path / "g2_canary_acceptance_review.json"),
            str(remote_run_path / "logs" / "g2_canary_stdout.log"),
            str(remote_run_path / "logs" / "g2_canary_stderr.log"),
            str(remote_run_path / "shared_ingress_preflight" / "manifest.json"),
        ]

    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "remote_run_dir": str(remote_run_path) if remote_run_path else remote_run_dir,
        "local_artifact_dir": str(local_artifact_dir) if local_artifact_dir else None,
        "expected_remote_paths": expected_remote_paths,
        "expected_remote_tree_roots": [
            str(remote_run_path / "logs") if remote_run_path else None,
            str(remote_run_path / "recorder") if remote_run_path else None,
            str(remote_run_path / "shared_ingress_preflight") if remote_run_path else None,
        ],
        "post_run_review_plan": {
            "summarizer": G2_CANARY_SUMMARIZER_SCRIPT,
            "argv": [
                "python3",
                G2_CANARY_SUMMARIZER_SCRIPT,
                str(local_artifact_dir) if local_artifact_dir else "",
                "--output",
                str(local_summary_path) if local_summary_path else "",
                "--check-acceptance",
            ],
            "summary_output_path": str(local_summary_path) if local_summary_path else None,
            "local_acceptance_review_path": str(local_acceptance_review_path)
            if local_acceptance_review_path
            else None,
            "require_check_acceptance": post_run_review.get("require_check_acceptance") is True,
            "require_secret_sentinel_scan": post_run_review.get("require_secret_sentinel_scan") is True,
            "secret_sentinel_source": "caller_provided_at_execution_time",
            "secret_sentinel_values_recorded": False,
            "secret_key_names_recorded": False,
        },
        "acceptance_requirements": {
            "summary_status": "PASS_G2_CANARY_ACCEPTANCE",
            "exact_approval_scope": True,
            "strategy": "xuan_b27_dplus",
            "market_slug": "btc-updown-5m",
            "source_truth_all_pass": True,
            "order_attempt_trace_linked": True,
            "venue_order_id_linked": True,
            "user_ws_connected": True,
            "decode_errors": 0,
            "recorder_critical_drops": 0,
            "unexpected_taker_fills": 0,
            "forbidden_side_effects_absent": True,
            "secret_sentinel_hits": 0,
        },
        "planned_side_effects": {
            "remote_files_read": False,
            "local_files_written": False,
            "summary_executed": False,
            "secret_scan_executed": False,
            "acceptance_review_written": False,
            "ssh_started": False,
            "network_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
            "shared_ingress_modified": False,
            "remote_env_file_written": False,
        },
    }


def apply_artifact_pullback_review_plan_to_phase_plan(
    phase_plan: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in phase_plan:
        updated = dict(item)
        if item.get("phase") == "pull_artifacts_to_xuan_research_artifacts":
            updated["executor_status"] = "PLANNED_NO_NETWORK_ARTIFACT_PULLBACK_REVIEW"
            updated["artifact_pullback_review_plan_status"] = plan.get("status")
            updated["remote_files_read"] = False
            updated["local_files_written"] = False
            updated["summary_executed"] = False
            updated["secret_scan_executed"] = False
            updated["side_effects_performed"] = False
        out.append(updated)
    return out


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
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_g2_canary_executor_{label}"
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
    approval_envelope = read_json(args.approval_envelope)
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

    phase_plan = build_executor_phase_plan(contract)
    run_root_plan = remote_run_root_plan(approval_envelope)
    manifest["remote_run_root_plan"] = run_root_plan
    if run_root_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_REMOTE_RUN_ROOT_PLAN_INVALID",
                "reason": "remote worktree/run directory plan is not isolated or structurally valid",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 69
    phase_plan = apply_remote_root_plan_to_phase_plan(phase_plan, run_root_plan)

    sync_plan = payload_sync_plan(approval_envelope, payload)
    manifest["payload_sync_plan"] = sync_plan
    if sync_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_PAYLOAD_SYNC_PLAN_INVALID",
                "reason": "payload allowlist cannot be mapped safely into the isolated remote worktree",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 70
    phase_plan = apply_payload_sync_plan_to_phase_plan(phase_plan, sync_plan)

    build_plan = remote_build_plan(approval_envelope, payload)
    manifest["remote_build_plan"] = build_plan
    if build_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_REMOTE_BUILD_PLAN_INVALID",
                "reason": "declared remote build plan is missing required payload files or has unsafe structure",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 71
    phase_plan = apply_remote_build_plan_to_phase_plan(phase_plan, build_plan)

    preflight_plan = shared_ingress_preflight_plan(approval_envelope, payload)
    manifest["shared_ingress_preflight_plan"] = preflight_plan
    if preflight_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_SHARED_INGRESS_PREFLIGHT_PLAN_INVALID",
                "reason": "read-only shared-ingress preflight plan is missing allowlisted root, client role, or script",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 72
    phase_plan = apply_shared_ingress_preflight_plan_to_phase_plan(phase_plan, preflight_plan)

    auth_plan = auth_injection_plan(approval_envelope)
    manifest["auth_injection_plan"] = auth_plan
    if auth_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_AUTH_INJECTION_PLAN_INVALID",
                "reason": "ephemeral auth injection plan does not match exact G2 canary environment contract",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 73
    phase_plan = apply_auth_injection_plan_to_phase_plan(phase_plan, auth_plan)

    process_plan = bounded_canary_process_plan(approval_envelope, auth_plan)
    manifest["bounded_canary_process_plan"] = process_plan
    if process_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_BOUNDED_CANARY_PROCESS_PLAN_INVALID",
                "reason": "bounded G2 canary process plan does not match exact approval, risk, or env contract",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 74
    phase_plan = apply_bounded_canary_process_plan_to_phase_plan(phase_plan, process_plan)

    supervision_plan = wait_stop_supervision_plan(approval_envelope, process_plan)
    manifest["wait_stop_supervision_plan"] = supervision_plan
    if supervision_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_WAIT_STOP_SUPERVISION_PLAN_INVALID",
                "reason": "wait/stop supervision plan does not match bounded canary process limits or stop conditions",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 76
    phase_plan = apply_wait_stop_supervision_plan_to_phase_plan(phase_plan, supervision_plan)

    pullback_plan = artifact_pullback_review_plan(
        approval_envelope,
        process_plan,
        supervision_plan,
        root,
    )
    manifest["artifact_pullback_review_plan"] = pullback_plan
    if pullback_plan.get("status") != "PASS":
        manifest.update(
            {
                "status": "REFUSED_ARTIFACT_PULLBACK_REVIEW_PLAN_INVALID",
                "reason": "artifact pullback/review plan is missing required local review, acceptance, or secret-sentinel boundaries",
            }
        )
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 77
    phase_plan = apply_artifact_pullback_review_plan_to_phase_plan(phase_plan, pullback_plan)

    first_unimplemented = next(
        (
            item["phase"]
            for item in phase_plan
            if item.get("effectful") is True
            and item.get("executor_status") != "PLANNED_NO_NETWORK_REMOTE_ROOT_CREATION"
            and item.get("executor_status") != "PLANNED_NO_NETWORK_ALLOWLISTED_PAYLOAD_SYNC"
            and item.get("executor_status") != "PLANNED_NO_NETWORK_DECLARED_REMOTE_BUILD"
            and item.get("executor_status") != "PLANNED_NO_NETWORK_SHARED_INGRESS_PREFLIGHT"
            and item.get("executor_status") != "PLANNED_NO_NETWORK_EPHEMERAL_AUTH_INJECTION"
            and item.get("executor_status") != "PLANNED_NO_NETWORK_BOUNDED_CANARY_PROCESS"
            and item.get("executor_status") != "PLANNED_NO_NETWORK_WAIT_STOP_SUPERVISION"
            and item.get("executor_status") != "PLANNED_NO_NETWORK_ARTIFACT_PULLBACK_REVIEW"
        ),
        None,
    )
    manifest.update(
        {
            "status": "REFUSED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED",
            "reason": "validated local preconditions and all executor phase plans through artifact pullback/review, but effectful execution is not implemented",
            "phase_plan": phase_plan,
            "mandatory_abort_conditions": contract.get("mandatory_abort_conditions") or [],
            "allowed_side_effects_after_exact_executor_approval": contract.get(
                "allowed_side_effects_after_exact_executor_approval"
            )
            or {},
            "forbidden_side_effects_even_after_exact_executor_approval": contract.get(
                "forbidden_side_effects_even_after_exact_executor_approval"
            )
            or {},
            "first_unimplemented_effectful_phase": first_unimplemented,
            "next_gate": "implement effectful phases one by one behind this preflight and preserve fail-closed tests",
        }
    )
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 75


if __name__ == "__main__":
    raise SystemExit(main())
