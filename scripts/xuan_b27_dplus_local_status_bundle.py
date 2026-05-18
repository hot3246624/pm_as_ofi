#!/usr/bin/env python3
"""Build a local, no-network status bundle for xuan_b27_dplus.

The bundle is a compact audit artifact that points to the latest local gates
and classifies whether the current blocker is local readiness or the external
shared-ingress broker. It does not start or connect to any process.
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path


AUTOMATION_PATH = Path.home() / ".codex" / "automations" / "xuan-research-dplus-implementation-heartbeat" / "automation.toml"
FORBIDDEN_EC2_READONLY_SIDE_EFFECT_FIELDS = (
    "systemd_called",
    "service_control_called",
    "broker_started",
    "broker_stopped",
    "broker_repaired",
    "orders_sent",
    "cancels_sent",
    "redeems_sent",
    "remote_files_written",
    "deployed_code",
    "scp_or_rsync_used",
    "tunnel_opened",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path: Path | None) -> dict:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def latest_manifest(root: Path, pattern: str, artifact: str | None = None) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json"))]
    if artifact:
        matches = [p for p in matches if read_json(p).get("artifact") == artifact]
        if artifact.endswith("_smoke"):
            pass_matches = [p for p in matches if read_json(p).get("status") == "PASS"]
            if pass_matches:
                matches = pass_matches
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime_ns)


def latest_json_file(root: Path, pattern: str, artifact: str | None = None) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern))]
    if artifact:
        matches = [p for p in matches if read_json(p).get("artifact") == artifact]
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime_ns)


def parse_simple_toml(path: Path) -> dict:
    out: dict[str, object] = {"path": str(path), "exists": path.exists()}
    if not path.exists():
        return out
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            out[key] = value[1:-1]
        elif value.lower() in {"true", "false"}:
            out[key] = value.lower() == "true"
        else:
            out[key] = value
    return out


def summarize_manifest(path: Path | None, value: dict, fields: tuple[str, ...]) -> dict:
    out = {"path": str(path) if path else None, "exists": bool(path and path.exists())}
    for field in fields:
        out[field] = value.get(field)
    return out


def summarize_remote_shared_ingress_probe(path: Path | None, value: dict) -> dict:
    remote = value.get("remote_shared_ingress") or {}
    socket_checks = remote.get("socket_checks") or []
    side_effects = value.get("side_effects") or {}
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "scope": value.get("scope"),
        "captured_utc": value.get("captured_utc"),
        "remote_root": remote.get("root"),
        "remote_manifest_path": remote.get("manifest_path"),
        "remote_manifest_exists": remote.get("manifest_exists"),
        "protocol_version": remote.get("protocol_version"),
        "schema_version": remote.get("schema_version"),
        "heartbeat_age_ms_at_probe": remote.get("heartbeat_age_ms_at_probe"),
        "socket_check_count": len(socket_checks),
        "all_socket_checks_ok": bool(socket_checks) and all(
            check.get("exists") is True and check.get("is_socket") is True for check in socket_checks
        ),
        "side_effects_all_false": bool(side_effects) and all(value is False for value in side_effects.values()),
        "local_process_can_use_remote_unix_sockets": False,
        "requires_same_host_observer": value.get("status") == "OK",
    }


def summarize_ec2_readonly_attempt(path: Path | None, value: dict) -> dict:
    remote = value.get("remote_shared_ingress") or {}
    discovery = value.get("remote_entrypoint_discovery") or {}
    socket_checks = remote.get("socket_checks") or []
    side_effects = value.get("side_effects") or {}
    forbidden_side_effects_absent = bool(side_effects) and all(
        side_effects.get(field) is False for field in FORBIDDEN_EC2_READONLY_SIDE_EFFECT_FIELDS
    )
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "created_utc": value.get("created_utc"),
        "remote_broker_root": remote.get("root"),
        "remote_broker_manifest_exists": remote.get("manifest_exists"),
        "remote_broker_protocol_version": remote.get("protocol_version"),
        "remote_broker_schema_version": remote.get("schema_version"),
        "remote_broker_socket_check_count": len(socket_checks),
        "remote_broker_socket_checks_ok": bool(socket_checks) and all(
            check.get("exists") is True and check.get("is_socket") is True for check in socket_checks
        ),
        "candidate_root_count": len(discovery.get("candidate_roots_checked") or []),
        "required_remote_entrypoint_found": discovery.get("required_files_found_in_any_candidate_root"),
        "reason": discovery.get("reason"),
        "side_effects_all_false": bool(side_effects) and all(value is False for value in side_effects.values()),
        "forbidden_side_effects_absent": forbidden_side_effects_absent,
        "observer_started": side_effects.get("observer_started"),
        "user_ws_started": side_effects.get("user_ws_started"),
        "orders_sent": side_effects.get("orders_sent"),
        "cancels_sent": side_effects.get("cancels_sent"),
        "redeems_sent": side_effects.get("redeems_sent"),
        "deployed_code": side_effects.get("deployed_code"),
        "remote_files_written": side_effects.get("remote_files_written"),
        "next_gate": value.get("next_gate"),
    }


def summarize_ec2_entrypoint_manifest_smoke(path: Path | None, value: dict) -> dict:
    entrypoint_manifest = value.get("entrypoint_manifest")
    entrypoint_path = Path(entrypoint_manifest) if isinstance(entrypoint_manifest, str) else None
    entrypoint = read_json(entrypoint_path)
    all_records = (
        (entrypoint.get("required_entrypoint_files") or [])
        + (entrypoint.get("supporting_gate_files") or [])
        + (entrypoint.get("required_rust_module_hints") or [])
    )
    expected_executable_files = entrypoint.get("expected_executable_files") or []
    missing_executable_files = entrypoint.get("missing_executable_files")
    executable_contract_ok = (
        entrypoint.get("status") == "PASS"
        and bool(expected_executable_files)
        and missing_executable_files == []
        and all(
            record.get("expected_executable") is not True
            or (record.get("is_executable") is True and record.get("has_shebang") is True)
            for record in all_records
        )
    )
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "scope": value.get("scope"),
        "orders_sent": value.get("orders_sent"),
        "auth_network_started": value.get("auth_network_started"),
        "entrypoint_manifest": entrypoint_manifest,
        "entrypoint_manifest_exists": bool(entrypoint_path and entrypoint_path.exists()),
        "entrypoint_manifest_status": entrypoint.get("status"),
        "expected_executable_count": len(expected_executable_files),
        "missing_executable_files": missing_executable_files,
        "executable_contract_ok": executable_contract_ok,
        "required_entrypoint_file_count": len(entrypoint.get("required_entrypoint_files") or []),
        "supporting_gate_file_count": len(entrypoint.get("supporting_gate_files") or []),
    }


def summarize_ec2_resync_diagnostic_plan_smoke(path: Path | None, value: dict) -> dict:
    plan_manifest = value.get("plan_manifest")
    plan_path = Path(plan_manifest) if isinstance(plan_manifest, str) else None
    plan = read_json(plan_path)
    proposed_run = plan.get("proposed_diagnostic_run") or {}
    proposed_resync = plan.get("proposed_resync") or {}
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "scope": value.get("scope"),
        "orders_sent": value.get("orders_sent"),
        "auth_network_started": value.get("auth_network_started"),
        "plan_manifest": plan_manifest,
        "plan_manifest_exists": bool(plan_path and plan_path.exists()),
        "plan_status": plan.get("status"),
        "local_gates_ok": plan.get("local_gates_ok"),
        "stale_sync_confirmed": plan.get("stale_sync_confirmed"),
        "requires_explicit_resync_rebuild_approval": proposed_resync.get(
            "requires_explicit_resync_rebuild_approval"
        ),
        "requires_explicit_exact_run_approval": proposed_run.get("requires_explicit_exact_run_approval"),
        "proposed_remote_worktree": proposed_resync.get("remote_worktree"),
        "proposed_duration_seconds": proposed_run.get("duration_seconds"),
        "proposed_market_slug": proposed_run.get("market_slug"),
        "shared_ingress_root": proposed_run.get("shared_ingress_root"),
    }


def summarize_ec2_30m_acceptance_plan_smoke(path: Path | None, value: dict) -> dict:
    plan_manifest = value.get("plan_manifest")
    plan_path = Path(plan_manifest) if isinstance(plan_manifest, str) else None
    plan = read_json(plan_path)
    proposed_run = plan.get("proposed_acceptance_run") or {}
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "scope": value.get("scope"),
        "orders_sent": value.get("orders_sent"),
        "auth_network_started": value.get("auth_network_started"),
        "plan_manifest": plan_manifest,
        "plan_manifest_exists": bool(plan_path and plan_path.exists()),
        "plan_status": plan.get("status"),
        "diagnostic_passed": plan.get("diagnostic_passed"),
        "resync_ready": plan.get("resync_ready"),
        "local_gates_ok": plan.get("local_gates_ok"),
        "requires_explicit_exact_run_approval": proposed_run.get("requires_explicit_exact_run_approval"),
        "remote_worktree": proposed_run.get("remote_worktree"),
        "remote_run_dir": proposed_run.get("remote_run_dir"),
        "duration_seconds": proposed_run.get("duration_seconds"),
        "market_slug": proposed_run.get("market_slug"),
        "shared_ingress_root": proposed_run.get("shared_ingress_root"),
    }


def summarize_canary_readiness_plan_smoke(path: Path | None, value: dict) -> dict:
    plan_manifest = value.get("plan_manifest")
    plan_path = Path(plan_manifest) if isinstance(plan_manifest, str) else None
    plan = read_json(plan_path)
    readiness = plan.get("canary_readiness") or {}
    runtime = plan.get("runtime_static_findings") or {}
    shadow_evidence = plan.get("shadow_strategy_evidence") or {}
    shadow_trading_evidence = plan.get("shadow_trading_evidence") or {}
    pair_arb_backtest_evidence = plan.get("pair_arb_backtest_evidence") or {}
    pair_arb_oos_compare_evidence = plan.get("pair_arb_oos_compare_evidence") or {}
    pair_arb_walkforward_compare_evidence = plan.get("pair_arb_walkforward_compare_evidence") or {}
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "scope": value.get("scope"),
        "orders_sent": value.get("orders_sent"),
        "auth_network_started": value.get("auth_network_started"),
        "plan_manifest": plan_manifest,
        "plan_manifest_exists": bool(plan_path and plan_path.exists()),
        "plan_status": plan.get("status"),
        "read_only_acceptance_passed": plan.get("read_only_acceptance_passed"),
        "local_gates_ok": plan.get("local_gates_ok"),
        "source_truth_schema_ready": plan.get("source_truth_schema_ready"),
        "observer_safety_ready": plan.get("observer_safety_ready"),
        "order_plan_module_registered": runtime.get("order_plan_module_registered"),
        "order_plan_smoke_passed": runtime.get("order_plan_smoke_passed"),
        "execution_controller_module_registered": runtime.get("execution_controller_module_registered"),
        "execution_controller_smoke_passed": runtime.get("execution_controller_smoke_passed"),
        "oms_adapter_module_registered": runtime.get("oms_adapter_module_registered"),
        "oms_adapter_smoke_passed": runtime.get("oms_adapter_smoke_passed"),
        "runtime_wiring_smoke_passed": runtime.get("runtime_wiring_smoke_passed"),
        "source_truth_runtime_gate_smoke_passed": runtime.get("source_truth_runtime_gate_smoke_passed"),
        "source_truth_event_type_registered": runtime.get("source_truth_event_type_registered"),
        "executor_order_truth_feed_wired": runtime.get("executor_order_truth_feed_wired"),
        "user_ws_fill_truth_feed_wired": runtime.get("user_ws_fill_truth_feed_wired"),
        "coordinator_source_truth_channel_wired": runtime.get(
            "coordinator_source_truth_channel_wired"
        ),
        "main_runtime_source_truth_channel_wired": runtime.get(
            "main_runtime_source_truth_channel_wired"
        ),
        "source_truth_event_feed_tests_present": runtime.get(
            "source_truth_event_feed_tests_present"
        ),
        "wallet_runtime_producer_wired": runtime.get("wallet_runtime_producer_wired"),
        "redeem_runtime_producer_wired": runtime.get("redeem_runtime_producer_wired"),
        "cashflow_runtime_producer_wired": runtime.get("cashflow_runtime_producer_wired"),
        "wallet_redeem_cashflow_runtime_producers_wired": runtime.get(
            "wallet_redeem_cashflow_runtime_producers_wired"
        ),
        "redeem_truth_runtime_confirmation_ready": runtime.get(
            "redeem_truth_runtime_confirmation_ready"
        ),
        "strategy_returns_empty_quotes": runtime.get("strategy_returns_empty_quotes"),
        "observer_payload_quotes_forced_empty": runtime.get("observer_payload_quotes_forced_empty"),
        "observer_order_controller_implemented_false": runtime.get(
            "observer_order_controller_implemented_false"
        ),
        "canary_blocked_event_present": runtime.get("canary_blocked_event_present"),
        "canary_block_reason_not_implemented": runtime.get("canary_block_reason_not_implemented"),
        "canary_run_ready": readiness.get("canary_run_ready"),
        "ready_to_execute": readiness.get("ready_to_execute"),
        "canary_run_authorized": readiness.get("canary_run_authorized"),
        "effectful_executor_implemented": readiness.get("effectful_executor_implemented"),
        "execution_readiness_status": readiness.get("execution_readiness_status"),
        "shadow_strategy_evidence_kind": readiness.get("shadow_strategy_evidence_kind"),
        "shadow_strategy_evidence_basis": readiness.get("shadow_strategy_evidence_basis"),
        "shadow_strategy_evidence_no_order": readiness.get("shadow_strategy_evidence_no_order"),
        "shadow_strategy_evidence_live_fill_pnl_queue_position_proof": readiness.get(
            "shadow_strategy_evidence_live_fill_pnl_queue_position_proof"
        ),
        "shadow_trading_acceptance_smoke_ready": readiness.get(
            "shadow_trading_acceptance_smoke_ready"
        ),
        "shadow_trading_acceptance_ready": readiness.get("shadow_trading_acceptance_ready"),
        "shadow_trading_report_discovery_smoke_ready": readiness.get(
            "shadow_trading_report_discovery_smoke_ready"
        ),
        "shadow_trading_report_discovery_ready": readiness.get(
            "shadow_trading_report_discovery_ready"
        ),
        "shadow_trading_report_discovery_status": readiness.get(
            "shadow_trading_report_discovery_status"
        ),
        "shadow_trading_report_discovery_candidate_count": readiness.get(
            "shadow_trading_report_discovery_candidate_count"
        ),
        "shadow_trading_report_discovery_real_candidate_count": readiness.get(
            "shadow_trading_report_discovery_real_candidate_count"
        ),
        "shadow_trading_report_discovery_valid_real_candidate_count": readiness.get(
            "shadow_trading_report_discovery_valid_real_candidate_count"
        ),
        "shadow_trading_evidence_kind": readiness.get("shadow_trading_evidence_kind"),
        "shadow_trading_evidence_basis": readiness.get("shadow_trading_evidence_basis"),
        "shadow_trading_pair_pnl": readiness.get("shadow_trading_pair_pnl"),
        "shadow_trading_pair_actions": readiness.get("shadow_trading_pair_actions"),
        "shadow_trading_residual_qty": readiness.get("shadow_trading_residual_qty"),
        "shadow_trading_residual_cost": readiness.get("shadow_trading_residual_cost"),
        "pair_arb_backtest_status": readiness.get("pair_arb_backtest_status"),
        "pair_arb_backtest_run_count": readiness.get("pair_arb_backtest_run_count"),
        "pair_arb_backtest_nonzero_fill_run_count": readiness.get(
            "pair_arb_backtest_nonzero_fill_run_count"
        ),
        "pair_arb_backtest_positive_nonzero_run_count": readiness.get(
            "pair_arb_backtest_positive_nonzero_run_count"
        ),
        "pair_arb_backtest_best_nonzero_pnl": readiness.get(
            "pair_arb_backtest_best_nonzero_pnl"
        ),
        "pair_arb_backtest_best_nonzero_avg_pnl": readiness.get(
            "pair_arb_backtest_best_nonzero_avg_pnl"
        ),
        "pair_arb_backtest_best_nonzero_residual_loss_rate_pct": readiness.get(
            "pair_arb_backtest_best_nonzero_residual_loss_rate_pct"
        ),
        "pair_arb_oos_compare_status": readiness.get("pair_arb_oos_compare_status"),
        "pair_arb_oos_compare_matched_config_count": readiness.get(
            "pair_arb_oos_compare_matched_config_count"
        ),
        "pair_arb_oos_compare_both_positive_config_count": readiness.get(
            "pair_arb_oos_compare_both_positive_config_count"
        ),
        "pair_arb_oos_compare_qualified_both_positive_config_count": readiness.get(
            "pair_arb_oos_compare_qualified_both_positive_config_count"
        ),
        "pair_arb_oos_compare_validation_passed": readiness.get(
            "pair_arb_oos_compare_validation_passed"
        ),
        "pair_arb_oos_compare_numeric_validation_passed": readiness.get(
            "pair_arb_oos_compare_numeric_validation_passed"
        ),
        "pair_arb_oos_compare_requires_compliant_dataset": readiness.get(
            "pair_arb_oos_compare_requires_compliant_dataset"
        ),
        "pair_arb_walkforward_compare_status": readiness.get(
            "pair_arb_walkforward_compare_status"
        ),
        "pair_arb_walkforward_compare_split_count": readiness.get(
            "pair_arb_walkforward_compare_split_count"
        ),
        "pair_arb_walkforward_compare_matched_config_count": readiness.get(
            "pair_arb_walkforward_compare_matched_config_count"
        ),
        "pair_arb_walkforward_compare_all_positive_config_count": readiness.get(
            "pair_arb_walkforward_compare_all_positive_config_count"
        ),
        "pair_arb_walkforward_compare_qualified_all_positive_config_count": readiness.get(
            "pair_arb_walkforward_compare_qualified_all_positive_config_count"
        ),
        "pair_arb_walkforward_compare_validation_passed": readiness.get(
            "pair_arb_walkforward_compare_validation_passed"
        ),
        "pair_arb_walkforward_compare_numeric_validation_passed": readiness.get(
            "pair_arb_walkforward_compare_numeric_validation_passed"
        ),
        "pair_arb_walkforward_compare_requires_compliant_dataset": readiness.get(
            "pair_arb_walkforward_compare_requires_compliant_dataset"
        ),
        "requires_strategy_retrain_or_positive_backtest_config": readiness.get(
            "requires_strategy_retrain_or_positive_backtest_config"
        ),
        "requires_live_fill_pnl_queue_position_proof_before_scale": readiness.get(
            "requires_live_fill_pnl_queue_position_proof_before_scale"
        ),
        "shadow_strategy_evidence": shadow_evidence,
        "shadow_trading_evidence": shadow_trading_evidence,
        "pair_arb_backtest_evidence": pair_arb_backtest_evidence,
        "pair_arb_oos_compare_evidence": pair_arb_oos_compare_evidence,
        "pair_arb_walkforward_compare_evidence": pair_arb_walkforward_compare_evidence,
        "order_intent_preview_surface_ready": readiness.get("order_intent_preview_surface_ready"),
        "execution_controller_decision_ready": readiness.get("execution_controller_decision_ready"),
        "oms_adapter_surface_ready": readiness.get("oms_adapter_surface_ready"),
        "runtime_sender_wiring_ready": readiness.get("runtime_sender_wiring_ready"),
        "source_truth_runtime_gate_ready": readiness.get("source_truth_runtime_gate_ready"),
        "order_fill_source_truth_event_feeds_ready": readiness.get(
            "order_fill_source_truth_event_feeds_ready"
        ),
        "wallet_redeem_cashflow_event_feeds_ready": readiness.get(
            "wallet_redeem_cashflow_event_feeds_ready"
        ),
        "g2_canary_runbook_ready": readiness.get("g2_canary_runbook_ready"),
        "g2_canary_acceptance_smoke_ready": readiness.get("g2_canary_acceptance_smoke_ready"),
        "g2_canary_launch_plan_ready": readiness.get("g2_canary_launch_plan_ready"),
        "g2_canary_review_smoke_ready": readiness.get("g2_canary_review_smoke_ready"),
        "g2_canary_approval_envelope_smoke_ready": readiness.get(
            "g2_canary_approval_envelope_smoke_ready"
        ),
        "g2_canary_launcher_refusal_smoke_ready": readiness.get(
            "g2_canary_launcher_refusal_smoke_ready"
        ),
        "g2_canary_executor_contract_smoke_ready": readiness.get(
            "g2_canary_executor_contract_smoke_ready"
        ),
        "g2_canary_executor_smoke_ready": readiness.get("g2_canary_executor_smoke_ready"),
        "g2_canary_executor_dry_run_smoke_ready": readiness.get(
            "g2_canary_executor_dry_run_smoke_ready"
        ),
        "g2_canary_executor_payload_manifest_smoke_ready": readiness.get(
            "g2_canary_executor_payload_manifest_smoke_ready"
        ),
        "g2_canary_effectful_executor_review_smoke_ready": readiness.get(
            "g2_canary_effectful_executor_review_smoke_ready"
        ),
        "shadow_edge_samples_smoke_ready": readiness.get(
            "shadow_edge_samples_smoke_ready"
        ),
        "l1_dry_run_outcome_labels_smoke_ready": readiness.get(
            "l1_dry_run_outcome_labels_smoke_ready"
        ),
        "l1_dry_run_outcome_labels_ready": readiness.get("l1_dry_run_outcome_labels_ready"),
        "no_order_shadow_run_artifact_smoke_ready": readiness.get(
            "no_order_shadow_run_artifact_smoke_ready"
        ),
        "shadow_acceptance_input_discovery_smoke_ready": readiness.get(
            "shadow_acceptance_input_discovery_smoke_ready"
        ),
        "shadow_acceptance_input_ready": readiness.get("shadow_acceptance_input_ready"),
        "realized_outcome_labels_smoke_ready": readiness.get(
            "realized_outcome_labels_smoke_ready"
        ),
        "outcome_label_bridge_smoke_ready": readiness.get(
            "outcome_label_bridge_smoke_ready"
        ),
        "shadow_performance_evidence_smoke_ready": readiness.get(
            "shadow_performance_evidence_smoke_ready"
        ),
        "rust_shadow_strategy_acceptance_smoke_ready": readiness.get(
            "rust_shadow_strategy_acceptance_smoke_ready"
        ),
        "rust_shadow_strategy_acceptance_runner_smoke_ready": readiness.get(
            "rust_shadow_strategy_acceptance_runner_smoke_ready"
        ),
        "pre_canary_plumbing_ready": readiness.get("pre_canary_plumbing_ready"),
        "rust_shadow_strategy_acceptance_ready": readiness.get(
            "rust_shadow_strategy_acceptance_ready"
        ),
        "ready_for_explicit_g2_canary_approval": readiness.get(
            "ready_for_explicit_g2_canary_approval"
        ),
        "requires_explicit_canary_approval": readiness.get("requires_explicit_canary_approval"),
        "requires_reviewed_effectful_executor_implementation": readiness.get(
            "requires_reviewed_effectful_executor_implementation"
        ),
        "requires_rust_shadow_strategy_acceptance": readiness.get(
            "requires_rust_shadow_strategy_acceptance"
        ),
        "requires_shadow_trading_acceptance": readiness.get("requires_shadow_trading_acceptance"),
        "requires_true_shadow_trading_pnl_residual_report": readiness.get(
            "requires_true_shadow_trading_pnl_residual_report"
        ),
        "requires_strategy_retrain_or_positive_backtest_config": readiness.get(
            "requires_strategy_retrain_or_positive_backtest_config"
        ),
        "requires_strategy_performance_shadow_or_dry_run": readiness.get(
            "requires_strategy_performance_shadow_or_dry_run"
        ),
        "requires_order_intent_surface": readiness.get("requires_order_intent_surface"),
        "requires_execution_controller": readiness.get("requires_execution_controller"),
        "requires_execution_controller_wiring": readiness.get("requires_execution_controller_wiring"),
        "requires_oms_adapter": readiness.get("requires_oms_adapter"),
        "requires_runtime_execution_wiring": readiness.get("requires_runtime_execution_wiring"),
        "requires_no_command_without_approval_gate": readiness.get(
            "requires_no_command_without_approval_gate"
        ),
        "requires_source_truth_runtime_account_gate": readiness.get(
            "requires_source_truth_runtime_account_gate"
        ),
        "requires_source_truth_event_feeds": readiness.get("requires_source_truth_event_feeds"),
        "requires_exact_g2_canary_runbook": readiness.get("requires_exact_g2_canary_runbook"),
        "requires_canary_acceptance_smoke": readiness.get("requires_canary_acceptance_smoke"),
        "requires_g2_canary_launch_plan": readiness.get("requires_g2_canary_launch_plan"),
        "requires_g2_canary_review_smoke": readiness.get("requires_g2_canary_review_smoke"),
        "requires_g2_canary_approval_envelope": readiness.get(
            "requires_g2_canary_approval_envelope"
        ),
        "requires_g2_canary_launcher_refusal_gate": readiness.get(
            "requires_g2_canary_launcher_refusal_gate"
        ),
        "requires_g2_canary_executor_contract": readiness.get(
            "requires_g2_canary_executor_contract"
        ),
        "requires_g2_canary_executor_preflight": readiness.get(
            "requires_g2_canary_executor_preflight"
        ),
        "requires_g2_canary_executor_dry_run": readiness.get(
            "requires_g2_canary_executor_dry_run"
        ),
        "requires_g2_canary_executor_payload_manifest": readiness.get(
            "requires_g2_canary_executor_payload_manifest"
        ),
        "requires_g2_canary_effectful_executor_review_smoke": readiness.get(
            "requires_g2_canary_effectful_executor_review_smoke"
        ),
        "requires_shadow_edge_samples_smoke": readiness.get(
            "requires_shadow_edge_samples_smoke"
        ),
        "requires_l1_dry_run_outcome_labels_smoke": readiness.get(
            "requires_l1_dry_run_outcome_labels_smoke"
        ),
        "requires_l1_dry_run_outcome_labels": readiness.get(
            "requires_l1_dry_run_outcome_labels"
        ),
        "requires_no_order_shadow_run_artifact_smoke": readiness.get(
            "requires_no_order_shadow_run_artifact_smoke"
        ),
        "requires_shadow_acceptance_input_discovery_smoke": readiness.get(
            "requires_shadow_acceptance_input_discovery_smoke"
        ),
        "requires_real_shadow_acceptance_input": readiness.get(
            "requires_real_shadow_acceptance_input"
        ),
        "requires_realized_outcome_labels_smoke": readiness.get(
            "requires_realized_outcome_labels_smoke"
        ),
        "requires_outcome_label_bridge_smoke": readiness.get(
            "requires_outcome_label_bridge_smoke"
        ),
        "requires_shadow_performance_evidence_smoke": readiness.get(
            "requires_shadow_performance_evidence_smoke"
        ),
        "requires_shadow_trading_report_discovery_smoke": readiness.get(
            "requires_shadow_trading_report_discovery_smoke"
        ),
        "requires_real_shadow_trading_report": readiness.get(
            "requires_real_shadow_trading_report"
        ),
        "requires_shadow_trading_acceptance_smoke": readiness.get(
            "requires_shadow_trading_acceptance_smoke"
        ),
        "requires_rust_shadow_strategy_acceptance_smoke": readiness.get(
            "requires_rust_shadow_strategy_acceptance_smoke"
        ),
        "requires_rust_shadow_strategy_acceptance_runner_smoke": readiness.get(
            "requires_rust_shadow_strategy_acceptance_runner_smoke"
        ),
        "requires_order_fill_truth_from_own_orders": readiness.get(
            "requires_order_fill_truth_from_own_orders"
        ),
        "requires_redeem_cashflow_truth_from_own_activity": readiness.get(
            "requires_redeem_cashflow_truth_from_own_activity"
        ),
        "next_gate": plan.get("next_gate"),
    }


def summarize_ec2_entrypoint_sync(path: Path | None, value: dict) -> dict:
    side_effects = value.get("side_effects") or {}
    forbidden_side_effects_absent = bool(side_effects) and all(
        side_effects.get(field) is False
        for field in (
            "started_observer",
            "started_user_ws",
            "started_broker",
            "stopped_broker",
            "systemd_called",
            "service_control_called",
            "orders_sent",
            "cancels_sent",
            "redeems_sent",
            "modified_srv_pm_as_ofi",
            "modified_shared_ingress",
            "env_files_written",
        )
    )
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "remote_root": value.get("remote_root"),
        "remote_worktree": value.get("remote_worktree"),
        "observer_binary_exists": value.get("observer_binary_exists"),
        "observer_binary_size": value.get("observer_binary_size"),
        "observer_binary_sha256": value.get("observer_binary_sha256"),
        "broker_preflight_status": value.get("broker_preflight_status"),
        "entrypoint_manifest_status": value.get("entrypoint_manifest_status"),
        "entrypoint_expected_executable_count": value.get("entrypoint_expected_executable_count"),
        "entrypoint_missing_executable_files": value.get("entrypoint_missing_executable_files"),
        "local_forbidden_path_hits": value.get("local_forbidden_path_hits"),
        "synced_temp_file_removed": value.get("synced_temp_file_removed"),
        "created_isolated_worktree": side_effects.get("created_isolated_worktree"),
        "synced_entrypoint_files": side_effects.get("synced_entrypoint_files"),
        "built_observer_binary": side_effects.get("built_observer_binary"),
        "cargo_network_allowed_for_dependency_fetch": side_effects.get("cargo_network_allowed_for_dependency_fetch"),
        "forbidden_side_effects_absent": forbidden_side_effects_absent,
        "next_gate": value.get("next_gate"),
    }


def summarize_ec2_resync_rebuild(path: Path | None, value: dict) -> dict:
    side_effects = value.get("side_effects") or {}
    forbidden_side_effects_absent = bool(side_effects) and all(
        side_effects.get(field) is False
        for field in (
            "started_observer",
            "started_user_ws",
            "started_broker",
            "stopped_broker",
            "systemd_called",
            "service_control_called",
            "orders_sent",
            "cancels_sent",
            "redeems_sent",
            "modified_srv_pm_as_ofi",
            "modified_shared_ingress",
            "env_files_written",
        )
    )
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "scope": value.get("scope"),
        "remote_root": value.get("remote_root"),
        "remote_worktree": value.get("remote_worktree"),
        "synced_file_count": value.get("synced_file_count"),
        "observer_binary_sha256": value.get("observer_binary_sha256"),
        "broker_preflight_status": value.get("broker_preflight_status"),
        "entrypoint_manifest_status": value.get("entrypoint_manifest_status"),
        "created_isolated_worktree": side_effects.get("created_isolated_worktree"),
        "synced_entrypoint_files": side_effects.get("synced_entrypoint_files"),
        "built_observer_binary": side_effects.get("built_observer_binary"),
        "forbidden_side_effects_absent": forbidden_side_effects_absent,
    }


def summarize_ec2_readonly_observer_run(path: Path | None, value: dict) -> dict:
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "acceptance_passed": value.get("acceptance_passed"),
        "remote_driver_status_raw": value.get("remote_driver_status_raw"),
        "driver_status_corrected": value.get("driver_status_corrected"),
        "wrapper_return_code": value.get("wrapper_return_code"),
        "wrapper_summary_return_code": value.get("wrapper_summary_return_code"),
        "run_status": value.get("run_status"),
        "summary_status": value.get("summary_status"),
        "require_user_ws_records_gate": value.get("require_user_ws_records_gate"),
        "user_ws_raw_count": value.get("user_ws_raw_count"),
        "forbidden_event_count": value.get("forbidden_event_count"),
        "event_decode_error_count": value.get("event_decode_error_count"),
        "user_ws_decode_error_count": value.get("user_ws_decode_error_count"),
        "orders_possible": value.get("orders_possible"),
        "fills_seen_manifest": value.get("fills_seen_manifest"),
        "recorder_critical_drop_count": value.get("recorder_critical_drop_count"),
        "secret_values_recorded": value.get("secret_values_recorded"),
        "secret_values_written_to_disk": value.get("secret_values_written_to_disk"),
        "broker_started_or_modified": value.get("broker_started_or_modified"),
        "systemd_or_service_control_used": value.get("systemd_or_service_control_used"),
        "order_cancel_redeem_allowed": value.get("order_cancel_redeem_allowed"),
    }


def summarize_ec2_readonly_diagnostic(path: Path | None, value: dict) -> dict:
    side_effects = value.get("side_effects") or {}
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "acceptance_passed": value.get("acceptance_passed"),
        "wrapper_status": value.get("wrapper_status"),
        "wrapper_return_code": value.get("wrapper_return_code"),
        "wrapper_summary_return_code": value.get("wrapper_summary_return_code"),
        "run_status": value.get("run_status"),
        "summary_status": value.get("summary_status"),
        "gate_status": value.get("gate_status"),
        "gate_failures": value.get("gate_failures"),
        "user_ws_lifecycle_event_count": value.get("user_ws_lifecycle_event_count"),
        "user_ws_connected_count": value.get("user_ws_connected_count"),
        "user_ws_subscribe_sent_count": value.get("user_ws_subscribe_sent_count"),
        "user_ws_raw_count": value.get("user_ws_raw_count"),
        "user_ws_error_event_count": value.get("user_ws_error_event_count"),
        "forbidden_event_count": value.get("forbidden_event_count"),
        "event_decode_error_count": value.get("event_decode_error_count"),
        "user_ws_decode_error_count": value.get("user_ws_decode_error_count"),
        "orders_possible": value.get("orders_possible"),
        "fills_seen_manifest": value.get("fills_seen_manifest"),
        "recorder_critical_drop_count": value.get("recorder_critical_drop_count"),
        "secret_values_recorded": value.get("secret_values_recorded"),
        "secret_values_written_to_disk": value.get("secret_values_written_to_disk"),
        "broker_started_or_modified": value.get("broker_started_or_modified"),
        "systemd_or_service_control_used": value.get("systemd_or_service_control_used"),
        "order_cancel_redeem_allowed": value.get("order_cancel_redeem_allowed"),
        "started_observer": side_effects.get("started_observer"),
        "started_user_ws": side_effects.get("started_user_ws"),
        "started_broker": side_effects.get("started_broker"),
        "orders_sent": side_effects.get("orders_sent"),
        "cancels_sent": side_effects.get("cancels_sent"),
        "redeems_sent": side_effects.get("redeems_sent"),
        "modified_shared_ingress": side_effects.get("modified_shared_ingress"),
        "env_files_written": side_effects.get("env_files_written"),
    }


def summarize_ec2_readonly_acceptance(path: Path | None, value: dict) -> dict:
    side_effects = value.get("side_effects") or {}
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "acceptance_passed": value.get("acceptance_passed"),
        "wrapper_status": value.get("wrapper_status"),
        "wrapper_return_code": value.get("wrapper_return_code"),
        "wrapper_summary_return_code": value.get("wrapper_summary_return_code"),
        "run_status": value.get("run_status"),
        "summary_status": value.get("summary_status"),
        "gate_status": value.get("gate_status"),
        "gate_failures": value.get("gate_failures"),
        "duration_secs": value.get("duration_secs"),
        "elapsed_ms": value.get("elapsed_ms"),
        "shared_ingress_status": value.get("shared_ingress_status"),
        "broker_preflight_status": value.get("broker_preflight_status"),
        "user_ws_lifecycle_event_count": value.get("user_ws_lifecycle_event_count"),
        "user_ws_connected_count": value.get("user_ws_connected_count"),
        "user_ws_subscribe_sent_count": value.get("user_ws_subscribe_sent_count"),
        "user_ws_raw_count": value.get("user_ws_raw_count"),
        "user_ws_error_event_count": value.get("user_ws_error_event_count"),
        "forbidden_event_count": value.get("forbidden_event_count"),
        "event_decode_error_count": value.get("event_decode_error_count"),
        "user_ws_decode_error_count": value.get("user_ws_decode_error_count"),
        "orders_possible": value.get("orders_possible"),
        "fills_seen_manifest": value.get("fills_seen_manifest"),
        "recorder_critical_drop_count": value.get("recorder_critical_drop_count"),
        "secret_values_recorded": value.get("secret_values_recorded"),
        "secret_values_written_to_disk": value.get("secret_values_written_to_disk"),
        "broker_started_or_modified": value.get("broker_started_or_modified"),
        "systemd_or_service_control_used": value.get("systemd_or_service_control_used"),
        "order_cancel_redeem_allowed": value.get("order_cancel_redeem_allowed"),
        "ssh_transport_closed_initial_session": side_effects.get("ssh_transport_closed_initial_session"),
        "started_observer": side_effects.get("started_observer"),
        "started_user_ws": side_effects.get("started_user_ws"),
        "started_broker": side_effects.get("started_broker"),
        "orders_sent": side_effects.get("orders_sent"),
        "cancels_sent": side_effects.get("cancels_sent"),
        "redeems_sent": side_effects.get("redeems_sent"),
        "modified_shared_ingress": side_effects.get("modified_shared_ingress"),
        "env_files_written": side_effects.get("env_files_written"),
    }


def summarize_readonly_user_ws_postmortem(path: Path | None, value: dict) -> dict:
    summary_path = Path(value.get("summary_output")) if isinstance(value.get("summary_output"), str) else None
    summary = read_json(summary_path)
    return {
        "path": str(path) if path else None,
        "exists": bool(path and path.exists()),
        "artifact": value.get("artifact"),
        "status": value.get("status"),
        "scope": value.get("scope"),
        "source_run_dir": value.get("source_run_dir"),
        "summary_output": value.get("summary_output"),
        "summary_output_exists": bool(summary_path and summary_path.exists()),
        "summary_return_code": value.get("summary_return_code"),
        "summary_status": summary.get("status"),
        "gate_status": summary.get("gate_status"),
        "gate_failures": summary.get("gate_failures"),
        "user_ws_lifecycle_event_count": summary.get("user_ws_lifecycle_event_count"),
        "user_ws_connected_count": summary.get("user_ws_connected_count"),
        "user_ws_subscribe_sent_count": summary.get("user_ws_subscribe_sent_count"),
        "user_ws_error_event_count": summary.get("user_ws_error_event_count"),
        "user_ws_raw_count": summary.get("user_ws_raw_count"),
        "observer_started": value.get("observer_started"),
        "user_ws_started": value.get("user_ws_started"),
        "network_started": value.get("network_started"),
        "orders_sent": value.get("orders_sent"),
        "cancels_sent": value.get("cancels_sent"),
        "redeems_sent": value.get("redeems_sent"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_local_status_bundle_{label}"

    readiness_path = latest_manifest(root, "xuan_b27_dplus_auth_observer_readiness_*")
    static_path = latest_manifest(root, "xuan_b27_dplus_readonly_user_ws_static_smoke_*")
    readonly_summary_gate_path = latest_manifest(
        root,
        "xuan_b27_dplus_readonly_user_ws_summary_gate_smoke_*",
        artifact="xuan_b27_dplus_readonly_user_ws_summary_gate_smoke",
    )
    readonly_wrapper_refusal_path = latest_manifest(
        root,
        "xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke_*",
        artifact="xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke",
    )
    broker_path = latest_manifest(
        root,
        "xuan_b27_dplus_shared_ingress_preflight_*",
        artifact="xuan_b27_dplus_shared_ingress_preflight",
    )
    broker_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_shared_ingress_preflight_smoke_*",
        artifact="xuan_b27_dplus_shared_ingress_preflight_smoke",
    )
    source_truth_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_source_of_truth_schema_smoke_*",
        artifact="xuan_b27_dplus_source_of_truth_schema_smoke",
    )
    readonly_attempt_path = latest_json_file(
        root,
        "xuan_b27_dplus_user_ws_observer_*/wrapper_manifest.json",
        artifact="xuan_b27_dplus_readonly_user_ws_observer_wrapper",
    )
    auth_source_path = latest_manifest(root, "xuan_b27_dplus_auth_source_preflight_*")
    auth_source_smoke_path = latest_manifest(root, "xuan_b27_dplus_auth_source_preflight_smoke_*")
    safety_path = latest_manifest(root, "xuan_b27_dplus_observer_safety_smoke_*")
    summary_gate_path = latest_manifest(root, "xuan_b27_dplus_observer_summary_gate_smoke_*")
    remote_broker_probe_path = latest_manifest(
        root,
        "xuan_b27_dplus_remote_shared_ingress_probe_*",
        artifact="xuan_b27_dplus_remote_shared_ingress_probe",
    )
    ec2_readonly_attempt_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_attempt_*",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_attempt",
    )
    ec2_readonly_attempt_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_readonly_attempt_smoke_*",
        artifact="xuan_b27_dplus_ec2_readonly_attempt_smoke",
    )
    ec2_entrypoint_manifest_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_entrypoint_manifest_smoke_*",
        artifact="xuan_b27_dplus_ec2_entrypoint_manifest_smoke",
    )
    ec2_resync_diagnostic_plan_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_resync_diagnostic_plan_smoke_*",
        artifact="xuan_b27_dplus_ec2_resync_diagnostic_plan_smoke",
    )
    ec2_30m_acceptance_plan_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_30m_acceptance_plan_smoke_*",
        artifact="xuan_b27_dplus_ec2_30m_acceptance_plan_smoke",
    )
    canary_readiness_plan_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_canary_readiness_plan_smoke_*",
        artifact="xuan_b27_dplus_canary_readiness_plan_smoke",
    )
    compliant_backtest_input_preflight_path = latest_manifest(
        root,
        "xuan_b27_dplus_compliant_backtest_input_preflight_*",
        artifact="xuan_b27_dplus_compliant_backtest_input_preflight",
    )
    compliant_backtest_input_preflight_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_compliant_backtest_input_preflight_smoke_*",
        artifact="xuan_b27_dplus_compliant_backtest_input_preflight_smoke",
    )
    compliant_backtest_run_plan_path = latest_manifest(
        root,
        "xuan_b27_dplus_compliant_backtest_run_plan_*",
        artifact="xuan_b27_dplus_compliant_backtest_run_plan",
    )
    compliant_backtest_run_plan_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_compliant_backtest_run_plan_smoke_*",
        artifact="xuan_b27_dplus_compliant_backtest_run_plan_smoke",
    )
    backtest_report_scope_audit_path = latest_manifest(
        root,
        "xuan_b27_dplus_backtest_report_scope_audit_*",
        artifact="xuan_b27_dplus_backtest_report_scope_audit",
    )
    backtest_report_scope_audit_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_backtest_report_scope_audit_smoke_*",
        artifact="xuan_b27_dplus_backtest_report_scope_audit_smoke",
    )
    completion_store_schema_probe_path = latest_manifest(
        root,
        "xuan_b27_dplus_completion_store_schema_probe_*",
        artifact="xuan_b27_dplus_completion_store_schema_probe",
    )
    completion_store_schema_probe_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_completion_store_schema_probe_smoke_*",
        artifact="xuan_b27_dplus_completion_store_schema_probe_smoke",
    )
    scope_limited_passive_probe_summary_path = latest_manifest(
        root,
        "xuan_b27_dplus_scope_limited_completion_passive_probe_summary_*",
        artifact="xuan_b27_dplus_scope_limited_completion_passive_probe_summary",
    )
    scope_limited_passive_probe_summary_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_scope_limited_completion_passive_probe_summary_smoke_*",
        artifact="xuan_b27_dplus_scope_limited_completion_passive_probe_summary_smoke",
    )
    g2_canary_review_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_review_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_review_smoke",
    )
    g2_canary_approval_envelope_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_approval_envelope_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_approval_envelope_smoke",
    )
    g2_canary_launcher_refusal_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_launcher_refusal_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_launcher_refusal_smoke",
    )
    g2_canary_executor_contract_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_contract_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_contract_smoke",
    )
    g2_canary_executor_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_smoke",
    )
    g2_canary_executor_dry_run_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_dry_run_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_dry_run_smoke",
    )
    g2_canary_executor_payload_manifest_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke",
    )
    g2_canary_effectful_executor_review_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_effectful_executor_review_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_effectful_executor_review_smoke",
    )
    shadow_performance_evidence_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_performance_evidence_smoke_*",
        artifact="xuan_b27_dplus_shadow_performance_evidence_smoke",
    )
    shadow_trading_acceptance_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_trading_acceptance_smoke_*",
        artifact="xuan_b27_dplus_shadow_trading_acceptance_smoke",
    )
    shadow_trading_acceptance_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_trading_acceptance_*",
        artifact="xuan_b27_dplus_shadow_trading_acceptance",
    )
    shadow_edge_samples_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_edge_samples_smoke_*",
        artifact="xuan_b27_dplus_shadow_edge_samples_smoke",
    )
    l1_dry_run_outcome_labels_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_l1_dry_run_outcome_labels_smoke_*",
        artifact="xuan_b27_dplus_l1_dry_run_outcome_labels_smoke",
    )
    l1_dry_run_outcome_labels_path = latest_manifest(
        root,
        "xuan_b27_dplus_l1_dry_run_outcome_labels_*",
        artifact="xuan_b27_dplus_l1_dry_run_outcome_labels",
    )
    no_order_shadow_run_artifact_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_no_order_shadow_run_artifact_smoke_*",
        artifact="xuan_b27_dplus_no_order_shadow_run_artifact_smoke",
    )
    shadow_acceptance_input_discovery_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_acceptance_input_discovery_smoke_*",
        artifact="xuan_b27_dplus_shadow_acceptance_input_discovery_smoke",
    )
    shadow_acceptance_input_discovery_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_acceptance_input_discovery_*",
        artifact="xuan_b27_dplus_shadow_acceptance_input_discovery",
    )
    outcome_label_bridge_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_outcome_label_bridge_smoke_*",
        artifact="xuan_b27_dplus_outcome_label_bridge_smoke",
    )
    realized_outcome_labels_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_realized_outcome_labels_smoke_*",
        artifact="xuan_b27_dplus_realized_outcome_labels_smoke",
    )
    rust_shadow_strategy_acceptance_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_rust_shadow_strategy_acceptance_smoke_*",
        artifact="xuan_b27_dplus_rust_shadow_strategy_acceptance_smoke",
    )
    rust_shadow_strategy_acceptance_runner_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_rust_shadow_strategy_acceptance_runner_smoke_*",
        artifact="xuan_b27_dplus_rust_shadow_strategy_acceptance_runner_smoke",
    )
    rust_shadow_strategy_acceptance_path = latest_manifest(
        root,
        "xuan_b27_dplus_rust_shadow_strategy_acceptance_*",
        artifact="xuan_b27_dplus_rust_shadow_strategy_acceptance",
    )
    ec2_entrypoint_sync_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_entrypoint_sync_*",
        artifact="xuan_b27_dplus_ec2_entrypoint_sync_local_summary",
    )
    ec2_resync_rebuild_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_resync_rebuild_*",
        artifact="xuan_b27_dplus_ec2_resync_rebuild_local_summary",
    )
    ec2_readonly_observer_run_path = latest_json_file(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_observer_*/local_acceptance_review.json",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_local_acceptance_review",
    )
    ec2_readonly_diagnostic_path = latest_json_file(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_diagnostic_*/local_acceptance_review.json",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_diagnostic_local_acceptance_review",
    )
    ec2_readonly_diagnostic_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_readonly_diagnostic_smoke_*",
        artifact="xuan_b27_dplus_ec2_readonly_diagnostic_smoke",
    )
    ec2_readonly_acceptance_path = latest_json_file(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_acceptance_*/local_acceptance_review.json",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_acceptance_local_acceptance_review",
    )
    ec2_30m_acceptance_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_30m_acceptance_smoke_*",
        artifact="xuan_b27_dplus_ec2_30m_acceptance_smoke",
    )
    readonly_user_ws_postmortem_path = latest_manifest(
        root,
        "xuan_b27_dplus_readonly_user_ws_postmortem_*",
        artifact="xuan_b27_dplus_readonly_user_ws_postmortem",
    )

    readiness = read_json(readiness_path)
    static_smoke = read_json(static_path)
    readonly_summary_gate = read_json(readonly_summary_gate_path)
    readonly_wrapper_refusal = read_json(readonly_wrapper_refusal_path)
    broker = read_json(broker_path)
    broker_smoke = read_json(broker_smoke_path)
    source_truth_smoke = read_json(source_truth_smoke_path)
    readonly_attempt = read_json(readonly_attempt_path)
    auth_source = read_json(auth_source_path)
    auth_source_smoke = read_json(auth_source_smoke_path)
    safety = read_json(safety_path)
    summary_gate = read_json(summary_gate_path)
    remote_broker_probe = read_json(remote_broker_probe_path)
    ec2_readonly_attempt = read_json(ec2_readonly_attempt_path)
    ec2_readonly_attempt_smoke = read_json(ec2_readonly_attempt_smoke_path)
    ec2_entrypoint_manifest_smoke = read_json(ec2_entrypoint_manifest_smoke_path)
    ec2_resync_diagnostic_plan_smoke = read_json(ec2_resync_diagnostic_plan_smoke_path)
    ec2_30m_acceptance_plan_smoke = read_json(ec2_30m_acceptance_plan_smoke_path)
    canary_readiness_plan_smoke = read_json(canary_readiness_plan_smoke_path)
    compliant_backtest_input_preflight = read_json(compliant_backtest_input_preflight_path)
    compliant_backtest_input_preflight_smoke = read_json(
        compliant_backtest_input_preflight_smoke_path
    )
    compliant_backtest_run_plan = read_json(compliant_backtest_run_plan_path)
    compliant_backtest_run_plan_smoke = read_json(compliant_backtest_run_plan_smoke_path)
    backtest_report_scope_audit = read_json(backtest_report_scope_audit_path)
    backtest_report_scope_audit_smoke = read_json(backtest_report_scope_audit_smoke_path)
    completion_store_schema_probe = read_json(completion_store_schema_probe_path)
    completion_store_schema_probe_smoke = read_json(completion_store_schema_probe_smoke_path)
    scope_limited_passive_probe_summary = read_json(scope_limited_passive_probe_summary_path)
    scope_limited_passive_probe_summary_smoke = read_json(
        scope_limited_passive_probe_summary_smoke_path
    )
    g2_canary_review_smoke = read_json(g2_canary_review_smoke_path)
    g2_canary_approval_envelope_smoke = read_json(g2_canary_approval_envelope_smoke_path)
    g2_canary_launcher_refusal_smoke = read_json(g2_canary_launcher_refusal_smoke_path)
    g2_canary_executor_contract_smoke = read_json(g2_canary_executor_contract_smoke_path)
    g2_canary_executor_smoke = read_json(g2_canary_executor_smoke_path)
    g2_canary_executor_dry_run_smoke = read_json(g2_canary_executor_dry_run_smoke_path)
    g2_canary_executor_payload_manifest_smoke = read_json(g2_canary_executor_payload_manifest_smoke_path)
    g2_canary_effectful_executor_review_smoke = read_json(
        g2_canary_effectful_executor_review_smoke_path
    )
    shadow_performance_evidence_smoke = read_json(shadow_performance_evidence_smoke_path)
    shadow_trading_acceptance_smoke = read_json(shadow_trading_acceptance_smoke_path)
    shadow_trading_acceptance = read_json(shadow_trading_acceptance_path)
    shadow_edge_samples_smoke = read_json(shadow_edge_samples_smoke_path)
    l1_dry_run_outcome_labels_smoke = read_json(l1_dry_run_outcome_labels_smoke_path)
    l1_dry_run_outcome_labels = read_json(l1_dry_run_outcome_labels_path)
    no_order_shadow_run_artifact_smoke = read_json(no_order_shadow_run_artifact_smoke_path)
    shadow_acceptance_input_discovery_smoke = read_json(
        shadow_acceptance_input_discovery_smoke_path
    )
    shadow_acceptance_input_discovery = read_json(shadow_acceptance_input_discovery_path)
    outcome_label_bridge_smoke = read_json(outcome_label_bridge_smoke_path)
    realized_outcome_labels_smoke = read_json(realized_outcome_labels_smoke_path)
    rust_shadow_strategy_acceptance_smoke = read_json(rust_shadow_strategy_acceptance_smoke_path)
    rust_shadow_strategy_acceptance_runner_smoke = read_json(
        rust_shadow_strategy_acceptance_runner_smoke_path
    )
    rust_shadow_strategy_acceptance = read_json(rust_shadow_strategy_acceptance_path)
    ec2_entrypoint_sync = read_json(ec2_entrypoint_sync_path)
    ec2_resync_rebuild = read_json(ec2_resync_rebuild_path)
    ec2_readonly_observer_run = read_json(ec2_readonly_observer_run_path)
    ec2_readonly_diagnostic = read_json(ec2_readonly_diagnostic_path)
    ec2_readonly_diagnostic_smoke = read_json(ec2_readonly_diagnostic_smoke_path)
    ec2_readonly_acceptance = read_json(ec2_readonly_acceptance_path)
    ec2_30m_acceptance_smoke = read_json(ec2_30m_acceptance_smoke_path)
    readonly_user_ws_postmortem = read_json(readonly_user_ws_postmortem_path)
    automation = parse_simple_toml(AUTOMATION_PATH)

    readiness_ok = readiness.get("status") == "READY_FOR_APPROVAL"
    static_ok = static_smoke.get("status") == "PASS"
    broker_ok = broker.get("status") == "OK"
    auth_source_ok = auth_source.get("status") == "OK"
    heartbeat_ok = automation.get("status") == "ACTIVE" and automation.get("kind") == "heartbeat"
    ec2_attempt_status = ec2_readonly_attempt.get("status")
    ec2_observer_status = ec2_readonly_observer_run.get("status")
    ec2_diagnostic_status = ec2_readonly_diagnostic.get("status")
    ec2_acceptance_status = ec2_readonly_acceptance.get("status")
    ec2_entrypoint_sync_ok = ec2_entrypoint_sync.get("status") == "PASS_ENTRYPOINT_SYNC_READY"
    canary_readiness_plan_smoke_summary = summarize_canary_readiness_plan_smoke(
        canary_readiness_plan_smoke_path,
        canary_readiness_plan_smoke,
    )
    canary_readiness_status = canary_readiness_plan_smoke_summary.get("plan_status") or "NO_CANARY_READINESS_PLAN"
    canary_next_gate = (
        canary_readiness_plan_smoke_summary.get("next_gate")
        or "create local no-network canary readiness plan before any canary discussion"
    )
    if ec2_acceptance_status == "PASS_READONLY_USER_WS_ACCEPTANCE":
        ec2_readonly_user_ws_status = "PASS_READONLY_USER_WS_ACCEPTANCE"
        ec2_next_gate = "review 30m EC2 readonly acceptance artifacts before any canary discussion"
    elif ec2_acceptance_status in {
        "FAIL_READONLY_USER_WS_ACCEPTANCE",
        "FAIL_READONLY_USER_WS_OBSERVER",
        "FAIL_NO_USER_WS_CONNECTION",
        "FAIL_NO_USER_WS_RECORDS",
        "FAIL_READONLY_VIOLATION",
        "FAIL_RECORDER_DECODE_ERROR",
        "FAIL_SUMMARY_GATE",
        "FAIL_PROCESS_EXIT_NONZERO",
    }:
        ec2_readonly_user_ws_status = "BLOCKED_EC2_READONLY_USER_WS_OBSERVER"
        ec2_next_gate = "inspect EC2 readonly acceptance artifacts before any rerun"
    elif ec2_diagnostic_status == "PASS_READONLY_USER_WS_DIAGNOSTIC":
        ec2_readonly_user_ws_status = "PASS_READONLY_USER_WS_DIAGNOSTIC"
        ec2_next_gate = "review 300s EC2 readonly diagnostic artifacts before any 30m acceptance or canary discussion"
    elif ec2_diagnostic_status in {
        "FAIL_READONLY_USER_WS_DIAGNOSTIC",
        "FAIL_READONLY_USER_WS_OBSERVER",
        "FAIL_NO_USER_WS_CONNECTION",
        "FAIL_NO_USER_WS_RECORDS",
        "FAIL_READONLY_VIOLATION",
        "FAIL_RECORDER_DECODE_ERROR",
        "FAIL_SUMMARY_GATE",
        "FAIL_PROCESS_EXIT_NONZERO",
    }:
        ec2_readonly_user_ws_status = "BLOCKED_EC2_READONLY_USER_WS_OBSERVER"
        ec2_next_gate = "inspect EC2 readonly diagnostic artifacts before any rerun"
    elif ec2_observer_status == "PASS_READONLY_USER_WS_OBSERVER":
        ec2_readonly_user_ws_status = "PASS_READONLY_USER_WS_OBSERVER"
        ec2_next_gate = "review EC2 readonly User WS artifacts before any canary discussion"
    elif ec2_observer_status in {
        "FAIL_READONLY_USER_WS_OBSERVER",
        "FAIL_NO_USER_WS_RECORDS",
        "FAIL_READONLY_VIOLATION",
        "FAIL_RECORDER_DECODE_ERROR",
        "FAIL_SUMMARY_GATE",
        "FAIL_PROCESS_EXIT_NONZERO",
    }:
        ec2_readonly_user_ws_status = "BLOCKED_EC2_READONLY_USER_WS_OBSERVER"
        ec2_next_gate = "inspect EC2 readonly User WS observer artifacts before any rerun"
    elif ec2_attempt_status == "FAILED_CLOSED_REMOTE_OBSERVER_ENTRYPOINT_MISSING" and ec2_entrypoint_sync_ok:
        ec2_readonly_user_ws_status = "READY_FOR_EXPLICIT_EC2_READONLY_USER_WS_APPROVAL"
        ec2_next_gate = "explicit exact EC2 read-only User WS observer run approval using synced remote_worktree"
    elif ec2_attempt_status == "FAILED_CLOSED_REMOTE_OBSERVER_ENTRYPOINT_MISSING":
        ec2_readonly_user_ws_status = "WAITING_FOR_EC2_OBSERVER_ENTRYPOINT"
        ec2_next_gate = "provide existing EC2 observer worktree/binary or explicitly approve deployment/sync plan"
    elif ec2_attempt_status == "PASS_READONLY_USER_WS_OBSERVER":
        ec2_readonly_user_ws_status = "PASS_READONLY_USER_WS_OBSERVER"
        ec2_next_gate = "review EC2 readonly User WS artifacts before any canary discussion"
    elif ec2_attempt_status == "FAIL_READONLY_USER_WS_OBSERVER":
        ec2_readonly_user_ws_status = "BLOCKED_EC2_READONLY_USER_WS_OBSERVER"
        ec2_next_gate = "inspect EC2 readonly User WS failure artifacts"
    else:
        ec2_readonly_user_ws_status = "NO_EC2_READONLY_USER_WS_ATTEMPT"
        ec2_next_gate = "explicit EC2 run approval plus existing remote observer entrypoint"

    if not heartbeat_ok:
        verdict = "BLOCKED_HEARTBEAT_NOT_ACTIVE"
        next_gate = "restore xuan-research heartbeat persistence"
    elif not readiness_ok or not static_ok:
        verdict = "BLOCKED_LOCAL_READINESS"
        next_gate = "fix local readiness/static smoke before any observer run"
    elif not auth_source_ok:
        verdict = "BLOCKED_USER_WS_AUTH_SOURCE"
        next_gate = "provide complete POLYMARKET_API credentials or POLYMARKET_PRIVATE_KEY"
    elif not broker_ok:
        verdict = "READY_FOR_APPROVAL_WAITING_FOR_SHARED_INGRESS_BROKER"
        next_gate = "healthy shared-ingress broker plus explicit main-thread observer approval"
    else:
        verdict = "READY_FOR_EXPLICIT_READONLY_USER_WS_APPROVAL"
        next_gate = "explicit main-thread approval for bounded read-only User WS observer"

    bundle = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_local_status_bundle",
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_status_bundle",
        "verdict": verdict,
        "next_gate": next_gate,
        "ec2_readonly_user_ws_status": ec2_readonly_user_ws_status,
        "ec2_next_gate": ec2_next_gate,
        "canary_readiness_status": canary_readiness_status,
        "canary_next_gate": canary_next_gate,
        "canary_run_ready": canary_readiness_plan_smoke_summary.get("canary_run_ready"),
        "canary_ready_to_execute": canary_readiness_plan_smoke_summary.get("ready_to_execute"),
        "canary_effectful_executor_implemented": canary_readiness_plan_smoke_summary.get(
            "effectful_executor_implemented"
        ),
        "canary_execution_readiness_status": canary_readiness_plan_smoke_summary.get(
            "execution_readiness_status"
        ),
        "canary_shadow_strategy_evidence_kind": canary_readiness_plan_smoke_summary.get(
            "shadow_strategy_evidence_kind"
        ),
        "canary_shadow_strategy_evidence_basis": canary_readiness_plan_smoke_summary.get(
            "shadow_strategy_evidence_basis"
        ),
        "canary_shadow_strategy_evidence_no_order": canary_readiness_plan_smoke_summary.get(
            "shadow_strategy_evidence_no_order"
        ),
        "canary_shadow_strategy_evidence_live_fill_pnl_queue_position_proof": (
            canary_readiness_plan_smoke_summary.get(
                "shadow_strategy_evidence_live_fill_pnl_queue_position_proof"
            )
        ),
        "canary_shadow_trading_acceptance_ready": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_acceptance_ready"
        ),
        "canary_shadow_trading_report_discovery_status": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_report_discovery_status"
        ),
        "canary_shadow_trading_report_discovery_ready": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_report_discovery_ready"
        ),
        "canary_shadow_trading_report_discovery_real_candidate_count": (
            canary_readiness_plan_smoke_summary.get("shadow_trading_report_discovery_real_candidate_count")
        ),
        "canary_shadow_trading_report_discovery_valid_real_candidate_count": (
            canary_readiness_plan_smoke_summary.get(
                "shadow_trading_report_discovery_valid_real_candidate_count"
            )
        ),
        "canary_shadow_trading_evidence_kind": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_evidence_kind"
        ),
        "canary_shadow_trading_evidence_basis": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_evidence_basis"
        ),
        "canary_shadow_trading_pair_pnl": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_pair_pnl"
        ),
        "canary_shadow_trading_residual_qty": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_residual_qty"
        ),
        "canary_shadow_trading_residual_cost": canary_readiness_plan_smoke_summary.get(
            "shadow_trading_residual_cost"
        ),
        "canary_pair_arb_backtest_status": canary_readiness_plan_smoke_summary.get(
            "pair_arb_backtest_status"
        ),
        "canary_pair_arb_backtest_run_count": canary_readiness_plan_smoke_summary.get(
            "pair_arb_backtest_run_count"
        ),
        "canary_pair_arb_backtest_nonzero_fill_run_count": (
            canary_readiness_plan_smoke_summary.get("pair_arb_backtest_nonzero_fill_run_count")
        ),
        "canary_pair_arb_backtest_positive_nonzero_run_count": (
            canary_readiness_plan_smoke_summary.get("pair_arb_backtest_positive_nonzero_run_count")
        ),
        "canary_pair_arb_backtest_best_nonzero_pnl": canary_readiness_plan_smoke_summary.get(
            "pair_arb_backtest_best_nonzero_pnl"
        ),
        "canary_pair_arb_backtest_best_nonzero_avg_pnl": (
            canary_readiness_plan_smoke_summary.get("pair_arb_backtest_best_nonzero_avg_pnl")
        ),
        "canary_pair_arb_backtest_best_nonzero_residual_loss_rate_pct": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_backtest_best_nonzero_residual_loss_rate_pct"
            )
        ),
        "canary_pair_arb_oos_compare_status": canary_readiness_plan_smoke_summary.get(
            "pair_arb_oos_compare_status"
        ),
        "canary_pair_arb_oos_compare_matched_config_count": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_oos_compare_matched_config_count"
            )
        ),
        "canary_pair_arb_oos_compare_both_positive_config_count": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_oos_compare_both_positive_config_count"
            )
        ),
        "canary_pair_arb_oos_compare_qualified_both_positive_config_count": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_oos_compare_qualified_both_positive_config_count"
            )
        ),
        "canary_pair_arb_oos_compare_validation_passed": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_oos_compare_validation_passed"
            )
        ),
        "canary_pair_arb_oos_compare_numeric_validation_passed": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_oos_compare_numeric_validation_passed"
            )
        ),
        "canary_pair_arb_oos_compare_requires_compliant_dataset": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_oos_compare_requires_compliant_dataset"
            )
        ),
        "canary_pair_arb_walkforward_compare_status": (
            canary_readiness_plan_smoke_summary.get("pair_arb_walkforward_compare_status")
        ),
        "canary_pair_arb_walkforward_compare_split_count": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_walkforward_compare_split_count"
            )
        ),
        "canary_pair_arb_walkforward_compare_matched_config_count": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_walkforward_compare_matched_config_count"
            )
        ),
        "canary_pair_arb_walkforward_compare_all_positive_config_count": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_walkforward_compare_all_positive_config_count"
            )
        ),
        "canary_pair_arb_walkforward_compare_qualified_all_positive_config_count": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_walkforward_compare_qualified_all_positive_config_count"
            )
        ),
        "canary_pair_arb_walkforward_compare_validation_passed": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_walkforward_compare_validation_passed"
            )
        ),
        "canary_pair_arb_walkforward_compare_numeric_validation_passed": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_walkforward_compare_numeric_validation_passed"
            )
        ),
        "canary_pair_arb_walkforward_compare_requires_compliant_dataset": (
            canary_readiness_plan_smoke_summary.get(
                "pair_arb_walkforward_compare_requires_compliant_dataset"
            )
        ),
        "canary_requires_strategy_retrain_or_positive_backtest_config": (
            canary_readiness_plan_smoke_summary.get(
                "requires_strategy_retrain_or_positive_backtest_config"
            )
        ),
        "compliant_backtest_input_status": compliant_backtest_input_preflight.get("status"),
        "compliant_backtest_input_preflight_passed": compliant_backtest_input_preflight.get(
            "preflight_passed"
        ),
        "compliant_backtest_can_run_locally": compliant_backtest_input_preflight.get(
            "can_run_compliant_backtest_locally"
        ),
        "compliant_backtest_missing_roots": compliant_backtest_input_preflight.get(
            "missing_roots"
        ),
        "compliant_backtest_strict_ready_label_count": compliant_backtest_input_preflight.get(
            "strict_ready_label_count"
        ),
        "compliant_backtest_completion_ready_label_count": compliant_backtest_input_preflight.get(
            "completion_ready_label_count"
        ),
        "compliant_backtest_public_truth_ready": (
            (compliant_backtest_input_preflight.get("public_account_execution_truth_v1") or {}).get(
                "ready"
            )
        ),
        "scope_limited_research_completion_available": compliant_backtest_input_preflight.get(
            "scope_limited_research_completion_available"
        ),
        "scope_limited_research_completion_ready_label_count": compliant_backtest_input_preflight.get(
            "scope_limited_research_completion_ready_label_count"
        ),
        "scope_limited_research_completion_can_support_promotion": compliant_backtest_input_preflight.get(
            "scope_limited_research_completion_can_support_promotion"
        ),
        "scope_limited_research_completion_conclusion_scope": compliant_backtest_input_preflight.get(
            "scope_limited_research_completion_conclusion_scope"
        ),
        "compliant_backtest_run_plan_status": compliant_backtest_run_plan.get("status"),
        "compliant_backtest_ready_to_run": compliant_backtest_run_plan.get(
            "ready_to_run_compliant_backtest"
        ),
        "compliant_backtest_requires_store_adapter": compliant_backtest_run_plan.get(
            "requires_compliant_store_adapter"
        ),
        "compliant_backtest_store_adapter_ready": compliant_backtest_run_plan.get(
            "compliant_store_adapter_ready"
        ),
        "compliant_backtest_existing_runner_input_type": compliant_backtest_run_plan.get(
            "existing_runner_input_type"
        ),
        "compliant_backtest_required_dataset_type": compliant_backtest_run_plan.get(
            "required_dataset_type"
        ),
        "backtest_report_scope_audit_status": backtest_report_scope_audit.get("status"),
        "backtest_report_scope_audit_passed": backtest_report_scope_audit.get("audit_passed"),
        "backtest_report_scope_audit_manifest_count": backtest_report_scope_audit.get(
            "audited_manifest_count"
        ),
        "backtest_report_scope_limited_report_count": backtest_report_scope_audit.get(
            "scope_limited_report_count"
        ),
        "backtest_report_scope_promotion_supported_report_count": (
            backtest_report_scope_audit.get("promotion_supported_report_count")
        ),
        "backtest_report_scope_requires_compliant_dataset": (
            backtest_report_scope_audit.get(
                "requires_compliant_backtest_dataset_for_promotion"
            )
        ),
        "completion_store_schema_probe_status": completion_store_schema_probe.get("status"),
        "completion_store_schema_probe_passed": completion_store_schema_probe.get("probe_passed"),
        "completion_store_schema_probe_row_count": completion_store_schema_probe.get("row_count"),
        "completion_store_schema_probe_market_count_sum_by_day": completion_store_schema_probe.get(
            "market_count_sum_by_day"
        ),
        "completion_store_schema_probe_dataset_type": completion_store_schema_probe.get(
            "dataset_type"
        ),
        "completion_store_schema_probe_can_support_promotion": completion_store_schema_probe.get(
            "can_support_strategy_promotion"
        ),
        "completion_store_schema_probe_requires_compliant_dataset": completion_store_schema_probe.get(
            "requires_compliant_backtest_dataset_for_promotion"
        ),
        "scope_limited_passive_probe_status": scope_limited_passive_probe_summary.get("status"),
        "scope_limited_passive_probe_positive_net_pnl_run_count": (
            scope_limited_passive_probe_summary.get("positive_net_pnl_run_count")
        ),
        "scope_limited_passive_probe_positive_stress100_run_count": (
            scope_limited_passive_probe_summary.get("positive_stress100_run_count")
        ),
        "scope_limited_passive_probe_positive_worst_residual_run_count": (
            scope_limited_passive_probe_summary.get("positive_worst_residual_run_count")
        ),
        "scope_limited_passive_probe_positive_stress100_worst_run_count": (
            scope_limited_passive_probe_summary.get("positive_stress100_worst_run_count")
        ),
        "scope_limited_passive_probe_best_net_pnl": scope_limited_passive_probe_summary.get(
            "best_net_pnl"
        ),
        "scope_limited_passive_probe_best_stress100_actual_pnl": (
            scope_limited_passive_probe_summary.get("best_stress100_actual_pnl")
        ),
        "scope_limited_passive_probe_best_worst_residual_net_pnl": (
            scope_limited_passive_probe_summary.get("best_worst_residual_net_pnl")
        ),
        "scope_limited_passive_probe_best_stress100_worst_pnl": (
            scope_limited_passive_probe_summary.get("best_stress100_worst_pnl")
        ),
        "scope_limited_passive_probe_can_support_promotion": (
            scope_limited_passive_probe_summary.get("can_support_strategy_promotion")
        ),
        "canary_requires_live_fill_pnl_queue_position_proof_before_scale": (
            canary_readiness_plan_smoke_summary.get(
                "requires_live_fill_pnl_queue_position_proof_before_scale"
            )
        ),
        "canary_requires_reviewed_effectful_executor_implementation": (
            canary_readiness_plan_smoke_summary.get(
                "requires_reviewed_effectful_executor_implementation"
            )
        ),
        "orders_sent": False,
        "auth_network_started": False,
        "side_effects": {
            "started_observer": False,
            "started_broker": False,
            "stopped_broker": False,
            "connected_to_broker": False,
            "modified_shared_ingress": False,
        },
        "heartbeat": {
            "path": str(AUTOMATION_PATH),
            "exists": automation.get("exists"),
            "id": automation.get("id"),
            "kind": automation.get("kind"),
            "status": automation.get("status"),
            "rrule": automation.get("rrule"),
            "target_thread_id": automation.get("target_thread_id"),
        },
        "readiness": summarize_manifest(
            readiness_path,
            readiness,
            ("status", "next_gate", "latest_shared_ingress_preflight_manifest"),
        ),
        "readonly_user_ws_static_smoke": summarize_manifest(static_path, static_smoke, ("status",)),
        "readonly_user_ws_summary_gate_smoke": summarize_manifest(
            readonly_summary_gate_path,
            readonly_summary_gate,
            ("artifact", "status", "scope", "orders_sent", "auth_network_started"),
        ),
        "readonly_user_ws_wrapper_refusal_smoke": summarize_manifest(
            readonly_wrapper_refusal_path,
            readonly_wrapper_refusal,
            ("artifact", "status", "scope", "orders_sent", "auth_network_started"),
        ),
        "shared_ingress_preflight": summarize_manifest(
            broker_path,
            broker,
            ("artifact", "status", "reason", "root", "manifest_path", "heartbeat_age_ms"),
        ),
        "shared_ingress_preflight_smoke": summarize_manifest(
            broker_smoke_path,
            broker_smoke,
            ("artifact", "status", "scope", "orders_sent", "auth_network_started"),
        ),
        "remote_shared_ingress_probe": summarize_remote_shared_ingress_probe(
            remote_broker_probe_path,
            remote_broker_probe,
        ),
        "latest_ec2_readonly_user_ws_attempt": summarize_ec2_readonly_attempt(
            ec2_readonly_attempt_path,
            ec2_readonly_attempt,
        ),
        "ec2_readonly_attempt_smoke": summarize_manifest(
            ec2_readonly_attempt_smoke_path,
            ec2_readonly_attempt_smoke,
            ("artifact", "status", "scope", "orders_sent", "auth_network_started", "attempt_manifest"),
        ),
        "ec2_entrypoint_manifest_smoke": summarize_ec2_entrypoint_manifest_smoke(
            ec2_entrypoint_manifest_smoke_path,
            ec2_entrypoint_manifest_smoke,
        ),
        "ec2_resync_diagnostic_plan_smoke": summarize_ec2_resync_diagnostic_plan_smoke(
            ec2_resync_diagnostic_plan_smoke_path,
            ec2_resync_diagnostic_plan_smoke,
        ),
        "ec2_30m_acceptance_plan_smoke": summarize_ec2_30m_acceptance_plan_smoke(
            ec2_30m_acceptance_plan_smoke_path,
            ec2_30m_acceptance_plan_smoke,
        ),
        "canary_readiness_plan_smoke": canary_readiness_plan_smoke_summary,
        "compliant_backtest_input_preflight": summarize_manifest(
            compliant_backtest_input_preflight_path,
            compliant_backtest_input_preflight,
            (
                "artifact",
                "status",
                "scope",
                "preflight_passed",
                "can_run_compliant_backtest_locally",
                "missing_roots",
                "scope_limited_research_completion_available",
                "scope_limited_research_completion_ready_label_count",
                "scope_limited_research_completion_can_support_promotion",
                "strict_ready_label_count",
                "completion_ready_label_count",
                "raw_replay_scanned",
                "duckdb_tables_read",
                "orders_sent",
                "auth_network_started",
                "next_gate",
            ),
        ),
        "compliant_backtest_input_preflight_smoke": summarize_manifest(
            compliant_backtest_input_preflight_smoke_path,
            compliant_backtest_input_preflight_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "raw_replay_scanned",
            ),
        ),
        "compliant_backtest_run_plan": summarize_manifest(
            compliant_backtest_run_plan_path,
            compliant_backtest_run_plan,
            (
                "artifact",
                "status",
                "scope",
                "ready_to_run_compliant_backtest",
                "inputs_available",
                "input_preflight_status",
                "requires_compliant_store_adapter",
                "compliant_store_adapter_ready",
                "existing_runner_input_type",
                "required_dataset_type",
                "raw_replay_scanned",
                "duckdb_tables_read",
                "orders_sent",
                "auth_network_started",
                "next_gate",
            ),
        ),
        "compliant_backtest_run_plan_smoke": summarize_manifest(
            compliant_backtest_run_plan_smoke_path,
            compliant_backtest_run_plan_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "raw_replay_scanned",
            ),
        ),
        "backtest_report_scope_audit": summarize_manifest(
            backtest_report_scope_audit_path,
            backtest_report_scope_audit,
            (
                "artifact",
                "status",
                "scope",
                "audit_passed",
                "audited_manifest_count",
                "scope_limited_report_count",
                "promotion_supported_report_count",
                "requires_compliant_backtest_dataset_for_promotion",
                "raw_replay_scanned",
                "duckdb_tables_read",
                "orders_sent",
                "auth_network_started",
                "next_gate",
            ),
        ),
        "backtest_report_scope_audit_smoke": summarize_manifest(
            backtest_report_scope_audit_smoke_path,
            backtest_report_scope_audit_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "raw_replay_scanned",
            ),
        ),
        "completion_store_schema_probe": summarize_manifest(
            completion_store_schema_probe_path,
            completion_store_schema_probe,
            (
                "artifact",
                "status",
                "scope",
                "probe_passed",
                "data_root",
                "dataset_type",
                "labels",
                "row_count",
                "market_count_sum_by_day",
                "can_support_strategy_promotion",
                "requires_compliant_backtest_dataset_for_promotion",
                "raw_replay_scanned",
                "duckdb_tables_read",
                "orders_sent",
                "auth_network_started",
                "next_gate",
            ),
        ),
        "completion_store_schema_probe_smoke": summarize_manifest(
            completion_store_schema_probe_smoke_path,
            completion_store_schema_probe_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "raw_replay_scanned",
                "duckdb_tables_read",
            ),
        ),
        "scope_limited_passive_probe_summary": summarize_manifest(
            scope_limited_passive_probe_summary_path,
            scope_limited_passive_probe_summary,
            (
                "artifact",
                "status",
                "scope",
                "summary_passed",
                "dataset_type",
                "labels",
                "row_count",
                "run_count",
                "nonzero_seed_run_count",
                "positive_net_pnl_run_count",
                "positive_stress100_run_count",
                "best_net_pnl",
                "best_stress100_actual_pnl",
                "best_worst_residual_net_pnl",
                "can_support_strategy_promotion",
                "requires_compliant_backtest_dataset_for_promotion",
                "raw_replay_scanned",
                "orders_sent",
                "auth_network_started",
                "next_gate",
            ),
        ),
        "scope_limited_passive_probe_summary_smoke": summarize_manifest(
            scope_limited_passive_probe_summary_smoke_path,
            scope_limited_passive_probe_summary_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "raw_replay_scanned",
            ),
        ),
        "g2_canary_review_smoke": summarize_manifest(
            g2_canary_review_smoke_path,
            g2_canary_review_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "summarizer",
            ),
        ),
        "g2_canary_approval_envelope_smoke": summarize_manifest(
            g2_canary_approval_envelope_smoke_path,
            g2_canary_approval_envelope_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "verifier",
            ),
        ),
        "g2_canary_launcher_refusal_smoke": summarize_manifest(
            g2_canary_launcher_refusal_smoke_path,
            g2_canary_launcher_refusal_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "launcher",
            ),
        ),
        "g2_canary_executor_contract_smoke": summarize_manifest(
            g2_canary_executor_contract_smoke_path,
            g2_canary_executor_contract_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "contract_manifest",
            ),
        ),
        "g2_canary_executor_smoke": summarize_manifest(
            g2_canary_executor_smoke_path,
            g2_canary_executor_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "executor",
            ),
        ),
        "g2_canary_executor_dry_run_smoke": summarize_manifest(
            g2_canary_executor_dry_run_smoke_path,
            g2_canary_executor_dry_run_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "executor",
            ),
        ),
        "g2_canary_executor_payload_manifest_smoke": summarize_manifest(
            g2_canary_executor_payload_manifest_smoke_path,
            g2_canary_executor_payload_manifest_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "payload_manifest",
            ),
        ),
        "g2_canary_effectful_executor_review_smoke": summarize_manifest(
            g2_canary_effectful_executor_review_smoke_path,
            g2_canary_effectful_executor_review_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_canary",
                "reviewer",
                "real_effectful_executor_review_status",
                "real_effectful_executor_implemented",
                "real_review_passed",
                "fixture_pass_artifact_published",
            ),
        ),
        "shadow_performance_evidence_smoke": summarize_manifest(
            shadow_performance_evidence_smoke_path,
            shadow_performance_evidence_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "performance_script",
                "real_performance_evidence_published",
            ),
        ),
        "shadow_edge_samples_smoke": summarize_manifest(
            shadow_edge_samples_smoke_path,
            shadow_edge_samples_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "extractor",
                "edge_only_rejected_by_performance_gate",
            ),
        ),
        "l1_dry_run_outcome_labels_smoke": summarize_manifest(
            l1_dry_run_outcome_labels_smoke_path,
            l1_dry_run_outcome_labels_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_observer",
                "started_user_ws",
                "started_canary",
                "producer",
                "real_outcome_labels_published",
                "real_shadow_run_artifact_published",
                "acceptance_artifact_published",
            ),
        ),
        "l1_dry_run_outcome_labels": summarize_manifest(
            l1_dry_run_outcome_labels_path,
            l1_dry_run_outcome_labels,
            (
                "artifact",
                "status",
                "scope",
                "label_count",
                "touched_count",
                "touch_ratio",
                "mean_realized_edge_bps",
                "median_realized_edge_bps",
                "real_outcome_labels_published",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_canary",
                "failures",
                "next_gate",
            ),
        ),
        "no_order_shadow_run_artifact_smoke": summarize_manifest(
            no_order_shadow_run_artifact_smoke_path,
            no_order_shadow_run_artifact_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_canary",
                "producer",
                "real_shadow_run_artifact_published",
                "acceptance_artifact_published",
            ),
        ),
        "shadow_acceptance_input_discovery_smoke": summarize_manifest(
            shadow_acceptance_input_discovery_smoke_path,
            shadow_acceptance_input_discovery_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_observer",
                "started_user_ws",
                "started_canary",
                "discovery_script",
                "actual_discovery_status",
                "real_shadow_acceptance_input_published",
            ),
        ),
        "shadow_acceptance_input_discovery": summarize_manifest(
            shadow_acceptance_input_discovery_path,
            shadow_acceptance_input_discovery,
            (
                "artifact",
                "status",
                "scope",
                "allow_fixture_root",
                "real_candidate_count",
                "valid_real_candidate_count",
                "invalid_real_candidate_count",
                "fixture_or_smoke_candidate_count",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "next_gate",
            ),
        ),
        "shadow_trading_acceptance_smoke": summarize_manifest(
            shadow_trading_acceptance_smoke_path,
            shadow_trading_acceptance_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_canary",
                "acceptance_script",
                "real_shadow_trading_acceptance_published",
            ),
        ),
        "shadow_trading_acceptance": summarize_manifest(
            shadow_trading_acceptance_path,
            shadow_trading_acceptance,
            (
                "artifact",
                "status",
                "scope",
                "source_tool",
                "acceptance_passed",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_canary",
                "next_gate",
            ),
        ),
        "outcome_label_bridge_smoke": summarize_manifest(
            outcome_label_bridge_smoke_path,
            outcome_label_bridge_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "bridge",
                "real_outcome_bridge_published",
                "acceptance_artifact_published",
            ),
        ),
        "realized_outcome_labels_smoke": summarize_manifest(
            realized_outcome_labels_smoke_path,
            realized_outcome_labels_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "producer",
                "real_outcome_labels_published",
                "acceptance_artifact_published",
            ),
        ),
        "rust_shadow_strategy_acceptance_smoke": summarize_manifest(
            rust_shadow_strategy_acceptance_smoke_path,
            rust_shadow_strategy_acceptance_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "auth_network_started",
                "started_canary",
                "acceptance_script",
                "acceptance_artifact_published",
            ),
        ),
        "rust_shadow_strategy_acceptance_runner_smoke": summarize_manifest(
            rust_shadow_strategy_acceptance_runner_smoke_path,
            rust_shadow_strategy_acceptance_runner_smoke,
            (
                "artifact",
                "status",
                "scope",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_canary",
                "runner",
                "real_acceptance_artifact_published",
                "acceptance_artifact_published",
            ),
        ),
        "rust_shadow_strategy_acceptance": summarize_manifest(
            rust_shadow_strategy_acceptance_path,
            rust_shadow_strategy_acceptance,
            (
                "artifact",
                "status",
                "scope",
                "strategy_acceptance_passed",
                "orders_sent",
                "cancels_sent",
                "redeems_sent",
                "auth_network_started",
                "started_canary",
                "failures",
                "next_gate",
            ),
        ),
        "latest_ec2_entrypoint_sync": summarize_ec2_entrypoint_sync(
            ec2_entrypoint_sync_path,
            ec2_entrypoint_sync,
        ),
        "latest_ec2_resync_rebuild": summarize_ec2_resync_rebuild(
            ec2_resync_rebuild_path,
            ec2_resync_rebuild,
        ),
        "latest_ec2_readonly_user_ws_observer": summarize_ec2_readonly_observer_run(
            ec2_readonly_observer_run_path,
            ec2_readonly_observer_run,
        ),
        "latest_ec2_readonly_user_ws_diagnostic": summarize_ec2_readonly_diagnostic(
            ec2_readonly_diagnostic_path,
            ec2_readonly_diagnostic,
        ),
        "latest_ec2_readonly_user_ws_acceptance": summarize_ec2_readonly_acceptance(
            ec2_readonly_acceptance_path,
            ec2_readonly_acceptance,
        ),
        "ec2_readonly_diagnostic_smoke": summarize_manifest(
            ec2_readonly_diagnostic_smoke_path,
            ec2_readonly_diagnostic_smoke,
            ("artifact", "status", "scope", "orders_sent", "auth_network_started", "diagnostic_review"),
        ),
        "ec2_30m_acceptance_smoke": summarize_manifest(
            ec2_30m_acceptance_smoke_path,
            ec2_30m_acceptance_smoke,
            ("artifact", "status", "scope", "orders_sent", "auth_network_started", "acceptance_review"),
        ),
        "latest_readonly_user_ws_postmortem": summarize_readonly_user_ws_postmortem(
            readonly_user_ws_postmortem_path,
            readonly_user_ws_postmortem,
        ),
        "source_of_truth_schema_smoke": summarize_manifest(
            source_truth_smoke_path,
            source_truth_smoke,
            ("artifact", "status", "scope", "orders_sent", "auth_network_started"),
        ),
        "latest_readonly_user_ws_attempt": summarize_manifest(
            readonly_attempt_path,
            readonly_attempt,
            (
                "artifact",
                "status",
                "created_utc",
                "mode",
                "strategy",
                "orders_allowed",
                "cancels_allowed",
                "redeems_allowed",
                "shared_ingress_role",
                "shared_ingress_root",
                "broker_preflight_status",
                "broker_preflight_reason",
                "broker_preflight_return_code",
                "broker_preflight_manifest",
            ),
        ),
        "auth_source_preflight": summarize_manifest(
            auth_source_path,
            auth_source,
            (
                "status",
                "reason",
                "auth_source",
                "dotenv_exists",
                "dotenv_is_symlink",
                "dotenv_target",
                "api_credentials_present",
                "private_key_present",
                "secrets_redacted",
            ),
        ),
        "auth_source_preflight_smoke": summarize_manifest(
            auth_source_smoke_path,
            auth_source_smoke,
            ("status", "scope", "orders_sent", "auth_network_started"),
        ),
        "observer_safety_smoke": summarize_manifest(safety_path, safety, ("status", "orders_sent")),
        "observer_summary_gate_smoke": summarize_manifest(summary_gate_path, summary_gate, ("status", "orders_sent")),
    }
    out_path = out_dir / "manifest.json"
    write_json(out_path, bundle)
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
