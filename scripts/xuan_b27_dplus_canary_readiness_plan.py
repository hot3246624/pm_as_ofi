#!/usr/bin/env python3
"""Create a local, no-network canary readiness decision artifact.

This intentionally does not propose or start a canary. It converts the latest
30m read-only EC2 User WS acceptance into a machine-checkable next gate for
G2 canary preparation.
"""

from __future__ import annotations

import argparse
import glob
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


def latest_manifest_with_field(
    root: Path,
    pattern: str,
    artifact: str,
    field: str,
    value: Any,
) -> Path | None:
    matches = [
        Path(p)
        for p in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json"))
    ]
    filtered = []
    for path in matches:
        manifest = read_json(path)
        if manifest.get("artifact") == artifact and manifest.get(field) == value:
            filtered.append(path)
    if not filtered:
        return None
    return max(filtered, key=lambda p: p.stat().st_mtime_ns)


def latest_json_file(root: Path, pattern: str, artifact: str | None = None) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern))]
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


def file_contains(path: Path, text: str) -> bool:
    if not path.exists():
        return False
    return text in path.read_text(errors="ignore")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_canary_readiness_plan_{label}"

    readiness_path = latest_manifest(
        root,
        "xuan_b27_dplus_auth_observer_readiness_*",
        artifact="xuan_b27_dplus_auth_observer_readiness_check",
    )
    bundle_path = latest_manifest(
        root,
        "xuan_b27_dplus_local_status_bundle_*",
        artifact="xuan_b27_dplus_local_status_bundle",
    )
    acceptance_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_ec2_30m_acceptance_smoke_*",
        artifact="xuan_b27_dplus_ec2_30m_acceptance_smoke",
    )
    source_truth_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_source_of_truth_schema_smoke_*",
        artifact="xuan_b27_dplus_source_of_truth_schema_smoke",
    )
    observer_safety_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_observer_safety_smoke_*",
        artifact="xuan_b27_dplus_observer_safety_smoke",
    )
    order_plan_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_canary_order_plan_smoke_*",
        artifact="xuan_b27_dplus_canary_order_plan_smoke",
    )
    execution_controller_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_execution_controller_smoke_*",
        artifact="xuan_b27_dplus_execution_controller_smoke",
    )
    oms_adapter_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_oms_adapter_smoke_*",
        artifact="xuan_b27_dplus_oms_adapter_smoke",
    )
    runtime_wiring_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_runtime_wiring_smoke_*",
        artifact="xuan_b27_dplus_runtime_wiring_smoke",
    )
    source_truth_runtime_gate_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_source_truth_runtime_gate_smoke_*",
        artifact="xuan_b27_dplus_source_truth_runtime_gate_smoke",
    )
    g2_canary_runbook_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_runbook_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_runbook_smoke",
    )
    g2_canary_acceptance_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_acceptance_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_acceptance_smoke",
    )
    g2_canary_launch_plan_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_g2_canary_launch_plan_smoke_*",
        artifact="xuan_b27_dplus_g2_canary_launch_plan_smoke",
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
    shadow_trading_report_discovery_smoke_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_trading_report_discovery_smoke_*",
        artifact="xuan_b27_dplus_shadow_trading_report_discovery_smoke",
    )
    shadow_trading_report_discovery_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_trading_report_discovery_*",
        artifact="xuan_b27_dplus_shadow_trading_report_discovery",
    )
    shadow_trading_acceptance_path = latest_manifest(
        root,
        "xuan_b27_dplus_shadow_trading_acceptance_*",
        artifact="xuan_b27_dplus_shadow_trading_acceptance",
    )
    pair_arb_backtest_json_grid_path = latest_manifest_with_field(
        root,
        "xuan_b27_dplus_pair_arb_backtest_json_grid_*",
        "xuan_b27_dplus_pair_arb_backtest_json_grid",
        "validation_scope",
        "oos",
    ) or latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_backtest_json_grid_*",
        artifact="xuan_b27_dplus_pair_arb_backtest_json_grid",
    )
    pair_arb_oos_compare_path = latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_oos_compare_*",
        artifact="xuan_b27_dplus_pair_arb_oos_compare",
    )
    pair_arb_walkforward_compare_path = latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_walkforward_compare_*",
        artifact="xuan_b27_dplus_pair_arb_walkforward_compare",
    )
    pair_arb_backtest_salvage_probe_path = latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_backtest_salvage_probe_*",
        artifact="xuan_b27_dplus_pair_arb_backtest_salvage_probe",
    )
    pair_arb_backtest_grid_aggressive_probe_path = latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_backtest_grid_aggressive_probe_*",
        artifact="xuan_b27_dplus_pair_arb_backtest_grid_aggressive_probe",
    )
    pair_arb_backtest_grid_broad_path = latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_backtest_grid_broad_*",
        artifact="xuan_b27_dplus_pair_arb_backtest_grid_broad",
    )
    pair_arb_backtest_grid_path = latest_manifest(
        root,
        "xuan_b27_dplus_pair_arb_backtest_grid_*",
        artifact="xuan_b27_dplus_pair_arb_backtest_grid",
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
    acceptance_review_path = latest_json_file(
        root,
        "xuan_b27_dplus_ec2_readonly_user_ws_acceptance_*/local_acceptance_review.json",
        artifact="xuan_b27_dplus_ec2_readonly_user_ws_acceptance_local_acceptance_review",
    )

    readiness = read_json(readiness_path)
    bundle = read_json(bundle_path)
    acceptance_smoke = read_json(acceptance_smoke_path)
    source_truth_smoke = read_json(source_truth_smoke_path)
    observer_safety_smoke = read_json(observer_safety_smoke_path)
    order_plan_smoke = read_json(order_plan_smoke_path)
    execution_controller_smoke = read_json(execution_controller_smoke_path)
    oms_adapter_smoke = read_json(oms_adapter_smoke_path)
    runtime_wiring_smoke = read_json(runtime_wiring_smoke_path)
    source_truth_runtime_gate_smoke = read_json(source_truth_runtime_gate_smoke_path)
    g2_canary_runbook_smoke = read_json(g2_canary_runbook_smoke_path)
    g2_canary_acceptance_smoke = read_json(g2_canary_acceptance_smoke_path)
    g2_canary_launch_plan_smoke = read_json(g2_canary_launch_plan_smoke_path)
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
    shadow_trading_report_discovery_smoke = read_json(
        shadow_trading_report_discovery_smoke_path
    )
    shadow_trading_report_discovery = read_json(shadow_trading_report_discovery_path)
    shadow_trading_acceptance = read_json(shadow_trading_acceptance_path)
    pair_arb_backtest_salvage_probe = read_json(pair_arb_backtest_salvage_probe_path)
    pair_arb_backtest_grid_aggressive_probe = read_json(
        pair_arb_backtest_grid_aggressive_probe_path
    )
    pair_arb_backtest_grid_broad = read_json(pair_arb_backtest_grid_broad_path)
    pair_arb_backtest_grid = read_json(pair_arb_backtest_grid_path)
    pair_arb_backtest_json_grid = read_json(pair_arb_backtest_json_grid_path)
    pair_arb_oos_compare = read_json(pair_arb_oos_compare_path)
    pair_arb_walkforward_compare = read_json(pair_arb_walkforward_compare_path)
    pair_arb_backtest = (
        pair_arb_backtest_json_grid
        or pair_arb_backtest_salvage_probe
        or pair_arb_backtest_grid_aggressive_probe
        or pair_arb_backtest_grid_broad
        or pair_arb_backtest_grid
    )
    pair_arb_backtest_path = (
        pair_arb_backtest_json_grid_path
        or pair_arb_backtest_salvage_probe_path
        or pair_arb_backtest_grid_aggressive_probe_path
        or pair_arb_backtest_grid_broad_path
        or pair_arb_backtest_grid_path
    )
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
    acceptance_review = read_json(acceptance_review_path)
    acceptance_side_effects = acceptance_review.get("side_effects") or {}

    strategy_path = root / "src" / "polymarket" / "strategy" / "xuan_b27_dplus.rs"
    coordinator_path = root / "src" / "polymarket" / "coordinator_xuan_b27_dplus.rs"
    coordinator_core_path = root / "src" / "polymarket" / "coordinator.rs"
    coordinator_tests_path = root / "src" / "polymarket" / "coordinator_tests.rs"
    messages_path = root / "src" / "polymarket" / "messages.rs"
    executor_path = root / "src" / "polymarket" / "executor.rs"
    user_ws_path = root / "src" / "polymarket" / "user_ws.rs"
    claims_path = root / "src" / "polymarket" / "claims.rs"
    main_bin_path = root / "src" / "bin" / "polymarket_v2.rs"
    main_bin_text = main_bin_path.read_text(errors="ignore") if main_bin_path.exists() else ""
    canary_design_path = root / "docs" / "research" / "xuan" / "DPLUS_CANARY_DESIGN_ZH.md"

    read_only_acceptance_passed = (
        acceptance_review.get("status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
        and acceptance_review.get("acceptance_passed") is True
        and acceptance_review.get("duration_secs") == 1800
        and acceptance_review.get("user_ws_connected_count", 0) > 0
        and acceptance_review.get("user_ws_subscribe_sent_count", 0) > 0
        and acceptance_review.get("user_ws_raw_count", 0) > 0
        and acceptance_review.get("user_ws_error_event_count") == 0
        and acceptance_review.get("forbidden_event_count") == 0
        and acceptance_review.get("event_decode_error_count") == 0
        and acceptance_review.get("user_ws_decode_error_count") == 0
        and acceptance_review.get("orders_possible") is False
        and acceptance_review.get("recorder_critical_drop_count") == 0
        and acceptance_review.get("broker_started_or_modified") is False
        and acceptance_review.get("systemd_or_service_control_used") is False
        and acceptance_review.get("order_cancel_redeem_allowed") is False
        and acceptance_side_effects.get("orders_sent") is False
        and acceptance_side_effects.get("cancels_sent") is False
        and acceptance_side_effects.get("redeems_sent") is False
        and acceptance_side_effects.get("started_broker") is False
        and acceptance_side_effects.get("modified_shared_ingress") is False
        and acceptance_side_effects.get("env_files_written") is False
    )
    local_gates_ok = (
        readiness.get("status") == "READY_FOR_APPROVAL"
        and acceptance_smoke.get("status") == "PASS"
        and source_truth_smoke.get("status") == "PASS"
        and observer_safety_smoke.get("status") == "PASS"
        and bundle.get("ec2_readonly_user_ws_status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
    )

    wallet_runtime_producer_wired = (
        file_contains(executor_path, "emit_xuan_b27_dplus_wallet_truth")
        and file_contains(executor_path, "XuanB27DplusSourceTruthEvent::WalletSnapshot")
        and file_contains(main_bin_path, "emit_xuan_b27_dplus_wallet_snapshot")
    )
    redeem_runtime_producer_wired = (
        file_contains(main_bin_path, "emit_xuan_b27_dplus_redeem_result_flags")
        and file_contains(main_bin_path, "XuanB27DplusSourceTruthEvent::RedeemResult")
        and file_contains(main_bin_path, "run_round_claim_window")
        and file_contains(claims_path, "struct RedeemExecutionEvidence")
        and file_contains(claims_path, "relayer_tx_id")
        and file_contains(claims_path, "tx_hash")
    )
    cashflow_runtime_producer_wired = (
        file_contains(main_bin_path, "emit_xuan_b27_dplus_cashflow_snapshot")
        and file_contains(main_bin_path, "XuanB27DplusSourceTruthEvent::CashflowSnapshot")
        and file_contains(main_bin_path, "Recycler post-merge balance")
    )
    wallet_redeem_cashflow_runtime_producers_wired = (
        wallet_runtime_producer_wired
        and redeem_runtime_producer_wired
        and cashflow_runtime_producer_wired
    )
    redeem_truth_runtime_confirmation_ready = (
        redeem_runtime_producer_wired
        and file_contains(messages_path, "has_redeem_confirmation")
        and file_contains(claims_path, "has_confirmation")
        and file_contains(main_bin_path, "outcome.has_redeem_confirmation()")
        and file_contains(coordinator_tests_path, "source_truth_confirmed_safe_redeem_id_passes_without_tx_hash")
    )

    runtime_static_findings = {
        "order_plan_module_registered": file_contains(
            root / "src" / "polymarket" / "mod.rs",
            "pub mod xuan_b27_dplus_order_plan",
        ),
        "order_plan_smoke_passed": order_plan_smoke.get("status") == "PASS",
        "execution_controller_module_registered": file_contains(
            root / "src" / "polymarket" / "mod.rs",
            "pub mod xuan_b27_dplus_execution_controller",
        ),
        "execution_controller_smoke_passed": execution_controller_smoke.get("status") == "PASS",
        "oms_adapter_module_registered": file_contains(
            root / "src" / "polymarket" / "mod.rs",
            "pub mod xuan_b27_dplus_oms_adapter",
        ),
        "oms_adapter_smoke_passed": oms_adapter_smoke.get("status") == "PASS",
        "runtime_wiring_smoke_passed": runtime_wiring_smoke.get("status") == "PASS",
        "source_truth_runtime_gate_smoke_passed": source_truth_runtime_gate_smoke.get("status") == "PASS",
        "source_truth_event_type_registered": file_contains(
            messages_path,
            "enum XuanB27DplusSourceTruthEvent",
        ),
        "executor_order_truth_feed_wired": (
            file_contains(executor_path, "with_xuan_b27_dplus_source_truth_tx")
            and file_contains(executor_path, "emit_xuan_b27_dplus_order_truth")
            and file_contains(executor_path, "XuanB27DplusSourceTruthEvent::OrderAccepted")
        ),
        "user_ws_fill_truth_feed_wired": (
            file_contains(user_ws_path, "with_xuan_b27_dplus_source_truth_tx")
            and file_contains(user_ws_path, "emit_user_ws_fill_parsed")
            and file_contains(user_ws_path, "XuanB27DplusSourceTruthEvent::UserWsFillParsed")
        ),
        "coordinator_source_truth_channel_wired": (
            file_contains(coordinator_core_path, "with_xuan_b27_dplus_source_truth_rx")
            and file_contains(coordinator_core_path, "xuan_b27_dplus_source_truth_rx.recv")
            and file_contains(coordinator_path, "handle_xuan_b27_dplus_source_truth_event")
        ),
        "main_runtime_source_truth_channel_wired": (
            file_contains(main_bin_path, "mpsc::channel::<XuanB27DplusSourceTruthEvent>")
            and file_contains(main_bin_path, "with_xuan_b27_dplus_source_truth_rx")
            and file_contains(main_bin_path, "executor.with_xuan_b27_dplus_source_truth_tx")
            and file_contains(main_bin_path, "user_ws.with_xuan_b27_dplus_source_truth_tx")
        ),
        "source_truth_event_feed_tests_present": (
            file_contains(coordinator_tests_path, "source_truth_events_feed_runtime_gate")
            and file_contains(coordinator_tests_path, "source_truth_fill_missing_trade_id_stays_unknown")
            and file_contains(coordinator_tests_path, "source_truth_feed_ignored_for_non_dplus_strategy")
        ),
        "wallet_runtime_producer_wired": wallet_runtime_producer_wired,
        "redeem_runtime_producer_wired": redeem_runtime_producer_wired,
        "cashflow_runtime_producer_wired": cashflow_runtime_producer_wired,
        "wallet_redeem_cashflow_runtime_producers_wired": wallet_redeem_cashflow_runtime_producers_wired,
        "redeem_truth_runtime_confirmation_ready": redeem_truth_runtime_confirmation_ready,
        "strategy_returns_empty_quotes": file_contains(strategy_path, "StrategyQuotes::default()"),
        "observer_payload_quotes_forced_empty": file_contains(coordinator_path, '"quotes_forced_empty": true'),
        "observer_order_controller_implemented_false": file_contains(
            coordinator_path,
            '"order_controller_implemented": false',
        ),
        "canary_blocked_event_present": file_contains(
            coordinator_path,
            "xuan_b27_dplus_canary_blocked_not_implemented",
        ),
        "canary_block_reason_not_implemented": file_contains(
            coordinator_path,
            "rust_order_controller_not_implemented",
        ),
        "canary_mode_invariant_tests_present": file_contains(
            coordinator_tests_path,
            "xuan_b27_dplus_canary_invariants_force_safe_execution_flags",
        ),
    }
    order_intent_preview_surface_ready = (
        runtime_static_findings["order_plan_module_registered"]
        and runtime_static_findings["order_plan_smoke_passed"]
    )
    execution_controller_decision_ready = (
        runtime_static_findings["execution_controller_module_registered"]
        and runtime_static_findings["execution_controller_smoke_passed"]
    )
    oms_adapter_surface_ready = (
        runtime_static_findings["oms_adapter_module_registered"]
        and runtime_static_findings["oms_adapter_smoke_passed"]
    )
    runtime_sender_wiring_ready = runtime_static_findings["runtime_wiring_smoke_passed"]
    source_truth_runtime_gate_ready = runtime_static_findings["source_truth_runtime_gate_smoke_passed"]
    order_fill_source_truth_event_feeds_ready = (
        runtime_static_findings["source_truth_event_type_registered"]
        and runtime_static_findings["executor_order_truth_feed_wired"]
        and runtime_static_findings["user_ws_fill_truth_feed_wired"]
        and runtime_static_findings["coordinator_source_truth_channel_wired"]
        and runtime_static_findings["main_runtime_source_truth_channel_wired"]
        and runtime_static_findings["source_truth_event_feed_tests_present"]
    )
    wallet_redeem_cashflow_event_feeds_ready = (
        runtime_static_findings["wallet_redeem_cashflow_runtime_producers_wired"]
        and runtime_static_findings["redeem_truth_runtime_confirmation_ready"]
    )
    g2_canary_runbook_ready = (
        g2_canary_runbook_smoke.get("status") == "PASS"
        and g2_canary_runbook_smoke.get("scope") == "local_no_network_g2_canary_runbook_gate"
        and g2_canary_runbook_smoke.get("exact_approval_required") is True
        and g2_canary_runbook_smoke.get("orders_sent") is False
        and g2_canary_runbook_smoke.get("auth_network_started") is False
        and g2_canary_runbook_smoke.get("started_canary") is False
    )
    g2_canary_acceptance_smoke_ready = (
        g2_canary_acceptance_smoke.get("status") == "PASS"
        and g2_canary_acceptance_smoke.get("scope") == "local_no_network_g2_canary_acceptance_fixture_gate"
        and g2_canary_acceptance_smoke.get("exact_approval_required") is True
        and g2_canary_acceptance_smoke.get("orders_sent") is False
        and g2_canary_acceptance_smoke.get("auth_network_started") is False
        and g2_canary_acceptance_smoke.get("started_canary") is False
    )
    g2_canary_launch_plan_ready = (
        g2_canary_launch_plan_smoke.get("status") == "PASS"
        and g2_canary_launch_plan_smoke.get("scope") == "local_no_network_g2_canary_launch_plan_gate"
        and g2_canary_launch_plan_smoke.get("orders_sent") is False
        and g2_canary_launch_plan_smoke.get("auth_network_started") is False
        and g2_canary_launch_plan_smoke.get("started_canary") is False
    )
    g2_canary_review_smoke_ready = (
        g2_canary_review_smoke.get("status") == "PASS"
        and g2_canary_review_smoke.get("scope") == "local_no_network_g2_canary_review_gate"
        and g2_canary_review_smoke.get("orders_sent") is False
        and g2_canary_review_smoke.get("auth_network_started") is False
        and g2_canary_review_smoke.get("started_canary") is False
        and g2_canary_review_smoke.get("summarizer") == "scripts/xuan_b27_dplus_summarize_g2_canary_run.py"
    )
    g2_canary_approval_envelope_smoke_ready = (
        g2_canary_approval_envelope_smoke.get("status") == "PASS"
        and g2_canary_approval_envelope_smoke.get("scope") == "local_no_network_g2_canary_approval_envelope_gate"
        and g2_canary_approval_envelope_smoke.get("orders_sent") is False
        and g2_canary_approval_envelope_smoke.get("auth_network_started") is False
        and g2_canary_approval_envelope_smoke.get("started_canary") is False
        and g2_canary_approval_envelope_smoke.get("verifier")
        == "scripts/xuan_b27_dplus_g2_canary_approval_envelope.py"
    )
    g2_canary_launcher_refusal_smoke_ready = (
        g2_canary_launcher_refusal_smoke.get("status") == "PASS"
        and g2_canary_launcher_refusal_smoke.get("scope") == "local_no_network_g2_canary_launcher_refusal_gate"
        and g2_canary_launcher_refusal_smoke.get("orders_sent") is False
        and g2_canary_launcher_refusal_smoke.get("auth_network_started") is False
        and g2_canary_launcher_refusal_smoke.get("started_canary") is False
        and g2_canary_launcher_refusal_smoke.get("launcher")
        == "scripts/xuan_b27_dplus_g2_canary_launcher_refusal.py"
    )
    g2_canary_executor_contract_smoke_ready = (
        g2_canary_executor_contract_smoke.get("status") == "PASS"
        and g2_canary_executor_contract_smoke.get("scope") == "local_no_network_g2_canary_executor_contract_gate"
        and g2_canary_executor_contract_smoke.get("orders_sent") is False
        and g2_canary_executor_contract_smoke.get("auth_network_started") is False
        and g2_canary_executor_contract_smoke.get("started_canary") is False
        and isinstance(g2_canary_executor_contract_smoke.get("contract_manifest"), str)
    )
    g2_canary_executor_smoke_ready = (
        g2_canary_executor_smoke.get("status") == "PASS"
        and g2_canary_executor_smoke.get("scope") == "local_no_network_g2_canary_executor_preflight_gate"
        and g2_canary_executor_smoke.get("orders_sent") is False
        and g2_canary_executor_smoke.get("auth_network_started") is False
        and g2_canary_executor_smoke.get("started_canary") is False
        and g2_canary_executor_smoke.get("executor") == "scripts/xuan_b27_dplus_g2_canary_executor.py"
    )
    g2_canary_executor_dry_run_smoke_ready = (
        g2_canary_executor_dry_run_smoke.get("status") == "PASS"
        and g2_canary_executor_dry_run_smoke.get("scope") == "local_no_network_g2_canary_executor_dry_run_gate"
        and g2_canary_executor_dry_run_smoke.get("orders_sent") is False
        and g2_canary_executor_dry_run_smoke.get("auth_network_started") is False
        and g2_canary_executor_dry_run_smoke.get("started_canary") is False
        and g2_canary_executor_dry_run_smoke.get("executor")
        == "scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py"
    )
    g2_canary_executor_payload_manifest_smoke_ready = (
        g2_canary_executor_payload_manifest_smoke.get("status") == "PASS"
        and g2_canary_executor_payload_manifest_smoke.get("scope")
        == "local_no_network_g2_canary_executor_payload_manifest_gate"
        and g2_canary_executor_payload_manifest_smoke.get("orders_sent") is False
        and g2_canary_executor_payload_manifest_smoke.get("auth_network_started") is False
        and g2_canary_executor_payload_manifest_smoke.get("started_canary") is False
        and isinstance(g2_canary_executor_payload_manifest_smoke.get("payload_manifest"), str)
    )
    g2_canary_effectful_executor_review_smoke_ready = (
        g2_canary_effectful_executor_review_smoke.get("status") == "PASS"
        and g2_canary_effectful_executor_review_smoke.get("scope")
        == "local_no_network_g2_canary_effectful_executor_review_gate"
        and g2_canary_effectful_executor_review_smoke.get("orders_sent") is False
        and g2_canary_effectful_executor_review_smoke.get("cancels_sent") is False
        and g2_canary_effectful_executor_review_smoke.get("redeems_sent") is False
        and g2_canary_effectful_executor_review_smoke.get("auth_network_started") is False
        and g2_canary_effectful_executor_review_smoke.get("started_canary") is False
        and g2_canary_effectful_executor_review_smoke.get("reviewer")
        == "scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py"
        and g2_canary_effectful_executor_review_smoke.get("real_effectful_executor_review_status")
        == "BLOCKED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
        and g2_canary_effectful_executor_review_smoke.get("real_effectful_executor_implemented")
        is False
        and g2_canary_effectful_executor_review_smoke.get("real_review_passed") is False
        and g2_canary_effectful_executor_review_smoke.get("fixture_pass_artifact_published")
        is False
    )
    shadow_performance_evidence_smoke_ready = (
        shadow_performance_evidence_smoke.get("status") == "PASS"
        and shadow_performance_evidence_smoke.get("scope")
        == "local_no_network_shadow_performance_evidence_gate"
        and shadow_performance_evidence_smoke.get("orders_sent") is False
        and shadow_performance_evidence_smoke.get("auth_network_started") is False
        and shadow_performance_evidence_smoke.get("started_canary") is False
        and shadow_performance_evidence_smoke.get("performance_script")
        == "scripts/xuan_b27_dplus_shadow_performance_evidence.py"
        and shadow_performance_evidence_smoke.get("real_performance_evidence_published") is False
    )
    shadow_trading_acceptance_smoke_ready = (
        shadow_trading_acceptance_smoke.get("status") == "PASS"
        and shadow_trading_acceptance_smoke.get("scope")
        == "local_no_network_shadow_trading_acceptance_gate"
        and shadow_trading_acceptance_smoke.get("orders_sent") is False
        and shadow_trading_acceptance_smoke.get("cancels_sent") is False
        and shadow_trading_acceptance_smoke.get("redeems_sent") is False
        and shadow_trading_acceptance_smoke.get("auth_network_started") is False
        and shadow_trading_acceptance_smoke.get("started_canary") is False
        and shadow_trading_acceptance_smoke.get("acceptance_script")
        == "scripts/xuan_b27_dplus_shadow_trading_acceptance.py"
        and shadow_trading_acceptance_smoke.get("real_shadow_trading_acceptance_published")
        is False
    )
    shadow_trading_report_discovery_smoke_ready = (
        shadow_trading_report_discovery_smoke.get("status") == "PASS"
        and shadow_trading_report_discovery_smoke.get("scope")
        == "local_no_network_shadow_trading_report_discovery_gate"
        and shadow_trading_report_discovery_smoke.get("orders_sent") is False
        and shadow_trading_report_discovery_smoke.get("cancels_sent") is False
        and shadow_trading_report_discovery_smoke.get("redeems_sent") is False
        and shadow_trading_report_discovery_smoke.get("auth_network_started") is False
        and shadow_trading_report_discovery_smoke.get("started_observer") is False
        and shadow_trading_report_discovery_smoke.get("started_user_ws") is False
        and shadow_trading_report_discovery_smoke.get("started_canary") is False
        and shadow_trading_report_discovery_smoke.get("discovery_script")
        == "scripts/xuan_b27_dplus_shadow_trading_report_discovery.py"
        and shadow_trading_report_discovery_smoke.get("real_shadow_trading_report_published")
        is False
    )
    shadow_trading_report_discovery_ready = (
        shadow_trading_report_discovery.get("status") == "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
        and shadow_trading_report_discovery.get("strategy") == "xuan_b27_dplus"
        and shadow_trading_report_discovery.get("scope")
        == "local_no_network_shadow_trading_report_discovery"
        and shadow_trading_report_discovery.get("include_fixtures") is False
        and shadow_trading_report_discovery.get("valid_real_candidate_count", 0) > 0
        and shadow_trading_report_discovery.get("orders_sent") is False
        and shadow_trading_report_discovery.get("auth_network_started") is False
        and shadow_trading_report_discovery.get("started_canary") is False
        and (shadow_trading_report_discovery.get("side_effects") or {}).get("shadow_runner_started")
        is False
    )
    shadow_edge_samples_smoke_ready = (
        shadow_edge_samples_smoke.get("status") == "PASS"
        and shadow_edge_samples_smoke.get("scope")
        == "local_no_network_shadow_edge_sample_extraction_gate"
        and shadow_edge_samples_smoke.get("orders_sent") is False
        and shadow_edge_samples_smoke.get("auth_network_started") is False
        and shadow_edge_samples_smoke.get("started_canary") is False
        and shadow_edge_samples_smoke.get("extractor")
        == "scripts/xuan_b27_dplus_extract_shadow_edge_samples.py"
        and shadow_edge_samples_smoke.get("edge_only_rejected_by_performance_gate") is True
    )
    l1_dry_run_outcome_labels_smoke_ready = (
        l1_dry_run_outcome_labels_smoke.get("status") == "PASS"
        and l1_dry_run_outcome_labels_smoke.get("scope")
        == "local_no_network_l1_dry_run_outcome_label_gate"
        and l1_dry_run_outcome_labels_smoke.get("orders_sent") is False
        and l1_dry_run_outcome_labels_smoke.get("cancels_sent") is False
        and l1_dry_run_outcome_labels_smoke.get("redeems_sent") is False
        and l1_dry_run_outcome_labels_smoke.get("auth_network_started") is False
        and l1_dry_run_outcome_labels_smoke.get("started_observer") is False
        and l1_dry_run_outcome_labels_smoke.get("started_user_ws") is False
        and l1_dry_run_outcome_labels_smoke.get("started_canary") is False
        and l1_dry_run_outcome_labels_smoke.get("producer")
        == "scripts/xuan_b27_dplus_l1_dry_run_outcome_labels.py"
        and l1_dry_run_outcome_labels_smoke.get("real_outcome_labels_published") is False
        and l1_dry_run_outcome_labels_smoke.get("real_shadow_run_artifact_published") is False
        and l1_dry_run_outcome_labels_smoke.get("acceptance_artifact_published") is False
    )
    l1_dry_run_outcome_labels_ready = (
        l1_dry_run_outcome_labels.get("status") == "PASS_L1_DRY_RUN_OUTCOME_LABELS"
        and l1_dry_run_outcome_labels.get("strategy") == "xuan_b27_dplus"
        and l1_dry_run_outcome_labels.get("scope")
        == "local_no_network_l1_dry_run_outcome_label_producer"
        and l1_dry_run_outcome_labels.get("real_outcome_labels_published") is True
        and l1_dry_run_outcome_labels.get("orders_sent") is False
        and l1_dry_run_outcome_labels.get("cancels_sent") is False
        and l1_dry_run_outcome_labels.get("redeems_sent") is False
        and l1_dry_run_outcome_labels.get("auth_network_started") is False
        and l1_dry_run_outcome_labels.get("started_canary") is False
    )
    no_order_shadow_run_artifact_smoke_ready = (
        no_order_shadow_run_artifact_smoke.get("status") == "PASS"
        and no_order_shadow_run_artifact_smoke.get("scope")
        == "local_no_network_no_order_shadow_run_artifact_gate"
        and no_order_shadow_run_artifact_smoke.get("orders_sent") is False
        and no_order_shadow_run_artifact_smoke.get("cancels_sent") is False
        and no_order_shadow_run_artifact_smoke.get("redeems_sent") is False
        and no_order_shadow_run_artifact_smoke.get("auth_network_started") is False
        and no_order_shadow_run_artifact_smoke.get("started_canary") is False
        and no_order_shadow_run_artifact_smoke.get("producer")
        == "scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py"
        and no_order_shadow_run_artifact_smoke.get("real_shadow_run_artifact_published") is False
        and no_order_shadow_run_artifact_smoke.get("acceptance_artifact_published") is False
    )
    shadow_acceptance_input_discovery_smoke_ready = (
        shadow_acceptance_input_discovery_smoke.get("status") == "PASS"
        and shadow_acceptance_input_discovery_smoke.get("scope")
        == "local_no_network_shadow_acceptance_input_discovery_gate"
        and shadow_acceptance_input_discovery_smoke.get("orders_sent") is False
        and shadow_acceptance_input_discovery_smoke.get("cancels_sent") is False
        and shadow_acceptance_input_discovery_smoke.get("redeems_sent") is False
        and shadow_acceptance_input_discovery_smoke.get("auth_network_started") is False
        and shadow_acceptance_input_discovery_smoke.get("started_observer") is False
        and shadow_acceptance_input_discovery_smoke.get("started_user_ws") is False
        and shadow_acceptance_input_discovery_smoke.get("started_canary") is False
        and shadow_acceptance_input_discovery_smoke.get("discovery_script")
        == "scripts/xuan_b27_dplus_shadow_acceptance_input_discovery.py"
        and shadow_acceptance_input_discovery_smoke.get("real_shadow_acceptance_input_published")
        is False
    )
    shadow_acceptance_input_ready = (
        shadow_acceptance_input_discovery.get("status")
        == "READY_FOR_RUST_SHADOW_STRATEGY_ACCEPTANCE_RUNNER"
        and shadow_acceptance_input_discovery.get("valid_real_candidate_count", 0) > 0
        and shadow_acceptance_input_discovery.get("allow_fixture_root") is False
        and shadow_acceptance_input_discovery.get("orders_sent") is False
        and shadow_acceptance_input_discovery.get("auth_network_started") is False
        and shadow_acceptance_input_discovery.get("started_canary") is False
    )
    outcome_label_bridge_smoke_ready = (
        outcome_label_bridge_smoke.get("status") == "PASS"
        and outcome_label_bridge_smoke.get("scope")
        == "local_no_network_outcome_label_bridge_gate"
        and outcome_label_bridge_smoke.get("orders_sent") is False
        and outcome_label_bridge_smoke.get("auth_network_started") is False
        and outcome_label_bridge_smoke.get("started_canary") is False
        and outcome_label_bridge_smoke.get("bridge")
        == "scripts/xuan_b27_dplus_outcome_label_bridge.py"
        and outcome_label_bridge_smoke.get("real_outcome_bridge_published") is False
        and outcome_label_bridge_smoke.get("acceptance_artifact_published") is False
    )
    realized_outcome_labels_smoke_ready = (
        realized_outcome_labels_smoke.get("status") == "PASS"
        and realized_outcome_labels_smoke.get("scope")
        == "local_no_network_realized_outcome_label_producer_gate"
        and realized_outcome_labels_smoke.get("orders_sent") is False
        and realized_outcome_labels_smoke.get("auth_network_started") is False
        and realized_outcome_labels_smoke.get("started_canary") is False
        and realized_outcome_labels_smoke.get("producer")
        == "scripts/xuan_b27_dplus_realized_outcome_labels.py"
        and realized_outcome_labels_smoke.get("real_outcome_labels_published") is False
        and realized_outcome_labels_smoke.get("acceptance_artifact_published") is False
    )
    rust_shadow_strategy_acceptance_smoke_ready = (
        rust_shadow_strategy_acceptance_smoke.get("status") == "PASS"
        and rust_shadow_strategy_acceptance_smoke.get("scope")
        == "local_no_network_rust_shadow_strategy_acceptance_gate"
        and rust_shadow_strategy_acceptance_smoke.get("orders_sent") is False
        and rust_shadow_strategy_acceptance_smoke.get("auth_network_started") is False
        and rust_shadow_strategy_acceptance_smoke.get("started_canary") is False
        and rust_shadow_strategy_acceptance_smoke.get("acceptance_script")
        == "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py"
        and rust_shadow_strategy_acceptance_smoke.get("acceptance_artifact_published") is False
    )
    rust_shadow_strategy_acceptance_runner_smoke_ready = (
        rust_shadow_strategy_acceptance_runner_smoke.get("status") == "PASS"
        and rust_shadow_strategy_acceptance_runner_smoke.get("scope")
        == "local_no_network_rust_shadow_strategy_acceptance_runner_gate"
        and rust_shadow_strategy_acceptance_runner_smoke.get("orders_sent") is False
        and rust_shadow_strategy_acceptance_runner_smoke.get("cancels_sent") is False
        and rust_shadow_strategy_acceptance_runner_smoke.get("redeems_sent") is False
        and rust_shadow_strategy_acceptance_runner_smoke.get("auth_network_started") is False
        and rust_shadow_strategy_acceptance_runner_smoke.get("started_canary") is False
        and rust_shadow_strategy_acceptance_runner_smoke.get("runner")
        == "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py"
        and rust_shadow_strategy_acceptance_runner_smoke.get("real_acceptance_artifact_published")
        is False
        and rust_shadow_strategy_acceptance_runner_smoke.get("acceptance_artifact_published")
        is False
    )
    rust_shadow_strategy_acceptance_ready = (
        rust_shadow_strategy_acceptance.get("status") == "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE"
        and rust_shadow_strategy_acceptance.get("strategy") == "xuan_b27_dplus"
        and rust_shadow_strategy_acceptance.get("scope")
        == "rust_no_order_shadow_or_dry_run_strategy_acceptance"
        and rust_shadow_strategy_acceptance.get("orders_sent") is False
        and rust_shadow_strategy_acceptance.get("cancels_sent") is False
        and rust_shadow_strategy_acceptance.get("redeems_sent") is False
        and rust_shadow_strategy_acceptance.get("auth_network_started") is False
        and rust_shadow_strategy_acceptance.get("started_canary") is False
        and rust_shadow_strategy_acceptance.get("strategy_acceptance_passed") is True
    )
    shadow_trading_checks = shadow_trading_acceptance.get("checks") or {}
    shadow_trading_metrics = shadow_trading_acceptance.get("trading_metrics") or {}
    shadow_trading_acceptance_ready = (
        shadow_trading_acceptance.get("status") == "PASS_SHADOW_TRADING_ACCEPTANCE"
        and shadow_trading_acceptance.get("strategy") == "xuan_b27_dplus"
        and shadow_trading_acceptance.get("scope") == "local_no_network_shadow_trading_acceptance"
        and shadow_trading_acceptance.get("source_tool")
        == "tools/xuan_dplus_passive_passive_shadow_runner.py"
        and shadow_trading_acceptance.get("acceptance_passed") is True
        and shadow_trading_acceptance.get("orders_sent") is False
        and shadow_trading_acceptance.get("cancels_sent") is False
        and shadow_trading_acceptance.get("redeems_sent") is False
        and shadow_trading_acceptance.get("auth_network_started") is False
        and shadow_trading_acceptance.get("started_canary") is False
        and shadow_trading_checks.get("report_shape_ok") is True
        and shadow_trading_checks.get("no_order_safety_ok") is True
        and shadow_trading_checks.get("sample_size_ok") is True
        and shadow_trading_checks.get("pnl_metrics_ok") is True
        and shadow_trading_checks.get("residual_risk_ok") is True
        and (shadow_trading_metrics.get("pair_pnl") or 0) >= 0
        and (shadow_trading_metrics.get("pair_actions") or 0) > 0
    )
    current_canary_controller_not_wired = (
        order_intent_preview_surface_ready
        and not execution_controller_decision_ready
        and runtime_static_findings["strategy_returns_empty_quotes"]
        and runtime_static_findings["observer_payload_quotes_forced_empty"]
        and runtime_static_findings["observer_order_controller_implemented_false"]
        and runtime_static_findings["canary_blocked_event_present"]
        and runtime_static_findings["canary_block_reason_not_implemented"]
        and runtime_static_findings["canary_mode_invariant_tests_present"]
    )
    execution_adapter_missing = (
        order_intent_preview_surface_ready
        and execution_controller_decision_ready
        and not oms_adapter_surface_ready
        and runtime_static_findings["strategy_returns_empty_quotes"]
        and runtime_static_findings["observer_payload_quotes_forced_empty"]
        and runtime_static_findings["observer_order_controller_implemented_false"]
        and runtime_static_findings["canary_blocked_event_present"]
        and runtime_static_findings["canary_block_reason_not_implemented"]
        and runtime_static_findings["canary_mode_invariant_tests_present"]
    )
    runtime_wiring_missing = (
        order_intent_preview_surface_ready
        and execution_controller_decision_ready
        and oms_adapter_surface_ready
        and not runtime_sender_wiring_ready
        and runtime_static_findings["strategy_returns_empty_quotes"]
        and runtime_static_findings["observer_payload_quotes_forced_empty"]
        and runtime_static_findings["observer_order_controller_implemented_false"]
        and runtime_static_findings["canary_blocked_event_present"]
        and runtime_static_findings["canary_block_reason_not_implemented"]
        and runtime_static_findings["canary_mode_invariant_tests_present"]
    )
    source_truth_runtime_not_ready = (
        order_intent_preview_surface_ready
        and execution_controller_decision_ready
        and oms_adapter_surface_ready
        and runtime_sender_wiring_ready
        and not source_truth_runtime_gate_ready
        and runtime_static_findings["strategy_returns_empty_quotes"]
        and runtime_static_findings["observer_payload_quotes_forced_empty"]
        and runtime_static_findings["observer_order_controller_implemented_false"]
        and runtime_static_findings["canary_blocked_event_present"]
        and runtime_static_findings["canary_block_reason_not_implemented"]
        and runtime_static_findings["canary_mode_invariant_tests_present"]
    )
    source_truth_feeds_not_wired = (
        order_intent_preview_surface_ready
        and execution_controller_decision_ready
        and oms_adapter_surface_ready
        and runtime_sender_wiring_ready
        and source_truth_runtime_gate_ready
        and not (
            order_fill_source_truth_event_feeds_ready
            and wallet_redeem_cashflow_event_feeds_ready
        )
        and runtime_static_findings["strategy_returns_empty_quotes"]
        and runtime_static_findings["observer_payload_quotes_forced_empty"]
        and runtime_static_findings["observer_order_controller_implemented_false"]
        and runtime_static_findings["canary_blocked_event_present"]
        and runtime_static_findings["canary_block_reason_not_implemented"]
        and runtime_static_findings["canary_mode_invariant_tests_present"]
    )
    canary_runbook_not_ready = (
        order_intent_preview_surface_ready
        and execution_controller_decision_ready
        and oms_adapter_surface_ready
        and runtime_sender_wiring_ready
        and source_truth_runtime_gate_ready
        and order_fill_source_truth_event_feeds_ready
        and wallet_redeem_cashflow_event_feeds_ready
        and runtime_static_findings["strategy_returns_empty_quotes"]
        and runtime_static_findings["observer_payload_quotes_forced_empty"]
        and runtime_static_findings["observer_order_controller_implemented_false"]
        and runtime_static_findings["canary_blocked_event_present"]
        and runtime_static_findings["canary_block_reason_not_implemented"]
        and runtime_static_findings["canary_mode_invariant_tests_present"]
        and not (
            g2_canary_runbook_ready
            and g2_canary_acceptance_smoke_ready
            and g2_canary_launch_plan_ready
            and g2_canary_review_smoke_ready
            and g2_canary_approval_envelope_smoke_ready
            and g2_canary_launcher_refusal_smoke_ready
            and g2_canary_executor_contract_smoke_ready
            and g2_canary_executor_smoke_ready
            and g2_canary_executor_dry_run_smoke_ready
            and g2_canary_executor_payload_manifest_smoke_ready
            and g2_canary_effectful_executor_review_smoke_ready
            and shadow_edge_samples_smoke_ready
            and l1_dry_run_outcome_labels_smoke_ready
            and no_order_shadow_run_artifact_smoke_ready
            and shadow_acceptance_input_discovery_smoke_ready
            and realized_outcome_labels_smoke_ready
            and outcome_label_bridge_smoke_ready
            and shadow_performance_evidence_smoke_ready
            and shadow_trading_acceptance_smoke_ready
            and shadow_trading_report_discovery_smoke_ready
            and rust_shadow_strategy_acceptance_smoke_ready
            and rust_shadow_strategy_acceptance_runner_smoke_ready
        )
    )
    pre_canary_plumbing_ready = (
        order_intent_preview_surface_ready
        and execution_controller_decision_ready
        and oms_adapter_surface_ready
        and runtime_sender_wiring_ready
        and source_truth_runtime_gate_ready
        and order_fill_source_truth_event_feeds_ready
        and wallet_redeem_cashflow_event_feeds_ready
        and g2_canary_runbook_ready
        and g2_canary_acceptance_smoke_ready
        and g2_canary_launch_plan_ready
        and g2_canary_review_smoke_ready
        and g2_canary_approval_envelope_smoke_ready
        and g2_canary_launcher_refusal_smoke_ready
        and g2_canary_executor_contract_smoke_ready
        and g2_canary_executor_smoke_ready
        and g2_canary_executor_dry_run_smoke_ready
        and g2_canary_executor_payload_manifest_smoke_ready
        and g2_canary_effectful_executor_review_smoke_ready
        and shadow_edge_samples_smoke_ready
        and l1_dry_run_outcome_labels_smoke_ready
        and no_order_shadow_run_artifact_smoke_ready
        and shadow_acceptance_input_discovery_smoke_ready
        and realized_outcome_labels_smoke_ready
        and outcome_label_bridge_smoke_ready
        and shadow_performance_evidence_smoke_ready
        and shadow_trading_acceptance_smoke_ready
        and shadow_trading_report_discovery_smoke_ready
        and rust_shadow_strategy_acceptance_smoke_ready
        and rust_shadow_strategy_acceptance_runner_smoke_ready
        and runtime_static_findings["strategy_returns_empty_quotes"]
        and runtime_static_findings["observer_payload_quotes_forced_empty"]
        and runtime_static_findings["observer_order_controller_implemented_false"]
        and runtime_static_findings["canary_blocked_event_present"]
        and runtime_static_findings["canary_block_reason_not_implemented"]
        and runtime_static_findings["canary_mode_invariant_tests_present"]
    )
    shadow_dry_run_strategy_acceptance_missing = (
        pre_canary_plumbing_ready and not rust_shadow_strategy_acceptance_ready
    )
    shadow_trading_acceptance_missing = (
        pre_canary_plumbing_ready
        and rust_shadow_strategy_acceptance_ready
        and not shadow_trading_acceptance_ready
        and not str(shadow_trading_acceptance.get("status") or "").startswith("FAIL_")
    )
    shadow_trading_acceptance_failed = (
        pre_canary_plumbing_ready
        and rust_shadow_strategy_acceptance_ready
        and not shadow_trading_acceptance_ready
        and str(shadow_trading_acceptance.get("status") or "").startswith("FAIL_")
    )
    ready_for_explicit_g2_canary_approval = (
        pre_canary_plumbing_ready
        and rust_shadow_strategy_acceptance_ready
        and shadow_trading_acceptance_ready
    )

    if read_only_acceptance_passed and local_gates_ok and ready_for_explicit_g2_canary_approval:
        status = "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL"
    elif read_only_acceptance_passed and local_gates_ok and shadow_trading_acceptance_failed:
        status = "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
    elif read_only_acceptance_passed and local_gates_ok and shadow_trading_acceptance_missing:
        status = "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_MISSING"
    elif read_only_acceptance_passed and local_gates_ok and shadow_dry_run_strategy_acceptance_missing:
        status = "CANARY_NOT_READY_SHADOW_DRY_RUN_STRATEGY_ACCEPTANCE_MISSING"
    elif read_only_acceptance_passed and local_gates_ok and source_truth_feeds_not_wired:
        status = "CANARY_NOT_READY_SOURCE_TRUTH_FEEDS_NOT_WIRED"
    elif read_only_acceptance_passed and local_gates_ok and canary_runbook_not_ready:
        status = "CANARY_NOT_READY_G2_CANARY_RUNBOOK_NOT_READY"
    elif read_only_acceptance_passed and local_gates_ok and source_truth_runtime_not_ready:
        status = "CANARY_NOT_READY_SOURCE_TRUTH_RUNTIME_NOT_READY"
    elif read_only_acceptance_passed and local_gates_ok and runtime_wiring_missing:
        status = "CANARY_NOT_READY_RUNTIME_WIRING_NOT_IMPLEMENTED"
    elif read_only_acceptance_passed and local_gates_ok and execution_adapter_missing:
        status = "CANARY_NOT_READY_OMS_ADAPTER_NOT_IMPLEMENTED"
    elif read_only_acceptance_passed and local_gates_ok and current_canary_controller_not_wired:
        status = "CANARY_NOT_READY_CONTROLLER_NOT_WIRED"
    elif read_only_acceptance_passed and local_gates_ok:
        status = "CANARY_NOT_READY_ORDER_CONTROLLER_NOT_IMPLEMENTED"
    else:
        status = "BLOCKED_CANARY_READINESS_INPUTS"
    canary_run_ready = False
    shadow_strategy_evidence = {
        "kind": "NO_ORDER_LOCAL_L1_TOUCH_DRY_RUN",
        "basis": l1_dry_run_outcome_labels.get("performance_basis"),
        "accepted": rust_shadow_strategy_acceptance_ready,
        "strategy_acceptance_status": rust_shadow_strategy_acceptance.get("status"),
        "strategy_acceptance_scope": rust_shadow_strategy_acceptance.get("scope"),
        "l1_label_status": l1_dry_run_outcome_labels.get("status"),
        "l1_label_scope": l1_dry_run_outcome_labels.get("scope"),
        "lookahead_seconds": l1_dry_run_outcome_labels.get("lookahead_seconds"),
        "label_count": l1_dry_run_outcome_labels.get("label_count"),
        "touched_count": l1_dry_run_outcome_labels.get("touched_count"),
        "touch_ratio": l1_dry_run_outcome_labels.get("touch_ratio"),
        "mean_realized_edge_bps": l1_dry_run_outcome_labels.get("mean_realized_edge_bps"),
        "median_realized_edge_bps": l1_dry_run_outcome_labels.get("median_realized_edge_bps"),
        "no_order": True,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "live_fill_pnl_queue_position_proof": False,
        "proves": [
            "local no-order L1 touch outcome labels passed the configured quality gate",
            "Rust shadow/dry-run strategy acceptance consumed realized no-order labels",
        ],
        "does_not_prove": [
            "live fill quality",
            "real PnL",
            "queue position",
            "maker rebate or fee realized economics",
            "authenticated order lifecycle under exchange conditions",
        ],
    }
    shadow_trading_evidence = {
        "kind": "SIMULATED_SHADOW_TRADING_REPORT",
        "basis": "xuan_dplus_passive_passive_shadow_runner.py aggregate_report",
        "accepted": shadow_trading_acceptance_ready,
        "report_discovery_ready": shadow_trading_report_discovery_ready,
        "report_discovery_status": shadow_trading_report_discovery.get("status"),
        "report_discovery_candidate_count": shadow_trading_report_discovery.get("candidate_count"),
        "report_discovery_fixture_or_smoke_candidate_count": (
            shadow_trading_report_discovery.get("fixture_or_smoke_candidate_count")
        ),
        "report_discovery_real_candidate_count": shadow_trading_report_discovery.get(
            "real_candidate_count"
        ),
        "report_discovery_valid_real_candidate_count": shadow_trading_report_discovery.get(
            "valid_real_candidate_count"
        ),
        "report_discovery_exact_shadow_run_plan": shadow_trading_report_discovery.get(
            "exact_shadow_run_plan"
        ),
        "required_before_g2_canary": True,
        "acceptance_status": shadow_trading_acceptance.get("status"),
        "acceptance_scope": shadow_trading_acceptance.get("scope"),
        "source_tool": shadow_trading_acceptance.get("source_tool"),
        "aggregate_report": shadow_trading_acceptance.get("aggregate_report"),
        "runner_manifest": shadow_trading_acceptance.get("runner_manifest"),
        "trading_metrics": shadow_trading_metrics,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "proves": [
            "simulated passive/passive virtual order fills",
            "internal YES/NO pairing metrics",
            "pair PnL and residual inventory risk under the runner assumptions",
        ],
        "does_not_prove": [
            "live authenticated exchange fills",
            "actual exchange queue position",
            "real wallet/cashflow PnL",
            "maker fee/rebate settlement",
        ],
    }
    pair_arb_best_nonzero = pair_arb_backtest.get("best_nonzero_fill_run") or {}
    pair_arb_backtest_evidence = {
        "kind": pair_arb_backtest.get("artifact"),
        "status": pair_arb_backtest.get("status"),
        "scope": pair_arb_backtest.get("scope"),
        "db": pair_arb_backtest.get("db"),
        "run_count": pair_arb_backtest.get("run_count"),
        "nonzero_fill_run_count": pair_arb_backtest.get("nonzero_fill_run_count"),
        "positive_nonzero_run_count": pair_arb_backtest.get("positive_nonzero_run_count"),
        "validation_scope": pair_arb_backtest.get("validation_scope"),
        "requires_oos_validation": pair_arb_backtest.get("requires_oos_validation", True),
        "oos_validation_passed": pair_arb_backtest.get("oos_validation_passed", False),
        "best_nonzero_fill_run": pair_arb_best_nonzero,
        "best_nonzero_pnl": pair_arb_best_nonzero.get("pnl"),
        "best_nonzero_avg_pnl": pair_arb_best_nonzero.get("avg"),
        "best_nonzero_residual_loss_rate_pct": pair_arb_best_nonzero.get(
            "residual_loss_rate", pair_arb_best_nonzero.get("resid")
        ),
        "raw_replay_scanned": pair_arb_backtest.get("raw_replay_scanned", False),
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "requires_positive_nonzero_backtest_before_strategy_promotion": (
            (pair_arb_backtest.get("positive_nonzero_run_count") or 0) <= 0
            or pair_arb_backtest.get("oos_validation_passed") is not True
        ),
    }
    pair_arb_oos_compare_evidence = {
        "kind": pair_arb_oos_compare.get("artifact"),
        "status": pair_arb_oos_compare.get("status"),
        "scope": pair_arb_oos_compare.get("scope"),
        "first_run_count": pair_arb_oos_compare.get("first_run_count"),
        "second_run_count": pair_arb_oos_compare.get("second_run_count"),
        "matched_config_count": pair_arb_oos_compare.get("matched_config_count"),
        "first_positive_nonzero_run_count": pair_arb_oos_compare.get(
            "first_positive_nonzero_run_count"
        ),
        "second_positive_nonzero_run_count": pair_arb_oos_compare.get(
            "second_positive_nonzero_run_count"
        ),
        "both_positive_config_count": pair_arb_oos_compare.get(
            "both_positive_config_count"
        ),
        "qualified_both_positive_config_count": pair_arb_oos_compare.get(
            "qualified_both_positive_config_count"
        ),
        "qualification_thresholds": pair_arb_oos_compare.get(
            "qualification_thresholds"
        ),
        "best_both_positive_config": pair_arb_oos_compare.get(
            "best_both_positive_config"
        ),
        "best_qualified_both_positive_config": pair_arb_oos_compare.get(
            "best_qualified_both_positive_config"
        ),
        "numeric_oos_validation_passed": pair_arb_oos_compare.get(
            "numeric_oos_validation_passed", pair_arb_oos_compare.get("oos_validation_passed", False)
        ),
        "oos_validation_passed": pair_arb_oos_compare.get(
            "oos_validation_passed", False
        ),
        "requires_compliant_backtest_dataset_for_promotion": pair_arb_oos_compare.get(
            "requires_compliant_backtest_dataset_for_promotion", False
        ),
        "raw_replay_scanned": pair_arb_oos_compare.get("raw_replay_scanned", False),
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "requires_strategy_retrain_or_positive_oos_config": (
            pair_arb_oos_compare.get("oos_validation_passed") is not True
        ),
    }
    pair_arb_walkforward_has_evidence = (
        pair_arb_walkforward_compare.get("artifact")
        == "xuan_b27_dplus_pair_arb_walkforward_compare"
    )
    pair_arb_walkforward_compare_evidence = {
        "kind": pair_arb_walkforward_compare.get("artifact"),
        "status": pair_arb_walkforward_compare.get("status"),
        "scope": pair_arb_walkforward_compare.get("scope"),
        "split_count": pair_arb_walkforward_compare.get("split_count"),
        "matched_config_count": pair_arb_walkforward_compare.get("matched_config_count"),
        "all_positive_config_count": pair_arb_walkforward_compare.get(
            "all_positive_config_count"
        ),
        "qualified_all_positive_config_count": pair_arb_walkforward_compare.get(
            "qualified_all_positive_config_count"
        ),
        "qualification_thresholds": pair_arb_walkforward_compare.get(
            "qualification_thresholds"
        ),
        "best_all_positive_config": pair_arb_walkforward_compare.get(
            "best_all_positive_config"
        ),
        "best_qualified_all_positive_config": pair_arb_walkforward_compare.get(
            "best_qualified_all_positive_config"
        ),
        "numeric_walkforward_validation_passed": pair_arb_walkforward_compare.get(
            "numeric_walkforward_validation_passed",
            pair_arb_walkforward_compare.get("walkforward_validation_passed", False),
        ),
        "walkforward_validation_passed": pair_arb_walkforward_compare.get(
            "walkforward_validation_passed", False
        ),
        "requires_compliant_backtest_dataset_for_promotion": (
            pair_arb_walkforward_compare.get(
                "requires_compliant_backtest_dataset_for_promotion", False
            )
        ),
        "raw_replay_scanned": pair_arb_walkforward_compare.get("raw_replay_scanned", False),
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "requires_strategy_retrain_or_positive_walkforward_config": (
            pair_arb_walkforward_has_evidence
            and pair_arb_walkforward_compare.get("walkforward_validation_passed") is not True
        ),
    }
    positive_backtest_ready = (
        pair_arb_oos_compare_evidence["oos_validation_passed"] is True
        and (
            not pair_arb_walkforward_has_evidence
            or pair_arb_walkforward_compare_evidence["walkforward_validation_passed"] is True
        )
    )
    positive_backtest_numeric_but_dataset_limited = (
        pair_arb_oos_compare_evidence["numeric_oos_validation_passed"] is True
        and (
            not pair_arb_walkforward_has_evidence
            or pair_arb_walkforward_compare_evidence["numeric_walkforward_validation_passed"]
            is True
        )
        and not positive_backtest_ready
    )
    shadow_trading_failed_next_gate = (
        "shadow trading acceptance failed; OOS and walk-forward qualified residual-control backtest exists, "
        "run larger no-order shadow trading with the same residual-control logic before any "
        "G2 read-write canary; retrain/tune strategy if shadow fails"
        if positive_backtest_ready
        else
        "shadow trading acceptance failed; numeric OOS/walk-forward residual-control backtest is positive "
        "but the backtest dataset is scope-limited, so rerun on compliant declared strict/cache/completion "
        "data and/or pass larger no-order shadow trading acceptance before any G2 read-write canary"
        if positive_backtest_numeric_but_dataset_limited
        else "shadow trading acceptance failed; collect larger positive nonzero backtest/shadow "
        "evidence or retrain/tune strategy before any G2 read-write canary"
    )
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_canary_readiness_plan",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_canary_readiness_plan",
        "read_only_acceptance_passed": read_only_acceptance_passed,
        "local_gates_ok": local_gates_ok,
        "source_truth_schema_ready": source_truth_smoke.get("status") == "PASS",
        "observer_safety_ready": observer_safety_smoke.get("status") == "PASS",
        "runtime_static_findings": runtime_static_findings,
        "shadow_strategy_evidence": shadow_strategy_evidence,
        "shadow_trading_evidence": shadow_trading_evidence,
        "pair_arb_backtest_evidence": pair_arb_backtest_evidence,
        "pair_arb_oos_compare_evidence": pair_arb_oos_compare_evidence,
        "pair_arb_walkforward_compare_evidence": pair_arb_walkforward_compare_evidence,
        "canary_readiness": {
            "canary_run_ready": canary_run_ready,
            "ready_to_execute": False,
            "canary_run_authorized": False,
            "effectful_executor_implemented": False,
            "execution_readiness_status": "NOT_READY_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED",
            "order_intent_preview_surface_ready": order_intent_preview_surface_ready,
            "execution_controller_decision_ready": execution_controller_decision_ready,
            "oms_adapter_surface_ready": oms_adapter_surface_ready,
            "runtime_sender_wiring_ready": runtime_sender_wiring_ready,
            "source_truth_runtime_gate_ready": source_truth_runtime_gate_ready,
            "order_fill_source_truth_event_feeds_ready": order_fill_source_truth_event_feeds_ready,
            "wallet_redeem_cashflow_event_feeds_ready": wallet_redeem_cashflow_event_feeds_ready,
            "g2_canary_runbook_ready": g2_canary_runbook_ready,
            "g2_canary_acceptance_smoke_ready": g2_canary_acceptance_smoke_ready,
            "g2_canary_launch_plan_ready": g2_canary_launch_plan_ready,
            "g2_canary_review_smoke_ready": g2_canary_review_smoke_ready,
            "g2_canary_approval_envelope_smoke_ready": g2_canary_approval_envelope_smoke_ready,
            "g2_canary_launcher_refusal_smoke_ready": g2_canary_launcher_refusal_smoke_ready,
            "g2_canary_executor_contract_smoke_ready": g2_canary_executor_contract_smoke_ready,
            "g2_canary_executor_smoke_ready": g2_canary_executor_smoke_ready,
            "g2_canary_executor_dry_run_smoke_ready": g2_canary_executor_dry_run_smoke_ready,
            "g2_canary_executor_payload_manifest_smoke_ready": g2_canary_executor_payload_manifest_smoke_ready,
            "g2_canary_effectful_executor_review_smoke_ready": (
                g2_canary_effectful_executor_review_smoke_ready
            ),
            "shadow_edge_samples_smoke_ready": shadow_edge_samples_smoke_ready,
            "l1_dry_run_outcome_labels_smoke_ready": l1_dry_run_outcome_labels_smoke_ready,
            "l1_dry_run_outcome_labels_ready": l1_dry_run_outcome_labels_ready,
            "no_order_shadow_run_artifact_smoke_ready": no_order_shadow_run_artifact_smoke_ready,
            "shadow_acceptance_input_discovery_smoke_ready": (
                shadow_acceptance_input_discovery_smoke_ready
            ),
            "shadow_acceptance_input_ready": shadow_acceptance_input_ready,
            "realized_outcome_labels_smoke_ready": realized_outcome_labels_smoke_ready,
            "outcome_label_bridge_smoke_ready": outcome_label_bridge_smoke_ready,
            "shadow_performance_evidence_smoke_ready": shadow_performance_evidence_smoke_ready,
            "shadow_trading_report_discovery_smoke_ready": (
                shadow_trading_report_discovery_smoke_ready
            ),
            "shadow_trading_report_discovery_ready": shadow_trading_report_discovery_ready,
            "shadow_trading_report_discovery_status": shadow_trading_report_discovery.get("status"),
            "shadow_trading_report_discovery_candidate_count": (
                shadow_trading_report_discovery.get("candidate_count")
            ),
            "shadow_trading_report_discovery_real_candidate_count": (
                shadow_trading_report_discovery.get("real_candidate_count")
            ),
            "shadow_trading_report_discovery_valid_real_candidate_count": (
                shadow_trading_report_discovery.get("valid_real_candidate_count")
            ),
            "rust_shadow_strategy_acceptance_smoke_ready": rust_shadow_strategy_acceptance_smoke_ready,
            "rust_shadow_strategy_acceptance_runner_smoke_ready": (
                rust_shadow_strategy_acceptance_runner_smoke_ready
            ),
            "pre_canary_plumbing_ready": pre_canary_plumbing_ready,
            "rust_shadow_strategy_acceptance_ready": rust_shadow_strategy_acceptance_ready,
            "shadow_strategy_evidence_kind": shadow_strategy_evidence["kind"],
            "shadow_strategy_evidence_basis": shadow_strategy_evidence["basis"],
            "shadow_strategy_evidence_no_order": shadow_strategy_evidence["no_order"],
            "shadow_strategy_evidence_live_fill_pnl_queue_position_proof": (
                shadow_strategy_evidence["live_fill_pnl_queue_position_proof"]
            ),
            "shadow_trading_acceptance_smoke_ready": shadow_trading_acceptance_smoke_ready,
            "shadow_trading_acceptance_ready": shadow_trading_acceptance_ready,
            "shadow_trading_evidence_kind": shadow_trading_evidence["kind"],
            "shadow_trading_evidence_basis": shadow_trading_evidence["basis"],
            "shadow_trading_pair_pnl": shadow_trading_metrics.get("pair_pnl"),
            "shadow_trading_pair_actions": shadow_trading_metrics.get("pair_actions"),
            "shadow_trading_residual_qty": shadow_trading_metrics.get("residual_qty"),
            "shadow_trading_residual_cost": shadow_trading_metrics.get("residual_cost"),
            "pair_arb_backtest_status": pair_arb_backtest_evidence["status"],
            "pair_arb_backtest_run_count": pair_arb_backtest_evidence["run_count"],
            "pair_arb_backtest_nonzero_fill_run_count": pair_arb_backtest_evidence[
                "nonzero_fill_run_count"
            ],
            "pair_arb_backtest_positive_nonzero_run_count": pair_arb_backtest_evidence[
                "positive_nonzero_run_count"
            ],
            "pair_arb_backtest_best_nonzero_pnl": pair_arb_backtest_evidence[
                "best_nonzero_pnl"
            ],
            "pair_arb_backtest_best_nonzero_avg_pnl": pair_arb_backtest_evidence[
                "best_nonzero_avg_pnl"
            ],
            "pair_arb_backtest_best_nonzero_residual_loss_rate_pct": (
                pair_arb_backtest_evidence["best_nonzero_residual_loss_rate_pct"]
            ),
            "pair_arb_oos_compare_status": pair_arb_oos_compare_evidence["status"],
            "pair_arb_oos_compare_matched_config_count": (
                pair_arb_oos_compare_evidence["matched_config_count"]
            ),
            "pair_arb_oos_compare_both_positive_config_count": (
                pair_arb_oos_compare_evidence["both_positive_config_count"]
            ),
            "pair_arb_oos_compare_qualified_both_positive_config_count": (
                pair_arb_oos_compare_evidence["qualified_both_positive_config_count"]
            ),
            "pair_arb_oos_compare_validation_passed": (
                pair_arb_oos_compare_evidence["oos_validation_passed"]
            ),
            "pair_arb_oos_compare_numeric_validation_passed": (
                pair_arb_oos_compare_evidence["numeric_oos_validation_passed"]
            ),
            "pair_arb_oos_compare_requires_compliant_dataset": (
                pair_arb_oos_compare_evidence[
                    "requires_compliant_backtest_dataset_for_promotion"
                ]
            ),
            "pair_arb_walkforward_compare_status": (
                pair_arb_walkforward_compare_evidence["status"]
            ),
            "pair_arb_walkforward_compare_split_count": (
                pair_arb_walkforward_compare_evidence["split_count"]
            ),
            "pair_arb_walkforward_compare_matched_config_count": (
                pair_arb_walkforward_compare_evidence["matched_config_count"]
            ),
            "pair_arb_walkforward_compare_all_positive_config_count": (
                pair_arb_walkforward_compare_evidence["all_positive_config_count"]
            ),
            "pair_arb_walkforward_compare_qualified_all_positive_config_count": (
                pair_arb_walkforward_compare_evidence[
                    "qualified_all_positive_config_count"
                ]
            ),
            "pair_arb_walkforward_compare_validation_passed": (
                pair_arb_walkforward_compare_evidence["walkforward_validation_passed"]
            ),
            "pair_arb_walkforward_compare_numeric_validation_passed": (
                pair_arb_walkforward_compare_evidence[
                    "numeric_walkforward_validation_passed"
                ]
            ),
            "pair_arb_walkforward_compare_requires_compliant_dataset": (
                pair_arb_walkforward_compare_evidence[
                    "requires_compliant_backtest_dataset_for_promotion"
                ]
            ),
            "requires_live_fill_pnl_queue_position_proof_before_scale": True,
            "ready_for_explicit_g2_canary_approval": status == "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL",
            "requires_explicit_canary_approval": True,
            "requires_reviewed_effectful_executor_implementation": True,
            "requires_rust_shadow_strategy_acceptance": not rust_shadow_strategy_acceptance_ready,
            "requires_shadow_trading_acceptance": not shadow_trading_acceptance_ready,
            "requires_true_shadow_trading_pnl_residual_report": not shadow_trading_acceptance_ready,
            "requires_strategy_retrain_or_positive_backtest_config": (
                pair_arb_backtest_evidence[
                    "requires_positive_nonzero_backtest_before_strategy_promotion"
                ]
                or pair_arb_oos_compare_evidence["oos_validation_passed"] is not True
                or pair_arb_walkforward_compare_evidence[
                    "requires_strategy_retrain_or_positive_walkforward_config"
                ]
            ),
            "requires_strategy_performance_shadow_or_dry_run": not (
                rust_shadow_strategy_acceptance_ready and shadow_trading_acceptance_ready
            ),
            "requires_order_intent_surface": True,
            "requires_execution_controller": True,
            "requires_execution_controller_wiring": True,
            "requires_oms_adapter": not oms_adapter_surface_ready,
            "requires_runtime_execution_wiring": not runtime_sender_wiring_ready,
            "requires_no_command_without_approval_gate": not runtime_sender_wiring_ready,
            "requires_source_truth_runtime_account_gate": not source_truth_runtime_gate_ready,
            "requires_source_truth_event_feeds": not (
                order_fill_source_truth_event_feeds_ready
                and wallet_redeem_cashflow_event_feeds_ready
            ),
            "requires_post_only_order_enforcement": True,
            "requires_stop_on_unknown": True,
            "requires_prefund_risk_envelope": True,
            "requires_kill_switch_runbook": True,
            "requires_order_fill_truth_from_own_orders": True,
            "requires_redeem_cashflow_truth_from_own_activity": True,
            "requires_canary_acceptance_smoke": not g2_canary_acceptance_smoke_ready,
            "requires_exact_g2_canary_runbook": not g2_canary_runbook_ready,
            "requires_g2_canary_launch_plan": not g2_canary_launch_plan_ready,
            "requires_g2_canary_review_smoke": not g2_canary_review_smoke_ready,
            "requires_g2_canary_approval_envelope": not g2_canary_approval_envelope_smoke_ready,
            "requires_g2_canary_launcher_refusal_gate": not g2_canary_launcher_refusal_smoke_ready,
            "requires_g2_canary_executor_contract": not g2_canary_executor_contract_smoke_ready,
            "requires_g2_canary_executor_preflight": not g2_canary_executor_smoke_ready,
            "requires_g2_canary_executor_dry_run": not g2_canary_executor_dry_run_smoke_ready,
            "requires_g2_canary_executor_payload_manifest": not g2_canary_executor_payload_manifest_smoke_ready,
            "requires_g2_canary_effectful_executor_review_smoke": (
                not g2_canary_effectful_executor_review_smoke_ready
            ),
            "requires_shadow_edge_samples_smoke": not shadow_edge_samples_smoke_ready,
            "requires_l1_dry_run_outcome_labels_smoke": not l1_dry_run_outcome_labels_smoke_ready,
            "requires_l1_dry_run_outcome_labels": not l1_dry_run_outcome_labels_ready,
            "requires_no_order_shadow_run_artifact_smoke": not no_order_shadow_run_artifact_smoke_ready,
            "requires_shadow_acceptance_input_discovery_smoke": (
                not shadow_acceptance_input_discovery_smoke_ready
            ),
            "requires_real_shadow_acceptance_input": not shadow_acceptance_input_ready,
            "requires_realized_outcome_labels_smoke": not realized_outcome_labels_smoke_ready,
            "requires_outcome_label_bridge_smoke": not outcome_label_bridge_smoke_ready,
            "requires_shadow_performance_evidence_smoke": not shadow_performance_evidence_smoke_ready,
            "requires_shadow_trading_report_discovery_smoke": (
                not shadow_trading_report_discovery_smoke_ready
            ),
            "requires_real_shadow_trading_report": not shadow_trading_report_discovery_ready,
            "requires_shadow_trading_acceptance_smoke": not shadow_trading_acceptance_smoke_ready,
            "requires_rust_shadow_strategy_acceptance_smoke": not rust_shadow_strategy_acceptance_smoke_ready,
            "requires_rust_shadow_strategy_acceptance_runner_smoke": (
                not rust_shadow_strategy_acceptance_runner_smoke_ready
            ),
        },
        "evidence": {
            "readiness": summarize(readiness_path, readiness, ("status", "next_gate")),
            "status_bundle": summarize(
                bundle_path,
                bundle,
                ("verdict", "ec2_readonly_user_ws_status", "ec2_next_gate"),
            ),
            "acceptance_smoke": summarize(
                acceptance_smoke_path,
                acceptance_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "acceptance_review"),
            ),
            "acceptance_review": summarize(
                acceptance_review_path,
                acceptance_review,
                (
                    "status",
                    "acceptance_passed",
                    "duration_secs",
                    "user_ws_connected_count",
                    "user_ws_subscribe_sent_count",
                    "user_ws_raw_count",
                    "forbidden_event_count",
                    "orders_possible",
                    "recorder_critical_drop_count",
                    "fills_seen_manifest",
                ),
            ),
            "source_truth_schema_smoke": summarize(
                source_truth_smoke_path,
                source_truth_smoke,
                ("status", "scope", "orders_sent", "auth_network_started"),
            ),
            "observer_safety_smoke": summarize(
                observer_safety_smoke_path,
                observer_safety_smoke,
                ("status", "orders_sent", "auth_network_started"),
            ),
            "order_plan_smoke": summarize(
                order_plan_smoke_path,
                order_plan_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "planner"),
            ),
            "execution_controller_smoke": summarize(
                execution_controller_smoke_path,
                execution_controller_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "controller"),
            ),
            "oms_adapter_smoke": summarize(
                oms_adapter_smoke_path,
                oms_adapter_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "adapter"),
            ),
            "runtime_wiring_smoke": summarize(
                runtime_wiring_smoke_path,
                runtime_wiring_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "coordinator"),
            ),
            "source_truth_runtime_gate_smoke": summarize(
                source_truth_runtime_gate_smoke_path,
                source_truth_runtime_gate_smoke,
                ("status", "scope", "orders_sent", "auth_network_started", "coordinator"),
            ),
            "g2_canary_runbook_smoke": summarize(
                g2_canary_runbook_smoke_path,
                g2_canary_runbook_smoke,
                (
                    "artifact",
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "exact_approval_required",
                    "runbook",
                ),
            ),
            "g2_canary_acceptance_smoke": summarize(
                g2_canary_acceptance_smoke_path,
                g2_canary_acceptance_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "exact_approval_required",
                    "fixture_results",
                ),
            ),
            "g2_canary_launch_plan_smoke": summarize(
                g2_canary_launch_plan_smoke_path,
                g2_canary_launch_plan_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "plan_manifest",
                ),
            ),
            "g2_canary_review_smoke": summarize(
                g2_canary_review_smoke_path,
                g2_canary_review_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "summarizer",
                ),
            ),
            "g2_canary_approval_envelope_smoke": summarize(
                g2_canary_approval_envelope_smoke_path,
                g2_canary_approval_envelope_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "verifier",
                ),
            ),
            "g2_canary_launcher_refusal_smoke": summarize(
                g2_canary_launcher_refusal_smoke_path,
                g2_canary_launcher_refusal_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "launcher",
                ),
            ),
            "g2_canary_executor_contract_smoke": summarize(
                g2_canary_executor_contract_smoke_path,
                g2_canary_executor_contract_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "contract_manifest",
                ),
            ),
            "g2_canary_executor_smoke": summarize(
                g2_canary_executor_smoke_path,
                g2_canary_executor_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "executor",
                ),
            ),
            "g2_canary_executor_dry_run_smoke": summarize(
                g2_canary_executor_dry_run_smoke_path,
                g2_canary_executor_dry_run_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "executor",
                ),
            ),
            "g2_canary_executor_payload_manifest_smoke": summarize(
                g2_canary_executor_payload_manifest_smoke_path,
                g2_canary_executor_payload_manifest_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "payload_manifest",
                ),
            ),
            "g2_canary_effectful_executor_review_smoke": summarize(
                g2_canary_effectful_executor_review_smoke_path,
                g2_canary_effectful_executor_review_smoke,
                (
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
            "rust_shadow_strategy_acceptance": summarize(
                rust_shadow_strategy_acceptance_path,
                rust_shadow_strategy_acceptance,
                (
                    "status",
                    "scope",
                    "strategy_acceptance_passed",
                    "orders_sent",
                    "cancels_sent",
                    "redeems_sent",
                    "auth_network_started",
                    "started_canary",
                ),
            ),
            "shadow_performance_evidence_smoke": summarize(
                shadow_performance_evidence_smoke_path,
                shadow_performance_evidence_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "performance_script",
                    "real_performance_evidence_published",
                ),
            ),
            "shadow_trading_acceptance_smoke": summarize(
                shadow_trading_acceptance_smoke_path,
                shadow_trading_acceptance_smoke,
                (
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
            "shadow_trading_report_discovery_smoke": summarize(
                shadow_trading_report_discovery_smoke_path,
                shadow_trading_report_discovery_smoke,
                (
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
                    "real_shadow_trading_report_published",
                ),
            ),
            "shadow_trading_report_discovery": summarize(
                shadow_trading_report_discovery_path,
                shadow_trading_report_discovery,
                (
                    "status",
                    "scope",
                    "candidate_count",
                    "fixture_or_smoke_candidate_count",
                    "real_candidate_count",
                    "valid_real_candidate_count",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "next_gate",
                ),
            ),
            "shadow_trading_acceptance": summarize(
                shadow_trading_acceptance_path,
                shadow_trading_acceptance,
                (
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
            "pair_arb_backtest_grid": summarize(
                pair_arb_backtest_path,
                pair_arb_backtest,
                (
                    "status",
                    "scope",
                    "run_count",
                    "nonzero_fill_run_count",
                    "positive_nonzero_run_count",
                    "raw_replay_scanned",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                ),
            ),
            "pair_arb_oos_compare": summarize(
                pair_arb_oos_compare_path,
                pair_arb_oos_compare,
                (
                    "status",
                    "scope",
                    "matched_config_count",
                    "both_positive_config_count",
                    "qualified_both_positive_config_count",
                    "oos_validation_passed",
                    "raw_replay_scanned",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                ),
            ),
            "pair_arb_walkforward_compare": summarize(
                pair_arb_walkforward_compare_path,
                pair_arb_walkforward_compare,
                (
                    "status",
                    "scope",
                    "split_count",
                    "matched_config_count",
                    "all_positive_config_count",
                    "qualified_all_positive_config_count",
                    "walkforward_validation_passed",
                    "raw_replay_scanned",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                ),
            ),
            "shadow_edge_samples_smoke": summarize(
                shadow_edge_samples_smoke_path,
                shadow_edge_samples_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "extractor",
                    "edge_only_rejected_by_performance_gate",
                ),
            ),
            "l1_dry_run_outcome_labels_smoke": summarize(
                l1_dry_run_outcome_labels_smoke_path,
                l1_dry_run_outcome_labels_smoke,
                (
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
            "l1_dry_run_outcome_labels": summarize(
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
            "no_order_shadow_run_artifact_smoke": summarize(
                no_order_shadow_run_artifact_smoke_path,
                no_order_shadow_run_artifact_smoke,
                (
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
            "shadow_acceptance_input_discovery_smoke": summarize(
                shadow_acceptance_input_discovery_smoke_path,
                shadow_acceptance_input_discovery_smoke,
                (
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
            "shadow_acceptance_input_discovery": summarize(
                shadow_acceptance_input_discovery_path,
                shadow_acceptance_input_discovery,
                (
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
            "outcome_label_bridge_smoke": summarize(
                outcome_label_bridge_smoke_path,
                outcome_label_bridge_smoke,
                (
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
            "realized_outcome_labels_smoke": summarize(
                realized_outcome_labels_smoke_path,
                realized_outcome_labels_smoke,
                (
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
            "rust_shadow_strategy_acceptance_smoke": summarize(
                rust_shadow_strategy_acceptance_smoke_path,
                rust_shadow_strategy_acceptance_smoke,
                (
                    "status",
                    "scope",
                    "orders_sent",
                    "auth_network_started",
                    "started_canary",
                    "acceptance_script",
                    "acceptance_artifact_published",
                ),
            ),
            "rust_shadow_strategy_acceptance_runner_smoke": summarize(
                rust_shadow_strategy_acceptance_runner_smoke_path,
                rust_shadow_strategy_acceptance_runner_smoke,
                (
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
            "canary_design_doc": {
                "path": str(canary_design_path),
                "exists": canary_design_path.exists(),
            },
        },
        "g2_smoke_canary_minimum_conditions": [
            "single BTC 5m market",
            "post-only maker-only passive legs",
            "max_live_orders <= 2",
            "max_open_cost_usdc <= 100",
            "orders/cancels/fills/redeem/cashflow recorder fields complete",
            "any UNKNOWN order/fill/wallet/cashflow state stops the run",
            "unexpected taker passive fill stops the run",
            "wallet/balance mismatch stops the run",
            "recorder critical drop stops the run",
            "post-run local review/secret-sentinel scan must pass",
            "structured exact approval envelope must pass before launcher accepts a run",
            "launcher must fail closed before any execution path exists",
            "reviewed executor contract must pass before any execution path implementation",
            "executor dry-run skeleton must refuse before SSH/sync/build/run side effects",
            "executor payload manifest must allowlist sync files before implementation",
            "Launch plan readiness is approvable evidence only; ready_to_execute stays false until a reviewed effectful executor is implemented",
            "Observer edge samples must be extracted locally but rejected as standalone performance evidence",
            "L1 dry-run outcome labels must pass local no-network smoke and real quality gates before acceptance",
            "No-order shadow run artifact harness must emit explicit outcome label events",
            "Shadow acceptance input discovery must distinguish real run artifacts from fixture/smoke inputs",
            "Realized outcome-label producer must read only explicit no-order outcome events",
            "Outcome-label bridge must require independent realized no-order labels before performance evidence",
            "Shadow performance evidence smoke must pass before Rust strategy acceptance can be trusted",
            "Rust shadow/dry-run strategy acceptance smoke must pass before strategy acceptance can be trusted",
            "Rust shadow/dry-run strategy acceptance runner must refuse fixture publication",
            "Rust shadow/dry-run strategy acceptance must pass before any read-write canary",
            "Shadow trading report discovery must find a non-fixture passive/passive aggregate_report before acceptance can pass",
            "D+ passive/passive shadow trading acceptance must report positive simulated pair PnL, pair coverage, and bounded residual risk before any read-write canary",
            "manual exact approval required before any network canary process",
        ],
        "forbidden_side_effects": {
            "systemd_or_service_control": False,
            "broker_start_stop_repair": False,
            "shared_ingress_modification": False,
            "order_cancel_redeem": False,
            "live_trading": False,
            "remote_env_file_write": False,
            "ssh_or_network": False,
            "git_fetch_push": False,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
        },
        "next_gate": (
            "explicit exact G2 canary approval plus reviewed effectful executor implementation required; heartbeat or generic authorization is not enough"
            if status == "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL"
            else
            shadow_trading_failed_next_gate
            if status == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
            else
            "run or locate D+ passive/passive shadow trading report with pair/PnL/residual metrics, then pass local shadow trading acceptance before any G2 read-write canary"
            if status == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_MISSING"
            else
            "run and pass Rust no-order shadow/dry-run strategy acceptance before any G2 read-write canary"
            if status == "CANARY_NOT_READY_SHADOW_DRY_RUN_STRATEGY_ACCEPTANCE_MISSING"
            else
            "wire redeem tx-hash confirmation into runtime source truth, then require stable all-PASS source truth before any canary run"
            if status == "CANARY_NOT_READY_SOURCE_TRUTH_FEEDS_NOT_WIRED"
            else
            "prepare exact G2 canary runbook and local canary acceptance smoke before any network canary approval"
            if status == "CANARY_NOT_READY_G2_CANARY_RUNBOOK_NOT_READY"
            else
            "wire own order/fill/redeem/cashflow source truth into the runtime account-truth gate before any canary run"
            if status == "CANARY_NOT_READY_SOURCE_TRUTH_RUNTIME_NOT_READY"
            else
            "wire exact-approval-gated controller/OMS adapter into runtime plus local no-command emission gates"
            if status == "CANARY_NOT_READY_RUNTIME_WIRING_NOT_IMPLEMENTED"
            else
            "implement exact-approval-gated OMS adapter plus local no-command-without-approval gates"
            if status == "CANARY_NOT_READY_OMS_ADAPTER_NOT_IMPLEMENTED"
            else
            "wire the preview-only canary order planner into an execution controller behind exact G2 approval gates"
            if status == "CANARY_NOT_READY_CONTROLLER_NOT_WIRED"
            else "implement and locally gate a canary-safe order-intent/controller plan before any exact G2 canary approval"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status in {
        "CANARY_NOT_READY_ORDER_CONTROLLER_NOT_IMPLEMENTED",
        "CANARY_NOT_READY_CONTROLLER_NOT_WIRED",
        "CANARY_NOT_READY_OMS_ADAPTER_NOT_IMPLEMENTED",
        "CANARY_NOT_READY_RUNTIME_WIRING_NOT_IMPLEMENTED",
        "CANARY_NOT_READY_SOURCE_TRUTH_RUNTIME_NOT_READY",
        "CANARY_NOT_READY_SOURCE_TRUTH_FEEDS_NOT_WIRED",
        "CANARY_NOT_READY_G2_CANARY_RUNBOOK_NOT_READY",
        "CANARY_NOT_READY_SHADOW_DRY_RUN_STRATEGY_ACCEPTANCE_MISSING",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED",
        "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_MISSING",
        "READY_FOR_EXPLICIT_G2_CANARY_APPROVAL",
    } else 1


if __name__ == "__main__":
    raise SystemExit(main())
