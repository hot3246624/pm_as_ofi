#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_canary_readiness_plan_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
plan_dir="$out_dir/plan_run"
plan_manifest="$plan_dir/manifest.json"
: > "$log"

run_check() {
  local name="$1"
  shift
  {
    printf '== %s ==\n' "$name"
    "$@"
  } >> "$log" 2>&1 || {
    status="FAIL"
    printf 'CHECK_FAILED %s\n' "$name" >> "$log"
  }
}

script="scripts/xuan_b27_dplus_canary_readiness_plan.py"

run_check "plan_script_exists" test -x "$script"
run_check "plan_script_py_compile" python3 -m py_compile "$script"
run_check "plan_script_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "plan_script_run" "$script" --output-dir "$plan_dir"
run_check "plan_manifest_exists" test -f "$plan_manifest"
run_check "plan_manifest_safe" python3 - "$plan_manifest" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
side_effects = data.get("side_effects") or {}
forbidden = data.get("forbidden_side_effects") or {}
readiness = data.get("canary_readiness") or {}
runtime = data.get("runtime_static_findings") or {}
evidence = data.get("evidence") or {}
acceptance = evidence.get("acceptance_review") or {}
oos_status = readiness.get("pair_arb_oos_compare_status")
oos_compare_ok = (
    (
        oos_status == "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS"
        and readiness.get("pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and readiness.get("pair_arb_oos_compare_qualified_both_positive_config_count", 0) > 0
        and readiness.get("pair_arb_oos_compare_validation_passed") is True
        and readiness.get("requires_strategy_retrain_or_positive_backtest_config") is False
    )
    or (
        oos_status == "FAIL_OOS_POSITIVE_CONFIGS_BELOW_SAMPLE_OR_SIZE"
        and readiness.get("pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and readiness.get("pair_arb_oos_compare_qualified_both_positive_config_count") == 0
        and readiness.get("pair_arb_oos_compare_validation_passed") is False
        and readiness.get("requires_strategy_retrain_or_positive_backtest_config") is True
    )
    or (
        oos_status == "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
        and readiness.get("pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and readiness.get("pair_arb_oos_compare_qualified_both_positive_config_count", 0) > 0
        and readiness.get("pair_arb_oos_compare_numeric_validation_passed") is True
        and readiness.get("pair_arb_oos_compare_validation_passed") is False
        and readiness.get("pair_arb_oos_compare_requires_compliant_dataset") is True
        and readiness.get("requires_strategy_retrain_or_positive_backtest_config") is True
    )
)
walkforward_status = readiness.get("pair_arb_walkforward_compare_status")
walkforward_ok = (
    (
        walkforward_status == "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS"
        and readiness.get("pair_arb_walkforward_compare_split_count", 0) >= 4
        and readiness.get("pair_arb_walkforward_compare_all_positive_config_count", 0) > 0
        and readiness.get(
            "pair_arb_walkforward_compare_qualified_all_positive_config_count", 0
        ) > 0
        and readiness.get("pair_arb_walkforward_compare_validation_passed") is True
    )
    or (
        walkforward_status
        == "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
        and readiness.get("pair_arb_walkforward_compare_split_count", 0) >= 4
        and readiness.get("pair_arb_walkforward_compare_all_positive_config_count", 0) > 0
        and readiness.get(
            "pair_arb_walkforward_compare_qualified_all_positive_config_count", 0
        ) > 0
        and readiness.get("pair_arb_walkforward_compare_numeric_validation_passed") is True
        and readiness.get("pair_arb_walkforward_compare_validation_passed") is False
        and readiness.get("pair_arb_walkforward_compare_requires_compliant_dataset") is True
        and readiness.get("requires_strategy_retrain_or_positive_backtest_config") is True
    )
)
ok = (
    data.get("artifact") == "xuan_b27_dplus_canary_readiness_plan"
    and data.get("status") == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
    and data.get("scope") == "local_no_network_canary_readiness_plan"
    and data.get("read_only_acceptance_passed") is True
    and data.get("local_gates_ok") is True
    and data.get("source_truth_schema_ready") is True
    and data.get("observer_safety_ready") is True
    and acceptance.get("status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
    and acceptance.get("acceptance_passed") is True
    and acceptance.get("duration_secs") == 1800
    and acceptance.get("user_ws_connected_count", 0) > 0
    and acceptance.get("user_ws_subscribe_sent_count", 0) > 0
    and acceptance.get("user_ws_raw_count", 0) > 0
    and acceptance.get("forbidden_event_count") == 0
    and acceptance.get("orders_possible") is False
    and evidence.get("acceptance_smoke", {}).get("status") == "PASS"
    and evidence.get("source_truth_schema_smoke", {}).get("status") == "PASS"
    and evidence.get("order_plan_smoke", {}).get("status") == "PASS"
    and evidence.get("execution_controller_smoke", {}).get("status") == "PASS"
    and evidence.get("oms_adapter_smoke", {}).get("status") == "PASS"
    and evidence.get("runtime_wiring_smoke", {}).get("status") == "PASS"
    and evidence.get("source_truth_runtime_gate_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_runbook_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_runbook_smoke", {}).get("exact_approval_required") is True
    and evidence.get("g2_canary_runbook_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_acceptance_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_acceptance_smoke", {}).get("exact_approval_required") is True
    and evidence.get("g2_canary_acceptance_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_launch_plan_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_launch_plan_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_launch_plan_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_review_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_review_smoke", {}).get("scope") == "local_no_network_g2_canary_review_gate"
    and evidence.get("g2_canary_review_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_review_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_review_smoke", {}).get("auth_network_started") is False
    and evidence.get("g2_canary_review_smoke", {}).get("summarizer") == "scripts/xuan_b27_dplus_summarize_g2_canary_run.py"
    and evidence.get("g2_canary_approval_envelope_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_approval_envelope_smoke", {}).get("scope") == "local_no_network_g2_canary_approval_envelope_gate"
    and evidence.get("g2_canary_approval_envelope_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_approval_envelope_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_approval_envelope_smoke", {}).get("auth_network_started") is False
    and evidence.get("g2_canary_approval_envelope_smoke", {}).get("verifier") == "scripts/xuan_b27_dplus_g2_canary_approval_envelope.py"
    and evidence.get("g2_canary_launcher_refusal_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_launcher_refusal_smoke", {}).get("scope") == "local_no_network_g2_canary_launcher_refusal_gate"
    and evidence.get("g2_canary_launcher_refusal_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_launcher_refusal_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_launcher_refusal_smoke", {}).get("auth_network_started") is False
    and evidence.get("g2_canary_launcher_refusal_smoke", {}).get("launcher") == "scripts/xuan_b27_dplus_g2_canary_launcher_refusal.py"
    and evidence.get("g2_canary_executor_contract_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_executor_contract_smoke", {}).get("scope") == "local_no_network_g2_canary_executor_contract_gate"
    and evidence.get("g2_canary_executor_contract_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_executor_contract_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_executor_contract_smoke", {}).get("auth_network_started") is False
    and isinstance(evidence.get("g2_canary_executor_contract_smoke", {}).get("contract_manifest"), str)
    and evidence.get("g2_canary_executor_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_executor_smoke", {}).get("scope") == "local_no_network_g2_canary_executor_preflight_gate"
    and evidence.get("g2_canary_executor_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_executor_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_executor_smoke", {}).get("auth_network_started") is False
    and evidence.get("g2_canary_executor_smoke", {}).get("executor") == "scripts/xuan_b27_dplus_g2_canary_executor.py"
    and evidence.get("g2_canary_executor_dry_run_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_executor_dry_run_smoke", {}).get("scope") == "local_no_network_g2_canary_executor_dry_run_gate"
    and evidence.get("g2_canary_executor_dry_run_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_executor_dry_run_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_executor_dry_run_smoke", {}).get("auth_network_started") is False
    and evidence.get("g2_canary_executor_dry_run_smoke", {}).get("executor") == "scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py"
    and evidence.get("g2_canary_executor_payload_manifest_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_executor_payload_manifest_smoke", {}).get("scope") == "local_no_network_g2_canary_executor_payload_manifest_gate"
    and evidence.get("g2_canary_executor_payload_manifest_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_executor_payload_manifest_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_executor_payload_manifest_smoke", {}).get("auth_network_started") is False
    and isinstance(evidence.get("g2_canary_executor_payload_manifest_smoke", {}).get("payload_manifest"), str)
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("status") == "PASS"
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("scope") == "local_no_network_g2_canary_effectful_executor_review_gate"
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("started_canary") is False
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("orders_sent") is False
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("cancels_sent") is False
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("redeems_sent") is False
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("auth_network_started") is False
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("reviewer") == "scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py"
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("real_effectful_executor_review_status") == "BLOCKED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("real_effectful_executor_implemented") is False
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("real_review_passed") is False
    and evidence.get("g2_canary_effectful_executor_review_smoke", {}).get("fixture_pass_artifact_published") is False
    and evidence.get("shadow_edge_samples_smoke", {}).get("status") == "PASS"
    and evidence.get("shadow_edge_samples_smoke", {}).get("scope") == "local_no_network_shadow_edge_sample_extraction_gate"
    and evidence.get("shadow_edge_samples_smoke", {}).get("orders_sent") is False
    and evidence.get("shadow_edge_samples_smoke", {}).get("auth_network_started") is False
    and evidence.get("shadow_edge_samples_smoke", {}).get("started_canary") is False
    and evidence.get("shadow_edge_samples_smoke", {}).get("extractor") == "scripts/xuan_b27_dplus_extract_shadow_edge_samples.py"
    and evidence.get("shadow_edge_samples_smoke", {}).get("edge_only_rejected_by_performance_gate") is True
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("status") == "PASS"
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("scope") == "local_no_network_l1_dry_run_outcome_label_gate"
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("orders_sent") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("cancels_sent") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("redeems_sent") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("auth_network_started") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("started_observer") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("started_user_ws") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("started_canary") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("producer") == "scripts/xuan_b27_dplus_l1_dry_run_outcome_labels.py"
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("real_outcome_labels_published") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("real_shadow_run_artifact_published") is False
    and evidence.get("l1_dry_run_outcome_labels_smoke", {}).get("acceptance_artifact_published") is False
    and evidence.get("l1_dry_run_outcome_labels", {}).get("exists") is True
    and evidence.get("l1_dry_run_outcome_labels", {}).get("artifact") == "xuan_b27_dplus_l1_dry_run_outcome_labels"
    and evidence.get("l1_dry_run_outcome_labels", {}).get("scope") == "local_no_network_l1_dry_run_outcome_label_producer"
    and evidence.get("l1_dry_run_outcome_labels", {}).get("orders_sent") is False
    and evidence.get("l1_dry_run_outcome_labels", {}).get("auth_network_started") is False
    and evidence.get("l1_dry_run_outcome_labels", {}).get("started_canary") is False
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("status") == "PASS"
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("scope") == "local_no_network_no_order_shadow_run_artifact_gate"
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("orders_sent") is False
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("cancels_sent") is False
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("redeems_sent") is False
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("auth_network_started") is False
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("started_canary") is False
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("producer") == "scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py"
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("real_shadow_run_artifact_published") is False
    and evidence.get("no_order_shadow_run_artifact_smoke", {}).get("acceptance_artifact_published") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("status") == "PASS"
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("scope") == "local_no_network_shadow_acceptance_input_discovery_gate"
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("orders_sent") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("cancels_sent") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("redeems_sent") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("auth_network_started") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("started_observer") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("started_user_ws") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("started_canary") is False
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("discovery_script") == "scripts/xuan_b27_dplus_shadow_acceptance_input_discovery.py"
    and evidence.get("shadow_acceptance_input_discovery_smoke", {}).get("real_shadow_acceptance_input_published") is False
    and evidence.get("realized_outcome_labels_smoke", {}).get("status") == "PASS"
    and evidence.get("realized_outcome_labels_smoke", {}).get("scope") == "local_no_network_realized_outcome_label_producer_gate"
    and evidence.get("realized_outcome_labels_smoke", {}).get("orders_sent") is False
    and evidence.get("realized_outcome_labels_smoke", {}).get("auth_network_started") is False
    and evidence.get("realized_outcome_labels_smoke", {}).get("started_canary") is False
    and evidence.get("realized_outcome_labels_smoke", {}).get("producer") == "scripts/xuan_b27_dplus_realized_outcome_labels.py"
    and evidence.get("realized_outcome_labels_smoke", {}).get("real_outcome_labels_published") is False
    and evidence.get("realized_outcome_labels_smoke", {}).get("acceptance_artifact_published") is False
    and evidence.get("outcome_label_bridge_smoke", {}).get("status") == "PASS"
    and evidence.get("outcome_label_bridge_smoke", {}).get("scope") == "local_no_network_outcome_label_bridge_gate"
    and evidence.get("outcome_label_bridge_smoke", {}).get("orders_sent") is False
    and evidence.get("outcome_label_bridge_smoke", {}).get("auth_network_started") is False
    and evidence.get("outcome_label_bridge_smoke", {}).get("started_canary") is False
    and evidence.get("outcome_label_bridge_smoke", {}).get("bridge") == "scripts/xuan_b27_dplus_outcome_label_bridge.py"
    and evidence.get("outcome_label_bridge_smoke", {}).get("real_outcome_bridge_published") is False
    and evidence.get("outcome_label_bridge_smoke", {}).get("acceptance_artifact_published") is False
    and evidence.get("shadow_performance_evidence_smoke", {}).get("status") == "PASS"
    and evidence.get("shadow_performance_evidence_smoke", {}).get("scope") == "local_no_network_shadow_performance_evidence_gate"
    and evidence.get("shadow_performance_evidence_smoke", {}).get("orders_sent") is False
    and evidence.get("shadow_performance_evidence_smoke", {}).get("auth_network_started") is False
    and evidence.get("shadow_performance_evidence_smoke", {}).get("started_canary") is False
    and evidence.get("shadow_performance_evidence_smoke", {}).get("performance_script") == "scripts/xuan_b27_dplus_shadow_performance_evidence.py"
    and evidence.get("shadow_performance_evidence_smoke", {}).get("real_performance_evidence_published") is False
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("status") == "PASS"
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("scope") == "local_no_network_shadow_trading_acceptance_gate"
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("orders_sent") is False
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("cancels_sent") is False
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("redeems_sent") is False
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("auth_network_started") is False
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("started_canary") is False
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("acceptance_script") == "scripts/xuan_b27_dplus_shadow_trading_acceptance.py"
    and evidence.get("shadow_trading_acceptance_smoke", {}).get("real_shadow_trading_acceptance_published") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("status") == "PASS"
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("scope") == "local_no_network_shadow_trading_report_discovery_gate"
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("orders_sent") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("cancels_sent") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("redeems_sent") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("auth_network_started") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("started_observer") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("started_user_ws") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("started_canary") is False
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("discovery_script") == "scripts/xuan_b27_dplus_shadow_trading_report_discovery.py"
    and evidence.get("shadow_trading_report_discovery_smoke", {}).get("real_shadow_trading_report_published") is False
    and evidence.get("shadow_trading_report_discovery", {}).get("exists") is True
    and evidence.get("shadow_trading_report_discovery", {}).get("status") == "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
    and evidence.get("shadow_trading_report_discovery", {}).get("real_candidate_count", 0) >= 1
    and evidence.get("shadow_trading_report_discovery", {}).get("valid_real_candidate_count", 0) >= 1
    and evidence.get("shadow_trading_acceptance", {}).get("exists") is True
    and evidence.get("shadow_trading_acceptance", {}).get("status")
    in {
        "FAIL_SHADOW_TRADING_SAMPLE_SIZE",
        "FAIL_SHADOW_TRADING_PNL_METRICS",
        "FAIL_SHADOW_TRADING_RESIDUAL_RISK",
    }
    and evidence.get("shadow_trading_acceptance", {}).get("acceptance_passed") is False
    and evidence.get("shadow_trading_acceptance", {}).get("orders_sent") is False
    and evidence.get("rust_shadow_strategy_acceptance_smoke", {}).get("status") == "PASS"
    and evidence.get("rust_shadow_strategy_acceptance_smoke", {}).get("scope") == "local_no_network_rust_shadow_strategy_acceptance_gate"
    and evidence.get("rust_shadow_strategy_acceptance_smoke", {}).get("orders_sent") is False
    and evidence.get("rust_shadow_strategy_acceptance_smoke", {}).get("auth_network_started") is False
    and evidence.get("rust_shadow_strategy_acceptance_smoke", {}).get("started_canary") is False
    and evidence.get("rust_shadow_strategy_acceptance_smoke", {}).get("acceptance_script") == "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py"
    and evidence.get("rust_shadow_strategy_acceptance_smoke", {}).get("acceptance_artifact_published") is False
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("status") == "PASS"
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("scope") == "local_no_network_rust_shadow_strategy_acceptance_runner_gate"
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("orders_sent") is False
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("cancels_sent") is False
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("redeems_sent") is False
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("auth_network_started") is False
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("started_canary") is False
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("runner") == "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py"
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("real_acceptance_artifact_published") is False
    and evidence.get("rust_shadow_strategy_acceptance_runner_smoke", {}).get("acceptance_artifact_published") is False
    and evidence.get("rust_shadow_strategy_acceptance", {}).get("exists") is True
    and evidence.get("rust_shadow_strategy_acceptance", {}).get("status") == "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE"
    and evidence.get("rust_shadow_strategy_acceptance", {}).get("scope") == "rust_no_order_shadow_or_dry_run_strategy_acceptance"
    and evidence.get("rust_shadow_strategy_acceptance", {}).get("strategy_acceptance_passed") is True
    and evidence.get("rust_shadow_strategy_acceptance", {}).get("orders_sent") is False
    and evidence.get("rust_shadow_strategy_acceptance", {}).get("auth_network_started") is False
    and evidence.get("rust_shadow_strategy_acceptance", {}).get("started_canary") is False
    and data.get("shadow_strategy_evidence", {}).get("kind") == "NO_ORDER_LOCAL_L1_TOUCH_DRY_RUN"
    and data.get("shadow_strategy_evidence", {}).get("basis") == "dry_run_outcome"
    and data.get("shadow_strategy_evidence", {}).get("accepted") is True
    and data.get("shadow_strategy_evidence", {}).get("no_order") is True
    and data.get("shadow_strategy_evidence", {}).get("orders_sent") is False
    and data.get("shadow_strategy_evidence", {}).get("cancels_sent") is False
    and data.get("shadow_strategy_evidence", {}).get("redeems_sent") is False
    and data.get("shadow_strategy_evidence", {}).get("auth_network_started") is False
    and data.get("shadow_strategy_evidence", {}).get("started_canary") is False
    and data.get("shadow_strategy_evidence", {}).get("live_fill_pnl_queue_position_proof") is False
    and "live fill quality" in (data.get("shadow_strategy_evidence", {}).get("does_not_prove") or [])
    and "real PnL" in (data.get("shadow_strategy_evidence", {}).get("does_not_prove") or [])
    and "queue position" in (data.get("shadow_strategy_evidence", {}).get("does_not_prove") or [])
    and data.get("shadow_trading_evidence", {}).get("kind") == "SIMULATED_SHADOW_TRADING_REPORT"
    and data.get("shadow_trading_evidence", {}).get("accepted") is False
    and data.get("shadow_trading_evidence", {}).get("report_discovery_status") == "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
    and data.get("shadow_trading_evidence", {}).get("report_discovery_ready") is True
    and data.get("shadow_trading_evidence", {}).get("report_discovery_real_candidate_count", 0) >= 1
    and data.get("shadow_trading_evidence", {}).get("report_discovery_valid_real_candidate_count", 0) >= 1
    and data.get("shadow_trading_evidence", {}).get("acceptance_status")
    in {
        "FAIL_SHADOW_TRADING_SAMPLE_SIZE",
        "FAIL_SHADOW_TRADING_PNL_METRICS",
        "FAIL_SHADOW_TRADING_RESIDUAL_RISK",
    }
    and data.get("shadow_trading_evidence", {}).get("required_before_g2_canary") is True
    and data.get("shadow_trading_evidence", {}).get("orders_sent") is False
    and data.get("shadow_trading_evidence", {}).get("auth_network_started") is False
    and data.get("shadow_trading_evidence", {}).get("started_canary") is False
    and evidence.get("canary_design_doc", {}).get("exists") is True
    and runtime.get("order_plan_module_registered") is True
    and runtime.get("order_plan_smoke_passed") is True
    and runtime.get("execution_controller_module_registered") is True
    and runtime.get("execution_controller_smoke_passed") is True
    and runtime.get("oms_adapter_module_registered") is True
    and runtime.get("oms_adapter_smoke_passed") is True
    and runtime.get("runtime_wiring_smoke_passed") is True
    and runtime.get("source_truth_runtime_gate_smoke_passed") is True
    and runtime.get("source_truth_event_type_registered") is True
    and runtime.get("executor_order_truth_feed_wired") is True
    and runtime.get("user_ws_fill_truth_feed_wired") is True
    and runtime.get("coordinator_source_truth_channel_wired") is True
    and runtime.get("main_runtime_source_truth_channel_wired") is True
    and runtime.get("source_truth_event_feed_tests_present") is True
    and runtime.get("wallet_runtime_producer_wired") is True
    and runtime.get("redeem_runtime_producer_wired") is True
    and runtime.get("cashflow_runtime_producer_wired") is True
    and runtime.get("wallet_redeem_cashflow_runtime_producers_wired") is True
    and runtime.get("redeem_truth_runtime_confirmation_ready") is True
    and runtime.get("strategy_returns_empty_quotes") is True
    and runtime.get("observer_payload_quotes_forced_empty") is True
    and runtime.get("observer_order_controller_implemented_false") is True
    and runtime.get("canary_blocked_event_present") is True
    and runtime.get("canary_block_reason_not_implemented") is True
    and runtime.get("canary_mode_invariant_tests_present") is True
    and readiness.get("canary_run_ready") is False
    and readiness.get("ready_to_execute") is False
    and readiness.get("canary_run_authorized") is False
    and readiness.get("effectful_executor_implemented") is False
    and readiness.get("execution_readiness_status") == "NOT_READY_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and readiness.get("order_intent_preview_surface_ready") is True
    and readiness.get("execution_controller_decision_ready") is True
    and readiness.get("oms_adapter_surface_ready") is True
    and readiness.get("runtime_sender_wiring_ready") is True
    and readiness.get("source_truth_runtime_gate_ready") is True
    and readiness.get("order_fill_source_truth_event_feeds_ready") is True
    and readiness.get("wallet_redeem_cashflow_event_feeds_ready") is True
    and readiness.get("g2_canary_runbook_ready") is True
    and readiness.get("g2_canary_acceptance_smoke_ready") is True
    and readiness.get("g2_canary_launch_plan_ready") is True
    and readiness.get("g2_canary_review_smoke_ready") is True
    and readiness.get("g2_canary_approval_envelope_smoke_ready") is True
    and readiness.get("g2_canary_launcher_refusal_smoke_ready") is True
    and readiness.get("g2_canary_executor_contract_smoke_ready") is True
    and readiness.get("g2_canary_executor_smoke_ready") is True
    and readiness.get("g2_canary_executor_dry_run_smoke_ready") is True
    and readiness.get("g2_canary_executor_payload_manifest_smoke_ready") is True
    and readiness.get("g2_canary_effectful_executor_review_smoke_ready") is True
    and readiness.get("shadow_edge_samples_smoke_ready") is True
    and readiness.get("l1_dry_run_outcome_labels_smoke_ready") is True
    and readiness.get("l1_dry_run_outcome_labels_ready") is True
    and readiness.get("no_order_shadow_run_artifact_smoke_ready") is True
    and readiness.get("shadow_acceptance_input_discovery_smoke_ready") is True
    and readiness.get("shadow_acceptance_input_ready") is True
    and readiness.get("realized_outcome_labels_smoke_ready") is True
    and readiness.get("outcome_label_bridge_smoke_ready") is True
    and readiness.get("shadow_performance_evidence_smoke_ready") is True
    and readiness.get("shadow_trading_report_discovery_smoke_ready") is True
    and readiness.get("shadow_trading_report_discovery_ready") is True
    and readiness.get("shadow_trading_report_discovery_status") == "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
    and readiness.get("shadow_trading_report_discovery_real_candidate_count", 0) >= 1
    and readiness.get("shadow_trading_report_discovery_valid_real_candidate_count", 0) >= 1
    and readiness.get("shadow_trading_acceptance_smoke_ready") is True
    and readiness.get("rust_shadow_strategy_acceptance_smoke_ready") is True
    and readiness.get("rust_shadow_strategy_acceptance_runner_smoke_ready") is True
    and readiness.get("pre_canary_plumbing_ready") is True
    and readiness.get("rust_shadow_strategy_acceptance_ready") is True
    and readiness.get("shadow_strategy_evidence_kind") == "NO_ORDER_LOCAL_L1_TOUCH_DRY_RUN"
    and readiness.get("shadow_strategy_evidence_basis") == "dry_run_outcome"
    and readiness.get("shadow_strategy_evidence_no_order") is True
    and readiness.get("shadow_strategy_evidence_live_fill_pnl_queue_position_proof") is False
    and readiness.get("shadow_trading_acceptance_ready") is False
    and readiness.get("shadow_trading_evidence_kind") == "SIMULATED_SHADOW_TRADING_REPORT"
    and readiness.get("shadow_trading_evidence_basis") == "xuan_dplus_passive_passive_shadow_runner.py aggregate_report"
    and readiness.get("shadow_trading_pair_pnl") is not None
    and readiness.get("pair_arb_backtest_status")
    in {
        "FAIL_NO_POSITIVE_BACKTEST_CONFIG",
        "PASS_OOS_POSITIVE_BACKTEST_CONFIG",
        "PASS_OOS_POSITIVE_BACKTEST_CONFIG_DATASET_SCOPE_LIMITED",
    }
    and readiness.get("pair_arb_backtest_run_count", 0) >= 1
    and readiness.get("pair_arb_backtest_nonzero_fill_run_count", 0) > 0
    and readiness.get("pair_arb_backtest_positive_nonzero_run_count", 0) >= 0
    and readiness.get("pair_arb_backtest_best_nonzero_pnl") is not None
    and oos_compare_ok
    and walkforward_ok
    and readiness.get("requires_live_fill_pnl_queue_position_proof_before_scale") is True
    and readiness.get("ready_for_explicit_g2_canary_approval") is False
    and readiness.get("requires_explicit_canary_approval") is True
    and readiness.get("requires_reviewed_effectful_executor_implementation") is True
    and readiness.get("requires_rust_shadow_strategy_acceptance") is False
    and readiness.get("requires_shadow_trading_acceptance") is True
    and readiness.get("requires_true_shadow_trading_pnl_residual_report") is True
    and readiness.get("requires_strategy_performance_shadow_or_dry_run") is True
    and readiness.get("requires_order_intent_surface") is True
    and readiness.get("requires_execution_controller") is True
    and readiness.get("requires_execution_controller_wiring") is True
    and readiness.get("requires_oms_adapter") is False
    and readiness.get("requires_runtime_execution_wiring") is False
    and readiness.get("requires_no_command_without_approval_gate") is False
    and readiness.get("requires_source_truth_runtime_account_gate") is False
    and readiness.get("requires_source_truth_event_feeds") is False
    and readiness.get("requires_exact_g2_canary_runbook") is False
    and readiness.get("requires_canary_acceptance_smoke") is False
    and readiness.get("requires_g2_canary_launch_plan") is False
    and readiness.get("requires_g2_canary_review_smoke") is False
    and readiness.get("requires_g2_canary_approval_envelope") is False
    and readiness.get("requires_g2_canary_launcher_refusal_gate") is False
    and readiness.get("requires_g2_canary_executor_contract") is False
    and readiness.get("requires_g2_canary_executor_preflight") is False
    and readiness.get("requires_g2_canary_executor_dry_run") is False
    and readiness.get("requires_g2_canary_executor_payload_manifest") is False
    and readiness.get("requires_g2_canary_effectful_executor_review_smoke") is False
    and readiness.get("requires_shadow_edge_samples_smoke") is False
    and readiness.get("requires_l1_dry_run_outcome_labels_smoke") is False
    and readiness.get("requires_l1_dry_run_outcome_labels") is False
    and readiness.get("requires_no_order_shadow_run_artifact_smoke") is False
    and readiness.get("requires_shadow_acceptance_input_discovery_smoke") is False
    and readiness.get("requires_real_shadow_acceptance_input") is False
    and readiness.get("requires_realized_outcome_labels_smoke") is False
    and readiness.get("requires_outcome_label_bridge_smoke") is False
    and readiness.get("requires_shadow_performance_evidence_smoke") is False
    and readiness.get("requires_shadow_trading_report_discovery_smoke") is False
    and readiness.get("requires_real_shadow_trading_report") is False
    and readiness.get("requires_shadow_trading_acceptance_smoke") is False
    and readiness.get("requires_rust_shadow_strategy_acceptance_smoke") is False
    and readiness.get("requires_rust_shadow_strategy_acceptance_runner_smoke") is False
    and readiness.get("requires_order_fill_truth_from_own_orders") is True
    and readiness.get("requires_redeem_cashflow_truth_from_own_activity") is True
    and readiness.get("requires_prefund_risk_envelope") is True
    and "any UNKNOWN order/fill/wallet/cashflow state stops the run" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "reviewed executor contract must pass before any execution path implementation" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "executor dry-run skeleton must refuse before SSH/sync/build/run side effects" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "executor payload manifest must allowlist sync files before implementation" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Observer edge samples must be extracted locally but rejected as standalone performance evidence" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "L1 dry-run outcome labels must pass local no-network smoke and real quality gates before acceptance" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "No-order shadow run artifact harness must emit explicit outcome label events" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Shadow acceptance input discovery must distinguish real run artifacts from fixture/smoke inputs" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Realized outcome-label producer must read only explicit no-order outcome events" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Outcome-label bridge must require independent realized no-order labels before performance evidence" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Shadow performance evidence smoke must pass before Rust strategy acceptance can be trusted" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Rust shadow/dry-run strategy acceptance smoke must pass before strategy acceptance can be trusted" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Rust shadow/dry-run strategy acceptance runner must refuse fixture publication" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Rust shadow/dry-run strategy acceptance must pass before any read-write canary" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "Shadow trading report discovery must find a non-fixture passive/passive aggregate_report before acceptance can pass" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and "D+ passive/passive shadow trading acceptance must report positive simulated pair PnL, pair coverage, and bounded residual risk before any read-write canary" in (
        data.get("g2_smoke_canary_minimum_conditions") or []
    )
    and all(value is False for value in side_effects.values())
    and all(value is False for value in forbidden.values())
    and "shadow trading acceptance failed" in data.get("next_gate", "")
    and (
        "retrain/tune strategy" in data.get("next_gate", "")
        or "compliant declared strict/cache/completion data" in data.get("next_gate", "")
        or "compliant adapter/join" in data.get("next_gate", "")
    )
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_canary_readiness_plan_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_canary_readiness_plan_gate",
  "plan_manifest": "$plan_manifest",
  "orders_sent": false,
  "auth_network_started": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
