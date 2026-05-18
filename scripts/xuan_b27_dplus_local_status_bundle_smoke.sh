#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_local_status_bundle_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
bundle_dir="$out_dir/status_bundle_run"
bundle_manifest="$bundle_dir/manifest.json"
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

script="scripts/xuan_b27_dplus_local_status_bundle.py"

run_check "status_bundle_script_exists" test -x "$script"
run_check "status_bundle_py_compile" python3 -m py_compile "$script"
run_check "status_bundle_has_no_network_or_process_control_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http)[[:space:]]+import|systemctl|run_shared_ingress|PM_SHARED_INGRESS_ROLE.*broker|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "status_bundle_run" "$script" --output-dir "$bundle_dir"
run_check "status_bundle_manifest_exists" test -f "$bundle_manifest"
run_check "status_bundle_manifest_is_safe" python3 - "$bundle_manifest" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
allowed_verdicts = {
    "BLOCKED_HEARTBEAT_NOT_ACTIVE",
    "BLOCKED_LOCAL_READINESS",
    "BLOCKED_USER_WS_AUTH_SOURCE",
    "READY_FOR_APPROVAL_WAITING_FOR_SHARED_INGRESS_BROKER",
    "READY_FOR_EXPLICIT_READONLY_USER_WS_APPROVAL",
}
side_effects = data.get("side_effects", {})
ec2_attempt = data.get("latest_ec2_readonly_user_ws_attempt", {})
ec2_sync = data.get("latest_ec2_entrypoint_sync", {})
ec2_resync_rebuild = data.get("latest_ec2_resync_rebuild", {})
ec2_observer = data.get("latest_ec2_readonly_user_ws_observer", {})
ec2_diagnostic = data.get("latest_ec2_readonly_user_ws_diagnostic", {})
ec2_diagnostic_smoke = data.get("ec2_readonly_diagnostic_smoke", {})
ec2_acceptance = data.get("latest_ec2_readonly_user_ws_acceptance", {})
ec2_acceptance_smoke = data.get("ec2_30m_acceptance_smoke", {})
postmortem = data.get("latest_readonly_user_ws_postmortem", {})
resync_plan = data.get("ec2_resync_diagnostic_plan_smoke", {})
acceptance_30m_plan = data.get("ec2_30m_acceptance_plan_smoke", {})
canary_plan = data.get("canary_readiness_plan_smoke", {})
g2_review = data.get("g2_canary_review_smoke", {})
g2_approval = data.get("g2_canary_approval_envelope_smoke", {})
g2_launcher = data.get("g2_canary_launcher_refusal_smoke", {})
g2_executor_contract = data.get("g2_canary_executor_contract_smoke", {})
g2_executor = data.get("g2_canary_executor_smoke", {})
g2_executor_dry_run = data.get("g2_canary_executor_dry_run_smoke", {})
g2_executor_payload = data.get("g2_canary_executor_payload_manifest_smoke", {})
g2_effectful_review = data.get("g2_canary_effectful_executor_review_smoke", {})
shadow_edge_smoke = data.get("shadow_edge_samples_smoke", {})
l1_labels_smoke = data.get("l1_dry_run_outcome_labels_smoke", {})
l1_labels = data.get("l1_dry_run_outcome_labels", {})
no_order_shadow_run_smoke = data.get("no_order_shadow_run_artifact_smoke", {})
shadow_acceptance_discovery_smoke = data.get("shadow_acceptance_input_discovery_smoke", {})
shadow_acceptance_discovery = data.get("shadow_acceptance_input_discovery", {})
realized_labels_smoke = data.get("realized_outcome_labels_smoke", {})
outcome_bridge_smoke = data.get("outcome_label_bridge_smoke", {})
shadow_performance_smoke = data.get("shadow_performance_evidence_smoke", {})
rust_shadow_acceptance_smoke = data.get("rust_shadow_strategy_acceptance_smoke", {})
rust_shadow_acceptance_runner_smoke = data.get(
    "rust_shadow_strategy_acceptance_runner_smoke", {}
)
rust_shadow_acceptance = data.get("rust_shadow_strategy_acceptance", {})
canary_plan_oos_status = canary_plan.get("pair_arb_oos_compare_status")
canary_plan_oos_ok = (
    (
        canary_plan_oos_status == "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS"
        and canary_plan.get("pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and canary_plan.get("pair_arb_oos_compare_qualified_both_positive_config_count", 0) > 0
        and canary_plan.get("pair_arb_oos_compare_validation_passed") is True
        and canary_plan.get("requires_strategy_retrain_or_positive_backtest_config") is False
    )
    or (
        canary_plan_oos_status == "FAIL_OOS_POSITIVE_CONFIGS_BELOW_SAMPLE_OR_SIZE"
        and canary_plan.get("pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and canary_plan.get("pair_arb_oos_compare_qualified_both_positive_config_count") == 0
        and canary_plan.get("pair_arb_oos_compare_validation_passed") is False
        and canary_plan.get("requires_strategy_retrain_or_positive_backtest_config") is True
    )
    or (
        canary_plan_oos_status == "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
        and canary_plan.get("pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and canary_plan.get("pair_arb_oos_compare_qualified_both_positive_config_count", 0) > 0
        and canary_plan.get("pair_arb_oos_compare_numeric_validation_passed") is True
        and canary_plan.get("pair_arb_oos_compare_validation_passed") is False
        and canary_plan.get("pair_arb_oos_compare_requires_compliant_dataset") is True
        and canary_plan.get("requires_strategy_retrain_or_positive_backtest_config") is True
    )
)
bundle_oos_status = data.get("canary_pair_arb_oos_compare_status")
bundle_oos_ok = (
    (
        bundle_oos_status == "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS"
        and data.get("canary_pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and data.get("canary_pair_arb_oos_compare_qualified_both_positive_config_count", 0) > 0
        and data.get("canary_pair_arb_oos_compare_validation_passed") is True
        and data.get("canary_requires_strategy_retrain_or_positive_backtest_config") is False
    )
    or (
        bundle_oos_status == "FAIL_OOS_POSITIVE_CONFIGS_BELOW_SAMPLE_OR_SIZE"
        and data.get("canary_pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and data.get("canary_pair_arb_oos_compare_qualified_both_positive_config_count") == 0
        and data.get("canary_pair_arb_oos_compare_validation_passed") is False
        and data.get("canary_requires_strategy_retrain_or_positive_backtest_config") is True
    )
    or (
        bundle_oos_status == "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
        and data.get("canary_pair_arb_oos_compare_both_positive_config_count", 0) > 0
        and data.get("canary_pair_arb_oos_compare_qualified_both_positive_config_count", 0) > 0
        and data.get("canary_pair_arb_oos_compare_numeric_validation_passed") is True
        and data.get("canary_pair_arb_oos_compare_validation_passed") is False
        and data.get("canary_pair_arb_oos_compare_requires_compliant_dataset") is True
        and data.get("canary_requires_strategy_retrain_or_positive_backtest_config") is True
    )
)
canary_plan_walkforward_status = canary_plan.get("pair_arb_walkforward_compare_status")
canary_plan_walkforward_ok = (
    (
        canary_plan_walkforward_status == "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS"
        and canary_plan.get("pair_arb_walkforward_compare_split_count", 0) >= 4
        and canary_plan.get("pair_arb_walkforward_compare_all_positive_config_count", 0) > 0
        and canary_plan.get(
            "pair_arb_walkforward_compare_qualified_all_positive_config_count", 0
        ) > 0
        and canary_plan.get("pair_arb_walkforward_compare_validation_passed") is True
    )
    or (
        canary_plan_walkforward_status
        == "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
        and canary_plan.get("pair_arb_walkforward_compare_split_count", 0) >= 4
        and canary_plan.get("pair_arb_walkforward_compare_all_positive_config_count", 0) > 0
        and canary_plan.get(
            "pair_arb_walkforward_compare_qualified_all_positive_config_count", 0
        ) > 0
        and canary_plan.get("pair_arb_walkforward_compare_numeric_validation_passed") is True
        and canary_plan.get("pair_arb_walkforward_compare_validation_passed") is False
        and canary_plan.get("pair_arb_walkforward_compare_requires_compliant_dataset") is True
        and canary_plan.get("requires_strategy_retrain_or_positive_backtest_config") is True
    )
)
bundle_walkforward_status = data.get("canary_pair_arb_walkforward_compare_status")
bundle_walkforward_ok = (
    (
        bundle_walkforward_status == "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS"
        and data.get("canary_pair_arb_walkforward_compare_split_count", 0) >= 4
        and data.get("canary_pair_arb_walkforward_compare_all_positive_config_count", 0) > 0
        and data.get(
            "canary_pair_arb_walkforward_compare_qualified_all_positive_config_count", 0
        ) > 0
        and data.get("canary_pair_arb_walkforward_compare_validation_passed") is True
    )
    or (
        bundle_walkforward_status
        == "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
        and data.get("canary_pair_arb_walkforward_compare_split_count", 0) >= 4
        and data.get("canary_pair_arb_walkforward_compare_all_positive_config_count", 0) > 0
        and data.get(
            "canary_pair_arb_walkforward_compare_qualified_all_positive_config_count", 0
        ) > 0
        and data.get("canary_pair_arb_walkforward_compare_numeric_validation_passed") is True
        and data.get("canary_pair_arb_walkforward_compare_validation_passed") is False
        and data.get("canary_pair_arb_walkforward_compare_requires_compliant_dataset") is True
        and data.get("canary_requires_strategy_retrain_or_positive_backtest_config") is True
    )
)
ec2_attempt_status = ec2_attempt.get("status")
ec2_status = data.get("ec2_readonly_user_ws_status")
canary_status = data.get("canary_readiness_status")
ec2_closed_missing_entrypoint_ok = (
    ec2_attempt_status == "FAILED_CLOSED_REMOTE_OBSERVER_ENTRYPOINT_MISSING"
    and ec2_status == "WAITING_FOR_EC2_OBSERVER_ENTRYPOINT"
    and ec2_attempt.get("required_remote_entrypoint_found") is False
    and ec2_attempt.get("side_effects_all_false") is True
    and ec2_attempt.get("forbidden_side_effects_absent") is True
    and isinstance(data.get("ec2_next_gate"), str)
    and "deployment/sync" in data.get("ec2_next_gate")
)
ec2_ready_after_sync_ok = (
    ec2_attempt_status == "FAILED_CLOSED_REMOTE_OBSERVER_ENTRYPOINT_MISSING"
    and ec2_status == "READY_FOR_EXPLICIT_EC2_READONLY_USER_WS_APPROVAL"
    and ec2_attempt.get("required_remote_entrypoint_found") is False
    and ec2_attempt.get("forbidden_side_effects_absent") is True
    and ec2_sync.get("status") == "PASS_ENTRYPOINT_SYNC_READY"
    and ec2_sync.get("observer_binary_exists") is True
    and isinstance(ec2_sync.get("observer_binary_sha256"), str)
    and len(ec2_sync.get("observer_binary_sha256")) == 64
    and ec2_sync.get("broker_preflight_status") == "OK"
    and ec2_sync.get("entrypoint_manifest_status") == "PASS"
    and ec2_sync.get("entrypoint_expected_executable_count", 0) >= 4
    and ec2_sync.get("entrypoint_missing_executable_files") == []
    and ec2_sync.get("local_forbidden_path_hits") == []
    and ec2_sync.get("created_isolated_worktree") is True
    and ec2_sync.get("synced_entrypoint_files") is True
    and ec2_sync.get("built_observer_binary") is True
    and ec2_sync.get("forbidden_side_effects_absent") is True
    and isinstance(data.get("ec2_next_gate"), str)
    and "explicit exact EC2 read-only User WS observer run approval" in data.get("ec2_next_gate")
)
ec2_observer_pass_ok = (
    ec2_attempt_status == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_status == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_attempt.get("forbidden_side_effects_absent") is True
    and ec2_attempt.get("observer_started") is True
    and ec2_attempt.get("user_ws_started") is True
    and ec2_attempt.get("orders_sent") is False
    and ec2_attempt.get("cancels_sent") is False
    and ec2_attempt.get("redeems_sent") is False
)
ec2_observer_fail_ok = (
    ec2_attempt_status == "FAIL_READONLY_USER_WS_OBSERVER"
    and ec2_status == "BLOCKED_EC2_READONLY_USER_WS_OBSERVER"
    and ec2_attempt.get("forbidden_side_effects_absent") is True
    and ec2_attempt.get("orders_sent") is False
    and ec2_attempt.get("cancels_sent") is False
    and ec2_attempt.get("redeems_sent") is False
)
ec2_actual_observer_fail_ok = (
    ec2_status == "BLOCKED_EC2_READONLY_USER_WS_OBSERVER"
    and ec2_observer.get("exists") is True
    and ec2_observer.get("artifact") == "xuan_b27_dplus_ec2_readonly_user_ws_local_acceptance_review"
    and ec2_observer.get("status") in {
        "FAIL_READONLY_USER_WS_OBSERVER",
        "FAIL_NO_USER_WS_RECORDS",
        "FAIL_READONLY_VIOLATION",
        "FAIL_RECORDER_DECODE_ERROR",
        "FAIL_SUMMARY_GATE",
        "FAIL_PROCESS_EXIT_NONZERO",
    }
    and ec2_observer.get("acceptance_passed") is False
    and ec2_observer.get("orders_possible") is False
    and ec2_observer.get("forbidden_event_count") == 0
    and ec2_observer.get("secret_values_recorded") is False
    and ec2_observer.get("secret_values_written_to_disk") is False
    and ec2_observer.get("broker_started_or_modified") is False
    and ec2_observer.get("systemd_or_service_control_used") is False
    and ec2_observer.get("order_cancel_redeem_allowed") is False
)
ec2_actual_observer_pass_ok = (
    ec2_status == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_observer.get("exists") is True
    and ec2_observer.get("artifact") == "xuan_b27_dplus_ec2_readonly_user_ws_local_acceptance_review"
    and ec2_observer.get("status") == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_observer.get("acceptance_passed") is True
    and ec2_observer.get("orders_possible") is False
    and ec2_observer.get("forbidden_event_count") == 0
    and ec2_observer.get("secret_values_recorded") is False
    and ec2_observer.get("secret_values_written_to_disk") is False
    and ec2_observer.get("broker_started_or_modified") is False
    and ec2_observer.get("systemd_or_service_control_used") is False
    and ec2_observer.get("order_cancel_redeem_allowed") is False
)
ec2_actual_diagnostic_pass_ok = (
    ec2_status == "PASS_READONLY_USER_WS_DIAGNOSTIC"
    and ec2_diagnostic.get("exists") is True
    and ec2_diagnostic.get("artifact") == "xuan_b27_dplus_ec2_readonly_user_ws_diagnostic_local_acceptance_review"
    and ec2_diagnostic.get("status") == "PASS_READONLY_USER_WS_DIAGNOSTIC"
    and ec2_diagnostic.get("acceptance_passed") is True
    and ec2_diagnostic.get("wrapper_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_diagnostic.get("wrapper_return_code") == 0
    and ec2_diagnostic.get("wrapper_summary_return_code") == 0
    and ec2_diagnostic.get("run_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_diagnostic.get("summary_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_diagnostic.get("gate_status") == "PASS"
    and ec2_diagnostic.get("gate_failures") == []
    and ec2_diagnostic.get("user_ws_connected_count", 0) > 0
    and ec2_diagnostic.get("user_ws_subscribe_sent_count", 0) > 0
    and ec2_diagnostic.get("user_ws_raw_count", 0) > 0
    and ec2_diagnostic.get("user_ws_error_event_count") == 0
    and ec2_diagnostic.get("forbidden_event_count") == 0
    and ec2_diagnostic.get("event_decode_error_count") == 0
    and ec2_diagnostic.get("user_ws_decode_error_count") == 0
    and ec2_diagnostic.get("orders_possible") is False
    and ec2_diagnostic.get("recorder_critical_drop_count") == 0
    and ec2_diagnostic.get("secret_values_recorded") is False
    and ec2_diagnostic.get("secret_values_written_to_disk") is False
    and ec2_diagnostic.get("broker_started_or_modified") is False
    and ec2_diagnostic.get("systemd_or_service_control_used") is False
    and ec2_diagnostic.get("order_cancel_redeem_allowed") is False
    and ec2_diagnostic.get("started_observer") is True
    and ec2_diagnostic.get("started_user_ws") is True
    and ec2_diagnostic.get("started_broker") is False
    and ec2_diagnostic.get("orders_sent") is False
    and ec2_diagnostic.get("cancels_sent") is False
    and ec2_diagnostic.get("redeems_sent") is False
    and ec2_diagnostic.get("modified_shared_ingress") is False
    and ec2_diagnostic.get("env_files_written") is False
)
ec2_actual_acceptance_pass_ok = (
    ec2_status == "PASS_READONLY_USER_WS_ACCEPTANCE"
    and ec2_acceptance.get("exists") is True
    and ec2_acceptance.get("artifact") == "xuan_b27_dplus_ec2_readonly_user_ws_acceptance_local_acceptance_review"
    and ec2_acceptance.get("status") == "PASS_READONLY_USER_WS_ACCEPTANCE"
    and ec2_acceptance.get("acceptance_passed") is True
    and ec2_acceptance.get("wrapper_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_acceptance.get("wrapper_return_code") == 0
    and ec2_acceptance.get("wrapper_summary_return_code") == 0
    and ec2_acceptance.get("run_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_acceptance.get("summary_status") == "PASS_READONLY_USER_WS_OBSERVER"
    and ec2_acceptance.get("gate_status") == "PASS"
    and ec2_acceptance.get("gate_failures") == []
    and ec2_acceptance.get("duration_secs") == 1800
    and ec2_acceptance.get("elapsed_ms", 0) >= 1790000
    and ec2_acceptance.get("shared_ingress_status") == "OK"
    and ec2_acceptance.get("broker_preflight_status") == "OK"
    and ec2_acceptance.get("user_ws_connected_count", 0) > 0
    and ec2_acceptance.get("user_ws_subscribe_sent_count", 0) > 0
    and ec2_acceptance.get("user_ws_raw_count", 0) > 0
    and ec2_acceptance.get("user_ws_error_event_count") == 0
    and ec2_acceptance.get("forbidden_event_count") == 0
    and ec2_acceptance.get("event_decode_error_count") == 0
    and ec2_acceptance.get("user_ws_decode_error_count") == 0
    and ec2_acceptance.get("orders_possible") is False
    and ec2_acceptance.get("recorder_critical_drop_count") == 0
    and ec2_acceptance.get("secret_values_recorded") is False
    and ec2_acceptance.get("secret_values_written_to_disk") is False
    and ec2_acceptance.get("broker_started_or_modified") is False
    and ec2_acceptance.get("systemd_or_service_control_used") is False
    and ec2_acceptance.get("order_cancel_redeem_allowed") is False
    and ec2_acceptance.get("started_observer") is True
    and ec2_acceptance.get("started_user_ws") is True
    and ec2_acceptance.get("started_broker") is False
    and ec2_acceptance.get("orders_sent") is False
    and ec2_acceptance.get("cancels_sent") is False
    and ec2_acceptance.get("redeems_sent") is False
    and ec2_acceptance.get("modified_shared_ingress") is False
    and ec2_acceptance.get("env_files_written") is False
)
ec2_attempt_state_ok = (
    ec2_closed_missing_entrypoint_ok
    or ec2_ready_after_sync_ok
    or ec2_observer_pass_ok
    or ec2_observer_fail_ok
    or ec2_actual_observer_pass_ok
    or ec2_actual_observer_fail_ok
    or ec2_actual_diagnostic_pass_ok
    or ec2_actual_acceptance_pass_ok
)
postmortem_ok = (
    not postmortem.get("exists")
    or (
        postmortem.get("artifact") == "xuan_b27_dplus_readonly_user_ws_postmortem"
        and postmortem.get("status") == "PASS"
        and postmortem.get("scope") == "local_no_network_existing_ec2_artifact_reclassification"
        and postmortem.get("summary_output_exists") is True
        and postmortem.get("summary_return_code") in {0, 2, 3, 4}
        and postmortem.get("gate_status") in {
            "PASS",
            "FAIL_READONLY_VIOLATION",
            "FAIL_RECORDER_DECODE_ERROR",
            "FAIL_USER_WS_TASK",
            "FAIL_USER_WS_ERROR_EVENT",
            "FAIL_NO_USER_WS_CONNECTION",
            "FAIL_NO_USER_WS_RECORDS",
        }
        and postmortem.get("observer_started") is False
        and postmortem.get("user_ws_started") is False
        and postmortem.get("network_started") is False
        and postmortem.get("orders_sent") is False
        and postmortem.get("cancels_sent") is False
        and postmortem.get("redeems_sent") is False
    )
)
resync_plan_ok = (
    resync_plan.get("exists") is True
    and resync_plan.get("artifact") == "xuan_b27_dplus_ec2_resync_diagnostic_plan_smoke"
    and resync_plan.get("status") == "PASS"
    and resync_plan.get("scope") == "local_no_network_ec2_resync_diagnostic_plan_gate"
    and resync_plan.get("orders_sent") is False
    and resync_plan.get("auth_network_started") is False
    and resync_plan.get("plan_manifest_exists") is True
    and resync_plan.get("plan_status") == "READY_FOR_EXPLICIT_SYNC_AND_DIAGNOSTIC_APPROVAL"
    and resync_plan.get("local_gates_ok") is True
    and resync_plan.get("stale_sync_confirmed") is True
    and resync_plan.get("requires_explicit_resync_rebuild_approval") is True
    and resync_plan.get("requires_explicit_exact_run_approval") is True
    and str(resync_plan.get("proposed_remote_worktree", "")).startswith(
        "/home/ubuntu/xuan_research_runs/xuan_research_"
    )
    and resync_plan.get("shared_ingress_root") == "/srv/pm_as_ofi/shared-ingress-main"
)
acceptance_30m_plan_ok = (
    acceptance_30m_plan.get("exists") is True
    and acceptance_30m_plan.get("artifact") == "xuan_b27_dplus_ec2_30m_acceptance_plan_smoke"
    and acceptance_30m_plan.get("status") == "PASS"
    and acceptance_30m_plan.get("scope") == "local_no_network_ec2_30m_acceptance_plan_gate"
    and acceptance_30m_plan.get("orders_sent") is False
    and acceptance_30m_plan.get("auth_network_started") is False
    and acceptance_30m_plan.get("plan_manifest_exists") is True
    and acceptance_30m_plan.get("plan_status") == "READY_FOR_EXPLICIT_EC2_30M_ACCEPTANCE_APPROVAL"
    and acceptance_30m_plan.get("diagnostic_passed") is True
    and acceptance_30m_plan.get("resync_ready") is True
    and acceptance_30m_plan.get("local_gates_ok") is True
    and acceptance_30m_plan.get("requires_explicit_exact_run_approval") is True
    and str(acceptance_30m_plan.get("remote_worktree", "")).startswith(
        "/home/ubuntu/xuan_research_runs/xuan_research_"
    )
    and str(acceptance_30m_plan.get("remote_run_dir", "")).startswith(
        "/home/ubuntu/xuan_research_runs/xuan_research_"
    )
    and acceptance_30m_plan.get("duration_seconds") == 1800
    and acceptance_30m_plan.get("market_slug") == "btc-updown-5m"
    and acceptance_30m_plan.get("shared_ingress_root") == "/srv/pm_as_ofi/shared-ingress-main"
)
canary_plan_ok = (
    canary_plan.get("exists") is True
    and canary_plan.get("artifact") == "xuan_b27_dplus_canary_readiness_plan_smoke"
    and canary_plan.get("status") == "PASS"
    and canary_plan.get("scope") == "local_no_network_canary_readiness_plan_gate"
    and canary_plan.get("orders_sent") is False
    and canary_plan.get("auth_network_started") is False
    and canary_plan.get("plan_manifest_exists") is True
    and canary_status == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
    and canary_plan.get("plan_status") == "CANARY_NOT_READY_SHADOW_TRADING_ACCEPTANCE_FAILED"
    and canary_plan.get("read_only_acceptance_passed") is True
    and canary_plan.get("local_gates_ok") is True
    and canary_plan.get("source_truth_schema_ready") is True
    and canary_plan.get("observer_safety_ready") is True
    and canary_plan.get("order_plan_module_registered") is True
    and canary_plan.get("order_plan_smoke_passed") is True
    and canary_plan.get("execution_controller_module_registered") is True
    and canary_plan.get("execution_controller_smoke_passed") is True
    and canary_plan.get("oms_adapter_module_registered") is True
    and canary_plan.get("oms_adapter_smoke_passed") is True
    and canary_plan.get("runtime_wiring_smoke_passed") is True
    and canary_plan.get("source_truth_runtime_gate_smoke_passed") is True
    and canary_plan.get("source_truth_event_type_registered") is True
    and canary_plan.get("executor_order_truth_feed_wired") is True
    and canary_plan.get("user_ws_fill_truth_feed_wired") is True
    and canary_plan.get("coordinator_source_truth_channel_wired") is True
    and canary_plan.get("main_runtime_source_truth_channel_wired") is True
    and canary_plan.get("source_truth_event_feed_tests_present") is True
    and canary_plan.get("wallet_runtime_producer_wired") is True
    and canary_plan.get("redeem_runtime_producer_wired") is True
    and canary_plan.get("cashflow_runtime_producer_wired") is True
    and canary_plan.get("wallet_redeem_cashflow_runtime_producers_wired") is True
    and canary_plan.get("redeem_truth_runtime_confirmation_ready") is True
    and canary_plan.get("strategy_returns_empty_quotes") is True
    and canary_plan.get("observer_payload_quotes_forced_empty") is True
    and canary_plan.get("observer_order_controller_implemented_false") is True
    and canary_plan.get("canary_blocked_event_present") is True
    and canary_plan.get("canary_block_reason_not_implemented") is True
    and canary_plan.get("canary_run_ready") is False
    and canary_plan.get("ready_to_execute") is False
    and canary_plan.get("canary_run_authorized") is False
    and canary_plan.get("effectful_executor_implemented") is False
    and canary_plan.get("execution_readiness_status") == "NOT_READY_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and canary_plan.get("shadow_strategy_evidence_kind") == "NO_ORDER_LOCAL_L1_TOUCH_DRY_RUN"
    and canary_plan.get("shadow_strategy_evidence_basis") == "dry_run_outcome"
    and canary_plan.get("shadow_strategy_evidence_no_order") is True
    and canary_plan.get("shadow_strategy_evidence_live_fill_pnl_queue_position_proof") is False
    and canary_plan.get("requires_live_fill_pnl_queue_position_proof_before_scale") is True
    and canary_plan.get("shadow_strategy_evidence", {}).get("live_fill_pnl_queue_position_proof") is False
    and "live fill quality" in (canary_plan.get("shadow_strategy_evidence", {}).get("does_not_prove") or [])
    and canary_plan.get("shadow_trading_acceptance_smoke_ready") is True
    and canary_plan.get("shadow_trading_acceptance_ready") is False
    and canary_plan.get("shadow_trading_report_discovery_smoke_ready") is True
    and canary_plan.get("shadow_trading_report_discovery_ready") is True
    and canary_plan.get("shadow_trading_report_discovery_status") == "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
    and canary_plan.get("shadow_trading_report_discovery_real_candidate_count", 0) >= 1
    and canary_plan.get("shadow_trading_report_discovery_valid_real_candidate_count", 0) >= 1
    and canary_plan.get("shadow_trading_evidence_kind") == "SIMULATED_SHADOW_TRADING_REPORT"
    and canary_plan.get("shadow_trading_evidence_basis") == "xuan_dplus_passive_passive_shadow_runner.py aggregate_report"
    and canary_plan.get("shadow_trading_evidence", {}).get("accepted") is False
    and canary_plan.get("shadow_trading_evidence", {}).get("required_before_g2_canary") is True
    and canary_plan.get("pair_arb_backtest_status")
    in {
        "FAIL_NO_POSITIVE_BACKTEST_CONFIG",
        "PASS_OOS_POSITIVE_BACKTEST_CONFIG",
        "PASS_OOS_POSITIVE_BACKTEST_CONFIG_DATASET_SCOPE_LIMITED",
    }
    and canary_plan.get("pair_arb_backtest_run_count", 0) >= 1
    and canary_plan.get("pair_arb_backtest_nonzero_fill_run_count", 0) > 0
    and canary_plan.get("pair_arb_backtest_positive_nonzero_run_count", 0) >= 0
    and canary_plan.get("pair_arb_backtest_best_nonzero_pnl") is not None
    and canary_plan_oos_ok
    and canary_plan_walkforward_ok
    and canary_plan.get("order_intent_preview_surface_ready") is True
    and canary_plan.get("execution_controller_decision_ready") is True
    and canary_plan.get("oms_adapter_surface_ready") is True
    and canary_plan.get("runtime_sender_wiring_ready") is True
    and canary_plan.get("source_truth_runtime_gate_ready") is True
    and canary_plan.get("order_fill_source_truth_event_feeds_ready") is True
    and canary_plan.get("wallet_redeem_cashflow_event_feeds_ready") is True
    and canary_plan.get("g2_canary_runbook_ready") is True
    and canary_plan.get("g2_canary_acceptance_smoke_ready") is True
    and canary_plan.get("g2_canary_launch_plan_ready") is True
    and canary_plan.get("g2_canary_review_smoke_ready") is True
    and canary_plan.get("g2_canary_approval_envelope_smoke_ready") is True
    and canary_plan.get("g2_canary_launcher_refusal_smoke_ready") is True
    and canary_plan.get("g2_canary_executor_contract_smoke_ready") is True
    and canary_plan.get("g2_canary_executor_smoke_ready") is True
    and canary_plan.get("g2_canary_executor_dry_run_smoke_ready") is True
    and canary_plan.get("g2_canary_executor_payload_manifest_smoke_ready") is True
    and canary_plan.get("g2_canary_effectful_executor_review_smoke_ready") is True
    and canary_plan.get("shadow_edge_samples_smoke_ready") is True
    and canary_plan.get("l1_dry_run_outcome_labels_smoke_ready") is True
    and canary_plan.get("l1_dry_run_outcome_labels_ready") is True
    and canary_plan.get("no_order_shadow_run_artifact_smoke_ready") is True
    and canary_plan.get("shadow_acceptance_input_discovery_smoke_ready") is True
    and canary_plan.get("shadow_acceptance_input_ready") is True
    and canary_plan.get("realized_outcome_labels_smoke_ready") is True
    and canary_plan.get("outcome_label_bridge_smoke_ready") is True
    and canary_plan.get("shadow_performance_evidence_smoke_ready") is True
    and canary_plan.get("rust_shadow_strategy_acceptance_smoke_ready") is True
    and canary_plan.get("rust_shadow_strategy_acceptance_runner_smoke_ready") is True
    and canary_plan.get("pre_canary_plumbing_ready") is True
    and canary_plan.get("rust_shadow_strategy_acceptance_ready") is True
    and canary_plan.get("ready_for_explicit_g2_canary_approval") is False
    and canary_plan.get("requires_explicit_canary_approval") is True
    and canary_plan.get("requires_reviewed_effectful_executor_implementation") is True
    and canary_plan.get("requires_rust_shadow_strategy_acceptance") is False
    and canary_plan.get("requires_shadow_trading_acceptance") is True
    and canary_plan.get("requires_true_shadow_trading_pnl_residual_report") is True
    and canary_plan.get("requires_strategy_performance_shadow_or_dry_run") is True
    and canary_plan.get("requires_order_intent_surface") is True
    and canary_plan.get("requires_execution_controller") is True
    and canary_plan.get("requires_execution_controller_wiring") is True
    and canary_plan.get("requires_oms_adapter") is False
    and canary_plan.get("requires_runtime_execution_wiring") is False
    and canary_plan.get("requires_no_command_without_approval_gate") is False
    and canary_plan.get("requires_source_truth_runtime_account_gate") is False
    and canary_plan.get("requires_source_truth_event_feeds") is False
    and canary_plan.get("requires_exact_g2_canary_runbook") is False
    and canary_plan.get("requires_canary_acceptance_smoke") is False
    and canary_plan.get("requires_g2_canary_launch_plan") is False
    and canary_plan.get("requires_g2_canary_review_smoke") is False
    and canary_plan.get("requires_g2_canary_approval_envelope") is False
    and canary_plan.get("requires_g2_canary_launcher_refusal_gate") is False
    and canary_plan.get("requires_g2_canary_executor_contract") is False
    and canary_plan.get("requires_g2_canary_executor_preflight") is False
    and canary_plan.get("requires_g2_canary_executor_dry_run") is False
    and canary_plan.get("requires_g2_canary_executor_payload_manifest") is False
    and canary_plan.get("requires_g2_canary_effectful_executor_review_smoke") is False
    and canary_plan.get("requires_shadow_edge_samples_smoke") is False
    and canary_plan.get("requires_l1_dry_run_outcome_labels_smoke") is False
    and canary_plan.get("requires_l1_dry_run_outcome_labels") is False
    and canary_plan.get("requires_no_order_shadow_run_artifact_smoke") is False
    and canary_plan.get("requires_shadow_acceptance_input_discovery_smoke") is False
    and canary_plan.get("requires_real_shadow_acceptance_input") is False
    and canary_plan.get("requires_realized_outcome_labels_smoke") is False
    and canary_plan.get("requires_outcome_label_bridge_smoke") is False
    and canary_plan.get("requires_shadow_performance_evidence_smoke") is False
    and canary_plan.get("requires_shadow_trading_report_discovery_smoke") is False
    and canary_plan.get("requires_real_shadow_trading_report") is False
    and canary_plan.get("requires_shadow_trading_acceptance_smoke") is False
    and canary_plan.get("requires_rust_shadow_strategy_acceptance_smoke") is False
    and canary_plan.get("requires_rust_shadow_strategy_acceptance_runner_smoke") is False
    and canary_plan.get("requires_order_fill_truth_from_own_orders") is True
    and canary_plan.get("requires_redeem_cashflow_truth_from_own_activity") is True
    and isinstance(data.get("canary_next_gate"), str)
    and "shadow trading acceptance failed" in data.get("canary_next_gate")
    and (
        "retrain/tune strategy" in data.get("canary_next_gate")
        or "compliant declared strict/cache/completion data" in data.get("canary_next_gate")
        or "compliant adapter/join" in data.get("canary_next_gate")
    )
)
ok = (
    data.get("artifact") == "xuan_b27_dplus_local_status_bundle"
    and data.get("scope") == "local_no_network_status_bundle"
    and data.get("verdict") in allowed_verdicts
    and ec2_status in {
        "NO_EC2_READONLY_USER_WS_ATTEMPT",
        "WAITING_FOR_EC2_OBSERVER_ENTRYPOINT",
        "READY_FOR_EXPLICIT_EC2_READONLY_USER_WS_APPROVAL",
        "PASS_READONLY_USER_WS_OBSERVER",
        "PASS_READONLY_USER_WS_DIAGNOSTIC",
        "PASS_READONLY_USER_WS_ACCEPTANCE",
        "BLOCKED_EC2_READONLY_USER_WS_OBSERVER",
    }
    and isinstance(data.get("ec2_next_gate"), str)
    and bool(data.get("ec2_next_gate"))
    and isinstance(data.get("canary_next_gate"), str)
    and bool(data.get("canary_next_gate"))
    and data.get("canary_run_ready") is False
    and data.get("canary_ready_to_execute") is False
    and data.get("canary_effectful_executor_implemented") is False
    and data.get("canary_execution_readiness_status") == "NOT_READY_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and data.get("canary_shadow_strategy_evidence_kind") == "NO_ORDER_LOCAL_L1_TOUCH_DRY_RUN"
    and data.get("canary_shadow_strategy_evidence_basis") == "dry_run_outcome"
    and data.get("canary_shadow_strategy_evidence_no_order") is True
    and data.get("canary_shadow_strategy_evidence_live_fill_pnl_queue_position_proof") is False
    and data.get("canary_shadow_trading_acceptance_ready") is False
    and data.get("canary_shadow_trading_report_discovery_status") == "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
    and data.get("canary_shadow_trading_report_discovery_ready") is True
    and data.get("canary_shadow_trading_report_discovery_real_candidate_count", 0) >= 1
    and data.get("canary_shadow_trading_report_discovery_valid_real_candidate_count", 0) >= 1
    and data.get("canary_shadow_trading_evidence_kind") == "SIMULATED_SHADOW_TRADING_REPORT"
    and data.get("canary_shadow_trading_evidence_basis") == "xuan_dplus_passive_passive_shadow_runner.py aggregate_report"
    and data.get("canary_pair_arb_backtest_status")
    in {
        "FAIL_NO_POSITIVE_BACKTEST_CONFIG",
        "PASS_OOS_POSITIVE_BACKTEST_CONFIG",
        "PASS_OOS_POSITIVE_BACKTEST_CONFIG_DATASET_SCOPE_LIMITED",
    }
    and data.get("canary_pair_arb_backtest_run_count", 0) >= 1
    and data.get("canary_pair_arb_backtest_nonzero_fill_run_count", 0) > 0
    and data.get("canary_pair_arb_backtest_positive_nonzero_run_count", 0) >= 0
    and data.get("canary_pair_arb_backtest_best_nonzero_pnl") is not None
    and bundle_oos_ok
    and bundle_walkforward_ok
    and data.get("backtest_report_scope_audit_status") == "PASS_BACKTEST_REPORT_SCOPE_AUDIT"
    and data.get("backtest_report_scope_audit_passed") is True
    and data.get("backtest_report_scope_audit_manifest_count", 0) >= 1
    and data.get("backtest_report_scope_limited_report_count", 0) >= 1
    and data.get("backtest_report_scope_promotion_supported_report_count") == 0
    and data.get("backtest_report_scope_requires_compliant_dataset") is True
    and data.get("completion_store_schema_probe_status") == "PASS_SCOPE_LIMITED_COMPLETION_STORE_SCHEMA_PROBE"
    and data.get("completion_store_schema_probe_passed") is True
    and data.get("completion_store_schema_probe_row_count", 0) > 0
    and data.get("completion_store_schema_probe_market_count_sum_by_day", 0) > 0
    and data.get("completion_store_schema_probe_can_support_promotion") is False
    and data.get("completion_store_schema_probe_requires_compliant_dataset") is True
    and data.get("scope_limited_passive_probe_status")
    in {
        "PASS_SCOPE_LIMITED_COMPLETION_PASSIVE_PROBE_SUMMARY",
        "PASS_LOCAL_COMPLETION_RESEARCH_ONLY",
    }
    and data.get("completion_passive_probe_status")
    in {
        "PASS_SCOPE_LIMITED_COMPLETION_PASSIVE_PROBE_SUMMARY",
        "PASS_LOCAL_COMPLETION_RESEARCH_ONLY",
    }
    and data.get("completion_passive_probe_dataset_type")
    in {
        "scope_limited_completion_unwind_event_store_v2",
        "local_poly_backtest_completion_unwind_event_store_v2",
    }
    and data.get("completion_passive_probe_requires_compliant_dataset") is True
    and data.get("scope_limited_passive_probe_positive_net_pnl_run_count", 0) >= 1
    and data.get("scope_limited_passive_probe_positive_stress100_run_count", 0) >= 0
    and data.get("scope_limited_passive_probe_positive_worst_residual_run_count", 0) >= 0
    and data.get("scope_limited_passive_probe_positive_stress100_worst_run_count", 0) >= 0
    and data.get("scope_limited_passive_probe_best_net_pnl", 0) > 0
    and data.get("scope_limited_passive_probe_best_stress100_actual_pnl") is not None
    and data.get("scope_limited_passive_probe_best_worst_residual_net_pnl") is not None
    and data.get("scope_limited_passive_probe_best_stress100_worst_pnl") is not None
    and data.get("scope_limited_passive_probe_can_support_promotion") is False
    and data.get("canary_requires_live_fill_pnl_queue_position_proof_before_scale") is True
    and data.get("canary_requires_reviewed_effectful_executor_implementation") is True
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and all(value is False for value in side_effects.values())
    and data.get("heartbeat", {}).get("id") == "xuan-research-dplus-implementation-heartbeat"
    and data.get("readiness", {}).get("exists") is True
    and data.get("readonly_user_ws_summary_gate_smoke", {}).get("exists") is True
    and data.get("readonly_user_ws_summary_gate_smoke", {}).get("artifact") == "xuan_b27_dplus_readonly_user_ws_summary_gate_smoke"
    and data.get("readonly_user_ws_summary_gate_smoke", {}).get("status") == "PASS"
    and data.get("readonly_user_ws_wrapper_refusal_smoke", {}).get("exists") is True
    and data.get("readonly_user_ws_wrapper_refusal_smoke", {}).get("artifact") == "xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke"
    and data.get("readonly_user_ws_wrapper_refusal_smoke", {}).get("status") == "PASS"
    and data.get("auth_source_preflight", {}).get("exists") is True
    and data.get("auth_source_preflight_smoke", {}).get("exists") is True
    and data.get("auth_source_preflight_smoke", {}).get("status") == "PASS"
    and data.get("shared_ingress_preflight", {}).get("exists") is True
    and data.get("shared_ingress_preflight", {}).get("artifact") == "xuan_b27_dplus_shared_ingress_preflight"
    and data.get("shared_ingress_preflight_smoke", {}).get("exists") is True
    and data.get("shared_ingress_preflight_smoke", {}).get("artifact") == "xuan_b27_dplus_shared_ingress_preflight_smoke"
    and data.get("shared_ingress_preflight_smoke", {}).get("status") == "PASS"
    and data.get("remote_shared_ingress_probe", {}).get("exists") is True
    and data.get("remote_shared_ingress_probe", {}).get("artifact") == "xuan_b27_dplus_remote_shared_ingress_probe"
    and data.get("remote_shared_ingress_probe", {}).get("status") == "OK"
    and data.get("remote_shared_ingress_probe", {}).get("all_socket_checks_ok") is True
    and data.get("remote_shared_ingress_probe", {}).get("side_effects_all_false") is True
    and data.get("remote_shared_ingress_probe", {}).get("local_process_can_use_remote_unix_sockets") is False
    and data.get("latest_ec2_readonly_user_ws_attempt", {}).get("exists") is True
    and data.get("latest_ec2_readonly_user_ws_attempt", {}).get("artifact") == "xuan_b27_dplus_ec2_readonly_user_ws_attempt"
    and data.get("latest_ec2_readonly_user_ws_attempt", {}).get("status") in {
        "FAILED_CLOSED_REMOTE_OBSERVER_ENTRYPOINT_MISSING",
        "PASS_READONLY_USER_WS_OBSERVER",
        "FAIL_READONLY_USER_WS_OBSERVER",
    }
    and data.get("latest_ec2_readonly_user_ws_attempt", {}).get("remote_broker_socket_checks_ok") is True
    and ec2_attempt_state_ok
    and data.get("ec2_readonly_attempt_smoke", {}).get("exists") is True
    and data.get("ec2_readonly_attempt_smoke", {}).get("artifact") == "xuan_b27_dplus_ec2_readonly_attempt_smoke"
    and data.get("ec2_readonly_attempt_smoke", {}).get("status") == "PASS"
    and data.get("ec2_readonly_attempt_smoke", {}).get("orders_sent") is False
    and data.get("ec2_readonly_attempt_smoke", {}).get("auth_network_started") is False
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("exists") is True
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("artifact") == "xuan_b27_dplus_ec2_entrypoint_manifest_smoke"
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("status") == "PASS"
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("orders_sent") is False
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("auth_network_started") is False
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("entrypoint_manifest_exists") is True
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("entrypoint_manifest_status") == "PASS"
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("expected_executable_count", 0) >= 4
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("missing_executable_files") == []
    and data.get("ec2_entrypoint_manifest_smoke", {}).get("executable_contract_ok") is True
    and resync_plan_ok
    and acceptance_30m_plan_ok
    and canary_plan_ok
    and data.get("backtest_report_scope_audit", {}).get("exists") is True
    and data.get("backtest_report_scope_audit", {}).get("artifact") == "xuan_b27_dplus_backtest_report_scope_audit"
    and data.get("backtest_report_scope_audit", {}).get("status") == "PASS_BACKTEST_REPORT_SCOPE_AUDIT"
    and data.get("backtest_report_scope_audit", {}).get("audit_passed") is True
    and data.get("backtest_report_scope_audit_smoke", {}).get("exists") is True
    and data.get("backtest_report_scope_audit_smoke", {}).get("artifact") == "xuan_b27_dplus_backtest_report_scope_audit_smoke"
    and data.get("backtest_report_scope_audit_smoke", {}).get("status") == "PASS"
    and data.get("completion_store_schema_probe", {}).get("exists") is True
    and data.get("completion_store_schema_probe", {}).get("artifact") == "xuan_b27_dplus_completion_store_schema_probe"
    and data.get("completion_store_schema_probe", {}).get("status") == "PASS_SCOPE_LIMITED_COMPLETION_STORE_SCHEMA_PROBE"
    and data.get("completion_store_schema_probe", {}).get("probe_passed") is True
    and data.get("completion_store_schema_probe_smoke", {}).get("exists") is True
    and data.get("completion_store_schema_probe_smoke", {}).get("artifact") == "xuan_b27_dplus_completion_store_schema_probe_smoke"
    and data.get("completion_store_schema_probe_smoke", {}).get("status") == "PASS"
    and data.get("scope_limited_passive_probe_summary", {}).get("exists") is True
    and data.get("scope_limited_passive_probe_summary", {}).get("artifact") == "xuan_b27_dplus_scope_limited_completion_passive_probe_summary"
    and data.get("scope_limited_passive_probe_summary", {}).get("status")
    in {
        "PASS_SCOPE_LIMITED_COMPLETION_PASSIVE_PROBE_SUMMARY",
        "PASS_LOCAL_COMPLETION_RESEARCH_ONLY",
    }
    and data.get("scope_limited_passive_probe_summary", {}).get("can_support_strategy_promotion") is False
    and data.get("scope_limited_passive_probe_summary_smoke", {}).get("exists") is True
    and data.get("scope_limited_passive_probe_summary_smoke", {}).get("artifact") == "xuan_b27_dplus_scope_limited_completion_passive_probe_summary_smoke"
    and data.get("scope_limited_passive_probe_summary_smoke", {}).get("status") == "PASS"
    and g2_review.get("exists") is True
    and g2_review.get("artifact") == "xuan_b27_dplus_g2_canary_review_smoke"
    and g2_review.get("status") == "PASS"
    and g2_review.get("scope") == "local_no_network_g2_canary_review_gate"
    and g2_review.get("orders_sent") is False
    and g2_review.get("auth_network_started") is False
    and g2_review.get("started_canary") is False
    and g2_review.get("summarizer") == "scripts/xuan_b27_dplus_summarize_g2_canary_run.py"
    and g2_approval.get("exists") is True
    and g2_approval.get("artifact") == "xuan_b27_dplus_g2_canary_approval_envelope_smoke"
    and g2_approval.get("status") == "PASS"
    and g2_approval.get("scope") == "local_no_network_g2_canary_approval_envelope_gate"
    and g2_approval.get("orders_sent") is False
    and g2_approval.get("auth_network_started") is False
    and g2_approval.get("started_canary") is False
    and g2_approval.get("verifier") == "scripts/xuan_b27_dplus_g2_canary_approval_envelope.py"
    and g2_launcher.get("exists") is True
    and g2_launcher.get("artifact") == "xuan_b27_dplus_g2_canary_launcher_refusal_smoke"
    and g2_launcher.get("status") == "PASS"
    and g2_launcher.get("scope") == "local_no_network_g2_canary_launcher_refusal_gate"
    and g2_launcher.get("orders_sent") is False
    and g2_launcher.get("auth_network_started") is False
    and g2_launcher.get("started_canary") is False
    and g2_launcher.get("launcher") == "scripts/xuan_b27_dplus_g2_canary_launcher_refusal.py"
    and g2_executor_contract.get("exists") is True
    and g2_executor_contract.get("artifact") == "xuan_b27_dplus_g2_canary_executor_contract_smoke"
    and g2_executor_contract.get("status") == "PASS"
    and g2_executor_contract.get("scope") == "local_no_network_g2_canary_executor_contract_gate"
    and g2_executor_contract.get("orders_sent") is False
    and g2_executor_contract.get("auth_network_started") is False
    and g2_executor_contract.get("started_canary") is False
    and isinstance(g2_executor_contract.get("contract_manifest"), str)
    and g2_executor.get("exists") is True
    and g2_executor.get("artifact") == "xuan_b27_dplus_g2_canary_executor_smoke"
    and g2_executor.get("status") == "PASS"
    and g2_executor.get("scope") == "local_no_network_g2_canary_executor_preflight_gate"
    and g2_executor.get("orders_sent") is False
    and g2_executor.get("auth_network_started") is False
    and g2_executor.get("started_canary") is False
    and g2_executor.get("executor") == "scripts/xuan_b27_dplus_g2_canary_executor.py"
    and g2_executor_dry_run.get("exists") is True
    and g2_executor_dry_run.get("artifact") == "xuan_b27_dplus_g2_canary_executor_dry_run_smoke"
    and g2_executor_dry_run.get("status") == "PASS"
    and g2_executor_dry_run.get("scope") == "local_no_network_g2_canary_executor_dry_run_gate"
    and g2_executor_dry_run.get("orders_sent") is False
    and g2_executor_dry_run.get("auth_network_started") is False
    and g2_executor_dry_run.get("started_canary") is False
    and g2_executor_dry_run.get("executor") == "scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py"
    and g2_executor_payload.get("exists") is True
    and g2_executor_payload.get("artifact") == "xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke"
    and g2_executor_payload.get("status") == "PASS"
    and g2_executor_payload.get("scope") == "local_no_network_g2_canary_executor_payload_manifest_gate"
    and g2_executor_payload.get("orders_sent") is False
    and g2_executor_payload.get("auth_network_started") is False
    and g2_executor_payload.get("started_canary") is False
    and isinstance(g2_executor_payload.get("payload_manifest"), str)
    and g2_effectful_review.get("exists") is True
    and g2_effectful_review.get("artifact") == "xuan_b27_dplus_g2_canary_effectful_executor_review_smoke"
    and g2_effectful_review.get("status") == "PASS"
    and g2_effectful_review.get("scope") == "local_no_network_g2_canary_effectful_executor_review_gate"
    and g2_effectful_review.get("orders_sent") is False
    and g2_effectful_review.get("cancels_sent") is False
    and g2_effectful_review.get("redeems_sent") is False
    and g2_effectful_review.get("auth_network_started") is False
    and g2_effectful_review.get("started_canary") is False
    and g2_effectful_review.get("reviewer") == "scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py"
    and g2_effectful_review.get("real_effectful_executor_review_status") == "BLOCKED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and g2_effectful_review.get("real_effectful_executor_implemented") is False
    and g2_effectful_review.get("real_review_passed") is False
    and g2_effectful_review.get("fixture_pass_artifact_published") is False
    and shadow_edge_smoke.get("exists") is True
    and shadow_edge_smoke.get("artifact") == "xuan_b27_dplus_shadow_edge_samples_smoke"
    and shadow_edge_smoke.get("status") == "PASS"
    and shadow_edge_smoke.get("scope") == "local_no_network_shadow_edge_sample_extraction_gate"
    and shadow_edge_smoke.get("orders_sent") is False
    and shadow_edge_smoke.get("auth_network_started") is False
    and shadow_edge_smoke.get("started_canary") is False
    and shadow_edge_smoke.get("extractor") == "scripts/xuan_b27_dplus_extract_shadow_edge_samples.py"
    and shadow_edge_smoke.get("edge_only_rejected_by_performance_gate") is True
    and l1_labels_smoke.get("exists") is True
    and l1_labels_smoke.get("artifact") == "xuan_b27_dplus_l1_dry_run_outcome_labels_smoke"
    and l1_labels_smoke.get("status") == "PASS"
    and l1_labels_smoke.get("scope") == "local_no_network_l1_dry_run_outcome_label_gate"
    and l1_labels_smoke.get("orders_sent") is False
    and l1_labels_smoke.get("cancels_sent") is False
    and l1_labels_smoke.get("redeems_sent") is False
    and l1_labels_smoke.get("auth_network_started") is False
    and l1_labels_smoke.get("started_observer") is False
    and l1_labels_smoke.get("started_user_ws") is False
    and l1_labels_smoke.get("started_canary") is False
    and l1_labels_smoke.get("producer") == "scripts/xuan_b27_dplus_l1_dry_run_outcome_labels.py"
    and l1_labels_smoke.get("real_outcome_labels_published") is False
    and l1_labels_smoke.get("real_shadow_run_artifact_published") is False
    and l1_labels_smoke.get("acceptance_artifact_published") is False
    and l1_labels.get("exists") is True
    and l1_labels.get("artifact") == "xuan_b27_dplus_l1_dry_run_outcome_labels"
    and l1_labels.get("scope") == "local_no_network_l1_dry_run_outcome_label_producer"
    and l1_labels.get("orders_sent") is False
    and l1_labels.get("auth_network_started") is False
    and l1_labels.get("started_canary") is False
    and no_order_shadow_run_smoke.get("exists") is True
    and no_order_shadow_run_smoke.get("artifact") == "xuan_b27_dplus_no_order_shadow_run_artifact_smoke"
    and no_order_shadow_run_smoke.get("status") == "PASS"
    and no_order_shadow_run_smoke.get("scope") == "local_no_network_no_order_shadow_run_artifact_gate"
    and no_order_shadow_run_smoke.get("orders_sent") is False
    and no_order_shadow_run_smoke.get("cancels_sent") is False
    and no_order_shadow_run_smoke.get("redeems_sent") is False
    and no_order_shadow_run_smoke.get("auth_network_started") is False
    and no_order_shadow_run_smoke.get("started_canary") is False
    and no_order_shadow_run_smoke.get("producer") == "scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py"
    and no_order_shadow_run_smoke.get("real_shadow_run_artifact_published") is False
    and no_order_shadow_run_smoke.get("acceptance_artifact_published") is False
    and shadow_acceptance_discovery_smoke.get("exists") is True
    and shadow_acceptance_discovery_smoke.get("artifact") == "xuan_b27_dplus_shadow_acceptance_input_discovery_smoke"
    and shadow_acceptance_discovery_smoke.get("status") == "PASS"
    and shadow_acceptance_discovery_smoke.get("scope") == "local_no_network_shadow_acceptance_input_discovery_gate"
    and shadow_acceptance_discovery_smoke.get("orders_sent") is False
    and shadow_acceptance_discovery_smoke.get("cancels_sent") is False
    and shadow_acceptance_discovery_smoke.get("redeems_sent") is False
    and shadow_acceptance_discovery_smoke.get("auth_network_started") is False
    and shadow_acceptance_discovery_smoke.get("started_observer") is False
    and shadow_acceptance_discovery_smoke.get("started_user_ws") is False
    and shadow_acceptance_discovery_smoke.get("started_canary") is False
    and shadow_acceptance_discovery_smoke.get("discovery_script") == "scripts/xuan_b27_dplus_shadow_acceptance_input_discovery.py"
    and shadow_acceptance_discovery_smoke.get("real_shadow_acceptance_input_published") is False
    and shadow_acceptance_discovery.get("exists") is True
    and shadow_acceptance_discovery.get("artifact") == "xuan_b27_dplus_shadow_acceptance_input_discovery"
    and shadow_acceptance_discovery.get("status") in {
        "BLOCKED_NO_REAL_SHADOW_ACCEPTANCE_INPUTS",
        "BLOCKED_SHADOW_ACCEPTANCE_INPUTS_INVALID",
        "READY_FOR_RUST_SHADOW_STRATEGY_ACCEPTANCE_RUNNER",
    }
    and shadow_acceptance_discovery.get("allow_fixture_root") is False
    and shadow_acceptance_discovery.get("orders_sent") is False
    and shadow_acceptance_discovery.get("auth_network_started") is False
    and shadow_acceptance_discovery.get("started_canary") is False
    and realized_labels_smoke.get("exists") is True
    and realized_labels_smoke.get("artifact") == "xuan_b27_dplus_realized_outcome_labels_smoke"
    and realized_labels_smoke.get("status") == "PASS"
    and realized_labels_smoke.get("scope") == "local_no_network_realized_outcome_label_producer_gate"
    and realized_labels_smoke.get("orders_sent") is False
    and realized_labels_smoke.get("auth_network_started") is False
    and realized_labels_smoke.get("started_canary") is False
    and realized_labels_smoke.get("producer") == "scripts/xuan_b27_dplus_realized_outcome_labels.py"
    and realized_labels_smoke.get("real_outcome_labels_published") is False
    and realized_labels_smoke.get("acceptance_artifact_published") is False
    and outcome_bridge_smoke.get("exists") is True
    and outcome_bridge_smoke.get("artifact") == "xuan_b27_dplus_outcome_label_bridge_smoke"
    and outcome_bridge_smoke.get("status") == "PASS"
    and outcome_bridge_smoke.get("scope") == "local_no_network_outcome_label_bridge_gate"
    and outcome_bridge_smoke.get("orders_sent") is False
    and outcome_bridge_smoke.get("auth_network_started") is False
    and outcome_bridge_smoke.get("started_canary") is False
    and outcome_bridge_smoke.get("bridge") == "scripts/xuan_b27_dplus_outcome_label_bridge.py"
    and outcome_bridge_smoke.get("real_outcome_bridge_published") is False
    and outcome_bridge_smoke.get("acceptance_artifact_published") is False
    and shadow_performance_smoke.get("exists") is True
    and shadow_performance_smoke.get("artifact") == "xuan_b27_dplus_shadow_performance_evidence_smoke"
    and shadow_performance_smoke.get("status") == "PASS"
    and shadow_performance_smoke.get("scope") == "local_no_network_shadow_performance_evidence_gate"
    and shadow_performance_smoke.get("orders_sent") is False
    and shadow_performance_smoke.get("auth_network_started") is False
    and shadow_performance_smoke.get("started_canary") is False
    and shadow_performance_smoke.get("performance_script") == "scripts/xuan_b27_dplus_shadow_performance_evidence.py"
    and shadow_performance_smoke.get("real_performance_evidence_published") is False
    and rust_shadow_acceptance_smoke.get("exists") is True
    and rust_shadow_acceptance_smoke.get("artifact") == "xuan_b27_dplus_rust_shadow_strategy_acceptance_smoke"
    and rust_shadow_acceptance_smoke.get("status") == "PASS"
    and rust_shadow_acceptance_smoke.get("scope") == "local_no_network_rust_shadow_strategy_acceptance_gate"
    and rust_shadow_acceptance_smoke.get("orders_sent") is False
    and rust_shadow_acceptance_smoke.get("auth_network_started") is False
    and rust_shadow_acceptance_smoke.get("started_canary") is False
    and rust_shadow_acceptance_smoke.get("acceptance_script") == "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py"
    and rust_shadow_acceptance_smoke.get("acceptance_artifact_published") is False
    and rust_shadow_acceptance_runner_smoke.get("exists") is True
    and rust_shadow_acceptance_runner_smoke.get("artifact") == "xuan_b27_dplus_rust_shadow_strategy_acceptance_runner_smoke"
    and rust_shadow_acceptance_runner_smoke.get("status") == "PASS"
    and rust_shadow_acceptance_runner_smoke.get("scope") == "local_no_network_rust_shadow_strategy_acceptance_runner_gate"
    and rust_shadow_acceptance_runner_smoke.get("orders_sent") is False
    and rust_shadow_acceptance_runner_smoke.get("cancels_sent") is False
    and rust_shadow_acceptance_runner_smoke.get("redeems_sent") is False
    and rust_shadow_acceptance_runner_smoke.get("auth_network_started") is False
    and rust_shadow_acceptance_runner_smoke.get("started_canary") is False
    and rust_shadow_acceptance_runner_smoke.get("runner") == "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py"
    and rust_shadow_acceptance_runner_smoke.get("real_acceptance_artifact_published") is False
    and rust_shadow_acceptance_runner_smoke.get("acceptance_artifact_published") is False
    and rust_shadow_acceptance.get("exists") is True
    and rust_shadow_acceptance.get("artifact") == "xuan_b27_dplus_rust_shadow_strategy_acceptance"
    and rust_shadow_acceptance.get("status") == "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE"
    and rust_shadow_acceptance.get("scope") == "rust_no_order_shadow_or_dry_run_strategy_acceptance"
    and rust_shadow_acceptance.get("strategy_acceptance_passed") is True
    and rust_shadow_acceptance.get("orders_sent") is False
    and rust_shadow_acceptance.get("cancels_sent") is False
    and rust_shadow_acceptance.get("redeems_sent") is False
    and rust_shadow_acceptance.get("auth_network_started") is False
    and rust_shadow_acceptance.get("started_canary") is False
    and data.get("latest_ec2_entrypoint_sync", {}).get("exists") is True
    and data.get("latest_ec2_entrypoint_sync", {}).get("artifact") == "xuan_b27_dplus_ec2_entrypoint_sync_local_summary"
    and data.get("latest_ec2_entrypoint_sync", {}).get("status") == "PASS_ENTRYPOINT_SYNC_READY"
    and data.get("latest_ec2_entrypoint_sync", {}).get("forbidden_side_effects_absent") is True
    and data.get("latest_ec2_entrypoint_sync", {}).get("observer_binary_exists") is True
    and ec2_resync_rebuild.get("exists") is True
    and ec2_resync_rebuild.get("artifact") == "xuan_b27_dplus_ec2_resync_rebuild_local_summary"
    and ec2_resync_rebuild.get("status") == "PASS_RESYNC_REBUILD_READY"
    and str(ec2_resync_rebuild.get("remote_worktree", "")).startswith(
        "/home/ubuntu/xuan_research_runs/xuan_research_"
    )
    and isinstance(ec2_resync_rebuild.get("observer_binary_sha256"), str)
    and len(ec2_resync_rebuild.get("observer_binary_sha256")) == 64
    and ec2_resync_rebuild.get("broker_preflight_status") == "OK"
    and ec2_resync_rebuild.get("entrypoint_manifest_status") == "PASS"
    and ec2_resync_rebuild.get("created_isolated_worktree") is True
    and ec2_resync_rebuild.get("synced_entrypoint_files") is True
    and ec2_resync_rebuild.get("built_observer_binary") is True
    and ec2_resync_rebuild.get("forbidden_side_effects_absent") is True
    and data.get("latest_ec2_readonly_user_ws_observer", {}).get("exists") in {True, False}
    and ec2_diagnostic_smoke.get("exists") is True
    and ec2_diagnostic_smoke.get("artifact") == "xuan_b27_dplus_ec2_readonly_diagnostic_smoke"
    and ec2_diagnostic_smoke.get("status") == "PASS"
    and ec2_diagnostic_smoke.get("scope") == "local_no_network_ec2_readonly_diagnostic_gate"
    and ec2_diagnostic_smoke.get("orders_sent") is False
    and ec2_diagnostic_smoke.get("auth_network_started") is False
    and ec2_diagnostic_smoke.get("diagnostic_review") == ec2_diagnostic.get("path")
    and ec2_acceptance_smoke.get("exists") is True
    and ec2_acceptance_smoke.get("artifact") == "xuan_b27_dplus_ec2_30m_acceptance_smoke"
    and ec2_acceptance_smoke.get("status") == "PASS"
    and ec2_acceptance_smoke.get("scope") == "local_no_network_ec2_30m_acceptance_gate"
    and ec2_acceptance_smoke.get("orders_sent") is False
    and ec2_acceptance_smoke.get("auth_network_started") is False
    and ec2_acceptance_smoke.get("acceptance_review") == ec2_acceptance.get("path")
    and postmortem_ok
    and data.get("source_of_truth_schema_smoke", {}).get("exists") is True
    and data.get("source_of_truth_schema_smoke", {}).get("artifact") == "xuan_b27_dplus_source_of_truth_schema_smoke"
    and data.get("source_of_truth_schema_smoke", {}).get("status") == "PASS"
    and data.get("latest_readonly_user_ws_attempt", {}).get("exists") is True
    and data.get("latest_readonly_user_ws_attempt", {}).get("artifact") == "xuan_b27_dplus_readonly_user_ws_observer_wrapper"
    and data.get("latest_readonly_user_ws_attempt", {}).get("orders_allowed") is False
    and data.get("latest_readonly_user_ws_attempt", {}).get("cancels_allowed") is False
    and data.get("latest_readonly_user_ws_attempt", {}).get("redeems_allowed") is False
    and "dotenv_loaded_keys" not in data.get("latest_readonly_user_ws_attempt", {})
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_local_status_bundle_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_status_bundle_gate",
  "bundle_manifest": "$bundle_manifest",
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
