#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_smoke_$ts}"
fixtures="$out_dir/fixtures"
mkdir -p "$fixtures"

status="PASS"
log="$out_dir/checks.log"
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

run_executor_case() {
  local name="$1"
  local expected_rc="$2"
  shift 2
  local case_dir="$out_dir/$name"
  local rc=0
  scripts/xuan_b27_dplus_g2_canary_executor.py --output-dir "$case_dir" "$@" >> "$log" 2>&1 || rc=$?
  if [[ "$rc" != "$expected_rc" ]]; then
    printf 'unexpected rc for %s: got %s want %s\n' "$name" "$rc" "$expected_rc" >> "$log"
    return 1
  fi
  test -f "$case_dir/manifest.json"
}

python3 - "$fixtures" <<'PY'
from __future__ import annotations

import copy
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
review_artifact = root / "valid_effectful_executor_review.json"
review_artifact.write_text(json.dumps({
    "artifact": "xuan_b27_dplus_g2_canary_effectful_executor_review",
    "status": "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION",
    "strategy": "xuan_b27_dplus",
    "scope": "local_no_network_g2_canary_effectful_executor_review",
    "review_passed": True,
    "fixture_review": False,
    "effectful_executor_implemented": True,
    "reviewed_effectful_executor_implementation": True,
    "canary_run_authorized": False,
    "orders_sent": False,
    "cancels_sent": False,
    "redeems_sent": False,
    "auth_network_started": False,
    "started_canary": False,
    "side_effects": {
        "ssh_started": False,
        "network_started": False,
        "started_canary": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "broker_modified": False,
        "service_control_called": False,
    },
}, sort_keys=True) + "\n")
base = {
    "artifact": "xuan_b27_dplus_g2_canary_approval_envelope",
    "approval_scope": "exact_g2_canary_sync_rebuild_and_run",
    "explicit_current_conversation_approval": True,
    "heartbeat_or_generic_approval": False,
    "strategy": "xuan_b27_dplus",
    "run": {
        "run_class": "G2_READ_WRITE_CANARY_SMOKE",
        "market_slug": "btc-updown-5m",
        "shared_ingress_role": "client",
        "shared_ingress_root": "/srv/pm_as_ofi/shared-ingress-main",
        "remote_worktree": "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree",
        "remote_run_dir": "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z",
        "duration_seconds": 1800,
        "max_rounds": 6,
        "sync_rebuild_approved": True,
        "canary_run_approved": True,
    },
    "risk": {
        "target_qty": 5,
        "max_live_orders": 2,
        "max_open_cost_usdc": 50,
        "max_strategy_exposure_usdc": 100,
        "max_active_markets": 1,
        "post_only": True,
        "allow_passive_taker": False,
        "stop_on_unknown": True,
    },
    "allowed_side_effects": {
        "capped_post_only_orders": True,
        "bounded_own_order_cancels": True,
        "redeem_or_claim": False,
        "broker_control": False,
        "service_control": False,
        "shared_ingress_write": False,
        "env_write": False,
    },
    "forbidden_side_effects": {
        "systemd_or_service_control": True,
        "broker_start_stop_repair": True,
        "shared_ingress_modification": True,
        "remote_env_file_write": True,
        "raw_replay_scan_or_write": True,
        "unbounded_live_trading": True,
    },
    "post_run_review": {
        "summarizer": "scripts/xuan_b27_dplus_summarize_g2_canary_run.py",
        "require_check_acceptance": True,
        "require_secret_sentinel_scan": True,
    },
    "executor_review": {
        "reviewed_effectful_executor_implementation": True,
        "review_status": "PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION",
        "review_artifact": str(review_artifact),
        "require_current_payload_allowlist_no_drift": True,
        "require_not_heartbeat_or_generic_approval": True,
    },
}
valid = copy.deepcopy(base)
invalid = copy.deepcopy(base)
invalid["run"]["remote_worktree"] = "/srv/pm_as_ofi/not-isolated"
bad_run_root = copy.deepcopy(base)
bad_run_root["run"]["remote_run_dir"] = (
    "/home/ubuntu/xuan_research_runs/"
    "xuan_research_other_20990101T000000Z/"
    "g2_canary_run_20990101T000000Z"
)
(root / "valid_envelope.json").write_text(json.dumps(valid, sort_keys=True) + "\n")
(root / "invalid_envelope.json").write_text(json.dumps(invalid, sort_keys=True) + "\n")
(root / "bad_run_root_envelope.json").write_text(json.dumps(bad_run_root, sort_keys=True) + "\n")
PY

payload_fixture_dir="$fixtures/payload_manifest_run"
scripts/xuan_b27_dplus_g2_canary_executor_payload_manifest.py --output-dir "$payload_fixture_dir" >> "$log" 2>&1
python3 - "$payload_fixture_dir/manifest.json" "$fixtures/stale_payload_manifest.json" <<'PY'
import copy
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
dest = pathlib.Path(sys.argv[2])
data = json.loads(source.read_text())
stale = copy.deepcopy(data)
stale["payload_records"][0]["sha256"] = "0" * 64
dest.write_text(json.dumps(stale, sort_keys=True) + "\n")
PY

script="scripts/xuan_b27_dplus_g2_canary_executor.py"

run_check "executor_exists" test -x "$script"
run_check "executor_py_compile" python3 -m py_compile "$script"
run_check "executor_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "missing_explicit_flag_refuses" run_executor_case missing_flag 64 --approval-envelope "$fixtures/valid_envelope.json"
run_check "missing_envelope_refuses" run_executor_case missing_envelope 64 --approved-g2-canary-sync-and-run
run_check "invalid_envelope_refuses" run_executor_case invalid_envelope 65 --approved-g2-canary-sync-and-run --approval-envelope "$fixtures/invalid_envelope.json"
run_check "stale_payload_manifest_refuses" run_executor_case stale_payload_manifest 68 --approved-g2-canary-sync-and-run --approval-envelope "$fixtures/valid_envelope.json" --payload-manifest "$fixtures/stale_payload_manifest.json"
run_check "bad_run_root_plan_refuses" run_executor_case bad_run_root_plan 69 --approved-g2-canary-sync-and-run --approval-envelope "$fixtures/bad_run_root_envelope.json" --payload-manifest "$payload_fixture_dir/manifest.json"
run_check "valid_preflight_refuses_effectful_executor" run_executor_case valid_preflight 75 --approved-g2-canary-sync-and-run --approval-envelope "$fixtures/valid_envelope.json" --payload-manifest "$payload_fixture_dir/manifest.json"
run_check "valid_preflight_manifest_contract" python3 - "$out_dir/valid_preflight/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
side_effects = data.get("side_effects") or {}
approval = data.get("approval_summary") or {}
contract = data.get("executor_contract") or {}
payload = data.get("payload_allowlist") or {}
payload_current = data.get("payload_current_check") or {}
run_root = data.get("remote_run_root_plan") or {}
sync_plan = data.get("payload_sync_plan") or {}
build_plan = data.get("remote_build_plan") or {}
preflight_plan = data.get("shared_ingress_preflight_plan") or {}
auth_plan = data.get("auth_injection_plan") or {}
process_plan = data.get("bounded_canary_process_plan") or {}
supervision_plan = data.get("wait_stop_supervision_plan") or {}
pullback_plan = data.get("artifact_pullback_review_plan") or {}
phase_plan = data.get("phase_plan") or []
local_artifact_dir = (
    "/Users/hot/web3Scientist/pm_as_ofi-xuan-research/"
    "xuan_research_artifacts/xuan_b27_dplus_g2_canary_artifacts_20990101T000000Z"
)
ok = (
    data.get("artifact") == "xuan_b27_dplus_g2_canary_executor"
    and data.get("status") == "REFUSED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED"
    and data.get("scope") == "local_no_network_g2_canary_executor_preflight"
    and data.get("executor_preflight_implemented") is True
    and data.get("effectful_executor_implemented") is False
    and data.get("canary_run_authorized") is False
    and approval.get("status") == "PASS_EXACT_G2_CANARY_APPROVAL_ENVELOPE"
    and contract.get("status") == "READY_FOR_REVIEWED_G2_CANARY_EXECUTOR_IMPLEMENTATION"
    and payload.get("status") == "PASS"
    and payload_current.get("status") == "PASS"
    and payload_current.get("drift_count") == 0
    and run_root.get("status") == "PASS"
    and run_root.get("remote_run_root") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z"
    and run_root.get("remote_worktree") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree"
    and run_root.get("remote_run_dir") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z"
    and sync_plan.get("status") == "PASS"
    and sync_plan.get("remote_worktree") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree"
    and sync_plan.get("payload_file_count", 0) >= 40
    and sync_plan.get("all_destinations_under_remote_worktree") is True
    and sync_plan.get("planned_side_effects", {}).get("sync_archive_created") is False
    and sync_plan.get("planned_side_effects", {}).get("remote_files_written") is False
    and build_plan.get("status") == "PASS"
    and build_plan.get("remote_worktree") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree"
    and build_plan.get("declared_build_targets") == ["polymarket_v2"]
    and build_plan.get("build_command_count") == 1
    and build_plan.get("all_build_cwds_under_remote_worktree") is True
    and build_plan.get("planned_side_effects", {}).get("cargo_invoked") is False
    and build_plan.get("planned_side_effects", {}).get("remote_files_written") is False
    and preflight_plan.get("status") == "PASS"
    and preflight_plan.get("shared_ingress_role") == "client"
    and preflight_plan.get("shared_ingress_root") == "/srv/pm_as_ofi/shared-ingress-main"
    and preflight_plan.get("preflight_script") == "scripts/xuan_b27_dplus_shared_ingress_preflight.py"
    and preflight_plan.get("remote_output_dir") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/shared_ingress_preflight"
    and preflight_plan.get("read_only_manifest_probe") is True
    and preflight_plan.get("planned_side_effects", {}).get("preflight_executed") is False
    and preflight_plan.get("planned_side_effects", {}).get("broker_started") is False
    and preflight_plan.get("planned_side_effects", {}).get("broker_repaired") is False
    and preflight_plan.get("planned_side_effects", {}).get("connected_to_broker") is False
    and preflight_plan.get("planned_side_effects", {}).get("modified_shared_ingress") is False
    and auth_plan.get("status") == "PASS"
    and auth_plan.get("non_secret_env", {}).get("PM_STRATEGY") == "xuan_b27_dplus"
    and auth_plan.get("non_secret_env", {}).get("PM_XUAN_B27_DPLUS_MODE") == "canary"
    and auth_plan.get("non_secret_env", {}).get("PM_XUAN_B27_DPLUS_MARKET_SLUG") == "btc-updown-5m"
    and auth_plan.get("non_secret_env", {}).get("PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC") == "50"
    and auth_plan.get("non_secret_env", {}).get("PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS") == "2"
    and auth_plan.get("non_secret_env", {}).get("PM_SHARED_INGRESS_ROOT") == "/srv/pm_as_ofi/shared-ingress-main"
    and auth_plan.get("secret_values_recorded") is False
    and auth_plan.get("secret_key_names_recorded") is False
    and auth_plan.get("credential_derivation_performed") is False
    and auth_plan.get("remote_env_file_written") is False
    and auth_plan.get("stdin_secret_payload_materialized") is False
    and auth_plan.get("planned_side_effects", {}).get("auth_env_injected") is False
    and auth_plan.get("planned_side_effects", {}).get("printed_secret_values") is False
    and auth_plan.get("planned_side_effects", {}).get("remote_env_file_written") is False
    and process_plan.get("status") == "PASS"
    and process_plan.get("remote_worktree") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree"
    and process_plan.get("remote_run_dir") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z"
    and process_plan.get("binary") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree/target/debug/polymarket_v2"
    and process_plan.get("argv") == ["/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/worktree/target/debug/polymarket_v2"]
    and process_plan.get("remote_log_root") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/logs"
    and process_plan.get("run_manifest_path") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/g2_canary_run_manifest.json"
    and process_plan.get("process_env_overlay", {}).get("PM_XUAN_B27_DPLUS_MODE") == "canary"
    and process_plan.get("process_env_overlay", {}).get("PM_XUAN_B27_DPLUS_MARKET_SLUG") == "btc-updown-5m"
    and process_plan.get("process_env_overlay", {}).get("POLYMARKET_MARKET_SLUG") == "btc-updown-5m"
    and process_plan.get("process_env_overlay", {}).get("PM_LOG_ROOT") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/logs"
    and process_plan.get("process_env_overlay", {}).get("PM_SHARED_INGRESS_ROOT") == "/srv/pm_as_ofi/shared-ingress-main"
    and process_plan.get("duration_seconds") == 1800
    and process_plan.get("max_rounds") == 6
    and process_plan.get("supervisor", {}).get("kill_after_seconds") == 30
    and process_plan.get("supervisor", {}).get("graceful_signal") == "TERM"
    and process_plan.get("stop_conditions", {}).get("stop_on_unknown") is True
    and process_plan.get("stop_conditions", {}).get("stop_on_source_truth_unknown_or_failed") is True
    and process_plan.get("stop_conditions", {}).get("stop_on_risk_cap_violation") is True
    and process_plan.get("stop_conditions", {}).get("stop_on_unexpected_taker_fill") is True
    and process_plan.get("stop_conditions", {}).get("stop_on_recorder_decode_or_critical_drop") is True
    and process_plan.get("stop_conditions", {}).get("stop_on_broker_preflight_failure") is True
    and process_plan.get("stop_conditions", {}).get("stop_on_duration_seconds") is True
    and process_plan.get("stop_conditions", {}).get("stop_on_max_rounds") is True
    and process_plan.get("secret_values_recorded") is False
    and process_plan.get("secret_key_names_recorded") is False
    and process_plan.get("planned_side_effects", {}).get("process_started") is False
    and process_plan.get("planned_side_effects", {}).get("orders_sent") is False
    and process_plan.get("planned_side_effects", {}).get("cancels_sent") is False
    and process_plan.get("planned_side_effects", {}).get("redeems_sent") is False
    and process_plan.get("planned_side_effects", {}).get("stdout_opened") is False
    and process_plan.get("planned_side_effects", {}).get("stderr_opened") is False
    and process_plan.get("planned_side_effects", {}).get("remote_files_written") is False
    and supervision_plan.get("status") == "PASS"
    and supervision_plan.get("mode") == "single_process_duration_or_round_limit_supervision"
    and supervision_plan.get("duration_seconds") == 1800
    and supervision_plan.get("max_rounds") == 6
    and supervision_plan.get("poll_interval_seconds") == 5
    and supervision_plan.get("graceful_signal") == "TERM"
    and supervision_plan.get("kill_after_seconds") == 30
    and supervision_plan.get("monitor_paths", {}).get("run_manifest_path") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/g2_canary_run_manifest.json"
    and supervision_plan.get("monitor_paths", {}).get("remote_log_root") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/logs"
    and supervision_plan.get("stop_triggers", {}).get("duration_seconds") is True
    and supervision_plan.get("stop_triggers", {}).get("max_rounds") is True
    and supervision_plan.get("stop_triggers", {}).get("process_exit") is True
    and supervision_plan.get("stop_triggers", {}).get("source_truth_unknown_or_failed") is True
    and supervision_plan.get("stop_triggers", {}).get("risk_cap_violation") is True
    and supervision_plan.get("stop_triggers", {}).get("unexpected_taker_fill") is True
    and supervision_plan.get("stop_triggers", {}).get("recorder_decode_or_critical_drop") is True
    and supervision_plan.get("stop_triggers", {}).get("broker_preflight_failure") is True
    and supervision_plan.get("termination_sequence") == [
        "observe_process_exit_or_stop_trigger",
        "send_graceful_term_if_still_running",
        "wait_kill_after_seconds",
        "send_kill_if_still_running",
        "record_exit_status_for_post_run_review",
    ]
    and supervision_plan.get("planned_side_effects", {}).get("wait_started") is False
    and supervision_plan.get("planned_side_effects", {}).get("process_polled") is False
    and supervision_plan.get("planned_side_effects", {}).get("signal_sent") is False
    and supervision_plan.get("planned_side_effects", {}).get("kill_sent") is False
    and supervision_plan.get("planned_side_effects", {}).get("process_reaped") is False
    and supervision_plan.get("planned_side_effects", {}).get("stdout_read") is False
    and supervision_plan.get("planned_side_effects", {}).get("stderr_read") is False
    and supervision_plan.get("planned_side_effects", {}).get("remote_files_read") is False
    and supervision_plan.get("planned_side_effects", {}).get("remote_files_written") is False
    and supervision_plan.get("planned_side_effects", {}).get("orders_sent") is False
    and supervision_plan.get("planned_side_effects", {}).get("cancels_sent") is False
    and supervision_plan.get("planned_side_effects", {}).get("redeems_sent") is False
    and pullback_plan.get("status") == "PASS"
    and pullback_plan.get("remote_run_dir") == "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z"
    and pullback_plan.get("local_artifact_dir") == local_artifact_dir
    and "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/g2_canary_run_manifest.json" in (pullback_plan.get("expected_remote_paths") or [])
    and "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/g2_canary_acceptance_review.json" in (pullback_plan.get("expected_remote_paths") or [])
    and "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/logs" in (pullback_plan.get("expected_remote_tree_roots") or [])
    and "/home/ubuntu/xuan_research_runs/xuan_research_dplus_g2_canary_20990101T000000Z/g2_canary_run_20990101T000000Z/recorder" in (pullback_plan.get("expected_remote_tree_roots") or [])
    and pullback_plan.get("post_run_review_plan", {}).get("summarizer") == "scripts/xuan_b27_dplus_summarize_g2_canary_run.py"
    and pullback_plan.get("post_run_review_plan", {}).get("argv") == [
        "python3",
        "scripts/xuan_b27_dplus_summarize_g2_canary_run.py",
        local_artifact_dir,
        "--output",
        local_artifact_dir + "/g2_canary_run_summary.json",
        "--check-acceptance",
    ]
    and pullback_plan.get("post_run_review_plan", {}).get("local_acceptance_review_path") == local_artifact_dir + "/local_acceptance_review.json"
    and pullback_plan.get("post_run_review_plan", {}).get("require_check_acceptance") is True
    and pullback_plan.get("post_run_review_plan", {}).get("require_secret_sentinel_scan") is True
    and pullback_plan.get("post_run_review_plan", {}).get("secret_sentinel_source") == "caller_provided_at_execution_time"
    and pullback_plan.get("post_run_review_plan", {}).get("secret_sentinel_values_recorded") is False
    and pullback_plan.get("post_run_review_plan", {}).get("secret_key_names_recorded") is False
    and pullback_plan.get("acceptance_requirements", {}).get("summary_status") == "PASS_G2_CANARY_ACCEPTANCE"
    and pullback_plan.get("acceptance_requirements", {}).get("exact_approval_scope") is True
    and pullback_plan.get("acceptance_requirements", {}).get("source_truth_all_pass") is True
    and pullback_plan.get("acceptance_requirements", {}).get("order_attempt_trace_linked") is True
    and pullback_plan.get("acceptance_requirements", {}).get("venue_order_id_linked") is True
    and pullback_plan.get("acceptance_requirements", {}).get("secret_sentinel_hits") == 0
    and pullback_plan.get("planned_side_effects", {}).get("remote_files_read") is False
    and pullback_plan.get("planned_side_effects", {}).get("local_files_written") is False
    and pullback_plan.get("planned_side_effects", {}).get("summary_executed") is False
    and pullback_plan.get("planned_side_effects", {}).get("secret_scan_executed") is False
    and pullback_plan.get("planned_side_effects", {}).get("acceptance_review_written") is False
    and pullback_plan.get("planned_side_effects", {}).get("orders_sent") is False
    and pullback_plan.get("planned_side_effects", {}).get("cancels_sent") is False
    and pullback_plan.get("planned_side_effects", {}).get("redeems_sent") is False
    and pullback_plan.get("planned_side_effects", {}).get("broker_modified") is False
    and pullback_plan.get("planned_side_effects", {}).get("shared_ingress_modified") is False
    and pullback_plan.get("planned_side_effects", {}).get("remote_env_file_written") is False
    and data.get("first_unimplemented_effectful_phase") is None
    and any(
        item.get("phase") == "create_isolated_remote_run_root_under_xuan_research_runs"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_REMOTE_ROOT_CREATION"
        and item.get("remote_run_root_plan_status") == "PASS"
        and item.get("remote_directory_created") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "sync_allowlisted_source_files_only"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_ALLOWLISTED_PAYLOAD_SYNC"
        and item.get("payload_sync_plan_status") == "PASS"
        and item.get("payload_file_count", 0) >= 40
        and item.get("sync_archive_created") is False
        and item.get("remote_files_written") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "build_declared_remote_targets_only"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_DECLARED_REMOTE_BUILD"
        and item.get("remote_build_plan_status") == "PASS"
        and item.get("declared_build_targets") == ["polymarket_v2"]
        and item.get("cargo_invoked") is False
        and item.get("remote_files_written") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "run_readonly_shared_ingress_preflight_before_canary"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_SHARED_INGRESS_PREFLIGHT"
        and item.get("shared_ingress_preflight_plan_status") == "PASS"
        and item.get("shared_ingress_root") == "/srv/pm_as_ofi/shared-ingress-main"
        and item.get("preflight_executed") is False
        and item.get("broker_modified") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "inject_auth_vars_ephemerally_only"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_EPHEMERAL_AUTH_INJECTION"
        and item.get("auth_injection_plan_status") == "PASS"
        and item.get("secret_values_recorded") is False
        and item.get("remote_env_file_written") is False
        and item.get("auth_env_injected") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "start_single_bounded_g2_canary_process"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_BOUNDED_CANARY_PROCESS"
        and item.get("bounded_canary_process_plan_status") == "PASS"
        and item.get("duration_seconds") == 1800
        and item.get("max_rounds") == 6
        and item.get("process_started") is False
        and item.get("orders_sent") is False
        and item.get("cancels_sent") is False
        and item.get("redeems_sent") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "wait_or_stop_at_duration_or_round_limit"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_WAIT_STOP_SUPERVISION"
        and item.get("wait_stop_supervision_plan_status") == "PASS"
        and item.get("duration_seconds") == 1800
        and item.get("max_rounds") == 6
        and item.get("poll_interval_seconds") == 5
        and item.get("signal_sent") is False
        and item.get("kill_sent") is False
        and item.get("process_reaped") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and any(
        item.get("phase") == "pull_artifacts_to_xuan_research_artifacts"
        and item.get("executor_status") == "PLANNED_NO_NETWORK_ARTIFACT_PULLBACK_REVIEW"
        and item.get("artifact_pullback_review_plan_status") == "PASS"
        and item.get("remote_files_read") is False
        and item.get("local_files_written") is False
        and item.get("summary_executed") is False
        and item.get("secret_scan_executed") is False
        and item.get("side_effects_performed") is False
        for item in phase_plan
    )
    and data.get("orders_sent") is False
    and data.get("cancels_sent") is False
    and data.get("redeems_sent") is False
    and data.get("auth_network_started") is False
    and data.get("started_canary") is False
    and all(value is False for value in side_effects.values())
)
raise SystemExit(0 if ok else 1)
PY
run_check "bad_run_root_plan_manifest_contract" python3 - "$out_dir/bad_run_root_plan/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
run_root = data.get("remote_run_root_plan") or {}
side_effects = data.get("side_effects") or {}
ok = (
    data.get("status") == "REFUSED_REMOTE_RUN_ROOT_PLAN_INVALID"
    and run_root.get("status") == "FAIL"
    and "remote_worktree_and_run_dir_not_same_root" in (run_root.get("failures") or [])
    and data.get("orders_sent") is False
    and data.get("cancels_sent") is False
    and data.get("redeems_sent") is False
    and data.get("auth_network_started") is False
    and data.get("started_canary") is False
    and all(value is False for value in side_effects.values())
)
raise SystemExit(0 if ok else 1)
PY
run_check "stale_payload_manifest_contract" python3 - "$out_dir/stale_payload_manifest/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
payload_current = data.get("payload_current_check") or {}
ok = (
    data.get("status") == "REFUSED_PAYLOAD_ALLOWLIST_DRIFT"
    and payload_current.get("status") == "FAIL"
    and payload_current.get("drift_count", 0) >= 1
    and data.get("orders_sent") is False
    and data.get("started_canary") is False
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_executor_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_executor_preflight_gate",
  "executor": "$script",
  "orders_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
