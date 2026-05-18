#!/usr/bin/env bash
set -euo pipefail

ts=$(date -u +%Y%m%dT%H%M%SZ)
out_dir="xuan_research_artifacts/xuan_b27_dplus_compliant_backtest_run_plan_smoke_${ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
status="PASS"

run_check() {
  local name="$1"
  shift
  printf '== %s ==\n' "$name" >> "$log"
  if ! "$@" >> "$log" 2>&1; then
    printf 'CHECK_FAILED %s\n' "$name" | tee -a "$log"
    status="FAIL"
  fi
}

script="scripts/xuan_b27_dplus_compliant_backtest_run_plan.py"
run_check "script_exists" test -f "$script"
run_check "py_compile" python3 -m py_compile "$script"
run_check "no_forbidden_process_or_network_imports" bash -c "! rg -n 'subprocess|requests|urllib|websocket|socket|paramiko|ssh|scp|rsync|systemctl|service ' '$script'"

blocked_preflight="$out_dir/blocked_preflight.json"
cat > "$blocked_preflight" <<'JSON'
{
  "artifact": "xuan_b27_dplus_compliant_backtest_input_preflight",
  "status": "BLOCKED_COMPLIANT_BACKTEST_INPUTS_UNAVAILABLE",
  "preflight_passed": false,
  "missing_roots": ["/mnt/poly-cache/taker_buy_signal_core_v2_strict_l1"],
  "strict_ready_label_count": 0,
  "completion_ready_label_count": 0,
  "public_account_execution_truth_v1": {"ready": false},
  "raw_replay_scanned": false,
  "duckdb_tables_read": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "required_report_declaration_fields": ["data_root", "dataset_type", "labels", "days", "market_prefix", "assets", "row_count", "excluded_20260514_20260515", "contains_20260518", "includes_public_account_execution_truth_v1"],
  "side_effects": {
    "raw_replay_scanned": false,
    "raw_replay_written": false,
    "duckdb_tables_read": false,
    "orders_sent": false,
    "cancels_sent": false,
    "redeems_sent": false,
    "auth_network_started": false,
    "shared_ingress_modified": false,
    "broker_modified": false,
    "service_control_used": false
  }
}
JSON

blocked_dir="$out_dir/blocked_plan"
run_check "blocked_plan_run" python3 "$script" \
  --input-preflight "$blocked_preflight" \
  --output-dir "$blocked_dir"
run_check "blocked_plan_manifest" python3 - "$blocked_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
side_effects = data.get("side_effects") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_compliant_backtest_run_plan"
    and data.get("status") == "BLOCKED_COMPLIANT_BACKTEST_INPUTS_UNAVAILABLE"
    and data.get("ready_to_run_compliant_backtest") is False
    and data.get("inputs_available") is False
    and data.get("requires_compliant_store_adapter") is True
    and data.get("compliant_store_adapter_ready") is False
    and data.get("raw_replay_scanned") is False
    and data.get("duckdb_tables_read") is False
    and all(value is False for value in side_effects.values())
)
raise SystemExit(0 if ok else 1)
PY

pass_preflight="$out_dir/pass_preflight.json"
cat > "$pass_preflight" <<'JSON'
{
  "artifact": "xuan_b27_dplus_compliant_backtest_input_preflight",
  "status": "PASS_COMPLIANT_BACKTEST_INPUTS_AVAILABLE",
  "preflight_passed": true,
  "strict_root": "/fixture/strict",
  "completion_root": "/fixture/completion",
  "public_truth_root": "/fixture/public_truth",
  "missing_roots": [],
  "strict_ready_label_count": 2,
  "completion_ready_label_count": 2,
  "public_account_execution_truth_v1": {"ready": true},
  "raw_replay_scanned": false,
  "duckdb_tables_read": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "required_report_declaration_fields": ["data_root", "dataset_type", "labels", "days", "market_prefix", "assets", "row_count", "excluded_20260514_20260515", "contains_20260518", "includes_public_account_execution_truth_v1"],
  "side_effects": {
    "raw_replay_scanned": false,
    "raw_replay_written": false,
    "duckdb_tables_read": false,
    "orders_sent": false,
    "cancels_sent": false,
    "redeems_sent": false,
    "auth_network_started": false,
    "shared_ingress_modified": false,
    "broker_modified": false,
    "service_control_used": false
  }
}
JSON

adapter_dir="$out_dir/adapter_plan"
run_check "adapter_plan_run" python3 "$script" \
  --input-preflight "$pass_preflight" \
  --output-dir "$adapter_dir"
run_check "adapter_plan_manifest" python3 - "$adapter_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "BLOCKED_COMPLIANT_BACKTEST_ADAPTER_NOT_IMPLEMENTED"
    and data.get("inputs_available") is True
    and data.get("ready_to_run_compliant_backtest") is False
    and data.get("existing_runner_input_type") == "local_sqlite_snapshot_btc5m_market_ticks"
    and data.get("required_dataset_type") == "declared_strict_cache_plus_completion_store"
    and data.get("requires_compliant_store_adapter") is True
    and data.get("compliant_store_adapter_ready") is False
    and data.get("raw_replay_scanned") is False
    and data.get("duckdb_tables_read") is False
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_compliant_backtest_run_plan_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_compliant_backtest_run_plan_gate",
  "created_utc": "$ts",
  "blocked_plan_manifest": "$blocked_dir/manifest.json",
  "adapter_plan_manifest": "$adapter_dir/manifest.json",
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "raw_replay_scanned": false
}
JSON

printf '%s\n' "$out_dir/manifest.json"
[ "$status" = "PASS" ]
