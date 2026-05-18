#!/usr/bin/env bash
set -euo pipefail

ts=$(date -u +%Y%m%dT%H%M%SZ)
out_dir="xuan_research_artifacts/xuan_b27_dplus_compliant_backtest_input_preflight_smoke_${ts}"
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

script="scripts/xuan_b27_dplus_compliant_backtest_input_preflight.py"

run_check "script_exists" test -f "$script"
run_check "py_compile" python3 -m py_compile "$script"
run_check "no_forbidden_process_or_network_imports" bash -c "! rg -n 'subprocess|requests|urllib|websocket|socket|paramiko|ssh|scp|rsync|systemctl|service ' '$script'"

fixture="$out_dir/fixture"
strict_root="$fixture/strict"
completion_root="$fixture/completion"
truth_root="$fixture/public_account_execution_truth_v1/20260502_20260513"
mkdir -p "$strict_root/20260502_20260507" "$strict_root/20260516" \
  "$completion_root/20260502_20260508" "$completion_root/20260517" "$truth_root"
printf '{}\n' > "$strict_root/20260502_20260507/CACHE_MANIFEST.json"
printf '{}\n' > "$strict_root/20260516/CACHE_MANIFEST.json"
printf '{}\n' > "$completion_root/20260502_20260508/EVENT_STORE_MANIFEST.json"
: > "$completion_root/20260502_20260508/event_store.duckdb"
printf '{}\n' > "$completion_root/20260517/EVENT_STORE_MANIFEST.json"
: > "$completion_root/20260517/event_store.duckdb"
printf '{}\n' > "$truth_root/EVENT_STORE_MANIFEST.json"
: > "$truth_root/event_store.duckdb"

pass_dir="$out_dir/pass_case"
run_check "fixture_pass" python3 "$script" \
  --strict-root "$strict_root" \
  --completion-root "$completion_root" \
  --public-truth-root "$truth_root" \
  --strict-labels "20260502_20260507,20260516" \
  --completion-labels "20260502_20260508,20260517" \
  --output-dir "$pass_dir"

run_check "fixture_pass_manifest" python3 - "$pass_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
side_effects = data.get("side_effects") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_compliant_backtest_input_preflight"
    and data.get("status") == "PASS_COMPLIANT_BACKTEST_INPUTS_AVAILABLE"
    and data.get("preflight_passed") is True
    and data.get("can_run_compliant_backtest_locally") is True
    and data.get("strict_ready_label_count") == 2
    and data.get("completion_ready_label_count") == 2
    and data.get("dataset_type") == "local_poly_backtest_cache_store"
    and data.get("public_account_execution_truth_v1", {}).get("event_store_duckdb_exists") is True
    and data.get("public_account_execution_truth_v1", {}).get("manifest_exists") is True
    and data.get("collector_scanned") is False
    and data.get("raw_replay_scanned") is False
    and data.get("duckdb_tables_read") is False
    and all(value is False for value in side_effects.values())
)
raise SystemExit(0 if ok else 1)
PY

auto_dir="$out_dir/auto_discovery_case"
run_check "fixture_auto_discovery_pass" python3 "$script" \
  --strict-root "$strict_root" \
  --completion-root "$completion_root" \
  --public-truth-root "$truth_root" \
  --output-dir "$auto_dir"

run_check "fixture_auto_discovery_manifest" python3 - "$auto_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "PASS_COMPLIANT_BACKTEST_INPUTS_AVAILABLE"
    and data.get("strict_labels_expected") == ["20260502_20260507", "20260516"]
    and data.get("completion_labels_expected") == ["20260502_20260508", "20260517"]
    and data.get("excluded_days") == ["20260514", "20260515"]
    and data.get("not_ready_days") == ["20260518"]
)
raise SystemExit(0 if ok else 1)
PY

missing_dir="$out_dir/missing_case"
set +e
python3 "$script" \
  --strict-root "$fixture/missing_strict" \
  --completion-root "$fixture/missing_completion" \
  --public-truth-root "$fixture/missing_truth" \
  --strict-labels "20260502_20260507" \
  --completion-labels "20260502_20260508" \
  --output-dir "$missing_dir" >> "$log" 2>&1
missing_rc=$?
set -e
if [ "$missing_rc" -ne 2 ]; then
  printf 'CHECK_FAILED missing_case_rc\n' | tee -a "$log"
  status="FAIL"
fi

run_check "missing_case_manifest" python3 - "$missing_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "BLOCKED_COMPLIANT_BACKTEST_INPUTS_UNAVAILABLE"
    and data.get("preflight_passed") is False
    and data.get("can_run_compliant_backtest_locally") is False
    and len(data.get("missing_roots") or []) == 3
    and data.get("raw_replay_scanned") is False
    and data.get("duckdb_tables_read") is False
)
raise SystemExit(0 if ok else 1)
PY

forbidden_dir="$out_dir/forbidden_label_case"
mkdir -p "$strict_root/20260514"
printf '{}\n' > "$strict_root/20260514/CACHE_MANIFEST.json"
set +e
python3 "$script" \
  --strict-root "$strict_root" \
  --completion-root "$completion_root" \
  --public-truth-root "$truth_root" \
  --strict-labels "20260514" \
  --completion-labels "20260502_20260508" \
  --output-dir "$forbidden_dir" >> "$log" 2>&1
forbidden_rc=$?
set -e
if [ "$forbidden_rc" -ne 2 ]; then
  printf 'CHECK_FAILED forbidden_label_rc\n' | tee -a "$log"
  status="FAIL"
fi

run_check "forbidden_label_manifest" python3 - "$forbidden_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
strict = data.get("strict_labels") or []
ok = (
    data.get("status") == "BLOCKED_COMPLIANT_BACKTEST_INPUTS_UNAVAILABLE"
    and data.get("preflight_passed") is False
    and strict
    and strict[0].get("allowed_label") is False
    and "20260514" in (strict[0].get("forbidden_or_not_ready_days") or [])
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_compliant_backtest_input_preflight_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_compliant_backtest_input_preflight_gate",
  "created_utc": "$ts",
  "pass_case_manifest": "$pass_dir/manifest.json",
  "auto_discovery_case_manifest": "$auto_dir/manifest.json",
  "missing_case_manifest": "$missing_dir/manifest.json",
  "forbidden_label_case_manifest": "$forbidden_dir/manifest.json",
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "raw_replay_scanned": false
}
JSON

printf '%s\n' "$out_dir/manifest.json"
[ "$status" = "PASS" ]
