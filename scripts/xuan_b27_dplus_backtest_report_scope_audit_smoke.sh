#!/usr/bin/env bash
set -euo pipefail

ts=$(date -u +%Y%m%dT%H%M%SZ)
out_dir="xuan_research_artifacts/xuan_b27_dplus_backtest_report_scope_audit_smoke_${ts}"
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

script="scripts/xuan_b27_dplus_backtest_report_scope_audit.py"
run_check "script_exists" test -f "$script"
run_check "py_compile" python3 -m py_compile "$script"
run_check "no_forbidden_process_or_network_imports" bash -c "! rg -n 'subprocess|requests|urllib|websocket|socket|paramiko|ssh|scp|rsync|systemctl|service ' '$script'"

good_grid="$out_dir/good_grid_manifest.json"
cat > "$good_grid" <<'JSON'
{
  "artifact": "xuan_b27_dplus_pair_arb_backtest_json_grid",
  "status": "PASS_OOS_POSITIVE_BACKTEST_CONFIG_DATASET_SCOPE_LIMITED",
  "data_root": "/Users/hot/web3Scientist/poly_trans_research/data/snapshots/btc5m.db",
  "dataset_type": "local_sqlite_snapshot_btc5m",
  "labels": ["btc5m.db"],
  "days": ["20260326", "20260327"],
  "market_prefix": "btc-updown-5m",
  "assets": ["BTC"],
  "row_count": 12345,
  "excluded_20260514_20260515": true,
  "contains_20260518": false,
  "includes_public_account_execution_truth_v1": false,
  "can_support_strategy_promotion": false,
  "raw_replay_scanned": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "side_effects": {
    "raw_replay_scanned": false,
    "orders_sent": false,
    "cancels_sent": false,
    "redeems_sent": false,
    "auth_network_started": false,
    "started_canary": false
  }
}
JSON

good_oos="$out_dir/good_oos_manifest.json"
cat > "$good_oos" <<'JSON'
{
  "artifact": "xuan_b27_dplus_pair_arb_oos_compare",
  "status": "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED",
  "data_root": "/Users/hot/web3Scientist/poly_trans_research/data/snapshots/btc5m.db",
  "dataset_type": "local_sqlite_snapshot_btc5m",
  "labels": ["btc5m.db"],
  "days": ["20260326", "20260327"],
  "market_prefix": "btc-updown-5m",
  "assets": ["BTC"],
  "row_count": 12345,
  "excluded_20260514_20260515": true,
  "contains_20260518": false,
  "includes_public_account_execution_truth_v1": false,
  "can_support_strategy_promotion": false,
  "requires_compliant_backtest_dataset_for_promotion": true,
  "numeric_oos_validation_passed": true,
  "oos_validation_passed": false,
  "raw_replay_scanned": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "side_effects": {
    "raw_replay_scanned": false,
    "orders_sent": false,
    "cancels_sent": false,
    "redeems_sent": false,
    "auth_network_started": false,
    "started_canary": false
  }
}
JSON

pass_dir="$out_dir/pass_audit"
run_check "pass_audit_run" python3 "$script" \
  --manifest "$good_grid" \
  --manifest "$good_oos" \
  --output-dir "$pass_dir"
run_check "pass_audit_manifest" python3 - "$pass_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "PASS_BACKTEST_REPORT_SCOPE_AUDIT"
    and data.get("audit_passed") is True
    and data.get("audited_manifest_count") == 2
    and data.get("scope_limited_report_count") == 2
    and data.get("promotion_supported_report_count") == 0
    and data.get("requires_compliant_backtest_dataset_for_promotion") is True
    and data.get("raw_replay_scanned") is False
    and data.get("orders_sent") is False
)
raise SystemExit(0 if ok else 1)
PY

missing_decl="$out_dir/missing_decl_manifest.json"
cat > "$missing_decl" <<'JSON'
{
  "artifact": "xuan_b27_dplus_pair_arb_backtest_json_grid",
  "status": "PASS_OOS_POSITIVE_BACKTEST_CONFIG",
  "data_root": "/Users/hot/web3Scientist/poly_trans_research/data/snapshots/btc5m.db",
  "dataset_type": "local_sqlite_snapshot_btc5m",
  "labels": ["btc5m.db"],
  "market_prefix": "btc-updown-5m",
  "assets": ["BTC"],
  "row_count": 12345,
  "excluded_20260514_20260515": true,
  "contains_20260518": false,
  "includes_public_account_execution_truth_v1": false,
  "can_support_strategy_promotion": false,
  "raw_replay_scanned": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "side_effects": {"raw_replay_scanned": false}
}
JSON

missing_dir="$out_dir/missing_decl_audit"
if python3 "$script" --manifest "$missing_decl" --output-dir "$missing_dir" >> "$log" 2>&1; then
  printf 'CHECK_FAILED missing_decl_rejected\n' | tee -a "$log"
  status="FAIL"
fi
run_check "missing_decl_failure_recorded" python3 - "$missing_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
failures = data.get("audits", [{}])[0].get("failures") or []
raise SystemExit(0 if "missing_required_declaration_fields" in failures else 1)
PY

bad_promotion="$out_dir/bad_promotion_manifest.json"
cat > "$bad_promotion" <<'JSON'
{
  "artifact": "xuan_b27_dplus_pair_arb_oos_compare",
  "status": "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS",
  "data_root": "/Users/hot/web3Scientist/poly_trans_research/data/snapshots/btc5m.db",
  "dataset_type": "local_sqlite_snapshot_btc5m",
  "labels": ["btc5m.db"],
  "days": ["20260326", "20260327"],
  "market_prefix": "btc-updown-5m",
  "assets": ["BTC"],
  "row_count": 12345,
  "excluded_20260514_20260515": true,
  "contains_20260518": false,
  "includes_public_account_execution_truth_v1": false,
  "can_support_strategy_promotion": true,
  "requires_compliant_backtest_dataset_for_promotion": false,
  "numeric_oos_validation_passed": true,
  "oos_validation_passed": true,
  "raw_replay_scanned": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "side_effects": {"raw_replay_scanned": false}
}
JSON

bad_dir="$out_dir/bad_promotion_audit"
if python3 "$script" --manifest "$bad_promotion" --output-dir "$bad_dir" >> "$log" 2>&1; then
  printf 'CHECK_FAILED bad_promotion_rejected\n' | tee -a "$log"
  status="FAIL"
fi
run_check "bad_promotion_failure_recorded" python3 - "$bad_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
failures = data.get("audits", [{}])[0].get("failures") or []
needed = {
    "scope_limited_report_can_support_promotion_not_false",
    "scope_limited_compare_missing_compliant_dataset_block",
    "scope_limited_compare_validation_not_false",
}
raise SystemExit(0 if needed.issubset(set(failures)) else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_backtest_report_scope_audit_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_backtest_report_scope_audit_gate",
  "created_utc": "$ts",
  "pass_audit_manifest": "$pass_dir/manifest.json",
  "missing_decl_audit_manifest": "$missing_dir/manifest.json",
  "bad_promotion_audit_manifest": "$bad_dir/manifest.json",
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "raw_replay_scanned": false
}
JSON

printf '%s\n' "$out_dir/manifest.json"
[ "$status" = "PASS" ]
