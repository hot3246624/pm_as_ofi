#!/usr/bin/env bash
set -euo pipefail

ts=$(date -u +%Y%m%dT%H%M%SZ)
out_dir="xuan_research_artifacts/xuan_b27_dplus_scope_limited_completion_passive_probe_summary_smoke_${ts}"
mkdir -p "$out_dir/runner" "$out_dir/schema_probe"
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

script="scripts/xuan_b27_dplus_scope_limited_completion_passive_probe_summary.py"
run_check "script_exists" test -f "$script"
run_check "py_compile" python3 -m py_compile "$script"
run_check "no_forbidden_process_or_network_imports" bash -c "! rg -n 'subprocess|requests|urllib|websocket|socket|paramiko|ssh|scp|rsync|systemctl|service ' '$script'"

cat > "$out_dir/runner/manifest.json" <<'JSON'
{
  "stores": [["/tmp/xuan_frontier_data/completion_unwind_event_store_v2/20260509/event_store.duckdb", ["2026-05-09"]]],
  "config_count": 2
}
JSON
cat > "$out_dir/runner/combined_results.json" <<'JSON'
[
  {
    "name": "positive_research_only",
    "seed_actions": 10,
    "pair_actions": 8,
    "gross_buy_qty": 20.0,
    "gross_buy_cost": 10.0,
    "pair_qty": 8.0,
    "residual_qty": 4.0,
    "residual_cost": 1.0,
    "pair_pnl": 0.2,
    "net_pnl": 0.5,
    "actual_settle_pnl": 0.5,
    "stress100_actual_pnl": -0.1,
    "worst_residual_net_pnl": -0.8
  },
  {
    "name": "negative",
    "seed_actions": 5,
    "pair_actions": 2,
    "net_pnl": -0.2,
    "stress100_actual_pnl": -0.4,
    "worst_residual_net_pnl": -1.0
  }
]
JSON
cat > "$out_dir/schema_probe/manifest.json" <<'JSON'
{
  "artifact": "xuan_b27_dplus_completion_store_schema_probe",
  "status": "PASS_SCOPE_LIMITED_COMPLETION_STORE_SCHEMA_PROBE",
  "probes": [{"label": "20260509", "row_count": 1000}]
}
JSON

summary_dir="$out_dir/summary"
run_check "summary_run" python3 "$script" \
  --runner-dir "$out_dir/runner" \
  --schema-probe "$out_dir/schema_probe/manifest.json" \
  --output-dir "$summary_dir"
run_check "summary_manifest" python3 - "$summary_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "PASS_SCOPE_LIMITED_COMPLETION_PASSIVE_PROBE_SUMMARY"
    and data.get("summary_passed") is True
    and data.get("run_count") == 2
    and data.get("nonzero_seed_run_count") == 2
    and data.get("positive_net_pnl_run_count") == 1
    and data.get("best_net_pnl") == 0.5
    and data.get("best_stress100_actual_pnl") == -0.1
    and data.get("row_count") == 1000
    and data.get("dataset_type") == "scope_limited_completion_unwind_event_store_v2"
    and data.get("can_support_strategy_promotion") is False
    and data.get("requires_compliant_backtest_dataset_for_promotion") is True
    and data.get("raw_replay_scanned") is False
    and data.get("orders_sent") is False
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_scope_limited_completion_passive_probe_summary_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_scope_limited_completion_passive_probe_summary_gate",
  "created_utc": "$ts",
  "summary_manifest": "$summary_dir/manifest.json",
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "raw_replay_scanned": false
}
JSON

printf '%s\n' "$out_dir/manifest.json"
[ "$status" = "PASS" ]
