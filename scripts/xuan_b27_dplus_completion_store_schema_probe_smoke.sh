#!/usr/bin/env bash
set -euo pipefail

ts=$(date -u +%Y%m%dT%H%M%SZ)
out_dir="xuan_research_artifacts/xuan_b27_dplus_completion_store_schema_probe_smoke_${ts}"
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

script="scripts/xuan_b27_dplus_completion_store_schema_probe.py"
run_check "script_exists" test -f "$script"
run_check "py_compile" python3 -m py_compile "$script"
run_check "no_forbidden_process_or_network_imports" bash -c "! rg -n 'subprocess|requests|urllib|websocket|socket|paramiko|ssh|scp|rsync|systemctl|service ' '$script'"

fixture_root="$out_dir/fixture_completion"
mkdir -p "$fixture_root/20260509"
fixture_db="$fixture_root/20260509/event_store.duckdb"
run_check "create_fixture_duckdb" python3 - "$fixture_db" <<'PY'
import duckdb
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
con = duckdb.connect(str(path))
con.execute(
    """
    create table completion_unwind_events (
      day varchar,
      event_kind varchar,
      event_id bigint,
      ts_ms bigint,
      condition_id varchar,
      offset_s double,
      side varchar,
      winner_side varchar,
      side_bid double,
      side_ask double,
      opp_bid double,
      opp_ask double,
      l1_pair_ask double,
      l1_pair_bid double,
      public_trade_taker_side varchar,
      public_trade_price double,
      public_trade_size double
    )
    """
)
con.execute(
    """
    insert into completion_unwind_events values
    ('2026-05-09','l1_price_change',1,1778284800000,'c1',1.0,'YES','YES',0.48,0.49,0.50,0.51,1.00,0.98,null,null,null),
    ('2026-05-09','public_trade',2,1778284801000,'c1',2.0,'NO','YES',0.50,0.51,0.48,0.49,1.00,0.98,'BUY',0.51,10.0)
    """
)
con.close()
PY

pass_dir="$out_dir/pass_probe"
run_check "pass_probe_run" python3 "$script" \
  --root "$fixture_root" \
  --labels "20260509" \
  --output-dir "$pass_dir"
run_check "pass_probe_manifest" python3 - "$pass_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "PASS_SCOPE_LIMITED_COMPLETION_STORE_SCHEMA_PROBE"
    and data.get("probe_passed") is True
    and data.get("row_count") == 2
    and data.get("can_support_strategy_promotion") is False
    and data.get("requires_compliant_backtest_dataset_for_promotion") is True
    and data.get("duckdb_tables_read") is True
    and data.get("raw_replay_scanned") is False
    and data.get("orders_sent") is False
)
raise SystemExit(0 if ok else 1)
PY

missing_dir="$out_dir/missing_probe"
if python3 "$script" --root "$fixture_root" --labels "20260510" --output-dir "$missing_dir" >> "$log" 2>&1; then
  printf 'CHECK_FAILED missing_label_rejected\n' | tee -a "$log"
  status="FAIL"
fi
run_check "missing_probe_manifest" python3 - "$missing_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
failures = data.get("probes", [{}])[0].get("failures") or []
raise SystemExit(0 if "missing_event_store_duckdb" in failures else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_completion_store_schema_probe_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_scope_limited_completion_store_schema_probe_gate",
  "created_utc": "$ts",
  "pass_probe_manifest": "$pass_dir/manifest.json",
  "missing_probe_manifest": "$missing_dir/manifest.json",
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "raw_replay_scanned": false,
  "duckdb_tables_read": true
}
JSON

printf '%s\n' "$out_dir/manifest.json"
[ "$status" = "PASS" ]
