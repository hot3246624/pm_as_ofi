#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_compliant_metrics_runner_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
status="PASS"
: > "$log"

run_check() {
  local name="$1"
  shift
  printf '== %s ==\n' "$name" >> "$log"
  if ! "$@" >> "$log" 2>&1; then
    printf 'CHECK_FAILED %s\n' "$name" | tee -a "$log"
    status="FAIL"
  fi
}

script="scripts/xuan_b27_dplus_compliant_metrics_runner.py"
fixture="$out_dir/fixture"
pass_out="$out_dir/pass_run"
blocked_out="$out_dir/blocked_run"

run_check "script_exists" test -f "$script"
run_check "py_compile" python3 -m py_compile "$script"
run_check "no_forbidden_process_or_network_imports" \
  bash -c "! rg -n 'subprocess|requests|urllib|websocket|socket|paramiko|ssh|scp|rsync|systemctl|service ' '$script'"

run_check "build_fixture_duckdbs" env FIXTURE="$fixture" uv run --with duckdb python - <<'PY'
import duckdb
import json
import os
from pathlib import Path

fixture = Path(os.environ["FIXTURE"])
strict_dir = fixture / "strict" / "20260513"
completion_dir = fixture / "completion" / "20260513"
public_dir = fixture / "public_account_execution_truth_v1" / "20260502_20260513"
for path in (strict_dir, completion_dir, public_dir):
    path.mkdir(parents=True, exist_ok=True)

(strict_dir / "CACHE_MANIFEST.json").write_text(json.dumps({"days": ["2026-05-13"]}) + "\n")
(completion_dir / "EVENT_STORE_MANIFEST.json").write_text(json.dumps({"days": ["2026-05-13"]}) + "\n")
(public_dir / "EVENT_STORE_MANIFEST.json").write_text(json.dumps({"days": ["2026-05-13"]}) + "\n")

con = duckdb.connect(str(strict_dir / "cache.duckdb"))
con.execute(
    """
    create table taker_buy_signal_candidates(
        day varchar,
        slug varchar,
        condition_id varchar,
        trigger_ts_ms bigint,
        first_side varchar,
        first_l2_vwap double,
        first_l2_filled double
    )
    """
)
con.execute(
    """
    insert into taker_buy_signal_candidates values
    ('2026-05-13', 'btc-updown-5m-1', 'cond1', 1000000, 'YES', 0.35, 10.0)
    """
)
con.close()

con = duckdb.connect(str(completion_dir / "event_store.duckdb"))
con.execute(
    """
    create table completion_unwind_events(
        day varchar,
        condition_id varchar,
        ts_ms bigint,
        side varchar,
        buy_full_10 boolean,
        buy_vwap_10 double,
        buy_filled_10 double,
        buy_worst_px_10 double
    )
    """
)
con.execute(
    """
    insert into completion_unwind_events values
    ('2026-05-13', 'cond1', 1005000, 'NO', true, 0.55, 10.0, 0.55)
    """
)
con.close()

con = duckdb.connect(str(public_dir / "event_store.duckdb"))
con.execute(
    """
    create table public_account_execution_events(
        day varchar,
        condition_id varchar,
        event_kind varchar,
        fill_price double,
        fill_qty double
    )
    """
)
con.execute(
    """
    insert into public_account_execution_events values
    ('2026-05-13', 'cond1', 'fill', 0.35, 10.0)
    """
)
con.close()

preflight = {
    "artifact": "xuan_b27_dplus_compliant_backtest_input_preflight",
    "status": "PASS_COMPLIANT_BACKTEST_INPUTS_AVAILABLE",
    "preflight_passed": True,
    "raw_replay_scanned": False,
    "duckdb_tables_read": False,
    "orders_sent": False,
    "cancels_sent": False,
    "redeems_sent": False,
    "auth_network_started": False,
    "side_effects": {
        "raw_replay_scanned": False,
        "raw_replay_written": False,
        "duckdb_tables_read": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "shared_ingress_modified": False,
        "broker_modified": False,
        "service_control_used": False,
    },
}
adapter = {
    "artifact": "xuan_b27_dplus_compliant_adapter_join_probe",
    "status": "PASS_COMPLIANT_ADAPTER_JOIN_FEASIBLE",
    "probe_passed": True,
    "row_count": 3,
    "raw_replay_scanned": False,
    "orders_sent": False,
    "cancels_sent": False,
    "redeems_sent": False,
    "auth_network_started": False,
    "side_effects": {
        "raw_replay_scanned": False,
        "raw_replay_written": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "shared_ingress_modified": False,
        "broker_modified": False,
        "service_control_used": False,
    },
}
(fixture / "preflight.json").write_text(json.dumps(preflight) + "\n")
(fixture / "adapter.json").write_text(json.dumps(adapter) + "\n")
PY

run_check "pass_run" uv run --with duckdb python "$script" \
  --input-preflight "$fixture/preflight.json" \
  --adapter-join-probe "$fixture/adapter.json" \
  --strict-root "$fixture/strict" \
  --completion-root "$fixture/completion" \
  --public-audit-db "$fixture/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb" \
  --clips 10 \
  --pair-caps 0.98 \
  --output-dir "$pass_out"

run_check "pass_manifest_contract" python3 - "$pass_out/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
decl = data.get("backtest_data_declaration") or {}
best = data.get("best_run") or {}
metrics = best.get("metrics") or {}
required = {
    "data_root",
    "dataset_type",
    "labels",
    "days",
    "market_prefix",
    "assets",
    "row_count",
    "excluded_20260514_20260515",
    "contains_20260518",
    "includes_public_account_execution_truth_v1",
}
ok = (
    data.get("artifact") == "xuan_b27_dplus_compliant_metrics_runner"
    and data.get("status") == "KEEP_DESCRIPTIVE_METRICS_TARGET"
    and data.get("input_safe") is True
    and required.issubset(decl)
    and data.get("dataset_type") == "local_poly_backtest_strict_cache_plus_completion_store_plus_public_audit_metrics"
    and data.get("excluded_20260514_20260515") is True
    and data.get("contains_20260518") is False
    and data.get("includes_public_account_execution_truth_v1") is True
    and data.get("can_support_strategy_promotion") is False
    and metrics.get("candidate_count") == 1
    and metrics.get("paired_candidate_count") == 1
    and metrics.get("qty_residual_rate") == 0.0
    and metrics.get("fee_worst_case_pnl", 0) > 0
    and data.get("raw_replay_scanned") is False
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY

run_check "blocked_unsafe_adapter_fails_closed" bash -c "
  python3 - <<'PY' > '$fixture/bad_adapter.json'
import json
print(json.dumps({
    'artifact': 'xuan_b27_dplus_compliant_adapter_join_probe',
    'status': 'BLOCKED',
    'probe_passed': False,
    'raw_replay_scanned': False,
    'orders_sent': False,
    'cancels_sent': False,
    'redeems_sent': False,
    'auth_network_started': False,
    'side_effects': {'raw_replay_scanned': False}
}))
PY
  set +e
  uv run --with duckdb python '$script' \
    --input-preflight '$fixture/preflight.json' \
    --adapter-join-probe '$fixture/bad_adapter.json' \
    --strict-root '$fixture/strict' \
    --completion-root '$fixture/completion' \
    --public-audit-db '$fixture/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb' \
    --clips 10 \
    --pair-caps 0.98 \
    --output-dir '$blocked_out'
  code=\$?
  set -e
  test \$code -eq 2
"

run_check "blocked_manifest_contract" python3 - "$blocked_out/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
ok = (
    data.get("status") == "BLOCKED_COMPLIANT_METRICS_INPUT_GAP"
    and data.get("input_safe") is False
    and data.get("raw_replay_scanned") is False
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_compliant_metrics_runner_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_compliant_metrics_runner_gate",
  "created_utc": "$ts",
  "pass_manifest": "$pass_out/manifest.json",
  "blocked_manifest": "$blocked_out/manifest.json",
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "raw_replay_scanned": false
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
