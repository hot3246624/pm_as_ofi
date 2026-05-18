#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_shadow_trading_report_discovery_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
script="scripts/xuan_b27_dplus_shadow_trading_report_discovery.py"
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

write_runner_report() {
  local dir="$1"
  local pair_pnl="$2"
  local script_name="${3:-xuan_dplus_passive_passive_shadow_runner.py}"
  mkdir -p "$dir"
  cat > "$dir/manifest.json" <<JSON
{
  "kind": "manifest",
  "script": "$script_name",
  "duration_s": 1800,
  "markets": [
    {
      "POLYMARKET_MARKET_SLUG": "btc-updown-5m-1900000000",
      "POLYMARKET_MARKET_ID": "0xmarket",
      "POLYMARKET_YES_ASSET_ID": "yes",
      "POLYMARKET_NO_ASSET_ID": "no"
    }
  ],
  "safety": {
    "orders_sent": false,
    "dry_run": "true",
    "shared_ingress_role": "client",
    "shared_ingress_root": "/srv/pm_as_ofi/shared-ingress-main"
  }
}
JSON
  cat > "$dir/aggregate_report.json" <<JSON
{
  "kind": "aggregate_report",
  "script": "$script_name",
  "slugs": 4,
  "metrics": {
    "candidates": 240,
    "queue_supported_fills": 160,
    "fill_rate": 0.6666666667,
    "pair_actions": 120,
    "filled_qty": 500.0,
    "filled_cost": 220.0,
    "pair_qty": 240.0,
    "qty_pair_share_of_filled": 0.96,
    "pair_pnl": $pair_pnl,
    "roi_on_seed_cost": 0.08,
    "roi_on_filled_cost": 0.10,
    "residual_qty": 2.0,
    "residual_cost": 0.8,
    "material_residual_lots": 0,
    "pair_cost_proxy_p50": 0.92,
    "pair_cost_proxy_p90": 0.96,
    "net_pair_cost_proxy_p50": 0.92,
    "net_pair_cost_proxy_p90": 0.97
  },
  "top_residual_lots": []
}
JSON
}

run_check "discovery_script_exists" test -x "$script"
run_check "discovery_script_py_compile" python3 -m py_compile "$script"
run_check "discovery_script_has_no_network_process_or_raw_replay_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|sqlite3)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|sqlite3)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"

fixture_root="$out_dir/fixtures/fixture_only"
real_root="$out_dir/real_shadow_reports"
invalid_root="$out_dir/real_invalid_reports"
write_runner_report "$fixture_root/fixture_run" 12.5
write_runner_report "$real_root/shadow_run" 12.5
write_runner_report "$invalid_root/bad_run" 12.5 "wrong_runner.py"

fixture_default_dir="$out_dir/fixture_default_discovery"
fixture_allowed_dir="$out_dir/fixture_allowed_discovery"
real_dir="$out_dir/real_discovery"
invalid_dir="$out_dir/invalid_discovery"

run_with_expected_status() {
  local name="$1"
  local expected="$2"
  local manifest="$3"
  shift 3
  {
    printf '== %s ==\n' "$name"
    set +e
    "$@"
    local rc=$?
    set -e
    python3 - "$manifest" "$expected" "$rc" <<'PY'
import json
import pathlib
import sys

manifest = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
rc = int(sys.argv[3])
data = json.loads(manifest.read_text())
if data.get("status") != expected:
    raise SystemExit(f"expected {expected}, got {data.get('status')}")
if expected.startswith("READY_") and rc != 0:
    raise SystemExit(f"expected rc 0, got {rc}")
if not expected.startswith("READY_") and rc == 0:
    raise SystemExit(f"expected nonzero rc for {expected}")
PY
  } >> "$log" 2>&1 || {
    status="FAIL"
    printf 'CHECK_FAILED %s\n' "$name" >> "$log"
  }
}

run_with_expected_status "fixture_default_blocked" "BLOCKED_NO_REAL_SHADOW_TRADING_REPORTS" "$fixture_default_dir/manifest.json" \
  "$script" --scan-root "$fixture_root" --output-dir "$fixture_default_dir"
run_with_expected_status "fixture_allowed_ready" "READY_FOR_SHADOW_TRADING_ACCEPTANCE" "$fixture_allowed_dir/manifest.json" \
  "$script" --scan-root "$fixture_root" --include-fixtures --output-dir "$fixture_allowed_dir"
run_with_expected_status "fixture_simulated_report_ready_when_explicitly_allowed" "READY_FOR_SHADOW_TRADING_ACCEPTANCE" "$real_dir/manifest.json" \
  "$script" --scan-root "$real_root" --include-fixtures --output-dir "$real_dir"
run_with_expected_status "fixture_simulated_invalid_report_blocked_when_explicitly_allowed" "BLOCKED_SHADOW_TRADING_REPORTS_INVALID" "$invalid_dir/manifest.json" \
  "$script" --scan-root "$invalid_root" --include-fixtures --output-dir "$invalid_dir"

run_check "real_discovery_manifest_contract" python3 - "$real_dir/manifest.json" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
best = data.get("best_candidate") or {}
metrics = best.get("trading_metrics") or {}
plan = data.get("exact_shadow_run_plan") or {}
side = data.get("side_effects") or {}
ok = (
    data.get("artifact") == "xuan_b27_dplus_shadow_trading_report_discovery"
    and data.get("status") == "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
    and data.get("scope") == "local_no_network_shadow_trading_report_discovery"
    and data.get("valid_real_candidate_count") == 1
    and best.get("fixture_or_smoke") is True
    and best.get("valid_shape") is True
    and metrics.get("pair_pnl") == 12.5
    and metrics.get("residual_qty") == 2.0
    and plan.get("requires_current_exact_main_thread_approval") is True
    and plan.get("heartbeat_or_generic_approval_is_not_enough") is True
    and plan.get("will_connect_to_shared_ingress_market_socket_if_executed") is True
    and plan.get("will_send_orders_if_executed") is False
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and data.get("started_canary") is False
    and all(value is False for value in side.values())
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_shadow_trading_report_discovery_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_shadow_trading_report_discovery_gate",
  "discovery_script": "$script",
  "real_discovery_manifest": "$real_dir/manifest.json",
  "fixture_default_manifest": "$fixture_default_dir/manifest.json",
  "real_shadow_trading_report_published": false,
  "orders_sent": false,
  "cancels_sent": false,
  "redeems_sent": false,
  "auth_network_started": false,
  "started_observer": false,
  "started_user_ws": false,
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
