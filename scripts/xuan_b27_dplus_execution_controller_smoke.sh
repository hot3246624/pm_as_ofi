#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_execution_controller_smoke_$ts}"
mkdir -p "$out_dir"

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

controller="src/polymarket/xuan_b27_dplus_execution_controller.rs"

run_check "controller_exists" test -f "$controller"
run_check "controller_registered" rg -q 'pub mod xuan_b27_dplus_execution_controller' src/polymarket/mod.rs
run_check "controller_calls_preview_planner" rg -q 'build_xuan_b27_dplus_canary_order_plan' "$controller"
run_check "controller_disabled_status_exists" rg -q 'PREVIEW_HELD_CONTROLLER_DISABLED|controller_disabled' "$controller"
run_check "controller_blocks_missing_oms_adapter" rg -q 'oms_adapter_not_implemented' "$controller"
run_check "controller_never_reports_oms_commands" rg -q 'oms_command_count: 0|orders_submitted: false' "$controller"
run_check "controller_tests_cover_missing_approval_truth_and_risk" \
  rg -q 'blocks_missing_explicit_approval|blocks_unknown_account_truth|blocks_risk_cap_violations' "$controller"
run_check "controller_has_no_oms_or_executor_calls" \
  bash -c "! rg -q 'OrderManagerCmd|SetTarget|SetTargetWithTrace|ExecuteIntent|Executor|post_order|place_post_only_order|create_order|CancelAll|execute_market_merge|maybe_auto_claim|run_auto_claim_once' '$controller'"
run_check "controller_tests_pass" cargo test xuan_b27_dplus_controller --lib

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_execution_controller_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_execution_controller_gate",
  "controller": "$controller",
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
