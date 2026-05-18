#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_observer_safety_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
checks="$out_dir/static_safety_checks.txt"
: > "$checks"

run_check() {
  local name="$1"
  shift
  if "$@"; then
    printf 'PASS %s\n' "$name" >> "$checks"
  else
    printf 'FAIL %s\n' "$name" >> "$checks"
    status="FAIL"
  fi
}

cargo test xuan_b27_dplus --lib > "$out_dir/cargo_test_xuan_b27_dplus.log" 2>&1 || status="FAIL"
cargo test strategy::tests --lib > "$out_dir/cargo_test_strategy.log" 2>&1 || status="FAIL"
cargo test user_ws --lib > "$out_dir/cargo_test_user_ws.log" 2>&1 || status="FAIL"
cargo test executor --lib > "$out_dir/cargo_test_executor.log" 2>&1 || status="FAIL"
cargo test order_manager --lib > "$out_dir/cargo_test_order_manager.log" 2>&1 || status="FAIL"

run_check "strategy_registered" \
  rg -q 'XuanB27Dplus' src/polymarket/strategy.rs
run_check "coordinator_force_clears_quotes" \
  rg -q 'if self\.cfg\.strategy == StrategyKind::XuanB27Dplus' src/polymarket/coordinator.rs
run_check "coordinator_clears_to_default_quotes" \
  rg -q 'quotes = StrategyQuotes::default\(\);' src/polymarket/coordinator.rs
run_check "observer_event_exists" \
  rg -q 'xuan_b27_dplus_observer_tick' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "canary_event_blocked" \
  rg -q 'xuan_b27_dplus_canary_blocked_not_implemented' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "observer_reports_no_orders" \
  rg -q '"orders_sent_by_this_module": false' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "source_of_truth_unknown_gate" \
  rg -q 'candidate_only_is_unknown' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "user_ws_structured_fill_truth_event" \
  rg -q 'user_ws_fill_parsed' src/polymarket/user_ws.rs
run_check "executor_order_accepted_records_order_id" \
  rg -q '"order_id": order_id\.clone\(\)' src/polymarket/executor.rs
run_check "executor_order_accepted_records_venue_order_id" \
  rg -q '"venue_order_id": order_id\.clone\(\)' src/polymarket/executor.rs
run_check "order_attempt_trace_type_exists" \
  rg -q 'struct OrderAttemptTrace' src/polymarket/messages.rs
run_check "set_target_with_trace_exists" \
  rg -q 'SetTargetWithTrace' src/polymarket/messages.rs
run_check "trade_intent_has_optional_trace" \
  rg -q 'trace: Option<OrderAttemptTrace>' src/polymarket/messages.rs
run_check "dplus_observer_has_order_attempt_preview" \
  rg -q 'order_attempt_trace_preview' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "dplus_observer_preview_is_not_submitted" \
  rg -q '"submitted": false' src/polymarket/coordinator_xuan_b27_dplus.rs
run_check "dplus_observer_preview_only_flag" \
  rg -q 'order_attempt_preview_only' src/polymarket/coordinator_xuan_b27_dplus.rs

if rg -q 'StrategyIntent|yes_buy|no_buy|yes_sell|no_sell' src/polymarket/strategy/xuan_b27_dplus.rs; then
  printf 'FAIL strategy_file_contains_order_intent_surface\n' >> "$checks"
  status="FAIL"
else
  printf 'PASS strategy_file_has_no_order_intent_surface\n' >> "$checks"
fi

if rg -q 'orders_sent_by_this_module": true|would_place": true' src/polymarket/coordinator_xuan_b27_dplus.rs; then
  printf 'FAIL observer_payload_has_positive_order_flag\n' >> "$checks"
  status="FAIL"
else
  printf 'PASS observer_payload_has_no_positive_order_flag\n' >> "$checks"
fi

cat > "$out_dir/manifest.json" <<EOF
{
  "schema_version": 1,
  "artifact": "xuan_b27_dplus_observer_safety_smoke",
  "status": "$status",
  "created_utc": "$ts",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_order_observer_safety",
  "orders_sent": false,
  "checks_file": "$checks",
  "cargo_xuan_b27_dplus_log": "$out_dir/cargo_test_xuan_b27_dplus.log",
  "cargo_strategy_log": "$out_dir/cargo_test_strategy.log",
  "cargo_user_ws_log": "$out_dir/cargo_test_user_ws.log",
  "cargo_executor_log": "$out_dir/cargo_test_executor.log",
  "cargo_order_manager_log": "$out_dir/cargo_test_order_manager.log"
}
EOF

printf '%s\n' "$out_dir/manifest.json"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi
