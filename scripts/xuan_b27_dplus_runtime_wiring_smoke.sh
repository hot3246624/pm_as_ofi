#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_runtime_wiring_smoke_$ts}"
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

coord="src/polymarket/coordinator_xuan_b27_dplus.rs"
main_coord="src/polymarket/coordinator.rs"
tests="src/polymarket/coordinator_tests.rs"

run_check "runtime_method_exists" rg -q 'maybe_dispatch_xuan_b27_dplus_canary_commands' "$coord"
run_check "runtime_called_from_tick_loop" rg -q 'maybe_dispatch_xuan_b27_dplus_canary_commands' "$main_coord"
run_check "runtime_uses_controller_and_adapter" \
  bash -c "rg -q 'evaluate_xuan_b27_dplus_controller' '$coord' && rg -q 'adapt_xuan_b27_dplus_to_oms_commands' '$coord'"
run_check "runtime_requires_explicit_approval_truth_and_risk_caps" \
  bash -c "rg -q 'explicit_canary_approval' '$coord' && rg -q 'account_truth_ready' '$coord' && rg -q 'xuan_b27_dplus_g2_risk_caps_ready' '$coord'"
run_check "runtime_send_is_only_oms_channel_send" \
  bash -c "rg -q 'self\\.om_tx\\.send\\(command\\)\\.await' '$coord'"
run_check "runtime_has_no_network_broker_or_executor_calls" \
  bash -c "! rg -q 'connect_async|websocket|reqwest|systemctl|run_shared_ingress|ssh|scp|rsync|Executor|post_order|place_post_only_order|create_order|cancel_order|execute_market_merge|maybe_auto_claim|run_auto_claim_once' '$coord'"
run_check "runtime_account_truth_not_env_overridable" \
  bash -c "! rg -q 'PM_XUAN_B27_DPLUS_ACCOUNT_TRUTH' src/polymarket/coordinator.rs"
run_check "runtime_config_defaults_disabled" \
  bash -c "rg -q 'explicit_canary_approval: false' src/polymarket/coordinator.rs && rg -q 'runtime_wiring_enabled: false' src/polymarket/coordinator.rs && rg -q 'oms_adapter_enabled: false' src/polymarket/coordinator.rs && rg -q 'account_truth_ready: false' src/polymarket/coordinator.rs"
run_check "runtime_tests_cover_no_command_gates" \
  rg -q 'runtime_wiring_emits_no_commands_without_explicit_approval|runtime_wiring_emits_no_commands_without_account_truth|runtime_wiring_emits_no_commands_when_risk_caps_fail|runtime_wiring_can_emit_traced_targets_only_after_all_gates' "$tests"
run_check "runtime_tests_pass" cargo test xuan_b27_dplus_runtime_wiring --lib

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_runtime_wiring_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_runtime_wiring_gate",
  "coordinator": "$coord",
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
