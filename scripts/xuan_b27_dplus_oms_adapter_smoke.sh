#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_oms_adapter_smoke_$ts}"
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

adapter="src/polymarket/xuan_b27_dplus_oms_adapter.rs"

run_check "adapter_exists" test -f "$adapter"
run_check "adapter_registered" rg -q 'pub mod xuan_b27_dplus_oms_adapter' src/polymarket/mod.rs
run_check "adapter_imports_oms_command_type_only" rg -q 'OrderManagerCmd' "$adapter"
run_check "adapter_requires_controller_ready" rg -q 'PreviewReadyForOmsAdapter|controller_not_ready_for_oms_adapter' "$adapter"
run_check "adapter_requires_explicit_approval" rg -q 'explicit_canary_approval_missing' "$adapter"
run_check "adapter_requires_account_truth" rg -q 'account_truth_unknown' "$adapter"
run_check "adapter_requires_risk_caps" rg -q 'risk_caps_not_ready' "$adapter"
run_check "adapter_has_disabled_hold_state" rg -q 'COMMANDS_HELD_ADAPTER_DISABLED|oms_adapter_disabled' "$adapter"
run_check "adapter_reports_no_submission" rg -q 'orders_submitted: false' "$adapter"
run_check "adapter_tests_cover_no_command_gates" \
  rg -q 'blocks_missing_explicit_approval|blocks_unknown_account_truth|blocks_risk_caps_not_ready|holds_commands_when_disabled' "$adapter"
run_check "adapter_has_no_sender_executor_or_network_calls" \
  bash -c "! rg -q 'mpsc|\\.send\\(|Sender<|tokio|Executor|execute_intent|post_order|place_post_only_order|create_order|cancel_order|CancelAll|execute_market_merge|maybe_auto_claim|run_auto_claim_once|websocket|connect_async|systemctl|ssh|scp|rsync' '$adapter'"
run_check "adapter_tests_pass" cargo test xuan_b27_dplus_oms_adapter --lib

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_oms_adapter_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_oms_adapter_gate",
  "adapter": "$adapter",
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
