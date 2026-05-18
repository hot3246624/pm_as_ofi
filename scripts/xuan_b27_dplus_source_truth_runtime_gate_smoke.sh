#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_source_truth_runtime_gate_smoke_$ts}"
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

coord="src/polymarket/coordinator.rs"
dplus_coord="src/polymarket/coordinator_xuan_b27_dplus.rs"
tests="src/polymarket/coordinator_tests.rs"
messages="src/polymarket/messages.rs"
executor="src/polymarket/executor.rs"
user_ws="src/polymarket/user_ws.rs"
bin="src/bin/polymarket_v2.rs"
claims="src/polymarket/claims.rs"

run_check "runtime_source_truth_type_exists" \
  rg -q 'struct XuanB27DplusRuntimeSourceTruth|enum XuanB27DplusRuntimeTruthStatus' "$coord"
run_check "runtime_source_truth_defaults_unknown" \
  bash -c "rg -q 'XuanB27DplusRuntimeTruthStatus::Unknown' '$coord' && rg -q 'missing_components' '$coord'"
run_check "runtime_source_truth_all_pass_constructor" \
  rg -q 'fn all_pass' "$coord"
run_check "coordinator_owns_runtime_source_truth_state" \
  bash -c "rg -q 'xuan_b27_dplus_source_truth: XuanB27DplusRuntimeSourceTruth' '$coord' && rg -q 'set_xuan_b27_dplus_runtime_source_truth' '$coord'"
run_check "runtime_dispatch_uses_computed_source_truth_gate" \
  bash -c "rg -q 'runtime_source_truth_ready = source_truth\\.account_truth_ready\\(\\)' '$dplus_coord' && rg -q 'account_truth_ready = cfg\\.account_truth_ready && runtime_source_truth_ready' '$dplus_coord'"
run_check "runtime_event_records_no_secret_source_truth_status" \
  bash -c "rg -q 'source_truth_payload' '$dplus_coord' && rg -q 'missing_components' '$dplus_coord' && rg -q 'failed_components' '$dplus_coord'"
run_check "source_truth_event_type_exists" \
  bash -c "rg -q 'enum XuanB27DplusSourceTruthEvent' '$messages' && rg -q 'OrderAccepted' '$messages' && rg -q 'UserWsFillParsed' '$messages' && rg -q 'WalletSnapshot' '$messages' && rg -q 'RedeemResult' '$messages' && rg -q 'has_redeem_confirmation' '$messages' && rg -q 'CashflowSnapshot' '$messages'"
run_check "executor_order_truth_feed_wired" \
  bash -c "rg -q 'with_xuan_b27_dplus_source_truth_tx' '$executor' && rg -q 'emit_xuan_b27_dplus_order_truth' '$executor' && rg -q 'XuanB27DplusSourceTruthEvent::OrderAccepted' '$executor'"
run_check "user_ws_fill_truth_feed_wired" \
  bash -c "rg -q 'with_xuan_b27_dplus_source_truth_tx' '$user_ws' && rg -q 'emit_user_ws_fill_parsed' '$user_ws' && rg -q 'XuanB27DplusSourceTruthEvent::UserWsFillParsed' '$user_ws'"
run_check "wallet_truth_runtime_producer_wired_to_balance_paths" \
  bash -c "rg -q 'emit_xuan_b27_dplus_wallet_truth' '$executor' && rg -q 'XuanB27DplusSourceTruthEvent::WalletSnapshot' '$executor' && rg -q 'emit_xuan_b27_dplus_wallet_snapshot' '$bin' && rg -q 'fetch_collateral_status' '$bin'"
run_check "redeem_truth_runtime_producer_wired_to_round_claim_recorder_path" \
  bash -c "rg -q 'emit_xuan_b27_dplus_redeem_result_flags' '$bin' && rg -q 'has_redeem_confirmation' '$bin' && rg -q 'XuanB27DplusSourceTruthEvent::RedeemResult' '$bin' && rg -q 'run_round_claim_window' '$bin'"
run_check "redeem_execution_evidence_preserves_tx_hash_or_confirmed_safe_id" \
  bash -c "rg -q 'struct RedeemExecutionEvidence' '$claims' && rg -q 'tx_hash' '$claims' && rg -q 'relayer_tx_id' '$claims' && rg -q 'has_confirmation' '$claims' && rg -q 'xuan_b27_dplus_redeem_execution_evidence_accepts_onchain_tx_hash' '$claims' && rg -q 'xuan_b27_dplus_redeem_execution_evidence_accepts_confirmed_safe_tx_id' '$claims' && rg -q 'xuan_b27_dplus_redeem_execution_evidence_rejects_unconfirmed_safe_submit' '$claims'"
run_check "cashflow_truth_runtime_producer_wired_to_post_merge_balance_snapshot" \
  bash -c "rg -q 'emit_xuan_b27_dplus_cashflow_snapshot' '$bin' && rg -q 'XuanB27DplusSourceTruthEvent::CashflowSnapshot' '$bin' && rg -q 'Recycler post-merge balance' '$bin'"
run_check "coordinator_source_truth_channel_wired" \
  bash -c "rg -q 'with_xuan_b27_dplus_source_truth_rx' '$coord' && rg -q 'handle_xuan_b27_dplus_source_truth_event' '$dplus_coord' && rg -q 'xuan_b27_dplus_source_truth_rx.recv' '$coord'"
run_check "main_runtime_wires_source_truth_channel_to_executor_userws_and_coordinator" \
  bash -c "rg -q 'mpsc::channel::<XuanB27DplusSourceTruthEvent>' '$bin' && rg -q 'with_xuan_b27_dplus_source_truth_rx' '$bin' && rg -q 'executor\\.with_xuan_b27_dplus_source_truth_tx' '$bin' && rg -q 'user_ws\\.with_xuan_b27_dplus_source_truth_tx' '$bin'"
run_check "runtime_source_truth_not_env_overridable" \
  bash -c "! rg -q 'PM_XUAN_B27_DPLUS_.*(ORDER_TRUTH|FILL_TRUTH|WALLET|REDEEM|CASHFLOW|SOURCE_TRUTH|ACCOUNT_TRUTH)' src/polymarket/coordinator.rs src/bin src/polymarket"
run_check "tests_cover_unknown_failed_and_all_pass_source_truth" \
  bash -c "rg -q 'runtime_source_truth_defaults_unknown' '$tests' && rg -q 'runtime_wiring_emits_no_commands_with_unknown_source_truth' '$tests' && rg -q 'runtime_wiring_emits_no_commands_with_wallet_truth_unknown' '$tests' && rg -q 'runtime_wiring_emits_no_commands_with_cashflow_truth_failed' '$tests' && rg -q 'runtime_wiring_can_emit_traced_targets_only_after_all_gates' '$tests'"
run_check "tests_cover_event_feed_bridge" \
  bash -c "rg -q 'source_truth_events_feed_runtime_gate' '$tests' && rg -q 'source_truth_fill_missing_trade_id_stays_unknown' '$tests' && rg -q 'source_truth_wallet_invalid_fails_runtime_gate' '$tests' && rg -q 'source_truth_redeem_missing_tx_hash_stays_unknown' '$tests' && rg -q 'source_truth_confirmed_safe_redeem_id_passes_without_tx_hash' '$tests' && rg -q 'source_truth_cashflow_missing_id_stays_unknown' '$tests' && rg -q 'source_truth_feed_ignored_for_non_dplus_strategy' '$tests'"
run_check "runtime_source_truth_tests_pass" cargo test xuan_b27_dplus --lib

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_source_truth_runtime_gate_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_source_truth_runtime_gate",
  "coordinator": "$dplus_coord",
  "source_truth_owner": "$coord",
  "source_truth_event_type": "$messages",
  "executor_order_feed": "$executor",
  "user_ws_fill_feed": "$user_ws",
  "redeem_execution_evidence": "$claims",
  "main_runtime_wiring": "$bin",
  "event_feeds_partially_wired": true,
  "wallet_runtime_producer_wired": true,
  "redeem_runtime_producer_wired_with_tx_hash_or_confirmed_safe_id": true,
  "cashflow_runtime_producer_wired": true,
  "wallet_redeem_cashflow_runtime_producers_wired": true,
  "redeem_truth_can_pass_from_runtime_producer": true,
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
