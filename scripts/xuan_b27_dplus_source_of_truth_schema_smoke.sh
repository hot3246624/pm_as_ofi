#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_source_of_truth_schema_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
doc="docs/research/xuan/DPLUS_SOURCE_OF_TRUTH_SCHEMA_ZH.md"
correlation_src="src/polymarket/xuan_b27_dplus_correlation.rs"
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

run_check "schema_doc_exists" test -f "$doc"
run_check "correlation_source_exists" test -f "$correlation_src"
run_check "schema_doc_names_private_truth_boundary" rg -q 'private source-of-truth|公开账户 proxy truth|不能升级为 `PASS`' "$doc"
run_check "schema_doc_defines_candidate_chain" bash -c "rg -q 'candidate_id' '$doc' && rg -q 'order_attempt_trace_preview.order_attempt_id' '$doc'"
run_check "schema_doc_defines_order_attempt_chain" bash -c "rg -q 'client_order_id' '$doc' && rg -q 'venue_order_id' '$doc' && rg -q 'order_accepted.data.correlation.order_attempt_id' '$doc'"
run_check "schema_doc_defines_fill_truth_chain" bash -c "rg -q 'user_ws_fill_parsed' '$doc' && rg -q 'trade_id' '$doc' && rg -q 'fee_rate_bps' '$doc'"
run_check "schema_doc_defines_lot_pair_chain" bash -c "rg -q 'lot_id' '$doc' && rg -q 'pair_id' '$doc' && rg -q 'lot 必须能回溯' '$doc'"
run_check "schema_doc_defines_redeem_cashflow_chain" bash -c "rg -q 'redeem_attempt_id' '$doc' && rg -q 'redeem_tx_hash' '$doc' && rg -q 'cashflow_snapshot_id' '$doc'"
run_check "schema_doc_forbids_public_proxy_substitution" bash -c "rg -q 'public-account audit|public Data API|virtual queue-supported fill|dry-run synthetic fill' '$doc'"
run_check "schema_doc_unknown_pass_rules_present" bash -c "rg -q 'order_fill_verdict\\(\\)' '$doc' && rg -q 'redeem_cashflow_verdict\\(\\)' '$doc' && rg -q '任一缺失 => .*UNKNOWN' '$doc'"

run_check "correlation_has_verdict_enum" rg -q 'enum XuanB27DplusTruthVerdict|XuanB27DplusTruthVerdict' "$correlation_src"
run_check "correlation_has_order_fill_fields" bash -c "rg -q 'client_order_id' '$correlation_src' && rg -q 'venue_order_id' '$correlation_src' && rg -q 'fill_event_id' '$correlation_src' && rg -q 'trade_id' '$correlation_src'"
run_check "correlation_has_redeem_cashflow_fields" bash -c "rg -q 'redeem_attempt_id' '$correlation_src' && rg -q 'redeem_tx_hash' '$correlation_src' && rg -q 'cashflow_snapshot_id' '$correlation_src'"
run_check "correlation_exposes_missing_field_methods" bash -c "rg -q 'missing_order_fill_truth_fields' '$correlation_src' && rg -q 'missing_redeem_cashflow_truth_fields' '$correlation_src'"
run_check "correlation_has_unknown_and_pass_tests" bash -c "rg -q 'candidate_only_correlation_is_unknown_for_order_fill_truth' '$correlation_src' && rg -q 'complete_order_fill_correlation_passes' '$correlation_src' && rg -q 'pair_only_correlation_is_unknown_for_redeem_cashflow_truth' '$correlation_src' && rg -q 'complete_redeem_cashflow_correlation_passes' '$correlation_src'"

run_check "executor_order_accepted_records_correlation" rg -q '"correlation": order_attempt_trace_payload' src/polymarket/executor.rs
run_check "executor_order_accepted_records_venue_order_id" rg -q '"venue_order_id": order_id\.clone\(\)' src/polymarket/executor.rs
run_check "user_ws_fill_parsed_records_private_fill_truth" bash -c "rg -q 'user_ws_fill_parsed' src/polymarket/user_ws.rs && rg -q 'truth_fields' src/polymarket/user_ws.rs"
run_check "recorder_has_redeem_result_event" rg -q 'emit_redeem_result' src/polymarket/recorder.rs
run_check "observer_payload_keeps_candidate_only_unknown" bash -c "rg -q 'candidate_only_is_unknown' src/polymarket/coordinator_xuan_b27_dplus.rs && rg -q 'source_of_truth_gate' src/polymarket/coordinator_xuan_b27_dplus.rs"
run_check "strategy_has_no_order_intent_surface" bash -c "! rg -q 'StrategyIntent|yes_buy|no_buy|yes_sell|no_sell' src/polymarket/strategy/xuan_b27_dplus.rs"

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_source_of_truth_schema_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_source_of_truth_schema_gate",
  "schema_doc": "$ROOT/$doc",
  "correlation_source": "$ROOT/$correlation_src",
  "checks_log": "$log",
  "generated_at_utc": "$ts",
  "orders_sent": false,
  "auth_network_started": false
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
