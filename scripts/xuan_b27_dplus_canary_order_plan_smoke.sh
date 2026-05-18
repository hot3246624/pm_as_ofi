#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_canary_order_plan_smoke_$ts}"
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

planner="src/polymarket/xuan_b27_dplus_order_plan.rs"

run_check "planner_exists" test -f "$planner"
run_check "planner_registered" rg -q 'pub mod xuan_b27_dplus_order_plan' src/polymarket/mod.rs
run_check "planner_has_preview_only_intent_type" rg -q 'XuanB27DplusOrderIntentPreview|preview_only: true|submitted: false' "$planner"
run_check "planner_requires_explicit_canary_approval" rg -q 'explicit_canary_approval_missing' "$planner"
run_check "planner_blocks_unknown_account_truth" rg -q 'account_truth_unknown' "$planner"
run_check "planner_enforces_post_only_and_no_taker" rg -q 'post_only_required|passive_taker_not_allowed|allow_taker: false' "$planner"
run_check "planner_enforces_g2_caps" rg -q 'max_live_orders_must_be_one_or_two|target_qty_outside_g2_cap|max_open_cost_usdc_outside_g2_cap' "$planner"
run_check "planner_uses_order_attempt_trace" rg -q 'OrderAttemptTrace' "$planner"
run_check "planner_preserves_preview_order_attempt_id" rg -Fq 'order_attempt_id: format!("{candidate_id}:attempt:preview")' "$planner"
run_check "planner_has_no_oms_or_executor_calls" \
  bash -c "! rg -q 'OrderManagerCmd|SetTarget|SetTargetWithTrace|ExecuteIntent|Executor|post_order|place_post_only_order|create_order|CancelAll|execute_market_merge|maybe_auto_claim|run_auto_claim_once' '$planner'"
run_check "planner_tests_pass" cargo test xuan_b27_dplus_canary_order_plan --lib

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_canary_order_plan_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_canary_order_plan_gate",
  "planner": "$planner",
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
