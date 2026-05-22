#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_micro_deficit_source_truth_readiness_smoke_$ts}"
fixture="$out_dir/fixture"
mkdir -p "$fixture/response" "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_micro_deficit_source_truth_readiness.py"
python3 -m py_compile "$script" >> "$log" 2>&1

cat > "$fixture/separator.csv" <<'CSV'
schema_version,profile_name,day,condition_id,slug,source_seed_action_id,source_seed_candidate_row_id,ts_ms,side,seed_qty,seed_px,official_fee_rate,source_risk_direction,pre_seed_same_qty,pre_seed_opp_qty,pre_seed_open_qty,trigger_px,trigger_size,source_pair_qty,source_pair_pnl,source_residual_qty,source_residual_cost,pair_outcome_bucket,residual_tail_outcome_bucket,outcome_labels_are_post_action
candidate_seed_outcome_separator_export_v1,control,2026-05-18,condition-a,slug-a,seed-a,seed-a,1779062400000,NO,1.25,0.42,0.07,repair_or_pairing_improving,0,0.2,0.2,0.48,8,0,0,1.25,0.51,no_pair_observed,residual_tail_cost_share_ge_50pct,True
candidate_seed_outcome_separator_export_v1,control,2026-05-18,condition-b,slug-b,seed-b,seed-b,1779062410000,YES,1.25,0.36,0.07,repair_or_pairing_improving,0.1,0.25,0.35,0.43,9,1.25,0.1,0,0,paired_nonzero,residual_zero,True
candidate_seed_outcome_separator_export_v1,control,2026-05-18,condition-c,slug-c,seed-c,seed-c,1779062420000,YES,1.25,0.36,0.07,risk_increasing,1.0,0,1.0,0.43,9,1.25,0.1,0,0,paired_nonzero,residual_zero,True
CSV

cat > "$fixture/response/action_decisions.csv" <<'CSV'
schema_version,validation_request_id,day,condition_id,slug,candidate_action_id,action_ts_ms,side,seed_qty,seed_px,fee_rate,fee_rate_source,official_fee,expected_decision,decision_reason,validation_status,reasons,source_rows_present,market_meta_ref,book_l1_ref,book_l2_ref,trade_before_ref,trade_after_ref,settlement_ref
truth_validation_response_v1,micro_deficit_repair_guard_source_truth_v1,2026-05-18,condition-a,slug-a,seed-a,1779062400000,NO,1.25,0.42,0.07,fixture,0.01,would_defer,fixture,SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY,,True,replay_store_v2:market_meta:2026-05-18:a,replay_store_v2:md_book_l1:2026-05-18:a,replay_store_v2:md_book_l2:2026-05-18:a,replay_store_v2:md_trades:2026-05-18:a,replay_store_v2:md_trades:2026-05-18:b,replay_store_v2:settlement_records:2026-05-18:a
truth_validation_response_v1,micro_deficit_repair_guard_source_truth_v1,2026-05-18,condition-b,slug-b,seed-b,1779062410000,YES,1.25,0.36,0.07,fixture,0.01,would_defer,fixture,SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY,,True,replay_store_v2:market_meta:2026-05-18:b,replay_store_v2:md_book_l1:2026-05-18:b,replay_store_v2:md_book_l2:2026-05-18:b,replay_store_v2:md_trades:2026-05-18:c,replay_store_v2:md_trades:2026-05-18:d,replay_store_v2:settlement_records:2026-05-18:b
CSV

cat > "$fixture/response/VALIDATION_RESPONSE_MANIFEST.json" <<'JSON'
{
  "status": "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY",
  "public_or_proxy_truth_only": true,
  "private_truth_ready": false,
  "deployable": false,
  "promotion_gate_pass": false,
  "private_table_counts": {
    "own_fill_events": 0,
    "own_inventory_events": 0,
    "own_order_events": 0,
    "user_ws_log": 0
  }
}
JSON

python3 "$script" \
  --separator-csv "$fixture/separator.csv" \
  --response-dir "$fixture/response" \
  --output-dir "$out_dir/readiness" \
  --sample-cap 2 \
  --per-day-cap 2 >> "$log" 2>&1

python3 - "$out_dir/readiness/manifest.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
manifest = json.loads(manifest_path.read_text())
checks = {
    "decision_keep": manifest["decision"] == "KEEP",
    "selected_two_targets": manifest["selection"]["selected_seed_count"] == 2,
    "non_target_excluded": manifest["selection"]["target_row_count"] == 2,
    "all_validated": manifest["response_summary"]["all_validated"] is True,
    "all_refs_present": manifest["response_summary"]["all_refs_present"] is True,
    "private_false": manifest["promotion_gate"]["private_truth_ready"] is False,
    "no_replay_query_by_script": manifest["side_effects"]["queried_replay_store_v2"] is False,
}
if not all(checks.values()):
    raise AssertionError([key for key, value in checks.items() if not value])
smoke = {
    "artifact": "xuan_b27_dplus_micro_deficit_source_truth_readiness_smoke",
    "status": "PASS",
    "decision_label": "KEEP_MICRO_DEFICIT_SOURCE_TRUTH_READINESS_SMOKE_PASS",
    "checks": checks,
    "outputs": {"manifest": str(manifest_path)},
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False
    },
    "side_effects": manifest["side_effects"],
}
(manifest_path.parent.parent / "manifest.json").write_text(json.dumps(smoke, indent=2, sort_keys=True) + "\n")
print(manifest_path.parent.parent / "manifest.json")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
printf 'PASS D+ micro-deficit source-truth readiness smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
