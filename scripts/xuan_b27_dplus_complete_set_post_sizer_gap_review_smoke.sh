#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-xuan_research_artifacts/xuan_b27_dplus_complete_set_post_sizer_gap_review_smoke_manual}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}/fixtures"

cat > "${OUT_DIR}/fixtures/adapter_score.json" <<'JSON'
{
  "decision": "KEEP",
  "decision_label": "KEEP_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_ADAPTER_READY"
}
JSON

cat > "${OUT_DIR}/fixtures/sizer_score.json" <<'JSON'
{
  "decision": "DISCARD",
  "decision_label": "DISCARD_COMPLETE_SET_DUST_OPP_SIZER_RESIDUAL_NOT_IMPROVED_ENOUGH",
  "research_ranking": {
    "metrics": {
      "seed_action_retention": 1.0,
      "pair_qty_retention": 0.998,
      "residual_qty_rate_reduction": 0.008,
      "residual_cost_rate_reduction": 0.011,
      "fee_after_pnl_delta": -7.8,
      "stress100_worst_pnl_delta": -1.0
    }
  }
}
JSON

cat > "${OUT_DIR}/fixtures/selected_seed_complete_set_context.csv" <<'CSV'
source_seed_candidate_row_id,day,condition_id,slug,ts_ms,side,offset_s,seed_qty,seed_px,pre_seed_same_qty,pre_seed_opp_qty,pre_seed_same_cost,pre_seed_opp_cost,ledger_proxy_before,ledger_proxy_after,source_risk_direction,source_pair_qty,source_pair_cost,source_pair_pnl,source_residual_qty,source_residual_cost,source_residual_age_s
a,2026-05-02,c1,s,1,YES,95,1.2,0.4,0.0,0.2,0.0,0.08,0.0,-1.0,repair_or_pairing_improving,0.2,0.16,0.04,1.0,0.4,120
b,2026-05-02,c2,s,2,NO,40,100,0.4,0.0,2.0,0.0,0.8,0.0,-1.0,repair_or_pairing_improving,99.0,80.0,19.0,1.0,0.4,10
CSV

set +e
python3 "${REPO_DIR}/scripts/xuan_b27_dplus_complete_set_post_sizer_gap_review.py" \
  --adapter-score "${OUT_DIR}/fixtures/adapter_score.json" \
  --selected-seed-context "${OUT_DIR}/fixtures/selected_seed_complete_set_context.csv" \
  --sizer-score "${OUT_DIR}/fixtures/sizer_score.json" \
  --output-dir "${OUT_DIR}/review"
status=$?
set -e
test "${status}" -ne 0
python3 -m json.tool "${OUT_DIR}/review/manifest.json" >/dev/null
python3 - "${OUT_DIR}/review/manifest.json" <<'PY'
import json
import sys
d = json.load(open(sys.argv[1]))
assert d["decision_label"] == "UNKNOWN_COMPLETE_SET_POST_SIZER_NO_SAFE_LOCAL_MECHANISM"
assert "sub_dust_qty_caps_would_violate_sample_preservation_or_order_minimum_proxy" in d["why_no_new_target_selected"]
assert d["promotion_gate"]["passed"] is False
assert d["side_effects"]["ssh_started"] is False
PY

cat > "${OUT_DIR}/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_complete_set_post_sizer_gap_review_smoke",
  "decision": "KEEP",
  "decision_label": "KEEP_COMPLETE_SET_POST_SIZER_GAP_REVIEW_SMOKE_PASS",
  "review_manifest": "${OUT_DIR}/review/manifest.json"
}
JSON
python3 -m json.tool "${OUT_DIR}/manifest.json" >/dev/null
echo "${OUT_DIR}/manifest.json"
