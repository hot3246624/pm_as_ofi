#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-xuan_research_artifacts/xuan_b27_dplus_complete_set_pair_cost_next_target_selector_smoke_manual}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}/fixtures/positive" "${OUT_DIR}/fixtures/weak"

cat > "${OUT_DIR}/fixtures/positive/adapter_score.json" <<'JSON'
{
  "decision": "KEEP",
  "decision_label": "KEEP_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_ADAPTER_READY",
  "scope": "smoke"
}
JSON

cat > "${OUT_DIR}/fixtures/positive/selected_seed_complete_set_context.csv" <<'CSV'
source_seed_candidate_row_id,day,condition_id,slug,ts_ms,side,offset_s,seed_qty,pre_seed_same_qty,pre_seed_opp_qty,pre_seed_same_cost,pre_seed_opp_cost,ledger_proxy_before,ledger_proxy_after,source_risk_direction,source_pair_qty,source_pair_cost,source_pair_pnl,source_residual_qty,source_residual_cost,source_residual_age_s
a,2026-05-02,c1,s,1,YES,95,3,0.0,0.5,0.0,0.2,0.0,-1.0,repair_or_pairing_improving,1.0,0.8,0.2,2.0,1.0,120
b,2026-05-02,c2,s,2,NO,96,3,0.0,0.4,0.0,0.2,0.0,-1.0,repair_or_pairing_improving,1.0,0.8,0.2,2.0,1.0,120
c,2026-05-02,c3,s,3,YES,50,100,0.0,2.0,0.0,0.2,0.0,-1.0,repair_or_pairing_improving,100.0,80.0,20.0,0.2,0.05,10
d,2026-05-02,c4,s,4,NO,40,100,0.0,2.0,0.0,0.2,0.0,-1.0,repair_or_pairing_improving,100.0,80.0,20.0,0.2,0.05,10
CSV

python3 "${REPO_DIR}/scripts/xuan_b27_dplus_complete_set_pair_cost_next_target_selector.py" \
  --adapter-score "${OUT_DIR}/fixtures/positive/adapter_score.json" \
  --selected-seed-context "${OUT_DIR}/fixtures/positive/selected_seed_complete_set_context.csv" \
  --output-dir "${OUT_DIR}/positive"
python3 -m json.tool "${OUT_DIR}/positive/manifest.json" >/dev/null
python3 - "${OUT_DIR}/positive/manifest.json" <<'PY'
import json, sys
d=json.load(open(sys.argv[1]))
assert d["decision_label"] == "KEEP_COMPLETE_SET_DUST_OPP_LATE_REPAIR_SIZER_TARGET_SELECTED"
assert d["selected_next_target"] == "complete_set_dust_opp_late_repair_sizer_v1"
assert d["promotion_gate"]["passed"] is False
PY

cat > "${OUT_DIR}/fixtures/weak/adapter_score.json" <<'JSON'
{
  "decision": "KEEP",
  "decision_label": "KEEP_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_ADAPTER_READY",
  "scope": "smoke"
}
JSON

cat > "${OUT_DIR}/fixtures/weak/selected_seed_complete_set_context.csv" <<'CSV'
source_seed_candidate_row_id,day,condition_id,slug,ts_ms,side,offset_s,seed_qty,pre_seed_same_qty,pre_seed_opp_qty,pre_seed_same_cost,pre_seed_opp_cost,ledger_proxy_before,ledger_proxy_after,source_risk_direction,source_pair_qty,source_pair_cost,source_pair_pnl,source_residual_qty,source_residual_cost,source_residual_age_s
a,2026-05-02,c1,s,1,YES,95,100,0.0,0.5,0.0,0.2,0.0,-1.0,repair_or_pairing_improving,95.0,80.0,15.0,1.0,0.1,120
b,2026-05-02,c2,s,2,NO,40,100,0.0,2.0,0.0,0.2,0.0,-1.0,repair_or_pairing_improving,95.0,80.0,15.0,1.0,0.1,120
CSV

set +e
python3 "${REPO_DIR}/scripts/xuan_b27_dplus_complete_set_pair_cost_next_target_selector.py" \
  --adapter-score "${OUT_DIR}/fixtures/weak/adapter_score.json" \
  --selected-seed-context "${OUT_DIR}/fixtures/weak/selected_seed_complete_set_context.csv" \
  --output-dir "${OUT_DIR}/weak"
weak_status=$?
set -e
test "${weak_status}" -ne 0
python3 -m json.tool "${OUT_DIR}/weak/manifest.json" >/dev/null
python3 - "${OUT_DIR}/weak/manifest.json" <<'PY'
import json, sys
d=json.load(open(sys.argv[1]))
assert d["decision"] == "UNKNOWN"
assert "target_residual_cost_not_concentrated_enough" in d["blockers"] or "target_blast_radius_too_large_for_first_local_sizer" in d["blockers"]
PY

cat > "${OUT_DIR}/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_complete_set_pair_cost_next_target_selector_smoke",
  "decision": "KEEP",
  "decision_label": "KEEP_COMPLETE_SET_PAIR_COST_NEXT_TARGET_SELECTOR_SMOKE_PASS",
  "positive_manifest": "${OUT_DIR}/positive/manifest.json",
  "weak_manifest": "${OUT_DIR}/weak/manifest.json"
}
JSON
python3 -m json.tool "${OUT_DIR}/manifest.json" >/dev/null
echo "${OUT_DIR}/manifest.json"
