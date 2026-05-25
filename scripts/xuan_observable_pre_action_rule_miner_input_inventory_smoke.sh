#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-xuan_research_artifacts/xuan_observable_pre_action_rule_miner_input_inventory_smoke_20260525T185536Z}"
FIXTURE_DIR="${OUT_DIR}/fixtures"
CREATED_UTC="2026-05-25T18:55:36Z"

rm -rf "${OUT_DIR}"
mkdir -p "${FIXTURE_DIR}"

GOOD_CANDIDATES="${FIXTURE_DIR}/good_candidate_rows.csv"
BAD_CANDIDATES="${FIXTURE_DIR}/bad_candidate_rows_missing_source.csv"
GOOD_SOURCE="${FIXTURE_DIR}/good_source_link_summary.json"
GOOD_DENOM="${FIXTURE_DIR}/good_no_order_denominator_summary.json"
BAD_DENOM="${FIXTURE_DIR}/bad_no_order_denominator_summary.json"

cat >"${GOOD_CANDIDATES}" <<'CSV'
condition_id,day_id,quote_ts_ms,side,offset_s,status_before_action,block_reason,source_sequence_present,quote_intent_present,source_order_present,pre_seed_same_qty,pre_seed_opp_qty,pre_seed_same_cost,pre_seed_opp_cost,open_qty_bucket,deficit_bucket,candidate_qty_bucket,source_risk_direction,source_pair_qty,source_pair_cost,source_pair_pnl,source_residual_qty,source_residual_cost,pair_outcome_bucket,residual_tail_outcome_bucket
0xaaa,2026-05-01,1777600000000,YES,88,admitted,candidate,true,true,true,1,2,0.4,0.8,open_le_1,deficit_0_0_25,candidate_qty_1_2,repair_or_pairing_improving,1,0.90,0.10,0,0,paired_nonzero,residual_zero
0xbbb,2026-05-02,1777686400000,NO,94,blocked,offset,true,true,true,2,1,0.9,0.3,open_1_2,deficit_0_25_1_25,candidate_qty_1_2,repair_or_pairing_improving,1,0.92,0.08,0.1,0.04,paired_nonzero,residual_small
CSV

cat >"${BAD_CANDIDATES}" <<'CSV'
condition_id,day_id,quote_ts_ms,side,offset_s,status_before_action,block_reason,quote_intent_present,source_order_present,pre_seed_same_qty,pre_seed_opp_qty,pre_seed_same_cost,pre_seed_opp_cost,open_qty_bucket,deficit_bucket,candidate_qty_bucket,source_risk_direction,source_pair_qty,source_pair_cost,source_pair_pnl,source_residual_qty,source_residual_cost,pair_outcome_bucket,residual_tail_outcome_bucket
0xaaa,2026-05-01,1777600000000,YES,88,admitted,candidate,true,true,1,2,0.4,0.8,open_le_1,deficit_0_0_25,candidate_qty_1_2,repair_or_pairing_improving,1,0.90,0.10,0,0,paired_nonzero,residual_zero
CSV

cat >"${GOOD_SOURCE}" <<'JSON'
{
  "decision_label": "KEEP_FIXTURE_SOURCE_LINK_SUMMARY",
  "no_order_marker_availability": {
    "aggregate_candidates": 30,
    "reason_source_coverage_available": true,
    "source_link_presence_by_status": {
      "source_sequence": {"admitted|present": 15, "blocked|present": 15},
      "quote_intent": {"admitted|present": 15, "blocked|present": 15},
      "source_order": {"admitted|present": 15, "blocked|present": 15}
    },
    "top_block_reasons": {"admitted|candidate": 15, "blocked|offset": 15}
  }
}
JSON

cat >"${GOOD_DENOM}" <<'JSON'
{
  "decision_label": "KEEP_FIXTURE_NO_ORDER_DENOMINATOR",
  "no_order_denominator_summary": {
    "available": true,
    "gate_passed": true,
    "marker_reproduced": true,
    "frozen_rule_id": "fixture_rule_001",
    "same_window_denominator_count": 30,
    "admitted_count": 15,
    "blocked_count": 15,
    "source_sequence_coverage": 1.0,
    "quote_intent_presence_rate": 1.0,
    "source_order_presence_rate": 1.0,
    "aggregate_parity": true,
    "source_opportunity_marker_total": 30,
    "candidates": 30
  },
  "train_holdout_split": {
    "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
    "holdout_days": ["2026-05-04", "2026-05-05"]
  },
  "frozen_rule_contract": {
    "predicate_fields": ["source_sequence_present", "source_risk_direction", "open_qty_bucket"],
    "uses_only_pre_action_fields": true,
    "uses_realized_pair_cost": false,
    "uses_future_labels": false,
    "train_holdout_gate_passed": true,
    "train_selected_rows": 120,
    "holdout_selected_rows": 60
  },
  "no_order_denominator_gate": {
    "same_window_denominator_reproduced": true,
    "same_window_denominator_count": 30,
    "source_sequence_coverage": 1.0,
    "aggregate_parity": true
  }
}
JSON

cat >"${BAD_DENOM}" <<'JSON'
{
  "decision_label": "UNKNOWN_FIXTURE_DENOMINATOR_MISSING_GATE",
  "no_order_denominator_summary": {
    "available": true,
    "gate_passed": false,
    "frozen_rule_id": "fixture_rule_bad",
    "same_window_denominator_count": 0,
    "admitted_count": 0,
    "blocked_count": 0,
    "source_sequence_coverage": 0.0,
    "quote_intent_presence_rate": 0.0,
    "source_order_presence_rate": 0.0,
    "aggregate_parity": false,
    "source_opportunity_marker_total": 0,
    "candidates": 0
  }
}
JSON

python3 scripts/xuan_observable_pre_action_rule_miner_input_inventory.py \
  --candidate-rows "${GOOD_CANDIDATES}" \
  --source-link-summary "${GOOD_SOURCE}" \
  --no-order-denominator-summary "${GOOD_DENOM}" \
  --fixture \
  --created-utc "${CREATED_UTC}" \
  --output-dir "${OUT_DIR}/good"

python3 scripts/xuan_observable_pre_action_rule_miner_spec.py \
  --candidate-input-manifest "${OUT_DIR}/good/observable_pre_action_rule_miner_input_manifest.json" \
  --min-candidate-rows 1 \
  --min-no-order-denominator 30 \
  --created-utc "${CREATED_UTC}" \
  --output-dir "${OUT_DIR}/good_validation"

python3 scripts/xuan_observable_pre_action_rule_miner_input_inventory.py \
  --candidate-rows "${BAD_CANDIDATES}" \
  --source-link-summary "${GOOD_SOURCE}" \
  --no-order-denominator-summary "${GOOD_DENOM}" \
  --fixture \
  --created-utc "${CREATED_UTC}" \
  --output-dir "${OUT_DIR}/bad_missing_source"

python3 scripts/xuan_observable_pre_action_rule_miner_input_inventory.py \
  --candidate-rows "${GOOD_CANDIDATES}" \
  --source-link-summary "${GOOD_SOURCE}" \
  --no-order-denominator-summary "${BAD_DENOM}" \
  --fixture \
  --created-utc "${CREATED_UTC}" \
  --output-dir "${OUT_DIR}/bad_missing_denominator"

python3 - <<'PY' "${OUT_DIR}" "${CREATED_UTC}"
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
created = sys.argv[2]

def load(path: Path):
    with path.open() as fh:
        return json.load(fh)

good = load(out / "good" / "manifest.json")
good_validation = load(out / "good_validation" / "manifest.json")
bad_source = load(out / "bad_missing_source" / "manifest.json")
bad_denominator = load(out / "bad_missing_denominator" / "manifest.json")

assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_READY", good["decision_label"]
assert good["candidate_input_manifest_readiness"]["ready"] is True
assert good_validation["candidate_input_status"]["ready"] is True, good_validation["candidate_input_status"]
assert bad_source["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_GAPS"
assert any("candidate_rows" in b for b in bad_source["blockers"]), bad_source["blockers"]
assert bad_denominator["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_GAPS"
assert any("no_order_denominator_summary" in b for b in bad_denominator["blockers"]), bad_denominator["blockers"]

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_input_inventory_smoke",
    "schema_version": 1,
    "created_utc": created,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_SMOKE_PASS",
    "good_fixture_decision": good["decision_label"],
    "good_validation_decision": good_validation["decision_label"],
    "bad_missing_source_decision": bad_source["decision_label"],
    "bad_missing_denominator_decision": bad_denominator["decision_label"],
    "strategy_evidence": False,
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False
    },
    "scope": {
        "current_worktree_only": True,
        "local_only": True,
        "new_data_fetched": False,
        "external_worktree_read": False,
        "ssh_used": False,
        "shadow_started": False,
        "canary_or_live_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_full_store_scanned": False,
        "shared_ingress_or_broker_or_live_modified": False,
        "shared_ws_or_local_agg_or_service_started": False,
        "orders_cancels_redeems_sent": False,
        "trading_behavior_changed": False
    }
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps({"decision_label": manifest["decision_label"], "output": str(out)}, sort_keys=True))
PY
