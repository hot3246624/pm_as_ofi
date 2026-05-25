#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-"$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_spec_smoke_20260525T182006Z"}"
SCRIPT="$ROOT/scripts/xuan_observable_pre_action_rule_miner_spec.py"

rm -rf "$OUT"
mkdir -p "$OUT/fixtures"

touch "$OUT/fixtures/candidate_rows.jsonl"
touch "$OUT/fixtures/source_link_summary.json"
touch "$OUT/fixtures/no_order_denominator_summary.json"

python3 - "$OUT/good_input.json" "$OUT/fixtures" <<'PY'
import json, sys
manifest_path, fixture_dir = sys.argv[1], sys.argv[2]
pre_action = [
  "condition_id", "day_id", "quote_ts_ms", "side", "offset_s", "status_before_action",
  "block_reason", "source_sequence_present", "quote_intent_present", "source_order_present",
  "pre_seed_same_qty", "pre_seed_opp_qty", "pre_seed_same_cost", "pre_seed_opp_cost",
  "open_qty_bucket", "deficit_bucket", "candidate_qty_bucket", "source_risk_direction"
]
labels = [
  "source_pair_qty", "source_pair_cost", "source_pair_pnl", "source_residual_qty",
  "source_residual_cost", "pair_outcome_bucket", "residual_tail_outcome_bucket"
]
no_order = [
  "frozen_rule_id", "same_window_denominator_count", "admitted_count", "blocked_count",
  "source_sequence_coverage", "quote_intent_presence_rate", "source_order_presence_rate",
  "aggregate_parity"
]
manifest = {
  "artifact": "observable_pre_action_rule_miner_input_fixture",
  "schema_version": "observable_pre_action_rule_miner_input_v1",
  "created_utc": "20260525T182006Z",
  "fixture": True,
  "strategy_evidence": False,
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
  },
  "materialized_sources": {
    "candidate_rows": {
      "path": f"{fixture_dir}/candidate_rows.jsonl",
      "row_count": 41580,
      "fields": pre_action + labels
    },
    "source_link_summary": {
      "path": f"{fixture_dir}/source_link_summary.json",
      "row_count": 26,
      "fields": ["source_sequence_presence_by_status", "quote_intent_presence_by_status", "source_order_presence_by_status"]
    },
    "no_order_denominator_summary": {
      "path": f"{fixture_dir}/no_order_denominator_summary.json",
      "row_count": 1,
      "fields": no_order
    }
  },
  "train_holdout_split": {
    "train_days": ["2026-05-02", "2026-05-04", "2026-05-06"],
    "holdout_days": ["2026-05-03", "2026-05-05"]
  },
  "feature_policy": {
    "allowed_live_rule_fields": pre_action,
    "offline_label_fields": labels
  },
  "frozen_rule_contract": {
    "frozen_rule_id": "fixture_pre_action_source_rule_v1",
    "predicate_fields": ["source_sequence_present", "quote_intent_present", "pre_seed_opp_qty", "offset_s"],
    "uses_only_pre_action_fields": True,
    "uses_realized_pair_cost": False,
    "uses_future_labels": False,
    "train_holdout_gate_passed": True,
    "train_selected_rows": 240,
    "holdout_selected_rows": 120
  },
  "no_order_denominator_gate": {
    "same_window_denominator_reproduced": True,
    "same_window_denominator_count": 64,
    "source_sequence_coverage": 1.0,
    "aggregate_parity": True
  },
  "promotion_gate": {
    "passed": False,
    "private_truth_ready": False,
    "deployable": False,
    "g2_canary_ready": False
  }
}
json.dump(manifest, open(manifest_path, "w"), indent=2, sort_keys=True)
PY

python3 "$SCRIPT" --output-dir "$OUT/default" >/dev/null
python3 "$SCRIPT" --candidate-input-manifest "$OUT/good_input.json" --output-dir "$OUT/good" >/dev/null

python3 - "$OUT/bad_missing_source.json" "$OUT/good_input.json" <<'PY'
import json, sys
bad_path, good_path = sys.argv[1], sys.argv[2]
manifest = json.load(open(good_path))
fields = manifest["materialized_sources"]["candidate_rows"]["fields"]
manifest["materialized_sources"]["candidate_rows"]["fields"] = [field for field in fields if field != "source_sequence_present"]
json.dump(manifest, open(bad_path, "w"), indent=2, sort_keys=True)
PY

python3 - "$OUT/bad_missing_denominator.json" "$OUT/good_input.json" <<'PY'
import json, sys
bad_path, good_path = sys.argv[1], sys.argv[2]
manifest = json.load(open(good_path))
manifest["no_order_denominator_gate"]["same_window_denominator_reproduced"] = False
manifest["no_order_denominator_gate"]["same_window_denominator_count"] = 0
json.dump(manifest, open(bad_path, "w"), indent=2, sort_keys=True)
PY

python3 "$SCRIPT" --candidate-input-manifest "$OUT/bad_missing_source.json" --output-dir "$OUT/bad_source" >/dev/null
python3 "$SCRIPT" --candidate-input-manifest "$OUT/bad_missing_denominator.json" --output-dir "$OUT/bad_denominator" >/dev/null

python3 - "$OUT/default/manifest.json" "$OUT/good/manifest.json" "$OUT/bad_source/manifest.json" "$OUT/bad_denominator/manifest.json" <<'PY'
import json, sys
default, good, bad_source, bad_den = [json.load(open(path)) for path in sys.argv[1:]]
assert default["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_REAL_INPUT_UNKNOWN"
assert default["real_input_status"] == "UNKNOWN_REAL_INPUT_MANIFEST_MISSING"
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_FIXTURE_PASS_NOT_REAL"
assert good["candidate_input_status"]["ready"] is True
assert good["candidate_input_status"]["fixture"] is True
assert bad_source["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_FAIL_CLOSED"
assert "candidate_rows_required_pre_action_fields_missing" in bad_source["blockers"]
assert bad_den["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_FAIL_CLOSED"
assert "same_window_no_order_denominator_not_reproduced" in bad_den["blockers"]
assert "same_window_no_order_denominator_below_min" in bad_den["blockers"]
for manifest in (default, good, bad_source, bad_den):
    assert manifest["promotion_gate"]["passed"] is False
    assert manifest["scope"]["shadow_started"] is False
    assert manifest["scope"]["events_jsonl_read"] is False
PY

python3 - "$OUT/manifest.json" <<'PY'
import json, sys
manifest = {
  "artifact": "xuan_observable_pre_action_rule_miner_spec_smoke",
  "decision": "KEEP",
  "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_SMOKE_PASS",
  "checks": {
    "default_real_input_unknown": True,
    "good_fixture_passes_not_real": True,
    "missing_source_field_fails_closed": True,
    "missing_no_order_denominator_fails_closed": True,
    "promotion_gate_false": True,
    "no_shadow_or_events_jsonl": True
  },
  "promotion_gate": {
    "passed": False,
    "private_truth_ready": False,
    "deployable": False,
    "g2_canary_ready": False
  },
  "side_effects": {
    "ssh_used": False,
    "shadow_started": False,
    "events_jsonl_read": False,
    "raw_replay_or_full_store_scanned": False,
    "shared_ingress_or_broker_or_live_modified": False,
    "orders_cancels_redeems_sent": False
  }
}
json.dump(manifest, open(sys.argv[1], "w"), indent=2, sort_keys=True)
PY

python3 -m json.tool "$OUT/manifest.json" >/dev/null
echo "$OUT/manifest.json"
