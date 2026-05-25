#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-"$ROOT/xuan_research_artifacts/xuan_local_strategy_pivot_selector_smoke_20260525T175006Z"}"
SCRIPT="$ROOT/scripts/xuan_local_strategy_pivot_selector.py"

rm -rf "$OUT"
mkdir -p "$OUT"

python3 "$SCRIPT" --output-dir "$OUT/good" >/dev/null
python3 - "$OUT/good/manifest.json" <<'PY'
import json, sys
manifest = json.load(open(sys.argv[1]))
assert manifest["decision_label"] == "KEEP_LOCAL_STRATEGY_PIVOT_OBSERVABLE_PRE_ACTION_RULE_MINER_SELECTED"
assert manifest["selected_next_lane"]["target"] == "observable_pre_action_rule_miner_spec_v1"
assert manifest["promotion_gate"]["passed"] is False
assert manifest["scope"]["shadow_started"] is False
assert manifest["research_ranking"]["no_order_diagnostic_allowed"] is False
PY

python3 - "$OUT/bad_source_gap.json" <<'PY'
import json, sys
json.dump({
  "artifact": "bad_source_gap",
  "decision_label": "KEEP_SOURCE_OPPORTUNITY_GAP_AUDIT_READY_MARKER_REPRODUCTION_GAP_NAMED",
  "exact_gap_classification": ["old_marker_threshold_sweep_possible"],
  "no_order_marker_availability": {
    "reason_source_coverage_available": True,
    "source_opportunity_marker_total": 0.0,
    "source_link_presence_by_status": {"source_sequence": {"admitted|present": 1.0}}
  },
  "promotion_gate": {"passed": False}
}, open(sys.argv[1], "w"), indent=2, sort_keys=True)
PY

python3 "$SCRIPT" \
  --source-opportunity-gap-manifest "$OUT/bad_source_gap.json" \
  --output-dir "$OUT/bad" >/dev/null
python3 - "$OUT/bad/manifest.json" <<'PY'
import json, sys
manifest = json.load(open(sys.argv[1]))
assert manifest["decision_label"] == "UNKNOWN_LOCAL_STRATEGY_PIVOT_NO_LEGAL_NEXT_LANE"
assert "gap_does_not_require_new_pre_action_signal_family" in manifest["blockers"]
assert manifest["selected_next_lane"] is None
assert manifest["promotion_gate"]["passed"] is False
PY

python3 - "$OUT/manifest.json" <<'PY'
import json, sys
manifest = {
  "artifact": "xuan_local_strategy_pivot_selector_smoke",
  "decision": "KEEP",
  "decision_label": "KEEP_LOCAL_STRATEGY_PIVOT_SELECTOR_SMOKE_PASS",
  "checks": {
    "good_selects_observable_pre_action_rule_miner": True,
    "good_keeps_no_order_disallowed": True,
    "bad_missing_new_signal_gap_fails_closed": True,
    "promotion_gate_false": True
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
