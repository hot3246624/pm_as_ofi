#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR="${XUAN_BLOCKER_CONTEXT_SPEC_SMOKE_DIR:-/tmp/xuan_b27_dplus_ledger_before_blocker_context_spec_smoke_20260524T014317Z}"
FIXTURE="$ARTIFACT_DIR/fixture"
mkdir -p "$FIXTURE"

cat >"$FIXTURE/completion.json" <<'JSON'
{
  "decision_label": "UNKNOWN_LEDGER_DELTA_TINY_DEFICIT_MARKER_NO_ORDER_REVIEW_THIN_MARKER_RISK_BUDGET_FAIL",
  "promotion_gate": {"passed": false, "private_truth_ready": false, "deployable": false}
}
JSON

cat >"$FIXTURE/gap.json" <<'JSON'
{
  "decision": "UNKNOWN",
  "decision_label": "UNKNOWN_LEDGER_BEFORE_DELTA_GAP_AUDIT_NO_SAFE_FAMILY",
  "blockers": ["no_train_holdout_stable_candidate_with_nonzero_before_or_delta_marker"]
}
JSON

cat >"$FIXTURE/score.json" <<'JSON'
{
  "decision_label": "KEEP_SOURCE_OPPORTUNITY_LEDGER_BEFORE_DELTA_MARKER_DENOMINATOR_SCORER_READY",
  "ledger_before_marker_denominator": {
    "marker_total": 77,
    "admitted_marker_count": 1,
    "blocked_marker_count": 76
  },
  "ledger_before_reason_source_coverage": {
    "matching_marker_rows": [
      {
        "status_reason_marker_key": "blocked|target|NO|offset_90_120|repair_or_pairing_improving|open_qty_gt_5|deficit_0_0_25|before_lt_m2",
        "transition_count": 43,
        "source_sequence_presence_rate": 1.0
      },
      {
        "status_reason_marker_key": "blocked|target|YES|offset_90_120|repair_or_pairing_improving|open_qty_gt_5|deficit_0_0_25|before_lt_m2",
        "transition_count": 30,
        "source_sequence_presence_rate": 1.0
      },
      {
        "status_reason_marker_key": "blocked|cooldown|NO|offset_90_120|repair_or_pairing_improving|open_qty_gt_5|deficit_0_0_25|before_lt_m2",
        "transition_count": 2,
        "source_sequence_presence_rate": 1.0
      },
      {
        "status_reason_marker_key": "admitted|candidate|YES|offset_90_120|repair_or_pairing_improving|open_qty_2_5|deficit_0_0_25|before_m1_m025",
        "transition_count": 1,
        "source_sequence_presence_rate": 1.0
      },
      {
        "status_reason_marker_key": "blocked|activation_opp_seen|YES|offset_90_120|repair_or_pairing_improving|open_qty_2_5|deficit_0_0_25|before_m1_m025",
        "transition_count": 1,
        "source_sequence_presence_rate": 1.0
      }
    ]
  },
  "ledger_delta_marker_denominator": {
    "marker_total": 1,
    "admitted_marker_count": 1,
    "blocked_marker_count": 0,
    "matched_marker_rows": [
      {
        "marker_key": "YES|offset_90_120|repair_or_pairing_improving|open_qty_2_5|deficit_0_0_25|delta_m1_m025",
        "status": "admitted",
        "transition_count": 1
      }
    ]
  }
}
JSON

cat >"$FIXTURE/runner.py" <<'PY'
target_room_bucket_by_status_reason_side_offset_risk_open_deficit = {}
transition_count_by_status_reason_side_offset_risk_open_deficit_ledger_before = {}
source_sequence_presence_by_status_reason_side_offset_risk_open_deficit_ledger_before = {}
def f():
    block_seed("cooldown")
PY

python3 "$ROOT/scripts/xuan_b27_dplus_ledger_before_blocker_context_spec.py" \
  --completion-manifest "$FIXTURE/completion.json" \
  --gap-manifest "$FIXTURE/gap.json" \
  --ledger-before-delta-score "$FIXTURE/score.json" \
  --runner "$FIXTURE/runner.py" \
  --output "$ARTIFACT_DIR/manifest.json" \
  >"$ARTIFACT_DIR/smoke.log"

python3 - "$ARTIFACT_DIR/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text())
assert manifest["decision"] == "KEEP", manifest["decision_label"]
assert manifest["observed_before_marker_mass"]["marker_total"] == 77
assert manifest["observed_before_marker_mass"]["target_cooldown_count"] == 75
assert manifest["observed_delta_marker"]["marker_total"] == 1
assert manifest["legal_assessment"]["changes_trading_behavior"] is False
assert manifest["promotion_gate"]["passed"] is False
assert "target_room_bucket_by_status_reason_side_offset_risk_open_deficit_ledger_before" in manifest["missing_exact_ledger_before_blocker_fields"]
PY

python3 -m json.tool "$ARTIFACT_DIR/manifest.json" >/dev/null
