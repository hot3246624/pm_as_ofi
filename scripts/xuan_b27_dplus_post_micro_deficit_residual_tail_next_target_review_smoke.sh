#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="xuan_research_artifacts/xuan_b27_dplus_post_micro_deficit_residual_tail_next_target_review_smoke_${RUN_ID}"
FIXTURE_DIR="${OUT_DIR}/fixture"
mkdir -p "${FIXTURE_DIR}/summaries"

python3 - <<'PY' "${FIXTURE_DIR}"
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])

completion = {
    "decision": "UNKNOWN",
    "decision_label": "UNKNOWN_MICRO_DEFICIT_NO_ORDER_REVIEW_NO_LIVE_SIGNATURE_RISK_BUDGET_FAIL",
    "aggregate_metrics": {
        "candidates": 97,
        "queue_supported_fills": 68,
        "pair_qty": 41.015,
        "pair_pnl": 2.420802,
        "fee_adjusted_pair_pnl_proxy": 1.756204,
        "net_pair_cost_proxy_p90": 1.10,
        "residual_qty_share_of_filled": 0.31,
        "residual_cost_share_of_filled_cost": 0.247,
        "material_residual_lots": 0,
        "micro_deficit_repair_guard_candidates": 0,
        "micro_deficit_repair_guard_blocks": 0,
    },
    "scorer_results": {
        "shadow_acceptance": {
            "promotion_gate": {
                "normalized_risk_budget": {
                    "pair_tail_loss_share_of_pair_pnl": 0.23,
                }
            }
        }
    },
}
residual_score = {
    "decision": "UNKNOWN",
    "decision_label": "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF",
    "exemplar_metrics": {
        "required_field_coverage": 1.0,
        "source_sequence_coverage": 1.0,
    },
}
micro_score = {
    "decision": "UNKNOWN",
    "decision_label": "UNKNOWN_MICRO_DEFICIT_NO_ORDER_SUMMARY_INPUTS_INSUFFICIENT",
    "metrics": {
        "micro_deficit_exemplar_count": 0,
    },
}
summary = {
    "event_lite": {
        "source_link_residual_tail_exemplars": [
            {
                "condition_id": "c1",
                "slug": "btc-updown-5m-fixture",
                "side": "YES",
                "offset_s": 70,
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_same_qty": 2.5,
                "pre_seed_opp_qty": 3.75,
                "pre_seed_open_qty": 6.25,
                "trigger_px": 0.62,
                "trigger_size": 5,
                "source_pair_qty": 0,
                "source_pair_pnl": 0,
                "source_residual_qty": 1.25,
                "source_residual_cost": 0.6875,
                "source_sequence_id": "seq1",
                "quote_intent_id": "quote1",
                "source_order_id": "order1",
            },
            {
                "condition_id": "c2",
                "slug": "btc-updown-5m-fixture",
                "side": "YES",
                "offset_s": 82,
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_same_qty": 3.75,
                "pre_seed_opp_qty": 5,
                "pre_seed_open_qty": 8.75,
                "trigger_px": 0.65,
                "trigger_size": 12,
                "source_pair_qty": 0,
                "source_pair_pnl": 0,
                "source_residual_qty": 1.25,
                "source_residual_cost": 0.725,
                "source_sequence_id": "seq2",
                "quote_intent_id": "quote2",
                "source_order_id": "order2",
            },
        ]
    }
}

files = {
    "completion.json": completion,
    "residual_score.json": residual_score,
    "micro_score.json": micro_score,
    "aggregate_report.json": completion["aggregate_metrics"],
    "summaries/fixture.summary.json": summary,
}
for rel, value in files.items():
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
PY

set +e
python3 scripts/xuan_b27_dplus_post_micro_deficit_residual_tail_next_target_review.py \
  --completion-manifest "${FIXTURE_DIR}/completion.json" \
  --residual-tail-score "${FIXTURE_DIR}/residual_score.json" \
  --micro-deficit-score "${FIXTURE_DIR}/micro_score.json" \
  --aggregate-report "${FIXTURE_DIR}/aggregate_report.json" \
  --summaries "${FIXTURE_DIR}/summaries" \
  --output "${OUT_DIR}/manifest.json" \
  > "${OUT_DIR}/stdout.log" 2> "${OUT_DIR}/stderr.log"
status=$?
set -e

if [[ "${status}" -eq 0 ]]; then
  echo "expected UNKNOWN nonzero exit, got ${status}" >&2
  exit 1
fi

python3 - <<'PY' "${OUT_DIR}/manifest.json"
import json
import sys

with open(sys.argv[1]) as fh:
    manifest = json.load(fh)
assert manifest["decision"] == "UNKNOWN"
assert manifest["decision_label"] == "UNKNOWN_POST_MICRO_DEFICIT_RESIDUAL_TAIL_NO_SAFE_PRE_ACTION_SEPARATOR"
assert manifest["selected_next_target"]["name"] is None
assert manifest["promotion_gate"]["passed"] is False
assert manifest["side_effects"]["shadow_started"] is False
assert manifest["side_effects"]["events_jsonl_read"] is False
assert "backtest_causality_audit_v1" in manifest["next_executable_action"]
assert manifest["source_evidence"]["exemplar_count"] == 2
PY

python3 -m json.tool "${OUT_DIR}/manifest.json" >/dev/null
echo "{\"decision\":\"KEEP\",\"artifact\":\"${OUT_DIR}/manifest.json\"}"
