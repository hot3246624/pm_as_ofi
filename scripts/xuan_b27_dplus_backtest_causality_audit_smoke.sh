#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$ROOT_DIR/xuan_research_artifacts/xuan_b27_dplus_backtest_causality_audit_smoke_${RUN_ID}"
FIXTURE_DIR="$OUT_DIR/fixture"
OUTPUT="$OUT_DIR/manifest.json"

rm -rf "$OUT_DIR"
mkdir -p "$FIXTURE_DIR"

python3 - "$FIXTURE_DIR" <<'PY'
import json
import sys
from pathlib import Path

fixture = Path(sys.argv[1])

def write_json(name, value):
    path = fixture / name
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return path

write_json(
    "micro_local.json",
    {
        "artifact": "xuan_b27_dplus_micro_deficit_repair_guard_verifier",
        "decision": "KEEP",
        "decision_label": "KEEP_MICRO_DEFICIT_REPAIR_GUARD_LOCAL_RESEARCH_ONLY_FEE_TRADEOFF",
        "research_ranking": {
            "label": "KEEP_MICRO_DEFICIT_REPAIR_GUARD_LOCAL_RESEARCH_ONLY_FEE_TRADEOFF",
            "seed_action_retention": 0.95,
            "pair_qty_retention": 0.96,
            "residual_qty_rate_reduction": 0.58,
            "residual_cost_rate_reduction": 0.72,
            "fee_after_pnl_delta": -109.0,
            "stress100_worst_pnl_delta": 67.0,
        },
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)
write_json(
    "micro_shadow.json",
    {
        "artifact": "xuan_b27_dplus_shadow_review_micro_deficit_completion",
        "decision": "UNKNOWN",
        "decision_label": "UNKNOWN_MICRO_DEFICIT_NO_ORDER_REVIEW_NO_LIVE_SIGNATURE_RISK_BUDGET_FAIL",
        "aggregate_metrics": {
            "candidates": 97,
            "fee_adjusted_pair_pnl_proxy": 1.7,
            "net_pair_cost_proxy_p90": 1.1,
            "residual_qty_share_of_filled": 0.31,
            "residual_cost_share_of_filled_cost": 0.247,
            "pair_tail_loss_share_of_pair_pnl": 0.233,
            "micro_deficit_repair_guard_candidates": 0,
        },
        "scorer_results": {
            "micro_deficit_summary_scorer": {"metrics": {"micro_deficit_exemplar_count": 0}},
            "residual_tail_exemplar_scorer": {
                "exemplar_metrics": {
                    "exemplar_count": 21,
                    "required_field_coverage": 1.0,
                    "source_sequence_coverage": 1.0,
                    "quote_intent_coverage": 1.0,
                    "source_order_coverage": 1.0,
                }
            },
        },
        "promotion_gate_passed": False,
        "private_truth_ready": False,
        "deployable": False,
    },
)
write_json(
    "post_review.json",
    {
        "artifact": "xuan_b27_dplus_post_micro_deficit_residual_tail_next_target_review",
        "decision": "UNKNOWN",
        "decision_label": "UNKNOWN_POST_MICRO_DEFICIT_RESIDUAL_TAIL_NO_SAFE_PRE_ACTION_SEPARATOR",
        "backtest_methodology_risk": {
            "post_action_label_target_selection": True,
            "state_machine_no_order_transfer_gap": True,
        },
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)
write_json(
    "generic.json",
    {
        "artifact": "generic",
        "decision": "UNKNOWN",
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)

(fixture / "state_machine.py").write_text(
    "CANDIDATE_SEED_OUTCOME_SEPARATOR_FIELDS = ['source_pair_qty', 'source_residual_cost']\n"
    "def _residual_tail_sort_key(row):\n"
    "    return -float(row.get('source_residual_cost') or 0.0)\n"
    "post_action_label_policy = 'source_pair_* and source_residual_* must not be used as live pre-action gates'\n"
)
(fixture / "runner.py").write_text(
    "def maybe_seed(row):\n"
    "    micro_deficit_repair_guard_candidate = True\n"
    "    source_pair_qty = 0.0\n"
    "    source_residual_cost = 0.0\n"
)
(fixture / "scoring.md").write_text("research_ranking and promotion_gate must be separated\n")
PY

python3 "$ROOT_DIR/scripts/xuan_b27_dplus_backtest_causality_audit.py" \
  --root "$ROOT_DIR" \
  --input-manifest "$FIXTURE_DIR/micro_local.json" \
  --input-manifest "$FIXTURE_DIR/micro_shadow.json" \
  --input-manifest "$FIXTURE_DIR/post_review.json" \
  --input-manifest "$FIXTURE_DIR/generic.json" \
  --source-file "$FIXTURE_DIR/state_machine.py" \
  --source-file "$FIXTURE_DIR/runner.py" \
  --source-file "$FIXTURE_DIR/scoring.md" \
  --output "$OUTPUT" >/dev/null

python3 - "$OUTPUT" <<'PY'
import json
import sys
from pathlib import Path

data = json.loads(Path(sys.argv[1]).read_text())
assert data["decision"] == "KEEP", data["decision"]
assert data["promotion_gate"]["passed"] is False
checks = {row["id"]: row for row in data["checks"]}
assert checks["live_rule_future_label_guard"]["status"] == "PASS"
assert checks["selector_outcome_label_usage"]["status"] == "FAIL_RESEARCH_DOWNGRADE_REQUIRED"
assert checks["train_holdout_freeze"]["status"] == "MISSING"
assert checks["state_machine_to_no_order_reproduction"]["status"] == "FAIL_REPRODUCTION"
assert data["field_registry"]["post_action_outcome_labels_not_live_criteria"]
assert any(
    item["classification"] == "DOWNGRADE_LOCAL_KEEP_TO_RESEARCH_LEAD_PENDING_REPRODUCTION"
    for item in data["mechanism_classification"]
)
print(json.dumps({"decision": data["decision"], "output": str(Path(sys.argv[1]))}, sort_keys=True))
PY
