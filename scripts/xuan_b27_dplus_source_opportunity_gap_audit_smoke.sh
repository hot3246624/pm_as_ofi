#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$ROOT_DIR/xuan_research_artifacts/xuan_b27_dplus_source_opportunity_gap_audit_smoke_${RUN_ID}"
FIXTURE_DIR="$OUT_DIR/fixture"
SUMMARY_DIR="$FIXTURE_DIR/summaries"
CSV_PATH="$FIXTURE_DIR/candidate_seed_outcome_separator.csv"
TRAIN_MANIFEST="$FIXTURE_DIR/train_holdout.json"
COMPLETION="$FIXTURE_DIR/completion.json"
OUTPUT="$OUT_DIR/manifest.json"

rm -rf "$OUT_DIR"
mkdir -p "$SUMMARY_DIR"

python3 - "$CSV_PATH" "$TRAIN_MANIFEST" "$COMPLETION" "$SUMMARY_DIR" <<'PY'
import csv
import json
import sys
from pathlib import Path

csv_path = Path(sys.argv[1])
train_manifest = Path(sys.argv[2])
completion = Path(sys.argv[3])
summary_dir = Path(sys.argv[4])

fields = [
    "day",
    "side",
    "source_risk_direction",
    "offset_s",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_open_qty",
    "seed_qty",
    "seed_cost",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "official_fee",
    "source_residual_qty",
    "source_residual_cost",
]
rows = []
for day in ["2026-05-02", "2026-05-03"]:
    for _ in range(20):
        rows.append(
            {
                "day": day,
                "side": "NO",
                "source_risk_direction": "repair_or_pairing_improving",
                "offset_s": 95,
                "pre_seed_same_qty": 0,
                "pre_seed_opp_qty": 0.2,
                "pre_seed_open_qty": 0.2,
                "seed_qty": 1,
                "seed_cost": 0.5,
                "source_pair_qty": 0.2,
                "source_pair_cost": 0.2,
                "source_pair_pnl": 0.03,
                "official_fee": 0.01,
                "source_residual_qty": 0.9,
                "source_residual_cost": 0.45,
            }
        )
    for _ in range(80):
        rows.append(
            {
                "day": day,
                "side": "YES",
                "source_risk_direction": "risk_increasing",
                "offset_s": 20,
                "pre_seed_same_qty": 0,
                "pre_seed_opp_qty": 0,
                "pre_seed_open_qty": 0,
                "seed_qty": 1,
                "seed_cost": 0.45,
                "source_pair_qty": 0.95,
                "source_pair_cost": 0.8,
                "source_pair_pnl": 0.15,
                "official_fee": 0.01,
                "source_residual_qty": 0.02,
                "source_residual_cost": 0.01,
            }
        )
with csv_path.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)

train_manifest.write_text(
    json.dumps(
        {
            "decision_label": "KEEP_SEPARATOR_TRAIN_HOLDOUT_OFFLINE_STABLE_NO_ORDER_REPRODUCTION_BLOCKED",
            "selection": {
                "selected_predicate": {
                    "side": "ANY",
                    "source_risk_direction": "repair_or_pairing_improving",
                    "offset_bucket": "offset_90_120",
                    "open_bucket": "open_le_1",
                    "deficit_bucket": "deficit_0_0_25",
                },
                "train": {"comparison": {"residual_cost_rate_reduction": 0.7}},
                "holdout": {"comparison": {"residual_cost_rate_reduction": 0.72}},
            },
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
completion.write_text(
    json.dumps(
        {
            "decision_label": "UNKNOWN_FIXTURE",
            "aggregate_metrics": {"candidates": 10, "micro_deficit_repair_guard_candidates": 0},
            "scorer_results": {"micro_deficit_summary": {"metrics": {"micro_deficit_exemplar_count": 0}}},
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
(summary_dir / "one.summary.json").write_text(
    json.dumps(
        {
            "event_lite": {
                "source_link_transition_diagnostics": {
                    "transition_count_by_status_side_offset_risk_direction": {
                        "admitted": {"YES": {"offset_90_120": {"repair_or_pairing_improving": 1}}},
                        "blocked": {"NO": {"offset_90_120": {"repair_or_pairing_improving": 8}}},
                    },
                    "pre_seed_same_qty_bucket_by_status_side_offset_risk_direction": {
                        "NO": {"offset_90_120": {"repair_or_pairing_improving": {"same_qty_1_2": 8}}}
                    },
                    "pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction": {
                        "NO": {"offset_90_120": {"repair_or_pairing_improving": {"opp_qty_2_5": 8}}}
                    },
                    "candidate_qty_bucket_by_status_side_offset_risk_direction": {
                        "NO": {"offset_90_120": {"repair_or_pairing_improving": {"candidate_qty_unknown": 8}}}
                    },
                    "transition_count_by_status_reason": {
                        "blocked": {"late_repair_only": 8},
                        "admitted": {"candidate": 1},
                    },
                    "quote_intent_presence_by_status": {"admitted": {"present": 1}, "blocked": {"missing": 8}},
                    "source_order_presence_by_status": {"admitted": {"present": 1}, "blocked": {"missing": 8}},
                    "source_sequence_presence_by_status": {"admitted": {"present": 1}, "blocked": {"missing": 8}},
                }
            }
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
PY

python3 "$ROOT_DIR/scripts/xuan_b27_dplus_source_opportunity_gap_audit.py" \
  --train-holdout-manifest "$TRAIN_MANIFEST" \
  --separator-csv "$CSV_PATH" \
  --no-order-completion-manifest "$COMPLETION" \
  --no-order-summary-dir "$SUMMARY_DIR" \
  --output "$OUTPUT" >/dev/null

python3 - "$OUTPUT" <<'PY'
import json
import sys
from pathlib import Path

data = json.loads(Path(sys.argv[1]).read_text())
assert data["decision"] == "KEEP", data["decision"]
gaps = set(data["exact_gap_classification"])
assert "no_order_micro_deficit_marker_count_zero" in gaps
assert "no_order_selected_offset_risk_has_no_open_le_1_micro_deficit_qty_bucket" in gaps
assert data["promotion_gate"]["passed"] is False
assert data["historical_separator_denominator"]["selected_rows"]["rows"] == 40
print(json.dumps({"decision": data["decision"], "output": str(Path(sys.argv[1]))}, sort_keys=True))
PY
