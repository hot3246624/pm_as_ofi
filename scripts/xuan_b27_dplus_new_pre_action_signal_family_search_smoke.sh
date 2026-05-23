#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$ROOT_DIR/xuan_research_artifacts/xuan_b27_dplus_new_pre_action_signal_family_search_smoke_${RUN_ID}"
FIXTURE_DIR="$OUT_DIR/fixture"
SUMMARY_DIR="$FIXTURE_DIR/summaries"
CSV_PATH="$FIXTURE_DIR/candidate_seed_outcome_separator.csv"
COMPLETION="$FIXTURE_DIR/completion.json"
OUTPUT="$OUT_DIR/manifest.json"

rm -rf "$OUT_DIR"
mkdir -p "$SUMMARY_DIR"

python3 - "$CSV_PATH" "$COMPLETION" "$SUMMARY_DIR" <<'PY'
import csv
import json
import sys
from pathlib import Path

csv_path = Path(sys.argv[1])
completion = Path(sys.argv[2])
summary_dir = Path(sys.argv[3])

fields = [
    "schema_version",
    "profile_name",
    "validation_request_id",
    "day",
    "condition_id",
    "slug",
    "source_seed_action_id",
    "source_seed_candidate_row_id",
    "source_label",
    "ts_ms",
    "ts_iso",
    "side",
    "opposite_side",
    "offset_s",
    "trigger_px",
    "trigger_size",
    "trigger_ts_ms",
    "seed_px",
    "seed_qty",
    "seed_cost",
    "official_fee_rate",
    "official_fee",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "outcome_labels_are_post_action",
]
rows = []
seq = 0
for day in ["2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"]:
    for _ in range(10):
        seq += 1
        rows.append(
            {
                "schema_version": "candidate_seed_outcome_separator_export_v1",
                "profile_name": "fixture",
                "validation_request_id": "fixture",
                "day": day,
                "condition_id": f"cond-{day}",
                "slug": f"slug-{day}",
                "source_seed_action_id": str(seq),
                "source_seed_candidate_row_id": str(seq),
                "source_label": "fixture",
                "ts_ms": str(seq),
                "ts_iso": f"{day}T00:00:00Z",
                "side": "YES",
                "opposite_side": "NO",
                "offset_s": "45",
                "trigger_px": "0.51",
                "trigger_size": "5",
                "trigger_ts_ms": str(seq),
                "seed_px": "0.49",
                "seed_qty": "1",
                "seed_cost": "0.49",
                "official_fee_rate": "0.07",
                "official_fee": "0.01",
                "source_risk_direction": "risk_increasing",
                "pre_seed_same_qty": "2",
                "pre_seed_opp_qty": "0",
                "pre_seed_same_cost": "1",
                "pre_seed_opp_cost": "0",
                "pre_seed_open_qty": "2",
                "pre_seed_open_cost": "1",
                "ledger_proxy_before": "1.5",
                "ledger_proxy_after": "-1.5",
                "source_pair_qty": "0.1",
                "source_pair_cost": "0.1",
                "source_pair_pnl": "0.0",
                "source_residual_qty": "0.9",
                "source_residual_cost": "0.45",
                "source_residual_age_s": "60",
                "pair_outcome_bucket": "paired_nonzero",
                "residual_tail_outcome_bucket": "residual_tail_cost_share_ge_50pct",
                "outcome_labels_are_post_action": "True",
            }
        )
    for _ in range(90):
        seq += 1
        rows.append(
            {
                "schema_version": "candidate_seed_outcome_separator_export_v1",
                "profile_name": "fixture",
                "validation_request_id": "fixture",
                "day": day,
                "condition_id": f"cond-{day}",
                "slug": f"slug-{day}",
                "source_seed_action_id": str(seq),
                "source_seed_candidate_row_id": str(seq),
                "source_label": "fixture",
                "ts_ms": str(seq),
                "ts_iso": f"{day}T00:00:00Z",
                "side": "NO",
                "opposite_side": "YES",
                "offset_s": "20",
                "trigger_px": "0.50",
                "trigger_size": "5",
                "trigger_ts_ms": str(seq),
                "seed_px": "0.48",
                "seed_qty": "1",
                "seed_cost": "0.48",
                "official_fee_rate": "0.07",
                "official_fee": "0.01",
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_same_qty": "0",
                "pre_seed_opp_qty": "0",
                "pre_seed_same_cost": "0",
                "pre_seed_opp_cost": "0",
                "pre_seed_open_qty": "0",
                "pre_seed_open_cost": "0",
                "ledger_proxy_before": "0.2",
                "ledger_proxy_after": "0.1",
                "source_pair_qty": "1",
                "source_pair_cost": "0.9",
                "source_pair_pnl": "0.12",
                "source_residual_qty": "0.0",
                "source_residual_cost": "0.0",
                "source_residual_age_s": "0",
                "pair_outcome_bucket": "paired_nonzero",
                "residual_tail_outcome_bucket": "residual_zero",
                "outcome_labels_are_post_action": "True",
            }
        )

with csv_path.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)

completion.write_text(
    json.dumps(
        {
            "decision_label": "UNKNOWN_FIXTURE",
            "aggregate_metrics": {
                "candidates": 120,
                "net_pair_cost_proxy_p90": 0.95,
                "residual_qty_share_of_filled": 0.1,
                "residual_cost_share_of_filled_cost": 0.1,
                "pair_tail_loss_share_of_pair_pnl": 0.01,
            },
            "scorer_results": {"shadow_acceptance": {"promotion_gate": {"passed": False}}},
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
                "source_opportunity_marker_summary": {
                    "schema_version": "source_opportunity_marker_summary_v1",
                    "field_contract": {"post_action_outcome_labels_included": False},
                    "transition_count_by_status_side_offset_risk_open_deficit": {
                        "admitted": {
                            "YES": {
                                "offset_30_60": {
                                    "risk_increasing": {"open_qty_1_2": {"deficit_le_0": 10}}
                                }
                            }
                        }
                    },
                }
            }
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
PY

python3 "$ROOT_DIR/scripts/xuan_b27_dplus_new_pre_action_signal_family_search.py" \
  --separator-csv "$CSV_PATH" \
  --no-order-completion-manifest "$COMPLETION" \
  --no-order-summary-dir "$SUMMARY_DIR" \
  --output "$OUTPUT" >/dev/null

python3 - "$OUTPUT" <<'PY'
import json
import sys
from pathlib import Path

data = json.loads(Path(sys.argv[1]).read_text())
assert data["decision"] == "UNKNOWN", data["decision"]
assert data["decision_label"] == "UNKNOWN_NEW_SIGNAL_FAMILY_HOLDOUT_STABLE_NO_ORDER_MARKER_FIELDS_MISSING"
selected_terms = data["search_summary"]["selected"]["predicate_terms"]
assert any(term["field"].startswith("ledger_") for term in selected_terms)
assert data["search_summary"]["selected"]["holdout_gate_passed"] is True
assert data["no_order_reproduction"]["selected_marker"]["supported_by_current_no_order_summary"] is False
assert data["promotion_gate"]["passed"] is False
print(json.dumps({"decision": data["decision"], "output": str(Path(sys.argv[1]))}, sort_keys=True))
PY
