#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$ROOT_DIR/xuan_research_artifacts/xuan_b27_dplus_candidate_separator_train_holdout_audit_smoke_${RUN_ID}"
FIXTURE_DIR="$OUT_DIR/fixture"
CSV_PATH="$FIXTURE_DIR/candidate_seed_outcome_separator.csv"
NO_ORDER="$FIXTURE_DIR/no_order_completion.json"
OUTPUT="$OUT_DIR/manifest.json"

rm -rf "$OUT_DIR"
mkdir -p "$FIXTURE_DIR"

python3 - "$CSV_PATH" "$NO_ORDER" <<'PY'
import csv
import json
import sys
from pathlib import Path

csv_path = Path(sys.argv[1])
no_order = Path(sys.argv[2])
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
days = ["2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"]
for idx, day in enumerate(days):
    for n in range(120):
        rows.append(
            {
                "day": day,
                "side": "YES" if n % 2 else "NO",
                "source_risk_direction": "risk_increasing",
                "offset_s": 20,
                "pre_seed_same_qty": 0,
                "pre_seed_opp_qty": 0,
                "pre_seed_open_qty": 0,
                "seed_qty": 1,
                "seed_cost": 0.45,
                "source_pair_qty": 0.95,
                "source_pair_cost": 0.83,
                "source_pair_pnl": 0.17,
                "official_fee": 0.01,
                "source_residual_qty": 0.02,
                "source_residual_cost": 0.01,
            }
        )
    for n in range(12):
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
                "seed_cost": 0.55,
                "source_pair_qty": 0.2,
                "source_pair_cost": 0.2,
                "source_pair_pnl": 0.02,
                "official_fee": 0.01,
                "source_residual_qty": 0.9,
                "source_residual_cost": 0.5,
            }
        )

with csv_path.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)

no_order.write_text(
    json.dumps(
        {
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_FIXTURE_NO_ORDER_MARKER_MISSING",
            "aggregate_metrics": {
                "candidates": 80,
                "net_pair_cost_proxy_p90": 1.1,
                "residual_qty_share_of_filled": 0.3,
                "residual_cost_share_of_filled_cost": 0.25,
                "micro_deficit_repair_guard_candidates": 0,
            },
            "scorer_results": {
                "micro_deficit_summary": {"metrics": {"micro_deficit_exemplar_count": 0}},
                "shadow_acceptance": {
                    "promotion_gate": {
                        "normalized_risk_budget": {
                            "pair_tail_loss_share_of_pair_pnl": 0.2
                        }
                    }
                },
            },
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
PY

python3 "$ROOT_DIR/scripts/xuan_b27_dplus_candidate_separator_train_holdout_audit.py" \
  --separator-csv "$CSV_PATH" \
  --no-order-completion-manifest "$NO_ORDER" \
  --output "$OUTPUT" >/dev/null

python3 - "$OUTPUT" <<'PY'
import json
import sys
from pathlib import Path

data = json.loads(Path(sys.argv[1]).read_text())
assert data["decision"] == "KEEP", data["decision"]
assert data["selection"]["holdout_gate_passed"] is True
assert data["no_order_reproduction"]["marker_reproduced"] is False
assert data["promotion_gate"]["passed"] is False
selected = data["selection"]["selected_predicate"]
assert selected["source_risk_direction"] == "repair_or_pairing_improving"
print(json.dumps({"decision": data["decision"], "output": str(Path(sys.argv[1]))}, sort_keys=True))
PY
