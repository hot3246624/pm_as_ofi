#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$ROOT_DIR/xuan_research_artifacts/xuan_b27_dplus_ledger_before_delta_gap_audit_smoke_${RUN_ID}"
FIXTURE_DIR="$OUT_DIR/fixtures"
mkdir -p "$FIXTURE_DIR/pass_summaries" "$FIXTURE_DIR/fail_summaries"

python3 - <<'PY' "$FIXTURE_DIR"
import csv
import json
import sys
from pathlib import Path

fixture = Path(sys.argv[1])
fieldnames = [
    "day",
    "side",
    "offset_s",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "seed_qty",
    "seed_cost",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "official_fee",
]
rows = []
for day in ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"]:
    for _ in range(20):
        rows.append(
            {
                "day": day,
                "side": "NO",
                "offset_s": "155",
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_same_qty": "2",
                "pre_seed_opp_qty": "5",
                "ledger_proxy_before": "-1.0",
                "ledger_proxy_after": "-1.5",
                "seed_qty": "10",
                "seed_cost": "10",
                "source_pair_qty": "10",
                "source_pair_cost": "10",
                "source_pair_pnl": "2",
                "source_residual_qty": "0",
                "source_residual_cost": "0",
                "official_fee": "0",
            }
        )
    for _ in range(2):
        rows.append(
            {
                "day": day,
                "side": "YES",
                "offset_s": "12",
                "source_risk_direction": "risk_increasing",
                "pre_seed_same_qty": "0",
                "pre_seed_opp_qty": "0",
                "ledger_proxy_before": "0.1",
                "ledger_proxy_after": "-0.5",
                "seed_qty": "1",
                "seed_cost": "1",
                "source_pair_qty": "0",
                "source_pair_cost": "0",
                "source_pair_pnl": "0",
                "source_residual_qty": "1",
                "source_residual_cost": "1",
                "official_fee": "0",
            }
        )
with (fixture / "separator.csv").open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

completion = {
    "decision_label": "UNKNOWN_FIXTURE",
    "trading_metrics": {
        "candidates": 103,
        "net_pair_cost_p90": 1.01,
        "residual_qty_share_of_filled": 0.25,
        "residual_cost_share_of_filled_cost": 0.16,
        "pair_tail_loss_share_of_pair_pnl": 0.01,
        "fee_adjusted_pair_pnl_proxy": 1.0,
        "conservative_risk_adjusted_pnl_proxy": -1.0,
    },
    "promotion_gate": {
        "passed": False,
        "source": {
            "hard_blockers": ["shadow_trading_promotion_risk_budget_failed"],
            "normalized_risk_budget": {
                "residual_qty_share_of_filled": 0.25,
                "residual_cost_share_of_filled_cost": 0.16,
                "pair_tail_loss_share_of_pair_pnl": 0.01,
            },
        },
    },
}
(fixture / "completion.json").write_text(json.dumps(completion, indent=2) + "\n")

summary_pass = {
    "event_lite": {
        "source_opportunity_marker_summary": {
            "transition_count_by_status_side_offset_risk_open_deficit_ledger_before": {
                "admitted": {
                    "YES|offset_0_30|risk_increasing|open_qty_zero|deficit_le_0|before_0_025": 3
                }
            },
            "transition_count_by_status_side_offset_risk_open_deficit_ledger_delta": {
                "admitted": {
                    "YES|offset_0_30|risk_increasing|open_qty_zero|deficit_le_0|delta_m1_m025": 3
                }
            },
        },
        "source_link_residual_tail_exemplars": [
            {
                "side": "YES",
                "offset_s": 12,
                "source_risk_direction": "risk_increasing",
                "pre_seed_same_qty": 0,
                "pre_seed_opp_qty": 0,
                "ledger_proxy_before": 0.1,
                "ledger_proxy_after": -0.5,
                "source_residual_cost": 1.0,
            }
        ],
    }
}
summary_fail = {
    "event_lite": {
        "source_opportunity_marker_summary": {
            "transition_count_by_status_side_offset_risk_open_deficit_ledger_before": {
                "admitted": {
                    "NO|offset_ge_120|repair_or_pairing_improving|open_qty_2_5|deficit_gt_2|before_m1_m025": 3
                }
            },
            "transition_count_by_status_side_offset_risk_open_deficit_ledger_delta": {
                "blocked": {
                    "NO|offset_ge_120|repair_or_pairing_improving|open_qty_2_5|deficit_gt_2|delta_unknown": 3
                }
            },
        },
        "source_link_residual_tail_exemplars": [],
    }
}
(fixture / "pass_summaries" / "fixture.summary.json").write_text(json.dumps(summary_pass, indent=2) + "\n")
(fixture / "fail_summaries" / "fixture.summary.json").write_text(json.dumps(summary_fail, indent=2) + "\n")
(fixture / "aggregate_pass.json").write_text(json.dumps(summary_pass, indent=2) + "\n")
(fixture / "aggregate_fail.json").write_text(json.dumps(summary_fail, indent=2) + "\n")
PY

python3 "$ROOT_DIR/scripts/xuan_b27_dplus_ledger_before_delta_gap_audit.py" \
  --separator-csv "$FIXTURE_DIR/separator.csv" \
  --completion-manifest "$FIXTURE_DIR/completion.json" \
  --aggregate-report "$FIXTURE_DIR/aggregate_pass.json" \
  --summary-dir "$FIXTURE_DIR/pass_summaries" \
  --output "$OUT_DIR/pass_manifest.json" \
  --min-match 1

python3 "$ROOT_DIR/scripts/xuan_b27_dplus_ledger_before_delta_gap_audit.py" \
  --separator-csv "$FIXTURE_DIR/separator.csv" \
  --completion-manifest "$FIXTURE_DIR/completion.json" \
  --aggregate-report "$FIXTURE_DIR/aggregate_fail.json" \
  --summary-dir "$FIXTURE_DIR/fail_summaries" \
  --output "$OUT_DIR/fail_manifest.json" \
  --min-match 1

python3 - <<'PY' "$OUT_DIR"
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
passed = json.loads((out / "pass_manifest.json").read_text())
failed = json.loads((out / "fail_manifest.json").read_text())
assert passed["decision_label"] == "KEEP_LEDGER_BEFORE_DELTA_GAP_AUDIT_NEW_PRE_ACTION_FAMILY_FOUND"
assert passed["candidate_search"]["train_holdout_marker_candidate_count"] >= 1
assert "latest_no_order_aggregate_risk_budget_failed_not_candidate_specific" in passed["cautions"]
assert failed["decision_label"] == "UNKNOWN_LEDGER_BEFORE_DELTA_GAP_AUDIT_NO_SAFE_FAMILY"
assert "no_train_holdout_stable_candidate_with_nonzero_before_or_delta_marker" in failed["blockers"]
manifest = {
    "artifact": "xuan_b27_dplus_ledger_before_delta_gap_audit_smoke",
    "checks": {
        "positive_fixture_keep": True,
        "negative_fixture_fail_closed": True,
        "risk_budget_reported_not_promotion": True,
    },
    "pass_manifest": str(out / "pass_manifest.json"),
    "fail_manifest": str(out / "fail_manifest.json"),
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest["checks"], sort_keys=True))
PY
