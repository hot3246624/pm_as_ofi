#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_post_guard_next_target_review_smoke_$ts}"
mkdir -p "$out_dir/fixtures"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_post_guard_next_target_review.py"
python3 -m py_compile "$script" >> "$log" 2>&1

cat > "$out_dir/fixtures/completion.json" <<'JSON'
{
  "decision": "UNKNOWN",
  "decision_label": "UNKNOWN_PORTFOLIO_LEDGER_SOURCE_LINK_EXEMPLAR_RISK_BUDGET_FAIL_BUT_INTERSECTION_KEEP",
  "scorers": {
    "portfolio_ledger_source_link_intersection": {
      "status": "KEEP_PORTFOLIO_LEDGER_SOURCE_LINK_INTERSECTION_SCORER_READY",
      "intersection_metrics": {
        "top_intersection_groups": [
          {
            "source_side_offset_risk_direction_ledger": "NO|offset_0_30|risk_increasing|risk_adjusted_negative",
            "source_residual_cost_share": 0.302158,
            "source_pair_qty": 0.0
          }
        ]
      }
    },
    "residual_tail_exemplar": {
      "status": "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF",
      "exemplar_metrics": {
        "required_field_coverage": 1.0,
        "source_sequence_coverage": 1.0
      }
    }
  }
}
JSON

cat > "$out_dir/fixtures/guard.json" <<'JSON'
{
  "decision": "DISCARD",
  "decision_label": "DISCARD_PORTFOLIO_NEGATIVE_RISK_GUARD_SAMPLE_OR_PAIR_COLLAPSE",
  "research_ranking": {
    "blockers": ["seed_action_retention_below_0.90"],
    "seed_action_retention": 0.87275132,
    "pair_qty_retention": 0.87308604,
    "residual_qty_rate_reduction": 0.00694387,
    "residual_cost_rate_reduction": -0.03474129
  },
  "delta_vs_control": {
    "stress100_worst_pnl": {"absolute_delta": -389.8289},
    "worst_day_fee_after_pnl": {"absolute_delta": -16.289847}
  }
}
JSON

set +e
python3 "$script" \
  --completion-manifest "$out_dir/fixtures/completion.json" \
  --guard-verifier-manifest "$out_dir/fixtures/guard.json" \
  --output "$out_dir/review.json" >> "$log" 2>&1
review_status=$?
set -e
test "$review_status" -eq 1
python3 -m json.tool "$out_dir/review.json" >/dev/null

python3 - "$out_dir/review.json" "$out_dir/manifest.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

review = json.loads(Path(sys.argv[1]).read_text())
checks = {
    "decision_unknown": review["decision"] == "UNKNOWN",
    "label_expected": review["decision_label"] == "UNKNOWN_POST_GUARD_NO_SAFE_NARROW_ACTION_LEVEL_MECHANISM",
    "missing_fields_named": len(review["exact_missing_fields_or_capabilities"]) >= 3,
    "no_target_selected": review["selected_next_target"]["name"] is None,
    "future_attribution_rejected": any(
        row["candidate"] == "source_pair_zero_residual_tail_guard" and row["decision"] == "DISCARD"
        for row in review["candidate_reviews"]
    ),
    "promotion_false": review["promotion_gate"]["passed"] is False,
    "no_side_effects": review["side_effects"]["ssh_started"] is False
    and review["side_effects"]["orders_sent"] is False,
}
failed = [key for key, value in checks.items() if not value]
if failed:
    raise AssertionError(f"post-guard next target review smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_post_guard_next_target_review_smoke",
    "status": "PASS",
    "decision": "KEEP",
    "decision_label": "KEEP_POST_GUARD_NEXT_TARGET_REVIEW_SMOKE_PASS",
    "review": sys.argv[1],
    "checks": checks,
    "promotion_gate": {
        "passed": False,
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False
    },
    "side_effects": review["side_effects"],
}
Path(sys.argv[2]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
printf 'PASS D+ post-guard next-target review smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
