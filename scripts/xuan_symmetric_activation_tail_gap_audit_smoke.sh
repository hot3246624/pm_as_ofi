#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_tail_gap_audit_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_symmetric_activation_tail_gap_audit.py >> "$log" 2>&1

good_root="$out_dir/good_fixture"
bad_root="$out_dir/bad_fixture"
good_out="$out_dir/good"
bad_out="$out_dir/bad"
mkdir -p "$good_root" "$bad_root"

python3 - "$good_root" "$bad_root" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

good_root = Path(sys.argv[1])
bad_root = Path(sys.argv[2])

aggregate = {
    "metrics": {
        "candidates": 140,
        "pair_actions": 20,
        "pair_pnl": 3.0,
        "residual_qty": 12.0,
        "residual_cost": 4.0,
        "net_pair_cost_proxy_p90": 1.08
    },
    "event_lite": {
        "net_pair_cost_buckets": {
            "pair_cost_lt_0p95": 8,
            "pair_cost_0p95_1p00": 4,
            "pair_cost_1p00_1p05": 5,
            "pair_cost_gt_1p05": 3
        },
        "pair_cost_by_source_offset_bucket": {
            "pair_cost_1p00_1p05": {"offset_ge_120": 8, "offset_30_60": 1},
            "pair_cost_gt_1p05": {"offset_ge_120": 3}
        },
        "pair_cost_by_source_public_px_bucket": {
            "pair_cost_1p00_1p05": {"px_ge_0p55": 5, "px_0p30_0p45": 4},
            "pair_cost_gt_1p05": {"px_ge_0p55": 3}
        },
        "residual_lot_side_cost": {"YES": 3.2, "NO": 0.8},
        "residual_lot_side_qty": {"YES": 9.0, "NO": 3.0},
        "residual_lot_px_qty_buckets": {"px_lt_0p30": 7.0, "px_0p30_0p45": 2.5, "px_ge_0p55": 2.5},
        "source_link_transition_diagnostics": {
            "residual_cost_by_source_side_offset_risk_direction": {"YES|offset_ge_120|risk_increasing": 2.5, "NO|offset_30_60|risk_increasing": 0.5},
            "residual_qty_by_source_side_offset_risk_direction": {"YES|offset_ge_120|risk_increasing": 8.0, "NO|offset_30_60|risk_increasing": 2.0},
            "residual_count_by_source_side_offset_risk_direction": {"YES|offset_ge_120|risk_increasing": 4, "NO|offset_30_60|risk_increasing": 1},
            "immediate_pair_cost_bucket_by_source_side_offset_risk_direction": {
                "YES|offset_ge_120|risk_increasing": {"pair_cost_1p00_1p05": 4, "pair_cost_gt_1p05": 2},
                "NO|offset_30_60|risk_increasing": {"pair_cost_lt_0p95": 2}
            }
        },
        "symmetric_activation_summary": {
            "schema_version": "symmetric_activation_contract_summary_v1",
            "field_contract": {
                "default_off": True,
                "post_action_outcome_labels_included": False,
                "realized_pair_cost_used_as_live_criteria": False,
                "trading_behavior_changed": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate_passed": False
            },
            "candidate_count_by_status_reason_activation_bucket": {
                "admitted|candidate": {"activation_required_activation_opp_age_1_5s": 7},
                "blocked|activation_opp_seen": {"activation_required_missing_opp": 3}
            },
            "source_sequence_presence_by_status_reason_activation_bucket": {
                "admitted|candidate|activation_required_activation_opp_age_1_5s": {"present": 7},
                "blocked|activation_opp_seen|activation_required_missing_opp": {"present": 3}
            }
        }
    }
}
shadow = {
    "acceptance_passed": False,
    "checks": {"no_order_safety_ok": True},
    "trading_metrics": {
        "candidates": 140,
        "pair_actions": 20,
        "pair_pnl": 3.0,
        "fee_adjusted_pair_pnl_proxy": 2.8,
        "net_pair_cost_p90": 1.08,
        "residual_qty": 12.0,
        "residual_cost": 4.0,
        "material_residual_lots": 0
    },
    "promotion_gate": {
        "normalized_risk_budget": {
            "residual_qty_share_of_filled": 0.18,
            "residual_cost_share_of_filled_cost": 0.12,
            "pair_tail_loss_share_of_pair_pnl": 0.08,
            "material_residual_lots": 0
        }
    }
}
scorer = {
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_READY",
    "score": {
        "checks": {
            "schema_ok": True,
            "field_contract_ok": True,
            "source_sequence_coverage_present": True,
            "aggregate_parity": True
        }
    },
    "promotion_gate": {"passed": False},
    "research_ranking": {"strategy_evidence": False}
}
adapter = {
    "adapter_evaluation": {
        "shadow_acceptance_gate": {"passed": False},
        "symmetric_activation_gate": {"passed": True}
    },
    "promotion_gate": {"passed": False},
    "research_ranking": {"strategy_evidence": False}
}
for name, value in {
    "aggregate.json": aggregate,
    "shadow.json": shadow,
    "scorer.json": scorer,
    "adapter.json": adapter,
}.items():
    (good_root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")

bad_aggregate = json.loads(json.dumps(aggregate))
bad_aggregate["event_lite"].pop("symmetric_activation_summary")
for name, value in {
    "aggregate.json": bad_aggregate,
    "shadow.json": shadow,
    "scorer.json": scorer,
    "adapter.json": adapter,
}.items():
    (bad_root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_symmetric_activation_tail_gap_audit.py \
  --aggregate "$good_root/aggregate.json" \
  --shadow-acceptance-manifest "$good_root/shadow.json" \
  --symmetric-activation-scorer-manifest "$good_root/scorer.json" \
  --acceptance-adapter-manifest "$good_root/adapter.json" \
  --output-dir "$good_out" \
  >> "$log" 2>&1

set +e
python3 scripts/xuan_symmetric_activation_tail_gap_audit.py \
  --aggregate "$bad_root/aggregate.json" \
  --shadow-acceptance-manifest "$bad_root/shadow.json" \
  --symmetric-activation-scorer-manifest "$bad_root/scorer.json" \
  --acceptance-adapter-manifest "$bad_root/adapter.json" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1
bad_rc=$?
set -e
if [[ "$bad_rc" -eq 0 ]]; then
  echo "bad fixture unexpectedly passed" >> "$log"
  exit 1
fi

python3 - "$good_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

good = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
checks = {
    "good_keep": good["decision_label"] == "KEEP_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_INSTRUMENTATION_TARGET_READY",
    "good_not_strategy_evidence": good["research_ranking"]["strategy_evidence"] is False,
    "good_promotion_false": good["promotion_gate"]["passed"] is False,
    "good_target_named": good["evaluation"]["recommended_instrumentation_target"]["name"] == "symmetric_activation_tail_attribution_summary_v1",
    "good_no_shadow_allowed": good["research_ranking"]["no_order_diagnostic_allowed"] is False,
    "bad_unknown": bad["decision_label"] == "UNKNOWN_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_INPUTS_INSUFFICIENT",
    "bad_no_promotion": bad["promotion_gate"]["passed"] is False,
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"symmetric activation tail-gap audit smoke failed: {failed}")
manifest = {
    "artifact": "xuan_symmetric_activation_tail_gap_audit_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_SMOKE_PASS",
    "checks": checks,
    "good_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
