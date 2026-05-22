#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_micro_deficit_no_order_summary_scorer_smoke_$ts}"
mkdir -p "$out_dir"/{positive,weak}

script="scripts/xuan_b27_dplus_micro_deficit_no_order_summary_scorer.py"

python3 - "$out_dir" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])

def write(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")

def diag(scale=1.0):
    return {
        "transition_count_by_status_side_offset_risk_direction": {
            "admitted": {"NO|offset_90_120|repair_or_pairing_improving": int(6 * scale)}
        },
        "pre_seed_same_qty_bucket_by_status_side_offset_risk_direction": {
            "admitted": {"NO|offset_90_120|repair_or_pairing_improving": {"same_qty_le_1": int(6 * scale)}}
        },
        "pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction": {
            "admitted": {"NO|offset_90_120|repair_or_pairing_improving": {"opp_qty_le_1": int(6 * scale)}}
        },
        "pre_seed_same_cost_bucket_by_status_side_offset_risk_direction": {
            "admitted": {"NO|offset_90_120|repair_or_pairing_improving": {"same_cost_le_1": int(6 * scale)}}
        },
        "pre_seed_opp_cost_bucket_by_status_side_offset_risk_direction": {
            "admitted": {"NO|offset_90_120|repair_or_pairing_improving": {"opp_cost_le_1": int(6 * scale)}}
        },
        "immediate_pair_qty_bucket_by_source_side_offset_risk_direction": {
            "NO|offset_90_120|repair_or_pairing_improving": {"pair_qty_le_1": int(1 * scale)}
        },
        "residual_cost_by_source_side_offset_risk_direction": {
            "NO|offset_90_120|repair_or_pairing_improving": 7.5 * scale
        },
    }

def exemplar(i, micro=True, residual=2.0):
    same = 0.1 if micro else 1.5
    opp = 0.25 if micro else 1.6
    return {
        "quote_intent_id": f"q{i}",
        "source_order_id": f"o{i}",
        "source_sequence_id": f"s{i}",
        "condition_id": f"c{i}",
        "slug": f"btc-updown-5m-{i}",
        "side": "NO",
        "offset_s": 95.0,
        "source_risk_direction": "risk_increasing" if micro else "repair_or_pairing_improving",
        "micro_deficit_repair_guard_candidate": micro,
        "micro_deficit_repair_deficit": max(0.0, opp - same),
        "trigger_px": 0.42,
        "trigger_size": 5.0,
        "pre_seed_same_qty": same,
        "pre_seed_opp_qty": opp,
        "pre_seed_open_qty": same + opp,
        "pre_seed_same_cost": same * 0.4,
        "pre_seed_opp_cost": opp * 0.4,
        "ledger_proxy_before": -0.1,
        "ledger_proxy_after": -0.05,
        "source_pair_qty": 0.05 if micro else 3.0,
        "source_pair_cost": 0.045 if micro else 2.7,
        "source_pair_pnl": 0.005 if micro else 0.3,
        "source_residual_qty": residual * 2.0,
        "source_residual_cost": residual,
        "source_residual_age_ms": 30000,
    }

runner = {
    "script": "xuan_dplus_passive_passive_shadow_runner.py",
    "safety": {"orders_sent": False},
    "orders_sent": False,
    "cancels_sent": False,
    "redeems_sent": False,
    "started_canary": False,
}
write(root / "positive" / "manifest.json", runner)
positive_summaries = []
for idx in range(2):
    rows = [exemplar(idx * 4 + j, micro=True, residual=2.0) for j in range(3)]
    rows.append(exemplar(idx * 4 + 3, micro=False, residual=0.5))
    positive_summaries.append({
        "event_lite": {
            "source_link_transition_diagnostics": diag(1.0),
            "source_link_residual_tail_exemplars": rows,
        }
    })
    write(root / "positive" / f"{idx}.summary.json", positive_summaries[-1])
write(root / "positive" / "aggregate_report.json", {
    "event_lite": {
        "source_link_transition_diagnostics": diag(2.0),
        "source_link_residual_tail_exemplars": [],
    }
})

write(root / "weak" / "manifest.json", runner)
weak_summary = {
    "event_lite": {
        "source_link_transition_diagnostics": {},
        "source_link_residual_tail_exemplars": [exemplar(100, micro=False, residual=1.0)],
    }
}
write(root / "weak" / "0.summary.json", weak_summary)
write(root / "weak" / "aggregate_report.json", {"event_lite": {"source_link_transition_diagnostics": {}}})
PY

python3 -m py_compile "$script"

python3 "$script" \
  --runner-manifest "$out_dir/positive/manifest.json" \
  --aggregate-report "$out_dir/positive/aggregate_report.json" \
  --summary "$out_dir/positive" \
  --output-dir "$out_dir/positive_score" \
  > "$out_dir/positive_score.log"

set +e
python3 "$script" \
  --runner-manifest "$out_dir/weak/manifest.json" \
  --aggregate-report "$out_dir/weak/aggregate_report.json" \
  --summary "$out_dir/weak" \
  --output-dir "$out_dir/weak_score" \
  > "$out_dir/weak_score.log"
weak_status=$?
set -e
if [[ "$weak_status" -ne 3 ]]; then
  echo "expected weak fixture to return 3, got $weak_status" >&2
  exit 1
fi

python3 - "$out_dir" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
positive = json.loads((root / "positive_score" / "manifest.json").read_text())
weak = json.loads((root / "weak_score" / "manifest.json").read_text())
assert positive["decision_label"] == "KEEP_MICRO_DEFICIT_NO_ORDER_SUMMARY_SCORER_READY"
assert positive["promotion_gate"]["passed"] is False
assert positive["scope"]["events_jsonl_read"] is False
assert positive["source_link_checks"]["aggregate_parity_passed"] is True
assert positive["metrics"]["micro_deficit_exemplar_count"] == 6
assert positive["metrics"]["micro_deficit_source_residual_cost_share"] > 0.8
assert "source_pair_qty" in positive["contract"]["post_action_labels_not_live_criteria"]
assert weak["decision_label"] == "UNKNOWN_MICRO_DEFICIT_NO_ORDER_SUMMARY_INPUTS_INSUFFICIENT"
assert "source_link_required_fields_missing" in weak["blockers"]
assert "micro_deficit_exemplar_count_below_threshold" in weak["blockers"]
manifest = {
    "artifact": "xuan_b27_dplus_micro_deficit_no_order_summary_scorer_smoke",
    "created_utc": root.name.rsplit("_", 1)[-1],
    "decision": "KEEP",
    "decision_label": "KEEP_MICRO_DEFICIT_NO_ORDER_SUMMARY_SCORER_SMOKE_PASS",
    "positive_score": str(root / "positive_score" / "manifest.json"),
    "weak_score": str(root / "weak_score" / "manifest.json"),
    "checks": {
        "positive_keep": True,
        "weak_unknown": True,
        "post_action_labels_not_live_criteria": True,
        "promotion_gate_false": True,
        "no_events_raw_replay_or_network": True,
    },
}
(root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
