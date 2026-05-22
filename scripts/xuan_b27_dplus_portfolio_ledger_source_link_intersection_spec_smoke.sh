#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_portfolio_ledger_source_link_intersection_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_portfolio_ledger_source_link_intersection_spec.py"
python3 -m py_compile "$script" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture = out_dir / "fixtures"
complete = fixture / "complete"
missing = fixture / "missing_portfolio"
complete.mkdir(parents=True, exist_ok=True)
missing.mkdir(parents=True, exist_ok=True)

def ledger(condition_id, bucket, residual_cost, pair_pnl):
    nonnegative = bucket == "risk_adjusted_nonnegative"
    return {
        "condition_id": condition_id,
        "slug": condition_id,
        "rule": "portfolio_risk_adjusted_nonnegative",
        "seed_actions": 10,
        "queue_supported_fills": 8,
        "pair_actions": 5,
        "pair_qty": 25.0,
        "pair_cost_sum": 22.5,
        "avg_pair_cost": 0.9,
        "net_pair_cost_sum": 22.8,
        "avg_net_pair_cost": 0.912,
        "pair_pnl": pair_pnl,
        "official_taker_fee_proxy": 0.3,
        "residual_qty": residual_cost / 0.5,
        "residual_cost": residual_cost,
        "residual_qty_by_side": {"YES": residual_cost / 0.5},
        "residual_cost_by_side": {"YES": residual_cost},
        "residual_lot_count_by_side": {"YES": 1},
        "stress_cost_proxy": 0.25,
        "conservative_risk_adjusted_proxy": pair_pnl - residual_cost - 0.55,
        "risk_adjusted_nonnegative": nonnegative,
        "risk_adjusted_bucket": bucket,
    }

aggregate = {
    "kind": "aggregate_report",
    "metrics": {"candidates": 140, "residual_cost": 4.0, "residual_qty": 8.0},
    "event_lite": {
        "portfolio_ledger_diagnostics": {
            "condition_count": 2,
            "condition_count_by_risk_adjusted_bucket": {
                "risk_adjusted_negative": 1.0,
                "risk_adjusted_nonnegative": 1.0,
            },
            "risk_adjusted_bucket": "risk_adjusted_negative",
            "risk_adjusted_nonnegative": False,
            "seed_actions": 20.0,
            "queue_supported_fills": 16.0,
            "pair_actions": 10.0,
            "pair_qty": 50.0,
            "pair_cost_sum": 45.0,
            "avg_pair_cost": 0.9,
            "net_pair_cost_sum": 45.6,
            "avg_net_pair_cost": 0.912,
            "pair_pnl": 2.1,
            "official_taker_fee_proxy": 0.6,
            "residual_qty": 8.0,
            "residual_cost": 4.0,
            "residual_qty_by_side": {"YES": 8.0},
            "residual_cost_by_side": {"YES": 4.0},
            "residual_lot_count_by_side": {"YES": 2.0},
            "stress_cost_proxy": 0.5,
            "conservative_risk_adjusted_proxy": -2.0,
        }
    },
}
source_link_score = {
    "status": "KEEP_SOURCE_LINK_TRANSITION_CROSS_BUCKET_SCORER_READY",
    "aggregate_parity": {"checked": True, "passed": True, "diff_count": 0, "diffs": []},
    "guardrails": {"missing_cross_bucket_fields": [], "summary_only_removal_recommended": False},
    "promotion_gate": {"passed": False},
}
exemplars = [
    {
        "condition_id": "cond-negative",
        "slug": "cond-negative",
        "side": "NO",
        "offset_s": 95.0,
        "source_risk_direction": "repair_or_pairing_improving",
        "source_residual_cost": 0.7,
        "source_pair_qty": 0.0,
        "quote_intent_id": "q-1",
        "source_order_id": "o-1",
        "source_sequence_id": "s-1",
    },
    {
        "condition_id": "cond-negative",
        "slug": "cond-negative",
        "side": "NO",
        "offset_s": 101.0,
        "source_risk_direction": "repair_or_pairing_improving",
        "source_residual_cost": 0.5,
        "source_pair_qty": 0.0,
        "quote_intent_id": "q-2",
        "source_order_id": "o-2",
        "source_sequence_id": "s-2",
    },
    {
        "condition_id": "cond-positive",
        "slug": "cond-positive",
        "side": "YES",
        "offset_s": 42.0,
        "source_risk_direction": "risk_increasing",
        "source_residual_cost": 0.2,
        "source_pair_qty": 4.0,
        "quote_intent_id": "q-3",
        "source_order_id": "o-3",
        "source_sequence_id": "s-3",
    },
]
residual_score = {
    "status": "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF",
    "exemplar_metrics": {
        "required_field_coverage": 1.0,
        "source_sequence_coverage": 1.0,
        "quote_intent_coverage": 1.0,
        "source_order_coverage": 1.0,
        "summary_exemplar_row_count": 3,
        "aggregate_exemplar_row_count": 3,
        "total_exemplar_source_residual_cost": 1.4,
    },
    "rankings": {
        "top_residual_tail_exemplars": exemplars,
        "top_source_side_offset_risk_direction_groups": [],
    },
    "promotion_gate": {"passed": False},
}
summary_negative = {
    "kind": "summary",
    "slug": "cond-negative",
    "event_lite": {"portfolio_ledger_diagnostics": ledger("cond-negative", "risk_adjusted_negative", 3.5, 0.4)},
}
summary_positive = {
    "kind": "summary",
    "slug": "cond-positive",
    "event_lite": {"portfolio_ledger_diagnostics": ledger("cond-positive", "risk_adjusted_nonnegative", 0.5, 1.7)},
}

(complete / "aggregate_report.json").write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")
(complete / "source_link_score.json").write_text(json.dumps(source_link_score, indent=2, sort_keys=True) + "\n")
(complete / "residual_tail_score.json").write_text(json.dumps(residual_score, indent=2, sort_keys=True) + "\n")
(complete / "cond-negative.summary.json").write_text(json.dumps(summary_negative, indent=2, sort_keys=True) + "\n")
(complete / "cond-positive.summary.json").write_text(json.dumps(summary_positive, indent=2, sort_keys=True) + "\n")

missing_aggregate = json.loads(json.dumps(aggregate))
del missing_aggregate["event_lite"]["portfolio_ledger_diagnostics"]
(missing / "aggregate_report.json").write_text(json.dumps(missing_aggregate, indent=2, sort_keys=True) + "\n")
(missing / "source_link_score.json").write_text(json.dumps(source_link_score, indent=2, sort_keys=True) + "\n")
(missing / "residual_tail_score.json").write_text(json.dumps(residual_score, indent=2, sort_keys=True) + "\n")
(missing / "cond-negative.summary.json").write_text(json.dumps({"kind": "summary", "slug": "cond-negative", "event_lite": {}}, indent=2, sort_keys=True) + "\n")
PY

complete_fixture="$out_dir/fixtures/complete"
missing_fixture="$out_dir/fixtures/missing_portfolio"

python3 "$script" \
  --aggregate-report "$complete_fixture/aggregate_report.json" \
  --summary "$complete_fixture" \
  --source-link-score "$complete_fixture/source_link_score.json" \
  --residual-tail-score "$complete_fixture/residual_tail_score.json" \
  --output-dir "$out_dir/complete_score" >> "$log" 2>&1

python3 "$script" \
  --aggregate-report "$missing_fixture/aggregate_report.json" \
  --summary "$missing_fixture" \
  --source-link-score "$missing_fixture/source_link_score.json" \
  --residual-tail-score "$missing_fixture/residual_tail_score.json" \
  --output-dir "$out_dir/missing_portfolio_score" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
complete = json.loads((out_dir / "complete_score" / "manifest.json").read_text())
missing = json.loads((out_dir / "missing_portfolio_score" / "manifest.json").read_text())
checks = {
    "complete_status_keep": complete["status"] == "KEEP_PORTFOLIO_LEDGER_SOURCE_LINK_INTERSECTION_SCORER_READY",
    "complete_joined_rows": complete["intersection_metrics"]["joined_exemplar_rows"] == 3,
    "complete_negative_share_high": complete["intersection_metrics"]["risk_adjusted_negative_joined_residual_cost_share"] > 0.8,
    "complete_promotion_false": complete["promotion_gate"]["passed"] is False,
    "missing_status_unknown": missing["status"] == "UNKNOWN_PORTFOLIO_LEDGER_SOURCE_LINK_INTERSECTION_INPUTS_MISSING",
    "missing_field_named": "aggregate_report.event_lite.portfolio_ledger_diagnostics" in missing["missing_files_or_fields"],
    "no_side_effects": complete["side_effects"]["orders_sent"] is False
    and complete["side_effects"]["ssh_started"] is False
    and complete["guardrails"]["no_events_jsonl_read"] is True,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise SystemExit(f"failed checks: {failed}\ncomplete={complete}\nmissing={missing}")
manifest = {
    "artifact": "xuan_b27_dplus_portfolio_ledger_source_link_intersection_spec_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "checks": checks,
    "outputs": {
        "complete_score": str(out_dir / "complete_score" / "manifest.json"),
        "missing_portfolio_score": str(out_dir / "missing_portfolio_score" / "manifest.json"),
    },
    "side_effects": {
        "ssh_started": False,
        "network_started": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "shared_ingress_modified": False,
        "broker_modified": False,
        "service_control_used": False,
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps({"status": "PASS", "checks": checks}, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "PASS $out_dir"
