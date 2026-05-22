#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_residual_tail_exemplar_shadow_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_residual_tail_exemplar_shadow_scorer.py"
python3 -m py_compile "$script" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture = out_dir / "fixtures" / "pass_shadow"
fixture.mkdir(parents=True, exist_ok=True)
slug = "btc-updown-5m-1900000000"
rows = [
    {
        "quote_intent_id": f"{slug}:quote:1",
        "source_order_id": "seed-1",
        "source_sequence_id": "seq-1",
        "condition_id": slug,
        "slug": slug,
        "side": "YES",
        "offset_s": 72.0,
        "source_risk_direction": "risk_increasing",
        "trigger_px": 0.41,
        "trigger_size": 20.0,
        "trigger_ts_ms": 1900000072000,
        "pre_seed_same_qty": 4.0,
        "pre_seed_opp_qty": 1.0,
        "pre_seed_same_cost": 1.64,
        "pre_seed_opp_cost": 0.38,
        "ledger_proxy_before": -1.26,
        "ledger_proxy_after": -1.86,
        "source_pair_qty": 8.0,
        "source_pair_cost": 7.72,
        "source_pair_pnl": 0.28,
        "source_residual_qty": 2.0,
        "source_residual_cost": 0.82,
        "source_residual_age_ms": 180000,
        "lot_id": 11,
        "seed_px": 0.41,
    },
    {
        "quote_intent_id": f"{slug}:quote:2",
        "source_order_id": "seed-2",
        "source_sequence_id": "seq-2",
        "condition_id": slug,
        "slug": slug,
        "side": "NO",
        "offset_s": 46.0,
        "source_risk_direction": "repair_or_pairing_improving",
        "trigger_px": 0.39,
        "trigger_size": 18.0,
        "trigger_ts_ms": 1900000046000,
        "pre_seed_same_qty": 0.5,
        "pre_seed_opp_qty": 3.0,
        "pre_seed_same_cost": 0.2,
        "pre_seed_opp_cost": 1.17,
        "ledger_proxy_before": 0.97,
        "ledger_proxy_after": 0.52,
        "source_pair_qty": 6.0,
        "source_pair_cost": 5.66,
        "source_pair_pnl": 0.34,
        "source_residual_qty": 1.0,
        "source_residual_cost": 0.39,
        "source_residual_age_ms": 90000,
        "lot_id": 12,
        "seed_px": 0.39,
    },
    {
        "quote_intent_id": f"{slug}:quote:3",
        "source_order_id": "seed-3",
        "source_sequence_id": "seq-3",
        "condition_id": slug,
        "slug": slug,
        "side": "YES",
        "offset_s": 102.0,
        "source_risk_direction": "risk_increasing",
        "trigger_px": 0.36,
        "trigger_size": 16.0,
        "trigger_ts_ms": 1900000102000,
        "pre_seed_same_qty": 2.0,
        "pre_seed_opp_qty": 2.0,
        "pre_seed_same_cost": 0.72,
        "pre_seed_opp_cost": 0.78,
        "ledger_proxy_before": -0.06,
        "ledger_proxy_after": -0.42,
        "source_pair_qty": 5.0,
        "source_pair_cost": 4.55,
        "source_pair_pnl": 0.45,
        "source_residual_qty": 0.75,
        "source_residual_cost": 0.27,
        "source_residual_age_ms": 45000,
        "lot_id": 13,
        "seed_px": 0.36,
    },
]
aggregate = {
    "kind": "aggregate_report",
    "script": "xuan_dplus_passive_passive_shadow_runner.py",
    "slugs": 1,
    "blocked": {},
    "metrics": {
        "candidates": 140,
        "queue_supported_fills": 100,
        "filled_qty": 180.0,
        "filled_cost": 82.0,
        "pair_actions": 82,
        "pair_qty": 150.0,
        "qty_pair_share_of_filled": 0.8333333333,
        "pair_pnl": 12.5,
        "taker_fee": 0.3,
        "roi_on_filled_cost": 0.1524390244,
        "residual_qty": 18.0,
        "residual_cost": 9.0,
        "material_residual_lots": 0,
        "net_pair_cost_proxy_p90": 0.98,
    },
    "event_lite": {
        "source_link_residual_tail_exemplars": rows,
        "source_link_transition_diagnostics": {},
    },
}
summary = {
    "kind": "summary",
    "script": "xuan_dplus_passive_passive_shadow_runner.py",
    "slug": slug,
    "final": True,
    "metrics": aggregate["metrics"],
    "event_lite": {
        "source_link_residual_tail_exemplars": rows,
    },
}
manifest = {
    "kind": "manifest",
    "script": "xuan_dplus_passive_passive_shadow_runner.py",
    "markets": [{"POLYMARKET_MARKET_SLUG": slug}],
    "safety": {
        "orders_sent": False,
        "dry_run": "true",
        "shared_ingress_role": "client",
        "shared_ingress_root": "/srv/pm_as_ofi/shared-ingress-main",
    },
    "orders_sent": False,
    "cancels_sent": False,
    "redeems_sent": False,
    "started_canary": False,
}
source_score = {
    "status": "KEEP_SOURCE_LINK_TRANSITION_CROSS_BUCKET_SCORER_READY",
    "aggregate_parity": {"checked": True, "passed": True},
    "guardrails": {"missing_cross_bucket_fields": []},
    "promotion_gate": {"passed": False},
}
(fixture / "aggregate_report.json").write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")
(fixture / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
(fixture / f"{slug}.summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
(fixture / "source_link_score.json").write_text(json.dumps(source_score, indent=2, sort_keys=True) + "\n")

missing_field = json.loads(json.dumps(aggregate))
del missing_field["event_lite"]["source_link_residual_tail_exemplars"][0]["source_sequence_id"]
(fixture / "aggregate_missing_field.json").write_text(json.dumps(missing_field, indent=2, sort_keys=True) + "\n")
PY

pass_fixture="$out_dir/fixtures/pass_shadow"

python3 "$script" \
  --aggregate-report "$pass_fixture/aggregate_report.json" \
  --runner-manifest "$pass_fixture/manifest.json" \
  --source-link-score "$pass_fixture/source_link_score.json" \
  --summary "$pass_fixture" \
  --output-dir "$out_dir/pass_score" >> "$log" 2>&1

set +e
python3 "$script" \
  --aggregate-report "$pass_fixture/aggregate_missing_field.json" \
  --runner-manifest "$pass_fixture/manifest.json" \
  --source-link-score "$pass_fixture/source_link_score.json" \
  --summary "$pass_fixture" \
  --output-dir "$out_dir/missing_field_score" >> "$log" 2>&1
missing_field_rc=$?
set -e

python3 - "$out_dir" "$missing_field_rc" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
missing_field_rc = int(sys.argv[2])
passed = json.loads((out_dir / "pass_score" / "score.json").read_text())
missing = json.loads((out_dir / "missing_field_score" / "score.json").read_text())
checks = {
    "pass_score_keep": passed["status"] == "KEEP_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_PASS",
    "pass_required_field_coverage_100": passed["exemplar_metrics"]["required_field_coverage"] == 1.0,
    "pass_source_sequence_coverage_100": passed["exemplar_metrics"]["source_sequence_coverage"] == 1.0,
    "pass_source_link_scorer_ok": passed["checks"]["source_link_scorer_ok"] is True,
    "pass_metric_budgets_ok": passed["checks"]["metric_budgets_ok"] is True,
    "pass_no_order_safety_ok": passed["checks"]["no_order_safety_ok"] is True,
    "pass_group_rankings_present": bool(passed["rankings"]["top_source_side_offset_risk_direction_groups"]),
    "promotion_gate_false": passed["promotion_gate"]["passed"] is False,
    "missing_field_rejected": missing_field_rc != 0 and missing["status"] == "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_FIELD_OR_PARITY_GAP",
    "missing_field_named": "source_sequence_id" in missing["field_failures"][0].get("missing_required_fields", [])
    or any("source_sequence_id" in str(item) for item in missing.get("field_failures", [])),
    "no_side_effects": passed["side_effects"]["orders_sent"] is False
    and passed["side_effects"]["ssh_started"] is False
    and passed["side_effects"]["events_jsonl_read"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"residual-tail exemplar scorer smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_residual_tail_exemplar_shadow_scorer_smoke",
    "status": "PASS",
    "decision_label": "KEEP_RESIDUAL_TAIL_EXEMPLAR_SHADOW_SCORER_READY",
    "hypothesis": "A local no-network scorer can evaluate residual-tail exemplar shadow pullbacks for safety, source-link parity, risk budgets, required field coverage, source_sequence coverage, concentration, and promotion-gate separation.",
    "inputs": {
        "pass_fixture": str(out_dir / "fixtures" / "pass_shadow"),
    },
    "outputs": {
        "pass_score": str(out_dir / "pass_score" / "score.json"),
        "missing_field_score": str(out_dir / "missing_field_score" / "score.json"),
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_RESIDUAL_TAIL_EXEMPLAR_SHADOW_SCORER_READY",
        "interpretation": "The scorer is ready for future allowlisted no-order shadow pullbacks. It does not start runs and does not prove private truth."
    },
    "promotion_gate": {
        "passed": False,
        "status": "SCORER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
        "deployable": False,
        "private_truth_ready": False,
        "g2_canary_ready": False
    },
    "side_effects": {
        "network_started": False,
        "ssh_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "service_control_used": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False
    },
    "next_action": "If the exact residual-tail exemplar no-order shadow is later approved and completed, pull back only allowlisted summaries/manifests and run this scorer before proposing a new local mechanism."
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ residual-tail exemplar shadow scorer smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
