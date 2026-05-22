#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_residual_tail_ledger_context_verifier_smoke_${TS}}"
LOG="$OUT_DIR/smoke.log"
mkdir -p "$OUT_DIR/fixtures" "$OUT_DIR/keep" "$OUT_DIR/unknown"
: > "$LOG"

SCRIPT="scripts/xuan_b27_dplus_residual_tail_ledger_context_verifier.py"
python3 -m py_compile "$SCRIPT" >> "$LOG" 2>&1

python3 - "$OUT_DIR" <<'PY' >> "$LOG" 2>&1
import csv
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixtures = out_dir / "fixtures"
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
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "quote_intent_id",
    "source_order_id",
    "source_sequence_id",
    "external_shadow_ids_available",
    "external_shadow_id_policy",
    "selected_context_reason",
]
selected_rows = [
    {
        "schema_version": "selected_seed_ledger_context_export_v1",
        "profile_name": "control_seed_offset_max_120",
        "validation_request_id": "residual_tail_ledger_context_verifier_v1:control",
        "day": "2026-05-18",
        "condition_id": "fixture-condition-1",
        "slug": "fixture-market",
        "source_seed_action_id": "seed-yes-1",
        "source_seed_candidate_row_id": "seed-yes-1",
        "source_label": "fixture",
        "ts_ms": "1779062400000",
        "ts_iso": "2026-05-18T00:00:00Z",
        "side": "YES",
        "opposite_side": "NO",
        "offset_s": "72.0",
        "trigger_px": "0.41",
        "trigger_size": "20.0",
        "trigger_ts_ms": "1779062400000",
        "seed_px": "0.355",
        "seed_qty": "5.0",
        "seed_cost": "1.775",
        "official_fee_rate": "0.07",
        "official_fee": "0.08014999999999999",
        "source_risk_direction": "risk_increasing",
        "pre_seed_same_qty": "4.0",
        "pre_seed_opp_qty": "1.0",
        "pre_seed_same_cost": "1.64",
        "pre_seed_opp_cost": "0.38",
        "ledger_proxy_before": "-1.26",
        "ledger_proxy_after": "-1.86",
        "source_pair_qty": "8.0",
        "source_pair_cost": "7.72",
        "source_pair_pnl": "0.28",
        "source_residual_qty": "2.0",
        "source_residual_cost": "0.82",
        "source_residual_age_s": "180.0",
        "quote_intent_id": "",
        "source_order_id": "",
        "source_sequence_id": "",
        "external_shadow_ids_available": "False",
        "external_shadow_id_policy": "candidate_base/state_machine export has no quote_intent_id, source_order_id, or source_sequence_id; future shadow exemplar joins must align by candidate_row_id/ts/condition/side and must not fabricate external ids.",
        "selected_context_reason": "selected_seed_ledger_context_export_default_off",
    },
    {
        "schema_version": "selected_seed_ledger_context_export_v1",
        "profile_name": "control_seed_offset_max_120",
        "validation_request_id": "residual_tail_ledger_context_verifier_v1:control",
        "day": "2026-05-18",
        "condition_id": "fixture-condition-1",
        "slug": "fixture-market",
        "source_seed_action_id": "seed-no-1",
        "source_seed_candidate_row_id": "seed-no-1",
        "source_label": "fixture",
        "ts_ms": "1779062410000",
        "ts_iso": "2026-05-18T00:00:10Z",
        "side": "NO",
        "opposite_side": "YES",
        "offset_s": "46.0",
        "trigger_px": "0.39",
        "trigger_size": "18.0",
        "trigger_ts_ms": "1779062410000",
        "seed_px": "0.335",
        "seed_qty": "4.5",
        "seed_cost": "1.5075",
        "official_fee_rate": "0.07",
        "official_fee": "0.069165",
        "source_risk_direction": "repair_or_pairing_improving",
        "pre_seed_same_qty": "0.5",
        "pre_seed_opp_qty": "3.0",
        "pre_seed_same_cost": "0.2",
        "pre_seed_opp_cost": "1.17",
        "ledger_proxy_before": "0.97",
        "ledger_proxy_after": "0.52",
        "source_pair_qty": "6.0",
        "source_pair_cost": "5.66",
        "source_pair_pnl": "0.34",
        "source_residual_qty": "1.0",
        "source_residual_cost": "0.39",
        "source_residual_age_s": "90.0",
        "quote_intent_id": "",
        "source_order_id": "",
        "source_sequence_id": "",
        "external_shadow_ids_available": "False",
        "external_shadow_id_policy": "candidate_base/state_machine export has no quote_intent_id, source_order_id, or source_sequence_id; future shadow exemplar joins must align by candidate_row_id/ts/condition/side and must not fabricate external ids.",
        "selected_context_reason": "selected_seed_ledger_context_export_default_off",
    },
]
with (fixtures / "selected_seed_ledger_contexts.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(selected_rows)

exemplars = [
    {
        "quote_intent_id": "fixture-market:quote:1",
        "source_order_id": "seed-yes-1",
        "source_sequence_id": "seq-1",
        "condition_id": "fixture-condition-1",
        "slug": "fixture-market",
        "side": "YES",
        "offset_s": 72.0,
        "source_risk_direction": "risk_increasing",
        "trigger_px": 0.41,
        "trigger_size": 20.0,
        "trigger_ts_ms": 1779062400000,
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
        "seed_px": 0.355,
    },
    {
        "quote_intent_id": "fixture-market:quote:2",
        "source_order_id": "seed-no-1",
        "source_sequence_id": "seq-2",
        "condition_id": "fixture-condition-1",
        "slug": "fixture-market",
        "side": "NO",
        "offset_s": 46.0,
        "source_risk_direction": "repair_or_pairing_improving",
        "trigger_px": 0.39,
        "trigger_size": 18.0,
        "trigger_ts_ms": 1779062410000,
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
        "seed_px": 0.335,
    },
]
score = {
    "schema_version": 1,
    "artifact": "xuan_b27_dplus_residual_tail_exemplar_shadow_scorer",
    "status": "KEEP_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_PASS",
    "decision": "KEEP",
    "checks": {
        "paths_safe": True,
        "report_shape_ok": True,
        "aggregate_exemplars_subset_of_summaries": True,
        "required_field_coverage_ok": True,
        "source_sequence_coverage_ok": True,
        "metric_budgets_ok": True,
        "source_link_scorer_ok": True,
        "no_order_safety_ok": True,
    },
    "exemplar_metrics": {
        "aggregate_exemplar_row_count": 2,
        "required_field_coverage": 1.0,
        "source_sequence_coverage": 1.0,
        "top1_residual_cost_share": 0.677686,
    },
    "rankings": {
        "top_residual_tail_exemplars": exemplars,
        "top_source_side_offset_risk_direction_groups": [
            {
                "source_side_offset_risk_direction": "YES|offset_60_90|risk_increasing",
                "source_residual_cost": 0.82,
                "source_residual_cost_share": 0.677686,
                "source_pair_qty": 8.0,
                "source_pair_qty_share": 0.571429,
            }
        ],
    },
    "promotion_gate": {
        "passed": False,
        "deployable": False,
        "private_truth_ready": False,
        "g2_canary_ready": False,
    },
    "side_effects": {
        "ssh_started": False,
        "events_jsonl_read": False,
        "orders_sent": False,
    },
}
(fixtures / "exemplar_score_keep.json").write_text(json.dumps(score, indent=2, sort_keys=True) + "\n")

unknown = json.loads(json.dumps(score))
unknown["rankings"]["top_residual_tail_exemplars"][0]["source_order_id"] = "unmatched-seed"
unknown["rankings"]["top_residual_tail_exemplars"][0]["condition_id"] = "unknown-condition"
(fixtures / "exemplar_score_unmatched.json").write_text(json.dumps(unknown, indent=2, sort_keys=True) + "\n")
PY

python3 "$SCRIPT" \
  --selected-seed-ledger-contexts "$OUT_DIR/fixtures/selected_seed_ledger_contexts.csv" \
  --exemplar-score "$OUT_DIR/fixtures/exemplar_score_keep.json" \
  --output-dir "$OUT_DIR/keep" >> "$LOG" 2>&1

set +e
python3 "$SCRIPT" \
  --selected-seed-ledger-contexts "$OUT_DIR/fixtures/selected_seed_ledger_contexts.csv" \
  --exemplar-score "$OUT_DIR/fixtures/exemplar_score_unmatched.json" \
  --output-dir "$OUT_DIR/unknown" >> "$LOG" 2>&1
unknown_rc=$?
set -e

python3 - "$OUT_DIR" "$unknown_rc" <<'PY' >> "$LOG" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
unknown_rc = int(sys.argv[2])
keep = json.loads((out_dir / "keep" / "score.json").read_text())
unknown = json.loads((out_dir / "unknown" / "score.json").read_text())
checks = {
    "keep_decision": keep["decision_label"] == "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_READY",
    "keep_join_coverage_100": keep["join_metrics"]["join_coverage"] == 1.0,
    "keep_id_join_mode": keep["join_metrics"]["join_mode_counts"].get("id") == 2,
    "keep_required_selected_coverage_100": keep["selected_export_metrics"]["required_field_coverage"] == 1.0,
    "keep_context_delta_zero": keep["join_metrics"]["max_context_abs_delta"] == 0.0,
    "keep_external_ids_unavailable": keep["checks"]["local_external_shadow_ids_unavailable_and_blank"] is True,
    "keep_promotion_false": keep["promotion_gate"]["passed"] is False,
    "unknown_rejected": unknown_rc != 0 and unknown["decision_label"] == "UNKNOWN_RESIDUAL_TAIL_LEDGER_CONTEXT_JOIN_OR_FIELD_GAP",
    "unknown_names_join_gap": "exemplar_to_selected_seed_join_coverage_below_threshold" in unknown["blockers"],
    "unknown_unmatched_examples_named": bool(unknown["join_metrics"]["unmatched_examples"]),
    "no_side_effects": keep["side_effects"]["ssh_started"] is False
    and keep["side_effects"]["events_jsonl_read"] is False
    and keep["side_effects"]["orders_sent"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"residual-tail ledger-context verifier smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_residual_tail_ledger_context_verifier_smoke",
    "status": "PASS",
    "decision_label": "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_READY",
    "hypothesis": "A local verifier can join selected seed ledger-context exports to residual-tail exemplar scorer rows without fabricating external ids, and can name exact join gaps when they are missing.",
    "outputs": {
        "keep_score": str(out_dir / "keep" / "score.json"),
        "unknown_score": str(out_dir / "unknown" / "score.json"),
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_READY",
        "interpretation": "Verifier readiness only; not strategy economics, private truth, deployable, canary, or promotion evidence."
    },
    "promotion_gate": {
        "passed": False,
        "status": "VERIFIER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
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
    "next_action": "Run the verifier on a future approved residual-tail exemplar shadow pullback plus matching selected_seed_ledger_context_export_v1; if KEEP, design exactly one local sample-preserving ledger/context mechanism."
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

python3 -m json.tool "$OUT_DIR/manifest.json" >/dev/null
