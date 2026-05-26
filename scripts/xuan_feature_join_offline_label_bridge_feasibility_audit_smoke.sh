#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-"$ROOT/xuan_research_artifacts/xuan_feature_join_offline_label_bridge_feasibility_audit_smoke_$(date -u +%Y%m%dT%H%M%SZ)"}"
mkdir -p "$OUT_DIR"

export OUT_DIR
python3 - <<'PY'
import csv
import json
import os
from pathlib import Path

out = Path(os.environ["OUT_DIR"])

feature_base = {
    "schema_version": "observable_pre_action_candidate_row_v1",
    "condition_id": "condition-a",
    "market_slug": "btc-updown-5m-2000000000",
    "day_id": "2026-05-26",
    "quote_ts_ms": 2000000062000,
    "side": "YES",
    "offset_s": 62.0,
    "offset_bucket": "offset_60_90",
    "status_before_action": "admitted",
    "reason": "candidate",
    "block_reason": "",
    "source_sequence_present": True,
    "quote_intent_present": False,
    "source_order_present": False,
    "pre_seed_same_qty": 0.0,
    "pre_seed_opp_qty": 0.0,
    "pre_seed_same_cost": 0.0,
    "pre_seed_opp_cost": 0.0,
    "pre_seed_open_qty": 0.0,
    "pre_seed_open_cost": 0.0,
    "candidate_qty": 5.0,
    "source_risk_direction": "risk_increasing",
    "post_action_outcome_labels_included": False,
    "realized_pair_cost_used_as_live_criteria": False,
    "trading_behavior_changed": False,
}

feature_exact = dict(feature_base, source_seed_candidate_row_id="candidate-1")
feature_composite = dict(
    feature_base,
    condition_id="condition-b",
    market_slug="btc-updown-5m-2000000300",
    quote_ts_ms=2000000362000,
    offset_s=62.1,
    side="NO",
    source_risk_direction="risk_decreasing",
)
feature_rows = [feature_exact, feature_composite]

feature_rows_path = out / "feature_rows.jsonl"
feature_rows_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in feature_rows))

feature_bad = [dict(feature_exact, realized_pair_cost_used_as_live_criteria=True)]
(out / "feature_rows_bad_live.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in feature_bad))

feature_no_overlap = [dict(feature_composite, condition_id="condition-z", market_slug="btc-updown-5m-2000000600", day_id="2026-05-27")]
(out / "feature_rows_no_overlap.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in feature_no_overlap))

feature_scorer = {"decision_label": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY"}
(out / "feature_scorer_manifest.json").write_text(json.dumps(feature_scorer, indent=2, sort_keys=True) + "\n")
bridge_contract = {"decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_READY"}
(out / "bridge_contract_manifest.json").write_text(json.dumps(bridge_contract, indent=2, sort_keys=True) + "\n")

fields = [
    "schema_version",
    "day",
    "condition_id",
    "slug",
    "source_seed_action_id",
    "source_seed_candidate_row_id",
    "ts_ms",
    "trigger_ts_ms",
    "side",
    "offset_s",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "seed_qty",
    "trigger_size",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "outcome_labels_are_post_action",
    "pre_action_field_policy",
    "post_action_label_policy",
    "live_rule_safety_policy",
]

def label_for(feature, exact_id=""):
    return {
        "schema_version": "candidate_seed_outcome_separator_v1",
        "day": feature["day_id"],
        "condition_id": feature["condition_id"],
        "slug": feature["market_slug"],
        "source_seed_action_id": "",
        "source_seed_candidate_row_id": exact_id,
        "ts_ms": str(feature["quote_ts_ms"]),
        "trigger_ts_ms": str(feature["quote_ts_ms"]),
        "side": feature["side"],
        "offset_s": str(feature["offset_s"]),
        "source_risk_direction": feature["source_risk_direction"],
        "pre_seed_same_qty": str(feature["pre_seed_same_qty"]),
        "pre_seed_opp_qty": str(feature["pre_seed_opp_qty"]),
        "pre_seed_same_cost": str(feature["pre_seed_same_cost"]),
        "pre_seed_opp_cost": str(feature["pre_seed_opp_cost"]),
        "pre_seed_open_qty": str(feature["pre_seed_open_qty"]),
        "pre_seed_open_cost": str(feature["pre_seed_open_cost"]),
        "seed_qty": str(feature["candidate_qty"]),
        "trigger_size": str(feature["candidate_qty"]),
        "source_pair_qty": "5",
        "source_pair_cost": "2.5",
        "source_pair_pnl": "0.5",
        "source_residual_qty": "0",
        "source_residual_cost": "0",
        "source_residual_age_s": "0",
        "pair_outcome_bucket": "pair_positive",
        "residual_tail_outcome_bucket": "no_tail",
        "outcome_labels_are_post_action": "True",
        "pre_action_field_policy": "pre-action fields only",
        "post_action_label_policy": "labels train holdout only",
        "live_rule_safety_policy": "offline labels are forbidden in live predicates",
    }

labels = [label_for(feature_exact, "candidate-1"), label_for(feature_composite)]
with (out / "labels.csv").open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()
    writer.writerows(labels)

bad_labels = [dict(labels[0], live_rule_safety_policy="allowed in live")]
with (out / "bad_labels.csv").open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()
    writer.writerows(bad_labels)
PY

python3 "$ROOT/scripts/xuan_feature_join_offline_label_bridge_feasibility_audit.py" \
  --feature-rows "$OUT_DIR/feature_rows.jsonl" \
  --feature-scorer-manifest "$OUT_DIR/feature_scorer_manifest.json" \
  --offline-label-csv "$OUT_DIR/labels.csv" \
  --bridge-contract-manifest "$OUT_DIR/bridge_contract_manifest.json" \
  --min-joined-rows 2 \
  --min-join-coverage 1.0 \
  --created-utc "2026-05-26T00:00:00Z" \
  --output-dir "$OUT_DIR/good" > "$OUT_DIR/good.log"

python3 "$ROOT/scripts/xuan_feature_join_offline_label_bridge_feasibility_audit.py" \
  --feature-rows "$OUT_DIR/feature_rows_no_overlap.jsonl" \
  --feature-scorer-manifest "$OUT_DIR/feature_scorer_manifest.json" \
  --offline-label-csv "$OUT_DIR/labels.csv" \
  --bridge-contract-manifest "$OUT_DIR/bridge_contract_manifest.json" \
  --min-joined-rows 1 \
  --min-join-coverage 1.0 \
  --created-utc "2026-05-26T00:00:00Z" \
  --output-dir "$OUT_DIR/no_overlap" > "$OUT_DIR/no_overlap.log"

python3 "$ROOT/scripts/xuan_feature_join_offline_label_bridge_feasibility_audit.py" \
  --feature-rows "$OUT_DIR/feature_rows_bad_live.jsonl" \
  --feature-scorer-manifest "$OUT_DIR/feature_scorer_manifest.json" \
  --offline-label-csv "$OUT_DIR/labels.csv" \
  --bridge-contract-manifest "$OUT_DIR/bridge_contract_manifest.json" \
  --min-joined-rows 1 \
  --min-join-coverage 1.0 \
  --created-utc "2026-05-26T00:00:00Z" \
  --output-dir "$OUT_DIR/bad_live" > "$OUT_DIR/bad_live.log"

python3 "$ROOT/scripts/xuan_feature_join_offline_label_bridge_feasibility_audit.py" \
  --feature-rows "$OUT_DIR/feature_rows.jsonl" \
  --feature-scorer-manifest "$OUT_DIR/feature_scorer_manifest.json" \
  --offline-label-csv "$OUT_DIR/bad_labels.csv" \
  --bridge-contract-manifest "$OUT_DIR/bridge_contract_manifest.json" \
  --min-joined-rows 1 \
  --min-join-coverage 1.0 \
  --created-utc "2026-05-26T00:00:00Z" \
  --output-dir "$OUT_DIR/bad_label_policy" > "$OUT_DIR/bad_label_policy.log"

export OUT_DIR
python3 - <<'PY'
import json
import os
from pathlib import Path

out = Path(os.environ["OUT_DIR"])
checks = {
    "good": "KEEP_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_FEASIBLE_VALIDATION_TARGET_READY",
    "no_overlap": "UNKNOWN_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_INPUT_CONTRACT_GAPS",
    "bad_live": "DISCARD_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_FORBIDDEN_LIVE_OR_PROVENANCE_VIOLATION",
    "bad_label_policy": "DISCARD_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_FORBIDDEN_LIVE_OR_PROVENANCE_VIOLATION",
}
results = {}
for name, expected in checks.items():
    manifest = json.loads((out / name / "manifest.json").read_text())
    actual = manifest["decision_label"]
    if actual != expected:
        raise SystemExit(f"{name}: expected {expected}, got {actual}")
    results[name] = actual
summary = {
    "artifact": "xuan_feature_join_offline_label_bridge_feasibility_audit_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_FEASIBILITY_AUDIT_SMOKE_READY",
    "results": results,
    "strategy_evidence": False,
    "no_order_diagnostic_allowed": False,
    "private_truth_ready": False,
    "deployable": False,
    "promotion_gate": {"passed": False},
}
(out / "manifest.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
PY

echo "smoke ok: $OUT_DIR"
