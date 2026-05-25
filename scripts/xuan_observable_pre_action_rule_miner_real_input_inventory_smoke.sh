#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_real_input_inventory_smoke_$ts}"
case_dir="${out_dir/_smoke_/_real_like_}"
mkdir -p "$out_dir" "$case_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile \
  scripts/xuan_observable_pre_action_rule_miner_real_input_inventory.py \
  scripts/xuan_observable_pre_action_rule_miner_spec.py >> "$log" 2>&1

python3 - "$case_dir" "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

case_dir = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
case_dir.mkdir(parents=True, exist_ok=True)

pre_action = [
    "condition_id",
    "day_id",
    "quote_ts_ms",
    "side",
    "offset_s",
    "status_before_action",
    "block_reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "open_qty_bucket",
    "deficit_bucket",
    "candidate_qty_bucket",
    "source_risk_direction",
]
labels = [
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
]
no_order_fields = [
    "frozen_rule_id",
    "same_window_denominator_count",
    "admitted_count",
    "blocked_count",
    "source_sequence_coverage",
    "quote_intent_presence_rate",
    "source_order_presence_rate",
    "aggregate_parity",
]


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows))


rows_path = case_dir / "observable_pre_action_candidate_rows.jsonl"
source_path = case_dir / "observable_pre_action_source_link_summary.json"
denom_path = case_dir / "observable_pre_action_no_order_denominator_summary.json"
input_manifest = case_dir / "observable_pre_action_rule_miner_input_manifest.json"
bad_fixture_manifest = out_dir / "fixture_input_manifest.json"
bad_missing_denom_manifest = out_dir / "missing_denom_input_manifest.json"

write_jsonl(
    rows_path,
    [
        {
            "schema_version": "observable_pre_action_candidate_row_v1",
            "condition_id": "condition-real-like",
            "day_id": "2026-05-01",
            "quote_ts_ms": 1900000000000,
            "side": "YES",
            "offset_s": 24.0,
            "status_before_action": "admitted",
            "block_reason": None,
            "source_sequence_present": True,
            "quote_intent_present": True,
            "source_order_present": True,
            "pre_seed_same_qty": 0.0,
            "pre_seed_opp_qty": 5.0,
            "pre_seed_same_cost": 0.0,
            "pre_seed_opp_cost": 1.75,
            "open_qty_bucket": "open_qty_eq_5",
            "deficit_bucket": "deficit_eq_5",
            "candidate_qty_bucket": "candidate_qty_eq_5",
            "source_risk_direction": "repair_or_pairing_improving",
            "source_pair_qty": 2.0,
            "source_pair_cost": 1.8,
            "source_pair_pnl": 0.2,
            "source_residual_qty": 0.0,
            "source_residual_cost": 0.0,
            "pair_outcome_bucket": "pair_positive",
            "residual_tail_outcome_bucket": "residual_none",
        }
    ],
)
write_json(
    source_path,
    {
        "schema_version": "observable_pre_action_source_link_summary_v1",
        "row_count": 1200,
        "source_sequence_presence_by_status": {"admitted": {"present": 900}, "blocked": {"present": 300}},
        "quote_intent_presence_by_status": {"admitted": {"present": 600}, "blocked": {"missing": 600}},
        "source_order_presence_by_status": {"admitted": {"present": 600}, "blocked": {"missing": 600}},
    },
)
write_json(
    denom_path,
    {
        "schema_version": "observable_pre_action_no_order_denominator_summary_v1",
        "frozen_rule_id": "real_like_pre_action_rule_v1",
        "same_window_denominator_count": 1200,
        "rule_match_count": 240,
        "admitted_count": 600,
        "blocked_count": 600,
        "source_sequence_coverage": 0.99,
        "quote_intent_presence_rate": 0.5,
        "source_order_presence_rate": 0.5,
        "aggregate_parity": True,
        "field_contract": {
            "fixture_only": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scan": False,
            "private_truth_ready": False,
            "deployable": False,
        },
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)

base_manifest = {
    "artifact": "observable_pre_action_rule_miner_input_real_like",
    "schema_version": "observable_pre_action_rule_miner_input_v1",
    "created_utc": "20260525T205536Z",
    "fixture": False,
    "strategy_evidence": False,
    "scope": {
        "current_worktree_only": True,
        "local_only": True,
        "new_data_fetched": False,
        "external_worktree_read": False,
        "ssh_used": False,
        "shadow_started": False,
        "canary_or_live_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_full_store_scanned": False,
        "shared_ingress_or_broker_or_live_modified": False,
        "shared_ws_or_local_agg_or_service_started": False,
        "orders_cancels_redeems_sent": False,
        "trading_behavior_changed": False,
    },
    "materialized_sources": {
        "candidate_rows": {"path": str(rows_path), "row_count": 1200, "fields": pre_action + labels},
        "source_link_summary": {
            "path": str(source_path),
            "row_count": 1,
            "fields": [
                "row_count",
                "source_sequence_presence_by_status",
                "quote_intent_presence_by_status",
                "source_order_presence_by_status",
            ],
        },
        "no_order_denominator_summary": {"path": str(denom_path), "row_count": 1, "fields": no_order_fields},
    },
    "train_holdout_split": {
        "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
        "holdout_days": ["2026-05-04", "2026-05-05"],
    },
    "feature_policy": {"allowed_live_rule_fields": pre_action + ["offset_bucket"], "offline_label_fields": labels},
    "frozen_rule_contract": {
        "frozen_rule_id": "real_like_pre_action_rule_v1",
        "predicate_fields": ["source_sequence_present", "source_risk_direction"],
        "uses_only_pre_action_fields": True,
        "uses_realized_pair_cost": False,
        "uses_future_labels": False,
        "train_holdout_gate_passed": True,
        "train_selected_rows": 240,
        "holdout_selected_rows": 120,
    },
    "no_order_denominator_gate": {
        "same_window_denominator_reproduced": True,
        "same_window_denominator_count": 1200,
        "source_sequence_coverage": 0.99,
        "aggregate_parity": True,
    },
    "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False, "g2_canary_ready": False},
}
write_json(input_manifest, base_manifest)
fixture_manifest = dict(base_manifest)
fixture_manifest["fixture"] = True
write_json(bad_fixture_manifest, fixture_manifest)
missing_denom = json.loads(json.dumps(base_manifest))
missing_denom["materialized_sources"]["no_order_denominator_summary"]["path"] = str(out_dir / "absent_denom.json")
write_json(bad_missing_denom_manifest, missing_denom)
PY

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory.py \
  --candidate-input-manifest "$case_dir/observable_pre_action_rule_miner_input_manifest.json" \
  --only-explicit-input-manifests \
  --output-dir "$out_dir/good" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory.py \
  --candidate-input-manifest "$out_dir/fixture_input_manifest.json" \
  --only-explicit-input-manifests \
  --output-dir "$out_dir/fixture" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory.py \
  --candidate-input-manifest "$out_dir/missing_denom_input_manifest.json" \
  --only-explicit-input-manifests \
  --output-dir "$out_dir/missing_denom" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory.py \
  --output-dir "$out_dir/default_current_worktree" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_spec.py \
  --candidate-input-manifest "$case_dir/observable_pre_action_rule_miner_input_manifest.json" \
  --output-dir "$out_dir/good_spec_validation" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])


def load(path: str) -> dict:
    return json.loads((out_dir / path / "manifest.json").read_text())


good = load("good")
fixture = load("fixture")
missing_denom = load("missing_denom")
default = load("default_current_worktree")
spec_good = load("good_spec_validation")

assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_READY"
assert good["readiness"]["ready"] is True
assert good["readiness"]["validation_commands"]
assert spec_good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_CONTRACT_READY_NOT_STRATEGY_EVIDENCE"
assert fixture["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_GAPS"
assert "non_fixture_observable_pre_action_input_manifest_absent" in fixture["readiness"]["blockers"]
assert missing_denom["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_GAPS"
assert "non_fixture_observable_pre_action_input_manifest_absent" in missing_denom["readiness"]["blockers"]
assert default["decision_label"] in {
    "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_GAPS",
    "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_READY",
}
for manifest in (good, fixture, missing_denom, default):
    assert manifest["strategy_evidence"] is False
    assert manifest["no_order_diagnostic_allowed"] is False
    assert manifest["promotion_gate"]["passed"] is False
    assert manifest["safety"]["events_jsonl_read"] is False
    assert manifest["safety"]["raw_replay_or_full_store_scan"] is False

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_real_input_inventory_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_SMOKE_PASS",
    "checks": {
        "real_like_non_fixture_input_keep": True,
        "real_like_input_passes_miner_spec": True,
        "fixture_input_fails_closed": True,
        "missing_denominator_fails_closed": True,
        "default_current_worktree_runs_without_events_jsonl": True,
        "promotion_gate_separated": True,
    },
    "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    "safety": {
        "ssh_used": False,
        "shadow_started": False,
        "events_jsonl_read": False,
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scan": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/fixture/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/missing_denom/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/default_current_worktree/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good_spec_validation/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
