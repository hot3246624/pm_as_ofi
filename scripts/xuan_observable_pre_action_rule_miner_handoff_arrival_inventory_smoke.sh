#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_handoff_arrival_inventory_smoke_$ts}"
case_dir="$ROOT/tmp_scripts/xuan_observable_pre_action_rule_miner_handoff_arrival_inventory_real_like_$ts"
mkdir -p "$out_dir" "$case_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile \
  scripts/xuan_observable_pre_action_rule_miner_handoff_arrival_inventory.py \
  scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py >> "$log" 2>&1

python3 - "$case_dir" "$out_dir" <<'PY' >> "$log" 2>&1
import csv
import json
import sys
from pathlib import Path

case_dir = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
case_dir.mkdir(parents=True, exist_ok=True)

feature_rows = case_dir / "observable_pre_action_candidate_rows.jsonl"
source_summary = case_dir / "observable_pre_action_source_link_summary.json"
feature_manifest = case_dir / "observable_pre_action_feature_join_manifest.json"
same_window = case_dir / "same_window_aggregate_summary.json"
offline_labels = case_dir / "offline_source_labels.csv"
bridge_rows = case_dir / "observable_pre_action_training_bridge_joined_rows.jsonl"
bridge_summary = case_dir / "observable_pre_action_training_bridge_summary.json"
frozen_rule = case_dir / "frozen_observable_pre_action_rule_manifest.json"
denom_summary = case_dir / "observable_pre_action_no_order_denominator_summary.json"
input_manifest = case_dir / "observable_pre_action_rule_miner_input_manifest.json"
good_handoff = case_dir / "safe_feature_join_pullback_handoff.json"


def write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


feature = []
bridge = []
labels = []
for idx in range(6):
    admitted = idx % 2 == 0
    same_qty = 0.0 if idx % 3 == 0 else 5.0
    opp_qty = 5.0 if same_qty == 0.0 else 0.0
    day = f"2026-05-0{1 + (idx % 5)}"
    common = {
        "condition_id": f"condition-{idx}",
        "market_slug": f"btc-updown-5m-{1900000000 + idx * 300}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + idx * 1000,
        "side": "YES" if admitted else "NO",
        "offset_s": 20.0 + idx,
        "offset_bucket": "offset_lt_120",
        "status_before_action": "admitted" if admitted else "blocked",
        "reason": "candidate" if admitted else "target",
        "block_reason": None if admitted else "target",
        "source_sequence_present": True,
        "quote_intent_present": admitted,
        "source_order_present": admitted,
        "pre_seed_same_qty": same_qty,
        "pre_seed_opp_qty": opp_qty,
        "pre_seed_same_cost": same_qty * 0.4,
        "pre_seed_opp_cost": opp_qty * 0.4,
        "pre_seed_open_qty": same_qty + opp_qty,
        "pre_seed_open_cost": (same_qty + opp_qty) * 0.4,
        "pre_seed_deficit_qty": max(0.0, opp_qty - same_qty),
        "candidate_qty": 5.0,
        "pre_seed_same_qty_bucket": "pre_seed_same_qty_zero" if same_qty == 0.0 else "pre_seed_same_qty_1_5",
        "pre_seed_opp_qty_bucket": "pre_seed_opp_qty_zero" if opp_qty == 0.0 else "pre_seed_opp_qty_1_5",
        "pre_seed_open_qty_bucket": "pre_seed_open_qty_1_5",
        "pre_seed_deficit_qty_bucket": "pre_seed_deficit_qty_1_5" if opp_qty > same_qty else "pre_seed_deficit_qty_zero",
        "candidate_qty_bucket": "candidate_qty_1_5",
        "source_risk_direction": "repair_or_pairing_improving" if admitted else "risk_increasing",
    }
    feature.append(
        {
            "schema_version": "observable_pre_action_candidate_row_v1",
            **common,
            "post_action_outcome_labels_included": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "trading_behavior_changed": False,
        }
    )
    bridge.append(
        {
            "schema_version": "observable_pre_action_training_bridge_joined_row_v1",
            "bridge_row_id": f"bridge-{idx}",
            "split": "train" if idx < 4 else "holdout",
            **common,
            "source_pair_qty": "2.0",
            "source_pair_cost": "1.75",
            "source_pair_pnl": "0.20",
            "source_residual_qty": "0.0",
            "source_residual_cost": "0.0",
            "source_residual_age_s": "0.0",
            "pair_outcome_bucket": "pair_positive",
            "residual_tail_outcome_bucket": "residual_none",
            "post_action_labels_allowed_for_train_holdout_scoring": True,
            "post_action_labels_allowed_in_live_predicate": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "future_labels_used_as_live_criteria": False,
            "trading_behavior_changed": False,
        }
    )
    labels.append(
        {
            "condition_id": common["condition_id"],
            "slug": common["market_slug"],
            "day": day,
            "ts_ms": common["quote_ts_ms"],
            "side": common["side"],
            "offset_s": common["offset_s"],
            "source_risk_direction": common["source_risk_direction"],
            "pre_seed_same_qty": same_qty,
            "pre_seed_opp_qty": opp_qty,
            "pre_seed_same_cost": same_qty * 0.4,
            "pre_seed_opp_cost": opp_qty * 0.4,
            "pre_seed_open_qty": same_qty + opp_qty,
            "pre_seed_open_cost": (same_qty + opp_qty) * 0.4,
            "seed_qty": 5.0,
            "source_pair_qty": "2.0",
            "source_pair_cost": "1.75",
            "source_pair_pnl": "0.20",
            "source_residual_qty": "0.0",
            "source_residual_cost": "0.0",
            "pair_outcome_bucket": "pair_positive",
            "residual_tail_outcome_bucket": "residual_none",
        }
    )

write_jsonl(feature_rows, feature)
write_jsonl(bridge_rows, bridge)
with offline_labels.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=list(labels[0]))
    writer.writeheader()
    writer.writerows(labels)

write_json(
    source_summary,
    {
        "schema_version": "observable_pre_action_source_link_summary_v1",
        "row_count": len(feature),
        "source_sequence_presence_by_status": {"admitted": {"present": 3}, "blocked": {"present": 3}},
    },
)
write_json(
    feature_manifest,
    {
        "schema_version": "observable_pre_action_feature_join_manifest_v1",
        "candidate_row_count": len(feature),
        "fixture": False,
        "field_contract": {"fixture_only": False, "reads_events_jsonl": False, "raw_replay_or_full_store_scan": False},
    },
)
write_json(same_window, {"schema_version": "same_window_aggregate_summary_v1", "candidate_row_count": len(feature)})
write_json(
    bridge_summary,
    {
        "schema_version": "observable_pre_action_training_bridge_summary_v1",
        "fixture_only": False,
        "joined_row_count": len(bridge),
        "split_counts": {"train": 4, "holdout": 2, "unused": 0},
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "trading_behavior_changed": False,
        "private_truth_ready": False,
        "deployable": False,
    },
)
write_json(
    frozen_rule,
    {
        "schema_version": "frozen_observable_pre_action_rule_manifest_v1",
        "frozen_rule_id": "arrival_smoke_rule_v1",
        "predicate": {"all": [{"field": "pre_seed_same_qty_bucket", "op": "eq", "value": "pre_seed_same_qty_zero"}]},
        "predicate_feature_names": ["pre_seed_same_qty_bucket"],
        "uses_only_pre_action_fields": True,
        "uses_realized_pair_cost": False,
        "uses_future_labels": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {"passed": False},
    },
)
write_json(
    denom_summary,
    {
        "schema_version": "observable_pre_action_no_order_denominator_summary_v1",
        "frozen_rule_id": "arrival_smoke_rule_v1",
        "same_window_denominator_count": 6,
        "rule_match_count": 2,
        "admitted_count": 3,
        "blocked_count": 3,
        "source_sequence_coverage": 1.0,
        "quote_intent_presence_rate": 0.5,
        "source_order_presence_rate": 0.5,
        "aggregate_parity": True,
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)
write_json(
    input_manifest,
    {
        "schema_version": "observable_pre_action_rule_miner_input_v1",
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
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)
base = {
    "schema_version": "observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_v1",
    "handoff_id": "arrival_smoke_handoff_v1",
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
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scanned": False,
        "shared_ingress_or_broker_or_live_modified": False,
        "shared_ws_or_local_agg_or_service_started": False,
        "orders_cancels_redeems_sent": False,
        "trading_behavior_changed": False,
    },
    "provenance": {
        "pullback_already_inside_current_worktree": True,
        "events_jsonl_read": False,
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scanned": False,
        "external_worktree_read": False,
        "ssh_used_by_validator": False,
        "orders_cancels_redeems_sent": False,
        "trading_behavior_changed": False,
    },
    "files": {
        "feature_join_candidate_rows": str(feature_rows),
        "feature_join_source_link_summary": str(source_summary),
        "feature_join_manifest": str(feature_manifest),
        "same_window_aggregate_summary": str(same_window),
        "offline_label_csv": str(offline_labels),
        "training_bridge_joined_rows": str(bridge_rows),
        "training_bridge_summary": str(bridge_summary),
        "frozen_rule_manifest": str(frozen_rule),
        "no_order_denominator_summary": str(denom_summary),
        "observable_pre_action_rule_miner_input_manifest": str(input_manifest),
    },
    "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
}
write_json(good_handoff, base)
fixture = json.loads(json.dumps(base))
fixture["fixture"] = True
write_json(out_dir / "fixture_handoff.json", fixture)
missing = json.loads(json.dumps(base))
missing["files"]["feature_join_candidate_rows"] = str(case_dir / "missing_candidate_rows.jsonl")
write_json(out_dir / "missing_handoff.json", missing)
PY

common_args=(
  --min-feature-join-rows 4
  --min-bridge-joined-rows 4
  --min-denominator 4
  --min-rule-matches 2
  --min-source-sequence-coverage 0.95
)

python3 scripts/xuan_observable_pre_action_rule_miner_handoff_arrival_inventory.py \
  "${common_args[@]}" \
  --output-dir "$out_dir/default" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_handoff_arrival_inventory.py \
  "${common_args[@]}" \
  --candidate-handoff-manifest "$case_dir/safe_feature_join_pullback_handoff.json" \
  --only-explicit-handoffs \
  --output-dir "$out_dir/good" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_handoff_arrival_inventory.py \
  "${common_args[@]}" \
  --candidate-handoff-manifest "$out_dir/fixture_handoff.json" \
  --only-explicit-handoffs \
  --output-dir "$out_dir/fixture" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_handoff_arrival_inventory.py \
  "${common_args[@]}" \
  --candidate-handoff-manifest "$out_dir/missing_handoff.json" \
  --only-explicit-handoffs \
  --output-dir "$out_dir/missing" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])


def load(name: str) -> dict:
    return json.loads((out_dir / name / "manifest.json").read_text())


default = load("default")
good = load("good")
fixture = load("fixture")
missing = load("missing")

assert default["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
assert "non_fixture_safe_handoff_manifest_absent" in default["readiness"]["blockers"]
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_READY"
assert good["readiness"]["ready"] is True
assert good["readiness"]["ready_validations"]
assert fixture["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
assert "non_fixture_safe_handoff_manifest_absent" in fixture["readiness"]["blockers"]
assert missing["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
assert "non_fixture_safe_handoff_manifest_absent" in missing["readiness"]["blockers"]
for manifest in (default, good, fixture, missing):
    assert manifest["strategy_evidence"] is False
    assert manifest["no_order_diagnostic_allowed"] is False
    assert manifest["private_truth_ready"] is False
    assert manifest["deployable"] is False
    assert manifest["promotion_gate"]["passed"] is False
    assert manifest["safety"]["events_jsonl_read"] is False
    assert manifest["safety"]["raw_replay_or_full_store_scan"] is False

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_handoff_arrival_inventory_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_SMOKE_PASS",
    "checks": {
        "default_no_real_handoff_unknown": True,
        "explicit_good_handoff_keep": True,
        "fixture_handoff_not_ready": True,
        "missing_handoff_not_ready": True,
        "promotion_gate_separated": True,
    },
    "strategy_evidence": False,
    "no_order_diagnostic_allowed": False,
    "private_truth_ready": False,
    "deployable": False,
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
python3 -m json.tool "$out_dir/default/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/fixture/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/missing/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
