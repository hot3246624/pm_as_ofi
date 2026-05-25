#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2_smoke_$ts}"
case_dir="$ROOT/tmp_specs/xuan_observable_pre_action_rule_miner_real_input_inventory_v2_real_like_$ts"
mkdir -p "$out_dir" "$case_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile \
  scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py \
  scripts/xuan_observable_pre_action_rule_miner_spec.py >> "$log" 2>&1

python3 - "$case_dir" "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

case_dir = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
case_dir.mkdir(parents=True, exist_ok=True)

feature_rows = case_dir / "observable_pre_action_candidate_rows.jsonl"
source_summary = case_dir / "observable_pre_action_source_link_summary.json"
feature_manifest = case_dir / "observable_pre_action_feature_join_manifest.json"
bridge_rows = case_dir / "observable_pre_action_training_bridge_joined_rows.jsonl"
bridge_summary = case_dir / "observable_pre_action_training_bridge_summary.json"
denom_summary = case_dir / "observable_pre_action_no_order_denominator_summary.json"
frozen_rule = case_dir / "frozen_observable_pre_action_rule_manifest.json"
input_manifest = case_dir / "observable_pre_action_rule_miner_input_manifest.json"

pre_action_fields = [
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
label_fields = [
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
]
denom_fields = [
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def bucket(value: float, prefix: str) -> str:
    if value <= 0:
        return f"{prefix}_zero"
    if value <= 5:
        return f"{prefix}_1_5"
    return f"{prefix}_gt_5"


feature = []
bridge = []
days = ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"]
for idx in range(120):
    day = days[idx % len(days)]
    split = "train" if day <= "2026-05-03" else "holdout"
    admitted = idx % 2 == 0
    same_qty = 0.0 if idx % 3 == 0 else 5.0
    opp_qty = 5.0 if same_qty == 0.0 else 0.0
    candidate_qty = 5.0
    status = "admitted" if admitted else "blocked"
    reason = "candidate" if admitted else "target"
    common = {
        "condition_id": f"condition-{idx % 12}",
        "market_slug": f"btc-updown-5m-{1900000000 + idx * 300}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + idx * 1000,
        "side": "YES" if idx % 2 == 0 else "NO",
        "offset_s": float(20 + (idx % 80)),
        "offset_bucket": "offset_lt_120",
        "status_before_action": status,
        "reason": reason,
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
        "candidate_qty": candidate_qty,
        "pre_seed_same_qty_bucket": bucket(same_qty, "pre_seed_same_qty"),
        "pre_seed_opp_qty_bucket": bucket(opp_qty, "pre_seed_opp_qty"),
        "pre_seed_open_qty_bucket": bucket(same_qty + opp_qty, "pre_seed_open_qty"),
        "pre_seed_deficit_qty_bucket": bucket(max(0.0, opp_qty - same_qty), "pre_seed_deficit_qty"),
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
    source_pair_pnl = 0.25 if same_qty == 0.0 else 0.05
    residual_cost = 0.0 if same_qty == 0.0 else 0.08
    bridge.append(
        {
            "schema_version": "observable_pre_action_training_bridge_joined_row_v1",
            "bridge_row_id": f"bridge-{idx}",
            "split": split,
            "join_method": "exact_id:source_seed_candidate_row_id",
            "join_confidence": "exact",
            "feature_row_index": idx,
            "offline_label_row_index": idx,
            "feature_row_hash": f"feature-hash-{idx}",
            "offline_label_row_hash": f"label-hash-{idx}",
            **common,
            "open_qty_bucket": common["pre_seed_open_qty_bucket"],
            "deficit_bucket": common["pre_seed_deficit_qty_bucket"],
            "source_pair_qty": "2.0",
            "source_pair_cost": "1.75",
            "source_pair_pnl": str(source_pair_pnl),
            "source_residual_qty": "0.0" if residual_cost == 0 else "1.0",
            "source_residual_cost": str(residual_cost),
            "source_residual_age_s": "0.0",
            "pair_outcome_bucket": "pair_positive",
            "residual_tail_outcome_bucket": "residual_none" if residual_cost == 0 else "residual_small",
            "post_action_labels_allowed_for_train_holdout_scoring": True,
            "post_action_labels_allowed_in_live_predicate": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "future_labels_used_as_live_criteria": False,
            "trading_behavior_changed": False,
        }
    )

write_jsonl(feature_rows, feature)
write_jsonl(bridge_rows, bridge)
write_json(
    source_summary,
    {
        "schema_version": "observable_pre_action_source_link_summary_v1",
        "row_count": len(feature),
        "field_contract": {
            "default_off": True,
            "writes_events_jsonl": False,
            "reads_events_jsonl": False,
            "raw_replay_or_full_store_scan": False,
            "post_action_outcome_labels_included": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "trading_behavior_changed": False,
            "strategy_evidence": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
        "source_sequence_presence_by_status": {"admitted": {"present": 60}, "blocked": {"present": 60}},
        "quote_intent_presence_by_status": {"admitted": {"present": 60}, "blocked": {"missing": 60}},
        "source_order_presence_by_status": {"admitted": {"present": 60}, "blocked": {"missing": 60}},
    },
)
write_json(
    feature_manifest,
    {
        "schema_version": "observable_pre_action_feature_join_manifest_v1",
        "candidate_row_count": len(feature),
        "fixture": False,
        "field_contract": {
            "fixture_only": False,
            "default_off": True,
            "writes_events_jsonl": False,
            "reads_events_jsonl": False,
            "raw_replay_or_full_store_scan": False,
            "post_action_outcome_labels_included": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "trading_behavior_changed": False,
            "strategy_evidence": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
    },
)
write_json(
    bridge_summary,
    {
        "schema_version": "observable_pre_action_training_bridge_summary_v1",
        "fixture_only": False,
        "joined_row_count": len(bridge),
        "feature_row_count": len(feature),
        "offline_label_row_count": len(bridge),
        "label_row_reuse_count": 0,
        "split_counts": {"train": 72, "holdout": 48, "unused": 0},
        "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
        "holdout_days": ["2026-05-04", "2026-05-05"],
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "trading_behavior_changed": False,
        "strategy_evidence": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {"passed": False},
        "events_jsonl_read": False,
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scan": False,
    },
)
write_json(
    denom_summary,
    {
        "schema_version": "observable_pre_action_no_order_denominator_summary_v1",
        "frozen_rule_id": "real_like_observable_pre_action_rule_v2",
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
write_json(
    frozen_rule,
    {
        "schema_version": "frozen_observable_pre_action_rule_manifest_v1",
        "frozen_rule_id": "real_like_observable_pre_action_rule_v2",
        "rule_version": "real_like_v2",
        "predicate": {"all": [{"field": "pre_seed_same_qty_bucket", "op": "eq", "value": "pre_seed_same_qty_zero"}]},
        "predicate_expression": 'pre_seed_same_qty_bucket == "pre_seed_same_qty_zero"',
        "predicate_feature_names": ["pre_seed_same_qty_bucket"],
        "uses_only_pre_action_fields": True,
        "uses_realized_pair_cost": False,
        "uses_future_labels": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {"passed": False, "reason": "inventory_smoke_real_like_only"},
        "train_holdout_split": {
            "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
            "holdout_days": ["2026-05-04", "2026-05-05"],
        },
        "objective_metrics": {
            "selected_count": 240,
            "train_rows": 120,
            "holdout_rows": 80,
            "train_objective": 10.0,
            "holdout_objective": 4.0,
            "selected_share_from_top_day": 0.25,
        },
        "stability_gates": {
            "fixture_only": False,
            "train_objective_positive": True,
            "holdout_objective_positive": True,
            "single_day_concentration_ok": True,
            "static_side_offset_public_price_cap": False,
        },
        "same_window_scope": {
            "requires_denominator_replay": True,
            "same_window_denominator_count_min": 100,
            "rule_match_count_min": 20,
            "source_sequence_coverage_min": 0.95,
        },
        "train_label_fields": label_fields,
        "holdout_label_fields": label_fields,
        "allowed_live_feature_families": pre_action_fields,
        "forbidden_live_feature_families": ["source_pair", "source_residual", "realized_pair_cost"],
    },
)

base_manifest = {
    "artifact": "observable_pre_action_rule_miner_input_real_like_v2",
    "schema_version": "observable_pre_action_rule_miner_input_v1",
    "created_utc": "2026-05-25T23:35:07Z",
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
        "candidate_rows": {"path": str(bridge_rows), "row_count": 1200, "fields": pre_action_fields + label_fields},
        "source_link_summary": {
            "path": str(source_summary),
            "row_count": 1,
            "fields": [
                "row_count",
                "source_sequence_presence_by_status",
                "quote_intent_presence_by_status",
                "source_order_presence_by_status",
            ],
        },
        "no_order_denominator_summary": {"path": str(denom_summary), "row_count": 1, "fields": denom_fields},
    },
    "supporting_materialized_sources": {
        "feature_join_candidate_rows": {"path": str(feature_rows), "row_count": len(feature)},
        "feature_join_manifest": {"path": str(feature_manifest), "row_count": 1},
        "training_bridge_joined_rows": {"path": str(bridge_rows), "row_count": len(bridge)},
        "training_bridge_summary": {"path": str(bridge_summary), "row_count": 1},
        "frozen_rule_manifest": {"path": str(frozen_rule), "row_count": 1},
    },
    "train_holdout_split": {
        "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
        "holdout_days": ["2026-05-04", "2026-05-05"],
    },
    "feature_policy": {
        "allowed_live_rule_fields": pre_action_fields
        + [
            "pre_seed_same_qty_bucket",
            "pre_seed_opp_qty_bucket",
            "pre_seed_open_qty_bucket",
            "pre_seed_deficit_qty_bucket",
            "offset_bucket",
        ],
        "offline_label_fields": label_fields,
    },
    "frozen_rule_contract": {
        "frozen_rule_id": "real_like_observable_pre_action_rule_v2",
        "predicate_fields": ["pre_seed_same_qty_bucket"],
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

fixture_manifest = json.loads(json.dumps(base_manifest))
fixture_manifest["fixture"] = True
write_json(out_dir / "fixture_input_manifest.json", fixture_manifest)

missing_bridge = json.loads(json.dumps(base_manifest))
missing_bridge["supporting_materialized_sources"]["training_bridge_joined_rows"]["path"] = str(case_dir / "missing_bridge_rows.jsonl")
write_json(out_dir / "missing_bridge_input_manifest.json", missing_bridge)

forbidden_rule = json.loads(json.dumps(base_manifest))
forbidden_frozen = json.loads(frozen_rule.read_text())
forbidden_frozen["predicate_feature_names"] = ["source_pair_qty"]
forbidden_frozen["predicate"] = {"all": [{"field": "source_pair_qty", "op": "eq", "value": "2.0"}]}
forbidden_frozen["uses_future_labels"] = True
forbidden_frozen_path = out_dir / "forbidden_frozen_rule.json"
write_json(forbidden_frozen_path, forbidden_frozen)
forbidden_rule["supporting_materialized_sources"]["frozen_rule_manifest"]["path"] = str(forbidden_frozen_path)
forbidden_rule["frozen_rule_contract"]["predicate_fields"] = ["source_pair_qty"]
write_json(out_dir / "forbidden_rule_input_manifest.json", forbidden_rule)
PY

common_args=(
  --min-candidate-rows 100
  --min-feature-join-rows 100
  --min-bridge-joined-rows 100
  --min-train-rows 50
  --min-holdout-rows 30
  --min-train-selected-rows 50
  --min-holdout-selected-rows 20
  --min-no-order-denominator 100
)
explicit_component_args=(
  --feature-join-candidate-rows "$case_dir/observable_pre_action_candidate_rows.jsonl"
  --training-bridge-joined-rows "$case_dir/observable_pre_action_training_bridge_joined_rows.jsonl"
  --denominator-summary "$case_dir/observable_pre_action_no_order_denominator_summary.json"
  --frozen-rule-manifest "$case_dir/frozen_observable_pre_action_rule_manifest.json"
)

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py \
  "${common_args[@]}" \
  "${explicit_component_args[@]}" \
  --candidate-input-manifest "$case_dir/observable_pre_action_rule_miner_input_manifest.json" \
  --only-explicit-inputs \
  --output-dir "$out_dir/good" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py \
  "${common_args[@]}" \
  "${explicit_component_args[@]}" \
  --candidate-input-manifest "$out_dir/fixture_input_manifest.json" \
  --only-explicit-inputs \
  --output-dir "$out_dir/fixture" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py \
  "${common_args[@]}" \
  "${explicit_component_args[@]}" \
  --candidate-input-manifest "$out_dir/missing_bridge_input_manifest.json" \
  --only-explicit-inputs \
  --output-dir "$out_dir/missing_bridge" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py \
  "${common_args[@]}" \
  "${explicit_component_args[@]}" \
  --candidate-input-manifest "$out_dir/forbidden_rule_input_manifest.json" \
  --only-explicit-inputs \
  --output-dir "$out_dir/forbidden_rule" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py \
  --output-dir "$out_dir/default_current_worktree" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_spec.py \
  --candidate-input-manifest "$case_dir/observable_pre_action_rule_miner_input_manifest.json" \
  --min-candidate-rows 100 \
  --min-train-selected-rows 50 \
  --min-holdout-selected-rows 20 \
  --min-no-order-denominator 100 \
  --output-dir "$out_dir/good_spec_validation" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])


def load(name: str) -> dict:
    return json.loads((out_dir / name / "manifest.json").read_text())


good = load("good")
fixture = load("fixture")
missing_bridge = load("missing_bridge")
forbidden_rule = load("forbidden_rule")
default = load("default_current_worktree")
spec_good = load("good_spec_validation")

assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_READY"
assert good["readiness"]["ready"] is True
assert good["readiness"]["complete_component_chain_ready"] is True
assert good["readiness"]["validation_commands"]
assert spec_good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_CONTRACT_READY_NOT_STRATEGY_EVIDENCE"

assert fixture["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_GAPS"
assert "non_fixture_observable_pre_action_input_manifest_absent" in fixture["readiness"]["blockers"]
assert missing_bridge["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_GAPS"
assert "non_fixture_observable_pre_action_input_manifest_absent" in missing_bridge["readiness"]["blockers"]
assert forbidden_rule["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_GAPS"
assert "non_fixture_observable_pre_action_input_manifest_absent" in forbidden_rule["readiness"]["blockers"]

assert default["decision_label"] in {
    "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_GAPS",
    "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_READY",
}
for manifest in (good, fixture, missing_bridge, forbidden_rule, default):
    assert manifest["strategy_evidence"] is False
    assert manifest["no_order_diagnostic_allowed"] is False
    assert manifest["private_truth_ready"] is False
    assert manifest["deployable"] is False
    assert manifest["promotion_gate"]["passed"] is False
    assert manifest["safety"]["events_jsonl_read"] is False
    assert manifest["safety"]["raw_replay_or_full_store_scan"] is False

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_real_input_inventory_v2_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_SMOKE_PASS",
    "checks": {
        "real_like_complete_chain_keep": True,
        "real_like_input_passes_miner_spec": True,
        "fixture_input_fails_closed": True,
        "missing_bridge_fails_closed": True,
        "forbidden_rule_fails_closed": True,
        "default_current_worktree_runs_without_events_jsonl": True,
        "promotion_gate_separated": True,
    },
    "good_ready_counts": {
        "ready_input_manifests": len(good["readiness"]["ready_input_manifests"]),
        "ready_feature_join_outputs": len(good["readiness"]["ready_feature_join_outputs"]),
        "ready_training_bridge_outputs": len(good["readiness"]["ready_training_bridge_outputs"]),
        "ready_denominator_summaries": len(good["readiness"]["ready_denominator_summaries"]),
        "ready_frozen_rules": len(good["readiness"]["ready_frozen_rules"]),
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
python3 -m json.tool "$out_dir/good/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/fixture/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/missing_bridge/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/forbidden_rule/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/default_current_worktree/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good_spec_validation/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
