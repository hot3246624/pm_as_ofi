#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_smoke_$ts}"
case_dir="$ROOT/tmp_specs/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_real_like_$ts"
mkdir -p "$out_dir" "$case_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py >> "$log" 2>&1

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
handoff = case_dir / "safe_feature_join_pullback_handoff.json"


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def bucket(value: float, prefix: str) -> str:
    if value <= 0:
        return f"{prefix}_zero"
    if value <= 5:
        return f"{prefix}_1_5"
    return f"{prefix}_gt_5"


days = ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"]
feature: list[dict[str, object]] = []
bridge: list[dict[str, object]] = []
label_rows: list[dict[str, object]] = []
for idx in range(120):
    day = days[idx % len(days)]
    split = "train" if day <= "2026-05-03" else "holdout"
    admitted = idx % 2 == 0
    same_qty = 0.0 if idx % 3 == 0 else 5.0
    opp_qty = 5.0 if same_qty == 0.0 else 0.0
    candidate_qty = 5.0
    common = {
        "condition_id": f"condition-{idx % 12}",
        "market_slug": f"btc-updown-5m-{1900000000 + idx * 300}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + idx * 1000,
        "side": "YES" if idx % 2 == 0 else "NO",
        "offset_s": float(20 + (idx % 80)),
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
            **common,
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
    label_rows.append(
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
            "seed_qty": candidate_qty,
            "source_pair_qty": "2.0",
            "source_pair_cost": "1.75",
            "source_pair_pnl": str(source_pair_pnl),
            "source_residual_qty": "0.0" if residual_cost == 0 else "1.0",
            "source_residual_cost": str(residual_cost),
            "pair_outcome_bucket": "pair_positive",
            "residual_tail_outcome_bucket": "residual_none" if residual_cost == 0 else "residual_small",
        }
    )

write_jsonl(feature_rows, feature)
write_jsonl(bridge_rows, bridge)
with offline_labels.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=list(label_rows[0]))
    writer.writeheader()
    writer.writerows(label_rows)

field_contract = {
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
}
write_json(
    source_summary,
    {
        "schema_version": "observable_pre_action_source_link_summary_v1",
        "row_count": len(feature),
        "field_contract": field_contract,
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
        "field_contract": field_contract,
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
    },
)
write_json(
    frozen_rule,
    {
        "schema_version": "frozen_observable_pre_action_rule_manifest_v1",
        "frozen_rule_id": "real_like_safe_handoff_rule_v1",
        "predicate": {"all": [{"field": "pre_seed_same_qty_bucket", "op": "eq", "value": "pre_seed_same_qty_zero"}]},
        "predicate_expression": 'pre_seed_same_qty_bucket == "pre_seed_same_qty_zero"',
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
    same_window,
    {
        "schema_version": "same_window_aggregate_summary_v1",
        "candidate_row_count": len(feature),
        "source_summary_row_count": len(feature),
        "events_jsonl_read": False,
        "raw_replay_or_full_store_scan": False,
    },
)
write_json(
    denom_summary,
    {
        "schema_version": "observable_pre_action_no_order_denominator_summary_v1",
        "frozen_rule_id": "real_like_safe_handoff_rule_v1",
        "same_window_denominator_count": 1200,
        "rule_match_count": 240,
        "admitted_count": 600,
        "blocked_count": 600,
        "source_sequence_coverage": 0.99,
        "quote_intent_presence_rate": 0.5,
        "source_order_presence_rate": 0.5,
        "aggregate_parity": True,
        "field_contract": {"fixture_only": False, "events_jsonl_read": False, "raw_replay_or_full_store_scan": False},
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)
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
        "materialized_sources": {
            "candidate_rows": {"path": str(bridge_rows), "row_count": 1200, "fields": pre_action_fields + label_fields},
            "source_link_summary": {"path": str(source_summary), "row_count": 1, "fields": ["row_count"]},
            "no_order_denominator_summary": {
                "path": str(denom_summary),
                "row_count": 1,
                "fields": [
                    "frozen_rule_id",
                    "same_window_denominator_count",
                    "admitted_count",
                    "blocked_count",
                    "source_sequence_coverage",
                    "quote_intent_presence_rate",
                    "source_order_presence_rate",
                    "aggregate_parity",
                ],
            },
        },
        "feature_policy": {"allowed_live_rule_fields": pre_action_fields + ["pre_seed_same_qty_bucket"], "offline_label_fields": label_fields},
        "frozen_rule_contract": {
            "frozen_rule_id": "real_like_safe_handoff_rule_v1",
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
        "train_holdout_split": {
            "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
            "holdout_days": ["2026-05-04", "2026-05-05"],
        },
        "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
    },
)
base_handoff = {
    "schema_version": "observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_v1",
    "handoff_id": "real_like_safe_handoff_v1",
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
write_json(handoff, base_handoff)
fixture_handoff = json.loads(json.dumps(base_handoff))
fixture_handoff["fixture"] = True
write_json(out_dir / "fixture_handoff.json", fixture_handoff)
missing_handoff = json.loads(json.dumps(base_handoff))
missing_handoff["files"]["feature_join_candidate_rows"] = str(case_dir / "missing_candidate_rows.jsonl")
write_json(out_dir / "missing_handoff.json", missing_handoff)
forbidden_rule = json.loads(frozen_rule.read_text())
forbidden_rule["predicate_feature_names"] = ["source_pair_qty"]
forbidden_rule["predicate"] = {"all": [{"field": "source_pair_qty", "op": "eq", "value": "2.0"}]}
forbidden_path = out_dir / "forbidden_frozen_rule.json"
write_json(forbidden_path, forbidden_rule)
forbidden_handoff = json.loads(json.dumps(base_handoff))
forbidden_handoff["files"]["frozen_rule_manifest"] = str(forbidden_path)
write_json(out_dir / "forbidden_handoff.json", forbidden_handoff)
PY

common_args=(
  --min-feature-join-rows 100
  --min-bridge-joined-rows 100
  --min-denominator 100
  --min-rule-matches 20
  --min-source-sequence-coverage 0.95
)

python3 scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py \
  "${common_args[@]}" \
  --output-dir "$out_dir/spec_default" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py \
  "${common_args[@]}" \
  --handoff-manifest "$case_dir/safe_feature_join_pullback_handoff.json" \
  --output-dir "$out_dir/good" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py \
  "${common_args[@]}" \
  --handoff-manifest "$out_dir/fixture_handoff.json" \
  --output-dir "$out_dir/fixture" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py \
  "${common_args[@]}" \
  --handoff-manifest "$out_dir/missing_handoff.json" \
  --output-dir "$out_dir/missing" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py \
  "${common_args[@]}" \
  --handoff-manifest "$out_dir/forbidden_handoff.json" \
  --output-dir "$out_dir/forbidden" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])


def load(name: str) -> dict:
    return json.loads((out_dir / name / "manifest.json").read_text())


spec = load("spec_default")
good = load("good")
fixture = load("fixture")
missing = load("missing")
forbidden = load("forbidden")

assert spec["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_SPEC_READY"
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_READY"
assert good["handoff_status"] == "HANDOFF_READY"
assert len(good["handoff_spec"]["validation_commands_template"]) == 6
assert fixture["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_FAIL_CLOSED"
assert "handoff_fixture_true" in fixture["blockers"]
assert missing["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_FAIL_CLOSED"
assert "feature_join_candidate_rows_missing_on_disk" in missing["blockers"]
assert forbidden["decision_label"] == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_FAIL_CLOSED"
assert "frozen_rule_forbidden_live_fields_present" in forbidden["blockers"]
for manifest in (spec, good, fixture, missing, forbidden):
    assert manifest["strategy_evidence"] is False
    assert manifest["no_order_diagnostic_allowed"] is False
    assert manifest["private_truth_ready"] is False
    assert manifest["deployable"] is False
    assert manifest["promotion_gate"]["passed"] is False
    assert manifest["safety"]["events_jsonl_read"] is False
    assert manifest["safety"]["raw_replay_or_full_store_scan"] is False

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_SMOKE_PASS",
    "checks": {
        "spec_default_keep": True,
        "complete_synthetic_handoff_keep": True,
        "fixture_handoff_fails_closed": True,
        "missing_handoff_fails_closed": True,
        "forbidden_handoff_fails_closed": True,
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
python3 -m json.tool "$out_dir/spec_default/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/fixture/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/missing/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/forbidden/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
