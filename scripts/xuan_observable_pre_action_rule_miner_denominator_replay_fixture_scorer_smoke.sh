#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile \
  scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer.py \
  scripts/xuan_observable_pre_action_rule_miner_spec.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_dir = out_dir / "fixtures"
fixture_dir.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def row(i: int, *, source_sequence_present: bool = True) -> dict[str, object]:
    admitted = i % 2 == 0
    repair = i % 3 == 0
    side = "NO" if repair else "YES"
    day = ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"][i % 5]
    offset_bucket = "offset_30_60" if i % 4 else "offset_0_30"
    same_qty = 0.0 if repair else 5.0
    opp_qty = 5.0 if repair else 0.0
    open_qty = same_qty + opp_qty
    deficit = max(0.0, opp_qty - same_qty)
    return {
        "schema_version": "observable_pre_action_candidate_row_v1",
        "condition_id": f"condition-{i % 26}",
        "market_slug": f"btc-updown-5m-{1900000000 + (i % 26) * 300}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + i * 1000,
        "side": side,
        "offset_s": 30.0 + (i % 60),
        "offset_bucket": offset_bucket,
        "status_before_action": "admitted" if admitted else "blocked",
        "reason": "candidate" if admitted else ("target" if i % 5 else "offset"),
        "block_reason": None if admitted else ("target" if i % 5 else "offset"),
        "source_sequence_present": source_sequence_present,
        "quote_intent_present": admitted,
        "source_order_present": admitted,
        "pre_seed_same_qty": same_qty,
        "pre_seed_opp_qty": opp_qty,
        "pre_seed_same_cost": same_qty * 0.40,
        "pre_seed_opp_cost": opp_qty * 0.40,
        "pre_seed_open_qty": open_qty,
        "pre_seed_open_cost": open_qty * 0.40,
        "pre_seed_deficit_qty": deficit,
        "candidate_qty": 5.0 if admitted else None,
        "pre_seed_same_qty_bucket": "same_qty_zero" if same_qty == 0 else "same_qty_eq_5",
        "pre_seed_opp_qty_bucket": "opp_qty_zero" if opp_qty == 0 else "opp_qty_eq_5",
        "pre_seed_open_qty_bucket": "open_qty_eq_5",
        "pre_seed_deficit_qty_bucket": "deficit_eq_5" if deficit else "deficit_zero",
        "open_qty_bucket": "open_qty_eq_5",
        "deficit_bucket": "deficit_eq_5" if deficit else "deficit_zero",
        "candidate_qty_bucket": "candidate_qty_eq_5" if admitted else "candidate_qty_unknown",
        "source_risk_direction": "repair_or_pairing_improving" if repair else "risk_increasing",
        "source_pair_qty": 2.0 if admitted and repair else 0.0,
        "source_pair_cost": 1.80 if admitted and repair else 0.0,
        "source_pair_pnl": 0.20 if admitted and repair else 0.0,
        "source_residual_qty": 0.0 if repair else 1.0,
        "source_residual_cost": 0.0 if repair else 0.40,
        "pair_outcome_bucket": "pair_positive" if repair else "pair_none",
        "residual_tail_outcome_bucket": "residual_none" if repair else "residual_small",
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }


def materialize_case(case: str, rows: list[dict[str, object]], *, aggregate_override=None, frozen_override=None) -> Path:
    case_dir = fixture_dir / case
    case_dir.mkdir(parents=True, exist_ok=True)
    rows_path = case_dir / "observable_pre_action_candidate_rows.jsonl"
    source_path = case_dir / "observable_pre_action_source_link_summary.json"
    feature_manifest_path = case_dir / "observable_pre_action_feature_join_manifest.json"
    frozen_path = case_dir / "frozen_observable_pre_action_rule_manifest.json"
    same_window_path = case_dir / "same_window_aggregate_summary.json"
    write_jsonl(rows_path, rows)
    admitted = sum(1 for item in rows if item["status_before_action"] == "admitted")
    blocked = sum(1 for item in rows if item["status_before_action"] == "blocked")
    source_sequence_present = sum(1 for item in rows if item["source_sequence_present"])
    quote_intent_present = sum(1 for item in rows if item["quote_intent_present"])
    source_order_present = sum(1 for item in rows if item["source_order_present"])
    source_summary = {
        "schema_version": "observable_pre_action_source_link_summary_v1",
        "row_count": len(rows),
        "source_sequence_presence_by_status": {
            "admitted": {"present": sum(1 for item in rows if item["status_before_action"] == "admitted" and item["source_sequence_present"])},
            "blocked": {"present": sum(1 for item in rows if item["status_before_action"] == "blocked" and item["source_sequence_present"])},
        },
        "quote_intent_presence_by_status": {
            "admitted": {"present": admitted},
            "blocked": {"missing": blocked},
        },
        "source_order_presence_by_status": {
            "admitted": {"present": admitted},
            "blocked": {"missing": blocked},
        },
    }
    write_json(source_path, source_summary)
    write_json(
        feature_manifest_path,
        {
            "schema_version": "observable_pre_action_feature_join_manifest_v1",
            "candidate_rows_path": rows_path.name,
            "source_link_summary_path": source_path.name,
            "candidate_row_count": len(rows),
            "field_contract": {
                "default_off": True,
                "events_jsonl_read": False,
                "raw_replay_or_full_store_scan": False,
                "private_truth_ready": False,
                "deployable": False,
            },
        },
    )
    frozen = {
        "schema_version": "frozen_observable_pre_action_rule_manifest_v1",
        "frozen_rule_id": "fixture_source_sequence_repair_rule_v1",
        "rule_version": "v1",
        "created_from_train_manifest": "fixture_train.json",
        "holdout_manifest": "fixture_holdout.json",
        "predicate_expression": "source_sequence_present and repair_or_pairing_improving",
        "predicate_feature_names": ["source_sequence_present", "source_risk_direction", "offset_bucket"],
        "predicate": {
            "all": [
                {"field": "source_sequence_present", "op": "eq", "value": True},
                {"field": "source_risk_direction", "op": "eq", "value": "repair_or_pairing_improving"},
                {"field": "offset_bucket", "op": "in", "value": ["offset_0_30", "offset_30_60"]},
            ]
        },
        "allowed_live_feature_families": ["source_sequence_present", "source_risk_direction", "offset_bucket"],
        "forbidden_live_feature_families": ["realized_pair_cost", "source_pair", "source_residual"],
        "train_label_fields": ["source_pair_qty", "source_residual_qty"],
        "holdout_label_fields": ["source_pair_qty", "source_residual_qty"],
        "train_holdout_split": {
            "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
            "holdout_days": ["2026-05-04", "2026-05-05"],
        },
        "same_window_scope": {"same_window_id": "fixture_same_window"},
        "uses_only_pre_action_fields": True,
        "uses_realized_pair_cost": False,
        "uses_future_labels": False,
        "train_holdout_gate_passed": True,
        "train_selected_rows": 300,
        "holdout_selected_rows": 120,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {"passed": False},
    }
    if frozen_override:
        frozen_override(frozen)
    write_json(frozen_path, frozen)
    aggregate = {
        "schema_version": "same_window_aggregate_summary_v1",
        "same_window_id": "fixture_same_window",
        "same_window_denominator_count": len(rows),
        "admitted_count": admitted,
        "blocked_count": blocked,
        "source_sequence_present_count": source_sequence_present,
        "quote_intent_present_count": quote_intent_present,
        "source_order_present_count": source_order_present,
    }
    if aggregate_override:
        aggregate_override(aggregate)
    write_json(same_window_path, aggregate)
    return case_dir


good_rows = [row(i) for i in range(1200)]
materialize_case("good", good_rows)
materialize_case("missing_frozen_id", good_rows, frozen_override=lambda frozen: frozen.pop("frozen_rule_id"))
materialize_case(
    "forbidden_live_feature",
    good_rows,
    frozen_override=lambda frozen: frozen["predicate_feature_names"].append("realized_pair_cost"),
)
materialize_case("low_denominator", [row(i) for i in range(80)])
low_coverage_rows = [row(i, source_sequence_present=(i % 4 != 0)) for i in range(1200)]
materialize_case("low_source_coverage", low_coverage_rows)
materialize_case("aggregate_mismatch", good_rows, aggregate_override=lambda aggregate: aggregate.update({"admitted_count": 1}))
materialize_case("private_claim", good_rows, frozen_override=lambda frozen: frozen.update({"deployable": True}))
PY

run_case() {
  local case="$1"
  python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer.py \
    --fixture \
    --candidate-rows "$out_dir/fixtures/$case/observable_pre_action_candidate_rows.jsonl" \
    --source-link-summary "$out_dir/fixtures/$case/observable_pre_action_source_link_summary.json" \
    --feature-join-manifest "$out_dir/fixtures/$case/observable_pre_action_feature_join_manifest.json" \
    --frozen-rule-manifest "$out_dir/fixtures/$case/frozen_observable_pre_action_rule_manifest.json" \
    --same-window-aggregate-summary "$out_dir/fixtures/$case/same_window_aggregate_summary.json" \
    --output-dir "$out_dir/$case" >> "$log" 2>&1
}

run_case good
run_case missing_frozen_id
run_case forbidden_live_feature
run_case low_denominator
run_case low_source_coverage
run_case aggregate_mismatch
run_case private_claim

python3 scripts/xuan_observable_pre_action_rule_miner_spec.py \
  --candidate-input-manifest "$out_dir/good/observable_pre_action_rule_miner_input_manifest.json" \
  --output-dir "$out_dir/miner_spec_good" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])

def load(name: str) -> dict:
    return json.loads((out_dir / name / "manifest.json").read_text())

good = load("good")
missing = load("missing_frozen_id")
forbidden = load("forbidden_live_feature")
low_denominator = load("low_denominator")
low_coverage = load("low_source_coverage")
aggregate_mismatch = load("aggregate_mismatch")
private_claim = load("private_claim")
miner_spec = load("miner_spec_good")

assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY"
assert good["score"]["same_window_denominator_count"] == 1200
assert good["score"]["rule_match_count"] >= 20
assert good["score"]["source_sequence_coverage"] >= 0.95
assert good["score"]["aggregate_parity"] is True
assert Path(good["outputs"]["denominator_summary"]).exists()
assert Path(good["outputs"]["input_manifest"]).exists()
assert miner_spec["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_FIXTURE_PASS_NOT_REAL"
assert "missing_frozen_rule_id" in missing["blockers"]
assert "predicate_uses_forbidden_live_feature_family" in forbidden["blockers"]
assert "denominator_sample_too_small" in low_denominator["blockers"]
assert "source_sequence_coverage_below_min" in low_coverage["blockers"]
assert "aggregate_parity_failed" in aggregate_mismatch["blockers"]
assert "private_or_deployable_claim_present" in private_claim["blockers"]
for manifest in (good, missing, forbidden, low_denominator, low_coverage, aggregate_mismatch, private_claim):
    assert manifest["strategy_evidence"] is False
    assert manifest["no_order_diagnostic_allowed"] is False
    assert manifest["promotion_gate"]["passed"] is False

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_SMOKE_PASS",
    "checks": {
        "good_fixture_scorer_keep": True,
        "generated_denominator_summary": True,
        "generated_input_manifest": True,
        "miner_spec_accepts_generated_fixture_input": True,
        "missing_frozen_rule_id_fail_closed": True,
        "forbidden_live_feature_fail_closed": True,
        "low_denominator_fail_closed": True,
        "low_source_coverage_fail_closed": True,
        "aggregate_mismatch_fail_closed": True,
        "private_claim_fail_closed": True,
        "promotion_gate_separated": True,
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good/observable_pre_action_no_order_denominator_summary.json" >/dev/null
python3 -m json.tool "$out_dir/good/observable_pre_action_rule_miner_input_manifest.json" >/dev/null
python3 -m json.tool "$out_dir/miner_spec_good/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
