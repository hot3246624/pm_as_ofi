#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_root = out_dir / "fixtures"
fixture_root.mkdir(parents=True, exist_ok=True)
script = Path("scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer.py")

csv_fields = [
    "schema_version",
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
    "trigger_size",
    "trigger_ts_ms",
    "seed_qty",
    "seed_cost",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
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
    "separator_export_reason",
]


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def write_csv(path, rows, fields=None):
    fields = fields or csv_fields
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def feature_row(i, *, exact=True):
    day = ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05"][i % 5]
    admitted = i % 2 == 0
    risk = "repair_or_pairing_improving" if i % 3 == 0 else "risk_increasing"
    row = {
        "schema_version": "observable_pre_action_candidate_row_v1",
        "condition_id": f"condition-{i}",
        "market_slug": f"btc-updown-5m-{1900000000 + i * 300}",
        "day_id": day,
        "quote_ts_ms": 1900000000000 + i * 1000,
        "side": "YES" if i % 2 == 0 else "NO",
        "offset_s": 30.0 + i,
        "offset_bucket": "offset_30_60",
        "status_before_action": "admitted" if admitted else "blocked",
        "reason": "candidate" if admitted else "target",
        "block_reason": None if admitted else "target",
        "source_sequence_present": True,
        "quote_intent_present": admitted,
        "source_order_present": admitted,
        "source_sequence_id_policy": "present_redacted",
        "quote_intent_id_policy": "present_redacted" if admitted else "missing",
        "source_order_id_policy": "present_redacted" if admitted else "missing",
        "pre_seed_same_qty": 0.0 if risk == "repair_or_pairing_improving" else 5.0,
        "pre_seed_opp_qty": 5.0 if risk == "repair_or_pairing_improving" else 0.0,
        "pre_seed_same_cost": 0.0 if risk == "repair_or_pairing_improving" else 2.0,
        "pre_seed_opp_cost": 2.0 if risk == "repair_or_pairing_improving" else 0.0,
        "pre_seed_open_qty": 5.0,
        "pre_seed_open_cost": 2.0,
        "pre_seed_deficit_qty": 5.0 if risk == "repair_or_pairing_improving" else 0.0,
        "candidate_qty": 5.0,
        "candidate_qty_bucket": "candidate_qty_eq_5",
        "source_risk_direction": risk,
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }
    if exact:
        row["source_seed_candidate_row_id"] = f"seed-row-{i}"
        row["source_seed_action_id"] = f"seed-action-{i}"
    return row


def label_row(feature, i):
    return {
        "schema_version": "candidate_seed_outcome_separator_v1",
        "day": feature["day_id"],
        "condition_id": feature["condition_id"],
        "slug": feature["market_slug"],
        "source_seed_action_id": f"seed-action-{i}",
        "source_seed_candidate_row_id": f"seed-row-{i}",
        "source_label": "fixture",
        "ts_ms": str(feature["quote_ts_ms"]),
        "ts_iso": "2030-03-17T00:00:00Z",
        "side": feature["side"],
        "opposite_side": "NO" if feature["side"] == "YES" else "YES",
        "offset_s": str(feature["offset_s"]),
        "trigger_size": "5.0",
        "trigger_ts_ms": str(feature["quote_ts_ms"]),
        "seed_qty": "5.0",
        "seed_cost": "2.0",
        "source_risk_direction": feature["source_risk_direction"],
        "pre_seed_same_qty": str(feature["pre_seed_same_qty"]),
        "pre_seed_opp_qty": str(feature["pre_seed_opp_qty"]),
        "pre_seed_same_cost": str(feature["pre_seed_same_cost"]),
        "pre_seed_opp_cost": str(feature["pre_seed_opp_cost"]),
        "pre_seed_open_qty": str(feature["pre_seed_open_qty"]),
        "pre_seed_open_cost": str(feature["pre_seed_open_cost"]),
        "source_pair_qty": "2.0",
        "source_pair_cost": "1.8",
        "source_pair_pnl": "0.2",
        "source_residual_qty": "0.0" if feature["source_risk_direction"] == "repair_or_pairing_improving" else "1.0",
        "source_residual_cost": "0.0" if feature["source_risk_direction"] == "repair_or_pairing_improving" else "0.4",
        "source_residual_age_s": "0.0",
        "pair_outcome_bucket": "pair_positive",
        "residual_tail_outcome_bucket": "residual_none",
        "outcome_labels_are_post_action": "true",
        "pre_action_field_policy": "pre_action_fields_only",
        "post_action_label_policy": "offline_train_holdout_only",
        "live_rule_safety_policy": "labels_forbidden_in_live_rule",
        "separator_export_reason": "fixture",
    }


def field_contract(**overrides):
    data = {
        "schema_version": "observable_pre_action_feature_join_field_contract_v1",
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
    data.update(overrides)
    return data


def frozen_rule(**overrides):
    data = {
        "schema_version": "frozen_observable_pre_action_rule_manifest_v1",
        "frozen_rule_id": "fixture_bridge_rule_v1",
        "predicate_feature_names": ["source_sequence_present", "source_risk_direction", "offset_bucket"],
        "train_label_fields": ["source_pair_qty", "source_residual_qty"],
        "holdout_label_fields": ["source_pair_qty", "source_residual_qty"],
        "train_holdout_split": {
            "train_days": ["2026-05-01", "2026-05-02", "2026-05-03"],
            "holdout_days": ["2026-05-04", "2026-05-05"],
        },
        "uses_only_pre_action_fields": True,
        "uses_realized_pair_cost": False,
        "uses_future_labels": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {"passed": False},
    }
    data.update(overrides)
    return data


def materialize(case, mutator=None):
    case_dir = fixture_root / case
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    features = [feature_row(i, exact=i < 5) for i in range(10)]
    labels = [label_row(feature, i) for i, feature in enumerate(features)]
    frozen = frozen_rule()
    source_contract = field_contract()
    manifest_contract = field_contract()
    fields = list(csv_fields)
    if mutator:
        mutator(features, labels, frozen, source_contract, manifest_contract, fields)
    write_jsonl(case_dir / "observable_pre_action_candidate_rows.jsonl", features)
    write_csv(case_dir / "offline_labels.csv", labels, fields)
    write_json(
        case_dir / "observable_pre_action_source_link_summary.json",
        {
            "schema_version": "observable_pre_action_source_link_summary_v1",
            "row_count": len(features),
            "field_contract": source_contract,
        },
    )
    write_json(
        case_dir / "observable_pre_action_feature_join_manifest.json",
        {
            "schema_version": "observable_pre_action_feature_join_manifest_v1",
            "candidate_row_count": len(features),
            "field_contract": manifest_contract,
        },
    )
    write_json(case_dir / "frozen_observable_pre_action_rule_manifest.json", frozen)
    return case_dir


def run_case(case, case_dir):
    output_dir = out_dir / case
    subprocess.run(
        [
            "python3",
            str(script),
            "--fixture",
            "--feature-rows",
            str(case_dir / "observable_pre_action_candidate_rows.jsonl"),
            "--source-link-summary",
            str(case_dir / "observable_pre_action_source_link_summary.json"),
            "--feature-join-manifest",
            str(case_dir / "observable_pre_action_feature_join_manifest.json"),
            "--offline-label-csv",
            str(case_dir / "offline_labels.csv"),
            "--frozen-rule-manifest",
            str(case_dir / "frozen_observable_pre_action_rule_manifest.json"),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
    )
    return json.loads((output_dir / "manifest.json").read_text())


def assert_unknown_contains(manifest, token):
    assert manifest["decision"] == "UNKNOWN", manifest
    assert any(token in blocker for blocker in manifest["blockers"]), manifest["blockers"]


cases = {}
cases["good"] = materialize("good")
cases["missing_join_key"] = materialize("missing_join_key", lambda features, labels, frozen, sc, mc, fields: features[0].pop("side"))
cases["multiple_matches"] = materialize("multiple_matches", lambda features, labels, frozen, sc, mc, fields: labels.append(dict(labels[7])))
cases["outside_tolerance"] = materialize("outside_tolerance", lambda features, labels, frozen, sc, mc, fields: labels.__setitem__(7, dict(labels[7], ts_ms=str(int(labels[7]["ts_ms"]) + 1000), trigger_ts_ms=str(int(labels[7]["trigger_ts_ms"]) + 1000))))
cases["missing_label_field"] = materialize("missing_label_field", lambda features, labels, frozen, sc, mc, fields: fields.remove("source_residual_cost"))
cases["label_used_live"] = materialize("label_used_live", lambda features, labels, frozen, sc, mc, fields: frozen["predicate_feature_names"].append("source_pair_qty"))
cases["aggregate_mismatch"] = materialize("aggregate_mismatch")
aggregate_summary_path = cases["aggregate_mismatch"] / "observable_pre_action_source_link_summary.json"
aggregate_summary = json.loads(aggregate_summary_path.read_text())
aggregate_summary["row_count"] = 1
write_json(aggregate_summary_path, aggregate_summary)
cases["private_claim"] = materialize("private_claim", lambda features, labels, frozen, sc, mc, fields: mc.update({"deployable": True}))

results = {name: run_case(name, path) for name, path in cases.items()}
good = results["good"]
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_READY", good
assert good["score"]["join_method_counts"]["composite_fallback"] > 0
assert sum(value for key, value in good["score"]["join_method_counts"].items() if key.startswith("exact_id:")) > 0
assert good["strategy_evidence"] is False
assert good["promotion_gate"]["passed"] is False

assert_unknown_contains(results["missing_join_key"], "required_fields_missing")
assert_unknown_contains(results["multiple_matches"], "multiple_composite_matches")
assert_unknown_contains(results["outside_tolerance"], "zero_composite_match")
assert_unknown_contains(results["missing_label_field"], "offline_label_fields_missing")
assert_unknown_contains(results["label_used_live"], "label_used_as_live_predicate")
assert_unknown_contains(results["aggregate_mismatch"], "row_count_mismatch")
assert_unknown_contains(results["private_claim"], "deployable_not_false")

summary = {
    "artifact": "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_smoke",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_SMOKE_READY",
    "cases": {name: result["decision_label"] for name, result in results.items()},
    "good_score": good["score"],
    "strategy_evidence": False,
    "no_order_diagnostic_allowed": False,
    "private_truth_ready": False,
    "deployable": False,
    "promotion_gate": {"passed": False},
}
write_json(out_dir / "manifest.json", summary)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" > /dev/null

echo "wrote $out_dir/manifest.json"
