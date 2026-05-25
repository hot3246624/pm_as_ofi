#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_fixture_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_dir = out_dir / "fixtures"
fixture_dir.mkdir(parents=True, exist_ok=True)

script = Path("scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py")
src_spec = Path("xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_spec_20260525T223507Z/manifest.json")
src_rows = Path(
    "xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/observable_pre_action_training_bridge_joined_rows.jsonl"
)


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def load_rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def materialize(case, mutator=None):
    case_dir = fixture_dir / case
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    spec = json.loads(src_spec.read_text())
    rows = load_rows(src_rows)
    if mutator:
        mutator(spec, rows)
    write_json(case_dir / "training_spec.json", spec)
    write_rows(case_dir / "joined_rows.jsonl", rows)
    return case_dir


def run_case(case, path, extra=None):
    output_dir = out_dir / case
    cmd = [
        "python3",
        str(script),
        "--training-spec-manifest",
        str(path / "training_spec.json"),
        "--joined-rows",
        str(path / "joined_rows.jsonl"),
        "--output-dir",
        str(output_dir),
    ]
    if extra:
        cmd.extend(extra)
    subprocess.run(cmd, check=True)
    return json.loads((output_dir / "manifest.json").read_text())


def assert_unknown_contains(manifest, token):
    assert manifest["decision"] == "UNKNOWN", manifest
    assert any(token in blocker for blocker in manifest["blockers"]), manifest["blockers"]


def mutate_forbidden_live_feature(spec, rows):
    spec["contract"]["allowed_live_feature_families"] = ["source_pair_qty"]


def mutate_label_leakage(spec, rows):
    rows[0]["post_action_labels_allowed_in_live_predicate"] = True


def mutate_insufficient_support(spec, rows):
    for row in rows:
        if row["split"] == "holdout" and row["source_risk_direction"] == "repair_or_pairing_improving":
            row["source_risk_direction"] = "risk_increasing"


def mutate_single_day_concentration(spec, rows):
    for row in rows:
        if row["source_risk_direction"] == "repair_or_pairing_improving":
            row["day_id"] = "2026-05-01"


def mutate_holdout_negative(spec, rows):
    for row in rows:
        if row["split"] == "holdout" and row["source_risk_direction"] == "repair_or_pairing_improving":
            row["source_residual_cost"] = "2.0"


def mutate_denominator_missing(spec, rows):
    spec["contract"]["denominator_replay_dependency"]["required_before_no_order_proposal"] = False


def mutate_private_claim(spec, rows):
    spec["deployable"] = True


cases = {
    "good": materialize("good"),
    "forbidden_live_feature": materialize("forbidden_live_feature", mutate_forbidden_live_feature),
    "label_leakage": materialize("label_leakage", mutate_label_leakage),
    "insufficient_support": materialize("insufficient_support", mutate_insufficient_support),
    "single_day_concentration": materialize("single_day_concentration", mutate_single_day_concentration),
    "holdout_negative": materialize("holdout_negative", mutate_holdout_negative),
    "denominator_missing": materialize("denominator_missing", mutate_denominator_missing),
    "private_claim": materialize("private_claim", mutate_private_claim),
}

results = {
    name: run_case(
        name,
        path,
        extra=(["--min-selected-holdout-rows", "99"] if name == "insufficient_support" else None),
    )
    for name, path in cases.items()
}
good = results["good"]
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_FIXTURE_SCORER_READY", good
best = good["score"]["best_predicate"]
assert best["predicate_feature_names"], best
assert not any("source_pair" in name or "source_residual" in name for name in best["predicate_feature_names"]), best
assert good["strategy_evidence"] is False
assert good["no_order_diagnostic_allowed"] is False
assert good["promotion_gate"]["passed"] is False

assert_unknown_contains(results["forbidden_live_feature"], "forbidden_live_feature")
assert_unknown_contains(results["label_leakage"], "label_leakage_live_predicate")
assert_unknown_contains(results["insufficient_support"], "selected_holdout")
assert_unknown_contains(results["single_day_concentration"], "single_day_concentration")
assert_unknown_contains(results["holdout_negative"], "holdout_objective_not_positive")
assert_unknown_contains(results["denominator_missing"], "denominator_dependency_missing")
assert_unknown_contains(results["private_claim"], "private_or_deployable")

summary = {
    "artifact": "xuan_observable_pre_action_rule_miner_training_fixture_scorer_smoke",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_FIXTURE_SCORER_SMOKE_READY",
    "cases": {name: result["decision_label"] for name, result in results.items()},
    "good_best_predicate": best,
    "good_best_metrics": good["score"]["best_metrics"],
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
