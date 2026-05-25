#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_rule_miner_training_spec.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture_dir = out_dir / "fixtures"
fixture_dir.mkdir(parents=True, exist_ok=True)

script = Path("scripts/xuan_observable_pre_action_rule_miner_training_spec.py")
src_manifest = Path(
    "xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/manifest.json"
)
src_rows = Path(
    "xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/observable_pre_action_training_bridge_joined_rows.jsonl"
)
src_summary = Path(
    "xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/observable_pre_action_training_bridge_summary.json"
)
src_denominator = Path(
    "xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec_20260525T195536Z/manifest.json"
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
    manifest = json.loads(src_manifest.read_text())
    rows = load_rows(src_rows)
    summary = json.loads(src_summary.read_text())
    denominator = json.loads(src_denominator.read_text())
    if mutator:
        mutator(manifest, rows, summary, denominator)
    write_json(case_dir / "bridge_manifest.json", manifest)
    write_rows(case_dir / "joined_rows.jsonl", rows)
    write_json(case_dir / "bridge_summary.json", summary)
    write_json(case_dir / "denominator_spec.json", denominator)
    return case_dir


def run_case(case, path):
    output_dir = out_dir / case
    subprocess.run(
        [
            "python3",
            str(script),
            "--bridge-scorer-manifest",
            str(path / "bridge_manifest.json"),
            "--joined-rows",
            str(path / "joined_rows.jsonl"),
            "--bridge-summary",
            str(path / "bridge_summary.json"),
            "--denominator-spec-manifest",
            str(path / "denominator_spec.json"),
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
cases["bad_bridge_decision"] = materialize(
    "bad_bridge_decision",
    lambda manifest, rows, summary, denominator: manifest.update({"decision_label": "UNKNOWN_BAD_BRIDGE"}),
)
cases["label_live"] = materialize(
    "label_live",
    lambda manifest, rows, summary, denominator: rows[0].update({"post_action_labels_allowed_in_live_predicate": True}),
)
cases["missing_label"] = materialize(
    "missing_label",
    lambda manifest, rows, summary, denominator: rows[0].pop("source_residual_cost"),
)
cases["no_holdout"] = materialize(
    "no_holdout",
    lambda manifest, rows, summary, denominator: [
        row.update({"split": "train"}) for row in rows
    ] or summary.update({"split_counts": {"train": len(rows), "holdout": 0, "unused": 0}, "holdout_days": []}),
)
cases["summary_private_claim"] = materialize(
    "summary_private_claim",
    lambda manifest, rows, summary, denominator: summary.update({"private_truth_ready": True}),
)
cases["denominator_not_ready"] = materialize(
    "denominator_not_ready",
    lambda manifest, rows, summary, denominator: denominator.update({"decision_label": "UNKNOWN_DENOMINATOR"}),
)

results = {name: run_case(name, path) for name, path in cases.items()}
good = results["good"]
assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_READY_REAL_INPUT_UNKNOWN", good
assert good["strategy_evidence"] is False
assert good["no_order_diagnostic_allowed"] is False
assert good["promotion_gate"]["passed"] is False
assert "source_pair_qty" in good["contract"]["forbidden_live_feature_families"]

assert_unknown_contains(results["bad_bridge_decision"], "bridge_fixture_scorer_not_ready")
assert_unknown_contains(results["label_live"], "label_live_predicate_not_false")
assert_unknown_contains(results["missing_label"], "required_fields_missing")
assert_unknown_contains(results["no_holdout"], "holdout")
assert_unknown_contains(results["summary_private_claim"], "private")
assert_unknown_contains(results["denominator_not_ready"], "denominator_replay_spec_not_ready")

summary = {
    "artifact": "xuan_observable_pre_action_rule_miner_training_spec_smoke",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_SMOKE_READY",
    "cases": {name: result["decision_label"] for name, result in results.items()},
    "good_fixture_score": good["fixture_input_score"],
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
