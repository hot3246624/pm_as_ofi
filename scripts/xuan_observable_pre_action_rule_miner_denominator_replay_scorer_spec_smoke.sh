#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec.py >> "$log" 2>&1

GOOD_FEATURE_JOIN="xuan_research_artifacts/xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z/manifest.json"

python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec.py \
  --feature-join-scorer-manifest "$GOOD_FEATURE_JOIN" \
  --output-dir "$out_dir/good" >> "$log" 2>&1

python3 - "$GOOD_FEATURE_JOIN" "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

good_path = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
good = json.loads(good_path.read_text())

bad = dict(good)
bad["decision_label"] = "UNKNOWN_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_FAIL_CLOSED"
bad_dir = out_dir / "fixtures"
bad_dir.mkdir(parents=True, exist_ok=True)
(bad_dir / "bad_feature_join_scorer_manifest.json").write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")

claim = dict(good)
claim["deployable"] = True
(bad_dir / "private_claim_feature_join_scorer_manifest.json").write_text(
    json.dumps(claim, indent=2, sort_keys=True) + "\n"
)
PY

python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec.py \
  --feature-join-scorer-manifest "$out_dir/fixtures/bad_feature_join_scorer_manifest.json" \
  --output-dir "$out_dir/bad_feature_join" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec.py \
  --feature-join-scorer-manifest "$out_dir/fixtures/private_claim_feature_join_scorer_manifest.json" \
  --output-dir "$out_dir/private_claim" >> "$log" 2>&1

python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec.py \
  --feature-join-scorer-manifest "$out_dir/fixtures/missing_feature_join_scorer_manifest.json" \
  --output-dir "$out_dir/missing" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
good = json.loads((out_dir / "good/manifest.json").read_text())
bad = json.loads((out_dir / "bad_feature_join/manifest.json").read_text())
claim = json.loads((out_dir / "private_claim/manifest.json").read_text())
missing = json.loads((out_dir / "missing/manifest.json").read_text())

assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_SCORER_SPEC_READY"
assert good["strategy_evidence"] is False
assert good["promotion_gate"]["passed"] is False
assert good["future_scorer_target"]["outputs"] == [
    "observable_pre_action_no_order_denominator_summary.json",
    "observable_pre_action_rule_miner_input_manifest.json",
]
assert "source_sequence_coverage" in good["denominator_summary_contract"]["required_fields"]
assert "realized_pair_cost" in good["frozen_rule_manifest_contract"]["forbidden_live_feature_families"]
assert bad["decision"] == "UNKNOWN"
assert "feature_join_output_scorer_not_ready" in bad["blockers"]
assert claim["decision"] == "UNKNOWN"
assert "feature_join_deployable_not_false" in claim["blockers"]
assert missing["decision"] == "UNKNOWN"
assert "feature_join_output_scorer_manifest_missing" in missing["blockers"]

manifest = {
    "artifact": "xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_SCORER_SPEC_SMOKE_PASS",
    "checks": {
        "good_spec_keep": True,
        "bad_feature_join_scorer_fail_closed": True,
        "private_claim_fail_closed": True,
        "missing_feature_join_scorer_fail_closed": True,
        "denominator_output_contract_present": True,
        "forbidden_live_features_present": True,
        "promotion_gate_separated": True,
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/bad_feature_join/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/private_claim/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/missing/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
