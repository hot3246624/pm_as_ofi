#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_symmetric_activation_summary_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_symmetric_activation_summary_scorer.py >> "$log" 2>&1

good_out="$out_dir/good"
bad_root="$out_dir/bad_fixture"
bad_out="$out_dir/bad"
missing_out="$out_dir/missing"

python3 scripts/xuan_symmetric_activation_summary_scorer.py \
  --output-dir "$good_out" \
  >> "$log" 2>&1

python3 - "$ROOT" "$bad_root" <<'PY' >> "$log" 2>&1
import json
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
bad_root = Path(sys.argv[2])
src = root / "xuan_research_artifacts/xuan_symmetric_activation_runner_instrumentation_smoke_20260524T165500Z"
shutil.copytree(src / "default_off", bad_root / "default_off", dirs_exist_ok=True)
shutil.copytree(src / "enabled", bad_root / "enabled", dirs_exist_ok=True)
agg_path = bad_root / "enabled" / "aggregate_report.json"
agg = json.loads(agg_path.read_text())
agg["event_lite"]["symmetric_activation_summary"]["field_contract"]["trading_behavior_changed"] = True
agg_path.write_text(json.dumps(agg, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_symmetric_activation_summary_scorer.py \
  --default-aggregate "$bad_root/default_off/aggregate_report.json" \
  --enabled-aggregate "$bad_root/enabled/aggregate_report.json" \
  --enabled-summary-dir "$bad_root/enabled" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1

python3 scripts/xuan_symmetric_activation_summary_scorer.py \
  --enabled-aggregate "$out_dir/does_not_exist.json" \
  --enabled-summary-dir "$out_dir/does_not_exist" \
  --output-dir "$missing_out" \
  >> "$log" 2>&1

python3 - "$good_out/manifest.json" "$bad_out/manifest.json" "$missing_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

good = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
missing = json.loads(Path(sys.argv[3]).read_text())
checks = {
    "good_keep": good["decision_label"] == "KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_READY",
    "good_default_off_absent": good["score"]["checks"]["default_off_summary_absent"],
    "good_enabled_present": good["score"]["checks"]["enabled_summary_present"],
    "good_field_contract": good["score"]["checks"]["field_contract_ok"],
    "good_aggregate_parity": good["score"]["checks"]["aggregate_parity"],
    "good_blocked_denominator": good["score"]["summary"]["blocked_activation_denominator"] >= 1,
    "good_admitted_denominator": good["score"]["summary"]["admitted_activation_denominator"] >= 1,
    "good_source_coverage": good["score"]["checks"]["source_sequence_coverage_present"],
    "good_projected_pair_bucket": good["score"]["checks"]["projected_pair_cost_bucket_present"],
    "good_not_strategy_evidence": not good["research_ranking"]["strategy_evidence"],
    "good_promotion_false": not good["promotion_gate"]["passed"],
    "bad_unknown": bad["decision_label"] == "UNKNOWN_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_INPUTS_INSUFFICIENT",
    "bad_field_contract_failed": not bad["score"]["checks"]["field_contract_ok"],
    "missing_unknown": missing["decision_label"] == "UNKNOWN_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_INPUTS_INSUFFICIENT",
    "missing_enabled_blocker": "enabled_aggregate_missing" in missing["score"]["blockers"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"symmetric activation summary scorer smoke failed: {failed}")
manifest = {
    "artifact": "xuan_symmetric_activation_summary_scorer_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_SMOKE_PASS",
    "checks": checks,
    "good_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
    "missing_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
