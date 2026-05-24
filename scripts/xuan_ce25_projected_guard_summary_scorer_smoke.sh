#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_projected_guard_summary_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_ce25_projected_guard_summary_scorer.py >> "$log" 2>&1

good_out="$out_dir/good"
bad_root="$out_dir/bad_fixture"
bad_out="$out_dir/bad"
missing_out="$out_dir/missing"

python3 scripts/xuan_ce25_projected_guard_summary_scorer.py \
  --output-dir "$good_out" \
  >> "$log" 2>&1

python3 - "$ROOT" "$bad_root" <<'PY' >> "$log" 2>&1
import json
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
bad_root = Path(sys.argv[2])
src = root / "xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_20260524T113800Z"
shutil.copytree(src / "default_off", bad_root / "default_off", dirs_exist_ok=True)
shutil.copytree(src / "enabled", bad_root / "enabled", dirs_exist_ok=True)
shutil.copy2(src / "manifest.json", bad_root / "manifest.json")
agg_path = bad_root / "enabled" / "aggregate_report.json"
agg = json.loads(agg_path.read_text())
agg["event_lite"]["ce25_projected_guard_summary"]["field_contract"]["trading_behavior_changed"] = True
agg_path.write_text(json.dumps(agg, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_ce25_projected_guard_summary_scorer.py \
  --runner-smoke-manifest "$bad_root/manifest.json" \
  --default-aggregate "$bad_root/default_off/aggregate_report.json" \
  --enabled-aggregate "$bad_root/enabled/aggregate_report.json" \
  --enabled-summary-dir "$bad_root/enabled" \
  --output-dir "$bad_out" \
  >> "$log" 2>&1

python3 scripts/xuan_ce25_projected_guard_summary_scorer.py \
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
    "good_keep": good["decision_label"] == "KEEP_CE25_PROJECTED_GUARD_SUMMARY_SCORER_READY",
    "good_default_off_absent": good["score"]["checks"]["default_off_summary_absent"],
    "good_enabled_present": good["score"]["checks"]["enabled_summary_present"],
    "good_aggregate_parity": good["score"]["checks"]["aggregate_parity"],
    "good_starter_count": good["score"]["summary"]["starter_allow_count"] == 1.0,
    "good_core_count": good["score"]["summary"]["core_allow_count"] == 1.0,
    "good_hard_kill_count": good["score"]["summary"]["hard_kill_pair_cost_count"] == 1.0,
    "good_research_not_strategy_evidence": not good["research_ranking"]["strategy_evidence"],
    "good_promotion_false": not good["promotion_gate"]["passed"],
    "bad_unknown": bad["decision_label"] == "UNKNOWN_CE25_PROJECTED_GUARD_SUMMARY_SCORER_INPUTS_INSUFFICIENT",
    "bad_field_contract_failed": not bad["score"]["checks"]["field_contract_ok"],
    "missing_unknown": missing["decision_label"] == "UNKNOWN_CE25_PROJECTED_GUARD_SUMMARY_SCORER_INPUTS_INSUFFICIENT",
    "no_side_effect_scope": not good["scope"]["shadow_started"] and not good["scope"]["orders_cancels_redeems_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 projected guard summary scorer smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_projected_guard_summary_scorer_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_CE25_PROJECTED_GUARD_SUMMARY_SCORER_SMOKE_PASS",
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
