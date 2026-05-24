#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_CE25_PROJECTED_GUARD_RUNNER_SPEC_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_spec_smoke_$ts}"
mkdir -p "$out_dir"

good_out="$out_dir/good"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
bad_runner="$tmp_dir/bad_runner.py"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_ce25_projected_guard_runner_instrumentation_spec.py" \
  --output-dir "$good_out" \
  >"$out_dir/good.log"

python3 - "$ROOT/tools/xuan_dplus_passive_passive_shadow_runner.py" "$bad_runner" <<'PY'
import sys
from pathlib import Path

src = Path(sys.argv[1]).read_text()
bad = src.replace("same_cost_before = self.exposure_cost(side)", "same_cost_before = 0.0  # smoke removed source cost token")
bad = bad.replace("opp_cost_before = self.exposure_cost(opp(side))", "opp_cost_before = 0.0  # smoke removed opp cost token")
Path(sys.argv[2]).write_text(bad)
PY

python3 "$ROOT/scripts/xuan_ce25_projected_guard_runner_instrumentation_spec.py" \
  --runner "$bad_runner" \
  --output-dir "$bad_out" \
  >"$out_dir/bad.log"

python3 - "$good_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

good = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
contract = good["instrumentation_contract"]
checks = {
    "good_keep": good["decision_label"] == "KEEP_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_SPEC_READY",
    "good_source_qty_available": good["capability_checks"]["same_opp_qty_available"],
    "good_source_cost_available": good["capability_checks"]["same_opp_cost_available"],
    "good_intended_order_available": good["capability_checks"]["intended_order_available"],
    "good_event_lite_pattern": good["capability_checks"]["event_lite_pattern_available"],
    "good_default_off_flag": contract["default_off_flag"] == "--ce25-projected-guard-event-lite-summary",
    "good_behavior_unchanged": not contract["behavior_change"],
    "good_has_summary_schema": "event_lite.ce25_projected_guard_summary.schema_version" in contract["summary_fields"],
    "good_smoke_plan_covers_default_off": any("default-off" in item for item in contract["smoke_plan"]),
    "good_next_target": good["research_ranking"]["next_local_only_target"] == "ce25_projected_guard_runner_instrumentation_default_off_v1",
    "good_no_order_not_allowed": not good["research_ranking"]["no_order_diagnostic_allowed"],
    "promotion_never_passes": not good["promotion_gate"]["passed"] and not bad["promotion_gate"]["passed"],
    "bad_unknown": bad["decision_label"] == "UNKNOWN_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_SPEC_BLOCKED",
    "bad_cost_check_failed": not bad["capability_checks"]["same_opp_cost_available"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 projected guard runner instrumentation spec smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_projected_guard_runner_instrumentation_spec_smoke",
    "decision_label": "KEEP_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_SPEC_SMOKE_PASS",
    "checks": checks,
    "good_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
