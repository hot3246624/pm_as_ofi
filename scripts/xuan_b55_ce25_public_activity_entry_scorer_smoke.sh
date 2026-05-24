#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_scorer_smoke_$ts}"
mkdir -p "$out_dir"

good_out="$out_dir/good"
bad_spec="$out_dir/bad_spec_fixture_manifest.json"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_scorer.py" \
  --output-dir "$good_out" \
  >"$out_dir/good.log"

python3 - "$ROOT" "$bad_spec" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
bad_path = Path(sys.argv[2])
src = root / "xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_exporter_fixture_smoke_20260524T075245Z/spec_fixture/manifest.json"
obj = json.loads(src.read_text())
status = obj["candidate_export_manifest_status"]
status["status"] = "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_FAIL_CLOSED"
status["ready"] = False
status["missing_fields"] = ["fair_probability"]
status["blockers"] = ["required_fields_missing"]
obj["research_ranking"]["real_export_status"] = "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_MISSING"
bad_path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
PY

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_scorer.py" \
  --spec-fixture-manifest "$bad_spec" \
  --output-dir "$bad_out" \
  >"$out_dir/bad.log"

python3 - "$good_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

good = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
checks = {
    "good_keep": good["decision_label"] == "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_READY_REAL_UNKNOWN_FIXTURE_READY",
    "good_real_unknown": good["research_ranking"]["real_export_status"] == "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING",
    "good_fixture_ready": good["research_ranking"]["fixture_export_status"] == "READY_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT_NOT_REAL_INPUT",
    "good_real_strategy_false": not good["score"]["real_score"]["strategy_evidence"],
    "good_fixture_strategy_false": not good["score"]["fixture_score"]["strategy_evidence"],
    "good_missing_liquidity_role": "liquidity_role" in good["score"]["real_score"]["missing_fields"],
    "good_requires_non_fixture_inputs": len(good["score"]["required_non_fixture_inputs_before_no_order_diagnostic"]) == 3,
    "bad_unknown": bad["decision_label"] == "UNKNOWN_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_INPUTS_INSUFFICIENT",
    "bad_fixture_check_failed": not bad["score"]["checks"]["fixture_spec_ready"],
    "promotion_never_passes": not good["promotion_gate"]["passed"] and not bad["promotion_gate"]["passed"],
    "no_side_effect_scope": not good["scope"]["shadow_started"] and not good["scope"]["orders_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"b55/ce25 public activity entry scorer smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b55_ce25_public_activity_entry_scorer_smoke",
    "decision_label": "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_SMOKE_PASS",
    "checks": checks,
    "good_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
