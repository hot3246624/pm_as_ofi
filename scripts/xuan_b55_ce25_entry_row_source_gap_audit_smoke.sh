#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_B55_CE25_ENTRY_ROW_SOURCE_GAP_AUDIT_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b55_ce25_entry_row_source_gap_audit_smoke_$ts}"
mkdir -p "$out_dir"

missing_out="$out_dir/missing"
complete_fixture="$out_dir/complete_source_manifest.json"
complete_out="$out_dir/complete"
bad_fixture="$out_dir/bad_source_manifest.json"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_b55_ce25_entry_row_source_gap_audit.py" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$ROOT" "$complete_fixture" "$bad_fixture" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
complete_path = Path(sys.argv[2])
bad_path = Path(sys.argv[3])
spec = json.loads(
    (root / "xuan_research_artifacts/xuan_b55_ce25_fair_probability_model_spec_20260524T055426Z/manifest.json").read_text()
)
required = spec["probability_model_contract"]["required_candidate_fields"]
joins = {
    "public_activity_trade_rows": True,
    "market_metadata_boundary": True,
    "fair_source_snapshot_rows": True,
    "volatility_rows": True,
    "liquidity_role_truth_source": True,
    "probability_model_output": True,
}
complete = {
    "artifact": "xuan_b55_ce25_entry_row_source_gap_complete_fixture",
    "decision_label": "KEEP_FIXTURE_SOURCE_MANIFEST_READY",
    "fixture": True,
    "provided_fields": required,
    "accounts_covered": ["b55", "ce25"],
    "joins_declared": joins,
}
bad_fields = [
    field
    for field in required
    if field not in {"liquidity_role", "fair_probability", "fair_probability_uncertainty"}
]
bad_joins = dict(joins)
bad_joins["liquidity_role_truth_source"] = False
bad_joins["probability_model_output"] = False
bad = {
    "artifact": "xuan_b55_ce25_entry_row_source_gap_bad_fixture",
    "decision_label": "UNKNOWN_BAD_SOURCE_MANIFEST_MISSING_ROLE_AND_PROBABILITY",
    "fixture": True,
    "provided_fields": bad_fields,
    "accounts_covered": ["b55", "ce25"],
    "joins_declared": bad_joins,
}
complete_path.write_text(json.dumps(complete, indent=2, sort_keys=True) + "\n")
bad_path.write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
PY

python3 "$ROOT/scripts/xuan_b55_ce25_entry_row_source_gap_audit.py" \
  --candidate-source-manifest "$complete_fixture" \
  --output-dir "$complete_out" \
  >"$out_dir/complete.log"

python3 "$ROOT/scripts/xuan_b55_ce25_entry_row_source_gap_audit.py" \
  --candidate-source-manifest "$bad_fixture" \
  --output-dir "$bad_out" \
  >"$out_dir/bad.log"

python3 - "$missing_out/manifest.json" "$complete_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

missing = json.loads(Path(sys.argv[1]).read_text())
complete = json.loads(Path(sys.argv[2]).read_text())
bad = json.loads(Path(sys.argv[3]).read_text())
checks = {
    "missing_decision_unknown": missing["decision_label"] == "UNKNOWN_B55_CE25_ENTRY_ROW_SOURCE_GAP_REAL_EXPORT_FIELDS_MISSING",
    "missing_candidate_source_absent": missing["candidate_source_manifest_status"]["status"] == "MISSING_CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST",
    "missing_names_liquidity_role": "liquidity_role" in missing["research_ranking"]["hard_missing_fields"],
    "missing_names_fair_probability": "fair_probability" in missing["research_ranking"]["hard_missing_fields"],
    "complete_fixture_ready": complete["candidate_source_manifest_status"]["status"] == "CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST_READY",
    "complete_fixture_not_real_source": complete["research_ranking"]["real_source_ready"] is False,
    "bad_fixture_fail_closed": bad["candidate_source_manifest_status"]["status"] == "CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST_FAIL_CLOSED",
    "bad_fixture_missing_role": "liquidity_role" in bad["candidate_source_manifest_status"]["missing_fields"],
    "bad_fixture_missing_probability": "fair_probability" in bad["candidate_source_manifest_status"]["missing_fields"],
    "minimal_exporter_contract_present": missing["minimal_safe_exporter_contract"]["contract_name"] == "b55_ce25_entry_row_source_export_v1",
    "promotion_never_passes": not missing["promotion_gate"]["passed"] and not complete["promotion_gate"]["passed"],
    "no_side_effect_scope": not missing["scope"]["shadow_started"] and not missing["scope"]["orders_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"b55/ce25 entry-row source gap audit smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b55_ce25_entry_row_source_gap_audit_smoke",
    "decision_label": "KEEP_B55_CE25_ENTRY_ROW_SOURCE_GAP_AUDIT_SMOKE_PASS",
    "checks": checks,
    "missing_manifest": sys.argv[1],
    "complete_manifest": sys.argv[2],
    "bad_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
