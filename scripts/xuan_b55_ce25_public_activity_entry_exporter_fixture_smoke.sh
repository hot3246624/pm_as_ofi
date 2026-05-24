#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_exporter_fixture_smoke_$ts}"
mkdir -p "$out_dir"

exporter_out="$out_dir/exporter"
spec_real_out="$out_dir/spec_real"
spec_fixture_out="$out_dir/spec_fixture"

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_exporter_fixture.py" \
  --output-dir "$exporter_out" \
  >"$out_dir/exporter.log"

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_export_spec.py" \
  --candidate-export-manifest "$exporter_out/candidate_real_export_manifest.json" \
  --output-dir "$spec_real_out" \
  >"$out_dir/spec_real.log"

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_export_spec.py" \
  --candidate-export-manifest "$exporter_out/candidate_fixture_export_manifest.json" \
  --output-dir "$spec_fixture_out" \
  >"$out_dir/spec_fixture.log"

python3 - "$exporter_out/manifest.json" "$spec_real_out/manifest.json" "$spec_fixture_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

exporter = json.loads(Path(sys.argv[1]).read_text())
spec_real = json.loads(Path(sys.argv[2]).read_text())
spec_fixture = json.loads(Path(sys.argv[3]).read_text())
fail_closed = exporter["fail_closed_probes"]["checks"]
real_status = spec_real["candidate_export_manifest_status"]
fixture_status = spec_fixture["candidate_export_manifest_status"]
checks = {
    "exporter_keep": exporter["decision_label"] == "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_READY_REAL_STILL_UNKNOWN",
    "real_export_unknown": exporter["real_export"]["status"] == "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING",
    "real_rows_both_accounts": set(exporter["real_export"]["account_counts"]) == {"b55", "ce25"},
    "real_rows_have_fee_usdc": "fee_usdc" in exporter["real_export"]["provided_fields"],
    "real_rows_missing_liquidity_role": "liquidity_role" in exporter["real_export"]["missing_fields"],
    "real_rows_missing_fair_probability": "fair_probability" in exporter["real_export"]["missing_fields"],
    "fixture_rows_both_accounts": set(exporter["fixture_export"]["account_counts"]) == {"b55", "ce25"},
    "fixture_rows_ready": exporter["fixture_export"]["status"] == "READY_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT",
    "negative_fee_fail_closed": bool(fail_closed["negative_fee_rejected"]),
    "bad_slug_fail_closed": bool(fail_closed["bad_slug_rejected"]),
    "sell_side_fail_closed": bool(fail_closed["sell_side_rejected"]),
    "spec_real_fail_closed": real_status["status"] == "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_FAIL_CLOSED",
    "spec_real_missing_role": "liquidity_role" in real_status["missing_fields"],
    "spec_real_missing_probability": "fair_probability" in real_status["missing_fields"],
    "spec_fixture_ready": fixture_status["status"] == "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_READY",
    "spec_fixture_still_not_real": spec_fixture["research_ranking"]["real_export_status"] == "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_MISSING",
    "promotion_never_passes": not exporter["promotion_gate"]["passed"] and not spec_fixture["promotion_gate"]["passed"],
    "no_side_effect_scope": not exporter["scope"]["shadow_started"] and not exporter["scope"]["orders_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"b55/ce25 activity entry exporter fixture smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b55_ce25_public_activity_entry_exporter_fixture_smoke",
    "decision_label": "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_SMOKE_PASS",
    "checks": checks,
    "exporter_manifest": sys.argv[1],
    "spec_real_manifest": sys.argv[2],
    "spec_fixture_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
