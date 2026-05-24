#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_B55_CE25_NON_FIXTURE_INPUT_SOURCE_REVIEW_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b55_ce25_non_fixture_input_source_review_smoke_$ts}"
mkdir -p "$out_dir"

missing_out="$out_dir/missing"
complete_candidate="$out_dir/complete_candidate_source_manifest.json"
complete_out="$out_dir/complete"
bad_candidate="$out_dir/bad_candidate_source_manifest.json"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_b55_ce25_non_fixture_input_source_review.py" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$complete_candidate" "$bad_candidate" <<'PY'
import json
import sys
from pathlib import Path

required = [
    "liquidity_role",
    "boundary_price",
    "fair_spot_mid",
    "source_count",
    "source_names",
    "source_dispersion_bps",
    "max_source_age_ms",
    "volatility_lookback_s",
    "volatility_bps",
    "fair_probability",
    "fair_probability_uncertainty",
    "edge_after_fee_and_uncertainty",
    "model_name",
    "model_version",
]
joins = {
    "public_activity_entry_rows": True,
    "liquidity_role_truth_source": True,
    "fair_source_snapshot_rows": True,
    "volatility_rows": True,
    "probability_model_output": True,
}
complete = {
    "artifact": "xuan_b55_ce25_non_fixture_input_source_review_complete_smoke_candidate",
    "decision_label": "KEEP_SMOKE_NON_FIXTURE_INPUT_SOURCE_READY",
    "fixture": False,
    "safe_non_service_export": True,
    "fetches_new_data": False,
    "starts_service": False,
    "uses_shared_ws": False,
    "uses_ssh": False,
    "reads_forbidden_store": False,
    "accounts_covered": ["b55", "ce25"],
    "provided_fields": required,
    "joins_declared": joins,
    "source_paths": ["/tmp/xuan_smoke_safe_non_service_export.json"],
}
bad = {
    "artifact": "xuan_b55_ce25_non_fixture_input_source_review_bad_smoke_candidate",
    "decision_label": "UNKNOWN_SMOKE_INPUT_SOURCE_MISSING_ROLE_PROBABILITY",
    "fixture": False,
    "safe_non_service_export": True,
    "fetches_new_data": False,
    "starts_service": False,
    "uses_shared_ws": False,
    "uses_ssh": False,
    "reads_forbidden_store": False,
    "accounts_covered": ["b55", "ce25"],
    "provided_fields": [field for field in required if field not in {"liquidity_role", "fair_probability"}],
    "joins_declared": dict(joins, liquidity_role_truth_source=False, probability_model_output=False),
    "source_paths": ["/tmp/xuan_smoke_safe_non_service_export.json"],
}
Path(sys.argv[1]).write_text(json.dumps(complete, indent=2, sort_keys=True) + "\n")
Path(sys.argv[2]).write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
PY

python3 "$ROOT/scripts/xuan_b55_ce25_non_fixture_input_source_review.py" \
  --candidate-input-source-manifest "$complete_candidate" \
  --output-dir "$complete_out" \
  >"$out_dir/complete.log"

python3 "$ROOT/scripts/xuan_b55_ce25_non_fixture_input_source_review.py" \
  --candidate-input-source-manifest "$bad_candidate" \
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
    "missing_unknown": missing["decision_label"] == "UNKNOWN_B55_CE25_NON_FIXTURE_INPUT_SOURCE_MISSING",
    "missing_stops_before_diagnostic": missing["research_ranking"]["stop_before_no_order_diagnostic"],
    "missing_names_liquidity_role": "liquidity_role" in missing["research_ranking"]["exact_missing_inputs"],
    "missing_names_fair_probability": "fair_probability" in missing["research_ranking"]["exact_missing_inputs"],
    "public_inputs_have_no_role": not missing["public_input_observation"]["b55"]["has_liquidity_role_truth"],
    "public_exports_have_no_probability": not missing["public_export_observation"]["ce25"]["has_non_fixture_fair_probability_source"],
    "complete_candidate_keep": complete["decision_label"] == "KEEP_B55_CE25_NON_FIXTURE_INPUT_SOURCE_READY",
    "complete_candidate_ready": complete["candidate_input_source_status"]["status"] == "CANDIDATE_NON_FIXTURE_INPUT_SOURCE_READY",
    "bad_candidate_unknown": bad["decision_label"] == "UNKNOWN_B55_CE25_NON_FIXTURE_INPUT_SOURCE_MISSING",
    "bad_candidate_fail_closed": bad["candidate_input_source_status"]["status"] == "CANDIDATE_NON_FIXTURE_INPUT_SOURCE_FAIL_CLOSED",
    "bad_missing_role": "liquidity_role" in bad["candidate_input_source_status"]["missing_fields"],
    "bad_missing_probability": "fair_probability" in bad["candidate_input_source_status"]["missing_fields"],
    "promotion_never_passes": not missing["promotion_gate"]["passed"] and not complete["promotion_gate"]["passed"],
    "no_side_effect_scope": not missing["scope"]["shadow_started"] and not missing["scope"]["orders_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"b55/ce25 non-fixture input source review smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b55_ce25_non_fixture_input_source_review_smoke",
    "decision_label": "KEEP_B55_CE25_NON_FIXTURE_INPUT_SOURCE_REVIEW_SMOKE_PASS",
    "checks": checks,
    "missing_manifest": sys.argv[1],
    "complete_manifest": sys.argv[2],
    "bad_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
