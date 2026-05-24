#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_B55_CE25_FAIR_PROBABILITY_MODEL_SPEC_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b55_ce25_fair_probability_model_spec_smoke_$ts}"
mkdir -p "$out_dir"

missing_out="$out_dir/missing"
complete_fixture="$out_dir/complete_fixture_manifest.json"
complete_out="$out_dir/complete"
bad_fixture="$out_dir/bad_fixture_manifest.json"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_b55_ce25_fair_probability_model_spec.py" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$complete_fixture" "$bad_fixture" <<'PY'
import json
import sys
from pathlib import Path

required_fields = [
    "account",
    "trade_id",
    "condition_id",
    "market_slug",
    "asset",
    "timeframe",
    "market_start_ts",
    "market_end_ts",
    "quote_ts",
    "entry_delta_s",
    "time_to_expiry_s",
    "outcome",
    "polymarket_price",
    "size",
    "gross_usdc",
    "fee_usdc",
    "actual_usdc",
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
complete = {
    "artifact": "b55_ce25_fair_probability_model_fixture",
    "fixture": True,
    "provided_fields": required_fields,
    "accounts_covered": ["b55", "ce25"],
    "assets_covered": ["BTC", "ETH"],
    "timeframes_covered": ["15m", "1h_or_named"],
    "sample_policy": {
        "entry_delta_s_min": -900,
        "entry_delta_s_max": -60,
        "polymarket_price_min": 0.35,
        "polymarket_price_max": 0.90,
    },
    "model": {
        "model_name": "fixture_entry_boundary_normal_probability",
        "model_version": "fixture-v1",
        "probability_output_basis": "entry_win_probability",
        "uses_volatility_or_uncertainty_input": True,
    },
    "quality_gates": {
        "min_source_count": 3,
        "max_source_age_ms": 1500,
        "max_source_dispersion_bps": 8.0,
        "max_probability_uncertainty": 0.03,
        "min_edge_after_fee_and_uncertainty": 0.02,
    },
}
bad = dict(complete)
bad["provided_fields"] = [field for field in required_fields if field not in {"fair_probability", "fair_probability_uncertainty"}]
bad["model"] = {
    "model_name": "bad_fixture",
    "model_version": "fixture-v0",
    "probability_output_basis": "direction_label",
    "uses_volatility_or_uncertainty_input": False,
}
Path(sys.argv[1]).write_text(json.dumps(complete, indent=2, sort_keys=True) + "\n")
Path(sys.argv[2]).write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
PY

python3 "$ROOT/scripts/xuan_b55_ce25_fair_probability_model_spec.py" \
  --candidate-fair-probability-manifest "$complete_fixture" \
  --output-dir "$complete_out" \
  >"$out_dir/complete.log"

python3 "$ROOT/scripts/xuan_b55_ce25_fair_probability_model_spec.py" \
  --candidate-fair-probability-manifest "$bad_fixture" \
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
    "missing_spec_keep": missing["decision_label"] == "KEEP_B55_CE25_FAIR_PROBABILITY_MODEL_SPEC_READY",
    "missing_real_inputs_unknown": missing["research_ranking"]["real_input_status"] == "UNKNOWN_REAL_FAIR_PROBABILITY_INPUTS_MISSING",
    "missing_candidate_absent": missing["candidate_manifest_status"]["status"] == "MISSING_CANDIDATE_FAIR_PROBABILITY_MANIFEST",
    "complete_fixture_ready": complete["candidate_manifest_status"]["status"] == "CANDIDATE_FAIR_PROBABILITY_MANIFEST_READY",
    "complete_fixture_not_real_inputs": complete["research_ranking"]["real_input_status"] == "UNKNOWN_REAL_FAIR_PROBABILITY_INPUTS_MISSING",
    "bad_fixture_fail_closed": bad["candidate_manifest_status"]["status"] == "CANDIDATE_MANIFEST_FAIL_CLOSED",
    "bad_fixture_missing_fields": "fair_probability" in bad["candidate_manifest_status"]["missing_fields"],
    "promotion_never_passes": not missing["promotion_gate"]["passed"] and not complete["promotion_gate"]["passed"],
    "no_side_effect_scope": not missing["scope"]["shadow_started"] and not missing["scope"]["orders_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"b55/ce25 fair-probability spec smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b55_ce25_fair_probability_model_spec_smoke",
    "decision_label": "KEEP_B55_CE25_FAIR_PROBABILITY_MODEL_SPEC_SMOKE_PASS",
    "checks": checks,
    "missing_manifest": sys.argv[1],
    "complete_manifest": sys.argv[2],
    "bad_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
