#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORT_SPEC_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b55_ce25_public_activity_entry_export_spec_smoke_$ts}"
mkdir -p "$out_dir"

missing_out="$out_dir/missing"
complete_fixture="$out_dir/complete_export_manifest.json"
complete_out="$out_dir/complete"
bad_fixture="$out_dir/bad_export_manifest.json"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_export_spec.py" \
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
    "fee_usdc_derivation": True,
    "liquidity_role_truth_source": True,
    "fair_source_snapshot_rows": True,
    "volatility_rows": True,
    "probability_model_output": True,
}
complete = {
    "artifact": "xuan_b55_ce25_public_activity_entry_export_spec_complete_fixture",
    "decision_label": "KEEP_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT_READY",
    "fixture": True,
    "provided_fields": required,
    "accounts_covered": ["b55", "ce25"],
    "joins_declared": joins,
    "fee_usdc_derivation_policy": {
        "actual_usdc_source": "usdcSize",
        "gross_usdc_formula": "polymarket_price * size",
        "fee_usdc_formula": "actual_usdc - gross_usdc",
        "usdc_size_basis_declared": True,
    },
    "liquidity_role_truth_source": {
        "per_trade_role_join_declared": True,
        "uses_rebate_or_fee_inference": False,
    },
    "fair_source_snapshot_policy": {
        "safe_export_only": True,
        "max_join_abs_delta_ms": 1000,
        "min_source_count": 3,
    },
    "probability_model_output": {
        "probability_output_basis": "purchased_outcome_win_probability",
        "uncertainty_declared": True,
    },
}
bad_fields = [
    field
    for field in required
    if field not in {"liquidity_role", "fair_probability", "fair_probability_uncertainty", "edge_after_fee_and_uncertainty"}
]
bad_joins = dict(joins)
bad_joins["liquidity_role_truth_source"] = False
bad_joins["probability_model_output"] = False
bad = {
    "artifact": "xuan_b55_ce25_public_activity_entry_export_spec_bad_fixture",
    "decision_label": "UNKNOWN_BAD_EXPORT_MISSING_ROLE_AND_PROBABILITY",
    "fixture": True,
    "provided_fields": bad_fields,
    "accounts_covered": ["b55", "ce25"],
    "joins_declared": bad_joins,
    "fee_usdc_derivation_policy": {
        "actual_usdc_source": "usdcSize",
        "gross_usdc_formula": "polymarket_price * size",
        "fee_usdc_formula": "actual_usdc - gross_usdc",
        "usdc_size_basis_declared": True,
    },
    "liquidity_role_truth_source": {
        "per_trade_role_join_declared": False,
        "uses_rebate_or_fee_inference": True,
    },
    "fair_source_snapshot_policy": {
        "safe_export_only": True,
        "max_join_abs_delta_ms": 1000,
        "min_source_count": 3,
    },
    "probability_model_output": {
        "probability_output_basis": "direction_label",
        "uncertainty_declared": False,
    },
}
complete_path.write_text(json.dumps(complete, indent=2, sort_keys=True) + "\n")
bad_path.write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
PY

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_export_spec.py" \
  --candidate-export-manifest "$complete_fixture" \
  --output-dir "$complete_out" \
  >"$out_dir/complete.log"

python3 "$ROOT/scripts/xuan_b55_ce25_public_activity_entry_export_spec.py" \
  --candidate-export-manifest "$bad_fixture" \
  --output-dir "$bad_out" \
  >"$out_dir/bad.log"

python3 - "$missing_out/manifest.json" "$complete_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

missing = json.loads(Path(sys.argv[1]).read_text())
complete = json.loads(Path(sys.argv[2]).read_text())
bad = json.loads(Path(sys.argv[3]).read_text())
contract = missing["public_activity_entry_export_contract"]
checks = {
    "missing_spec_keep": missing["decision_label"] == "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORT_SPEC_READY",
    "missing_real_export_unknown": missing["research_ranking"]["real_export_status"] == "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_MISSING",
    "missing_candidate_absent": missing["candidate_export_manifest_status"]["status"] == "MISSING_CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST",
    "contract_has_fee_policy": contract["fee_usdc_derivation_policy_v1"]["fee_usdc_formula"] == "actual_usdc - gross_usdc",
    "contract_has_role_truth_rejections": "fee magnitude guessed into maker/taker" in contract["liquidity_role_truth_source_v1"]["rejected_sources"],
    "contract_has_source_join_tolerance": contract["fair_source_snapshot_rows_v1"]["max_join_abs_delta_ms"] == 1500.0,
    "complete_fixture_ready": complete["candidate_export_manifest_status"]["status"] == "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_READY",
    "complete_fixture_not_real_export": complete["research_ranking"]["real_export_status"] == "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_MISSING",
    "bad_fixture_fail_closed": bad["candidate_export_manifest_status"]["status"] == "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_FAIL_CLOSED",
    "bad_fixture_missing_role": "liquidity_role" in bad["candidate_export_manifest_status"]["missing_fields"],
    "bad_fixture_missing_probability": "fair_probability" in bad["candidate_export_manifest_status"]["missing_fields"],
    "promotion_never_passes": not missing["promotion_gate"]["passed"] and not complete["promotion_gate"]["passed"],
    "no_side_effect_scope": not missing["scope"]["shadow_started"] and not missing["scope"]["orders_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"b55/ce25 public activity entry export spec smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b55_ce25_public_activity_entry_export_spec_smoke",
    "decision_label": "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORT_SPEC_SMOKE_PASS",
    "checks": checks,
    "missing_manifest": sys.argv[1],
    "complete_manifest": sys.argv[2],
    "bad_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
