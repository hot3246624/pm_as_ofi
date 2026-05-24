#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_PUBLIC_PROFILE_NEXT_LEAD_GATE_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_public_profile_next_lead_gate_smoke_$ts}"
mkdir -p "$out_dir"

real_out="$out_dir/real"
missing_dir="$out_dir/missing_deep_compare"
missing_out="$out_dir/missing"
mkdir -p "$missing_dir"

python3 "$ROOT/scripts/xuan_public_profile_next_lead_gate.py" \
  --output-dir "$real_out" \
  >"$out_dir/real.log"

python3 "$ROOT/scripts/xuan_public_profile_next_lead_gate.py" \
  --deep-compare-dir "$missing_dir" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$real_out/manifest.json" "$missing_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
missing = json.loads(Path(sys.argv[2]).read_text())
checks = {
    "real_keep": real["decision_label"] == "KEEP_PUBLIC_PROFILE_NEXT_LEAD_GATE_CE25_MIMICABILITY_SELECTED_B55_CONTROL",
    "ce25_mimicability_selected": real["research_ranking"]["mimicability_target"] == "ce25",
    "b55_strength_selected": real["research_ranking"]["strength_lead"] == "b55",
    "b55_control_account": real["research_ranking"]["control_account"] == "b55",
    "no_order_still_blocked": not real["research_ranking"]["no_order_diagnostic_allowed"],
    "ce25_gates_all_pass": all(real["mimicability_ranking"]["gates"]["ce25"].values()),
    "b55_not_mimicability_clean": bool(real["mimicability_ranking"]["blockers"]["b55"]),
    "ce25_proxy_names_core_15m": real["ce25_first_pass_proxy"]["primary_subgroup"] == "ce25 BTC/ETH/SOL 15m late-window cohort",
    "promotion_never_passes": not real["promotion_gate"]["passed"],
    "missing_fail_closed": missing["decision_label"] == "UNKNOWN_PUBLIC_PROFILE_NEXT_LEAD_GATE_DEEP_COMPARE_INPUTS_MISSING",
    "missing_stops_before_diagnostic": missing["research_ranking"]["stop_before_no_order_diagnostic"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"public profile next-lead gate smoke failed: {failed}")
manifest = {
    "artifact": "xuan_public_profile_next_lead_gate_smoke",
    "decision_label": "KEEP_PUBLIC_PROFILE_NEXT_LEAD_GATE_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "missing_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
