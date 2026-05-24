#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_CE25_15M_MARKET_TAIL_GAP_AUDIT_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_15m_market_tail_gap_audit_smoke_$ts}"
mkdir -p "$out_dir"

real_out="$out_dir/real"
missing_dir="$out_dir/missing_deep_compare"
missing_out="$out_dir/missing"
mkdir -p "$missing_dir"

python3 "$ROOT/scripts/xuan_ce25_15m_market_tail_gap_audit.py" \
  --output-dir "$real_out" \
  >"$out_dir/real.log"

python3 "$ROOT/scripts/xuan_ce25_15m_market_tail_gap_audit.py" \
  --deep-compare-dir "$missing_dir" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$real_out/manifest.json" "$missing_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
missing = json.loads(Path(sys.argv[2]).read_text())
selected = real["selected_context"]
checks = {
    "real_keep": real["decision_label"] == "KEEP_CE25_15M_MARKET_TAIL_GAP_AUDIT_FINAL_TOUCH_TARGET_PAIR_TAIL_BROAD",
    "selected_last_touch_context": selected["context_key"] == "last_touch_to_expiry_bucket",
    "selected_last_30_60": selected["context_value"] == "last_touch_30_60s",
    "selected_sample_large_enough": selected["sample_markets"] >= 30,
    "selected_cash_drag_negative": selected["removed_summary"]["cash_pnl"] < -1000,
    "selected_residual_drag_negative": selected["removed_summary"]["residual_pnl_est"] < -1000,
    "pair_tail_still_broad": real["research_ranking"]["pair_tail_status"] == "BROAD_NOT_SOLVED_BY_SELECTED_CONTEXT",
    "public_rows_have_selected_sample": real["proxy_checks"]["public_activity_rows_have_selected_context_sample"],
    "no_order_not_allowed": not real["research_ranking"]["no_order_diagnostic_allowed"],
    "promotion_never_passes": not real["promotion_gate"]["passed"],
    "missing_fail_closed": missing["decision_label"] == "UNKNOWN_CE25_15M_MARKET_TAIL_GAP_AUDIT_INPUTS_MISSING",
    "missing_not_deployable": not missing["promotion_gate"]["deployable"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 15m market tail gap audit smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_15m_market_tail_gap_audit_smoke",
    "decision_label": "KEEP_CE25_15M_MARKET_TAIL_GAP_AUDIT_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "missing_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
