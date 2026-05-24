#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_late_window_pair_arb_proxy_gate_smoke_$ts}"
mkdir -p "$out_dir"

real_out="$out_dir/real"
missing_dir="$out_dir/missing_deep_compare"
missing_out="$out_dir/missing"
mkdir -p "$missing_dir"

python3 "$ROOT/scripts/xuan_ce25_late_window_pair_arb_proxy_gate.py" \
  --output-dir "$real_out" \
  >"$out_dir/real.log"

python3 "$ROOT/scripts/xuan_ce25_late_window_pair_arb_proxy_gate.py" \
  --deep-compare-dir "$missing_dir" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$real_out/manifest.json" "$missing_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
missing = json.loads(Path(sys.argv[2]).read_text())
primary = real["group_metrics"]["ce25_primary_15m_btc_eth_sol"]
negative = real["group_metrics"]["ce25_btc_5m_negative_control"]
checks = {
    "real_keep": real["decision_label"] == "KEEP_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_READY_TAIL_RESIDUAL_BLOCKED",
    "selected_primary_group": real["research_ranking"]["selected_public_proxy"] == "ce25_btc_eth_sol_15m_late_window",
    "primary_pair_positive": primary["cohort_metrics"]["pair_pnl_base"] > 0,
    "primary_cohort_positive": primary["cohort_metrics"]["cohort_cash_ex_rebate"] > 0,
    "primary_tail_blocker_visible": primary["market_metrics"]["actual_pair_cost_p90"] > 1.0,
    "primary_residual_drag_visible": primary["cohort_metrics"]["residual_pnl_est_base"] < 0,
    "btc_5m_negative_control_negative": negative["cohort_metrics"]["cohort_cash_ex_rebate"] < 0,
    "b55_strength_control": real["research_ranking"]["strength_control"] == "b55",
    "no_order_not_allowed": not real["research_ranking"]["no_order_diagnostic_allowed"],
    "promotion_never_passes": not real["promotion_gate"]["passed"],
    "missing_fail_closed": missing["decision_label"] == "UNKNOWN_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_INPUTS_MISSING",
    "missing_not_deployable": not missing["promotion_gate"]["deployable"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 late-window pair-arb proxy gate smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_late_window_pair_arb_proxy_gate_smoke",
    "decision_label": "KEEP_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "missing_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
