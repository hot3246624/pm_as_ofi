#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_CE25_PROJECTED_GUARD_SPEC_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_projected_pair_cost_residual_guard_spec_smoke_$ts}"
mkdir -p "$out_dir"

real_out="$out_dir/real"
missing_dir="$out_dir/missing_deep_compare"
missing_out="$out_dir/missing"
mkdir -p "$missing_dir"

python3 "$ROOT/scripts/xuan_ce25_projected_pair_cost_residual_guard_spec.py" \
  --output-dir "$real_out" \
  >"$out_dir/real.log"

python3 "$ROOT/scripts/xuan_ce25_projected_pair_cost_residual_guard_spec.py" \
  --deep-compare-dir "$missing_dir" \
  --rule-report "$missing_dir/strategy_rules_report.md" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$real_out/manifest.json" "$missing_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

real = json.loads(Path(sys.argv[1]).read_text())
missing = json.loads(Path(sys.argv[2]).read_text())
buckets = real["retrospective_rule_buckets"]
contract = real["projected_guard_contract"]
checks = {
    "real_keep": real["decision_label"] == "KEEP_CE25_PROJECTED_PAIR_COST_RESIDUAL_GUARD_SPEC_READY",
    "ce25_starter_positive": buckets["ce25"]["starter_eth_sol_5m15m_pair_cost_lt_090_residual_lt_15"]["cohort_pnl"] > 1700,
    "ce25_core_positive": buckets["ce25"]["core_btc_eth_sol_5m15m_pair_cost_lt_095_residual_lt_10"]["cohort_pnl"] > 5000,
    "ce25_hard_loss_negative": buckets["ce25"]["hard_loss_btc_eth_sol_5m15m_pair_cost_ge_100"]["cohort_pnl"] < -5000,
    "starter_contract_threshold": contract["starter_guard"]["projected_pair_cost_lt"] == 0.90,
    "core_contract_threshold": contract["core_guard"]["projected_pair_cost_lt"] == 0.95,
    "hard_kill_pair_cost_present": any(rule["rule"] == "projected_pair_cost_gte_1_00" for rule in contract["hard_kill_rules"]),
    "required_projected_fields_present": "projected_pair_cost" in contract["required_pre_action_fields"] and "projected_residual_rate_on_bought_qty" in contract["required_pre_action_fields"],
    "no_order_not_allowed": not real["research_ranking"]["no_order_diagnostic_allowed"],
    "promotion_never_passes": not real["promotion_gate"]["passed"],
    "missing_fail_closed": missing["decision_label"] == "UNKNOWN_CE25_PROJECTED_GUARD_SPEC_INPUTS_MISSING",
    "missing_not_deployable": not missing["promotion_gate"]["deployable"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 projected guard spec smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_projected_pair_cost_residual_guard_spec_smoke",
    "decision_label": "KEEP_CE25_PROJECTED_PAIR_COST_RESIDUAL_GUARD_SPEC_SMOKE_PASS",
    "checks": checks,
    "real_manifest": sys.argv[1],
    "missing_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
