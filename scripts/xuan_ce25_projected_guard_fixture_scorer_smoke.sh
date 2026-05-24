#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_CE25_PROJECTED_GUARD_FIXTURE_SCORER_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_projected_guard_fixture_scorer_smoke_$ts}"
mkdir -p "$out_dir"

good_out="$out_dir/good"
bad_fixture="$out_dir/bad_fixture.json"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_ce25_projected_guard_fixture_scorer.py" \
  --output-dir "$good_out" \
  >"$out_dir/good.log"

cat >"$bad_fixture" <<'JSON'
{
  "fixtures": [
    {
      "case": "bad_missing_cost",
      "condition_id": "bad",
      "asset": "ETH",
      "timeframe": "15m",
      "market_start_ts": "2026-05-24T10:00:00Z",
      "market_end_ts": "2026-05-24T10:15:00Z",
      "now_ts": "2026-05-24T10:07:00Z",
      "seconds_to_expiry": 480,
      "action_intent": "completion_cleanup",
      "outcome": "NO",
      "order_qty": 1.0,
      "order_price": 0.43,
      "estimated_fee_per_share": 0.0,
      "pre_yes_qty": 10.0,
      "pre_no_qty": 9.0,
      "pre_yes_actual_cost": 4.0,
      "expected_status": "ALLOW_PROJECTED_GUARD"
    }
  ]
}
JSON

python3 "$ROOT/scripts/xuan_ce25_projected_guard_fixture_scorer.py" \
  --fixture-file "$bad_fixture" \
  --output-dir "$bad_out" \
  >"$out_dir/bad.log"

python3 - "$good_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

good = json.loads(Path(sys.argv[1]).read_text())
bad = json.loads(Path(sys.argv[2]).read_text())
checks = {
    "good_keep": good["decision_label"] == "KEEP_CE25_PROJECTED_GUARD_FIXTURE_SCORER_READY",
    "good_all_fixture_expectations_met": good["score"]["checks"]["all_fixture_expectations_met"],
    "good_starter_allowed": good["score"]["checks"]["starter_fixture_allowed"],
    "good_core_allowed": good["score"]["checks"]["core_fixture_allowed"],
    "good_pair_cost_hard_kill": good["score"]["checks"]["pair_cost_hard_kill_blocked"],
    "good_residual_hard_kill": good["score"]["checks"]["residual_near_close_hard_kill_blocked"],
    "good_final60_block": good["score"]["checks"]["final60_initiation_blocked"],
    "good_missing_pre_action_fail_closed": good["score"]["checks"]["missing_pre_action_field_fail_closed"],
    "good_missing_projected_fail_closed": good["score"]["checks"]["missing_projected_field_fail_closed"],
    "good_retrospective_reference_only": good["retrospective_rule_bucket_status"]["status"] == "REFERENCE_ONLY_NOT_LIVE_CRITERIA",
    "good_no_order_not_allowed": not good["research_ranking"]["no_order_diagnostic_allowed"],
    "good_fixture_not_strategy_evidence": not good["research_ranking"]["fixture_strategy_evidence"],
    "promotion_never_passes": not good["promotion_gate"]["passed"] and not bad["promotion_gate"]["passed"],
    "bad_unknown": bad["decision_label"] == "UNKNOWN_CE25_PROJECTED_GUARD_FIXTURE_SCORER_INPUTS_INSUFFICIENT",
    "bad_expectation_failed": not bad["score"]["checks"]["all_fixture_expectations_met"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 projected guard fixture scorer smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_projected_guard_fixture_scorer_smoke",
    "decision_label": "KEEP_CE25_PROJECTED_GUARD_FIXTURE_SCORER_SMOKE_PASS",
    "checks": checks,
    "good_manifest": sys.argv[1],
    "bad_manifest": sys.argv[2],
}
Path(sys.argv[3]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
