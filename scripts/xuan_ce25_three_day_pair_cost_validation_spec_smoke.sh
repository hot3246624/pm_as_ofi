#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_ce25_three_day_pair_cost_validation_spec.py >> "$log" 2>&1

python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py \
  --output-dir "$out_dir/default" \
  >> "$log" 2>&1

python3 - "$out_dir/good_three_day.json" "$out_dir/bad_one_day.json" "$out_dir/bad_hard_loss.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

def row(day, cohort, markets, pnl, pair_cost, residual, wins, losses):
    return {
        "day_id": day,
        "account": "ce25",
        "cohort": cohort,
        "markets": markets,
        "buy_actual": markets * 100.0,
        "cohort_pnl_ex_rebate": pnl,
        "cohort_pnl_including_rebate": pnl,
        "pair_pnl": abs(pnl) + 100.0,
        "residual_pnl_est": pnl - (abs(pnl) + 100.0),
        "wins": wins,
        "losses": losses,
        "avg_pair_cost": pair_cost,
        "residual_rate": residual,
        "old_redeem_contamination": 0.0,
        "post_window_same_condition_buy": 0.0,
        "rebate": 0.0,
    }

good_rows = []
for idx, day in enumerate(("2026-05-21", "2026-05-22", "2026-05-23"), start=1):
    good_rows.extend(
        [
            row(day, "starter_eth_sol_5m15m", 24 + idx, 350.0 + idx, 0.84, 0.08, 20, 3),
            row(day, "core_btc_eth_sol_5m15m", 48 + idx, 900.0 + idx, 0.91, 0.07, 42, 5),
            row(day, "hard_loss_pair_cost_ge_1", 35 + idx, -700.0 - idx, 1.08, 0.18, 5, 30),
        ]
    )
good = {
    "schema_version": "ce25_three_day_pair_cost_validation_v1",
    "fixture": True,
    "daily_cohort_rows": good_rows,
}
bad_one_day = {
    "schema_version": "ce25_three_day_pair_cost_validation_v1",
    "fixture": True,
    "daily_cohort_rows": good_rows[:3],
}
bad_hard = json.loads(json.dumps(good))
for item in bad_hard["daily_cohort_rows"]:
    if item["day_id"] == "2026-05-22" and item["cohort"] == "hard_loss_pair_cost_ge_1":
        item["cohort_pnl_ex_rebate"] = 10.0
Path(sys.argv[1]).write_text(json.dumps(good, indent=2, sort_keys=True) + "\n")
Path(sys.argv[2]).write_text(json.dumps(bad_one_day, indent=2, sort_keys=True) + "\n")
Path(sys.argv[3]).write_text(json.dumps(bad_hard, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py \
  --candidate-three-day-manifest "$out_dir/good_three_day.json" \
  --output-dir "$out_dir/good" \
  >> "$log" 2>&1

python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py \
  --candidate-three-day-manifest "$out_dir/bad_one_day.json" \
  --output-dir "$out_dir/bad_one_day" \
  >> "$log" 2>&1

python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py \
  --candidate-three-day-manifest "$out_dir/bad_hard_loss.json" \
  --output-dir "$out_dir/bad_hard_loss" \
  >> "$log" 2>&1

python3 - "$out_dir/default/manifest.json" "$out_dir/good/manifest.json" "$out_dir/bad_one_day/manifest.json" "$out_dir/bad_hard_loss/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

default = json.loads(Path(sys.argv[1]).read_text())
good = json.loads(Path(sys.argv[2]).read_text())
bad_one = json.loads(Path(sys.argv[3]).read_text())
bad_hard = json.loads(Path(sys.argv[4]).read_text())
checks = {
    "default_spec_keep": default["decision_label"] == "KEEP_CE25_THREE_DAY_PAIR_COST_VALIDATION_SPEC_READY_REAL_INPUT_UNKNOWN",
    "default_real_input_missing": default["candidate_three_day_score"]["evidence_status"] == "UNKNOWN_THREE_DAY_INPUT_MISSING",
    "good_fixture_candidate_ready": good["candidate_three_day_score"]["candidate_status"] == "READY",
    "good_fixture_not_strategy_evidence": not good["candidate_three_day_score"]["strategy_evidence"],
    "good_three_days": good["candidate_three_day_score"]["day_count"] == 3,
    "bad_one_day_unknown": bad_one["candidate_three_day_score"]["candidate_status"] == "UNKNOWN",
    "bad_one_day_blocker": "fewer_than_3_days" in bad_one["candidate_three_day_score"]["blockers"],
    "bad_hard_unknown": bad_hard["candidate_three_day_score"]["candidate_status"] == "UNKNOWN",
    "bad_hard_blocker": "2026-05-22_hard_loss_pnl_not_negative" in bad_hard["candidate_three_day_score"]["blockers"],
    "promotion_never_passes": not default["promotion_gate"]["passed"] and not good["promotion_gate"]["passed"],
    "no_no_order_allowed": not default["research_ranking"]["no_order_diagnostic_allowed"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 three-day validation spec smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_three_day_pair_cost_validation_spec_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_CE25_THREE_DAY_PAIR_COST_VALIDATION_SPEC_SMOKE_PASS",
    "checks": checks,
    "default_manifest": sys.argv[1],
    "good_manifest": sys.argv[2],
    "bad_one_day_manifest": sys.argv[3],
    "bad_hard_loss_manifest": sys.argv[4],
}
Path(sys.argv[5]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
