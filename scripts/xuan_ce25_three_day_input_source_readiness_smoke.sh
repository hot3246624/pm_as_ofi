#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_ce25_three_day_input_source_readiness_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_ce25_three_day_input_source_readiness.py >> "$log" 2>&1

python3 scripts/xuan_ce25_three_day_input_source_readiness.py \
  --output-dir "$out_dir/default" \
  >> "$log" 2>&1

python3 - "$out_dir/good_three_day.json" "$out_dir/good_source_manifest.json" "$out_dir/fixture_three_day.json" "$out_dir/fixture_source_manifest.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

def row(day, cohort, markets, pnl, pair_cost, residual):
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
        "wins": max(markets - 2, 0),
        "losses": 2,
        "avg_pair_cost": pair_cost,
        "residual_rate": residual,
        "old_redeem_contamination": 0.0,
        "post_window_same_condition_buy": 0.0,
        "rebate": 0.0,
    }

rows = []
for day in ("2026-05-21", "2026-05-22", "2026-05-23"):
    rows.extend(
        [
            row(day, "starter_eth_sol_5m15m", 25, 300.0, 0.84, 0.08),
            row(day, "core_btc_eth_sol_5m15m", 50, 850.0, 0.91, 0.07),
            row(day, "hard_loss_pair_cost_ge_1", 35, -600.0, 1.08, 0.18),
        ]
    )

good_three_day = {
    "schema_version": "ce25_three_day_pair_cost_validation_v1",
    "fixture": False,
    "daily_cohort_rows": rows,
}
fixture_three_day = dict(good_three_day)
fixture_three_day["fixture"] = True

good_three_day_path = Path(sys.argv[1])
good_source_path = Path(sys.argv[2])
fixture_three_day_path = Path(sys.argv[3])
fixture_source_path = Path(sys.argv[4])
good_three_day_path.write_text(json.dumps(good_three_day, indent=2, sort_keys=True) + "\n")
fixture_three_day_path.write_text(json.dumps(fixture_three_day, indent=2, sort_keys=True) + "\n")

def source_manifest(three_day_path, fixture=False):
    return {
        "artifact": "xuan_ce25_three_day_input_source_readiness_smoke_candidate",
        "contract_name": "ce25_three_day_input_source_readiness_v1",
        "account": "ce25",
        "fixture": fixture,
        "safe_offline_72h_export": True,
        "fetches_new_data": False,
        "enters_external_worktree": False,
        "starts_service": False,
        "uses_local_agg_service": False,
        "uses_shared_ws": False,
        "uses_ssh": False,
        "starts_shadow": False,
        "starts_canary_or_live": False,
        "reads_raw_replay_or_full_store": False,
        "changes_trading_behavior": False,
        "source_paths": [str(three_day_path)],
        "candidate_three_day_manifest_path": str(three_day_path),
        "validation_command": (
            "python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py "
            f"--candidate-three-day-manifest {three_day_path} "
            "--output-dir xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_score_SMOKE"
        ),
    }

good_source_path.write_text(json.dumps(source_manifest(good_three_day_path), indent=2, sort_keys=True) + "\n")
fixture_source_path.write_text(
    json.dumps(source_manifest(fixture_three_day_path, fixture=True), indent=2, sort_keys=True) + "\n"
)
PY

python3 scripts/xuan_ce25_three_day_input_source_readiness.py \
  --candidate-source-manifest "$out_dir/good_source_manifest.json" \
  --output-dir "$out_dir/good" \
  >> "$log" 2>&1

python3 scripts/xuan_ce25_three_day_input_source_readiness.py \
  --candidate-source-manifest "$out_dir/fixture_source_manifest.json" \
  --output-dir "$out_dir/fixture_fail" \
  >> "$log" 2>&1

python3 scripts/xuan_ce25_three_day_input_source_readiness.py \
  --candidate-source-manifest "/Users/hot/web3Scientist/poly_trans_research/data/exports/ce25_three_day_source_manifest.json" \
  --output-dir "$out_dir/external_fail" \
  >> "$log" 2>&1

python3 - "$out_dir/default/manifest.json" "$out_dir/good/manifest.json" "$out_dir/fixture_fail/manifest.json" "$out_dir/external_fail/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

default = json.loads(Path(sys.argv[1]).read_text())
good = json.loads(Path(sys.argv[2]).read_text())
fixture = json.loads(Path(sys.argv[3]).read_text())
external = json.loads(Path(sys.argv[4]).read_text())
checks = {
    "default_unknown": default["decision_label"] == "UNKNOWN_CE25_THREE_DAY_INPUT_SOURCE_NOT_IN_CURRENT_WORKTREE",
    "default_absent_blocker": "candidate_source_manifest_absent" in default["candidate_source_status"]["blockers"],
    "good_ready": good["decision_label"] == "KEEP_CE25_THREE_DAY_INPUT_SOURCE_READY",
    "good_source_ready": good["candidate_source_status"]["ready"] is True,
    "good_daily_shape_ready": good["candidate_source_status"]["candidate_three_day_manifest_shape"]["shape_ready"] is True,
    "fixture_fail_closed": fixture["decision"] == "UNKNOWN",
    "fixture_source_blocker": "candidate_source_is_fixture" in fixture["candidate_source_status"]["blockers"],
    "fixture_daily_blocker": "candidate_three_day_manifest_is_fixture" in fixture["candidate_source_status"]["blockers"],
    "external_fail_closed": external["decision"] == "UNKNOWN",
    "external_blocker": "outside_current_worktree" in external["candidate_source_status"]["blockers"],
    "promotion_never_passes": not default["promotion_gate"]["passed"] and not good["promotion_gate"]["passed"],
    "no_no_order_allowed": not default["research_ranking"]["no_order_diagnostic_allowed"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"ce25 three-day source readiness smoke failed: {failed}")
manifest = {
    "artifact": "xuan_ce25_three_day_input_source_readiness_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_CE25_THREE_DAY_INPUT_SOURCE_READINESS_SMOKE_PASS",
    "checks": checks,
    "default_manifest": sys.argv[1],
    "good_manifest": sys.argv[2],
    "fixture_fail_manifest": sys.argv[3],
    "external_fail_manifest": sys.argv[4],
}
Path(sys.argv[5]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
