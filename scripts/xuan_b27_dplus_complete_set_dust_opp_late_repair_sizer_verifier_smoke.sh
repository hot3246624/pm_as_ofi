#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_complete_set_dust_opp_late_repair_sizer_verifier_smoke_$ts}"
mkdir -p "$out_dir/fixture/candidate_base" "$out_dir/fixture/baseline"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_complete_set_dust_opp_late_repair_sizer_verifier.py"
python3 -m py_compile "$script" scripts/xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

import duckdb

out = Path(sys.argv[1])
candidate_dir = out / "fixture" / "candidate_base"
baseline_dir = out / "fixture" / "baseline"
db_path = candidate_dir / "candidate_base.duckdb"
if db_path.exists():
    db_path.unlink()
con = duckdb.connect(str(db_path))
con.execute(
    """
    create table candidate_base (
      candidate_row_id varchar,
      source_label varchar,
      day varchar,
      condition_id varchar,
      slug varchar,
      ts_ms bigint,
      ts_iso varchar,
      offset_s double,
      side varchar,
      opposite_side varchar,
      winner_side varchar,
      side_alignment varchar,
      candidate_reason varchar,
      public_trade_price double,
      public_trade_size double,
      l1_pair_ask double
    )
    """
)
rows = [
    ("no-seed", "fixture", "2026-05-18", "condition-1", "btc-updown-5m-fixture", 1_779_062_400_000, "2026-05-18T00:00:00Z", 20.0, "NO", "YES", "YES", "opposite", "public_sell", 0.45, 5.0, 0.55),
    ("yes-repair", "fixture", "2026-05-18", "condition-1", "btc-updown-5m-fixture", 1_779_062_410_000, "2026-05-18T00:00:10Z", 40.0, "YES", "NO", "YES", "same", "public_sell", 0.45, 4.4, 0.55),
    ("yes-late-dust-opp", "fixture", "2026-05-18", "condition-1", "btc-updown-5m-fixture", 1_779_062_500_000, "2026-05-18T00:01:40Z", 95.0, "YES", "NO", "YES", "same", "public_sell", 0.45, 20.0, 0.55)
]
con.executemany("insert into candidate_base values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
con.close()
candidate_dir.mkdir(parents=True, exist_ok=True)
(candidate_dir / "CANDIDATE_BASE_MANIFEST.json").write_text(json.dumps({
    "dataset_type": "fixture_candidate_base",
    "labels": ["20260518"],
    "days": ["2026-05-18"],
    "excluded_labels_or_days": ["2026-05-14", "2026-05-15", "2026-05-19"],
    "day_counts": {"2026-05-18": len(rows)}
}, indent=2, sort_keys=True) + "\n")
baseline_dir.mkdir(parents=True, exist_ok=True)
(baseline_dir / "RESULT_SUMMARY_MANIFEST.json").write_text(json.dumps({"artifact": "fixture"}, indent=2) + "\n")
(baseline_dir / "COMPLIANCE_MANIFEST.json").write_text(json.dumps({
    "decision": "KEEP_FIXTURE",
    "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False}
}, indent=2, sort_keys=True) + "\n")
PY

set +e
python3 "$script" \
  --candidate-base-dir "$out_dir/fixture/candidate_base" \
  --baseline-result-dir "$out_dir/fixture/baseline" \
  --output "$out_dir/score.json" >> "$log" 2>&1
status=$?
set -e
test "$status" -eq 0 || test "$status" -eq 1

python3 - "$out_dir/score.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

score_path = Path(sys.argv[1])
score = json.loads(score_path.read_text())
variant = score["variant"]
control = score["control"]
checks = {
    "decision_known": score["decision"] in {"KEEP", "UNKNOWN", "DISCARD"},
    "profile_default_off_field_present": "complete_set_dust_opp_late_repair_sizer" in score["mechanism"]["name"] or score["mechanism"]["default_off"] is True,
    "candidate_count_positive": variant.get("seed_complete_set_dust_opp_late_repair_sizer_candidate", 0) > 0,
    "cap_count_positive": variant.get("seed_qty_cap_complete_set_dust_opp_late_repair_sizer", 0) > 0,
    "control_no_cap_count": control.get("seed_qty_cap_complete_set_dust_opp_late_repair_sizer", 0) == 0,
    "fee_formula_recorded": score["mechanism"]["official_fee_formula"] == "shares * fee_rate * price * (1 - price)",
    "promotion_gate_false": score["promotion_gate"]["passed"] is False,
    "no_side_effects": score["side_effects"]["ssh_started"] is False
    and score["side_effects"]["shadow_started"] is False
    and score["side_effects"]["orders_sent"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"complete-set dust-opp sizer smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_complete_set_dust_opp_late_repair_sizer_verifier_smoke",
    "status": "PASS",
    "decision_label": "KEEP_COMPLETE_SET_DUST_OPP_LATE_REPAIR_SIZER_VERIFIER_SMOKE_PASS",
    "outputs": {"score": str(score_path)},
    "checks": checks,
    "promotion_gate": {
        "passed": False,
        "status": "SMOKE_ONLY_NOT_PROMOTION_EVIDENCE",
        "deployable": False,
        "private_truth_ready": False,
        "g2_canary_ready": False
    },
    "side_effects": score["side_effects"]
}
(score_path.parent / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(score_path.parent / "manifest.json")
PY

printf 'PASS D+ complete-set dust-opp late repair sizer verifier smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
