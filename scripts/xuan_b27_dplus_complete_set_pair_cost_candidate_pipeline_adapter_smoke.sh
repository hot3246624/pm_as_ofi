#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_complete_set_pair_cost_candidate_pipeline_adapter_smoke_$ts}"
mkdir -p "$out_dir/fixture/candidate_base" "$out_dir/adapter"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_complete_set_pair_cost_candidate_pipeline_adapter.py"
python3 -m py_compile "$script" scripts/xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

import duckdb

out = Path(sys.argv[1])
candidate_dir = out / "fixture" / "candidate_base"
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
    (
        "seed-yes-1",
        "fixture",
        "2026-05-18",
        "fixture-condition-1",
        "btc-updown-5m-fixture",
        1_779_062_400_000,
        "2026-05-18T00:00:00Z",
        20.0,
        "YES",
        "NO",
        "YES",
        "same",
        "public_sell",
        0.48,
        20.0,
        0.52,
    ),
    (
        "seed-no-1",
        "fixture",
        "2026-05-18",
        "fixture-condition-1",
        "btc-updown-5m-fixture",
        1_779_062_410_000,
        "2026-05-18T00:00:10Z",
        25.0,
        "NO",
        "YES",
        "YES",
        "opposite",
        "public_sell",
        0.45,
        20.0,
        0.55,
    ),
]
con.executemany("insert into candidate_base values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
con.close()
(candidate_dir / "CANDIDATE_BASE_MANIFEST.json").write_text(
    json.dumps(
        {
            "dataset_type": "fixture_candidate_base",
            "labels": ["20260518"],
            "days": ["2026-05-18"],
            "excluded_labels_or_days": ["2026-05-14", "2026-05-15", "2026-05-19"],
            "day_counts": {"2026-05-18": 2},
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
PY

python3 "$script" \
  --candidate-base-dir "$out_dir/fixture/candidate_base" \
  --output-dir "$out_dir/adapter" >> "$log" 2>&1

python3 - "$out_dir/adapter/complete_set_pair_cost_candidate_pipeline_adapter_score.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

score_path = Path(sys.argv[1])
score = json.loads(score_path.read_text())
checks = {
    "decision_keep": score["decision_label"] == "KEEP_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_ADAPTER_READY",
    "seed_context_coverage": score["field_coverage"]["selected_seed_context"]["overall"] == 1.0,
    "candidate_l1_coverage": score["field_coverage"]["candidate_l1_context"]["overall"] == 1.0,
    "context_rows_present": score["complete_set_pair_cost_context"]["seed_context_rows"] == 2,
    "realized_pair_context_present": score["complete_set_pair_cost_context"]["realized_source_pair_qty"] > 0,
    "csv_outputs_present": all(
        (score_path.parent / name).exists()
        for name in [
            "selected_seed_complete_set_context.csv",
            "realized_pair_cost_bucket_groups.csv",
            "l1_complete_set_edge_bucket_groups.csv",
        ]
    ),
    "promotion_gate_false": score["promotion_gate"]["passed"] is False,
    "no_side_effects": score["side_effects"]["ssh_started"] is False
    and score["side_effects"]["shadow_started"] is False
    and score["side_effects"]["orders_sent"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"complete-set candidate-pipeline adapter smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_complete_set_pair_cost_candidate_pipeline_adapter_smoke",
    "status": "PASS",
    "decision_label": "KEEP_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_ADAPTER_READY",
    "hypothesis": "The adapter can quantify complete-set pair-cost context from candidate_base/state-machine fields without remote or live side effects.",
    "outputs": {"score": str(score_path)},
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_ADAPTER_READY",
        "interpretation": "Adapter readiness only, not mechanism promotion."
    },
    "promotion_gate": {
        "passed": False,
        "status": "LOCAL_ADAPTER_ONLY_NOT_PROMOTION_EVIDENCE",
        "deployable": False,
        "private_truth_ready": False,
        "g2_canary_ready": False
    },
    "side_effects": {
        "network_started": False,
        "ssh_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "service_control_used": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False
    },
    "next_action": "Run adapter on allowed local candidate_base and use output to select or reject a non-price sample-preserving mechanism."
}
(score_path.parent.parent / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(score_path.parent.parent / "manifest.json")
PY

printf 'PASS D+ complete-set candidate-pipeline adapter smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
