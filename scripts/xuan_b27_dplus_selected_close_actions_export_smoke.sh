#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT="${ARTIFACT:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_selected_close_actions_export_smoke_${TS}}"

cd "$ROOT"
mkdir -p "$ARTIFACT"/fixture/candidate_base "$ARTIFACT"/fixture/baseline "$ARTIFACT"/default "$ARTIFACT"/export

python3 -m py_compile scripts/xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py

ARTIFACT="$ARTIFACT" python3 - <<'PY'
import json
import os
from pathlib import Path

import duckdb

artifact = Path(os.environ["ARTIFACT"])
candidate_dir = artifact / "fixture" / "candidate_base"
baseline_dir = artifact / "fixture" / "baseline"
candidate_dir.mkdir(parents=True, exist_ok=True)
baseline_dir.mkdir(parents=True, exist_ok=True)

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
        "1",
        "fixture",
        "2026-05-18",
        "fixture-condition-1",
        "fixture-market",
        1_779_062_400_000,
        "2026-05-18T00:00:00Z",
        60.0,
        "YES",
        "NO",
        "YES",
        "same",
        "public_sell",
        0.50,
        20.0,
        0.90,
    ),
    (
        "2",
        "fixture",
        "2026-05-18",
        "fixture-condition-1",
        "fixture-market",
        1_779_062_410_000,
        "2026-05-18T00:00:10Z",
        65.0,
        "NO",
        "YES",
        "YES",
        "opposite",
        "public_sell",
        0.50,
        20.0,
        0.90,
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
core_fields = [
    "seed_actions",
    "active_markets",
    "pair_actions",
    "pair_qty",
    "weighted_pair_cost",
    "gross_pnl",
    "official_taker_fee",
    "fee_after_pnl",
    "stress100_worst_pnl",
    "worst_day_fee_after_pnl",
    "residual_qty_rate",
    "residual_cost_rate",
]
(baseline_dir / "RESULT_SUMMARY_MANIFEST.json").write_text(
    json.dumps({"core_metrics": {field: 0 for field in core_fields}}, indent=2, sort_keys=True) + "\n"
)
(baseline_dir / "COMPLIANCE_MANIFEST.json").write_text(
    json.dumps({"compliance_summary": {"fixture": True}}, indent=2, sort_keys=True) + "\n"
)
PY

python3 scripts/xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py \
  --candidate-base-dir "$ARTIFACT/fixture/candidate_base" \
  --baseline-result-dir "$ARTIFACT/fixture/baseline" \
  --output-dir "$ARTIFACT/default" >/dev/null

python3 scripts/xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py \
  --candidate-base-dir "$ARTIFACT/fixture/candidate_base" \
  --baseline-result-dir "$ARTIFACT/fixture/baseline" \
  --output-dir "$ARTIFACT/export" \
  --selected-close-actions-output "$ARTIFACT/export/selected_close_actions.csv" \
  --selected-close-actions-profile variant_late_repair_only_after_90 \
  --selected-close-actions-sample-cap 30 \
  --selected-close-actions-per-day-cap 2 >/dev/null

ARTIFACT="$ARTIFACT" python3 - <<'PY'
import csv
import json
import os
from pathlib import Path

artifact = Path(os.environ["ARTIFACT"])
default_manifest = json.loads((artifact / "default" / "manifest.json").read_text())
export_manifest = json.loads((artifact / "export" / "manifest.json").read_text())
with (artifact / "export" / "selected_close_actions.csv").open(newline="") as handle:
    rows = list(csv.DictReader(handle))

profile = "variant_late_repair_only_after_90"
metric_fields = [
    "seed_actions",
    "pair_actions",
    "pair_qty",
    "gross_buy_qty",
    "gross_buy_cost",
    "official_taker_fee",
    "pair_pnl",
    "residual_qty",
    "residual_cost",
]
behavior_unchanged = all(
    default_manifest["variant_late_repair90_rerun"][field] == export_manifest["variant_late_repair90_rerun"][field]
    for field in metric_fields
)
row = rows[0] if rows else {}
checks = {
    "default_export_disabled": default_manifest["selected_close_actions_export"]["enabled"] is False,
    "export_enabled": export_manifest["selected_close_actions_export"]["enabled"] is True,
    "default_behavior_metrics_unchanged": behavior_unchanged,
    "one_selected_close_action": len(rows) == 1,
    "profile_name": row.get("profile_name") == profile,
    "close_side_is_new_seed": row.get("close_side") == "NO",
    "residual_lot_side_is_prior_inventory": row.get("residual_lot_side") == "YES",
    "source_seed_candidate_row_id": row.get("source_seed_candidate_row_id") == "2",
    "residual_source_candidate_row_id": row.get("residual_lot_source_candidate_row_id") == "1",
    "fee_rate_source_present": bool(row.get("close_fee_rate_source")),
    "lookup_keys_present": all(row.get(field) for field in ["lookup_day", "lookup_condition_id", "lookup_close_ts_ms", "lookup_close_side"]),
    "close_refs_blank_until_replay_validation": all(
        row.get(field) == ""
        for field in ["close_book_l1_ref", "close_book_l2_ref", "close_trade_before_ref", "close_trade_after_ref"]
    ),
}
manifest = {
    "artifact": "xuan_b27_dplus_selected_close_actions_export_smoke",
    "schema_version": "selected_close_action_export_smoke_v1",
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "label": "KEEP_SELECTED_CLOSE_ACTION_EXPORT_SMOKE_PASS" if all(checks.values()) else "BLOCKED_SELECTED_CLOSE_ACTION_EXPORT_SMOKE_FAIL",
    "checks": checks,
    "selected_close_actions_summary": export_manifest["selected_close_actions_export"]["summary"],
    "selected_close_actions_csv": str(artifact / "export" / "selected_close_actions.csv"),
    "side_effects": {
        "fixture_duckdb_only": True,
        "replay_store_v2_queried": False,
        "raw_scanned": False,
        "full_completion_store_scanned": False,
        "ssh_started": False,
        "shared_ingress_connected": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
    },
}
(artifact / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
if manifest["decision"] != "KEEP":
    raise SystemExit(json.dumps(manifest, indent=2, sort_keys=True))
print(artifact / "manifest.json")
PY

python3 -m json.tool "$ARTIFACT/manifest.json" >/dev/null
