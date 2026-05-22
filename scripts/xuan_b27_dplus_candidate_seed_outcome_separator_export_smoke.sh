#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT="${ARTIFACT:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_export_smoke_${TS}}"

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
        "seed-yes-1",
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
        "seed-no-1",
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
        4.8,
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
  --candidate-seed-outcome-separator-output "$ARTIFACT/export/candidate_seed_outcome_separator.csv" \
  --candidate-seed-outcome-separator-profile control_seed_offset_max_120 \
  --candidate-seed-outcome-separator-sample-cap 10 \
  --candidate-seed-outcome-separator-per-day-cap 10 \
  --candidate-seed-outcome-separator-selection-mode residual_tail >/dev/null

ARTIFACT="$ARTIFACT" python3 - <<'PY'
import csv
import json
import os
from pathlib import Path

artifact = Path(os.environ["ARTIFACT"])
default_manifest = json.loads((artifact / "default" / "manifest.json").read_text())
export_manifest = json.loads((artifact / "export" / "manifest.json").read_text())
with (artifact / "export" / "candidate_seed_outcome_separator.csv").open(newline="") as handle:
    rows = list(csv.DictReader(handle))

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
    default_manifest["control_rerun"][field] == export_manifest["control_rerun"][field]
    for field in metric_fields
)
by_source = {row.get("source_seed_candidate_row_id"): row for row in rows}
residual_row = by_source.get("seed-yes-1", {})
paired_row = by_source.get("seed-no-1", {})
required_fields = [
    "source_seed_candidate_row_id",
    "day",
    "condition_id",
    "slug",
    "side",
    "offset_s",
    "trigger_px",
    "trigger_size",
    "seed_qty",
    "seed_px",
    "seed_cost",
    "official_fee_rate",
    "official_fee",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "pre_action_field_policy",
    "post_action_label_policy",
    "live_rule_safety_policy",
]
checks = {
    "default_export_disabled": default_manifest["candidate_seed_outcome_separator_export"]["enabled"] is False,
    "export_enabled": export_manifest["candidate_seed_outcome_separator_export"]["enabled"] is True,
    "default_behavior_metrics_unchanged": behavior_unchanged,
    "selected_rows_present": len(rows) == 2,
    "required_fields_present": all(all(row.get(field) not in (None, "") for field in required_fields) for row in rows),
    "external_ids_marked_unavailable": all(
        row.get("external_shadow_ids_available") == "False"
        and row.get("quote_intent_id") == ""
        and row.get("source_order_id") == ""
        and row.get("source_sequence_id") == ""
        for row in rows
    ),
    "labels_marked_post_action": all(row.get("outcome_labels_are_post_action") == "True" for row in rows),
    "post_action_label_policy_blocks_live_use": all(
        "must not be used as live pre-action gates" in row.get("post_action_label_policy", "")
        for row in rows
    ),
    "residual_tail_row_selected_first": rows[0].get("source_seed_candidate_row_id") == "seed-yes-1" if rows else False,
    "residual_qty_attributed": abs(float(residual_row.get("source_residual_qty") or 0.0) - 0.05) < 1e-9,
    "residual_cost_attributed": abs(float(residual_row.get("source_residual_cost") or 0.0) - 0.02225) < 1e-9,
    "pair_qty_attributed_to_both_sources": (
        abs(float(residual_row.get("source_pair_qty") or 0.0) - 1.2) < 1e-9
        and abs(float(paired_row.get("source_pair_qty") or 0.0) - 1.2) < 1e-9
    ),
    "pair_bucket_recorded": (
        residual_row.get("pair_outcome_bucket") == "paired_nonzero"
        and paired_row.get("pair_outcome_bucket") == "paired_nonzero"
    ),
    "residual_bucket_recorded": (
        residual_row.get("residual_tail_outcome_bucket") == "residual_nonzero"
        and paired_row.get("residual_tail_outcome_bucket") == "residual_zero"
    ),
    "pre_seed_inventory_context_present": (
        float(residual_row.get("pre_seed_open_qty") or 0.0) == 0.0
        and float(paired_row.get("pre_seed_opp_qty") or 0.0) == 1.25
        and float(paired_row.get("pre_seed_open_qty") or 0.0) == 1.25
    ),
    "risk_direction_recorded": (
        residual_row.get("source_risk_direction") == "risk_increasing"
        and paired_row.get("source_risk_direction") == "repair_or_pairing_improving"
    ),
}
manifest = {
    "artifact": "xuan_b27_dplus_candidate_seed_outcome_separator_export_smoke",
    "schema_version": "candidate_seed_outcome_separator_export_smoke_v1",
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "label": (
        "KEEP_CANDIDATE_SEED_OUTCOME_SEPARATOR_EXPORT_SMOKE_PASS"
        if all(checks.values())
        else "BLOCKED_CANDIDATE_SEED_OUTCOME_SEPARATOR_EXPORT_SMOKE_FAIL"
    ),
    "checks": checks,
    "candidate_seed_outcome_separator_summary": export_manifest["candidate_seed_outcome_separator_export"]["summary"],
    "candidate_seed_outcome_separator_csv": str(artifact / "export" / "candidate_seed_outcome_separator.csv"),
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
