#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT="${ARTIFACT:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_strict_rescue_close_response_adapter_smoke_${TS}}"
STORE="$ARTIFACT/replay_store_fixture"
FIXTURES="$ARTIFACT/fixtures"
RESPONSE="$ARTIFACT/response"
SCORE="$ARTIFACT/score"

cd "$ROOT"
mkdir -p "$STORE" "$FIXTURES" "$RESPONSE" "$SCORE"

python3 -m py_compile \
  scripts/xuan_b27_dplus_replay_store_truth_validation_response_builder.py \
  scripts/xuan_b27_dplus_truth_validation_response_action_scorer.py

ARTIFACT="$ARTIFACT" STORE="$STORE" FIXTURES="$FIXTURES" python3 - <<'PY'
import csv
import json
import os
from pathlib import Path

import duckdb

artifact = Path(os.environ["ARTIFACT"])
store = Path(os.environ["STORE"])
fixtures = Path(os.environ["FIXTURES"])
store.mkdir(parents=True, exist_ok=True)
fixtures.mkdir(parents=True, exist_ok=True)
db_path = store / "store.duckdb"
if db_path.exists():
    db_path.unlink()
con = duckdb.connect(str(db_path))
con.execute(
    """
    create table market_meta (
      source_row_id bigint, condition_id varchar, slug varchar, start_ms bigint, end_ms bigint,
      yes_token_id varchar, no_token_id varchar, tick_size double, day varchar
    )
    """
)
con.execute(
    """
    create table md_book_l1 (
      source_row_id bigint, condition_id varchar, recv_ms bigint, source_ts_ms bigint,
      yes_bid_px double, yes_ask_px double, no_bid_px double, no_ask_px double,
      yes_bid_sz double, yes_ask_sz double, no_bid_sz double, no_ask_sz double,
      source_kind varchar, day varchar
    )
    """
)
con.execute(
    """
    create table md_book_l2 (
      source_row_id bigint, condition_id varchar, recv_ms bigint, source_ts_ms bigint,
      market_side varchar, depth int, bid1_px double, bid1_sz double, ask1_px double, ask1_sz double,
      source_kind varchar, day varchar
    )
    """
)
con.execute(
    """
    create table md_trades (
      source_row_id bigint, condition_id varchar, trade_ts_ms bigint, recv_ms bigint, trade_id varchar,
      market_side varchar, taker_side varchar, price double, size double, source_quality varchar, day varchar
    )
    """
)
con.execute(
    """
    create table settlement_records (
      source_row_id bigint, condition_id varchar, official_outcome varchar, winner_side varchar,
      settle_ms bigint, resolution_source varchar, day varchar
    )
    """
)

day = "2026-05-18"
condition_id = "fixture-condition-1"
close_ts_ms = 1_779_062_410_000
con.execute(
    "insert into market_meta values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [11, condition_id, "fixture-market", close_ts_ms - 60_000, close_ts_ms + 60_000, "yes", "no", 0.01, day],
)
con.execute(
    "insert into md_book_l1 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [21, condition_id, close_ts_ms - 1, close_ts_ms - 1, 0.44, 0.45, 0.44, 0.45, 10.0, 10.0, 10.0, 10.0, "fixture", day],
)
con.execute(
    "insert into md_book_l2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [31, condition_id, close_ts_ms - 1, close_ts_ms - 1, "NO", 1, 0.44, 10.0, 0.45, 10.0, "fixture", day],
)
con.execute(
    "insert into md_trades values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [41, condition_id, close_ts_ms - 500, close_ts_ms - 500, "trade-before", "NO", "BUY", 0.445, 1.25, "fixture", day],
)
con.execute(
    "insert into md_trades values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [42, condition_id, close_ts_ms + 500, close_ts_ms + 500, "trade-after", "NO", "BUY", 0.445, 1.25, "fixture", day],
)
con.execute(
    "insert into settlement_records values (?, ?, ?, ?, ?, ?, ?)",
    [51, condition_id, "YES", "YES", close_ts_ms + 3_600_000, "fixture", day],
)
con.close()

(store / "REPLAY_STORE_V2_MANIFEST.json").write_text(
    json.dumps(
        {
            "schema_version": "replay_store_v2",
            "source_truth_ready": True,
            "private_truth_ready": False,
            "public_or_proxy_truth_only": True,
            "days": [day],
            "private_table_counts": {
                "own_order_events": 0,
                "own_fill_events": 0,
                "own_inventory_events": 0,
                "user_ws_log": 0,
            },
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)

row = {
    "schema_version": "selected_close_action_export_v1",
    "profile_name": "variant_late_repair_only_after_90",
    "validation_request_id": "strict_rescue_close_selected_request_v1:fixture",
    "day": day,
    "condition_id": condition_id,
    "slug": "fixture-market",
    "close_action_id": "close:variant_late_repair_only_after_90:2026-05-18:fixture-condition-1:1",
    "close_sequence": 1,
    "source_seed_action_id": "2",
    "source_seed_candidate_row_id": "2",
    "source_seed_ts_ms": close_ts_ms,
    "source_seed_side": "NO",
    "source_seed_qty": 1.25,
    "source_seed_px": 0.445,
    "residual_lot_id": "residual_lot:2026-05-18:1",
    "residual_lot_source_seed_action_id": "1",
    "residual_lot_source_candidate_row_id": "1",
    "residual_lot_side": "YES",
    "residual_lot_remaining_qty_before_close": 1.25,
    "residual_lot_remaining_cost_before_close": 0.55625,
    "close_ts_ms": close_ts_ms,
    "close_side": "NO",
    "close_qty": 1.25,
    "close_px": 0.445,
    "close_cost": 0.55625,
    "close_fee_rate": 0.07,
    "close_fee_rate_source": "state_machine_result_config:fixture:official_fee_rate=0.07",
    "close_official_fee": 0.0216103125,
    "expected_close_decision": "ACCEPTED_SELECTED_CLOSE_ACTION",
    "expected_close_status": "SOURCE_TRUTH_RESEARCH_ONLY_PENDING",
    "close_reason": "pair_inventory_new_seed_offsets_residual",
    "paired_qty": 1.25,
    "paired_yes_source_action_id": "1",
    "paired_no_source_action_id": "2",
    "paired_yes_px": 0.445,
    "paired_no_px": 0.445,
    "paired_cost": 0.89,
    "pair_pnl_delta": 0.1375,
    "lookup_day": day,
    "lookup_condition_id": condition_id,
    "lookup_close_ts_ms": close_ts_ms,
    "lookup_close_side": "NO",
    "close_book_l1_ref": "",
    "close_book_l2_ref": "",
    "close_trade_before_ref": "",
    "close_trade_after_ref": "",
}
csv_path = fixtures / "selected_close_actions.csv"
with csv_path.open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
    writer.writeheader()
    writer.writerow(row)
(fixtures / "selected_close_actions.json").write_text(json.dumps({"actions": [row]}, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_b27_dplus_replay_store_truth_validation_response_builder.py \
  --store-dir "$STORE" \
  --requests "$FIXTURES/selected_close_actions.csv" \
  --output-dir "$RESPONSE" \
  --max-actions 5

python3 scripts/xuan_b27_dplus_truth_validation_response_action_scorer.py \
  --response-manifest "$RESPONSE/VALIDATION_RESPONSE_MANIFEST.json" \
  --selected-actions "$FIXTURES/selected_close_actions.json" \
  --output-dir "$SCORE"

ARTIFACT="$ARTIFACT" RESPONSE="$RESPONSE" SCORE="$SCORE" STORE="$STORE" FIXTURES="$FIXTURES" python3 - <<'PY'
import csv
import json
import os
from pathlib import Path

artifact = Path(os.environ["ARTIFACT"])
response = Path(os.environ["RESPONSE"])
score_dir = Path(os.environ["SCORE"])
store = Path(os.environ["STORE"])
fixtures = Path(os.environ["FIXTURES"])

manifest = json.loads((response / "VALIDATION_RESPONSE_MANIFEST.json").read_text())
score = json.loads((score_dir / "score.json").read_text())
with (response / "action_decisions.csv").open(newline="") as handle:
    action_rows = list(csv.DictReader(handle))
with (response / "strict_rescue_closes.csv").open(newline="") as handle:
    close_rows = list(csv.DictReader(handle))

close = close_rows[0] if close_rows else {}
expected_fee = 1.25 * 0.07 * 0.445 * (1.0 - 0.445)
checks = {
    "builder_status_keep": manifest["status"] == "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY",
    "action_decision_validated": len(action_rows) == 1
    and action_rows[0]["validation_status"] == "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY",
    "strict_close_row_present": len(close_rows) == 1,
    "strict_close_refs_present": all(
        close.get(field, "").startswith(prefix)
        for field, prefix in {
            "book_l1_ref": "replay_store_v2:md_book_l1:2026-05-18:",
            "book_l2_ref": "replay_store_v2:md_book_l2:2026-05-18:",
            "trade_before_ref": "replay_store_v2:md_trades:2026-05-18:",
            "trade_after_ref": "replay_store_v2:md_trades:2026-05-18:",
        }.items()
    ),
    "strict_close_fee_source_present": close.get("close_fee_rate_source") == "state_machine_result_config:fixture:official_fee_rate=0.07"
    and close.get("fee_rate_source") == "state_machine_result_config:fixture:official_fee_rate=0.07",
    "strict_close_fee_formula": abs(float(close.get("official_fee") or 0.0) - expected_fee) < 1e-10,
    "strict_close_linkage_present": close.get("source_seed_action_id") == "2"
    and close.get("residual_lot_id") == "residual_lot:2026-05-18:1"
    and close.get("paired_yes_source_action_id") == "1"
    and close.get("paired_no_source_action_id") == "2",
    "scorer_keep": score["decision"] == "KEEP",
    "scorer_strict_close_complete": score["strict_rescue_closes"]["strict_rescue_close_row_count"] == 1
    and score["strict_rescue_closes"]["missing_fields"] == []
    and score["strict_rescue_closes"]["complete_row_count"] == 1,
    "private_promotion_guards_false": manifest["private_truth_ready"] is False
    and manifest["deployable"] is False
    and manifest["promotion_gate"]["passed"] is False
    and score["promotion_gate"]["passed"] is False,
    "no_side_effect_flags": not any(manifest["side_effects"].values()) and not any(score["side_effects"].values()),
}
result = {
    "artifact": "xuan_b27_dplus_strict_rescue_close_response_adapter_smoke",
    "schema_version": "strict_rescue_close_response_adapter_smoke_v1",
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "label": "KEEP_STRICT_RESCUE_CLOSE_RESPONSE_ADAPTER_SMOKE_PASS"
    if all(checks.values())
    else "BLOCKED_STRICT_RESCUE_CLOSE_RESPONSE_ADAPTER_SMOKE_FAIL",
    "checks": checks,
    "store_fixture": str(store),
    "selected_close_actions": str(fixtures / "selected_close_actions.csv"),
    "response_manifest": str(response / "VALIDATION_RESPONSE_MANIFEST.json"),
    "score": str(score_dir / "score.json"),
    "side_effects": {
        "tiny_replay_store_fixture_only": True,
        "real_replay_store_v2_queried": False,
        "raw_scanned": False,
        "full_completion_store_scanned": False,
        "ssh_started": False,
        "shared_ingress_connected": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
    },
}
(artifact / "manifest.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
if result["decision"] != "KEEP":
    raise SystemExit(json.dumps(result, indent=2, sort_keys=True))
print(artifact / "manifest.json")
PY

python3 -m json.tool "$ARTIFACT/manifest.json" >/dev/null
