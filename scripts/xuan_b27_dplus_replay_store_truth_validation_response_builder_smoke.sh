#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STORE="${STORE:-/Users/hot/web3Scientist/poly_backtest_data/verification_store/replay_store_v2/20260502_20260518}"
TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT="${ARTIFACT:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_replay_store_truth_validation_response_builder_smoke_${TS}}"
FIXTURES="$ARTIFACT/fixtures"
VALID_OUT="$ARTIFACT/valid_response"
MISSING_FEE_OUT="$ARTIFACT/missing_fee_response"
FORBIDDEN_OUT="$ARTIFACT/forbidden_day_response"

mkdir -p "$FIXTURES" "$VALID_OUT" "$MISSING_FEE_OUT" "$FORBIDDEN_OUT"

python3 -m py_compile scripts/xuan_b27_dplus_replay_store_truth_validation_response_builder.py

python3 - "$STORE" "$FIXTURES" <<'PY'
import json
import sys
from pathlib import Path

import duckdb

store = Path(sys.argv[1])
fixtures = Path(sys.argv[2])
con = duckdb.connect(str(store / "store.duckdb"), read_only=True)

book_l2 = con.execute(
    """
    select condition_id, recv_ms, market_side, ask1_px, bid1_px, source_row_id, day
    from md_book_l2
    where day = '2026-05-18' and market_side = 'YES'
    order by recv_ms, source_row_id
    limit 1
    """
).fetchone()
if book_l2 is None:
    raise SystemExit("smoke fixture could not find selected md_book_l2 row")

condition_id, recv_ms, market_side, ask1_px, bid1_px, source_row_id, day = book_l2
book_l1 = con.execute(
    """
    select source_row_id, recv_ms
    from md_book_l1
    where day = ? and condition_id = ? and recv_ms <= ?
    order by recv_ms desc, source_row_id desc
    limit 1
    """,
    [day, condition_id, int(recv_ms) + 1],
).fetchone()
if book_l1 is None:
    raise SystemExit("smoke fixture could not find selected md_book_l1 row")

market = con.execute(
    """
    select source_row_id, slug
    from market_meta
    where day = ? and condition_id = ?
    limit 1
    """,
    [day, condition_id],
).fetchone()
if market is None:
    raise SystemExit("smoke fixture could not find selected market_meta row")

seed_px = float(ask1_px or bid1_px or 0.5)
if not 0.0 < seed_px < 1.0:
    seed_px = 0.5

action = {
    "validation_request_id": "smoke_valid_20260518",
    "day": str(day),
    "condition_id": str(condition_id),
    "slug": str(market[1]),
    "candidate_action_id": "smoke_candidate_action_1",
    "action_ts_ms": int(max(int(recv_ms), int(book_l1[1])) + 1),
    "side": str(market_side),
    "seed_qty": 1.25,
    "seed_px": seed_px,
    "expected_decision": "accept",
    "decision_reason": "smoke_selected_action_fixture",
    "fee_rate": 0.07,
    "fee_rate_source": "smoke_explicit_fixture",
    "lot_id": "smoke_lot_1",
    "residual_target_qty": 0.0,
    "residual_target_cost": 0.0,
    "close_action_id": "smoke_close_1",
    "close_ts_ms": int(max(int(recv_ms), int(book_l1[1])) + 2),
    "close_side": "NO" if str(market_side).upper() == "YES" else "YES",
    "close_qty": 0.25,
    "close_px": 1.0 - seed_px,
}

missing_fee = dict(action)
missing_fee["validation_request_id"] = "smoke_missing_fee_source"
missing_fee["candidate_action_id"] = "smoke_candidate_action_missing_fee"
missing_fee.pop("fee_rate_source", None)

forbidden = dict(action)
forbidden["validation_request_id"] = "smoke_forbidden_20260519"
forbidden["candidate_action_id"] = "smoke_candidate_action_forbidden_day"
forbidden["day"] = "2026-05-19"

(fixtures / "valid_actions.json").write_text(json.dumps({"actions": [action]}, indent=2, sort_keys=True) + "\n")
(fixtures / "missing_fee_source_actions.json").write_text(json.dumps({"actions": [missing_fee]}, indent=2, sort_keys=True) + "\n")
(fixtures / "forbidden_day_actions.json").write_text(json.dumps({"actions": [forbidden]}, indent=2, sort_keys=True) + "\n")
PY

python3 scripts/xuan_b27_dplus_replay_store_truth_validation_response_builder.py \
  --store-dir "$STORE" \
  --requests "$FIXTURES/valid_actions.json" \
  --output-dir "$VALID_OUT"

python3 scripts/xuan_b27_dplus_replay_store_truth_validation_response_builder.py \
  --store-dir "$STORE" \
  --requests "$FIXTURES/missing_fee_source_actions.json" \
  --output-dir "$MISSING_FEE_OUT"

python3 scripts/xuan_b27_dplus_replay_store_truth_validation_response_builder.py \
  --store-dir "$STORE" \
  --requests "$FIXTURES/forbidden_day_actions.json" \
  --output-dir "$FORBIDDEN_OUT"

python3 - "$ARTIFACT" "$STORE" <<'PY'
import csv
import json
import sys
from pathlib import Path

artifact = Path(sys.argv[1])
store = Path(sys.argv[2])


def load_manifest(path: Path) -> dict:
    return json.loads(path.read_text())


def load_action_rows(path: Path) -> list[dict]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


valid = load_manifest(artifact / "valid_response" / "VALIDATION_RESPONSE_MANIFEST.json")
missing_fee = load_manifest(artifact / "missing_fee_response" / "VALIDATION_RESPONSE_MANIFEST.json")
forbidden = load_manifest(artifact / "forbidden_day_response" / "VALIDATION_RESPONSE_MANIFEST.json")
valid_rows = load_action_rows(artifact / "valid_response" / "action_decisions.csv")
missing_rows = load_action_rows(artifact / "missing_fee_response" / "action_decisions.csv")
forbidden_rows = load_action_rows(artifact / "forbidden_day_response" / "action_decisions.csv")

checks = {
    "py_compile_passed": True,
    "valid_selected_action_source_truth_validated": valid["status"] == "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY"
    and valid["row_count"] == 1
    and valid_rows[0]["validation_status"] == "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY",
    "valid_source_refs_present": valid_rows[0]["market_meta_ref"].startswith("replay_store_v2:market_meta:2026-05-18:")
    and valid_rows[0]["book_l1_ref"].startswith("replay_store_v2:md_book_l1:2026-05-18:")
    and valid_rows[0]["book_l2_ref"].startswith("replay_store_v2:md_book_l2:2026-05-18:"),
    "forbidden_day_rejected": forbidden["status"] == "DISCARD_SOURCE_TRUTH_MISMATCH"
    and "forbidden_or_unknown_day" in forbidden_rows[0]["reasons"],
    "missing_fee_rate_source_unknown": missing_fee["status"] == "UNKNOWN_SOURCE_TRUTH_INSUFFICIENT"
    and "missing_fee_rate_source" in missing_rows[0]["reasons"],
    "private_truth_guard_false": valid["is_private_truth"] is False
    and valid["private_truth_ready"] is False
    and valid["deployable"] is False
    and valid["promotion_gate"]["passed"] is False,
    "private_tables_empty": all(int(v) == 0 for v in valid["private_table_counts"].values()),
    "output_files_exist": all(
        (artifact / rel).exists()
        for rel in [
            "valid_response/VALIDATION_RESPONSE_MANIFEST.json",
            "valid_response/action_decisions.csv",
            "valid_response/residual_fifo_lots.csv",
            "valid_response/strict_rescue_closes.csv",
            "missing_fee_response/VALIDATION_RESPONSE_MANIFEST.json",
            "forbidden_day_response/VALIDATION_RESPONSE_MANIFEST.json",
        ]
    ),
    "no_side_effect_flags": not any(valid["side_effects"].values()),
    "promotion_gate_separated": valid["research_ranking"]["decision"] == "KEEP"
    and valid["promotion_gate"]["status"] == "SOURCE_TRUTH_RESEARCH_ONLY_NOT_PRIVATE_TRUTH",
}
passed = all(checks.values())
if not passed:
    failed = [name for name, ok in checks.items() if not ok]
    raise SystemExit("smoke checks failed: " + ",".join(failed))

manifest = {
    "artifact": "xuan_b27_dplus_replay_store_truth_validation_response_builder_smoke",
    "schema_version": "xuan_research_smoke_v1",
    "created_utc": artifact.name.rsplit("_", 1)[-1] if "_" in artifact.name else "",
    "status": "PASS",
    "decision_label": "KEEP_REPLAY_STORE_V2_TRUTH_VALIDATION_RESPONSE_BUILDER_READY",
    "store_dir": str(store),
    "builder": "scripts/xuan_b27_dplus_replay_store_truth_validation_response_builder.py",
    "fixtures": {
        "valid_actions": str(artifact / "fixtures" / "valid_actions.json"),
        "missing_fee_source_actions": str(artifact / "fixtures" / "missing_fee_source_actions.json"),
        "forbidden_day_actions": str(artifact / "fixtures" / "forbidden_day_actions.json"),
    },
    "responses": {
        "valid": str(artifact / "valid_response" / "VALIDATION_RESPONSE_MANIFEST.json"),
        "missing_fee_source": str(artifact / "missing_fee_response" / "VALIDATION_RESPONSE_MANIFEST.json"),
        "forbidden_day": str(artifact / "forbidden_day_response" / "VALIDATION_RESPONSE_MANIFEST.json"),
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_REPLAY_STORE_V2_TRUTH_VALIDATION_RESPONSE_BUILDER_READY",
        "interpretation": "Builder can emit replay_store_v2 selected-action source-truth responses locally. This is tooling readiness only.",
    },
    "promotion_gate": {
        "passed": False,
        "status": "BUILDER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
    },
    "guardrails": {
        "selected_action_lookup_only": True,
        "valid_days_enforced": True,
        "forbidden_days_rejected": ["2026-05-14", "2026-05-15", "2026-05-19"],
        "fee_rate_source_required": True,
        "official_fee_formula": "fee = shares * fee_rate * price * (1 - price)",
        "private_truth_not_claimed_from_replay_store": True,
    },
    "side_effects": {
        "ssh_started": False,
        "network_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "broad_strategy_search": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
    },
    "next_action": "Run a local selected-candidate source-truth response against candidate_registry/candidate_base-derived validation requests; keep result SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY unless our own private truth appears.",
}
(artifact / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(artifact / "manifest.json")
PY

python3 -m json.tool "$ARTIFACT/manifest.json" >/dev/null
python3 -m json.tool "$VALID_OUT/VALIDATION_RESPONSE_MANIFEST.json" >/dev/null
python3 -m json.tool "$MISSING_FEE_OUT/VALIDATION_RESPONSE_MANIFEST.json" >/dev/null
python3 -m json.tool "$FORBIDDEN_OUT/VALIDATION_RESPONSE_MANIFEST.json" >/dev/null

echo "PASS $ARTIFACT/manifest.json"
