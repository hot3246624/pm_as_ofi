#!/usr/bin/env python3
"""Build selected-action replay_store_v2 source-truth validation responses.

This is a local-only validator for explicitly selected candidate actions. It
does not discover strategy candidates, scan raw/replay stores, read events
JSONL, connect to shared-ingress, or touch order/cancel/redeem paths.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_STORE_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/verification_store/replay_store_v2/20260502_20260518"
)
SCHEMA_VERSION = "truth_validation_response_v1"
SOURCE_KIND = "replay_store_v2"
VALIDATED_STATUS = "SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY"
UNKNOWN_STATUS = "UNKNOWN_SOURCE_TRUTH_INSUFFICIENT"
DISCARD_STATUS = "DISCARD_SOURCE_TRUTH_MISMATCH"
FORBIDDEN_DAYS = {"2026-05-14", "2026-05-15", "2026-05-19"}
REQUIRED_ACTION_FIELDS = (
    "validation_request_id",
    "day",
    "condition_id",
    "candidate_action_id",
    "action_ts_ms",
    "side",
    "seed_qty",
    "seed_px",
    "expected_decision",
    "fee_rate",
    "fee_rate_source",
)
CSV_NULL = ""


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store-dir", type=Path, default=DEFAULT_STORE_DIR)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-actions", type=int, default=50)
    parser.add_argument("--book-lookback-ms", type=int, default=60_000)
    parser.add_argument("--trade-window-ms", type=int, default=30_000)
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def as_int(value: Any, default: int = 0) -> int:
    return int(as_float(value, default))


def load_requests(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    elif suffix == ".csv":
        with path.open(newline="") as fh:
            rows = list(csv.DictReader(fh))
    else:
        obj = json.loads(path.read_text())
        if isinstance(obj, dict) and isinstance(obj.get("actions"), list):
            rows = obj["actions"]
        elif isinstance(obj, list):
            rows = obj
        else:
            raise ValueError("requests JSON must be a list or an object with actions[]")
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError("all request rows must be objects")
    return [dict(row) for row in rows]


def stable_ref(table: str, day: str, row: dict[str, Any] | None) -> str:
    if not row:
        return CSV_NULL
    source_row_id = row.get("source_row_id")
    if source_row_id is None:
        return CSV_NULL
    return f"{SOURCE_KIND}:{table}:{day}:{source_row_id}"


def fetch_one(con: duckdb.DuckDBPyConnection, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    cur = con.execute(query, params)
    names = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip(names, row))


def private_counts(manifest: dict[str, Any]) -> dict[str, int]:
    counts = manifest.get("private_table_counts")
    if isinstance(counts, dict):
        return {str(key): as_int(value) for key, value in counts.items()}
    totals = manifest.get("table_totals") or {}
    return {
        key: as_int(totals.get(key))
        for key in ("own_order_events", "own_fill_events", "own_inventory_events", "user_ws_log")
    }


def valid_days(manifest: dict[str, Any]) -> set[str]:
    days = manifest.get("days") or manifest.get("valid_days") or []
    return {str(day) for day in days}


def fee_amount(shares: float, fee_rate: float, price: float) -> float:
    return shares * fee_rate * price * (1.0 - price)


def validate_action(
    con: duckdb.DuckDBPyConnection,
    action: dict[str, Any],
    allowed_days: set[str],
    args: argparse.Namespace,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    day = str(action.get("day", ""))
    condition_id = str(action.get("condition_id", ""))
    side = str(action.get("side", "")).upper()
    action_ts_ms = as_int(action.get("action_ts_ms"), -1)
    seed_qty = as_float(action.get("seed_qty"))
    seed_px = as_float(action.get("seed_px"))
    fee_rate = as_float(action.get("fee_rate"), -1.0)

    reasons: list[str] = []
    missing_required = [
        field
        for field in REQUIRED_ACTION_FIELDS
        if action.get(field) in (None, "")
    ]
    if missing_required:
        reasons.append("missing_required_fields:" + ",".join(missing_required))
    if day in FORBIDDEN_DAYS or day not in allowed_days:
        reasons.append("forbidden_or_unknown_day")
    if side not in {"YES", "NO"}:
        reasons.append("invalid_side")
    if action_ts_ms < 0:
        reasons.append("invalid_action_ts_ms")
    if seed_qty <= 0.0:
        reasons.append("invalid_seed_qty")
    if not (0.0 < seed_px < 1.0):
        reasons.append("invalid_seed_px")
    if fee_rate < 0.0:
        reasons.append("invalid_fee_rate")
    if not action.get("fee_rate_source"):
        reasons.append("missing_fee_rate_source")

    if "forbidden_or_unknown_day" in reasons:
        status = DISCARD_STATUS
        refs = {
            "market_meta_ref": CSV_NULL,
            "book_l1_ref": CSV_NULL,
            "book_l2_ref": CSV_NULL,
            "trade_before_ref": CSV_NULL,
            "trade_after_ref": CSV_NULL,
            "settlement_ref": CSV_NULL,
        }
        source_rows_present = False
    else:
        market = fetch_one(
            con,
            """
            select source_row_id, condition_id, slug, start_ms, end_ms, yes_token_id, no_token_id, tick_size, day
            from market_meta
            where day = ? and condition_id = ?
            limit 1
            """,
            (day, condition_id),
        )
        l1 = fetch_one(
            con,
            """
            select source_row_id, condition_id, recv_ms, source_ts_ms,
                   yes_bid_px, yes_ask_px, no_bid_px, no_ask_px,
                   yes_bid_sz, yes_ask_sz, no_bid_sz, no_ask_sz, source_kind, day
            from md_book_l1
            where day = ? and condition_id = ?
              and recv_ms <= ? and recv_ms >= ?
            order by recv_ms desc, source_row_id desc
            limit 1
            """,
            (day, condition_id, action_ts_ms, action_ts_ms - args.book_lookback_ms),
        )
        l2 = fetch_one(
            con,
            """
            select source_row_id, condition_id, recv_ms, source_ts_ms, market_side,
                   depth, bid1_px, bid1_sz, ask1_px, ask1_sz, source_kind, day
            from md_book_l2
            where day = ? and condition_id = ? and market_side = ?
              and recv_ms <= ? and recv_ms >= ?
            order by recv_ms desc, source_row_id desc
            limit 1
            """,
            (day, condition_id, side, action_ts_ms, action_ts_ms - args.book_lookback_ms),
        )
        trade_before = fetch_one(
            con,
            """
            select source_row_id, condition_id, trade_ts_ms, recv_ms, trade_id,
                   market_side, taker_side, price, size, source_quality, day
            from md_trades
            where day = ? and condition_id = ? and recv_ms <= ? and recv_ms >= ?
            order by recv_ms desc, source_row_id desc
            limit 1
            """,
            (day, condition_id, action_ts_ms, action_ts_ms - args.trade_window_ms),
        )
        trade_after = fetch_one(
            con,
            """
            select source_row_id, condition_id, trade_ts_ms, recv_ms, trade_id,
                   market_side, taker_side, price, size, source_quality, day
            from md_trades
            where day = ? and condition_id = ? and recv_ms > ? and recv_ms <= ?
            order by recv_ms asc, source_row_id asc
            limit 1
            """,
            (day, condition_id, action_ts_ms, action_ts_ms + args.trade_window_ms),
        )
        settlement = fetch_one(
            con,
            """
            select source_row_id, condition_id, official_outcome, winner_side, settle_ms, resolution_source, day
            from settlement_records
            where day = ? and condition_id = ?
            limit 1
            """,
            (day, condition_id),
        )
        refs = {
            "market_meta_ref": stable_ref("market_meta", day, market),
            "book_l1_ref": stable_ref("md_book_l1", day, l1),
            "book_l2_ref": stable_ref("md_book_l2", day, l2),
            "trade_before_ref": stable_ref("md_trades", day, trade_before),
            "trade_after_ref": stable_ref("md_trades", day, trade_after),
            "settlement_ref": stable_ref("settlement_records", day, settlement),
        }
        source_rows_present = bool(market and l1 and l2)
        if not market:
            reasons.append("missing_market_meta")
        if not l1:
            reasons.append("missing_pre_action_l1")
        if not l2:
            reasons.append("missing_pre_action_l2")
        if missing_required or "missing_fee_rate_source" in reasons or "invalid_fee_rate" in reasons or not source_rows_present:
            status = UNKNOWN_STATUS
        else:
            status = VALIDATED_STATUS

    fee = fee_amount(seed_qty, max(fee_rate, 0.0), seed_px) if seed_qty > 0.0 and 0.0 < seed_px < 1.0 else 0.0
    row = {
        "schema_version": SCHEMA_VERSION,
        "validation_request_id": action.get("validation_request_id", CSV_NULL),
        "day": day,
        "condition_id": condition_id,
        "slug": action.get("slug", CSV_NULL),
        "candidate_action_id": action.get("candidate_action_id", CSV_NULL),
        "action_ts_ms": action_ts_ms if action_ts_ms >= 0 else CSV_NULL,
        "side": side,
        "seed_qty": round(seed_qty, 8),
        "seed_px": round(seed_px, 8),
        "fee_rate": fee_rate if fee_rate >= 0.0 else CSV_NULL,
        "fee_rate_source": action.get("fee_rate_source", CSV_NULL),
        "official_fee": round(fee, 10),
        "expected_decision": action.get("expected_decision", CSV_NULL),
        "decision_reason": action.get("decision_reason", CSV_NULL),
        "validation_status": status,
        "reasons": "|".join(reasons),
        "source_rows_present": source_rows_present,
        **refs,
    }

    residual_rows: list[dict[str, Any]] = []
    if action.get("lot_id") or action.get("residual_target_qty") is not None or action.get("source_seed_action_id"):
        residual_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "validation_request_id": action.get("validation_request_id", CSV_NULL),
                "candidate_action_id": action.get("candidate_action_id", CSV_NULL),
                "lot_id": action.get("lot_id", CSV_NULL),
                "source_seed_action_id": action.get("source_seed_action_id", action.get("candidate_action_id", CSV_NULL)),
                "remaining_qty": action.get("residual_target_qty", CSV_NULL),
                "remaining_cost": action.get("residual_target_cost", CSV_NULL),
                "source_ref": row["book_l2_ref"] or row["book_l1_ref"],
                "validation_status": status,
            }
        )

    close_rows: list[dict[str, Any]] = []
    if action.get("close_action_id"):
        close_qty = as_float(action.get("close_qty"))
        close_px = as_float(action.get("close_px"))
        close_fee = fee_amount(close_qty, max(fee_rate, 0.0), close_px) if close_qty > 0.0 and 0.0 < close_px < 1.0 else 0.0
        close_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "validation_request_id": action.get("validation_request_id", CSV_NULL),
                "candidate_action_id": action.get("candidate_action_id", CSV_NULL),
                "close_action_id": action.get("close_action_id", CSV_NULL),
                "close_ts_ms": action.get("close_ts_ms", CSV_NULL),
                "close_side": action.get("close_side", CSV_NULL),
                "close_qty": action.get("close_qty", CSV_NULL),
                "close_px": action.get("close_px", CSV_NULL),
                "official_fee": round(close_fee, 10),
                "fee_rate": fee_rate if fee_rate >= 0.0 else CSV_NULL,
                "fee_rate_source": action.get("fee_rate_source", CSV_NULL),
                "book_l1_ref": row["book_l1_ref"],
                "book_l2_ref": row["book_l2_ref"],
                "trade_before_ref": row["trade_before_ref"],
                "trade_after_ref": row["trade_after_ref"],
                "validation_status": status,
            }
        )
    return row, residual_rows, close_rows


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, CSV_NULL) for field in fieldnames})


def main() -> int:
    args = parse_args()
    store_dir = args.store_dir
    manifest_path = store_dir / "REPLAY_STORE_V2_MANIFEST.json"
    duckdb_path = store_dir / "store.duckdb"
    store_manifest = read_json(manifest_path)
    actions = load_requests(args.requests)
    if len(actions) > args.max_actions:
        raise ValueError(f"selected action request too large: {len(actions)} > max_actions={args.max_actions}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    allowed_days = valid_days(store_manifest)
    private_table_counts = private_counts(store_manifest)
    con = duckdb.connect(str(duckdb_path), read_only=True)

    action_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []
    close_rows: list[dict[str, Any]] = []
    for action in actions:
        row, residual, closes = validate_action(con, action, allowed_days, args)
        action_rows.append(row)
        residual_rows.extend(residual)
        close_rows.extend(closes)

    statuses = {row["validation_status"] for row in action_rows}
    if DISCARD_STATUS in statuses:
        status = DISCARD_STATUS
        decision = "DISCARD"
    elif UNKNOWN_STATUS in statuses or not action_rows:
        status = UNKNOWN_STATUS
        decision = "UNKNOWN"
    else:
        status = VALIDATED_STATUS
        decision = "KEEP"

    action_fields = [
        "schema_version",
        "validation_request_id",
        "day",
        "condition_id",
        "slug",
        "candidate_action_id",
        "action_ts_ms",
        "side",
        "seed_qty",
        "seed_px",
        "fee_rate",
        "fee_rate_source",
        "official_fee",
        "expected_decision",
        "decision_reason",
        "validation_status",
        "reasons",
        "source_rows_present",
        "market_meta_ref",
        "book_l1_ref",
        "book_l2_ref",
        "trade_before_ref",
        "trade_after_ref",
        "settlement_ref",
    ]
    residual_fields = [
        "schema_version",
        "validation_request_id",
        "candidate_action_id",
        "lot_id",
        "source_seed_action_id",
        "remaining_qty",
        "remaining_cost",
        "source_ref",
        "validation_status",
    ]
    close_fields = [
        "schema_version",
        "validation_request_id",
        "candidate_action_id",
        "close_action_id",
        "close_ts_ms",
        "close_side",
        "close_qty",
        "close_px",
        "official_fee",
        "fee_rate",
        "fee_rate_source",
        "book_l1_ref",
        "book_l2_ref",
        "trade_before_ref",
        "trade_after_ref",
        "validation_status",
    ]
    write_csv(args.output_dir / "action_decisions.csv", action_rows, action_fields)
    write_csv(args.output_dir / "residual_fifo_lots.csv", residual_rows, residual_fields)
    write_csv(args.output_dir / "strict_rescue_closes.csv", close_rows, close_fields)

    days = sorted({str(row.get("day")) for row in action_rows if row.get("day")})
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "artifact": "xuan_b27_dplus_replay_store_truth_validation_response",
        "status": status,
        "created_utc": utc_label(),
        "source_kind": SOURCE_KIND,
        "is_private_truth": False,
        "private_truth_ready": False,
        "public_or_proxy_truth_only": True,
        "deployable": False,
        "promotion_gate_pass": False,
        "store_dir": str(store_dir),
        "store_manifest": str(manifest_path),
        "store_duckdb": str(duckdb_path),
        "days": days,
        "valid_days": sorted(allowed_days),
        "forbidden_days": sorted(FORBIDDEN_DAYS),
        "row_count": len(action_rows),
        "status_counts": {key: sum(1 for row in action_rows if row["validation_status"] == key) for key in sorted(statuses)},
        "private_table_counts": private_table_counts,
        "selected_action_lookup_only": True,
        "max_actions": args.max_actions,
        "outputs": {
            "manifest": str(args.output_dir / "VALIDATION_RESPONSE_MANIFEST.json"),
            "action_decisions": str(args.output_dir / "action_decisions.csv"),
            "residual_fifo_lots": str(args.output_dir / "residual_fifo_lots.csv"),
            "strict_rescue_closes": str(args.output_dir / "strict_rescue_closes.csv"),
        },
        "fee_model": {
            "formula": "fee = shares * fee_rate * price * (1 - price)",
            "fee_rate_required_per_action": True,
            "fee_rate_source_required_per_action": True,
            "hard_coded_account_average_fee_rate": False,
        },
        "research_ranking": {
            "decision": decision,
            "label": (
                "KEEP_SOURCE_TRUTH_VALIDATED_RESEARCH_ONLY"
                if decision == "KEEP"
                else ("UNKNOWN_SOURCE_TRUTH_INSUFFICIENT" if decision == "UNKNOWN" else "DISCARD_SOURCE_TRUTH_MISMATCH")
            ),
            "interpretation": "Selected candidate actions were validated only against replay-derived source rows. This is research-only source truth, not private owner-trade truth.",
        },
        "promotion_gate": {
            "passed": False,
            "status": "SOURCE_TRUTH_RESEARCH_ONLY_NOT_PRIVATE_TRUTH",
            "deployable": False,
            "can_support_strategy_promotion": False,
            "g2_canary_ready": False,
            "private_truth_ready": False,
        },
        "side_effects": {
            "network_started": False,
            "ssh_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "broad_strategy_search": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "guardrails": {
            "external_learning_account_private_truth_required": False,
            "private_truth_can_only_mean_our_execution_system_truth": True,
            "source_truth_can_prepare_shadow_review_but_not_deployable": True,
        },
    }
    write_json(args.output_dir / "VALIDATION_RESPONSE_MANIFEST.json", manifest)
    print(args.output_dir / "VALIDATION_RESPONSE_MANIFEST.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
