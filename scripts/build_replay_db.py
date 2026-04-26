#!/usr/bin/env python3
"""Build replay sqlite from recorder JSONL raw captures.

Usage:
  python scripts/build_replay_db.py --date 2026-04-24
  python scripts/build_replay_db.py --input-root data/recorder --date 2026-04-24 --output-root data/replay
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


ORDER_EVENTS = {
    "intent_sent",
    "order_accepted",
    "order_failed",
    "placement_rejected",
    "cancel_sent",
    "cancel_ack",
    "taker_repair_sent",
}
TRANCHE_EVENTS = {
    "tranche_opened",
    "tranche_entered_completion_only",
    "tranche_pair_covered",
    "tranche_overshoot_rolled",
    "same_side_add_before_covered",
    "merge_pairable_reduced",
}
BUDGET_EVENTS = {"surplus_bank_updated", "repair_budget_spent"}
CAPITAL_EVENTS = {"capital_state_snapshot"}
INVENTORY_EVENTS = {"fill_snapshot", "merge_sync", *TRANCHE_EVENTS, *BUDGET_EVENTS, *CAPITAL_EVENTS}
SETTLEMENT_EVENTS = {"redeem_result", "market_resolved"}


@dataclass
class Envelope:
    capture_seq: int
    recv_unix_ms: int
    stream: str
    slug: str
    condition_id: str
    market_id: str
    strategy: str
    dry_run: bool
    payload: Dict[str, Any]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input-root", default="data/recorder")
    p.add_argument("--output-root", default="data/replay")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    return p.parse_args()


def load_jsonl(path: Path) -> Iterator[Envelope]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                yield Envelope(
                    capture_seq=int(obj.get("capture_seq", 0)),
                    recv_unix_ms=int(obj.get("recv_unix_ms", 0)),
                    stream=str(obj.get("stream", "")),
                    slug=str(obj.get("slug", "")),
                    condition_id=str(obj.get("condition_id", "")),
                    market_id=str(obj.get("market_id", "")),
                    strategy=str(obj.get("strategy", "")),
                    dry_run=bool(obj.get("dry_run", False)),
                    payload=obj.get("payload") or {},
                )
            except Exception:
                continue


def json_dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def as_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def as_ts_ms(v: Any) -> Optional[int]:
    """Parse timestamp-like values into unix milliseconds."""
    if v is None:
        return None
    try:
        raw = float(v)
    except Exception:
        return None
    if raw <= 0:
        return None
    iv = int(raw)
    # ns
    if iv >= 10**15:
        return iv // 1_000_000
    # ms
    if iv >= 10**12:
        return iv
    # sec
    return iv * 1000


def parse_slot_side_direction(payload: Dict[str, Any]) -> Tuple[str, str, str]:
    slot = str(payload.get("slot") or "").strip().upper()
    side = str(payload.get("side") or "").strip().upper()
    direction = str(payload.get("direction") or "").strip().upper()
    if slot and "_" in slot:
        parts = slot.split("_", 1)
        if len(parts) == 2:
            side = parts[0].strip().upper()
            direction = parts[1].strip().upper()
    return slot, side, direction


def classify_exec_path(event: str, payload: Dict[str, Any]) -> str:
    if event == "taker_repair_sent":
        return "TAKER_INTENT"
    if event == "order_accepted":
        if payload.get("purpose") is not None:
            return "TAKER_ACCEPTED"
        return "MAKER_ACCEPTED"
    if event == "placement_rejected":
        if payload.get("purpose") is not None:
            return "TAKER_REJECTED"
        return "MAKER_REJECTED"
    return ""


def parse_top_price(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float, str)):
        return as_float(v)
    if isinstance(v, list) and v:
        if isinstance(v[0], (list, tuple)) and v[0]:
            return as_float(v[0][0])
        return as_float(v[0])
    if isinstance(v, dict):
        for k in ("price", "p", "value"):
            if k in v:
                return as_float(v[k])
    return None


def parse_top_size(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float, str)):
        return as_float(v)
    if isinstance(v, list) and v:
        if isinstance(v[0], (list, tuple)) and len(v[0]) >= 2:
            return as_float(v[0][1])
        if len(v) >= 2:
            return as_float(v[1])
        return None
    if isinstance(v, dict):
        for k in ("size", "s", "amount", "qty", "quantity", "value"):
            if k in v:
                return as_float(v[k])
    return None


def extract_book_rows(env: Envelope, raw_payload: str) -> List[Tuple]:
    rows: List[Tuple] = []
    try:
        msg = json.loads(raw_payload)
    except Exception:
        return rows

    entries = msg if isinstance(msg, list) else [msg]
    for item in entries:
        if not isinstance(item, dict):
            continue
        evt = str(item.get("event_type") or item.get("type") or "").lower()
        if evt not in {"book", "price_change", "best_bid_ask"}:
            continue

        bid = None
        ask = None
        bid_sz = None
        ask_sz = None
        for k in ("best_bid", "bid", "b", "yes_bid", "no_bid"):
            if k in item:
                bid = parse_top_price(item.get(k))
                if k == "best_bid":
                    bid_sz = parse_top_size(item.get("best_bid_size"))
                elif k == "bid":
                    bid_sz = parse_top_size(item.get("bid_size"))
                elif k == "yes_bid":
                    bid_sz = parse_top_size(item.get("yes_bid_size"))
                elif k == "no_bid":
                    bid_sz = parse_top_size(item.get("no_bid_size"))
                if bid is not None:
                    break
        for k in ("best_ask", "ask", "a", "yes_ask", "no_ask"):
            if k in item:
                ask = parse_top_price(item.get(k))
                if k == "best_ask":
                    ask_sz = parse_top_size(item.get("best_ask_size"))
                elif k == "ask":
                    ask_sz = parse_top_size(item.get("ask_size"))
                elif k == "yes_ask":
                    ask_sz = parse_top_size(item.get("yes_ask_size"))
                elif k == "no_ask":
                    ask_sz = parse_top_size(item.get("no_ask_size"))
                if ask is not None:
                    break

        if bid is None and "bids" in item:
            bid = parse_top_price(item.get("bids"))
            bid_sz = parse_top_size(item.get("bids"))
        if ask is None and "asks" in item:
            ask = parse_top_price(item.get("asks"))
            ask_sz = parse_top_size(item.get("asks"))

        if bid is None and ask is None:
            continue

        side = (
            str(item.get("market_side") or item.get("side") or item.get("outcome") or item.get("asset_id") or "unknown")
            .strip()
            .upper()
        )
        rows.append(
            (
                env.slug,
                env.recv_unix_ms,
                env.capture_seq,
                side,
                bid,
                ask,
                bid_sz,
                ask_sz,
                evt,
                json_dumps(item),
            )
        )
    return rows


def extract_trade_rows(env: Envelope, raw_payload: str, seen_trade_ids: set[str], seen_trade_keys: set[Tuple]) -> List[Tuple]:
    rows: List[Tuple] = []
    try:
        msg = json.loads(raw_payload)
    except Exception:
        return rows

    entries = msg if isinstance(msg, list) else [msg]
    for item in entries:
        if not isinstance(item, dict):
            continue
        evt = str(item.get("event_type") or item.get("type") or "").lower()
        if evt not in {"trade", "last_trade_price", "tick"}:
            continue

        trade_id = str(item.get("trade_id") or item.get("id") or item.get("hash") or "").strip()
        asset_id = str(item.get("asset_id") or "")
        side = str(item.get("side") or item.get("market_side") or item.get("taker_side") or "").upper()
        taker_side = str(item.get("taker_side") or item.get("side") or "").upper()
        price = as_float(item.get("price") or item.get("last_trade_price") or item.get("p"))
        size = as_float(item.get("size") or item.get("amount") or item.get("s"))
        trade_ts_ms = as_ts_ms(
            item.get("trade_ts_ms")
            or item.get("timestamp_ms")
            or item.get("ts")
            or item.get("timestamp")
            or item.get("time")
        )
        if price is None:
            continue
        if size is None:
            size = 0.0
        if trade_ts_ms is None:
            trade_ts_ms = env.recv_unix_ms

        if trade_id:
            if trade_id in seen_trade_ids:
                continue
            seen_trade_ids.add(trade_id)
        else:
            dedup_key = (env.condition_id, env.recv_unix_ms, side, round(price, 9), round(size, 9))
            if dedup_key in seen_trade_keys:
                continue
            seen_trade_keys.add(dedup_key)

        rows.append(
            (
                env.slug,
                env.recv_unix_ms,
                env.capture_seq,
                asset_id,
                side,
                taker_side,
                price,
                size,
                trade_id,
                trade_ts_ms,
                json_dumps(item),
            )
        )
    return rows


def extract_structured_book_rows(env: Envelope) -> List[Tuple]:
    rows: List[Tuple] = []
    payload = env.payload if isinstance(env.payload, dict) else {}
    if str(payload.get("kind") or "").lower() != "book_l1":
        return rows

    yes_bid = as_float(payload.get("yes_bid"))
    yes_ask = as_float(payload.get("yes_ask"))
    no_bid = as_float(payload.get("no_bid"))
    no_ask = as_float(payload.get("no_ask"))
    yes_bid_sz = as_float(payload.get("yes_bid_sz"))
    yes_ask_sz = as_float(payload.get("yes_ask_sz"))
    no_bid_sz = as_float(payload.get("no_bid_sz"))
    no_ask_sz = as_float(payload.get("no_ask_sz"))
    if yes_bid is None or yes_ask is None or no_bid is None or no_ask is None:
        return rows

    raw_json = json_dumps(payload)
    rows.append(
        (
            env.slug,
            env.recv_unix_ms,
            env.capture_seq,
            "YES",
            yes_bid,
            yes_ask,
            yes_bid_sz,
            yes_ask_sz,
            "structured_book_l1",
            raw_json,
        )
    )
    rows.append(
        (
            env.slug,
            env.recv_unix_ms,
            env.capture_seq,
            "NO",
            no_bid,
            no_ask,
            no_bid_sz,
            no_ask_sz,
            "structured_book_l1",
            raw_json,
        )
    )
    return rows


def extract_structured_trade_rows(
    env: Envelope, seen_trade_ids: set[str], seen_trade_keys: set[Tuple]
) -> List[Tuple]:
    rows: List[Tuple] = []
    payload = env.payload if isinstance(env.payload, dict) else {}
    if str(payload.get("kind") or "").lower() != "trade":
        return rows

    asset_id = str(payload.get("asset_id") or "")
    market_side = str(payload.get("market_side") or "").upper()
    side = str(payload.get("taker_side") or payload.get("side") or market_side).upper()
    taker_side = str(payload.get("taker_side") or side).upper()
    price = as_float(payload.get("price"))
    size = as_float(payload.get("size"))
    trade_id = str(payload.get("trade_id") or "").strip()
    trade_ts_ms = as_ts_ms(payload.get("trade_ts_ms")) or env.recv_unix_ms
    if price is None:
        return rows
    if size is None:
        size = 0.0

    if trade_id:
        if trade_id in seen_trade_ids:
            return rows
        seen_trade_ids.add(trade_id)
    else:
        dedup_key = (env.slug, env.recv_unix_ms, market_side, round(price, 9), round(size, 9))
        if dedup_key in seen_trade_keys:
            return rows
        seen_trade_keys.add(dedup_key)

    rows.append(
        (
            env.slug,
            env.recv_unix_ms,
            env.capture_seq,
            asset_id,
            side,
            taker_side,
            price,
            size,
            trade_id,
            trade_ts_ms,
            json_dumps(payload),
        )
    )
    return rows


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS market_meta (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            condition_id TEXT,
            market_id TEXT,
            strategy TEXT,
            dry_run INTEGER,
            recv_unix_ms INTEGER,
            event TEXT,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS md_book_l1 (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            side TEXT,
            bid REAL,
            ask REAL,
            bid_sz REAL,
            ask_sz REAL,
            source TEXT,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS md_trades (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            asset_id TEXT,
            side TEXT,
            taker_side TEXT,
            price REAL,
            size REAL,
            trade_id TEXT,
            trade_ts_ms INTEGER,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS own_order_events (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS own_order_lifecycle (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
            slot TEXT,
            side TEXT,
            direction TEXT,
            order_id TEXT,
            reason TEXT,
            purpose TEXT,
            reject_kind TEXT,
            retry TEXT,
            cooldown_ms INTEGER,
            price REAL,
            size REAL,
            exec_path TEXT,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS own_inventory_events (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS pair_tranche_events (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
            tranche_id INTEGER,
            state TEXT,
            first_side TEXT,
            pairable_qty REAL,
            residual_qty REAL,
            pair_cost_tranche REAL,
            pair_cost_fifo_ref REAL,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS pair_budget_events (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
            surplus_bank REAL,
            repair_budget_available REAL,
            before_budget REAL,
            after_budget REAL,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS capital_state_events (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
            working_capital REAL,
            locked_in_active_tranches REAL,
            locked_in_pair_covered REAL,
            mergeable_full_sets REAL,
            locked_capital_ratio REAL,
            would_block_new_open_due_to_capital INTEGER,
            would_trigger_merge_due_to_capital INTEGER,
            capital_pressure_merge_batch_shadow REAL,
            clean_closed_episode_ratio REAL,
            same_side_add_qty_ratio REAL,
            payload_json TEXT
        );

        CREATE TABLE IF NOT EXISTS settlement_records (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
            payload_json TEXT
        );
        """
    )


def build_for_slug(conn: sqlite3.Connection, day: str, slug_dir: Path) -> None:
    slug = slug_dir.name
    seen_trade_ids: set[str] = set()
    seen_trade_keys: set[Tuple] = set()

    meta_rows = []
    for env in load_jsonl(slug_dir / "meta.jsonl") or []:
        event = str((env.payload or {}).get("event") or "")
        meta_rows.append(
            (
                day,
                env.slug,
                env.condition_id,
                env.market_id,
                env.strategy,
                1 if env.dry_run else 0,
                env.recv_unix_ms,
                event,
                json_dumps(env.payload),
            )
        )
    if meta_rows:
        conn.executemany(
            "INSERT INTO market_meta VALUES (?,?,?,?,?,?,?,?,?)",
            meta_rows,
        )

    market_md_path = slug_dir / "market_md.jsonl"
    if market_md_path.exists():
        for env in load_jsonl(market_md_path) or []:
            book_rows = extract_structured_book_rows(env)
            if book_rows:
                conn.executemany(
                    "INSERT INTO md_book_l1 VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    [(day, *r) for r in book_rows],
                )
            trade_rows = extract_structured_trade_rows(env, seen_trade_ids, seen_trade_keys)
            if trade_rows:
                conn.executemany(
                    "INSERT INTO md_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(day, *r) for r in trade_rows],
                )
    else:
        for env in load_jsonl(slug_dir / "market_ws.jsonl") or []:
            raw_payload = str((env.payload or {}).get("raw_text") or "")
            if not raw_payload:
                continue
            book_rows = extract_book_rows(env, raw_payload)
            if book_rows:
                conn.executemany(
                    "INSERT INTO md_book_l1 VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    [(day, *r) for r in book_rows],
                )
            trade_rows = extract_trade_rows(env, raw_payload, seen_trade_ids, seen_trade_keys)
            if trade_rows:
                conn.executemany(
                    "INSERT INTO md_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(day, *r) for r in trade_rows],
                )

    for env in load_jsonl(slug_dir / "events.jsonl") or []:
        event = str((env.payload or {}).get("event") or "")
        payload = (env.payload or {}).get("data")
        row = (day, env.slug, env.recv_unix_ms, env.capture_seq, event, json_dumps(payload))
        if event in ORDER_EVENTS:
            conn.execute("INSERT INTO own_order_events VALUES (?,?,?,?,?,?)", row)
        elif event in INVENTORY_EVENTS:
            conn.execute("INSERT INTO own_inventory_events VALUES (?,?,?,?,?,?)", row)
            if event in TRANCHE_EVENTS:
                payload_dict = payload if isinstance(payload, dict) else {}
                conn.execute(
                    "INSERT INTO pair_tranche_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        day,
                        env.slug,
                        env.recv_unix_ms,
                        env.capture_seq,
                        event,
                        int(payload_dict.get("tranche_id") or payload_dict.get("new_tranche_id") or 0),
                        str(payload_dict.get("state") or ""),
                        str(payload_dict.get("first_side") or ""),
                        as_float(payload_dict.get("pairable_qty")),
                        as_float(payload_dict.get("residual_qty") or payload_dict.get("first_qty")),
                        as_float(payload_dict.get("pair_cost_tranche")),
                        as_float(payload_dict.get("pair_cost_fifo_ref")),
                        json_dumps(payload),
                    ),
                )
            if event in BUDGET_EVENTS:
                payload_dict = payload if isinstance(payload, dict) else {}
                conn.execute(
                    "INSERT INTO pair_budget_events VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        day,
                        env.slug,
                        env.recv_unix_ms,
                        env.capture_seq,
                        event,
                        as_float(payload_dict.get("surplus_bank")),
                        as_float(payload_dict.get("repair_budget_available")),
                        as_float(payload_dict.get("before")),
                        as_float(payload_dict.get("after")),
                        json_dumps(payload),
                    ),
                )
            if event in CAPITAL_EVENTS:
                payload_dict = payload if isinstance(payload, dict) else {}
                conn.execute(
                    "INSERT INTO capital_state_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        day,
                        env.slug,
                        env.recv_unix_ms,
                        env.capture_seq,
                        event,
                        as_float(payload_dict.get("working_capital")),
                        as_float(payload_dict.get("locked_in_active_tranches")),
                        as_float(payload_dict.get("locked_in_pair_covered")),
                        as_float(payload_dict.get("mergeable_full_sets")),
                        as_float(payload_dict.get("locked_capital_ratio")),
                        1 if payload_dict.get("would_block_new_open_due_to_capital") else 0,
                        1 if payload_dict.get("would_trigger_merge_due_to_capital") else 0,
                        as_float(payload_dict.get("capital_pressure_merge_batch_shadow")),
                        as_float(payload_dict.get("clean_closed_episode_ratio")),
                        as_float(payload_dict.get("same_side_add_qty_ratio")),
                        json_dumps(payload),
                    ),
                )
        elif event in SETTLEMENT_EVENTS:
            conn.execute("INSERT INTO settlement_records VALUES (?,?,?,?,?,?)", row)
        if event in ORDER_EVENTS:
            payload_dict = payload if isinstance(payload, dict) else {}
            slot, side, direction = parse_slot_side_direction(payload_dict)
            conn.execute(
                "INSERT INTO own_order_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    day,
                    env.slug,
                    env.recv_unix_ms,
                    env.capture_seq,
                    event,
                    slot,
                    side,
                    direction,
                    str(payload_dict.get("order_id") or ""),
                    str(payload_dict.get("reason") or ""),
                    str(payload_dict.get("purpose") or ""),
                    str(payload_dict.get("reject_kind") or ""),
                    str(payload_dict.get("retry") or ""),
                    as_int(payload_dict.get("cooldown_ms")),
                    as_float(payload_dict.get("price") or payload_dict.get("limit_price")),
                    as_float(payload_dict.get("size")),
                    classify_exec_path(event, payload_dict),
                    json_dumps(payload),
                ),
            )


def main() -> None:
    args = parse_args()
    input_day = Path(args.input_root) / args.date
    if not input_day.exists():
        raise SystemExit(f"input day not found: {input_day}")

    out_dir = Path(args.output_root) / args.date
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "crypto_5m.sqlite"

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        init_db(conn)
        slugs = sorted([p for p in input_day.iterdir() if p.is_dir()])
        for slug_dir in slugs:
            build_for_slug(conn, args.date, slug_dir)
        conn.commit()
    finally:
        conn.close()

    print(f"✅ replay db built: {db_path}")


if __name__ == "__main__":
    main()
