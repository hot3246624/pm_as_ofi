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
INVENTORY_EVENTS = {
    "fill_snapshot",
    "merge_sync",
    "pair_tranche_events",
    "pair_budget_events",
    "capital_state_events",
    "completion_first_seed_built",
    "completion_first_open_gate_decision",
    "completion_first_completion_built",
    "completion_first_same_side_add_blocked",
    "completion_first_merge_requested",
    "completion_first_merge_executed",
    "completion_first_redeem_requested",
    "completion_first_shadow_summary",
}
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
        for k in ("best_bid", "bid", "b", "yes_bid", "no_bid"):
            if k in item:
                bid = parse_top_price(item.get(k))
                if bid is not None:
                    break
        for k in ("best_ask", "ask", "a", "yes_ask", "no_ask"):
            if k in item:
                ask = parse_top_price(item.get(k))
                if ask is not None:
                    break

        if bid is None and "bids" in item:
            bid = parse_top_price(item.get("bids"))
        if ask is None and "asks" in item:
            ask = parse_top_price(item.get("asks"))

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
        price = as_float(item.get("price") or item.get("last_trade_price") or item.get("p"))
        size = as_float(item.get("size") or item.get("amount") or item.get("s"))
        if price is None:
            continue
        if size is None:
            size = 0.0

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
                price,
                size,
                trade_id,
                json_dumps(item),
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
            price REAL,
            size REAL,
            trade_id TEXT,
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

        CREATE TABLE IF NOT EXISTS own_inventory_events (
            date TEXT NOT NULL,
            slug TEXT NOT NULL,
            recv_unix_ms INTEGER,
            capture_seq INTEGER,
            event TEXT,
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

    for env in load_jsonl(slug_dir / "market_ws.jsonl") or []:
        raw_payload = str((env.payload or {}).get("raw_text") or "")
        if not raw_payload:
            continue
        book_rows = extract_book_rows(env, raw_payload)
        if book_rows:
            conn.executemany(
                "INSERT INTO md_book_l1 VALUES (?,?,?,?,?,?,?,?,?)",
                [(day, *r) for r in book_rows],
            )
        trade_rows = extract_trade_rows(env, raw_payload, seen_trade_ids, seen_trade_keys)
        if trade_rows:
            conn.executemany(
                "INSERT INTO md_trades VALUES (?,?,?,?,?,?,?,?,?,?)",
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
        elif event in SETTLEMENT_EVENTS:
            conn.execute("INSERT INTO settlement_records VALUES (?,?,?,?,?,?)", row)


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
