#!/usr/bin/env python3
"""Infer maker/taker-like behavior for xuan trades from local md_book_l1 snapshots.

This is an offline research tool. It does not change live strategy behavior.

Method:
- Load public xuan trades from data/xuan/trades_long.json
- For each BUY trade, look up the latest local md_book_l1 snapshot at-or-before
  the trade timestamp for the same condition_id
- Compare trade price to winner-side or outcome-side best bid/ask
- Classify the trade as maker-like / taker-like / inside-spread / stale / no-book

Outputs:
- data/xuan/xuan_maker_taker_inference.csv
- data/xuan/xuan_maker_taker_inference.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TRADES = REPO_ROOT / "data" / "xuan" / "trades_long.json"
DEFAULT_OUT_CSV = REPO_ROOT / "data" / "xuan" / "xuan_maker_taker_inference.csv"
DEFAULT_OUT_JSON = REPO_ROOT / "data" / "xuan" / "xuan_maker_taker_inference.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--trades", type=Path, default=DEFAULT_TRADES)
    p.add_argument(
        "--db",
        type=Path,
        action="append",
        default=None,
        help="Replay sqlite path. May be provided multiple times. Defaults to all data/replay_recorder/*/crypto_5m.sqlite, then replay_sidecar as fallback.",
    )
    p.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)
    p.add_argument("--out-json", type=Path, default=DEFAULT_OUT_JSON)
    p.add_argument("--max-book-age-ms", type=int, default=1500)
    p.add_argument("--only-btc-5m", action="store_true", default=True)
    p.add_argument("--no-only-btc-5m", dest="only_btc_5m", action="store_false")
    return p.parse_args()


def find_default_replay_dbs() -> list[Path]:
    matches = []
    for pattern in (
        "data/replay_recorder/*/crypto_5m.sqlite",
        "data/replay_sidecar/*/crypto_5m.sqlite",
    ):
        matches.extend(sorted(REPO_ROOT.glob(pattern)))
    if not matches:
        raise SystemExit(
            "no replay sqlite found under data/replay_recorder/*/crypto_5m.sqlite or data/replay_sidecar/*/crypto_5m.sqlite"
        )
    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in matches:
        if path not in seen:
            seen.add(path)
            deduped.append(path)
    return deduped


def load_trades(path: Path, only_btc_5m: bool) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"missing trades file: {path}")
    rows = json.loads(path.read_text())
    if not isinstance(rows, list):
        raise SystemExit("expected trades json to be a list")
    out: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        slug = str(row.get("slug") or "")
        if only_btc_5m and not slug.startswith("btc-updown-5m-"):
            continue
        out.append(row)
    return out


def side_from_outcome(outcome: str) -> str:
    normalized = outcome.strip().lower()
    if normalized in {"up", "yes"}:
        return "YES"
    if normalized in {"down", "no"}:
        return "NO"
    return "UNKNOWN"


def tick_tol(price: float, tick_size: float | None) -> float:
    tick = tick_size if tick_size and tick_size > 0 else 0.01
    if price >= 0.96 or price <= 0.04:
        tick = min(tick, 0.001)
    return max(tick * 0.51, 5e-4)


@dataclass
class ReplaySource:
    path: Path
    conn: sqlite3.Connection
    schema_kind: str
    condition_meta: dict[str, dict[str, Any]]
    slug_set: set[str]


def classify_trade(
    trade_price: float,
    bid: float | None,
    ask: float | None,
    age_ms: int | None,
    max_book_age_ms: int,
    tol: float,
) -> str:
    if age_ms is None or age_ms < 0:
        return "no_book"
    if age_ms > max_book_age_ms:
        return "stale_book"
    if bid is None and ask is None:
        return "no_side_quote"
    if ask is not None and abs(trade_price - ask) <= tol:
        return "taker_at_ask"
    if bid is not None and abs(trade_price - bid) <= tol:
        return "maker_at_bid"
    if bid is not None and ask is not None and bid + tol < trade_price < ask - tol:
        return "inside_spread"
    if ask is not None and trade_price > ask + tol:
        return "above_ask"
    if bid is not None and trade_price < bid - tol:
        return "below_bid"
    if ask is not None and trade_price < ask - tol:
        return "improved_bid"
    if bid is not None and trade_price > bid + tol:
        return "lifted_from_bid"
    return "unclassified"


def classify_proximity(
    trade_price: float,
    bid: float | None,
    ask: float | None,
    age_ms: int | None,
    max_book_age_ms: int,
    tol: float,
) -> str | None:
    if age_ms is None or age_ms < 0 or age_ms > max_book_age_ms:
        return None
    if bid is None or ask is None:
        return None
    dist_to_bid = abs(trade_price - bid)
    dist_to_ask = abs(ask - trade_price)
    if dist_to_bid + tol < dist_to_ask:
        return "maker_proximity"
    if dist_to_ask + tol < dist_to_bid:
        return "taker_proximity"
    return "balanced_proximity"


def table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def detect_schema_kind(conn: sqlite3.Connection) -> str:
    cols = set(table_columns(conn, "md_book_l1"))
    if {"condition_id", "recv_ms", "yes_bid_px", "yes_ask_px", "no_bid_px", "no_ask_px"}.issubset(cols):
        return "wide_condition"
    if {"slug", "recv_unix_ms", "side", "bid", "ask"}.issubset(cols):
        return "rowwise_asset"
    raise SystemExit(f"unsupported md_book_l1 schema columns: {sorted(cols)}")


def load_replay_source(path: Path) -> ReplaySource:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    schema_kind = detect_schema_kind(conn)
    condition_meta: dict[str, dict[str, Any]] = {}
    slug_set: set[str] = set()
    if schema_kind == "wide_condition":
        rows = conn.execute(
            """
            SELECT condition_id, slug, tick_size
            FROM market_meta
            """
        ).fetchall()
        condition_meta = {
            str(condition_id): {
                "slug": slug,
                "tick_size": float(tick_size) if tick_size is not None else None,
            }
            for condition_id, slug, tick_size in rows
        }
        slug_set = {str(meta["slug"]) for meta in condition_meta.values() if meta.get("slug")}
    else:
        rows = conn.execute("SELECT DISTINCT slug FROM md_book_l1").fetchall()
        slug_set = {str(row[0]) for row in rows}
    return ReplaySource(
        path=path,
        conn=conn,
        schema_kind=schema_kind,
        condition_meta=condition_meta,
        slug_set=slug_set,
    )


def market_present(source: ReplaySource, condition_id: str, slug: str) -> bool:
    if source.schema_kind == "wide_condition":
        return condition_id in source.condition_meta
    return slug in source.slug_set


def query_latest_book(
    source: ReplaySource,
    condition_id: str,
    slug: str,
    asset_id: str,
    outcome_side: str,
    trade_ts_ms: int,
) -> tuple[int, float | None, float | None, float | None] | None:
    if source.schema_kind == "wide_condition":
        row = source.conn.execute(
            """
            SELECT recv_ms, yes_bid_px, yes_ask_px, no_bid_px, no_ask_px
            FROM md_book_l1
            WHERE condition_id = ? AND recv_ms <= ?
            ORDER BY recv_ms DESC, capture_seq DESC
            LIMIT 1
            """,
            (condition_id, trade_ts_ms),
        ).fetchone()
        if row is None:
            return None
        recv_ms, yes_bid, yes_ask, no_bid, no_ask = row
        if outcome_side == "YES":
            return int(recv_ms), yes_bid, yes_ask, source.condition_meta.get(condition_id, {}).get("tick_size")
        if outcome_side == "NO":
            return int(recv_ms), no_bid, no_ask, source.condition_meta.get(condition_id, {}).get("tick_size")
        return int(recv_ms), None, None, source.condition_meta.get(condition_id, {}).get("tick_size")
    row = source.conn.execute(
        """
        SELECT recv_unix_ms, bid, ask
        FROM md_book_l1
        WHERE slug = ? AND side = ? AND recv_unix_ms <= ?
        ORDER BY recv_unix_ms DESC, capture_seq DESC
        LIMIT 1
        """,
        (slug, asset_id, trade_ts_ms),
    ).fetchone()
    if row is None:
        return None
    recv_ms, bid, ask = row
    return int(recv_ms), bid, ask, 0.01


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(r["classification"] for r in rows)
    proximity_counts = Counter(r["proximity_classification"] for r in rows if r.get("proximity_classification"))
    by_outcome = defaultdict(Counter)
    ages = []
    matched_market_rows = 0
    for row in rows:
        by_outcome[row["outcome_side"]][row["classification"]] += 1
        if row["market_present"]:
            matched_market_rows += 1
        if row["age_ms"] is not None and row["classification"] != "no_book":
            ages.append(int(row["age_ms"]))

    maker_like = counts["maker_at_bid"]
    taker_like = counts["taker_at_ask"]
    decidable = maker_like + taker_like
    maker_proximity = proximity_counts["maker_proximity"]
    taker_proximity = proximity_counts["taker_proximity"]
    proximity_decidable = maker_proximity + taker_proximity
    ages_sorted = sorted(ages)

    def pct(p: float) -> float | None:
        if not ages_sorted:
            return None
        idx = min(len(ages_sorted) - 1, max(0, round((len(ages_sorted) - 1) * p)))
        return float(ages_sorted[idx])

    return {
        "rows": len(rows),
        "matched_market_rows": matched_market_rows,
        "classification_counts": dict(counts),
        "outcome_side_counts": {k: dict(v) for k, v in by_outcome.items()},
        "maker_like_ratio_among_decidable": (maker_like / decidable) if decidable else None,
        "decidable_rows": decidable,
        "proximity_counts": dict(proximity_counts),
        "maker_proximity_ratio_among_decidable": (
            maker_proximity / proximity_decidable
        )
        if proximity_decidable
        else None,
        "proximity_decidable_rows": proximity_decidable,
        "book_age_ms": {
            "p50": pct(0.50),
            "p90": pct(0.90),
            "p99": pct(0.99),
        },
    }


def main() -> None:
    args = parse_args()
    db_paths = args.db or find_default_replay_dbs()
    trades = load_trades(args.trades, args.only_btc_5m)
    sources = [load_replay_source(path) for path in db_paths]
    try:
        out_rows: list[dict[str, Any]] = []
        for trade in trades:
            condition_id = str(trade.get("conditionId") or "")
            slug = str(trade.get("slug") or "")
            asset_id = str(trade.get("asset") or "")
            outcome_side = side_from_outcome(str(trade.get("outcome") or ""))
            trade_ts_ms = int(float(trade["timestamp"]) * 1000)
            market_is_present = any(
                market_present(source, condition_id, slug) for source in sources
            )
            bid = ask = None
            age_ms = None
            recv_ms = None
            tick_size = None
            best_book = None
            for source in sources:
                if not market_present(source, condition_id, slug):
                    continue
                book = query_latest_book(
                    source,
                    condition_id=condition_id,
                    slug=slug,
                    asset_id=asset_id,
                    outcome_side=outcome_side,
                    trade_ts_ms=trade_ts_ms,
                )
                if book is None:
                    continue
                if best_book is None or book[0] > best_book[0]:
                    best_book = book
            if best_book is not None:
                recv_ms, bid, ask, tick_size = best_book
                age_ms = trade_ts_ms - recv_ms
            price = float(trade.get("price") or 0.0)
            tol = tick_tol(price, tick_size)
            classification = (
                classify_trade(price, bid, ask, age_ms, args.max_book_age_ms, tol)
                if market_is_present
                else "no_local_market"
            )
            proximity_classification = (
                classify_proximity(price, bid, ask, age_ms, args.max_book_age_ms, tol)
                if market_is_present
                else None
            )
            out_rows.append(
                {
                    "slug": slug,
                    "condition_id": condition_id,
                    "asset_id": asset_id,
                    "market_present": market_is_present,
                    "timestamp_s": int(trade.get("timestamp") or 0),
                    "trade_ts_ms": trade_ts_ms,
                    "recv_ms": recv_ms,
                    "age_ms": age_ms,
                    "outcome": str(trade.get("outcome") or ""),
                    "outcome_side": outcome_side,
                    "price": price,
                    "size": float(trade.get("size") or 0.0),
                    "bid": bid,
                    "ask": ask,
                    "tick_size": tick_size,
                    "tol": tol,
                    "classification": classification,
                    "proximity_classification": proximity_classification,
                    "tx": str(trade.get("transactionHash") or ""),
                }
            )
    finally:
        for source in sources:
            source.conn.close()

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    with args.out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "slug",
                "condition_id",
                "asset_id",
                "market_present",
                "timestamp_s",
                "trade_ts_ms",
                "recv_ms",
                "age_ms",
                "outcome",
                "outcome_side",
                "price",
                "size",
                "bid",
                "ask",
                "tick_size",
                "tol",
                "classification",
                "proximity_classification",
                "tx",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    summary = {
        "db_paths": [str(path) for path in db_paths],
        "trades_path": str(args.trades),
        "max_book_age_ms": args.max_book_age_ms,
        "summary": summarize(out_rows),
    }
    args.out_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"wrote {args.out_csv}")
    print(f"wrote {args.out_json}")


if __name__ == "__main__":
    main()
