#!/usr/bin/env python3
"""Calibrate PGT shadow book-touch fills against public replay trades.

The PGT dry-run executor can mark maker fills when the public L1 book touches
our bid. That is useful for strategy shape, but optimistic for queue position.
This script keeps the recorder side read-only, opens replay SQLite files in
read-only immutable mode, and checks whether a later public SELL trade prints at
or through the shadow bid.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RECORDER_ROOT = REPO_ROOT / "data" / "recorder"
DEFAULT_REPLAY_ROOT = Path("/Users/hot/web3Scientist/poly_trans_research/data/replay")
DEFAULT_WINDOWS_MS = (1_000, 5_000, 30_000, 120_000)


@dataclass(frozen=True)
class ShadowTouch:
    round_id: int
    slug: str
    condition_id: str
    date: str
    source: str
    side: str
    price: float
    size: float
    accepted_ms: int | None
    touch_ms: int
    slot: str
    order_end_ms: int | None = None


@dataclass(frozen=True)
class ReplayMarket:
    db_path: Path
    condition_id: str
    slug: str
    start_ms: int
    end_ms: int


@dataclass
class FollowThrough:
    touch: ShadowTouch
    matched_replay: bool
    pre_touch_sell_count: int = 0
    pre_touch_sell_size: float = 0.0
    first_after_delay_ms: int | None = None
    first_after_price: float | None = None
    window_counts: dict[int, int] | None = None
    window_sizes: dict[int, float] | None = None

    def hit_within(self, window_ms: int) -> bool:
        return bool(self.window_counts and self.window_counts.get(window_ms, 0) > 0)

    def size_ge_order_within(self, window_ms: int) -> bool:
        if not self.window_sizes:
            return False
        return self.window_sizes.get(window_ms, 0.0) + 1e-9 >= self.touch.size


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--instance", default="xuan_ladder_v1_brake_full")
    p.add_argument("--date", action="append", help="Recorder date YYYY-MM-DD; repeatable")
    p.add_argument("--recorder-root", default=str(DEFAULT_RECORDER_ROOT))
    p.add_argument("--replay-root", default=str(DEFAULT_REPLAY_ROOT))
    p.add_argument(
        "--windows-ms",
        default=",".join(str(v) for v in DEFAULT_WINDOWS_MS),
        help="Comma-separated follow-through windows after book-touch time",
    )
    p.add_argument("--last", type=int, default=0, help="Only analyze last N touches after loading")
    p.add_argument("--details", type=int, default=20, help="Print up to N touch rows")
    p.add_argument(
        "--legacy-orders",
        action="store_true",
        help="Also treat historical order_accepted lifetimes as fill candidates",
    )
    p.add_argument("--json", action="store_true")
    return p.parse_args()


def parse_windows(value: str) -> list[int]:
    out = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return sorted(set(out))


def event_payload(row: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    payload = row.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        return payload.get("event"), data if isinstance(data, dict) else {}
    return row.get("event"), row.get("data") if isinstance(row.get("data"), dict) else {}


def round_id_from_slug(slug: str) -> int:
    return int(slug.rsplit("-", 1)[1])


def close_active_order(
    touches: list[ShadowTouch],
    slug: str,
    date: str,
    condition_id: str,
    slot: str,
    order: dict[str, Any] | None,
    end_ms: int,
) -> None:
    if not order:
        return
    side = str(order.get("side") or "").upper()
    if side not in {"YES", "NO"}:
        return
    accepted_ms = order.get("accepted_ms")
    if accepted_ms is None:
        return
    touches.append(
        ShadowTouch(
            round_id=round_id_from_slug(slug),
            slug=slug,
            condition_id=condition_id,
            date=date,
            source="legacy_order_lifetime",
            side=side,
            price=float(order.get("price") or 0.0),
            size=float(order.get("size") or 0.0),
            accepted_ms=int(accepted_ms),
            touch_ms=int(accepted_ms),
            slot=slot,
            order_end_ms=end_ms,
        )
    )


def load_touches(path: Path, legacy_orders: bool = False) -> list[ShadowTouch]:
    slug = path.parent.name
    date = path.parent.parent.name
    active: dict[str, dict[str, Any]] = {}
    touches: list[ShadowTouch] = []
    for line in path.open(encoding="utf-8", errors="ignore"):
        try:
            row = json.loads(line)
        except Exception:
            continue
        recv_ms = int(row.get("recv_unix_ms") or 0)
        condition_id = str(row.get("condition_id") or row.get("market_id") or "")
        event, data = event_payload(row)
        if event == "order_accepted" and str(data.get("direction", "")).lower() == "buy":
            slot = str(data.get("slot") or "")
            if slot:
                active[slot] = {
                    "accepted_ms": recv_ms,
                    "side": str(data.get("side") or "").upper(),
                    "price": float(data.get("price") or 0.0),
                    "size": float(data.get("size") or 0.0),
                }
        elif event == "cancel_ack":
            slot = str(data.get("slot") or "")
            if slot:
                order = active.pop(slot, None)
                if legacy_orders:
                    close_active_order(touches, slug, date, condition_id, slot, order, recv_ms)
        elif event == "dry_run_touch_fill_confirmed":
            side = str(data.get("side") or "").upper()
            slot = str(data.get("slot") or "")
            source = str(data.get("source") or "")
            active_order = active.get(slot) or {}
            touches.append(
                ShadowTouch(
                    round_id=round_id_from_slug(slug),
                    slug=slug,
                    condition_id=condition_id,
                    date=date,
                    source=source,
                    side=side,
                    price=float(data.get("price") or active_order.get("price") or 0.0),
                    size=float(data.get("size") or active_order.get("size") or 0.0),
                    accepted_ms=active_order.get("accepted_ms"),
                    touch_ms=recv_ms,
                    slot=slot,
                    order_end_ms=recv_ms,
                )
            )
        elif event == "fill_snapshot":
            slot = str(data.get("slot") or "")
            if slot:
                order = active.pop(slot, None)
                if legacy_orders:
                    close_active_order(touches, slug, date, condition_id, slot, order, recv_ms)
    if legacy_orders and active:
        # Open orders with no terminal event are bounded by the last recorder row.
        for slot, order in list(active.items()):
            close_active_order(touches, slug, date, "", slot, order, recv_ms)
    return touches


def collect_touches(
    root: Path, instance: str, dates: list[str] | None, legacy_orders: bool = False
) -> list[ShadowTouch]:
    base = root / instance
    touches: list[ShadowTouch] = []
    if dates:
        patterns = []
        for date in dates:
            patterns.append(f"[0-9]*/{date}/btc-updown-5m-*/events.jsonl")
            patterns.append(f"{date}/btc-updown-5m-*/events.jsonl")
    else:
        patterns = [
            "[0-9]*/20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/btc-updown-5m-*/events.jsonl",
            "20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/btc-updown-5m-*/events.jsonl",
        ]
    for pattern in patterns:
        for path in base.glob(pattern):
            touches.extend(load_touches(path, legacy_orders=legacy_orders))
    return sorted(touches, key=lambda t: (t.round_id, t.touch_ms, t.slot))


def connect_ro(path: Path) -> sqlite3.Connection:
    # immutable=1 prevents SQLite from trying to create journal/WAL side files.
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def build_market_index(replay_root: Path, slugs: set[str]) -> dict[str, ReplayMarket]:
    if not slugs:
        return {}
    out: dict[str, ReplayMarket] = {}
    placeholders = ",".join("?" for _ in slugs)
    params = tuple(sorted(slugs))
    for db_path in sorted(replay_root.glob("20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]/crypto_5m.sqlite")):
        try:
            conn = connect_ro(db_path)
        except sqlite3.Error:
            continue
        try:
            query = (
                "SELECT slug, condition_id, start_ms, end_ms "
                f"FROM market_meta WHERE symbol='BTC' AND slug IN ({placeholders})"
            )
            for slug, condition_id, start_ms, end_ms in conn.execute(query, params):
                out[str(slug)] = ReplayMarket(
                    db_path=db_path,
                    condition_id=str(condition_id),
                    slug=str(slug),
                    start_ms=int(start_ms),
                    end_ms=int(end_ms),
                )
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    return out


def trade_summary(
    conn: sqlite3.Connection,
    condition_id: str,
    side: str,
    price: float,
    start_ms: int,
    end_ms: int,
) -> tuple[int, float]:
    row = conn.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(size), 0.0)
        FROM md_trades
        WHERE condition_id=?
          AND UPPER(COALESCE(market_side, ''))=?
          AND UPPER(COALESCE(taker_side, ''))='SELL'
          AND COALESCE(trade_ts_ms, recv_ms) >= ?
          AND COALESCE(trade_ts_ms, recv_ms) <= ?
          AND price <= ? + 1e-9
        """,
        (condition_id, side, start_ms, end_ms, price),
    ).fetchone()
    return int(row[0] or 0), float(row[1] or 0.0)


def first_trade_after(
    conn: sqlite3.Connection,
    condition_id: str,
    side: str,
    price: float,
    start_ms: int,
    end_ms: int,
) -> tuple[int | None, float | None]:
    row = conn.execute(
        """
        SELECT COALESCE(trade_ts_ms, recv_ms), price
        FROM md_trades
        WHERE condition_id=?
          AND UPPER(COALESCE(market_side, ''))=?
          AND UPPER(COALESCE(taker_side, ''))='SELL'
          AND COALESCE(trade_ts_ms, recv_ms) >= ?
          AND COALESCE(trade_ts_ms, recv_ms) <= ?
          AND price <= ? + 1e-9
        ORDER BY COALESCE(trade_ts_ms, recv_ms), capture_seq
        LIMIT 1
        """,
        (condition_id, side, start_ms, end_ms, price),
    ).fetchone()
    if row is None:
        return None, None
    return int(row[0]) - start_ms, float(row[1])


def calibrate(touches: list[ShadowTouch], market_index: dict[str, ReplayMarket], windows_ms: list[int]) -> list[FollowThrough]:
    conns: dict[Path, sqlite3.Connection] = {}
    out: list[FollowThrough] = []
    max_window = max(windows_ms) if windows_ms else 0
    try:
        for touch in touches:
            market = market_index.get(touch.slug)
            if market is None:
                out.append(FollowThrough(touch=touch, matched_replay=False))
                continue
            conn = conns.get(market.db_path)
            if conn is None:
                conn = connect_ro(market.db_path)
                conns[market.db_path] = conn
            pre_count = 0
            pre_size = 0.0
            touch_start_ms = touch.accepted_ms if touch.accepted_ms is not None else touch.touch_ms
            touch_end_ms = touch.order_end_ms if touch.order_end_ms is not None else touch.touch_ms
            if touch.accepted_ms is not None and touch.accepted_ms <= touch_end_ms:
                pre_count, pre_size = trade_summary(
                    conn,
                    market.condition_id,
                    touch.side,
                    touch.price,
                    touch.accepted_ms,
                    touch_end_ms,
                )
            window_counts: dict[int, int] = {}
            window_sizes: dict[int, float] = {}
            for window in windows_ms:
                count, size = trade_summary(
                    conn,
                    market.condition_id,
                    touch.side,
                    touch.price,
                    touch_start_ms,
                    min(touch_start_ms + window, touch_end_ms if touch.order_end_ms else touch_start_ms + window),
                )
                window_counts[window] = count
                window_sizes[window] = size
            first_delay, first_price = first_trade_after(
                conn,
                market.condition_id,
                touch.side,
                touch.price,
                touch_start_ms,
                min(touch_start_ms + max_window, touch_end_ms if touch.order_end_ms else touch_start_ms + max_window),
            )
            out.append(
                FollowThrough(
                    touch=touch,
                    matched_replay=True,
                    pre_touch_sell_count=pre_count,
                    pre_touch_sell_size=pre_size,
                    first_after_delay_ms=first_delay,
                    first_after_price=first_price,
                    window_counts=window_counts,
                    window_sizes=window_sizes,
                )
            )
    finally:
        for conn in conns.values():
            conn.close()
    return out


def pct(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return num / den


def percentile(values: list[float], pct_value: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * pct_value / 100.0
    lo = math.floor(rank)
    hi = min(lo + 1, len(values) - 1)
    frac = rank - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def price_bucket(price: float) -> str:
    buckets = ((0.0, 0.30), (0.30, 0.42), (0.42, 0.45), (0.45, 0.50), (0.50, 1.0))
    for lo, hi in buckets:
        if lo < price <= hi:
            return f"({lo:.2f},{hi:.2f}]"
    return "other"


def summarize(rows: list[FollowThrough], windows_ms: list[int]) -> dict[str, Any]:
    matched = [r for r in rows if r.matched_replay]
    book = [
        r
        for r in matched
        if r.touch.source in {"book_touch", "legacy_order_lifetime"}
    ]
    delays = [r.first_after_delay_ms for r in book if r.first_after_delay_ms is not None]
    out: dict[str, Any] = {
        "touches": len(rows),
        "matched_replay": len(matched),
        "unmatched_replay": len(rows) - len(matched),
        "book_touches_matched": len(book),
        "pre_touch_sell_through": sum(1 for r in book if r.pre_touch_sell_count > 0),
        "pre_touch_sell_through_ratio": pct(sum(1 for r in book if r.pre_touch_sell_count > 0), len(book)),
        "first_after_delay_ms_p50": statistics.median(delays) if delays else None,
        "first_after_delay_ms_p90": percentile([float(v) for v in delays], 90.0),
    }
    for window in windows_ms:
        hit = sum(1 for r in book if r.hit_within(window))
        size_ge = sum(1 for r in book if r.size_ge_order_within(window))
        out[f"hit_within_{window}ms"] = hit
        out[f"hit_within_{window}ms_ratio"] = pct(hit, len(book))
        out[f"size_ge_order_within_{window}ms"] = size_ge
        out[f"size_ge_order_within_{window}ms_ratio"] = pct(size_ge, len(book))
    bucket_rows: dict[str, list[FollowThrough]] = {}
    for row in book:
        bucket_rows.setdefault(price_bucket(row.touch.price), []).append(row)
    out["price_buckets"] = []
    for bucket, items in sorted(bucket_rows.items()):
        item: dict[str, Any] = {
            "bucket": bucket,
            "book_touches": len(items),
            "pre_touch_sell_through_ratio": pct(
                sum(1 for r in items if r.pre_touch_sell_count > 0), len(items)
            ),
        }
        for window in windows_ms:
            item[f"hit_within_{window}ms_ratio"] = pct(
                sum(1 for r in items if r.hit_within(window)), len(items)
            )
            item[f"size_ge_order_within_{window}ms_ratio"] = pct(
                sum(1 for r in items if r.size_ge_order_within(window)), len(items)
            )
        out["price_buckets"].append(item)
    return out


def row_to_dict(row: FollowThrough, windows_ms: list[int]) -> dict[str, Any]:
    item: dict[str, Any] = {
        "round_id": row.touch.round_id,
        "slug": row.touch.slug,
        "source": row.touch.source,
        "side": row.touch.side,
        "price": row.touch.price,
        "size": row.touch.size,
        "accepted_ms": row.touch.accepted_ms,
        "touch_ms": row.touch.touch_ms,
        "order_end_ms": row.touch.order_end_ms,
        "matched_replay": row.matched_replay,
        "pre_touch_sell_count": row.pre_touch_sell_count,
        "pre_touch_sell_size": row.pre_touch_sell_size,
        "first_after_delay_ms": row.first_after_delay_ms,
        "first_after_price": row.first_after_price,
    }
    for window in windows_ms:
        item[f"count_{window}ms"] = (row.window_counts or {}).get(window, 0)
        item[f"size_{window}ms"] = (row.window_sizes or {}).get(window, 0.0)
    return item


def main() -> None:
    args = parse_args()
    windows_ms = parse_windows(args.windows_ms)
    touches = collect_touches(
        Path(args.recorder_root),
        args.instance,
        args.date,
        legacy_orders=args.legacy_orders,
    )
    if args.last > 0:
        touches = touches[-args.last :]
    market_index = build_market_index(Path(args.replay_root), {t.slug for t in touches})
    rows = calibrate(touches, market_index, windows_ms)
    result = {
        "instance": args.instance,
        "dates": args.date,
        "windows_ms": windows_ms,
        "summary": summarize(rows, windows_ms),
        "details": [row_to_dict(r, windows_ms) for r in rows[-args.details :]],
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    s = result["summary"]
    print(
        f"instance={args.instance} touches={s['touches']} matched={s['matched_replay']} "
        f"unmatched={s['unmatched_replay']} book_matched={s['book_touches_matched']}"
    )
    print(
        f"pre_touch_sell_through={s['pre_touch_sell_through']} "
        f"ratio={s['pre_touch_sell_through_ratio']}"
    )
    for window in windows_ms:
        print(
            f"after_{window}ms: hit={s[f'hit_within_{window}ms']} "
            f"ratio={s[f'hit_within_{window}ms_ratio']} "
            f"size_ge_order={s[f'size_ge_order_within_{window}ms']} "
            f"size_ge_order_ratio={s[f'size_ge_order_within_{window}ms_ratio']}"
        )
    print(
        f"first_after_delay_ms p50={s['first_after_delay_ms_p50']} "
        f"p90={s['first_after_delay_ms_p90']}"
    )
    if s["price_buckets"]:
        print("price_buckets:")
        for bucket in s["price_buckets"]:
            print("  " + json.dumps(bucket, ensure_ascii=False, sort_keys=True))
    if args.details:
        print("details_tail:")
        for item in result["details"]:
            print("  " + json.dumps(item, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
