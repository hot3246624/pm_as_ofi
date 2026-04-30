#!/usr/bin/env python3
"""Validate poly_trans_research replay DB coverage for PGT offline replay.

Example:
  python scripts/validate_poly_trans_replay_coverage.py \
    --db /path/to/poly_trans_research/data/replay/2026-04-30/crypto_5m.sqlite \
    --symbol btc --output-dir data/poly_trans_validation

  python scripts/validate_poly_trans_replay_coverage.py \
    --replay-root /path/to/poly_trans_research/data/replay \
    --date 2026-04-28 --date 2026-04-29 --date 2026-04-30
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


REQUIRED_TABLES = ("market_meta", "md_book_l1", "md_trades")
OPTIONAL_TABLES = ("settlement_records", "xuan_trades", "xuan_activity")


@dataclass
class RoundCoverage:
    db: str
    slug: str
    condition_id: str
    symbol: str
    start_ms: int
    end_ms: int
    book_rows_total: int
    book_rows_active: int
    book_rows_open_60s: int
    book_rows_tail_180s: int
    book_rows_post_45s: int
    book_first_ms: int | None
    book_last_ms: int | None
    book_max_gap_ms: int | None
    trade_rows_total: int
    trade_rows_active: int
    trade_rows_post_45s: int
    settlement_rows: int
    xuan_trade_rows: int | None

    @property
    def usable_for_pgt_replay(self) -> bool:
        return self.book_rows_active > 0 and self.trade_rows_total >= 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db", action="append", help="Replay sqlite path; repeatable")
    p.add_argument("--replay-root", help="poly_trans_research/data/replay root")
    p.add_argument("--date", action="append", help="UTC YYYY-MM-DD under --replay-root; repeatable")
    p.add_argument("--symbol", default="btc", help="symbol prefix, default btc")
    p.add_argument("--hours", type=float, default=72.0, help="expected collection horizon")
    p.add_argument("--output-dir", default="data/poly_trans_validation")
    p.add_argument("--skip-gaps", action="store_true", help="skip max book gap calculation")
    return p.parse_args()


def resolve_dbs(args: argparse.Namespace) -> list[Path]:
    out: list[Path] = []
    for raw in args.db or []:
        out.append(Path(raw).expanduser())
    if args.replay_root and args.date:
        root = Path(args.replay_root).expanduser()
        for day in args.date:
            out.append(root / day / "crypto_5m.sqlite")
    if not out:
        raise SystemExit("provide --db or --replay-root with --date")
    missing = [str(p) for p in out if not p.exists()]
    if missing:
        raise SystemExit(f"missing DB(s): {missing}")
    return out


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
    ).fetchone()
    return row is not None


def require_tables(conn: sqlite3.Connection, db: Path) -> None:
    missing = [table for table in REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise SystemExit(f"{db}: missing required tables: {', '.join(missing)}")


def slug_matches(symbol: str) -> str:
    return f"{symbol.lower()}-updown-5m-%"


def scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> int:
    row = conn.execute(sql, params).fetchone()
    return int((row[0] if row else 0) or 0)


def scalar_opt_int(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> int | None:
    row = conn.execute(sql, params).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def max_gap_ms(conn: sqlite3.Connection, condition_id: str, lo_ms: int, hi_ms: int) -> int | None:
    rows = conn.execute(
        """
        SELECT recv_ms FROM md_book_l1
        WHERE condition_id = ? AND recv_ms BETWEEN ? AND ?
        ORDER BY recv_ms ASC
        """,
        (condition_id, lo_ms, hi_ms),
    ).fetchall()
    if len(rows) < 2:
        return None
    prev = int(rows[0][0])
    gap = 0
    for row in rows[1:]:
        cur = int(row[0])
        gap = max(gap, cur - prev)
        prev = cur
    return gap


def iter_rounds(conn: sqlite3.Connection, db: Path, symbol: str, skip_gaps: bool) -> Iterable[RoundCoverage]:
    require_tables(conn, db)
    has_settlement = table_exists(conn, "settlement_records")
    has_xuan = table_exists(conn, "xuan_trades")
    rows = conn.execute(
        """
        SELECT condition_id, slug, symbol, start_ms, end_ms
        FROM market_meta
        WHERE lower(slug) LIKE ?
        ORDER BY end_ms ASC
        """,
        (slug_matches(symbol),),
    ).fetchall()
    for condition_id, slug, sym, start_ms, end_ms in rows:
        start_ms = int(start_ms)
        end_ms = int(end_ms)
        cid = str(condition_id)
        book_total = scalar(conn, "SELECT COUNT(*) FROM md_book_l1 WHERE condition_id=?", (cid,))
        book_active = scalar(
            conn,
            "SELECT COUNT(*) FROM md_book_l1 WHERE condition_id=? AND recv_ms BETWEEN ? AND ?",
            (cid, start_ms, end_ms),
        )
        book_open = scalar(
            conn,
            "SELECT COUNT(*) FROM md_book_l1 WHERE condition_id=? AND recv_ms BETWEEN ? AND ?",
            (cid, start_ms, start_ms + 60_000),
        )
        book_tail = scalar(
            conn,
            "SELECT COUNT(*) FROM md_book_l1 WHERE condition_id=? AND recv_ms BETWEEN ? AND ?",
            (cid, max(start_ms, end_ms - 180_000), end_ms),
        )
        book_post = scalar(
            conn,
            "SELECT COUNT(*) FROM md_book_l1 WHERE condition_id=? AND recv_ms BETWEEN ? AND ?",
            (cid, end_ms, end_ms + 45_000),
        )
        trade_total = scalar(conn, "SELECT COUNT(*) FROM md_trades WHERE condition_id=?", (cid,))
        trade_active = scalar(
            conn,
            "SELECT COUNT(*) FROM md_trades WHERE condition_id=? AND COALESCE(trade_ts_ms, recv_ms) BETWEEN ? AND ?",
            (cid, start_ms, end_ms),
        )
        trade_post = scalar(
            conn,
            "SELECT COUNT(*) FROM md_trades WHERE condition_id=? AND COALESCE(trade_ts_ms, recv_ms) BETWEEN ? AND ?",
            (cid, end_ms, end_ms + 45_000),
        )
        settlement_rows = (
            scalar(conn, "SELECT COUNT(*) FROM settlement_records WHERE condition_id=?", (cid,))
            if has_settlement
            else 0
        )
        xuan_rows = (
            scalar(conn, "SELECT COUNT(*) FROM xuan_trades WHERE condition_id=?", (cid,))
            if has_xuan
            else None
        )
        yield RoundCoverage(
            db=str(db),
            slug=str(slug),
            condition_id=cid,
            symbol=str(sym),
            start_ms=start_ms,
            end_ms=end_ms,
            book_rows_total=book_total,
            book_rows_active=book_active,
            book_rows_open_60s=book_open,
            book_rows_tail_180s=book_tail,
            book_rows_post_45s=book_post,
            book_first_ms=scalar_opt_int(conn, "SELECT MIN(recv_ms) FROM md_book_l1 WHERE condition_id=?", (cid,)),
            book_last_ms=scalar_opt_int(conn, "SELECT MAX(recv_ms) FROM md_book_l1 WHERE condition_id=?", (cid,)),
            book_max_gap_ms=None if skip_gaps else max_gap_ms(conn, cid, start_ms, end_ms),
            trade_rows_total=trade_total,
            trade_rows_active=trade_active,
            trade_rows_post_45s=trade_post,
            settlement_rows=settlement_rows,
            xuan_trade_rows=xuan_rows,
        )


def pct(n: int, d: int) -> float:
    return float(n) / float(d) if d else 0.0


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * q
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return values[lo]
    frac = rank - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def summarize(rounds: list[RoundCoverage], hours: float) -> dict[str, Any]:
    total = len(rounds)
    expected = int(round(hours * 12.0))
    active_book = sum(1 for r in rounds if r.book_rows_active > 0)
    open_book = sum(1 for r in rounds if r.book_rows_open_60s > 0)
    tail_book = sum(1 for r in rounds if r.book_rows_tail_180s > 0)
    post_book = sum(1 for r in rounds if r.book_rows_post_45s > 0)
    trades = sum(1 for r in rounds if r.trade_rows_total > 0)
    settlement = sum(1 for r in rounds if r.settlement_rows > 0)
    usable = sum(1 for r in rounds if r.usable_for_pgt_replay)
    gaps = [float(r.book_max_gap_ms) for r in rounds if r.book_max_gap_ms is not None]
    return {
        "rounds": total,
        "expected_rounds_for_hours": expected,
        "round_coverage_ratio_vs_expected": pct(total, expected),
        "rounds_with_active_book": active_book,
        "active_book_ratio": pct(active_book, total),
        "rounds_with_open_60s_book": open_book,
        "open_60s_book_ratio": pct(open_book, total),
        "rounds_with_tail_180s_book": tail_book,
        "tail_180s_book_ratio": pct(tail_book, total),
        "rounds_with_post_45s_book": post_book,
        "post_45s_book_ratio": pct(post_book, total),
        "rounds_with_trades": trades,
        "trades_ratio": pct(trades, total),
        "rounds_with_settlement": settlement,
        "settlement_ratio": pct(settlement, total),
        "rounds_usable_for_pgt_replay": usable,
        "usable_for_pgt_replay_ratio": pct(usable, total),
        "book_max_gap_ms_p50": percentile(gaps, 0.50),
        "book_max_gap_ms_p90": percentile(gaps, 0.90),
        "book_max_gap_ms_p99": percentile(gaps, 0.99),
    }


def main() -> None:
    args = parse_args()
    dbs = resolve_dbs(args)
    rounds: list[RoundCoverage] = []
    table_counts: dict[str, dict[str, int]] = {}
    for db in dbs:
        conn = sqlite3.connect(db)
        try:
            conn.row_factory = sqlite3.Row
            counts: dict[str, int] = {}
            for table in REQUIRED_TABLES + OPTIONAL_TABLES:
                if table_exists(conn, table):
                    counts[table] = scalar(conn, f"SELECT COUNT(*) FROM {table}", ())
            table_counts[str(db)] = counts
            rounds.extend(iter_rounds(conn, db, args.symbol, args.skip_gaps))
        finally:
            conn.close()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"poly_trans_{args.symbol.lower()}_coverage"
    csv_path = output_dir / f"{stem}.csv"
    json_path = output_dir / f"{stem}.json"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(rounds[0]).keys()) if rounds else ["slug"])
        writer.writeheader()
        for row in rounds:
            writer.writerow(asdict(row))

    payload = {
        "symbol": args.symbol.lower(),
        "dbs": [str(p) for p in dbs],
        "table_counts": table_counts,
        "summary": summarize(rounds, args.hours),
        "outputs": {"csv": str(csv_path), "json": str(json_path)},
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2, ensure_ascii=False))
    print(f"wrote {csv_path}")
    print(f"wrote {json_path}")


if __name__ == "__main__":
    main()
