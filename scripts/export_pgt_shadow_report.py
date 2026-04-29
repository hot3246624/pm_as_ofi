#!/usr/bin/env python3
"""Export pair_gated_tranche shadow lifecycle report from replay sqlite.

Usage:
  python scripts/export_pgt_shadow_report.py --date 2026-04-26
  python scripts/export_pgt_shadow_report.py --db data/replay/2026-04-26/crypto_5m.sqlite
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    instance_id = os.environ.get("PM_INSTANCE_ID", "").strip()
    default_output_dir = (
        f"data/replay_reports/{instance_id}" if instance_id else "data/replay_reports"
    )
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD replay date")
    p.add_argument("--db", help="direct path to replay sqlite")
    p.add_argument("--output-dir", default=default_output_dir)
    return p.parse_args()


def as_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def resolve_db_path(args: argparse.Namespace) -> Path:
    if args.db:
        return Path(args.db)
    if not args.date:
        raise SystemExit("either --date or --db is required")
    instance_id = os.environ.get("PM_INSTANCE_ID", "").strip()
    if instance_id:
        return Path("data/replay") / instance_id / args.date / "crypto_5m.sqlite"
    return Path("data/replay") / args.date / "crypto_5m.sqlite"


def latest_metric_row(conn: sqlite3.Connection, slug: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT clean_closed_episode_ratio,
               same_side_add_qty_ratio,
               payload_json
        FROM capital_state_events
        WHERE slug = ?
        ORDER BY recv_unix_ms DESC, capture_seq DESC
        LIMIT 1
        """,
        (slug,),
    ).fetchone()


def latest_shadow_summary_payload(
    conn: sqlite3.Connection, slug: str
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT payload_json
        FROM own_inventory_events
        WHERE slug = ? AND event = 'pgt_shadow_summary'
        ORDER BY recv_unix_ms DESC, capture_seq DESC
        LIMIT 1
        """,
        (slug,),
    ).fetchone()
    if not row or not row[0]:
        return None
    try:
        payload = json.loads(row[0])
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def session_end_ms(conn: sqlite3.Connection, slug: str) -> int | None:
    row = conn.execute(
        """
        SELECT payload_json
        FROM market_meta
        WHERE slug = ? AND event = 'session_start'
        ORDER BY recv_unix_ms ASC
        LIMIT 1
        """,
        (slug,),
    ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(row[0] or "{}")
    except Exception:
        return None
    end_ts = payload.get("round_end_ts")
    if end_ts is None:
        return None
    try:
        return int(end_ts) * 1000
    except Exception:
        return None


def first_last_rel_secs(
    conn: sqlite3.Connection, slug: str, event: str, end_ms: int | None
) -> tuple[float | None, float | None]:
    rows = conn.execute(
        """
        SELECT recv_unix_ms
        FROM pair_tranche_events
        WHERE slug = ? AND event = ?
        ORDER BY recv_unix_ms ASC, capture_seq ASC
        """,
        (slug, event),
    ).fetchall()
    if not rows or end_ms is None:
        return None, None
    rel = [(int(r[0]) - end_ms) / 1000.0 for r in rows]
    return rel[0], rel[-1]


def count_event(conn: sqlite3.Connection, slug: str, event: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM pair_tranche_events WHERE slug = ? AND event = ?",
        (slug, event),
    ).fetchone()
    return int(row[0] or 0)


def payload_metric(payload_json: str | None, key: str) -> float | None:
    if not payload_json:
        return None
    try:
        payload = json.loads(payload_json)
    except Exception:
        return None
    v = payload.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def build_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    slugs = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT slug FROM market_meta ORDER BY slug ASC"
        ).fetchall()
    ]
    rows: list[dict[str, Any]] = []
    for slug in slugs:
        end_ms = session_end_ms(conn, slug)
        metric = latest_metric_row(conn, slug)
        summary_payload = latest_shadow_summary_payload(conn, slug) or {}
        clean_ratio = float(metric[0]) if metric and metric[0] is not None else None
        same_side_ratio = float(metric[1]) if metric and metric[1] is not None else None
        payload_json = metric[2] if metric else None
        row = {
            "slug": slug,
            "clean_closed_episode_ratio": clean_ratio,
            "same_side_add_qty_ratio": same_side_ratio,
            "episode_close_delay_p50": payload_metric(payload_json, "episode_close_delay_p50"),
            "episode_close_delay_p90": payload_metric(payload_json, "episode_close_delay_p90"),
            "round_buy_fill_count": payload_metric(payload_json, "round_buy_fill_count"),
            "conditional_second_same_side_would_allow": payload_metric(
                payload_json, "conditional_second_same_side_would_allow"
            ),
            "summary_paired_qty": as_float(summary_payload.get("paired_qty")),
            "summary_pair_cost": as_float(summary_payload.get("pair_cost")),
            "summary_residual_qty": as_float(summary_payload.get("residual_qty")),
            "single_seed_first_side": summary_payload.get("pgt_single_seed_first_side"),
            "single_seed_last_side": summary_payload.get("pgt_single_seed_last_side"),
            "single_seed_flip_count": as_float(
                summary_payload.get("pgt_single_seed_flip_count")
            ),
            "dual_seed_quotes": as_float(summary_payload.get("pgt_dual_seed_quotes")),
            "single_seed_released_to_dual": as_float(
                summary_payload.get("pgt_single_seed_released_to_dual")
            ),
            "taker_shadow_would_open": as_float(
                summary_payload.get("pgt_taker_shadow_would_open")
            ),
            "taker_shadow_would_close": as_float(
                summary_payload.get("pgt_taker_shadow_would_close")
            ),
            "dispatch_taker_open": as_float(
                summary_payload.get("pgt_dispatch_taker_open")
            ),
            "dispatch_taker_close": as_float(
                summary_payload.get("pgt_dispatch_taker_close")
            ),
            "maker_only_missed_open_round": 1.0
            if summary_payload.get("maker_only_missed_open_round")
            else 0.0
            if summary_payload
            else None,
            "maker_only_missed_close_round": 1.0
            if summary_payload.get("maker_only_missed_close_round")
            else 0.0
            if summary_payload
            else None,
            "tranche_opened_count": count_event(conn, slug, "tranche_opened"),
            "same_side_add_count": count_event(conn, slug, "same_side_add_before_covered"),
            "merge_skipped_count": count_event(conn, slug, "merge_skipped"),
            "merge_requested_count": count_event(conn, slug, "merge_requested"),
            "merge_executed_count": count_event(conn, slug, "merge_executed"),
            "redeem_requested_count": count_event(conn, slug, "redeem_requested"),
        }
        row["merge_skipped_first_rel_s"], row["merge_skipped_last_rel_s"] = (
            first_last_rel_secs(conn, slug, "merge_skipped", end_ms)
        )
        row["merge_requested_first_rel_s"], row["merge_requested_last_rel_s"] = (
            first_last_rel_secs(conn, slug, "merge_requested", end_ms)
        )
        row["merge_executed_first_rel_s"], row["merge_executed_last_rel_s"] = (
            first_last_rel_secs(conn, slug, "merge_executed", end_ms)
        )
        row["redeem_requested_first_rel_s"], row["redeem_requested_last_rel_s"] = (
            first_last_rel_secs(conn, slug, "redeem_requested", end_ms)
        )
        rows.append(row)
    return rows


def median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2 == 1:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def collect(key: str) -> list[float]:
        return [float(r[key]) for r in rows if r.get(key) is not None]

    return {
        "markets": len(rows),
        "median_clean_closed_episode_ratio": median(collect("clean_closed_episode_ratio")),
        "median_same_side_add_qty_ratio": median(collect("same_side_add_qty_ratio")),
        "median_episode_close_delay_p50": median(collect("episode_close_delay_p50")),
        "median_episode_close_delay_p90": median(collect("episode_close_delay_p90")),
        "median_round_buy_fill_count": median(collect("round_buy_fill_count")),
        "median_conditional_second_same_side_would_allow": median(
            collect("conditional_second_same_side_would_allow")
        ),
        "median_summary_pair_cost": median(collect("summary_pair_cost")),
        "median_summary_paired_qty": median(collect("summary_paired_qty")),
        "median_summary_residual_qty": median(collect("summary_residual_qty")),
        "median_single_seed_flip_count": median(collect("single_seed_flip_count")),
        "median_dual_seed_quotes": median(collect("dual_seed_quotes")),
        "total_single_seed_released_to_dual": int(
            sum(int(r["single_seed_released_to_dual"] or 0) for r in rows)
        ),
        "median_taker_shadow_would_open": median(collect("taker_shadow_would_open")),
        "median_dispatch_taker_open": median(collect("dispatch_taker_open")),
        "total_maker_only_missed_open_rounds": int(
            sum(int(r["maker_only_missed_open_round"] or 0) for r in rows)
        ),
        "total_maker_only_missed_close_rounds": int(
            sum(int(r["maker_only_missed_close_round"] or 0) for r in rows)
        ),
        "total_merge_skipped": int(sum(r["merge_skipped_count"] for r in rows)),
        "total_merge_requested": int(sum(r["merge_requested_count"] for r in rows)),
        "total_merge_executed": int(sum(r["merge_executed_count"] for r in rows)),
        "total_redeem_requested": int(sum(r["redeem_requested_count"] for r in rows)),
    }


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args)
    if not db_path.exists():
        raise SystemExit(f"replay db not found: {db_path}")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = db_path.parent.name if args.date else db_path.stem
    csv_path = out_dir / f"pgt_shadow_report_{stem}.csv"
    json_path = out_dir / f"pgt_shadow_report_{stem}.json"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = build_rows(conn)
    finally:
        conn.close()

    fieldnames = [
        "slug",
        "clean_closed_episode_ratio",
        "same_side_add_qty_ratio",
        "episode_close_delay_p50",
        "episode_close_delay_p90",
        "round_buy_fill_count",
        "conditional_second_same_side_would_allow",
        "summary_paired_qty",
        "summary_pair_cost",
        "summary_residual_qty",
        "single_seed_first_side",
        "single_seed_last_side",
        "single_seed_flip_count",
        "dual_seed_quotes",
        "single_seed_released_to_dual",
        "taker_shadow_would_open",
        "taker_shadow_would_close",
        "dispatch_taker_open",
        "dispatch_taker_close",
        "maker_only_missed_open_round",
        "maker_only_missed_close_round",
        "tranche_opened_count",
        "same_side_add_count",
        "merge_skipped_count",
        "merge_requested_count",
        "merge_executed_count",
        "redeem_requested_count",
        "merge_skipped_first_rel_s",
        "merge_skipped_last_rel_s",
        "merge_requested_first_rel_s",
        "merge_requested_last_rel_s",
        "merge_executed_first_rel_s",
        "merge_executed_last_rel_s",
        "redeem_requested_first_rel_s",
        "redeem_requested_last_rel_s",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary = build_summary(rows)
    json_path.write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"✅ pgt shadow report csv: {csv_path}")
    print(f"✅ pgt shadow report json: {json_path}")


if __name__ == "__main__":
    main()
