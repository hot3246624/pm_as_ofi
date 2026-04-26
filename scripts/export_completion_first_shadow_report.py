#!/usr/bin/env python3
"""Export completion_first shadow validation report from replay sqlite.

Usage:
  python scripts/export_completion_first_shadow_report.py --date 2026-04-26
  python scripts/export_completion_first_shadow_report.py --db data/replay/2026-04-26/crypto_5m.sqlite
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD under data/replay/<date>/crypto_5m.sqlite")
    p.add_argument("--db", help="Explicit sqlite path")
    p.add_argument("--output-dir", help="Optional explicit output dir")
    return p.parse_args()


def load_json(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def as_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def as_int(v: Any) -> int:
    try:
        if v is None:
            return 0
        return int(v)
    except Exception:
        return 0


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    idx = round((len(vals) - 1) * max(0.0, min(1.0, q)))
    return vals[idx]


def resolve_db_and_output(args: argparse.Namespace) -> tuple[Path, Path]:
    if args.db:
        db_path = Path(args.db)
        if not db_path.exists():
            raise SystemExit(f"db not found: {db_path}")
        out_dir = Path(args.output_dir) if args.output_dir else db_path.parent
        return db_path, out_dir
    if not args.date:
        raise SystemExit("either --db or --date is required")
    db_path = Path("data/replay") / args.date / "crypto_5m.sqlite"
    if not db_path.exists():
        raise SystemExit(f"db not found: {db_path}")
    out_dir = Path(args.output_dir) if args.output_dir else db_path.parent
    return db_path, out_dir


def event_counts(conn: sqlite3.Connection, slug: str) -> Dict[str, int]:
    rows = conn.execute(
        """
        SELECT event, COUNT(*)
        FROM own_inventory_events
        WHERE slug = ?
          AND event IN (
            'completion_first_seed_built',
            'completion_first_completion_built',
            'completion_first_same_side_add_blocked',
            'completion_first_merge_requested',
            'completion_first_merge_executed',
            'completion_first_redeem_requested'
          )
        GROUP BY event
        """,
        (slug,),
    ).fetchall()
    counts = {str(event): int(cnt) for event, cnt in rows}
    for key in (
        "completion_first_seed_built",
        "completion_first_completion_built",
        "completion_first_same_side_add_blocked",
        "completion_first_merge_requested",
        "completion_first_merge_executed",
        "completion_first_redeem_requested",
    ):
        counts.setdefault(key, 0)
    return counts


def latest_payload(conn: sqlite3.Connection, slug: str, event: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT payload_json
        FROM own_inventory_events
        WHERE slug = ? AND event = ?
        ORDER BY recv_unix_ms DESC, capture_seq DESC
        LIMIT 1
        """,
        (slug, event),
    ).fetchone()
    if not row:
        return {}
    return load_json(row[0])


def strategy_for_slug(conn: sqlite3.Connection, slug: str) -> str:
    row = conn.execute(
        """
        SELECT strategy
        FROM market_meta
        WHERE slug = ?
        ORDER BY recv_unix_ms ASC
        LIMIT 1
        """,
        (slug,),
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else ""


def iter_completion_first_slugs(conn: sqlite3.Connection) -> Iterable[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT slug
        FROM market_meta
        WHERE strategy IN ('completion_first', 'xuan_clone')
        ORDER BY slug
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def build_rows(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for slug in iter_completion_first_slugs(conn):
        summary = latest_payload(conn, slug, "completion_first_shadow_summary")
        tranche = latest_payload(conn, slug, "pair_tranche_events")
        budget = latest_payload(conn, slug, "pair_budget_events")
        capital = latest_payload(conn, slug, "capital_state_events")
        counts = event_counts(conn, slug)

        source = summary or tranche
        row = {
            "slug": slug,
            "strategy": strategy_for_slug(conn, slug),
            "summary_present": bool(summary),
            "active_tranche_id": source.get("active_tranche_id"),
            "active_state": source.get("active_state"),
            "winner_side": summary.get("winner_side"),
            "residual_side": summary.get("residual_side", tranche.get("first_side")),
            "residual_qty": as_float(summary.get("residual_qty", tranche.get("residual_qty"))),
            "pairable_qty": as_float(summary.get("pairable_qty", tranche.get("pairable_qty"))),
            "buy_fill_count": as_int(summary.get("buy_fill_count", tranche.get("buy_fill_count"))),
            "same_side_run_count": as_int(summary.get("same_side_run_count", tranche.get("same_side_add_count"))),
            "clean_closed_episode_ratio": as_float(
                summary.get("clean_closed_episode_ratio", tranche.get("clean_closed_episode_ratio"))
            ),
            "same_side_add_qty_ratio": as_float(
                summary.get("same_side_add_qty_ratio", tranche.get("same_side_add_qty_ratio"))
            ),
            "episode_close_delay_p50": as_float(
                summary.get("episode_close_delay_p50", tranche.get("episode_close_delay_p50"))
            ),
            "episode_close_delay_p90": as_float(
                summary.get("episode_close_delay_p90", tranche.get("episode_close_delay_p90"))
            ),
            "conditional_second_same_side_would_allow": as_int(
                summary.get("conditional_second_same_side_would_allow")
            ),
            "surplus_bank": as_float(summary.get("surplus_bank", budget.get("surplus_bank"))),
            "repair_budget_available": as_float(
                summary.get("repair_budget_available", budget.get("repair_budget_available"))
            ),
            "mergeable_full_sets": as_float(
                summary.get("mergeable_full_sets", capital.get("mergeable_full_sets"))
            ),
            "working_capital": as_float(summary.get("working_capital", capital.get("working_capital"))),
            "locked_capital_ratio": as_float(
                summary.get("locked_capital_ratio", capital.get("locked_capital_ratio"))
            ),
            "seed_built_count": counts["completion_first_seed_built"],
            "completion_built_count": counts["completion_first_completion_built"],
            "same_side_add_blocked_count": counts["completion_first_same_side_add_blocked"],
            "merge_requested_count": counts["completion_first_merge_requested"],
            "merge_executed_count": counts["completion_first_merge_executed"],
            "redeem_requested_count": counts["completion_first_redeem_requested"],
        }
        out.append(row)
    return out


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "slug",
        "strategy",
        "summary_present",
        "active_tranche_id",
        "active_state",
        "winner_side",
        "residual_side",
        "residual_qty",
        "pairable_qty",
        "buy_fill_count",
        "same_side_run_count",
        "clean_closed_episode_ratio",
        "same_side_add_qty_ratio",
        "episode_close_delay_p50",
        "episode_close_delay_p90",
        "conditional_second_same_side_would_allow",
        "surplus_bank",
        "repair_budget_available",
        "mergeable_full_sets",
        "working_capital",
        "locked_capital_ratio",
        "seed_built_count",
        "completion_built_count",
        "same_side_add_blocked_count",
        "merge_requested_count",
        "merge_executed_count",
        "redeem_requested_count",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, rows: List[Dict[str, Any]]) -> None:
    clean = [as_float(r["clean_closed_episode_ratio"]) for r in rows]
    same_side = [as_float(r["same_side_add_qty_ratio"]) for r in rows]
    close_p50 = [as_float(r["episode_close_delay_p50"]) for r in rows]
    close_p90 = [as_float(r["episode_close_delay_p90"]) for r in rows]
    residual = [as_float(r["residual_qty"]) for r in rows]
    summary = {
        "market_count": len(rows),
        "summary_present_count": sum(1 for r in rows if r["summary_present"]),
        "clean_closed_episode_ratio_median": median(clean) if clean else 0.0,
        "clean_closed_episode_ratio_p90": percentile(clean, 0.90),
        "same_side_add_qty_ratio_median": median(same_side) if same_side else 0.0,
        "same_side_add_qty_ratio_p90": percentile(same_side, 0.90),
        "episode_close_delay_p50_median": median(close_p50) if close_p50 else 0.0,
        "episode_close_delay_p90_median": median(close_p90) if close_p90 else 0.0,
        "residual_qty_median": median(residual) if residual else 0.0,
        "merge_requested_total": sum(as_int(r["merge_requested_count"]) for r in rows),
        "merge_executed_total": sum(as_int(r["merge_executed_count"]) for r in rows),
        "redeem_requested_total": sum(as_int(r["redeem_requested_count"]) for r in rows),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    db_path, out_dir = resolve_db_and_output(args)
    conn = sqlite3.connect(db_path)
    try:
        rows = build_rows(conn)
    finally:
        conn.close()

    csv_path = out_dir / "completion_first_shadow_report.csv"
    summary_path = out_dir / "completion_first_shadow_summary.json"
    write_csv(csv_path, rows)
    write_summary(summary_path, rows)
    print(f"✅ completion_first report: {csv_path}")
    print(f"✅ completion_first summary: {summary_path}")


if __name__ == "__main__":
    main()
