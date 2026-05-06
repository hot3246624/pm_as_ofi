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
    p.add_argument(
        "--include-incomplete",
        action="store_true",
        help="include rounds that have not emitted pgt_shadow_summary yet",
    )
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


def residual_cost_worst_case(
    yes_qty: float | None,
    yes_avg_cost: float | None,
    no_qty: float | None,
    no_avg_cost: float | None,
    paired_qty: float | None,
) -> float | None:
    paired = paired_qty or 0.0
    yes_residual = max((yes_qty or 0.0) - paired, 0.0)
    no_residual = max((no_qty or 0.0) - paired, 0.0)
    if yes_residual <= 1e-9 and no_residual <= 1e-9:
        return 0.0
    if yes_residual > 1e-9 and yes_avg_cost is None:
        return None
    if no_residual > 1e-9 and no_avg_cost is None:
        return None
    return yes_residual * (yes_avg_cost or 0.0) + no_residual * (no_avg_cost or 0.0)


def rel_secs(ms: int | None, end_ms: int | None) -> float | None:
    if ms is None or end_ms is None:
        return None
    return (int(ms) - int(end_ms)) / 1000.0


def normalized_side(v: Any) -> str:
    return str(v or "").strip().upper()


def buy_fill_rows(conn: sqlite3.Connection, slug: str) -> list[tuple[int, str]]:
    rows = conn.execute(
        """
        SELECT recv_unix_ms, payload_json
        FROM own_inventory_events
        WHERE slug = ? AND event = 'fill_snapshot'
        ORDER BY recv_unix_ms ASC, capture_seq ASC
        """,
        (slug,),
    ).fetchall()
    out: list[tuple[int, str]] = []
    for recv_ms, payload_json in rows:
        try:
            payload = json.loads(payload_json or "{}")
        except Exception:
            continue
        if normalized_side(payload.get("direction")) != "BUY":
            continue
        side = normalized_side(payload.get("side"))
        if side not in {"YES", "NO"}:
            continue
        out.append((int(recv_ms), side))
    return out


def order_lifecycle_metrics(
    conn: sqlite3.Connection, slug: str, end_ms: int | None
) -> dict[str, Any]:
    accepted = conn.execute(
        """
        SELECT recv_unix_ms, side, direction, reason, price, size
        FROM own_order_lifecycle
        WHERE slug = ? AND event = 'order_accepted'
        ORDER BY recv_unix_ms ASC, capture_seq ASC
        """,
        (slug,),
    ).fetchall()
    buy_accepts = [
        {
            "recv_ms": int(r[0]),
            "side": normalized_side(r[1]),
            "direction": normalized_side(r[2]),
            "reason": str(r[3] or ""),
            "price": as_float(r[4]),
            "size": as_float(r[5]),
        }
        for r in accepted
        if normalized_side(r[2]) == "BUY" and normalized_side(r[1]) in {"YES", "NO"}
    ]
    buy_fills = buy_fill_rows(conn, slug)

    first_fill_ms = buy_fills[0][0] if buy_fills else None
    first_fill_side = buy_fills[0][1] if buy_fills else None
    first_cover_ms = None
    if first_fill_side:
        for fill_ms, side in buy_fills[1:]:
            if side != first_fill_side:
                first_cover_ms = fill_ms
                break

    seed_accepts = [r for r in buy_accepts if r["reason"] == "Provide"]
    first_seed_ms = seed_accepts[0]["recv_ms"] if seed_accepts else None

    pre_fill_cutoff = first_fill_ms if first_fill_ms is not None else end_ms
    initial_seed_accepts = [
        r
        for r in seed_accepts
        if pre_fill_cutoff is None or r["recv_ms"] <= pre_fill_cutoff
    ]
    seen_seed_sides: set[str] = set()
    dual_seed_ms = None
    for row in initial_seed_accepts:
        seen_seed_sides.add(row["side"])
        if len(seen_seed_sides) >= 2:
            dual_seed_ms = row["recv_ms"]
            break

    completion_accepts = [
        r
        for r in buy_accepts
        if r["reason"] == "Hedge"
        and (first_fill_ms is None or r["recv_ms"] >= first_fill_ms)
    ]
    first_completion_accept_ms = (
        completion_accepts[0]["recv_ms"] if completion_accepts else None
    )

    same_side_add_accepts: list[dict[str, Any]] = []
    if first_fill_ms is not None and first_fill_side:
        cover_cutoff = first_cover_ms if first_cover_ms is not None else end_ms
        same_side_add_accepts = [
            r
            for r in seed_accepts
            if r["side"] == first_fill_side
            and r["recv_ms"] > first_fill_ms
            and (cover_cutoff is None or r["recv_ms"] <= cover_cutoff)
        ]
    same_side_add_accept_qty = sum(
        float(r["size"] or 0.0) for r in same_side_add_accepts
    )

    cancel_rows = conn.execute(
        """
        SELECT recv_unix_ms, reason
        FROM own_order_lifecycle
        WHERE slug = ? AND event IN ('cancel_ack', 'cancel_sent')
        ORDER BY recv_unix_ms ASC, capture_seq ASC
        """,
        (slug,),
    ).fetchall()
    first_harvest_cancel_ms = next(
        (
            int(recv_ms)
            for recv_ms, reason in cancel_rows
            if str(reason or "") == "EndgameRiskGate"
        ),
        None,
    )

    seed_live_until_ms = first_fill_ms or first_harvest_cancel_ms or end_ms
    seed_live_secs = (
        (seed_live_until_ms - first_seed_ms) / 1000.0
        if first_seed_ms is not None and seed_live_until_ms is not None
        else None
    )

    return {
        "first_seed_accept_rel_s": rel_secs(first_seed_ms, end_ms),
        "dual_seed_accept_rel_s": rel_secs(dual_seed_ms, end_ms),
        "first_buy_fill_rel_s": rel_secs(first_fill_ms, end_ms),
        "first_cover_fill_rel_s": rel_secs(first_cover_ms, end_ms),
        "first_completion_accept_rel_s": rel_secs(first_completion_accept_ms, end_ms),
        "first_same_side_add_accept_rel_s": rel_secs(
            same_side_add_accepts[0]["recv_ms"] if same_side_add_accepts else None,
            end_ms,
        ),
        "harvest_cancel_first_rel_s": rel_secs(first_harvest_cancel_ms, end_ms),
        "first_seed_to_first_fill_s": (
            (first_fill_ms - first_seed_ms) / 1000.0
            if first_seed_ms is not None and first_fill_ms is not None
            else None
        ),
        "first_completion_delay_s": (
            (first_cover_ms - first_fill_ms) / 1000.0
            if first_fill_ms is not None and first_cover_ms is not None
            else None
        ),
        "seed_live_before_first_fill_or_cancel_s": seed_live_secs,
        "initial_seed_accept_count": len(initial_seed_accepts),
        "initial_seed_side_count": len({r["side"] for r in initial_seed_accepts}),
        "completion_accept_count": len(completion_accepts),
        "same_side_add_accept_count_before_cover": len(same_side_add_accepts),
        "same_side_add_accept_qty_before_cover": same_side_add_accept_qty,
        "maker_buy_accept_count": len(seed_accepts) + len(completion_accepts),
    }


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
        has_shadow_summary = bool(summary_payload)
        clean_ratio = float(metric[0]) if metric and metric[0] is not None else None
        same_side_ratio = float(metric[1]) if metric and metric[1] is not None else None
        payload_json = metric[2] if metric else None
        round_buy_fill_count = payload_metric(payload_json, "round_buy_fill_count")
        summary_paired_qty = as_float(summary_payload.get("paired_qty"))
        summary_pair_cost = as_float(summary_payload.get("pair_cost"))
        summary_residual_qty = as_float(summary_payload.get("residual_qty"))
        summary_yes_qty = as_float(summary_payload.get("yes_qty"))
        summary_yes_avg_cost = as_float(summary_payload.get("yes_avg_cost"))
        summary_no_qty = as_float(summary_payload.get("no_qty"))
        summary_no_avg_cost = as_float(summary_payload.get("no_avg_cost"))
        summary_locked_pnl = (
            summary_paired_qty * (1.0 - summary_pair_cost)
            if summary_paired_qty is not None and summary_pair_cost is not None
            else None
        )
        summary_residual_cost_worst_case = residual_cost_worst_case(
            summary_yes_qty,
            summary_yes_avg_cost,
            summary_no_qty,
            summary_no_avg_cost,
            summary_paired_qty,
        )
        summary_worst_case_pnl = (
            summary_locked_pnl - summary_residual_cost_worst_case
            if summary_locked_pnl is not None
            and summary_residual_cost_worst_case is not None
            else None
        )
        summary_turnover_cost = (
            summary_paired_qty * summary_pair_cost + summary_residual_cost_worst_case
            if summary_paired_qty is not None
            and summary_pair_cost is not None
            and summary_residual_cost_worst_case is not None
            else None
        )
        taker_shadow_would_close = as_float(
            summary_payload.get("pgt_taker_shadow_would_close")
        )
        dispatch_taker_close = as_float(summary_payload.get("pgt_dispatch_taker_close"))
        tranche_opened_count = count_event(conn, slug, "tranche_opened")
        same_side_add_count = count_event(conn, slug, "same_side_add_before_covered")
        merge_skipped_count = count_event(conn, slug, "merge_skipped")
        merge_requested_count = count_event(conn, slug, "merge_requested")
        merge_executed_count = count_event(conn, slug, "merge_executed")
        redeem_requested_count = count_event(conn, slug, "redeem_requested")
        lifecycle = order_lifecycle_metrics(conn, slug, end_ms)
        lifecycle_same_side_qty = as_float(
            lifecycle.get("same_side_add_accept_qty_before_cover")
        )
        if (
            (same_side_ratio is None or same_side_ratio <= 1e-12)
            and lifecycle_same_side_qty is not None
            and lifecycle_same_side_qty > 1e-9
            and (summary_paired_qty or 0.0) > 1e-9
        ):
            same_side_ratio = lifecycle_same_side_qty / float(summary_paired_qty)
        has_any_payload = bool(summary_payload) or metric is not None
        has_active_episode = (
            1.0
            if (round_buy_fill_count or 0.0) > 0.0
            or tranche_opened_count > 0
            or (summary_paired_qty or 0.0) > 0.0
            or (summary_residual_qty or 0.0) > 0.0
            else 0.0
            if has_any_payload
            else None
        )
        residual_round = (
            1.0
            if (summary_residual_qty or 0.0) > 1e-6
            else 0.0
            if has_any_payload
            else None
        )
        row = {
            "slug": slug,
            "round_complete": 1.0 if has_shadow_summary else 0.0,
            "has_shadow_summary": 1.0 if has_shadow_summary else 0.0,
            "clean_closed_episode_ratio": clean_ratio,
            "same_side_add_qty_ratio": same_side_ratio,
            "episode_close_delay_p50": payload_metric(payload_json, "episode_close_delay_p50"),
            "episode_close_delay_p90": payload_metric(payload_json, "episode_close_delay_p90"),
            "round_buy_fill_count": round_buy_fill_count,
            "conditional_second_same_side_would_allow": payload_metric(
                payload_json, "conditional_second_same_side_would_allow"
            ),
            "summary_paired_qty": summary_paired_qty,
            "summary_pair_cost": summary_pair_cost,
            "summary_residual_qty": summary_residual_qty,
            "summary_yes_qty": summary_yes_qty,
            "summary_yes_avg_cost": summary_yes_avg_cost,
            "summary_no_qty": summary_no_qty,
            "summary_no_avg_cost": summary_no_avg_cost,
            "summary_locked_pnl": summary_locked_pnl,
            "summary_residual_cost_worst_case": summary_residual_cost_worst_case,
            "summary_worst_case_pnl": summary_worst_case_pnl,
            "summary_turnover_cost": summary_turnover_cost,
            "has_active_episode": has_active_episode,
            "residual_round": residual_round,
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
            "dispatch_taker_close": dispatch_taker_close,
            "taker_close_dispatch_gap": (
                taker_shadow_would_close - dispatch_taker_close
                if taker_shadow_would_close is not None
                and dispatch_taker_close is not None
                else None
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
            "tranche_opened_count": tranche_opened_count,
            "same_side_add_count": same_side_add_count,
            "merge_skipped_count": merge_skipped_count,
            "merge_requested_count": merge_requested_count,
            "merge_executed_count": merge_executed_count,
            "redeem_requested_count": redeem_requested_count,
            **lifecycle,
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


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * pct / 100.0
    lo = int(rank)
    hi = min(lo + 1, len(values) - 1)
    frac = rank - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def collect(key: str) -> list[float]:
        return [float(r[key]) for r in rows if r.get(key) is not None]

    active_rows = [r for r in rows if r.get("has_active_episode") is not None]
    filled_rows = [
        r for r in rows if float(r.get("has_active_episode") or 0.0) > 0.5
    ]

    def collect_filled(key: str) -> list[float]:
        return [float(r[key]) for r in filled_rows if r.get(key) is not None]

    active_episode_rounds = sum(
        1 for r in active_rows if float(r.get("has_active_episode") or 0.0) > 0.5
    )
    residual_rounds = sum(
        1 for r in rows if float(r.get("residual_round") or 0.0) > 0.5
    )
    taker_close_opportunity_rounds = sum(
        1 for r in rows if float(r.get("taker_shadow_would_close") or 0.0) > 0.0
    )
    taker_close_dispatched_rounds = sum(
        1 for r in rows if float(r.get("dispatch_taker_close") or 0.0) > 0.0
    )
    seed_exposed_rounds = sum(
        1 for r in rows if r.get("first_seed_accept_rel_s") is not None
    )
    dual_seed_rounds = sum(
        1 for r in rows if r.get("dual_seed_accept_rel_s") is not None
    )
    first_fill_rounds = sum(
        1 for r in rows if r.get("first_buy_fill_rel_s") is not None
    )
    total_taker_shadow_would_close = sum(collect("taker_shadow_would_close"))
    total_dispatch_taker_close = sum(collect("dispatch_taker_close"))
    total_summary_locked_pnl = sum(collect("summary_locked_pnl"))
    total_summary_residual_cost_worst_case = sum(
        collect("summary_residual_cost_worst_case")
    )
    total_summary_worst_case_pnl = sum(collect("summary_worst_case_pnl"))
    total_summary_turnover_cost = sum(collect("summary_turnover_cost"))

    return {
        "markets": len(rows),
        "active_episode_rounds": active_episode_rounds,
        "active_episode_ratio": (
            active_episode_rounds / len(active_rows) if active_rows else None
        ),
        "residual_rounds": residual_rounds,
        "residual_round_ratio": (residual_rounds / len(rows) if rows else None),
        "median_clean_closed_episode_ratio": median(collect("clean_closed_episode_ratio")),
        "median_same_side_add_qty_ratio": median(collect("same_side_add_qty_ratio")),
        "median_episode_close_delay_p50": median(collect("episode_close_delay_p50")),
        "median_episode_close_delay_p90": median(collect("episode_close_delay_p90")),
        "median_round_buy_fill_count": median(collect("round_buy_fill_count")),
        "median_conditional_second_same_side_would_allow": median(
            collect("conditional_second_same_side_would_allow")
        ),
        "median_summary_pair_cost": median(collect_filled("summary_pair_cost")),
        "median_summary_paired_qty": median(collect_filled("summary_paired_qty")),
        "median_summary_residual_qty": median(collect("summary_residual_qty")),
        "total_summary_locked_pnl": total_summary_locked_pnl,
        "total_summary_residual_cost_worst_case": total_summary_residual_cost_worst_case,
        "total_summary_worst_case_pnl": total_summary_worst_case_pnl,
        "total_summary_turnover_cost": total_summary_turnover_cost,
        "summary_locked_roi": (
            total_summary_locked_pnl / total_summary_turnover_cost
            if total_summary_turnover_cost > 1e-9
            else None
        ),
        "summary_worst_case_roi": (
            total_summary_worst_case_pnl / total_summary_turnover_cost
            if total_summary_turnover_cost > 1e-9
            else None
        ),
        "median_single_seed_flip_count": median(collect("single_seed_flip_count")),
        "median_dual_seed_quotes": median(collect("dual_seed_quotes")),
        "total_single_seed_released_to_dual": int(
            sum(int(r["single_seed_released_to_dual"] or 0) for r in rows)
        ),
        "median_taker_shadow_would_open": median(collect("taker_shadow_would_open")),
        "median_dispatch_taker_open": median(collect("dispatch_taker_open")),
        "taker_close_opportunity_rounds": taker_close_opportunity_rounds,
        "taker_close_dispatched_rounds": taker_close_dispatched_rounds,
        "taker_close_dispatch_round_ratio": (
            taker_close_dispatched_rounds / taker_close_opportunity_rounds
            if taker_close_opportunity_rounds
            else None
        ),
        "total_taker_shadow_would_close": total_taker_shadow_would_close,
        "total_dispatch_taker_close": total_dispatch_taker_close,
        "seed_exposed_rounds": seed_exposed_rounds,
        "seed_exposed_fill_rounds": first_fill_rounds,
        "seed_exposed_fill_ratio": (
            first_fill_rounds / seed_exposed_rounds if seed_exposed_rounds else None
        ),
        "dual_seed_rounds": dual_seed_rounds,
        "dual_seed_ratio": (
            dual_seed_rounds / seed_exposed_rounds if seed_exposed_rounds else None
        ),
        "median_first_seed_accept_rel_s": median(collect("first_seed_accept_rel_s")),
        "median_dual_seed_accept_rel_s": median(collect("dual_seed_accept_rel_s")),
        "median_first_buy_fill_rel_s": median(collect("first_buy_fill_rel_s")),
        "median_first_seed_to_first_fill_s": median(
            collect("first_seed_to_first_fill_s")
        ),
        "median_first_completion_delay_s": median(collect("first_completion_delay_s")),
        "p90_first_completion_delay_s": percentile(
            collect("first_completion_delay_s"), 90.0
        ),
        "median_seed_live_before_first_fill_or_cancel_s": median(
            collect("seed_live_before_first_fill_or_cancel_s")
        ),
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
    if not args.include_incomplete:
        rows = [r for r in rows if float(r.get("round_complete") or 0.0) > 0.5]

    fieldnames = [
        "slug",
        "round_complete",
        "has_shadow_summary",
        "clean_closed_episode_ratio",
        "same_side_add_qty_ratio",
        "episode_close_delay_p50",
        "episode_close_delay_p90",
        "round_buy_fill_count",
        "conditional_second_same_side_would_allow",
        "summary_paired_qty",
        "summary_pair_cost",
        "summary_residual_qty",
        "summary_yes_qty",
        "summary_yes_avg_cost",
        "summary_no_qty",
        "summary_no_avg_cost",
        "summary_locked_pnl",
        "summary_residual_cost_worst_case",
        "summary_worst_case_pnl",
        "summary_turnover_cost",
        "has_active_episode",
        "residual_round",
        "single_seed_first_side",
        "single_seed_last_side",
        "single_seed_flip_count",
        "dual_seed_quotes",
        "single_seed_released_to_dual",
        "taker_shadow_would_open",
        "taker_shadow_would_close",
        "dispatch_taker_open",
        "dispatch_taker_close",
        "taker_close_dispatch_gap",
        "maker_only_missed_open_round",
        "maker_only_missed_close_round",
        "tranche_opened_count",
        "same_side_add_count",
        "merge_skipped_count",
        "merge_requested_count",
        "merge_executed_count",
        "redeem_requested_count",
        "first_seed_accept_rel_s",
        "dual_seed_accept_rel_s",
        "first_buy_fill_rel_s",
        "first_cover_fill_rel_s",
        "first_completion_accept_rel_s",
        "first_same_side_add_accept_rel_s",
        "harvest_cancel_first_rel_s",
        "first_seed_to_first_fill_s",
        "first_completion_delay_s",
        "seed_live_before_first_fill_or_cancel_s",
        "initial_seed_accept_count",
        "initial_seed_side_count",
        "completion_accept_count",
        "same_side_add_accept_count_before_cover",
        "same_side_add_accept_qty_before_cover",
        "maker_buy_accept_count",
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
    summary["include_incomplete"] = args.include_incomplete
    json_path.write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"✅ pgt shadow report csv: {csv_path}")
    print(f"✅ pgt shadow report json: {json_path}")


if __name__ == "__main__":
    main()
