#!/usr/bin/env python3
"""
Pair-Gated Tranche replay analyzer.

This is intentionally separate from `backtest_pair_arb.py`:
- `backtest_pair_arb.py` remains the legacy pair_arb simulator.
- this script consumes replay-db tranche/capital events emitted by the new
  maker-first PGT path and produces shadow metrics for M1/M2 gating.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True, help="Path to replay sqlite DB")
    p.add_argument("--slug", help="Optional market slug filter")
    p.add_argument("--json", action="store_true", help="Emit JSON only")
    p.add_argument(
        "--close-deadline-secs",
        type=float,
        default=30.0,
        help="Pair close success deadline in seconds (default: 30)",
    )
    return p.parse_args()


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = round((len(ordered) - 1) * max(0.0, min(1.0, p)))
    return ordered[int(pos)]


def maybe_values(rows: Iterable[sqlite3.Row], key: str) -> List[float]:
    out: List[float] = []
    for row in rows:
        value = row[key]
        if value is None:
            continue
        try:
            out.append(float(value))
        except Exception:
            continue
    return out


def safe_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def ratio(num: float, den: float) -> float:
    if den <= 0.0:
        return 0.0
    return num / den


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def compute_tranche_close_stats(
    tranche_rows: List[sqlite3.Row],
    deadline_secs: float,
) -> Dict[str, float]:
    opened_at_ms: Dict[int, int] = {}
    covered_at_ms: Dict[int, int] = {}
    for row in tranche_rows:
        event = str(row["event"] or "")
        tranche_id = int(row["tranche_id"] or 0)
        ts = int(row["recv_unix_ms"] or 0)
        if tranche_id <= 0 or ts <= 0:
            continue
        if event in {"tranche_opened", "tranche_overshoot_rolled"}:
            opened_at_ms.setdefault(tranche_id, ts)
        elif event == "tranche_pair_covered":
            covered_at_ms.setdefault(tranche_id, ts)

    close_delays = []
    closed_within_deadline = 0
    closed_within_60s = 0
    for tranche_id, open_ts in opened_at_ms.items():
        close_ts = covered_at_ms.get(tranche_id)
        if close_ts is None or close_ts < open_ts:
            continue
        delay_secs = (close_ts - open_ts) / 1000.0
        close_delays.append(delay_secs)
        if delay_secs <= deadline_secs + 1e-9:
            closed_within_deadline += 1
        if delay_secs <= 60.0 + 1e-9:
            closed_within_60s += 1

    open_count = len(opened_at_ms)
    covered_count = len(close_delays)
    return {
        "opened_episode_count": float(open_count),
        "covered_episode_count": float(covered_count),
        "uncovered_episode_count": float(max(open_count - covered_count, 0)),
        "close_delay_p50": percentile(close_delays, 0.50),
        "close_delay_p90": percentile(close_delays, 0.90),
        "close_within_deadline_ratio": ratio(closed_within_deadline, open_count),
        "close_within_60s_ratio": ratio(closed_within_60s, open_count),
        "would_need_taker_by_deadline_count": float(max(open_count - closed_within_deadline, 0)),
        "would_need_taker_by_60s_count": float(max(open_count - closed_within_60s, 0)),
    }


def count_order_lifecycle(rows: List[sqlite3.Row]) -> Dict[str, object]:
    by_exec_path: Dict[str, int] = {}
    by_event: Dict[str, int] = {}
    reject_kind_counts: Dict[str, int] = {}
    slot_counts: Dict[str, int] = {}
    for row in rows:
        event = str(row["event"] or "")
        exec_path = str(row["exec_path"] or "")
        reject_kind = str(row["reject_kind"] or "")
        slot = str(row["slot"] or "")
        if event:
            by_event[event] = by_event.get(event, 0) + 1
        if exec_path:
            by_exec_path[exec_path] = by_exec_path.get(exec_path, 0) + 1
        if reject_kind:
            reject_kind_counts[reject_kind] = reject_kind_counts.get(reject_kind, 0) + 1
        if slot:
            slot_counts[slot] = slot_counts.get(slot, 0) + 1
    return {
        "event_counts": by_event,
        "exec_path_counts": by_exec_path,
        "reject_kind_counts": reject_kind_counts,
        "slot_counts": slot_counts,
        "maker_accepted": by_exec_path.get("MAKER_ACCEPTED", 0),
        "taker_accepted": by_exec_path.get("TAKER_ACCEPTED", 0),
        "maker_rejected": by_exec_path.get("MAKER_REJECTED", 0),
        "taker_rejected": by_exec_path.get("TAKER_REJECTED", 0),
        "taker_intents": by_exec_path.get("TAKER_INTENT", 0),
    }


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"db not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        tranche_sql = """
            SELECT slug, recv_unix_ms, event, tranche_id, pair_cost_tranche, pair_cost_fifo_ref, payload_json
            FROM pair_tranche_events
        """
        capital_sql = """
            SELECT slug, recv_unix_ms, working_capital, locked_capital_ratio,
                   clean_closed_episode_ratio, same_side_add_qty_ratio,
                   capital_pressure_merge_batch_shadow, payload_json
            FROM capital_state_events
        """
        lifecycle_sql = """
            SELECT slug, recv_unix_ms, capture_seq, event, slot, side, direction,
                   order_id, reason, purpose, reject_kind, retry, cooldown_ms,
                   price, size, exec_path, payload_json
            FROM own_order_lifecycle
        """
        params: List[object] = []
        if args.slug:
            tranche_sql += " WHERE slug = ?"
            capital_sql += " WHERE slug = ?"
            lifecycle_sql += " WHERE slug = ?"
            params.append(args.slug)
        tranche_sql += " ORDER BY recv_unix_ms, tranche_id"
        capital_sql += " ORDER BY recv_unix_ms"
        lifecycle_sql += " ORDER BY recv_unix_ms, capture_seq"

        tranche_rows = conn.execute(tranche_sql, params).fetchall()
        capital_rows = conn.execute(capital_sql, params).fetchall()
        lifecycle_rows = (
            conn.execute(lifecycle_sql, params).fetchall()
            if table_exists(conn, "own_order_lifecycle")
            else []
        )

        pair_cost_tranche = maybe_values(tranche_rows, "pair_cost_tranche")
        pair_cost_fifo = maybe_values(tranche_rows, "pair_cost_fifo_ref")
        pair_cost_deltas = [
            abs(float(row["pair_cost_tranche"]) - float(row["pair_cost_fifo_ref"]))
            for row in tranche_rows
            if row["pair_cost_tranche"] is not None and row["pair_cost_fifo_ref"] is not None
        ]
        capital_ratios = maybe_values(capital_rows, "locked_capital_ratio")
        close_stats = compute_tranche_close_stats(tranche_rows, args.close_deadline_secs)
        lifecycle_stats = count_order_lifecycle(lifecycle_rows)

        latest_clean_ratio = (
            safe_float(capital_rows[-1]["clean_closed_episode_ratio"]) if capital_rows else 0.0
        )
        latest_same_side_ratio = (
            safe_float(capital_rows[-1]["same_side_add_qty_ratio"]) if capital_rows else 0.0
        )
        latest_merge_batch_shadow = (
            safe_float(capital_rows[-1]["capital_pressure_merge_batch_shadow"]) if capital_rows else 0.0
        )

        tranche_events = {}
        for row in tranche_rows:
            event = str(row["event"] or "")
            tranche_events[event] = tranche_events.get(event, 0) + 1
        locked_capital_ratio_p90 = percentile(capital_ratios, 0.90)
        delta_p50 = percentile(pair_cost_deltas, 0.50)
        provisional_gate_checks = {
            "delta_p50_le_0_05": len(pair_cost_deltas) > 0 and delta_p50 <= 0.05 + 1e-9,
            "clean_closed_episode_ratio_ge_0_90": latest_clean_ratio >= 0.90 - 1e-9,
            "same_side_add_qty_ratio_le_0_10": latest_same_side_ratio <= 0.10 + 1e-9,
            "close_within_60s_ratio_ge_0_90": close_stats["close_within_60s_ratio"] >= 0.90 - 1e-9,
            "locked_capital_ratio_p90_lt_0_60": len(capital_ratios) > 0 and locked_capital_ratio_p90 < 0.60,
        }

        result = {
            "slug": args.slug or "*",
            "pair_cost_tranche_p50": percentile(pair_cost_tranche, 0.50),
            "pair_cost_fifo_p50": percentile(pair_cost_fifo, 0.50),
            "delta_p50": delta_p50,
            "pair_cost_event_count": len(pair_cost_deltas),
            "maker_shadow": {
                "clean_closed_episode_ratio": latest_clean_ratio,
                "same_side_add_qty_ratio": latest_same_side_ratio,
                "opened_episode_count": int(close_stats["opened_episode_count"]),
                "covered_episode_count": int(close_stats["covered_episode_count"]),
                "uncovered_episode_count": int(close_stats["uncovered_episode_count"]),
                "close_delay_p50_secs": close_stats["close_delay_p50"],
                "close_delay_p90_secs": close_stats["close_delay_p90"],
                "close_within_deadline_ratio": close_stats["close_within_deadline_ratio"],
                "close_within_60s_ratio": close_stats["close_within_60s_ratio"],
                "event_counts": tranche_events,
            },
            "taker_shadow": {
                "available": True,
                "close_deadline_secs": args.close_deadline_secs,
                "would_need_taker_by_deadline_count": int(close_stats["would_need_taker_by_deadline_count"]),
                "would_need_taker_by_60s_count": int(close_stats["would_need_taker_by_60s_count"]),
                "assumption": "episodes not covered within deadline are approximated as taker-required",
            },
            "capital_state": {
                "locked_capital_ratio_p50": percentile(capital_ratios, 0.50),
                "locked_capital_ratio_p90": locked_capital_ratio_p90,
                "capital_pressure_merge_batch_shadow_latest": latest_merge_batch_shadow,
                "sample_count": len(capital_rows),
            },
            "order_lifecycle": lifecycle_stats,
            "provisional_h0_gate": {
                "pass": all(provisional_gate_checks.values()),
                "checks": provisional_gate_checks,
            },
            "data_completeness": {
                "pair_tranche_event_rows": len(tranche_rows),
                "capital_state_event_rows": len(capital_rows),
                "order_lifecycle_rows": len(lifecycle_rows),
            },
        }

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return

        print("Pair-Gated Tranche Replay Summary")
        print(f"db: {db_path}")
        print(f"slug: {result['slug']}")
        print(
            "pair_cost_tranche_p50={:.4f} pair_cost_fifo_p50={:.4f} delta_p50={:.4f} samples={}".format(
                result["pair_cost_tranche_p50"],
                result["pair_cost_fifo_p50"],
                result["delta_p50"],
                result["pair_cost_event_count"],
            )
        )
        print(
            "maker_shadow clean_closed_episode_ratio={:.4f} same_side_add_qty_ratio={:.4f}".format(
                result["maker_shadow"]["clean_closed_episode_ratio"],
                result["maker_shadow"]["same_side_add_qty_ratio"],
            )
        )
        print(
            "maker_shadow close_delay_p50={:.2f}s close_delay_p90={:.2f}s close<=deadline({:.0f}s)={:.2%} close<=60s={:.2%}".format(
                result["maker_shadow"]["close_delay_p50_secs"],
                result["maker_shadow"]["close_delay_p90_secs"],
                args.close_deadline_secs,
                result["maker_shadow"]["close_within_deadline_ratio"],
                result["maker_shadow"]["close_within_60s_ratio"],
            )
        )
        print(
            "capital_state locked_capital_ratio_p50={:.4f} locked_capital_ratio_p90={:.4f} batch_shadow={:.1f} samples={}".format(
                result["capital_state"]["locked_capital_ratio_p50"],
                result["capital_state"]["locked_capital_ratio_p90"],
                result["capital_state"]["capital_pressure_merge_batch_shadow_latest"],
                result["capital_state"]["sample_count"],
            )
        )
        print(
            "taker_shadow would_need_taker<=deadline={} would_need_taker<=60s={}".format(
                result["taker_shadow"]["would_need_taker_by_deadline_count"],
                result["taker_shadow"]["would_need_taker_by_60s_count"],
            )
        )
        print(
            "order_lifecycle maker_accepted={} taker_accepted={} maker_rejected={} taker_rejected={} taker_intents={}".format(
                result["order_lifecycle"]["maker_accepted"],
                result["order_lifecycle"]["taker_accepted"],
                result["order_lifecycle"]["maker_rejected"],
                result["order_lifecycle"]["taker_rejected"],
                result["order_lifecycle"]["taker_intents"],
            )
        )
        print(
            "provisional_h0_gate pass={} checks={}".format(
                result["provisional_h0_gate"]["pass"],
                json.dumps(result["provisional_h0_gate"]["checks"], ensure_ascii=False),
            )
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
