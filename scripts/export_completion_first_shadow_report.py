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


def rate(num: int, den: int) -> float:
    return float(num) / float(den) if den else 0.0


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
            'completion_first_open_gate_decision',
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
        "completion_first_open_gate_decision",
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


def load_event_series(
    conn: sqlite3.Connection, slug: str, event: str
) -> List[tuple[int, Dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT recv_unix_ms, capture_seq, payload_json
        FROM own_inventory_events
        WHERE slug = ? AND event = ?
        ORDER BY recv_unix_ms ASC, capture_seq ASC
        """,
        (slug, event),
    ).fetchall()
    return [(as_int(recv_unix_ms), load_json(payload_json)) for recv_unix_ms, _seq, payload_json in rows]


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


def match_gate_decision(
    gate_decisions: List[Dict[str, Any]], first_side: str, opened_ts: int
) -> Optional[Dict[str, Any]]:
    matches = [
        event
        for event in gate_decisions
        if event.get("side") == first_side
        and opened_ts - 5_000 <= as_int(event.get("recv_unix_ms")) <= opened_ts + 1_000
    ]
    if not matches:
        return None
    return max(matches, key=lambda event: as_int(event.get("recv_unix_ms")))


def reconstruct_episode_metrics(
    conn: sqlite3.Connection, slug: str
) -> Dict[str, Any]:
    gate_rows = load_event_series(conn, slug, "completion_first_open_gate_decision")
    gate_decisions = []
    block_reasons: Dict[str, int] = {}
    score_buckets: Dict[str, int] = {}
    session_buckets: Dict[str, int] = {}
    for recv_unix_ms, payload in gate_rows:
        allowed = bool(payload.get("allowed"))
        block_reason = str(payload.get("block_reason") or "unknown")
        score_bucket = str(payload.get("score_bucket") or "unknown")
        session_bucket = str(payload.get("utc_hour_bucket") if payload.get("utc_hour_bucket") is not None else "unknown")
        gate_decisions.append(
            {
                "recv_unix_ms": recv_unix_ms,
                "side": str(payload.get("side") or ""),
                "allowed": allowed,
                "score_bucket": score_bucket,
                "session_bucket": session_bucket,
            }
        )
        score_buckets[score_bucket] = score_buckets.get(score_bucket, 0) + 1
        session_buckets[session_bucket] = session_buckets.get(session_bucket, 0) + 1
        if not allowed:
            block_reasons[block_reason] = block_reasons.get(block_reason, 0) + 1

    tranche_rows = load_event_series(conn, slug, "pair_tranche_events")
    episodes: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None

    def close_current(ts_ms: int) -> None:
        nonlocal current
        if current is None:
            return
        current["close_ts_ms"] = current.get("close_ts_ms") or ts_ms
        episodes.append(current)
        current = None

    for recv_unix_ms, payload in tranche_rows:
        active_id = payload.get("active_tranche_id")
        active_state = str(payload.get("active_state") or "")
        first_side = str(payload.get("first_side") or "")
        residual_qty = as_float(payload.get("residual_qty"))
        pairable_qty = as_float(payload.get("pairable_qty"))
        same_side_add_count = as_int(payload.get("same_side_add_count"))

        if current is not None and current.get("tranche_id") != active_id:
            close_current(recv_unix_ms)

        if active_id is None:
            continue

        if current is None:
            gate_match = match_gate_decision(gate_decisions, first_side, recv_unix_ms)
            current = {
                "tranche_id": active_id,
                "first_side": first_side,
                "opened_ts_ms": recv_unix_ms,
                "first_opposite_ts_ms": None,
                "close_ts_ms": None,
                "same_side_before_opposite": False,
                "matched_gate_allowed": None if gate_match is None else bool(gate_match.get("allowed")),
                "matched_score_bucket": None if gate_match is None else gate_match.get("score_bucket"),
                "matched_session_bucket": None if gate_match is None else gate_match.get("session_bucket"),
            }

        if current["first_opposite_ts_ms"] is None:
            if same_side_add_count > 0:
                current["same_side_before_opposite"] = True
            if active_state in {"CompletionOnly", "PairCovered", "Closed"} or pairable_qty > 1e-9:
                current["first_opposite_ts_ms"] = recv_unix_ms

        if residual_qty <= 1e-9 and active_state in {"PairCovered", "Closed"}:
            close_current(recv_unix_ms)

    if current is not None:
        episodes.append(current)

    completion_hits = 0
    gate_on_hits = 0
    gate_off_hits = 0
    gate_on_count = 0
    gate_off_count = 0
    opposite_delays: List[float] = []

    for episode in episodes:
        opened = as_int(episode.get("opened_ts_ms"))
        first_opp = episode.get("first_opposite_ts_ms")
        close_ts = episode.get("close_ts_ms")
        same_side_before = bool(episode.get("same_side_before_opposite"))
        first_delay_s = None
        close_delay_s = None
        if first_opp is not None:
            first_delay_s = max(0.0, (as_int(first_opp) - opened) / 1000.0)
            opposite_delays.append(first_delay_s)
        if close_ts is not None:
            close_delay_s = max(0.0, (as_int(close_ts) - opened) / 1000.0)
        hit = (
            first_delay_s is not None
            and first_delay_s <= 30.0
            and close_delay_s is not None
            and close_delay_s <= 60.0
            and not same_side_before
        )
        episode["label_complete_30s"] = hit
        episode["label_clean_close_60s"] = bool(close_delay_s is not None and close_delay_s <= 60.0)
        episode["first_opposite_delay_s"] = first_delay_s
        episode["close_delay_s"] = close_delay_s
        if hit:
            completion_hits += 1
        gate_allowed = episode.get("matched_gate_allowed")
        if gate_allowed is True:
            gate_on_count += 1
            if hit:
                gate_on_hits += 1
        elif gate_allowed is False:
            gate_off_count += 1
            if hit:
                gate_off_hits += 1

    return {
        "episode_count": len(episodes),
        "completion_30s_hit_count": completion_hits,
        "30s_completion_hit_rate": rate(completion_hits, len(episodes)),
        "30s_completion_hit_rate_when_gate_on": rate(gate_on_hits, gate_on_count),
        "30s_completion_hit_rate_when_gate_off": rate(gate_off_hits, gate_off_count),
        "gate_on_episode_count": gate_on_count,
        "gate_on_hit_count": gate_on_hits,
        "gate_off_episode_count": gate_off_count,
        "gate_off_hit_count": gate_off_hits,
        "median_first_opposite_delay_s": median(opposite_delays) if opposite_delays else 0.0,
        "open_candidate_count": len(gate_decisions),
        "open_allowed_count": sum(1 for event in gate_decisions if event.get("allowed")),
        "open_blocked_count": sum(1 for event in gate_decisions if not event.get("allowed")),
        "gate_block_reason_counts": block_reasons,
        "score_bucket_distribution": score_buckets,
        "session_bucket_distribution": session_buckets,
    }


def build_rows(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for slug in iter_completion_first_slugs(conn):
        summary = latest_payload(conn, slug, "completion_first_shadow_summary")
        tranche = latest_payload(conn, slug, "pair_tranche_events")
        budget = latest_payload(conn, slug, "pair_budget_events")
        capital = latest_payload(conn, slug, "capital_state_events")
        counts = event_counts(conn, slug)
        episode_metrics = reconstruct_episode_metrics(conn, slug)

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
            "open_candidate_count": episode_metrics["open_candidate_count"],
            "open_allowed_count": episode_metrics["open_allowed_count"],
            "open_blocked_count": episode_metrics["open_blocked_count"],
            "episode_count": episode_metrics["episode_count"],
            "completion_30s_hit_count": episode_metrics["completion_30s_hit_count"],
            "30s_completion_hit_rate": episode_metrics["30s_completion_hit_rate"],
            "30s_completion_hit_rate_when_gate_on": episode_metrics["30s_completion_hit_rate_when_gate_on"],
            "30s_completion_hit_rate_when_gate_off": episode_metrics["30s_completion_hit_rate_when_gate_off"],
            "median_first_opposite_delay_s": episode_metrics["median_first_opposite_delay_s"],
            "gate_on_episode_count": episode_metrics["gate_on_episode_count"],
            "gate_on_hit_count": episode_metrics["gate_on_hit_count"],
            "gate_off_episode_count": episode_metrics["gate_off_episode_count"],
            "gate_off_hit_count": episode_metrics["gate_off_hit_count"],
            "gate_block_reason_counts_json": json.dumps(
                episode_metrics["gate_block_reason_counts"], ensure_ascii=False, sort_keys=True
            ),
            "score_bucket_distribution_json": json.dumps(
                episode_metrics["score_bucket_distribution"], ensure_ascii=False, sort_keys=True
            ),
            "session_bucket_distribution_json": json.dumps(
                episode_metrics["session_bucket_distribution"], ensure_ascii=False, sort_keys=True
            ),
            "open_gate_event_count": counts["completion_first_open_gate_decision"],
            "seed_built_count": counts["completion_first_seed_built"],
            "completion_built_count": counts["completion_first_completion_built"],
            "same_side_add_blocked_count": counts["completion_first_same_side_add_blocked"],
            "merge_requested_count": counts["completion_first_merge_requested"],
            "merge_executed_count": counts["completion_first_merge_executed"],
            "redeem_requested_count": counts["completion_first_redeem_requested"],
        }
        out.append(row)
    return out


def merge_counter_json(rows: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    merged: Dict[str, int] = {}
    for row in rows:
        payload = load_json(row.get(key))
        for item_key, item_value in payload.items():
            merged[str(item_key)] = merged.get(str(item_key), 0) + as_int(item_value)
    return merged


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
        "open_candidate_count",
        "open_allowed_count",
        "open_blocked_count",
        "episode_count",
        "completion_30s_hit_count",
        "30s_completion_hit_rate",
        "30s_completion_hit_rate_when_gate_on",
        "30s_completion_hit_rate_when_gate_off",
        "median_first_opposite_delay_s",
        "gate_on_episode_count",
        "gate_on_hit_count",
        "gate_off_episode_count",
        "gate_off_hit_count",
        "gate_block_reason_counts_json",
        "score_bucket_distribution_json",
        "session_bucket_distribution_json",
        "open_gate_event_count",
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


def build_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    clean = [as_float(r["clean_closed_episode_ratio"]) for r in rows]
    same_side = [as_float(r["same_side_add_qty_ratio"]) for r in rows]
    close_p50 = [as_float(r["episode_close_delay_p50"]) for r in rows]
    close_p90 = [as_float(r["episode_close_delay_p90"]) for r in rows]
    residual = [as_float(r["residual_qty"]) for r in rows]
    first_opposite = [as_float(r["median_first_opposite_delay_s"]) for r in rows if as_float(r["median_first_opposite_delay_s"]) > 0.0]
    total_episodes = sum(as_int(r["episode_count"]) for r in rows)
    total_hits = sum(as_int(r["completion_30s_hit_count"]) for r in rows)
    gate_on_episodes = sum(as_int(r["gate_on_episode_count"]) for r in rows)
    gate_on_hits = sum(as_int(r["gate_on_hit_count"]) for r in rows)
    gate_off_episodes = sum(as_int(r["gate_off_episode_count"]) for r in rows)
    gate_off_hits = sum(as_int(r["gate_off_hit_count"]) for r in rows)
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
        "open_candidate_total": sum(as_int(r["open_candidate_count"]) for r in rows),
        "open_allowed_total": sum(as_int(r["open_allowed_count"]) for r in rows),
        "open_blocked_total": sum(as_int(r["open_blocked_count"]) for r in rows),
        "episode_total": total_episodes,
        "30s_completion_hit_rate": rate(total_hits, total_episodes),
        "30s_completion_hit_rate_when_gate_on": rate(gate_on_hits, gate_on_episodes),
        "30s_completion_hit_rate_when_gate_off": rate(gate_off_hits, gate_off_episodes),
        "median_first_opposite_delay_s": median(first_opposite) if first_opposite else 0.0,
        "gate_block_reason_counts": merge_counter_json(rows, "gate_block_reason_counts_json"),
        "score_bucket_distribution": merge_counter_json(rows, "score_bucket_distribution_json"),
        "session_bucket_distribution": merge_counter_json(rows, "session_bucket_distribution_json"),
        "merge_requested_total": sum(as_int(r["merge_requested_count"]) for r in rows),
        "merge_executed_total": sum(as_int(r["merge_executed_count"]) for r in rows),
        "redeem_requested_total": sum(as_int(r["redeem_requested_count"]) for r in rows),
    }
    return {"summary": summary, "rows": rows}


def write_summary(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    write_summary(summary_path, build_summary(rows))
    print(f"✅ completion_first report: {csv_path}")
    print(f"✅ completion_first summary: {summary_path}")


if __name__ == "__main__":
    main()
