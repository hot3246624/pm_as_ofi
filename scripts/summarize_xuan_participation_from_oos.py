#!/usr/bin/env python3
"""Summarize participation gap from frozen OOS artifacts plus replay counts."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def connect_ro(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def rate(num: int | float, den: int | float) -> float | None:
    return None if den == 0 else round(float(num) / float(den), 6)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def official_btc_markets(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        select m.condition_id
        from market_meta m
        join settlement_records s using(condition_id)
        where m.symbol = 'BTC'
          and m.interval_sec = 300
          and s.winner_side is not null
          and coalesce(s.resolution_source, '') != 'inferred'
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def scalar(conn: sqlite3.Connection, query: str, params: list[Any]) -> int:
    return int(conn.execute(query, params).fetchone()[0] or 0)


def replay_counts(replay_root: Path, days: list[str]) -> tuple[dict[str, Any], dict[str, dict[str, int]]]:
    aggregate: Counter[str] = Counter()
    by_day: dict[str, dict[str, int]] = {}
    for day in days:
        conn = connect_ro(replay_root / day / "crypto_5m.sqlite")
        markets = official_btc_markets(conn)
        if not markets:
            by_day[day] = {
                "market_count": 0,
                "xuan_market_count": 0,
                "public_buy": 0,
                "strict_price": 0,
                "strict_size": 0,
                "strict_price_size": 0,
            }
            continue
        placeholders = ",".join("?" for _ in markets)
        params = markets
        counts = {
            "market_count": len(markets),
            "xuan_market_count": scalar(
                conn,
                f"select count(distinct condition_id) from xuan_trades where condition_id in ({placeholders})",
                params,
            ),
            "public_buy": scalar(
                conn,
                f"""
                select count(*) from md_trades
                where condition_id in ({placeholders})
                  and upper(coalesce(taker_side, '')) = 'BUY'
                """,
                params,
            ),
            "strict_price": scalar(
                conn,
                f"""
                select count(*) from md_trades
                where condition_id in ({placeholders})
                  and upper(coalesce(taker_side, '')) = 'BUY'
                  and price >= 0.55 and price < 0.70
                """,
                params,
            ),
            "strict_size": scalar(
                conn,
                f"""
                select count(*) from md_trades
                where condition_id in ({placeholders})
                  and upper(coalesce(taker_side, '')) = 'BUY'
                  and size >= 100 and size < 150
                """,
                params,
            ),
            "strict_price_size": scalar(
                conn,
                f"""
                select count(*) from md_trades
                where condition_id in ({placeholders})
                  and upper(coalesce(taker_side, '')) = 'BUY'
                  and price >= 0.55 and price < 0.70
                  and size >= 100 and size < 150
                """,
                params,
            ),
        }
        by_day[day] = counts
        aggregate.update(counts)
    return dict(aggregate), by_day


def deep_gate_counts(cache_rows: list[dict[str, str]]) -> Counter[str]:
    gates: Counter[str] = Counter()
    for row in cache_rows:
        try:
            first = float(row["first_l2_vwap"])
            l1_pair = float(row["l1_immediate_pair"])
            min_pair = float(row["min_pair_cost_30s"])
        except (KeyError, TypeError, ValueError):
            gates["bad_numeric"] += 1
            continue
        if row.get("side_alignment") != "high":
            gates["not_high_side"] += 1
        elif not (0.60 <= first < 0.75):
            gates["first_l2_vwap_out_of_range"] += 1
        elif not (l1_pair <= 0.995):
            gates["l1_immediate_pair_too_high"] += 1
        elif not (min_pair <= 0.95):
            gates["completion_pair_ceiling_fail"] += 1
        else:
            gates["pass_static_before_cooldown_block"] += 1
    return gates


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--days", required=True)
    parser.add_argument("--candidate-cache-csv", type=Path, required=True)
    parser.add_argument("--policy-rows-csv", type=Path, required=True)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()

    days = [day.strip() for day in args.days.split(",") if day.strip()]
    aggregate, by_day = replay_counts(args.replay_root, days)
    cache_rows = read_csv(args.candidate_cache_csv)
    policy_rows = read_csv(args.policy_rows_csv)
    cache_by_day = Counter(row["day"] for row in cache_rows)
    policy_by_day = Counter(row["day"] for row in policy_rows)
    cache_markets_by_day: dict[str, set[str]] = defaultdict(set)
    policy_markets_by_day: dict[str, set[str]] = defaultdict(set)
    for row in cache_rows:
        cache_markets_by_day[row["day"]].add(row["slug"])
    for row in policy_rows:
        policy_markets_by_day[row["day"]].add(row["slug"])

    policy_markets = {row["slug"] for row in policy_rows}
    cache_markets = {row["slug"] for row in cache_rows}
    market_count = int(aggregate.get("market_count", 0))
    public_buy = int(aggregate.get("public_buy", 0))
    report = {
        "days": days,
        "aggregate": aggregate,
        "by_day": by_day,
        "candidate_cache_rows": len(cache_rows),
        "policy_rows": len(policy_rows),
        "candidate_cache_by_day": dict(cache_by_day),
        "policy_by_day": dict(policy_by_day),
        "candidate_cache_market_count_by_day": {day: len(markets) for day, markets in cache_markets_by_day.items()},
        "policy_market_count_by_day": {day: len(markets) for day, markets in policy_markets_by_day.items()},
        "deep_gate_from_candidate_cache": dict(deep_gate_counts(cache_rows)),
        "aggregate_rates": {
            "xuan_market_coverage": rate(int(aggregate.get("xuan_market_count", 0)), market_count),
            "strict_price_size_per_public_buy": rate(int(aggregate.get("strict_price_size", 0)), public_buy),
            "candidate_cache_per_public_buy": rate(len(cache_rows), public_buy),
            "policy_rows_per_public_buy": rate(len(policy_rows), public_buy),
            "candidate_market_coverage": rate(len(cache_markets), market_count),
            "policy_market_coverage": rate(len(policy_markets), market_count),
            "policy_vs_xuan_market_coverage": rate(len(policy_markets), int(aggregate.get("xuan_market_count", 0))),
        },
        "interpretation": {
            "candidate_cache": "public BUY events already inside frozen price/size window",
            "policy_rows": "final frozen rows after high-side, L2 VWAP, pair ceiling, cooldown/block",
        },
    }
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
