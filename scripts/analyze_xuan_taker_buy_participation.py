#!/usr/bin/env python3
"""Analyze participation gap for the xuan taker-BUY finalist.

This is an explanatory replay analysis, not a parameter search. It keeps the
frozen finalist unchanged, then measures where public taker-BUY opportunities
drop out and how strict market coverage compares with xuan's coverage.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TRUSTED_START_MS = 1777274700000  # 2026-04-27T07:25:00Z


@dataclass(frozen=True)
class Rule:
    price_lo: float = 0.55
    price_hi: float = 0.70
    size_lo: float = 100.0
    size_hi: float = 150.0
    first_lo: float = 0.60
    first_hi: float = 0.75
    max_l1_pair: float = 0.995
    pair_ceiling: float = 0.95
    clip: float = 60.0
    max_l1_age_ms: int = 750
    max_l2_age_ms: int = 750
    completion_window_ms: int = 30_000
    cooldown_ms: int = 10_000


@dataclass
class Market:
    condition_id: str
    slug: str
    start_ms: int
    end_ms: int
    winner_side: str | None
    resolution_source: str | None


def connect_ro(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def pct(num: int | float, den: int | float) -> float | None:
    return None if den == 0 else round(float(num) / float(den), 6)


def sweep_vwap(levels: list[tuple[float, float]], clip: float) -> tuple[float | None, float]:
    filled = 0.0
    notional = 0.0
    for px, size in sorted(levels, key=lambda item: item[0]):
        if px <= 0.0 or size <= 0.0:
            continue
        take = min(size, clip - filled)
        if take <= 0.0:
            continue
        filled += take
        notional += take * px
        if filled + 1e-9 >= clip:
            return notional / filled, filled
    return None, filled


def l2_asks(row: sqlite3.Row) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for idx in range(1, 6):
        px = parse_float(row[f"ask{idx}_px"])
        size = parse_float(row[f"ask{idx}_sz"])
        if px is not None and size is not None:
            levels.append((px, size))
    return levels


TimeIndex = tuple[list[int], list[sqlite3.Row]]


def make_time_index(rows: list[sqlite3.Row]) -> TimeIndex:
    return [int(row["recv_ms"]) for row in rows], rows


def latest_before(index: TimeIndex, ts_ms: int) -> sqlite3.Row | None:
    ts_values, rows = index
    if not rows:
        return None
    idx = bisect.bisect_right(ts_values, ts_ms) - 1
    return rows[idx] if idx >= 0 else None


def side_mid(l1: sqlite3.Row, side: str) -> float | None:
    bid = parse_float(l1["yes_bid_px"] if side == "YES" else l1["no_bid_px"])
    ask = parse_float(l1["yes_ask_px"] if side == "YES" else l1["no_ask_px"])
    if bid is None or ask is None or bid <= 0.0 or ask <= 0.0:
        return None
    return (bid + ask) / 2.0


def high_side(l1: sqlite3.Row) -> str | None:
    yes = side_mid(l1, "YES")
    no = side_mid(l1, "NO")
    if yes is None or no is None:
        return None
    return "YES" if yes >= no else "NO"


def opp_l1_ask(l1: sqlite3.Row, side: str) -> float | None:
    key = "no_ask_px" if side == "YES" else "yes_ask_px"
    return parse_float(l1[key])


def load_markets(conn: sqlite3.Connection, symbol: str, trusted_start_ms: int) -> dict[str, Market]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        select m.condition_id, m.slug, m.start_ms, m.end_ms, s.winner_side, s.resolution_source
        from market_meta m
        left join settlement_records s on s.condition_id = m.condition_id
        where m.symbol = ?
          and m.interval_sec = 300
          and m.end_ms > ?
          and s.winner_side is not null
          and coalesce(s.resolution_source, '') != 'inferred'
        order by m.start_ms
        """,
        (symbol, trusted_start_ms),
    ).fetchall()
    return {
        str(row["condition_id"]): Market(
            condition_id=str(row["condition_id"]),
            slug=str(row["slug"]),
            start_ms=int(row["start_ms"]),
            end_ms=int(row["end_ms"]),
            winner_side=str(row["winner_side"]) if row["winner_side"] is not None else None,
            resolution_source=str(row["resolution_source"]) if row["resolution_source"] is not None else None,
        )
        for row in rows
    }


def load_rows_by_condition(conn: sqlite3.Connection, table: str, condition_ids: list[str], extra_where: str = "") -> dict[str, list[sqlite3.Row]]:
    out: dict[str, list[sqlite3.Row]] = defaultdict(list)
    if not condition_ids:
        return out
    placeholders = ",".join("?" for _ in condition_ids)
    query = f"select * from {table} where condition_id in ({placeholders}) {extra_where} order by condition_id, recv_ms"
    for row in conn.execute(query, condition_ids):
        out[str(row["condition_id"])].append(row)
    return out


def classify_price(price: float, rule: Rule) -> str:
    if rule.price_lo <= price < rule.price_hi:
        return "strict"
    if 0.50 <= price < rule.price_lo:
        return "near_low_0.50_0.55"
    if rule.price_hi <= price <= 0.75:
        return "near_high_0.70_0.75"
    if 0.45 <= price < 0.50:
        return "wide_low_0.45_0.50"
    if 0.75 < price <= 0.80:
        return "wide_high_0.75_0.80"
    return "far"


def classify_size(size: float, rule: Rule) -> str:
    if rule.size_lo <= size < rule.size_hi:
        return "strict"
    if 60.0 <= size < rule.size_lo:
        return "near_small_60_100"
    if rule.size_hi <= size <= 220.0:
        return "near_large_150_220"
    if 20.0 <= size < 60.0:
        return "wide_small_20_60"
    if 220.0 < size <= 400.0:
        return "wide_large_220_400"
    return "far"


def evaluate_trade(
    trade: sqlite3.Row,
    market: Market,
    l1_index: TimeIndex,
    l2_indices_by_side: dict[str, TimeIndex],
    rule: Rule,
) -> dict[str, Any]:
    ts_ms = int(trade["trade_ts_ms"] or trade["recv_ms"])
    side = str(trade["market_side"] or "").upper()
    price = float(trade["price"])
    size = float(trade["size"])
    result: dict[str, Any] = {
        "ts_ms": ts_ms,
        "side": side,
        "price": price,
        "size": size,
        "price_bucket": classify_price(price, rule),
        "size_bucket": classify_size(size, rule),
        "strict_price": rule.price_lo <= price < rule.price_hi,
        "strict_size": rule.size_lo <= size < rule.size_hi,
        "winner_side": market.winner_side,
        "first_is_winner": side == market.winner_side,
    }
    if side not in {"YES", "NO"}:
        result["gate"] = "bad_side"
        return result
    l1 = latest_before(l1_index, ts_ms)
    if l1 is None:
        result["gate"] = "missing_l1"
        return result
    l1_age = ts_ms - int(l1["recv_ms"])
    result["l1_age_ms"] = l1_age
    if l1_age < -1000 or l1_age > rule.max_l1_age_ms:
        result["gate"] = "stale_l1"
        return result
    high = high_side(l1)
    result["high_side"] = high
    result["is_high_side"] = side == high
    if side != high:
        result["gate"] = "not_high_side"
        return result
    l2 = latest_before(l2_indices_by_side.get(side, ([], [])), ts_ms)
    if l2 is None:
        result["gate"] = "missing_first_l2"
        return result
    l2_age = ts_ms - int(l2["recv_ms"])
    result["first_l2_age_ms"] = l2_age
    if l2_age < -1000 or l2_age > rule.max_l2_age_ms:
        result["gate"] = "stale_first_l2"
        return result
    first_vwap, first_fill = sweep_vwap(l2_asks(l2), rule.clip)
    result["first_l2_vwap"] = first_vwap
    result["first_l2_fill"] = first_fill
    if first_vwap is None:
        result["gate"] = "insufficient_first_l2"
        return result
    result["strict_first_vwap"] = rule.first_lo <= first_vwap < rule.first_hi
    if not result["strict_first_vwap"]:
        result["gate"] = "first_l2_vwap_out_of_range"
        return result
    opp_ask = opp_l1_ask(l1, side)
    result["opp_l1_ask"] = opp_ask
    if opp_ask is None or opp_ask <= 0.0:
        result["gate"] = "missing_opp_l1_ask"
        return result
    l1_pair = first_vwap + opp_ask
    result["l1_immediate_pair"] = l1_pair
    result["strict_l1_pair"] = l1_pair <= rule.max_l1_pair + 1e-9
    if not result["strict_l1_pair"]:
        result["gate"] = "l1_immediate_pair_too_high"
        return result
    opp = "NO" if side == "YES" else "YES"
    min_pair: float | None = None
    min_delay: float | None = None
    for row in l2_indices_by_side.get(opp, ([], []))[1]:
        recv_ms = int(row["recv_ms"])
        if recv_ms < ts_ms:
            continue
        if recv_ms > ts_ms + rule.completion_window_ms:
            break
        comp_vwap, filled = sweep_vwap(l2_asks(row), rule.clip)
        if comp_vwap is None:
            continue
        pair_cost = first_vwap + comp_vwap
        if min_pair is None or pair_cost < min_pair:
            min_pair = pair_cost
            min_delay = (recv_ms - ts_ms) / 1000.0
    result["min_pair_cost_30s"] = min_pair
    result["min_pair_delay_s"] = min_delay
    result["strict_completion"] = min_pair is not None and min_pair <= rule.pair_ceiling + 1e-9
    result["gate"] = "pass_static_gates" if result["strict_completion"] else "completion_pair_ceiling_fail"
    return result


def analyze_day(db_path: Path, day: str, symbol: str, rule: Rule, trusted_start_ms: int) -> dict[str, Any]:
    conn = connect_ro(db_path)
    conn.row_factory = sqlite3.Row
    markets = load_markets(conn, symbol, trusted_start_ms)
    condition_ids = list(markets)
    trades_by_condition = load_rows_by_condition(
        conn,
        "md_trades",
        condition_ids,
        "and upper(coalesce(taker_side, '')) = 'BUY' and trade_ts_ms is not null",
    )
    l1_by_condition = load_rows_by_condition(conn, "md_book_l1", condition_ids)
    l1_index_by_condition = {condition_id: make_time_index(rows) for condition_id, rows in l1_by_condition.items()}
    l2_by_condition_side: dict[str, dict[str, list[sqlite3.Row]]] = defaultdict(lambda: defaultdict(list))
    if condition_ids:
        placeholders = ",".join("?" for _ in condition_ids)
        for row in conn.execute(
            f"select * from md_book_l2 where condition_id in ({placeholders}) order by condition_id, market_side, recv_ms",
            condition_ids,
        ):
            l2_by_condition_side[str(row["condition_id"])][str(row["market_side"]).upper()].append(row)
    l2_index_by_condition_side: dict[str, dict[str, TimeIndex]] = {
        condition_id: {side: make_time_index(rows) for side, rows in by_side.items()}
        for condition_id, by_side in l2_by_condition_side.items()
    }

    xuan_markets = {
        str(row["condition_id"])
        for row in conn.execute(
            f"select distinct condition_id from xuan_trades where condition_id in ({','.join('?' for _ in condition_ids)})",
            condition_ids,
        )
    } if condition_ids else set()

    funnel = Counter()
    price_buckets = Counter()
    size_buckets = Counter()
    finalist_markets: set[str] = set()
    envelope_markets: set[str] = set()
    first_winner = Counter()
    near_miss_distances: list[dict[str, Any]] = []
    broad_pass_static = 0
    strict_rows = 0
    for condition_id, market in markets.items():
        market_blocked_after_residual = False
        blocked_until = 0
        for trade in trades_by_condition.get(condition_id, []):
            ts_ms = int(trade["trade_ts_ms"] or trade["recv_ms"])
            if ts_ms < max(market.start_ms, trusted_start_ms) or ts_ms >= market.end_ms:
                continue
            price = float(trade["price"])
            size = float(trade["size"])
            funnel["public_buy"] += 1
            price_buckets[classify_price(price, rule)] += 1
            size_buckets[classify_size(size, rule)] += 1
            strict_price = rule.price_lo <= price < rule.price_hi
            strict_size = rule.size_lo <= size < rule.size_hi
            if strict_price:
                funnel["strict_price"] += 1
            if strict_size:
                funnel["strict_size"] += 1
            if strict_price and strict_size:
                funnel["strict_price_size"] += 1
            is_broad = 0.45 <= price <= 0.80 and 20.0 <= size <= 400.0
            if not (strict_price and strict_size):
                if not is_broad:
                    continue
            evaluated = evaluate_trade(
                trade,
                market,
                l1_index_by_condition.get(condition_id, ([], [])),
                l2_index_by_condition_side.get(condition_id, {}),
                rule,
            )
            gate = str(evaluated["gate"])
            if is_broad:
                funnel[f"broad_{gate}"] += 1
            if strict_price and strict_size:
                funnel[f"strict_{gate}"] += 1
                if evaluated.get("is_high_side"):
                    funnel["strict_high_side"] += 1
                if evaluated.get("strict_first_vwap"):
                    funnel["strict_first_vwap"] += 1
                if evaluated.get("strict_l1_pair"):
                    funnel["strict_l1_pair"] += 1
                if evaluated.get("strict_completion"):
                    if market_blocked_after_residual or ts_ms < blocked_until:
                        funnel["strict_cooldown_or_blocked"] += 1
                    else:
                        funnel["strict_finalist"] += 1
                        strict_rows += 1
                        finalist_markets.add(condition_id)
                        first_winner["winner" if evaluated.get("first_is_winner") else "loser"] += 1
                        # Replay execution rule blocks future entries after residual.
                        if evaluated.get("min_pair_cost_30s") is None or evaluated.get("min_pair_cost_30s", 99.0) > rule.pair_ceiling:
                            market_blocked_after_residual = True
                        else:
                            blocked_until = ts_ms + rule.cooldown_ms
            if is_broad and evaluated.get("strict_completion"):
                broad_pass_static += 1
                envelope_markets.add(condition_id)
                if not (strict_price and strict_size):
                    near_miss_distances.append(
                        {
                            "slug": market.slug,
                            "price": price,
                            "size": size,
                            "price_bucket": evaluated["price_bucket"],
                            "size_bucket": evaluated["size_bucket"],
                            "first_l2_vwap": evaluated.get("first_l2_vwap"),
                            "l1_immediate_pair": evaluated.get("l1_immediate_pair"),
                            "min_pair_cost_30s": evaluated.get("min_pair_cost_30s"),
                            "first_is_winner": evaluated.get("first_is_winner"),
                        }
                    )

    return {
        "day": day,
        "market_count": len(markets),
        "xuan_market_count": len(xuan_markets),
        "xuan_market_coverage": pct(len(xuan_markets), len(markets)),
        "strict_finalist_rows": strict_rows,
        "strict_finalist_market_count": len(finalist_markets),
        "strict_finalist_market_coverage": pct(len(finalist_markets), len(markets)),
        "broad_static_pass_rows": broad_pass_static,
        "broad_static_market_count": len(envelope_markets),
        "broad_static_market_coverage": pct(len(envelope_markets), len(markets)),
        "coverage_gap_xuan_vs_strict": None
        if not markets
        else round((len(xuan_markets) - len(finalist_markets)) / len(markets), 6),
        "funnel": dict(funnel),
        "price_buckets": dict(price_buckets),
        "size_buckets": dict(size_buckets),
        "first_winner_counts": dict(first_winner),
        "near_miss_examples": near_miss_distances[:50],
        "near_miss_count": len(near_miss_distances),
    }


def aggregate(days: list[dict[str, Any]]) -> dict[str, Any]:
    total_markets = sum(int(day["market_count"]) for day in days)
    total_xuan_markets = sum(int(day["xuan_market_count"]) for day in days)
    total_strict_markets = sum(int(day["strict_finalist_market_count"]) for day in days)
    total_broad_markets = sum(int(day["broad_static_market_count"]) for day in days)
    funnel = Counter()
    price_buckets = Counter()
    size_buckets = Counter()
    first_winner = Counter()
    for day in days:
        funnel.update(day["funnel"])
        price_buckets.update(day["price_buckets"])
        size_buckets.update(day["size_buckets"])
        first_winner.update(day["first_winner_counts"])
    return {
        "market_count": total_markets,
        "xuan_market_count": total_xuan_markets,
        "xuan_market_coverage": pct(total_xuan_markets, total_markets),
        "strict_finalist_rows": sum(int(day["strict_finalist_rows"]) for day in days),
        "strict_finalist_market_count": total_strict_markets,
        "strict_finalist_market_coverage": pct(total_strict_markets, total_markets),
        "broad_static_pass_rows": sum(int(day["broad_static_pass_rows"]) for day in days),
        "broad_static_market_count": total_broad_markets,
        "broad_static_market_coverage": pct(total_broad_markets, total_markets),
        "coverage_gap_xuan_vs_strict": pct(total_xuan_markets - total_strict_markets, total_markets),
        "funnel": dict(funnel),
        "price_buckets": dict(price_buckets),
        "size_buckets": dict(size_buckets),
        "first_winner_counts": dict(first_winner),
        "strict_first_winner_rate": pct(first_winner["winner"], first_winner["winner"] + first_winner["loser"]),
    }


def write_csv(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for day in report["days"]:
        rows.append(
            {
                "day": day["day"],
                "market_count": day["market_count"],
                "xuan_market_coverage": day["xuan_market_coverage"],
                "strict_finalist_rows": day["strict_finalist_rows"],
                "strict_finalist_market_coverage": day["strict_finalist_market_coverage"],
                "broad_static_pass_rows": day["broad_static_pass_rows"],
                "broad_static_market_coverage": day["broad_static_market_coverage"],
                "coverage_gap_xuan_vs_strict": day["coverage_gap_xuan_vs_strict"],
                "near_miss_count": day["near_miss_count"],
            }
        )
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["day"])
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-root", type=Path, default=Path("/mnt/poly-replay"))
    parser.add_argument("--days", required=True, help="Comma-separated YYYY-MM-DD days.")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-csv", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rule = Rule()
    day_reports = []
    for day in [item.strip() for item in args.days.split(",") if item.strip()]:
        db_path = args.replay_root / day / "crypto_5m.sqlite"
        if not db_path.exists():
            raise SystemExit(f"missing replay db: {db_path}")
        trusted_start = TRUSTED_START_MS if day == "2026-04-27" else 0
        day_reports.append(analyze_day(db_path, day, args.symbol, rule, trusted_start))
    report = {
        "rule": rule.__dict__,
        "symbol": args.symbol,
        "replay_root": str(args.replay_root),
        "aggregate": aggregate(day_reports),
        "days": day_reports,
        "interpretation": {
            "strict_finalist": "frozen execution-quality core",
            "broad_static_pass": "wider price/size envelope that still passes high-side, first VWAP, l1 pair, and completion gates before cooldown/blocking",
            "do_not_treat_as_parameter_search": True,
        },
    }
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text, encoding="utf-8")
    if args.output_csv:
        write_csv(args.output_csv, report)
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
