#!/usr/bin/env python3
"""
Self-check for the completion-first strategy design.

This script does not backtest the strategy on full book replay.
It validates whether a public trade/activity sample matches the
structural assumptions of the designed state machine:

- round selection exists
- active sessions show high participation
- first-leg fills are quickly followed by opposite-side fills
- same-side repeats before completion are rare
- merge happens quickly after the final trade
- merge is followed by a short cooldown before the next round
- unmatched inventory cost stays small
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROUND_RE = re.compile(r"btc-updown-5m-(\d+)$")


@dataclass
class Thresholds:
    active_session_participation_min_pct: float = 90.0
    pair_within_30s_min_pct: float = 90.0
    opp_is_second_trade_min_pct: float = 90.0
    same_side_repeat_before_opp_max_pct: float = 10.0
    markets_with_merge_min_pct: float = 40.0
    merge_to_next_trade_min_median_sec: float = 5.0
    merge_to_next_trade_max_median_sec: float = 20.0
    unmatched_cost_max_pct: float = 2.0


def load_json(path: Path):
    with path.open() as f:
        return json.load(f)


def median_or_none(values: Iterable[float]):
    values = list(values)
    if not values:
        return None
    return statistics.median(values)


def parse_round_start(slug: str) -> int | None:
    m = ROUND_RE.search(slug)
    if not m:
        return None
    return int(m.group(1))


def build_trade_groups(trades):
    by_condition = defaultdict(list)
    for trade in trades:
        by_condition[trade["conditionId"]].append(trade)
    return by_condition


def build_activity_groups(activities):
    by_condition = defaultdict(list)
    for activity in activities:
        by_condition[activity["conditionId"]].append(activity)
    return by_condition


def classify_breaks(missing_rounds):
    streaks = []
    if not missing_rounds:
        return streaks
    start = prev = missing_rounds[0]
    for ts in missing_rounds[1:]:
        if ts == prev + 300:
            prev = ts
            continue
        streaks.append((start, prev, (prev - start) // 300 + 1))
        start = prev = ts
    streaks.append((start, prev, (prev - start) // 300 + 1))
    return streaks


def summarize(trades, activities):
    by_condition = build_trade_groups(trades)
    by_activity = build_activity_groups(activities)

    round_starts = {}
    for rows in by_condition.values():
        start = parse_round_start(rows[0]["slug"])
        if start is not None:
            round_starts[start] = rows[0]["conditionId"]

    all_rounds = []
    if round_starts:
        mn = min(round_starts)
        mx = max(round_starts)
        all_rounds = list(range(mn, mx + 1, 300))
    seen_rounds = set(round_starts)
    missing_rounds = [ts for ts in all_rounds if ts not in seen_rounds]
    break_streaks = classify_breaks(missing_rounds)

    long_break_rounds = set()
    for start, end, n in break_streaks:
        if n >= 3:
            long_break_rounds.update(range(start, end + 1, 300))
    active_rounds = [ts for ts in all_rounds if ts not in long_break_rounds]
    active_participated = [ts for ts in active_rounds if ts in seen_rounds]

    pairable = 0
    pair_within_30s = 0
    opp_is_second_trade = 0
    same_side_repeat_before_opp = 0
    first_after_open = []
    first_leg_to_opp = []

    total_cost = 0.0
    unmatched_cost = 0.0
    merge_before_end = []
    merge_to_next_trade = []
    overlap_merge_markets = 0
    paired_markets = 0

    for condition_id, rows in by_condition.items():
        rows = sorted(rows, key=lambda x: (x["timestamp"], x["transactionHash"]))
        start = parse_round_start(rows[0]["slug"])
        if start is not None:
            first_after_open.append(rows[0]["timestamp"] - start)

        first = rows[0]
        opp = None
        same_repeat = 0
        if len(rows) >= 2 and rows[1]["outcome"] != first["outcome"]:
            opp_is_second_trade += 1
        else:
            for later in rows[1:]:
                if later["outcome"] == first["outcome"]:
                    same_repeat += 1
                    continue
                opp = later
                break
            if same_repeat > 0 and opp is not None:
                same_side_repeat_before_opp += 1
        if opp is None:
            for later in rows[1:]:
                if later["outcome"] != first["outcome"]:
                    opp = later
                    break
        if opp is not None:
            pairable += 1
            lag = opp["timestamp"] - first["timestamp"]
            first_leg_to_opp.append(lag)
            if lag <= 30:
                pair_within_30s += 1

        up_sh = sum(float(r["size"]) for r in rows if r["outcome"] == "Up")
        dn_sh = sum(float(r["size"]) for r in rows if r["outcome"] == "Down")
        up_cost = sum(float(r["size"]) * float(r["price"]) for r in rows if r["outcome"] == "Up")
        dn_cost = sum(float(r["size"]) * float(r["price"]) for r in rows if r["outcome"] == "Down")
        total_cost += up_cost + dn_cost
        if up_sh and dn_sh:
            paired_markets += 1
            if up_sh > dn_sh:
                unmatched_cost += (up_sh - dn_sh) * (up_cost / up_sh)
            elif dn_sh > up_sh:
                unmatched_cost += (dn_sh - up_sh) * (dn_cost / dn_sh)

        acts = sorted(by_activity.get(condition_id, []), key=lambda x: (x["timestamp"], x["transactionHash"]))
        merge_events = [a for a in acts if a["type"] == "MERGE"]
        if merge_events and up_sh and dn_sh:
            overlap_merge_markets += 1

        if merge_events and start is not None:
            round_end = start + 300
            merge_before_end.append(round_end - merge_events[0]["timestamp"])

            events = [("TRADE", r["timestamp"]) for r in rows]
            events.extend((a["type"], a["timestamp"]) for a in acts if a["type"] in ("MERGE", "REDEEM"))
            events.sort(key=lambda x: x[1])
            for i, (typ, ts) in enumerate(events):
                if typ != "MERGE":
                    continue
                next_trade = next((next_ts for next_typ, next_ts in events[i + 1 :] if next_typ == "TRADE"), None)
                if next_trade is not None:
                    merge_to_next_trade.append(next_trade - ts)
                    break

    return {
        "total_rounds": len(all_rounds),
        "participated_rounds": len(seen_rounds),
        "participation_pct": 100.0 * len(seen_rounds) / len(all_rounds) if all_rounds else None,
        "skip_rounds": len(missing_rounds),
        "long_break_rounds": len(long_break_rounds),
        "active_session_participation_pct": 100.0 * len(active_participated) / len(active_rounds)
        if active_rounds
        else None,
        "paired_markets": paired_markets,
        "markets_with_merge_pct": 100.0 * overlap_merge_markets / paired_markets if paired_markets else None,
        "pair_within_30s_pct": 100.0 * pair_within_30s / pairable if pairable else None,
        "opp_is_second_trade_pct": 100.0 * opp_is_second_trade / len(by_condition) if by_condition else None,
        "same_side_repeat_before_opp_pct": 100.0 * same_side_repeat_before_opp / len(by_condition)
        if by_condition
        else None,
        "first_trade_after_open_median_sec": median_or_none(first_after_open),
        "first_leg_to_opp_median_sec": median_or_none(first_leg_to_opp),
        "first_merge_before_end_median_sec": median_or_none(merge_before_end),
        "merge_to_next_trade_median_sec": median_or_none(merge_to_next_trade),
        "unmatched_cost_pct": 100.0 * unmatched_cost / total_cost if total_cost else None,
        "long_break_streaks_top3": sorted(break_streaks, key=lambda x: x[2], reverse=True)[:3],
    }


def evaluate(summary, thresholds: Thresholds):
    checks = []

    def add(name: str, passed: bool, value, target: str):
        checks.append({"name": name, "passed": passed, "value": value, "target": target})

    add(
        "active_session_participation",
        summary["active_session_participation_pct"] is not None
        and summary["active_session_participation_pct"] >= thresholds.active_session_participation_min_pct,
        summary["active_session_participation_pct"],
        f">= {thresholds.active_session_participation_min_pct:.1f}%",
    )
    add(
        "pair_within_30s",
        summary["pair_within_30s_pct"] is not None
        and summary["pair_within_30s_pct"] >= thresholds.pair_within_30s_min_pct,
        summary["pair_within_30s_pct"],
        f">= {thresholds.pair_within_30s_min_pct:.1f}%",
    )
    add(
        "opp_is_second_trade",
        summary["opp_is_second_trade_pct"] is not None
        and summary["opp_is_second_trade_pct"] >= thresholds.opp_is_second_trade_min_pct,
        summary["opp_is_second_trade_pct"],
        f">= {thresholds.opp_is_second_trade_min_pct:.1f}%",
    )
    add(
        "same_side_repeat_before_opp",
        summary["same_side_repeat_before_opp_pct"] is not None
        and summary["same_side_repeat_before_opp_pct"] <= thresholds.same_side_repeat_before_opp_max_pct,
        summary["same_side_repeat_before_opp_pct"],
        f"<= {thresholds.same_side_repeat_before_opp_max_pct:.1f}%",
    )
    add(
        "markets_with_merge",
        summary["markets_with_merge_pct"] is not None
        and summary["markets_with_merge_pct"] >= thresholds.markets_with_merge_min_pct,
        summary["markets_with_merge_pct"],
        f">= {thresholds.markets_with_merge_min_pct:.1f}%",
    )
    add(
        "merge_to_next_trade_median",
        summary["merge_to_next_trade_median_sec"] is not None
        and thresholds.merge_to_next_trade_min_median_sec
        <= summary["merge_to_next_trade_median_sec"]
        <= thresholds.merge_to_next_trade_max_median_sec,
        summary["merge_to_next_trade_median_sec"],
        f"in [{thresholds.merge_to_next_trade_min_median_sec:.1f}, {thresholds.merge_to_next_trade_max_median_sec:.1f}]s",
    )
    add(
        "unmatched_cost_pct",
        summary["unmatched_cost_pct"] is not None
        and summary["unmatched_cost_pct"] <= thresholds.unmatched_cost_max_pct,
        summary["unmatched_cost_pct"],
        f"<= {thresholds.unmatched_cost_max_pct:.1f}%",
    )
    return checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trades",
        type=Path,
        default=Path("/tmp/xuanxuan008_trades.json"),
        help="Path to trade JSON from Polymarket Data API",
    )
    parser.add_argument(
        "--activity",
        type=Path,
        default=Path("/tmp/xuanxuan008_activity_long.json"),
        help="Path to activity JSON from Polymarket Data API",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON only")
    args = parser.parse_args()

    trades = load_json(args.trades)
    activities = load_json(args.activity)
    summary = summarize(trades, activities)
    checks = evaluate(summary, Thresholds())
    overall = all(item["passed"] for item in checks)

    if args.json:
        print(json.dumps({"summary": summary, "checks": checks, "overall_pass": overall}, indent=2))
        return

    print("Completion-First Strategy Self-Check")
    print("=" * 40)
    for key, value in summary.items():
        print(f"{key}: {value}")
    print("\nChecks")
    for item in checks:
        status = "PASS" if item["passed"] else "WARN"
        print(f"[{status}] {item['name']}: value={item['value']} target={item['target']}")
    print(f"\nOverall: {'PASS' if overall else 'WARN'}")


if __name__ == "__main__":
    main()
