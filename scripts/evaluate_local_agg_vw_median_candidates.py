#!/usr/bin/env python3
"""Evaluate VW-median style local price candidates against RTDS closes.

This is intentionally a *lite* evaluator: it uses the currently recorded
boundary tape (trade/ticker/mid prices), not true L2 orderbook depth. Its job is
to keep the Chainlink-like median hypothesis measurable while we build a proper
L2/depth tape.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable


SOURCES = ("binance", "coinbase", "okx", "bybit", "hyperliquid")

CANDIDATES = {
    "equal_core_cex_median": {
        "sources": ("binance", "coinbase", "okx", "bybit"),
        "weights": {"binance": 1.0, "coinbase": 1.0, "okx": 1.0, "bybit": 1.0},
        "aggregator": "weighted_median",
        "pick_rule": "after_then_before",
    },
    "depth_proxy_core_cex_median": {
        "sources": ("binance", "coinbase", "okx", "bybit"),
        "weights": {"binance": 2.0, "coinbase": 2.0, "okx": 1.5, "bybit": 1.5},
        "aggregator": "weighted_median",
        "pick_rule": "after_then_before",
    },
    "depth_proxy_all_sources_median": {
        "sources": SOURCES,
        "weights": {"binance": 2.0, "coinbase": 2.0, "okx": 1.5, "bybit": 1.5, "hyperliquid": 0.5},
        "aggregator": "weighted_median",
        "pick_rule": "after_then_before",
    },
    "depth_proxy_all_sources_mean": {
        "sources": SOURCES,
        "weights": {"binance": 2.0, "coinbase": 2.0, "okx": 1.5, "bybit": 1.5, "hyperliquid": 0.5},
        "aggregator": "weighted_mean",
        "pick_rule": "after_then_before",
    },
    "nearest_core_cex_median": {
        "sources": ("binance", "coinbase", "okx", "bybit"),
        "weights": {"binance": 1.0, "coinbase": 1.0, "okx": 1.0, "bybit": 1.0},
        "aggregator": "weighted_median",
        "pick_rule": "nearest_abs",
    },
}


def fnum(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * pct / 100.0) - 1))
    return ordered[idx]


def pick_price(points: Iterable[tuple[int, float]], rule: str, pre_ms: int, post_ms: int) -> tuple[int, float] | None:
    rows = sorted(points, key=lambda item: item[0])
    if rule == "last_before":
        candidates = [(off, px) for off, px in rows if -pre_ms <= off <= 0]
        return max(candidates, key=lambda item: item[0]) if candidates else None
    if rule == "nearest_abs":
        candidates = [(off, px) for off, px in rows if -pre_ms <= off <= post_ms]
        return min(candidates, key=lambda item: (abs(item[0]), item[0] < 0)) if candidates else None
    if rule == "after_then_before":
        after = [(off, px) for off, px in rows if 0 <= off <= post_ms]
        if after:
            return min(after, key=lambda item: item[0])
        before = [(off, px) for off, px in rows if -pre_ms <= off < 0]
        return max(before, key=lambda item: item[0]) if before else None
    raise ValueError(f"unknown pick_rule: {rule}")


def weighted_median(points: list[tuple[float, float]]) -> float | None:
    total = sum(weight for _, weight in points if weight > 0)
    if total <= 0:
        return None
    acc = 0.0
    for price, weight in sorted(points, key=lambda item: item[0]):
        if weight <= 0:
            continue
        acc += weight
        if acc >= total / 2.0:
            return price
    return points[-1][0] if points else None


def weighted_mean(points: list[tuple[float, float]]) -> float | None:
    total = sum(weight for _, weight in points if weight > 0)
    if total <= 0:
        return None
    return sum(price * weight for price, weight in points if weight > 0) / total


def aggregate(points: list[tuple[float, float]], name: str) -> float | None:
    if name == "weighted_median":
        return weighted_median(points)
    if name == "weighted_mean":
        return weighted_mean(points)
    raise ValueError(f"unknown aggregator: {name}")


def load_samples(path: Path) -> list[dict]:
    rows = list(csv.DictReader(path.open()))
    grouped: dict[tuple[str, int, str], list[dict]] = defaultdict(list)
    for row in rows:
        if row.get("phase") != "close":
            continue
        grouped[(row["symbol"], int(row["round_end_ts"]), row["instance_id"])].append(row)

    samples = []
    for (symbol, round_end_ts, instance_id), rs in grouped.items():
        first = rs[0]
        rtds_open = fnum(first.get("rtds_open"))
        rtds_close = fnum(first.get("rtds_close"))
        if rtds_open is None or rtds_close is None:
            continue
        close_points = []
        for row in rs:
            source = row.get("source", "").lower()
            if source not in SOURCES:
                continue
            offset = int(row["offset_ms"])
            price = fnum(row.get("price"))
            if price is None:
                continue
            close_points.append((source, offset, price))
        samples.append(
            {
                "symbol": symbol,
                "round_end_ts": round_end_ts,
                "instance_id": instance_id,
                "rtds_open": rtds_open,
                "rtds_close": rtds_close,
                "truth_yes": rtds_close >= rtds_open,
                "close_points": close_points,
            }
        )
    return samples


def filter_outliers(
    picked: list[tuple[str, int, float, float]],
    threshold_bps: float,
) -> tuple[list[tuple[str, int, float, float]], int]:
    if threshold_bps <= 0 or len(picked) < 3:
        return picked, 0
    median_price = statistics.median(price for _, _, price, _ in picked)
    if abs(median_price) <= 1e-12:
        return picked, 0
    kept = [
        row
        for row in picked
        if abs(row[2] - median_price) / abs(median_price) * 10_000.0 <= threshold_bps
    ]
    if not kept:
        return picked, 0
    return kept, len(picked) - len(kept)


def evaluate_candidate(
    sample: dict,
    config: dict,
    pre_ms: int,
    post_ms: int,
    decay_ms: float,
    outlier_threshold_bps: float,
) -> dict:
    picked = []
    for source in config["sources"]:
        hit = pick_price(
            ((off, price) for src, off, price in sample["close_points"] if src == source),
            config["pick_rule"],
            pre_ms,
            post_ms,
        )
        if hit is None:
            continue
        off, price = hit
        base = config["weights"].get(source, 1.0)
        weight = base * math.exp(-abs(off) / max(decay_ms, 1.0))
        picked.append((source, off, price, weight))
    if not picked:
        return {"status": "missing"}
    picked, outlier_count = filter_outliers(picked, outlier_threshold_bps)
    pred_close = aggregate([(price, weight) for _, _, price, weight in picked], config["aggregator"])
    if pred_close is None:
        return {"status": "missing"}
    rtds_open = sample["rtds_open"]
    rtds_close = sample["rtds_close"]
    pred_yes = pred_close >= rtds_open
    close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
    direction_margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    prices = [price for _, _, price, _ in picked]
    spread_bps = (max(prices) - min(prices)) / max(abs(pred_close), 1e-12) * 10_000.0 if len(prices) >= 2 else 0.0
    return {
        "status": "ok",
        "pred_close": pred_close,
        "pred_yes": pred_yes,
        "truth_yes": sample["truth_yes"],
        "side_error": pred_yes != sample["truth_yes"],
        "close_diff_bps": close_diff_bps,
        "direction_margin_bps": direction_margin_bps,
        "source_count": len(picked),
        "outlier_count": outlier_count,
        "sources": ";".join(source for source, _, _, _ in picked),
        "median_abs_delta_ms": sorted(abs(off) for _, off, _, _ in picked)[len(picked) // 2],
        "source_spread_bps": spread_bps,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate VW-median-lite candidates on local agg boundary tape.")
    parser.add_argument("--boundary-csv", required=True)
    parser.add_argument("--out-json", default="")
    parser.add_argument("--pre-ms", type=int, default=5_000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument("--decay-ms", type=float, default=900.0)
    parser.add_argument("--min-margin-bps", type=float, default=0.0)
    parser.add_argument(
        "--outlier-threshold-bps",
        type=float,
        default=50.0,
        help="Drop source points farther than this bps from the cross-source median; 50bps=0.5%%.",
    )
    args = parser.parse_args()

    samples = load_samples(Path(args.boundary_csv))
    summaries = []
    for name, config in CANDIDATES.items():
        rows = []
        for sample in samples:
            row = evaluate_candidate(
                sample,
                config,
                args.pre_ms,
                args.post_ms,
                args.decay_ms,
                args.outlier_threshold_bps,
            )
            if row["status"] == "ok" and row["direction_margin_bps"] < args.min_margin_bps:
                row = {**row, "status": "filtered", "filter_reason": "below_min_margin"}
            rows.append(row)
        ok = [row for row in rows if row["status"] == "ok"]
        vals = [row["close_diff_bps"] for row in ok]
        summary = {
            "candidate": name,
            "rows": len(rows),
            "ok": len(ok),
            "filtered": sum(1 for row in rows if row["status"] == "filtered"),
            "missing": sum(1 for row in rows if row["status"] == "missing"),
            "coverage": round(len(ok) / len(rows), 6) if rows else 0.0,
            "side_errors": sum(1 for row in ok if row["side_error"]),
            "outlier_filtered_points": sum(row.get("outlier_count", 0) for row in ok),
            "mean_bps": round(statistics.mean(vals), 6) if vals else None,
            "p95_bps": round(percentile(vals, 95), 6) if vals else None,
            "p99_bps": round(percentile(vals, 99), 6) if vals else None,
            "max_bps": round(max(vals), 6) if vals else None,
            "config": config,
        }
        summaries.append(summary)

    summaries.sort(key=lambda row: (row["side_errors"], row["max_bps"] or 9999.0, -(row["coverage"] or 0.0)))
    text = json.dumps({"samples": len(samples), "summaries": summaries}, sort_keys=True, indent=2)
    if args.out_json:
        Path(args.out_json).write_text(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
