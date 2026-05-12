#!/usr/bin/env python3
"""Offline research harness for local-agg price projection models.

This script intentionally evaluates price models before any uncertainty gate.
The objective is to see whether the aggregation model itself can move realized
close error toward 1bps without merely filtering away boundary opportunities.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


SOURCES = ("binance", "bybit", "okx", "coinbase", "hyperliquid")
BASE_WEIGHTS = {
    "binance": 1.0,
    "bybit": 1.0,
    "okx": 1.0,
    "coinbase": 1.0,
    "hyperliquid": 0.5,
}

CURRENT_SOURCE_SETS = {
    "bnb/usd": ("binance", "bybit", "coinbase", "hyperliquid"),
    "btc/usd": ("coinbase",),
    "doge/usd": ("bybit", "okx", "coinbase", "hyperliquid"),
    "eth/usd": ("coinbase",),
    "hype/usd": ("bybit", "okx", "coinbase", "hyperliquid"),
    "sol/usd": ("okx", "coinbase"),
    "xrp/usd": ("binance", "coinbase"),
}


@dataclass
class Sample:
    symbol: str
    round_end_ts: int
    instance_id: str
    rtds_open: float
    rtds_close: float
    local_ready_ms: int | None
    points: dict[str, list[tuple[int, int, float]]]


def fnum(raw: str | None) -> float | None:
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except Exception:
        return None
    return value if math.isfinite(value) else None


def inum(raw: str | None) -> int | None:
    if raw in (None, ""):
        return None
    try:
        return int(float(raw))
    except Exception:
        return None


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - rank) + ordered[hi] * (rank - lo)


def bps_diff(pred: float, truth: float) -> float:
    return abs(pred - truth) / max(abs(truth), 1e-12) * 10_000.0


def signed_bps(pred: float, truth: float) -> float:
    return (pred - truth) / max(abs(truth), 1e-12) * 10_000.0


def load_samples(path: Path, respect_ready_ms: bool = True) -> list[Sample]:
    grouped: dict[tuple[str, int], dict[str, object]] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            if row.get("phase") != "close":
                continue
            symbol = str(row.get("symbol", "")).lower()
            if symbol not in CURRENT_SOURCE_SETS:
                continue
            round_end_ts = inum(row.get("round_end_ts"))
            rtds_open = fnum(row.get("rtds_open"))
            rtds_close = fnum(row.get("rtds_close"))
            ts_ms = inum(row.get("ts_ms"))
            offset_ms = inum(row.get("offset_ms"))
            price = fnum(row.get("price"))
            source = str(row.get("source", "")).lower()
            if (
                round_end_ts is None
                or rtds_open is None
                or rtds_close is None
                or ts_ms is None
                or offset_ms is None
                or price is None
                or source not in SOURCES
            ):
                continue
            local_ready_ms = inum(row.get("local_ready_ms"))
            if respect_ready_ms and local_ready_ms is not None and ts_ms > local_ready_ms:
                continue
            instance_id = str(row.get("instance_id", ""))
            key = (symbol, round_end_ts)
            cur = grouped.get(key)
            if cur is None or instance_id >= str(cur["instance_id"]):
                if cur is None or instance_id > str(cur["instance_id"]):
                    grouped[key] = {
                        "symbol": symbol,
                        "round_end_ts": round_end_ts,
                        "instance_id": instance_id,
                        "rtds_open": rtds_open,
                        "rtds_close": rtds_close,
                        "local_ready_ms": local_ready_ms,
                        "points": defaultdict(list),
                    }
                cur = grouped[key]
            if instance_id == str(cur["instance_id"]):
                cur["points"][source].append((offset_ms, ts_ms, price))  # type: ignore[index]
    samples = []
    for cur in grouped.values():
        points = {
            source: sorted(rows, key=lambda item: item[0])
            for source, rows in dict(cur["points"]).items()  # type: ignore[arg-type]
            if rows
        }
        if points:
            samples.append(
                Sample(
                    symbol=str(cur["symbol"]),
                    round_end_ts=int(cur["round_end_ts"]),
                    instance_id=str(cur["instance_id"]),
                    rtds_open=float(cur["rtds_open"]),
                    rtds_close=float(cur["rtds_close"]),
                    local_ready_ms=cur["local_ready_ms"],  # type: ignore[arg-type]
                    points=points,
                )
            )
    return sorted(samples, key=lambda sample: (sample.round_end_ts, sample.symbol))


def pick_direct(
    points: list[tuple[int, int, float]],
    method: str,
    pre_ms: int,
    post_ms: int,
) -> tuple[float, int, bool] | None:
    window = [(off, price) for off, _, price in points if -pre_ms <= off <= post_ms]
    if not window:
        return None
    exact = [(off, price) for off, price in window if off == 0]
    if exact:
        off, price = exact[-1]
        return price, abs(off), True
    if method == "last_before":
        before = [(off, price) for off, price in window if off <= 0]
        if not before:
            return None
        off, price = max(before, key=lambda item: item[0])
        return price, abs(off), False
    if method == "after_then_before":
        after = [(off, price) for off, price in window if off >= 0]
        if after:
            off, price = min(after, key=lambda item: item[0])
            return price, abs(off), False
        before = [(off, price) for off, price in window if off < 0]
        if before:
            off, price = max(before, key=lambda item: item[0])
            return price, abs(off), False
        return None
    if method == "nearest_abs":
        off, price = min(window, key=lambda item: (abs(item[0]), item[0] > 0, item[0]))
        return price, abs(off), False
    raise ValueError(f"unsupported direct method: {method}")


def pick_projected(
    points: list[tuple[int, int, float]],
    method: str,
    pre_ms: int,
    post_ms: int,
    max_project_bps: float,
) -> tuple[float, int, bool] | None:
    window = [(off, price) for off, _, price in points if -pre_ms <= off <= post_ms]
    if not window:
        return None
    exact = [(off, price) for off, price in window if off == 0]
    if exact:
        off, price = exact[-1]
        return price, abs(off), True
    before = [(off, price) for off, price in window if off < 0]
    after = [(off, price) for off, price in window if off > 0]
    if before and after:
        left = max(before, key=lambda item: item[0])
        right = min(after, key=lambda item: item[0])
        lo, lp = left
        hi, hp = right
        if hi == lo:
            pred = (lp + hp) / 2.0
        else:
            pred = lp + (0 - lo) * (hp - lp) / (hi - lo)
        return pred, max(abs(lo), abs(hi)), False
    if method != "linear_project":
        return pick_direct(points, "nearest_abs", pre_ms, post_ms)
    same_side = before if before else after
    same_side = sorted(same_side, key=lambda item: abs(item[0]))[:2]
    if len(same_side) < 2:
        return pick_direct(points, "nearest_abs", pre_ms, post_ms)
    (off1, price1), (off2, price2) = same_side
    if off2 == off1:
        pred = price1
    else:
        pred = price1 + (0 - off1) * (price2 - price1) / (off2 - off1)
    nearest_price = price1
    move_bps = bps_diff(pred, nearest_price)
    if max_project_bps > 0 and move_bps > max_project_bps:
        direction = 1.0 if pred >= nearest_price else -1.0
        pred = nearest_price * (1.0 + direction * max_project_bps / 10_000.0)
    return pred, max(abs(off1), abs(off2)), False


def source_price(
    points: list[tuple[int, int, float]],
    method: str,
    pre_ms: int,
    post_ms: int,
    max_project_bps: float,
) -> tuple[float, int, bool] | None:
    if method in {"last_before", "after_then_before", "nearest_abs"}:
        return pick_direct(points, method, pre_ms, post_ms)
    if method in {"linear_interp", "linear_project"}:
        return pick_projected(points, method, pre_ms, post_ms, max_project_bps)
    raise ValueError(f"unsupported source method: {method}")


def weighted_median(values: list[tuple[float, float]]) -> float | None:
    total = sum(weight for _, weight in values if weight > 0)
    if total <= 0:
        return None
    acc = 0.0
    for price, weight in sorted(values, key=lambda item: item[0]):
        if weight <= 0:
            continue
        acc += weight
        if acc >= total / 2.0:
            return price
    return values[-1][0] if values else None


def aggregate(rows: list[tuple[str, float, float]], method: str) -> float | None:
    if not rows:
        return None
    if method == "mean":
        total = sum(weight for _, _, weight in rows)
        if total <= 0:
            return None
        return sum(price * weight for _, price, weight in rows) / total
    if method == "median":
        return weighted_median([(price, weight) for _, price, weight in rows])
    if method == "trimmed_mean":
        prices = [price for _, price, _ in rows]
        med = statistics.median(prices)
        kept = [
            (source, price, weight)
            for source, price, weight in rows
            if bps_diff(price, med) <= 8.0 or len(rows) <= 2
        ]
        return aggregate(kept or rows, "mean")
    raise ValueError(f"unsupported aggregate method: {method}")


def temporal_weight(abs_delta_ms: int, decay_ms: float) -> float:
    return 1.0 / (1.0 + abs_delta_ms / max(decay_ms, 1.0))


def source_sets_for_symbol(symbol: str) -> list[tuple[str, tuple[str, ...]]]:
    sets: list[tuple[str, tuple[str, ...]]] = []
    current = tuple(src for src in CURRENT_SOURCE_SETS[symbol] if src in SOURCES)
    sets.append(("current", current))
    sets.append(("full", SOURCES))
    sets.append(("core_cex", ("binance", "bybit", "okx", "coinbase")))
    for source in SOURCES:
        sets.append((f"only_{source}", (source,)))
    for dropped in SOURCES:
        sets.append((f"drop_{dropped}", tuple(src for src in SOURCES if src != dropped)))
    seen = set()
    out = []
    for name, sources in sets:
        key = tuple(sources)
        if key in seen:
            continue
        seen.add(key)
        out.append((name, key))
    return out


def eval_model_on_sample(
    sample: Sample,
    source_set: tuple[str, ...],
    source_method: str,
    agg_method: str,
    min_sources: int,
    pre_ms: int,
    post_ms: int,
    decay_ms: float,
    max_project_bps: float,
) -> dict | None:
    picked = []
    exact_count = 0
    deltas = []
    for source in source_set:
        points = sample.points.get(source)
        if not points:
            continue
        got = source_price(points, source_method, pre_ms, post_ms, max_project_bps)
        if got is None:
            continue
        price, abs_delta_ms, exact = got
        exact_count += 1 if exact else 0
        deltas.append(abs_delta_ms)
        weight = BASE_WEIGHTS.get(source, 1.0) * temporal_weight(abs_delta_ms, decay_ms)
        if exact:
            weight *= 1.25
        if weight > 0 and math.isfinite(price):
            picked.append((source, price, weight))
    if len(picked) < min_sources:
        return None
    pred = aggregate(picked, agg_method)
    if pred is None or not math.isfinite(pred) or pred <= 0:
        return None
    prices = [price for _, price, _ in picked]
    spread = 0.0
    if len(prices) > 1:
        spread = (max(prices) - min(prices)) / max(abs(pred), 1e-12) * 10_000.0
    return {
        "pred_close": pred,
        "source_count": len(picked),
        "exact_sources": exact_count,
        "median_abs_delta_ms": statistics.median(deltas) if deltas else None,
        "source_spread_bps": spread,
        "sources": ";".join(source for source, _, _ in picked),
    }


def model_name(parts: tuple[str, str, str, int]) -> str:
    set_name, source_method, agg_method, min_sources = parts
    return f"{set_name}|{source_method}|{agg_method}|n{min_sources}"


def score_rows(rows: list[dict]) -> dict:
    errors = [row["close_diff_bps"] for row in rows]
    if not errors:
        return {}
    return {
        "n": len(errors),
        "mean_bps": statistics.mean(errors),
        "p50_bps": percentile(errors, 0.50),
        "p90_bps": percentile(errors, 0.90),
        "p95_bps": percentile(errors, 0.95),
        "p99_bps": percentile(errors, 0.99),
        "max_bps": max(errors),
        "pct_le_1bps": sum(1 for err in errors if err <= 1.0) / len(errors),
        "side_errors": sum(1 for row in rows if row["side_error"]),
    }


def raw_model_rows(
    samples: list[Sample],
    source_methods: list[str],
    agg_methods: list[str],
    pre_ms: int,
    post_ms: int,
    decay_ms: float,
    max_project_bps: float,
) -> dict[tuple[str, str], list[dict]]:
    out: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for sample in samples:
        for set_name, source_set in source_sets_for_symbol(sample.symbol):
            for source_method, agg_method, min_sources in itertools.product(source_methods, agg_methods, (1, 2, 3)):
                if len(source_set) < min_sources:
                    continue
                hit = eval_model_on_sample(
                    sample,
                    source_set,
                    source_method,
                    agg_method,
                    min_sources,
                    pre_ms,
                    post_ms,
                    decay_ms,
                    max_project_bps,
                )
                if hit is None:
                    continue
                pred = hit["pred_close"]
                pred_yes = pred >= sample.rtds_open
                truth_yes = sample.rtds_close >= sample.rtds_open
                model = model_name((set_name, source_method, agg_method, min_sources))
                out[(sample.symbol, model)].append(
                    {
                        "symbol": sample.symbol,
                        "round_end_ts": sample.round_end_ts,
                        "model": model,
                        "pred_close": pred,
                        "rtds_open": sample.rtds_open,
                        "rtds_close": sample.rtds_close,
                        "pred_yes": pred_yes,
                        "truth_yes": truth_yes,
                        "side_error": pred_yes != truth_yes,
                        "close_diff_bps": bps_diff(pred, sample.rtds_close),
                        "signed_error_bps": signed_bps(pred, sample.rtds_close),
                        **hit,
                    }
                )
    return out


def summarize_static_models(model_rows: dict[tuple[str, str], list[dict]], min_coverage: float, sample_counts: dict[str, int]) -> list[dict]:
    summaries = []
    for (symbol, model), rows in model_rows.items():
        score = score_rows(rows)
        if not score:
            continue
        coverage = len(rows) / max(sample_counts[symbol], 1)
        if coverage < min_coverage:
            continue
        summaries.append(
            {
                "symbol": symbol,
                "model": model,
                "coverage": coverage,
                **score,
            }
        )
    summaries.sort(key=lambda row: (row["symbol"], row["p95_bps"], row["max_bps"], row["side_errors"], -row["coverage"]))
    return summaries


def walk_forward_bias_adjusted(
    rows: list[dict],
    min_train: int,
    train_rounds: int,
    bias_key: str,
) -> list[dict]:
    by_round = sorted({row["round_end_ts"] for row in rows})
    adjusted = []
    rows_by_round: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        rows_by_round[row["round_end_ts"]].append(row)
    for idx, round_ts in enumerate(by_round):
        if idx < min_train:
            continue
        train_set = set(by_round[max(0, idx - train_rounds) : idx])
        train_rows = [row for ts in train_set for row in rows_by_round[ts]]
        medians: dict[tuple[str, ...], float] = {}
        grouped: dict[tuple[str, ...], list[float]] = defaultdict(list)
        for row in train_rows:
            if bias_key == "model":
                key = (row["model"],)
            elif bias_key == "model_sources":
                key = (row["model"], row["sources"])
            else:
                key = (row["model"],)
            grouped[key].append(row["signed_error_bps"])
        for key, vals in grouped.items():
            if len(vals) >= 10:
                medians[key] = statistics.median(vals)
        for row in rows_by_round[round_ts]:
            if bias_key == "model":
                key = (row["model"],)
            elif bias_key == "model_sources":
                key = (row["model"], row["sources"])
            else:
                key = (row["model"],)
            bias = medians.get(key, 0.0)
            corrected = row["pred_close"] / (1.0 + bias / 10_000.0)
            pred_yes = corrected >= row["rtds_open"]
            truth_yes = row["truth_yes"]
            out = dict(row)
            out["pred_close"] = corrected
            out["bias_bps"] = bias
            out["close_diff_bps"] = bps_diff(corrected, row["rtds_close"])
            out["signed_error_bps"] = signed_bps(corrected, row["rtds_close"])
            out["pred_yes"] = pred_yes
            out["side_error"] = pred_yes != truth_yes
            adjusted.append(out)
    return adjusted


def summarize_bias_models(
    model_rows: dict[tuple[str, str], list[dict]],
    min_coverage: float,
    sample_counts: dict[str, int],
    min_train: int,
    train_rounds: int,
) -> list[dict]:
    out = []
    for (symbol, model), rows in model_rows.items():
        if len(rows) / max(sample_counts[symbol], 1) < min_coverage:
            continue
        for bias_key in ("model", "model_sources"):
            adjusted = walk_forward_bias_adjusted(rows, min_train, train_rounds, bias_key)
            score = score_rows(adjusted)
            if not score:
                continue
            out.append(
                {
                    "symbol": symbol,
                    "model": model,
                    "variant": f"bias_{bias_key}",
                    "coverage": len(rows) / max(sample_counts[symbol], 1),
                    **score,
                }
            )
    out.sort(key=lambda row: (row["symbol"], row["p95_bps"], row["max_bps"], row["side_errors"], -row["coverage"]))
    return out


def best_per_symbol(summaries: list[dict], limit: int) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in summaries:
        grouped[row["symbol"]].append(row)
    return {symbol: rows[:limit] for symbol, rows in sorted(grouped.items())}


def main() -> int:
    parser = argparse.ArgumentParser(description="Research local-agg price projection models before gating.")
    parser.add_argument("--boundary-csv", required=True)
    parser.add_argument("--out-json", default="/tmp/local_agg_price_projection_research.json")
    parser.add_argument("--pre-ms", type=int, default=5_000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument("--decay-ms", type=float, default=900.0)
    parser.add_argument("--max-project-bps", type=float, default=2.0)
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--min-train-rounds", type=int, default=80)
    parser.add_argument("--bias-train-rounds", type=int, default=240)
    parser.add_argument("--top-n", type=int, default=8)
    parser.add_argument("--ignore-ready-ms", action="store_true")
    args = parser.parse_args()

    samples = load_samples(Path(args.boundary_csv), respect_ready_ms=not args.ignore_ready_ms)
    sample_counts = defaultdict(int)
    for sample in samples:
        sample_counts[sample.symbol] += 1
    source_methods = ["last_before", "after_then_before", "nearest_abs", "linear_interp", "linear_project"]
    agg_methods = ["mean", "median", "trimmed_mean"]
    model_rows = raw_model_rows(
        samples,
        source_methods,
        agg_methods,
        args.pre_ms,
        args.post_ms,
        args.decay_ms,
        args.max_project_bps,
    )
    static = summarize_static_models(model_rows, args.min_coverage, sample_counts)
    bias = summarize_bias_models(
        model_rows,
        args.min_coverage,
        sample_counts,
        args.min_train_rounds,
        args.bias_train_rounds,
    )
    result = {
        "sample_counts": dict(sorted(sample_counts.items())),
        "config": {
            "pre_ms": args.pre_ms,
            "post_ms": args.post_ms,
            "decay_ms": args.decay_ms,
            "max_project_bps": args.max_project_bps,
            "min_coverage": args.min_coverage,
            "min_train_rounds": args.min_train_rounds,
            "bias_train_rounds": args.bias_train_rounds,
            "ready_aware": not args.ignore_ready_ms,
        },
        "best_static_by_symbol": best_per_symbol(static, args.top_n),
        "best_bias_by_symbol": best_per_symbol(bias, args.top_n),
    }
    Path(args.out_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
