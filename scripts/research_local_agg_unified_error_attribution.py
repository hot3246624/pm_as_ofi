#!/usr/bin/env python3
"""Unified local-agg model and error-attribution research.

This offline tool tests whether one common estimator family can explain all
seven assets before introducing per-symbol behavior. It scores shared
source/method/aggregation choices, then attributes residuals to normalized
volatility, source dispersion, and freshness.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

from research_local_agg_price_projection import (
    CURRENT_SOURCE_SETS,
    SOURCES,
    Sample,
    aggregate,
    bps_diff,
    load_samples,
    percentile,
    source_price,
)


SCOPES = {
    "current": None,
    "full": SOURCES,
    "core_cex": ("binance", "bybit", "okx", "coinbase"),
}
SOURCE_METHODS = ("last_before", "after_then_before", "linear_interp", "linear_project")
AGG_METHODS = ("mean", "median", "trimmed_mean")


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


def source_set_for(sample: Sample, scope: str) -> tuple[str, ...]:
    if scope == "current":
        return CURRENT_SOURCE_SETS[sample.symbol]
    return tuple(SCOPES[scope] or ())


def temporal_weight(abs_delta_ms: int, decay_ms: float) -> float:
    return 1.0 / (1.0 + abs_delta_ms / max(decay_ms, 1.0))


def boundary_vol_bps(sample: Sample, source_set: tuple[str, ...], lookback_ms: int, post_ms: int) -> float:
    ranges = []
    for source in source_set:
        points = sample.points.get(source)
        if not points:
            continue
        prices = [price for offset_ms, _, price in points if -lookback_ms <= offset_ms <= post_ms]
        if len(prices) >= 2:
            med = statistics.median(prices)
            ranges.append((max(prices) - min(prices)) / max(abs(med), 1e-12) * 10_000.0)
    return statistics.median(ranges) if ranges else 0.0


def eval_sample(
    sample: Sample,
    scope: str,
    method: str,
    agg_method: str,
    min_sources: int,
    pre_ms: int,
    post_ms: int,
    decay_ms: float,
    max_project_bps: float,
    vol_lookback_ms: int,
) -> dict | None:
    source_set = source_set_for(sample, scope)
    picked = []
    deltas = []
    for source in source_set:
        points = sample.points.get(source)
        if not points:
            continue
        got = source_price(points, method, pre_ms, post_ms, max_project_bps)
        if got is None:
            continue
        price, abs_delta_ms, exact = got
        weight = temporal_weight(abs_delta_ms, decay_ms)
        if exact:
            weight *= 1.25
        picked.append((source, price, weight))
        deltas.append(abs_delta_ms)
    if len(picked) < min_sources:
        return None
    pred = aggregate(picked, agg_method)
    if pred is None or not math.isfinite(pred) or pred <= 0:
        return None
    prices = [price for _, price, _ in picked]
    source_spread_bps = 0.0
    if len(prices) > 1:
        source_spread_bps = (max(prices) - min(prices)) / max(abs(statistics.median(prices)), 1e-12) * 10_000.0
    cex_prices = [price for source, price, _ in picked if source in {"binance", "bybit", "okx", "coinbase"}]
    hl_prices = [price for source, price, _ in picked if source == "hyperliquid"]
    cex_hl_spread_bps = None
    if cex_prices and hl_prices:
        cex = statistics.median(cex_prices)
        hl = statistics.median(hl_prices)
        cex_hl_spread_bps = abs(cex - hl) / max(abs(statistics.median([cex, hl])), 1e-12) * 10_000.0
    pred_yes = pred >= sample.rtds_open
    truth_yes = sample.rtds_close >= sample.rtds_open
    vol = boundary_vol_bps(sample, source_set, vol_lookback_ms, post_ms)
    err = bps_diff(pred, sample.rtds_close)
    return {
        "symbol": sample.symbol,
        "round_end_ts": sample.round_end_ts,
        "pred_close": pred,
        "close_diff_bps": err,
        "side_error": pred_yes != truth_yes,
        "source_count": len(picked),
        "source_spread_bps": source_spread_bps,
        "cex_hl_spread_bps": cex_hl_spread_bps,
        "median_abs_delta_ms": statistics.median(deltas) if deltas else 0.0,
        "max_abs_delta_ms": max(deltas) if deltas else 0.0,
        "boundary_vol_bps": vol,
        "error_over_vol": err / max(vol, 0.25),
        "error_over_spread": err / max(source_spread_bps, 0.25),
    }


def model_name(scope: str, method: str, agg_method: str, min_sources: int) -> str:
    return f"{scope}|{method}|{agg_method}|n{min_sources}"


def evaluate_models(
    samples: list[Sample],
    pre_ms: int,
    post_ms: int,
    decay_ms: float,
    max_project_bps: float,
    vol_lookback_ms: int,
    min_coverage: float,
) -> tuple[list[dict], dict[str, list[dict]]]:
    total = len(samples)
    rows_by_model: dict[str, list[dict]] = defaultdict(list)
    for scope, method, agg_method, min_sources in itertools.product(SCOPES, SOURCE_METHODS, AGG_METHODS, (1, 2, 3)):
        name = model_name(scope, method, agg_method, min_sources)
        for sample in samples:
            row = eval_sample(
                sample,
                scope,
                method,
                agg_method,
                min_sources,
                pre_ms,
                post_ms,
                decay_ms,
                max_project_bps,
                vol_lookback_ms,
            )
            if row is not None:
                rows_by_model[name].append(row)
    summaries = []
    for name, rows in rows_by_model.items():
        score = score_rows(rows)
        if not score:
            continue
        coverage = len(rows) / max(total, 1)
        if coverage < min_coverage:
            continue
        by_symbol = defaultdict(list)
        for row in rows:
            by_symbol[row["symbol"]].append(row)
        symbol_p95 = {
            symbol: score_rows(symbol_rows)["p95_bps"]
            for symbol, symbol_rows in by_symbol.items()
            if score_rows(symbol_rows)
        }
        summaries.append(
            {
                "model": name,
                "coverage": coverage,
                "worst_symbol_p95_bps": max(symbol_p95.values()) if symbol_p95 else None,
                "symbol_p95_bps": symbol_p95,
                **score,
            }
        )
    summaries.sort(
        key=lambda row: (
            row["worst_symbol_p95_bps"] if row["worst_symbol_p95_bps"] is not None else float("inf"),
            row["p95_bps"],
            row["max_bps"],
            row["side_errors"],
            -row["coverage"],
        )
    )
    return summaries, rows_by_model


def bin_label(value: float | None, cuts: tuple[float, ...]) -> str:
    if value is None:
        return "missing"
    prev = 0.0
    for cut in cuts:
        if value < cut:
            return f"{prev:g}-{cut:g}"
        prev = cut
    return f">={cuts[-1]:g}"


def summarize_groups(rows: list[dict], key_fn) -> dict[str, dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[key_fn(row)].append(row)
    out = {}
    for key, group_rows in sorted(groups.items()):
        errors = [row["close_diff_bps"] for row in group_rows]
        out[str(key)] = {
            "n": len(group_rows),
            "p50_bps": percentile(errors, 0.50),
            "p95_bps": percentile(errors, 0.95),
            "max_bps": max(errors) if errors else None,
            "pct_le_1bps": sum(1 for err in errors if err <= 1.0) / len(errors) if errors else None,
            "side_errors": sum(1 for row in group_rows if row["side_error"]),
        }
    return out


def attribution(rows: list[dict]) -> dict:
    by_symbol = {}
    for symbol in sorted({row["symbol"] for row in rows}):
        symbol_rows = [row for row in rows if row["symbol"] == symbol]
        score = score_rows(symbol_rows)
        by_symbol[symbol] = {
            **score,
            "boundary_vol_p50_bps": percentile([row["boundary_vol_bps"] for row in symbol_rows], 0.50),
            "boundary_vol_p95_bps": percentile([row["boundary_vol_bps"] for row in symbol_rows], 0.95),
            "source_spread_p50_bps": percentile([row["source_spread_bps"] for row in symbol_rows], 0.50),
            "source_spread_p95_bps": percentile([row["source_spread_bps"] for row in symbol_rows], 0.95),
            "error_over_vol_p95": percentile([row["error_over_vol"] for row in symbol_rows], 0.95),
            "error_over_spread_p95": percentile([row["error_over_spread"] for row in symbol_rows], 0.95),
            "freshness_p95_ms": percentile([row["median_abs_delta_ms"] for row in symbol_rows], 0.95),
        }
    return {
        "by_symbol": by_symbol,
        "by_boundary_vol_bin": summarize_groups(
            rows, lambda row: bin_label(row["boundary_vol_bps"], (1.0, 2.0, 5.0, 10.0, 20.0))
        ),
        "by_source_spread_bin": summarize_groups(
            rows, lambda row: bin_label(row["source_spread_bps"], (1.0, 2.0, 5.0, 10.0, 20.0))
        ),
        "by_freshness_bin_ms": summarize_groups(
            rows, lambda row: bin_label(row["median_abs_delta_ms"], (50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0))
        ),
        "by_source_count": summarize_groups(rows, lambda row: row["source_count"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Research unified local-agg estimator error attribution.")
    parser.add_argument("--boundary-csv", required=True)
    parser.add_argument("--out-json", default="/tmp/local_agg_unified_error_attribution.json")
    parser.add_argument("--pre-ms", type=int, default=5_000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument("--decay-ms", type=float, default=900.0)
    parser.add_argument("--max-project-bps", type=float, default=2.0)
    parser.add_argument("--vol-lookback-ms", type=int, default=2_000)
    parser.add_argument("--min-coverage", type=float, default=0.75)
    parser.add_argument("--top-n", type=int, default=8)
    parser.add_argument("--ignore-ready-ms", action="store_true")
    args = parser.parse_args()

    samples = load_samples(Path(args.boundary_csv), respect_ready_ms=not args.ignore_ready_ms)
    summaries, rows_by_model = evaluate_models(
        samples,
        args.pre_ms,
        args.post_ms,
        args.decay_ms,
        args.max_project_bps,
        args.vol_lookback_ms,
        args.min_coverage,
    )
    best_model = summaries[0]["model"] if summaries else ""
    best_rows = rows_by_model.get(best_model, [])
    result = {
        "config": {
            "pre_ms": args.pre_ms,
            "post_ms": args.post_ms,
            "decay_ms": args.decay_ms,
            "max_project_bps": args.max_project_bps,
            "vol_lookback_ms": args.vol_lookback_ms,
            "min_coverage": args.min_coverage,
            "ready_aware": not args.ignore_ready_ms,
        },
        "sample_counts": dict(sorted((symbol, sum(1 for sample in samples if sample.symbol == symbol)) for symbol in {s.symbol for s in samples})),
        "best_global_models": summaries[: args.top_n],
        "best_model_attribution": attribution(best_rows) if best_rows else {},
    }
    Path(args.out_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
