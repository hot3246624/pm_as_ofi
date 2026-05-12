#!/usr/bin/env python3
"""Walk-forward architecture models for local-agg close projection.

This is an offline research harness. It tries to reduce price error by learning
source-level timestamp alignment, residual bias, and reliability weights from
past rounds. It does not implement or relax any runtime gate.
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
    BASE_WEIGHTS,
    SOURCES,
    Sample,
    aggregate,
    bps_diff,
    load_samples,
    percentile,
    signed_bps,
    source_price,
    source_sets_for_symbol,
    temporal_weight,
)


SOURCE_METHODS = ("last_before", "after_then_before", "linear_interp", "linear_project")
AGG_METHODS = ("mean", "median", "trimmed_mean")

TARGETED_CANDIDATES = {
    "bnb/usd": (
        ("current", "after_then_before", "mean", 1),
        ("current", "after_then_before", "mean", 2),
        ("current", "linear_interp", "trimmed_mean", 2),
        ("full", "after_then_before", "trimmed_mean", 3),
        ("core_cex", "linear_interp", "trimmed_mean", 3),
    ),
    "btc/usd": (
        ("current", "last_before", "mean", 1),
        ("current", "after_then_before", "mean", 1),
        ("drop_bybit", "after_then_before", "mean", 3),
        ("core_cex", "linear_interp", "trimmed_mean", 3),
        ("full", "linear_interp", "trimmed_mean", 3),
    ),
    "doge/usd": (
        ("drop_bybit", "linear_interp", "trimmed_mean", 1),
        ("current", "linear_interp", "trimmed_mean", 2),
        ("current", "linear_project", "trimmed_mean", 2),
        ("drop_hyperliquid", "linear_interp", "trimmed_mean", 2),
        ("only_coinbase", "after_then_before", "mean", 1),
    ),
    "eth/usd": (
        ("current", "last_before", "mean", 1),
        ("current", "after_then_before", "mean", 1),
        ("drop_bybit", "after_then_before", "mean", 1),
        ("drop_bybit", "after_then_before", "mean", 2),
        ("full", "linear_interp", "trimmed_mean", 3),
    ),
    "hype/usd": (
        ("current", "linear_project", "mean", 1),
        ("current", "linear_project", "mean", 2),
        ("current", "linear_project", "mean", 3),
        ("core_cex", "linear_project", "mean", 2),
        ("core_cex", "linear_project", "mean", 3),
        ("drop_hyperliquid", "linear_interp", "trimmed_mean", 2),
        ("drop_hyperliquid", "linear_project", "trimmed_mean", 2),
        ("drop_bybit", "linear_project", "trimmed_mean", 2),
    ),
    "sol/usd": (
        ("only_coinbase", "last_before", "mean", 1),
        ("only_coinbase", "after_then_before", "mean", 1),
        ("current", "after_then_before", "mean", 2),
        ("drop_bybit", "after_then_before", "mean", 2),
        ("full", "linear_interp", "trimmed_mean", 3),
    ),
    "xrp/usd": (
        ("only_coinbase", "after_then_before", "mean", 1),
        ("only_coinbase", "last_before", "mean", 1),
        ("current", "after_then_before", "mean", 2),
        ("drop_okx", "after_then_before", "mean", 2),
        ("drop_okx", "linear_interp", "trimmed_mean", 2),
    ),
}


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


def shifted_source_price(
    sample: Sample,
    source: str,
    method: str,
    target_shift_ms: int,
    pre_ms: int,
    post_ms: int,
    max_project_bps: float,
) -> tuple[float, int, bool] | None:
    points = sample.points.get(source)
    if not points:
        return None
    shifted = [(offset_ms - target_shift_ms, ts_ms, price) for offset_ms, ts_ms, price in points]
    return source_price(shifted, method, pre_ms, post_ms, max_project_bps)


def precompute_source_picks(
    samples: list[Sample],
    sources: set[str],
    methods: set[str],
    shifts_ms: tuple[int, ...],
    pre_ms: int,
    post_ms: int,
    max_project_bps: float,
) -> dict[tuple[int, str, str, int], tuple[float, int, bool] | None]:
    cache = {}
    for idx, sample in enumerate(samples):
        for source in sources:
            if source not in sample.points:
                continue
            for method in methods:
                for shift_ms in shifts_ms:
                    cache[(idx, source, method, shift_ms)] = shifted_source_price(
                        sample,
                        source,
                        method,
                        shift_ms,
                        pre_ms,
                        post_ms,
                        max_project_bps,
                    )
    return cache


def source_shift_errors(
    train_indices: range,
    samples: list[Sample],
    pick_cache: dict[tuple[int, str, str, int], tuple[float, int, bool] | None],
    source: str,
    method: str,
    shift_ms: int,
) -> list[float]:
    errors = []
    for idx in train_indices:
        sample = samples[idx]
        got = pick_cache.get((idx, source, method, shift_ms))
        if got is None:
            continue
        price, _, _ = got
        errors.append(signed_bps(price, sample.rtds_close))
    return errors


def train_source_params(
    train_indices: range,
    samples: list[Sample],
    pick_cache: dict[tuple[int, str, str, int], tuple[float, int, bool] | None],
    source: str,
    method: str,
    shifts_ms: tuple[int, ...],
    min_rows: int,
) -> dict | None:
    candidates = []
    for shift_ms in shifts_ms:
        signed_errors = source_shift_errors(
            train_indices,
            samples,
            pick_cache,
            source,
            method,
            shift_ms,
        )
        if len(signed_errors) < min_rows:
            continue
        abs_errors = [abs(err) for err in signed_errors]
        candidates.append(
            {
                "shift_ms": shift_ms,
                "n": len(signed_errors),
                "bias_bps": statistics.median(signed_errors),
                "median_abs_bps": statistics.median(abs_errors),
                "p90_abs_bps": percentile(abs_errors, 0.90),
                "p95_abs_bps": percentile(abs_errors, 0.95),
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda row: (
            row["p95_abs_bps"],
            row["median_abs_bps"],
            abs(row["shift_ms"]),
            -row["n"],
        )
    )
    best = dict(candidates[0])
    best["reliability"] = min(4.0, max(0.15, 1.0 / max(best["p95_abs_bps"], 0.25)))
    return best


def candidate_configs(symbol: str) -> list[tuple[str, tuple[str, ...], str, str, int]]:
    by_name = dict(source_sets_for_symbol(symbol))
    configs = []
    if symbol in TARGETED_CANDIDATES:
        for set_name, method, agg_method, min_sources in TARGETED_CANDIDATES[symbol]:
            source_set = by_name.get(set_name)
            if source_set and len(source_set) >= min_sources:
                configs.append((set_name, source_set, method, agg_method, min_sources))
        return configs
    names = ("current", "full", "core_cex", "drop_bybit", "drop_okx", "drop_hyperliquid", "drop_binance", "only_coinbase")
    for set_name in names:
        source_set = by_name.get(set_name)
        if not source_set:
            continue
        for method, agg_method, min_sources in itertools.product(SOURCE_METHODS, AGG_METHODS, (1, 2, 3)):
            if len(source_set) < min_sources:
                continue
            configs.append((set_name, source_set, method, agg_method, min_sources))
    return configs


def model_name(set_name: str, method: str, agg_method: str, min_sources: int, variant: str) -> str:
    return f"{set_name}|{method}|{agg_method}|n{min_sources}|{variant}"


def eval_arch_models_for_symbol(
    symbol: str,
    samples: list[Sample],
    shifts_ms: tuple[int, ...],
    pre_ms: int,
    post_ms: int,
    decay_ms: float,
    max_project_bps: float,
    min_train_rounds: int,
    train_rounds: int,
    min_source_train_rows: int,
) -> dict[str, list[dict]]:
    rows_by_model: dict[str, list[dict]] = defaultdict(list)
    param_cache: dict[tuple[int, str, str], dict | None] = {}
    configs = candidate_configs(symbol)
    needed_sources = {source for _, source_set, _, _, _ in configs for source in source_set}
    needed_methods = {method for _, _, method, _, _ in configs}
    pick_cache = precompute_source_picks(
        samples,
        needed_sources,
        needed_methods,
        shifts_ms,
        pre_ms,
        post_ms,
        max_project_bps,
    )

    for idx, sample in enumerate(samples):
        if idx < min_train_rounds:
            continue
        train_indices = range(max(0, idx - train_rounds), idx)
        for set_name, source_set, method, agg_method, min_sources in configs:
            picked_by_variant: dict[str, list[tuple[str, float, float]]] = {
                "source_shift": [],
                "source_shift_bias": [],
                "source_shift_bias_rel": [],
            }
            details_by_variant: dict[str, list[str]] = {key: [] for key in picked_by_variant}
            for source in source_set:
                cache_key = (idx, source, method)
                if cache_key not in param_cache:
                    param_cache[cache_key] = train_source_params(
                        train_indices,
                        samples,
                        pick_cache,
                        source,
                        method,
                        shifts_ms,
                        min_source_train_rows,
                    )
                params = param_cache[cache_key]
                if params is None:
                    continue
                got = pick_cache.get((idx, source, method, int(params["shift_ms"])))
                if got is None:
                    continue
                price, abs_delta_ms, exact = got
                base_weight = BASE_WEIGHTS.get(source, 1.0) * temporal_weight(abs_delta_ms, decay_ms)
                if exact:
                    base_weight *= 1.25
                bias_price = price / (1.0 + float(params["bias_bps"]) / 10_000.0)
                rel_weight = base_weight * float(params["reliability"])
                picked_by_variant["source_shift"].append((source, price, base_weight))
                picked_by_variant["source_shift_bias"].append((source, bias_price, base_weight))
                picked_by_variant["source_shift_bias_rel"].append((source, bias_price, rel_weight))
                detail = (
                    f"{source}:shift={int(params['shift_ms'])}:bias={float(params['bias_bps']):.3f}:"
                    f"p95={float(params['p95_abs_bps']):.3f}:d={abs_delta_ms}"
                )
                for variant in details_by_variant:
                    details_by_variant[variant].append(detail)

            for variant, picked in picked_by_variant.items():
                if len(picked) < min_sources:
                    continue
                pred = aggregate(picked, agg_method)
                if pred is None or not math.isfinite(pred) or pred <= 0:
                    continue
                pred_yes = pred >= sample.rtds_open
                truth_yes = sample.rtds_close >= sample.rtds_open
                model = model_name(set_name, method, agg_method, min_sources, variant)
                rows_by_model[model].append(
                    {
                        "symbol": symbol,
                        "round_end_ts": sample.round_end_ts,
                        "model": model,
                        "pred_close": pred,
                        "rtds_open": sample.rtds_open,
                        "rtds_close": sample.rtds_close,
                        "close_diff_bps": bps_diff(pred, sample.rtds_close),
                        "signed_error_bps": signed_bps(pred, sample.rtds_close),
                        "side_error": pred_yes != truth_yes,
                        "source_count": len(picked),
                        "source_details": ";".join(details_by_variant[variant]),
                    }
                )
    return rows_by_model


def summarize_models(rows_by_model: dict[str, list[dict]], sample_count: int, min_coverage: float) -> list[dict]:
    out = []
    for model, rows in rows_by_model.items():
        score = score_rows(rows)
        if not score:
            continue
        coverage = len(rows) / max(sample_count, 1)
        if coverage < min_coverage:
            continue
        out.append({"model": model, "coverage": coverage, **score})
    out.sort(key=lambda row: (row["p95_bps"], row["max_bps"], row["side_errors"], -row["coverage"]))
    return out


def top_tails(rows_by_model: dict[str, list[dict]], model: str, limit: int) -> list[dict]:
    rows = sorted(rows_by_model.get(model, []), key=lambda row: row["close_diff_bps"], reverse=True)
    return [
        {
            "round_end_ts": row["round_end_ts"],
            "close_diff_bps": row["close_diff_bps"],
            "signed_error_bps": row["signed_error_bps"],
            "side_error": row["side_error"],
            "source_count": row["source_count"],
            "source_details": row["source_details"],
        }
        for row in rows[:limit]
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Research source-aligned local-agg architecture models.")
    parser.add_argument("--boundary-csv", required=True)
    parser.add_argument("--out-json", default="/tmp/local_agg_arch_model_research.json")
    parser.add_argument("--pre-ms", type=int, default=5_000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument("--decay-ms", type=float, default=900.0)
    parser.add_argument("--max-project-bps", type=float, default=2.0)
    parser.add_argument("--min-coverage", type=float, default=0.30)
    parser.add_argument("--min-train-rounds", type=int, default=80)
    parser.add_argument("--train-rounds", type=int, default=240)
    parser.add_argument("--min-source-train-rows", type=int, default=40)
    parser.add_argument("--top-n", type=int, default=8)
    parser.add_argument("--tail-n", type=int, default=5)
    parser.add_argument(
        "--shifts-ms",
        default="-2000,-1000,-500,-250,-100,-50,0,50,100,250,500,1000,2000",
    )
    parser.add_argument("--ignore-ready-ms", action="store_true")
    args = parser.parse_args()

    shifts_ms = tuple(int(item) for item in args.shifts_ms.split(",") if item.strip())
    samples = load_samples(Path(args.boundary_csv), respect_ready_ms=not args.ignore_ready_ms)
    by_symbol: dict[str, list[Sample]] = defaultdict(list)
    for sample in samples:
        by_symbol[sample.symbol].append(sample)

    best_by_symbol = {}
    tails_by_symbol = {}
    for symbol, symbol_samples in sorted(by_symbol.items()):
        rows_by_model = eval_arch_models_for_symbol(
            symbol,
            symbol_samples,
            shifts_ms,
            args.pre_ms,
            args.post_ms,
            args.decay_ms,
            args.max_project_bps,
            args.min_train_rounds,
            args.train_rounds,
            args.min_source_train_rows,
        )
        summaries = summarize_models(rows_by_model, len(symbol_samples), args.min_coverage)
        best_by_symbol[symbol] = summaries[: args.top_n]
        if summaries:
            tails_by_symbol[symbol] = top_tails(rows_by_model, summaries[0]["model"], args.tail_n)

    result = {
        "config": {
            "pre_ms": args.pre_ms,
            "post_ms": args.post_ms,
            "decay_ms": args.decay_ms,
            "max_project_bps": args.max_project_bps,
            "min_coverage": args.min_coverage,
            "min_train_rounds": args.min_train_rounds,
            "train_rounds": args.train_rounds,
            "min_source_train_rows": args.min_source_train_rows,
            "ready_aware": not args.ignore_ready_ms,
            "shifts_ms": shifts_ms,
        },
        "sample_counts": {symbol: len(rows) for symbol, rows in sorted(by_symbol.items())},
        "best_by_symbol": best_by_symbol,
        "tails_by_symbol": tails_by_symbol,
    }
    Path(args.out_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
