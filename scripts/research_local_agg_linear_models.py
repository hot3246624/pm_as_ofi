#!/usr/bin/env python3
"""Walk-forward linear close-return models for local-agg research.

This is an offline modeling harness. It learns a small ridge regression from
available source prices, source timing, and cross-source spread to RDTS close
return. It is intended to test whether architecture-level modeling can reduce
close error toward 1bps before introducing any gate changes.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

from research_local_agg_price_projection import (
    SOURCES,
    Sample,
    bps_diff,
    load_samples,
    percentile,
    source_price,
    source_sets_for_symbol,
)


LAMBDA_VALUES = (1.0, 10.0, 100.0)
METHOD_GROUPS = ("linear_project", "after_then_before", "combo")

TARGETED_SET_NAMES = {
    "bnb/usd": ("current", "full", "core_cex"),
    "btc/usd": ("current", "core_cex", "drop_bybit", "full"),
    "doge/usd": ("current", "drop_bybit", "drop_hyperliquid", "only_coinbase"),
    "eth/usd": ("current", "drop_bybit", "full"),
    "hype/usd": ("current", "core_cex", "drop_bybit", "drop_hyperliquid"),
    "sol/usd": ("current", "drop_bybit", "full", "only_coinbase"),
    "xrp/usd": ("current", "drop_okx", "only_coinbase"),
}

TARGETED_LINEAR_CANDIDATES = {
    "bnb/usd": (
        ("current", "after_then_before", 1),
        ("current", "after_then_before", 2),
        ("current", "combo", 2),
        ("full", "combo", 3),
    ),
    "doge/usd": (
        ("drop_bybit", "linear_project", 1),
        ("drop_bybit", "combo", 1),
        ("current", "linear_project", 2),
        ("drop_hyperliquid", "combo", 2),
        ("only_coinbase", "combo", 1),
    ),
    "hype/usd": (
        ("current", "linear_project", 2),
        ("current", "combo", 2),
        ("current", "linear_project", 3),
        ("core_cex", "linear_project", 2),
        ("drop_hyperliquid", "combo", 2),
        ("drop_bybit", "combo", 2),
    ),
}


def solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float] | None:
    n = len(vector)
    aug = [row[:] + [vector[i]] for i, row in enumerate(matrix)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda idx: abs(aug[idx][col]))
        if abs(aug[pivot][col]) < 1e-10:
            return None
        if pivot != col:
            aug[col], aug[pivot] = aug[pivot], aug[col]
        div = aug[col][col]
        for j in range(col, n + 1):
            aug[col][j] /= div
        for r in range(n):
            if r == col:
                continue
            factor = aug[r][col]
            if factor == 0:
                continue
            for j in range(col, n + 1):
                aug[r][j] -= factor * aug[col][j]
    return [aug[i][n] for i in range(n)]


def ridge_fit(xs: list[list[float]], ys: list[float], lam: float) -> tuple[list[float], list[float], list[float]] | None:
    if not xs:
        return None
    cols = len(xs[0])
    means = [0.0] * cols
    stds = [1.0] * cols
    for col in range(1, cols):
        vals = [row[col] for row in xs]
        means[col] = statistics.mean(vals)
        std = statistics.pstdev(vals)
        stds[col] = std if std > 1e-9 else 1.0
    zxs = []
    for row in xs:
        zxs.append([row[0]] + [(row[col] - means[col]) / stds[col] for col in range(1, cols)])
    xtx = [[0.0 for _ in range(cols)] for _ in range(cols)]
    xty = [0.0 for _ in range(cols)]
    for row, y in zip(zxs, ys):
        for i in range(cols):
            xty[i] += row[i] * y
            for j in range(cols):
                xtx[i][j] += row[i] * row[j]
    for i in range(1, cols):
        xtx[i][i] += lam
    beta = solve_linear_system(xtx, xty)
    if beta is None:
        return None
    return beta, means, stds


def ridge_fit_from_sums(xtx_flat: list[float], xty: list[float], cols: int, lam: float) -> list[float] | None:
    matrix = []
    for i in range(cols):
        row = xtx_flat[i * cols : (i + 1) * cols]
        if i > 0:
            row = row[:]
            row[i] += lam
        matrix.append(row)
    return solve_linear_system(matrix, xty[:])


def ridge_predict(model: tuple[list[float], list[float], list[float]], x: list[float]) -> float:
    beta, means, stds = model
    zx = [x[0]] + [(x[col] - means[col]) / stds[col] for col in range(1, len(x))]
    return sum(coef * val for coef, val in zip(beta, zx))


def linear_predict(beta: list[float], x: list[float]) -> float:
    return sum(coef * val for coef, val in zip(beta, x))


def selected_prices(
    sample: Sample,
    method_group: str,
    pre_ms: int,
    post_ms: int,
    max_project_bps: float,
) -> dict[str, tuple[float, int]]:
    out = {}
    for source in SOURCES:
        points = sample.points.get(source)
        if not points:
            continue
        methods = ("linear_project", "last_before") if method_group == "combo" else (method_group,)
        chosen = None
        for method in methods:
            got = source_price(points, method, pre_ms, post_ms, max_project_bps)
            if got is None:
                continue
            price, abs_delta_ms, _ = got
            if chosen is None or abs_delta_ms < chosen[1]:
                chosen = (price, abs_delta_ms)
        if chosen is not None:
            out[source] = chosen
    return out


def feature_vector(
    sample: Sample,
    source_set: tuple[str, ...],
    method_group: str,
    pre_ms: int,
    post_ms: int,
    max_project_bps: float,
) -> tuple[list[float], int] | None:
    selected = selected_prices(sample, method_group, pre_ms, post_ms, max_project_bps)
    source_rows = [(src, selected[src]) for src in source_set if src in selected]
    if not source_rows:
        return None
    returns = {src: (price / sample.rtds_open - 1.0) * 10_000.0 for src, (price, _) in source_rows}
    prices = [price for _, (price, _) in source_rows]
    cex_returns = [returns[src] for src in ("binance", "bybit", "okx", "coinbase") if src in returns]
    all_returns = list(returns.values())
    hl_ret = returns.get("hyperliquid", 0.0)
    cex_med = statistics.median(cex_returns) if cex_returns else 0.0
    all_med = statistics.median(all_returns) if all_returns else 0.0
    spread_bps = 0.0
    if len(prices) > 1:
        spread_bps = (max(prices) - min(prices)) / max(abs(statistics.median(prices)), 1e-12) * 10_000.0
    features = [1.0, float(len(source_rows)), cex_med, all_med, hl_ret, hl_ret - cex_med, abs(hl_ret - cex_med), spread_bps]
    for src in SOURCES:
        if src in returns:
            price, abs_delta_ms = selected[src]
            features.extend([1.0, returns[src], min(abs_delta_ms, 5000) / 1000.0])
        else:
            features.extend([0.0, 0.0, 5.0])
    return features, len(source_rows)


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


def model_configs(symbol: str) -> list[tuple[str, tuple[str, ...], str, int, float]]:
    by_name = dict(source_sets_for_symbol(symbol))
    if symbol in TARGETED_LINEAR_CANDIDATES:
        configs = []
        for set_name, method_group, min_sources in TARGETED_LINEAR_CANDIDATES[symbol]:
            source_set = by_name.get(set_name)
            if not source_set or len(source_set) < min_sources:
                continue
            for lam in LAMBDA_VALUES:
                configs.append((set_name, source_set, method_group, min_sources, lam))
        return configs
    set_names = TARGETED_SET_NAMES.get(
        symbol,
        ("current", "full", "core_cex", "drop_bybit", "drop_okx", "drop_hyperliquid", "only_coinbase"),
    )
    configs = []
    for set_name in set_names:
        source_set = by_name.get(set_name)
        if not source_set:
            continue
        for method_group in METHOD_GROUPS:
            for min_sources in (1, 2, 3):
                if len(source_set) < min_sources:
                    continue
                for lam in LAMBDA_VALUES:
                    configs.append((set_name, source_set, method_group, min_sources, lam))
    return configs


def evaluate_symbol(
    symbol: str,
    samples: list[Sample],
    pre_ms: int,
    post_ms: int,
    max_project_bps: float,
    min_train_rounds: int,
    train_rounds: int,
    min_coverage: float,
) -> tuple[list[dict], dict[str, list[dict]]]:
    rows_by_model: dict[str, list[dict]] = defaultdict(list)
    configs = model_configs(symbol)
    base_configs = {}
    for set_name, source_set, method_group, min_sources, _ in configs:
        base_configs[(set_name, method_group, min_sources)] = (set_name, source_set, method_group, min_sources)

    for set_name, source_set, method_group, min_sources in base_configs.values():
        features: list[tuple[list[float], int] | None] = [
            feature_vector(sample, source_set, method_group, pre_ms, post_ms, max_project_bps)
            for sample in samples
        ]
        first = next((row[0] for row in features if row is not None), None)
        if first is None:
            continue
        cols = len(first)
        zero_flat = [0.0] * (cols * cols)
        zero_vec = [0.0] * cols
        prefix_xtx = [zero_flat]
        prefix_xty = [zero_vec]
        prefix_n = [0]
        for sample, row in zip(samples, features):
            xtx = prefix_xtx[-1].copy()
            xty = prefix_xty[-1].copy()
            count = prefix_n[-1]
            if row is not None and row[1] >= min_sources:
                x = row[0]
                y = (sample.rtds_close / sample.rtds_open - 1.0) * 10_000.0
                for i, xi in enumerate(x):
                    xty[i] += xi * y
                    base = i * cols
                    for j, xj in enumerate(x):
                        xtx[base + j] += xi * xj
                count += 1
            prefix_xtx.append(xtx)
            prefix_xty.append(xty)
            prefix_n.append(count)

        for idx, sample in enumerate(samples):
            if idx < min_train_rounds:
                continue
            test = features[idx]
            if test is None or test[1] < min_sources:
                continue
            train_start = max(0, idx - train_rounds)
            train_n = prefix_n[idx] - prefix_n[train_start]
            if train_n < min_train_rounds:
                continue
            xtx = [prefix_xtx[idx][i] - prefix_xtx[train_start][i] for i in range(cols * cols)]
            xty = [prefix_xty[idx][i] - prefix_xty[train_start][i] for i in range(cols)]
            for lam in LAMBDA_VALUES:
                beta = ridge_fit_from_sums(xtx, xty, cols, lam)
                if beta is None:
                    continue
                pred_ret_bps = linear_predict(beta, test[0])
                pred = sample.rtds_open * (1.0 + pred_ret_bps / 10_000.0)
                truth_yes = sample.rtds_close >= sample.rtds_open
                pred_yes = pred >= sample.rtds_open
                model_name = f"{set_name}|{method_group}|n{min_sources}|ridge{lam:g}"
                rows_by_model[model_name].append(
                    {
                        "round_end_ts": sample.round_end_ts,
                        "model": model_name,
                        "pred_close": pred,
                        "rtds_open": sample.rtds_open,
                        "rtds_close": sample.rtds_close,
                        "close_diff_bps": bps_diff(pred, sample.rtds_close),
                        "side_error": pred_yes != truth_yes,
                        "source_count": test[1],
                    }
                )

    summaries = []
    for model_name, rows in rows_by_model.items():
        score = score_rows(rows)
        if not score:
            continue
        coverage = len(rows) / max(len(samples), 1)
        if coverage < min_coverage:
            continue
        summaries.append({"model": model_name, "coverage": coverage, **score})
    summaries.sort(key=lambda row: (row["p95_bps"], row["max_bps"], row["side_errors"], -row["coverage"]))
    return summaries, rows_by_model


def top_tails(rows_by_model: dict[str, list[dict]], model_name: str, limit: int) -> list[dict]:
    rows = sorted(rows_by_model.get(model_name, []), key=lambda row: row["close_diff_bps"], reverse=True)
    return [
        {
            "round_end_ts": row["round_end_ts"],
            "close_diff_bps": row["close_diff_bps"],
            "side_error": row["side_error"],
            "source_count": row["source_count"],
        }
        for row in rows[:limit]
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Research walk-forward linear local-agg models.")
    parser.add_argument("--boundary-csv", required=True)
    parser.add_argument("--out-json", default="/tmp/local_agg_linear_model_research.json")
    parser.add_argument("--pre-ms", type=int, default=5_000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument("--max-project-bps", type=float, default=2.0)
    parser.add_argument("--min-coverage", type=float, default=0.30)
    parser.add_argument("--min-train-rounds", type=int, default=80)
    parser.add_argument("--train-rounds", type=int, default=240)
    parser.add_argument("--top-n", type=int, default=8)
    parser.add_argument("--tail-n", type=int, default=5)
    parser.add_argument("--symbols", default="", help="Comma-separated symbol filter, for example hype/usd,bnb/usd")
    parser.add_argument("--ignore-ready-ms", action="store_true")
    args = parser.parse_args()

    samples = load_samples(Path(args.boundary_csv), respect_ready_ms=not args.ignore_ready_ms)
    by_symbol: dict[str, list[Sample]] = defaultdict(list)
    for sample in samples:
        by_symbol[sample.symbol].append(sample)

    best_by_symbol = {}
    tails_by_symbol = {}
    wanted = {item.strip().lower() for item in args.symbols.split(",") if item.strip()}
    for symbol, symbol_samples in sorted(by_symbol.items()):
        if wanted and symbol not in wanted:
            continue
        summaries, rows_by_model = evaluate_symbol(
            symbol,
            symbol_samples,
            args.pre_ms,
            args.post_ms,
            args.max_project_bps,
            args.min_train_rounds,
            args.train_rounds,
            args.min_coverage,
        )
        best_by_symbol[symbol] = summaries[: args.top_n]
        if summaries:
            tails_by_symbol[symbol] = top_tails(rows_by_model, summaries[0]["model"], args.tail_n)

    result = {
        "config": {
            "pre_ms": args.pre_ms,
            "post_ms": args.post_ms,
            "max_project_bps": args.max_project_bps,
            "min_coverage": args.min_coverage,
            "min_train_rounds": args.min_train_rounds,
            "train_rounds": args.train_rounds,
            "ready_aware": not args.ignore_ready_ms,
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
