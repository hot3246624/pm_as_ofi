#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_WEIGHTS = {
    "binance": 0.5,
    "bybit": 0.5,
    "okx": 0.5,
    "coinbase": 1.0,
    "hyperliquid": 1.5,
}


def serialize_weights(weights: dict[str, float], allowed: set[str]) -> str:
    return ";".join(f"{src}:{weights[src]}" for src in sorted(allowed) if src in weights)


def pick_price(src_points: list[tuple[int, float]], rule: str, pre_ms: int, post_ms: int) -> float | None:
    pts = [(off, price) for off, price in src_points if -pre_ms <= off <= post_ms]
    if not pts:
        return None
    if rule == "last_before":
        before = [(off, price) for off, price in pts if off <= 0]
        if before:
            return max(before, key=lambda x: x[0])[1]
        return None
    if rule == "nearest_abs":
        return min(pts, key=lambda x: (abs(x[0]), x[0] > 0, x[0]))[1]
    if rule == "after_then_before":
        after = [(off, price) for off, price in pts if off >= 0]
        if after:
            return min(after, key=lambda x: x[0])[1]
        before = [(off, price) for off, price in pts if off <= 0]
        if before:
            return max(before, key=lambda x: x[0])[1]
        return None
    raise ValueError(f"unknown rule {rule}")


def canonical_samples(path: Path) -> list[dict]:
    rows = list(csv.DictReader(path.open()))
    by_instance = defaultdict(list)
    for row in rows:
        by_instance[(row["symbol"], row["round_end_ts"], row["instance_id"])].append(row)

    best = {}
    for (symbol, round_end_ts, instance_id), rs in by_instance.items():
        close_count = sum(1 for row in rs if row["phase"] == "close")
        total_count = len(rs)
        cand = (close_count, total_count, instance_id, rs)
        cur = best.get((symbol, round_end_ts))
        if cur is None or cand[:2] > cur[:2]:
            best[(symbol, round_end_ts)] = cand

    samples = []
    for (symbol, round_end_ts), (_, _, instance_id, rs) in best.items():
        first = rs[0]
        close_points = [
            (row["source"], int(row["offset_ms"]), float(row["price"]))
            for row in rs
            if row["phase"] == "close"
        ]
        samples.append(
            {
                "symbol": symbol,
                "round_end_ts": round_end_ts,
                "instance_id": instance_id,
                "rtds_open": float(first["rtds_open"]),
                "rtds_close": float(first["rtds_close"]),
                "truth_yes": float(first["rtds_close"]) >= float(first["rtds_open"]),
                "close_points": close_points,
            }
        )
    return samples


def source_subsets(all_sources: Iterable[str]) -> dict[str, set[str]]:
    all_sources = list(all_sources)
    out = {"full": set(all_sources)}
    for src in all_sources:
        out[f"drop_{src}"] = set(s for s in all_sources if s != src)
        out[f"only_{src}"] = {src}
    for i, a in enumerate(all_sources):
        for b in all_sources[i + 1 :]:
            out[f"only_{a}_{b}"] = {a, b}
    return out


def evaluate_symbol(
    samples: list[dict],
    symbol: str,
    test_rounds: set[str],
    weights: dict[str, float],
    pre_ms: int,
    post_ms: int,
) -> list[dict]:
    symbol_samples = [sample for sample in samples if sample["symbol"] == symbol]
    rules = ("last_before", "nearest_abs", "after_then_before")
    subsets = source_subsets(weights.keys())
    candidates: list[dict] = []

    for subset_name, allowed in subsets.items():
        for rule in rules:
            for min_sources in (1, 2, 3):
                train_side_errors = 0
                train_missing = 0
                train_bps: list[float] = []
                test_side_errors = 0
                test_missing = 0
                test_bps: list[float] = []

                for sample in symbol_samples:
                    per_source = []
                    for src in allowed:
                        src_points = [(off, price) for s, off, price in sample["close_points"] if s == src]
                        picked = pick_price(src_points, rule, pre_ms, post_ms)
                        if picked is not None:
                            per_source.append((src, picked))

                    is_test = sample["round_end_ts"] in test_rounds
                    if len(per_source) < min_sources:
                        if is_test:
                            test_missing += 1
                        else:
                            train_missing += 1
                        continue

                    num = sum(weights[src] * price for src, price in per_source)
                    den = sum(weights[src] for src, _ in per_source)
                    pred_close = num / den
                    pred_yes = pred_close >= sample["rtds_open"]
                    bps = abs(pred_close - sample["rtds_close"]) / sample["rtds_close"] * 1e4

                    if is_test:
                        if pred_yes != sample["truth_yes"]:
                            test_side_errors += 1
                        test_bps.append(bps)
                    else:
                        if pred_yes != sample["truth_yes"]:
                            train_side_errors += 1
                        train_bps.append(bps)

                candidates.append(
                    {
                        "symbol": symbol,
                        "source_subset": subset_name,
                        "rule": rule,
                        "min_sources": min_sources,
                        "weights": serialize_weights(weights, allowed),
                        "train_side_errors": train_side_errors,
                        "test_side_errors": test_side_errors,
                        "train_missing": train_missing,
                        "test_missing": test_missing,
                        "train_mean_bps": (sum(train_bps) / len(train_bps)) if train_bps else None,
                        "test_mean_bps": (sum(test_bps) / len(test_bps)) if test_bps else None,
                    }
                )

    candidates.sort(
        key=lambda row: (
            row["test_side_errors"],
            row["test_missing"],
            float("inf") if row["test_mean_bps"] is None else row["test_mean_bps"],
            row["train_side_errors"],
            row["train_missing"],
            float("inf") if row["train_mean_bps"] is None else row["train_mean_bps"],
        )
    )
    return candidates


def main() -> int:
    parser = argparse.ArgumentParser(description="Recommend per-symbol boundary close rules from canonical boundary tape dataset.")
    parser.add_argument("--boundary-csv", default="logs/local_agg_boundary_dataset_close_only_latest.csv")
    parser.add_argument("--out-csv", default="logs/local_agg_boundary_symbol_recommendation.csv")
    parser.add_argument("--test-rounds", type=int, default=4)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--pre-ms", type=int, default=5000)
    parser.add_argument("--post-ms", type=int, default=500)
    args = parser.parse_args()

    samples = canonical_samples(Path(args.boundary_csv))
    rounds = sorted({sample["round_end_ts"] for sample in samples})
    test_rounds = set(rounds[-args.test_rounds :]) if args.test_rounds > 0 else set()
    symbols = sorted({sample["symbol"] for sample in samples})

    rows: list[dict] = []
    for symbol in symbols:
        rows.extend(
            evaluate_symbol(
                samples=samples,
                symbol=symbol,
                test_rounds=test_rounds,
                weights=DEFAULT_WEIGHTS,
                pre_ms=args.pre_ms,
                post_ms=args.post_ms,
            )[: args.top_k]
        )

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "symbol",
                "source_subset",
                "rule",
                "min_sources",
                "weights",
                "train_side_errors",
                "test_side_errors",
                "train_missing",
                "test_missing",
                "train_mean_bps",
                "test_mean_bps",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {len(rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
