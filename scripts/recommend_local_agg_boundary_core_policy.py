#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


DEFAULT_WEIGHTS = {
    "binance": 0.5,
    "bybit": 0.5,
    "okx": 0.5,
    "coinbase": 1.0,
    "hyperliquid": 1.5,
}


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


def source_subsets(all_sources: list[str]) -> dict[str, set[str]]:
    out = {"full": set(all_sources)}
    for src in all_sources:
        out[f"drop_{src}"] = {s for s in all_sources if s != src}
    return out


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


def evaluate_subset(
    samples: list[dict],
    symbol: str,
    subset_name: str,
    allowed_sources: set[str],
    min_sources: int,
    core_rule: str,
    test_rounds: set[str],
    pre_ms: int,
    post_ms: int,
) -> tuple[int, int, float | None]:
    side_errors = 0
    missing = 0
    bps_values: list[float] = []
    for sample in samples:
        if sample["symbol"] != symbol:
            continue
        if sample["round_end_ts"] in test_rounds:
            continue
        per_source = []
        for src in allowed_sources:
            src_points = [(off, price) for s, off, price in sample["close_points"] if s == src]
            picked = pick_price(src_points, core_rule, pre_ms, post_ms)
            if picked is not None:
                per_source.append((src, picked))
        if len(per_source) < min_sources:
            missing += 1
            continue
        pred_close = sum(DEFAULT_WEIGHTS[src] * price for src, price in per_source) / sum(
            DEFAULT_WEIGHTS[src] for src, _ in per_source
        )
        pred_yes = pred_close >= sample["rtds_open"]
        if pred_yes != sample["truth_yes"]:
            side_errors += 1
        bps_values.append(abs(pred_close - sample["rtds_close"]) / sample["rtds_close"] * 1e4)
    return side_errors, missing, (sum(bps_values) / len(bps_values) if bps_values else None)


def main() -> int:
    parser = argparse.ArgumentParser(description="Recommend a shared-core boundary policy with per-symbol source subsets.")
    parser.add_argument("--boundary-csv", default="logs/local_agg_boundary_dataset_close_only_latest.csv")
    parser.add_argument("--out-csv", default="logs/local_agg_boundary_core_policy_recommendation.csv")
    parser.add_argument("--test-rounds", type=int, default=4)
    parser.add_argument("--pre-ms", type=int, default=5000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument("--top-k", type=int, default=3)
    args = parser.parse_args()

    samples = canonical_samples(Path(args.boundary_csv))
    rounds = sorted({sample["round_end_ts"] for sample in samples})
    test_rounds = set(rounds[-args.test_rounds :]) if args.test_rounds > 0 else set()
    symbols = sorted({sample["symbol"] for sample in samples})
    subsets = source_subsets(list(DEFAULT_WEIGHTS.keys()))
    core_rules = ("last_before", "nearest_abs", "after_then_before")

    all_rows: list[dict] = []
    best_core_summary = None

    for core_rule in core_rules:
        policy_rows = []
        for symbol in symbols:
            candidates = []
            for subset_name, allowed_sources in subsets.items():
                for min_sources in (1, 2, 3):
                    train_err, train_missing, train_mean_bps = evaluate_subset(
                        samples=samples,
                        symbol=symbol,
                        subset_name=subset_name,
                        allowed_sources=allowed_sources,
                        min_sources=min_sources,
                        core_rule=core_rule,
                        test_rounds=test_rounds,
                        pre_ms=args.pre_ms,
                        post_ms=args.post_ms,
                    )
                    candidates.append(
                        {
                            "symbol": symbol,
                            "core_rule": core_rule,
                            "source_subset": subset_name,
                            "rule": core_rule,
                            "min_sources": min_sources,
                            "train_side_errors": train_err,
                            "train_missing": train_missing,
                            "train_mean_bps": train_mean_bps,
                        }
                    )
            candidates.sort(
                key=lambda row: (
                    row["train_side_errors"],
                    row["train_missing"],
                    float("inf") if row["train_mean_bps"] is None else row["train_mean_bps"],
                )
            )
            policy_rows.extend(candidates[: args.top_k])

        # Evaluate top-1 per symbol as a full policy to rank the core rule.
        top1 = {}
        for row in policy_rows:
            top1.setdefault(row["symbol"], row)
        test_side_errors = 0
        test_missing = 0
        test_bps_sum = 0.0
        test_count = 0
        for sample in samples:
            row = top1[sample["symbol"]]
            subset_name = row["source_subset"]
            allowed_sources = subsets[subset_name]
            per_source = []
            for src in allowed_sources:
                src_points = [(off, price) for s, off, price in sample["close_points"] if s == src]
                picked = pick_price(src_points, core_rule, args.pre_ms, args.post_ms)
                if picked is not None:
                    per_source.append((src, picked))
            if sample["round_end_ts"] not in test_rounds:
                continue
            if len(per_source) < int(row["min_sources"]):
                test_missing += 1
                continue
            pred_close = sum(DEFAULT_WEIGHTS[src] * price for src, price in per_source) / sum(
                DEFAULT_WEIGHTS[src] for src, _ in per_source
            )
            pred_yes = pred_close >= sample["rtds_open"]
            if pred_yes != sample["truth_yes"]:
                test_side_errors += 1
            test_bps_sum += abs(pred_close - sample["rtds_close"]) / sample["rtds_close"] * 1e4
            test_count += 1

        summary = {
            "core_rule": core_rule,
            "test_side_errors": test_side_errors,
            "test_missing": test_missing,
            "test_mean_bps": (test_bps_sum / test_count) if test_count else None,
        }
        if best_core_summary is None or (
            summary["test_side_errors"],
            summary["test_missing"],
            float("inf") if summary["test_mean_bps"] is None else summary["test_mean_bps"],
        ) < (
            best_core_summary["test_side_errors"],
            best_core_summary["test_missing"],
            float("inf") if best_core_summary["test_mean_bps"] is None else best_core_summary["test_mean_bps"],
        ):
            best_core_summary = summary
        all_rows.extend(policy_rows)

    all_rows.sort(
        key=lambda row: (
            row["core_rule"] != best_core_summary["core_rule"],
            row["symbol"],
            row["train_side_errors"],
            row["train_missing"],
            float("inf") if row["train_mean_bps"] is None else row["train_mean_bps"],
        )
    )

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "symbol",
                "core_rule",
                "source_subset",
                "rule",
                "min_sources",
                "train_side_errors",
                "train_missing",
                "train_mean_bps",
            ],
        )
        writer.writeheader()
        writer.writerows(all_rows)

    print({"best_core_summary": best_core_summary, "rows": len(all_rows), "out_csv": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
