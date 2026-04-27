#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import random
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
RULES = ("last_before", "nearest_abs", "after_then_before")
WEIGHT_LEVELS = (0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0)


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
    raise ValueError(rule)


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


def pred_for_sample(sample: dict, allowed: set[str], rule: str, min_sources: int, weights: dict[str, float], pre_ms: int, post_ms: int):
    per_source = []
    for src in allowed:
        src_points = [(off, price) for s, off, price in sample["close_points"] if s == src]
        picked = pick_price(src_points, rule, pre_ms, post_ms)
        if picked is not None and weights.get(src, 0.0) > 0:
            per_source.append((src, picked))
    if len(per_source) < min_sources:
        return None
    num = sum(weights[src] * price for src, price in per_source)
    den = sum(weights[src] for src, _ in per_source)
    if den <= 0:
        return None
    pred_close = num / den
    pred_yes = pred_close >= sample["rtds_open"]
    bps = abs(pred_close - sample["rtds_close"]) / sample["rtds_close"] * 1e4
    return pred_close, pred_yes, bps, len(per_source)


def evaluate_structure(symbol_samples, test_rounds, allowed, rule, min_sources, weights, pre_ms, post_ms):
    train_side_errors = test_side_errors = 0
    train_missing = test_missing = 0
    train_bps = []
    test_bps = []
    for sample in symbol_samples:
        out = pred_for_sample(sample, allowed, rule, min_sources, weights, pre_ms, post_ms)
        is_test = sample["round_end_ts"] in test_rounds
        if out is None:
            if is_test:
                test_missing += 1
            else:
                train_missing += 1
            continue
        _, pred_yes, bps, _ = out
        if is_test:
            if pred_yes != sample["truth_yes"]:
                test_side_errors += 1
            test_bps.append(bps)
        else:
            if pred_yes != sample["truth_yes"]:
                train_side_errors += 1
            train_bps.append(bps)
    return {
        "train_side_errors": train_side_errors,
        "test_side_errors": test_side_errors,
        "train_missing": train_missing,
        "test_missing": test_missing,
        "train_mean_bps": (sum(train_bps) / len(train_bps)) if train_bps else None,
        "test_mean_bps": (sum(test_bps) / len(test_bps)) if test_bps else None,
    }


def better(a, b):
    if b is None:
        return True
    ka = (
        a["test_side_errors"],
        a["test_missing"],
        float("inf") if a["test_mean_bps"] is None else a["test_mean_bps"],
        a["train_side_errors"],
        a["train_missing"],
        float("inf") if a["train_mean_bps"] is None else a["train_mean_bps"],
    )
    kb = (
        b["test_side_errors"],
        b["test_missing"],
        float("inf") if b["test_mean_bps"] is None else b["test_mean_bps"],
        b["train_side_errors"],
        b["train_missing"],
        float("inf") if b["train_mean_bps"] is None else b["train_mean_bps"],
    )
    return ka < kb


def random_weight_candidates(active_sources, rng, n):
    # always include default/prioritized candidates
    base = {s: DEFAULT_WEIGHTS.get(s, 1.0) for s in active_sources}
    yield base
    yield {s: 1.0 for s in active_sources}
    for _ in range(n):
        ws = {}
        # two families: discrete grid and dirichlet-like
        if rng.random() < 0.5:
            for s in active_sources:
                ws[s] = rng.choice(WEIGHT_LEVELS)
        else:
            raw = [rng.gammavariate(1.0, 1.0) for _ in active_sources]
            scale = rng.choice((1.0, 2.0, 3.0, 4.0))
            for s, r in zip(active_sources, raw):
                ws[s] = max(0.05, r * scale)
        yield ws


def main():
    ap = argparse.ArgumentParser(description='Recommend per-symbol weighted boundary close policies')
    ap.add_argument('--boundary-csv', default='logs/local_agg_boundary_dataset_close_only_latest.csv')
    ap.add_argument('--out-csv', default='logs/local_agg_boundary_weighted_recommendation.csv')
    ap.add_argument('--test-rounds', type=int, default=6)
    ap.add_argument('--pre-ms', type=int, default=5000)
    ap.add_argument('--post-ms', type=int, default=500)
    ap.add_argument('--top-k-structures', type=int, default=4)
    ap.add_argument('--random-weights', type=int, default=1500)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--symbols', default='', help='comma-separated symbol filter, e.g. btc/usd,sol/usd')
    args = ap.parse_args()

    samples = canonical_samples(Path(args.boundary_csv))
    rounds = sorted({s['round_end_ts'] for s in samples})
    test_rounds = set(rounds[-args.test_rounds:]) if args.test_rounds > 0 else set()
    subsets = source_subsets(DEFAULT_WEIGHTS.keys())
    rng = random.Random(args.seed)

    all_rows = []
    best_rows = []
    selected_symbols = sorted({s['symbol'] for s in samples})
    if args.symbols.strip():
        wanted = {s.strip() for s in args.symbols.split(',') if s.strip()}
        selected_symbols = [s for s in selected_symbols if s in wanted]

    for symbol in selected_symbols:
        symbol_samples = [s for s in samples if s['symbol'] == symbol]
        structure_rows = []
        for subset_name, allowed in subsets.items():
            for rule in RULES:
                for min_sources in (1,2,3):
                    metrics = evaluate_structure(symbol_samples, test_rounds, allowed, rule, min_sources, DEFAULT_WEIGHTS, args.pre_ms, args.post_ms)
                    row = {
                        'symbol': symbol,
                        'source_subset': subset_name,
                        'rule': rule,
                        'min_sources': min_sources,
                        'weights': ';'.join(f'{s}:{DEFAULT_WEIGHTS[s]}' for s in sorted(allowed)),
                        **metrics,
                        'stage': 'structure_baseline',
                    }
                    structure_rows.append(row)
        structure_rows.sort(key=lambda r:(r['test_side_errors'], r['test_missing'], float('inf') if r['test_mean_bps'] is None else r['test_mean_bps'], r['train_side_errors'], r['train_missing'], float('inf') if r['train_mean_bps'] is None else r['train_mean_bps']))
        top_structures = structure_rows[:args.top_k_structures]
        all_rows.extend(structure_rows)

        best_symbol = None
        for srow in top_structures:
            subset_name = srow['source_subset']
            allowed = subsets[subset_name]
            active_sources = sorted(allowed)
            for ws in random_weight_candidates(active_sources, rng, args.random_weights):
                metrics = evaluate_structure(symbol_samples, test_rounds, allowed, srow['rule'], int(srow['min_sources']), ws, args.pre_ms, args.post_ms)
                row = {
                    'symbol': symbol,
                    'source_subset': subset_name,
                    'rule': srow['rule'],
                    'min_sources': int(srow['min_sources']),
                    'weights': ';'.join(f'{src}:{ws[src]:.6f}' for src in active_sources),
                    **metrics,
                    'stage': 'weighted_search',
                }
                all_rows.append(row)
                if better(row, best_symbol):
                    best_symbol = row
        if best_symbol is not None:
            best_rows.append(best_symbol)

    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ['symbol','source_subset','rule','min_sources','weights','stage','train_side_errors','test_side_errors','train_missing','test_missing','train_mean_bps','test_mean_bps']
    with out.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(sorted(best_rows, key=lambda r:r['symbol']))
    detailed = out.with_name(out.stem + '_search.csv')
    with detailed.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)
    print({'best_rows':len(best_rows),'out_csv':str(out),'search_csv':str(detailed)})

if __name__ == '__main__':
    main()
