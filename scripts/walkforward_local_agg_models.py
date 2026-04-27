#!/usr/bin/env python3
import argparse
import csv
import importlib.util
import math
import statistics
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


FIT_SCRIPT = Path("/Users/hot/web3Scientist/pm_as_ofi/scripts/fit_local_agg_models_from_training_csv.py")


def load_fit_module():
    spec = importlib.util.spec_from_file_location("fitmod", FIT_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def build_catalog(mod, grid_start: float, grid_end: float, grid_step: float, decay_values, boost_values):
    models = mod.make_simple_models()
    models.extend(list(mod.build_weighted_models(mod.frange(grid_start, grid_end, grid_step), decay_values, boost_values)))
    models.extend(list(mod.build_return_anchor_models(mod.frange(grid_start, grid_end, grid_step), decay_values, boost_values)))
    return models


def family_filter(models, family: str):
    if family == "all":
        return list(models)
    if family == "close_only":
        return [m for m in models if not m.name.startswith("return_anchor")]
    if family == "return_anchor":
        return [m for m in models if m.name.startswith("return_anchor")]
    raise ValueError(f"unsupported family: {family}")


def score_windows(mod, samples, models, test_rounds: int, min_train_rounds: int):
    round_keys = sorted({s.round_end_ts for s in samples})
    out = []
    for i in range(min_train_rounds, len(round_keys) - test_rounds + 1):
        train_rounds = set(round_keys[:i])
        test_rounds_set = set(round_keys[i : i + test_rounds])
        train = [s for s in samples if s.round_end_ts in train_rounds]
        test = [s for s in samples if s.round_end_ts in test_rounds_set]
        if not train or not test:
            continue
        ranked = []
        for model in models:
            train_m = mod.score_model(train, model)
            ranked.append(
                (
                    (
                        train_m["missing"],
                        train_m["side_errors"],
                        train_m["p99_bps"],
                        train_m["p95_bps"],
                        train_m["mean_bps"],
                    ),
                    model,
                    train_m,
                )
            )
        ranked.sort(key=lambda x: x[0])
        best_key, best_model, train_m = ranked[0]
        test_m = mod.score_model(test, best_model)
        out.append(
            {
                "split_start_round": round_keys[i],
                "train_rounds": len(train_rounds),
                "test_rounds": len(test_rounds_set),
                "model": best_model.name,
                "train_missing": train_m["missing"],
                "train_side_errors": train_m["side_errors"],
                "train_mean_bps": train_m["mean_bps"],
                "train_p95_bps": train_m["p95_bps"],
                "train_p99_bps": train_m["p99_bps"],
                "test_n": test_m["n"],
                "test_missing": test_m["missing"],
                "test_side_errors": test_m["side_errors"],
                "test_mean_bps": test_m["mean_bps"],
                "test_p95_bps": test_m["p95_bps"],
                "test_p99_bps": test_m["p99_bps"],
                "test_side_match": test_m["side_match"],
            }
        )
    return out


def summarize(rows: List[Dict[str, object]]) -> Dict[str, float]:
    if not rows:
        return {
            "windows": 0,
            "total_test_side_errors": float("inf"),
            "total_test_missing": float("inf"),
            "mean_test_mean_bps": float("inf"),
            "max_test_p99_bps": float("inf"),
        }
    return {
        "windows": len(rows),
        "total_test_side_errors": sum(int(r["test_side_errors"]) for r in rows),
        "total_test_missing": sum(int(r["test_missing"]) for r in rows),
        "mean_test_mean_bps": statistics.fmean(float(r["test_mean_bps"]) for r in rows),
        "max_test_p99_bps": max(float(r["test_p99_bps"]) for r in rows),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Walk-forward evaluation for local price aggregation model families.")
    ap.add_argument("--training-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_training_canonical_rounds.csv")
    ap.add_argument("--symbols", default="", help="Comma-separated symbols. Default: all")
    ap.add_argument("--family", default="all", choices=["all", "close_only", "return_anchor"])
    ap.add_argument("--test-rounds", type=int, default=6)
    ap.add_argument("--min-train-rounds", type=int, default=12)
    ap.add_argument("--grid-start", type=float, default=0.5)
    ap.add_argument("--grid-end", type=float, default=1.5)
    ap.add_argument("--grid-step", type=float, default=0.5)
    ap.add_argument("--close-decay-grid", default="500,900")
    ap.add_argument("--exact-boost-grid", default="1.0,1.25")
    ap.add_argument("--out-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_walkforward.csv")
    args = ap.parse_args()

    mod = load_fit_module()
    symbols = {x.strip().lower() for x in args.symbols.split(",") if x.strip()} or None
    samples = mod.parse_training_csv(Path(args.training_csv), None, symbols)
    if not samples:
        print("No samples.")
        return 1
    decay_values = mod.parse_float_list(args.close_decay_grid)
    boost_values = mod.parse_float_list(args.exact_boost_grid)
    catalog = build_catalog(mod, args.grid_start, args.grid_end, args.grid_step, decay_values, boost_values)
    models = family_filter(catalog, args.family)
    rows = score_windows(mod, samples, models, args.test_rounds, args.min_train_rounds)
    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["split_start_round"])
        writer.writeheader()
        writer.writerows(rows)
    summ = summarize(rows)
    print(
        f"family={args.family} windows={summ['windows']} total_test_side_errors={summ['total_test_side_errors']} "
        f"total_test_missing={summ['total_test_missing']} mean_test_mean_bps={summ['mean_test_mean_bps']:.6f} "
        f"max_test_p99_bps={summ['max_test_p99_bps']:.6f}"
    )
    print(f"out_csv={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
