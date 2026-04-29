#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_WEIGHTS = {
    "binance": 0.5,
    "bybit": 0.5,
    "okx": 0.5,
    "coinbase": 1.0,
    "hyperliquid": 1.5,
}

ROUTER_POLICY = {
    "bnb/usd": {
        "source_subset": "drop_okx",
        "rule": "after_then_before",
        "min_sources": 1,
        "weights": {
            "binance": 0.5,
            "bybit": 0.5,
            "coinbase": 1.0,
            "hyperliquid": 1.5,
        },
    },
    "btc/usd": {
        "source_subset": "only_coinbase",
        "rule": "after_then_before",
        "min_sources": 1,
        "weights": {"coinbase": 1.0},
    },
    "doge/usd": {
        "source_subset": "drop_binance",
        "rule": "last_before",
        "min_sources": 1,
        "weights": {
            "bybit": 0.5,
            "okx": 0.5,
            "coinbase": 1.0,
            "hyperliquid": 1.5,
        },
    },
    "eth/usd": {
        "source_subset": "only_coinbase",
        "rule": "last_before",
        "min_sources": 1,
        "weights": {"coinbase": 1.0},
    },
    "hype/usd": {
        "source_subset": "drop_binance",
        "rule": "after_then_before",
        "min_sources": 2,
        "weights": {
            "bybit": 1.0,
            "okx": 1.0,
            "coinbase": 1.0,
            "hyperliquid": 0.25,
        },
    },
    "sol/usd": {
        "source_subset": "only_okx_coinbase",
        "rule": "after_then_before",
        "min_sources": 2,
        "weights": {"okx": 0.5, "coinbase": 1.0},
    },
    "xrp/usd": {
        "source_subset": "only_binance_coinbase",
        "rule": "nearest_abs",
        "min_sources": 1,
        "weights": {"binance": 0.462086, "coinbase": 2.329335},
    },
}


def allowed_sources(source_subset: str) -> set[str]:
    if source_subset == "full":
        return set(DEFAULT_WEIGHTS)
    if source_subset.startswith("drop_"):
        dropped = set(token for token in source_subset.removeprefix("drop_").split("_") if token)
        return set(DEFAULT_WEIGHTS) - dropped
    if source_subset.startswith("only_"):
        return set(token for token in source_subset.removeprefix("only_").split("_") if token)
    raise ValueError(f"unsupported source_subset={source_subset}")


def pick_price(
    src_points: Iterable[tuple[int, float]],
    rule: str,
    pre_ms: int,
    post_ms: int,
) -> tuple[int, float] | None:
    pts = [(off, price) for off, price in src_points if -pre_ms <= off <= post_ms]
    if not pts:
        return None
    if rule == "last_before":
        before = [(off, price) for off, price in pts if off <= 0]
        return max(before, key=lambda x: x[0]) if before else None
    if rule == "nearest_abs":
        return min(pts, key=lambda x: (abs(x[0]), x[0] > 0, x[0]))
    if rule == "after_then_before":
        after = [(off, price) for off, price in pts if off >= 0]
        if after:
            return min(after, key=lambda x: x[0])
        before = [(off, price) for off, price in pts if off <= 0]
        return max(before, key=lambda x: x[0]) if before else None
    raise ValueError(f"unsupported rule={rule}")


def instance_samples(path: Path) -> list[dict]:
    rows = list(csv.DictReader(path.open()))
    by_instance = defaultdict(list)
    for row in rows:
        by_instance[(row["symbol"], int(row["round_end_ts"]), row["instance_id"])].append(row)

    samples = []
    for (symbol, round_end_ts, instance_id), rs in by_instance.items():
        first = rs[0]
        samples.append(
            {
                "symbol": symbol,
                "round_end_ts": round_end_ts,
                "instance_id": instance_id,
                "log_file": first.get("log_file", ""),
                "rtds_open": float(first["rtds_open"]),
                "rtds_close": float(first["rtds_close"]),
                "close_points": [
                    (row["source"], int(row["offset_ms"]), float(row["price"]))
                    for row in rs
                    if row["phase"] == "close"
                ],
            }
        )
    return samples


def select_samples(samples: list[dict], sample_mode: str, instance_id: str) -> list[dict]:
    if instance_id:
        return [sample for sample in samples if sample["instance_id"] == instance_id]
    if sample_mode == "all":
        return samples

    selected = {}
    for sample in samples:
        key = (sample["symbol"], sample["round_end_ts"])
        if sample_mode == "latest":
            candidate = (sample["instance_id"], len(sample["close_points"]), sample)
            current = selected.get(key)
            if current is None or candidate[:2] > current[:2]:
                selected[key] = candidate
            continue

        close_count = len(sample["close_points"])
        candidate = (close_count, sample["instance_id"], sample)
        current = selected.get(key)
        if current is None or candidate[:2] > current[:2]:
            selected[key] = candidate
    return [candidate[-1] for candidate in selected.values()]


def router_filter_reason(symbol: str, rule: str, source_count: int, exact_sources: int,
                         spread_bps: float, margin_bps: float, side_yes: bool) -> str | None:
    if symbol == "bnb/usd":
        if (
            rule == "after_then_before"
            and source_count == 1
            and exact_sources == 0
            and side_yes
            and margin_bps < 1.0
        ):
            return "bnb_single_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and spread_bps >= 2.0
            and margin_bps < 1.5
        ):
            return "bnb_high_spread_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and spread_bps <= 0.8
            and margin_bps < 2.2
        ):
            return "bnb_tight_spread_yes_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and side_yes
            and margin_bps < 1.0
        ):
            return "bnb_yes_near_flat"
    elif symbol == "btc/usd":
        if rule == "after_then_before" and source_count == 1 and exact_sources == 0 and margin_bps < 0.25:
            return "btc_single_near_flat"
        if rule == "after_then_before" and source_count == 2 and exact_sources == 0 and margin_bps < 1.0:
            return "btc_two_source_near_flat"
    elif symbol == "xrp/usd":
        if rule == "nearest_abs" and source_count >= 2 and exact_sources == 0 and margin_bps < 0.45:
            return "xrp_nearest_near_flat"
        if (
            rule == "nearest_abs"
            and source_count >= 2
            and exact_sources == 0
            and spread_bps >= 2.0
            and margin_bps < 2.0
        ):
            return "xrp_nearest_wide_spread_near_flat"
        if rule == "nearest_abs" and source_count == 1 and exact_sources == 0 and margin_bps < 0.5:
            return "xrp_single_nearest_near_flat"
        if rule == "last_before" and exact_sources == 0 and side_yes and margin_bps < 1.5:
            return "xrp_last_yes_near_flat"
    elif symbol == "doge/usd":
        if rule == "last_before" and source_count == 1 and exact_sources == 0 and margin_bps < 1.5:
            return "doge_single_last_near_flat"
        if rule == "nearest_abs" and source_count >= 2 and exact_sources == 0 and margin_bps < 0.5:
            return "doge_nearest_near_flat"
    elif symbol == "hype/usd":
        if rule == "after_then_before" and source_count == 1 and exact_sources == 0 and margin_bps < 3.0:
            return "hype_single_after_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps <= 1.0
            and margin_bps < 2.0
        ):
            return "hype_two_after_tight_near_flat"
        if (
            rule == "after_then_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps >= 2.0
            and margin_bps < 1.5
        ):
            return "hype_two_after_wide_spread_near_flat"
        if rule == "nearest_abs" and exact_sources == 0:
            if source_count >= 2 and spread_bps <= 1.0 and margin_bps < 1.8:
                return "hype_nearest_near_flat"
            if source_count == 1 and margin_bps < 2.0:
                return "hype_single_nearest_near_flat"
    elif symbol == "eth/usd":
        if rule == "last_before" and source_count == 1 and exact_sources == 0 and margin_bps < 1.5:
            return "eth_single_last_near_flat"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps <= 1.5
            and margin_bps < 0.35
        ):
            return "eth_two_last_near_flat"
        if (
            rule == "last_before"
            and source_count == 2
            and exact_sources == 0
            and spread_bps <= 1.1
            and margin_bps < 0.2
        ):
            return "eth_two_last_tight_near_flat"
    elif symbol == "sol/usd":
        if rule == "after_then_before" and source_count == 2 and exact_sources == 0 and margin_bps < 1.0:
            return "sol_after_near_flat"
    return None


def evaluate_sample(sample: dict, pre_ms: int, post_ms: int) -> dict:
    symbol = sample["symbol"]
    policy = ROUTER_POLICY[symbol]
    per_source = []
    for source in allowed_sources(policy["source_subset"]):
        picked = pick_price(
            (
                (off, price)
                for src, off, price in sample["close_points"]
                if src == source
            ),
            policy["rule"],
            pre_ms,
            post_ms,
        )
        if picked is None:
            continue
        weight = policy["weights"].get(source, 0.0)
        if weight > 0.0:
            per_source.append((source, picked[0], picked[1], weight))

    if len(per_source) < policy["min_sources"]:
        return {
            "status": "missing",
            "symbol": symbol,
            "round_end_ts": sample["round_end_ts"],
            "source_count": len(per_source),
        }

    pred_close = sum(weight * price for _, _, price, weight in per_source) / sum(
        weight for _, _, _, weight in per_source
    )
    rtds_open = sample["rtds_open"]
    rtds_close = sample["rtds_close"]
    side_yes = pred_close >= rtds_open
    truth_yes = rtds_close >= rtds_open
    exact_sources = sum(1 for _, off, _, _ in per_source if off == 0)
    spread_bps = 0.0
    if len(per_source) >= 2:
        prices = [price for _, _, price, _ in per_source]
        spread_bps = (max(prices) - min(prices)) / max(abs(pred_close), 1e-12) * 10_000.0
    margin_bps = abs(pred_close - rtds_open) / max(abs(rtds_open), 1e-12) * 10_000.0
    close_diff_bps = abs(pred_close - rtds_close) / max(abs(rtds_close), 1e-12) * 10_000.0
    reason = router_filter_reason(
        symbol,
        policy["rule"],
        len(per_source),
        exact_sources,
        spread_bps,
        margin_bps,
        side_yes,
    )
    return {
        "status": "filtered" if reason else "ok",
        "filter_reason": reason or "",
        "symbol": symbol,
        "round_end_ts": sample["round_end_ts"],
        "instance_id": sample["instance_id"],
        "log_file": sample.get("log_file", ""),
        "source_subset": policy["source_subset"],
        "rule": policy["rule"],
        "min_sources": policy["min_sources"],
        "pred_close": pred_close,
        "rtds_open": rtds_open,
        "rtds_close": rtds_close,
        "pred_yes": side_yes,
        "truth_yes": truth_yes,
        "side_error": side_yes != truth_yes,
        "close_diff_bps": close_diff_bps,
        "direction_margin_bps": margin_bps,
        "source_count": len(per_source),
        "source_spread_bps": spread_bps,
        "exact_sources": exact_sources,
        "sources": ";".join(source for source, _, _, _ in sorted(per_source)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the Rust boundary_symbol_router_v1 policy.")
    parser.add_argument("--boundary-csv", default="logs/local_agg_boundary_dataset_close_only_latest.csv")
    parser.add_argument("--out-csv", default="logs/local_agg_boundary_router_eval.csv")
    parser.add_argument("--test-rounds", type=int, default=16)
    parser.add_argument("--pre-ms", type=int, default=5000)
    parser.add_argument("--post-ms", type=int, default=500)
    parser.add_argument(
        "--sample-mode",
        choices=("best", "latest", "all"),
        default="best",
        help="best=max close ticks per symbol/round; latest=max instance id; all=every instance.",
    )
    parser.add_argument("--instance-id", default="", help="Evaluate only one instance id, e.g. 20260429_180155.")
    args = parser.parse_args()

    samples = [
        sample
        for sample in select_samples(
            instance_samples(Path(args.boundary_csv)),
            args.sample_mode,
            args.instance_id,
        )
        if sample["symbol"] in ROUTER_POLICY
    ]
    rounds = sorted({sample["round_end_ts"] for sample in samples})
    test_rounds = set(rounds[-args.test_rounds:]) if args.test_rounds > 0 else set()

    rows = []
    summary = defaultdict(lambda: {"ok": 0, "side": 0, "filtered": 0, "missing": 0, "bps_sum": 0.0, "bps_max": 0.0})
    reasons = Counter()
    for sample in samples:
        row = evaluate_sample(sample, args.pre_ms, args.post_ms)
        split = "test" if sample["round_end_ts"] in test_rounds else "train"
        row["split"] = split
        rows.append(row)
        if row["status"] == "filtered":
            reasons[(row["symbol"], row["filter_reason"])] += 1
        for key in ((row["symbol"], split), ("ALL", split)):
            bucket = summary[key]
            if row["status"] == "missing":
                bucket["missing"] += 1
            elif row["status"] == "filtered":
                bucket["filtered"] += 1
            else:
                bucket["ok"] += 1
                bucket["side"] += int(row["side_error"])
                bucket["bps_sum"] += row["close_diff_bps"]
                bucket["bps_max"] = max(bucket["bps_max"], row["close_diff_bps"])

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "split",
        "instance_id",
        "log_file",
        "symbol",
        "round_end_ts",
        "status",
        "filter_reason",
        "source_subset",
        "rule",
        "min_sources",
        "source_count",
        "sources",
        "exact_sources",
        "source_spread_bps",
        "direction_margin_bps",
        "close_diff_bps",
        "pred_close",
        "rtds_open",
        "rtds_close",
        "pred_yes",
        "truth_yes",
        "side_error",
    ]
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    for key in sorted(summary):
        bucket = summary[key]
        mean_bps = bucket["bps_sum"] / bucket["ok"] if bucket["ok"] else None
        print(
            key,
            {
                "ok": bucket["ok"],
                "side": bucket["side"],
                "filtered": bucket["filtered"],
                "missing": bucket["missing"],
                "mean_bps": None if mean_bps is None else round(mean_bps, 6),
                "max_bps": round(bucket["bps_max"], 6),
            },
        )
    print("filter_reasons", dict(sorted(reasons.items())))
    print({"rows": len(rows), "out_csv": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
