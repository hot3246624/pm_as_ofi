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


def parse_weights(raw: str | None, allowed: set[str]) -> dict[str, float]:
    if not raw:
        return {src: DEFAULT_WEIGHTS[src] for src in allowed}
    out: dict[str, float] = {}
    for token in raw.split(';'):
        token = token.strip()
        if not token or ':' not in token:
            continue
        src, val = token.split(':', 1)
        src = src.strip()
        if src not in allowed:
            continue
        try:
            w = float(val)
        except Exception:
            continue
        if w > 0:
            out[src] = w
    if out:
        return out
    return {src: DEFAULT_WEIGHTS[src] for src in allowed}


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


def load_policy(path: Path) -> dict[str, dict]:
    rows = list(csv.DictReader(path.open()))
    policy: dict[str, dict] = {}
    for row in rows:
        symbol = row["symbol"]
        if symbol in policy:
            continue
        subset = row["source_subset"]
        if subset == "full":
            allowed = set(DEFAULT_WEIGHTS)
        elif subset.startswith("drop_"):
            dropped = set(token for token in subset.removeprefix("drop_").split("_") if token)
            allowed = {src for src in DEFAULT_WEIGHTS if src not in dropped}
        elif subset.startswith("only_"):
            allowed = set(token for token in subset.removeprefix("only_").split("_") if token)
        else:
            raise ValueError(f"unsupported subset {subset}")
        policy[symbol] = {
            "source_subset": subset,
            "allowed_sources": allowed,
            "rule": row["rule"],
            "min_sources": int(row["min_sources"]),
            "weights": parse_weights(row.get("weights"), allowed),
        }
    return policy


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate a per-symbol boundary policy against canonical boundary dataset.")
    parser.add_argument("--boundary-csv", default="logs/local_agg_boundary_dataset_close_only_latest.csv")
    parser.add_argument("--policy-csv", default="logs/local_agg_boundary_symbol_recommendation.csv")
    parser.add_argument("--out-csv", default="logs/local_agg_boundary_policy_eval.csv")
    parser.add_argument("--test-rounds", type=int, default=4)
    parser.add_argument("--pre-ms", type=int, default=5000)
    parser.add_argument("--post-ms", type=int, default=500)
    args = parser.parse_args()

    samples = canonical_samples(Path(args.boundary_csv))
    policy = load_policy(Path(args.policy_csv))
    rounds = sorted({sample["round_end_ts"] for sample in samples})
    test_rounds = set(rounds[-args.test_rounds :]) if args.test_rounds > 0 else set()

    out_rows = []
    summary = {
        "train_side_errors": 0,
        "test_side_errors": 0,
        "train_missing": 0,
        "test_missing": 0,
        "train_count": 0,
        "test_count": 0,
        "train_bps_sum": 0.0,
        "test_bps_sum": 0.0,
    }

    for sample in samples:
        rule = policy[sample["symbol"]]
        per_source = []
        for src in rule["allowed_sources"]:
            src_points = [(off, price) for s, off, price in sample["close_points"] if s == src]
            picked = pick_price(src_points, rule["rule"], args.pre_ms, args.post_ms)
            if picked is not None:
                per_source.append((src, picked))

        is_test = sample["round_end_ts"] in test_rounds
        row = {
            "symbol": sample["symbol"],
            "round_end_ts": sample["round_end_ts"],
            "split": "test" if is_test else "train",
            "source_subset": rule["source_subset"],
            "rule": rule["rule"],
            "min_sources": rule["min_sources"],
            "weights": ";".join(
                f"{src}:{rule['weights'][src]}" for src in sorted(rule["weights"])
            ),
            "source_count": len(per_source),
        }
        if len(per_source) < rule["min_sources"]:
            row["status"] = "missing"
            if is_test:
                summary["test_missing"] += 1
            else:
                summary["train_missing"] += 1
            out_rows.append(row)
            continue

        weights = rule["weights"]
        num = sum(weights[src] * price for src, price in per_source)
        den = sum(weights[src] for src, _ in per_source)
        pred_close = num / den
        pred_yes = pred_close >= sample["rtds_open"]
        side_error = pred_yes != sample["truth_yes"]
        bps = abs(pred_close - sample["rtds_close"]) / sample["rtds_close"] * 1e4

        row.update(
            {
                "status": "ok",
                "pred_close": pred_close,
                "rtds_open": sample["rtds_open"],
                "rtds_close": sample["rtds_close"],
                "truth_yes": sample["truth_yes"],
                "pred_yes": pred_yes,
                "side_error": side_error,
                "close_diff_bps": bps,
            }
        )

        if is_test:
            summary["test_count"] += 1
            summary["test_bps_sum"] += bps
            if side_error:
                summary["test_side_errors"] += 1
        else:
            summary["train_count"] += 1
            summary["train_bps_sum"] += bps
            if side_error:
                summary["train_side_errors"] += 1
        out_rows.append(row)

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "round_end_ts",
        "split",
                "source_subset",
                "rule",
                "min_sources",
                "weights",
                "source_count",
                "status",
                "pred_close",
        "rtds_open",
        "rtds_close",
        "truth_yes",
        "pred_yes",
        "side_error",
        "close_diff_bps",
    ]
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(
        {
            "train_side_errors": summary["train_side_errors"],
            "test_side_errors": summary["test_side_errors"],
            "train_missing": summary["train_missing"],
            "test_missing": summary["test_missing"],
            "train_mean_bps": (summary["train_bps_sum"] / summary["train_count"]) if summary["train_count"] else None,
            "test_mean_bps": (summary["test_bps_sum"] / summary["test_count"]) if summary["test_count"] else None,
            "rows": len(out_rows),
            "out_csv": str(out_path),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
