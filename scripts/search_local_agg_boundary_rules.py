#!/usr/bin/env python3
import argparse
import csv
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

SOURCES = ("binance", "bybit", "okx", "coinbase", "hyperliquid")


@dataclass
class Tick:
    offset_ms: int
    price: float


@dataclass
class Sample:
    symbol: str
    round_end_ts: int
    rtds_open: float
    rtds_close: float
    source_ticks: Dict[str, List[Tick]]


def to_float(raw: str) -> Optional[float]:
    if raw is None or raw == "":
        return None
    try:
        v = float(raw)
    except Exception:
        return None
    return v if math.isfinite(v) else None


def to_int(raw: str) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except Exception:
        return None


def parse_csv(path: Path, symbols: Optional[set[str]]) -> List[Sample]:
    groups: Dict[Tuple[str, int, str], Dict[str, object]] = {}
    with path.open() as f:
        for row in csv.DictReader(f):
            if row.get("phase") != "close":
                continue
            symbol = (row.get("symbol") or "").lower()
            if not symbol or (symbols and symbol not in symbols):
                continue
            round_end_ts = to_int(row.get("round_end_ts"))
            rtds_open = to_float(row.get("rtds_open"))
            rtds_close = to_float(row.get("rtds_close"))
            source = (row.get("source") or "").lower()
            offset_ms = to_int(row.get("offset_ms"))
            price = to_float(row.get("price"))
            if None in (round_end_ts, rtds_open, rtds_close, offset_ms, price) or source not in SOURCES:
                continue
            key = (symbol, round_end_ts, row.get("instance_id") or "")
            g = groups.setdefault(key, {
                "symbol": symbol,
                "round_end_ts": round_end_ts,
                "rtds_open": rtds_open,
                "rtds_close": rtds_close,
                "ticks": {src: [] for src in SOURCES},
            })
            g["ticks"][source].append(Tick(offset_ms=offset_ms, price=price))

    # Canonicalize duplicates across instances: keep the richest tape for each symbol/round.
    best: Dict[Tuple[str, int], Sample] = {}
    for (_, _, _inst), g in groups.items():
        ticks = {src: sorted(lst, key=lambda t: (abs(t.offset_ms), t.offset_ms)) for src, lst in g["ticks"].items() if lst}
        sample = Sample(
            symbol=g["symbol"],
            round_end_ts=g["round_end_ts"],
            rtds_open=g["rtds_open"],
            rtds_close=g["rtds_close"],
            source_ticks=ticks,
        )
        k = (sample.symbol, sample.round_end_ts)
        prev = best.get(k)
        if prev is None:
            best[k] = sample
            continue
        score = sum(len(v) for v in sample.source_ticks.values()) + 100 * len(sample.source_ticks)
        prev_score = sum(len(v) for v in prev.source_ticks.values()) + 100 * len(prev.source_ticks)
        if score > prev_score:
            best[k] = sample
    return sorted(best.values(), key=lambda s: (s.round_end_ts, s.symbol))


def frange(start: float, end: float, step: float) -> Iterable[float]:
    x = start
    while x <= end + 1e-9:
        yield round(x, 6)
        x += step


def parse_int_list(spec: str) -> List[int]:
    return [int(x.strip()) for x in spec.split(",") if x.strip()]


def choose_price(ticks: Sequence[Tick], rule: str, pre_ms: int, post_ms: int) -> Optional[float]:
    in_window = [t for t in ticks if -pre_ms <= t.offset_ms <= post_ms]
    if not in_window:
        return None
    after = [t for t in in_window if t.offset_ms >= 0]
    before = [t for t in in_window if t.offset_ms <= 0]
    if rule == "first_after":
        return min(after, key=lambda t: (t.offset_ms, abs(t.offset_ms))).price if after else None
    if rule == "last_before":
        return max(before, key=lambda t: (t.offset_ms, -abs(t.offset_ms))).price if before else None
    if rule == "nearest_abs":
        return min(in_window, key=lambda t: (abs(t.offset_ms), t.offset_ms)).price
    if rule == "after_then_nearest":
        return min(after, key=lambda t: t.offset_ms).price if after else min(in_window, key=lambda t: (abs(t.offset_ms), t.offset_ms)).price
    if rule == "after_then_before":
        if after:
            return min(after, key=lambda t: t.offset_ms).price
        if before:
            return max(before, key=lambda t: t.offset_ms).price
        return None
    if rule == "before_then_after":
        if before:
            return max(before, key=lambda t: t.offset_ms).price
        if after:
            return min(after, key=lambda t: t.offset_ms).price
        return None
    if rule == "mid_cross":
        if after and before:
            a = min(after, key=lambda t: t.offset_ms).price
            b = max(before, key=lambda t: t.offset_ms).price
            return 0.5 * (a + b)
        return None
    if rule == "mean_window":
        return statistics.fmean(t.price for t in in_window)
    if rule == "median_window":
        return statistics.median(t.price for t in in_window)
    raise ValueError(f"unsupported rule: {rule}")


def aggregate(points: List[Tuple[str, float]], agg: str, weights: Dict[str, float]) -> Optional[float]:
    if not points:
        return None
    vals = [(price, float(weights.get(src, 1.0))) for src, price in points if math.isfinite(price) and price > 0]
    if not vals:
        return None
    if agg == "weighted_mean":
        numer = sum(px * w for px, w in vals)
        denom = sum(w for _, w in vals)
        return numer / denom if denom > 0 else None
    if agg == "weighted_median":
        pts = sorted(vals, key=lambda x: x[0])
        total = sum(w for _, w in pts)
        acc = 0.0
        for px, w in pts:
            acc += w
            if acc >= total / 2:
                return px
        return pts[-1][0]
    if agg == "mean":
        return statistics.fmean(px for px, _ in vals)
    if agg == "median":
        return statistics.median(px for px, _ in vals)
    raise ValueError(f"unsupported agg: {agg}")


def score(samples: Sequence[Sample], rule: str, agg: str, pre_ms: int, post_ms: int, weights: Dict[str, float], min_sources: int, allowed_sources: set[str]) -> Dict[str, float]:
    n = 0
    missing = 0
    side_errors = 0
    diffs = []
    for s in samples:
        pts = []
        for src, ticks in s.source_ticks.items():
            if src not in allowed_sources:
                continue
            px = choose_price(ticks, rule, pre_ms, post_ms)
            if px is not None:
                pts.append((src, px))
        if len(pts) < min_sources:
            missing += 1
            continue
        pred = aggregate(pts, agg, weights)
        if pred is None:
            missing += 1
            continue
        n += 1
        local_side = "Yes" if pred >= s.rtds_open else "No"
        rtds_side = "Yes" if s.rtds_close >= s.rtds_open else "No"
        if local_side != rtds_side:
            side_errors += 1
        diffs.append(abs(pred - s.rtds_close) / s.rtds_close * 10000.0)
    diffs.sort()
    def pct(q: float) -> float:
        if not diffs:
            return float("inf")
        idx = max(0, math.ceil(len(diffs) * q) - 1)
        return diffs[idx]
    return {
        "n": n,
        "missing": missing,
        "side_errors": side_errors,
        "mean_bps": statistics.fmean(diffs) if diffs else float("inf"),
        "p95_bps": pct(0.95),
        "p99_bps": pct(0.99),
    }




def build_source_subsets(mode: str) -> List[Tuple[str, set[str]]]:
    full = set(SOURCES)
    out: List[Tuple[str, set[str]]] = [("all", full)]
    if mode == "full_only":
        return out
    if mode == "drop_one":
        for src in SOURCES:
            out.append((f"drop_{src}", full - {src}))
        return out
    if mode == "drop_one_and_pairs":
        for src in SOURCES:
            out.append((f"drop_{src}", full - {src}))
        for i, a in enumerate(SOURCES):
            for b in SOURCES[i+1:]:
                out.append((f"drop_{a}_{b}", full - {a, b}))
        return out
    raise ValueError(f"unsupported subset mode: {mode}")

def split_rounds(samples: Sequence[Sample], test_rounds: int) -> Tuple[List[Sample], List[Sample]]:
    rounds = sorted({s.round_end_ts for s in samples})
    test_set = set(rounds[-test_rounds:])
    train = [s for s in samples if s.round_end_ts not in test_set]
    test = [s for s in samples if s.round_end_ts in test_set]
    return train, test


def main() -> int:
    ap = argparse.ArgumentParser(description="Search boundary-window close estimators against RTDS truth.")
    ap.add_argument("--boundary-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_boundary_dataset_close_only.csv")
    ap.add_argument("--symbols", default="")
    ap.add_argument("--test-rounds", type=int, default=6)
    ap.add_argument("--pre-ms-grid", default="250,500,1000,2000,5000")
    ap.add_argument("--post-ms-grid", default="250,500,1000,2000,5000")
    ap.add_argument("--rules", default="first_after,after_then_before,nearest_abs,last_before,mid_cross,mean_window,median_window")
    ap.add_argument("--aggregators", default="weighted_mean,weighted_median,mean,median")
    ap.add_argument("--min-sources-grid", default="1,2,3")
    ap.add_argument("--grid-start", type=float, default=0.5)
    ap.add_argument("--grid-end", type=float, default=1.5)
    ap.add_argument("--grid-step", type=float, default=0.5)
    ap.add_argument("--source-subset-mode", default="drop_one", choices=["full_only", "drop_one", "drop_one_and_pairs"])
    ap.add_argument("--top-k", type=int, default=80)
    ap.add_argument("--out-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_boundary_rule_search.csv")
    args = ap.parse_args()

    symbols = {x.strip().lower() for x in args.symbols.split(",") if x.strip()} or None
    samples = parse_csv(Path(args.boundary_csv), symbols)
    if not samples:
        print("No boundary samples.")
        return 1
    train, test = split_rounds(samples, args.test_rounds)
    weights_grid = list(frange(args.grid_start, args.grid_end, args.grid_step))
    pre_values = parse_int_list(args.pre_ms_grid)
    post_values = parse_int_list(args.post_ms_grid)
    min_sources_values = parse_int_list(args.min_sources_grid)
    rules = [x.strip() for x in args.rules.split(",") if x.strip()]
    aggs = [x.strip() for x in args.aggregators.split(",") if x.strip()]

    rows = []
    source_subsets = build_source_subsets(args.source_subset_mode)
    for wb in weights_grid:
        for wy in weights_grid:
            for wo in weights_grid:
                for wc in weights_grid:
                    for wh in weights_grid:
                        weights = {
                            "binance": wb,
                            "bybit": wy,
                            "okx": wo,
                            "coinbase": wc,
                            "hyperliquid": wh,
                        }
                        for pre_ms in pre_values:
                            for post_ms in post_values:
                                for min_sources in min_sources_values:
                                    for subset_name, allowed_sources in source_subsets:
                                        for rule in rules:
                                            for agg in aggs:
                                                train_m = score(train, rule, agg, pre_ms, post_ms, weights, min_sources, allowed_sources)
                                                test_m = score(test, rule, agg, pre_ms, post_ms, weights, min_sources, allowed_sources)
                                                rows.append({
                                                    "source_subset": subset_name,
                                                    "rule": rule,
                                                    "aggregator": agg,
                                                    "pre_ms": pre_ms,
                                                    "post_ms": post_ms,
                                                    "min_sources": min_sources,
                                                    "wb": wb,
                                                    "wy": wy,
                                                    "wo": wo,
                                                    "wc": wc,
                                                    "wh": wh,
                                                    "train_n": train_m["n"],
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
                                                })
    rows.sort(key=lambda r: (
        r["train_missing"], r["train_side_errors"], r["train_p99_bps"], r["train_p95_bps"], r["train_mean_bps"],
        r["test_missing"], r["test_side_errors"], r["test_p99_bps"], r["test_p95_bps"], r["test_mean_bps"],
    ))
    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["rule"])
        writer.writeheader()
        writer.writerows(rows[: args.top_k])
    print(f"samples={len(samples)} train={len(train)} test={len(test)} out_csv={out}")
    if rows:
        best = rows[0]
        print(
            f"best subset={best['source_subset']} rule={best['rule']} agg={best['aggregator']} pre_ms={best['pre_ms']} post_ms={best['post_ms']} min_sources={best['min_sources']} "
            f"train_side_errors={best['train_side_errors']} test_side_errors={best['test_side_errors']} train_mean_bps={best['train_mean_bps']:.6f} test_mean_bps={best['test_mean_bps']:.6f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
