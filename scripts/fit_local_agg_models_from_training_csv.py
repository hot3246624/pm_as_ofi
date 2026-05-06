#!/usr/bin/env python3
import argparse
import csv
import itertools
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple


SOURCES = ("binance", "bybit", "okx", "coinbase", "hyperliquid")


@dataclass
class Sample:
    instance_id: str
    symbol: str
    round_end_ts: int
    rtds_open: float
    rtds_close: float
    source_points: Dict[str, Dict[str, object]]


@dataclass
class ModelSpec:
    name: str
    fn: Callable[[Sample], Optional[float]]


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


def to_bool(raw: str) -> Optional[bool]:
    if raw in ("True", "true", "1"):
        return True
    if raw in ("False", "false", "0"):
        return False
    return None


def parse_training_csv(path: Path, instance_ids: Optional[set[str]], symbols: Optional[set[str]]) -> List[Sample]:
    rows = list(csv.DictReader(path.open()))
    out: List[Sample] = []
    for row in rows:
        if instance_ids and row["instance_id"] not in instance_ids:
            continue
        rtds_open = to_float(row["rtds_open"])
        rtds_close = to_float(row["rtds_close"])
        round_end_ts = to_int(row["round_end_ts"])
        symbol = row["symbol"]
        if symbols and symbol not in symbols:
            continue
        if not symbol or rtds_open is None or rtds_close is None or round_end_ts is None:
            continue
        source_points: Dict[str, Dict[str, object]] = {}
        for src in SOURCES:
            open_price = to_float(row.get(f"{src}_open_price", ""))
            open_ts_ms = to_int(row.get(f"{src}_open_ts_ms", ""))
            close_price = to_float(row.get(f"{src}_close_price", ""))
            raw_close_price = to_float(row.get(f"{src}_raw_close_price", ""))
            close_ts_ms = to_int(row.get(f"{src}_close_ts_ms", ""))
            if close_price is None and raw_close_price is None:
                continue
            source_points[src] = {
                "open_price": open_price,
                "open_ts_ms": open_ts_ms,
                "open_exact": to_bool(row.get(f"{src}_open_exact", "")),
                "close_price": close_price,
                "raw_close_price": raw_close_price if raw_close_price is not None else close_price,
                "close_ts_ms": close_ts_ms,
                "close_exact": to_bool(row.get(f"{src}_close_exact", "")),
                "close_abs_delta_ms": to_int(row.get(f"{src}_close_abs_delta_ms", "")),
                "close_pick_kind": row.get(f"{src}_close_pick_kind", "") or "",
            }
        if not source_points:
            continue
        out.append(
            Sample(
                instance_id=row["instance_id"],
                symbol=symbol,
                round_end_ts=round_end_ts,
                rtds_open=rtds_open,
                rtds_close=rtds_close,
                source_points=source_points,
            )
        )
    return out


def percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        return float("inf")
    idx = max(0, math.ceil(len(sorted_vals) * q) - 1)
    return sorted_vals[idx]


def count_matching_fractional_digits(a: float, b: float, decimals: int = 15) -> int:
    sa = f"{a:.{decimals}f}".split(".", 1)[1]
    sb = f"{b:.{decimals}f}".split(".", 1)[1]
    c = 0
    for x, y in zip(sa, sb):
        if x != y:
            break
        c += 1
    return c


def weighted_mean(values: Sequence[Tuple[float, float]]) -> Optional[float]:
    numer = 0.0
    denom = 0.0
    for px, w in values:
        if px > 0 and math.isfinite(px) and w > 0 and math.isfinite(w):
            numer += px * w
            denom += w
    return numer / denom if denom > 0 else None


def weighted_median(values: Sequence[Tuple[float, float]]) -> Optional[float]:
    pts = sorted((px, w) for px, w in values if px > 0 and math.isfinite(px) and w > 0 and math.isfinite(w))
    if not pts:
        return None
    total = sum(w for _, w in pts)
    acc = 0.0
    threshold = total / 2.0
    for px, w in pts:
        acc += w
        if acc >= threshold:
            return px
    return pts[-1][0]


def weighted_mean_any(values: Sequence[Tuple[float, float]]) -> Optional[float]:
    numer = 0.0
    denom = 0.0
    for value, w in values:
        if math.isfinite(value) and w > 0 and math.isfinite(w):
            numer += value * w
            denom += w
    return numer / denom if denom > 0 else None


def weighted_median_any(values: Sequence[Tuple[float, float]]) -> Optional[float]:
    pts = sorted((value, w) for value, w in values if math.isfinite(value) and w > 0 and math.isfinite(w))
    if not pts:
        return None
    total = sum(w for _, w in pts)
    acc = 0.0
    threshold = total / 2.0
    for value, w in pts:
        acc += w
        if acc >= threshold:
            return value
    return pts[-1][0]


def simple_median(vals: Sequence[float]) -> Optional[float]:
    pts = sorted(v for v in vals if v > 0 and math.isfinite(v))
    return statistics.median(pts) if pts else None


def trimmed_mean(vals: Sequence[float], trim_each_side: int = 1) -> Optional[float]:
    pts = sorted(v for v in vals if v > 0 and math.isfinite(v))
    if not pts:
        return None
    if len(pts) <= trim_each_side * 2:
        return statistics.fmean(pts)
    pts = pts[trim_each_side : len(pts) - trim_each_side]
    return statistics.fmean(pts)


def temporal_weight(base: float, point: Dict[str, object], decay_ms: float, exact_boost: float) -> float:
    delta = point.get("close_abs_delta_ms")
    delta_ms = int(delta) if isinstance(delta, int) or (isinstance(delta, float) and math.isfinite(delta)) else 0
    temporal = 1.0 / (1.0 + (delta_ms / decay_ms)) if decay_ms > 0 and math.isfinite(decay_ms) else 1.0
    exact = exact_boost if point.get("close_exact") is True else 1.0
    return base * temporal * exact


def make_weighted_mean_model(name: str, source_weights: Dict[str, float], decay_ms: float, exact_boost: float, use_raw: bool) -> ModelSpec:
    def _fn(sample: Sample) -> Optional[float]:
        values = []
        for src, point in sample.source_points.items():
            px = point["raw_close_price"] if use_raw else point["close_price"]
            if px is None:
                continue
            w = temporal_weight(float(source_weights.get(src, 0.0)), point, decay_ms, exact_boost)
            values.append((float(px), w))
        return weighted_mean(values)
    return ModelSpec(name=name, fn=_fn)


def make_weighted_median_model(name: str, source_weights: Dict[str, float], decay_ms: float, exact_boost: float, use_raw: bool) -> ModelSpec:
    def _fn(sample: Sample) -> Optional[float]:
        values = []
        for src, point in sample.source_points.items():
            px = point["raw_close_price"] if use_raw else point["close_price"]
            if px is None:
                continue
            w = temporal_weight(float(source_weights.get(src, 0.0)), point, decay_ms, exact_boost)
            values.append((float(px), w))
        return weighted_median(values)
    return ModelSpec(name=name, fn=_fn)


def make_single_source_model(source: str, use_raw: bool) -> ModelSpec:
    def _fn(sample: Sample) -> Optional[float]:
        point = sample.source_points.get(source)
        if point is None:
            return None
        px = point["raw_close_price"] if use_raw else point["close_price"]
        return float(px) if px is not None else None
    return ModelSpec(name=f"single_source:{source}:{'raw' if use_raw else 'adj'}", fn=_fn)


def make_single_source_return_anchor_model(source: str, mode: str, use_raw: bool) -> ModelSpec:
    def _fn(sample: Sample) -> Optional[float]:
        point = sample.source_points.get(source)
        if point is None:
            return None
        open_px = point.get("open_price")
        close_px = point["raw_close_price"] if use_raw else point["close_price"]
        if open_px is None or close_px is None:
            return None
        open_px = float(open_px)
        close_px = float(close_px)
        if open_px <= 0 or close_px <= 0:
            return None
        if mode == "ratio":
            return sample.rtds_open * (close_px / open_px)
        return sample.rtds_open + (close_px - open_px)

    return ModelSpec(name=f"single_source_return_anchor_{mode}:{source}:{'raw' if use_raw else 'adj'}", fn=_fn)


def make_simple_models() -> List[ModelSpec]:
    models: List[ModelSpec] = []
    for use_raw in (False, True):
        kind = "raw" if use_raw else "adj"

        def _median(sample: Sample, use_raw=use_raw) -> Optional[float]:
            vals = [float(p["raw_close_price"] if use_raw else p["close_price"]) for p in sample.source_points.values() if (p["raw_close_price"] if use_raw else p["close_price"]) is not None]
            return simple_median(vals)

        def _trimmed(sample: Sample, use_raw=use_raw) -> Optional[float]:
            vals = [float(p["raw_close_price"] if use_raw else p["close_price"]) for p in sample.source_points.values() if (p["raw_close_price"] if use_raw else p["close_price"]) is not None]
            return trimmed_mean(vals, trim_each_side=1)

        def _closest(sample: Sample, use_raw=use_raw) -> Optional[float]:
            points = [p for p in sample.source_points.values() if (p["raw_close_price"] if use_raw else p["close_price"]) is not None]
            if not points:
                return None
            point = min(points, key=lambda p: (int(p.get("close_abs_delta_ms") or 10**9), 0 if p.get("close_exact") is True else 1))
            px = point["raw_close_price"] if use_raw else point["close_price"]
            return float(px) if px is not None else None

        models.append(ModelSpec(name=f"median:{kind}", fn=_median))
        models.append(ModelSpec(name=f"trimmed_mean:{kind}", fn=_trimmed))
        models.append(ModelSpec(name=f"closest_delta:{kind}", fn=_closest))
        for source in SOURCES:
            models.append(make_single_source_model(source, use_raw))
            models.append(make_single_source_return_anchor_model(source, "ratio", use_raw))
            models.append(make_single_source_return_anchor_model(source, "delta", use_raw))
    return models


def build_weighted_models(grid_values: Sequence[float], decay_values: Sequence[float], boost_values: Sequence[float]) -> Iterable[ModelSpec]:
    for decay, boost in itertools.product(decay_values, boost_values):
        for wb, wy, wo, wc, wh in itertools.product(grid_values, grid_values, grid_values, grid_values, grid_values):
            weights = {"binance": wb, "bybit": wy, "okx": wo, "coinbase": wc, "hyperliquid": wh}
            tag = f"wb={wb},wy={wy},wo={wo},wc={wc},wh={wh},decay={decay},boost={boost}"
            yield make_weighted_mean_model(f"weighted_mean_adj|{tag}", weights, decay, boost, use_raw=False)
            yield make_weighted_mean_model(f"weighted_mean_raw|{tag}", weights, decay, boost, use_raw=True)
            yield make_weighted_median_model(f"weighted_median_adj|{tag}", weights, decay, boost, use_raw=False)
            yield make_weighted_median_model(f"weighted_median_raw|{tag}", weights, decay, boost, use_raw=True)


def make_return_anchor_model(
    name: str,
    source_weights: Dict[str, float],
    decay_ms: float,
    exact_boost: float,
    use_raw: bool,
    aggregate_kind: str,
    anchor_kind: str,
) -> ModelSpec:
    def _fn(sample: Sample) -> Optional[float]:
        values = []
        for src, point in sample.source_points.items():
            open_px = point.get("open_price")
            close_px = point["raw_close_price"] if use_raw else point["close_price"]
            if open_px is None or close_px is None:
                continue
            open_px = float(open_px)
            close_px = float(close_px)
            if open_px <= 0 or close_px <= 0:
                continue
            metric = (close_px / open_px) if anchor_kind == "ratio" else (close_px - open_px)
            w = temporal_weight(float(source_weights.get(src, 0.0)), point, decay_ms, exact_boost)
            values.append((metric, w))
        if not values:
            return None
        if aggregate_kind == "weighted_median":
            agg = weighted_median(values) if anchor_kind == "ratio" else weighted_median_any(values)
        else:
            agg = weighted_mean(values) if anchor_kind == "ratio" else weighted_mean_any(values)
        if agg is None:
            return None
        if anchor_kind == "ratio":
            return sample.rtds_open * agg
        return sample.rtds_open + agg

    return ModelSpec(name=name, fn=_fn)


def build_return_anchor_models(grid_values: Sequence[float], decay_values: Sequence[float], boost_values: Sequence[float]) -> Iterable[ModelSpec]:
    for decay, boost in itertools.product(decay_values, boost_values):
        for wb, wy, wo, wc, wh in itertools.product(grid_values, grid_values, grid_values, grid_values, grid_values):
            weights = {"binance": wb, "bybit": wy, "okx": wo, "coinbase": wc, "hyperliquid": wh}
            tag = f"wb={wb},wy={wy},wo={wo},wc={wc},wh={wh},decay={decay},boost={boost}"
            for use_raw in (False, True):
                kind = "raw" if use_raw else "adj"
                yield make_return_anchor_model(
                    f"return_anchor_ratio_weighted_mean_{kind}|{tag}",
                    weights,
                    decay,
                    boost,
                    use_raw,
                    "weighted_mean",
                    "ratio",
                )
                yield make_return_anchor_model(
                    f"return_anchor_delta_weighted_mean_{kind}|{tag}",
                    weights,
                    decay,
                    boost,
                    use_raw,
                    "weighted_mean",
                    "delta",
                )
                yield make_return_anchor_model(
                    f"return_anchor_ratio_weighted_median_{kind}|{tag}",
                    weights,
                    decay,
                    boost,
                    use_raw,
                    "weighted_median",
                    "ratio",
                )
                yield make_return_anchor_model(
                    f"return_anchor_delta_weighted_median_{kind}|{tag}",
                    weights,
                    decay,
                    boost,
                    use_raw,
                    "weighted_median",
                    "delta",
                )


def parse_float_list(raw: str) -> List[float]:
    return [float(tok.strip()) for tok in raw.split(",") if tok.strip()]


def frange(start: float, end: float, step: float) -> List[float]:
    out = []
    x = start
    while x <= end + 1e-12:
        out.append(round(x, 6))
        x += step
    return out


def score_model(dataset: List[Sample], model: ModelSpec) -> Dict[str, float]:
    bps: List[float] = []
    side_hits = 0
    valid = 0
    d12_hits = 0
    for sample in dataset:
        pred = model.fn(sample)
        if pred is None or not math.isfinite(pred) or pred <= 0:
            continue
        valid += 1
        bps_abs = abs(pred - sample.rtds_close) / sample.rtds_close * 10_000.0
        bps.append(bps_abs)
        pred_side_yes = pred >= sample.rtds_open
        true_side_yes = sample.rtds_close >= sample.rtds_open
        if pred_side_yes == true_side_yes:
            side_hits += 1
        if count_matching_fractional_digits(pred, sample.rtds_close, 15) >= 12:
            d12_hits += 1
    if valid == 0:
        return {"n": 0, "missing": len(dataset), "side_errors": float("inf"), "mean_bps": float("inf"), "p50_bps": float("inf"), "p95_bps": float("inf"), "p99_bps": float("inf"), "side_match": 0.0, "match_12dp": 0.0}
    bps_sorted = sorted(bps)
    return {
        "n": valid,
        "missing": len(dataset) - valid,
        "side_errors": valid - side_hits,
        "mean_bps": statistics.fmean(bps),
        "p50_bps": statistics.median(bps),
        "p95_bps": percentile(bps_sorted, 0.95),
        "p99_bps": percentile(bps_sorted, 0.99),
        "side_match": side_hits / valid,
        "match_12dp": d12_hits / valid,
    }


def split_train_test(samples: List[Sample], test_rounds: int) -> Tuple[List[Sample], List[Sample]]:
    round_keys = sorted({s.round_end_ts for s in samples})
    if len(round_keys) <= test_rounds:
        return samples, samples
    test_set = set(round_keys[-test_rounds:])
    train = [s for s in samples if s.round_end_ts not in test_set]
    test = [s for s in samples if s.round_end_ts in test_set]
    return train, test


def main() -> int:
    ap = argparse.ArgumentParser(description="Fit local close estimators from unified training CSV.")
    ap.add_argument("--training-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_training_canonical_rounds.csv")
    ap.add_argument("--instance-ids", default="", help="Comma-separated instance ids to include. Default: all")
    ap.add_argument("--symbols", default="", help="Comma-separated symbol list, e.g. btc/usd,eth/usd. Default: all")
    ap.add_argument("--test-rounds", type=int, default=6)
    ap.add_argument("--grid-start", type=float, default=0.2)
    ap.add_argument("--grid-end", type=float, default=2.0)
    ap.add_argument("--grid-step", type=float, default=0.2)
    ap.add_argument("--close-decay-grid", default="500,900,1300")
    ap.add_argument("--exact-boost-grid", default="1.0,1.25,1.6")
    ap.add_argument("--top-k", type=int, default=20)
    ap.add_argument("--out-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_train_test_search.csv")
    args = ap.parse_args()

    instance_ids = {x.strip() for x in args.instance_ids.split(",") if x.strip()} or None
    symbols = {x.strip().lower() for x in args.symbols.split(",") if x.strip()} or None
    samples = parse_training_csv(Path(args.training_csv), instance_ids, symbols)
    if not samples:
        print("No samples.")
        return 1
    train, test = split_train_test(samples, args.test_rounds)
    models = make_simple_models()
    grid_values = frange(args.grid_start, args.grid_end, args.grid_step)
    decay_values = parse_float_list(args.close_decay_grid)
    boost_values = parse_float_list(args.exact_boost_grid)
    models.extend(list(build_weighted_models(grid_values, decay_values, boost_values)))
    models.extend(list(build_return_anchor_models(grid_values, decay_values, boost_values)))

    rows = []
    for model in models:
        train_m = score_model(train, model)
        test_m = score_model(test, model)
        rows.append({
            "model": model.name,
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
            "test_side_match": test_m["side_match"],
            "test_match_12dp": test_m["match_12dp"],
        })
    rows.sort(key=lambda r: (r["train_missing"], r["train_side_errors"], r["train_p99_bps"], r["train_p95_bps"], r["train_mean_bps"], r["test_side_errors"], r["test_p99_bps"], r["test_mean_bps"]))

    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows[: max(args.top_k, 1)])

    best = rows[0]
    print(f"samples={len(samples)} train={len(train)} test={len(test)}")
    print(f"best_model={best['model']}")
    print(
        f"train_side_errors={best['train_side_errors']} train_mean_bps={best['train_mean_bps']:.6f} "
        f"test_side_errors={best['test_side_errors']} test_mean_bps={best['test_mean_bps']:.6f} test_p99_bps={best['test_p99_bps']:.6f}"
    )
    print(f"out_csv={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
