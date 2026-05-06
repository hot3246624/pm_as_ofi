#!/usr/bin/env python3
import argparse
import csv
import itertools
import json
import math
import re
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple


KNOWN_SOURCES = ("binance", "bybit", "okx", "coinbase", "hyperliquid")


def parse_slug_anchor_ts(slug: str) -> Optional[int]:
    try:
        return int(slug.rsplit("-", 1)[1])
    except Exception:
        return None


def detect_interval_secs_from_slug(slug: str) -> Optional[int]:
    m = re.search(r"-updown-(\d+)([mh])(?:-|$)", slug)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    return n * 60 if unit == "m" else n * 3600 if unit == "h" else None


def parse_slug_round_end(slug: str) -> Optional[int]:
    anchor = parse_slug_anchor_ts(slug)
    if anchor is None:
        return None
    interval = detect_interval_secs_from_slug(slug)
    return anchor + interval if interval is not None else anchor


@dataclass
class CompareObs:
    slug: str
    symbol: str
    round_end_ts: int
    rtds_open: float
    rtds_close: float


@dataclass
class SourcePoint:
    source: str
    open_price: Optional[float]
    open_ts_ms: Optional[int]
    open_exact: bool
    close_price: float
    raw_close_price: float
    close_ts_ms: int
    close_exact: bool
    close_abs_delta_ms: int
    close_pick_kind: str


@dataclass
class RoundSourceClose:
    symbol: str
    round_end_ts: int
    unix_ms: int
    source_points: Dict[str, SourcePoint]


@dataclass
class ModelSpec:
    name: str
    fn: Callable[[RoundSourceClose], Optional[float]]


def parse_key_values_from_compare_line(line: str) -> Dict[str, str]:
    if "|" not in line:
        return {}
    payload = line.split("|", 1)[1].strip()
    out: Dict[str, str] = {}
    for token in payload.split():
        if "=" not in token:
            continue
        k, v = token.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def parse_compare_logs(paths: List[Path]) -> Dict[Tuple[str, int], CompareObs]:
    out: Dict[Tuple[str, int], CompareObs] = {}
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "local_price_agg_vs_rtds |" not in line:
                    continue
                if "local_price_agg_vs_rtds_unresolved" in line:
                    continue
                kv = parse_key_values_from_compare_line(line)
                slug = kv.get("slug", "")
                symbol = kv.get("symbol", "").lower()
                rtds_open_s = kv.get("rtds_open")
                rtds_close_s = kv.get("rtds_close")
                if not slug or not symbol or rtds_open_s is None or rtds_close_s is None:
                    continue
                round_end = parse_slug_round_end(slug)
                if round_end is None:
                    continue
                try:
                    rtds_open = float(rtds_open_s)
                    rtds_close = float(rtds_close_s)
                except Exception:
                    continue
                if not (math.isfinite(rtds_open) and math.isfinite(rtds_close) and rtds_open > 0 and rtds_close > 0):
                    continue
                out[(symbol, round_end)] = CompareObs(slug=slug, symbol=symbol, round_end_ts=round_end, rtds_open=rtds_open, rtds_close=rtds_close)
    return out


def parse_chainlink_alignment(path: Path) -> Dict[Tuple[str, int], CompareObs]:
    out: Dict[Tuple[str, int], CompareObs] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            symbol = str(obj.get("symbol", "")).lower()
            round_end = obj.get("round_end_ts")
            open_t = obj.get("open_t")
            close_t = obj.get("close_t")
            if (
                not symbol
                or not isinstance(round_end, int)
                or not isinstance(open_t, (int, float))
                or not isinstance(close_t, (int, float))
            ):
                continue
            o = float(open_t)
            c = float(close_t)
            if not (math.isfinite(o) and math.isfinite(c) and o > 0 and c > 0):
                continue
            slug = f"{symbol.split('/', 1)[0]}-updown-5m-{round_end}"
            out[(symbol, round_end)] = CompareObs(slug=slug, symbol=symbol, round_end_ts=round_end, rtds_open=o, rtds_close=c)
    return out


def parse_source_probe(path: Path, mode: str) -> Dict[Tuple[str, int], RoundSourceClose]:
    out: Dict[Tuple[str, int], RoundSourceClose] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("mode") != mode or obj.get("status") != "ready":
                continue
            symbol = str(obj.get("symbol", "")).lower()
            round_end = obj.get("round_end_ts")
            unix_ms = int(obj.get("unix_ms", 0))
            if not symbol or not isinstance(round_end, int):
                continue
            points: Dict[str, SourcePoint] = {}
            for sp in obj.get("source_points") or []:
                src = str(sp.get("source", "")).lower()
                if src not in KNOWN_SOURCES:
                    continue
                close_price = sp.get("close_price")
                close_ts_ms = sp.get("close_ts_ms")
                if not isinstance(close_price, (int, float)) or not isinstance(close_ts_ms, (int, float)):
                    continue
                px = float(close_price)
                ts_ms = int(close_ts_ms)
                if not (math.isfinite(px) and px > 0 and ts_ms > 0):
                    continue
                raw_close = sp.get("raw_close_price")
                if not isinstance(raw_close, (int, float)):
                    raw_close = px
                raw_close = float(raw_close)
                open_price = sp.get("open_price")
                if not isinstance(open_price, (int, float)):
                    open_price = None
                open_ts_ms = sp.get("open_ts_ms")
                if not isinstance(open_ts_ms, (int, float)):
                    open_ts_ms = None
                points[src] = SourcePoint(
                    source=src,
                    open_price=float(open_price) if open_price is not None else None,
                    open_ts_ms=int(open_ts_ms) if open_ts_ms is not None else None,
                    open_exact=bool(sp.get("open_exact", False)),
                    close_price=px,
                    raw_close_price=raw_close,
                    close_ts_ms=ts_ms,
                    close_exact=bool(sp.get("close_exact", False)),
                    close_abs_delta_ms=int(sp.get("close_abs_delta_ms") or abs(ts_ms - round_end * 1000)),
                    close_pick_kind=str(sp.get("close_pick_kind") or "unknown"),
                )
            if not points:
                continue
            key = (symbol, round_end)
            prev = out.get(key)
            if prev is None or unix_ms >= prev.unix_ms:
                out[key] = RoundSourceClose(symbol=symbol, round_end_ts=round_end, unix_ms=unix_ms, source_points=points)
    return out


def count_matching_fractional_digits(a: float, b: float, decimals: int = 15) -> int:
    sa = f"{a:.{decimals}f}".split(".", 1)[1]
    sb = f"{b:.{decimals}f}".split(".", 1)[1]
    c = 0
    for x, y in zip(sa, sb):
        if x != y:
            break
        c += 1
    return c


def percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        return float("inf")
    idx = max(0, math.ceil(len(sorted_vals) * q) - 1)
    return sorted_vals[idx]


def weighted_mean(values: Sequence[Tuple[float, float]]) -> Optional[float]:
    numer = 0.0
    denom = 0.0
    for px, w in values:
        if px > 0 and math.isfinite(px) and w > 0 and math.isfinite(w):
            numer += px * w
            denom += w
    if denom <= 0:
        return None
    return numer / denom


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


def simple_median(vals: Sequence[float]) -> Optional[float]:
    pts = sorted(v for v in vals if v > 0 and math.isfinite(v))
    if not pts:
        return None
    return statistics.median(pts)


def trimmed_mean(vals: Sequence[float], trim_each_side: int = 1) -> Optional[float]:
    pts = sorted(v for v in vals if v > 0 and math.isfinite(v))
    if not pts:
        return None
    if len(pts) <= trim_each_side * 2:
        return statistics.fmean(pts)
    pts = pts[trim_each_side: len(pts) - trim_each_side]
    return statistics.fmean(pts)


def temporal_weight(base: float, point: SourcePoint, decay_ms: float, exact_boost: float) -> float:
    temporal = 1.0 / (1.0 + (point.close_abs_delta_ms / decay_ms)) if decay_ms > 0 and math.isfinite(decay_ms) else 1.0
    exact = exact_boost if point.close_exact else 1.0
    return base * temporal * exact


def make_weighted_mean_model(name: str, source_weights: Dict[str, float], decay_ms: float, exact_boost: float, use_raw: bool) -> ModelSpec:
    def _fn(src: RoundSourceClose) -> Optional[float]:
        values = []
        for s, point in src.source_points.items():
            px = point.raw_close_price if use_raw else point.close_price
            w = temporal_weight(float(source_weights.get(s, 0.0)), point, decay_ms, exact_boost)
            values.append((px, w))
        return weighted_mean(values)
    return ModelSpec(name=name, fn=_fn)


def make_weighted_median_model(name: str, source_weights: Dict[str, float], decay_ms: float, exact_boost: float, use_raw: bool) -> ModelSpec:
    def _fn(src: RoundSourceClose) -> Optional[float]:
        values = []
        for s, point in src.source_points.items():
            px = point.raw_close_price if use_raw else point.close_price
            w = temporal_weight(float(source_weights.get(s, 0.0)), point, decay_ms, exact_boost)
            values.append((px, w))
        return weighted_median(values)
    return ModelSpec(name=name, fn=_fn)


def make_single_source_model(source: str, use_raw: bool) -> ModelSpec:
    def _fn(src: RoundSourceClose) -> Optional[float]:
        point = src.source_points.get(source)
        if point is None:
            return None
        return point.raw_close_price if use_raw else point.close_price
    kind = "raw" if use_raw else "adj"
    return ModelSpec(name=f"single_source:{source}:{kind}", fn=_fn)


def make_simple_models() -> List[ModelSpec]:
    models: List[ModelSpec] = []
    for use_raw in (False, True):
        kind = "raw" if use_raw else "adj"

        def _median(src: RoundSourceClose, use_raw=use_raw) -> Optional[float]:
            vals = [p.raw_close_price if use_raw else p.close_price for p in src.source_points.values()]
            return simple_median(vals)

        def _trimmed(src: RoundSourceClose, use_raw=use_raw) -> Optional[float]:
            vals = [p.raw_close_price if use_raw else p.close_price for p in src.source_points.values()]
            return trimmed_mean(vals, trim_each_side=1)

        def _closest(src: RoundSourceClose, use_raw=use_raw) -> Optional[float]:
            if not src.source_points:
                return None
            point = min(src.source_points.values(), key=lambda p: (p.close_abs_delta_ms, 0 if p.close_exact else 1, p.source))
            return point.raw_close_price if use_raw else point.close_price

        models.append(ModelSpec(name=f"median:{kind}", fn=_median))
        models.append(ModelSpec(name=f"trimmed_mean:{kind}", fn=_trimmed))
        models.append(ModelSpec(name=f"closest_delta:{kind}", fn=_closest))
        for source in KNOWN_SOURCES:
            models.append(make_single_source_model(source, use_raw))
    for reducer in ("mean", "median", "trimmed_mean"):
        models.append(make_return_anchor_model(f"return_anchor_ratio|{reducer}", use_ratio=True, reducer=reducer))
        models.append(make_return_anchor_model(f"return_anchor_delta|{reducer}", use_ratio=False, reducer=reducer))
    return models


def make_return_anchor_model(name: str, use_ratio: bool, reducer: str) -> ModelSpec:
    def _reduce(values: Sequence[float]) -> Optional[float]:
        if not values:
            return None
        if reducer == "median":
            return simple_median(values)
        if reducer == "trimmed_mean":
            return trimmed_mean(values, trim_each_side=1)
        if reducer == "mean":
            return statistics.fmean(values)
        return None

    def _fn(src: RoundSourceClose) -> Optional[float]:
        rets: List[float] = []
        for point in src.source_points.values():
            if point.open_price is None or not math.isfinite(point.open_price) or point.open_price <= 0:
                continue
            if use_ratio:
                rets.append(point.close_price / point.open_price)
            else:
                rets.append(point.close_price - point.open_price)
        return _reduce(rets)

    return ModelSpec(name=name, fn=_fn)


def materialize_return_anchor_close(obs: CompareObs, pred_stat: float, use_ratio: bool) -> Optional[float]:
    if pred_stat is None or not math.isfinite(pred_stat):
        return None
    if use_ratio:
        if pred_stat <= 0:
            return None
        return obs.rtds_open * pred_stat
    return obs.rtds_open + pred_stat


def score_model(dataset: List[Tuple[CompareObs, RoundSourceClose]], model: ModelSpec) -> Dict[str, float]:
    bps: List[float] = []
    side_hits = 0
    valid = 0
    d12_hits = 0
    for obs, src in dataset:
        pred = model.fn(src)
        if model.name.startswith("return_anchor_ratio|"):
            pred = materialize_return_anchor_close(obs, pred, use_ratio=True)
        elif model.name.startswith("return_anchor_delta|"):
            pred = materialize_return_anchor_close(obs, pred, use_ratio=False)
        if pred is None or obs.rtds_close <= 0 or not math.isfinite(pred) or pred <= 0:
            continue
        valid += 1
        bps_abs = abs(pred - obs.rtds_close) / obs.rtds_close * 10_000.0
        bps.append(bps_abs)
        pred_side_yes = pred >= obs.rtds_open
        true_side_yes = obs.rtds_close >= obs.rtds_open
        if pred_side_yes == true_side_yes:
            side_hits += 1
        if count_matching_fractional_digits(pred, obs.rtds_close, 15) >= 12:
            d12_hits += 1
    if valid == 0:
        return {
            "n": 0,
            "missing": len(dataset),
            "side_errors": float("inf"),
            "mean_bps": float("inf"),
            "p50_bps": float("inf"),
            "p95_bps": float("inf"),
            "p99_bps": float("inf"),
            "side_match": 0.0,
            "match_12dp": 0.0,
        }
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


def build_weighted_models(grid_values: Sequence[float], decay_values: Sequence[float], boost_values: Sequence[float]) -> Iterable[ModelSpec]:
    for decay, boost in itertools.product(decay_values, boost_values):
        for wb, wy, wo, wc, wh in itertools.product(
            grid_values, grid_values, grid_values, grid_values, grid_values
        ):
            weights = {
                "binance": wb,
                "bybit": wy,
                "okx": wo,
                "coinbase": wc,
                "hyperliquid": wh,
            }
            tag = f"wb={wb},wy={wy},wo={wo},wc={wc},wh={wh},decay={decay},boost={boost}"
            yield make_weighted_mean_model(f"weighted_mean_adj|{tag}", weights, decay, boost, use_raw=False)
            yield make_weighted_mean_model(f"weighted_mean_raw|{tag}", weights, decay, boost, use_raw=True)
            yield make_weighted_median_model(f"weighted_median_adj|{tag}", weights, decay, boost, use_raw=False)
            yield make_weighted_median_model(f"weighted_median_raw|{tag}", weights, decay, boost, use_raw=True)


def parse_float_list(raw: str) -> List[float]:
    return [float(tok.strip()) for tok in raw.split(",") if tok.strip()]


def frange(start: float, end: float, step: float) -> List[float]:
    out = []
    x = start
    while x <= end + 1e-12:
        out.append(round(x, 6))
        x += step
    return out


def write_round_csv(path: Path, dataset: List[Tuple[CompareObs, RoundSourceClose]], model: ModelSpec) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "slug", "symbol", "round_end_ts", "rtds_open", "rtds_close", "pred_close", "diff_bps", "pred_side", "true_side", "side_match",
            "src_binance", "src_bybit", "src_okx", "src_coinbase", "src_hyperliquid",
            "ts_binance", "ts_bybit", "ts_okx", "ts_coinbase", "ts_hyperliquid",
            "pick_binance", "pick_bybit", "pick_okx", "pick_coinbase", "pick_hyperliquid"
        ])
        for obs, src in sorted(dataset, key=lambda x: (x[0].round_end_ts, x[0].symbol)):
            pred = model.fn(src)
            if pred is None:
                continue
            diff_bps = abs(pred - obs.rtds_close) / obs.rtds_close * 10_000.0 if obs.rtds_close > 0 else float("inf")
            pred_side = "Yes" if pred >= obs.rtds_open else "No"
            true_side = "Yes" if obs.rtds_close >= obs.rtds_open else "No"
            def gp(name: str, attr: str):
                p = src.source_points.get(name)
                return getattr(p, attr) if p is not None else ""
            writer.writerow([
                obs.slug, obs.symbol, obs.round_end_ts, f"{obs.rtds_open:.15f}", f"{obs.rtds_close:.15f}", f"{pred:.15f}", f"{diff_bps:.6f}", pred_side, true_side, pred_side == true_side,
                gp("binance", "close_price"), gp("bybit", "close_price"), gp("okx", "close_price"), gp("coinbase", "close_price"), gp("hyperliquid", "close_price"),
                gp("binance", "close_ts_ms"), gp("bybit", "close_ts_ms"), gp("okx", "close_ts_ms"), gp("coinbase", "close_ts_ms"), gp("hyperliquid", "close_ts_ms"),
                gp("binance", "close_pick_kind"), gp("bybit", "close_pick_kind"), gp("okx", "close_pick_kind"), gp("coinbase", "close_pick_kind"), gp("hyperliquid", "close_pick_kind"),
            ])


def main() -> int:
    ap = argparse.ArgumentParser(description="Search local price aggregation model families against RTDS closes")
    ap.add_argument("--compare-log", action="append", default=[])
    ap.add_argument("--sources-jsonl", default="logs/local_price_agg_sources.jsonl")
    ap.add_argument("--chainlink-alignment-jsonl", default="logs/chainlink_round_alignment.jsonl")
    ap.add_argument("--mode", default="close_only", choices=["close_only", "full"])
    ap.add_argument("--grid-start", type=float, default=0.2)
    ap.add_argument("--grid-end", type=float, default=2.0)
    ap.add_argument("--grid-step", type=float, default=0.2)
    ap.add_argument("--close-decay-grid", default="500,900,1300")
    ap.add_argument("--exact-boost-grid", default="1.0,1.25,1.6")
    ap.add_argument("--top-k", type=int, default=40)
    ap.add_argument("--per-symbol-candidates", type=int, default=128)
    ap.add_argument("--out-csv", default="logs/localagg_model_search.csv")
    ap.add_argument("--out-best-round-csv", default="logs/localagg_model_best_rounds.csv")
    ap.add_argument("--out-best-per-symbol-csv", default="logs/localagg_model_best_per_symbol.csv")
    args = ap.parse_args()

    log_paths = [Path(p) for p in args.compare_log]
    compare_obs = parse_compare_logs(log_paths) if log_paths else {}
    if not compare_obs:
        compare_obs = parse_chainlink_alignment(Path(args.chainlink_alignment_jsonl))
    source_obs = parse_source_probe(Path(args.sources_jsonl), mode=args.mode)

    dataset: List[Tuple[CompareObs, RoundSourceClose]] = []
    for key, obs in compare_obs.items():
        src = source_obs.get(key)
        if src is not None:
            dataset.append((obs, src))
    if not dataset:
        print("No joined samples.")
        return 1

    models = make_simple_models()
    grid_values = frange(args.grid_start, args.grid_end, args.grid_step)
    decay_values = parse_float_list(args.close_decay_grid)
    boost_values = parse_float_list(args.exact_boost_grid)
    models.extend(list(build_weighted_models(grid_values, decay_values, boost_values)))

    rows = []
    for model in models:
        m = score_model(dataset, model)
        row = {"model": model.name, **m}
        rows.append((row, model))
    rows.sort(key=lambda item: (item[0]["missing"], item[0]["side_errors"], item[0]["p99_bps"], item[0]["p95_bps"], item[0]["mean_bps"], -item[0]["match_12dp"]))

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["model", "n", "missing", "side_errors", "mean_bps", "p50_bps", "p95_bps", "p99_bps", "side_match", "match_12dp"])
        writer.writeheader()
        for row, _ in rows[: max(args.top_k, 1)]:
            writer.writerow(row)

    best_row, best_model = rows[0]
    write_round_csv(Path(args.out_best_round_csv), dataset, best_model)

    symbols = sorted({obs.symbol for obs, _ in dataset})
    per_symbol_rows = []
    shortlist = [model for _, model in rows[: max(args.per_symbol_candidates, 1)]]
    for symbol in symbols:
        symbol_ds = [(obs, src) for obs, src in dataset if obs.symbol == symbol]
        local_rows = [(score_model(symbol_ds, model), model) for model in shortlist]
        local_rows.sort(key=lambda item: (item[0]["missing"], item[0]["side_errors"], item[0]["p99_bps"], item[0]["p95_bps"], item[0]["mean_bps"], -item[0]["match_12dp"]))
        row, model = local_rows[0]
        per_symbol_rows.append({"symbol": symbol, "model": model.name, **row})
    with Path(args.out_best_per_symbol_csv).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol", "model", "n", "missing", "side_errors", "mean_bps", "p50_bps", "p95_bps", "p99_bps", "side_match", "match_12dp"])
        writer.writeheader()
        writer.writerows(per_symbol_rows)

    print(f"Joined samples: {len(dataset)}")
    print(f"Best global model: {best_model.name}")
    print(
        f"side_errors={best_row['side_errors']} side_match={best_row['side_match']:.3f} "
        f"mean_bps={best_row['mean_bps']:.6f} p95_bps={best_row['p95_bps']:.6f} p99_bps={best_row['p99_bps']:.6f} match_12dp={best_row['match_12dp']:.3f}"
    )
    print(f"Top models CSV: {args.out_csv}")
    print(f"Best global per-round CSV: {args.out_best_round_csv}")
    print(f"Best per-symbol CSV: {args.out_best_per_symbol_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
