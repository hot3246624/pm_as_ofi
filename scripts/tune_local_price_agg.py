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
from typing import Dict, List, Optional, Tuple


def parse_slug_anchor_ts(slug: str) -> Optional[int]:
    try:
        return int(slug.rsplit('-', 1)[1])
    except Exception:
        return None


def detect_interval_secs_from_slug(slug: str) -> Optional[int]:
    m = re.search(r"-updown-(\d+)([mh])(?:-|$)", slug)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "m":
        return n * 60
    if unit == "h":
        return n * 3600
    return None


def parse_slug_round_end(slug: str) -> Optional[int]:
    """
    In this codebase, market slug suffix is round_start_ts for updown markets.
    Convert it to round_end_ts = round_start_ts + interval.
    """
    anchor = parse_slug_anchor_ts(slug)
    if anchor is None:
        return None
    interval = detect_interval_secs_from_slug(slug)
    if interval is None:
        # Fallback for unknown slug patterns: keep historical behavior.
        return anchor
    return anchor + interval


def parse_key_values_from_compare_line(line: str) -> Dict[str, str]:
    # line sample: ... local_price_agg_vs_rtds | slug=... symbol=... rtds_open=... rtds_close=...
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


@dataclass
class CompareObs:
    slug: str
    symbol: str
    round_end_ts: int
    rtds_open: float
    rtds_close: float


@dataclass
class RoundSourceClose:
    symbol: str
    round_end_ts: int
    unix_ms: int
    source_points: Dict[str, Tuple[float, int, bool]]


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
                key = (symbol, round_end)
                out[key] = CompareObs(
                    slug=slug,
                    symbol=symbol,
                    round_end_ts=round_end,
                    rtds_open=rtds_open,
                    rtds_close=rtds_close,
                )
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
            out[(symbol, round_end)] = CompareObs(
                slug=slug,
                symbol=symbol,
                round_end_ts=round_end,
                rtds_open=o,
                rtds_close=c,
            )
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
            if obj.get("mode") != mode:
                continue
            if obj.get("status") != "ready":
                continue
            symbol = str(obj.get("symbol", "")).lower()
            round_end = obj.get("round_end_ts")
            unix_ms = int(obj.get("unix_ms", 0))
            if not symbol or not isinstance(round_end, int):
                continue
            source_points = obj.get("source_points") or []
            points: Dict[str, Tuple[float, int, bool]] = {}
            for sp in source_points:
                src = str(sp.get("source", "")).lower()
                close_price = sp.get("close_price")
                close_ts_ms = sp.get("close_ts_ms")
                close_exact = bool(sp.get("close_exact", False))
                if src in ("binance", "bybit", "okx", "coinbase") and isinstance(close_price, (int, float)):
                    px = float(close_price)
                    if (
                        px > 0
                        and math.isfinite(px)
                        and isinstance(close_ts_ms, (int, float))
                        and float(close_ts_ms) > 0
                    ):
                        points[src] = (px, int(close_ts_ms), close_exact)
            if not points:
                continue
            key = (symbol, round_end)
            prev = out.get(key)
            # keep latest ready snapshot per round
            if prev is None or unix_ms >= prev.unix_ms:
                out[key] = RoundSourceClose(
                    symbol=symbol,
                    round_end_ts=round_end,
                    unix_ms=unix_ms,
                    source_points=points,
                )
    return out


def weighted_close(
    source_points: Dict[str, Tuple[float, int, bool]],
    round_end_ts: int,
    weights: Dict[str, float],
    close_decay_ms: float,
    exact_boost: float,
) -> Optional[float]:
    numer = 0.0
    denom = 0.0
    end_ms = round_end_ts * 1000
    for src, point in source_points.items():
        px, close_ts_ms, close_exact = point
        w = float(weights.get(src, 0.0))
        if w <= 0 or not math.isfinite(w):
            continue
        if not math.isfinite(px) or px <= 0:
            continue
        delta_ms = abs(close_ts_ms - end_ms)
        if close_decay_ms > 0 and math.isfinite(close_decay_ms):
            temporal = 1.0 / (1.0 + (delta_ms / close_decay_ms))
        else:
            temporal = 1.0
        exact = exact_boost if close_exact else 1.0
        eff_w = w * temporal * exact
        if eff_w <= 0 or not math.isfinite(eff_w):
            continue
        numer += px * eff_w
        denom += eff_w
    if denom <= 0:
        return None
    return numer / denom


def count_matching_fractional_digits(a: float, b: float, decimals: int = 15) -> int:
    sa = f"{a:.{decimals}f}".split(".", 1)[1]
    sb = f"{b:.{decimals}f}".split(".", 1)[1]
    c = 0
    for x, y in zip(sa, sb):
        if x != y:
            break
        c += 1
    return c


def round_metrics(
    dataset: List[Tuple[CompareObs, RoundSourceClose]],
    weights: Dict[str, float],
    close_decay_ms: float,
    exact_boost: float,
) -> Dict[str, float]:
    bps: List[float] = []
    side_hits = 0
    valid = 0
    d12_hits = 0
    for obs, src in dataset:
        pred = weighted_close(src.source_points, obs.round_end_ts, weights, close_decay_ms, exact_boost)
        if pred is None or obs.rtds_close <= 0:
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
        "p95_bps": bps_sorted[max(0, math.ceil(0.95 * len(bps_sorted)) - 1)],
        "p99_bps": bps_sorted[max(0, math.ceil(0.99 * len(bps_sorted)) - 1)],
        "side_match": side_hits / valid,
        "match_12dp": d12_hits / valid,
    }


def frange(start: float, end: float, step: float) -> List[float]:
    out = []
    x = start
    while x <= end + 1e-12:
        out.append(round(x, 6))
        x += step
    return out


def parse_float_list(raw: str) -> List[float]:
    out: List[float] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(float(tok))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Tune local price agg source weights against RTDS closes")
    ap.add_argument("--compare-log", action="append", default=[], help="Path to log file containing local_price_agg_vs_rtds lines (repeatable)")
    ap.add_argument("--sources-jsonl", default="logs/local_price_agg_sources.jsonl", help="Path to local source probe jsonl")
    ap.add_argument("--chainlink-alignment-jsonl", default="logs/chainlink_round_alignment.jsonl", help="Fallback truth source when compare logs are absent")
    ap.add_argument("--mode", default="close_only", choices=["close_only", "full"], help="Probe mode to use")
    ap.add_argument("--grid-start", type=float, default=0.4)
    ap.add_argument("--grid-end", type=float, default=2.0)
    ap.add_argument("--grid-step", type=float, default=0.1)
    ap.add_argument("--close-decay-ms", type=float, default=900.0)
    ap.add_argument("--exact-boost", type=float, default=1.25)
    ap.add_argument("--close-decay-grid", default="", help="Comma-separated decay candidates, e.g. 500,900,1300")
    ap.add_argument("--exact-boost-grid", default="", help="Comma-separated exact boost candidates, e.g. 1.0,1.25,1.6")
    ap.add_argument("--top-k", type=int, default=20)
    ap.add_argument("--out-csv", default="logs/localagg_weight_search.csv")
    ap.add_argument("--out-round-csv", default="logs/localagg_best_per_round.csv")
    args = ap.parse_args()

    log_paths = [Path(p) for p in args.compare_log]
    if not log_paths:
        # fallback to common recent logs
        log_paths = sorted(Path("logs").glob("dryrun-localagg-*.log"))

    compare_obs = parse_compare_logs(log_paths)
    if not compare_obs:
        compare_obs = parse_chainlink_alignment(Path(args.chainlink_alignment_jsonl))
    source_obs = parse_source_probe(Path(args.sources_jsonl), mode=args.mode)

    joined: List[Tuple[CompareObs, RoundSourceClose]] = []
    exact_hits = 0
    offset_hits = 0

    by_symbol: Dict[str, List[RoundSourceClose]] = {}
    for src in source_obs.values():
        by_symbol.setdefault(src.symbol, []).append(src)
    for items in by_symbol.values():
        items.sort(key=lambda x: x.round_end_ts)

    for key, obs in compare_obs.items():
        src = source_obs.get(key)
        if src is not None:
            joined.append((obs, src))
            exact_hits += 1
            continue

        # Safety fallback: allow nearest same-symbol round if exact key is absent.
        # This should normally stay at 0 after slug round_end normalization above.
        cands = by_symbol.get(obs.symbol, [])
        best = None
        best_diff = None
        for cand in cands:
            diff = abs(cand.round_end_ts - obs.round_end_ts)
            if best_diff is None or diff < best_diff:
                best = cand
                best_diff = diff
        if best is not None and best_diff is not None and best_diff <= 300:
            joined.append((obs, best))
            offset_hits += 1

    if not joined:
        print("No joined samples. Need both compare logs and local_price_agg_sources.jsonl ready rows.")
        return 1

    values = frange(args.grid_start, args.grid_end, args.grid_step)
    decay_values = (
        parse_float_list(args.close_decay_grid)
        if args.close_decay_grid.strip()
        else [float(args.close_decay_ms)]
    )
    boost_values = (
        parse_float_list(args.exact_boost_grid)
        if args.exact_boost_grid.strip()
        else [float(args.exact_boost)]
    )
    rows = []
    for decay, boost in itertools.product(decay_values, boost_values):
        for wb, wy, wo, wc in itertools.product(values, values, values, values):
            weights = {"binance": wb, "bybit": wy, "okx": wo, "coinbase": wc}
            m = round_metrics(
                joined,
                weights,
                close_decay_ms=decay,
                exact_boost=boost,
            )
            rows.append(
                {
                    "close_decay_ms": decay,
                    "exact_boost": boost,
                    "w_binance": wb,
                    "w_bybit": wy,
                    "w_okx": wo,
                    "w_coinbase": wc,
                    **m,
                }
            )

    # For this project, direction correctness is the hard constraint.
    # We sort lexicographically: first eliminate side flips, then reduce worst-case price error.
    rows.sort(
        key=lambda r: (
            r["missing"],
            r["side_errors"],
            r["p99_bps"],
            r["p95_bps"],
            r["mean_bps"],
            -r["match_12dp"],
        )
    )

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "close_decay_ms",
                "exact_boost",
                "w_binance",
                "w_bybit",
                "w_okx",
                "w_coinbase",
                "n",
                "missing",
                "side_errors",
                "mean_bps",
                "p50_bps",
                "p95_bps",
                "p99_bps",
                "side_match",
                "match_12dp",
            ],
        )
        writer.writeheader()
        for r in rows[: max(args.top_k, 1)]:
            writer.writerow(r)

    best = rows[0]
    best_weights = {
        "binance": float(best["w_binance"]),
        "bybit": float(best["w_bybit"]),
        "okx": float(best["w_okx"]),
        "coinbase": float(best["w_coinbase"]),
    }
    best_decay = float(best["close_decay_ms"])
    best_boost = float(best["exact_boost"])

    out_round = Path(args.out_round_csv)
    out_round.parent.mkdir(parents=True, exist_ok=True)
    with out_round.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "slug",
                "symbol",
                "round_end_ts",
                "rtds_open",
                "rtds_close",
                "pred_close",
                "abs_diff",
                "diff_bps",
                "match_frac_digits",
                "match_12dp",
                "pred_side",
                "true_side",
                "side_match",
                "src_binance",
                "src_bybit",
                "src_okx",
                "src_coinbase",
            ]
        )
        for obs, src in sorted(joined, key=lambda x: (x[0].round_end_ts, x[0].symbol)):
            pred = weighted_close(
                src.source_points,
                obs.round_end_ts,
                best_weights,
                close_decay_ms=best_decay,
                exact_boost=best_boost,
            )
            if pred is None:
                continue
            abs_diff = abs(pred - obs.rtds_close)
            bps = abs_diff / obs.rtds_close * 10_000.0 if obs.rtds_close > 0 else float("inf")
            pred_side = "Yes" if pred >= obs.rtds_open else "No"
            true_side = "Yes" if obs.rtds_close >= obs.rtds_open else "No"
            mfd = count_matching_fractional_digits(pred, obs.rtds_close, 15)
            writer.writerow(
                [
                    obs.slug,
                    obs.symbol,
                    obs.round_end_ts,
                    f"{obs.rtds_open:.15f}",
                    f"{obs.rtds_close:.15f}",
                    f"{pred:.15f}",
                    f"{abs_diff:.15f}",
                    f"{bps:.6f}",
                    mfd,
                    mfd >= 12,
                    pred_side,
                    true_side,
                    pred_side == true_side,
                    (src.source_points.get("binance") or ("", "", ""))[0],
                    (src.source_points.get("bybit") or ("", "", ""))[0],
                    (src.source_points.get("okx") or ("", "", ""))[0],
                    (src.source_points.get("coinbase") or ("", "", ""))[0],
                ]
            )

    print(f"Joined samples: {len(joined)} (exact={exact_hits}, offset<=300s={offset_hits})")
    print(
        "Best weights => "
        f"binance={best['w_binance']}, bybit={best['w_bybit']}, okx={best['w_okx']}, coinbase={best['w_coinbase']}, "
        f"close_decay_ms={best['close_decay_ms']}, exact_boost={best['exact_boost']} | "
        f"side_errors={best['side_errors']}, side_match={best['side_match']:.3f}, "
        f"mean_bps={best['mean_bps']:.6f}, p95_bps={best['p95_bps']:.6f}, p99_bps={best['p99_bps']:.6f}, "
        f"match_12dp={best['match_12dp']:.3f}"
    )
    print(f"Top grid CSV: {out_csv}")
    print(f"Per-round CSV: {out_round}")
    print(
        "Suggested env:\n"
        f"PM_LOCAL_PRICE_AGG_WEIGHT_BINANCE={best['w_binance']}\n"
        f"PM_LOCAL_PRICE_AGG_WEIGHT_BYBIT={best['w_bybit']}\n"
        f"PM_LOCAL_PRICE_AGG_WEIGHT_OKX={best['w_okx']}\n"
        f"PM_LOCAL_PRICE_AGG_WEIGHT_COINBASE={best['w_coinbase']}\n"
        f"PM_LOCAL_PRICE_AGG_CLOSE_TIME_DECAY_MS={best['close_decay_ms']}\n"
        f"PM_LOCAL_PRICE_AGG_EXACT_BOOST={best['exact_boost']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
