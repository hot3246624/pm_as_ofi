#!/usr/bin/env python3
import argparse
import math
import re
from collections import defaultdict
from pathlib import Path


KV_RE = re.compile(r"(\w+)=([^ ]+)")
LINE_RE = re.compile(r"local_price_agg_vs_rtds \|")


def parse_slug_round_end_ms(slug: str) -> int | None:
    # slug pattern: <token>-updown-5m-<round_start_ts>
    parts = slug.rsplit("-", 1)
    if len(parts) != 2:
        return None
    try:
        round_start = int(parts[1])
    except ValueError:
        return None
    interval_secs = 300
    return (round_start + interval_secs) * 1000


def q(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    xs = sorted(values)
    i = (len(xs) - 1) * p
    lo = int(math.floor(i))
    hi = int(math.ceil(i))
    if lo == hi:
        return xs[lo]
    frac = i - lo
    return xs[lo] * (1 - frac) + xs[hi] * frac


def summarize(name: str, values: list[float]) -> str:
    if not values:
        return f"{name}: n=0"
    return (
        f"{name}: n={len(values)} min={min(values):.0f} "
        f"p50={q(values, 0.50):.0f} p90={q(values, 0.90):.0f} "
        f"p95={q(values, 0.95):.0f} max={max(values):.0f} mean={sum(values)/len(values):.1f}"
    )


def parse_args():
    ap = argparse.ArgumentParser(
        description="Summarize local aggregator lab metrics from polymarket logs."
    )
    ap.add_argument(
        "logs",
        nargs="+",
        help="One or more log files (e.g. logs/local_agg_lab_*.log)",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    rows = []
    unresolved = 0

    for p in args.logs:
        path = Path(p)
        if not path.exists():
            print(f"[warn] missing file: {path}")
            continue
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "local_price_agg_vs_rtds_unresolved" in line:
                    unresolved += 1
                    continue
                if not LINE_RE.search(line):
                    continue
                kv = dict(KV_RE.findall(line))
                slug = kv.get("slug", "")
                symbol = kv.get("symbol", "")
                if not slug or not symbol:
                    continue

                round_end_ms = parse_slug_round_end_ms(slug)
                ready_ms = int(kv.get("local_ready_ms", "0"))
                rtds_latency = float(kv.get("local_to_rtds_detect_gap_ms", "nan"))
                close_diff_bps = float(kv.get("close_diff_bps", "nan"))
                side_match = kv.get("side_match_vs_rtds_open", "false").lower() == "true"
                if round_end_ms is None or ready_ms <= 0:
                    continue
                local_latency = ready_ms - round_end_ms
                rows.append(
                    {
                        "symbol": symbol,
                        "slug": slug,
                        "local_latency_ms": local_latency,
                        "local_to_rtds_gap_ms": rtds_latency,
                        "close_diff_bps": close_diff_bps,
                        "side_match": side_match,
                    }
                )

    if not rows:
        print("No local_price_agg_vs_rtds rows found.")
        return

    local_lat = [r["local_latency_ms"] for r in rows]
    gap = [r["local_to_rtds_gap_ms"] for r in rows if not math.isnan(r["local_to_rtds_gap_ms"])]
    bps = [r["close_diff_bps"] for r in rows if not math.isnan(r["close_diff_bps"])]
    side_ok = sum(1 for r in rows if r["side_match"])

    print(summarize("local_latency_ms", local_lat))
    print(summarize("local_to_rtds_detect_gap_ms (positive=local earlier)", gap))
    print(summarize("close_diff_bps", bps))
    print(f"side_match_vs_rtds_open: {side_ok}/{len(rows)} ({(100.0*side_ok/len(rows)):.1f}%)")
    print(f"unresolved_rows: {unresolved}")
    print("")

    by_symbol = defaultdict(list)
    for r in rows:
        by_symbol[r["symbol"]].append(r)
    print("Per-symbol:")
    for symbol in sorted(by_symbol.keys()):
        sym_rows = by_symbol[symbol]
        sym_lat = [r["local_latency_ms"] for r in sym_rows]
        sym_bps = [r["close_diff_bps"] for r in sym_rows if not math.isnan(r["close_diff_bps"])]
        sym_side_ok = sum(1 for r in sym_rows if r["side_match"])
        print(
            f"  {symbol:<10} n={len(sym_rows):<3} "
            f"lat_p50={q(sym_lat,0.5):.0f}ms lat_p95={q(sym_lat,0.95):.0f}ms "
            f"bps_p50={q(sym_bps,0.5):.3f} bps_p95={q(sym_bps,0.95):.3f} "
            f"side_match={sym_side_ok}/{len(sym_rows)}"
        )


if __name__ == "__main__":
    main()

