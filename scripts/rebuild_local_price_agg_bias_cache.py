#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

RESIDUAL_CLAMP_BPS = 25.0
BIAS_CLAMP_BPS = 10.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Rebuild local price agg bias cache from historical lab compare logs."
    )
    p.add_argument(
        "--logs-root",
        default="/Users/hot/web3Scientist/pm_as_ofi/logs",
        help="Root directory containing local-agg-closeonly-gated-lab* folders.",
    )
    p.add_argument(
        "--glob",
        default="local-agg-closeonly-gated-lab*",
        help="Directory glob under logs-root to scan.",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Output JSON path for local_price_agg_bias_cache.json format.",
    )
    return p.parse_args()


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def collect_compare_truth(log_path: Path) -> dict[tuple[str, int], float]:
    out: dict[tuple[str, int], float] = {}
    for line in log_path.open(errors="ignore"):
        if "local_price_agg_vs_rtds |" not in line:
            continue
        mslug = re.search(r"slug=(\S+)", line)
        msym = re.search(r"symbol=(\S+)", line)
        mclose = re.search(r"rtds_close=([0-9.]+)", line)
        if not (mslug and msym and mclose):
            continue
        try:
            round_start_ts = int(mslug.group(1).rsplit("-", 1)[1])
        except Exception:
            continue
        out[(msym.group(1), round_start_ts)] = float(mclose.group(1))
    return out


def main() -> int:
    args = parse_args()
    logs_root = Path(args.logs_root)
    all_residuals: dict[tuple[str, str], list[float]] = defaultdict(list)
    joined_rows = 0

    for lab_dir in sorted(p for p in logs_root.glob(args.glob) if p.is_dir()):
        src_file = lab_dir / "local_price_agg_sources.jsonl"
        if not src_file.exists():
            continue

        src_index: dict[tuple[str, int], dict] = {}
        for line in src_file.open():
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get("mode") == "close_only" and row.get("status") == "ready":
                src_index[(row.get("symbol"), row.get("round_start_ts"))] = row

        truth_index: dict[tuple[str, int], float] = {}
        for log_file in sorted(lab_dir.glob("local_agg_lab_*.log")):
            truth_index.update(collect_compare_truth(log_file))

        for key, truth in truth_index.items():
            row = src_index.get(key)
            if not row:
                continue
            symbol, _round_start_ts = key
            for sp in row.get("source_points", []):
                source = sp.get("source")
                raw_close = sp.get("raw_close_price")
                if not source or raw_close is None:
                    continue
                if not isinstance(raw_close, (int, float)) or raw_close <= 0.0:
                    continue
                residual_bps = ((truth - float(raw_close)) / abs(truth)) * 10_000.0
                residual_bps = clamp(
                    residual_bps, -RESIDUAL_CLAMP_BPS, RESIDUAL_CLAMP_BPS
                )
                all_residuals[(symbol, source)].append(residual_bps)
                joined_rows += 1

    entries = []
    for (symbol, source), residuals in sorted(all_residuals.items()):
        if not residuals:
            continue
        bias_bps = statistics.median(residuals)
        bias_bps = clamp(bias_bps, -BIAS_CLAMP_BPS, BIAS_CLAMP_BPS)
        entries.append(
            {
                "symbol": symbol,
                "source": source,
                "bias_bps": bias_bps,
                "samples": len(residuals),
                "median_residual_bps": statistics.median(residuals),
                "mean_residual_bps": sum(residuals) / len(residuals),
            }
        )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {"symbol": e["symbol"], "source": e["source"], "bias_bps": e["bias_bps"]}
        for e in entries
    ]
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

    print(f"joined_rows={joined_rows}")
    print(f"entries={len(entries)}")
    for e in entries:
        print(
            f"{e['symbol']:10s} {e['source']:12s} "
            f"samples={e['samples']:2d} bias_bps={e['bias_bps']:+.6f} "
            f"median={e['median_residual_bps']:+.6f} mean={e['mean_residual_bps']:+.6f}"
        )
    print(f"wrote={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
