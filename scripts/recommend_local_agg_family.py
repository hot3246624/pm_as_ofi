#!/usr/bin/env python3
import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_LOGS = Path("/Users/hot/web3Scientist/pm_as_ofi/logs")
SYMBOLS = ["bnb/usd", "btc/usd", "doge/usd", "eth/usd", "hype/usd", "sol/usd", "xrp/usd"]


@dataclass
class FamilySummary:
    family: str
    windows: int
    total_test_side_errors: int
    total_test_missing: int
    mean_test_mean_bps: float
    max_test_p99_bps: float


def parse_stdout_summary(line: str) -> FamilySummary:
    parts = dict(tok.split("=", 1) for tok in line.strip().split() if "=" in tok)
    return FamilySummary(
        family=parts["family"],
        windows=int(parts["windows"]),
        total_test_side_errors=int(parts["total_test_side_errors"]),
        total_test_missing=int(parts["total_test_missing"]),
        mean_test_mean_bps=float(parts["mean_test_mean_bps"]),
        max_test_p99_bps=float(parts["max_test_p99_bps"]),
    )


def summarize_walkforward_csv(path: Path, family: str) -> FamilySummary:
    rows = list(csv.DictReader(path.open()))
    windows = len(rows)
    if windows == 0:
        return FamilySummary(family, 0, 10**9, 10**9, float("inf"), float("inf"))
    return FamilySummary(
        family=family,
        windows=windows,
        total_test_side_errors=sum(int(r["test_side_errors"]) for r in rows),
        total_test_missing=sum(int(r["test_missing"]) for r in rows),
        mean_test_mean_bps=sum(float(r["test_mean_bps"]) for r in rows) / windows,
        max_test_p99_bps=max(float(r["test_p99_bps"]) for r in rows),
    )


def pick_better(a: FamilySummary, b: FamilySummary, min_windows_for_switch: int) -> Tuple[str, str]:
    # reasoned recommendation:
    # 1. fewer direction errors
    # 2. fewer missing
    # 3. enough windows for challenger to be trusted
    # 4. lower mean bps
    key_a = (a.total_test_side_errors, a.total_test_missing)
    key_b = (b.total_test_side_errors, b.total_test_missing)
    if key_a < key_b:
        return a.family, f"lower risk ({a.total_test_side_errors}<{b.total_test_side_errors} side_errors)"
    if key_b < key_a:
        return b.family, f"lower risk ({b.total_test_side_errors}<{a.total_test_side_errors} side_errors)"

    # same risk profile
    if a.family == "return_anchor" and a.windows < min_windows_for_switch:
        return b.family, f"insufficient windows for return_anchor ({a.windows}<{min_windows_for_switch})"
    if b.family == "return_anchor" and b.windows < min_windows_for_switch:
        return a.family, f"insufficient windows for return_anchor ({b.windows}<{min_windows_for_switch})"

    if a.mean_test_mean_bps <= b.mean_test_mean_bps:
        return a.family, f"same risk, lower mean_bps ({a.mean_test_mean_bps:.6f}<={b.mean_test_mean_bps:.6f})"
    return b.family, f"same risk, lower mean_bps ({b.mean_test_mean_bps:.6f}<{a.mean_test_mean_bps:.6f})"


def fmt_summary(s: FamilySummary) -> str:
    return (
        f"windows={s.windows};side_errors={s.total_test_side_errors};missing={s.total_test_missing};"
        f"mean_bps={s.mean_test_mean_bps:.6f};max_p99={s.max_test_p99_bps:.6f}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate local aggregator family recommendation table from walk-forward CSVs.")
    ap.add_argument("--logs-root", default=str(DEFAULT_LOGS))
    ap.add_argument("--min-windows-for-switch", type=int, default=8)
    ap.add_argument("--out-csv", default="/Users/hot/web3Scientist/pm_as_ofi/logs/local_agg_family_recommendation.csv")
    args = ap.parse_args()

    logs_root = Path(args.logs_root)
    rows: List[Dict[str, str]] = []

    global_close = summarize_walkforward_csv(logs_root / "local_agg_walkforward_full_close_only_rerun.csv", "close_only")
    global_ra = summarize_walkforward_csv(logs_root / "local_agg_walkforward_full_return_anchor_rerun.csv", "return_anchor")
    chosen, reason = pick_better(global_close, global_ra, args.min_windows_for_switch)
    rows.append(
        {
            "scope": "global",
            "symbol": "*",
            "recommended_family": chosen,
            "challenger_family": "return_anchor" if chosen == "close_only" else "close_only",
            "reason": reason,
            "close_only_summary": fmt_summary(global_close),
            "return_anchor_summary": fmt_summary(global_ra),
        }
    )

    for symbol in SYMBOLS:
        stem = symbol.replace("/", "_")
        close_s = summarize_walkforward_csv(logs_root / f"wf_rerun_{stem}_close_only.csv", "close_only")
        ra_s = summarize_walkforward_csv(logs_root / f"wf_rerun_{stem}_return_anchor.csv", "return_anchor")
        chosen, reason = pick_better(close_s, ra_s, args.min_windows_for_switch)
        rows.append(
            {
                "scope": "symbol",
                "symbol": symbol,
                "recommended_family": chosen,
                "challenger_family": "return_anchor" if chosen == "close_only" else "close_only",
                "reason": reason,
                "close_only_summary": fmt_summary(close_s),
                "return_anchor_summary": fmt_summary(ra_s),
            }
        )

    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "scope",
                "symbol",
                "recommended_family",
                "challenger_family",
                "reason",
                "close_only_summary",
                "return_anchor_summary",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"out_csv={out}")
    for row in rows:
        print(
            f"{row['scope']} {row['symbol']}: recommended={row['recommended_family']} "
            f"challenger={row['challenger_family']} reason={row['reason']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
