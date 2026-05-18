#!/usr/bin/env python3
"""Fast CSV-cache parameter sweep for xuan pair-quality v3.

This intentionally reads only candidate-cache CSV files, not replay SQLite.
It is for the research EC2 host and writes a small JSON summary to /tmp.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", type=Path, required=True, action="append")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--market-total", type=int, required=True)
    return parser.parse_args()


def load_rows(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(
                    {
                        "day": row["day"],
                        "slug": row["slug"],
                        "ts": int(row["trigger_ts_ms"]),
                        "align": row["side_alignment"],
                        "price": float(row["public_trade_price"] or 0.0),
                        "size": float(row["public_trade_size"] or 0.0),
                        "first": float(row["first_l2_vwap"] or "nan"),
                        "l1": float(row["l1_immediate_pair"] or "nan"),
                        "first_win": str(row["first_is_winner"]).lower() == "true",
                        "ceil_95_hit": str(row["ceil_0_95_hit"]).lower() == "true",
                        "ceil_95_pair": row["ceil_0_95_pair_cost"],
                        "ceil_95_delay": row["ceil_0_95_delay_s"],
                        "ceil_96_hit": str(row["ceil_0_96_hit"]).lower() == "true",
                        "ceil_96_pair": row["ceil_0_96_pair_cost"],
                        "ceil_96_delay": row["ceil_0_96_delay_s"],
                        "ceil_98_hit": str(row["ceil_0_98_hit"]).lower() == "true",
                        "ceil_98_pair": row["ceil_0_98_pair_cost"],
                        "ceil_98_delay": row["ceil_0_98_delay_s"],
                    }
                )
    rows.sort(key=lambda item: (item["slug"], item["ts"]))
    return rows


def max_drawdown(seq: list[tuple[int, float, float, float, float]], idx: int) -> float:
    equity = 0.0
    peak = 0.0
    out = 0.0
    for item in sorted(seq):
        equity += item[idx]
        peak = max(peak, equity)
        out = min(out, equity - peak)
    return out


def evaluate(rows: list[dict[str, Any]], market_total: int, params: dict[str, Any]) -> dict[str, Any]:
    state: dict[str, dict[str, Any]] = {}
    gates: Counter[str] = Counter()
    seq: list[tuple[int, float, float, float, float]] = []
    clip = 60.0
    ceil_key = {0.95: "ceil_95", 0.96: "ceil_96", 0.98: "ceil_98"}[params["completion"]]
    accepted = 0
    closed = 0
    residuals = 0
    residual_winners = 0
    residual_losers = 0
    pnl = 0.0
    stress = 0.0
    worst = 0.0
    worst_stress = 0.0
    first_cost_sum = 0.0
    markets: set[str] = set()

    for row in rows:
        st = state.setdefault(row["slug"], {"entries": 0, "residuals": 0, "active_until": -1, "blocked": False})
        ts_ms = int(row["ts"])
        if st["blocked"]:
            gates["blocked_after_residual"] += 1
            continue
        if st["active_until"] >= ts_ms:
            gates["busy"] += 1
            continue
        if st["entries"] >= params["max_entries"]:
            gates["max_entries"] += 1
            continue
        if row["align"] != "high":
            gates["not_high"] += 1
            continue
        if not (params["price_min"] <= row["price"] < params["price_max"]):
            gates["price"] += 1
            continue
        if not (params["size_min"] <= row["size"] < 160.0):
            gates["size"] += 1
            continue
        if not (0.50 <= row["first"] <= 0.80):
            gates["vwap"] += 1
            continue
        if row["l1"] > params["l1_cap"]:
            gates["l1"] += 1
            continue

        first_cost = clip * row["first"]
        hit = bool(row[f"{ceil_key}_hit"])
        if hit:
            pair_cost = float(row[f"{ceil_key}_pair"])
            delay_s = float(row[f"{ceil_key}_delay"] or 0.0)
            trade_pnl = clip * (1.0 - pair_cost)
            trade_stress = trade_pnl - 0.02 * clip
            trade_worst = trade_pnl
            trade_worst_stress = trade_stress
            is_closed = 1
            is_residual = 0
            st["active_until"] = ts_ms + int(delay_s * 1000) + params["cooldown_s"] * 1000
        else:
            if st["residuals"] >= params["max_residuals"]:
                gates["max_residuals"] += 1
                continue
            if row["first_win"]:
                trade_pnl = clip * (1.0 - row["first"])
                residual_winners += 1
            else:
                trade_pnl = -first_cost
                residual_losers += 1
            trade_stress = trade_pnl - 0.01 * clip
            trade_worst = -first_cost
            trade_worst_stress = trade_worst - 0.01 * clip
            is_closed = 0
            is_residual = 1
            st["residuals"] += 1
            st["active_until"] = ts_ms + 30_000 + params["cooldown_s"] * 1000
            if params["block_after_residual"]:
                st["blocked"] = True

        st["entries"] += 1
        accepted += 1
        closed += is_closed
        residuals += is_residual
        pnl += trade_pnl
        stress += trade_stress
        worst += trade_worst
        worst_stress += trade_worst_stress
        first_cost_sum += first_cost
        markets.add(row["slug"])
        seq.append((ts_ms, trade_pnl, trade_stress, trade_worst, trade_worst_stress))

    rows_den = accepted or 1
    cost_den = first_cost_sum or 1.0
    return {
        **params,
        "rows": accepted,
        "markets": len(markets),
        "market_total": market_total,
        "coverage_pct": len(markets) / market_total * 100.0,
        "pnl": pnl,
        "stress_100bps": stress,
        "worst_residual_pnl": worst,
        "worst_residual_100bps": worst_stress,
        "closed_rate": closed / rows_den * 100.0,
        "residual_rate": residuals / rows_den * 100.0,
        "residuals": residuals,
        "residual_winners": residual_winners,
        "residual_losers": residual_losers,
        "roi_first_cost_pct": pnl / cost_den * 100.0,
        "stress_roi_first_cost_pct": stress / cost_den * 100.0,
        "worst_stress_roi_first_cost_pct": worst_stress / cost_den * 100.0,
        "max_drawdown": max_drawdown(seq, 1),
        "stress_max_drawdown": max_drawdown(seq, 2),
        "worst_stress_max_drawdown": max_drawdown(seq, 4),
        "avg_pnl_per_row": pnl / rows_den,
        "gates": dict(gates),
    }


def param_grid() -> list[dict[str, Any]]:
    params = []
    for price_min, price_max in ((0.45, 0.75), (0.45, 0.70), (0.50, 0.75), (0.50, 0.70)):
        for size_min in (10, 20, 30):
            for l1_cap in (0.96, 0.97, 0.98):
                for completion in (0.95, 0.96, 0.98):
                    for max_entries in (4, 6):
                        for max_residuals in (1, 2):
                            for cooldown_s in (0, 5, 10):
                                for block_after_residual in (False, True):
                                    params.append(
                                        {
                                            "price_min": price_min,
                                            "price_max": price_max,
                                            "size_min": size_min,
                                            "l1_cap": l1_cap,
                                            "completion": completion,
                                            "max_entries": max_entries,
                                            "max_residuals": max_residuals,
                                            "cooldown_s": cooldown_s,
                                            "block_after_residual": block_after_residual,
                                        }
                                    )
    return params


def is_base(result: dict[str, Any]) -> bool:
    return (
        result["price_min"] == 0.45
        and result["price_max"] == 0.75
        and result["size_min"] == 10
        and result["l1_cap"] == 0.98
        and result["completion"] == 0.98
        and result["max_entries"] == 6
        and result["max_residuals"] == 2
        and result["cooldown_s"] == 0
        and result["block_after_residual"] is False
    )


def main() -> int:
    args = parse_args()
    start = time.time()
    rows = load_rows(args.cache)
    results = [evaluate(rows, args.market_total, params) for params in param_grid()]
    eligible = [
        r
        for r in results
        if r["rows"] >= 3000
        and r["coverage_pct"] >= 85
        and r["closed_rate"] >= 99.0
        and r["worst_residual_100bps"] > 0
    ]
    ranked = sorted(
        eligible,
        key=lambda r: (r["worst_residual_100bps"], r["stress_100bps"], -r["residual_rate"], r["coverage_pct"]),
        reverse=True,
    )
    by_pnl = sorted(eligible, key=lambda r: (r["pnl"], r["worst_residual_100bps"]), reverse=True)
    base = next(r for r in results if is_base(r))
    output = {
        "cache": [str(path) for path in args.cache],
        "rows_loaded": len(rows),
        "elapsed_sec": round(time.time() - start, 3),
        "base": base,
        "top_by_worst_residual_100bps": ranked[:30],
        "top_by_pnl": by_pnl[:20],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({k: output[k] for k in ("rows_loaded", "elapsed_sec", "base")}, ensure_ascii=False, indent=2))
    print("top_by_worst_residual_100bps")
    for result in ranked[:12]:
        print(
            "p={price_min}-{price_max} size>={size_min} l1<={l1_cap} comp<={completion} "
            "entries={max_entries} maxres={max_residuals} cd={cooldown_s} block={block_after_residual} "
            "rows={rows} cov={coverage_pct:.2f}% pnl={pnl:.2f} stress={stress_100bps:.2f} "
            "worst100={worst_residual_100bps:.2f} closed={closed_rate:.3f}% resid={residual_rate:.3f}%".format(
                **result
            )
        )
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
