#!/usr/bin/env python3
"""Analyze xuanxuan008 positions snapshot for pair-gated tranche fingerprints.

Inputs:
- data/xuan/positions_snapshot_<date>.json  (from pull_xuan_long_window.py)

Outputs:
- data/xuan/positions_analysis.json  (machine-readable summary)
- stdout report (human-readable)

Key questions answered:
1. Per-condition UP/DOWN balance — does pair-gated hold *currently*?
2. Mergeable vs redeemable composition — capital recycling profile
3. avgPrice (UP) + avgPrice (DOWN) per condition — current pair_cost histogram
4. Winner residual: in resolved markets (curPrice in {0,1}), is residual side biased?
5. negativeRisk vs binary distribution
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "xuan"


def percentiles(xs: list[float], qs=(10, 25, 50, 75, 90, 95, 99)) -> dict[str, float]:
    if not xs:
        return {f"p{q}": float("nan") for q in qs}
    s = sorted(xs)
    n = len(s)
    out = {}
    for q in qs:
        idx = min(n - 1, max(0, int(round(q / 100.0 * (n - 1)))))
        out[f"p{q}"] = s[idx]
    return out


def load_snapshot() -> tuple[Path, list[dict]]:
    files = sorted(OUT_DIR.glob("positions_snapshot_*.json"))
    if not files:
        raise SystemExit("no positions snapshot under data/xuan/")
    p = files[-1]
    return p, json.loads(p.read_text())


def main() -> None:
    src, positions = load_snapshot()
    print(f"# positions snapshot: {src.name}  rows={len(positions)}")

    # Group by conditionId
    by_cond: dict[str, dict[str, dict]] = defaultdict(dict)
    for r in positions:
        cond = r["conditionId"]
        outcome = r.get("outcome", "?")
        by_cond[cond][outcome] = r

    print(f"# unique conditions: {len(by_cond)}")
    sides_per_cond = [len(v) for v in by_cond.values()]
    print(f"# sides_per_cond distribution: 1side={sum(1 for x in sides_per_cond if x==1)}  2sides={sum(1 for x in sides_per_cond if x==2)}  3+={sum(1 for x in sides_per_cond if x>=3)}")

    # Per-condition pair balance
    pair_balance: list[float] = []  # |up_size - down_size| / max(up,down)
    pair_costs: list[float] = []    # avgPrice(UP) + avgPrice(DOWN)
    pair_total_bought_diff_ratio: list[float] = []
    only_up_count = 0
    only_down_count = 0
    both_count = 0
    redeemable_conds = 0
    mergeable_conds = 0
    cur_price_zero_one_conds = 0
    winner_resid_bias: list[tuple[str, float]] = []  # (residual_side, residual_qty/total)

    # Slug breakdown
    slug_market_count: dict[str, int] = defaultdict(int)
    neg_risk_count = 0

    for cond, sides in by_cond.items():
        up = sides.get("Up") or sides.get("Yes")
        down = sides.get("Down") or sides.get("No")
        if up:
            slug_market_count[up.get("slug", "?")] += 1
            if up.get("negativeRisk"):
                neg_risk_count += 1
        if up and not down:
            only_up_count += 1
        elif down and not up:
            only_down_count += 1
        elif up and down:
            both_count += 1
            up_size = up.get("size", 0) or 0
            down_size = down.get("size", 0) or 0
            total = up_size + down_size
            if total > 0:
                pair_balance.append(abs(up_size - down_size) / max(up_size, down_size, 1e-9))
            up_avg = up.get("avgPrice", 0) or 0
            down_avg = down.get("avgPrice", 0) or 0
            if up_avg > 0 and down_avg > 0:
                pair_costs.append(up_avg + down_avg)
            up_tb = up.get("totalBought", 0) or 0
            down_tb = down.get("totalBought", 0) or 0
            tb_total = up_tb + down_tb
            if tb_total > 0:
                pair_total_bought_diff_ratio.append(abs(up_tb - down_tb) / tb_total)

            up_cur = up.get("curPrice", 0)
            down_cur = down.get("curPrice", 0)
            if {up_cur, down_cur} == {0, 1} or {up_cur, down_cur} == {0.0, 1.0}:
                cur_price_zero_one_conds += 1
                # Resolved: which side is winner? (curPrice=1)
                winner_side = "Up" if up_cur == 1 else "Down"
                resid = up_size if winner_side == "Up" else down_size
                loser = down_size if winner_side == "Up" else up_size
                # Net residual on winner side after merging pairable
                pairable = min(up_size, down_size)
                winner_resid = resid - pairable
                loser_resid = loser - pairable
                if up_size + down_size > 0:
                    winner_resid_bias.append((winner_side, winner_resid - loser_resid))

        # Aggregate redeemable / mergeable across both sides
        for r in sides.values():
            if r.get("redeemable"):
                redeemable_conds += 1
                break
        for r in sides.values():
            if r.get("mergeable"):
                mergeable_conds += 1
                break

    print()
    print("## Per-condition UP/DOWN coverage")
    print(f"  both sides held : {both_count}")
    print(f"  only Up         : {only_up_count}")
    print(f"  only Down       : {only_down_count}")

    print()
    print("## Pair balance (|up-down| / max) — for both-sided conditions")
    pb = percentiles(pair_balance)
    print(f"  count           : {len(pair_balance)}")
    print(f"  p10={pb['p10']:.4f}  p50={pb['p50']:.4f}  p90={pb['p90']:.4f}  p99={pb['p99']:.4f}")
    print(f"  mean            : {statistics.mean(pair_balance) if pair_balance else float('nan'):.4f}")

    print()
    print("## Current pair_cost = avgPrice(UP)+avgPrice(DOWN)")
    pc = percentiles(pair_costs)
    print(f"  count           : {len(pair_costs)}")
    print(f"  p10={pc['p10']:.4f}  p50={pc['p50']:.4f}  p90={pc['p90']:.4f}")

    # bucket pair_costs
    buckets = {"<=0.95": 0, "0.95-1.00": 0, "1.00-1.04": 0, ">1.04": 0}
    for v in pair_costs:
        if v <= 0.95:
            buckets["<=0.95"] += 1
        elif v <= 1.00:
            buckets["0.95-1.00"] += 1
        elif v <= 1.04:
            buckets["1.00-1.04"] += 1
        else:
            buckets[">1.04"] += 1
    print(f"  buckets: {buckets}")

    print()
    print("## totalBought balance per condition (|up_tb - down_tb| / total)")
    tb = percentiles(pair_total_bought_diff_ratio)
    print(f"  count={len(pair_total_bought_diff_ratio)}  p10={tb['p10']:.4f}  p50={tb['p50']:.4f}  p90={tb['p90']:.4f}")

    print()
    print("## Recycling state")
    print(f"  conditions with mergeable=true (any side) : {mergeable_conds}")
    print(f"  conditions with redeemable=true (any side): {redeemable_conds}")
    print(f"  resolved (curPrice {{0,1}}): {cur_price_zero_one_conds}")

    print()
    print("## Winner residual bias (resolved both-sided conditions)")
    if winner_resid_bias:
        winner_resid_qty = [b for _, b in winner_resid_bias]
        wp = percentiles(winner_resid_qty)
        n_pos = sum(1 for x in winner_resid_qty if x > 0)
        n_neg = sum(1 for x in winner_resid_qty if x < 0)
        n_zero = sum(1 for x in winner_resid_qty if x == 0)
        print(f"  count={len(winner_resid_qty)}  winner-heavier={n_pos}  loser-heavier={n_neg}  equal={n_zero}")
        print(f"  resid_qty p10={wp['p10']:.2f}  p50={wp['p50']:.2f}  p90={wp['p90']:.2f}")
    else:
        print("  no resolved both-sided conditions (cannot bias-test)")

    print()
    print("## Slug / event coverage")
    print(f"  unique slugs: {len(slug_market_count)}  (each slug ≈ one round)")
    btc_slugs = [s for s in slug_market_count if "btc-updown" in s]
    other_slugs = [s for s in slug_market_count if "btc-updown" not in s]
    print(f"  btc-updown rounds: {len(btc_slugs)}  other slugs: {len(other_slugs)}")
    if other_slugs[:10]:
        print(f"  sample non-btc-updown: {other_slugs[:10]}")
    print(f"  negativeRisk conditions: {neg_risk_count}")

    summary = {
        "snapshot_path": str(src),
        "n_positions": len(positions),
        "n_conditions": len(by_cond),
        "both_sides": both_count,
        "only_up": only_up_count,
        "only_down": only_down_count,
        "pair_balance_pct": pb,
        "pair_cost_pct": pc,
        "pair_cost_buckets": buckets,
        "total_bought_balance_pct": tb,
        "mergeable_conds": mergeable_conds,
        "redeemable_conds": redeemable_conds,
        "resolved_conds": cur_price_zero_one_conds,
        "winner_resid_count": len(winner_resid_bias),
        "winner_resid_pct": percentiles([b for _, b in winner_resid_bias]) if winner_resid_bias else {},
        "winner_heavier": sum(1 for _, b in winner_resid_bias if b > 0),
        "loser_heavier": sum(1 for _, b in winner_resid_bias if b < 0),
        "unique_slugs": len(slug_market_count),
        "btc_updown_rounds": len(btc_slugs),
        "other_slug_count": len(other_slugs),
        "negative_risk_count": neg_risk_count,
    }
    out = OUT_DIR / "positions_analysis.json"
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nwrote {out}")

    # ----- Deep dive on single-sided residual structure -----
    print()
    print("## Single-side residual structure (post-MERGE leftover)")
    single_residuals: list[dict] = []
    for cond, sides in by_cond.items():
        if len(sides) != 1:
            continue
        only = next(iter(sides.values()))
        size = only.get("size", 0) or 0
        total_bought = only.get("totalBought", 0) or 0
        avg_price = only.get("avgPrice", 0) or 0
        cur_price = only.get("curPrice", 0) or 0
        outcome = only.get("outcome", "?")
        slug = only.get("slug", "?")
        merged_implied = max(0.0, total_bought - size)
        merged_ratio = merged_implied / total_bought if total_bought > 0 else 0.0
        single_residuals.append({
            "slug": slug,
            "outcome": outcome,
            "size": size,
            "total_bought": total_bought,
            "merged_implied": merged_implied,
            "merged_ratio": merged_ratio,
            "avg_price": avg_price,
            "cur_price": cur_price,
            "redeemable": only.get("redeemable", False),
            "mergeable": only.get("mergeable", False),
        })

    sizes = [r["size"] for r in single_residuals]
    tbs = [r["total_bought"] for r in single_residuals]
    merged_ratios = [r["merged_ratio"] for r in single_residuals]
    cur_prices = [r["cur_price"] for r in single_residuals]
    print(f"  count={len(single_residuals)}")
    sp = percentiles(sizes)
    print(f"  residual size       p10={sp['p10']:.1f}  p50={sp['p50']:.1f}  p90={sp['p90']:.1f}  p99={sp['p99']:.1f}")
    tp = percentiles(tbs)
    print(f"  total_bought        p10={tp['p10']:.1f}  p50={tp['p50']:.1f}  p90={tp['p90']:.1f}  p99={tp['p99']:.1f}")
    mp = percentiles(merged_ratios)
    print(f"  merged_ratio        p10={mp['p10']:.4f}  p50={mp['p50']:.4f}  p90={mp['p90']:.4f}  p99={mp['p99']:.4f}")
    cpc_zero = sum(1 for c in cur_prices if c <= 0.001)
    cpc_one = sum(1 for c in cur_prices if c >= 0.999)
    cpc_other = len(cur_prices) - cpc_zero - cpc_one
    print(f"  cur_price=0 (loser-resid): {cpc_zero}  cur_price=1 (winner-resid): {cpc_one}  in_between: {cpc_other}")

    # Cross with outcome side
    winner_side_outcome = defaultdict(lambda: {"size_sum": 0.0, "tb_sum": 0.0, "n": 0})
    for r in single_residuals:
        bucket = "winner" if r["cur_price"] >= 0.999 else ("loser" if r["cur_price"] <= 0.001 else "open")
        winner_side_outcome[bucket]["size_sum"] += r["size"]
        winner_side_outcome[bucket]["tb_sum"] += r["total_bought"]
        winner_side_outcome[bucket]["n"] += 1
    print(f"  by outcome bucket:")
    for k, v in winner_side_outcome.items():
        print(f"    {k:>6}: n={v['n']}  size_sum={v['size_sum']:.0f}  tb_sum={v['tb_sum']:.0f}  avg_resid_per_round={v['size_sum']/max(v['n'],1):.1f}")

    # Check: Up residuals vs Down residuals — distribution test
    up_residuals = [r["size"] for r in single_residuals if r["outcome"] == "Up"]
    down_residuals = [r["size"] for r in single_residuals if r["outcome"] == "Down"]
    print(f"  Up-side residuals : n={len(up_residuals)}  size_p50={percentiles(up_residuals)['p50']:.1f}  sum={sum(up_residuals):.0f}")
    print(f"  Down-side residuals: n={len(down_residuals)}  size_p50={percentiles(down_residuals)['p50']:.1f}  sum={sum(down_residuals):.0f}")

    # Save detail csv
    detail_path = OUT_DIR / "positions_single_residuals.json"
    detail_path.write_text(json.dumps(single_residuals, indent=2, ensure_ascii=False))
    print(f"  wrote {detail_path}")

    # ----- Currency / capital snapshot -----
    print()
    print("## Capital snapshot")
    cur_value_sum = sum((p.get("currentValue", 0) or 0) for p in positions)
    initial_value_sum = sum((p.get("initialValue", 0) or 0) for p in positions)
    cash_pnl_sum = sum((p.get("cashPnl", 0) or 0) for p in positions)
    realized_pnl_sum = sum((p.get("realizedPnl", 0) or 0) for p in positions)
    total_bought_sum = sum((p.get("totalBought", 0) or 0) for p in positions)
    print(f"  totalBought sum (cumul gross buys, $) : {total_bought_sum:,.0f}")
    print(f"  initialValue sum (cost remaining, $)  : {initial_value_sum:,.2f}")
    print(f"  currentValue sum (mark-to-mkt, $)     : {cur_value_sum:,.2f}")
    print(f"  realizedPnl sum (after merges/redeems): {realized_pnl_sum:,.2f}")
    print(f"  cashPnl sum (open-position MTM PnL)   : {cash_pnl_sum:,.2f}")


if __name__ == "__main__":
    main()
