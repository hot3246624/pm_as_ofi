#!/usr/bin/env python3
"""A2: 重建 xuan 完整 cash-flow ledger，解开 realizedPnl=-$23k 与 FIFO +$5k 口径差。

事件类型：
- BUY (TRADE): 现金流出 = price × size, 持仓加 size on outcome side
- MERGE: 现金流入 = pairable_qty × $1, 持仓 up_qty/down_qty 各减 size
- REDEEM: 现金流入 = winner_qty × $1, 持仓 winner_side qty 减为 0

每个 round 维护：
  total_buy_usdc, total_merge_recv, total_redeem_recv, residual_qty (per side)

完整 ledger PnL = sum(merge_recv + redeem_recv) - sum(buy_usdc) + residual_mark_to_market
其中 residual_mark_to_market 用 positions snapshot 的 currentValue.

输出：
  data/xuan/cash_flow_ledger.json — per-slug PnL 明细 + 全局汇总
"""
import json
from collections import defaultdict
from pathlib import Path

OUT = Path("/Users/hot/web3Scientist/pm_as_ofi/data/xuan")

def main():
    trades = json.loads((OUT / "trades_long.json").read_text())
    activity = json.loads((OUT / "activity_long.json").read_text())
    positions = json.loads((OUT / "positions_snapshot_2026-04-26.json").read_text())
    winner_map = json.loads((OUT / "slug_winner_map_full.json").read_text())

    # Build per-slug ledger
    by_slug = defaultdict(lambda: {
        "buy_usdc": 0.0, "buy_qty_up": 0.0, "buy_qty_down": 0.0, "n_buys": 0,
        "merge_qty": 0.0, "merge_recv_usdc": 0.0, "n_merges": 0,
        "redeem_qty": 0.0, "redeem_recv_usdc": 0.0, "n_redeems": 0,
        "winner_side": None,
        "residual_currentvalue_usdc": 0.0,
        "residual_initialvalue_usdc": 0.0,
        "residual_qty": 0.0, "residual_outcome": None,
    })

    # 1) Aggregate trades (BUY only - xuan no SELL)
    for t in trades:
        slug = t["slug"]; ent = by_slug[slug]
        side = t.get("side")
        if side != "BUY":
            continue
        usdc_out = float(t["price"]) * float(t["size"])
        ent["buy_usdc"] += usdc_out
        ent["n_buys"] += 1
        if t["outcome"] == "Up":
            ent["buy_qty_up"] += float(t["size"])
        else:
            ent["buy_qty_down"] += float(t["size"])

    # 2) Aggregate MERGE / REDEEM from activity
    for a in activity:
        atype = a.get("type")
        slug = a.get("slug")
        if not slug:
            continue
        ent = by_slug[slug]
        if atype == "MERGE":
            qty = float(a.get("size", 0) or 0)
            usdc = float(a.get("usdcSize", 0) or 0)
            ent["merge_qty"] += qty
            ent["merge_recv_usdc"] += usdc if usdc else qty  # if usdcSize missing, qty = USDC since 1:1
            ent["n_merges"] += 1
        elif atype == "REDEEM":
            qty = float(a.get("size", 0) or 0)
            usdc = float(a.get("usdcSize", 0) or 0)
            ent["redeem_qty"] += qty
            ent["redeem_recv_usdc"] += usdc if usdc else qty
            ent["n_redeems"] += 1

    # 3) Set winner side from full mapping
    for slug, ent in by_slug.items():
        wm = winner_map.get(slug)
        if wm:
            ent["winner_side"] = wm["winner"]

    # 4) Add residual from positions snapshot
    pos_by_slug = defaultdict(list)
    for p in positions:
        pos_by_slug[p["slug"]].append(p)
    for slug, plist in pos_by_slug.items():
        ent = by_slug[slug]
        for p in plist:
            ent["residual_currentvalue_usdc"] += float(p.get("currentValue", 0) or 0)
            ent["residual_initialvalue_usdc"] += float(p.get("initialValue", 0) or 0)
            ent["residual_qty"] += float(p.get("size", 0) or 0)
            if p.get("size", 0) > 0:
                ent["residual_outcome"] = p.get("outcome")

    # 5) Compute per-slug PnL
    summary = {
        "total_buy_usdc": 0.0,
        "total_merge_recv_usdc": 0.0,
        "total_redeem_recv_usdc": 0.0,
        "total_residual_currentvalue": 0.0,
        "total_residual_initialvalue": 0.0,
        "n_slugs_buy": 0, "n_slugs_merge": 0, "n_slugs_redeem": 0, "n_slugs_residual": 0,
        "n_slugs_clean": 0,  # buy+merge complete, no residual
    }
    per_slug_pnl = []
    for slug, ent in by_slug.items():
        # cash flow PnL: net cash + residual mtm
        net_cash = ent["merge_recv_usdc"] + ent["redeem_recv_usdc"] - ent["buy_usdc"]
        # mtm contribution: residual currentValue
        ledger_pnl = net_cash + ent["residual_currentvalue_usdc"]
        per_slug_pnl.append({
            "slug": slug,
            "buy_usdc": ent["buy_usdc"],
            "merge_recv": ent["merge_recv_usdc"],
            "redeem_recv": ent["redeem_recv_usdc"],
            "residual_currentvalue": ent["residual_currentvalue_usdc"],
            "net_cash": net_cash,
            "ledger_pnl": ledger_pnl,
            "winner_side": ent["winner_side"],
            "residual_outcome": ent["residual_outcome"],
            "residual_qty": ent["residual_qty"],
            "n_buys": ent["n_buys"],
            "n_merges": ent["n_merges"],
            "n_redeems": ent["n_redeems"],
        })
        summary["total_buy_usdc"] += ent["buy_usdc"]
        summary["total_merge_recv_usdc"] += ent["merge_recv_usdc"]
        summary["total_redeem_recv_usdc"] += ent["redeem_recv_usdc"]
        summary["total_residual_currentvalue"] += ent["residual_currentvalue_usdc"]
        summary["total_residual_initialvalue"] += ent["residual_initialvalue_usdc"]
        if ent["n_buys"] > 0: summary["n_slugs_buy"] += 1
        if ent["n_merges"] > 0: summary["n_slugs_merge"] += 1
        if ent["n_redeems"] > 0: summary["n_slugs_redeem"] += 1
        if ent["residual_qty"] > 0: summary["n_slugs_residual"] += 1
        if ent["n_buys"] > 0 and ent["residual_qty"] == 0: summary["n_slugs_clean"] += 1

    # Compute total ledger PnL
    summary["total_ledger_pnl"] = (
        summary["total_merge_recv_usdc"] + summary["total_redeem_recv_usdc"]
        - summary["total_buy_usdc"] + summary["total_residual_currentvalue"]
    )
    summary["total_net_cash"] = (
        summary["total_merge_recv_usdc"] + summary["total_redeem_recv_usdc"]
        - summary["total_buy_usdc"]
    )

    # data-api lifetime fields
    summary["data_api_realizedpnl_sum"] = sum(p.get("realizedPnl", 0) or 0 for p in positions)
    summary["data_api_cashpnl_sum"] = sum(p.get("cashPnl", 0) or 0 for p in positions)
    summary["data_api_totalbought_sum"] = sum(p.get("totalBought", 0) or 0 for p in positions)

    # Print summary
    print("=" * 76)
    print("## xuan cash-flow ledger reconstruction (37h trade window only)")
    print("=" * 76)
    print(f"  unique slugs: {len(by_slug)}")
    print(f"  slugs with BUYs:    {summary['n_slugs_buy']}")
    print(f"  slugs with MERGE:   {summary['n_slugs_merge']}")
    print(f"  slugs with REDEEM:  {summary['n_slugs_redeem']}")
    print(f"  slugs clean (no residual): {summary['n_slugs_clean']}")
    print(f"  slugs with residual: {summary['n_slugs_residual']}")
    print()
    print(f"  TOTAL BUY USDC out:      ${summary['total_buy_usdc']:>14,.2f}")
    print(f"  TOTAL MERGE recv USDC:   ${summary['total_merge_recv_usdc']:>14,.2f}")
    print(f"  TOTAL REDEEM recv USDC:  ${summary['total_redeem_recv_usdc']:>14,.2f}")
    print(f"  TOTAL net cash flow:     ${summary['total_net_cash']:>14,.2f}")
    print(f"  + residual MTM (currentValue): ${summary['total_residual_currentvalue']:>14,.2f}")
    print(f"  = LEDGER PnL (window only):    ${summary['total_ledger_pnl']:>14,.2f}")
    print()
    print(f"## data-api 字段对照（lifetime, 不限于 37h 窗口）")
    print(f"  realizedPnl sum:  ${summary['data_api_realizedpnl_sum']:>14,.2f}")
    print(f"  cashPnl sum:      ${summary['data_api_cashpnl_sum']:>14,.2f}")
    print(f"  totalBought sum:  ${summary['data_api_totalbought_sum']:>14,.2f}")
    print()
    print(f"## V1 deep dive FIFO 估算: +$5,422.02")
    print(f"## 我们 ledger（基于 trades+activity 完整 cash-flow，37h 窗口）: ${summary['total_ledger_pnl']:,.2f}")
    print(f"## data-api realizedPnl: {summary['data_api_realizedpnl_sum']:,.2f}（lifetime, 不限 37h）")

    # Per-slug PnL distribution
    pnls = [s["ledger_pnl"] for s in per_slug_pnl]
    pnls.sort()
    n = len(pnls)
    print()
    print("## per-slug ledger PnL distribution")
    if n:
        print(f"  count={n}, mean=${sum(pnls)/n:.2f}")
        print(f"  p5=${pnls[max(0,int(0.05*n))]:.2f}  p25=${pnls[int(0.25*n)]:.2f}  p50=${pnls[n//2]:.2f}  p75=${pnls[int(0.75*n)]:.2f}  p95=${pnls[int(0.95*n)]:.2f}")
    losses = [p for p in pnls if p < 0]
    profits = [p for p in pnls if p > 0]
    zeros = [p for p in pnls if p == 0]
    print(f"  profit slugs: {len(profits)}  loss slugs: {len(losses)}  zero: {len(zeros)}")
    if losses: print(f"  total loss sum: ${sum(losses):,.2f}")
    if profits: print(f"  total profit sum: ${sum(profits):,.2f}")

    # Save
    out = OUT / "cash_flow_ledger.json"
    out.write_text(json.dumps({
        "summary": summary,
        "per_slug": per_slug_pnl[:600],  # cap to avoid huge file
        "per_slug_count": len(per_slug_pnl),
    }, indent=2, ensure_ascii=False))
    print(f"\nwrote {out}")

if __name__ == "__main__":
    main()
