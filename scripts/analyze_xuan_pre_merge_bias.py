#!/usr/bin/env python3
"""xuan §11 第 5 条：MERGE 之前的瞬时仓位时序重建。

目的：解 V2 §2.2 的 winner-bias 悬念。如果 xuan 在 MERGE 触发瞬间已经累积了 winner-bias，
那 §2.2「彻底否定 winner residual」的 A 级断言就要被推翻；如果 MERGE 前 (winner_qty − loser_qty)
中位 ≈ 0，则 §2.2 升回 A 级。

算法（纯本地，无新数据拉取）：
1. 读 trades_long.json (BUY events) + activity_long.json (含 MERGE/REDEEM)
2. 读 positions_snapshot 拿 540 condition → loser_side 映射（cur_price=0 那侧）
3. 按 slug 分组，把 BUY + MERGE 事件合并按 timestamp 排序
4. 逐事件 replay：
   - BUY Up:   up_qty += size
   - BUY Down: down_qty += size
   - MERGE:    在该事件之前 snapshot (up_qty_before, down_qty_before)，
                然后 up_qty -= size; down_qty -= size
5. 对每个 MERGE 事件：
   - 查该 slug 的 winner side（来自 positions snapshot 反查）
   - 计算 (winner_qty_pre_merge, loser_qty_pre_merge, delta = winner - loser)
6. 输出全集 + 按 MERGE 序号 + 按距收盘时间分桶的分布

输出：
- data/xuan/pre_merge_bias.json
- 控制台 human-readable 报告
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict, Counter
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "data" / "xuan"


def percentiles(xs, qs=(5, 10, 25, 50, 75, 90, 95)):
    if not xs:
        return {f"p{q}": float("nan") for q in qs}
    s = sorted(xs)
    n = len(s)
    return {f"p{q}": s[min(n - 1, int(round(q / 100.0 * (n - 1))))] for q in qs}


def slug_round_open_ts(slug):
    parts = slug.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return None


def main():
    trades = json.loads((OUT / "trades_long.json").read_text())
    activity = json.loads((OUT / "activity_long.json").read_text())
    positions = json.loads((OUT / "positions_snapshot_2026-04-26.json").read_text())

    # 1) Build slug -> winner_side map
    # xuan 的 positions 都是 cur_price=0 的 loser side（V2 §2.2），所以 winner = opposite
    slug_to_winner = {}
    for p in positions:
        slug = p.get("slug")
        cur = p.get("curPrice", 0.5)
        outcome = p.get("outcome")
        if slug and cur is not None:
            if cur <= 0.001:  # loser side
                winner = "Down" if outcome == "Up" else "Up"
                slug_to_winner[slug] = winner
            elif cur >= 0.999:  # winner side directly
                slug_to_winner[slug] = outcome
    print(f"# slug→winner mapping size: {len(slug_to_winner)}")

    # 2) Group BUYs and MERGEs by slug
    by_slug_events = defaultdict(list)
    for t in trades:
        by_slug_events[t["slug"]].append({
            "ts": t["timestamp"],
            "type": "BUY",
            "side": t["outcome"],
            "size": t["size"],
        })
    merge_count = 0
    for a in activity:
        if a.get("type") != "MERGE":
            continue
        slug = a.get("slug")
        if not slug:
            continue
        by_slug_events[slug].append({
            "ts": a["timestamp"],
            "type": "MERGE",
            "size": a.get("size", 0) or 0,
            "usdcSize": a.get("usdcSize", 0) or 0,
        })
        merge_count += 1
    print(f"# MERGE events found in activity: {merge_count}")
    print(f"# slugs with events: {len(by_slug_events)}")

    # 3) Replay per slug, snapshot at each MERGE
    samples = []  # each entry: dict with delta etc
    no_winner = 0
    skip_negative = 0
    for slug, events in by_slug_events.items():
        events.sort(key=lambda e: e["ts"])
        winner_side = slug_to_winner.get(slug)
        round_open_ts = slug_round_open_ts(slug)
        up_qty = 0.0
        down_qty = 0.0
        merge_idx_in_round = 0
        for ev in events:
            if ev["type"] == "BUY":
                if ev["side"] == "Up":
                    up_qty += ev["size"]
                else:
                    down_qty += ev["size"]
            else:  # MERGE
                merge_idx_in_round += 1
                # snapshot BEFORE applying merge
                up_before = up_qty
                down_before = down_qty
                merge_size = ev["size"]
                offset = ev["ts"] - round_open_ts if round_open_ts else None

                if winner_side is None:
                    no_winner += 1
                    # apply merge anyway to keep bookkeeping consistent
                    up_qty -= merge_size
                    down_qty -= merge_size
                    continue

                # Some events may have inconsistent bookkeeping (we BUY less than what's MERGEd
                # because earlier BUYs are outside our 3500-trade window).
                if up_before < 0 or down_before < 0:
                    skip_negative += 1
                    up_qty -= merge_size
                    down_qty -= merge_size
                    continue

                if winner_side == "Up":
                    winner_qty_pre = up_before
                    loser_qty_pre = down_before
                else:
                    winner_qty_pre = down_before
                    loser_qty_pre = up_before
                delta = winner_qty_pre - loser_qty_pre
                total = winner_qty_pre + loser_qty_pre

                samples.append({
                    "slug": slug,
                    "merge_idx": merge_idx_in_round,
                    "merge_offset_s": offset,
                    "winner_side": winner_side,
                    "winner_qty_pre": winner_qty_pre,
                    "loser_qty_pre": loser_qty_pre,
                    "delta": delta,
                    "total": total,
                    "delta_normalized": (delta / total) if total > 0 else 0.0,
                    "merge_size": merge_size,
                })
                up_qty -= merge_size
                down_qty -= merge_size

    print(f"# MERGE events with winner-side mapping: {len(samples)}")
    print(f"# MERGE events without winner mapping (slug not in positions): {no_winner}")
    print(f"# MERGE events skipped (negative bookkeeping): {skip_negative}")

    # 4) Aggregate stats
    deltas = [s["delta"] for s in samples]
    deltas_norm = [s["delta_normalized"] for s in samples]

    print()
    print("=" * 72)
    print("## (winner_qty − loser_qty) distribution PRE-MERGE")
    print("=" * 72)
    pp = percentiles(deltas)
    print(f"  count={len(deltas)}")
    print(f"  delta (shares)        p5={pp['p5']:>8.2f}  p10={pp['p10']:>8.2f}  p25={pp['p25']:>8.2f}  p50={pp['p50']:>8.2f}  p75={pp['p75']:>8.2f}  p90={pp['p90']:>8.2f}  p95={pp['p95']:>8.2f}")
    pp_n = percentiles(deltas_norm)
    print(f"  delta_norm (/total)   p5={pp_n['p5']:>8.4f}  p10={pp_n['p10']:>8.4f}  p25={pp_n['p25']:>8.4f}  p50={pp_n['p50']:>8.4f}  p75={pp_n['p75']:>8.4f}  p90={pp_n['p90']:>8.4f}  p95={pp_n['p95']:>8.4f}")

    n_winner_h = sum(1 for d in deltas if d > 0)
    n_loser_h = sum(1 for d in deltas if d < 0)
    n_eq = sum(1 for d in deltas if d == 0)
    print()
    print(f"  winner-heavier  : {n_winner_h:>5} ({100*n_winner_h/max(len(deltas),1):.1f}%)")
    print(f"  loser-heavier   : {n_loser_h:>5} ({100*n_loser_h/max(len(deltas),1):.1f}%)")
    print(f"  exactly balanced: {n_eq:>5} ({100*n_eq/max(len(deltas),1):.1f}%)")
    print(f"  mean delta      : {statistics.mean(deltas):.3f}")
    print(f"  median delta    : {statistics.median(deltas):.3f}")
    print(f"  mean delta_norm : {statistics.mean(deltas_norm):.4f}")

    # 5) By merge index (1st vs 2nd vs 3rd... merge in same round)
    print()
    print("## delta by merge_idx within round")
    by_idx = defaultdict(list)
    for s in samples:
        by_idx[s["merge_idx"]].append(s["delta"])
    for idx in sorted(by_idx.keys())[:6]:
        d = by_idx[idx]
        p = percentiles(d)
        wh = sum(1 for x in d if x > 0)
        lh = sum(1 for x in d if x < 0)
        print(f"  merge #{idx}: n={len(d):>4}  p10={p['p10']:>7.1f} p50={p['p50']:>7.1f} p90={p['p90']:>7.1f}  winnerH={wh}  loserH={lh}")

    # 6) By merge offset bucket (early vs late within round)
    print()
    print("## delta by merge offset bucket (s vs round open)")
    bucket_edges = [(0, 100), (100, 200), (200, 270), (270, 300), (300, 9999)]
    for lo, hi in bucket_edges:
        bucket = [s["delta"] for s in samples if s["merge_offset_s"] is not None and lo <= s["merge_offset_s"] < hi]
        if bucket:
            p = percentiles(bucket)
            wh = sum(1 for x in bucket if x > 0)
            lh = sum(1 for x in bucket if x < 0)
            print(f"  [{lo:>4},{hi:>4})s  n={len(bucket):>4}  p10={p['p10']:>7.1f} p50={p['p50']:>7.1f} p90={p['p90']:>7.1f}  winnerH={wh}  loserH={lh}")

    # 7) Verdict
    print()
    print("=" * 72)
    print("## V2 §2.2 verdict")
    print("=" * 72)
    p50 = pp["p50"]
    p50_norm = pp_n["p50"]
    if abs(p50) <= 5 and abs(p50_norm) <= 0.05:
        verdict = "**A** — winner-bias 在 MERGE 前不显著存在 → §2.2 可升回 A 级"
    elif p50 > 5 or p50_norm > 0.05:
        verdict = "**降级 / 维持 B** — winner-bias 显著 → §0.5 修正 4 「MERGE = winner-bias prevention timer」获得直接证据"
    elif p50 < -5 or p50_norm < -0.05:
        verdict = "**异常 / 维持 B** — loser-bias 显著（出乎意料的反向）→ 需进一步研究"
    else:
        verdict = "**临界** — 不显著但有微弱趋势"
    print(f"  delta p50 = {p50:.2f} shares; delta_norm p50 = {p50_norm:.4f}")
    print(f"  verdict: {verdict}")

    # 8) Save
    out_path = OUT / "pre_merge_bias.json"
    payload = {
        "n_merges_with_winner": len(samples),
        "n_merges_no_winner": no_winner,
        "n_merges_skip_negative": skip_negative,
        "delta_pct": pp,
        "delta_norm_pct": pp_n,
        "winner_heavier": n_winner_h,
        "loser_heavier": n_loser_h,
        "balanced": n_eq,
        "mean_delta": statistics.mean(deltas) if deltas else None,
        "median_delta": statistics.median(deltas) if deltas else None,
        "by_merge_idx": {str(k): {"count": len(v), **percentiles(v)} for k, v in by_idx.items()},
        "verdict": verdict,
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
