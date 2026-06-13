#!/usr/bin/env python3
"""A1 follow-up: 用完整 slug_winner_map_full.json 重跑 MERGE 前 winner-loser bias 分析。

修正之前 (analyze_xuan_pre_merge_bias.py) 仅 44/497 MERGE 有 winner mapping 的 selection bias。
现在 425 unique slugs 全部有 winner mapping，应能覆盖 ≥ 80% MERGE events。

输出：data/xuan/pre_merge_bias_full.json + 控制台报告
"""
import json
import statistics
from collections import defaultdict, Counter
from pathlib import Path

OUT = Path("/Users/hot/web3Scientist/pm_as_ofi/data/xuan")

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
    winner_map = json.loads((OUT / "slug_winner_map_full.json").read_text())
    print(f"# slug→winner full mapping: {len(winner_map)} entries")

    # Group BUYs and MERGEs by slug
    by_slug_events = defaultdict(list)
    for t in trades:
        by_slug_events[t["slug"]].append({
            "ts": t["timestamp"], "type": "BUY",
            "side": t["outcome"], "size": t["size"],
        })
    merge_count = 0
    for a in activity:
        if a.get("type") != "MERGE":
            continue
        slug = a.get("slug")
        if not slug:
            continue
        by_slug_events[slug].append({
            "ts": a["timestamp"], "type": "MERGE",
            "size": a.get("size", 0) or 0,
        })
        merge_count += 1
    print(f"# MERGE events found: {merge_count}")
    print(f"# slugs with events: {len(by_slug_events)}")

    samples = []
    no_winner = 0
    for slug, events in by_slug_events.items():
        events.sort(key=lambda e: e["ts"])
        winner_info = winner_map.get(slug)
        winner_side = winner_info["winner"] if winner_info else None
        round_open_ts = slug_round_open_ts(slug)
        up_qty = down_qty = 0.0
        merge_idx_in_round = 0
        source = winner_info.get("source") if winner_info else None
        for ev in events:
            if ev["type"] == "BUY":
                if ev["side"] == "Up":
                    up_qty += ev["size"]
                else:
                    down_qty += ev["size"]
            else:  # MERGE
                merge_idx_in_round += 1
                if winner_side is None:
                    no_winner += 1
                    up_qty -= ev["size"]
                    down_qty -= ev["size"]
                    continue
                up_before, down_before = up_qty, down_qty
                merge_size = ev["size"]
                offset = ev["ts"] - round_open_ts if round_open_ts else None
                if winner_side == "Up":
                    winner_qty_pre = up_before
                    loser_qty_pre = down_before
                else:
                    winner_qty_pre = down_before
                    loser_qty_pre = up_before
                delta = winner_qty_pre - loser_qty_pre
                total = winner_qty_pre + loser_qty_pre
                samples.append({
                    "slug": slug, "merge_idx": merge_idx_in_round,
                    "merge_offset_s": offset, "winner_side": winner_side,
                    "winner_qty_pre": winner_qty_pre, "loser_qty_pre": loser_qty_pre,
                    "delta": delta, "total": total,
                    "delta_normalized": (delta / total) if total > 0 else 0.0,
                    "merge_size": merge_size, "source": source,
                })
                up_qty -= merge_size
                down_qty -= merge_size

    print(f"# MERGE events with winner mapping: {len(samples)}")
    print(f"# MERGE events without winner mapping: {no_winner}")
    print(f"# coverage: {len(samples) / max(merge_count, 1) * 100:.1f}%")

    deltas = [s["delta"] for s in samples]
    deltas_norm = [s["delta_normalized"] for s in samples]

    print()
    print("=" * 76)
    print(f"## (winner_qty − loser_qty) PRE-MERGE  (FULL SAMPLE n={len(deltas)})")
    print("=" * 76)
    pp = percentiles(deltas)
    print(f"  delta (shares)    p5={pp['p5']:>9.2f} p10={pp['p10']:>9.2f} p25={pp['p25']:>9.2f} p50={pp['p50']:>9.2f} p75={pp['p75']:>9.2f} p90={pp['p90']:>9.2f} p95={pp['p95']:>9.2f}")
    pp_n = percentiles(deltas_norm)
    print(f"  delta_norm        p5={pp_n['p5']:>9.4f} p10={pp_n['p10']:>9.4f} p25={pp_n['p25']:>9.4f} p50={pp_n['p50']:>9.4f} p75={pp_n['p75']:>9.4f} p90={pp_n['p90']:>9.4f} p95={pp_n['p95']:>9.4f}")

    n_winner_h = sum(1 for d in deltas if d > 0)
    n_loser_h = sum(1 for d in deltas if d < 0)
    n_eq = sum(1 for d in deltas if d == 0)
    print()
    print(f"  winner-heavier:    {n_winner_h:>5} ({100*n_winner_h/max(len(deltas),1):.1f}%)")
    print(f"  loser-heavier:     {n_loser_h:>5} ({100*n_loser_h/max(len(deltas),1):.1f}%)")
    print(f"  exactly balanced:  {n_eq:>5} ({100*n_eq/max(len(deltas),1):.1f}%)")
    print(f"  mean delta:        {statistics.mean(deltas):.3f}")
    print(f"  median delta:      {statistics.median(deltas):.3f}")
    print(f"  mean delta_norm:   {statistics.mean(deltas_norm):.4f}")

    # By source: positions vs gamma-events
    print()
    print("## delta by winner-mapping source")
    for src_name in ("positions:loser-side", "positions:winner-side", "gamma-events"):
        bucket = [s["delta"] for s in samples if s["source"] == src_name]
        if bucket:
            p = percentiles(bucket)
            wh = sum(1 for x in bucket if x > 0)
            lh = sum(1 for x in bucket if x < 0)
            print(f"  {src_name:<24} n={len(bucket):>4}  p10={p['p10']:>7.1f} p50={p['p50']:>7.1f} p90={p['p90']:>7.1f}  W:L = {wh}:{lh}")

    # By merge index
    print()
    print("## delta by merge_idx within round (full sample)")
    by_idx = defaultdict(list)
    for s in samples:
        by_idx[s["merge_idx"]].append(s["delta"])
    for idx in sorted(by_idx.keys())[:6]:
        d = by_idx[idx]
        p = percentiles(d)
        wh = sum(1 for x in d if x > 0)
        lh = sum(1 for x in d if x < 0)
        print(f"  merge #{idx}: n={len(d):>4}  p10={p['p10']:>7.1f} p50={p['p50']:>7.1f} p90={p['p90']:>7.1f}  W:L = {wh}:{lh}")

    # By merge offset bucket
    print()
    print("## delta by merge offset bucket")
    bucket_edges = [(0, 100), (100, 200), (200, 270), (270, 300), (300, 9999)]
    for lo, hi in bucket_edges:
        bucket = [s["delta"] for s in samples if s["merge_offset_s"] is not None and lo <= s["merge_offset_s"] < hi]
        if bucket:
            p = percentiles(bucket)
            wh = sum(1 for x in bucket if x > 0)
            lh = sum(1 for x in bucket if x < 0)
            print(f"  [{lo:>4},{hi:>4})s  n={len(bucket):>4}  p10={p['p10']:>7.1f} p50={p['p50']:>7.1f} p90={p['p90']:>7.1f}  W:L = {wh}:{lh}")

    # Verdict
    print()
    print("=" * 76)
    print("## V2 §0.5 修正 5b VERDICT (full sample)")
    print("=" * 76)
    p50 = pp["p50"]
    p50_norm = pp_n["p50"]
    coverage = len(samples) / max(merge_count, 1)
    if abs(p50) <= 5 and abs(p50_norm) <= 0.05:
        verdict = "**balanced** — 修正 5b 仅成立于 selection-biased subset；全样本接近 balanced"
    elif p50 > 5 or p50_norm > 0.05:
        verdict = "**winner-bias** — 出乎意料的方向，需重大修订"
    elif p50 < -5 or p50_norm < -0.05:
        verdict = f"**loser-bias 全样本验证**（p50={p50:.1f} shares, normalized={p50_norm:.3f}）"
    else:
        verdict = "**临界** — 不显著但有微弱趋势"
    print(f"  coverage: {coverage*100:.1f}%")
    print(f"  full sample delta p50 = {p50:.2f} shares  delta_norm p50 = {p50_norm:.4f}")
    print(f"  W:L overall = {n_winner_h}:{n_loser_h}")
    print(f"  verdict: {verdict}")

    # Save
    out_path = OUT / "pre_merge_bias_full.json"
    payload = {
        "merge_total": merge_count, "samples": len(samples),
        "coverage_pct": coverage * 100,
        "no_winner_count": no_winner,
        "delta_pct": pp, "delta_norm_pct": pp_n,
        "winner_heavier": n_winner_h, "loser_heavier": n_loser_h, "balanced": n_eq,
        "mean_delta": statistics.mean(deltas) if deltas else None,
        "median_delta": statistics.median(deltas) if deltas else None,
        "mean_delta_norm": statistics.mean(deltas_norm) if deltas_norm else None,
        "by_merge_idx": {str(k): {"count": len(v), **percentiles(v)} for k, v in by_idx.items()},
        "verdict": verdict,
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nwrote {out_path}")

if __name__ == "__main__":
    main()
