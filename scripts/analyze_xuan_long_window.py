#!/usr/bin/env python3
"""Long-window trade/activity analysis for xuanxuan008.

Reads:
- data/xuan/trades_long.json
- data/xuan/activity_long.json
- data/xuan/positions_snapshot_<date>.json (latest)

Answers:
- B2 Session vs Round: hourly histogram, gap analysis (rounds joined vs available)
- B3 Clip size rules: distribution + conditioning on (intra-round position, prior side, prior clip size, time-since-round-open)
- B4 Same-second cross-side fills: how often UP & DOWN buys land in the same second
- Imbalance half-life and final_gap (for cross-check vs deep dive baseline)
- MERGE/REDEEM cadence and round-coverage

Outputs:
- data/xuan/long_window_analysis.json (machine-readable)
- stdout (human report)
"""
from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parent.parent / "data" / "xuan"


def percentiles(xs, qs=(10, 25, 50, 75, 90, 95, 99)):
    if not xs:
        return {f"p{q}": float("nan") for q in qs}
    s = sorted(xs)
    n = len(s)
    return {f"p{q}": s[min(n - 1, max(0, int(round(q / 100.0 * (n - 1)))))] for q in qs}


def load_trades():
    return json.loads((OUT / "trades_long.json").read_text())


def load_activity():
    return json.loads((OUT / "activity_long.json").read_text())


def slug_round_open_ts(slug: str) -> int | None:
    """`btc-updown-5m-1777036500` → 1777036500 (round open unix seconds)."""
    parts = slug.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return None


def round_total_secs(slug: str) -> int:
    if "-5m-" in slug:
        return 300
    if "-15m-" in slug:
        return 900
    if "-1h-" in slug:
        return 3600
    return 300


def main():
    trades = load_trades()
    activity = load_activity()

    print(f"# trades: {len(trades)}")
    print(f"# activity: {len(activity)}  by-type: {Counter(a.get('type') for a in activity)}")

    ts_min = min(t["timestamp"] for t in trades)
    ts_max = max(t["timestamp"] for t in trades)
    print(f"# window UTC : {datetime.fromtimestamp(ts_min, tz=timezone.utc).isoformat()} → {datetime.fromtimestamp(ts_max, tz=timezone.utc).isoformat()}")
    print(f"# window span: {(ts_max-ts_min)/3600:.2f} hours")

    by_slug: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_slug[t["slug"]].append(t)
    for v in by_slug.values():
        v.sort(key=lambda x: x["timestamp"])
    print(f"# unique slugs (rounds): {len(by_slug)}")

    # ---- B2 Session: hourly distribution ----
    hour_bins = Counter()
    weekday_bins = Counter()
    for t in trades:
        dt = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc)
        hour_bins[dt.hour] += 1
        weekday_bins[dt.weekday()] += 1
    print()
    print("## B2 hourly trade distribution (UTC hour → count)")
    for h in range(24):
        print(f"  {h:02d}: {hour_bins[h]}")
    print(f"## B2 weekday distribution (0=Mon..6=Sun)")
    for d in range(7):
        print(f"  {d}: {weekday_bins[d]}")

    # Gap detection: was every BTC 5m round in window participated in?
    earliest_round = (ts_min // 300) * 300
    latest_round = (ts_max // 300) * 300
    expected_rounds = list(range(earliest_round - 600, latest_round + 600, 300))
    seen_rounds = {slug_round_open_ts(s) for s in by_slug if "btc-updown-5m" in s}
    seen_rounds.discard(None)
    overlap = sum(1 for r in expected_rounds if r in seen_rounds)
    print(f"## B2 round coverage")
    print(f"  expected btc-5m rounds in window  : {len(expected_rounds)}")
    print(f"  actually-traded rounds            : {len(seen_rounds)}")
    print(f"  overlap (rounds joined / expected): {overlap} / {len(expected_rounds)}  ratio={overlap/max(len(expected_rounds),1):.3f}")

    # ---- B3 Clip size rules ----
    clip_sizes = [t["size"] for t in trades]
    cs = percentiles(clip_sizes)
    print()
    print("## B3 clip size distribution (all trades)")
    print(f"  n={len(clip_sizes)}  p10={cs['p10']:.1f}  p50={cs['p50']:.1f}  p90={cs['p90']:.1f}  p99={cs['p99']:.1f}  max={max(clip_sizes):.1f}")

    # Conditional: time-since-round-open bucket
    print()
    print("## B3 clip size by (seconds since round open) bucket")
    bucket_edges = [(0, 30), (30, 60), (60, 120), (120, 180), (180, 240), (240, 300), (300, 9999)]
    for lo, hi in bucket_edges:
        sizes = []
        for t in trades:
            r0 = slug_round_open_ts(t["slug"])
            if r0 is None:
                continue
            elapsed = t["timestamp"] - r0
            if lo <= elapsed < hi:
                sizes.append(t["size"])
        if sizes:
            p = percentiles(sizes)
            print(f"  [{lo:>4},{hi:>4}) n={len(sizes):>4}  p10={p['p10']:>6.1f} p50={p['p50']:>6.1f} p90={p['p90']:>6.1f}")

    # Conditional: clip size by side-sequence position within round (1st, 2nd, 3rd...)
    print()
    print("## B3 clip size by intra-round trade index")
    by_index_size = defaultdict(list)
    for slug, ts_list in by_slug.items():
        for i, t in enumerate(ts_list):
            by_index_size[i + 1].append(t["size"])
    for idx in range(1, 11):
        if idx in by_index_size:
            p = percentiles(by_index_size[idx])
            print(f"  trade #{idx:>2}: n={len(by_index_size[idx]):>4}  p10={p['p10']:>6.1f} p50={p['p50']:>6.1f} p90={p['p90']:>6.1f}")

    # Conditional: clip size by prior cumulative imbalance
    print()
    print("## B3 clip size vs prior imbalance |up-down|/total")
    imb_buckets = [(0, 0.05), (0.05, 0.15), (0.15, 0.30), (0.30, 1.0)]
    bucket_data = {b: [] for b in imb_buckets}
    for slug, ts_list in by_slug.items():
        up_acc = 0.0
        down_acc = 0.0
        for t in ts_list[1:]:  # skip first trade (no prior context)
            if t["outcome"] == "Up":
                pass
            cur_total = up_acc + down_acc
            if cur_total > 0:
                imb = abs(up_acc - down_acc) / cur_total
                for lo, hi in imb_buckets:
                    if lo <= imb < hi:
                        bucket_data[(lo, hi)].append(t["size"])
                        break
            if t["outcome"] == "Up":
                up_acc += t["size"]
            else:
                down_acc += t["size"]
    for b, sizes in bucket_data.items():
        if sizes:
            p = percentiles(sizes)
            print(f"  imb {b[0]:.2f}-{b[1]:.2f} n={len(sizes):>4}  p10={p['p10']:>6.1f} p50={p['p50']:>6.1f} p90={p['p90']:>6.1f}")

    # ---- B4 Same-second cross-side fills (pre-placed double ladder hint) ----
    print()
    print("## B4 same-second cross-side fills")
    same_sec_pairs = 0
    same_sec_total = 0
    for slug, ts_list in by_slug.items():
        prev: dict | None = None
        for t in ts_list:
            if prev and prev["timestamp"] == t["timestamp"]:
                same_sec_total += 1
                if prev["outcome"] != t["outcome"]:
                    same_sec_pairs += 1
            prev = t
    print(f"  same-second adjacent fills total: {same_sec_total}")
    print(f"  same-second cross-side pairs    : {same_sec_pairs}")
    print(f"  ratio (same-sec cross / total fills-1): {same_sec_pairs/max(len(trades)-1,1):.4f}")

    # Inter-fill delay distribution
    inter_delays = []
    cross_inter_delays = []
    same_inter_delays = []
    for slug, ts_list in by_slug.items():
        for i in range(1, len(ts_list)):
            d = ts_list[i]["timestamp"] - ts_list[i-1]["timestamp"]
            inter_delays.append(d)
            if ts_list[i]["outcome"] != ts_list[i-1]["outcome"]:
                cross_inter_delays.append(d)
            else:
                same_inter_delays.append(d)
    p_all = percentiles(inter_delays)
    p_cross = percentiles(cross_inter_delays)
    p_same = percentiles(same_inter_delays)
    print(f"  inter-fill delay all   : n={len(inter_delays)}  p10={p_all['p10']}  p50={p_all['p50']}  p90={p_all['p90']}")
    print(f"  inter-fill delay cross : n={len(cross_inter_delays)}  p10={p_cross['p10']}  p50={p_cross['p50']}  p90={p_cross['p90']}")
    print(f"  inter-fill delay same  : n={len(same_inter_delays)}  p10={p_same['p10']}  p50={p_same['p50']}  p90={p_same['p90']}")

    # ---- Side run length (cross-check vs deconstruction) ----
    print()
    print("## Cross-check: same-side run length distribution")
    runs = []
    for slug, ts_list in by_slug.items():
        if not ts_list:
            continue
        cur_side = ts_list[0]["outcome"]
        cur_len = 1
        for t in ts_list[1:]:
            if t["outcome"] == cur_side:
                cur_len += 1
            else:
                runs.append(cur_len)
                cur_side = t["outcome"]
                cur_len = 1
        runs.append(cur_len)
    rp = percentiles(runs)
    print(f"  n_runs={len(runs)}  p10={rp['p10']}  p50={rp['p50']}  p90={rp['p90']}  max={max(runs)}")
    rc = Counter(runs)
    print(f"  run length histogram: {dict(sorted(rc.items())[:8])} ...")

    # ---- Episode reconstruction (cross-check vs deep dive eps=10/25) ----
    print()
    print("## Cross-check: episode reconstruction (eps=10 strict, eps=25 loose)")

    def episodes_for(eps: float):
        opened = 0
        closed = 0
        clean_closed = 0
        same_side_add_qty = 0.0
        total_qty = 0.0
        close_delays = []
        for slug, ts_list in by_slug.items():
            up_acc = 0.0
            down_acc = 0.0
            in_episode = False
            episode_start_ts = None
            episode_first_side = None
            episode_first_qty = 0.0
            same_side_added = False
            for t in ts_list:
                qty = t["size"]
                total_qty += qty
                if t["outcome"] == "Up":
                    up_acc += qty
                else:
                    down_acc += qty
                net = up_acc - down_acc
                if not in_episode:
                    if abs(net) > eps:
                        in_episode = True
                        episode_start_ts = t["timestamp"]
                        episode_first_side = t["outcome"]
                        episode_first_qty = qty
                        same_side_added = False
                        opened += 1
                else:
                    if t["outcome"] == episode_first_side:
                        same_side_added = True
                        same_side_add_qty += qty
                    if abs(net) <= eps:
                        closed += 1
                        if not same_side_added:
                            clean_closed += 1
                        close_delays.append(t["timestamp"] - episode_start_ts)
                        in_episode = False
        return {
            "eps": eps,
            "opened": opened,
            "closed": closed,
            "clean_closed": clean_closed,
            "clean_closed_ratio": clean_closed / max(opened, 1),
            "same_side_add_qty": same_side_add_qty,
            "total_qty": total_qty,
            "same_side_add_ratio": same_side_add_qty / max(total_qty, 1e-9),
            "close_delay_p50": percentiles(close_delays)["p50"] if close_delays else None,
            "close_delay_p90": percentiles(close_delays)["p90"] if close_delays else None,
        }

    e10 = episodes_for(10)
    e25 = episodes_for(25)
    for label, e in [("eps=10", e10), ("eps=25", e25)]:
        print(f"  {label}: opened={e['opened']} closed={e['closed']} clean={e['clean_closed']} "
              f"clean_ratio={e['clean_closed_ratio']:.4f} same_side_add_ratio={e['same_side_add_ratio']:.4f} "
              f"close_delay_p50={e['close_delay_p50']} close_delay_p90={e['close_delay_p90']}")

    # ---- MERGE/REDEEM cadence within rounds ----
    print()
    print("## MERGE/REDEEM cadence")
    merge_offsets = []
    redeem_offsets = []
    for a in activity:
        if a.get("type") == "MERGE":
            r0 = slug_round_open_ts(a.get("slug", ""))
            if r0:
                merge_offsets.append(a["timestamp"] - r0)
        elif a.get("type") == "REDEEM":
            r0 = slug_round_open_ts(a.get("slug", ""))
            if r0:
                redeem_offsets.append(a["timestamp"] - r0)
    if merge_offsets:
        mp = percentiles(merge_offsets)
        print(f"  MERGE offset (s vs round open): n={len(merge_offsets)} p10={mp['p10']} p50={mp['p50']} p90={mp['p90']}")
    if redeem_offsets:
        rp = percentiles(redeem_offsets)
        print(f"  REDEEM offset (s vs round open): n={len(redeem_offsets)} p10={rp['p10']} p50={rp['p50']} p90={rp['p90']}")
    # MERGE often after round close (300s), REDEEM nearly always after close
    after_close = sum(1 for o in merge_offsets if o > 300)
    print(f"  MERGE after round close  : {after_close}/{len(merge_offsets)}  (in-round before close: {len(merge_offsets)-after_close})")
    redeem_after_close = sum(1 for o in redeem_offsets if o > 300)
    print(f"  REDEEM after round close : {redeem_after_close}/{len(redeem_offsets)}")

    # ---- MERGE size distribution (USDC and shares) ----
    merge_usdc = [a.get("usdcSize", 0) for a in activity if a.get("type") == "MERGE"]
    merge_size = [a.get("size", 0) for a in activity if a.get("type") == "MERGE"]
    redeem_usdc = [a.get("usdcSize", 0) for a in activity if a.get("type") == "REDEEM"]
    redeem_size = [a.get("size", 0) for a in activity if a.get("type") == "REDEEM"]
    print(f"  MERGE  usdc total: {sum(merge_usdc):.0f}  shares total: {sum(merge_size):.0f}")
    print(f"  REDEEM usdc total: {sum(redeem_usdc):.0f}  shares total: {sum(redeem_size):.0f}")

    # ---- Save summary ----
    summary = {
        "trades_count": len(trades),
        "activity_count": len(activity),
        "window_utc_min": datetime.fromtimestamp(ts_min, tz=timezone.utc).isoformat(),
        "window_utc_max": datetime.fromtimestamp(ts_max, tz=timezone.utc).isoformat(),
        "window_hours": (ts_max - ts_min) / 3600,
        "unique_slugs": len(by_slug),
        "round_coverage": {"expected": len(expected_rounds), "joined": overlap, "ratio": overlap/max(len(expected_rounds),1)},
        "hour_bins": dict(hour_bins),
        "clip_size_pct": cs,
        "same_sec_cross_side_pairs": same_sec_pairs,
        "same_sec_total_adjacent": same_sec_total,
        "inter_fill_delay_pct_all": p_all,
        "inter_fill_delay_pct_cross": p_cross,
        "inter_fill_delay_pct_same": p_same,
        "run_length_pct": rp,
        "run_length_histogram": dict(rc),
        "episodes_eps10": e10,
        "episodes_eps25": e25,
        "merge_offset_pct": percentiles(merge_offsets) if merge_offsets else {},
        "redeem_offset_pct": percentiles(redeem_offsets) if redeem_offsets else {},
        "merge_usdc_total": sum(merge_usdc),
        "redeem_usdc_total": sum(redeem_usdc),
        "merge_count": len(merge_usdc),
        "redeem_count": len(redeem_usdc),
    }
    out_path = OUT / "long_window_analysis.json"
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
