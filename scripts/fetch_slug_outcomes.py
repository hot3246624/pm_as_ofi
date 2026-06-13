#!/usr/bin/env python3
"""A1: 补全 xuan trades 中所有 slug 的 winner side 映射。

策略：
1. positions_snapshot 已有 540 conditionId 的 winner mapping（cur_price=0 那侧 = loser, 反推 winner）
2. xuan trades 涉及 425 unique slugs，其中 ~44 在 positions（已知 winner），其余 381 通过 gamma-api 补全

gamma-api 路径：
  events?slug=<slug> → events[0].markets[0]
  - umaResolutionStatuses (resolved 后)
  - outcomePrices  (字符串数组 "Up","Down" 或 token-encoded)
  - 也可看 markets[0].outcomes + markets[0].outcomePrices

输出 data/xuan/slug_winner_map_full.json，结构：
  {
    "<slug>": {"winner": "Up"|"Down", "source": "positions"|"gamma-events"},
    ...
  }
"""
import json
import urllib.request
import time
from collections import Counter
from pathlib import Path

XUAN_DIR = Path("/Users/hot/web3Scientist/pm_as_ofi/data/xuan")
OUT_PATH = XUAN_DIR / "slug_winner_map_full.json"

def fetch(url, retries=3):
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pm-research/A1"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except Exception as e:
            last_err = e
            time.sleep(0.4 * (i + 1))
    return None

def winner_from_event(event):
    """从 gamma-api event 对象提取 winner side ("Up" or "Down")."""
    if not event:
        return None
    markets = event.get("markets") or []
    if not markets:
        return None
    m = markets[0]
    # outcomes is e.g. '["Up","Down"]' string
    outcomes_raw = m.get("outcomes")
    prices_raw = m.get("outcomePrices")
    if not outcomes_raw or not prices_raw:
        return None
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    except Exception:
        return None
    if len(outcomes) != 2 or len(prices) != 2:
        return None
    # winner = outcome with price = "1" (or close)
    p0 = float(prices[0]) if prices[0] else 0.0
    p1 = float(prices[1]) if prices[1] else 0.0
    if p0 >= 0.999:
        return outcomes[0]
    if p1 >= 0.999:
        return outcomes[1]
    return None  # not yet resolved

def main():
    print("loading data...")
    trades = json.loads((XUAN_DIR / "trades_long.json").read_text())
    positions = json.loads((XUAN_DIR / "positions_snapshot_2026-04-26.json").read_text())

    # Step 1: extract winner from positions snapshot
    slug_winner = {}
    pos_by_slug = {}
    for p in positions:
        slug = p.get("slug")
        if slug:
            pos_by_slug.setdefault(slug, []).append(p)
    for slug, plist in pos_by_slug.items():
        # Use the first loser side (cur_price=0) to infer winner = opposite outcome
        for p in plist:
            cur = p.get("curPrice", 0.5)
            outcome = p.get("outcome")
            if cur is None:
                continue
            if cur <= 0.001:
                winner = "Down" if outcome == "Up" else "Up"
                slug_winner[slug] = {"winner": winner, "source": "positions:loser-side"}
                break
            elif cur >= 0.999:
                slug_winner[slug] = {"winner": outcome, "source": "positions:winner-side"}
                break
    print(f"  from positions: {len(slug_winner)} slug→winner mappings")

    # Step 2: identify slugs in trades not yet mapped
    unique_slugs = sorted({t["slug"] for t in trades})
    missing = [s for s in unique_slugs if s not in slug_winner]
    print(f"  total unique slugs in trades: {len(unique_slugs)}")
    print(f"  missing (need gamma-api fetch): {len(missing)}")

    # Step 3: gamma-api fetch each missing slug
    failed = []
    success_via_events = 0
    for i, slug in enumerate(missing):
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        events = fetch(url)
        winner = None
        if isinstance(events, list) and events:
            winner = winner_from_event(events[0])
        if winner:
            slug_winner[slug] = {"winner": winner, "source": "gamma-events"}
            success_via_events += 1
        else:
            failed.append(slug)
        if (i + 1) % 30 == 0:
            print(f"  progress {i+1}/{len(missing)}  ok={success_via_events}  failed={len(failed)}")
        time.sleep(0.18)

    # Save
    OUT_PATH.write_text(json.dumps(slug_winner, indent=2, ensure_ascii=False))
    print(f"\nDONE")
    print(f"  total mappings: {len(slug_winner)}")
    print(f"  via positions:  {sum(1 for v in slug_winner.values() if v['source'].startswith('positions'))}")
    print(f"  via gamma-events: {success_via_events}")
    print(f"  failed (slug not yet resolved or no event): {len(failed)}")
    if failed[:10]:
        print(f"  sample failures: {failed[:10]}")
    print(f"\nwrote {OUT_PATH}")

if __name__ == "__main__":
    main()
