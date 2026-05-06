#!/usr/bin/env python3
"""Pull xuanxuan008 long-window public data from Polymarket data-api.

Endpoints (no auth):
- /positions  — current open positions (paginated by offset)
- /trades     — trade tape (paginated by offset, capped ~1k per call)
- /activity   — TRADE/MERGE/REDEEM events (paginated by offset)

Outputs (under data/xuan/):
- positions_snapshot_<date>.json
- trades_long.json   (list of dicts; ample fields)
- activity_long.json
- pull_summary.json  (cursor stats: max offset reached, time span)

This script is idempotent and safe to re-run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import urllib.request
import urllib.parse
import urllib.error


WALLET = "0xcfb103c37c0234f524c632d964ed31f117b5f694"
DATA_API = "https://data-api.polymarket.com"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "xuan"
USER_AGENT = "pm_as_ofi/xuan-research"


def fetch_json(url: str, retries: int = 3, sleep_s: float = 0.5) -> Any:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last_err = e
            time.sleep(sleep_s * (i + 1))
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def paginate(endpoint: str, params: dict[str, Any], page_size: int = 500, max_pages: int = 200) -> list[dict]:
    """Generic offset-based paginator. Stops when a page is short or server 400s.

    On HTTP 400 (server-side offset cap), gracefully returns what we have.
    """
    out: list[dict] = []
    offset = 0
    page_idx = 0
    while page_idx < max_pages:
        p = dict(params)
        p["limit"] = page_size
        p["offset"] = offset
        url = f"{DATA_API}{endpoint}?{urllib.parse.urlencode(p)}"
        try:
            page = fetch_json(url)
        except RuntimeError as e:
            msg = str(e)
            print(f"  {endpoint} offset={offset} STOP ({msg[:120]})")
            break
        if not isinstance(page, list):
            print(f"WARN: non-list response on {endpoint} offset={offset}: {type(page)}", file=sys.stderr)
            break
        out.extend(page)
        got = len(page)
        print(f"  {endpoint} offset={offset} got={got} total={len(out)}")
        if got < page_size:
            break
        offset += got
        page_idx += 1
        time.sleep(0.15)
    return out


def paginate_by_before(endpoint: str, params: dict[str, Any], ts_field: str = "timestamp", page_size: int = 500, max_pages: int = 400) -> list[dict]:
    """Time-cursor paginator: walk backward via ?before=<ts>.

    Avoids offset cap. Requires response items to expose a unix-seconds timestamp.
    """
    out: list[dict] = []
    before: int | None = None
    page_idx = 0
    seen_ids: set[str] = set()
    while page_idx < max_pages:
        p = dict(params)
        p["limit"] = page_size
        if before is not None:
            p["before"] = before
        url = f"{DATA_API}{endpoint}?{urllib.parse.urlencode(p)}"
        try:
            page = fetch_json(url)
        except RuntimeError as e:
            print(f"  {endpoint} before={before} STOP ({str(e)[:120]})")
            break
        if not isinstance(page, list) or not page:
            print(f"  {endpoint} before={before} empty/end")
            break
        new_count = 0
        min_ts = None
        for item in page:
            tid = item.get("transactionHash") or item.get("id") or item.get("hash") or json.dumps([item.get(ts_field), item.get("size"), item.get("price")], sort_keys=True)
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            out.append(item)
            new_count += 1
            ts = item.get(ts_field)
            if isinstance(ts, (int, float)) and (min_ts is None or ts < min_ts):
                min_ts = int(ts)
        print(f"  {endpoint} before={before} got={len(page)} new={new_count} total={len(out)} min_ts={min_ts}")
        if new_count == 0 or min_ts is None:
            break
        before = min_ts
        page_idx += 1
        time.sleep(0.2)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--positions-only", action="store_true")
    parser.add_argument("--skip-positions", action="store_true")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=400)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    summary: dict[str, Any] = {
        "wallet": WALLET,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "page_size": args.page_size,
    }

    if not args.skip_positions:
        print(f"[positions] starting page_size={args.page_size}")
        positions = paginate("/positions", {"user": WALLET}, args.page_size, args.max_pages)
        path = OUT_DIR / f"positions_snapshot_{today}.json"
        path.write_text(json.dumps(positions, ensure_ascii=False, indent=2))
        print(f"[positions] wrote {len(positions)} → {path}")
        summary["positions_count"] = len(positions)
        summary["positions_path"] = str(path)

    if args.positions_only:
        (OUT_DIR / "pull_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    # trades — first walk offset until truncated, then continue with ?before= cursor
    print(f"[trades] starting offset walk page_size={args.page_size}")
    trades = paginate("/trades", {"user": WALLET}, args.page_size, args.max_pages)
    if trades:
        ts_field_candidates = ("timestamp", "time", "matchedAt", "ts")
        ts_key = next((k for k in ts_field_candidates if k in trades[0]), None)
    else:
        ts_key = None
    if ts_key and trades:
        oldest = min(int(t[ts_key]) for t in trades if t.get(ts_key))
        print(f"[trades] offset wall hit at total={len(trades)}, switching to before={oldest} cursor")
        more = paginate_by_before("/trades", {"user": WALLET, "before": oldest}, ts_field=ts_key, page_size=args.page_size, max_pages=args.max_pages)
        # dedupe by transactionHash
        seen = {t.get("transactionHash") for t in trades if t.get("transactionHash")}
        for t in more:
            th = t.get("transactionHash")
            if th and th in seen:
                continue
            trades.append(t)
            if th:
                seen.add(th)
        print(f"[trades] after before-cursor extend: total={len(trades)}")
    path = OUT_DIR / "trades_long.json"
    path.write_text(json.dumps(trades, ensure_ascii=False))
    print(f"[trades] wrote {len(trades)} → {path}")
    summary["trades_count"] = len(trades)
    summary["trades_path"] = str(path)
    if ts_key:
        ts_vals = [t[ts_key] for t in trades if t.get(ts_key)]
        if ts_vals:
            summary["trades_ts_field"] = ts_key
            summary["trades_ts_min"] = min(ts_vals)
            summary["trades_ts_max"] = max(ts_vals)

    # activity (TRADE / MERGE / REDEEM)
    print(f"[activity] starting offset walk page_size={args.page_size}")
    activity = paginate("/activity", {"user": WALLET}, args.page_size, args.max_pages)
    if activity:
        ts_field_candidates = ("timestamp", "time", "matchedAt", "ts")
        a_ts_key = next((k for k in ts_field_candidates if k in activity[0]), None)
    else:
        a_ts_key = None
    if a_ts_key and activity:
        oldest = min(int(a[a_ts_key]) for a in activity if a.get(a_ts_key))
        print(f"[activity] offset wall hit at total={len(activity)}, switching to before={oldest} cursor")
        more = paginate_by_before("/activity", {"user": WALLET, "before": oldest}, ts_field=a_ts_key, page_size=args.page_size, max_pages=args.max_pages)
        seen = {a.get("transactionHash") for a in activity if a.get("transactionHash")}
        for a in more:
            th = a.get("transactionHash")
            if th and th in seen:
                continue
            activity.append(a)
            if th:
                seen.add(th)
        print(f"[activity] after before-cursor extend: total={len(activity)}")
    path = OUT_DIR / "activity_long.json"
    path.write_text(json.dumps(activity, ensure_ascii=False))
    print(f"[activity] wrote {len(activity)} → {path}")
    summary["activity_count"] = len(activity)
    summary["activity_path"] = str(path)
    if a_ts_key:
        a_vals = [a[a_ts_key] for a in activity if a.get(a_ts_key)]
        if a_vals:
            summary["activity_ts_field"] = a_ts_key
            summary["activity_ts_min"] = min(a_vals)
            summary["activity_ts_max"] = max(a_vals)

    (OUT_DIR / "pull_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"[summary] {summary}")


if __name__ == "__main__":
    main()
