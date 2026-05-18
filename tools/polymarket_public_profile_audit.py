#!/usr/bin/env python3
"""Audit a public Polymarket profile for strategy clues.

The script uses only public Polymarket web/Data API responses. It is intended
for lightweight account-level research: profile stats, current positions,
activity/trades samples, takerOnly true/false deltas, BTC 5m structure, and
simple fee-delta proxies.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DATA_API = "https://data-api.polymarket.com"
PROFILE_URL = "https://polymarket.com/@{handle}?tab=positions"


def fetch_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/html,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def fetch_json(url: str, timeout: int = 30) -> Any:
    return json.loads(fetch_text(url, timeout=timeout))


def data_url(path: str, params: dict[str, Any]) -> str:
    return f"{DATA_API}{path}?{urllib.parse.urlencode(params)}"


def extract_next_data(page_html: str) -> dict[str, Any]:
    m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', page_html, re.S)
    if not m:
        return {}
    return json.loads(html.unescape(m.group(1)))


def find_query(next_data: dict[str, Any], pred) -> Any:
    queries = next_data.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
    for q in queries:
        key = q.get("queryKey")
        if pred(key):
            return q.get("state", {}).get("data")
    return None


def flatten_pages(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict) and isinstance(data.get("pages"), list):
        out: list[dict[str, Any]] = []
        for page in data["pages"]:
            if isinstance(page, list):
                out.extend(x for x in page if isinstance(x, dict))
        return out
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def trade_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("transactionHash"),
        row.get("conditionId"),
        row.get("asset"),
        row.get("side"),
        row.get("price"),
        row.get("size"),
        row.get("timestamp"),
    )


def activity_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("transactionHash"),
        row.get("conditionId"),
        row.get("asset"),
        row.get("type"),
        row.get("side"),
        row.get("price"),
        row.get("size"),
        row.get("timestamp"),
    )


def dedup(rows: list[dict[str, Any]], key_fn) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for row in rows:
        key = key_fn(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def pull_endpoint(path: str, user: str, limit: int, offsets: list[int], extra: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for offset in offsets:
        params = {"user": user, "limit": limit, "offset": offset}
        if extra:
            params.update(extra)
        url = data_url(path, params)
        try:
            data = fetch_json(url)
        except Exception as e:
            rows.append({"__error__": f"{type(e).__name__}: {e}", "__url__": url})
            break
        if not isinstance(data, list):
            rows.append({"__error__": "non_list_response", "__url__": url, "__data__": data})
            break
        rows.extend(x for x in data if isinstance(x, dict))
        if len(data) < limit:
            break
        time.sleep(0.15)
    return rows


def num(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(row.get(key, default) or default)
    except Exception:
        return default


def summarize_activity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [r for r in rows if "__error__" not in r]
    by_type = Counter(str(r.get("type") or "") for r in rows)
    by_side = Counter(str(r.get("side") or "") for r in rows)
    by_slug_prefix = Counter()
    btc_rows = []
    fee_delta_pos = 0.0
    fee_delta_neg = 0.0
    buy_rows = 0
    buy_usdc = 0.0
    buy_notional = 0.0
    for r in rows:
        slug = str(r.get("slug") or r.get("eventSlug") or "")
        if "btc-updown-5m" in slug:
            btc_rows.append(r)
        if slug:
            by_slug_prefix[slug.split("-")[0]] += 1
        if r.get("type") == "TRADE" and r.get("side") == "BUY":
            buy_rows += 1
            usdc = num(r, "usdcSize")
            notional = num(r, "size") * num(r, "price")
            buy_usdc += usdc
            buy_notional += notional
            delta = usdc - notional
            if delta > 1e-9:
                fee_delta_pos += delta
            elif delta < -1e-9:
                fee_delta_neg += delta
    return {
        "rows": len(rows),
        "type_counts": by_type.most_common(),
        "side_counts": by_side.most_common(),
        "btc_5m_rows": len(btc_rows),
        "buy_rows": buy_rows,
        "buy_usdc": round(buy_usdc, 6),
        "buy_notional": round(buy_notional, 6),
        "fee_delta_pos_sum": round(fee_delta_pos, 6),
        "fee_delta_neg_sum": round(fee_delta_neg, 6),
        "top_slug_prefixes": by_slug_prefix.most_common(12),
    }


def summarize_trades(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [r for r in rows if "__error__" not in r]
    by_side = Counter(str(r.get("side") or "") for r in rows)
    by_outcome = Counter(str(r.get("outcome") or "") for r in rows)
    btc = [r for r in rows if "btc-updown-5m" in str(r.get("slug") or r.get("eventSlug") or "")]
    px_buckets = Counter()
    btc_by_outcome = Counter()
    btc_by_price = Counter()
    btc_buy_qty = 0.0
    btc_buy_cost = 0.0
    btc_slugs = set()
    for r in btc:
        px = num(r, "price")
        bucket = f"{int(px * 100):02d}c"
        px_buckets[bucket] += 1
        btc_by_price[px] += 1
        btc_by_outcome[str(r.get("outcome") or "")] += 1
        btc_slugs.add(str(r.get("slug") or r.get("eventSlug") or ""))
        if r.get("side") == "BUY":
            btc_buy_qty += num(r, "size")
            btc_buy_cost += num(r, "size") * px
    return {
        "rows": len(rows),
        "side_counts": by_side.most_common(),
        "outcome_counts": by_outcome.most_common(10),
        "btc_5m_rows": len(btc),
        "btc_5m_slugs": len(btc_slugs),
        "btc_buy_qty": round(btc_buy_qty, 6),
        "btc_buy_cost": round(btc_buy_cost, 6),
        "btc_outcome_counts": btc_by_outcome.most_common(),
        "btc_price_buckets": sorted(px_buckets.items()),
        "btc_top_exact_prices": btc_by_price.most_common(20),
    }


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    clean = [r for r in rows if "__error__" not in r]
    if not clean:
        return
    keys = sorted(set().union(*(r.keys() for r in clean)))
    with path.with_suffix(".csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(clean)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--handle", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--offsets", default="0,500,1000,1500,2000,2500,3000")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    offsets = [int(x) for x in args.offsets.split(",") if x.strip()]

    page_url = PROFILE_URL.format(handle=args.handle)
    page_html = fetch_text(page_url)
    (out / "profile_page.html").write_text(page_html, encoding="utf-8")
    next_data = extract_next_data(page_html)
    (out / "next_data.json").write_text(json.dumps(next_data, indent=2, sort_keys=True), encoding="utf-8")

    user_data = find_query(next_data, lambda k: isinstance(k, list) and k and k[0] == "/api/profile/userData")
    wallet = (user_data or {}).get("proxyWallet")
    if not wallet:
        wallets = re.findall(r"0x[a-fA-F0-9]{40}", page_html)
        wallet = Counter(wallets).most_common(1)[0][0] if wallets else ""
    if not wallet:
        raise SystemExit("could not resolve proxy wallet")

    positions_embedded = flatten_pages(find_query(next_data, lambda k: isinstance(k, list) and k[:2] == ["profile", "positions"]))
    user_stats = find_query(next_data, lambda k: isinstance(k, list) and k and k[0] == "user-stats")
    profile_volume = find_query(next_data, lambda k: isinstance(k, list) and k and k[0] == "/api/profile/volume")
    biggest_wins = find_query(next_data, lambda k: isinstance(k, list) and k and k[0] == "profile-biggest-wins")
    portfolio_all = find_query(next_data, lambda k: isinstance(k, list) and k[:2] == ["portfolio-pnl", wallet] and len(k) > 2 and k[2] == "ALL")

    positions = pull_endpoint("/positions", wallet, args.limit, [0])
    activity = dedup(pull_endpoint("/activity", wallet, args.limit, offsets), activity_key)
    trades_false = dedup(pull_endpoint("/trades", wallet, args.limit, offsets, {"takerOnly": "false"}), trade_key)
    trades_true = dedup(pull_endpoint("/trades", wallet, args.limit, offsets, {"takerOnly": "true"}), trade_key)
    true_keys = {trade_key(r) for r in trades_true if "__error__" not in r}
    false_keys = {trade_key(r) for r in trades_false if "__error__" not in r}
    maker_like_diff = [r for r in trades_false if "__error__" not in r and trade_key(r) not in true_keys]
    only_true = [r for r in trades_true if "__error__" not in r and trade_key(r) not in false_keys]

    btc_false = [r for r in trades_false if "btc-updown-5m" in str(r.get("slug") or r.get("eventSlug") or "")]
    btc_true = [r for r in trades_true if "btc-updown-5m" in str(r.get("slug") or r.get("eventSlug") or "")]
    btc_diff = [r for r in maker_like_diff if "btc-updown-5m" in str(r.get("slug") or r.get("eventSlug") or "")]
    btc_activity = [r for r in activity if "btc-updown-5m" in str(r.get("slug") or r.get("eventSlug") or "")]

    artifacts = {
        "positions_embedded.json": positions_embedded,
        "positions_api.json": positions,
        "activity.json": activity,
        "trades_taker_false.json": trades_false,
        "trades_taker_true.json": trades_true,
        "trades_maker_like_diff.json": maker_like_diff[:1000],
        "btc_trades_taker_false.json": btc_false,
        "btc_trades_taker_true.json": btc_true,
        "btc_trades_maker_like_diff.json": btc_diff,
        "btc_activity.json": btc_activity,
    }
    for name, rows in artifacts.items():
        write_rows(out / name, rows)

    summary = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "handle": args.handle,
        "profile_url": page_url,
        "proxy_wallet": wallet,
        "page_profile": {
            "user_data": user_data,
            "user_stats": user_stats,
            "profile_volume": profile_volume,
            "positions_value": find_query(next_data, lambda k: isinstance(k, list) and k[:2] == ["positions", "value"]),
            "embedded_positions_count": len(positions_embedded),
            "biggest_wins_sample": biggest_wins,
            "portfolio_all_points": len(portfolio_all or []),
            "portfolio_all_first": (portfolio_all or [None])[0],
            "portfolio_all_last": (portfolio_all or [None])[-1],
        },
        "api_pull": {
            "limit": args.limit,
            "offsets": offsets,
            "positions_rows": len([r for r in positions if "__error__" not in r]),
            "activity_rows": len([r for r in activity if "__error__" not in r]),
            "trades_false_rows": len([r for r in trades_false if "__error__" not in r]),
            "trades_true_rows": len([r for r in trades_true if "__error__" not in r]),
            "maker_like_diff_rows": len(maker_like_diff),
            "only_true_rows": len(only_true),
            "btc_false_rows": len(btc_false),
            "btc_true_rows": len(btc_true),
            "btc_maker_like_diff_rows": len(btc_diff),
            "btc_activity_rows": len(btc_activity),
        },
        "activity_summary": summarize_activity(activity),
        "trades_false_summary": summarize_trades(trades_false),
        "trades_true_summary": summarize_trades(trades_true),
        "btc_activity_summary": summarize_activity(btc_activity),
        "btc_false_summary": summarize_trades(btc_false),
        "btc_true_summary": summarize_trades(btc_true),
        "interpretation": {
            "notes": [
                "takerOnly diff is public endpoint evidence, not authenticated trader_side truth.",
                "fee_delta from activity usdcSize - size*price is a proxy and can be zero for passive/maker-like fills.",
                "Use this audit to extract design motifs; do not copy profile behavior without P2/shadow verification.",
            ]
        },
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"out_dir": str(out), "summary": summary}, indent=2, sort_keys=True)[:12000])


if __name__ == "__main__":
    main()
