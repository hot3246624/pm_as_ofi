#!/usr/bin/env python3
"""Public-only Polymarket profile strategy review for D+ research.

This script uses only public Polymarket web/API endpoints. It does not read
private owner-trade truth, local raw/replay data, collector paths, shared
ingress, broker state, or any trading path.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_public_profile_strategy_review"
DEFAULT_PROFILE = "0xb55fa1296e6ec55d0ce53d93b9237389f11764d4"
DEFAULT_PROFILE_URL = (
    "https://polymarket.com/@0xb55fa1296e6ec55d0ce53d93b9237389f11764d4-1777575277609"
    "?tab=activity"
)
DATA_API = "https://data-api.polymarket.com"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_url_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 xuan-research-public-review"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return round(float(ordered[idx]), 6)


def price_bucket(price: float) -> str:
    if price < 0.10:
        return "px_lt_0p10"
    if price < 0.30:
        return "px_0p10_0p30"
    if price < 0.45:
        return "px_0p30_0p45"
    if price < 0.55:
        return "px_0p45_0p55"
    if price < 0.70:
        return "px_0p55_0p70"
    if price < 0.90:
        return "px_0p70_0p90"
    return "px_ge_0p90"


def fetch_paged(endpoint: str, profile: str, *, limit: int, max_rows: int, extra: dict[str, str] | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while len(out) < max_rows:
        params = {"user": profile, "limit": str(min(limit, max_rows - len(out))), "offset": str(offset)}
        if extra:
            params.update(extra)
        url = f"{DATA_API}/{endpoint}?{urllib.parse.urlencode(params)}"
        page = read_url_json(url)
        if not isinstance(page, list):
            raise RuntimeError(f"unexpected {endpoint} payload type: {type(page).__name__}")
        out.extend(page)
        if len(page) < limit:
            break
        offset += len(page)
        time.sleep(0.15)
    return out


def summarize_activity(trades: list[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {"trade_count": 0}
    timestamps = [int(t.get("timestamp") or 0) for t in trades if t.get("timestamp")]
    timestamps_sorted = sorted(timestamps)
    interarrival = [
        float(b - a)
        for a, b in zip(timestamps_sorted, timestamps_sorted[1:])
        if b >= a and b - a < 3600
    ]
    by_slug: dict[str, dict[str, Any]] = {}
    slug_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        slug_rows[str(trade.get("slug") or "")].append(trade)
    for slug, rows in sorted(slug_rows.items(), key=lambda item: -len(item[1]))[:25]:
        prices = [float(r.get("price") or 0.0) for r in rows]
        sizes = [float(r.get("size") or 0.0) for r in rows]
        sides = Counter(str(r.get("side") or "") for r in rows)
        outcomes = Counter(str(r.get("outcome") or "") for r in rows)
        by_slug[slug] = {
            "trade_count": len(rows),
            "side_counts": dict(sides),
            "outcome_counts": dict(outcomes),
            "outcome_count": len(outcomes),
            "size_sum": round(sum(sizes), 6),
            "notional_sum": round(sum(float(r.get("usdcSize") or 0.0) for r in rows), 6),
            "price_min": pct(prices, 0.0),
            "price_p25": pct(prices, 0.25),
            "price_p50": pct(prices, 0.5),
            "price_p75": pct(prices, 0.75),
            "price_max": pct(prices, 1.0),
            "first_timestamp": min(int(r.get("timestamp") or 0) for r in rows),
            "last_timestamp": max(int(r.get("timestamp") or 0) for r in rows),
        }
    assets = Counter()
    for trade in trades:
        title = str(trade.get("title") or "")
        token = title.split(" Up or Down", 1)[0] if " Up or Down" in title else title
        assets[token] += 1
    prices_all = [float(t.get("price") or 0.0) for t in trades]
    return {
        "trade_count": len(trades),
        "timestamp_min": min(timestamps) if timestamps else None,
        "timestamp_max": max(timestamps) if timestamps else None,
        "timestamp_min_utc": datetime.fromtimestamp(min(timestamps), timezone.utc).isoformat() if timestamps else None,
        "timestamp_max_utc": datetime.fromtimestamp(max(timestamps), timezone.utc).isoformat() if timestamps else None,
        "unique_slugs": len(slug_rows),
        "unique_conditions": len({str(t.get("conditionId") or "") for t in trades}),
        "side_counts": dict(Counter(str(t.get("side") or "") for t in trades)),
        "outcome_counts": dict(Counter(str(t.get("outcome") or "") for t in trades)),
        "asset_title_counts": dict(assets.most_common(20)),
        "price_buckets": dict(Counter(price_bucket(float(t.get("price") or 0.0)) for t in trades)),
        "price_p10": pct(prices_all, 0.10),
        "price_p50": pct(prices_all, 0.50),
        "price_p90": pct(prices_all, 0.90),
        "notional_sum": round(sum(float(t.get("usdcSize") or 0.0) for t in trades), 6),
        "size_sum": round(sum(float(t.get("size") or 0.0) for t in trades), 6),
        "interarrival_s_p50": pct(interarrival, 0.50),
        "interarrival_s_p90": pct(interarrival, 0.90),
        "top_slugs": by_slug,
    }


def summarize_positions(positions: list[dict[str, Any]]) -> dict[str, Any]:
    by_condition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pos in positions:
        by_condition[str(pos.get("conditionId") or "")].append(pos)
    pair_books: list[dict[str, Any]] = []
    for condition_id, rows in by_condition.items():
        outcomes = {str(row.get("outcome") or ""): row for row in rows}
        if len(outcomes) < 2:
            continue
        # Crypto binary markets use Up/Down outcomes in this profile.
        if not {"Up", "Down"}.issubset(set(outcomes)):
            continue
        up = outcomes["Up"]
        down = outcomes["Down"]
        up_size = float(up.get("size") or 0.0)
        down_size = float(down.get("size") or 0.0)
        up_avg = float(up.get("avgPrice") or 0.0)
        down_avg = float(down.get("avgPrice") or 0.0)
        pair_qty = min(up_size, down_size)
        pair_cost = up_avg + down_avg
        pair_pnl_proxy = pair_qty * (1.0 - pair_cost)
        residual_side = "Up" if up_size > down_size else "Down"
        residual_qty = abs(up_size - down_size)
        residual_avg = up_avg if residual_side == "Up" else down_avg
        pair_books.append(
            {
                "condition_id": condition_id,
                "slug": str(up.get("slug") or down.get("slug") or ""),
                "title": str(up.get("title") or down.get("title") or ""),
                "up_size": round(up_size, 6),
                "down_size": round(down_size, 6),
                "up_avg_price": round(up_avg, 6),
                "down_avg_price": round(down_avg, 6),
                "pair_qty": round(pair_qty, 6),
                "avg_pair_cost": round(pair_cost, 6),
                "pair_pnl_proxy_before_fees": round(pair_pnl_proxy, 6),
                "residual_side": residual_side,
                "residual_qty": round(residual_qty, 6),
                "residual_avg_price": round(residual_avg, 6),
                "mergeable": bool(up.get("mergeable")) or bool(down.get("mergeable")),
                "cash_pnl_sum": round(float(up.get("cashPnl") or 0.0) + float(down.get("cashPnl") or 0.0), 6),
                "current_value_sum": round(float(up.get("currentValue") or 0.0) + float(down.get("currentValue") or 0.0), 6),
                "initial_value_sum": round(float(up.get("initialValue") or 0.0) + float(down.get("initialValue") or 0.0), 6),
            }
        )
    total_pair_qty = sum(row["pair_qty"] for row in pair_books)
    total_pair_pnl_proxy = sum(row["pair_pnl_proxy_before_fees"] for row in pair_books)
    residual_qty = sum(row["residual_qty"] for row in pair_books)
    return {
        "position_count": len(positions),
        "conditions_with_positions": len(by_condition),
        "conditions_with_both_sides": len(pair_books),
        "both_side_condition_share": round(len(pair_books) / len(by_condition), 6) if by_condition else None,
        "total_initial_value": round(sum(float(p.get("initialValue") or 0.0) for p in positions), 6),
        "total_current_value": round(sum(float(p.get("currentValue") or 0.0) for p in positions), 6),
        "total_cash_pnl": round(sum(float(p.get("cashPnl") or 0.0) for p in positions), 6),
        "total_pair_qty_proxy": round(total_pair_qty, 6),
        "total_pair_pnl_proxy_before_fees": round(total_pair_pnl_proxy, 6),
        "total_both_side_residual_qty": round(residual_qty, 6),
        "top_pair_books": sorted(pair_books, key=lambda row: -abs(row["pair_pnl_proxy_before_fees"]))[:20],
    }


def classify(activity: dict[str, Any], positions: dict[str, Any]) -> tuple[str, str, list[str], dict[str, Any]]:
    both_side_share = positions.get("both_side_condition_share") or 0.0
    pair_pnl = float(positions.get("total_pair_pnl_proxy_before_fees") or 0.0)
    trade_count = int(activity.get("trade_count") or 0)
    top_slugs = activity.get("top_slugs") or {}
    multi_outcome_slug_count = sum(1 for row in top_slugs.values() if int(row.get("outcome_count") or 0) >= 2)
    if both_side_share >= 0.25 and pair_pnl > 0 and trade_count >= 100:
        decision = "KEEP"
        status = "KEEP_PUBLIC_PROFILE_TWO_SIDED_PAIRING_HYPOTHESIS"
    elif trade_count == 0:
        decision = "UNKNOWN"
        status = "UNKNOWN_PUBLIC_PROFILE_ACTIVITY_EMPTY"
    else:
        decision = "DISCARD"
        status = "DISCARD_PUBLIC_PROFILE_NOT_DPLUS_RELEVANT"
    notes = [
        "Public data shows current positions and recent activity only; it is proxy evidence, not private owner-trade truth.",
        "The profile concentrates in short-horizon crypto Up/Down markets and often holds both outcomes in the same condition.",
        "The observable hypothesis is inventory-level two-sided pairing/mergeability, not the frozen fill-to-balance tiny-deficit cap family.",
    ]
    hypothesis = {
        "name": "public_profile_two_sided_inventory_pairing",
        "description": (
            "Review whether D+ should rank variants by account-level paired inventory value: allow larger participation "
            "when the overlapped YES/NO book has avg_pair_cost < 1.0 and residual is covered by mark/stress budget, "
            "rather than rejecting solely on action-level p90 or fixed residual qty."
        ),
        "local_verifier_outline": [
            "Use candidate_base/candidate_registry only; no shadow or raw/replay scan.",
            "Build per-condition paired-inventory ledger with avg_pair_cost, pair_qty, residual_qty/cost, and marked residual proxy.",
            "Compare late_repair90 control versus a portfolio-level risk-budget ranking rule.",
            "Do not reuse fill-to-balance90, source-public-price caps, static target/cap sweeps, or cooldown/admission caps.",
        ],
        "public_observable_support": {
            "both_side_condition_share": both_side_share,
            "public_position_pair_pnl_proxy_before_fees": round(pair_pnl, 6),
            "top_recent_slugs_with_both_outcomes_traded": multi_outcome_slug_count,
        },
    }
    return decision, status, notes, hypothesis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--profile-url", default=DEFAULT_PROFILE_URL)
    parser.add_argument("--activity-limit", type=int, default=500)
    parser.add_argument("--activity-max-rows", type=int, default=1500)
    parser.add_argument("--positions-limit", type=int, default=500)
    parser.add_argument("--positions-max-rows", type=int, default=500)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    activity_url = (
        f"{DATA_API}/activity?user={urllib.parse.quote(args.profile)}"
        f"&limit={args.activity_limit}&offset=0&type=TRADE"
    )
    positions_url = (
        f"{DATA_API}/positions?user={urllib.parse.quote(args.profile)}"
        f"&limit={args.positions_limit}&offset=0"
    )
    try:
        trades = fetch_paged(
            "activity",
            args.profile,
            limit=args.activity_limit,
            max_rows=args.activity_max_rows,
            extra={"type": "TRADE"},
        )
        positions = fetch_paged(
            "positions",
            args.profile,
            limit=args.positions_limit,
            max_rows=args.positions_max_rows,
        )
    except Exception as exc:  # public API may be unavailable/rate limited
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": label,
            "lane": "public_profile_review",
            "decision_label": "UNKNOWN",
            "status": "UNKNOWN_PUBLIC_PROFILE_ACCESS_BLOCKED",
            "error": f"{type(exc).__name__}: {exc}",
            "public_sources_attempted": [args.profile_url, activity_url, positions_url],
            "scope": {
                "public_web_api_only": True,
                "private_truth_used": False,
                "ssh_used": False,
                "shared_ingress_connected": False,
                "raw_replay_collector_scanned": False,
                "full_completion_store_scanned": False,
                "orders_cancels_redeems_sent": False,
            },
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps({"status": manifest["status"], "manifest": str(output_dir / "manifest.json")}, indent=2))
        return 0

    activity_summary = summarize_activity(trades)
    position_summary = summarize_positions(positions)
    decision, status, notes, hypothesis = classify(activity_summary, position_summary)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "public_profile_review",
        "decision_label": decision,
        "status": status,
        "profile": {
            "proxy_wallet": args.profile.lower(),
            "profile_url": args.profile_url,
        },
        "scope": {
            "public_web_api_only": True,
            "private_owner_trade_truth_used": False,
            "public_account_audit_is_proxy_truth_only": True,
            "ssh_used": False,
            "shared_ingress_connected": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_collector_scanned": False,
            "full_completion_store_scanned": False,
            "runner_or_trading_paths_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "public_sources_used": [
            args.profile_url,
            activity_url,
            positions_url,
        ],
        "activity_sample": {
            "requested_trade_rows": args.activity_max_rows,
            "fetched_trade_rows": len(trades),
            "requested_position_rows": args.positions_max_rows,
            "fetched_position_rows": len(positions),
        },
        "observed_activity_patterns": activity_summary,
        "observed_position_patterns": position_summary,
        "research_ranking": {
            "label": status,
            "decision": decision,
            "notes": notes,
            "hypothesis": hypothesis,
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_PROFILE_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
            "notes": [
                "Public profile review cannot support canary/promotion.",
                "Any derived idea must go through local candidate-pipeline verifier and later no-order/source-of-truth validation.",
            ],
        },
        "limitations": [
            "The public API exposes activity and current positions, not order intent, queue priority, hidden cancellations, or private owner-trade truth.",
            "Open positions are mark-to-market snapshots and may change after this artifact.",
            "Pair PnL proxy ignores fees, settlement timing, and realized merge/redeem cashflow.",
        ],
        "next_executable_action": (
            "Run a local no-network candidate-pipeline verifier for public_profile_two_sided_inventory_pairing: "
            "compare late_repair90 control against a portfolio-level paired-inventory risk-adjusted ranking rule, "
            "without fill-to-balance90, public/source price caps, static target/cap sweeps, cooldown/admission caps, EC2, or raw/replay scans."
            if decision == "KEEP"
            else "Do not derive a D+ verifier from this profile; choose a different local non-price mechanism."
        ),
        "deployable": False,
        "can_support_strategy_promotion": False,
        "g2_canary_ready": False,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "status": status,
                "decision_label": decision,
                "manifest": str(output_dir / "manifest.json"),
                "fetched_trade_rows": len(trades),
                "fetched_position_rows": len(positions),
                "conditions_with_both_sides": position_summary.get("conditions_with_both_sides"),
                "pair_pnl_proxy_before_fees": position_summary.get("total_pair_pnl_proxy_before_fees"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
