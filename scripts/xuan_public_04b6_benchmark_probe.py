#!/usr/bin/env python3
"""Probe 04b6 as a public execution benchmark for xuan-frontier.

This script is local-only and public-data-only. It reads Polymarket public
profile APIs, writes xuan-owned scorecards, and never treats public/proxy data
as owner private truth or live authorization.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import statistics
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DEFAULT_WALLET = "0x04b6d7e930cf9e493c5e6ef24b496294f95594c8"
DEFAULT_SCORECARD_ROOT = Path(".tmp_xuan/scorecards")
DEFAULT_ARTIFACT_ROOT = Path(".tmp_xuan/local_verifier_artifacts")
DATA_API = "https://data-api.polymarket.com"
PROFILE_URL = (
    "https://polymarket.com/zh/@"
    "0x04b6d7e930cf9e493c5e6ef24b496294f95594c8-1774448369789"
)


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%MZ")


def iso(ts: int | float | None) -> str | None:
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def fnum(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return out if math.isfinite(out) else 0.0


def api_get(path: str, params: dict[str, Any], timeout_s: float) -> list[dict[str, Any]]:
    url = f"{DATA_API}/{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json,text/plain,*/*",
            "Origin": "https://polymarket.com",
            "Referer": "https://polymarket.com/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        data = json.loads(response.read())
    if not isinstance(data, list):
        raise TypeError(f"expected list from {url}")
    return data


def fetch_pages(
    path: str,
    params: dict[str, Any],
    *,
    limit: int,
    max_pages: int,
    timeout_s: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen: set[tuple[Any, ...]] = set()
    for offset in range(0, limit * max_pages, limit):
        try:
            batch = api_get(
                path,
                {**params, "limit": str(limit), "offset": str(offset)},
                timeout_s,
            )
        except Exception as exc:  # noqa: BLE001 - public API failures are soft evidence.
            warnings.append(f"{path}_offset_{offset}_fetch_failed:{type(exc).__name__}")
            break
        if not batch:
            break
        new_rows = 0
        for row in batch:
            key = (
                row.get("transactionHash"),
                row.get("asset"),
                row.get("timestamp"),
                row.get("type"),
                row.get("side"),
                row.get("size"),
                row.get("price"),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
            new_rows += 1
        if len(batch) < limit or new_rows == 0:
            break
        time.sleep(0.05)
    return rows, warnings


def classify_timeframe(row: dict[str, Any]) -> str:
    text = " ".join(
        str(row.get(key, "")) for key in ("slug", "title", "eventSlug")
    ).lower()
    if "15m" in text:
        return "15m"
    if "5m" in text:
        return "5m"
    if "up or down" in text:
        return "named_updown"
    return "other"


def is_btc(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key, "")) for key in ("slug", "title", "eventSlug")
    ).lower()
    return "btc" in text or "bitcoin" in text


def row_usdc(row: dict[str, Any]) -> float:
    return fnum(row.get("usdcSize")) or fnum(row.get("size")) * fnum(row.get("price"))


def taker_fee_upper_bound(rows: list[dict[str, Any]], fee_rate: float) -> float:
    total = 0.0
    for row in rows:
        if row.get("type") != "TRADE":
            continue
        price = fnum(row.get("price"))
        total += fnum(row.get("size")) * fee_rate * price * (1.0 - price)
    return total


def paired_buy_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_condition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("type") == "TRADE" and row.get("side") == "BUY" and is_btc(row):
            by_condition[str(row.get("conditionId"))].append(row)

    pairs: list[dict[str, Any]] = []
    for condition_id, condition_rows in by_condition.items():
        outcomes: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "qty": 0.0,
                "cost": 0.0,
                "slug": None,
                "title": None,
                "first_ts": 10**18,
                "last_ts": 0,
            }
        )
        for row in condition_rows:
            outcome = str(row.get("outcome"))
            qty = fnum(row.get("size"))
            price = fnum(row.get("price"))
            ts = int(fnum(row.get("timestamp")))
            item = outcomes[outcome]
            item["qty"] += qty
            item["cost"] += qty * price
            item["slug"] = row.get("slug") or row.get("eventSlug")
            item["title"] = row.get("title")
            item["first_ts"] = min(item["first_ts"], ts)
            item["last_ts"] = max(item["last_ts"], ts)

        if len(outcomes) < 2:
            continue
        vals = list(outcomes.items())[:2]
        paired_qty = min(item["qty"] for _, item in vals)
        if paired_qty <= 0:
            continue
        pair_cost = sum(
            item["cost"] / item["qty"] if item["qty"] else 0.0 for _, item in vals
        )
        total_qty = sum(item["qty"] for _, item in vals)
        residual_qty = total_qty - 2.0 * paired_qty
        pairs.append(
            {
                "condition_id": condition_id,
                "slug": vals[0][1]["slug"],
                "title": vals[0][1]["title"],
                "pair_cost_avg_all_buys": pair_cost,
                "paired_qty": paired_qty,
                "total_buy_qty": total_qty,
                "residual_qty": residual_qty,
                "residual_qty_share_all_buys": residual_qty / total_qty
                if total_qty
                else None,
                "first_iso": iso(min(item["first_ts"] for _, item in vals)),
                "last_iso": iso(max(item["last_ts"] for _, item in vals)),
            }
        )

    if not pairs:
        return {
            "paired_condition_count": 0,
            "weighted_pair_cost_avg_all_buys": None,
            "residual_qty_share_all_buys": None,
            "examples_low_pair_cost": [],
            "examples_high_pair_cost": [],
        }

    paired_den = sum(item["paired_qty"] for item in pairs)
    total_qty = sum(item["total_buy_qty"] for item in pairs)
    costs = [item["pair_cost_avg_all_buys"] for item in pairs]
    return {
        "paired_condition_count": len(pairs),
        "weighted_pair_cost_avg_all_buys": sum(
            item["pair_cost_avg_all_buys"] * item["paired_qty"] for item in pairs
        )
        / paired_den
        if paired_den
        else None,
        "median_pair_cost_avg_all_buys": statistics.median(costs),
        "min_pair_cost_avg_all_buys": min(costs),
        "max_pair_cost_avg_all_buys": max(costs),
        "paired_qty": paired_den,
        "total_buy_qty": total_qty,
        "residual_qty": sum(item["residual_qty"] for item in pairs),
        "residual_qty_share_all_buys": sum(item["residual_qty"] for item in pairs)
        / total_qty
        if total_qty
        else None,
        "examples_low_pair_cost": sorted(
            pairs, key=lambda item: item["pair_cost_avg_all_buys"]
        )[:5],
        "examples_high_pair_cost": sorted(
            pairs, key=lambda item: item["pair_cost_avg_all_buys"], reverse=True
        )[:5],
    }


def positions_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "rows": len(rows),
        "btc_rows": sum(1 for row in rows if is_btc(row)),
        "timeframes": dict(Counter(classify_timeframe(row) for row in rows)),
        "initial_value": sum(fnum(row.get("initialValue")) for row in rows),
        "current_value": sum(fnum(row.get("currentValue")) for row in rows),
        "cash_pnl": sum(fnum(row.get("cashPnl")) for row in rows),
        "top_positions": [
            {
                "slug": row.get("slug"),
                "outcome": row.get("outcome"),
                "size": fnum(row.get("size")),
                "avg_price": fnum(row.get("avgPrice")),
                "cur_price": fnum(row.get("curPrice")),
                "initial_value": fnum(row.get("initialValue")),
                "current_value": fnum(row.get("currentValue")),
                "cash_pnl": fnum(row.get("cashPnl")),
            }
            for row in sorted(
                rows, key=lambda item: fnum(item.get("currentValue")), reverse=True
            )[:10]
        ],
    }


def build_scorecard(args: argparse.Namespace) -> dict[str, Any]:
    wallet = args.wallet.lower()
    activity, activity_warnings = fetch_pages(
        "activity",
        {"user": wallet},
        limit=args.limit,
        max_pages=args.max_pages,
        timeout_s=args.timeout_s,
    )
    positions = api_get(
        "positions",
        {"user": wallet, "limit": str(args.limit)},
        timeout_s=args.timeout_s,
    )

    timestamps = [int(fnum(row.get("timestamp"))) for row in activity if row.get("timestamp")]
    latest_ts = max(timestamps) if timestamps else int(time.time())
    cutoff_ts = latest_ts - int(args.window_hours * 3600)
    window_rows = [
        row for row in activity if int(fnum(row.get("timestamp"))) >= cutoff_ts
    ]
    trade_rows = [row for row in window_rows if row.get("type") == "TRADE"]
    redeem_rows = [row for row in window_rows if row.get("type") == "REDEEM"]
    trade_usdc = sum(row_usdc(row) for row in trade_rows)
    redeem_usdc = sum(row_usdc(row) for row in redeem_rows)
    window_condition_ids = {
        str(row.get("conditionId"))
        for row in window_rows
        if row.get("conditionId") not in (None, "")
    }
    window_positions = [
        row
        for row in positions
        if str(row.get("conditionId")) in window_condition_ids
    ]
    all_position = positions_summary(positions)
    window_position = positions_summary(window_positions)
    public_mtm_pnl = redeem_usdc + window_position["current_value"] - trade_usdc
    fee_upper_bound = taker_fee_upper_bound(trade_rows, args.taker_fee_rate)
    pair_stats = paired_buy_stats(window_rows)
    btc_trade_share = (
        sum(1 for row in trade_rows if is_btc(row)) / len(trade_rows)
        if trade_rows
        else None
    )

    useful_checks = {
        "min_trade_usdc": trade_usdc >= args.min_trade_usdc,
        "min_paired_conditions": pair_stats["paired_condition_count"]
        >= args.min_paired_conditions,
        "max_residual_qty_share": (
            pair_stats["residual_qty_share_all_buys"] is not None
            and pair_stats["residual_qty_share_all_buys"]
            <= args.max_residual_qty_share
        ),
        "min_btc_trade_share": (
            btc_trade_share is not None and btc_trade_share >= args.min_btc_trade_share
        ),
        "positive_public_mtm": public_mtm_pnl > 0.0,
    }
    public_benchmark_useful = all(useful_checks.values())
    caveats = [
        "public_proxy_not_owner_private_truth",
        "maker_taker_status_not_proven_by_public_activity_api",
        "fee_truth_not_owner_reconciled",
        "pair_cost_is_buy_only_average_not_lot_matched_execution_truth",
        "open_position_mtm_is_public_mark_to_market_not_settled_cash",
        "not_deployable_not_live_authorized",
    ]
    xuan_implications = [
        "use_04b6_as_execution_shape_benchmark_not_prediction_model",
        "target_residual_qty_share_mid_teens_or_lower_before_capacity_expansion",
        "study_maker_queue_and_inventory_rebalance_for_future_canary_owner_truth",
        "do_not_treat_public_redeem_mtm_profit_as_private_truth_ready",
        "do_not_skip_cap25_residual_tail_work_to_run_cap75",
    ]
    hard_blockers = [
        name for name, passed in useful_checks.items() if not bool(passed)
    ]

    return {
        "artifact": "xuan_public_04b6_benchmark_probe",
        "created_utc": iso(time.time()),
        "script": "scripts/xuan_public_04b6_benchmark_probe.py",
        "wallet": wallet,
        "profile_url": PROFILE_URL,
        "source_scope": "public_polymarket_activity_positions_only",
        "research_only": True,
        "deployable": False,
        "live_orders_allowed": False,
        "private_truth_ready": False,
        "remote_runner_allowed": False,
        "window": {
            "hours": args.window_hours,
            "latest_ts": latest_ts,
            "latest_iso": iso(latest_ts),
            "start_ts": cutoff_ts,
            "start_iso": iso(cutoff_ts),
        },
        "fetch": {
            "activity_rows_total": len(activity),
            "activity_rows_window": len(window_rows),
            "position_rows": len(positions),
            "warnings": activity_warnings,
        },
        "activity_window": {
            "type_counts": dict(Counter(row.get("type") for row in window_rows)),
            "type_usdc": {
                key: sum(
                    row_usdc(row)
                    for row in window_rows
                    if row.get("type") == key
                )
                for key in sorted(set(row.get("type") for row in window_rows))
            },
            "trade_side_counts": dict(Counter(row.get("side") for row in trade_rows)),
            "trade_side_usdc": {
                key: sum(row_usdc(row) for row in trade_rows if row.get("side") == key)
                for key in sorted(set(row.get("side") for row in trade_rows))
            },
            "btc_trade_share": btc_trade_share,
            "timeframes": dict(Counter(classify_timeframe(row) for row in trade_rows)),
        },
        "position_summary_all_open": all_position,
        "position_summary_window_conditions": window_position,
        "fee_model_public_upper_bound": {
            "taker_fee_rate": args.taker_fee_rate,
            "formula": "fee = size * fee_rate * price * (1 - price)",
            "all_trade_rows_treated_as_taker_fee_upper_bound": fee_upper_bound,
            "fee_upper_bound_on_trade_usdc": fee_upper_bound / trade_usdc
            if trade_usdc
            else None,
            "maker_fee_is_zero_in_polymarket_docs": True,
        },
        "public_pnl_proxy": {
            "trade_buy_usdc": trade_usdc,
            "redeem_usdc": redeem_usdc,
            "current_window_position_value": window_position["current_value"],
            "redeem_plus_current_window_value": redeem_usdc
            + window_position["current_value"],
            "simple_public_mtm_pnl": public_mtm_pnl,
            "simple_public_mtm_roi_on_trade_usdc": public_mtm_pnl / trade_usdc
            if trade_usdc
            else None,
            "scope": "public_activity_redeem_plus_current_positions_minus_window_trade_usdc",
        },
        "pairish_buy_only": pair_stats,
        "decision": {
            "public_benchmark_useful_for_xuan": public_benchmark_useful,
            "useful_checks": useful_checks,
            "hard_blockers": hard_blockers,
            "caveats": caveats,
            "xuan_implications": xuan_implications,
        },
        "status": "KEEP_PUBLIC_04B6_BENCHMARK_USEFUL_LOCAL_ONLY"
        if public_benchmark_useful
        else "UNKNOWN_PUBLIC_04B6_BENCHMARK_INSUFFICIENT_OR_DIRTY_LOCAL_ONLY",
    }


def write_markdown(scorecard: dict[str, Any], path: Path) -> None:
    decision = scorecard["decision"]
    public_pnl = scorecard["public_pnl_proxy"]
    pairish = scorecard["pairish_buy_only"]
    fee = scorecard["fee_model_public_upper_bound"]
    lines = [
        "# Xuan 04b6 Public Benchmark Probe",
        "",
        f"- status: `{scorecard['status']}`",
        f"- wallet: `{scorecard['wallet']}`",
        f"- profile: {scorecard['profile_url']}",
        f"- window: `{scorecard['window']['start_iso']}` to `{scorecard['window']['latest_iso']}`",
        f"- research_only: `{scorecard['research_only']}`",
        f"- private_truth_ready: `{scorecard['private_truth_ready']}`",
        f"- deployable: `{scorecard['deployable']}`",
        "",
        "## Public Proxy Metrics",
        "",
        f"- trade_buy_usdc: `{public_pnl['trade_buy_usdc']:.6f}`",
        f"- redeem_usdc: `{public_pnl['redeem_usdc']:.6f}`",
        f"- current_window_position_value: `{public_pnl['current_window_position_value']:.6f}`",
        f"- simple_public_mtm_pnl: `{public_pnl['simple_public_mtm_pnl']:.6f}`",
        f"- simple_public_mtm_roi_on_trade_usdc: `{public_pnl['simple_public_mtm_roi_on_trade_usdc']:.6f}`",
        f"- btc_trade_share: `{scorecard['activity_window']['btc_trade_share']:.6f}`",
        f"- timeframes: `{json.dumps(scorecard['activity_window']['timeframes'], sort_keys=True)}`",
        "",
        "## Pairish Buy-Only Shape",
        "",
        f"- paired_condition_count: `{pairish['paired_condition_count']}`",
        f"- weighted_pair_cost_avg_all_buys: `{pairish['weighted_pair_cost_avg_all_buys']:.6f}`",
        f"- residual_qty_share_all_buys: `{pairish['residual_qty_share_all_buys']:.6f}`",
        "",
        "## Fee Boundary",
        "",
        f"- taker_fee_rate: `{fee['taker_fee_rate']}`",
        f"- all_taker_fee_upper_bound: `{fee['all_trade_rows_treated_as_taker_fee_upper_bound']:.6f}`",
        f"- fee_upper_bound_on_trade_usdc: `{fee['fee_upper_bound_on_trade_usdc']:.6f}`",
        "- maker fee is zero per Polymarket docs, but maker/taker status is not proven by this public API.",
        "",
        "## Decision",
        "",
        f"- public_benchmark_useful_for_xuan: `{decision['public_benchmark_useful_for_xuan']}`",
        f"- hard_blockers: `{json.dumps(decision['hard_blockers'], sort_keys=True)}`",
        f"- caveats: `{json.dumps(decision['caveats'], sort_keys=True)}`",
        "",
        "## Xuan Implications",
        "",
    ]
    lines.extend(f"- `{item}`" for item in decision["xuan_implications"])
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--window-hours", type=float, default=12.0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=8)
    parser.add_argument("--timeout-s", type=float, default=25.0)
    parser.add_argument("--taker-fee-rate", type=float, default=0.07)
    parser.add_argument("--min-trade-usdc", type=float, default=10_000.0)
    parser.add_argument("--min-paired-conditions", type=int, default=5)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.15)
    parser.add_argument("--min-btc-trade-share", type=float, default=0.9)
    parser.add_argument("--scorecard-root", type=Path, default=DEFAULT_SCORECARD_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--stamp", default=utc_stamp())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scorecard = build_scorecard(args)
    args.scorecard_root.mkdir(parents=True, exist_ok=True)
    artifact_dir = args.artifact_root / f"xuan_public_04b6_benchmark_probe_{args.stamp}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    scorecard_path = (
        args.scorecard_root / f"xuan_public_04b6_benchmark_probe_{args.stamp}.json"
    )
    markdown_path = artifact_dir / "PUBLIC_04B6_BENCHMARK_PROBE.md"
    scorecard["scorecard_path"] = str(scorecard_path)
    scorecard["markdown_path"] = str(markdown_path)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(scorecard, markdown_path)
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
