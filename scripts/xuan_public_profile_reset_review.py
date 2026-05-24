#!/usr/bin/env python3
"""Fresh public Polymarket profile reset review.

This script is intentionally independent from the failed D+ micro-deficit and
ledger research branches. It uses only public Polymarket profile pages and
public API endpoints to extract profile-level motifs that can seed a new local
research lane. Public accounts are proxy inspiration only.
"""

from __future__ import annotations

import argparse
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

from xuan_b27_dplus_complete_set_pair_cost_context_verifier import (
    market_family,
    normalize_outcome,
    score_public_trades,
    timeframe_s,
)
from xuan_b27_dplus_public_profile_strategy_review import summarize_positions


ARTIFACT = "xuan_public_profile_reset_review"
DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_PROFILE = "https://polymarket.com/zh/@{handle}"
DEFAULT_PROFILES = (
    "https://polymarket.com/zh/@0x04b6d7e930cf9e493c5e6ef24b496294f95594c8-1774448369789",
    "https://polymarket.com/zh/@0x9f5ffe76a818dce37c70f947998b52b70671a008-1772728605528",
    "https://polymarket.com/zh/@0x8dxd",
    "https://polymarket.com/zh/@gabagool22",
)
FORBIDDEN_OUTPUT_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_OUTPUT_FRAGMENTS)


def fetch_text(url: str, *, timeout: int = 40, retries: int = 3) -> str:
    last_exc: Exception | None = None
    for attempt in range(retries):
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 xuan-public-profile-reset-review",
                "Accept": "application/json,text/html,text/plain,*/*",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except Exception as exc:
            last_exc = exc
            if attempt + 1 >= retries:
                break
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"fetch failed after {retries} attempts for {url}: {last_exc}")


def fetch_json(url: str, *, timeout: int = 40) -> Any:
    return json.loads(fetch_text(url, timeout=timeout))


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return round(float(ordered[idx]), 8)


def ratio(num: float, den: float) -> float | None:
    return round(float(num) / float(den), 8) if den else None


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_profile_slug(raw: str) -> tuple[str, str]:
    text = raw.strip()
    if text.startswith("http://") or text.startswith("https://"):
        parsed = urllib.parse.urlparse(text)
        tail = urllib.parse.unquote(parsed.path.rstrip("/").split("/")[-1])
        if tail.startswith("@"):
            tail = tail[1:]
        return tail, text
    return text[1:] if text.startswith("@") else text, POLYMARKET_PROFILE.format(handle=text.lstrip("@"))


def extract_next_data(page_html: str) -> dict[str, Any]:
    match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', page_html, re.S)
    if not match:
        return {}
    try:
        return json.loads(html.unescape(match.group(1)))
    except json.JSONDecodeError:
        return {}


def find_proxy_wallet_in_json(value: Any) -> str | None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "proxyWallet" and isinstance(item, str) and re.fullmatch(r"0x[a-fA-F0-9]{40}", item):
                return item.lower()
            found = find_proxy_wallet_in_json(item)
            if found:
                return found
    if isinstance(value, list):
        for item in value:
            found = find_proxy_wallet_in_json(item)
            if found:
                return found
    return None


def resolve_profile(raw: str) -> dict[str, Any]:
    slug, profile_url = parse_profile_slug(raw)
    wallet_match = re.search(r"0x[a-fA-F0-9]{40}", slug)
    if wallet_match:
        wallet = wallet_match.group(0).lower()
        return {
            "input": raw,
            "handle": slug,
            "profile_url": profile_url,
            "proxy_wallet": wallet,
            "resolution_source": "profile_slug_wallet_prefix",
            "gamma_profile": None,
        }

    query = urllib.parse.urlencode({"q": slug, "search_profiles": "true", "limit_per_type": "10"})
    gamma_url = f"{GAMMA_API}/public-search?{query}"
    gamma = fetch_json(gamma_url)
    profiles = gamma.get("profiles") if isinstance(gamma, dict) else []
    exact = None
    if isinstance(profiles, list):
        for candidate in profiles:
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("name") or "").lower() == slug.lower():
                exact = candidate
                break
        if exact is None and profiles:
            exact = next((p for p in profiles if isinstance(p, dict)), None)
    if exact and re.fullmatch(r"0x[a-fA-F0-9]{40}", str(exact.get("proxyWallet") or "")):
        return {
            "input": raw,
            "handle": str(exact.get("name") or slug),
            "profile_url": profile_url,
            "proxy_wallet": str(exact["proxyWallet"]).lower(),
            "resolution_source": "gamma_public_search_exact_or_first_profile",
            "gamma_profile": exact,
            "gamma_url": gamma_url,
        }

    page_html = fetch_text(profile_url)
    next_data = extract_next_data(page_html)
    wallet = find_proxy_wallet_in_json(next_data)
    if not wallet:
        wallets = re.findall(r"0x[a-fA-F0-9]{40}", page_html)
        wallet = Counter(w.lower() for w in wallets).most_common(1)[0][0] if wallets else None
    if not wallet:
        raise RuntimeError(f"could not resolve proxy wallet for {raw}")
    return {
        "input": raw,
        "handle": slug,
        "profile_url": profile_url,
        "proxy_wallet": wallet,
        "resolution_source": "polymarket_profile_page_next_data_or_html",
        "gamma_profile": None,
    }


def data_url(endpoint: str, params: dict[str, Any]) -> str:
    return f"{DATA_API}/{endpoint}?{urllib.parse.urlencode(params)}"


def fetch_paged(
    endpoint: str,
    wallet: str,
    *,
    max_rows: int,
    page_size: int,
    extra: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
    errors: list[str] = []
    offset = 0
    while len(rows) < max_rows:
        limit = min(page_size, max_rows - len(rows))
        params: dict[str, Any] = {"user": wallet, "limit": limit, "offset": offset}
        if extra:
            params.update(extra)
        url = data_url(endpoint, params)
        urls.append(url)
        try:
            page = fetch_json(url)
        except Exception as exc:  # public API can rate-limit or return transient errors
            errors.append(f"{type(exc).__name__} offset={offset}: {exc}")
            break
        if not isinstance(page, list):
            errors.append(f"non_list_response offset={offset}: {type(page).__name__}")
            break
        rows.extend(row for row in page if isinstance(row, dict))
        if len(page) < limit:
            break
        offset += len(page)
        time.sleep(0.12)
    return rows, urls, errors


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


def dedup(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = activity_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def market_label(row: dict[str, Any]) -> str:
    slug = str(row.get("slug") or row.get("eventSlug") or "")
    if slug.startswith("btc-updown-5m"):
        return "btc_updown_5m"
    if slug.startswith("btc-updown-15m"):
        return "btc_updown_15m"
    if slug.startswith("btc-updown"):
        return "btc_updown_other"
    if slug.startswith("eth-updown") or slug.startswith("ethereum-updown"):
        return "eth_updown"
    family = market_family(slug)
    return family or "unknown"


def timestamp_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = sorted(int(row.get("timestamp") or 0) for row in rows if row.get("timestamp"))
    interarrival = [
        float(b - a)
        for a, b in zip(timestamps, timestamps[1:])
        if b >= a and b - a <= 3600
    ]
    by_day: Counter[str] = Counter()
    for ts in timestamps:
        by_day[datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")] += 1
    return {
        "timestamp_min": timestamps[0] if timestamps else None,
        "timestamp_max": timestamps[-1] if timestamps else None,
        "timestamp_min_utc": datetime.fromtimestamp(timestamps[0], timezone.utc).isoformat() if timestamps else None,
        "timestamp_max_utc": datetime.fromtimestamp(timestamps[-1], timezone.utc).isoformat() if timestamps else None,
        "active_day_count": len(by_day),
        "top_trade_days": by_day.most_common(10),
        "interarrival_s_p10": pct(interarrival, 0.10),
        "interarrival_s_p50": pct(interarrival, 0.50),
        "interarrival_s_p90": pct(interarrival, 0.90),
        "interarrival_lte_2s_share": ratio(sum(1 for value in interarrival if value <= 2.0), len(interarrival)),
        "interarrival_lte_10s_share": ratio(sum(1 for value in interarrival if value <= 10.0), len(interarrival)),
    }


def summarize_trade_rows(trades: list[dict[str, Any]]) -> dict[str, Any]:
    prices = [as_float(row.get("price")) for row in trades]
    sizes = [as_float(row.get("size")) for row in trades]
    usdc = [as_float(row.get("usdcSize")) for row in trades]
    side_counts = Counter(str(row.get("side") or "") for row in trades)
    outcome_counts = Counter(normalize_outcome(row.get("outcome")) for row in trades)
    market_counts = Counter(market_label(row) for row in trades)
    timeframe_counts = Counter(str(timeframe_s(str(row.get("slug") or ""))) for row in trades)
    condition_outcomes: dict[str, set[str]] = defaultdict(set)
    condition_trade_counts: Counter[str] = Counter()
    rounded_size_counts: Counter[str] = Counter()
    rounded_price_counts: Counter[str] = Counter()
    for row in trades:
        condition_id = str(row.get("conditionId") or "")
        condition_outcomes[condition_id].add(normalize_outcome(row.get("outcome")))
        condition_trade_counts[condition_id] += 1
        rounded_size_counts[f"{as_float(row.get('size')):.2f}"] += 1
        rounded_price_counts[f"{as_float(row.get('price')):.3f}"] += 1
    both_outcome_conditions = sum(1 for outcomes in condition_outcomes.values() if {"Up", "Down"}.issubset(outcomes))
    top_size_count = rounded_size_counts.most_common(1)[0][1] if rounded_size_counts else 0
    top_price_count = rounded_price_counts.most_common(1)[0][1] if rounded_price_counts else 0
    return {
        "trade_count": len(trades),
        "unique_condition_count": len(condition_outcomes),
        "both_outcome_condition_count": both_outcome_conditions,
        "both_outcome_condition_ratio": ratio(both_outcome_conditions, len(condition_outcomes)),
        "side_counts": dict(side_counts),
        "outcome_counts": dict(outcome_counts),
        "market_counts": dict(market_counts.most_common(20)),
        "timeframe_s_counts": dict(sorted(timeframe_counts.items())),
        "size_sum": round(sum(sizes), 8),
        "usdc_sum": round(sum(usdc), 8),
        "price_p10": pct(prices, 0.10),
        "price_p50": pct(prices, 0.50),
        "price_p90": pct(prices, 0.90),
        "size_p10": pct(sizes, 0.10),
        "size_p50": pct(sizes, 0.50),
        "size_p90": pct(sizes, 0.90),
        "usdc_p10": pct(usdc, 0.10),
        "usdc_p50": pct(usdc, 0.50),
        "usdc_p90": pct(usdc, 0.90),
        "top_rounded_sizes": rounded_size_counts.most_common(12),
        "top_rounded_size_share": ratio(top_size_count, len(trades)),
        "top_rounded_prices": rounded_price_counts.most_common(12),
        "top_rounded_price_share": ratio(top_price_count, len(trades)),
        "top_condition_trade_counts": condition_trade_counts.most_common(12),
        "time": timestamp_summary(trades),
    }


def summarize_all_activity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    type_counts = Counter(str(row.get("type") or "") for row in rows)
    market_counts = Counter(market_label(row) for row in rows)
    return {
        "activity_count": len(rows),
        "type_counts": dict(type_counts.most_common(20)),
        "market_counts": dict(market_counts.most_common(20)),
        "merge_or_redeem_count": type_counts.get("MERGE", 0) + type_counts.get("REDEEM", 0),
        "maker_rebate_count": type_counts.get("MAKER_REBATE", 0),
        "time": timestamp_summary(rows),
    }


def classify_profile(
    trade_summary: dict[str, Any],
    all_activity_summary: dict[str, Any],
    position_summary: dict[str, Any],
    pair_score: dict[str, Any],
) -> dict[str, Any]:
    trade_count = int(trade_summary.get("trade_count") or 0)
    market_counts = trade_summary.get("market_counts") if isinstance(trade_summary.get("market_counts"), dict) else {}
    btc_5m = int(market_counts.get("btc_updown_5m") or 0)
    btc_15m = int(market_counts.get("btc_updown_15m") or 0)
    short_crypto_share = ratio(btc_5m + btc_15m, trade_count) or 0.0
    both_ratio = as_float(pair_score.get("both_outcome_condition_ratio"))
    residual_share = as_float(pair_score.get("unpaired_residual_qty_share_of_public_size"))
    weighted_edge = as_float(pair_score.get("weighted_pair_edge"), -999.0)
    open_position_count = int(position_summary.get("position_count") or 0)
    merge_or_redeem_count = int(all_activity_summary.get("merge_or_redeem_count") or 0)
    motifs: list[str] = []
    if trade_count >= 1000 and short_crypto_share >= 0.70:
        motifs.append("high_frequency_short_crypto_updown")
    if both_ratio >= 0.65 and weighted_edge > 0:
        motifs.append("positive_complete_set_pairing_proxy")
    if open_position_count == 0 and trade_count >= 1000:
        motifs.append("flat_or_closed_cycle_profile")
    if residual_share <= 0.20 and trade_count >= 1000:
        motifs.append("low_public_pairing_residual")
    if merge_or_redeem_count > 0:
        motifs.append("observable_merge_or_redeem_cycle")
    if as_float(trade_summary.get("top_rounded_size_share")) >= 0.25:
        motifs.append("standardized_clip_ladder")

    if "positive_complete_set_pairing_proxy" in motifs and "high_frequency_short_crypto_updown" in motifs:
        label = "KEEP_PUBLIC_PROFILE_RESET_STRONG_SHORT_HORIZON_PAIRING_PROXY"
        decision = "KEEP"
    elif trade_count >= 1000 and short_crypto_share >= 0.50:
        label = "KEEP_PUBLIC_PROFILE_RESET_SHORT_HORIZON_CONTROL_OR_LEAD"
        decision = "KEEP"
    elif trade_count >= 100:
        label = "UNKNOWN_PUBLIC_PROFILE_RESET_SIGNAL_MIXED"
        decision = "UNKNOWN"
    else:
        label = "DISCARD_PUBLIC_PROFILE_RESET_INSUFFICIENT_ACTIVITY"
        decision = "DISCARD"
    return {
        "decision": decision,
        "label": label,
        "motifs": motifs,
        "short_crypto_trade_share": short_crypto_share,
        "btc_5m_trade_share": ratio(btc_5m, trade_count),
        "btc_15m_trade_share": ratio(btc_15m, trade_count),
        "public_complete_set_weighted_pair_edge": pair_score.get("weighted_pair_edge"),
        "public_complete_set_residual_share": pair_score.get("unpaired_residual_qty_share_of_public_size"),
        "directly_copyable_rule": False,
        "private_truth_ready": False,
        "deployable": False,
    }


def review_profile(
    resolved: dict[str, Any],
    *,
    output_dir: Path,
    trade_max_rows: int,
    activity_max_rows: int,
    position_max_rows: int,
    page_size: int,
    fee_rate: float,
) -> dict[str, Any]:
    wallet = str(resolved["proxy_wallet"]).lower()
    trades, trade_urls, trade_errors = fetch_paged(
        "activity",
        wallet,
        max_rows=trade_max_rows,
        page_size=page_size,
        extra={"type": "TRADE"},
    )
    all_activity, all_urls, all_errors = fetch_paged(
        "activity",
        wallet,
        max_rows=activity_max_rows,
        page_size=page_size,
    )
    positions, position_urls, position_errors = fetch_paged(
        "positions",
        wallet,
        max_rows=position_max_rows,
        page_size=page_size,
    )
    trades = dedup(trades)
    all_activity = dedup(all_activity)
    profile_dir = output_dir / "public_inputs" / wallet
    write_json(profile_dir / "activity_trade_rows.json", trades)
    write_json(profile_dir / "activity_all_rows.json", all_activity)
    write_json(profile_dir / "positions_rows.json", positions)

    trade_summary = summarize_trade_rows(trades)
    all_activity_summary = summarize_all_activity(all_activity)
    position_summary = summarize_positions(positions)
    pair_score = score_public_trades(trades, fee_rate=fee_rate)
    classification = classify_profile(trade_summary, all_activity_summary, position_summary, pair_score)
    return {
        "resolved_profile": resolved,
        "public_sources": {
            "profile_url": resolved.get("profile_url"),
            "gamma_url": resolved.get("gamma_url"),
            "activity_trade_urls_sample": trade_urls[:2],
            "activity_all_urls_sample": all_urls[:2],
            "positions_urls_sample": position_urls[:2],
            "activity_trade_fetch_errors": trade_errors,
            "activity_all_fetch_errors": all_errors,
            "positions_fetch_errors": position_errors,
        },
        "raw_public_input_paths_untracked": {
            "activity_trade_rows": str(profile_dir / "activity_trade_rows.json"),
            "activity_all_rows": str(profile_dir / "activity_all_rows.json"),
            "positions_rows": str(profile_dir / "positions_rows.json"),
        },
        "sample": {
            "trade_max_rows": trade_max_rows,
            "fetched_trade_rows": len(trades),
            "activity_max_rows": activity_max_rows,
            "fetched_activity_rows": len(all_activity),
            "position_max_rows": position_max_rows,
            "fetched_position_rows": len(positions),
        },
        "trade_summary": trade_summary,
        "activity_summary": all_activity_summary,
        "position_summary": position_summary,
        "complete_set_pair_cost_proxy": pair_score,
        "classification": classification,
    }


def build_cross_profile_summary(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(
        reviews,
        key=lambda review: (
            review["classification"]["decision"] == "KEEP",
            as_float(review["complete_set_pair_cost_proxy"].get("weighted_pair_edge"), -999.0),
            int(review["trade_summary"].get("trade_count") or 0),
        ),
        reverse=True,
    )
    motif_counts: Counter[str] = Counter()
    for review in reviews:
        motif_counts.update(review["classification"].get("motifs") or [])
    return {
        "profile_count": len(reviews),
        "profiles_with_keep_signal": sum(1 for review in reviews if review["classification"]["decision"] == "KEEP"),
        "combined_trade_rows": sum(int(review["trade_summary"].get("trade_count") or 0) for review in reviews),
        "combined_activity_rows": sum(int(review["activity_summary"].get("activity_count") or 0) for review in reviews),
        "motif_counts": dict(motif_counts.most_common()),
        "ranked_profile_handles": [
            {
                "handle": review["resolved_profile"].get("handle"),
                "wallet": review["resolved_profile"].get("proxy_wallet"),
                "label": review["classification"].get("label"),
                "trade_count": review["trade_summary"].get("trade_count"),
                "short_crypto_trade_share": review["classification"].get("short_crypto_trade_share"),
                "weighted_pair_edge": review["complete_set_pair_cost_proxy"].get("weighted_pair_edge"),
                "residual_share": review["complete_set_pair_cost_proxy"].get("unpaired_residual_qty_share_of_public_size"),
            }
            for review in ranked
        ],
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    resolved_profiles = [resolve_profile(value) for value in args.profile]
    reviews = [
        review_profile(
            resolved,
            output_dir=output_dir,
            trade_max_rows=args.trade_max_rows,
            activity_max_rows=args.activity_max_rows,
            position_max_rows=args.position_max_rows,
            page_size=args.page_size,
            fee_rate=args.fee_rate,
        )
        for resolved in resolved_profiles
    ]
    cross = build_cross_profile_summary(reviews)
    keep_profiles = int(cross["profiles_with_keep_signal"])
    decision = "KEEP" if keep_profiles else "UNKNOWN"
    label = (
        "KEEP_PUBLIC_PROFILE_RESET_RESEARCH_READY"
        if keep_profiles
        else "UNKNOWN_PUBLIC_PROFILE_RESET_NO_STRONG_PROXY_SIGNAL"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "public_profile_reset_review",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "public_web_api_only": True,
            "public_profile_proxy_truth_only": True,
            "dplus_failed_learning_objects_reused_as_candidates": False,
            "private_owner_trade_truth_used": False,
            "our_private_truth_ready": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "dplus_discard_boundary": {
            "current_dplus_deployable_path": "DISCARD_CURRENT_DPLUS_DEPLOYABLE_PATH",
            "reason": (
                "The strict no-order D+ gate repeatedly produced positive pair PnL but negative risk-adjusted PnL "
                "or failed marker reproduction; this reset review does not continue those families."
            ),
            "frozen_families": [
                "micro_deficit",
                "ledger_after_open_le_1",
                "ledger_before_delta_tiny_deficit",
                "target_or_cooldown_static_deletion",
            ],
        },
        "fee_policy": {
            "fee_rate": args.fee_rate,
            "fee_rate_source": args.fee_rate_source,
            "fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "account_average_fee_rate_used": False,
        },
        "profiles_reviewed": reviews,
        "cross_profile_summary": cross,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "candidate_direction": {
                "name": "fresh_public_profile_closed_cycle_pairing_review",
                "description": (
                    "Start from public high-frequency profiles as closed-cycle inventory/complete-set patterns. "
                    "Do not inherit D+ micro-deficit or ledger tiny-deficit hypotheses. The next local verifier "
                    "must score completed account cycles or paired-inventory economics directly, including residual "
                    "stress, rather than treating gross pair PnL as success."
                ),
                "required_next_checks": [
                    "Use only allowed local candidate/state-machine exports, not raw/replay/full-store scans.",
                    "Separate average pair edge from tail pair-cost and residual inventory risk.",
                    "Require train/holdout stability plus a same-window no-order marker before any shadow.",
                    "Keep private_truth_ready=false, deployable=false, promotion_gate.passed=false.",
                ],
            },
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_PROFILE_RESET_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement local-only closed_cycle_pairing_objective_audit_v1: define success as risk-adjusted closed-cycle "
            "PnL after fees and residual stress, compare profile-inspired motifs across allowed candidate exports, "
            "and reject any candidate that only has positive gross pair PnL while residual stress is negative."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", action="append", default=list(DEFAULT_PROFILES))
    parser.add_argument("--trade-max-rows", type=int, default=6000)
    parser.add_argument("--activity-max-rows", type=int, default=6000)
    parser.add_argument("--position-max-rows", type=int, default=1000)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument(
        "--fee-rate-source",
        default="explicit_public_proxy_fee_rate_zero_for_cross_profile_pattern_scoring",
    )
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    manifest = build_manifest(args, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "profiles_with_keep_signal": manifest["cross_profile_summary"]["profiles_with_keep_signal"],
                "combined_trade_rows": manifest["cross_profile_summary"]["combined_trade_rows"],
                "ranked_profile_handles": manifest["cross_profile_summary"]["ranked_profile_handles"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
