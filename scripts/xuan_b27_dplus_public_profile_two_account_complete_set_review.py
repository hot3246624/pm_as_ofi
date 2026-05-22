#!/usr/bin/env python3
"""Public/proxy two-account complete-set strategy review for D+ research.

This uses only public Polymarket pages/API responses. Public accounts are proxy
inspiration only; this script never reads private owner-trade truth, raw/replay
stores, collector paths, shared ingress, broker state, or trading paths.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.parse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from xuan_b27_dplus_complete_set_pair_cost_context_verifier import (
    market_family,
    normalize_outcome,
    score_public_trades,
    timeframe_s,
)
from xuan_b27_dplus_public_profile_strategy_review import (
    summarize_activity,
    summarize_positions,
)


ARTIFACT = "xuan_b27_dplus_public_profile_two_account_complete_set_review"
DATA_API = "https://data-api.polymarket.com"
DEFAULT_PROFILES = (
    "0x04b6d7e930cf9e493c5e6ef24b496294f95594c8|"
    "https://polymarket.com/zh/@0x04b6d7e930cf9e493c5e6ef24b496294f95594c8-1774448369789",
    "0x9f5ffe76a818dce37c70f947998b52b70671a008|"
    "https://polymarket.com/zh/@0x9f5ffe76a818dce37c70f947998b52b70671a008-1772728605528",
)

FORBIDDEN_OUTPUT_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "collector",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    resolved = str(path.resolve())
    return not any(fragment in resolved for fragment in FORBIDDEN_OUTPUT_FRAGMENTS)


def curl_json(url: str) -> Any:
    cmd = [
        "curl",
        "-sS",
        "-L",
        "--retry",
        "3",
        "--retry-delay",
        "1",
        "--connect-timeout",
        "10",
        "--max-time",
        "45",
        "-H",
        "User-Agent: Mozilla/5.0 xuan-research-public-review",
        url,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(proc.stdout)


def fetch_paged(
    endpoint: str,
    profile: str,
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
        params = {"user": profile, "limit": str(limit), "offset": str(offset)}
        if extra:
            params.update(extra)
        url = f"{DATA_API}/{endpoint}?{urllib.parse.urlencode(params)}"
        urls.append(url)
        try:
            page = curl_json(url)
        except Exception as exc:
            errors.append(f"{type(exc).__name__} at offset={offset}: {exc}")
            if rows:
                break
            raise
        if not isinstance(page, list):
            raise RuntimeError(f"unexpected {endpoint} payload type {type(page).__name__}")
        rows.extend(page)
        if len(page) < limit:
            break
        offset += len(page)
        time.sleep(0.12)
    return rows, urls, errors


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return round(float(ordered[idx]), 8)


def ratio(num: float, den: float) -> float | None:
    return round(float(num) / float(den), 8) if den else None


def public_trade_profile_features(trades: list[dict[str, Any]]) -> dict[str, Any]:
    prices = [float(row.get("price") or 0.0) for row in trades]
    sizes = [float(row.get("size") or 0.0) for row in trades]
    notional = [float(row.get("usdcSize") or 0.0) for row in trades]
    side_counts = Counter(str(row.get("side") or "") for row in trades)
    outcome_counts = Counter(normalize_outcome(row.get("outcome")) for row in trades)
    timeframe_counts = Counter(str(timeframe_s(str(row.get("slug") or ""))) for row in trades)
    family_counts = Counter(market_family(str(row.get("slug") or "")) for row in trades)
    condition_outcomes: dict[str, set[str]] = {}
    for row in trades:
        condition_id = str(row.get("conditionId") or "")
        condition_outcomes.setdefault(condition_id, set()).add(normalize_outcome(row.get("outcome")))
    both_outcome_conditions = sum(1 for outcomes in condition_outcomes.values() if {"Up", "Down"}.issubset(outcomes))
    return {
        "trade_count": len(trades),
        "condition_count": len(condition_outcomes),
        "both_outcome_condition_count": both_outcome_conditions,
        "both_outcome_condition_ratio": ratio(both_outcome_conditions, len(condition_outcomes)),
        "side_counts": dict(side_counts),
        "outcome_counts": dict(outcome_counts),
        "timeframe_s_trade_count": dict(sorted(timeframe_counts.items())),
        "market_family_trade_count": dict(sorted(family_counts.items())),
        "buy_trade_ratio": ratio(side_counts.get("BUY", 0), len(trades)),
        "five_min_trade_ratio": ratio(timeframe_counts.get("300", 0), len(trades)),
        "fifteen_min_trade_ratio": ratio(timeframe_counts.get("900", 0), len(trades)),
        "price_min": pct(prices, 0.0),
        "price_p25": pct(prices, 0.25),
        "price_p50": pct(prices, 0.50),
        "price_p75": pct(prices, 0.75),
        "price_max": pct(prices, 1.0),
        "size_sum": round(sum(sizes), 8),
        "notional_sum": round(sum(notional), 8),
    }


def classify_profile(features: dict[str, Any], pair_score: dict[str, Any], positions: dict[str, Any]) -> dict[str, Any]:
    pair_edge = float(pair_score.get("weighted_pair_edge") or 0.0)
    both_ratio = float(pair_score.get("both_outcome_condition_ratio") or 0.0)
    residual_share = float(pair_score.get("unpaired_residual_qty_share_of_public_size") or 0.0)
    five_min_share = float(features.get("five_min_trade_ratio") or 0.0)
    trade_count = int(features.get("trade_count") or 0)
    positive_proxy = trade_count >= 1000 and both_ratio >= 0.8 and pair_edge > 0.0
    dplus_timeframe_relevant = five_min_share >= 0.25
    if positive_proxy and dplus_timeframe_relevant:
        label = "STRONG_PUBLIC_PROXY_POSITIVE_COMPLETE_SET_PAIRING"
    elif positive_proxy:
        label = "PUBLIC_PROXY_POSITIVE_COMPLETE_SET_PAIRING_TIMEFRAME_MIXED"
    elif trade_count >= 1000 and residual_share >= 0.40:
        label = "NEGATIVE_CONTROL_HIGH_TURNOVER_RESIDUAL_HEAVY"
    else:
        label = "UNKNOWN_PUBLIC_PROXY_PROFILE_SIGNAL_MIXED"
    return {
        "profile_label": label,
        "positive_complete_set_pair_edge": pair_edge > 0.0,
        "both_outcome_complete_set_proxy": both_ratio >= 0.8,
        "dplus_5m_relevance_proxy": dplus_timeframe_relevant,
        "residual_heavy": residual_share >= 0.20,
        "current_position_both_side_share": positions.get("both_side_condition_share"),
        "interpretation": {
            "can_help_dplus": label.startswith("STRONG") or label.startswith("PUBLIC_PROXY_POSITIVE"),
            "directly_copyable_rule": False,
            "reason": (
                "Useful as public/proxy complete-set and condition-ledger structure; not a private-truth "
                "or deployable strategy because public activity lacks order intent, queue/cancel state, and our own fills."
            ),
        },
    }


def parse_profile(value: str) -> tuple[str, str]:
    if "|" in value:
        wallet, url = value.split("|", 1)
        return wallet.strip().lower(), url.strip()
    wallet = value.strip().lower()
    return wallet, f"https://polymarket.com/zh/@{wallet}"


def review_profile(
    wallet: str,
    profile_url: str,
    *,
    output_dir: Path,
    trades_max_rows: int,
    positions_max_rows: int,
    page_size: int,
    fee_rate: float,
) -> dict[str, Any]:
    trades, trade_urls, trade_errors = fetch_paged(
        "activity",
        wallet,
        max_rows=trades_max_rows,
        page_size=page_size,
        extra={"type": "TRADE"},
    )
    positions, position_urls, position_errors = fetch_paged(
        "positions",
        wallet,
        max_rows=positions_max_rows,
        page_size=page_size,
    )
    profile_dir = output_dir / "public_inputs" / wallet
    write_json(profile_dir / "activity_trade_rows.json", trades)
    write_json(profile_dir / "positions_rows.json", positions)

    features = public_trade_profile_features(trades)
    pair_score = score_public_trades(trades, fee_rate=fee_rate)
    activity_summary = summarize_activity(trades)
    position_summary = summarize_positions(positions)
    classification = classify_profile(features, pair_score, position_summary)
    return {
        "wallet": wallet,
        "profile_url": profile_url,
        "public_sources": {
            "profile_url": profile_url,
            "activity_urls_sample": trade_urls[:2],
            "positions_urls_sample": position_urls[:2],
            "activity_fetch_errors": trade_errors,
            "positions_fetch_errors": position_errors,
        },
        "raw_public_input_paths_untracked": {
            "activity_trade_rows": str(profile_dir / "activity_trade_rows.json"),
            "positions_rows": str(profile_dir / "positions_rows.json"),
        },
        "activity_sample": {
            "requested_trade_rows": trades_max_rows,
            "fetched_trade_rows": len(trades),
            "requested_position_rows": positions_max_rows,
            "fetched_position_rows": len(positions),
        },
        "public_activity_features": features,
        "observed_activity_patterns": activity_summary,
        "observed_position_patterns": position_summary,
        "complete_set_pair_cost_proxy": pair_score,
        "profile_classification": classification,
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    profiles = [parse_profile(value) for value in args.profile]
    reviews = [
        review_profile(
            wallet,
            url,
            output_dir=output_dir,
            trades_max_rows=args.trades_max_rows,
            positions_max_rows=args.positions_max_rows,
            page_size=args.page_size,
            fee_rate=args.fee_rate,
        )
        for wallet, url in profiles
    ]
    strong_profiles = [
        review
        for review in reviews
        if review["profile_classification"]["profile_label"].startswith("STRONG")
        or review["profile_classification"]["profile_label"].startswith("PUBLIC_PROXY_POSITIVE")
    ]
    negative_controls = [
        review
        for review in reviews
        if review["profile_classification"]["profile_label"].startswith("NEGATIVE_CONTROL")
    ]
    decision = "KEEP" if strong_profiles else "UNKNOWN"
    label = (
        "KEEP_PUBLIC_PROFILE_TWO_ACCOUNT_COMPLETE_SET_PROXY_REVIEW_READY"
        if strong_profiles
        else "UNKNOWN_PUBLIC_PROFILE_TWO_ACCOUNT_COMPLETE_SET_PROXY_SIGNAL_WEAK"
    )
    best_positive = max(
        strong_profiles,
        key=lambda review: float(review["complete_set_pair_cost_proxy"].get("weighted_pair_edge") or 0.0),
        default=None,
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "public_profile_review",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "public_web_api_only": True,
            "public_profile_proxy_truth_only": True,
            "learning_target_private_truth_used": False,
            "learning_target_private_truth_required": False,
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
        "fee_policy": {
            "fee_rate": args.fee_rate,
            "fee_rate_source": args.fee_rate_source,
            "fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "account_average_fee_rate_used": False,
        },
        "profiles_reviewed": reviews,
        "cross_profile_summary": {
            "profile_count": len(reviews),
            "positive_complete_set_proxy_profiles": len(strong_profiles),
            "negative_control_profiles": len(negative_controls),
            "best_positive_profile_wallet": best_positive["wallet"] if best_positive else None,
            "best_positive_profile_label": best_positive["profile_classification"]["profile_label"] if best_positive else None,
            "combined_trade_rows": sum(review["activity_sample"]["fetched_trade_rows"] for review in reviews),
            "combined_position_rows": sum(review["activity_sample"]["fetched_position_rows"] for review in reviews),
        },
        "interpretation": {
            "does_this_help_us": bool(strong_profiles),
            "answer": (
                "Yes, as public/proxy inspiration: at least one account shows high-frequency two-sided "
                "complete-set pairing with positive weighted pair edge and material 5m relevance. The second "
                "account is useful as a negative control because high turnover alone has poor residual and pair-cost proxy."
                if strong_profiles
                else "Not enough public/proxy signal from these accounts to select a D+ local verifier target."
            ),
            "not_evidence_for": [
                "learning target private truth",
                "our private order/fill/inventory truth",
                "deployable",
                "shadow-ready",
                "canary",
                "promotion",
            ],
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "hypothesis": {
                "name": "public_profile_positive_complete_set_condition_ledger",
                "selected_next_target": "public_profile_pairing_verifier_revisit_v1",
                "description": (
                    "Use the stronger public positive-edge complete-set proxy as inspiration to revisit the local "
                    "condition-ledger paired-inventory ranker. The local verifier must preserve sample and improve "
                    "residual/stress metrics on allowed candidate_base only."
                ),
                "guardrails": [
                    "Do not copy public account trades directly.",
                    "Do not use public/profile data as private truth or canary evidence.",
                    "Avoid summary-only removal, static side/offset cutoff, L1/public price caps, cooldown/admission caps, and broad replay search.",
                ],
            },
            "profile_labels": {
                review["wallet"]: review["profile_classification"]["profile_label"] for review in reviews
            },
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_PROFILE_PROXY_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Run local-only public_profile_pairing_verifier_revisit_v1 using allowed candidate_base/state-machine "
            "and this stronger two-account proxy review. KEEP requires sample/pair retention, residual-rate reduction, "
            "positive stress/worst-day PnL, and private/deployable/promotion guards false."
            if strong_profiles
            else "Return UNKNOWN and wait for same-window/action-level fields; do not start shadow/canary."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", action="append", default=list(DEFAULT_PROFILES))
    parser.add_argument("--trades-max-rows", type=int, default=3000)
    parser.add_argument("--positions-max-rows", type=int, default=1000)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument(
        "--fee-rate-source",
        default="explicit_public_proxy_complete_set_fee_rate_zero_for_profile_scoring",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args, args.output_dir)
    write_json(args.output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(args.output_dir / "manifest.json"),
                "positive_complete_set_proxy_profiles": manifest["cross_profile_summary"][
                    "positive_complete_set_proxy_profiles"
                ],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
