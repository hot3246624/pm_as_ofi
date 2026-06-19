#!/usr/bin/env python3
"""Choose the next ce25 fair-price pair-completion profile from local evidence."""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    num = fnum(value)
    return int(num) if num is not None else default


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def market_bucket(slug: str) -> tuple[str, str]:
    lower = slug.lower()
    if lower.startswith("btc-") or "bitcoin" in lower:
        asset = "BTC"
    elif lower.startswith("eth-") or "ethereum" in lower:
        asset = "ETH"
    elif lower.startswith("sol-") or "solana" in lower:
        asset = "SOL"
    elif lower.startswith("xrp-"):
        asset = "XRP"
    else:
        asset = "OTHER"
    if "updown-5m-" in lower:
        timeframe = "5m"
    elif "updown-15m-" in lower:
        timeframe = "15m"
    elif re.search(r"\b1h\b|hour", lower):
        timeframe = "1h"
    else:
        timeframe = "other"
    return asset, timeframe


def aggregate_buckets(event_diag: dict[str, Any]) -> list[dict[str, Any]]:
    per_slug = event_diag.get("per_slug_event_counts")
    if not isinstance(per_slug, dict):
        return []
    buckets: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for slug, counts in per_slug.items():
        if not isinstance(counts, dict):
            continue
        bucket = market_bucket(str(slug))
        for key, value in counts.items():
            buckets[bucket][str(key)] += inum(value)
    rows: list[dict[str, Any]] = []
    for (asset, timeframe), counts in sorted(buckets.items()):
        candidates = counts.get("candidate", 0)
        fills = counts.get("queue_supported_fill", 0)
        rescues = counts.get("fak_salvage", 0)
        rows.append(
            {
                "asset": asset,
                "timeframe": timeframe,
                "candidate": candidates,
                "queue_supported_fill": fills,
                "strict_rescue_or_salvage": rescues,
                "fair_price_admission_block": counts.get("fair_price_admission_block", 0),
                "pair_completion_block": counts.get("pair_completion_block", 0),
                "activation_block": counts.get("activation_block", 0),
                "score": candidates * 3 + fills * 5 + rescues * 8,
            }
        )
    return sorted(rows, key=lambda row: (-row["score"], row["asset"], row["timeframe"]))


def stat_count(diag: dict[str, Any], path: str) -> int:
    current: Any = diag
    for part in path.split("."):
        if not isinstance(current, dict):
            return 0
        current = current.get(part)
    return inum(current)


def stat_max(diag: dict[str, Any], key: str) -> float | None:
    raw = diag.get(key)
    if isinstance(raw, dict):
        return fnum(raw.get("max"))
    return None


def density_status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def build(args: argparse.Namespace) -> dict[str, Any]:
    latest_event = load_json(args.latest_event_diagnostics)
    latest_density = load_json(args.latest_density_gate)
    reference_event = load_json(args.reference_event_diagnostics)
    reference_density = load_json(args.reference_density_gate)

    latest_observed = latest_density.get("observed") if isinstance(latest_density.get("observed"), dict) else {}
    latest_decision = latest_density.get("decision") if isinstance(latest_density.get("decision"), dict) else {}
    latest_candidates = inum(latest_observed.get("candidate_rows"), stat_count(latest_event, "event_counts.candidate"))
    latest_fills = inum(latest_observed.get("queue_supported_fills"), stat_count(latest_event, "event_counts.queue_supported_fill"))
    latest_pair_pnl = fnum(latest_observed.get("pair_pnl"), 0.0) or 0.0
    latest_pair_completion_blocks = inum(
        latest_observed.get("pair_completion_blocks"),
        stat_count(latest_event, "event_counts.pair_completion_block"),
    )
    latest_fair_blocks = inum(
        latest_observed.get("fair_price_admission_blocks"),
        stat_count(latest_event, "event_counts.fair_price_admission_block"),
    )
    latest_density_blocked = density_status(latest_density).startswith("BLOCKED")

    pair_completion_pnl_max = stat_max(latest_event, "pair_completion_block_projected_pair_pnl_after")
    fair_block_min = None
    fair_block_stats = latest_event.get("fair_price_block_pair_cost_after_fee")
    if isinstance(fair_block_stats, dict):
        fair_block_min = fnum(fair_block_stats.get("min"))

    reference_buckets = aggregate_buckets(reference_event)
    best_buckets = [row for row in reference_buckets if row["score"] > 0][: args.max_recommended_buckets]
    reference_density_passed = density_status(reference_density).startswith("KEEP")

    option_assessments: list[dict[str, Any]] = []
    same_profile_blockers: list[str] = []
    if latest_density_blocked:
        same_profile_blockers.append("latest_density_gate_blocked")
    if latest_candidates < args.min_candidates:
        same_profile_blockers.append("latest_candidate_count_below_min")
    if latest_fills < args.min_fills:
        same_profile_blockers.append("latest_fill_count_below_min")
    option_assessments.append(
        {
            "option": "repeat_same_cap099_pair_completion_profile",
            "decision": "BLOCK" if same_profile_blockers else "ALLOW_WITH_FUTURE_HEARTBEAT",
            "blockers": same_profile_blockers,
            "reason": "same profile just produced a zero-density window" if same_profile_blockers else "latest density was adequate",
        }
    )

    cap0985_blockers: list[str] = []
    if latest_candidates == 0:
        cap0985_blockers.append("cap099_had_zero_candidates_so_cap0985_cannot_increase_density")
    if fair_block_min is not None and fair_block_min > 0.99 + 1e-12:
        cap0985_blockers.append("latest_fair_price_blocks_start_above_cap099")
    option_assessments.append(
        {
            "option": "tighten_to_cap0985_same_window",
            "decision": "BLOCK" if cap0985_blockers else "ALLOW_LOCAL_PROFILE_PREP_ONLY",
            "blockers": cap0985_blockers,
            "reason": "tightening after a zero-candidate cap099 run is expected to starve further"
            if cap0985_blockers
            else "cap0985 may reduce risk but still needs density preflight",
        }
    )

    lower_min_qty_blockers: list[str] = []
    if latest_pair_completion_blocks and (pair_completion_pnl_max is None or pair_completion_pnl_max <= args.min_pair_completion_pnl + 1e-12):
        lower_min_qty_blockers.append("pair_completion_blocks_have_no_positive_projected_pair_pnl")
    if latest_candidates == 0 and latest_pair_completion_blocks:
        lower_min_qty_blockers.append("lowering_min_qty_would_be_the_only_density_source")
    option_assessments.append(
        {
            "option": "lower_pair_completion_min_qty",
            "decision": "BLOCK" if lower_min_qty_blockers else "ALLOW_LOCAL_COUNTERFACTUAL_ONLY",
            "blockers": lower_min_qty_blockers,
            "reason": "do not admit zero-pair-pnl completion blocks to chase density"
            if lower_min_qty_blockers
            else "requires a local counterfactual before any remote",
        }
    )

    market_selection_blockers: list[str] = []
    if not best_buckets:
        market_selection_blockers.append("no_reference_bucket_with_positive_density")
    option_assessments.append(
        {
            "option": "adjust_market_timeframe_selection",
            "decision": "ALLOW_LOCAL_PROFILE_PREP_ONLY" if best_buckets else "BLOCK",
            "blockers": market_selection_blockers,
            "recommended_buckets": best_buckets,
            "reason": "use prior observed density to avoid spending 30m on inactive exact slugs",
        }
    )

    hard_blockers: list[str] = []
    if latest_density_blocked:
        hard_blockers.append("latest_density_gate_blocked")
    if latest_candidates == 0:
        hard_blockers.append("latest_zero_candidates")
    if latest_fills == 0:
        hard_blockers.append("latest_zero_fills")
    if latest_pair_completion_blocks and (pair_completion_pnl_max is None or pair_completion_pnl_max <= args.min_pair_completion_pnl + 1e-12):
        hard_blockers.append("latest_pair_completion_blocks_not_economic")

    next_action = (
        "prepare_local_market_timeframe_profile_with_density_preflight"
        if best_buckets
        else "wait_for_new_density_signal_before_remote"
    )
    status = (
        "KEEP_CE25_NEXT_PROFILE_DECISION_LOCAL_PROFILE_PREP_READY"
        if best_buckets
        else "BLOCKED_CE25_NEXT_PROFILE_DECISION_NO_SAFE_REMOTE_PROFILE_LOCAL_ONLY"
    )

    return {
        "artifact": "xuan_ce25_next_profile_decision_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_ce25_next_profile_decision_scorer.py",
        "status": status,
        "inputs": {
            "latest_event_diagnostics": str(Path(args.latest_event_diagnostics).expanduser()),
            "latest_density_gate": str(Path(args.latest_density_gate).expanduser()),
            "reference_event_diagnostics": str(Path(args.reference_event_diagnostics).expanduser())
            if args.reference_event_diagnostics
            else None,
            "reference_density_gate": str(Path(args.reference_density_gate).expanduser())
            if args.reference_density_gate
            else None,
        },
        "latest_window": {
            "candidate_rows": latest_candidates,
            "queue_supported_fills": latest_fills,
            "pair_pnl": round(latest_pair_pnl, 12),
            "fair_price_admission_blocks": latest_fair_blocks,
            "pair_completion_blocks": latest_pair_completion_blocks,
            "pair_completion_block_projected_pair_pnl_after_max": pair_completion_pnl_max,
            "fair_price_block_pair_cost_after_fee": latest_event.get("fair_price_block_pair_cost_after_fee"),
            "density_status": density_status(latest_density),
            "density_blockers": latest_decision.get("hard_blockers"),
        },
        "reference_window": {
            "density_status": density_status(reference_density),
            "density_passed": reference_density_passed,
            "bucket_density": reference_buckets,
        },
        "option_assessments": option_assessments,
        "decision": {
            "next_action": next_action,
            "same_profile_remote_repeat_allowed": False,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers_for_immediate_remote": hard_blockers,
            "recommended_buckets": best_buckets,
        },
        "guardrails": [
            "Do not widen above cap099 as a promotion path.",
            "Do not lower pair-completion min_qty when projected pair PnL after completion is non-positive.",
            "Do not rerun the same exact-slug profile after a zero-density window.",
            "Any future remote remains PM_DRY_RUN=1, 1800s max, one run per wake, and requires fresh slugs/admission JSONL plus density rationale.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest-event-diagnostics", required=True)
    parser.add_argument("--latest-density-gate", required=True)
    parser.add_argument("--reference-event-diagnostics")
    parser.add_argument("--reference-density-gate")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-candidates", type=int, default=5)
    parser.add_argument("--min-fills", type=int, default=1)
    parser.add_argument("--min-pair-completion-pnl", type=float, default=0.0)
    parser.add_argument("--max-recommended-buckets", type=int, default=4)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
