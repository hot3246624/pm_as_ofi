#!/usr/bin/env python3
"""Gate repeated ce25 fair-price pair-completion diagnostics by observed density."""

from __future__ import annotations

import argparse
import json
import math
import time
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


def nested(data: dict[str, Any], *keys: str) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def build(args: argparse.Namespace) -> dict[str, Any]:
    event = load_json(args.event_diagnostics)
    postrun = load_json(args.postrun_bundle)
    metrics = nested(postrun, "runtime_summary", "metrics") or {}
    statuses = postrun.get("scorecard_statuses") if isinstance(postrun.get("scorecard_statuses"), dict) else {}
    event_counts = event.get("event_counts") if isinstance(event.get("event_counts"), dict) else {}
    block_reasons = event.get("block_reason_counts") if isinstance(event.get("block_reason_counts"), dict) else {}

    candidate_rows = inum(event_counts.get("candidate"), inum(metrics.get("accepted_actions")))
    fill_rows = inum(event_counts.get("queue_supported_fill"), inum(metrics.get("queue_supported_fills")))
    rescue_rows = inum(event_counts.get("fak_salvage"), inum(metrics.get("strict_rescue_closes")))
    pair_pnl = fnum(metrics.get("pair_pnl"), 0.0) or 0.0
    accepted_actions = inum(metrics.get("accepted_actions"), candidate_rows)
    fair_price_blocks = inum(event_counts.get("fair_price_admission_block"))
    pair_completion_blocks = inum(event_counts.get("pair_completion_block"))
    source_blocks = sum(
        count
        for reason, count in block_reasons.items()
        if str(reason).startswith("source_quality") or str(reason).endswith("_source")
    )

    hard_blockers: list[str] = []
    if inum(event.get("bad_json")):
        hard_blockers.append("bad_event_json_present")
    if candidate_rows < args.min_candidates:
        hard_blockers.append("candidate_density_below_min")
    if accepted_actions < args.min_accepted_actions:
        hard_blockers.append("accepted_actions_below_min")
    if fill_rows < args.min_fills:
        hard_blockers.append("fill_density_below_min")
    if pair_pnl < args.min_pair_pnl - 1e-12:
        hard_blockers.append("pair_pnl_below_min")
    if source_blocks:
        hard_blockers.append("source_blocks_present")

    repeat_allowed = not hard_blockers
    status = (
        "KEEP_CE25_PAIR_COMPLETION_DENSITY_GATE_REPEAT_ALLOWED_LOCAL_ONLY"
        if repeat_allowed
        else "BLOCKED_CE25_PAIR_COMPLETION_DENSITY_GATE_REPEAT_STARVED_LOCAL_ONLY"
    )
    if pair_completion_blocks and not candidate_rows:
        recommendation = "do_not_repeat_same_profile_until_market_window_changes_or_preflight_density_recovers"
    elif repeat_allowed:
        recommendation = "same_profile_repeat_can_be_considered_on_a_future_heartbeat"
    else:
        recommendation = "treat_as_starvation_sample_and_continue_local_profile_analysis"

    return {
        "artifact": "xuan_ce25_pair_completion_density_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_ce25_pair_completion_density_gate.py",
        "status": status,
        "inputs": {
            "event_diagnostics": str(Path(args.event_diagnostics).expanduser()),
            "postrun_bundle": str(Path(args.postrun_bundle).expanduser()) if args.postrun_bundle else None,
        },
        "thresholds": {
            "min_candidates": args.min_candidates,
            "min_accepted_actions": args.min_accepted_actions,
            "min_fills": args.min_fills,
            "min_pair_pnl": args.min_pair_pnl,
        },
        "observed": {
            "candidate_rows": candidate_rows,
            "accepted_actions": accepted_actions,
            "queue_supported_fills": fill_rows,
            "strict_rescue_or_salvage_rows": rescue_rows,
            "pair_pnl": round(pair_pnl, 12),
            "fair_price_admission_blocks": fair_price_blocks,
            "pair_completion_blocks": pair_completion_blocks,
            "source_blocks": source_blocks,
            "event_counts": event_counts,
            "block_reason_counts": block_reasons,
            "scorecard_statuses": statuses,
            "candidate_pair_cost_after_fee": event.get("candidate_fair_price_pair_cost_after_fee"),
            "fair_price_block_pair_cost_after_fee": event.get("fair_price_block_pair_cost_after_fee"),
            "pair_completion_block_projected_pair_pnl_after": event.get(
                "pair_completion_block_projected_pair_pnl_after"
            ),
        },
        "decision": {
            "same_profile_remote_repeat_allowed": repeat_allowed,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
            "recommendation": recommendation,
        },
        "interpretation": [
            "This gate is local/read-only and does not relax pair-cost, closeability, or rescue economics.",
            "It prevents spending another 30-minute remote dry-run on the same profile after an observed zero-density window.",
            "Passing this gate is not promotion evidence; it only says a future bounded dry-run may have enough sample density to be worth running.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-diagnostics", required=True)
    parser.add_argument("--postrun-bundle")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-candidates", type=int, default=5)
    parser.add_argument("--min-accepted-actions", type=int, default=5)
    parser.add_argument("--min-fills", type=int, default=1)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
