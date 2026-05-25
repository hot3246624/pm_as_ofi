#!/usr/bin/env python3
"""Score early density prefixes for soft-mainline no-cancel windows.

This local-only scorer asks whether the first N minutes of a no-order event
stream already show enough fill/rescue density to justify spending a full
bounded 30 minute sample on the same soft-mainline family. It is planning
evidence only; it does not launch remote jobs and is not promotion evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter
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


def safe_rate(num: float, den: float) -> float | None:
    return num / den if den > 0 else None


def event_ts_ms(obj: dict[str, Any]) -> int | None:
    for key in ("ts_ms", "fill_ts_ms", "placed_ts_ms", "trigger_ts_ms"):
        val = fnum(obj.get(key))
        if val is not None:
            return int(val)
    return None


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def scan_events(root: Path) -> tuple[list[tuple[int, str, dict[str, Any]]], int]:
    rows: list[tuple[int, str, dict[str, Any]]] = []
    bad_json = 0
    for path in sorted(root.glob("*.events.jsonl")):
        with path.open() as handle:
            for line in handle:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue
                ts_ms = event_ts_ms(obj)
                if ts_ms is None:
                    continue
                rows.append((ts_ms, str(obj.get("kind") or "<missing>"), obj))
    rows.sort(key=lambda item: item[0])
    return rows, bad_json


def source_block_count(rows: list[tuple[int, str, dict[str, Any]]]) -> int:
    total = 0
    for _, kind, obj in rows:
        reason = str(
            obj.get("block_reason")
            or obj.get("source_quality_block_reason")
            or obj.get("reason")
            or ""
        )
        if reason.startswith("source_quality") or reason.endswith("_source"):
            total += 1
        if kind == "candidate" and not (
            obj.get("source_quality_trade_source_sequence_id")
            and obj.get("source_quality_l1_source_sequence_id")
            and obj.get("source_quality_l2_source_sequence_id")
        ):
            total += 1
    return total


def max_field(rows: list[tuple[int, str, dict[str, Any]]], kind: str, field: str) -> float | None:
    vals = [fnum(obj.get(field)) for _, row_kind, obj in rows if row_kind == kind]
    nums = [val for val in vals if val is not None]
    return max(nums) if nums else None


def scaled_min(full_min: int, elapsed_minutes: float, full_minutes: float, floor_min: int = 0) -> int:
    if elapsed_minutes <= 0 or full_minutes <= 0:
        return floor_min
    return max(floor_min, int(math.floor(full_min * elapsed_minutes / full_minutes)))


def evaluate_prefix(prefix: dict[str, Any], args: argparse.Namespace) -> list[str]:
    blockers: list[str] = []
    if inum(prefix.get("bad_event_json")):
        blockers.append("bad_event_json_present")
    if inum(prefix.get("source_blocks")) > args.max_source_blocks:
        blockers.append("source_blocks_above_max")
    if inum(prefix.get("candidate_rows")) < inum(prefix.get("min_candidates")):
        blockers.append("candidate_density_below_scaled_min")
    if inum(prefix.get("queue_supported_fills")) < inum(prefix.get("min_fills")):
        blockers.append("fill_density_below_scaled_min")
    if inum(prefix.get("strict_rescue_or_salvage_rows")) < inum(prefix.get("min_rescue_closes")):
        blockers.append("rescue_density_below_scaled_min")

    fill_rate = fnum(prefix.get("fill_rate"), 0.0) or 0.0
    rescue_per_candidate = fnum(prefix.get("rescue_per_candidate"), 0.0) or 0.0
    rescue_per_fill = fnum(prefix.get("rescue_per_fill"), 0.0) or 0.0
    if fill_rate < args.min_fill_rate - 1e-12:
        blockers.append("fill_rate_below_min")
    if rescue_per_candidate < args.min_rescue_per_candidate - 1e-12:
        blockers.append("rescue_per_candidate_below_min")
    if rescue_per_fill < args.min_rescue_per_fill - 1e-12:
        blockers.append("rescue_per_fill_below_min")

    accepted_l1_age = fnum(prefix.get("accepted_l1_age_ms_max"), 0.0) or 0.0
    rescue_l1_age = fnum(prefix.get("rescue_l1_age_ms_max"), 0.0) or 0.0
    if accepted_l1_age > args.max_accepted_l1_age_ms:
        blockers.append("accepted_l1_age_above_max")
    if rescue_l1_age > args.max_rescue_l1_age_ms:
        blockers.append("rescue_l1_age_above_max")
    return blockers


def summarize_prefix(
    *,
    rows: list[tuple[int, str, dict[str, Any]]],
    cutoff_ms: int,
    elapsed_minutes: float,
    args: argparse.Namespace,
    bad_json: int,
) -> dict[str, Any]:
    prefix_rows = [row for row in rows if row[0] <= cutoff_ms]
    counts = Counter(kind for _, kind, _ in prefix_rows)
    candidates = counts.get("candidate", 0)
    fills = counts.get("queue_supported_fill", 0)
    rescues = counts.get("fak_salvage", 0)
    summary: dict[str, Any] = {
        "elapsed_minutes": elapsed_minutes,
        "event_rows": len(prefix_rows),
        "candidate_rows": candidates,
        "queue_supported_fills": fills,
        "strict_rescue_or_salvage_rows": rescues,
        "internal_pair_rows": counts.get("internal_pair", 0),
        "cancel_rows": counts.get("cancel", 0),
        "touch_rows": counts.get("touch", 0),
        "activation_block_rows": counts.get("activation_block", 0),
        "pair_completion_block_rows": counts.get("pair_completion_block", 0),
        "risk_seed_closeability_block_rows": counts.get("risk_seed_closeability_block", 0),
        "fill_rate": safe_rate(fills, candidates),
        "rescue_per_candidate": safe_rate(rescues, candidates),
        "rescue_per_fill": safe_rate(rescues, fills),
        "accepted_l1_age_ms_max": max_field(prefix_rows, "candidate", "source_quality_l1_age_ms") or 0.0,
        "rescue_l1_age_ms_max": max_field(prefix_rows, "fak_salvage", "strict_rescue_l1_age_ms") or 0.0,
        "source_blocks": source_block_count(prefix_rows),
        "bad_event_json": bad_json,
        "event_counts": dict(counts),
        "min_candidates": scaled_min(
            args.full_min_candidates,
            elapsed_minutes,
            args.full_window_minutes,
            args.prefix_min_candidate_floor,
        ),
        "min_fills": scaled_min(
            args.full_min_fills,
            elapsed_minutes,
            args.full_window_minutes,
            args.prefix_min_fill_floor,
        ),
        "min_rescue_closes": scaled_min(
            args.full_min_rescue_closes,
            elapsed_minutes,
            args.full_window_minutes,
            args.prefix_min_rescue_floor,
        ),
    }
    summary["hard_blockers"] = evaluate_prefix(summary, args)
    summary["pass"] = not summary["hard_blockers"]
    return summary


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    rows, bad_json = scan_events(root)
    if not rows:
        return {
            "artifact": "xuan_soft_mainline_density_prefix_scorer",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_soft_mainline_density_prefix_scorer.py",
            "status": "BLOCKED_SOFT_MAINLINE_DENSITY_PREFIX_SCORER_NO_EVENTS_LOCAL_ONLY",
            "output_root": str(root),
            "decision": {
                "remote_runner_allowed": False,
                "research_only": True,
                "same_profile_full_window_density_worthwhile": False,
                "shadow_ready": False,
                "deployable": False,
                "hard_blockers": ["no_event_rows"],
            },
        }

    t0 = rows[0][0]
    observed_duration_minutes = (rows[-1][0] - t0) / 60000.0
    prefix_minutes = sorted({float(item) for item in args.prefix_minutes})
    prefixes = [
        summarize_prefix(
            rows=rows,
            cutoff_ms=t0 + int(minutes * 60000),
            elapsed_minutes=minutes,
            args=args,
            bad_json=bad_json,
        )
        for minutes in prefix_minutes
    ]
    decision_candidates = [
        item
        for item in prefixes
        if item["elapsed_minutes"] >= args.min_decision_minutes - 1e-12 and item["pass"]
    ]
    earliest_pass = decision_candidates[0] if decision_candidates else None
    final_prefix = prefixes[-1] if prefixes else None
    hard_blockers = [] if earliest_pass else ["no_prefix_passed_density_gate_after_min_decision_minutes"]
    status = (
        "KEEP_SOFT_MAINLINE_DENSITY_PREFIX_SCORER_PASS_LOCAL_ONLY"
        if earliest_pass
        else "BLOCKED_SOFT_MAINLINE_DENSITY_PREFIX_SCORER_WEAK_RESCUE_DENSITY_LOCAL_ONLY"
    )
    return {
        "artifact": "xuan_soft_mainline_density_prefix_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_density_prefix_scorer.py",
        "status": status,
        "output_root": str(root),
        "observed_duration_minutes": observed_duration_minutes,
        "thresholds": {
            "full_window_minutes": args.full_window_minutes,
            "full_min_candidates": args.full_min_candidates,
            "full_min_fills": args.full_min_fills,
            "full_min_rescue_closes": args.full_min_rescue_closes,
            "prefix_min_candidate_floor": args.prefix_min_candidate_floor,
            "prefix_min_fill_floor": args.prefix_min_fill_floor,
            "prefix_min_rescue_floor": args.prefix_min_rescue_floor,
            "min_decision_minutes": args.min_decision_minutes,
            "min_fill_rate": args.min_fill_rate,
            "min_rescue_per_candidate": args.min_rescue_per_candidate,
            "min_rescue_per_fill": args.min_rescue_per_fill,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
            "max_source_blocks": args.max_source_blocks,
        },
        "prefixes": prefixes,
        "earliest_passing_decision_prefix": earliest_pass,
        "final_prefix": final_prefix,
        "decision": {
            "same_profile_full_window_density_worthwhile": earliest_pass is not None,
            "earliest_pass_minutes": earliest_pass.get("elapsed_minutes") if earliest_pass else None,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
            "recommendation": (
                "same_profile_full_window_can_be_considered_on_future_heartbeat"
                if earliest_pass
                else "do_not_spend_full_1800s_sample_without_stronger_early_rescue_density"
            ),
        },
        "guardrails": [
            "This scorer is local planning evidence only and does not launch remote jobs.",
            "Passing an early prefix is not shadow or promotion evidence.",
            "Future remote remains PM_DRY_RUN=1, 1800s max, one run per wake, and only after manifest verification.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--prefix-minutes", nargs="+", type=float, default=[3, 5, 10, 15, 20, 30])
    parser.add_argument("--full-window-minutes", type=float, default=30.0)
    parser.add_argument("--full-min-candidates", type=int, default=20)
    parser.add_argument("--full-min-fills", type=int, default=18)
    parser.add_argument("--full-min-rescue-closes", type=int, default=7)
    parser.add_argument("--prefix-min-candidate-floor", type=int, default=2)
    parser.add_argument("--prefix-min-fill-floor", type=int, default=1)
    parser.add_argument("--prefix-min-rescue-floor", type=int, default=1)
    parser.add_argument("--min-decision-minutes", type=float, default=10.0)
    parser.add_argument("--min-fill-rate", type=float, default=0.70)
    parser.add_argument("--min-rescue-per-candidate", type=float, default=0.25)
    parser.add_argument("--min-rescue-per-fill", type=float, default=0.30)
    parser.add_argument("--max-accepted-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-source-blocks", type=int, default=0)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rounded(card), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(card), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
