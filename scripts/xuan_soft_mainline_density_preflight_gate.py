#!/usr/bin/env python3
"""Gate soft-mainline no-cancel repeats by observed fill/rescue density.

This is a local-only planning scorer. It does not launch remote jobs and does
not promote a strategy. It turns recent no-order evidence into a simple answer:
is the same soft-mainline profile worth another bounded 30 minute dry-run, or
did the latest window show weak rescue density?
"""

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


def safe_rate(num: float, den: float) -> float | None:
    return num / den if den > 0 else None


def source_block_count(event: dict[str, Any], metrics: dict[str, Any]) -> int:
    block_reasons = event.get("block_reason_counts")
    total = inum(event.get("accepted_source_missing_any"))
    total += inum(metrics.get("strict_rescue_source_blocks"))
    if isinstance(block_reasons, dict):
        for reason, count in block_reasons.items():
            reason_s = str(reason)
            if reason_s.startswith("source_quality") or reason_s.endswith("_source"):
                total += inum(count)
    return total


def extract_observed(event: dict[str, Any], runtime: dict[str, Any], postrun: dict[str, Any]) -> dict[str, Any]:
    metrics = runtime.get("metrics") if isinstance(runtime.get("metrics"), dict) else {}
    if not metrics:
        metrics = nested(postrun, "runtime_summary", "metrics") or {}
    event_counts = event.get("event_counts") if isinstance(event.get("event_counts"), dict) else {}
    block_reasons = event.get("block_reason_counts") if isinstance(event.get("block_reason_counts"), dict) else {}

    candidates = inum(event_counts.get("candidate"), inum(metrics.get("accepted_actions")))
    fills = inum(event_counts.get("queue_supported_fill"), inum(metrics.get("queue_supported_fills")))
    rescues = inum(event_counts.get("fak_salvage"), inum(metrics.get("strict_rescue_closes")))
    accepted = inum(metrics.get("accepted_actions"), candidates)
    pair_pnl = fnum(metrics.get("pair_pnl"), 0.0) or 0.0
    residual_qty_share = fnum(metrics.get("residual_qty_share"), 0.0) or 0.0
    residual_cost_share = fnum(metrics.get("residual_cost_share"), 0.0) or 0.0
    accepted_l1_age = fnum(metrics.get("accepted_l1_age_ms_max"), 0.0) or 0.0
    rescue_l1_age = fnum(metrics.get("rescue_l1_age_ms_max"), 0.0) or 0.0
    closeability_blocks = inum(event_counts.get("risk_seed_closeability_block"))
    pair_completion_blocks = inum(event_counts.get("pair_completion_block"))
    ttl_cancels = 0
    cancel_reasons = event.get("cancel_reasons")
    if isinstance(cancel_reasons, dict):
        ttl_cancels = inum(cancel_reasons.get("ttl"))

    return {
        "accepted_actions": accepted,
        "candidate_rows": candidates,
        "queue_supported_fills": fills,
        "strict_rescue_or_salvage_rows": rescues,
        "fill_rate": safe_rate(fills, candidates),
        "rescue_per_candidate": safe_rate(rescues, candidates),
        "rescue_per_fill": safe_rate(rescues, fills),
        "pair_pnl": round(pair_pnl, 12),
        "residual_qty_share": round(residual_qty_share, 12),
        "residual_cost_share": round(residual_cost_share, 12),
        "accepted_l1_age_ms_max": accepted_l1_age,
        "rescue_l1_age_ms_max": rescue_l1_age,
        "source_blocks": source_block_count(event, metrics),
        "bad_event_json": inum(event.get("bad_json")),
        "closeability_blocks": closeability_blocks,
        "pair_completion_blocks": pair_completion_blocks,
        "ttl_cancels": ttl_cancels,
        "event_counts": event_counts,
        "block_reason_counts": block_reasons,
    }


def evaluate(observed: dict[str, Any], args: argparse.Namespace) -> list[str]:
    blockers: list[str] = []
    if inum(observed.get("bad_event_json")):
        blockers.append("bad_event_json_present")
    if inum(observed.get("source_blocks")) > args.max_source_blocks:
        blockers.append("source_blocks_above_max")
    if inum(observed.get("candidate_rows")) < args.min_candidates:
        blockers.append("candidate_density_below_min")
    if inum(observed.get("accepted_actions")) < args.min_accepted_actions:
        blockers.append("accepted_actions_below_min")
    if inum(observed.get("queue_supported_fills")) < args.min_fills:
        blockers.append("fill_density_below_min")
    if inum(observed.get("strict_rescue_or_salvage_rows")) < args.min_rescue_closes:
        blockers.append("rescue_density_below_min")

    fill_rate = fnum(observed.get("fill_rate"), 0.0) or 0.0
    rescue_per_candidate = fnum(observed.get("rescue_per_candidate"), 0.0) or 0.0
    rescue_per_fill = fnum(observed.get("rescue_per_fill"), 0.0) or 0.0
    if fill_rate < args.min_fill_rate - 1e-12:
        blockers.append("fill_rate_below_min")
    if rescue_per_candidate < args.min_rescue_per_candidate - 1e-12:
        blockers.append("rescue_per_candidate_below_min")
    if rescue_per_fill < args.min_rescue_per_fill - 1e-12:
        blockers.append("rescue_per_fill_below_min")

    if (fnum(observed.get("pair_pnl"), 0.0) or 0.0) < args.min_pair_pnl - 1e-12:
        blockers.append("pair_pnl_below_min")
    if (fnum(observed.get("residual_qty_share"), 0.0) or 0.0) > args.max_residual_qty_share + 1e-12:
        blockers.append("residual_qty_share_above_max")
    if (fnum(observed.get("residual_cost_share"), 0.0) or 0.0) > args.max_residual_cost_share + 1e-12:
        blockers.append("residual_cost_share_above_max")
    if (fnum(observed.get("accepted_l1_age_ms_max"), 0.0) or 0.0) > args.max_accepted_l1_age_ms:
        blockers.append("accepted_l1_age_above_max")
    if (fnum(observed.get("rescue_l1_age_ms_max"), 0.0) or 0.0) > args.max_rescue_l1_age_ms:
        blockers.append("rescue_l1_age_above_max")
    return blockers


def build(args: argparse.Namespace) -> dict[str, Any]:
    event = load_json(args.event_diagnostics)
    runtime = load_json(args.runtime_summary)
    postrun = load_json(args.postrun_bundle)
    observed = extract_observed(event, runtime, postrun)
    hard_blockers = evaluate(observed, args)

    reference_observed = None
    reference_blockers = None
    if args.reference_event_diagnostics:
        reference_observed = extract_observed(
            load_json(args.reference_event_diagnostics),
            load_json(args.reference_runtime_summary),
            load_json(args.reference_postrun_bundle),
        )
        reference_blockers = evaluate(reference_observed, args)

    passed = not hard_blockers
    status = (
        "KEEP_SOFT_MAINLINE_DENSITY_PREFLIGHT_GATE_PASS_LOCAL_ONLY"
        if passed
        else "BLOCKED_SOFT_MAINLINE_DENSITY_PREFLIGHT_GATE_WEAK_RESCUE_DENSITY_LOCAL_ONLY"
    )
    if reference_observed and not reference_blockers and hard_blockers:
        recommendation = "same_profile_remains_viable_but_wait_for_higher_density_preflight_before_remote"
    elif passed:
        recommendation = "same_profile_remote_repeat_can_be_considered_on_a_future_heartbeat"
    else:
        recommendation = "do_not_spend_another_1800s_remote_sample_without_better_density_signal"

    return {
        "artifact": "xuan_soft_mainline_density_preflight_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_density_preflight_gate.py",
        "status": status,
        "inputs": {
            "event_diagnostics": str(Path(args.event_diagnostics).expanduser()),
            "runtime_summary": str(Path(args.runtime_summary).expanduser()) if args.runtime_summary else None,
            "postrun_bundle": str(Path(args.postrun_bundle).expanduser()) if args.postrun_bundle else None,
            "reference_event_diagnostics": str(Path(args.reference_event_diagnostics).expanduser())
            if args.reference_event_diagnostics
            else None,
        },
        "thresholds": {
            "min_candidates": args.min_candidates,
            "min_accepted_actions": args.min_accepted_actions,
            "min_fills": args.min_fills,
            "min_rescue_closes": args.min_rescue_closes,
            "min_fill_rate": args.min_fill_rate,
            "min_rescue_per_candidate": args.min_rescue_per_candidate,
            "min_rescue_per_fill": args.min_rescue_per_fill,
            "min_pair_pnl": args.min_pair_pnl,
            "max_residual_qty_share": args.max_residual_qty_share,
            "max_residual_cost_share": args.max_residual_cost_share,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
            "max_source_blocks": args.max_source_blocks,
        },
        "observed": observed,
        "reference_observed": reference_observed,
        "decision": {
            "same_profile_remote_repeat_density_worthwhile": passed,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
            "reference_hard_blockers": reference_blockers,
            "recommendation": recommendation,
        },
        "guardrails": [
            "This gate is not promotion evidence and does not relax economics.",
            "Do not re-enable risk_seed_cancel_on_closeability_net_cap for the soft-mainline evidence lane.",
            "Do not return to ce25 hard gates as the mainline admission controller.",
            "Future remote remains PM_DRY_RUN=1, 1800s max, one run per wake, and only after manifest verification.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-diagnostics", required=True)
    parser.add_argument("--runtime-summary")
    parser.add_argument("--postrun-bundle")
    parser.add_argument("--reference-event-diagnostics")
    parser.add_argument("--reference-runtime-summary")
    parser.add_argument("--reference-postrun-bundle")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-candidates", type=int, default=20)
    parser.add_argument("--min-accepted-actions", type=int, default=20)
    parser.add_argument("--min-fills", type=int, default=18)
    parser.add_argument("--min-rescue-closes", type=int, default=7)
    parser.add_argument("--min-fill-rate", type=float, default=0.70)
    parser.add_argument("--min-rescue-per-candidate", type=float, default=0.25)
    parser.add_argument("--min-rescue-per-fill", type=float, default=0.30)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-accepted-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-source-blocks", type=int, default=0)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
