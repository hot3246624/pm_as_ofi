#!/usr/bin/env python3
"""Build a local density history ledger for soft-mainline no-cancel windows.

This scorer is intentionally local-only. It rolls multiple postrun/prefix
windows into a compact history so the next heartbeat can tell whether the lane
is improving, deteriorating, or waiting for a fresh density signal before a
bounded no-order dry-run is worth spending.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


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


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def status_is_keep(card: dict[str, Any]) -> bool:
    return str(card.get("status") or "").startswith("KEEP")


def status_is_wait(card: dict[str, Any]) -> bool:
    status = str(card.get("status") or "")
    return status.startswith("UNKNOWN") and "WAIT" in status


def status_is_blocked(card: dict[str, Any]) -> bool:
    return str(card.get("status") or "").startswith("BLOCKED")


def metric_dict(runtime: dict[str, Any]) -> dict[str, Any]:
    metrics = runtime.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def event_counts(event: dict[str, Any]) -> dict[str, Any]:
    counts = event.get("event_counts")
    return counts if isinstance(counts, dict) else {}


def prefix_decision(prefix: dict[str, Any]) -> dict[str, Any]:
    decision = prefix.get("decision")
    return decision if isinstance(decision, dict) else {}


def final_prefix(prefix: dict[str, Any]) -> dict[str, Any]:
    final = prefix.get("final_prefix")
    return final if isinstance(final, dict) else {}


def parse_window(raw: list[str]) -> dict[str, str]:
    if len(raw) != 4:
        raise SystemExit("--window requires LABEL RUNTIME_SUMMARY EVENT_DIAGNOSTICS PREFIX_SCORECARD")
    label, runtime, event, prefix = raw
    return {
        "label": label,
        "runtime_summary": runtime,
        "event_diagnostics": event,
        "prefix_scorecard": prefix,
    }


def summarize_window(raw: list[str], args: argparse.Namespace) -> dict[str, Any]:
    spec = parse_window(raw)
    runtime = load_json(spec["runtime_summary"])
    event = load_json(spec["event_diagnostics"])
    prefix = load_json(spec["prefix_scorecard"])
    metrics = metric_dict(runtime)
    counts = event_counts(event)
    prefix_final = final_prefix(prefix)
    prefix_dec = prefix_decision(prefix)

    candidates = inum(counts.get("candidate"), inum(metrics.get("accepted_actions")))
    fills = inum(counts.get("queue_supported_fill"), inum(metrics.get("queue_supported_fills")))
    rescues = inum(counts.get("fak_salvage"), inum(metrics.get("strict_rescue_closes")))
    accepted = inum(metrics.get("accepted_actions"), candidates)
    pair_pnl = fnum(metrics.get("pair_pnl"), 0.0) or 0.0
    residual_qty_share = fnum(metrics.get("residual_qty_share"), 0.0) or 0.0
    residual_cost_share = fnum(metrics.get("residual_cost_share"), 0.0) or 0.0

    fill_rate = safe_rate(fills, candidates)
    rescue_per_candidate = safe_rate(rescues, candidates)
    rescue_per_fill = safe_rate(rescues, fills)
    density_pass = (
        candidates >= args.min_candidates
        and accepted >= args.min_accepted_actions
        and fills >= args.min_fills
        and rescues >= args.min_rescue_closes
        and (fill_rate or 0.0) >= args.min_fill_rate
        and (rescue_per_candidate or 0.0) >= args.min_rescue_per_candidate
        and (rescue_per_fill or 0.0) >= args.min_rescue_per_fill
        and pair_pnl >= args.min_pair_pnl
        and residual_qty_share <= args.max_residual_qty_share
        and residual_cost_share <= args.max_residual_cost_share
        and inum(metrics.get("strict_rescue_source_blocks")) <= args.max_source_blocks
    )

    return {
        "label": spec["label"],
        "inputs": spec,
        "runtime_status": runtime.get("status"),
        "prefix_status": prefix.get("status"),
        "prefix_pass": status_is_keep(prefix),
        "prefix_wait": status_is_wait(prefix),
        "prefix_blocked": status_is_blocked(prefix),
        "density_pass": density_pass,
        "observed": {
            "candidate_rows": candidates,
            "touch_rows": inum(counts.get("touch")),
            "accepted_actions": accepted,
            "queue_supported_fills": fills,
            "strict_rescue_closes": rescues,
            "rescue_slugs": inum(metrics.get("rescue_slugs")),
            "fill_rate": fill_rate,
            "rescue_per_candidate": rescue_per_candidate,
            "rescue_per_fill": rescue_per_fill,
            "pair_pnl": pair_pnl,
            "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "rescue_net_pair_cost_max": fnum(metrics.get("rescue_net_pair_cost_max")),
            "source_blocks": inum(metrics.get("strict_rescue_source_blocks")),
        },
        "prefix": {
            "earliest_pass_minutes": prefix_dec.get("earliest_pass_minutes"),
            "hard_blockers": prefix_dec.get("hard_blockers"),
            "final": {
                "candidate_rows": prefix_final.get("candidate_rows"),
                "queue_supported_fills": prefix_final.get("queue_supported_fills"),
                "strict_rescue_or_salvage_rows": prefix_final.get("strict_rescue_or_salvage_rows"),
                "fill_rate": prefix_final.get("fill_rate"),
                "rescue_per_candidate": prefix_final.get("rescue_per_candidate"),
                "rescue_per_fill": prefix_final.get("rescue_per_fill"),
            },
        },
    }


def trend(previous: dict[str, Any] | None, latest: dict[str, Any]) -> dict[str, Any]:
    if previous is None:
        return {"state": "single_window", "details": []}
    prev_obs = previous["observed"]
    latest_obs = latest["observed"]
    details: list[str] = []
    if latest["density_pass"] and not previous["density_pass"]:
        details.append("latest_recovered_density_pass")
    if previous["density_pass"] and not latest["density_pass"]:
        details.append("latest_lost_density_pass")
    if (latest_obs.get("candidate_rows") or 0) >= (prev_obs.get("candidate_rows") or 0):
        details.append("latest_candidate_count_not_lower")
    if (latest_obs.get("strict_rescue_closes") or 0) < (prev_obs.get("strict_rescue_closes") or 0):
        details.append("strict_rescue_closes_decreased")
    if (latest_obs.get("rescue_per_candidate") or 0.0) < (prev_obs.get("rescue_per_candidate") or 0.0):
        details.append("rescue_per_candidate_decreased")
    if (latest_obs.get("residual_cost_share") or 0.0) > (prev_obs.get("residual_cost_share") or 0.0):
        details.append("residual_cost_share_increased")
    if latest["prefix_pass"] and not previous["prefix_pass"]:
        details.append("latest_recovered_prefix_pass")
    if previous["prefix_pass"] and not latest["prefix_pass"]:
        details.append("latest_lost_prefix_pass")
    state = "mixed"
    if "latest_recovered_density_pass" in details or "latest_recovered_prefix_pass" in details:
        state = "improving"
    elif "latest_lost_density_pass" in details or "latest_lost_prefix_pass" in details:
        state = "deteriorating"
    return {"state": state, "details": details}


def build(args: argparse.Namespace) -> dict[str, Any]:
    windows = [summarize_window(raw, args) for raw in args.window]
    if not windows:
        raise SystemExit("provide at least one --window")
    latest = windows[-1]
    previous = windows[-2] if len(windows) >= 2 else None
    trend_summary = trend(previous, latest)
    pass_count = sum(1 for item in windows if item["density_pass"] or item["prefix_pass"])
    blocked_count = sum(1 for item in windows if item["prefix_blocked"] and not item["density_pass"])
    waiting_count = sum(1 for item in windows if item["prefix_wait"])

    if latest["prefix_wait"]:
        status = "UNKNOWN_SOFT_MAINLINE_DENSITY_HISTORY_WAIT_FOR_PREFIX_LOCAL_ONLY"
        next_action = "wait_for_min_decision_prefix_before_remote_decision"
        hard_blockers = ["latest_prefix_waiting"]
    elif latest["density_pass"] or latest["prefix_pass"]:
        status = "KEEP_SOFT_MAINLINE_DENSITY_HISTORY_LATEST_READY_LOCAL_ONLY"
        next_action = "prepare_one_bounded_no_cancel_soft_mainline_remote_on_future_heartbeat_after_verifier"
        hard_blockers = []
    else:
        status = "BLOCKED_SOFT_MAINLINE_DENSITY_HISTORY_LATEST_WEAK_LOCAL_ONLY"
        next_action = "wait_for_fresh_prefix_or_preflight_density_signal_before_remote"
        hard_blockers = ["latest_density_not_ready"]

    return {
        "artifact": "xuan_soft_mainline_density_history_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_density_history_scorer.py",
        "status": status,
        "windows": windows,
        "summary": {
            "window_count": len(windows),
            "pass_count": pass_count,
            "blocked_count": blocked_count,
            "waiting_count": waiting_count,
            "latest_label": latest["label"],
            "latest_density_pass": latest["density_pass"],
            "latest_prefix_pass": latest["prefix_pass"],
            "trend": trend_summary,
        },
        "decision": {
            "next_action": next_action,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
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
            "max_source_blocks": args.max_source_blocks,
        },
        "guardrails": [
            "This history ledger is local planning evidence only.",
            "It does not approve shadow, deployment, or live trading.",
            "It does not launch remote jobs.",
            "Do not return to ce25 hard gates as the mainline controller.",
            "Future remote remains PM_DRY_RUN=1, 1800s max, one run per wake, and requires manifest verification.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--window",
        nargs=4,
        action="append",
        metavar=("LABEL", "RUNTIME_SUMMARY", "EVENT_DIAGNOSTICS", "PREFIX_SCORECARD"),
        required=True,
    )
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
    parser.add_argument("--max-source-blocks", type=int, default=0)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rounded(card), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(card), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
