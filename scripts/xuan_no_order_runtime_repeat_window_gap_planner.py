#!/usr/bin/env python3
"""Plan the remaining repeat-window gap for xuan-frontier no-order evidence.

This is a local planning helper. It does not promote a strategy or score new
runtime evidence; it reads a repeat-window scorecard and turns the remaining
shadow-review blockers into explicit next-window targets.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        val = float(value)
        return val if math.isfinite(val) else default
    except (TypeError, ValueError):
        return default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def pick_reference_window(windows: list[dict[str, Any]]) -> dict[str, Any] | None:
    pass_windows = [window for window in windows if window.get("status") == "PASS"]
    if not pass_windows:
        return None
    return max(
        pass_windows,
        key=lambda window: (
            as_float(window.get("sample", {}).get("strict_rescue_closes")),
            as_float(window.get("sample", {}).get("accepted_actions")),
            as_float(window.get("sample", {}).get("filled_qty")),
        ),
    )


def safe_share(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator > 0 else None


def next_window_residual_headroom(
    current_residual: float,
    current_filled: float,
    max_total_share: float,
    expected_next_filled: float,
) -> dict[str, Any]:
    allowed_abs = max_total_share * (current_filled + expected_next_filled) - current_residual
    return {
        "allowed_abs": allowed_abs,
        "allowed_share_at_expected_scale": safe_share(allowed_abs, expected_next_filled),
    }


def score(args: argparse.Namespace) -> dict[str, Any]:
    repeat = read_json(Path(args.repeat_scorecard).expanduser().resolve())
    aggregate = repeat.get("aggregate", {})
    thresholds = repeat.get("thresholds", {})
    blockers = repeat.get("hard_blockers", [])
    windows = repeat.get("windows", [])
    reference = pick_reference_window(windows)

    reference_sample = reference.get("sample", {}) if reference else {}
    reference_econ = reference.get("economics", {}) if reference else {}
    expected_filled_qty = (
        args.expected_next_filled_qty
        if args.expected_next_filled_qty is not None
        else as_float(reference_sample.get("filled_qty"))
    )
    expected_filled_cost = (
        args.expected_next_filled_cost
        if args.expected_next_filled_cost is not None
        else as_float(reference_sample.get("filled_cost"))
    )

    gaps = {
        "total_strict_rescue_closes": max(
            0,
            int(as_float(thresholds.get("min_total_rescue_closes")))
            - int(as_float(aggregate.get("strict_rescue_closes"))),
        ),
        "total_accepted_actions": max(
            0,
            int(as_float(thresholds.get("min_total_accepted_actions")))
            - int(as_float(aggregate.get("accepted_actions"))),
        ),
        "total_fills": max(
            0,
            int(as_float(thresholds.get("min_total_fills")))
            - int(as_float(aggregate.get("queue_supported_fills"))),
        ),
        "total_pair_pnl": max(
            0.0,
            as_float(thresholds.get("min_total_pair_pnl")) - as_float(aggregate.get("pair_pnl")),
        ),
    }

    next_window_floor = {
        "must_pass_per_window_gates": True,
        "min_accepted_actions": int(as_float(thresholds.get("min_window_accepted_actions"))),
        "min_fills": int(as_float(thresholds.get("min_window_fills"))),
        "min_pair_pnl": as_float(thresholds.get("min_window_pair_pnl")),
        "max_residual_qty_share": as_float(thresholds.get("max_window_residual_qty_share")),
        "max_residual_cost_share": as_float(thresholds.get("max_window_residual_cost_share")),
        "max_rescue_net_pair_cost": as_float(thresholds.get("max_rescue_net_pair_cost")),
        "max_accepted_l1_age_ms": as_float(thresholds.get("max_accepted_l1_age_ms")),
        "max_rescue_l1_age_ms": as_float(thresholds.get("max_rescue_l1_age_ms")),
        "min_strict_rescue_closes_to_clear_aggregate": gaps["total_strict_rescue_closes"],
    }

    qty_headroom = next_window_residual_headroom(
        as_float(aggregate.get("residual_qty")),
        as_float(aggregate.get("filled_qty")),
        as_float(thresholds.get("max_total_residual_qty_share")),
        expected_filled_qty,
    )
    cost_headroom = next_window_residual_headroom(
        as_float(aggregate.get("residual_cost")),
        as_float(aggregate.get("filled_cost")),
        as_float(thresholds.get("max_total_residual_cost_share")),
        expected_filled_cost,
    )

    remaining_blockers_after_target = [
        blocker
        for blocker in blockers
        if blocker != "total_strict_rescue_closes_below_min" or gaps["total_strict_rescue_closes"] > 0
    ]
    status = (
        "KEEP_REPEAT_WINDOW_ALREADY_SHADOW_REVIEW_READY_LOCAL_ONLY"
        if repeat.get("decision", {}).get("shadow_review_ready") is True
        else "KEEP_REPEAT_WINDOW_GAP_PLAN_READY_LOCAL_ONLY"
    )
    if reference is None:
        status = "UNKNOWN_REPEAT_WINDOW_GAP_PLAN_NO_PASS_WINDOWS"

    return {
        "status": status,
        "input_repeat_scorecard": str(Path(args.repeat_scorecard).expanduser().resolve()),
        "current_repeat_status": repeat.get("status"),
        "current_blockers": blockers,
        "current_aggregate": aggregate,
        "reference_window": {
            "instance_id": reference.get("instance_id") if reference else None,
            "root": reference.get("root") if reference else None,
            "sample": reference_sample,
            "economics": reference_econ,
        },
        "remaining_gaps": gaps,
        "next_window_floor": next_window_floor,
        "aggregate_residual_headroom_at_reference_scale": {
            "expected_next_filled_qty": expected_filled_qty,
            "expected_next_filled_cost": expected_filled_cost,
            "residual_qty": qty_headroom,
            "residual_cost": cost_headroom,
        },
        "interpretation": {
            "short_text": (
                "Existing pass windows clear source, safety, debt, economics, and residual gates; "
                "the next same-profile window needs at least the remaining clean strict rescue closes "
                "while preserving per-window gates."
            ),
            "remote_runner_allowed": False,
            "deployable": False,
            "promotion_evidence": False,
        },
        "decision": {
            "local_plan_ready": reference is not None,
            "remote_runner_allowed": False,
            "shadow_review_ready_now": repeat.get("decision", {}).get("shadow_review_ready") is True,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--expected-next-filled-qty", type=float)
    parser.add_argument("--expected-next-filled-cost", type=float)
    args = parser.parse_args()

    started = time.time()
    result = score(args)
    scorecard = {
        "artifact": "xuan_no_order_runtime_repeat_window_gap_planner",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_runtime_repeat_window_gap_planner.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
