#!/usr/bin/env python3
"""Bridge-score soft-closeability windows against explicit surplus rescue policy.

This is a local planning scorer. It separates three cases that the stricter
repeat-window scorer intentionally conflates:

1. target-cap clean rescue rows, where net_pair_cost <= target cap;
2. explicit surplus rescue rows, where the runtime emitted allow_surplus with
   a configured surplus cap and projected pair-PnL floor; and
3. legacy/post-hoc surplus-shaped rows, where a historical clean window has
   positive aggregate economics and low residual risk, but its rescue rows lack
   explicit surplus approval fields.

Case 3 is useful for planning a fresh explicit-surplus dry-run, but is not
shadow-review evidence by itself.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


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


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    idx = (len(vals) - 1) * q
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return vals[int(idx)]
    return vals[lo] * (hi - idx) + vals[hi] * (idx - lo)


def read_lifecycle_rows(root: Path) -> dict[str, list[dict[str, str]]]:
    rows = {"actions": [], "rescues": []}
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = read_json(manifest_path)
        files = manifest.get("files", {})
        rows["actions"].extend(read_csv(root / files.get("would_action_decisions", "")))
        rows["rescues"].extend(read_csv(root / files.get("strict_rescue_closes", "")))
    return rows


def max_float(rows: list[dict[str, str]], field: str) -> float | None:
    vals = [as_float(row.get(field), float("nan")) for row in rows]
    nums = [val for val in vals if math.isfinite(val)]
    return max(nums) if nums else None


def count_nonempty(rows: list[dict[str, str]], field: str) -> int:
    return sum(1 for row in rows if row.get(field) not in (None, ""))


def summarize_rescues(
    rescues: list[dict[str, str]],
    *,
    target_cap: float,
    surplus_cap: float,
    surplus_floor: float,
) -> dict[str, Any]:
    net_pairs = [
        as_float(row.get("net_pair_cost"), float("nan"))
        for row in rescues
        if math.isfinite(as_float(row.get("net_pair_cost"), float("nan")))
    ]
    target_rows = [row for row in rescues if as_float(row.get("net_pair_cost"), float("inf")) <= target_cap + 1e-12]
    over_target = [row for row in rescues if as_float(row.get("net_pair_cost"), float("-inf")) > target_cap + 1e-12]
    over_bridge_cap = [
        row for row in over_target if as_float(row.get("net_pair_cost"), float("inf")) > surplus_cap + 1e-12
    ]
    explicit_allowed: list[dict[str, str]] = []
    explicit_blockers: list[str] = []
    missing_explicit = 0
    for row in over_target:
        decision = row.get("strict_rescue_surplus_decision")
        net_pair = as_float(row.get("net_pair_cost"), float("nan"))
        projected = as_float(row.get("strict_rescue_projected_pair_pnl_after"), float("nan"))
        if decision != "allow_surplus":
            missing_explicit += 1
            continue
        row_blockers: list[str] = []
        if not math.isfinite(net_pair) or net_pair > surplus_cap + 1e-12:
            row_blockers.append("rescue_surplus_net_pair_cost_above_cap")
        if not math.isfinite(projected):
            row_blockers.append("rescue_surplus_projected_pair_pnl_missing")
        elif projected < surplus_floor - 1e-12:
            row_blockers.append("rescue_surplus_projected_pair_pnl_below_floor")
        if row_blockers:
            explicit_blockers.extend(row_blockers)
        else:
            explicit_allowed.append(row)

    missing_fields: list[str] = []
    for field in (
        "source_sequence_id",
        "market_md_source_sequence_id",
        "strict_rescue_l2_source_sequence_id",
        "source_lot_id",
        "source_lot_sequence_id",
        "fee_per_share",
        "net_pair_cost",
    ):
        if count_nonempty(rescues, field) != len(rescues):
            missing_fields.append(field)

    projected_vals = [
        as_float(row.get("strict_rescue_projected_pair_pnl_after"), float("nan"))
        for row in explicit_allowed
        if math.isfinite(as_float(row.get("strict_rescue_projected_pair_pnl_after"), float("nan")))
    ]
    return {
        "count": len(rescues),
        "target_cap_count": len(target_rows),
        "over_target_count": len(over_target),
        "missing_explicit_surplus_approval_count": missing_explicit,
        "explicit_surplus_allowed_count": len(explicit_allowed),
        "over_bridge_surplus_cap_count": len(over_bridge_cap),
        "explicit_surplus_blockers": sorted(set(explicit_blockers)),
        "missing_source_or_fee_fields": missing_fields,
        "net_pair_cost_min": min(net_pairs) if net_pairs else None,
        "net_pair_cost_p50": statistics.median(net_pairs) if net_pairs else None,
        "net_pair_cost_p90": percentile(net_pairs, 0.90),
        "net_pair_cost_max": max(net_pairs) if net_pairs else None,
        "target_cap_net_pair_cost_max": max(
            [as_float(row.get("net_pair_cost")) for row in target_rows], default=None
        ),
        "surplus_net_pair_cost_max": max(
            [as_float(row.get("net_pair_cost")) for row in over_target], default=None
        ),
        "explicit_surplus_projected_pair_pnl_after_min": min(projected_vals) if projected_vals else None,
    }


def score_window(root: Path, role: str, args: argparse.Namespace) -> dict[str, Any]:
    required = ["manifest.json", "aggregate_report.json", "run_exit_code.txt", "run_stderr.log"]
    missing = [name for name in required if not (root / name).exists()]
    if missing:
        return {
            "root": str(root),
            "role": role,
            "status": "BLOCKED_WINDOW_MISSING_FILES",
            "hard_blockers": ["missing_files"],
            "missing_files": missing,
            "bridge_eligible": False,
        }

    manifest = read_json(root / "manifest.json")
    aggregate = read_json(root / "aggregate_report.json")
    metrics = aggregate.get("metrics", {})
    safety = manifest.get("safety", {})
    config = manifest.get("config", {})
    rows = read_lifecycle_rows(root)
    accepted = [row for row in rows["actions"] if row.get("decision") == "accept"]
    rescues = rows["rescues"]
    rescue_summary = summarize_rescues(
        rescues,
        target_cap=args.target_rescue_net_cap,
        surplus_cap=args.bridge_surplus_net_cap,
        surplus_floor=args.bridge_min_pair_pnl_after,
    )

    filled_qty = as_float(metrics.get("filled_qty"))
    filled_cost = as_float(metrics.get("filled_cost"))
    residual_qty = as_float(metrics.get("residual_qty"))
    residual_cost = as_float(metrics.get("residual_cost"))
    residual_qty_share = residual_qty / filled_qty if filled_qty else None
    residual_cost_share = residual_cost / filled_cost if filled_cost else None
    pair_pnl = as_float(metrics.get("pair_pnl"))
    accepted_l1_max = max_float(accepted, "source_quality_l1_age_ms")
    rescue_l1_max = max_float(rescues, "strict_rescue_l1_age_ms")

    hard_blockers: list[str] = []
    if (root / "run_exit_code.txt").read_text().strip() != "0":
        hard_blockers.append("nonzero_run_exit_code")
    if (root / "run_stderr.log").read_text():
        hard_blockers.append("stderr_not_empty")
    if safety.get("orders_sent") is not False or str(safety.get("dry_run")) != "1":
        hard_blockers.append("dry_run_safety_failed")
    if len(accepted) < args.min_window_accepted_actions:
        hard_blockers.append("window_accepted_actions_below_min")
    if as_float(metrics.get("queue_supported_fills")) < args.min_window_fills:
        hard_blockers.append("window_fills_below_min")
    if pair_pnl < args.min_window_pair_pnl:
        hard_blockers.append("window_pair_pnl_below_min")
    if residual_qty_share is None or residual_qty_share > args.max_window_residual_qty_share:
        hard_blockers.append("window_residual_qty_share_above_max")
    if residual_cost_share is None or residual_cost_share > args.max_window_residual_cost_share:
        hard_blockers.append("window_residual_cost_share_above_max")
    if as_float(metrics.get("strict_rescue_source_blocks")) != 0:
        hard_blockers.append("strict_rescue_source_blocks_nonzero")
    if accepted_l1_max is None or accepted_l1_max > args.max_accepted_l1_age_ms:
        hard_blockers.append("accepted_l1_age_above_max")
    if rescue_l1_max is None or rescue_l1_max > args.max_rescue_l1_age_ms:
        hard_blockers.append("rescue_l1_age_above_max")
    if rescue_summary["count"] == 0:
        hard_blockers.append("strict_rescue_closes_missing")
    if rescue_summary["over_bridge_surplus_cap_count"]:
        hard_blockers.append("rescue_surplus_net_pair_cost_above_bridge_cap")
    if rescue_summary["explicit_surplus_blockers"]:
        hard_blockers.extend(rescue_summary["explicit_surplus_blockers"])
    if rescue_summary["missing_source_or_fee_fields"]:
        hard_blockers.append("rescue_missing_source_or_fee_fields")

    explicit_policy = (
        config.get("strict_rescue_surplus_net_cap") is not None
        and config.get("strict_rescue_min_pair_pnl_after") is not None
    )
    strict_target_clean = rescue_summary["over_target_count"] == 0
    explicit_surplus_clean = (
        explicit_policy
        and rescue_summary["missing_explicit_surplus_approval_count"] == 0
        and not rescue_summary["explicit_surplus_blockers"]
        and rescue_summary["over_bridge_surplus_cap_count"] == 0
    )
    legacy_surplus_shaped = (
        rescue_summary["over_target_count"] > 0
        and rescue_summary["missing_explicit_surplus_approval_count"] == rescue_summary["over_target_count"]
        and rescue_summary["over_bridge_surplus_cap_count"] == 0
        and pair_pnl >= args.min_window_pair_pnl
    )

    bridge_blockers = sorted(set(hard_blockers))
    bridge_eligible = not bridge_blockers and (strict_target_clean or explicit_surplus_clean or legacy_surplus_shaped)
    evidence_class = "blocked"
    requires_fresh_explicit_surplus_run = False
    if bridge_eligible and strict_target_clean:
        evidence_class = "strict_target_cap_clean"
    elif bridge_eligible and explicit_surplus_clean:
        evidence_class = "explicit_surplus_clean"
    elif bridge_eligible and legacy_surplus_shaped:
        evidence_class = "legacy_surplus_shaped_requires_fresh_explicit_surplus_runtime"
        requires_fresh_explicit_surplus_run = True

    return {
        "root": str(root),
        "role": role,
        "instance_id": safety.get("instance_id"),
        "status": "BRIDGE_ELIGIBLE" if bridge_eligible else "BLOCKED",
        "evidence_class": evidence_class,
        "hard_blockers": bridge_blockers,
        "bridge_eligible": bridge_eligible,
        "requires_fresh_explicit_surplus_run": requires_fresh_explicit_surplus_run,
        "sample": {
            "accepted_actions": len(accepted),
            "queue_supported_fills": int(as_float(metrics.get("queue_supported_fills"))),
            "strict_rescue_closes": len(rescues),
            "strict_rescue_qty": as_float(metrics.get("strict_rescue_qty")),
            "filled_qty": filled_qty,
            "filled_cost": filled_cost,
        },
        "economics": {
            "pair_pnl": pair_pnl,
            "roi_on_filled_cost": as_float(metrics.get("roi_on_filled_cost")),
            "residual_qty": residual_qty,
            "residual_cost": residual_cost,
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
        },
        "source_age": {
            "accepted_l1_age_ms_max": accepted_l1_max,
            "rescue_l1_age_ms_max": rescue_l1_max,
        },
        "rescue_policy": {
            "runtime_salvage_net_cap": config.get("salvage_net_cap"),
            "runtime_strict_rescue_surplus_net_cap": config.get("strict_rescue_surplus_net_cap"),
            "runtime_strict_rescue_min_pair_pnl_after": config.get("strict_rescue_min_pair_pnl_after"),
            "target_rescue_net_cap": args.target_rescue_net_cap,
            "bridge_surplus_net_cap": args.bridge_surplus_net_cap,
            "bridge_min_pair_pnl_after": args.bridge_min_pair_pnl_after,
            **rescue_summary,
        },
    }


def aggregate_windows(windows: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [window for window in windows if window["bridge_eligible"]]
    aggregate = {
        "window_count": len(windows),
        "bridge_eligible_window_count": len(eligible),
        "accepted_actions": sum(window["sample"]["accepted_actions"] for window in eligible),
        "queue_supported_fills": sum(window["sample"]["queue_supported_fills"] for window in eligible),
        "strict_rescue_closes": sum(window["sample"]["strict_rescue_closes"] for window in eligible),
        "strict_rescue_qty": sum(window["sample"]["strict_rescue_qty"] for window in eligible),
        "pair_pnl": sum(window["economics"]["pair_pnl"] for window in eligible),
        "filled_qty": sum(window["sample"]["filled_qty"] for window in eligible),
        "filled_cost": sum(window["sample"]["filled_cost"] for window in eligible),
        "residual_qty": sum(window["economics"]["residual_qty"] for window in eligible),
        "residual_cost": sum(window["economics"]["residual_cost"] for window in eligible),
        "requires_fresh_explicit_surplus_run": any(
            window["requires_fresh_explicit_surplus_run"] for window in eligible
        ),
    }
    aggregate["residual_qty_share"] = (
        aggregate["residual_qty"] / aggregate["filled_qty"] if aggregate["filled_qty"] else None
    )
    aggregate["residual_cost_share"] = (
        aggregate["residual_cost"] / aggregate["filled_cost"] if aggregate["filled_cost"] else None
    )
    aggregate["roi_on_filled_cost"] = (
        aggregate["pair_pnl"] / aggregate["filled_cost"] if aggregate["filled_cost"] else None
    )
    return aggregate


def score(args: argparse.Namespace) -> dict[str, Any]:
    base_roots = [Path(root).expanduser().resolve() for root in args.base_roots]
    bridge_roots = [Path(root).expanduser().resolve() for root in args.bridge_roots]
    supporting_roots = [Path(root).expanduser().resolve() for root in args.supporting_roots]
    main_windows = [score_window(root, "base", args) for root in base_roots] + [
        score_window(root, "bridge_candidate", args) for root in bridge_roots
    ]
    supporting_windows = [score_window(root, "supporting", args) for root in supporting_roots]
    aggregate = aggregate_windows(main_windows)

    hard_blockers: list[str] = []
    if len(base_roots) + len(bridge_roots) < args.min_windows:
        hard_blockers.append("window_count_below_min")
    if aggregate["bridge_eligible_window_count"] < args.min_bridge_eligible_windows:
        hard_blockers.append("bridge_eligible_window_count_below_min")
    if any(not window["bridge_eligible"] for window in main_windows):
        hard_blockers.append("one_or_more_main_windows_blocked")
    if aggregate["strict_rescue_closes"] < args.min_total_rescue_closes:
        hard_blockers.append("total_strict_rescue_closes_below_min")
    if aggregate["accepted_actions"] < args.min_total_accepted_actions:
        hard_blockers.append("total_accepted_actions_below_min")
    if aggregate["queue_supported_fills"] < args.min_total_fills:
        hard_blockers.append("total_fills_below_min")
    if aggregate["pair_pnl"] < args.min_total_pair_pnl:
        hard_blockers.append("total_pair_pnl_below_min")
    if aggregate["residual_qty_share"] is None or aggregate["residual_qty_share"] > args.max_total_residual_qty_share:
        hard_blockers.append("total_residual_qty_share_above_max")
    if aggregate["residual_cost_share"] is None or aggregate["residual_cost_share"] > args.max_total_residual_cost_share:
        hard_blockers.append("total_residual_cost_share_above_max")

    would_clear_shadow_repeat_gap = not hard_blockers
    requires_fresh = aggregate["requires_fresh_explicit_surplus_run"]
    if would_clear_shadow_repeat_gap and requires_fresh:
        status = "KEEP_SOFT_MAINLINE_SURPLUS_BRIDGE_NEEDS_FRESH_EXPLICIT_SURPLUS_RUNTIME_LOCAL_ONLY"
    elif would_clear_shadow_repeat_gap:
        status = "KEEP_SOFT_MAINLINE_SURPLUS_BRIDGE_CLEARS_REPEAT_GAP_LOCAL_ONLY"
    else:
        status = "UNKNOWN_SOFT_MAINLINE_SURPLUS_BRIDGE_BLOCKED_LOCAL_ONLY"

    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "main_windows": main_windows,
        "supporting_windows": supporting_windows,
        "aggregate": aggregate,
        "thresholds": {
            "target_rescue_net_cap": args.target_rescue_net_cap,
            "bridge_surplus_net_cap": args.bridge_surplus_net_cap,
            "bridge_min_pair_pnl_after": args.bridge_min_pair_pnl_after,
            "min_windows": args.min_windows,
            "min_bridge_eligible_windows": args.min_bridge_eligible_windows,
            "min_total_rescue_closes": args.min_total_rescue_closes,
            "min_total_accepted_actions": args.min_total_accepted_actions,
            "min_total_fills": args.min_total_fills,
            "min_total_pair_pnl": args.min_total_pair_pnl,
            "max_total_residual_qty_share": args.max_total_residual_qty_share,
            "max_total_residual_cost_share": args.max_total_residual_cost_share,
            "min_window_accepted_actions": args.min_window_accepted_actions,
            "min_window_fills": args.min_window_fills,
            "min_window_pair_pnl": args.min_window_pair_pnl,
            "max_window_residual_qty_share": args.max_window_residual_qty_share,
            "max_window_residual_cost_share": args.max_window_residual_cost_share,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
        },
        "decision": {
            "deployable": False,
            "remote_runner_allowed": False,
            "shadow_review_ready": False,
            "would_clear_shadow_repeat_gap_if_reproduced_with_explicit_surplus_policy": (
                would_clear_shadow_repeat_gap and requires_fresh
            ),
            "fresh_explicit_surplus_runtime_required": requires_fresh,
            "recommended_next_profile": {
                "family": "soft_closeability_mainline",
                "risk_seed_closeability_soft_net_cap": 0.98,
                "risk_seed_closeability_debt_floor": 0.95,
                "risk_seed_closeability_debt_budget": 1.0,
                "risk_seed_pending_opp_credit": 0.5,
                "strict_rescue_target_net_cap": args.target_rescue_net_cap,
                "strict_rescue_surplus_net_cap": args.bridge_surplus_net_cap,
                "strict_rescue_min_pair_pnl_after": args.bridge_min_pair_pnl_after,
                "duration_s": 1800,
                "timeout_s": 2100,
                "pm_dry_run": True,
                "source_gates": "on",
                "diagnostics": "on",
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-roots", nargs="+", required=True)
    parser.add_argument("--bridge-roots", nargs="+", required=True)
    parser.add_argument("--supporting-roots", nargs="*", default=[])
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--target-rescue-net-cap", type=float, default=0.95)
    parser.add_argument("--bridge-surplus-net-cap", type=float, default=1.02)
    parser.add_argument("--bridge-min-pair-pnl-after", type=float, default=0.0)
    parser.add_argument("--min-windows", type=int, default=2)
    parser.add_argument("--min-bridge-eligible-windows", type=int, default=2)
    parser.add_argument("--min-total-rescue-closes", type=int, default=20)
    parser.add_argument("--min-total-accepted-actions", type=int, default=150)
    parser.add_argument("--min-total-fills", type=int, default=120)
    parser.add_argument("--min-total-pair-pnl", type=float, default=2.0)
    parser.add_argument("--max-total-residual-qty-share", type=float, default=0.25)
    parser.add_argument("--max-total-residual-cost-share", type=float, default=0.25)
    parser.add_argument("--min-window-accepted-actions", type=int, default=25)
    parser.add_argument("--min-window-fills", type=int, default=20)
    parser.add_argument("--min-window-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-window-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-window-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-accepted-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    args = parser.parse_args()

    started = time.time()
    result = score(args)
    scorecard = {
        "artifact": "xuan_soft_mainline_surplus_bridge_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_soft_mainline_surplus_bridge_scorer.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
