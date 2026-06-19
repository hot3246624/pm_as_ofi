#!/usr/bin/env python3
"""Score repeated xuan-frontier no-order runtime windows.

This scorer is for shadow-review evidence maturity. It aggregates multiple
non-forced no-order dry-run windows only after checking each window's safety,
source, residual, economics, and closeability-debt invariants. Passing here is
still research-only review readiness, not deployable/live.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
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


def slug_debt_metrics(root: Path) -> dict[str, float]:
    open_max = 0.0
    max_open_max = 0.0
    for summary_path in sorted(root.glob("*.summary.json")):
        metrics = read_json(summary_path).get("metrics", {})
        open_max = max(open_max, as_float(metrics.get("closeability_debt_open")))
        max_open_max = max(max_open_max, as_float(metrics.get("closeability_debt_max_open")))
    return {
        "closeability_debt_open_per_slug_max": open_max,
        "closeability_debt_max_open_per_slug_max": max_open_max,
    }


def strict_rescue_net_pair_gate(
    rescues: list[dict[str, str]],
    target_cap: float,
    surplus_cap: float | None,
    surplus_floor: float | None,
) -> dict[str, Any]:
    blockers: list[str] = []
    target_values: list[float] = []
    surplus_values: list[float] = []
    surplus_projected: list[float] = []
    over_target = 0
    surplus_allowed = 0
    for row in rescues:
        net_pair = as_float(row.get("net_pair_cost"), float("nan"))
        if not math.isfinite(net_pair):
            continue
        if net_pair <= target_cap + 1e-12:
            target_values.append(net_pair)
            continue
        over_target += 1
        decision = row.get("strict_rescue_surplus_decision")
        projected_after = as_float(row.get("strict_rescue_projected_pair_pnl_after"), float("nan"))
        if decision != "allow_surplus":
            blockers.append("rescue_net_pair_cost_above_target_without_surplus_approval")
            continue
        if surplus_cap is None or surplus_floor is None:
            blockers.append("rescue_surplus_policy_missing_from_config")
            continue
        row_blockers: list[str] = []
        if net_pair > surplus_cap + 1e-12:
            row_blockers.append("rescue_surplus_net_pair_cost_above_cap")
        if not math.isfinite(projected_after):
            row_blockers.append("rescue_surplus_projected_pair_pnl_missing")
        elif projected_after < surplus_floor - 1e-12:
            row_blockers.append("rescue_surplus_projected_pair_pnl_below_floor")
        else:
            surplus_projected.append(projected_after)
        surplus_values.append(net_pair)
        blockers.extend(row_blockers)
        if not row_blockers:
            surplus_allowed += 1
    all_values = target_values + surplus_values
    return {
        "hard_blockers": sorted(set(blockers)),
        "rescue_net_pair_cost_max": max(all_values) if all_values else None,
        "rescue_target_cap_net_pair_cost_max": max(target_values) if target_values else None,
        "rescue_surplus_net_pair_cost_max": max(surplus_values) if surplus_values else None,
        "rescue_over_target_count": over_target,
        "rescue_surplus_allowed_count": surplus_allowed,
        "rescue_surplus_projected_pair_pnl_after_min": min(surplus_projected) if surplus_projected else None,
    }


def score_window(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    required = ["manifest.json", "aggregate_report.json", "run_exit_code.txt", "run_stderr.log"]
    missing = [name for name in required if not (root / name).exists()]
    if missing:
        return {
            "root": str(root),
            "status": "BLOCKED_WINDOW_MISSING_FILES",
            "hard_blockers": ["missing_files"],
            "missing_files": missing,
        }

    manifest = read_json(root / "manifest.json")
    aggregate = read_json(root / "aggregate_report.json")
    metrics = aggregate.get("metrics", {})
    blocked = aggregate.get("blocked", {})
    config = manifest.get("config", {})
    safety = manifest.get("safety", {})
    rows = read_lifecycle_rows(root)
    accepted = [row for row in rows["actions"] if row.get("decision") == "accept"]
    rescues = rows["rescues"]
    slugs_active = 0
    slugs_rescue = 0
    for lifecycle_path in root.glob("*.normalized_lifecycle_manifest.json"):
        row_counts = read_json(lifecycle_path).get("row_counts", {})
        if sum(row_counts.values()) > 0:
            slugs_active += 1
        if row_counts.get("strict_rescue_closes", 0) > 0:
            slugs_rescue += 1

    filled_qty = as_float(metrics.get("filled_qty"))
    filled_cost = as_float(metrics.get("filled_cost"))
    residual_qty = as_float(metrics.get("residual_qty"))
    residual_cost = as_float(metrics.get("residual_cost"))
    residual_qty_share = residual_qty / filled_qty if filled_qty else None
    residual_cost_share = residual_cost / filled_cost if filled_cost else None
    accepted_l1_max = max_float(accepted, "source_quality_l1_age_ms")
    rescue_l1_max = max_float(rescues, "strict_rescue_l1_age_ms")
    strict_rescue_surplus_net_cap = as_float(config.get("strict_rescue_surplus_net_cap"), float("nan"))
    strict_rescue_min_pair_pnl_after = as_float(config.get("strict_rescue_min_pair_pnl_after"), float("nan"))
    rescue_net_pair_gate = strict_rescue_net_pair_gate(
        rescues,
        args.max_rescue_net_pair_cost,
        strict_rescue_surplus_net_cap if math.isfinite(strict_rescue_surplus_net_cap) else None,
        strict_rescue_min_pair_pnl_after if math.isfinite(strict_rescue_min_pair_pnl_after) else None,
    )
    debt = slug_debt_metrics(root)
    soft_debt_budget = as_float(config.get("risk_seed_closeability_debt_budget"))

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
    if as_float(metrics.get("pair_pnl")) < args.min_window_pair_pnl:
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
    if rescue_net_pair_gate["rescue_net_pair_cost_max"] is None:
        hard_blockers.append("rescue_net_pair_cost_missing")
    hard_blockers.extend(rescue_net_pair_gate["hard_blockers"])
    if soft_debt_budget > 0:
        if debt["closeability_debt_open_per_slug_max"] > soft_debt_budget + 1e-12:
            hard_blockers.append("closeability_debt_open_per_slug_exceeds_config_budget")
        if debt["closeability_debt_max_open_per_slug_max"] > soft_debt_budget + 1e-12:
            hard_blockers.append("closeability_debt_max_open_per_slug_exceeds_config_budget")

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
            hard_blockers.append(f"rescue_missing_{field}")

    return {
        "root": str(root),
        "status": "PASS" if not hard_blockers else "BLOCKED",
        "hard_blockers": sorted(set(hard_blockers)),
        "instance_id": safety.get("instance_id"),
        "sample": {
            "active_slugs": slugs_active,
            "rescue_slugs": slugs_rescue,
            "accepted_actions": len(accepted),
            "queue_supported_fills": int(as_float(metrics.get("queue_supported_fills"))),
            "strict_rescue_closes": len(rescues),
            "strict_rescue_qty": as_float(metrics.get("strict_rescue_qty")),
            "filled_qty": filled_qty,
            "filled_cost": filled_cost,
        },
        "economics": {
            "pair_pnl": as_float(metrics.get("pair_pnl")),
            "roi_on_filled_cost": as_float(metrics.get("roi_on_filled_cost")),
            "residual_qty": residual_qty,
            "residual_cost": residual_cost,
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            **{key: value for key, value in rescue_net_pair_gate.items() if key != "hard_blockers"},
            "strict_rescue_l1_age_blocks": int(blocked.get("strict_rescue_l1_age") or 0),
        },
        "source_age": {
            "accepted_l1_age_ms_max": accepted_l1_max,
            "rescue_l1_age_ms_max": rescue_l1_max,
        },
        "closeability_risk": {
            **debt,
            "soft_closeability_debt_budget": soft_debt_budget,
        },
    }


def score(args: argparse.Namespace) -> dict[str, Any]:
    roots = [Path(root).expanduser().resolve() for root in args.output_roots]
    windows = [score_window(root, args) for root in roots]
    pass_windows = [window for window in windows if window["status"] == "PASS"]
    aggregate = {
        "window_count": len(windows),
        "pass_window_count": len(pass_windows),
        "accepted_actions": sum(window.get("sample", {}).get("accepted_actions", 0) for window in pass_windows),
        "queue_supported_fills": sum(window.get("sample", {}).get("queue_supported_fills", 0) for window in pass_windows),
        "strict_rescue_closes": sum(window.get("sample", {}).get("strict_rescue_closes", 0) for window in pass_windows),
        "strict_rescue_qty": sum(window.get("sample", {}).get("strict_rescue_qty", 0.0) for window in pass_windows),
        "pair_pnl": sum(window.get("economics", {}).get("pair_pnl", 0.0) for window in pass_windows),
        "filled_qty": sum(window.get("sample", {}).get("filled_qty", 0.0) for window in pass_windows),
        "filled_cost": sum(window.get("sample", {}).get("filled_cost", 0.0) for window in pass_windows),
        "residual_qty": sum(window.get("economics", {}).get("residual_qty", 0.0) for window in pass_windows),
        "residual_cost": sum(window.get("economics", {}).get("residual_cost", 0.0) for window in pass_windows),
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

    hard_blockers: list[str] = []
    if len(windows) < args.min_windows:
        hard_blockers.append("window_count_below_min")
    if len(pass_windows) < args.min_pass_windows:
        hard_blockers.append("pass_window_count_below_min")
    if any(window["status"] != "PASS" for window in windows):
        hard_blockers.append("one_or_more_windows_blocked")
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

    status = (
        "KEEP_REPEAT_WINDOW_SHADOW_REVIEW_SCORER_PASS_RESEARCH_ONLY_READY_FOR_REVIEW"
        if not hard_blockers
        else "UNKNOWN_REPEAT_WINDOW_SHADOW_REVIEW_SCORER_SAMPLE_OR_RISK_BLOCKED"
    )
    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "windows": windows,
        "aggregate": aggregate,
        "thresholds": {
            "min_windows": args.min_windows,
            "min_pass_windows": args.min_pass_windows,
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
            "max_rescue_net_pair_cost": args.max_rescue_net_pair_cost,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
        },
        "decision": {
            "deployable": False,
            "remote_runner_allowed": False,
            "shadow_review_ready": not hard_blockers,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-roots", nargs="+", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-windows", type=int, default=2)
    parser.add_argument("--min-pass-windows", type=int, default=2)
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
    parser.add_argument("--max-rescue-net-pair-cost", type=float, default=0.95)
    args = parser.parse_args()

    started = time.time()
    result = score(args)
    scorecard = {
        "artifact": "xuan_no_order_runtime_repeat_window_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_runtime_repeat_window_scorer.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
