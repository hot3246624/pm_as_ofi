#!/usr/bin/env python3
"""Score xuan-frontier no-order runtime output against shadow-review gates.

This is stricter than the lifecycle scorer: it requires enough non-forced
runtime sample, source-clean strict rescue closes, positive pair surplus, and a
bounded residual budget. Passing this scorer still means shadow-review
readiness only, not deployable/live.
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


def as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def load_rows(root: Path) -> dict[str, list[dict[str, str]]]:
    rows = {"actions": [], "fills": [], "rescues": [], "lots": []}
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = read_json(manifest_path)
        files = manifest.get("files", {})
        rows["actions"].extend(read_csv(root / files.get("would_action_decisions", "")))
        rows["fills"].extend(read_csv(root / files.get("would_fill_events", "")))
        rows["rescues"].extend(read_csv(root / files.get("strict_rescue_closes", "")))
        rows["lots"].extend(read_csv(root / files.get("residual_fifo_lots", "")))
    return rows


def load_slug_metrics(root: Path) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    for summary_path in sorted(root.glob("*.summary.json")):
        summary = read_json(summary_path)
        slug = summary.get("slug") or summary_path.name.replace(".summary.json", "")
        metrics.append({"slug": slug, "metrics": summary.get("metrics", {})})
    return metrics


def max_float(rows: list[dict[str, str]], field: str) -> float | None:
    vals = [as_float(row.get(field)) for row in rows]
    nums = [val for val in vals if val is not None]
    return max(nums) if nums else None


def count_nonempty(rows: list[dict[str, str]], field: str) -> int:
    return sum(1 for row in rows if row.get(field) not in (None, ""))


def sum_float(rows: list[dict[str, str]], field: str) -> float:
    return sum(val for val in (as_float(row.get(field)) for row in rows) if val is not None)


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
        net_pair = as_float(row.get("net_pair_cost"))
        if net_pair is None:
            continue
        if net_pair <= target_cap + 1e-12:
            target_values.append(net_pair)
            continue
        over_target += 1
        decision = row.get("strict_rescue_surplus_decision")
        projected_after = as_float(row.get("strict_rescue_projected_pair_pnl_after"))
        if decision != "allow_surplus":
            blockers.append("rescue_net_pair_cost_above_target_without_surplus_approval")
            continue
        if surplus_cap is None or surplus_floor is None:
            blockers.append("rescue_surplus_policy_missing_from_config")
            continue
        row_blockers: list[str] = []
        if net_pair > surplus_cap + 1e-12:
            row_blockers.append("rescue_surplus_net_pair_cost_above_cap")
        if projected_after is None:
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


def score(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    hard_blockers: list[str] = []
    root_files = ["manifest.json", "aggregate_report.json", "run_exit_code.txt", "run_stderr.log"]
    missing = [name for name in root_files if not (root / name).exists()]
    if missing:
        return {
            "status": "BLOCKED_RUNTIME_SHADOW_READINESS_MISSING_FILES",
            "hard_blockers": ["missing_root_files"],
            "missing_files": missing,
            "output_root": str(root),
        }

    manifest = read_json(root / "manifest.json")
    aggregate = read_json(root / "aggregate_report.json")
    config = manifest.get("config", {})
    metrics = aggregate.get("metrics", {})
    blocked = aggregate.get("blocked", {})
    rows = load_rows(root)
    slug_metrics = load_slug_metrics(root)
    accepted = [row for row in rows["actions"] if row.get("decision") == "accept"]
    rescues = rows["rescues"]
    slugs_with_activity = sum(
        1
        for path in root.glob("*.normalized_lifecycle_manifest.json")
        if sum(read_json(path).get("row_counts", {}).values()) > 0
    )
    slugs_with_rescue = sum(
        1
        for path in root.glob("*.normalized_lifecycle_manifest.json")
        if read_json(path).get("row_counts", {}).get("strict_rescue_closes", 0) > 0
    )

    if (root / "run_exit_code.txt").read_text().strip() != "0":
        hard_blockers.append("nonzero_run_exit_code")
    if (root / "run_stderr.log").read_text():
        hard_blockers.append("stderr_not_empty")
    safety = manifest.get("safety", {})
    if safety.get("orders_sent") is not False or str(safety.get("dry_run")) != "1":
        hard_blockers.append("dry_run_safety_boundary_failed")

    accepted_actions = len(accepted)
    fills = int(metrics.get("queue_supported_fills") or 0)
    rescue_count = len(rescues)
    pair_pnl = float(metrics.get("pair_pnl") or 0.0)
    filled_qty = float(metrics.get("filled_qty") or 0.0)
    filled_cost = float(metrics.get("filled_cost") or 0.0)
    residual_qty = float(metrics.get("residual_qty") or 0.0)
    residual_cost = float(metrics.get("residual_cost") or 0.0)
    residual_qty_share = residual_qty / filled_qty if filled_qty else None
    residual_cost_share = residual_cost / filled_cost if filled_cost else None
    rescue_qty = float(metrics.get("strict_rescue_qty") or 0.0)
    strict_rescue_source_blocks = float(metrics.get("strict_rescue_source_blocks") or 0.0)
    strict_l1_blocks = int(blocked.get("strict_rescue_l1_age") or 0)
    accepted_l1_max = max_float(accepted, "source_quality_l1_age_ms")
    rescue_l1_max = max_float(rescues, "strict_rescue_l1_age_ms")
    strict_rescue_surplus_net_cap = as_float(config.get("strict_rescue_surplus_net_cap"))
    strict_rescue_min_pair_pnl_after = as_float(config.get("strict_rescue_min_pair_pnl_after"))
    rescue_net_pair_gate = strict_rescue_net_pair_gate(
        rescues,
        args.max_rescue_net_pair_cost,
        strict_rescue_surplus_net_cap,
        strict_rescue_min_pair_pnl_after,
    )
    closeability_debt_open = float(metrics.get("closeability_debt_open") or 0.0)
    closeability_debt_reserved = float(metrics.get("closeability_debt_reserved") or 0.0)
    closeability_debt_released = float(metrics.get("closeability_debt_released") or 0.0)
    closeability_debt_max_open = float(metrics.get("closeability_debt_max_open") or 0.0)
    accepted_closeability_debt = sum_float(accepted, "closeability_debt")
    accepted_closeability_debt_rows = sum(
        1 for row in accepted if (as_float(row.get("closeability_debt")) or 0.0) > 1e-12
    )
    residual_closeability_debt = sum_float(rows["lots"], "closeability_debt")
    soft_closeability_cap = as_float(config.get("risk_seed_closeability_soft_net_cap"))
    soft_closeability_debt_budget = float(config.get("risk_seed_closeability_debt_budget") or 0.0)
    closeability_debt_accounting_delta = closeability_debt_reserved - closeability_debt_released - closeability_debt_open
    closeability_residual_delta = closeability_debt_open - residual_closeability_debt
    closeability_debt_open_per_slug_max = max(
        (float(slug.get("metrics", {}).get("closeability_debt_open") or 0.0) for slug in slug_metrics),
        default=0.0,
    )
    closeability_debt_max_open_per_slug_max = max(
        (float(slug.get("metrics", {}).get("closeability_debt_max_open") or 0.0) for slug in slug_metrics),
        default=0.0,
    )
    closeability_debt_per_slug = [
        {
            "slug": slug.get("slug"),
            "closeability_debt_open": float(slug.get("metrics", {}).get("closeability_debt_open") or 0.0),
            "closeability_debt_max_open": float(slug.get("metrics", {}).get("closeability_debt_max_open") or 0.0),
            "closeability_debt_reserved": float(slug.get("metrics", {}).get("closeability_debt_reserved") or 0.0),
            "closeability_debt_released": float(slug.get("metrics", {}).get("closeability_debt_released") or 0.0),
        }
        for slug in slug_metrics
    ]

    if accepted_actions < args.min_accepted_actions:
        hard_blockers.append("sample_accepted_actions_below_min")
    if fills < args.min_fills:
        hard_blockers.append("sample_fills_below_min")
    if rescue_count < args.min_rescue_closes:
        hard_blockers.append("sample_strict_rescue_closes_below_min")
    if slugs_with_activity < args.min_active_slugs:
        hard_blockers.append("active_slug_count_below_min")
    if slugs_with_rescue < args.min_rescue_slugs:
        hard_blockers.append("rescue_slug_count_below_min")
    if pair_pnl < args.min_pair_pnl:
        hard_blockers.append("pair_pnl_below_min")
    if rescue_qty < args.min_rescue_qty:
        hard_blockers.append("strict_rescue_qty_below_min")
    if residual_qty_share is None or residual_qty_share > args.max_residual_qty_share:
        hard_blockers.append("residual_qty_share_above_max")
    if residual_cost_share is None or residual_cost_share > args.max_residual_cost_share:
        hard_blockers.append("residual_cost_share_above_max")
    if strict_rescue_source_blocks != 0:
        hard_blockers.append("strict_rescue_source_blocks_nonzero")
    if accepted_l1_max is None or accepted_l1_max > args.max_accepted_l1_age_ms:
        hard_blockers.append("accepted_l1_age_above_max")
    if rescue_l1_max is None or rescue_l1_max > args.max_rescue_l1_age_ms:
        hard_blockers.append("rescue_l1_age_above_max")
    if rescue_net_pair_gate["rescue_net_pair_cost_max"] is None:
        hard_blockers.append("rescue_net_pair_cost_missing")
    hard_blockers.extend(rescue_net_pair_gate["hard_blockers"])
    if closeability_debt_open > args.max_closeability_debt_open + 1e-12:
        hard_blockers.append("closeability_debt_open_above_max")
    if closeability_debt_max_open > args.max_closeability_debt_max_open + 1e-12:
        hard_blockers.append("closeability_debt_max_open_above_max")
    if (
        args.max_closeability_debt_open_per_slug is not None
        and closeability_debt_open_per_slug_max > args.max_closeability_debt_open_per_slug + 1e-12
    ):
        hard_blockers.append("closeability_debt_open_per_slug_above_max")
    if (
        args.max_closeability_debt_max_open_per_slug is not None
        and closeability_debt_max_open_per_slug_max > args.max_closeability_debt_max_open_per_slug + 1e-12
    ):
        hard_blockers.append("closeability_debt_max_open_per_slug_above_max")
    if soft_closeability_cap is not None:
        if closeability_debt_open_per_slug_max > soft_closeability_debt_budget + 1e-12:
            hard_blockers.append("closeability_debt_open_per_slug_exceeds_config_budget")
        if closeability_debt_max_open_per_slug_max > soft_closeability_debt_budget + 1e-12:
            hard_blockers.append("closeability_debt_max_open_per_slug_exceeds_config_budget")
    if abs(closeability_debt_accounting_delta) > args.closeability_debt_tolerance:
        hard_blockers.append("closeability_debt_accounting_mismatch")
    if abs(closeability_residual_delta) > args.closeability_debt_tolerance:
        hard_blockers.append("closeability_debt_residual_mismatch")

    for field in (
        "source_sequence_id",
        "market_md_source_sequence_id",
        "strict_rescue_l2_source_sequence_id",
        "source_lot_id",
        "source_lot_sequence_id",
        "fee_per_share",
        "net_pair_cost",
    ):
        if count_nonempty(rescues, field) != rescue_count:
            hard_blockers.append(f"rescue_missing_{field}")

    status = (
        "KEEP_RUNTIME_SHADOW_REVIEW_SCORER_PASS_RESEARCH_ONLY_READY_FOR_REVIEW"
        if not hard_blockers
        else "UNKNOWN_RUNTIME_SHADOW_REVIEW_SCORER_SAMPLE_OR_RISK_BLOCKED"
    )
    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "output_root": str(root),
        "thresholds": {
            "min_accepted_actions": args.min_accepted_actions,
            "min_fills": args.min_fills,
            "min_rescue_closes": args.min_rescue_closes,
            "min_active_slugs": args.min_active_slugs,
            "min_rescue_slugs": args.min_rescue_slugs,
            "min_pair_pnl": args.min_pair_pnl,
            "min_rescue_qty": args.min_rescue_qty,
            "max_residual_qty_share": args.max_residual_qty_share,
            "max_residual_cost_share": args.max_residual_cost_share,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
            "target_rescue_net_pair_cost": args.max_rescue_net_pair_cost,
            "strict_rescue_surplus_net_cap": strict_rescue_surplus_net_cap,
            "strict_rescue_min_pair_pnl_after": strict_rescue_min_pair_pnl_after,
            "max_closeability_debt_open": args.max_closeability_debt_open,
            "max_closeability_debt_max_open": args.max_closeability_debt_max_open,
            "max_closeability_debt_open_per_slug": args.max_closeability_debt_open_per_slug,
            "max_closeability_debt_max_open_per_slug": args.max_closeability_debt_max_open_per_slug,
            "closeability_debt_tolerance": args.closeability_debt_tolerance,
        },
        "safety": {
            "orders_sent": safety.get("orders_sent"),
            "dry_run": safety.get("dry_run"),
            "instance_id": safety.get("instance_id"),
            "shared_ingress_root": safety.get("shared_ingress_root"),
        },
        "sample": {
            "active_slugs": slugs_with_activity,
            "rescue_slugs": slugs_with_rescue,
            "accepted_actions": accepted_actions,
            "queue_supported_fills": fills,
            "strict_rescue_closes": rescue_count,
            "strict_rescue_qty": rescue_qty,
            "filled_qty": filled_qty,
            "filled_cost": filled_cost,
        },
        "economics": {
            "pair_pnl": pair_pnl,
            "roi_on_filled_cost": metrics.get("roi_on_filled_cost"),
            **{key: value for key, value in rescue_net_pair_gate.items() if key != "hard_blockers"},
            "strict_rescue_source_blocks": strict_rescue_source_blocks,
            "strict_rescue_l1_age_blocks": strict_l1_blocks,
        },
        "residual": {
            "residual_qty": residual_qty,
            "residual_cost": residual_cost,
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "residual_closeability_debt": residual_closeability_debt,
        },
        "closeability_risk": {
            "soft_closeability_cap": soft_closeability_cap,
            "soft_closeability_debt_budget": soft_closeability_debt_budget,
            "accepted_closeability_debt_rows": accepted_closeability_debt_rows,
            "accepted_closeability_debt": accepted_closeability_debt,
            "closeability_debt_open": closeability_debt_open,
            "closeability_debt_reserved": closeability_debt_reserved,
            "closeability_debt_released": closeability_debt_released,
            "closeability_debt_max_open": closeability_debt_max_open,
            "closeability_debt_open_per_slug_max": closeability_debt_open_per_slug_max,
            "closeability_debt_max_open_per_slug_max": closeability_debt_max_open_per_slug_max,
            "closeability_debt_per_slug": closeability_debt_per_slug,
            "closeability_debt_accounting_delta": closeability_debt_accounting_delta,
            "closeability_residual_delta": closeability_residual_delta,
        },
        "source_age": {
            "accepted_l1_age_ms_max": accepted_l1_max,
            "rescue_l1_age_ms_max": rescue_l1_max,
        },
        "decision": {
            "deployable": False,
            "remote_runner_allowed": False,
            "shadow_review_ready": not hard_blockers,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-accepted-actions", type=int, default=100)
    parser.add_argument("--min-fills", type=int, default=60)
    parser.add_argument("--min-rescue-closes", type=int, default=20)
    parser.add_argument("--min-active-slugs", type=int, default=5)
    parser.add_argument("--min-rescue-slugs", type=int, default=3)
    parser.add_argument("--min-pair-pnl", type=float, default=1.0)
    parser.add_argument("--min-rescue-qty", type=float, default=10.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.25)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.25)
    parser.add_argument("--max-accepted-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-rescue-net-pair-cost", type=float, default=0.95)
    parser.add_argument("--max-closeability-debt-open", type=float, default=1_000_000_000.0)
    parser.add_argument("--max-closeability-debt-max-open", type=float, default=1_000_000_000.0)
    parser.add_argument("--max-closeability-debt-open-per-slug", type=float)
    parser.add_argument("--max-closeability-debt-max-open-per-slug", type=float)
    parser.add_argument("--closeability-debt-tolerance", type=float, default=1e-5)
    args = parser.parse_args()

    started = time.time()
    result = score(Path(args.output_root).expanduser().resolve(), args)
    scorecard = {
        "artifact": "xuan_no_order_runtime_shadow_readiness_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_runtime_shadow_readiness_scorer.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
