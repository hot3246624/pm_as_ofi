#!/usr/bin/env python3
"""Score xuan-frontier no-order strict-rescue lifecycle exports.

This scorer is intentionally narrow: it validates normalized no-order lifecycle
plumbing and source-linkage invariants. It does not promote strategy economics,
and forced diagnostics must stay marked as forced diagnostics.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


REQUIRED_RESCUE_FIELDS = [
    "source_sequence_id",
    "market_md_source_sequence_id",
    "strict_rescue_l2_source_sequence_id",
    "source_lot_id",
    "source_lot_sequence_id",
    "fee_per_share",
    "net_pair_cost",
    "strict_rescue_l1_age_ms",
    "strict_rescue_l2_age_ms",
]

REQUIRED_ACCEPTED_ACTION_FIELDS = [
    "source_sequence_id",
    "market_md_source_sequence_id",
    "source_quality_l2_source_sequence_id",
    "source_quality_l1_age_ms",
]


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


def count_nonempty(rows: list[dict[str, str]], field: str) -> int:
    return sum(1 for row in rows if row.get(field) not in (None, ""))


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def load_lifecycle_rows(root: Path) -> dict[str, list[dict[str, str]]]:
    rows = {
        "actions": [],
        "fills": [],
        "inventory": [],
        "lots": [],
        "rescues": [],
    }
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = read_json(manifest_path)
        files = manifest.get("files", {})
        rows["actions"].extend(read_csv(root / files.get("would_action_decisions", "")))
        rows["fills"].extend(read_csv(root / files.get("would_fill_events", "")))
        rows["inventory"].extend(read_csv(root / files.get("simulated_inventory_events", "")))
        rows["lots"].extend(read_csv(root / files.get("residual_fifo_lots", "")))
        rows["rescues"].extend(read_csv(root / files.get("strict_rescue_closes", "")))
    return rows


def validate_lifecycle_manifests(root: Path) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = read_json(manifest_path)
        missing_files = [
            rel
            for rel in manifest.get("files", {}).values()
            if not (root / rel).exists()
        ]
        checks.append(
            {
                "manifest": str(manifest_path),
                "slug": manifest.get("slug"),
                "orders_sent": manifest.get("orders_sent"),
                "missing_files": missing_files,
                "row_counts": manifest.get("row_counts", {}),
                "status": (
                    "PASS"
                    if manifest.get("orders_sent") is False and not missing_files
                    else "FAIL"
                ),
            }
        )
    return checks


def score(
    root: Path,
    forced_diagnostic: bool,
    min_rescue_rows: int,
    max_action_l1_age_ms: float,
    max_rescue_l1_age_ms: float,
) -> dict[str, Any]:
    hard_blockers: list[str] = []
    warnings: list[str] = []
    missing_root_files = [
        name
        for name in [
            "manifest.json",
            "aggregate_report.json",
            "run_exit_code.txt",
            "run_stderr.log",
            "run_stdout.log",
        ]
        if not (root / name).exists()
    ]
    if missing_root_files:
        hard_blockers.append("missing_root_files")

    manifest: dict[str, Any] = {}
    aggregate: dict[str, Any] = {}
    if not missing_root_files:
        manifest = read_json(root / "manifest.json")
        aggregate = read_json(root / "aggregate_report.json")
        if (root / "run_exit_code.txt").read_text().strip() != "0":
            hard_blockers.append("nonzero_run_exit_code")
        if (root / "run_stderr.log").read_text():
            hard_blockers.append("stderr_not_empty")
        safety = manifest.get("safety", {})
        if safety.get("orders_sent") is not False or str(safety.get("dry_run")) != "1":
            hard_blockers.append("dry_run_safety_boundary_failed")

    lifecycle_checks = validate_lifecycle_manifests(root)
    if not lifecycle_checks or any(check["status"] != "PASS" for check in lifecycle_checks):
        hard_blockers.append("normalized_lifecycle_manifest_failed")

    rows = load_lifecycle_rows(root)
    accepted = [row for row in rows["actions"] if row.get("decision") == "accept"]
    rescues = rows["rescues"]
    fill_source_sequences = {
        row.get("source_sequence_id")
        for row in rows["fills"]
        if row.get("source_sequence_id")
    }
    fill_lot_ids = {
        row.get("lot_id")
        for row in rows["inventory"]
        if row.get("inventory_event_type") == "would_fill_add" and row.get("lot_id")
    }

    accepted_field_counts = {
        field: count_nonempty(accepted, field)
        for field in REQUIRED_ACCEPTED_ACTION_FIELDS
    }
    rescue_field_counts = {
        field: count_nonempty(rescues, field)
        for field in REQUIRED_RESCUE_FIELDS
    }

    if not accepted:
        hard_blockers.append("no_accepted_actions")
    for field, count in accepted_field_counts.items():
        if count != len(accepted):
            hard_blockers.append(f"accepted_missing_{field}")

    action_l1_ages = [
        value
        for value in (as_float(row.get("source_quality_l1_age_ms")) for row in accepted)
        if value is not None
    ]
    if action_l1_ages and max(action_l1_ages) > max_action_l1_age_ms:
        hard_blockers.append("accepted_action_l1_age_exceeds_limit")

    if len(rescues) < min_rescue_rows:
        hard_blockers.append("insufficient_strict_rescue_closes")
    for field, count in rescue_field_counts.items():
        if count != len(rescues):
            hard_blockers.append(f"rescue_missing_{field}")

    rescue_l1_ages = [
        value
        for value in (as_float(row.get("strict_rescue_l1_age_ms")) for row in rescues)
        if value is not None
    ]
    if rescue_l1_ages and max(rescue_l1_ages) > max_rescue_l1_age_ms:
        hard_blockers.append("rescue_l1_age_exceeds_limit")

    unmatched_source_lot_sequences = [
        row.get("source_lot_sequence_id")
        for row in rescues
        if row.get("source_lot_sequence_id") not in fill_source_sequences
    ]
    unmatched_source_lot_ids = [
        row.get("source_lot_id")
        for row in rescues
        if row.get("source_lot_id") not in fill_lot_ids
    ]
    if unmatched_source_lot_sequences:
        hard_blockers.append("rescue_source_lot_sequence_not_in_fills")
    if unmatched_source_lot_ids:
        hard_blockers.append("rescue_source_lot_id_not_in_inventory")

    metrics = aggregate.get("metrics", {})
    if float(metrics.get("strict_rescue_source_blocks") or 0) != 0:
        hard_blockers.append("strict_rescue_source_blocks_nonzero")

    pair_pnl = as_float(metrics.get("pair_pnl")) or 0.0
    net_pair_costs = [
        value
        for value in (as_float(row.get("net_pair_cost")) for row in rescues)
        if value is not None
    ]
    if forced_diagnostic:
        warnings.append("forced_diagnostic_not_economic_promotion_evidence")
        if pair_pnl < 0:
            warnings.append("forced_diagnostic_negative_pair_pnl_expected")
        if net_pair_costs and max(net_pair_costs) > 1.0:
            warnings.append("forced_diagnostic_net_pair_cost_above_one_expected")
    elif pair_pnl < 0:
        hard_blockers.append("negative_pair_pnl_for_non_forced_run")

    status = (
        "KEEP_NO_ORDER_STRICT_RESCUE_LIFECYCLE_SCORER_PASS_RESEARCH_ONLY"
        if not hard_blockers
        else "UNKNOWN_NO_ORDER_STRICT_RESCUE_LIFECYCLE_SCORER_BLOCKED"
    )
    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "warnings": sorted(set(warnings)),
        "output_root": str(root),
        "forced_diagnostic": forced_diagnostic,
        "safety": {
            "orders_sent": manifest.get("safety", {}).get("orders_sent"),
            "dry_run": manifest.get("safety", {}).get("dry_run"),
            "shared_ingress_root": manifest.get("safety", {}).get("shared_ingress_root"),
            "instance_id": manifest.get("safety", {}).get("instance_id"),
        },
        "lifecycle_manifest_checks": lifecycle_checks,
        "row_counts": {
            "accepted_actions": len(accepted),
            "would_action_decisions": len(rows["actions"]),
            "would_fill_events": len(rows["fills"]),
            "simulated_inventory_events": len(rows["inventory"]),
            "residual_fifo_lots": len(rows["lots"]),
            "strict_rescue_closes": len(rescues),
        },
        "accepted_action_source_counts": accepted_field_counts,
        "strict_rescue_source_counts": rescue_field_counts,
        "source_lot_linkage": {
            "fill_source_sequence_count": len(fill_source_sequences),
            "fill_lot_id_count": len(fill_lot_ids),
            "unmatched_source_lot_sequence_count": len(unmatched_source_lot_sequences),
            "unmatched_source_lot_id_count": len(unmatched_source_lot_ids),
        },
        "age_bounds": {
            "accepted_action_l1_age_ms_max": max(action_l1_ages) if action_l1_ages else None,
            "accepted_action_l1_age_ms_limit": max_action_l1_age_ms,
            "strict_rescue_l1_age_ms_max": max(rescue_l1_ages) if rescue_l1_ages else None,
            "strict_rescue_l1_age_ms_limit": max_rescue_l1_age_ms,
        },
        "diagnostic_metrics": {
            "candidates": metrics.get("candidates"),
            "queue_supported_fills": metrics.get("queue_supported_fills"),
            "strict_rescue_actions": metrics.get("strict_rescue_actions"),
            "strict_rescue_qty": metrics.get("strict_rescue_qty"),
            "strict_rescue_source_blocks": metrics.get("strict_rescue_source_blocks"),
            "pair_pnl": metrics.get("pair_pnl"),
            "net_pair_cost_min": min(net_pair_costs) if net_pair_costs else None,
            "net_pair_cost_max": max(net_pair_costs) if net_pair_costs else None,
            "residual_qty": metrics.get("residual_qty"),
            "residual_cost": metrics.get("residual_cost"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--forced-diagnostic", action="store_true")
    parser.add_argument("--min-rescue-rows", type=int, default=1)
    parser.add_argument("--max-action-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=1000.0)
    args = parser.parse_args()

    started = time.time()
    result = score(
        Path(args.output_root).expanduser().resolve(),
        forced_diagnostic=args.forced_diagnostic,
        min_rescue_rows=args.min_rescue_rows,
        max_action_l1_age_ms=args.max_action_l1_age_ms,
        max_rescue_l1_age_ms=args.max_rescue_l1_age_ms,
    )
    scorecard = {
        "artifact": "xuan_no_order_strict_rescue_lifecycle_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_strict_rescue_lifecycle_scorer.py",
        **result,
        "decision": {
            "deployable": False,
            "remote_runner_allowed": False,
            "interpretation": (
                "No-order strict-rescue lifecycle/source plumbing is internally consistent."
                if result["status"].startswith("KEEP_")
                else "No-order strict-rescue lifecycle/source plumbing needs review."
            ),
        },
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))
    if not result["status"].startswith("KEEP_"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
