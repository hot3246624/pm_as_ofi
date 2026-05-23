#!/usr/bin/env python3
"""Compare a closeability-gated no-order runtime run against a baseline.

This scorer is intentionally comparative. The closeability gate is meant to
reduce residual-producing admissions without relaxing strict-rescue economics.
Passing here means the gated profile is a better research-only runtime profile,
not deployable/live.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


CLOSEABILITY_BLOCK_REASONS = {
    "risk_seed_closeability_net_cap",
    "risk_seed_closeability_soft_net_cap",
    "risk_seed_closeability_debt_budget",
}


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


def load_actions(root: Path) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = read_json(manifest_path)
        action_file = manifest.get("files", {}).get("would_action_decisions")
        if action_file:
            actions.extend(read_csv(root / action_file))
    return actions


def load_run(root: Path) -> dict[str, Any]:
    required = ["manifest.json", "aggregate_report.json", "run_stderr.log"]
    missing = [name for name in required if not (root / name).exists()]
    if missing:
        return {"missing_files": missing, "root": str(root)}

    manifest = read_json(root / "manifest.json")
    aggregate = read_json(root / "aggregate_report.json")
    metrics = aggregate.get("metrics", {})
    blocked = aggregate.get("blocked", {})
    actions = load_actions(root)
    accepted = [row for row in actions if row.get("decision") == "accept"]
    closeability_blocks = [
        row for row in actions if row.get("block_reason") in CLOSEABILITY_BLOCK_REASONS
    ]
    closeability_block_counts: dict[str, int] = {}
    for row in closeability_blocks:
        reason = row.get("block_reason") or "unknown"
        closeability_block_counts[reason] = closeability_block_counts.get(reason, 0) + 1
    accepted_closeability_debt = sum(as_float(row.get("closeability_debt")) for row in accepted)
    accepted_closeability_debt_rows = sum(1 for row in accepted if as_float(row.get("closeability_debt")) > 0)
    filled_qty = as_float(metrics.get("filled_qty"))
    filled_cost = as_float(metrics.get("filled_cost"))
    residual_qty = as_float(metrics.get("residual_qty"))
    residual_cost = as_float(metrics.get("residual_cost"))
    diag_path = root / "strict_rescue_block_diagnostics_summary.json"
    diagnostics = read_json(diag_path) if diag_path.exists() else {}
    safety = manifest.get("safety", {})
    return {
        "root": str(root),
        "missing_files": [],
        "config": manifest.get("config", {}),
        "safety": {
            "orders_sent": safety.get("orders_sent"),
            "dry_run": safety.get("dry_run"),
            "instance_id": safety.get("instance_id"),
            "stderr_bytes": (root / "run_stderr.log").stat().st_size,
            "exit_code": (root / "run_exit_code.txt").read_text().strip()
            if (root / "run_exit_code.txt").exists()
            else None,
        },
        "sample": {
            "accepted_actions": len(accepted),
            "action_rows": len(actions),
            "queue_supported_fills": int(as_float(metrics.get("queue_supported_fills"))),
            "strict_rescue_closes": int(as_float(metrics.get("strict_rescue_actions"))),
            "strict_rescue_qty": as_float(metrics.get("strict_rescue_qty")),
            "closeability_blocks": len(closeability_blocks),
            "closeability_block_counts": closeability_block_counts,
            "accepted_closeability_debt_rows": accepted_closeability_debt_rows,
        },
        "economics": {
            "pair_pnl": as_float(metrics.get("pair_pnl")),
            "roi_on_filled_cost": as_float(metrics.get("roi_on_filled_cost")),
            "filled_qty": filled_qty,
            "filled_cost": filled_cost,
            "residual_qty": residual_qty,
            "residual_cost": residual_cost,
            "residual_qty_share": residual_qty / filled_qty if filled_qty else None,
            "residual_cost_share": residual_cost / filled_cost if filled_cost else None,
        },
        "closeability_risk": {
            "accepted_closeability_debt": accepted_closeability_debt,
            "closeability_debt_open": as_float(metrics.get("closeability_debt_open")),
            "closeability_debt_reserved": as_float(metrics.get("closeability_debt_reserved")),
            "closeability_debt_released": as_float(metrics.get("closeability_debt_released")),
            "closeability_debt_max_open": as_float(metrics.get("closeability_debt_max_open")),
        },
        "blocked": blocked,
        "diagnostics": {
            "reason_counts": diagnostics.get("reason_counts", {}),
            "net_pair_cost_stats": diagnostics.get("net_pair_cost_stats", {}),
        },
    }


def score(args: argparse.Namespace) -> dict[str, Any]:
    baseline = load_run(Path(args.baseline_root).expanduser().resolve())
    candidate = load_run(Path(args.candidate_root).expanduser().resolve())
    hard_blockers: list[str] = []
    warnings: list[str] = []

    if baseline.get("missing_files"):
        hard_blockers.append("baseline_missing_files")
    if candidate.get("missing_files"):
        hard_blockers.append("candidate_missing_files")
    if hard_blockers:
        return {
            "status": "BLOCKED_CLOSEABILITY_GATE_COMPARISON_MISSING_FILES",
            "hard_blockers": hard_blockers,
            "baseline": baseline,
            "candidate": candidate,
        }

    cand_safety = candidate["safety"]
    if cand_safety.get("orders_sent") is not False or str(cand_safety.get("dry_run")) != "1":
        hard_blockers.append("candidate_dry_run_safety_failed")
    if cand_safety.get("stderr_bytes") != 0:
        hard_blockers.append("candidate_stderr_not_empty")
    if cand_safety.get("exit_code") not in {"0", None}:
        hard_blockers.append("candidate_nonzero_exit")

    b_sample = baseline["sample"]
    c_sample = candidate["sample"]
    b_econ = baseline["economics"]
    c_econ = candidate["economics"]

    accepted_ratio = (
        c_sample["accepted_actions"] / b_sample["accepted_actions"]
        if b_sample["accepted_actions"]
        else 0.0
    )
    fills_ratio = (
        c_sample["queue_supported_fills"] / b_sample["queue_supported_fills"]
        if b_sample["queue_supported_fills"]
        else 0.0
    )
    pnl_delta = c_econ["pair_pnl"] - b_econ["pair_pnl"]
    residual_qty_share_delta = (
        c_econ["residual_qty_share"] - b_econ["residual_qty_share"]
        if c_econ["residual_qty_share"] is not None and b_econ["residual_qty_share"] is not None
        else None
    )
    residual_cost_share_delta = (
        c_econ["residual_cost_share"] - b_econ["residual_cost_share"]
        if c_econ["residual_cost_share"] is not None and b_econ["residual_cost_share"] is not None
        else None
    )

    if c_sample["closeability_blocks"] <= 0:
        warnings.append("candidate_has_no_closeability_blocks")
    if accepted_ratio < args.min_accepted_ratio:
        hard_blockers.append("accepted_actions_collapsed")
    if fills_ratio < args.min_fills_ratio:
        hard_blockers.append("fills_collapsed")
    if pnl_delta < -args.max_pair_pnl_drop:
        hard_blockers.append("pair_pnl_drop_exceeds_budget")
    if residual_qty_share_delta is None or residual_qty_share_delta > -args.min_residual_qty_share_improvement:
        hard_blockers.append("residual_qty_share_not_improved_enough")
    if residual_cost_share_delta is None or residual_cost_share_delta > -args.min_residual_cost_share_improvement:
        hard_blockers.append("residual_cost_share_not_improved_enough")

    status = (
        "KEEP_CLOSEABILITY_GATE_RUNTIME_COMPARISON_PASS_RESEARCH_ONLY"
        if not hard_blockers
        else "UNKNOWN_CLOSEABILITY_GATE_RUNTIME_COMPARISON_BLOCKED"
    )
    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "warnings": warnings,
        "thresholds": {
            "min_accepted_ratio": args.min_accepted_ratio,
            "min_fills_ratio": args.min_fills_ratio,
            "max_pair_pnl_drop": args.max_pair_pnl_drop,
            "min_residual_qty_share_improvement": args.min_residual_qty_share_improvement,
            "min_residual_cost_share_improvement": args.min_residual_cost_share_improvement,
        },
        "baseline": baseline,
        "candidate": candidate,
        "comparison": {
            "accepted_ratio": accepted_ratio,
            "fills_ratio": fills_ratio,
            "pair_pnl_delta": pnl_delta,
            "residual_qty_share_delta": residual_qty_share_delta,
            "residual_cost_share_delta": residual_cost_share_delta,
            "candidate_closeability_blocks": c_sample["closeability_blocks"],
            "candidate_closeability_block_counts": c_sample["closeability_block_counts"],
            "candidate_accepted_closeability_debt_rows": c_sample["accepted_closeability_debt_rows"],
            "candidate_closeability_risk": candidate["closeability_risk"],
        },
        "decision": {
            "deployable": False,
            "shadow_ready": False,
            "remote_runner_allowed": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-root", required=True)
    parser.add_argument("--candidate-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--min-accepted-ratio", type=float, default=0.6)
    parser.add_argument("--min-fills-ratio", type=float, default=0.6)
    parser.add_argument("--max-pair-pnl-drop", type=float, default=0.25)
    parser.add_argument("--min-residual-qty-share-improvement", type=float, default=0.05)
    parser.add_argument("--min-residual-cost-share-improvement", type=float, default=0.05)
    args = parser.parse_args()

    started = time.time()
    result = score(args)
    scorecard = {
        "artifact": "xuan_no_order_closeability_gate_comparison_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_closeability_gate_comparison_scorer.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
