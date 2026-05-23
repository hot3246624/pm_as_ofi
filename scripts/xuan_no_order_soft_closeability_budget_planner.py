#!/usr/bin/env python3
"""Plan soft closeability caps from an existing hard-cap runtime output.

This is a local planning tool, not a replay scorer. Hard-cap block rows from
older runner outputs do not include final admitted qty because they were blocked
before sizing. The planner therefore uses an explicit or observed typical qty
only to bound the next runtime debt-budget sweep.
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


def as_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        val = float(value)
        return val if math.isfinite(val) else default
    except (TypeError, ValueError):
        return default


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


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
    manifests = sorted(root.glob("*.normalized_lifecycle_manifest.json"))
    if manifests:
        for manifest_path in manifests:
            manifest = read_json(manifest_path)
            action_file = manifest.get("files", {}).get("would_action_decisions")
            if action_file:
                actions.extend(read_csv(root / action_file))
        return actions
    for action_path in sorted(root.glob("*.would_action_decisions.csv")):
        actions.extend(read_csv(action_path))
    return actions


def plan(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.runtime_root).expanduser().resolve()
    actions = load_actions(root)
    accepted = [row for row in actions if row.get("decision") == "accept"]
    closeability_blocks = [
        row
        for row in actions
        if row.get("block_reason") == "risk_seed_closeability_net_cap"
        and as_float(row.get("closeability_net_pair_cost")) is not None
    ]
    accepted_qtys = [
        qty for qty in (as_float(row.get("qty")) for row in accepted) if qty is not None and qty > 0
    ]
    typical_qty = args.typical_qty
    if typical_qty is None:
        typical_qty = statistics.median(accepted_qtys) if accepted_qtys else 2.0

    floor = args.debt_floor
    caps = [float(item) for item in args.soft_caps.split(",") if item.strip()]
    budgets = [float(item) for item in args.debt_budgets.split(",") if item.strip()]
    costs = [
        cost
        for cost in (as_float(row.get("closeability_net_pair_cost")) for row in closeability_blocks)
        if cost is not None
    ]

    cap_rows: list[dict[str, Any]] = []
    for cap in caps:
        eligible_costs = [cost for cost in costs if floor < cost <= cap + 1e-12]
        debt_per_share = [max(0.0, cost - floor) for cost in eligible_costs]
        estimated_debts = [debt * typical_qty for debt in debt_per_share]
        cumulative_debt = sum(estimated_debts)
        budget_pass_counts = {}
        for budget in budgets:
            used = 0.0
            count = 0
            for debt in estimated_debts:
                if used + debt <= budget + 1e-12:
                    used += debt
                    count += 1
            budget_pass_counts[str(budget)] = {
                "estimated_pass_rows": count,
                "estimated_debt_used": used,
            }
        cap_rows.append(
            {
                "soft_cap": cap,
                "eligible_block_rows": len(eligible_costs),
                "eligible_share_of_closeability_blocks": (
                    len(eligible_costs) / len(closeability_blocks) if closeability_blocks else 0.0
                ),
                "debt_per_share": {
                    "min": pct(debt_per_share, 0.0),
                    "p50": pct(debt_per_share, 0.5),
                    "p90": pct(debt_per_share, 0.9),
                    "p99": pct(debt_per_share, 0.99),
                    "max": pct(debt_per_share, 1.0),
                },
                "estimated_debt_with_typical_qty": {
                    "typical_qty": typical_qty,
                    "total_if_all_eligible_pass": cumulative_debt,
                    "per_row_p50": pct(estimated_debts, 0.5),
                    "per_row_p90": pct(estimated_debts, 0.9),
                    "budget_pass_counts": budget_pass_counts,
                },
            }
        )

    recommendation = {
        "soft_cap": 0.98,
        "debt_floor": floor,
        "debt_budget_candidates": [0.5, 1.0, 2.0],
        "reason": (
            "0.98 recovers most 0.95 hard-cap blocks in this upper-bound view, "
            "while the debt budget bounds risk-increasing seed admissions. "
            "Budget must be validated in a real no-order runtime run because "
            "blocked rows lack final qty and would alter cooldown/inventory state."
        ),
    }

    return {
        "status": "KEEP_SOFT_CLOSEABILITY_BUDGET_PLAN_LOCAL_ONLY",
        "runtime_root": str(root),
        "inputs": {
            "action_rows": len(actions),
            "accepted_rows": len(accepted),
            "closeability_block_rows_with_cost": len(closeability_blocks),
            "accepted_qty_count": len(accepted_qtys),
            "accepted_qty_median": statistics.median(accepted_qtys) if accepted_qtys else None,
            "typical_qty_used": typical_qty,
            "debt_floor": floor,
            "soft_caps": caps,
            "debt_budgets": budgets,
        },
        "cost_distribution": {
            "min": pct(costs, 0.0),
            "p50": pct(costs, 0.5),
            "p90": pct(costs, 0.9),
            "p99": pct(costs, 0.99),
            "max": pct(costs, 1.0),
        },
        "cap_plan": cap_rows,
        "recommendation": recommendation,
        "limitations": [
            "Local planning only; not replay evidence and not promotion evidence.",
            "Hard-cap block rows do not include final admitted qty, so debt is estimated with typical_qty.",
            "Extra admissions would change cooldown, inventory, surplus budget, fills, and rescue opportunities.",
        ],
        "decision": {
            "deployable": False,
            "shadow_ready": False,
            "remote_runner_allowed": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--debt-floor", type=float, default=0.95)
    parser.add_argument("--soft-caps", default="0.97,0.975,0.98,0.985,0.99")
    parser.add_argument("--debt-budgets", default="0.25,0.5,1.0,2.0,5.0,10.0")
    parser.add_argument("--typical-qty", type=float, default=None)
    args = parser.parse_args()

    started = time.time()
    result = plan(args)
    scorecard = {
        "artifact": "xuan_no_order_soft_closeability_budget_planner",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_soft_closeability_budget_planner.py",
        **result,
    }
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
