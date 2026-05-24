#!/usr/bin/env python3
"""Classify no-order residual lots by maturity and materiality.

This scorer is local/read-only. It does not relax economics or certify shadow
readiness; it separates mature/material residual risk from young or tiny lots so
thin runtime windows are not misdiagnosed as source, L1-age, or net-cap issues.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def summarize(values: list[float]) -> dict[str, float | int]:
    vals = sorted(v for v in values if math.isfinite(v))
    if not vals:
        return {}

    def pct(q: float) -> float:
        if len(vals) == 1:
            return vals[0]
        pos = (len(vals) - 1) * q
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return vals[lo]
        weight = pos - lo
        return vals[lo] * (1.0 - weight) + vals[hi] * weight

    return {
        "count": len(vals),
        "min": round(vals[0], 6),
        "p50": round(pct(0.5), 6),
        "p90": round(pct(0.9), 6),
        "p99": round(pct(0.99), 6),
        "max": round(vals[-1], 6),
    }


def read_residual_lots(root: Path, completed_ms: int) -> list[dict[str, Any]]:
    lots: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.residual_fifo_lots.csv")):
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not row:
                    continue
                fill_ms = as_int(row.get("fill_ms"))
                cost = as_float(row.get("cost"))
                qty = as_float(row.get("qty"))
                age_ms = max(0, completed_ms - fill_ms) if completed_ms and fill_ms else 0
                lots.append(
                    {
                        "slug": row.get("slug") or path.name.replace(".residual_fifo_lots.csv", ""),
                        "side": row.get("side"),
                        "qty": qty,
                        "px": as_float(row.get("px")),
                        "cost": cost,
                        "fill_ms": fill_ms,
                        "age_ms_at_run_end": age_ms,
                        "quote_intent_id": row.get("quote_intent_id"),
                        "source_order_id": row.get("source_order_id"),
                    }
                )
    return lots


def classify_lot(lot: dict[str, Any], salvage_age_ms: float, salvage_min_lot_cost: float) -> str:
    mature = as_float(lot.get("age_ms_at_run_end")) >= salvage_age_ms
    material = as_float(lot.get("cost")) >= salvage_min_lot_cost
    if mature and material:
        return "mature_material"
    if mature:
        return "mature_tiny"
    if material:
        return "young_material"
    return "young_tiny"


def scan(root: Path, max_material_residual_cost: float) -> dict[str, Any]:
    manifest = load_json(root / "manifest.json")
    aggregate = load_json(root / "aggregate_report.json")
    metrics = aggregate.get("metrics") or {}
    completed_ms = as_int(manifest.get("completed_ms"))

    config: dict[str, Any] = {}
    for summary_path in sorted(root.glob("*.summary.json")):
        summary = load_json(summary_path)
        if isinstance(summary.get("config"), dict):
            config = summary["config"]
            break

    salvage_age_ms = as_float(config.get("salvage_age_ms"), 30000.0)
    salvage_min_lot_cost = as_float(config.get("salvage_min_lot_cost"), 0.25)
    lots = read_residual_lots(root, completed_ms)

    class_counts: Counter[str] = Counter()
    class_cost: defaultdict[str, float] = defaultdict(float)
    class_qty: defaultdict[str, float] = defaultdict(float)
    lot_ages: list[float] = []
    lot_costs: list[float] = []
    enriched: list[dict[str, Any]] = []
    for lot in lots:
        klass = classify_lot(lot, salvage_age_ms, salvage_min_lot_cost)
        class_counts[klass] += 1
        class_cost[klass] += as_float(lot.get("cost"))
        class_qty[klass] += as_float(lot.get("qty"))
        lot_ages.append(as_float(lot.get("age_ms_at_run_end")))
        lot_costs.append(as_float(lot.get("cost")))
        row = dict(lot)
        row["residual_class"] = klass
        row["age_ms_to_salvage"] = round(max(0.0, salvage_age_ms - as_float(lot.get("age_ms_at_run_end"))), 6)
        row["cost_to_material"] = round(max(0.0, salvage_min_lot_cost - as_float(lot.get("cost"))), 6)
        enriched.append(row)

    mature_material_cost = class_cost["mature_material"]
    material_cost = class_cost["mature_material"] + class_cost["young_material"]
    tiny_cost = class_cost["young_tiny"] + class_cost["mature_tiny"]
    hard_blockers: list[str] = []
    if mature_material_cost > max_material_residual_cost:
        hard_blockers.append("mature_material_residual_cost_above_max")
    if class_counts["young_material"] > 0:
        hard_blockers.append("young_material_residual_present")

    if hard_blockers:
        status = "UNKNOWN_YOUNG_TINY_RESIDUAL_SCORER_MATERIAL_RISK_PRESENT"
    elif lots:
        status = "KEEP_YOUNG_TINY_RESIDUAL_SCORER_NO_MATURE_MATERIAL_RISK_LOCAL_ONLY"
    else:
        status = "KEEP_YOUNG_TINY_RESIDUAL_SCORER_NO_RESIDUAL_LOCAL_ONLY"

    return {
        "artifact": "xuan_no_order_young_tiny_residual_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "root": str(root),
        "status": status,
        "decision": {
            "deployable": False,
            "shadow_ready": False,
            "remote_runner_allowed": False,
            "controller_change_required": False,
        },
        "thresholds": {
            "salvage_age_ms": salvage_age_ms,
            "salvage_min_lot_cost": salvage_min_lot_cost,
            "max_material_residual_cost": max_material_residual_cost,
        },
        "runtime_metrics": {
            "accepted_actions": metrics.get("candidates"),
            "queue_supported_fills": metrics.get("queue_supported_fills"),
            "strict_rescue_closes": metrics.get("strict_rescue_actions"),
            "pair_pnl": metrics.get("pair_pnl"),
            "residual_qty": metrics.get("residual_qty"),
            "residual_cost": metrics.get("residual_cost"),
            "material_residual_lots": metrics.get("material_residual_lots"),
        },
        "residual_summary": {
            "lot_count": len(lots),
            "class_counts": dict(class_counts.most_common()),
            "class_cost": {k: round(v, 6) for k, v in sorted(class_cost.items())},
            "class_qty": {k: round(v, 6) for k, v in sorted(class_qty.items())},
            "mature_material_cost": round(mature_material_cost, 6),
            "material_cost": round(material_cost, 6),
            "tiny_cost": round(tiny_cost, 6),
            "age_ms_at_run_end": summarize(lot_ages),
            "cost": summarize(lot_costs),
            "top_lots": sorted(enriched, key=lambda x: as_float(x.get("cost")), reverse=True)[:10],
        },
        "hard_blockers": hard_blockers,
        "interpretation": [
            "This scorer does not relax pair-cost or rescue economics.",
            "Young or tiny residuals should be reported separately from mature material residual risk.",
            "A pass here can support local diagnosis only; shadow review still needs independent sample, scale, source, and PnL gates.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--max-material-residual-cost", type=float, default=0.0)
    args = parser.parse_args()

    root = Path(args.output_root).expanduser().resolve()
    out = Path(args.scorecard_json).expanduser().resolve()
    result = scan(root, args.max_material_residual_cost)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
