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


def load_optional_json(path_arg: str | None) -> dict[str, Any]:
    if not path_arg:
        return {}
    return load_json(Path(path_arg).expanduser().resolve())


def status_is_keep(card: dict[str, Any]) -> bool:
    return str(card.get("status", "")).startswith("KEEP")


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


def scan(
    root: Path,
    max_material_residual_cost: float,
    runtime_summary: dict[str, Any],
    bridge_shadow_gap: dict[str, Any],
    surplus_bridge: dict[str, Any],
    max_residual_qty_share: float,
    max_residual_cost_share: float,
) -> dict[str, Any]:
    manifest = load_json(root / "manifest.json")
    aggregate = load_json(root / "aggregate_report.json")
    metrics = aggregate.get("metrics") or {}
    runtime_metrics = runtime_summary.get("metrics", {}) if runtime_summary else {}
    runtime_context_available = bool(runtime_metrics)
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

    accepted_actions = as_int(runtime_metrics.get("accepted_actions"), as_int(metrics.get("candidates")))
    fills = as_int(runtime_metrics.get("queue_supported_fills"), as_int(metrics.get("queue_supported_fills")))
    rescues = as_int(runtime_metrics.get("strict_rescue_closes"), as_int(metrics.get("strict_rescue_actions")))
    pair_pnl = as_float(runtime_metrics.get("pair_pnl"), as_float(metrics.get("pair_pnl")))
    residual_qty_share = as_float(runtime_metrics.get("residual_qty_share"), 0.0)
    residual_cost_share = as_float(runtime_metrics.get("residual_cost_share"), 0.0)
    per_window_residual_gate_clear = (
        runtime_context_available
        and status_is_keep(runtime_summary)
        and accepted_actions >= 25
        and fills >= 20
        and rescues >= 7
        and pair_pnl >= 0.0
        and residual_qty_share <= max_residual_qty_share
        and residual_cost_share <= max_residual_cost_share
    )
    bridge_residual_gate_clear = (
        status_is_keep(bridge_shadow_gap)
        and status_is_keep(surplus_bridge)
        and as_float(surplus_bridge.get("aggregate", {}).get("residual_qty_share"), 1.0)
        <= max_residual_qty_share
        and as_float(surplus_bridge.get("aggregate", {}).get("residual_cost_share"), 1.0)
        <= max_residual_cost_share
        and not (bridge_shadow_gap.get("decision", {}).get("hard_blockers") or [])
    )
    residual_caveat_reclassified = bool(
        hard_blockers
        and set(hard_blockers).issubset(
            {"mature_material_residual_cost_above_max", "young_material_residual_present"}
        )
        and (bridge_residual_gate_clear or per_window_residual_gate_clear)
    )
    effective_hard_blockers = [] if residual_caveat_reclassified else list(hard_blockers)

    if residual_caveat_reclassified and bridge_residual_gate_clear:
        status = "KEEP_YOUNG_TINY_RESIDUAL_SCORER_BRIDGE_RESIDUAL_CAVEAT_LOCAL_ONLY"
    elif residual_caveat_reclassified:
        status = "KEEP_YOUNG_TINY_RESIDUAL_SCORER_PER_WINDOW_RESIDUAL_CAVEAT_LOCAL_ONLY"
    elif hard_blockers:
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
            "hard_blockers": effective_hard_blockers,
            "residual_caveats": hard_blockers,
            "residual_caveat_reclassified_for_review": residual_caveat_reclassified,
            "per_window_residual_gate_clear": per_window_residual_gate_clear,
            "bridge_residual_gate_clear": bridge_residual_gate_clear,
        },
        "thresholds": {
            "salvage_age_ms": salvage_age_ms,
            "salvage_min_lot_cost": salvage_min_lot_cost,
            "max_material_residual_cost": max_material_residual_cost,
            "max_residual_qty_share": max_residual_qty_share,
            "max_residual_cost_share": max_residual_cost_share,
        },
        "runtime_metrics": {
            "accepted_actions": accepted_actions,
            "queue_supported_fills": fills,
            "strict_rescue_closes": rescues,
            "pair_pnl": pair_pnl,
            "residual_qty": metrics.get("residual_qty"),
            "residual_cost": metrics.get("residual_cost"),
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "material_residual_lots": metrics.get("material_residual_lots"),
        },
        "bridge_interpretation": {
            "runtime_summary_status": runtime_summary.get("status") if runtime_summary else None,
            "bridge_shadow_gap_status": bridge_shadow_gap.get("status") if bridge_shadow_gap else None,
            "surplus_bridge_status": surplus_bridge.get("status") if surplus_bridge else None,
            "per_window_residual_gate_clear": per_window_residual_gate_clear,
            "bridge_residual_gate_clear": bridge_residual_gate_clear,
            "residual_caveat_reclassified_for_review": residual_caveat_reclassified,
            "interpretation_scope": "local review caveat only; does not change residual lots or approve deploy",
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
        "hard_blockers": effective_hard_blockers,
        "residual_caveats": hard_blockers,
        "interpretation": [
            "This scorer does not relax pair-cost or rescue economics.",
            "Young or tiny residuals should be reported separately from mature material residual risk.",
            "When per-window or bridge residual-share gates clear, material residual lots are a review caveat rather than a stale packet blocker.",
            "A pass here can support local diagnosis only; shadow review still needs independent sample, scale, source, and PnL gates.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--max-material-residual-cost", type=float, default=0.0)
    parser.add_argument("--runtime-summary-scorecard", default=None)
    parser.add_argument("--bridge-shadow-gap-scorecard", default=None)
    parser.add_argument("--surplus-bridge-scorecard", default=None)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    args = parser.parse_args()

    root = Path(args.output_root).expanduser().resolve()
    out = Path(args.scorecard_json).expanduser().resolve()
    result = scan(
        root,
        args.max_material_residual_cost,
        load_optional_json(args.runtime_summary_scorecard),
        load_optional_json(args.bridge_shadow_gap_scorecard),
        load_optional_json(args.surplus_bridge_scorecard),
        args.max_residual_qty_share,
        args.max_residual_cost_share,
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
