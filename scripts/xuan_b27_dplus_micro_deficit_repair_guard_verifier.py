#!/usr/bin/env python3
"""Local verifier for D+ micro-deficit repair guard.

The verifier reuses the candidate-pipeline V1 state machine and reads only
allowed candidate_base inputs plus an optional local separator export CSV for
offline target evidence. It does not read raw/replay stores, events JSONL,
sockets, SSH, shared-ingress, or live trading state.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
STATE_MACHINE_PATH = SCRIPT_DIR / "xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py"


def load_state_machine() -> Any:
    spec = importlib.util.spec_from_file_location("xuan_dplus_state_machine", STATE_MACHINE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {STATE_MACHINE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def rel_delta(actual: float, expected: float) -> float | None:
    if expected == 0:
        return None
    return (actual - expected) / expected


def first_existing(base: Path, names: list[str]) -> Path:
    for name in names:
        path = base / name
        if path.exists():
            return path
    return base / names[0]


def as_float(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def direct_delta(control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "seed_actions",
        "active_markets",
        "pair_actions",
        "pair_qty",
        "weighted_pair_cost",
        "gross_buy_qty",
        "gross_buy_cost",
        "pair_pnl",
        "official_taker_fee",
        "fee_after_pnl",
        "stress100_worst_pnl",
        "worst_day_fee_after_pnl",
        "residual_qty",
        "residual_cost",
        "residual_qty_rate",
        "residual_cost_rate",
        "seed_micro_deficit_repair_guard_candidate",
        "seed_block_micro_deficit_repair_guard",
    ]
    delta: dict[str, Any] = {}
    for field in fields:
        base = float(control.get(field) or 0.0)
        var = float(variant.get(field) or 0.0)
        delta[field] = {
            "control": base,
            "variant": var,
            "absolute_delta": round(var - base, 6),
            "relative_delta": rel_delta(var, base),
        }
    delta["seed_action_retention"] = (
        float(variant["seed_actions"]) / float(control["seed_actions"]) if control.get("seed_actions") else None
    )
    delta["gross_buy_qty_retention"] = (
        float(variant["gross_buy_qty"]) / float(control["gross_buy_qty"]) if control.get("gross_buy_qty") else None
    )
    delta["pair_qty_retention"] = (
        float(variant["pair_qty"]) / float(control["pair_qty"]) if control.get("pair_qty") else None
    )
    delta["residual_qty_rate_reduction"] = (
        1.0 - float(variant["residual_qty_rate"]) / float(control["residual_qty_rate"])
        if control.get("residual_qty_rate")
        else None
    )
    delta["residual_cost_rate_reduction"] = (
        1.0 - float(variant["residual_cost_rate"]) / float(control["residual_cost_rate"])
        if control.get("residual_cost_rate")
        else None
    )
    delta["weighted_pair_cost_worse_ratio"] = (
        float(variant["weighted_pair_cost"]) / float(control["weighted_pair_cost"])
        if control.get("weighted_pair_cost")
        else None
    )
    return delta


def ranking(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    seed_retention = float(delta["seed_action_retention"] or 0.0)
    gross_buy_retention = float(delta["gross_buy_qty_retention"] or 0.0)
    pair_retention = float(delta["pair_qty_retention"] or 0.0)
    residual_qty_reduction = float(delta["residual_qty_rate_reduction"] or 0.0)
    residual_cost_reduction = float(delta["residual_cost_rate_reduction"] or 0.0)
    weighted_pair_cost_worse_ratio = float(delta["weighted_pair_cost_worse_ratio"] or 0.0)
    fee_delta = float(delta["fee_after_pnl"]["absolute_delta"])
    stress_delta = float(delta["stress100_worst_pnl"]["absolute_delta"])
    worst_day_delta = float(delta["worst_day_fee_after_pnl"]["absolute_delta"])
    candidate_count = float(variant.get("seed_micro_deficit_repair_guard_candidate") or 0.0)
    block_count = float(variant.get("seed_block_micro_deficit_repair_guard") or 0.0)
    economic_positive = (
        float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    )
    sample_ok = seed_retention >= 0.90 and gross_buy_retention >= 0.90 and pair_retention >= 0.90
    residual_ok = residual_qty_reduction >= 0.20 and residual_cost_reduction >= 0.20
    pair_cost_ok = weighted_pair_cost_worse_ratio <= 1.01
    stress_material_drop = stress_delta < -0.05 * abs(float(control["stress100_worst_pnl"]))
    worst_day_material_drop = worst_day_delta < -0.05 * abs(float(control["worst_day_fee_after_pnl"]))
    fee_material_drop = fee_delta < -0.05 * abs(float(control["fee_after_pnl"]))

    if candidate_count <= 0.0 or block_count <= 0.0:
        decision = "UNKNOWN"
        label = "UNKNOWN_MICRO_DEFICIT_REPAIR_GUARD_NO_ACTIVE_BLOCKS"
    elif not economic_positive:
        decision = "DISCARD"
        label = "DISCARD_MICRO_DEFICIT_REPAIR_GUARD_ECONOMIC_NEGATIVE"
    elif not sample_ok:
        decision = "UNKNOWN"
        label = "TRADEOFF_MICRO_DEFICIT_REPAIR_GUARD_SAMPLE_OR_PAIR_DROP"
    elif not residual_ok:
        decision = "DISCARD"
        label = "DISCARD_MICRO_DEFICIT_REPAIR_GUARD_RESIDUAL_NOT_IMPROVED_ENOUGH"
    elif not pair_cost_ok:
        decision = "UNKNOWN"
        label = "TRADEOFF_MICRO_DEFICIT_REPAIR_GUARD_PAIR_COST_WORSE"
    elif stress_material_drop or worst_day_material_drop:
        decision = "UNKNOWN"
        label = "TRADEOFF_MICRO_DEFICIT_REPAIR_GUARD_RISK_OR_PNL_DROP"
    elif fee_material_drop:
        decision = "KEEP"
        label = "KEEP_MICRO_DEFICIT_REPAIR_GUARD_LOCAL_RESEARCH_ONLY_FEE_TRADEOFF"
    else:
        decision = "KEEP"
        label = "KEEP_MICRO_DEFICIT_REPAIR_GUARD_LOCAL_RESEARCH_ONLY"

    return {
        "decision": decision,
        "label": label,
        "economic_positive": economic_positive,
        "sample_ok": sample_ok,
        "residual_ok": residual_ok,
        "pair_cost_ok": pair_cost_ok,
        "seed_action_retention": seed_retention,
        "gross_buy_qty_retention": gross_buy_retention,
        "pair_qty_retention": pair_retention,
        "residual_qty_rate_reduction": residual_qty_reduction,
        "residual_cost_rate_reduction": residual_cost_reduction,
        "weighted_pair_cost_worse_ratio": weighted_pair_cost_worse_ratio,
        "fee_after_pnl_delta": round(fee_delta, 6),
        "stress100_worst_pnl_delta": round(stress_delta, 6),
        "worst_day_fee_after_pnl_delta": round(worst_day_delta, 6),
        "candidate_count": candidate_count,
        "block_count": block_count,
        "stress_material_drop": stress_material_drop,
        "fee_material_drop": fee_material_drop,
        "worst_day_material_drop": worst_day_material_drop,
        "interpretation": (
            "Local candidate-pipeline verifier only. KEEP means the default-off guard can proceed to bounded "
            "source-truth/shadow-review question selection. It does not prove private truth, deployability, "
            "canary readiness, or promotion."
        ),
    }


def separator_target_evidence(path: Path | None, max_deficit_qty: float, open_qty_cap: float) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False, "reason": "separator_export_csv_not_supplied"}
    rows = list(csv.DictReader(path.open()))
    if not rows:
        return {"available": False, "reason": "separator_export_csv_empty", "path": str(path)}

    def is_target(row: dict[str, str]) -> bool:
        same_qty = as_float(row, "pre_seed_same_qty")
        opp_qty = as_float(row, "pre_seed_opp_qty")
        return (
            row.get("source_risk_direction") == "repair_or_pairing_improving"
            and 0.0 < opp_qty - same_qty <= max_deficit_qty + 1e-12
            and same_qty + opp_qty <= open_qty_cap + 1e-12
        )

    def totals(selected: list[dict[str, str]]) -> dict[str, float]:
        return {
            "rows": float(len(selected)),
            "seed_qty": sum(as_float(row, "seed_qty") for row in selected),
            "pair_qty": sum(as_float(row, "source_pair_qty") for row in selected),
            "pair_pnl": sum(as_float(row, "source_pair_pnl") for row in selected),
            "residual_qty": sum(as_float(row, "source_residual_qty") for row in selected),
            "residual_cost": sum(as_float(row, "source_residual_cost") for row in selected),
        }

    target_rows = [row for row in rows if is_target(row)]
    all_totals = totals(rows)
    target_totals = totals(target_rows)
    shares = {
        "row_share": target_totals["rows"] / all_totals["rows"] if all_totals["rows"] else None,
        "seed_qty_share": target_totals["seed_qty"] / all_totals["seed_qty"] if all_totals["seed_qty"] else None,
        "pair_qty_share": target_totals["pair_qty"] / all_totals["pair_qty"] if all_totals["pair_qty"] else None,
        "residual_qty_share": (
            target_totals["residual_qty"] / all_totals["residual_qty"] if all_totals["residual_qty"] else None
        ),
        "residual_cost_share": (
            target_totals["residual_cost"] / all_totals["residual_cost"] if all_totals["residual_cost"] else None
        ),
    }
    return {
        "available": True,
        "path": str(path),
        "target_predicate": {
            "source_risk_direction": "repair_or_pairing_improving",
            "pre_seed_opp_minus_same_qty_gt": 0.0,
            "pre_seed_opp_minus_same_qty_lte": max_deficit_qty,
            "pre_seed_open_qty_lte": open_qty_cap,
        },
        "all_seed_totals": all_totals,
        "target_totals": target_totals,
        "target_shares": shares,
        "post_action_label_policy": (
            "The export source_pair/source_residual fields are used only for offline scoring here; the verifier "
            "rule below uses only pre-action inventory/risk fields."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir")
    parser.add_argument("--baseline-result-dir")
    parser.add_argument("--separator-export-csv")
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-deficit-qty", type=float, default=0.25)
    parser.add_argument("--open-qty-cap", type=float, default=1.0)
    args = parser.parse_args()
    sm = load_state_machine()

    candidate_base_dir = Path(args.candidate_base_dir) if args.candidate_base_dir else sm.DEFAULT_CANDIDATE_BASE_DIR
    baseline_result_dir = Path(args.baseline_result_dir) if args.baseline_result_dir else sm.DEFAULT_BASELINE_RESULT_DIR
    candidate_manifest_path = candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_db_path = candidate_base_dir / "candidate_base.duckdb"
    result_manifest_path = first_existing(baseline_result_dir, ["RESULT_SUMMARY_MANIFEST.json", "result_manifest.json"])
    compliance_manifest_path = first_existing(baseline_result_dir, ["COMPLIANCE_MANIFEST.json", "compliance_manifest.json"])
    required = [candidate_manifest_path, candidate_db_path, result_manifest_path, compliance_manifest_path]
    missing = [str(path) for path in required if not path.exists()]
    unsafe = [str(path) for path in required if path.exists() and not sm.path_safe(path)]
    if missing:
        raise SystemExit(f"missing required files: {missing}")
    if unsafe:
        raise SystemExit(f"unsafe input paths rejected: {unsafe}")

    candidate_manifest = sm.read_json(candidate_manifest_path)
    result_manifest = sm.read_json(result_manifest_path)
    compliance_manifest = sm.read_json(compliance_manifest_path)
    base_day_counts = {str(day): int(count) for day, count in (candidate_manifest.get("day_counts") or {}).items()}
    profiles = [
        sm.Profile(
            name="control_late_repair90",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
        ),
        sm.Profile(
            name="variant_micro_deficit_repair_guard",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            micro_deficit_repair_guard=True,
            micro_deficit_repair_max_deficit_qty=args.max_deficit_qty,
            micro_deficit_repair_open_qty_cap=args.open_qty_cap,
        ),
    ]
    result = sm.run_profiles(candidate_db_path, profiles, base_day_counts)
    control = result["profiles"]["control_late_repair90"]
    variant = result["profiles"]["variant_micro_deficit_repair_guard"]
    delta = direct_delta(control, variant)
    rank = ranking(control, variant, delta)
    decision = rank["decision"]
    label = rank["label"]
    separator_csv = Path(args.separator_export_csv) if args.separator_export_csv else None

    report = {
        "artifact": "xuan_b27_dplus_micro_deficit_repair_guard_verifier",
        "created_utc": sm.utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": "local_candidate_pipeline_causal_verifier",
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "baseline_result_manifest": str(result_manifest_path),
            "compliance_manifest": str(compliance_manifest_path),
            "separator_export_csv": str(separator_csv) if separator_csv is not None else None,
        },
        "dataset_scope": {
            "dataset_type": candidate_manifest.get("dataset_type"),
            "labels": candidate_manifest.get("labels"),
            "days": candidate_manifest.get("days"),
            "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
            "allowed_candidate_pipeline_v1_only": True,
        },
        "baseline_core_metrics": result_manifest.get("core_metrics"),
        "baseline_compliance_context": {
            "decision": compliance_manifest.get("decision"),
            "promotion_gate": compliance_manifest.get("promotion_gate"),
            "promotion_gate_pass": compliance_manifest.get("promotion_gate_pass"),
        },
        "offline_separator_target_evidence": separator_target_evidence(
            separator_csv,
            max_deficit_qty=args.max_deficit_qty,
            open_qty_cap=args.open_qty_cap,
        ),
        "mechanism": {
            "name": "micro_deficit_repair_guard_v1",
            "default_off": True,
            "predicate": {
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_opp_qty_minus_same_qty_min_exclusive": 0.0,
                "pre_seed_opp_qty_minus_same_qty_max_inclusive": args.max_deficit_qty,
                "pre_seed_open_qty_max_inclusive": args.open_qty_cap,
            },
            "action": "defer seed because current opposite-side deficit is below practical order-size repair range",
            "uses_only_pre_action_fields": True,
            "not_static_side_or_offset_deletion": True,
            "not_price_or_public_l1_cap": True,
            "not_summary_only_removal": True,
            "official_fee_formula": "shares * fee_rate * price * (1 - price)",
        },
        "control": control,
        "variant": variant,
        "delta_vs_control": delta,
        "research_ranking": rank,
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_CANDIDATE_PIPELINE_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
            "requires_replay_store_v2_source_truth": decision == "KEEP",
            "requires_no_order_shadow_review": decision == "KEEP",
        },
        "side_effects": {
            "ssh_started": False,
            "shadow_started": False,
            "canary_started": False,
            "events_jsonl_read": False,
            "raw_replay_scan": False,
            "shared_ingress_connected_or_modified": False,
            "broker_service_env_live_modified": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "If KEEP, select a bounded replay_store_v2 source-truth response for representative affected seeds before "
            "any no-order shadow question. If UNKNOWN/DISCARD, freeze this exact micro-deficit guard and return to "
            "offline separator discovery with named missing fields."
        ),
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if decision == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
