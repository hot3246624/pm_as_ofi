#!/usr/bin/env python3
"""Local verifier for D+ complete-set dust-opposite late repair sizing.

The mechanism is default-off in the candidate-pipeline state machine. It uses
only pre-seed inventory context: when a late repair/pairing-improving seed sees
0 < opposite quantity < dust_qty, it caps the new seed to opposite_qty plus a
small explicit buffer. This verifier reads only allowed candidate_base inputs.
"""

from __future__ import annotations

import argparse
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
        "seed_complete_set_dust_opp_late_repair_sizer_candidate",
        "seed_qty_cap_complete_set_dust_opp_late_repair_sizer",
        "seed_complete_set_dust_opp_late_repair_qty_reduction",
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
    candidate_count = float(variant.get("seed_complete_set_dust_opp_late_repair_sizer_candidate") or 0.0)
    cap_count = float(variant.get("seed_qty_cap_complete_set_dust_opp_late_repair_sizer") or 0.0)
    economic_positive = (
        float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    )
    sample_ok = seed_retention >= 0.995 and gross_buy_retention >= 0.97 and pair_retention >= 0.98
    residual_ok = residual_qty_reduction >= 0.20 and residual_cost_reduction >= 0.20
    pair_cost_ok = weighted_pair_cost_worse_ratio <= 1.01
    stress_material_drop = stress_delta < -0.05 * abs(float(control["stress100_worst_pnl"]))
    fee_material_drop = fee_delta < -0.05 * abs(float(control["fee_after_pnl"]))
    worst_day_material_drop = worst_day_delta < -0.05 * abs(float(control["worst_day_fee_after_pnl"]))

    if candidate_count <= 0.0 or cap_count <= 0.0:
        decision = "UNKNOWN"
        label = "UNKNOWN_COMPLETE_SET_DUST_OPP_SIZER_NO_ACTIVE_CAPS"
    elif not economic_positive:
        decision = "DISCARD"
        label = "DISCARD_COMPLETE_SET_DUST_OPP_SIZER_ECONOMIC_NEGATIVE"
    elif not sample_ok:
        decision = "UNKNOWN"
        label = "TRADEOFF_COMPLETE_SET_DUST_OPP_SIZER_SAMPLE_OR_PAIR_DROP"
    elif not residual_ok:
        decision = "DISCARD"
        label = "DISCARD_COMPLETE_SET_DUST_OPP_SIZER_RESIDUAL_NOT_IMPROVED_ENOUGH"
    elif not pair_cost_ok:
        decision = "UNKNOWN"
        label = "TRADEOFF_COMPLETE_SET_DUST_OPP_SIZER_PAIR_COST_WORSE"
    elif stress_material_drop or fee_material_drop or worst_day_material_drop:
        decision = "UNKNOWN"
        label = "TRADEOFF_COMPLETE_SET_DUST_OPP_SIZER_RISK_OR_PNL_DROP"
    else:
        decision = "KEEP"
        label = "KEEP_COMPLETE_SET_DUST_OPP_LATE_REPAIR_SIZER_LOCAL_RESEARCH_ONLY"

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
        "cap_count": cap_count,
        "stress_material_drop": stress_material_drop,
        "fee_material_drop": fee_material_drop,
        "worst_day_material_drop": worst_day_material_drop,
        "interpretation": (
            "Local candidate-pipeline verifier only. KEEP means the default-off sizer can proceed to source-truth "
            "or no-order question selection. It does not prove private truth, deployability, canary readiness, or promotion."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir")
    parser.add_argument("--baseline-result-dir")
    parser.add_argument("--output", required=True)
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
    compliance_manifest = sm.read_json(compliance_manifest_path)
    base_day_counts = {str(day): int(count) for day, count in (candidate_manifest.get("day_counts") or {}).items()}
    profiles = [
        sm.Profile(
            name="control_late_repair90",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
        ),
        sm.Profile(
            name="variant_complete_set_dust_opp_late_repair_sizer",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            complete_set_dust_opp_late_repair_sizer=True,
            complete_set_dust_opp_late_repair_after_s=90.0,
            complete_set_dust_opp_late_repair_qty_buffer=1.0,
        ),
    ]
    result = sm.run_profiles(candidate_db_path, profiles, base_day_counts)
    control = result["profiles"]["control_late_repair90"]
    variant = result["profiles"]["variant_complete_set_dust_opp_late_repair_sizer"]
    delta = direct_delta(control, variant)
    rank = ranking(control, variant, delta)
    decision = rank["decision"]
    label = rank["label"]

    report = {
        "artifact": "xuan_b27_dplus_complete_set_dust_opp_late_repair_sizer_verifier",
        "created_utc": sm.utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": "local_candidate_pipeline_causal_verifier",
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "baseline_result_manifest": str(result_manifest_path),
            "compliance_manifest": str(compliance_manifest_path),
        },
        "dataset_scope": {
            "dataset_type": candidate_manifest.get("dataset_type"),
            "labels": candidate_manifest.get("labels"),
            "days": candidate_manifest.get("days"),
            "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
            "allowed_candidate_pipeline_v1_only": True,
        },
        "compliance_context": {
            "baseline_decision": compliance_manifest.get("decision"),
            "promotion_gate": compliance_manifest.get("promotion_gate"),
        },
        "mechanism": {
            "name": "complete_set_dust_opp_late_repair_sizer_v1",
            "default_off": True,
            "predicate": {
                "late_repair_only_after_s": 90.0,
                "source_risk_direction": "repair_or_pairing_improving",
                "pre_seed_opp_qty_min_exclusive": 0.0,
                "pre_seed_opp_qty_max_exclusive": "profile.dust_qty",
            },
            "quantity_formula": "qty=min(base_qty, pre_seed_opp_qty + complete_set_dust_opp_late_repair_qty_buffer)",
            "quantity_buffer": 1.0,
            "fee_rate_source": "state_machine_profile:official_fee_rate",
            "official_fee_formula": "shares * fee_rate * price * (1 - price)",
            "not_a_price_cap": True,
            "not_summary_only_removal": True,
        },
        "control": control,
        "variant": variant,
        "delta": delta,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "metrics": rank,
            "interpretation": rank["interpretation"],
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "candidate_base_duckdb_read_only": True,
            "ssh_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "If KEEP, run source-truth selected-action validation or select a bounded no-order shadow question; "
            "if UNKNOWN/DISCARD, return to complete-set field attribution without reviving frozen mechanisms."
        ),
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    sm.write_json(output, report)
    print(output)
    return 0 if decision == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
