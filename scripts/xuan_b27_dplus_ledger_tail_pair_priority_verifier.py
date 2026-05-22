#!/usr/bin/env python3
"""Local verifier for D+ ledger-tail-first pair priority.

The mechanism keeps seed admission unchanged and changes only internal FIFO
pair selection when both sides have residual lots. It prioritizes lots with
ledger/context tail-risk markers so pairing consumes likely residual-tail lots
first. This verifier reads only allowed candidate_base/state-machine inputs.
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
    delta["weighted_pair_cost_reduction"] = (
        1.0 - float(variant["weighted_pair_cost"]) / float(control["weighted_pair_cost"])
        if control.get("weighted_pair_cost")
        else None
    )
    return delta


def ranking(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    seed_retention = float(delta["seed_action_retention"] or 0.0)
    pair_retention = float(delta["pair_qty_retention"] or 0.0)
    residual_qty_reduction = float(delta["residual_qty_rate_reduction"] or 0.0)
    residual_cost_reduction = float(delta["residual_cost_rate_reduction"] or 0.0)
    fee_delta = float(delta["fee_after_pnl"]["absolute_delta"])
    stress_delta = float(delta["stress100_worst_pnl"]["absolute_delta"])
    worst_day_delta = float(delta["worst_day_fee_after_pnl"]["absolute_delta"])
    economic_positive = (
        float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    )
    sample_preserved = seed_retention >= 0.999999 and pair_retention >= 0.999999
    residual_cost_improved = residual_cost_reduction > 0.0
    residual_qty_not_worse = residual_qty_reduction >= -1e-9
    stress_material_drop = stress_delta < -0.01 * abs(float(control["stress100_worst_pnl"]))
    fee_material_drop = fee_delta < -0.01 * abs(float(control["fee_after_pnl"]))
    worst_day_material_drop = worst_day_delta < -0.01 * abs(float(control["worst_day_fee_after_pnl"]))

    if not economic_positive:
        decision = "DISCARD"
        label = "DISCARD_LEDGER_TAIL_PAIR_PRIORITY_ECONOMIC_NEGATIVE"
    elif not sample_preserved:
        decision = "DISCARD"
        label = "DISCARD_LEDGER_TAIL_PAIR_PRIORITY_SAMPLE_NOT_PRESERVED"
    elif not residual_cost_improved or not residual_qty_not_worse:
        decision = "DISCARD"
        label = "DISCARD_LEDGER_TAIL_PAIR_PRIORITY_RESIDUAL_NOT_IMPROVED"
    elif stress_material_drop or fee_material_drop or worst_day_material_drop:
        decision = "UNKNOWN"
        label = "TRADEOFF_LEDGER_TAIL_PAIR_PRIORITY_RISK_RESHUFFLE_COST"
    else:
        decision = "KEEP"
        label = "KEEP_LEDGER_TAIL_PAIR_PRIORITY_LOCAL_RESEARCH_ONLY"

    return {
        "decision_label": decision,
        "label": label,
        "economic_positive": economic_positive,
        "sample_preserved": sample_preserved,
        "seed_action_retention": seed_retention,
        "pair_qty_retention": pair_retention,
        "residual_qty_rate_reduction": residual_qty_reduction,
        "residual_cost_rate_reduction": residual_cost_reduction,
        "fee_after_pnl_delta": round(fee_delta, 6),
        "stress100_worst_pnl_delta": round(stress_delta, 6),
        "worst_day_fee_after_pnl_delta": round(worst_day_delta, 6),
        "stress_material_drop": stress_material_drop,
        "fee_material_drop": fee_material_drop,
        "worst_day_material_drop": worst_day_material_drop,
        "interpretation": (
            "Local candidate-pipeline verifier only. KEEP means the sample-preserving pair-priority mechanism can "
            "proceed to source-truth/shadow question selection. It does not prove private truth, deployability, "
            "canary readiness, or promotion."
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
    result_manifest_path = baseline_result_dir / "RESULT_SUMMARY_MANIFEST.json"
    compliance_manifest_path = baseline_result_dir / "COMPLIANCE_MANIFEST.json"
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
            name="control_late_repair90_fifo",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            pair_inventory_priority="fifo",
        ),
        sm.Profile(
            name="variant_late_repair90_ledger_tail_first_pair_priority",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            pair_inventory_priority="ledger_tail_first",
        ),
    ]
    run_result = sm.run_profiles(candidate_db_path, profiles, base_day_counts)
    control = run_result["profiles"]["control_late_repair90_fifo"]
    variant = run_result["profiles"]["variant_late_repair90_ledger_tail_first_pair_priority"]
    delta = direct_delta(control, variant)
    rank = ranking(control, variant, delta)
    manifest = {
        "artifact": "xuan_b27_dplus_ledger_tail_pair_priority_verifier",
        "created_utc": sm.utc_label(),
        "lane": "causal_verifier",
        "decision_label": rank["label"],
        "status": rank["decision_label"],
        "hypothesis": (
            "Residual-tail ledger context indicates some residual risk may be a lot-selection problem rather than a "
            "seed-admission problem. This verifier keeps all seed admissions unchanged and only pairs tail-risk "
            "ledger lots first when both sides have inventory."
        ),
        "mechanism": {
            "name": "ledger_tail_first_pair_priority_v1",
            "default_off_profile_field": "pair_inventory_priority='ledger_tail_first'",
            "admission_changes": False,
            "price_cap": False,
            "static_side_offset_deletion": False,
            "cooldown_or_admission_cap": False,
            "uses_future_outcome_to_trade": False,
            "pair_priority_features": [
                "source_risk_direction",
                "offset_s",
                "pre_seed_same_qty",
                "pre_seed_opp_qty",
                "ledger_proxy_after"
            ]
        },
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "baseline_compliance_manifest": str(compliance_manifest_path),
        },
        "scope": {
            "dataset_type": candidate_manifest.get("dataset_type"),
            "labels": candidate_manifest.get("labels"),
            "days": candidate_manifest.get("days"),
            "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
            "candidate_pipeline_v1_allowed_days_only": True,
            "public_or_proxy_truth_only": True,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_pass": False,
            "result_classification": "PASS_LOCAL_COMPLETION_RESEARCH_ONLY",
        },
        "control": control,
        "variant": variant,
        "delta": delta,
        "research_ranking": rank,
        "promotion_gate": {
            "passed": False,
            "reason": "local candidate-pipeline state-machine verifier only",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
            "requires_source_truth_replay": rank["decision_label"] == "KEEP",
            "requires_no_order_shadow": rank["decision_label"] == "KEEP",
        },
        "side_effects": {
            "candidate_base_duckdb_read_only": True,
            "full_completion_store_scanned": False,
            "raw_scanned": False,
            "replay_scanned": False,
            "collector_scanned": False,
            "ssh_started": False,
            "shared_ingress_connected": False,
            "network_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "compliance_summary": compliance_manifest.get("compliance_summary"),
        "next_action": (
            "If KEEP, create a bounded source-truth/no-order question for ledger_tail_first_pair_priority_v1; "
            "if DISCARD/UNKNOWN, do not run remote and return to local mechanism selection."
        ),
    }
    out = Path(args.output)
    sm.write_json(out, manifest)
    print(out)
    return 0 if rank["decision_label"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
