#!/usr/bin/env python3
"""Local verifier for D+ risk-increasing pair-fill cap.

The verifier reuses the candidate-pipeline V1 state machine and reads only the
derived candidate_base plus baseline manifests. It does not read raw/replay
stores, events JSONL, sockets, SSH, shared-ingress, or live trading state.
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
    delta["gross_buy_qty_retention"] = (
        float(variant["gross_buy_qty"]) / float(control["gross_buy_qty"]) if control.get("gross_buy_qty") else None
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


def rank(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    seed_retention = float(delta["seed_action_retention"] or 0.0)
    pair_retention = float(delta["pair_qty_retention"] or 0.0)
    residual_qty_reduction = float(delta["residual_qty_rate_reduction"] or 0.0)
    residual_cost_reduction = float(delta["residual_cost_rate_reduction"] or 0.0)
    stress_delta = float(delta["stress100_worst_pnl"]["absolute_delta"])
    fee_delta = float(delta["fee_after_pnl"]["absolute_delta"])
    worst_day_delta = float(delta["worst_day_fee_after_pnl"]["absolute_delta"])
    economic_positive = (
        float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    )
    stress_material_drop = stress_delta < -0.05 * abs(float(control["stress100_worst_pnl"]))
    if not economic_positive:
        decision = "DISCARD"
        label = "DISCARD_RISK_PAIR_FILL_CAP_ECONOMIC_NEGATIVE"
    elif seed_retention < 0.75 or pair_retention < 0.85:
        decision = "UNKNOWN"
        label = "TRADEOFF_RISK_PAIR_FILL_CAP_CAPACITY_DROP"
    elif residual_qty_reduction <= 0.0 or residual_cost_reduction <= 0.0:
        decision = "DISCARD"
        label = "DISCARD_RISK_PAIR_FILL_CAP_RESIDUAL_NOT_IMPROVED"
    elif stress_material_drop:
        decision = "UNKNOWN"
        label = "TRADEOFF_RISK_PAIR_FILL_CAP_STRESS_DROP"
    elif fee_delta < -0.05 * abs(float(control["fee_after_pnl"])):
        decision = "UNKNOWN"
        label = "TRADEOFF_RISK_PAIR_FILL_CAP_FEE_DROP"
    else:
        decision = "KEEP"
        label = "KEEP_RISK_PAIR_FILL_CAP_LOCAL_RESEARCH_ONLY"
    return {
        "decision_label": decision,
        "label": label,
        "economic_positive": economic_positive,
        "seed_action_retention": seed_retention,
        "pair_qty_retention": pair_retention,
        "residual_qty_rate_reduction": residual_qty_reduction,
        "residual_cost_rate_reduction": residual_cost_reduction,
        "fee_after_pnl_delta": round(fee_delta, 6),
        "stress100_worst_pnl_delta": round(stress_delta, 6),
        "worst_day_fee_after_pnl_delta": round(worst_day_delta, 6),
        "stress_material_drop": stress_material_drop,
        "interpretation": (
            "Local candidate-pipeline verifier only. KEEP means the mechanism can proceed to source-truth/shadow "
            "question selection; it does not prove private truth, deployability, canary readiness, or promotion."
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
    unsafe = [str(path) for path in required if not sm.path_safe(path)]
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
            name="variant_late_repair90_risk_increasing_pair_fill_cap",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            risk_increasing_pair_fill_cap=True,
        ),
    ]
    run_result = sm.run_profiles(candidate_db_path, profiles, base_day_counts)
    control = run_result["profiles"]["control_late_repair90"]
    variant = run_result["profiles"]["variant_late_repair90_risk_increasing_pair_fill_cap"]
    delta = direct_delta(control, variant)
    ranking = rank(control, variant, delta)
    source_truth = {
        "dataset_type": candidate_manifest.get("dataset_type"),
        "labels": candidate_manifest.get("labels"),
        "days": candidate_manifest.get("days"),
        "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
        "candidate_pipeline_v1_allowed_days_only": True,
        "public_or_proxy_truth_only": True,
        "private_truth_ready": False,
    }
    manifest = {
        "artifact": "xuan_b27_dplus_risk_increasing_pair_fill_cap_verifier",
        "created_utc": sm.utc_label(),
        "lane": "causal_verifier",
        "decision_label": ranking["label"],
        "status": ranking["decision_label"],
        "hypothesis": (
            "The latest no-order source-link shadow concentrated residual tail in YES risk-increasing seeds. "
            "Instead of deleting a summary bucket or applying a price cap, cap each risk-increasing seed to the "
            "currently available opposite residual quantity so the seed can fill/close existing inventory without "
            "creating new same-side residual."
        ),
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "baseline_result_manifest": str(result_manifest_path),
            "baseline_compliance_manifest": str(compliance_manifest_path),
        },
        "scope": source_truth,
        "processed_public_sell_rows": run_result["processed_public_sell_rows"],
        "baseline_core_metrics": result_manifest.get("core_metrics"),
        "baseline_compliance_summary": {
            "status": compliance_manifest.get("status"),
            "decision_scope": compliance_manifest.get("decision_scope"),
            "promotion_gate_pass": compliance_manifest.get("promotion_gate_pass"),
        },
        "config": {
            "control": sm.asdict(profiles[0]),
            "variant": sm.asdict(profiles[1]),
            "candidate_source": "candidate_base table, public_sell rows with 0 <= offset_s < 300",
            "official_fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "official_fee_rate": profiles[0].official_fee_rate,
            "mechanism_is_default_off": True,
        },
        "control_late_repair90": control,
        "variant_late_repair90_risk_increasing_pair_fill_cap": variant,
        "delta_vs_control": delta,
        "research_ranking": ranking,
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_CANDIDATE_PIPELINE_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
            "requires_replay_store_v2_source_truth": ranking["decision_label"] == "KEEP",
            "requires_no_order_shadow_review": ranking["decision_label"] == "KEEP",
        },
        "side_effects": {
            "ssh": False,
            "events_jsonl_read": False,
            "raw_replay_scan": False,
            "shared_ingress_modified": False,
            "orders_cancels_redeems": False,
        },
        "next_executable_action": (
            "If KEEP, run bounded replay_store_v2 selected-action source-truth and then select an exact no-order "
            "shadow-review question; if UNKNOWN/DISCARD, freeze this variant and return to local source-link attribution."
        ),
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return 0 if ranking["decision_label"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
