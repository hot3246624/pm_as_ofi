#!/usr/bin/env python3
"""Run shadow-aligned D+ fill-to-balance candidate-pipeline verifier.

This script reads only the derived candidate_base DuckDB plus local candidate
pipeline manifests. It does not read raw/replay/collector stores, the full
completion event store, sockets, SSH, or event JSONL.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_candidate_pipeline_shadow_aligned_fill_rerun"
DEFAULT_CANDIDATE_BASE_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "local_20260502_20260518_paircap102"
)
DEFAULT_BASELINE_RESULT_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
    "20260502_20260518_publicfull_v2"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
)


def load_rerun_module() -> Any:
    path = Path(__file__).resolve().with_name("xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py")
    spec = importlib.util.spec_from_file_location("xuan_dplus_state_machine_rerun", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load state-machine rerun module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RERUN = load_rerun_module()


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    resolved = str(path.resolve())
    return not any(fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def as_float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def round6(value: Any) -> float:
    return round(as_float(value), 6)


def ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return numerator / denominator


def direct_delta(control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "seed_actions",
        "active_markets",
        "pair_actions",
        "pair_qty",
        "weighted_pair_cost",
        "gross_pnl",
        "official_taker_fee",
        "fee_after_pnl",
        "stress100_worst_pnl",
        "worst_day_fee_after_pnl",
        "residual_qty",
        "residual_cost",
        "residual_qty_rate",
        "residual_cost_rate",
        "seed_qty_cap_late_fill_to_balance",
        "seed_block_late_fill_to_balance_qty",
        "seed_late_fill_to_balance_qty_reduction",
    ]
    out: dict[str, Any] = {}
    for field in fields:
        c = as_float(control.get(field))
        v = as_float(variant.get(field))
        out[field] = {
            "control": round6(c),
            "variant": round6(v),
            "absolute_delta": round6(v - c),
            "relative_delta": None if c == 0.0 else round((v - c) / c, 8),
        }
    out["seed_action_retention"] = ratio(as_float(variant.get("seed_actions")), as_float(control.get("seed_actions")))
    out["pair_qty_retention"] = ratio(as_float(variant.get("pair_qty")), as_float(control.get("pair_qty")))
    out["fee_after_pnl_retention"] = ratio(
        as_float(variant.get("fee_after_pnl")), as_float(control.get("fee_after_pnl"))
    )
    out["stress100_worst_pnl_retention"] = ratio(
        as_float(variant.get("stress100_worst_pnl")), as_float(control.get("stress100_worst_pnl"))
    )
    out["residual_qty_rate_reduction"] = (
        None
        if as_float(control.get("residual_qty_rate")) == 0.0
        else 1.0 - as_float(variant.get("residual_qty_rate")) / as_float(control.get("residual_qty_rate"))
    )
    out["residual_cost_rate_reduction"] = (
        None
        if as_float(control.get("residual_cost_rate")) == 0.0
        else 1.0 - as_float(variant.get("residual_cost_rate")) / as_float(control.get("residual_cost_rate"))
    )
    return out


def day_delta(control: dict[str, Any], variant: dict[str, Any]) -> list[dict[str, Any]]:
    control_by_day = {row["day"]: row for row in control.get("summary_by_day") or []}
    variant_by_day = {row["day"]: row for row in variant.get("summary_by_day") or []}
    rows: list[dict[str, Any]] = []
    for day in sorted(set(control_by_day) | set(variant_by_day)):
        c = control_by_day.get(day, {})
        v = variant_by_day.get(day, {})
        rows.append(
            {
                "day": day,
                "seed_actions_delta": round6(as_float(v.get("seed_actions")) - as_float(c.get("seed_actions"))),
                "pair_actions_delta": round6(as_float(v.get("pair_actions")) - as_float(c.get("pair_actions"))),
                "pair_qty_delta": round6(as_float(v.get("pair_qty")) - as_float(c.get("pair_qty"))),
                "fee_after_pnl_delta": round6(as_float(v.get("fee_after_pnl")) - as_float(c.get("fee_after_pnl"))),
                "stress100_worst_pnl_delta": round6(
                    as_float(v.get("stress100_worst_pnl")) - as_float(c.get("stress100_worst_pnl"))
                ),
                "residual_qty_delta": round6(as_float(v.get("residual_qty")) - as_float(c.get("residual_qty"))),
                "residual_cost_delta": round6(as_float(v.get("residual_cost")) - as_float(c.get("residual_cost"))),
                "control_residual_qty_rate": round6(c.get("qty_residual_rate")),
                "variant_residual_qty_rate": round6(v.get("qty_residual_rate")),
                "control_residual_cost_rate": round6(c.get("cost_residual_rate")),
                "variant_residual_cost_rate": round6(v.get("cost_residual_rate")),
            }
        )
    return rows


def classify(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    seed_retention = float(delta.get("seed_action_retention") or 0.0)
    pair_qty_retention = float(delta.get("pair_qty_retention") or 0.0)
    fee_retention = float(delta.get("fee_after_pnl_retention") or 0.0)
    stress_retention = float(delta.get("stress100_worst_pnl_retention") or 0.0)
    residual_qty_reduction = float(delta.get("residual_qty_rate_reduction") or 0.0)
    residual_cost_reduction = float(delta.get("residual_cost_rate_reduction") or 0.0)
    weighted_pair_cost_delta = as_float(delta["weighted_pair_cost"]["absolute_delta"])
    fee_positive = as_float(variant.get("fee_after_pnl")) > 0.0
    stress_positive = as_float(variant.get("stress100_worst_pnl")) > 0.0
    worst_day_positive = as_float(variant.get("worst_day_fee_after_pnl")) > 0.0
    economics_ok = fee_positive and stress_positive and worst_day_positive
    material_sample_ok = seed_retention >= 0.80 and pair_qty_retention >= 0.80
    risk_improves = residual_qty_reduction > 0.0 and residual_cost_reduction > 0.0
    material_economic_loss = fee_retention < 0.85 or stress_retention < 0.85
    if not economics_ok:
        decision_label = "DISCARD"
        label = "DISCARD_ECONOMIC_NEGATIVE_SHADOW_ALIGNED_FILL_TO_BALANCE"
    elif material_sample_ok and risk_improves and not material_economic_loss:
        decision_label = "KEEP"
        label = "KEEP_SHADOW_ALIGNED_FILL_TO_BALANCE_RESEARCH_ONLY"
    elif risk_improves and material_sample_ok:
        decision_label = "UNKNOWN"
        label = "TRADEOFF_RISK_IMPROVES_ECONOMIC_CAPACITY_LOSS"
    elif not risk_improves:
        decision_label = "DISCARD"
        label = "DISCARD_SHADOW_ALIGNED_FILL_TO_BALANCE_RISK_NOT_IMPROVED"
    else:
        decision_label = "UNKNOWN"
        label = "UNKNOWN_SHADOW_ALIGNED_FILL_TO_BALANCE_MIXED"
    risk_adjusted_score_delta_proxy = (
        as_float(delta["fee_after_pnl"]["absolute_delta"])
        - as_float(delta["residual_cost"]["absolute_delta"])
        + as_float(delta["stress100_worst_pnl"]["absolute_delta"])
    )
    return {
        "decision_label": decision_label,
        "label": label,
        "fee_after_pnl_positive": fee_positive,
        "stress100_worst_pnl_positive": stress_positive,
        "worst_day_fee_after_pnl_positive": worst_day_positive,
        "seed_action_retention": seed_retention,
        "pair_qty_retention": pair_qty_retention,
        "fee_after_pnl_retention": fee_retention,
        "stress100_worst_pnl_retention": stress_retention,
        "residual_qty_rate_reduction": residual_qty_reduction,
        "residual_cost_rate_reduction": residual_cost_reduction,
        "weighted_pair_cost_delta": round(weighted_pair_cost_delta, 8),
        "risk_adjusted_score_delta_proxy": round(risk_adjusted_score_delta_proxy, 6),
        "notes": [
            "This is local completion candidate-pipeline research only, not deployable evidence.",
            "The config is shadow-aligned to edge=0.07 and max_open_cost=80 to match the failed same-host shadow surface.",
            "Promotion still requires no-order shadow and source-of-truth replay validation.",
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument("--baseline-result-dir", type=Path, default=DEFAULT_BASELINE_RESULT_DIR)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"

    candidate_manifest_path = args.candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_db_path = args.candidate_base_dir / "candidate_base.duckdb"
    result_manifest_path = args.baseline_result_dir / "RESULT_SUMMARY_MANIFEST.json"
    compliance_manifest_path = args.baseline_result_dir / "COMPLIANCE_MANIFEST.json"
    required_paths = [candidate_manifest_path, candidate_db_path, result_manifest_path, compliance_manifest_path]
    missing = [str(path) for path in required_paths if not path.exists()]
    unsafe = [str(path) for path in required_paths if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": label,
            "decision_label": "BLOCKED",
            "status": "BLOCKED_CANDIDATE_PIPELINE_INPUT_UNAVAILABLE",
            "missing_paths": missing,
            "unsafe_paths": unsafe,
            "side_effects": {
                "ssh": False,
                "network": False,
                "shadow_started": False,
                "raw_replay_scanned": False,
                "events_jsonl_read": False,
                "full_completion_store_scanned": False,
            },
        }
        write_json(output_dir / "manifest.json", manifest)
        print(output_dir / "manifest.json")
        return 2

    candidate_manifest = read_json(candidate_manifest_path)
    result_manifest = read_json(result_manifest_path)
    compliance_manifest = read_json(compliance_manifest_path)
    base_day_counts = {str(day): int(count) for day, count in (candidate_manifest.get("day_counts") or {}).items()}
    profiles = [
        RERUN.Profile(
            name="shadow_aligned_late_repair90_control",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            edge=0.07,
            max_open_cost=80.0,
        ),
        RERUN.Profile(
            name="shadow_aligned_late_repair90_fill_to_balance_after_90",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            late_repair_fill_to_balance_after_s=90.0,
            edge=0.07,
            max_open_cost=80.0,
        ),
    ]
    run_result = RERUN.run_profiles(candidate_db_path, profiles, base_day_counts)
    control = run_result["profiles"][profiles[0].name]
    variant = run_result["profiles"][profiles[1].name]
    delta = direct_delta(control, variant)
    ranking = classify(control, variant, delta)
    days = day_delta(control, variant)
    worst_residual_delta_days = sorted(days, key=lambda row: (row["residual_qty_delta"], row["residual_cost_delta"]), reverse=True)[
        :5
    ]
    best_residual_delta_days = sorted(days, key=lambda row: (row["residual_qty_delta"], row["residual_cost_delta"]))[:5]
    status = ranking["label"]
    decision_label = ranking["decision_label"]
    next_action = (
        "Implement or run a bounded no-order shadow with shadow-aligned fill-to-balance only after reviewing "
        "the local verifier; still require no-order shadow acceptance and source-of-truth replay before promotion."
        if decision_label == "KEEP"
        else (
            "Do not run EC2 shadow; inspect a new non-price inventory mechanism or add event-lite source fields "
            "before spending another shadow window."
            if decision_label == "DISCARD"
            else "Review the tradeoff and add exact source fields or a narrower local verifier before another EC2 shadow."
        )
    )
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "causal_verifier",
        "decision_label": decision_label,
        "status": status,
        "hypothesis": (
            "The prior fill-to-balance candidate-pipeline KEEP used edge=0.055/max_open_cost=250, while the "
            "same-host shadow used edge=0.07/max_open_cost=80. This verifier reruns the same local state machine "
            "on candidate_base with the shadow-aligned config to test whether fill-to-balance survives that surface."
        ),
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "baseline_result_manifest": str(result_manifest_path),
            "baseline_compliance_manifest": str(compliance_manifest_path),
        },
        "scope": {
            "dataset_type": candidate_manifest.get("dataset_type"),
            "labels": candidate_manifest.get("labels"),
            "days": candidate_manifest.get("days"),
            "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
            "public_account_execution_truth_v1_included": True,
            "public_account_execution_truth_v1_private_truth": False,
            "deployable": False,
            "can_support_strategy_promotion": False,
            "promotion_gate_pass": False,
            "result_classification": "PASS_LOCAL_COMPLETION_RESEARCH_ONLY",
        },
        "config": {
            "control": asdict(profiles[0]),
            "fill_to_balance_variant": asdict(profiles[1]),
            "candidate_source": "candidate_base table, public_sell rows with 0 <= offset_s < 300",
            "shadow_alignment": {
                "edge": 0.07,
                "max_open_cost": 80.0,
                "target_qty": 5.0,
                "imbalance_qty_cap": 1.25,
                "late_repair_only_after_s": 90.0,
                "late_repair_fill_to_balance_after_s": 90.0,
            },
            "official_fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "official_fee_rate": profiles[0].official_fee_rate,
        },
        "processed_public_sell_rows": run_result["processed_public_sell_rows"],
        "shadow_aligned_late_repair90_control_rerun": control,
        "shadow_aligned_late_repair90_fill_to_balance_after_90_rerun": variant,
        "delta_vs_shadow_aligned_late_repair90": delta,
        "research_ranking": ranking,
        "promotion_gate": {
            "passed": False,
            "required_before_g2_canary": True,
            "reason": "local completion candidate-pipeline state-machine research only",
            "deployable": False,
            "can_support_strategy_promotion": False,
            "requires_no_order_shadow": decision_label == "KEEP",
            "requires_source_of_truth_replay": True,
        },
        "compliance_summary": {
            "strict_cache_pass": (compliance_manifest.get("strict_cache") or {}).get("coverage_pass"),
            "public_account_audit_coverage_pass": (compliance_manifest.get("public_account_audit") or {}).get(
                "coverage_pass"
            ),
            "public_account_audit_missing_days": (compliance_manifest.get("public_account_audit") or {}).get(
                "missing_days"
            ),
            "promotion_gate_pass": compliance_manifest.get("promotion_gate_pass"),
            "baseline_result_status": result_manifest.get("status"),
            "baseline_can_support_strategy_promotion": result_manifest.get("can_support_strategy_promotion"),
        },
        "scoreboard_delta": {
            "control_seed_actions": control["seed_actions"],
            "variant_seed_actions": variant["seed_actions"],
            "seed_action_retention": delta["seed_action_retention"],
            "control_pair_actions": control["pair_actions"],
            "variant_pair_actions": variant["pair_actions"],
            "control_pair_qty": control["pair_qty"],
            "variant_pair_qty": variant["pair_qty"],
            "pair_qty_retention": delta["pair_qty_retention"],
            "control_fee_after_pnl": control["fee_after_pnl"],
            "variant_fee_after_pnl": variant["fee_after_pnl"],
            "fee_after_pnl_retention": delta["fee_after_pnl_retention"],
            "control_stress100_worst_pnl": control["stress100_worst_pnl"],
            "variant_stress100_worst_pnl": variant["stress100_worst_pnl"],
            "stress100_worst_pnl_retention": delta["stress100_worst_pnl_retention"],
            "control_worst_day_fee_after_pnl": control["worst_day_fee_after_pnl"],
            "variant_worst_day_fee_after_pnl": variant["worst_day_fee_after_pnl"],
            "control_residual_qty_rate": control["residual_qty_rate"],
            "variant_residual_qty_rate": variant["residual_qty_rate"],
            "residual_qty_rate_reduction": delta["residual_qty_rate_reduction"],
            "control_residual_cost_rate": control["residual_cost_rate"],
            "variant_residual_cost_rate": variant["residual_cost_rate"],
            "residual_cost_rate_reduction": delta["residual_cost_rate_reduction"],
            "fill_to_balance_qty_cap_count": variant["seed_qty_cap_late_fill_to_balance"],
            "fill_to_balance_block_count": variant["seed_block_late_fill_to_balance_qty"],
            "fill_to_balance_qty_reduction": variant["seed_late_fill_to_balance_qty_reduction"],
        },
        "day_delta": {
            "worst_residual_delta_days": worst_residual_delta_days,
            "best_residual_delta_days": best_residual_delta_days,
            "days_with_residual_qty_rate_improvement": sum(
                1 for row in days if row["variant_residual_qty_rate"] < row["control_residual_qty_rate"]
            ),
            "days_with_stress_improvement": sum(1 for row in days if row["stress100_worst_pnl_delta"] > 0.0),
            "all_days": days,
        },
        "side_effects": {
            "ssh": False,
            "network": False,
            "shadow_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_touched": False,
            "raw_replay_scanned": False,
            "events_jsonl_read": False,
            "full_completion_store_scanned": False,
        },
        "next_action": next_action,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
