#!/usr/bin/env python3
"""Local verifier for a portfolio-negative risk-increasing source guard.

This verifier is intentionally local-only. It replays the allowed
candidate_base/state-machine path and tests whether an online condition-ledger
guard can block only risk-increasing seeds when that condition is already
portfolio-risk negative. It does not read raw/replay stores, events JSONL,
sockets, SSH targets, shared-ingress, or live trading state.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_portfolio_negative_risk_increasing_source_guard_verifier"
SCRIPT_DIR = Path(__file__).resolve().parent
STATE_MACHINE_PATH = SCRIPT_DIR / "xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py"
ENGINE_PATH = SCRIPT_DIR / "xuan_b27_dplus_dynamic_condition_ledger_risk_state_verifier.py"
DEFAULT_COMPLETION_MANIFEST = (
    Path("xuan_research_artifacts")
    / "xuan_b27_dplus_shadow_review_portfolio_source_link_exemplar_completion_20260522T163243Z"
    / "manifest.json"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
    "shared-ingress",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    resolved = str(path.resolve())
    return not any(fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def first_existing(base: Path, names: list[str]) -> Path:
    for name in names:
        path = base / name
        if path.exists():
            return path
    return base / names[0]


def rel_delta(new: float, old: float) -> float | None:
    return round((new - old) / old, 8) if old else None


def ratio(num: float, den: float) -> float | None:
    return round(num / den, 8) if den else None


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
        "worst_residual_net_pnl",
        "seed_block_condition_ledger_risk_state",
        "condition_ledger_risk_negative_candidates",
        "condition_ledger_risk_negative_repair_allowed",
    ]
    out: dict[str, Any] = {}
    for field in fields:
        base = float(control.get(field) or 0.0)
        var = float(variant.get(field) or 0.0)
        out[field] = {
            "control": round(base, 6),
            "variant": round(var, 6),
            "absolute_delta": round(var - base, 6),
            "relative_delta": rel_delta(var, base),
        }
    out["seed_action_retention"] = ratio(float(variant.get("seed_actions") or 0.0), float(control.get("seed_actions") or 0.0))
    out["pair_qty_retention"] = ratio(float(variant.get("pair_qty") or 0.0), float(control.get("pair_qty") or 0.0))
    out["gross_buy_qty_retention"] = ratio(
        float(variant.get("gross_buy_qty") or 0.0),
        float(control.get("gross_buy_qty") or 0.0),
    )
    out["residual_qty_rate_reduction"] = (
        round(1.0 - float(variant["residual_qty_rate"]) / float(control["residual_qty_rate"]), 8)
        if control.get("residual_qty_rate")
        else None
    )
    out["residual_cost_rate_reduction"] = (
        round(1.0 - float(variant["residual_cost_rate"]) / float(control["residual_cost_rate"]), 8)
        if control.get("residual_cost_rate")
        else None
    )
    out["weighted_pair_cost_worse_ratio"] = (
        round(float(variant["weighted_pair_cost"]) / float(control["weighted_pair_cost"]), 8)
        if control.get("weighted_pair_cost")
        else None
    )
    return out


def classify(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> tuple[str, str, list[str]]:
    blockers: list[str] = []
    seed_retention = float(delta.get("seed_action_retention") or 0.0)
    pair_retention = float(delta.get("pair_qty_retention") or 0.0)
    residual_qty_reduction = float(delta.get("residual_qty_rate_reduction") or 0.0)
    residual_cost_reduction = float(delta.get("residual_cost_rate_reduction") or 0.0)
    weighted_pair_cost_worse_ratio = float(delta.get("weighted_pair_cost_worse_ratio") or 999.0)
    block_count = int(variant.get("seed_block_condition_ledger_risk_state") or 0)
    repair_allowed = int(variant.get("condition_ledger_risk_negative_repair_allowed") or 0)
    fee_positive = float(variant.get("fee_after_pnl") or 0.0) > 0.0
    stress_positive = float(variant.get("stress100_worst_pnl") or 0.0) > 0.0
    worst_day_positive = float(variant.get("worst_day_fee_after_pnl") or 0.0) > 0.0
    stress_delta = float(delta["stress100_worst_pnl"]["absolute_delta"])
    worst_day_delta = float(delta["worst_day_fee_after_pnl"]["absolute_delta"])

    if block_count <= 0:
        blockers.append("no_risk_increasing_negative_ledger_seed_blocked")
    if repair_allowed <= 0:
        blockers.append("no_negative_ledger_repair_seed_observed_as_preserved")
    if not fee_positive:
        blockers.append("fee_after_pnl_not_positive")
    if not stress_positive:
        blockers.append("stress100_worst_pnl_not_positive")
    if not worst_day_positive:
        blockers.append("worst_day_fee_after_pnl_not_positive")
    if seed_retention < 0.90:
        blockers.append("seed_action_retention_below_0.90")
    if pair_retention < 0.90:
        blockers.append("pair_qty_retention_below_0.90")
    if residual_qty_reduction < 0.20:
        blockers.append("residual_qty_rate_reduction_below_0.20")
    if residual_cost_reduction < 0.20:
        blockers.append("residual_cost_rate_reduction_below_0.20")
    if weighted_pair_cost_worse_ratio > 1.01:
        blockers.append("weighted_pair_cost_worse_ratio_above_1.01")
    if stress_delta < 0.0:
        blockers.append("stress100_worst_pnl_worse")
    if worst_day_delta < 0.0:
        blockers.append("worst_day_fee_after_pnl_worse")

    if not (fee_positive and stress_positive and worst_day_positive):
        return "DISCARD", "DISCARD_PORTFOLIO_NEGATIVE_RISK_GUARD_ECONOMIC_NEGATIVE", blockers
    if seed_retention < 0.90 or pair_retention < 0.90:
        return "DISCARD", "DISCARD_PORTFOLIO_NEGATIVE_RISK_GUARD_SAMPLE_OR_PAIR_COLLAPSE", blockers
    if stress_delta < 0.0 or worst_day_delta < 0.0:
        return "DISCARD", "DISCARD_PORTFOLIO_NEGATIVE_RISK_GUARD_STRESS_OR_WORST_DAY_WORSE", blockers
    if residual_qty_reduction <= 0.0 or residual_cost_reduction <= 0.0:
        return "DISCARD", "DISCARD_PORTFOLIO_NEGATIVE_RISK_GUARD_RESIDUAL_NOT_IMPROVED", blockers
    if (
        block_count > 0
        and residual_qty_reduction >= 0.20
        and residual_cost_reduction >= 0.20
        and weighted_pair_cost_worse_ratio <= 1.01
    ):
        return "KEEP", "KEEP_PORTFOLIO_NEGATIVE_RISK_INCREASING_SOURCE_GUARD_LOCAL_ONLY", blockers
    return "UNKNOWN", "UNKNOWN_PORTFOLIO_NEGATIVE_RISK_GUARD_RESIDUAL_IMPROVES_BUT_WEAK", blockers


def load_day_counts(candidate_db: Path) -> dict[str, int]:
    con = duckdb.connect(str(candidate_db), read_only=True)
    try:
        return {
            str(row[0]): int(row[1])
            for row in con.execute("select day, count(*) from candidate_base group by 1 order by 1").fetchall()
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path)
    parser.add_argument("--baseline-result-dir", type=Path)
    parser.add_argument("--completion-manifest", type=Path, default=DEFAULT_COMPLETION_MANIFEST)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    sm = load_module(STATE_MACHINE_PATH, "xuan_b27_dplus_state_machine")
    engine = load_module(ENGINE_PATH, "xuan_b27_dplus_dynamic_ledger_engine")
    candidate_base_dir = args.candidate_base_dir or sm.DEFAULT_CANDIDATE_BASE_DIR
    baseline_result_dir = args.baseline_result_dir or sm.DEFAULT_BASELINE_RESULT_DIR
    output_dir = args.output_dir or (
        Path(__file__).resolve().parents[1]
        / "xuan_research_artifacts"
        / f"{ARTIFACT}_{utc_label()}"
    )
    candidate_manifest_path = candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_db_path = candidate_base_dir / "candidate_base.duckdb"
    result_manifest_path = first_existing(baseline_result_dir, ["RESULT_SUMMARY_MANIFEST.json", "result_manifest.json"])
    compliance_manifest_path = first_existing(baseline_result_dir, ["COMPLIANCE_MANIFEST.json", "compliance_manifest.json"])
    required = [
        candidate_manifest_path,
        candidate_db_path,
        result_manifest_path,
        compliance_manifest_path,
        args.completion_manifest,
    ]
    missing = [str(path) for path in required if not path.exists()]
    unsafe = [str(path) for path in required if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": utc_label(),
            "lane": "causal_verifier",
            "decision": "BLOCKED",
            "decision_label": "BLOCKED_PORTFOLIO_NEGATIVE_RISK_GUARD_INPUT_UNAVAILABLE",
            "missing_paths": missing,
            "unsafe_paths": unsafe,
            "next_executable_action": "Restore required local candidate-pipeline/completion artifacts and rerun locally.",
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps({"decision_label": manifest["decision_label"], "manifest": str(output_dir / "manifest.json")}))
        return 2

    candidate_manifest = read_json(candidate_manifest_path)
    result_manifest = read_json(result_manifest_path)
    compliance_manifest = read_json(compliance_manifest_path)
    completion_manifest = read_json(args.completion_manifest)
    base_day_counts = load_day_counts(candidate_db_path)

    control_replay = engine.run_profile(candidate_db_path, base_day_counts, gate_enabled=False)
    variant_replay = engine.run_profile(candidate_db_path, base_day_counts, gate_enabled=True)
    control = control_replay["profile"]
    variant = variant_replay["profile"]
    delta = direct_delta(control, variant)
    decision, decision_label, blockers = classify(control, variant, delta)

    intersection = completion_manifest.get("scorers", {}).get("portfolio_ledger_source_link_intersection", {})
    top_groups = intersection.get("intersection_metrics", {}).get("top_intersection_groups", [])
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "causal_verifier",
        "decision": decision,
        "decision_label": decision_label,
        "scope": {
            "local_only": True,
            "candidate_base_duckdb_read_only": True,
            "candidate_pipeline_v1_allowed_days_only": True,
            "public_or_proxy_truth_only": True,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_pass": False,
        },
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "baseline_result_manifest": str(result_manifest_path),
            "baseline_compliance_manifest": str(compliance_manifest_path),
            "completion_manifest": str(args.completion_manifest),
        },
        "source_evidence": {
            "completion_status": completion_manifest.get("status"),
            "shadow_acceptance_status": completion_manifest.get("shadow_trading_acceptance", {}).get("status"),
            "intersection_status": intersection.get("status"),
            "top_intersection_groups": top_groups[:5] if isinstance(top_groups, list) else [],
            "public_profile_next_lane_status": completion_manifest.get("scorers", {})
            .get("public_profile_next_lane_review", {})
            .get("status"),
        },
        "mechanism": {
            "name": "portfolio_negative_risk_increasing_source_guard_verifier_v1",
            "default_off_local_verifier": True,
            "control": "shadow-aligned late_repair90, edge=0.07, target_qty=5, max_open_cost=80, imbalance_qty_cap=1.25",
            "variant": "same control plus online condition-ledger negative-risk guard for risk-increasing seeds only",
            "pre_action_condition_ledger_proxy": (
                "pair_pnl - official_taker_fee - current_unpaired_cost - "
                "0.01 * (2*pair_qty + current_unpaired_qty)"
            ),
            "source_risk_direction_rule": "risk_increasing when same-side unpaired qty >= opposite-side unpaired qty",
            "blocked_seed_rule": "proxy < 0 and source_risk_direction == risk_increasing",
            "preserved_seed_rule": "repair_or_pairing_improving seeds remain allowed even when proxy < 0",
            "fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "official_fee_rate": 0.07,
            "not_price_cap": True,
            "not_summary_only_removal": True,
            "not_static_side_offset_deletion": True,
            "not_cooldown_or_admission_cap": True,
            "not_fill_to_balance_revival": True,
            "not_portfolio_ledger_trading_rule": True,
        },
        "coverage": {
            "candidate_base_labels": candidate_manifest.get("labels", []),
            "candidate_base_days": candidate_manifest.get("days", []),
            "candidate_base_excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days", []),
            "candidate_base_row_count": candidate_manifest.get("row_count"),
            "processed_public_sell_rows": {
                "control": control_replay["processed_public_sell_rows"],
                "variant": variant_replay["processed_public_sell_rows"],
            },
        },
        "control_late_repair90": control,
        "variant_portfolio_negative_risk_guard": variant,
        "delta_vs_control": delta,
        "control_condition_ledger_summary": engine.ledgers_summary(control_replay["ledgers"]),
        "variant_condition_ledger_summary": engine.ledgers_summary(variant_replay["ledgers"]),
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "blockers": blockers,
            "seed_action_retention": delta["seed_action_retention"],
            "pair_qty_retention": delta["pair_qty_retention"],
            "gross_buy_qty_retention": delta["gross_buy_qty_retention"],
            "residual_qty_rate_reduction": delta["residual_qty_rate_reduction"],
            "residual_cost_rate_reduction": delta["residual_cost_rate_reduction"],
            "weighted_pair_cost_worse_ratio": delta["weighted_pair_cost_worse_ratio"],
            "fee_after_pnl_positive": float(variant.get("fee_after_pnl") or 0.0) > 0.0,
            "stress100_worst_pnl_positive": float(variant.get("stress100_worst_pnl") or 0.0) > 0.0,
            "worst_day_fee_after_pnl_positive": float(variant.get("worst_day_fee_after_pnl") or 0.0) > 0.0,
            "risk_guard_block_count": variant.get("seed_block_condition_ledger_risk_state"),
            "negative_ledger_repair_allowed_count": variant.get("condition_ledger_risk_negative_repair_allowed"),
            "interpretation": (
                "Local candidate-pipeline causal verifier only. KEEP would nominate source-truth/shadow-review "
                "preparation; UNKNOWN/DISCARD must not be promoted or sent remote directly."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_CANDIDATE_PIPELINE_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "requires_source_truth_replay": decision == "KEEP",
            "requires_no_order_shadow_review": decision == "KEEP",
            "baseline_promotion_gate": compliance_manifest.get("promotion_gate") or compliance_manifest.get("promotion_gate_pass"),
            "baseline_result_core_metrics": result_manifest.get("core_metrics", {}),
        },
        "side_effects": {
            "candidate_base_duckdb_read_only": True,
            "events_jsonl_read": False,
            "full_completion_store_scanned": False,
            "raw_replay_collector_scanned": False,
            "ssh_started": False,
            "shared_ingress_connected": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "If KEEP, prepare bounded selected source-truth/no-order-review question selection without starting remote. "
            "If UNKNOWN/DISCARD, freeze this exact guard and select a different local-only mechanism."
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    write_json(output_dir / "score.json", manifest)
    print(json.dumps({"decision": decision, "decision_label": decision_label, "manifest": str(output_dir / "manifest.json")}))
    return 0 if decision == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
