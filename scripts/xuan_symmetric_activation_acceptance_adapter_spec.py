#!/usr/bin/env python3
"""Specify and score symmetric-activation shadow acceptance adapters.

This local-only adapter defines how a future allowlisted no-order pullback that
contains both normal shadow acceptance metrics and symmetric activation summary
scoring should be evaluated. It does not start a runner, fetch data, inspect
events JSONL/raw stores, or touch trading paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_symmetric_activation_acceptance_adapter_spec"
INPUT_SCHEMA = "symmetric_activation_acceptance_adapter_input_v1"
DEFAULT_SUMMARY_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_summary_scorer_20260524T171000Z/"
    "manifest.json"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        if value == "inf":
            return float("inf")
        try:
            return float(value)
        except ValueError:
            return default
    return default


def as_int(value: Any, default: int = 0) -> int:
    return int(as_float(value, default))


def path_is_forbidden(path: Path) -> bool:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    return any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    if path_is_forbidden(path):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def safe_load(path: Path | None, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    if path is None:
        return None, "missing"
    ok, reason = path_safe(path, root)
    if not ok:
        return None, reason
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def nested_get(obj: dict[str, Any], path: list[str], default: Any = None) -> Any:
    current: Any = obj
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return default if current is None else current


def thresholds(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "min_candidates": args.min_candidates,
        "max_net_pair_cost_p90": args.max_net_pair_cost_p90,
        "max_residual_qty_share": args.max_residual_qty_share,
        "max_residual_cost_share": args.max_residual_cost_share,
        "max_pair_tail_loss_share": args.max_pair_tail_loss_share,
        "max_material_residual_lots": args.max_material_residual_lots,
        "min_fee_adjusted_pair_pnl_proxy": args.min_fee_adjusted_pair_pnl_proxy,
        "min_blocked_activation_denominator": args.min_blocked_activation_denominator,
        "min_admitted_activation_denominator": args.min_admitted_activation_denominator,
        "require_source_sequence_coverage": True,
        "require_aggregate_parity": True,
    }


def adapter_contract(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "contract_name": "symmetric_activation_acceptance_adapter_spec_v1",
        "purpose": (
            "Join normal no-order shadow acceptance metrics with "
            "symmetric_activation_summary_v1 scoring for future allowlisted pullbacks."
        ),
        "default_off": True,
        "required_inputs": [
            "allowlisted output/aggregate_report.json and output/manifest.json pullback",
            "normal shadow acceptance manifest from scripts/xuan_b27_dplus_shadow_trading_acceptance.py",
            "symmetric activation summary scorer manifest from scripts/xuan_symmetric_activation_summary_scorer.py",
            "output/*.summary.json already included in allowlisted pullback",
        ],
        "required_shadow_acceptance_gates": {
            "no_order_safety_ok": True,
            "candidates_gte": args.min_candidates,
            "net_pair_cost_p90_lte": args.max_net_pair_cost_p90,
            "residual_qty_share_lte": args.max_residual_qty_share,
            "residual_cost_share_lte": args.max_residual_cost_share,
            "pair_tail_loss_share_lte": args.max_pair_tail_loss_share,
            "material_residual_lots_lte": args.max_material_residual_lots,
            "fee_adjusted_pair_pnl_proxy_gt": args.min_fee_adjusted_pair_pnl_proxy,
        },
        "required_symmetric_activation_gates": {
            "blocked_activation_denominator_gte": args.min_blocked_activation_denominator,
            "admitted_activation_denominator_gte": args.min_admitted_activation_denominator,
            "source_sequence_coverage_present": True,
            "aggregate_parity": True,
            "field_contract_ok": True,
            "schema_ok": True,
        },
        "field_contract": {
            "post_action_outcome_labels_included": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "trading_behavior_changed": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
        "fail_closed_conditions": [
            "candidate pullback or adapter input is absent",
            "input path is outside current worktree or contains forbidden raw/replay/events/shared-ingress fragments",
            "normal no-order safety is absent or false",
            "sample size below 100 candidates",
            "net_pair_cost_p90 exceeds 1.0",
            "residual quantity share exceeds 15%",
            "residual cost share exceeds 20%",
            "pair tail loss share exceeds 5%",
            "material residual lots are nonzero",
            "fee-adjusted pair PnL is not positive",
            "activation blocked/admitted denominators are zero",
            "source_sequence coverage or aggregate parity is missing",
            "synthetic fixture/scorer readiness is presented as strategy evidence",
            "private truth, deployable, canary, or promotion is claimed",
        ],
        "thresholds": thresholds(args),
    }


def summary_scorer_ready(manifest: dict[str, Any] | None) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    if not manifest:
        return False, ["summary_scorer_manifest_missing"]
    if manifest.get("decision_label") != "KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_READY":
        blockers.append("summary_scorer_not_keep_ready")
    score = manifest.get("score") or {}
    checks = score.get("checks") or {}
    required_true = [
        "default_off_summary_absent",
        "enabled_summary_present",
        "field_contract_ok",
        "aggregate_parity",
        "blocked_activation_denominator_positive",
        "admitted_activation_denominator_positive",
        "source_sequence_coverage_present",
    ]
    for key in required_true:
        if checks.get(key) is not True:
            blockers.append(f"summary_scorer_{key}_not_true")
    if nested_get(manifest, ["promotion_gate", "passed"]) is not False:
        blockers.append("summary_scorer_promotion_gate_not_false")
    if nested_get(manifest, ["research_ranking", "strategy_evidence"]) is not False:
        blockers.append("summary_scorer_claims_strategy_evidence")
    return not blockers, blockers


def extract_shadow(candidate: dict[str, Any]) -> dict[str, Any]:
    value = candidate.get("shadow_acceptance")
    return value if isinstance(value, dict) else candidate


def extract_activation(candidate: dict[str, Any]) -> dict[str, Any]:
    value = candidate.get("symmetric_activation_score")
    return value if isinstance(value, dict) else candidate


def metric_value(metrics: dict[str, Any], keys: list[str], default: float = 0.0) -> float:
    for key in keys:
        if key in metrics:
            return as_float(metrics.get(key), default)
    return default


def evaluate_shadow_acceptance(shadow: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    checks = shadow.get("checks") if isinstance(shadow.get("checks"), dict) else {}
    metrics = shadow.get("trading_metrics") if isinstance(shadow.get("trading_metrics"), dict) else {}
    blockers: list[str] = []

    acceptance_passed = shadow.get("acceptance_passed")
    no_order_safety_ok = checks.get("no_order_safety_ok")
    candidates = as_int(metrics.get("candidates"))
    net_pair_cost_p90 = metric_value(metrics, ["net_pair_cost_p90", "net_pair_cost_proxy_p90"])
    residual_qty_share = metric_value(
        metrics,
        ["residual_qty_share_of_filled", "residual_qty_share", "residual_qty_share_of_filled_qty"],
    )
    residual_cost_share = metric_value(
        metrics,
        ["residual_cost_share_of_filled_cost", "residual_cost_share"],
    )
    pair_tail_loss_share = metric_value(
        metrics,
        ["pair_tail_loss_share_of_pair_pnl", "pair_tail_loss_share"],
    )
    material_residual_lots = as_int(metrics.get("material_residual_lots"))
    fee_adjusted_pair_pnl_proxy = as_float(metrics.get("fee_adjusted_pair_pnl_proxy"))

    if acceptance_passed is False:
        blockers.append("normal_shadow_acceptance_passed_false")
    if no_order_safety_ok is not True:
        blockers.append("no_order_safety_not_true")
    if candidates < args.min_candidates:
        blockers.append("candidates_below_min")
    if net_pair_cost_p90 > args.max_net_pair_cost_p90:
        blockers.append("net_pair_cost_p90_above_max")
    if residual_qty_share > args.max_residual_qty_share:
        blockers.append("residual_qty_share_above_max")
    if residual_cost_share > args.max_residual_cost_share:
        blockers.append("residual_cost_share_above_max")
    if pair_tail_loss_share > args.max_pair_tail_loss_share:
        blockers.append("pair_tail_loss_share_above_max")
    if material_residual_lots > args.max_material_residual_lots:
        blockers.append("material_residual_lots_above_max")
    if fee_adjusted_pair_pnl_proxy <= args.min_fee_adjusted_pair_pnl_proxy:
        blockers.append("fee_adjusted_pair_pnl_proxy_not_positive")

    return {
        "passed": not blockers,
        "blockers": blockers,
        "metrics": {
            "candidates": candidates,
            "net_pair_cost_p90": net_pair_cost_p90,
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "pair_tail_loss_share": pair_tail_loss_share,
            "material_residual_lots": material_residual_lots,
            "fee_adjusted_pair_pnl_proxy": fee_adjusted_pair_pnl_proxy,
        },
        "checks": {
            "normal_acceptance_passed_not_false": acceptance_passed is not False,
            "no_order_safety_ok": no_order_safety_ok is True,
            "candidates_ok": candidates >= args.min_candidates,
            "net_pair_cost_p90_ok": net_pair_cost_p90 <= args.max_net_pair_cost_p90,
            "residual_qty_share_ok": residual_qty_share <= args.max_residual_qty_share,
            "residual_cost_share_ok": residual_cost_share <= args.max_residual_cost_share,
            "pair_tail_loss_share_ok": pair_tail_loss_share <= args.max_pair_tail_loss_share,
            "material_residual_lots_ok": material_residual_lots <= args.max_material_residual_lots,
            "fee_adjusted_pair_pnl_proxy_positive": fee_adjusted_pair_pnl_proxy > args.min_fee_adjusted_pair_pnl_proxy,
        },
    }


def evaluate_activation_score(activation: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    score = activation.get("score") if isinstance(activation.get("score"), dict) else activation
    checks = score.get("checks") if isinstance(score.get("checks"), dict) else {}
    summary = score.get("summary") if isinstance(score.get("summary"), dict) else {}
    blockers: list[str] = []

    blocked = as_float(summary.get("blocked_activation_denominator"))
    admitted = as_float(summary.get("admitted_activation_denominator"))
    required_true = {
        "field_contract_ok": checks.get("field_contract_ok"),
        "aggregate_parity": checks.get("aggregate_parity"),
        "source_sequence_coverage_present": checks.get("source_sequence_coverage_present"),
    }
    schema_value = checks.get("schema_ok")
    if schema_value is not None:
        required_true["schema_ok"] = schema_value
    for key, value in required_true.items():
        if value is not True:
            blockers.append(f"activation_{key}_not_true")
    if blocked < args.min_blocked_activation_denominator:
        blockers.append("blocked_activation_denominator_below_min")
    if admitted < args.min_admitted_activation_denominator:
        blockers.append("admitted_activation_denominator_below_min")
    if nested_get(activation, ["promotion_gate", "passed"]) is True:
        blockers.append("activation_score_claims_promotion")
    if nested_get(activation, ["research_ranking", "strategy_evidence"]) is True:
        blockers.append("activation_score_claims_strategy_evidence")

    return {
        "passed": not blockers,
        "blockers": blockers,
        "summary": {
            "blocked_activation_denominator": blocked,
            "admitted_activation_denominator": admitted,
            "aggregate_total": as_float(summary.get("aggregate_total")),
            "summary_file_total": as_float(summary.get("summary_file_total")),
        },
        "checks": {
            **{key: value is True for key, value in required_true.items()},
            "blocked_activation_denominator_ok": blocked >= args.min_blocked_activation_denominator,
            "admitted_activation_denominator_ok": admitted >= args.min_admitted_activation_denominator,
        },
    }


def evaluate_candidate(candidate: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    blockers: list[str] = []
    schema = candidate.get("schema_version")
    if schema != INPUT_SCHEMA:
        blockers.append("candidate_schema_version_mismatch")
    if candidate.get("private_truth_ready") is True:
        blockers.append("candidate_claims_private_truth_ready")
    if candidate.get("deployable") is True:
        blockers.append("candidate_claims_deployable")
    if nested_get(candidate, ["promotion_gate", "passed"]) is True:
        blockers.append("candidate_claims_promotion_gate_passed")

    fixture = candidate.get("fixture") is True
    shadow_result = evaluate_shadow_acceptance(extract_shadow(candidate), args)
    activation_result = evaluate_activation_score(extract_activation(candidate), args)
    blockers.extend(f"shadow_{name}" for name in shadow_result["blockers"])
    blockers.extend(f"activation_{name}" for name in activation_result["blockers"])
    passed = not blockers
    if passed and fixture:
        candidate_status = "FIXTURE_ADAPTER_CONTRACT_PASS_NOT_REAL"
    elif passed:
        candidate_status = "REAL_PULLBACK_ADAPTER_CONTRACT_PASS_RESEARCH_ONLY"
    else:
        candidate_status = "ADAPTER_INPUT_FAIL_CLOSED"
    return {
        "candidate_gate_passed": passed,
        "candidate_status": candidate_status,
        "fixture": fixture,
        "blockers": blockers,
        "shadow_acceptance_gate": shadow_result,
        "symmetric_activation_gate": activation_result,
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    created = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{created}"
    summary_manifest, summary_error = safe_load(args.summary_scorer_manifest, root)
    summary_ready, summary_blockers = summary_scorer_ready(summary_manifest)
    candidate_manifest, candidate_error = safe_load(args.candidate_manifest, root) if args.candidate_manifest else (None, None)

    contract = adapter_contract(args)
    adapter_eval: dict[str, Any]
    blockers = []
    if summary_error:
        blockers.append(f"summary_scorer_manifest_{summary_error}")
    blockers.extend(summary_blockers)

    if candidate_manifest is None:
        adapter_eval = {
            "candidate_gate_passed": False,
            "candidate_status": "UNKNOWN_NO_SAFE_PULLBACK",
            "fixture": False,
            "blockers": ["candidate_pullback_missing"],
            "shadow_acceptance_gate": {"passed": False, "blockers": ["not_evaluated"]},
            "symmetric_activation_gate": {"passed": summary_ready, "blockers": summary_blockers},
        }
        real_input_status = "UNKNOWN_NO_SAFE_PULLBACK"
        decision = "KEEP" if summary_ready and not blockers else "UNKNOWN"
        decision_label = (
            "KEEP_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_SPEC_READY_REAL_INPUT_UNKNOWN"
            if decision == "KEEP"
            else "UNKNOWN_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_SPEC_INPUTS_INSUFFICIENT"
        )
    else:
        if candidate_error:
            blockers.append(f"candidate_manifest_{candidate_error}")
        adapter_eval = evaluate_candidate(candidate_manifest, args)
        blockers.extend(adapter_eval["blockers"])
        if not blockers and adapter_eval["fixture"]:
            decision = "KEEP"
            decision_label = "KEEP_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_SPEC_READY_FIXTURE_PASS_NOT_REAL"
            real_input_status = "UNKNOWN_FIXTURE_ONLY"
        elif not blockers:
            decision = "KEEP"
            decision_label = "KEEP_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_SPEC_READY_REAL_PULLBACK_PASS_RESEARCH_ONLY"
            real_input_status = "REAL_PULLBACK_ADAPTER_PASS_RESEARCH_ONLY"
        else:
            decision = "UNKNOWN"
            decision_label = "UNKNOWN_SYMMETRIC_ACTIVATION_ACCEPTANCE_ADAPTER_INPUTS_INSUFFICIENT"
            real_input_status = "UNKNOWN_ADAPTER_INPUT_FAIL_CLOSED"

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": created,
        "decision": decision,
        "decision_label": decision_label,
        "adapter_contract": contract,
        "summary_scorer_manifest": str(args.summary_scorer_manifest),
        "candidate_manifest": str(args.candidate_manifest) if args.candidate_manifest else None,
        "real_input_status": real_input_status,
        "summary_scorer_ready": summary_ready,
        "adapter_evaluation": adapter_eval,
        "blockers": blockers,
        "scope": {
            "local_only": True,
            "current_worktree_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "orders_cancels_redeems_sent": False,
            "trading_behavior_changed": False,
        },
        "research_ranking": {
            "status": decision_label,
            "summary_scorer_ready": summary_ready,
            "adapter_candidate_gate_passed": adapter_eval["candidate_gate_passed"],
            "strategy_evidence": (
                adapter_eval["candidate_gate_passed"]
                and not adapter_eval.get("fixture", False)
                and real_input_status == "REAL_PULLBACK_ADAPTER_PASS_RESEARCH_ONLY"
            ),
            "no_order_diagnostic_allowed": False,
            "notes": [
                "Adapter readiness alone must not start a no-order diagnostic.",
                "Synthetic fixtures and scorer readiness are not strategy evidence.",
                "A real future pass would still be no-order research evidence, not private truth or deployable evidence.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "status": "ADAPTER_SPEC_READY_NOT_PROMOTION_EVIDENCE",
            "normal_shadow_acceptance_gate_passed": bool(
                adapter_eval.get("shadow_acceptance_gate", {}).get("passed")
            ),
            "symmetric_activation_gate_passed": bool(
                adapter_eval.get("symmetric_activation_gate", {}).get("passed")
            ),
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "hard_blockers": [
                "private_truth_missing",
                "deployable_evidence_missing",
                "exact_user_approval_required_before_any_no_order_diagnostic",
            ],
        },
        "next_executable_action": (
            "Apply this adapter only to a future allowlisted no-order pullback that already contains "
            "symmetric_activation_summary_v1, or obtain explicit approval for an exact bounded no-order diagnostic; "
            "do not start diagnostics from adapter/spec readiness alone."
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-scorer-manifest", type=Path, default=DEFAULT_SUMMARY_SCORER)
    parser.add_argument("--candidate-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--min-candidates", type=int, default=100)
    parser.add_argument("--max-net-pair-cost-p90", type=float, default=1.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.15)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.20)
    parser.add_argument("--max-pair-tail-loss-share", type=float, default=0.05)
    parser.add_argument("--max-material-residual-lots", type=int, default=0)
    parser.add_argument("--min-fee-adjusted-pair-pnl-proxy", type=float, default=0.0)
    parser.add_argument("--min-blocked-activation-denominator", type=int, default=1)
    parser.add_argument("--min-admitted-activation-denominator", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    output_dir = args.output_dir or Path("xuan_research_artifacts") / f"{ARTIFACT}_{manifest['created_utc']}"
    print(output_dir / "manifest.json")
    return 0 if manifest["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
