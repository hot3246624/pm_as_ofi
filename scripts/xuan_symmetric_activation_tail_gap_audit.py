#!/usr/bin/env python3
"""Audit symmetric-activation no-order tail gaps.

This local-only audit reads allowlisted aggregate/summary pullback artifacts and
the already-produced scorer manifests. It looks for legal pre-action attribution
gaps behind p90 pair-cost and residual failures, then nominates instrumentation
only. It does not fetch data, inspect events JSONL/raw stores, start services,
or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_symmetric_activation_tail_gap_audit"
DEFAULT_DRIVER_ROOT = Path(
    "xuan_research_artifacts/xuan_symmetric_activation_shadow_review_driver_20260525T011338Z"
)
DEFAULT_AGGREGATE = DEFAULT_DRIVER_ROOT / "remote_clean/output/aggregate_report.json"
DEFAULT_SHADOW_ACCEPTANCE = DEFAULT_DRIVER_ROOT / "local_review_20260525T022531Z/shadow_acceptance/manifest.json"
DEFAULT_SYMMETRIC_SCORER = DEFAULT_DRIVER_ROOT / "local_review_20260525T022531Z/symmetric_activation_scorer/manifest.json"
DEFAULT_ACCEPTANCE_ADAPTER = DEFAULT_DRIVER_ROOT / "local_review_20260525T022531Z/acceptance_adapter/manifest.json"
SCHEMA_VERSION = "symmetric_activation_tail_gap_audit_v1"
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
HIGH_PAIR_COST_BUCKETS = ("pair_cost_1p00_1p05", "pair_cost_gt_1p05")
LOW_PAIR_COST_BUCKETS = ("pair_cost_lt_0p95",)
PAIR_COST_BUCKETS = HIGH_PAIR_COST_BUCKETS + ("pair_cost_0p95_1p00",) + LOW_PAIR_COST_BUCKETS


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
        try:
            return float(value)
        except ValueError:
            return default
    return default


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


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
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


def dict_sum(value: Any) -> float:
    if isinstance(value, dict):
        return sum(dict_sum(child) for child in value.values())
    return as_float(value)


def top_items(mapping: dict[str, Any], total: float, limit: int = 8) -> list[dict[str, Any]]:
    rows = []
    for key, value in mapping.items():
        amount = dict_sum(value)
        rows.append(
            {
                "key": key,
                "value": round(amount, 6),
                "share": round(amount / total, 6) if total else None,
            }
        )
    return sorted(rows, key=lambda row: row["value"], reverse=True)[:limit]


def combine_high_pair_cost_by_axis(axis_hist: dict[str, Any]) -> dict[str, float]:
    combined: dict[str, float] = {}
    for bucket in HIGH_PAIR_COST_BUCKETS:
        values = axis_hist.get(bucket)
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            combined[key] = combined.get(key, 0.0) + as_float(value)
    return combined


def cost_bucket_count(buckets: dict[str, Any], names: tuple[str, ...]) -> float:
    return sum(as_float(buckets.get(name)) for name in names)


def activation_summary_missing_tail_links(summary: dict[str, Any]) -> list[str]:
    required = [
        "pair_cost_bucket_by_status_reason_activation_bucket",
        "pair_cost_bucket_by_status_reason_side_offset_activation_bucket",
        "residual_qty_by_status_reason_activation_bucket",
        "residual_qty_by_status_reason_side_offset_activation_bucket",
        "residual_cost_by_status_reason_activation_bucket",
        "residual_cost_by_status_reason_side_offset_activation_bucket",
        "pair_tail_loss_by_status_reason_activation_bucket",
        "source_link_residual_tail_exemplars_by_activation_bucket",
    ]
    return [name for name in required if name not in summary]


def source_link_tail_summary(source_link: dict[str, Any], residual_cost: float, residual_qty: float) -> dict[str, Any]:
    cost_by_key = source_link.get("residual_cost_by_source_side_offset_risk_direction") or {}
    qty_by_key = source_link.get("residual_qty_by_source_side_offset_risk_direction") or {}
    count_by_key = source_link.get("residual_count_by_source_side_offset_risk_direction") or {}
    pair_cost_by_key = source_link.get("immediate_pair_cost_bucket_by_source_side_offset_risk_direction") or {}
    high_by_key: dict[str, float] = {}
    for key, buckets in pair_cost_by_key.items():
        if isinstance(buckets, dict):
            high = cost_bucket_count(buckets, HIGH_PAIR_COST_BUCKETS)
            if high:
                high_by_key[key] = high
    high_total = sum(high_by_key.values())
    return {
        "residual_cost_by_source_side_offset_risk_direction_top": top_items(cost_by_key, residual_cost),
        "residual_qty_by_source_side_offset_risk_direction_top": top_items(qty_by_key, residual_qty),
        "residual_count_by_source_side_offset_risk_direction_top": top_items(count_by_key, dict_sum(count_by_key)),
        "high_pair_cost_source_side_offset_risk_direction_top": top_items(high_by_key, high_total),
        "high_pair_cost_source_record_total": round(high_total, 6),
    }


def build_contract() -> dict[str, Any]:
    return {
        "contract_name": "symmetric_activation_tail_gap_audit_v1",
        "default_off": True,
        "purpose": (
            "Audit whether the failed no-order risk budget can be attributed to legal pre-action "
            "activation/source buckets and nominate instrumentation only."
        ),
        "allowed_inputs": [
            "allowlisted output/aggregate_report.json pullback",
            "allowlisted output/*.summary.json-derived aggregate fields",
            "shadow acceptance manifest",
            "symmetric activation summary scorer manifest",
            "symmetric activation acceptance adapter manifest",
            "current committed source/spec files",
        ],
        "forbidden_interpretations": [
            "using realized pair_cost buckets as live criteria",
            "adding a static side, price, or offset cap from this audit",
            "reviving public-profile single-day filters",
            "reviving D+ micro-deficit, ledger, tiny-deficit, static, cooldown, or fill-to-balance families",
            "claiming private truth, deployability, canary readiness, or promotion readiness",
            "starting SSH, shadow, canary, live, local agg, shared WS, or service processes",
            "reading events JSONL, raw/replay stores, or full completion stores",
        ],
        "field_contract": {
            "post_action_outcome_labels_included": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "trading_behavior_changed": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
    }


def evaluate(
    aggregate: dict[str, Any],
    shadow_acceptance: dict[str, Any],
    symmetric_scorer: dict[str, Any],
    acceptance_adapter: dict[str, Any],
) -> dict[str, Any]:
    metrics = aggregate.get("metrics") if isinstance(aggregate.get("metrics"), dict) else {}
    event_lite = aggregate.get("event_lite") if isinstance(aggregate.get("event_lite"), dict) else {}
    sym_summary = event_lite.get("symmetric_activation_summary") if isinstance(event_lite.get("symmetric_activation_summary"), dict) else {}
    source_link = event_lite.get("source_link_transition_diagnostics")
    source_link = source_link if isinstance(source_link, dict) else {}

    trading_metrics = shadow_acceptance.get("trading_metrics") if isinstance(shadow_acceptance.get("trading_metrics"), dict) else {}
    promotion = shadow_acceptance.get("promotion_gate") if isinstance(shadow_acceptance.get("promotion_gate"), dict) else {}
    checks = shadow_acceptance.get("checks") if isinstance(shadow_acceptance.get("checks"), dict) else {}
    residual_budget = promotion.get("normalized_risk_budget") if isinstance(promotion.get("normalized_risk_budget"), dict) else {}

    candidates = as_float(trading_metrics.get("candidates", metrics.get("candidates")))
    pair_actions = as_float(trading_metrics.get("pair_actions", metrics.get("pair_actions")))
    pair_pnl = as_float(trading_metrics.get("pair_pnl", metrics.get("pair_pnl")))
    fee_adjusted_pair_pnl = as_float(trading_metrics.get("fee_adjusted_pair_pnl_proxy"))
    net_pair_cost_p90 = as_float(trading_metrics.get("net_pair_cost_p90", metrics.get("net_pair_cost_proxy_p90")))
    residual_qty = as_float(trading_metrics.get("residual_qty", metrics.get("residual_qty")))
    residual_cost = as_float(trading_metrics.get("residual_cost", metrics.get("residual_cost")))
    residual_qty_share = as_float(residual_budget.get("residual_qty_share_of_filled", trading_metrics.get("residual_qty_share_of_filled")))
    residual_cost_share = as_float(residual_budget.get("residual_cost_share_of_filled_cost", trading_metrics.get("residual_cost_share_of_filled_cost")))
    pair_tail_loss_share = as_float(residual_budget.get("pair_tail_loss_share_of_pair_pnl", trading_metrics.get("pair_tail_loss_share_of_pair_pnl")))
    material_residual_lots = as_float(residual_budget.get("material_residual_lots", trading_metrics.get("material_residual_lots")))

    pair_cost_buckets = event_lite.get("net_pair_cost_buckets") or event_lite.get("pair_cost_buckets") or {}
    pair_bucket_total = dict_sum(pair_cost_buckets)
    high_pair_count = cost_bucket_count(pair_cost_buckets, HIGH_PAIR_COST_BUCKETS)
    high_pair_share = high_pair_count / pair_bucket_total if pair_bucket_total else 0.0

    high_by_offset = combine_high_pair_cost_by_axis(event_lite.get("pair_cost_by_source_offset_bucket") or {})
    high_by_public_px = combine_high_pair_cost_by_axis(event_lite.get("pair_cost_by_source_public_px_bucket") or {})
    high_offset_total = sum(high_by_offset.values())
    high_px_total = sum(high_by_public_px.values())

    residual_side_cost = event_lite.get("residual_lot_side_cost") or {}
    residual_side_qty = event_lite.get("residual_lot_side_qty") or {}
    residual_px_qty = event_lite.get("residual_lot_px_qty_buckets") or {}
    residual_low_px_qty = as_float(residual_px_qty.get("px_lt_0p30")) + as_float(residual_px_qty.get("px_0p30_0p45"))
    residual_yes_cost = as_float(residual_side_cost.get("YES"))
    residual_yes_qty = as_float(residual_side_qty.get("YES"))

    activation_counts = sym_summary.get("candidate_count_by_status_reason_activation_bucket") or {}
    missing_tail_links = activation_summary_missing_tail_links(sym_summary)
    scorer_checks = nested_get(symmetric_scorer, ["score", "checks"], {})
    adapter_eval = acceptance_adapter.get("adapter_evaluation") if isinstance(acceptance_adapter.get("adapter_evaluation"), dict) else {}

    failure_profile = {
        "economic_positive": pair_pnl > 0.0 and fee_adjusted_pair_pnl > 0.0,
        "no_order_safety_ok": checks.get("no_order_safety_ok") is True,
        "sample_size_ok": candidates >= 100.0,
        "net_pair_cost_p90_failed": net_pair_cost_p90 > 1.0,
        "residual_qty_share_failed": residual_qty_share > 0.15,
        "residual_cost_share_failed": residual_cost_share > 0.20,
        "pair_tail_loss_share_failed": pair_tail_loss_share > 0.05,
        "material_residual_lots_failed": material_residual_lots > 0.0,
        "activation_summary_present": bool(sym_summary),
        "summary_scorer_ready": symmetric_scorer.get("decision_label") == "KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_READY",
        "summary_schema_ok": scorer_checks.get("schema_ok") is True,
        "summary_field_contract_ok": scorer_checks.get("field_contract_ok") is True,
        "summary_source_sequence_coverage_present": scorer_checks.get("source_sequence_coverage_present") is True,
        "adapter_failed_only_on_shadow_risk_budget": (
            adapter_eval.get("symmetric_activation_gate", {}).get("passed") is True
            and adapter_eval.get("shadow_acceptance_gate", {}).get("passed") is False
        ),
    }

    legal_concentration = {
        "high_pair_cost_pair_action_share": round(high_pair_share, 6) if pair_bucket_total else None,
        "high_pair_cost_pair_actions": round(high_pair_count, 6),
        "pair_cost_bucket_total": round(pair_bucket_total, 6),
        "high_pair_cost_by_offset_source_records_top": top_items(high_by_offset, high_offset_total),
        "high_pair_cost_by_public_px_source_records_top": top_items(high_by_public_px, high_px_total),
        "residual_side_cost": residual_side_cost,
        "residual_side_qty": residual_side_qty,
        "residual_yes_cost_share": round(residual_yes_cost / residual_cost, 6) if residual_cost else None,
        "residual_yes_qty_share": round(residual_yes_qty / residual_qty, 6) if residual_qty else None,
        "residual_low_px_qty_share": round(residual_low_px_qty / residual_qty, 6) if residual_qty else None,
        "source_link_tail_summary": source_link_tail_summary(source_link, residual_cost, residual_qty),
        "activation_candidate_counts": activation_counts,
        "activation_missing_tail_join_fields": missing_tail_links,
    }

    instrumentation_target_ready = (
        failure_profile["economic_positive"]
        and failure_profile["no_order_safety_ok"]
        and failure_profile["sample_size_ok"]
        and failure_profile["activation_summary_present"]
        and failure_profile["summary_scorer_ready"]
        and failure_profile["summary_field_contract_ok"]
        and failure_profile["adapter_failed_only_on_shadow_risk_budget"]
        and bool(missing_tail_links)
        and (
            (high_offset_total and top_items(high_by_offset, high_offset_total, 1)[0]["share"] >= 0.35)
            or (residual_cost and residual_yes_cost / residual_cost >= 0.50)
            or (residual_qty and residual_low_px_qty / residual_qty >= 0.50)
        )
    )

    if not failure_profile["economic_positive"]:
        decision = "DISCARD"
        decision_label = "DISCARD_SYMMETRIC_ACTIVATION_TAIL_GAP_ECONOMIC_NEGATIVE"
    elif instrumentation_target_ready:
        decision = "KEEP"
        decision_label = "KEEP_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_INSTRUMENTATION_TARGET_READY"
    elif not failure_profile["activation_summary_present"] or not failure_profile["summary_scorer_ready"] or not source_link:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_INPUTS_INSUFFICIENT"
    else:
        decision = "DISCARD"
        decision_label = "DISCARD_SYMMETRIC_ACTIVATION_TAIL_GAP_BROAD_RESIDUAL_OR_FORBIDDEN_ONLY"

    return {
        "decision": decision,
        "decision_label": decision_label,
        "shadow_metrics": {
            "candidates": candidates,
            "pair_actions": pair_actions,
            "pair_pnl": pair_pnl,
            "fee_adjusted_pair_pnl_proxy": fee_adjusted_pair_pnl,
            "net_pair_cost_p90": net_pair_cost_p90,
            "residual_qty": residual_qty,
            "residual_cost": residual_cost,
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "pair_tail_loss_share": pair_tail_loss_share,
            "material_residual_lots": material_residual_lots,
        },
        "failure_profile": failure_profile,
        "legal_concentration": legal_concentration,
        "recommended_instrumentation_target": {
            "name": "symmetric_activation_tail_attribution_summary_v1",
            "default_off": True,
            "behavior_change": False,
            "purpose": (
                "Join activation_bucket and activation age with pair-cost, pair-tail-loss, residual qty/cost, "
                "side, offset, source risk direction, and source_sequence coverage so a future pullback can "
                "explain risk-budget failures without static caps or future labels."
            ),
            "required_summary_fields": missing_tail_links,
            "explicitly_not_live_rules": [
                "Do not trade on realized pair_cost buckets.",
                "Do not add a YES/NO side cap.",
                "Do not add public-price, offset, or static source caps from this audit.",
                "Do not restart D+ micro-deficit/ledger/tiny-deficit/fill-to-balance/public-profile families.",
            ],
        },
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    created = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{created}"

    loads: dict[str, tuple[dict[str, Any] | None, str | None]] = {
        "aggregate": safe_load(args.aggregate, root),
        "shadow_acceptance": safe_load(args.shadow_acceptance_manifest, root),
        "symmetric_scorer": safe_load(args.symmetric_activation_scorer_manifest, root),
        "acceptance_adapter": safe_load(args.acceptance_adapter_manifest, root),
    }
    load_errors = {name: error for name, (_, error) in loads.items() if error}
    if load_errors:
        evaluation = {
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_SYMMETRIC_ACTIVATION_TAIL_GAP_AUDIT_INPUTS_INSUFFICIENT",
            "load_errors": load_errors,
        }
    else:
        evaluation = evaluate(
            loads["aggregate"][0] or {},
            loads["shadow_acceptance"][0] or {},
            loads["symmetric_scorer"][0] or {},
            loads["acceptance_adapter"][0] or {},
        )

    decision = evaluation["decision"]
    decision_label = evaluation["decision_label"]
    manifest = {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "audit_schema_version": SCHEMA_VERSION,
        "created_utc": created,
        "decision": decision,
        "decision_label": decision_label,
        "inputs": {
            "aggregate": str(args.aggregate),
            "shadow_acceptance_manifest": str(args.shadow_acceptance_manifest),
            "symmetric_activation_scorer_manifest": str(args.symmetric_activation_scorer_manifest),
            "acceptance_adapter_manifest": str(args.acceptance_adapter_manifest),
        },
        "load_errors": load_errors,
        "audit_contract": build_contract(),
        "evaluation": evaluation,
        "next_executable_action": (
            "Implement default-off symmetric_activation_tail_attribution_summary_v1 plus smoke, then score only "
            "future allowlisted pullbacks; do not start another no-order diagnostic from this audit alone."
            if decision == "KEEP"
            else (
                "Stop this symmetric-activation parameter line and pivot; do not add forbidden static caps or run another shadow."
                if decision == "DISCARD"
                else "Fix missing allowlisted aggregate/scorer fields before continuing; do not start another shadow."
            )
        ),
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "notes": [
                "This audit explains an UNKNOWN no-order result; it is not private truth.",
                "Observed tail buckets may nominate instrumentation, not live trading filters.",
                "research_ranking and promotion_gate remain separate.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "TAIL_GAP_AUDIT_NOT_PROMOTION_EVIDENCE",
        },
        "scope": {
            "current_worktree_only": True,
            "local_only": True,
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
    }
    write_json(out_dir / "manifest.json", manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregate", type=Path, default=DEFAULT_AGGREGATE)
    parser.add_argument("--shadow-acceptance-manifest", type=Path, default=DEFAULT_SHADOW_ACCEPTANCE)
    parser.add_argument("--symmetric-activation-scorer-manifest", type=Path, default=DEFAULT_SYMMETRIC_SCORER)
    parser.add_argument("--acceptance-adapter-manifest", type=Path, default=DEFAULT_ACCEPTANCE_ADAPTER)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
