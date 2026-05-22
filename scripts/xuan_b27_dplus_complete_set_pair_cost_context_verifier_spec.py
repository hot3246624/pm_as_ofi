#!/usr/bin/env python3
"""Review D+ complete-set pair-cost context verifier implementability.

This is a local-only spec builder. It converts a public Polymarket profile
probe plus the latest no-order residual-tail exemplar review into a concrete
verifier target. The public profile remains proxy inspiration only; this script
does not read private account truth, replay/raw stores, events JSONL, sockets,
SSH, or live paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


DEFAULT_PUBLIC_PROFILE_SUMMARY = Path(
    "xuan_research_artifacts/"
    "public_profile_probe_0xce25e214d5cfe4f459cf67f08df581885aae7fdc_20260522T0910Z/"
    "summary.json"
)
DEFAULT_COMPLETION_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_shadow_review_residual_tail_exemplar_completion_20260522T092012Z/"
    "manifest.json"
)
DEFAULT_EXEMPLAR_SCORE = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_shadow_review_residual_tail_exemplar_driver_20260522T083136Z/"
    "local_review_20260522T092012Z/residual_tail_exemplar_score/score.json"
)
DEFAULT_AGGREGATE_REPORT = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_shadow_review_residual_tail_exemplar_driver_20260522T083136Z/"
    "remote_clean/output/aggregate_report.json"
)

FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)

PUBLIC_PROFILE_REQUIRED_FIELDS = (
    "counts",
    "by_side",
    "by_outcome",
    "by_slug",
    "conditions_total",
    "both_outcome_conditions",
    "top_conditions",
)

PUBLIC_TOP_CONDITION_FIELDS = (
    "conditionId",
    "slug",
    "n",
    "outcomes",
    "sides",
    "notional",
    "size",
    "min_ts",
    "max_ts",
    "span_s",
    "both_outcomes",
)

NO_ORDER_REQUIRED_EXEMPLAR_FIELDS = (
    "condition_id",
    "slug",
    "side",
    "offset_s",
    "source_risk_direction",
    "trigger_px",
    "trigger_size",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_ms",
    "source_sequence_id",
    "quote_intent_id",
    "source_order_id",
)

PAIR_COST_CONTEXT_FIELDS = (
    "condition_id",
    "slug",
    "market_family",
    "timeframe_s",
    "outcome",
    "side",
    "ts_ms",
    "price",
    "size",
    "public_trade_row_id_or_tx_hash",
    "paired_opposite_outcome_qty",
    "complete_set_matched_qty",
    "complete_set_pair_cost_sum",
    "complete_set_pair_cost_edge",
    "fee_rate",
    "fee_rate_source",
    "unpaired_residual_qty",
    "unpaired_residual_cost",
    "near_dual_outcome_window_s",
    "same_condition_dual_outcome_evidence",
)

FROZEN_OR_DISALLOWED_MECHANISMS = (
    "summary_only_removal",
    "static_side_offset_cutoff",
    "source_public_price_cap",
    "public_trade_px_cap",
    "residual_cooldown",
    "admission_cap",
    "risk_increasing_pair_fill_cap_v1",
    "ledger_tail_first_pair_priority_v1",
    "fill_to_balance90_shadow",
    "portfolio_ledger_shadow",
    "promotion_or_canary",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def get_nested(obj: dict[str, Any], path: Iterable[str], default: Any = None) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def slug_items(by_slug: Any) -> list[tuple[str, int]]:
    items: list[tuple[str, int]] = []
    if isinstance(by_slug, dict):
        for key, value in by_slug.items():
            items.append((str(key), as_int(value)))
    elif isinstance(by_slug, list):
        for row in by_slug:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                items.append((str(row[0]), as_int(row[1])))
            elif isinstance(row, dict):
                slug = row.get("slug") or row.get("key")
                count = row.get("n") or row.get("count") or row.get("value")
                if isinstance(count, list) and len(count) >= 2:
                    slug = count[0]
                    count = count[1]
                if slug is not None:
                    items.append((str(slug), as_int(count)))
    return items


def missing(required: tuple[str, ...], obj: dict[str, Any]) -> list[str]:
    return [field for field in required if field not in obj]


def public_profile_checks(summary: dict[str, Any]) -> dict[str, Any]:
    counts = summary.get("counts") if isinstance(summary.get("counts"), dict) else {}
    by_side = summary.get("by_side") if isinstance(summary.get("by_side"), dict) else {}
    by_outcome = summary.get("by_outcome") if isinstance(summary.get("by_outcome"), dict) else {}
    top_conditions = summary.get("top_conditions") if isinstance(summary.get("top_conditions"), list) else []
    trades = as_int(counts.get("trades"))
    conditions_total = as_int(summary.get("conditions_total"))
    both_outcome_conditions = as_int(summary.get("both_outcome_conditions"))
    both_outcome_ratio = both_outcome_conditions / conditions_total if conditions_total else 0.0
    buy_count = as_int(by_side.get("BUY") or by_side.get("buy"))
    buy_ratio = buy_count / trades if trades else 0.0
    up_count = as_int(by_outcome.get("Up") or by_outcome.get("UP") or by_outcome.get("Yes"))
    down_count = as_int(by_outcome.get("Down") or by_outcome.get("DOWN") or by_outcome.get("No"))
    slugs = slug_items(summary.get("by_slug"))
    updown_15m_slug_count = sum(count for slug, count in slugs if "updown-15m" in slug)
    top_missing_by_index = {
        str(index): missing(PUBLIC_TOP_CONDITION_FIELDS, row)
        for index, row in enumerate(top_conditions[:10])
        if isinstance(row, dict) and missing(PUBLIC_TOP_CONDITION_FIELDS, row)
    }
    top_both_count = sum(
        1
        for row in top_conditions
        if isinstance(row, dict) and bool(row.get("both_outcomes"))
    )
    top_condition_count = len(top_conditions)
    top_both_ratio = top_both_count / top_condition_count if top_condition_count else 0.0
    proxy_signal_ok = (
        trades >= 500
        and conditions_total >= 20
        and both_outcome_ratio >= 0.80
        and buy_ratio >= 0.95
        and up_count > 0
        and down_count > 0
        and updown_15m_slug_count > 0
        and not top_missing_by_index
    )
    return {
        "required_fields_present": not missing(PUBLIC_PROFILE_REQUIRED_FIELDS, summary),
        "missing_fields": missing(PUBLIC_PROFILE_REQUIRED_FIELDS, summary),
        "trades": trades,
        "conditions_total": conditions_total,
        "both_outcome_conditions": both_outcome_conditions,
        "both_outcome_ratio": round(both_outcome_ratio, 6),
        "buy_count": buy_count,
        "buy_ratio": round(buy_ratio, 6),
        "up_count": up_count,
        "down_count": down_count,
        "updown_15m_slug_count": updown_15m_slug_count,
        "top_condition_count": top_condition_count,
        "top_both_outcome_ratio": round(top_both_ratio, 6),
        "top_condition_missing_fields": top_missing_by_index,
        "proxy_complete_set_signal_ok": proxy_signal_ok,
        "interpretation": (
            "Public/proxy signal suggests complete-set style because recent trades are overwhelmingly BUY, "
            "both Up/Down outcomes are present, and most conditions have both outcomes. This is inspiration only."
        ),
    }


def no_order_contract_checks(score: dict[str, Any], completion_manifest: dict[str, Any]) -> dict[str, Any]:
    checks = score.get("checks") if isinstance(score.get("checks"), dict) else {}
    metrics = score.get("exemplar_metrics") if isinstance(score.get("exemplar_metrics"), dict) else {}
    coverage_by_field = (
        metrics.get("required_field_coverage_by_field")
        if isinstance(metrics.get("required_field_coverage_by_field"), dict)
        else {}
    )
    missing_coverage_fields = [
        field
        for field in NO_ORDER_REQUIRED_EXEMPLAR_FIELDS
        if as_float(coverage_by_field.get(field), -1.0) < 1.0
    ]
    required_field_coverage = as_float(metrics.get("required_field_coverage"))
    source_sequence_coverage = as_float(metrics.get("source_sequence_coverage"))
    quote_intent_coverage = as_float(metrics.get("quote_intent_coverage"))
    source_order_coverage = as_float(metrics.get("source_order_coverage"))
    aggregate_rows = as_int(metrics.get("aggregate_exemplar_row_count"))
    field_contract_ok = (
        checks.get("report_shape_ok") is True
        and checks.get("required_field_coverage_ok") is True
        and required_field_coverage >= 1.0
        and source_sequence_coverage >= 0.95
        and quote_intent_coverage >= 0.95
        and source_order_coverage >= 0.95
        and aggregate_rows > 0
        and not missing_coverage_fields
    )
    completion_score = (
        completion_manifest.get("residual_tail_exemplar_score")
        if isinstance(completion_manifest.get("residual_tail_exemplar_score"), dict)
        else {}
    )
    return {
        "score_status": score.get("status") or score.get("decision_label"),
        "completion_status": completion_score.get("status"),
        "field_contract_ok": field_contract_ok,
        "report_shape_ok": checks.get("report_shape_ok") is True,
        "required_field_coverage": required_field_coverage,
        "source_sequence_coverage": source_sequence_coverage,
        "quote_intent_coverage": quote_intent_coverage,
        "source_order_coverage": source_order_coverage,
        "aggregate_exemplar_row_count": aggregate_rows,
        "missing_coverage_fields": missing_coverage_fields,
        "metric_budgets_ok": checks.get("metric_budgets_ok") is True,
        "metric_budget_status": "NOT_REQUIRED_FOR_SPEC_SELECTION",
        "shadow_gate_failures": score.get("shadow_gate_failures", []),
        "interpretation": (
            "No-order exemplar fields are sufficient for verifier design even though the latest shadow failed "
            "sample/risk budgets. This spec does not convert the shadow into promotion evidence."
        ),
    }


def aggregate_capability(aggregate_report: dict[str, Any]) -> dict[str, Any]:
    if not aggregate_report:
        return {"present": False, "used_for_decision": False}
    text = json.dumps(aggregate_report, sort_keys=True)
    return {
        "present": True,
        "used_for_decision": False,
        "contains_residual_tail_exemplar_summary": "residual_tail" in text or "source_residual" in text,
        "contains_pair_source_summary": "source_pair" in text or "pair_source" in text,
        "contains_source_link_transition_summary": "source_link" in text,
    }


def review(
    *,
    public_profile_summary_path: Path,
    completion_manifest_path: Path,
    exemplar_score_path: Path,
    aggregate_report_path: Path | None,
) -> dict[str, Any]:
    paths = {
        "public_profile_summary": public_profile_summary_path,
        "completion_manifest": completion_manifest_path,
        "residual_tail_exemplar_score": exemplar_score_path,
    }
    if aggregate_report_path is not None:
        paths["aggregate_report"] = aggregate_report_path
    paths_safe = {key: path_safe(path) for key, path in paths.items()}
    paths_exist = {key: path.exists() for key, path in paths.items()}

    public_summary = load_json(public_profile_summary_path)
    completion_manifest = load_json(completion_manifest_path)
    exemplar_score = load_json(exemplar_score_path)
    aggregate_report = load_json(aggregate_report_path) if aggregate_report_path and aggregate_report_path.exists() else {}

    public_checks = public_profile_checks(public_summary)
    no_order_checks = no_order_contract_checks(exemplar_score, completion_manifest)
    aggregate_checks = aggregate_capability(aggregate_report)
    inputs_ok = all(paths_safe.values()) and all(paths_exist.values())
    spec_ready = (
        inputs_ok
        and public_checks["required_fields_present"]
        and public_checks["proxy_complete_set_signal_ok"]
        and no_order_checks["field_contract_ok"]
    )
    decision = "KEEP" if spec_ready else "UNKNOWN"
    label = (
        "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_SPEC_READY"
        if spec_ready
        else "UNKNOWN_COMPLETE_SET_PAIR_COST_CONTEXT_INPUT_GAP"
    )
    blockers: list[str] = []
    if not inputs_ok:
        blockers.append("unsafe_or_missing_input_paths")
    if not public_checks["required_fields_present"]:
        blockers.append("public_profile_summary_missing_required_fields")
    if not public_checks["proxy_complete_set_signal_ok"]:
        blockers.append("public_profile_complete_set_proxy_signal_insufficient")
    if not no_order_checks["field_contract_ok"]:
        blockers.append("no_order_residual_tail_exemplar_field_contract_insufficient")

    return {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_complete_set_pair_cost_context_verifier_spec",
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": "local_implementability_review_verifier_spec",
        "inputs": {key: str(path) for key, path in paths.items()},
        "path_safety": paths_safe,
        "paths_exist": paths_exist,
        "blockers": blockers,
        "public_proxy_boundary": {
            "public_profile_is_proxy_inspiration_only": True,
            "learning_target_private_truth_available": False,
            "learning_target_private_truth_required": False,
            "our_private_truth_ready": False,
            "interpretation": (
                "The public profile can suggest a complete-set/pair-cost hypothesis, but it cannot prove "
                "owner orders, fills, queue state, inventory, wallet, redeem, deployability, or promotion."
            ),
        },
        "public_profile_complete_set_proxy_signal": public_checks,
        "no_order_residual_tail_field_contract": no_order_checks,
        "aggregate_report_capability": aggregate_checks,
        "verifier_spec": {
            "name": "complete_set_pair_cost_context_verifier_v1",
            "lane": "causal_verifier",
            "status": "SPEC_READY_LOCAL_VERIFIER_NEEDED" if spec_ready else "INPUT_GAP",
            "purpose": (
                "Test whether a complete-set / paired-opposite-outcome cost context can explain the public "
                "profile pattern and map to our no-order residual-tail contexts without summary-only deletion."
            ),
            "must_start_from": [
                "public profile summary as proxy inspiration",
                "allowlisted no-order aggregate/summary score outputs",
                "candidate_base or candidate_registry selected actions when implementing the local verifier",
            ],
            "must_not_start_from": [
                "learning target private order/fill/inventory data",
                "replay_store_v2 broad strategy search",
                "events JSONL",
                "raw/replay/collector/full completion-store scan",
                "summary-only removal",
                "static side/offset deletion",
                "price cap/cooldown/admission-cap families",
            ],
            "public_profile_trade_fields_needed": [
                "condition_id",
                "slug",
                "market_family",
                "timeframe_s",
                "outcome",
                "side",
                "ts_ms",
                "price",
                "size",
                "public_trade_row_id_or_tx_hash",
            ],
            "no_order_exemplar_fields_needed": list(NO_ORDER_REQUIRED_EXEMPLAR_FIELDS),
            "complete_set_pair_cost_context_fields": list(PAIR_COST_CONTEXT_FIELDS),
            "join_and_alignment_policy": {
                "public_profile": "Group by condition_id/slug and pair same-condition opposite outcomes within bounded windows.",
                "local_candidate_or_shadow": (
                    "Align no-order exemplar rows by condition_id, slug, side, trigger_ts_ms, trigger_px, "
                    "trigger_size, source_sequence_id, quote_intent_id, or source_order_id when present."
                ),
                "do_not_fabricate_missing_ids": True,
                "fallback_if_public_trade_ids_absent": (
                    "Use condition/slug/outcome/ts/price/size as public proxy keys and report exact missing ids."
                ),
            },
            "future_verifier_output_contract": {
                "score_json": "complete_set_pair_cost_context_score.json",
                "ranked_groups": [
                    "condition_complete_set_pair_cost_groups",
                    "near_dual_outcome_windows",
                    "unpaired_residual_contexts",
                    "no_order_exemplar_alignment_gaps",
                ],
                "required_sections": [
                    "research_ranking",
                    "promotion_gate",
                    "public_proxy_boundary",
                    "missing_fields",
                    "guardrails",
                ],
            },
            "future_verifier_pass_criteria": {
                "public_proxy_both_outcome_condition_ratio_min": 0.80,
                "public_proxy_buy_ratio_min": 0.95,
                "no_order_required_field_coverage_min": 1.0,
                "source_sequence_coverage_min": 0.95,
                "complete_set_pair_cost_edge_must_be_quantified": True,
                "sample_preserving_rule_required_before_shadow": True,
                "promotion_gate_passed": False,
                "private_truth_ready": False,
                "deployable": False,
            },
        },
        "guardrails": {
            "remote_run_approved": False,
            "shadow_or_canary_start_allowed": False,
            "summary_only_removal_recommended": False,
            "promotion_or_canary_claim_allowed": False,
            "fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "fee_rate_policy": "fee_rate must come from run/config/API/explicit experiment; never from account-average hard-coding.",
            "frozen_mechanisms_not_allowed": list(FROZEN_OR_DISALLOWED_MECHANISMS),
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "Spec readiness only. A KEEP means the public/proxy complete-set signal and no-order exemplar "
                "field contract are sufficient to implement a local verifier, not that the strategy passes "
                "economics or promotion."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "VERIFIER_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
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
            "Implement local complete_set_pair_cost_context_verifier_v1 scorer over the public profile summary "
            "and no-order residual-tail exemplar score; do not start remote shadow/canary from this spec alone."
            if spec_ready
            else "Fix exact input gaps before implementing the local verifier scorer."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-profile-summary", type=Path, default=DEFAULT_PUBLIC_PROFILE_SUMMARY)
    parser.add_argument("--completion-manifest", type=Path, default=DEFAULT_COMPLETION_MANIFEST)
    parser.add_argument("--exemplar-score", type=Path, default=DEFAULT_EXEMPLAR_SCORE)
    parser.add_argument("--aggregate-report", type=Path, default=DEFAULT_AGGREGATE_REPORT)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = review(
        public_profile_summary_path=args.public_profile_summary,
        completion_manifest_path=args.completion_manifest,
        exemplar_score_path=args.exemplar_score,
        aggregate_report_path=args.aggregate_report,
    )
    write_json(args.output, report)
    print(args.output)
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
