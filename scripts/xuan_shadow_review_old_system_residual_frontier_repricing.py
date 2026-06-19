#!/usr/bin/env python3
"""Build an old-system residual frontier and repricing review.

This artifact deliberately ignores the newer backtest V1 candidate audit pack.
It uses existing xuan no-order/shadow scorecards and copied local remote outputs
to answer whether the old-system evidence shows a stable tradeoff between ROI,
participation, and residual exposure.

Local/research-only: it does not authorize remote dry-runs, deployment, live
orders, capacity expansion, or private-truth claims.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0245Z"
RUN_BEST = "xuan-frontier-soft-closeability-comparable-20260522T1705Z"
RUN_CAP25 = "xuan-frontier-soft-mainline-cap25-reproduction-20260525T2041Z"
RUN_CAPPED = "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z"
RUN_MATURE = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"

DEFAULT_BEST = Path(".tmp_xuan/scorecards/no_order_soft_closeability_comparable_runtime_run_20260522T1808Z.json")
DEFAULT_CAP25_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{RUN_CAP25}_runtime_summary.json")
DEFAULT_CAP25_CAPITAL = Path(f".tmp_xuan/scorecards/no_order_{RUN_CAP25}_capital_reuse_roi.json")
DEFAULT_CAPPED_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{RUN_CAPPED}_runtime_summary.json")
DEFAULT_CAPPED_CAPITAL = Path(f".tmp_xuan/scorecards/no_order_{RUN_CAPPED}_capital_reuse_roi.json")
DEFAULT_CAPPED_CAPACITY = Path(".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_capacity_interpretation_20260526T1915Z.json")
DEFAULT_MATURE_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_result_20260526T2148Z.json")
DEFAULT_PUBLIC_04B6 = Path(".tmp_xuan/scorecards/xuan_public_04b6_current_proxy_review_20260526T1802Z.json")
DEFAULT_CAPPED_REMOTE_OUTPUTS = Path(f".tmp_xuan/local_verifier_artifacts/{RUN_CAPPED}/remote_outputs")
DEFAULT_SCORECARD = Path(f".tmp_xuan/scorecards/xuan_shadow_review_old_system_residual_frontier_repricing_{STAMP}.json")
DEFAULT_MARKDOWN = Path(
    f".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_old_system_residual_frontier_repricing_{STAMP}/"
    "OLD_SYSTEM_RESIDUAL_FRONTIER_REPRICING.md"
)


def load_json(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    with resolved.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def status(card: dict[str, Any] | None) -> str:
    return str((card or {}).get("status") or "")


def decision(card: dict[str, Any] | None) -> dict[str, Any]:
    return body(card or {}, "decision")


def pct(value: float | None) -> float | None:
    return None if value is None else value * 100.0


def sample_from_metrics(
    *,
    run_id: str,
    label: str,
    metrics: dict[str, Any],
    sample_minutes: float,
    sample_kind: str,
    capital_card: dict[str, Any] | None = None,
    capacity_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pair_pnl = fnum(metrics.get("pair_pnl"), 0.0) or 0.0
    filled_cost = fnum(metrics.get("filled_cost"))
    roi_on_filled_cost = fnum(metrics.get("roi_on_filled_cost"))
    residual_cost = fnum(metrics.get("residual_cost"))
    residual_qty = fnum(metrics.get("residual_qty"))
    residual_cost_share = fnum(metrics.get("residual_cost_share"))
    residual_qty_share = fnum(metrics.get("residual_qty_share"))
    pair_qty = fnum((capacity_metrics or {}).get("pair_qty_redeem_notional"))
    if pair_qty is None:
        pair_qty = fnum((capital_card or {}).get("aggregate", {}).get("round_roi", {}).get("pair_qty"))
    if pair_qty is None:
        pair_qty = fnum((capital_card or {}).get("aggregate", {}).get("totals", {}).get("pair_qty"))
    residual_cost_to_pair_qty = fnum((capacity_metrics or {}).get("residual_cost_to_pair_qty"))
    if residual_cost_to_pair_qty is None and residual_cost is not None and pair_qty:
        residual_cost_to_pair_qty = residual_cost / pair_qty

    periods_per_day = 1440.0 / sample_minutes if sample_minutes > 0 else None
    current_scale_daily_pair_pnl = pair_pnl * periods_per_day if periods_per_day else None

    projection = body(body(capital_card or {}, "aggregate"), "projection")
    return rounded(
        {
            "run_id": run_id,
            "label": label,
            "sample_kind": sample_kind,
            "sample_minutes": sample_minutes,
            "accepted_actions": fnum(metrics.get("accepted_actions")),
            "queue_supported_fills": fnum(metrics.get("queue_supported_fills")),
            "strict_rescue_closes": fnum(metrics.get("strict_rescue_closes")),
            "pair_pnl": pair_pnl,
            "roi_on_filled_cost": roi_on_filled_cost,
            "roi_on_filled_cost_pct": pct(roi_on_filled_cost),
            "filled_cost": filled_cost,
            "residual_cost": residual_cost,
            "residual_qty": residual_qty,
            "residual_cost_share": residual_cost_share,
            "residual_cost_share_pct": pct(residual_cost_share),
            "residual_qty_share": residual_qty_share,
            "residual_qty_share_pct": pct(residual_qty_share),
            "residual_cost_to_pair_qty": residual_cost_to_pair_qty,
            "residual_cost_to_pair_qty_pct": pct(residual_cost_to_pair_qty),
            "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max")),
            "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
            "strict_rescue_source_blocks": fnum(metrics.get("strict_rescue_source_blocks")),
            "current_scale_daily_pair_pnl": current_scale_daily_pair_pnl,
            "capital_reuse_projection": {
                "round_minutes": fnum(projection.get("round_minutes")),
                "rounds_per_day": fnum(projection.get("rounds_per_day")),
                "profit_per_day_if_pair_cost_filled_every_round": fnum(
                    projection.get("profit_per_day_if_pair_cost_filled_every_round")
                ),
                "profit_per_day_if_redeem_notional_filled_every_round": fnum(
                    projection.get("profit_per_day_if_redeem_notional_filled_every_round")
                ),
            },
        }
    )


def load_residual_lots(remote_outputs: Path) -> list[dict[str, Any]]:
    lots: list[dict[str, Any]] = []
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.residual_fifo_lots.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                cost = fnum(row.get("cost"), 0.0) or 0.0
                qty = fnum(row.get("qty"), 0.0) or 0.0
                if cost <= 0 and qty <= 0:
                    continue
                lots.append(
                    {
                        "source_file": path.name,
                        "quote_intent_id": row.get("quote_intent_id"),
                        "slug": row.get("slug"),
                        "side": row.get("side"),
                        "qty": qty,
                        "px": fnum(row.get("px")),
                        "cost": cost,
                        "fill_ms": fnum(row.get("fill_ms")),
                        "closeability_debt": fnum(row.get("closeability_debt"), 0.0) or 0.0,
                    }
                )
    return lots


def aggregate_by_slug(lots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    total_cost = sum(fnum(lot.get("cost"), 0.0) or 0.0 for lot in lots)
    total_qty = sum(fnum(lot.get("qty"), 0.0) or 0.0 for lot in lots)
    for lot in lots:
        slug = str(lot.get("slug") or "")
        row = grouped.setdefault(slug, {"slug": slug, "cost": 0.0, "qty": 0.0, "lot_count": 0})
        row["cost"] += fnum(lot.get("cost"), 0.0) or 0.0
        row["qty"] += fnum(lot.get("qty"), 0.0) or 0.0
        row["lot_count"] += 1
    out = []
    for row in grouped.values():
        out.append(
            {
                **row,
                "cost_share_of_residual": row["cost"] / total_cost if total_cost else None,
                "qty_share_of_residual": row["qty"] / total_qty if total_qty else None,
            }
        )
    return sorted(rounded(out), key=lambda item: item.get("cost", 0.0), reverse=True)


def greedy_reduction(lots: list[dict[str, Any]], required_reduction: float) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    selected_cost = 0.0
    selected_qty = 0.0
    for lot in sorted(lots, key=lambda item: (fnum(item.get("cost"), 0.0) or 0.0), reverse=True):
        if selected_cost >= required_reduction:
            break
        selected.append(lot)
        selected_cost += fnum(lot.get("cost"), 0.0) or 0.0
        selected_qty += fnum(lot.get("qty"), 0.0) or 0.0
    return rounded(
        {
            "required_reduction": required_reduction,
            "selected_lot_count": len(selected),
            "selected_cost": selected_cost,
            "selected_qty": selected_qty,
            "covers_required_reduction": selected_cost >= required_reduction,
            "selected_lots": selected[:8],
        }
    )


def repricing_scenarios(pair_pnl: float, residual_cost: float, residual_qty: float) -> list[dict[str, Any]]:
    scenarios = []
    for recovery in [0.0, 0.25, 0.5, 0.514326, 0.6, 0.75, 1.0]:
        residual_mark_value = residual_cost * recovery
        adjusted_pnl = pair_pnl - residual_cost + residual_mark_value
        scenarios.append(
            {
                "residual_cost_recovery_rate": recovery,
                "residual_mark_value": residual_mark_value,
                "adjusted_pair_pnl_after_residual_mark": adjusted_pnl,
                "adjusted_pnl_per_residual_qty": adjusted_pnl / residual_qty if residual_qty else None,
            }
        )
    return rounded(scenarios)


def frontier_deltas(samples: list[dict[str, Any]]) -> dict[str, Any]:
    by_label = {str(sample["label"]): sample for sample in samples}

    def delta(a_label: str, b_label: str) -> dict[str, Any]:
        a = by_label[a_label]
        b = by_label[b_label]
        fields = [
            "accepted_actions",
            "queue_supported_fills",
            "strict_rescue_closes",
            "pair_pnl",
            "roi_on_filled_cost",
            "residual_cost_share",
            "residual_qty_share",
            "residual_cost_to_pair_qty",
            "current_scale_daily_pair_pnl",
        ]
        return {
            field: (
                (fnum(b.get(field)) - fnum(a.get(field)))
                if fnum(a.get(field)) is not None and fnum(b.get(field)) is not None
                else None
            )
            for field in fields
        }

    return rounded(
        {
            "old_best_to_cap25": delta("old_best_balanced", "cap25_high_roi"),
            "cap25_to_capped_cap25": delta("cap25_high_roi", "capped_cap25_density_preserving"),
            "capped_cap25_to_mature_tail_attempt": delta(
                "capped_cap25_density_preserving", "mature_tail_surplus_budget_130"
            ),
        }
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    best = load_json(args.best_scorecard)
    cap25_runtime = load_json(args.cap25_runtime_summary)
    cap25_capital = load_json(args.cap25_capital_reuse)
    capped_runtime = load_json(args.capped_runtime_summary)
    capped_capital = load_json(args.capped_capital_reuse)
    capped_capacity = load_json(args.capped_capacity_interpretation)
    mature = load_json(args.mature_tail_result)
    public_04b6 = load_json(args.public_04b6_review)

    best_metrics = body(best, "metrics")
    cap25_metrics = body(cap25_runtime, "metrics")
    capped_metrics = body(capped_runtime, "metrics")
    mature_runtime_metrics = body(mature, "runtime_metrics")
    mature_capacity_metrics = body(mature, "capacity_metrics")
    capped_capacity_metrics = body(capped_capacity, "current_cap25_metrics")
    residual_gaps = body(capped_capacity, "residual_gate_gaps")
    public_pair = body(public_04b6, "paired_5m_15m_false")

    samples = [
        sample_from_metrics(
            run_id=RUN_BEST,
            label="old_best_balanced",
            sample_kind="old_system_comparable_runtime",
            metrics=best_metrics,
            sample_minutes=60.0,
        ),
        sample_from_metrics(
            run_id=RUN_CAP25,
            label="cap25_high_roi",
            sample_kind="old_system_cap25_reproduction",
            metrics=cap25_metrics,
            sample_minutes=30.0,
            capital_card=cap25_capital,
        ),
        sample_from_metrics(
            run_id=RUN_CAPPED,
            label="capped_cap25_density_preserving",
            sample_kind="old_system_capped_cap25",
            metrics=capped_metrics,
            sample_minutes=30.0,
            capital_card=capped_capital,
            capacity_metrics=capped_capacity_metrics,
        ),
        sample_from_metrics(
            run_id=RUN_MATURE,
            label="mature_tail_surplus_budget_130",
            sample_kind="old_system_mature_tail_failed_fix",
            metrics=mature_runtime_metrics,
            sample_minutes=30.0,
            capacity_metrics=mature_capacity_metrics,
        ),
    ]

    lots = load_residual_lots(args.capped_remote_outputs)
    residual_cost = fnum(capped_metrics.get("residual_cost"), 0.0) or 0.0
    residual_qty = fnum(capped_metrics.get("residual_qty"), 0.0) or 0.0
    pair_pnl = fnum(capped_metrics.get("pair_pnl"), 0.0) or 0.0
    filled_cost = fnum(capped_metrics.get("filled_cost"), 0.0) or 0.0
    pair_qty = fnum(capped_capacity_metrics.get("pair_qty"), 0.0) or 0.0
    max_residual_cost_by_cost_share = filled_cost * 0.15
    max_residual_cost_by_pair_qty = pair_qty * 0.05
    required_reduction_by_cost_share = max(0.0, residual_cost - max_residual_cost_by_cost_share)
    required_reduction_by_pair_qty = max(0.0, residual_cost - max_residual_cost_by_pair_qty)
    required_reduction_by_zero_stress = max(0.0, residual_cost - pair_pnl)
    binding_required_reduction = max(
        required_reduction_by_cost_share, required_reduction_by_pair_qty, required_reduction_by_zero_stress
    )
    break_even_recovery = 1.0 - (pair_pnl / residual_cost) if residual_cost else None

    high_roi_samples = [sample for sample in samples if (fnum(sample.get("roi_on_filled_cost")) or 0.0) >= 0.08]
    high_roi_low_residual_samples = [
        sample
        for sample in high_roi_samples
        if (fnum(sample.get("residual_cost_share")) or 1.0) <= 0.15
        and (fnum(sample.get("residual_qty_share")) or 1.0) <= 0.20
        and (
            sample.get("residual_cost_to_pair_qty") is None
            or (fnum(sample.get("residual_cost_to_pair_qty")) or 1.0) <= 0.05
        )
    ]
    balanced_samples = [
        sample
        for sample in samples
        if (fnum(sample.get("residual_cost_share")) or 1.0) <= 0.15
        and (fnum(sample.get("residual_qty_share")) or 1.0) <= 0.20
    ]

    hard_blockers: list[str] = []
    if not lots:
        hard_blockers.append("capped_residual_lots_missing")
    if not best_metrics or not cap25_metrics or not capped_metrics or not mature_runtime_metrics:
        hard_blockers.append("old_system_sample_metrics_missing")

    status_value = (
        "KEEP_SHADOW_REVIEW_OLD_SYSTEM_RESIDUAL_FRONTIER_REPRICING_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_OLD_SYSTEM_RESIDUAL_FRONTIER_REPRICING_INPUT_BLOCKED_LOCAL_ONLY"
    )

    card = {
        "artifact": "xuan_shadow_review_old_system_residual_frontier_repricing",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_old_system_residual_frontier_repricing.py",
        "status": status_value,
        "inputs": {
            "best_scorecard": str(args.best_scorecard),
            "cap25_runtime_summary": str(args.cap25_runtime_summary),
            "cap25_capital_reuse": str(args.cap25_capital_reuse),
            "capped_runtime_summary": str(args.capped_runtime_summary),
            "capped_capital_reuse": str(args.capped_capital_reuse),
            "capped_capacity_interpretation": str(args.capped_capacity_interpretation),
            "mature_tail_result": str(args.mature_tail_result),
            "public_04b6_review": str(args.public_04b6_review),
            "capped_remote_outputs": str(args.capped_remote_outputs),
        },
        "source_statuses": {
            "best_scorecard": status(best),
            "cap25_runtime_summary": status(cap25_runtime),
            "cap25_capital_reuse": status(cap25_capital),
            "capped_runtime_summary": status(capped_runtime),
            "capped_capital_reuse": status(capped_capital),
            "capped_capacity_interpretation": status(capped_capacity),
            "mature_tail_result": status(mature),
            "public_04b6_review": status(public_04b6),
        },
        "old_system_samples": samples,
        "frontier_deltas": frontier_deltas(samples),
        "frontier_read": {
            "best_balanced_label": "old_best_balanced",
            "best_roi_label": "cap25_high_roi",
            "best_current_reviewable_label": "capped_cap25_density_preserving",
            "failed_mitigation_label": "mature_tail_surplus_budget_130",
            "high_roi_sample_count": len(high_roi_samples),
            "high_roi_low_residual_sample_count": len(high_roi_low_residual_samples),
            "balanced_low_residual_sample_count": len(balanced_samples),
            "triobjective_jointly_proven": bool(high_roi_low_residual_samples),
            "read": (
                "old-system evidence shows positive edge, but high ROI, high participation, and hard residual "
                "control are not jointly proven in the observed samples"
            ),
        },
        "capped_cap25_repricing": {
            "pair_pnl_before_residual_mark": pair_pnl,
            "residual_cost": residual_cost,
            "residual_qty": residual_qty,
            "break_even_residual_cost_recovery_rate": break_even_recovery,
            "repricing_scenarios": repricing_scenarios(pair_pnl, residual_cost, residual_qty),
            "gross_exposure_gates": {
                "max_residual_cost_share": 0.15,
                "max_residual_qty_share": 0.20,
                "max_residual_cost_to_pair_qty": 0.05,
                "max_residual_cost_by_cost_share": max_residual_cost_by_cost_share,
                "max_residual_cost_by_pair_qty": max_residual_cost_by_pair_qty,
                "required_reduction_by_cost_share": required_reduction_by_cost_share,
                "required_reduction_by_pair_qty": required_reduction_by_pair_qty,
                "required_reduction_by_zero_stress": required_reduction_by_zero_stress,
                "binding_required_reduction": binding_required_reduction,
                "required_reduction_from_prior_artifact": residual_gaps.get(
                    "required_residual_cost_reduction_before_capacity_rationale"
                ),
                "read": "repricing can change economic PnL, but gross capacity gates still require lower residual exposure",
            },
        },
        "capped_residual_lot_frontier": {
            "lot_count": len(lots),
            "total_lot_cost": sum(fnum(lot.get("cost"), 0.0) or 0.0 for lot in lots),
            "total_lot_qty": sum(fnum(lot.get("qty"), 0.0) or 0.0 for lot in lots),
            "top_lots_by_cost": sorted(lots, key=lambda item: fnum(item.get("cost"), 0.0) or 0.0, reverse=True)[:10],
            "top_slugs_by_cost": aggregate_by_slug(lots)[:10],
            "greedy_lot_cut_to_binding_gate": greedy_reduction(lots, binding_required_reduction),
            "posthoc_cut_read": (
                "two largest lots cover the binding old-system residual reduction in the capped sample, "
                "but the subsequent mature-tail runner attempt proved that simple budget tightening is not an "
                "implementable fix; it changed fill/pair dynamics and worsened residual"
            ),
        },
        "public_proxy_context": {
            "public_proxy_only_not_private_truth": True,
            "public_04b6_weighted_pair_cost": public_pair.get("weighted_pair_cost"),
            "public_04b6_residual_cost_share": public_pair.get("residual_cost_share"),
            "public_04b6_residual_qty_share": public_pair.get("residual_qty_share"),
            "read": (
                "04b6 makes the 15%/20% residual guardrail plausible as a conservative benchmark, "
                "but it cannot prove xuan private execution truth or be copied directly"
            ),
        },
        "decision": {
            "old_system_residual_frontier_ready": not hard_blockers,
            "repricing_boundary_ready": not hard_blockers,
            "use_backtest_v1_candidate_audit_pack": False,
            "positive_edge_under_old_system": True,
            "residual_metric_reassessment_required": True,
            "high_roi_low_residual_high_participation_jointly_unproven": not bool(high_roi_low_residual_samples),
            "same_single_profile_tuning_exhausted": True,
            "bounded_cap25_remote_rationale_ready": False,
            "cap75_remote_rationale_ready": False,
            "future_remote_allowed_by_this_artifact": False,
            "future_remote_requires_new_local_frontier_or_repricing_gate": True,
            "private_truth_ready": False,
            "promotion_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "hard_blockers": hard_blockers,
            "next_action": (
                "build old-system batch frontier or residual mark/recovery gate before any new remote; "
                "stop treating residual as one undifferentiated hard count"
            ),
        },
    }
    return rounded(card)


def render_markdown(card: dict[str, Any]) -> str:
    decision_card = card["decision"]
    lines = [
        "# Xuan Old-System Residual Frontier Repricing",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- old_system_residual_frontier_ready: `{decision_card['old_system_residual_frontier_ready']}`",
        f"- positive_edge_under_old_system: `{decision_card['positive_edge_under_old_system']}`",
        f"- residual_metric_reassessment_required: `{decision_card['residual_metric_reassessment_required']}`",
        f"- high_roi_low_residual_high_participation_jointly_unproven: `{decision_card['high_roi_low_residual_high_participation_jointly_unproven']}`",
        f"- future_remote_allowed_by_this_artifact: `{decision_card['future_remote_allowed_by_this_artifact']}`",
        f"- deployable/live_orders_allowed: `{decision_card['deployable']}` / `{decision_card['live_orders_allowed']}`",
        "",
        "## Old-System Samples",
        "",
        "| label | pair_pnl | ROI % | residual cost % | residual qty % | current-size pnl/day |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for sample in card["old_system_samples"]:
        lines.append(
            "| {label} | {pnl:.6f} | {roi:.2f} | {cost:.2f} | {qty:.2f} | {daily:.2f} |".format(
                label=sample["label"],
                pnl=sample.get("pair_pnl") or 0.0,
                roi=sample.get("roi_on_filled_cost_pct") or 0.0,
                cost=sample.get("residual_cost_share_pct") or 0.0,
                qty=sample.get("residual_qty_share_pct") or 0.0,
                daily=sample.get("current_scale_daily_pair_pnl") or 0.0,
            )
        )
    repricing = card["capped_cap25_repricing"]
    gates = repricing["gross_exposure_gates"]
    lines.extend(
        [
            "",
            "## Repricing Boundary",
            "",
            f"- capped cap25 pair_pnl before residual mark: `{repricing['pair_pnl_before_residual_mark']}`",
            f"- residual_cost: `{repricing['residual_cost']}`",
            f"- break_even_residual_cost_recovery_rate: `{repricing['break_even_residual_cost_recovery_rate']}`",
            f"- binding_required_reduction: `{gates['binding_required_reduction']}`",
            f"- gate read: {gates['read']}",
            "",
            "| residual cost recovery | adjusted pnl |",
            "|---:|---:|",
        ]
    )
    for scenario in repricing["repricing_scenarios"]:
        lines.append(
            "| {rec:.2f} | {pnl:.6f} |".format(
                rec=scenario["residual_cost_recovery_rate"],
                pnl=scenario["adjusted_pair_pnl_after_residual_mark"],
            )
        )
    lots = card["capped_residual_lot_frontier"]
    lines.extend(
        [
            "",
            "## Residual Lot Frontier",
            "",
            f"- lot_count: `{lots['lot_count']}`",
            f"- total_lot_cost: `{lots['total_lot_cost']}`",
            f"- greedy selected lot cost to binding gate: `{lots['greedy_lot_cut_to_binding_gate']['selected_cost']}`",
            f"- greedy covers required reduction: `{lots['greedy_lot_cut_to_binding_gate']['covers_required_reduction']}`",
            f"- posthoc read: {lots['posthoc_cut_read']}",
            "",
            "## Interpretation",
            "",
            f"- {card['frontier_read']['read']}",
            "- The residual gate should be split into bad zero-stress risk, recoverable/markable residual, and gross capacity exposure.",
            "- No cap75/150/300 and no new remote is justified by this local artifact.",
            "- Historical no-order evidence remains research-only and not private truth.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--best-scorecard", type=Path, default=DEFAULT_BEST)
    parser.add_argument("--cap25-runtime-summary", type=Path, default=DEFAULT_CAP25_RUNTIME)
    parser.add_argument("--cap25-capital-reuse", type=Path, default=DEFAULT_CAP25_CAPITAL)
    parser.add_argument("--capped-runtime-summary", type=Path, default=DEFAULT_CAPPED_RUNTIME)
    parser.add_argument("--capped-capital-reuse", type=Path, default=DEFAULT_CAPPED_CAPITAL)
    parser.add_argument("--capped-capacity-interpretation", type=Path, default=DEFAULT_CAPPED_CAPACITY)
    parser.add_argument("--mature-tail-result", type=Path, default=DEFAULT_MATURE_RESULT)
    parser.add_argument("--public-04b6-review", type=Path, default=DEFAULT_PUBLIC_04B6)
    parser.add_argument("--capped-remote-outputs", type=Path, default=DEFAULT_CAPPED_REMOTE_OUTPUTS)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(card) + "\n", encoding="utf-8")

    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
