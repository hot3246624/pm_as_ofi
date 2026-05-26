#!/usr/bin/env python3
"""Explain cap25 residual-tail gaps after bridge-aware shadow review.

This local-only scorer turns residual capacity blockers into concrete gaps. It
does not approve deploy, live orders, remote runs, or strategy relaxation.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def status(card: dict[str, Any]) -> str | None:
    raw = card.get("status")
    return str(raw) if raw else None


def is_keep(card: dict[str, Any]) -> bool:
    return (status(card) or "").startswith("KEEP")


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def gate_thresholds(stage_gate: dict[str, Any], public_benchmark: dict[str, Any]) -> dict[str, float]:
    thresholds = {
        "residual_qty_share_hard": 0.2,
        "residual_qty_share_target": 0.15,
        "residual_cost_share_hard": 0.15,
        "residual_cost_to_pair_qty_hard": 0.05,
        "public_residual_rate_hard": 0.2,
        "public_residual_rate_target": 0.15,
    }
    for gate in listify(stage_gate.get("hard_gates")) + listify(stage_gate.get("soft_review_gates")):
        if not isinstance(gate, dict):
            continue
        name = str(gate.get("name") or "")
        threshold = fnum(gate.get("threshold"))
        if threshold is None:
            continue
        if name == "max_residual_qty_share":
            thresholds["residual_qty_share_hard"] = threshold
        elif name == "max_residual_cost_share":
            thresholds["residual_cost_share_hard"] = threshold
        elif name == "max_residual_cost_to_pair_qty":
            thresholds["residual_cost_to_pair_qty_hard"] = threshold
        elif name == "public_review_residual_rate_hard":
            thresholds["public_residual_rate_hard"] = threshold
        elif name == "public_target_residual_rate":
            thresholds["public_residual_rate_target"] = threshold
            thresholds["residual_qty_share_target"] = threshold
    public_targets = body(public_benchmark, "public_review_targets")
    hard_public = fnum(public_targets.get("hard_residual_rate_lte"))
    target_public = fnum(public_targets.get("target_residual_rate_lte"))
    if hard_public is not None:
        thresholds["public_residual_rate_hard"] = hard_public
    if target_public is not None:
        thresholds["public_residual_rate_target"] = target_public
        thresholds["residual_qty_share_target"] = target_public
    return thresholds


def share_gap(actual: float | None, threshold: float | None, denominator: float | None) -> dict[str, Any]:
    if actual is None or threshold is None:
        return {
            "actual": actual,
            "threshold": threshold,
            "passed": False,
            "excess_share": None,
            "max_allowed_value": None,
            "required_reduction": None,
            "required_reduction_share_of_current": None,
        }
    passed = actual <= threshold
    max_allowed = denominator * threshold if denominator is not None else None
    reduction = None
    reduction_share = None
    if max_allowed is not None:
        current_value = denominator * actual
        reduction = max(0.0, current_value - max_allowed)
        reduction_share = reduction / current_value if current_value > 0 else 0.0
    return {
        "actual": actual,
        "threshold": threshold,
        "passed": passed,
        "excess_share": max(0.0, actual - threshold),
        "max_allowed_value": max_allowed,
        "required_reduction": reduction,
        "required_reduction_share_of_current": reduction_share,
    }


def residual_lot_summary(young_tiny: dict[str, Any]) -> dict[str, Any]:
    summary = body(young_tiny, "residual_summary")
    class_cost = body(summary, "class_cost")
    class_qty = body(summary, "class_qty")
    total_cost = sum(fnum(value, 0.0) or 0.0 for value in class_cost.values())
    total_qty = sum(fnum(value, 0.0) or 0.0 for value in class_qty.values())
    mature_material_cost = fnum(class_cost.get("mature_material"), 0.0) or 0.0
    mature_material_qty = fnum(class_qty.get("mature_material"), 0.0) or 0.0
    top_lots = [lot for lot in listify(summary.get("top_lots")) if isinstance(lot, dict)]
    top_two_cost = sum(fnum(lot.get("cost"), 0.0) or 0.0 for lot in top_lots[:2])
    slug_cost: defaultdict[str, float] = defaultdict(float)
    slug_qty: defaultdict[str, float] = defaultdict(float)
    for lot in top_lots:
        slug = str(lot.get("slug") or "unknown")
        slug_cost[slug] += fnum(lot.get("cost"), 0.0) or 0.0
        slug_qty[slug] += fnum(lot.get("qty"), 0.0) or 0.0
    top_slug_cost = sorted(slug_cost.items(), key=lambda item: (-item[1], item[0]))
    top_slug_qty = sorted(slug_qty.items(), key=lambda item: (-item[1], item[0]))
    return {
        "lot_count": summary.get("lot_count"),
        "class_counts": body(summary, "class_counts"),
        "class_cost": class_cost,
        "class_qty": class_qty,
        "total_residual_cost_from_lots": total_cost,
        "total_residual_qty_from_lots": total_qty,
        "mature_material_cost": mature_material_cost,
        "mature_material_qty": mature_material_qty,
        "mature_material_cost_share": mature_material_cost / total_cost if total_cost > 0 else None,
        "mature_material_qty_share": mature_material_qty / total_qty if total_qty > 0 else None,
        "top_two_lot_cost": top_two_cost,
        "top_two_lot_cost_share": top_two_cost / total_cost if total_cost > 0 else None,
        "top_slug_by_cost": [
            {"slug": slug, "cost": cost, "cost_share": cost / total_cost if total_cost > 0 else None}
            for slug, cost in top_slug_cost[:5]
        ],
        "top_slug_by_qty": [
            {"slug": slug, "qty": qty, "qty_share": qty / total_qty if total_qty > 0 else None}
            for slug, qty in top_slug_qty[:5]
        ],
        "top_lots": top_lots[:10],
    }


def build_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    current = card["current_stage_metrics"]
    gaps = card["residual_gaps"]
    lots = card["residual_tail_diagnosis"]
    lines = [
        "# Xuan Shadow Review Residual Tail Gap",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- residual_tail_gap_plan_ready: `{decision['residual_tail_gap_plan_ready']}`",
        f"- residual_tail_primary_blocker: `{decision['residual_tail_primary_blocker']}`",
        f"- cap75_remote_rationale_ready: `{decision['cap75_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_plan: `{decision['future_remote_allowed_by_this_plan']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        f"- capacity_expansion_blockers: `{', '.join(decision['capacity_expansion_blockers']) or 'none'}`",
        "",
        "## Current Cap25 Metrics",
        "",
        f"- accepted/fills/rescues: `{current['accepted_actions']}` / `{current['queue_supported_fills']}` / `{current['strict_rescue_closes']}`",
        f"- pair PnL: `{current['pair_pnl']}`",
        f"- residual qty/cost: `{current['residual_qty']}` / `{current['residual_cost']}`",
        f"- residual qty/cost share: `{current['residual_qty_share']}` / `{current['residual_cost_share']}`",
        f"- residual cost to pair qty: `{current['residual_cost_to_pair_qty']}`",
        f"- worst residual-zero PnL: `{current['worst_case_pair_pnl_if_residual_zero']}`",
        "",
        "## Gap To Pass",
        "",
        "| Gate | Current | Target | Required reduction | Reduction share |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name in [
        "residual_qty_share_hard",
        "residual_qty_share_target",
        "residual_cost_share_hard",
        "residual_cost_to_pair_qty_hard",
        "public_residual_rate_hard",
        "public_residual_rate_target",
    ]:
        gap = gaps[name]
        lines.append(
            "| "
            + name
            + f" | `{gap['actual']}` | `{gap['threshold']}` | `{gap['required_reduction']}` | `{gap['required_reduction_share_of_current']}` |"
        )
    lines.extend(
        [
            "",
            "## Tail Diagnosis",
            "",
            f"- mature material cost/share: `{lots['mature_material_cost']}` / `{lots['mature_material_cost_share']}`",
            f"- mature material qty/share: `{lots['mature_material_qty']}` / `{lots['mature_material_qty_share']}`",
            f"- top two residual lots cost/share: `{lots['top_two_lot_cost']}` / `{lots['top_two_lot_cost_share']}`",
            "",
            "Top slugs by residual cost:",
            "",
        ]
    )
    for row in lots["top_slug_by_cost"]:
        lines.append(f"- `{row['slug']}` cost `{row['cost']}` share `{row['cost_share']}`")
    lines.extend(
        [
            "",
            "## Required Before Future Remote",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in decision["required_before_future_remote"])
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- This is a local scorecard, not remote authorization.",
            "- Do not expand to cap75 while current cap25 residual gates are blocked.",
            "- Do not widen surplus caps or re-enable closeability-cancel starvation to hide residuals.",
            "- Do not deploy, restart, send live orders, or mutate shared services.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    capacity_plan = load_json(args.capacity_ladder_evidence_plan_scorecard)
    stage_gate = load_json(args.current_stage_public_benchmark_gate_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    young_tiny = load_json(args.young_tiny_residual_scorecard)
    capital = load_json(args.capital_roi_scorecard)
    public = load_json(args.public_benchmark_comparison_scorecard)
    surplus = load_json(args.surplus_bridge_scorecard)

    runtime_metrics = body(runtime, "metrics")
    stage_decision = body(stage_gate, "decision")
    stage_metrics = body(stage_gate, "metrics")
    capital_aggregate = body(capital, "aggregate")
    capital_totals = body(capital_aggregate, "totals")
    capital_round = body(capital_aggregate, "round_roi")
    public_comparison = body(public, "comparison")
    public_vs_b55 = body(public_comparison, "vs_b55")
    surplus_aggregate = body(surplus, "aggregate")
    thresholds = gate_thresholds(stage_gate, public)

    filled_qty = fnum(runtime_metrics.get("filled_qty"), fnum(capital_totals.get("filled_qty")))
    filled_cost = fnum(runtime_metrics.get("filled_cost"), fnum(capital_totals.get("filled_cost")))
    pair_qty = fnum(capital_totals.get("pair_qty"), fnum(public_comparison.get("pair_qty")))
    residual_qty = fnum(runtime_metrics.get("residual_qty"), fnum(capital_totals.get("residual_qty")))
    residual_cost = fnum(runtime_metrics.get("residual_cost"), fnum(capital_totals.get("residual_cost")))
    residual_qty_share = fnum(runtime_metrics.get("residual_qty_share"), fnum(stage_metrics.get("residual_qty_share")))
    residual_cost_share = fnum(runtime_metrics.get("residual_cost_share"), fnum(stage_metrics.get("residual_cost_share")))
    residual_cost_to_pair_qty = fnum(
        capital_round.get("residual_cost_to_pair_qty"), fnum(stage_metrics.get("residual_cost_to_pair_qty"))
    )

    residual_gaps = {
        "residual_qty_share_hard": share_gap(
            residual_qty_share, thresholds["residual_qty_share_hard"], filled_qty
        ),
        "residual_qty_share_target": share_gap(
            residual_qty_share, thresholds["residual_qty_share_target"], filled_qty
        ),
        "residual_cost_share_hard": share_gap(
            residual_cost_share, thresholds["residual_cost_share_hard"], filled_cost
        ),
        "residual_cost_to_pair_qty_hard": share_gap(
            residual_cost_to_pair_qty, thresholds["residual_cost_to_pair_qty_hard"], pair_qty
        ),
        "public_residual_rate_hard": share_gap(
            residual_qty_share, thresholds["public_residual_rate_hard"], filled_qty
        ),
        "public_residual_rate_target": share_gap(
            residual_qty_share, thresholds["public_residual_rate_target"], filled_qty
        ),
    }

    stage_blockers = [str(item) for item in listify(stage_decision.get("hard_blockers")) if item]
    residual_blockers = [
        name
        for name in stage_blockers
        if name
        in {
            "max_residual_cost_share",
            "max_residual_qty_share",
            "max_residual_cost_to_pair_qty",
            "public_review_residual_rate_hard",
            "public_target_residual_rate",
        }
    ]
    lot_summary = residual_lot_summary(young_tiny)
    mature_material_share = fnum(lot_summary.get("mature_material_cost_share"), 0.0) or 0.0
    primary_blocker = (
        "mature_material_residual_tail"
        if mature_material_share >= args.mature_material_dominance_threshold
        else "mixed_residual_tail"
    )
    hardest_gap_name = max(
        residual_gaps,
        key=lambda name: fnum(residual_gaps[name].get("required_reduction_share_of_current"), -1.0) or -1.0,
    )

    essential_inputs_ready = bool(runtime_metrics) and bool(stage_metrics) and bool(lot_summary)
    current_stage_gate_pass = stage_decision.get("capacity_stage_gate_pass") is True
    cap75_rationale_ready = (
        current_stage_gate_pass
        and not residual_blockers
        and is_keep(surplus)
        and fnum(surplus_aggregate.get("residual_qty_share"), 1.0) <= thresholds["residual_qty_share_hard"]
        and fnum(surplus_aggregate.get("residual_cost_share"), 1.0) <= thresholds["residual_cost_share_hard"]
    )
    status_value = (
        "KEEP_SHADOW_REVIEW_RESIDUAL_TAIL_GAP_PLAN_READY_LOCAL_ONLY"
        if essential_inputs_ready
        else "UNKNOWN_SHADOW_REVIEW_RESIDUAL_TAIL_GAP_INPUT_BLOCKED_LOCAL_ONLY"
    )

    required_before_future_remote = [
        "build a residual-zero stress capacity plan before any cap expansion",
        "define a local residual-tail mitigation hypothesis with measurable residual reduction targets",
        "prove current cap25 residual/public residual gates are resolved or explicitly bounded",
        "keep strict_rescue_surplus_net_cap <= 1.02 unless a separate local scorer proves safety",
        "do not re-enable closeability-cancel starvation as a residual workaround",
        "manifest builder and verifier must KEEP on the future heartbeat",
        "future run, if separately justified, must be one bounded PM_DRY_RUN no-order run with duration_s<=1800",
    ]

    return rounded(
        {
            "artifact": "xuan_shadow_review_residual_tail_gap_scorer",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_residual_tail_gap_scorer.py",
            "status": status_value,
            "inputs": {
                "capacity_ladder_evidence_plan_scorecard": args.capacity_ladder_evidence_plan_scorecard,
                "current_stage_public_benchmark_gate_scorecard": args.current_stage_public_benchmark_gate_scorecard,
                "runtime_summary_scorecard": args.runtime_summary_scorecard,
                "young_tiny_residual_scorecard": args.young_tiny_residual_scorecard,
                "capital_roi_scorecard": args.capital_roi_scorecard,
                "public_benchmark_comparison_scorecard": args.public_benchmark_comparison_scorecard,
                "surplus_bridge_scorecard": args.surplus_bridge_scorecard,
            },
            "source_statuses": {
                "capacity_ladder_evidence_plan": status(capacity_plan),
                "current_stage_public_benchmark_gate": status(stage_gate),
                "runtime_summary": status(runtime),
                "young_tiny_residual": status(young_tiny),
                "capital_roi": status(capital),
                "public_benchmark_comparison": status(public),
                "surplus_bridge": status(surplus),
            },
            "decision": {
                "residual_tail_gap_plan_ready": status_value.startswith("KEEP"),
                "research_shadow_review_ready": True,
                "paper_shadow_only": True,
                "research_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "remote_runner_allowed": False,
                "future_remote_allowed_by_this_plan": False,
                "cap75_remote_rationale_ready": cap75_rationale_ready,
                "current_stage_gate_pass": current_stage_gate_pass,
                "capacity_expansion_blockers": residual_blockers,
                "residual_tail_primary_blocker": primary_blocker,
                "hardest_residual_gap": hardest_gap_name,
                "hard_blockers": [] if essential_inputs_ready else ["residual_tail_gap_inputs_missing"],
                "required_before_future_remote": required_before_future_remote,
                "next_action": "build_residual_zero_stress_capacity_plan_or_local_tail_mitigation_hypothesis",
            },
            "current_stage_metrics": {
                "accepted_actions": runtime_metrics.get("accepted_actions"),
                "queue_supported_fills": runtime_metrics.get("queue_supported_fills"),
                "strict_rescue_closes": runtime_metrics.get("strict_rescue_closes"),
                "pair_pnl": runtime_metrics.get("pair_pnl"),
                "filled_qty": filled_qty,
                "filled_cost": filled_cost,
                "pair_qty": pair_qty,
                "residual_qty": residual_qty,
                "residual_cost": residual_cost,
                "residual_qty_share": residual_qty_share,
                "residual_cost_share": residual_cost_share,
                "residual_cost_to_pair_qty": residual_cost_to_pair_qty,
                "worst_case_pair_pnl_if_residual_zero": capital_round.get(
                    "worst_case_pair_pnl_if_residual_zero"
                ),
                "residual_qty_share_delta_vs_b55": public_vs_b55.get("residual_qty_share_delta_vs_b55"),
                "source_blocks": runtime_metrics.get("strict_rescue_source_blocks"),
                "rescue_l1_age_ms_max": runtime_metrics.get("rescue_l1_age_ms_max"),
            },
            "thresholds": thresholds,
            "residual_gaps": residual_gaps,
            "residual_tail_diagnosis": lot_summary,
            "bridge_context": {
                "surplus_bridge_status": status(surplus),
                "bridge_residual_qty_share": surplus_aggregate.get("residual_qty_share"),
                "bridge_residual_cost_share": surplus_aggregate.get("residual_cost_share"),
                "bridge_pair_pnl": surplus_aggregate.get("pair_pnl"),
                "bridge_eligible_window_count": surplus_aggregate.get("bridge_eligible_window_count"),
                "bridge_clears_review_surface_but_not_cap25_capacity_expansion": is_keep(surplus)
                and bool(residual_blockers),
            },
            "interpretation": {
                "residual_tail_is_not_young_or_tiny_noise": primary_blocker == "mature_material_residual_tail",
                "current_stage_residual_blocked": bool(residual_blockers),
                "economics_source_safety_otherwise_clean": not [
                    blocker
                    for blocker in stage_blockers
                    if blocker not in set(residual_blockers)
                ],
                "do_not_expand_capacity_while_residual_blocked": True,
                "do_not_relax_economics_or_source_gates": True,
                "do_not_run_remote_from_this_plan": True,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capacity-ladder-evidence-plan-scorecard", required=True)
    parser.add_argument("--current-stage-public-benchmark-gate-scorecard", required=True)
    parser.add_argument("--runtime-summary-scorecard", required=True)
    parser.add_argument("--young-tiny-residual-scorecard", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--public-benchmark-comparison-scorecard", required=True)
    parser.add_argument("--surplus-bridge-scorecard", required=True)
    parser.add_argument("--mature-material-dominance-threshold", type=float, default=0.75)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    scorecard = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(build_markdown(scorecard) + "\n")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if scorecard["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
