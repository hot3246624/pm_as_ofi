#!/usr/bin/env python3
"""Build an old-system residual mark/recovery gate.

This local-only gate consumes the old-system residual frontier/repricing
artifact and turns the residual repricing discussion into explicit pass/fail
tiers:

1. economic break-even repricing,
2. research mark/recovery review,
3. gross capacity exposure,
4. private truth / live promotion.

It intentionally does not use the newer backtest V1 candidate audit pack.
It does not authorize remote runs, capacity expansion, deployment, live orders,
or private-truth claims.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0305Z"
DEFAULT_FRONTIER = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_old_system_residual_frontier_repricing_20260527T0245Z.json"
)
DEFAULT_PRIVATE_TRUTH = Path(".tmp_xuan/scorecards/xuan_shadow_review_private_truth_request_20260526T0824Z.json")
DEFAULT_SCORECARD = Path(f".tmp_xuan/scorecards/xuan_shadow_review_old_system_residual_mark_recovery_gate_{STAMP}.json")
DEFAULT_MARKDOWN = Path(
    f".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_old_system_residual_mark_recovery_gate_{STAMP}/"
    "OLD_SYSTEM_RESIDUAL_MARK_RECOVERY_GATE.md"
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


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def scenario_at_or_above(scenarios: list[Any], recovery: float) -> dict[str, Any]:
    best: dict[str, Any] | None = None
    for item in scenarios:
        if not isinstance(item, dict):
            continue
        rate = fnum(item.get("residual_cost_recovery_rate"))
        if rate is None:
            continue
        if rate >= recovery and (best is None or rate < fnum(best.get("residual_cost_recovery_rate"), 99.0)):
            best = item
    return best or {}


def classify_layers(frontier: dict[str, Any], private_truth: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    repricing = body(frontier, "capped_cap25_repricing")
    gates = body(repricing, "gross_exposure_gates")
    lots = body(frontier, "capped_residual_lot_frontier")
    greedy = body(lots, "greedy_lot_cut_to_binding_gate")
    scenarios = listify(repricing.get("repricing_scenarios"))
    frontier_read = body(frontier, "frontier_read")
    frontier_decision = body(frontier, "decision")
    private_decision = body(private_truth, "decision")

    pair_pnl = fnum(repricing.get("pair_pnl_before_residual_mark"), 0.0) or 0.0
    residual_cost = fnum(repricing.get("residual_cost"), 0.0) or 0.0
    residual_qty = fnum(repricing.get("residual_qty"), 0.0) or 0.0
    break_even = fnum(repricing.get("break_even_residual_cost_recovery_rate"), 1.0) or 1.0
    review_recovery = max(args.min_research_review_recovery_rate, break_even + args.min_recovery_margin)
    capacity_reduction = fnum(gates.get("binding_required_reduction"), 0.0) or 0.0
    zero_stress_reduction = fnum(gates.get("required_reduction_by_zero_stress"), 0.0) or 0.0
    required_recovery_value = residual_cost * review_recovery
    break_even_value = residual_cost * break_even
    review_scenario = scenario_at_or_above(scenarios, review_recovery)
    break_even_scenario = scenario_at_or_above(scenarios, break_even)

    # Current old-system evidence is a repricing boundary, not observed recovery.
    observed_recovery_rate = None
    observed_recovery_value = None
    observed_adjusted_pnl = None
    observed_recovery_evidence_ready = False

    economic_break_even_pass = (
        observed_recovery_rate is not None and observed_recovery_rate >= break_even
    )
    research_recovery_pass = (
        observed_recovery_rate is not None and observed_recovery_rate >= review_recovery
    )
    gross_capacity_pass = False
    if observed_recovery_value is not None:
        gross_capacity_pass = observed_recovery_value >= capacity_reduction
    private_truth_ready = private_decision.get("private_truth_ready") is True

    top_lots = listify(lots.get("top_lots_by_cost"))
    top_slugs = listify(lots.get("top_slugs_by_cost"))
    selected_lots = listify(greedy.get("selected_lots"))
    selected_cost = fnum(greedy.get("selected_cost"), 0.0) or 0.0

    hard_blockers: list[str] = []
    if not is_keep(frontier):
        hard_blockers.append("frontier_not_keep")
    if not top_lots:
        hard_blockers.append("residual_lot_frontier_missing")

    status_value = (
        "KEEP_SHADOW_REVIEW_OLD_SYSTEM_RESIDUAL_MARK_RECOVERY_GATE_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_OLD_SYSTEM_RESIDUAL_MARK_RECOVERY_GATE_INPUT_BLOCKED_LOCAL_ONLY"
    )

    layer_results = {
        "economic_break_even_repricing": {
            "purpose": "avoid negative PnL after residual is marked instead of zeroed",
            "required_recovery_rate": break_even,
            "required_recovery_value": break_even_value,
            "current_observed_recovery_rate": observed_recovery_rate,
            "current_observed_recovery_value": observed_recovery_value,
            "projected_adjusted_pnl_at_threshold": break_even_scenario.get(
                "adjusted_pair_pnl_after_residual_mark"
            ),
            "passed": economic_break_even_pass,
            "blocked_by": ["no_observed_lot_level_recovery_or_mark_evidence"],
        },
        "research_mark_recovery_review": {
            "purpose": "require positive cushion beyond break-even before residual is treated as recoverable research risk",
            "required_recovery_rate": review_recovery,
            "required_recovery_value": required_recovery_value,
            "current_observed_recovery_rate": observed_recovery_rate,
            "current_observed_recovery_value": observed_recovery_value,
            "projected_adjusted_pnl_at_threshold": review_scenario.get(
                "adjusted_pair_pnl_after_residual_mark"
            ),
            "passed": research_recovery_pass,
            "blocked_by": ["no_old_system_mark_recovery_evidence_gate_yet"],
        },
        "gross_capacity_exposure": {
            "purpose": "keep residual exposure small enough for capacity interpretation, independent of economic mark",
            "binding_required_reduction": capacity_reduction,
            "zero_stress_required_reduction": zero_stress_reduction,
            "selected_posthoc_lot_cost_cover": selected_cost,
            "posthoc_selected_lots_cover_required_reduction": greedy.get("covers_required_reduction") is True,
            "passed": gross_capacity_pass,
            "blocked_by": [
                "posthoc_lot_cut_is_not_runner_implementable",
                "2148Z_mature_tail_attempt_worsened_residual",
            ],
        },
        "private_truth_live_promotion": {
            "purpose": "validate owner private truth and live execution/accounting boundary",
            "private_truth_ready": private_truth_ready,
            "passed": False,
            "blocked_by": ["historical_shadow_no_order_cannot_prove_private_truth"],
        },
    }

    return rounded(
        {
            "status": status_value,
            "hard_blockers": hard_blockers,
            "layer_results": layer_results,
            "residual_risk_split": {
                "bad_zero_stress_risk": zero_stress_reduction,
                "recoverable_mark_boundary": break_even_value,
                "research_recovery_value_required": required_recovery_value,
                "gross_capacity_excess_to_remove": capacity_reduction,
                "residual_cost": residual_cost,
                "residual_qty": residual_qty,
                "pair_pnl_before_residual_mark": pair_pnl,
                "read": (
                    "economic repricing and gross capacity are separate; a residual can be mark-recoverable "
                    "but still too large for capacity expansion"
                ),
            },
            "lot_recovery_targets": {
                "top_lots_by_cost": top_lots[:6],
                "top_slugs_by_cost": top_slugs[:6],
                "posthoc_selected_lots": selected_lots,
                "posthoc_selected_lot_cost": selected_cost,
                "posthoc_selected_lot_cost_share": selected_cost / residual_cost if residual_cost else None,
                "required_next_evidence": [
                    "lot-level observable mark or actual close/merge path for the two largest residual lots",
                    "no lookahead: mark source must exist at or before the decision boundary being evaluated",
                    "source/economic gates unchanged from old runner lane",
                    "batch frontier must show recovery without starving accepted/fill/rescue density",
                ],
            },
            "frontier_context": {
                "positive_edge_under_old_system": frontier_decision.get("positive_edge_under_old_system") is True,
                "triobjective_jointly_proven": frontier_read.get("triobjective_jointly_proven") is True,
                "high_roi_sample_count": frontier_read.get("high_roi_sample_count"),
                "high_roi_low_residual_sample_count": frontier_read.get("high_roi_low_residual_sample_count"),
            },
            "decision": {
                "old_system_residual_mark_recovery_gate_ready": not hard_blockers,
                "use_backtest_v1_candidate_audit_pack": False,
                "observed_recovery_evidence_ready": observed_recovery_evidence_ready,
                "economic_break_even_repricing_pass": economic_break_even_pass,
                "research_mark_recovery_review_pass": research_recovery_pass,
                "gross_capacity_exposure_pass": gross_capacity_pass,
                "private_truth_ready": private_truth_ready,
                "residual_reclassification_allowed_for_research": research_recovery_pass,
                "residual_reclassification_allowed_for_capacity": gross_capacity_pass,
                "bounded_cap25_remote_rationale_ready": False,
                "cap75_remote_rationale_ready": False,
                "future_remote_allowed_by_this_gate": False,
                "future_remote_requires_old_system_batch_frontier": True,
                "same_profile_repeat_allowed": False,
                "promotion_ready": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "research_only": True,
                "paper_shadow_only": True,
                "next_action": (
                    "build old-system lot-level mark/recovery batch frontier; prove >= research recovery "
                    "threshold and gross capacity reduction together before any new remote rationale"
                ),
            },
        }
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    frontier = load_json(args.frontier_scorecard)
    private_truth = load_json(args.private_truth_scorecard)
    gate = classify_layers(frontier, private_truth, args)
    return rounded(
        {
            "artifact": "xuan_shadow_review_old_system_residual_mark_recovery_gate",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_old_system_residual_mark_recovery_gate.py",
            "status": gate["status"],
            "inputs": {
                "frontier_scorecard": str(args.frontier_scorecard),
                "private_truth_scorecard": str(args.private_truth_scorecard),
                "min_research_review_recovery_rate": args.min_research_review_recovery_rate,
                "min_recovery_margin": args.min_recovery_margin,
            },
            "source_statuses": {
                "frontier_scorecard": status(frontier),
                "private_truth_scorecard": status(private_truth),
            },
            "layer_results": gate["layer_results"],
            "residual_risk_split": gate["residual_risk_split"],
            "lot_recovery_targets": gate["lot_recovery_targets"],
            "frontier_context": gate["frontier_context"],
            "decision": gate["decision"],
            "hard_blockers": gate["hard_blockers"],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    risk = card["residual_risk_split"]
    lots = card["lot_recovery_targets"]
    lines = [
        "# Xuan Old-System Residual Mark Recovery Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- old_system_residual_mark_recovery_gate_ready: `{d['old_system_residual_mark_recovery_gate_ready']}`",
        f"- observed_recovery_evidence_ready: `{d['observed_recovery_evidence_ready']}`",
        f"- economic_break_even_repricing_pass: `{d['economic_break_even_repricing_pass']}`",
        f"- research_mark_recovery_review_pass: `{d['research_mark_recovery_review_pass']}`",
        f"- gross_capacity_exposure_pass: `{d['gross_capacity_exposure_pass']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        f"- deployable/live_orders_allowed: `{d['deployable']}` / `{d['live_orders_allowed']}`",
        "",
        "## Layer Results",
        "",
    ]
    for name, layer in card["layer_results"].items():
        lines.extend(
            [
                f"### {name}",
                "",
                f"- purpose: {layer['purpose']}",
                f"- passed: `{layer['passed']}`",
            ]
        )
        if "required_recovery_rate" in layer:
            lines.append(f"- required_recovery_rate: `{layer['required_recovery_rate']}`")
        if "required_recovery_value" in layer:
            lines.append(f"- required_recovery_value: `{layer['required_recovery_value']}`")
        if "binding_required_reduction" in layer:
            lines.append(f"- binding_required_reduction: `{layer['binding_required_reduction']}`")
        lines.append(f"- blocked_by: `{', '.join(layer['blocked_by']) or 'none'}`")
        lines.append("")
    lines.extend(
        [
            "## Residual Risk Split",
            "",
            f"- residual_cost: `{risk['residual_cost']}`",
            f"- pair_pnl_before_residual_mark: `{risk['pair_pnl_before_residual_mark']}`",
            f"- bad_zero_stress_risk: `{risk['bad_zero_stress_risk']}`",
            f"- recoverable_mark_boundary: `{risk['recoverable_mark_boundary']}`",
            f"- research_recovery_value_required: `{risk['research_recovery_value_required']}`",
            f"- gross_capacity_excess_to_remove: `{risk['gross_capacity_excess_to_remove']}`",
            f"- read: {risk['read']}",
            "",
            "## Lot Targets",
            "",
            f"- posthoc_selected_lot_cost: `{lots['posthoc_selected_lot_cost']}`",
            f"- posthoc_selected_lot_cost_share: `{lots['posthoc_selected_lot_cost_share']}`",
            "",
            "Required next evidence:",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in lots["required_next_evidence"])
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Uses old/current xuan no-order scorecards only; does not use backtest V1 candidate audit pack.",
            "- This gate does not authorize a remote sample, cap75/150/300, deploy, live orders, or private truth.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--frontier-scorecard", type=Path, default=DEFAULT_FRONTIER)
    parser.add_argument("--private-truth-scorecard", type=Path, default=DEFAULT_PRIVATE_TRUTH)
    parser.add_argument("--min-research-review-recovery-rate", type=float, default=0.60)
    parser.add_argument("--min-recovery-margin", type=float, default=0.05)
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
