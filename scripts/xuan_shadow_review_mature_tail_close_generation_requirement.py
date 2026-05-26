#!/usr/bin/env python3
"""Local close-generation requirement for the mature-tail cap25 failure.

This artifact converts the mature-tail failure into explicit local
requirements for the next profile family. It does not authorize remote runs;
it only decides whether an existing runner-supported blocking/rescue route is
enough, or whether a new residual-aware close generation design is required.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_FAILURE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_failure_diagnosis_20260526T2148Z.json")
DEFAULT_COUNTERFACTUAL = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_counterfactual_redesign_20260526T2148Z.json")
DEFAULT_RESCUE_HYPOTHESIS = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_rescue_increase_hypothesis_20260526T2148Z.json"
)
DEFAULT_PUBLIC_04B6 = Path(".tmp_xuan/scorecards/xuan_public_04b6_current_proxy_review_20260526T1802Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_close_generation_requirement_20260526T2148Z.json")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def iter_events(root: Path) -> Any:
    for path in sorted(root.glob("*.events.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def pair_completion_block_summary(root: Path) -> dict[str, Any]:
    rows = [
        event
        for event in iter_events(root)
        if event.get("kind") == "pair_completion_block" and event.get("block_reason") == "pair_completion_net_cap"
    ]
    pnl_after_pass = [
        event for event in rows if fnum(event.get("pair_completion_projected_pair_pnl_after"), -99.0) >= 0.01
    ]
    pnl_after_02_pass = [
        event for event in rows if fnum(event.get("pair_completion_projected_pair_pnl_after"), -99.0) >= 0.02
    ]
    by_slug: dict[str, int] = {}
    for event in rows:
        slug = str(event.get("slug") or "")
        by_slug[slug] = by_slug.get(slug, 0) + 1
    top = sorted(
        rows,
        key=lambda event: fnum(event.get("pair_completion_projected_pair_pnl_after"), -99.0),
        reverse=True,
    )[:5]
    return {
        "pair_completion_net_cap_block_count": len(rows),
        "projected_pair_pnl_after_ge_001_count": len(pnl_after_pass),
        "projected_pair_pnl_after_ge_002_count": len(pnl_after_02_pass),
        "blocked_slugs": by_slug,
        "best_blocked_examples": [
            {
                "slug": event.get("slug"),
                "side": event.get("side"),
                "price": fnum(event.get("price")),
                "qty": fnum(event.get("qty")),
                "pair_completion_qty": fnum(event.get("pair_completion_qty")),
                "pair_completion_worst_net_pair_cost": fnum(event.get("pair_completion_worst_net_pair_cost")),
                "pair_completion_pair_pnl_delta": fnum(event.get("pair_completion_pair_pnl_delta")),
                "pair_completion_projected_pair_pnl_after": fnum(event.get("pair_completion_projected_pair_pnl_after")),
            }
            for event in top
        ],
    }


def minimal_close_sets(lots: list[dict[str, Any]], need_cost: float, need_qty: float) -> list[dict[str, Any]]:
    best: list[tuple[int, float, float, tuple[dict[str, Any], ...]]] = []
    for size in range(1, len(lots) + 1):
        for combo in itertools.combinations(lots, size):
            cost = sum(fnum(lot.get("cost")) for lot in combo)
            qty = sum(fnum(lot.get("qty")) for lot in combo)
            if cost >= need_cost - 1e-12 and qty >= need_qty - 1e-12:
                best.append((size, qty, cost, combo))
        if best:
            break
    best.sort(key=lambda row: (row[0], row[1], row[2]))
    out = []
    for size, qty, cost, combo in best[:5]:
        out.append(
            {
                "lot_count": size,
                "close_qty": qty,
                "close_cost": cost,
                "lots": [
                    {
                        "slug": lot.get("slug"),
                        "quote_intent_id": lot.get("quote_intent_id"),
                        "side": lot.get("side"),
                        "qty": fnum(lot.get("qty")),
                        "px": fnum(lot.get("px")),
                        "cost": fnum(lot.get("cost")),
                    }
                    for lot in combo
                ],
            }
        )
    return out


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    r = card["required_close_generation"]
    lines = [
        "# Xuan Mature Tail Close-Generation Requirement",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- close_generation_requirement_ready: `{d['close_generation_requirement_ready']}`",
        f"- runner_supported_profile_ready: `{d['runner_supported_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_requirement: `{d['future_remote_allowed_by_this_requirement']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Requirements",
        "",
        f"- additional strict rescues required: `{r['additional_strict_rescue_closes_required']}`",
        f"- residual cost reduction required: `{r['residual_cost_reduction_required']}`",
        f"- residual qty reduction required: `{r['residual_qty_reduction_required']}`",
        f"- minimum residual lots to close or pair-complete: `{r['minimum_residual_lots_to_close_or_pair_complete']}`",
        "",
        "## Existing Routes",
        "",
        f"- blocking route viable: `{card['existing_routes']['blocking_route_viable']}`",
        f"- age rescue route viable: `{card['existing_routes']['age_rescue_route_viable']}`",
        f"- pair-completion taker route viable: `{card['existing_routes']['pair_completion_taker_route_viable']}`",
        "",
        "## Interpretation",
        "",
        f"- failure_mode: `{card['interpretation']['failure_mode']}`",
        f"- next_local_target: `{card['interpretation']['next_local_target']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Does not authorize another remote run.",
        "- Does not authorize cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    runtime = load_json(args.runtime_summary_scorecard)
    density = load_json(args.density_preflight_scorecard)
    failure = load_json(args.failure_diagnosis_scorecard)
    counterfactual = load_json(args.counterfactual_redesign_scorecard)
    rescue_hypothesis = load_json(args.rescue_increase_hypothesis_scorecard)
    public_04b6 = load_json(args.public_04b6_scorecard) if Path(args.public_04b6_scorecard).exists() else {}

    metrics = runtime.get("metrics", {})
    thresholds = density.get("thresholds", {})
    required = failure.get("required_reductions", {})
    lots = failure.get("residual_lots", {}).get("top_lots", [])
    need_cost = fnum(required.get("binding_residual_cost_reduction"))
    need_qty = fnum(required.get("residual_qty_reduction_for_qty_share"))
    close_sets = minimal_close_sets(lots, need_cost, need_qty)
    min_close_lots = close_sets[0]["lot_count"] if close_sets else None
    pair_blocks = pair_completion_block_summary(root)

    strict_rescue_closes = fnum(metrics.get("strict_rescue_closes"))
    min_rescue = fnum(thresholds.get("min_rescue_closes"))
    fills = fnum(metrics.get("queue_supported_fills"))
    min_fills = fnum(thresholds.get("min_fills"))
    accepted = fnum(metrics.get("accepted_actions"))
    min_accepted = fnum(thresholds.get("min_accepted_actions"))
    rescue_shortfall = max(0.0, min_rescue - strict_rescue_closes)
    fill_shortfall = max(0.0, min_fills - fills)

    blocking_route_viable = counterfactual.get("decision", {}).get("existing_runner_blocking_profile_ready") is True
    age_rescue_route_viable = rescue_hypothesis.get("decision", {}).get("rescue_increase_profile_ready") is True
    pair_completion_taker_route_viable = pair_blocks["projected_pair_pnl_after_ge_001_count"] > 0
    runner_supported_profile_ready = blocking_route_viable or age_rescue_route_viable or pair_completion_taker_route_viable

    hard_blockers = []
    if rescue_shortfall > 0:
        hard_blockers.append("strict_rescue_density_requires_more_closes")
    if fill_shortfall > 0:
        hard_blockers.append("fill_density_already_below_min_so_blocking_cannot_be_primary_fix")
    if not blocking_route_viable:
        hard_blockers.append("existing_blocking_controls_not_viable")
    if not age_rescue_route_viable:
        hard_blockers.append("existing_age_rescue_knob_not_viable")
    if not pair_completion_taker_route_viable:
        hard_blockers.append("existing_pair_completion_net_cap_blocks_have_no_positive_projected_pnl_after")
    if min_close_lots is None:
        hard_blockers.append("residual_lot_close_set_not_found")

    public_pair = public_04b6.get("pair_proxy", {})
    card = {
        "artifact": "xuan_shadow_review_mature_tail_close_generation_requirement",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_mature_tail_close_generation_requirement.py",
        "status": "KEEP_SHADOW_REVIEW_MATURE_TAIL_CLOSE_GENERATION_REQUIREMENT_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "failure_diagnosis_scorecard": str(Path(args.failure_diagnosis_scorecard).expanduser().resolve()),
            "counterfactual_redesign_scorecard": str(Path(args.counterfactual_redesign_scorecard).expanduser().resolve()),
            "rescue_increase_hypothesis_scorecard": str(Path(args.rescue_increase_hypothesis_scorecard).expanduser().resolve()),
            "public_04b6_scorecard": str(Path(args.public_04b6_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "runtime": runtime.get("status"),
            "density": density.get("status"),
            "failure_diagnosis": failure.get("status"),
            "counterfactual_redesign": counterfactual.get("status"),
            "rescue_increase_hypothesis": rescue_hypothesis.get("status"),
            "public_04b6": public_04b6.get("status"),
        },
        "observed": {
            "accepted_actions": accepted,
            "queue_supported_fills": fills,
            "strict_rescue_closes": strict_rescue_closes,
            "residual_qty": fnum(metrics.get("residual_qty")),
            "residual_cost": fnum(metrics.get("residual_cost")),
            "residual_qty_share": fnum(metrics.get("residual_qty_share")),
            "residual_cost_share": fnum(metrics.get("residual_cost_share")),
            "pair_pnl": fnum(metrics.get("pair_pnl")),
        },
        "thresholds": {
            "min_accepted_actions": min_accepted,
            "min_fills": min_fills,
            "min_rescue_closes": min_rescue,
            "max_residual_qty_share": fnum(thresholds.get("max_residual_qty_share")),
            "max_residual_cost_share": fnum(thresholds.get("max_residual_cost_share")),
        },
        "required_close_generation": {
            "additional_strict_rescue_closes_required": rescue_shortfall,
            "additional_fills_required_for_density": fill_shortfall,
            "residual_cost_reduction_required": need_cost,
            "residual_qty_reduction_required": need_qty,
            "minimum_residual_lots_to_close_or_pair_complete": min_close_lots,
            "minimum_close_sets": close_sets,
        },
        "existing_routes": {
            "blocking_route_viable": blocking_route_viable,
            "age_rescue_route_viable": age_rescue_route_viable,
            "pair_completion_taker_route_viable": pair_completion_taker_route_viable,
            "pair_completion_net_cap_blocks": pair_blocks,
            "age_rescue_proxy": rescue_hypothesis.get("age_relaxation_proxy"),
            "counterfactual_best_by_residual": counterfactual.get("counterfactual", {}).get("best_by_residual"),
        },
        "public_benchmark_context": {
            "public_04b6_weighted_pair_cost": public_pair.get("weighted_pair_cost"),
            "public_04b6_residual_qty_share": public_pair.get("residual_qty_share"),
            "public_04b6_residual_cost_share": public_pair.get("residual_cost_share"),
            "interpretation": "auxiliary calibration only; not a hard gate or private truth",
        },
        "interpretation": {
            "failure_mode": (
                "mature-tail surplus budget reduced accepted density without generating enough economic closes; "
                "the current window needs close generation, not more blocking"
            ),
            "local_conclusion": (
                "existing runner-supported blocking, age-rescue, and taker pair-completion routes do not jointly clear "
                "density and residual requirements"
            ),
            "next_local_target": (
                "design a residual-aware maker completion or close-generation controller, then simulate locally before any "
                "new cap25 PM_DRY_RUN"
            ),
        },
        "decision": {
            "close_generation_requirement_ready": True,
            "runner_supported_profile_ready": runner_supported_profile_ready,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_requirement": False,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "next_action": "local_residual_aware_maker_completion_design_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--runtime-summary-scorecard", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--failure-diagnosis-scorecard", type=Path, default=DEFAULT_FAILURE)
    parser.add_argument("--counterfactual-redesign-scorecard", type=Path, default=DEFAULT_COUNTERFACTUAL)
    parser.add_argument("--rescue-increase-hypothesis-scorecard", type=Path, default=DEFAULT_RESCUE_HYPOTHESIS)
    parser.add_argument("--public-04b6-scorecard", type=Path, default=DEFAULT_PUBLIC_04B6)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=None)
    args = parser.parse_args()
    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
