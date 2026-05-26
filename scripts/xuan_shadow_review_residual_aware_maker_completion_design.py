#!/usr/bin/env python3
"""Local residual-aware maker-completion design for xuan cap25.

This is a design artifact, not a runnable profile. It translates the mature
tail close-generation requirement into conservative maker-completion limits and
acceptance criteria for a future local implementation pass.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_REQUIREMENT = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_close_generation_requirement_20260526T2148Z.json"
)
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_FAILURE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_failure_diagnosis_20260526T2148Z.json")
DEFAULT_PUBLIC_04B6 = Path(".tmp_xuan/scorecards/xuan_public_04b6_current_proxy_review_20260526T1802Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_residual_aware_maker_completion_design_20260526T2148Z.json")


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


def fee_per_share(px: float, rate: float) -> float:
    price = min(max(px, 0.0), 1.0)
    return rate * price * (1.0 - price)


def max_comp_px_for_net_cap(held_px: float, net_cap: float, fee_rate: float) -> float:
    lo = 0.0
    hi = 1.0
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if held_px + mid + fee_per_share(mid, fee_rate) <= net_cap + 1e-12:
            lo = mid
        else:
            hi = mid
    return lo


def completion_targets(requirement: dict[str, Any], fee_rate: float, strict_cap: float, target_cap: float) -> list[dict[str, Any]]:
    close_sets = requirement.get("required_close_generation", {}).get("minimum_close_sets", [])
    if not close_sets:
        return []
    targets = []
    for lot in close_sets[0].get("lots", []):
        held_px = fnum(lot.get("px"))
        targets.append(
            {
                "slug": lot.get("slug"),
                "quote_intent_id": lot.get("quote_intent_id"),
                "held_side": lot.get("side"),
                "close_side": "NO" if lot.get("side") == "YES" else "YES",
                "held_qty": fnum(lot.get("qty")),
                "held_px": held_px,
                "held_cost": fnum(lot.get("cost")),
                "max_comp_px_under_strict_cap": max_comp_px_for_net_cap(held_px, strict_cap, fee_rate),
                "target_comp_px_under_pair_cost_cap": max_comp_px_for_net_cap(held_px, target_cap, fee_rate),
            }
        )
    return targets


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    lines = [
        "# Xuan Residual-Aware Maker Completion Design",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- design_ready: `{d['design_ready']}`",
        f"- runner_feature_required: `{d['runner_feature_required']}`",
        f"- candidate_profile_ready: `{d['candidate_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_design: `{d['future_remote_allowed_by_this_design']}`",
        "",
        "## Target",
        "",
        f"- target residual lots: `{card['target']['target_residual_lots']}`",
        f"- target close qty/cost: `{card['target']['target_close_qty']}` / `{card['target']['target_close_cost']}`",
        f"- required additional rescues/fills: `{card['target']['additional_strict_rescue_closes_required']}` / `{card['target']['additional_fills_required_for_density']}`",
        "",
        "## Design",
        "",
        f"- controller: `{card['design']['controller']}`",
        f"- economic boundary: `{card['design']['economic_boundary']}`",
        f"- source boundary: `{card['design']['source_boundary']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- No remote run is authorized by this design.",
        "- No cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    requirement = load_json(args.requirement_scorecard)
    profile = load_json(args.profile_scorecard)
    failure = load_json(args.failure_diagnosis_scorecard)
    public_04b6 = load_json(args.public_04b6_scorecard) if Path(args.public_04b6_scorecard).exists() else {}
    candidate_profile = profile.get("candidate_profile", {})
    fee_rate = fnum(candidate_profile.get("taker_fee_rate"), 0.07)
    strict_cap = fnum(candidate_profile.get("strict_rescue_surplus_net_cap"), 1.02)
    target_cap = min(0.99, strict_cap)
    targets = completion_targets(requirement, fee_rate, strict_cap, target_cap)
    req = requirement.get("required_close_generation", {})
    required = failure.get("required_reductions", {})
    target_close_qty = sum(fnum(row.get("held_qty")) for row in targets)
    target_close_cost = sum(fnum(row.get("held_cost")) for row in targets)
    public_pair = public_04b6.get("pair_proxy", {})

    hard_blockers = [
        "runner_feature_not_implemented",
        "must_locally_simulate_maker_completion_before_remote",
        "private_truth_not_ready",
    ]
    if requirement.get("decision", {}).get("runner_supported_profile_ready") is not False:
        hard_blockers.append("requirement_runner_supported_state_unexpected")
    if not targets:
        hard_blockers.append("target_residual_lots_missing")

    card = {
        "artifact": "xuan_shadow_review_residual_aware_maker_completion_design",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_residual_aware_maker_completion_design.py",
        "status": "KEEP_SHADOW_REVIEW_RESIDUAL_AWARE_MAKER_COMPLETION_DESIGN_READY_LOCAL_ONLY",
        "inputs": {
            "requirement_scorecard": str(Path(args.requirement_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
            "failure_diagnosis_scorecard": str(Path(args.failure_diagnosis_scorecard).expanduser().resolve()),
            "public_04b6_scorecard": str(Path(args.public_04b6_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "requirement": requirement.get("status"),
            "profile": profile.get("status"),
            "failure_diagnosis": failure.get("status"),
            "public_04b6": public_04b6.get("status"),
        },
        "target": {
            "target_residual_lots": len(targets),
            "target_close_qty": target_close_qty,
            "target_close_cost": target_close_cost,
            "residual_cost_reduction_required": fnum(req.get("residual_cost_reduction_required")),
            "residual_qty_reduction_required": fnum(req.get("residual_qty_reduction_required")),
            "max_residual_cost_after": fnum(required.get("max_residual_cost_by_pair_qty")),
            "max_residual_qty_after": fnum(required.get("max_residual_qty")),
            "additional_strict_rescue_closes_required": fnum(req.get("additional_strict_rescue_closes_required")),
            "additional_fills_required_for_density": fnum(req.get("additional_fills_required_for_density")),
        },
        "completion_targets": targets,
        "design": {
            "controller": "residual_aware_passive_maker_completion",
            "profile_family": "cap25_residual_aware_maker_completion_design",
            "economic_boundary": (
                "quote opposite passive close orders no worse than strict_rescue_surplus_net_cap using conservative "
                "taker-fee math; target pair cost cap stays <=0.99 where available"
            ),
            "source_boundary": "preserve current source-quality requirements; no stale source admission or source relaxation",
            "admission_boundary": (
                "do not solve residual by blocking additional fills; current fill density is already one fill below the gate"
            ),
            "required_runner_work": [
                "track residual FIFO lots as close-generation targets",
                "emit passive opposite-side completion intents for target lots with per-lot max_comp_px limits",
                "account completed maker close fills as strict/pair completion evidence without taker-order mutation",
                "cap outstanding completion cost and cancel stale passive completion intents locally in dry-run only",
            ],
            "must_not_change": [
                "strict_rescue_surplus_net_cap",
                "strict_rescue_min_pair_pnl_after",
                "source-quality requirements",
                "PM_DRY_RUN and no-order boundary",
                "cap25 only until capacity and private-truth gates clear",
            ],
        },
        "public_benchmark_context": {
            "public_04b6_weighted_pair_cost": public_pair.get("weighted_pair_cost"),
            "public_04b6_residual_qty_share": public_pair.get("residual_qty_share"),
            "public_04b6_residual_cost_share": public_pair.get("residual_cost_share"),
            "interpretation": "04b6 supports maker-heavy low-residual calibration, not direct copying or private truth",
        },
        "acceptance_criteria_for_future_local_simulation": {
            "accepted_actions_min": requirement.get("thresholds", {}).get("min_accepted_actions"),
            "queue_supported_fills_min": requirement.get("thresholds", {}).get("min_fills"),
            "strict_rescue_closes_min": requirement.get("thresholds", {}).get("min_rescue_closes"),
            "residual_cost_after_max": fnum(required.get("max_residual_cost_by_pair_qty")),
            "residual_qty_after_max": fnum(required.get("max_residual_qty")),
            "strict_rescue_source_blocks_max": 0,
            "future_remote_requires_manifest_verifier": True,
        },
        "interpretation": {
            "why_this_design": (
                "the latest cap25 run needs more economic close generation; existing blocking, earlier rescue, and taker "
                "pair-completion paths do not clear both density and residual gates"
            ),
            "next_local_target": (
                "implement or simulate the residual-aware passive maker-completion controller locally before considering "
                "any new bounded cap25 PM_DRY_RUN"
            ),
        },
        "decision": {
            "design_ready": True,
            "runner_feature_required": True,
            "candidate_profile_ready": False,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_design": False,
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
            "next_action": "local_runner_or_simulator_implementation_for_residual_aware_maker_completion",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requirement-scorecard", type=Path, default=DEFAULT_REQUIREMENT)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--failure-diagnosis-scorecard", type=Path, default=DEFAULT_FAILURE)
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
