#!/usr/bin/env python3
"""Local design artifact for symmetric/prior-resting completion.

Consumes the prepositioned completion simulator plus causality audit and
defines the next implementable local design boundary. It intentionally does not
declare a runner profile ready or authorize remote runs.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_SIM = Path(".tmp_xuan/scorecards/xuan_shadow_review_prepositioned_maker_completion_simulator_20260526T2148Z.json")
DEFAULT_CAUSALITY = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_prepositioned_completion_causality_audit_20260526T2148Z.json"
)
DEFAULT_DESIGN = Path(".tmp_xuan/scorecards/xuan_shadow_review_residual_aware_maker_completion_design_20260526T2148Z.json")
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_symmetric_prepositioning_design_20260526T2148Z.json")


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


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    r = card["design_requirements"]
    lines = [
        "# Xuan Symmetric Prepositioning Design",
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
        "## Requirements",
        "",
        f"- target mode: `{r['target_mode']}`",
        f"- uncloseable seed block required: `{r['uncloseable_seed_block_required']}`",
        f"- minimum local scaffold gates: `{r['minimum_local_scaffold_gates']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Does not authorize remote runs, cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    sim = load_json(args.prepositioned_simulator_scorecard)
    causality = load_json(args.causality_audit_scorecard)
    design = load_json(args.residual_aware_design_scorecard)
    hybrid = sim.get("hybrid_uncloseable_lot_block_proxy", {})
    causality_summary = causality.get("causality_summary", {})
    completion_targets = design.get("completion_targets", [])
    unclosed_orders = hybrid.get("unclosed_orders", [])
    noncausal_cost = fnum(causality_summary.get("before_seed_accept_closed_residual_cost"))
    hard_blockers = [
        "runner_feature_not_implemented",
        "causality_safe_scaffold_not_built",
        "private_truth_not_ready",
    ]
    if noncausal_cost > 1e-12:
        hard_blockers.append("after_seed_completion_not_sufficient")
    if sim.get("decision", {}).get("hybrid_prepositioned_plus_uncloseable_block_proxy_pass") is not True:
        hard_blockers.append("hybrid_proxy_not_passed")
    card = {
        "artifact": "xuan_shadow_review_symmetric_prepositioning_design",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_symmetric_prepositioning_design.py",
        "status": "KEEP_SHADOW_REVIEW_SYMMETRIC_PREPOSITIONING_DESIGN_READY_LOCAL_ONLY",
        "inputs": {
            "prepositioned_simulator_scorecard": str(
                Path(args.prepositioned_simulator_scorecard).expanduser().resolve()
            ),
            "causality_audit_scorecard": str(Path(args.causality_audit_scorecard).expanduser().resolve()),
            "residual_aware_design_scorecard": str(Path(args.residual_aware_design_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "prepositioned_simulator": sim.get("status"),
            "causality_audit": causality.get("status"),
            "residual_aware_design": design.get("status"),
        },
        "evidence": {
            "hybrid_proxy_pass": sim.get("decision", {}).get("hybrid_prepositioned_plus_uncloseable_block_proxy_pass"),
            "hybrid_residual_qty_after": hybrid.get("residual_qty_after"),
            "hybrid_residual_cost_after": hybrid.get("residual_cost_after"),
            "hybrid_accepted_actions_proxy": hybrid.get("accepted_actions_proxy"),
            "hybrid_queue_supported_fills_proxy": hybrid.get("queue_supported_fills_proxy"),
            "hybrid_strict_rescue_closes_proxy": hybrid.get("strict_rescue_closes_proxy"),
            "before_seed_accept_orders": causality_summary.get("before_seed_accept_orders"),
            "before_seed_accept_closed_residual_cost": causality_summary.get(
                "before_seed_accept_closed_residual_cost"
            ),
            "after_seed_accept_before_seed_fill_orders": causality_summary.get(
                "after_seed_accept_before_seed_fill_orders"
            ),
            "after_seed_fill_orders": causality_summary.get("after_seed_fill_orders"),
        },
        "design_requirements": {
            "target_mode": "symmetric_or_prior_resting_completion",
            "target_residual_lot_count": len(completion_targets),
            "completion_target_limits": completion_targets,
            "uncloseable_seed_block_required": True,
            "uncloseable_seed_block_targets": unclosed_orders,
            "must_preserve": [
                "source_quality_require_trade_source",
                "source_quality_require_l1_source",
                "source_quality_require_l2_source",
                "strict_rescue_surplus_net_cap",
                "strict_rescue_min_pair_pnl_after",
                "PM_DRY_RUN/no-order boundary",
            ],
            "minimum_local_scaffold_gates": [
                "causality-safe placement timestamps",
                "queue-supported fill accounting for completion orders",
                "residual FIFO reduction from actual completion fills",
                "uncloseable seed block emits explicit diagnostic",
                "accepted/fill/rescue density still pass after block and completion accounting",
                "source blocks remain zero",
            ],
        },
        "interpretation": {
            "why_symmetric": (
                "most passing completion fills in the proxy occur before seed acceptance, so the next implementable "
                "artifact must model symmetric/prior-resting completion rather than after-fill repair"
            ),
            "remote_boundary": (
                "the design is not a candidate profile; it requires local runner/scaffold implementation and validation "
                "before any bounded cap25 dry-run rationale can be reconsidered"
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
            "next_action": "implement_local_scaffold_for_symmetric_prepositioned_completion",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepositioned-simulator-scorecard", type=Path, default=DEFAULT_SIM)
    parser.add_argument("--causality-audit-scorecard", type=Path, default=DEFAULT_CAUSALITY)
    parser.add_argument("--residual-aware-design-scorecard", type=Path, default=DEFAULT_DESIGN)
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
