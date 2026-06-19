#!/usr/bin/env python3
"""Implementability audit for symmetric/prior-resting completion design.

The symmetric prepositioning proxy is useful only if it can be translated into
rules available before target residual lots are known. This audit separates:
- after-seed-accept pending-pair opportunities,
- before-seed-accept opportunities that require prior resting exposure, and
- target-specific limit choices that depend on future seed price/lot identity.

It is local/research-only and does not authorize remote runs.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_CAUSALITY = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_prepositioned_completion_causality_audit_20260526T2148Z.json"
)
DEFAULT_SYMMETRIC = Path(".tmp_xuan/scorecards/xuan_shadow_review_symmetric_prepositioning_design_20260526T2148Z.json")
DEFAULT_SIM = Path(".tmp_xuan/scorecards/xuan_shadow_review_prepositioned_maker_completion_simulator_20260526T2148Z.json")
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_symmetric_prepositioning_implementability_audit_20260526T2148Z.json"
)


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


def load_action_rows(root: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted(root.glob("*.would_action_decisions.csv")):
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                qid = row.get("quote_intent_id")
                if qid:
                    rows[qid] = row
    return rows


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    s = card["summary"]
    lines = [
        "# Xuan Symmetric Prepositioning Implementability Audit",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- implementability_audit_ready: `{d['implementability_audit_ready']}`",
        f"- target_specific_prepositioning_lookahead: `{d['target_specific_prepositioning_lookahead']}`",
        f"- after_accept_pending_pair_only_sufficient: `{d['after_accept_pending_pair_only_sufficient']}`",
        f"- generic_prior_resting_design_required: `{d['generic_prior_resting_design_required']}`",
        f"- future_remote_allowed_by_this_audit: `{d['future_remote_allowed_by_this_audit']}`",
        "",
        "## Summary",
        "",
        f"- before-seed residual cost: `{s['before_seed_accept_closed_residual_cost']}`",
        f"- after-accept-before-fill residual cost: `{s['after_seed_accept_before_seed_fill_closed_residual_cost']}`",
        f"- target-specific lookahead cost: `{s['target_specific_lookahead_residual_cost']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Does not authorize remote runs, cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    causality = load_json(args.causality_audit_scorecard)
    symmetric = load_json(args.symmetric_design_scorecard)
    sim = load_json(args.prepositioned_simulator_scorecard)
    action_rows = load_action_rows(root)
    audited = []
    target_specific_lookahead_cost = 0.0
    after_accept_cost = 0.0
    before_accept_cost = 0.0
    for row in causality.get("audited_orders", []):
        qid = str(row.get("quote_intent_id"))
        action = action_rows.get(qid, {})
        classification = row.get("classification")
        closed_cost = fnum(row.get("closed_residual_cost"))
        seed_px = fnum(action.get("price"))
        seed_ts = fnum(action.get("ts_ms"))
        fills = row.get("fills", [])
        before_seed_fill_count = 0
        after_accept_before_fill_count = 0
        for fill in fills:
            fill_ts = fnum(fill.get("ts_ms"))
            if fill_ts < seed_ts - 1e-12:
                before_seed_fill_count += 1
            elif classification == "after_seed_accept_before_seed_fill":
                after_accept_before_fill_count += 1
        target_limit_depends_on_future_seed = bool(fills) and before_seed_fill_count > 0 and seed_px > 0
        if target_limit_depends_on_future_seed:
            target_specific_lookahead_cost += closed_cost
        if classification == "before_seed_accept":
            before_accept_cost += closed_cost
        if classification == "after_seed_accept_before_seed_fill":
            after_accept_cost += closed_cost
        audited.append(
            {
                "quote_intent_id": qid,
                "classification": classification,
                "closed_residual_cost": closed_cost,
                "accepted_ts_ms": fnum(row.get("accepted_ts_ms")),
                "seed_fill_ts_ms": fnum(row.get("seed_fill_ts_ms")),
                "seed_action_ts_ms": seed_ts,
                "seed_px": seed_px,
                "seed_side": action.get("side"),
                "seed_source_quality": action.get("source_quality_decision"),
                "pre_seed_fill_events": before_seed_fill_count,
                "after_accept_before_fill_events": after_accept_before_fill_count,
                "target_limit_depends_on_future_seed": target_limit_depends_on_future_seed,
                "implementability_read": (
                    "target_specific_prior_resting_lookahead"
                    if target_limit_depends_on_future_seed
                    else "pending_pair_possible_after_accept"
                    if classification == "after_seed_accept_before_seed_fill"
                    else "unfilled_or_not_material"
                    if fnum(row.get("closed_qty")) <= 1e-12
                    else "requires_generic_prior_resting_rule"
                ),
            }
        )

    hybrid = sim.get("hybrid_uncloseable_lot_block_proxy", {})
    residual_cost_after_max = 0.522125
    # If only after-accept-before-fill opportunities are allowed, prior-resting
    # lookahead fills are removed and the residual gate remains blocked.
    strict = sim.get("simulation", {}).get("strict_surplus_cap_102", {})
    strict_closed_cost = fnum(strict.get("total_closed_residual_cost"))
    after_accept_only_closed_cost = after_accept_cost
    base_residual_cost = fnum(hybrid.get("residual_cost_after")) + fnum(hybrid.get("unclosed_residual_cost")) + strict_closed_cost
    after_accept_only_residual_cost_after = max(
        0.0,
        base_residual_cost - after_accept_only_closed_cost - fnum(hybrid.get("unclosed_residual_cost")),
    )
    after_accept_pending_pair_only_sufficient = after_accept_only_residual_cost_after <= residual_cost_after_max + 1e-12

    hard_blockers = [
        "target_specific_prepositioning_depends_on_future_seed_lots",
        "after_accept_pending_pair_only_does_not_clear_residual",
        "generic_prior_resting_rule_not_designed",
        "private_truth_not_ready",
    ]
    card = {
        "artifact": "xuan_shadow_review_symmetric_prepositioning_implementability_audit",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_symmetric_prepositioning_implementability_audit.py",
        "status": "KEEP_SHADOW_REVIEW_SYMMETRIC_PREPOSITIONING_IMPLEMENTABILITY_AUDIT_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "causality_audit_scorecard": str(Path(args.causality_audit_scorecard).expanduser().resolve()),
            "symmetric_design_scorecard": str(Path(args.symmetric_design_scorecard).expanduser().resolve()),
            "prepositioned_simulator_scorecard": str(Path(args.prepositioned_simulator_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "causality_audit": causality.get("status"),
            "symmetric_design": symmetric.get("status"),
            "prepositioned_simulator": sim.get("status"),
        },
        "summary": {
            "before_seed_accept_closed_residual_cost": before_accept_cost,
            "after_seed_accept_before_seed_fill_closed_residual_cost": after_accept_cost,
            "target_specific_lookahead_residual_cost": target_specific_lookahead_cost,
            "after_accept_only_residual_cost_after": after_accept_only_residual_cost_after,
            "hybrid_proxy_residual_cost_after": hybrid.get("residual_cost_after"),
            "hybrid_proxy_residual_qty_after": hybrid.get("residual_qty_after"),
        },
        "audited_orders": audited,
        "interpretation": {
            "core_read": (
                "the passing hybrid proxy is not yet implementable as a target-specific prepositioned profile because "
                "much of the close opportunity depends on seed price/lot identity that is only known later"
            ),
            "pending_pair_read": (
                "after-accept pending-pair completion alone is insufficient; a generic prior-resting rule is required "
                "and must be designed without target-lot lookahead"
            ),
            "next_local_target": "search for a generic symmetric admission/preposition rule using only contemporaneous source-quality signals",
        },
        "decision": {
            "implementability_audit_ready": True,
            "target_specific_prepositioning_lookahead": target_specific_lookahead_cost > 1e-12,
            "after_accept_pending_pair_only_sufficient": after_accept_pending_pair_only_sufficient,
            "generic_prior_resting_design_required": True,
            "candidate_profile_ready": False,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_audit": False,
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
            "next_action": "generic_symmetric_admission_rule_search_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--causality-audit-scorecard", type=Path, default=DEFAULT_CAUSALITY)
    parser.add_argument("--symmetric-design-scorecard", type=Path, default=DEFAULT_SYMMETRIC)
    parser.add_argument("--prepositioned-simulator-scorecard", type=Path, default=DEFAULT_SIM)
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
