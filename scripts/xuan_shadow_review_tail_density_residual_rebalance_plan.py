#!/usr/bin/env python3
"""Plan a residual-aware follow-up to the low-lot density rebalance.

The 0432Z tail profile held residuals down but missed rescue density by one
strict rescue. The 0700Z low-lot rebalance recovered density but regressed
residuals. This local-only helper proposes a midpoint profile candidate that
keeps the residual guardrails and raises the rescue PnL floor. It does not
launch remote jobs or authorize deploy/live execution.
"""

from __future__ import annotations

import argparse
import json
import math
import time
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


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def diff(new: float | None, old: float | None) -> float | None:
    if new is None or old is None:
        return None
    return round(new - old, 6)


def stat(card: dict[str, Any], key: str, field: str) -> float | None:
    raw = card.get(key)
    if not isinstance(raw, dict):
        return None
    return fnum(raw.get(field))


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    p = card["next_profile_hypothesis"]["profile_overrides"]
    r = card["runtime_tradeoff"]
    lines = [
        "# Xuan Tail Density Residual Rebalance Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- residual_aware_rebalance_plan_ready: `{d['residual_aware_rebalance_plan_ready']}`",
        f"- future_remote_rationale_candidate: `{d['future_remote_rationale_candidate']}`",
        f"- future_remote_allowed_by_this_plan: `{d['future_remote_allowed_by_this_plan']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- next_action: `{d['next_action']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Proposed Profile",
        "",
        f"- salvage_min_lot_cost: `{p['salvage_min_lot_cost']}`",
        f"- strict_rescue_min_pair_pnl_after: `{p['strict_rescue_min_pair_pnl_after']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{p['surplus_budget_max_abs_unpaired_cost']}`",
        f"- imbalance_qty_cap: `{p['imbalance_qty_cap']}`",
        f"- strict_rescue_surplus_net_cap: `{p['strict_rescue_surplus_net_cap']}`",
        "",
        "## Observed Tradeoff",
        "",
        f"- rescue delta 0700Z vs 0432Z: `{r['strict_rescue_closes_delta_low_lot_vs_tail']}`",
        f"- residual cost delta 0700Z vs 0432Z: `{r['residual_cost_delta_low_lot_vs_tail']}`",
        f"- residual qty delta 0700Z vs 0432Z: `{r['residual_qty_delta_low_lot_vs_tail']}`",
        f"- pair pnl delta 0700Z vs 0432Z: `{r['pair_pnl_delta_low_lot_vs_tail']}`",
        "",
        "## Guardrails",
        "",
        "- This is local planning only; it does not launch a remote run.",
        "- The same 0.1825 low-lot profile must not be repeated.",
        "- Keep cap25; do not expand to cap75 while residual gates are blocked.",
        "- Do not relax strict_rescue_surplus_net_cap above 1.02.",
        "- Do not enable closeability cancellation on the soft-mainline lane.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    tail_runtime = load_json(args.tail_runtime_scorecard)
    low_lot_runtime = load_json(args.low_lot_runtime_scorecard)
    tail_diag = load_json(args.tail_event_diagnostics)
    low_lot_diag = load_json(args.low_lot_event_diagnostics)
    low_lot_result = load_json(args.low_lot_result_scorecard)
    capacity_gate = load_json(args.capacity_stage_public_benchmark_scorecard)
    tail_profile = load_json(args.tail_profile_scorecard)
    low_lot_profile = load_json(args.low_lot_profile_scorecard)

    tail_metrics = body(tail_runtime, "metrics")
    low_metrics = body(low_lot_runtime, "metrics")
    tail_profile_body = body(tail_profile, "profile") or body(tail_runtime, "profile")
    low_profile_body = body(low_lot_profile, "profile") or body(low_lot_runtime, "profile")
    low_result_decision = body(low_lot_result, "decision")
    capacity_decision = body(capacity_gate, "decision")

    tail_salvage_min = fnum(tail_profile_body.get("salvage_min_lot_cost"), 0.25) or 0.25
    low_lot_salvage_min = fnum(low_profile_body.get("salvage_min_lot_cost"), 0.1825) or 0.1825
    midpoint_salvage_min = args.proposed_salvage_min_lot_cost
    if midpoint_salvage_min is None:
        midpoint_salvage_min = (tail_salvage_min + low_lot_salvage_min) / 2.0
    midpoint_salvage_min = max(low_lot_salvage_min, min(tail_salvage_min, midpoint_salvage_min))
    old_min_cost_p50 = stat(tail_diag, "strict_rescue_block_lot_cost", "p50")
    if old_min_cost_p50 is not None and midpoint_salvage_min <= old_min_cost_p50:
        midpoint_salvage_min = min(tail_salvage_min, old_min_cost_p50 + args.min_cost_p50_buffer)

    low_projected_min = stat(low_lot_diag, "strict_rescue_close_projected_pair_pnl_after", "min")
    proposed_pair_pnl_floor = args.proposed_strict_rescue_min_pair_pnl_after
    if proposed_pair_pnl_floor is None:
        proposed_pair_pnl_floor = max(args.min_strict_rescue_pair_pnl_floor, 0.0)
        if low_projected_min is not None and low_projected_min < 0:
            proposed_pair_pnl_floor = max(proposed_pair_pnl_floor, min(args.max_strict_rescue_pair_pnl_floor, abs(low_projected_min) / 2.0))

    low_density_recovered = bool(low_result_decision.get("density_recovered"))
    low_residual_regressed = bool(low_result_decision.get("residual_regressed_vs_0432_tail_mitigation")) or bool(
        low_result_decision.get("residual_regressed_vs_2041_baseline")
    )
    residual_blockers = list(body(low_lot_result, "decision").get("residual_blockers") or [])
    capacity_blockers = list(capacity_decision.get("hard_blockers") or [])

    profile_overrides = {
        "salvage_min_lot_cost": midpoint_salvage_min,
        "strict_rescue_skip_low_cost_lots": True,
        "strict_rescue_min_pair_pnl_after": proposed_pair_pnl_floor,
        "strict_rescue_surplus_net_cap": min(
            fnum(tail_profile_body.get("strict_rescue_surplus_net_cap"), 1.02) or 1.02,
            1.02,
        ),
        "surplus_budget_max_abs_unpaired_cost": fnum(
            tail_profile_body.get("surplus_budget_max_abs_unpaired_cost"), 1.44
        ),
        "imbalance_qty_cap": fnum(tail_profile_body.get("imbalance_qty_cap"), 5.0),
        "max_salvage_qty": fnum(tail_profile_body.get("max_salvage_qty"), 250.0),
        "risk_seed_cancel_on_closeability_net_cap": None,
        "target_qty": fnum(tail_profile_body.get("target_qty"), 25.0),
        "profile_family": "residual_guard",
        "tail_mitigation_hypothesis": "cap25_low_lot_rescue_density_midpoint_with_pnl_floor",
    }

    hard_blockers: list[str] = []
    if not is_keep(tail_profile):
        hard_blockers.append("tail_profile_not_keep")
    if not is_keep(low_lot_profile):
        hard_blockers.append("low_lot_profile_not_keep")
    if not low_density_recovered:
        hard_blockers.append("low_lot_density_not_recovered")
    if not low_residual_regressed:
        hard_blockers.append("low_lot_residual_regression_not_confirmed")
    if midpoint_salvage_min >= tail_salvage_min:
        hard_blockers.append("midpoint_salvage_min_not_below_tail_profile")
    if midpoint_salvage_min <= low_lot_salvage_min:
        hard_blockers.append("midpoint_salvage_min_not_above_failed_low_lot_profile")
    if profile_overrides["strict_rescue_surplus_net_cap"] > 1.02:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if profile_overrides["risk_seed_cancel_on_closeability_net_cap"] is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")
    if not residual_blockers:
        hard_blockers.append("residual_blockers_missing")

    plan_ready = not hard_blockers
    status_value = (
        "KEEP_SHADOW_REVIEW_TAIL_DENSITY_RESIDUAL_REBALANCE_PLAN_READY_LOCAL_ONLY"
        if plan_ready
        else "UNKNOWN_SHADOW_REVIEW_TAIL_DENSITY_RESIDUAL_REBALANCE_PLAN_BLOCKED_LOCAL_ONLY"
    )
    return rounded(
        {
            "artifact": "xuan_shadow_review_tail_density_residual_rebalance_plan",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_tail_density_residual_rebalance_plan.py",
            "status": status_value,
            "inputs": {
                "tail_runtime_scorecard": args.tail_runtime_scorecard,
                "low_lot_runtime_scorecard": args.low_lot_runtime_scorecard,
                "tail_event_diagnostics": args.tail_event_diagnostics,
                "low_lot_event_diagnostics": args.low_lot_event_diagnostics,
                "low_lot_result_scorecard": args.low_lot_result_scorecard,
                "capacity_stage_public_benchmark_scorecard": args.capacity_stage_public_benchmark_scorecard,
                "tail_profile_scorecard": args.tail_profile_scorecard,
                "low_lot_profile_scorecard": args.low_lot_profile_scorecard,
            },
            "source_statuses": {
                "tail_runtime": status(tail_runtime),
                "low_lot_runtime": status(low_lot_runtime),
                "low_lot_result": status(low_lot_result),
                "capacity_stage_public_benchmark": status(capacity_gate),
                "tail_profile": status(tail_profile),
                "low_lot_profile": status(low_lot_profile),
            },
            "decision": {
                "residual_aware_rebalance_plan_ready": plan_ready,
                "future_remote_rationale_candidate": plan_ready,
                "future_remote_allowed_by_this_plan": False,
                "requires_manifest_verifier_pass": True,
                "requires_runner_conflict_check": True,
                "remote_runner_allowed": False,
                "cap75_remote_rationale_ready": False,
                "same_low_lot_profile_repeat_allowed": False,
                "do_not_expand_capacity": True,
                "research_only": True,
                "paper_shadow_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "hard_blockers": hard_blockers,
                "residual_blockers": residual_blockers,
                "capacity_stage_blockers": capacity_blockers,
                "next_action": "materialize_midpoint_profile_and_run_manifest_verifier_before_any_future_remote"
                if plan_ready
                else "stay_local_until_density_residual_tradeoff_inputs_are_clean",
            },
            "runtime_tradeoff": {
                "strict_rescue_closes_tail": fnum(tail_metrics.get("strict_rescue_closes")),
                "strict_rescue_closes_low_lot": fnum(low_metrics.get("strict_rescue_closes")),
                "strict_rescue_closes_delta_low_lot_vs_tail": diff(
                    fnum(low_metrics.get("strict_rescue_closes")),
                    fnum(tail_metrics.get("strict_rescue_closes")),
                ),
                "residual_qty_tail": fnum(tail_metrics.get("residual_qty")),
                "residual_qty_low_lot": fnum(low_metrics.get("residual_qty")),
                "residual_qty_delta_low_lot_vs_tail": diff(
                    fnum(low_metrics.get("residual_qty")),
                    fnum(tail_metrics.get("residual_qty")),
                ),
                "residual_cost_tail": fnum(tail_metrics.get("residual_cost")),
                "residual_cost_low_lot": fnum(low_metrics.get("residual_cost")),
                "residual_cost_delta_low_lot_vs_tail": diff(
                    fnum(low_metrics.get("residual_cost")),
                    fnum(tail_metrics.get("residual_cost")),
                ),
                "pair_pnl_tail": fnum(tail_metrics.get("pair_pnl")),
                "pair_pnl_low_lot": fnum(low_metrics.get("pair_pnl")),
                "pair_pnl_delta_low_lot_vs_tail": diff(
                    fnum(low_metrics.get("pair_pnl")),
                    fnum(tail_metrics.get("pair_pnl")),
                ),
            },
            "diagnostics": {
                "tail_salvage_min_lot_cost": tail_salvage_min,
                "failed_low_lot_salvage_min_lot_cost": low_lot_salvage_min,
                "tail_block_lot_cost_p50": old_min_cost_p50,
                "tail_block_lot_cost_to_threshold_p50": stat(tail_diag, "strict_rescue_block_lot_cost_to_threshold", "p50"),
                "low_lot_strict_rescue_projected_pair_pnl_after_min": low_projected_min,
                "low_lot_strict_rescue_projected_pair_pnl_after_p50": stat(
                    low_lot_diag, "strict_rescue_close_projected_pair_pnl_after", "p50"
                ),
                "low_lot_strict_rescue_net_pair_cost_max": stat(low_lot_diag, "strict_rescue_close_net_pair_cost", "max"),
            },
            "next_profile_hypothesis": {
                "candidate": "cap25_low_lot_rescue_density_midpoint_with_pnl_floor",
                "profile_overrides": profile_overrides,
                "rationale": [
                    "0432Z residual-bound profile missed rescue density by one strict rescue.",
                    "0700Z lower low-lot threshold recovered density but regressed residual qty and cost.",
                    "The midpoint threshold admits fewer low-lot rescues than 0700Z while still targeting the 0432Z density miss.",
                    "Raising strict_rescue_min_pair_pnl_after is a conservative filter against the negative projected rescue closes seen in 0700Z.",
                    "Capacity expansion remains blocked until cap25 residual gates clear.",
                ],
            },
            "minimum_future_window_acceptance": {
                "accepted_actions": 25,
                "queue_supported_fills": 20,
                "strict_rescue_closes": 7,
                "pair_pnl": 0.0,
                "max_residual_qty_share": 0.2,
                "max_residual_cost_share": 0.15,
                "max_residual_cost_to_pair_qty": 0.05,
                "max_rescue_l1_age_ms": 50,
                "strict_rescue_source_blocks": 0,
                "orders_sent": False,
                "stderr_bytes": 0,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tail-runtime-scorecard", required=True)
    parser.add_argument("--low-lot-runtime-scorecard", required=True)
    parser.add_argument("--tail-event-diagnostics", required=True)
    parser.add_argument("--low-lot-event-diagnostics", required=True)
    parser.add_argument("--low-lot-result-scorecard", required=True)
    parser.add_argument("--capacity-stage-public-benchmark-scorecard", required=True)
    parser.add_argument("--tail-profile-scorecard", required=True)
    parser.add_argument("--low-lot-profile-scorecard", required=True)
    parser.add_argument("--proposed-salvage-min-lot-cost", type=float)
    parser.add_argument("--proposed-strict-rescue-min-pair-pnl-after", type=float)
    parser.add_argument("--min-strict-rescue-pair-pnl-floor", type=float, default=0.02)
    parser.add_argument("--max-strict-rescue-pair-pnl-floor", type=float, default=0.05)
    parser.add_argument("--min-cost-p50-buffer", type=float, default=0.02)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown(card) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
