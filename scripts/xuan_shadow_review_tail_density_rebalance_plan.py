#!/usr/bin/env python3
"""Plan the next local tail-density re-balance after a tail mitigation run.

This local-only planner turns the tail-mitigated runtime result into a concrete
next-profile hypothesis. It does not launch remote jobs, approve deploy, approve
live orders, or relax the source/economic caps that define the current bridge.
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


def get_stat(stats: dict[str, Any], key: str, stat: str) -> float | None:
    raw = stats.get(key)
    if not isinstance(raw, dict):
        return None
    return fnum(raw.get(stat))


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
    g = card["gaps_to_next_clean_window"]
    h = card["next_profile_hypothesis"]
    lines = [
        "# Xuan Tail Density Rebalance Plan",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- rebalance_plan_ready: `{d['rebalance_plan_ready']}`",
        f"- future_remote_rationale_candidate: `{d['future_remote_rationale_candidate']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- do_not_expand_capacity: `{d['do_not_expand_capacity']}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Remaining Gaps",
        "",
        f"- strict_rescue_closes gap: `{g['strict_rescue_closes']}`",
        f"- rescue_per_candidate gap: `{g['rescue_per_candidate']}`",
        f"- rescue_per_fill gap: `{g['rescue_per_fill']}`",
        f"- residual_cost hard gap: `{g['residual_cost_hard']}`",
        "",
        "## Next Profile Hypothesis",
        "",
        f"- candidate: `{h['candidate']}`",
        f"- salvage_min_lot_cost: `{h['profile_overrides']['salvage_min_lot_cost']}`",
        f"- surplus_budget_max_abs_unpaired_cost: `{h['profile_overrides']['surplus_budget_max_abs_unpaired_cost']}`",
        f"- imbalance_qty_cap: `{h['profile_overrides']['imbalance_qty_cap']}`",
        f"- strict_rescue_surplus_net_cap: `{h['profile_overrides']['strict_rescue_surplus_net_cap']}`",
        "",
        "## Guardrails",
        "",
        "- This is a local planning artifact, not remote authorization.",
        "- Keep cap25 until current-stage residual and density gates both clear.",
        "- Do not widen strict_rescue_surplus_net_cap above 1.02.",
        "- Do not enable closeability cancel on the soft-mainline lane.",
        "- Do not deploy, restart, send live orders, or mutate shared services.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    baseline_runtime = load_json(args.baseline_runtime_scorecard)
    baseline_density = load_json(args.baseline_density_scorecard)
    tail_runtime = load_json(args.tail_runtime_scorecard)
    tail_density = load_json(args.tail_density_scorecard)
    tail_result = load_json(args.tail_result_scorecard)
    tail_profile = load_json(args.tail_profile_scorecard)
    tail_diag = load_json(args.tail_event_diagnostics)
    baseline_diag = load_json(args.baseline_event_diagnostics)

    base_metrics = body(baseline_runtime, "metrics")
    tail_metrics = body(tail_runtime, "metrics")
    base_obs = body(baseline_density, "observed")
    tail_obs = body(tail_density, "observed")
    result_decision = body(tail_result, "decision")
    result_gates = body(tail_result, "residual_gates")
    result_targets = body(tail_result, "targets")
    source_profile = body(tail_profile, "profile") or body(tail_runtime, "profile")

    tail_block_counts = body(tail_diag, "block_reason_counts")
    base_block_counts = body(baseline_diag, "block_reason_counts")
    tail_lot_classes = body(tail_diag, "strict_rescue_lot_age_min_cost_classes")
    tail_components = body(tail_diag, "strict_rescue_block_components")

    tail_lot_cost_p50 = get_stat(tail_diag, "strict_rescue_block_lot_cost", "p50")
    tail_cost_gap_p50 = get_stat(tail_diag, "strict_rescue_block_lot_cost_to_threshold", "p50")
    current_salvage_min = fnum(source_profile.get("salvage_min_lot_cost"), args.current_salvage_min_lot_cost)
    proposed_salvage_min = args.proposed_salvage_min_lot_cost
    if proposed_salvage_min is None:
        if tail_lot_cost_p50 is not None:
            proposed_salvage_min = max(args.min_salvage_min_lot_cost, min(args.max_salvage_min_lot_cost, tail_lot_cost_p50 - 0.005))
        elif tail_cost_gap_p50 is not None and current_salvage_min is not None:
            proposed_salvage_min = max(
                args.min_salvage_min_lot_cost,
                min(args.max_salvage_min_lot_cost, current_salvage_min - tail_cost_gap_p50),
            )
        else:
            proposed_salvage_min = args.default_proposed_salvage_min_lot_cost

    profile_overrides = {
        "salvage_min_lot_cost": proposed_salvage_min,
        "strict_rescue_skip_low_cost_lots": True,
        "surplus_budget_max_abs_unpaired_cost": fnum(
            source_profile.get("surplus_budget_max_abs_unpaired_cost"), 1.44
        ),
        "imbalance_qty_cap": fnum(source_profile.get("imbalance_qty_cap"), 5.0),
        "strict_rescue_surplus_net_cap": min(
            fnum(source_profile.get("strict_rescue_surplus_net_cap"), 1.02) or 1.02,
            1.02,
        ),
        "strict_rescue_min_pair_pnl_after": max(
            fnum(source_profile.get("strict_rescue_min_pair_pnl_after"), 0.0) or 0.0,
            0.0,
        ),
        "risk_seed_cancel_on_closeability_net_cap": None,
        "profile_family": "residual_guard",
        "tail_mitigation_hypothesis": "cap25_low_lot_rescue_density_rebalance",
    }

    strict_rescue_gap = max(0.0, args.min_strict_rescue_closes - (fnum(tail_metrics.get("strict_rescue_closes"), 0.0) or 0.0))
    rescue_per_candidate_gap = max(
        0.0,
        args.min_rescue_per_candidate - (fnum(tail_obs.get("rescue_per_candidate"), 0.0) or 0.0),
    )
    rescue_per_fill_gap = max(
        0.0,
        args.min_rescue_per_fill - (fnum(tail_obs.get("rescue_per_fill"), 0.0) or 0.0),
    )
    residual_cost_gap = max(0.0, fnum(result_gates.get("residual_cost_hard_gap"), 0.0) or 0.0)

    low_lot_block_signal = (
        (fnum(tail_components.get("lot_min_cost"), 0.0) or 0.0)
        > (fnum(tail_components.get("lot_age"), 0.0) or 0.0)
        and (fnum(tail_lot_classes.get("min_cost_only"), 0.0) or 0.0) > 0
    )
    residual_direction = result_decision.get("residual_direction_validated") is True
    quality_blocked = result_decision.get("quality_gate_clear") is False
    profile_ready = is_keep(tail_profile) and residual_direction and quality_blocked and low_lot_block_signal

    hard_blockers: list[str] = []
    if not is_keep(tail_profile):
        hard_blockers.append("tail_profile_not_keep")
    if not residual_direction:
        hard_blockers.append("tail_result_did_not_validate_residual_direction")
    if not quality_blocked:
        hard_blockers.append("tail_result_not_quality_blocked")
    if not low_lot_block_signal:
        hard_blockers.append("low_lot_rescue_block_signal_missing")
    if profile_overrides["strict_rescue_surplus_net_cap"] > 1.02:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if profile_overrides["risk_seed_cancel_on_closeability_net_cap"] is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")

    status_value = (
        "KEEP_SHADOW_REVIEW_TAIL_DENSITY_REBALANCE_PLAN_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_TAIL_DENSITY_REBALANCE_PLAN_BLOCKED_LOCAL_ONLY"
    )

    return rounded(
        {
            "artifact": "xuan_shadow_review_tail_density_rebalance_plan",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_tail_density_rebalance_plan.py",
            "status": status_value,
            "inputs": {
                "baseline_runtime_scorecard": args.baseline_runtime_scorecard,
                "baseline_density_scorecard": args.baseline_density_scorecard,
                "tail_runtime_scorecard": args.tail_runtime_scorecard,
                "tail_density_scorecard": args.tail_density_scorecard,
                "tail_result_scorecard": args.tail_result_scorecard,
                "tail_profile_scorecard": args.tail_profile_scorecard,
                "tail_event_diagnostics": args.tail_event_diagnostics,
                "baseline_event_diagnostics": args.baseline_event_diagnostics,
            },
            "source_statuses": {
                "baseline_runtime": status(baseline_runtime),
                "baseline_density": status(baseline_density),
                "tail_runtime": status(tail_runtime),
                "tail_density": status(tail_density),
                "tail_result": status(tail_result),
                "tail_profile": status(tail_profile),
            },
            "decision": {
                "rebalance_plan_ready": profile_ready,
                "future_remote_rationale_candidate": profile_ready,
                "future_remote_allowed_by_this_plan": False,
                "requires_manifest_verifier_pass": True,
                "requires_runner_scaffold_or_manifest_profile_check": True,
                "requires_no_runner_conflict_check": True,
                "remote_runner_allowed": False,
                "research_only": True,
                "paper_shadow_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "do_not_expand_capacity": True,
                "do_not_repeat_same_tail_profile": True,
                "hard_blockers": hard_blockers,
                "next_action": "prepare_manifest_verifier_for_low_lot_tail_density_rebalance_profile_before_any_future_remote"
                if profile_ready
                else "stay_local_until_tail_density_rebalance_inputs_are_clean",
            },
            "gaps_to_next_clean_window": {
                "strict_rescue_closes": strict_rescue_gap,
                "rescue_per_candidate": rescue_per_candidate_gap,
                "rescue_per_fill": rescue_per_fill_gap,
                "residual_cost_hard": residual_cost_gap,
                "residual_cost_target": result_targets.get("max_residual_cost_for_remote_hard"),
                "residual_qty_target": result_targets.get("max_residual_qty_for_public_target"),
            },
            "runtime_contrast": {
                "accepted_actions_delta_tail_vs_baseline": diff(
                    fnum(tail_metrics.get("accepted_actions")), fnum(base_metrics.get("accepted_actions"))
                ),
                "fills_delta_tail_vs_baseline": diff(
                    fnum(tail_metrics.get("queue_supported_fills")),
                    fnum(base_metrics.get("queue_supported_fills")),
                ),
                "strict_rescue_closes_delta_tail_vs_baseline": diff(
                    fnum(tail_metrics.get("strict_rescue_closes")),
                    fnum(base_metrics.get("strict_rescue_closes")),
                ),
                "pair_pnl_delta_tail_vs_baseline": diff(
                    fnum(tail_metrics.get("pair_pnl")), fnum(base_metrics.get("pair_pnl"))
                ),
                "residual_qty_delta_tail_vs_baseline": diff(
                    fnum(tail_metrics.get("residual_qty")), fnum(base_metrics.get("residual_qty"))
                ),
                "residual_cost_delta_tail_vs_baseline": diff(
                    fnum(tail_metrics.get("residual_cost")), fnum(base_metrics.get("residual_cost"))
                ),
                "rescue_per_candidate_delta_tail_vs_baseline": diff(
                    fnum(tail_obs.get("rescue_per_candidate")), fnum(base_obs.get("rescue_per_candidate"))
                ),
                "rescue_per_fill_delta_tail_vs_baseline": diff(
                    fnum(tail_obs.get("rescue_per_fill")), fnum(base_obs.get("rescue_per_fill"))
                ),
            },
            "block_diagnostics": {
                "tail_block_reason_counts": tail_block_counts,
                "baseline_block_reason_counts": base_block_counts,
                "tail_strict_rescue_block_components": tail_components,
                "tail_lot_age_min_cost_classes": tail_lot_classes,
                "tail_block_lot_cost_p50": tail_lot_cost_p50,
                "tail_block_lot_cost_to_threshold_p50": tail_cost_gap_p50,
                "low_lot_block_signal": low_lot_block_signal,
            },
            "next_profile_hypothesis": {
                "candidate": "cap25_low_lot_rescue_density_rebalance",
                "profile_overrides": profile_overrides,
                "rationale": [
                    "The tail profile validated residual direction but lost rescue density.",
                    "Low-cost lot gating is the dominant tail strict-rescue block component.",
                    "Lowering salvage_min_lot_cost targets one additional clean rescue without widening net-cap economics.",
                    "Surplus budget, imbalance cap, source gates, no-cancel mainline, and strict rescue PnL floor stay constrained.",
                ],
            },
            "minimum_future_window_acceptance": {
                "accepted_actions": 25,
                "queue_supported_fills": 20,
                "strict_rescue_closes": 7,
                "rescue_per_candidate": args.min_rescue_per_candidate,
                "rescue_per_fill": args.min_rescue_per_fill,
                "pair_pnl": 0.0,
                "max_residual_cost": result_targets.get("max_residual_cost_for_remote_hard"),
                "max_residual_qty": result_targets.get("max_residual_qty_for_public_target"),
                "max_rescue_l1_age_ms": 50,
                "strict_rescue_source_blocks": 0,
                "orders_sent": False,
                "stderr_bytes": 0,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-runtime-scorecard", required=True)
    parser.add_argument("--baseline-density-scorecard", required=True)
    parser.add_argument("--tail-runtime-scorecard", required=True)
    parser.add_argument("--tail-density-scorecard", required=True)
    parser.add_argument("--tail-result-scorecard", required=True)
    parser.add_argument("--tail-profile-scorecard", required=True)
    parser.add_argument("--tail-event-diagnostics", required=True)
    parser.add_argument("--baseline-event-diagnostics")
    parser.add_argument("--min-strict-rescue-closes", type=float, default=7.0)
    parser.add_argument("--min-rescue-per-candidate", type=float, default=0.25)
    parser.add_argument("--min-rescue-per-fill", type=float, default=0.30)
    parser.add_argument("--current-salvage-min-lot-cost", type=float, default=0.25)
    parser.add_argument("--proposed-salvage-min-lot-cost", type=float)
    parser.add_argument("--default-proposed-salvage-min-lot-cost", type=float, default=0.18)
    parser.add_argument("--min-salvage-min-lot-cost", type=float, default=0.10)
    parser.add_argument("--max-salvage-min-lot-cost", type=float, default=0.20)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        md = Path(args.markdown).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(markdown(card) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
