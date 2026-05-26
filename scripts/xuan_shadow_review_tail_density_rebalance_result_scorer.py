#!/usr/bin/env python3
"""Score the low-lot tail-density rebalance result.

This local-only scorer separates a useful density result from a failed
residual/capacity result. It does not approve remote runs, capacity expansion,
deploys, restarts, live orders, or shared-service mutation.
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


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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


def passed_lte(actual: float | None, threshold: float | None) -> bool:
    return actual is not None and threshold is not None and actual <= threshold


def passed_gte(actual: float | None, threshold: float | None) -> bool:
    return actual is not None and threshold is not None and actual >= threshold


def residual_targets(tail_gate: dict[str, Any], capacity_stage_gate: dict[str, Any]) -> dict[str, float | None]:
    mitigation = body(tail_gate, "mitigation_targets")
    targets = {
        "max_residual_cost_for_remote_hard": fnum(mitigation.get("max_residual_cost_for_remote_hard")),
        "max_residual_qty_for_remote_hard": fnum(mitigation.get("max_residual_qty_for_remote_hard")),
        "max_residual_qty_for_public_target": fnum(mitigation.get("max_residual_qty_for_public_target")),
        "max_residual_qty_share": 0.2,
        "max_residual_cost_share": 0.15,
        "max_residual_cost_to_pair_qty": 0.05,
        "public_review_residual_rate_hard": 0.2,
    }
    for gate in listify(capacity_stage_gate.get("hard_gates")):
        if not isinstance(gate, dict):
            continue
        name = str(gate.get("name") or "")
        threshold = fnum(gate.get("threshold"))
        if threshold is None:
            continue
        if name == "max_residual_qty_share":
            targets["max_residual_qty_share"] = threshold
        elif name == "max_residual_cost_share":
            targets["max_residual_cost_share"] = threshold
        elif name == "max_residual_cost_to_pair_qty":
            targets["max_residual_cost_to_pair_qty"] = threshold
        elif name == "public_review_residual_rate_hard":
            targets["public_review_residual_rate_hard"] = threshold
    return targets


def density_effective_clear(density: dict[str, Any], source_audit: dict[str, Any]) -> tuple[bool, list[str]]:
    observed = body(density, "observed")
    thresholds = body(density, "thresholds")
    blockers: list[str] = []
    if not passed_gte(fnum(observed.get("candidate_rows")), fnum(thresholds.get("min_candidates"), 20.0)):
        blockers.append("candidate_rows_below_min")
    if not passed_gte(fnum(observed.get("accepted_actions")), fnum(thresholds.get("min_accepted_actions"), 20.0)):
        blockers.append("accepted_actions_below_min")
    if not passed_gte(fnum(observed.get("queue_supported_fills")), fnum(thresholds.get("min_fills"), 18.0)):
        blockers.append("fills_below_min")
    if not passed_gte(fnum(observed.get("strict_rescue_or_salvage_rows")), fnum(thresholds.get("min_rescue_closes"), 7.0)):
        blockers.append("strict_rescue_closes_below_min")
    if not passed_gte(fnum(observed.get("fill_rate")), fnum(thresholds.get("min_fill_rate"), 0.7)):
        blockers.append("fill_rate_below_min")
    if not passed_gte(fnum(observed.get("rescue_per_candidate")), fnum(thresholds.get("min_rescue_per_candidate"), 0.25)):
        blockers.append("rescue_per_candidate_below_min")
    if not passed_gte(fnum(observed.get("rescue_per_fill")), fnum(thresholds.get("min_rescue_per_fill"), 0.3)):
        blockers.append("rescue_per_fill_below_min")
    if not passed_gte(fnum(observed.get("pair_pnl")), fnum(thresholds.get("min_pair_pnl"), 0.0)):
        blockers.append("pair_pnl_below_min")
    if not passed_lte(fnum(observed.get("accepted_l1_age_ms_max")), fnum(thresholds.get("max_accepted_l1_age_ms"), 1000.0)):
        blockers.append("accepted_l1_age_above_max")
    if not passed_lte(fnum(observed.get("rescue_l1_age_ms_max")), fnum(thresholds.get("max_rescue_l1_age_ms"), 50.0)):
        blockers.append("rescue_l1_age_above_max")

    source_blocks = fnum(observed.get("source_blocks"), 0.0) or 0.0
    source_decision = body(source_audit, "decision")
    source_absorbed = (
        is_keep(source_audit)
        and bool(source_decision.get("source_caveat_absorbed") or status(source_audit).endswith("_NO_SOURCE_BLOCKS_LOCAL_ONLY"))
        and not bool(source_decision.get("accepted_or_rescue_source_contamination"))
    )
    if source_blocks > (fnum(thresholds.get("max_source_blocks"), 0.0) or 0.0) and not source_absorbed:
        blockers.append("source_blocks_not_absorbed")
    return (not blockers), blockers


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    observed = card["observed"]
    density = card["density"]
    residual = card["residual_gates"]
    comparisons = card["comparisons"]
    lines = [
        "# Xuan Tail Density Rebalance Result",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- density_recovered: `{decision['density_recovered']}`",
        f"- source_caveat_absorbed: `{decision['source_caveat_absorbed']}`",
        f"- residual_capacity_clear: `{decision['residual_capacity_clear']}`",
        f"- same_profile_repeat_allowed: `{decision['same_profile_repeat_allowed']}`",
        f"- cap75_remote_rationale_ready: `{decision['cap75_remote_rationale_ready']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- next_action: `{decision['next_action']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) or 'none'}`",
        "",
        "## Observed",
        "",
        f"- accepted/fills/rescues: `{observed['accepted_actions']}` / `{observed['queue_supported_fills']}` / `{observed['strict_rescue_closes']}`",
        f"- pair_pnl/ROI: `{observed['pair_pnl']}` / `{observed['roi_on_filled_cost']}`",
        f"- residual qty/cost: `{observed['residual_qty']}` / `{observed['residual_cost']}`",
        f"- residual qty/cost share: `{observed['residual_qty_share']}` / `{observed['residual_cost_share']}`",
        f"- residual cost to pair qty: `{observed['residual_cost_to_pair_qty']}`",
        f"- accepted/rescue L1 age max ms: `{observed['accepted_l1_age_ms_max']}` / `{observed['rescue_l1_age_ms_max']}`",
        "",
        "## Density",
        "",
        f"- rescue_per_candidate/rescue_per_fill: `{density['rescue_per_candidate']}` / `{density['rescue_per_fill']}`",
        f"- fill_rate: `{density['fill_rate']}`",
        f"- raw_source_blocks/effective_source_blocks: `{density['raw_source_blocks']}` / `{density['effective_source_blocks']}`",
        "",
        "## Residual Gates",
        "",
    ]
    for name, gate in residual.items():
        lines.append(
            f"- {name}: actual `{gate['actual']}` threshold `{gate['threshold']}` passed `{gate['passed']}` gap `{gate['gap']}`"
        )
    lines.extend(["", "## Comparisons", ""])
    for name, value in comparisons.items():
        lines.append(f"- {name}: `{value}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Lowering the low-lot rescue threshold recovered rescue density.",
            "- The same profile is not a capacity path because residual qty/cost regressed and current-stage residual gates remain blocked.",
            "- Next work should keep the recovered density signal but add residual-cost-aware low-lot limits before any further remote sample.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    runtime = load_json(args.runtime_summary_scorecard)
    baseline_runtime = load_json(args.baseline_runtime_scorecard)
    prior_tail_runtime = load_json(args.prior_tail_runtime_scorecard)
    density = load_json(args.density_preflight_scorecard)
    source_audit = load_json(args.source_caveat_audit_scorecard)
    public_benchmark = load_json(args.public_benchmark_scorecard)
    surplus_bridge = load_json(args.surplus_bridge_scorecard)
    tail_gate = load_json(args.tail_mitigation_gate_scorecard)
    capacity_stage = load_json(args.capacity_stage_public_benchmark_scorecard)
    profile = load_json(args.profile_scorecard)

    metrics = body(runtime, "metrics")
    baseline_metrics = body(baseline_runtime, "metrics")
    prior_tail_metrics = body(prior_tail_runtime, "metrics")
    capital_round = body(body(load_json(args.capital_roi_scorecard), "aggregate"), "round_roi")
    density_observed = body(density, "observed")
    targets = residual_targets(tail_gate, capacity_stage)
    source_decision = body(source_audit, "decision")
    source_absorbed = (
        is_keep(source_audit)
        and not bool(source_decision.get("accepted_or_rescue_source_contamination"))
        and (
            bool(source_decision.get("source_caveat_absorbed"))
            or status(source_audit).endswith("_NO_SOURCE_BLOCKS_LOCAL_ONLY")
        )
    )

    observed = {
        "accepted_actions": fnum(metrics.get("accepted_actions")),
        "queue_supported_fills": fnum(metrics.get("queue_supported_fills")),
        "strict_rescue_closes": fnum(metrics.get("strict_rescue_closes")),
        "strict_rescue_qty": fnum(metrics.get("strict_rescue_qty")),
        "pair_pnl": fnum(metrics.get("pair_pnl")),
        "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
        "filled_qty": fnum(metrics.get("filled_qty")),
        "filled_cost": fnum(metrics.get("filled_cost")),
        "residual_qty": fnum(metrics.get("residual_qty")),
        "residual_cost": fnum(metrics.get("residual_cost")),
        "residual_qty_share": fnum(metrics.get("residual_qty_share")),
        "residual_cost_share": fnum(metrics.get("residual_cost_share")),
        "residual_cost_to_pair_qty": fnum(capital_round.get("residual_cost_to_pair_qty")),
        "worst_case_pair_pnl_if_residual_zero": fnum(capital_round.get("worst_case_pair_pnl_if_residual_zero")),
        "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max")),
        "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
        "strict_rescue_source_blocks": fnum(metrics.get("strict_rescue_source_blocks"), 0.0),
        "rescue_net_pair_cost_max": fnum(metrics.get("rescue_net_pair_cost_max")),
    }
    density_summary = {
        "candidate_rows": fnum(density_observed.get("candidate_rows")),
        "fill_rate": fnum(density_observed.get("fill_rate")),
        "rescue_per_candidate": fnum(density_observed.get("rescue_per_candidate")),
        "rescue_per_fill": fnum(density_observed.get("rescue_per_fill")),
        "strict_rescue_or_salvage_rows": fnum(density_observed.get("strict_rescue_or_salvage_rows")),
        "raw_source_blocks": fnum(density_observed.get("source_blocks"), 0.0),
        "effective_source_blocks": 0.0 if source_absorbed else fnum(density_observed.get("source_blocks"), 0.0),
    }
    density_clear, density_blockers = density_effective_clear(density, source_audit)
    residual_gates = {
        "tail_residual_cost_hard": {
            "actual": observed["residual_cost"],
            "threshold": targets["max_residual_cost_for_remote_hard"],
            "passed": passed_lte(observed["residual_cost"], targets["max_residual_cost_for_remote_hard"]),
        },
        "tail_residual_qty_hard": {
            "actual": observed["residual_qty"],
            "threshold": targets["max_residual_qty_for_remote_hard"],
            "passed": passed_lte(observed["residual_qty"], targets["max_residual_qty_for_remote_hard"]),
        },
        "tail_residual_qty_public_target": {
            "actual": observed["residual_qty"],
            "threshold": targets["max_residual_qty_for_public_target"],
            "passed": passed_lte(observed["residual_qty"], targets["max_residual_qty_for_public_target"]),
        },
        "capacity_residual_qty_share": {
            "actual": observed["residual_qty_share"],
            "threshold": targets["max_residual_qty_share"],
            "passed": passed_lte(observed["residual_qty_share"], targets["max_residual_qty_share"]),
        },
        "capacity_residual_cost_share": {
            "actual": observed["residual_cost_share"],
            "threshold": targets["max_residual_cost_share"],
            "passed": passed_lte(observed["residual_cost_share"], targets["max_residual_cost_share"]),
        },
        "capacity_residual_cost_to_pair_qty": {
            "actual": observed["residual_cost_to_pair_qty"],
            "threshold": targets["max_residual_cost_to_pair_qty"],
            "passed": passed_lte(observed["residual_cost_to_pair_qty"], targets["max_residual_cost_to_pair_qty"]),
        },
        "public_review_residual_rate_hard": {
            "actual": observed["residual_qty_share"],
            "threshold": targets["public_review_residual_rate_hard"],
            "passed": passed_lte(observed["residual_qty_share"], targets["public_review_residual_rate_hard"]),
        },
    }
    for gate in residual_gates.values():
        actual = gate["actual"]
        threshold = gate["threshold"]
        gate["gap"] = round(max(0.0, (actual or 0.0) - (threshold or 0.0)), 6) if actual is not None and threshold is not None else None

    residual_blockers = [name for name, gate in residual_gates.items() if not gate["passed"]]
    quality_blockers = []
    if not is_keep(runtime):
        quality_blockers.append("runtime_summary_not_keep")
    if (observed["strict_rescue_source_blocks"] or 0.0) > 0:
        quality_blockers.append("strict_rescue_source_blocks_above_zero")
    if not is_keep(surplus_bridge):
        quality_blockers.append("surplus_bridge_not_keep")
    if not (status(public_benchmark).startswith("KEEP") or body(public_benchmark, "decision").get("shadow_review_compatible_via_bridge")):
        quality_blockers.append("public_benchmark_not_bridge_compatible")

    comparisons = {
        "strict_rescue_closes_delta_vs_2041_baseline": diff(
            observed["strict_rescue_closes"], fnum(baseline_metrics.get("strict_rescue_closes"))
        ),
        "residual_qty_delta_vs_2041_baseline": diff(observed["residual_qty"], fnum(baseline_metrics.get("residual_qty"))),
        "residual_cost_delta_vs_2041_baseline": diff(
            observed["residual_cost"], fnum(baseline_metrics.get("residual_cost"))
        ),
        "pair_pnl_delta_vs_2041_baseline": diff(observed["pair_pnl"], fnum(baseline_metrics.get("pair_pnl"))),
        "strict_rescue_closes_delta_vs_0432_tail_mitigation": diff(
            observed["strict_rescue_closes"], fnum(prior_tail_metrics.get("strict_rescue_closes"))
        ),
        "residual_qty_delta_vs_0432_tail_mitigation": diff(
            observed["residual_qty"], fnum(prior_tail_metrics.get("residual_qty"))
        ),
        "residual_cost_delta_vs_0432_tail_mitigation": diff(
            observed["residual_cost"], fnum(prior_tail_metrics.get("residual_cost"))
        ),
        "pair_pnl_delta_vs_0432_tail_mitigation": diff(observed["pair_pnl"], fnum(prior_tail_metrics.get("pair_pnl"))),
    }
    density_recovered = density_clear and observed["strict_rescue_closes"] is not None and observed["strict_rescue_closes"] >= 7
    residual_capacity_clear = not residual_blockers
    residual_regressed_vs_baseline = (
        (comparisons["residual_qty_delta_vs_2041_baseline"] or 0.0) > 0
        or (comparisons["residual_cost_delta_vs_2041_baseline"] or 0.0) > 0
    )
    residual_regressed_vs_prior_tail = (
        (comparisons["residual_qty_delta_vs_0432_tail_mitigation"] or 0.0) > 0
        or (comparisons["residual_cost_delta_vs_0432_tail_mitigation"] or 0.0) > 0
    )

    hard_blockers = density_blockers + quality_blockers + residual_blockers
    if density_recovered and residual_capacity_clear and not quality_blockers:
        card_status = "KEEP_SHADOW_REVIEW_TAIL_DENSITY_REBALANCE_RESULT_CLEAR_LOCAL_ONLY"
        next_action = "recheck_capacity_stage_gate_before_any_future_capacity_ladder_sample"
    elif density_recovered:
        card_status = "UNKNOWN_SHADOW_REVIEW_TAIL_DENSITY_REBALANCE_RESULT_DENSITY_RECOVERED_RESIDUAL_REGRESSED_LOCAL_ONLY"
        next_action = "do_not_repeat_same_profile; design_residual_cost_aware_low_lot_rescue_cap_or_midpoint_profile_locally"
    else:
        card_status = "DISCARD_SHADOW_REVIEW_TAIL_DENSITY_REBALANCE_RESULT_DENSITY_NOT_RECOVERED_LOCAL_ONLY"
        next_action = "discard_low_lot_rebalance_profile_for_capacity_path"

    card = {
        "artifact": "xuan_shadow_review_tail_density_rebalance_result_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_tail_density_rebalance_result_scorer.py",
        "status": card_status,
        "inputs": {
            "runtime_summary_scorecard": args.runtime_summary_scorecard,
            "baseline_runtime_scorecard": args.baseline_runtime_scorecard,
            "prior_tail_runtime_scorecard": args.prior_tail_runtime_scorecard,
            "density_preflight_scorecard": args.density_preflight_scorecard,
            "source_caveat_audit_scorecard": args.source_caveat_audit_scorecard,
            "public_benchmark_scorecard": args.public_benchmark_scorecard,
            "surplus_bridge_scorecard": args.surplus_bridge_scorecard,
            "tail_mitigation_gate_scorecard": args.tail_mitigation_gate_scorecard,
            "capacity_stage_public_benchmark_scorecard": args.capacity_stage_public_benchmark_scorecard,
            "profile_scorecard": args.profile_scorecard,
        },
        "source_statuses": {
            "runtime_summary": status(runtime),
            "density_preflight": status(density),
            "source_caveat_audit": status(source_audit),
            "public_benchmark": status(public_benchmark),
            "surplus_bridge": status(surplus_bridge),
            "capacity_stage_public_benchmark": status(capacity_stage),
            "profile": status(profile),
        },
        "decision": {
            "density_recovered": density_recovered,
            "source_caveat_absorbed": source_absorbed,
            "residual_capacity_clear": residual_capacity_clear,
            "residual_regressed_vs_2041_baseline": residual_regressed_vs_baseline,
            "residual_regressed_vs_0432_tail_mitigation": residual_regressed_vs_prior_tail,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "future_remote_allowed_by_this_result": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "hard_blockers": hard_blockers,
            "density_blockers": density_blockers,
            "quality_blockers": quality_blockers,
            "residual_blockers": residual_blockers,
            "next_action": next_action,
        },
        "observed": observed,
        "density": density_summary,
        "targets": targets,
        "residual_gates": residual_gates,
        "comparisons": comparisons,
        "profile_excerpt": {
            "tail_mitigation_hypothesis": body(profile, "profile").get("tail_mitigation_hypothesis"),
            "salvage_min_lot_cost": body(profile, "profile").get("salvage_min_lot_cost"),
            "surplus_budget_max_abs_unpaired_cost": body(profile, "profile").get("surplus_budget_max_abs_unpaired_cost"),
            "strict_rescue_surplus_net_cap": body(profile, "profile").get("strict_rescue_surplus_net_cap"),
            "target_qty": body(profile, "profile").get("target_qty"),
            "imbalance_qty_cap": body(profile, "profile").get("imbalance_qty_cap"),
        },
        "interpretation": {
            "density_signal": "low_lot_rescue_threshold_recovered_strict_rescue_density" if density_recovered else "density_not_recovered",
            "capacity_signal": "blocked_by_residual_regression" if not residual_capacity_clear else "residual_capacity_clear",
            "do_not_expand_capacity": True,
            "do_not_repeat_same_profile": True,
            "do_not_relax_source_or_economic_gates": True,
            "suggested_local_next_step": "test a midpoint/residual-cost-aware low-lot gate locally before any future bounded dry-run rationale",
        },
    }
    return card


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-summary-scorecard", required=True)
    parser.add_argument("--baseline-runtime-scorecard", required=True)
    parser.add_argument("--prior-tail-runtime-scorecard", required=True)
    parser.add_argument("--density-preflight-scorecard", required=True)
    parser.add_argument("--source-caveat-audit-scorecard", required=True)
    parser.add_argument("--public-benchmark-scorecard", required=True)
    parser.add_argument("--surplus-bridge-scorecard", required=True)
    parser.add_argument("--tail-mitigation-gate-scorecard", required=True)
    parser.add_argument("--capacity-stage-public-benchmark-scorecard", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
    parser.add_argument("--profile-scorecard", required=True)
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
