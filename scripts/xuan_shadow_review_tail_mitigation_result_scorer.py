#!/usr/bin/env python3
"""Score the tail-mitigated cap25 runtime against residual and quality gates."""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: str) -> dict[str, Any]:
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
    if value is None or value == "":
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


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    o = card["observed"]
    t = card["targets"]
    lines = [
        "# Xuan Tail Mitigation Runtime Result",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- residual_direction_validated: `{d['residual_direction_validated']}`",
        f"- quality_gate_clear: `{d['quality_gate_clear']}`",
        f"- remote_runner_allowed: `{d['remote_runner_allowed']}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Observed",
        "",
        f"- accepted/fills/rescues: `{o['accepted_actions']}` / `{o['queue_supported_fills']}` / `{o['strict_rescue_closes']}`",
        f"- pair_pnl: `{o['pair_pnl']}`",
        f"- residual qty/cost: `{o['residual_qty']}` / `{o['residual_cost']}`",
        f"- residual qty/cost share: `{o['residual_qty_share']}` / `{o['residual_cost_share']}`",
        f"- rescue_per_candidate/fill: `{o['rescue_per_candidate']}` / `{o['rescue_per_fill']}`",
        "",
        "## Targets",
        "",
        f"- residual_cost <= `{t['max_residual_cost_for_remote_hard']}`",
        f"- residual_qty <= `{t['max_residual_qty_for_remote_hard']}`",
        f"- public residual_qty <= `{t['max_residual_qty_for_public_target']}`",
        f"- strict_rescue_closes >= `{t['min_strict_rescue_closes']}`",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    runtime = load_json(args.runtime_summary_scorecard)
    baseline = load_json(args.baseline_runtime_scorecard)
    tail_gate = load_json(args.tail_mitigation_gate_scorecard)
    density = load_json(args.density_preflight_scorecard)
    prefix = load_json(args.density_prefix_scorecard)
    public = load_json(args.public_benchmark_scorecard)
    bridge = load_json(args.surplus_bridge_scorecard)

    metrics = body(runtime, "metrics")
    base_metrics = body(baseline, "metrics")
    targets = body(tail_gate, "mitigation_targets")
    observed = {
        "accepted_actions": fnum(metrics.get("accepted_actions")),
        "queue_supported_fills": fnum(metrics.get("queue_supported_fills")),
        "strict_rescue_closes": fnum(metrics.get("strict_rescue_closes")),
        "strict_rescue_qty": fnum(metrics.get("strict_rescue_qty")),
        "pair_pnl": fnum(metrics.get("pair_pnl")),
        "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
        "residual_qty": fnum(metrics.get("residual_qty")),
        "residual_cost": fnum(metrics.get("residual_cost")),
        "residual_qty_share": fnum(metrics.get("residual_qty_share")),
        "residual_cost_share": fnum(metrics.get("residual_cost_share")),
        "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max")),
        "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max")),
        "strict_rescue_source_blocks": fnum(metrics.get("strict_rescue_source_blocks"), 0.0),
        "rescue_per_candidate": fnum(body(density, "observed").get("rescue_per_candidate")),
        "rescue_per_fill": fnum(body(density, "observed").get("rescue_per_fill")),
    }
    min_targets = {
        "min_accepted_actions": 25.0,
        "min_queue_supported_fills": 20.0,
        "min_strict_rescue_closes": 7.0,
        "min_pair_pnl": 0.0,
        "max_rescue_l1_age_ms": 50.0,
        "max_strict_rescue_source_blocks": 0.0,
        "max_residual_cost_for_remote_hard": fnum(targets.get("max_residual_cost_for_remote_hard")),
        "max_residual_qty_for_remote_hard": fnum(targets.get("max_residual_qty_for_remote_hard")),
        "max_residual_qty_for_public_target": fnum(targets.get("max_residual_qty_for_public_target")),
    }
    comparisons = {
        "residual_qty_delta_vs_baseline": diff(observed["residual_qty"], fnum(base_metrics.get("residual_qty"))),
        "residual_cost_delta_vs_baseline": diff(observed["residual_cost"], fnum(base_metrics.get("residual_cost"))),
        "residual_qty_share_delta_vs_baseline": diff(observed["residual_qty_share"], fnum(base_metrics.get("residual_qty_share"))),
        "residual_cost_share_delta_vs_baseline": diff(observed["residual_cost_share"], fnum(base_metrics.get("residual_cost_share"))),
        "strict_rescue_closes_delta_vs_baseline": diff(observed["strict_rescue_closes"], fnum(base_metrics.get("strict_rescue_closes"))),
        "pair_pnl_delta_vs_baseline": diff(observed["pair_pnl"], fnum(base_metrics.get("pair_pnl"))),
    }
    residual_cost_gap = (
        observed["residual_cost"] - min_targets["max_residual_cost_for_remote_hard"]
        if observed["residual_cost"] is not None and min_targets["max_residual_cost_for_remote_hard"] is not None
        else None
    )
    residual_gates = {
        "residual_qty_hard_pass": observed["residual_qty"] is not None
        and observed["residual_qty"] <= min_targets["max_residual_qty_for_remote_hard"],
        "residual_qty_public_pass": observed["residual_qty"] is not None
        and observed["residual_qty"] <= min_targets["max_residual_qty_for_public_target"],
        "residual_cost_hard_pass": observed["residual_cost"] is not None
        and observed["residual_cost"] <= min_targets["max_residual_cost_for_remote_hard"],
        "residual_cost_hard_gap": round(residual_cost_gap, 6) if residual_cost_gap is not None else None,
    }
    quality_blockers = []
    if (observed["accepted_actions"] or 0) < min_targets["min_accepted_actions"]:
        quality_blockers.append("accepted_actions_below_min")
    if (observed["queue_supported_fills"] or 0) < min_targets["min_queue_supported_fills"]:
        quality_blockers.append("fills_below_min")
    if (observed["strict_rescue_closes"] or 0) < min_targets["min_strict_rescue_closes"]:
        quality_blockers.append("strict_rescue_closes_below_min")
    if (observed["pair_pnl"] or 0) < min_targets["min_pair_pnl"]:
        quality_blockers.append("pair_pnl_below_min")
    if (observed["rescue_l1_age_ms_max"] or 0) > min_targets["max_rescue_l1_age_ms"]:
        quality_blockers.append("rescue_l1_age_above_max")
    if (observed["strict_rescue_source_blocks"] or 0) > min_targets["max_strict_rescue_source_blocks"]:
        quality_blockers.append("strict_rescue_source_blocks_above_max")
    if not is_keep(density):
        quality_blockers.append("density_preflight_not_keep")
    if not is_keep(prefix):
        quality_blockers.append("density_prefix_not_keep")
    if not is_keep(bridge):
        quality_blockers.append("surplus_bridge_not_keep")

    residual_direction = (
        comparisons["residual_qty_delta_vs_baseline"] is not None
        and comparisons["residual_cost_delta_vs_baseline"] is not None
        and comparisons["residual_qty_delta_vs_baseline"] < 0
        and comparisons["residual_cost_delta_vs_baseline"] < 0
        and residual_gates["residual_qty_hard_pass"]
        and is_keep(public)
    )
    full_clear = residual_direction and residual_gates["residual_cost_hard_pass"] and not quality_blockers
    if full_clear:
        card_status = "KEEP_SHADOW_REVIEW_TAIL_MITIGATION_RESULT_CLEAR_LOCAL_ONLY"
        next_action = "consider_capacity_stage_gate_recheck_before_any_capacity_expansion"
    elif residual_direction:
        card_status = "UNKNOWN_SHADOW_REVIEW_TAIL_MITIGATION_RESULT_RESIDUAL_IMPROVED_QUALITY_BLOCKED_LOCAL_ONLY"
        next_action = "do_not_repeat_blindly; design_next_local_profile_to_preserve_rescue_density_while_holding_low_residual"
    else:
        card_status = "DISCARD_SHADOW_REVIEW_TAIL_MITIGATION_RESULT_NO_RESIDUAL_EDGE_LOCAL_ONLY"
        next_action = "discard_tail_mitigation_profile_for_capacity_path"

    return {
        "artifact": "xuan_shadow_review_tail_mitigation_result_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_tail_mitigation_result_scorer.py",
        "status": card_status,
        "inputs": {
            "runtime_summary_scorecard": args.runtime_summary_scorecard,
            "baseline_runtime_scorecard": args.baseline_runtime_scorecard,
            "tail_mitigation_gate_scorecard": args.tail_mitigation_gate_scorecard,
            "density_preflight_scorecard": args.density_preflight_scorecard,
            "density_prefix_scorecard": args.density_prefix_scorecard,
            "public_benchmark_scorecard": args.public_benchmark_scorecard,
            "surplus_bridge_scorecard": args.surplus_bridge_scorecard,
        },
        "source_statuses": {
            "runtime": status(runtime),
            "density_preflight": status(density),
            "density_prefix": status(prefix),
            "public_benchmark": status(public),
            "surplus_bridge": status(bridge),
        },
        "decision": {
            "residual_direction_validated": residual_direction,
            "residual_cost_hard_clear": residual_gates["residual_cost_hard_pass"],
            "quality_gate_clear": not quality_blockers,
            "shadow_or_capacity_ready": full_clear,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "hard_blockers": [] if full_clear else quality_blockers + (
                [] if residual_gates["residual_cost_hard_pass"] else ["residual_cost_hard_target_missed"]
            ),
            "next_action": next_action,
        },
        "observed": observed,
        "targets": min_targets,
        "residual_gates": residual_gates,
        "comparisons_vs_baseline": comparisons,
        "interpretation": {
            "short_text": "Tail mitigation lowered residual qty/cost and cleared public residual comparison, but missed rescue-density and strict-rescue quality gates.",
            "do_not_expand_capacity": not full_clear,
            "do_not_blind_repeat_same_profile": not full_clear,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-summary-scorecard", required=True)
    parser.add_argument("--baseline-runtime-scorecard", required=True)
    parser.add_argument("--tail-mitigation-gate-scorecard", required=True)
    parser.add_argument("--density-preflight-scorecard", required=True)
    parser.add_argument("--density-prefix-scorecard", required=True)
    parser.add_argument("--public-benchmark-scorecard", required=True)
    parser.add_argument("--surplus-bridge-scorecard", required=True)
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
    return 0 if not card["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
