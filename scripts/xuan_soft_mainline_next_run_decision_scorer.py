#!/usr/bin/env python3
"""Decide whether the soft-mainline no-cancel lane deserves another run.

This local-only scorer combines full-window density and early-prefix density
scorecards into one operational decision for the next heartbeat. It does not
launch remote jobs, does not approve shadow, and does not treat density as
promotion evidence.
"""

from __future__ import annotations

import argparse
import json
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


def status_is_keep(card: dict[str, Any]) -> bool:
    return str(card.get("status") or "").startswith("KEEP")


def status_is_blocked(card: dict[str, Any]) -> bool:
    return str(card.get("status") or "").startswith("BLOCKED")


def summarize_window(prefix: dict[str, Any], density: dict[str, Any], label: str) -> dict[str, Any]:
    prefix_decision = prefix.get("decision") if isinstance(prefix.get("decision"), dict) else {}
    density_decision = density.get("decision") if isinstance(density.get("decision"), dict) else {}
    final_prefix = prefix.get("final_prefix") if isinstance(prefix.get("final_prefix"), dict) else {}
    density_observed = density.get("observed") if isinstance(density.get("observed"), dict) else {}
    return {
        "label": label,
        "prefix_status": prefix.get("status"),
        "density_status": density.get("status"),
        "prefix_pass": status_is_keep(prefix),
        "density_pass": status_is_keep(density),
        "prefix_blocked": status_is_blocked(prefix),
        "density_blocked": status_is_blocked(density),
        "earliest_prefix_pass_minutes": prefix_decision.get("earliest_pass_minutes"),
        "prefix_hard_blockers": prefix_decision.get("hard_blockers"),
        "density_hard_blockers": density_decision.get("hard_blockers"),
        "final_prefix": {
            "candidate_rows": final_prefix.get("candidate_rows"),
            "queue_supported_fills": final_prefix.get("queue_supported_fills"),
            "strict_rescue_or_salvage_rows": final_prefix.get("strict_rescue_or_salvage_rows"),
            "fill_rate": final_prefix.get("fill_rate"),
            "rescue_per_candidate": final_prefix.get("rescue_per_candidate"),
            "rescue_per_fill": final_prefix.get("rescue_per_fill"),
            "source_blocks": final_prefix.get("source_blocks"),
        },
        "density_observed": {
            "candidate_rows": density_observed.get("candidate_rows"),
            "accepted_actions": density_observed.get("accepted_actions"),
            "queue_supported_fills": density_observed.get("queue_supported_fills"),
            "strict_rescue_or_salvage_rows": density_observed.get("strict_rescue_or_salvage_rows"),
            "fill_rate": density_observed.get("fill_rate"),
            "rescue_per_candidate": density_observed.get("rescue_per_candidate"),
            "rescue_per_fill": density_observed.get("rescue_per_fill"),
            "pair_pnl": density_observed.get("pair_pnl"),
            "residual_qty_share": density_observed.get("residual_qty_share"),
            "residual_cost_share": density_observed.get("residual_cost_share"),
            "source_blocks": density_observed.get("source_blocks"),
        },
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    latest_prefix = load_json(args.latest_prefix_scorecard)
    latest_density = load_json(args.latest_density_scorecard)
    reference_prefix = load_json(args.reference_prefix_scorecard)
    reference_density = load_json(args.reference_density_scorecard)
    latest = summarize_window(latest_prefix, latest_density, "latest")
    reference = summarize_window(reference_prefix, reference_density, "reference") if reference_prefix else None

    latest_ready = bool(latest["prefix_pass"] or latest["density_pass"])
    reference_ready = bool(reference and (reference["prefix_pass"] or reference["density_pass"]))
    hard_blockers: list[str] = []
    if not latest_ready:
        hard_blockers.append("latest_window_density_not_ready")
    if latest["prefix_blocked"]:
        hard_blockers.append("latest_prefix_density_blocked")
    if latest["density_blocked"]:
        hard_blockers.append("latest_full_window_density_blocked")

    if latest_ready:
        status = "KEEP_SOFT_MAINLINE_NEXT_RUN_DECISION_DENSITY_READY_LOCAL_ONLY"
        next_action = "prepare_one_bounded_no_cancel_soft_mainline_remote_on_future_heartbeat"
        recommendation = "latest density evidence supports considering one future 1800s dry-run after manifest verification"
    elif reference_ready:
        status = "BLOCKED_SOFT_MAINLINE_NEXT_RUN_DECISION_WAIT_FOR_FRESH_DENSITY_SIGNAL_LOCAL_ONLY"
        next_action = "do_not_launch_full_remote_until_fresh_prefix_or_preflight_density_passes"
        recommendation = "reference window proves the lane can work, but the latest window is weak; wait for fresh density signal or keep local"
    else:
        status = "BLOCKED_SOFT_MAINLINE_NEXT_RUN_DECISION_NO_DENSITY_EVIDENCE_LOCAL_ONLY"
        next_action = "stay_local_and_refine_scorers_or_profile"
        recommendation = "no provided window has density evidence strong enough to justify another remote sample"

    return {
        "artifact": "xuan_soft_mainline_next_run_decision_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_next_run_decision_scorer.py",
        "status": status,
        "inputs": {
            "latest_prefix_scorecard": str(Path(args.latest_prefix_scorecard).expanduser()),
            "latest_density_scorecard": str(Path(args.latest_density_scorecard).expanduser()),
            "reference_prefix_scorecard": str(Path(args.reference_prefix_scorecard).expanduser())
            if args.reference_prefix_scorecard
            else None,
            "reference_density_scorecard": str(Path(args.reference_density_scorecard).expanduser())
            if args.reference_density_scorecard
            else None,
        },
        "latest_window": latest,
        "reference_window": reference,
        "decision": {
            "next_action": next_action,
            "recommendation": recommendation,
            "same_family_no_cancel_remote_density_worthwhile": latest_ready,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
        },
        "guardrails": [
            "This decision is local planning evidence only.",
            "Density readiness is not shadow approval or promotion evidence.",
            "Do not return to ce25 hard gates as the mainline controller.",
            "Do not re-enable closeability deterioration cancel guard for this lane without a separate local proof.",
            "Any future remote remains PM_DRY_RUN=1, 1800s max, one run per wake, and requires manifest verification.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest-prefix-scorecard", required=True)
    parser.add_argument("--latest-density-scorecard", required=True)
    parser.add_argument("--reference-prefix-scorecard")
    parser.add_argument("--reference-density-scorecard")
    parser.add_argument("--scorecard-json", required=True)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
