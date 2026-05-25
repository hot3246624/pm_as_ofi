#!/usr/bin/env python3
"""Build the local run-trigger policy for soft-mainline no-cancel samples.

This script turns the current autoresearch lesson into a machine-readable
policy: another 30 minute no-order run is useful only when the lane shows a
fresh density signal. It does not launch remote jobs, approve shadow, or
change strategy economics.
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


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    num = fnum(value)
    return int(num) if num is not None else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def is_keep(card: dict[str, Any]) -> bool:
    return status(card).startswith("KEEP")


def is_blocked(card: dict[str, Any]) -> bool:
    return status(card).startswith("BLOCKED")


def is_wait(card: dict[str, Any]) -> bool:
    return status(card).startswith("UNKNOWN") and "WAIT" in status(card)


def latest_history_window(history: dict[str, Any]) -> dict[str, Any]:
    windows = history.get("windows")
    if isinstance(windows, list) and windows:
        latest = windows[-1]
        return latest if isinstance(latest, dict) else {}
    return {}


def observed_from_history(history: dict[str, Any]) -> dict[str, Any]:
    latest = latest_history_window(history)
    observed = latest.get("observed")
    return observed if isinstance(observed, dict) else {}


def observed_from_density(density: dict[str, Any]) -> dict[str, Any]:
    observed = density.get("observed")
    return observed if isinstance(observed, dict) else {}


def next_action_from_cards(
    *,
    density: dict[str, Any],
    prefix: dict[str, Any],
    next_decision: dict[str, Any],
    history: dict[str, Any],
) -> tuple[str, str, list[str]]:
    blockers: list[str] = []
    if is_keep(history) or is_keep(next_decision) or is_keep(density) or is_keep(prefix):
        return (
            "KEEP_SOFT_MAINLINE_RUN_TRIGGER_POLICY_READY_FOR_ONE_BOUNDED_SAMPLE_LOCAL_ONLY",
            "prepare_one_bounded_no_cancel_soft_mainline_remote_on_future_heartbeat_after_verifier",
            blockers,
        )
    if is_wait(next_decision) or is_wait(prefix) or is_wait(density) or is_wait(history):
        blockers.append("waiting_for_min_decision_prefix")
        return (
            "UNKNOWN_SOFT_MAINLINE_RUN_TRIGGER_POLICY_WAIT_FOR_PREFIX_LOCAL_ONLY",
            "wait_for_min_decision_prefix_before_remote_decision",
            blockers,
        )
    if is_blocked(history):
        blockers.append("latest_density_history_weak")
    if is_blocked(next_decision):
        blockers.append("next_run_decision_blocked")
    if is_blocked(prefix):
        blockers.append("latest_prefix_density_blocked")
    if is_blocked(density):
        blockers.append("latest_full_window_density_blocked")
    if not blockers:
        blockers.append("no_fresh_density_signal")
    return (
        "BLOCKED_SOFT_MAINLINE_RUN_TRIGGER_POLICY_WAIT_FOR_FRESH_DENSITY_LOCAL_ONLY",
        "wait_for_fresh_prefix_or_preflight_density_signal_before_remote",
        blockers,
    )


def build_markdown(card: dict[str, Any]) -> str:
    observed = card["latest_observed_density"]
    thresholds = card["trigger_thresholds"]
    decision = card["decision"]
    lines = [
        "# Soft-Mainline Run Trigger Policy",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{decision['next_action']}`",
        "",
        "## Answer",
        "",
        "Running longer is not the current lever. The standard dry-run remains 1800s.",
        "The useful trigger is a fresh no-cancel soft-mainline density signal, visible either in a completed postrun gate or in a prefix-style read after at least 10 minutes.",
        "",
        "## Current Evidence",
        "",
        f"- Latest candidates: `{observed.get('candidate_rows')}`",
        f"- Latest fills: `{observed.get('queue_supported_fills')}`",
        f"- Latest strict rescue closes: `{observed.get('strict_rescue_closes')}`",
        f"- Latest rescue per candidate: `{observed.get('rescue_per_candidate')}`",
        f"- Latest rescue per fill: `{observed.get('rescue_per_fill')}`",
        f"- Latest residual cost share: `{observed.get('residual_cost_share')}`",
        "",
        "## Run Trigger",
        "",
        f"- Prefix age must be at least `{thresholds['min_prefix_minutes']}` minutes before deciding.",
        f"- Candidates >= `{thresholds['min_candidates']}` and accepted actions >= `{thresholds['min_accepted_actions']}`.",
        f"- Fills >= `{thresholds['min_fills']}` and strict rescue closes >= `{thresholds['min_rescue_closes']}`.",
        f"- Fill rate >= `{thresholds['min_fill_rate']}`, rescue/candidate >= `{thresholds['min_rescue_per_candidate']}`, rescue/fill >= `{thresholds['min_rescue_per_fill']}`.",
        f"- Pair PnL >= `{thresholds['min_pair_pnl']}` and residual qty/cost shares <= `{thresholds['max_residual_qty_share']}` / `{thresholds['max_residual_cost_share']}`.",
        "",
        "## Guardrails",
        "",
        "- PM_DRY_RUN=1 only, one remote run per wake, 1800s max.",
        "- Keep no-cancel soft-mainline economics; do not return to ce25 hard gates.",
        "- Do not treat density readiness as shadow approval.",
        "- Do not deploy, restart, or mutate shared services.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    density = load_json(args.latest_density_scorecard)
    prefix = load_json(args.latest_prefix_scorecard)
    next_decision = load_json(args.next_run_decision_scorecard)
    history = load_json(args.density_history_scorecard)
    contrast = load_json(args.window_contrast_scorecard)

    card_status, next_action, hard_blockers = next_action_from_cards(
        density=density,
        prefix=prefix,
        next_decision=next_decision,
        history=history,
    )
    history_summary = history.get("summary") if isinstance(history.get("summary"), dict) else {}
    history_decision = history.get("decision") if isinstance(history.get("decision"), dict) else {}
    next_decision_body = next_decision.get("decision") if isinstance(next_decision.get("decision"), dict) else {}
    contrast_decision = contrast.get("decision") if isinstance(contrast.get("decision"), dict) else {}
    latest_observed = observed_from_history(history) or observed_from_density(density)

    return {
        "artifact": "xuan_soft_mainline_run_trigger_policy_builder",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_run_trigger_policy_builder.py",
        "status": card_status,
        "inputs": {
            "latest_density_scorecard": str(Path(args.latest_density_scorecard).expanduser())
            if args.latest_density_scorecard
            else None,
            "latest_prefix_scorecard": str(Path(args.latest_prefix_scorecard).expanduser())
            if args.latest_prefix_scorecard
            else None,
            "next_run_decision_scorecard": str(Path(args.next_run_decision_scorecard).expanduser())
            if args.next_run_decision_scorecard
            else None,
            "density_history_scorecard": str(Path(args.density_history_scorecard).expanduser())
            if args.density_history_scorecard
            else None,
            "window_contrast_scorecard": str(Path(args.window_contrast_scorecard).expanduser())
            if args.window_contrast_scorecard
            else None,
        },
        "source_statuses": {
            "latest_density": status(density) or None,
            "latest_prefix": status(prefix) or None,
            "next_run_decision": status(next_decision) or None,
            "density_history": status(history) or None,
            "window_contrast": status(contrast) or None,
        },
        "latest_observed_density": {
            "candidate_rows": inum(latest_observed.get("candidate_rows")),
            "accepted_actions": inum(latest_observed.get("accepted_actions")),
            "queue_supported_fills": inum(latest_observed.get("queue_supported_fills")),
            "strict_rescue_closes": inum(latest_observed.get("strict_rescue_closes")),
            "fill_rate": fnum(latest_observed.get("fill_rate")),
            "rescue_per_candidate": fnum(latest_observed.get("rescue_per_candidate")),
            "rescue_per_fill": fnum(latest_observed.get("rescue_per_fill")),
            "pair_pnl": fnum(latest_observed.get("pair_pnl")),
            "residual_qty_share": fnum(latest_observed.get("residual_qty_share")),
            "residual_cost_share": fnum(latest_observed.get("residual_cost_share")),
            "source_blocks": inum(latest_observed.get("source_blocks")),
        },
        "history_summary": {
            "window_count": history_summary.get("window_count"),
            "pass_count": history_summary.get("pass_count"),
            "latest_label": history_summary.get("latest_label"),
            "latest_density_pass": history_summary.get("latest_density_pass"),
            "latest_prefix_pass": history_summary.get("latest_prefix_pass"),
            "trend": history_summary.get("trend"),
        },
        "run_duration_policy": {
            "standard_duration_s": 1800,
            "max_duration_s": 1800,
            "run_longer_recommended": False,
            "reason": (
                "The weak 0621Z window had similar or higher candidate/touch count than 0510Z but much lower rescue "
                "density and worse residual quality, so extra wall time is not the first lever."
            ),
        },
        "stage_policy": {
            "special_clock_stage_required": False,
            "empirical_stage_required": "fresh no-cancel soft-mainline prefix or postrun density pass",
            "minimum_prefix_minutes_before_decision": args.min_prefix_minutes,
            "notes": [
                "The desired stage is active, two-sided, near-expiry rescue density, not a fixed wall-clock phase.",
                "If prefix output is younger than the minimum decision age, wait instead of launching or blocking.",
                "If the latest completed density history is weak, stay local until a fresh density signal appears.",
            ],
        },
        "trigger_thresholds": {
            "min_prefix_minutes": args.min_prefix_minutes,
            "min_candidates": args.min_candidates,
            "min_accepted_actions": args.min_accepted_actions,
            "min_fills": args.min_fills,
            "min_rescue_closes": args.min_rescue_closes,
            "min_fill_rate": args.min_fill_rate,
            "min_rescue_per_candidate": args.min_rescue_per_candidate,
            "min_rescue_per_fill": args.min_rescue_per_fill,
            "min_pair_pnl": args.min_pair_pnl,
            "max_residual_qty_share": args.max_residual_qty_share,
            "max_residual_cost_share": args.max_residual_cost_share,
            "max_source_blocks": args.max_source_blocks,
        },
        "formal_shadow_window_target": {
            "accepted_actions": 25,
            "queue_supported_fills": 20,
            "strict_rescue_closes": 7,
            "min_pair_pnl": 0.0,
            "max_residual_qty_share": 0.35,
            "max_residual_cost_share": 0.30,
            "max_rescue_l1_age_ms": 50,
            "max_source_blocks": 0,
        },
        "autoresearch_loop": [
            "observe current/future exact slugs and no-order events",
            "score prefix density once the minimum prefix age is reached",
            "run exactly one bounded 1800s no-order sample only when density is ready",
            "postrun score runtime, event diagnostics, density, contrast, history, repeat, and shadow readiness",
            "feed the blocker back into the next local scorer or wait state",
        ],
        "decision": {
            "next_action": next_action,
            "recommendation": (
                history_decision.get("next_action")
                or next_decision_body.get("next_action")
                or contrast_decision.get("next_action")
                or next_action
            ),
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
        },
        "guardrails": [
            "This policy is local planning evidence only.",
            "It does not launch remote jobs or approve shadow.",
            "Do not run longer than 1800s to compensate for weak rescue density.",
            "Do not return to ce25 hard gates as the mainline controller.",
            "Do not re-enable closeability deterioration cancel guard for this lane without a separate local proof.",
            "Any future remote remains PM_DRY_RUN=1, one run per wake, and requires manifest verification.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest-density-scorecard")
    parser.add_argument("--latest-prefix-scorecard")
    parser.add_argument("--next-run-decision-scorecard")
    parser.add_argument("--density-history-scorecard")
    parser.add_argument("--window-contrast-scorecard")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-output")
    parser.add_argument("--min-prefix-minutes", type=float, default=10.0)
    parser.add_argument("--min-candidates", type=int, default=20)
    parser.add_argument("--min-accepted-actions", type=int, default=20)
    parser.add_argument("--min-fills", type=int, default=18)
    parser.add_argument("--min-rescue-closes", type=int, default=7)
    parser.add_argument("--min-fill-rate", type=float, default=0.70)
    parser.add_argument("--min-rescue-per-candidate", type=float, default=0.25)
    parser.add_argument("--min-rescue-per-fill", type=float, default=0.30)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-source-blocks", type=int, default=0)
    args = parser.parse_args()

    card = rounded(build(args))
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card))
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
