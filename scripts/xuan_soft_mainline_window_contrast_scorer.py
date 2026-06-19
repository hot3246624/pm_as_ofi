#!/usr/bin/env python3
"""Contrast good and weak soft-mainline no-cancel windows.

This local-only scorer turns the 0510Z-vs-0621Z lesson into a repeatable
scorecard. It does not launch remote jobs, does not approve shadow, and does
not relax economics. Its purpose is narrower: explain whether a weak window
failed because of traffic starvation, fill quality, rescue density, or residual
risk, then decide whether another same-family remote sample needs fresh density
evidence first.
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


def safe_rate(num: float, den: float) -> float | None:
    return num / den if den > 0 else None


def safe_ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or den == 0:
        return None
    return num / den


def pp_delta(new: float | None, old: float | None) -> float | None:
    if new is None or old is None:
        return None
    return new - old


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def get_metrics(runtime: dict[str, Any]) -> dict[str, Any]:
    metrics = runtime.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def get_counts(event: dict[str, Any], key: str) -> dict[str, Any]:
    counts = event.get(key)
    return counts if isinstance(counts, dict) else {}


def extract_window(
    *,
    label: str,
    runtime: dict[str, Any],
    event: dict[str, Any],
    prefix: dict[str, Any],
    runtime_path: str,
    event_path: str,
    prefix_path: str,
) -> dict[str, Any]:
    metrics = get_metrics(runtime)
    event_counts = get_counts(event, "event_counts")
    cancel_reasons = get_counts(event, "cancel_reasons")
    block_reasons = get_counts(event, "block_reason_counts")
    pair_completion_reasons = get_counts(event, "pair_completion_block_reasons")
    closeability_reasons = get_counts(event, "risk_seed_closeability_block_reasons")

    candidates = inum(event_counts.get("candidate"), inum(metrics.get("accepted_actions")))
    fills = inum(event_counts.get("queue_supported_fill"), inum(metrics.get("queue_supported_fills")))
    rescues = inum(event_counts.get("fak_salvage"), inum(metrics.get("strict_rescue_closes")))
    accepted = inum(metrics.get("accepted_actions"), candidates)
    pair_pnl = fnum(metrics.get("pair_pnl"), 0.0) or 0.0
    residual_qty_share = fnum(metrics.get("residual_qty_share"), 0.0) or 0.0
    residual_cost_share = fnum(metrics.get("residual_cost_share"), 0.0) or 0.0

    decision = prefix.get("decision") if isinstance(prefix.get("decision"), dict) else {}
    earliest_prefix = (
        prefix.get("earliest_passing_decision_prefix")
        if isinstance(prefix.get("earliest_passing_decision_prefix"), dict)
        else None
    )
    final_prefix = prefix.get("final_prefix") if isinstance(prefix.get("final_prefix"), dict) else {}

    return {
        "label": label,
        "inputs": {
            "runtime_summary": str(Path(runtime_path).expanduser()),
            "event_diagnostics": str(Path(event_path).expanduser()),
            "prefix_scorecard": str(Path(prefix_path).expanduser()),
        },
        "status": {
            "runtime": runtime.get("status"),
            "prefix": prefix.get("status"),
        },
        "runtime": {
            "accepted_actions": accepted,
            "queue_supported_fills": fills,
            "strict_rescue_closes": rescues,
            "strict_rescue_qty": fnum(metrics.get("strict_rescue_qty"), 0.0) or 0.0,
            "rescue_slugs": inum(metrics.get("rescue_slugs")),
            "filled_cost": fnum(metrics.get("filled_cost"), 0.0) or 0.0,
            "pair_pnl": pair_pnl,
            "roi_on_filled_cost": fnum(metrics.get("roi_on_filled_cost")),
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "rescue_net_pair_cost_max": fnum(metrics.get("rescue_net_pair_cost_max")),
            "accepted_l1_age_ms_max": fnum(metrics.get("accepted_l1_age_ms_max"), 0.0) or 0.0,
            "rescue_l1_age_ms_max": fnum(metrics.get("rescue_l1_age_ms_max"), 0.0) or 0.0,
            "strict_rescue_source_blocks": inum(metrics.get("strict_rescue_source_blocks")),
        },
        "density": {
            "candidate_rows": candidates,
            "touch_rows": inum(event_counts.get("touch")),
            "activation_block_rows": inum(event_counts.get("activation_block")),
            "pair_completion_block_rows": inum(event_counts.get("pair_completion_block")),
            "risk_seed_closeability_block_rows": inum(event_counts.get("risk_seed_closeability_block")),
            "internal_pair_rows": inum(event_counts.get("internal_pair")),
            "cancel_rows": inum(event_counts.get("cancel")),
            "ttl_cancel_rows": inum(cancel_reasons.get("ttl")),
            "fill_rate": safe_rate(fills, candidates),
            "rescue_per_candidate": safe_rate(rescues, candidates),
            "rescue_per_fill": safe_rate(rescues, fills),
        },
        "event_counts": event_counts,
        "block_reasons": {
            "all": block_reasons,
            "pair_completion": pair_completion_reasons,
            "risk_seed_closeability": closeability_reasons,
            "cancel": cancel_reasons,
        },
        "prefix": {
            "run_complete": prefix.get("run_complete"),
            "observed_duration_minutes": fnum(prefix.get("observed_duration_minutes")),
            "earliest_pass_minutes": decision.get("earliest_pass_minutes"),
            "hard_blockers": decision.get("hard_blockers"),
            "earliest_passing_decision_prefix": earliest_prefix,
            "final_prefix": final_prefix,
        },
    }


def prefix_at(card: dict[str, Any], minutes: float) -> dict[str, Any] | None:
    prefixes = card.get("prefixes")
    if not isinstance(prefixes, list):
        return None
    best: dict[str, Any] | None = None
    best_dist: float | None = None
    for item in prefixes:
        if not isinstance(item, dict):
            continue
        elapsed = fnum(item.get("elapsed_minutes"))
        if elapsed is None:
            continue
        dist = abs(elapsed - minutes)
        if best_dist is None or dist < best_dist:
            best = item
            best_dist = dist
    if best_dist is None or best_dist > 1e-9:
        return None
    return best


def metric_delta(reference: dict[str, Any], latest: dict[str, Any], section: str, key: str) -> dict[str, Any]:
    old = fnum(reference.get(section, {}).get(key))
    new = fnum(latest.get(section, {}).get(key))
    return {
        "reference": old,
        "latest": new,
        "delta": pp_delta(new, old),
        "latest_to_reference_ratio": safe_ratio(new, old),
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    reference_runtime = load_json(args.reference_runtime_summary)
    reference_event = load_json(args.reference_event_diagnostics)
    reference_prefix = load_json(args.reference_prefix_scorecard)
    latest_runtime = load_json(args.latest_runtime_summary)
    latest_event = load_json(args.latest_event_diagnostics)
    latest_prefix = load_json(args.latest_prefix_scorecard)

    reference = extract_window(
        label=args.reference_label,
        runtime=reference_runtime,
        event=reference_event,
        prefix=reference_prefix,
        runtime_path=args.reference_runtime_summary,
        event_path=args.reference_event_diagnostics,
        prefix_path=args.reference_prefix_scorecard,
    )
    latest = extract_window(
        label=args.latest_label,
        runtime=latest_runtime,
        event=latest_event,
        prefix=latest_prefix,
        runtime_path=args.latest_runtime_summary,
        event_path=args.latest_event_diagnostics,
        prefix_path=args.latest_prefix_scorecard,
    )

    deltas = {
        "candidate_rows": metric_delta(reference, latest, "density", "candidate_rows"),
        "touch_rows": metric_delta(reference, latest, "density", "touch_rows"),
        "queue_supported_fills": metric_delta(reference, latest, "runtime", "queue_supported_fills"),
        "strict_rescue_closes": metric_delta(reference, latest, "runtime", "strict_rescue_closes"),
        "rescue_slugs": metric_delta(reference, latest, "runtime", "rescue_slugs"),
        "fill_rate": metric_delta(reference, latest, "density", "fill_rate"),
        "rescue_per_candidate": metric_delta(reference, latest, "density", "rescue_per_candidate"),
        "rescue_per_fill": metric_delta(reference, latest, "density", "rescue_per_fill"),
        "pair_pnl": metric_delta(reference, latest, "runtime", "pair_pnl"),
        "residual_qty_share": metric_delta(reference, latest, "runtime", "residual_qty_share"),
        "residual_cost_share": metric_delta(reference, latest, "runtime", "residual_cost_share"),
        "pair_completion_block_rows": metric_delta(reference, latest, "density", "pair_completion_block_rows"),
        "risk_seed_closeability_block_rows": metric_delta(
            reference, latest, "density", "risk_seed_closeability_block_rows"
        ),
        "ttl_cancel_rows": metric_delta(reference, latest, "density", "ttl_cancel_rows"),
    }

    reference_prefix_at_decision = prefix_at(reference_prefix, args.decision_prefix_minutes)
    latest_prefix_at_decision = prefix_at(latest_prefix, args.decision_prefix_minutes)
    prefix_contrast = {
        "decision_prefix_minutes": args.decision_prefix_minutes,
        "reference_prefix": reference_prefix_at_decision,
        "latest_prefix": latest_prefix_at_decision,
        "reference_earliest_pass_minutes": reference["prefix"]["earliest_pass_minutes"],
        "latest_earliest_pass_minutes": latest["prefix"]["earliest_pass_minutes"],
    }

    hard_blockers: list[str] = []
    if (
        deltas["candidate_rows"]["latest"] is not None
        and deltas["candidate_rows"]["reference"] is not None
        and deltas["candidate_rows"]["latest"] >= deltas["candidate_rows"]["reference"]
    ):
        hard_blockers.append("latest_not_traffic_starved_candidate_count_not_lower")
    if (deltas["strict_rescue_closes"]["latest_to_reference_ratio"] or 0.0) < args.min_rescue_ratio:
        hard_blockers.append("strict_rescue_close_ratio_collapsed")
    if (deltas["rescue_per_candidate"]["latest_to_reference_ratio"] or 0.0) < args.min_rescue_density_ratio:
        hard_blockers.append("rescue_per_candidate_ratio_collapsed")
    if (deltas["rescue_per_fill"]["latest_to_reference_ratio"] or 0.0) < args.min_rescue_density_ratio:
        hard_blockers.append("rescue_per_fill_ratio_collapsed")
    if (deltas["fill_rate"]["latest_to_reference_ratio"] or 0.0) < args.min_fill_rate_ratio:
        hard_blockers.append("fill_rate_ratio_weakened")
    if (deltas["residual_cost_share"]["delta"] or 0.0) > args.max_residual_cost_share_worsening:
        hard_blockers.append("residual_cost_share_worsened")
    if (deltas["residual_qty_share"]["delta"] or 0.0) > args.max_residual_qty_share_worsening:
        hard_blockers.append("residual_qty_share_worsened")
    if reference["prefix"]["earliest_pass_minutes"] is not None and latest["prefix"]["earliest_pass_minutes"] is None:
        hard_blockers.append("latest_lost_prefix_density_pass")

    status = (
        "KEEP_SOFT_MAINLINE_WINDOW_CONTRAST_RESCUE_DENSITY_DECAY_LOCAL_ONLY"
        if hard_blockers
        else "KEEP_SOFT_MAINLINE_WINDOW_CONTRAST_NO_MAJOR_DECAY_LOCAL_ONLY"
    )
    if hard_blockers:
        next_action = "wait_for_fresh_prefix_or_preflight_density_signal_before_remote"
        recommendation = (
            "The latest window had enough candidates but weaker fill/rescue quality and higher residual risk; "
            "do not spend another 1800s sample until the same-family density gate passes again."
        )
    else:
        next_action = "same_family_remote_can_be_considered_after_manifest_verifier_on_future_heartbeat"
        recommendation = (
            "The latest window did not show a material contrast decay versus reference; one bounded future dry-run "
            "can be considered after the normal verifier and process checks."
        )

    return {
        "artifact": "xuan_soft_mainline_window_contrast_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_window_contrast_scorer.py",
        "status": status,
        "reference_window": reference,
        "latest_window": latest,
        "deltas": deltas,
        "prefix_contrast": prefix_contrast,
        "diagnosis": {
            "not_primary_traffic_starvation": "latest_not_traffic_starved_candidate_count_not_lower" in hard_blockers,
            "primary_decay": [
                item
                for item in hard_blockers
                if item
                in {
                    "strict_rescue_close_ratio_collapsed",
                    "rescue_per_candidate_ratio_collapsed",
                    "rescue_per_fill_ratio_collapsed",
                    "fill_rate_ratio_weakened",
                    "residual_cost_share_worsened",
                    "residual_qty_share_worsened",
                    "latest_lost_prefix_density_pass",
                }
            ],
            "plain_english": (
                "The weak window was not simply short on touches/candidates; it admitted similar or more candidates, "
                "but converted far fewer into strict rescues and carried materially more residual risk."
                if hard_blockers
                else "No material density decay was detected against the reference window."
            ),
        },
        "decision": {
            "next_action": next_action,
            "recommendation": recommendation,
            "same_family_no_cancel_remote_density_worthwhile": not hard_blockers,
            "remote_runner_allowed": False,
            "research_only": True,
            "shadow_ready": False,
            "deployable": False,
            "hard_blockers": hard_blockers,
        },
        "thresholds": {
            "decision_prefix_minutes": args.decision_prefix_minutes,
            "min_rescue_ratio": args.min_rescue_ratio,
            "min_rescue_density_ratio": args.min_rescue_density_ratio,
            "min_fill_rate_ratio": args.min_fill_rate_ratio,
            "max_residual_qty_share_worsening": args.max_residual_qty_share_worsening,
            "max_residual_cost_share_worsening": args.max_residual_cost_share_worsening,
        },
        "guardrails": [
            "This contrast is local planning evidence only.",
            "It is not shadow approval, deployment approval, or promotion evidence.",
            "Do not return to ce25 hard gates as the mainline controller.",
            "Do not re-enable closeability deterioration cancel guard for this lane without separate local proof.",
            "Any future remote remains PM_DRY_RUN=1, 1800s max, one run per wake, and requires manifest verification.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference-runtime-summary", required=True)
    parser.add_argument("--reference-event-diagnostics", required=True)
    parser.add_argument("--reference-prefix-scorecard", required=True)
    parser.add_argument("--latest-runtime-summary", required=True)
    parser.add_argument("--latest-event-diagnostics", required=True)
    parser.add_argument("--latest-prefix-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--reference-label", default="reference_good_window")
    parser.add_argument("--latest-label", default="latest_window")
    parser.add_argument("--decision-prefix-minutes", type=float, default=10.0)
    parser.add_argument("--min-rescue-ratio", type=float, default=0.50)
    parser.add_argument("--min-rescue-density-ratio", type=float, default=0.50)
    parser.add_argument("--min-fill-rate-ratio", type=float, default=0.80)
    parser.add_argument("--max-residual-qty-share-worsening", type=float, default=0.10)
    parser.add_argument("--max-residual-cost-share-worsening", type=float, default=0.10)
    args = parser.parse_args()

    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rounded(card), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(card), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
