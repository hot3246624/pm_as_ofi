#!/usr/bin/env python3
"""Score the residual-aware tail-density midpoint remote result.

This local-only scorer turns the midpoint run's remote exit, postrun bundle,
and partial event-density diagnostics into one review artifact. It does not
launch remote jobs, approve capacity expansion, deploy, restart services, or
send orders.
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


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def hard_blockers_from(card: dict[str, Any]) -> list[str]:
    blockers = listify(card.get("hard_blockers"))
    blockers.extend(listify(body(card, "decision").get("hard_blockers")))
    seen: set[str] = set()
    out: list[str] = []
    for item in blockers:
        text = str(item)
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def stat(card: dict[str, Any], key: str, field: str) -> float | None:
    raw = card.get(key)
    if not isinstance(raw, dict):
        return None
    return fnum(raw.get(field))


def observed_density(density: dict[str, Any], prefix: dict[str, Any]) -> dict[str, Any]:
    observed = body(density, "observed")
    final_prefix = body(prefix, "final_prefix")
    return {
        "candidate_rows": fnum(observed.get("candidate_rows")),
        "accepted_actions": fnum(observed.get("accepted_actions")),
        "queue_supported_fills": fnum(observed.get("queue_supported_fills")),
        "strict_rescue_or_salvage_rows": fnum(observed.get("strict_rescue_or_salvage_rows")),
        "fill_rate": fnum(observed.get("fill_rate")),
        "rescue_per_candidate": fnum(observed.get("rescue_per_candidate")),
        "rescue_per_fill": fnum(observed.get("rescue_per_fill")),
        "source_blocks": fnum(observed.get("source_blocks"), 0.0),
        "accepted_l1_age_ms_max": fnum(observed.get("accepted_l1_age_ms_max")),
        "rescue_l1_age_ms_max": fnum(observed.get("rescue_l1_age_ms_max")),
        "prefix_earliest_pass_minutes": body(prefix, "decision").get("earliest_pass_minutes"),
        "prefix_final_hard_blockers": listify(final_prefix.get("hard_blockers")),
    }


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    o = card["observed"]
    event = card["event_diagnostics"]
    lines = [
        "# Xuan Tail Density Midpoint Remote Result",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- remote_process_blocked: `{d['remote_process_blocked']}`",
        f"- full_window_promotion_evidence_valid: `{d['full_window_promotion_evidence_valid']}`",
        f"- partial_density_recovered: `{d['partial_density_recovered']}`",
        f"- source_clean_or_absorbed: `{d['source_clean_or_absorbed']}`",
        f"- pnl_floor_effective_on_partial_rescues: `{d['pnl_floor_effective_on_partial_rescues']}`",
        f"- same_profile_repeat_allowed: `{d['same_profile_repeat_allowed']}`",
        f"- future_remote_allowed_by_this_result: `{d['future_remote_allowed_by_this_result']}`",
        f"- next_action: `{d['next_action']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Remote Outcome",
        "",
        f"- run_exit_code: `{o['run_exit_code']}`",
        f"- run_stderr_bytes: `{o['run_stderr_bytes']}`",
        f"- remote_wrapper_stderr_bytes: `{o['remote_wrapper_stderr_bytes']}`",
        f"- postrun_status: `{card['source_statuses']['postrun_bundle']}`",
        "",
        "## Partial Density",
        "",
        f"- accepted/fills/rescues: `{o['accepted_actions']}` / `{o['queue_supported_fills']}` / `{o['strict_rescue_or_salvage_rows']}`",
        f"- fill_rate: `{o['fill_rate']}`",
        f"- rescue_per_candidate/rescue_per_fill: `{o['rescue_per_candidate']}` / `{o['rescue_per_fill']}`",
        f"- source_blocks: `{o['source_blocks']}`",
        "",
        "## Event Diagnostics",
        "",
        f"- strict rescue projected pnl after min/p50/max: `{event['strict_rescue_projected_pair_pnl_after_min']}` / `{event['strict_rescue_projected_pair_pnl_after_p50']}` / `{event['strict_rescue_projected_pair_pnl_after_max']}`",
        f"- strict rescue net pair cost max: `{event['strict_rescue_net_pair_cost_max']}`",
        f"- strict rescue block reasons: `{event['strict_rescue_block_reasons']}`",
        f"- lot age/min-cost classes: `{event['strict_rescue_lot_age_min_cost_classes']}`",
        f"- surplus budget projected unpaired cost p50/max: `{event['surplus_budget_projected_unpaired_cost_p50']}` / `{event['surplus_budget_projected_unpaired_cost_max']}`",
        "",
        "## Interpretation",
        "",
        "- Exit 137 means this run is not valid full-window promotion evidence.",
        "- The partial event stream is still useful for local diagnosis: source stayed clean and the PnL floor kept observed rescue projected PnL positive.",
        "- The same midpoint profile should not be repeated because rescue density fell below the cap25 threshold and no prefix passed.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    launch = load_json(args.remote_launch_scorecard)
    postrun = load_json(args.postrun_bundle_scorecard)
    density = load_json(args.density_preflight_scorecard)
    prefix = load_json(args.density_prefix_scorecard)
    source_audit = load_json(args.source_caveat_audit_scorecard)
    event_diag = load_json(args.event_diagnostics_scorecard)
    profile = load_json(args.profile_scorecard)
    manifest_verifier = load_json(args.manifest_verifier_scorecard)

    density_observed = observed_density(density, prefix)
    launch_decision = body(launch, "decision")
    source_decision = body(source_audit, "decision")
    remote_process_blocked = fnum(launch.get("run_exit_code")) not in (None, 0.0)
    postrun_incomplete = not is_keep(postrun)
    source_clean_or_absorbed = (
        is_keep(source_audit)
        and not bool(source_decision.get("accepted_or_rescue_source_contamination"))
        and (
            bool(source_decision.get("source_caveat_absorbed"))
            or status(source_audit).endswith("_NO_SOURCE_BLOCKS_LOCAL_ONLY")
        )
    )

    density_blockers = hard_blockers_from(density)
    prefix_blockers = hard_blockers_from(prefix)
    partial_density_recovered = (
        not density_blockers
        and not prefix_blockers
        and (density_observed["strict_rescue_or_salvage_rows"] or 0.0) >= 7.0
    )
    projected_min = stat(event_diag, "strict_rescue_close_projected_pair_pnl_after", "min")
    pnl_floor_effective = projected_min is not None and projected_min >= args.min_projected_pair_pnl_after

    hard_blockers: list[str] = []
    if remote_process_blocked:
        code = launch.get("run_exit_code")
        hard_blockers.append(f"run_exit_code_{code}")
    if postrun_incomplete:
        hard_blockers.append("postrun_bundle_incomplete")
    if not partial_density_recovered:
        hard_blockers.extend(density_blockers or ["density_preflight_not_keep"])
        hard_blockers.extend(f"prefix:{item}" for item in (prefix_blockers or ["density_prefix_not_keep"]))
    if not source_clean_or_absorbed:
        hard_blockers.append("source_caveat_not_clean")

    seen: set[str] = set()
    hard_blockers = [item for item in hard_blockers if not (item in seen or seen.add(item))]
    full_window_promotion_evidence_valid = not remote_process_blocked and not postrun_incomplete and not hard_blockers

    if remote_process_blocked:
        card_status = "BLOCKED_SHADOW_REVIEW_TAIL_DENSITY_MIDPOINT_RESULT_REMOTE_EXIT_137_RESEARCH_ONLY"
        next_action = "do_not_run_another_remote_this_wake; keep local and redesign before any future bounded sample"
    elif not partial_density_recovered:
        card_status = "DISCARD_SHADOW_REVIEW_TAIL_DENSITY_MIDPOINT_RESULT_DENSITY_WEAK_LOCAL_ONLY"
        next_action = "discard_midpoint_profile_for_capacity_path; design_local_density_proxy_before_remote"
    elif full_window_promotion_evidence_valid:
        card_status = "KEEP_SHADOW_REVIEW_TAIL_DENSITY_MIDPOINT_RESULT_CLEAR_LOCAL_ONLY"
        next_action = "recheck_capacity_stage_and_manifest_before_any_future_capacity_ladder_sample"
    else:
        card_status = "UNKNOWN_SHADOW_REVIEW_TAIL_DENSITY_MIDPOINT_RESULT_INCOMPLETE_LOCAL_ONLY"
        next_action = "stay_local_until_runtime_and_postrun_evidence_are_complete"

    profile_body = body(profile, "profile")
    card = {
        "artifact": "xuan_shadow_review_tail_density_midpoint_result_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_tail_density_midpoint_result_scorer.py",
        "status": card_status,
        "inputs": {
            "remote_launch_scorecard": args.remote_launch_scorecard,
            "postrun_bundle_scorecard": args.postrun_bundle_scorecard,
            "density_preflight_scorecard": args.density_preflight_scorecard,
            "density_prefix_scorecard": args.density_prefix_scorecard,
            "source_caveat_audit_scorecard": args.source_caveat_audit_scorecard,
            "event_diagnostics_scorecard": args.event_diagnostics_scorecard,
            "profile_scorecard": args.profile_scorecard,
            "manifest_verifier_scorecard": args.manifest_verifier_scorecard,
        },
        "source_statuses": {
            "remote_launch": status(launch),
            "postrun_bundle": status(postrun),
            "density_preflight": status(density),
            "density_prefix": status(prefix),
            "source_caveat_audit": status(source_audit),
            "profile": status(profile),
            "manifest_verifier": status(manifest_verifier),
        },
        "decision": {
            "remote_process_blocked": remote_process_blocked,
            "remote_exit_code": fnum(launch.get("run_exit_code")),
            "remote_runner_completed": bool(launch_decision.get("remote_runner_completed")),
            "full_window_promotion_evidence_valid": full_window_promotion_evidence_valid,
            "partial_density_recovered": partial_density_recovered,
            "source_clean_or_absorbed": source_clean_or_absorbed,
            "pnl_floor_effective_on_partial_rescues": pnl_floor_effective,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "future_remote_allowed_by_this_result": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "hard_blockers": hard_blockers,
            "density_blockers": density_blockers,
            "prefix_blockers": prefix_blockers,
            "next_action": next_action,
        },
        "observed": {
            "run_exit_code": fnum(launch.get("run_exit_code")),
            "run_stderr_bytes": fnum(launch.get("run_stderr_bytes")),
            "remote_wrapper_stderr_bytes": fnum(launch.get("remote_wrapper_stderr_bytes")),
            **density_observed,
        },
        "event_diagnostics": {
            "event_file_count": fnum(event_diag.get("event_file_count")),
            "event_rows_scanned": fnum(event_diag.get("event_rows_scanned")),
            "bad_json": fnum(event_diag.get("bad_json"), 0.0),
            "strict_rescue_projected_pair_pnl_after_min": projected_min,
            "strict_rescue_projected_pair_pnl_after_p50": stat(
                event_diag, "strict_rescue_close_projected_pair_pnl_after", "p50"
            ),
            "strict_rescue_projected_pair_pnl_after_max": stat(
                event_diag, "strict_rescue_close_projected_pair_pnl_after", "max"
            ),
            "strict_rescue_net_pair_cost_max": stat(event_diag, "strict_rescue_close_net_pair_cost", "max"),
            "strict_rescue_block_reasons": body(event_diag, "strict_rescue_block_reasons"),
            "strict_rescue_lot_age_min_cost_classes": body(event_diag, "strict_rescue_lot_age_min_cost_classes"),
            "strict_rescue_block_components": body(event_diag, "strict_rescue_block_components"),
            "surplus_budget_projected_unpaired_cost_p50": stat(
                event_diag, "surplus_budget_projected_unpaired_cost", "p50"
            ),
            "surplus_budget_projected_unpaired_cost_max": stat(
                event_diag, "surplus_budget_projected_unpaired_cost", "max"
            ),
        },
        "profile_excerpt": {
            "tail_mitigation_hypothesis": profile_body.get("tail_mitigation_hypothesis"),
            "salvage_min_lot_cost": profile_body.get("salvage_min_lot_cost"),
            "strict_rescue_min_pair_pnl_after": profile_body.get("strict_rescue_min_pair_pnl_after"),
            "strict_rescue_surplus_net_cap": profile_body.get("strict_rescue_surplus_net_cap"),
            "surplus_budget_max_abs_unpaired_cost": profile_body.get("surplus_budget_max_abs_unpaired_cost"),
            "imbalance_qty_cap": profile_body.get("imbalance_qty_cap"),
        },
        "interpretation": {
            "process_signal": "remote_child_killed_exit_137" if remote_process_blocked else "remote_child_completed",
            "promotion_signal": "invalid_full_window_evidence" if postrun_incomplete else "postrun_complete",
            "partial_density_signal": "density_recovered" if partial_density_recovered else "density_weak",
            "pnl_floor_signal": "projected_rescue_pnl_floor_positive_on_partial_rescues"
            if pnl_floor_effective
            else "projected_rescue_pnl_floor_not_confirmed",
            "do_not_expand_capacity": True,
            "do_not_repeat_same_profile": True,
            "do_not_relax_source_or_economic_gates": True,
            "do_not_run_remote_again_this_wake": True,
            "suggested_local_next_step": "build a local residual-density alternative or runner-resource mitigation before any new bounded dry-run rationale",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote-launch-scorecard", required=True)
    parser.add_argument("--postrun-bundle-scorecard", required=True)
    parser.add_argument("--density-preflight-scorecard", required=True)
    parser.add_argument("--density-prefix-scorecard", required=True)
    parser.add_argument("--source-caveat-audit-scorecard", required=True)
    parser.add_argument("--event-diagnostics-scorecard", required=True)
    parser.add_argument("--profile-scorecard", required=True)
    parser.add_argument("--manifest-verifier-scorecard")
    parser.add_argument("--min-projected-pair-pnl-after", type=float, default=0.02)
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
