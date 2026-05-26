#!/usr/bin/env python3
"""Score the cap25 density-preserving pair-completion remote result.

This local-only scorer turns the 1654Z bounded no-order sample into a single
research decision artifact. It never launches remote jobs, approves capacity
expansion, deploys, restarts services, sends orders, or mutates shared state.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


TAG = "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-20260526T1654Z"
DEFAULT_REMOTE_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{TAG}/remote_outputs")
DEFAULT_REMOTE_LAUNCH = Path(".tmp_xuan/scorecards/xuan_density_preserving_pair_completion_remote_launch_20260526T1654Z.json")
DEFAULT_PREAUTH = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_pre_authorization_gate_20260526T1650Z.json"
)
DEFAULT_POSTRUN = Path(f".tmp_xuan/scorecards/{TAG}_postrun_bundle.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{TAG}_density_preflight_gate.json")
DEFAULT_PREFIX = Path(f".tmp_xuan/scorecards/no_order_{TAG}_density_prefix_scorer.json")
DEFAULT_SOURCE = Path(f".tmp_xuan/scorecards/no_order_{TAG}_source_caveat_audit.json")
DEFAULT_EVENT_DIAG = Path(f".tmp_xuan/scorecards/no_order_{TAG}_event_diagnostics.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_profile_20260526T1610Z.json")
DEFAULT_MANIFEST_VERIFIER = Path(
    ".tmp_xuan/scorecards/no_order_soft_mainline_density_preserving_pair_completion_manifest_verifier_20260526T1614Z.json"
)
DEFAULT_PRIOR_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_seed_risk_reduction_result_20260526T1510Z.json")
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_density_preserving_pair_completion_result_20260526T1654Z.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/xuan_shadow_review_density_preserving_pair_completion_result_20260526T1654Z/RESULT.md"
)


def load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open(encoding="utf-8") as handle:
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


def inum(value: Any, default: int | None = None) -> int | None:
    num = fnum(value)
    if num is None:
        return default
    return int(num)


def passed_gte(actual: float | None, threshold: float | None) -> bool:
    return actual is not None and threshold is not None and actual >= threshold


def passed_lte(actual: float | None, threshold: float | None) -> bool:
    return actual is not None and threshold is not None and actual <= threshold


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def read_exit_code(remote_output_root: str | Path | None, launch: dict[str, Any]) -> int | None:
    if remote_output_root:
        path = Path(remote_output_root).expanduser().resolve() / "run_exit_code.txt"
        if path.exists():
            text = path.read_text(errors="replace").strip()
            try:
                return int(text)
            except ValueError:
                return None
    return inum(launch.get("run_exit_code"))


def file_size(remote_output_root: str | Path | None, name: str) -> int | None:
    if not remote_output_root:
        return None
    path = Path(remote_output_root).expanduser().resolve() / name
    if not path.exists():
        return None
    return path.stat().st_size


def file_contains(remote_output_root: str | Path | None, name: str, needle: str) -> bool:
    if not remote_output_root:
        return False
    path = Path(remote_output_root).expanduser().resolve() / name
    if not path.exists():
        return False
    return needle in path.read_text(errors="replace")


def stat(card: dict[str, Any], key: str, field: str) -> float | None:
    raw = card.get(key)
    if not isinstance(raw, dict):
        return None
    return fnum(raw.get(field))


def metric_delta(current: dict[str, Any], prior: dict[str, Any], key: str) -> dict[str, Any]:
    now = fnum(current.get(key))
    before = fnum(prior.get(key))
    delta = None if now is None or before is None else now - before
    return {"current": now, "prior": before, "delta": delta}


def density_gate(density: dict[str, Any], prefix: dict[str, Any]) -> tuple[bool, dict[str, Any], list[str]]:
    observed = body(density, "observed")
    thresholds = body(density, "thresholds")
    final_prefix = body(prefix, "final_prefix")
    blockers: list[str] = []

    checks = {
        "candidate_rows": (
            fnum(observed.get("candidate_rows")),
            fnum(thresholds.get("min_candidates"), 20.0),
            "candidate_density_below_min",
        ),
        "accepted_actions": (
            fnum(observed.get("accepted_actions")),
            fnum(thresholds.get("min_accepted_actions"), 20.0),
            "accepted_actions_below_min",
        ),
        "queue_supported_fills": (
            fnum(observed.get("queue_supported_fills")),
            fnum(thresholds.get("min_fills"), 18.0),
            "fill_density_below_min",
        ),
        "strict_rescue_or_salvage_rows": (
            fnum(observed.get("strict_rescue_or_salvage_rows")),
            fnum(thresholds.get("min_rescue_closes"), 7.0),
            "rescue_density_below_min",
        ),
        "fill_rate": (
            fnum(observed.get("fill_rate")),
            fnum(thresholds.get("min_fill_rate"), 0.7),
            "fill_rate_below_min",
        ),
        "rescue_per_candidate": (
            fnum(observed.get("rescue_per_candidate")),
            fnum(thresholds.get("min_rescue_per_candidate"), 0.25),
            "rescue_per_candidate_below_min",
        ),
        "rescue_per_fill": (
            fnum(observed.get("rescue_per_fill")),
            fnum(thresholds.get("min_rescue_per_fill"), 0.3),
            "rescue_per_fill_below_min",
        ),
        "pair_pnl": (
            fnum(observed.get("pair_pnl")),
            fnum(thresholds.get("min_pair_pnl"), 0.0),
            "pair_pnl_below_min",
        ),
    }
    for actual, threshold, blocker in checks.values():
        if not passed_gte(actual, threshold):
            blockers.append(blocker)

    if not passed_lte(
        fnum(observed.get("accepted_l1_age_ms_max")),
        fnum(thresholds.get("max_accepted_l1_age_ms"), 1000.0),
    ):
        blockers.append("accepted_l1_age_above_max")
    if not passed_lte(
        fnum(observed.get("rescue_l1_age_ms_max")),
        fnum(thresholds.get("max_rescue_l1_age_ms"), 50.0),
    ):
        blockers.append("rescue_l1_age_above_max")
    if (fnum(observed.get("source_blocks"), 0.0) or 0.0) > (
        fnum(thresholds.get("max_source_blocks"), 0.0) or 0.0
    ):
        blockers.append("source_blocks_above_max")
    if (fnum(observed.get("bad_event_json"), 0.0) or 0.0) > 0.0:
        blockers.append("bad_event_json_present")
    if not bool(final_prefix.get("pass")):
        blockers.append("no_prefix_passed_density_gate_after_min_decision_minutes")

    summary = {
        "candidate_rows": fnum(observed.get("candidate_rows")),
        "accepted_actions": fnum(observed.get("accepted_actions")),
        "queue_supported_fills": fnum(observed.get("queue_supported_fills")),
        "strict_rescue_or_salvage_rows": fnum(observed.get("strict_rescue_or_salvage_rows")),
        "fill_rate": fnum(observed.get("fill_rate")),
        "rescue_per_candidate": fnum(observed.get("rescue_per_candidate")),
        "rescue_per_fill": fnum(observed.get("rescue_per_fill")),
        "pair_pnl": fnum(observed.get("pair_pnl")),
        "residual_qty_share": fnum(observed.get("residual_qty_share")),
        "residual_cost_share": fnum(observed.get("residual_cost_share")),
        "accepted_l1_age_ms_max": fnum(observed.get("accepted_l1_age_ms_max")),
        "rescue_l1_age_ms_max": fnum(observed.get("rescue_l1_age_ms_max")),
        "source_blocks": fnum(observed.get("source_blocks"), 0.0),
        "bad_event_json": fnum(observed.get("bad_event_json"), 0.0),
        "pair_completion_blocks": fnum(observed.get("pair_completion_blocks"), 0.0),
        "surplus_budget_blocks": fnum(body(observed, "event_counts").get("surplus_budget_block"), 0.0),
        "density_thresholds": thresholds,
        "prefix_earliest_pass_minutes": body(prefix, "decision").get("earliest_pass_minutes"),
        "prefix_final_hard_blockers": listify(final_prefix.get("hard_blockers")),
    }
    seen: set[str] = set()
    blockers = [item for item in blockers if not (item in seen or seen.add(item))]
    return (not blockers), summary, blockers


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    o = card["observed"]
    p = card["prior_comparison"]
    e = card["event_diagnostics"]
    lines = [
        "# Xuan Density-Preserving Pair-Completion Remote Result",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- remote_exit_code: `{d['remote_exit_code']}`",
        f"- wrapper_killed: `{d['wrapper_killed']}`",
        f"- aggregate_missing: `{d['aggregate_missing']}`",
        f"- full_window_promotion_evidence_valid: `{d['full_window_promotion_evidence_valid']}`",
        f"- density_gate_pass: `{d['density_gate_pass']}`",
        f"- residual_preflight_zero: `{d['residual_preflight_zero']}`",
        f"- source_review_clean: `{d['source_review_clean']}`",
        f"- same_profile_repeat_allowed: `{d['same_profile_repeat_allowed']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_result: `{d['future_remote_allowed_by_this_result']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Observed Density",
        "",
        f"- accepted/fills/rescues: `{o['accepted_actions']}` / `{o['queue_supported_fills']}` / `{o['strict_rescue_or_salvage_rows']}`",
        f"- fill_rate: `{o['fill_rate']}`",
        f"- rescue_per_candidate/rescue_per_fill: `{o['rescue_per_candidate']}` / `{o['rescue_per_fill']}`",
        f"- residual qty/cost share: `{o['residual_qty_share']}` / `{o['residual_cost_share']}`",
        f"- source_blocks/bad_json: `{o['source_blocks']}` / `{o['bad_event_json']}`",
        "",
        "## Prior Comparison",
        "",
        f"- accepted delta: `{p['accepted_actions']['delta']}`",
        f"- fills delta: `{p['queue_supported_fills']['delta']}`",
        f"- rescues delta: `{p['strict_rescue_or_salvage_rows']['delta']}`",
        f"- pair_completion_blocks delta: `{p['pair_completion_blocks']['delta']}`",
        f"- source_blocks delta: `{p['source_blocks']['delta']}`",
        f"- density_recovered_partially: `{d['density_recovered_partially']}`",
        f"- pair_completion_pressure_reduced: `{d['pair_completion_pressure_reduced']}`",
        "",
        "## Guard Pressure",
        "",
        f"- pair_completion_blocks: `{e['pair_completion_blocks']}`",
        f"- surplus_budget_blocks: `{e['surplus_budget_blocks']}`",
        f"- risk_seed_candidates true/false: `{e['risk_seed_candidate_true']}` / `{e['risk_seed_candidate_false']}`",
        f"- strict rescue net pair cost min/p50/max: `{e['strict_rescue_net_pair_cost_min']}` / `{e['strict_rescue_net_pair_cost_p50']}` / `{e['strict_rescue_net_pair_cost_max']}`",
        f"- strict rescue projected pnl after min/p50/max: `{e['strict_rescue_projected_pair_pnl_after_min']}` / `{e['strict_rescue_projected_pair_pnl_after_p50']}` / `{e['strict_rescue_projected_pair_pnl_after_max']}`",
        f"- surplus projected unpaired cost p50/max: `{e['surplus_projected_unpaired_cost_p50']}` / `{e['surplus_projected_unpaired_cost_max']}`",
        "",
        "## Interpretation",
        "",
        "- This is not promotion evidence: the wrapper was killed with exit 137 and no aggregate report was produced.",
        "- The local partial signal improved over 1510Z on accepted actions, fills, source cleanliness, bad JSON, and pair-completion pressure.",
        "- The remaining blocker is rescue density: only 6 rescues versus a 7-rescue minimum, with rescue ratios below thresholds.",
        "- Do not repeat the same profile or expand capacity until a local artifact resolves both the exit-137/aggregate path and rescue-density gap.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    launch = load_json(args.remote_launch_scorecard)
    preauth = load_json(args.pre_authorization_scorecard)
    postrun = load_json(args.postrun_bundle_scorecard)
    density = load_json(args.density_preflight_scorecard)
    prefix = load_json(args.density_prefix_scorecard)
    source_audit = load_json(args.source_caveat_audit_scorecard)
    event_diag = load_json(args.event_diagnostics_scorecard)
    profile = load_json(args.profile_scorecard)
    manifest_verifier = load_json(args.manifest_verifier_scorecard)
    prior_result = load_json(args.prior_result_scorecard)

    remote_exit_code = read_exit_code(args.remote_output_root, launch)
    wrapper_killed = file_contains(args.remote_output_root, "remote_wrapper_stderr.log", "Killed")
    remote_completed = remote_exit_code == 0
    aggregate_missing = "aggregate_report.json" in listify(body(postrun, "runtime_summary").get("missing_files"))
    postrun_complete = is_keep(postrun) and not aggregate_missing
    density_pass, density_summary, density_blockers = density_gate(density, prefix)
    source_clean = is_keep(source_audit)
    manifest_clean = is_keep(manifest_verifier)
    preauth_clean = is_keep(preauth)

    prior_observed = body(prior_result, "observed")
    prior_comparison = {
        key: metric_delta(density_summary, prior_observed, key)
        for key in (
            "accepted_actions",
            "queue_supported_fills",
            "strict_rescue_or_salvage_rows",
            "rescue_per_candidate",
            "rescue_per_fill",
            "source_blocks",
            "bad_event_json",
            "pair_completion_blocks",
            "surplus_budget_blocks",
        )
    }
    density_recovered_partially = (
        (prior_comparison["accepted_actions"]["delta"] or 0.0) > 0.0
        and (prior_comparison["queue_supported_fills"]["delta"] or 0.0) > 0.0
        and (prior_comparison["strict_rescue_or_salvage_rows"]["delta"] or 0.0) > 0.0
    )
    pair_completion_pressure_reduced = (prior_comparison["pair_completion_blocks"]["delta"] or 0.0) < 0.0
    source_quality_recovered = (
        (density_summary.get("source_blocks") or 0.0) == 0.0
        and (density_summary.get("bad_event_json") or 0.0) == 0.0
        and (prior_comparison["source_blocks"]["delta"] or 0.0) < 0.0
    )
    residual_preflight_zero = (
        (density_summary.get("residual_qty_share") or 0.0) == 0.0
        and (density_summary.get("residual_cost_share") or 0.0) == 0.0
    )

    event_counts = body(event_diag, "event_counts")
    candidate_seed_values = body(event_diag, "candidate_risk_increasing_seed_values")
    final_prefix = body(prefix, "final_prefix")

    hard_blockers: list[str] = []
    if remote_exit_code is None:
        hard_blockers.append("remote_exit_code_missing")
    elif remote_exit_code != 0:
        hard_blockers.append(f"run_exit_code_{remote_exit_code}")
    if wrapper_killed:
        hard_blockers.append("remote_wrapper_killed")
    if aggregate_missing:
        hard_blockers.append("aggregate_report_missing")
    if not postrun_complete:
        hard_blockers.append("postrun_bundle_incomplete")
    hard_blockers.extend(density_blockers)
    if not source_clean:
        hard_blockers.append("source_caveat_audit_not_keep")
    if not manifest_clean:
        hard_blockers.append("manifest_verifier_not_keep")
    if not preauth_clean:
        hard_blockers.append("pre_authorization_gate_not_keep")
    seen: set[str] = set()
    hard_blockers = [item for item in hard_blockers if not (item in seen or seen.add(item))]

    full_window_promotion_evidence_valid = (
        remote_completed and postrun_complete and density_pass and source_clean and manifest_clean and preauth_clean
    )

    if remote_exit_code == 137 and not density_pass:
        card_status = "BLOCKED_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_RESULT_EXIT_137_RESCUE_DENSITY_WEAK_LOCAL_ONLY"
    elif remote_exit_code and remote_exit_code != 0:
        card_status = "BLOCKED_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_RESULT_REMOTE_EXIT_NONZERO_LOCAL_ONLY"
    elif not density_pass:
        card_status = "BLOCKED_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_RESULT_RESCUE_DENSITY_WEAK_LOCAL_ONLY"
    elif full_window_promotion_evidence_valid:
        card_status = "KEEP_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_RESULT_CLEAR_LOCAL_ONLY"
    else:
        card_status = "UNKNOWN_SHADOW_REVIEW_DENSITY_PRESERVING_PAIR_COMPLETION_RESULT_INCOMPLETE_LOCAL_ONLY"

    profile_body = body(profile, "profile")
    source_observed = body(source_audit, "observed")
    card = {
        "artifact": "xuan_shadow_review_density_preserving_pair_completion_result_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_density_preserving_pair_completion_result_scorer.py",
        "status": card_status,
        "inputs": {
            "remote_output_root": str(args.remote_output_root),
            "remote_launch_scorecard": str(args.remote_launch_scorecard),
            "pre_authorization_scorecard": str(args.pre_authorization_scorecard),
            "postrun_bundle_scorecard": str(args.postrun_bundle_scorecard),
            "density_preflight_scorecard": str(args.density_preflight_scorecard),
            "density_prefix_scorecard": str(args.density_prefix_scorecard),
            "source_caveat_audit_scorecard": str(args.source_caveat_audit_scorecard),
            "event_diagnostics_scorecard": str(args.event_diagnostics_scorecard),
            "profile_scorecard": str(args.profile_scorecard),
            "manifest_verifier_scorecard": str(args.manifest_verifier_scorecard),
            "prior_result_scorecard": str(args.prior_result_scorecard),
        },
        "source_statuses": {
            "remote_launch": status(launch),
            "pre_authorization_gate": status(preauth),
            "postrun_bundle": status(postrun),
            "density_preflight": status(density),
            "density_prefix": status(prefix),
            "source_caveat_audit": status(source_audit),
            "event_diagnostics": status(event_diag) or "NO_STATUS_FIELD",
            "profile": status(profile),
            "manifest_verifier": status(manifest_verifier),
            "prior_seed_risk_reduction_result": status(prior_result),
        },
        "decision": {
            "remote_exit_code": remote_exit_code,
            "wrapper_killed": wrapper_killed,
            "remote_completed": remote_completed,
            "aggregate_missing": aggregate_missing,
            "postrun_complete": postrun_complete,
            "full_window_promotion_evidence_valid": full_window_promotion_evidence_valid,
            "density_gate_pass": density_pass,
            "density_blockers": density_blockers,
            "density_recovered_partially": density_recovered_partially,
            "pair_completion_pressure_reduced": pair_completion_pressure_reduced,
            "source_quality_recovered": source_quality_recovered,
            "residual_preflight_zero": residual_preflight_zero,
            "source_review_clean": source_clean,
            "manifest_verifier_clean": manifest_clean,
            "pre_authorization_gate_clean": preauth_clean,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "future_remote_allowed_by_this_result": False,
            "future_remote_requires_new_local_rationale": True,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "do_not_run_remote_again_this_wake": True,
            "hard_blockers": hard_blockers,
            "next_action": (
                "local_only_fix_exit_137_aggregate_path_and_rescue_density_gap_before_any_new_bounded_cap25_sample"
            ),
        },
        "observed": {
            "run_exit_code": remote_exit_code,
            "run_stdout_bytes": file_size(args.remote_output_root, "run_stdout.log"),
            "run_stderr_bytes": file_size(args.remote_output_root, "run_stderr.log"),
            "remote_wrapper_stdout_bytes": file_size(args.remote_output_root, "remote_wrapper_stdout.log"),
            "remote_wrapper_stderr_bytes": file_size(args.remote_output_root, "remote_wrapper_stderr.log"),
            **density_summary,
            "source_block_rows": fnum(source_observed.get("source_block_rows"), 0.0),
            "strict_rescue_source_blocks": fnum(source_observed.get("strict_rescue_source_blocks"), 0.0),
            "source_block_l1_age_ms_max": fnum(source_observed.get("source_block_l1_age_ms_max")),
            "final_prefix_pass": bool(final_prefix.get("pass")),
            "final_prefix_elapsed_minutes": fnum(final_prefix.get("elapsed_minutes")),
        },
        "prior_comparison": prior_comparison,
        "event_diagnostics": {
            "pair_completion_blocks": fnum(event_counts.get("pair_completion_block"), 0.0),
            "surplus_budget_blocks": fnum(event_counts.get("surplus_budget_block"), 0.0),
            "ttl_cancels": fnum(event_counts.get("cancel"), 0.0),
            "risk_seed_candidate_true": fnum(candidate_seed_values.get("true"), 0.0),
            "risk_seed_candidate_false": fnum(candidate_seed_values.get("false"), 0.0),
            "strict_rescue_net_pair_cost_min": stat(event_diag, "strict_rescue_close_net_pair_cost", "min"),
            "strict_rescue_net_pair_cost_p50": stat(event_diag, "strict_rescue_close_net_pair_cost", "p50"),
            "strict_rescue_net_pair_cost_p90": stat(event_diag, "strict_rescue_close_net_pair_cost", "p90"),
            "strict_rescue_net_pair_cost_max": stat(event_diag, "strict_rescue_close_net_pair_cost", "max"),
            "strict_rescue_projected_pair_pnl_after_min": stat(
                event_diag, "strict_rescue_close_projected_pair_pnl_after", "min"
            ),
            "strict_rescue_projected_pair_pnl_after_p50": stat(
                event_diag, "strict_rescue_close_projected_pair_pnl_after", "p50"
            ),
            "strict_rescue_projected_pair_pnl_after_max": stat(
                event_diag, "strict_rescue_close_projected_pair_pnl_after", "max"
            ),
            "surplus_projected_unpaired_cost_p50": stat(
                event_diag, "surplus_budget_projected_unpaired_cost", "p50"
            ),
            "surplus_projected_unpaired_cost_max": stat(
                event_diag, "surplus_budget_projected_unpaired_cost", "max"
            ),
            "strict_rescue_block_reasons": body(event_diag, "strict_rescue_block_reasons"),
            "risk_seed_closeability_block_reasons": body(event_diag, "risk_seed_closeability_block_reasons"),
        },
        "profile_excerpt": {
            "target_qty": profile_body.get("target_qty"),
            "risk_seed_pair_completion_required_above_net_cap": profile_body.get(
                "risk_seed_pair_completion_required_above_net_cap"
            ),
            "risk_seed_pair_completion_min_qty": profile_body.get("risk_seed_pair_completion_min_qty"),
            "pair_completion_net_cap": profile_body.get("pair_completion_net_cap"),
            "pair_completion_min_pair_pnl_after": profile_body.get("pair_completion_min_pair_pnl_after"),
            "risk_seed_pending_opp_credit": profile_body.get("risk_seed_pending_opp_credit"),
            "surplus_budget_max_abs_unpaired_cost": profile_body.get("surplus_budget_max_abs_unpaired_cost"),
            "strict_rescue_surplus_net_cap": profile_body.get("strict_rescue_surplus_net_cap"),
        },
        "interpretation": {
            "promotion_signal": "invalid_full_window_evidence",
            "runtime_signal": "exit_137_wrapper_killed_aggregate_missing",
            "density_signal": "partial_recovery_but_rescue_density_still_weak",
            "residual_signal": "preflight_zero_but_not_promotion" if residual_preflight_zero else "residual_not_clear",
            "source_signal": "review_clean" if source_clean else "not_review_clean",
            "do_not_expand_capacity": True,
            "do_not_repeat_same_profile": True,
            "do_not_weaken_source_or_economic_gates": True,
            "suggested_local_next_step": (
                "diagnose local output/aggregate resource path and design a rescue-density-preserving cap25 variant"
            ),
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote-output-root", type=Path, default=DEFAULT_REMOTE_OUTPUT_ROOT)
    parser.add_argument("--remote-launch-scorecard", type=Path, default=DEFAULT_REMOTE_LAUNCH)
    parser.add_argument("--pre-authorization-scorecard", type=Path, default=DEFAULT_PREAUTH)
    parser.add_argument("--postrun-bundle-scorecard", type=Path, default=DEFAULT_POSTRUN)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--density-prefix-scorecard", type=Path, default=DEFAULT_PREFIX)
    parser.add_argument("--source-caveat-audit-scorecard", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--event-diagnostics-scorecard", type=Path, default=DEFAULT_EVENT_DIAG)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--manifest-verifier-scorecard", type=Path, default=DEFAULT_MANIFEST_VERIFIER)
    parser.add_argument("--prior-result-scorecard", type=Path, default=DEFAULT_PRIOR_RESULT)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown(card) + "\n", encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
