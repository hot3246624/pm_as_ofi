#!/usr/bin/env python3
"""Score the cap25 seed-risk-reduction remote result.

This local-only scorer converts a bounded no-order dry-run into a single
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


def inum(value: Any, default: int | None = None) -> int | None:
    num = fnum(value)
    if num is None:
        return default
    return int(num)


def passed_gte(actual: float | None, threshold: float | None) -> bool:
    return actual is not None and threshold is not None and actual >= threshold


def passed_lte(actual: float | None, threshold: float | None) -> bool:
    return actual is not None and threshold is not None and actual <= threshold


def read_exit_code(remote_output_root: str | None, launch: dict[str, Any]) -> int | None:
    if remote_output_root:
        path = Path(remote_output_root).expanduser().resolve() / "run_exit_code.txt"
        if path.exists():
            text = path.read_text(errors="replace").strip()
            try:
                return int(text)
            except ValueError:
                return None
    return inum(launch.get("run_exit_code"))


def file_size(remote_output_root: str | None, name: str) -> int | None:
    if not remote_output_root:
        return None
    path = Path(remote_output_root).expanduser().resolve() / name
    if not path.exists():
        return None
    return path.stat().st_size


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
        blockers.append("no_prefix_passed_density_gate")

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
    e = card["event_diagnostics"]
    lines = [
        "# Xuan Seed Risk Reduction Remote Result",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- remote_exit_code: `{d['remote_exit_code']}`",
        f"- full_window_promotion_evidence_valid: `{d['full_window_promotion_evidence_valid']}`",
        f"- density_preserved: `{d['density_preserved']}`",
        f"- pair_completion_overfiltered: `{d['pair_completion_overfiltered']}`",
        f"- residual_preflight_zero: `{d['residual_preflight_zero']}`",
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
        f"- source_blocks/bad_json: `{o['source_blocks']}` / `{o['bad_event_json']}`",
        f"- residual qty/cost share from density preflight: `{o['residual_qty_share']}` / `{o['residual_cost_share']}`",
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
        "- This is not promotion evidence: the remote wrapper timed out and no aggregate report was produced.",
        "- The local signal is still useful: residual preflight went to zero, but density fell just below cap25 thresholds.",
        "- The pair-completion guard appears too restrictive for the current density target and needs local redesign before any fresh bounded sample.",
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

    remote_exit_code = read_exit_code(args.remote_output_root, launch)
    remote_completed = remote_exit_code == 0
    aggregate_missing = "aggregate_report.json" in listify(body(postrun, "runtime_summary").get("missing_files"))
    postrun_complete = is_keep(postrun) and not aggregate_missing
    density_preserved, density_summary, density_blockers = density_gate(density, prefix)

    event_counts = body(event_diag, "event_counts")
    candidate_seed_values = body(event_diag, "candidate_risk_increasing_seed_values")
    pair_completion_blocks = fnum(event_counts.get("pair_completion_block"), 0.0) or 0.0
    accepted_actions = density_summary.get("accepted_actions") or 0.0
    pair_completion_overfiltered = pair_completion_blocks >= max(30.0, accepted_actions * 3.0)
    residual_preflight_zero = (
        (density_summary.get("residual_qty_share") or 0.0) == 0.0
        and (density_summary.get("residual_cost_share") or 0.0) == 0.0
    )
    source_not_clean = not is_keep(source_audit)
    manifest_not_clean = not is_keep(manifest_verifier)

    hard_blockers: list[str] = []
    if remote_exit_code is None:
        hard_blockers.append("remote_exit_code_missing")
    elif remote_exit_code != 0:
        hard_blockers.append(f"run_exit_code_{remote_exit_code}")
    if aggregate_missing:
        hard_blockers.append("aggregate_report_missing")
    if not postrun_complete:
        hard_blockers.append("postrun_bundle_incomplete")
    hard_blockers.extend(density_blockers)
    if source_not_clean:
        hard_blockers.append("source_caveat_audit_not_keep")
    if manifest_not_clean:
        hard_blockers.append("manifest_verifier_not_keep")
    if pair_completion_overfiltered:
        hard_blockers.append("pair_completion_guard_overfiltered_density")

    seen: set[str] = set()
    hard_blockers = [item for item in hard_blockers if not (item in seen or seen.add(item))]
    full_window_promotion_evidence_valid = (
        remote_completed and postrun_complete and density_preserved and not source_not_clean and not manifest_not_clean
    )

    if remote_exit_code == 124:
        card_status = "BLOCKED_SHADOW_REVIEW_SEED_RISK_REDUCTION_RESULT_TIMEOUT_DENSITY_WEAK_LOCAL_ONLY"
    elif not density_preserved:
        card_status = "BLOCKED_SHADOW_REVIEW_SEED_RISK_REDUCTION_RESULT_DENSITY_WEAK_LOCAL_ONLY"
    elif full_window_promotion_evidence_valid:
        card_status = "KEEP_SHADOW_REVIEW_SEED_RISK_REDUCTION_RESULT_CLEAR_LOCAL_ONLY"
    else:
        card_status = "UNKNOWN_SHADOW_REVIEW_SEED_RISK_REDUCTION_RESULT_INCOMPLETE_LOCAL_ONLY"

    profile_body = body(profile, "profile")
    source_observed = body(source_audit, "observed")
    card = {
        "artifact": "xuan_shadow_review_seed_risk_reduction_result_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_seed_risk_reduction_result_scorer.py",
        "status": card_status,
        "inputs": {
            "remote_output_root": args.remote_output_root,
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
            "event_diagnostics": status(event_diag) or "NO_STATUS_FIELD",
            "profile": status(profile),
            "manifest_verifier": status(manifest_verifier),
        },
        "decision": {
            "remote_exit_code": remote_exit_code,
            "remote_completed": remote_completed,
            "aggregate_missing": aggregate_missing,
            "postrun_complete": postrun_complete,
            "full_window_promotion_evidence_valid": full_window_promotion_evidence_valid,
            "density_preserved": density_preserved,
            "density_blockers": density_blockers,
            "pair_completion_overfiltered": pair_completion_overfiltered,
            "residual_preflight_zero": residual_preflight_zero,
            "source_review_clean": not source_not_clean,
            "manifest_verifier_clean": not manifest_not_clean,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "future_remote_allowed_by_this_result": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "paper_shadow_only": True,
            "research_only": True,
            "do_not_run_remote_again_this_wake": True,
            "hard_blockers": hard_blockers,
            "next_action": (
                "local_only_redesign_pair_completion_guard_to_preserve_cap25_density_before_any_new_bounded_sample"
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
        },
        "event_diagnostics": {
            "pair_completion_blocks": pair_completion_blocks,
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
            "risk_seed_pair_completion_counterfactual": body(
                event_diag, "risk_seed_pair_completion_required_counterfactual"
            ),
        },
        "profile_excerpt": {
            "tail_mitigation_hypothesis": profile_body.get("tail_mitigation_hypothesis"),
            "risk_seed_pair_completion_required_above_net_cap": profile_body.get(
                "risk_seed_pair_completion_required_above_net_cap"
            ),
            "risk_seed_pair_completion_min_qty": profile_body.get("risk_seed_pair_completion_min_qty"),
            "pair_completion_net_cap": profile_body.get("pair_completion_net_cap"),
            "risk_seed_pending_opp_credit": profile_body.get("risk_seed_pending_opp_credit"),
            "surplus_budget_max_abs_unpaired_cost": profile_body.get("surplus_budget_max_abs_unpaired_cost"),
            "strict_rescue_surplus_net_cap": profile_body.get("strict_rescue_surplus_net_cap"),
        },
        "interpretation": {
            "promotion_signal": "invalid_full_window_evidence",
            "density_signal": "guard_overfiltered_density" if pair_completion_overfiltered else "density_not_keep",
            "residual_signal": "preflight_zero_but_not_promotion" if residual_preflight_zero else "residual_not_clear",
            "source_signal": "not_review_clean",
            "do_not_expand_capacity": True,
            "do_not_repeat_same_profile": True,
            "do_not_weaken_source_or_economic_gates": True,
            "suggested_local_next_step": (
                "build a density-preserving pair-completion variant; only a later local artifact may justify a fresh cap25 PM_DRY_RUN"
            ),
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote-output-root")
    parser.add_argument("--remote-launch-scorecard")
    parser.add_argument("--postrun-bundle-scorecard", required=True)
    parser.add_argument("--density-preflight-scorecard", required=True)
    parser.add_argument("--density-prefix-scorecard", required=True)
    parser.add_argument("--source-caveat-audit-scorecard", required=True)
    parser.add_argument("--event-diagnostics-scorecard", required=True)
    parser.add_argument("--profile-scorecard", required=True)
    parser.add_argument("--manifest-verifier-scorecard")
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
