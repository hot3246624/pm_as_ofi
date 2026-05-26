#!/usr/bin/env python3
"""Decide whether the cap25 midpoint profile can be rerun as-is.

This is a local-only gate. It consumes the prior midpoint result and event
diagnostics, then separates a valid bounded rerun rationale from a profile
redesign requirement. It never launches a remote job or authorizes deploy/live.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TRANSFER_PLAN = Path(".tmp_xuan/scorecards/xuan_public_04b6_residual_transfer_plan_20260526T1338Z.json")
DEFAULT_MIDPOINT_RESULT = Path(".tmp_xuan/scorecards/xuan_shadow_review_tail_density_midpoint_result_20260526T0945Z.json")
DEFAULT_EVENT_DIAGNOSTICS = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-density-midpoint-20260526T0945Z_event_diagnostics.json"
)
DEFAULT_DENSITY_PREFLIGHT = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-density-midpoint-20260526T0945Z_density_preflight_gate.json"
)
DEFAULT_DENSITY_PREFIX = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-density-midpoint-20260526T0945Z_density_prefix_scorer.json"
)
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_tail_density_residual_rebalance_profile_20260526T1704Z.json")
DEFAULT_MANIFEST_VERIFIER = Path(
    ".tmp_xuan/scorecards/no_order_soft_mainline_tail_density_residual_rebalance_manifest_verifier_20260526T1704Z.json"
)
DEFAULT_SCORECARD_ROOT = Path(".tmp_xuan/scorecards")
DEFAULT_ARTIFACT_ROOT = Path(".tmp_xuan/local_verifier_artifacts")


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
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


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def stat(card: dict[str, Any], key: str, field: str) -> float | None:
    raw = card.get(key)
    if not isinstance(raw, dict):
        return None
    return fnum(raw.get(field))


def markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    obs = card["midpoint_observed"]
    diag = card["diagnostics"]
    lines = [
        "# Xuan Midpoint Rerun Readiness Gate",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- same_profile_rerun_ready: `{d['same_profile_rerun_ready']}`",
        f"- profile_redesign_required: `{d['profile_redesign_required']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- next_action: `{d['next_action']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Midpoint Observed",
        "",
        f"- remote_exit_code: `{obs['remote_exit_code']}`",
        f"- partial accepted/fills/rescues: `{obs['accepted_actions']}` / `{obs['queue_supported_fills']}` / `{obs['strict_rescue_or_salvage_rows']}`",
        f"- rescue_per_candidate: `{obs['rescue_per_candidate']}`",
        f"- rescue_per_fill: `{obs['rescue_per_fill']}`",
        f"- same_profile_repeat_allowed: `{obs['same_profile_repeat_allowed']}`",
        "",
        "## Diagnostics",
        "",
        f"- risk_increasing_seed_true_share: `{diag['risk_increasing_seed_true_share']}`",
        f"- strict_rescue_block_lot_cost_to_threshold_p50: `{diag['strict_rescue_block_lot_cost_to_threshold_p50']}`",
        f"- surplus_budget_projected_unpaired_cost_p50: `{diag['surplus_budget_projected_unpaired_cost_p50']}`",
        f"- strict_rescue_close_projected_pair_pnl_after_min: `{diag['strict_rescue_close_projected_pair_pnl_after_min']}`",
        "",
        "## Interpretation",
        "",
    ]
    lines.extend(f"- `{item}`" for item in card["interpretation"])
    lines.append("")
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    transfer = load_json(args.transfer_plan_scorecard)
    midpoint = load_json(args.midpoint_result_scorecard)
    events = load_json(args.event_diagnostics_scorecard)
    density = load_json(args.density_preflight_scorecard)
    prefix = load_json(args.density_prefix_scorecard)
    profile = load_json(args.profile_scorecard)
    manifest = load_json(args.manifest_verifier_scorecard)

    midpoint_decision = body(midpoint, "decision")
    density_observed = body(density, "observed")
    profile_body = body(profile, "profile")
    risk_counts = events.get("candidate_risk_increasing_seed_values")
    risk_counts = risk_counts if isinstance(risk_counts, dict) else {}
    risk_true = fnum(risk_counts.get("true"), 0.0) or 0.0
    risk_false = fnum(risk_counts.get("false"), 0.0) or 0.0
    risk_total = risk_true + risk_false
    risk_true_share = risk_true / risk_total if risk_total else None

    remote_exit = fnum(midpoint_decision.get("remote_exit_code"))
    same_repeat_allowed = bool(midpoint_decision.get("same_profile_repeat_allowed"))
    full_window_valid = bool(midpoint_decision.get("full_window_promotion_evidence_valid"))
    transfer_ready = body(transfer, "decision").get("transfer_plan_ready") is True
    manifest_keep = is_keep(manifest)
    profile_keep = is_keep(profile)
    density_keep = is_keep(density)
    prefix_keep = is_keep(prefix)
    surplus_p50 = stat(events, "surplus_budget_projected_unpaired_cost", "p50")
    surplus_allowed = fnum(profile_body.get("surplus_budget_max_abs_unpaired_cost"))
    lot_cost_to_threshold_p50 = stat(events, "strict_rescue_block_lot_cost_to_threshold", "p50")
    projected_pnl_after_min = stat(events, "strict_rescue_close_projected_pair_pnl_after", "min")

    hard_blockers: list[str] = []
    if not transfer_ready:
        hard_blockers.append("04b6_residual_transfer_plan_not_ready")
    if not manifest_keep:
        hard_blockers.append("manifest_verifier_not_keep")
    if not profile_keep:
        hard_blockers.append("profile_not_keep")
    if remote_exit not in (None, 0.0):
        hard_blockers.append(f"previous_midpoint_remote_exit_{remote_exit:g}")
    if not full_window_valid:
        hard_blockers.append("previous_midpoint_not_full_window_promotion_evidence")
    if not same_repeat_allowed:
        hard_blockers.append("same_midpoint_profile_repeat_disallowed")
    if not density_keep:
        hard_blockers.append("density_preflight_not_keep")
    if not prefix_keep:
        hard_blockers.append("density_prefix_not_keep")
    if risk_true_share is not None and risk_true_share >= args.max_risk_increasing_seed_share_for_rerun:
        hard_blockers.append("risk_increasing_seed_share_too_high_for_same_profile")
    if surplus_p50 is not None and surplus_allowed is not None and surplus_p50 > surplus_allowed:
        hard_blockers.append("surplus_budget_blocks_midpoint_density")

    same_profile_rerun_ready = not hard_blockers
    profile_redesign_required = not same_profile_rerun_ready
    status_value = (
        "KEEP_SHADOW_REVIEW_MIDPOINT_RERUN_READY_LOCAL_ONLY"
        if same_profile_rerun_ready
        else "BLOCKED_SHADOW_REVIEW_MIDPOINT_RERUN_PROFILE_REDESIGN_REQUIRED_LOCAL_ONLY"
    )
    interpretation = [
        "same midpoint profile cannot be rerun as-is because the prior result explicitly disallowed repeats",
        "exit 137 makes the prior midpoint run unusable as promotion evidence",
        "weak rescue density is not solved by relaxing surplus caps because that would re-open the residual tail blocker",
        "risk-increasing seed share is high; the next local redesign should reduce risky seed admissions rather than widen rescue economics",
        "cap75 remains blocked until cap25 residual_cost_to_pair_qty and density pass together",
    ]

    return rounded(
        {
            "artifact": "xuan_shadow_review_midpoint_rerun_readiness_gate",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_midpoint_rerun_readiness_gate.py",
            "status": status_value,
            "inputs": {
                "transfer_plan_scorecard": str(args.transfer_plan_scorecard),
                "midpoint_result_scorecard": str(args.midpoint_result_scorecard),
                "event_diagnostics_scorecard": str(args.event_diagnostics_scorecard),
                "density_preflight_scorecard": str(args.density_preflight_scorecard),
                "density_prefix_scorecard": str(args.density_prefix_scorecard),
                "profile_scorecard": str(args.profile_scorecard),
                "manifest_verifier_scorecard": str(args.manifest_verifier_scorecard),
            },
            "source_statuses": {
                "transfer_plan": status(transfer),
                "midpoint_result": status(midpoint),
                "density_preflight": status(density),
                "density_prefix": status(prefix),
                "profile": status(profile),
                "manifest_verifier": status(manifest),
            },
            "midpoint_observed": {
                "remote_exit_code": remote_exit,
                "accepted_actions": fnum(density_observed.get("accepted_actions")),
                "queue_supported_fills": fnum(density_observed.get("queue_supported_fills")),
                "strict_rescue_or_salvage_rows": fnum(density_observed.get("strict_rescue_or_salvage_rows")),
                "rescue_per_candidate": fnum(density_observed.get("rescue_per_candidate")),
                "rescue_per_fill": fnum(density_observed.get("rescue_per_fill")),
                "same_profile_repeat_allowed": same_repeat_allowed,
                "full_window_promotion_evidence_valid": full_window_valid,
            },
            "diagnostics": {
                "risk_increasing_seed_true": risk_true,
                "risk_increasing_seed_false": risk_false,
                "risk_increasing_seed_true_share": risk_true_share,
                "max_risk_increasing_seed_share_for_rerun": args.max_risk_increasing_seed_share_for_rerun,
                "strict_rescue_block_lot_cost_to_threshold_p50": lot_cost_to_threshold_p50,
                "surplus_budget_projected_unpaired_cost_p50": surplus_p50,
                "surplus_budget_max_abs_unpaired_cost": surplus_allowed,
                "strict_rescue_close_projected_pair_pnl_after_min": projected_pnl_after_min,
                "strict_rescue_block_reasons": events.get("strict_rescue_block_reasons", {}),
                "candidate_soft_decisions": events.get("candidate_soft_decisions", {}),
            },
            "interpretation": interpretation,
            "decision": {
                "same_profile_rerun_ready": same_profile_rerun_ready,
                "profile_redesign_required": profile_redesign_required,
                "future_remote_allowed_by_this_gate": False,
                "cap75_remote_rationale_ready": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "research_only": True,
                "hard_blockers": hard_blockers,
                "required_before_future_remote": [
                    "build a local profile redesign that reduces risk-increasing seed admissions",
                    "keep strict_rescue_surplus_net_cap <= 1.02 and do not relax surplus residual budget",
                    "prove the redesign can target >=7 strict rescues while preserving residual_cost_to_pair_qty <= 0.05",
                    "rerun manifest builder and verifier after any profile change",
                    "check active remote runner conflict before any future bounded PM_DRY_RUN",
                ],
                "next_action": "build_local_post_midpoint_seed_risk_reduction_profile_before_any_remote",
            },
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transfer-plan-scorecard", type=Path, default=DEFAULT_TRANSFER_PLAN)
    parser.add_argument("--midpoint-result-scorecard", type=Path, default=DEFAULT_MIDPOINT_RESULT)
    parser.add_argument("--event-diagnostics-scorecard", type=Path, default=DEFAULT_EVENT_DIAGNOSTICS)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY_PREFLIGHT)
    parser.add_argument("--density-prefix-scorecard", type=Path, default=DEFAULT_DENSITY_PREFIX)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--manifest-verifier-scorecard", type=Path, default=DEFAULT_MANIFEST_VERIFIER)
    parser.add_argument("--max-risk-increasing-seed-share-for-rerun", type=float, default=0.5)
    parser.add_argument("--scorecard-root", type=Path, default=DEFAULT_SCORECARD_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--stamp", default=time.strftime("%Y%m%dT%H%MZ", time.gmtime()))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scorecard = build(args)
    args.scorecard_root.mkdir(parents=True, exist_ok=True)
    artifact_dir = args.artifact_root / f"xuan_shadow_review_midpoint_rerun_readiness_gate_{args.stamp}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    scorecard_path = args.scorecard_root / f"xuan_shadow_review_midpoint_rerun_readiness_gate_{args.stamp}.json"
    markdown_path = artifact_dir / "MIDPOINT_RERUN_READINESS_GATE.md"
    scorecard["scorecard_path"] = str(scorecard_path)
    scorecard["markdown_path"] = str(markdown_path)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(markdown(scorecard), encoding="utf-8")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
