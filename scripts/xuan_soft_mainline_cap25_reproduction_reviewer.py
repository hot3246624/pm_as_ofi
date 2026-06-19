#!/usr/bin/env python3
"""Review the cap25 backfill window as a soft-mainline reproduction candidate.

This local-only scorer compares the cap25 backfill runtime/profile against the
current no-cancel soft-mainline lane. It is deliberately narrower than the
generic existing-window scan: the goal is to decide whether the cap25 evidence
is worth a fresh bounded reproduction plan, not to promote historical evidence.
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
    p = Path(path).expanduser().resolve()
    if not p.exists():
        return {}
    raw = json.loads(p.read_text())
    return raw if isinstance(raw, dict) else {}


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    out = fnum(value)
    return int(out) if out is not None else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def metrics(card: dict[str, Any]) -> dict[str, Any]:
    src = body(card, "metrics")
    return {
        "status": card.get("status"),
        "accepted_actions": inum(src.get("accepted_actions")),
        "queue_supported_fills": inum(src.get("queue_supported_fills")),
        "strict_rescue_closes": inum(src.get("strict_rescue_closes")),
        "strict_rescue_qty": fnum(src.get("strict_rescue_qty")),
        "filled_cost": fnum(src.get("filled_cost")),
        "pair_pnl": fnum(src.get("pair_pnl"), 0.0) or 0.0,
        "roi_on_filled_cost": fnum(src.get("roi_on_filled_cost")),
        "residual_qty_share": fnum(src.get("residual_qty_share")),
        "residual_cost_share": fnum(src.get("residual_cost_share")),
        "rescue_net_pair_cost_max": fnum(src.get("rescue_net_pair_cost_max")),
        "accepted_l1_age_ms_max": fnum(src.get("accepted_l1_age_ms_max")),
        "rescue_l1_age_ms_max": fnum(src.get("rescue_l1_age_ms_max")),
        "strict_rescue_source_blocks": fnum(src.get("strict_rescue_source_blocks"), 0.0) or 0.0,
    }


PROFILE_KEYS = [
    "target_qty",
    "max_seed_qty",
    "max_open_cost",
    "imbalance_qty_cap",
    "risk_seed_closeability_soft_net_cap",
    "risk_seed_closeability_debt_floor",
    "risk_seed_closeability_debt_budget",
    "risk_seed_pending_opp_credit",
    "risk_seed_cancel_on_closeability_net_cap",
    "pair_completion_net_cap",
    "pair_completion_min_pair_pnl_after",
    "strict_rescue_surplus_net_cap",
    "strict_rescue_min_pair_pnl_after",
    "strict_rescue_salvage_net_cap",
    "salvage_net_cap",
    "activation_mode",
    "activation_window_s",
    "late_repair_after_s",
    "strict_rescue_skip_low_cost_lots",
]


def profile_summary(card: dict[str, Any]) -> dict[str, Any]:
    src = body(card, "profile")
    return {key: src.get(key) for key in PROFILE_KEYS}


def compare_profiles(candidate: dict[str, Any], mainline: dict[str, Any]) -> dict[str, Any]:
    diffs: dict[str, Any] = {}
    for key in PROFILE_KEYS:
        left = candidate.get(key)
        right = mainline.get(key)
        if left != right:
            diffs[key] = {"candidate": left, "mainline": right}
    return diffs


def gate_candidate(m: dict[str, Any], p: dict[str, Any], args: argparse.Namespace) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not str(m.get("status") or "").startswith("KEEP"):
        blockers.append("runtime_status_not_keep")
    if m["accepted_actions"] < args.min_accepted_actions:
        blockers.append("accepted_actions_below_reproduction_floor")
    if m["queue_supported_fills"] < args.min_fills:
        blockers.append("fills_below_reproduction_floor")
    if m["strict_rescue_closes"] < args.min_strict_rescue_closes:
        blockers.append("strict_rescue_closes_below_reproduction_floor")
    if m["pair_pnl"] < args.min_pair_pnl:
        blockers.append("pair_pnl_below_floor")
    if m["residual_qty_share"] is None or m["residual_qty_share"] > args.max_residual_qty_share:
        blockers.append("residual_qty_share_above_max")
    if m["residual_cost_share"] is None or m["residual_cost_share"] > args.max_residual_cost_share:
        blockers.append("residual_cost_share_above_max")
    if m["rescue_l1_age_ms_max"] is not None and m["rescue_l1_age_ms_max"] > args.max_rescue_l1_age_ms:
        blockers.append("rescue_l1_age_above_max")
    if m["accepted_l1_age_ms_max"] is not None and m["accepted_l1_age_ms_max"] > args.max_accepted_l1_age_ms:
        blockers.append("accepted_l1_age_above_max")
    if m["strict_rescue_source_blocks"] > args.max_source_blocks:
        blockers.append("source_blocks_present")

    if fnum(p.get("risk_seed_cancel_on_closeability_net_cap")) is not None:
        blockers.append("closeability_cancel_guard_enabled")
    if fnum(p.get("risk_seed_pending_opp_credit")) != args.expected_pending_opp_credit:
        blockers.append("pending_opp_credit_not_current")
    if fnum(p.get("pair_completion_net_cap")) is None or fnum(p.get("pair_completion_net_cap")) > args.max_pair_completion_net_cap:
        blockers.append("pair_completion_net_cap_relaxed")
    if fnum(p.get("pair_completion_min_pair_pnl_after"), 0.0) < args.min_pair_completion_pair_pnl_after:
        blockers.append("pair_completion_pair_pnl_floor_relaxed")
    if fnum(p.get("strict_rescue_surplus_net_cap")) is None:
        blockers.append("missing_explicit_surplus_cap")
    elif fnum(p.get("strict_rescue_surplus_net_cap")) > args.max_strict_rescue_surplus_net_cap:
        blockers.append("strict_rescue_surplus_net_cap_relaxed")
    if fnum(p.get("strict_rescue_min_pair_pnl_after"), 0.0) < args.min_strict_rescue_pair_pnl_after:
        blockers.append("strict_rescue_pair_pnl_floor_relaxed")
    if p.get("strict_rescue_skip_low_cost_lots") is not True:
        blockers.append("low_cost_lot_skip_not_enabled")

    rescue_max = m["rescue_net_pair_cost_max"]
    if rescue_max is not None and rescue_max > args.target_rescue_net_pair_cost:
        warnings.append("strict_target_rescue_cap_missed_but_explicit_surplus_policy_used")
    if fnum(p.get("target_qty")) and fnum(p.get("target_qty")) > args.mainline_reference_target_qty:
        warnings.append("candidate_is_capacity_scaled_not_same_size_as_mainline")
    if fnum(p.get("imbalance_qty_cap")) and fnum(p.get("imbalance_qty_cap")) > args.mainline_reference_imbalance_qty_cap:
        warnings.append("candidate_allows_larger_unpaired_qty_than_mainline")
    return blockers, warnings


def failed_repair_summary(card: dict[str, Any]) -> dict[str, Any]:
    if not card:
        return {}
    m = metrics(card)
    return {
        "status": m["status"],
        "accepted_actions": m["accepted_actions"],
        "queue_supported_fills": m["queue_supported_fills"],
        "strict_rescue_closes": m["strict_rescue_closes"],
        "pair_pnl": m["pair_pnl"],
        "residual_qty_share": m["residual_qty_share"],
        "residual_cost_share": m["residual_cost_share"],
        "interpretation": "failed_residual_repair_variant_do_not_copy",
    }


def build_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    candidate = card["candidate"]
    lines = [
        "# Soft-Mainline Cap25 Reproduction Review",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{decision['next_action']}`",
        "",
        "## Candidate Runtime",
        "",
        f"- accepted/fills/rescues: `{candidate['metrics']['accepted_actions']}` / "
        f"`{candidate['metrics']['queue_supported_fills']}` / `{candidate['metrics']['strict_rescue_closes']}`",
        f"- pair PnL: `{candidate['metrics']['pair_pnl']}`",
        f"- residual qty/cost share: `{candidate['metrics']['residual_qty_share']}` / "
        f"`{candidate['metrics']['residual_cost_share']}`",
        f"- rescue net pair cost max: `{candidate['metrics']['rescue_net_pair_cost_max']}`",
        "",
        "## Profile Decision",
        "",
        f"- recommended lane: `{decision['recommended_lane']}`",
        f"- do not copy residual repair variant: `{decision['do_not_copy_failed_repair_variant']}`",
        f"- remote runner allowed now: `{decision['remote_runner_allowed']}`",
        "",
        "## Guardrails",
        "",
        "- This is local planning evidence only, not shadow or deployment approval.",
        "- Keep no-cancel soft-closeability as the mainline.",
        "- Do not widen strict rescue surplus cap above 1.02 for this reproduction lane.",
        "- Require a fresh 1800s PM_DRY_RUN reproduction before treating cap25 as current evidence.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    candidate_runtime = load_json(args.candidate_runtime)
    candidate_profile_card = load_json(args.candidate_profile)
    mainline_profile_card = load_json(args.mainline_profile)
    good_runtime = load_json(args.reference_good_runtime)
    weak_runtime = load_json(args.reference_weak_runtime)
    failed_repair_runtime = load_json(args.failed_repair_runtime)
    residual_diagnostic = load_json(args.residual_diagnostic)

    candidate_profile = profile_summary(candidate_profile_card or candidate_runtime)
    if not any(value is not None for value in candidate_profile.values()):
        candidate_profile = profile_summary(candidate_runtime)
    mainline_profile = profile_summary(mainline_profile_card)
    candidate_metrics = metrics(candidate_runtime)
    blockers, warnings = gate_candidate(candidate_metrics, candidate_profile, args)

    residual_diag_blockers = residual_diagnostic.get("hard_blockers")
    if not isinstance(residual_diag_blockers, list):
        residual_diag_blockers = []
    if "strict_rescue_surplus_net_cap_dominant" in residual_diag_blockers:
        warnings.append("residual_diagnostic_says_surplus_cap_was_active")
    if "soft_admission_above_strict_rescue_cap" in residual_diag_blockers:
        warnings.append("soft_admission_above_strict_target_requires_explicit_surplus_accounting")

    failed_summary = failed_repair_summary(failed_repair_runtime)
    failed_pair_pnl = fnum(failed_summary.get("pair_pnl"))
    if failed_pair_pnl is not None and failed_pair_pnl < 0:
        warnings.append("failed_residual_repair_variant_had_negative_pair_pnl")

    if blockers:
        status = "BLOCKED_SOFT_MAINLINE_CAP25_REPRODUCTION_REVIEW_PROFILE_OR_SAMPLE_LOCAL_ONLY"
        next_action = "stay_local_fix_profile_before_manifest"
        recommended_lane = "do_not_reproduce_until_blockers_clear"
    else:
        status = "KEEP_SOFT_MAINLINE_CAP25_REPRODUCTION_REVIEW_READY_LOCAL_ONLY"
        next_action = "prepare_cap25_current_economics_manifest_for_future_density_gated_reproduction"
        recommended_lane = "raw_cap25_current_economics_reproduction_not_residual_repair"

    card = {
        "artifact": "xuan_soft_mainline_cap25_reproduction_reviewer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_cap25_reproduction_reviewer.py",
        "status": status,
        "inputs": {
            "candidate_runtime": str(Path(args.candidate_runtime).expanduser()),
            "candidate_profile": str(Path(args.candidate_profile).expanduser()) if args.candidate_profile else None,
            "mainline_profile": str(Path(args.mainline_profile).expanduser()) if args.mainline_profile else None,
            "reference_good_runtime": str(Path(args.reference_good_runtime).expanduser())
            if args.reference_good_runtime
            else None,
            "reference_weak_runtime": str(Path(args.reference_weak_runtime).expanduser())
            if args.reference_weak_runtime
            else None,
            "failed_repair_runtime": str(Path(args.failed_repair_runtime).expanduser())
            if args.failed_repair_runtime
            else None,
            "residual_diagnostic": str(Path(args.residual_diagnostic).expanduser())
            if args.residual_diagnostic
            else None,
        },
        "decision": {
            "deployable": False,
            "shadow_ready": False,
            "remote_runner_allowed": False,
            "research_only": True,
            "hard_blockers": blockers,
            "warnings": sorted(set(warnings)),
            "next_action": next_action,
            "recommended_lane": recommended_lane,
            "do_not_copy_failed_repair_variant": bool(failed_summary),
            "requires_fresh_reproduction_runtime": True,
        },
        "candidate": {
            "metrics": candidate_metrics,
            "profile": candidate_profile,
            "profile_diff_vs_mainline": compare_profiles(candidate_profile, mainline_profile),
        },
        "references": {
            "good_window_metrics": metrics(good_runtime) if good_runtime else {},
            "weak_window_metrics": metrics(weak_runtime) if weak_runtime else {},
            "failed_repair_summary": failed_summary,
            "residual_diagnostic_status": residual_diagnostic.get("status"),
            "residual_diagnostic_hard_blockers": residual_diag_blockers,
        },
        "reproduction_profile_constraints": {
            "keep_no_cancel": True,
            "max_strict_rescue_surplus_net_cap": args.max_strict_rescue_surplus_net_cap,
            "max_pair_completion_net_cap": args.max_pair_completion_net_cap,
            "min_pair_completion_pair_pnl_after": args.min_pair_completion_pair_pnl_after,
            "min_strict_rescue_pair_pnl_after": args.min_strict_rescue_pair_pnl_after,
            "expected_pending_opp_credit": args.expected_pending_opp_credit,
            "target_rescue_net_pair_cost": args.target_rescue_net_pair_cost,
            "allow_capacity_scale_only_as_fresh_reproduction": True,
        },
        "thresholds": {
            "min_accepted_actions": args.min_accepted_actions,
            "min_fills": args.min_fills,
            "min_strict_rescue_closes": args.min_strict_rescue_closes,
            "min_pair_pnl": args.min_pair_pnl,
            "max_residual_qty_share": args.max_residual_qty_share,
            "max_residual_cost_share": args.max_residual_cost_share,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_source_blocks": args.max_source_blocks,
        },
        "guardrails": [
            "Local review only; no remote launch and no shadow/deploy approval.",
            "Historical cap25 evidence requires fresh reproduction under current manifest launch and postrun scorers.",
            "Do not copy the failed residual-repair profile that widened surplus cap to 1.05.",
            "Do not re-enable closeability deterioration cancel guard for this lane.",
        ],
    }
    return rounded(card)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-runtime",
        default=".tmp_xuan/scorecards/no_order_xuan-frontier-capacity-ladder-cap25-20260524T0723Z_runtime_summary.json",
    )
    parser.add_argument(
        "--candidate-profile",
        default=".tmp_xuan/scorecards/xuan_frontier_capacity_ladder_cap25_profile_20260524T0000Z.json",
    )
    parser.add_argument(
        "--mainline-profile",
        default=".tmp_xuan/scorecards/no_order_soft_mainline_explicit_surplus_no_cancel_profile_20260525T0505Z.json",
    )
    parser.add_argument(
        "--reference-good-runtime",
        default=".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-explicit-surplus-no-cancel-20260525T0510Z_runtime_summary.json",
    )
    parser.add_argument(
        "--reference-weak-runtime",
        default=".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-explicit-surplus-no-cancel-20260525T0621Z_runtime_summary.json",
    )
    parser.add_argument(
        "--failed-repair-runtime",
        default=".tmp_xuan/scorecards/no_order_xuan-frontier-capacity-ladder-cap25-residual-repair-20260524T0900Z_runtime_summary.json",
    )
    parser.add_argument(
        "--residual-diagnostic",
        default=".tmp_xuan/scorecards/no_order_xuan-frontier-capacity-ladder-cap25-20260524T0723Z_residual_regime_diagnostic.json",
    )
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-output")
    parser.add_argument("--min-accepted-actions", type=int, default=25)
    parser.add_argument("--min-fills", type=int, default=20)
    parser.add_argument("--min-strict-rescue-closes", type=int, default=7)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-accepted-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-source-blocks", type=float, default=0.0)
    parser.add_argument("--expected-pending-opp-credit", type=float, default=0.5)
    parser.add_argument("--max-pair-completion-net-cap", type=float, default=1.05)
    parser.add_argument("--min-pair-completion-pair-pnl-after", type=float, default=0.0)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
    parser.add_argument("--min-strict-rescue-pair-pnl-after", type=float, default=0.0)
    parser.add_argument("--target-rescue-net-pair-cost", type=float, default=0.95)
    parser.add_argument("--mainline-reference-target-qty", type=float, default=5.0)
    parser.add_argument("--mainline-reference-imbalance-qty-cap", type=float, default=1.25)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
