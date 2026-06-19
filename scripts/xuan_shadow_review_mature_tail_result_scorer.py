#!/usr/bin/env python3
"""Score the mature-tail size-control cap25 dry-run result.

This scorer is intentionally local/research-only. It evaluates whether the
cap25_mature_tail_surplus_budget_130_profile actually fixed the residual tail
without weakening density, source, or safety gates.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_LAUNCH = Path(".tmp_xuan/scorecards/xuan_mature_tail_remote_launch_20260526T2148Z.json")
DEFAULT_PREAUTH = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_pre_authorization_gate_20260526T2145Z.json")
DEFAULT_POSTRUN = Path(f".tmp_xuan/scorecards/{DEFAULT_TAG}_postrun_bundle.json")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_PREFIX = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_prefix_scorer.json")
DEFAULT_SOURCE = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_source_caveat_audit.json")
DEFAULT_CAPACITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_capacity_stage_public_benchmark_gate.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_PRIOR_RUNTIME = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z_runtime_summary.json"
)
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_result_20260526T2148Z.json")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))


def load_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        return None
    return load_json(resolved)


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def status(card: dict[str, Any] | None) -> str | None:
    if not isinstance(card, dict):
        return None
    return str(card.get("status", ""))


def is_keep(card: dict[str, Any] | None) -> bool:
    return bool(status(card) and status(card).startswith("KEEP"))


def decision_blockers(card: dict[str, Any] | None) -> list[str]:
    if not isinstance(card, dict):
        return []
    blockers = card.get("hard_blockers")
    if blockers is None:
        blockers = card.get("decision", {}).get("hard_blockers", [])
    return list(blockers or [])


def pct(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value * 100.0


def delta(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None:
        return None
    return current - prior


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    m = card["runtime_metrics"]
    c = card["capacity_metrics"]
    lines = [
        "# Xuan Mature Tail Result",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- mature_tail_runtime_success: `{d['mature_tail_runtime_success']}`",
        f"- runtime_clean: `{d['runtime_clean']}`",
        f"- source_review_clean: `{d['source_review_clean']}`",
        f"- density_gate_pass: `{d['density_gate_pass']}`",
        f"- residual_capacity_gate_pass: `{d['residual_capacity_gate_pass']}`",
        f"- future_remote_allowed_by_this_result: `{d['future_remote_allowed_by_this_result']}`",
        f"- cap75_remote_rationale_ready: `{d['cap75_remote_rationale_ready']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Runtime Metrics",
        "",
        f"- accepted_actions: `{m['accepted_actions']}`",
        f"- queue_supported_fills: `{m['queue_supported_fills']}`",
        f"- strict_rescue_closes: `{m['strict_rescue_closes']}`",
        f"- fill_rate: `{m['fill_rate']}`",
        f"- pair_pnl: `{m['pair_pnl']}`",
        f"- roi_on_filled_cost_pct: `{m['roi_on_filled_cost_pct']}`",
        f"- residual_qty_share_pct: `{m['residual_qty_share_pct']}`",
        f"- residual_cost_share_pct: `{m['residual_cost_share_pct']}`",
        f"- rescue_net_pair_cost_max: `{m['rescue_net_pair_cost_max']}`",
        "",
        "## Capacity Metrics",
        "",
        f"- residual_cost_to_pair_qty_pct: `{c.get('residual_cost_to_pair_qty_pct')}`",
        f"- actual_pair_cost_after_fee: `{c.get('actual_pair_cost_after_fee')}`",
        f"- pair_qty_redeem_notional: `{c.get('pair_qty_redeem_notional')}`",
        "",
        "## Interpretation",
        "",
        f"- runtime_signal: `{card['interpretation']['runtime_signal']}`",
        f"- result_interpretation: `{card['interpretation']['result_interpretation']}`",
        f"- next_action: `{d['next_action']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Does not authorize another remote run by itself.",
        "- Does not authorize cap75/150/300, deploy, restart, live orders, shared service mutation, collector rebuild, or remote repo mutation.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    launch = load_json(args.remote_launch_scorecard)
    preauth = load_json(args.pre_authorization_scorecard)
    postrun = load_json(args.postrun_bundle_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    density = load_json(args.density_preflight_scorecard)
    prefix = load_json(args.density_prefix_scorecard)
    source = load_json(args.source_caveat_audit_scorecard)
    capacity = load_json(args.capacity_stage_scorecard)
    profile = load_json(args.profile_scorecard)
    prior_runtime = load_optional_json(args.prior_runtime_summary_scorecard)

    runtime_metrics = runtime.get("metrics", {})
    capacity_metrics = capacity.get("metrics", {})
    safety = runtime.get("safety", {})
    profile_density = profile.get("density_lower_bound", {})

    accepted = fnum(runtime_metrics.get("accepted_actions"))
    fills = fnum(runtime_metrics.get("queue_supported_fills"))
    fill_rate = fills / accepted if accepted > 0 else 0.0
    strict_rescues = fnum(runtime_metrics.get("strict_rescue_closes"))
    residual_qty_share = fnum(runtime_metrics.get("residual_qty_share"))
    residual_cost_share = fnum(runtime_metrics.get("residual_cost_share"))
    residual_cost_to_pair_qty = fnum(capacity_metrics.get("residual_cost_to_pair_qty"))

    runtime_clean = (
        is_keep(runtime)
        and safety.get("pm_dry_run") is True
        and safety.get("orders_sent") is False
        and safety.get("deploy_or_restart") is False
        and safety.get("shared_service_mutation") is False
        and safety.get("remote_repo_mutation") is False
        and launch.get("status") == "KEEP_MATURE_TAIL_REMOTE_LAUNCH_STARTED_LOCAL_ONLY"
    )
    postrun_complete = is_keep(postrun)
    source_review_clean = is_keep(source) and not decision_blockers(source)
    density_gate_pass = is_keep(density) and is_keep(prefix)
    residual_capacity_gate_pass = capacity.get("decision", {}).get("capacity_stage_gate_pass") is True

    density_lower_bound = {
        "accepted_actual": accepted,
        "accepted_min": fnum(profile_density.get("min_accepted_after")),
        "accepted_pass": accepted >= fnum(profile_density.get("min_accepted_after")),
        "fills_actual": fills,
        "fills_min": fnum(profile_density.get("min_fills_after")),
        "fills_pass": fills >= fnum(profile_density.get("min_fills_after")),
        "fill_rate_actual": fill_rate,
        "fill_rate_min": fnum(profile_density.get("min_fill_rate_after")),
        "fill_rate_pass": fill_rate >= fnum(profile_density.get("min_fill_rate_after")),
        "strict_rescue_actual": strict_rescues,
    }
    density_lower_bound["all_pass"] = (
        density_lower_bound["accepted_pass"]
        and density_lower_bound["fills_pass"]
        and density_lower_bound["fill_rate_pass"]
    )

    residual_gate_checks = {
        "residual_cost_share": {
            "actual": residual_cost_share,
            "max": 0.15,
            "passed": residual_cost_share <= 0.15,
        },
        "residual_qty_share": {
            "actual": residual_qty_share,
            "max": 0.20,
            "passed": residual_qty_share <= 0.20,
        },
        "residual_cost_to_pair_qty": {
            "actual": residual_cost_to_pair_qty,
            "max": 0.05,
            "passed": residual_cost_to_pair_qty <= 0.05,
        },
    }

    hard_blockers: list[str] = []
    if not runtime_clean:
        hard_blockers.append("runtime_not_clean")
    if not postrun_complete:
        hard_blockers.append("postrun_bundle_incomplete")
    if not source_review_clean:
        hard_blockers.extend(f"source:{item}" for item in decision_blockers(source) or ["source_not_keep"])
    if not density_gate_pass:
        hard_blockers.extend(f"density:{item}" for item in decision_blockers(density) or [status(density) or "density_not_keep"])
        hard_blockers.extend(f"prefix:{item}" for item in decision_blockers(prefix) or [status(prefix) or "prefix_not_keep"])
    if not residual_capacity_gate_pass:
        hard_blockers.extend(f"capacity:{item}" for item in decision_blockers(capacity) or ["capacity_stage_gate_blocked"])
    for name, gate in residual_gate_checks.items():
        if not gate["passed"] and f"capacity:max_{name}" not in hard_blockers:
            hard_blockers.append(f"residual_gate:{name}")

    if not runtime_clean or not postrun_complete:
        result_status = "BLOCKED_SHADOW_REVIEW_MATURE_TAIL_RESULT_RUNTIME_OR_POSTRUN_BLOCKED_LOCAL_ONLY"
        interpretation = "runtime_or_postrun_incomplete"
    elif not source_review_clean:
        result_status = "BLOCKED_SHADOW_REVIEW_MATURE_TAIL_RESULT_SOURCE_BLOCKED_LOCAL_ONLY"
        interpretation = "source_not_clean"
    elif not density_gate_pass and not residual_capacity_gate_pass:
        result_status = "BLOCKED_SHADOW_REVIEW_MATURE_TAIL_RESULT_DENSITY_AND_RESIDUAL_BLOCKED_LOCAL_ONLY"
        interpretation = "mature_tail_profile_did_not_preserve_density_or_residual_gates"
    elif not residual_capacity_gate_pass:
        result_status = "BLOCKED_SHADOW_REVIEW_MATURE_TAIL_RESULT_RESIDUAL_CAPACITY_BLOCKED_LOCAL_ONLY"
        interpretation = "mature_tail_profile_left_residual_capacity_blocked"
    elif not density_gate_pass:
        result_status = "BLOCKED_SHADOW_REVIEW_MATURE_TAIL_RESULT_DENSITY_BLOCKED_LOCAL_ONLY"
        interpretation = "mature_tail_profile_starved_density"
    else:
        result_status = "KEEP_SHADOW_REVIEW_MATURE_TAIL_RESULT_PASS_RESEARCH_ONLY"
        interpretation = "mature_tail_profile_passed_local_research_gates"

    mature_tail_runtime_success = result_status.startswith("KEEP")
    prior_metrics = (prior_runtime or {}).get("metrics", {})
    comparisons = {
        "prior_runtime_status": status(prior_runtime),
        "accepted_delta_vs_prior": delta(accepted, fnum(prior_metrics.get("accepted_actions")) if prior_metrics else None),
        "fills_delta_vs_prior": delta(fills, fnum(prior_metrics.get("queue_supported_fills")) if prior_metrics else None),
        "strict_rescue_delta_vs_prior": delta(strict_rescues, fnum(prior_metrics.get("strict_rescue_closes")) if prior_metrics else None),
        "residual_qty_share_delta_vs_prior": delta(
            residual_qty_share, fnum(prior_metrics.get("residual_qty_share")) if prior_metrics else None
        ),
        "residual_cost_share_delta_vs_prior": delta(
            residual_cost_share, fnum(prior_metrics.get("residual_cost_share")) if prior_metrics else None
        ),
    }

    card = {
        "artifact": "xuan_shadow_review_mature_tail_result_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_mature_tail_result_scorer.py",
        "status": result_status,
        "inputs": {
            "remote_launch_scorecard": str(Path(args.remote_launch_scorecard).expanduser().resolve()),
            "pre_authorization_scorecard": str(Path(args.pre_authorization_scorecard).expanduser().resolve()),
            "postrun_bundle_scorecard": str(Path(args.postrun_bundle_scorecard).expanduser().resolve()),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "density_prefix_scorecard": str(Path(args.density_prefix_scorecard).expanduser().resolve()),
            "source_caveat_audit_scorecard": str(Path(args.source_caveat_audit_scorecard).expanduser().resolve()),
            "capacity_stage_scorecard": str(Path(args.capacity_stage_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
            "prior_runtime_summary_scorecard": str(Path(args.prior_runtime_summary_scorecard).expanduser().resolve())
            if args.prior_runtime_summary_scorecard
            else None,
        },
        "source_statuses": {
            "remote_launch": status(launch),
            "pre_authorization": status(preauth),
            "postrun_bundle": status(postrun),
            "runtime_summary": status(runtime),
            "density_preflight": status(density),
            "density_prefix": status(prefix),
            "source_caveat_audit": status(source),
            "capacity_stage": status(capacity),
            "profile": status(profile),
        },
        "runtime_metrics": {
            "accepted_actions": accepted,
            "queue_supported_fills": fills,
            "strict_rescue_closes": strict_rescues,
            "fill_rate": fill_rate,
            "pair_pnl": fnum(runtime_metrics.get("pair_pnl")),
            "roi_on_filled_cost": fnum(runtime_metrics.get("roi_on_filled_cost")),
            "roi_on_filled_cost_pct": pct(fnum(runtime_metrics.get("roi_on_filled_cost"))),
            "residual_qty_share": residual_qty_share,
            "residual_qty_share_pct": pct(residual_qty_share),
            "residual_cost_share": residual_cost_share,
            "residual_cost_share_pct": pct(residual_cost_share),
            "rescue_net_pair_cost_max": fnum(runtime_metrics.get("rescue_net_pair_cost_max")),
            "strict_rescue_source_blocks": fnum(runtime_metrics.get("strict_rescue_source_blocks")),
        },
        "capacity_metrics": capacity_metrics,
        "density_lower_bound": density_lower_bound,
        "residual_gate_checks": residual_gate_checks,
        "comparison_vs_prior_capped": comparisons,
        "interpretation": {
            "runtime_signal": "completed_cleanly_with_aggregate" if runtime_clean and postrun_complete else "runtime_or_postrun_blocked",
            "source_signal": "source_clean" if source_review_clean else "source_blocked",
            "density_signal": "density_pass" if density_gate_pass else "density_blocked",
            "residual_signal": "residual_capacity_pass" if residual_capacity_gate_pass else "residual_capacity_blocked",
            "result_interpretation": interpretation,
        },
        "decision": {
            "mature_tail_runtime_success": mature_tail_runtime_success,
            "runtime_clean": runtime_clean,
            "postrun_complete": postrun_complete,
            "source_review_clean": source_review_clean,
            "density_gate_pass": density_gate_pass,
            "density_lower_bound_pass": density_lower_bound["all_pass"],
            "residual_capacity_gate_pass": residual_capacity_gate_pass,
            "hard_blockers": sorted(set(hard_blockers)),
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_result": False,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "next_action": (
                "local_tail_diagnosis_or_profile_redesign_before_any_new_remote"
                if not mature_tail_runtime_success
                else "local_review_packet_before_any_capacity_or_private_truth_decision"
            ),
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote-launch-scorecard", type=Path, default=DEFAULT_LAUNCH)
    parser.add_argument("--pre-authorization-scorecard", type=Path, default=DEFAULT_PREAUTH)
    parser.add_argument("--postrun-bundle-scorecard", type=Path, default=DEFAULT_POSTRUN)
    parser.add_argument("--runtime-summary-scorecard", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--density-prefix-scorecard", type=Path, default=DEFAULT_PREFIX)
    parser.add_argument("--source-caveat-audit-scorecard", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--capacity-stage-scorecard", type=Path, default=DEFAULT_CAPACITY)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--prior-runtime-summary-scorecard", type=Path, default=DEFAULT_PRIOR_RUNTIME)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=None)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
