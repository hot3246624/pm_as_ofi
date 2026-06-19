#!/usr/bin/env python3
"""Build a concrete cap25 tail-mitigation profile hypothesis.

This local-only builder converts the residual-tail mitigation gate into a
profile scorecard that can be staged by the manifest builder later. It does not
launch remote jobs, approve deploy, approve live orders, or relax economics.
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


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def status(card: dict[str, Any]) -> str | None:
    raw = card.get("status")
    return str(raw) if raw else None


def is_keep(card: dict[str, Any]) -> bool:
    return (status(card) or "").startswith("KEEP")


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def with_default_profile(profile: dict[str, Any]) -> dict[str, Any]:
    out = dict(profile)
    out.setdefault("profile_family", "residual_guard")
    out.setdefault("duration_s", 1800)
    out.setdefault("round_offsets", "0,1,2,3,4,5,6,7,8,9,10,11,12")
    out.setdefault("allow_concurrent_shared_ingress_readers", True)
    out.setdefault("write_normalized_lifecycle", True)
    out.setdefault("write_rescue_block_diagnostics", True)
    out.setdefault("source_quality_require_l1_source", True)
    out.setdefault("source_quality_require_l2_source", True)
    out.setdefault("source_quality_require_trade_source", True)
    out.setdefault("strict_rescue_require_book_source", True)
    out.setdefault("strict_rescue_require_l2_source", True)
    out.setdefault("strict_rescue_skip_low_cost_lots", True)
    return out


def build_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    target = card["tail_targets"]
    lines = [
        "# Xuan Tail-Mitigation Profile Hypothesis",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- profile_hypothesis_ready: `{decision['profile_hypothesis_ready']}`",
        f"- future_remote_rationale_candidate: `{decision['future_remote_rationale_candidate']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        f"- next_action: `{decision['next_action']}`",
        "",
        "## Tail Targets",
        "",
        f"- residual_cost_lte: `{target['residual_cost_lte']}`",
        f"- residual_qty_lte: `{target['residual_qty_lte']}`",
        f"- public_target_residual_qty_lte: `{target['public_target_residual_qty_lte']}`",
        "",
        "## Profile Changes",
        "",
        "| Field | Current | Proposed | Reason |",
        "| --- | ---: | ---: | --- |",
    ]
    for row in card["profile_changes"]:
        lines.append(f"| `{row['field']}` | `{row['current']}` | `{row['proposed']}` | {row['reason']} |")
    lines.extend(
        [
            "",
            "## Pass Criteria For Future Sample",
            "",
        ]
    )
    for key, value in card["minimum_window_acceptance"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- This is a local profile hypothesis, not remote authorization.",
            "- The profile must pass manifest verifier before any future bounded dry-run.",
            "- Do not widen strict_rescue_surplus_net_cap above 1.02.",
            "- Do not enable closeability cancel guard for the mainline.",
            "- Do not deploy, restart, send live orders, or mutate shared services.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    gate = load_json(args.tail_mitigation_gate_scorecard)
    base = load_json(args.base_profile_scorecard)
    gate_decision = body(gate, "decision")
    targets = body(gate, "mitigation_targets")
    base_profile = with_default_profile(body(base, "profile"))

    cost_target = fnum(targets.get("max_residual_cost_for_remote_hard"))
    qty_target = fnum(targets.get("max_residual_qty_for_remote_hard"))
    public_qty_target = fnum(targets.get("max_residual_qty_for_public_target"))
    current_surplus_budget = fnum(base_profile.get("surplus_budget_max_abs_unpaired_cost"))
    current_imbalance = fnum(base_profile.get("imbalance_qty_cap"))
    proposed_surplus_budget = min(current_surplus_budget or args.default_surplus_budget, cost_target or args.default_surplus_budget)
    proposed_surplus_budget = math.floor(proposed_surplus_budget * 100.0) / 100.0
    proposed_imbalance = min(current_imbalance or args.default_imbalance_qty_cap, args.proposed_imbalance_qty_cap)

    proposed_profile = dict(base_profile)
    proposed_profile["surplus_budget_max_abs_unpaired_cost"] = proposed_surplus_budget
    proposed_profile["imbalance_qty_cap"] = proposed_imbalance
    proposed_profile["strict_rescue_surplus_net_cap"] = min(
        fnum(base_profile.get("strict_rescue_surplus_net_cap"), args.max_strict_rescue_surplus_net_cap)
        or args.max_strict_rescue_surplus_net_cap,
        args.max_strict_rescue_surplus_net_cap,
    )
    proposed_profile["risk_seed_cancel_on_closeability_net_cap"] = None
    proposed_profile["risk_seed_pending_opp_credit"] = fnum(
        base_profile.get("risk_seed_pending_opp_credit"), 0.5
    )
    proposed_profile["profile_family"] = "residual_guard"
    proposed_profile["tail_mitigation_hypothesis"] = "cap25_surplus_budget_bound_imbalance_trim"

    hard_blockers: list[str] = []
    if not is_keep(gate):
        hard_blockers.append("tail_mitigation_gate_not_keep")
    if not base_profile:
        hard_blockers.append("base_profile_missing")
    if proposed_surplus_budget > (cost_target or proposed_surplus_budget):
        hard_blockers.append("proposed_surplus_budget_above_tail_cost_target")
    if proposed_profile.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        hard_blockers.append("closeability_cancel_guard_enabled")
    if fnum(proposed_profile.get("strict_rescue_surplus_net_cap"), 9.0) > args.max_strict_rescue_surplus_net_cap:
        hard_blockers.append("strict_rescue_surplus_net_cap_relaxed")

    status_value = (
        "KEEP_SHADOW_REVIEW_TAIL_MITIGATION_PROFILE_HYPOTHESIS_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_TAIL_MITIGATION_PROFILE_HYPOTHESIS_BLOCKED_LOCAL_ONLY"
    )
    profile_changes = [
        {
            "field": "surplus_budget_max_abs_unpaired_cost",
            "current": current_surplus_budget,
            "proposed": proposed_profile.get("surplus_budget_max_abs_unpaired_cost"),
            "reason": "bound mature-material residual cost to the tail gate hard target",
        },
        {
            "field": "imbalance_qty_cap",
            "current": current_imbalance,
            "proposed": proposed_profile.get("imbalance_qty_cap"),
            "reason": "trim unpaired qty while keeping cap25 capacity scale",
        },
        {
            "field": "strict_rescue_surplus_net_cap",
            "current": base_profile.get("strict_rescue_surplus_net_cap"),
            "proposed": proposed_profile.get("strict_rescue_surplus_net_cap"),
            "reason": "preserve explicit-surplus bridge economics without widening",
        },
        {
            "field": "risk_seed_cancel_on_closeability_net_cap",
            "current": base_profile.get("risk_seed_cancel_on_closeability_net_cap"),
            "proposed": proposed_profile.get("risk_seed_cancel_on_closeability_net_cap"),
            "reason": "keep no-cancel mainline to avoid fill starvation",
        },
    ]

    return rounded(
        {
            "artifact": "xuan_shadow_review_tail_mitigation_profile_hypothesis",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_tail_mitigation_profile_hypothesis.py",
            "status": status_value,
            "candidate": "cap25_tail_budget_bound_imbalance_trim",
            "inputs": {
                "tail_mitigation_gate_scorecard": args.tail_mitigation_gate_scorecard,
                "base_profile_scorecard": args.base_profile_scorecard,
            },
            "source_statuses": {
                "tail_mitigation_gate": status(gate),
                "base_profile": status(base),
            },
            "decision": {
                "profile_hypothesis_ready": status_value.startswith("KEEP"),
                "future_remote_rationale_candidate": status_value.startswith("KEEP"),
                "future_remote_allowed_by_this_hypothesis": False,
                "requires_manifest_verifier_pass": True,
                "requires_no_runner_conflict_check": True,
                "requires_future_heartbeat": True,
                "remote_runner_allowed": False,
                "research_only": True,
                "paper_shadow_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "hard_blockers": hard_blockers,
                "next_action": "prepare_manifest_verifier_for_tail_mitigation_profile_before_any_future_remote"
                if status_value.startswith("KEEP")
                else "fix_tail_mitigation_profile_hypothesis_locally",
            },
            "tail_targets": {
                "residual_cost_lte": cost_target,
                "residual_qty_lte": qty_target,
                "public_target_residual_qty_lte": public_qty_target,
                "required_residual_cost_reduction": targets.get(
                    "required_residual_cost_reduction_for_remote_hard"
                ),
                "required_residual_qty_reduction": targets.get(
                    "required_residual_qty_reduction_for_remote_hard"
                ),
            },
            "profile_changes": profile_changes,
            "profile": proposed_profile,
            "minimum_window_acceptance": {
                "accepted_actions": 25,
                "queue_supported_fills": 20,
                "strict_rescue_closes": 7,
                "pair_pnl": 0.0,
                "max_residual_cost": cost_target,
                "max_residual_qty": qty_target,
                "max_residual_cost_share": 0.15,
                "max_residual_qty_share": 0.2,
                "max_residual_cost_to_pair_qty": 0.05,
                "max_rescue_l1_age_ms": 50,
                "strict_rescue_source_blocks": 0,
                "orders_sent": False,
                "stderr_bytes": 0,
            },
            "safety": {
                "remote_runner_allowed": False,
                "deployable": False,
                "live_trading_allowed": False,
                "orders_sent": False,
                "shared_service_mutation": False,
                "remote_repo_mutation": False,
                "collector_rebuild": False,
                "cron_loop": False,
            },
            "interpretation": {
                "decision_changing_artifact": True,
                "not_remote_authorization": True,
                "next_decision_depends_on_manifest_verifier": True,
                "do_not_count_prior_bridge_as_mitigation_evidence": True,
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tail-mitigation-gate-scorecard", required=True)
    parser.add_argument("--base-profile-scorecard", required=True)
    parser.add_argument("--max-strict-rescue-surplus-net-cap", type=float, default=1.02)
    parser.add_argument("--proposed-imbalance-qty-cap", type=float, default=5.0)
    parser.add_argument("--default-surplus-budget", type=float, default=2.0)
    parser.add_argument("--default-imbalance-qty-cap", type=float, default=6.25)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    scorecard = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(build_markdown(scorecard) + "\n")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if scorecard["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
