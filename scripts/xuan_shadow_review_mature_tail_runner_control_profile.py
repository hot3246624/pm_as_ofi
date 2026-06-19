#!/usr/bin/env python3
"""Score a runner-supported profile for mature residual-tail control.

This local-only scorer turns the mature-tail size-control hypothesis into a
concrete runner-supported profile delta. It audits the capped cap25 run output
to verify that a tighter surplus budget would cut or pair-complete the selected
mature tail lots while preserving source/economic gates and density lower
bounds. It does not authorize remote runs, deployment, live orders, capacity
expansion, or private-truth claims.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


RUN_TAG = "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z"
DEFAULT_REMOTE_OUTPUTS = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"{RUN_TAG}/remote_outputs"
)
DEFAULT_MATURE_TAIL_PLAN = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_size_control_plan_20260526T1938Z.json"
)
DEFAULT_CURRENT_PROFILE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_density_preserving_pair_completion_profile_capped_diagnostics_20260526T1750Z.json"
)
DEFAULT_CAPACITY_INTERPRETATION = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_cap25_capped_capacity_interpretation_20260526T1915Z.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    "xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z/"
    "MATURE_TAIL_RUNNER_CONTROL_PROFILE.md"
)


def load_json(path: str | Path) -> dict[str, Any]:
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


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def slug_from_path(path: Path, suffix: str) -> str:
    name = path.name
    return name[: -len(suffix)] if name.endswith(suffix) else path.stem


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_accepted_actions(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    accepted: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.glob("*.would_action_decisions.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if row.get("kind") == "candidate" and row.get("decision") == "accept" and qid:
                out: dict[str, Any] = dict(row)
                out["_source_file"] = str(path)
                out["_slug"] = row.get("slug") or slug_from_path(path, ".would_action_decisions.csv")
                accepted[qid] = out
    return accepted


def load_fill_qids(remote_outputs: Path) -> set[str]:
    fills: set[str] = set()
    for path in sorted(remote_outputs.glob("*.would_fill_events.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if qid:
                fills.add(qid)
    return fills


def load_residual_lots(remote_outputs: Path) -> list[dict[str, Any]]:
    lots: list[dict[str, Any]] = []
    for path in sorted(remote_outputs.glob("*.residual_fifo_lots.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if not qid:
                continue
            out: dict[str, Any] = dict(row)
            out["_source_file"] = str(path)
            out["_slug"] = row.get("slug") or slug_from_path(path, ".residual_fifo_lots.csv")
            out["_qty"] = fnum(row.get("qty"), 0.0) or 0.0
            out["_cost"] = fnum(row.get("cost"), 0.0) or 0.0
            lots.append(out)
    return lots


def load_slug_metrics(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.glob("*.summary.json")):
        card = load_json(path)
        slug = str(card.get("slug") or slug_from_path(path, ".summary.json"))
        metrics[slug] = body(card, "metrics")
    return metrics


def aggregate_metrics(remote_outputs: Path) -> dict[str, Any]:
    return body(load_json(remote_outputs / "aggregate_report.json"), "metrics")


def selected_qids(plan: dict[str, Any]) -> set[str]:
    selected = body(plan, "selected_tail_cut")
    out: set[str] = set()
    for lot in listify(selected.get("selected_lots")):
        if isinstance(lot, dict) and lot.get("quote_intent_id"):
            out.add(str(lot["quote_intent_id"]))
    return out


def pair_completion_covers_full_action(row: dict[str, Any]) -> bool:
    qty = fnum(row.get("qty"), 0.0) or 0.0
    completion_qty = fnum(row.get("pair_completion_qty"), 0.0) or 0.0
    worst_net = fnum(row.get("pair_completion_worst_net_pair_cost"))
    net_cap = fnum(row.get("pair_completion_net_cap"))
    projected_pair_pnl_after = fnum(row.get("pair_completion_projected_pair_pnl_after"), 0.0) or 0.0
    min_pair_pnl_after = fnum(row.get("pair_completion_min_pair_pnl_after"), 0.0) or 0.0
    decision = row.get("pair_completion_decision")
    return (
        decision == "allow"
        and qty > 0.0
        and completion_qty + 1e-12 >= qty
        and worst_net is not None
        and net_cap is not None
        and worst_net <= net_cap + 1e-12
        and projected_pair_pnl_after + 1e-12 >= min_pair_pnl_after
    )


def action_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "slug": row.get("_slug") or row.get("slug"),
        "quote_intent_id": row.get("quote_intent_id"),
        "side": row.get("side"),
        "price": fnum(row.get("price")),
        "qty": fnum(row.get("qty")),
        "surplus_budget_projected_unpaired_cost": fnum(row.get("surplus_budget_projected_unpaired_cost")),
        "pair_completion_qty": fnum(row.get("pair_completion_qty")),
        "pair_completion_worst_net_pair_cost": fnum(row.get("pair_completion_worst_net_pair_cost")),
        "pair_completion_projected_pair_pnl_after": fnum(row.get("pair_completion_projected_pair_pnl_after")),
        "source_quality_decision": row.get("source_quality_decision"),
        "source_quality_l1_age_ms": fnum(row.get("source_quality_l1_age_ms")),
        "closeability_net_pair_cost": fnum(row.get("closeability_net_pair_cost")),
    }


def lot_snapshot(lot: dict[str, Any], action: str, row: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {
        "slug": lot.get("_slug") or lot.get("slug"),
        "quote_intent_id": lot.get("quote_intent_id"),
        "side": lot.get("side"),
        "qty": fnum(lot.get("qty")),
        "px": fnum(lot.get("px")),
        "cost": fnum(lot.get("cost")),
        "source_order_id": lot.get("source_order_id"),
        "tail_control_action": action,
    }
    if row:
        out.update(
            {
                "action_qty": fnum(row.get("qty")),
                "surplus_budget_projected_unpaired_cost": fnum(row.get("surplus_budget_projected_unpaired_cost")),
                "pair_completion_qty": fnum(row.get("pair_completion_qty")),
                "pair_completion_worst_net_pair_cost": fnum(row.get("pair_completion_worst_net_pair_cost")),
                "pair_completion_projected_pair_pnl_after": fnum(row.get("pair_completion_projected_pair_pnl_after")),
            }
        )
    return out


def build(args: argparse.Namespace) -> dict[str, Any]:
    remote_outputs = Path(args.remote_outputs).expanduser().resolve()
    mature_tail_plan = load_json(args.mature_tail_plan_scorecard)
    current_profile_card = load_json(args.current_profile_scorecard)
    capacity_interpretation = load_json(args.capacity_interpretation_scorecard)
    current_profile = body(current_profile_card, "profile")
    capacity_decision = body(capacity_interpretation, "decision")

    accepted = load_accepted_actions(remote_outputs)
    fill_qids = load_fill_qids(remote_outputs)
    residual_lots = load_residual_lots(remote_outputs)
    slug_metrics = load_slug_metrics(remote_outputs)
    aggregate = aggregate_metrics(remote_outputs)

    selected = selected_qids(mature_tail_plan)
    current_budget = fnum(current_profile.get("surplus_budget_max_abs_unpaired_cost"))
    candidate_budget = args.candidate_surplus_budget_max_abs_unpaired_cost
    candidate_profile = dict(current_profile)
    candidate_profile.update(
        {
            "tail_mitigation_hypothesis": "cap25_mature_tail_surplus_budget_130_profile",
            "surplus_budget_max_abs_unpaired_cost": candidate_budget,
        }
    )

    blocked_qids = {
        qid
        for qid, row in accepted.items()
        if (fnum(row.get("surplus_budget_projected_unpaired_cost"), 0.0) or 0.0)
        > candidate_budget + 1e-12
    }
    blocked_fill_qids = blocked_qids & fill_qids
    residual_qids = {str(lot.get("quote_intent_id")) for lot in residual_lots if lot.get("quote_intent_id")}

    residual_actions: list[dict[str, Any]] = []
    selected_actions: list[dict[str, Any]] = []
    residual_cut_cost = 0.0
    residual_cut_qty = 0.0
    selected_cut_cost = 0.0
    selected_cut_qty = 0.0
    selected_covered: set[str] = set()

    for lot in residual_lots:
        qid = str(lot.get("quote_intent_id") or "")
        row = accepted.get(qid)
        action = "unchanged_residual"
        if qid in blocked_qids:
            action = "block_surplus_budget"
        elif row and pair_completion_covers_full_action(row):
            action = "pair_complete_existing_audit"

        cut_cost = lot["_cost"] if action != "unchanged_residual" else 0.0
        cut_qty = lot["_qty"] if action != "unchanged_residual" else 0.0
        residual_cut_cost += cut_cost
        residual_cut_qty += cut_qty
        snapshot = lot_snapshot(lot, action, row)
        residual_actions.append(snapshot)
        if qid in selected:
            selected_cut_cost += cut_cost
            selected_cut_qty += cut_qty
            if action != "unchanged_residual":
                selected_covered.add(qid)
            selected_actions.append(snapshot)

    non_residual_blocked_fills = sorted(blocked_fill_qids - residual_qids)
    non_residual_blocked_slugs = sorted(
        {
            str(accepted[qid].get("_slug") or accepted[qid].get("slug"))
            for qid in non_residual_blocked_fills
            if qid in accepted
        }
    )
    non_residual_blocked_pair_pnl_worst_case = sum(
        max(0.0, fnum(slug_metrics.get(slug, {}).get("pair_pnl"), 0.0) or 0.0)
        for slug in non_residual_blocked_slugs
    )

    current_residual_cost = fnum(aggregate.get("residual_cost"), 0.0) or 0.0
    current_residual_qty = fnum(aggregate.get("residual_qty"), 0.0) or 0.0
    current_filled_cost = fnum(aggregate.get("filled_cost"), 0.0) or 0.0
    current_filled_qty = fnum(aggregate.get("filled_qty"), 0.0) or 0.0
    current_pair_qty = fnum(aggregate.get("pair_qty"), 0.0) or 0.0
    current_pair_pnl = fnum(aggregate.get("pair_pnl"), 0.0) or 0.0

    after_residual_cost = max(0.0, current_residual_cost - residual_cut_cost)
    after_residual_qty = max(0.0, current_residual_qty - residual_cut_qty)
    after_filled_cost = max(0.0, current_filled_cost - residual_cut_cost)
    after_filled_qty = max(0.0, current_filled_qty - residual_cut_qty)
    after_cost_share = after_residual_cost / after_filled_cost if after_filled_cost > 0 else None
    after_qty_share = after_residual_qty / after_filled_qty if after_filled_qty > 0 else None
    after_cost_to_pair_qty = after_residual_cost / current_pair_qty if current_pair_qty > 0 else None
    after_zero_stress = current_pair_pnl - after_residual_cost
    after_zero_stress_non_residual_pnl_stressed = (
        current_pair_pnl - non_residual_blocked_pair_pnl_worst_case - after_residual_cost
    )

    accepted_before = len(accepted)
    fills_before = len(fill_qids)
    accepted_after_lower_bound = accepted_before - len(blocked_qids)
    fills_after_lower_bound = fills_before - len(blocked_fill_qids)
    fill_rate_after_lower_bound = (
        fills_after_lower_bound / accepted_after_lower_bound if accepted_after_lower_bound > 0 else None
    )

    source_economic_relaxations: list[str] = []
    if fnum(candidate_profile.get("pair_completion_net_cap"), 99.0) > fnum(
        current_profile.get("pair_completion_net_cap"), 99.0
    ):
        source_economic_relaxations.append("pair_completion_net_cap_relaxed")
    if fnum(candidate_profile.get("strict_rescue_surplus_net_cap"), 99.0) > 1.02:
        source_economic_relaxations.append("strict_rescue_surplus_net_cap_relaxed")
    if candidate_profile.get("risk_seed_cancel_on_closeability_net_cap") is not None:
        source_economic_relaxations.append("closeability_cancel_enabled")
    for key in [
        "source_quality_require_l1_source",
        "source_quality_require_l2_source",
        "source_quality_require_trade_source",
        "strict_rescue_require_book_source",
        "strict_rescue_require_l2_source",
    ]:
        if current_profile.get(key) is True and candidate_profile.get(key) is not True:
            source_economic_relaxations.append(f"{key}_relaxed")

    selected_tail_controls_ready = selected and selected_covered == selected
    residual_gates_pass = (
        after_cost_share is not None
        and after_cost_share <= args.max_residual_cost_share
        and after_cost_to_pair_qty is not None
        and after_cost_to_pair_qty <= args.max_residual_cost_to_pair_qty
        and after_qty_share is not None
        and after_qty_share <= args.max_residual_qty_share
        and after_zero_stress >= 0.0
        and after_zero_stress_non_residual_pnl_stressed >= 0.0
    )
    density_lower_bound_pass = (
        accepted_after_lower_bound >= args.min_accepted_after
        and fills_after_lower_bound >= args.min_fills_after
        and fill_rate_after_lower_bound is not None
        and fill_rate_after_lower_bound >= args.min_fill_rate_after
    )
    essential_inputs_ready = (
        remote_outputs.exists()
        and is_keep(mature_tail_plan)
        and is_keep(current_profile_card)
        and is_keep(capacity_interpretation)
        and body(mature_tail_plan, "decision").get("mature_tail_size_control_plan_ready") is True
        and capacity_decision.get("residual_tail_blocks_new_bounded_cap25_sample") is True
        and accepted_before > 0
        and fills_before > 0
        and bool(residual_lots)
        and current_budget is not None
    )

    hard_blockers: list[str] = []
    if not essential_inputs_ready:
        hard_blockers.append("mature_tail_runner_control_inputs_missing_or_not_ready")
    if current_budget is not None and candidate_budget >= current_budget - 1e-12:
        hard_blockers.append("candidate_surplus_budget_not_tighter_than_current")
    if source_economic_relaxations:
        hard_blockers.append("source_or_economic_gate_relaxed")
    if not selected_tail_controls_ready:
        hard_blockers.append("selected_mature_tail_lots_not_cut_or_pair_completed")
    if not residual_gates_pass:
        hard_blockers.append("residual_or_zero_stress_gates_not_jointly_passed")
    if not density_lower_bound_pass:
        hard_blockers.append("density_lower_bound_starved")

    status_value = (
        "KEEP_SHADOW_REVIEW_MATURE_TAIL_RUNNER_CONTROL_PROFILE_READY_LOCAL_ONLY"
        if not hard_blockers
        else "BLOCKED_SHADOW_REVIEW_MATURE_TAIL_RUNNER_CONTROL_PROFILE_LOCAL_ONLY"
    )

    blocked_action_rows = [action_snapshot(accepted[qid]) for qid in sorted(blocked_qids) if qid in accepted]
    non_residual_blocked_rows = [
        action_snapshot(accepted[qid]) for qid in non_residual_blocked_fills if qid in accepted
    ]

    return rounded(
        {
            "artifact": "xuan_shadow_review_mature_tail_runner_control_profile",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_mature_tail_runner_control_profile.py",
            "status": status_value,
            "inputs": {
                "remote_outputs": str(args.remote_outputs),
                "mature_tail_plan_scorecard": str(args.mature_tail_plan_scorecard),
                "current_profile_scorecard": str(args.current_profile_scorecard),
                "capacity_interpretation_scorecard": str(args.capacity_interpretation_scorecard),
            },
            "source_statuses": {
                "mature_tail_plan": status(mature_tail_plan),
                "current_profile": status(current_profile_card),
                "capacity_interpretation": status(capacity_interpretation),
            },
            "decision": {
                "runner_control_profile_ready": status_value.startswith("KEEP"),
                "candidate_profile_ready": status_value.startswith("KEEP"),
                "selected_tail_cut_or_pair_complete": selected_tail_controls_ready,
                "residual_gates_pass_under_local_proxy": residual_gates_pass,
                "density_lower_bound_pass": density_lower_bound_pass,
                "non_residual_pair_pnl_sensitivity_pass": after_zero_stress_non_residual_pnl_stressed >= 0.0,
                "source_economic_gates_preserved": not source_economic_relaxations,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_profile": False,
                "future_remote_requires_manifest_and_separate_pre_authorization": True,
                "same_profile_repeat_allowed": False,
                "cap75_remote_rationale_ready": False,
                "capacity_expansion_allowed": False,
                "private_truth_ready": False,
                "paper_shadow_only": True,
                "research_only": True,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "hard_blockers": hard_blockers,
                "next_action": "build_manifest_scaffold_or_pre_authorization_gate_for_this_profile_before_any_future_cap25_dry_run",
            },
            "candidate_profile": candidate_profile,
            "profile_delta": {
                "surplus_budget_max_abs_unpaired_cost": {
                    "current": current_budget,
                    "candidate": candidate_budget,
                    "direction": "tighten",
                },
                "tail_mitigation_hypothesis": {
                    "current": current_profile.get("tail_mitigation_hypothesis"),
                    "candidate": candidate_profile.get("tail_mitigation_hypothesis"),
                },
                "unchanged_controls": {
                    "target_qty": candidate_profile.get("target_qty"),
                    "pair_completion_net_cap": candidate_profile.get("pair_completion_net_cap"),
                    "pair_completion_min_pair_pnl_after": candidate_profile.get(
                        "pair_completion_min_pair_pnl_after"
                    ),
                    "risk_seed_pair_completion_required_above_net_cap": candidate_profile.get(
                        "risk_seed_pair_completion_required_above_net_cap"
                    ),
                    "risk_seed_pair_completion_min_qty": candidate_profile.get(
                        "risk_seed_pair_completion_min_qty"
                    ),
                    "strict_rescue_surplus_net_cap": candidate_profile.get("strict_rescue_surplus_net_cap"),
                    "risk_seed_cancel_on_closeability_net_cap": candidate_profile.get(
                        "risk_seed_cancel_on_closeability_net_cap"
                    ),
                    "risk_seed_pending_opp_credit": candidate_profile.get("risk_seed_pending_opp_credit"),
                    "source_quality_require_l1_source": candidate_profile.get("source_quality_require_l1_source"),
                    "source_quality_require_l2_source": candidate_profile.get("source_quality_require_l2_source"),
                    "source_quality_require_trade_source": candidate_profile.get(
                        "source_quality_require_trade_source"
                    ),
                    "rescue_block_diagnostics_max_per_slug": candidate_profile.get(
                        "rescue_block_diagnostics_max_per_slug"
                    ),
                },
                "source_economic_relaxations": source_economic_relaxations,
            },
            "runner_supported_control_audit": {
                "control": "surplus_budget_max_abs_unpaired_cost",
                "control_mode": candidate_profile.get("surplus_budget_mode"),
                "candidate_budget": candidate_budget,
                "blocked_accept_qids_count": len(blocked_qids),
                "blocked_fill_qids_count": len(blocked_fill_qids),
                "blocked_action_rows": blocked_action_rows,
            },
            "selected_mature_tail_control": {
                "selected_qids": sorted(selected),
                "covered_selected_qids": sorted(selected_covered),
                "selected_cut_cost": selected_cut_cost,
                "selected_cut_qty": selected_cut_qty,
                "selected_actions": selected_actions,
            },
            "residual_proxy_after_control": {
                "current_residual_cost": current_residual_cost,
                "current_residual_qty": current_residual_qty,
                "current_filled_cost": current_filled_cost,
                "current_filled_qty": current_filled_qty,
                "current_pair_qty": current_pair_qty,
                "current_pair_pnl": current_pair_pnl,
                "residual_cut_cost": residual_cut_cost,
                "residual_cut_qty": residual_cut_qty,
                "after_residual_cost": after_residual_cost,
                "after_residual_qty": after_residual_qty,
                "after_residual_cost_share": after_cost_share,
                "after_residual_qty_share": after_qty_share,
                "after_residual_cost_to_pair_qty": after_cost_to_pair_qty,
                "after_residual_zero_stress_pnl": after_zero_stress,
                "non_residual_blocked_pair_pnl_worst_case_subtract": non_residual_blocked_pair_pnl_worst_case,
                "after_zero_stress_after_non_residual_blocked_slug_pnl_subtract": (
                    after_zero_stress_non_residual_pnl_stressed
                ),
                "max_residual_cost_share": args.max_residual_cost_share,
                "max_residual_cost_to_pair_qty": args.max_residual_cost_to_pair_qty,
                "max_residual_qty_share": args.max_residual_qty_share,
                "residual_actions": residual_actions,
                "non_residual_blocked_fill_qids": non_residual_blocked_fills,
                "non_residual_blocked_slugs": non_residual_blocked_slugs,
                "non_residual_blocked_rows": non_residual_blocked_rows,
            },
            "density_lower_bound": {
                "accepted_before": accepted_before,
                "fills_before": fills_before,
                "blocked_accepts": len(blocked_qids),
                "blocked_fills": len(blocked_fill_qids),
                "accepted_after_lower_bound": accepted_after_lower_bound,
                "fills_after_lower_bound": fills_after_lower_bound,
                "fill_rate_after_lower_bound": fill_rate_after_lower_bound,
                "min_accepted_after": args.min_accepted_after,
                "min_fills_after": args.min_fills_after,
                "min_fill_rate_after": args.min_fill_rate_after,
                "rescue_density_replay_required": True,
                "interpretation": (
                    "The local lower bound removes only directly blocked accepted/fill rows. "
                    "A clean future dry-run is still required before treating rescue density as runtime evidence."
                ),
            },
            "required_before_future_remote": [
                "build a fresh manifest from this candidate profile and pass the manifest verifier",
                "run runner scaffold smoke with this exact surplus budget delta",
                "perform active xuan-runner conflict check immediately before launch",
                "use at most one bounded cap25 PM_DRY_RUN in a future wake if separately pre-authorized",
                "do not run cap75/150/300 until cap25 residual/capacity/private-truth gates are jointly clear",
                "do not deploy, restart, send live orders, or claim private-truth readiness from this artifact",
            ],
            "interpretation": {
                "profile_read": "concrete_runner_supported_surplus_budget_tightening_ready_local_only",
                "residual_read": "selected_mature_tail_lots_are_cut_or_pair_completed_under_local_proxy",
                "density_read": "accepted_and_fill_lower_bounds_not_starved_but_rescue_density_needs_fresh_runtime",
                "remote_read": "no_remote_from_this_profile_without_separate_manifest_pre_authorization",
                "capacity_read": "cap25_tail_control_profile_candidate_only_cap75_still_blocked",
                "live_read": "not_deployable_not_live_authorized",
            },
        }
    )


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    profile_delta = card["profile_delta"]
    runner = card["runner_supported_control_audit"]
    selected = card["selected_mature_tail_control"]
    residual = card["residual_proxy_after_control"]
    density = card["density_lower_bound"]
    lines = [
        "# Xuan Mature Tail Runner-Control Profile",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- runner_control_profile_ready: `{decision['runner_control_profile_ready']}`",
        f"- selected_tail_cut_or_pair_complete: `{decision['selected_tail_cut_or_pair_complete']}`",
        f"- residual_gates_pass_under_local_proxy: `{decision['residual_gates_pass_under_local_proxy']}`",
        f"- density_lower_bound_pass: `{decision['density_lower_bound_pass']}`",
        f"- non_residual_pair_pnl_sensitivity_pass: `{decision['non_residual_pair_pnl_sensitivity_pass']}`",
        f"- source_economic_gates_preserved: `{decision['source_economic_gates_preserved']}`",
        f"- bounded_cap25_remote_rationale_ready: `{decision['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_profile: `{decision['future_remote_allowed_by_this_profile']}`",
        f"- cap75_remote_rationale_ready: `{decision['cap75_remote_rationale_ready']}`",
        f"- hard_blockers: `{decision['hard_blockers']}`",
        "",
        "## Profile Delta",
        "",
        f"- surplus_budget_max_abs_unpaired_cost: "
        f"`{profile_delta['surplus_budget_max_abs_unpaired_cost']['current']}` -> "
        f"`{profile_delta['surplus_budget_max_abs_unpaired_cost']['candidate']}`",
        f"- tail_mitigation_hypothesis: `{profile_delta['tail_mitigation_hypothesis']['candidate']}`",
        f"- pair_completion_net_cap unchanged: "
        f"`{profile_delta['unchanged_controls']['pair_completion_net_cap']}`",
        f"- strict_rescue_surplus_net_cap unchanged: "
        f"`{profile_delta['unchanged_controls']['strict_rescue_surplus_net_cap']}`",
        f"- risk_seed_cancel_on_closeability_net_cap: "
        f"`{profile_delta['unchanged_controls']['risk_seed_cancel_on_closeability_net_cap']}`",
        "",
        "## Runner-Supported Control",
        "",
        f"- control: `{runner['control']}`",
        f"- mode: `{runner['control_mode']}`",
        f"- candidate budget: `{runner['candidate_budget']}`",
        f"- blocked accepted/fill rows: `{runner['blocked_accept_qids_count']}` / "
        f"`{runner['blocked_fill_qids_count']}`",
        "",
        "## Selected Mature Tail Control",
        "",
        f"- selected qids: `{selected['selected_qids']}`",
        f"- covered selected qids: `{selected['covered_selected_qids']}`",
        f"- selected cut cost/qty: `{selected['selected_cut_cost']}` / `{selected['selected_cut_qty']}`",
        "",
        "| Slug | Quote | Action | Qty | Cost | Surplus Cost | Pair Completion Qty | Worst Net |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in selected["selected_actions"]:
        lines.append(
            f"| `{row['slug']}` | `{row['quote_intent_id']}` | `{row['tail_control_action']}` | "
            f"`{row['qty']}` | `{row['cost']}` | "
            f"`{row.get('surplus_budget_projected_unpaired_cost')}` | "
            f"`{row.get('pair_completion_qty')}` | `{row.get('pair_completion_worst_net_pair_cost')}` |"
        )
    lines.extend(
        [
            "",
            "## Residual Proxy After Control",
            "",
            f"- residual cut cost/qty: `{residual['residual_cut_cost']}` / `{residual['residual_cut_qty']}`",
            f"- after residual cost/qty: `{residual['after_residual_cost']}` / "
            f"`{residual['after_residual_qty']}`",
            f"- after residual cost share: `{residual['after_residual_cost_share']}`",
            f"- after residual cost-to-pair qty: `{residual['after_residual_cost_to_pair_qty']}`",
            f"- after residual-zero stress PnL: `{residual['after_residual_zero_stress_pnl']}`",
            f"- non-residual blocked pair PnL worst-case subtract: "
            f"`{residual['non_residual_blocked_pair_pnl_worst_case_subtract']}`",
            f"- after zero-stress with non-residual slug PnL subtract: "
            f"`{residual['after_zero_stress_after_non_residual_blocked_slug_pnl_subtract']}`",
            "",
            "## Density Lower Bound",
            "",
            f"- accepted before/after lower bound: `{density['accepted_before']}` / "
            f"`{density['accepted_after_lower_bound']}`",
            f"- fills before/after lower bound: `{density['fills_before']}` / "
            f"`{density['fills_after_lower_bound']}`",
            f"- fill-rate lower bound: `{density['fill_rate_after_lower_bound']}`",
            f"- rescue_density_replay_required: `{density['rescue_density_replay_required']}`",
            "",
            "## Required Before Future Remote",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in card["required_before_future_remote"])
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Local-only runner-control profile scorer; no remote authorization from this artifact.",
            "- No source or economic gate relaxation, no closeability cancel, no cap75/150/300.",
            "- Historical no-order dry-runs do not establish private truth or live readiness.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote-outputs", type=Path, default=DEFAULT_REMOTE_OUTPUTS)
    parser.add_argument("--mature-tail-plan-scorecard", type=Path, default=DEFAULT_MATURE_TAIL_PLAN)
    parser.add_argument("--current-profile-scorecard", type=Path, default=DEFAULT_CURRENT_PROFILE)
    parser.add_argument("--capacity-interpretation-scorecard", type=Path, default=DEFAULT_CAPACITY_INTERPRETATION)
    parser.add_argument("--candidate-surplus-budget-max-abs-unpaired-cost", type=float, default=1.30)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.10)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.20)
    parser.add_argument("--max-residual-cost-to-pair-qty", type=float, default=0.05)
    parser.add_argument("--min-accepted-after", type=int, default=20)
    parser.add_argument("--min-fills-after", type=int, default=15)
    parser.add_argument("--min-fill-rate-after", type=float, default=0.70)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(markdown(card) + "\n", encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["status"].startswith("KEEP") else 2


if __name__ == "__main__":
    raise SystemExit(main())
