#!/usr/bin/env python3
"""Build an old-system residual age/fee-aware control frontier.

This is a local-only follow-up to the tail-mark snapshot run.  It does not try
another remote profile.  Instead it asks which residual lots are genuinely bad
after mature, fee-aware marks and whether a selective control could restore
residual capacity/recovery without treating all residual as equally toxic.

Backtest V1 candidate audit packs, raw/shared data, live/private truth, deploys,
and remote authorization are intentionally out of scope.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0457Z"

DEFAULT_MARK_FRONTIER = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier_20260527T0407Z.json"
)
DEFAULT_RESULT_GATE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_tail_mark_snapshot_result_gate_20260527T0407Z.json"
)
DEFAULT_RUNTIME_SUMMARY = Path(
    ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-tail-mark-snapshot-20260527T0407Z_runtime_summary.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_residual_age_fee_control_frontier_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_old_system_residual_age_fee_control_frontier_{STAMP}/"
    "RESIDUAL_AGE_FEE_CONTROL_FRONTIER.md"
)


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def sort_num(value: Any, default: float) -> float:
    out = fnum(value)
    return out if out is not None else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    with resolved.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def lot_rows(mark_frontier: dict[str, Any], break_even_rate: float, research_rate: float) -> list[dict[str, Any]]:
    observed = body(mark_frontier, "batch_observed")
    rows = []
    for row in listify(observed.get("top_residual_lot_marks")):
        if not isinstance(row, dict):
            continue
        cost = fnum(row.get("cost"), 0.0) or 0.0
        qty = fnum(row.get("qty"), 0.0) or 0.0
        early_rate = fnum(row.get("best_early_recovery_rate_no_fee"))
        mature_after_fee_rate = fnum(row.get("best_mature_recovery_rate_after_fee"))
        mature_no_fee_rate = fnum(row.get("best_mature_recovery_rate_no_fee"))
        early_value = cost * early_rate if early_rate is not None else None
        mature_after_fee_value = cost * mature_after_fee_rate if mature_after_fee_rate is not None else None
        mature_no_fee_value = cost * mature_no_fee_rate if mature_no_fee_rate is not None else None
        decay_pp = (
            early_rate - mature_after_fee_rate
            if early_rate is not None and mature_after_fee_rate is not None
            else None
        )
        block_reason_counts = body(row, "block_reason_counts")
        strict_age_blocks = fnum(block_reason_counts.get("strict_rescue_lot_age_or_min_cost"), 0.0) or 0.0
        rows.append(
            {
                "quote_intent_id": row.get("quote_intent_id"),
                "slug": row.get("slug"),
                "side": row.get("side"),
                "qty": qty,
                "cost": cost,
                "cost_share_of_residual": None,
                "best_early_age_ms": row.get("best_early_age_ms"),
                "best_early_close_ask": row.get("best_early_close_ask"),
                "best_mature_after_fee_close_ask": row.get("best_mature_after_fee_close_ask"),
                "early_recovery_rate_no_fee": early_rate,
                "mature_recovery_rate_no_fee": mature_no_fee_rate,
                "mature_recovery_rate_after_fee": mature_after_fee_rate,
                "early_mark_value_no_fee": early_value,
                "mature_mark_value_no_fee": mature_no_fee_value,
                "mature_mark_value_after_fee": mature_after_fee_value,
                "early_to_mature_after_fee_decay_pp": decay_pp,
                "strict_rescue_lot_age_or_min_cost_rows": strict_age_blocks,
                "tail_mark_snapshot_rows_for_lot": row.get("tail_mark_snapshot_rows_for_lot"),
                "bad_tail_flags": [
                    flag
                    for flag, active in {
                        "mature_after_fee_below_break_even": mature_after_fee_rate is not None
                        and mature_after_fee_rate < break_even_rate,
                        "mature_after_fee_below_research": mature_after_fee_rate is not None
                        and mature_after_fee_rate < research_rate,
                        "early_to_mature_fee_decay_ge_20pp": decay_pp is not None and decay_pp >= 0.20,
                        "strict_rescue_age_or_min_cost_pressure": strict_age_blocks > 0.0,
                    }.items()
                    if active
                ],
            }
        )
    total_cost = sum(fnum(row.get("cost"), 0.0) or 0.0 for row in rows)
    for row in rows:
        cost = fnum(row.get("cost"), 0.0) or 0.0
        row["cost_share_of_residual"] = cost / total_cost if total_cost else None
    return sorted(rows, key=lambda item: fnum(item.get("cost"), 0.0) or 0.0, reverse=True)


def capacity_state(
    metrics: dict[str, Any],
    residual_cost_after: float,
    residual_qty_after: float,
    removed_cost: float,
    removed_qty: float,
    *,
    max_residual_cost_share: float,
    max_residual_cost_to_pair_qty: float,
    max_residual_qty_share: float,
    public_review_residual_rate: float,
) -> dict[str, Any]:
    filled_cost = fnum(metrics.get("filled_cost"), 0.0) or 0.0
    filled_qty = fnum(metrics.get("filled_qty"), 0.0) or 0.0
    held_cost_share = residual_cost_after / filled_cost if filled_cost else None
    held_cost_to_pair_qty = residual_cost_after / filled_qty if filled_qty else None
    held_qty_share = residual_qty_after / filled_qty if filled_qty else None

    conservative_filled_cost = max(0.0, filled_cost - removed_cost)
    conservative_filled_qty = max(0.0, filled_qty - removed_qty)
    conservative_cost_share = (
        residual_cost_after / conservative_filled_cost if conservative_filled_cost else None
    )
    conservative_cost_to_pair_qty = (
        residual_cost_after / conservative_filled_qty if conservative_filled_qty else None
    )
    conservative_qty_share = (
        residual_qty_after / conservative_filled_qty if conservative_filled_qty else None
    )

    return rounded(
        {
            "residual_cost_after": residual_cost_after,
            "residual_qty_after": residual_qty_after,
            "held_denominator": {
                "residual_cost_share": held_cost_share,
                "residual_cost_to_pair_qty": held_cost_to_pair_qty,
                "residual_qty_share": held_qty_share,
                "residual_cost_share_pass": held_cost_share is not None
                and held_cost_share <= max_residual_cost_share,
                "residual_cost_to_pair_qty_pass": held_cost_to_pair_qty is not None
                and held_cost_to_pair_qty <= max_residual_cost_to_pair_qty,
                "residual_qty_share_pass": held_qty_share is not None and held_qty_share <= max_residual_qty_share,
                "public_residual_qty_pass": held_qty_share is not None
                and held_qty_share <= public_review_residual_rate,
            },
            "conservative_admission_denominator": {
                "filled_cost_after": conservative_filled_cost,
                "filled_qty_after": conservative_filled_qty,
                "residual_cost_share": conservative_cost_share,
                "residual_cost_to_pair_qty": conservative_cost_to_pair_qty,
                "residual_qty_share": conservative_qty_share,
                "residual_cost_share_pass": conservative_cost_share is not None
                and conservative_cost_share <= max_residual_cost_share,
                "residual_cost_to_pair_qty_pass": conservative_cost_to_pair_qty is not None
                and conservative_cost_to_pair_qty <= max_residual_cost_to_pair_qty,
                "residual_qty_share_pass": conservative_qty_share is not None
                and conservative_qty_share <= max_residual_qty_share,
                "public_residual_qty_pass": conservative_qty_share is not None
                and conservative_qty_share <= public_review_residual_rate,
            },
        }
    )


def capacity_passes(state: dict[str, Any]) -> bool:
    held = body(state, "held_denominator")
    conservative = body(state, "conservative_admission_denominator")
    keys = (
        "residual_cost_share_pass",
        "residual_cost_to_pair_qty_pass",
        "residual_qty_share_pass",
        "public_residual_qty_pass",
    )
    return all(held.get(key) is True for key in keys) and all(conservative.get(key) is True for key in keys)


def capacity_cost_mark_passes(state: dict[str, Any]) -> bool:
    held = body(state, "held_denominator")
    conservative = body(state, "conservative_admission_denominator")
    keys = (
        "residual_cost_share_pass",
        "residual_cost_to_pair_qty_pass",
    )
    return all(held.get(key) is True for key in keys) and all(conservative.get(key) is True for key in keys)


def enumerate_control_subsets(
    rows: list[dict[str, Any]],
    metrics: dict[str, Any],
    *,
    break_even_rate: float,
    research_rate: float,
    max_residual_cost_share: float,
    max_residual_cost_to_pair_qty: float,
    max_residual_qty_share: float,
    public_review_residual_rate: float,
) -> list[dict[str, Any]]:
    total_cost = sum(fnum(row.get("cost"), 0.0) or 0.0 for row in rows)
    total_qty = sum(fnum(row.get("qty"), 0.0) or 0.0 for row in rows)
    total_after_fee_mark = sum(fnum(row.get("mature_mark_value_after_fee"), 0.0) or 0.0 for row in rows)
    candidates = []
    for size in range(1, min(len(rows), 6) + 1):
        for combo in itertools.combinations(rows, size):
            removed_cost = sum(fnum(row.get("cost"), 0.0) or 0.0 for row in combo)
            removed_qty = sum(fnum(row.get("qty"), 0.0) or 0.0 for row in combo)
            removed_after_fee_mark = sum(
                fnum(row.get("mature_mark_value_after_fee"), 0.0) or 0.0 for row in combo
            )
            residual_cost_after = max(0.0, total_cost - removed_cost)
            residual_qty_after = max(0.0, total_qty - removed_qty)
            mark_after = max(0.0, total_after_fee_mark - removed_after_fee_mark)
            recovery_after = mark_after / residual_cost_after if residual_cost_after else 1.0
            cap = capacity_state(
                metrics,
                residual_cost_after,
                residual_qty_after,
                removed_cost,
                removed_qty,
                max_residual_cost_share=max_residual_cost_share,
                max_residual_cost_to_pair_qty=max_residual_cost_to_pair_qty,
                max_residual_qty_share=max_residual_qty_share,
                public_review_residual_rate=public_review_residual_rate,
            )
            removed_good_lot_count = sum(
                1
                for row in combo
                if (fnum(row.get("mature_recovery_rate_after_fee"), 0.0) or 0.0) >= research_rate
            )
            bad_flags_removed = sorted({flag for row in combo for flag in listify(row.get("bad_tail_flags"))})
            candidates.append(
                rounded(
                    {
                        "controlled_quote_intent_ids": [row.get("quote_intent_id") for row in combo],
                        "controlled_slug_sides": [
                            f"{row.get('slug')}:{row.get('side')}:{row.get('cost')}" for row in combo
                        ],
                        "controlled_lot_count": len(combo),
                        "controlled_cost": removed_cost,
                        "controlled_qty": removed_qty,
                        "controlled_after_fee_mark_value_removed": removed_after_fee_mark,
                        "removed_good_lot_count": removed_good_lot_count,
                        "bad_tail_flags_removed": bad_flags_removed,
                        "after_mature_after_fee_mark_value": mark_after,
                        "after_mature_after_fee_recovery_rate": recovery_after,
                        "after_recovery_break_even_pass": recovery_after >= break_even_rate,
                        "after_recovery_research_pass": recovery_after >= research_rate,
                        "after_capacity_state": cap,
                        "after_cost_mark_capacity_pass": capacity_cost_mark_passes(cap),
                        "after_capacity_all_pass": capacity_passes(cap),
                    }
                )
            )
    candidates.sort(
        key=lambda item: (
            sort_num(item.get("removed_good_lot_count"), 99.0),
            sort_num(item.get("controlled_lot_count"), 99.0),
            sort_num(item.get("controlled_cost"), 999.0),
        )
    )
    return candidates


def build(args: argparse.Namespace) -> dict[str, Any]:
    mark_frontier = load_json(args.mark_frontier)
    result_gate = load_json(args.result_gate)
    runtime_summary = load_json(args.runtime_summary)
    thresholds = body(mark_frontier, "thresholds")
    metrics = body(runtime_summary, "metrics")

    break_even_rate = fnum(thresholds.get("economic_break_even_recovery_rate"), 0.514317) or 0.514317
    research_rate = fnum(thresholds.get("research_review_recovery_rate"), 0.60) or 0.60
    rows = lot_rows(mark_frontier, break_even_rate, research_rate)

    observed = body(mark_frontier, "batch_observed")
    early = body(observed, "early_mark_proxy_aggregate_no_fee")
    mature_after_fee = body(observed, "mature_actionable_mark_aggregate_after_fee")
    current = capacity_state(
        metrics,
        fnum(metrics.get("residual_cost"), 0.0) or 0.0,
        fnum(metrics.get("residual_qty"), 0.0) or 0.0,
        0.0,
        0.0,
        max_residual_cost_share=args.max_residual_cost_share,
        max_residual_cost_to_pair_qty=args.max_residual_cost_to_pair_qty,
        max_residual_qty_share=args.max_residual_qty_share,
        public_review_residual_rate=args.public_review_residual_rate,
    )
    frontier = enumerate_control_subsets(
        rows,
        metrics,
        break_even_rate=break_even_rate,
        research_rate=research_rate,
        max_residual_cost_share=args.max_residual_cost_share,
        max_residual_cost_to_pair_qty=args.max_residual_cost_to_pair_qty,
        max_residual_qty_share=args.max_residual_qty_share,
        public_review_residual_rate=args.public_review_residual_rate,
    )
    cost_mark_candidates = [
        item
        for item in frontier
        if item.get("after_recovery_research_pass") is True and item.get("after_cost_mark_capacity_pass") is True
    ]
    full_hard_candidates = [
        item
        for item in frontier
        if item.get("after_recovery_research_pass") is True and item.get("after_capacity_all_pass") is True
    ]
    selected_cost_mark = cost_mark_candidates[0] if cost_mark_candidates else {}
    selected_full_hard = full_hard_candidates[0] if full_hard_candidates else {}
    selected_qids = set(listify(selected_cost_mark.get("controlled_quote_intent_ids")))
    selected_rows = [row for row in rows if row.get("quote_intent_id") in selected_qids]
    full_hard_qids = set(listify(selected_full_hard.get("controlled_quote_intent_ids")))
    selected_full_hard_rows = [row for row in rows if row.get("quote_intent_id") in full_hard_qids]

    candidate_ready = bool(selected_cost_mark)
    full_hard_overbroad = bool(selected_full_hard) and (
        fnum(selected_full_hard.get("removed_good_lot_count"), 0.0) > 0.0
        or fnum(selected_full_hard.get("controlled_lot_count"), 0.0) == len(rows)
    )
    hard_blockers: list[str] = []
    if not candidate_ready:
        hard_blockers.append("no_selective_age_fee_control_subset_passes_recovery_and_cost_capacity")
    if full_hard_overbroad:
        hard_blockers.append("residual_qty_hard_requires_overbroad_control_in_this_sample")
    hard_blockers.extend(
        [
            "candidate_control_not_runner_supported_yet",
            "density_preservation_not_proven_by_local_runtime_yet",
            "private_truth_not_ready",
        ]
    )

    total_residual_cost = fnum(metrics.get("residual_cost"), 0.0) or 0.0
    total_after_fee_mark = fnum(mature_after_fee.get("mark_value_after_fee"), 0.0) or 0.0
    early_mark_value = fnum(early.get("mark_value_no_fee"), 0.0) or 0.0
    research_required_value = total_residual_cost * research_rate
    break_even_required_value = total_residual_cost * break_even_rate

    return rounded(
        {
            "artifact": "xuan_shadow_review_old_system_residual_age_fee_control_frontier",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_old_system_residual_age_fee_control_frontier.py",
            "status": (
                "KEEP_SHADOW_REVIEW_OLD_SYSTEM_RESIDUAL_AGE_FEE_CONTROL_FRONTIER_READY_LOCAL_ONLY"
                if candidate_ready
                else "BLOCKED_SHADOW_REVIEW_OLD_SYSTEM_RESIDUAL_AGE_FEE_CONTROL_FRONTIER_NO_SELECTIVE_CONTROL_LOCAL_ONLY"
            ),
            "inputs": {
                "mark_frontier": str(args.mark_frontier),
                "result_gate": str(args.result_gate),
                "runtime_summary": str(args.runtime_summary),
            },
            "source_statuses": {
                "mark_frontier": status(mark_frontier),
                "result_gate": status(result_gate),
                "runtime_summary": status(runtime_summary),
            },
            "thresholds": {
                "economic_break_even_recovery_rate": break_even_rate,
                "research_review_recovery_rate": research_rate,
                "max_residual_cost_share": args.max_residual_cost_share,
                "max_residual_cost_to_pair_qty": args.max_residual_cost_to_pair_qty,
                "max_residual_qty_share": args.max_residual_qty_share,
                "public_review_residual_rate": args.public_review_residual_rate,
            },
            "current_runtime": {
                "accepted_actions": metrics.get("accepted_actions"),
                "queue_supported_fills": metrics.get("queue_supported_fills"),
                "strict_rescue_closes": metrics.get("strict_rescue_closes"),
                "pair_pnl": metrics.get("pair_pnl"),
                "roi_on_filled_cost": metrics.get("roi_on_filled_cost"),
                "filled_cost": metrics.get("filled_cost"),
                "filled_qty": metrics.get("filled_qty"),
                "residual_cost": metrics.get("residual_cost"),
                "residual_qty": metrics.get("residual_qty"),
                "residual_cost_share": metrics.get("residual_cost_share"),
                "residual_qty_share": metrics.get("residual_qty_share"),
                "current_capacity_state": current,
            },
            "mark_recovery_read": {
                "early_no_fee_mark_value": early_mark_value,
                "mature_after_fee_mark_value": total_after_fee_mark,
                "early_no_fee_recovery_rate": early.get("mark_value_no_fee_recovery_rate_on_total_cost"),
                "mature_after_fee_recovery_rate": mature_after_fee.get(
                    "mark_value_after_fee_recovery_rate_on_total_cost"
                ),
                "break_even_required_mark_value": break_even_required_value,
                "research_required_mark_value": research_required_value,
                "break_even_mark_shortfall": max(0.0, break_even_required_value - total_after_fee_mark),
                "research_mark_shortfall": max(0.0, research_required_value - total_after_fee_mark),
                "early_to_mature_after_fee_mark_decay": max(0.0, early_mark_value - total_after_fee_mark),
                "interpretation": (
                    "early/no-fee residual looked acceptable, but mature/after-fee value is below both "
                    "break-even and research recovery thresholds; this is time/fee-sensitive residual risk."
                ),
            },
            "lot_frontier": {
                "residual_lot_count": len(rows),
                "top_residual_lots": rows,
                "cost_mark_passing_control_subsets": cost_mark_candidates[:8],
                "full_hard_passing_control_subsets": full_hard_candidates[:8],
                "selected_cost_mark_control_subset": selected_cost_mark,
                "selected_cost_mark_control_lots": selected_rows,
                "selected_full_hard_control_subset": selected_full_hard,
                "selected_full_hard_control_lots": selected_full_hard_rows,
            },
            "candidate_control_hypothesis": {
                "name": "cap25_age_fee_bad_tail_cost_mark_control_local_proxy",
                "candidate_control_rationale_ready": candidate_ready,
                "profile_or_runner_change_ready": False,
                "selected_rule": (
                    "control large residual lots with mature_after_fee_recovery below break-even and "
                    "strict_rescue_lot_age_or_min_cost pressure; do not cut high-recovery residual lots"
                ),
                "suggested_local_thresholds": {
                    "min_material_lot_cost": min(
                        [fnum(row.get("cost"), 0.0) or 0.0 for row in selected_rows], default=None
                    ),
                    "max_allowed_mature_after_fee_recovery_for_control": max(
                        [fnum(row.get("mature_recovery_rate_after_fee"), 0.0) or 0.0 for row in selected_rows],
                        default=None,
                    ),
                    "require_strict_rescue_lot_age_or_min_cost_pressure": True,
                    "preserve_pair_completion_net_cap_lte": 0.99,
                    "preserve_strict_rescue_surplus_net_cap_lte": 1.02,
                    "preserve_source_quality_gates": True,
                    "do_not_use_closeability_cancel": True,
                },
            },
            "interpretation": {
                "residual_reassessment": (
                    "residual is not uniformly bad: one 0.352625-cost lot had strong mature after-fee recovery, "
                    "while the two largest low-recovery lots drove both recovery and capacity failures."
                ),
                "frontier_read": (
                    "a selective cost/mark subset restores research recovery and residual cost capacity under local "
                    "proxy math, but the full residual-qty hard gate requires overbroad control that cuts recoverable "
                    "residual too; this supports separating economic residual from qty exposure in the next scorer."
                    if candidate_ready
                    else "no selective local subset restored both recovery and residual cost capacity."
                ),
                "why_not_repeat_remote": (
                    "the same tail-mark snapshot profile already answered the mark question; repeating it would not "
                    "test a new control and is explicitly disallowed."
                ),
            },
            "decision": {
                "old_system_residual_age_fee_control_frontier_ready": True,
                "use_backtest_v1_candidate_audit_pack": False,
                "tail_mark_evidence_ready": True,
                "mature_after_fee_residual_is_bad_risk_in_this_sample": True,
                "selective_cost_mark_bad_tail_control_candidate_found": candidate_ready,
                "full_residual_hard_control_candidate_found": bool(selected_full_hard),
                "full_residual_hard_control_is_overbroad": full_hard_overbroad,
                "selected_lots_restore_research_mark_recovery": bool(
                    selected_cost_mark.get("after_recovery_research_pass") is True
                ),
                "selected_lots_restore_cost_capacity_under_local_proxy": bool(
                    selected_cost_mark.get("after_cost_mark_capacity_pass") is True
                ),
                "selected_lots_restore_full_residual_hard_gates_under_local_proxy": bool(
                    selected_full_hard.get("after_capacity_all_pass") is True
                ),
                "same_tail_mark_snapshot_profile_repeat_allowed": False,
                "candidate_profile_ready": False,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_frontier": False,
                "future_remote_requires_profile_or_scorer": True,
                "cap75_remote_rationale_ready": False,
                "promotion_ready": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "research_only": True,
                "paper_shadow_only": True,
                "hard_blockers": hard_blockers,
                "next_action": (
                    "build a runner-supported old-system age/fee-aware residual control profile or local scorer, "
                    "then scaffold/manifest/preauthorize before any future bounded cap25 PM_DRY_RUN"
                ),
            },
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    current = card["current_runtime"]
    mark = card["mark_recovery_read"]
    selected = card["lot_frontier"]["selected_cost_mark_control_subset"]
    lots = card["lot_frontier"]["selected_cost_mark_control_lots"]
    full_selected = card["lot_frontier"]["selected_full_hard_control_subset"]
    full_lots = card["lot_frontier"]["selected_full_hard_control_lots"]
    hypothesis = card["candidate_control_hypothesis"]
    lines = [
        "# Xuan Old-System Residual Age/Fee Control Frontier",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- selective_cost_mark_bad_tail_control_candidate_found: `{d['selective_cost_mark_bad_tail_control_candidate_found']}`",
        f"- full_residual_hard_control_is_overbroad: `{d['full_residual_hard_control_is_overbroad']}`",
        f"- selected_lots_restore_research_mark_recovery: `{d['selected_lots_restore_research_mark_recovery']}`",
        f"- selected_lots_restore_cost_capacity_under_local_proxy: `{d['selected_lots_restore_cost_capacity_under_local_proxy']}`",
        f"- selected_lots_restore_full_residual_hard_gates_under_local_proxy: `{d['selected_lots_restore_full_residual_hard_gates_under_local_proxy']}`",
        f"- candidate_profile_ready: `{d['candidate_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_frontier: `{d['future_remote_allowed_by_this_frontier']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers'])}`",
        "",
        "## Current Runtime",
        "",
        f"- pair_pnl: `{current['pair_pnl']}`",
        f"- ROI on filled cost: `{current['roi_on_filled_cost']}`",
        f"- accepted/fills/rescues: `{current['accepted_actions']}` / `{current['queue_supported_fills']}` / `{current['strict_rescue_closes']}`",
        f"- residual cost/qty: `{current['residual_cost']}` / `{current['residual_qty']}`",
        f"- residual cost share/qty share: `{current['residual_cost_share']}` / `{current['residual_qty_share']}`",
        "",
        "## Mark Recovery",
        "",
        f"- early no-fee recovery: `{mark['early_no_fee_recovery_rate']}`",
        f"- mature after-fee recovery: `{mark['mature_after_fee_recovery_rate']}`",
        f"- break-even/research required value: `{mark['break_even_required_mark_value']}` / `{mark['research_required_mark_value']}`",
        f"- break-even/research shortfall: `{mark['break_even_mark_shortfall']}` / `{mark['research_mark_shortfall']}`",
        f"- early-to-mature after-fee mark decay: `{mark['early_to_mature_after_fee_mark_decay']}`",
        "",
        "## Selected Cost/Mark Local Proxy Control",
        "",
        f"- hypothesis: `{hypothesis['name']}`",
        f"- controlled qids: `{', '.join(selected.get('controlled_quote_intent_ids', [])) if selected else 'none'}`",
        f"- controlled cost/qty: `{selected.get('controlled_cost') if selected else None}` / `{selected.get('controlled_qty') if selected else None}`",
        f"- after recovery: `{selected.get('after_mature_after_fee_recovery_rate') if selected else None}`",
        f"- after cost/mark capacity pass: `{selected.get('after_cost_mark_capacity_pass') if selected else None}`",
        f"- after full hard capacity pass: `{selected.get('after_capacity_all_pass') if selected else None}`",
        "",
        "## Selected Cost/Mark Lots",
        "",
    ]
    for lot in lots:
        lines.append(
            "- "
            f"`{lot['quote_intent_id']}` cost `{lot['cost']}` qty `{lot['qty']}` "
            f"mature_after_fee_recovery `{lot['mature_recovery_rate_after_fee']}` "
            f"flags `{', '.join(lot['bad_tail_flags'])}`"
        )
    lines.extend(
        [
            "",
            "## Full Hard Gate Control",
            "",
            f"- controlled qids: `{', '.join(full_selected.get('controlled_quote_intent_ids', [])) if full_selected else 'none'}`",
            f"- controlled cost/qty: `{full_selected.get('controlled_cost') if full_selected else None}` / `{full_selected.get('controlled_qty') if full_selected else None}`",
            f"- removed good lot count: `{full_selected.get('removed_good_lot_count') if full_selected else None}`",
            f"- lot count: `{full_selected.get('controlled_lot_count') if full_selected else None}`",
        ]
    )
    for lot in full_lots:
        lines.append(
            "- "
            f"`{lot['quote_intent_id']}` cost `{lot['cost']}` "
            f"mature_after_fee_recovery `{lot['mature_recovery_rate_after_fee']}`"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Old/current xuan no-order/shadow system only; backtest V1 candidate audit pack is not used.",
            "- Local-only and research-only; no remote authorization, no cap75/150/300, no deploy, no live orders, no private truth.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mark-frontier", type=Path, default=DEFAULT_MARK_FRONTIER)
    parser.add_argument("--result-gate", type=Path, default=DEFAULT_RESULT_GATE)
    parser.add_argument("--runtime-summary", type=Path, default=DEFAULT_RUNTIME_SUMMARY)
    parser.add_argument("--scorecard-out", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown-out", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.15)
    parser.add_argument("--max-residual-cost-to-pair-qty", type=float, default=0.05)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.20)
    parser.add_argument("--public-review-residual-rate", type=float, default=0.20)
    args = parser.parse_args()

    card = build(args)
    args.scorecard_out.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard_out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown_out.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps({"status": card["status"], "scorecard": str(args.scorecard_out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
