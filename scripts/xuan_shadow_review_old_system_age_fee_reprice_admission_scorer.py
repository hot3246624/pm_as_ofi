#!/usr/bin/env python3
"""Score age/fee residual controls under the old xuan no-order system.

This local-only scorer separates two ideas that were previously conflated:

* mature repricing/closing bad residual after the tail mark is observed;
* admission/size avoidance of the same bad-tail class before it becomes
  residual exposure.

It deliberately does not use backtest V1 candidate packs, qid allow/deny lists
as a deployable rule, raw/shared data, remote runs, deploys, live orders, or
private-truth claims.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0557Z"
RUN_TAG = "xuan-frontier-soft-mainline-cap25-tail-mark-snapshot-20260527T0407Z"

DEFAULT_REMOTE_OUTPUTS = Path(f".tmp_xuan/local_verifier_artifacts/{RUN_TAG}/remote_outputs")
DEFAULT_RUNTIME_SUMMARY = Path(
    f".tmp_xuan/scorecards/no_order_{RUN_TAG}_runtime_summary.json"
)
DEFAULT_MARK_FRONTIER = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_lot_mark_recovery_batch_frontier_20260527T0407Z.json"
)
DEFAULT_AGE_FEE_FRONTIER = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_residual_age_fee_control_frontier_20260527T0457Z.json"
)
DEFAULT_RUNNER_CONTROL = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_age_fee_runner_control_profile_20260527T0527Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_age_fee_reprice_admission_scorer_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_old_system_age_fee_reprice_admission_scorer_{STAMP}/"
    "AGE_FEE_REPRICE_ADMISSION_SCORER.md"
)


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


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_accepted_actions(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.would_action_decisions.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if row.get("kind") == "candidate" and row.get("decision") == "accept" and qid:
                row = dict(row)
                row["_source_file"] = path.name
                out[qid] = row
    return out


def load_fill_qids(remote_outputs: Path) -> set[str]:
    out: set[str] = set()
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.would_fill_events.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if qid:
                out.add(qid)
    return out


def load_residual_lots(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.residual_fifo_lots.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if not qid:
                continue
            out[qid] = {
                "quote_intent_id": qid,
                "slug": row.get("slug"),
                "side": row.get("side"),
                "qty": fnum(row.get("qty"), 0.0) or 0.0,
                "px": fnum(row.get("px"), 0.0) or 0.0,
                "cost": fnum(row.get("cost"), 0.0) or 0.0,
            }
    return out


def load_candidate_events(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.expanduser().resolve().glob("*.events.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                qid = event.get("quote_intent_id")
                if event.get("kind") == "candidate" and qid:
                    out[str(qid)] = event
    return out


def load_best_tail_marks(mark_frontier: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = body(mark_frontier, "batch_observed").get("top_residual_lot_marks")
    out: dict[str, dict[str, Any]] = {}
    for row in listify(rows):
        if not isinstance(row, dict):
            continue
        qid = row.get("quote_intent_id")
        if qid:
            out[str(qid)] = row
    return out


def selected_qids(age_fee_frontier: dict[str, Any], key: str) -> set[str]:
    subset = body(body(age_fee_frontier, "lot_frontier"), key)
    return {str(qid) for qid in listify(subset.get("controlled_quote_intent_ids")) if qid}


def high_recovery_qids(age_fee_frontier: dict[str, Any]) -> set[str]:
    threshold = fnum(body(age_fee_frontier, "thresholds").get("research_review_recovery_rate"), 0.60) or 0.60
    out: set[str] = set()
    for row in listify(body(age_fee_frontier, "lot_frontier").get("top_residual_lots")):
        if not isinstance(row, dict):
            continue
        qid = row.get("quote_intent_id")
        recovery = fnum(row.get("mature_recovery_rate_after_fee"))
        if qid and recovery is not None and recovery >= threshold:
            out.add(str(qid))
    return out


def admission_features(
    qid: str,
    accepted: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    row = accepted.get(qid, {})
    event = events.get(qid, {})
    return {
        "quote_intent_id": qid,
        "slug": row.get("slug") or event.get("slug"),
        "side": row.get("side") or event.get("side"),
        "price": fnum(row.get("price"), fnum(event.get("price"))),
        "qty": fnum(row.get("qty"), fnum(event.get("qty"))),
        "risk_increasing_seed": event.get("risk_increasing_seed"),
        "same_exposure_qty": fnum(event.get("same_exposure_qty"), 0.0) or 0.0,
        "opp_exposure_qty": fnum(event.get("opp_exposure_qty"), 0.0) or 0.0,
        "closeability_net_pair_cost": fnum(row.get("closeability_net_pair_cost"), fnum(event.get("closeability_net_pair_cost"))),
        "pair_completion_qty": fnum(row.get("pair_completion_qty"), fnum(event.get("pair_completion_qty"), 0.0)) or 0.0,
        "pair_completion_decision": row.get("pair_completion_decision") or event.get("pair_completion_decision"),
        "surplus_budget_projected_unpaired_cost": (
            fnum(
                row.get("surplus_budget_projected_unpaired_cost"),
                fnum(event.get("surplus_budget_projected_unpaired_cost"), 0.0),
            )
            or 0.0
        ),
        "source_quality_decision": row.get("source_quality_decision") or event.get("source_quality_decision"),
        "source_quality_l1_age_ms": fnum(row.get("source_quality_l1_age_ms"), fnum(event.get("source_quality_l1_age_ms"))),
        "public_trade_px": fnum(event.get("public_trade_px")),
        "public_trade_size": fnum(event.get("public_trade_size")),
        "open_cost": fnum(event.get("open_cost")),
    }


def feature_hypothesis_select(features: dict[str, Any]) -> bool:
    """Single-sample non-qid hypothesis for bad-tail admission/sizing control.

    The clauses are intentionally explicit and treated as research-only until
    cross-window validation proves they are not just a one-window fit.
    """
    if features.get("risk_increasing_seed") is not True:
        return False
    if (fnum(features.get("pair_completion_qty"), 0.0) or 0.0) > 1e-12:
        return False
    if features.get("source_quality_decision") != "allow":
        return False
    qty = fnum(features.get("qty"), 0.0) or 0.0
    price = fnum(features.get("price"), 0.0) or 0.0
    surplus = fnum(features.get("surplus_budget_projected_unpaired_cost"), 0.0) or 0.0
    same = fnum(features.get("same_exposure_qty"), 0.0) or 0.0
    opp = fnum(features.get("opp_exposure_qty"), 0.0) or 0.0
    large_fresh_unpaired_seed = surplus >= 0.90 and qty >= 2.0 and same <= 1e-12 and opp <= 1e-12
    large_low_price_no_pair_seed = surplus <= 1e-12 and qty >= 3.5 and price <= 0.20
    tiny_price_large_qty_seed = price <= 0.02 and qty >= 3.5
    return large_fresh_unpaired_seed or large_low_price_no_pair_seed or tiny_price_large_qty_seed


def capacity_state(
    metrics: dict[str, Any],
    residual_lots: dict[str, dict[str, Any]],
    controlled_qids: set[str],
    *,
    max_residual_cost_share: float,
    max_residual_cost_to_pair_qty: float,
    max_residual_qty_share: float,
) -> dict[str, Any]:
    residual_removed_cost = sum(
        (fnum(residual_lots[qid].get("cost"), 0.0) or 0.0) for qid in controlled_qids if qid in residual_lots
    )
    residual_removed_qty = sum(
        (fnum(residual_lots[qid].get("qty"), 0.0) or 0.0) for qid in controlled_qids if qid in residual_lots
    )
    residual_cost_after = max(0.0, (fnum(metrics.get("residual_cost"), 0.0) or 0.0) - residual_removed_cost)
    residual_qty_after = max(0.0, (fnum(metrics.get("residual_qty"), 0.0) or 0.0) - residual_removed_qty)
    filled_cost_after = max(0.0, (fnum(metrics.get("filled_cost"), 0.0) or 0.0) - residual_removed_cost)
    filled_qty_after = max(0.0, (fnum(metrics.get("filled_qty"), 0.0) or 0.0) - residual_removed_qty)
    residual_cost_share = residual_cost_after / filled_cost_after if filled_cost_after else None
    residual_cost_to_pair_qty = residual_cost_after / filled_qty_after if filled_qty_after else None
    residual_qty_share = residual_qty_after / filled_qty_after if filled_qty_after else None
    return rounded(
        {
            "residual_removed_cost": residual_removed_cost,
            "residual_removed_qty": residual_removed_qty,
            "residual_cost_after": residual_cost_after,
            "residual_qty_after": residual_qty_after,
            "filled_cost_after": filled_cost_after,
            "filled_qty_after": filled_qty_after,
            "residual_cost_share_after": residual_cost_share,
            "residual_cost_to_pair_qty_after": residual_cost_to_pair_qty,
            "residual_qty_share_after": residual_qty_share,
            "residual_cost_share_pass": residual_cost_share is not None and residual_cost_share <= max_residual_cost_share,
            "residual_cost_to_pair_qty_pass": (
                residual_cost_to_pair_qty is not None
                and residual_cost_to_pair_qty <= max_residual_cost_to_pair_qty
            ),
            "residual_qty_share_pass": residual_qty_share is not None and residual_qty_share <= max_residual_qty_share,
            "all_capacity_pass": (
                residual_cost_share is not None
                and residual_cost_share <= max_residual_cost_share
                and residual_cost_to_pair_qty is not None
                and residual_cost_to_pair_qty <= max_residual_cost_to_pair_qty
                and residual_qty_share is not None
                and residual_qty_share <= max_residual_qty_share
            ),
        }
    )


def admission_proxy(
    name: str,
    controlled_qids: set[str],
    accepted: dict[str, dict[str, Any]],
    fills: set[str],
    residual_lots: dict[str, dict[str, Any]],
    metrics: dict[str, Any],
    selected_full_hard: set[str],
    high_recovery: set[str],
    *,
    min_accepted_after: int,
    min_fills_after: int,
    min_fill_rate_after: float,
    max_residual_cost_share: float,
    max_residual_cost_to_pair_qty: float,
    max_residual_qty_share: float,
) -> dict[str, Any]:
    selected_accepts = controlled_qids & set(accepted)
    selected_fills = controlled_qids & fills
    selected_residual = controlled_qids & set(residual_lots)
    accepted_after = len(accepted) - len(selected_accepts)
    fills_after = len(fills) - len(selected_fills)
    fill_rate_after = fills_after / accepted_after if accepted_after else None
    cap = capacity_state(
        metrics,
        residual_lots,
        controlled_qids,
        max_residual_cost_share=max_residual_cost_share,
        max_residual_cost_to_pair_qty=max_residual_cost_to_pair_qty,
        max_residual_qty_share=max_residual_qty_share,
    )
    filled_cost_after = fnum(cap.get("filled_cost_after"), 0.0) or 0.0
    pair_pnl = fnum(metrics.get("pair_pnl"), 0.0) or 0.0
    roi_after = pair_pnl / filled_cost_after if filled_cost_after else None
    density_pass = (
        accepted_after >= min_accepted_after
        and fills_after >= min_fills_after
        and fill_rate_after is not None
        and fill_rate_after >= min_fill_rate_after
    )
    return rounded(
        {
            "name": name,
            "controlled_qids": sorted(controlled_qids),
            "controlled_accept_count": len(selected_accepts),
            "controlled_fill_count": len(selected_fills),
            "controlled_residual_count": len(selected_residual),
            "non_residual_accepts_controlled": sorted(selected_accepts - selected_residual),
            "accepted_after_lower_bound": accepted_after,
            "fills_after_lower_bound": fills_after,
            "fill_rate_after_lower_bound": fill_rate_after,
            "density_lower_bound_pass": density_pass,
            "selected_full_hard_qids_covered": selected_full_hard <= controlled_qids,
            "selected_full_hard_missing_qids": sorted(selected_full_hard - controlled_qids),
            "high_recovery_residual_preserved": not bool(controlled_qids & high_recovery),
            "high_recovery_residual_qids_controlled": sorted(controlled_qids & high_recovery),
            "capacity_state": cap,
            "pair_pnl_local_proxy": pair_pnl,
            "roi_after_local_proxy": roi_after,
            "positive_pair_pnl_proxy": pair_pnl > 0,
            "admission_proxy_pass": (
                density_pass
                and selected_full_hard <= controlled_qids
                and not bool(controlled_qids & high_recovery)
                and bool(cap.get("all_capacity_pass"))
                and pair_pnl > 0
            ),
        }
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    runtime = load_json(args.runtime_summary)
    mark_frontier = load_json(args.mark_frontier)
    age_fee_frontier = load_json(args.age_fee_frontier)
    runner_control = load_json(args.runner_control)
    metrics = body(runtime, "metrics")
    accepted = load_accepted_actions(args.remote_outputs)
    fills = load_fill_qids(args.remote_outputs)
    residual_lots = load_residual_lots(args.remote_outputs)
    events = load_candidate_events(args.remote_outputs)
    marks = load_best_tail_marks(mark_frontier)

    selected_cost_mark = selected_qids(age_fee_frontier, "selected_cost_mark_control_subset")
    selected_full_hard = selected_qids(age_fee_frontier, "selected_full_hard_control_subset")
    high_recovery = high_recovery_qids(age_fee_frontier)

    pair_pnl = fnum(metrics.get("pair_pnl"), 0.0) or 0.0
    residual_cost = fnum(metrics.get("residual_cost"), 0.0) or 0.0
    dynamic_break_even = max(0.0, min(1.0, (residual_cost - pair_pnl) / residual_cost)) if residual_cost else 0.0
    dynamic_research = max(args.min_research_recovery_rate, min(1.0, dynamic_break_even + args.research_margin))
    mature_after_fee = body(body(mark_frontier, "batch_observed"), "mature_actionable_mark_aggregate_after_fee")
    total_mark_after_fee = fnum(mature_after_fee.get("mark_value_after_fee"), 0.0) or 0.0
    current_marked_pnl = pair_pnl - residual_cost + total_mark_after_fee

    selected_cost_mark_card = body(body(age_fee_frontier, "lot_frontier"), "selected_cost_mark_control_subset")
    selected_full_hard_card = body(body(age_fee_frontier, "lot_frontier"), "selected_full_hard_control_subset")

    feature_rows = {
        qid: {
            **admission_features(qid, accepted, events),
            "is_fill": qid in fills,
            "is_residual": qid in residual_lots,
            "is_selected_full_hard": qid in selected_full_hard,
            "is_selected_cost_mark": qid in selected_cost_mark,
            "is_high_recovery_residual": qid in high_recovery,
            "best_mature_after_fee_recovery": fnum(marks.get(qid, {}).get("best_mature_recovery_rate_after_fee")),
        }
        for qid in sorted(accepted)
    }
    feature_hypothesis_qids = {qid for qid, row in feature_rows.items() if feature_hypothesis_select(row)}
    mark_oracle_qids = {
        qid
        for qid, lot in residual_lots.items()
        if (
            (fnum(marks.get(qid, {}).get("best_mature_recovery_rate_after_fee"), 1.0) or 1.0)
            < dynamic_break_even
            and ((fnum(lot.get("cost"), 0.0) or 0.0) >= 0.70 or (fnum(lot.get("qty"), 0.0) or 0.0) >= 3.50)
        )
    }

    feature_proxy = admission_proxy(
        "non_qid_admission_feature_hypothesis_single_window",
        feature_hypothesis_qids,
        accepted,
        fills,
        residual_lots,
        metrics,
        selected_full_hard,
        high_recovery,
        min_accepted_after=args.min_accepted_after,
        min_fills_after=args.min_fills_after,
        min_fill_rate_after=args.min_fill_rate_after,
        max_residual_cost_share=args.max_residual_cost_share,
        max_residual_cost_to_pair_qty=args.max_residual_cost_to_pair_qty,
        max_residual_qty_share=args.max_residual_qty_share,
    )
    mark_oracle_proxy = admission_proxy(
        "observed_mark_bad_tail_oracle_not_deployable",
        mark_oracle_qids,
        accepted,
        fills,
        residual_lots,
        metrics,
        selected_full_hard,
        high_recovery,
        min_accepted_after=args.min_accepted_after,
        min_fills_after=args.min_fills_after,
        min_fill_rate_after=args.min_fill_rate_after,
        max_residual_cost_share=args.max_residual_cost_share,
        max_residual_cost_to_pair_qty=args.max_residual_cost_to_pair_qty,
        max_residual_qty_share=args.max_residual_qty_share,
    )
    qid_oracle_proxy = admission_proxy(
        "qid_oracle_frontier_not_deployable",
        selected_full_hard,
        accepted,
        fills,
        residual_lots,
        metrics,
        selected_full_hard,
        high_recovery,
        min_accepted_after=args.min_accepted_after,
        min_fills_after=args.min_fills_after,
        min_fill_rate_after=args.min_fill_rate_after,
        max_residual_cost_share=args.max_residual_cost_share,
        max_residual_cost_to_pair_qty=args.max_residual_cost_to_pair_qty,
        max_residual_qty_share=args.max_residual_qty_share,
    )

    mature_reprice_pass = current_marked_pnl > 0.0 and (
        total_mark_after_fee / residual_cost if residual_cost else 1.0
    ) >= dynamic_research
    admission_candidate_found = bool(feature_proxy.get("admission_proxy_pass"))

    hard_blockers = ["private_truth_not_ready", "runner_support_not_implemented"]
    if not mature_reprice_pass:
        hard_blockers.append("mature_reprice_marked_pnl_or_recovery_fails_dynamic_runtime_gate")
    if admission_candidate_found:
        hard_blockers.append("single_window_feature_hypothesis_requires_cross_window_validation")
    else:
        hard_blockers.append("no_non_qid_admission_feature_hypothesis_passes_local_proxy")

    status_value = (
        "KEEP_SHADOW_REVIEW_OLD_SYSTEM_AGE_FEE_REPRICE_ADMISSION_SCORER_READY_LOCAL_ONLY"
        if admission_candidate_found
        else "BLOCKED_SHADOW_REVIEW_OLD_SYSTEM_AGE_FEE_REPRICE_ADMISSION_SCORER_NO_FEATURE_PROXY_LOCAL_ONLY"
    )

    return rounded(
        {
            "artifact": "xuan_shadow_review_old_system_age_fee_reprice_admission_scorer",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_old_system_age_fee_reprice_admission_scorer.py",
            "status": status_value,
            "inputs": {
                "remote_outputs": str(args.remote_outputs),
                "runtime_summary": str(args.runtime_summary),
                "mark_frontier": str(args.mark_frontier),
                "age_fee_frontier": str(args.age_fee_frontier),
                "runner_control": str(args.runner_control),
            },
            "source_statuses": {
                "runtime_summary": status(runtime),
                "mark_frontier": status(mark_frontier),
                "age_fee_frontier": status(age_fee_frontier),
                "runner_control": status(runner_control),
            },
            "dynamic_runtime_recovery_gate": {
                "pair_pnl_before_residual_mark": pair_pnl,
                "residual_cost": residual_cost,
                "mature_after_fee_mark_value": total_mark_after_fee,
                "mature_after_fee_recovery_rate": total_mark_after_fee / residual_cost if residual_cost else None,
                "dynamic_break_even_recovery_rate": dynamic_break_even,
                "dynamic_research_recovery_rate": dynamic_research,
                "dynamic_break_even_mark_value_required": max(0.0, residual_cost - pair_pnl),
                "dynamic_research_mark_value_required": residual_cost * dynamic_research,
                "current_marked_pnl_after_mature_after_fee_marks": current_marked_pnl,
                "mature_reprice_dynamic_gate_pass": mature_reprice_pass,
                "read": (
                    "Mature after-fee repricing is not enough in this runtime; the control has to avoid or size "
                    "bad-tail exposure before the mark decays, not merely reclassify residual after the fact."
                ),
            },
            "frontier_reprice_subsets": {
                "selected_cost_mark_subset": selected_cost_mark_card,
                "selected_full_hard_subset": selected_full_hard_card,
                "economic_note": (
                    "Removing bad lots from residual capacity improves residual ratios, but if those lots are closed "
                    "at observed mature marks the realized loss still leaves the full marked runtime below break-even."
                ),
            },
            "admission_feature_rows": feature_rows,
            "admission_avoidance_proxies": {
                "qid_oracle_frontier_not_deployable": qid_oracle_proxy,
                "observed_mark_bad_tail_oracle_not_deployable": mark_oracle_proxy,
                "non_qid_admission_feature_hypothesis_single_window": feature_proxy,
                "feature_hypothesis_rule": {
                    "risk_increasing_seed": True,
                    "pair_completion_qty_eq_0": True,
                    "source_quality_decision": "allow",
                    "clauses": [
                        "surplus_budget_projected_unpaired_cost>=0.90 and qty>=2.0 and same_exposure_qty==0 and opp_exposure_qty==0",
                        "surplus_budget_projected_unpaired_cost==0 and qty>=3.5 and price<=0.20",
                        "price<=0.02 and qty>=3.5",
                    ],
                    "non_qid": True,
                    "single_window_only": True,
                },
            },
            "decision": {
                "old_system_age_fee_reprice_admission_scorer_ready": True,
                "use_backtest_v1_candidate_audit_pack": False,
                "mature_reprice_dynamic_gate_pass": mature_reprice_pass,
                "mature_reprice_is_sufficient": False,
                "non_qid_admission_feature_hypothesis_found": admission_candidate_found,
                "non_qid_admission_feature_hypothesis_local_proxy_pass": admission_candidate_found,
                "candidate_profile_ready": False,
                "runner_support_ready": False,
                "cross_window_validation_ready": False,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_scorer": False,
                "future_remote_requires_runner_support_and_cross_window_validation": True,
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
                    "local-only cross-window validate the non-qid admission feature hypothesis against prior old-system "
                    "positive samples before adding runner support or building any manifest/preauthorization"
                ),
            },
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    gate = card["dynamic_runtime_recovery_gate"]
    proxies = card["admission_avoidance_proxies"]
    feature = proxies["non_qid_admission_feature_hypothesis_single_window"]
    lines = [
        "# Xuan Old-System Age/Fee Reprice Admission Scorer",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- mature_reprice_dynamic_gate_pass: `{d['mature_reprice_dynamic_gate_pass']}`",
        f"- non_qid_admission_feature_hypothesis_found: `{d['non_qid_admission_feature_hypothesis_found']}`",
        f"- candidate_profile_ready: `{d['candidate_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_scorer: `{d['future_remote_allowed_by_this_scorer']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers'])}`",
        "",
        "## Dynamic Runtime Recovery Gate",
        "",
        f"- pair_pnl_before_residual_mark: `{gate['pair_pnl_before_residual_mark']}`",
        f"- residual_cost: `{gate['residual_cost']}`",
        f"- mature_after_fee_mark_value: `{gate['mature_after_fee_mark_value']}`",
        f"- mature_after_fee_recovery_rate: `{gate['mature_after_fee_recovery_rate']}`",
        f"- dynamic_break_even_recovery_rate: `{gate['dynamic_break_even_recovery_rate']}`",
        f"- dynamic_research_recovery_rate: `{gate['dynamic_research_recovery_rate']}`",
        f"- current_marked_pnl_after_mature_after_fee_marks: `{gate['current_marked_pnl_after_mature_after_fee_marks']}`",
        f"- read: {gate['read']}",
        "",
        "## Non-QID Admission Feature Hypothesis",
        "",
        f"- controlled qids: `{feature['controlled_qids']}`",
        f"- non-residual accepts controlled: `{feature['non_residual_accepts_controlled']}`",
        f"- accepted/fills after: `{feature['accepted_after_lower_bound']}` / `{feature['fills_after_lower_bound']}`",
        f"- fill_rate_after: `{feature['fill_rate_after_lower_bound']}`",
        f"- selected_full_hard_qids_covered: `{feature['selected_full_hard_qids_covered']}`",
        f"- high_recovery_residual_preserved: `{feature['high_recovery_residual_preserved']}`",
        f"- capacity all pass: `{feature['capacity_state']['all_capacity_pass']}`",
        f"- pair_pnl local proxy: `{feature['pair_pnl_local_proxy']}`",
        f"- ROI after local proxy: `{feature['roi_after_local_proxy']}`",
        f"- admission_proxy_pass: `{feature['admission_proxy_pass']}`",
        "",
        "## Boundary",
        "",
        "- This is a single-window local scorer, not a deployable runner profile.",
        "- It uses old/current xuan no-order outputs only; V1 candidate audit pack is not used.",
        "- No remote authorization, cap75/150/300, deploy, live orders, or private truth.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote-outputs", type=Path, default=DEFAULT_REMOTE_OUTPUTS)
    parser.add_argument("--runtime-summary", type=Path, default=DEFAULT_RUNTIME_SUMMARY)
    parser.add_argument("--mark-frontier", type=Path, default=DEFAULT_MARK_FRONTIER)
    parser.add_argument("--age-fee-frontier", type=Path, default=DEFAULT_AGE_FEE_FRONTIER)
    parser.add_argument("--runner-control", type=Path, default=DEFAULT_RUNNER_CONTROL)
    parser.add_argument("--min-research-recovery-rate", type=float, default=0.60)
    parser.add_argument("--research-margin", type=float, default=0.05)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.15)
    parser.add_argument("--max-residual-cost-to-pair-qty", type=float, default=0.05)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.20)
    parser.add_argument("--min-accepted-after", type=int, default=10)
    parser.add_argument("--min-fills-after", type=int, default=8)
    parser.add_argument("--min-fill-rate-after", type=float, default=0.70)
    parser.add_argument("--scorecard-out", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown-out", type=Path, default=DEFAULT_MARKDOWN)
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
