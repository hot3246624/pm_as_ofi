#!/usr/bin/env python3
"""Cross-window validate old-system age/fee admission features.

This local-only artifact tests whether a non-qid admission/size feature that
fixed the 20260527T0407Z bad residual tail is merely a one-window fit.  It
checks prior old-system positive windows where normalized event data is
available, records schema gaps where it is not, and keeps remote/deploy/live
authorization out of scope.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0627Z"

RUNS = [
    {
        "key": "old_best_20260522T1705",
        "run_tag": "no_order_soft_closeability_comparable_runtime_20260522T1705Z",
        "runtime_summary": None,
    },
    {
        "key": "cap25_high_roi_20260525T2041",
        "run_tag": "xuan-frontier-soft-mainline-cap25-reproduction-20260525T2041Z",
        "runtime_summary": ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-reproduction-20260525T2041Z_runtime_summary.json",
    },
    {
        "key": "capped_cap25_20260526T1757",
        "run_tag": "xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z",
        "runtime_summary": ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-density-preserving-pair-completion-capped-20260526T1757Z_runtime_summary.json",
    },
    {
        "key": "tail_mark_snapshot_20260527T0407",
        "run_tag": "xuan-frontier-soft-mainline-cap25-tail-mark-snapshot-20260527T0407Z",
        "runtime_summary": ".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-cap25-tail-mark-snapshot-20260527T0407Z_runtime_summary.json",
    },
]

DEFAULT_ARTIFACT_ROOT = Path(".tmp_xuan/local_verifier_artifacts")
DEFAULT_AGE_FEE_SCORER = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_old_system_age_fee_reprice_admission_scorer_20260527T0557Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_old_system_age_fee_cross_window_validation_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_old_system_age_fee_cross_window_validation_{STAMP}/"
    "AGE_FEE_CROSS_WINDOW_VALIDATION.md"
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
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def load_runtime_metrics(remote_outputs: Path, runtime_summary: str | None) -> dict[str, Any]:
    if runtime_summary:
        path = Path(runtime_summary)
        if path.exists():
            return normalize_metrics(body(load_json(path), "metrics"))
    aggregate = remote_outputs / "aggregate_report.json"
    if aggregate.exists():
        return normalize_metrics(body(load_json(aggregate), "metrics"))

    totals = {
        "accepted_actions": 0.0,
        "queue_supported_fills": 0.0,
        "filled_cost": 0.0,
        "filled_qty": 0.0,
        "pair_pnl": 0.0,
        "residual_cost": 0.0,
        "residual_qty": 0.0,
        "strict_rescue_closes": 0.0,
        "strict_rescue_source_blocks": 0.0,
    }
    for path in sorted(remote_outputs.glob("*.summary.json")):
        metrics = body(load_json(path), "metrics")
        totals["accepted_actions"] += fnum(metrics.get("candidates"), 0.0) or 0.0
        totals["queue_supported_fills"] += fnum(metrics.get("queue_supported_fills"), 0.0) or 0.0
        totals["filled_cost"] += fnum(metrics.get("filled_cost"), 0.0) or 0.0
        totals["filled_qty"] += fnum(metrics.get("filled_qty"), 0.0) or 0.0
        totals["pair_pnl"] += fnum(metrics.get("pair_pnl"), 0.0) or 0.0
        totals["residual_cost"] += fnum(metrics.get("residual_cost"), 0.0) or 0.0
        totals["residual_qty"] += fnum(metrics.get("residual_qty"), 0.0) or 0.0
        totals["strict_rescue_closes"] += fnum(metrics.get("strict_rescue_actions"), 0.0) or 0.0
        totals["strict_rescue_source_blocks"] += fnum(metrics.get("strict_rescue_source_blocks"), 0.0) or 0.0
    if totals["filled_cost"]:
        totals["roi_on_filled_cost"] = totals["pair_pnl"] / totals["filled_cost"]
        totals["residual_cost_share"] = totals["residual_cost"] / totals["filled_cost"]
    if totals["filled_qty"]:
        totals["residual_qty_share"] = totals["residual_qty"] / totals["filled_qty"]
    return normalize_metrics(totals)


def normalize_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    out = dict(metrics)
    if out.get("accepted_actions") is None and out.get("candidates") is not None:
        out["accepted_actions"] = out.get("candidates")
    if out.get("strict_rescue_closes") is None and out.get("strict_rescue_actions") is not None:
        out["strict_rescue_closes"] = out.get("strict_rescue_actions")
    filled_cost = fnum(out.get("filled_cost"), 0.0) or 0.0
    filled_qty = fnum(out.get("filled_qty"), 0.0) or 0.0
    pair_pnl = fnum(out.get("pair_pnl"), 0.0) or 0.0
    residual_cost = fnum(out.get("residual_cost"), 0.0) or 0.0
    residual_qty = fnum(out.get("residual_qty"), 0.0) or 0.0
    if out.get("roi_on_filled_cost") is None and filled_cost:
        out["roi_on_filled_cost"] = pair_pnl / filled_cost
    if out.get("residual_cost_share") is None and filled_cost:
        out["residual_cost_share"] = residual_cost / filled_cost
    if out.get("residual_qty_share") is None and filled_qty:
        out["residual_qty_share"] = residual_qty / filled_qty
    return rounded(out)


def load_accepted_actions(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.glob("*.would_action_decisions.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if row.get("kind") == "candidate" and row.get("decision") == "accept" and qid:
                row = dict(row)
                row["_source_file"] = path.name
                out[qid] = row
    return out


def load_candidate_events(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.glob("*.events.jsonl")):
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


def load_fill_qids(remote_outputs: Path) -> set[str]:
    out: set[str] = set()
    for path in sorted(remote_outputs.glob("*.would_fill_events.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if qid:
                out.add(qid)
    return out


def load_residual_lots(remote_outputs: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(remote_outputs.glob("*.residual_fifo_lots.csv")):
        for row in read_csv_rows(path):
            qid = str(row.get("quote_intent_id") or "")
            if qid:
                out[qid] = {
                    "quote_intent_id": qid,
                    "slug": row.get("slug"),
                    "side": row.get("side"),
                    "qty": fnum(row.get("qty"), 0.0) or 0.0,
                    "px": fnum(row.get("px"), 0.0) or 0.0,
                    "cost": fnum(row.get("cost"), 0.0) or 0.0,
                }
    return out


def feature_row(qid: str, accepted: dict[str, dict[str, Any]], events: dict[str, dict[str, Any]]) -> dict[str, Any]:
    row = accepted.get(qid, {})
    event = events.get(qid, {})
    return {
        "quote_intent_id": qid,
        "slug": row.get("slug") or event.get("slug"),
        "side": row.get("side") or event.get("side"),
        "price": fnum(row.get("price"), fnum(event.get("price"))),
        "qty": fnum(row.get("qty"), fnum(event.get("qty"))),
        "risk_increasing_seed": event.get("risk_increasing_seed"),
        "same_exposure_qty": fnum(event.get("same_exposure_qty")),
        "opp_exposure_qty": fnum(event.get("opp_exposure_qty")),
        "closeability_net_pair_cost": fnum(row.get("closeability_net_pair_cost"), fnum(event.get("closeability_net_pair_cost"))),
        "pair_completion_qty": fnum(row.get("pair_completion_qty"), fnum(event.get("pair_completion_qty"))),
        "pair_completion_decision": row.get("pair_completion_decision") or event.get("pair_completion_decision"),
        "surplus_budget_projected_unpaired_cost": fnum(
            row.get("surplus_budget_projected_unpaired_cost"),
            fnum(event.get("surplus_budget_projected_unpaired_cost")),
        ),
        "source_quality_decision": row.get("source_quality_decision") or event.get("source_quality_decision"),
        "source_quality_l1_age_ms": fnum(row.get("source_quality_l1_age_ms"), fnum(event.get("source_quality_l1_age_ms"))),
        "public_trade_px": fnum(event.get("public_trade_px")),
        "public_trade_size": fnum(event.get("public_trade_size")),
        "open_cost": fnum(event.get("open_cost")),
    }


def rule_v1(row: dict[str, Any]) -> bool:
    if row.get("risk_increasing_seed") is not True:
        return False
    if (fnum(row.get("pair_completion_qty"), 0.0) or 0.0) > 1e-12:
        return False
    if row.get("source_quality_decision") != "allow":
        return False
    qty = fnum(row.get("qty"), 0.0) or 0.0
    price = fnum(row.get("price"), 0.0) or 0.0
    surplus = fnum(row.get("surplus_budget_projected_unpaired_cost"), 0.0) or 0.0
    same = fnum(row.get("same_exposure_qty"), 0.0) or 0.0
    opp = fnum(row.get("opp_exposure_qty"), 0.0) or 0.0
    return (
        (surplus >= 0.90 and qty >= 2.0 and same <= 1e-12 and opp <= 1e-12)
        or (surplus <= 1e-12 and qty >= 3.5 and price <= 0.20)
        or (price <= 0.02 and qty >= 3.5)
    )


def rule_v2(row: dict[str, Any]) -> bool:
    if row.get("risk_increasing_seed") is not True:
        return False
    if (fnum(row.get("pair_completion_qty"), 0.0) or 0.0) > 1e-12:
        return False
    if row.get("source_quality_decision") != "allow":
        return False
    qty = fnum(row.get("qty"), 0.0) or 0.0
    price = fnum(row.get("price"), 0.0) or 0.0
    surplus = fnum(row.get("surplus_budget_projected_unpaired_cost"), 0.0) or 0.0
    same = fnum(row.get("same_exposure_qty"), 0.0) or 0.0
    opp = fnum(row.get("opp_exposure_qty"), 0.0) or 0.0
    age = fnum(row.get("source_quality_l1_age_ms"), 0.0) or 0.0
    high_surplus_clause = surplus >= 0.90 and qty >= 2.0 and same <= 1e-12 and opp <= 1e-12 and age >= 12.0
    low_price_no_pair_clause = surplus <= 1e-12 and qty >= 3.5 and price <= 0.20
    tiny_price_large_qty_clause = price <= 0.02 and qty >= 3.5
    return high_surplus_clause or low_price_no_pair_clause or tiny_price_large_qty_clause


def capacity_after(
    metrics: dict[str, Any],
    residual_lots: dict[str, dict[str, Any]],
    controlled_qids: set[str],
    *,
    max_residual_cost_share: float,
    max_residual_qty_share: float,
    max_residual_cost_to_pair_qty: float,
) -> dict[str, Any]:
    removed_cost = sum((fnum(residual_lots[qid].get("cost"), 0.0) or 0.0) for qid in controlled_qids if qid in residual_lots)
    removed_qty = sum((fnum(residual_lots[qid].get("qty"), 0.0) or 0.0) for qid in controlled_qids if qid in residual_lots)
    residual_cost = max(0.0, (fnum(metrics.get("residual_cost"), 0.0) or 0.0) - removed_cost)
    residual_qty = max(0.0, (fnum(metrics.get("residual_qty"), 0.0) or 0.0) - removed_qty)
    filled_cost = max(0.0, (fnum(metrics.get("filled_cost"), 0.0) or 0.0) - removed_cost)
    filled_qty = max(0.0, (fnum(metrics.get("filled_qty"), 0.0) or 0.0) - removed_qty)
    cost_share = residual_cost / filled_cost if filled_cost else None
    qty_share = residual_qty / filled_qty if filled_qty else None
    cost_to_pair_qty = residual_cost / filled_qty if filled_qty else None
    return rounded(
        {
            "removed_residual_cost": removed_cost,
            "removed_residual_qty": removed_qty,
            "residual_cost_after": residual_cost,
            "residual_qty_after": residual_qty,
            "filled_cost_after": filled_cost,
            "filled_qty_after": filled_qty,
            "residual_cost_share_after": cost_share,
            "residual_qty_share_after": qty_share,
            "residual_cost_to_pair_qty_after": cost_to_pair_qty,
            "residual_cost_share_pass": cost_share is not None and cost_share <= max_residual_cost_share,
            "residual_qty_share_pass": qty_share is not None and qty_share <= max_residual_qty_share,
            "residual_cost_to_pair_qty_pass": cost_to_pair_qty is not None and cost_to_pair_qty <= max_residual_cost_to_pair_qty,
            "all_capacity_pass": (
                cost_share is not None
                and cost_share <= max_residual_cost_share
                and qty_share is not None
                and qty_share <= max_residual_qty_share
                and cost_to_pair_qty is not None
                and cost_to_pair_qty <= max_residual_cost_to_pair_qty
            ),
        }
    )


def evaluate_rule(
    name: str,
    selector: Any,
    rows: dict[str, dict[str, Any]],
    accepted: dict[str, dict[str, Any]],
    fills: set[str],
    residual_lots: dict[str, dict[str, Any]],
    metrics: dict[str, Any],
    *,
    target_bad_tail_qids: set[str],
    high_recovery_qids: set[str],
    min_accepted_after: int,
    min_fills_after: int,
    min_fill_rate_after: float,
    max_residual_cost_share: float,
    max_residual_qty_share: float,
    max_residual_cost_to_pair_qty: float,
) -> dict[str, Any]:
    controlled = {qid for qid, row in rows.items() if selector(row)}
    controlled_accepts = controlled & set(accepted)
    controlled_fills = controlled & fills
    controlled_residual = controlled & set(residual_lots)
    accepted_after = len(accepted) - len(controlled_accepts)
    fills_after = len(fills) - len(controlled_fills)
    fill_rate_after = fills_after / accepted_after if accepted_after else None
    capacity = capacity_after(
        metrics,
        residual_lots,
        controlled,
        max_residual_cost_share=max_residual_cost_share,
        max_residual_qty_share=max_residual_qty_share,
        max_residual_cost_to_pair_qty=max_residual_cost_to_pair_qty,
    )
    density_pass = (
        accepted_after >= min_accepted_after
        and fills_after >= min_fills_after
        and fill_rate_after is not None
        and fill_rate_after >= min_fill_rate_after
    )
    non_residual = controlled_accepts - controlled_residual
    target_coverage_pass = target_bad_tail_qids <= controlled if target_bad_tail_qids else True
    high_recovery_preserved = not bool(controlled & high_recovery_qids)
    no_false_positive_pass = not non_residual
    return rounded(
        {
            "rule": name,
            "controlled_qids": sorted(controlled),
            "controlled_accept_count": len(controlled_accepts),
            "controlled_fill_count": len(controlled_fills),
            "controlled_residual_count": len(controlled_residual),
            "non_residual_accepts_controlled": sorted(non_residual),
            "accepted_after": accepted_after,
            "fills_after": fills_after,
            "fill_rate_after": fill_rate_after,
            "density_pass": density_pass,
            "target_bad_tail_qids_covered": target_coverage_pass,
            "target_bad_tail_missing_qids": sorted(target_bad_tail_qids - controlled),
            "high_recovery_preserved": high_recovery_preserved,
            "high_recovery_controlled_qids": sorted(controlled & high_recovery_qids),
            "no_false_positive_accept_control_pass": no_false_positive_pass,
            "capacity_after": capacity,
            "pair_pnl_proxy": metrics.get("pair_pnl"),
            "roi_after_proxy": (
                (fnum(metrics.get("pair_pnl"), 0.0) or 0.0) / (fnum(capacity.get("filled_cost_after"), 0.0) or 1.0)
                if capacity.get("filled_cost_after") is not None
                else None
            ),
            "validation_pass": density_pass and high_recovery_preserved and no_false_positive_pass and target_coverage_pass,
        }
    )


def run_card(run: dict[str, Any], args: argparse.Namespace, target_bad_tail_qids: set[str], high_recovery_qids: set[str]) -> dict[str, Any]:
    remote_outputs = args.artifact_root / run["run_tag"] / "remote_outputs"
    accepted = load_accepted_actions(remote_outputs)
    events = load_candidate_events(remote_outputs)
    fills = load_fill_qids(remote_outputs)
    residual_lots = load_residual_lots(remote_outputs)
    metrics = load_runtime_metrics(remote_outputs, run.get("runtime_summary"))
    rows = {qid: feature_row(qid, accepted, events) for qid in accepted}
    missing_event_rows = [qid for qid, row in rows.items() if row.get("risk_increasing_seed") is None]
    schema_complete = bool(accepted) and not missing_event_rows
    run_targets = target_bad_tail_qids & set(accepted)
    run_high_recovery = high_recovery_qids & set(accepted)
    v1 = evaluate_rule(
        "v1_single_window_broad",
        rule_v1,
        rows,
        accepted,
        fills,
        residual_lots,
        metrics,
        target_bad_tail_qids=run_targets,
        high_recovery_qids=run_high_recovery,
        min_accepted_after=args.min_accepted_after,
        min_fills_after=args.min_fills_after,
        min_fill_rate_after=args.min_fill_rate_after,
        max_residual_cost_share=args.max_residual_cost_share,
        max_residual_qty_share=args.max_residual_qty_share,
        max_residual_cost_to_pair_qty=args.max_residual_cost_to_pair_qty,
    )
    v2 = evaluate_rule(
        "v2_source_age_refined",
        rule_v2,
        rows,
        accepted,
        fills,
        residual_lots,
        metrics,
        target_bad_tail_qids=run_targets,
        high_recovery_qids=run_high_recovery,
        min_accepted_after=args.min_accepted_after,
        min_fills_after=args.min_fills_after,
        min_fill_rate_after=args.min_fill_rate_after,
        max_residual_cost_share=args.max_residual_cost_share,
        max_residual_qty_share=args.max_residual_qty_share,
        max_residual_cost_to_pair_qty=args.max_residual_cost_to_pair_qty,
    )
    if not schema_complete:
        reason = "candidate event features missing; strict non-qid rule cannot be evaluated"
        for rule in (v1, v2):
            rule["strict_feature_validation_inconclusive"] = True
            rule["validation_pass"] = None
            rule["inconclusive_reason"] = reason
    return rounded(
        {
            "key": run["key"],
            "run_tag": run["run_tag"],
            "remote_outputs": str(remote_outputs),
            "schema_complete_for_strict_feature_validation": schema_complete,
            "accepted_actions_loaded": len(accepted),
            "candidate_events_loaded": len(events),
            "missing_event_feature_rows": len(missing_event_rows),
            "unique_fill_qids_loaded": len(fills),
            "residual_lots_loaded": len(residual_lots),
            "metrics": {
                "accepted_actions": metrics.get("accepted_actions"),
                "queue_supported_fills": metrics.get("queue_supported_fills"),
                "filled_cost": metrics.get("filled_cost"),
                "filled_qty": metrics.get("filled_qty"),
                "pair_pnl": metrics.get("pair_pnl"),
                "roi_on_filled_cost": metrics.get("roi_on_filled_cost"),
                "residual_cost": metrics.get("residual_cost"),
                "residual_qty": metrics.get("residual_qty"),
                "residual_cost_share": metrics.get("residual_cost_share"),
                "residual_qty_share": metrics.get("residual_qty_share"),
                "strict_rescue_closes": metrics.get("strict_rescue_closes"),
                "strict_rescue_source_blocks": metrics.get("strict_rescue_source_blocks"),
            },
            "rules": {
                "v1_single_window_broad": v1,
                "v2_source_age_refined": v2,
            },
        }
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    age_fee_scorer = load_json(args.age_fee_scorer)
    feature_proxy = body(body(age_fee_scorer, "admission_avoidance_proxies"), "non_qid_admission_feature_hypothesis_single_window")
    target_bad_tail_qids = set(feature_proxy.get("controlled_qids") or [])
    high_recovery_qids = set(feature_proxy.get("high_recovery_residual_qids_controlled") or [])
    # The scorer records controlled high-recovery qids, not all qids to preserve.  Load the
    # source artifact for the full preserve set when available.
    age_fee_frontier_path = Path(
        ".tmp_xuan/scorecards/xuan_shadow_review_old_system_residual_age_fee_control_frontier_20260527T0457Z.json"
    )
    if age_fee_frontier_path.exists():
        frontier = load_json(age_fee_frontier_path)
        high_recovery_qids = {
            str(row.get("quote_intent_id"))
            for row in body(frontier, "lot_frontier").get("top_residual_lots", [])
            if isinstance(row, dict) and (fnum(row.get("mature_recovery_rate_after_fee"), 0.0) or 0.0) >= 0.60
        }

    cards = [run_card(run, args, target_bad_tail_qids, high_recovery_qids) for run in RUNS]
    complete = [card for card in cards if card["schema_complete_for_strict_feature_validation"]]
    incomplete = [card for card in cards if not card["schema_complete_for_strict_feature_validation"]]
    v1_false_positive_runs = [
        card["key"]
        for card in complete
        if card["rules"]["v1_single_window_broad"]["no_false_positive_accept_control_pass"] is not True
    ]
    v2_failed_runs = [
        card["key"]
        for card in complete
        if card["rules"]["v2_source_age_refined"]["validation_pass"] is not True
    ]
    v2_target_runs = [
        card["key"]
        for card in complete
        if card["rules"]["v2_source_age_refined"]["controlled_residual_count"] > 0
    ]
    v2_0407 = next((card for card in cards if card["key"] == "tail_mark_snapshot_20260527T0407"), {})
    v2_0407_rule = body(body(v2_0407, "rules"), "v2_source_age_refined")
    complete_validation_pass = bool(complete) and not v2_failed_runs
    has_incomplete_anchor = bool(incomplete)
    candidate_ready = complete_validation_pass and bool(v2_0407_rule.get("capacity_after", {}).get("all_capacity_pass"))

    hard_blockers = ["private_truth_not_ready", "runner_support_not_implemented"]
    if v1_false_positive_runs:
        hard_blockers.append("v1_broad_feature_false_positive_on_prior_positive_windows")
    if has_incomplete_anchor:
        hard_blockers.append("old_best_1705_missing_event_feature_schema_for_strict_validation")
    if v2_failed_runs:
        hard_blockers.append("v2_refined_feature_failed_complete_window_validation")
    hard_blockers.append("v2_refined_feature_needs_independent_holdout_or_l2_backtest_bridge")

    return rounded(
        {
            "artifact": "xuan_shadow_review_old_system_age_fee_cross_window_validation",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_old_system_age_fee_cross_window_validation.py",
            "status": "KEEP_SHADOW_REVIEW_OLD_SYSTEM_AGE_FEE_CROSS_WINDOW_VALIDATION_READY_LOCAL_ONLY",
            "inputs": {
                "artifact_root": str(args.artifact_root),
                "age_fee_scorer": str(args.age_fee_scorer),
            },
            "source_statuses": {
                "age_fee_scorer": status(age_fee_scorer),
            },
            "target_sets": {
                "bad_tail_qids_from_0407_single_window_scorer": sorted(target_bad_tail_qids),
                "high_recovery_residual_qids_to_preserve": sorted(high_recovery_qids),
            },
            "rule_history": {
                "v1_single_window_broad": {
                    "validation": "failed",
                    "false_positive_runs": v1_false_positive_runs,
                    "read": "The first single-window rule over-controls prior positive windows, so it is not suitable for runner support.",
                },
                "v2_source_age_refined": {
                    "validation": "passes_complete_local_windows_but_not_full_program_ready",
                    "failed_complete_runs": v2_failed_runs,
                    "target_runs": v2_target_runs,
                    "rule": {
                        "common": [
                            "risk_increasing_seed is true",
                            "pair_completion_qty == 0",
                            "source_quality_decision == allow",
                        ],
                        "clauses": [
                            "surplus_budget_projected_unpaired_cost>=0.90 and qty>=2.0 and same_exposure_qty==0 and opp_exposure_qty==0 and source_quality_l1_age_ms>=12",
                            "surplus_budget_projected_unpaired_cost==0 and qty>=3.5 and price<=0.20",
                            "price<=0.02 and qty>=3.5",
                        ],
                    },
                },
            },
            "runs": cards,
            "decision": {
                "old_system_age_fee_cross_window_validation_ready": True,
                "use_backtest_v1_candidate_audit_pack": False,
                "complete_window_validation_pass": complete_validation_pass,
                "v1_broad_rule_discarded": bool(v1_false_positive_runs),
                "v2_refined_rule_no_false_positive_on_complete_windows": not v2_failed_runs,
                "v2_refined_rule_fixes_0407_bad_tail_under_local_proxy": bool(
                    v2_0407_rule.get("capacity_after", {}).get("all_capacity_pass")
                ),
                "v2_refined_rule_is_universal_residual_fix": False,
                "old_best_1705_strict_feature_validation_complete": not has_incomplete_anchor,
                "candidate_profile_ready": False,
                "runner_support_ready": False,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_validation": False,
                "future_remote_requires_runner_support_and_independent_validation": True,
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
                    "local-only convert v2 into an explicit offline scorer/runner-support proposal and validate against "
                    "an independent L2/V1 xuan bridge or additional old-system windows before any manifest/preauthorization"
                ),
            },
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    lines = [
        "# Xuan Old-System Age/Fee Cross-Window Validation",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- complete_window_validation_pass: `{d['complete_window_validation_pass']}`",
        f"- v1_broad_rule_discarded: `{d['v1_broad_rule_discarded']}`",
        f"- v2_refined_rule_no_false_positive_on_complete_windows: `{d['v2_refined_rule_no_false_positive_on_complete_windows']}`",
        f"- v2_refined_rule_fixes_0407_bad_tail_under_local_proxy: `{d['v2_refined_rule_fixes_0407_bad_tail_under_local_proxy']}`",
        f"- old_best_1705_strict_feature_validation_complete: `{d['old_best_1705_strict_feature_validation_complete']}`",
        f"- candidate_profile_ready: `{d['candidate_profile_ready']}`",
        f"- future_remote_allowed_by_this_validation: `{d['future_remote_allowed_by_this_validation']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers'])}`",
        "",
        "## Rule History",
        "",
        f"- v1 broad false-positive runs: `{card['rule_history']['v1_single_window_broad']['false_positive_runs']}`",
        f"- v2 failed complete runs: `{card['rule_history']['v2_source_age_refined']['failed_complete_runs']}`",
        f"- v2 target runs: `{card['rule_history']['v2_source_age_refined']['target_runs']}`",
        "",
        "## Runs",
        "",
    ]
    for run in card["runs"]:
        v2 = run["rules"]["v2_source_age_refined"]
        lines.extend(
            [
                f"### {run['key']}",
                "",
                f"- schema complete: `{run['schema_complete_for_strict_feature_validation']}`",
                f"- accepted/events/residual lots: `{run['accepted_actions_loaded']}` / `{run['candidate_events_loaded']}` / `{run['residual_lots_loaded']}`",
                f"- pair_pnl/ROI: `{run['metrics']['pair_pnl']}` / `{run['metrics']['roi_on_filled_cost']}`",
                f"- residual cost/qty share: `{run['metrics']['residual_cost_share']}` / `{run['metrics']['residual_qty_share']}`",
                f"- v2 controlled qids: `{v2['controlled_qids']}`",
                f"- v2 non-residual accepts controlled: `{v2['non_residual_accepts_controlled']}`",
                f"- v2 accepted/fills after: `{v2['accepted_after']}` / `{v2['fills_after']}`",
                f"- v2 capacity pass: `{v2['capacity_after']['all_capacity_pass']}`",
                f"- v2 validation pass: `{v2['validation_pass']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Boundary",
            "",
            "- Old/current xuan no-order outputs only; V1 candidate audit pack is not used.",
            "- Local-only; no remote authorization, cap75/150/300, deploy, live orders, or private truth.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--age-fee-scorer", type=Path, default=DEFAULT_AGE_FEE_SCORER)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.15)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.20)
    parser.add_argument("--max-residual-cost-to-pair-qty", type=float, default=0.05)
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
