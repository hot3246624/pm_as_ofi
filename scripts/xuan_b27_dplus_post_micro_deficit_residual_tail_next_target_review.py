#!/usr/bin/env python3
"""Review next target after the micro-deficit no-order shadow review.

This is a local-only status-hygiene reader for already-pulled no-order
manifest/aggregate/summary/scorer artifacts. It does not read events JSONL,
raw/replay stores, sockets, SSH, or live order paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/replay",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return numerator / denominator


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def expand_summary_paths(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        if path.is_dir():
            out.extend(sorted(path.glob("*.summary.json")))
        else:
            out.append(path)
    if not out:
        raise ValueError("no summary JSON files found")
    return out


def residual_tail_exemplars(summary: dict[str, Any]) -> list[dict[str, Any]]:
    event_lite = summary.get("event_lite")
    if not isinstance(event_lite, dict):
        return []
    rows = event_lite.get("source_link_residual_tail_exemplars")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def offset_bucket(value: Any) -> str:
    offset = as_float(value, default=-1.0)
    if offset < 0.0:
        return "offset_unknown"
    if offset < 30.0:
        return "offset_0_30"
    if offset < 60.0:
        return "offset_30_60"
    if offset < 90.0:
        return "offset_60_90"
    if offset < 120.0:
        return "offset_90_120"
    return "offset_gte_120"


def open_qty_bucket(value: Any) -> str:
    open_qty = as_float(value)
    if open_qty <= 1.0:
        return "open_le_1"
    if open_qty <= 2.5:
        return "open_le_2_5"
    if open_qty <= 5.0:
        return "open_le_5"
    return "open_gt_5"


def deficit_bucket(row: dict[str, Any]) -> str:
    deficit = as_float(row.get("pre_seed_opp_qty")) - as_float(row.get("pre_seed_same_qty"))
    if deficit <= 0.0:
        return "deficit_le_0"
    if deficit <= 0.25:
        return "deficit_le_0_25"
    if deficit <= 1.25:
        return "deficit_le_1_25"
    return "deficit_gt_1_25"


def pre_seed_open_qty(row: dict[str, Any]) -> float:
    if "pre_seed_open_qty" in row:
        return as_float(row.get("pre_seed_open_qty"))
    return as_float(row.get("pre_seed_same_qty")) + as_float(row.get("pre_seed_opp_qty"))


def group_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("side") or "UNKNOWN"),
            offset_bucket(row.get("offset_s")),
            str(row.get("source_risk_direction") or "UNKNOWN"),
            open_qty_bucket(pre_seed_open_qty(row)),
            deficit_bucket(row),
        ]
    )


def group_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = group_key(row)
        group = groups.setdefault(
            key,
            {
                "pre_action_group": key,
                "row_count": 0,
                "condition_count": 0,
                "conditions": set(),
                "source_pair_qty": 0.0,
                "source_pair_pnl": 0.0,
                "source_residual_qty": 0.0,
                "source_residual_cost": 0.0,
            },
        )
        group["row_count"] += 1
        group["conditions"].add(str(row.get("condition_id") or ""))
        group["source_pair_qty"] += as_float(row.get("source_pair_qty"))
        group["source_pair_pnl"] += as_float(row.get("source_pair_pnl"))
        group["source_residual_qty"] += as_float(row.get("source_residual_qty"))
        group["source_residual_cost"] += as_float(row.get("source_residual_cost"))

    total_cost = sum(as_float(row.get("source_residual_cost")) for row in rows)
    total_qty = sum(as_float(row.get("source_residual_qty")) for row in rows)
    output: list[dict[str, Any]] = []
    for group in groups.values():
        output.append(
            {
                "pre_action_group": group["pre_action_group"],
                "row_count": group["row_count"],
                "condition_count": len(group["conditions"]),
                "source_pair_qty": round(as_float(group["source_pair_qty"]), 8),
                "source_pair_pnl": round(as_float(group["source_pair_pnl"]), 8),
                "source_residual_qty": round(as_float(group["source_residual_qty"]), 8),
                "source_residual_cost": round(as_float(group["source_residual_cost"]), 8),
                "source_residual_cost_share": round(
                    safe_ratio(as_float(group["source_residual_cost"]), total_cost), 8
                ),
                "source_residual_qty_share": round(
                    safe_ratio(as_float(group["source_residual_qty"]), total_qty), 8
                ),
            }
        )
    return sorted(output, key=lambda item: (-as_float(item.get("source_residual_cost")), -int(item["row_count"])))


def collect_exemplars(summary_paths: list[Path], residual_score: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in summary_paths:
        rows.extend(residual_tail_exemplars(load_json(path)))
    if rows:
        return rows
    ranked = residual_score.get("rankings", {}).get("top_residual_tail_exemplars", [])
    return [row for row in ranked if isinstance(row, dict)]


def review(
    *,
    completion_manifest_path: Path,
    residual_tail_score_path: Path,
    micro_deficit_score_path: Path,
    aggregate_report_path: Path,
    summary_paths: list[Path],
) -> dict[str, Any]:
    input_paths = [
        completion_manifest_path,
        residual_tail_score_path,
        micro_deficit_score_path,
        aggregate_report_path,
        *summary_paths,
    ]
    unsafe_paths = [str(path) for path in input_paths if not path_safe(path)]
    completion = load_json(completion_manifest_path)
    residual_score = load_json(residual_tail_score_path)
    micro_score = load_json(micro_deficit_score_path)
    aggregate = load_json(aggregate_report_path)
    rows = collect_exemplars(summary_paths, residual_score)
    groups = group_rows(rows)

    aggregate_metrics = completion.get("aggregate_metrics")
    if not isinstance(aggregate_metrics, dict):
        aggregate_metrics = aggregate.get("aggregate_metrics")
    if not isinstance(aggregate_metrics, dict):
        aggregate_metrics = aggregate

    residual_metrics = residual_score.get("exemplar_metrics", {})
    if not isinstance(residual_metrics, dict):
        residual_metrics = {}
    micro_metrics = micro_score.get("metrics", {})
    if not isinstance(micro_metrics, dict):
        micro_metrics = {}

    total_residual_cost = sum(as_float(row.get("source_residual_cost")) for row in rows)
    total_pair_qty = sum(as_float(row.get("source_pair_qty")) for row in rows)
    repair_rows = [row for row in rows if row.get("source_risk_direction") == "repair_or_pairing_improving"]
    risk_increasing_rows = [row for row in rows if row.get("source_risk_direction") == "risk_increasing"]
    top_group = groups[0] if groups else {}

    blocker_notes = []
    if unsafe_paths:
        blocker_notes.append("unsafe_input_path")
    if completion.get("decision") != "UNKNOWN":
        blocker_notes.append("completion_decision_not_unknown")
    if as_float(micro_metrics.get("micro_deficit_exemplar_count")) != 0.0:
        blocker_notes.append("micro_deficit_signature_present_unexpectedly")
    if as_float(aggregate_metrics.get("candidates")) < 100.0:
        blocker_notes.append("shadow_sample_below_100")
    if as_float(aggregate_metrics.get("net_pair_cost_proxy_p90") or aggregate_metrics.get("net_pair_cost_p90")) > 1.0:
        blocker_notes.append("net_pair_cost_p90_above_1")
    if as_float(aggregate_metrics.get("residual_qty_share_of_filled")) > 0.15:
        blocker_notes.append("residual_qty_share_above_budget")
    if as_float(aggregate_metrics.get("residual_cost_share_of_filled_cost")) > 0.20:
        blocker_notes.append("residual_cost_share_above_budget")
    if as_float(residual_metrics.get("required_field_coverage")) < 0.95:
        blocker_notes.append("residual_tail_required_field_coverage_below_95pct")
    if as_float(residual_metrics.get("source_sequence_coverage")) < 0.95:
        blocker_notes.append("source_sequence_coverage_below_95pct")

    candidate_reviews = [
        {
            "candidate": "strict_micro_deficit_repair_guard_v1",
            "decision": "DISCARD_FOR_THIS_WINDOW",
            "reason": (
                "The exact approved no-order window exposed zero strict micro-deficit exemplars and zero aggregate "
                "micro_deficit_repair_guard_candidates, so this live window cannot justify enabling the guard."
            ),
            "evidence": {
                "micro_deficit_exemplar_count": micro_metrics.get("micro_deficit_exemplar_count"),
                "micro_deficit_repair_guard_candidates": aggregate_metrics.get("micro_deficit_repair_guard_candidates"),
                "micro_deficit_repair_guard_blocks": aggregate_metrics.get("micro_deficit_repair_guard_blocks"),
            },
        },
        {
            "candidate": "top_residual_tail_pre_action_group_guard",
            "decision": "DISCARD",
            "reason": (
                "The strongest remaining group is a side/offset/risk/open/deficit slice observed only among residual-tail "
                "exemplars. Converting it to a rule would be static side/offset deletion plus an ungrounded threshold, "
                "with no denominator for admitted non-tail candidates."
            ),
            "evidence": top_group,
        },
        {
            "candidate": "repair_or_pairing_improving_tail_guard",
            "decision": "DISCARD",
            "reason": (
                "Repair_or_pairing_improving dominates the residual-tail exemplars, but that label also carries needed "
                "repair/pair flow. A broad guard would repeat the sample/pair collapse pattern from earlier local guards."
            ),
            "evidence": {
                "repair_exemplar_count": len(repair_rows),
                "repair_residual_cost_share": round(
                    safe_ratio(sum(as_float(row.get("source_residual_cost")) for row in repair_rows), total_residual_cost),
                    8,
                ),
                "risk_increasing_exemplar_count": len(risk_increasing_rows),
            },
        },
        {
            "candidate": "source_pair_zero_or_source_residual_cost_rule",
            "decision": "DISCARD",
            "reason": (
                "All residual-tail exemplars are post-action zero-pair labels in this pullback. source_pair_* and "
                "source_residual_* are valid offline labels but invalid live pre-action criteria."
            ),
            "evidence": {
                "total_exemplar_source_pair_qty": round(total_pair_qty, 8),
                "total_exemplar_source_residual_cost": round(total_residual_cost, 8),
            },
        },
        {
            "candidate": "trigger_px_or_trigger_size_tail_cap",
            "decision": "DISCARD",
            "reason": (
                "Trigger price/size slicing would become a price/cap family or ungrounded threshold sweep; prior "
                "price/cap families are frozen unless a new pre-action causal denominator is available."
            ),
        },
    ]

    exact_missing = [
        "all same-window admitted candidates with the same pre-action fields, not only residual-tail exemplars",
        "non-tail and paired control rows joined to source_sequence_id/quote_intent_id/source_order_id",
        "per-candidate denominator for side/offset/risk/open/deficit groups, including pair_qty and residual labels",
        "runner-observed pending/opposite order availability and queue/close opportunity at source action time",
        "action-level follow-up close/rescue linkage after each seed",
        "candidate_row_id or equivalent local candidate-pipeline join key for causality verifier replay",
        "train/holdout split proving the separator was not chosen from post-action residual labels only",
        "a conservative fill/queue model calibrated against no-order marker counts before any new shadow question",
    ]

    decision_label = "UNKNOWN_POST_MICRO_DEFICIT_RESIDUAL_TAIL_NO_SAFE_PRE_ACTION_SEPARATOR"
    return {
        "artifact": "xuan_b27_dplus_post_micro_deficit_residual_tail_next_target_review",
        "created_utc": utc_now(),
        "schema_version": "post_micro_deficit_residual_tail_next_target_review_v1",
        "decision": "UNKNOWN",
        "decision_label": decision_label,
        "scope": "local_only_post_run_residual_tail_next_target_review",
        "inputs": {
            "completion_manifest": str(completion_manifest_path),
            "residual_tail_score": str(residual_tail_score_path),
            "micro_deficit_score": str(micro_deficit_score_path),
            "aggregate_report": str(aggregate_report_path),
            "summary_count": len(summary_paths),
        },
        "source_evidence": {
            "completion_decision_label": completion.get("decision_label"),
            "residual_tail_status": residual_score.get("decision_label") or residual_score.get("decision"),
            "micro_deficit_decision_label": micro_score.get("decision_label"),
            "exemplar_count": len(rows),
            "residual_tail_required_field_coverage": residual_metrics.get("required_field_coverage"),
            "residual_tail_source_sequence_coverage": residual_metrics.get("source_sequence_coverage"),
            "total_exemplar_source_residual_cost": round(total_residual_cost, 8),
            "total_exemplar_source_pair_qty": round(total_pair_qty, 8),
            "top_pre_action_groups": groups[:8],
        },
        "shadow_budget_summary": {
            "candidates": aggregate_metrics.get("candidates"),
            "queue_supported_fills": aggregate_metrics.get("queue_supported_fills"),
            "pair_qty": aggregate_metrics.get("pair_qty"),
            "pair_pnl": aggregate_metrics.get("pair_pnl"),
            "fee_adjusted_pair_pnl_proxy": aggregate_metrics.get("fee_adjusted_pair_pnl_proxy"),
            "net_pair_cost_p90": aggregate_metrics.get("net_pair_cost_proxy_p90")
            or aggregate_metrics.get("net_pair_cost_p90"),
            "residual_qty_share_of_filled": aggregate_metrics.get("residual_qty_share_of_filled"),
            "residual_cost_share_of_filled_cost": aggregate_metrics.get("residual_cost_share_of_filled_cost"),
            "pair_tail_loss_share_of_pair_pnl": completion.get("scorer_results", {})
            .get("shadow_acceptance", {})
            .get("promotion_gate", {})
            .get("normalized_risk_budget", {})
            .get("pair_tail_loss_share_of_pair_pnl"),
            "material_residual_lots": aggregate_metrics.get("material_residual_lots"),
        },
        "blockers": blocker_notes,
        "candidate_reviews": candidate_reviews,
        "selected_next_target": {
            "name": None,
            "reason": (
                "No genuinely new non-price, sample-preserving pre-action separator is expressible from the completed "
                "micro-deficit pullback. The remaining patterns are residual-tail labels, static side/offset slices, "
                "or ungrounded threshold/cap sweeps."
            ),
        },
        "exact_missing_fields_or_capabilities": exact_missing,
        "backtest_methodology_risk": {
            "selection_leakage_risk": True,
            "reason": (
                "The strongest local historical targets were discovered with post-action source_pair/source_residual "
                "labels. That is useful for diagnostics but cannot be treated as out-of-sample trading evidence."
            ),
            "state_machine_to_no_order_gap": (
                "Local state-machine fills and immediate pairing do not model live queue/opportunity availability; "
                "the no-order marker count mismatch is direct evidence of that gap."
            ),
        },
        "rejected_next_directions": [
            "repeat or tighten micro-deficit threshold/cap sweep",
            "source_pair/source_residual future-label live gate",
            "static side/offset deletion",
            "public/L1 price cap",
            "trigger price or size cap family",
            "cooldown/admission cap",
            "fill-to-balance or portfolio-ledger trading-rule revival",
            "broad replay search",
            "shadow/canary/private truth/promotion claim",
        ],
        "research_ranking": {
            "decision": "UNKNOWN",
            "label": decision_label,
            "interpretation": (
                "The no-order economics are positive but sample/risk budgets fail, and the local micro-deficit "
                "signature did not reproduce live. This is an evidence/methodology gap, not D+ total failure."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "LOCAL_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "side_effects": {
            "ssh_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "Freeze new micro-mechanism selection and implement local-only backtest_causality_audit_v1: classify "
            "pre-action versus post-action fields, flag selector outcome-label usage, add train/holdout checks, and "
            "downgrade state-machine results that lack no-order marker reproduction."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--completion-manifest", type=Path, required=True)
    parser.add_argument("--residual-tail-score", type=Path, required=True)
    parser.add_argument("--micro-deficit-score", type=Path, required=True)
    parser.add_argument("--aggregate-report", type=Path, required=True)
    parser.add_argument("--summaries", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    summary_paths = expand_summary_paths(args.summaries)
    result = review(
        completion_manifest_path=args.completion_manifest,
        residual_tail_score_path=args.residual_tail_score,
        micro_deficit_score_path=args.micro_deficit_score,
        aggregate_report_path=args.aggregate_report,
        summary_paths=summary_paths,
    )
    write_json(args.output, result)
    print(json.dumps({"decision": result["decision"], "decision_label": result["decision_label"], "output": str(args.output)}))
    return 0 if result["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
