#!/usr/bin/env python3
"""Audit why a stable historical D+ separator does not reproduce in no-order.

This local-only reader compares the frozen train/holdout predicate against
already-pulled no-order summary/source-link artifacts. It does not read events
JSONL, raw/replay stores, collector stores, sockets, SSH, shared-ingress, or
live trading state.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_source_opportunity_gap_audit_v1"
DEFAULT_TRAIN_HOLDOUT_MANIFEST = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_separator_train_holdout_audit_20260523T041233Z/"
    "manifest.json"
)
DEFAULT_SEPARATOR_CSV = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
DEFAULT_NO_ORDER_COMPLETION = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_micro_deficit_completion_20260523T022553Z/"
    "manifest.json"
)
DEFAULT_NO_ORDER_SUMMARY_DIR = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_review_micro_deficit_driver_20260523T005615Z/"
    "remote_clean/output"
)
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
DUST = 1e-12


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path) -> dict[str, Any]:
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
    if denominator <= DUST:
        return 0.0
    return numerator / denominator


def offset_bucket_value(offset: float) -> str:
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


def offset_bucket(row: dict[str, Any]) -> str:
    return offset_bucket_value(as_float(row.get("offset_s"), default=-1.0))


def open_qty(row: dict[str, Any]) -> float:
    if row.get("pre_seed_open_qty") not in (None, ""):
        return as_float(row.get("pre_seed_open_qty"))
    return as_float(row.get("pre_seed_same_qty")) + as_float(row.get("pre_seed_opp_qty"))


def open_bucket(row: dict[str, Any]) -> str:
    value = open_qty(row)
    if value <= 1.0 + DUST:
        return "open_le_1"
    if value <= 2.5 + DUST:
        return "open_le_2_5"
    if value <= 5.0 + DUST:
        return "open_le_5"
    return "open_gt_5"


def deficit(row: dict[str, Any]) -> float:
    return as_float(row.get("pre_seed_opp_qty")) - as_float(row.get("pre_seed_same_qty"))


def deficit_bucket(row: dict[str, Any]) -> str:
    value = deficit(row)
    if value <= 0.0 + DUST:
        return "deficit_le_0"
    if value <= 0.25 + DUST:
        return "deficit_0_0_25"
    if value <= 1.25 + DUST:
        return "deficit_0_25_1_25"
    return "deficit_gt_1_25"


def predicate_matches(row: dict[str, Any], predicate: dict[str, Any]) -> bool:
    side = str(predicate.get("side") or "ANY")
    if side != "ANY" and str(row.get("side") or "") != side:
        return False
    risk = str(predicate.get("source_risk_direction") or "ANY")
    if risk != "ANY" and str(row.get("source_risk_direction") or "") != risk:
        return False
    if str(predicate.get("offset_bucket") or "ANY") != "ANY" and offset_bucket(row) != predicate["offset_bucket"]:
        return False
    if str(predicate.get("open_bucket") or "ANY") != "ANY" and open_bucket(row) != predicate["open_bucket"]:
        return False
    if str(predicate.get("deficit_bucket") or "ANY") != "ANY" and deficit_bucket(row) != predicate["deficit_bucket"]:
        return False
    return True


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    seed_qty = sum(as_float(row.get("seed_qty")) for row in rows)
    seed_cost = sum(as_float(row.get("seed_cost")) for row in rows)
    pair_qty = sum(as_float(row.get("source_pair_qty")) for row in rows)
    pair_cost = sum(as_float(row.get("source_pair_cost")) for row in rows)
    pair_pnl = sum(as_float(row.get("source_pair_pnl")) for row in rows)
    residual_qty = sum(as_float(row.get("source_residual_qty")) for row in rows)
    residual_cost = sum(as_float(row.get("source_residual_cost")) for row in rows)
    fee = sum(as_float(row.get("official_fee")) for row in rows)
    return {
        "rows": len(rows),
        "seed_qty": round(seed_qty, 8),
        "seed_cost": round(seed_cost, 8),
        "pair_qty": round(pair_qty, 8),
        "pair_cost": round(pair_cost, 8),
        "pair_pnl": round(pair_pnl, 8),
        "official_fee": round(fee, 8),
        "residual_qty": round(residual_qty, 8),
        "residual_cost": round(residual_cost, 8),
        "weighted_pair_cost": round(safe_ratio(pair_cost, pair_qty), 8),
        "residual_qty_rate": round(safe_ratio(residual_qty, seed_qty), 8),
        "residual_cost_rate": round(safe_ratio(residual_cost, seed_cost), 8),
        "fee_after_pnl_proxy": round(pair_pnl - fee - residual_cost, 8),
    }


def nested_add(counter: Counter[str], prefix: list[str], obj: Any) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            nested_add(counter, [*prefix, str(key)], value)
    elif isinstance(obj, (int, float)):
        counter["|".join(prefix)] += float(obj)


def aggregate_source_link(summary_paths: list[Path]) -> dict[str, Counter[str]]:
    keys = [
        "transition_count_by_status_side_offset_risk_direction",
        "transition_count_by_status_reason",
        "pre_seed_same_qty_bucket_by_status_side_offset_risk_direction",
        "pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction",
        "pre_seed_same_cost_bucket_by_status_side_offset_risk_direction",
        "pre_seed_opp_cost_bucket_by_status_side_offset_risk_direction",
        "candidate_qty_bucket_by_status_side_offset_risk_direction",
        "source_sequence_presence_by_status",
        "source_order_presence_by_status",
        "quote_intent_presence_by_status",
    ]
    out = {key: Counter() for key in keys}
    for path in summary_paths:
        data = read_json(path)
        diag = ((data.get("event_lite") or {}).get("source_link_transition_diagnostics") or {})
        if not isinstance(diag, dict):
            continue
        for key in keys:
            nested_add(out[key], [], diag.get(key) or {})
    return out


def selected_cross_counts(source_link: dict[str, Counter[str]], predicate: dict[str, Any]) -> dict[str, Any]:
    risk = str(predicate.get("source_risk_direction") or "")
    offset = str(predicate.get("offset_bucket") or "")
    transitions = source_link["transition_count_by_status_side_offset_risk_direction"]
    selected_keys = {
        key: value
        for key, value in transitions.items()
        if f"|{offset}|{risk}" in key and (key.startswith("admitted|") or key.startswith("blocked|"))
    }
    admitted = sum(value for key, value in selected_keys.items() if key.startswith("admitted|"))
    blocked = sum(value for key, value in selected_keys.items() if key.startswith("blocked|"))
    same_buckets = {
        key: value
        for key, value in source_link["pre_seed_same_qty_bucket_by_status_side_offset_risk_direction"].items()
        if f"|{offset}|{risk}|" in key
    }
    opp_buckets = {
        key: value
        for key, value in source_link["pre_seed_opp_qty_bucket_by_status_side_offset_risk_direction"].items()
        if f"|{offset}|{risk}|" in key
    }
    candidate_qty_buckets = {
        key: value
        for key, value in source_link["candidate_qty_bucket_by_status_side_offset_risk_direction"].items()
        if f"|{offset}|{risk}|" in key
    }
    has_open_le_1_proxy = any("same_qty_zero" in key for key in same_buckets) and any(
        "opp_qty_zero" in key or "opp_qty_0_1" in key for key in opp_buckets
    )
    has_exact_open_deficit_cross = any(
        "open" in key or "deficit" in key
        for key in source_link["transition_count_by_status_side_offset_risk_direction"].keys()
    )
    return {
        "selected_status_side_offset_risk_direction_counts": dict(sorted(selected_keys.items())),
        "admitted_selected_side_offset_risk_count": round(admitted, 8),
        "blocked_selected_side_offset_risk_count": round(blocked, 8),
        "pre_seed_same_qty_buckets_for_selected_offset_risk": dict(sorted(same_buckets.items())),
        "pre_seed_opp_qty_buckets_for_selected_offset_risk": dict(sorted(opp_buckets.items())),
        "candidate_qty_buckets_for_selected_offset_risk": dict(sorted(candidate_qty_buckets.items())),
        "has_open_le_1_micro_deficit_proxy": has_open_le_1_proxy,
        "has_exact_open_deficit_cross_bucket": has_exact_open_deficit_cross,
    }


def source_opportunity_marker_summary(completion: dict[str, Any]) -> dict[str, Any]:
    marker = ((completion.get("scorer_results") or {}).get("source_opportunity_marker_summary") or {})
    return marker if isinstance(marker, dict) else {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-holdout-manifest", type=Path, default=DEFAULT_TRAIN_HOLDOUT_MANIFEST)
    parser.add_argument("--separator-csv", type=Path, default=DEFAULT_SEPARATOR_CSV)
    parser.add_argument("--no-order-completion-manifest", type=Path, default=DEFAULT_NO_ORDER_COMPLETION)
    parser.add_argument("--no-order-summary-dir", type=Path, default=DEFAULT_NO_ORDER_SUMMARY_DIR)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    train_manifest_path = args.train_holdout_manifest if args.train_holdout_manifest.is_absolute() else root / args.train_holdout_manifest
    separator_csv = args.separator_csv if args.separator_csv.is_absolute() else root / args.separator_csv
    completion_path = (
        args.no_order_completion_manifest
        if args.no_order_completion_manifest.is_absolute()
        else root / args.no_order_completion_manifest
    )
    summary_dir = args.no_order_summary_dir if args.no_order_summary_dir.is_absolute() else root / args.no_order_summary_dir
    output = args.output if args.output.is_absolute() else root / args.output
    summary_paths = sorted(summary_dir.glob("*.summary.json")) if summary_dir.exists() else []
    required = [train_manifest_path, separator_csv, completion_path, *summary_paths]
    unsafe = [str(path) for path in [*required, output] if not path_safe(path)]
    missing = [str(path) for path in [train_manifest_path, separator_csv, completion_path] if not path.exists()]
    if unsafe or missing or not summary_paths:
        manifest = {
            "artifact": ARTIFACT,
            "created_utc": utc_now(),
            "decision": "BLOCKED",
            "decision_label": "BLOCKED_SOURCE_OPPORTUNITY_GAP_INPUT_UNAVAILABLE",
            "unsafe_paths": unsafe,
            "missing": missing,
            "summary_count": len(summary_paths),
            "promotion_gate": {"passed": False, "private_truth_ready": False, "deployable": False},
        }
        write_json(output, manifest)
        print(json.dumps({"decision": manifest["decision"], "output": str(output)}, sort_keys=True))
        return 2

    train_manifest = read_json(train_manifest_path)
    completion = read_json(completion_path)
    rows = load_csv(separator_csv)
    predicate = train_manifest["selection"]["selected_predicate"]
    selected_rows = [row for row in rows if predicate_matches(row, predicate)]
    total = summarize_rows(rows)
    selected = summarize_rows(selected_rows)
    selected_days: dict[str, int] = defaultdict(int)
    for row in selected_rows:
        selected_days[str(row.get("day") or "")] += 1

    source_link = aggregate_source_link(summary_paths)
    selected_cross = selected_cross_counts(source_link, predicate)
    aggregate = completion.get("aggregate_metrics") if isinstance(completion.get("aggregate_metrics"), dict) else {}
    micro = ((completion.get("scorer_results") or {}).get("micro_deficit_summary") or {})
    micro_metrics = micro.get("metrics") if isinstance(micro, dict) and isinstance(micro.get("metrics"), dict) else {}
    marker_summary = source_opportunity_marker_summary(completion)
    marker_total = as_float(marker_summary.get("marker_total"))
    admitted_marker_count = as_float(marker_summary.get("admitted_marker_count"))
    blocked_marker_count = as_float(marker_summary.get("blocked_marker_count"))
    strict_micro_deficit_marker_total = as_float(marker_summary.get("strict_micro_deficit_marker_total"))
    reason_source_available = bool(marker_summary.get("reason_source_coverage_available"))
    reason_source_exact_reason_marker_total = as_float(marker_summary.get("reason_source_exact_reason_marker_total"))
    reason_source_exact_reason_micro_deficit_marker_total = as_float(
        marker_summary.get("reason_source_exact_reason_micro_deficit_marker_total")
    )
    exact_marker_denominator_available = as_float(marker_summary.get("summary_with_source_opportunity_marker_count")) > 0.0

    historical_selected_row_share = safe_ratio(selected["rows"], total["rows"])
    no_order_marker_count = (
        as_float(aggregate.get("micro_deficit_repair_guard_candidates"))
        + as_float(micro_metrics.get("micro_deficit_exemplar_count"))
        + marker_total
        + strict_micro_deficit_marker_total
        + reason_source_exact_reason_marker_total
        + reason_source_exact_reason_micro_deficit_marker_total
    )
    marker_reproduction_ratio = safe_ratio(no_order_marker_count, selected["rows"])
    concrete_gaps = []
    if no_order_marker_count <= 0:
        concrete_gaps.append("no_order_micro_deficit_marker_count_zero")
    if not selected_cross["has_open_le_1_micro_deficit_proxy"]:
        concrete_gaps.append("no_order_selected_offset_risk_has_no_open_le_1_micro_deficit_qty_bucket")
    if not exact_marker_denominator_available and not selected_cross["has_exact_open_deficit_cross_bucket"]:
        concrete_gaps.append("no_order_source_link_lacks_exact_open_qty_and_deficit_cross_denominator")
    elif marker_total <= 0.0 and reason_source_available:
        concrete_gaps.append("source_opportunity_exact_marker_denominator_zero_even_with_reason_source_coverage")
    if selected_cross["blocked_selected_side_offset_risk_count"] > selected_cross["admitted_selected_side_offset_risk_count"]:
        if reason_source_available:
            concrete_gaps.append("selected_offset_risk_more_often_blocked_than_admitted_with_reason_source_available")
        else:
            concrete_gaps.append("selected_offset_risk_more_often_blocked_than_admitted_without_marker_reason_join")
    if reason_source_available:
        top_micro_bucket = str(marker_summary.get("top_micro_deficit_like_bucket") or "")
        if top_micro_bucket and str(predicate.get("offset_bucket") or "") not in top_micro_bucket:
            concrete_gaps.append("closest_micro_deficit_like_bucket_outside_frozen_offset_bucket")
        if top_micro_bucket and as_float(marker_summary.get("blocked_offset_quote_intent_presence_rate"), default=0.0) <= 0.0:
            concrete_gaps.append("closest_micro_deficit_like_contexts_blocked_before_quote_order_creation")
        concrete_gaps.extend(
            [
                "reason_source_denominators_present_but_not_joined_to_local_candidate_row_id",
                "remaining_gap_requires_new_pre_action_signal_family_not_threshold_sweep",
            ]
        )
    else:
        concrete_gaps.extend(
            [
                "blocked_transitions_have_missing_quote_order_sequence_ids",
                "candidate_qty_for_blocked_selected_offset_risk_is_unknown",
                "pending_queue_or_opposite_order_availability_not_exported_for_blocked_candidate_context",
                "activation_age_and_order_room_after_gates_not_joined_to_frozen_predicate",
            ]
        )

    decision = "KEEP"
    decision_label = "KEEP_SOURCE_OPPORTUNITY_GAP_AUDIT_READY_MARKER_REPRODUCTION_GAP_NAMED"
    manifest = {
        "artifact": ARTIFACT,
        "schema_version": "source_opportunity_gap_audit_v1",
        "created_utc": utc_now(),
        "decision": decision,
        "decision_label": decision_label,
        "scope": {
            "local_only": True,
            "read_events_jsonl": False,
            "read_raw_replay_or_collector": False,
            "used_ssh_or_shadow": False,
            "modified_shared_ingress_or_live": False,
            "sent_orders_cancels_redeems": False,
        },
        "inputs": {
            "train_holdout_manifest": str(train_manifest_path),
            "separator_csv": str(separator_csv),
            "no_order_completion_manifest": str(completion_path),
            "no_order_summary_dir": str(summary_dir),
            "no_order_summary_count": len(summary_paths),
            "unsafe_paths": unsafe,
        },
        "frozen_predicate": predicate,
        "historical_separator_denominator": {
            "all_rows": total,
            "selected_rows": selected,
            "selected_row_share": round(historical_selected_row_share, 8),
            "selected_by_day": dict(sorted(selected_days.items())),
            "train_holdout_decision": train_manifest.get("decision_label"),
            "train_comparison": train_manifest.get("selection", {}).get("train", {}).get("comparison"),
            "holdout_comparison": train_manifest.get("selection", {}).get("holdout", {}).get("comparison"),
        },
        "no_order_marker_availability": {
            "completion_decision": completion.get("decision_label"),
            "aggregate_candidates": aggregate.get("candidates"),
            "aggregate_micro_deficit_repair_guard_candidates": aggregate.get("micro_deficit_repair_guard_candidates"),
            "strict_micro_deficit_exemplar_count": micro_metrics.get("micro_deficit_exemplar_count"),
            "source_opportunity_marker_summary_present": bool(marker_summary),
            "source_opportunity_marker_total": marker_total,
            "source_opportunity_admitted_marker_count": admitted_marker_count,
            "source_opportunity_blocked_marker_count": blocked_marker_count,
            "strict_micro_deficit_marker_total": strict_micro_deficit_marker_total,
            "reason_source_coverage_available": reason_source_available,
            "reason_source_exact_reason_marker_total": reason_source_exact_reason_marker_total,
            "reason_source_exact_reason_micro_deficit_marker_total": reason_source_exact_reason_micro_deficit_marker_total,
            "top_micro_deficit_like_bucket": marker_summary.get("top_micro_deficit_like_bucket"),
            "top_micro_deficit_like_bucket_count": marker_summary.get("top_micro_deficit_like_bucket_count"),
            "blocked_offset_quote_intent_presence_rate": marker_summary.get("blocked_offset_quote_intent_presence_rate"),
            "blocked_offset_source_order_presence_rate": marker_summary.get("blocked_offset_source_order_presence_rate"),
            "blocked_offset_source_sequence_presence_rate": marker_summary.get("blocked_offset_source_sequence_presence_rate"),
            "marker_reproduction_ratio_vs_historical_selected_rows": round(marker_reproduction_ratio, 8),
            "source_link_selected_offset_risk": selected_cross,
            "source_link_presence_by_status": {
                "quote_intent": dict(sorted(source_link["quote_intent_presence_by_status"].items())),
                "source_order": dict(sorted(source_link["source_order_presence_by_status"].items())),
                "source_sequence": dict(sorted(source_link["source_sequence_presence_by_status"].items())),
            },
            "top_block_reasons": dict(source_link["transition_count_by_status_reason"].most_common(20)),
        },
        "exact_gap_classification": concrete_gaps,
        "remediation_contract": {
            "runner_summary_fields_needed": (
                [
                    "candidate_row_id/day/condition/ts join from historical separator rows to same-window no-order marker contexts",
                    "admitted and non-tail denominators joined to residual-tail labels without using source_pair/source_residual as live criteria",
                    "new pre-action feature family whose exact marker reproduces in no-order before any new shadow promotion review",
                    "private order/fill/inventory truth only in the later private-truth phase, not in this no-order audit",
                ]
                if reason_source_available
                else [
                    "transition_count_by_status_side_offset_risk_open_bucket_deficit_bucket",
                    "micro_deficit_repair_guard_candidate_count_by_status_reason_side_offset",
                    "blocked_micro_deficit_candidate_count_by_block_reason",
                    "pre_gate and post_gate pre_seed_same_qty/pre_seed_opp_qty/pre_seed_open_qty/deficit exact buckets",
                    "candidate_qty/base_qty/room_cost/imbalance_room for blocked contexts",
                    "activation_opp_age_ms/opposite_seen status for blocked repair contexts",
                    "pending opposite order availability and queue credit/touch opportunity at source event",
                    "quote_intent_id/source_order_id/source_sequence_id coverage for blocked marker contexts",
                ]
            ),
            "local_rule_policy": (
                "Do not enable, reshadow, or sweep the guard while the exact frozen marker has zero no-order denominator. "
                "Reason/source coverage is sufficient to explain this transfer failure when present; the next research "
                "step must find a different pre-action signal family with strict train/holdout plus no-order reproduction."
            ),
        },
        "research_ranking": {
            "decision": decision,
            "label": decision_label,
            "interpretation": (
                "The offline predicate is stable, but the no-order window lacks the micro-deficit/open<=1 tiny-deficit "
                "opportunity surface. This points to a source/opportunity instrumentation gap, not a reason to sweep "
                "thresholds or promote the guard."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "SOURCE_OPPORTUNITY_GAP_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": (
            "Apply the strict no-order reproduction gate to future separator selection and start a new local-only search "
            "for a genuinely different pre-action signal family; do not repeat micro-deficit threshold/cap sweeps."
        ),
    }
    write_json(output, manifest)
    print(json.dumps({"decision": decision, "decision_label": decision_label, "output": str(output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
