#!/usr/bin/env python3
"""Score source-opportunity closed-cycle marker summaries.

This local-only reader consumes already-pulled runner manifest,
aggregate_report.json, and *.summary.json files. It refuses event JSONL,
raw/replay stores, collector paths, sockets, SSH, and live order paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUNNER_SCRIPT = "xuan_dplus_passive_passive_shadow_runner.py"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)

REQUIRED_CLOSED_CYCLE_FIELDS = (
    "schema_version",
    "field_contract",
    "transition_count_by_status_side_offset_risk_open_balance_sides_same_opp",
    "transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "candidate_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "base_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "target_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "room_cost_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "imbalance_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "opposite_seen_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "quote_intent_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "source_order_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    "source_sequence_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
)

MERGE_FIELDS = tuple(field for field in REQUIRED_CLOSED_CYCLE_FIELDS if field not in {"schema_version", "field_contract"})


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


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
    return numerator / denominator if denominator > 0.0 else 0.0


def path_is_safe(path: Path | None) -> bool:
    if path is None:
        return True
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


def marker_diag(obj: dict[str, Any]) -> dict[str, Any]:
    event_lite = obj.get("event_lite")
    if not isinstance(event_lite, dict):
        return {}
    diag = event_lite.get("source_opportunity_marker_summary")
    return diag if isinstance(diag, dict) else {}


def nested_add(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key, value in src.items():
        key_s = str(key)
        if isinstance(value, dict):
            child = dest.setdefault(key_s, {})
            if isinstance(child, dict):
                nested_add(child, value)
            else:
                dest[key_s] = value
        elif isinstance(value, (int, float)):
            dest[key_s] = round(float(dest.get(key_s, 0.0)) + float(value), 8)
        else:
            dest[key_s] = value


def merge_diags(diags: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for diag in diags:
        if diag.get("schema_version"):
            merged["schema_version"] = diag.get("schema_version")
        if diag.get("field_contract"):
            merged["field_contract"] = diag.get("field_contract")
        for field in MERGE_FIELDS:
            value = diag.get(field)
            if isinstance(value, dict):
                nested_add(merged.setdefault(field, {}), value)
    return merged


def compare_nested(expected: Any, actual: Any, path: str = "") -> list[str]:
    diffs: list[str] = []
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            child = f"{path}.{key}" if path else str(key)
            if key not in expected:
                diffs.append(f"{child}: unexpected aggregate key")
            elif key not in actual:
                diffs.append(f"{child}: missing aggregate key")
            else:
                diffs.extend(compare_nested(expected[key], actual[key], child))
        return diffs
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if abs(float(expected) - float(actual)) > 1e-6:
            diffs.append(f"{path}: expected {expected}, aggregate {actual}")
        return diffs
    if expected != actual:
        diffs.append(f"{path}: expected {expected!r}, aggregate {actual!r}")
    return diffs


def sum_numbers(obj: Any) -> float:
    if isinstance(obj, dict):
        return sum(sum_numbers(value) for value in obj.values())
    if isinstance(obj, (int, float)):
        return float(obj)
    return 0.0


def top_items(hist: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    rows = [
        {"bucket": str(key), "value": round(as_float(value), 8)}
        for key, value in hist.items()
        if isinstance(value, (int, float)) and abs(as_float(value)) > 1e-12
    ]
    rows.sort(key=lambda row: (-abs(row["value"]), row["bucket"]))
    return rows[:limit]


def flatten_status_counts(table: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for status, bucket in table.items():
        if not isinstance(bucket, dict):
            continue
        for marker_key, value in bucket.items():
            rows.append(
                {
                    "status": str(status),
                    "marker_key": str(marker_key),
                    "transition_count": round(as_float(value), 8),
                }
            )
    rows.sort(key=lambda row: (-row["transition_count"], row["status"], row["marker_key"]))
    return rows


def marker_key_matches(marker_key: str, args: argparse.Namespace) -> bool:
    parts = marker_key.split("|")
    if len(parts) != 8:
        return False
    side, offset, risk, open_value, balance, preseed_sides, same_qty, opp_qty = parts
    terms = {
        "side": side,
        "offset": offset,
        "risk": risk,
        "open": open_value.replace("open_qty_", "open_"),
        "balance": balance,
        "preseed_sides": preseed_sides,
        "same_qty": same_qty,
        "opp_qty": opp_qty,
    }
    requested = {
        "side": args.side,
        "offset": args.offset_bucket,
        "risk": args.source_risk_direction,
        "open": args.open_bucket,
        "balance": args.balance_bucket,
        "preseed_sides": args.preseed_sides,
        "same_qty": args.same_qty_bucket,
        "opp_qty": args.opp_qty_bucket,
    }
    return all(value == "ANY" or terms.get(field) == value for field, value in requested.items())


def status_reason_marker_matches(status_reason_marker: str, args: argparse.Namespace) -> bool:
    parts = status_reason_marker.split("|")
    if len(parts) != 10:
        return False
    return marker_key_matches("|".join(parts[2:]), args)


def source_presence_rate(diag: dict[str, Any], field: str, status_reason_marker: str) -> float:
    table = diag.get(field, {})
    bucket = table.get(status_reason_marker, {}) if isinstance(table, dict) else {}
    if not isinstance(bucket, dict):
        return 0.0
    present = as_float(bucket.get("present"))
    return safe_ratio(present, sum_numbers(bucket))


def marker_denominator(diag: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    field = "transition_count_by_status_side_offset_risk_open_balance_sides_same_opp"
    by_status = diag.get(field, {})
    if not isinstance(by_status, dict):
        by_status = {}
    totals_by_status: dict[str, float] = {}
    matching_rows: list[dict[str, Any]] = []
    for row in flatten_status_counts(by_status):
        if not marker_key_matches(row["marker_key"], args):
            continue
        status = row["status"]
        count = as_float(row["transition_count"])
        totals_by_status[status] = round(totals_by_status.get(status, 0.0) + count, 8)
        matching_rows.append(row)
    return {
        "selected_predicate": {
            "side": args.side,
            "offset": args.offset_bucket,
            "source_risk_direction": args.source_risk_direction,
            "open": args.open_bucket,
            "balance": args.balance_bucket,
            "preseed_sides": args.preseed_sides,
            "same_qty": args.same_qty_bucket,
            "opp_qty": args.opp_qty_bucket,
        },
        "marker_transition_count_by_status": totals_by_status,
        "marker_total": round(sum(totals_by_status.values()), 8),
        "admitted_marker_count": round(totals_by_status.get("admitted", 0.0), 8),
        "blocked_marker_count": round(totals_by_status.get("blocked", 0.0), 8),
        "matched_marker_rows": matching_rows[:100],
    }


def exact_reason_source_coverage(diag: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    count_field = "transition_count_by_status_reason_side_offset_risk_open_balance_sides_same_opp"
    count_table = diag.get(count_field, {})
    if not isinstance(count_table, dict):
        count_table = {}
    rows: list[dict[str, Any]] = []
    total = 0.0
    for status_reason_marker, value in count_table.items():
        key = str(status_reason_marker)
        if not status_reason_marker_matches(key, args):
            continue
        count = as_float(value)
        total += count
        rows.append(
            {
                "status_reason_marker_key": key,
                "transition_count": round(count, 8),
                "quote_intent_presence_rate": round(
                    source_presence_rate(
                        diag,
                        "quote_intent_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
                        key,
                    ),
                    8,
                ),
                "source_order_presence_rate": round(
                    source_presence_rate(
                        diag,
                        "source_order_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
                        key,
                    ),
                    8,
                ),
                "source_sequence_presence_rate": round(
                    source_presence_rate(
                        diag,
                        "source_sequence_presence_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
                        key,
                    ),
                    8,
                ),
            }
        )
    rows.sort(key=lambda row: (-row["transition_count"], row["status_reason_marker_key"]))
    return {
        "available": bool(count_table),
        "exact_reason_marker_total": round(total, 8),
        "matching_marker_rows": rows,
        "top_reason_marker_buckets": top_items(count_table, limit=10),
    }


def numeric_sums(diag: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    fields = (
        "candidate_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "base_qty_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "target_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "room_cost_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "imbalance_room_sum_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    )
    out: dict[str, Any] = {}
    for field in fields:
        table = diag.get(field, {})
        total = 0.0
        rows: list[dict[str, Any]] = []
        if isinstance(table, dict):
            for key, value in table.items():
                key_s = str(key)
                if not status_reason_marker_matches(key_s, args):
                    continue
                amount = as_float(value)
                total += amount
                rows.append({"status_reason_marker_key": key_s, "value": round(amount, 8)})
        rows.sort(key=lambda row: (-abs(row["value"]), row["status_reason_marker_key"]))
        out[field] = {"total": round(total, 8), "matching_rows": rows[:20]}
    return out


def bucket_context(diag: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    fields = (
        "pending_same_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "pending_opp_qty_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "pending_same_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "pending_opp_order_count_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "opposite_seen_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
        "activation_opp_age_bucket_by_status_reason_side_offset_risk_open_balance_sides_same_opp",
    )
    out: dict[str, Any] = {}
    for field in fields:
        table = diag.get(field, {})
        merged: dict[str, float] = {}
        rows: list[dict[str, Any]] = []
        if isinstance(table, dict):
            for key, bucket in table.items():
                key_s = str(key)
                if not status_reason_marker_matches(key_s, args) or not isinstance(bucket, dict):
                    continue
                rows.append({"status_reason_marker_key": key_s, "bucket": bucket})
                for bucket_key, value in bucket.items():
                    merged[str(bucket_key)] = round(merged.get(str(bucket_key), 0.0) + as_float(value), 8)
        out[field] = {"merged_bucket_counts": merged, "matching_rows": rows[:20]}
    return out


def no_order_safety(runner_manifest: dict[str, Any]) -> dict[str, Any]:
    if not runner_manifest:
        return {"checked": False, "passed": None, "reason": "no runner manifest supplied"}
    safety = runner_manifest.get("safety") if isinstance(runner_manifest.get("safety"), dict) else {}
    checks = {
        "script_ok": runner_manifest.get("script") in (RUNNER_SCRIPT, None),
        "orders_sent_false": safety.get("orders_sent", False) is False and runner_manifest.get("orders_sent", False) is False,
        "cancels_sent_false": runner_manifest.get("cancels_sent", False) is False,
        "redeems_sent_false": runner_manifest.get("redeems_sent", False) is False,
        "started_canary_false": runner_manifest.get("started_canary", False) is False,
    }
    return {"checked": True, "passed": all(checks.values()), "checks": checks}


def risk_budget_from_completion(completion: dict[str, Any]) -> dict[str, Any]:
    if not completion:
        return {"checked": False, "passed": None, "reason": "no completion manifest supplied"}
    aggregate = completion.get("aggregate_metrics") if isinstance(completion.get("aggregate_metrics"), dict) else {}
    candidates = as_float(aggregate.get("candidates"))
    net_pair_cost_p90 = as_float(aggregate.get("net_pair_cost_p90") or aggregate.get("net_pair_cost_proxy_p90"))
    residual_qty_share = as_float(aggregate.get("residual_qty_share_of_filled"))
    residual_cost_share = as_float(aggregate.get("residual_cost_share_of_filled_cost"))
    pair_tail_loss_share = as_float(aggregate.get("pair_tail_loss_share_of_pair_pnl"))
    fee_adjusted_pair_pnl = as_float(aggregate.get("fee_adjusted_pair_pnl_proxy"))
    checks = {
        "candidates_ge_100": candidates >= 100.0,
        "net_pair_cost_p90_le_1": net_pair_cost_p90 <= 1.0,
        "residual_qty_share_le_15pct": residual_qty_share <= 0.15,
        "residual_cost_share_le_20pct": residual_cost_share <= 0.20,
        "pair_tail_loss_share_le_5pct": pair_tail_loss_share <= 0.05,
        "fee_adjusted_pair_pnl_positive": fee_adjusted_pair_pnl > 0.0,
    }
    return {"checked": True, "passed": all(checks.values()), "checks": checks}


def build_score(args: argparse.Namespace) -> dict[str, Any]:
    paths = [args.runner_manifest, args.aggregate_report, args.completion_manifest, args.output_dir] + args.summary
    unsafe = [str(path) for path in paths if not path_is_safe(path)]
    if unsafe:
        raise ValueError(f"refusing forbidden paths: {unsafe}")

    runner_manifest = load_json(args.runner_manifest)
    completion_manifest = load_json(args.completion_manifest)
    aggregate_report = load_json(args.aggregate_report)
    summary_paths = expand_summary_paths(args.summary)
    summaries = [load_json(path) for path in summary_paths]
    summary_diags = [diag for diag in (marker_diag(summary) for summary in summaries) if diag]
    aggregate_diag = marker_diag(aggregate_report)
    merged_diag = merge_diags(summary_diags)

    missing_fields = [field for field in REQUIRED_CLOSED_CYCLE_FIELDS if field not in aggregate_diag and field not in merged_diag]
    aggregate_parity_diffs: list[str] = []
    if aggregate_diag and merged_diag:
        for field in MERGE_FIELDS:
            if field in aggregate_diag or field in merged_diag:
                aggregate_parity_diffs.extend(compare_nested(merged_diag.get(field, {}), aggregate_diag.get(field, {}), field))
        if aggregate_diag.get("schema_version") != merged_diag.get("schema_version"):
            aggregate_parity_diffs.append("schema_version: aggregate and merged summaries differ")

    diag_for_scoring = aggregate_diag if aggregate_diag else merged_diag
    field_contract = diag_for_scoring.get("field_contract", {})
    live_pre_action_fields = field_contract.get("live_pre_action_fields", []) if isinstance(field_contract, dict) else []
    schema_ok = bool(
        isinstance(field_contract, dict)
        and field_contract.get("closed_cycle_marker_schema_version") == "source_opportunity_closed_cycle_marker_v1"
    )
    expected_join_key = (
        "status|reason|side|offset_bucket|source_risk_direction|open_bucket|balance_bucket|"
        "preseed_sides|same_qty_bucket|opp_qty_bucket"
    )
    join_key_ok = bool(isinstance(field_contract, dict) and field_contract.get("closed_cycle_marker_join_key") == expected_join_key)
    post_action_outcomes_excluded = bool(
        isinstance(field_contract, dict)
        and field_contract.get("post_action_outcome_labels_included") is False
    )
    post_action_pair_residual_excluded = bool(
        isinstance(field_contract, dict)
        and field_contract.get("post_action_pair_residual_labels_included") is False
    )
    gross_pair_unavailable = bool(
        isinstance(field_contract, dict)
        and field_contract.get("gross_pair_pnl_available_at_marker") is False
    )
    residual_stress_unavailable = bool(
        isinstance(field_contract, dict)
        and field_contract.get("residual_stress_available_at_marker") is False
    )
    requires_post_run_join = bool(
        isinstance(field_contract, dict)
        and field_contract.get("requires_post_run_scorer_join_for_pnl") is True
    )
    private_truth_false = bool(isinstance(field_contract, dict) and field_contract.get("private_truth_ready") is False)
    deployable_false = bool(isinstance(field_contract, dict) and field_contract.get("deployable") is False)
    promotion_false = bool(isinstance(field_contract, dict) and field_contract.get("promotion_gate_passed") is False)
    closed_cycle_fields_present = all(
        field in live_pre_action_fields
        for field in (
            "closed_cycle_balance_bucket",
            "closed_cycle_preseed_sides",
            "closed_cycle_same_qty_bucket",
            "closed_cycle_opp_qty_bucket",
        )
    )

    denominator = marker_denominator(diag_for_scoring, args)
    reason_source = exact_reason_source_coverage(diag_for_scoring, args)
    numeric_context = numeric_sums(diag_for_scoring, args)
    context_buckets = bucket_context(diag_for_scoring, args)
    safety = no_order_safety(runner_manifest)
    risk_budget = risk_budget_from_completion(completion_manifest)

    blockers: list[str] = []
    if args.runner_manifest is not None and not safety["passed"]:
        blockers.append("no_order_safety_not_confirmed")
    if not summary_diags:
        blockers.append("summary_source_opportunity_marker_missing")
    if not aggregate_diag:
        blockers.append("aggregate_source_opportunity_marker_missing")
    if missing_fields:
        blockers.append("required_source_opportunity_closed_cycle_marker_fields_missing")
    if aggregate_parity_diffs:
        blockers.append("aggregate_parity_failed")
    if not schema_ok:
        blockers.append("closed_cycle_marker_schema_missing")
    if not join_key_ok:
        blockers.append("closed_cycle_marker_join_key_mismatch")
    if not closed_cycle_fields_present:
        blockers.append("closed_cycle_pre_action_fields_not_declared")
    if not (post_action_outcomes_excluded and post_action_pair_residual_excluded):
        blockers.append("field_contract_does_not_exclude_post_action_labels")
    if not (gross_pair_unavailable and residual_stress_unavailable and requires_post_run_join):
        blockers.append("field_contract_allows_marker_time_pnl_or_stress")
    if not (private_truth_false and deployable_false and promotion_false):
        blockers.append("promotion_private_deployable_guards_not_false")
    if denominator["marker_total"] < args.min_marker_count:
        blockers.append("selected_closed_cycle_marker_denominator_below_threshold")
    if reason_source["available"] and reason_source["exact_reason_marker_total"] < args.min_marker_count:
        blockers.append("selected_closed_cycle_reason_marker_denominator_below_threshold")
    if args.require_risk_budget and not risk_budget["passed"]:
        blockers.append("normalized_risk_budget_not_passed")

    if blockers:
        decision = "UNKNOWN"
        label = "UNKNOWN_SOURCE_OPPORTUNITY_CLOSED_CYCLE_MARKER_SUMMARY_INPUTS_INSUFFICIENT"
        next_action = (
            "Do not enable any guard or canary. Use blockers to decide whether closed-cycle denominators "
            "are absent, marker reproduction is thin, or risk budget failed."
        )
    else:
        decision = "KEEP"
        label = "KEEP_SOURCE_OPPORTUNITY_CLOSED_CYCLE_MARKER_DENOMINATOR_SCORER_READY"
        next_action = (
            "Use this scorer after an approved bounded no-order pullback for b55-style closed-cycle "
            "candidate screening. This is not private truth, deployable evidence, canary evidence, or promotion."
        )

    return {
        "artifact": "xuan_source_opportunity_closed_cycle_marker_summary_scorer",
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only_summary_reader": True,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected_or_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "runner_manifest": str(args.runner_manifest) if args.runner_manifest else None,
            "completion_manifest": str(args.completion_manifest) if args.completion_manifest else None,
            "aggregate_report": str(args.aggregate_report),
            "summary_count": len(summary_paths),
            "summary_with_source_opportunity_marker_count": len(summary_diags),
        },
        "contract": {
            "schema_version": diag_for_scoring.get("schema_version"),
            "closed_cycle_marker_schema_version": (
                field_contract.get("closed_cycle_marker_schema_version") if isinstance(field_contract, dict) else None
            ),
            "selected_predicate": denominator["selected_predicate"],
            "gross_pair_pnl_available_at_marker": False,
            "residual_stress_available_at_marker": False,
            "post_run_scorer_join_required_for_pnl": True,
            "source_pair_source_residual_used_as_live_criteria": False,
            "post_action_outcome_labels_included": False,
            "post_action_pair_residual_labels_included": False,
        },
        "no_order_safety": safety,
        "risk_budget": risk_budget,
        "field_checks": {
            "missing_required_fields": missing_fields,
            "schema_ok": schema_ok,
            "join_key_ok": join_key_ok,
            "closed_cycle_pre_action_fields_declared": closed_cycle_fields_present,
            "post_action_outcome_labels_excluded": post_action_outcomes_excluded,
            "post_action_pair_residual_labels_excluded": post_action_pair_residual_excluded,
            "gross_pair_pnl_unavailable_at_marker": gross_pair_unavailable,
            "residual_stress_unavailable_at_marker": residual_stress_unavailable,
            "requires_post_run_scorer_join_for_pnl": requires_post_run_join,
            "private_truth_ready_false": private_truth_false,
            "deployable_false": deployable_false,
            "promotion_gate_passed_false": promotion_false,
        },
        "aggregate_parity": {
            "checked": bool(aggregate_diag and merged_diag),
            "passed": not aggregate_parity_diffs,
            "diff_count": len(aggregate_parity_diffs),
            "diffs": aggregate_parity_diffs[:20],
        },
        "closed_cycle_marker_denominator": denominator,
        "closed_cycle_reason_source_coverage": reason_source,
        "closed_cycle_numeric_context": numeric_context,
        "closed_cycle_bucket_context": context_buckets,
        "thresholds": {
            "min_marker_count": args.min_marker_count,
            "require_risk_budget": args.require_risk_budget,
        },
        "blockers": blockers,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "KEEP means the closed-cycle denominator scorer can verify selected no-order marker "
                "reproduction, source coverage, and field parity. It is not strategy economics, private "
                "truth, deployable evidence, canary evidence, or promotion."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "CLOSED_CYCLE_MARKER_SCORER_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": next_action,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-manifest", type=Path)
    parser.add_argument("--completion-manifest", type=Path)
    parser.add_argument("--aggregate-report", type=Path, required=True)
    parser.add_argument("--summary", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--side", default="ANY")
    parser.add_argument("--source-risk-direction", default="ANY")
    parser.add_argument("--offset-bucket", default="ANY")
    parser.add_argument("--open-bucket", default="ANY")
    parser.add_argument("--balance-bucket", default="deficit_0_25_1_25")
    parser.add_argument("--preseed-sides", default="ANY")
    parser.add_argument("--same-qty-bucket", default="ANY")
    parser.add_argument("--opp-qty-bucket", default="opp_le_5")
    parser.add_argument("--min-marker-count", type=float, default=1.0)
    parser.add_argument("--require-risk-budget", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_score(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["decision"] == "KEEP" else 3


if __name__ == "__main__":
    raise SystemExit(main())
