#!/usr/bin/env python3
"""Score source-opportunity marker summaries from D+ no-order artifacts.

This is a local-only reader for already-pulled runner manifest,
aggregate_report.json, and *.summary.json files. It does not read events
JSONL, raw/replay stores, sockets, SSH, or live order paths.
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
)

REQUIRED_FIELDS = (
    "schema_version",
    "transition_count_by_status",
    "transition_count_by_reason",
    "transition_count_by_status_reason",
    "transition_count_by_side_offset_risk_open_deficit",
    "transition_count_by_status_side_offset_risk_open_deficit",
    "micro_deficit_marker_count_by_status",
    "micro_deficit_marker_count_by_status_reason",
    "micro_deficit_marker_count_by_status_side_offset_risk_open_deficit",
    "candidate_qty_sum_by_status_reason",
    "base_qty_sum_by_status_reason",
    "target_room_sum_by_status_reason",
    "room_cost_sum_by_status_reason",
    "imbalance_room_sum_by_status_reason",
    "candidate_qty_bucket_by_status_reason",
    "base_qty_bucket_by_status_reason",
    "target_room_bucket_by_status_reason",
    "room_cost_bucket_by_status_reason",
    "imbalance_room_bucket_by_status_reason",
    "pending_same_qty_bucket_by_status_reason",
    "pending_opp_qty_bucket_by_status_reason",
    "pending_same_order_count_bucket_by_status_reason",
    "pending_opp_order_count_bucket_by_status_reason",
    "opposite_seen_by_status_reason",
    "activation_opp_age_bucket_by_status_reason",
    "quote_intent_presence_by_status_reason",
    "source_order_presence_by_status_reason",
    "source_sequence_presence_by_status_reason",
    "field_contract",
)

MERGE_COUNT_FIELDS = (
    "transition_count_by_status",
    "transition_count_by_reason",
    "transition_count_by_side_offset_risk_open_deficit",
    "micro_deficit_marker_count_by_status",
    "candidate_qty_sum_by_status_reason",
    "base_qty_sum_by_status_reason",
    "target_room_sum_by_status_reason",
    "room_cost_sum_by_status_reason",
    "imbalance_room_sum_by_status_reason",
)

MERGE_NESTED_FIELDS = (
    "transition_count_by_status_reason",
    "transition_count_by_status_side_offset_risk_open_deficit",
    "micro_deficit_marker_count_by_status_reason",
    "micro_deficit_marker_count_by_status_side_offset_risk_open_deficit",
    "candidate_qty_bucket_by_status_reason",
    "base_qty_bucket_by_status_reason",
    "target_room_bucket_by_status_reason",
    "room_cost_bucket_by_status_reason",
    "imbalance_room_bucket_by_status_reason",
    "pending_same_qty_bucket_by_status_reason",
    "pending_opp_qty_bucket_by_status_reason",
    "pending_same_order_count_bucket_by_status_reason",
    "pending_opp_order_count_bucket_by_status_reason",
    "opposite_seen_by_status_reason",
    "activation_opp_age_bucket_by_status_reason",
    "quote_intent_presence_by_status_reason",
    "source_order_presence_by_status_reason",
    "source_sequence_presence_by_status_reason",
)

DEFAULT_MARKER_SUFFIX = "offset_90_120|repair_or_pairing_improving|open_qty_le_1|deficit_0_0_25"


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
    if denominator <= 0.0:
        return 0.0
    return numerator / denominator


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
        for field in MERGE_COUNT_FIELDS:
            value = diag.get(field)
            if isinstance(value, dict):
                nested_add(merged.setdefault(field, {}), value)
        for field in MERGE_NESTED_FIELDS:
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


def hist_value(hist: dict[str, Any], key: str) -> float:
    value = hist.get(key)
    return as_float(value) if isinstance(value, (int, float, str)) else 0.0


def source_presence_rate(diag: dict[str, Any], field: str, status_reason: str) -> float:
    table = diag.get(field, {})
    bucket = table.get(status_reason, {}) if isinstance(table, dict) else {}
    if not isinstance(bucket, dict):
        return 0.0
    present = as_float(bucket.get("present"))
    return safe_ratio(present, sum_numbers(bucket))


def top_items(hist: dict[str, Any], limit: int = 8) -> list[dict[str, Any]]:
    rows = [
        {"bucket": str(key), "value": round(as_float(value), 8)}
        for key, value in hist.items()
        if isinstance(value, (int, float)) and abs(as_float(value)) > 1e-12
    ]
    rows.sort(key=lambda row: (-abs(row["value"]), row["bucket"]))
    return rows[:limit]


def top_nested(hist: dict[str, Any], limit: int = 8) -> list[dict[str, Any]]:
    rows = []
    for key, value in hist.items():
        if isinstance(value, dict):
            rows.append({"bucket": str(key), "total": round(sum_numbers(value), 8), "detail": value})
    rows.sort(key=lambda row: (-row["total"], row["bucket"]))
    return rows[:limit]


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


def marker_keys(marker_suffix: str) -> list[str]:
    return [f"YES|{marker_suffix}", f"NO|{marker_suffix}"]


def marker_denominator(diag: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    by_status = diag.get("transition_count_by_status_side_offset_risk_open_deficit", {})
    micro_by_status = diag.get("micro_deficit_marker_count_by_status_side_offset_risk_open_deficit", {})
    status_reason = diag.get("micro_deficit_marker_count_by_status_reason", {})
    totals_by_status: dict[str, float] = {}
    micro_totals_by_status: dict[str, float] = {}
    key_rows: list[dict[str, Any]] = []
    for status, bucket in by_status.items() if isinstance(by_status, dict) else []:
        if not isinstance(bucket, dict):
            continue
        for key in keys:
            count = hist_value(bucket, key)
            totals_by_status[status] = round(totals_by_status.get(status, 0.0) + count, 8)
            key_rows.append({"status": status, "marker_key": key, "transition_count": round(count, 8)})
    for status, bucket in micro_by_status.items() if isinstance(micro_by_status, dict) else []:
        if not isinstance(bucket, dict):
            continue
        for key in keys:
            count = hist_value(bucket, key)
            micro_totals_by_status[status] = round(micro_totals_by_status.get(status, 0.0) + count, 8)
    marker_status_reasons: dict[str, dict[str, float]] = {}
    if isinstance(status_reason, dict):
        for status, reasons in status_reason.items():
            if isinstance(reasons, dict):
                marker_status_reasons[str(status)] = {
                    str(reason): round(as_float(value), 8)
                    for reason, value in reasons.items()
                    if isinstance(value, (int, float)) and as_float(value) > 0.0
                }
    return {
        "marker_transition_count_by_status": totals_by_status,
        "strict_micro_deficit_marker_count_by_status": micro_totals_by_status,
        "strict_micro_deficit_marker_count_by_status_reason": marker_status_reasons,
        "marker_key_rows": key_rows,
        "marker_total": round(sum(totals_by_status.values()), 8),
        "strict_micro_deficit_marker_total": round(sum(micro_totals_by_status.values()), 8),
        "admitted_marker_count": round(totals_by_status.get("admitted", 0.0), 8),
        "blocked_marker_count": round(totals_by_status.get("blocked", 0.0), 8),
    }


def status_reason_details(diag: dict[str, Any], status_reasons: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    detail_fields = (
        "candidate_qty_sum_by_status_reason",
        "base_qty_sum_by_status_reason",
        "target_room_sum_by_status_reason",
        "room_cost_sum_by_status_reason",
        "imbalance_room_sum_by_status_reason",
        "candidate_qty_bucket_by_status_reason",
        "base_qty_bucket_by_status_reason",
        "target_room_bucket_by_status_reason",
        "room_cost_bucket_by_status_reason",
        "imbalance_room_bucket_by_status_reason",
        "pending_same_qty_bucket_by_status_reason",
        "pending_opp_qty_bucket_by_status_reason",
        "pending_same_order_count_bucket_by_status_reason",
        "pending_opp_order_count_bucket_by_status_reason",
        "opposite_seen_by_status_reason",
        "activation_opp_age_bucket_by_status_reason",
        "quote_intent_presence_by_status_reason",
        "source_order_presence_by_status_reason",
        "source_sequence_presence_by_status_reason",
    )
    for status_reason in status_reasons:
        row: dict[str, Any] = {}
        for field in detail_fields:
            table = diag.get(field, {})
            if isinstance(table, dict) and status_reason in table:
                row[field] = table[status_reason]
        row["quote_intent_presence_rate"] = round(source_presence_rate(diag, "quote_intent_presence_by_status_reason", status_reason), 8)
        row["source_order_presence_rate"] = round(source_presence_rate(diag, "source_order_presence_by_status_reason", status_reason), 8)
        row["source_sequence_presence_rate"] = round(source_presence_rate(diag, "source_sequence_presence_by_status_reason", status_reason), 8)
        out[status_reason] = row
    return out


def build_score(args: argparse.Namespace) -> dict[str, Any]:
    paths = [args.runner_manifest, args.aggregate_report, args.output_dir] + args.summary
    unsafe = [str(path) for path in paths if not path_is_safe(path)]
    if unsafe:
        raise ValueError(f"refusing forbidden paths: {unsafe}")

    runner_manifest = load_json(args.runner_manifest)
    aggregate_report = load_json(args.aggregate_report)
    summary_paths = expand_summary_paths(args.summary)
    summaries = [load_json(path) for path in summary_paths]
    summary_diags = [marker_diag(summary) for summary in summaries]
    summary_diags = [diag for diag in summary_diags if diag]
    aggregate_diag = marker_diag(aggregate_report)
    merged_diag = merge_diags(summary_diags)

    missing_fields = [field for field in REQUIRED_FIELDS if field not in aggregate_diag and field not in merged_diag]
    aggregate_parity_diffs: list[str] = []
    if aggregate_diag and merged_diag:
        for field in MERGE_COUNT_FIELDS + MERGE_NESTED_FIELDS:
            if field in aggregate_diag or field in merged_diag:
                aggregate_parity_diffs.extend(compare_nested(merged_diag.get(field, {}), aggregate_diag.get(field, {}), field))
        if aggregate_diag.get("schema_version") != merged_diag.get("schema_version"):
            aggregate_parity_diffs.append("schema_version: aggregate and merged summaries differ")

    diag_for_scoring = aggregate_diag if aggregate_diag else merged_diag
    keys = marker_keys(args.marker_suffix)
    marker = marker_denominator(diag_for_scoring, keys)
    total_transitions = sum_numbers(diag_for_scoring.get("transition_count_by_status", {}))
    status_reason_counts = diag_for_scoring.get("transition_count_by_status_reason", {})
    micro_status_reasons: list[str] = []
    for status, reasons in marker["strict_micro_deficit_marker_count_by_status_reason"].items():
        if isinstance(reasons, dict):
            micro_status_reasons.extend(f"{status}|{reason}" for reason in reasons)
    micro_status_reasons = sorted(set(micro_status_reasons))
    detail_by_status_reason = status_reason_details(diag_for_scoring, micro_status_reasons)

    field_contract = diag_for_scoring.get("field_contract", {})
    post_action_labels_excluded = bool(
        isinstance(field_contract, dict)
        and field_contract.get("post_action_outcome_labels_included") is False
    )
    private_truth_false = bool(isinstance(field_contract, dict) and field_contract.get("private_truth_ready") is False)
    deployable_false = bool(isinstance(field_contract, dict) and field_contract.get("deployable") is False)
    promotion_false = bool(isinstance(field_contract, dict) and field_contract.get("promotion_gate_passed") is False)

    blockers: list[str] = []
    if args.runner_manifest is not None and not no_order_safety(runner_manifest)["passed"]:
        blockers.append("no_order_safety_not_confirmed")
    if not summary_diags:
        blockers.append("summary_source_opportunity_marker_missing")
    if not aggregate_diag:
        blockers.append("aggregate_source_opportunity_marker_missing")
    if missing_fields:
        blockers.append("required_source_opportunity_marker_fields_missing")
    if aggregate_parity_diffs:
        blockers.append("aggregate_parity_failed")
    if not post_action_labels_excluded:
        blockers.append("field_contract_does_not_exclude_post_action_labels")
    if not (private_truth_false and deployable_false and promotion_false):
        blockers.append("promotion_private_deployable_guards_not_false")
    if marker["marker_total"] < args.min_marker_count:
        blockers.append("exact_frozen_marker_denominator_below_threshold")
    if marker["strict_micro_deficit_marker_total"] < args.min_marker_count:
        blockers.append("strict_micro_deficit_marker_count_below_threshold")

    status_reason_source_sequence_rates = {
        key: value.get("source_sequence_presence_rate", 0.0)
        for key, value in detail_by_status_reason.items()
        if isinstance(value, dict)
    }
    if status_reason_source_sequence_rates and min(status_reason_source_sequence_rates.values()) < args.min_source_sequence_status_reason_coverage:
        blockers.append("source_sequence_status_reason_coverage_below_threshold")

    reason_join_limitation = (
        "The runner summary exposes exact marker counts by status+side+offset+risk+open+deficit "
        "and micro-deficit counts by status+reason, but not exact marker-by-reason-by-source-id. "
        "This scorer uses status_reason-level coverage as diagnostic context, not private/order truth."
    )

    if blockers:
        decision = "UNKNOWN"
        label = "UNKNOWN_SOURCE_OPPORTUNITY_MARKER_SUMMARY_INPUTS_INSUFFICIENT"
        next_action = (
            "Do not start remote/canary from this scorer output. If this came from an approved no-order run, "
            "use the named blockers to decide whether marker-denominator instrumentation is sufficient or "
            "whether a marker-by-reason/source join field is still missing."
        )
    else:
        decision = "KEEP"
        label = "KEEP_SOURCE_OPPORTUNITY_MARKER_DENOMINATOR_SCORER_READY"
        next_action = (
            "Use this scorer after an approved source-opportunity marker no-order pullback to judge whether "
            "the frozen historical marker reproduced in same-window admitted/blocked denominators. This is "
            "not private truth, deployable evidence, canary evidence, or promotion."
        )

    return {
        "artifact": "xuan_b27_dplus_source_opportunity_marker_summary_scorer",
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
            "aggregate_report": str(args.aggregate_report),
            "summary_count": len(summary_paths),
            "summary_with_source_opportunity_marker_count": len(summary_diags),
        },
        "contract": {
            "schema_version": diag_for_scoring.get("schema_version"),
            "frozen_predicate_suffix": args.marker_suffix,
            "frozen_marker_keys": keys,
            "post_action_outcome_labels_included": False,
            "source_pair_source_residual_used_as_live_criteria": False,
            "reason_join_limitation": reason_join_limitation,
        },
        "no_order_safety": no_order_safety(runner_manifest),
        "field_checks": {
            "missing_required_fields": missing_fields,
            "post_action_labels_excluded": post_action_labels_excluded,
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
        "transition_totals": {
            "total": round(total_transitions, 8),
            "by_status": diag_for_scoring.get("transition_count_by_status", {}),
            "by_status_reason": status_reason_counts,
            "marker_share_of_total_transitions": round(safe_ratio(marker["marker_total"], total_transitions), 8),
        },
        "marker_denominator": marker,
        "marker_status_reason_context": detail_by_status_reason,
        "rankings": {
            "top_status_reasons": top_nested(status_reason_counts if isinstance(status_reason_counts, dict) else {}),
            "top_exact_marker_buckets": top_items(diag_for_scoring.get("transition_count_by_side_offset_risk_open_deficit", {}) if isinstance(diag_for_scoring.get("transition_count_by_side_offset_risk_open_deficit"), dict) else {}),
            "top_micro_deficit_status_reasons": top_nested(diag_for_scoring.get("micro_deficit_marker_count_by_status_reason", {}) if isinstance(diag_for_scoring.get("micro_deficit_marker_count_by_status_reason"), dict) else {}),
        },
        "thresholds": {
            "min_marker_count": args.min_marker_count,
            "min_source_sequence_status_reason_coverage": args.min_source_sequence_status_reason_coverage,
        },
        "blockers": blockers,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "KEEP means source-opportunity marker summary surfaces are sufficient to review same-window "
                "marker reproduction. It is denominator instrumentation, not strategy economics, private truth, "
                "deployable evidence, canary evidence, or promotion."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "SUMMARY_SCORER_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": next_action,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-manifest", type=Path)
    parser.add_argument("--aggregate-report", type=Path, required=True)
    parser.add_argument("--summary", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--marker-suffix", default=DEFAULT_MARKER_SUFFIX)
    parser.add_argument("--min-marker-count", type=float, default=1.0)
    parser.add_argument("--min-source-sequence-status-reason-coverage", type=float, default=0.95)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_score(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["decision"] == "KEEP" else 3


if __name__ == "__main__":
    raise SystemExit(main())
