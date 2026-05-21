#!/usr/bin/env python3
"""Score D+ source-link transition diagnostics from summary JSON.

This is a local-only reader for event_lite.source_link_transition_diagnostics.
It does not read events JSONL, raw/replay data, sockets, or remote paths.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = (
    "transition_count_by_status",
    "transition_count_by_reason",
    "transition_count_by_status_reason",
    "transition_count_by_side",
    "transition_count_by_offset_bucket",
    "transition_count_by_risk_direction",
    "quote_intent_presence_by_status",
    "source_order_presence_by_status",
    "source_sequence_presence_by_status",
    "pre_seed_same_qty_bucket_by_status_reason",
    "pre_seed_opp_qty_bucket_by_status_reason",
    "pre_seed_same_cost_bucket_by_status_reason",
    "pre_seed_opp_cost_bucket_by_status_reason",
    "ledger_proxy_before_bucket_by_status_reason",
    "ledger_proxy_after_bucket_by_status_reason",
    "candidate_qty_bucket_by_status_reason",
    "immediate_pair_action_count_by_source_side_offset",
    "immediate_pair_qty_bucket_by_source_side_offset",
    "immediate_pair_cost_bucket_by_source_side_offset",
    "immediate_pair_source_quote_presence_by_side_offset",
    "immediate_pair_source_order_presence_by_side_offset",
    "residual_qty_by_source_side_offset",
    "residual_cost_by_source_side_offset",
    "residual_count_by_source_side_offset",
    "residual_cost_bucket_by_source_side_offset",
    "residual_source_quote_presence_by_side_offset",
    "residual_source_order_presence_by_side_offset",
)

FLOAT_TOL = 1e-6


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def source_link_diag(obj: dict[str, Any], path: Path) -> dict[str, Any]:
    event_lite = obj.get("event_lite")
    if not isinstance(event_lite, dict):
        raise ValueError(f"{path} has no event_lite object")
    diag = event_lite.get("source_link_transition_diagnostics")
    if not isinstance(diag, dict):
        raise ValueError(f"{path} has no event_lite.source_link_transition_diagnostics")
    missing = [key for key in REQUIRED_FIELDS if key not in diag]
    if missing:
        raise ValueError(f"{path} is missing source-link fields: {', '.join(missing)}")
    return diag


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


def add_nested(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key, value in src.items():
        if isinstance(value, dict):
            child = dest.setdefault(str(key), {})
            if isinstance(child, dict):
                add_nested(child, value)
            else:
                dest[str(key)] = value
        elif isinstance(value, (int, float)):
            dest[str(key)] = round(float(dest.get(str(key), 0.0)) + float(value), 6)
        else:
            dest[str(key)] = value


def merge_diags(diags: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {key: {} for key in REQUIRED_FIELDS}
    for diag in diags:
        add_nested(merged, diag)
    return merged


def compare_nested(expected: Any, actual: Any, path: str = "") -> list[str]:
    diffs: list[str] = []
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            child_path = f"{path}.{key}" if path else str(key)
            if key not in expected:
                diffs.append(f"{child_path}: unexpected aggregate key")
            elif key not in actual:
                diffs.append(f"{child_path}: missing aggregate key")
            else:
                diffs.extend(compare_nested(expected[key], actual[key], child_path))
        return diffs
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if abs(float(expected) - float(actual)) > FLOAT_TOL:
            diffs.append(f"{path}: expected {expected}, aggregate {actual}")
        return diffs
    if expected != actual:
        diffs.append(f"{path}: expected {expected!r}, aggregate {actual!r}")
    return diffs


def sum_numbers(obj: Any) -> float:
    if isinstance(obj, dict):
        return round(sum(sum_numbers(value) for value in obj.values()), 6)
    if isinstance(obj, (int, float)):
        return float(obj)
    return 0.0


def nested_bucket_proxy(nested: dict[str, Any], key: str, prefix: str) -> float:
    bucket = nested.get(key, {})
    if not isinstance(bucket, dict):
        return 0.0
    total = 0.0
    for label, count in bucket.items():
        if not isinstance(count, (int, float)):
            continue
        total += qty_bucket_midpoint(str(label), prefix) * float(count)
    return round(total, 6)


def qty_bucket_midpoint(label: str, prefix: str) -> float:
    if label == f"{prefix}_zero" or label == f"{prefix}_unknown":
        return 0.0
    if label == f"{prefix}_le_1":
        return 0.5
    if label == f"{prefix}_1_2":
        return 1.5
    if label == f"{prefix}_2_5":
        return 3.5
    if label == f"{prefix}_eq_5":
        return 5.0
    if label == f"{prefix}_gt_5":
        return 6.0
    return 0.0


def top_items(hist: dict[str, Any], limit: int = 8) -> list[dict[str, Any]]:
    rows = [
        {"bucket": str(key), "value": round(float(value), 6)}
        for key, value in hist.items()
        if isinstance(value, (int, float)) and abs(float(value)) > FLOAT_TOL
    ]
    rows.sort(key=lambda row: (-abs(row["value"]), row["bucket"]))
    return rows[:limit]


def top_nested_items(hist: dict[str, Any], limit: int = 8) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, bucket in hist.items():
        if not isinstance(bucket, dict):
            continue
        total = sum_numbers(bucket)
        rows.append({"bucket": str(key), "total": round(total, 6), "detail": bucket})
    rows.sort(key=lambda row: (-abs(row["total"]), row["bucket"]))
    return rows[:limit]


def source_bucket_scores(diag: dict[str, Any]) -> list[dict[str, Any]]:
    pair_actions = diag.get("immediate_pair_action_count_by_source_side_offset", {})
    pair_qty_buckets = diag.get("immediate_pair_qty_bucket_by_source_side_offset", {})
    pair_cost_buckets = diag.get("immediate_pair_cost_bucket_by_source_side_offset", {})
    residual_qty = diag.get("residual_qty_by_source_side_offset", {})
    residual_cost = diag.get("residual_cost_by_source_side_offset", {})
    residual_count = diag.get("residual_count_by_source_side_offset", {})
    keys = sorted(set(pair_actions) | set(pair_qty_buckets) | set(pair_cost_buckets) | set(residual_qty) | set(residual_cost))
    total_pair_actions = sum_numbers(pair_actions)
    total_pair_qty_proxy = sum(nested_bucket_proxy(pair_qty_buckets, key, "pair_qty") for key in keys)
    total_residual_cost = sum_numbers(residual_cost)
    rows: list[dict[str, Any]] = []
    for key in keys:
        pair_action_count = float(pair_actions.get(key, 0.0)) if isinstance(pair_actions.get(key, 0.0), (int, float)) else 0.0
        pair_qty_proxy = nested_bucket_proxy(pair_qty_buckets, key, "pair_qty")
        resid_cost = float(residual_cost.get(key, 0.0)) if isinstance(residual_cost.get(key, 0.0), (int, float)) else 0.0
        resid_qty = float(residual_qty.get(key, 0.0)) if isinstance(residual_qty.get(key, 0.0), (int, float)) else 0.0
        pair_action_share = pair_action_count / total_pair_actions if total_pair_actions > 0 else 0.0
        pair_qty_share = pair_qty_proxy / total_pair_qty_proxy if total_pair_qty_proxy > 0 else 0.0
        residual_cost_share = resid_cost / total_residual_cost if total_residual_cost > 0 else 0.0
        summary_removal_refused = residual_cost_share > 0 and (pair_action_share >= 0.20 or pair_qty_share >= 0.20)
        rows.append(
            {
                "source_side_offset": key,
                "immediate_pair_actions": round(pair_action_count, 6),
                "immediate_pair_action_share": round(pair_action_share, 6),
                "immediate_pair_qty_proxy": round(pair_qty_proxy, 6),
                "immediate_pair_qty_share": round(pair_qty_share, 6),
                "immediate_pair_cost_buckets": pair_cost_buckets.get(key, {}),
                "residual_qty": round(resid_qty, 6),
                "residual_cost": round(resid_cost, 6),
                "residual_cost_share": round(residual_cost_share, 6),
                "residual_count": round(float(residual_count.get(key, 0.0)), 6) if isinstance(residual_count.get(key, 0.0), (int, float)) else 0.0,
                "summary_only_removal_refused": bool(summary_removal_refused),
            }
        )
    rows.sort(key=lambda row: (-row["residual_cost"], -row["immediate_pair_qty_proxy"], row["source_side_offset"]))
    return rows


def ledger_movement(diag: dict[str, Any]) -> dict[str, Any]:
    before = diag.get("ledger_proxy_before_bucket_by_status_reason", {})
    after = diag.get("ledger_proxy_after_bucket_by_status_reason", {})
    keys = sorted(set(before) | set(after))
    out: dict[str, Any] = {}
    for key in keys:
        before_bucket = before.get(key, {}) if isinstance(before.get(key, {}), dict) else {}
        after_bucket = after.get(key, {}) if isinstance(after.get(key, {}), dict) else {}
        out[key] = {
            "before": before_bucket,
            "after": after_bucket,
            "before_negative_or_unknown": round(
                sum(float(before_bucket.get(label, 0.0)) for label in ("ledger_proxy_lt_m1", "ledger_proxy_m1_0", "ledger_proxy_unknown")),
                6,
            ),
            "after_negative_or_unknown": round(
                sum(float(after_bucket.get(label, 0.0)) for label in ("ledger_proxy_lt_m1", "ledger_proxy_m1_0", "ledger_proxy_unknown")),
                6,
            ),
        }
    return out


def score(summary_paths: list[Path], aggregate_path: Path | None) -> dict[str, Any]:
    summary_paths = expand_summary_paths(summary_paths)
    summaries = [load_json(path) for path in summary_paths]
    summary_diags = [source_link_diag(obj, path) for obj, path in zip(summaries, summary_paths)]
    merged = merge_diags(summary_diags)
    aggregate_parity: dict[str, Any]
    if aggregate_path is None:
        aggregate_parity = {"checked": False, "passed": None, "reason": "no aggregate_report.json provided"}
    else:
        aggregate_obj = load_json(aggregate_path)
        aggregate_diag = source_link_diag(aggregate_obj, aggregate_path)
        diffs = compare_nested(merged, aggregate_diag)
        aggregate_parity = {
            "checked": True,
            "passed": not diffs,
            "diff_count": len(diffs),
            "diffs": diffs[:20],
        }

    status_counts = merged.get("transition_count_by_status", {})
    admitted = float(status_counts.get("admitted", 0.0)) if isinstance(status_counts.get("admitted", 0.0), (int, float)) else 0.0
    blocked = float(status_counts.get("blocked", 0.0)) if isinstance(status_counts.get("blocked", 0.0), (int, float)) else 0.0
    total_transitions = admitted + blocked
    admitted_share = admitted / total_transitions if total_transitions > 0 else 0.0
    source_scores = source_bucket_scores(merged)
    summary_only_refusals = [row for row in source_scores if row["summary_only_removal_refused"]]
    top_residual = sorted(
        [row for row in source_scores if row["residual_cost"] > 0 or row["residual_qty"] > 0],
        key=lambda row: (-row["residual_cost"], -row["residual_qty"], row["source_side_offset"]),
    )[:8]
    top_pair = sorted(
        [row for row in source_scores if row["immediate_pair_qty_proxy"] > 0 or row["immediate_pair_actions"] > 0],
        key=lambda row: (-row["immediate_pair_qty_proxy"], -row["immediate_pair_actions"], row["source_side_offset"]),
    )[:8]

    field_checks = {key: key in merged for key in REQUIRED_FIELDS}
    keep_ready = all(field_checks.values()) and (aggregate_parity["passed"] is not False)
    return {
        "status": "KEEP_SOURCE_LINK_TRANSITION_BUCKET_SCORER_READY" if keep_ready else "UNKNOWN_SOURCE_LINK_TRANSITION_BUCKET_SCORER_FIELD_OR_PARITY_GAP",
        "input_summaries": [str(path) for path in summary_paths],
        "input_aggregate": str(aggregate_path) if aggregate_path else None,
        "field_checks": field_checks,
        "aggregate_parity": aggregate_parity,
        "transition_totals": {
            "admitted": round(admitted, 6),
            "blocked": round(blocked, 6),
            "total": round(total_transitions, 6),
            "admitted_transition_share": round(admitted_share, 6),
        },
        "rankings": {
            "blocked_reason_buckets": top_items(merged.get("transition_count_by_status_reason", {}).get("blocked", {}) if isinstance(merged.get("transition_count_by_status_reason", {}), dict) else {}),
            "blocked_offset_buckets": top_items(merged.get("transition_count_by_status_offset_bucket", {}).get("blocked", {}) if isinstance(merged.get("transition_count_by_status_offset_bucket", {}), dict) else {}),
            "risk_direction_by_status": merged.get("transition_count_by_risk_direction", {}),
            "top_immediate_pair_source_buckets": top_pair,
            "top_residual_source_buckets": top_residual,
            "quote_intent_presence": merged.get("quote_intent_presence_by_status", {}),
            "source_order_presence": merged.get("source_order_presence_by_status", {}),
            "source_sequence_presence": merged.get("source_sequence_presence_by_status", {}),
            "ledger_proxy_movement": ledger_movement(merged),
        },
        "guardrails": {
            "future_bucket_rule_min_seed_or_pair_qty_retention": 0.85,
            "future_bucket_rule_max_residual_cost_share": 0.20,
            "future_bucket_rule_min_immediate_pair_qty_share_for_context": 0.10,
            "summary_only_removal_recommended": False,
            "summary_only_removal_refusal_count": len(summary_only_refusals),
            "summary_only_removal_refusal_examples": summary_only_refusals[:5],
            "interpretation": "Use high residual or pair-tail buckets as diagnostic questions only. Do not convert bucket rankings into after-the-fact condition/summary removal unless a separate local verifier proves pre-trade sample retention and economics.",
        },
        "research_ranking": {
            "decision": "KEEP" if keep_ready else "UNKNOWN",
            "label": "KEEP_SOURCE_LINK_TRANSITION_BUCKET_SCORER_READY" if keep_ready else "UNKNOWN_SOURCE_LINK_TRANSITION_BUCKET_SCORER_FIELD_OR_PARITY_GAP",
            "interpretation": "This scorer validates and ranks source-link diagnostic buckets. It is attribution tooling, not strategy economics or promotion evidence.",
        },
        "promotion_gate": {
            "passed": False,
            "status": "SCORER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "can_support_strategy_promotion": False,
            "g2_canary_ready": False,
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", action="append", required=True, help="summary JSON path or directory containing *.summary.json; may repeat")
    ap.add_argument("--aggregate", help="optional aggregate_report.json for parity validation")
    ap.add_argument("--output", required=True, help="score JSON output path")
    args = ap.parse_args(argv)

    try:
        report = score(
            [Path(item) for item in args.summary],
            Path(args.aggregate) if args.aggregate else None,
        )
    except Exception as exc:
        print(f"source-link transition bucket scorer failed: {exc}", file=sys.stderr)
        return 2

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if report["research_ranking"]["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
