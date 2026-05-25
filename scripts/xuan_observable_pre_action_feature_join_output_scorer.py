#!/usr/bin/env python3
"""Validate observable pre-action feature-join materialized outputs.

This scorer only reads the allowlisted feature-join JSON/JSONL outputs produced
by the default-off runner instrumentation. It does not read events JSONL or any
raw/replay store, and it does not score strategy profitability.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_feature_join_output_scorer"
CONTRACT_NAME = "observable_pre_action_feature_join_output_scorer_v1"
ROW_SCHEMA = "observable_pre_action_candidate_row_v1"
SOURCE_SCHEMA = "observable_pre_action_source_link_summary_v1"
MANIFEST_SCHEMA = "observable_pre_action_feature_join_manifest_v1"

REQUIRED_ROW_FIELDS = (
    "schema_version",
    "condition_id",
    "market_slug",
    "day_id",
    "quote_ts_ms",
    "side",
    "offset_s",
    "offset_bucket",
    "status_before_action",
    "reason",
    "block_reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "pre_seed_deficit_qty",
    "candidate_qty",
    "pre_seed_same_qty_bucket",
    "pre_seed_opp_qty_bucket",
    "pre_seed_open_qty_bucket",
    "pre_seed_deficit_qty_bucket",
    "candidate_qty_bucket",
    "source_risk_direction",
)

FALSE_FIELD_CONTRACT_KEYS = (
    "writes_events_jsonl",
    "reads_events_jsonl",
    "raw_replay_or_full_store_scan",
    "post_action_outcome_labels_included",
    "realized_pair_cost_used_as_live_criteria",
    "trading_behavior_changed",
    "strategy_evidence",
    "private_truth_ready",
    "deployable",
    "promotion_gate_passed",
)

FORBIDDEN_ROW_KEY_FRAGMENTS = (
    "source_pair",
    "source_residual",
    "settlement",
    "redeem",
)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line_no, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError(f"{path}:{line_no} is not a JSON object")
        rows.append(row)
    return rows


def add_count(hist: dict[str, float], key: str, value: float = 1.0) -> None:
    hist[key] = round(float(hist.get(key, 0.0)) + float(value), 6)


def add_nested_count(hist: dict[str, dict[str, float]], key: str, subkey: str, value: float = 1.0) -> None:
    bucket = hist.setdefault(key, {})
    add_count(bucket, subkey, value)


def row_presence_key(row: dict[str, Any], field: str) -> str:
    return "present" if row.get(field) else "missing"


def row_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, Any] = {
        "row_count_by_status": {},
        "row_count_by_status_reason": {},
        "source_sequence_presence_by_status": {},
        "quote_intent_presence_by_status": {},
        "source_order_presence_by_status": {},
        "candidate_qty_bucket_by_status_reason": {},
        "pre_seed_open_qty_bucket_by_status_reason": {},
        "pre_seed_deficit_qty_bucket_by_status_reason": {},
        "source_risk_direction_by_status_reason": {},
    }
    for row in rows:
        status = str(row.get("status_before_action") or "unknown")
        reason = str(row.get("reason") or "unknown")
        status_reason = f"{status}|{reason}"
        add_count(counts["row_count_by_status"], status)
        add_nested_count(counts["row_count_by_status_reason"], status, reason)
        add_nested_count(
            counts["source_sequence_presence_by_status"],
            status,
            row_presence_key(row, "source_sequence_present"),
        )
        add_nested_count(
            counts["quote_intent_presence_by_status"],
            status,
            row_presence_key(row, "quote_intent_present"),
        )
        add_nested_count(
            counts["source_order_presence_by_status"],
            status,
            row_presence_key(row, "source_order_present"),
        )
        add_nested_count(
            counts["candidate_qty_bucket_by_status_reason"],
            status_reason,
            str(row.get("candidate_qty_bucket") or "candidate_qty_unknown"),
        )
        add_nested_count(
            counts["pre_seed_open_qty_bucket_by_status_reason"],
            status_reason,
            str(row.get("pre_seed_open_qty_bucket") or "open_qty_unknown"),
        )
        add_nested_count(
            counts["pre_seed_deficit_qty_bucket_by_status_reason"],
            status_reason,
            str(row.get("pre_seed_deficit_qty_bucket") or "deficit_unknown"),
        )
        add_nested_count(
            counts["source_risk_direction_by_status_reason"],
            status_reason,
            str(row.get("source_risk_direction") or "unknown"),
        )
    return counts


def field_contract_blockers(field_contract: dict[str, Any], prefix: str) -> list[str]:
    blockers = []
    if field_contract.get("default_off") is not True:
        blockers.append(f"{prefix}_default_off_not_true")
    for key in FALSE_FIELD_CONTRACT_KEYS:
        if field_contract.get(key) is not False:
            blockers.append(f"{prefix}_{key}_not_false")
    return blockers


def compare_nested(actual: Any, expected: Any, key: str) -> list[str]:
    return [] if actual == expected else [f"{key}_mismatch"]


def score(candidate_rows_path: Path, source_link_summary_path: Path, feature_join_manifest_path: Path) -> dict[str, Any]:
    blockers: list[str] = []
    for label, path in (
        ("candidate_rows", candidate_rows_path),
        ("source_link_summary", source_link_summary_path),
        ("feature_join_manifest", feature_join_manifest_path),
    ):
        if not path.exists():
            blockers.append(f"{label}_missing")
    if blockers:
        return build_manifest(blockers, {}, {}, [], candidate_rows_path, source_link_summary_path, feature_join_manifest_path)

    rows = load_rows(candidate_rows_path)
    source_summary = load_json(source_link_summary_path)
    feature_manifest = load_json(feature_join_manifest_path)

    if not rows:
        blockers.append("candidate_rows_empty")
    if source_summary.get("schema_version") != SOURCE_SCHEMA:
        blockers.append("source_summary_schema_mismatch")
    if feature_manifest.get("schema_version") != MANIFEST_SCHEMA:
        blockers.append("feature_manifest_schema_mismatch")
    if int(source_summary.get("row_count") or 0) != len(rows):
        blockers.append("source_summary_row_count_mismatch")
    if int(feature_manifest.get("candidate_row_count") or 0) != len(rows):
        blockers.append("feature_manifest_candidate_row_count_mismatch")

    status_values = {str(row.get("status_before_action") or "") for row in rows}
    if "admitted" not in status_values:
        blockers.append("admitted_status_missing")
    if "blocked" not in status_values:
        blockers.append("blocked_status_missing")

    for idx, row in enumerate(rows):
        if row.get("schema_version") != ROW_SCHEMA:
            blockers.append(f"row_{idx}_schema_mismatch")
        missing = [field for field in REQUIRED_ROW_FIELDS if field not in row]
        if missing:
            blockers.append(f"row_{idx}_missing_required_fields:{','.join(missing)}")
        if row.get("post_action_outcome_labels_included") is not False:
            blockers.append(f"row_{idx}_post_action_outcome_labels_not_false")
        if row.get("realized_pair_cost_used_as_live_criteria") is not False:
            blockers.append(f"row_{idx}_realized_pair_cost_live_criteria_not_false")
        if row.get("trading_behavior_changed") is not False:
            blockers.append(f"row_{idx}_trading_behavior_changed_not_false")
        for key in row:
            lower = key.lower()
            if any(fragment in lower for fragment in FORBIDDEN_ROW_KEY_FRAGMENTS):
                blockers.append(f"row_{idx}_forbidden_key:{key}")

    expected = row_counts(rows)
    for key, value in expected.items():
        blockers.extend(compare_nested(source_summary.get(key), value, f"source_summary_{key}"))

    blockers.extend(field_contract_blockers(source_summary.get("field_contract") or {}, "source_summary_field_contract"))
    blockers.extend(field_contract_blockers(feature_manifest.get("field_contract") or {}, "feature_manifest_field_contract"))

    return build_manifest(
        blockers,
        source_summary,
        feature_manifest,
        rows,
        candidate_rows_path,
        source_link_summary_path,
        feature_join_manifest_path,
    )


def build_manifest(
    blockers: list[str],
    source_summary: dict[str, Any],
    feature_manifest: dict[str, Any],
    rows: list[dict[str, Any]],
    candidate_rows_path: Path,
    source_link_summary_path: Path,
    feature_join_manifest_path: Path,
) -> dict[str, Any]:
    ready = not blockers
    status_values = sorted({str(row.get("status_before_action") or "unknown") for row in rows})
    source_seq_counts = (source_summary.get("source_sequence_presence_by_status") or {}) if source_summary else {}
    return {
        "artifact": ARTIFACT,
        "contract_name": CONTRACT_NAME,
        "decision": "KEEP" if ready else "UNKNOWN",
        "decision_label": (
            "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY"
            if ready
            else "UNKNOWN_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_FAIL_CLOSED"
        ),
        "blockers": blockers,
        "inputs": {
            "candidate_rows_path": str(candidate_rows_path),
            "source_link_summary_path": str(source_link_summary_path),
            "feature_join_manifest_path": str(feature_join_manifest_path),
        },
        "score": {
            "row_count": len(rows),
            "status_values": status_values,
            "source_sequence_presence_by_status": source_seq_counts,
            "manifest_candidate_row_count": feature_manifest.get("candidate_row_count") if feature_manifest else None,
            "source_summary_row_count": source_summary.get("row_count") if source_summary else None,
        },
        "research_ranking": "SCORER_READY_NO_STRATEGY_EVIDENCE" if ready else "INPUT_CONTRACT_FAIL_CLOSED",
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "local_feature_join_output_scorer_only",
        },
        "safety": {
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scan": False,
        },
        "next_executable_action": (
            "Implement observable_pre_action_rule_miner_denominator_replay_scorer_spec_v1 local-only: "
            "define how a frozen predicate consumes feature-join rows and same-window summaries to produce "
            "observable_pre_action_no_order_denominator_summary.json and an input manifest, fail-closed before "
            "any no-order diagnostic."
            if ready
            else "Fix the allowlisted feature-join output contract or wait for a complete current-worktree output."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-rows", type=Path, required=True)
    parser.add_argument("--source-link-summary", type=Path, required=True)
    parser.add_argument("--feature-join-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = score(args.candidate_rows, args.source_link_summary, args.feature_join_manifest)
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
