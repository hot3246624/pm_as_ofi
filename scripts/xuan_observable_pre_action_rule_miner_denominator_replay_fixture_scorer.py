#!/usr/bin/env python3
"""Replay a frozen observable pre-action denominator from fixture/materialized rows.

This local-only scorer consumes allowlisted feature-join style rows plus a
frozen pre-action predicate and same-window aggregate summary. It writes a
denominator summary and an observable_pre_action_rule_miner_input_v1 manifest.
It does not read events JSONL, raw/replay stores, or change runner behavior.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer"
CONTRACT_NAME = "observable_pre_action_rule_miner_denominator_replay_fixture_scorer_v1"
INPUT_SCHEMA = "observable_pre_action_rule_miner_input_v1"
DENOMINATOR_SCHEMA = "observable_pre_action_no_order_denominator_summary_v1"

REQUIRED_PRE_ACTION_FIELDS = (
    "condition_id",
    "day_id",
    "quote_ts_ms",
    "side",
    "offset_s",
    "status_before_action",
    "block_reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "open_qty_bucket",
    "deficit_bucket",
    "candidate_qty_bucket",
    "source_risk_direction",
)

OFFLINE_LABEL_FIELDS = (
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)

REQUIRED_NO_ORDER_FIELDS = (
    "frozen_rule_id",
    "same_window_denominator_count",
    "admitted_count",
    "blocked_count",
    "source_sequence_coverage",
    "quote_intent_presence_rate",
    "source_order_presence_rate",
    "aggregate_parity",
)

ALLOWED_LIVE_FEATURE_FAMILIES = {
    "status_before_action",
    "block_reason",
    "reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "side",
    "offset_bucket",
    "offset_s",
    "pre_seed_same_qty_bucket",
    "pre_seed_opp_qty_bucket",
    "pre_seed_open_qty_bucket",
    "pre_seed_deficit_qty_bucket",
    "open_qty_bucket",
    "deficit_bucket",
    "candidate_qty_bucket",
    "source_risk_direction",
}

FORBIDDEN_LIVE_FEATURE_FAMILIES = {
    "source_pair",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "realized_pair_cost",
    "settlement",
    "redeem",
    "future_label",
    "private_truth",
    "public_profile_single_day_profit",
    "static_side_offset_price_cap",
    "dplus_failed_family_marker",
}

FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/replay",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    if any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS):
        return False, "forbidden_path_fragment"
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False, "outside_current_worktree"
    return True, None


def as_bool(value: Any) -> bool:
    return bool(value)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def add_count(hist: dict[str, float], key: str, value: float = 1.0) -> None:
    hist[key] = round(float(hist.get(key, 0.0)) + float(value), 6)


def presence_rate(rows: list[dict[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    return round(sum(1 for row in rows if row.get(field) is True) / len(rows), 6)


def field_contract() -> dict[str, Any]:
    return {
        "schema_version": "observable_pre_action_denominator_replay_field_contract_v1",
        "fixture_only": True,
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "events_jsonl_read": False,
        "events_jsonl_pulled": False,
        "raw_replay_or_full_store_scan": False,
        "trading_behavior_changed": False,
        "strategy_evidence": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate_passed": False,
    }


def condition_matches(row: dict[str, Any], condition: dict[str, Any]) -> bool:
    field = str(condition.get("field") or "")
    op = str(condition.get("op") or "eq")
    expected = condition.get("value")
    actual = row.get(field)
    if op == "eq":
        return actual == expected
    if op == "ne":
        return actual != expected
    if op == "in":
        return actual in (expected if isinstance(expected, list) else [])
    if op == "not_in":
        return actual not in (expected if isinstance(expected, list) else [])
    if op in {"lt", "le", "gt", "ge"}:
        actual_float = as_float(actual, None)  # type: ignore[arg-type]
        expected_float = as_float(expected, None)  # type: ignore[arg-type]
        if actual_float is None or expected_float is None:
            return False
        if op == "lt":
            return actual_float < expected_float
        if op == "le":
            return actual_float <= expected_float
        if op == "gt":
            return actual_float > expected_float
        return actual_float >= expected_float
    raise ValueError(f"unsupported predicate op: {op}")


def predicate_matches(row: dict[str, Any], frozen_rule: dict[str, Any]) -> bool:
    predicate = frozen_rule.get("predicate")
    if not isinstance(predicate, dict):
        return False
    all_conditions = predicate.get("all")
    if not isinstance(all_conditions, list):
        return False
    return all(condition_matches(row, cond) for cond in all_conditions if isinstance(cond, dict))


def validate_paths(paths: list[Path], root: Path) -> list[str]:
    blockers = []
    for path in paths:
        ok, reason = path_safe(path, root)
        if not ok:
            blockers.append(f"{path.name}_{reason}")
        elif not path.exists():
            blockers.append(f"{path.name}_missing")
    return blockers


def validate_frozen_rule(frozen_rule: dict[str, Any]) -> list[str]:
    blockers = []
    if not frozen_rule.get("frozen_rule_id"):
        blockers.append("missing_frozen_rule_id")
    if frozen_rule.get("schema_version") != "frozen_observable_pre_action_rule_manifest_v1":
        blockers.append("frozen_rule_schema_mismatch")
    predicate_fields = [str(field) for field in frozen_rule.get("predicate_feature_names") or []]
    if not predicate_fields:
        blockers.append("predicate_feature_names_missing")
    forbidden = sorted(set(predicate_fields).intersection(FORBIDDEN_LIVE_FEATURE_FAMILIES))
    if forbidden:
        blockers.append("predicate_uses_forbidden_live_feature_family")
    outside_allowed = sorted(set(predicate_fields).difference(ALLOWED_LIVE_FEATURE_FAMILIES))
    if outside_allowed:
        blockers.append("predicate_uses_unknown_or_unallowed_feature_family")
    if frozen_rule.get("uses_only_pre_action_fields") is not True:
        blockers.append("frozen_rule_not_declared_pre_action_only")
    if frozen_rule.get("uses_realized_pair_cost") is not False:
        blockers.append("realized_pair_cost_used_as_live_criteria")
    if frozen_rule.get("uses_future_labels") is not False:
        blockers.append("post_action_label_used_in_live_predicate")
    if frozen_rule.get("train_holdout_gate_passed") is not True:
        blockers.append("train_holdout_gate_not_passed")
    if as_int(frozen_rule.get("train_selected_rows")) < 100:
        blockers.append("train_selected_rows_below_min")
    if as_int(frozen_rule.get("holdout_selected_rows")) < 50:
        blockers.append("holdout_selected_rows_below_min")
    if frozen_rule.get("private_truth_ready") is True or frozen_rule.get("deployable") is True:
        blockers.append("private_or_deployable_claim_present")
    promotion_gate = frozen_rule.get("promotion_gate") if isinstance(frozen_rule.get("promotion_gate"), dict) else {}
    if promotion_gate.get("passed") is True:
        blockers.append("private_or_deployable_or_promotion_claim_present")
    for label_field in list(frozen_rule.get("train_label_fields") or []) + list(frozen_rule.get("holdout_label_fields") or []):
        if label_field not in OFFLINE_LABEL_FIELDS:
            blockers.append("train_holdout_label_field_not_allowlisted")
    return blockers


def row_field_blockers(rows: list[dict[str, Any]]) -> list[str]:
    blockers = []
    if not rows:
        return ["missing_feature_join_rows"]
    fields = set().union(*(row.keys() for row in rows))
    missing_pre = sorted(set(REQUIRED_PRE_ACTION_FIELDS).difference(fields))
    missing_labels = sorted(set(OFFLINE_LABEL_FIELDS).difference(fields))
    if missing_pre:
        blockers.append("candidate_rows_required_pre_action_fields_missing")
    if missing_labels:
        blockers.append("candidate_rows_offline_label_fields_missing")
    return blockers


def aggregate_parity_blockers(
    *,
    rows: list[dict[str, Any]],
    source_summary: dict[str, Any],
    same_window_summary: dict[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    blockers = []
    denominator_count = len(rows)
    admitted_count = sum(1 for row in rows if row.get("status_before_action") == "admitted")
    blocked_count = sum(1 for row in rows if row.get("status_before_action") == "blocked")
    source_sequence_present_count = sum(1 for row in rows if row.get("source_sequence_present") is True)
    quote_intent_present_count = sum(1 for row in rows if row.get("quote_intent_present") is True)
    source_order_present_count = sum(1 for row in rows if row.get("source_order_present") is True)
    parity = {
        "row_count_equals_source_summary_row_count": int(source_summary.get("row_count") or -1) == denominator_count,
        "admitted_plus_blocked_equals_denominator_count": admitted_count + blocked_count == denominator_count,
        "same_window_denominator_count_matches_rows": int(
            same_window_summary.get("same_window_denominator_count") or -1
        )
        == denominator_count,
        "same_window_admitted_count_matches_rows": int(same_window_summary.get("admitted_count") or -1)
        == admitted_count,
        "same_window_blocked_count_matches_rows": int(same_window_summary.get("blocked_count") or -1)
        == blocked_count,
        "same_window_source_sequence_present_count_matches_rows": int(
            same_window_summary.get("source_sequence_present_count") or -1
        )
        == source_sequence_present_count,
        "same_window_quote_intent_present_count_matches_rows": int(
            same_window_summary.get("quote_intent_present_count") or -1
        )
        == quote_intent_present_count,
        "same_window_source_order_present_count_matches_rows": int(
            same_window_summary.get("source_order_present_count") or -1
        )
        == source_order_present_count,
    }
    for key, passed in parity.items():
        if not passed:
            blockers.append("aggregate_parity_failed")
            break
    return blockers, parity


def build_input_manifest(
    *,
    args: argparse.Namespace,
    frozen_rule: dict[str, Any],
    rows: list[dict[str, Any]],
    denominator_summary: dict[str, Any],
    denominator_path: Path,
) -> dict[str, Any]:
    fields = sorted(set().union(*(row.keys() for row in rows))) if rows else []
    split = frozen_rule.get("train_holdout_split") if isinstance(frozen_rule.get("train_holdout_split"), dict) else {}
    predicate_fields = [str(field) for field in frozen_rule.get("predicate_feature_names") or []]
    allowed_live_fields = sorted(set(REQUIRED_PRE_ACTION_FIELDS).union(predicate_fields))
    return {
        "artifact": "observable_pre_action_rule_miner_input_from_denominator_replay_fixture",
        "schema_version": INPUT_SCHEMA,
        "created_utc": utc_label(),
        "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_INPUT_READY",
        "fixture": bool(args.fixture),
        "strategy_evidence": False,
        "scope": {
            "current_worktree_only": True,
            "local_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "orders_cancels_redeems_sent": False,
            "trading_behavior_changed": False,
        },
        "materialized_sources": {
            "candidate_rows": {
                "path": str(args.candidate_rows),
                "row_count": len(rows),
                "fields": fields,
            },
            "source_link_summary": {
                "path": str(args.source_link_summary),
                "row_count": 1,
                "fields": [
                    "row_count",
                    "source_sequence_presence_by_status",
                    "quote_intent_presence_by_status",
                    "source_order_presence_by_status",
                ],
            },
            "no_order_denominator_summary": {
                "path": str(denominator_path),
                "row_count": 1,
                "fields": list(REQUIRED_NO_ORDER_FIELDS),
            },
        },
        "train_holdout_split": {
            "train_days": list(split.get("train_days") or []),
            "holdout_days": list(split.get("holdout_days") or []),
        },
        "feature_policy": {
            "allowed_live_rule_fields": allowed_live_fields,
            "offline_label_fields": list(OFFLINE_LABEL_FIELDS),
        },
        "frozen_rule_contract": {
            "frozen_rule_id": frozen_rule.get("frozen_rule_id"),
            "predicate_fields": predicate_fields,
            "uses_only_pre_action_fields": frozen_rule.get("uses_only_pre_action_fields") is True,
            "uses_realized_pair_cost": frozen_rule.get("uses_realized_pair_cost") is True,
            "uses_future_labels": frozen_rule.get("uses_future_labels") is True,
            "train_holdout_gate_passed": frozen_rule.get("train_holdout_gate_passed") is True,
            "train_selected_rows": as_int(frozen_rule.get("train_selected_rows")),
            "holdout_selected_rows": as_int(frozen_rule.get("holdout_selected_rows")),
        },
        "no_order_denominator_gate": {
            "same_window_denominator_reproduced": denominator_summary.get("aggregate_parity") is True,
            "same_window_denominator_count": denominator_summary.get("same_window_denominator_count"),
            "source_sequence_coverage": denominator_summary.get("source_sequence_coverage"),
            "aggregate_parity": denominator_summary.get("aggregate_parity") is True,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
    }


def score(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    paths = [
        args.candidate_rows,
        args.source_link_summary,
        args.feature_join_manifest,
        args.frozen_rule_manifest,
        args.same_window_aggregate_summary,
    ]
    blockers = validate_paths(paths, root)
    if blockers:
        return build_fail_manifest(args, blockers)

    rows = load_rows(args.candidate_rows)
    source_summary = read_json(args.source_link_summary)
    feature_manifest = read_json(args.feature_join_manifest)
    frozen_rule = read_json(args.frozen_rule_manifest)
    same_window_summary = read_json(args.same_window_aggregate_summary)

    blockers.extend(row_field_blockers(rows))
    blockers.extend(validate_frozen_rule(frozen_rule))
    feature_contract = feature_manifest.get("field_contract") if isinstance(feature_manifest, dict) else {}
    if feature_contract.get("events_jsonl_read") is True or feature_contract.get("raw_replay_or_full_store_scan") is True:
        blockers.append("feature_join_manifest_forbidden_read_or_scan_claim")
    if feature_contract.get("private_truth_ready") is True or feature_contract.get("deployable") is True:
        blockers.append("feature_join_manifest_private_or_deployable_claim")
    if source_summary.get("schema_version") != "observable_pre_action_source_link_summary_v1":
        blockers.append("source_link_summary_schema_mismatch")
    if feature_manifest.get("schema_version") != "observable_pre_action_feature_join_manifest_v1":
        blockers.append("feature_join_manifest_schema_mismatch")
    if same_window_summary.get("schema_version") != "same_window_aggregate_summary_v1":
        blockers.append("same_window_aggregate_summary_schema_mismatch")

    parity_blockers, parity = aggregate_parity_blockers(
        rows=rows,
        source_summary=source_summary,
        same_window_summary=same_window_summary,
    )
    blockers.extend(parity_blockers)

    matched_rows = [row for row in rows if predicate_matches(row, frozen_rule)]
    denominator_count = len(rows)
    admitted_count = sum(1 for row in rows if row.get("status_before_action") == "admitted")
    blocked_count = sum(1 for row in rows if row.get("status_before_action") == "blocked")
    source_sequence_coverage = presence_rate(rows, "source_sequence_present")
    quote_intent_presence_rate = presence_rate(rows, "quote_intent_present")
    source_order_presence_rate = presence_rate(rows, "source_order_present")

    if denominator_count < args.min_denominator:
        blockers.append("denominator_sample_too_small")
    if len(matched_rows) < args.min_rule_matches:
        blockers.append("rule_match_sample_too_small")
    if source_sequence_coverage < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_min")

    aggregate_parity = not parity_blockers
    denominator_summary = {
        "schema_version": DENOMINATOR_SCHEMA,
        "frozen_rule_id": frozen_rule.get("frozen_rule_id"),
        "same_window_id": same_window_summary.get("same_window_id"),
        "same_window_denominator_count": denominator_count,
        "rule_match_count": len(matched_rows),
        "admitted_count": admitted_count,
        "blocked_count": blocked_count,
        "source_sequence_coverage": source_sequence_coverage,
        "quote_intent_presence_rate": quote_intent_presence_rate,
        "source_order_presence_rate": source_order_presence_rate,
        "aggregate_parity": aggregate_parity,
        "aggregate_parity_detail": parity,
        "field_contract": field_contract(),
        "strategy_evidence": False,
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
        },
    }

    denominator_path = args.output_dir / "observable_pre_action_no_order_denominator_summary.json"
    input_manifest_path = args.output_dir / "observable_pre_action_rule_miner_input_manifest.json"
    if not blockers:
        write_json(denominator_path, denominator_summary)
        input_manifest = build_input_manifest(
            args=args,
            frozen_rule=frozen_rule,
            rows=rows,
            denominator_summary=denominator_summary,
            denominator_path=denominator_path,
        )
        write_json(input_manifest_path, input_manifest)
    else:
        input_manifest = None

    ready = not blockers
    return {
        "artifact": ARTIFACT,
        "contract_name": CONTRACT_NAME,
        "decision": "KEEP" if ready else "UNKNOWN",
        "decision_label": (
            "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY"
            if ready
            else "UNKNOWN_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_FAIL_CLOSED"
        ),
        "blockers": sorted(set(blockers)),
        "inputs": {
            "candidate_rows": str(args.candidate_rows),
            "source_link_summary": str(args.source_link_summary),
            "feature_join_manifest": str(args.feature_join_manifest),
            "frozen_rule_manifest": str(args.frozen_rule_manifest),
            "same_window_aggregate_summary": str(args.same_window_aggregate_summary),
        },
        "outputs": {
            "denominator_summary": str(denominator_path) if ready else None,
            "input_manifest": str(input_manifest_path) if ready else None,
        },
        "score": {
            "same_window_denominator_count": denominator_count,
            "rule_match_count": len(matched_rows),
            "admitted_count": admitted_count,
            "blocked_count": blocked_count,
            "source_sequence_coverage": source_sequence_coverage,
            "quote_intent_presence_rate": quote_intent_presence_rate,
            "source_order_presence_rate": source_order_presence_rate,
            "aggregate_parity": aggregate_parity,
        },
        "research_ranking": "DENOMINATOR_REPLAY_FIXTURE_SCORER_READY_NO_STRATEGY_EVIDENCE" if ready else "DENOMINATOR_REPLAY_FAIL_CLOSED",
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "denominator_replay_fixture_scorer_only",
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
            "Run observable_pre_action_rule_miner_spec.py on the generated input manifest, then implement "
            "observable_pre_action_rule_miner_real_input_inventory_v1 local-only to determine whether any "
            "current-worktree non-fixture materialized source can satisfy this denominator replay contract."
            if ready
            else "Fix frozen-rule, denominator, source coverage, or aggregate-parity inputs before mining."
        ),
    }


def build_fail_manifest(args: argparse.Namespace, blockers: list[str]) -> dict[str, Any]:
    return {
        "artifact": ARTIFACT,
        "contract_name": CONTRACT_NAME,
        "decision": "UNKNOWN",
        "decision_label": "UNKNOWN_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_FAIL_CLOSED",
        "blockers": sorted(set(blockers)),
        "inputs": {
            "candidate_rows": str(args.candidate_rows),
            "source_link_summary": str(args.source_link_summary),
            "feature_join_manifest": str(args.feature_join_manifest),
            "frozen_rule_manifest": str(args.frozen_rule_manifest),
            "same_window_aggregate_summary": str(args.same_window_aggregate_summary),
        },
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "denominator_replay_fixture_scorer_fail_closed",
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
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-rows", type=Path, required=True)
    parser.add_argument("--source-link-summary", type=Path, required=True)
    parser.add_argument("--feature-join-manifest", type=Path, required=True)
    parser.add_argument("--frozen-rule-manifest", type=Path, required=True)
    parser.add_argument("--same-window-aggregate-summary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--min-denominator", type=int, default=100)
    parser.add_argument("--min-rule-matches", type=int, default=20)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = score(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
