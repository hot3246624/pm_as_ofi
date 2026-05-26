#!/usr/bin/env python3
"""Specify and validate same-window offline-label handoffs.

This local-only contract sits between observable pre-action feature-join output
and training bridge rows. It validates that an in-worktree offline label handoff
overlaps the feature rows by exact source ids or strict same-window composite
keys, while keeping post-action labels train/holdout-only. It does not read
events JSONL, scan raw/replay stores, use SSH, start services, or change trading
behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_same_window_offline_label_handoff_contract"
CONTRACT_NAME = "observable_pre_action_same_window_offline_label_handoff_contract_v1"
HANDOFF_SCHEMA = "observable_pre_action_same_window_offline_label_handoff_v1"
ROW_SCHEMA = "observable_pre_action_candidate_row_v1"

DEFAULT_FEATURE_HANDOFF_COMPLETION = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_feature_join_handoff_completion_20260526T041336Z/"
    "manifest.json"
)
DEFAULT_FEATURE_JOIN_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_feature_join_handoff_completion_20260526T041336Z/"
    "feature_join_scorer/manifest.json"
)
DEFAULT_BRIDGE_CONTRACT = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_contract_20260525T213507Z/"
    "manifest.json"
)
DEFAULT_BRIDGE_FEASIBILITY_AUDIT = Path(
    "xuan_research_artifacts/"
    "xuan_feature_join_offline_label_bridge_feasibility_audit_20260526T053411Z/"
    "manifest.json"
)

SUPPORTING_DECISIONS = {
    "feature_handoff_completion": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_HANDOFF_READY_REAL_MINER_INPUT_UNKNOWN",
    "feature_join_scorer": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY",
    "bridge_contract": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_READY",
    "bridge_feasibility_audit": "UNKNOWN_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_INPUT_CONTRACT_GAPS",
}

FALSE_SCOPE_FIELDS = (
    "new_data_fetched",
    "external_worktree_read",
    "ssh_used",
    "shadow_started",
    "canary_or_live_started",
    "events_jsonl_read",
    "events_jsonl_pulled",
    "raw_replay_or_full_store_scanned",
    "shared_ingress_or_broker_or_live_modified",
    "shared_ws_or_local_agg_or_service_started",
    "orders_cancels_redeems_sent",
    "trading_behavior_changed",
)

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

REQUIRED_FILE_KEYS = (
    "feature_join_candidate_rows",
    "feature_join_source_link_summary",
    "feature_join_manifest",
    "same_window_aggregate_summary",
    "offline_label_csv",
)

EXACT_ID_FIELDS = (
    "source_seed_candidate_row_id",
    "source_seed_action_id",
)

FEATURE_REQUIRED_FIELDS = (
    "schema_version",
    "condition_id",
    "market_slug",
    "day_id",
    "quote_ts_ms",
    "side",
    "offset_s",
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
    "candidate_qty",
    "source_risk_direction",
    "post_action_outcome_labels_included",
    "realized_pair_cost_used_as_live_criteria",
    "trading_behavior_changed",
)

OFFLINE_LABEL_CSV_FIELDS = (
    "condition_id",
    "slug",
    "day",
    "ts_ms",
    "trigger_ts_ms",
    "side",
    "offset_s",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "seed_qty",
    "trigger_size",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "outcome_labels_are_post_action",
    "pre_action_field_policy",
    "post_action_label_policy",
    "live_rule_safety_policy",
)

LABEL_FIELDS = (
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_float(value: Any, default: float | None = 0.0) -> float | None:
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
        return int(float(value))
    except (TypeError, ValueError):
        return default


def truthy_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def label_live_policy_safe(value: Any) -> bool:
    policy = str(value or "").strip().lower()
    if not policy:
        return False
    if "allowed in live" in policy or "may use post-action" in policy:
        return False
    if "forbidden" in policy:
        return True
    return "pre-action" in policy and "not private truth" in policy and "deployable" in policy and "promotion" in policy


def present(value: Any) -> bool:
    return value is not None and str(value) != ""


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


def safe_json(path: Path, root: Path) -> tuple[dict[str, Any], str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {}, reason
    if not path.exists():
        return {}, "missing"
    try:
        obj = read_json(path)
    except Exception as exc:
        return {}, f"{type(exc).__name__}:{exc}"
    if not isinstance(obj, dict):
        return {}, "not_json_object"
    return obj, None


def fixture_like_path(path: Path) -> bool:
    lowered = "/".join(path.parts).lower()
    markers = (
        "_smoke_",
        "/fixture/",
        "/fixtures/",
        "/good/",
        "/bad",
        "fixture_scorer",
        "synthetic_fixture",
    )
    return any(marker in lowered for marker in markers)


def load_jsonl(path: Path) -> tuple[list[dict[str, Any]], str | None]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open() as fh:
            for line_no, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                row = json.loads(line)
                if not isinstance(row, dict):
                    return rows, f"line_{line_no}_not_json_object"
                rows.append(row)
    except Exception as exc:
        return rows, f"{type(exc).__name__}:{exc}"
    return rows, None


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]], str | None]:
    try:
        with path.open(newline="") as fh:
            reader = csv.DictReader(fh)
            return list(reader.fieldnames or []), [dict(row) for row in reader], None
    except Exception as exc:
        return [], [], f"{type(exc).__name__}:{exc}"


def missing_fields(fields: set[str], required: tuple[str, ...]) -> list[str]:
    return sorted(set(required).difference(fields))


def compact(values: set[str], limit: int = 20) -> list[str]:
    return sorted(values)[:limit]


def observe_supporting(path: Path, root: Path, expected_decision: str) -> dict[str, Any]:
    data, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        blockers.append(f"manifest_{error}")
    elif data.get("decision_label") != expected_decision:
        blockers.append("decision_label_mismatch")
    promotion = as_dict(data.get("promotion_gate"))
    if data.get("private_truth_ready") is True or data.get("deployable") is True or promotion.get("passed") is True:
        blockers.append("private_deployable_or_promotion_claim_present")
    return {
        "path": str(path),
        "expected_decision_label": expected_decision,
        "decision_label": data.get("decision_label"),
        "ready": not blockers,
        "blockers": blockers,
    }


def numeric_match(feature: dict[str, Any], label: dict[str, str], feature_field: str, label_fields: tuple[str, ...], tol: float) -> bool:
    fv = as_float(feature.get(feature_field), None)
    if fv is None:
        return False
    for label_field in label_fields:
        lv = as_float(label.get(label_field), None)
        if lv is not None and abs(fv - lv) <= tol:
            return True
    return False


def composite_base_key(feature: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(feature.get("condition_id") or ""),
        str(feature.get("market_slug") or ""),
        str(feature.get("day_id") or ""),
        str(feature.get("side") or ""),
        str(feature.get("source_risk_direction") or ""),
    )


def label_base_key(label: dict[str, str]) -> tuple[str, str, str, str, str]:
    return (
        str(label.get("condition_id") or ""),
        str(label.get("slug") or ""),
        str(label.get("day") or ""),
        str(label.get("side") or ""),
        str(label.get("source_risk_direction") or ""),
    )


def composite_match(feature: dict[str, Any], label: dict[str, str], args: argparse.Namespace) -> bool:
    if composite_base_key(feature) != label_base_key(label):
        return False
    checks = (
        ("quote_ts_ms", ("ts_ms", "trigger_ts_ms"), args.timestamp_tolerance_ms),
        ("offset_s", ("offset_s",), args.offset_tolerance_s),
        ("pre_seed_same_qty", ("pre_seed_same_qty",), args.qty_tolerance),
        ("pre_seed_opp_qty", ("pre_seed_opp_qty",), args.qty_tolerance),
        ("pre_seed_same_cost", ("pre_seed_same_cost",), args.cost_tolerance),
        ("pre_seed_opp_cost", ("pre_seed_opp_cost",), args.cost_tolerance),
        ("pre_seed_open_qty", ("pre_seed_open_qty",), args.qty_tolerance),
        ("pre_seed_open_cost", ("pre_seed_open_cost",), args.cost_tolerance),
        ("candidate_qty", ("seed_qty", "trigger_size"), args.qty_tolerance),
    )
    return all(numeric_match(feature, label, feature_field, label_fields, tol) for feature_field, label_fields, tol in checks)


def build_label_indexes(labels: list[dict[str, str]]) -> tuple[dict[str, dict[str, list[int]]], dict[tuple[str, str, str, str, str], list[int]]]:
    exact_indexes: dict[str, dict[str, list[int]]] = {field: {} for field in EXACT_ID_FIELDS}
    composite_index: dict[tuple[str, str, str, str, str], list[int]] = {}
    for idx, label in enumerate(labels):
        for field in EXACT_ID_FIELDS:
            value = str(label.get(field) or "")
            if value:
                exact_indexes[field].setdefault(value, []).append(idx)
        composite_index.setdefault(label_base_key(label), []).append(idx)
    return exact_indexes, composite_index


def joinability(feature_rows: list[dict[str, Any]], labels: list[dict[str, str]], args: argparse.Namespace) -> dict[str, Any]:
    exact_indexes, composite_index = build_label_indexes(labels)
    exact_joined = 0
    composite_joined = 0
    multiple_exact = 0
    multiple_composite = 0
    zero_exact = 0
    zero_composite = 0
    for feature in feature_rows:
        exact_used = False
        for field in EXACT_ID_FIELDS:
            value = str(feature.get(field) or "")
            if not value:
                continue
            exact_used = True
            matches = exact_indexes.get(field, {}).get(value, [])
            if len(matches) == 1:
                exact_joined += 1
            elif len(matches) > 1:
                multiple_exact += 1
            else:
                zero_exact += 1
            break
        if exact_used:
            continue
        candidates = composite_index.get(composite_base_key(feature), [])
        matches = [idx for idx in candidates if composite_match(feature, labels[idx], args)]
        if len(matches) == 1:
            composite_joined += 1
        elif len(matches) > 1:
            multiple_composite += 1
        else:
            zero_composite += 1
    joined = exact_joined + composite_joined
    return {
        "composite_base_key_overlap_count": sum(1 for row in feature_rows if composite_base_key(row) in composite_index),
        "composite_joined": composite_joined,
        "exact_joined": exact_joined,
        "join_coverage": round(joined / len(feature_rows), 6) if feature_rows else 0.0,
        "joined_rows": joined,
        "multiple_composite_matches": multiple_composite,
        "multiple_exact_matches": multiple_exact,
        "zero_composite_matches": zero_composite,
        "zero_exact_matches": zero_exact,
    }


def handoff_contract(validation_root: Path) -> dict[str, Any]:
    return {
        "schema_version": HANDOFF_SCHEMA,
        "required_files": {
            "feature_join_candidate_rows": "observable_pre_action_candidate_rows.jsonl",
            "feature_join_source_link_summary": "observable_pre_action_source_link_summary.json",
            "feature_join_manifest": "observable_pre_action_feature_join_manifest.json",
            "same_window_aggregate_summary": "same_window_aggregate_summary.json",
            "offline_label_csv": "same-window offline source_pair/source_residual label CSV",
        },
        "required_join_keys": {
            "exact_id_preferred": list(EXACT_ID_FIELDS),
            "strict_composite_fallback": [
                "condition_id",
                "market_slug/slug",
                "day_id/day",
                "side",
                "source_risk_direction",
                "quote_ts_ms within tolerance of ts_ms or trigger_ts_ms",
                "offset_s",
                "pre_seed_same_qty",
                "pre_seed_opp_qty",
                "pre_seed_same_cost",
                "pre_seed_opp_cost",
                "pre_seed_open_qty",
                "pre_seed_open_cost",
                "candidate_qty against seed_qty or trigger_size",
            ],
        },
        "minimums": {
            "min_join_coverage": 0.95,
            "min_joined_rows": 100,
            "min_day_overlap_count": 1,
            "min_slug_overlap_count": 1,
            "min_condition_overlap_count": 1,
            "min_source_sequence_coverage": 0.95,
        },
        "label_policy": {
            "source_pair_source_residual_labels_are_post_action": True,
            "labels_allowed_for_train_holdout_scoring": True,
            "labels_allowed_in_live_predicate": False,
            "realized_pair_cost_allowed_as_live_criteria": False,
        },
        "validation_order": [
            "same_window_offline_label_handoff_contract_validator",
            "feature_join_output_scorer",
            "feature_join_offline_label_bridge_feasibility_audit",
            "training_input_bridge_scorer",
            "offline_training_scorer",
            "denominator_replay_scorer",
            "observable_pre_action_rule_miner_spec",
            "real_input_inventory_v2",
        ],
        "validation_commands_template": [
            (
                "python3 scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py "
                "--handoff-manifest ${same_window_offline_label_handoff_manifest} "
                f"--output-dir {validation_root}/00_same_window_label_handoff"
            ),
            (
                "python3 scripts/xuan_feature_join_offline_label_bridge_feasibility_audit.py "
                "--feature-rows ${feature_join_candidate_rows} "
                "--feature-scorer-manifest ${feature_join_scorer_manifest} "
                "--offline-label-csv ${offline_label_csv} "
                "--bridge-contract-manifest ${bridge_contract_manifest} "
                f"--output-dir {validation_root}/01_bridge_feasibility"
            ),
        ],
        "fail_closed_conditions": [
            "handoff_schema_mismatch",
            "fixture_or_smoke_handoff_as_real",
            "events_jsonl_or_raw_replay_path",
            "outside_current_worktree_path",
            "feature_rows_or_label_csv_missing_required_fields",
            "source_pair_or_source_residual_label_allowed_in_live_predicate",
            "realized_pair_cost_used_as_live_criteria",
            "day_slug_condition_overlap_absent",
            "exact_id_and_strict_composite_join_coverage_below_min",
            "ambiguous_or_duplicate_join_matches_present",
            "private_truth_deployable_or_promotion_claim",
        ],
    }


def validate_scope(scope: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if scope.get("current_worktree_only") is not True:
        blockers.append("scope_current_worktree_only_not_true")
    if scope.get("local_only") is not True:
        blockers.append("scope_local_only_not_true")
    for field in FALSE_SCOPE_FIELDS:
        if scope.get(field) is not False:
            blockers.append(f"scope_{field}_not_false")
    return blockers


def resolve_handoff_path(raw: Any, base_dir: Path) -> Path:
    path = Path(str(raw))
    return path if path.is_absolute() else base_dir / path


def load_required_path(
    files: dict[str, Any],
    key: str,
    root: Path,
    allow_fixture_paths: bool,
    base_dir: Path,
) -> tuple[Path | None, list[str]]:
    raw = files.get(key)
    if not raw:
        return None, [f"{key}_path_missing"]
    path = resolve_handoff_path(raw, base_dir)
    ok, reason = path_safe(path, root)
    blockers: list[str] = []
    if not ok:
        blockers.append(f"{key}_{reason}")
    elif not path.exists():
        blockers.append(f"{key}_missing_on_disk")
    elif fixture_like_path(path) and not allow_fixture_paths:
        blockers.append(f"{key}_fixture_or_smoke_path")
    return path, blockers


def validate_json_contract(path: Path, root: Path, label: str, expected_schema: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    data, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        blockers.append(f"{label}_{error}")
    if expected_schema and data.get("schema_version") != expected_schema:
        blockers.append(f"{label}_schema_mismatch")
    promotion = as_dict(data.get("promotion_gate"))
    if data.get("private_truth_ready") is True or data.get("deployable") is True or promotion.get("passed") is True:
        blockers.append(f"{label}_private_deployable_or_promotion_claim")
    return data, {
        "path": str(path),
        "schema_version": data.get("schema_version"),
        "ready": not blockers,
        "blockers": sorted(set(blockers)),
    }


def validate_feature_rows(rows: list[dict[str, Any]], args: argparse.Namespace) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    field_union: set[str] = set()
    status_counts: dict[str, int] = {}
    exact_counts = {field: 0 for field in EXACT_ID_FIELDS}
    for idx, row in enumerate(rows):
        field_union.update(str(key) for key in row)
        if row.get("schema_version") != ROW_SCHEMA:
            blockers.append(f"feature_row_{idx}_schema_mismatch")
        missing = missing_fields(set(row), FEATURE_REQUIRED_FIELDS)
        if missing:
            blockers.append(f"feature_row_{idx}_required_fields_missing")
        if row.get("post_action_outcome_labels_included") is not False:
            blockers.append(f"feature_row_{idx}_post_action_labels_present")
        if row.get("realized_pair_cost_used_as_live_criteria") is not False:
            blockers.append(f"feature_row_{idx}_realized_pair_cost_live")
        if row.get("trading_behavior_changed") is not False:
            blockers.append(f"feature_row_{idx}_trading_behavior_changed")
        status = str(row.get("status_before_action") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        for field in EXACT_ID_FIELDS:
            if present(row.get(field)):
                exact_counts[field] += 1
    if len(rows) < args.min_feature_rows:
        blockers.append("feature_rows_below_min")
    return blockers, {
        "exact_id_present_counts": exact_counts,
        "field_count": len(field_union),
        "row_count": len(rows),
        "status_counts": status_counts,
    }


def validate_labels(fields: list[str], labels: list[dict[str, str]], args: argparse.Namespace) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    missing = missing_fields(set(fields), OFFLINE_LABEL_CSV_FIELDS)
    if missing:
        blockers.append("offline_label_csv_required_fields_missing")
    if len(labels) < args.min_feature_rows:
        blockers.append("offline_label_rows_below_min")
    post_action_missing = 0
    live_policy_bad = 0
    missing_label_counts: dict[str, int] = {}
    exact_counts = {field: 0 for field in EXACT_ID_FIELDS}
    for row in labels:
        if not truthy_text(row.get("outcome_labels_are_post_action")):
            post_action_missing += 1
        if not label_live_policy_safe(row.get("live_rule_safety_policy")):
            live_policy_bad += 1
        for field in LABEL_FIELDS:
            if field not in fields or not present(row.get(field)):
                missing_label_counts[field] = missing_label_counts.get(field, 0) + 1
        for field in EXACT_ID_FIELDS:
            if present(row.get(field)):
                exact_counts[field] += 1
    if post_action_missing:
        blockers.append(f"offline_label_post_action_policy_missing_count:{post_action_missing}")
    if live_policy_bad:
        blockers.append(f"offline_label_live_policy_not_safe_count:{live_policy_bad}")
    for field, count in sorted(missing_label_counts.items()):
        blockers.append(f"offline_label_field_missing_count:{field}:{count}")
    return blockers, {
        "exact_id_present_counts": exact_counts,
        "field_count": len(fields),
        "row_count": len(labels),
    }


def overlap_observations(feature_rows: list[dict[str, Any]], labels: list[dict[str, str]]) -> dict[str, Any]:
    feature_days = {str(row.get("day_id") or "") for row in feature_rows}
    label_days = {str(row.get("day") or "") for row in labels}
    feature_slugs = {str(row.get("market_slug") or "") for row in feature_rows}
    label_slugs = {str(row.get("slug") or "") for row in labels}
    feature_conditions = {str(row.get("condition_id") or "") for row in feature_rows}
    label_conditions = {str(row.get("condition_id") or "") for row in labels}
    return {
        "condition_overlap_count": len(feature_conditions.intersection(label_conditions)),
        "day_overlap": compact(feature_days.intersection(label_days)),
        "feature_day_sample": compact(feature_days),
        "feature_slug_count": len(feature_slugs),
        "label_day_sample": compact(label_days),
        "label_slug_count": len(label_slugs),
        "slug_overlap_count": len(feature_slugs.intersection(label_slugs)),
    }


def validate_handoff(args: argparse.Namespace, root: Path) -> tuple[dict[str, Any], list[str]]:
    handoff, error = safe_json(args.handoff_manifest, root)
    blockers: list[str] = []
    observations: dict[str, Any] = {}
    if error:
        return {"handoff_manifest_error": error}, [f"handoff_manifest_{error}"]
    if handoff.get("schema_version") != HANDOFF_SCHEMA:
        blockers.append("handoff_schema_mismatch")
    if handoff.get("fixture") is True:
        blockers.append("handoff_fixture_true")
    if handoff.get("strategy_evidence") is True:
        blockers.append("handoff_claims_strategy_evidence")
    promotion = as_dict(handoff.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("handoff_private_deployable_or_promotion_claim")
    blockers.extend(validate_scope(as_dict(handoff.get("scope"))))
    provenance = as_dict(handoff.get("provenance"))
    for field in (
        "events_jsonl_read",
        "events_jsonl_pulled",
        "raw_replay_or_full_store_scanned",
        "external_worktree_read",
        "ssh_used_by_validator",
        "orders_cancels_redeems_sent",
        "trading_behavior_changed",
    ):
        if provenance.get(field) is not False:
            blockers.append(f"provenance_{field}_not_false")
    label_policy = as_dict(handoff.get("label_policy"))
    if label_policy.get("labels_allowed_for_train_holdout_scoring") is not True:
        blockers.append("labels_not_allowed_for_train_holdout_scoring")
    if label_policy.get("labels_allowed_in_live_predicate") is not False:
        blockers.append("labels_allowed_in_live_predicate_not_false")
    if label_policy.get("realized_pair_cost_allowed_as_live_criteria") is not False:
        blockers.append("realized_pair_cost_live_criteria_not_false")

    files = as_dict(handoff.get("files"))
    allow_fixture_paths = bool(args.allow_fixture_paths_for_smoke)
    handoff_base_dir = args.handoff_manifest.parent
    paths: dict[str, Path] = {}
    for key in REQUIRED_FILE_KEYS:
        path, path_blockers = load_required_path(files, key, root, allow_fixture_paths, handoff_base_dir)
        blockers.extend(path_blockers)
        if path is not None:
            paths[key] = path

    feature_rows: list[dict[str, Any]] = []
    labels: list[dict[str, str]] = []
    if "feature_join_candidate_rows" in paths:
        feature_rows, error = load_jsonl(paths["feature_join_candidate_rows"])
        if error:
            blockers.append(f"feature_rows_{error}")
        row_blockers, obs = validate_feature_rows(feature_rows, args)
        blockers.extend(row_blockers)
        observations["feature_rows"] = obs
    if "offline_label_csv" in paths:
        fields, labels, error = load_csv(paths["offline_label_csv"])
        if error:
            blockers.append(f"offline_label_csv_{error}")
        label_blockers, obs = validate_labels(fields, labels, args)
        blockers.extend(label_blockers)
        observations["offline_label_csv"] = obs

    json_specs = {
        "feature_join_source_link_summary": "observable_pre_action_source_link_summary_v1",
        "feature_join_manifest": "observable_pre_action_feature_join_manifest_v1",
    }
    for key, schema in json_specs.items():
        if key in paths:
            data, obs = validate_json_contract(paths[key], root, key, schema)
            observations[key] = obs
            blockers.extend(obs["blockers"])
            field_contract = as_dict(data.get("field_contract"))
            if field_contract:
                if field_contract.get("post_action_outcome_labels_included") is not False:
                    blockers.append(f"{key}_post_action_label_contract_not_false")
                if field_contract.get("realized_pair_cost_used_as_live_criteria") is not False:
                    blockers.append(f"{key}_realized_pair_cost_live_contract_not_false")
                if field_contract.get("trading_behavior_changed") is not False:
                    blockers.append(f"{key}_trading_behavior_contract_not_false")
    if "same_window_aggregate_summary" in paths:
        _, obs = validate_json_contract(paths["same_window_aggregate_summary"], root, "same_window_aggregate_summary")
        observations["same_window_aggregate_summary"] = obs
        blockers.extend(obs["blockers"])

    if feature_rows and labels:
        overlap = overlap_observations(feature_rows, labels)
        join = joinability(feature_rows, labels, args)
        observations["overlap"] = overlap
        observations["joinability"] = join
        if not overlap["day_overlap"]:
            blockers.append("feature_label_day_overlap_absent")
        if int(overlap["slug_overlap_count"]) < args.min_slug_overlap_count:
            blockers.append("feature_label_slug_overlap_below_min")
        if int(overlap["condition_overlap_count"]) < args.min_condition_overlap_count:
            blockers.append("feature_label_condition_overlap_below_min")
        if int(join["joined_rows"]) < args.min_joined_rows:
            blockers.append("joined_rows_below_min")
        if float(join["join_coverage"]) < args.min_join_coverage:
            blockers.append("join_coverage_below_min")
        if int(join["multiple_exact_matches"]) or int(join["multiple_composite_matches"]):
            blockers.append("ambiguous_join_matches_present")

    return observations, sorted(set(blockers))


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    validation_root = args.validation_root or (args.output_dir / "validation")
    supporting = {
        "feature_handoff_completion": observe_supporting(
            args.feature_handoff_completion_manifest, root, SUPPORTING_DECISIONS["feature_handoff_completion"]
        ),
        "feature_join_scorer": observe_supporting(
            args.feature_join_scorer_manifest, root, SUPPORTING_DECISIONS["feature_join_scorer"]
        ),
        "bridge_contract": observe_supporting(
            args.bridge_contract_manifest, root, SUPPORTING_DECISIONS["bridge_contract"]
        ),
        "bridge_feasibility_audit": observe_supporting(
            args.bridge_feasibility_audit_manifest, root, SUPPORTING_DECISIONS["bridge_feasibility_audit"]
        ),
    }
    supporting_ready = all(item.get("ready") for item in supporting.values())

    blockers: list[str] = []
    observations: dict[str, Any] = {}
    handoff_status = "UNKNOWN_SAME_WINDOW_LABEL_HANDOFF_MANIFEST_MISSING"
    if not supporting_ready:
        blockers.append("supporting_contract_or_scorer_not_ready")
    if args.handoff_manifest:
        observations, handoff_blockers = validate_handoff(args, root)
        blockers.extend(handoff_blockers)
        handoff_status = "HANDOFF_READY" if not handoff_blockers else "HANDOFF_FAIL_CLOSED"

    if args.handoff_manifest and not blockers:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_READY"
        next_action = (
            "Run feature_join_offline_label_bridge_feasibility_audit and then training bridge scorer on this "
            "same-window label handoff; do not start no-order diagnostics automatically."
        )
    elif args.handoff_manifest:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_FAIL_CLOSED"
        next_action = "Fix same-window label handoff path/schema/overlap/provenance blockers before bridge scoring."
    elif supporting_ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_CONTRACT_READY"
        next_action = (
            "Wait for or create a future in-worktree same-window offline-label handoff that overlaps feature rows "
            "by exact ids or strict composite keys; do not start no-order diagnostics automatically."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_CONTRACT_FAIL_CLOSED"
        next_action = "Repair supporting feature-join/bridge audit contract readiness before accepting label handoffs."

    return {
        "artifact": ARTIFACT,
        "blockers": sorted(set(blockers)),
        "contract": handoff_contract(validation_root),
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "handoff_manifest": str(args.handoff_manifest) if args.handoff_manifest else None,
        "handoff_observations": observations,
        "handoff_status": handoff_status,
        "lane": "observable_pre_action_same_window_offline_label_handoff_contract",
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "promotion_gate": {"passed": False, "reason": "same_window_offline_label_handoff_contract_only"},
        "research_ranking": {
            "handoff_ready": bool(args.handoff_manifest and not blockers),
            "interpretation": "Contract/handoff validation only. KEEP handoff permits local bridge scoring, not promotion or deployment.",
            "no_order_diagnostic_allowed": False,
            "status": decision_label,
            "strategy_evidence": False,
        },
        "safety": {
            "broker_modified": False,
            "canary_or_live_started": False,
            "cancels_sent": False,
            "events_jsonl_pulled": False,
            "events_jsonl_read": False,
            "external_worktree_read": False,
            "new_data_fetched": False,
            "orders_sent": False,
            "raw_replay_or_full_store_scan": False,
            "redeems_sent": False,
            "service_control_used": False,
            "shadow_started": False,
            "shared_ingress_modified": False,
            "ssh_used": False,
            "trading_behavior_changed": False,
        },
        "schema_version": 1,
        "strategy_evidence": False,
        "supporting_contracts_and_scorers": supporting,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff-manifest", type=Path)
    parser.add_argument("--feature-handoff-completion-manifest", type=Path, default=DEFAULT_FEATURE_HANDOFF_COMPLETION)
    parser.add_argument("--feature-join-scorer-manifest", type=Path, default=DEFAULT_FEATURE_JOIN_SCORER)
    parser.add_argument("--bridge-contract-manifest", type=Path, default=DEFAULT_BRIDGE_CONTRACT)
    parser.add_argument("--bridge-feasibility-audit-manifest", type=Path, default=DEFAULT_BRIDGE_FEASIBILITY_AUDIT)
    parser.add_argument("--validation-root", type=Path)
    parser.add_argument("--created-utc", default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    parser.add_argument("--output-dir", type=Path, default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}")
    parser.add_argument("--timestamp-tolerance-ms", type=float, default=250.0)
    parser.add_argument("--offset-tolerance-s", type=float, default=0.25)
    parser.add_argument("--qty-tolerance", type=float, default=0.000001)
    parser.add_argument("--cost-tolerance", type=float, default=0.000001)
    parser.add_argument("--min-feature-rows", type=int, default=100)
    parser.add_argument("--min-joined-rows", type=int, default=100)
    parser.add_argument("--min-join-coverage", type=float, default=0.95)
    parser.add_argument("--min-slug-overlap-count", type=int, default=1)
    parser.add_argument("--min-condition-overlap-count", type=int, default=1)
    parser.add_argument("--allow-fixture-paths-for-smoke", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
