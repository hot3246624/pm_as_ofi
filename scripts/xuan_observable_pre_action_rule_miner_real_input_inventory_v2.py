#!/usr/bin/env python3
"""Inventory real inputs for observable pre-action rule mining v2.

This local-only inventory extends v1 after the training fixture scorer landed.
It checks whether the current worktree contains a complete non-fixture input
chain:

1. observable pre-action feature-join rows,
2. bridge-joined train/holdout rows with offline labels,
3. a frozen pre-action rule and same-window denominator summary, and
4. observable_pre_action_rule_miner_input_v1.

It does not read events JSONL, raw/replay stores, use SSH, start shadows or
services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_real_input_inventory_v2"
CONTRACT_NAME = "observable_pre_action_rule_miner_real_input_inventory_v2"
INPUT_SCHEMA = "observable_pre_action_rule_miner_input_v1"

DEFAULT_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_spec_20260525T182006Z/"
    "manifest.json"
)
DEFAULT_FEATURE_JOIN_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z/"
    "manifest.json"
)
DEFAULT_DENOMINATOR_FIXTURE_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_20260525T202536Z/"
    "manifest.json"
)
DEFAULT_BRIDGE_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/"
    "manifest.json"
)
DEFAULT_TRAINING_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_spec_20260525T223507Z/"
    "manifest.json"
)
DEFAULT_TRAINING_FIXTURE_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_fixture_scorer_20260525T230507Z/"
    "manifest.json"
)
DEFAULT_CANDIDATE_CSV = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)

SUPPORTING_DECISIONS = {
    "spec": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_REAL_INPUT_UNKNOWN",
    "feature_join_scorer": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY",
    "denominator_fixture_scorer": "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY",
    "bridge_fixture_scorer": "KEEP_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_READY",
    "training_spec": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_READY_REAL_INPUT_UNKNOWN",
    "training_fixture_scorer": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_FIXTURE_SCORER_READY",
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

REQUIRED_SCOPE_FALSE_FIELDS = (
    "new_data_fetched",
    "external_worktree_read",
    "ssh_used",
    "shadow_started",
    "canary_or_live_started",
    "events_jsonl_read",
    "raw_replay_or_full_store_scanned",
    "shared_ingress_or_broker_or_live_modified",
    "shared_ws_or_local_agg_or_service_started",
    "orders_cancels_redeems_sent",
    "trading_behavior_changed",
)

INPUT_PRE_ACTION_FIELDS = (
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

FEATURE_JOIN_ROW_FIELDS = (
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

BRIDGE_JOINED_ROW_FIELDS = (
    "schema_version",
    "bridge_row_id",
    "split",
    "day_id",
    "condition_id",
    "market_slug",
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
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "post_action_labels_allowed_for_train_holdout_scoring",
    "post_action_labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "trading_behavior_changed",
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

FORBIDDEN_LIVE_FIELD_FRAGMENTS = (
    "source_pair",
    "source_residual",
    "pair_outcome",
    "residual_tail",
    "realized_pair_cost",
    "settlement",
    "redeem",
    "future",
    "private_truth",
    "public_profile",
    "dplus_failed",
)

CSV_SOURCE_ID_FIELDS = {
    "source_sequence_present": "source_sequence_id",
    "quote_intent_present": "quote_intent_id",
    "source_order_present": "source_order_id",
}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


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


def git_ls_files(patterns: list[str]) -> set[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", *patterns],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return set()
    return {line for line in result.stdout.splitlines() if line.strip()}


def discover_files(root: Path, filename: str, explicit: list[Path] | None = None, only_explicit: bool = False) -> list[Path]:
    paths = set(explicit or [])
    if not only_explicit:
        paths.update(Path(path) for path in git_ls_files([f"xuan_research_artifacts/**/{filename}"]))
        base = root / "xuan_research_artifacts"
        if base.exists():
            for path in base.rglob(filename):
                ok, _ = path_safe(path, root)
                if ok:
                    paths.add(path)
    return sorted(paths)


def fixture_like_path(path: Path) -> bool:
    lowered = "/".join(path.parts).lower()
    markers = (
        "_smoke_",
        "/fixtures/",
        "/fixture/",
        "/good/",
        "/bad",
        "fixture_scorer",
        "real_input_inventory_smoke",
        "feature_join_runner_instrumentation_smoke",
        "feature_join_output_scorer_smoke",
    )
    return any(marker in lowered for marker in markers)


def source_path(manifest: dict[str, Any], source_name: str) -> Path | None:
    sources = as_dict(manifest.get("materialized_sources"))
    source = as_dict(sources.get(source_name))
    path = source.get("path")
    return Path(str(path)) if path else None


def source_fields(manifest: dict[str, Any], source_name: str) -> list[str]:
    sources = as_dict(manifest.get("materialized_sources"))
    source = as_dict(sources.get(source_name))
    return [str(field) for field in as_list(source.get("fields"))]


def source_row_count(manifest: dict[str, Any], source_name: str) -> int:
    sources = as_dict(manifest.get("materialized_sources"))
    source = as_dict(sources.get(source_name))
    return as_int(source.get("row_count"))


def line_count_and_sample(path: Path, sample_limit: int = 20) -> tuple[int, list[dict[str, Any]], str | None]:
    rows: list[dict[str, Any]] = []
    count = 0
    try:
        with path.open() as fh:
            for line_no, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                count += 1
                if len(rows) < sample_limit:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        return count, rows, f"line_{line_no}_not_json_object"
                    rows.append(obj)
    except Exception as exc:
        return count, rows, f"{type(exc).__name__}:{exc}"
    return count, rows, None


def missing_fields(fields: set[str], required: tuple[str, ...]) -> list[str]:
    return sorted(set(required).difference(fields))


def any_forbidden_live_field(fields: list[str]) -> list[str]:
    forbidden: list[str] = []
    for field in fields:
        lower = str(field).lower()
        if any(fragment in lower for fragment in FORBIDDEN_LIVE_FIELD_FRAGMENTS):
            forbidden.append(str(field))
    return sorted(set(forbidden))


def validate_scope(scope: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if scope.get("current_worktree_only") is not True:
        blockers.append("scope_current_worktree_only_not_true")
    if scope.get("local_only") is not True:
        blockers.append("scope_local_only_not_true")
    for field in REQUIRED_SCOPE_FALSE_FIELDS:
        if scope.get(field) is not False:
            blockers.append(f"scope_{field}_not_false")
    return blockers


def observe_supporting_manifest(path: Path, root: Path, expected_decision: str) -> dict[str, Any]:
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


def observe_feature_join_output(candidate_rows_path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    parent = candidate_rows_path.parent
    source_summary_path = parent / "observable_pre_action_source_link_summary.json"
    manifest_path = parent / "observable_pre_action_feature_join_manifest.json"
    blockers: list[str] = []
    for label, path in (
        ("candidate_rows", candidate_rows_path),
        ("source_link_summary", source_summary_path),
        ("feature_join_manifest", manifest_path),
    ):
        ok, reason = path_safe(path, root)
        if not ok:
            blockers.append(f"{label}_{reason}")
        elif not path.exists():
            blockers.append(f"{label}_missing")

    row_count, sample, row_error = (0, [], None)
    fields: set[str] = set()
    if candidate_rows_path.exists():
        row_count, sample, row_error = line_count_and_sample(candidate_rows_path)
        if row_error:
            blockers.append(f"candidate_rows_{row_error}")
        for row in sample:
            fields.update(str(key) for key in row)

    source_summary, source_error = safe_json(source_summary_path, root)
    feature_manifest, feature_error = safe_json(manifest_path, root)
    if source_error:
        blockers.append(f"source_link_summary_{source_error}")
    if feature_error:
        blockers.append(f"feature_join_manifest_{feature_error}")

    field_contract = as_dict(feature_manifest.get("field_contract"))
    fixture = fixture_like_path(candidate_rows_path) or bool(feature_manifest.get("fixture")) or bool(field_contract.get("fixture_only"))
    if fixture:
        blockers.append("feature_join_output_fixture_or_smoke_only")
    if row_count < args.min_feature_join_rows:
        blockers.append("feature_join_rows_below_min")
    missing = missing_fields(fields, FEATURE_JOIN_ROW_FIELDS)
    if missing:
        blockers.append("feature_join_rows_required_pre_action_fields_missing")
    if int(source_summary.get("row_count") or 0) not in (0, row_count):
        blockers.append("source_link_summary_row_count_mismatch")
    if int(feature_manifest.get("candidate_row_count") or 0) not in (0, row_count):
        blockers.append("feature_join_manifest_row_count_mismatch")
    for key in (
        "reads_events_jsonl",
        "raw_replay_or_full_store_scan",
        "post_action_outcome_labels_included",
        "realized_pair_cost_used_as_live_criteria",
        "trading_behavior_changed",
        "strategy_evidence",
        "private_truth_ready",
        "deployable",
        "promotion_gate_passed",
    ):
        if field_contract.get(key) is not False:
            blockers.append(f"field_contract_{key}_not_false")

    return {
        "dir": str(parent),
        "candidate_rows_path": str(candidate_rows_path),
        "source_link_summary_path": str(source_summary_path),
        "feature_join_manifest_path": str(manifest_path),
        "fixture_or_smoke": fixture,
        "row_count": row_count,
        "missing_required_fields_from_sample": missing,
        "ready_as_non_fixture_feature_join": not blockers,
        "blockers": sorted(set(blockers)),
    }


def observe_bridge_joined_rows(joined_rows_path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    parent = joined_rows_path.parent
    summary_path = parent / "observable_pre_action_training_bridge_summary.json"
    blockers: list[str] = []
    for label, path in (("joined_rows", joined_rows_path), ("bridge_summary", summary_path)):
        ok, reason = path_safe(path, root)
        if not ok:
            blockers.append(f"{label}_{reason}")
        elif not path.exists():
            blockers.append(f"{label}_missing")

    row_count, sample, row_error = (0, [], None)
    fields: set[str] = set()
    if joined_rows_path.exists():
        row_count, sample, row_error = line_count_and_sample(joined_rows_path)
        if row_error:
            blockers.append(f"joined_rows_{row_error}")
        for row in sample:
            fields.update(str(key) for key in row)

    summary, summary_error = safe_json(summary_path, root)
    if summary_error:
        blockers.append(f"bridge_summary_{summary_error}")

    fixture = fixture_like_path(joined_rows_path) or bool(summary.get("fixture_only"))
    if fixture:
        blockers.append("training_bridge_joined_rows_fixture_or_smoke_only")
    if row_count < args.min_bridge_joined_rows:
        blockers.append("training_bridge_joined_rows_below_min")
    missing = missing_fields(fields, BRIDGE_JOINED_ROW_FIELDS)
    if missing:
        blockers.append("training_bridge_joined_rows_required_fields_missing")
    if as_int(summary.get("joined_row_count")) not in (0, row_count):
        blockers.append("bridge_summary_joined_row_count_mismatch")
    if as_int(summary.get("label_row_reuse_count")) != 0:
        blockers.append("bridge_summary_label_row_reuse_nonzero")
    if summary.get("post_action_labels_allowed_for_train_holdout_scoring") is not True:
        blockers.append("bridge_summary_train_holdout_labels_not_allowed")
    for field in (
        "post_action_labels_allowed_in_live_predicate",
        "realized_pair_cost_used_as_live_criteria",
        "future_labels_used_as_live_criteria",
        "trading_behavior_changed",
        "strategy_evidence",
        "private_truth_ready",
        "deployable",
    ):
        if summary.get(field) is not False:
            blockers.append(f"bridge_summary_{field}_not_false")
    train_days = {str(day) for day in as_list(summary.get("train_days"))}
    holdout_days = {str(day) for day in as_list(summary.get("holdout_days"))}
    split_counts = as_dict(summary.get("split_counts"))
    if len(train_days) < args.min_train_days:
        blockers.append("training_bridge_train_days_below_min")
    if len(holdout_days) < args.min_holdout_days:
        blockers.append("training_bridge_holdout_days_below_min")
    if as_int(split_counts.get("train")) < args.min_train_rows:
        blockers.append("training_bridge_train_rows_below_min")
    if as_int(split_counts.get("holdout")) < args.min_holdout_rows:
        blockers.append("training_bridge_holdout_rows_below_min")

    return {
        "dir": str(parent),
        "joined_rows_path": str(joined_rows_path),
        "bridge_summary_path": str(summary_path),
        "fixture_or_smoke": fixture,
        "row_count": row_count,
        "train_days": sorted(train_days),
        "holdout_days": sorted(holdout_days),
        "split_counts": split_counts,
        "missing_required_fields_from_sample": missing,
        "ready_as_non_fixture_training_bridge": not blockers,
        "blockers": sorted(set(blockers)),
    }


def observe_denominator_summary(path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    data, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        return {"path": str(path), "exists": path.exists(), "ready_as_non_fixture_denominator": False, "blockers": [f"denominator_{error}"]}
    fixture = fixture_like_path(path) or bool(as_dict(data.get("field_contract")).get("fixture_only"))
    if fixture:
        blockers.append("denominator_summary_fixture_or_smoke_only")
    missing = [field for field in REQUIRED_NO_ORDER_FIELDS if field not in data]
    if missing:
        blockers.append("denominator_required_fields_missing")
    if as_int(data.get("same_window_denominator_count")) < args.min_no_order_denominator:
        blockers.append("same_window_denominator_below_min")
    if as_int(data.get("rule_match_count")) < args.min_rule_matches:
        blockers.append("rule_match_count_below_min")
    if as_float(data.get("source_sequence_coverage")) < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_min")
    if data.get("aggregate_parity") is not True:
        blockers.append("aggregate_parity_not_true")
    promotion = as_dict(data.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("private_deployable_or_promotion_claim_present")
    return {
        "path": str(path),
        "fixture_or_smoke": fixture,
        "frozen_rule_id": data.get("frozen_rule_id"),
        "same_window_denominator_count": as_int(data.get("same_window_denominator_count")),
        "rule_match_count": as_int(data.get("rule_match_count")),
        "source_sequence_coverage": as_float(data.get("source_sequence_coverage")),
        "aggregate_parity": data.get("aggregate_parity"),
        "ready_as_non_fixture_denominator": not blockers,
        "blockers": sorted(set(blockers)),
    }


def observe_frozen_rule(path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    data, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        return {"path": str(path), "exists": path.exists(), "ready_as_non_fixture_frozen_rule": False, "blockers": [f"frozen_rule_{error}"]}
    fixture = fixture_like_path(path) or bool(as_dict(data.get("stability_gates")).get("fixture_only"))
    if fixture:
        blockers.append("frozen_rule_fixture_or_smoke_only")
    if data.get("schema_version") != "frozen_observable_pre_action_rule_manifest_v1":
        blockers.append("frozen_rule_schema_mismatch")
    predicate_fields = [str(field) for field in as_list(data.get("predicate_feature_names"))]
    if not predicate_fields:
        predicate = as_dict(data.get("predicate"))
        predicate_fields = [str(item.get("field")) for item in as_list(predicate.get("all")) if isinstance(item, dict)]
    if not predicate_fields:
        blockers.append("frozen_rule_predicate_fields_missing")
    forbidden = any_forbidden_live_field(predicate_fields)
    if forbidden:
        blockers.append("frozen_rule_forbidden_live_fields_present")
    if data.get("uses_only_pre_action_fields") is not True:
        blockers.append("frozen_rule_not_pre_action_only")
    if data.get("uses_realized_pair_cost") is not False:
        blockers.append("frozen_rule_realized_pair_cost_not_false")
    if data.get("uses_future_labels") is not False:
        blockers.append("frozen_rule_future_labels_not_false")
    if data.get("private_truth_ready") is True or data.get("deployable") is True or as_dict(data.get("promotion_gate")).get("passed") is True:
        blockers.append("private_deployable_or_promotion_claim_present")

    metrics = as_dict(data.get("objective_metrics"))
    gates = as_dict(data.get("stability_gates"))
    if as_int(metrics.get("train_rows")) < args.min_train_selected_rows:
        blockers.append("frozen_rule_train_rows_below_min")
    if as_int(metrics.get("holdout_rows")) < args.min_holdout_selected_rows:
        blockers.append("frozen_rule_holdout_rows_below_min")
    if as_float(metrics.get("train_objective")) <= 0:
        blockers.append("frozen_rule_train_objective_not_positive")
    if as_float(metrics.get("holdout_objective")) <= 0:
        blockers.append("frozen_rule_holdout_objective_not_positive")
    if gates.get("static_side_offset_public_price_cap") is True:
        blockers.append("frozen_rule_static_side_offset_public_price_cap")
    split = as_dict(data.get("train_holdout_split"))
    train_days = {str(day) for day in as_list(split.get("train_days"))}
    holdout_days = {str(day) for day in as_list(split.get("holdout_days"))}
    if len(train_days) < args.min_train_days:
        blockers.append("frozen_rule_train_days_below_min")
    if len(holdout_days) < args.min_holdout_days:
        blockers.append("frozen_rule_holdout_days_below_min")
    if train_days.intersection(holdout_days):
        blockers.append("frozen_rule_train_holdout_days_overlap")
    same_window = as_dict(data.get("same_window_scope"))
    if same_window.get("requires_denominator_replay") is not True:
        blockers.append("frozen_rule_denominator_replay_not_required")
    return {
        "path": str(path),
        "fixture_or_smoke": fixture,
        "frozen_rule_id": data.get("frozen_rule_id"),
        "predicate_expression": data.get("predicate_expression"),
        "predicate_feature_names": predicate_fields,
        "forbidden_live_fields_present": forbidden,
        "objective_metrics": metrics,
        "train_days": sorted(train_days),
        "holdout_days": sorted(holdout_days),
        "ready_as_non_fixture_frozen_rule": not blockers,
        "blockers": sorted(set(blockers)),
    }


def observe_input_manifest(path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    data, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        return {"path": str(path), "exists": path.exists(), "non_fixture_ready": False, "blockers": [f"input_manifest_{error}"]}
    fixture = bool(data.get("fixture")) or fixture_like_path(path)
    if fixture:
        blockers.append("input_manifest_fixture_or_smoke_only")
    if data.get("schema_version") != INPUT_SCHEMA:
        blockers.append("schema_version_mismatch")
    blockers.extend(validate_scope(as_dict(data.get("scope"))))
    if data.get("strategy_evidence") is True:
        blockers.append("candidate_manifest_claims_strategy_evidence")

    for source_name in ("candidate_rows", "source_link_summary", "no_order_denominator_summary"):
        src = source_path(data, source_name)
        if src is None:
            blockers.append(f"{source_name}_path_missing")
            continue
        ok, reason = path_safe(src, root)
        if not ok:
            blockers.append(f"{source_name}_{reason}")
        elif not src.exists():
            blockers.append(f"{source_name}_path_missing_on_disk")
        elif fixture_like_path(src):
            blockers.append(f"{source_name}_fixture_or_smoke_path")

    candidate_fields = set(source_fields(data, "candidate_rows"))
    no_order_fields = set(source_fields(data, "no_order_denominator_summary"))
    missing_pre = missing_fields(candidate_fields, INPUT_PRE_ACTION_FIELDS)
    missing_labels = missing_fields(candidate_fields, OFFLINE_LABEL_FIELDS)
    missing_no_order = missing_fields(no_order_fields, REQUIRED_NO_ORDER_FIELDS)
    if missing_pre:
        blockers.append("candidate_rows_required_pre_action_fields_missing")
    if missing_labels:
        blockers.append("candidate_rows_offline_label_fields_missing")
    if missing_no_order:
        blockers.append("no_order_denominator_required_fields_missing")
    if source_row_count(data, "candidate_rows") < args.min_candidate_rows:
        blockers.append("candidate_rows_below_min")

    feature_policy = as_dict(data.get("feature_policy"))
    allowed_live_fields = [str(field) for field in as_list(feature_policy.get("allowed_live_rule_fields"))]
    offline_label_fields = [str(field) for field in as_list(feature_policy.get("offline_label_fields"))]
    forbidden_live = any_forbidden_live_field(allowed_live_fields)
    if forbidden_live:
        blockers.append("forbidden_fields_in_allowed_live_rule_fields")
    if not set(OFFLINE_LABEL_FIELDS).issubset(set(offline_label_fields)):
        blockers.append("offline_label_fields_do_not_cover_required_labels")

    frozen = as_dict(data.get("frozen_rule_contract"))
    predicate_fields = [str(field) for field in as_list(frozen.get("predicate_fields"))]
    if not predicate_fields:
        blockers.append("frozen_rule_predicate_fields_missing")
    if any_forbidden_live_field(predicate_fields):
        blockers.append("frozen_rule_uses_forbidden_live_field")
    if not set(predicate_fields).issubset(set(allowed_live_fields)):
        blockers.append("frozen_rule_uses_fields_outside_allowed_live_fields")
    if frozen.get("uses_only_pre_action_fields") is not True:
        blockers.append("frozen_rule_not_pre_action_only")
    if frozen.get("uses_realized_pair_cost") is not False:
        blockers.append("frozen_rule_realized_pair_cost_not_false")
    if frozen.get("uses_future_labels") is not False:
        blockers.append("frozen_rule_future_labels_not_false")
    if frozen.get("train_holdout_gate_passed") is not True:
        blockers.append("train_holdout_gate_not_passed")
    if as_int(frozen.get("train_selected_rows")) < args.min_train_selected_rows:
        blockers.append("train_selected_rows_below_min")
    if as_int(frozen.get("holdout_selected_rows")) < args.min_holdout_selected_rows:
        blockers.append("holdout_selected_rows_below_min")

    split = as_dict(data.get("train_holdout_split"))
    train_days = {str(day) for day in as_list(split.get("train_days"))}
    holdout_days = {str(day) for day in as_list(split.get("holdout_days"))}
    if len(train_days) < args.min_train_days:
        blockers.append("train_days_below_min")
    if len(holdout_days) < args.min_holdout_days:
        blockers.append("holdout_days_below_min")
    if train_days.intersection(holdout_days):
        blockers.append("train_holdout_days_overlap")

    denom_gate = as_dict(data.get("no_order_denominator_gate"))
    if denom_gate.get("same_window_denominator_reproduced") is not True:
        blockers.append("same_window_denominator_not_reproduced")
    if as_int(denom_gate.get("same_window_denominator_count")) < args.min_no_order_denominator:
        blockers.append("same_window_denominator_below_min")
    if as_float(denom_gate.get("source_sequence_coverage")) < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_min")
    if denom_gate.get("aggregate_parity") is not True:
        blockers.append("aggregate_parity_not_true")

    supporting = as_dict(data.get("supporting_materialized_sources"))
    supporting_paths = {
        key: Path(str(value.get("path")))
        for key, value in supporting.items()
        if isinstance(value, dict) and value.get("path")
    }
    for source_name in (
        "feature_join_candidate_rows",
        "feature_join_manifest",
        "training_bridge_joined_rows",
        "training_bridge_summary",
        "frozen_rule_manifest",
    ):
        src = supporting_paths.get(source_name)
        if src is None:
            blockers.append(f"supporting_{source_name}_path_missing")
            continue
        ok, reason = path_safe(src, root)
        if not ok:
            blockers.append(f"supporting_{source_name}_{reason}")
        elif not src.exists():
            blockers.append(f"supporting_{source_name}_missing_on_disk")
        elif fixture_like_path(src):
            blockers.append(f"supporting_{source_name}_fixture_or_smoke_path")

    promotion = as_dict(data.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("private_deployable_or_promotion_claim_present")

    return {
        "path": str(path),
        "fixture_or_smoke": fixture,
        "schema_version": data.get("schema_version"),
        "decision_label": data.get("decision_label"),
        "candidate_row_count": source_row_count(data, "candidate_rows"),
        "same_window_denominator_count": as_int(denom_gate.get("same_window_denominator_count")),
        "source_sequence_coverage": as_float(denom_gate.get("source_sequence_coverage")),
        "missing_pre_action_fields": missing_pre,
        "missing_offline_label_fields": missing_labels,
        "missing_no_order_fields": missing_no_order,
        "forbidden_live_fields_present": forbidden_live,
        "non_fixture_ready": not blockers,
        "blockers": sorted(set(blockers)),
    }


def observe_candidate_csv(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {"path": str(path), "exists": path.exists(), "safe": False, "blockers": [str(reason)]}
    if not path.exists():
        return {"path": str(path), "exists": False, "safe": True, "blockers": ["candidate_csv_missing"]}
    nonempty_source_ids = {field: 0 for field in CSV_SOURCE_ID_FIELDS.values()}
    row_count = 0
    days: set[str] = set()
    fields: list[str] = []
    external_shadow_ids_available_true = 0
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        for row in reader:
            row_count += 1
            if row.get("day"):
                days.add(str(row["day"]))
            if str(row.get("external_shadow_ids_available", "")).lower() == "true":
                external_shadow_ids_available_true += 1
            for field in nonempty_source_ids:
                if row.get(field):
                    nonempty_source_ids[field] += 1
    field_set = set(fields)
    missing_pre = [
        field
        for field in INPUT_PRE_ACTION_FIELDS
        if field not in field_set and field not in {"day_id", "quote_ts_ms", "open_qty_bucket", "deficit_bucket", "candidate_qty_bucket"}
    ]
    missing_labels = [field for field in OFFLINE_LABEL_FIELDS if field not in field_set]
    source_presence_blockers = []
    if external_shadow_ids_available_true == 0:
        source_presence_blockers.append("external_shadow_ids_available_never_true")
    for source_id, count in nonempty_source_ids.items():
        if count <= 0:
            source_presence_blockers.append(f"{source_id}_empty")
    blockers = []
    if missing_pre:
        blockers.append("candidate_csv_required_pre_action_fields_missing")
    if missing_labels:
        blockers.append("candidate_csv_offline_label_fields_missing")
    if source_presence_blockers:
        blockers.append("candidate_csv_source_presence_ids_not_real_truth")
    blockers.append("candidate_csv_lacks_status_block_reason_feature_join_and_same_window_denominator")
    return {
        "path": str(path),
        "exists": True,
        "safe": True,
        "row_count": row_count,
        "day_count": len(days),
        "days_sample": sorted(days)[:8],
        "field_count": len(fields),
        "nonempty_source_ids": nonempty_source_ids,
        "external_shadow_ids_available_true": external_shadow_ids_available_true,
        "missing_pre_action_fields": missing_pre,
        "missing_offline_label_fields": missing_labels,
        "ready_as_non_fixture_training_input": False,
        "blockers": blockers,
    }


def observe_pullback_dirs(root: Path) -> dict[str, Any]:
    base = root / "xuan_research_artifacts"
    dirs: set[Path] = set()
    if base.exists():
        for aggregate in base.rglob("remote_clean/output/aggregate_report.json"):
            ok, _ = path_safe(aggregate, root)
            if ok:
                dirs.add(aggregate.parent)
    summaries = []
    for directory in sorted(dirs):
        summaries.append(
            {
                "output_dir": str(directory),
                "summary_json_count": len(list(directory.glob("*.summary.json"))),
                "aggregate_report_present": True,
                "observable_pre_action_candidate_rows_present": (directory / "observable_pre_action_candidate_rows.jsonl").exists(),
                "observable_pre_action_source_link_summary_present": (directory / "observable_pre_action_source_link_summary.json").exists(),
                "observable_pre_action_feature_join_manifest_present": (directory / "observable_pre_action_feature_join_manifest.json").exists(),
                "observable_pre_action_training_bridge_joined_rows_present": (
                    directory / "observable_pre_action_training_bridge_joined_rows.jsonl"
                ).exists(),
                "observable_pre_action_no_order_denominator_summary_present": (
                    directory / "observable_pre_action_no_order_denominator_summary.json"
                ).exists(),
            }
        )
    return {
        "allowlisted_pullback_output_dir_count": len(summaries),
        "dirs_with_feature_join_outputs": sum(
            1
            for item in summaries
            if item["observable_pre_action_candidate_rows_present"]
            and item["observable_pre_action_source_link_summary_present"]
            and item["observable_pre_action_feature_join_manifest_present"]
        ),
        "dirs_with_complete_training_chain": sum(
            1
            for item in summaries
            if item["observable_pre_action_candidate_rows_present"]
            and item["observable_pre_action_source_link_summary_present"]
            and item["observable_pre_action_feature_join_manifest_present"]
            and item["observable_pre_action_training_bridge_joined_rows_present"]
            and item["observable_pre_action_no_order_denominator_summary_present"]
        ),
        "sample": summaries[:15],
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    supporting = {
        "spec": observe_supporting_manifest(args.spec_manifest, root, SUPPORTING_DECISIONS["spec"]),
        "feature_join_scorer": observe_supporting_manifest(
            args.feature_join_scorer_manifest, root, SUPPORTING_DECISIONS["feature_join_scorer"]
        ),
        "denominator_fixture_scorer": observe_supporting_manifest(
            args.denominator_fixture_scorer_manifest, root, SUPPORTING_DECISIONS["denominator_fixture_scorer"]
        ),
        "bridge_fixture_scorer": observe_supporting_manifest(
            args.bridge_scorer_manifest, root, SUPPORTING_DECISIONS["bridge_fixture_scorer"]
        ),
        "training_spec": observe_supporting_manifest(args.training_spec_manifest, root, SUPPORTING_DECISIONS["training_spec"]),
        "training_fixture_scorer": observe_supporting_manifest(
            args.training_fixture_scorer_manifest, root, SUPPORTING_DECISIONS["training_fixture_scorer"]
        ),
    }

    explicit_inputs = args.candidate_input_manifest or []
    input_paths = discover_files(
        root,
        "observable_pre_action_rule_miner_input_manifest.json",
        explicit=explicit_inputs,
        only_explicit=args.only_explicit_inputs,
    )
    feature_paths = discover_files(
        root,
        "observable_pre_action_candidate_rows.jsonl",
        explicit=args.feature_join_candidate_rows or [],
    )
    bridge_paths = discover_files(
        root,
        "observable_pre_action_training_bridge_joined_rows.jsonl",
        explicit=args.training_bridge_joined_rows or [],
    )
    denom_paths = discover_files(
        root,
        "observable_pre_action_no_order_denominator_summary.json",
        explicit=args.denominator_summary or [],
    )
    frozen_paths = discover_files(
        root,
        "frozen_observable_pre_action_rule_manifest.json",
        explicit=args.frozen_rule_manifest or [],
    )

    input_observations = [observe_input_manifest(path, root, args) for path in input_paths]
    feature_observations = [observe_feature_join_output(path, root, args) for path in feature_paths]
    bridge_observations = [observe_bridge_joined_rows(path, root, args) for path in bridge_paths]
    denom_observations = [observe_denominator_summary(path, root, args) for path in denom_paths]
    frozen_observations = [observe_frozen_rule(path, root, args) for path in frozen_paths]

    ready_inputs = [item for item in input_observations if item.get("non_fixture_ready")]
    ready_features = [item for item in feature_observations if item.get("ready_as_non_fixture_feature_join")]
    ready_bridges = [item for item in bridge_observations if item.get("ready_as_non_fixture_training_bridge")]
    ready_denoms = [item for item in denom_observations if item.get("ready_as_non_fixture_denominator")]
    ready_frozen = [item for item in frozen_observations if item.get("ready_as_non_fixture_frozen_rule")]
    complete_component_chain_ready = bool(ready_features and ready_bridges and ready_denoms and ready_frozen)
    supporting_ready = all(item.get("ready") for item in supporting.values())

    candidate_csv = observe_candidate_csv(args.candidate_csv, root)
    pullbacks = observe_pullback_dirs(root)

    blockers: list[str] = []
    if not supporting_ready:
        blockers.append("supporting_contract_or_scorer_not_ready")
    if not ready_inputs:
        blockers.append("non_fixture_observable_pre_action_input_manifest_absent")
    if not ready_features:
        blockers.append("non_fixture_feature_join_outputs_absent_or_incomplete")
    if not ready_bridges:
        blockers.append("non_fixture_training_bridge_joined_rows_absent_or_incomplete")
    if not ready_denoms:
        blockers.append("non_fixture_denominator_summary_absent")
    if not ready_frozen:
        blockers.append("non_fixture_frozen_rule_manifest_absent_or_incomplete")
    if not candidate_csv.get("ready_as_non_fixture_training_input"):
        blockers.append("legacy_candidate_csv_not_assemble_ready")
    if as_int(pullbacks.get("dirs_with_complete_training_chain")) <= 0:
        blockers.append("allowlisted_pullbacks_lack_complete_observable_pre_action_chain")

    ready = bool(ready_inputs) and complete_component_chain_ready and supporting_ready
    if ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_READY"
        next_action = (
            "Run scripts/xuan_observable_pre_action_rule_miner_spec.py on the named non-fixture input manifest, "
            "then run the local observable pre-action rule miner only if the contract stays KEEP; do not start "
            "no-order diagnostics automatically."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_GAPS"
        next_action = (
            "Wait for a future allowlisted feature-join pullback or implement a local-only safe pullback handoff spec; "
            "current worktree still lacks non-fixture feature-join, bridge-joined labels, denominator, frozen rule, "
            "or input manifest for real mining. Do not start no-order diagnostics automatically."
        )

    validation_commands = [
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_spec.py "
            f"--candidate-input-manifest {item['path']} "
            f"--output-dir {args.output_dir / ('validation_' + str(idx))}"
        )
        for idx, item in enumerate(ready_inputs, start=1)
    ]

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_real_input_inventory_v2",
        "source_inputs": {
            "spec_manifest": str(args.spec_manifest),
            "feature_join_scorer_manifest": str(args.feature_join_scorer_manifest),
            "denominator_fixture_scorer_manifest": str(args.denominator_fixture_scorer_manifest),
            "bridge_scorer_manifest": str(args.bridge_scorer_manifest),
            "training_spec_manifest": str(args.training_spec_manifest),
            "training_fixture_scorer_manifest": str(args.training_fixture_scorer_manifest),
            "candidate_csv": str(args.candidate_csv),
            "explicit_candidate_input_manifests": [str(path) for path in explicit_inputs],
            "explicit_feature_join_candidate_rows": [str(path) for path in args.feature_join_candidate_rows or []],
            "explicit_training_bridge_joined_rows": [str(path) for path in args.training_bridge_joined_rows or []],
            "explicit_denominator_summaries": [str(path) for path in args.denominator_summary or []],
            "explicit_frozen_rule_manifests": [str(path) for path in args.frozen_rule_manifest or []],
        },
        "readiness": {
            "ready": ready,
            "supporting_ready": supporting_ready,
            "complete_component_chain_ready": complete_component_chain_ready,
            "ready_input_manifests": ready_inputs,
            "ready_feature_join_outputs": ready_features,
            "ready_training_bridge_outputs": ready_bridges,
            "ready_denominator_summaries": ready_denoms,
            "ready_frozen_rules": ready_frozen,
            "validation_commands": validation_commands,
            "blockers": sorted(set(blockers)),
        },
        "observations": {
            "supporting_contracts_and_scorers": supporting,
            "input_manifest_count": len(input_observations),
            "input_manifests": input_observations[:30],
            "feature_join_output_count": len(feature_observations),
            "feature_join_outputs": feature_observations[:30],
            "training_bridge_output_count": len(bridge_observations),
            "training_bridge_outputs": bridge_observations[:30],
            "denominator_summary_count": len(denom_observations),
            "denominator_summaries": denom_observations[:30],
            "frozen_rule_count": len(frozen_observations),
            "frozen_rules": frozen_observations[:30],
            "legacy_candidate_csv": candidate_csv,
            "allowlisted_pullbacks": pullbacks,
        },
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "real_input_ready": ready,
            "complete_component_chain_ready": complete_component_chain_ready,
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "Inventory only. KEEP would name non-fixture input paths and validation commands; UNKNOWN means "
                "the local chain still lacks safe real materialized inputs. Neither state is private truth, "
                "deployable evidence, or promotion evidence."
            ),
        },
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "real_input_inventory_only",
        },
        "safety": {
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scan": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": next_action,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec-manifest", type=Path, default=DEFAULT_SPEC_MANIFEST)
    parser.add_argument("--feature-join-scorer-manifest", type=Path, default=DEFAULT_FEATURE_JOIN_SCORER_MANIFEST)
    parser.add_argument("--denominator-fixture-scorer-manifest", type=Path, default=DEFAULT_DENOMINATOR_FIXTURE_SCORER_MANIFEST)
    parser.add_argument("--bridge-scorer-manifest", type=Path, default=DEFAULT_BRIDGE_SCORER_MANIFEST)
    parser.add_argument("--training-spec-manifest", type=Path, default=DEFAULT_TRAINING_SPEC_MANIFEST)
    parser.add_argument("--training-fixture-scorer-manifest", type=Path, default=DEFAULT_TRAINING_FIXTURE_SCORER_MANIFEST)
    parser.add_argument("--candidate-csv", type=Path, default=DEFAULT_CANDIDATE_CSV)
    parser.add_argument("--candidate-input-manifest", type=Path, action="append")
    parser.add_argument("--feature-join-candidate-rows", type=Path, action="append")
    parser.add_argument("--training-bridge-joined-rows", type=Path, action="append")
    parser.add_argument("--denominator-summary", type=Path, action="append")
    parser.add_argument("--frozen-rule-manifest", type=Path, action="append")
    parser.add_argument("--only-explicit-inputs", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--min-candidate-rows", type=int, default=1000)
    parser.add_argument("--min-feature-join-rows", type=int, default=100)
    parser.add_argument("--min-bridge-joined-rows", type=int, default=100)
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=100)
    parser.add_argument("--min-holdout-rows", type=int, default=50)
    parser.add_argument("--min-train-selected-rows", type=int, default=100)
    parser.add_argument("--min-holdout-selected-rows", type=int, default=50)
    parser.add_argument("--min-no-order-denominator", type=int, default=100)
    parser.add_argument("--min-rule-matches", type=int, default=20)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
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
