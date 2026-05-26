#!/usr/bin/env python3
"""Specify and validate safe feature-join pullback handoffs.

This local-only handoff defines the exact files a future safe pullback must
place inside the current worktree before observable pre-action rule mining can
continue. It can also validate an explicit handoff manifest. It does not read
events JSONL, raw/replay stores, use SSH, start shadows/services, or change
trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff"
CONTRACT_NAME = "observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_v1"
HANDOFF_SCHEMA = "observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_v1"

DEFAULT_REAL_INPUT_V2 = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_real_input_inventory_v2_20260525T233507Z/"
    "manifest.json"
)
DEFAULT_FEATURE_JOIN_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z/"
    "manifest.json"
)
DEFAULT_BRIDGE_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/"
    "manifest.json"
)
DEFAULT_TRAINING_FIXTURE_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_fixture_scorer_20260525T230507Z/"
    "manifest.json"
)
DEFAULT_DENOMINATOR_FIXTURE_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_20260525T202536Z/"
    "manifest.json"
)

SUPPORTING_DECISIONS = {
    "real_input_inventory_v2": "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_V2_GAPS",
    "feature_join_scorer": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY",
    "bridge_fixture_scorer": "KEEP_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_READY",
    "training_fixture_scorer": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_FIXTURE_SCORER_READY",
    "denominator_fixture_scorer": "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY",
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

OFFLINE_LABEL_CSV_FIELDS = (
    "condition_id",
    "slug",
    "day",
    "ts_ms",
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
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)

NO_ORDER_DENOMINATOR_FIELDS = (
    "frozen_rule_id",
    "same_window_denominator_count",
    "rule_match_count",
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

REQUIRED_FILE_KEYS = (
    "feature_join_candidate_rows",
    "feature_join_source_link_summary",
    "feature_join_manifest",
    "same_window_aggregate_summary",
    "offline_label_csv",
    "training_bridge_joined_rows",
    "training_bridge_summary",
    "frozen_rule_manifest",
    "no_order_denominator_summary",
    "observable_pre_action_rule_miner_input_manifest",
)


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


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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
        "/fixtures/",
        "/fixture/",
        "/good/",
        "/bad",
        "fixture_scorer",
        "real_input_inventory_smoke",
    )
    return any(marker in lowered for marker in markers)


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


def forbidden_live_fields(fields: list[str]) -> list[str]:
    found: list[str] = []
    for field in fields:
        lower = field.lower()
        if any(fragment in lower for fragment in FORBIDDEN_LIVE_FIELD_FRAGMENTS):
            found.append(field)
    return sorted(set(found))


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


def handoff_spec(validation_root: Path) -> dict[str, Any]:
    pullback_required = {
        "feature_join_candidate_rows": "output/observable_pre_action_candidate_rows.jsonl",
        "feature_join_source_link_summary": "output/observable_pre_action_source_link_summary.json",
        "feature_join_manifest": "output/observable_pre_action_feature_join_manifest.json",
        "same_window_aggregate_summary": "output/same_window_aggregate_summary.json",
        "offline_label_csv": "xuan_research_artifacts/.../candidate_seed_outcome_separator.csv or a newer in-worktree label CSV",
    }
    local_derived = {
        "training_bridge_joined_rows": "output/observable_pre_action_training_bridge_joined_rows.jsonl",
        "training_bridge_summary": "output/observable_pre_action_training_bridge_summary.json",
        "frozen_rule_manifest": "output/frozen_observable_pre_action_rule_manifest.json",
        "no_order_denominator_summary": "output/observable_pre_action_no_order_denominator_summary.json",
        "observable_pre_action_rule_miner_input_manifest": "output/observable_pre_action_rule_miner_input_manifest.json",
    }
    return {
        "schema_version": HANDOFF_SCHEMA,
        "required_pullback_files": pullback_required,
        "required_local_derived_files": local_derived,
        "required_provenance": {
            "current_worktree_only": True,
            "pullback_already_inside_current_worktree": True,
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scanned": False,
            "external_worktree_read": False,
            "ssh_used_by_validator": False,
            "trading_behavior_changed": False,
            "orders_cancels_redeems_sent": False,
        },
        "validation_order": [
            "feature_join_output_scorer",
            "training_bridge_joined_rows_or_bridge_scorer",
            "offline_training_scorer",
            "denominator_replay_scorer",
            "observable_pre_action_rule_miner_spec",
            "real_input_inventory_v2",
        ],
        "validation_commands_template": validation_commands_template(validation_root),
        "fail_closed_conditions": [
            "handoff_manifest_missing_or_wrong_schema",
            "path_outside_current_worktree",
            "forbidden_path_fragment_or_events_jsonl",
            "fixture_or_smoke_path_as_real_input",
            "missing_required_pullback_or_derived_file",
            "feature_join_schema_or_row_field_missing",
            "bridge_joined_rows_schema_or_label_separation_failed",
            "offline_label_csv_missing_join_or_label_fields",
            "frozen_rule_uses_forbidden_live_field",
            "denominator_sample_or_source_coverage_below_min",
            "input_manifest_missing_or_claims_strategy_promotion",
            "private_truth_or_deployable_or_promotion_claim_present",
        ],
    }


def validation_commands_template(validation_root: Path) -> list[str]:
    return [
        (
            "python3 scripts/xuan_observable_pre_action_feature_join_output_scorer.py "
            "--candidate-rows ${feature_join_candidate_rows} "
            "--source-link-summary ${feature_join_source_link_summary} "
            "--feature-join-manifest ${feature_join_manifest} "
            f"--output-dir {validation_root}/01_feature_join_scorer"
        ),
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer.py "
            "--feature-rows ${feature_join_candidate_rows} "
            "--source-link-summary ${feature_join_source_link_summary} "
            "--feature-join-manifest ${feature_join_manifest} "
            "--offline-label-csv ${offline_label_csv} "
            "--frozen-rule-manifest ${frozen_rule_manifest} "
            f"--output-dir {validation_root}/02_training_bridge_or_joined_rows"
        ),
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py "
            "--joined-rows ${training_bridge_joined_rows} "
            f"--output-dir {validation_root}/03_training_scorer"
        ),
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer.py "
            "--candidate-rows ${feature_join_candidate_rows} "
            "--source-link-summary ${feature_join_source_link_summary} "
            "--feature-join-manifest ${feature_join_manifest} "
            "--frozen-rule-manifest ${frozen_rule_manifest} "
            "--same-window-aggregate-summary ${same_window_aggregate_summary} "
            f"--output-dir {validation_root}/04_denominator_replay"
        ),
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_spec.py "
            "--candidate-input-manifest ${observable_pre_action_rule_miner_input_manifest} "
            f"--output-dir {validation_root}/05_miner_spec"
        ),
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_real_input_inventory_v2.py "
            "--candidate-input-manifest ${observable_pre_action_rule_miner_input_manifest} "
            "--feature-join-candidate-rows ${feature_join_candidate_rows} "
            "--training-bridge-joined-rows ${training_bridge_joined_rows} "
            "--denominator-summary ${no_order_denominator_summary} "
            "--frozen-rule-manifest ${frozen_rule_manifest} "
            f"--output-dir {validation_root}/06_real_input_inventory_v2"
        ),
    ]


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


def load_required_path(files: dict[str, Any], key: str, root: Path) -> tuple[Path | None, list[str]]:
    raw = files.get(key)
    blockers: list[str] = []
    if not raw:
        return None, [f"{key}_path_missing"]
    path = Path(str(raw))
    ok, reason = path_safe(path, root)
    if not ok:
        blockers.append(f"{key}_{reason}")
    elif not path.exists():
        blockers.append(f"{key}_missing_on_disk")
    elif fixture_like_path(path):
        blockers.append(f"{key}_fixture_or_smoke_path")
    return path, blockers


def validate_jsonl_rows(path: Path, required: tuple[str, ...], schema: str, label: str, min_rows: int) -> dict[str, Any]:
    row_count, sample, error = line_count_and_sample(path)
    blockers: list[str] = []
    fields: set[str] = set()
    for row in sample:
        fields.update(str(key) for key in row)
        if row.get("schema_version") != schema:
            blockers.append(f"{label}_schema_mismatch")
    if error:
        blockers.append(f"{label}_{error}")
    if row_count < min_rows:
        blockers.append(f"{label}_rows_below_min")
    missing = missing_fields(fields, required)
    if missing:
        blockers.append(f"{label}_required_fields_missing")
    if label == "training_bridge_joined_rows":
        for idx, row in enumerate(sample):
            if row.get("post_action_labels_allowed_for_train_holdout_scoring") is not True:
                blockers.append(f"{label}_row_{idx}_train_holdout_labels_not_allowed")
            for field in (
                "post_action_labels_allowed_in_live_predicate",
                "realized_pair_cost_used_as_live_criteria",
                "future_labels_used_as_live_criteria",
                "trading_behavior_changed",
            ):
                if row.get(field) is not False:
                    blockers.append(f"{label}_row_{idx}_{field}_not_false")
    return {
        "path": str(path),
        "row_count": row_count,
        "missing_fields": missing,
        "ready": not blockers,
        "blockers": sorted(set(blockers)),
    }


def validate_csv(path: Path) -> dict[str, Any]:
    blockers: list[str] = []
    row_count = 0
    fields: list[str] = []
    try:
        with path.open(newline="") as fh:
            reader = csv.DictReader(fh)
            fields = list(reader.fieldnames or [])
            for _ in reader:
                row_count += 1
    except Exception as exc:
        blockers.append(f"offline_label_csv_{type(exc).__name__}:{exc}")
    missing = missing_fields(set(fields), OFFLINE_LABEL_CSV_FIELDS)
    if missing:
        blockers.append("offline_label_csv_required_fields_missing")
    if row_count <= 0:
        blockers.append("offline_label_csv_empty")
    return {
        "path": str(path),
        "row_count": row_count,
        "missing_fields": missing,
        "ready": not blockers,
        "blockers": sorted(set(blockers)),
    }


def validate_json_file(path: Path, root: Path, label: str, expected_schema: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    data, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        blockers.append(f"{label}_{error}")
    if expected_schema and data.get("schema_version") != expected_schema:
        blockers.append(f"{label}_schema_mismatch")
    return data, {
        "path": str(path),
        "schema_version": data.get("schema_version"),
        "ready": not blockers,
        "blockers": blockers,
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
    blockers.extend(validate_scope(as_dict(handoff.get("scope"))))
    provenance = as_dict(handoff.get("provenance"))
    if provenance.get("pullback_already_inside_current_worktree") is not True:
        blockers.append("pullback_not_declared_inside_current_worktree")
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

    files = as_dict(handoff.get("files"))
    paths: dict[str, Path] = {}
    for key in REQUIRED_FILE_KEYS:
        path, path_blockers = load_required_path(files, key, root)
        blockers.extend(path_blockers)
        if path is not None:
            paths[key] = path

    if "feature_join_candidate_rows" in paths:
        obs = validate_jsonl_rows(
            paths["feature_join_candidate_rows"],
            FEATURE_JOIN_ROW_FIELDS,
            "observable_pre_action_candidate_row_v1",
            "feature_join_candidate_rows",
            args.min_feature_join_rows,
        )
        observations["feature_join_candidate_rows"] = obs
        blockers.extend(obs["blockers"])
    if "training_bridge_joined_rows" in paths:
        obs = validate_jsonl_rows(
            paths["training_bridge_joined_rows"],
            BRIDGE_JOINED_ROW_FIELDS,
            "observable_pre_action_training_bridge_joined_row_v1",
            "training_bridge_joined_rows",
            args.min_bridge_joined_rows,
        )
        observations["training_bridge_joined_rows"] = obs
        blockers.extend(obs["blockers"])
    if "offline_label_csv" in paths:
        obs = validate_csv(paths["offline_label_csv"])
        observations["offline_label_csv"] = obs
        blockers.extend(obs["blockers"])

    json_specs = {
        "feature_join_source_link_summary": "observable_pre_action_source_link_summary_v1",
        "feature_join_manifest": "observable_pre_action_feature_join_manifest_v1",
        "training_bridge_summary": "observable_pre_action_training_bridge_summary_v1",
        "no_order_denominator_summary": "observable_pre_action_no_order_denominator_summary_v1",
        "observable_pre_action_rule_miner_input_manifest": "observable_pre_action_rule_miner_input_v1",
    }
    loaded_json: dict[str, dict[str, Any]] = {}
    for key, schema in json_specs.items():
        if key in paths:
            data, obs = validate_json_file(paths[key], root, key, schema)
            loaded_json[key] = data
            observations[key] = obs
            blockers.extend(obs["blockers"])

    if "same_window_aggregate_summary" in paths:
        data, obs = validate_json_file(paths["same_window_aggregate_summary"], root, "same_window_aggregate_summary")
        loaded_json["same_window_aggregate_summary"] = data
        observations["same_window_aggregate_summary"] = obs
        blockers.extend(obs["blockers"])

    if "frozen_rule_manifest" in paths:
        frozen, obs = validate_json_file(
            paths["frozen_rule_manifest"],
            root,
            "frozen_rule_manifest",
            "frozen_observable_pre_action_rule_manifest_v1",
        )
        predicate_fields = [str(field) for field in as_list(frozen.get("predicate_feature_names"))]
        if not predicate_fields:
            predicate_fields = [
                str(item.get("field"))
                for item in as_list(as_dict(frozen.get("predicate")).get("all"))
                if isinstance(item, dict)
            ]
        forbidden = forbidden_live_fields(predicate_fields)
        if not predicate_fields:
            obs["blockers"].append("frozen_rule_predicate_fields_missing")
        if forbidden:
            obs["blockers"].append("frozen_rule_forbidden_live_fields_present")
        if frozen.get("uses_only_pre_action_fields") is not True:
            obs["blockers"].append("frozen_rule_not_pre_action_only")
        if frozen.get("uses_realized_pair_cost") is not False:
            obs["blockers"].append("frozen_rule_realized_pair_cost_not_false")
        if frozen.get("uses_future_labels") is not False:
            obs["blockers"].append("frozen_rule_future_labels_not_false")
        if frozen.get("private_truth_ready") is True or frozen.get("deployable") is True:
            obs["blockers"].append("frozen_rule_private_or_deployable_claim")
        if as_dict(frozen.get("promotion_gate")).get("passed") is True:
            obs["blockers"].append("frozen_rule_promotion_claim")
        obs["predicate_feature_names"] = predicate_fields
        obs["forbidden_live_fields_present"] = forbidden
        obs["ready"] = not obs["blockers"]
        observations["frozen_rule_manifest"] = obs
        blockers.extend(obs["blockers"])

    denominator = loaded_json.get("no_order_denominator_summary", {})
    missing_denominator = missing_fields(set(denominator), NO_ORDER_DENOMINATOR_FIELDS)
    if missing_denominator:
        blockers.append("no_order_denominator_required_fields_missing")
    if as_int(denominator.get("same_window_denominator_count")) < args.min_denominator:
        blockers.append("same_window_denominator_below_min")
    if as_int(denominator.get("rule_match_count")) < args.min_rule_matches:
        blockers.append("rule_match_count_below_min")
    if as_float(denominator.get("source_sequence_coverage")) < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_min")
    if denominator.get("aggregate_parity") is not True:
        blockers.append("denominator_aggregate_parity_not_true")

    bridge_summary = loaded_json.get("training_bridge_summary", {})
    if bridge_summary.get("post_action_labels_allowed_in_live_predicate") is not False:
        blockers.append("training_bridge_summary_live_label_leakage_not_false")
    if bridge_summary.get("realized_pair_cost_used_as_live_criteria") is not False:
        blockers.append("training_bridge_summary_realized_pair_cost_live_not_false")
    if bridge_summary.get("future_labels_used_as_live_criteria") is not False:
        blockers.append("training_bridge_summary_future_label_live_not_false")
    if bridge_summary.get("private_truth_ready") is True or bridge_summary.get("deployable") is True:
        blockers.append("training_bridge_summary_private_or_deployable_claim")

    input_manifest = loaded_json.get("observable_pre_action_rule_miner_input_manifest", {})
    if input_manifest.get("strategy_evidence") is True:
        blockers.append("input_manifest_claims_strategy_evidence")
    if input_manifest.get("fixture") is True:
        blockers.append("input_manifest_fixture_true")
    promotion = as_dict(input_manifest.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("input_manifest_private_deployable_or_promotion_claim")

    return observations, sorted(set(blockers))


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    validation_root = args.validation_root or (args.output_dir / "validation")
    supporting = {
        "real_input_inventory_v2": observe_supporting(
            args.real_input_inventory_v2_manifest, root, SUPPORTING_DECISIONS["real_input_inventory_v2"]
        ),
        "feature_join_scorer": observe_supporting(
            args.feature_join_scorer_manifest, root, SUPPORTING_DECISIONS["feature_join_scorer"]
        ),
        "bridge_fixture_scorer": observe_supporting(
            args.bridge_scorer_manifest, root, SUPPORTING_DECISIONS["bridge_fixture_scorer"]
        ),
        "training_fixture_scorer": observe_supporting(
            args.training_fixture_scorer_manifest, root, SUPPORTING_DECISIONS["training_fixture_scorer"]
        ),
        "denominator_fixture_scorer": observe_supporting(
            args.denominator_fixture_scorer_manifest, root, SUPPORTING_DECISIONS["denominator_fixture_scorer"]
        ),
    }
    supporting_ready = all(item.get("ready") for item in supporting.values())

    blockers: list[str] = []
    observations: dict[str, Any] = {}
    handoff_status = "UNKNOWN_SAFE_HANDOFF_MANIFEST_MISSING"
    if not supporting_ready:
        blockers.append("supporting_contract_or_scorer_not_ready")
    if args.handoff_manifest:
        observations, handoff_blockers = validate_handoff(args, root)
        blockers.extend(handoff_blockers)
        handoff_status = "HANDOFF_READY" if not handoff_blockers else "HANDOFF_FAIL_CLOSED"

    if args.handoff_manifest and not blockers:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_READY"
        next_action = (
            "Run the listed validation commands in order on this handoff. If they remain KEEP, run real-input "
            "inventory v2 and miner spec; do not start no-order diagnostics automatically."
        )
    elif args.handoff_manifest:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_FAIL_CLOSED"
        next_action = "Fix handoff path/schema/provenance/label-separation blockers before any mining."
    elif supporting_ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_SPEC_READY"
        next_action = (
            "Wait for a future non-fixture feature-join pullback inside the current worktree, then validate it "
            "with this handoff spec and the listed scorer chain; do not start no-order diagnostics automatically."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_SPEC_FAIL_CLOSED"
        next_action = "Repair supporting scorer/spec readiness before accepting any pullback handoff."

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_safe_feature_join_pullback_handoff",
        "handoff_manifest": str(args.handoff_manifest) if args.handoff_manifest else None,
        "handoff_status": handoff_status,
        "supporting_contracts_and_scorers": supporting,
        "handoff_spec": handoff_spec(validation_root),
        "handoff_observations": observations,
        "blockers": sorted(set(blockers)),
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "handoff_ready": bool(args.handoff_manifest and not blockers),
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "Handoff/spec only. A KEEP handoff means the materialized files can enter local validation; it is "
                "not private truth, deployable evidence, or promotion evidence."
            ),
        },
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "safe_feature_join_pullback_handoff_only",
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
    parser.add_argument("--handoff-manifest", type=Path)
    parser.add_argument("--real-input-inventory-v2-manifest", type=Path, default=DEFAULT_REAL_INPUT_V2)
    parser.add_argument("--feature-join-scorer-manifest", type=Path, default=DEFAULT_FEATURE_JOIN_SCORER)
    parser.add_argument("--bridge-scorer-manifest", type=Path, default=DEFAULT_BRIDGE_SCORER)
    parser.add_argument("--training-fixture-scorer-manifest", type=Path, default=DEFAULT_TRAINING_FIXTURE_SCORER)
    parser.add_argument("--denominator-fixture-scorer-manifest", type=Path, default=DEFAULT_DENOMINATOR_FIXTURE_SCORER)
    parser.add_argument("--validation-root", type=Path)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-feature-join-rows", type=int, default=100)
    parser.add_argument("--min-bridge-joined-rows", type=int, default=100)
    parser.add_argument("--min-denominator", type=int, default=100)
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
