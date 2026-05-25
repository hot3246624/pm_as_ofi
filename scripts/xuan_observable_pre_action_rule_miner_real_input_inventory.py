#!/usr/bin/env python3
"""Inventory non-fixture inputs for observable pre-action rule mining.

This local-only inventory looks only inside the current worktree for safe
materialized CSV/JSON exports, allowlisted pullback summaries, and committed
spec artifacts. It does not read events JSONL, raw/replay stores, use SSH,
start shadows/services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_real_input_inventory"
CONTRACT_NAME = "observable_pre_action_rule_miner_real_input_inventory_v1"
INPUT_SCHEMA = "observable_pre_action_rule_miner_input_v1"

DEFAULT_SPEC_MANIFEST = Path(
    "xuan_research_artifacts"
    "/xuan_observable_pre_action_rule_miner_spec_20260525T182006Z"
    "/manifest.json"
)
DEFAULT_FEATURE_JOIN_SCORER_MANIFEST = Path(
    "xuan_research_artifacts"
    "/xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z"
    "/manifest.json"
)
DEFAULT_DENOMINATOR_FIXTURE_SCORER_MANIFEST = Path(
    "xuan_research_artifacts"
    "/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_20260525T202536Z"
    "/manifest.json"
)
DEFAULT_CANDIDATE_CSV = Path(
    "xuan_research_artifacts"
    "/xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z"
    "/candidate_seed_outcome_separator.csv"
)

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

CSV_ALIASES = {
    "day_id": ("day",),
    "quote_ts_ms": ("ts_ms", "trigger_ts_ms"),
}
CSV_DERIVED = {
    "open_qty_bucket": ("pre_seed_open_qty",),
    "deficit_bucket": ("pre_seed_same_qty", "pre_seed_opp_qty"),
    "candidate_qty_bucket": ("seed_qty", "trigger_size"),
}
CSV_SOURCE_ID_FIELDS = {
    "source_sequence_present": "source_sequence_id",
    "quote_intent_present": "quote_intent_id",
    "source_order_present": "source_order_id",
}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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


def discover_files(root: Path, name: str) -> list[Path]:
    paths = []
    base = root / "xuan_research_artifacts"
    if not base.exists():
        return paths
    for path in base.rglob(name):
        if ".events.jsonl" in str(path):
            continue
        ok, _ = path_safe(path, root)
        if ok:
            paths.append(path)
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
        "input_inventory_smoke",
        "feature_join_runner_instrumentation_smoke",
        "feature_join_output_scorer_smoke",
    )
    return any(marker in lowered for marker in markers)


def source_path(manifest: dict[str, Any], source_name: str) -> Path | None:
    sources = manifest.get("materialized_sources")
    if not isinstance(sources, dict):
        return None
    source = sources.get(source_name)
    if not isinstance(source, dict):
        return None
    path = source.get("path")
    return Path(str(path)) if path else None


def source_fields(manifest: dict[str, Any], source_name: str) -> list[str]:
    sources = manifest.get("materialized_sources")
    if not isinstance(sources, dict):
        return []
    source = sources.get(source_name)
    if not isinstance(source, dict):
        return []
    return [str(field) for field in as_list(source.get("fields"))]


def source_row_count(manifest: dict[str, Any], source_name: str) -> int:
    sources = manifest.get("materialized_sources")
    if not isinstance(sources, dict):
        return 0
    source = sources.get(source_name)
    if not isinstance(source, dict):
        return 0
    return as_int(source.get("row_count"))


def validate_input_manifest(path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    manifest, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        return {
            "path": str(path),
            "exists": path.exists(),
            "ready": False,
            "non_fixture_ready": False,
            "fixture": fixture_like_path(path),
            "blockers": [f"input_manifest_{error}"],
        }

    if manifest.get("schema_version") != INPUT_SCHEMA:
        blockers.append("schema_version_mismatch")
    fixture = bool(manifest.get("fixture")) or fixture_like_path(path)
    if fixture:
        blockers.append("candidate_input_manifest_fixture_only")
    if manifest.get("strategy_evidence") is True:
        blockers.append("candidate_input_manifest_claims_strategy_evidence")

    scope = as_dict(manifest.get("scope"))
    if scope.get("current_worktree_only") is not True:
        blockers.append("scope_current_worktree_only_not_true")
    if scope.get("local_only") is not True:
        blockers.append("scope_local_only_not_true")
    for field in REQUIRED_SCOPE_FALSE_FIELDS:
        if scope.get(field) is not False:
            blockers.append(f"scope_{field}_not_false")

    for source_name in ("candidate_rows", "source_link_summary", "no_order_denominator_summary"):
        src = source_path(manifest, source_name)
        if src is None:
            blockers.append(f"{source_name}_path_missing")
            continue
        ok, reason = path_safe(src, root)
        if not ok:
            blockers.append(f"{source_name}_{reason}")
        elif not src.exists():
            blockers.append(f"{source_name}_path_missing_on_disk")

    candidate_fields = set(source_fields(manifest, "candidate_rows"))
    no_order_fields = set(source_fields(manifest, "no_order_denominator_summary"))
    missing_pre = sorted(set(REQUIRED_PRE_ACTION_FIELDS).difference(candidate_fields))
    missing_labels = sorted(set(OFFLINE_LABEL_FIELDS).difference(candidate_fields))
    missing_no_order = sorted(set(REQUIRED_NO_ORDER_FIELDS).difference(no_order_fields))
    if missing_pre:
        blockers.append("candidate_rows_required_pre_action_fields_missing")
    if missing_labels:
        blockers.append("candidate_rows_offline_label_fields_missing")
    if missing_no_order:
        blockers.append("no_order_denominator_required_fields_missing")
    if source_row_count(manifest, "candidate_rows") < args.min_candidate_rows:
        blockers.append("candidate_rows_below_min")

    frozen = as_dict(manifest.get("frozen_rule_contract"))
    predicate_fields = {str(field) for field in as_list(frozen.get("predicate_fields"))}
    allowed_live_fields = {str(field) for field in as_list(as_dict(manifest.get("feature_policy")).get("allowed_live_rule_fields"))}
    if not predicate_fields:
        blockers.append("frozen_rule_predicate_fields_missing")
    if not predicate_fields.issubset(allowed_live_fields):
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

    split = as_dict(manifest.get("train_holdout_split"))
    train_days = {str(day) for day in as_list(split.get("train_days"))}
    holdout_days = {str(day) for day in as_list(split.get("holdout_days"))}
    if len(train_days) < args.min_train_days:
        blockers.append("train_days_below_min")
    if len(holdout_days) < args.min_holdout_days:
        blockers.append("holdout_days_below_min")
    if train_days.intersection(holdout_days):
        blockers.append("train_holdout_days_overlap")

    denom_gate = as_dict(manifest.get("no_order_denominator_gate"))
    if denom_gate.get("same_window_denominator_reproduced") is not True:
        blockers.append("same_window_denominator_not_reproduced")
    if as_int(denom_gate.get("same_window_denominator_count")) < args.min_no_order_denominator:
        blockers.append("same_window_denominator_below_min")
    if as_float(denom_gate.get("source_sequence_coverage")) < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_min")
    if denom_gate.get("aggregate_parity") is not True:
        blockers.append("aggregate_parity_not_true")

    promotion = as_dict(manifest.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("private_deployable_or_promotion_claim_present")

    ready = not blockers
    return {
        "path": str(path),
        "exists": True,
        "schema_version": manifest.get("schema_version"),
        "decision_label": manifest.get("decision_label"),
        "fixture": fixture,
        "ready": ready,
        "non_fixture_ready": ready and not fixture,
        "candidate_row_count": source_row_count(manifest, "candidate_rows"),
        "same_window_denominator_count": as_int(denom_gate.get("same_window_denominator_count")),
        "source_sequence_coverage": as_float(denom_gate.get("source_sequence_coverage")),
        "missing_pre_action_fields": missing_pre,
        "missing_offline_label_fields": missing_labels,
        "missing_no_order_fields": missing_no_order,
        "blockers": sorted(set(blockers)),
    }


def line_count_and_first_json(path: Path, limit: int = 3) -> tuple[int, list[dict[str, Any]]]:
    count = 0
    sample: list[dict[str, Any]] = []
    with path.open() as fh:
        for line in fh:
            if not line.strip():
                continue
            count += 1
            if len(sample) < limit:
                try:
                    obj = json.loads(line)
                except Exception:
                    obj = {}
                if isinstance(obj, dict):
                    sample.append(obj)
    return count, sample


def observe_feature_join_dir(candidate_rows_path: Path, root: Path) -> dict[str, Any]:
    parent = candidate_rows_path.parent
    source_path = parent / "observable_pre_action_source_link_summary.json"
    manifest_path = parent / "observable_pre_action_feature_join_manifest.json"
    blockers = []
    for label, path in (
        ("candidate_rows", candidate_rows_path),
        ("source_link_summary", source_path),
        ("feature_join_manifest", manifest_path),
    ):
        ok, reason = path_safe(path, root)
        if not ok:
            blockers.append(f"{label}_{reason}")
        elif not path.exists():
            blockers.append(f"{label}_missing")

    row_count = 0
    fields: set[str] = set()
    if candidate_rows_path.exists():
        row_count, sample = line_count_and_first_json(candidate_rows_path)
        for row in sample:
            fields.update(row.keys())
    manifest, error = safe_json(manifest_path, root)
    if error:
        manifest = {}
    field_contract = as_dict(manifest.get("field_contract"))
    fixture = fixture_like_path(candidate_rows_path) or bool(manifest.get("fixture")) or bool(field_contract.get("fixture_only"))
    if fixture:
        blockers.append("feature_join_output_fixture_or_smoke_only")
    missing_pre = sorted(set(REQUIRED_PRE_ACTION_FIELDS).difference(fields))
    missing_labels = sorted(set(OFFLINE_LABEL_FIELDS).difference(fields))
    if missing_pre:
        blockers.append("feature_join_rows_required_pre_action_fields_missing")
    if missing_labels:
        blockers.append("feature_join_rows_offline_label_fields_missing")
    if row_count < args_min_default_candidate_rows():
        blockers.append("feature_join_rows_below_miner_min_candidate_rows")
    if field_contract.get("trading_behavior_changed") is True:
        blockers.append("feature_join_contract_claims_behavior_changed")
    if field_contract.get("private_truth_ready") is True or field_contract.get("deployable") is True:
        blockers.append("feature_join_contract_claims_private_or_deployable")

    return {
        "dir": str(parent),
        "candidate_rows_path": str(candidate_rows_path),
        "source_link_summary_path": str(source_path),
        "feature_join_manifest_path": str(manifest_path),
        "fixture_or_smoke": fixture,
        "row_count": row_count,
        "schema_version": manifest.get("schema_version"),
        "missing_pre_action_fields_from_sample": missing_pre,
        "missing_offline_label_fields_from_sample": missing_labels,
        "ready_as_non_fixture_feature_join_component": not blockers,
        "blockers": sorted(set(blockers)),
    }


def args_min_default_candidate_rows() -> int:
    return 1000


def observe_denominator_summary(path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    data, error = safe_json(path, root)
    blockers = []
    if error:
        return {"path": str(path), "exists": path.exists(), "ready": False, "blockers": [f"denominator_{error}"]}
    fixture = fixture_like_path(path) or bool(as_dict(data.get("field_contract")).get("fixture_only"))
    if fixture:
        blockers.append("denominator_summary_fixture_only")
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
        "exists": True,
        "schema_version": data.get("schema_version"),
        "frozen_rule_id": data.get("frozen_rule_id"),
        "fixture": fixture,
        "same_window_denominator_count": data.get("same_window_denominator_count"),
        "rule_match_count": data.get("rule_match_count"),
        "source_sequence_coverage": data.get("source_sequence_coverage"),
        "aggregate_parity": data.get("aggregate_parity"),
        "ready_as_non_fixture_denominator": not blockers,
        "blockers": sorted(set(blockers)),
    }


def observe_candidate_csv(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {"path": str(path), "exists": path.exists(), "safe": False, "blockers": [reason]}
    if not path.exists():
        return {"path": str(path), "exists": False, "safe": True, "blockers": ["candidate_csv_missing"]}
    row_count = 0
    nonempty_source_ids = {field: 0 for field in CSV_SOURCE_ID_FIELDS.values()}
    external_shadow_ids_available_true = 0
    days: set[str] = set()
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
    alias_available = {
        field: [alias for alias in CSV_ALIASES.get(field, ()) if alias in field_set]
        for field in REQUIRED_PRE_ACTION_FIELDS
    }
    alias_available = {field: aliases for field, aliases in alias_available.items() if aliases}
    derived_available = {
        field: list(sources)
        for field, sources in CSV_DERIVED.items()
        if all(source in field_set for source in sources)
    }
    source_id_available = {
        field: source for field, source in CSV_SOURCE_ID_FIELDS.items() if source in field_set
    }
    missing_pre = [
        field
        for field in REQUIRED_PRE_ACTION_FIELDS
        if field not in field_set
        and field not in alias_available
        and field not in derived_available
        and field not in source_id_available
    ]
    source_presence_blockers = []
    if external_shadow_ids_available_true == 0:
        source_presence_blockers.append("external_shadow_ids_available_never_true")
    for source_id, count in nonempty_source_ids.items():
        if count <= 0:
            source_presence_blockers.append(f"{source_id}_empty")
    missing_labels = [field for field in OFFLINE_LABEL_FIELDS if field not in field_set]
    blockers = []
    if missing_pre:
        blockers.append("candidate_csv_required_pre_action_fields_missing")
    if source_presence_blockers:
        blockers.append("candidate_csv_source_presence_ids_not_real_truth")
    if missing_labels:
        blockers.append("candidate_csv_offline_label_fields_missing")
    blockers.append("candidate_csv_lacks_status_block_reason_and_exact_no_order_denominator")
    return {
        "path": str(path),
        "exists": True,
        "safe": True,
        "row_count": row_count,
        "day_count": len(days),
        "days_sample": sorted(days)[:8],
        "field_count": len(fields),
        "field_sample": fields[:80],
        "alias_available": alias_available,
        "derived_available": derived_available,
        "source_id_available": source_id_available,
        "nonempty_source_ids": nonempty_source_ids,
        "external_shadow_ids_available_true": external_shadow_ids_available_true,
        "missing_pre_action_fields": missing_pre,
        "missing_offline_label_fields": missing_labels,
        "ready_as_non_fixture_miner_candidate_rows": False,
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
        summary_count = len(list(directory.glob("*.summary.json")))
        has_candidate_rows = (directory / "observable_pre_action_candidate_rows.jsonl").exists()
        has_source_summary = (directory / "observable_pre_action_source_link_summary.json").exists()
        has_feature_manifest = (directory / "observable_pre_action_feature_join_manifest.json").exists()
        summaries.append(
            {
                "output_dir": str(directory),
                "summary_json_count": summary_count,
                "aggregate_report_present": True,
                "observable_pre_action_candidate_rows_present": has_candidate_rows,
                "observable_pre_action_source_link_summary_present": has_source_summary,
                "observable_pre_action_feature_join_manifest_present": has_feature_manifest,
                "ready_as_feature_join_source": has_candidate_rows and has_source_summary and has_feature_manifest,
            }
        )
    return {
        "allowlisted_pullback_output_dir_count": len(summaries),
        "dirs_with_observable_pre_action_outputs": sum(1 for item in summaries if item["ready_as_feature_join_source"]),
        "sample": summaries[:12],
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    spec_manifest, spec_error = safe_json(args.spec_manifest, root)
    feature_scorer, feature_scorer_error = safe_json(args.feature_join_scorer_manifest, root)
    denominator_fixture_scorer, denominator_fixture_error = safe_json(args.denominator_fixture_scorer_manifest, root)

    explicit_input_manifests = set(args.candidate_input_manifest or [])
    if args.only_explicit_input_manifests:
        input_manifest_paths = sorted(explicit_input_manifests)
    else:
        tracked_input_manifests = {
            Path(path)
            for path in git_ls_files(["xuan_research_artifacts/**/observable_pre_action_rule_miner_input_manifest.json"])
        }
        discovered_input_manifests = set(discover_files(root, "observable_pre_action_rule_miner_input_manifest.json"))
        input_manifest_paths = sorted(tracked_input_manifests.union(discovered_input_manifests).union(explicit_input_manifests))
    input_manifest_observations = [
        validate_input_manifest(path, root, args)
        for path in input_manifest_paths
    ]
    ready_input_manifests = [item for item in input_manifest_observations if item.get("non_fixture_ready")]

    feature_join_candidates = discover_files(root, "observable_pre_action_candidate_rows.jsonl")
    feature_join_observations = [
        observe_feature_join_dir(path, root)
        for path in feature_join_candidates
        if path.name == "observable_pre_action_candidate_rows.jsonl"
    ]
    non_fixture_feature_join_ready = [
        item for item in feature_join_observations if item.get("ready_as_non_fixture_feature_join_component")
    ]

    denominator_paths = discover_files(root, "observable_pre_action_no_order_denominator_summary.json")
    denominator_observations = [observe_denominator_summary(path, root, args) for path in denominator_paths]
    non_fixture_denominators_ready = [
        item for item in denominator_observations if item.get("ready_as_non_fixture_denominator")
    ]

    candidate_csv = observe_candidate_csv(args.candidate_csv, root)
    pullbacks = observe_pullback_dirs(root)

    blockers = []
    if spec_error or spec_manifest.get("decision_label") != "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_REAL_INPUT_UNKNOWN":
        blockers.append("observable_pre_action_rule_miner_spec_not_ready")
    if feature_scorer_error or feature_scorer.get("decision_label") != "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY":
        blockers.append("feature_join_output_scorer_not_ready")
    if denominator_fixture_error or denominator_fixture_scorer.get("decision_label") != "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY":
        blockers.append("denominator_replay_fixture_scorer_not_ready")
    if not ready_input_manifests:
        blockers.append("non_fixture_observable_pre_action_input_manifest_absent")
    if not non_fixture_feature_join_ready:
        blockers.append("non_fixture_feature_join_outputs_absent_or_incomplete")
    if not non_fixture_denominators_ready:
        blockers.append("non_fixture_denominator_summary_absent")
    if not bool(candidate_csv.get("ready_as_non_fixture_miner_candidate_rows")):
        blockers.append("legacy_candidate_csv_not_assemble_ready")
    if as_int(pullbacks.get("dirs_with_observable_pre_action_outputs")) <= 0:
        blockers.append("allowlisted_pullbacks_lack_observable_pre_action_outputs")

    ready = bool(ready_input_manifests)
    if ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_READY"
        next_action = (
            "Run scripts/xuan_observable_pre_action_rule_miner_spec.py on the named non-fixture input manifest, "
            "then implement the local rule miner training/scorer only if the contract passes."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_GAPS"
        next_action = (
            "Implement observable_pre_action_rule_miner_training_input_bridge_contract_v1 local-only: define how a "
            "future allowlisted feature-join output can be joined to offline source_pair/source_residual labels and "
            "same-window denominator summaries without events JSONL/raw/replay/SSH/shadow; do not start diagnostics."
        )

    validation_commands = [
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_spec.py "
            f"--candidate-input-manifest {item['path']} "
            f"--output-dir {args.output_dir / ('validation_' + str(idx))}"
        )
        for idx, item in enumerate(ready_input_manifests, start=1)
    ]

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_real_input_inventory",
        "source_inputs": {
            "spec_manifest": str(args.spec_manifest),
            "feature_join_scorer_manifest": str(args.feature_join_scorer_manifest),
            "denominator_fixture_scorer_manifest": str(args.denominator_fixture_scorer_manifest),
            "candidate_csv": str(args.candidate_csv),
            "explicit_candidate_input_manifests": [str(path) for path in args.candidate_input_manifest or []],
        },
        "readiness": {
            "ready": ready,
            "ready_input_manifests": ready_input_manifests,
            "validation_commands": validation_commands,
            "blockers": sorted(set(blockers)),
        },
        "observations": {
            "input_manifest_count": len(input_manifest_observations),
            "input_manifests": input_manifest_observations[:20],
            "feature_join_output_count": len(feature_join_observations),
            "feature_join_outputs": feature_join_observations[:20],
            "denominator_summary_count": len(denominator_observations),
            "denominator_summaries": denominator_observations[:20],
            "legacy_candidate_csv": candidate_csv,
            "allowlisted_pullbacks": pullbacks,
            "supporting_scorers": {
                "spec_decision_label": spec_manifest.get("decision_label"),
                "feature_join_scorer_decision_label": feature_scorer.get("decision_label"),
                "denominator_fixture_scorer_decision_label": denominator_fixture_scorer.get("decision_label"),
            },
        },
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "real_input_ready": ready,
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "Inventory only. A KEEP would only name validation commands for non-fixture input; it would still "
                "not be private truth, deployable evidence, or promotion evidence."
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
    parser.add_argument("--candidate-csv", type=Path, default=DEFAULT_CANDIDATE_CSV)
    parser.add_argument("--candidate-input-manifest", type=Path, action="append")
    parser.add_argument("--only-explicit-input-manifests", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--min-candidate-rows", type=int, default=1000)
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
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
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
