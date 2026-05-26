#!/usr/bin/env python3
"""Materialize non-fixture observable pre-action training bridge rows.

This local-only scorer consumes an allowlisted same-window feature/label
handoff and writes bridge-joined rows when exact-id joins and provenance checks
are safe. It does not train a rule, replay raw data, read events JSONL, use SSH,
start services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer as bridge


ARTIFACT = "xuan_observable_pre_action_non_fixture_training_bridge_materializer"
CONTRACT_NAME = "observable_pre_action_non_fixture_training_bridge_materializer_v1"
HANDOFF_SCHEMA = "observable_pre_action_same_window_offline_label_handoff_v1"
SAME_WINDOW_AGGREGATE_SCHEMA = "same_window_aggregate_summary_v1"
TRAINING_SPEC_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_READY_REAL_INPUT_UNKNOWN"

DEFAULT_BRIDGE_CONTRACT_MANIFEST = bridge.DEFAULT_BRIDGE_CONTRACT_MANIFEST
DEFAULT_TRAINING_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_spec_20260525T223507Z/"
    "manifest.json"
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def validate_training_spec(spec: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if spec.get("decision_label") != TRAINING_SPEC_DECISION:
        blockers.append("training_spec_not_ready")
    if spec.get("private_truth_ready") is True or spec.get("deployable") is True:
        blockers.append("training_spec_private_or_deployable_claim")
    if as_dict(spec.get("promotion_gate")).get("passed") is True:
        blockers.append("training_spec_promotion_claim")
    contract = as_dict(spec.get("contract"))
    gates = as_dict(contract.get("minimum_real_training_gates"))
    for key in ("train_days", "holdout_days", "train_rows", "holdout_rows"):
        if key not in gates:
            blockers.append(f"training_spec_gate_missing:{key}")
    return blockers


def validate_handoff_manifest(handoff: dict[str, Any], expected_root: Path) -> list[str]:
    blockers: list[str] = []
    if handoff.get("schema_version") != HANDOFF_SCHEMA:
        blockers.append("handoff_schema_mismatch")
    if handoff.get("fixture") is True:
        blockers.append("handoff_fixture_not_allowed")
    if int(handoff.get("row_count") or -1) <= 0:
        blockers.append("handoff_row_count_missing")

    scope = as_dict(handoff.get("scope"))
    provenance = as_dict(handoff.get("provenance"))
    for key in (
        "events_jsonl_pulled",
        "events_jsonl_read",
        "raw_replay_or_full_store_scanned",
        "orders_cancels_redeems_sent",
        "shared_ingress_or_broker_or_live_modified",
        "shared_ws_or_local_agg_or_service_started",
        "trading_behavior_changed",
    ):
        if scope.get(key) is True or provenance.get(key) is True:
            blockers.append(f"handoff_forbidden_scope_or_provenance:{key}")
    if provenance.get("generated_from_in_memory_no_order_runner_state") is not True:
        blockers.append("handoff_in_memory_runner_provenance_missing")

    label_policy = as_dict(handoff.get("label_policy"))
    if label_policy.get("labels_allowed_for_train_holdout_scoring") is not True:
        blockers.append("handoff_labels_not_allowed_for_offline_scoring")
    if label_policy.get("labels_allowed_in_live_predicate") is not False:
        blockers.append("handoff_labels_allowed_in_live_predicate")
    if label_policy.get("realized_pair_cost_allowed_as_live_criteria") is not False:
        blockers.append("handoff_realized_pair_cost_live_criteria")
    if label_policy.get("source_pair_source_residual_labels_are_post_action") is not True:
        blockers.append("handoff_label_post_action_policy_missing")

    promotion = as_dict(handoff.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("handoff_private_deployable_or_promotion_claim")

    files = as_dict(handoff.get("files"))
    expected_files = {
        "feature_join_candidate_rows",
        "feature_join_source_link_summary",
        "feature_join_manifest",
        "offline_label_csv",
        "same_window_aggregate_summary",
    }
    missing = sorted(expected_files.difference(files))
    if missing:
        blockers.append("handoff_required_files_missing:" + ",".join(missing))
    for key in expected_files.intersection(files):
        file_path = expected_root / str(files[key])
        ok, reason = bridge.path_safe(file_path, Path.cwd())
        if not ok:
            blockers.append(f"handoff_file_{key}_{reason}")
        elif not file_path.exists():
            blockers.append(f"handoff_file_{key}_missing")
    return blockers


def validate_same_window_aggregate(summary: dict[str, Any], feature_count: int, label_count: int) -> list[str]:
    blockers: list[str] = []
    if summary.get("schema_version") != SAME_WINDOW_AGGREGATE_SCHEMA:
        blockers.append("same_window_aggregate_schema_mismatch")
    if int(summary.get("offline_label_row_count") or -1) != label_count:
        blockers.append("same_window_aggregate_label_row_count_mismatch")
    if summary.get("strategy_evidence") is not False:
        blockers.append("same_window_aggregate_strategy_evidence_claim")
    if summary.get("private_truth_ready") is not False or summary.get("deployable") is not False:
        blockers.append("same_window_aggregate_private_or_deployable_claim")
    if as_dict(summary.get("promotion_gate")).get("passed") is True:
        blockers.append("same_window_aggregate_promotion_claim")
    manifest_count = as_int_safe(as_dict(summary.get("metrics")).get("candidates"))
    if feature_count <= 0 or label_count <= 0:
        blockers.append("same_window_aggregate_empty_inputs")
    if manifest_count is None:
        blockers.append("same_window_aggregate_candidate_metric_missing")
    return blockers


def as_int_safe(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def validate_exact_id_indexes(
    feature_rows: list[dict[str, Any]], labels: list[dict[str, str]], label_fields: set[str]
) -> tuple[list[str], dict[str, int], dict[str, list[int]]]:
    blockers: list[str] = []
    counts = {"feature_candidate_ids": 0, "feature_action_ids": 0, "label_candidate_ids": 0, "label_action_ids": 0}
    if "source_seed_candidate_row_id" not in label_fields:
        blockers.append("offline_label_candidate_exact_id_field_missing")
    if "source_seed_action_id" not in label_fields:
        blockers.append("offline_label_action_exact_id_field_missing")

    candidate_index: dict[str, list[int]] = {}
    action_index: dict[str, list[int]] = {}
    for idx, label in enumerate(labels):
        candidate_id = str(label.get("source_seed_candidate_row_id") or "")
        action_id = str(label.get("source_seed_action_id") or "")
        if candidate_id:
            counts["label_candidate_ids"] += 1
            candidate_index.setdefault(candidate_id, []).append(idx)
        if action_id:
            counts["label_action_ids"] += 1
            action_index.setdefault(action_id, []).append(idx)
    duplicate_candidate_ids = [key for key, indexes in candidate_index.items() if len(indexes) > 1]
    duplicate_action_ids = [key for key, indexes in action_index.items() if len(indexes) > 1]
    if duplicate_candidate_ids:
        blockers.append("duplicate_label_candidate_exact_ids")
    if duplicate_action_ids:
        blockers.append("duplicate_label_action_exact_ids")

    for idx, row in enumerate(feature_rows):
        candidate_id = str(row.get("source_seed_candidate_row_id") or "")
        action_id = str(row.get("source_seed_action_id") or "")
        if candidate_id:
            counts["feature_candidate_ids"] += 1
        if action_id:
            counts["feature_action_ids"] += 1
        if not candidate_id:
            blockers.append(f"feature_row_{idx}_candidate_exact_id_missing")
    return sorted(set(blockers)), counts, {"candidate": candidate_index, "action": action_index}


def split_days(days: list[str], min_train_days: int, min_holdout_days: int) -> tuple[set[str], set[str]]:
    unique_days = sorted({day for day in days if day})
    needed = min_train_days + min_holdout_days
    if len(unique_days) < needed:
        return set(), set()
    holdout_days = set(unique_days[-min_holdout_days:])
    train_days = set(unique_days[:-min_holdout_days])
    return train_days, holdout_days


def exact_id_cross_field_match(feature: dict[str, Any], label: dict[str, str], args: argparse.Namespace) -> tuple[bool, str | None]:
    """Validate same-row provenance fields without requiring absent blocked candidate qty."""
    exact_pairs = (
        ("condition_id", "condition_id"),
        ("market_slug", "slug"),
        ("day_id", "day"),
        ("side", "side"),
        ("source_risk_direction", "source_risk_direction"),
    )
    for feature_field, label_field in exact_pairs:
        if str(feature.get(feature_field) or "") != str(label.get(label_field) or ""):
            return False, f"{feature_field}_mismatch"

    checks = (
        ("quote_ts_ms", ("ts_ms", "trigger_ts_ms"), args.timestamp_tolerance_ms),
        ("offset_s", ("offset_s",), args.offset_tolerance_s),
        ("pre_seed_same_qty", ("pre_seed_same_qty",), args.qty_tolerance),
        ("pre_seed_opp_qty", ("pre_seed_opp_qty",), args.qty_tolerance),
        ("pre_seed_same_cost", ("pre_seed_same_cost",), args.cost_tolerance),
        ("pre_seed_opp_cost", ("pre_seed_opp_cost",), args.cost_tolerance),
        ("pre_seed_open_qty", ("pre_seed_open_qty",), args.qty_tolerance),
        ("pre_seed_open_cost", ("pre_seed_open_cost",), args.cost_tolerance),
    )
    for feature_field, label_fields, tolerance in checks:
        if not bridge.numeric_match(feature, label, feature_field, label_fields, tolerance):
            return False, f"{feature_field}_mismatch"

    candidate_qty = feature.get("candidate_qty")
    label_seed_qty = label.get("seed_qty")
    label_trigger_size = label.get("trigger_size")
    if candidate_qty in (None, "") and label_seed_qty in (None, "") and label_trigger_size in (None, ""):
        return True, None
    if not bridge.numeric_match(feature, label, "candidate_qty", ("seed_qty", "trigger_size"), args.qty_tolerance):
        return False, "candidate_qty_mismatch"
    return True, None


def exact_join_rows(
    *,
    feature_rows: list[dict[str, Any]],
    labels: list[dict[str, str]],
    label_indexes: dict[str, list[int]],
    train_days: set[str],
    holdout_days: set[str],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], list[str], dict[str, int]]:
    blockers: list[str] = []
    joined: list[dict[str, Any]] = []
    used_label_indexes: set[int] = set()
    method_counts = {"exact_id:source_seed_candidate_row_id": 0}
    for idx, feature in enumerate(feature_rows):
        candidate_id = str(feature.get("source_seed_candidate_row_id") or "")
        matches = label_indexes.get(candidate_id, [])
        if not candidate_id:
            blockers.append(f"feature_row_{idx}_candidate_exact_id_missing")
            continue
        if len(matches) == 0:
            blockers.append(f"feature_row_{idx}_zero_exact_id_match:source_seed_candidate_row_id")
            continue
        if len(matches) > 1:
            blockers.append(f"feature_row_{idx}_multiple_exact_id_matches:source_seed_candidate_row_id")
            continue
        label_idx = matches[0]
        if label_idx in used_label_indexes:
            blockers.append(f"feature_row_{idx}_label_row_reuse")
            continue
        label = labels[label_idx]
        cross_field_ok, cross_field_reason = exact_id_cross_field_match(feature, label, args)
        if not cross_field_ok:
            blockers.append(f"feature_row_{idx}_exact_id_cross_field_mismatch:{cross_field_reason}")
            continue
        used_label_indexes.add(label_idx)
        day = str(feature.get("day_id") or "")
        split = "train" if day in train_days else ("holdout" if day in holdout_days else "unused")
        joined.append(
            bridge.joined_row(
                feature_idx=idx,
                feature=feature,
                label_idx=label_idx,
                label=label,
                method="exact_id:source_seed_candidate_row_id",
                split=split,
            )
        )
        method_counts["exact_id:source_seed_candidate_row_id"] += 1
    return joined, blockers, method_counts


def classify_blockers(blockers: list[str], readiness_blockers: list[str]) -> tuple[str, str, str]:
    joined = set(blockers + readiness_blockers)
    discard_tokens = (
        "live_predicate",
        "realized_pair_cost",
        "private",
        "deployable",
        "promotion",
        "strategy_evidence_claim",
        "raw_replay",
        "events_jsonl",
        "forbidden_scope",
    )
    if any(any(token in blocker for token in discard_tokens) for blocker in joined):
        return (
            "DISCARD",
            "DISCARD_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_FORBIDDEN_POLICY",
            "Forbidden policy/provenance claim found; do not mine this handoff.",
        )
    if blockers:
        return (
            "UNKNOWN",
            "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZER_FAIL_CLOSED",
            "Fix materialization blockers before training or denominator replay.",
        )
    if readiness_blockers:
        return (
            "UNKNOWN",
            "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_TRAIN_HOLDOUT_GAPS",
            "Collect additional same-contract non-fixture handoffs across enough train/holdout days, then rerun materialization/training.",
        )
    return (
        "KEEP",
        "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_READY",
        "Run the named training scorer and denominator replay scorer locally; do not start no-order diagnostics.",
    )


def score(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    paths = [
        args.handoff_manifest,
        args.feature_rows,
        args.source_link_summary,
        args.feature_join_manifest,
        args.offline_label_csv,
        args.same_window_aggregate_summary,
        args.bridge_contract_manifest,
        args.training_spec_manifest,
    ]
    blockers = bridge.validate_paths(paths, root)
    if blockers:
        return build_manifest(args, blockers, [], {}, {}, {}, {}, materialized=False)

    feature_rows = bridge.load_jsonl(args.feature_rows)
    source_summary = bridge.read_json(args.source_link_summary)
    feature_manifest = bridge.read_json(args.feature_join_manifest)
    label_fields, labels = bridge.load_csv(args.offline_label_csv)
    handoff = bridge.read_json(args.handoff_manifest)
    same_window_aggregate = bridge.read_json(args.same_window_aggregate_summary)
    bridge_contract = bridge.read_json(args.bridge_contract_manifest)
    training_spec = bridge.read_json(args.training_spec_manifest)

    blockers.extend(validate_handoff_manifest(handoff, args.handoff_manifest.parent))
    blockers.extend(bridge.validate_bridge_contract(bridge_contract))
    blockers.extend(validate_training_spec(training_spec))
    blockers.extend(
        bridge.validate_feature_sources(
            rows=feature_rows,
            source_summary=source_summary,
            feature_manifest=feature_manifest,
        )
    )
    blockers.extend(bridge.validate_label_csv(label_fields, labels))
    blockers.extend(validate_same_window_aggregate(same_window_aggregate, len(feature_rows), len(labels)))
    exact_blockers, exact_counts, exact_indexes = validate_exact_id_indexes(feature_rows, labels, set(label_fields))
    blockers.extend(exact_blockers)

    train_days, holdout_days = split_days(
        [str(row.get("day_id") or "") for row in feature_rows],
        args.min_train_days,
        args.min_holdout_days,
    )
    joined, join_blockers, method_counts = exact_join_rows(
        feature_rows=feature_rows,
        labels=labels,
        label_indexes=exact_indexes["candidate"],
        train_days=train_days,
        holdout_days=holdout_days,
        args=args,
    )
    blockers.extend(join_blockers)

    split_counts = {
        "train": sum(1 for row in joined if row["split"] == "train"),
        "holdout": sum(1 for row in joined if row["split"] == "holdout"),
        "unused": sum(1 for row in joined if row["split"] == "unused"),
    }
    joined_days = sorted({str(row.get("day_id") or "") for row in joined})
    source_sequence_coverage = (
        sum(1 for row in feature_rows if row.get("source_sequence_present") is True) / len(feature_rows)
        if feature_rows
        else 0.0
    )

    readiness_blockers: list[str] = []
    join_coverage = len(joined) / len(feature_rows) if feature_rows else 0.0
    if len(joined) < args.min_joined_rows:
        readiness_blockers.append("joined_rows_below_min")
    if join_coverage < args.min_join_coverage:
        readiness_blockers.append("join_coverage_below_min")
    if len(train_days) < args.min_train_days:
        readiness_blockers.append("train_day_diversity_below_min")
    if len(holdout_days) < args.min_holdout_days:
        readiness_blockers.append("holdout_day_diversity_below_min")
    if split_counts["train"] < args.min_train_rows:
        readiness_blockers.append("train_rows_below_min")
    if split_counts["holdout"] < args.min_holdout_rows:
        readiness_blockers.append("holdout_rows_below_min")
    if source_sequence_coverage < args.min_source_sequence_coverage:
        readiness_blockers.append("source_sequence_coverage_below_min")

    fatal_blockers = sorted(set(blockers))
    materialized = not fatal_blockers and len(joined) == len(feature_rows) and bool(joined)
    outputs = {
        "joined_rows": str(args.output_dir / "observable_pre_action_training_bridge_joined_rows.jsonl")
        if materialized
        else None,
        "summary": str(args.output_dir / "observable_pre_action_training_bridge_summary.json") if materialized else None,
    }
    summary = {
        "schema_version": "observable_pre_action_training_bridge_summary_v1",
        "fixture_only": False,
        "materializer_contract": CONTRACT_NAME,
        "join_policy_version": bridge.JOIN_POLICY_VERSION,
        "feature_row_count": len(feature_rows),
        "offline_label_row_count": len(labels),
        "joined_row_count": len(joined),
        "join_coverage": join_coverage,
        "join_method_counts": method_counts,
        "exact_id_counts": exact_counts,
        "split_counts": split_counts,
        "train_days": sorted(train_days),
        "holdout_days": sorted(holdout_days),
        "available_days": joined_days,
        "source_sequence_coverage": source_sequence_coverage,
        "label_row_reuse_count": max(0, len(joined) - len({row["offline_label_row_index"] for row in joined})),
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
        "promotion_gate": {"passed": False},
    }
    if materialized:
        write_jsonl(args.output_dir / "observable_pre_action_training_bridge_joined_rows.jsonl", joined)
        write_json(args.output_dir / "observable_pre_action_training_bridge_summary.json", summary)

    return build_manifest(
        args,
        fatal_blockers,
        sorted(set(readiness_blockers)),
        joined,
        method_counts,
        exact_counts,
        summary,
        materialized=materialized,
        outputs=outputs,
    )


def build_manifest(
    args: argparse.Namespace,
    blockers: list[str],
    readiness_blockers: list[str] | list[dict[str, Any]],
    joined: list[dict[str, Any]],
    join_method_counts: dict[str, int],
    exact_counts: dict[str, int],
    summary: dict[str, Any],
    *,
    materialized: bool,
    outputs: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    readiness = [str(item) for item in readiness_blockers]
    decision, decision_label, next_action = classify_blockers(blockers, readiness)
    score = {
        "available_days": summary.get("available_days", []) if summary else [],
        "exact_id_counts": exact_counts,
        "join_coverage": summary.get("join_coverage", 0.0) if summary else 0.0,
        "join_method_counts": join_method_counts,
        "joined_row_count": len(joined),
        "materialized": materialized,
        "source_sequence_coverage": summary.get("source_sequence_coverage", 0.0) if summary else 0.0,
        "split_counts": summary.get("split_counts") if summary else None,
    }
    return {
        "artifact": ARTIFACT,
        "blockers": blockers,
        "contract_name": CONTRACT_NAME,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "inputs": {
            "bridge_contract_manifest": str(args.bridge_contract_manifest),
            "feature_join_manifest": str(args.feature_join_manifest),
            "feature_rows": str(args.feature_rows),
            "handoff_manifest": str(args.handoff_manifest),
            "offline_label_csv": str(args.offline_label_csv),
            "same_window_aggregate_summary": str(args.same_window_aggregate_summary),
            "source_link_summary": str(args.source_link_summary),
            "training_spec_manifest": str(args.training_spec_manifest),
        },
        "lane": "observable_pre_action_non_fixture_training_bridge_materialization",
        "materialized": materialized,
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "outputs": outputs or {"joined_rows": None, "summary": None},
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "reason": "non_fixture_training_bridge_materializer_only",
        },
        "readiness_blockers": readiness,
        "research_ranking": {
            "bridge_rows_materialized": materialized,
            "interpretation": (
                "Materialized bridge rows are offline research input only. KEEP requires enough train/holdout "
                "diversity; UNKNOWN can still include safe joined rows but cannot feed real mining yet."
            ),
            "real_miner_input_ready": decision == "KEEP",
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
            "local_agg_or_shared_ws_started": False,
            "orders_sent": False,
            "raw_replay_or_full_store_scan": False,
            "redeems_sent": False,
            "service_control_used": False,
            "shared_ingress_modified": False,
            "ssh_used": False,
            "trading_behavior_changed": False,
        },
        "score": score,
        "strategy_evidence": False,
        "validation_commands": [
            "python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py "
            "--joined-rows ${joined_rows} --output-dir ${training_output_dir}",
            "python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer.py "
            "--candidate-rows ${feature_rows} --source-link-summary ${source_link_summary} "
            "--feature-join-manifest ${feature_join_manifest} --frozen-rule-manifest ${frozen_rule_manifest} "
            "--same-window-aggregate-summary ${same_window_aggregate_summary} --output-dir ${denominator_output_dir}",
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff-manifest", type=Path, required=True)
    parser.add_argument("--feature-rows", type=Path, required=True)
    parser.add_argument("--source-link-summary", type=Path, required=True)
    parser.add_argument("--feature-join-manifest", type=Path, required=True)
    parser.add_argument("--offline-label-csv", type=Path, required=True)
    parser.add_argument("--same-window-aggregate-summary", type=Path, required=True)
    parser.add_argument("--bridge-contract-manifest", type=Path, default=DEFAULT_BRIDGE_CONTRACT_MANIFEST)
    parser.add_argument("--training-spec-manifest", type=Path, default=DEFAULT_TRAINING_SPEC_MANIFEST)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timestamp-tolerance-ms", type=float, default=250.0)
    parser.add_argument("--offset-tolerance-s", type=float, default=0.25)
    parser.add_argument("--qty-tolerance", type=float, default=0.000001)
    parser.add_argument("--cost-tolerance", type=float, default=0.000001)
    parser.add_argument("--min-joined-rows", type=int, default=100)
    parser.add_argument("--min-join-coverage", type=float, default=0.95)
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=100)
    parser.add_argument("--min-holdout-rows", type=int, default=50)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = score(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
