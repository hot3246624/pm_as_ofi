#!/usr/bin/env python3
"""Specify safe multi-day same-window label handoff accumulation.

This local-only contract defines how multiple non-fixture same-window
feature/label handoffs may later be accumulated, deduplicated, and chronologically
split into train/holdout bridge rows. It does not read events JSONL, scan
raw/replay stores, use SSH, start services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_multiday_same_window_label_handoff_accumulation_contract"
CONTRACT_NAME = "observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_v1"
CASE_SCHEMA = "observable_pre_action_multiday_same_window_label_handoff_accumulation_case_v1"

READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CONTRACT_READY"
CASE_READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CASE_READY"
CASE_GAPS_DECISION = "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CASE_GAPS"
CONTRACT_GAPS_DECISION = "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CONTRACT_GAPS"
FORBIDDEN_DECISION = "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_FORBIDDEN_INPUT"

DEFAULT_SAME_WINDOW_CONTRACT_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_same_window_offline_label_handoff_contract_20260526T065812Z/"
    "manifest.json"
)
DEFAULT_MATERIALIZER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_non_fixture_training_bridge_materialization_20260526T142913Z/"
    "manifest.json"
)
DEFAULT_MULTIDAY_INVENTORY_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory_20260526T151842Z/"
    "manifest.json"
)

ACCEPTABLE_SUPPORTING_DECISIONS = {
    "same_window_handoff_contract": {
        "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_CONTRACT_READY",
        "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_READY",
    },
    "non_fixture_bridge_materializer": {
        "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_TRAIN_HOLDOUT_GAPS",
        "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MATERIALIZED_READY",
    },
    "multiday_bridge_inventory": {
        "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_GAPS",
        "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_READY",
    },
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

FORBIDDEN_CASE_FIELDS = (
    "events_jsonl_read",
    "events_jsonl_pulled",
    "raw_replay_or_full_store_scan",
    "raw_replay_or_full_store_scanned",
    "external_worktree_read",
    "ssh_used",
    "shadow_started",
    "canary_or_live_started",
    "shared_ws_or_local_agg_or_service_started",
    "shared_ingress_or_broker_or_live_modified",
    "orders_cancels_redeems_sent",
    "orders_sent",
    "cancels_sent",
    "redeems_sent",
    "trading_behavior_changed",
)

FORBIDDEN_LIVE_POLICY_FIELDS = (
    "labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "realized_pair_cost_allowed_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "source_pair_source_residual_used_as_live_predicate",
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


def truthy(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"true", "1", "yes"}


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


def observe_manifest(path: Path, role: str, expected_decisions: set[str], root: Path) -> dict[str, Any]:
    blockers: list[str] = []
    ok, reason = path_safe(path, root)
    if not ok:
        blockers.append(reason or "unsafe_path")
        return {"blockers": blockers, "path": str(path), "ready": False, "role": role}
    if not path.exists():
        blockers.append("missing")
        return {"blockers": blockers, "path": str(path), "ready": False, "role": role}
    try:
        data = read_json(path)
    except Exception as exc:  # pragma: no cover - represented in manifest.
        blockers.append(f"{type(exc).__name__}:{exc}")
        return {"blockers": blockers, "path": str(path), "ready": False, "role": role}
    if not isinstance(data, dict):
        blockers.append("not_json_object")
        data = {}
    decision_label = str(data.get("decision_label") or "")
    if decision_label not in expected_decisions:
        blockers.append("unexpected_decision_label")
    if data.get("private_truth_ready") is True or data.get("deployable") is True:
        blockers.append("private_or_deployable_claim")
    if as_dict(data.get("promotion_gate")).get("passed") is True:
        blockers.append("promotion_claim")
    if data.get("strategy_evidence") is True:
        blockers.append("strategy_evidence_claim")

    if role == "non_fixture_bridge_materializer" and data.get("materialized") is not True:
        blockers.append("materializer_not_materialized")
    if role == "multiday_bridge_inventory" and as_dict(data.get("research_ranking")).get("bridge_rows_materialized") is not True:
        blockers.append("inventory_bridge_rows_not_materialized")

    return {
        "blockers": sorted(set(blockers)),
        "decision_label": decision_label,
        "expected_decision_labels": sorted(expected_decisions),
        "path": str(path),
        "ready": not blockers,
        "role": role,
    }


def contract(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "accumulation_output_schema": {
            "manifest": "observable_pre_action_multiday_handoff_accumulation_manifest_v1",
            "joined_rows": "observable_pre_action_training_bridge_joined_rows.jsonl with schema observable_pre_action_training_bridge_joined_row_v1",
            "summary": "observable_pre_action_training_bridge_summary.json with schema observable_pre_action_training_bridge_summary_v1",
        },
        "dedupe_policy": {
            "duplicate_handoff_id": "fail_closed",
            "duplicate_source_seed_action_id": "fail_closed_when_present",
            "duplicate_source_seed_candidate_row_id": "fail_closed",
            "silent_dedupe_allowed": False,
        },
        "fail_closed_conditions": [
            "handoff_schema_mismatch",
            "fixture_or_smoke_handoff_as_real",
            "missing_required_per_day_file",
            "outside_current_worktree_path",
            "events_jsonl_or_raw_replay_or_full_store_dependency",
            "source_seed_candidate_row_id_missing",
            "duplicate_exact_id_within_or_across_handoffs",
            "join_coverage_below_1_0_for_materialized_exact_id_path",
            "source_sequence_coverage_below_min",
            "less_than_3_train_days_or_2_holdout_days",
            "train_or_holdout_rows_below_min",
            "labels_used_as_live_predicate",
            "realized_pair_cost_or_future_label_used_as_live_criterion",
            "private_truth_deployable_or_promotion_claim",
        ],
        "label_policy": {
            "labels_allowed_for_train_holdout_scoring": True,
            "labels_allowed_in_live_predicate": False,
            "realized_pair_cost_allowed_as_live_criteria": False,
            "source_pair_source_residual_labels_are_post_action": True,
        },
        "minimums": {
            "min_distinct_days": args.min_train_days + args.min_holdout_days,
            "min_holdout_days": args.min_holdout_days,
            "min_holdout_rows": args.min_holdout_rows,
            "min_join_coverage": args.min_join_coverage,
            "min_source_sequence_coverage": args.min_source_sequence_coverage,
            "min_train_days": args.min_train_days,
            "min_train_rows": args.min_train_rows,
        },
        "per_day_required_files": {
            "feature_join_candidate_rows": "observable_pre_action_candidate_rows.jsonl",
            "feature_join_manifest": "observable_pre_action_feature_join_manifest.json",
            "feature_join_source_link_summary": "observable_pre_action_source_link_summary.json",
            "offline_label_csv": "observable_pre_action_same_window_offline_labels.csv",
            "same_window_aggregate_summary": "same_window_aggregate_summary.json",
            "same_window_label_handoff_manifest": "observable_pre_action_same_window_offline_label_handoff.json",
        },
        "required_exact_ids": {
            "primary": "source_seed_candidate_row_id",
            "secondary_when_present": "source_seed_action_id",
            "uniqueness_scope": "within each handoff and across all accumulated handoffs",
        },
        "split_policy": {
            "chronological": True,
            "holdout_days": "last N distinct days where N=min_holdout_days",
            "train_days": "all earlier distinct days; must be at least min_train_days",
            "unused_rows_after_accumulation": 0,
        },
        "validation_command_order": [
            "python3 scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py --handoff-manifest ${handoff_manifest} --output-dir ${validation_dir}/00_same_window_handoff",
            "python3 scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py --handoff-manifest ${handoff_manifest} --feature-rows ${feature_rows} --source-link-summary ${source_link_summary} --feature-join-manifest ${feature_join_manifest} --offline-label-csv ${offline_label_csv} --same-window-aggregate-summary ${same_window_aggregate_summary} --output-dir ${per_day_bridge_output_dir}",
            "python3 scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py --bridge-summary ${per_day_bridge_summaries...} --output-dir ${accumulated_bridge_output_dir}",
            "python3 scripts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory.py --bridge-summary ${accumulated_bridge_summary} --only-explicit --output-dir ${inventory_output_dir}",
            "python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py --joined-rows ${accumulated_joined_rows} --output-dir ${training_output_dir}",
            "python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer.py --candidate-rows ${feature_rows} --source-link-summary ${source_link_summary} --feature-join-manifest ${feature_join_manifest} --frozen-rule-manifest ${frozen_rule_manifest} --same-window-aggregate-summary ${same_window_aggregate_summary} --output-dir ${denominator_output_dir}",
            "python3 scripts/xuan_observable_pre_action_rule_miner_spec.py --input-manifest ${miner_input_manifest}",
        ],
    }


def collect_ids(handoff: dict[str, Any], prefix: str, key: str) -> list[str]:
    ids = [str(item) for item in as_list(handoff.get(key)) if str(item)]
    if ids:
        return ids
    row_count = as_int(handoff.get("row_count"))
    if truthy(handoff.get("generate_synthetic_exact_ids_for_smoke")) and row_count > 0:
        return [f"{prefix}-{idx}" for idx in range(row_count)]
    return ids


def validate_case(data: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    blockers: list[str] = []
    forbidden: list[str] = []
    handoff_ids: set[str] = set()
    candidate_ids: set[str] = set()
    action_ids: set[str] = set()
    day_counts: Counter[str] = Counter()

    if data.get("schema_version") != CASE_SCHEMA:
        blockers.append("case_schema_mismatch")
    if data.get("fixture") is True:
        forbidden.append("case_fixture_true")
    if data.get("strategy_evidence") is True:
        forbidden.append("case_strategy_evidence_claim")
    if data.get("private_truth_ready") is True or data.get("deployable") is True:
        forbidden.append("case_private_or_deployable_claim")
    if as_dict(data.get("promotion_gate")).get("passed") is True:
        forbidden.append("case_promotion_claim")
    for field in FORBIDDEN_CASE_FIELDS:
        if data.get(field) is True:
            forbidden.append(f"case_{field}_true")
    for field in FORBIDDEN_LIVE_POLICY_FIELDS:
        if data.get(field) is True:
            forbidden.append(f"case_{field}_true")

    handoffs = as_list(data.get("handoffs"))
    if not handoffs:
        blockers.append("handoffs_missing")

    for idx, raw_handoff in enumerate(handoffs):
        handoff = as_dict(raw_handoff)
        handoff_id = str(handoff.get("handoff_id") or "")
        day = str(handoff.get("day") or "")
        row_count = as_int(handoff.get("row_count"))
        if not handoff_id:
            blockers.append(f"handoff_{idx}_id_missing")
            handoff_id = f"missing-handoff-{idx}"
        if handoff_id in handoff_ids:
            blockers.append("duplicate_handoff_id")
        handoff_ids.add(handoff_id)
        if not day:
            blockers.append(f"handoff_{idx}_day_missing")
        if row_count <= 0:
            blockers.append(f"handoff_{idx}_row_count_missing")
        if handoff.get("fixture") is True:
            forbidden.append(f"handoff_{idx}_fixture_true")
        if handoff.get("private_truth_ready") is True or handoff.get("deployable") is True:
            forbidden.append(f"handoff_{idx}_private_or_deployable_claim")
        if as_dict(handoff.get("promotion_gate")).get("passed") is True:
            forbidden.append(f"handoff_{idx}_promotion_claim")
        if handoff.get("strategy_evidence") is True:
            forbidden.append(f"handoff_{idx}_strategy_evidence_claim")
        for field in FORBIDDEN_CASE_FIELDS:
            if handoff.get(field) is True:
                forbidden.append(f"handoff_{idx}_{field}_true")
        for field in FORBIDDEN_LIVE_POLICY_FIELDS:
            if handoff.get(field) is True:
                forbidden.append(f"handoff_{idx}_{field}_true")
        if handoff.get("labels_allowed_for_train_holdout_scoring") is not True:
            blockers.append(f"handoff_{idx}_labels_not_train_holdout_only")
        if handoff.get("source_pair_source_residual_labels_are_post_action") is not True:
            blockers.append(f"handoff_{idx}_post_action_label_policy_missing")
        join_coverage = as_float(handoff.get("join_coverage"))
        if join_coverage < args.min_join_coverage:
            blockers.append(f"handoff_{idx}_join_coverage_below_min")
        source_sequence_coverage = as_float(handoff.get("source_sequence_coverage"))
        if source_sequence_coverage < args.min_source_sequence_coverage:
            blockers.append(f"handoff_{idx}_source_sequence_coverage_below_min")

        candidate_exact_ids = collect_ids(handoff, f"{handoff_id}-candidate", "source_seed_candidate_row_ids")
        action_exact_ids = collect_ids(handoff, f"{handoff_id}-action", "source_seed_action_ids")
        if len(candidate_exact_ids) != row_count:
            blockers.append(f"handoff_{idx}_candidate_exact_id_count_mismatch")
        if len(set(candidate_exact_ids)) != len(candidate_exact_ids):
            blockers.append(f"handoff_{idx}_duplicate_candidate_exact_ids")
        if len(set(action_exact_ids)) != len(action_exact_ids):
            blockers.append(f"handoff_{idx}_duplicate_action_exact_ids")
        for candidate_id in candidate_exact_ids:
            if candidate_id in candidate_ids:
                blockers.append("duplicate_candidate_exact_id_across_handoffs")
            candidate_ids.add(candidate_id)
        for action_id in action_exact_ids:
            if action_id in action_ids:
                blockers.append("duplicate_action_exact_id_across_handoffs")
            action_ids.add(action_id)
        if day:
            day_counts[day] += row_count

    distinct_days = sorted(day_counts)
    holdout_days = set(distinct_days[-args.min_holdout_days :]) if len(distinct_days) >= args.min_holdout_days else set()
    train_days = set(distinct_days).difference(holdout_days)
    train_rows = sum(count for day, count in day_counts.items() if day in train_days)
    holdout_rows = sum(count for day, count in day_counts.items() if day in holdout_days)
    if len(train_days) < args.min_train_days:
        blockers.append("train_day_diversity_below_min")
    if len(holdout_days) < args.min_holdout_days:
        blockers.append("holdout_day_diversity_below_min")
    if train_rows < args.min_train_rows:
        blockers.append("train_rows_below_min")
    if holdout_rows < args.min_holdout_rows:
        blockers.append("holdout_rows_below_min")

    decision = "KEEP"
    decision_label = CASE_READY_DECISION
    if forbidden:
        decision = "DISCARD"
        decision_label = FORBIDDEN_DECISION
    elif blockers:
        decision = "UNKNOWN"
        decision_label = CASE_GAPS_DECISION

    return {
        "blockers": sorted(set(blockers)),
        "decision": decision,
        "decision_label": decision_label,
        "forbidden_blockers": sorted(set(forbidden)),
        "score": {
            "available_day_count": len(distinct_days),
            "available_days": distinct_days,
            "candidate_exact_id_count": len(candidate_ids),
            "day_counts": dict(sorted(day_counts.items())),
            "holdout_days": sorted(holdout_days),
            "holdout_rows": holdout_rows,
            "handoff_count": len(handoffs),
            "train_days": sorted(train_days),
            "train_rows": train_rows,
        },
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    supporting = {
        "multiday_bridge_inventory": observe_manifest(
            args.multiday_inventory_manifest,
            "multiday_bridge_inventory",
            ACCEPTABLE_SUPPORTING_DECISIONS["multiday_bridge_inventory"],
            root,
        ),
        "non_fixture_bridge_materializer": observe_manifest(
            args.materializer_manifest,
            "non_fixture_bridge_materializer",
            ACCEPTABLE_SUPPORTING_DECISIONS["non_fixture_bridge_materializer"],
            root,
        ),
        "same_window_handoff_contract": observe_manifest(
            args.same_window_contract_manifest,
            "same_window_handoff_contract",
            ACCEPTABLE_SUPPORTING_DECISIONS["same_window_handoff_contract"],
            root,
        ),
    }
    support_blockers = sorted(
        {f"{name}:{blocker}" for name, item in supporting.items() for blocker in as_list(item.get("blockers"))}
    )
    case_validation: dict[str, Any] | None = None
    if args.case_json:
        case_data = read_json(args.case_json)
        if not isinstance(case_data, dict):
            case_validation = {
                "blockers": ["case_not_json_object"],
                "decision": "UNKNOWN",
                "decision_label": CASE_GAPS_DECISION,
                "forbidden_blockers": [],
                "score": {},
            }
        else:
            case_validation = validate_case(case_data, args)

    if case_validation:
        decision = case_validation["decision"]
        decision_label = case_validation["decision_label"]
        next_action = (
            "Use only KEEP multi-day accumulation cases to build a local accumulator scorer; keep labels train/holdout-only."
            if decision == "KEEP"
            else "Fix multi-day accumulation blockers before bridge/training scoring; do not substitute fixtures or old CSVs."
        )
        blockers = as_list(case_validation.get("blockers")) + support_blockers
        forbidden = as_list(case_validation.get("forbidden_blockers"))
    elif support_blockers:
        decision = "UNKNOWN"
        decision_label = CONTRACT_GAPS_DECISION
        next_action = "Repair supporting same-window contract/materializer/multiday inventory readiness before accepting accumulated handoffs."
        blockers = support_blockers
        forbidden = []
    else:
        decision = "KEEP"
        decision_label = READY_DECISION
        next_action = (
            "Implement a local accumulator scorer that accepts multiple non-fixture same-window handoffs, enforces exact-id uniqueness, "
            "chronologically resplits rows, and emits a single training bridge summary/JSONL."
        )
        blockers = []
        forbidden = []

    current_inventory = {}
    try:
        inventory = read_json(args.multiday_inventory_manifest)
        if isinstance(inventory, dict):
            current_inventory = {
                "blockers": inventory.get("blockers", []),
                "decision_label": inventory.get("decision_label"),
                "real_miner_input_ready": as_dict(inventory.get("research_ranking")).get("real_miner_input_ready"),
                "score": inventory.get("score", {}),
            }
    except Exception:
        current_inventory = {"error": "unable_to_read_multiday_inventory_manifest"}

    return {
        "artifact": ARTIFACT,
        "blockers": sorted(set(str(item) for item in blockers)),
        "case_validation": case_validation,
        "contract": contract(args),
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "current_real_inventory": current_inventory,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "forbidden_blockers": sorted(set(str(item) for item in forbidden)),
        "lane": "observable_pre_action_multiday_same_window_label_handoff_accumulation_contract",
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "reason": "multiday_same_window_label_handoff_accumulation_contract_only",
        },
        "research_ranking": {
            "accumulation_contract_ready": decision == "KEEP" and not case_validation,
            "case_ready": bool(case_validation and decision == "KEEP"),
            "interpretation": (
                "Contract/case validation only. Accumulated handoffs remain offline research inputs and cannot prove "
                "strategy evidence, private truth, deployability, or promotion."
            ),
            "real_miner_input_ready": False,
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
        "schema_version": 1,
        "strategy_evidence": False,
        "supporting_manifests": supporting,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-json", type=Path)
    parser.add_argument("--same-window-contract-manifest", type=Path, default=DEFAULT_SAME_WINDOW_CONTRACT_MANIFEST)
    parser.add_argument("--materializer-manifest", type=Path, default=DEFAULT_MATERIALIZER_MANIFEST)
    parser.add_argument("--multiday-inventory-manifest", type=Path, default=DEFAULT_MULTIDAY_INVENTORY_MANIFEST)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=100)
    parser.add_argument("--min-holdout-rows", type=int, default=50)
    parser.add_argument("--min-join-coverage", type=float, default=1.0)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
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
