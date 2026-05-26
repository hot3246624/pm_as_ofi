#!/usr/bin/env python3
"""Accumulate multi-day same-window bridge rows into one train/holdout input.

This local-only accumulator consumes already materialized observable pre-action
training bridge summaries and joined-row JSONL files. It enforces global exact-id
uniqueness, chronologically resplits rows into train/holdout, and emits a single
bridge summary/JSONL only when the accumulated input satisfies the committed
multi-day accumulation contract. It does not read events JSONL, scan raw/replay
stores, use SSH, start services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator"
CONTRACT_NAME = "observable_pre_action_multiday_same_window_label_handoff_accumulator_v1"
ACCUMULATION_CONTRACT_NAME = "observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_v1"
SUMMARY_SCHEMA = "observable_pre_action_training_bridge_summary_v1"
JOINED_ROW_SCHEMA = "observable_pre_action_training_bridge_joined_row_v1"
SOURCE_MATERIALIZER_CONTRACT = "observable_pre_action_non_fixture_training_bridge_materializer_v1"

READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_READY"
UNKNOWN_DECISION = "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_GAPS"
DISCARD_DECISION = "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATOR_FORBIDDEN_INPUT"

DEFAULT_ACCUMULATION_CONTRACT = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_multiday_same_window_label_handoff_accumulation_contract_20260526T160213Z/"
    "manifest.json"
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

FIXTURE_PATH_MARKERS = (
    "_smoke_",
    "_postmerge",
    "/fixtures/",
    "/fixture/",
    "/good/",
    "/bad",
    "/tmp_specs/",
    "fixture_scorer",
    "synthetic_fixture",
)

FALSE_SUMMARY_FIELDS = (
    "post_action_labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "events_jsonl_read",
    "events_jsonl_pulled",
    "raw_replay_or_full_store_scan",
    "trading_behavior_changed",
    "strategy_evidence",
    "private_truth_ready",
    "deployable",
)

REQUIRED_LABEL_FIELDS = (
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)

REQUIRED_FALSE_ROW_FIELDS = (
    "post_action_labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "trading_behavior_changed",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError(f"{path}:{line_no} is not a JSON object")
        rows.append(row)
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


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


def fixture_like_path(path: Path) -> bool:
    lowered = "/" + "/".join(path.parts).lower()
    return any(marker in lowered for marker in FIXTURE_PATH_MARKERS)


def sibling_joined_rows(summary_path: Path) -> Path:
    return summary_path.parent / "observable_pre_action_training_bridge_joined_rows.jsonl"


def sibling_manifest(summary_path: Path) -> Path:
    return summary_path.parent / "manifest.json"


def validate_accumulation_contract(path: Path, root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    ok, reason = path_safe(path, root)
    if not ok:
        return {}, [f"accumulation_contract_{reason}"]
    if not path.exists():
        return {}, ["accumulation_contract_missing"]
    data = read_json(path)
    if not isinstance(data, dict):
        return {}, ["accumulation_contract_not_json_object"]
    if data.get("contract_name") != ACCUMULATION_CONTRACT_NAME:
        blockers.append("accumulation_contract_name_mismatch")
    if data.get("decision_label") != "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ACCUMULATION_CONTRACT_READY":
        blockers.append("accumulation_contract_not_ready")
    if data.get("private_truth_ready") is True or data.get("deployable") is True:
        blockers.append("accumulation_contract_private_or_deployable_claim")
    if as_dict(data.get("promotion_gate")).get("passed") is True:
        blockers.append("accumulation_contract_promotion_claim")
    return data, blockers


def validate_summary(summary: dict[str, Any], args: argparse.Namespace) -> tuple[list[str], list[str], str]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if summary.get("schema_version") != SUMMARY_SCHEMA:
        blockers.append("summary_schema_mismatch")
    if summary.get("fixture_only") is True:
        forbidden.append("summary_fixture_only_true")
    if summary.get("materializer_contract") != SOURCE_MATERIALIZER_CONTRACT:
        blockers.append("summary_materializer_contract_mismatch")
    source_handoff_id = str(summary.get("source_handoff_id") or summary.get("handoff_id") or "")
    if not source_handoff_id:
        blockers.append("summary_source_handoff_id_missing")
    if as_int(summary.get("joined_row_count"), -1) <= 0:
        blockers.append("summary_joined_row_count_missing")
    if as_int(summary.get("feature_row_count"), -1) != as_int(summary.get("joined_row_count"), -2):
        blockers.append("summary_feature_joined_row_count_mismatch")
    if as_int(summary.get("offline_label_row_count"), -1) != as_int(summary.get("joined_row_count"), -2):
        blockers.append("summary_label_joined_row_count_mismatch")
    if as_float(summary.get("join_coverage")) < args.min_join_coverage:
        blockers.append("summary_join_coverage_below_min")
    if as_float(summary.get("source_sequence_coverage")) < args.min_source_sequence_coverage:
        blockers.append("summary_source_sequence_coverage_below_min")
    if as_int(summary.get("label_row_reuse_count"), 0) != 0:
        blockers.append("summary_label_row_reuse_present")
    available_days = [str(day) for day in as_list(summary.get("available_days")) if str(day)]
    if len(set(available_days)) != 1:
        blockers.append("summary_not_single_day")
    if as_dict(summary.get("promotion_gate")).get("passed") is True:
        forbidden.append("summary_promotion_claim")
    for field in FALSE_SUMMARY_FIELDS:
        if summary.get(field) is not False:
            forbidden.append(f"summary_{field}_not_false")
    return blockers, forbidden, source_handoff_id


def validate_manifest(manifest: dict[str, Any]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if not manifest:
        blockers.append("materializer_manifest_missing")
        return blockers, forbidden
    if manifest.get("materialized") is not True:
        blockers.append("materializer_manifest_not_materialized")
    if manifest.get("strategy_evidence") is not False:
        forbidden.append("materializer_manifest_strategy_evidence_claim")
    if manifest.get("private_truth_ready") is not False or manifest.get("deployable") is not False:
        forbidden.append("materializer_manifest_private_or_deployable_claim")
    if as_dict(manifest.get("promotion_gate")).get("passed") is True:
        forbidden.append("materializer_manifest_promotion_claim")
    safety = as_dict(manifest.get("safety"))
    for field in (
        "events_jsonl_read",
        "events_jsonl_pulled",
        "raw_replay_or_full_store_scan",
        "ssh_used",
        "canary_or_live_started",
        "orders_sent",
        "cancels_sent",
        "redeems_sent",
        "trading_behavior_changed",
    ):
        if safety.get(field, False) is not False:
            forbidden.append(f"materializer_manifest_safety_{field}_not_false")
    if manifest.get("blockers"):
        blockers.append("materializer_manifest_blockers_present")
    return blockers, forbidden


def inspect_source(summary_path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    blockers: list[str] = []
    forbidden: list[str] = []
    rows: list[dict[str, Any]] = []
    source_handoff_id = ""
    if fixture_like_path(summary_path) and not args.allow_fixture_like_paths_for_smoke:
        blockers.append("source_fixture_or_smoke_path")
    ok, reason = path_safe(summary_path, root)
    if not ok:
        blockers.append(f"summary_path_{reason}")
    if not summary_path.exists():
        blockers.append("summary_missing")
        summary = {}
    else:
        try:
            summary_raw = read_json(summary_path)
            summary = summary_raw if isinstance(summary_raw, dict) else {}
        except Exception as exc:  # pragma: no cover - represented in manifest.
            summary = {}
            blockers.append(f"summary_{type(exc).__name__}:{exc}")
    if summary:
        summary_blockers, summary_forbidden, source_handoff_id = validate_summary(summary, args)
        blockers.extend(summary_blockers)
        forbidden.extend(summary_forbidden)

    manifest_path = sibling_manifest(summary_path)
    manifest: dict[str, Any] = {}
    ok, reason = path_safe(manifest_path, root)
    if not ok:
        blockers.append(f"manifest_path_{reason}")
    elif not manifest_path.exists():
        blockers.append("manifest_missing")
    else:
        try:
            manifest_raw = read_json(manifest_path)
            manifest = manifest_raw if isinstance(manifest_raw, dict) else {}
        except Exception as exc:  # pragma: no cover - represented in manifest.
            blockers.append(f"manifest_{type(exc).__name__}:{exc}")
    manifest_blockers, manifest_forbidden = validate_manifest(manifest)
    blockers.extend(manifest_blockers)
    forbidden.extend(manifest_forbidden)

    rows_path = sibling_joined_rows(summary_path)
    ok, reason = path_safe(rows_path, root)
    if not ok:
        blockers.append(f"joined_rows_path_{reason}")
    elif not rows_path.exists():
        blockers.append("joined_rows_missing")
    else:
        try:
            rows = load_jsonl(rows_path)
        except Exception as exc:  # pragma: no cover - represented in manifest.
            blockers.append(f"joined_rows_{type(exc).__name__}:{exc}")
    expected_count = as_int(summary.get("joined_row_count"), -1) if summary else -1
    if rows and expected_count != len(rows):
        blockers.append("joined_rows_summary_count_mismatch")

    day_counts = Counter(str(row.get("day_id") or "") for row in rows if str(row.get("day_id") or ""))
    return {
        "accepted_pre_global": not blockers and not forbidden,
        "blockers": sorted(set(blockers)),
        "forbidden": sorted(set(forbidden)),
        "joined_rows": str(rows_path),
        "manifest": str(manifest_path),
        "row_count": len(rows),
        "rows": rows,
        "source_handoff_id": source_handoff_id,
        "summary": str(summary_path),
        "summary_day_counts": dict(sorted(day_counts.items())),
        "summary_joined_row_count": expected_count,
    }


def split_days(days: list[str], min_holdout_days: int) -> tuple[set[str], set[str]]:
    unique_days = sorted({day for day in days if day})
    if len(unique_days) < min_holdout_days:
        return set(unique_days), set()
    holdout_days = set(unique_days[-min_holdout_days:])
    train_days = set(unique_days).difference(holdout_days)
    return train_days, holdout_days


def validate_and_resplit_sources(sources: list[dict[str, Any]], args: argparse.Namespace) -> tuple[list[str], list[str], list[dict[str, Any]], dict[str, Any]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    handoff_ids: set[str] = set()
    candidate_ids: set[str] = set()
    action_ids: set[str] = set()
    bridge_ids: set[str] = set()
    label_keys: set[str] = set()
    all_rows: list[tuple[str, dict[str, Any], str]] = []
    day_counts: Counter[str] = Counter()
    source_sequence_present = 0

    for source_index, source in enumerate(sources):
        source_forbidden = [f"source_{source_index}:{item}" for item in as_list(source.get("forbidden"))]
        forbidden.extend(source_forbidden)
        blockers.extend(f"source_{source_index}:{item}" for item in as_list(source.get("blockers")))
        handoff_id = str(source.get("source_handoff_id") or "")
        if handoff_id:
            if handoff_id in handoff_ids:
                blockers.append("duplicate_handoff_id")
            handoff_ids.add(handoff_id)
        row_issue_counts: Counter[str] = Counter()
        missing_label_fields: Counter[str] = Counter()
        for row_index, row in enumerate(as_list(source.get("rows"))):
            if not isinstance(row, dict):
                row_issue_counts["not_object"] += 1
                continue
            if row.get("schema_version") != JOINED_ROW_SCHEMA:
                row_issue_counts["schema_mismatch"] += 1
            bridge_id = str(row.get("bridge_row_id") or "")
            if not bridge_id:
                row_issue_counts["bridge_row_id_missing"] += 1
            elif bridge_id in bridge_ids:
                blockers.append("duplicate_bridge_row_id_across_sources")
            bridge_ids.add(bridge_id)
            candidate_id = str(row.get("source_seed_candidate_row_id") or "")
            if not candidate_id:
                row_issue_counts["candidate_exact_id_missing"] += 1
            elif candidate_id in candidate_ids:
                blockers.append("duplicate_candidate_exact_id_across_sources")
            if candidate_id:
                candidate_ids.add(candidate_id)
            action_id = str(row.get("source_seed_action_id") or "")
            if action_id:
                if action_id in action_ids:
                    blockers.append("duplicate_action_exact_id_across_sources")
                action_ids.add(action_id)
            raw_label_index = row.get("offline_label_row_index")
            label_key = f"{handoff_id}:{raw_label_index}"
            if raw_label_index in (None, ""):
                row_issue_counts["offline_label_row_index_missing"] += 1
            elif label_key in label_keys:
                blockers.append("offline_label_reuse_within_handoff")
            label_keys.add(label_key)
            if row.get("post_action_labels_allowed_for_train_holdout_scoring") is not True:
                row_issue_counts["label_train_holdout_policy_missing"] += 1
            for field in REQUIRED_FALSE_ROW_FIELDS:
                if row.get(field) is not False:
                    forbidden.append(f"source_{source_index}_{field}_not_false")
            if row.get("private_truth_ready") is True or row.get("deployable") is True:
                forbidden.append(f"source_{source_index}_private_or_deployable_claim")
            for field in REQUIRED_LABEL_FIELDS:
                if row.get(field) in (None, ""):
                    missing_label_fields[field] += 1
            day = str(row.get("day_id") or "")
            if not day:
                row_issue_counts["day_id_missing"] += 1
            else:
                day_counts[day] += 1
            if row.get("source_sequence_present") is True:
                source_sequence_present += 1
            all_rows.append((handoff_id, row, str(source.get("summary") or "")))
        for issue, count in sorted(row_issue_counts.items()):
            blockers.append(f"source_{source_index}_{issue}_count:{count}")
        for field, count in sorted(missing_label_fields.items()):
            blockers.append(f"source_{source_index}_label_field_missing_count:{field}:{count}")

    train_days, holdout_days = split_days(list(day_counts), args.min_holdout_days)
    resplit_rows: list[dict[str, Any]] = []
    split_counts: Counter[str] = Counter()
    for handoff_id, row, summary_path in all_rows:
        day = str(row.get("day_id") or "")
        if day in train_days:
            split = "train"
        elif day in holdout_days:
            split = "holdout"
        else:
            split = "unused"
        output_row = dict(row)
        output_row["split"] = split
        output_row["accumulation_source_handoff_id"] = handoff_id
        output_row["accumulation_source_summary"] = summary_path
        output_row["accumulation_contract"] = CONTRACT_NAME
        resplit_rows.append(output_row)
        split_counts[split] += 1

    train_rows = int(split_counts.get("train", 0))
    holdout_rows = int(split_counts.get("holdout", 0))
    source_sequence_coverage = round(source_sequence_present / len(all_rows), 8) if all_rows else 0.0
    readiness_blockers: list[str] = []
    if len(train_days) < args.min_train_days:
        readiness_blockers.append("train_day_diversity_below_min")
    if len(holdout_days) < args.min_holdout_days:
        readiness_blockers.append("holdout_day_diversity_below_min")
    if train_rows < args.min_train_rows:
        readiness_blockers.append("train_rows_below_min")
    if holdout_rows < args.min_holdout_rows:
        readiness_blockers.append("holdout_rows_below_min")
    if source_sequence_coverage < args.min_source_sequence_coverage:
        readiness_blockers.append("source_sequence_coverage_below_min")
    if split_counts.get("unused", 0):
        readiness_blockers.append("unused_rows_after_accumulation")
    metrics = {
        "available_day_count": len(day_counts),
        "available_days": sorted(day_counts),
        "candidate_exact_id_count": len(candidate_ids),
        "day_counts": dict(sorted(day_counts.items())),
        "handoff_count": len(handoff_ids),
        "holdout_days": sorted(holdout_days),
        "holdout_rows": holdout_rows,
        "label_row_reuse_count": max(0, len(all_rows) - len(label_keys)),
        "source_sequence_coverage": source_sequence_coverage,
        "split_counts": dict(sorted(split_counts.items())),
        "train_days": sorted(train_days),
        "train_rows": train_rows,
    }
    blockers.extend(readiness_blockers)
    return sorted(set(blockers)), sorted(set(forbidden)), resplit_rows, metrics


def summary_from_rows(rows: list[dict[str, Any]], metrics: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    return {
        "schema_version": SUMMARY_SCHEMA,
        "fixture_only": args.fixture_output,
        "materializer_contract": SOURCE_MATERIALIZER_CONTRACT,
        "accumulator_contract": CONTRACT_NAME,
        "accumulation_contract": ACCUMULATION_CONTRACT_NAME,
        "join_policy_version": "observable_pre_action_training_input_bridge_join_policy_v1",
        "feature_row_count": len(rows),
        "offline_label_row_count": len(rows),
        "joined_row_count": len(rows),
        "join_coverage": 1.0 if rows else 0.0,
        "join_method_counts": {"exact_id:source_seed_candidate_row_id": len(rows)},
        "split_counts": metrics.get("split_counts", {}),
        "train_days": metrics.get("train_days", []),
        "holdout_days": metrics.get("holdout_days", []),
        "available_days": metrics.get("available_days", []),
        "source_sequence_coverage": metrics.get("source_sequence_coverage", 0.0),
        "label_row_reuse_count": metrics.get("label_row_reuse_count", 0),
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


def classify(blockers: list[str], forbidden: list[str]) -> tuple[str, str, str]:
    if forbidden:
        return (
            "DISCARD",
            DISCARD_DECISION,
            "Discard forbidden/private/deployable/live-label accumulated bridge input; do not mine or replay it.",
        )
    if not blockers:
        return (
            "KEEP",
            READY_DECISION,
            "Run multiday bridge inventory on the accumulated summary, then the offline training scorer; denominator replay remains separate after a frozen rule.",
        )
    return (
        "UNKNOWN",
        UNKNOWN_DECISION,
        "Fix accumulator blockers or wait for enough non-fixture same-window handoffs; do not start no-order diagnostics automatically.",
    )


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    contract_manifest, contract_blockers = validate_accumulation_contract(args.accumulation_contract_manifest, root)
    sources = [inspect_source(path, root, args) for path in args.bridge_summary]
    blockers, forbidden, accumulated_rows, metrics = validate_and_resplit_sources(sources, args)
    blockers = sorted(set(blockers + contract_blockers))
    decision, decision_label, next_action = classify(blockers, forbidden)
    materialized = decision == "KEEP"
    outputs = {
        "joined_rows": str(args.output_dir / "observable_pre_action_training_bridge_joined_rows.jsonl")
        if materialized
        else None,
        "summary": str(args.output_dir / "observable_pre_action_training_bridge_summary.json") if materialized else None,
    }
    output_summary = summary_from_rows(accumulated_rows, metrics, args)
    if materialized:
        write_jsonl(args.output_dir / "observable_pre_action_training_bridge_joined_rows.jsonl", accumulated_rows)
        write_json(args.output_dir / "observable_pre_action_training_bridge_summary.json", output_summary)

    return {
        "artifact": ARTIFACT,
        "blockers": blockers,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "forbidden_blockers": forbidden,
        "fixture_only": args.fixture_output,
        "inputs": {
            "accumulation_contract_manifest": str(args.accumulation_contract_manifest),
            "bridge_summaries": [str(path) for path in args.bridge_summary],
        },
        "lane": "observable_pre_action_multiday_same_window_label_handoff_accumulator",
        "materialized": materialized,
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "outputs": outputs,
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "reason": "multiday_same_window_label_handoff_accumulator_only",
        },
        "research_ranking": {
            "accumulated_bridge_rows_ready": materialized,
            "interpretation": (
                "Accumulator readiness only. Rows remain offline research inputs and cannot prove strategy evidence, "
                "private truth, deployability, or promotion."
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
        "score": metrics,
        "source_inspections": [
            {key: value for key, value in source.items() if key != "rows"}
            for source in sources
        ],
        "strategy_evidence": False,
        "supporting_contract": {
            "blockers": contract_blockers,
            "decision_label": contract_manifest.get("decision_label") if isinstance(contract_manifest, dict) else None,
            "path": str(args.accumulation_contract_manifest),
            "ready": not contract_blockers,
        },
        "validation_commands": [
            (
                "python3 scripts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory.py "
                "--only-explicit --bridge-summary ${accumulated_bridge_summary} --output-dir ${inventory_output_dir}"
            ),
            (
                "python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py "
                "--joined-rows ${accumulated_joined_rows} --output-dir ${training_output_dir}"
            ),
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge-summary", type=Path, action="append", required=True)
    parser.add_argument("--accumulation-contract-manifest", type=Path, default=DEFAULT_ACCUMULATION_CONTRACT)
    parser.add_argument("--allow-fixture-like-paths-for-smoke", action="store_true")
    parser.add_argument("--fixture-output", action="store_true")
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
