#!/usr/bin/env python3
"""Inventory multi-day same-window label handoff arrivals for accumulator readiness.

This local-only inventory scans current-worktree materialized bridge summaries
and joined-row files, then checks whether enough non-fixture, exact-id-ready
same-window label handoff outputs have arrived for the multi-day accumulator.
It does not read events JSONL, scan raw/replay stores, use SSH, start services,
or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator as accumulator


ARTIFACT = "xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory"
CONTRACT_NAME = "observable_pre_action_multiday_same_window_label_handoff_arrival_inventory_v1"

READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_READY"
UNKNOWN_DECISION = "UNKNOWN_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_GAPS"
DISCARD_DECISION = "DISCARD_OBSERVABLE_PRE_ACTION_MULTIDAY_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_FORBIDDEN_INPUT"


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


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def discover_summaries(root: Path, explicit: list[Path], only_explicit: bool, allow_fixture_like: bool) -> list[Path]:
    candidates = set(explicit)
    if not only_explicit:
        base = root / "xuan_research_artifacts"
        if base.exists():
            for path in base.rglob("observable_pre_action_training_bridge_summary.json"):
                ok, _reason = accumulator.path_safe(path, root)
                if not ok:
                    continue
                if accumulator.fixture_like_path(path) and not allow_fixture_like:
                    continue
                candidates.add(path)
    return sorted(candidates)


def count_duplicate(values: list[str]) -> int:
    counts = Counter(value for value in values if value)
    return sum(count - 1 for count in counts.values() if count > 1)


def present_string(value: Any) -> str:
    return "" if value in (None, "") else str(value)


def inspect_exact_id_readiness(source: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    rows = [row for row in as_list(source.get("rows")) if isinstance(row, dict)]
    handoff_id = str(source.get("source_handoff_id") or "")
    candidate_ids = [present_string(row.get("source_seed_candidate_row_id")) for row in rows]
    action_ids = [present_string(row.get("source_seed_action_id")) for row in rows]
    label_indexes = [present_string(row.get("offline_label_row_index")) for row in rows]
    day_counts = Counter(str(row.get("day_id") or "") for row in rows if str(row.get("day_id") or ""))
    source_sequence_present = sum(1 for row in rows if row.get("source_sequence_present") is True)
    candidate_missing = sum(1 for value in candidate_ids if not value)
    action_present = sum(1 for value in action_ids if value)
    label_index_missing = sum(1 for value in label_indexes if not value)
    duplicate_candidate_count = count_duplicate(candidate_ids)
    duplicate_action_count = count_duplicate(action_ids)
    duplicate_label_index_count = count_duplicate([f"{handoff_id}:{value}" for value in label_indexes])
    source_sequence_coverage = round(source_sequence_present / len(rows), 8) if rows else 0.0
    source_blockers: list[str] = []
    if not handoff_id:
        source_blockers.append("summary_source_handoff_id_missing")
    if not rows:
        source_blockers.append("joined_rows_absent")
    if candidate_missing:
        source_blockers.append(f"candidate_exact_id_missing_count:{candidate_missing}")
    if duplicate_candidate_count:
        source_blockers.append(f"duplicate_candidate_exact_id_count:{duplicate_candidate_count}")
    if duplicate_action_count:
        source_blockers.append(f"duplicate_action_exact_id_count:{duplicate_action_count}")
    if label_index_missing:
        source_blockers.append(f"offline_label_row_index_missing_count:{label_index_missing}")
    if duplicate_label_index_count:
        source_blockers.append(f"offline_label_reuse_count:{duplicate_label_index_count}")
    if source_sequence_coverage < args.min_source_sequence_coverage:
        source_blockers.append("source_sequence_coverage_below_min")
    if len(day_counts) != 1:
        source_blockers.append("source_not_single_day")
    if as_list(source.get("blockers")):
        source_blockers.append("source_contract_blockers_present")
    if as_list(source.get("forbidden")):
        source_blockers.append("source_forbidden_blockers_present")
    return {
        "available_days": sorted(day_counts),
        "blockers": sorted(set(source_blockers)),
        "candidate_exact_id_missing_count": candidate_missing,
        "candidate_exact_id_present_count": len(rows) - candidate_missing,
        "duplicate_action_exact_id_count": duplicate_action_count,
        "duplicate_candidate_exact_id_count": duplicate_candidate_count,
        "duplicate_label_index_count": duplicate_label_index_count,
        "exact_id_ready": not source_blockers,
        "handoff_id": handoff_id,
        "label_index_missing_count": label_index_missing,
        "row_count": len(rows),
        "source_sequence_coverage": source_sequence_coverage,
        "summary_day_counts": dict(sorted(day_counts.items())),
    }


def public_source(source: dict[str, Any], readiness: dict[str, Any]) -> dict[str, Any]:
    return {
        "accepted_pre_global": source.get("accepted_pre_global"),
        "blockers": source.get("blockers", []),
        "exact_id_readiness": readiness,
        "forbidden": source.get("forbidden", []),
        "joined_rows": source.get("joined_rows"),
        "manifest": source.get("manifest"),
        "row_count": source.get("row_count"),
        "source_handoff_id": source.get("source_handoff_id"),
        "summary": source.get("summary"),
        "summary_day_counts": source.get("summary_day_counts"),
        "summary_joined_row_count": source.get("summary_joined_row_count"),
    }


def aggregate_source_metrics(sources: list[dict[str, Any]], readiness: list[dict[str, Any]]) -> dict[str, Any]:
    day_counts: Counter[str] = Counter()
    total_rows = 0
    candidate_present = 0
    candidate_missing = 0
    exact_ready_count = 0
    handoff_ids: list[str] = []
    for source, item in zip(sources, readiness):
        total_rows += as_int(source.get("row_count"))
        candidate_present += as_int(item.get("candidate_exact_id_present_count"))
        candidate_missing += as_int(item.get("candidate_exact_id_missing_count"))
        if item.get("exact_id_ready"):
            exact_ready_count += 1
            handoff_ids.append(str(item.get("handoff_id") or ""))
        for day, count in as_dict(item.get("summary_day_counts")).items():
            day_counts[str(day)] += as_int(count)
    return {
        "accepted_exact_id_ready_bridge_output_count": exact_ready_count,
        "available_day_count": len(day_counts),
        "available_days": sorted(day_counts),
        "candidate_exact_id_missing_count": candidate_missing,
        "candidate_exact_id_present_count": candidate_present,
        "day_counts": dict(sorted(day_counts.items())),
        "distinct_handoff_count": len({value for value in handoff_ids if value}),
        "total_joined_rows": total_rows,
    }


def classify(forbidden: list[str], accepted_sources: list[dict[str, Any]], global_blockers: list[str]) -> tuple[str, str, str]:
    if forbidden:
        return (
            "DISCARD",
            DISCARD_DECISION,
            "Discard forbidden/private/deployable/live-label bridge arrivals; do not mine or replay them.",
        )
    if accepted_sources and not global_blockers:
        return (
            "KEEP",
            READY_DECISION,
            "Run the named multiday accumulator command, then local training scorer and denominator replay only after a frozen rule.",
        )
    return (
        "UNKNOWN",
        UNKNOWN_DECISION,
        "Wait for additional non-fixture same-window handoff materializations with row-level exact ids and source_handoff_id; do not start no-order diagnostics automatically.",
    )


def validation_commands(accepted_sources: list[dict[str, Any]]) -> list[str]:
    bridge_args = " ".join(f"--bridge-summary {source['summary']}" for source in accepted_sources)
    return [
        (
            "python3 scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py "
            f"{bridge_args or '--bridge-summary ${bridge_summary}'} --output-dir ${{accumulator_output_dir}}"
        ),
        (
            "python3 scripts/xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory.py "
            "--only-explicit --bridge-summary ${accumulated_bridge_summary} --output-dir ${inventory_output_dir}"
        ),
        (
            "python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py "
            "--joined-rows ${accumulated_joined_rows} --output-dir ${training_output_dir}"
        ),
    ]


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    contract_manifest, contract_blockers = accumulator.validate_accumulation_contract(
        args.accumulation_contract_manifest,
        root,
    )
    summary_paths = discover_summaries(root, args.bridge_summary, args.only_explicit, args.allow_fixture_like_paths_for_smoke)
    inspected_sources = [accumulator.inspect_source(path, root, args) for path in summary_paths]
    readiness = [inspect_exact_id_readiness(source, args) for source in inspected_sources]
    accepted_sources = [
        source
        for source, item in zip(inspected_sources, readiness)
        if item.get("exact_id_ready")
    ]
    global_blockers: list[str] = []
    global_forbidden: list[str] = []
    accumulator_rows: list[dict[str, Any]] = []
    accumulator_metrics: dict[str, Any] = {}
    if accepted_sources:
        global_blockers, global_forbidden, accumulator_rows, accumulator_metrics = accumulator.validate_and_resplit_sources(
            accepted_sources,
            args,
        )
    else:
        global_blockers.append("non_fixture_exact_id_ready_bridge_output_absent")
    global_blockers = sorted(set(global_blockers + contract_blockers))
    forbidden = sorted(
        set(
            global_forbidden
            + [
                f"source_{idx}:{item}"
                for idx, source in enumerate(inspected_sources)
                for item in as_list(source.get("forbidden"))
            ]
        )
    )
    score = aggregate_source_metrics(inspected_sources, readiness)
    score.update(
        {
            "accumulator_available_day_count": accumulator_metrics.get("available_day_count", 0),
            "accumulator_available_days": accumulator_metrics.get("available_days", []),
            "accumulator_day_counts": accumulator_metrics.get("day_counts", {}),
            "accumulator_handoff_count": accumulator_metrics.get("handoff_count", 0),
            "accumulator_holdout_days": accumulator_metrics.get("holdout_days", []),
            "accumulator_holdout_rows": accumulator_metrics.get("holdout_rows", 0),
            "accumulator_output_row_count_if_ready": len(accumulator_rows),
            "accumulator_source_sequence_coverage": accumulator_metrics.get("source_sequence_coverage", 0.0),
            "accumulator_split_counts": accumulator_metrics.get("split_counts", {}),
            "accumulator_train_days": accumulator_metrics.get("train_days", []),
            "accumulator_train_rows": accumulator_metrics.get("train_rows", 0),
            "candidate_bridge_output_count": len(inspected_sources),
        }
    )
    decision, decision_label, next_action = classify(forbidden, accepted_sources, global_blockers)
    return {
        "artifact": ARTIFACT,
        "blockers": global_blockers,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "forbidden_blockers": forbidden,
        "inputs": {
            "accumulation_contract_manifest": str(args.accumulation_contract_manifest),
            "explicit_bridge_summaries": [str(path) for path in args.bridge_summary],
            "only_explicit": args.only_explicit,
        },
        "inspected_candidates": [
            public_source(source, item)
            for source, item in zip(inspected_sources, readiness)
        ],
        "lane": "observable_pre_action_multiday_same_window_label_handoff_arrival_inventory",
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "reason": "multiday_same_window_label_handoff_arrival_inventory_only",
        },
        "readiness": {
            "accumulator_input_ready": decision == "KEEP",
            "exact_id_ready_bridge_outputs": [
                {
                    "joined_rows": source.get("joined_rows"),
                    "source_handoff_id": source.get("source_handoff_id"),
                    "summary": source.get("summary"),
                }
                for source in accepted_sources
            ],
            "validation_commands": validation_commands(accepted_sources),
        },
        "research_ranking": {
            "accumulator_input_ready": decision == "KEEP",
            "arrival_inventory_ready": decision == "KEEP",
            "interpretation": (
                "Arrival inventory readiness only. KEEP means enough local exact-id-ready same-window bridge outputs "
                "can enter the accumulator; it remains offline research input, not strategy evidence or promotion."
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
            "new_no_order_diagnostic_started": False,
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
        "supporting_contract": {
            "blockers": contract_blockers,
            "decision_label": contract_manifest.get("decision_label") if isinstance(contract_manifest, dict) else None,
            "path": str(args.accumulation_contract_manifest),
            "ready": not contract_blockers,
        },
        "validation_commands": validation_commands(accepted_sources),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge-summary", type=Path, action="append", default=[])
    parser.add_argument("--only-explicit", action="store_true")
    parser.add_argument("--allow-fixture-like-paths-for-smoke", action="store_true")
    parser.add_argument(
        "--accumulation-contract-manifest",
        type=Path,
        default=accumulator.DEFAULT_ACCUMULATION_CONTRACT,
    )
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
