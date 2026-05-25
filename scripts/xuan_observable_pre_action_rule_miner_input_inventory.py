#!/usr/bin/env python3
"""Inventory safe inputs for observable pre-action rule miner.

This local-only inventory checks whether current-worktree artifacts can be
assembled into observable_pre_action_rule_miner_input_v1. It does not fetch
data, use SSH, start shadows/services, read events JSONL, scan raw/replay
stores, or change trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_input_inventory"
CONTRACT_NAME = "observable_pre_action_rule_miner_input_inventory_v1"
INPUT_SCHEMA = "observable_pre_action_rule_miner_input_v1"
DEFAULT_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_spec_20260525T182006Z/"
    "manifest.json"
)
DEFAULT_CANDIDATE_ROWS = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
DEFAULT_SOURCE_LINK_SUMMARY = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_source_opportunity_gap_audit_strict_20260523T111500Z/"
    "manifest.json"
)
DEFAULT_NO_ORDER_DENOMINATOR = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_candidate_separator_train_holdout_audit_strict_no_order_20260523T111500Z/"
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

DIRECT_FIELD_ALIASES = {
    "day_id": ("day",),
    "quote_ts_ms": ("ts_ms", "trigger_ts_ms"),
}

DERIVED_FIELD_SOURCES = {
    "open_qty_bucket": ("pre_seed_open_qty",),
    "deficit_bucket": ("pre_seed_same_qty", "pre_seed_opp_qty"),
    "candidate_qty_bucket": ("seed_qty", "trigger_size"),
}

SOURCE_ID_FIELDS = {
    "source_sequence_present": "source_sequence_id",
    "quote_intent_present": "quote_intent_id",
    "source_order_present": "source_order_id",
}


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


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
        return int(float(value))
    except (TypeError, ValueError):
        return default


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def path_has_forbidden_fragment(path: Path) -> bool:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    return any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    if path_has_forbidden_fragment(path):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def read_csv_header_and_count(path: Path) -> tuple[list[str], int, dict[str, Any]]:
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        row_count = 0
        nonempty_source_ids = {field: 0 for field in SOURCE_ID_FIELDS.values()}
        external_shadow_ids_available_true = 0
        schema_versions: set[str] = set()
        days: set[str] = set()
        for row in reader:
            row_count += 1
            schema_versions.add(str(row.get("schema_version", "")))
            day = row.get("day")
            if day:
                days.add(str(day))
            if str(row.get("external_shadow_ids_available", "")).lower() == "true":
                external_shadow_ids_available_true += 1
            for field in nonempty_source_ids:
                if row.get(field):
                    nonempty_source_ids[field] += 1
    return fields, row_count, {
        "nonempty_source_ids": nonempty_source_ids,
        "external_shadow_ids_available_true": external_shadow_ids_available_true,
        "schema_versions": sorted(schema_versions),
        "day_count": len(days),
        "days_sample": sorted(days)[:10],
    }


def load_json_dict(path: Path) -> dict[str, Any]:
    obj = load_json(path)
    return obj if isinstance(obj, dict) else {}


def observe_candidate_rows(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {
            "path": str(path),
            "exists": path.exists(),
            "safe": False,
            "blockers": [reason],
            "ready_as_materialized_candidate_rows": False,
        }
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "safe": True,
            "blockers": ["candidate_rows_missing"],
            "ready_as_materialized_candidate_rows": False,
        }

    fields, row_count, stats = read_csv_header_and_count(path)
    field_set = set(fields)
    direct_present = [field for field in REQUIRED_PRE_ACTION_FIELDS if field in field_set]
    alias_present = {
        field: [alias for alias in DIRECT_FIELD_ALIASES.get(field, ()) if alias in field_set]
        for field in REQUIRED_PRE_ACTION_FIELDS
        if field not in field_set
    }
    alias_present = {field: aliases for field, aliases in alias_present.items() if aliases}
    derived_available = {
        field: list(sources)
        for field, sources in DERIVED_FIELD_SOURCES.items()
        if field not in field_set and all(source in field_set for source in sources)
    }
    source_presence_id_sources = {
        field: source
        for field, source in SOURCE_ID_FIELDS.items()
        if field not in field_set and source in field_set
    }
    materially_missing = [
        field
        for field in REQUIRED_PRE_ACTION_FIELDS
        if field not in field_set
        and field not in alias_present
        and field not in derived_available
        and field not in source_presence_id_sources
    ]

    source_presence_blockers: list[str] = []
    nonempty_ids = as_dict(stats.get("nonempty_source_ids"))
    if source_presence_id_sources:
        if as_int(stats.get("external_shadow_ids_available_true")) == 0:
            source_presence_blockers.append("candidate_rows_external_shadow_ids_unavailable")
        for source in source_presence_id_sources.values():
            if as_int(nonempty_ids.get(source)) == 0:
                source_presence_blockers.append(f"candidate_rows_{source}_empty")

    missing_offline_labels = [field for field in OFFLINE_LABEL_FIELDS if field not in field_set]
    ready = (
        not materially_missing
        and not source_presence_blockers
        and not missing_offline_labels
        and row_count > 0
    )
    blockers = []
    if materially_missing:
        blockers.append("candidate_rows_required_pre_action_fields_missing")
    if source_presence_blockers:
        blockers.append("candidate_rows_source_presence_not_row_level_truth")
    if missing_offline_labels:
        blockers.append("candidate_rows_offline_label_fields_missing")
    if row_count <= 0:
        blockers.append("candidate_rows_empty")

    return {
        "path": str(path),
        "exists": True,
        "safe": True,
        "row_count": row_count,
        "field_count": len(fields),
        "fields": fields,
        "direct_required_pre_action_fields_present": direct_present,
        "alias_required_pre_action_fields_available": alias_present,
        "derived_required_pre_action_fields_available": derived_available,
        "source_presence_id_sources_available": source_presence_id_sources,
        "source_presence_blockers": source_presence_blockers,
        "materially_missing_required_pre_action_fields": materially_missing,
        "missing_offline_label_fields": missing_offline_labels,
        "stats": stats,
        "ready_as_materialized_candidate_rows": ready,
        "blockers": blockers,
    }


def observe_source_link_summary(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {"path": str(path), "exists": path.exists(), "safe": False, "ready": False, "blockers": [reason]}
    if not path.exists():
        return {"path": str(path), "exists": False, "safe": True, "ready": False, "blockers": ["source_link_summary_missing"]}

    data = load_json_dict(path)
    availability = as_dict(data.get("no_order_marker_availability"))
    presence = as_dict(availability.get("source_link_presence_by_status"))
    required_groups = ("source_sequence", "quote_intent", "source_order")
    missing_groups = [group for group in required_groups if group not in presence]
    coverage_available = availability.get("reason_source_coverage_available") is True
    ready = coverage_available and not missing_groups
    blockers = []
    if not coverage_available:
        blockers.append("reason_source_coverage_not_available")
    if missing_groups:
        blockers.append("source_link_presence_groups_missing")
    return {
        "path": str(path),
        "exists": True,
        "safe": True,
        "ready": ready,
        "decision_label": data.get("decision_label"),
        "reason_source_coverage_available": coverage_available,
        "source_link_presence_by_status": presence,
        "missing_presence_groups": missing_groups,
        "aggregate_candidates": as_int(availability.get("aggregate_candidates")),
        "top_block_reasons": availability.get("top_block_reasons"),
        "blockers": blockers,
        "limitation": (
            "This is aggregate source-link evidence; it does not attach source_sequence/quote_intent/source_order "
            "presence to each candidate row."
        ),
    }


def observe_no_order_denominator(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {"path": str(path), "exists": path.exists(), "safe": False, "ready": False, "blockers": [reason]}
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "safe": True,
            "ready": False,
            "blockers": ["no_order_denominator_summary_missing"],
        }

    data = load_json_dict(path)
    no_order = as_dict(data.get("no_order_denominator_summary")) or as_dict(data.get("no_order_reproduction"))
    field_policy = as_dict(data.get("field_policy"))
    split = as_dict(data.get("train_holdout_split"))
    frozen = as_dict(data.get("frozen_rule_contract"))
    denominator_gate = as_dict(data.get("no_order_denominator_gate"))
    available = no_order.get("available") is True
    reproduced = (
        no_order.get("marker_reproduced") is True
        or no_order.get("gate_passed") is True
        or denominator_gate.get("same_window_denominator_reproduced") is True
    )
    marker_total = as_float(no_order.get("source_opportunity_marker_total"), as_float(no_order.get("same_window_denominator_count")))
    candidates = as_int(no_order.get("candidates"), as_int(no_order.get("same_window_denominator_count")))
    missing_required_no_order_fields = [field for field in REQUIRED_NO_ORDER_FIELDS if field not in no_order]
    blockers = []
    if not available:
        blockers.append("no_order_reproduction_not_available")
    if not reproduced:
        blockers.append("old_no_order_marker_not_reproduced")
    if marker_total <= 0:
        blockers.append("old_no_order_marker_total_zero")
    if missing_required_no_order_fields:
        blockers.append("new_frozen_rule_denominator_required_fields_missing")
    if candidates <= 0:
        blockers.append("no_order_candidates_zero")
    if not split:
        blockers.append("train_holdout_split_absent")
    if not frozen:
        blockers.append("frozen_rule_contract_absent")
    if not denominator_gate:
        blockers.append("no_order_denominator_gate_absent")
    ready = (
        available
        and reproduced
        and marker_total > 0
        and not missing_required_no_order_fields
        and bool(split)
        and bool(frozen)
        and bool(denominator_gate)
    )
    return {
        "path": str(path),
        "exists": True,
        "safe": True,
        "ready_as_new_rule_denominator": ready,
        "decision_label": data.get("decision_label"),
        "available": available,
        "old_marker_reproduced": reproduced,
        "old_marker_total": marker_total,
        "old_no_order_candidates": candidates,
        "field_policy": field_policy,
        "no_order_reproduction": no_order,
        "train_holdout_split": split,
        "frozen_rule_contract": frozen,
        "no_order_denominator_gate": denominator_gate,
        "missing_required_no_order_fields": missing_required_no_order_fields,
        "blockers": blockers,
        "limitation": (
            "This denominator belongs to the old source-opportunity/candidate separator marker. It cannot validate "
            "a newly mined frozen rule unless an exact same-window denominator summary is materialized."
        ),
    }


def observe_spec_manifest(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {"path": str(path), "exists": path.exists(), "safe": False, "ready": False, "blockers": [reason]}
    if not path.exists():
        return {"path": str(path), "exists": False, "safe": True, "ready": False, "blockers": ["spec_manifest_missing"]}
    data = load_json_dict(path)
    ready = data.get("decision_label") == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_REAL_INPUT_UNKNOWN"
    return {
        "path": str(path),
        "exists": True,
        "safe": True,
        "ready": ready,
        "decision_label": data.get("decision_label"),
        "real_input_status": data.get("real_input_status"),
        "blockers": [] if ready else ["observable_pre_action_rule_miner_spec_not_ready"],
    }


def build_ready_input_manifest(
    args: argparse.Namespace,
    candidate: dict[str, Any],
    source_link: dict[str, Any],
    denominator: dict[str, Any],
) -> dict[str, Any]:
    fields = list(candidate.get("fields") or [])
    split = as_dict(denominator.get("train_holdout_split"))
    frozen = as_dict(denominator.get("frozen_rule_contract"))
    denominator_gate = as_dict(denominator.get("no_order_denominator_gate"))
    return {
        "artifact": "xuan_observable_pre_action_rule_miner_input_manifest_candidate",
        "schema_version": INPUT_SCHEMA,
        "created_utc": args.created_utc,
        "fixture": args.fixture,
        "strategy_evidence": False,
        "materialized_sources": {
            "candidate_rows": {
                "path": str(args.candidate_rows),
                "row_count": as_int(candidate.get("row_count")),
                "fields": fields,
            },
            "source_link_summary": {
                "path": str(args.source_link_summary),
                "row_count": as_int(source_link.get("aggregate_candidates")),
                "fields": ["source_link_presence_by_status", "top_block_reasons", "reason_source_coverage_available"],
            },
            "no_order_denominator_summary": {
                "path": str(args.no_order_denominator_summary),
                "row_count": as_int(denominator.get("old_no_order_candidates")),
                "fields": list(REQUIRED_NO_ORDER_FIELDS),
            },
        },
        "train_holdout_split": split,
        "feature_policy": {
            "allowed_live_rule_fields": list(REQUIRED_PRE_ACTION_FIELDS),
            "offline_label_fields": list(OFFLINE_LABEL_FIELDS),
        },
        "frozen_rule_contract": frozen,
        "no_order_denominator_gate": denominator_gate,
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "scope": scope_flags(),
    }


def scope_flags() -> dict[str, Any]:
    scope = {
        "current_worktree_only": True,
        "local_only": True,
    }
    for field in REQUIRED_SCOPE_FALSE_FIELDS:
        scope[field] = False
    return scope


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    spec = observe_spec_manifest(args.spec_manifest, root)
    candidate = observe_candidate_rows(args.candidate_rows, root)
    source_link = observe_source_link_summary(args.source_link_summary, root)
    denominator = observe_no_order_denominator(args.no_order_denominator_summary, root)

    blockers: list[str] = []
    for prefix, observation in (
        ("spec", spec),
        ("candidate_rows", candidate),
        ("source_link_summary", source_link),
        ("no_order_denominator_summary", denominator),
    ):
        for blocker in observation.get("blockers", []) or []:
            blockers.append(f"{prefix}:{blocker}")

    candidate_ready = bool(candidate.get("ready_as_materialized_candidate_rows"))
    source_ready = bool(source_link.get("ready"))
    denominator_ready = bool(denominator.get("ready_as_new_rule_denominator"))
    inventory_ready = bool(spec.get("ready")) and candidate_ready and source_ready and denominator_ready

    ready_input_path = None
    validation_command = None
    assembled_input_manifest = None
    if inventory_ready:
        ready_input_path = args.output_dir / "observable_pre_action_rule_miner_input_manifest.json"
        assembled_input_manifest = build_ready_input_manifest(args, candidate, source_link, denominator)
        write_json(ready_input_path, assembled_input_manifest)
        validation_command = (
            "python3 scripts/xuan_observable_pre_action_rule_miner_spec.py "
            f"--candidate-input-manifest {ready_input_path} "
            f"--output-dir {args.output_dir / 'validation'}"
        )

    if inventory_ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_READY"
        next_action = (
            "Run the named validation command against the non-fixture input manifest, then implement the local "
            "rule miner/scorer if the contract passes. Do not start no-order diagnostics."
        )
        input_status = "READY_NON_FIXTURE_INPUT_MANIFEST_AVAILABLE" if not args.fixture else "READY_FIXTURE_ONLY"
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_GAPS"
        input_status = "UNKNOWN_INPUT_PIECES_PRESENT_BUT_NOT_ASSEMBLABLE"
        next_action = (
            "Implement observable_pre_action_rule_miner_feature_join_contract_v1 or provide a safe current-worktree "
            "non-fixture input manifest with row-level source presence and exact same-window no-order denominator; "
            "do not start no-order diagnostics."
        )

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_input_inventory",
        "input_schema_target": INPUT_SCHEMA,
        "fixture": args.fixture,
        "source_inputs": {
            "spec_manifest": str(args.spec_manifest),
            "candidate_rows": str(args.candidate_rows),
            "source_link_summary": str(args.source_link_summary),
            "no_order_denominator_summary": str(args.no_order_denominator_summary),
        },
        "observations": {
            "spec_manifest": spec,
            "candidate_rows": candidate,
            "source_link_summary": source_link,
            "no_order_denominator_summary": denominator,
        },
        "candidate_input_manifest_readiness": {
            "ready": inventory_ready,
            "status": input_status,
            "blockers": blockers,
            "ready_input_manifest": str(ready_input_path) if ready_input_path else None,
            "validation_command": validation_command,
            "summary": {
                "candidate_rows_present": candidate.get("exists") is True,
                "candidate_rows_row_count": candidate.get("row_count"),
                "source_link_summary_present": source_link.get("exists") is True,
                "source_link_aggregate_ready": source_ready,
                "no_order_denominator_present": denominator.get("exists") is True,
                "no_order_new_rule_denominator_ready": denominator_ready,
            },
        },
        "assembled_input_manifest": assembled_input_manifest,
        "blockers": blockers,
        "next_executable_action": next_action,
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "input_inventory_ready": inventory_ready,
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "Inventory only. It may identify safe materialized input paths, but it does not mine rules, "
                "prove private truth, change behavior, or authorize a diagnostic run."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "INPUT_INVENTORY_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "scope": scope_flags(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec-manifest", type=Path, default=DEFAULT_SPEC_MANIFEST)
    parser.add_argument("--candidate-rows", type=Path, default=DEFAULT_CANDIDATE_ROWS)
    parser.add_argument("--source-link-summary", type=Path, default=DEFAULT_SOURCE_LINK_SUMMARY)
    parser.add_argument("--no-order-denominator-summary", type=Path, default=DEFAULT_NO_ORDER_DENOMINATOR)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--fixture", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
