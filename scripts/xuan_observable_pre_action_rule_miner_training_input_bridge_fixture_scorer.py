#!/usr/bin/env python3
"""Score fixture training-input bridge joins for observable pre-action mining.

This local-only scorer joins synthetic/allowlisted feature-join rows to offline
source_pair/source_residual label rows under the committed bridge contract. It
writes joined rows and a bridge summary only when the join is one-to-one,
pre-action/live-label separation is intact, and provenance checks pass. It does
not read events JSONL, raw/replay stores, use SSH, start shadows/services, or
change trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer"
CONTRACT_NAME = "observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_v1"
JOIN_POLICY_VERSION = "observable_pre_action_training_input_bridge_join_policy_v1"

ROW_SCHEMA = "observable_pre_action_candidate_row_v1"
SOURCE_SUMMARY_SCHEMA = "observable_pre_action_source_link_summary_v1"
FEATURE_MANIFEST_SCHEMA = "observable_pre_action_feature_join_manifest_v1"
FROZEN_RULE_SCHEMA = "frozen_observable_pre_action_rule_manifest_v1"
BRIDGE_CONTRACT_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_READY"

DEFAULT_BRIDGE_CONTRACT_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_contract_20260525T213507Z/"
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

REQUIRED_FEATURE_FIELDS = (
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
    "candidate_qty",
    "candidate_qty_bucket",
    "source_risk_direction",
    "post_action_outcome_labels_included",
    "realized_pair_cost_used_as_live_criteria",
    "trading_behavior_changed",
)

CSV_JOIN_FIELDS = (
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
)

EXACT_ID_FIELDS = (
    "source_seed_candidate_row_id",
    "source_seed_action_id",
)

OFFLINE_LABEL_FIELDS = (
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)

CSV_POLICY_FIELDS = (
    "outcome_labels_are_post_action",
    "pre_action_field_policy",
    "post_action_label_policy",
    "live_rule_safety_policy",
)

FORBIDDEN_LIVE_FEATURES = set(OFFLINE_LABEL_FIELDS).union(
    {
        "source_pair",
        "source_residual",
        "realized_pair_cost",
        "settlement",
        "redeem",
        "future_label",
        "private_truth",
        "public_profile_single_day_profit",
        "static_side_offset_price_cap",
        "dplus_failed_family_marker",
    }
)

FIELD_CONTRACT_FALSE_KEYS = (
    "writes_events_jsonl",
    "reads_events_jsonl",
    "raw_replay_or_full_store_scan",
    "post_action_outcome_labels_included",
    "realized_pair_cost_used_as_live_criteria",
    "trading_behavior_changed",
    "strategy_evidence",
    "private_truth_ready",
    "deployable",
    "promotion_gate_passed",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


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


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        rows = [dict(row) for row in reader]
        return list(reader.fieldnames or []), rows


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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


def truthy_text(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def present(value: Any) -> bool:
    return value is not None and str(value) != ""


def digest_obj(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    if any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def validate_paths(paths: list[Path], root: Path) -> list[str]:
    blockers = []
    for path in paths:
        ok, reason = path_safe(path, root)
        if not ok:
            blockers.append(f"{path.name}_{reason}")
        elif not path.exists():
            blockers.append(f"{path.name}_missing")
    return blockers


def field_contract_blockers(field_contract: dict[str, Any], prefix: str) -> list[str]:
    blockers = []
    if field_contract.get("default_off") is not True:
        blockers.append(f"{prefix}_default_off_not_true")
    for key in FIELD_CONTRACT_FALSE_KEYS:
        if field_contract.get(key) is not False:
            blockers.append(f"{prefix}_{key}_not_false")
    return blockers


def validate_feature_sources(
    *,
    rows: list[dict[str, Any]],
    source_summary: dict[str, Any],
    feature_manifest: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not rows:
        blockers.append("feature_rows_empty")
    if source_summary.get("schema_version") != SOURCE_SUMMARY_SCHEMA:
        blockers.append("source_link_summary_schema_mismatch")
    if feature_manifest.get("schema_version") != FEATURE_MANIFEST_SCHEMA:
        blockers.append("feature_join_manifest_schema_mismatch")
    if as_int(source_summary.get("row_count"), -1) != len(rows):
        blockers.append("source_link_summary_row_count_mismatch")
    if as_int(feature_manifest.get("candidate_row_count"), -1) != len(rows):
        blockers.append("feature_join_manifest_candidate_row_count_mismatch")

    feature_contract = as_dict(feature_manifest.get("field_contract"))
    if feature_contract:
        blockers.extend(field_contract_blockers(feature_contract, "feature_manifest_field_contract"))
    source_contract = as_dict(source_summary.get("field_contract"))
    if source_contract:
        blockers.extend(field_contract_blockers(source_contract, "source_summary_field_contract"))

    statuses = {str(row.get("status_before_action") or "") for row in rows}
    if "admitted" not in statuses:
        blockers.append("admitted_feature_status_missing")
    if "blocked" not in statuses:
        blockers.append("blocked_feature_status_missing")

    for idx, row in enumerate(rows):
        if row.get("schema_version") != ROW_SCHEMA:
            blockers.append(f"feature_row_{idx}_schema_mismatch")
        missing = [field for field in REQUIRED_FEATURE_FIELDS if field not in row]
        if missing:
            blockers.append(f"feature_row_{idx}_required_fields_missing:{','.join(missing)}")
        if row.get("post_action_outcome_labels_included") is not False:
            blockers.append(f"feature_row_{idx}_post_action_labels_present")
        if row.get("realized_pair_cost_used_as_live_criteria") is not False:
            blockers.append(f"feature_row_{idx}_realized_pair_cost_live_criteria")
        if row.get("trading_behavior_changed") is not False:
            blockers.append(f"feature_row_{idx}_trading_behavior_changed")
        if row.get("private_truth_ready") is True or row.get("deployable") is True:
            blockers.append(f"feature_row_{idx}_private_or_deployable_claim")
        forbidden_keys = sorted(set(row).intersection(OFFLINE_LABEL_FIELDS))
        if forbidden_keys:
            blockers.append(f"feature_row_{idx}_offline_label_fields_present:{','.join(forbidden_keys)}")
    return blockers


def validate_bridge_contract(contract_manifest: dict[str, Any]) -> list[str]:
    blockers = []
    if contract_manifest.get("decision_label") != BRIDGE_CONTRACT_DECISION:
        blockers.append("bridge_contract_decision_not_keep")
    if contract_manifest.get("private_truth_ready") is True or contract_manifest.get("deployable") is True:
        blockers.append("bridge_contract_private_or_deployable_claim")
    promotion = as_dict(contract_manifest.get("promotion_gate"))
    if promotion.get("passed") is True:
        blockers.append("bridge_contract_promotion_claim")
    join_policy = as_dict(as_dict(contract_manifest.get("contract")).get("join_key_policy"))
    if not join_policy:
        blockers.append("bridge_contract_join_policy_missing")
    return blockers


def validate_label_csv(fields: list[str], labels: list[dict[str, str]]) -> list[str]:
    blockers = []
    field_set = set(fields)
    if not labels:
        blockers.append("offline_label_rows_empty")
    missing_join = [field for field in CSV_JOIN_FIELDS if field not in field_set]
    missing_labels = [field for field in OFFLINE_LABEL_FIELDS if field not in field_set]
    missing_policy = [field for field in CSV_POLICY_FIELDS if field not in field_set]
    if missing_join:
        blockers.append("offline_label_join_fields_missing")
    if missing_labels:
        blockers.append("offline_label_fields_missing")
    if missing_policy:
        blockers.append("offline_label_policy_fields_missing")
    for idx, row in enumerate(labels):
        if row.get("schema_version") and row.get("schema_version") != "candidate_seed_outcome_separator_v1":
            blockers.append(f"offline_label_row_{idx}_schema_mismatch")
        if not truthy_text(row.get("outcome_labels_are_post_action")):
            blockers.append(f"offline_label_row_{idx}_post_action_label_policy_missing")
        live_policy = str(row.get("live_rule_safety_policy") or "")
        if "forbidden" not in live_policy:
            blockers.append(f"offline_label_row_{idx}_live_rule_safety_policy_not_forbidden")
        for label_field in OFFLINE_LABEL_FIELDS:
            if not present(row.get(label_field)):
                blockers.append(f"offline_label_row_{idx}_label_field_missing:{label_field}")
    return blockers


def validate_frozen_rule(frozen_rule: dict[str, Any], min_train_days: int, min_holdout_days: int) -> tuple[list[str], dict[str, Any]]:
    blockers = []
    if frozen_rule.get("schema_version") != FROZEN_RULE_SCHEMA:
        blockers.append("frozen_rule_schema_mismatch")
    if not frozen_rule.get("frozen_rule_id"):
        blockers.append("frozen_rule_id_missing")
    predicate_fields = [str(field) for field in as_list(frozen_rule.get("predicate_feature_names"))]
    if not predicate_fields:
        blockers.append("predicate_feature_names_missing")
    forbidden = []
    for field in predicate_fields:
        if field in FORBIDDEN_LIVE_FEATURES or any(token in field for token in ("source_pair", "source_residual")):
            forbidden.append(field)
    if forbidden:
        blockers.append("label_used_as_live_predicate")
    if frozen_rule.get("uses_only_pre_action_fields") is not True:
        blockers.append("frozen_rule_not_pre_action_only")
    if frozen_rule.get("uses_realized_pair_cost") is not False:
        blockers.append("realized_pair_cost_used_as_live_criteria")
    if frozen_rule.get("uses_future_labels") is not False:
        blockers.append("future_labels_used_as_live_criteria")
    if frozen_rule.get("private_truth_ready") is True or frozen_rule.get("deployable") is True:
        blockers.append("frozen_rule_private_or_deployable_claim")
    promotion = as_dict(frozen_rule.get("promotion_gate"))
    if promotion.get("passed") is True:
        blockers.append("frozen_rule_promotion_claim")
    for label_field in as_list(frozen_rule.get("train_label_fields")) + as_list(frozen_rule.get("holdout_label_fields")):
        if str(label_field) not in OFFLINE_LABEL_FIELDS:
            blockers.append("train_holdout_label_field_not_allowlisted")
    split = as_dict(frozen_rule.get("train_holdout_split"))
    train_days = {str(day) for day in as_list(split.get("train_days"))}
    holdout_days = {str(day) for day in as_list(split.get("holdout_days"))}
    if len(train_days) < min_train_days:
        blockers.append("train_days_below_min")
    if len(holdout_days) < min_holdout_days:
        blockers.append("holdout_days_below_min")
    if train_days.intersection(holdout_days):
        blockers.append("train_holdout_days_overlap")
    return blockers, {
        "train_days": sorted(train_days),
        "holdout_days": sorted(holdout_days),
        "predicate_feature_names": predicate_fields,
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


def composite_match(feature: dict[str, Any], label: dict[str, str], args: argparse.Namespace) -> bool:
    exact_pairs = (
        ("condition_id", "condition_id"),
        ("market_slug", "slug"),
        ("day_id", "day"),
        ("side", "side"),
        ("source_risk_direction", "source_risk_direction"),
    )
    for feature_field, label_field in exact_pairs:
        if str(feature.get(feature_field) or "") != str(label.get(label_field) or ""):
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


def find_label_match(
    feature: dict[str, Any],
    labels: list[dict[str, str]],
    fields: set[str],
    args: argparse.Namespace,
) -> tuple[int | None, str | None, list[str]]:
    for exact_field in EXACT_ID_FIELDS:
        if present(feature.get(exact_field)) and exact_field in fields:
            feature_value = str(feature.get(exact_field))
            matches = [idx for idx, label in enumerate(labels) if str(label.get(exact_field) or "") == feature_value]
            if len(matches) == 1:
                return matches[0], f"exact_id:{exact_field}", []
            if len(matches) > 1:
                return None, None, [f"multiple_exact_id_matches:{exact_field}"]
            return None, None, [f"zero_exact_id_match:{exact_field}"]
    matches = [idx for idx, label in enumerate(labels) if composite_match(feature, label, args)]
    if len(matches) == 1:
        return matches[0], "composite_fallback", []
    if len(matches) > 1:
        return None, None, ["multiple_composite_matches"]
    return None, None, ["zero_composite_match"]


def joined_row(
    *,
    feature_idx: int,
    feature: dict[str, Any],
    label_idx: int,
    label: dict[str, str],
    method: str,
    split: str,
) -> dict[str, Any]:
    feature_hash = digest_obj(feature)
    label_hash = digest_obj(label)
    bridge_row_id = digest_obj({"feature": feature_hash, "label": label_hash, "policy": JOIN_POLICY_VERSION})
    row = {
        "schema_version": "observable_pre_action_training_bridge_joined_row_v1",
        "bridge_row_id": bridge_row_id,
        "feature_row_index": feature_idx,
        "offline_label_row_index": label_idx,
        "feature_row_hash": feature_hash,
        "offline_label_row_hash": label_hash,
        "join_policy_version": JOIN_POLICY_VERSION,
        "join_method": method,
        "join_confidence": "exact" if method.startswith("exact_id:") else "composite_strict",
        "split": split,
        "condition_id": feature.get("condition_id"),
        "market_slug": feature.get("market_slug"),
        "day_id": feature.get("day_id"),
        "quote_ts_ms": feature.get("quote_ts_ms"),
        "side": feature.get("side"),
        "offset_s": feature.get("offset_s"),
        "status_before_action": feature.get("status_before_action"),
        "reason": feature.get("reason"),
        "block_reason": feature.get("block_reason"),
        "source_sequence_present": feature.get("source_sequence_present"),
        "quote_intent_present": feature.get("quote_intent_present"),
        "source_order_present": feature.get("source_order_present"),
        "source_risk_direction": feature.get("source_risk_direction"),
        "pre_seed_same_qty": feature.get("pre_seed_same_qty"),
        "pre_seed_opp_qty": feature.get("pre_seed_opp_qty"),
        "pre_seed_same_cost": feature.get("pre_seed_same_cost"),
        "pre_seed_opp_cost": feature.get("pre_seed_opp_cost"),
        "pre_seed_open_qty": feature.get("pre_seed_open_qty"),
        "pre_seed_open_cost": feature.get("pre_seed_open_cost"),
        "candidate_qty": feature.get("candidate_qty"),
        "post_action_labels_allowed_for_train_holdout_scoring": True,
        "post_action_labels_allowed_in_live_predicate": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "future_labels_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }
    for field in OFFLINE_LABEL_FIELDS:
        row[field] = label.get(field)
    return row


def score(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    paths = [
        args.feature_rows,
        args.source_link_summary,
        args.feature_join_manifest,
        args.offline_label_csv,
        args.frozen_rule_manifest,
        args.bridge_contract_manifest,
    ]
    blockers = validate_paths(paths, root)
    if blockers:
        return build_manifest(args, blockers, [], {}, {}, None)

    feature_rows = load_jsonl(args.feature_rows)
    source_summary = read_json(args.source_link_summary)
    feature_manifest = read_json(args.feature_join_manifest)
    frozen_rule = read_json(args.frozen_rule_manifest)
    bridge_contract = read_json(args.bridge_contract_manifest)
    label_fields, labels = load_csv(args.offline_label_csv)

    blockers.extend(validate_bridge_contract(bridge_contract))
    blockers.extend(validate_feature_sources(rows=feature_rows, source_summary=source_summary, feature_manifest=feature_manifest))
    blockers.extend(validate_label_csv(label_fields, labels))
    rule_blockers, split = validate_frozen_rule(frozen_rule, args.min_train_days, args.min_holdout_days)
    blockers.extend(rule_blockers)

    joined: list[dict[str, Any]] = []
    used_label_indexes: set[int] = set()
    join_method_counts: dict[str, int] = {}
    row_match_blockers: list[str] = []
    label_field_set = set(label_fields)
    train_days = set(split.get("train_days", []))
    holdout_days = set(split.get("holdout_days", []))
    for feature_idx, feature in enumerate(feature_rows):
        label_idx, method, match_blockers = find_label_match(feature, labels, label_field_set, args)
        if match_blockers:
            row_match_blockers.extend(f"feature_row_{feature_idx}_{blocker}" for blocker in match_blockers)
            continue
        assert label_idx is not None and method is not None
        if label_idx in used_label_indexes:
            row_match_blockers.append(f"feature_row_{feature_idx}_label_row_reuse")
            continue
        used_label_indexes.add(label_idx)
        day = str(feature.get("day_id") or "")
        split_name = "train" if day in train_days else ("holdout" if day in holdout_days else "unused")
        joined.append(
            joined_row(
                feature_idx=feature_idx,
                feature=feature,
                label_idx=label_idx,
                label=labels[label_idx],
                method=method,
                split=split_name,
            )
        )
        join_method_counts[method] = join_method_counts.get(method, 0) + 1
    blockers.extend(row_match_blockers)

    split_counts = {
        "train": sum(1 for row in joined if row["split"] == "train"),
        "holdout": sum(1 for row in joined if row["split"] == "holdout"),
        "unused": sum(1 for row in joined if row["split"] == "unused"),
    }
    if split_counts["train"] < args.min_train_rows:
        blockers.append("joined_train_rows_below_min")
    if split_counts["holdout"] < args.min_holdout_rows:
        blockers.append("joined_holdout_rows_below_min")
    if len(joined) != len(feature_rows):
        blockers.append("not_all_feature_rows_joined")
    if sum(count for method, count in join_method_counts.items() if method.startswith("exact_id:")) <= 0:
        blockers.append("exact_id_join_not_exercised")
    if join_method_counts.get("composite_fallback", 0) <= 0:
        blockers.append("composite_fallback_join_not_exercised")

    summary = {
        "schema_version": "observable_pre_action_training_bridge_summary_v1",
        "fixture_only": bool(args.fixture),
        "join_policy_version": JOIN_POLICY_VERSION,
        "feature_row_count": len(feature_rows),
        "offline_label_row_count": len(labels),
        "joined_row_count": len(joined),
        "join_method_counts": join_method_counts,
        "split_counts": split_counts,
        "train_days": split.get("train_days", []),
        "holdout_days": split.get("holdout_days", []),
        "label_row_reuse_count": max(0, len(joined) - len(used_label_indexes)),
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

    ready = not blockers
    joined_path = args.output_dir / "observable_pre_action_training_bridge_joined_rows.jsonl"
    summary_path = args.output_dir / "observable_pre_action_training_bridge_summary.json"
    if ready:
        write_jsonl(joined_path, joined)
        write_json(summary_path, summary)

    return build_manifest(
        args,
        sorted(set(blockers)),
        joined,
        summary,
        join_method_counts,
        {
            "joined_rows": str(joined_path) if ready else None,
            "summary": str(summary_path) if ready else None,
        },
    )


def build_manifest(
    args: argparse.Namespace,
    blockers: list[str],
    joined: list[dict[str, Any]],
    summary: dict[str, Any],
    join_method_counts: dict[str, int],
    outputs: dict[str, str | None] | None,
) -> dict[str, Any]:
    ready = not blockers
    return {
        "artifact": ARTIFACT,
        "contract_name": CONTRACT_NAME,
        "created_utc": utc_label(),
        "decision": "KEEP" if ready else "UNKNOWN",
        "decision_label": (
            "KEEP_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_READY"
            if ready
            else "UNKNOWN_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_FAIL_CLOSED"
        ),
        "fixture_only": bool(args.fixture),
        "blockers": blockers,
        "inputs": {
            "feature_rows": str(args.feature_rows),
            "source_link_summary": str(args.source_link_summary),
            "feature_join_manifest": str(args.feature_join_manifest),
            "offline_label_csv": str(args.offline_label_csv),
            "frozen_rule_manifest": str(args.frozen_rule_manifest),
            "bridge_contract_manifest": str(args.bridge_contract_manifest),
        },
        "outputs": outputs or {"joined_rows": None, "summary": None},
        "score": {
            "joined_row_count": len(joined),
            "join_method_counts": join_method_counts,
            "split_counts": summary.get("split_counts") if summary else None,
            "label_separation_ok": ready,
            "aggregate_provenance_ok": ready,
        },
        "research_ranking": (
            "TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_READY_NO_STRATEGY_EVIDENCE"
            if ready
            else "TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_FAIL_CLOSED"
        ),
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "training_input_bridge_fixture_scorer_only",
        },
        "safety": {
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scan": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "trading_behavior_changed": False,
        },
        "next_executable_action": (
            "Implement observable_pre_action_rule_miner_training_input_real_wait_or_local_miner_spec_v1: "
            "either wait for a non-fixture feature-join pullback with offline-label bridge inputs, or define the "
            "offline miner training spec over this joined-row contract; do not start no-order diagnostics."
            if ready
            else "Fix bridge fixture join/provenance/label-separation blockers before mining."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--feature-rows", type=Path, required=True)
    parser.add_argument("--source-link-summary", type=Path, required=True)
    parser.add_argument("--feature-join-manifest", type=Path, required=True)
    parser.add_argument("--offline-label-csv", type=Path, required=True)
    parser.add_argument("--frozen-rule-manifest", type=Path, required=True)
    parser.add_argument("--bridge-contract-manifest", type=Path, default=DEFAULT_BRIDGE_CONTRACT_MANIFEST)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timestamp-tolerance-ms", type=float, default=250.0)
    parser.add_argument("--offset-tolerance-s", type=float, default=0.25)
    parser.add_argument("--qty-tolerance", type=float, default=0.000001)
    parser.add_argument("--cost-tolerance", type=float, default=0.000001)
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=3)
    parser.add_argument("--min-holdout-rows", type=int, default=2)
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
