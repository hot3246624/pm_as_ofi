#!/usr/bin/env python3
"""Audit whether feature-join rows can safely bridge to offline labels.

This local-only audit checks if allowlisted observable pre-action feature rows
can be joined to an in-worktree offline label CSV under the committed bridge
contract. It does not write joined rows, train rules, read events JSONL, scan
raw/replay stores, use SSH, start services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_feature_join_offline_label_bridge_feasibility_audit"
CONTRACT_NAME = "feature_join_offline_label_bridge_feasibility_audit_v1"
ROW_SCHEMA = "observable_pre_action_candidate_row_v1"
BRIDGE_CONTRACT_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_READY"
FEATURE_SCORER_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY"

DEFAULT_FEATURE_ROWS = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_feature_join_handoff_driver_20260526T020736Z/"
    "remote_clean/output/observable_pre_action_candidate_rows.jsonl"
)
DEFAULT_FEATURE_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_feature_join_handoff_completion_20260526T041336Z/"
    "feature_join_scorer/manifest.json"
)
DEFAULT_OFFLINE_LABEL_CSV = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
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

FORBIDDEN_LIVE_KEY_FRAGMENTS = (
    "source_pair",
    "source_residual",
    "realized_pair_cost",
    "settlement",
    "redeem",
    "future_label",
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


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), [dict(row) for row in reader]


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
    blockers: list[str] = []
    for path in paths:
        ok, reason = path_safe(path, root)
        if not ok:
            blockers.append(f"{path.name}_{reason}")
        elif not path.exists():
            blockers.append(f"{path.name}_missing")
    return blockers


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


def compact_values(values: set[str], limit: int = 12) -> list[str]:
    return sorted(values)[:limit]


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


def exact_match(feature: dict[str, Any], exact_indexes: dict[str, dict[str, list[int]]]) -> tuple[str | None, list[int]]:
    for field in EXACT_ID_FIELDS:
        value = str(feature.get(field) or "")
        if not value:
            continue
        return field, list(exact_indexes.get(field, {}).get(value, []))
    return None, []


def audit_joinability(
    *,
    feature_rows: list[dict[str, Any]],
    label_rows: list[dict[str, str]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    exact_indexes, composite_index = build_label_indexes(label_rows)
    exact_joined = 0
    composite_joined = 0
    zero_exact = 0
    multiple_exact = 0
    zero_composite = 0
    multiple_composite = 0
    outside_tolerance_samples: list[dict[str, Any]] = []
    ambiguous_samples: list[dict[str, Any]] = []

    for idx, feature in enumerate(feature_rows):
        exact_field, exact_matches = exact_match(feature, exact_indexes)
        if exact_field is not None:
            if len(exact_matches) == 1:
                exact_joined += 1
                continue
            if len(exact_matches) > 1:
                multiple_exact += 1
                if len(ambiguous_samples) < args.sample_limit:
                    ambiguous_samples.append({"feature_row_index": idx, "reason": f"multiple_exact_id_matches:{exact_field}", "match_count": len(exact_matches)})
                continue
            zero_exact += 1
            continue

        candidates = composite_index.get(composite_base_key(feature), [])
        matches = [label_idx for label_idx in candidates if composite_match(feature, label_rows[label_idx], args)]
        if len(matches) == 1:
            composite_joined += 1
        elif len(matches) > 1:
            multiple_composite += 1
            if len(ambiguous_samples) < args.sample_limit:
                ambiguous_samples.append({"feature_row_index": idx, "reason": "multiple_composite_matches", "match_count": len(matches)})
        else:
            zero_composite += 1
            if candidates and len(outside_tolerance_samples) < args.sample_limit:
                outside_tolerance_samples.append(
                    {
                        "feature_row_index": idx,
                        "base_key_candidate_count": len(candidates),
                        "condition_id": feature.get("condition_id"),
                        "market_slug": feature.get("market_slug"),
                        "day_id": feature.get("day_id"),
                    }
                )

    joined = exact_joined + composite_joined
    return {
        "ambiguous_samples": ambiguous_samples,
        "composite_candidate_base_key_overlap_count": sum(1 for row in feature_rows if composite_base_key(row) in composite_index),
        "composite_joined": composite_joined,
        "exact_joined": exact_joined,
        "join_coverage": round(joined / len(feature_rows), 6) if feature_rows else 0.0,
        "joined_rows": joined,
        "multiple_composite_matches": multiple_composite,
        "multiple_exact_matches": multiple_exact,
        "outside_tolerance_samples": outside_tolerance_samples,
        "zero_composite_matches": zero_composite,
        "zero_exact_matches": zero_exact,
    }


def validate_supporting_inputs(feature_rows: list[dict[str, Any]], feature_scorer: dict[str, Any], bridge_contract: dict[str, Any], fields: list[str], labels: list[dict[str, str]]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if not feature_rows:
        blockers.append("feature_rows_empty")
    if feature_scorer.get("decision_label") != FEATURE_SCORER_DECISION:
        blockers.append("feature_scorer_decision_not_keep")
    if bridge_contract.get("decision_label") != BRIDGE_CONTRACT_DECISION:
        blockers.append("bridge_contract_decision_not_keep")

    field_set = set(fields)
    missing_join = [field for field in CSV_JOIN_FIELDS if field not in field_set]
    missing_labels = [field for field in OFFLINE_LABEL_FIELDS if field not in field_set]
    missing_policy = [field for field in CSV_POLICY_FIELDS if field not in field_set]
    if missing_join:
        blockers.append("offline_label_join_fields_missing")
    if missing_labels:
        blockers.append("offline_label_fields_missing")
    if missing_policy:
        blockers.append("offline_label_policy_fields_missing")
    if not labels:
        blockers.append("offline_label_rows_empty")

    for idx, row in enumerate(feature_rows):
        if row.get("schema_version") != ROW_SCHEMA:
            blockers.append(f"feature_row_{idx}_schema_mismatch")
        missing = [field for field in FEATURE_REQUIRED_FIELDS if field not in row]
        if missing:
            blockers.append(f"feature_row_{idx}_required_fields_missing:{','.join(missing)}")
        if row.get("post_action_outcome_labels_included") is not False:
            forbidden.append(f"feature_row_{idx}_post_action_labels_present")
        if row.get("realized_pair_cost_used_as_live_criteria") is not False:
            forbidden.append(f"feature_row_{idx}_realized_pair_cost_live_criteria")
        if row.get("trading_behavior_changed") is not False:
            forbidden.append(f"feature_row_{idx}_trading_behavior_changed")
        for key in row:
            if key == "realized_pair_cost_used_as_live_criteria":
                continue
            if any(fragment in key for fragment in FORBIDDEN_LIVE_KEY_FRAGMENTS):
                forbidden.append(f"feature_row_{idx}_forbidden_live_key:{key}")
    post_action_policy_missing_count = 0
    live_policy_not_safe_count = 0
    label_field_missing_counts: dict[str, int] = {}
    for label in labels:
        if not truthy_text(label.get("outcome_labels_are_post_action")):
            post_action_policy_missing_count += 1
        if not label_live_policy_safe(label.get("live_rule_safety_policy")):
            live_policy_not_safe_count += 1
        for field in OFFLINE_LABEL_FIELDS:
            if field in field_set and not present(label.get(field)):
                label_field_missing_counts[field] = label_field_missing_counts.get(field, 0) + 1
    if post_action_policy_missing_count:
        blockers.append(f"offline_label_post_action_policy_missing_count:{post_action_policy_missing_count}")
    if live_policy_not_safe_count:
        forbidden.append(f"offline_label_live_rule_safety_policy_not_safe_count:{live_policy_not_safe_count}")
    for field, count in sorted(label_field_missing_counts.items()):
        blockers.append(f"offline_label_field_missing_count:{field}:{count}")
    return blockers, forbidden


def inventory(feature_rows: list[dict[str, Any]], labels: list[dict[str, str]]) -> dict[str, Any]:
    feature_days = {str(row.get("day_id") or "") for row in feature_rows}
    label_days = {str(row.get("day") or "") for row in labels}
    feature_slugs = {str(row.get("market_slug") or "") for row in feature_rows}
    label_slugs = {str(row.get("slug") or "") for row in labels}
    feature_conditions = {str(row.get("condition_id") or "") for row in feature_rows}
    label_conditions = {str(row.get("condition_id") or "") for row in labels}
    feature_status_counts: dict[str, int] = {}
    for row in feature_rows:
        status = str(row.get("status_before_action") or "unknown")
        feature_status_counts[status] = feature_status_counts.get(status, 0) + 1
    return {
        "condition_overlap_count": len(feature_conditions.intersection(label_conditions)),
        "day_overlap": compact_values(feature_days.intersection(label_days)),
        "feature_condition_count": len(feature_conditions),
        "feature_day_sample": compact_values(feature_days),
        "feature_exact_id_present_counts": {
            field: sum(1 for row in feature_rows if present(row.get(field))) for field in EXACT_ID_FIELDS
        },
        "feature_row_count": len(feature_rows),
        "feature_slug_count": len(feature_slugs),
        "feature_status_counts": feature_status_counts,
        "label_condition_count": len(label_conditions),
        "label_day_sample": compact_values(label_days, limit=20),
        "label_exact_id_present_counts": {
            field: sum(1 for row in labels if present(row.get(field))) for field in EXACT_ID_FIELDS
        },
        "label_row_count": len(labels),
        "label_slug_count": len(label_slugs),
        "slug_overlap_count": len(feature_slugs.intersection(label_slugs)),
    }


def decision_from(blockers: list[str], forbidden: list[str], joinability: dict[str, Any], args: argparse.Namespace) -> tuple[str, str, list[str]]:
    all_blockers = sorted(set(blockers))
    if forbidden:
        return "DISCARD", "DISCARD_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_FORBIDDEN_LIVE_OR_PROVENANCE_VIOLATION", sorted(set(forbidden + all_blockers))
    if all_blockers:
        return "UNKNOWN", "UNKNOWN_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_INPUT_CONTRACT_GAPS", all_blockers
    if int(joinability.get("joined_rows") or 0) < args.min_joined_rows:
        all_blockers.append("joined_rows_below_min")
    if float(joinability.get("join_coverage") or 0.0) < args.min_join_coverage:
        all_blockers.append("join_coverage_below_min")
    if int(joinability.get("multiple_exact_matches") or 0) or int(joinability.get("multiple_composite_matches") or 0):
        all_blockers.append("ambiguous_join_matches_present")
    if all_blockers:
        return "UNKNOWN", "UNKNOWN_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_NOT_JOINABLE", sorted(set(all_blockers))
    return "KEEP", "KEEP_FEATURE_JOIN_OFFLINE_LABEL_BRIDGE_FEASIBLE_VALIDATION_TARGET_READY", []


def build_manifest(args: argparse.Namespace, blockers: list[str], forbidden: list[str], observations: dict[str, Any], joinability: dict[str, Any]) -> dict[str, Any]:
    decision, decision_label, final_blockers = decision_from(blockers, forbidden, joinability, args)
    validation_commands = []
    if decision == "KEEP":
        validation_commands = [
            "python3 scripts/xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer.py "
            "--feature-rows {feature_rows} "
            "--source-link-summary <same-window-source-link-summary.json> "
            "--feature-join-manifest <same-window-feature-join-manifest.json> "
            "--offline-label-csv {offline_label_csv} "
            "--frozen-rule-manifest <frozen_observable_pre_action_rule_manifest.json> "
            "--output-dir <non_fixture_training_bridge_output_dir>"
        ]
    return {
        "artifact": ARTIFACT,
        "blockers": final_blockers,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "inputs": {
            "bridge_contract_manifest": str(args.bridge_contract_manifest),
            "feature_rows": str(args.feature_rows),
            "feature_scorer_manifest": str(args.feature_scorer_manifest),
            "offline_label_csv": str(args.offline_label_csv),
        },
        "join_policy": {
            "candidate_qty_label_fields": ["seed_qty", "trigger_size"],
            "composite_base_fields": ["condition_id", "market_slug/slug", "day_id/day", "side", "source_risk_direction"],
            "cost_tolerance": args.cost_tolerance,
            "exact_id_fields": list(EXACT_ID_FIELDS),
            "offset_tolerance_s": args.offset_tolerance_s,
            "qty_tolerance": args.qty_tolerance,
            "timestamp_tolerance_ms": args.timestamp_tolerance_ms,
        },
        "joinability": joinability,
        "next_executable_action": next_action(decision),
        "no_order_diagnostic_allowed": False,
        "observations": observations,
        "private_truth_ready": False,
        "promotion_gate": {"passed": False, "reason": "offline_label_bridge_feasibility_audit_only"},
        "research_ranking": {
            "bridge_feasible": decision == "KEEP",
            "interpretation": "Feasibility audit only. KEEP names a safe join target; UNKNOWN means the current worktree cannot safely assemble non-fixture training rows. Neither is promotion evidence.",
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
        "validation_commands": validation_commands,
    }


def next_action(decision: str) -> str:
    if decision == "KEEP":
        return "Run the named non-fixture training bridge validation chain, then continue to training/denominator replay only if every scorer remains KEEP; do not start no-order diagnostics."
    if decision == "DISCARD":
        return "Do not use this bridge path; return to local-only strategy/input inventory without public-profile/static-cap/future-label alternatives."
    return "Specify or wait for a same-window offline-label handoff that overlaps the feature-join rows by exact ids or strict composite keys; do not start no-order diagnostics automatically."


def run(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    paths = [args.feature_rows, args.feature_scorer_manifest, args.offline_label_csv, args.bridge_contract_manifest]
    path_blockers = validate_paths(paths, root)
    if path_blockers:
        manifest = build_manifest(args, path_blockers, [], {}, {})
        write_json(args.output_dir / "manifest.json", manifest)
        return manifest

    feature_rows = load_jsonl(args.feature_rows)
    feature_scorer = read_json(args.feature_scorer_manifest)
    bridge_contract = read_json(args.bridge_contract_manifest)
    fields, labels = load_csv(args.offline_label_csv)

    blockers, forbidden = validate_supporting_inputs(feature_rows, feature_scorer, bridge_contract, fields, labels)
    observations = inventory(feature_rows, labels)
    joinability = audit_joinability(feature_rows=feature_rows, label_rows=labels, args=args)

    if observations.get("day_overlap") == []:
        blockers.append("feature_label_day_overlap_absent")
    if observations.get("condition_overlap_count") == 0:
        blockers.append("feature_label_condition_overlap_absent")
    if observations.get("slug_overlap_count") == 0:
        blockers.append("feature_label_slug_overlap_absent")
    if sum(observations.get("feature_exact_id_present_counts", {}).values()) == 0:
        blockers.append("feature_exact_id_fields_absent")

    manifest = build_manifest(args, blockers, forbidden, observations, joinability)
    write_json(args.output_dir / "manifest.json", manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-rows", type=Path, default=DEFAULT_FEATURE_ROWS)
    parser.add_argument("--feature-scorer-manifest", type=Path, default=DEFAULT_FEATURE_SCORER_MANIFEST)
    parser.add_argument("--offline-label-csv", type=Path, default=DEFAULT_OFFLINE_LABEL_CSV)
    parser.add_argument("--bridge-contract-manifest", type=Path, default=DEFAULT_BRIDGE_CONTRACT_MANIFEST)
    parser.add_argument("--output-dir", type=Path, default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}")
    parser.add_argument("--created-utc", default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    parser.add_argument("--timestamp-tolerance-ms", type=float, default=250.0)
    parser.add_argument("--offset-tolerance-s", type=float, default=0.25)
    parser.add_argument("--qty-tolerance", type=float, default=0.000001)
    parser.add_argument("--cost-tolerance", type=float, default=0.000001)
    parser.add_argument("--min-joined-rows", type=int, default=100)
    parser.add_argument("--min-join-coverage", type=float, default=0.95)
    parser.add_argument("--sample-limit", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = run(args)
    print(json.dumps({"decision_label": manifest["decision_label"], "blockers": manifest["blockers"]}, sort_keys=True))


if __name__ == "__main__":
    main()
