#!/usr/bin/env python3
"""Specify offline training for observable pre-action rule mining.

This local-only spec defines how future non-fixture bridge-joined rows may be
mined offline into a frozen observable pre-action rule. It validates the current
fixture bridge output only as a schema/leakage proof. It does not mine a real
rule, read events JSONL, scan raw/replay stores, use SSH, start shadows, or
change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_training_spec"
CONTRACT_NAME = "observable_pre_action_rule_miner_training_spec_v1"

JOINED_ROW_SCHEMA = "observable_pre_action_training_bridge_joined_row_v1"
BRIDGE_SUMMARY_SCHEMA = "observable_pre_action_training_bridge_summary_v1"
EXPECTED_BRIDGE_SCORER_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_TRAINING_INPUT_BRIDGE_FIXTURE_SCORER_READY"

DEFAULT_BRIDGE_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/"
    "manifest.json"
)
DEFAULT_JOINED_ROWS = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/"
    "observable_pre_action_training_bridge_joined_rows.jsonl"
)
DEFAULT_BRIDGE_SUMMARY = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/"
    "observable_pre_action_training_bridge_summary.json"
)
DEFAULT_DENOMINATOR_SPEC = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_denominator_replay_scorer_spec_20260525T195536Z/"
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

REQUIRED_JOINED_ROW_FIELDS = (
    "schema_version",
    "bridge_row_id",
    "feature_row_hash",
    "offline_label_row_hash",
    "join_policy_version",
    "join_method",
    "join_confidence",
    "split",
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
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "candidate_qty",
    "post_action_labels_allowed_for_train_holdout_scoring",
    "post_action_labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "trading_behavior_changed",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)

PRE_ACTION_LIVE_FEATURE_FAMILIES = (
    "status_before_action",
    "reason",
    "block_reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "side",
    "offset_bucket_derived_from_offset_s",
    "offset_s_bucketized",
    "pre_seed_same_qty_bucket",
    "pre_seed_opp_qty_bucket",
    "pre_seed_open_qty_bucket",
    "pre_seed_deficit_qty_bucket_derived",
    "pre_seed_same_cost_bucket",
    "pre_seed_opp_cost_bucket",
    "pre_seed_open_cost_bucket",
    "candidate_qty_bucket",
    "source_risk_direction",
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

FORBIDDEN_LIVE_FEATURE_FAMILIES = (
    "source_pair",
    "source_residual",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "realized_pair_cost",
    "settlement",
    "redeem",
    "future_label",
    "private_truth",
    "public_profile_single_day_profit",
    "static_side_offset_price_cap",
    "dplus_failed_family_marker",
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


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


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


def scope_flags() -> dict[str, Any]:
    scope = {"current_worktree_only": True, "local_only": True}
    for field in REQUIRED_SCOPE_FALSE_FIELDS:
        scope[field] = False
    return scope


def validate_bridge_scorer(manifest: dict[str, Any]) -> list[str]:
    blockers = []
    if manifest.get("decision_label") != EXPECTED_BRIDGE_SCORER_DECISION:
        blockers.append("bridge_fixture_scorer_not_ready")
    if manifest.get("strategy_evidence") is not False:
        blockers.append("bridge_fixture_scorer_strategy_evidence_not_false")
    if manifest.get("no_order_diagnostic_allowed") is not False:
        blockers.append("bridge_fixture_scorer_no_order_allowed_not_false")
    if manifest.get("private_truth_ready") is not False:
        blockers.append("bridge_fixture_scorer_private_truth_not_false")
    if manifest.get("deployable") is not False:
        blockers.append("bridge_fixture_scorer_deployable_not_false")
    promotion = as_dict(manifest.get("promotion_gate"))
    if promotion.get("passed") is not False:
        blockers.append("bridge_fixture_scorer_promotion_not_false")
    safety = as_dict(manifest.get("safety"))
    for key in (
        "events_jsonl_read",
        "events_jsonl_pulled",
        "raw_replay_or_full_store_scan",
        "ssh_used",
        "shadow_started",
        "canary_or_live_started",
        "orders_sent",
        "cancels_sent",
        "redeems_sent",
        "trading_behavior_changed",
    ):
        if safety.get(key) is not False:
            blockers.append(f"bridge_fixture_scorer_safety_{key}_not_false")
    return blockers


def validate_summary(summary: dict[str, Any], rows: list[dict[str, Any]], args: argparse.Namespace) -> list[str]:
    blockers = []
    if summary.get("schema_version") != BRIDGE_SUMMARY_SCHEMA:
        blockers.append("bridge_summary_schema_mismatch")
    if summary.get("joined_row_count") != len(rows):
        blockers.append("bridge_summary_joined_row_count_mismatch")
    if summary.get("post_action_labels_allowed_in_live_predicate") is not False:
        blockers.append("bridge_summary_label_live_predicate_not_false")
    if summary.get("realized_pair_cost_used_as_live_criteria") is not False:
        blockers.append("bridge_summary_realized_pair_cost_live_criteria_not_false")
    if summary.get("future_labels_used_as_live_criteria") is not False:
        blockers.append("bridge_summary_future_labels_live_criteria_not_false")
    if summary.get("trading_behavior_changed") is not False:
        blockers.append("bridge_summary_trading_behavior_changed_not_false")
    if summary.get("private_truth_ready") is not False or summary.get("deployable") is not False:
        blockers.append("bridge_summary_private_or_deployable_claim")
    promotion = as_dict(summary.get("promotion_gate"))
    if promotion.get("passed") is not False:
        blockers.append("bridge_summary_promotion_claim")
    split_counts = as_dict(summary.get("split_counts"))
    if int(split_counts.get("train") or 0) < args.min_fixture_train_rows:
        blockers.append("fixture_train_rows_below_min")
    if int(split_counts.get("holdout") or 0) < args.min_fixture_holdout_rows:
        blockers.append("fixture_holdout_rows_below_min")
    if len(as_list(summary.get("train_days"))) < args.min_fixture_train_days:
        blockers.append("fixture_train_days_below_min")
    if len(as_list(summary.get("holdout_days"))) < args.min_fixture_holdout_days:
        blockers.append("fixture_holdout_days_below_min")
    if set(as_list(summary.get("train_days"))).intersection({str(day) for day in as_list(summary.get("holdout_days"))}):
        blockers.append("fixture_train_holdout_days_overlap")
    if int(summary.get("label_row_reuse_count") or 0) != 0:
        blockers.append("label_row_reuse_not_zero")
    return blockers


def validate_joined_rows(rows: list[dict[str, Any]], summary: dict[str, Any]) -> list[str]:
    blockers = []
    if not rows:
        return ["joined_rows_empty"]
    bridge_ids = set()
    feature_hashes = set()
    label_hashes = set()
    splits = set()
    for idx, row in enumerate(rows):
        if row.get("schema_version") != JOINED_ROW_SCHEMA:
            blockers.append(f"joined_row_{idx}_schema_mismatch")
        missing = [field for field in REQUIRED_JOINED_ROW_FIELDS if field not in row]
        if missing:
            blockers.append(f"joined_row_{idx}_required_fields_missing:{','.join(missing)}")
        for key in ("bridge_row_id", "feature_row_hash", "offline_label_row_hash"):
            value = row.get(key)
            if not value:
                blockers.append(f"joined_row_{idx}_{key}_missing")
        if row.get("bridge_row_id") in bridge_ids:
            blockers.append("bridge_row_id_duplicate")
        bridge_ids.add(row.get("bridge_row_id"))
        if row.get("feature_row_hash") in feature_hashes:
            blockers.append("feature_row_hash_duplicate")
        feature_hashes.add(row.get("feature_row_hash"))
        if row.get("offline_label_row_hash") in label_hashes:
            blockers.append("offline_label_row_hash_duplicate")
        label_hashes.add(row.get("offline_label_row_hash"))
        if row.get("post_action_labels_allowed_for_train_holdout_scoring") is not True:
            blockers.append(f"joined_row_{idx}_labels_not_allowed_for_train_holdout")
        if row.get("post_action_labels_allowed_in_live_predicate") is not False:
            blockers.append(f"joined_row_{idx}_label_live_predicate_not_false")
        if row.get("realized_pair_cost_used_as_live_criteria") is not False:
            blockers.append(f"joined_row_{idx}_realized_pair_cost_live_criteria_not_false")
        if row.get("future_labels_used_as_live_criteria") is not False:
            blockers.append(f"joined_row_{idx}_future_labels_live_criteria_not_false")
        if row.get("trading_behavior_changed") is not False:
            blockers.append(f"joined_row_{idx}_trading_behavior_changed_not_false")
        splits.add(str(row.get("split") or ""))
        for label_field in OFFLINE_LABEL_FIELDS:
            if row.get(label_field) in (None, ""):
                blockers.append(f"joined_row_{idx}_offline_label_missing:{label_field}")
    if "train" not in splits:
        blockers.append("train_split_missing")
    if "holdout" not in splits:
        blockers.append("holdout_split_missing")
    method_counts = as_dict(summary.get("join_method_counts"))
    if int(method_counts.get("composite_fallback") or 0) <= 0:
        blockers.append("composite_fallback_join_not_present")
    if not any(str(key).startswith("exact_id:") and int(value or 0) > 0 for key, value in method_counts.items()):
        blockers.append("exact_id_join_not_present")
    return blockers


def validate_denominator_spec(manifest: dict[str, Any]) -> list[str]:
    blockers = []
    if manifest.get("decision_label") != "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_SCORER_SPEC_READY":
        blockers.append("denominator_replay_spec_not_ready")
    if manifest.get("strategy_evidence") is not False:
        blockers.append("denominator_replay_spec_strategy_evidence_not_false")
    if manifest.get("no_order_diagnostic_allowed") is not False:
        blockers.append("denominator_replay_spec_no_order_allowed_not_false")
    if manifest.get("private_truth_ready") is not False or manifest.get("deployable") is not False:
        blockers.append("denominator_replay_spec_private_or_deployable_claim")
    promotion = as_dict(manifest.get("promotion_gate"))
    if promotion.get("passed") is not False:
        blockers.append("denominator_replay_spec_promotion_claim")
    return blockers


def training_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "purpose": (
            "Define offline train/holdout mining over bridge-joined rows. The miner may use only pre-action "
            "observable features for predicates and post-action labels only as train/holdout objectives."
        ),
        "input_schema": {
            "joined_rows": "observable_pre_action_training_bridge_joined_row_v1",
            "bridge_summary": "observable_pre_action_training_bridge_summary_v1",
        },
        "allowed_live_feature_families": list(PRE_ACTION_LIVE_FEATURE_FAMILIES),
        "derived_pre_action_features_allowed": {
            "offset_bucket": "derived from offset_s only",
            "qty_cost_buckets": "derived from pre_seed/candidate fields only",
            "deficit_bucket": "derived from pre_seed_opp_qty - pre_seed_same_qty only",
            "source_presence_crosses": "derived from source_sequence/quote_intent/source_order booleans only",
        },
        "forbidden_live_feature_families": list(FORBIDDEN_LIVE_FEATURE_FAMILIES),
        "label_objectives": {
            "train_holdout_label_fields": list(OFFLINE_LABEL_FIELDS),
            "primary_objective": (
                "maximize fee-agnostic offline source_pair_pnl while penalizing source_residual_cost and "
                "residual_tail_outcome_bucket; this objective is offline scoring only."
            ),
            "hard_leakage_rule": "No source_pair/source_residual/outcome bucket/realized pair-cost/future label may appear in predicate_feature_names.",
        },
        "minimum_real_training_gates": {
            "train_days": 3,
            "holdout_days": 2,
            "train_rows": 100,
            "holdout_rows": 50,
            "selected_train_rows": 50,
            "selected_holdout_rows": 20,
            "max_selected_share_from_single_day": 0.50,
            "source_sequence_coverage": 0.95,
            "composite_or_exact_join_coverage": 1.0,
        },
        "stability_gates": {
            "train_objective_positive": True,
            "holdout_objective_positive": True,
            "holdout_selected_residual_cost_share_not_worse_than_train_by_more_than": 0.50,
            "holdout_selected_pair_pnl_not_worse_than_train_by_more_than": 0.50,
            "at_least_two_holdout_days_with_selected_rows": True,
            "no_single_static_side_offset_public_price_cap": True,
        },
        "rule_freezing_output_schema": {
            "schema_version": "frozen_observable_pre_action_rule_manifest_v1",
            "required_fields": [
                "frozen_rule_id",
                "rule_version",
                "created_from_training_spec",
                "train_manifest",
                "holdout_manifest",
                "predicate_expression",
                "predicate_feature_names",
                "allowed_live_feature_families",
                "forbidden_live_feature_families",
                "train_label_fields",
                "holdout_label_fields",
                "train_holdout_split",
                "objective_metrics",
                "stability_gates",
                "same_window_scope",
                "uses_only_pre_action_fields",
                "uses_realized_pair_cost",
                "uses_future_labels",
                "private_truth_ready",
                "deployable",
                "promotion_gate",
            ],
            "required_boolean_values": {
                "uses_only_pre_action_fields": True,
                "uses_realized_pair_cost": False,
                "uses_future_labels": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate.passed": False,
            },
        },
        "denominator_replay_dependency": {
            "required_before_no_order_proposal": True,
            "scorer": "observable_pre_action_rule_miner_denominator_replay_scorer_v1",
            "same_window_denominator_count_min": 100,
            "rule_match_count_min": 20,
            "source_sequence_coverage_min": 0.95,
            "aggregate_parity_required": True,
        },
        "fail_closed_conditions": [
            "joined rows missing train or holdout split",
            "train/holdout days overlap",
            "offline labels missing",
            "offline labels allowed in live predicate",
            "realized pair cost used as live criterion",
            "future labels used as live criterion",
            "source_pair/source_residual/outcome labels in predicate_feature_names",
            "feature/label hash duplicate or label row reuse",
            "denominator replay spec missing or not KEEP",
            "strategy/private/deployable/promotion claim present",
        ],
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    blockers = validate_paths(
        [args.bridge_scorer_manifest, args.joined_rows, args.bridge_summary, args.denominator_spec_manifest],
        root,
    )
    bridge_manifest: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    denominator_spec: dict[str, Any] = {}
    if not blockers:
        bridge_manifest = read_json(args.bridge_scorer_manifest)
        rows = load_jsonl(args.joined_rows)
        summary = read_json(args.bridge_summary)
        denominator_spec = read_json(args.denominator_spec_manifest)
        blockers.extend(validate_bridge_scorer(bridge_manifest))
        blockers.extend(validate_summary(summary, rows, args))
        blockers.extend(validate_joined_rows(rows, summary))
        blockers.extend(validate_denominator_spec(denominator_spec))

    ready = not blockers
    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": "KEEP" if ready else "UNKNOWN",
        "decision_label": (
            "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_READY_REAL_INPUT_UNKNOWN"
            if ready
            else "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_FAIL_CLOSED"
        ),
        "lane": "observable_pre_action_rule_miner_training_spec",
        "inputs": {
            "bridge_scorer_manifest": str(args.bridge_scorer_manifest),
            "joined_rows": str(args.joined_rows),
            "bridge_summary": str(args.bridge_summary),
            "denominator_spec_manifest": str(args.denominator_spec_manifest),
        },
        "fixture_input_score": {
            "joined_row_count": len(rows),
            "split_counts": summary.get("split_counts") if summary else None,
            "join_method_counts": summary.get("join_method_counts") if summary else None,
            "train_days": summary.get("train_days") if summary else None,
            "holdout_days": summary.get("holdout_days") if summary else None,
        },
        "contract": training_contract(),
        "blockers": sorted(set(blockers)),
        "current_real_input_status": "UNKNOWN_NON_FIXTURE_JOINED_ROWS_MISSING",
        "next_executable_action": (
            "Implement observable_pre_action_rule_miner_training_fixture_scorer_v1 to exercise offline rule "
            "discovery on synthetic joined rows and prove leakage/stability fail-closed; do not start no-order diagnostics."
            if ready
            else "Fix joined-row, label-separation, denominator-spec, or provenance blockers before training."
        ),
        "research_ranking": {
            "status": (
                "TRAINING_SPEC_READY_FIXTURE_SCHEMA_ONLY_REAL_INPUT_UNKNOWN"
                if ready
                else "TRAINING_SPEC_FAIL_CLOSED"
            ),
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "This spec defines leakage-safe offline mining. It is not a trained rule, not a real-input scorer, "
                "and not authorization for no-order diagnostics."
            ),
        },
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "reason": "training_spec_only_real_input_unknown",
        },
        "scope": scope_flags(),
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
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge-scorer-manifest", type=Path, default=DEFAULT_BRIDGE_SCORER_MANIFEST)
    parser.add_argument("--joined-rows", type=Path, default=DEFAULT_JOINED_ROWS)
    parser.add_argument("--bridge-summary", type=Path, default=DEFAULT_BRIDGE_SUMMARY)
    parser.add_argument("--denominator-spec-manifest", type=Path, default=DEFAULT_DENOMINATOR_SPEC)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--min-fixture-train-days", type=int, default=3)
    parser.add_argument("--min-fixture-holdout-days", type=int, default=2)
    parser.add_argument("--min-fixture-train-rows", type=int, default=3)
    parser.add_argument("--min-fixture-holdout-rows", type=int, default=2)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
