#!/usr/bin/env python3
"""Specify the observable pre-action training-input bridge contract.

This local-only contract defines how a future allowlisted feature-join output
can be joined to offline source_pair/source_residual labels and a same-window
denominator summary before observable pre-action rule mining. It does not read
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


ARTIFACT = "xuan_observable_pre_action_rule_miner_training_input_bridge_contract"
CONTRACT_NAME = "observable_pre_action_rule_miner_training_input_bridge_contract_v1"

DEFAULT_FEATURE_JOIN_CONTRACT_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_feature_join_contract_20260525T192536Z/"
    "manifest.json"
)
DEFAULT_FEATURE_JOIN_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_feature_join_output_scorer_20260525T195536Z/"
    "manifest.json"
)
DEFAULT_DENOMINATOR_FIXTURE_SCORER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_20260525T202536Z/"
    "manifest.json"
)
DEFAULT_REAL_INPUT_INVENTORY_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_real_input_inventory_20260525T205536Z/"
    "manifest.json"
)
DEFAULT_CANDIDATE_CSV = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_candidate_seed_outcome_separator_full_20260522T185614Z/"
    "candidate_seed_outcome_separator.csv"
)
DEFAULT_RUNNER = Path("tools/xuan_dplus_passive_passive_shadow_runner.py")

EXPECTED_DECISIONS = {
    "feature_join_contract": "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_READY_EXPORTER_TARGET_NAMED",
    "feature_join_scorer": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY",
    "denominator_fixture_scorer": "KEEP_OBSERVABLE_PRE_ACTION_DENOMINATOR_REPLAY_FIXTURE_SCORER_READY",
    "real_input_inventory": "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_INVENTORY_GAPS",
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

FEATURE_ROW_JOIN_FIELDS = (
    "condition_id",
    "market_slug",
    "day_id",
    "quote_ts_ms",
    "side",
    "offset_s",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "candidate_qty",
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

OPTIONAL_EXACT_ID_FIELDS = (
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
    "separator_export_reason",
)

RUNNER_OUTPUT_TOKENS = (
    "--observable-pre-action-rule-miner-feature-join-output",
    "observable_pre_action_candidate_rows.jsonl",
    "observable_pre_action_source_link_summary.json",
    "observable_pre_action_feature_join_manifest.json",
    "source_sequence_id_policy",
    "quote_intent_id_policy",
    "source_order_id_policy",
    "post_action_outcome_labels_included",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_bool_text(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes"}


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


def safe_json(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return None, reason
    try:
        obj = read_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def safe_text(path: Path, root: Path) -> tuple[str, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return "", reason
    try:
        return path.read_text(encoding="utf-8", errors="replace"), None
    except FileNotFoundError:
        return "", "missing"
    except Exception as exc:
        return "", f"{type(exc).__name__}: {exc}"


def review_manifest(
    *,
    label: str,
    path: Path,
    expected_decision: str,
    root: Path,
) -> dict[str, Any]:
    manifest, error = safe_json(path, root)
    blockers = []
    if error or manifest is None:
        blockers.append(f"{label}_manifest_{error or 'missing'}")
        return {
            "path": str(path),
            "ready": False,
            "decision_label": None,
            "blockers": blockers,
        }
    decision = manifest.get("decision_label")
    if decision != expected_decision:
        blockers.append(f"{label}_decision_not_expected")
    if manifest.get("private_truth_ready") is True or manifest.get("deployable") is True:
        blockers.append(f"{label}_private_or_deployable_claim_present")
    promotion = as_dict(manifest.get("promotion_gate"))
    if promotion.get("passed") is True:
        blockers.append(f"{label}_promotion_claim_present")
    return {
        "path": str(path),
        "ready": not blockers,
        "decision_label": decision,
        "expected_decision_label": expected_decision,
        "blockers": blockers,
    }


def review_runner(path: Path, root: Path) -> dict[str, Any]:
    source, error = safe_text(path, root)
    blockers = []
    missing = []
    if error:
        blockers.append(f"runner_{error}")
    else:
        missing = [token for token in RUNNER_OUTPUT_TOKENS if token not in source]
        if missing:
            blockers.append("runner_feature_join_output_tokens_missing")
    return {
        "path": str(path),
        "ready": not blockers,
        "missing_tokens": missing,
        "blockers": blockers,
    }


def review_candidate_csv(path: Path, root: Path, min_days: int) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {
            "path": str(path),
            "ready_as_offline_label_source": False,
            "blockers": [f"candidate_csv_{reason}"],
        }
    if not path.exists():
        return {
            "path": str(path),
            "ready_as_offline_label_source": False,
            "blockers": ["candidate_csv_missing"],
        }

    row_count = 0
    days: set[str] = set()
    post_action_label_true = 0
    post_action_policy_values: dict[str, int] = {}
    live_rule_policy_values: dict[str, int] = {}
    external_shadow_ids_available_true = 0
    source_id_nonempty = {
        "quote_intent_id": 0,
        "source_order_id": 0,
        "source_sequence_id": 0,
    }
    sample: dict[str, Any] = {}
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        for row in reader:
            row_count += 1
            if row_count == 1:
                sample = {key: row.get(key) for key in fields[:40]}
            day = row.get("day")
            if day:
                days.add(str(day))
            if as_bool_text(row.get("outcome_labels_are_post_action")):
                post_action_label_true += 1
            post_policy = str(row.get("post_action_label_policy") or "")
            live_policy = str(row.get("live_rule_safety_policy") or "")
            if post_policy:
                post_action_policy_values[post_policy] = post_action_policy_values.get(post_policy, 0) + 1
            if live_policy:
                live_rule_policy_values[live_policy] = live_rule_policy_values.get(live_policy, 0) + 1
            if as_bool_text(row.get("external_shadow_ids_available")):
                external_shadow_ids_available_true += 1
            for field in source_id_nonempty:
                if row.get(field):
                    source_id_nonempty[field] += 1

    field_set = set(fields)
    missing_join = [field for field in CSV_JOIN_FIELDS if field not in field_set]
    missing_optional_ids = [field for field in OPTIONAL_EXACT_ID_FIELDS if field not in field_set]
    missing_labels = [field for field in OFFLINE_LABEL_FIELDS if field not in field_set]
    missing_policy = [field for field in CSV_POLICY_FIELDS if field not in field_set]
    blockers = []
    if row_count <= 0:
        blockers.append("candidate_csv_empty")
    if len(days) < min_days:
        blockers.append("candidate_csv_days_below_min")
    if missing_join:
        blockers.append("candidate_csv_join_fields_missing")
    if missing_labels:
        blockers.append("candidate_csv_offline_label_fields_missing")
    if missing_policy:
        blockers.append("candidate_csv_policy_fields_missing")
    if post_action_label_true <= 0:
        blockers.append("candidate_csv_post_action_label_policy_not_observed")

    return {
        "path": str(path),
        "ready_as_offline_label_source": not blockers,
        "row_count": row_count,
        "day_count": len(days),
        "days_sample": sorted(days)[:12],
        "field_count": len(fields),
        "required_join_fields": list(CSV_JOIN_FIELDS),
        "required_offline_label_fields": list(OFFLINE_LABEL_FIELDS),
        "missing_join_fields": missing_join,
        "missing_optional_exact_id_fields": missing_optional_ids,
        "missing_offline_label_fields": missing_labels,
        "missing_policy_fields": missing_policy,
        "post_action_label_true_rows": post_action_label_true,
        "post_action_policy_values": post_action_policy_values,
        "live_rule_policy_values": live_rule_policy_values,
        "external_shadow_ids_available_true": external_shadow_ids_available_true,
        "source_id_nonempty": source_id_nonempty,
        "sample": sample,
        "blockers": blockers,
        "interpretation": (
            "This CSV is an offline label source only. Empty quote/order/source ids are acceptable for the bridge "
            "contract because row-level source truth must come from future feature-join output, not this CSV."
        ),
    }


def bridge_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "purpose": (
            "Define the smallest safe bridge from future allowlisted no-order feature-join rows to offline "
            "train/holdout labels and later same-window denominator replay. The bridge is a research input "
            "contract only, not a live rule and not strategy evidence."
        ),
        "allowed_inputs": {
            "feature_join_rows": {
                "schema_version": "observable_pre_action_candidate_row_v1",
                "provenance": (
                    "Future allowlisted output/observable_pre_action_candidate_rows.jsonl produced by the "
                    "default-off runner flag --observable-pre-action-rule-miner-feature-join-output."
                ),
                "required_fields": list(FEATURE_ROW_JOIN_FIELDS)
                + [
                    "status_before_action",
                    "block_reason",
                    "source_sequence_present",
                    "quote_intent_present",
                    "source_order_present",
                    "post_action_outcome_labels_included",
                    "realized_pair_cost_used_as_live_criteria",
                    "trading_behavior_changed",
                ],
            },
            "offline_label_rows": {
                "schema": "candidate_seed_outcome_separator.csv",
                "provenance": (
                    "Current-worktree materialized separator export with post-action labels. It may provide "
                    "source_pair/source_residual labels only for offline train/holdout scoring."
                ),
                "required_join_fields": list(CSV_JOIN_FIELDS),
                "optional_exact_id_fields": list(OPTIONAL_EXACT_ID_FIELDS),
                "required_label_fields": list(OFFLINE_LABEL_FIELDS),
                "labels_allowed_in_live_predicate": False,
            },
            "same_window_denominator_summary": {
                "schema_version": "observable_pre_action_no_order_denominator_summary_v1",
                "provenance": (
                    "Produced only after a frozen predicate by the denominator replay scorer over the same "
                    "feature-join rows/window. It cannot be substituted by old marker denominators."
                ),
                "dependency": "observable_pre_action_rule_miner_denominator_replay_scorer_v1",
            },
        },
        "join_key_policy": {
            "preferred_exact_key": {
                "when_available_in_both_sources": ["source_seed_candidate_row_id"],
                "fallback": "source_seed_action_id",
                "require_unique_match": True,
            },
            "required_composite_fallback": {
                "exact_equalities": [
                    ["feature.condition_id", "label.condition_id"],
                    ["feature.market_slug", "label.slug"],
                    ["feature.day_id", "label.day"],
                    ["feature.side", "label.side"],
                    ["feature.source_risk_direction", "label.source_risk_direction"],
                ],
                "numeric_tolerances": {
                    "abs(feature.quote_ts_ms - label.ts_ms)": {"max_ms": 250, "alternate_label_field": "trigger_ts_ms"},
                    "abs(feature.offset_s - label.offset_s)": {"max_seconds": 0.25},
                    "abs(feature.pre_seed_same_qty - label.pre_seed_same_qty)": {"max_qty": 0.000001},
                    "abs(feature.pre_seed_opp_qty - label.pre_seed_opp_qty)": {"max_qty": 0.000001},
                    "abs(feature.pre_seed_same_cost - label.pre_seed_same_cost)": {"max_cost": 0.000001},
                    "abs(feature.pre_seed_opp_cost - label.pre_seed_opp_cost)": {"max_cost": 0.000001},
                    "abs(feature.pre_seed_open_qty - label.pre_seed_open_qty)": {"max_qty": 0.000001},
                    "abs(feature.pre_seed_open_cost - label.pre_seed_open_cost)": {"max_cost": 0.000001},
                    "abs(feature.candidate_qty - label.seed_qty)": {
                        "max_qty": 0.000001,
                        "alternate_label_field": "trigger_size",
                    },
                },
                "require_unique_match": True,
                "feature_row_max_matches": 1,
                "label_row_reuse_allowed": False,
            },
            "output_identity": [
                "bridge_row_id",
                "feature_row_hash",
                "offline_label_row_hash",
                "join_policy_version",
                "join_confidence",
            ],
            "fail_closed_on": [
                "zero_match",
                "multiple_matches",
                "same_label_row_matched_by_multiple_feature_rows",
                "condition_slug_day_side_conflict",
                "timestamp_outside_tolerance",
                "offset_outside_tolerance",
                "pre_seed_or_candidate_qty_cost_conflict",
                "offline_label_fields_missing",
                "feature_row_has_post_action_labels",
                "feature_row_claims_realized_pair_cost_live_criteria",
            ],
        },
        "train_holdout_label_policy": {
            "split_key": "day_id",
            "minimum_train_days": 3,
            "minimum_holdout_days": 2,
            "train_holdout_day_overlap_allowed": False,
            "offline_labels_allowed_for_train_scoring": list(OFFLINE_LABEL_FIELDS),
            "offline_labels_allowed_for_holdout_scoring": list(OFFLINE_LABEL_FIELDS),
            "offline_labels_allowed_in_frozen_live_predicate": False,
            "frozen_rule_predicate_must_use_only_feature_row_pre_action_fields": True,
        },
        "denominator_dependency": {
            "required_before_miner_input_manifest_is_ready": True,
            "same_window_rows": "the same feature-join rows used by the frozen predicate",
            "minimum_same_window_denominator_count": 100,
            "minimum_rule_match_count": 20,
            "minimum_source_sequence_coverage": 0.95,
            "requires_admitted_and_blocked_counts": True,
            "requires_aggregate_parity": True,
        },
        "future_scorer_target": {
            "name": "observable_pre_action_rule_miner_training_input_bridge_scorer_v1",
            "inputs": [
                "observable_pre_action_candidate_rows.jsonl",
                "observable_pre_action_source_link_summary.json",
                "observable_pre_action_feature_join_manifest.json",
                "candidate_seed_outcome_separator.csv or safe offline label JSONL",
                "training_input_bridge_contract_manifest.json",
            ],
            "outputs": [
                "observable_pre_action_training_bridge_joined_rows.jsonl",
                "observable_pre_action_training_bridge_summary.json",
            ],
            "must_fail_closed_for": [
                "missing preferred/composite join keys",
                "duplicate matches",
                "timestamp/offset/pre-seed/candidate mismatch",
                "label source outside current worktree",
                "labels used as live predicate features",
                "private/deployable/promotion claims",
            ],
        },
        "forbidden_criteria": [
            "events JSONL read or pull",
            "raw/replay/full-store scan",
            "SSH/shadow/canary/local agg/shared WS/live action",
            "public-profile single-day filters",
            "realized pair_cost live criteria",
            "static side/offset/public-price cap strategy",
            "D+ failed-family revival",
            "source_pair/source_residual/future labels in live predicate",
            "private truth/deployable/promotion claims",
        ],
    }


def scope_flags() -> dict[str, Any]:
    scope = {"current_worktree_only": True, "local_only": True}
    for field in REQUIRED_SCOPE_FALSE_FIELDS:
        scope[field] = False
    return scope


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    reviews = {
        "feature_join_contract": review_manifest(
            label="feature_join_contract",
            path=args.feature_join_contract_manifest,
            expected_decision=EXPECTED_DECISIONS["feature_join_contract"],
            root=root,
        ),
        "feature_join_scorer": review_manifest(
            label="feature_join_scorer",
            path=args.feature_join_scorer_manifest,
            expected_decision=EXPECTED_DECISIONS["feature_join_scorer"],
            root=root,
        ),
        "denominator_fixture_scorer": review_manifest(
            label="denominator_fixture_scorer",
            path=args.denominator_fixture_scorer_manifest,
            expected_decision=EXPECTED_DECISIONS["denominator_fixture_scorer"],
            root=root,
        ),
        "real_input_inventory": review_manifest(
            label="real_input_inventory",
            path=args.real_input_inventory_manifest,
            expected_decision=EXPECTED_DECISIONS["real_input_inventory"],
            root=root,
        ),
        "runner": review_runner(args.runner, root),
        "offline_label_csv": review_candidate_csv(args.candidate_csv, root, args.min_label_days),
    }

    blockers: list[str] = []
    for review_name, review in reviews.items():
        for blocker in review.get("blockers", []) or []:
            blockers.append(f"{review_name}:{blocker}")

    ready = not blockers
    if ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_READY"
        next_action = (
            "Implement observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_v1 using synthetic "
            "feature-join rows and offline-label fixtures to prove one-to-one join, tolerance, label separation, "
            "and fail-closed behavior; do not start no-order diagnostics."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_INPUT_BRIDGE_CONTRACT_BLOCKED"
        next_action = (
            "Resolve bridge contract source/tooling blockers before implementing the fixture scorer. Do not start "
            "no-order diagnostics."
        )

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_training_input_bridge_contract",
        "source_inputs": {
            "feature_join_contract_manifest": str(args.feature_join_contract_manifest),
            "feature_join_scorer_manifest": str(args.feature_join_scorer_manifest),
            "denominator_fixture_scorer_manifest": str(args.denominator_fixture_scorer_manifest),
            "real_input_inventory_manifest": str(args.real_input_inventory_manifest),
            "candidate_csv": str(args.candidate_csv),
            "runner": str(args.runner),
        },
        "source_review": reviews,
        "contract": bridge_contract(),
        "current_real_join_ready": False,
        "current_real_join_ready_reason": (
            "Current worktree still lacks a non-fixture feature-join output and exact same-window denominator. "
            "This KEEP, if present, is contract/scorer-target readiness only."
        ),
        "blockers": sorted(set(blockers)),
        "next_executable_action": next_action,
        "research_ranking": {
            "status": decision_label,
            "contract_ready": ready,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "The bridge can be specified safely from current artifacts, but it cannot produce real strategy "
                "evidence until a future allowlisted feature-join output and denominator replay exist."
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
            "reason": "training_input_bridge_contract_only",
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
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-join-contract-manifest", type=Path, default=DEFAULT_FEATURE_JOIN_CONTRACT_MANIFEST)
    parser.add_argument("--feature-join-scorer-manifest", type=Path, default=DEFAULT_FEATURE_JOIN_SCORER_MANIFEST)
    parser.add_argument("--denominator-fixture-scorer-manifest", type=Path, default=DEFAULT_DENOMINATOR_FIXTURE_SCORER_MANIFEST)
    parser.add_argument("--real-input-inventory-manifest", type=Path, default=DEFAULT_REAL_INPUT_INVENTORY_MANIFEST)
    parser.add_argument("--candidate-csv", type=Path, default=DEFAULT_CANDIDATE_CSV)
    parser.add_argument("--runner", type=Path, default=DEFAULT_RUNNER)
    parser.add_argument("--min-label-days", type=int, default=5)
    parser.add_argument("--created-utc", default=utc_label())
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
