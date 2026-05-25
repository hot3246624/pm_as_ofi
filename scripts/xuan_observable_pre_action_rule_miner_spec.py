#!/usr/bin/env python3
"""Define the observable pre-action rule miner contract.

This local-only spec is the next lane after public-profile and symmetric
activation were frozen/discarded. It defines the exact input manifest and
fail-closed gates for a future offline rule miner. It does not mine rules by
itself, fetch data, start SSH/shadows/services, read events JSONL, scan raw or
replay stores, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_spec"
CONTRACT_NAME = "observable_pre_action_rule_miner_spec_v1"
INPUT_SCHEMA = "observable_pre_action_rule_miner_input_v1"
DEFAULT_PIVOT_SELECTOR = Path(
    "xuan_research_artifacts/"
    "xuan_local_strategy_pivot_selector_20260525T175006Z/"
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

FORBIDDEN_LIVE_FIELDS = (
    "realized_pair_cost",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
    "settlement_outcome",
    "future_redeem",
    "public_profile_single_day_profit",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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
        return int(value)
    except (TypeError, ValueError):
        return default


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
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


def source_fields(manifest: dict[str, Any], source_name: str) -> list[str]:
    sources = manifest.get("materialized_sources")
    if not isinstance(sources, dict):
        return []
    source = sources.get(source_name)
    if not isinstance(source, dict):
        return []
    return [str(field) for field in as_list(source.get("fields"))]


def source_path(manifest: dict[str, Any], source_name: str) -> Path | None:
    sources = manifest.get("materialized_sources")
    if not isinstance(sources, dict):
        return None
    source = sources.get(source_name)
    if not isinstance(source, dict):
        return None
    path = source.get("path")
    return Path(str(path)) if path else None


def source_row_count(manifest: dict[str, Any], source_name: str) -> int:
    sources = manifest.get("materialized_sources")
    if not isinstance(sources, dict):
        return 0
    source = sources.get(source_name)
    if not isinstance(source, dict):
        return 0
    return as_int(source.get("row_count"))


def validate_candidate_input(path: Path | None, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    if path is None:
        return {
            "status": "MISSING_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT",
            "ready": False,
            "path": None,
            "fixture": False,
            "blockers": ["candidate_input_manifest_absent"],
            "missing_source_fields": list(REQUIRED_PRE_ACTION_FIELDS),
            "missing_no_order_fields": list(REQUIRED_NO_ORDER_FIELDS),
        }

    manifest, error = safe_load(path, root)
    if error or manifest is None:
        return {
            "status": "OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_FAIL_CLOSED",
            "ready": False,
            "path": str(path),
            "fixture": False,
            "blockers": [f"input_manifest_{error}"],
            "missing_source_fields": list(REQUIRED_PRE_ACTION_FIELDS),
            "missing_no_order_fields": list(REQUIRED_NO_ORDER_FIELDS),
        }

    blockers: list[str] = []
    if manifest.get("schema_version") != INPUT_SCHEMA:
        blockers.append("schema_version_mismatch")

    fixture = bool(manifest.get("fixture"))
    scope = manifest.get("scope") if isinstance(manifest.get("scope"), dict) else {}
    if scope.get("current_worktree_only") is not True:
        blockers.append("scope_current_worktree_only_not_true")
    if scope.get("local_only") is not True:
        blockers.append("scope_local_only_not_true")
    for field in REQUIRED_SCOPE_FALSE_FIELDS:
        if scope.get(field) is not False:
            blockers.append(f"scope_{field}_not_false")

    for source_name in ("candidate_rows", "source_link_summary", "no_order_denominator_summary"):
        p = source_path(manifest, source_name)
        if p is None:
            blockers.append(f"{source_name}_path_missing")
            continue
        ok, reason = path_safe(p, root)
        if not ok:
            blockers.append(f"{source_name}_{reason}")
        elif not p.exists():
            blockers.append(f"{source_name}_path_missing_on_disk")

    candidate_fields = source_fields(manifest, "candidate_rows")
    no_order_fields = source_fields(manifest, "no_order_denominator_summary")
    missing_pre_action = [field for field in REQUIRED_PRE_ACTION_FIELDS if field not in candidate_fields]
    missing_no_order = [field for field in REQUIRED_NO_ORDER_FIELDS if field not in no_order_fields]
    if missing_pre_action:
        blockers.append("candidate_rows_required_pre_action_fields_missing")
    if missing_no_order:
        blockers.append("no_order_denominator_required_fields_missing")
    if source_row_count(manifest, "candidate_rows") < args.min_candidate_rows:
        blockers.append("candidate_rows_below_min")

    split = manifest.get("train_holdout_split") if isinstance(manifest.get("train_holdout_split"), dict) else {}
    train_days = [str(day) for day in as_list(split.get("train_days"))]
    holdout_days = [str(day) for day in as_list(split.get("holdout_days"))]
    if len(set(train_days)) < args.min_train_days:
        blockers.append("train_days_below_min")
    if len(set(holdout_days)) < args.min_holdout_days:
        blockers.append("holdout_days_below_min")
    if set(train_days).intersection(holdout_days):
        blockers.append("train_holdout_days_overlap")

    feature_policy = manifest.get("feature_policy") if isinstance(manifest.get("feature_policy"), dict) else {}
    allowed_live_fields = [str(field) for field in as_list(feature_policy.get("allowed_live_rule_fields"))]
    offline_label_fields = [str(field) for field in as_list(feature_policy.get("offline_label_fields"))]
    if not set(REQUIRED_PRE_ACTION_FIELDS).issubset(set(allowed_live_fields)):
        blockers.append("allowed_live_rule_fields_do_not_cover_required_pre_action_fields")
    if not set(OFFLINE_LABEL_FIELDS).issubset(set(offline_label_fields)):
        blockers.append("offline_label_fields_do_not_cover_required_labels")
    forbidden_live = sorted(set(allowed_live_fields).intersection(FORBIDDEN_LIVE_FIELDS))
    if forbidden_live:
        blockers.append("forbidden_fields_in_allowed_live_rule_fields")

    frozen = manifest.get("frozen_rule_contract") if isinstance(manifest.get("frozen_rule_contract"), dict) else {}
    predicate_fields = [str(field) for field in as_list(frozen.get("predicate_fields"))]
    if not predicate_fields:
        blockers.append("frozen_rule_predicate_fields_missing")
    if not set(predicate_fields).issubset(set(allowed_live_fields)):
        blockers.append("frozen_rule_uses_fields_outside_allowed_pre_action")
    if set(predicate_fields).intersection(FORBIDDEN_LIVE_FIELDS):
        blockers.append("frozen_rule_uses_forbidden_live_field")
    if frozen.get("uses_only_pre_action_fields") is not True:
        blockers.append("frozen_rule_not_declared_pre_action_only")
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

    denominator = (
        manifest.get("no_order_denominator_gate")
        if isinstance(manifest.get("no_order_denominator_gate"), dict)
        else {}
    )
    if denominator.get("same_window_denominator_reproduced") is not True:
        blockers.append("same_window_no_order_denominator_not_reproduced")
    if as_int(denominator.get("same_window_denominator_count")) < args.min_no_order_denominator:
        blockers.append("same_window_no_order_denominator_below_min")
    if as_float(denominator.get("source_sequence_coverage")) < args.min_source_sequence_coverage:
        blockers.append("source_sequence_coverage_below_min")
    if denominator.get("aggregate_parity") is not True:
        blockers.append("no_order_denominator_aggregate_parity_not_true")

    if manifest.get("strategy_evidence") is True:
        blockers.append("candidate_manifest_claims_strategy_evidence")
    promotion_gate = manifest.get("promotion_gate") if isinstance(manifest.get("promotion_gate"), dict) else {}
    if promotion_gate.get("passed") is True:
        blockers.append("candidate_manifest_claims_promotion_gate_passed")

    ready = not blockers
    status = "OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_READY" if ready else "OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_FAIL_CLOSED"
    return {
        "status": status,
        "ready": ready,
        "path": str(path),
        "fixture": fixture,
        "blockers": blockers,
        "missing_source_fields": missing_pre_action,
        "missing_no_order_fields": missing_no_order,
        "train_day_count": len(set(train_days)),
        "holdout_day_count": len(set(holdout_days)),
        "candidate_row_count": source_row_count(manifest, "candidate_rows"),
        "same_window_denominator_count": as_int(denominator.get("same_window_denominator_count")),
        "source_sequence_coverage": as_float(denominator.get("source_sequence_coverage")),
        "forbidden_live_fields_present": forbidden_live,
        "input_manifest_summary": {
            "artifact": manifest.get("artifact"),
            "decision_label": manifest.get("decision_label"),
            "created_utc": manifest.get("created_utc"),
        },
    }


def miner_contract(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "input_schema": INPUT_SCHEMA,
        "purpose": (
            "Discover new local research predicates only from observable pre-action fields after old public-profile, "
            "symmetric activation, closed-cycle, complete-set, and micro-deficit/source-opportunity lines failed or froze."
        ),
        "required_materialized_sources": {
            "candidate_rows": {
                "purpose": "safe current-worktree row export for offline train/holdout selection",
                "min_rows": args.min_candidate_rows,
                "required_pre_action_fields": list(REQUIRED_PRE_ACTION_FIELDS),
                "offline_label_fields_allowed_for_training_only": list(OFFLINE_LABEL_FIELDS),
            },
            "source_link_summary": {
                "purpose": "same-window source/quote/order presence and reason buckets",
                "required_properties": [
                    "reason/source coverage",
                    "source_sequence presence by status",
                    "quote_intent/source_order presence by status",
                    "admitted and blocked denominators",
                ],
            },
            "no_order_denominator_summary": {
                "purpose": "exact frozen-rule denominator reproduction before any economic diagnostic",
                "required_fields": list(REQUIRED_NO_ORDER_FIELDS),
                "min_same_window_denominator": args.min_no_order_denominator,
                "min_source_sequence_coverage": args.min_source_sequence_coverage,
            },
        },
        "three_gate_policy": {
            "gate_1_train_discovery": [
                "post-action source_pair/source_residual labels may be used only to score candidate predicates on train days",
                "candidate predicates must be frozen before holdout evaluation",
                "frozen predicates must use only allowed_live_rule_fields",
            ],
            "gate_2_holdout_stability": [
                f"train days >= {args.min_train_days}",
                f"holdout days >= {args.min_holdout_days}",
                f"train selected rows >= {args.min_train_selected_rows}",
                f"holdout selected rows >= {args.min_holdout_selected_rows}",
                "train and holdout days must not overlap",
            ],
            "gate_3_no_order_denominator": [
                "exact frozen predicate denominator must reproduce in allowlisted no-order summaries",
                f"same-window denominator count >= {args.min_no_order_denominator}",
                f"source_sequence coverage >= {args.min_source_sequence_coverage}",
                "aggregate parity must pass",
                "economic no-order diagnostic is not allowed until this gate passes",
            ],
        },
        "allowed_pre_action_feature_families": [
            "source_sequence/quote_intent/source_order presence before action",
            "admit/block reason and status buckets",
            "pre_seed same/opposite qty and cost buckets",
            "candidate qty bucket and time-to-expiry/offset bucket",
            "source risk direction and open/deficit bucket",
            "activation status only when observed before action",
        ],
        "forbidden_shortcuts": [
            "public-profile single-day filters",
            "realized pair_cost as live criteria",
            "future source_pair/source_residual labels as live fields",
            "pure Polymarket public-price cap",
            "static side/offset/timeframe deletion",
            "D+ micro-deficit/fill-to-balance/portfolio-ledger/closed-cycle/symmetric-activation failed-family revival",
            "SSH/shadow/canary/local agg/shared WS/live from this spec",
            "raw/replay/full-store scans",
            "private/deployable/promotion claims",
        ],
        "future_economic_diagnostic_gate": {
            "requires_prior_no_order_denominator_gate": True,
            "requires_explicit_user_approval_for_exact_bounded_run": True,
            "min_candidates": 100,
            "max_net_pair_cost_p90": 1.0,
            "max_residual_qty_share": 0.15,
            "max_residual_cost_share": 0.20,
            "max_pair_tail_loss_share": 0.05,
            "max_material_residual_lots": 0,
            "require_positive_fee_adjusted_pair_pnl_proxy": True,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    pivot, pivot_error = safe_load(args.pivot_selector_manifest, root)
    input_status = validate_candidate_input(args.candidate_input_manifest, root, args)
    pivot_ready = (
        pivot_error is None
        and isinstance(pivot, dict)
        and pivot.get("decision_label") == "KEEP_LOCAL_STRATEGY_PIVOT_OBSERVABLE_PRE_ACTION_RULE_MINER_SELECTED"
    )
    blockers: list[str] = []
    if not pivot_ready:
        blockers.append("pivot_selector_not_ready")

    candidate_supplied = args.candidate_input_manifest is not None
    candidate_ready = bool(input_status["ready"])
    fixture_ready = candidate_ready and bool(input_status["fixture"])
    real_ready = candidate_ready and not bool(input_status["fixture"])

    if not candidate_supplied:
        decision = "KEEP" if pivot_ready else "UNKNOWN"
        decision_label = (
            "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_REAL_INPUT_UNKNOWN"
            if pivot_ready
            else "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_PIVOT_NOT_READY"
        )
        real_input_status = "UNKNOWN_REAL_INPUT_MANIFEST_MISSING"
        next_action = (
            "Implement observable_pre_action_rule_miner_input_inventory_v1 local-only: locate or define a safe "
            "current-worktree materialized input manifest for this contract; do not start no-order diagnostics."
        )
    elif fixture_ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_FIXTURE_PASS_NOT_REAL"
        real_input_status = "FIXTURE_PASS_NOT_REAL_STRATEGY_EVIDENCE"
        next_action = (
            "Use the fixture result only to validate the contract; next local action is to inventory real safe "
            "materialized inputs, not to start no-order diagnostics."
        )
    elif real_ready:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_REAL_INPUT_CONTRACT_READY_NOT_STRATEGY_EVIDENCE"
        real_input_status = "REAL_INPUT_CONTRACT_READY_NOT_STRATEGY_EVIDENCE"
        next_action = (
            "Implement the local rule miner/scorer over the ready input manifest, keeping train/holdout and "
            "no-order-denominator gates separate from promotion."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_FAIL_CLOSED"
        real_input_status = "INPUT_FAIL_CLOSED"
        blockers.extend(input_status["blockers"])
        next_action = "Fix missing source/no-order-denominator input fields before any miner/scorer work."

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_spec",
        "contract": miner_contract(args),
        "source_inputs": {
            "pivot_selector_manifest": str(args.pivot_selector_manifest),
            "pivot_selector_error": pivot_error,
            "candidate_input_manifest": str(args.candidate_input_manifest) if args.candidate_input_manifest else None,
        },
        "candidate_input_status": input_status,
        "real_input_status": real_input_status,
        "blockers": blockers,
        "next_executable_action": next_action,
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "contract_ready": decision == "KEEP",
            "candidate_input_ready": candidate_ready,
            "fixture_only": bool(input_status.get("fixture")),
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "This is a local spec/input contract. It can enable future local rule mining only after safe "
                "materialized inputs exist; it is not private truth, deployable evidence, or promotion evidence."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "SPEC_AND_INPUT_CONTRACT_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "scope": {
            "current_worktree_only": True,
            "local_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "orders_cancels_redeems_sent": False,
            "trading_behavior_changed": False,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pivot-selector-manifest", type=Path, default=DEFAULT_PIVOT_SELECTOR)
    parser.add_argument("--candidate-input-manifest", type=Path, default=None)
    parser.add_argument("--min-candidate-rows", type=int, default=1000)
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
    parser.add_argument("--min-train-selected-rows", type=int, default=100)
    parser.add_argument("--min-holdout-selected-rows", type=int, default=50)
    parser.add_argument("--min-no-order-denominator", type=int, default=30)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
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
