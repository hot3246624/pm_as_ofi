#!/usr/bin/env python3
"""Build a local-only same-window handoff proposal for the HYPE backtest lead.

The proposal consumes the already committed backtest V1 candidate-filter plan
and the derived validation manifests referenced by that plan. It does not start
diagnostics, read raw/replay stores, inspect events JSONL, or claim private
truth. Its output is an exact future handoff request plus fail-closed gates.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_backtest_v1_hype_lead_same_window_handoff_proposal"
CONTRACT_NAME = "xuan_backtest_v1_hype_lead_same_window_handoff_proposal_v1"

READY_DECISION = "KEEP_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_READY_APPROVAL_REQUIRED"
UNKNOWN_DECISION = "UNKNOWN_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_GAPS"
DISCARD_DECISION = "DISCARD_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_FORBIDDEN_INPUT"

DEFAULT_PLAN_MANIFEST = Path(
    "xuan_research_artifacts/xuan_backtest_v1_candidate_filter_handoff_plan_20260527T010000Z/manifest.json"
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

REQUIRED_PLAN_DECISION = "KEEP_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_READY_PRIVATE_BLOCKED"
REQUIRED_PARAM_KEYS = (
    "asset",
    "price_lo",
    "price_hi",
    "size_lo",
    "size_hi",
    "offset_lo",
    "offset_hi",
    "max_l1_pair_ask",
    "max_l1_immediate_pair",
    "side_alignment",
    "pnl_cost_source",
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


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def path_safe(path: Path) -> tuple[bool, str | None]:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    if any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS):
        return False, "forbidden_path_fragment"
    if path.suffix == ".jsonl":
        return False, "jsonl_input_not_allowed_for_handoff_proposal"
    return True, None


def normalize_params(value: Any) -> dict[str, str]:
    params = as_dict(value)
    return {key: str(params.get(key, "")) for key in REQUIRED_PARAM_KEYS}


def missing_param_keys(params: dict[str, str]) -> list[str]:
    return [key for key in REQUIRED_PARAM_KEYS if params.get(key, "") == ""]


def validate_plan_manifest(
    plan: dict[str, Any], expected_asset: str, expected_candidate_key: str
) -> tuple[list[str], list[str], dict[str, Any]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if plan.get("contract_name") != "xuan_backtest_v1_candidate_filter_handoff_plan_v1":
        blockers.append("plan_contract_name_mismatch")
    if plan.get("decision_label") != REQUIRED_PLAN_DECISION:
        blockers.append("plan_decision_not_keep_private_blocked")
    if plan.get("decision") != "KEEP":
        blockers.append("plan_decision_not_keep")
    if as_bool(plan.get("private_truth_ready")):
        forbidden.append("plan_claims_private_truth_ready")
    if as_bool(plan.get("deployable")):
        forbidden.append("plan_claims_deployable")
    if as_bool(plan.get("strategy_evidence")):
        forbidden.append("plan_claims_strategy_evidence")
    if as_bool(plan.get("no_order_diagnostic_allowed")):
        forbidden.append("plan_allows_no_order_without_approval")
    if as_dict(plan.get("promotion_gate")).get("passed") is True:
        forbidden.append("plan_promotion_gate_passed")
    if as_list(plan.get("forbidden_blockers")):
        forbidden.append("plan_forbidden_blockers_present")
    lead = as_dict(plan.get("lead_candidate"))
    if not lead:
        blockers.append("lead_candidate_missing")
    if str(lead.get("asset") or "") != expected_asset:
        blockers.append("lead_asset_mismatch")
    if str(lead.get("candidate_key") or "") != expected_candidate_key:
        blockers.append("lead_candidate_key_mismatch")
    if str(lead.get("audit_status") or "") != "SEARCH_SAFE_READY_PRIVATE_BLOCKED":
        blockers.append("lead_audit_status_not_search_safe_private_blocked")
    if as_bool(lead.get("deployable_ready")):
        forbidden.append("lead_claims_deployable_ready")
    if as_bool(lead.get("private_promotion_gate_pass")):
        forbidden.append("lead_private_promotion_gate_pass")
    if as_int(lead.get("private_truth_ready_count")) != 0:
        forbidden.append("lead_private_truth_ready_count_nonzero")
    research = as_dict(plan.get("research_ranking"))
    if research.get("strategy_evidence") is True:
        forbidden.append("research_ranking_claims_strategy_evidence")
    if research.get("real_miner_input_ready") is True:
        forbidden.append("research_ranking_claims_real_miner_input_ready")
    safety = as_dict(plan.get("safety"))
    for key, value in safety.items():
        if key in {"new_no_order_diagnostic_started", "events_jsonl_read", "raw_replay_or_full_store_scan"} and value is True:
            forbidden.append(f"plan_safety_{key}_true")
    return blockers, forbidden, lead


def load_validation_manifest(path: Path) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    ok, reason = path_safe(path)
    if not ok:
        forbidden.append(f"validation_manifest_{reason}:{path}")
        return {}, blockers, forbidden
    if not path.exists():
        blockers.append(f"validation_manifest_missing:{path}")
        return {}, blockers, forbidden
    data = read_json(path)
    if not isinstance(data, dict):
        blockers.append(f"validation_manifest_not_json_object:{path}")
        return {}, blockers, forbidden
    return data, blockers, forbidden


def candidate_params_from_evidence(
    plan: dict[str, Any], expected_asset: str, expected_candidate_key: str
) -> tuple[dict[str, str], list[dict[str, Any]], list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    evidence_out: list[dict[str, Any]] = []
    param_versions: list[dict[str, str]] = []
    evidence_rows = [item for item in as_list(plan.get("lead_candidate_evidence")) if isinstance(item, dict)]
    if not evidence_rows:
        blockers.append("lead_candidate_evidence_missing")
    for item in evidence_rows:
        manifest_path = Path(str(item.get("validation_manifest") or ""))
        manifest, load_blockers, load_forbidden = load_validation_manifest(manifest_path)
        blockers.extend(load_blockers)
        forbidden.extend(load_forbidden)
        scope = as_dict(manifest.get("validation_scope"))
        promotion = as_dict(manifest.get("promotion"))
        params = normalize_params(manifest.get("candidate_params"))
        row_summary = {
            "asset": manifest.get("asset"),
            "candidate_key": manifest.get("candidate_key"),
            "candidate_params": params,
            "experiment_label": item.get("experiment_label"),
            "manifest_ok": manifest.get("ok") is True,
            "private_truth_ready": promotion.get("private_truth_ready") is True,
            "requires_raw": scope.get("requires_raw") is True or item.get("requires_raw") is True,
            "requires_remote": scope.get("requires_remote") is True or item.get("requires_remote") is True,
            "search_safe_validation_ready": promotion.get("search_safe_validation_ready") is True,
            "validation_manifest": str(manifest_path),
        }
        evidence_out.append(row_summary)
        if manifest:
            if manifest.get("ok") is not True:
                blockers.append(f"validation_manifest_not_ok:{manifest_path}")
            if str(manifest.get("asset") or "") != expected_asset:
                blockers.append(f"validation_manifest_asset_mismatch:{manifest_path}")
            if str(manifest.get("candidate_key") or "") != expected_candidate_key:
                blockers.append(f"validation_manifest_candidate_key_mismatch:{manifest_path}")
            if promotion.get("private_truth_ready") is True:
                forbidden.append(f"validation_manifest_private_truth_ready:{manifest_path}")
            if promotion.get("deployable_ready") is True:
                forbidden.append(f"validation_manifest_deployable_ready:{manifest_path}")
            if scope.get("requires_raw") is True or item.get("requires_raw") is True:
                forbidden.append(f"validation_manifest_requires_raw:{manifest_path}")
            if scope.get("requires_remote") is True or item.get("requires_remote") is True:
                forbidden.append(f"validation_manifest_requires_remote:{manifest_path}")
            missing = missing_param_keys(params)
            if missing:
                blockers.append(f"validation_manifest_candidate_params_missing:{manifest_path}:{','.join(missing)}")
            else:
                param_versions.append(params)
    unique_param_blobs = {json.dumps(params, sort_keys=True) for params in param_versions}
    if len(unique_param_blobs) > 1:
        blockers.append("candidate_params_inconsistent_across_validation_manifests")
    if not param_versions:
        return {}, evidence_out, blockers, forbidden
    return param_versions[0], evidence_out, blockers, forbidden


def candidate_filter_spec(params: dict[str, str]) -> dict[str, Any]:
    return {
        "asset": params.get("asset"),
        "max_l1_immediate_pair": params.get("max_l1_immediate_pair"),
        "max_l1_pair_ask": params.get("max_l1_pair_ask"),
        "offset_s": {"high": params.get("offset_hi"), "low": params.get("offset_lo")},
        "pnl_cost_source": params.get("pnl_cost_source"),
        "price": {"high": params.get("price_hi"), "low": params.get("price_lo")},
        "side_alignment": params.get("side_alignment"),
        "size": {"high": params.get("size_hi"), "low": params.get("size_lo")},
    }


def handoff_request(
    *,
    asset: str,
    candidate_key: str,
    candidate_params: dict[str, str],
    created_utc: str,
    min_train_days: int,
    min_holdout_days: int,
    min_train_rows: int,
    min_holdout_rows: int,
    timeframe: str,
) -> dict[str, Any]:
    prefix = f"{asset.lower()}-updown-{timeframe}"
    distinct_days = min_train_days + min_holdout_days
    return {
        "approval_required_in_thread": True,
        "asset": asset,
        "bounded_future_request_name": (
            f"xuan_research_backtest_v1_{asset.lower()}_{candidate_key[:12]}_same_window_label_handoff_APPROVAL_TS"
        ),
        "candidate_filter": candidate_filter_spec(candidate_params),
        "candidate_key": candidate_key,
        "diagnostic_allowed_now": False,
        "expected_output_files": [
            "observable_pre_action_candidate_rows.jsonl",
            "observable_pre_action_source_link_summary.json",
            "observable_pre_action_feature_join_manifest.json",
            "observable_pre_action_same_window_offline_labels.csv",
            "same_window_aggregate_summary.json",
            "observable_pre_action_same_window_offline_label_handoff.json",
            "observable_pre_action_training_bridge_joined_rows.jsonl",
            "observable_pre_action_training_bridge_summary.json",
        ],
        "fail_closed_blockers": [
            "missing_or_unresolved_hype_updown_market_prefix",
            "less_than_3_train_days_or_less_than_2_holdout_days",
            "row_level_source_handoff_id_missing",
            "row_level_source_seed_candidate_row_id_missing",
            "duplicate_source_handoff_id_or_candidate_exact_id",
            "join_coverage_below_100_percent_for_exact_id_materialization",
            "source_sequence_coverage_below_95_percent",
            "labels_used_as_live_predicates",
            "realized_pair_cost_or_future_label_used_as_live_criterion",
            "raw_replay_full_store_or_events_jsonl_dependency",
            "ssh_shadow_canary_live_local_agg_shared_ws_dependency",
            "private_truth_deployable_or_promotion_claim_present",
        ],
        "future_run_template_status": "proposal_only_not_executable_without_new_explicit_approval",
        "market_prefix": prefix,
        "market_prefix_preflight": (
            "Before any approved run, resolve market ids for this prefix and requested offsets without repo sync, "
            "service controls, live/canary/order actions, raw/replay scans, or events JSONL reads."
        ),
        "minimum_multiday_window": {
            "distinct_days": distinct_days,
            "holdout_days": min_holdout_days,
            "holdout_rows": min_holdout_rows,
            "source_sequence_coverage": 0.95,
            "train_days": min_train_days,
            "train_rows": min_train_rows,
        },
        "no_order_safety_boundary": {
            "canary_or_live": False,
            "events_jsonl_pull_or_read": False,
            "orders_cancels_redeems": False,
            "raw_replay_or_full_store_scan": False,
            "repo_sync_or_build_for_canary": False,
            "shared_ingress_or_broker_writes": False,
            "systemd_or_service_control": False,
        },
        "proposal_created_utc": created_utc,
        "required_row_fields": [
            "source_handoff_id",
            "source_seed_candidate_row_id",
            "source_seed_action_id",
            "day_id",
            "condition_id",
            "market_slug",
            "status_before_action",
            "side",
            "offset_s",
            "source_sequence_present",
            "source_pair_qty",
            "source_pair_cost",
            "source_pair_pnl",
            "source_residual_qty",
            "source_residual_cost",
        ],
        "timeframe": timeframe,
        "train_holdout_label_policy": {
            "future_labels_allowed_as_live_criteria": False,
            "labels_allowed_for_offline_train_holdout_scoring": True,
            "labels_allowed_in_live_predicate": False,
            "realized_pair_cost_allowed_as_live_criteria": False,
            "source_pair_source_residual_labels_train_holdout_only": True,
        },
        "validation_command_order": [
            "python3 scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py --handoff-manifest ${same_window_handoff_manifest} --output-dir ${contract_output_dir}",
            "python3 scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py --handoff-manifest ${same_window_handoff_manifest} --output-dir ${materializer_output_dir}",
            "python3 scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory.py --only-explicit --bridge-summary ${bridge_summary_1} ... --output-dir ${arrival_inventory_output_dir}",
            "python3 scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py --bridge-summary ${bridge_summary_1} ... --output-dir ${accumulator_output_dir}",
            "python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py --joined-rows ${accumulated_joined_rows} --output-dir ${training_output_dir}",
            "denominator replay scorer only after a frozen rule manifest exists; do not use labels as live predicates",
        ],
    }


def classify(blockers: list[str], forbidden: list[str]) -> tuple[str, str, str]:
    if forbidden:
        return (
            "DISCARD",
            DISCARD_DECISION,
            "Discard this HYPE handoff proposal input; it crosses forbidden raw/remote/private/live/promotion boundaries.",
        )
    if blockers:
        return (
            "UNKNOWN",
            UNKNOWN_DECISION,
            "Fix the HYPE lead proposal gaps with local source/spec review; do not start a diagnostic.",
        )
    return (
        "KEEP",
        READY_DECISION,
        "Wait for explicit approval of this exact bounded future HYPE same-window handoff request; do not start it automatically.",
    )


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    blockers: list[str] = []
    forbidden: list[str] = []
    ok, reason = path_safe(args.plan_manifest)
    if not ok:
        forbidden.append(f"plan_manifest_{reason}")
    if not args.plan_manifest.exists():
        blockers.append("plan_manifest_missing")
        plan: dict[str, Any] = {}
    else:
        raw_plan = read_json(args.plan_manifest)
        if isinstance(raw_plan, dict):
            plan = raw_plan
        else:
            plan = {}
            blockers.append("plan_manifest_not_json_object")

    plan_blockers, plan_forbidden, lead = validate_plan_manifest(plan, args.expected_asset, args.expected_candidate_key)
    blockers.extend(plan_blockers)
    forbidden.extend(plan_forbidden)
    candidate_params, evidence, param_blockers, param_forbidden = candidate_params_from_evidence(
        plan, args.expected_asset, args.expected_candidate_key
    )
    blockers.extend(param_blockers)
    forbidden.extend(param_forbidden)
    if candidate_params and candidate_params.get("asset") != args.expected_asset:
        blockers.append("candidate_params_asset_mismatch")

    proposal = (
        handoff_request(
            asset=args.expected_asset,
            candidate_key=args.expected_candidate_key,
            candidate_params=candidate_params,
            created_utc=args.created_utc,
            min_train_days=args.min_train_days,
            min_holdout_days=args.min_holdout_days,
            min_train_rows=args.min_train_rows,
            min_holdout_rows=args.min_holdout_rows,
            timeframe=args.timeframe,
        )
        if candidate_params
        else None
    )
    decision, decision_label, next_action = classify(blockers, forbidden)
    return {
        "artifact": ARTIFACT,
        "blockers": sorted(set(blockers)),
        "candidate_params": candidate_params,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "forbidden_blockers": sorted(set(forbidden)),
        "inputs": {"candidate_filter_plan_manifest": str(args.plan_manifest)},
        "lead_candidate": {
            "asset": lead.get("asset"),
            "audit_rank": lead.get("audit_rank"),
            "audit_status": lead.get("audit_status"),
            "best_queue_pnl": lead.get("best_queue_pnl"),
            "candidate_key": lead.get("candidate_key"),
            "experiment_labels": lead.get("experiment_labels"),
            "private_truth_ready_count": lead.get("private_truth_ready_count"),
            "promotion_blockers": lead.get("promotion_blockers"),
        },
        "lead_validation_evidence": evidence,
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "reason": "backtest_v1_hype_handoff_proposal_only_private_blocked",
        },
        "proposal": proposal,
        "research_ranking": {
            "backtest_candidate_filter_ready": plan.get("decision") == "KEEP",
            "handoff_proposal_ready": decision == "KEEP",
            "lead_asset": args.expected_asset,
            "lead_candidate_key": args.expected_candidate_key,
            "real_miner_input_ready": False,
            "status": decision_label,
            "strategy_evidence": False,
        },
        "safety": {
            "aws_or_remote_read": False,
            "broker_modified": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "new_no_order_diagnostic_started": False,
            "orders_cancels_redeems_sent": False,
            "raw_replay_or_full_store_scan": False,
            "shared_ingress_modified": False,
            "systemd_or_service_control_used": False,
            "trading_behavior_changed": False,
        },
        "strategy_evidence": False,
        "suite": as_dict(plan.get("suite")),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan-manifest", type=Path, default=DEFAULT_PLAN_MANIFEST)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--expected-asset", default="HYPE")
    parser.add_argument("--expected-candidate-key", default="30cfee296e1d0912f78109dd")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=100)
    parser.add_argument("--min-holdout-rows", type=int, default=50)
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
