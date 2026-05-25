#!/usr/bin/env python3
"""Specify the observable pre-action feature-join contract.

This local-only source/spec review defines the smallest safe materialized
feature join needed before the observable pre-action rule miner can run on real
inputs. It inspects only current-worktree source files and committed manifests.
It does not fetch data, use SSH, start shadows/services, read events JSONL,
scan raw/replay stores, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_feature_join_contract"
CONTRACT_NAME = "observable_pre_action_rule_miner_feature_join_contract_v1"
DEFAULT_RUNNER = Path("tools/xuan_dplus_passive_passive_shadow_runner.py")
DEFAULT_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_spec_20260525T182006Z/"
    "manifest.json"
)
DEFAULT_INVENTORY_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_input_inventory_20260525T185536Z/"
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

RUNNER_TOKEN_GROUPS = {
    "source_sequence_parser": ("def source_sequence_id", "source_sequence_id(msg)"),
    "status_reason_hook": ("def record_source_link_transition", "status: str", "reason: str"),
    "blocked_status_recording": ("self.block(", "status=\"blocked\"", "reason=reason"),
    "admitted_status_recording": ("status=\"admitted\"", "reason=\"candidate\""),
    "pre_seed_qty_cost_available": (
        "same_qty = self.exposure_qty(side)",
        "opp_qty = self.exposure_qty(opp(side))",
        "same_cost_before = self.exposure_cost(side)",
        "opp_cost_before = self.exposure_cost(opp(side))",
    ),
    "source_ids_available": (
        "quote_intent_id = self.quote_intent_id(self.next_order_id)",
        "source_order_id=order.id",
        "source_sequence_id=trigger_source_sequence_id",
    ),
    "event_lite_default_off_pattern": (
        "event_lite_summary: bool = False",
        "source_link_transition_event_lite_summary: bool = False",
        "requires --event-lite-summary",
    ),
    "current_rows_only_in_events_jsonl": (
        "self.emit({\"kind\": \"candidate\"",
        "\"source_sequence_id\": trigger_source_sequence_id",
    ),
}

REQUIRED_MATERIALIZED_ROW_FIELDS = (
    "condition_id",
    "market_slug",
    "day_id",
    "quote_ts_ms",
    "side",
    "offset_s",
    "status_before_action",
    "block_reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "source_sequence_id_policy",
    "quote_intent_id_policy",
    "source_order_id_policy",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "open_qty_bucket",
    "deficit_bucket",
    "candidate_qty_bucket",
    "source_risk_direction",
)

TRAIN_ONLY_LABEL_FIELDS = (
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
)

DENOMINATOR_FIELDS = (
    "frozen_rule_id",
    "same_window_denominator_count",
    "admitted_count",
    "blocked_count",
    "source_sequence_coverage",
    "quote_intent_presence_rate",
    "source_order_presence_rate",
    "aggregate_parity",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


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


def safe_load_json(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
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


def safe_read_text(path: Path, root: Path) -> tuple[str, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return "", reason
    try:
        return path.read_text(encoding="utf-8", errors="replace"), None
    except FileNotFoundError:
        return "", "missing"
    except Exception as exc:
        return "", f"{type(exc).__name__}: {exc}"


def token_group_checks(source: str) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for group, tokens in RUNNER_TOKEN_GROUPS.items():
        missing = [token for token in tokens if token not in source]
        checks[group] = {
            "ok": not missing,
            "missing": missing,
            "tokens": list(tokens),
        }
    return checks


def source_review(source: str, error: str | None) -> dict[str, Any]:
    if error:
        return {
            "ready": False,
            "blockers": [error],
            "token_groups": {},
            "current_direct_materialized_export_present": False,
        }
    groups = token_group_checks(source)
    blockers = [f"missing_runner_token_group_{name}" for name, item in groups.items() if not item["ok"]]
    direct_export_tokens = (
        "--observable-pre-action-rule-miner-feature-join",
        "observable_pre_action_candidate_rows",
        "observable_pre_action_no_order_denominator_summary",
    )
    current_direct_export_present = all(token in source for token in direct_export_tokens)
    return {
        "ready": not blockers,
        "blockers": blockers,
        "token_groups": groups,
        "current_direct_materialized_export_present": current_direct_export_present,
        "interpretation": (
            "The runner already carries the pre-action state and source-link hooks needed for a default-off "
            "feature-join exporter. Current row-level candidates are still emitted only through events JSONL or "
            "aggregate summaries, so a safe materialized output target is required before real mining."
        ),
    }


def spec_review(spec: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    if error or not spec:
        return {"ready": False, "blockers": [error or "missing"], "decision_label": None}
    label = spec.get("decision_label")
    ready = label == "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SPEC_READY_REAL_INPUT_UNKNOWN"
    return {
        "ready": ready,
        "decision_label": label,
        "blockers": [] if ready else ["observable_pre_action_rule_miner_spec_not_ready"],
    }


def inventory_review(inventory: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    if error or not inventory:
        return {"ready": False, "expected_gap_state": False, "blockers": [error or "missing"], "decision_label": None}
    label = inventory.get("decision_label")
    readiness = as_dict(inventory.get("candidate_input_manifest_readiness"))
    blockers = [str(item) for item in readiness.get("blockers", []) if isinstance(item, str)]
    expected_gap_state = label == "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_INPUT_INVENTORY_GAPS"
    has_candidate_rows = readiness.get("summary", {}).get("candidate_rows_present") is True if isinstance(readiness.get("summary"), dict) else False
    has_source_summary = readiness.get("summary", {}).get("source_link_summary_present") is True if isinstance(readiness.get("summary"), dict) else False
    return {
        "ready": expected_gap_state,
        "expected_gap_state": expected_gap_state,
        "decision_label": label,
        "candidate_rows_present": has_candidate_rows,
        "source_link_summary_present": has_source_summary,
        "blockers_from_inventory": blockers,
        "blockers": [] if expected_gap_state else ["input_inventory_not_in_expected_gap_state"],
    }


def scope_flags() -> dict[str, Any]:
    scope = {
        "current_worktree_only": True,
        "local_only": True,
    }
    for field in REQUIRED_SCOPE_FALSE_FIELDS:
        scope[field] = False
    return scope


def feature_join_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "purpose": (
            "Create a safe, default-off materialized bridge from no-order pre-action runner state to "
            "observable_pre_action_rule_miner_input_v1 without reading events JSONL or changing trading behavior."
        ),
        "phase_1_default_off_runner_exporter_target": {
            "future_flag": "--observable-pre-action-rule-miner-feature-join-output",
            "requires": ["--event-lite-summary", "--source-link-transition-event-lite-summary"],
            "behavior_change": False,
            "writes_allowlisted_output_files": [
                "output/observable_pre_action_candidate_rows.jsonl",
                "output/observable_pre_action_source_link_summary.json",
                "output/observable_pre_action_feature_join_manifest.json",
            ],
            "recording_hook": (
                "record_source_link_transition should append compact rows for both admitted and blocked transitions "
                "when the flag is enabled; no order decision should read these rows."
            ),
            "row_identity": [
                "condition_id",
                "market_slug",
                "quote_ts_ms",
                "side",
                "status_before_action",
                "block_reason",
                "quote_intent_id_policy",
                "source_order_id_policy",
                "source_sequence_id_policy",
            ],
            "required_materialized_row_fields": list(REQUIRED_MATERIALIZED_ROW_FIELDS),
            "train_only_label_join_policy": (
                "Offline source_pair/source_residual labels may be joined only after the runner output is materialized "
                "and only for train/holdout scoring. These labels must be absent from any live predicate."
            ),
        },
        "phase_2_frozen_rule_denominator_target": {
            "future_scorer": "observable_pre_action_rule_miner_denominator_replay_scorer_v1",
            "inputs": [
                "output/observable_pre_action_candidate_rows.jsonl",
                "a frozen predicate produced by train/holdout miner",
            ],
            "writes_allowlisted_output_files": [
                "output/observable_pre_action_no_order_denominator_summary.json",
                "output/observable_pre_action_rule_miner_input_manifest.json",
            ],
            "required_denominator_fields": list(DENOMINATOR_FIELDS),
            "fail_closed_until": [
                "frozen_rule_id is present",
                "same_window_denominator_count is positive and >= contract threshold",
                "admitted_count and blocked_count are nonzero enough for denominator sanity",
                "source_sequence_coverage/quote_intent/source_order coverage are reported",
                "aggregate_parity is true",
            ],
        },
        "feature_policy": {
            "allowed_live_rule_fields": list(REQUIRED_MATERIALIZED_ROW_FIELDS),
            "train_only_label_fields": list(TRAIN_ONLY_LABEL_FIELDS),
            "forbidden_live_criteria": [
                "realized_pair_cost",
                "source_pair_qty",
                "source_pair_cost",
                "source_pair_pnl",
                "source_residual_qty",
                "source_residual_cost",
                "pair_outcome_bucket",
                "residual_tail_outcome_bucket",
                "settlement/future redeem labels",
                "public-profile single-day profit filters",
                "static side/offset/public-price caps as standalone strategy",
            ],
        },
        "fail_closed_conditions": [
            "candidate rows are only available through events JSONL",
            "row-level status_before_action or block_reason missing",
            "row-level source_sequence/quote_intent/source_order presence missing",
            "source-link evidence is aggregate only",
            "no exact frozen-rule same-window denominator summary exists",
            "output path is outside current worktree or contains forbidden raw/replay/events/shared-ingress fragments",
            "implementation changes order/cancel/redeem behavior",
            "manifest claims private truth, deployability, canary readiness, or promotion pass",
        ],
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    runner_source, runner_error = safe_read_text(args.runner, root)
    spec, spec_error = safe_load_json(args.spec_manifest, root)
    inventory, inventory_error = safe_load_json(args.inventory_manifest, root)

    runner = source_review(runner_source, runner_error)
    spec_status = spec_review(spec, spec_error)
    inventory_status = inventory_review(inventory, inventory_error)

    blockers: list[str] = []
    for prefix, status in (
        ("runner", runner),
        ("spec", spec_status),
        ("inventory", inventory_status),
    ):
        for blocker in status.get("blockers", []) or []:
            blockers.append(f"{prefix}:{blocker}")

    ready = not blockers
    current_export_present = bool(runner.get("current_direct_materialized_export_present"))
    if ready:
        decision = "KEEP"
        decision_label = (
            "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_READY_EXPORTER_TARGET_NAMED"
        )
        next_action = (
            "Implement observable_pre_action_rule_miner_feature_join_runner_instrumentation_default_off_v1: "
            "add a default-off materialized output writer and smoke it locally; do not start no-order diagnostics."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_FEATURE_JOIN_CONTRACT_SOURCE_BLOCKED"
        next_action = (
            "Resolve source/spec/inventory blockers before feature-join instrumentation. Do not start no-order diagnostics."
        )

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_feature_join_contract",
        "source_inputs": {
            "runner": str(args.runner),
            "spec_manifest": str(args.spec_manifest),
            "inventory_manifest": str(args.inventory_manifest),
        },
        "source_review": {
            "runner": runner,
            "spec": spec_status,
            "inventory": inventory_status,
        },
        "contract": feature_join_contract(),
        "current_direct_materialized_export_present": current_export_present,
        "blockers": blockers,
        "next_executable_action": next_action,
        "research_ranking": {
            "status": decision_label,
            "contract_ready": ready,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "This is an instrumentation/export contract only. It names the safe bridge required before rule "
                "mining, but it is not strategy evidence and does not authorize any diagnostic run."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "FEATURE_JOIN_CONTRACT_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "scope": scope_flags(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, default=DEFAULT_RUNNER)
    parser.add_argument("--spec-manifest", type=Path, default=DEFAULT_SPEC_MANIFEST)
    parser.add_argument("--inventory-manifest", type=Path, default=DEFAULT_INVENTORY_MANIFEST)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    parser.add_argument("--created-utc", default=utc_label())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
