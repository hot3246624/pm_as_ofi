#!/usr/bin/env python3
"""Inventory arrived safe handoff manifests for observable pre-action mining.

This local-only inventory looks for non-fixture safe feature-join pullback
handoff manifests inside the current worktree. Candidate handoffs are validated
with the committed safe handoff validator. It does not read events JSONL,
raw/replay stores, use SSH, start shadows/services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_rule_miner_handoff_arrival_inventory"
CONTRACT_NAME = "observable_pre_action_rule_miner_handoff_arrival_inventory_v1"
HANDOFF_SCHEMA = "observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_v1"
SAFE_HANDOFF_SPEC_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_SPEC_READY"
SAFE_HANDOFF_READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_SAFE_FEATURE_JOIN_PULLBACK_HANDOFF_READY"

DEFAULT_HANDOFF_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff_20260526T000507Z/"
    "manifest.json"
)
DEFAULT_HANDOFF_VALIDATOR = Path("scripts/xuan_observable_pre_action_rule_miner_safe_feature_join_pullback_handoff.py")

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

FALSE_SCOPE_FIELDS = (
    "new_data_fetched",
    "external_worktree_read",
    "ssh_used",
    "shadow_started",
    "canary_or_live_started",
    "events_jsonl_read",
    "events_jsonl_pulled",
    "raw_replay_or_full_store_scanned",
    "shared_ingress_or_broker_or_live_modified",
    "shared_ws_or_local_agg_or_service_started",
    "orders_cancels_redeems_sent",
    "trading_behavior_changed",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


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


def safe_json(path: Path, root: Path) -> tuple[dict[str, Any], str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {}, reason
    if not path.exists():
        return {}, "missing"
    try:
        obj = read_json(path)
    except Exception as exc:
        return {}, f"{type(exc).__name__}:{exc}"
    if not isinstance(obj, dict):
        return {}, "not_json_object"
    return obj, None


def fixture_like_path(path: Path) -> bool:
    lowered = "/".join(path.parts).lower()
    markers = (
        "_smoke_",
        "/fixtures/",
        "/fixture/",
        "/good/",
        "/bad",
        "/tmp_specs/",
        "fixture_scorer",
        "_postmerge",
        "real_input_inventory_smoke",
    )
    return any(marker in lowered for marker in markers)


def discover_handoff_candidates(root: Path, explicit: list[Path] | None, only_explicit: bool) -> list[Path]:
    candidates = set(explicit or [])
    if only_explicit:
        return sorted(candidates)
    base = root / "xuan_research_artifacts"
    if not base.exists():
        return sorted(candidates)
    patterns = (
        "*safe_feature_join_pullback_handoff*.json",
        "*feature_join*handoff*.json",
        "*handoff*.json",
    )
    for pattern in patterns:
        for path in base.rglob(pattern):
            ok, _ = path_safe(path, root)
            if ok:
                candidates.add(path)
    return sorted(candidates)


def validate_scope(scope: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if scope.get("current_worktree_only") is not True:
        blockers.append("scope_current_worktree_only_not_true")
    if scope.get("local_only") is not True:
        blockers.append("scope_local_only_not_true")
    for field in FALSE_SCOPE_FIELDS:
        if scope.get(field) is not False:
            blockers.append(f"scope_{field}_not_false")
    return blockers


def inspect_candidate(path: Path, root: Path) -> dict[str, Any]:
    data, error = safe_json(path, root)
    blockers: list[str] = []
    if error:
        blockers.append(f"candidate_{error}")
    fixture = fixture_like_path(path) or bool(data.get("fixture"))
    if fixture:
        blockers.append("candidate_fixture_or_smoke_only")
    if data.get("schema_version") != HANDOFF_SCHEMA:
        blockers.append("candidate_schema_mismatch")
    if data.get("strategy_evidence") is True:
        blockers.append("candidate_claims_strategy_evidence")
    blockers.extend(validate_scope(as_dict(data.get("scope"))))
    provenance = as_dict(data.get("provenance"))
    if provenance.get("pullback_already_inside_current_worktree") is not True:
        blockers.append("candidate_pullback_not_declared_inside_current_worktree")
    for field in (
        "events_jsonl_read",
        "events_jsonl_pulled",
        "raw_replay_or_full_store_scanned",
        "external_worktree_read",
        "ssh_used_by_validator",
        "orders_cancels_redeems_sent",
        "trading_behavior_changed",
    ):
        if provenance.get(field) is not False:
            blockers.append(f"candidate_provenance_{field}_not_false")
    promotion = as_dict(data.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("candidate_private_deployable_or_promotion_claim")
    files = as_dict(data.get("files"))
    missing_files = [
        key
        for key in (
            "feature_join_candidate_rows",
            "feature_join_source_link_summary",
            "feature_join_manifest",
            "same_window_aggregate_summary",
            "offline_label_csv",
            "training_bridge_joined_rows",
            "training_bridge_summary",
            "frozen_rule_manifest",
            "no_order_denominator_summary",
            "observable_pre_action_rule_miner_input_manifest",
        )
        if not files.get(key)
    ]
    if missing_files:
        blockers.append("candidate_required_file_keys_missing")
    return {
        "path": str(path),
        "schema_version": data.get("schema_version"),
        "handoff_id": data.get("handoff_id"),
        "fixture_or_smoke": fixture,
        "prevalidation_ready": not blockers,
        "missing_file_keys": missing_files,
        "blockers": sorted(set(blockers)),
    }


def run_validator(path: Path, output_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    command = [
        "python3",
        str(args.handoff_validator),
        "--handoff-manifest",
        str(path),
        "--output-dir",
        str(output_dir),
        "--min-feature-join-rows",
        str(args.min_feature_join_rows),
        "--min-bridge-joined-rows",
        str(args.min_bridge_joined_rows),
        "--min-denominator",
        str(args.min_denominator),
        "--min-rule-matches",
        str(args.min_rule_matches),
        "--min-source-sequence-coverage",
        str(args.min_source_sequence_coverage),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    manifest_path = output_dir / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        obj = read_json(manifest_path)
        manifest = obj if isinstance(obj, dict) else {}
    return {
        "command": " ".join(command),
        "returncode": result.returncode,
        "stdout_tail": result.stdout[-1000:],
        "stderr_tail": result.stderr[-1000:],
        "manifest_path": str(manifest_path),
        "decision_label": manifest.get("decision_label"),
        "blockers": as_list(manifest.get("blockers")),
        "ready": result.returncode == 0 and manifest.get("decision_label") == SAFE_HANDOFF_READY_DECISION,
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    spec, spec_error = safe_json(args.handoff_spec_manifest, root)
    validator_ok, validator_error = path_safe(args.handoff_validator, root)

    blockers: list[str] = []
    if spec_error:
        blockers.append(f"handoff_spec_manifest_{spec_error}")
    elif spec.get("decision_label") != SAFE_HANDOFF_SPEC_DECISION:
        blockers.append("handoff_spec_not_ready")
    if not validator_ok:
        blockers.append(f"handoff_validator_{validator_error}")
    elif not args.handoff_validator.exists():
        blockers.append("handoff_validator_missing")

    candidates = discover_handoff_candidates(root, args.candidate_handoff_manifest, args.only_explicit_handoffs)
    candidate_observations = [inspect_candidate(path, root) for path in candidates]
    prevalidation_ready = [
        (path, obs)
        for path, obs in zip(candidates, candidate_observations)
        if obs.get("prevalidation_ready")
    ]

    validations = []
    for idx, (path, _obs) in enumerate(prevalidation_ready, start=1):
        validations.append(run_validator(path, args.output_dir / f"validation_{idx}", args))

    ready_validations = [item for item in validations if item.get("ready")]
    if not ready_validations:
        blockers.append("non_fixture_safe_handoff_manifest_absent")

    if ready_validations and not [b for b in blockers if b != "non_fixture_safe_handoff_manifest_absent"]:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_READY"
        next_action = (
            "Run the named handoff validation commands and then real-input inventory v2/miner spec; do not start "
            "no-order diagnostics automatically."
        )
    elif blockers == ["non_fixture_safe_handoff_manifest_absent"] or (
        "non_fixture_safe_handoff_manifest_absent" in blockers and spec.get("decision_label") == SAFE_HANDOFF_SPEC_DECISION
    ):
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
        next_action = (
            "Wait for a non-fixture safe feature-join pullback handoff manifest inside the current worktree; "
            "do not mine fixtures and do not start no-order diagnostics."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_HANDOFF_ARRIVAL_INVENTORY_FAIL_CLOSED"
        next_action = "Fix handoff spec/validator/candidate blockers before any mining."

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "lane": "observable_pre_action_rule_miner_handoff_arrival_inventory",
        "source_inputs": {
            "handoff_spec_manifest": str(args.handoff_spec_manifest),
            "handoff_validator": str(args.handoff_validator),
            "explicit_candidate_handoff_manifests": [str(path) for path in args.candidate_handoff_manifest or []],
        },
        "readiness": {
            "ready": bool(ready_validations),
            "ready_validations": ready_validations,
            "validation_commands": [str(item.get("command")) for item in ready_validations],
            "blockers": sorted(set(blockers)),
        },
        "observations": {
            "handoff_spec_decision_label": spec.get("decision_label"),
            "candidate_handoff_count": len(candidate_observations),
            "prevalidation_ready_count": len(prevalidation_ready),
            "candidate_handoffs": candidate_observations[:40],
            "validations": validations,
        },
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "handoff_ready": bool(ready_validations),
            "no_order_diagnostic_allowed": False,
            "interpretation": (
                "Arrival inventory only. KEEP means a non-fixture handoff is ready for local scorer validation; "
                "UNKNOWN means no real handoff has arrived. Neither is promotion evidence."
            ),
        },
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "handoff_arrival_inventory_only",
        },
        "safety": {
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scan": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": next_action,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff-spec-manifest", type=Path, default=DEFAULT_HANDOFF_SPEC_MANIFEST)
    parser.add_argument("--handoff-validator", type=Path, default=DEFAULT_HANDOFF_VALIDATOR)
    parser.add_argument("--candidate-handoff-manifest", type=Path, action="append")
    parser.add_argument("--only-explicit-handoffs", action="store_true")
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-feature-join-rows", type=int, default=100)
    parser.add_argument("--min-bridge-joined-rows", type=int, default=100)
    parser.add_argument("--min-denominator", type=int, default=100)
    parser.add_argument("--min-rule-matches", type=int, default=20)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
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
