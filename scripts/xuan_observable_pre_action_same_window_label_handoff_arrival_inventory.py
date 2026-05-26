#!/usr/bin/env python3
"""Inventory arrived same-window offline-label handoff manifests.

This local-only inventory looks for non-fixture same-window offline-label
handoffs in the current worktree and validates candidates with the committed
handoff contract. It does not read events JSONL, scan raw/replay stores, use
SSH, start services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_same_window_label_handoff_arrival_inventory"
CONTRACT_NAME = "observable_pre_action_same_window_label_handoff_arrival_inventory_v1"
HANDOFF_SCHEMA = "observable_pre_action_same_window_offline_label_handoff_v1"
CONTRACT_READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_CONTRACT_READY"
HANDOFF_READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_OFFLINE_LABEL_HANDOFF_READY"

DEFAULT_CONTRACT_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_same_window_offline_label_handoff_contract_20260526T065812Z/"
    "manifest.json"
)
DEFAULT_VALIDATOR = Path("scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py")

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

REQUIRED_FILE_KEYS = (
    "feature_join_candidate_rows",
    "feature_join_source_link_summary",
    "feature_join_manifest",
    "same_window_aggregate_summary",
    "offline_label_csv",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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
        "synthetic_fixture",
    )
    return any(marker in lowered for marker in markers)


def resolve_handoff_path(raw: Any, base_dir: Path) -> Path:
    path = Path(str(raw))
    return path if path.is_absolute() else base_dir / path


def discover_candidates(root: Path, explicit: list[Path] | None, only_explicit: bool) -> list[Path]:
    candidates = set(explicit or [])
    if only_explicit:
        return sorted(candidates)
    base = root / "xuan_research_artifacts"
    if not base.exists():
        return sorted(candidates)
    patterns = (
        "*same_window*offline_label*handoff*.json",
        "*same_window*offline*label*handoff*.json",
        "*same_window*label*handoff*.json",
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
    label_policy = as_dict(data.get("label_policy"))
    if label_policy.get("labels_allowed_for_train_holdout_scoring") is not True:
        blockers.append("candidate_labels_not_train_holdout_allowed")
    if label_policy.get("labels_allowed_in_live_predicate") is not False:
        blockers.append("candidate_labels_live_predicate_not_false")
    if label_policy.get("realized_pair_cost_allowed_as_live_criteria") is not False:
        blockers.append("candidate_realized_pair_cost_live_not_false")
    promotion = as_dict(data.get("promotion_gate"))
    if promotion.get("passed") is True or promotion.get("private_truth_ready") is True or promotion.get("deployable") is True:
        blockers.append("candidate_private_deployable_or_promotion_claim")
    files = as_dict(data.get("files"))
    missing_files = [key for key in REQUIRED_FILE_KEYS if not files.get(key)]
    if missing_files:
        blockers.append("candidate_required_file_keys_missing")
    for key, raw in files.items():
        path_value = resolve_handoff_path(raw, path.parent)
        ok, reason = path_safe(path_value, root)
        if not ok:
            blockers.append(f"candidate_file_{key}_{reason}")
    return {
        "blockers": sorted(set(blockers)),
        "fixture_or_smoke": fixture,
        "handoff_id": data.get("handoff_id"),
        "missing_file_keys": missing_files,
        "path": str(path),
        "prevalidation_ready": not blockers,
        "schema_version": data.get("schema_version"),
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
        "--min-feature-rows",
        str(args.min_feature_rows),
        "--min-joined-rows",
        str(args.min_joined_rows),
        "--min-join-coverage",
        str(args.min_join_coverage),
        "--min-slug-overlap-count",
        str(args.min_slug_overlap_count),
        "--min-condition-overlap-count",
        str(args.min_condition_overlap_count),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    manifest_path = output_dir / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        obj = read_json(manifest_path)
        manifest = obj if isinstance(obj, dict) else {}
    return {
        "blockers": as_list(manifest.get("blockers")),
        "command": " ".join(command),
        "decision_label": manifest.get("decision_label"),
        "manifest_path": str(manifest_path),
        "ready": result.returncode == 0 and manifest.get("decision_label") == HANDOFF_READY_DECISION,
        "returncode": result.returncode,
        "stderr_tail": result.stderr[-1000:],
        "stdout_tail": result.stdout[-1000:],
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    contract, contract_error = safe_json(args.contract_manifest, root)
    validator_ok, validator_error = path_safe(args.handoff_validator, root)

    blockers: list[str] = []
    if contract_error:
        blockers.append(f"contract_manifest_{contract_error}")
    elif contract.get("decision_label") != CONTRACT_READY_DECISION:
        blockers.append("contract_not_ready")
    if not validator_ok:
        blockers.append(f"handoff_validator_{validator_error}")
    elif not args.handoff_validator.exists():
        blockers.append("handoff_validator_missing")

    candidates = discover_candidates(root, args.candidate_handoff_manifest, args.only_explicit_handoffs)
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
        blockers.append("non_fixture_same_window_label_handoff_manifest_absent")

    if ready_validations and not [b for b in blockers if b != "non_fixture_same_window_label_handoff_manifest_absent"]:
        decision = "KEEP"
        decision_label = "KEEP_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_READY"
        next_action = (
            "Run the named validation commands, then feature_join_offline_label_bridge_feasibility_audit and "
            "training bridge scorer; do not start no-order diagnostics automatically."
        )
    elif blockers == ["non_fixture_same_window_label_handoff_manifest_absent"] or (
        "non_fixture_same_window_label_handoff_manifest_absent" in blockers
        and contract.get("decision_label") == CONTRACT_READY_DECISION
    ):
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_NO_REAL_HANDOFF"
        next_action = (
            "Wait for a non-fixture same-window offline-label handoff manifest inside the current worktree; "
            "do not mine fixtures and do not start no-order diagnostics."
        )
    else:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_OBSERVABLE_PRE_ACTION_SAME_WINDOW_LABEL_HANDOFF_ARRIVAL_INVENTORY_FAIL_CLOSED"
        next_action = "Fix contract/validator/candidate blockers before any bridge scoring."

    return {
        "artifact": ARTIFACT,
        "blockers": sorted(set(blockers)),
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "lane": "observable_pre_action_same_window_label_handoff_arrival_inventory",
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "observations": {
            "candidate_handoff_count": len(candidate_observations),
            "candidate_handoffs": candidate_observations[:50],
            "contract_decision_label": contract.get("decision_label"),
            "prevalidation_ready_count": len(prevalidation_ready),
            "validations": validations,
        },
        "private_truth_ready": False,
        "promotion_gate": {"passed": False, "reason": "same_window_label_handoff_arrival_inventory_only"},
        "readiness": {
            "blockers": sorted(set(blockers)),
            "ready": bool(ready_validations),
            "ready_validations": ready_validations,
            "validation_commands": [str(item.get("command")) for item in ready_validations],
        },
        "research_ranking": {
            "handoff_ready": bool(ready_validations),
            "interpretation": "Arrival inventory only. KEEP means a same-window label handoff can enter local bridge validation; UNKNOWN means no real handoff has arrived. Neither is promotion evidence.",
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
        "source_inputs": {
            "contract_manifest": str(args.contract_manifest),
            "explicit_candidate_handoff_manifests": [str(path) for path in args.candidate_handoff_manifest or []],
            "handoff_validator": str(args.handoff_validator),
        },
        "strategy_evidence": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract-manifest", type=Path, default=DEFAULT_CONTRACT_MANIFEST)
    parser.add_argument("--handoff-validator", type=Path, default=DEFAULT_VALIDATOR)
    parser.add_argument("--candidate-handoff-manifest", type=Path, action="append")
    parser.add_argument("--only-explicit-handoffs", action="store_true")
    parser.add_argument("--created-utc", default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-feature-rows", type=int, default=100)
    parser.add_argument("--min-joined-rows", type=int, default=100)
    parser.add_argument("--min-join-coverage", type=float, default=0.95)
    parser.add_argument("--min-slug-overlap-count", type=int, default=1)
    parser.add_argument("--min-condition-overlap-count", type=int, default=1)
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
