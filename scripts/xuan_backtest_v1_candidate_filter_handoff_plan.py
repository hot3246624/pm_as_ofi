#!/usr/bin/env python3
"""Select search-safe backtest V1 candidates and plan same-window handoff.

This local-only planner reads only the backtest V1 candidate audit pack and
suite manifests/CSVs. It ranks search-safe private-blocked candidates, preserves
the private-truth boundary, and emits the exact same-window handoff requirements
needed before observable pre-action miner training can continue.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_backtest_v1_candidate_filter_handoff_plan"
CONTRACT_NAME = "xuan_backtest_v1_candidate_filter_handoff_plan_v1"

READY_DECISION = "KEEP_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_READY_PRIVATE_BLOCKED"
UNKNOWN_DECISION = "UNKNOWN_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_GAPS"
DISCARD_DECISION = "DISCARD_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_FORBIDDEN_INPUT"

DEFAULT_AUDIT_MANIFEST = Path(
    "/Volumes/PolyData/poly_backtest_data/derived/contract_examples/"
    "backtest_candidate_audit_pack_latest/BACKTEST_CANDIDATE_AUDIT_PACK_MANIFEST.json"
)
DEFAULT_SUITE_MANIFEST = Path(
    "/Volumes/PolyData/poly_backtest_data/derived/contract_examples/"
    "backtest_experiment_suite_deep_v1/BACKTEST_EXPERIMENT_SUITE_MANIFEST.json"
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

EXPECTED_ASSETS = ("BNB", "BTC", "DOGE", "ETH", "HYPE", "SOL", "XRP")
REQUIRED_FALSE_FIELDS = (
    "private_promotion_gate_pass",
    "deployable_ready",
)
REQUIRED_TRUE_FIELDS = (
    "search_safe_gate_pass",
    "historical_private_boundary_ok",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


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


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def path_safe(path: Path) -> tuple[bool, str | None]:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    if any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS):
        return False, "forbidden_path_fragment"
    if path.suffix == ".jsonl":
        return False, "jsonl_input_not_allowed_for_backtest_plan"
    return True, None


def validate_manifest_paths(audit_manifest: dict[str, Any], suite_manifest: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    for key in (
        "candidate_audit_pack_csv",
        "candidate_audit_pack_evidence_csv",
        "candidate_audit_pack_duckdb",
    ):
        raw = audit_manifest.get(key)
        if not raw:
            blockers.append(f"audit_manifest_{key}_missing")
            continue
        ok, reason = path_safe(Path(str(raw)))
        if not ok:
            blockers.append(f"audit_manifest_{key}_{reason}")
    for key, raw in as_dict(suite_manifest.get("input_hashes")).items():
        ok, reason = path_safe(Path(str(key)))
        if not ok:
            blockers.append(f"suite_input_hash_path_{reason}:{key}")
        if str(raw) == "":
            blockers.append(f"suite_input_hash_empty:{key}")
    return blockers


def validate_audit_manifest(manifest: dict[str, Any]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if manifest.get("schema_version") != "backtest_candidate_audit_pack_manifest_v1":
        blockers.append("audit_manifest_schema_mismatch")
    if manifest.get("ok") is not True:
        blockers.append("audit_manifest_not_ok")
    if as_dict(manifest.get("forbidden_result_columns")).get("candidates"):
        forbidden.append("audit_manifest_forbidden_candidate_columns_present")
    if as_dict(manifest.get("forbidden_result_columns")).get("evidence"):
        forbidden.append("audit_manifest_forbidden_evidence_columns_present")
    if as_int(manifest.get("private_promotion_ready_count")) != 0:
        forbidden.append("audit_manifest_private_promotion_ready_count_nonzero")
    if as_int(manifest.get("selected_candidate_count")) <= 0:
        blockers.append("audit_manifest_selected_candidate_count_missing")
    if as_int(manifest.get("search_safe_private_blocked_count")) <= 0:
        blockers.append("audit_manifest_search_safe_private_blocked_count_missing")
    if as_list(manifest.get("errors")):
        blockers.append("audit_manifest_errors_present")
    return blockers, forbidden


def validate_suite_manifest(manifest: dict[str, Any]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if manifest.get("schema_version") != "backtest_experiment_suite_manifest_v1":
        blockers.append("suite_manifest_schema_mismatch")
    if manifest.get("ok") is not True:
        blockers.append("suite_manifest_not_ok")
    failed_steps = [
        str(step.get("name"))
        for step in as_list(manifest.get("steps"))
        if isinstance(step, dict) and step.get("ok") is not True
    ]
    if failed_steps:
        blockers.append("suite_manifest_failed_steps:" + ",".join(failed_steps))
    if as_list(manifest.get("errors")):
        blockers.append("suite_manifest_errors_present")
    candidate_pack = as_dict(manifest.get("candidate_audit_pack"))
    if as_int(candidate_pack.get("private_promotion_ready_count")) != 0:
        forbidden.append("suite_manifest_private_promotion_ready_count_nonzero")
    return blockers, forbidden


def candidate_safe(row: dict[str, str], min_experiments: int) -> tuple[bool, list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if row.get("audit_status") != "SEARCH_SAFE_READY_PRIVATE_BLOCKED":
        blockers.append("audit_status_not_search_safe_private_blocked")
    if as_int(row.get("experiment_count")) < min_experiments:
        blockers.append("experiment_count_below_min")
    if as_int(row.get("private_truth_ready_count")) != 0:
        forbidden.append("candidate_private_truth_ready_count_nonzero")
    if as_int(row.get("scope_unsafe_count")) != 0:
        forbidden.append("candidate_scope_unsafe_count_nonzero")
    for field in REQUIRED_TRUE_FIELDS:
        if not as_bool(row.get(field)):
            blockers.append(f"{field}_not_true")
    for field in REQUIRED_FALSE_FIELDS:
        if as_bool(row.get(field)):
            forbidden.append(f"{field}_not_false")
    if "owner_private_truth_missing_for_deployable_promotion" not in str(row.get("promotion_blockers", "")):
        blockers.append("owner_private_truth_blocker_missing")
    return not blockers and not forbidden, blockers, forbidden


def sort_key(row: dict[str, str]) -> tuple[int, int, int, float, str]:
    return (
        as_int(row.get("audit_rank"), 10**9),
        -as_int(row.get("experiment_count")),
        -as_int(row.get("max_seen_matrix_count")),
        -as_float(row.get("best_queue_pnl"), -10**9),
        row.get("candidate_key", ""),
    )


def load_validation_manifest(path: Path) -> dict[str, Any]:
    ok, reason = path_safe(path)
    if not ok:
        return {"path": str(path), "blockers": [reason]}
    if not path.exists():
        return {"path": str(path), "blockers": ["missing"]}
    data = read_json(path)
    if not isinstance(data, dict):
        return {"path": str(path), "blockers": ["not_json_object"]}
    return data


def evidence_for_candidate(candidate_key: str, evidence_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    rows = [row for row in evidence_rows if row.get("candidate_key") == candidate_key]
    output: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: (item.get("experiment_label", ""), item.get("job_id", ""))):
        manifest_path = Path(row.get("validation_manifest", ""))
        manifest = load_validation_manifest(manifest_path)
        output.append(
            {
                "audit_status": row.get("audit_status"),
                "experiment_label": row.get("experiment_label"),
                "job_id": row.get("job_id"),
                "ok": as_bool(row.get("ok")),
                "private_truth_ready": as_bool(row.get("private_truth_ready")),
                "public_or_proxy_truth_only": as_bool(row.get("public_or_proxy_truth_only")),
                "queue_avg_pnl": row.get("queue_avg_pnl"),
                "queue_best_pnl": row.get("queue_best_pnl"),
                "requires_owner_private_truth": as_bool(row.get("requires_owner_private_truth")),
                "requires_raw": as_bool(row.get("requires_raw")),
                "requires_remote": as_bool(row.get("requires_remote")),
                "search_safe_validation_ready": as_bool(row.get("search_safe_validation_ready")),
                "source_truth_ready": as_bool(row.get("source_truth_ready")),
                "validation_fingerprint": row.get("validation_fingerprint"),
                "validation_manifest": str(manifest_path),
                "validation_manifest_ok": manifest.get("ok") is True,
                "validation_manifest_private_truth_ready": as_dict(manifest.get("promotion")).get("private_truth_ready") is True,
                "validation_scope": manifest.get("validation_scope", {}),
            }
        )
    return output


def summarize_assets(candidates: list[dict[str, str]]) -> dict[str, Any]:
    counts = Counter(row.get("asset", "") for row in candidates)
    return {
        "candidate_assets": sorted(key for key in counts if key),
        "candidate_count_by_asset": dict(sorted(counts.items())),
        "expected_assets": list(EXPECTED_ASSETS),
        "missing_candidate_assets": sorted(set(EXPECTED_ASSETS).difference({key for key in counts if key})),
    }


def handoff_plan_for_lead(lead: dict[str, str]) -> dict[str, Any]:
    asset = lead.get("asset", "")
    candidate_key = lead.get("candidate_key", "")
    return {
        "lead_asset": asset,
        "lead_candidate_key": candidate_key,
        "required_files": [
            "observable_pre_action_candidate_rows.jsonl",
            "observable_pre_action_source_link_summary.json",
            "observable_pre_action_feature_join_manifest.json",
            "observable_pre_action_same_window_offline_labels.csv",
            "same_window_aggregate_summary.json",
            "observable_pre_action_same_window_offline_label_handoff.json",
            "observable_pre_action_training_bridge_joined_rows.jsonl",
            "observable_pre_action_training_bridge_summary.json",
        ],
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
        "minimum_window": {
            "holdout_days": 2,
            "holdout_rows": 50,
            "source_sequence_coverage": 0.95,
            "train_days": 3,
            "train_rows": 100,
        },
        "label_policy": {
            "labels_allowed_for_train_holdout_scoring": True,
            "labels_allowed_in_live_predicate": False,
            "realized_pair_cost_allowed_as_live_criteria": False,
            "future_labels_allowed_as_live_criteria": False,
        },
        "validation_order": [
            "same-window handoff contract validator",
            "non-fixture training bridge materializer",
            "multiday same-window label handoff arrival inventory",
            "multiday same-window label handoff accumulator",
            "offline training scorer",
            "denominator replay scorer after frozen rule only",
        ],
        "safe_generation_note": (
            "Generate future same-window handoffs for this asset/candidate only through a bounded no-order diagnostic "
            "or already approved local materialization that writes row-level exact ids. Do not use raw/replay scans, "
            "public-profile single-day filters, live orders, or owner-private claims."
        ),
    }


def classify(blockers: list[str], forbidden: list[str], accepted: list[dict[str, str]]) -> tuple[str, str, str]:
    if forbidden:
        return (
            "DISCARD",
            DISCARD_DECISION,
            "Fix forbidden/private/promotion/scope issues before using the backtest audit pack.",
        )
    if blockers or not accepted:
        return (
            "UNKNOWN",
            UNKNOWN_DECISION,
            "Fix audit-pack readiness gaps or rerun the V1 suite; do not generate handoffs from unsafe candidates.",
        )
    return (
        "KEEP",
        READY_DECISION,
        "Use the selected search-safe private-blocked lead to request/generate a future same-window handoff with row-level exact ids; do not claim private truth or deployability.",
    )


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    blockers: list[str] = []
    forbidden: list[str] = []
    for path_name, path in (("audit_manifest", args.audit_manifest), ("suite_manifest", args.suite_manifest)):
        ok, reason = path_safe(path)
        if not ok:
            forbidden.append(f"{path_name}_{reason}")
        if not path.exists():
            blockers.append(f"{path_name}_missing")

    audit_manifest = read_json(args.audit_manifest) if args.audit_manifest.exists() else {}
    suite_manifest = read_json(args.suite_manifest) if args.suite_manifest.exists() else {}
    if not isinstance(audit_manifest, dict):
        audit_manifest = {}
        blockers.append("audit_manifest_not_json_object")
    if not isinstance(suite_manifest, dict):
        suite_manifest = {}
        blockers.append("suite_manifest_not_json_object")
    audit_blockers, audit_forbidden = validate_audit_manifest(audit_manifest)
    suite_blockers, suite_forbidden = validate_suite_manifest(suite_manifest)
    blockers.extend(audit_blockers + suite_blockers + validate_manifest_paths(audit_manifest, suite_manifest))
    forbidden.extend(audit_forbidden + suite_forbidden)

    candidate_csv = Path(str(audit_manifest.get("candidate_audit_pack_csv", "")))
    evidence_csv = Path(str(audit_manifest.get("candidate_audit_pack_evidence_csv", "")))
    if not candidate_csv.exists():
        blockers.append("candidate_audit_pack_csv_missing")
        candidate_rows: list[dict[str, str]] = []
    else:
        candidate_rows = read_csv(candidate_csv)
    if not evidence_csv.exists():
        blockers.append("candidate_evidence_csv_missing")
        evidence_rows: list[dict[str, str]] = []
    else:
        evidence_rows = read_csv(evidence_csv)

    accepted: list[dict[str, str]] = []
    rejected: list[dict[str, Any]] = []
    for row in candidate_rows:
        ok, row_blockers, row_forbidden = candidate_safe(row, args.min_experiment_count)
        if ok:
            accepted.append(row)
        else:
            rejected.append(
                {
                    "asset": row.get("asset"),
                    "blockers": row_blockers,
                    "candidate_key": row.get("candidate_key"),
                    "forbidden": row_forbidden,
                }
            )
        forbidden.extend(f"candidate_{row.get('candidate_key')}:{item}" for item in row_forbidden)
    accepted = sorted(accepted, key=sort_key)
    lead = accepted[0] if accepted else None
    lead_evidence = evidence_for_candidate(lead["candidate_key"], evidence_rows) if lead else []
    if lead and not lead_evidence:
        blockers.append("lead_candidate_evidence_missing")
    if lead and any(item.get("validation_manifest_private_truth_ready") for item in lead_evidence):
        forbidden.append("lead_candidate_validation_claims_private_truth")
    if lead and any(item.get("requires_raw") or item.get("requires_remote") for item in lead_evidence):
        forbidden.append("lead_candidate_evidence_requires_raw_or_remote")

    decision, decision_label, next_action = classify(blockers, forbidden, accepted)
    selected_candidates = [
        {
            "asset": row.get("asset"),
            "audit_rank": as_int(row.get("audit_rank")),
            "audit_status": row.get("audit_status"),
            "avg_queue_pnl": as_float(row.get("avg_queue_pnl")),
            "best_queue_pnl": as_float(row.get("best_queue_pnl")),
            "candidate_key": row.get("candidate_key"),
            "deployable_ready": as_bool(row.get("deployable_ready")),
            "experiment_count": as_int(row.get("experiment_count")),
            "experiment_labels": row.get("experiment_labels"),
            "max_seen_matrix_count": as_int(row.get("max_seen_matrix_count")),
            "private_promotion_gate_pass": as_bool(row.get("private_promotion_gate_pass")),
            "private_truth_ready_count": as_int(row.get("private_truth_ready_count")),
            "promotion_blockers": row.get("promotion_blockers"),
            "search_safe_gate_pass": as_bool(row.get("search_safe_gate_pass")),
        }
        for row in accepted[: args.top_n]
    ]
    return {
        "artifact": ARTIFACT,
        "blockers": sorted(set(blockers)),
        "candidate_filter": {
            "accepted_candidate_count": len(accepted),
            "input_candidate_count": len(candidate_rows),
            "min_experiment_count": args.min_experiment_count,
            "rejected_candidates": rejected[:20],
            "selected_candidates": selected_candidates,
        },
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "forbidden_blockers": sorted(set(forbidden)),
        "handoff_plan": handoff_plan_for_lead(lead) if lead else None,
        "inputs": {
            "audit_manifest": str(args.audit_manifest),
            "candidate_audit_pack_csv": str(candidate_csv),
            "candidate_audit_pack_evidence_csv": str(evidence_csv),
            "suite_manifest": str(args.suite_manifest),
        },
        "lead_candidate": selected_candidates[0] if selected_candidates else None,
        "lead_candidate_evidence": lead_evidence,
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "private_promotion_ready_count": as_int(audit_manifest.get("private_promotion_ready_count")),
            "reason": "backtest_v1_candidate_filter_private_blocked_only",
        },
        "research_ranking": {
            "backtest_candidate_filter_ready": decision == "KEEP",
            "candidate_assets": summarize_assets(accepted),
            "interpretation": (
                "Backtest V1 audit pack is a search-safe proxy candidate filter. Leads remain private-blocked and "
                "must not be treated as owner private truth, deployable rules, or promotion evidence."
            ),
            "lead_asset": lead.get("asset") if lead else None,
            "lead_candidate_key": lead.get("candidate_key") if lead else None,
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
        "suite": {
            "candidate_audit_pack_fingerprint": as_dict(suite_manifest.get("candidate_audit_pack")).get("candidate_audit_pack_fingerprint")
            or audit_manifest.get("candidate_audit_pack_fingerprint"),
            "ok": suite_manifest.get("ok") is True,
            "readiness_fingerprint": next(
                (
                    step.get("readiness_fingerprint")
                    for step in as_list(suite_manifest.get("steps"))
                    if isinstance(step, dict) and step.get("name") == "readiness_report"
                ),
                None,
            ),
            "suite_fingerprint": suite_manifest.get("suite_fingerprint"),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-manifest", type=Path, default=DEFAULT_AUDIT_MANIFEST)
    parser.add_argument("--suite-manifest", type=Path, default=DEFAULT_SUITE_MANIFEST)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--min-experiment-count", type=int, default=2)
    parser.add_argument("--top-n", type=int, default=6)
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
