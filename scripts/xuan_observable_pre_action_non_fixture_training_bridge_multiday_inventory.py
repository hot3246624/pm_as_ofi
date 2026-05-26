#!/usr/bin/env python3
"""Inventory non-fixture observable pre-action bridge rows for train/holdout readiness.

This local-only inventory scans current-worktree materialized bridge outputs and
checks whether any non-fixture joined-row source has enough train/holdout day
diversity for the offline training scorer. It does not read events JSONL, scan
raw/replay stores, use SSH, start services, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_observable_pre_action_non_fixture_training_bridge_multiday_inventory"
CONTRACT_NAME = "observable_pre_action_non_fixture_training_bridge_multiday_inventory_v1"
SUMMARY_SCHEMA = "observable_pre_action_training_bridge_summary_v1"
JOINED_ROW_SCHEMA = "observable_pre_action_training_bridge_joined_row_v1"
MATERIALIZER_CONTRACT = "observable_pre_action_non_fixture_training_bridge_materializer_v1"

READY_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_READY"
UNKNOWN_DECISION = "UNKNOWN_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_MULTIDAY_GAPS"
DISCARD_DECISION = "DISCARD_OBSERVABLE_PRE_ACTION_NON_FIXTURE_TRAINING_BRIDGE_FORBIDDEN_INPUT"

DEFAULT_TRAINING_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_spec_20260525T223507Z/"
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

FIXTURE_PATH_MARKERS = (
    "_smoke_",
    "_postmerge",
    "/fixtures/",
    "/fixture/",
    "/good/",
    "/bad",
    "/tmp_specs/",
    "fixture_scorer",
    "synthetic_fixture",
)

FALSE_SUMMARY_FIELDS = (
    "post_action_labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "events_jsonl_read",
    "events_jsonl_pulled",
    "raw_replay_or_full_store_scan",
    "trading_behavior_changed",
    "strategy_evidence",
    "private_truth_ready",
    "deployable",
)

REQUIRED_TRUE_ROW_FIELDS = ("post_action_labels_allowed_for_train_holdout_scoring",)
REQUIRED_FALSE_ROW_FIELDS = (
    "post_action_labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "trading_behavior_changed",
)
REQUIRED_LABEL_FIELDS = (
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "pair_outcome_bucket",
    "residual_tail_outcome_bucket",
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


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


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


def fixture_like_path(path: Path) -> bool:
    lowered = "/" + "/".join(path.parts).lower()
    return any(marker in lowered for marker in FIXTURE_PATH_MARKERS)


def discover_summaries(root: Path, explicit: list[Path], only_explicit: bool) -> list[Path]:
    candidates = set(explicit)
    if not only_explicit:
        base = root / "xuan_research_artifacts"
        if base.exists():
            for path in base.rglob("observable_pre_action_training_bridge_summary.json"):
                ok, _ = path_safe(path, root)
                if ok and not fixture_like_path(path):
                    candidates.add(path)
    return sorted(candidates)


def sibling_joined_rows(summary_path: Path) -> Path:
    return summary_path.parent / "observable_pre_action_training_bridge_joined_rows.jsonl"


def sibling_manifest(summary_path: Path) -> Path:
    return summary_path.parent / "manifest.json"


def safe_read_json(path: Path, root: Path) -> tuple[dict[str, Any], str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {}, reason
    if not path.exists():
        return {}, "missing"
    try:
        value = read_json(path)
    except Exception as exc:  # pragma: no cover - represented in manifest.
        return {}, f"{type(exc).__name__}:{exc}"
    if not isinstance(value, dict):
        return {}, "not_json_object"
    return value, None


def validate_summary(summary: dict[str, Any]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if summary.get("schema_version") != SUMMARY_SCHEMA:
        blockers.append("summary_schema_mismatch")
    if summary.get("fixture_only") is not False:
        blockers.append("summary_fixture_only_not_false")
    if summary.get("materializer_contract") != MATERIALIZER_CONTRACT:
        blockers.append("summary_materializer_contract_mismatch")
    if as_int(summary.get("joined_row_count"), -1) <= 0:
        blockers.append("summary_joined_row_count_missing")
    if as_int(summary.get("feature_row_count"), -1) != as_int(summary.get("joined_row_count"), -2):
        blockers.append("summary_feature_joined_row_count_mismatch")
    if as_int(summary.get("offline_label_row_count"), -1) != as_int(summary.get("joined_row_count"), -2):
        blockers.append("summary_label_joined_row_count_mismatch")
    if as_dict(summary.get("promotion_gate")).get("passed") is True:
        forbidden.append("summary_promotion_claim")
    for field in FALSE_SUMMARY_FIELDS:
        if summary.get(field) is not False:
            forbidden.append(f"summary_{field}_not_false")
    return blockers, forbidden


def validate_manifest(manifest: dict[str, Any]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if not manifest:
        blockers.append("materializer_manifest_missing")
        return blockers, forbidden
    if manifest.get("materialized") is not True:
        blockers.append("materializer_manifest_not_materialized")
    if manifest.get("strategy_evidence") is not False:
        forbidden.append("materializer_manifest_strategy_evidence_claim")
    if manifest.get("private_truth_ready") is not False or manifest.get("deployable") is not False:
        forbidden.append("materializer_manifest_private_or_deployable_claim")
    if as_dict(manifest.get("promotion_gate")).get("passed") is True:
        forbidden.append("materializer_manifest_promotion_claim")
    for field, expected in (
        ("events_jsonl_read", False),
        ("events_jsonl_pulled", False),
        ("raw_replay_or_full_store_scan", False),
        ("ssh_used", False),
        ("canary_or_live_started", False),
        ("orders_sent", False),
        ("cancels_sent", False),
        ("redeems_sent", False),
        ("trading_behavior_changed", False),
    ):
        if as_dict(manifest.get("safety")).get(field, False) is not expected:
            forbidden.append(f"materializer_manifest_safety_{field}_not_false")
    if manifest.get("blockers"):
        blockers.append("materializer_manifest_blockers_present")
    return blockers, forbidden


def validate_rows(rows: list[dict[str, Any]]) -> tuple[list[str], list[str], dict[str, Any]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    split_counts: Counter[str] = Counter()
    day_counts: Counter[str] = Counter()
    train_days: set[str] = set()
    holdout_days: set[str] = set()
    bridge_ids: set[str] = set()
    label_indexes: set[str] = set()
    source_sequence_present = 0
    for idx, row in enumerate(rows):
        if row.get("schema_version") != JOINED_ROW_SCHEMA:
            blockers.append(f"row_{idx}_schema_mismatch")
        bridge_id = str(row.get("bridge_row_id") or "")
        if not bridge_id:
            blockers.append(f"row_{idx}_bridge_row_id_missing")
        elif bridge_id in bridge_ids:
            blockers.append(f"row_{idx}_duplicate_bridge_row_id")
        bridge_ids.add(bridge_id)
        raw_label_index = row.get("offline_label_row_index")
        label_index = "" if raw_label_index in (None, "") else str(raw_label_index)
        if label_index:
            if label_index in label_indexes:
                blockers.append(f"row_{idx}_offline_label_reuse")
            label_indexes.add(label_index)
        for field in REQUIRED_TRUE_ROW_FIELDS:
            if row.get(field) is not True:
                blockers.append(f"row_{idx}_{field}_not_true")
        for field in REQUIRED_FALSE_ROW_FIELDS:
            if row.get(field) is not False:
                forbidden.append(f"row_{idx}_{field}_not_false")
        if row.get("private_truth_ready") is True or row.get("deployable") is True:
            forbidden.append(f"row_{idx}_private_or_deployable_claim")
        for field in REQUIRED_LABEL_FIELDS:
            if row.get(field) in (None, ""):
                blockers.append(f"row_{idx}_label_field_missing:{field}")
        split = str(row.get("split") or "missing")
        day = str(row.get("day_id") or "")
        split_counts[split] += 1
        if day:
            day_counts[day] += 1
            if split == "train":
                train_days.add(day)
            elif split == "holdout":
                holdout_days.add(day)
        else:
            blockers.append(f"row_{idx}_day_id_missing")
        if row.get("source_sequence_present") is True:
            source_sequence_present += 1
    metrics = {
        "available_days": sorted(day_counts),
        "available_day_count": len(day_counts),
        "day_counts": dict(sorted(day_counts.items())),
        "holdout_days": sorted(holdout_days),
        "label_row_reuse_count": max(0, len(rows) - len(label_indexes)),
        "source_sequence_coverage": round(source_sequence_present / len(rows), 8) if rows else 0.0,
        "split_counts": dict(sorted(split_counts.items())),
        "train_days": sorted(train_days),
    }
    return blockers, forbidden, metrics


def inspect_candidate(summary_path: Path, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if fixture_like_path(summary_path) and not args.allow_fixture_like_paths_for_smoke:
        blockers.append("candidate_fixture_or_smoke_path")
    ok, reason = path_safe(summary_path, root)
    if not ok:
        blockers.append(f"summary_path_{reason}")

    summary, summary_error = safe_read_json(summary_path, root)
    if summary_error:
        blockers.append(f"summary_{summary_error}")
    else:
        summary_blockers, summary_forbidden = validate_summary(summary)
        blockers.extend(summary_blockers)
        forbidden.extend(summary_forbidden)

    manifest, manifest_error = safe_read_json(sibling_manifest(summary_path), root)
    if manifest_error:
        blockers.append(f"manifest_{manifest_error}")
    else:
        manifest_blockers, manifest_forbidden = validate_manifest(manifest)
        blockers.extend(manifest_blockers)
        forbidden.extend(manifest_forbidden)

    joined_rows_path = sibling_joined_rows(summary_path)
    row_count = 0
    row_metrics: dict[str, Any] = {}
    ok, reason = path_safe(joined_rows_path, root)
    if not ok:
        blockers.append(f"joined_rows_{reason}")
    elif not joined_rows_path.exists():
        blockers.append("joined_rows_missing")
    else:
        try:
            rows = load_jsonl(joined_rows_path)
            row_count = len(rows)
            row_blockers, row_forbidden, row_metrics = validate_rows(rows)
            blockers.extend(row_blockers)
            forbidden.extend(row_forbidden)
        except Exception as exc:  # pragma: no cover - represented in manifest.
            blockers.append(f"joined_rows_{type(exc).__name__}:{exc}")

    summary_count = as_int(summary.get("joined_row_count"), -1) if summary else -1
    if row_count and summary_count != row_count:
        blockers.append("joined_rows_summary_count_mismatch")

    candidate_ready = not blockers and not forbidden
    return {
        "accepted": candidate_ready,
        "blockers": sorted(set(blockers)),
        "forbidden": sorted(set(forbidden)),
        "joined_rows": str(joined_rows_path),
        "manifest": str(sibling_manifest(summary_path)),
        "metrics": row_metrics,
        "row_count": row_count,
        "summary": str(summary_path),
        "summary_joined_row_count": summary_count,
    }


def choose_ready_candidate(candidates: list[dict[str, Any]], args: argparse.Namespace) -> tuple[dict[str, Any] | None, list[str]]:
    readiness_blockers: list[str] = []
    ready: list[dict[str, Any]] = []
    for candidate in candidates:
        metrics = as_dict(candidate.get("metrics"))
        split_counts = as_dict(metrics.get("split_counts"))
        train_rows = as_int(split_counts.get("train"))
        holdout_rows = as_int(split_counts.get("holdout"))
        train_days = as_list(metrics.get("train_days"))
        holdout_days = as_list(metrics.get("holdout_days"))
        candidate_blockers: list[str] = []
        if len(train_days) < args.min_train_days:
            candidate_blockers.append("train_day_diversity_below_min")
        if len(holdout_days) < args.min_holdout_days:
            candidate_blockers.append("holdout_day_diversity_below_min")
        if train_rows < args.min_train_rows:
            candidate_blockers.append("train_rows_below_min")
        if holdout_rows < args.min_holdout_rows:
            candidate_blockers.append("holdout_rows_below_min")
        if float(metrics.get("source_sequence_coverage") or 0.0) < args.min_source_sequence_coverage:
            candidate_blockers.append("source_sequence_coverage_below_min")
        if not candidate_blockers:
            ready.append(candidate)
        readiness_blockers.extend(candidate_blockers)
    if ready:
        return max(ready, key=lambda item: int(item.get("row_count") or 0)), []
    return None, sorted(set(readiness_blockers))


def aggregate_accepted(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    days: Counter[str] = Counter()
    split_counts: Counter[str] = Counter()
    total_rows = 0
    for candidate in candidates:
        metrics = as_dict(candidate.get("metrics"))
        total_rows += int(candidate.get("row_count") or 0)
        for day, count in as_dict(metrics.get("day_counts")).items():
            days[str(day)] += int(count)
        for split, count in as_dict(metrics.get("split_counts")).items():
            split_counts[str(split)] += int(count)
    return {
        "accepted_bridge_output_count": len(candidates),
        "available_day_count": len(days),
        "available_days": sorted(days),
        "day_counts": dict(sorted(days.items())),
        "split_counts": dict(sorted(split_counts.items())),
        "total_joined_rows": total_rows,
    }


def classify(forbidden: list[str], accepted: list[dict[str, Any]], readiness_blockers: list[str]) -> tuple[str, str, str]:
    if forbidden:
        return (
            "DISCARD",
            DISCARD_DECISION,
            "Discard forbidden/private/deployable/live-label bridge inputs; do not mine or replay them.",
        )
    if accepted and not readiness_blockers:
        return (
            "KEEP",
            READY_DECISION,
            "Run the named offline training scorer on the ready joined rows, then denominator replay only after a frozen rule is produced.",
        )
    return (
        "UNKNOWN",
        UNKNOWN_DECISION,
        "Wait for or materialize additional non-fixture same-contract bridge rows across enough train/holdout days; do not start no-order diagnostics automatically.",
    )


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    summary_paths = discover_summaries(root, args.bridge_summary, args.only_explicit)
    inspected = [inspect_candidate(path, root, args) for path in summary_paths]
    accepted = [item for item in inspected if item["accepted"]]
    forbidden = sorted(
        {
            blocker
            for item in inspected
            if "candidate_fixture_or_smoke_path" not in item["blockers"]
            for blocker in item["forbidden"]
        }
    )
    ready_candidate, readiness_blockers = choose_ready_candidate(accepted, args)
    if not accepted:
        readiness_blockers = sorted(set(readiness_blockers + ["non_fixture_bridge_output_absent_or_not_ready"]))
    aggregate = aggregate_accepted(accepted)
    decision, decision_label, next_action = classify(forbidden, accepted, readiness_blockers)
    ready_joined_rows = ready_candidate.get("joined_rows") if ready_candidate else None
    training_output_dir = (
        "xuan_research_artifacts/xuan_observable_pre_action_rule_miner_training_fixture_scorer_${TS}"
    )
    denominator_output_dir = (
        "xuan_research_artifacts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer_${TS}"
    )
    return {
        "artifact": ARTIFACT,
        "blockers": sorted(set(readiness_blockers)),
        "candidate_bridge_output_count": len(inspected),
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "forbidden_blockers": forbidden,
        "inspected_candidates": inspected,
        "lane": "observable_pre_action_non_fixture_training_bridge_multiday_inventory",
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "reason": "training_bridge_multiday_inventory_only",
        },
        "ready_candidate": ready_candidate,
        "research_ranking": {
            "bridge_rows_materialized": bool(accepted),
            "interpretation": (
                "Inventory readiness only. Non-fixture bridge rows remain offline research input; KEEP requires "
                "a ready train/holdout split and still cannot prove private truth, deployability, or promotion."
            ),
            "real_miner_input_ready": decision == "KEEP",
            "status": decision_label,
            "strategy_evidence": False,
        },
        "score": aggregate,
        "strategy_evidence": False,
        "validation_commands": [
            (
                "python3 scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py "
                f"--joined-rows {ready_joined_rows or '${ready_joined_rows}'} "
                f"--output-dir {training_output_dir}"
            ),
            (
                "python3 scripts/xuan_observable_pre_action_rule_miner_denominator_replay_fixture_scorer.py "
                "--candidate-rows ${feature_rows} --source-link-summary ${source_link_summary} "
                "--feature-join-manifest ${feature_join_manifest} --frozen-rule-manifest ${frozen_rule_manifest} "
                f"--same-window-aggregate-summary ${{same_window_aggregate_summary}} --output-dir {denominator_output_dir}"
            ),
        ],
        "safety": {
            "broker_modified": False,
            "canary_or_live_started": False,
            "cancels_sent": False,
            "events_jsonl_pulled": False,
            "events_jsonl_read": False,
            "external_worktree_read": False,
            "local_agg_or_shared_ws_started": False,
            "orders_sent": False,
            "raw_replay_or_full_store_scan": False,
            "redeems_sent": False,
            "service_control_used": False,
            "shared_ingress_modified": False,
            "ssh_used": False,
            "trading_behavior_changed": False,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge-summary", type=Path, action="append", default=[])
    parser.add_argument("--only-explicit", action="store_true")
    parser.add_argument("--allow-fixture-like-paths-for-smoke", action="store_true")
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--min-holdout-days", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=100)
    parser.add_argument("--min-holdout-rows", type=int, default=50)
    parser.add_argument("--min-source-sequence-coverage", type=float, default=0.95)
    parser.add_argument("--training-spec-manifest", type=Path, default=DEFAULT_TRAINING_SPEC_MANIFEST)
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
