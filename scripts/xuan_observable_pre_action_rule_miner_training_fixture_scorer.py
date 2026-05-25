#!/usr/bin/env python3
"""Run fixture offline rule discovery for observable pre-action mining.

This local-only scorer exercises the offline training mechanics over synthetic
bridge-joined rows. It enumerates predicates from allowed pre-action feature
families only, scores source_pair/source_residual labels only as offline
train/holdout objectives, emits a frozen rule manifest on fixture pass, and
fails closed for leakage, insufficient support, unstable holdout, missing
denominator dependency, or promotion/private/deployable claims.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ARTIFACT = "xuan_observable_pre_action_rule_miner_training_fixture_scorer"
CONTRACT_NAME = "observable_pre_action_rule_miner_training_fixture_scorer_v1"
FROZEN_SCHEMA = "frozen_observable_pre_action_rule_manifest_v1"
JOINED_ROW_SCHEMA = "observable_pre_action_training_bridge_joined_row_v1"
TRAINING_SPEC_DECISION = "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_SPEC_READY_REAL_INPUT_UNKNOWN"

DEFAULT_TRAINING_SPEC = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_spec_20260525T223507Z/"
    "manifest.json"
)
DEFAULT_JOINED_ROWS = Path(
    "xuan_research_artifacts/"
    "xuan_observable_pre_action_rule_miner_training_input_bridge_fixture_scorer_20260525T220507Z/"
    "observable_pre_action_training_bridge_joined_rows.jsonl"
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

FORBIDDEN_LIVE_FIELD_FRAGMENTS = (
    "source_pair",
    "source_residual",
    "pair_outcome",
    "residual_tail",
    "realized_pair_cost",
    "settlement",
    "redeem",
    "future",
    "private_truth",
    "public_profile",
    "dplus_failed",
)

BASE_REQUIRED_ROW_FIELDS = (
    "schema_version",
    "bridge_row_id",
    "split",
    "day_id",
    "status_before_action",
    "reason",
    "block_reason",
    "source_sequence_present",
    "quote_intent_present",
    "source_order_present",
    "side",
    "offset_s",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "pre_seed_open_qty",
    "pre_seed_open_cost",
    "candidate_qty",
    "source_risk_direction",
    "post_action_labels_allowed_for_train_holdout_scoring",
    "post_action_labels_allowed_in_live_predicate",
    "realized_pair_cost_used_as_live_criteria",
    "future_labels_used_as_live_criteria",
    "trading_behavior_changed",
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


def numeric_bucket(prefix: str, value: Any) -> str:
    val = as_float(value, None)  # type: ignore[arg-type]
    if val is None:
        return f"{prefix}_unknown"
    if abs(val) <= 1e-9:
        return f"{prefix}_zero"
    if val < 1:
        return f"{prefix}_lt_1"
    if val <= 5:
        return f"{prefix}_1_5"
    if val <= 20:
        return f"{prefix}_5_20"
    return f"{prefix}_gt_20"


def offset_bucket(value: Any) -> str:
    val = as_float(value, None)  # type: ignore[arg-type]
    if val is None:
        return "offset_unknown"
    if val < 30:
        return "offset_lt_30"
    if val < 60:
        return "offset_30_60"
    if val < 120:
        return "offset_60_120"
    return "offset_ge_120"


def row_feature_value(row: dict[str, Any], feature_name: str) -> Any:
    if feature_name == "offset_bucket_derived_from_offset_s":
        return offset_bucket(row.get("offset_s"))
    if feature_name == "offset_s_bucketized":
        return offset_bucket(row.get("offset_s"))
    if feature_name == "pre_seed_same_qty_bucket":
        return numeric_bucket("pre_seed_same_qty", row.get("pre_seed_same_qty"))
    if feature_name == "pre_seed_opp_qty_bucket":
        return numeric_bucket("pre_seed_opp_qty", row.get("pre_seed_opp_qty"))
    if feature_name == "pre_seed_open_qty_bucket":
        return numeric_bucket("pre_seed_open_qty", row.get("pre_seed_open_qty"))
    if feature_name == "pre_seed_deficit_qty_bucket_derived":
        deficit = max(0.0, as_float(row.get("pre_seed_opp_qty")) - as_float(row.get("pre_seed_same_qty")))
        return numeric_bucket("pre_seed_deficit_qty", deficit)
    if feature_name == "pre_seed_same_cost_bucket":
        return numeric_bucket("pre_seed_same_cost", row.get("pre_seed_same_cost"))
    if feature_name == "pre_seed_opp_cost_bucket":
        return numeric_bucket("pre_seed_opp_cost", row.get("pre_seed_opp_cost"))
    if feature_name == "pre_seed_open_cost_bucket":
        return numeric_bucket("pre_seed_open_cost", row.get("pre_seed_open_cost"))
    if feature_name == "candidate_qty_bucket":
        return numeric_bucket("candidate_qty", row.get("candidate_qty"))
    return row.get(feature_name)


def row_objective(row: dict[str, Any]) -> float:
    pair_pnl = as_float(row.get("source_pair_pnl"))
    residual_cost = as_float(row.get("source_residual_cost"))
    residual_tail = str(row.get("residual_tail_outcome_bucket") or "")
    tail_penalty = 0.0 if residual_tail in {"", "residual_none"} else 0.1
    return pair_pnl - residual_cost - tail_penalty


def row_pair_pnl(row: dict[str, Any]) -> float:
    return as_float(row.get("source_pair_pnl"))


def row_residual_cost(row: dict[str, Any]) -> float:
    return as_float(row.get("source_residual_cost"))


def has_forbidden_feature_name(name: str) -> bool:
    lowered = name.lower()
    return any(fragment in lowered for fragment in FORBIDDEN_LIVE_FIELD_FRAGMENTS)


def validate_training_spec(manifest: dict[str, Any]) -> list[str]:
    blockers = []
    if manifest.get("decision_label") != TRAINING_SPEC_DECISION:
        blockers.append("training_spec_not_ready")
    if manifest.get("strategy_evidence") is not False:
        blockers.append("training_spec_strategy_evidence_not_false")
    if manifest.get("no_order_diagnostic_allowed") is not False:
        blockers.append("training_spec_no_order_allowed_not_false")
    if manifest.get("private_truth_ready") is not False or manifest.get("deployable") is not False:
        blockers.append("training_spec_private_or_deployable_claim")
    promotion = as_dict(manifest.get("promotion_gate"))
    if promotion.get("passed") is not False:
        blockers.append("training_spec_promotion_claim")
    dependency = as_dict(as_dict(manifest.get("contract")).get("denominator_replay_dependency"))
    if dependency.get("required_before_no_order_proposal") is not True:
        blockers.append("denominator_dependency_missing")
    return blockers


def allowed_features_from_spec(manifest: dict[str, Any]) -> list[str]:
    return [str(item) for item in as_list(as_dict(manifest.get("contract")).get("allowed_live_feature_families"))]


def forbidden_features_from_spec(manifest: dict[str, Any]) -> list[str]:
    return [str(item) for item in as_list(as_dict(manifest.get("contract")).get("forbidden_live_feature_families"))]


def validate_rows(rows: list[dict[str, Any]]) -> list[str]:
    blockers = []
    if not rows:
        return ["joined_rows_empty"]
    for idx, row in enumerate(rows):
        if row.get("schema_version") != JOINED_ROW_SCHEMA:
            blockers.append(f"row_{idx}_schema_mismatch")
        missing = [field for field in BASE_REQUIRED_ROW_FIELDS + OFFLINE_LABEL_FIELDS if field not in row]
        if missing:
            blockers.append(f"row_{idx}_required_fields_missing:{','.join(missing)}")
        if row.get("post_action_labels_allowed_for_train_holdout_scoring") is not True:
            blockers.append(f"row_{idx}_offline_labels_not_allowed_for_scoring")
        if row.get("post_action_labels_allowed_in_live_predicate") is not False:
            blockers.append(f"row_{idx}_label_leakage_live_predicate")
        if row.get("realized_pair_cost_used_as_live_criteria") is not False:
            blockers.append(f"row_{idx}_realized_pair_cost_live_criteria")
        if row.get("future_labels_used_as_live_criteria") is not False:
            blockers.append(f"row_{idx}_future_labels_live_criteria")
        if row.get("trading_behavior_changed") is not False:
            blockers.append(f"row_{idx}_trading_behavior_changed")
        if row.get("private_truth_ready") is True or row.get("deployable") is True:
            blockers.append(f"row_{idx}_private_or_deployable_claim")
    if "train" not in {str(row.get("split")) for row in rows}:
        blockers.append("train_split_missing")
    if "holdout" not in {str(row.get("split")) for row in rows}:
        blockers.append("holdout_split_missing")
    return blockers


def build_candidate_predicates(rows: list[dict[str, Any]], allowed_features: list[str]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for feature in allowed_features:
        if has_forbidden_feature_name(feature):
            continue
        values = sorted({str(row_feature_value(row, feature)) for row in rows})
        for value in values:
            if value in {"", "None", "unknown"}:
                continue
            candidates.append(
                {
                    "predicate_expression": f"{feature} == {json.dumps(value, sort_keys=True)}",
                    "predicate_feature_names": [feature],
                    "conditions": [{"field": feature, "op": "eq", "value": value}],
                }
            )
    return candidates


def predicate_matches(row: dict[str, Any], predicate: dict[str, Any]) -> bool:
    for condition in as_list(predicate.get("conditions")):
        if not isinstance(condition, dict):
            return False
        field = str(condition.get("field") or "")
        op = str(condition.get("op") or "eq")
        expected = condition.get("value")
        actual = row_feature_value(row, field)
        if op == "eq" and str(actual) != str(expected):
            return False
        if op != "eq":
            return False
    return True


def selected_metrics(rows: list[dict[str, Any]], predicate: dict[str, Any]) -> dict[str, Any]:
    selected = [row for row in rows if predicate_matches(row, predicate)]
    by_split = {
        "train": [row for row in selected if row.get("split") == "train"],
        "holdout": [row for row in selected if row.get("split") == "holdout"],
    }
    metrics: dict[str, Any] = {
        "selected_count": len(selected),
        "selected_share_from_top_day": 0.0,
    }
    day_counts: dict[str, int] = {}
    for row in selected:
        day = str(row.get("day_id") or "unknown")
        day_counts[day] = day_counts.get(day, 0) + 1
    if selected:
        metrics["selected_share_from_top_day"] = round(max(day_counts.values()) / len(selected), 6)
    metrics["selected_day_counts"] = day_counts
    for split, split_rows in by_split.items():
        pair = sum(row_pair_pnl(row) for row in split_rows)
        residual = sum(row_residual_cost(row) for row in split_rows)
        objective = sum(row_objective(row) for row in split_rows)
        metrics[f"{split}_rows"] = len(split_rows)
        metrics[f"{split}_days"] = sorted({str(row.get("day_id")) for row in split_rows})
        metrics[f"{split}_pair_pnl"] = round(pair, 6)
        metrics[f"{split}_residual_cost"] = round(residual, 6)
        metrics[f"{split}_objective"] = round(objective, 6)
        metrics[f"{split}_objective_per_row"] = round(objective / len(split_rows), 6) if split_rows else 0.0
    return metrics


def static_side_offset_only(predicate: dict[str, Any]) -> bool:
    feature_names = {str(name) for name in as_list(predicate.get("predicate_feature_names"))}
    static = {"side", "offset_bucket_derived_from_offset_s", "offset_s_bucketized"}
    return bool(feature_names) and feature_names.issubset(static)


def predicate_blockers(
    predicate: dict[str, Any],
    metrics: dict[str, Any],
    args: argparse.Namespace,
) -> list[str]:
    blockers = []
    feature_names = [str(name) for name in as_list(predicate.get("predicate_feature_names"))]
    if not feature_names:
        blockers.append("predicate_feature_names_missing")
    forbidden = [name for name in feature_names if has_forbidden_feature_name(name)]
    if forbidden:
        blockers.append("forbidden_live_feature")
    if metrics.get("train_rows", 0) < args.min_selected_train_rows:
        blockers.append("selected_train_rows_below_min")
    if metrics.get("holdout_rows", 0) < args.min_selected_holdout_rows:
        blockers.append("selected_holdout_rows_below_min")
    if len(metrics.get("train_days") or []) < args.min_selected_train_days:
        blockers.append("selected_train_days_below_min")
    if len(metrics.get("holdout_days") or []) < args.min_selected_holdout_days:
        blockers.append("selected_holdout_days_below_min")
    if metrics.get("selected_share_from_top_day", 1.0) > args.max_selected_share_from_single_day:
        blockers.append("single_day_concentration_above_max")
    if metrics.get("train_objective", 0.0) <= 0:
        blockers.append("train_objective_not_positive")
    if metrics.get("holdout_objective", 0.0) <= 0:
        blockers.append("holdout_objective_not_positive")
    train_avg = as_float(metrics.get("train_objective_per_row"))
    holdout_avg = as_float(metrics.get("holdout_objective_per_row"))
    if train_avg > 0 and holdout_avg < train_avg * (1.0 - args.max_holdout_objective_degradation):
        blockers.append("holdout_objective_degraded")
    if static_side_offset_only(predicate):
        blockers.append("static_side_offset_only_predicate")
    return blockers


def choose_best_candidate(
    rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    args: argparse.Namespace,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[str]]:
    scored: list[dict[str, Any]] = []
    for predicate in candidates:
        metrics = selected_metrics(rows, predicate)
        blockers = predicate_blockers(predicate, metrics, args)
        scored.append(
            {
                "predicate": predicate,
                "metrics": metrics,
                "blockers": blockers,
                "passed": not blockers,
            }
        )
    passed = [item for item in scored if item["passed"]]
    if not passed:
        aggregate_blockers = sorted({blocker for item in scored for blocker in item["blockers"]})
        return None, scored[:20], aggregate_blockers or ["no_candidate_predicate_passed"]
    passed.sort(
        key=lambda item: (
            item["metrics"].get("holdout_objective", 0.0),
            item["metrics"].get("train_objective", 0.0),
            item["metrics"].get("holdout_rows", 0),
            item["metrics"].get("train_rows", 0),
        ),
        reverse=True,
    )
    return passed[0], scored[:20], []


def frozen_rule_manifest(
    *,
    best: dict[str, Any],
    training_spec_path: Path,
    joined_rows_path: Path,
    args: argparse.Namespace,
    allowed_features: list[str],
    forbidden_features: list[str],
) -> dict[str, Any]:
    predicate = best["predicate"]
    metrics = best["metrics"]
    return {
        "schema_version": FROZEN_SCHEMA,
        "frozen_rule_id": "fixture_observable_pre_action_rule_v1",
        "rule_version": "fixture_v1",
        "created_utc": utc_label(),
        "created_from_training_spec": str(training_spec_path),
        "train_manifest": str(joined_rows_path),
        "holdout_manifest": str(joined_rows_path),
        "predicate_expression": predicate["predicate_expression"],
        "predicate_feature_names": predicate["predicate_feature_names"],
        "predicate": {"all": predicate["conditions"]},
        "allowed_live_feature_families": allowed_features,
        "forbidden_live_feature_families": forbidden_features,
        "train_label_fields": list(OFFLINE_LABEL_FIELDS),
        "holdout_label_fields": list(OFFLINE_LABEL_FIELDS),
        "train_holdout_split": {
            "train_days": metrics.get("train_days", []),
            "holdout_days": metrics.get("holdout_days", []),
        },
        "objective_metrics": metrics,
        "stability_gates": {
            "train_objective_positive": metrics.get("train_objective", 0.0) > 0,
            "holdout_objective_positive": metrics.get("holdout_objective", 0.0) > 0,
            "single_day_concentration_ok": metrics.get("selected_share_from_top_day", 1.0)
            <= args.max_selected_share_from_single_day,
            "static_side_offset_public_price_cap": False,
            "fixture_only": True,
        },
        "same_window_scope": {
            "requires_denominator_replay": True,
            "denominator_replay_not_run": True,
            "same_window_denominator_count_min": 100,
            "rule_match_count_min": 20,
            "source_sequence_coverage_min": 0.95,
        },
        "uses_only_pre_action_fields": True,
        "uses_realized_pair_cost": False,
        "uses_future_labels": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "fixture_training_scorer_only",
        },
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    blockers = validate_paths([args.training_spec_manifest, args.joined_rows], root)
    training_spec: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    allowed_features: list[str] = []
    forbidden_features: list[str] = []
    candidate_count = 0
    best: dict[str, Any] | None = None
    candidate_preview: list[dict[str, Any]] = []
    outputs = {"frozen_rule_manifest": None}

    if not blockers:
        training_spec = read_json(args.training_spec_manifest)
        rows = load_jsonl(args.joined_rows)
        allowed_features = [str(item) for item in as_list(as_dict(training_spec.get("contract")).get("allowed_live_feature_families"))]
        forbidden_features = [
            str(item) for item in as_list(as_dict(training_spec.get("contract")).get("forbidden_live_feature_families"))
        ]
        blockers.extend(validate_training_spec(training_spec))
        blockers.extend(validate_rows(rows))
        if not allowed_features:
            blockers.append("allowed_live_feature_families_missing")
        if any(has_forbidden_feature_name(feature) for feature in allowed_features):
            blockers.append("forbidden_live_feature")
        candidates = build_candidate_predicates(rows, allowed_features)
        candidate_count = len(candidates)
        if not blockers:
            best, candidate_preview, candidate_blockers = choose_best_candidate(rows, candidates, args)
            blockers.extend(candidate_blockers)
            if best is not None:
                frozen = frozen_rule_manifest(
                    best=best,
                    training_spec_path=args.training_spec_manifest,
                    joined_rows_path=args.joined_rows,
                    args=args,
                    allowed_features=allowed_features,
                    forbidden_features=forbidden_features,
                )
                if frozen.get("private_truth_ready") is True or frozen.get("deployable") is True:
                    blockers.append("private_or_deployable_claim_present")
                promotion = as_dict(frozen.get("promotion_gate"))
                if promotion.get("passed") is True:
                    blockers.append("promotion_claim_present")
                if not blockers:
                    frozen_path = args.output_dir / "frozen_observable_pre_action_rule_manifest.json"
                    write_json(frozen_path, frozen)
                    outputs["frozen_rule_manifest"] = str(frozen_path)

    ready = not blockers
    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": "KEEP" if ready else "UNKNOWN",
        "decision_label": (
            "KEEP_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_FIXTURE_SCORER_READY"
            if ready
            else "UNKNOWN_OBSERVABLE_PRE_ACTION_RULE_MINER_TRAINING_FIXTURE_SCORER_FAIL_CLOSED"
        ),
        "fixture_only": True,
        "inputs": {
            "training_spec_manifest": str(args.training_spec_manifest),
            "joined_rows": str(args.joined_rows),
        },
        "outputs": outputs,
        "blockers": sorted(set(blockers)),
        "score": {
            "joined_row_count": len(rows),
            "candidate_predicate_count": candidate_count,
            "best_predicate": best["predicate"] if best else None,
            "best_metrics": best["metrics"] if best else None,
            "candidate_preview": candidate_preview,
        },
        "research_ranking": (
            "TRAINING_FIXTURE_SCORER_READY_NO_STRATEGY_EVIDENCE"
            if ready
            else "TRAINING_FIXTURE_SCORER_FAIL_CLOSED"
        ),
        "strategy_evidence": False,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate": {
            "passed": False,
            "reason": "training_fixture_scorer_only",
        },
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
        "next_executable_action": (
            "Run observable_pre_action_rule_miner_real_input_inventory_v1 again or wait for a non-fixture "
            "feature-join pullback with bridge-joined rows; do not start no-order diagnostics automatically."
            if ready
            else "Fix fixture training scorer leakage/support/stability blockers before any real mining."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training-spec-manifest", type=Path, default=DEFAULT_TRAINING_SPEC)
    parser.add_argument("--joined-rows", type=Path, default=DEFAULT_JOINED_ROWS)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument("--min-selected-train-rows", type=int, default=2)
    parser.add_argument("--min-selected-holdout-rows", type=int, default=2)
    parser.add_argument("--min-selected-train-days", type=int, default=2)
    parser.add_argument("--min-selected-holdout-days", type=int, default=2)
    parser.add_argument("--max-selected-share-from-single-day", type=float, default=0.60)
    parser.add_argument("--max-holdout-objective-degradation", type=float, default=0.50)
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
