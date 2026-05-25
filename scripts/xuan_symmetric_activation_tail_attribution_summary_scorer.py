#!/usr/bin/env python3
"""Score symmetric activation tail-attribution event-lite summaries."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_symmetric_activation_tail_attribution_summary_scorer"
SCHEMA_VERSION = "symmetric_activation_tail_attribution_summary_v1"
DEFAULT_SMOKE_ROOT = Path(
    "xuan_research_artifacts/xuan_symmetric_activation_tail_attribution_summary_smoke_20260525T000000Z"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_is_forbidden(path: Path) -> bool:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    return any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    if path_is_forbidden(path):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return None, reason
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def nested_get(obj: dict[str, Any], path: list[str]) -> Any:
    current: Any = obj
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def nested_sum(hist: Any) -> float:
    if isinstance(hist, (int, float)):
        return float(hist)
    if isinstance(hist, dict):
        return sum(nested_sum(value) for value in hist.values())
    return 0.0


def summary_from_aggregate(aggregate: dict[str, Any]) -> dict[str, Any] | None:
    value = nested_get(aggregate, ["event_lite", "symmetric_activation_tail_attribution_summary"])
    return value if isinstance(value, dict) else None


def load_summary_files(summary_dir: Path, root: Path) -> tuple[list[dict[str, Any]], str | None]:
    ok, reason = path_safe(summary_dir, root)
    if not ok:
        return [], reason
    if not summary_dir.exists():
        return [], "missing"
    out = []
    for path in sorted(summary_dir.glob("*.summary.json")):
        try:
            obj = load_json(path)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        summary = nested_get(obj, ["event_lite", "symmetric_activation_tail_attribution_summary"])
        if isinstance(summary, dict):
            out.append(summary)
    return out, None


def validate_field_contract(summary: dict[str, Any]) -> tuple[bool, list[str]]:
    contract = summary.get("field_contract")
    blockers: list[str] = []
    if not isinstance(contract, dict):
        return False, ["field_contract_missing"]
    expected_false = [
        "post_action_outcome_labels_included",
        "realized_pair_cost_used_as_live_criteria",
        "trading_behavior_changed",
        "private_truth_ready",
        "deployable",
        "promotion_gate_passed",
    ]
    for key in expected_false:
        if contract.get(key) is not False:
            blockers.append(f"field_contract_{key}_not_false")
    if contract.get("default_off") is not True:
        blockers.append("field_contract_default_off_not_true")
    if contract.get("status_reason_scope") != ["admitted|candidate"]:
        blockers.append("field_contract_status_reason_scope_mismatch")
    return not blockers, blockers


def score(
    default_aggregate: dict[str, Any] | None,
    enabled_aggregate: dict[str, Any] | None,
    summary_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blockers: list[str] = []
    checks: dict[str, Any] = {}
    default_summary = summary_from_aggregate(default_aggregate or {})
    enabled_summary = summary_from_aggregate(enabled_aggregate or {})
    checks["default_off_summary_absent"] = default_summary is None
    checks["enabled_summary_present"] = enabled_summary is not None
    if default_summary is not None:
        blockers.append("default_off_summary_present")
    if enabled_summary is None:
        blockers.append("enabled_summary_missing")
        enabled_summary = {}

    checks["schema_ok"] = enabled_summary.get("schema_version") == SCHEMA_VERSION
    if not checks["schema_ok"]:
        blockers.append("schema_version_mismatch")
    field_ok, field_blockers = validate_field_contract(enabled_summary)
    checks["field_contract_ok"] = field_ok
    blockers.extend(field_blockers)

    activation_key = "admitted|candidate|activation_required_activation_opp_age_1_5s"
    side_offset_key = "admitted|candidate|YES|offset_60_90|activation_required_activation_opp_age_1_5s"
    pair_qty = float(nested_get(enabled_summary, ["pair_qty_sum_by_status_reason_activation_bucket", activation_key]) or 0.0)
    residual_qty = float(
        nested_get(enabled_summary, ["residual_qty_sum_by_status_reason_activation_bucket", activation_key]) or 0.0
    )
    pair_tail_loss = float(
        nested_get(enabled_summary, ["pair_tail_loss_sum_by_status_reason_activation_bucket", activation_key]) or 0.0
    )
    checks["pair_qty_positive"] = pair_qty > 0.0
    checks["residual_qty_positive"] = residual_qty > 0.0
    checks["pair_tail_loss_positive"] = pair_tail_loss > 0.0
    if not checks["pair_qty_positive"]:
        blockers.append("pair_qty_missing")
    if not checks["residual_qty_positive"]:
        blockers.append("residual_qty_missing")
    if not checks["pair_tail_loss_positive"]:
        blockers.append("pair_tail_loss_missing")

    pair_bucket = float(
        nested_get(
            enabled_summary,
            ["pair_cost_bucket_by_status_reason_activation_bucket", activation_key, "pair_cost_1p00_1p05"],
        )
        or 0.0
    )
    checks["pair_cost_bucket_present"] = pair_bucket > 0.0
    if not checks["pair_cost_bucket_present"]:
        blockers.append("pair_cost_bucket_missing")

    seq_present = float(
        nested_get(
            enabled_summary,
            ["source_sequence_presence_by_status_reason_activation_bucket", activation_key, "present"],
        )
        or 0.0
    )
    checks["source_sequence_coverage_present"] = seq_present > 0.0
    if not checks["source_sequence_coverage_present"]:
        blockers.append("source_sequence_coverage_missing")

    side_offset_residual = float(
        nested_get(
            enabled_summary,
            ["residual_qty_sum_by_status_reason_side_offset_activation_bucket", side_offset_key],
        )
        or 0.0
    )
    checks["side_offset_activation_residual_present"] = side_offset_residual > 0.0
    if not checks["side_offset_activation_residual_present"]:
        blockers.append("side_offset_activation_residual_missing")

    exemplars = enabled_summary.get("residual_tail_exemplars_by_status_reason_activation_bucket")
    exemplar_match = False
    if isinstance(exemplars, list):
        exemplar_match = any(
            isinstance(item, dict) and item.get("status_reason_activation_bucket") == activation_key for item in exemplars
        )
    checks["residual_tail_exemplar_present"] = exemplar_match
    if not exemplar_match:
        blockers.append("residual_tail_exemplar_missing")

    aggregate_pair_total = nested_sum(enabled_summary.get("pair_qty_sum_by_status_reason_activation_bucket"))
    summary_pair_total = sum(nested_sum(row.get("pair_qty_sum_by_status_reason_activation_bucket")) for row in summary_rows)
    aggregate_residual_total = nested_sum(enabled_summary.get("residual_qty_sum_by_status_reason_activation_bucket"))
    summary_residual_total = sum(
        nested_sum(row.get("residual_qty_sum_by_status_reason_activation_bucket")) for row in summary_rows
    )
    checks["aggregate_pair_parity"] = not summary_rows or abs(aggregate_pair_total - summary_pair_total) <= 1e-9
    checks["aggregate_residual_parity"] = not summary_rows or abs(aggregate_residual_total - summary_residual_total) <= 1e-9
    if not checks["aggregate_pair_parity"]:
        blockers.append("aggregate_pair_parity_failed")
    if not checks["aggregate_residual_parity"]:
        blockers.append("aggregate_residual_parity_failed")

    return {
        "checks": checks,
        "blockers": blockers,
        "summary": {
            "activation_key": activation_key,
            "pair_qty": pair_qty,
            "residual_qty": residual_qty,
            "pair_tail_loss": pair_tail_loss,
            "aggregate_pair_total": aggregate_pair_total,
            "summary_pair_total": summary_pair_total,
            "aggregate_residual_total": aggregate_residual_total,
            "summary_residual_total": summary_residual_total,
            "summary_file_count": len(summary_rows),
        },
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    default_aggregate, default_error = safe_load(args.default_aggregate, root)
    enabled_aggregate, enabled_error = safe_load(args.enabled_aggregate, root)
    summary_rows, summary_error = load_summary_files(args.enabled_summary_dir, root)
    score_obj = score(default_aggregate, enabled_aggregate, summary_rows)
    blockers = list(score_obj["blockers"])
    for prefix, error in (("default_aggregate", default_error), ("enabled_aggregate", enabled_error)):
        if error:
            blockers.append(f"{prefix}_{error}")
    if summary_error and summary_error != "missing":
        blockers.append(f"enabled_summary_dir_{summary_error}")
    decision = "KEEP" if not blockers else "UNKNOWN"
    decision_label = (
        "KEEP_SYMMETRIC_ACTIVATION_TAIL_ATTRIBUTION_SUMMARY_SCORER_READY"
        if decision == "KEEP"
        else "UNKNOWN_SYMMETRIC_ACTIVATION_TAIL_ATTRIBUTION_SUMMARY_SCORER_INPUTS_INSUFFICIENT"
    )
    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "default_aggregate": str(args.default_aggregate),
        "enabled_aggregate": str(args.enabled_aggregate),
        "enabled_summary_dir": str(args.enabled_summary_dir),
        "score": {**score_obj, "blockers": blockers},
        "next_executable_action": (
            "Use this scorer only on allowlisted local pullbacks that already include tail attribution summaries; "
            "do not treat scorer readiness as promotion evidence."
            if decision == "KEEP"
            else "Fix missing tail attribution summary fields before any further local review."
        ),
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "SCORER_READY_NOT_PROMOTION_EVIDENCE",
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
    parser.add_argument("--default-aggregate", type=Path, default=DEFAULT_SMOKE_ROOT / "default_off/aggregate_report.json")
    parser.add_argument("--enabled-aggregate", type=Path, default=DEFAULT_SMOKE_ROOT / "enabled/aggregate_report.json")
    parser.add_argument("--enabled-summary-dir", type=Path, default=DEFAULT_SMOKE_ROOT / "enabled")
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
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
