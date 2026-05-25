#!/usr/bin/env python3
"""Audit the edge0.075 symmetric-activation residual gap.

This local-only audit reads only allowlisted no-order pullback summaries and
already-produced scorer manifests. It checks whether the failed residual budget
can be explained with legal pre-action activation/source diagnostics, or whether
the parameter line should be discarded. It does not start runners, fetch data,
inspect events JSONL/raw stores, or change trading behavior.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_symmetric_activation_edge075_residual_gap_audit"
SCHEMA_VERSION = "symmetric_activation_edge075_residual_gap_audit_v1"
DEFAULT_DRIVER = Path(
    "xuan_research_artifacts/xuan_symmetric_activation_shadow_review_edge0075_driver_20260525T120202Z"
)
DEFAULT_REVIEW = DEFAULT_DRIVER / "local_review_20260525T133422Z"
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


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


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


def nested_get(obj: dict[str, Any], path: list[str], default: Any = None) -> Any:
    current: Any = obj
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return default if current is None else current


def nested_sum(value: Any) -> float:
    if isinstance(value, dict):
        return sum(nested_sum(child) for child in value.values())
    return as_float(value)


def top_items(mapping: Any, total: float | None = None, limit: int = 8) -> list[dict[str, Any]]:
    if not isinstance(mapping, dict):
        return []
    if total is None:
        total = nested_sum(mapping)
    rows = []
    for key, value in mapping.items():
        amount = nested_sum(value)
        rows.append(
            {
                "key": str(key),
                "value": round(amount, 6),
                "share": round(amount / total, 6) if total else None,
            }
        )
    return sorted(rows, key=lambda row: row["value"], reverse=True)[:limit]


def bucket_value(hist: Any, *names: str) -> float:
    if not isinstance(hist, dict):
        return 0.0
    return sum(as_float(hist.get(name)) for name in names)


def sum_nested_bucket(mapping: Any, bucket_names: tuple[str, ...]) -> float:
    if not isinstance(mapping, dict):
        return 0.0
    return sum(bucket_value(value, *bucket_names) for value in mapping.values())


def top_nested_bucket(mapping: Any, bucket_names: tuple[str, ...], limit: int = 8) -> list[dict[str, Any]]:
    if not isinstance(mapping, dict):
        return []
    rows = []
    total = 0.0
    for key, value in mapping.items():
        amount = bucket_value(value, *bucket_names)
        total += amount
        rows.append({"key": str(key), "value": amount})
    rows = sorted(rows, key=lambda row: row["value"], reverse=True)[:limit]
    for row in rows:
        row["value"] = round(row["value"], 6)
        row["share"] = round(row["value"] / total, 6) if total else None
    return rows


def audit_contract() -> dict[str, Any]:
    return {
        "contract_name": SCHEMA_VERSION,
        "purpose": (
            "Decide whether edge0.075/window20 residual failure has a legal local-only next target "
            "or should be discarded."
        ),
        "allowed_inputs": [
            "allowlisted output/aggregate_report.json",
            "allowlisted output/*.summary.json-derived aggregate fields",
            "shadow acceptance manifest",
            "symmetric activation summary scorer manifest",
            "symmetric activation tail-attribution scorer manifest",
            "acceptance adapter manifest",
        ],
        "forbidden_interpretations": [
            "future labels as live criteria",
            "realized pair_cost as live criteria",
            "static YES/NO side cap",
            "static public-price cap",
            "static offset cap",
            "public-profile single-day filters",
            "D+ failed micro-deficit/ledger/tiny-deficit/fill-to-balance families",
            "private truth, deployable, canary, or promotion claims",
        ],
        "field_contract": {
            "post_action_outcome_labels_included": False,
            "realized_pair_cost_used_as_live_criteria": False,
            "trading_behavior_changed": False,
            "private_truth_ready": False,
            "deployable": False,
            "promotion_gate_passed": False,
        },
    }


def evaluate(
    aggregate: dict[str, Any],
    shadow_acceptance: dict[str, Any],
    activation_scorer: dict[str, Any],
    tail_scorer: dict[str, Any],
    acceptance_adapter: dict[str, Any],
) -> dict[str, Any]:
    event_lite = aggregate.get("event_lite") if isinstance(aggregate.get("event_lite"), dict) else {}
    activation = event_lite.get("symmetric_activation_summary")
    activation = activation if isinstance(activation, dict) else {}
    tail = event_lite.get("symmetric_activation_tail_attribution_summary")
    tail = tail if isinstance(tail, dict) else {}
    source_link = event_lite.get("source_link_transition_diagnostics")
    source_link = source_link if isinstance(source_link, dict) else {}
    metrics = shadow_acceptance.get("trading_metrics") if isinstance(shadow_acceptance.get("trading_metrics"), dict) else {}
    promotion = shadow_acceptance.get("promotion_gate") if isinstance(shadow_acceptance.get("promotion_gate"), dict) else {}
    normalized = promotion.get("normalized_risk_budget") if isinstance(promotion.get("normalized_risk_budget"), dict) else {}
    adapter_eval = acceptance_adapter.get("adapter_evaluation") if isinstance(acceptance_adapter.get("adapter_evaluation"), dict) else {}

    residual_qty = as_float(metrics.get("residual_qty"))
    residual_cost = as_float(metrics.get("residual_cost"))
    residual_qty_share = as_float(normalized.get("residual_qty_share_of_filled", metrics.get("residual_qty_share_of_filled")))
    residual_cost_share = as_float(
        normalized.get("residual_cost_share_of_filled_cost", metrics.get("residual_cost_share_of_filled_cost"))
    )
    fee_adjusted_pair_pnl = as_float(metrics.get("fee_adjusted_pair_pnl_proxy"))

    residual_by_activation = tail.get("residual_qty_sum_by_status_reason_activation_bucket") or {}
    residual_cost_by_activation = tail.get("residual_cost_sum_by_status_reason_activation_bucket") or {}
    residual_by_side_offset = tail.get("residual_qty_sum_by_status_reason_side_offset_activation_bucket") or {}
    residual_cost_by_side_offset = tail.get("residual_cost_sum_by_status_reason_side_offset_activation_bucket") or {}
    pair_qty_by_activation = tail.get("pair_qty_sum_by_status_reason_activation_bucket") or {}
    pair_cost_by_activation = tail.get("pair_cost_bucket_by_status_reason_activation_bucket") or {}

    projected_residual_by_activation = activation.get("projected_residual_rate_bucket_by_status_reason_activation_bucket") or {}
    projected_pair_cost_by_activation = activation.get("projected_pair_cost_bucket_by_status_reason_activation_bucket") or {}
    candidate_count_by_activation = activation.get("candidate_count_by_status_reason_activation_bucket") or {}
    source_sequence_by_activation = activation.get("source_sequence_presence_by_status_reason_activation_bucket") or {}

    gt20_projected_admitted = sum_nested_bucket(
        {k: v for k, v in projected_residual_by_activation.items() if str(k).startswith("admitted|candidate|")},
        ("projected_residual_gt_20pct",),
    )
    admitted_candidates = nested_sum(
        {k: v for k, v in candidate_count_by_activation.items() if str(k).startswith("admitted|candidate")}
    )
    residual_total_by_activation = nested_sum(residual_by_activation)
    residual_cost_total_by_activation = nested_sum(residual_cost_by_activation)
    top_residual_activation = top_items(residual_by_activation, residual_total_by_activation, 6)
    top_residual_cost_activation = top_items(residual_cost_by_activation, residual_cost_total_by_activation, 6)
    top_residual_side_offset = top_items(residual_by_side_offset, nested_sum(residual_by_side_offset), 10)
    top_residual_cost_side_offset = top_items(residual_cost_by_side_offset, nested_sum(residual_cost_by_side_offset), 10)
    top_pair_qty_activation = top_items(pair_qty_by_activation, nested_sum(pair_qty_by_activation), 6)

    # Key gap: current tail attribution joins activation bucket and side/offset,
    # but not projected residual bucket. The activation summary shows many
    # admitted candidates already had projected residual >20%, yet we cannot
    # directly assign realized residual to that pre-action bucket.
    missing_legal_join_fields = []
    if not tail.get("residual_qty_sum_by_status_reason_projected_residual_bucket"):
        missing_legal_join_fields.append("residual_qty_sum_by_status_reason_projected_residual_bucket")
    if not tail.get("residual_cost_sum_by_status_reason_projected_residual_bucket"):
        missing_legal_join_fields.append("residual_cost_sum_by_status_reason_projected_residual_bucket")
    if not tail.get("residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket"):
        missing_legal_join_fields.append("residual_qty_sum_by_status_reason_side_offset_projected_residual_bucket")
    if not tail.get("pair_qty_sum_by_status_reason_projected_residual_bucket"):
        missing_legal_join_fields.append("pair_qty_sum_by_status_reason_projected_residual_bucket")
    if not tail.get("source_sequence_presence_by_status_reason_projected_residual_bucket"):
        missing_legal_join_fields.append("source_sequence_presence_by_status_reason_projected_residual_bucket")

    activation_gate_passed = (
        nested_get(acceptance_adapter, ["adapter_evaluation", "symmetric_activation_gate", "passed"]) is True
    )
    normal_shadow_gate_passed = (
        nested_get(acceptance_adapter, ["adapter_evaluation", "shadow_acceptance_gate", "passed"]) is True
    )
    scorer_ok = (
        activation_scorer.get("decision_label") == "KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_READY"
        and tail_scorer.get("decision_label") == "KEEP_SYMMETRIC_ACTIVATION_TAIL_ATTRIBUTION_SUMMARY_SCORER_READY"
    )

    concentration = {
        "top_residual_activation_share": top_residual_activation[0]["share"] if top_residual_activation else None,
        "top_residual_cost_activation_share": top_residual_cost_activation[0]["share"] if top_residual_cost_activation else None,
        "top_residual_side_offset_share": top_residual_side_offset[0]["share"] if top_residual_side_offset else None,
        "top_residual_cost_side_offset_share": top_residual_cost_side_offset[0]["share"] if top_residual_cost_side_offset else None,
        "admitted_projected_residual_gt20_count": round(gt20_projected_admitted, 6),
        "admitted_candidate_count": round(admitted_candidates, 6),
        "admitted_projected_residual_gt20_share": round(gt20_projected_admitted / admitted_candidates, 6)
        if admitted_candidates
        else None,
        "source_link_residual_cost_top": top_items(
            source_link.get("residual_cost_by_source_side_offset_risk_direction") or {},
            residual_cost,
            8,
        ),
        "source_link_residual_qty_top": top_items(
            source_link.get("residual_qty_by_source_side_offset_risk_direction") or {},
            residual_qty,
            8,
        ),
    }

    failure_profile = {
        "economic_pnl_positive": fee_adjusted_pair_pnl > 0,
        "residual_qty_share_failed": residual_qty_share > 0.15,
        "residual_cost_share_failed": residual_cost_share > 0.20,
        "normal_shadow_gate_passed": normal_shadow_gate_passed,
        "activation_gate_passed": activation_gate_passed,
        "activation_and_tail_scorers_keep": scorer_ok,
        "tail_summary_present": bool(tail),
        "source_link_present": bool(source_link),
    }

    legal_next_target = (
        failure_profile["economic_pnl_positive"]
        and failure_profile["residual_qty_share_failed"]
        and failure_profile["residual_cost_share_failed"]
        and activation_gate_passed
        and scorer_ok
        and bool(missing_legal_join_fields)
        and concentration["admitted_projected_residual_gt20_share"] is not None
        and concentration["admitted_projected_residual_gt20_share"] >= 0.25
        and concentration["top_residual_activation_share"] is not None
        and concentration["top_residual_activation_share"] >= 0.45
    )

    if not failure_profile["economic_pnl_positive"]:
        decision = "DISCARD"
        decision_label = "DISCARD_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_ECONOMIC_NEGATIVE"
    elif legal_next_target:
        decision = "KEEP"
        decision_label = "KEEP_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_PROJECTED_RESIDUAL_JOIN_TARGET_READY"
    elif not scorer_ok or not tail or not source_link:
        decision = "UNKNOWN"
        decision_label = "UNKNOWN_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_INPUTS_INSUFFICIENT"
    else:
        decision = "DISCARD"
        decision_label = "DISCARD_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_BROAD_OR_FORBIDDEN_ONLY"

    return {
        "decision": decision,
        "decision_label": decision_label,
        "shadow_metrics": {
            "candidates": as_float(metrics.get("candidates")),
            "queue_supported_fills": as_float(metrics.get("queue_supported_fills")),
            "pair_actions": as_float(metrics.get("pair_actions")),
            "pair_qty": as_float(metrics.get("pair_qty")),
            "pair_pnl": as_float(metrics.get("pair_pnl")),
            "fee_adjusted_pair_pnl_proxy": fee_adjusted_pair_pnl,
            "net_pair_cost_p90": as_float(metrics.get("net_pair_cost_p90")),
            "residual_qty": residual_qty,
            "residual_cost": residual_cost,
            "residual_qty_share": residual_qty_share,
            "residual_cost_share": residual_cost_share,
            "pair_tail_loss_share": as_float(metrics.get("pair_tail_loss_share_of_pair_pnl")),
            "material_residual_lots": as_float(metrics.get("material_residual_lots")),
            "risk_adjusted_pnl_proxy": as_float(metrics.get("conservative_risk_adjusted_pnl_proxy")),
        },
        "failure_profile": failure_profile,
        "legal_concentration": {
            "residual_qty_by_activation_top": top_residual_activation,
            "residual_cost_by_activation_top": top_residual_cost_activation,
            "residual_qty_by_side_offset_activation_top": top_residual_side_offset,
            "residual_cost_by_side_offset_activation_top": top_residual_cost_side_offset,
            "pair_qty_by_activation_top": top_pair_qty_activation,
            "projected_residual_bucket_by_activation": projected_residual_by_activation,
            "projected_pair_cost_bucket_by_activation": projected_pair_cost_by_activation,
            "source_sequence_by_activation": source_sequence_by_activation,
            **concentration,
        },
        "missing_legal_join_fields": missing_legal_join_fields,
        "recommended_local_target": {
            "name": "symmetric_activation_projected_residual_tail_join_summary_v1",
            "default_off": True,
            "behavior_change": False,
            "purpose": (
                "Join realized residual qty/cost and source_sequence coverage to pre-action "
                "projected_residual_rate_bucket and activation bucket, so the next review can "
                "tell whether residual failure comes from legally observable pre-action residual pressure."
            ),
            "required_fields": missing_legal_join_fields,
            "explicitly_not": [
                "not a YES/NO side cap",
                "not an offset cap",
                "not a public-price cap",
                "not realized pair_cost as live criteria",
                "not a shadow/canary/deploy approval",
            ],
        },
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    inputs = {
        "aggregate": args.aggregate,
        "shadow_acceptance": args.shadow_acceptance_manifest,
        "activation_scorer": args.symmetric_activation_scorer_manifest,
        "tail_scorer": args.tail_attribution_scorer_manifest,
        "acceptance_adapter": args.acceptance_adapter_manifest,
    }
    loaded: dict[str, dict[str, Any] | None] = {}
    load_errors: dict[str, str] = {}
    for name, path in inputs.items():
        obj, error = safe_load(path, root)
        loaded[name] = obj
        if error:
            load_errors[name] = error

    if load_errors:
        evaluation = {
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_SYMMETRIC_ACTIVATION_EDGE075_RESIDUAL_GAP_INPUTS_INSUFFICIENT",
            "load_errors": load_errors,
        }
    else:
        evaluation = evaluate(
            loaded["aggregate"] or {},
            loaded["shadow_acceptance"] or {},
            loaded["activation_scorer"] or {},
            loaded["tail_scorer"] or {},
            loaded["acceptance_adapter"] or {},
        )

    decision = evaluation["decision"]
    decision_label = evaluation["decision_label"]
    created = utc_label()
    output_dir = args.output_dir or Path("xuan_research_artifacts") / f"{ARTIFACT}_{created}"
    manifest = {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "audit_schema_version": SCHEMA_VERSION,
        "created_utc": created,
        "decision": decision,
        "decision_label": decision_label,
        "inputs": {name: str(path) for name, path in inputs.items()},
        "load_errors": load_errors,
        "audit_contract": audit_contract(),
        "evaluation": evaluation,
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "notes": [
                "This is local-only diagnosis of an UNKNOWN no-order result.",
                "Projected residual joins may nominate instrumentation/spec work, not live filters.",
                "research_ranking and promotion_gate remain separate.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "RESIDUAL_GAP_AUDIT_NOT_PROMOTION_EVIDENCE",
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
        "next_executable_action": (
            "Implement default-off symmetric_activation_projected_residual_tail_join_summary_v1 plus smoke/scorer; "
            "do not start another no-order diagnostic."
            if decision == "KEEP"
            else (
                "Discard edge0.075/window20 symmetric-activation parameter line and pivot locally; do not add forbidden caps or run another shadow."
                if decision == "DISCARD"
                else "Fix missing allowlisted scorer/aggregate inputs before continuing; do not run another shadow."
            )
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregate", type=Path, default=DEFAULT_DRIVER / "remote_clean/output/aggregate_report.json")
    parser.add_argument(
        "--shadow-acceptance-manifest",
        type=Path,
        default=DEFAULT_REVIEW / "shadow_acceptance/manifest.json",
    )
    parser.add_argument(
        "--symmetric-activation-scorer-manifest",
        type=Path,
        default=DEFAULT_REVIEW / "symmetric_activation_summary_scorer/manifest.json",
    )
    parser.add_argument(
        "--tail-attribution-scorer-manifest",
        type=Path,
        default=DEFAULT_REVIEW / "symmetric_activation_tail_attribution_scorer_v3/manifest.json",
    )
    parser.add_argument(
        "--acceptance-adapter-manifest",
        type=Path,
        default=DEFAULT_REVIEW / "symmetric_activation_acceptance_adapter_wrapped/manifest.json",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
