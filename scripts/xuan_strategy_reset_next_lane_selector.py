#!/usr/bin/env python3
"""Select the next research lane after public-profile input blocking.

This local-only selector makes the discard/freeze decision explicit for the
public-profile mimicability line, then selects a new lane only from current
worktree manifests that already contain multi-day covered+holdout evidence.
It does not fetch data, start services, use SSH/shadow/live, or scan forbidden
stores.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_strategy_reset_next_lane_selector"
CONTRACT_NAME = "xuan_strategy_reset_next_lane_selector_v1"
DEFAULT_PUBLIC_HANDOFF = Path(
    "xuan_research_artifacts/"
    "xuan_public_profile_input_blocked_handoff_20260524T125716Z/"
    "manifest.json"
)
DEFAULT_PRIMARY_SYMMETRIC = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_residual_stress_strict_count_boundary_20260519T1431Z/"
    "manifest.json"
)
DEFAULT_SUPPORT_SYMMETRIC = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_activation_residual_stress_20260519T1231Z/"
    "manifest.json"
)
DEFAULT_BASE_RESIDUAL = Path(
    "xuan_research_artifacts/"
    "xuan_symmetric_admission_residual_stress_w10_20260519T1128Z/"
    "manifest.json"
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
MIN_COVERED_PAIR_ACTIONS = 1000
MIN_HOLDOUT_PAIR_ACTIONS = 200
MAX_COVERED_RESIDUAL_QTY_RATE = 0.05
MAX_HOLDOUT_RESIDUAL_QTY_RATE = 0.05


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
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


def safe_load_manifest(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
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


def public_profile_disposition(handoff: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    if error:
        return {
            "status": "UNKNOWN_PUBLIC_PROFILE_HANDOFF_UNAVAILABLE",
            "decision": "UNKNOWN",
            "blockers": [error],
            "discard_or_freeze": False,
        }
    label = str(handoff.get("decision_label") or "")
    ranking = handoff.get("research_ranking") or {}
    blocked = (
        label == "UNKNOWN_PUBLIC_PROFILE_INPUT_BLOCKED_SAFE_72H_SOURCE_REQUIRED"
        or ranking.get("status") == "UNKNOWN_PUBLIC_PROFILE_INPUT_BLOCKED_SAFE_72H_SOURCE_REQUIRED"
    )
    if blocked:
        return {
            "status": "DISCARD_PUBLIC_PROFILE_MIMICABILITY_INPUT_BLOCKED",
            "decision": "DISCARD",
            "blockers": list(ranking.get("exact_blockers") or []),
            "discard_or_freeze": True,
            "reason": (
                "User requested pivot instead of waiting; current worktree has no "
                "safe 72h public-profile manifest, so this line is frozen and not "
                "eligible for automatic no-order diagnostics."
            ),
        }
    return {
        "status": "UNKNOWN_PUBLIC_PROFILE_HANDOFF_NOT_BLOCKED",
        "decision": "UNKNOWN",
        "blockers": ["public_profile_handoff_not_in_expected_input_blocked_state"],
        "discard_or_freeze": False,
    }


def robust_row_check(row: dict[str, Any]) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    if as_int(row.get("covered_pair_actions")) < MIN_COVERED_PAIR_ACTIONS:
        blockers.append("covered_pair_actions_below_min")
    if as_int(row.get("holdout_pair_actions")) < MIN_HOLDOUT_PAIR_ACTIONS:
        blockers.append("holdout_pair_actions_below_min")
    if as_float(row.get("covered_residual_qty_rate"), 1.0) > MAX_COVERED_RESIDUAL_QTY_RATE:
        blockers.append("covered_residual_qty_rate_above_max")
    if as_float(row.get("holdout_residual_qty_rate"), 1.0) > MAX_HOLDOUT_RESIDUAL_QTY_RATE:
        blockers.append("holdout_residual_qty_rate_above_max")
    if as_float(row.get("covered_worst_day")) <= 0.0:
        blockers.append("covered_worst_day_not_positive")
    if as_float(row.get("holdout_worst_day")) <= 0.0:
        blockers.append("holdout_worst_day_not_positive")
    if as_float(row.get("covered_worst_net_fee_after")) <= 0.0:
        blockers.append("covered_worst_net_fee_after_not_positive")
    if as_float(row.get("holdout_worst_net_fee_after")) <= 0.0:
        blockers.append("holdout_worst_net_fee_after_not_positive")
    return not blockers, blockers


def row_score(row: dict[str, Any]) -> tuple[float, float, int, float]:
    return (
        as_float(row.get("holdout_worst_net_fee_after")),
        as_float(row.get("holdout_worst_day")),
        as_int(row.get("holdout_pair_actions")),
        -as_float(row.get("holdout_residual_qty_rate"), 1.0),
    )


def normalize_candidate(source_name: str, source_path: Path, row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_name": source_name,
        "source_manifest": str(source_path),
        "edge": as_float(row.get("edge")),
        "leak_rate": as_float(row.get("leak_rate")),
        "activation_window_s": as_float(row.get("activation_window_s")),
        "min_opp_count": as_int(row.get("min_opp_count")),
        "covered_pair_actions": as_int(row.get("covered_pair_actions")),
        "holdout_pair_actions": as_int(row.get("holdout_pair_actions")),
        "covered_residual_qty_rate": as_float(row.get("covered_residual_qty_rate")),
        "holdout_residual_qty_rate": as_float(row.get("holdout_residual_qty_rate")),
        "covered_worst_day": as_float(row.get("covered_worst_day")),
        "holdout_worst_day": as_float(row.get("holdout_worst_day")),
        "covered_worst_net_fee_after": as_float(row.get("covered_worst_net_fee_after")),
        "holdout_worst_net_fee_after": as_float(row.get("holdout_worst_net_fee_after")),
        "selection_score": list(row_score(row)),
    }


def summarize_symmetric_manifest(
    source_name: str,
    source_path: Path,
    manifest: dict[str, Any] | None,
    error: str | None,
) -> dict[str, Any]:
    if error:
        return {
            "source_name": source_name,
            "source_manifest": str(source_path),
            "status": "UNAVAILABLE",
            "blockers": [error],
            "qualified_count": 0,
            "robust_candidate_count": 0,
            "robust_candidates": [],
        }
    qualified = manifest.get("qualified")
    qualified_rows = [row for row in qualified if isinstance(row, dict)] if isinstance(qualified, list) else []
    robust_candidates: list[dict[str, Any]] = []
    rejected_count = 0
    for row in qualified_rows:
        ok, _blockers = robust_row_check(row)
        if ok:
            robust_candidates.append(normalize_candidate(source_name, source_path, row))
        else:
            rejected_count += 1
    robust_candidates.sort(key=lambda item: tuple(item["selection_score"]), reverse=True)
    data_root = str(manifest.get("data_root") or "")
    return {
        "source_name": source_name,
        "source_manifest": str(source_path),
        "status": str(manifest.get("status") or manifest.get("decision_label") or "UNKNOWN"),
        "artifact": manifest.get("artifact"),
        "dataset_type": manifest.get("dataset_type"),
        "assets": manifest.get("assets"),
        "market_prefix": manifest.get("market_prefix"),
        "covered_days": ((manifest.get("days") or {}).get("covered") or []),
        "holdout_days": ((manifest.get("days") or {}).get("holdout") or []),
        "qualified_count": len(qualified_rows),
        "robust_candidate_count": len(robust_candidates),
        "rejected_qualified_count": rejected_count,
        "manifest_declares_external_data_root": bool(data_root and not data_root.startswith(str(Path.cwd()))),
        "external_data_root_not_read": True,
        "robust_candidates": robust_candidates[:10],
    }


def select_candidate(primary: dict[str, Any], support: dict[str, Any]) -> dict[str, Any] | None:
    primary_candidates = primary.get("robust_candidates") or []
    if primary_candidates:
        return primary_candidates[0]
    support_candidates = support.get("robust_candidates") or []
    if support_candidates:
        candidate = dict(support_candidates[0])
        candidate["source_note"] = "selected_from_support_manifest_because_primary_had_no_robust_candidate"
        return candidate
    return None


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    public_handoff, public_error = safe_load_manifest(args.public_profile_handoff, root)
    primary_manifest, primary_error = safe_load_manifest(args.primary_symmetric_manifest, root)
    support_manifest, support_error = safe_load_manifest(args.support_symmetric_manifest, root)
    base_manifest, base_error = safe_load_manifest(args.base_residual_manifest, root)

    public_status = public_profile_disposition(public_handoff, public_error)
    primary_summary = summarize_symmetric_manifest(
        "strict_count_boundary_activation_residual_stress",
        args.primary_symmetric_manifest,
        primary_manifest,
        primary_error,
    )
    support_summary = summarize_symmetric_manifest(
        "activation_residual_stress_low_leak_support",
        args.support_symmetric_manifest,
        support_manifest,
        support_error,
    )
    base_summary = summarize_symmetric_manifest(
        "base_symmetric_admission_residual_stress_control",
        args.base_residual_manifest,
        base_manifest,
        base_error,
    )
    selected = select_candidate(primary_summary, support_summary)

    blockers: list[str] = []
    if not public_status.get("discard_or_freeze"):
        blockers.append("public_profile_line_not_confirmed_frozen")
    if selected is None:
        blockers.append("no_robust_symmetric_activation_candidate")

    decision = "KEEP" if not blockers else "UNKNOWN"
    decision_label = (
        "KEEP_SYMMETRIC_ACTIVATION_NEXT_STRATEGY_SELECTED_RESEARCH_ONLY"
        if decision == "KEEP"
        else "UNKNOWN_STRATEGY_RESET_NEXT_LANE_SELECTOR_INSUFFICIENT"
    )
    next_action = (
        "Implement symmetric_activation_contract_v1 local-only: define default-off "
        "pre-action fields for opposite-side strict-flow activation, projected pair "
        "cost, residual/leak stress, and summary/scorer fail-closed checks; do not "
        "start shadow/no-order diagnostics."
        if decision == "KEEP"
        else "Provide a current-worktree multi-day manifest with robust covered+holdout "
        "evidence, or keep the research line paused."
    )

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "contract_name": CONTRACT_NAME,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "lane": "strategy_reset_next_lane_selector",
        "public_profile_disposition": public_status,
        "selected_strategy_line": {
            "name": "symmetric_activation_residual_stress",
            "selected": selected,
            "why_new": [
                "uses current-worktree multi-day covered+holdout strict-cache evidence",
                "does not depend on public-profile 72h manifests",
                "does not reuse micro-deficit/ledger/tiny-deficit/closed-cycle target-cooldown families",
                "uses pre-action opposite-side strict-flow activation rather than realized pair_cost as live criterion",
            ],
            "not_claimed": [
                "not private truth",
                "not deployable",
                "not canary-ready",
                "not promotion evidence",
                "not an approval to start no-order diagnostics",
            ],
        },
        "source_summaries": {
            "primary": primary_summary,
            "support": support_summary,
            "base_residual_control": base_summary,
        },
        "selection_thresholds": {
            "min_covered_pair_actions": MIN_COVERED_PAIR_ACTIONS,
            "min_holdout_pair_actions": MIN_HOLDOUT_PAIR_ACTIONS,
            "max_covered_residual_qty_rate": MAX_COVERED_RESIDUAL_QTY_RATE,
            "max_holdout_residual_qty_rate": MAX_HOLDOUT_RESIDUAL_QTY_RATE,
            "require_positive_covered_worst_day": True,
            "require_positive_holdout_worst_day": True,
            "require_positive_covered_worst_net_fee_after": True,
            "require_positive_holdout_worst_net_fee_after": True,
        },
        "blockers": blockers,
        "next_executable_action": next_action,
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "public_profile_line_discarded_or_frozen": bool(public_status.get("discard_or_freeze")),
            "selected_line": "symmetric_activation_residual_stress" if selected is not None else None,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "RESEARCH_ONLY_NEXT_LANE_SELECTION_NOT_PROMOTION_EVIDENCE",
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
    parser.add_argument("--public-profile-handoff", type=Path, default=DEFAULT_PUBLIC_HANDOFF)
    parser.add_argument("--primary-symmetric-manifest", type=Path, default=DEFAULT_PRIMARY_SYMMETRIC)
    parser.add_argument("--support-symmetric-manifest", type=Path, default=DEFAULT_SUPPORT_SYMMETRIC)
    parser.add_argument("--base-residual-manifest", type=Path, default=DEFAULT_BASE_RESIDUAL)
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
