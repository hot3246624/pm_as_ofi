#!/usr/bin/env python3
"""Specify the symmetric activation residual-stress contract.

This local-only contract converts the selected symmetric activation research
lane into an implementable, default-off diagnostics target. It inspects only
current-worktree manifests/source files and fails closed if the selected
pre-action activation fields cannot be represented safely.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_symmetric_activation_contract"
CONTRACT_NAME = "symmetric_activation_contract_v1"
DEFAULT_SELECTOR = Path(
    "xuan_research_artifacts/"
    "xuan_strategy_reset_next_lane_selector_20260524T162937Z/"
    "manifest.json"
)
DEFAULT_RUNNER = Path("tools/xuan_dplus_passive_passive_shadow_runner.py")
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
RUNNER_REQUIRED_TOKENS = {
    "activation_mode_config": "activation_mode: str",
    "activation_window_config": "activation_window_s",
    "opp_seen_mode": "opp_seen",
    "activation_state": "activation_last_seen_ms",
    "activation_check": "activation_allows_seed",
    "activation_age_field": "activation_opp_age_ms",
    "opposite_trigger_field": "opposite_trigger_ts_ms",
    "pre_seed_same_qty": "pre_seed_same_qty",
    "pre_seed_opp_qty": "pre_seed_opp_qty",
    "pre_seed_same_cost": "pre_seed_same_cost",
    "pre_seed_opp_cost": "pre_seed_opp_cost",
    "source_sequence_field": "source_sequence_id",
}


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


def safe_read_text(path: Path, root: Path) -> tuple[str, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return "", reason
    try:
        return path.read_text(), None
    except FileNotFoundError:
        return "", "missing"
    except Exception as exc:
        return "", f"{type(exc).__name__}: {exc}"


def selected_params(selector: dict[str, Any] | None) -> dict[str, Any] | None:
    if not selector:
        return None
    selected = ((selector.get("selected_strategy_line") or {}).get("selected") or None)
    return selected if isinstance(selected, dict) else None


def inspect_runner(source_text: str, error: str | None, min_opp_count: int) -> dict[str, Any]:
    if error:
        return {
            "ready": False,
            "blockers": [error],
            "token_checks": {},
            "supports_min_opp_count": False,
        }
    token_checks = {name: token in source_text for name, token in RUNNER_REQUIRED_TOKENS.items()}
    blockers = [f"missing_runner_token_{name}" for name, ok in token_checks.items() if not ok]
    supports_min_opp_count = min_opp_count <= 1
    if not supports_min_opp_count:
        blockers.append("runner_only_proves_opp_seen_boolean_not_min_opp_count_gt_1")
    return {
        "ready": not blockers,
        "blockers": blockers,
        "token_checks": token_checks,
        "supports_min_opp_count": supports_min_opp_count,
        "current_runner_support_note": (
            "Runner has activation_mode=opp_seen, activation_window_s, last opposite seen timestamp, "
            "activation_opp_age_ms, opposite_trigger_ts_ms, pre-seed same/opp qty/cost, and source_sequence_id. "
            "The selected research lane uses min_opp_count=1, so boolean opposite-seen support is sufficient for a "
            "default-off diagnostics contract."
        ),
    }


def contract_fields(selected: dict[str, Any]) -> dict[str, Any]:
    activation_window = as_float(selected.get("activation_window_s"))
    edge = as_float(selected.get("edge"))
    leak_rate = as_float(selected.get("leak_rate"))
    min_opp_count = as_int(selected.get("min_opp_count"))
    return {
        "contract_name": CONTRACT_NAME,
        "selected_parameters": {
            "edge": edge,
            "activation_mode": "opp_seen",
            "activation_window_s": activation_window,
            "min_opp_count": min_opp_count,
            "leak_rate": leak_rate,
            "asset": "BTC",
            "market_prefix": "btc-updown-5m",
        },
        "default_off_cli": {
            "future_summary_flag": "--symmetric-activation-event-lite-summary",
            "required_existing_flags": ["--event-lite-summary", "--activation-mode opp_seen"],
            "required_parameter_binding": {
                "--edge": edge,
                "--activation-window-s": activation_window,
            },
            "not_a_trading_rule": True,
        },
        "required_pre_action_fields": [
            "condition_id",
            "market_slug",
            "side",
            "ts_ms",
            "offset_s",
            "source_sequence_id",
            "public_trade_px",
            "public_trade_size",
            "seed_px",
            "candidate_qty",
            "pre_seed_same_qty",
            "pre_seed_opp_qty",
            "pre_seed_same_cost",
            "pre_seed_opp_cost",
            "risk_increasing_seed",
            "activation_required",
            "activation_mode",
            "activation_window_s",
            "activation_opposite_seen",
            "activation_opp_age_ms",
            "opposite_trigger_ts_ms",
        ],
        "derived_diagnostics": [
            "projected_pair_qty",
            "projected_pair_cost",
            "projected_residual_qty",
            "projected_residual_rate_on_bought_qty",
            "residual_leak_stress_cost",
            "activation_bucket",
            "activation_opp_age_bucket",
            "projected_pair_cost_bucket",
            "projected_residual_rate_bucket",
            "source_sequence_presence",
        ],
        "summary_schema": {
            "schema_version": "symmetric_activation_contract_summary_v1",
            "counts": [
                "candidate_count_by_status_reason_activation_bucket",
                "candidate_count_by_status_reason_side_offset_activation_bucket",
                "projected_pair_cost_bucket_by_status_reason_activation_bucket",
                "projected_residual_rate_bucket_by_status_reason_activation_bucket",
                "source_sequence_presence_by_status_reason_activation_bucket",
            ],
            "field_contract": {
                "post_action_outcome_labels_included": False,
                "realized_pair_cost_used_as_live_criteria": False,
                "trading_behavior_changed": False,
                "private_truth_ready": False,
                "deployable": False,
                "promotion_gate_passed": False,
            },
        },
        "fail_closed_conditions": [
            "selected strategy line missing",
            "selected min_opp_count greater than runner-supported opposite-seen boolean",
            "activation_mode is not opp_seen",
            "activation_window_s is absent or mismatched",
            "pre_seed same/opp qty or cost fields are missing",
            "activation_opp_age_ms or opposite_trigger_ts_ms is missing",
            "source_sequence_id coverage is unavailable",
            "summary flag changes trading behavior",
            "future/post-action labels are used as live criteria",
            "private truth/deployable/promotion is claimed",
        ],
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    selector, selector_error = safe_load_manifest(args.selector_manifest, root)
    runner_text, runner_error = safe_read_text(args.runner_source, root)
    selected = selected_params(selector)
    blockers: list[str] = []
    if selector_error:
        blockers.append(f"selector_manifest_{selector_error}")
    if not selected:
        blockers.append("selected_strategy_line_missing")
        selected = {}
    if selector and selector.get("decision") != "KEEP":
        blockers.append("selector_decision_not_keep")
    if ((selector or {}).get("research_ranking") or {}).get("selected_line") != "symmetric_activation_residual_stress":
        blockers.append("selector_did_not_select_symmetric_activation")

    min_opp_count = as_int(selected.get("min_opp_count"))
    runner_status = inspect_runner(runner_text, runner_error, min_opp_count)
    blockers.extend(runner_status["blockers"])
    contract = contract_fields(selected)

    decision = "KEEP" if not blockers else "UNKNOWN"
    decision_label = (
        "KEEP_SYMMETRIC_ACTIVATION_CONTRACT_READY"
        if decision == "KEEP"
        else "UNKNOWN_SYMMETRIC_ACTIVATION_CONTRACT_INPUTS_INSUFFICIENT"
    )
    next_action = (
        "Implement default-off --symmetric-activation-event-lite-summary in the runner, "
        "using only pre-action activation/opposite-seen and inventory fields, plus a smoke "
        "that proves default-off absence, enabled summary presence, aggregate parity, and "
        "field_contract false for private/deployable/promotion."
        if decision == "KEEP"
        else "Name exact missing runner/source fields before any runner instrumentation or no-order diagnostic."
    )

    return {
        "artifact": ARTIFACT,
        "schema_version": 1,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": decision_label,
        "lane": "symmetric_activation_contract",
        "selector_manifest": str(args.selector_manifest),
        "runner_source": str(args.runner_source),
        "selected_research_parameters": selected,
        "contract": contract,
        "runner_implementability": runner_status,
        "blockers": blockers,
        "next_executable_action": next_action,
        "research_ranking": {
            "status": decision_label,
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "selected_line": "symmetric_activation_residual_stress" if decision == "KEEP" else None,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "CONTRACT_READY_NOT_PROMOTION_EVIDENCE",
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
    parser.add_argument("--selector-manifest", type=Path, default=DEFAULT_SELECTOR)
    parser.add_argument("--runner-source", type=Path, default=DEFAULT_RUNNER)
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
