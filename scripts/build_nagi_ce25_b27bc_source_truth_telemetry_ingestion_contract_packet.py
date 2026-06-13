#!/usr/bin/env python3
"""Build a local-only source truth and telemetry ingestion contract packet.

This packet turns the current fail-closed maker-shadow research state into an
explicit data contract. It reads existing local decision artifacts only; it
does not fetch data, import credentials, connect to Polymarket, or execute
orders.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATUS = (
    "KEEP_NAGI_CE25_B27BC_SOURCE_TRUTH_TELEMETRY_INGESTION_CONTRACT_REVIEWED_"
    "PUBLIC_PROXY_EXHAUSTED_SOURCE_TRUTH_OR_OWN_TELEMETRY_REQUIRED_NOT_READY"
)
DEFAULT_PUBLIC_PROXY_DECISION = (
    "data/exports/nagi_ce25_b27bc_public_proxy_decision_packet_20260609T0113Z_public_fetch/"
    "decision_register.json"
)
DEFAULT_PREOPEN_STABILITY = (
    "data/exports/nagi_ce25_b27bc_preopen_candidate_stability_audit_20260609T0113Z/"
    "decision_register.json"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def non_claims_false(*items: dict[str, Any]) -> bool:
    for item in items:
        non_claims = item.get("non_claims") or {}
        if any(bool(value) for value in non_claims.values()):
            return False
    return True


def source_truth_schema() -> dict[str, Any]:
    return {
        "accepted_file_patterns": [
            "data/inputs/**/*maker_shadow_input*.csv",
            "data/inputs/**/*public_activity*.json",
            "data/inputs/**/*activity_trade_rows*.json",
            "data/inputs/**/*.events.jsonl",
            "evidence/**/*maker_shadow_input*.csv",
            "evidence/**/*public_activity*.json",
            "evidence/**/*activity_trade_rows*.json",
            "evidence/**/*.events.jsonl",
        ],
        "excluded_path_tokens": ["smoke", "fixture", "testdata", "__pycache__"],
        "required_fields_any_schema": [
            "market_slug or slug",
            "timestamp_ms or ts_ms or timestamp",
            "side or source_side or outcome",
            "price or public_trade_px or yes_bid/no_bid",
            "size or qty or public_trade_size",
            "remaining_s or market_end_ts/end_ts",
        ],
        "btc_5m_filters": {
            "market_slug_required": True,
            "btc_5m_required": True,
            "source_side_sell_is_public_sell_touch_proxy": True,
            "public_account_buy_rows_are_public_account_buy_proxy_only": True,
        },
        "truth_boundaries": [
            "public rows are queue/source proxies only",
            "public SELL touch does not prove our maker fill",
            "public account BUY rows do not prove maker execution",
            "L1/L2/source-truth rows must preserve timestamp, price, side, size, and market end timing",
        ],
    }


def telemetry_schema() -> dict[str, Any]:
    return {
        "accepted_file_patterns": [
            "data/inputs/**/*own*maker*telemetry*.csv",
            "data/inputs/**/*maker*fill*telemetry*.csv",
            "evidence/**/*own*maker*telemetry*.csv",
            "evidence/**/*maker*fill*telemetry*.csv",
        ],
        "required_fields": [
            "market_slug",
            "order_id",
            "client_order_id",
            "submitted_ts_ms",
            "decision_ts_ms",
            "side",
            "limit_price",
            "order_qty",
            "filled_qty",
            "fill_ts_ms",
            "maker_or_taker",
            "fee_rate",
            "fee_paid",
            "queue_proxy_open",
            "public_touch_seen",
            "pair_cost_at_decision",
            "pair_cost_realized",
            "residual_cost",
            "residual_rate",
        ],
        "pass_fail_requirements": {
            "maker_fee0_required": True,
            "taker_or_ambiguous_fills_excluded": True,
            "own_fill_conversion_required": True,
            "residual_cost_rate_reported": True,
            "pair_cost_reported": True,
            "queue_open_evidence_required": True,
            "minimum_sample_gate": {
                "own_maker_filled_markets": 100,
                "own_maker_filled_actions": 500,
            },
        },
    }


def replay_trigger_contract() -> dict[str, Any]:
    return {
        "new_source_truth_trigger": [
            "materialize_nagi_ce25_b27bc_maker_shadow_input.py",
            "inventory_nagi_ce25_b27bc_maker_shadow_inputs.py",
            "run_nagi_ce25_b27bc_maker_shadow.py",
            "build_nagi_ce25_b27bc_residual_failure_audit.py",
            "run_nagi_ce25_b27bc_residual_guard_grid.py",
            "build_nagi_ce25_b27bc_residual_model_v2_plan.py",
            "run_nagi_ce25_b27bc_pre_open_support_grid.py",
            "run_nagi_ce25_b27bc_per_market_residual_budget_grid.py",
            "run_nagi_ce25_b27bc_first_side_hazard_grid.py",
            "build_nagi_ce25_b27bc_public_proxy_exhaustion_decision_packet.py",
        ],
        "own_telemetry_trigger": [
            "build_or_run_own_maker_telemetry_validator_packet",
            "do_not_import_keys_or_call_apis",
            "do_not_execute_orders",
            "do_not_claim_private_truth_until validator gates pass",
        ],
        "file_drop_locations": {
            "source_truth_or_public_event_rows": ["data/inputs", "evidence"],
            "own_maker_telemetry_samples": ["data/inputs", "evidence"],
            "do_not_use_for_new_source_truth": ["logs", "xuan_research_artifacts smoke/fixture paths"],
        },
    }


def order_minimum_contract() -> dict[str, Any]:
    return {
        "limit_post_only_maker_min_shares": 5.0,
        "market_order_min_usdc": 1.0,
        "market_orders_used_by_this_pipeline": False,
        "insufficient_depth_rule": "public SELL touch or visible/proxy depth below 5 shares is insufficient_depth, not queue_proxy_open",
    }


def build_decision(args: argparse.Namespace) -> dict[str, Any]:
    public_proxy_path = Path(args.public_proxy_decision)
    preopen_path = Path(args.preopen_stability)
    public_proxy = read_json(public_proxy_path)
    preopen = read_json(preopen_path)
    if not non_claims_false(public_proxy, preopen):
        raise RuntimeError("canonical artifacts have non_claims set true")
    public_proxy_exhausted = bool(public_proxy.get("public_proxy_families_exhausted"))
    no_passing_preopen = int(preopen.get("passing_grid_row_count") or 0) == 0
    return {
        "generated_at": utc_now(),
        "status": STATUS,
        "evidence_level": "research_contract",
        "canonical_inputs": {
            "public_proxy_decision": str(public_proxy_path),
            "preopen_stability": str(preopen_path),
        },
        "current_blocked_state": {
            "public_proxy_families_exhausted": public_proxy_exhausted,
            "best_residual_rate": public_proxy.get("best_residual_rate"),
            "residual_target_miss_bps": public_proxy.get("residual_target_miss_bps"),
            "preopen_passing_grid_row_count": preopen.get("passing_grid_row_count"),
            "b27bc_direct_pooling_rejected": public_proxy_exhausted and no_passing_preopen,
            "reason": (
                "B27BC direct public-account pooling invalidated the prior narrow NAGI+CE25 pre-open pass; "
                "public proxy tuning is exhausted until new source-truth rows or own maker telemetry arrives."
            ),
        },
        "source_truth_input_schema": source_truth_schema(),
        "own_maker_telemetry_schema": telemetry_schema(),
        "order_minimum_contract": order_minimum_contract(),
        "replay_trigger_contract": replay_trigger_contract(),
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "next_executable_action": "build_local_only_own_maker_telemetry_validator_packet_with_fixtures",
    }


def write_report(path: Path, decision: dict[str, Any]) -> None:
    source = decision["source_truth_input_schema"]
    telemetry = decision["own_maker_telemetry_schema"]
    trigger = decision["replay_trigger_contract"]
    order_minimum = decision["order_minimum_contract"]
    blocked = decision["current_blocked_state"]
    lines = [
        "# NAGI + CE25 + B27BC Source Truth and Telemetry Ingestion Contract",
        "",
        f"Generated at: `{decision['generated_at']}`",
        "",
        "## Current Decision",
        "",
        f"- Status: `{decision['status']}`",
        f"- Public proxy families exhausted: `{blocked['public_proxy_families_exhausted']}`",
        f"- B27BC direct pooling rejected: `{blocked['b27bc_direct_pooling_rejected']}`",
        f"- Best residual rate: `{blocked['best_residual_rate']}`",
        f"- Residual target miss bps: `{blocked['residual_target_miss_bps']}`",
        f"- Reason: {blocked['reason']}",
        "",
        "## Accepted Source Truth Inputs",
        "",
        "Drop new non-smoke/non-fixture source truth or public event rows into:",
    ]
    for item in trigger["file_drop_locations"]["source_truth_or_public_event_rows"]:
        lines.append(f"- `{item}`")
    lines.extend(["", "Required source truth fields:"])
    for field in source["required_fields_any_schema"]:
        lines.append(f"- `{field}`")
    lines.extend(["", "Accepted source truth file patterns:"])
    for pattern in source["accepted_file_patterns"]:
        lines.append(f"- `{pattern}`")
    lines.extend(["", "## Accepted Own Maker Telemetry Inputs", "", "Drop own maker telemetry samples into:"])
    for item in trigger["file_drop_locations"]["own_maker_telemetry_samples"]:
        lines.append(f"- `{item}`")
    lines.extend(["", "Required telemetry fields:"])
    for field in telemetry["required_fields"]:
        lines.append(f"- `{field}`")
    lines.extend(
        [
            "",
            "## Replay Trigger",
            "",
            "When a new source-truth input appears, run:",
        ]
    )
    for step in trigger["new_source_truth_trigger"]:
        lines.append(f"- `{step}`")
    lines.extend(
        [
            "",
            "## Order Minimum Guards",
            "",
            f"- Limit/post-only maker minimum shares: `{order_minimum['limit_post_only_maker_min_shares']}`",
            f"- Market order minimum USDC: `{order_minimum['market_order_min_usdc']}`",
            f"- Market orders used by this pipeline: `{order_minimum['market_orders_used_by_this_pipeline']}`",
            f"- Insufficient depth rule: {order_minimum['insufficient_depth_rule']}",
            "",
            "## Non-Claims",
            "",
            "This contract is local research only. It does not prove maker fill truth, private queue priority, OOS readiness, canary readiness, or live readiness.",
            "",
            "## Next Executable Action",
            "",
            f"- `{decision['next_executable_action']}`",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--public-proxy-decision", default=DEFAULT_PUBLIC_PROXY_DECISION)
    p.add_argument("--preopen-stability", default=DEFAULT_PREOPEN_STABILITY)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    decision = build_decision(args)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_json(out / "decision_register.json", decision)
    write_report(out / "SOURCE_TRUTH_TELEMETRY_INGESTION_CONTRACT_REPORT.md", decision)
    print(
        json.dumps(
            {
                "status": decision["status"],
                "output_dir": str(out),
                "public_proxy_families_exhausted": decision["current_blocked_state"][
                    "public_proxy_families_exhausted"
                ],
                "next_executable_action": decision["next_executable_action"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
