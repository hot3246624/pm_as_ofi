#!/usr/bin/env python3
"""Emit the runner_owner_truth_v1 private validation contract.

This local-only contract records the boundary agreed for future owner execution:
historical shadow/no-order runs cannot be backfilled into owner-private truth,
and runner owner truth belongs in the private validation/scoring layer, not in
search-safe/search. It reads only local scorecards and source-file presence.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open() as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def status(card: dict[str, Any]) -> str | None:
    raw = card.get("status")
    return str(raw) if raw else None


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def file_summary(paths: list[str]) -> list[dict[str, Any]]:
    out = []
    for raw in paths:
        path = Path(raw)
        out.append(
            {
                "path": raw,
                "exists": path.exists(),
                "bytes": path.stat().st_size if path.exists() and path.is_file() else None,
            }
        )
    return out


def contract_schema() -> dict[str, Any]:
    return {
        "owner_order_events": {
            "required": True,
            "source": "authenticated_owner_order_lifecycle",
            "minimum_fields": [
                "owner_account_or_wallet",
                "source_row_hash",
                "order_id",
                "client_order_id",
                "condition_id",
                "market_id",
                "slug",
                "side",
                "action",
                "order_type",
                "limit_price",
                "original_qty",
                "remaining_qty",
                "status",
                "created_ts_ms",
                "updated_ts_ms",
            ],
        },
        "owner_fill_events": {
            "required": True,
            "source": "authenticated_owner_fill_stream_or_statement",
            "minimum_fields": [
                "owner_account_or_wallet",
                "source_row_hash",
                "order_id",
                "trade_id",
                "condition_id",
                "market_id",
                "slug",
                "side",
                "liquidity_role",
                "fill_ts_ms",
                "fill_price",
                "fill_qty",
                "actual_fee",
                "fee_params_source_hash",
                "cash_delta",
                "token_delta",
            ],
        },
        "owner_inventory_snapshots_or_deltas": {
            "required": True,
            "source": "authenticated_owner_position_snapshot_or_delta",
            "minimum_fields": [
                "owner_account_or_wallet",
                "source_row_hash",
                "condition_id",
                "market_id",
                "slug",
                "event_ts_ms",
                "yes_qty_before",
                "no_qty_before",
                "yes_cost_before",
                "no_cost_before",
                "yes_qty_after",
                "no_qty_after",
                "yes_cost_after",
                "no_cost_after",
                "source_order_id",
                "source_trade_id",
            ],
        },
        "owner_redeem_or_settlement_events": {
            "required": True,
            "source": "authenticated_owner_redeem_or_settlement_statement",
            "minimum_fields": [
                "owner_account_or_wallet",
                "source_row_hash",
                "condition_id",
                "market_id",
                "slug",
                "event_ts_ms",
                "redeem_qty",
                "redeem_cash",
                "settled_side",
                "tx_hash_or_statement_id",
            ],
        },
        "market_fee_params": {
            "required": True,
            "source": "market_fee_params_collected_at_or_before_fill",
            "minimum_fields": [
                "condition_id",
                "market_id",
                "slug",
                "fee_rate",
                "fee_formula",
                "params_ts_ms",
                "source_row_hash",
            ],
        },
    }


def acceptance_criteria() -> list[str]:
    return [
        "Every owner order/fill/inventory/redeem row has an authenticated owner source row hash.",
        "Every owner fill links to an owner order lifecycle row or is explicitly blocked as unmatched.",
        "Inventory ledger reconciles with owner position snapshots/deltas.",
        "Residual lots are derived from owner inventory ledger, not public/proxy rows.",
        "Fee uses actual fill fee when present; otherwise collected market fee params must recompute the fee.",
        "After-fee PnL reconciliation covers orders, fills, inventory, redeems/settlements, fees, and residual lots.",
        "private_truth_ready can be true only after order/fill/inventory/redeem/PnL all reconcile.",
        "Historical shadow/no-order runs remain replay/source-truth, public/proxy evidence, and shadow intent ledger only.",
    ]


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    lines = [
        "# runner_owner_truth_v1 Contract",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Boundary",
        "",
        f"- layer: `{card['layer']}`",
        f"- enters_search_safe_or_search: `{card['enters_search_safe_or_search']}`",
        f"- historical_shadow_backfill_required: `{decision['historical_shadow_private_truth_backfill_required']}`",
        f"- private_truth_ready: `{decision['private_truth_ready']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        "",
        "## Acceptance Criteria",
        "",
    ]
    lines.extend(f"- {item}" for item in card["acceptance_criteria"])
    lines.extend(["", "## Required Streams", ""])
    for name, spec in card["schema"].items():
        lines.extend([f"### {name}", ""])
        lines.append(f"- source: `{spec['source']}`")
        lines.append(f"- required: `{spec['required']}`")
        lines.append("- minimum_fields:")
        lines.extend(f"  - `{field}`" for field in spec["minimum_fields"])
        lines.append("")
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    future_request = load_json(args.future_owner_truth_request_scorecard)
    replay = load_json(args.replay_source_truth_scorecard)
    request_decision = body(future_request, "decision")
    truth_boundary = body(future_request, "current_truth_boundary")
    source_files = file_summary(
        [
            "scripts/build_replay_db.py",
            "scripts/xuan_replay_store_v2_source_quality_market_budget_rescue_validator.py",
            "src/polymarket/recorder.rs",
        ]
    )
    hard_blockers: list[str] = []
    if not (status(future_request) or "").startswith("KEEP"):
        hard_blockers.append("future_owner_truth_request_not_keep")
    if truth_boundary.get("historical_shadow_owner_truth_status") != "not_applicable_no_owner_execution":
        hard_blockers.append("historical_shadow_boundary_not_explicit")
    if request_decision.get("historical_shadow_private_truth_backfill_required") is not False:
        hard_blockers.append("historical_shadow_backfill_not_explicitly_false")
    if not all(item["exists"] for item in source_files):
        hard_blockers.append("expected_existing_truth_source_file_missing")
    card_status = (
        "KEEP_RUNNER_OWNER_TRUTH_V1_CONTRACT_READY_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_RUNNER_OWNER_TRUTH_V1_CONTRACT_BLOCKED_LOCAL_ONLY"
    )
    return {
        "artifact": "xuan_runner_owner_truth_v1_contract",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_runner_owner_truth_v1_contract.py",
        "status": card_status,
        "layer": "private_validation_scoring",
        "enters_search_safe_or_search": False,
        "inputs": {
            "future_owner_truth_request_scorecard": args.future_owner_truth_request_scorecard,
            "replay_source_truth_scorecard": args.replay_source_truth_scorecard,
        },
        "source_statuses": {
            "future_owner_truth_request": status(future_request),
            "replay_source_truth": status(replay),
        },
        "existing_repo_support": {
            "source_files": source_files,
            "known_tables_or_logic": [
                "own_order_events",
                "own_fill_events",
                "own_inventory_events",
                "settlement_records",
                "user_truth normalize/auth logic",
            ],
            "collector_feasible": True,
            "historical_shadow_owner_fills_exist": False,
        },
        "decision": {
            "contract_ready": not hard_blockers,
            "future_owner_truth_pipeline_feasible": True,
            "private_truth_ready": False,
            "private_truth_ready_requires_future_owner_execution": True,
            "historical_shadow_private_truth_backfill_required": False,
            "historical_shadow_private_truth_backfill_possible": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "hard_blockers": hard_blockers,
            "next_action": "implement_or_review_runner_owner_truth_v1_schema_collector_in_private_validation_layer",
        },
        "schema": contract_schema(),
        "acceptance_criteria": acceptance_criteria(),
        "non_goals": [
            "No historical shadow owner fill backfill.",
            "No search-safe/search layer dependency.",
            "No live order authorization.",
            "No deployment approval.",
            "No public/proxy evidence counted as owner-private truth.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--future-owner-truth-request-scorecard", required=True)
    parser.add_argument("--replay-source-truth-scorecard", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    scorecard = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown(scorecard) + "\n")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if not scorecard["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
