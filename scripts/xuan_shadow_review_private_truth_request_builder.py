#!/usr/bin/env python3
"""Build an owner-scoped future private-truth pipeline request.

This local-only helper converts the remaining private_truth_not_ready caveat
into a concrete future-execution data request and pass/fail contract. Historical
shadow/no-order runs do not have owner orders, fills, inventory deltas, redeem
events, or actual charged fees, so they cannot be backfilled into owner-private
truth. They remain replay/source-truth and shadow-intent evidence only.

The request produced here is for the future owner execution pipeline. It reads
only local scorecards and aggregate metrics. It does not scan raw/replay stores,
launch remote jobs, approve deploy, or approve live orders.
"""

from __future__ import annotations

import argparse
import json
import math
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


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def listify(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def status(card: dict[str, Any]) -> str | None:
    raw = card.get("status")
    return str(raw) if raw else None


def is_keep(card: dict[str, Any]) -> bool:
    raw_status = status(card) or ""
    return raw_status.startswith("KEEP")


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def bridge_windows(surplus_bridge: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for raw in listify(surplus_bridge.get("main_windows")):
        if not isinstance(raw, dict):
            continue
        sample = body(raw, "sample")
        economics = body(raw, "economics")
        source_age = body(raw, "source_age")
        rescue_policy = body(raw, "rescue_policy")
        out.append(
            {
                "instance_id": raw.get("instance_id"),
                "role": raw.get("role"),
                "evidence_class": raw.get("evidence_class"),
                "bridge_eligible": raw.get("bridge_eligible"),
                "status": raw.get("status"),
                "sample": {
                    "accepted_actions": sample.get("accepted_actions"),
                    "queue_supported_fills": sample.get("queue_supported_fills"),
                    "strict_rescue_closes": sample.get("strict_rescue_closes"),
                    "strict_rescue_qty": sample.get("strict_rescue_qty"),
                    "filled_qty": sample.get("filled_qty"),
                    "filled_cost": sample.get("filled_cost"),
                },
                "economics": {
                    "pair_pnl": economics.get("pair_pnl"),
                    "roi_on_filled_cost": economics.get("roi_on_filled_cost"),
                    "residual_qty_share": economics.get("residual_qty_share"),
                    "residual_cost_share": economics.get("residual_cost_share"),
                    "residual_qty": economics.get("residual_qty"),
                    "residual_cost": economics.get("residual_cost"),
                },
                "source_age": {
                    "accepted_l1_age_ms_max": source_age.get("accepted_l1_age_ms_max"),
                    "rescue_l1_age_ms_max": source_age.get("rescue_l1_age_ms_max"),
                },
                "rescue_policy": {
                    "target_rescue_net_cap": rescue_policy.get("target_rescue_net_cap"),
                    "bridge_surplus_net_cap": rescue_policy.get("bridge_surplus_net_cap"),
                    "net_pair_cost_max": rescue_policy.get("net_pair_cost_max"),
                    "surplus_net_pair_cost_max": rescue_policy.get("surplus_net_pair_cost_max"),
                    "explicit_surplus_allowed_count": rescue_policy.get(
                        "explicit_surplus_allowed_count"
                    ),
                    "over_bridge_surplus_cap_count": rescue_policy.get(
                        "over_bridge_surplus_cap_count"
                    ),
                },
            }
        )
    return out


def private_counts(replay_scorecard: dict[str, Any]) -> dict[str, Any]:
    metrics = body(replay_scorecard, "metrics")
    counts = metrics.get("private_truth_table_counts")
    return counts if isinstance(counts, dict) else {}


def request_schema() -> dict[str, list[str]]:
    return {
        "owner_order_events": [
            "owner_account_or_wallet",
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
            "cancel_ts_ms",
            "cancel_reason",
        ],
        "owner_fill_events": [
            "owner_account_or_wallet",
            "order_id",
            "trade_id",
            "condition_id",
            "market_id",
            "slug",
            "side",
            "action",
            "liquidity_role",
            "fill_ts_ms",
            "fill_price",
            "fill_qty",
            "fee",
            "fee_model",
            "counterparty_or_venue_trade_ref",
        ],
        "owner_inventory_events": [
            "owner_account_or_wallet",
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
            "event_kind",
            "source_order_id",
            "source_trade_id",
        ],
        "owner_redeem_or_settlement_events": [
            "owner_account_or_wallet",
            "condition_id",
            "market_id",
            "slug",
            "event_ts_ms",
            "redeem_qty",
            "redeem_cash",
            "settled_side",
            "tx_hash_or_statement_id",
        ],
    }


def validation_contract() -> dict[str, Any]:
    return {
        "required_tables": list(request_schema().keys()),
        "scope_boundary": [
            "Historical shadow/no-order dry-runs have no owner orders, fills, inventory deltas, redeem events, or actual owner fees.",
            "Historical shadow evidence must remain replay/source-truth, public/proxy evidence, and shadow intent ledger evidence.",
            "Owner-private-truth validation starts only after a future controlled owner execution produces owner-side records.",
        ],
        "join_keys": [
            "owner_account_or_wallet",
            "condition_id or market_id",
            "slug",
            "order_id",
            "trade_id when available",
            "event_ts_ms/fill_ts_ms with millisecond precision",
        ],
        "required_reconciliations": [
            "For future owner execution only, every owner fill links to exactly one order lifecycle row or is explicitly marked unmatched with reason.",
            "For future owner execution only, inventory before/after rows reproduce fills, redeems, residual qty, residual cost, and pair PnL.",
            "For future owner execution only, actual maker/taker role, fee params, fee amount, cash delta, and inventory delta reproduce after-fee PnL within rounding tolerance.",
            "No public/proxy truth row is counted as owner private truth.",
            "No dry-run/no-order simulated action is counted as a private owner fill.",
            "Historical shadow fills/residual/PnL are not required to match owner fills because no owner execution existed.",
            "Unmatched future owner rows and unmatched future local evidence rows are reported as blockers, not silently dropped.",
        ],
        "expected_verdicts": [
            "KEEP_PRIVATE_TRUTH_RECONCILIATION_PASS_RESEARCH_ONLY",
            "UNKNOWN_PRIVATE_TRUTH_RECONCILIATION_PARTIAL_COVERAGE",
            "DISCARD_PRIVATE_TRUTH_RECONCILIATION_MISMATCH",
            "BLOCKED_PRIVATE_TRUTH_RECONCILIATION_NO_FUTURE_OWNER_EXECUTION_TRUTH",
        ],
    }


def markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    current = card["current_truth_boundary"]
    bridge = card["bridge_aggregate"]
    lines = [
        "# Xuan Shadow Review Future Owner Truth Pipeline Request",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- request_ready: `{decision['private_truth_request_ready']}`",
        f"- private_truth_ready: `{decision['private_truth_ready']}`",
        f"- research_shadow_review_ready: `{decision['research_shadow_review_ready']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- live_orders_allowed: `{decision['live_orders_allowed']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- next_action: `{decision['next_action']}`",
        "",
        "## Current Boundary",
        "",
        f"- replay/source truth status: `{current['replay_source_truth_status']}`",
        f"- private truth table counts: `{json.dumps(current['private_truth_table_counts'], sort_keys=True)}`",
        "- The current bridge approval is research-only. It does not prove owner private order/fill/inventory truth.",
        "- Historical shadow/no-order evidence has no owner orders/fills/inventory/redeem events to backfill.",
        "- Owner-private-truth reconciliation starts only after a future controlled owner execution creates owner-side records.",
        "",
        "## Bridge Evidence To Reconcile",
        "",
        f"- accepted actions: `{bridge.get('accepted_actions')}`",
        f"- fills: `{bridge.get('queue_supported_fills')}`",
        f"- strict rescue closes: `{bridge.get('strict_rescue_closes')}`",
        f"- pair PnL: `{bridge.get('pair_pnl')}`",
        f"- residual qty/cost shares: `{bridge.get('residual_qty_share')}` / `{bridge.get('residual_cost_share')}`",
        f"- bridge eligible windows: `{bridge.get('bridge_eligible_window_count')}`",
        "",
        "## Requested Tables",
        "",
    ]
    for table, fields in card["requested_private_truth_schema"].items():
        lines.extend([f"### {table}", ""])
        lines.extend(f"- `{field}`" for field in fields)
        lines.append("")
    lines.extend(
        [
            "## Pass Criteria",
            "",
            *(f"- {item}" for item in card["validation_contract"]["required_reconciliations"]),
            "",
            "## Historical Boundary",
            "",
            "- Do not request historical owner fills for shadow/no-order runs; they do not exist.",
            "- Do not mark historical shadow/no-order runs as `private_truth_ready=true`.",
            "- Treat historical evidence as replay/source-truth validation, public/proxy evidence, and shadow intent ledger only.",
            "",
            "## Non-Goals",
            "",
            "- Do not deploy or restart services.",
            "- Do not send live orders.",
            "- Do not mutate shared ingress, raw/replay, collectors, or production executors.",
            "- Do not treat this request as historical private truth validation.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    caveat_gap = load_json(args.caveat_gap_scorecard)
    approval = load_json(args.approval_gate_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    surplus = load_json(args.surplus_bridge_scorecard)
    packet = load_json(args.shadow_packet_scorecard)
    capital = load_json(args.capital_roi_scorecard)
    replay = load_json(args.replay_source_truth_scorecard)

    caveat_decision = body(caveat_gap, "decision")
    approval_decision = body(approval, "decision")
    runtime_metrics = body(runtime, "metrics")
    surplus_aggregate = body(surplus, "aggregate")
    packet_summary = body(packet, "summary")
    capital_aggregate = body(capital, "aggregate")
    replay_decision = body(replay, "decision")
    counts = private_counts(replay)
    replay_source_truth_ready = replay_decision.get("source_truth_ready") is True or (
        "SOURCE_TRUTH_VALIDATED" in (status(replay) or "")
    )
    replay_private_truth_ready_flag = (
        replay_decision.get("private_truth_ready") is True
        or replay.get("private_truth_ready") is True
    )
    private_truth_ready = replay_private_truth_ready_flag and any(
        fnum(value, 0.0) for value in counts.values()
    )
    owner_execution_observed = any(fnum(value, 0.0) for value in counts.values())
    historical_shadow_owner_truth_applicable = False
    caveats = [str(item) for item in listify(caveat_decision.get("caveats"))]
    has_private_caveat = "private_truth_not_ready" in caveats
    request_ready = is_keep(caveat_gap) and is_keep(approval) and has_private_caveat
    hard_blockers: list[str] = []
    if not is_keep(caveat_gap):
        hard_blockers.append("caveat_gap_plan_not_keep")
    if not is_keep(approval):
        hard_blockers.append("approval_gate_not_keep")
    if not has_private_caveat and not private_truth_ready:
        hard_blockers.append("private_truth_caveat_missing_but_truth_not_ready")
    card_status = (
        "KEEP_SHADOW_REVIEW_FUTURE_OWNER_TRUTH_PIPELINE_REQUEST_READY_LOCAL_ONLY"
        if request_ready and not hard_blockers
        else "UNKNOWN_SHADOW_REVIEW_FUTURE_OWNER_TRUTH_PIPELINE_REQUEST_BLOCKED_LOCAL_ONLY"
    )
    return rounded(
        {
            "artifact": "xuan_shadow_review_private_truth_request_builder",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_private_truth_request_builder.py",
            "status": card_status,
            "inputs": {
                "caveat_gap_scorecard": args.caveat_gap_scorecard,
                "approval_gate_scorecard": args.approval_gate_scorecard,
                "runtime_summary_scorecard": args.runtime_summary_scorecard,
                "surplus_bridge_scorecard": args.surplus_bridge_scorecard,
                "shadow_packet_scorecard": args.shadow_packet_scorecard,
                "capital_roi_scorecard": args.capital_roi_scorecard,
                "replay_source_truth_scorecard": args.replay_source_truth_scorecard,
            },
            "source_statuses": {
                "caveat_gap": status(caveat_gap),
                "approval_gate": status(approval),
                "runtime_summary": status(runtime),
                "surplus_bridge": status(surplus),
                "shadow_packet": status(packet),
                "capital_roi": status(capital),
                "replay_source_truth": status(replay),
            },
            "decision": {
                "private_truth_request_ready": request_ready and not hard_blockers,
                "future_owner_truth_pipeline_request_ready": request_ready and not hard_blockers,
                "private_truth_ready": private_truth_ready,
                "historical_shadow_owner_truth_applicable": historical_shadow_owner_truth_applicable,
                "historical_shadow_private_truth_backfill_required": False,
                "owner_execution_observed": owner_execution_observed,
                "research_shadow_review_ready": bool(
                    approval_decision.get("shadow_review_approval_ready")
                ),
                "paper_shadow_only": True,
                "research_only": True,
                "deployable": False,
                "live_orders_allowed": False,
                "remote_runner_allowed": False,
                "future_remote_allowed_by_this_request": False,
                "hard_blockers": hard_blockers,
                "remaining_live_deploy_blockers": ["private_truth_not_ready"]
                if not private_truth_ready
                else [],
                "next_action": "prepare_future_owner_truth_schema_collector_before_any_owner_execution",
            },
            "current_truth_boundary": {
                "replay_source_truth_status": status(replay),
                "source_truth_ready": replay_source_truth_ready,
                "private_truth_ready": private_truth_ready,
                "private_truth_table_counts": counts,
                "public_or_replay_truth_is_private_truth": False,
                "no_order_dry_run_can_prove_private_truth": False,
                "historical_shadow_owner_truth_status": "not_applicable_no_owner_execution",
                "historical_shadow_evidence_classes": [
                    "replay/source-truth validation",
                    "public/proxy evidence",
                    "shadow intent ledger",
                ],
                "future_owner_execution_required_for_private_truth": True,
            },
            "bridge_aggregate": {
                "accepted_actions": surplus_aggregate.get("accepted_actions"),
                "queue_supported_fills": surplus_aggregate.get("queue_supported_fills"),
                "strict_rescue_closes": surplus_aggregate.get("strict_rescue_closes"),
                "pair_pnl": surplus_aggregate.get("pair_pnl"),
                "roi_on_filled_cost": surplus_aggregate.get("roi_on_filled_cost"),
                "residual_qty_share": surplus_aggregate.get("residual_qty_share"),
                "residual_cost_share": surplus_aggregate.get("residual_cost_share"),
                "bridge_eligible_window_count": surplus_aggregate.get(
                    "bridge_eligible_window_count"
                ),
            },
            "latest_runtime_aggregate": {
                "accepted_actions": runtime_metrics.get("accepted_actions"),
                "queue_supported_fills": runtime_metrics.get("queue_supported_fills"),
                "strict_rescue_closes": runtime_metrics.get("strict_rescue_closes"),
                "pair_pnl": runtime_metrics.get("pair_pnl"),
                "residual_qty_share": runtime_metrics.get("residual_qty_share"),
                "residual_cost_share": runtime_metrics.get("residual_cost_share"),
                "accepted_l1_age_ms_max": runtime_metrics.get("accepted_l1_age_ms_max"),
                "rescue_l1_age_ms_max": runtime_metrics.get("rescue_l1_age_ms_max"),
                "strict_rescue_source_blocks": runtime_metrics.get("strict_rescue_source_blocks"),
            },
            "capital_context": {
                "round_roi": body(capital_aggregate, "round_roi"),
                "capital_reuse": body(capital_aggregate, "capital_reuse"),
                "projection": body(capital_aggregate, "projection"),
            },
            "shadow_packet_context": {
                "shadow_review_ready": packet_summary.get("shadow_review_ready"),
                "shadow_review_ready_source": packet_summary.get("shadow_review_ready_source"),
                "public_benchmark_caveats": packet_summary.get("public_benchmark_caveats"),
                "young_tiny_residual_caveats": packet_summary.get("young_tiny_residual_caveats"),
                "hard_blockers": packet_summary.get("hard_blockers"),
            },
            "bridge_windows_to_reconcile": bridge_windows(surplus),
            "requested_private_truth_schema": request_schema(),
            "validation_contract": validation_contract(),
            "owner_request_scope": {
                "allowed": [
                    "future owner truth schema design",
                    "future owner order/fill/inventory/redeem collector readiness",
                    "future read-only owner order/fill/inventory export after controlled owner execution exists",
                    "future read-only reconciliation scorecard after controlled owner execution exists",
                    "aggregate metric comparison against listed scorecards",
                ],
                "not_allowed": [
                    "requesting nonexistent historical owner fills for no-order shadow runs",
                    "marking historical shadow/no-order runs private_truth_ready",
                    "live_orders",
                    "production_deploy",
                    "service_restart",
                    "shared_ingress_mutation",
                    "collector_rebuild_or_publish",
                    "raw_replay_mutation",
                    "counting public/proxy truth as private truth",
                ],
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--caveat-gap-scorecard", required=True)
    parser.add_argument("--approval-gate-scorecard", required=True)
    parser.add_argument("--runtime-summary-scorecard", required=True)
    parser.add_argument("--surplus-bridge-scorecard", required=True)
    parser.add_argument("--shadow-packet-scorecard", required=True)
    parser.add_argument("--capital-roi-scorecard", required=True)
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
