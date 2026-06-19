#!/usr/bin/env python3
"""Local simulator for prepositioned residual-aware maker completion.

Unlike the post-fill simulator, this assumes residual-aware opposite-side
completion intents are resting before the target residual lots would otherwise
remain open. It is a replay proxy only: no remote run, no live orders, and no
shared-service mutation.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_DESIGN = Path(".tmp_xuan/scorecards/xuan_shadow_review_residual_aware_maker_completion_design_20260526T2148Z.json")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_prepositioned_maker_completion_simulator_20260526T2148Z.json"
)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def fee_per_share(px: float, rate: float) -> float:
    price = min(max(px, 0.0), 1.0)
    return rate * price * (1.0 - price)


def iter_events(root: Path) -> Any:
    for path in sorted(root.glob("*.events.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def collect_trade_proxy_events(root: Path) -> list[dict[str, Any]]:
    out = []
    for event in iter_events(root):
        if not event.get("side") or event.get("public_trade_px") is None or event.get("public_trade_size") is None:
            continue
        if event.get("source_quality_decision") not in (None, "", "allow"):
            continue
        out.append(
            {
                "ts_ms": fnum(event.get("ts_ms")),
                "slug": event.get("slug"),
                "side": event.get("side"),
                "public_trade_px": fnum(event.get("public_trade_px")),
                "public_trade_size": fnum(event.get("public_trade_size")),
                "kind": event.get("kind"),
            }
        )
    out.sort(key=lambda row: (row["ts_ms"], str(row["slug"]), str(row["side"])))
    return out


def build_orders(targets: list[dict[str, Any]], limit_field: str) -> list[dict[str, Any]]:
    orders = []
    for target in targets:
        held_qty = fnum(target.get("held_qty"))
        orders.append(
            {
                "quote_intent_id": target.get("quote_intent_id"),
                "slug": target.get("slug"),
                "held_side": target.get("held_side"),
                "close_side": target.get("close_side"),
                "held_qty": held_qty,
                "remaining_qty": held_qty,
                "closed_qty": 0.0,
                "held_px": fnum(target.get("held_px")),
                "held_cost": fnum(target.get("held_cost")),
                "limit_px": fnum(target.get(limit_field)),
                "fills": [],
            }
        )
    return orders


def simulate_prepositioned(
    targets: list[dict[str, Any]],
    trade_events: list[dict[str, Any]],
    *,
    limit_field: str,
    fee_rate: float,
    queue_share: float,
) -> dict[str, Any]:
    orders = build_orders(targets, limit_field)
    for event in trade_events:
        available = queue_share * event["public_trade_size"]
        if available <= 1e-12:
            continue
        eligible = [
            order
            for order in orders
            if order["remaining_qty"] > 1e-12
            and order["slug"] == event["slug"]
            and order["close_side"] == event["side"]
            and event["public_trade_px"] <= order["limit_px"] + 1e-12
        ]
        eligible.sort(key=lambda order: order["limit_px"], reverse=True)
        for order in eligible:
            if available <= 1e-12:
                break
            take = min(order["remaining_qty"], available)
            if take <= 1e-12:
                continue
            order["remaining_qty"] -= take
            order["closed_qty"] += take
            available -= take
            order["fills"].append(
                {
                    "ts_ms": event["ts_ms"],
                    "kind": event["kind"],
                    "public_trade_px": event["public_trade_px"],
                    "public_trade_size": event["public_trade_size"],
                    "filled_qty": take,
                }
            )

    total_close_qty = 0.0
    total_closed_residual_cost = 0.0
    total_completion_cost = 0.0
    total_pair_pnl_delta = 0.0
    for order in orders:
        total_close_qty += order["closed_qty"]
        total_closed_residual_cost += order["closed_qty"] * order["held_px"]
        total_completion_cost += order["closed_qty"] * order["limit_px"]
        total_pair_pnl_delta += order["closed_qty"] * (
            1.0 - order["held_px"] - order["limit_px"] - fee_per_share(order["limit_px"], fee_rate)
        )
    closed = [order for order in orders if order["remaining_qty"] <= 1e-12]
    partial = [order for order in orders if order["closed_qty"] > 1e-12 and order["remaining_qty"] > 1e-12]
    unfilled = [order for order in orders if order["closed_qty"] <= 1e-12]
    return rounded(
        {
            "limit_field": limit_field,
            "target_count": len(orders),
            "closed_target_count": len(closed),
            "partial_target_count": len(partial),
            "unfilled_target_count": len(unfilled),
            "total_close_qty": total_close_qty,
            "total_closed_residual_cost": total_closed_residual_cost,
            "total_completion_cost": total_completion_cost,
            "total_pair_pnl_delta_bound": total_pair_pnl_delta,
            "orders": [
                {
                    "quote_intent_id": order["quote_intent_id"],
                    "slug": order["slug"],
                    "held_side": order["held_side"],
                    "close_side": order["close_side"],
                    "held_qty": order["held_qty"],
                    "closed_qty": order["closed_qty"],
                    "remaining_qty": max(0.0, order["remaining_qty"]),
                    "held_px": order["held_px"],
                    "held_cost": order["held_cost"],
                    "limit_px": order["limit_px"],
                    "fill_event_count": len(order["fills"]),
                    "fills": order["fills"][:10],
                }
                for order in orders
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    h = card["hybrid_uncloseable_lot_block_proxy"]
    lines = [
        "# Xuan Prepositioned Maker Completion Simulator",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- prepositioned_simulator_ready: `{d['prepositioned_simulator_ready']}`",
        f"- pure_prepositioned_pass: `{d['pure_prepositioned_pass']}`",
        f"- hybrid_prepositioned_plus_uncloseable_block_proxy_pass: `{d['hybrid_prepositioned_plus_uncloseable_block_proxy_pass']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_simulator: `{d['future_remote_allowed_by_this_simulator']}`",
        "",
        "## Result",
        "",
        f"- strict102 closed targets: `{card['simulation']['strict_surplus_cap_102']['closed_target_count']}` / `{card['simulation']['strict_surplus_cap_102']['target_count']}`",
        f"- strict102 closed qty/residual cost: `{card['simulation']['strict_surplus_cap_102']['total_close_qty']}` / `{card['simulation']['strict_surplus_cap_102']['total_closed_residual_cost']}`",
        f"- hybrid residual qty/cost after: `{h['residual_qty_after']}` / `{h['residual_cost_after']}`",
        f"- hybrid density proxy accepted/fills/rescues: `{h['accepted_actions_proxy']}` / `{h['queue_supported_fills_proxy']}` / `{h['strict_rescue_closes_proxy']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Proxy only; runner feature and local scaffold are still required.",
        "- Does not authorize remote runs, cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    design = load_json(args.design_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    density = load_json(args.density_preflight_scorecard)
    profile = load_json(args.profile_scorecard)
    candidate_profile = profile.get("candidate_profile", {})
    queue_share = fnum(candidate_profile.get("queue_share"), 0.5)
    fee_rate = fnum(candidate_profile.get("taker_fee_rate"), 0.07)
    targets = design.get("completion_targets", [])
    trade_events = collect_trade_proxy_events(root)
    target_099 = simulate_prepositioned(
        targets,
        trade_events,
        limit_field="target_comp_px_under_pair_cost_cap",
        fee_rate=fee_rate,
        queue_share=queue_share,
    )
    strict_102 = simulate_prepositioned(
        targets,
        trade_events,
        limit_field="max_comp_px_under_strict_cap",
        fee_rate=fee_rate,
        queue_share=queue_share,
    )
    metrics = runtime.get("metrics", {})
    thresholds = density.get("thresholds", {})
    criteria = design.get("acceptance_criteria_for_future_local_simulation", {})
    unclosed_orders = [order for order in strict_102["orders"] if fnum(order.get("remaining_qty")) > 1e-12]
    unclosed_residual_cost = sum(fnum(order.get("remaining_qty")) * fnum(order.get("held_px")) for order in unclosed_orders)
    unclosed_residual_qty = sum(fnum(order.get("remaining_qty")) for order in unclosed_orders)
    unclosed_seed_count = len(unclosed_orders)
    residual_cost_after_hybrid = max(
        0.0,
        fnum(metrics.get("residual_cost")) - fnum(strict_102.get("total_closed_residual_cost")) - unclosed_residual_cost,
    )
    residual_qty_after_hybrid = max(
        0.0,
        fnum(metrics.get("residual_qty")) - fnum(strict_102.get("total_close_qty")) - unclosed_residual_qty,
    )
    completion_fill_count = fnum(strict_102.get("closed_target_count"))
    accepted_proxy = fnum(metrics.get("accepted_actions")) - unclosed_seed_count + completion_fill_count
    fills_proxy = fnum(metrics.get("queue_supported_fills")) - unclosed_seed_count + completion_fill_count
    rescue_proxy = fnum(metrics.get("strict_rescue_closes")) + completion_fill_count
    hybrid_gates = {
        "accepted_actions": accepted_proxy >= fnum(thresholds.get("min_accepted_actions")) - 1e-12,
        "queue_supported_fills": fills_proxy >= fnum(thresholds.get("min_fills")) - 1e-12,
        "strict_rescue_closes": rescue_proxy >= fnum(thresholds.get("min_rescue_closes")) - 1e-12,
        "residual_cost_after": residual_cost_after_hybrid <= fnum(criteria.get("residual_cost_after_max")) + 1e-12,
        "residual_qty_after": residual_qty_after_hybrid <= fnum(criteria.get("residual_qty_after_max")) + 1e-12,
    }
    hybrid_pass = all(hybrid_gates.values()) and bool(unclosed_orders)
    pure_prepositioned_pass = (
        fnum(strict_102.get("total_close_qty")) >= fnum(design.get("target", {}).get("target_close_qty")) - 1e-12
        and max(0.0, fnum(metrics.get("residual_cost")) - fnum(strict_102.get("total_closed_residual_cost")))
        <= fnum(criteria.get("residual_cost_after_max")) + 1e-12
        and max(0.0, fnum(metrics.get("residual_qty")) - fnum(strict_102.get("total_close_qty")))
        <= fnum(criteria.get("residual_qty_after_max")) + 1e-12
    )

    hard_blockers = ["runner_feature_not_implemented", "must_validate_in_local_runner_scaffold", "private_truth_not_ready"]
    if not hybrid_pass:
        hard_blockers.append("hybrid_proxy_does_not_clear_density_and_residual")
    card = {
        "artifact": "xuan_shadow_review_prepositioned_maker_completion_simulator",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_prepositioned_maker_completion_simulator.py",
        "status": "KEEP_SHADOW_REVIEW_PREPOSITIONED_MAKER_COMPLETION_SIMULATOR_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "design_scorecard": str(Path(args.design_scorecard).expanduser().resolve()),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "design": design.get("status"),
            "runtime": runtime.get("status"),
            "density": density.get("status"),
            "profile": profile.get("status"),
        },
        "method": {
            "placement_assumption": "target completion orders rest before observed same-side trade-through opportunities",
            "fill_rule": "same-slug same-close-side public_trade_px <= limit_px",
            "allocation_rule": "higher passive bid limit gets queue allocation first",
            "qty_rule": "filled_qty <= queue_share * public_trade_size",
            "cost_rule": "use passive limit plus taker-fee formula as conservative net pair bound",
            "queue_share": queue_share,
            "taker_fee_rate": fee_rate,
            "trade_proxy_event_count": len(trade_events),
        },
        "simulation": {
            "target_pair_cost_cap_099": target_099,
            "strict_surplus_cap_102": strict_102,
        },
        "pure_prepositioned_residual_after": {
            "residual_cost_after": max(0.0, fnum(metrics.get("residual_cost")) - fnum(strict_102.get("total_closed_residual_cost"))),
            "residual_qty_after": max(0.0, fnum(metrics.get("residual_qty")) - fnum(strict_102.get("total_close_qty"))),
        },
        "hybrid_uncloseable_lot_block_proxy": {
            "unclosed_seed_count": unclosed_seed_count,
            "unclosed_residual_qty": unclosed_residual_qty,
            "unclosed_residual_cost": unclosed_residual_cost,
            "unclosed_orders": unclosed_orders,
            "residual_cost_after": residual_cost_after_hybrid,
            "residual_qty_after": residual_qty_after_hybrid,
            "accepted_actions_proxy": accepted_proxy,
            "queue_supported_fills_proxy": fills_proxy,
            "strict_rescue_closes_proxy": rescue_proxy,
            "gates": hybrid_gates,
        },
        "interpretation": {
            "pure_prepositioned_read": (
                "prepositioned completion closes most target lots but leaves one uncloseable residual lot, so pure "
                "prepositioning still misses the residual-cost gate"
            ),
            "hybrid_read": (
                "prepositioned completion plus blocking the single uncloseable residual seed is the first local proxy "
                "that clears density and residual together, but it requires runner implementation before any remote"
            ),
            "next_local_target": "implement local runner/scaffold controls for prepositioned completion plus uncloseable-lot block",
        },
        "decision": {
            "prepositioned_simulator_ready": True,
            "pure_prepositioned_pass": pure_prepositioned_pass,
            "hybrid_prepositioned_plus_uncloseable_block_proxy_pass": hybrid_pass,
            "candidate_profile_ready": False,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_simulator": False,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "next_action": "local_runner_scaffold_for_hybrid_prepositioned_completion_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--design-scorecard", type=Path, default=DEFAULT_DESIGN)
    parser.add_argument("--runtime-summary-scorecard", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=None)
    args = parser.parse_args()
    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
