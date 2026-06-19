#!/usr/bin/env python3
"""Conservative local simulator for residual-aware passive maker completion.

The simulator replays observed public-trade proxy events after each target
residual lot fill. A target close is counted only when a later same-slug,
same-close-side trade has source-quality allowed and trades at or below the
passive close limit. Fill quantity is haircut by queue_share and economics use
the limit price plus taker-fee math as a conservative bound.

This is local/research-only and does not authorize remote runs.
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
DEFAULT_FAILURE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_failure_diagnosis_20260526T2148Z.json")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_residual_aware_maker_completion_simulator_20260526T2148Z.json"
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
    rows = []
    for event in iter_events(root):
        side = event.get("side")
        trade_px = event.get("public_trade_px")
        trade_size = event.get("public_trade_size")
        if not side or trade_px is None or trade_size is None:
            continue
        source_decision = event.get("source_quality_decision")
        if source_decision not in (None, "", "allow"):
            continue
        rows.append(
            {
                "ts_ms": fnum(event.get("ts_ms")),
                "slug": event.get("slug"),
                "side": side,
                "public_trade_px": fnum(trade_px),
                "public_trade_size": fnum(trade_size),
                "kind": event.get("kind"),
                "source_quality_decision": source_decision or "not_recorded",
            }
        )
    rows.sort(key=lambda row: (row["ts_ms"], str(row["slug"]), str(row["side"])))
    return rows


def lot_fill_times(failure: dict[str, Any]) -> dict[str, float]:
    return {
        str(lot.get("quote_intent_id")): fnum(lot.get("fill_ms"))
        for lot in failure.get("residual_lots", {}).get("top_lots", [])
        if lot.get("quote_intent_id")
    }


def simulate_mode(
    targets: list[dict[str, Any]],
    fill_times: dict[str, float],
    trade_events: list[dict[str, Any]],
    *,
    limit_field: str,
    fee_rate: float,
    queue_share: float,
) -> dict[str, Any]:
    target_results = []
    total_close_qty = 0.0
    total_closed_residual_cost = 0.0
    total_completion_cost = 0.0
    total_pair_pnl_delta = 0.0
    closed_target_count = 0
    partial_target_count = 0

    for target in targets:
        qid = str(target.get("quote_intent_id"))
        start_ms = fill_times.get(qid, 0.0)
        limit_px = fnum(target.get(limit_field))
        remaining = fnum(target.get("held_qty"))
        held_px = fnum(target.get("held_px"))
        fills = []
        future_prices = []
        for event in trade_events:
            if event["ts_ms"] <= start_ms:
                continue
            if event["slug"] != target.get("slug") or event["side"] != target.get("close_side"):
                continue
            future_prices.append(event["public_trade_px"])
            if event["public_trade_px"] > limit_px + 1e-12:
                continue
            take = min(remaining, queue_share * event["public_trade_size"])
            if take <= 1e-12:
                continue
            fills.append(
                {
                    "ts_ms": event["ts_ms"],
                    "kind": event["kind"],
                    "public_trade_px": event["public_trade_px"],
                    "public_trade_size": event["public_trade_size"],
                    "filled_qty": take,
                    "limit_px": limit_px,
                }
            )
            remaining -= take
            if remaining <= 1e-12:
                break
        closed_qty = fnum(target.get("held_qty")) - max(0.0, remaining)
        closed_residual_cost = closed_qty * held_px
        completion_cost = closed_qty * limit_px
        fee = fee_per_share(limit_px, fee_rate)
        pair_pnl_delta = closed_qty * (1.0 - held_px - limit_px - fee)
        total_close_qty += closed_qty
        total_closed_residual_cost += closed_residual_cost
        total_completion_cost += completion_cost
        total_pair_pnl_delta += pair_pnl_delta
        if remaining <= 1e-12:
            closed_target_count += 1
        elif closed_qty > 1e-12:
            partial_target_count += 1
        min_future_trade_px = min(future_prices) if future_prices else None
        target_results.append(
            {
                "quote_intent_id": qid,
                "slug": target.get("slug"),
                "held_side": target.get("held_side"),
                "close_side": target.get("close_side"),
                "held_qty": fnum(target.get("held_qty")),
                "closed_qty": closed_qty,
                "remaining_qty": max(0.0, remaining),
                "limit_px": limit_px,
                "held_px": held_px,
                "net_pair_cost_bound": held_px + limit_px + fee,
                "pair_pnl_delta_bound": pair_pnl_delta,
                "future_trade_count": len(future_prices),
                "min_future_trade_px": min_future_trade_px,
                "min_future_trade_px_gap_vs_limit": None
                if min_future_trade_px is None
                else min_future_trade_px - limit_px,
                "fill_event_count": len(fills),
                "fills": fills[:10],
            }
        )

    return rounded(
        {
            "limit_field": limit_field,
            "target_count": len(targets),
            "closed_target_count": closed_target_count,
            "partial_target_count": partial_target_count,
            "total_close_qty": total_close_qty,
            "total_closed_residual_cost": total_closed_residual_cost,
            "total_completion_cost": total_completion_cost,
            "total_pair_pnl_delta_bound": total_pair_pnl_delta,
            "target_results": target_results,
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    lines = [
        "# Xuan Residual-Aware Maker Completion Simulator",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- simulator_ready: `{d['simulator_ready']}`",
        f"- passive_completion_simulation_pass: `{d['passive_completion_simulation_pass']}`",
        f"- candidate_profile_ready: `{d['candidate_profile_ready']}`",
        f"- bounded_cap25_remote_rationale_ready: `{d['bounded_cap25_remote_rationale_ready']}`",
        f"- future_remote_allowed_by_this_simulator: `{d['future_remote_allowed_by_this_simulator']}`",
        f"- hard_blockers: `{', '.join(d['hard_blockers']) or 'none'}`",
        "",
        "## Result",
        "",
        f"- target close qty/cost required: `{card['requirements']['target_close_qty_required']}` / `{card['requirements']['target_close_cost_required']}`",
        f"- target099 close qty/residual_cost: `{card['simulation']['target_pair_cost_cap_099']['total_close_qty']}` / `{card['simulation']['target_pair_cost_cap_099']['total_closed_residual_cost']}`",
        f"- strict102 close qty/residual_cost: `{card['simulation']['strict_surplus_cap_102']['total_close_qty']}` / `{card['simulation']['strict_surplus_cap_102']['total_closed_residual_cost']}`",
        "",
        "## Interpretation",
        "",
        f"- conclusion: `{card['interpretation']['conclusion']}`",
        f"- next_local_target: `{card['interpretation']['next_local_target']}`",
        "",
        "## Boundary",
        "",
        "- Local/research-only.",
        "- Does not authorize remote runs, cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).expanduser().resolve()
    design = load_json(args.design_scorecard)
    failure = load_json(args.failure_diagnosis_scorecard)
    runtime = load_json(args.runtime_summary_scorecard)
    density = load_json(args.density_preflight_scorecard)
    profile = load_json(args.profile_scorecard)
    candidate_profile = profile.get("candidate_profile", {})
    queue_share = fnum(candidate_profile.get("queue_share"), 0.5)
    fee_rate = fnum(candidate_profile.get("taker_fee_rate"), 0.07)
    targets = design.get("completion_targets", [])
    fills_by_qid = lot_fill_times(failure)
    trade_events = collect_trade_proxy_events(root)

    target_099 = simulate_mode(
        targets,
        fills_by_qid,
        trade_events,
        limit_field="target_comp_px_under_pair_cost_cap",
        fee_rate=fee_rate,
        queue_share=queue_share,
    )
    strict_102 = simulate_mode(
        targets,
        fills_by_qid,
        trade_events,
        limit_field="max_comp_px_under_strict_cap",
        fee_rate=fee_rate,
        queue_share=queue_share,
    )

    req = design.get("target", {})
    criteria = design.get("acceptance_criteria_for_future_local_simulation", {})
    runtime_metrics = runtime.get("metrics", {})
    thresholds = density.get("thresholds", {})
    required_close_qty = fnum(req.get("target_close_qty"))
    required_close_cost = fnum(req.get("target_close_cost"))
    residual_cost_after_strict = max(0.0, fnum(runtime_metrics.get("residual_cost")) - strict_102["total_closed_residual_cost"])
    residual_qty_after_strict = max(0.0, fnum(runtime_metrics.get("residual_qty")) - strict_102["total_close_qty"])
    strict_pass = (
        strict_102["total_close_qty"] >= required_close_qty - 1e-12
        and residual_cost_after_strict <= fnum(criteria.get("residual_cost_after_max"), 0.0) + 1e-12
        and residual_qty_after_strict <= fnum(criteria.get("residual_qty_after_max"), 0.0) + 1e-12
    )

    hard_blockers = []
    if not strict_pass:
        hard_blockers.append("post_fill_passive_maker_completion_has_no_sufficient_replay_fill_opportunity")
    if strict_102["total_close_qty"] <= 1e-12:
        hard_blockers.append("zero_target_lots_fill_under_strict_cap_replay_proxy")
    if fnum(runtime_metrics.get("queue_supported_fills")) < fnum(thresholds.get("min_fills")):
        hard_blockers.append("base_run_fill_density_below_min")
    if fnum(runtime_metrics.get("strict_rescue_closes")) < fnum(thresholds.get("min_rescue_closes")):
        hard_blockers.append("base_run_rescue_density_below_min")

    card = {
        "artifact": "xuan_shadow_review_residual_aware_maker_completion_simulator",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_residual_aware_maker_completion_simulator.py",
        "status": "KEEP_SHADOW_REVIEW_RESIDUAL_AWARE_MAKER_COMPLETION_SIMULATOR_READY_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "design_scorecard": str(Path(args.design_scorecard).expanduser().resolve()),
            "failure_diagnosis_scorecard": str(Path(args.failure_diagnosis_scorecard).expanduser().resolve()),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "design": design.get("status"),
            "failure_diagnosis": failure.get("status"),
            "runtime": runtime.get("status"),
            "density": density.get("status"),
            "profile": profile.get("status"),
        },
        "method": {
            "fill_rule": "future same-slug same-close-side public_trade_px <= passive_limit_px",
            "source_rule": "count only events with source_quality_decision empty/not_recorded/allow",
            "qty_rule": "filled_qty <= queue_share * public_trade_size",
            "cost_rule": "use passive limit price and taker-fee formula as conservative bound",
            "queue_share": queue_share,
            "taker_fee_rate": fee_rate,
            "trade_proxy_event_count": len(trade_events),
        },
        "requirements": {
            "target_close_qty_required": required_close_qty,
            "target_close_cost_required": required_close_cost,
            "residual_cost_after_max": fnum(criteria.get("residual_cost_after_max")),
            "residual_qty_after_max": fnum(criteria.get("residual_qty_after_max")),
            "queue_supported_fills_min": fnum(criteria.get("queue_supported_fills_min")),
            "strict_rescue_closes_min": fnum(criteria.get("strict_rescue_closes_min")),
        },
        "simulation": {
            "target_pair_cost_cap_099": target_099,
            "strict_surplus_cap_102": strict_102,
            "residual_after_strict_surplus_cap_102": {
                "residual_cost_after": residual_cost_after_strict,
                "residual_qty_after": residual_qty_after_strict,
            },
        },
        "interpretation": {
            "conclusion": (
                "post-fill passive maker completion does not find sufficient replay trade-through opportunities under "
                "the current conservative economic caps"
            ),
            "failure_mode": (
                "the needed residual closes require either earlier/pre-positioned maker completion, different market "
                "selection, or a locally implemented close-generation controller with evidence of actual fills"
            ),
            "next_local_target": (
                "design local pre-positioned or trigger-earlier maker-completion simulation before any further cap25 remote"
            ),
        },
        "decision": {
            "simulator_ready": True,
            "passive_completion_simulation_pass": strict_pass,
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
            "next_action": "local_prepositioned_or_trigger_earlier_maker_completion_design_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--design-scorecard", type=Path, default=DEFAULT_DESIGN)
    parser.add_argument("--failure-diagnosis-scorecard", type=Path, default=DEFAULT_FAILURE)
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
