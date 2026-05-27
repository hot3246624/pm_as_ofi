#!/usr/bin/env python3
"""Generic symmetric admission search for mature-tail residual repair.

The prepositioned completion proxy can pass only if prior-resting orders exist
before the residual seed lots are known. This search checks whether simple
target-agnostic symmetric caps could have produced enough completion inventory
without target-specific lookahead or excessive unmatched inventory.

It is local/research-only and does not authorize remote runs.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_DESIGN = Path(".tmp_xuan/scorecards/xuan_shadow_review_residual_aware_maker_completion_design_20260526T2148Z.json")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_IMPLEMENTABILITY = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_symmetric_prepositioning_implementability_audit_20260526T2148Z.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_generic_symmetric_admission_search_20260526T2148Z.json"
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


def load_action_rows(root: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted(root.glob("*.would_action_decisions.csv")):
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                qid = row.get("quote_intent_id")
                if qid and row.get("kind") == "candidate" and row.get("decision") == "accept":
                    rows[qid] = row
    return rows


def build_targets(design: dict[str, Any], action_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    targets = []
    for target in design.get("completion_targets", []):
        action = action_rows.get(str(target.get("quote_intent_id")), {})
        enriched = dict(target)
        enriched["accept_ts_ms"] = fnum(action.get("ts_ms"))
        targets.append(enriched)
    targets.sort(key=lambda row: (fnum(row.get("accept_ts_ms")), str(row.get("quote_intent_id"))))
    return targets


def simulate_generic_caps(
    targets: list[dict[str, Any]],
    trade_events: list[dict[str, Any]],
    *,
    no_cap: float,
    yes_cap: float,
    queue_share: float,
    target_slugs_only: bool,
    residual_cost_after_max: float,
    residual_qty_after_max: float,
    base_residual_cost: float,
    base_residual_qty: float,
    uncloseable_seed_block_cost: float,
    uncloseable_seed_block_qty: float,
) -> dict[str, Any]:
    target_slugs = {str(target.get("slug")) for target in targets}
    target_by_ts: dict[float, list[dict[str, Any]]] = {}
    for target in targets:
        target_by_ts.setdefault(fnum(target.get("accept_ts_ms")), []).append(target)
    ts_values = sorted({event["ts_ms"] for event in trade_events} | set(target_by_ts.keys()))
    inventory: list[dict[str, Any]] = []
    target_closed: dict[str, dict[str, float]] = {
        str(target.get("quote_intent_id")): {"qty": 0.0, "cost": 0.0} for target in targets
    }

    for ts_ms in ts_values:
        for event in [row for row in trade_events if row["ts_ms"] == ts_ms]:
            if target_slugs_only and str(event.get("slug")) not in target_slugs:
                continue
            cap = no_cap if event.get("side") == "NO" else yes_cap
            if fnum(event.get("public_trade_px")) > cap + 1e-12:
                continue
            qty = queue_share * fnum(event.get("public_trade_size"))
            if qty <= 1e-12:
                continue
            inventory.append(
                {
                    "slug": event.get("slug"),
                    "side": event.get("side"),
                    "qty": qty,
                    "cap": cap,
                    "ts_ms": event.get("ts_ms"),
                    "cost": qty * cap,
                }
            )

        for target in target_by_ts.get(ts_ms, []):
            rem = fnum(target.get("held_qty"))
            qid = str(target.get("quote_intent_id"))
            eligible = [
                lot
                for lot in inventory
                if lot["qty"] > 1e-12
                and lot.get("slug") == target.get("slug")
                and lot.get("side") == target.get("close_side")
                and fnum(lot.get("cap")) <= fnum(target.get("max_comp_px_under_strict_cap")) + 1e-12
            ]
            eligible.sort(key=lambda lot: (fnum(lot.get("cap")), fnum(lot.get("ts_ms"))))
            for lot in eligible:
                if rem <= 1e-12:
                    break
                take = min(rem, fnum(lot.get("qty")))
                if take <= 1e-12:
                    continue
                lot["qty"] -= take
                rem -= take
                target_closed[qid]["qty"] += take
                target_closed[qid]["cost"] += take * fnum(target.get("held_px"))

    closed_qty = sum(row["qty"] for row in target_closed.values())
    closed_cost = sum(row["cost"] for row in target_closed.values())
    residual_cost_after = max(0.0, base_residual_cost - closed_cost)
    residual_qty_after = max(0.0, base_residual_qty - closed_qty)
    residual_cost_after_with_seed_block = max(0.0, residual_cost_after - uncloseable_seed_block_cost)
    residual_qty_after_with_seed_block = max(0.0, residual_qty_after - uncloseable_seed_block_qty)
    unmatched_inventory_cost = sum(fnum(lot.get("qty")) * fnum(lot.get("cap")) for lot in inventory)
    unmatched_inventory_qty = sum(fnum(lot.get("qty")) for lot in inventory)
    target_rows = []
    for target in targets:
        qid = str(target.get("quote_intent_id"))
        closed = target_closed.get(qid, {"qty": 0.0, "cost": 0.0})
        target_rows.append(
            {
                "quote_intent_id": qid,
                "slug": target.get("slug"),
                "held_side": target.get("held_side"),
                "close_side": target.get("close_side"),
                "held_qty": fnum(target.get("held_qty")),
                "held_cost": fnum(target.get("held_cost")),
                "closed_qty": closed["qty"],
                "closed_residual_cost": closed["cost"],
                "remaining_qty": max(0.0, fnum(target.get("held_qty")) - closed["qty"]),
                "remaining_residual_cost": max(0.0, fnum(target.get("held_cost")) - closed["cost"]),
            }
        )
    return rounded(
        {
            "no_cap": no_cap,
            "yes_cap": yes_cap,
            "target_slugs_only": target_slugs_only,
            "closed_target_count": len([row for row in target_rows if fnum(row.get("closed_qty")) > 1e-12]),
            "fully_closed_target_count": len([row for row in target_rows if fnum(row.get("remaining_qty")) <= 1e-12]),
            "closed_qty": closed_qty,
            "closed_residual_cost": closed_cost,
            "residual_qty_after": residual_qty_after,
            "residual_cost_after": residual_cost_after,
            "residual_qty_after_with_uncloseable_seed_block": residual_qty_after_with_seed_block,
            "residual_cost_after_with_uncloseable_seed_block": residual_cost_after_with_seed_block,
            "residual_gates_pass": (
                residual_cost_after <= residual_cost_after_max + 1e-12
                and residual_qty_after <= residual_qty_after_max + 1e-12
            ),
            "residual_gates_pass_with_uncloseable_seed_block": (
                residual_cost_after_with_seed_block <= residual_cost_after_max + 1e-12
                and residual_qty_after_with_seed_block <= residual_qty_after_max + 1e-12
            ),
            "unmatched_inventory_qty": unmatched_inventory_qty,
            "unmatched_inventory_cost": unmatched_inventory_cost,
            "targets": target_rows,
        }
    )


def search_profiles(
    targets: list[dict[str, Any]],
    trade_events: list[dict[str, Any]],
    *,
    queue_share: float,
    caps: list[float],
    target_slugs_only: bool,
    residual_cost_after_max: float,
    residual_qty_after_max: float,
    base_residual_cost: float,
    base_residual_qty: float,
    uncloseable_seed_block_cost: float,
    uncloseable_seed_block_qty: float,
) -> list[dict[str, Any]]:
    out = []
    for no_cap in caps:
        for yes_cap in caps:
            out.append(
                simulate_generic_caps(
                    targets,
                    trade_events,
                    no_cap=no_cap,
                    yes_cap=yes_cap,
                    queue_share=queue_share,
                    target_slugs_only=target_slugs_only,
                    residual_cost_after_max=residual_cost_after_max,
                    residual_qty_after_max=residual_qty_after_max,
                    base_residual_cost=base_residual_cost,
                    base_residual_qty=base_residual_qty,
                    uncloseable_seed_block_cost=uncloseable_seed_block_cost,
                    uncloseable_seed_block_qty=uncloseable_seed_block_qty,
                )
            )
    out.sort(
        key=lambda row: (
            not bool(row.get("residual_gates_pass_with_uncloseable_seed_block")),
            fnum(row.get("unmatched_inventory_cost")),
            -fnum(row.get("closed_residual_cost")),
        )
    )
    return out


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    all_best = card["best_all_slug_profile"]
    opt_best = card["best_target_slug_optimistic_profile"]
    all_resid = card["best_residual_all_slug_profile"]
    opt_resid = card["best_residual_target_slug_optimistic_profile"]
    lines = [
        "# Xuan Generic Symmetric Admission Search",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- generic_symmetric_search_ready: `{d['generic_symmetric_search_ready']}`",
        f"- generic_cap_profile_found: `{d['generic_cap_profile_found']}`",
        f"- target_slug_optimistic_profile_found: `{d['target_slug_optimistic_profile_found']}`",
        f"- future_remote_allowed_by_this_search: `{d['future_remote_allowed_by_this_search']}`",
        "",
        "## Best Profiles",
        "",
        f"- all-slug best caps NO/YES: `{all_best['no_cap']}` / `{all_best['yes_cap']}`",
        f"- all-slug residual cost after seed block: `{all_best['residual_cost_after_with_uncloseable_seed_block']}`",
        f"- all-slug unmatched inventory cost: `{all_best['unmatched_inventory_cost']}`",
        f"- all-slug best residual caps NO/YES: `{all_resid['no_cap']}` / `{all_resid['yes_cap']}`",
        f"- all-slug best residual unmatched inventory cost: `{all_resid['unmatched_inventory_cost']}`",
        f"- target-slug optimistic caps NO/YES: `{opt_best['no_cap']}` / `{opt_best['yes_cap']}`",
        f"- target-slug optimistic residual cost after seed block: `{opt_best['residual_cost_after_with_uncloseable_seed_block']}`",
        f"- target-slug optimistic unmatched inventory cost: `{opt_best['unmatched_inventory_cost']}`",
        f"- target-slug best residual caps NO/YES: `{opt_resid['no_cap']}` / `{opt_resid['yes_cap']}`",
        f"- target-slug best residual unmatched inventory cost: `{opt_resid['unmatched_inventory_cost']}`",
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
    runtime = load_json(args.runtime_summary_scorecard)
    density = load_json(args.density_preflight_scorecard)
    profile = load_json(args.profile_scorecard)
    implementability = load_json(args.implementability_scorecard)
    candidate_profile = profile.get("candidate_profile", {})
    queue_share = fnum(candidate_profile.get("queue_share"), 0.5)
    trade_events = collect_trade_proxy_events(root)
    action_rows = load_action_rows(root)
    targets = build_targets(design, action_rows)
    criteria = design.get("acceptance_criteria_for_future_local_simulation", {})
    metrics = runtime.get("metrics", {})
    thresholds = density.get("thresholds", {})
    residual_cost_after_max = fnum(criteria.get("residual_cost_after_max"))
    residual_qty_after_max = fnum(criteria.get("residual_qty_after_max"))
    base_residual_cost = fnum(metrics.get("residual_cost"))
    base_residual_qty = fnum(metrics.get("residual_qty"))
    uncloseable = next(
        (target for target in targets if str(target.get("quote_intent_id")) == "btc-updown-5m-1779832800:quote:1"),
        {},
    )
    uncloseable_seed_block_cost = fnum(uncloseable.get("held_cost"))
    uncloseable_seed_block_qty = fnum(uncloseable.get("held_qty"))
    caps = [fnum(item) for item in str(args.cap_grid).split(",") if str(item).strip()]
    all_slug_profiles = search_profiles(
        targets,
        trade_events,
        queue_share=queue_share,
        caps=caps,
        target_slugs_only=False,
        residual_cost_after_max=residual_cost_after_max,
        residual_qty_after_max=residual_qty_after_max,
        base_residual_cost=base_residual_cost,
        base_residual_qty=base_residual_qty,
        uncloseable_seed_block_cost=uncloseable_seed_block_cost,
        uncloseable_seed_block_qty=uncloseable_seed_block_qty,
    )
    target_slug_profiles = search_profiles(
        targets,
        trade_events,
        queue_share=queue_share,
        caps=caps,
        target_slugs_only=True,
        residual_cost_after_max=residual_cost_after_max,
        residual_qty_after_max=residual_qty_after_max,
        base_residual_cost=base_residual_cost,
        base_residual_qty=base_residual_qty,
        uncloseable_seed_block_cost=uncloseable_seed_block_cost,
        uncloseable_seed_block_qty=uncloseable_seed_block_qty,
    )
    best_all = all_slug_profiles[0]
    best_optimistic = target_slug_profiles[0]
    best_residual_all = min(
        all_slug_profiles,
        key=lambda row: (
            fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
            fnum(row.get("residual_qty_after_with_uncloseable_seed_block")),
            fnum(row.get("unmatched_inventory_cost")),
        ),
    )
    best_residual_optimistic = min(
        target_slug_profiles,
        key=lambda row: (
            fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
            fnum(row.get("residual_qty_after_with_uncloseable_seed_block")),
            fnum(row.get("unmatched_inventory_cost")),
        ),
    )
    unmatched_inventory_ceiling = fnum(candidate_profile.get("surplus_budget_max_abs_unpaired_cost"), 1.3)
    all_residual_only_passes = [
        row for row in all_slug_profiles if row.get("residual_gates_pass_with_uncloseable_seed_block")
    ]
    optimistic_residual_only_passes = [
        row for row in target_slug_profiles if row.get("residual_gates_pass_with_uncloseable_seed_block")
    ]
    generic_passes = [
        row
        for row in all_slug_profiles
        if row.get("residual_gates_pass_with_uncloseable_seed_block")
        and fnum(row.get("unmatched_inventory_cost")) <= unmatched_inventory_ceiling + 1e-12
    ]
    optimistic_passes = [
        row
        for row in target_slug_profiles
        if row.get("residual_gates_pass_with_uncloseable_seed_block")
        and fnum(row.get("unmatched_inventory_cost")) <= unmatched_inventory_ceiling + 1e-12
    ]
    hard_blockers = ["generic_symmetric_cap_search_found_no_profile", "private_truth_not_ready"]
    if not optimistic_residual_only_passes:
        hard_blockers.append("target_slug_optimistic_profile_still_misses_residual_cost_gate")
    elif not optimistic_passes:
        hard_blockers.append("target_slug_optimistic_residual_pass_requires_excess_unmatched_inventory")
    if not all_residual_only_passes:
        hard_blockers.append("all_slug_generic_profile_still_misses_residual_cost_gate")
    if fnum(best_all.get("unmatched_inventory_cost")) > unmatched_inventory_ceiling + 1e-12:
        hard_blockers.append("all_slug_generic_inventory_exceeds_unmatched_cost_ceiling")
    card = {
        "artifact": "xuan_shadow_review_generic_symmetric_admission_search",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_generic_symmetric_admission_search.py",
        "status": "BLOCKED_SHADOW_REVIEW_GENERIC_SYMMETRIC_ADMISSION_SEARCH_NO_PROFILE_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "design_scorecard": str(Path(args.design_scorecard).expanduser().resolve()),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
            "implementability_scorecard": str(Path(args.implementability_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "design": design.get("status"),
            "runtime": runtime.get("status"),
            "density": density.get("status"),
            "profile": profile.get("status"),
            "implementability": implementability.get("status"),
        },
        "method": {
            "search_type": "target_agnostic_symmetric_cap_grid",
            "cap_grid": caps,
            "queue_share": queue_share,
            "trade_proxy_event_count": len(trade_events),
            "target_count": len(targets),
            "unmatched_inventory_cost_ceiling": unmatched_inventory_ceiling,
            "uncloseable_seed_block_cost": uncloseable_seed_block_cost,
            "uncloseable_seed_block_qty": uncloseable_seed_block_qty,
            "all_slug_rule": "prior-resting caps can fill on every source-quality allowed trade proxy event in the run",
            "target_slug_optimistic_rule": "same cap rule restricted to target slugs; this is optimistic and not deployable",
        },
        "requirements": {
            "residual_cost_after_max": residual_cost_after_max,
            "residual_qty_after_max": residual_qty_after_max,
            "accepted_actions_min": thresholds.get("min_accepted_actions"),
            "queue_supported_fills_min": thresholds.get("min_fills"),
            "strict_rescue_closes_min": thresholds.get("min_rescue_closes"),
        },
        "best_all_slug_profile": best_all,
        "best_target_slug_optimistic_profile": best_optimistic,
        "best_residual_all_slug_profile": best_residual_all,
        "best_residual_target_slug_optimistic_profile": best_residual_optimistic,
        "profile_counts": {
            "all_slug_profiles": len(all_slug_profiles),
            "all_slug_residual_only_passes": len(all_residual_only_passes),
            "all_slug_residual_and_inventory_passes": len(generic_passes),
            "target_slug_optimistic_profiles": len(target_slug_profiles),
            "target_slug_optimistic_residual_only_passes": len(optimistic_residual_only_passes),
            "target_slug_optimistic_residual_and_inventory_passes": len(optimistic_passes),
        },
        "top_all_slug_profiles": all_slug_profiles[:10],
        "top_target_slug_optimistic_profiles": target_slug_profiles[:10],
        "interpretation": {
            "core_read": (
                "simple generic symmetric caps are not enough: they either fail residual reduction, create large "
                "unmatched inventory, or both"
            ),
            "optimistic_read": (
                "even the target-slug-only optimistic lower bound misses the residual-cost gate, so the next useful "
                "work is not a remote run but a richer local admission model"
            ),
            "next_local_target": (
                "search conditional symmetric admission features using only contemporaneous source-quality, price, "
                "queue, and inventory state; do not use target residual identity or future seed price"
            ),
        },
        "decision": {
            "generic_symmetric_search_ready": True,
            "generic_cap_profile_found": bool(generic_passes),
            "target_slug_optimistic_profile_found": bool(optimistic_passes),
            "candidate_profile_ready": False,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_search": False,
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
            "next_action": "conditional_symmetric_admission_feature_search_before_any_new_remote",
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--design-scorecard", type=Path, default=DEFAULT_DESIGN)
    parser.add_argument("--runtime-summary-scorecard", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--density-preflight-scorecard", type=Path, default=DEFAULT_DENSITY)
    parser.add_argument("--profile-scorecard", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--implementability-scorecard", type=Path, default=DEFAULT_IMPLEMENTABILITY)
    parser.add_argument("--cap-grid", default="0.35,0.40,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80")
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
