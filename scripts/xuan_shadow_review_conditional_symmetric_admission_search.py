#!/usr/bin/env python3
"""Conditional symmetric admission search for mature-tail residual repair.

The generic symmetric cap search showed that plain always-on caps are either
too weak or produce too much unmatched inventory. This local scorer narrows the
search to rules that still avoid target residual lookahead but condition on
contemporaneous replay-observable event fields: event kind, public trade size,
side caps, and optional target-slug optimistic scope.

It is local/research-only and does not authorize remote runs.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any

from xuan_shadow_review_generic_symmetric_admission_search import (
    build_targets,
    collect_trade_proxy_events,
    fnum,
    load_action_rows,
    load_json,
    rounded,
    simulate_generic_caps,
)


DEFAULT_TAG = "xuan-frontier-soft-mainline-cap25-mature-tail-surplus-budget-130-20260526T2148Z"
DEFAULT_OUTPUT_ROOT = Path(f".tmp_xuan/local_verifier_artifacts/{DEFAULT_TAG}/remote_outputs")
DEFAULT_DESIGN = Path(".tmp_xuan/scorecards/xuan_shadow_review_residual_aware_maker_completion_design_20260526T2148Z.json")
DEFAULT_RUNTIME = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_runtime_summary.json")
DEFAULT_DENSITY = Path(f".tmp_xuan/scorecards/no_order_{DEFAULT_TAG}_density_preflight_gate.json")
DEFAULT_PROFILE = Path(".tmp_xuan/scorecards/xuan_shadow_review_mature_tail_runner_control_profile_20260526T2038Z.json")
DEFAULT_GENERIC = Path(".tmp_xuan/scorecards/xuan_shadow_review_generic_symmetric_admission_search_20260526T2148Z.json")
DEFAULT_IMPLEMENTABILITY = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_symmetric_prepositioning_implementability_audit_20260526T2148Z.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/xuan_shadow_review_conditional_symmetric_admission_search_20260526T2148Z.json"
)


def parse_caps(value: str) -> list[float]:
    return [fnum(item) for item in value.split(",") if item.strip()]


def parse_kind_sets(value: str) -> list[tuple[str, ...]]:
    out = []
    for raw_group in value.split(";"):
        group = tuple(item.strip() for item in raw_group.split("+") if item.strip())
        if group:
            out.append(group)
    return out


def parse_size_buckets(value: str) -> list[tuple[float, float]]:
    out = []
    for raw_bucket in value.split(","):
        if not raw_bucket.strip():
            continue
        left, _, right = raw_bucket.partition(":")
        out.append((fnum(left), fnum(right, 999999.0)))
    return out


def profile_passes(row: dict[str, Any], unmatched_inventory_ceiling: float) -> bool:
    return bool(row.get("residual_gates_pass_with_uncloseable_seed_block")) and (
        fnum(row.get("unmatched_inventory_cost")) <= unmatched_inventory_ceiling + 1e-12
    )


def find_best(rows: list[dict[str, Any]], mode: str) -> dict[str, Any]:
    if not rows:
        return {}
    if mode == "low_inventory":
        candidates = [row for row in rows if fnum(row.get("closed_residual_cost")) > 1e-12]
        if not candidates:
            candidates = rows
        return min(
            candidates,
            key=lambda row: (
                fnum(row.get("unmatched_inventory_cost")),
                fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
                -fnum(row.get("closed_residual_cost")),
            ),
        )
    if mode == "residual":
        return min(
            rows,
            key=lambda row: (
                fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
                fnum(row.get("residual_qty_after_with_uncloseable_seed_block")),
                fnum(row.get("unmatched_inventory_cost")),
            ),
        )
    if mode == "balanced":
        return min(
            rows,
            key=lambda row: (
                not bool(row.get("residual_gates_pass_with_uncloseable_seed_block")),
                fnum(row.get("unmatched_inventory_cost")),
                fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
                -fnum(row.get("closed_residual_cost")),
            ),
        )
    raise ValueError(mode)


def search(
    *,
    targets: list[dict[str, Any]],
    trade_events: list[dict[str, Any]],
    caps: list[float],
    kind_sets: list[tuple[str, ...]],
    size_buckets: list[tuple[float, float]],
    queue_share: float,
    target_slugs_only: bool,
    residual_cost_after_max: float,
    residual_qty_after_max: float,
    base_residual_cost: float,
    base_residual_qty: float,
    uncloseable_seed_block_cost: float,
    uncloseable_seed_block_qty: float,
    unmatched_inventory_ceiling: float,
) -> dict[str, Any]:
    rows = []
    for kind_set in kind_sets:
        for min_size, max_size in size_buckets:
            filtered_events = [
                event
                for event in trade_events
                if event.get("kind") in kind_set
                and fnum(event.get("public_trade_size")) >= min_size - 1e-12
                and fnum(event.get("public_trade_size")) <= max_size + 1e-12
            ]
            if not filtered_events:
                continue
            for no_cap in caps:
                for yes_cap in caps:
                    row = simulate_generic_caps(
                        targets,
                        filtered_events,
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
                    row["kind_set"] = list(kind_set)
                    row["min_public_trade_size"] = min_size
                    row["max_public_trade_size"] = max_size
                    row["filtered_trade_event_count"] = len(filtered_events)
                    row["residual_and_inventory_pass"] = profile_passes(row, unmatched_inventory_ceiling)
                    rows.append(row)
    residual_only_passes = [row for row in rows if row.get("residual_gates_pass_with_uncloseable_seed_block")]
    residual_and_inventory_passes = [row for row in rows if row.get("residual_and_inventory_pass")]
    nonzero_close_rows = [row for row in rows if fnum(row.get("closed_residual_cost")) > 1e-12]
    return {
        "profile_count": len(rows),
        "nonzero_close_profile_count": len(nonzero_close_rows),
        "residual_only_pass_count": len(residual_only_passes),
        "residual_and_inventory_pass_count": len(residual_and_inventory_passes),
        "best_low_inventory_profile": find_best(rows, "low_inventory"),
        "best_residual_profile": find_best(rows, "residual"),
        "best_balanced_profile": find_best(rows, "balanced"),
        "top_low_inventory_profiles": sorted(
            nonzero_close_rows or rows,
            key=lambda row: (
                fnum(row.get("unmatched_inventory_cost")),
                fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
                -fnum(row.get("closed_residual_cost")),
            ),
        )[:10],
        "top_residual_profiles": sorted(
            rows,
            key=lambda row: (
                fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
                fnum(row.get("residual_qty_after_with_uncloseable_seed_block")),
                fnum(row.get("unmatched_inventory_cost")),
            ),
        )[:10],
        "top_passing_profiles": sorted(
            residual_and_inventory_passes,
            key=lambda row: (
                fnum(row.get("unmatched_inventory_cost")),
                fnum(row.get("residual_cost_after_with_uncloseable_seed_block")),
            ),
        )[:10],
    }


def compact_profile(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind_set": row.get("kind_set"),
        "min_public_trade_size": row.get("min_public_trade_size"),
        "max_public_trade_size": row.get("max_public_trade_size"),
        "no_cap": row.get("no_cap"),
        "yes_cap": row.get("yes_cap"),
        "target_slugs_only": row.get("target_slugs_only"),
        "closed_residual_cost": row.get("closed_residual_cost"),
        "closed_qty": row.get("closed_qty"),
        "residual_cost_after_with_uncloseable_seed_block": row.get(
            "residual_cost_after_with_uncloseable_seed_block"
        ),
        "residual_qty_after_with_uncloseable_seed_block": row.get("residual_qty_after_with_uncloseable_seed_block"),
        "unmatched_inventory_cost": row.get("unmatched_inventory_cost"),
        "unmatched_inventory_qty": row.get("unmatched_inventory_qty"),
        "residual_gates_pass_with_uncloseable_seed_block": row.get(
            "residual_gates_pass_with_uncloseable_seed_block"
        ),
        "residual_and_inventory_pass": row.get("residual_and_inventory_pass"),
        "filtered_trade_event_count": row.get("filtered_trade_event_count"),
    }


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    all_best = card["all_slug_results"]["best_residual_profile"]
    opt_best = card["target_slug_optimistic_results"]["best_residual_profile"]
    lines = [
        "# Xuan Conditional Symmetric Admission Search",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- conditional_symmetric_search_ready: `{d['conditional_symmetric_search_ready']}`",
        f"- conditional_profile_found: `{d['conditional_profile_found']}`",
        f"- target_slug_optimistic_profile_found: `{d['target_slug_optimistic_profile_found']}`",
        f"- future_remote_allowed_by_this_search: `{d['future_remote_allowed_by_this_search']}`",
        "",
        "## Best Residual Profiles",
        "",
        f"- all-slug rule: `{all_best['kind_set']}` size `{all_best['min_public_trade_size']}`-`{all_best['max_public_trade_size']}` caps NO/YES `{all_best['no_cap']}`/`{all_best['yes_cap']}`",
        f"- all-slug residual after seed block: `{all_best['residual_cost_after_with_uncloseable_seed_block']}` / `{all_best['residual_qty_after_with_uncloseable_seed_block']}`",
        f"- all-slug unmatched inventory cost: `{all_best['unmatched_inventory_cost']}`",
        f"- target-slug optimistic rule: `{opt_best['kind_set']}` size `{opt_best['min_public_trade_size']}`-`{opt_best['max_public_trade_size']}` caps NO/YES `{opt_best['no_cap']}`/`{opt_best['yes_cap']}`",
        f"- target-slug optimistic residual after seed block: `{opt_best['residual_cost_after_with_uncloseable_seed_block']}` / `{opt_best['residual_qty_after_with_uncloseable_seed_block']}`",
        f"- target-slug optimistic unmatched inventory cost: `{opt_best['unmatched_inventory_cost']}`",
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
    generic = load_json(args.generic_search_scorecard)
    implementability = load_json(args.implementability_scorecard)
    candidate_profile = profile.get("candidate_profile", {})
    queue_share = fnum(candidate_profile.get("queue_share"), 0.5)
    trade_events = collect_trade_proxy_events(root)
    targets = build_targets(design, load_action_rows(root))
    criteria = design.get("acceptance_criteria_for_future_local_simulation", {})
    metrics = runtime.get("metrics", {})
    caps = parse_caps(args.cap_grid)
    kind_sets = parse_kind_sets(args.kind_sets)
    size_buckets = parse_size_buckets(args.size_buckets)
    residual_cost_after_max = fnum(criteria.get("residual_cost_after_max"))
    residual_qty_after_max = fnum(criteria.get("residual_qty_after_max"))
    uncloseable = next(
        (target for target in targets if str(target.get("quote_intent_id")) == "btc-updown-5m-1779832800:quote:1"),
        {},
    )
    unmatched_inventory_ceiling = fnum(candidate_profile.get("surplus_budget_max_abs_unpaired_cost"), 1.3)
    common = {
        "targets": targets,
        "trade_events": trade_events,
        "caps": caps,
        "kind_sets": kind_sets,
        "size_buckets": size_buckets,
        "queue_share": queue_share,
        "residual_cost_after_max": residual_cost_after_max,
        "residual_qty_after_max": residual_qty_after_max,
        "base_residual_cost": fnum(metrics.get("residual_cost")),
        "base_residual_qty": fnum(metrics.get("residual_qty")),
        "uncloseable_seed_block_cost": fnum(uncloseable.get("held_cost")),
        "uncloseable_seed_block_qty": fnum(uncloseable.get("held_qty")),
        "unmatched_inventory_ceiling": unmatched_inventory_ceiling,
    }
    all_slug = search(target_slugs_only=False, **common)
    target_slug_optimistic = search(target_slugs_only=True, **common)
    all_found = all_slug["residual_and_inventory_pass_count"] > 0
    optimistic_found = target_slug_optimistic["residual_and_inventory_pass_count"] > 0
    hard_blockers = ["conditional_symmetric_search_found_no_all_slug_profile", "private_truth_not_ready"]
    if all_slug["residual_only_pass_count"] <= 0:
        hard_blockers.append("all_slug_conditional_search_still_misses_residual_gate")
    if target_slug_optimistic["residual_only_pass_count"] <= 0:
        hard_blockers.append("target_slug_optimistic_search_still_misses_residual_gate")
    if not optimistic_found:
        hard_blockers.append("target_slug_optimistic_search_found_no_profile")
    thresholds = density.get("thresholds", {})
    card = {
        "artifact": "xuan_shadow_review_conditional_symmetric_admission_search",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_conditional_symmetric_admission_search.py",
        "status": "BLOCKED_SHADOW_REVIEW_CONDITIONAL_SYMMETRIC_ADMISSION_SEARCH_NO_PROFILE_LOCAL_ONLY",
        "inputs": {
            "output_root": str(root),
            "design_scorecard": str(Path(args.design_scorecard).expanduser().resolve()),
            "runtime_summary_scorecard": str(Path(args.runtime_summary_scorecard).expanduser().resolve()),
            "density_preflight_scorecard": str(Path(args.density_preflight_scorecard).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
            "generic_search_scorecard": str(Path(args.generic_search_scorecard).expanduser().resolve()),
            "implementability_scorecard": str(Path(args.implementability_scorecard).expanduser().resolve()),
        },
        "source_statuses": {
            "design": design.get("status"),
            "runtime": runtime.get("status"),
            "density": density.get("status"),
            "profile": profile.get("status"),
            "generic_search": generic.get("status"),
            "implementability": implementability.get("status"),
        },
        "method": {
            "search_type": "conditional_symmetric_admission_grid",
            "cap_grid": caps,
            "kind_sets": [list(group) for group in kind_sets],
            "size_buckets": [[left, right] for left, right in size_buckets],
            "queue_share": queue_share,
            "trade_proxy_event_count": len(trade_events),
            "target_count": len(targets),
            "unmatched_inventory_cost_ceiling": unmatched_inventory_ceiling,
            "uncloseable_seed_block_cost": fnum(uncloseable.get("held_cost")),
            "uncloseable_seed_block_qty": fnum(uncloseable.get("held_qty")),
            "lookahead_boundary": (
                "rules can condition on contemporaneous event kind/price/size and fixed caps only; they cannot use "
                "future residual target identity, future seed price, or target-specific completion limits"
            ),
        },
        "requirements": {
            "residual_cost_after_max": residual_cost_after_max,
            "residual_qty_after_max": residual_qty_after_max,
            "accepted_actions_min": thresholds.get("min_accepted_actions"),
            "queue_supported_fills_min": thresholds.get("min_fills"),
            "strict_rescue_closes_min": thresholds.get("min_rescue_closes"),
        },
        "all_slug_results": {
            "profile_count": all_slug["profile_count"],
            "nonzero_close_profile_count": all_slug["nonzero_close_profile_count"],
            "residual_only_pass_count": all_slug["residual_only_pass_count"],
            "residual_and_inventory_pass_count": all_slug["residual_and_inventory_pass_count"],
            "best_low_inventory_profile": compact_profile(all_slug["best_low_inventory_profile"]),
            "best_residual_profile": compact_profile(all_slug["best_residual_profile"]),
            "best_balanced_profile": compact_profile(all_slug["best_balanced_profile"]),
            "top_low_inventory_profiles": [compact_profile(row) for row in all_slug["top_low_inventory_profiles"]],
            "top_residual_profiles": [compact_profile(row) for row in all_slug["top_residual_profiles"]],
            "top_passing_profiles": [compact_profile(row) for row in all_slug["top_passing_profiles"]],
        },
        "target_slug_optimistic_results": {
            "profile_count": target_slug_optimistic["profile_count"],
            "nonzero_close_profile_count": target_slug_optimistic["nonzero_close_profile_count"],
            "residual_only_pass_count": target_slug_optimistic["residual_only_pass_count"],
            "residual_and_inventory_pass_count": target_slug_optimistic["residual_and_inventory_pass_count"],
            "best_low_inventory_profile": compact_profile(target_slug_optimistic["best_low_inventory_profile"]),
            "best_residual_profile": compact_profile(target_slug_optimistic["best_residual_profile"]),
            "best_balanced_profile": compact_profile(target_slug_optimistic["best_balanced_profile"]),
            "top_low_inventory_profiles": [
                compact_profile(row) for row in target_slug_optimistic["top_low_inventory_profiles"]
            ],
            "top_residual_profiles": [compact_profile(row) for row in target_slug_optimistic["top_residual_profiles"]],
            "top_passing_profiles": [compact_profile(row) for row in target_slug_optimistic["top_passing_profiles"]],
        },
        "interpretation": {
            "core_read": (
                "conditioning on simple contemporaneous event kind and trade-size features still fails to produce a "
                "causality-safe symmetric admission profile"
            ),
            "why_not_remote": (
                "there is no all-slug candidate profile and even the target-slug optimistic boundary has no passing "
                "profile, so another bounded cap25 dry-run would only repeat the same residual blocker"
            ),
            "next_local_target": (
                "move from simple event filters to per-window inventory-state features, or abandon symmetric "
                "prepositioning for a different residual-control mechanism"
            ),
        },
        "decision": {
            "conditional_symmetric_search_ready": True,
            "conditional_profile_found": all_found,
            "target_slug_optimistic_profile_found": optimistic_found,
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
            "next_action": "inventory_state_symmetric_admission_or_non_prepositioned_residual_control_design",
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
    parser.add_argument("--generic-search-scorecard", type=Path, default=DEFAULT_GENERIC)
    parser.add_argument("--implementability-scorecard", type=Path, default=DEFAULT_IMPLEMENTABILITY)
    parser.add_argument("--cap-grid", default="0.35,0.40,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80")
    parser.add_argument(
        "--kind-sets",
        default=(
            "candidate;pair_completion_block;activation_block;"
            "candidate+pair_completion_block;candidate+activation_block;pair_completion_block+activation_block"
        ),
    )
    parser.add_argument("--size-buckets", default="0:999,1:999,2:999,4:999,5:999,8:999,10:999,0:25,0:10,0:8,0:5")
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
