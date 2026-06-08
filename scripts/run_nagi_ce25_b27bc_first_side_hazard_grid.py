#!/usr/bin/env python3
"""Run local no-order first-side hazard grids.

This evaluates dynamic first-side controls for the maker-shadow proxy.  It is
local-only: it reads an existing materialized CSV, does not fetch data, does
not import credentials, and does not execute orders.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
from pathlib import Path
from typing import Any

import run_nagi_ce25_b27bc_maker_shadow as shadow


KEEP_STATUS = "KEEP_NAGI_CE25_B27BC_FIRST_SIDE_HAZARD_GRID_CANDIDATE_FOUND_RESEARCH_PROXY_NOT_READY"
BLOCKED_STATUS = "BLOCKED_NAGI_CE25_B27BC_FIRST_SIDE_HAZARD_GRID_NO_PASS_NOT_READY"

GATE_PROFILES: dict[str, tuple[str, ...]] = {
    "all_gates": (),
    "primary_35_65": ("ce25_last60_35_50_primary", "ce25_last60_50_65_primary"),
}

SIDE_PROFILES: dict[str, tuple[str, ...]] = {
    "all_sides": (),
    "no_only": ("NO",),
}

YES_BAND_PROFILES: dict[str, tuple[str, ...]] = {
    "yes_all_bands": (),
    "reject_yes_35_50": ("35_50",),
    "reject_yes_35_65": ("35_50", "50_65"),
}


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def metric(row: dict[str, Any], key: str, default: float) -> float:
    value = row.get(key)
    return float(value) if value is not None else default


def run_variant(rows: list[dict[str, str]], cfg: shadow.PipelineConfig) -> dict[str, Any]:
    pipeline = shadow.MakerShadowPipeline(cfg)
    pipeline.run(rows)
    summary_rows = pipeline.summary_rows()
    overall = next((row for row in summary_rows if row.get("window_key") == "ALL"), summary_rows[0])
    event_counts: dict[str, int] = {}
    first_side_reasons: dict[str, int] = {}
    for event in pipeline.events:
        name = str(event.get("event") or "unknown")
        event_counts[name] = event_counts.get(name, 0) + 1
        if name == "open_rejected_first_side_hazard":
            reason = str(event.get("reason") or "unknown")
            first_side_reasons[reason] = first_side_reasons.get(reason, 0) + 1
    return {
        **overall,
        "open_rejected_first_side_hazard": event_counts.get("open_rejected_first_side_hazard", 0),
        "first_side_hazard_reasons": json.dumps(first_side_reasons, sort_keys=True),
        "open_rejected_per_market_residual_budget": event_counts.get(
            "open_rejected_per_market_residual_budget", 0
        ),
        "open_rejected_max_open_cost_below_minimum": event_counts.get(
            "open_rejected_max_open_cost_below_minimum", 0
        ),
        "open_rejected_residual_quarantine": event_counts.get("open_rejected_residual_quarantine", 0),
        "open_rejected_coverage_profile_filter": event_counts.get(
            "open_rejected_coverage_profile_filter", 0
        ),
        "open_rejected_side_profile_filter": event_counts.get("open_rejected_side_profile_filter", 0),
        "completion_rejected_pair_cost": event_counts.get("completion_rejected_pair_cost", 0),
        "queue_proxy_touch_insufficient_depth": event_counts.get("queue_proxy_touch_insufficient_depth", 0),
        "open_skipped_active_residual": event_counts.get("open_skipped_active_residual", 0),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-csv", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--yes-hazard-threshold", type=int, action="append", default=[])
    p.add_argument("--yes-remaining-max-s", type=float, action="append", default=[])
    p.add_argument("--max-open-cost-usdc", type=float, action="append", default=[])
    p.add_argument("--pair-cost-cap", type=float, action="append", default=[])
    p.add_argument("--include-no-quarantine", action="store_true")
    p.add_argument("--include-primary-35-65-profile", action="store_true")
    p.add_argument("--include-no-only-profile", action="store_true")
    p.add_argument("--residual-budget-usdc", type=float, default=3.0)
    p.add_argument("--residual-discount-s", type=float, default=15.0)
    p.add_argument("--hard-timeout-s", type=float, default=180.0)
    p.add_argument("--queue-conversion", type=float, default=shadow.PipelineConfig.queue_conversion)
    p.add_argument("--queue-ahead-multiplier", type=float, default=shadow.PipelineConfig.queue_ahead_multiplier)
    p.add_argument("--max-shadow-qty", type=float, default=shadow.PipelineConfig.max_shadow_qty)
    p.add_argument("--min-shadow-qty", type=float, default=shadow.PipelineConfig.min_shadow_qty)
    p.add_argument("--residual-rate-target", type=float, default=shadow.PipelineConfig.residual_rate_target)
    p.add_argument("--bad-pair-cost-target", type=float, default=shadow.PipelineConfig.bad_pair_cost_target)
    p.add_argument("--min-markets-for-review", type=int, default=shadow.PipelineConfig.min_markets_for_review)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    rows = shadow.load_csv(Path(args.input_csv))
    yes_thresholds = args.yes_hazard_threshold or [0, 1, 25, 100]
    yes_remaining_maxes = args.yes_remaining_max_s or [0.0, 30.0, 45.0]
    max_open_costs = args.max_open_cost_usdc or [2.25, 2.5, 3.0, 4.0]
    pair_caps = args.pair_cost_cap or [0.975, 0.995]
    gate_profiles = (
        GATE_PROFILES if args.include_primary_35_65_profile else {"all_gates": ()}
    )
    side_profiles = SIDE_PROFILES if args.include_no_only_profile else {"all_sides": ()}
    quarantine_values = [True]
    if args.include_no_quarantine:
        quarantine_values = [False, True]
    results: list[dict[str, Any]] = []

    for (
        yes_threshold,
        yes_remaining_max_s,
        yes_band_profile_name,
        max_open_cost,
        pair_cost_cap,
        quarantine,
        gate_profile_name,
        side_profile_name,
    ) in itertools.product(
        yes_thresholds,
        yes_remaining_maxes,
        YES_BAND_PROFILES,
        max_open_costs,
        pair_caps,
        quarantine_values,
        gate_profiles,
        side_profiles,
    ):
        cfg = shadow.PipelineConfig(
            queue_conversion=args.queue_conversion,
            queue_ahead_multiplier=args.queue_ahead_multiplier,
            max_shadow_qty=args.max_shadow_qty,
            min_shadow_qty=args.min_shadow_qty,
            pair_cost_cap=pair_cost_cap,
            residual_discount_s=args.residual_discount_s,
            hard_timeout_s=args.hard_timeout_s,
            quarantine_after_residual_discount=quarantine,
            allowed_coverage_gates=gate_profiles[gate_profile_name],
            allowed_open_sides=side_profiles[side_profile_name],
            per_market_residual_budget_usdc=args.residual_budget_usdc,
            max_open_cost_usdc=max_open_cost,
            reject_yes_open_after_up_residual_events=yes_threshold,
            reject_yes_open_when_remaining_gt_s=yes_remaining_max_s,
            reject_yes_open_in_price_bands=YES_BAND_PROFILES[yes_band_profile_name],
            residual_rate_target=args.residual_rate_target,
            bad_pair_cost_target=args.bad_pair_cost_target,
            min_markets_for_review=args.min_markets_for_review,
        )
        row = run_variant(rows, cfg)
        variant_id = (
            f"yh{yes_threshold}_yr{int(yes_remaining_max_s)}_{yes_band_profile_name}_"
            f"oc{max_open_cost:g}_pc{pair_cost_cap:.3f}_q{int(quarantine)}_"
            f"{gate_profile_name}_{side_profile_name}"
        )
        row.update(
            {
                "variant_id": variant_id,
                "yes_hazard_threshold": yes_threshold,
                "yes_remaining_max_s": yes_remaining_max_s,
                "yes_band_profile": yes_band_profile_name,
                "reject_yes_open_in_price_bands": json.dumps(YES_BAND_PROFILES[yes_band_profile_name]),
                "per_market_residual_budget_usdc": args.residual_budget_usdc,
                "max_open_cost_usdc": max_open_cost,
                "pair_cost_cap": pair_cost_cap,
                "hard_timeout_s": args.hard_timeout_s,
                "residual_discount_s": args.residual_discount_s,
                "quarantine_after_residual_discount": quarantine,
                "coverage_profile": gate_profile_name,
                "side_profile": side_profile_name,
                "allowed_coverage_gates": json.dumps(gate_profiles[gate_profile_name]),
                "allowed_open_sides": json.dumps(side_profiles[side_profile_name]),
            }
        )
        row["passes_gate"] = (
            metric(row, "roi_proxy_conservative", -1.0) > 0.0
            and metric(row, "resid_rate", 1.0) <= args.residual_rate_target
            and metric(row, "bad_pc_ge_100_share", 1.0) <= args.bad_pair_cost_target
            and metric(row, "queue_proxy_opens", 0.0) > 0.0
        )
        results.append(row)

    results.sort(
        key=lambda row: (
            row["passes_gate"],
            metric(row, "roi_proxy_conservative", -1.0),
            -metric(row, "resid_rate", 1.0),
            metric(row, "queue_proxy_opens", 0.0),
        ),
        reverse=True,
    )
    passing = [row for row in results if row["passes_gate"]]
    best_by_roi = max(results, key=lambda row: metric(row, "roi_proxy_conservative", -999.0), default=None)
    best_by_residual = min(results, key=lambda row: metric(row, "resid_rate", 999.0), default=None)
    status = KEEP_STATUS if passing else BLOCKED_STATUS
    out = Path(args.output_dir)
    write_csv(out / "grid_results.csv", results)
    write_csv(out / "top10_by_roi.csv", results[:10])
    decision = {
        "status": status,
        "evidence_level": "research_proxy",
        "input_csv": str(Path(args.input_csv).resolve()),
        "variant_count": len(results),
        "pass_count": len(passing),
        "best_by_roi": best_by_roi,
        "best_by_residual": best_by_residual,
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "next_executable_action": (
            "replay_best_first_side_hazard_variant_as_full_artifact"
            if passing
            else "require_private_maker_telemetry_or_new_source_truth"
        ),
    }
    write_json(out / "decision_register.json", decision)
    print(
        json.dumps(
            {
                "status": status,
                "output_dir": str(out),
                "variant_count": len(results),
                "pass_count": len(passing),
                "best_roi": best_by_roi.get("roi_proxy_conservative") if best_by_roi else None,
                "best_resid_rate": best_by_residual.get("resid_rate") if best_by_residual else None,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
