#!/usr/bin/env python3
"""Run local no-order pre-open opposite-side support grid.

This grid evaluates a stricter research proxy: before a candidate first leg can
open, the same market must have a recent opposite-side public sell-touch proxy
with enough fillable quantity and acceptable pair cost.  It imports the
research-only runner, does not fetch data, does not import credentials, and
does not execute orders.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
from pathlib import Path
from typing import Any

import run_nagi_ce25_b27bc_maker_shadow as shadow


KEEP_STATUS = "KEEP_NAGI_CE25_B27BC_PRE_OPEN_SUPPORT_GRID_CANDIDATE_FOUND_RESEARCH_PROXY_NOT_READY"
BLOCKED_STATUS = "BLOCKED_NAGI_CE25_B27BC_PRE_OPEN_SUPPORT_GRID_NO_PASS_NOT_READY"

GATE_PROFILES: dict[str, tuple[str, ...]] = {
    "all_gates": (),
    "primary_35_65": ("ce25_last60_35_50_primary", "ce25_last60_50_65_primary"),
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


def bool_values(enabled: bool) -> list[bool]:
    return [False, True] if enabled else [False]


def metric(row: dict[str, Any], key: str, default: float) -> float:
    value = row.get(key)
    return float(value) if value is not None else default


def run_variant(rows: list[dict[str, str]], cfg: shadow.PipelineConfig) -> dict[str, Any]:
    pipeline = shadow.MakerShadowPipeline(cfg)
    pipeline.run(rows)
    summary_rows = pipeline.summary_rows()
    overall = next((row for row in summary_rows if row.get("window_key") == "ALL"), summary_rows[0])
    event_counts: dict[str, int] = {}
    for event in pipeline.events:
        name = str(event.get("event") or "unknown")
        event_counts[name] = event_counts.get(name, 0) + 1
    return {
        **overall,
        "open_rejected_no_recent_opposite_support": event_counts.get(
            "open_rejected_no_recent_opposite_support", 0
        ),
        "open_rejected_residual_quarantine": event_counts.get("open_rejected_residual_quarantine", 0),
        "open_rejected_coverage_profile_filter": event_counts.get(
            "open_rejected_coverage_profile_filter", 0
        ),
        "completion_rejected_pair_cost": event_counts.get("completion_rejected_pair_cost", 0),
        "queue_proxy_touch_insufficient_depth": event_counts.get("queue_proxy_touch_insufficient_depth", 0),
        "open_skipped_active_residual": event_counts.get("open_skipped_active_residual", 0),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-csv", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--lookback-s", type=float, action="append", default=[])
    p.add_argument("--opposite-min-qty", type=float, action="append", default=[])
    p.add_argument("--opposite-pair-cost-cap", type=float, action="append", default=[])
    p.add_argument("--include-residual-quarantine", action="store_true")
    p.add_argument("--include-primary-35-65-profile", action="store_true")
    p.add_argument("--residual-discount-s", type=float, default=15.0)
    p.add_argument("--hard-timeout-s", type=float, default=180.0)
    p.add_argument("--pair-cost-cap", type=float, default=0.975)
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
    lookbacks = args.lookback_s or [15.0, 30.0, 60.0]
    min_qtys = args.opposite_min_qty or [5.0, 10.0, 20.0]
    support_pair_caps = args.opposite_pair_cost_cap or [0.95, 0.975, 0.995]
    gate_profiles = (
        GATE_PROFILES if args.include_primary_35_65_profile else {"all_gates": ()}
    )
    results: list[dict[str, Any]] = []

    for lookback_s, opposite_min_qty, support_pair_cap, quarantine, gate_profile_name in itertools.product(
        lookbacks,
        min_qtys,
        support_pair_caps,
        bool_values(args.include_residual_quarantine),
        gate_profiles,
    ):
        cfg = shadow.PipelineConfig(
            queue_conversion=args.queue_conversion,
            queue_ahead_multiplier=args.queue_ahead_multiplier,
            max_shadow_qty=args.max_shadow_qty,
            min_shadow_qty=args.min_shadow_qty,
            residual_discount_s=args.residual_discount_s,
            hard_timeout_s=args.hard_timeout_s,
            pair_cost_cap=args.pair_cost_cap,
            quarantine_after_residual_discount=quarantine,
            allowed_coverage_gates=gate_profiles[gate_profile_name],
            require_opposite_support_before_open=True,
            opposite_support_lookback_s=lookback_s,
            opposite_support_min_qty=opposite_min_qty,
            opposite_support_pair_cost_cap=support_pair_cap,
            residual_rate_target=args.residual_rate_target,
            bad_pair_cost_target=args.bad_pair_cost_target,
            min_markets_for_review=args.min_markets_for_review,
        )
        row = run_variant(rows, cfg)
        variant_id = (
            f"lb{int(lookback_s)}_oq{int(opposite_min_qty)}_"
            f"opc{support_pair_cap:.3f}_q{int(quarantine)}_{gate_profile_name}"
        )
        row.update(
            {
                "variant_id": variant_id,
                "lookback_s": lookback_s,
                "opposite_min_qty": opposite_min_qty,
                "opposite_pair_cost_cap": support_pair_cap,
                "quarantine_after_residual_discount": quarantine,
                "coverage_profile": gate_profile_name,
                "allowed_coverage_gates": json.dumps(gate_profiles[gate_profile_name]),
                "residual_discount_s": args.residual_discount_s,
                "hard_timeout_s": args.hard_timeout_s,
                "pair_cost_cap": args.pair_cost_cap,
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
            "replay_best_pre_open_support_variant_as_full_artifact"
            if passing
            else "tighten_support_features_or_require_private_maker_telemetry"
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
