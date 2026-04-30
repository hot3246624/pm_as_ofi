#!/usr/bin/env python3
"""Compare PGT shadow report against xuan target profile from research docs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT_DIR = REPO_ROOT / "data" / "replay_reports"

XUAN_TARGETS = {
    "clean_closed_episode_ratio": 0.913,
    "same_side_add_qty_ratio": 0.105,
    "merge_window_rel_s_low": -25.0,
    "merge_window_rel_s_high": -18.0,
    "redeem_window_rel_s_low": 35.0,
    "redeem_window_rel_s_high": 50.0,
}

PGT_SHADOW_GATES = {
    "clean_closed_episode_ratio_min": 0.90,
    "same_side_add_qty_ratio_max": 0.15,
    "episode_close_delay_p90_max": 100.0,
    "p90_first_completion_delay_s_max": 100.0,
    "summary_pair_cost_median_max": 1.00,
    "summary_pair_cost_p90_max": 1.02,
    "summary_pair_cost_gt_1_02_ratio_max": 0.0,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD; resolves data/replay_reports/pgt_shadow_report_<date>.json")
    p.add_argument("--report-json", help="Direct path to pgt shadow report json")
    p.add_argument("--output-json", help="Optional explicit output path")
    p.add_argument("--min-end-ts", type=int, help="Only include slugs with trailing end_ts >= this value")
    p.add_argument("--max-end-ts", type=int, help="Only include slugs with trailing end_ts <= this value")
    p.add_argument(
        "--include-incomplete",
        action="store_true",
        help="include rows without pgt_shadow_summary/round_complete",
    )
    return p.parse_args()


def median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * pct / 100.0
    lo = int(rank)
    hi = min(lo + 1, len(values) - 1)
    frac = rank - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def resolve_report_path(args: argparse.Namespace) -> Path:
    if args.report_json:
        return Path(args.report_json)
    if not args.date:
        raise SystemExit("either --date or --report-json is required")
    instance_id = os.environ.get("PM_INSTANCE_ID", "").strip()
    report_dir = DEFAULT_REPORT_DIR / instance_id if instance_id else DEFAULT_REPORT_DIR
    return report_dir / f"pgt_shadow_report_{args.date}.json"


def slug_end_ts(slug: str) -> int | None:
    tail = slug.rsplit("-", 1)[-1]
    try:
        return int(tail)
    except Exception:
        return None


def filter_rows(
    rows: list[dict[str, Any]],
    min_end_ts: int | None = None,
    max_end_ts: int | None = None,
    include_incomplete: bool = False,
) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if not include_incomplete:
            complete = row.get("round_complete", row.get("has_shadow_summary", 1.0))
            if complete is not None and float(complete or 0.0) <= 0.5:
                continue
        end_ts = slug_end_ts(str(row.get("slug") or ""))
        if end_ts is None:
            continue
        if min_end_ts is not None and end_ts < min_end_ts:
            continue
        if max_end_ts is not None and end_ts > max_end_ts:
            continue
        out.append(row)
    return out


def build_gap(
    payload: dict[str, Any],
    min_end_ts: int | None = None,
    max_end_ts: int | None = None,
    include_incomplete: bool = False,
) -> dict[str, Any]:
    rows = filter_rows(
        payload.get("rows") or [],
        min_end_ts,
        max_end_ts,
        include_incomplete=include_incomplete,
    )

    def collect(key: str) -> list[float]:
        out = []
        for row in rows:
            value = row.get(key)
            if value is None:
                continue
            out.append(float(value))
        return out

    def collect_active(key: str) -> list[float]:
        out = []
        for row in rows:
            if float(row.get("has_active_episode") or 0.0) <= 0.5:
                continue
            value = row.get(key)
            if value is None:
                continue
            out.append(float(value))
        return out

    def collect_active_pair_cost() -> list[float]:
        out = []
        for row in rows:
            if float(row.get("has_active_episode") or 0.0) <= 0.5:
                continue
            paired_qty = row.get("summary_paired_qty")
            if paired_qty is not None and float(paired_qty or 0.0) <= 1e-9:
                continue
            value = row.get("summary_pair_cost")
            if value is None:
                continue
            value = float(value)
            if value <= 0.0:
                continue
            out.append(value)
        return out

    median_clean = median(collect("clean_closed_episode_ratio"))
    median_same_side = median(collect("same_side_add_qty_ratio"))
    median_delay_p90 = median(collect("episode_close_delay_p90"))
    median_merge_first = median(collect("merge_requested_first_rel_s"))
    median_redeem_first = median(collect("redeem_requested_first_rel_s"))
    pair_costs = collect_active_pair_cost()
    median_pair_cost = median(pair_costs)
    p90_pair_cost = percentile(pair_costs, 90.0)
    max_pair_cost = max(pair_costs) if pair_costs else None
    pair_cost_gt_1_00_rounds = sum(1 for value in pair_costs if value > 1.00 + 1e-9)
    pair_cost_gt_1_01_rounds = sum(1 for value in pair_costs if value > 1.01 + 1e-9)
    pair_cost_gt_1_02_rounds = sum(1 for value in pair_costs if value > 1.02 + 1e-9)
    pair_cost_gt_1_02_ratio = (
        pair_cost_gt_1_02_rounds / len(pair_costs) if pair_costs else None
    )
    median_single_seed_flip_count = median(collect("single_seed_flip_count"))
    median_dual_seed_quotes = median(collect("dual_seed_quotes"))
    median_taker_shadow_would_open = median(collect("taker_shadow_would_open"))
    median_dispatch_taker_open = median(collect("dispatch_taker_open"))
    median_first_seed_accept = median(collect("first_seed_accept_rel_s"))
    median_dual_seed_accept = median(collect("dual_seed_accept_rel_s"))
    median_first_buy_fill = median(collect("first_buy_fill_rel_s"))
    median_first_seed_to_fill = median(collect("first_seed_to_first_fill_s"))
    median_first_completion_delay = median(collect("first_completion_delay_s"))
    p90_first_completion_delay = percentile(collect("first_completion_delay_s"), 90.0)
    median_seed_live = median(collect("seed_live_before_first_fill_or_cancel_s"))
    active_episode_rounds = sum(
        1 for row in rows if float(row.get("has_active_episode") or 0.0) > 0.5
    )
    residual_rounds = sum(
        1 for row in rows if float(row.get("residual_round") or 0.0) > 0.5
    )
    taker_close_opportunity_rounds = sum(
        1 for row in rows if float(row.get("taker_shadow_would_close") or 0.0) > 0.0
    )
    taker_close_dispatched_rounds = sum(
        1 for row in rows if float(row.get("dispatch_taker_close") or 0.0) > 0.0
    )
    total_taker_shadow_would_close = sum(collect("taker_shadow_would_close"))
    total_dispatch_taker_close = sum(collect("dispatch_taker_close"))
    maker_only_missed_open_rounds = sum(
        1 for row in rows if float(row.get("maker_only_missed_open_round") or 0.0) > 0.5
    )
    single_seed_flip_rounds = sum(
        1 for row in rows if float(row.get("single_seed_flip_count") or 0.0) > 0.5
    )
    single_seed_released_to_dual_rounds = sum(
        1 for row in rows if float(row.get("single_seed_released_to_dual") or 0.0) > 0.5
    )
    seed_exposed_rounds = sum(
        1 for row in rows if row.get("first_seed_accept_rel_s") is not None
    )
    seed_exposed_fill_rounds = sum(
        1 for row in rows if row.get("first_buy_fill_rel_s") is not None
    )
    dual_seed_rounds = sum(
        1 for row in rows if row.get("dual_seed_accept_rel_s") is not None
    )

    summary = {
        "markets": len(rows),
        "filters": {
            "min_end_ts": min_end_ts,
            "max_end_ts": max_end_ts,
            "include_incomplete": include_incomplete,
        },
        "pgt_medians": {
            "clean_closed_episode_ratio": median_clean,
            "same_side_add_qty_ratio": median_same_side,
            "episode_close_delay_p90": median_delay_p90,
            "merge_requested_first_rel_s": median_merge_first,
            "redeem_requested_first_rel_s": median_redeem_first,
            "summary_pair_cost": median_pair_cost,
            "single_seed_flip_count": median_single_seed_flip_count,
            "dual_seed_quotes": median_dual_seed_quotes,
            "taker_shadow_would_open": median_taker_shadow_would_open,
            "dispatch_taker_open": median_dispatch_taker_open,
            "first_seed_accept_rel_s": median_first_seed_accept,
            "dual_seed_accept_rel_s": median_dual_seed_accept,
            "first_buy_fill_rel_s": median_first_buy_fill,
            "first_seed_to_first_fill_s": median_first_seed_to_fill,
            "first_completion_delay_s": median_first_completion_delay,
            "p90_first_completion_delay_s": p90_first_completion_delay,
            "seed_live_before_first_fill_or_cancel_s": median_seed_live,
        },
        "pgt_rates": {
            "active_episode_rounds": active_episode_rounds,
            "active_episode_ratio": (active_episode_rounds / len(rows)) if rows else None,
            "residual_rounds": residual_rounds,
            "residual_round_ratio": (residual_rounds / len(rows)) if rows else None,
            "taker_close_opportunity_rounds": taker_close_opportunity_rounds,
            "taker_close_dispatched_rounds": taker_close_dispatched_rounds,
            "taker_close_dispatch_round_ratio": (
                taker_close_dispatched_rounds / taker_close_opportunity_rounds
                if taker_close_opportunity_rounds
                else None
            ),
            "total_taker_shadow_would_close": total_taker_shadow_would_close,
            "total_dispatch_taker_close": total_dispatch_taker_close,
            "seed_exposed_rounds": seed_exposed_rounds,
            "seed_exposed_fill_rounds": seed_exposed_fill_rounds,
            "seed_exposed_fill_ratio": (
                seed_exposed_fill_rounds / seed_exposed_rounds
                if seed_exposed_rounds
                else None
            ),
            "dual_seed_rounds": dual_seed_rounds,
            "dual_seed_ratio": (
                dual_seed_rounds / seed_exposed_rounds if seed_exposed_rounds else None
            ),
        },
        "pgt_distributions": {
            "summary_pair_cost_rounds": len(pair_costs),
            "summary_pair_cost_p90": p90_pair_cost,
            "summary_pair_cost_max": max_pair_cost,
            "summary_pair_cost_gt_1_00_rounds": pair_cost_gt_1_00_rounds,
            "summary_pair_cost_gt_1_01_rounds": pair_cost_gt_1_01_rounds,
            "summary_pair_cost_gt_1_02_rounds": pair_cost_gt_1_02_rounds,
            "summary_pair_cost_gt_1_02_ratio": pair_cost_gt_1_02_ratio,
        },
        "xuan_targets": XUAN_TARGETS,
        "pgt_shadow_gates": PGT_SHADOW_GATES,
        "gap_vs_xuan": {
            "clean_closed_episode_ratio": None if median_clean is None else median_clean - XUAN_TARGETS["clean_closed_episode_ratio"],
            "same_side_add_qty_ratio": None if median_same_side is None else median_same_side - XUAN_TARGETS["same_side_add_qty_ratio"],
            "merge_requested_first_rel_s": None if median_merge_first is None else median_merge_first + 22.0,
            "redeem_requested_first_rel_s": None if median_redeem_first is None else median_redeem_first - 38.0,
        },
        "within_xuan_windows": {
            "merge_requested_first_rel_s": (
                median_merge_first is not None
                and XUAN_TARGETS["merge_window_rel_s_low"] <= median_merge_first <= XUAN_TARGETS["merge_window_rel_s_high"]
            ),
            "redeem_requested_first_rel_s": (
                median_redeem_first is not None
                and XUAN_TARGETS["redeem_window_rel_s_low"] <= median_redeem_first <= XUAN_TARGETS["redeem_window_rel_s_high"]
            ),
        },
        "passes_pgt_shadow_gate": {
            "clean_closed_episode_ratio": (
                median_clean is not None and median_clean >= PGT_SHADOW_GATES["clean_closed_episode_ratio_min"]
            ),
            "same_side_add_qty_ratio": (
                median_same_side is not None and median_same_side <= PGT_SHADOW_GATES["same_side_add_qty_ratio_max"]
            ),
            "episode_close_delay_p90": (
                median_delay_p90 is not None and median_delay_p90 <= PGT_SHADOW_GATES["episode_close_delay_p90_max"]
            ),
            "p90_first_completion_delay_s": (
                p90_first_completion_delay is not None
                and p90_first_completion_delay <= PGT_SHADOW_GATES["p90_first_completion_delay_s_max"]
            ),
            "summary_pair_cost_median": (
                median_pair_cost is not None
                and median_pair_cost <= PGT_SHADOW_GATES["summary_pair_cost_median_max"]
            ),
            "summary_pair_cost_p90": (
                p90_pair_cost is not None
                and p90_pair_cost <= PGT_SHADOW_GATES["summary_pair_cost_p90_max"]
            ),
            "summary_pair_cost_tail": (
                pair_cost_gt_1_02_ratio is not None
                and pair_cost_gt_1_02_ratio <= PGT_SHADOW_GATES["summary_pair_cost_gt_1_02_ratio_max"]
            ),
        },
        "counterfactual_readout": {
            "maker_only_missed_open_rounds": maker_only_missed_open_rounds,
            "maker_only_missed_open_ratio": (
                (maker_only_missed_open_rounds / len(rows)) if rows else None
            ),
            "single_seed_flip_rounds": single_seed_flip_rounds,
            "single_seed_flip_ratio": (
                (single_seed_flip_rounds / len(rows)) if rows else None
            ),
            "single_seed_released_to_dual_rounds": single_seed_released_to_dual_rounds,
            "single_seed_released_to_dual_ratio": (
                (single_seed_released_to_dual_rounds / len(rows)) if rows else None
            ),
            "median_taker_shadow_open_gap": (
                None
                if median_taker_shadow_would_open is None
                or median_dispatch_taker_open is None
                else median_taker_shadow_would_open - median_dispatch_taker_open
            ),
        },
    }
    return summary


def main() -> None:
    args = parse_args()
    report_path = resolve_report_path(args)
    if not report_path.exists():
        raise SystemExit(f"report json not found: {report_path}")
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    out = build_gap(
        payload,
        min_end_ts=args.min_end_ts,
        max_end_ts=args.max_end_ts,
        include_incomplete=args.include_incomplete,
    )

    if args.output_json:
        output_path = Path(args.output_json)
    else:
        suffix = report_path.stem.removeprefix("pgt_shadow_report_")
        if args.min_end_ts is not None:
            suffix = f"{suffix}_from_{args.min_end_ts}"
        if args.max_end_ts is not None:
            suffix = f"{suffix}_to_{args.max_end_ts}"
        output_path = report_path.with_name(f"xuan_pgt_gap_report_{suffix}.json")
    output_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
