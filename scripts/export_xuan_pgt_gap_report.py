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
    "same_side_add_qty_ratio_max": 0.10,
    "episode_close_delay_p90_max": 100.0,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD; resolves data/replay_reports/pgt_shadow_report_<date>.json")
    p.add_argument("--report-json", help="Direct path to pgt shadow report json")
    p.add_argument("--output-json", help="Optional explicit output path")
    return p.parse_args()


def median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def resolve_report_path(args: argparse.Namespace) -> Path:
    if args.report_json:
        return Path(args.report_json)
    if not args.date:
        raise SystemExit("either --date or --report-json is required")
    instance_id = os.environ.get("PM_INSTANCE_ID", "").strip()
    report_dir = DEFAULT_REPORT_DIR / instance_id if instance_id else DEFAULT_REPORT_DIR
    return report_dir / f"pgt_shadow_report_{args.date}.json"


def build_gap(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("rows") or []

    def collect(key: str) -> list[float]:
        out = []
        for row in rows:
            value = row.get(key)
            if value is None:
                continue
            out.append(float(value))
        return out

    median_clean = median(collect("clean_closed_episode_ratio"))
    median_same_side = median(collect("same_side_add_qty_ratio"))
    median_delay_p90 = median(collect("episode_close_delay_p90"))
    median_merge_first = median(collect("merge_requested_first_rel_s"))
    median_redeem_first = median(collect("redeem_requested_first_rel_s"))
    median_pair_cost = median(collect("summary_pair_cost"))
    median_single_seed_flip_count = median(collect("single_seed_flip_count"))
    median_dual_seed_quotes = median(collect("dual_seed_quotes"))
    median_taker_shadow_would_open = median(collect("taker_shadow_would_open"))
    median_dispatch_taker_open = median(collect("dispatch_taker_open"))
    maker_only_missed_open_rounds = sum(
        1 for row in rows if float(row.get("maker_only_missed_open_round") or 0.0) > 0.5
    )
    single_seed_flip_rounds = sum(
        1 for row in rows if float(row.get("single_seed_flip_count") or 0.0) > 0.5
    )
    single_seed_released_to_dual_rounds = sum(
        1 for row in rows if float(row.get("single_seed_released_to_dual") or 0.0) > 0.5
    )

    summary = {
        "markets": len(rows),
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
    out = build_gap(payload)

    if args.output_json:
        output_path = Path(args.output_json)
    else:
        suffix = report_path.stem.removeprefix("pgt_shadow_report_")
        output_path = report_path.with_name(f"xuan_pgt_gap_report_{suffix}.json")
    output_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
