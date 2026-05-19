#!/usr/bin/env python3
"""Compare D+ edge=0.07 Sunday calendar proxy across split summaries.

This is a manifest-only reducer. It does not read DuckDB/raw/replay paths,
start network processes, or submit orders.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_edge07_calendar_proxy_split_compare"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def compact(name: str, manifest: dict[str, Any], path: Path) -> dict[str, Any]:
    base = manifest["baseline_all"]
    candidate = manifest["candidate_exclude_sunday"]
    sunday = (manifest.get("weekday_summaries") or {}).get("sunday") or {}
    return {
        "name": name,
        "manifest_path": str(path),
        "status": manifest.get("status"),
        "labels": manifest.get("labels"),
        "days": manifest.get("days"),
        "row_count": manifest.get("row_count"),
        "market_count": manifest.get("market_count"),
        "public_account_truth_missing_days": manifest.get("public_account_truth_missing_days"),
        "baseline": {
            "strict_candidate_rows": base.get("strict_candidate_rows"),
            "pair_pnl": base.get("pair_pnl"),
            "net_pnl": base.get("net_pnl"),
            "residual_rate": base.get("residual_rate"),
            "worst_residual_net_pnl": base.get("worst_residual_net_pnl"),
            "stress100_worst_pnl": base.get("stress100_worst_pnl"),
            "pair_cost_wavg_proxy": base.get("pair_cost_wavg_proxy"),
            "no_repair_opportunity_per_seed": base.get("no_repair_opportunity_per_seed"),
        },
        "exclude_sunday": {
            "strict_candidate_rows": candidate.get("strict_candidate_rows"),
            "pair_pnl": candidate.get("pair_pnl"),
            "net_pnl": candidate.get("net_pnl"),
            "residual_rate": candidate.get("residual_rate"),
            "worst_residual_net_pnl": candidate.get("worst_residual_net_pnl"),
            "stress100_worst_pnl": candidate.get("stress100_worst_pnl"),
            "pair_cost_wavg_proxy": candidate.get("pair_cost_wavg_proxy"),
            "no_repair_opportunity_per_seed": candidate.get("no_repair_opportunity_per_seed"),
        },
        "sunday": {
            "strict_candidate_rows": sunday.get("strict_candidate_rows"),
            "pair_pnl": sunday.get("pair_pnl"),
            "net_pnl": sunday.get("net_pnl"),
            "residual_rate": sunday.get("residual_rate"),
            "worst_residual_net_pnl": sunday.get("worst_residual_net_pnl"),
            "stress100_worst_pnl": sunday.get("stress100_worst_pnl"),
            "pair_cost_wavg_proxy": sunday.get("pair_cost_wavg_proxy"),
            "no_repair_opportunity_per_seed": sunday.get("no_repair_opportunity_per_seed"),
        },
        "scoreboard_delta": manifest.get("scoreboard_delta"),
    }


def f(row: dict[str, Any], path: list[str]) -> float:
    value: Any = row
    for key in path:
        value = (value or {}).get(key)
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare D+ edge=0.07 Sunday proxy split summaries.")
    parser.add_argument("--full-manifest", required=True)
    parser.add_argument("--covered-manifest", required=True)
    parser.add_argument("--uncovered-manifest", required=True)
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    full_path = Path(args.full_manifest)
    covered_path = Path(args.covered_manifest)
    uncovered_path = Path(args.uncovered_manifest)
    full = compact("full_allowed", load(full_path), full_path)
    covered = compact("public_audit_covered_20260502_20260513", load(covered_path), covered_path)
    uncovered = compact("public_audit_uncovered_20260516_20260517", load(uncovered_path), uncovered_path)
    splits = {
        "full_allowed": full,
        "public_audit_covered": covered,
        "public_audit_uncovered": uncovered,
    }
    scoreboard = {
        "full_stress_delta": f(full, ["scoreboard_delta", "exclude_sunday_stress_delta"]),
        "covered_stress_delta": f(covered, ["scoreboard_delta", "exclude_sunday_stress_delta"]),
        "uncovered_stress_delta": f(uncovered, ["scoreboard_delta", "exclude_sunday_stress_delta"]),
        "full_row_retention": f(full, ["scoreboard_delta", "exclude_sunday_row_retention"]),
        "covered_row_retention": f(covered, ["scoreboard_delta", "exclude_sunday_row_retention"]),
        "uncovered_row_retention": f(uncovered, ["scoreboard_delta", "exclude_sunday_row_retention"]),
        "full_exclude_sunday_stress": f(full, ["exclude_sunday", "stress100_worst_pnl"]),
        "covered_exclude_sunday_stress": f(covered, ["exclude_sunday", "stress100_worst_pnl"]),
        "uncovered_exclude_sunday_stress": f(uncovered, ["exclude_sunday", "stress100_worst_pnl"]),
        "full_sunday_stress": f(full, ["sunday", "stress100_worst_pnl"]),
        "covered_sunday_stress": f(covered, ["sunday", "stress100_worst_pnl"]),
        "uncovered_sunday_stress": f(uncovered, ["sunday", "stress100_worst_pnl"]),
    }
    covered_ok = (
        scoreboard["covered_stress_delta"] > 0.0
        and scoreboard["covered_exclude_sunday_stress"] > 0.0
        and scoreboard["covered_row_retention"] >= 0.80
    )
    uncovered_ok_but_thin = (
        scoreboard["uncovered_stress_delta"] > 0.0
        and scoreboard["uncovered_exclude_sunday_stress"] > 0.0
        and scoreboard["uncovered_row_retention"] < 0.70
    )
    if covered_ok and uncovered_ok_but_thin:
        status = "KEEP_SUNDAY_PROXY_RESEARCH_ONLY_SPLIT_MIXED"
        next_action = (
            "validate Sunday proxy with source-of-truth replay or no-order shadow proxy; "
            "uncovered split becomes positive only after dropping Sunday and is too thin for promotion"
        )
    elif covered_ok:
        status = "KEEP_SUNDAY_PROXY_COVERED_ONLY_RESEARCH_LEAD"
        next_action = "find a non-calendar proxy for uncovered tail before no-order shadow"
    else:
        status = "DISCARD_SUNDAY_PROXY_NOT_SPLIT_STABLE"
        next_action = "kill Sunday proxy and test another pre-trade no-repair-opportunity risk signal"

    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "strategy": "xuan_b27_dplus",
        "status": status,
        "hypothesis": (
            "the Sunday calendar proxy should improve the edge=0.07 repair lead in covered and uncovered splits, "
            "not just the full aggregate"
        ),
        "splits": splits,
        "scoreboard_delta": scoreboard,
        "can_support_strategy_promotion": False,
        "requires_source_of_truth_replay_for_promotion": True,
        "conclusion_scope": (
            "manifest-only split comparison of local completion-store attribution summaries; "
            "not private owner truth, not source-of-truth replay, not deployable as-is"
        ),
        "next_action": next_action,
        "raw_replay_scanned": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(manifest_path)
    print(status)
    print(next_action)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
