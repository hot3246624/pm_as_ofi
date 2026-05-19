#!/usr/bin/env python3
"""Summarize deployable calendar proxies for the D+ edge=0.07 repair lead.

This reducer reads an existing local failure-decomposition CSV. It does not
read raw/replay data, start network processes, or submit orders.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable


ARTIFACT = "xuan_b27_dplus_edge07_calendar_proxy_summary"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def f(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def summarize(
    name: str,
    rows: list[dict[str, Any]],
    predicate: Callable[[dict[str, Any]], bool],
) -> dict[str, Any]:
    selected = [row for row in rows if predicate(row)]
    additive = [
        "strict_candidate_rows",
        "active_markets",
        "seed_actions",
        "repair_actions",
        "pair_actions",
        "gross_buy_qty",
        "gross_buy_cost",
        "repair_qty",
        "repair_cost",
        "pair_qty",
        "residual_qty",
        "residual_cost",
        "pair_pnl",
        "net_pnl",
        "worst_residual_net_pnl",
        "stress100_worst_pnl",
        "stress_fee_drag",
        "repair_no_opportunity",
        "repair_block_pair_cap",
        "risk_block_without_repair",
        "negative_stress_contribution",
        "negative_worst_residual_contribution",
        "residual_winner_qty",
        "residual_loser_qty",
        "residual_winner_cost",
        "residual_loser_cost",
    ]
    out: dict[str, Any] = {
        "name": name,
        "market_count": len(selected),
        "negative_stress_market_count": sum(1 for row in selected if f(row, "stress100_worst_pnl") < 0),
        "worst_residual_negative_market_count": sum(
            1 for row in selected if f(row, "worst_residual_net_pnl") < 0
        ),
    }
    for key in additive:
        out[key] = round(sum(f(row, key) for row in selected), 6)
    out["residual_rate"] = (
        round(out["residual_qty"] / out["gross_buy_qty"], 6)
        if out["gross_buy_qty"]
        else 0.0
    )
    out["pair_cost_wavg_proxy"] = (
        round(1.0 - out["pair_pnl"] / out["pair_qty"], 6)
        if out["pair_qty"]
        else 0.0
    )
    out["no_repair_opportunity_per_seed"] = (
        round(out["repair_no_opportunity"] / out["seed_actions"], 6)
        if out["seed_actions"]
        else 0.0
    )
    out["pair_cap_block_per_seed"] = (
        round(out["repair_block_pair_cap"] / out["seed_actions"], 6)
        if out["seed_actions"]
        else 0.0
    )
    return out


def weekday(row: dict[str, Any]) -> int:
    return date.fromisoformat(str(row["day"])).weekday()


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize D+ edge=0.07 calendar proxy filters.")
    parser.add_argument("--market-failures-csv", required=True)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    market_csv = Path(args.market_failures_csv)
    source_manifest = Path(args.source_manifest)
    source = json.loads(source_manifest.read_text(encoding="utf-8"))
    rows = load_csv(market_csv)

    filters: list[tuple[str, Callable[[dict[str, Any]], bool]]] = [
        ("all", lambda row: True),
        ("exclude_2026_05_17", lambda row: row.get("day") != "2026-05-17"),
        ("exclude_sunday", lambda row: weekday(row) != 6),
        ("exclude_weekend", lambda row: weekday(row) < 5),
        ("covered_only_20260502_20260513", lambda row: str(row.get("day")) <= "2026-05-13"),
        ("uncovered_only_20260516_20260517", lambda row: str(row.get("day")) >= "2026-05-16"),
    ]
    weekday_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    filters.extend(
        (f"weekday_{name}", lambda row, idx=idx: weekday(row) == idx)
        for idx, name in enumerate(weekday_names)
    )
    summaries = [summarize(name, rows, predicate) for name, predicate in filters]
    by_name = {row["name"]: row for row in summaries}
    baseline = by_name["all"]
    exclude_sunday = by_name["exclude_sunday"]
    deltas = {
        "exclude_sunday_stress_delta": round(
            exclude_sunday["stress100_worst_pnl"] - baseline["stress100_worst_pnl"], 6
        ),
        "exclude_sunday_worst_residual_delta": round(
            exclude_sunday["worst_residual_net_pnl"] - baseline["worst_residual_net_pnl"], 6
        ),
        "exclude_sunday_residual_rate_delta": round(
            exclude_sunday["residual_rate"] - baseline["residual_rate"], 6
        ),
        "exclude_sunday_row_retention": round(
            exclude_sunday["strict_candidate_rows"] / baseline["strict_candidate_rows"], 6
        )
        if baseline["strict_candidate_rows"]
        else 0.0,
        "exclude_sunday_pair_pnl_retention": round(
            exclude_sunday["pair_pnl"] / baseline["pair_pnl"], 6
        )
        if baseline["pair_pnl"]
        else 0.0,
    }
    if (
        deltas["exclude_sunday_stress_delta"] > 20.0
        and exclude_sunday["strict_candidate_rows"] >= 100_000
        and exclude_sunday["stress100_worst_pnl"] > 0.0
        and exclude_sunday["pair_pnl"] > 0.0
    ):
        status = "KEEP_CALENDAR_PROXY_RESEARCH_ONLY"
        next_action = (
            "validate Sunday/no-repair tail proxy on source-of-truth replay or no-order shadow proxy; "
            "do not promote from local completion-store attribution alone"
        )
    else:
        status = "DISCARD_CALENDAR_PROXY_NO_MATERIAL_IMPROVEMENT"
        next_action = "kill calendar proxy and test another deployable no-repair-opportunity admission signal"

    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries_path = output_dir / "calendar_summaries.json"
    summaries_path.write_text(json.dumps(summaries, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "strategy": "xuan_b27_dplus",
        "status": status,
        "hypothesis": (
            "a deployable day-of-week calendar proxy can avoid the no-repair-opportunity tail "
            "localized by edge=0.07 attribution"
        ),
        "source_manifest": str(source_manifest),
        "source_market_failures_csv": str(market_csv),
        "data_root": source.get("data_root"),
        "dataset_type": "metadata_reducer_over_local_edge07_failure_decomposition",
        "labels": source.get("labels"),
        "days": source.get("days"),
        "market_prefix": source.get("market_prefix"),
        "assets": source.get("assets"),
        "row_count": source.get("row_count"),
        "market_count": source.get("market_count"),
        "excluded_20260514_20260515": bool(source.get("excluded_20260514_20260515")),
        "contains_20260518": bool(source.get("contains_20260518")),
        "includes_public_account_execution_truth_v1": bool(
            source.get("includes_public_account_execution_truth_v1")
        ),
        "public_account_truth_missing_days": source.get("public_account_truth_missing_days"),
        "baseline_all": baseline,
        "candidate_exclude_sunday": exclude_sunday,
        "weekday_summaries": {
            row["name"].replace("weekday_", ""): row
            for row in summaries
            if str(row["name"]).startswith("weekday_")
        },
        "scoreboard_delta": deltas,
        "can_support_strategy_promotion": False,
        "requires_source_of_truth_replay_for_promotion": True,
        "conclusion_scope": (
            "calendar metadata reducer over local strict/cache plus completion-store attribution; "
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
        "outputs": {
            "calendar_summaries_json": str(summaries_path),
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
