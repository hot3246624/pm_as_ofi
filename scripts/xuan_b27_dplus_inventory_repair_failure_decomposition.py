#!/usr/bin/env python3
"""Decompose remaining D+ inventory-repair stress failures.

This offline probe replays one compliant inventory-repair profile over local
strict/cache plus completion-store inputs, then decomposes remaining negative
stress by market/day/side/opportunity buckets. It does not read raw/replay
paths, start network processes, or submit orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import xuan_b27_dplus_compliant_inventory_repair_probe as repair
import xuan_b27_dplus_residual_cooldown_compliant_metrics_runner as base


ARTIFACT = "xuan_b27_dplus_inventory_repair_failure_decomposition"


def bucket(value: float, cuts: list[float]) -> str:
    last = "-inf"
    for cut in cuts:
        if value <= cut:
            return f"({last},{cut:g}]"
        last = f"{cut:g}"
    return f"({last},inf)"


def make_profile(args: argparse.Namespace) -> repair.RepairProfile:
    return repair.RepairProfile(
        name=repair.profile_name(
            args.target_qty,
            args.repair_after_s,
            args.repair_max_qty,
            args.repair_pair_cap,
            args.repair_budget_mode,
            args.pair_select,
            args.edge,
        ),
        edge=args.edge,
        target_qty=args.target_qty,
        repair_after_s=args.repair_after_s,
        repair_max_qty=args.repair_max_qty,
        repair_pair_cap=args.repair_pair_cap,
        repair_budget_mode=args.repair_budget_mode,
        repair_budget_fraction=args.repair_budget_fraction,
        min_edge_per_pair=args.min_edge_per_pair,
        pair_select=args.pair_select,
        block_risk_increasing_after_repair_threshold=True,
    )


def group_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = f"{base.dashed_day(str(row.get('day') or ''))}:{row.get('condition_id')}"
        grouped[key].append(row)
    return dict(grouped)


def residual_side_summary(state: base.State, winner: str | None) -> dict[str, Any]:
    assert state.inv is not None
    yes_qty = base.lot_qty(state.inv["YES"])
    no_qty = base.lot_qty(state.inv["NO"])
    yes_cost = base.lot_cost(state.inv["YES"])
    no_cost = base.lot_cost(state.inv["NO"])
    if yes_qty > 1e-9 and no_qty > 1e-9:
        residual_side = "MIXED"
    elif yes_qty > 1e-9:
        residual_side = "YES"
    elif no_qty > 1e-9:
        residual_side = "NO"
    else:
        residual_side = "FLAT"
    residual_qty = yes_qty + no_qty
    residual_cost = yes_cost + no_cost
    residual_winner_qty = 0.0
    residual_loser_qty = 0.0
    residual_winner_cost = 0.0
    residual_loser_cost = 0.0
    for side, qty, cost in (("YES", yes_qty, yes_cost), ("NO", no_qty, no_cost)):
        if side == winner:
            residual_winner_qty += qty
            residual_winner_cost += cost
        else:
            residual_loser_qty += qty
            residual_loser_cost += cost
    return {
        "residual_side": residual_side,
        "residual_yes_qty": round(yes_qty, 6),
        "residual_no_qty": round(no_qty, 6),
        "residual_yes_cost": round(yes_cost, 6),
        "residual_no_cost": round(no_cost, 6),
        "residual_winner_qty": round(residual_winner_qty, 6),
        "residual_loser_qty": round(residual_loser_qty, 6),
        "residual_winner_cost": round(residual_winner_cost, 6),
        "residual_loser_cost": round(residual_loser_cost, 6),
        "residual_cost_wavg": round(residual_cost / residual_qty, 6) if residual_qty else 0.0,
        "residual_side_matches_winner": residual_side == winner if residual_side in {"YES", "NO"} else None,
    }


def simulate_market(
    key: str,
    rows: list[dict[str, Any]],
    profile: repair.RepairProfile,
    public_keys: set[tuple[str, str]],
) -> dict[str, Any]:
    rows = sorted(rows, key=lambda row: (int(row.get("trigger_ts_ms") or 0), str(row.get("first_side") or "")))
    first = rows[0]
    day = base.dashed_day(str(first.get("day") or ""))
    condition_id = str(first.get("condition_id") or "")
    state = base.State(
        winner=str(first.get("winner_side")) if first.get("winner_side") else None,
        day=day,
        slug=str(first.get("slug") or ""),
    )
    metrics: defaultdict[str, float] = defaultdict(float)
    metrics["strict_candidate_rows"] = len(rows)
    market_public_key = (day, condition_id)
    metrics["public_audit_covered_markets"] = 1 if market_public_key in public_keys else 0
    for row in rows:
        if market_public_key in public_keys:
            metrics["public_audit_covered_candidates"] += 1
        if row.get("completion_min_opp_ask_30s") is not None:
            metrics["completion_joined_candidates"] += 1
        pair_cost = row.get("completion_min_pair_cost_30s")
        if pair_cost is not None and float(pair_cost) <= 1.0:
            metrics["completion_pair_le_100_candidates"] += 1
        if state.winner is None and row.get("winner_side"):
            state.winner = str(row.get("winner_side"))
        state.last_ts_ms = max(state.last_ts_ms, int(row.get("trigger_ts_ms") or 0))
        repair.maybe_seed_with_repair(row, profile, state, metrics)
    base.settle({key: state}, profile, metrics)
    metrics["repair_budget_available_end"] += repair.repair_budget_available(state)
    result = repair.finish(profile, metrics)
    stress_fee_drag = 0.01 * (2.0 * float(result["pair_qty"]) + float(result["residual_qty"]))
    out = {
        "day": day,
        "condition_id": condition_id,
        "slug": state.slug,
        "winner_side": state.winner,
        "public_audit_covered": market_public_key in public_keys,
        **result,
        "stress_fee_drag": round(stress_fee_drag, 6),
        "stress_negative": float(result["stress100_worst_pnl"]) < 0.0,
        "worst_residual_negative": float(result["worst_residual_net_pnl"]) < 0.0,
        "negative_stress_contribution": round(min(0.0, float(result["stress100_worst_pnl"])), 6),
        "negative_worst_residual_contribution": round(min(0.0, float(result["worst_residual_net_pnl"])), 6),
    }
    out.update(residual_side_summary(state, state.winner))
    out.update(
        {
            "residual_cost_bucket": bucket(float(result["residual_cost"]), [0, 0.5, 1, 2, 4, 6]),
            "residual_qty_bucket": bucket(float(result["residual_qty"]), [0, 1, 2, 4, 8, 12]),
            "pair_cost_wavg_bucket": bucket(float(result["pair_cost_wavg"]), [0.88, 0.90, 0.92, 0.94, 0.96, 1.00]),
            "pair_qty_bucket": bucket(float(result["pair_qty"]), [0, 1, 2, 4, 8, 12]),
            "repair_opportunity_bucket": (
                "no_repair_opportunity"
                if int(result.get("repair_no_opportunity") or 0) > 0
                else "repair_opportunity_seen"
            ),
            "repair_pair_cap_bucket": (
                "pair_cap_blocked"
                if int(result.get("repair_block_pair_cap") or 0) > 0
                else "no_pair_cap_block"
            ),
            "risk_block_bucket": (
                "risk_block_without_repair"
                if int(result.get("risk_block_without_repair") or 0) > 0
                else "no_unrepaired_risk_block"
            ),
            "stress_driver_hint": "fee_drag" if stress_fee_drag > abs(float(result["worst_residual_net_pnl"])) else "residual_cost",
        }
    )
    return out


def sum_group(rows: list[dict[str, Any]], fields: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {field: rows[0].get(field) for field in fields}
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
        "seed_block_risk_increasing_repair_mode",
        "negative_stress_contribution",
        "negative_worst_residual_contribution",
        "residual_winner_qty",
        "residual_loser_qty",
        "residual_winner_cost",
        "residual_loser_cost",
    ]
    for key in additive:
        out[key] = round(sum(float(row.get(key) or 0.0) for row in rows), 6)
    out["market_count"] = len(rows)
    out["negative_stress_market_count"] = sum(1 for row in rows if row.get("stress_negative"))
    out["worst_residual_negative_market_count"] = sum(
        1 for row in rows if row.get("worst_residual_negative")
    )
    out["residual_rate"] = round(out["residual_qty"] / out["gross_buy_qty"], 6) if out["gross_buy_qty"] else 0.0
    out["pair_cost_wavg_proxy"] = round(
        1.0 - out["pair_pnl"] / out["pair_qty"], 6
    ) if out["pair_qty"] else 0.0
    return out


def group_summary(markets: list[dict[str, Any]], fields: list[str], limit: int = 20) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in markets:
        grouped[tuple(row.get(field) for field in fields)].append(row)
    rows = [sum_group(group, fields) for group in grouped.values()]
    rows.sort(key=lambda row: (row["stress100_worst_pnl"], row["worst_residual_net_pnl"]))
    return rows[:limit]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompose D+ inventory repair failure buckets.")
    parser.add_argument("--strict-root", default=str(base.DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(base.DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(base.DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--strict-labels", required=True)
    parser.add_argument("--completion-labels", required=True)
    parser.add_argument("--exclude-label-fragments", default="20260514,20260515,20260518")
    parser.add_argument("--edge", type=float, default=0.055)
    parser.add_argument("--target-qty", type=float, default=3.0)
    parser.add_argument("--repair-after-s", type=float, default=60.0)
    parser.add_argument("--repair-max-qty", type=float, default=1.25)
    parser.add_argument("--repair-pair-cap", type=float, default=1.02)
    parser.add_argument("--repair-budget-mode", default="none")
    parser.add_argument("--repair-budget-fraction", type=float, default=0.50)
    parser.add_argument("--min-edge-per-pair", type=float, default=0.005)
    parser.add_argument("--pair-select", default="fifo")
    parser.add_argument("--completion-window-ms", type=int, default=30_000)
    parser.add_argument("--max-strict-candidates-per-plan", type=int, default=0)
    parser.add_argument("--output-dir")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    strict_root = Path(args.strict_root)
    completion_root = Path(args.completion_root)
    public_audit_db = Path(args.public_audit_db)
    strict_labels_all = base.discover_labels(strict_root, "CACHE_MANIFEST.json")
    completion_labels_all = base.discover_labels(completion_root, "EVENT_STORE_MANIFEST.json")
    strict_wanted = base.split_labels(args.strict_labels)
    completion_wanted = base.split_labels(args.completion_labels)
    strict_labels = {label: strict_labels_all[label] for label in strict_wanted if label in strict_labels_all}
    completion_labels = {
        label: completion_labels_all[label] for label in completion_wanted if label in completion_labels_all
    }
    excluded_label_fragments = repair.parse_csv_strings(args.exclude_label_fragments)
    strict_labels = repair.filter_excluded_labels(strict_labels, excluded_label_fragments)
    completion_labels = repair.filter_excluded_labels(completion_labels, excluded_label_fragments)
    profile = make_profile(args)
    plans: list[tuple[str, str, list[str]]] = []
    for strict_label, strict_days in strict_labels.items():
        for completion_label, completion_days in completion_labels.items():
            overlap = sorted(set(strict_days) & set(completion_days))
            if overlap:
                plans.append((strict_label, completion_label, overlap))
    all_days = sorted({day for _, _, days in plans for day in days})
    public_keys, public_day_counts, public_ready = base.load_public_audit_keys(public_audit_db, all_days)
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{base.utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)

    market_rows: list[dict[str, Any]] = []
    plan_manifests: list[dict[str, Any]] = []
    row_count = 0
    for strict_label, completion_label, days in plans:
        strict_db = strict_root / strict_label / "cache.duckdb"
        completion_db = completion_root / completion_label / "event_store.duckdb"
        rows = base.load_joined_candidates(
            strict_db,
            completion_db,
            days,
            args.max_strict_candidates_per_plan,
            args.completion_window_ms,
        )
        row_count += len(rows)
        for key, grouped in group_rows(rows).items():
            result = simulate_market(key, grouped, profile, public_keys)
            result.update({"strict_label": strict_label, "completion_label": completion_label})
            market_rows.append(result)
        plan_manifests.append(
            {
                "strict_label": strict_label,
                "completion_label": completion_label,
                "days": days,
                "strict_candidate_rows": len(rows),
            }
        )

    market_rows.sort(key=lambda row: (float(row["stress100_worst_pnl"]), float(row["worst_residual_net_pnl"])))
    global_summary = sum_group(market_rows, ["name"])
    fee_drag = float(global_summary["stress_fee_drag"])
    worst_residual = float(global_summary["worst_residual_net_pnl"])
    stress = float(global_summary["stress100_worst_pnl"])
    fee_drag_share = round(fee_drag / abs(stress), 6) if stress < 0 else 0.0
    residual_deficit_share = round(abs(min(0.0, worst_residual)) / abs(stress), 6) if stress < 0 else 0.0
    groups = {
        "by_day": group_summary(market_rows, ["day"], limit=20),
        "by_winner_side": group_summary(market_rows, ["winner_side"], limit=20),
        "by_residual_side": group_summary(market_rows, ["residual_side"], limit=20),
        "by_residual_winner_match": group_summary(market_rows, ["residual_side_matches_winner"], limit=20),
        "by_repair_opportunity": group_summary(market_rows, ["repair_opportunity_bucket"], limit=20),
        "by_pair_cap_block": group_summary(market_rows, ["repair_pair_cap_bucket"], limit=20),
        "by_risk_block": group_summary(market_rows, ["risk_block_bucket"], limit=20),
        "by_pair_cost_bucket": group_summary(market_rows, ["pair_cost_wavg_bucket"], limit=20),
        "by_residual_cost_bucket": group_summary(market_rows, ["residual_cost_bucket"], limit=20),
        "by_stress_driver": group_summary(market_rows, ["stress_driver_hint"], limit=20),
    }
    top_markets = market_rows[:100]
    top_markets_path = output_dir / "top_stress_markets.json"
    top_markets_path.write_text(json.dumps(top_markets, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    groups_path = output_dir / "group_summaries.json"
    groups_path.write_text(json.dumps(groups, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    market_csv_path = output_dir / "market_failures.csv"
    write_csv(market_csv_path, market_rows)

    if stress < 0 and fee_drag_share >= 0.75 and abs(worst_residual) <= fee_drag:
        status = "KEEP_FEE_DRAG_DOMINATED_FAILURE_LEAD"
        next_action = (
            "test a fee-aware repair selector that reduces churn/pair_qty or requires larger per-pair edge; "
            "do not spend EC2 shadow until stress fee drag improves on covered data"
        )
    elif stress < 0:
        status = "KEEP_RESIDUAL_COST_DOMINATED_FAILURE_LEAD"
        next_action = (
            "test residual-risk mechanism targeting worst residual cost buckets before EC2 shadow"
        )
    else:
        status = "PASS_FAILURE_DECOMPOSITION_STRESS_NONNEGATIVE"
        next_action = "rerun no-order shadow only after independent local validation confirms nonnegative stress"

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": base.utc_label(),
        "strategy": "xuan_b27_dplus",
        "status": status,
        "hypothesis": (
            "remaining negative covered split stress is diagnosable as fee-drag/churn versus residual-cost tail"
        ),
        "data_root": str(base.DEFAULT_POLY_BT_ROOT),
        "dataset_type": "local_poly_backtest_strict_cache_plus_completion_store_plus_public_audit_failure_decomposition",
        "strict_root": str(strict_root),
        "completion_root": str(completion_root),
        "public_audit_db": str(public_audit_db),
        "labels": {
            "strict": sorted(strict_labels),
            "completion": sorted(completion_labels),
            "public_audit": public_audit_db.parent.name if public_ready else None,
        },
        "days": all_days,
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": row_count,
        "market_count": len(market_rows),
        "profile": asdict(profile),
        "plans": plan_manifests,
        "public_audit_ready": public_ready,
        "public_audit_day_counts": public_day_counts,
        "public_account_truth_missing_days": sorted(day for day in all_days if day not in set(public_day_counts)),
        "excluded_label_fragments": excluded_label_fragments,
        "excluded_20260514_20260515": not (
            "2026-05-14" in all_days or "2026-05-15" in all_days
        ),
        "contains_20260518": "2026-05-18" in all_days,
        "includes_public_account_execution_truth_v1": public_ready,
        "public_account_truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
        "global_summary": global_summary,
        "scoreboard_delta": {
            "stress100_worst_pnl": stress,
            "worst_residual_net_pnl": worst_residual,
            "stress_fee_drag": fee_drag,
            "fee_drag_share_of_negative_stress": fee_drag_share,
            "residual_deficit_share_of_negative_stress": residual_deficit_share,
            "qty_residual_rate": global_summary["residual_rate"],
            "pair_cost_wavg_proxy": global_summary["pair_cost_wavg_proxy"],
        },
        "top_failure_groups": {key: value[:5] for key, value in groups.items()},
        "can_support_strategy_promotion": False,
        "requires_source_of_truth_replay_for_promotion": True,
        "conclusion_scope": (
            "local covered split failure decomposition; not private owner truth, not source-of-truth replay, not deployable as-is"
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
            "top_stress_markets_json": str(top_markets_path),
            "group_summaries_json": str(groups_path),
            "market_failures_csv": str(market_csv_path),
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
