#!/usr/bin/env python3
"""Probe structural inventory repair on local compliant D+ inputs.

This is an offline research-only probe. It uses strict/cache rows as the
admission stream and completion-store opposite-side asks as an oracle-like
repair opportunity. The goal is to falsify whether explicit opposite-side
repair can materially reduce residual tail before spending EC2 shadow windows.

It does not read raw/replay paths, start network processes, or submit orders.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import xuan_b27_dplus_residual_cooldown_compliant_metrics_runner as base


ARTIFACT = "xuan_b27_dplus_compliant_inventory_repair_probe"
INF = 1_000_000_000.0


@dataclass(frozen=True)
class RepairProfile:
    name: str
    edge: float = 0.055
    target_qty: float = 5.0
    seed_px_lo: float = 0.05
    seed_px_hi: float = 0.90
    alignment: str = "all"
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_open_cost: float = 250.0
    seed_offset_min_s: float = 0.0
    seed_offset_max_s: float = 120.0
    seed_l1_pair_cap: float = 1.02
    cooldown_ms: int = 5_000
    imbalance_qty_cap: float = 1.25
    imbalance_cost_cap: float = INF
    dust_qty: float = 1.0
    residual_cooldown_age_s: float = 30.0
    residual_cooldown_cost_cap: float = INF
    pair_select: str = "fifo"
    repair_after_s: float = 75.0
    repair_max_qty: float = 0.0
    repair_pair_cap: float = 1.00
    repair_budget_mode: str = "none"
    repair_budget_fraction: float = 0.50
    min_edge_per_pair: float = 0.005
    block_risk_increasing_after_repair_threshold: bool = True


def parse_csv_floats(raw: str) -> list[float]:
    return [float(item.strip()) for item in raw.split(",") if item.strip()]


def parse_csv_strings(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def profile_name(
    target_qty: float,
    after_s: float,
    repair_qty: float,
    pair_cap: float,
    budget_mode: str = "none",
    pair_select: str = "fifo",
) -> str:
    if repair_qty <= 0:
        base_name = f"baseline_t{target_qty:g}"
    else:
        base_name = f"repair_t{target_qty:g}_a{after_s:g}_q{repair_qty:g}_cap{pair_cap:g}"
        if budget_mode != "none":
            base_name += f"_budget{budget_mode}"
    if pair_select != "fifo":
        base_name += f"_pair{pair_select}"
    return base_name.replace(".", "p")


def build_profiles(args: argparse.Namespace) -> list[RepairProfile]:
    pair_selects = parse_csv_strings(args.pair_selects)
    repair_budget_modes = parse_csv_strings(args.repair_budget_modes)
    profiles = [
        RepairProfile(
            name=profile_name(target, 0, 0, 0, pair_select=pair_select),
            target_qty=target,
            pair_select=pair_select,
            repair_max_qty=0.0,
            block_risk_increasing_after_repair_threshold=False,
        )
        for target in parse_csv_floats(args.target_qtys)
        for pair_select in pair_selects
    ]
    for target in parse_csv_floats(args.target_qtys):
        for after_s in parse_csv_floats(args.repair_after_s):
            for repair_qty in parse_csv_floats(args.repair_max_qtys):
                for pair_cap in parse_csv_floats(args.repair_pair_caps):
                    for budget_mode in repair_budget_modes:
                        for pair_select in pair_selects:
                            profiles.append(
                                RepairProfile(
                                    name=profile_name(
                                        target,
                                        after_s,
                                        repair_qty,
                                        pair_cap,
                                        budget_mode,
                                        pair_select,
                                    ),
                                    target_qty=target,
                                    pair_select=pair_select,
                                    repair_after_s=after_s,
                                    repair_max_qty=repair_qty,
                                    repair_pair_cap=pair_cap,
                                    repair_budget_mode=budget_mode,
                                    repair_budget_fraction=args.repair_budget_fraction,
                                    min_edge_per_pair=args.min_edge_per_pair,
                                )
                        )
    return profiles


def dominant_side(state: base.State) -> tuple[str | None, float, float, float]:
    assert state.inv is not None
    yes_qty = base.lot_qty(state.inv["YES"])
    no_qty = base.lot_qty(state.inv["NO"])
    if yes_qty > no_qty + 1e-9:
        return "YES", yes_qty, no_qty, yes_qty - no_qty
    if no_qty > yes_qty + 1e-9:
        return "NO", no_qty, yes_qty, no_qty - yes_qty
    return None, yes_qty, no_qty, 0.0


def oldest_age_s(state: base.State, side: str, ts_ms: int) -> float:
    assert state.inv is not None
    lots = state.inv[side]
    if not lots:
        return 0.0
    oldest = min(lot.ts_ms for lot in lots)
    return max(0.0, (ts_ms - oldest) / 1000.0)


def avg_cost(state: base.State, side: str) -> float:
    assert state.inv is not None
    qty = base.lot_qty(state.inv[side])
    return base.lot_cost(state.inv[side]) / qty if qty > 1e-9 else 0.0


def selected_lot_px(state: base.State, side: str, pair_select: str) -> float:
    assert state.inv is not None
    lots = state.inv[side]
    if not lots:
        return 0.0
    if pair_select == "low_pair_cost":
        return min(lot.px for lot in lots)
    if pair_select == "high_cost":
        return max(lot.px for lot in lots)
    return lots[0].px


def repair_budget_available(state: base.State) -> float:
    return float(getattr(state, "repair_budget_available", 0.0))


def add_repair_budget(state: base.State, amount: float) -> None:
    setattr(state, "repair_budget_available", max(0.0, repair_budget_available(state) + amount))


def pair_inventory_budgeted(
    profile: RepairProfile,
    state: base.State,
    metrics: defaultdict[str, float],
    ts_ms: int,
) -> None:
    before_pair_qty = metrics["pair_qty"]
    before_pair_pnl = metrics["pair_pnl"]
    base.pair_inventory(profile, state, metrics, ts_ms)
    pair_qty_delta = metrics["pair_qty"] - before_pair_qty
    pair_pnl_delta = metrics["pair_pnl"] - before_pair_pnl
    if pair_qty_delta <= 1e-9:
        return
    spendable = max(0.0, pair_pnl_delta - profile.min_edge_per_pair * pair_qty_delta)
    deposit = spendable * max(0.0, profile.repair_budget_fraction)
    add_repair_budget(state, deposit)
    metrics["repair_budget_deposited"] += deposit
    metrics["pair_qty_budget_observed"] += pair_qty_delta


def maybe_seed_budgeted(
    row: dict[str, Any],
    profile: RepairProfile,
    state: base.State,
    metrics: defaultdict[str, float],
) -> None:
    before_pair_qty = metrics["pair_qty"]
    before_pair_pnl = metrics["pair_pnl"]
    base.maybe_seed(row, profile, state, metrics)
    pair_qty_delta = metrics["pair_qty"] - before_pair_qty
    pair_pnl_delta = metrics["pair_pnl"] - before_pair_pnl
    if pair_qty_delta <= 1e-9:
        return
    spendable = max(0.0, pair_pnl_delta - profile.min_edge_per_pair * pair_qty_delta)
    deposit = spendable * max(0.0, profile.repair_budget_fraction)
    add_repair_budget(state, deposit)
    metrics["repair_budget_deposited"] += deposit
    metrics["pair_qty_budget_observed"] += pair_qty_delta


def can_use_repair_row(row: dict[str, Any], repair_side: str) -> tuple[bool, float]:
    first_side = str(row.get("first_side") or "")
    repair_px = row.get("completion_min_opp_ask_30s")
    if first_side not in {"YES", "NO"} or base.other(first_side) != repair_side:
        return False, 0.0
    if repair_px is None:
        return False, 0.0
    px = float(repair_px)
    return math.isfinite(px) and px > 0.0, px


def maybe_completion_repair(
    row: dict[str, Any],
    profile: RepairProfile,
    state: base.State,
    metrics: defaultdict[str, float],
) -> bool:
    if profile.repair_max_qty <= profile.dust_qty:
        return False
    ts_ms = int(row.get("trigger_ts_ms") or 0)
    dom_side, dom_qty, opp_qty, imbalance_qty = dominant_side(state)
    if dom_side is None or imbalance_qty <= profile.dust_qty:
        return False
    if oldest_age_s(state, dom_side, ts_ms) < profile.repair_after_s:
        return False
    repair_side = base.other(dom_side)
    ok, repair_px = can_use_repair_row(row, repair_side)
    if not ok:
        metrics["repair_no_opportunity"] += 1
        return False
    dom_avg = avg_cost(state, dom_side)
    dom_pair_px = selected_lot_px(state, dom_side, profile.pair_select)
    projected_pair_cost = dom_pair_px + repair_px
    if projected_pair_cost > profile.repair_pair_cap + 1e-12:
        metrics["repair_block_pair_cap"] += 1
        return False
    assert state.inv is not None
    open_cost = base.lot_cost(state.inv["YES"]) + base.lot_cost(state.inv["NO"])
    cost_room_qty = (profile.max_open_cost - open_cost) / max(repair_px, 1e-9)
    budget_per_qty = max(0.0, projected_pair_cost + profile.min_edge_per_pair - 1.0)
    if profile.repair_budget_mode == "surplus" and budget_per_qty > 1e-12:
        budget_room_qty = repair_budget_available(state) / budget_per_qty
    else:
        budget_room_qty = INF
    qty = min(profile.repair_max_qty, imbalance_qty, cost_room_qty, budget_room_qty)
    if qty <= profile.dust_qty:
        if budget_room_qty <= profile.dust_qty:
            metrics["repair_block_surplus_budget"] += 1
        else:
            metrics["repair_block_budget"] += 1
        return False
    budget_spend = budget_per_qty * qty if profile.repair_budget_mode == "surplus" else 0.0
    if budget_spend > 0.0:
        add_repair_budget(state, -budget_spend)
        metrics["repair_budget_spent"] += budget_spend
        metrics["repair_budget_required"] += budget_spend
    metrics["repair_projected_pair_cost_sum"] += qty * projected_pair_cost
    state.inv[repair_side].append(base.Lot(qty=qty, px=repair_px, ts_ms=ts_ms, side=repair_side))
    state.last_seed_ts = ts_ms
    state.last_ts_ms = max(state.last_ts_ms, ts_ms)
    state.active = True
    metrics["repair_actions"] += 1
    metrics["repair_qty"] += qty
    metrics["repair_cost"] += qty * repair_px
    metrics["gross_buy_qty"] += qty
    metrics["gross_buy_cost"] += qty * repair_px
    pair_inventory_budgeted(profile, state, metrics, ts_ms)
    return True


def maybe_seed_with_repair(
    row: dict[str, Any],
    profile: RepairProfile,
    state: base.State,
    metrics: defaultdict[str, float],
) -> None:
    side = str(row.get("first_side") or "")
    ts_ms = int(row.get("trigger_ts_ms") or 0)
    dom_side, _, _, imbalance_qty = dominant_side(state)
    at_repair_threshold = (
        dom_side is not None
        and imbalance_qty > profile.dust_qty
        and oldest_age_s(state, dom_side, ts_ms) >= profile.repair_after_s
    )
    repaired = maybe_completion_repair(row, profile, state, metrics)
    if (
        profile.block_risk_increasing_after_repair_threshold
        and at_repair_threshold
        and side == dom_side
    ):
        metrics["seed_block_risk_increasing_repair_mode"] += 1
        if not repaired:
            metrics["risk_block_without_repair"] += 1
        return
    maybe_seed_budgeted(row, profile, state, metrics)


def run_metrics(
    rows: list[dict[str, Any]],
    profiles: list[RepairProfile],
    public_keys: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    candidate_keys = {(base.dashed_day(str(row["day"])), str(row["condition_id"])) for row in rows}
    public_covered_markets = candidate_keys & public_keys
    for profile in profiles:
        states: dict[str, base.State] = {}
        metrics: defaultdict[str, float] = defaultdict(float)
        metrics["strict_candidate_rows"] = len(rows)
        metrics["public_audit_covered_markets"] = len(public_covered_markets)
        for row in rows:
            key = (base.dashed_day(str(row["day"])), str(row["condition_id"]))
            if key in public_keys:
                metrics["public_audit_covered_candidates"] += 1
            if row.get("completion_min_opp_ask_30s") is not None:
                metrics["completion_joined_candidates"] += 1
            pair_cost = row.get("completion_min_pair_cost_30s")
            if pair_cost is not None and float(pair_cost) <= 1.0:
                metrics["completion_pair_le_100_candidates"] += 1
            condition_id = str(row.get("condition_id") or "")
            if not condition_id:
                continue
            state_key = f"{base.dashed_day(str(row.get('day') or ''))}:{condition_id}"
            state = states.get(state_key)
            if state is None:
                state = base.State(
                    winner=str(row.get("winner_side")) if row.get("winner_side") else None,
                    day=base.dashed_day(str(row.get("day") or "")),
                    slug=str(row.get("slug") or ""),
                )
                states[state_key] = state
            elif state.winner is None and row.get("winner_side"):
                state.winner = str(row.get("winner_side"))
            state.last_ts_ms = max(state.last_ts_ms, int(row.get("trigger_ts_ms") or 0))
            maybe_seed_with_repair(row, profile, state, metrics)
        base.settle(states, profile, metrics)
        for state in states.values():
            metrics["repair_budget_available_end"] += repair_budget_available(state)
        results.append(finish(profile, metrics))
    results.sort(key=lambda row: (row["stress100_worst_pnl"], row["net_pnl"]), reverse=True)
    return results


def finish(profile: RepairProfile, metrics: defaultdict[str, float]) -> dict[str, Any]:
    out = base.finish(profile, metrics)
    out.update(
        {
            "pair_cost_sum": round(metrics["pair_cost_sum"], 6),
            "pair_delay_ms": round(metrics["pair_delay_ms"], 6),
            "repair_actions": int(metrics["repair_actions"]),
            "repair_qty": round(metrics["repair_qty"], 6),
            "repair_cost": round(metrics["repair_cost"], 6),
            "repair_no_opportunity": int(metrics["repair_no_opportunity"]),
            "repair_block_pair_cap": int(metrics["repair_block_pair_cap"]),
            "repair_block_budget": int(metrics["repair_block_budget"]),
            "repair_block_surplus_budget": int(metrics["repair_block_surplus_budget"]),
            "repair_budget_deposited": round(metrics["repair_budget_deposited"], 6),
            "repair_budget_spent": round(metrics["repair_budget_spent"], 6),
            "repair_budget_required": round(metrics["repair_budget_required"], 6),
            "repair_budget_available_end": round(metrics["repair_budget_available_end"], 6),
            "repair_pair_cost_wavg_projected": round(
                (metrics["repair_projected_pair_cost_sum"] / metrics["repair_qty"])
                if metrics["repair_qty"]
                else 0.0,
                6,
            ),
            "seed_block_risk_increasing_repair_mode": int(metrics["seed_block_risk_increasing_repair_mode"]),
            "risk_block_without_repair": int(metrics["risk_block_without_repair"]),
        }
    )
    return out


def aggregate_results(profiles: list[RepairProfile], combined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    additive = {
        "active_markets",
        "strict_candidate_rows",
        "completion_joined_candidates",
        "completion_pair_le_100_candidates",
        "public_audit_covered_candidates",
        "public_audit_covered_markets",
        "seed_actions",
        "repair_actions",
        "pair_actions",
        "gross_buy_qty",
        "gross_buy_cost",
        "repair_qty",
        "repair_cost",
        "pair_qty",
        "pair_cost_sum",
        "pair_delay_ms",
        "residual_qty",
        "residual_cost",
        "residual_settle_payout",
        "residual_settle_pnl",
        "pair_pnl",
        "seed_block_alignment",
        "seed_block_offset",
        "seed_block_price_band",
        "seed_block_l1_pair_cap",
        "seed_block_cooldown",
        "seed_block_target",
        "seed_block_imbalance_qty",
        "seed_block_imbalance_cost",
        "seed_block_residual_cooldown",
        "repair_no_opportunity",
        "repair_block_pair_cap",
        "repair_block_budget",
        "repair_block_surplus_budget",
        "repair_budget_deposited",
        "repair_budget_spent",
        "repair_budget_required",
        "repair_budget_available_end",
        "repair_projected_pair_cost_sum",
        "pair_qty_budget_observed",
        "seed_block_risk_increasing_repair_mode",
        "risk_block_without_repair",
        "resid_cost_gt6_markets",
    }
    by_profile: dict[str, defaultdict[str, float]] = {}
    for row in combined:
        name = str(row["name"])
        by_profile.setdefault(name, defaultdict(float))
        for key in additive:
            by_profile[name][key] += float(row.get(key) or 0.0)
    out: list[dict[str, Any]] = []
    for profile in profiles:
        metrics = by_profile.get(profile.name, defaultdict(float))
        out.append(finish(profile, metrics))
    out.sort(key=lambda row: (row["stress100_worst_pnl"], row["net_pnl"]), reverse=True)
    return out


def status_from_results(results: list[dict[str, Any]]) -> tuple[str, dict[str, Any], dict[str, Any], dict[str, float], str]:
    baselines = [row for row in results if float(row.get("repair_max_qty") or 0.0) <= 0.0]
    repair_rows = [row for row in results if float(row.get("repair_max_qty") or 0.0) > 0.0]
    baseline_t5 = next((row for row in baselines if float(row.get("target_qty") or 0.0) == 5.0), baselines[0])
    best = repair_rows[0] if repair_rows else baseline_t5
    residual_delta = float(baseline_t5["qty_residual_rate"]) - float(best["qty_residual_rate"])
    stress_delta = float(best["stress100_worst_pnl"]) - float(baseline_t5["stress100_worst_pnl"])
    deltas = {
        "qty_residual_rate_improvement": round(residual_delta, 6),
        "residual_qty_delta": round(float(best["residual_qty"]) - float(baseline_t5["residual_qty"]), 6),
        "stress100_worst_pnl_delta": round(stress_delta, 6),
        "worst_residual_net_pnl_delta": round(
            float(best["worst_residual_net_pnl"]) - float(baseline_t5["worst_residual_net_pnl"]),
            6,
        ),
        "net_pnl_delta": round(float(best["net_pnl"]) - float(baseline_t5["net_pnl"]), 6),
    }
    if (
        int(best["seed_actions"]) >= 100
        and residual_delta >= 0.02
        and stress_delta > 0.0
        and float(best["stress100_worst_pnl"]) > 0.0
    ):
        return (
            "KEEP_INVENTORY_REPAIR_CANDIDATE",
            best,
            baseline_t5,
            deltas,
            "repair improves residual and stress enough for a follow-up local split or shadow design review",
        )
    if residual_delta >= 0.02 and stress_delta > 0.0:
        return (
            "KEEP_RESEARCH_ONLY_REPAIR_LEAD",
            best,
            baseline_t5,
            deltas,
            "repair improves residual/stress but remains below promotion thresholds; split by public-audit-covered days next",
        )
    return (
        "DISCARD_NO_INVENTORY_REPAIR_IMPROVEMENT",
        best,
        baseline_t5,
        deltas,
        "inventory repair did not improve residual/stress versus baseline; kill this repair mechanism",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe D+ compliant inventory repair mechanisms.")
    parser.add_argument("--strict-root", default=str(base.DEFAULT_STRICT_ROOT))
    parser.add_argument("--completion-root", default=str(base.DEFAULT_COMPLETION_ROOT))
    parser.add_argument("--public-audit-db", default=str(base.DEFAULT_PUBLIC_AUDIT_DB))
    parser.add_argument("--strict-labels", default="")
    parser.add_argument("--completion-labels", default="")
    parser.add_argument("--target-qtys", default="3,5")
    parser.add_argument("--repair-after-s", default="60,75")
    parser.add_argument("--repair-max-qtys", default="1.25,2.5,5")
    parser.add_argument("--repair-pair-caps", default="1.00,1.02")
    parser.add_argument("--repair-budget-modes", default="none")
    parser.add_argument("--repair-budget-fraction", type=float, default=0.50)
    parser.add_argument("--min-edge-per-pair", type=float, default=0.005)
    parser.add_argument("--pair-selects", default="fifo")
    parser.add_argument("--completion-window-ms", type=int, default=30_000)
    parser.add_argument("--max-strict-candidates-per-plan", type=int, default=0)
    parser.add_argument("--output-dir")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    strict_root = Path(args.strict_root)
    completion_root = Path(args.completion_root)
    public_audit_db = Path(args.public_audit_db)
    strict_labels = base.discover_labels(strict_root, "CACHE_MANIFEST.json")
    completion_labels = base.discover_labels(completion_root, "EVENT_STORE_MANIFEST.json")
    if args.strict_labels:
        wanted = base.split_labels(args.strict_labels)
        strict_labels = {label: strict_labels[label] for label in wanted if label in strict_labels}
    if args.completion_labels:
        wanted = base.split_labels(args.completion_labels)
        completion_labels = {label: completion_labels[label] for label in wanted if label in completion_labels}
    profiles = build_profiles(args)
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
    combined: list[dict[str, Any]] = []
    row_count = 0
    plan_manifests: list[dict[str, Any]] = []
    for idx, (strict_label, completion_label, days) in enumerate(plans, start=1):
        strict_db = strict_root / strict_label / "cache.duckdb"
        completion_db = completion_root / completion_label / "event_store.duckdb"
        if not strict_db.exists() or not completion_db.exists():
            continue
        rows = base.load_joined_candidates(
            strict_db,
            completion_db,
            days,
            args.max_strict_candidates_per_plan,
            args.completion_window_ms,
        )
        row_count += len(rows)
        results = run_metrics(rows, profiles, public_keys)
        for result in results:
            result.update({"strict_label": strict_label, "completion_label": completion_label, "days": days})
        combined.extend(results)
        plan_manifests.append(
            {
                "strict_label": strict_label,
                "completion_label": completion_label,
                "days": days,
                "strict_candidate_rows": len(rows),
            }
        )
    results = aggregate_results(profiles, combined)
    status, best, baseline, scoreboard_delta, next_action = status_from_results(results)
    results_path = output_dir / "combined_results.json"
    results_path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    best_path = output_dir / "best_result.json"
    best_path.write_text(json.dumps(best, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": base.utc_label(),
        "strategy": "xuan_b27_dplus",
        "scope": "local_read_only_compliant_inventory_repair_probe",
        "status": status,
        "hypothesis": (
            "explicit opposite-side completion-store repair after aged one-sided inventory "
            "can reduce residual tail under strict/cache admission"
        ),
        "data_root": str(base.DEFAULT_POLY_BT_ROOT),
        "dataset_type": "local_poly_backtest_strict_cache_plus_completion_store_plus_public_audit_metrics",
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
        "profile_count": len(profiles),
        "plans": plan_manifests,
        "public_audit_ready": public_ready,
        "public_audit_day_counts": public_day_counts,
        "public_account_truth_missing_days": sorted(
            day for day in all_days if day not in set(public_day_counts)
        ),
        "excluded_20260514_20260515": True,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": public_ready,
        "public_account_truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
        "can_support_strategy_promotion": False,
        "requires_source_of_truth_replay_for_promotion": True,
        "conclusion_scope": (
            "strict/cache admission plus completion-store oracle-like repair metrics; "
            "not private queue truth, not source-of-truth replay, not deployable as-is"
        ),
        "kill_list_respected": [
            "no public_trade_size cap sweep",
            "no public-trade price/slippage filter",
            "no residual cooldown cap mechanism",
            "no raw/replay/service/shared-ingress/order path",
        ],
        "best_result": best,
        "baseline_result": baseline,
        "scoreboard_delta_vs_baseline": scoreboard_delta,
        "next_action": next_action,
        "raw_replay_scanned": False,
        "duckdb_tables_read": True,
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
            "combined_results_json": str(results_path),
            "best_result_json": str(best_path),
        },
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
