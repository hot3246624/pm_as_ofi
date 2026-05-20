#!/usr/bin/env python3
"""Rerun D+ candidate-pipeline V1 state machine from candidate_base.

This script reads only the derived candidate_base DuckDB plus the existing
candidate-pipeline result manifests for baseline comparison. It does not read
raw/replay/collector stores, the full completion event store, sockets, SSH, or
event JSONL.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_CANDIDATE_BASE_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "local_20260502_20260518_paircap102"
)
DEFAULT_BASELINE_RESULT_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
    "20260502_20260518_publicfull_v2"
)
ARTIFACT = "xuan_b27_dplus_candidate_pipeline_state_machine_side_risk_px_cap_rerun"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
)
DUST = 1e-9


@dataclass(frozen=True)
class Profile:
    name: str
    seed_offset_max_s: float
    late_repair_only_after_s: float | None = None
    edge: float = 0.055
    target_qty: float = 5.0
    seed_px_lo: float = 0.05
    seed_px_hi: float = 0.90
    public_trade_px_hi: float | None = None
    risk_increasing_public_trade_px_hi: float | None = None
    risk_increasing_public_trade_px_hi_side: str | None = None
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_open_cost: float = 250.0
    seed_offset_min_s: float = 0.0
    seed_l1_pair_cap: float = 1.02
    cooldown_s: float = 5.0
    imbalance_qty_cap: float = 1.25
    imbalance_cost_cap: float = 1_000_000_000.0
    dust_qty: float = 1.0
    residual_cooldown_age_s: float = 30.0
    residual_cooldown_cost_cap: float = 0.5
    official_fee_rate: float = 0.07


@dataclass
class Lot:
    qty: float
    px: float
    ts_ms: int
    side: str
    source_candidate_row_id: str


@dataclass
class State:
    day: str
    slug: str
    winner_side: str | None
    lots: dict[str, deque[Lot]]
    last_seed_ts_ms: int = -(10**18)
    last_ts_ms: int = 0
    active: bool = False


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    resolved = str(path.resolve())
    return not any(fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def other(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def lot_qty(lots: deque[Lot]) -> float:
    return sum(lot.qty for lot in lots)


def lot_cost(lots: deque[Lot]) -> float:
    return sum(lot.qty * lot.px for lot in lots)


def aged_lot_cost(lots: deque[Lot], ts_ms: int, age_s: float) -> float:
    cutoff_ms = age_s * 1000.0
    return sum(lot.qty * lot.px for lot in lots if ts_ms - lot.ts_ms >= cutoff_ms)


def official_taker_fee(qty: float, px: float, fee_rate: float) -> float:
    return qty * fee_rate * px * (1.0 - px)


def day_metrics(metrics: defaultdict[str, float], day: str) -> defaultdict[str, float]:
    key = f"day::{day}"
    value = metrics.get(key)
    if value is None:
        value = defaultdict(float)
        metrics[key] = value  # type: ignore[assignment]
    return value  # type: ignore[return-value]


def add_metric(metrics: defaultdict[str, float], day: str, key: str, value: float) -> None:
    metrics[key] += value
    day_metrics(metrics, day)[key] += value


def add_count(metrics: defaultdict[str, float], day: str, key: str, value: int = 1) -> None:
    add_metric(metrics, day, key, float(value))


def pop_lot(lots: deque[Lot], idx: int) -> Lot:
    if idx == 0:
        return lots.popleft()
    lots.rotate(-idx)
    lot = lots.popleft()
    lots.rotate(idx)
    return lot


def pair_inventory(profile: Profile, state: State, metrics: defaultdict[str, float], ts_ms: int) -> None:
    yes = state.lots["YES"]
    no = state.lots["NO"]
    while yes and no:
        yes_idx = 0
        no_idx = 0
        yes_lot = yes[yes_idx]
        no_lot = no[no_idx]
        take = min(yes_lot.qty, no_lot.qty)
        if take <= DUST:
            break
        pair_cost = yes_lot.px + no_lot.px
        older_ts = min(yes_lot.ts_ms, no_lot.ts_ms)
        add_count(metrics, state.day, "pair_actions")
        add_metric(metrics, state.day, "pair_qty", take)
        add_metric(metrics, state.day, "pair_cost_sum", take * pair_cost)
        add_metric(metrics, state.day, "pair_pnl", take * (1.0 - pair_cost))
        add_metric(metrics, state.day, "pair_delay_ms", take * max(0, ts_ms - older_ts))
        yes_lot.qty -= take
        no_lot.qty -= take
        if yes_lot.qty <= DUST:
            pop_lot(yes, yes_idx)
        if no_lot.qty <= DUST:
            pop_lot(no, no_idx)


def state_for(states: dict[str, State], row: dict[str, Any]) -> State:
    condition_id = str(row["condition_id"])
    state = states.get(condition_id)
    if state is None:
        state = State(
            day=str(row["day"]),
            slug=str(row["slug"]),
            winner_side=str(row["winner_side"]) if row.get("winner_side") else None,
            lots={"YES": deque(), "NO": deque()},
        )
        states[condition_id] = state
    elif state.winner_side is None and row.get("winner_side"):
        state.winner_side = str(row["winner_side"])
    state.last_ts_ms = max(state.last_ts_ms, int(row["ts_ms"]))
    return state


def maybe_seed(row: dict[str, Any], profile: Profile, state: State, metrics: defaultdict[str, float]) -> None:
    day = str(row["day"])
    add_count(metrics, day, "candidate_count")
    side = str(row["side"])
    if side not in {"YES", "NO"}:
        return
    offset_s = float(row["offset_s"])
    if not (profile.seed_offset_min_s <= offset_s < profile.seed_offset_max_s):
        add_count(metrics, day, "seed_block_offset")
        return
    trade_px = float(row["public_trade_price"])
    trade_size = float(row["public_trade_size"])
    if trade_size <= DUST or not math.isfinite(trade_px):
        return
    if profile.public_trade_px_hi is not None and trade_px > profile.public_trade_px_hi:
        add_count(metrics, day, "seed_block_public_trade_px_hi")
        return
    if not (profile.seed_px_lo <= trade_px <= profile.seed_px_hi):
        add_count(metrics, day, "seed_block_price_band")
        return
    l1_pair_ask = float(row["l1_pair_ask"])
    if not math.isfinite(l1_pair_ask) or l1_pair_ask > profile.seed_l1_pair_cap + 1e-12:
        add_count(metrics, day, "seed_block_l1_pair_cap")
        return
    ts_ms = int(row["ts_ms"])
    if ts_ms - state.last_seed_ts_ms < profile.cooldown_s * 1000.0:
        add_count(metrics, day, "seed_block_cooldown")
        return

    same_qty = lot_qty(state.lots[side])
    opp_qty = lot_qty(state.lots[other(side)])
    if (
        profile.risk_increasing_public_trade_px_hi is not None
        and trade_px > profile.risk_increasing_public_trade_px_hi
        and (
            profile.risk_increasing_public_trade_px_hi_side is None
            or side == profile.risk_increasing_public_trade_px_hi_side
        )
        and same_qty >= opp_qty
    ):
        add_count(metrics, day, "seed_block_risk_increasing_public_trade_px_hi")
        return
    aged_cost = aged_lot_cost(state.lots["YES"], ts_ms, profile.residual_cooldown_age_s) + aged_lot_cost(
        state.lots["NO"], ts_ms, profile.residual_cooldown_age_s
    )
    if aged_cost > profile.residual_cooldown_cost_cap + 1e-12 and same_qty + profile.dust_qty >= opp_qty:
        add_count(metrics, day, "seed_block_residual_cooldown")
        return
    if (
        profile.late_repair_only_after_s is not None
        and offset_s >= profile.late_repair_only_after_s
        and same_qty >= opp_qty
    ):
        add_count(metrics, day, "seed_block_late_repair_only")
        return
    if same_qty >= profile.target_qty - profile.dust_qty:
        add_count(metrics, day, "seed_block_target")
        return
    same_cost = lot_cost(state.lots[side])
    opp_cost = lot_cost(state.lots[other(side)])
    if max(0.0, same_cost - opp_cost) > profile.imbalance_cost_cap + 1e-12:
        add_count(metrics, day, "seed_block_imbalance_cost")
        return

    seed_px = max(0.01, trade_px - profile.edge)
    open_cost = lot_cost(state.lots["YES"]) + lot_cost(state.lots["NO"])
    imbalance_room = profile.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
    if imbalance_room <= profile.dust_qty:
        add_count(metrics, day, "seed_block_imbalance_qty")
        return
    qty = min(
        profile.max_seed_qty,
        trade_size * profile.fill_haircut,
        profile.target_qty - same_qty,
        (profile.max_open_cost - open_cost) / max(seed_px, 1e-9),
        imbalance_room,
    )
    if qty <= profile.dust_qty:
        return

    state.lots[side].append(
        Lot(
            qty=qty,
            px=seed_px,
            ts_ms=ts_ms,
            side=side,
            source_candidate_row_id=str(row["candidate_row_id"]),
        )
    )
    state.last_seed_ts_ms = ts_ms
    state.active = True
    fee = official_taker_fee(qty, seed_px, profile.official_fee_rate)
    add_count(metrics, day, "seed_actions")
    add_metric(metrics, day, "gross_buy_qty", qty)
    add_metric(metrics, day, "gross_buy_cost", qty * seed_px)
    add_metric(metrics, day, "official_taker_fee", fee)
    pair_inventory(profile, state, metrics, ts_ms)


def settle(states: dict[str, State], profile: Profile, metrics: defaultdict[str, float]) -> None:
    for state in states.values():
        if not state.active:
            continue
        pair_inventory(profile, state, metrics, state.last_ts_ms)
        add_count(metrics, state.day, "active_markets")
        winner = state.winner_side
        for side in ("YES", "NO"):
            for lot in state.lots[side]:
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                payout = lot.qty if winner == side else 0.0
                add_metric(metrics, state.day, "residual_qty", lot.qty)
                add_metric(metrics, state.day, "residual_cost", cost)
                add_metric(metrics, state.day, "residual_settle_payout", payout)
                add_metric(metrics, state.day, "residual_settle_pnl", payout - cost)


def finish_metrics(profile: Profile, metrics: defaultdict[str, float], base_day_counts: dict[str, int]) -> dict[str, Any]:
    buy_cost = float(metrics["gross_buy_cost"])
    buy_qty = float(metrics["gross_buy_qty"])
    pair_qty = float(metrics["pair_qty"])
    actual_pnl = float(metrics["pair_pnl"] + metrics["residual_settle_pnl"])
    fee_after_pnl = actual_pnl - float(metrics["official_taker_fee"])
    worst_residual_pnl = float(metrics["pair_pnl"] - metrics["residual_cost"])
    stress100_worst = worst_residual_pnl - 0.01 * (2.0 * pair_qty + float(metrics["residual_qty"]))
    summary_by_day = []
    for day in sorted(base_day_counts):
        dm: defaultdict[str, float] = day_metrics(metrics, day)
        day_buy_cost = float(dm["gross_buy_cost"])
        day_buy_qty = float(dm["gross_buy_qty"])
        day_pair_qty = float(dm["pair_qty"])
        day_actual = float(dm["pair_pnl"] + dm["residual_settle_pnl"])
        day_fee_after = day_actual - float(dm["official_taker_fee"])
        day_worst = float(dm["pair_pnl"] - dm["residual_cost"])
        day_stress = day_worst - 0.01 * (2.0 * day_pair_qty + float(dm["residual_qty"]))
        summary_by_day.append(
            {
                "day": day,
                "candidate_count": int(base_day_counts[day]),
                "public_sell_candidate_count": int(dm["candidate_count"]),
                "active_markets": int(dm["active_markets"]),
                "seed_actions": int(dm["seed_actions"]),
                "pair_actions": int(dm["pair_actions"]),
                "gross_buy_qty": round(day_buy_qty, 6),
                "gross_buy_cost": round(day_buy_cost, 6),
                "pair_qty": round(day_pair_qty, 6),
                "pair_cost_wavg": round(float(dm["pair_cost_sum"]) / day_pair_qty, 6) if day_pair_qty else 0.0,
                "pair_pnl": round(float(dm["pair_pnl"]), 6),
                "actual_settle_pnl": round(day_actual, 6),
                "official_taker_fee": round(float(dm["official_taker_fee"]), 6),
                "fee_after_pnl": round(day_fee_after, 6),
                "worst_residual_net_pnl": round(day_worst, 6),
                "stress100_worst_pnl": round(day_stress, 6),
                "residual_qty": round(float(dm["residual_qty"]), 6),
                "residual_cost": round(float(dm["residual_cost"]), 6),
                "qty_residual_rate": round(float(dm["residual_qty"]) / day_buy_qty, 6) if day_buy_qty else 0.0,
                "cost_residual_rate": round(float(dm["residual_cost"]) / day_buy_cost, 6) if day_buy_cost else 0.0,
            }
        )
    return {
        **asdict(profile),
        "candidate_count": int(metrics["candidate_count"]),
        "active_markets": int(metrics["active_markets"]),
        "seed_actions": int(metrics["seed_actions"]),
        "pair_actions": int(metrics["pair_actions"]),
        "pair_qty": round(pair_qty, 6),
        "weighted_pair_cost": round(float(metrics["pair_cost_sum"]) / pair_qty, 6) if pair_qty else 0.0,
        "gross_buy_qty": round(buy_qty, 6),
        "gross_buy_cost": round(buy_cost, 6),
        "gross_pnl": round(actual_pnl, 6),
        "actual_settle_pnl": round(actual_pnl, 6),
        "official_taker_fee": round(float(metrics["official_taker_fee"]), 6),
        "fee_after_pnl": round(fee_after_pnl, 6),
        "pair_pnl": round(float(metrics["pair_pnl"]), 6),
        "residual_settle_pnl": round(float(metrics["residual_settle_pnl"]), 6),
        "residual_qty": round(float(metrics["residual_qty"]), 6),
        "residual_cost": round(float(metrics["residual_cost"]), 6),
        "residual_qty_rate": round(float(metrics["residual_qty"]) / buy_qty, 6) if buy_qty else 0.0,
        "residual_cost_rate": round(float(metrics["residual_cost"]) / buy_cost, 6) if buy_cost else 0.0,
        "worst_residual_net_pnl": round(worst_residual_pnl, 6),
        "stress100_worst_pnl": round(stress100_worst, 6),
        "worst_day_fee_after_pnl": round(min((row["fee_after_pnl"] for row in summary_by_day), default=0.0), 6),
        "seed_block_offset": int(metrics["seed_block_offset"]),
        "seed_block_price_band": int(metrics["seed_block_price_band"]),
        "seed_block_public_trade_px_hi": int(metrics["seed_block_public_trade_px_hi"]),
        "seed_block_risk_increasing_public_trade_px_hi": int(
            metrics["seed_block_risk_increasing_public_trade_px_hi"]
        ),
        "seed_block_l1_pair_cap": int(metrics["seed_block_l1_pair_cap"]),
        "seed_block_cooldown": int(metrics["seed_block_cooldown"]),
        "seed_block_residual_cooldown": int(metrics["seed_block_residual_cooldown"]),
        "seed_block_late_repair_only": int(metrics["seed_block_late_repair_only"]),
        "seed_block_target": int(metrics["seed_block_target"]),
        "seed_block_imbalance_qty": int(metrics["seed_block_imbalance_qty"]),
        "seed_block_imbalance_cost": int(metrics["seed_block_imbalance_cost"]),
        "summary_by_day": summary_by_day,
    }


def run_profiles(candidate_base_db: Path, profiles: list[Profile], base_day_counts: dict[str, int]) -> dict[str, Any]:
    con = duckdb.connect(str(candidate_base_db), read_only=True)
    select_cols = [
        "candidate_row_id",
        "source_label",
        "day",
        "condition_id",
        "slug",
        "ts_ms",
        "ts_iso",
        "offset_s",
        "side",
        "opposite_side",
        "winner_side",
        "side_alignment",
        "candidate_reason",
        "public_trade_price",
        "public_trade_size",
        "l1_pair_ask",
    ]
    query = f"""
        select {", ".join(select_cols)}
        from candidate_base
        where candidate_reason = 'public_sell'
          and offset_s >= 0
          and offset_s < 300
        order by ts_ms, condition_id, side, candidate_row_id
    """
    cursor = con.execute(query)
    metrics_by_profile: dict[str, defaultdict[str, float]] = {profile.name: defaultdict(float) for profile in profiles}
    states_by_profile: dict[str, dict[str, State]] = {profile.name: {} for profile in profiles}
    row_count = 0
    while True:
        rows = cursor.fetchmany(50_000)
        if not rows:
            break
        for raw in rows:
            if len(raw) != len(select_cols):
                raise RuntimeError(f"candidate_base row width mismatch: got {len(raw)} expected {len(select_cols)}")
            row = dict(zip(select_cols, raw))
            row_count += 1
            for profile in profiles:
                state = state_for(states_by_profile[profile.name], row)
                maybe_seed(row, profile, state, metrics_by_profile[profile.name])
    for profile in profiles:
        settle(states_by_profile[profile.name], profile, metrics_by_profile[profile.name])
    con.close()
    return {
        "processed_public_sell_rows": row_count,
        "profiles": {
            profile.name: finish_metrics(profile, metrics_by_profile[profile.name], base_day_counts)
            for profile in profiles
        },
    }


def rel_delta(actual: float, expected: float) -> float | None:
    if expected == 0:
        return None
    return (actual - expected) / expected


def build_comparison(baseline: dict[str, Any], control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "seed_actions",
        "active_markets",
        "pair_actions",
        "pair_qty",
        "weighted_pair_cost",
        "gross_pnl",
        "official_taker_fee",
        "fee_after_pnl",
        "stress100_worst_pnl",
        "worst_day_fee_after_pnl",
        "residual_qty_rate",
        "residual_cost_rate",
    ]
    reproduction = {}
    variant_delta = {}
    for field in fields:
        expected = float(baseline.get(field) or 0)
        actual = float(control.get(field) or 0)
        var = float(variant.get(field) or 0)
        reproduction[field] = {
            "expected": expected,
            "control": actual,
            "absolute_delta": actual - expected,
            "relative_delta": rel_delta(actual, expected),
        }
        variant_delta[field] = {
            "control": actual,
            "variant": var,
            "absolute_delta": var - actual,
            "relative_delta": rel_delta(var, actual),
        }
    variant_delta["seed_action_retention"] = (
        float(variant["seed_actions"]) / float(control["seed_actions"]) if control.get("seed_actions") else None
    )
    variant_delta["pair_qty_retention"] = (
        float(variant["pair_qty"]) / float(control["pair_qty"]) if control.get("pair_qty") else None
    )
    variant_delta["residual_qty_rate_reduction"] = (
        1.0 - float(variant["residual_qty_rate"]) / float(control["residual_qty_rate"])
        if control.get("residual_qty_rate")
        else None
    )
    variant_delta["residual_cost_rate_reduction"] = (
        1.0 - float(variant["residual_cost_rate"]) / float(control["residual_cost_rate"])
        if control.get("residual_cost_rate")
        else None
    )
    return {"control_reproduction": reproduction, "variant_delta": variant_delta}


def build_direct_delta(control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "seed_actions",
        "active_markets",
        "pair_actions",
        "pair_qty",
        "weighted_pair_cost",
        "gross_pnl",
        "official_taker_fee",
        "fee_after_pnl",
        "stress100_worst_pnl",
        "worst_day_fee_after_pnl",
        "residual_qty_rate",
        "residual_cost_rate",
    ]
    delta = {}
    for field in fields:
        actual = float(control.get(field) or 0)
        var = float(variant.get(field) or 0)
        delta[field] = {
            "control": actual,
            "variant": var,
            "absolute_delta": var - actual,
            "relative_delta": rel_delta(var, actual),
        }
    delta["seed_action_retention"] = (
        float(variant["seed_actions"]) / float(control["seed_actions"]) if control.get("seed_actions") else None
    )
    delta["pair_qty_retention"] = (
        float(variant["pair_qty"]) / float(control["pair_qty"]) if control.get("pair_qty") else None
    )
    delta["residual_qty_rate_reduction"] = (
        1.0 - float(variant["residual_qty_rate"]) / float(control["residual_qty_rate"])
        if control.get("residual_qty_rate")
        else None
    )
    delta["residual_cost_rate_reduction"] = (
        1.0 - float(variant["residual_cost_rate"]) / float(control["residual_cost_rate"])
        if control.get("residual_cost_rate")
        else None
    )
    delta["weighted_pair_cost_reduction"] = (
        1.0 - float(variant["weighted_pair_cost"]) / float(control["weighted_pair_cost"])
        if control.get("weighted_pair_cost")
        else None
    )
    return delta


def decision_from(control: dict[str, Any], variant: dict[str, Any], comparison: dict[str, Any]) -> tuple[str, str]:
    reproduction = comparison["control_reproduction"]
    critical = ["seed_actions", "fee_after_pnl", "stress100_worst_pnl", "residual_qty_rate"]
    max_abs_rel = max(abs(float(reproduction[field]["relative_delta"] or 0.0)) for field in critical)
    if max_abs_rel > 0.10:
        return "UNKNOWN", "UNKNOWN_STATE_MACHINE_REPRODUCTION_GAP"
    retention = comparison["variant_delta"]["seed_action_retention"] or 0.0
    residual_reduction = comparison["variant_delta"]["residual_qty_rate_reduction"] or 0.0
    residual_cost_reduction = comparison["variant_delta"]["residual_cost_rate_reduction"] or 0.0
    if residual_reduction <= 0.0 or residual_cost_reduction <= 0.0:
        return "DISCARD", "DISCARD_LATE_REPAIR90_STATE_MACHINE_RESIDUAL_NOT_IMPROVED"
    if (
        retention >= 0.65
        and residual_reduction >= 0.50
        and residual_cost_reduction >= 0.50
        and float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    ):
        return "KEEP", "KEEP_LATE_REPAIR90_STATE_MACHINE_RESEARCH_ONLY"
    if retention < 0.50 or float(variant["fee_after_pnl"]) <= 0.0 or float(variant["stress100_worst_pnl"]) <= 0.0:
        return "DISCARD", "DISCARD_LATE_REPAIR90_SAMPLE_OR_STRESS_COLLAPSE"
    return "UNKNOWN", "UNKNOWN_LATE_REPAIR90_MIXED_STATE_MACHINE_RESULT"


def decision_public_px_cap(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> tuple[str, str]:
    retention = delta["seed_action_retention"] or 0.0
    pair_retention = delta["pair_qty_retention"] or 0.0
    residual_reduction = delta["residual_qty_rate_reduction"] or 0.0
    residual_cost_reduction = delta["residual_cost_rate_reduction"] or 0.0
    pair_cost_reduction = delta["weighted_pair_cost_reduction"] or 0.0
    if retention < 0.50 or float(variant["fee_after_pnl"]) <= 0.0 or float(variant["stress100_worst_pnl"]) <= 0.0:
        return "DISCARD", "DISCARD_PUBLIC_PX_CAP_SAMPLE_OR_STRESS_COLLAPSE"
    if residual_reduction <= 0.0 or residual_cost_reduction <= 0.0:
        return "DISCARD", "DISCARD_PUBLIC_PX_CAP_RESIDUAL_NOT_IMPROVED"
    if pair_cost_reduction <= 0.0:
        return "UNKNOWN", "UNKNOWN_PUBLIC_PX_CAP_RESIDUAL_IMPROVES_PAIR_COST_NOT"
    if (
        retention >= 0.65
        and pair_retention >= 0.65
        and residual_reduction >= 0.10
        and residual_cost_reduction >= 0.10
        and float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    ):
        return "KEEP", "KEEP_PUBLIC_PX_CAP_RESEARCH_ONLY"
    return "UNKNOWN", "UNKNOWN_PUBLIC_PX_CAP_MIXED_STATE_MACHINE_RESULT"


def decision_risk_px_cap(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> tuple[str, str]:
    retention = delta["seed_action_retention"] or 0.0
    pair_retention = delta["pair_qty_retention"] or 0.0
    residual_reduction = delta["residual_qty_rate_reduction"] or 0.0
    residual_cost_reduction = delta["residual_cost_rate_reduction"] or 0.0
    pair_cost_reduction = delta["weighted_pair_cost_reduction"] or 0.0
    if retention < 0.50 or float(variant["fee_after_pnl"]) <= 0.0 or float(variant["stress100_worst_pnl"]) <= 0.0:
        return "DISCARD", "DISCARD_RISK_PUBLIC_PX_CAP_SAMPLE_OR_STRESS_COLLAPSE"
    if residual_reduction < -1e-9 or residual_cost_reduction < -1e-9:
        return "DISCARD", "DISCARD_RISK_PUBLIC_PX_CAP_RESIDUAL_WORSE"
    if pair_cost_reduction <= 0.0:
        return "UNKNOWN", "UNKNOWN_RISK_PUBLIC_PX_CAP_NO_PAIR_COST_IMPROVEMENT"
    if (
        retention >= 0.65
        and pair_retention >= 0.65
        and float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    ):
        return "KEEP", "KEEP_RISK_PUBLIC_PX_CAP_RESEARCH_ONLY"
    return "UNKNOWN", "UNKNOWN_RISK_PUBLIC_PX_CAP_MIXED_STATE_MACHINE_RESULT"


def decision_side_risk_px_cap(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> tuple[str, str]:
    retention = delta["seed_action_retention"] or 0.0
    pair_retention = delta["pair_qty_retention"] or 0.0
    residual_reduction = delta["residual_qty_rate_reduction"] or 0.0
    residual_cost_reduction = delta["residual_cost_rate_reduction"] or 0.0
    pair_cost_reduction = delta["weighted_pair_cost_reduction"] or 0.0
    if retention < 0.50 or float(variant["fee_after_pnl"]) <= 0.0 or float(variant["stress100_worst_pnl"]) <= 0.0:
        return "DISCARD", "DISCARD_SIDE_RISK_PUBLIC_PX_CAP_SAMPLE_OR_STRESS_COLLAPSE"
    if residual_reduction < -1e-9 or residual_cost_reduction < -1e-9:
        return "DISCARD", "DISCARD_SIDE_RISK_PUBLIC_PX_CAP_RESIDUAL_WORSE"
    if pair_cost_reduction <= 0.0:
        return "DISCARD", "DISCARD_SIDE_RISK_PUBLIC_PX_CAP_NO_PAIR_COST_IMPROVEMENT"
    if (
        retention >= 0.80
        and pair_retention >= 0.80
        and float(variant["fee_after_pnl"]) > 0.0
        and float(variant["stress100_worst_pnl"]) > 0.0
        and float(variant["worst_day_fee_after_pnl"]) > 0.0
    ):
        return "KEEP", "KEEP_SIDE_RISK_PUBLIC_PX_CAP_RESEARCH_ONLY"
    return "UNKNOWN", "UNKNOWN_SIDE_RISK_PUBLIC_PX_CAP_MIXED_STATE_MACHINE_RESULT"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", default=str(DEFAULT_CANDIDATE_BASE_DIR))
    parser.add_argument("--baseline-result-dir", default=str(DEFAULT_BASELINE_RESULT_DIR))
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = Path(args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}")
    candidate_base_dir = Path(args.candidate_base_dir)
    baseline_result_dir = Path(args.baseline_result_dir)
    candidate_manifest_path = candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_db_path = candidate_base_dir / "candidate_base.duckdb"
    result_manifest_path = baseline_result_dir / "RESULT_SUMMARY_MANIFEST.json"
    compliance_manifest_path = baseline_result_dir / "COMPLIANCE_MANIFEST.json"
    required = [candidate_manifest_path, candidate_db_path, result_manifest_path, compliance_manifest_path]
    missing = [str(path) for path in required if not path.exists()]
    unsafe = [str(path) for path in required if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "artifact": ARTIFACT,
            "created_utc": label,
            "decision_label": "BLOCKED",
            "status": "BLOCKED_CANDIDATE_PIPELINE_INPUT_UNAVAILABLE",
            "missing": missing,
            "unsafe": unsafe,
            "next_action": "Restore required local candidate pipeline V1 artifacts and rerun the late-repair90 state-machine verifier.",
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    candidate_manifest = read_json(candidate_manifest_path)
    result_manifest = read_json(result_manifest_path)
    compliance_manifest = read_json(compliance_manifest_path)
    base_day_counts = {str(day): int(count) for day, count in (candidate_manifest.get("day_counts") or {}).items()}
    core = result_manifest.get("core_metrics") or {}
    profiles = [
        Profile(name="control_seed_offset_max_120", seed_offset_max_s=120.0),
        Profile(name="discarded_hard_seed_offset_max_90", seed_offset_max_s=90.0),
        Profile(
            name="variant_late_repair_only_after_90",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
        ),
        Profile(
            name="variant_late_repair90_public_trade_px_hi_0p55",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            public_trade_px_hi=0.55,
        ),
        Profile(
            name="variant_late_repair90_risk_public_trade_px_hi_0p55",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            risk_increasing_public_trade_px_hi=0.55,
        ),
        Profile(
            name="variant_late_repair90_no_side_risk_public_trade_px_hi_0p55",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            risk_increasing_public_trade_px_hi=0.55,
            risk_increasing_public_trade_px_hi_side="NO",
        ),
    ]
    run_result = run_profiles(candidate_db_path, profiles, base_day_counts)
    control = run_result["profiles"]["control_seed_offset_max_120"]
    hard_offset90 = run_result["profiles"]["discarded_hard_seed_offset_max_90"]
    variant = run_result["profiles"]["variant_late_repair_only_after_90"]
    public_px_variant = run_result["profiles"]["variant_late_repair90_public_trade_px_hi_0p55"]
    risk_px_variant = run_result["profiles"]["variant_late_repair90_risk_public_trade_px_hi_0p55"]
    no_side_risk_px_variant = run_result["profiles"]["variant_late_repair90_no_side_risk_public_trade_px_hi_0p55"]
    baseline = {
        "seed_actions": core.get("seed_actions"),
        "active_markets": core.get("active_markets"),
        "pair_actions": core.get("pair_actions"),
        "pair_qty": core.get("pair_qty"),
        "weighted_pair_cost": core.get("weighted_pair_cost"),
        "gross_pnl": core.get("gross_pnl"),
        "official_taker_fee": core.get("official_taker_fee"),
        "fee_after_pnl": core.get("fee_after_pnl"),
        "stress100_worst_pnl": core.get("stress100_worst_pnl"),
        "worst_day_fee_after_pnl": core.get("worst_day_fee_after_pnl"),
        "residual_qty_rate": core.get("residual_qty_rate"),
        "residual_cost_rate": core.get("residual_cost_rate"),
    }
    hard_offset90_comparison = build_comparison(baseline, control, hard_offset90)
    comparison = build_comparison(baseline, control, variant)
    late_repair_decision_label, late_repair_status = decision_from(control, variant, comparison)
    public_px_delta = build_direct_delta(variant, public_px_variant)
    risk_px_delta = build_direct_delta(variant, risk_px_variant)
    no_side_risk_px_delta = build_direct_delta(variant, no_side_risk_px_variant)
    hard_cap_decision_label, hard_cap_status = decision_public_px_cap(variant, public_px_variant, public_px_delta)
    all_side_risk_decision_label, all_side_risk_status = decision_risk_px_cap(
        variant, risk_px_variant, risk_px_delta
    )
    decision_label, status = decision_side_risk_px_cap(variant, no_side_risk_px_variant, no_side_risk_px_delta)
    manifest = {
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "causal_verifier",
        "decision_label": decision_label,
        "status": status,
        "hypothesis": (
            "If the no-order pair-source p90 tail is driven by high source public_trade_px, then adding a bounded "
            "NO-side-only risk-increasing public_trade_px_hi cap to late-repair90 should reduce pair-cost risk while "
            "preserving repair seeds, material sample, and positive fee/stress/worst-day."
        ),
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "baseline_result_manifest": str(result_manifest_path),
            "baseline_compliance_manifest": str(compliance_manifest_path),
        },
        "scope": {
            "dataset_type": candidate_manifest.get("dataset_type"),
            "labels": candidate_manifest.get("labels"),
            "days": candidate_manifest.get("days"),
            "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
            "public_account_execution_truth_v1_included": True,
            "public_account_execution_truth_v1_private_truth": False,
            "deployable": False,
            "can_support_strategy_promotion": False,
            "promotion_gate_pass": False,
            "result_classification": "PASS_LOCAL_COMPLETION_RESEARCH_ONLY",
        },
        "config": {
            "control": asdict(profiles[0]),
            "discarded_hard_offset90": asdict(profiles[1]),
            "late_repair90_variant": asdict(profiles[2]),
            "discarded_public_px_cap_variant": asdict(profiles[3]),
            "discarded_all_side_risk_public_px_cap_variant": asdict(profiles[4]),
            "no_side_risk_public_px_cap_variant": asdict(profiles[5]),
            "candidate_source": "candidate_base table, public_sell rows with 0 <= offset_s < 300",
            "official_fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "official_fee_rate": profiles[0].official_fee_rate,
        },
        "processed_public_sell_rows": run_result["processed_public_sell_rows"],
        "baseline_reported": baseline,
        "control_rerun": control,
        "discarded_hard_offset90_rerun": hard_offset90,
        "variant_late_repair90_rerun": variant,
        "variant_late_repair90_public_trade_px_hi_0p55_rerun": public_px_variant,
        "variant_late_repair90_risk_public_trade_px_hi_0p55_rerun": risk_px_variant,
        "variant_late_repair90_no_side_risk_public_trade_px_hi_0p55_rerun": no_side_risk_px_variant,
        "hard_offset90_comparison": hard_offset90_comparison,
        "late_repair90_comparison": comparison,
        "late_repair90_decision_label": late_repair_decision_label,
        "late_repair90_status": late_repair_status,
        "hard_public_px_cap_decision_label": hard_cap_decision_label,
        "hard_public_px_cap_status": hard_cap_status,
        "all_side_risk_public_px_cap_decision_label": all_side_risk_decision_label,
        "all_side_risk_public_px_cap_status": all_side_risk_status,
        "public_px_cap_delta_vs_late_repair90": public_px_delta,
        "risk_public_px_cap_delta_vs_late_repair90": risk_px_delta,
        "no_side_risk_public_px_cap_delta_vs_late_repair90": no_side_risk_px_delta,
        "scoreboard_delta": {
            "control_seed_actions": control["seed_actions"],
            "late_repair90_seed_actions": variant["seed_actions"],
            "no_side_risk_public_px_cap_seed_actions": no_side_risk_px_variant["seed_actions"],
            "no_side_risk_public_px_cap_seed_action_retention_vs_late_repair90": no_side_risk_px_delta[
                "seed_action_retention"
            ],
            "late_repair90_fee_after_pnl": variant["fee_after_pnl"],
            "no_side_risk_public_px_cap_fee_after_pnl": no_side_risk_px_variant["fee_after_pnl"],
            "late_repair90_stress100_worst_pnl": variant["stress100_worst_pnl"],
            "no_side_risk_public_px_cap_stress100_worst_pnl": no_side_risk_px_variant["stress100_worst_pnl"],
            "late_repair90_worst_day_fee_after_pnl": variant["worst_day_fee_after_pnl"],
            "no_side_risk_public_px_cap_worst_day_fee_after_pnl": no_side_risk_px_variant[
                "worst_day_fee_after_pnl"
            ],
            "late_repair90_weighted_pair_cost": variant["weighted_pair_cost"],
            "no_side_risk_public_px_cap_weighted_pair_cost": no_side_risk_px_variant["weighted_pair_cost"],
            "weighted_pair_cost_reduction": no_side_risk_px_delta["weighted_pair_cost_reduction"],
            "late_repair90_residual_qty_rate": variant["residual_qty_rate"],
            "no_side_risk_public_px_cap_residual_qty_rate": no_side_risk_px_variant["residual_qty_rate"],
            "residual_qty_rate_reduction": no_side_risk_px_delta["residual_qty_rate_reduction"],
            "late_repair90_residual_cost_rate": variant["residual_cost_rate"],
            "no_side_risk_public_px_cap_residual_cost_rate": no_side_risk_px_variant["residual_cost_rate"],
            "residual_cost_rate_reduction": no_side_risk_px_delta["residual_cost_rate_reduction"],
            "no_side_risk_public_px_cap_block_count": no_side_risk_px_variant[
                "seed_block_risk_increasing_public_trade_px_hi"
            ],
            "discarded_all_side_risk_public_px_cap_seed_actions": risk_px_variant["seed_actions"],
            "discarded_all_side_risk_public_px_cap_residual_qty_rate": risk_px_variant["residual_qty_rate"],
            "discarded_hard_public_px_cap_seed_actions": public_px_variant["seed_actions"],
            "discarded_hard_public_px_cap_residual_qty_rate": public_px_variant["residual_qty_rate"],
        },
        "interpretation": [
            "This is a local candidate-base state-machine rerun, not source-of-truth replay and not deployable evidence.",
            "The control profile is included to expose implementation drift against the official pipeline result.",
            "The discarded hard offset90 profile is retained only as a contrast against the repair-only late gate.",
            "The late-repair90 variant is retained as the control for the new source-public-price cap.",
            "The hard public price cap is retained only as a discarded contrast.",
            "The all-side risk-increasing public price cap is retained only as a discarded contrast.",
            "The NO-side-only risk-increasing public price cap is a local candidate-base proxy for the no-order pair-source attribution and is not source-of-truth replay.",
        ],
        "next_action": (
            "If KEEP, implement/smoke the bounded NO-side-only risk-increasing public_trade_px_hi admission cap in the "
            "no-order shadow runner before spending another EC2 shadow; if DISCARD, freeze source-public-price cap "
            "families and choose a new non-price mechanism."
        ),
        "side_effects": {
            "candidate_base_duckdb_read_only": True,
            "full_completion_store_scanned": False,
            "raw_scanned": False,
            "replay_scanned": False,
            "collector_scanned": False,
            "ssh_started": False,
            "shared_ingress_connected": False,
            "network_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "compliance_summary": compliance_manifest.get("compliance_summary"),
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
