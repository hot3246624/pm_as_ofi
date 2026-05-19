#!/usr/bin/env python3
"""D-branch passive/passive + redeem research backtest on P2 event stores.

This is an offline research tool. It reads only completion_unwind_event_store_v2
DuckDB files and writes independent artifacts. It does not touch raw/replay data
or live services.

Model intent:
- Passive BUY both YES/NO from public SELL flow, using a configurable passive
  price improvement approximation.
- Pair complementary inventory internally with no taker fee.
- Settle residual inventory by winner_side, while also reporting worst-residual
  stress to avoid directional-luck illusions.

This is a B27-inspired branch combined with xuan-style lot/inventory accounting.
It is research-only; public SELL flow is not private queue truth.
"""

from __future__ import annotations

import argparse
import csv
import gc
import json
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


DUST = 1e-9
TIERS = (10, 25, 60, 100, 250)


@dataclass(frozen=True)
class Config:
    name: str
    edge: float
    target_qty: float
    seed_px_lo: float
    seed_px_hi: float
    target_policy: str = "fixed"
    alignment: str = "high"
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_open_cost: float = 250.0
    seed_offset_min_s: float = 0.0
    seed_offset_max_s: float = 120.0
    seed_l1_pair_cap: float = 1.02
    cooldown_ms: int = 5_000
    imbalance_qty_cap: float = 1_000_000_000.0
    imbalance_cost_cap: float = 1_000_000_000.0
    dust_qty: float = 1.0
    taker_fee_rate: float = 0.07
    salvage_net_cap: float = 0.0
    salvage_age_s: float = 30.0
    salvage_min_lot_cost: float = 0.25
    max_salvage_qty: float = 250.0
    salvage_select: str = "fifo"
    residual_cooldown_age_s: float = 0.0
    residual_cooldown_cost_cap: float = 1_000_000_000.0
    pair_select: str = "fifo"


@dataclass
class Lot:
    qty: float
    px: float
    ts_ms: int
    side: str


@dataclass
class State:
    winner: str | None = None
    day: str | None = None
    slug: str | None = None
    inv: dict[str, deque[Lot]] | None = None
    last_seed_ts: int = -(10**18)
    last_ts_ms: int = 0
    active: bool = False

    def __post_init__(self) -> None:
        if self.inv is None:
            self.inv = {"YES": deque(), "NO": deque()}


COLS = [
    "day",
    "event_kind",
    "event_id",
    "ts_ms",
    "condition_id",
    "slug",
    "offset_s",
    "side",
    "winner_side",
    "side_alignment",
    "l1_pair_ask",
    "public_trade_taker_side",
    "public_trade_price",
    "public_trade_size",
    "book_update_reason",
]
for tier in TIERS:
    COLS.extend([f"buy_full_{tier}", f"buy_vwap_{tier}", f"buy_filled_{tier}", f"buy_worst_px_{tier}"])

IDX = {name: idx for idx, name in enumerate(COLS)}


def get(row: tuple[Any, ...], name: str) -> Any:
    return row[IDX[name]]


def fnum(value: Any, default: float = math.nan) -> float:
    if value is None:
        return default
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def other(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def lot_qty(lots: deque[Lot]) -> float:
    return sum(lot.qty for lot in lots)


def lot_cost(lots: deque[Lot]) -> float:
    return sum(lot.qty * lot.px for lot in lots)


def aged_lot_cost(lots: deque[Lot], ts_ms: int, age_s: float) -> float:
    if age_s <= 0.0:
        return lot_cost(lots)
    cutoff_ms = age_s * 1000.0
    return sum(lot.qty * lot.px for lot in lots if ts_ms - lot.ts_ms >= cutoff_ms)


def fee_per_share(px: float, rate: float) -> float:
    if not math.isfinite(px):
        return 0.0
    return rate * min(max(px, 0.0), max(1.0 - px, 0.0))


def target_for_px(cfg: Config, px: float) -> float:
    if cfg.target_policy == "mid3":
        return min(cfg.target_qty, 3.0) if 0.25 <= px <= 0.75 else cfg.target_qty
    if cfg.target_policy == "mid2":
        return min(cfg.target_qty, 2.0) if 0.25 <= px <= 0.75 else cfg.target_qty
    if cfg.target_policy == "edge7_mid3":
        return min(cfg.target_qty, 3.0) if 0.25 <= px <= 0.75 else max(cfg.target_qty, 7.0)
    if cfg.target_policy == "edge8_mid2":
        return min(cfg.target_qty, 2.0) if 0.25 <= px <= 0.75 else max(cfg.target_qty, 8.0)
    return cfg.target_qty


def choose_buy_vwap(row: tuple[Any, ...], qty: float) -> tuple[float, float, int]:
    for tier in TIERS:
        full = bool(get(row, f"buy_full_{tier}"))
        filled = fnum(get(row, f"buy_filled_{tier}"), 0.0)
        vwap = fnum(get(row, f"buy_vwap_{tier}"))
        if full and filled + 1e-9 >= qty and not math.isnan(vwap):
            return vwap, qty, tier
    tier = TIERS[-1]
    filled = min(qty, fnum(get(row, f"buy_filled_{tier}"), 0.0))
    vwap = fnum(get(row, f"buy_vwap_{tier}"))
    if filled <= DUST or math.isnan(vwap):
        return math.nan, 0.0, tier
    return vwap, filled, tier


def new_metrics() -> defaultdict[str, float]:
    return defaultdict(float)


def pop_lot(lots: deque[Lot], idx: int) -> Lot:
    if idx == 0:
        return lots.popleft()
    lots.rotate(-idx)
    lot = lots.popleft()
    lots.rotate(idx)
    return lot


def choose_pair_indices(yes: deque[Lot], no: deque[Lot], mode: str) -> tuple[int, int]:
    if mode == "low_pair_cost":
        return (
            min(range(len(yes)), key=lambda idx: yes[idx].px),
            min(range(len(no)), key=lambda idx: no[idx].px),
        )
    if mode == "high_cost":
        return (
            max(range(len(yes)), key=lambda idx: yes[idx].px),
            max(range(len(no)), key=lambda idx: no[idx].px),
        )
    return 0, 0


def pair_inventory(cfg: Config, state: State, m: defaultdict[str, float], ts_ms: int) -> None:
    yes = state.inv["YES"]  # type: ignore[index]
    no = state.inv["NO"]  # type: ignore[index]
    while yes and no:
        yes_idx, no_idx = choose_pair_indices(yes, no, cfg.pair_select)
        a = yes[yes_idx]
        b = no[no_idx]
        take = min(a.qty, b.qty)
        if take <= DUST:
            break
        pair_cost = a.px + b.px
        older_ts = min(a.ts_ms, b.ts_ms)
        m["pair_actions"] += 1
        m["pair_qty"] += take
        m["pair_cost_sum"] += take * pair_cost
        m["net_pair_cost_sum"] += take * pair_cost
        m["pair_pnl"] += take * (1.0 - pair_cost)
        m["pair_delay_ms"] += take * max(0, ts_ms - older_ts)
        a.qty -= take
        b.qty -= take
        if a.qty <= DUST:
            pop_lot(yes, yes_idx)
        if b.qty <= DUST:
            pop_lot(no, no_idx)


def maybe_seed(row: tuple[Any, ...], cfg: Config, state: State, m: defaultdict[str, float]) -> None:
    event_kind = get(row, "event_kind")
    if event_kind != "public_trade":
        return
    taker_side = get(row, "public_trade_taker_side")
    if taker_side != "SELL":
        return
    side = str(get(row, "side") or "")
    if side not in ("YES", "NO"):
        return
    alignment = str(get(row, "side_alignment") or "")
    if cfg.alignment != "all" and alignment != cfg.alignment:
        m["seed_block_alignment"] += 1
        return
    offset_s = fnum(get(row, "offset_s"))
    if not (cfg.seed_offset_min_s <= offset_s < cfg.seed_offset_max_s):
        m["seed_block_offset"] += 1
        return
    trade_px = fnum(get(row, "public_trade_price"))
    trade_size = fnum(get(row, "public_trade_size"), 0.0)
    if trade_size <= DUST or math.isnan(trade_px):
        return
    if not (cfg.seed_px_lo <= trade_px <= cfg.seed_px_hi):
        m["seed_block_price_band"] += 1
        return
    target_qty = target_for_px(cfg, trade_px)
    l1_pair = fnum(get(row, "l1_pair_ask"))
    if math.isnan(l1_pair) or l1_pair > cfg.seed_l1_pair_cap + 1e-12:
        m["seed_block_l1_pair_cap"] += 1
        return
    ts_ms = int(get(row, "ts_ms") or 0)
    if ts_ms - state.last_seed_ts < cfg.cooldown_ms:
        m["seed_block_cooldown"] += 1
        return

    inv = state.inv  # type: ignore[assignment]
    same_qty = lot_qty(inv[side])
    opp_qty = lot_qty(inv[other(side)])
    aged_cost = aged_lot_cost(inv["YES"], ts_ms, cfg.residual_cooldown_age_s) + aged_lot_cost(
        inv["NO"], ts_ms, cfg.residual_cooldown_age_s
    )
    if (
        aged_cost > cfg.residual_cooldown_cost_cap + 1e-12
        and same_qty + cfg.dust_qty >= opp_qty
    ):
        m["seed_block_residual_cooldown"] += 1
        return
    if same_qty >= target_qty - cfg.dust_qty:
        m["seed_block_target"] += 1
        return
    same_cost = lot_cost(inv[side])
    opp_cost = lot_cost(inv[other(side)])
    if max(0.0, same_cost - opp_cost) > cfg.imbalance_cost_cap + 1e-12:
        m["seed_block_imbalance_cost"] += 1
        return

    px = max(0.01, trade_px - cfg.edge)
    open_cost = lot_cost(inv["YES"]) + lot_cost(inv["NO"])
    imbalance_room = cfg.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
    if imbalance_room <= cfg.dust_qty:
        m["seed_block_imbalance_qty"] += 1
        return
    qty = min(
        cfg.max_seed_qty,
        trade_size * cfg.fill_haircut,
        target_qty - same_qty,
        (cfg.max_open_cost - open_cost) / max(px, 1e-9),
        imbalance_room,
    )
    if qty <= cfg.dust_qty:
        return

    inv[side].append(Lot(qty=qty, px=px, ts_ms=ts_ms, side=side))
    state.last_seed_ts = ts_ms
    state.active = True
    m["seed_actions"] += 1
    m["gross_buy_qty"] += qty
    m["gross_buy_cost"] += qty * px
    m["seed_qty"] += qty
    m["seed_cost"] += qty * px
    pair_inventory(cfg, state, m, ts_ms)


def maybe_fak_salvage(row: tuple[Any, ...], cfg: Config, state: State, m: defaultdict[str, float]) -> None:
    if cfg.salvage_net_cap <= 0.0:
        return
    side = str(get(row, "side") or "")
    if side not in ("YES", "NO"):
        return
    held_side = other(side)
    lots = state.inv[held_side]  # type: ignore[index]
    if not lots:
        return
    ts_ms = int(get(row, "ts_ms") or 0)
    desired = min(lot_qty(lots), cfg.max_salvage_qty)
    comp_px, fill_qty, _tier = choose_buy_vwap(row, desired)
    if fill_qty <= DUST or math.isnan(comp_px):
        return

    comp_fee = fee_per_share(comp_px, cfg.taker_fee_rate)

    def choose_lot_index() -> int | None:
        if cfg.salvage_select == "fifo":
            return 0 if lots else None
        best: tuple[float, float, int] | None = None
        for idx, candidate in enumerate(lots):
            age_s = (ts_ms - candidate.ts_ms) / 1000.0
            lot_cost_value = candidate.qty * candidate.px
            if age_s < cfg.salvage_age_s or lot_cost_value < cfg.salvage_min_lot_cost:
                continue
            net_pair = candidate.px + comp_px + comp_fee
            if net_pair > cfg.salvage_net_cap + 1e-12:
                continue
            # Prefer the lot that gives the cleanest executable pair; tie-break
            # toward older/larger lots so dust does not block material repair.
            key = (net_pair, -age_s, -lot_cost_value)
            if best is None or key < best:
                best = (key[0], key[1], idx)
        return best[2] if best is not None else None

    paired = 0.0
    while lots and paired < fill_qty - DUST:
        lot_idx = choose_lot_index()
        if lot_idx is None:
            break
        lot = lots[lot_idx]
        age_s = (ts_ms - lot.ts_ms) / 1000.0
        lot_cost_value = lot.qty * lot.px
        if age_s < cfg.salvage_age_s or lot_cost_value < cfg.salvage_min_lot_cost:
            break
        gross_pair = lot.px + comp_px
        net_pair = gross_pair + comp_fee
        if net_pair > cfg.salvage_net_cap + 1e-12:
            break
        take = min(lot.qty, fill_qty - paired)
        paired += take
        m["salvage_actions"] += 1
        m["salvage_qty"] += take
        m["taker_fee"] += take * comp_fee
        m["gross_buy_qty"] += take
        m["gross_buy_cost"] += take * comp_px + take * comp_fee
        m["completion_cost"] += take * comp_px
        m["pair_actions"] += 1
        m["pair_qty"] += take
        m["pair_cost_sum"] += take * gross_pair
        m["net_pair_cost_sum"] += take * net_pair
        m["pair_pnl"] += take * (1.0 - net_pair)
        m["pair_delay_ms"] += take * max(0, ts_ms - lot.ts_ms)
        lot.qty -= take
        if lot.qty <= DUST:
            if lot_idx == 0:
                lots.popleft()
            else:
                lots.rotate(-lot_idx)
                lots.popleft()
                lots.rotate(lot_idx)
        else:
            break


def settle(states: dict[str, State], cfg: Config, m: defaultdict[str, float], residual_rows: list[dict[str, Any]] | None) -> None:
    for condition_id, state in states.items():
        if not state.active:
            continue
        pair_inventory(cfg, state, m, state.last_ts_ms)
        m["active_markets"] += 1
        winner = state.winner
        for side in ("YES", "NO"):
            lots = state.inv[side]  # type: ignore[index]
            for lot in lots:
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                payout = lot.qty if winner == side else 0.0
                m["residual_qty"] += lot.qty
                m["residual_cost"] += cost
                m["residual_settle_payout"] += payout
                m["residual_settle_pnl"] += payout - cost
                if cost > 6.0:
                    m["resid_cost_gt6_markets"] += 1
                if residual_rows is not None:
                    residual_rows.append(
                        {
                            "config": cfg.name,
                            "condition_id": condition_id,
                            "day": state.day,
                            "slug": state.slug,
                            "winner": winner,
                            "side": side,
                            "qty": lot.qty,
                            "px": lot.px,
                            "cost": cost,
                            "payout": payout,
                            "pnl": payout - cost,
                            "age_s": max(0.0, (state.last_ts_ms - lot.ts_ms) / 1000.0),
                        }
                    )


def finish(cfg: Config, m: defaultdict[str, float]) -> dict[str, Any]:
    buy_cost = m["gross_buy_cost"]
    buy_qty = m["gross_buy_qty"]
    pair_qty = m["pair_qty"]
    actual_pnl = m["pair_pnl"] + m["residual_settle_pnl"]
    worst_residual_pnl = m["pair_pnl"] - m["residual_cost"]
    flat_residual_pnl = m["pair_pnl"]
    active = m["active_markets"]
    return {
        "name": cfg.name,
        "edge": cfg.edge,
        "target_qty": cfg.target_qty,
        "target_policy": cfg.target_policy,
        "seed_px_lo": cfg.seed_px_lo,
        "seed_px_hi": cfg.seed_px_hi,
        "alignment": cfg.alignment,
        "fill_haircut": cfg.fill_haircut,
        "seed_l1_pair_cap": cfg.seed_l1_pair_cap,
        "imbalance_qty_cap": cfg.imbalance_qty_cap,
        "imbalance_cost_cap": cfg.imbalance_cost_cap,
        "salvage_net_cap": cfg.salvage_net_cap,
        "salvage_age_s": cfg.salvage_age_s,
        "salvage_min_lot_cost": cfg.salvage_min_lot_cost,
        "salvage_select": cfg.salvage_select,
        "residual_cooldown_age_s": cfg.residual_cooldown_age_s,
        "residual_cooldown_cost_cap": cfg.residual_cooldown_cost_cap,
        "pair_select": cfg.pair_select,
        "active_markets": int(active),
        "seed_actions": int(m["seed_actions"]),
        "pair_actions": int(m["pair_actions"]),
        "gross_buy_qty": round(buy_qty, 6),
        "gross_buy_cost": round(buy_cost, 6),
        "taker_fee": round(m["taker_fee"], 6),
        "salvage_actions": int(m["salvage_actions"]),
        "salvage_qty": round(m["salvage_qty"], 6),
        "completion_cost": round(m["completion_cost"], 6),
        "pair_qty": round(pair_qty, 6),
        "pair_share_rate": round((2 * pair_qty / buy_qty) if buy_qty else 0.0, 6),
        "pair_cost_wavg": round((m["pair_cost_sum"] / pair_qty) if pair_qty else 0.0, 6),
        "net_pair_cost_wavg": round((m["net_pair_cost_sum"] / pair_qty) if pair_qty else 0.0, 6),
        "pair_delay_wavg_s": round((m["pair_delay_ms"] / pair_qty / 1000.0) if pair_qty else 0.0, 6),
        "rounds_per_market": round((m["pair_actions"] / active) if active else 0.0, 6),
        "residual_qty": round(m["residual_qty"], 6),
        "residual_cost": round(m["residual_cost"], 6),
        "residual_settle_payout": round(m["residual_settle_payout"], 6),
        "residual_settle_pnl": round(m["residual_settle_pnl"], 6),
        "qty_residual_rate": round((m["residual_qty"] / buy_qty) if buy_qty else 0.0, 6),
        "cost_residual_rate": round((m["residual_cost"] / buy_cost) if buy_cost else 0.0, 6),
        "residual_cost_gt6_market_rate": round((m["resid_cost_gt6_markets"] / active) if active else 0.0, 6),
        "pair_pnl": round(m["pair_pnl"], 6),
        "gross_pnl": round(actual_pnl, 6),
        "net_pnl": round(actual_pnl, 6),
        "net_roi": round((actual_pnl / buy_cost) if buy_cost else 0.0, 6),
        "actual_settle_pnl": round(actual_pnl, 6),
        "actual_settle_roi": round((actual_pnl / buy_cost) if buy_cost else 0.0, 6),
        "worst_residual_net_pnl": round(worst_residual_pnl, 6),
        "worst_residual_roi": round((worst_residual_pnl / buy_cost) if buy_cost else 0.0, 6),
        "flat_residual_net_pnl": round(flat_residual_pnl, 6),
        "flat_residual_roi": round((flat_residual_pnl / buy_cost) if buy_cost else 0.0, 6),
        "stress100_actual_pnl": round(actual_pnl - 0.01 * (2 * pair_qty + m["residual_qty"]), 6),
        "stress100_worst_pnl": round(worst_residual_pnl - 0.01 * (2 * pair_qty + m["residual_qty"]), 6),
        "seed_block_alignment": int(m["seed_block_alignment"]),
        "seed_block_price_band": int(m["seed_block_price_band"]),
        "seed_block_l1_pair_cap": int(m["seed_block_l1_pair_cap"]),
        "seed_block_cooldown": int(m["seed_block_cooldown"]),
        "seed_block_target": int(m["seed_block_target"]),
        "seed_block_imbalance_qty": int(m["seed_block_imbalance_qty"]),
        "seed_block_imbalance_cost": int(m["seed_block_imbalance_cost"]),
        "seed_block_residual_cooldown": int(m["seed_block_residual_cooldown"]),
    }


def query_rows(con: duckdb.DuckDBPyConnection, days: list[str]) -> Any:
    select_cols = ",".join(COLS)
    placeholders = ",".join("?" for _ in days)
    return con.execute(
        f"""
        select {select_cols}
        from completion_unwind_events
        where day in ({placeholders})
          and offset_s >= 0 and offset_s < 300
          and side in ('YES','NO')
          and event_kind in ('l1_price_change','public_trade')
        order by condition_id, ts_ms, event_id, side
        """,
        days,
    )


def run_store(
    store: str,
    days: list[str],
    configs: list[Config],
    tmp: str,
    capture_residuals: bool = False,
) -> dict[str, Any]:
    con = duckdb.connect(store, read_only=True)
    con.execute(f"PRAGMA temp_directory='{tmp}'")
    con.execute("PRAGMA threads=2")
    con.execute("PRAGMA memory_limit='4GB'")
    states: dict[str, dict[str, State]] = {cfg.name: {} for cfg in configs}
    metrics: dict[str, defaultdict[str, float]] = {cfg.name: new_metrics() for cfg in configs}
    cur = query_rows(con, days)
    start = time.time()
    batches = 0
    while True:
        rows = cur.fetchmany(50_000)
        if not rows:
            break
        batches += 1
        if batches % 20 == 0:
            print(json.dumps({"store": store, "days": days, "batches": batches, "elapsed_s": round(time.time() - start, 1)}), flush=True)
        for row in rows:
            condition_id = str(get(row, "condition_id") or "")
            if not condition_id:
                continue
            winner = get(row, "winner_side")
            ts_ms = int(get(row, "ts_ms") or 0)
            for cfg in configs:
                by_market = states[cfg.name]
                state = by_market.get(condition_id)
                if state is None:
                    state = State(
                        winner=str(winner) if winner else None,
                        day=str(get(row, "day") or ""),
                        slug=str(get(row, "slug") or ""),
                    )
                    by_market[condition_id] = state
                elif state.winner is None and winner:
                    state.winner = str(winner)
                state.last_ts_ms = max(state.last_ts_ms, ts_ms)
                maybe_fak_salvage(row, cfg, state, metrics[cfg.name])
                maybe_seed(row, cfg, state, metrics[cfg.name])
    residual_rows: list[dict[str, Any]] = []
    results: dict[str, Any] = {}
    for cfg in configs:
        settle(states[cfg.name], cfg, metrics[cfg.name], residual_rows if capture_residuals else None)
        results[cfg.name] = finish(cfg, metrics[cfg.name])
        results[cfg.name]["store"] = store
        results[cfg.name]["days"] = days
    if capture_residuals:
        results["__residual_rows__"] = residual_rows
    con.close()
    del states
    gc.collect()
    return results


def parse_store_arg(values: list[str]) -> list[tuple[str, list[str]]]:
    out: list[tuple[str, list[str]]] = []
    for value in values:
        store, _, days_s = value.partition(":")
        days = [item for item in days_s.split(",") if item]
        if not days:
            raise SystemExit(f"store argument must be STORE:YYYY-MM-DD[,YYYY-MM-DD...], got {value}")
        out.append((store, days))
    return out


def aggregate_results(configs: list[Config], per_store: list[dict[str, Any]]) -> list[dict[str, Any]]:
    numeric_keys = [
        "active_markets",
        "seed_actions",
        "pair_actions",
        "gross_buy_qty",
        "gross_buy_cost",
        "pair_qty",
        "residual_qty",
        "residual_cost",
        "residual_settle_payout",
        "residual_settle_pnl",
        "pair_pnl",
        "gross_pnl",
        "net_pnl",
        "actual_settle_pnl",
        "worst_residual_net_pnl",
        "flat_residual_net_pnl",
        "taker_fee",
        "salvage_actions",
        "salvage_qty",
        "completion_cost",
        "seed_block_alignment",
        "seed_block_price_band",
        "seed_block_l1_pair_cap",
        "seed_block_cooldown",
        "seed_block_target",
        "seed_block_imbalance_qty",
        "seed_block_imbalance_cost",
        "seed_block_residual_cooldown",
    ]
    weighted_keys = ["pair_cost_wavg", "net_pair_cost_wavg", "pair_delay_wavg_s"]
    combined: list[dict[str, Any]] = []
    for cfg in configs:
        rows = [result[cfg.name] for result in per_store if cfg.name in result]
        agg: dict[str, Any] = {
            "name": cfg.name,
            "edge": cfg.edge,
            "target_qty": cfg.target_qty,
            "target_policy": cfg.target_policy,
            "seed_px_lo": cfg.seed_px_lo,
            "seed_px_hi": cfg.seed_px_hi,
            "alignment": cfg.alignment,
            "fill_haircut": cfg.fill_haircut,
            "seed_l1_pair_cap": cfg.seed_l1_pair_cap,
            "imbalance_qty_cap": cfg.imbalance_qty_cap,
            "imbalance_cost_cap": cfg.imbalance_cost_cap,
            "taker_fee_rate": cfg.taker_fee_rate,
            "salvage_net_cap": cfg.salvage_net_cap,
            "salvage_age_s": cfg.salvage_age_s,
            "salvage_min_lot_cost": cfg.salvage_min_lot_cost,
            "salvage_select": cfg.salvage_select,
            "residual_cooldown_age_s": cfg.residual_cooldown_age_s,
            "residual_cooldown_cost_cap": cfg.residual_cooldown_cost_cap,
            "pair_select": cfg.pair_select,
        }
        for key in numeric_keys:
            agg[key] = round(sum(float(row.get(key, 0) or 0) for row in rows), 6)
        pair_qty = float(agg["pair_qty"])
        buy_qty = float(agg["gross_buy_qty"])
        buy_cost = float(agg["gross_buy_cost"])
        active = float(agg["active_markets"])
        for key in weighted_keys:
            agg[key] = round(
                (sum(float(row.get(key, 0) or 0) * float(row.get("pair_qty", 0) or 0) for row in rows) / pair_qty)
                if pair_qty
                else 0.0,
                6,
            )
        agg["pair_share_rate"] = round((2 * pair_qty / buy_qty) if buy_qty else 0.0, 6)
        agg["rounds_per_market"] = round((float(agg["pair_actions"]) / active) if active else 0.0, 6)
        agg["qty_residual_rate"] = round((float(agg["residual_qty"]) / buy_qty) if buy_qty else 0.0, 6)
        agg["cost_residual_rate"] = round((float(agg["residual_cost"]) / buy_cost) if buy_cost else 0.0, 6)
        agg["actual_settle_roi"] = round((float(agg["actual_settle_pnl"]) / buy_cost) if buy_cost else 0.0, 6)
        agg["net_roi"] = agg["actual_settle_roi"]
        agg["worst_residual_roi"] = round((float(agg["worst_residual_net_pnl"]) / buy_cost) if buy_cost else 0.0, 6)
        agg["flat_residual_roi"] = round((float(agg["flat_residual_net_pnl"]) / buy_cost) if buy_cost else 0.0, 6)
        agg["stress100_actual_pnl"] = round(float(agg["actual_settle_pnl"]) - 0.01 * (2 * pair_qty + float(agg["residual_qty"])), 6)
        agg["stress100_worst_pnl"] = round(float(agg["worst_residual_net_pnl"]) - 0.01 * (2 * pair_qty + float(agg["residual_qty"])), 6)
        combined.append(agg)
    combined.sort(key=lambda row: (row["stress100_worst_pnl"], row["actual_settle_pnl"]), reverse=True)
    return combined


def build_configs(args: argparse.Namespace) -> list[Config]:
    configs: list[Config] = []
    for alignment in args.alignments:
        for edge in args.edges:
            for target in args.targets:
                for lo, hi in args.px_bands:
                    for target_policy in args.target_policies:
                            for imb in args.imbalance_qty_caps:
                                for imb_cost in args.imbalance_cost_caps:
                                    for salvage_cap in args.salvage_net_caps:
                                        for salvage_select in args.salvage_selects:
                                            for residual_cooldown_cost_cap in args.residual_cooldown_cost_caps:
                                                for pair_select in args.pair_selects:
                                                    imb_suffix = "" if imb >= 1e8 else f"_imb{int(round(imb * 100)):03d}"
                                                    imb_cost_suffix = "" if imb_cost >= 1e8 else f"_imbc{int(round(imb_cost * 100)):03d}"
                                                    salvage_suffix = "" if salvage_cap <= 0 else f"_sv{int(round(salvage_cap*1000)):04d}"
                                                    select_suffix = "" if salvage_select == "fifo" else f"_{salvage_select}"
                                                    pair_suffix = "" if pair_select == "fifo" else f"_pair_{pair_select}"
                                                    rc_suffix = (
                                                        ""
                                                        if residual_cooldown_cost_cap >= 1e8
                                                        else f"_rc{int(round(args.residual_cooldown_age_s))}_{int(round(residual_cooldown_cost_cap*100)):03d}"
                                                    )
                                                    policy_suffix = "" if target_policy == "fixed" else f"_tp_{target_policy}"
                                                    name = (
                                                        f"dpass_{alignment}_e{int(round(edge*1000)):03d}_t{int(target)}"
                                                        f"_px{int(round(lo*1000)):03d}_{int(round(hi*1000)):03d}"
                                                        f"{imb_suffix}{imb_cost_suffix}{salvage_suffix}{select_suffix}{rc_suffix}{pair_suffix}{policy_suffix}"
                                                    )
                                                    configs.append(
                                                        Config(
                                                            name=name,
                                                            edge=edge,
                                                            target_qty=target,
                                                            seed_px_lo=lo,
                                                            seed_px_hi=hi,
                                                            target_policy=target_policy,
                                                            alignment=alignment,
                                                            fill_haircut=args.fill_haircut,
                                                            max_seed_qty=args.max_seed_qty,
                                                            max_open_cost=args.max_open_cost,
                                                            seed_offset_max_s=args.seed_offset_max_s,
                                                            seed_l1_pair_cap=args.seed_l1_pair_cap,
                                                            imbalance_qty_cap=imb,
                                                            imbalance_cost_cap=imb_cost,
                                                            taker_fee_rate=args.taker_fee_rate,
                                                            salvage_net_cap=salvage_cap,
                                                            salvage_age_s=args.salvage_age_s,
                                                            salvage_min_lot_cost=args.salvage_min_lot_cost,
                                                            salvage_select=salvage_select,
                                                            residual_cooldown_age_s=args.residual_cooldown_age_s,
                                                            residual_cooldown_cost_cap=residual_cooldown_cost_cap,
                                                            pair_select=pair_select,
                                                        )
                                                    )
    return configs


def parse_px_bands(values: list[str]) -> list[tuple[float, float]]:
    bands: list[tuple[float, float]] = []
    for value in values:
        lo_s, _, hi_s = value.partition(":")
        if not hi_s:
            raise SystemExit(f"px band must be LO:HI, got {value}")
        bands.append((float(lo_s), float(hi_s)))
    return bands


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", action="append", required=True, help="STORE:day[,day...]")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--tmp-dir", default="/home/ubuntu/duckdb_tmp_xuan_d_branch")
    parser.add_argument("--edges", type=float, nargs="+", default=[0.0, 0.022, 0.040])
    parser.add_argument("--targets", type=float, nargs="+", default=[30.0])
    parser.add_argument(
        "--target-policies",
        nargs="+",
        choices=["fixed", "mid3", "mid2", "edge7_mid3", "edge8_mid2"],
        default=["fixed"],
    )
    parser.add_argument("--px-bands", nargs="+", default=["0.750:0.900", "0.800:0.900", "0.850:0.900"])
    parser.add_argument("--alignments", nargs="+", default=["high"])
    parser.add_argument("--fill-haircut", type=float, default=0.25)
    parser.add_argument("--max-seed-qty", type=float, default=60.0)
    parser.add_argument("--max-open-cost", type=float, default=250.0)
    parser.add_argument("--seed-offset-max-s", type=float, default=120.0)
    parser.add_argument("--seed-l1-pair-cap", type=float, default=1.02)
    parser.add_argument("--imbalance-qty-caps", type=float, nargs="+", default=[1_000_000_000.0])
    parser.add_argument("--imbalance-cost-cap", type=float, default=None)
    parser.add_argument("--imbalance-cost-caps", type=float, nargs="+", default=None)
    parser.add_argument("--taker-fee-rate", type=float, default=0.07)
    parser.add_argument("--salvage-net-caps", type=float, nargs="+", default=[0.0])
    parser.add_argument("--salvage-age-s", type=float, default=30.0)
    parser.add_argument("--salvage-min-lot-cost", type=float, default=0.25)
    parser.add_argument("--salvage-selects", nargs="+", choices=["fifo", "scan_best"], default=["fifo"])
    parser.add_argument("--residual-cooldown-age-s", type=float, default=0.0)
    parser.add_argument("--residual-cooldown-cost-caps", type=float, nargs="+", default=[1_000_000_000.0])
    parser.add_argument("--pair-selects", nargs="+", choices=["fifo", "low_pair_cost", "high_cost"], default=["fifo"])
    parser.add_argument("--write-residuals", action="store_true")
    args = parser.parse_args()

    args.px_bands = parse_px_bands(args.px_bands)
    if args.imbalance_cost_caps is None:
        args.imbalance_cost_caps = [
            1_000_000_000.0 if args.imbalance_cost_cap is None else args.imbalance_cost_cap
        ]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    os.makedirs(args.tmp_dir, exist_ok=True)
    configs = build_configs(args)
    stores = parse_store_arg(args.store)
    manifest = {
        "model": "D branch passive/passive dual-side BUY from public SELL flow; internal pair ledger; residual settled by winner_side/redeem; zero taker fee; public flow is not private queue truth",
        "stores": stores,
        "config_count": len(configs),
        "edges": args.edges,
        "targets": args.targets,
        "target_policies": args.target_policies,
        "px_bands": args.px_bands,
        "alignments": args.alignments,
        "fill_haircut": args.fill_haircut,
        "max_seed_qty": args.max_seed_qty,
        "max_open_cost": args.max_open_cost,
        "seed_offset_max_s": args.seed_offset_max_s,
        "seed_l1_pair_cap": args.seed_l1_pair_cap,
        "imbalance_qty_caps": args.imbalance_qty_caps,
        "imbalance_cost_caps": args.imbalance_cost_caps,
        "taker_fee_rate": args.taker_fee_rate,
        "salvage_net_caps": args.salvage_net_caps,
        "salvage_age_s": args.salvage_age_s,
        "salvage_min_lot_cost": args.salvage_min_lot_cost,
        "salvage_selects": args.salvage_selects,
        "residual_cooldown_age_s": args.residual_cooldown_age_s,
        "residual_cooldown_cost_caps": args.residual_cooldown_cost_caps,
        "pair_selects": args.pair_selects,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    per_store: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []
    for store, days in stores:
        print(json.dumps({"stage": "run_store", "store": store, "days": days, "configs": len(configs)}), flush=True)
        result = run_store(store, days, configs, args.tmp_dir, capture_residuals=args.write_residuals)
        if args.write_residuals:
            residual_rows.extend(result.pop("__residual_rows__", []))
        per_store.append(result)
        (out_dir / f"per_store_{len(per_store):02d}.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    combined = aggregate_results(configs, per_store)
    (out_dir / "combined_results.json").write_text(json.dumps(combined, indent=2), encoding="utf-8")
    with (out_dir / "combined_results.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(combined[0].keys()) if combined else [])
        if combined:
            writer.writeheader()
            writer.writerows(combined)
    if args.write_residuals:
        with (out_dir / "residual_rows.csv").open("w", newline="", encoding="utf-8") as f:
            fieldnames = list(residual_rows[0].keys()) if residual_rows else [
                "config",
                "condition_id",
                "day",
                "slug",
                "winner",
                "side",
                "qty",
                "px",
                "cost",
                "payout",
                "pnl",
                "age_s",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(residual_rows)
    print(json.dumps({"stage": "done", "out_dir": str(out_dir), "top": combined[:10]}, indent=2), flush=True)


if __name__ == "__main__":
    main()
