#!/usr/bin/env python3
"""Hybrid passive-first + FAK-second research backtest on P2 event stores.

This is an offline research tool. It reads only
completion_unwind_event_store_v2 DuckDB files and writes independent JSON/CSV
artifacts. It does not touch replay/raw data or live services.

Model intent:
- First leg: passive/improved high-side fill approximation, seeded by public
  SELL flow. This is a hypothesis about a resting bid catching cheap inventory,
  not proof of private queue truth.
- Second leg: active taker/FAK completion using executable buy VWAP from P2.
- Economics: report both gross and estimated net-after-taker-fee metrics.
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


TIERS = (10, 25, 60, 100, 250)
DUST = 1e-9


@dataclass(frozen=True)
class Config:
    name: str
    edge: float
    target_qty: float
    net_cap: float
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_completion_qty: float = 250.0
    seed_l1_pair_cap: float = 1.02
    seed_offset_min_s: float = 0.0
    seed_offset_max_s: float = 240.0
    seed_px_lo: float = 0.05
    seed_px_hi: float = 0.90
    seed_book_reasons: tuple[str, ...] = ()
    seed_min_bid_delta_qty: float = 0.0
    seed_min_bid_drop_qty: float = 0.0
    cooldown_ms: int = 5_000
    max_open_cost: float = 250.0
    skip_age_s: float = 120.0
    skip_margin: float = 0.001
    dust_qty: float = 1.0
    taker_fee_rate: float = 0.07
    same_side_open_qty_cap: float = 1_000_000_000.0
    salvage_net_cap: float = 0.0
    salvage_age_s: float = 90.0
    salvage_min_lot_cost: float = 3.0
    salvage_surplus_buffer: float = 0.0


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
    open_cost: float = 0.0
    active: bool = False

    def __post_init__(self) -> None:
        if self.inv is None:
            self.inv = {"YES": deque(), "NO": deque()}


def other(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def lot_qty(lots: deque[Lot]) -> float:
    return sum(lot.qty for lot in lots)


def fee_per_share(px: float, rate: float) -> float:
    if not math.isfinite(px):
        return 0.0
    return rate * min(max(px, 0.0), max(1.0 - px, 0.0))


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
    "side_bid_delta_qty",
    "side_bid_level_drop_qty",
    "side_ask_delta_qty",
    "side_ask_level_lift_qty",
    "book_update_reason",
]
for prefix in ("buy", "sell"):
    for tier in TIERS:
        COLS.extend(
            [
                f"{prefix}_full_{tier}",
                f"{prefix}_vwap_{tier}",
                f"{prefix}_filled_{tier}",
                f"{prefix}_worst_px_{tier}",
            ]
        )

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


def choose_vwap(row: tuple[Any, ...], qty: float, prefix: str) -> tuple[float, float, int]:
    for tier in TIERS:
        full = bool(get(row, f"{prefix}_full_{tier}"))
        filled = fnum(get(row, f"{prefix}_filled_{tier}"), 0.0)
        vwap = fnum(get(row, f"{prefix}_vwap_{tier}"))
        if full and filled + 1e-9 >= qty and not math.isnan(vwap):
            return vwap, qty, tier
    tier = TIERS[-1]
    filled = min(qty, fnum(get(row, f"{prefix}_filled_{tier}"), 0.0))
    vwap = fnum(get(row, f"{prefix}_vwap_{tier}"))
    if filled <= DUST or math.isnan(vwap):
        return math.nan, 0.0, tier
    return vwap, filled, tier


def new_metrics() -> defaultdict[str, float]:
    return defaultdict(float)


def maybe_seed(row: tuple[Any, ...], cfg: Config, state: State, m: defaultdict[str, float]) -> None:
    event_kind = get(row, "event_kind")
    ts_ms = int(get(row, "ts_ms"))
    offset_s = fnum(get(row, "offset_s"))
    side = str(get(row, "side") or "")
    alignment = get(row, "side_alignment")
    l1_pair = fnum(get(row, "l1_pair_ask"))
    taker_side = get(row, "public_trade_taker_side")
    trade_px = fnum(get(row, "public_trade_price"))
    trade_size = fnum(get(row, "public_trade_size"), 0.0)
    reason = str(get(row, "book_update_reason") or "")
    bid_delta_qty = fnum(get(row, "side_bid_delta_qty"), 0.0)
    bid_drop_qty = fnum(get(row, "side_bid_level_drop_qty"), 0.0)

    if event_kind != "public_trade" or taker_side != "SELL" or alignment != "high":
        return
    if side not in ("YES", "NO"):
        return
    if not (cfg.seed_offset_min_s <= offset_s < cfg.seed_offset_max_s):
        return
    if not (cfg.seed_px_lo <= trade_px <= cfg.seed_px_hi):
        return
    if cfg.seed_book_reasons and reason not in cfg.seed_book_reasons:
        m["seed_block_book_reason"] += 1
        return
    if bid_delta_qty + 1e-12 < cfg.seed_min_bid_delta_qty:
        m["seed_block_bid_delta"] += 1
        return
    if bid_drop_qty + 1e-12 < cfg.seed_min_bid_drop_qty:
        m["seed_block_bid_drop"] += 1
        return
    if trade_size <= 0 or math.isnan(l1_pair) or l1_pair > cfg.seed_l1_pair_cap + 1e-12:
        return
    if ts_ms - state.last_seed_ts < cfg.cooldown_ms:
        m["seed_block_cooldown"] += 1
        return

    # Completion/inventory discipline: do not add new exposure on one side while
    # the opposite side is already waiting to be paired.
    if lot_qty(state.inv[other(side)]) > cfg.dust_qty:  # type: ignore[index]
        m["seed_block_opposite_inventory"] += 1
        return

    same_qty = lot_qty(state.inv[side])  # type: ignore[index]
    if same_qty > cfg.same_side_open_qty_cap + 1e-12:
        m["seed_block_same_side_open_cap"] += 1
        return
    if same_qty >= cfg.target_qty - cfg.dust_qty:
        m["seed_block_target"] += 1
        return
    if state.open_cost >= cfg.max_open_cost:
        m["seed_block_open_cost"] += 1
        return

    px = max(0.01, trade_px - cfg.edge)
    qty = min(
        cfg.max_seed_qty,
        trade_size * cfg.fill_haircut,
        cfg.target_qty - same_qty,
        (cfg.max_open_cost - state.open_cost) / max(px, 1e-9),
    )
    if qty <= cfg.dust_qty:
        return

    state.inv[side].append(Lot(qty=qty, px=px, ts_ms=ts_ms, side=side))  # type: ignore[index]
    state.last_seed_ts = ts_ms
    state.open_cost += qty * px
    state.active = True
    m["seed_actions"] += 1
    m["gross_buy_qty"] += qty
    m["gross_buy_cost"] += qty * px
    m["seed_qty"] += qty
    m["seed_cost"] += qty * px


def select_lot_for_completion(
    lots: deque[Lot], comp_px: float, cfg: Config, ts_ms: int
) -> tuple[Lot | None, bool]:
    if not lots:
        return None, False
    first = lots[0]
    first_net = first.px + comp_px + fee_per_share(comp_px, cfg.taker_fee_rate)
    if first_net <= cfg.net_cap + 1e-12:
        return first, False
    if (ts_ms - first.ts_ms) < cfg.skip_age_s * 1000:
        return None, False
    for lot in list(lots)[1:]:
        net_cost = lot.px + comp_px + fee_per_share(comp_px, cfg.taker_fee_rate)
        if net_cost <= cfg.net_cap - cfg.skip_margin + 1e-12:
            return lot, True
    return None, False


def select_lot_for_salvage(
    lots: deque[Lot], comp_px: float, cfg: Config, ts_ms: int, fill_left: float, m: defaultdict[str, float], local_net_pnl: float
) -> Lot | None:
    if cfg.salvage_net_cap <= 0.0 or fill_left <= DUST or not lots:
        return None
    lot = lots[0]
    age_s = (ts_ms - lot.ts_ms) / 1000.0
    lot_cost = lot.qty * lot.px
    if age_s < cfg.salvage_age_s or lot_cost < cfg.salvage_min_lot_cost:
        return None
    comp_fee = fee_per_share(comp_px, cfg.taker_fee_rate)
    net_pair = lot.px + comp_px + comp_fee
    if net_pair > cfg.salvage_net_cap + 1e-12:
        return None
    take = min(lot.qty, fill_left)
    salvage_pnl = take * (1.0 - net_pair)
    available_after = m["net_pair_pnl"] + local_net_pnl + salvage_pnl - cfg.salvage_surplus_buffer
    if available_after < -1e-12:
        m["salvage_block_surplus"] += 1
        return None
    return lot


def maybe_complete(row: tuple[Any, ...], cfg: Config, state: State, m: defaultdict[str, float]) -> None:
    ts_ms = int(get(row, "ts_ms"))
    side = str(get(row, "side") or "")
    if side not in ("YES", "NO"):
        return
    held_side = other(side)
    lots = state.inv[held_side]  # type: ignore[index]
    if not lots or lot_qty(lots) <= cfg.dust_qty:
        return

    desired = min(lot_qty(lots), cfg.max_completion_qty)
    comp_px, fill_qty, tier = choose_vwap(row, desired, "buy")
    if fill_qty <= cfg.dust_qty or math.isnan(comp_px):
        m["completion_no_depth"] += 1
        return

    paired = 0.0
    gross_cost_sum = 0.0
    net_cost_sum = 0.0
    fee_sum = 0.0
    delay_sum = 0.0
    skipped = 0
    salvaged = 0.0

    while paired < fill_qty - DUST:
        selected, did_skip = select_lot_for_completion(lots, comp_px, cfg, ts_ms)
        if selected is None:
            break
        take = min(selected.qty, fill_qty - paired)
        comp_fee = fee_per_share(comp_px, cfg.taker_fee_rate)
        gross_pair = selected.px + comp_px
        net_pair = gross_pair + comp_fee
        if net_pair > cfg.net_cap + 1e-12:
            break
        paired += take
        gross_cost_sum += take * gross_pair
        net_cost_sum += take * net_pair
        fee_sum += take * comp_fee
        delay_sum += take * max(0, ts_ms - selected.ts_ms)
        selected.qty -= take
        state.open_cost = max(0.0, state.open_cost - take * selected.px)
        if did_skip:
            skipped += 1
        if selected.qty <= DUST:
            try:
                lots.remove(selected)
            except ValueError:
                pass

    while paired < fill_qty - DUST:
        local_net_pnl = paired - net_cost_sum
        selected = select_lot_for_salvage(lots, comp_px, cfg, ts_ms, fill_qty - paired, m, local_net_pnl)
        if selected is None:
            break
        take = min(selected.qty, fill_qty - paired)
        comp_fee = fee_per_share(comp_px, cfg.taker_fee_rate)
        gross_pair = selected.px + comp_px
        net_pair = gross_pair + comp_fee
        paired += take
        salvaged += take
        gross_cost_sum += take * gross_pair
        net_cost_sum += take * net_pair
        fee_sum += take * comp_fee
        delay_sum += take * max(0, ts_ms - selected.ts_ms)
        selected.qty -= take
        state.open_cost = max(0.0, state.open_cost - take * selected.px)
        if selected.qty <= DUST:
            try:
                lots.remove(selected)
            except ValueError:
                pass

    if paired <= DUST:
        m["completion_cap_block"] += 1
        return

    state.active = True
    m["completion_actions"] += 1
    m["completion_qty"] += paired
    m["gross_buy_qty"] += paired
    m["gross_buy_cost"] += paired * comp_px
    m["completion_cost"] += paired * comp_px
    m["taker_fee"] += fee_sum
    m["pair_qty"] += paired
    m["gross_pair_cost_sum"] += gross_cost_sum
    m["net_pair_cost_sum"] += net_cost_sum
    m["pair_delay_ms"] += delay_sum
    m["gross_pair_pnl"] += paired - gross_cost_sum
    m["net_pair_pnl"] += paired - net_cost_sum
    m["completion_tier_sum"] += tier
    m["skip_completions"] += skipped
    if salvaged > DUST:
        m["salvage_actions"] += 1
        m["salvage_qty"] += salvaged


def settle(
    states: dict[str, State],
    cfg: Config,
    m: defaultdict[str, float],
    residual_rows: list[dict[str, Any]] | None = None,
) -> None:
    for condition_id, state in states.items():
        if state.active:
            m["active_markets"] += 1
        residual_qty = 0.0
        residual_cost = 0.0
        residual_pnl = 0.0
        for side, lots in state.inv.items():  # type: ignore[union-attr]
            for lot in lots:
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                pnl = lot.qty * ((1.0 if state.winner == side else 0.0) - lot.px)
                residual_qty += lot.qty
                residual_cost += cost
                residual_pnl += pnl
                if residual_rows is not None:
                    residual_rows.append(
                        {
                            "config": cfg.name,
                            "day": state.day,
                            "condition_id": condition_id,
                            "slug": state.slug,
                            "side": side,
                            "winner_side": state.winner,
                            "qty": round(lot.qty, 6),
                            "px": round(lot.px, 6),
                            "cost": round(cost, 6),
                            "settlement_pnl": round(pnl, 6),
                            "age_s": round(max(0, state.last_ts_ms - lot.ts_ms) / 1000.0, 3),
                            "lot_ts_ms": lot.ts_ms,
                        }
                    )
        if residual_qty > DUST:
            m["residual_qty"] += residual_qty
            m["residual_cost"] += residual_cost
            m["residual_settlement_pnl"] += residual_pnl
            if residual_qty > 6:
                m["resid_gt6_markets"] += 1
            if residual_cost > 6:
                m["resid_cost_gt6_markets"] += 1


def finish(cfg: Config, m: defaultdict[str, float]) -> dict[str, Any]:
    buy_cost = m["gross_buy_cost"]
    pair_qty = m["pair_qty"]
    gross_pnl = m["gross_pair_pnl"] + m["residual_settlement_pnl"]
    net_pnl = m["net_pair_pnl"] + m["residual_settlement_pnl"]
    out: dict[str, Any] = {
        "name": cfg.name,
        "edge": cfg.edge,
        "target_qty": cfg.target_qty,
        "net_cap": cfg.net_cap,
        "salvage_net_cap": cfg.salvage_net_cap,
        "salvage_age_s": cfg.salvage_age_s,
        "salvage_min_lot_cost": cfg.salvage_min_lot_cost,
        "salvage_surplus_buffer": cfg.salvage_surplus_buffer,
        "seed_book_reasons": list(cfg.seed_book_reasons),
        "seed_min_bid_delta_qty": cfg.seed_min_bid_delta_qty,
        "seed_min_bid_drop_qty": cfg.seed_min_bid_drop_qty,
        "fill_haircut": cfg.fill_haircut,
        "taker_fee_rate": cfg.taker_fee_rate,
        "active_markets": int(m["active_markets"]),
        "seed_actions": int(m["seed_actions"]),
        "completion_actions": int(m["completion_actions"]),
        "gross_buy_qty": round(m["gross_buy_qty"], 6),
        "gross_buy_cost": round(buy_cost, 6),
        "seed_cost": round(m["seed_cost"], 6),
        "completion_cost": round(m["completion_cost"], 6),
        "taker_fee": round(m["taker_fee"], 6),
        "pair_qty": round(pair_qty, 6),
        "pair_share_rate": round((2 * pair_qty / m["gross_buy_qty"]) if m["gross_buy_qty"] else 0.0, 6),
        "gross_pair_cost_wavg": round((m["gross_pair_cost_sum"] / pair_qty) if pair_qty else 0.0, 6),
        "net_pair_cost_wavg": round((m["net_pair_cost_sum"] / pair_qty) if pair_qty else 0.0, 6),
        "pair_delay_wavg_s": round((m["pair_delay_ms"] / pair_qty / 1000.0) if pair_qty else 0.0, 6),
        "rounds_per_market": round((m["completion_actions"] / m["active_markets"]) if m["active_markets"] else 0.0, 6),
        "residual_qty": round(m["residual_qty"], 6),
        "residual_cost": round(m["residual_cost"], 6),
        "qty_residual_rate": round((m["residual_qty"] / m["gross_buy_qty"]) if m["gross_buy_qty"] else 0.0, 6),
        "cost_residual_rate": round((m["residual_cost"] / buy_cost) if buy_cost else 0.0, 6),
        "residual_cost_gt6_market_rate": round(
            (m["resid_cost_gt6_markets"] / m["active_markets"]) if m["active_markets"] else 0.0, 6
        ),
        "gross_pnl": round(gross_pnl, 6),
        "net_pnl": round(net_pnl, 6),
        "gross_roi": round((gross_pnl / buy_cost) if buy_cost else 0.0, 6),
        "net_roi": round((net_pnl / buy_cost) if buy_cost else 0.0, 6),
        "stress100_net_pnl": round(net_pnl - 0.01 * (2 * pair_qty + m["residual_qty"]), 6),
        "worst_residual_net_pnl": round(m["net_pair_pnl"] - m["residual_cost"], 6),
        "skip_completions": int(m["skip_completions"]),
        "salvage_actions": int(m["salvage_actions"]),
        "salvage_qty": round(m["salvage_qty"], 6),
        "salvage_block_surplus": int(m["salvage_block_surplus"]),
        "seed_block_opposite_inventory": int(m["seed_block_opposite_inventory"]),
        "seed_block_target": int(m["seed_block_target"]),
        "seed_block_same_side_open_cap": int(m["seed_block_same_side_open_cap"]),
        "seed_block_book_reason": int(m["seed_block_book_reason"]),
        "seed_block_bid_delta": int(m["seed_block_bid_delta"]),
        "seed_block_bid_drop": int(m["seed_block_bid_drop"]),
        "completion_cap_block": int(m["completion_cap_block"]),
    }
    return out


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
            print(json.dumps({"store": store, "batches": batches, "elapsed_s": round(time.time() - start, 1)}), flush=True)
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
                m = metrics[cfg.name]
                maybe_complete(row, cfg, state, m)
                maybe_seed(row, cfg, state, m)

    results: dict[str, Any] = {}
    residual_rows: list[dict[str, Any]] = []
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
    combined: list[dict[str, Any]] = []
    numeric_keys = [
        "active_markets",
        "seed_actions",
        "completion_actions",
        "gross_buy_qty",
        "gross_buy_cost",
        "seed_cost",
        "completion_cost",
        "taker_fee",
        "pair_qty",
        "residual_qty",
        "residual_cost",
        "gross_pnl",
        "net_pnl",
        "skip_completions",
        "seed_block_opposite_inventory",
        "seed_block_target",
        "seed_block_same_side_open_cap",
        "completion_cap_block",
        "salvage_actions",
        "salvage_qty",
        "salvage_block_surplus",
        "seed_block_book_reason",
        "seed_block_bid_delta",
        "seed_block_bid_drop",
    ]
    weighted_pair_keys = ["gross_pair_cost_wavg", "net_pair_cost_wavg", "pair_delay_wavg_s"]
    for cfg in configs:
        rows = [result[cfg.name] for result in per_store if cfg.name in result]
        agg: dict[str, Any] = {
            "name": cfg.name,
            "edge": cfg.edge,
            "target_qty": cfg.target_qty,
            "net_cap": cfg.net_cap,
            "salvage_net_cap": cfg.salvage_net_cap,
            "salvage_age_s": cfg.salvage_age_s,
            "salvage_min_lot_cost": cfg.salvage_min_lot_cost,
            "salvage_surplus_buffer": cfg.salvage_surplus_buffer,
            "seed_book_reasons": list(cfg.seed_book_reasons),
            "seed_min_bid_delta_qty": cfg.seed_min_bid_delta_qty,
            "seed_min_bid_drop_qty": cfg.seed_min_bid_drop_qty,
            "fill_haircut": cfg.fill_haircut,
            "taker_fee_rate": cfg.taker_fee_rate,
        }
        for key in numeric_keys:
            agg[key] = round(sum(float(row.get(key, 0) or 0) for row in rows), 6)
        pair_qty = float(agg["pair_qty"])
        buy_qty = float(agg["gross_buy_qty"])
        buy_cost = float(agg["gross_buy_cost"])
        active = float(agg["active_markets"])
        for key in weighted_pair_keys:
            agg[key] = round(
                (sum(float(row.get(key, 0) or 0) * float(row.get("pair_qty", 0) or 0) for row in rows) / pair_qty)
                if pair_qty
                else 0.0,
                6,
            )
        agg["pair_share_rate"] = round((2 * pair_qty / buy_qty) if buy_qty else 0.0, 6)
        agg["rounds_per_market"] = round((float(agg["completion_actions"]) / active) if active else 0.0, 6)
        agg["qty_residual_rate"] = round((float(agg["residual_qty"]) / buy_qty) if buy_qty else 0.0, 6)
        agg["cost_residual_rate"] = round((float(agg["residual_cost"]) / buy_cost) if buy_cost else 0.0, 6)
        agg["gross_roi"] = round((float(agg["gross_pnl"]) / buy_cost) if buy_cost else 0.0, 6)
        agg["net_roi"] = round((float(agg["net_pnl"]) / buy_cost) if buy_cost else 0.0, 6)
        agg["stress100_net_pnl"] = round(
            float(agg["net_pnl"]) - 0.01 * (2 * pair_qty + float(agg["residual_qty"])), 6
        )
        agg["worst_residual_net_pnl"] = round(float(agg["net_pnl"]) - float(agg["residual_cost"]), 6)
        # Cannot exactly aggregate market-rate tails without per-store tail counts in
        # this compact path, so leave the direct residual rates as primary.
        combined.append(agg)
    combined.sort(key=lambda row: (row["net_pnl"], row["stress100_net_pnl"]), reverse=True)
    return combined


def build_configs(args: argparse.Namespace) -> list[Config]:
    configs: list[Config] = []
    for edge in args.edges:
        for target in args.targets:
            for cap in args.net_caps:
                for salvage_cap in args.salvage_net_caps:
                    suffix = "" if salvage_cap <= 0 else f"_sv{int(round(salvage_cap*1000)):04d}"
                    px_suffix = ""
                    if abs(args.seed_px_lo - 0.05) > DUST or abs(args.seed_px_hi - 0.90) > DUST:
                        px_suffix = f"_px{int(round(args.seed_px_lo*1000)):03d}_{int(round(args.seed_px_hi*1000)):03d}"
                    reason_suffix = ""
                    if args.seed_book_reasons:
                        reason_suffix = "_br" + "".join(item[:2].replace("_", "") for item in args.seed_book_reasons)
                    delta_suffix = ""
                    if args.seed_min_bid_delta_qty > 0:
                        delta_suffix += f"_bd{int(round(args.seed_min_bid_delta_qty))}"
                    if args.seed_min_bid_drop_qty > 0:
                        delta_suffix += f"_bp{int(round(args.seed_min_bid_drop_qty))}"
                    configs.append(
                        Config(
                            name=(
                                f"hybrid_e{int(round(edge*1000)):03d}_t{int(target)}_net{int(round(cap*1000)):04d}"
                                f"{suffix}{px_suffix}{reason_suffix}{delta_suffix}"
                            ),
                            edge=edge,
                            target_qty=target,
                            net_cap=cap,
                            fill_haircut=args.fill_haircut,
                            taker_fee_rate=args.taker_fee_rate,
                            seed_l1_pair_cap=args.seed_l1_pair_cap,
                            seed_offset_max_s=args.seed_offset_max_s,
                            seed_px_lo=args.seed_px_lo,
                            seed_px_hi=args.seed_px_hi,
                            seed_book_reasons=tuple(args.seed_book_reasons or ()),
                            seed_min_bid_delta_qty=args.seed_min_bid_delta_qty,
                            seed_min_bid_drop_qty=args.seed_min_bid_drop_qty,
                            same_side_open_qty_cap=args.same_side_open_qty_cap,
                            salvage_net_cap=salvage_cap,
                            salvage_age_s=args.salvage_age_s,
                            salvage_min_lot_cost=args.salvage_min_lot_cost,
                            salvage_surplus_buffer=args.salvage_surplus_buffer,
                        )
                    )
    return configs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", action="append", required=True, help="STORE:day[,day...]")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--tmp-dir", default="/home/ubuntu/duckdb_tmp_xuan_hybrid_fak")
    parser.add_argument("--edges", type=float, nargs="+", default=[0.010, 0.015, 0.020, 0.022])
    parser.add_argument("--targets", type=float, nargs="+", default=[30.0, 60.0, 115.0])
    parser.add_argument("--net-caps", type=float, nargs="+", default=[0.970, 0.980, 0.990, 1.000])
    parser.add_argument("--fill-haircut", type=float, default=0.25)
    parser.add_argument("--taker-fee-rate", type=float, default=0.07)
    parser.add_argument("--seed-l1-pair-cap", type=float, default=1.02)
    parser.add_argument("--seed-offset-max-s", type=float, default=240.0)
    parser.add_argument("--seed-px-lo", type=float, default=0.05)
    parser.add_argument("--seed-px-hi", type=float, default=0.90)
    parser.add_argument("--seed-book-reasons", nargs="+", default=[])
    parser.add_argument("--seed-min-bid-delta-qty", type=float, default=0.0)
    parser.add_argument("--seed-min-bid-drop-qty", type=float, default=0.0)
    parser.add_argument("--same-side-open-qty-cap", type=float, default=1_000_000_000.0)
    parser.add_argument("--salvage-net-caps", type=float, nargs="+", default=[0.0])
    parser.add_argument("--salvage-age-s", type=float, default=90.0)
    parser.add_argument("--salvage-min-lot-cost", type=float, default=3.0)
    parser.add_argument("--salvage-surplus-buffer", type=float, default=0.0)
    parser.add_argument("--write-residuals", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    os.makedirs(args.tmp_dir, exist_ok=True)
    configs = build_configs(args)
    stores = parse_store_arg(args.store)
    manifest = {
        "model": "passive/improved first leg from public SELL high-side seed; active FAK second leg from P2 buy VWAP; net cap includes taker fee on completion leg",
        "stores": stores,
        "config_count": len(configs),
        "edges": args.edges,
        "targets": args.targets,
        "net_caps": args.net_caps,
        "fill_haircut": args.fill_haircut,
        "taker_fee_rate": args.taker_fee_rate,
        "seed_l1_pair_cap": args.seed_l1_pair_cap,
        "seed_offset_max_s": args.seed_offset_max_s,
        "seed_px_lo": args.seed_px_lo,
        "seed_px_hi": args.seed_px_hi,
        "seed_book_reasons": args.seed_book_reasons,
        "seed_min_bid_delta_qty": args.seed_min_bid_delta_qty,
        "seed_min_bid_drop_qty": args.seed_min_bid_drop_qty,
        "same_side_open_qty_cap": args.same_side_open_qty_cap,
        "salvage_net_caps": args.salvage_net_caps,
        "salvage_age_s": args.salvage_age_s,
        "salvage_min_lot_cost": args.salvage_min_lot_cost,
        "salvage_surplus_buffer": args.salvage_surplus_buffer,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    per_store: list[dict[str, Any]] = []
    start = time.time()
    for store, days in stores:
        result = run_store(store, days, configs, args.tmp_dir, args.write_residuals)
        per_store.append(result)
        name = Path(store).parent.name
        (out_dir / f"result_{name}.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    residual_rows: list[dict[str, Any]] = []
    if args.write_residuals:
        for result in per_store:
            residual_rows.extend(result.get("__residual_rows__", []))
        residual_rows.sort(key=lambda row: (row.get("cost") or 0.0), reverse=True)
        with (out_dir / "residual_lots.csv").open("w", newline="", encoding="utf-8") as f:
            fieldnames = [
                "config",
                "day",
                "condition_id",
                "slug",
                "side",
                "winner_side",
                "qty",
                "px",
                "cost",
                "settlement_pnl",
                "age_s",
                "lot_ts_ms",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(residual_rows)
        by_market: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
        for row in residual_rows:
            key = (row["config"], row["day"], row["condition_id"])
            item = by_market.setdefault(
                key,
                {
                    "config": row["config"],
                    "day": row["day"],
                    "condition_id": row["condition_id"],
                    "slug": row["slug"],
                    "cost": 0.0,
                    "qty": 0.0,
                    "settlement_pnl": 0.0,
                    "lots": 0,
                },
            )
            item["cost"] += float(row["cost"])
            item["qty"] += float(row["qty"])
            item["settlement_pnl"] += float(row["settlement_pnl"])
            item["lots"] += 1
        top_markets = sorted(by_market.values(), key=lambda row: row["cost"], reverse=True)[:100]
        for row in top_markets:
            row["cost"] = round(row["cost"], 6)
            row["qty"] = round(row["qty"], 6)
            row["settlement_pnl"] = round(row["settlement_pnl"], 6)
        (out_dir / "top_residual_markets.json").write_text(json.dumps(top_markets, indent=2), encoding="utf-8")
    combined = aggregate_results(configs, per_store)
    (out_dir / "combined_results.json").write_text(json.dumps(combined, indent=2), encoding="utf-8")
    with (out_dir / "combined_results.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(combined[0].keys()) if combined else [])
        writer.writeheader()
        writer.writerows(combined)
    summary = {
        "elapsed_s": round(time.time() - start, 3),
        "out_dir": str(out_dir),
        "top_by_net_pnl": combined[:12],
        "top_positive_net": [row for row in combined if row["net_pnl"] > 0][:12],
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
