#!/usr/bin/env python3
"""Shadow-realistic D+ verifier on completion_unwind_event_store_v2.

This research tool intentionally differs from the older instant-lot D+ verifier:
public SELL flow first creates a passive BUY order, then only a later same-side
SELL print at/through that order price can fill it. The goal is to test whether
the D+/B27 edge survives live/shadow order semantics before any Rust mapping work.
"""

from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import json
import math
import socket
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


DUST = 1e-9
TIERS = (10, 25, 60, 100, 250)
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
]
for tier in TIERS:
    COLS.extend([f"buy_full_{tier}", f"buy_vwap_{tier}", f"buy_filled_{tier}"])
IDX = {name: idx for idx, name in enumerate(COLS)}


@dataclass(frozen=True)
class Config:
    name: str
    edge: float
    target_qty: float
    seed_px_lo: float
    seed_px_hi: float
    order_delay_ms: int
    order_ttl_ms: int
    fill_haircut: float
    trade_fill_fraction: float
    imbalance_qty_cap: float
    salvage_net_cap: float
    salvage_age_s: float = 30.0
    salvage_min_lot_cost: float = 0.25
    seed_l1_pair_cap: float = 1.02
    max_seed_qty: float = 60.0
    max_open_cost: float = 160.0
    seed_offset_max_s: float = 120.0
    cooldown_ms: int = 5_000
    taker_fee_rate: float = 0.07
    dust_qty: float = 1.0


@dataclass
class Order:
    side: str
    qty: float
    px: float
    created_ts: int
    active_ts: int
    expire_ts: int


@dataclass
class Lot:
    side: str
    qty: float
    px: float
    ts_ms: int


@dataclass
class State:
    winner: str | None = None
    day: str = ""
    slug: str = ""
    last_ts_ms: int = 0
    last_seed_ts: int = -(10**18)
    active: bool = False
    inv: dict[str, deque[Lot]] | None = None
    orders: dict[str, Order | None] | None = None

    def __post_init__(self) -> None:
        if self.inv is None:
            self.inv = {"YES": deque(), "NO": deque()}
        if self.orders is None:
            self.orders = {"YES": None, "NO": None}


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


def q2(value: float) -> float:
    return math.floor(value * 100.0 + 1e-9) / 100.0


def qtenth(value: float) -> float:
    return math.floor(value * 10.0 + 1e-9) / 10.0


def other(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def lot_qty(lots: deque[Lot]) -> float:
    return sum(lot.qty for lot in lots)


def lot_cost(lots: deque[Lot]) -> float:
    return sum(lot.qty * lot.px for lot in lots)


def pending_qty(state: State, side: str) -> float:
    order = state.orders[side]  # type: ignore[index]
    return order.qty if order is not None else 0.0


def pending_cost(state: State) -> float:
    return sum((order.qty * order.px) for order in state.orders.values() if order is not None)  # type: ignore[union-attr]


def fee_per_share(px: float, rate: float) -> float:
    return rate * min(max(px, 0.0), max(1.0 - px, 0.0))


def choose_buy_vwap(row: tuple[Any, ...], qty: float) -> tuple[float, float]:
    for tier in TIERS:
        full = bool(get(row, f"buy_full_{tier}"))
        filled = fnum(get(row, f"buy_filled_{tier}"), 0.0)
        vwap = fnum(get(row, f"buy_vwap_{tier}"))
        if full and filled + 1e-9 >= qty and not math.isnan(vwap):
            return vwap, qty
    filled = min(qty, fnum(get(row, "buy_filled_250"), 0.0))
    vwap = fnum(get(row, "buy_vwap_250"))
    if filled <= DUST or math.isnan(vwap):
        return math.nan, 0.0
    return vwap, filled


def new_metrics() -> defaultdict[str, float]:
    return defaultdict(float)


def expire_orders(state: State, cfg: Config, ts_ms: int, m: defaultdict[str, float]) -> None:
    for side in ("YES", "NO"):
        order = state.orders[side]  # type: ignore[index]
        if order is not None and ts_ms > order.expire_ts:
            state.orders[side] = None  # type: ignore[index]
            m["order_expire"] += 1


def pair_inventory(state: State, m: defaultdict[str, float], ts_ms: int) -> None:
    yes = state.inv["YES"]  # type: ignore[index]
    no = state.inv["NO"]  # type: ignore[index]
    while yes and no:
        a = yes[0]
        b = no[0]
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
            yes.popleft()
        if b.qty <= DUST:
            no.popleft()


def maybe_fill_order(row: tuple[Any, ...], cfg: Config, state: State, m: defaultdict[str, float]) -> None:
    if get(row, "event_kind") != "public_trade" or get(row, "public_trade_taker_side") != "SELL":
        return
    side = str(get(row, "side") or "")
    if side not in ("YES", "NO"):
        return
    order = state.orders[side]  # type: ignore[index]
    if order is None:
        return
    ts_ms = int(get(row, "ts_ms") or 0)
    if ts_ms < order.active_ts:
        m["fill_block_not_active"] += 1
        return
    trade_px = fnum(get(row, "public_trade_price"))
    trade_size = fnum(get(row, "public_trade_size"), 0.0)
    if math.isnan(trade_px) or trade_size <= DUST:
        return
    if trade_px > order.px + 1e-9:
        m["fill_miss_price"] += 1
        return
    fill_qty = min(order.qty, trade_size * cfg.trade_fill_fraction)
    fill_qty = qtenth(fill_qty)
    if fill_qty <= DUST:
        return
    state.inv[side].append(Lot(side=side, qty=fill_qty, px=order.px, ts_ms=ts_ms))  # type: ignore[index]
    order.qty -= fill_qty
    state.active = True
    m["fill_events"] += 1
    m["gross_buy_qty"] += fill_qty
    m["gross_buy_cost"] += fill_qty * order.px
    m["seed_fill_qty"] += fill_qty
    m["seed_fill_cost"] += fill_qty * order.px
    if order.qty <= DUST:
        state.orders[side] = None  # type: ignore[index]
        m["order_full"] += 1
    pair_inventory(state, m, ts_ms)


def maybe_salvage(row: tuple[Any, ...], cfg: Config, state: State, m: defaultdict[str, float]) -> None:
    if cfg.salvage_net_cap <= 0:
        return
    side = str(get(row, "side") or "")
    if side not in ("YES", "NO"):
        return
    held_side = other(side)
    lots = state.inv[held_side]  # type: ignore[index]
    if not lots:
        return
    ts_ms = int(get(row, "ts_ms") or 0)
    desired = lot_qty(lots)
    comp_px, fill_qty = choose_buy_vwap(row, desired)
    if fill_qty <= DUST or math.isnan(comp_px):
        return
    paired = 0.0
    while lots and paired < fill_qty - DUST:
        lot = lots[0]
        age_s = (ts_ms - lot.ts_ms) / 1000.0
        lot_cost_value = lot.qty * lot.px
        if age_s < cfg.salvage_age_s or lot_cost_value < cfg.salvage_min_lot_cost:
            break
        comp_fee = fee_per_share(comp_px, cfg.taker_fee_rate)
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
            lots.popleft()
        else:
            break


def maybe_place_order(row: tuple[Any, ...], cfg: Config, state: State, m: defaultdict[str, float]) -> None:
    if get(row, "event_kind") != "public_trade" or get(row, "public_trade_taker_side") != "SELL":
        return
    side = str(get(row, "side") or "")
    if side not in ("YES", "NO"):
        return
    offset_s = fnum(get(row, "offset_s"))
    if not (0.0 <= offset_s < cfg.seed_offset_max_s):
        m["seed_block_offset"] += 1
        return
    trade_px = fnum(get(row, "public_trade_price"))
    trade_size = fnum(get(row, "public_trade_size"), 0.0)
    if trade_size <= DUST or math.isnan(trade_px):
        return
    if not (cfg.seed_px_lo <= trade_px <= cfg.seed_px_hi):
        m["seed_block_price_band"] += 1
        return
    l1_pair = fnum(get(row, "l1_pair_ask"))
    if math.isnan(l1_pair) or l1_pair > cfg.seed_l1_pair_cap + 1e-12:
        m["seed_block_l1_pair_cap"] += 1
        return
    ts_ms = int(get(row, "ts_ms") or 0)
    if ts_ms - state.last_seed_ts < cfg.cooldown_ms:
        m["seed_block_cooldown"] += 1
        return

    px = q2(max(0.01, trade_px - cfg.edge))
    same_qty = lot_qty(state.inv[side]) + pending_qty(state, side)  # type: ignore[index]
    opp_qty = lot_qty(state.inv[other(side)]) + pending_qty(state, other(side))  # type: ignore[index]
    if same_qty >= cfg.target_qty - cfg.dust_qty:
        m["seed_block_target"] += 1
        return
    imbalance_room = cfg.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
    if imbalance_room <= cfg.dust_qty:
        m["seed_block_imbalance_qty"] += 1
        return
    open_cost = lot_cost(state.inv["YES"]) + lot_cost(state.inv["NO"]) + pending_cost(state)  # type: ignore[index]
    qty = min(
        cfg.max_seed_qty,
        trade_size * cfg.fill_haircut,
        cfg.target_qty - same_qty,
        (cfg.max_open_cost - open_cost) / max(px, 1e-9),
        imbalance_room,
    )
    qty = qtenth(qty)
    if qty < cfg.dust_qty:
        m["seed_block_small"] += 1
        return
    if state.orders[side] is not None:  # type: ignore[index]
        m["order_replace"] += 1
    state.orders[side] = Order(  # type: ignore[index]
        side=side,
        qty=qty,
        px=px,
        created_ts=ts_ms,
        active_ts=ts_ms + cfg.order_delay_ms,
        expire_ts=ts_ms + cfg.order_delay_ms + cfg.order_ttl_ms,
    )
    state.last_seed_ts = ts_ms
    state.active = True
    m["order_place"] += 1
    m["order_qty"] += qty


def settle(states: dict[str, State], m: defaultdict[str, float]) -> None:
    for state in states.values():
        if not state.active:
            continue
        pair_inventory(state, m, state.last_ts_ms)
        m["active_markets"] += 1
        for side in ("YES", "NO"):
            for lot in state.inv[side]:  # type: ignore[index]
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                payout = lot.qty if state.winner == side else 0.0
                m["residual_qty"] += lot.qty
                m["residual_cost"] += cost
                m["residual_settle_pnl"] += payout - cost


def finish(cfg: Config, m: defaultdict[str, float]) -> dict[str, Any]:
    buy_cost = m["gross_buy_cost"]
    buy_qty = m["gross_buy_qty"]
    pair_qty = m["pair_qty"]
    active = m["active_markets"]
    actual = m["pair_pnl"] + m["residual_settle_pnl"]
    worst = m["pair_pnl"] - m["residual_cost"]
    row = {
        "name": cfg.name,
        "edge": cfg.edge,
        "target_qty": cfg.target_qty,
        "order_delay_ms": cfg.order_delay_ms,
        "order_ttl_ms": cfg.order_ttl_ms,
        "fill_haircut": cfg.fill_haircut,
        "trade_fill_fraction": cfg.trade_fill_fraction,
        "imbalance_qty_cap": cfg.imbalance_qty_cap,
        "salvage_net_cap": cfg.salvage_net_cap,
        "active_markets": int(active),
        "order_place": int(m["order_place"]),
        "order_replace": int(m["order_replace"]),
        "order_expire": int(m["order_expire"]),
        "fill_events": int(m["fill_events"]),
        "pair_actions": int(m["pair_actions"]),
        "salvage_actions": int(m["salvage_actions"]),
        "gross_buy_qty": round(buy_qty, 6),
        "gross_buy_cost": round(buy_cost, 6),
        "pair_qty": round(pair_qty, 6),
        "pair_share_rate": round((2 * pair_qty / buy_qty) if buy_qty else 0.0, 6),
        "net_pair_cost_wavg": round((m["net_pair_cost_sum"] / pair_qty) if pair_qty else 0.0, 6),
        "pair_delay_wavg_s": round((m["pair_delay_ms"] / pair_qty / 1000.0) if pair_qty else 0.0, 6),
        "cycles_per_market": round((m["pair_actions"] / active) if active else 0.0, 6),
        "residual_qty": round(m["residual_qty"], 6),
        "residual_cost": round(m["residual_cost"], 6),
        "qty_residual_rate": round((m["residual_qty"] / buy_qty) if buy_qty else 0.0, 6),
        "actual_pnl": round(actual, 6),
        "worst_case_pnl": round(worst, 6),
        "roi": round((actual / buy_cost) if buy_cost else 0.0, 6),
        "stress100_actual_pnl": round(actual - 0.01 * (2 * pair_qty + m["residual_qty"]), 6),
        "stress100_worst_pnl": round(worst - 0.01 * (2 * pair_qty + m["residual_qty"]), 6),
        "fill_miss_price": int(m["fill_miss_price"]),
        "fill_block_not_active": int(m["fill_block_not_active"]),
        "seed_block_cooldown": int(m["seed_block_cooldown"]),
        "seed_block_target": int(m["seed_block_target"]),
        "seed_block_imbalance_qty": int(m["seed_block_imbalance_qty"]),
    }
    return row


def query_rows(con: duckdb.DuckDBPyConnection, days: list[str]) -> Any:
    placeholders = ",".join("?" for _ in days)
    select_cols = ",".join(COLS)
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


def run_store(store: str, days: list[str], configs: list[Config], tmp_dir: str) -> list[dict[str, Any]]:
    con = duckdb.connect(store, read_only=True)
    con.execute(f"PRAGMA temp_directory='{tmp_dir}'")
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
            cid = str(get(row, "condition_id") or "")
            if not cid:
                continue
            winner = get(row, "winner_side")
            ts_ms = int(get(row, "ts_ms") or 0)
            for cfg in configs:
                by_market = states[cfg.name]
                state = by_market.get(cid)
                if state is None:
                    state = State(
                        winner=str(winner) if winner else None,
                        day=str(get(row, "day") or ""),
                        slug=str(get(row, "slug") or ""),
                    )
                    by_market[cid] = state
                elif state.winner is None and winner:
                    state.winner = str(winner)
                state.last_ts_ms = max(state.last_ts_ms, ts_ms)
                expire_orders(state, cfg, ts_ms, metrics[cfg.name])
                maybe_fill_order(row, cfg, state, metrics[cfg.name])
                maybe_salvage(row, cfg, state, metrics[cfg.name])
                maybe_place_order(row, cfg, state, metrics[cfg.name])
    out = []
    for cfg in configs:
        settle(states[cfg.name], metrics[cfg.name])
        row = finish(cfg, metrics[cfg.name])
        row["store"] = store
        row["days"] = ",".join(days)
        out.append(row)
    con.close()
    del states
    gc.collect()
    return out


def parse_store(values: list[str]) -> list[tuple[str, list[str]]]:
    out = []
    for value in values:
        store, _, days_s = value.partition(":")
        days = [item for item in days_s.split(",") if item]
        if not store or not days:
            raise SystemExit(f"--store must be STORE:day[,day], got {value}")
        out.append((store, days))
    return out


def build_configs(args: argparse.Namespace) -> list[Config]:
    configs = []
    for edge in args.edges:
        for target in args.targets:
            for delay in args.order_delay_ms:
                for ttl in args.order_ttl_ms:
                    for fh in args.fill_haircuts:
                        for frac in args.trade_fill_fractions:
                            for imb in args.imbalance_qty_caps:
                                for salvage in args.salvage_net_caps:
                                    name = (
                                        f"shadow_e{int(round(edge*1000)):03d}_t{int(target)}"
                                        f"_d{delay}_ttl{ttl}_fh{int(round(fh*100)):03d}"
                                        f"_tf{int(round(frac*100)):03d}_imb{int(imb)}"
                                        f"_sv{int(round(salvage*1000)):04d}"
                                    )
                                    configs.append(
                                        Config(
                                            name=name,
                                            edge=float(edge),
                                            target_qty=float(target),
                                            seed_px_lo=0.010,
                                            seed_px_hi=0.990,
                                            order_delay_ms=int(delay),
                                            order_ttl_ms=int(ttl),
                                            fill_haircut=float(fh),
                                            trade_fill_fraction=float(frac),
                                            imbalance_qty_cap=float(imb),
                                            salvage_net_cap=float(salvage),
                                        )
                                    )
    return configs


def aggregate(configs: list[Config], per_store_rows: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rows in per_store_rows:
        for row in rows:
            by_name[row["name"]].append(row)
    out = []
    for cfg in configs:
        rows = by_name[cfg.name]
        sums: defaultdict[str, float] = defaultdict(float)
        for row in rows:
            for key, value in row.items():
                if isinstance(value, (int, float)):
                    sums[key] += float(value)
        pair_qty = sums["pair_qty"]
        buy_qty = sums["gross_buy_qty"]
        buy_cost = sums["gross_buy_cost"]
        active = sums["active_markets"]
        actual = sums["actual_pnl"]
        worst = sums["worst_case_pnl"]
        agg = {
            "name": cfg.name,
            "order_delay_ms": cfg.order_delay_ms,
            "order_ttl_ms": cfg.order_ttl_ms,
            "fill_haircut": cfg.fill_haircut,
            "trade_fill_fraction": cfg.trade_fill_fraction,
            "imbalance_qty_cap": cfg.imbalance_qty_cap,
            "salvage_net_cap": cfg.salvage_net_cap,
            "active_markets": int(active),
            "order_place": int(sums["order_place"]),
            "fill_events": int(sums["fill_events"]),
            "pair_actions": int(sums["pair_actions"]),
            "salvage_actions": int(sums["salvage_actions"]),
            "gross_buy_qty": round(buy_qty, 6),
            "gross_buy_cost": round(buy_cost, 6),
            "pair_qty": round(pair_qty, 6),
            "pair_share_rate": round((2 * pair_qty / buy_qty) if buy_qty else 0.0, 6),
            "net_pair_cost_wavg": round((sum(float(row["net_pair_cost_wavg"]) * float(row["pair_qty"]) for row in rows) / pair_qty) if pair_qty else 0.0, 6),
            "pair_delay_wavg_s": round((sum(float(row["pair_delay_wavg_s"]) * float(row["pair_qty"]) for row in rows) / pair_qty) if pair_qty else 0.0, 6),
            "cycles_per_market": round((sums["pair_actions"] / active) if active else 0.0, 6),
            "residual_qty": round(sums["residual_qty"], 6),
            "residual_cost": round(sums["residual_cost"], 6),
            "qty_residual_rate": round((sums["residual_qty"] / buy_qty) if buy_qty else 0.0, 6),
            "actual_pnl": round(actual, 6),
            "worst_case_pnl": round(worst, 6),
            "roi": round((actual / buy_cost) if buy_cost else 0.0, 6),
            "stress100_actual_pnl": round(actual - 0.01 * (2 * pair_qty + sums["residual_qty"]), 6),
            "stress100_worst_pnl": round(worst - 0.01 * (2 * pair_qty + sums["residual_qty"]), 6),
        }
        out.append(agg)
    out.sort(key=lambda row: (row["stress100_worst_pnl"], row["actual_pnl"]), reverse=True)
    return out


def verdict(row: dict[str, Any]) -> str:
    if (
        float(row.get("stress100_worst_pnl", 0)) > 0
        and float(row.get("actual_pnl", 0)) > 0
        and float(row.get("net_pair_cost_wavg", 9)) < 0.94
        and float(row.get("cycles_per_market", 0)) > 8
        and float(row.get("qty_residual_rate", 9)) <= 0.04
    ):
        return "KEEP"
    if float(row.get("stress100_worst_pnl", 0)) <= 0 or float(row.get("cycles_per_market", 0)) < 4:
        return "DISCARD"
    return "UNKNOWN"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", action="append", required=True, help="STORE:day[,day]")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--tmp-dir", default="/tmp/duckdb_dplus_shadow_realistic")
    parser.add_argument("--edges", type=float, nargs="+", default=[0.04])
    parser.add_argument("--targets", type=float, nargs="+", default=[10.0])
    parser.add_argument("--order-delay-ms", type=int, nargs="+", default=[0, 500, 1000])
    parser.add_argument("--order-ttl-ms", type=int, nargs="+", default=[1500, 3000])
    parser.add_argument("--fill-haircuts", type=float, nargs="+", default=[0.15, 0.20, 0.25])
    parser.add_argument("--trade-fill-fractions", type=float, nargs="+", default=[0.25])
    parser.add_argument("--imbalance-qty-caps", type=float, nargs="+", default=[6.0, 8.0])
    parser.add_argument("--salvage-net-caps", type=float, nargs="+", default=[0.95, 0.96])
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    Path(args.tmp_dir).mkdir(parents=True, exist_ok=True)
    configs = build_configs(args)
    stores = parse_store(args.store)
    started = time.time()
    per_store = []
    for idx, (store, days) in enumerate(stores, 1):
        rows = run_store(store, days, configs, args.tmp_dir)
        per_store.append(rows)
        (out_dir / f"per_store_{idx:02d}.json").write_text(json.dumps(rows, indent=2))
    combined = aggregate(configs, per_store)
    (out_dir / "combined_results.json").write_text(json.dumps(combined, indent=2))
    with (out_dir / "combined_results.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(combined[0].keys()))
        writer.writeheader()
        writer.writerows(combined)
    best = combined[0] if combined else {}
    manifest = {
        "script": Path(__file__).name,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "host": socket.gethostname(),
        "started_epoch": started,
        "ended_epoch": time.time(),
        "stores": [{"store": store, "days": days} for store, days in stores],
        "configs": len(configs),
        "best": best,
        "verdict": verdict(best) if best else "UNKNOWN",
    }
    (out_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({"out_dir": str(out_dir), "verdict": manifest["verdict"], "best": best}, indent=2))


if __name__ == "__main__":
    main()
