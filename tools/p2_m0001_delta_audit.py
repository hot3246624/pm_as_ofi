#!/usr/bin/env python3
"""Audit c6_e022 + conditional-skip m0001 actions against P2 L1 delta evidence.

This is an offline research script. It reads only completion_unwind_event_store_v2
and writes CSV/JSON artifacts. It does not touch live systems.
"""

from __future__ import annotations

import csv
import json
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


STORE = os.environ.get(
    "P2_STORE",
    "/mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb",
)
OUT = Path(
    os.environ.get(
        "OUT_DIR",
        "/home/ubuntu/xuan_frontier_runs/c6_m0001_p2_delta_audit_20260512_1030",
    )
)
TMP = os.environ.get("XUAN_DUCKDB_TMP", "/home/ubuntu/duckdb_tmp_p2_delta_audit")

DAYS = [
    "2026-05-02",
    "2026-05-03",
    "2026-05-04",
    "2026-05-05",
    "2026-05-06",
    "2026-05-07",
    "2026-05-08",
]

EDGE = 0.022
TARGET_QTY = 115.0
FILL_HAIRCUT = 0.25
CYCLE_CAP = 6
BASE_CAP = 0.990
LATE_CAP = 0.995
FINAL_CAP = 1.010
LATE_START_S = 240.0
FINAL_START_S = 285.0
SEED_PAIR_CAP = 1.020
SKIP_AGE_S = 120.0
SKIP_MARGIN = 0.001
MAX_SEED_QTY = 60.0
MAX_COMPLETION_QTY = 250.0
MAX_UNWIND_QTY = 250.0
MAX_OPEN_COST = 250.0
COOLDOWN_MS = 5000
DUST_QTY = 1.0
SEED_PX_LO = 0.05
SEED_PX_HI = 0.90
UNWIND_START_S = 270.0
FORCE_UNWIND_S = 300.0
MAX_UNWIND_LOSS = 0.0

TIERS = [10, 25, 60, 100, 250]


@dataclass
class Lot:
    qty: float
    px: float
    ts_ms: int
    side: str
    day: str
    market: str
    seed_event_id: str


@dataclass
class MarketState:
    lots: dict[str, deque[Lot]]
    winner: str | None = None
    cycles: int = 0
    last_seed_ts: int = -(10**18)
    open_cost: float = 0.0
    active: bool = False


def fnum(value: Any, default: float = math.nan) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def clean(value: Any) -> Any:
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


COLS = [
    "day",
    "event_kind",
    "event_id",
    "ts_ms",
    "condition_id",
    "offset_s",
    "side",
    "opposite_side",
    "winner_side",
    "side_alignment",
    "l1_pair_ask",
    "public_trade_taker_side",
    "public_trade_price",
    "public_trade_size",
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
COLS.extend(
    [
        "prev_side_bid",
        "prev_side_bid_sz",
        "prev_side_ask",
        "prev_side_ask_sz",
        "side_bid",
        "side_bid_sz",
        "side_ask",
        "side_ask_sz",
        "side_bid_delta_qty",
        "side_bid_level_drop_qty",
        "side_ask_delta_qty",
        "side_ask_level_lift_qty",
        "book_update_reason",
    ]
)
IDX = {name: idx for idx, name in enumerate(COLS)}


def row_get(row: tuple[Any, ...], name: str) -> Any:
    return row[IDX[name]]


def choose_clip(row: tuple[Any, ...], qty: float, prefix: str) -> tuple[float, float, int]:
    """Return vwap, filled qty, and tier for the requested clip."""
    for tier in TIERS:
        filled = fnum(row_get(row, f"{prefix}_filled_{tier}"), 0.0)
        full = bool(row_get(row, f"{prefix}_full_{tier}"))
        vwap = fnum(row_get(row, f"{prefix}_vwap_{tier}"))
        if full and filled + 1e-9 >= qty and not math.isnan(vwap):
            return vwap, qty, tier
    best_tier = TIERS[-1]
    filled = min(qty, fnum(row_get(row, f"{prefix}_filled_{best_tier}"), 0.0))
    vwap = fnum(row_get(row, f"{prefix}_vwap_{best_tier}"))
    if filled <= 0.0 or math.isnan(vwap):
        return math.nan, 0.0, best_tier
    return vwap, filled, best_tier


def seed_touch_support(row: tuple[Any, ...], qty: float, seed_px: float) -> tuple[bool, int, float, float]:
    """Conservative public-book support for filling a virtual bid at seed_px.

    This checks whether a same-side SELL clip of the desired size had a worst
    execution price at or below the virtual maker bid. It is public L1/L2
    evidence, not private queue truth.
    """
    for tier in TIERS:
        filled = fnum(row_get(row, f"sell_filled_{tier}"), 0.0)
        worst = fnum(row_get(row, f"sell_worst_px_{tier}"))
        if filled + 1e-9 >= qty and not math.isnan(worst):
            return worst <= seed_px + 1e-12, tier, worst, filled
    filled = fnum(row_get(row, "sell_filled_250"), 0.0)
    worst = fnum(row_get(row, "sell_worst_px_250"))
    return bool(filled > 0 and not math.isnan(worst) and worst <= seed_px + 1e-12), 250, worst, filled


def completion_cap(offset_s: float) -> float:
    if offset_s >= 270.0:
        return FINAL_CAP
    if offset_s >= LATE_START_S:
        return LATE_CAP
    return BASE_CAP


def target_for(offset_s: float) -> float:
    if offset_s >= 240.0:
        return 0.0
    if offset_s >= 180.0:
        return TARGET_QTY * 0.5
    return TARGET_QTY


def side_key(row: tuple[Any, ...]) -> str:
    side = row_get(row, "side")
    return str(side or "")


def opp_side_key(row: tuple[Any, ...]) -> str:
    opp = row_get(row, "opposite_side")
    return str(opp or "")


def other(side: str) -> str:
    if side == "YES":
        return "NO"
    if side == "NO":
        return "YES"
    return ""


def band_trade_size(size: float) -> str:
    if size < 20:
        return "lt20"
    if size < 60:
        return "20_60"
    if size < 120:
        return "60_120"
    if size < 250:
        return "120_250"
    return "ge250"


def band_pair(pair: float) -> str:
    if math.isnan(pair):
        return "nan"
    if pair <= 0.98:
        return "le098"
    if pair <= 0.99:
        return "098_099"
    if pair <= 1.00:
        return "099_100"
    if pair <= 1.01:
        return "100_101"
    if pair <= 1.02:
        return "101_102"
    return "gt102"


def add_bucket(bucket: dict[str, Any], qty: float, cost: float, supported: bool) -> None:
    bucket["actions"] += 1
    bucket["qty"] += qty
    bucket["cost"] += cost
    if supported:
        bucket["touch_actions"] += 1
        bucket["touch_qty"] += qty


def bucket_factory() -> dict[str, Any]:
    return {"actions": 0, "qty": 0.0, "cost": 0.0, "touch_actions": 0, "touch_qty": 0.0}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    Path(TMP).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(STORE, read_only=True)
    con.execute(f"pragma temp_directory='{TMP}'")
    con.execute("pragma memory_limit='3GB'")
    query = f"""
        select {", ".join(COLS)}
        from completion_unwind_events
        where day = ?
          and offset_s >= 0 and offset_s < 300
          and side in ('YES','NO')
          and event_kind in ('l1_price_change','public_trade')
        order by condition_id, ts_ms, event_id
    """

    states: dict[str, MarketState] = defaultdict(lambda: MarketState(lots={"YES": deque(), "NO": deque()}))
    totals: dict[str, Any] = defaultdict(float)
    totals["seed_actions"] = 0
    totals["completion_actions"] = 0
    totals["touch_supported_actions"] = 0
    totals["bid_drop_actions"] = 0
    totals["bid_delta_neg_actions"] = 0

    by_reason = defaultdict(bucket_factory)
    by_size = defaultdict(bucket_factory)
    by_pair_band = defaultdict(bucket_factory)

    seed_path = OUT / "seed_actions.csv"
    completion_path = OUT / "completion_actions.csv"
    seed_fields = [
        "day",
        "event_id",
        "market",
        "ts_ms",
        "offset_s",
        "side",
        "trade_px",
        "trade_size",
        "seed_px",
        "qty",
        "cost",
        "l1_pair_ask",
        "touch_supported",
        "touch_tier",
        "touch_worst_px",
        "touch_filled_qty",
        "book_update_reason",
        "side_bid",
        "prev_side_bid",
        "side_bid_sz",
        "prev_side_bid_sz",
        "side_bid_delta_qty",
        "side_bid_level_drop_qty",
    ]
    completion_fields = [
        "day",
        "event_id",
        "market",
        "ts_ms",
        "offset_s",
        "side",
        "paired_qty",
        "completion_px",
        "pair_cost_wavg",
        "cap",
        "delay_wavg_s",
        "book_update_reason",
        "side_ask_delta_qty",
        "side_ask_level_lift_qty",
    ]

    with seed_path.open("w", newline="") as sf, completion_path.open("w", newline="") as cf:
        seed_writer = csv.DictWriter(sf, fieldnames=seed_fields)
        comp_writer = csv.DictWriter(cf, fieldnames=completion_fields)
        seed_writer.writeheader()
        comp_writer.writeheader()

        for day in DAYS:
            print(f"processing {day}", flush=True)
            result = con.execute(query, [day])
            while True:
                rows = result.fetchmany(100_000)
                if not rows:
                    break
                for row in rows:
                    market = str(row_get(row, "condition_id"))
                    state = states[market]
                    if state.winner is None and row_get(row, "winner_side") is not None:
                        state.winner = str(row_get(row, "winner_side"))
                    event_kind = row_get(row, "event_kind")
                    offset_s = fnum(row_get(row, "offset_s"), 0.0)
                    ts_ms = int(row_get(row, "ts_ms"))
                    l1_pair = fnum(row_get(row, "l1_pair_ask"))

                    # Completion has priority.
                    completion_buy_side = side_key(row)
                    inventory_side = other(completion_buy_side)
                    lots = state.lots.get(inventory_side)
                    if lots:
                        cap = completion_cap(offset_s)
                        remaining = sum(l.qty for l in lots)
                        px, avail, _tier = choose_clip(row, min(remaining, MAX_COMPLETION_QTY), "buy")
                        paired = 0.0
                        pnl = 0.0
                        buy_cost = 0.0
                        delay_qty_s = 0.0
                        pair_cost_qty = 0.0
                        skipped: deque[Lot] = deque()
                        while lots and avail > 1e-9 and not math.isnan(px):
                            lot = lots[0]
                            pair_cost = lot.px + px
                            lot_age_s = max(0.0, (ts_ms - lot.ts_ms) / 1000.0)
                            if pair_cost <= cap + 1e-12:
                                take = min(lot.qty, avail)
                                paired += take
                                buy_cost += take * px
                                pnl += take * (1.0 - pair_cost)
                                delay_qty_s += take * lot_age_s
                                pair_cost_qty += take * pair_cost
                                lot.qty -= take
                                state.open_cost = max(0.0, state.open_cost - take * lot.px)
                                avail -= take
                                if lot.qty <= 1e-9:
                                    lots.popleft()
                                continue

                            # Conditional skip: if an aged blocked lot sits in front,
                            # let a younger lot pair only when it has a safety margin.
                            if lot_age_s >= SKIP_AGE_S:
                                found = False
                                for idx in range(1, len(lots)):
                                    younger = lots[idx]
                                    younger_pair_cost = younger.px + px
                                    if younger_pair_cost <= cap - SKIP_MARGIN + 1e-12:
                                        skipped.append(lots.popleft())
                                        found = True
                                        break
                                if found:
                                    continue
                            break

                        if skipped:
                            while skipped:
                                lots.appendleft(skipped.pop())

                        if paired > 0:
                            state.cycles += 1
                            totals["completion_actions"] += 1
                            totals["paired_qty"] += paired
                            totals["pnl"] += pnl
                            totals["completion_buy_cost"] += buy_cost
                            totals["delay_qty_s"] += delay_qty_s
                            totals["pair_cost_qty"] += pair_cost_qty
                            state.active = True
                            comp_writer.writerow(
                                {
                                    "day": row_get(row, "day"),
                                    "event_id": row_get(row, "event_id"),
                                    "market": market,
                                    "ts_ms": ts_ms,
                                    "offset_s": offset_s,
                                    "side": completion_buy_side,
                                    "paired_qty": paired,
                                    "completion_px": px,
                                    "pair_cost_wavg": pair_cost_qty / paired,
                                    "cap": cap,
                                    "delay_wavg_s": delay_qty_s / paired,
                                    "book_update_reason": row_get(row, "book_update_reason"),
                                    "side_ask_delta_qty": row_get(row, "side_ask_delta_qty"),
                                    "side_ask_level_lift_qty": row_get(row, "side_ask_level_lift_qty"),
                                }
                            )
                            continue

                    # Breakeven same-side unwind near the end of the market.
                    unwind_side = side_key(row)
                    unwind_lots = state.lots.get(unwind_side)
                    if unwind_lots and offset_s >= UNWIND_START_S:
                        desired = min(sum(l.qty for l in unwind_lots), MAX_UNWIND_QTY)
                        sell_px, sell_avail, _sell_tier = choose_clip(row, desired, "sell")
                        sold = 0.0
                        unwind_pnl = 0.0
                        if sell_avail > 1e-9 and not math.isnan(sell_px):
                            while unwind_lots and sold < desired - 1e-9:
                                lot = unwind_lots[0]
                                if sell_px < lot.px - MAX_UNWIND_LOSS and offset_s < FORCE_UNWIND_S:
                                    break
                                take = min(lot.qty, desired - sold)
                                sold += take
                                unwind_pnl += take * (sell_px - lot.px)
                                lot.qty -= take
                                state.open_cost = max(0.0, state.open_cost - take * lot.px)
                                if lot.qty <= 1e-9:
                                    unwind_lots.popleft()
                        if sold > 1e-9:
                            totals["unwind_actions"] += 1
                            totals["unwind_qty"] += sold
                            totals["pnl"] += unwind_pnl
                            state.active = True
                            continue

                    # Seed with maker-like first-leg improvement.
                    if event_kind != "public_trade":
                        continue
                    if row_get(row, "public_trade_taker_side") != "SELL":
                        continue
                    if row_get(row, "side_alignment") != "high":
                        continue
                    if math.isnan(l1_pair) or l1_pair > SEED_PAIR_CAP:
                        continue
                    if ts_ms - state.last_seed_ts < COOLDOWN_MS:
                        continue
                    if state.cycles >= CYCLE_CAP:
                        continue
                    seed_side = side_key(row)
                    if not seed_side:
                        continue
                    # Do not add when opposite inventory is unresolved.
                    if state.lots.get(other(seed_side)) and sum(l.qty for l in state.lots[other(seed_side)]) > DUST_QTY:
                        continue
                    trade_px = fnum(row_get(row, "public_trade_price"))
                    trade_size = fnum(row_get(row, "public_trade_size"), 0.0)
                    if math.isnan(trade_px) or trade_size <= 0:
                        continue
                    if not (SEED_PX_LO <= trade_px <= SEED_PX_HI):
                        continue
                    target = target_for(offset_s)
                    current_qty = sum(l.qty for l in state.lots[seed_side])
                    if target <= current_qty + DUST_QTY or state.open_cost >= MAX_OPEN_COST:
                        continue
                    seed_px = trade_px - EDGE
                    if seed_px <= 0:
                        continue
                    seed_px = max(0.01, seed_px)
                    qty = min(
                        MAX_SEED_QTY,
                        trade_size * FILL_HAIRCUT,
                        target - current_qty,
                        (MAX_OPEN_COST - state.open_cost) / max(seed_px, 1e-9),
                    )
                    if qty <= DUST_QTY:
                        continue
                    cost = qty * seed_px
                    supported, tier, worst, filled = seed_touch_support(row, qty, seed_px)

                    state.lots[seed_side].append(
                        Lot(
                            qty=qty,
                            px=seed_px,
                            ts_ms=ts_ms,
                            side=seed_side,
                            day=str(row_get(row, "day")),
                            market=market,
                            seed_event_id=str(row_get(row, "event_id")),
                        )
                    )
                    totals["seed_actions"] += 1
                    totals["seed_qty"] += qty
                    totals["seed_cost"] += cost
                    state.open_cost += cost
                    state.last_seed_ts = ts_ms
                    state.active = True
                    if supported:
                        totals["touch_supported_actions"] += 1
                        totals["touch_supported_qty"] += qty
                    if fnum(row_get(row, "side_bid_level_drop_qty"), 0.0) > 0:
                        totals["bid_drop_actions"] += 1
                        totals["bid_drop_qty"] += qty
                    if fnum(row_get(row, "side_bid_delta_qty"), 0.0) < 0:
                        totals["bid_delta_neg_actions"] += 1
                        totals["bid_delta_neg_qty"] += qty

                    reason = str(row_get(row, "book_update_reason") or "none")
                    add_bucket(by_reason[reason], qty, cost, supported)
                    add_bucket(by_size[band_trade_size(trade_size)], qty, cost, supported)
                    add_bucket(by_pair_band[band_pair(l1_pair)], qty, cost, supported)

                    seed_writer.writerow(
                        {
                            "day": row_get(row, "day"),
                            "event_id": row_get(row, "event_id"),
                            "market": market,
                            "ts_ms": ts_ms,
                            "offset_s": offset_s,
                            "side": seed_side,
                            "trade_px": trade_px,
                            "trade_size": trade_size,
                            "seed_px": seed_px,
                            "qty": qty,
                            "cost": cost,
                            "l1_pair_ask": l1_pair,
                            "touch_supported": int(supported),
                            "touch_tier": tier,
                            "touch_worst_px": worst,
                            "touch_filled_qty": filled,
                            "book_update_reason": reason,
                            "side_bid": row_get(row, "side_bid"),
                            "prev_side_bid": row_get(row, "prev_side_bid"),
                            "side_bid_sz": row_get(row, "side_bid_sz"),
                            "prev_side_bid_sz": row_get(row, "prev_side_bid_sz"),
                            "side_bid_delta_qty": row_get(row, "side_bid_delta_qty"),
                            "side_bid_level_drop_qty": row_get(row, "side_bid_level_drop_qty"),
                        }
                    )

    residual_qty = 0.0
    residual_cost = 0.0
    residual_markets = []
    for market, state in states.items():
        mq = 0.0
        mc = 0.0
        for lots in state.lots.values():
            for lot in lots:
                if lot.qty <= 1e-9:
                    continue
                mq += lot.qty
                mc += lot.qty * lot.px
        residual_qty += mq
        residual_cost += mc
        if mc > 0:
            residual_markets.append({"market": market, "residual_qty": mq, "residual_cost": mc})

    active_markets = sum(1 for state in states.values() if state.active)
    seed_qty = float(totals["seed_qty"])
    seed_cost = float(totals["seed_cost"])
    paired_qty = float(totals["paired_qty"])
    summary = {
        "store": STORE,
        "days": DAYS,
        "params": {
            "edge": EDGE,
            "target_qty": TARGET_QTY,
            "fill_haircut": FILL_HAIRCUT,
            "cycle_cap": CYCLE_CAP,
            "seed_pair_cap": SEED_PAIR_CAP,
            "skip_age_s": SKIP_AGE_S,
            "skip_margin": SKIP_MARGIN,
            "max_seed_qty": MAX_SEED_QTY,
            "max_completion_qty": MAX_COMPLETION_QTY,
            "max_open_cost": MAX_OPEN_COST,
            "cooldown_ms": COOLDOWN_MS,
            "seed_px_lo": SEED_PX_LO,
            "seed_px_hi": SEED_PX_HI,
            "unwind_start_s": UNWIND_START_S,
            "force_unwind_s": FORCE_UNWIND_S,
            "max_unwind_loss": MAX_UNWIND_LOSS,
        },
        "active_markets": active_markets,
        "seed_actions": int(totals["seed_actions"]),
        "seed_qty": seed_qty,
        "seed_cost": seed_cost,
        "completion_actions": int(totals["completion_actions"]),
        "paired_qty": paired_qty,
        "pnl": float(totals["pnl"]),
        "roi": float(totals["pnl"]) / seed_cost if seed_cost else None,
        "pair_cost_wavg": float(totals["pair_cost_qty"]) / paired_qty if paired_qty else None,
        "pair_delay_wavg_s": float(totals["delay_qty_s"]) / paired_qty if paired_qty else None,
        "qty_residual_rate": residual_qty / seed_qty if seed_qty else None,
        "cost_residual_rate": residual_cost / seed_cost if seed_cost else None,
        "residual_qty": residual_qty,
        "residual_cost": residual_cost,
        "residual_cost_gt6_market_rate": (
            sum(1 for r in residual_markets if r["residual_cost"] > 6.0) / active_markets if active_markets else None
        ),
        "touch_supported_actions": int(totals["touch_supported_actions"]),
        "touch_supported_qty": float(totals["touch_supported_qty"]),
        "touch_supported_action_rate": (
            float(totals["touch_supported_actions"]) / float(totals["seed_actions"])
            if totals["seed_actions"]
            else None
        ),
        "touch_supported_qty_rate": float(totals["touch_supported_qty"]) / seed_qty if seed_qty else None,
        "bid_drop_action_rate": float(totals["bid_drop_actions"]) / float(totals["seed_actions"]) if totals["seed_actions"] else None,
        "bid_delta_neg_action_rate": (
            float(totals["bid_delta_neg_actions"]) / float(totals["seed_actions"]) if totals["seed_actions"] else None
        ),
        "by_book_update_reason": by_reason,
        "by_trade_size_band": by_size,
        "by_l1_pair_band": by_pair_band,
        "top_residual_markets": sorted(residual_markets, key=lambda r: r["residual_cost"], reverse=True)[:20],
        "seed_actions_csv": str(seed_path),
        "completion_actions_csv": str(completion_path),
    }

    def convert(obj: Any) -> Any:
        if isinstance(obj, defaultdict):
            obj = dict(obj)
        if isinstance(obj, dict):
            return {k: convert(clean(v)) for k, v in obj.items()}
        if isinstance(obj, list):
            return [convert(v) for v in obj]
        return clean(obj)

    (OUT / "summary.json").write_text(json.dumps(convert(summary), indent=2, sort_keys=True))
    print(json.dumps(convert({k: summary[k] for k in [
        "active_markets",
        "seed_actions",
        "seed_qty",
        "completion_actions",
        "pnl",
        "roi",
        "pair_cost_wavg",
        "pair_delay_wavg_s",
        "qty_residual_rate",
        "cost_residual_rate",
        "residual_cost_gt6_market_rate",
        "touch_supported_action_rate",
        "touch_supported_qty_rate",
        "bid_drop_action_rate",
        "bid_delta_neg_action_rate",
        "seed_actions_csv",
        "completion_actions_csv",
    ]}), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
