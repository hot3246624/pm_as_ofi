#!/usr/bin/env python3
"""Research-only maker-shadow runner for the Xuan m0001 candidate.

The runner is intentionally offline: it reads copied CSV rows from strict V2
taker-buy/cache or public-truth exports and writes local artifacts only.  It is
not a deploy verifier and must not be used as source-of-truth proof.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


class FillClass(str, Enum):
    REAL_FILL = "real_fill"
    QUEUE_SUPPORTED_FILL = "queue_supported_fill"
    TOUCH_ONLY = "touch_only"
    NO_TOUCH = "no_touch"


class ResidualBranch(str, Enum):
    NONE = "none"
    COMPLETION = "completion"
    AGED_OBSERVE = "aged_residual_observe"
    AGED_UNWIND = "aged_residual_unwind"
    BLOCKED_LOT_SKIP = "blocked_lot_skip"
    MATERIAL_LOCKOUT = "material_residual_lockout"


@dataclass(frozen=True)
class RunnerConfig:
    seed_edge: float = 0.022
    tick_size: float = 0.001
    max_cycles: int = 6
    max_seed_qty: float = 60.0
    min_fill_qty: float = 1.0
    open_pair_cap: float = 1.020
    queue_share: float = 0.25
    queue_haircut: float = 0.75
    completion_pair_cap: float = 0.990
    late_completion_pair_cap: float = 0.995
    final_completion_pair_cap: float = 1.010
    late_remaining_s: float = 60.0
    final_remaining_s: float = 30.0
    aged_unwind_pair_cap: float = 1.000
    blocked_skip_age_s: float = 120.0
    blocked_skip_margin: float = 0.001
    material_residual_qty: float = 6.0
    material_residual_cost: float = 6.0


@dataclass
class ShadowLot:
    side: str
    qty: float
    first_px: float
    opened_ts_ms: int
    cycle_id: int
    fill_class: FillClass

    def age_s(self, ts_ms: int) -> float:
        return max(0.0, (ts_ms - self.opened_ts_ms) / 1000.0)


@dataclass
class ShadowSummary:
    rows: int = 0
    seed_attempts: int = 0
    seed_fills: int = 0
    completion_attempts: int = 0
    completion_fills: int = 0
    touch_only: int = 0
    queue_supported_fills: int = 0
    real_fills: int = 0
    blocked_lot_skips: int = 0
    material_lockouts: int = 0
    completed_cycles: int = 0
    completed_qty: float = 0.0
    residual_qty: float = 0.0
    residual_cost: float = 0.0
    pnl_proxy: float = 0.0
    pair_cost_values: list[float] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        pair_cost_avg = (
            sum(self.pair_cost_values) / len(self.pair_cost_values)
            if self.pair_cost_values
            else None
        )
        return {
            "rows": self.rows,
            "seed_attempts": self.seed_attempts,
            "seed_fills": self.seed_fills,
            "completion_attempts": self.completion_attempts,
            "completion_fills": self.completion_fills,
            "touch_only": self.touch_only,
            "queue_supported_fills": self.queue_supported_fills,
            "real_fills": self.real_fills,
            "blocked_lot_skips": self.blocked_lot_skips,
            "material_lockouts": self.material_lockouts,
            "completed_cycles": self.completed_cycles,
            "completed_qty": self.completed_qty,
            "residual_qty": self.residual_qty,
            "residual_cost": self.residual_cost,
            "pnl_proxy": self.pnl_proxy,
            "pair_cost_avg": pair_cost_avg,
            "pair_cost_count": len(self.pair_cost_values),
        }


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_float(v: Any) -> float | None:
    try:
        if v in (None, ""):
            return None
        out = float(v)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def parse_int(v: Any) -> int | None:
    f = parse_float(v)
    return int(f) if f is not None else None


def parse_bool(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in {"1", "true", "yes", "y"}:
        return True
    if s in {"0", "false", "no", "n"}:
        return False
    return None


def first_present(row: dict[str, Any], names: Iterable[str]) -> float | None:
    for name in names:
        if name in row:
            v = parse_float(row.get(name))
            if v is not None:
                return v
    return None


def first_bool(row: dict[str, Any], names: Iterable[str]) -> bool | None:
    for name in names:
        if name in row:
            v = parse_bool(row.get(name))
            if v is not None:
                return v
    return None


def norm_side(v: Any) -> str | None:
    s = str(v or "").strip().upper()
    if s in {"YES", "Y"}:
        return "YES"
    if s in {"NO", "N"}:
        return "NO"
    return None


def norm_taker_side(v: Any) -> str | None:
    s = str(v or "").strip().upper()
    if s in {"BUY", "B", "TAKER_BUY", "TAKERSIDE::BUY"}:
        return "BUY"
    if s in {"SELL", "S", "TAKER_SELL", "TAKERSIDE::SELL"}:
        return "SELL"
    return None


def opposite_side(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def floor_to_tick(px: float, tick: float) -> float:
    if tick <= 0:
        return px
    return math.floor((px + 1e-12) / tick) * tick


def seed_price(public_trade_px: float, cfg: RunnerConfig) -> float:
    return max(cfg.tick_size, floor_to_tick(public_trade_px - cfg.seed_edge, cfg.tick_size))


def infer_remaining_s(row: dict[str, Any]) -> float | None:
    return first_present(
        row,
        (
            "remaining_s",
            "remaining_secs",
            "seconds_to_end",
            "secs_to_end",
            "seconds_to_market_end",
            "secs_to_market_end",
            "time_to_end_s",
            "round_remaining_s",
        ),
    )


def completion_pair_cap_for_row(row: dict[str, Any], cfg: RunnerConfig) -> float:
    remaining_s = infer_remaining_s(row)
    if remaining_s is None:
        return cfg.completion_pair_cap
    if remaining_s <= cfg.final_remaining_s + 1e-9:
        return cfg.final_completion_pair_cap
    if remaining_s <= cfg.late_remaining_s + 1e-9:
        return cfg.late_completion_pair_cap
    return cfg.completion_pair_cap


def infer_ts_ms(row: dict[str, Any], fallback: int) -> int:
    return (
        parse_int(row.get("ts_ms"))
        or parse_int(row.get("timestamp_ms"))
        or parse_int(row.get("event_ts_ms"))
        or fallback
    )


def infer_public_side(row: dict[str, Any]) -> str | None:
    return (
        norm_side(row.get("side"))
        or norm_side(row.get("market_side"))
        or norm_side(row.get("public_trade_side"))
        or norm_side(row.get("first_side"))
    )


def infer_public_taker_side(row: dict[str, Any]) -> str | None:
    for name in (
        "public_taker_side",
        "public_trade_taker_side",
        "trade_taker_side",
        "taker_side",
        "aggressor_side",
    ):
        if name in row:
            side = norm_taker_side(row.get(name))
            if side is not None:
                return side
    taker_buy = first_bool(row, ("public_taker_buy", "trade_taker_buy", "taker_buy"))
    if taker_buy is not None:
        return "BUY" if taker_buy else "SELL"
    taker_sell = first_bool(row, ("public_taker_sell", "trade_taker_sell", "taker_sell"))
    if taker_sell is not None:
        return "SELL" if taker_sell else "BUY"
    return None


def infer_public_trade_px(row: dict[str, Any], side: str | None = None) -> float | None:
    px = first_present(
        row,
        (
            "public_trade_px",
            "public_trade_price",
            "trade_px",
            "trade_price",
            "first_price",
            "first_vwap",
        ),
    )
    if px is not None:
        return px
    if side == "YES":
        return first_present(row, ("yes_trade_px", "yes_ask", "YES_ask"))
    if side == "NO":
        return first_present(row, ("no_trade_px", "no_ask", "NO_ask"))
    return None


def infer_public_trade_qty(row: dict[str, Any]) -> float:
    return (
        first_present(
            row,
            (
                "public_trade_size",
                "public_trade_qty",
                "trade_size",
                "trade_qty",
                "size",
                "qty",
                "clip",
                "first_l2_filled",
            ),
        )
        or 0.0
    )


def infer_completion_px(row: dict[str, Any], side: str) -> float | None:
    names = (
        ("no_completion_px", "no_ask", "NO_ask", "opp_l1_ask", "opposite_l1_ask", "low_l1_ask")
        if side == "NO"
        else ("yes_completion_px", "yes_ask", "YES_ask", "opp_l1_ask", "opposite_l1_ask", "low_l1_ask")
    )
    return first_present(row, names)


def infer_support_qty(row: dict[str, Any], action: str) -> float:
    action_names = (
        f"{action}_queue_supported_qty",
        f"{action}_depleted_qty",
        f"{action}_trade_through_qty",
    )
    generic_names = (
        "queue_supported_qty",
        "queue_depleted_qty",
        "low_l1_depleted_qty",
        "opposite_low_l1_depleted_qty",
        "depleted_qty",
        "trade_through_qty",
    )
    explicit = first_present(row, action_names)
    if explicit is not None:
        return explicit
    generic = first_present(row, generic_names)
    if generic is not None:
        return generic
    return infer_public_trade_qty(row)


def classify_fill(
    row: dict[str, Any],
    *,
    quote_px: float,
    reference_px: float | None,
    requested_qty: float,
    action: str,
    cfg: RunnerConfig,
) -> tuple[FillClass, float, dict[str, Any]]:
    real = first_bool(row, (f"{action}_real_fill", "real_fill", "xuan_public_fill", "official_fill"))
    if real:
        return FillClass.REAL_FILL, requested_qty, {"reason": "explicit_real_fill"}

    queue_supported = first_bool(
        row,
        (f"{action}_queue_supported_fill", "queue_supported_fill", "queue_fill"),
    )
    support_qty = infer_support_qty(row, action)
    fillable = support_qty * cfg.queue_haircut
    if queue_supported or fillable + 1e-9 >= requested_qty:
        return (
            FillClass.QUEUE_SUPPORTED_FILL,
            min(requested_qty, max(fillable, 0.0)) if not queue_supported else requested_qty,
            {
                "reason": "queue_supported",
                "support_qty": support_qty,
                "queue_share": cfg.queue_share,
                "queue_haircut": cfg.queue_haircut,
            },
        )

    touched = first_bool(row, (f"{action}_touch", "touch", "touched"))
    if touched is None and reference_px is not None:
        touched = reference_px + 1e-12 >= quote_px
    if touched:
        return (
            FillClass.TOUCH_ONLY,
            0.0,
            {
                "reason": "price_touched_without_queue_support",
                "support_qty": support_qty,
                "queue_share": cfg.queue_share,
                "queue_haircut": cfg.queue_haircut,
            },
        )

    return FillClass.NO_TOUCH, 0.0, {"reason": "not_touched", "support_qty": support_qty}


def update_fill_counts(summary: ShadowSummary, fill_class: FillClass) -> None:
    if fill_class == FillClass.REAL_FILL:
        summary.real_fills += 1
    elif fill_class == FillClass.QUEUE_SUPPORTED_FILL:
        summary.queue_supported_fills += 1
    elif fill_class == FillClass.TOUCH_ONLY:
        summary.touch_only += 1


class XuanM0001MakerShadowRunner:
    def __init__(self, cfg: RunnerConfig):
        self.cfg = cfg
        self.active: ShadowLot | None = None
        self.summary = ShadowSummary()
        self.events: list[dict[str, Any]] = []

    def emit(self, row: dict[str, Any], event: str, payload: dict[str, Any]) -> None:
        out = {
            "event": event,
            "market_id": row.get("market_id") or row.get("slug"),
            "ts_ms": payload.pop("ts_ms", None),
            **payload,
        }
        self.events.append(out)

    def run(self, rows: Iterable[dict[str, Any]]) -> ShadowSummary:
        for idx, row in enumerate(rows):
            self.summary.rows += 1
            ts_ms = infer_ts_ms(row, idx)
            had_active_lot = self.active is not None
            if self.active is not None:
                self._try_completion(row, ts_ms)
            if self.active is None and not had_active_lot:
                self._try_seed(row, ts_ms)
            elif self.active is not None and self._active_material_residual_blocks_new_seed():
                self.summary.material_lockouts += 1
                self.emit(
                    row,
                    "material_residual_lockout",
                    {
                        "ts_ms": ts_ms,
                        "branch": ResidualBranch.MATERIAL_LOCKOUT.value,
                        "cycle_id": self.active.cycle_id,
                        "residual_qty": self.active.qty,
                        "residual_cost": self.active.qty * self.active.first_px,
                    },
                )
        if self.active is not None:
            self.summary.residual_qty = self.active.qty
            self.summary.residual_cost = self.active.qty * self.active.first_px
            self.summary.pnl_proxy -= self.summary.residual_cost
        return self.summary

    def _active_material_residual_blocks_new_seed(self) -> bool:
        if self.active is None:
            return False
        residual_qty = max(0.0, self.active.qty)
        residual_cost = residual_qty * max(0.0, self.active.first_px)
        return (
            residual_qty > self.cfg.material_residual_qty
            or residual_cost > self.cfg.material_residual_cost
        )

    def _try_seed(self, row: dict[str, Any], ts_ms: int) -> None:
        if self.summary.completed_cycles >= self.cfg.max_cycles:
            self.emit(
                row,
                "seed_blocked_cycle_cap",
                {
                    "ts_ms": ts_ms,
                    "completed_cycles": self.summary.completed_cycles,
                    "cycle_cap": self.cfg.max_cycles,
                },
            )
            return
        side = infer_public_side(row)
        taker_side = infer_public_taker_side(row)
        if taker_side is not None and taker_side != "SELL":
            self.emit(
                row,
                "seed_skipped_non_sell_public_trade",
                {
                    "ts_ms": ts_ms,
                    "taker_side": taker_side,
                },
            )
            return
        public_px = infer_public_trade_px(row, side)
        public_qty = infer_public_trade_qty(row)
        if side is None or public_px is None or public_qty <= 0.0:
            return
        completion_side = opposite_side(side)
        opposite_ask = infer_completion_px(row, completion_side)
        if opposite_ask is None:
            return
        if public_px < 0.05 - 1e-9 or public_px > 0.90 + 1e-9:
            return
        if public_px + opposite_ask > self.cfg.open_pair_cap + 1e-9:
            return
        quote_px = seed_price(public_px, self.cfg)
        requested_qty = min(public_qty * self.cfg.queue_share, self.cfg.max_seed_qty)
        if requested_qty < self.cfg.min_fill_qty:
            return
        self.summary.seed_attempts += 1
        fill_class, filled_qty, meta = classify_fill(
            row,
            quote_px=quote_px,
            reference_px=public_px,
            requested_qty=requested_qty,
            action="seed",
            cfg=self.cfg,
        )
        update_fill_counts(self.summary, fill_class)
        self.emit(
            row,
            "seed_evaluated",
            {
                "ts_ms": ts_ms,
                "side": side,
                "taker_side": taker_side or "unknown",
                "quote_px": quote_px,
                "public_trade_px": public_px,
                "opposite_ask": opposite_ask,
                "requested_qty": requested_qty,
                "filled_qty": filled_qty,
                "fill_class": fill_class.value,
                **meta,
            },
        )
        if fill_class not in {FillClass.REAL_FILL, FillClass.QUEUE_SUPPORTED_FILL}:
            return
        if filled_qty < self.cfg.min_fill_qty:
            return
        self.summary.seed_fills += 1
        self.active = ShadowLot(
            side=side,
            qty=filled_qty,
            first_px=quote_px,
            opened_ts_ms=ts_ms,
            cycle_id=self.summary.completed_cycles + 1,
            fill_class=fill_class,
        )

    def _try_completion(self, row: dict[str, Any], ts_ms: int) -> None:
        assert self.active is not None
        lot = self.active
        completion_side = opposite_side(lot.side)
        completion_px = infer_completion_px(row, completion_side)
        if completion_px is None or lot.qty < self.cfg.min_fill_qty:
            return
        age_s = lot.age_s(ts_ms)
        pair_cost = lot.first_px + completion_px
        branch = ResidualBranch.COMPLETION
        pair_cap = completion_pair_cap_for_row(row, self.cfg)
        if age_s >= self.cfg.blocked_skip_age_s:
            if pair_cost > self.cfg.aged_unwind_pair_cap + self.cfg.blocked_skip_margin:
                self.summary.blocked_lot_skips += 1
                self.emit(
                    row,
                    "blocked_lot_skip",
                    {
                        "ts_ms": ts_ms,
                        "branch": ResidualBranch.BLOCKED_LOT_SKIP.value,
                        "cycle_id": lot.cycle_id,
                        "age_s": age_s,
                        "residual_qty": lot.qty,
                        "first_px": lot.first_px,
                        "completion_px": completion_px,
                        "pair_cost": pair_cost,
                        "margin": self.cfg.blocked_skip_margin,
                    },
                )
                return
            branch = (
                ResidualBranch.AGED_UNWIND
                if pair_cost <= self.cfg.aged_unwind_pair_cap
                else ResidualBranch.AGED_OBSERVE
            )
            pair_cap = self.cfg.aged_unwind_pair_cap
        if pair_cost > pair_cap + 1e-12:
            self.emit(
                row,
                "completion_observed",
                {
                    "ts_ms": ts_ms,
                    "branch": branch.value,
                    "cycle_id": lot.cycle_id,
                    "age_s": age_s,
                    "residual_qty": lot.qty,
                    "first_px": lot.first_px,
                    "completion_px": completion_px,
                    "pair_cost": pair_cost,
                    "pair_cap": pair_cap,
                },
            )
            return

        self.summary.completion_attempts += 1
        fill_class, filled_qty, meta = classify_fill(
            row,
            quote_px=completion_px,
            reference_px=completion_px,
            requested_qty=lot.qty,
            action="completion",
            cfg=self.cfg,
        )
        update_fill_counts(self.summary, fill_class)
        self.emit(
            row,
            "completion_evaluated",
            {
                "ts_ms": ts_ms,
                "branch": branch.value,
                "cycle_id": lot.cycle_id,
                "completion_side": completion_side,
                "age_s": age_s,
                "first_px": lot.first_px,
                "completion_px": completion_px,
                "pair_cost": pair_cost,
                "requested_qty": lot.qty,
                "filled_qty": filled_qty,
                "fill_class": fill_class.value,
                **meta,
            },
        )
        if fill_class not in {FillClass.REAL_FILL, FillClass.QUEUE_SUPPORTED_FILL}:
            return
        paired_qty = min(lot.qty, filled_qty)
        if paired_qty < self.cfg.min_fill_qty:
            return
        lot.qty -= paired_qty
        self.summary.completion_fills += 1
        self.summary.completed_qty += paired_qty
        self.summary.pair_cost_values.append(pair_cost)
        self.summary.pnl_proxy += paired_qty * (1.0 - pair_cost)
        if lot.qty < self.cfg.min_fill_qty:
            self.summary.completed_cycles += 1
            self.emit(
                row,
                "cycle_completed",
                {
                    "ts_ms": ts_ms,
                    "cycle_id": lot.cycle_id,
                    "paired_qty": paired_qty,
                    "pair_cost": pair_cost,
                    "completed_cycles": self.summary.completed_cycles,
                },
            )
            self.active = None


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-csv", required=True, help="Copied strict V2/public-truth CSV input")
    p.add_argument("--output-dir", required=True, help="Local artifact directory")
    p.add_argument("--queue-share", type=float, default=RunnerConfig.queue_share)
    p.add_argument("--queue-haircut", type=float, default=RunnerConfig.queue_haircut)
    p.add_argument("--seed-edge", type=float, default=RunnerConfig.seed_edge)
    p.add_argument("--open-pair-cap", type=float, default=RunnerConfig.open_pair_cap)
    p.add_argument("--completion-pair-cap", type=float, default=RunnerConfig.completion_pair_cap)
    p.add_argument(
        "--late-completion-pair-cap",
        type=float,
        default=RunnerConfig.late_completion_pair_cap,
    )
    p.add_argument(
        "--final-completion-pair-cap",
        type=float,
        default=RunnerConfig.final_completion_pair_cap,
    )
    p.add_argument("--late-remaining-s", type=float, default=RunnerConfig.late_remaining_s)
    p.add_argument("--final-remaining-s", type=float, default=RunnerConfig.final_remaining_s)
    p.add_argument("--aged-unwind-pair-cap", type=float, default=RunnerConfig.aged_unwind_pair_cap)
    p.add_argument("--max-cycles", type=int, default=RunnerConfig.max_cycles)
    p.add_argument("--max-seed-qty", type=float, default=RunnerConfig.max_seed_qty)
    p.add_argument("--blocked-skip-age-s", type=float, default=RunnerConfig.blocked_skip_age_s)
    p.add_argument("--blocked-skip-margin", type=float, default=RunnerConfig.blocked_skip_margin)
    p.add_argument("--material-residual-qty", type=float, default=RunnerConfig.material_residual_qty)
    p.add_argument("--material-residual-cost", type=float, default=RunnerConfig.material_residual_cost)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = RunnerConfig(
        seed_edge=args.seed_edge,
        open_pair_cap=args.open_pair_cap,
        queue_share=args.queue_share,
        queue_haircut=args.queue_haircut,
        completion_pair_cap=args.completion_pair_cap,
        late_completion_pair_cap=args.late_completion_pair_cap,
        final_completion_pair_cap=args.final_completion_pair_cap,
        late_remaining_s=args.late_remaining_s,
        final_remaining_s=args.final_remaining_s,
        aged_unwind_pair_cap=args.aged_unwind_pair_cap,
        max_cycles=args.max_cycles,
        max_seed_qty=args.max_seed_qty,
        blocked_skip_age_s=args.blocked_skip_age_s,
        blocked_skip_margin=args.blocked_skip_margin,
        material_residual_qty=args.material_residual_qty,
        material_residual_cost=args.material_residual_cost,
    )
    rows = load_csv(Path(args.input_csv))
    runner = XuanM0001MakerShadowRunner(cfg)
    summary = runner.run(rows)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "xuan_m0001_maker_shadow_summary.json"
    events_path = output_dir / "xuan_m0001_maker_shadow_events.jsonl"
    summary_path.write_text(
        json.dumps(
            {
                "generated_at": utc_now(),
                "input_csv": str(Path(args.input_csv).resolve()),
                "config": cfg.__dict__,
                "summary": summary.to_dict(),
                "warning": "research-only cache/store runner; queue-supported fills are not deploy proof",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    with events_path.open("w", encoding="utf-8") as f:
        for event in runner.events:
            f.write(json.dumps(event, sort_keys=True) + "\n")
    print(json.dumps({"summary": str(summary_path), "events": str(events_path)}, sort_keys=True))


if __name__ == "__main__":
    main()
