#!/usr/bin/env python3
"""Read-only D+ passive/passive shadow runner.

This adapter is for the B27-inspired D+ branch. It sends no orders. It creates
virtual passive BUY bids on both YES and NO from public SELL flow, counts a fill
only when later public SELL flow crosses the virtual bid with configurable queue
share, pairs filled YES/NO lots internally, and records optional FAK salvage
opportunities for aged residual lots.

It is a live-style fill plausibility probe, not source-of-truth verification.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DUST = 1e-9


def now_ms() -> int:
    return int(time.time() * 1000)


def pct(vals: list[float] | list[int], p: float) -> float | None:
    if not vals:
        return None
    xs = sorted(vals)
    idx = min(len(xs) - 1, max(0, round((len(xs) - 1) * p)))
    return float(xs[idx])


def parse_env_exports(text: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("export "):
            continue
        key, _, raw = line[len("export ") :].partition("=")
        if key and raw:
            env[key] = shlex.split(raw)[0] if raw.startswith(("'", '"')) else raw
    return env


def round_start_from_slug(slug: str) -> int:
    m = re.search(r"-(\d+)$", slug)
    return int(m.group(1)) if m else 0


def side_from_str(raw: str) -> str:
    s = (raw or "").upper()
    if s in {"YES", "UP"}:
        return "YES"
    if s in {"NO", "DOWN"}:
        return "NO"
    return s


def opp(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def side_bid(book: dict[str, float], side: str) -> float:
    return book.get("yes_bid" if side == "YES" else "no_bid", 0.0)


def side_ask(book: dict[str, float], side: str) -> float:
    return book.get("yes_ask" if side == "YES" else "no_ask", 0.0)


def fee_per_share(px: float, rate: float) -> float:
    price = min(max(px, 0.0), 1.0)
    return rate * price * (1.0 - price)


def market_duration_s_from_slug(slug: str) -> float | None:
    if "-15m-" in slug:
        return 15.0 * 60.0
    if "-5m-" in slug:
        return 5.0 * 60.0
    if "up-or-down" in slug:
        return 60.0 * 60.0
    return None


def jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSONL at {path}:{lineno}: {exc}") from exc
        if not isinstance(row, dict):
            raise RuntimeError(f"invalid JSONL at {path}:{lineno}: expected object row")
        rows.append(row)
    return rows


def row_float(row: dict[str, Any], keys: tuple[str, ...], default: float | None = None) -> float | None:
    for key in keys:
        value = row.get(key)
        if value is None or value == "":
            continue
        try:
            out = float(value)
        except (TypeError, ValueError):
            continue
        return out if out == out else default
    return default


def fair_probability_for_side(row: dict[str, Any], side: str) -> float | None:
    direct = row_float(
        row,
        (
            "fair_side_probability",
            "side_fair_probability",
            "fair_probability_side",
            "fair_prob_side",
        ),
    )
    if direct is not None:
        return direct
    yes_prob = row_float(
        row,
        (
            "fair_probability_yes",
            "aggregate_fair_probability_yes",
            "fair_probability",
            "aggregate_fair_probability",
            "fair_prob",
            "p_yes",
        ),
    )
    if yes_prob is None:
        return None
    return yes_prob if side == "YES" else 1.0 - yes_prob


def source_sequence_id(msg: dict[str, Any]) -> Any:
    for key in ("source_sequence_id", "source_seq", "sequence_id", "seq"):
        value = msg.get(key)
        if value is not None:
            return value
    return None


def first_present(msg: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = msg.get(key)
        if value is not None and value != "":
            return value
    return None


def int_field(msg: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    value = first_present(msg, keys)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def event_time_ms(msg: dict[str, Any], fallback_ts_ms: int) -> int:
    value = msg.get("event_time_ms") or msg.get("market_event_time_ms") or msg.get("ts_ms")
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback_ts_ms


def l1_source_sequence_id(msg: dict[str, Any]) -> Any:
    return first_present(
        msg,
        (
            "l1_source_sequence_id",
            "book_l1_source_sequence_id",
            "strict_l1_source_sequence_id",
            "l1_source_row_id",
            "strict_l1_row_id",
            "book_l1_source_row_id",
        ),
    ) or source_sequence_id(msg)


def l2_source_sequence_id(msg: dict[str, Any]) -> Any:
    return first_present(
        msg,
        (
            "l2_source_sequence_id",
            "book_l2_source_sequence_id",
            "strict_l2_source_sequence_id",
            "l2_source_row_id",
            "strict_l2_row_id",
            "book_l2_source_row_id",
        ),
    )


def l2_event_time_ms(msg: dict[str, Any], fallback_ts_ms: int) -> int | None:
    value = int_field(
        msg,
        (
            "l2_event_time_ms",
            "book_l2_event_time_ms",
            "strict_l2_event_time_ms",
            "l2_recv_ms",
            "book_l2_recv_ms",
            "strict_l2_recv_ms",
        ),
    )
    if value is not None:
        return value
    return event_time_ms(msg, fallback_ts_ms) if l2_source_sequence_id(msg) is not None else None


def csv_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_value(row.get(key)) for key in fieldnames})


def collect_runner_process_snapshot() -> list[dict[str, Any]]:
    """Return same-runner process metadata for concurrent-reader audit only."""
    try:
        proc = subprocess.run(
            ["/bin/ps", "-eo", "pid=,user=,lstart=,args="],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return []
    if proc.returncode != 0:
        return []
    current_pid = os.getpid()
    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        if "xuan_dplus_passive_passive_shadow_runner" not in line:
            continue
        parts = line.strip().split(None, 8)
        if len(parts) < 9:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        if pid == current_pid:
            continue
        rows.append(
            {
                "pid": pid,
                "user": parts[1],
                "lstart": " ".join(parts[2:7]),
                "args": parts[8],
            }
        )
    return rows


def empty_source_linkage_stats() -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for prefix in ("book_l1", "book_l2", "trade"):
        stats.update(
            {
                f"{prefix}_observations": 0,
                f"{prefix}_source_nonempty": 0,
                f"{prefix}_source_missing": 0,
                f"{prefix}_event_time_nonempty": 0,
                f"{prefix}_first_source_sequence_id": None,
                f"{prefix}_last_source_sequence_id": None,
                f"{prefix}_event_time_min_ms": None,
                f"{prefix}_event_time_max_ms": None,
                f"{prefix}_recv_time_min_ms": None,
                f"{prefix}_recv_time_max_ms": None,
            }
        )
    return stats


class FairPriceAdmissionGate:
    """Default-off fair-price gate for b55/ce25-style admission rows."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        min_edge: float,
        max_pair_cost: float,
        min_seconds_to_close: float | None,
        max_seconds_to_close: float | None,
        max_row_age_ms: int | None,
    ) -> None:
        self.rows = rows
        self.min_edge = min_edge
        self.max_pair_cost = max_pair_cost
        self.min_seconds_to_close = min_seconds_to_close
        self.max_seconds_to_close = max_seconds_to_close
        self.max_row_age_ms = max_row_age_ms
        self.by_slug_side: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            slug = str(row.get("market_slug") or row.get("slug") or "")
            side = side_from_str(str(row.get("side") or row.get("outcome") or "YES"))
            if not slug or side not in {"YES", "NO"}:
                continue
            self.by_slug_side.setdefault((slug, side), []).append(row)

    @classmethod
    def from_path(
        cls,
        path: Path,
        *,
        min_edge: float,
        max_pair_cost: float,
        min_seconds_to_close: float | None,
        max_seconds_to_close: float | None,
        max_row_age_ms: int | None,
    ) -> "FairPriceAdmissionGate":
        return cls(
            jsonl_rows(path),
            min_edge=min_edge,
            max_pair_cost=max_pair_cost,
            min_seconds_to_close=min_seconds_to_close,
            max_seconds_to_close=max_seconds_to_close,
            max_row_age_ms=max_row_age_ms,
        )

    def row_applies(
        self,
        row: dict[str, Any],
        *,
        ts_ms: int,
        seconds_to_close: float | None,
    ) -> bool:
        valid_from = row_float(row, ("valid_from_ts_ms", "start_ts_ms"))
        valid_to = row_float(row, ("valid_to_ts_ms", "end_ts_ms"))
        if valid_from is not None and ts_ms < valid_from:
            return False
        if valid_to is not None and ts_ms > valid_to:
            return False
        row_ts = row_float(row, ("fair_probability_ts_ms", "sample_ts_ms", "venue_sample_ts_ms", "ts_ms"))
        if self.max_row_age_ms is not None and row_ts is not None and ts_ms - row_ts > self.max_row_age_ms:
            return False
        min_stc = row_float(row, ("min_seconds_to_close",), self.min_seconds_to_close)
        max_stc = row_float(row, ("max_seconds_to_close",), self.max_seconds_to_close)
        if seconds_to_close is not None:
            if min_stc is not None and seconds_to_close < min_stc:
                return False
            if max_stc is not None and seconds_to_close > max_stc:
                return False
        return True

    def select_row(self, slug: str, side: str, ts_ms: int, seconds_to_close: float | None) -> dict[str, Any] | None:
        candidates = self.by_slug_side.get((slug, side), [])
        selected = None
        selected_ts = -(10**18)
        for row in candidates:
            if not self.row_applies(row, ts_ms=ts_ms, seconds_to_close=seconds_to_close):
                continue
            row_ts = row_float(row, ("fair_probability_ts_ms", "sample_ts_ms", "venue_sample_ts_ms", "ts_ms"), 0.0) or 0.0
            if row_ts >= selected_ts:
                selected = row
                selected_ts = row_ts
        return selected

    def audit(
        self,
        *,
        slug: str,
        side: str,
        ts_ms: int,
        seed_px: float,
        opposite_ask: float,
        taker_fee_rate: float,
        seconds_to_close: float | None,
    ) -> tuple[str | None, dict[str, Any]]:
        row = self.select_row(slug, side, ts_ms, seconds_to_close)
        audit: dict[str, Any] = {
            "fair_price_admission_enabled": True,
            "fair_price_admission_decision": "allow",
            "fair_price_admission_block_reason": "",
            "fair_price_min_edge": self.min_edge,
            "fair_price_max_pair_cost": self.max_pair_cost,
            "fair_price_seconds_to_close": seconds_to_close,
            "fair_price_side_probability": None,
            "fair_price_edge_after_fee": None,
            "fair_price_pair_cost_after_fee": None,
            "fair_price_row_id": None,
        }
        if row is None:
            audit["fair_price_admission_decision"] = "block"
            audit["fair_price_admission_block_reason"] = "fair_price_admission_missing"
            return "fair_price_admission_missing", audit
        fair_prob = fair_probability_for_side(row, side)
        audit["fair_price_row_id"] = row.get("row_id") or row.get("id") or row.get("market_slug") or slug
        audit["fair_price_side_probability"] = fair_prob
        if fair_prob is None:
            audit["fair_price_admission_decision"] = "block"
            audit["fair_price_admission_block_reason"] = "fair_price_probability_missing"
            return "fair_price_probability_missing", audit
        seed_fee = fee_per_share(seed_px, taker_fee_rate)
        edge_after_fee = fair_prob - seed_px - seed_fee
        comp_fee = fee_per_share(opposite_ask, taker_fee_rate) if opposite_ask > 0 else None
        pair_cost = seed_px + opposite_ask + seed_fee + comp_fee if comp_fee is not None else None
        audit["fair_price_edge_after_fee"] = round(edge_after_fee, 12)
        audit["fair_price_pair_cost_after_fee"] = round(pair_cost, 12) if pair_cost is not None else None
        if edge_after_fee < self.min_edge - 1e-12:
            audit["fair_price_admission_decision"] = "block"
            audit["fair_price_admission_block_reason"] = "fair_price_edge_after_fee"
            return "fair_price_edge_after_fee", audit
        if pair_cost is None or pair_cost > self.max_pair_cost + 1e-12:
            audit["fair_price_admission_decision"] = "block"
            audit["fair_price_admission_block_reason"] = "fair_price_pair_cost_after_fee"
            return "fair_price_pair_cost_after_fee", audit
        return None, audit


@dataclass(frozen=True)
class RunnerConfig:
    edge: float = 0.040
    queue_share: float = 0.50
    target_qty: float = 5.0
    fill_haircut: float = 0.25
    max_seed_qty: float = 60.0
    max_open_cost: float = 80.0
    seed_l1_cap: float = 1.02
    seed_px_lo: float = 0.05
    seed_px_hi: float = 0.90
    seed_offset_min_s: float = 0.0
    seed_offset_max_s: float = 120.0
    late_target_after_s: float | None = None
    late_target_qty: float | None = None
    late_repair_after_s: float | None = None
    cooldown_ms: int = 5_000
    order_ttl_ms: int = 120_000
    imbalance_qty_cap: float = 2.0
    imbalance_cost_cap: float = 1_000_000_000.0
    pairing_only_when_residual: bool = False
    high_price_mode: str = "none"
    high_price_normal_px_hi: float | None = None
    activation_mode: str = "none"
    activation_window_s: float = 60.0
    risk_seed_closeability_net_cap: float | None = None
    risk_seed_closeability_soft_net_cap: float | None = None
    risk_seed_closeability_debt_floor: float | None = None
    risk_seed_closeability_debt_budget: float = 0.0
    risk_seed_pending_opp_credit: float = 1.0
    pair_completion_net_cap: float | None = None
    pair_completion_min_pair_pnl_after: float | None = None
    dust_qty: float = 1.0
    taker_fee_rate: float = 0.07
    salvage_net_cap: float = 0.95
    salvage_age_ms: int = 30_000
    salvage_min_lot_cost: float = 0.25
    strict_rescue_skip_low_cost_lots: bool = False
    max_salvage_qty: float = 250.0
    surplus_budget_mode: str = "none"
    surplus_budget_bootstrap: float = 0.0
    surplus_budget_mult: float = 0.0
    surplus_budget_max_abs_unpaired_cost: float | None = None
    strict_rescue_mode: str = "none"
    strict_rescue_l1_age_max_ms: int | None = None
    strict_rescue_max_wait_ms: int | None = None
    strict_rescue_close_size_haircut: float = 1.0
    strict_rescue_close_ask_slip: float = 0.0
    strict_rescue_surplus_net_cap: float | None = None
    strict_rescue_min_pair_pnl_after: float | None = None
    strict_rescue_require_book_source: bool = False
    strict_rescue_require_l2_source: bool = False
    source_quality_require_trade_source: bool = False
    source_quality_require_l1_source: bool = False
    source_quality_l1_age_max_ms: int | None = None
    source_quality_require_l2_source: bool = False
    fair_price_admission_path: str | None = None
    fair_price_min_edge: float = 0.015
    fair_price_max_pair_cost: float = 0.975
    fair_price_min_seconds_to_close: float | None = None
    fair_price_max_seconds_to_close: float | None = None
    fair_price_max_row_age_ms: int | None = None
    write_normalized_lifecycle: bool = False
    write_rescue_block_diagnostics: bool = False
    allow_concurrent_shared_ingress_readers: bool = False

    def target_for(self, offset_s: float | None) -> float:
        if (
            offset_s is not None
            and self.late_target_after_s is not None
            and self.late_target_qty is not None
            and offset_s >= self.late_target_after_s
        ):
            return min(self.target_qty, self.late_target_qty)
        return self.target_qty

    def late_target_active(self, offset_s: float | None) -> bool:
        return (
            offset_s is not None
            and self.late_target_after_s is not None
            and self.late_target_qty is not None
            and offset_s >= self.late_target_after_s
        )

    def late_repair_active(self, offset_s: float | None) -> bool:
        return offset_s is not None and self.late_repair_after_s is not None and offset_s >= self.late_repair_after_s


@dataclass
class VirtualOrder:
    id: int
    quote_intent_id: str
    condition_id: str
    side: str
    px: float
    qty: float
    created_ms: int
    accepted_ms: int
    offset_s: float
    trigger_px: float
    trigger_size: float
    trigger_ts_ms: int
    trigger_source_sequence_id: Any | None = None
    opposite_trigger_ts_ms: int | None = None
    closeability_comp_ask: float | None = None
    closeability_net_pair_cost: float | None = None
    risk_seed_closeability_soft_net_cap: float | None = None
    risk_seed_closeability_debt_floor: float | None = None
    risk_seed_closeability_debt_budget: float = 0.0
    closeability_debt_per_share: float = 0.0
    closeability_debt: float = 0.0
    queue_credit: float = 0.0
    first_bid_touch_ms: int | None = None
    first_trade_touch_ms: int | None = None
    fill_ms: int | None = None
    cancel_ms: int | None = None
    cancel_reason: str | None = None


@dataclass
class Lot:
    id: int
    quote_intent_id: str
    side: str
    qty: float
    px: float
    fill_ms: int
    source_order_id: int
    source_sequence_id: Any | None = None
    source_event_time_ms: int | None = None
    closeability_debt_per_share: float = 0.0

    @property
    def cost(self) -> float:
        return self.qty * self.px


@dataclass
class Metrics:
    candidates: int = 0
    cancelled_orders: int = 0
    queue_supported_fills: int = 0
    touch_only_orders: int = 0
    pair_actions: int = 0
    salvage_actions: int = 0
    strict_rescue_actions: int = 0
    strict_rescue_source_blocks: int = 0
    surplus_budget_blocks: int = 0
    material_lockout_blocks: int = 0
    seed_qty: float = 0.0
    seed_cost: float = 0.0
    filled_qty: float = 0.0
    filled_cost: float = 0.0
    pair_qty: float = 0.0
    pair_pnl: float = 0.0
    taker_fee: float = 0.0
    completion_cost: float = 0.0
    salvage_qty: float = 0.0
    strict_rescue_qty: float = 0.0
    surplus_budget_capped_qty: float = 0.0
    residual_qty: float = 0.0
    residual_cost: float = 0.0
    pair_costs: list[float] = field(default_factory=list)
    net_pair_costs: list[float] = field(default_factory=list)
    fill_wait_ms: list[int] = field(default_factory=list)
    pair_wait_ms: list[int] = field(default_factory=list)
    salvage_wait_ms: list[int] = field(default_factory=list)
    closeability_debt_reserved: float = 0.0
    closeability_debt_released: float = 0.0
    closeability_debt_max_open: float = 0.0


class DPlusRunner:
    def __init__(
        self,
        slug: str,
        out_dir: Path,
        cfg: RunnerConfig,
        condition_id: str | None = None,
        fair_price_gate: FairPriceAdmissionGate | None = None,
    ) -> None:
        self.slug = slug
        self.condition_id = condition_id or slug
        self.start_s = round_start_from_slug(slug)
        self.duration_s = market_duration_s_from_slug(slug)
        self.out_dir = out_dir
        self.cfg = cfg
        self.fair_price_gate = fair_price_gate
        self.book: dict[str, float] = {}
        self.book_ts_ms: int | None = None
        self.book_event_time_ms: int | None = None
        self.book_source_sequence_id: Any | None = None
        self.book_l2_ts_ms: int | None = None
        self.book_l2_event_time_ms: int | None = None
        self.book_l2_source_sequence_id: Any | None = None
        self.pending: list[VirtualOrder] = []
        self.lots: dict[str, list[Lot]] = {"YES": [], "NO": []}
        self.metrics = Metrics()
        self.surplus_bank = cfg.surplus_budget_bootstrap
        self.book_ticks = 0
        self.trade_ticks = 0
        self.sell_triggers = 0
        self.blocked: dict[str, int] = {}
        self.closeability_debt_open = 0.0
        self.source_linkage = empty_source_linkage_stats()
        self.next_order_id = 1
        self.next_lot_id = 1
        self.last_seed_ms = -(10**18)
        self.activation_last_seen_ms: dict[str, int | None] = {"YES": None, "NO": None}
        self.events_path = out_dir / f"{slug}.events.jsonl"
        self.summary_path = out_dir / f"{slug}.summary.json"

    def quote_intent_id(self, order_id: int) -> str:
        return f"{self.slug}:quote:{order_id}"

    def blocked_quote_intent_id(self, side: str, ts_ms: int) -> str:
        return f"{self.slug}:blocked:{side}:{ts_ms}:{self.blocked.get('activation_opp_seen', 0)}"

    def offset_s(self, ts_ms: int) -> float | None:
        return ts_ms / 1000.0 - self.start_s if self.start_s else None

    def seconds_to_close(self, ts_ms: int) -> float | None:
        offset = self.offset_s(ts_ms)
        if offset is None or self.duration_s is None:
            return None
        return max(0.0, self.duration_s - offset)

    def emit(self, obj: dict[str, Any]) -> None:
        with self.events_path.open("a") as f:
            f.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")

    def block(self, reason: str) -> None:
        self.blocked[reason] = self.blocked.get(reason, 0) + 1

    def adjust_closeability_debt(self, delta: float) -> None:
        if abs(delta) <= 1e-12:
            return
        if delta > 0:
            self.metrics.closeability_debt_reserved += delta
        else:
            self.metrics.closeability_debt_released += -delta
        self.closeability_debt_open = max(0.0, self.closeability_debt_open + delta)
        self.metrics.closeability_debt_max_open = max(
            self.metrics.closeability_debt_max_open,
            self.closeability_debt_open,
        )

    def observe_source_linkage(
        self,
        prefix: str,
        source_id: Any | None,
        event_time_ms_value: int | None,
        recv_time_ms_value: int | None,
    ) -> None:
        stats = self.source_linkage
        stats[f"{prefix}_observations"] += 1
        if source_id is None or source_id == "":
            stats[f"{prefix}_source_missing"] += 1
        else:
            stats[f"{prefix}_source_nonempty"] += 1
            if stats[f"{prefix}_first_source_sequence_id"] is None:
                stats[f"{prefix}_first_source_sequence_id"] = source_id
            stats[f"{prefix}_last_source_sequence_id"] = source_id
        if event_time_ms_value is not None:
            stats[f"{prefix}_event_time_nonempty"] += 1
            event_min_key = f"{prefix}_event_time_min_ms"
            event_max_key = f"{prefix}_event_time_max_ms"
            stats[event_min_key] = (
                event_time_ms_value
                if stats[event_min_key] is None
                else min(int(stats[event_min_key]), event_time_ms_value)
            )
            stats[event_max_key] = (
                event_time_ms_value
                if stats[event_max_key] is None
                else max(int(stats[event_max_key]), event_time_ms_value)
            )
        if recv_time_ms_value is not None:
            recv_min_key = f"{prefix}_recv_time_min_ms"
            recv_max_key = f"{prefix}_recv_time_max_ms"
            stats[recv_min_key] = (
                recv_time_ms_value
                if stats[recv_min_key] is None
                else min(int(stats[recv_min_key]), recv_time_ms_value)
            )
            stats[recv_max_key] = (
                recv_time_ms_value
                if stats[recv_max_key] is None
                else max(int(stats[recv_max_key]), recv_time_ms_value)
            )

    def l1_age_ms(self, ts_ms: int) -> int | None:
        return ts_ms - self.book_ts_ms if self.book_ts_ms is not None else None

    def l2_age_ms(self, ts_ms: int) -> int | None:
        return ts_ms - self.book_l2_ts_ms if self.book_l2_ts_ms is not None else None

    def source_quality_audit(self, ts_ms: int, trade_source_sequence_id: Any | None) -> tuple[str | None, dict[str, Any]]:
        l1_age_ms = self.l1_age_ms(ts_ms)
        l2_age_ms = self.l2_age_ms(ts_ms)
        audit = {
            "source_quality_require_trade_source": self.cfg.source_quality_require_trade_source,
            "source_quality_require_l1_source": self.cfg.source_quality_require_l1_source,
            "source_quality_l1_age_max_ms": self.cfg.source_quality_l1_age_max_ms,
            "source_quality_require_l2_source": self.cfg.source_quality_require_l2_source,
            "source_quality_trade_source_sequence_id": trade_source_sequence_id,
            "source_quality_l1_source_sequence_id": self.book_source_sequence_id,
            "source_quality_l1_event_time_ms": self.book_event_time_ms,
            "source_quality_l1_age_ms": l1_age_ms,
            "source_quality_l2_source_sequence_id": self.book_l2_source_sequence_id,
            "source_quality_l2_event_time_ms": self.book_l2_event_time_ms,
            "source_quality_l2_age_ms": l2_age_ms,
            "source_quality_decision": "allow",
            "source_quality_block_reason": "",
        }
        reason: str | None = None
        if self.cfg.source_quality_require_trade_source and trade_source_sequence_id is None:
            reason = "source_quality_missing_trade_source"
        elif self.cfg.source_quality_require_l1_source and self.book_source_sequence_id is None:
            reason = "source_quality_missing_l1_source"
        elif (
            self.cfg.source_quality_l1_age_max_ms is not None
            and (l1_age_ms is None or l1_age_ms > self.cfg.source_quality_l1_age_max_ms)
        ):
            reason = "source_quality_l1_age"
        elif self.cfg.source_quality_require_l2_source and self.book_l2_source_sequence_id is None:
            reason = "source_quality_missing_l2_source"
        if reason:
            audit["source_quality_decision"] = "block"
            audit["source_quality_block_reason"] = reason
        return reason, audit

    def emit_source_quality_block(
        self,
        ts_ms: int,
        side: str,
        px: float,
        size: float,
        trigger_event_time_ms: int,
        audit: dict[str, Any],
    ) -> None:
        self.emit(
            {
                "kind": "source_quality_block",
                "slug": self.slug,
                "condition_id": self.condition_id,
                "ts_ms": ts_ms,
                "side": side,
                "price": max(0.01, px - self.cfg.edge),
                "size": min(self.cfg.max_seed_qty, size * self.cfg.fill_haircut),
                "accepted_ts_ms": None,
                "source": "no_order_source_quality_gate",
                "source_sequence_id": audit.get("source_quality_trade_source_sequence_id"),
                "market_md_source_sequence_id": audit.get("source_quality_trade_source_sequence_id"),
                "trigger_ts_ms": ts_ms,
                "trigger_event_time_ms": trigger_event_time_ms,
                "public_trade_px": px,
                "public_trade_size": size,
                **audit,
            }
        )

    def record_activation_seen(self, side: str, ts_ms: int) -> None:
        if side in self.activation_last_seen_ms:
            self.activation_last_seen_ms[side] = ts_ms

    def activation_allows_seed(self, side: str, ts_ms: int) -> tuple[bool, int | None]:
        if self.cfg.activation_mode == "none":
            return True, None
        if self.cfg.activation_mode != "opp_seen":
            raise ValueError(f"unsupported activation_mode={self.cfg.activation_mode!r}")
        opp_seen_ms = self.activation_last_seen_ms.get(opp(side))
        if opp_seen_ms is None:
            return False, None
        age_ms = ts_ms - opp_seen_ms
        return age_ms >= 0 and age_ms <= self.cfg.activation_window_s * 1000.0 + 1e-9, int(age_ms)

    def pending_orders(self, side: str | None = None) -> list[VirtualOrder]:
        out = [o for o in self.pending if not o.fill_ms and not o.cancel_ms]
        return [o for o in out if o.side == side] if side else out

    def pending_qty(self, side: str) -> float:
        return sum(order.qty for order in self.pending_orders(side))

    def pending_cost(self, side: str) -> float:
        return sum(order.qty * order.px for order in self.pending_orders(side))

    def lot_qty(self, side: str) -> float:
        return sum(lot.qty for lot in self.lots[side])

    def lot_cost(self, side: str) -> float:
        return sum(lot.cost for lot in self.lots[side])

    def exposure_qty(self, side: str) -> float:
        return self.lot_qty(side) + self.pending_qty(side)

    def exposure_cost(self, side: str) -> float:
        return self.lot_cost(side) + self.pending_cost(side)

    def credited_opp_qty_for_seed(self, side: str) -> float:
        opposite = opp(side)
        return self.lot_qty(opposite) + self.cfg.risk_seed_pending_opp_credit * self.pending_qty(opposite)

    def pair_completion_audit(self, side: str, seed_px: float, qty: float) -> dict[str, Any]:
        matched_qty = 0.0
        weighted_cost = 0.0
        projected_pair_pnl_delta = 0.0
        worst_cost: float | None = None
        remaining = qty
        for lot in self.lots[opp(side)]:
            if remaining <= DUST:
                break
            take = min(lot.qty, remaining)
            if take <= DUST:
                continue
            cost = seed_px + lot.px
            matched_qty += take
            weighted_cost += take * cost
            projected_pair_pnl_delta += take * (1.0 - cost)
            worst_cost = cost if worst_cost is None else max(worst_cost, cost)
            remaining -= take
        avg_cost = weighted_cost / matched_qty if matched_qty > DUST else None
        projected_pair_pnl_after = self.metrics.pair_pnl + projected_pair_pnl_delta
        decision = "not_enabled" if self.cfg.pair_completion_net_cap is None else "allow"
        if self.cfg.pair_completion_min_pair_pnl_after is not None:
            decision = "allow"
        if matched_qty <= DUST:
            decision = "no_opposite_lots" if self.cfg.pair_completion_net_cap is not None else "not_enabled"
            if self.cfg.pair_completion_min_pair_pnl_after is not None:
                decision = "no_opposite_lots"
        return {
            "pair_completion_net_cap": self.cfg.pair_completion_net_cap,
            "pair_completion_min_pair_pnl_after": self.cfg.pair_completion_min_pair_pnl_after,
            "pair_completion_qty": round(matched_qty, 12),
            "pair_completion_avg_net_pair_cost": round(avg_cost, 12) if avg_cost is not None else None,
            "pair_completion_worst_net_pair_cost": round(worst_cost, 12) if worst_cost is not None else None,
            "pair_completion_pair_pnl_delta": round(projected_pair_pnl_delta, 12),
            "pair_completion_projected_pair_pnl_after": round(projected_pair_pnl_after, 12),
            "pair_completion_decision": decision,
        }

    def total_open_cost(self) -> float:
        return self.exposure_cost("YES") + self.exposure_cost("NO")

    def surplus_budget_allowance(self) -> float | None:
        if self.cfg.surplus_budget_mode == "none":
            return None
        allowed: float | None = None
        if self.cfg.surplus_budget_bootstrap > 0.0 or self.cfg.surplus_budget_mult > 0.0:
            allowed = self.cfg.surplus_budget_bootstrap + self.cfg.surplus_budget_mult * max(0.0, self.surplus_bank)
        if self.cfg.surplus_budget_max_abs_unpaired_cost is not None:
            allowed = (
                self.cfg.surplus_budget_max_abs_unpaired_cost
                if allowed is None
                else min(allowed, self.cfg.surplus_budget_max_abs_unpaired_cost)
            )
        return allowed

    def apply_surplus_budget(
        self,
        side: str,
        qty: float,
        seed_px: float,
        ts_ms: int,
        trigger_source_sequence_id: Any | None,
    ) -> tuple[float | None, dict[str, Any]]:
        audit = {
            "surplus_budget_mode": self.cfg.surplus_budget_mode,
            "surplus_budget_pre_bank": round(self.surplus_bank, 12),
            "surplus_budget_allowed": None,
            "surplus_budget_projected_unpaired_cost": None,
            "surplus_budget_decision": "not_enabled",
        }
        if self.cfg.surplus_budget_mode == "none":
            return qty, audit
        allowed = self.surplus_budget_allowance()
        if allowed is None:
            audit["surplus_budget_decision"] = "allow_unbounded"
            return qty, audit
        side_cost = self.exposure_cost(side)
        opp_cost = self.exposure_cost(opp(side))
        projected_unpaired_cost = max(0.0, side_cost + qty * seed_px - opp_cost)
        audit.update(
            {
                "surplus_budget_allowed": round(allowed, 12),
                "surplus_budget_projected_unpaired_cost": round(projected_unpaired_cost, 12),
            }
        )
        if projected_unpaired_cost <= allowed + 1e-12:
            audit["surplus_budget_decision"] = "allow"
            return qty, audit
        if self.cfg.surplus_budget_mode == "cap":
            capped_qty = max(0.0, (allowed + opp_cost - side_cost) / max(seed_px, 1e-9))
            capped_qty = min(qty, capped_qty)
            if capped_qty <= self.cfg.dust_qty:
                self.metrics.surplus_budget_blocks += 1
                self.block("surplus_budget")
                audit["surplus_budget_decision"] = "block_after_cap"
                self.emit({"kind": "surplus_budget_block", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "side": side, "qty": qty, "seed_px": seed_px, "source_sequence_id": trigger_source_sequence_id, **audit})
                return None, audit
            self.metrics.surplus_budget_capped_qty += qty - capped_qty
            audit["surplus_budget_decision"] = "cap"
            audit["surplus_budget_capped_qty"] = round(qty - capped_qty, 12)
            return capped_qty, audit
        if self.cfg.surplus_budget_mode == "block":
            self.metrics.surplus_budget_blocks += 1
            self.block("surplus_budget")
            audit["surplus_budget_decision"] = "block"
            self.emit({"kind": "surplus_budget_block", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "side": side, "qty": qty, "seed_px": seed_px, "source_sequence_id": trigger_source_sequence_id, **audit})
            return None, audit
        raise ValueError(f"unsupported surplus_budget_mode={self.cfg.surplus_budget_mode!r}")

    def mark_touches(self, ts_ms: int, trade_side: str | None = None, trade_px: float | None = None) -> None:
        for order in self.pending_orders():
            bid = side_bid(self.book, order.side)
            if bid >= order.px - 1e-12 and order.first_bid_touch_ms is None:
                order.first_bid_touch_ms = ts_ms
                self.emit({"kind": "touch", "touch_type": "bid_touch", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "side": order.side, "order_px": order.px, "bid": bid, "wait_ms": ts_ms - order.created_ms})
            if trade_side == order.side and trade_px is not None and trade_px <= order.px + 1e-12 and order.first_trade_touch_ms is None:
                order.first_trade_touch_ms = ts_ms
                self.emit({"kind": "touch", "touch_type": "trade_through", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "side": order.side, "order_px": order.px, "trade_px": trade_px, "wait_ms": ts_ms - order.created_ms})

    def cancel_expired(self, ts_ms: int) -> None:
        offset = self.offset_s(ts_ms)
        for order in self.pending_orders():
            expired = ts_ms - order.created_ms >= self.cfg.order_ttl_ms
            too_late = offset is not None and offset >= 240
            if not expired and not too_late:
                continue
            debt_pre_open = self.closeability_debt_open
            self.adjust_closeability_debt(-order.closeability_debt)
            order.cancel_ms = ts_ms
            order.cancel_reason = "offset_240" if too_late else "ttl"
            self.metrics.cancelled_orders += 1
            if order.first_bid_touch_ms or order.first_trade_touch_ms:
                self.metrics.touch_only_orders += 1
            self.emit({"kind": "cancel", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "cancel_ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "reason": order.cancel_reason, "side": order.side, "price": order.px, "px": order.px, "size": order.qty, "qty": order.qty, "placed_ts_ms": order.created_ms, "accepted_ts_ms": order.accepted_ms, "opposite_trigger_ts_ms": order.opposite_trigger_ts_ms, "queue_credit": order.queue_credit, "closeability_debt": order.closeability_debt, "closeability_debt_per_share": order.closeability_debt_per_share, "closeability_debt_pre_open": round(debt_pre_open, 12), "closeability_debt_post_open": round(self.closeability_debt_open, 12)})

    def fill_order(
        self,
        order: VirtualOrder,
        ts_ms: int,
        trade_px: float,
        trade_size: float,
        trigger_source_sequence_id: Any | None,
        trigger_event_time_ms: int,
    ) -> None:
        order.fill_ms = ts_ms
        self.metrics.queue_supported_fills += 1
        self.metrics.filled_qty += order.qty
        self.metrics.filled_cost += order.qty * order.px
        self.metrics.fill_wait_ms.append(ts_ms - order.created_ms)
        lot = Lot(id=self.next_lot_id, quote_intent_id=order.quote_intent_id, side=order.side, qty=order.qty, px=order.px, fill_ms=ts_ms, source_order_id=order.id, source_sequence_id=trigger_source_sequence_id, source_event_time_ms=trigger_event_time_ms, closeability_debt_per_share=order.closeability_debt_per_share)
        self.next_lot_id += 1
        self.lots[order.side].append(lot)
        self.emit({"kind": "queue_supported_fill", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "fill_ts_ms": ts_ms, "event_time_ms": trigger_event_time_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "lot_id": lot.id, "side": order.side, "source": "no_order_public_trade_queue_proxy", "seed_px": order.px, "price": order.px, "size": order.qty, "qty": order.qty, "queue_share": self.cfg.queue_share, "queue_credit": order.queue_credit, "trade_px": trade_px, "trade_size": trade_size, "trigger_ts_ms": ts_ms, "trigger_source_sequence_id": trigger_source_sequence_id, "source_sequence_id": trigger_source_sequence_id, "market_md_source_sequence_id": trigger_source_sequence_id, "placed_ts_ms": order.created_ms, "accepted_ts_ms": order.accepted_ms, "opposite_trigger_ts_ms": order.opposite_trigger_ts_ms, "fill_wait_ms": ts_ms - order.created_ms, "closeability_debt_per_share": order.closeability_debt_per_share, "closeability_debt": order.closeability_debt})

    def pair_inventory(self, ts_ms: int) -> None:
        yes = self.lots["YES"]
        no = self.lots["NO"]
        while yes and no:
            a, b = yes[0], no[0]
            take = min(a.qty, b.qty)
            if take <= DUST:
                break
            pair_cost = a.px + b.px
            older = min(a.fill_ms, b.fill_ms)
            self.metrics.pair_actions += 1
            self.metrics.pair_qty += take
            pnl = take * (1.0 - pair_cost)
            self.metrics.pair_pnl += pnl
            self.surplus_bank += pnl
            self.metrics.pair_costs.append(pair_cost)
            self.metrics.net_pair_costs.append(pair_cost)
            self.metrics.pair_wait_ms.append(ts_ms - older)
            closeability_debt_release = take * (a.closeability_debt_per_share + b.closeability_debt_per_share)
            debt_pre_open = self.closeability_debt_open
            self.adjust_closeability_debt(-closeability_debt_release)
            a.qty -= take
            b.qty -= take
            matched_pair_id = f"{self.slug}:pair:{self.metrics.pair_actions}"
            self.emit({"kind": "internal_pair", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "matched_pair_id": matched_pair_id, "yes_quote_intent_id": a.quote_intent_id, "no_quote_intent_id": b.quote_intent_id, "quote_intent_ids": [a.quote_intent_id, b.quote_intent_id], "qty": take, "yes_px": a.px, "no_px": b.px, "pair_cost": pair_cost, "delay_ms": ts_ms - older, "closeability_debt_release": round(closeability_debt_release, 12), "closeability_debt_pre_open": round(debt_pre_open, 12), "closeability_debt_post_open": round(self.closeability_debt_open, 12)})
            if a.qty <= DUST:
                yes.pop(0)
            if b.qty <= DUST:
                no.pop(0)

    def try_salvage(self, ts_ms: int) -> None:
        if self.cfg.salvage_net_cap <= 0:
            return
        for held_side in ("YES", "NO"):
            lots = self.lots[held_side]
            if not lots:
                continue
            comp_side = opp(held_side)
            raw_ask = side_ask(self.book, comp_side)
            if raw_ask <= 0:
                continue
            strict_rescue_active = self.cfg.strict_rescue_mode != "none"
            book_age_ms = self.l1_age_ms(ts_ms)
            l2_age_ms = self.l2_age_ms(ts_ms)
            diag_base = {
                "kind": "strict_rescue_block",
                "slug": self.slug,
                "condition_id": self.condition_id,
                "ts_ms": ts_ms,
                "held_side": held_side,
                "comp_side": comp_side,
                "raw_comp_ask": raw_ask,
                "source_sequence_id": self.book_source_sequence_id,
                "market_md_source_sequence_id": self.book_source_sequence_id,
                "book_event_time_ms": self.book_event_time_ms,
                "strict_rescue_l1_age_ms": book_age_ms,
                "strict_rescue_l1_age_max_ms": self.cfg.strict_rescue_l1_age_max_ms,
                "strict_rescue_l2_source_sequence_id": self.book_l2_source_sequence_id,
                "strict_rescue_l2_event_time_ms": self.book_l2_event_time_ms,
                "strict_rescue_l2_age_ms": l2_age_ms,
                "residual_lot_count": len(lots),
                "oldest_lot_age_ms": ts_ms - lots[0].fill_ms if lots else None,
                "oldest_lot_cost": lots[0].cost if lots else None,
                "oldest_lot_px": lots[0].px if lots else None,
                "oldest_lot_qty": lots[0].qty if lots else None,
                "oldest_lot_id": lots[0].id if lots else None,
                "oldest_quote_intent_id": lots[0].quote_intent_id if lots else None,
            }
            if strict_rescue_active:
                if self.cfg.strict_rescue_require_book_source and self.book_source_sequence_id is None:
                    self.metrics.strict_rescue_source_blocks += 1
                    self.block("strict_rescue_missing_book_source")
                    if self.cfg.write_rescue_block_diagnostics:
                        self.emit({**diag_base, "block_reason": "strict_rescue_missing_book_source"})
                    continue
                if self.cfg.strict_rescue_require_l2_source and self.book_l2_source_sequence_id is None:
                    self.metrics.strict_rescue_source_blocks += 1
                    self.block("strict_rescue_missing_l2_source")
                    if self.cfg.write_rescue_block_diagnostics:
                        self.emit({**diag_base, "block_reason": "strict_rescue_missing_l2_source"})
                    continue
                if (
                    self.cfg.strict_rescue_l1_age_max_ms is not None
                    and book_age_ms is not None
                    and book_age_ms > self.cfg.strict_rescue_l1_age_max_ms
                ):
                    self.block("strict_rescue_l1_age")
                    if self.cfg.write_rescue_block_diagnostics:
                        self.emit({**diag_base, "block_reason": "strict_rescue_l1_age"})
                    continue
            ask = raw_ask + (self.cfg.strict_rescue_close_ask_slip if strict_rescue_active else 0.0)
            fee = fee_per_share(ask, self.cfg.taker_fee_rate)
            paired = 0.0
            lot_idx = 0
            skipped_low_cost_lots = 0
            while lot_idx < len(lots) and paired < self.cfg.max_salvage_qty - DUST:
                lot = lots[lot_idx]
                age = ts_ms - lot.fill_ms
                if age < self.cfg.salvage_age_ms:
                    if strict_rescue_active and self.cfg.write_rescue_block_diagnostics:
                        self.emit(
                            {
                                **diag_base,
                                "block_reason": "strict_rescue_lot_age_or_min_cost",
                                "block_component": "lot_age",
                                "lot_index": lot_idx,
                                "lot_age_ms": age,
                                "lot_cost": lot.cost,
                                "salvage_age_ms": self.cfg.salvage_age_ms,
                                "salvage_min_lot_cost": self.cfg.salvage_min_lot_cost,
                                "strict_rescue_skip_low_cost_lots": self.cfg.strict_rescue_skip_low_cost_lots,
                                "strict_rescue_skipped_low_cost_lots": skipped_low_cost_lots,
                            }
                        )
                    break
                if lot.cost < self.cfg.salvage_min_lot_cost:
                    if strict_rescue_active and self.cfg.write_rescue_block_diagnostics:
                        self.emit(
                            {
                                **diag_base,
                                "block_reason": "strict_rescue_lot_age_or_min_cost",
                                "block_component": "lot_min_cost",
                                "lot_index": lot_idx,
                                "lot_age_ms": age,
                                "lot_cost": lot.cost,
                                "salvage_age_ms": self.cfg.salvage_age_ms,
                                "salvage_min_lot_cost": self.cfg.salvage_min_lot_cost,
                                "strict_rescue_skip_low_cost_lots": self.cfg.strict_rescue_skip_low_cost_lots,
                                "strict_rescue_skipped_low_cost_lots": skipped_low_cost_lots,
                            }
                        )
                    if self.cfg.strict_rescue_skip_low_cost_lots:
                        skipped_low_cost_lots += 1
                        lot_idx += 1
                        continue
                    break
                if strict_rescue_active and self.cfg.strict_rescue_max_wait_ms is not None and age > self.cfg.strict_rescue_max_wait_ms:
                    self.block("strict_rescue_max_wait")
                    if self.cfg.write_rescue_block_diagnostics:
                        self.emit(
                            {
                                **diag_base,
                                "block_reason": "strict_rescue_max_wait",
                                "lot_index": lot_idx,
                                "lot_age_ms": age,
                                "strict_rescue_max_wait_ms": self.cfg.strict_rescue_max_wait_ms,
                                "strict_rescue_skip_low_cost_lots": self.cfg.strict_rescue_skip_low_cost_lots,
                                "strict_rescue_skipped_low_cost_lots": skipped_low_cost_lots,
                            }
                        )
                    break
                gross_pair = lot.px + ask
                net_pair = gross_pair + fee
                size_haircut = self.cfg.strict_rescue_close_size_haircut if strict_rescue_active else 1.0
                take = min(lot.qty * max(0.0, size_haircut), self.cfg.max_salvage_qty - paired)
                if take <= DUST:
                    break
                projected_pair_pnl_delta = take * (1.0 - net_pair)
                projected_pair_pnl_after = self.metrics.pair_pnl + projected_pair_pnl_delta
                surplus_rescue_enabled = (
                    strict_rescue_active
                    and self.cfg.strict_rescue_surplus_net_cap is not None
                    and self.cfg.strict_rescue_min_pair_pnl_after is not None
                )
                surplus_rescue_allowed = (
                    surplus_rescue_enabled
                    and net_pair <= self.cfg.strict_rescue_surplus_net_cap + 1e-12
                    and projected_pair_pnl_after >= self.cfg.strict_rescue_min_pair_pnl_after - 1e-12
                )
                if net_pair > self.cfg.salvage_net_cap + 1e-12 and not surplus_rescue_allowed:
                    block_reason = "strict_rescue_net_pair_cap"
                    if surplus_rescue_enabled:
                        if net_pair > self.cfg.strict_rescue_surplus_net_cap + 1e-12:
                            block_reason = "strict_rescue_surplus_net_cap"
                        else:
                            block_reason = "strict_rescue_pair_pnl_floor"
                    if strict_rescue_active and self.cfg.write_rescue_block_diagnostics:
                        self.emit(
                            {
                                **diag_base,
                                "block_reason": block_reason,
                                "lot_index": lot_idx,
                                "lot_age_ms": age,
                                "held_px": lot.px,
                                "comp_ask": ask,
                                "fee_per_share": fee,
                                "gross_pair_cost": gross_pair,
                                "net_pair_cost": net_pair,
                                "salvage_net_cap": self.cfg.salvage_net_cap,
                                "strict_rescue_surplus_net_cap": self.cfg.strict_rescue_surplus_net_cap,
                                "strict_rescue_min_pair_pnl_after": self.cfg.strict_rescue_min_pair_pnl_after,
                                "strict_rescue_pair_pnl_delta": round(projected_pair_pnl_delta, 12),
                                "strict_rescue_projected_pair_pnl_after": round(projected_pair_pnl_after, 12),
                                "strict_rescue_surplus_decision": "block",
                                "strict_rescue_skip_low_cost_lots": self.cfg.strict_rescue_skip_low_cost_lots,
                                "strict_rescue_skipped_low_cost_lots": skipped_low_cost_lots,
                            }
                        )
                    break
                paired += take
                self.metrics.salvage_actions += 1
                if strict_rescue_active:
                    self.metrics.strict_rescue_actions += 1
                    self.metrics.strict_rescue_qty += take
                self.metrics.salvage_qty += take
                self.metrics.taker_fee += take * fee
                self.metrics.completion_cost += take * ask
                self.metrics.pair_actions += 1
                self.metrics.pair_qty += take
                pnl = projected_pair_pnl_delta
                self.metrics.pair_pnl += pnl
                self.surplus_bank += pnl
                self.metrics.pair_costs.append(gross_pair)
                self.metrics.net_pair_costs.append(net_pair)
                self.metrics.salvage_wait_ms.append(age)
                self.metrics.pair_wait_ms.append(age)
                matched_pair_id = f"{self.slug}:salvage:{self.metrics.salvage_actions}"
                closeability_debt_release = take * lot.closeability_debt_per_share
                debt_pre_open = self.closeability_debt_open
                self.adjust_closeability_debt(-closeability_debt_release)
                self.emit({"kind": "fak_salvage", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "matched_pair_id": matched_pair_id, "quote_intent_id": lot.quote_intent_id, "held_side": held_side, "comp_side": comp_side, "qty": take, "held_px": lot.px, "comp_ask": ask, "raw_comp_ask": raw_ask, "fee_per_share": fee, "net_pair_cost": net_pair, "age_ms": age, "source_sequence_id": self.book_source_sequence_id, "market_md_source_sequence_id": self.book_source_sequence_id, "book_event_time_ms": self.book_event_time_ms, "strict_rescue_mode": self.cfg.strict_rescue_mode, "strict_rescue_l1_age_ms": book_age_ms, "strict_rescue_l1_age_max_ms": self.cfg.strict_rescue_l1_age_max_ms, "strict_rescue_close_ask_slip": self.cfg.strict_rescue_close_ask_slip if strict_rescue_active else 0.0, "strict_rescue_close_size_haircut": self.cfg.strict_rescue_close_size_haircut if strict_rescue_active else 1.0, "strict_rescue_surplus_net_cap": self.cfg.strict_rescue_surplus_net_cap, "strict_rescue_min_pair_pnl_after": self.cfg.strict_rescue_min_pair_pnl_after, "strict_rescue_pair_pnl_delta": round(projected_pair_pnl_delta, 12), "strict_rescue_projected_pair_pnl_after": round(projected_pair_pnl_after, 12), "strict_rescue_surplus_decision": "allow_surplus" if net_pair > self.cfg.salvage_net_cap + 1e-12 else "within_target_cap", "strict_rescue_l2_required": self.cfg.strict_rescue_require_l2_source, "strict_rescue_l2_source_sequence_id": self.book_l2_source_sequence_id, "strict_rescue_l2_event_time_ms": self.book_l2_event_time_ms, "strict_rescue_l2_age_ms": l2_age_ms, "strict_rescue_skip_low_cost_lots": self.cfg.strict_rescue_skip_low_cost_lots, "strict_rescue_skipped_low_cost_lots": skipped_low_cost_lots, "source_lot_id": lot.id, "source_lot_sequence_id": lot.source_sequence_id, "source_lot_event_time_ms": lot.source_event_time_ms, "source_lot_closeability_debt_per_share": lot.closeability_debt_per_share, "source_lot_closeability_debt_release": round(closeability_debt_release, 12), "closeability_debt_pre_open": round(debt_pre_open, 12), "closeability_debt_post_open": round(self.closeability_debt_open, 12)})
                lot.qty -= take
                if lot.qty <= DUST:
                    lots.pop(lot_idx)
                else:
                    break

    def on_book(self, msg: dict[str, Any]) -> None:
        self.book_ticks += 1
        ts_ms = int(msg.get("ts_ms") or now_ms())
        self.book = {
            "yes_bid": float(msg.get("yes_bid") or 0.0),
            "yes_ask": float(msg.get("yes_ask") or 0.0),
            "no_bid": float(msg.get("no_bid") or 0.0),
            "no_ask": float(msg.get("no_ask") or 0.0),
        }
        self.book_ts_ms = ts_ms
        self.book_event_time_ms = event_time_ms(msg, ts_ms)
        self.book_source_sequence_id = l1_source_sequence_id(msg)
        self.book_l2_source_sequence_id = l2_source_sequence_id(msg)
        self.book_l2_event_time_ms = l2_event_time_ms(msg, ts_ms)
        self.book_l2_ts_ms = (
            int_field(msg, ("l2_recv_ms", "book_l2_recv_ms", "strict_l2_recv_ms", "l2_ts_ms", "book_l2_ts_ms"))
            if self.book_l2_source_sequence_id is not None
            else None
        )
        if self.book_l2_source_sequence_id is not None and self.book_l2_ts_ms is None:
            self.book_l2_ts_ms = ts_ms
        self.observe_source_linkage("book_l1", self.book_source_sequence_id, self.book_event_time_ms, ts_ms)
        self.observe_source_linkage("book_l2", self.book_l2_source_sequence_id, self.book_l2_event_time_ms, self.book_l2_ts_ms)
        self.mark_touches(ts_ms)
        self.cancel_expired(ts_ms)
        self.pair_inventory(ts_ms)
        self.try_salvage(ts_ms)

    def on_trade(self, msg: dict[str, Any]) -> None:
        self.trade_ticks += 1
        ts_ms = int(msg.get("ts_ms") or now_ms())
        side = side_from_str(str(msg.get("market_side") or ""))
        taker = str(msg.get("taker_side") or "").upper()
        px = float(msg.get("price") or 0.0)
        size = float(msg.get("size") or 0.0)
        offset = self.offset_s(ts_ms)
        trigger_source_sequence_id = source_sequence_id(msg)
        trigger_event_time_ms = event_time_ms(msg, ts_ms)
        self.observe_source_linkage("trade", trigger_source_sequence_id, trigger_event_time_ms, ts_ms)

        if taker == "SELL" and side in {"YES", "NO"}:
            self.mark_touches(ts_ms, side, px)
            for order in self.pending_orders(side):
                if px > order.px + 1e-12:
                    continue
                order.queue_credit += max(0.0, size * self.cfg.queue_share)
                if order.queue_credit + 1e-9 >= order.qty:
                    self.fill_order(order, ts_ms, px, size, trigger_source_sequence_id, trigger_event_time_ms)
            self.pair_inventory(ts_ms)
            self.try_salvage(ts_ms)
        else:
            self.mark_touches(ts_ms)
            self.try_salvage(ts_ms)
            return

        self.sell_triggers += 1
        self.cancel_expired(ts_ms)
        source_quality_reason, source_quality_audit = self.source_quality_audit(ts_ms, trigger_source_sequence_id)
        if source_quality_reason:
            self.block(source_quality_reason)
            self.emit_source_quality_block(ts_ms, side, px, size, trigger_event_time_ms, source_quality_audit)
            self.record_activation_seen(side, ts_ms)
            return
        if offset is None or offset < self.cfg.seed_offset_min_s or offset >= self.cfg.seed_offset_max_s:
            self.block("offset")
            return
        if not (self.cfg.seed_px_lo <= px <= self.cfg.seed_px_hi):
            self.block("price")
            return
        yes_ask = side_ask(self.book, "YES")
        no_ask = side_ask(self.book, "NO")
        if yes_ask <= 0 or no_ask <= 0:
            self.block("missing_pair_ask")
            return
        l1_pair = yes_ask + no_ask
        if l1_pair > self.cfg.seed_l1_cap + 1e-12:
            self.block("l1_pair_ask_gt_cap")
            return
        if ts_ms - self.last_seed_ms < self.cfg.cooldown_ms:
            self.block("cooldown")
            return

        same_qty = self.exposure_qty(side)
        opp_qty = self.exposure_qty(opp(side))
        credited_opp_qty = self.credited_opp_qty_for_seed(side)
        pending_opp_qty = self.pending_qty(opp(side))
        target_qty = self.cfg.target_for(offset)
        seed_px = max(0.01, px - self.cfg.edge)
        opposite_seen_ms = self.activation_last_seen_ms.get(opp(side))
        high_price_close_only_active = (
            self.cfg.high_price_mode == "close_only"
            and self.cfg.high_price_normal_px_hi is not None
            and px > self.cfg.high_price_normal_px_hi + 1e-12
        )
        if high_price_close_only_active and credited_opp_qty <= same_qty + self.cfg.dust_qty:
            self.block("high_price_close_only_no_opp")
            self.record_activation_seen(side, ts_ms)
            return
        if self.cfg.pairing_only_when_residual and same_qty > credited_opp_qty + self.cfg.dust_qty:
            self.block("pairing_only_when_residual")
            self.record_activation_seen(side, ts_ms)
            return
        if self.cfg.late_repair_active(offset) and same_qty + self.cfg.dust_qty >= credited_opp_qty:
            self.block("late_repair_only")
            self.record_activation_seen(side, ts_ms)
            return
        risk_increasing_seed = same_qty + self.cfg.dust_qty >= credited_opp_qty
        activation_ok, activation_opp_age_ms = self.activation_allows_seed(side, ts_ms)
        if risk_increasing_seed and not activation_ok:
            self.block("activation_opp_seen")
            blocked_quote_intent_id = self.blocked_quote_intent_id(side, ts_ms)
            self.emit(
                {
                    "kind": "activation_block",
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "ts_ms": ts_ms,
                    "quote_intent_id": blocked_quote_intent_id,
                    "side": side,
                    "price": seed_px,
                    "size": min(self.cfg.max_seed_qty, size * self.cfg.fill_haircut),
                    "placed_ts_ms": ts_ms,
                    "accepted_ts_ms": None,
                    "source": "no_order_public_trade_activation_gate",
                    "source_sequence_id": trigger_source_sequence_id,
                    "market_md_source_sequence_id": trigger_source_sequence_id,
                    "trigger_ts_ms": ts_ms,
                    "trigger_event_time_ms": trigger_event_time_ms,
                    "public_trade_px": px,
                    "public_trade_size": size,
                    "offset_s": offset,
                    "activation_mode": self.cfg.activation_mode,
                    "activation_window_s": self.cfg.activation_window_s,
                    "activation_required": True,
                    "risk_increasing_seed": risk_increasing_seed,
                    "risk_seed_pending_opp_credit": self.cfg.risk_seed_pending_opp_credit,
                    "risk_seed_pending_opp_qty": pending_opp_qty,
                    "risk_seed_credited_opp_qty": credited_opp_qty,
                    "opposite_trigger_ts_ms": opposite_seen_ms,
                    "opp_last_seen_ms": opposite_seen_ms,
                }
            )
            self.record_activation_seen(side, ts_ms)
            return
        closeability_net_pair_cost = None
        closeability_comp_ask = side_ask(self.book, opp(side))
        if closeability_comp_ask > 0:
            closeability_fee = fee_per_share(
                closeability_comp_ask + self.cfg.strict_rescue_close_ask_slip,
                self.cfg.taker_fee_rate,
            )
            closeability_net_pair_cost = (
                seed_px
                + closeability_comp_ask
                + self.cfg.strict_rescue_close_ask_slip
                + closeability_fee
            )
        closeability_debt_floor = (
            self.cfg.risk_seed_closeability_debt_floor
            if self.cfg.risk_seed_closeability_debt_floor is not None
            else self.cfg.salvage_net_cap
        )
        closeability_audit = {
            "risk_seed_closeability_soft_net_cap": self.cfg.risk_seed_closeability_soft_net_cap,
            "risk_seed_closeability_debt_floor": (
                closeability_debt_floor
                if self.cfg.risk_seed_closeability_soft_net_cap is not None
                else self.cfg.risk_seed_closeability_debt_floor
            ),
            "risk_seed_closeability_debt_budget": self.cfg.risk_seed_closeability_debt_budget,
            "closeability_debt_per_share": 0.0,
            "closeability_debt": 0.0,
            "closeability_debt_pre_open": round(self.closeability_debt_open, 12),
            "closeability_debt_post_open": round(self.closeability_debt_open, 12),
            "risk_seed_closeability_soft_decision": "not_enabled",
        }
        fair_price_audit = {
            "fair_price_admission_enabled": False,
            "fair_price_admission_decision": "not_enabled",
            "fair_price_admission_block_reason": "",
            "fair_price_min_edge": self.cfg.fair_price_min_edge,
            "fair_price_max_pair_cost": self.cfg.fair_price_max_pair_cost,
            "fair_price_seconds_to_close": self.seconds_to_close(ts_ms),
            "fair_price_side_probability": None,
            "fair_price_edge_after_fee": None,
            "fair_price_pair_cost_after_fee": None,
            "fair_price_row_id": None,
        }
        if self.fair_price_gate is not None:
            fair_price_reason, fair_price_audit = self.fair_price_gate.audit(
                slug=self.slug,
                side=side,
                ts_ms=ts_ms,
                seed_px=seed_px,
                opposite_ask=closeability_comp_ask,
                taker_fee_rate=self.cfg.taker_fee_rate,
                seconds_to_close=self.seconds_to_close(ts_ms),
            )
            if fair_price_reason:
                self.block(fair_price_reason)
                self.emit(
                    {
                        "kind": "fair_price_admission_block",
                        "slug": self.slug,
                        "condition_id": self.condition_id,
                        "ts_ms": ts_ms,
                        "side": side,
                        "price": seed_px,
                        "public_trade_px": px,
                        "public_trade_size": size,
                        "source_sequence_id": trigger_source_sequence_id,
                        "market_md_source_sequence_id": trigger_source_sequence_id,
                        "block_reason": fair_price_reason,
                        "risk_increasing_seed": risk_increasing_seed,
                        "same_exposure_qty": same_qty,
                        "opp_exposure_qty": opp_qty,
                        "closeability_comp_ask": closeability_comp_ask,
                        "closeability_net_pair_cost": closeability_net_pair_cost,
                        **fair_price_audit,
                        **source_quality_audit,
                    }
                )
                self.record_activation_seen(side, ts_ms)
                return
        if (
            risk_increasing_seed
            and self.cfg.risk_seed_closeability_net_cap is not None
            and (
                closeability_net_pair_cost is None
                or closeability_net_pair_cost > self.cfg.risk_seed_closeability_net_cap + 1e-12
            )
        ):
            self.block("risk_seed_closeability_net_cap")
            self.emit(
                {
                    "kind": "risk_seed_closeability_block",
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "ts_ms": ts_ms,
                    "side": side,
                    "price": seed_px,
                    "public_trade_px": px,
                    "public_trade_size": size,
                    "source_sequence_id": trigger_source_sequence_id,
                    "market_md_source_sequence_id": trigger_source_sequence_id,
                    "block_reason": "risk_seed_closeability_net_cap",
                    "risk_increasing_seed": True,
                    "same_exposure_qty": same_qty,
                    "opp_exposure_qty": opp_qty,
                    "risk_seed_pending_opp_credit": self.cfg.risk_seed_pending_opp_credit,
                    "risk_seed_pending_opp_qty": pending_opp_qty,
                    "risk_seed_credited_opp_qty": credited_opp_qty,
                    "closeability_comp_ask": closeability_comp_ask,
                    "closeability_net_pair_cost": closeability_net_pair_cost,
                    "risk_seed_closeability_net_cap": self.cfg.risk_seed_closeability_net_cap,
                    "strict_rescue_close_ask_slip": self.cfg.strict_rescue_close_ask_slip,
                    **closeability_audit,
                    **fair_price_audit,
                }
            )
            self.record_activation_seen(side, ts_ms)
            return
        if same_qty >= target_qty - self.cfg.dust_qty:
            self.block("target")
            self.record_activation_seen(side, ts_ms)
            return
        if max(0.0, self.exposure_cost(side) - self.exposure_cost(opp(side))) > self.cfg.imbalance_cost_cap + 1e-12:
            self.block("imbalance_cost")
            self.record_activation_seen(side, ts_ms)
            return
        imbalance_room = self.cfg.imbalance_qty_cap - max(0.0, same_qty - credited_opp_qty)
        if high_price_close_only_active:
            imbalance_room = min(imbalance_room, max(0.0, credited_opp_qty - same_qty))
        if imbalance_room <= self.cfg.dust_qty:
            self.block("imbalance_qty")
            self.record_activation_seen(side, ts_ms)
            return

        room_cost = self.cfg.max_open_cost - self.total_open_cost()
        qty = min(self.cfg.max_seed_qty, size * self.cfg.fill_haircut, target_qty - same_qty, room_cost / max(seed_px, 1e-9), imbalance_room)
        if qty <= self.cfg.dust_qty:
            self.block("qty_zero")
            self.record_activation_seen(side, ts_ms)
            return
        qty, surplus_audit = self.apply_surplus_budget(side, qty, seed_px, ts_ms, trigger_source_sequence_id)
        if qty is None:
            self.record_activation_seen(side, ts_ms)
            return
        pair_completion_audit = self.pair_completion_audit(side, seed_px, qty)
        if (
            self.cfg.pair_completion_net_cap is not None
            and pair_completion_audit["pair_completion_qty"] > self.cfg.dust_qty
            and (
                pair_completion_audit["pair_completion_worst_net_pair_cost"] is None
                or pair_completion_audit["pair_completion_worst_net_pair_cost"]
                > self.cfg.pair_completion_net_cap + 1e-12
            )
        ):
            self.block("pair_completion_net_cap")
            pair_completion_audit["pair_completion_decision"] = "block_net_cap"
            self.emit(
                {
                    "kind": "pair_completion_block",
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "ts_ms": ts_ms,
                    "side": side,
                    "price": seed_px,
                    "qty": qty,
                    "public_trade_px": px,
                    "public_trade_size": size,
                    "source_sequence_id": trigger_source_sequence_id,
                    "market_md_source_sequence_id": trigger_source_sequence_id,
                    "block_reason": "pair_completion_net_cap",
                    "same_exposure_qty": same_qty,
                    "opp_exposure_qty": opp_qty,
                    "risk_seed_pending_opp_credit": self.cfg.risk_seed_pending_opp_credit,
                    "risk_seed_pending_opp_qty": pending_opp_qty,
                    "risk_seed_credited_opp_qty": credited_opp_qty,
                    **pair_completion_audit,
                    **closeability_audit,
                    **fair_price_audit,
                    **source_quality_audit,
                    **surplus_audit,
                }
            )
            self.record_activation_seen(side, ts_ms)
            return
        if (
            self.cfg.pair_completion_min_pair_pnl_after is not None
            and pair_completion_audit["pair_completion_qty"] > self.cfg.dust_qty
            and pair_completion_audit["pair_completion_projected_pair_pnl_after"]
            < self.cfg.pair_completion_min_pair_pnl_after - 1e-12
        ):
            self.block("pair_completion_pair_pnl_floor")
            pair_completion_audit["pair_completion_decision"] = "block_pair_pnl_floor"
            self.emit(
                {
                    "kind": "pair_completion_block",
                    "slug": self.slug,
                    "condition_id": self.condition_id,
                    "ts_ms": ts_ms,
                    "side": side,
                    "price": seed_px,
                    "qty": qty,
                    "public_trade_px": px,
                    "public_trade_size": size,
                    "source_sequence_id": trigger_source_sequence_id,
                    "market_md_source_sequence_id": trigger_source_sequence_id,
                    "block_reason": "pair_completion_pair_pnl_floor",
                    "same_exposure_qty": same_qty,
                    "opp_exposure_qty": opp_qty,
                    "risk_seed_pending_opp_credit": self.cfg.risk_seed_pending_opp_credit,
                    "risk_seed_pending_opp_qty": pending_opp_qty,
                    "risk_seed_credited_opp_qty": credited_opp_qty,
                    **pair_completion_audit,
                    **closeability_audit,
                    **fair_price_audit,
                    **source_quality_audit,
                    **surplus_audit,
                }
            )
            self.record_activation_seen(side, ts_ms)
            return
        if risk_increasing_seed and self.cfg.risk_seed_closeability_soft_net_cap is not None:
            closeability_audit["risk_seed_closeability_soft_decision"] = "allow"
            closeability_audit["closeability_debt_pre_open"] = round(self.closeability_debt_open, 12)
            if (
                closeability_net_pair_cost is None
                or closeability_net_pair_cost > self.cfg.risk_seed_closeability_soft_net_cap + 1e-12
            ):
                self.block("risk_seed_closeability_soft_net_cap")
                closeability_audit["risk_seed_closeability_soft_decision"] = "block_soft_cap"
                self.emit(
                    {
                        "kind": "risk_seed_closeability_block",
                        "slug": self.slug,
                        "condition_id": self.condition_id,
                        "ts_ms": ts_ms,
                        "side": side,
                        "price": seed_px,
                        "qty": qty,
                        "public_trade_px": px,
                        "public_trade_size": size,
                        "source_sequence_id": trigger_source_sequence_id,
                        "market_md_source_sequence_id": trigger_source_sequence_id,
                        "block_reason": "risk_seed_closeability_soft_net_cap",
                        "risk_increasing_seed": True,
                        "same_exposure_qty": same_qty,
                        "opp_exposure_qty": opp_qty,
                        "risk_seed_pending_opp_credit": self.cfg.risk_seed_pending_opp_credit,
                        "risk_seed_pending_opp_qty": pending_opp_qty,
                        "risk_seed_credited_opp_qty": credited_opp_qty,
                        "closeability_comp_ask": closeability_comp_ask,
                        "closeability_net_pair_cost": closeability_net_pair_cost,
                        "risk_seed_closeability_net_cap": self.cfg.risk_seed_closeability_net_cap,
                        "strict_rescue_close_ask_slip": self.cfg.strict_rescue_close_ask_slip,
                        **closeability_audit,
                        **fair_price_audit,
                        **surplus_audit,
                    }
                )
                self.record_activation_seen(side, ts_ms)
                return
            debt_per_share = max(0.0, closeability_net_pair_cost - closeability_debt_floor)
            closeability_debt = debt_per_share * qty
            closeability_audit["closeability_debt_per_share"] = round(debt_per_share, 12)
            closeability_audit["closeability_debt"] = round(closeability_debt, 12)
            projected_debt = self.closeability_debt_open + closeability_debt
            closeability_audit["closeability_debt_post_open"] = round(projected_debt, 12)
            if projected_debt > self.cfg.risk_seed_closeability_debt_budget + 1e-12:
                self.block("risk_seed_closeability_debt_budget")
                closeability_audit["risk_seed_closeability_soft_decision"] = "block_debt_budget"
                self.emit(
                    {
                        "kind": "risk_seed_closeability_block",
                        "slug": self.slug,
                        "condition_id": self.condition_id,
                        "ts_ms": ts_ms,
                        "side": side,
                        "price": seed_px,
                        "qty": qty,
                        "public_trade_px": px,
                        "public_trade_size": size,
                        "source_sequence_id": trigger_source_sequence_id,
                        "market_md_source_sequence_id": trigger_source_sequence_id,
                        "block_reason": "risk_seed_closeability_debt_budget",
                        "risk_increasing_seed": True,
                        "same_exposure_qty": same_qty,
                        "opp_exposure_qty": opp_qty,
                        "risk_seed_pending_opp_credit": self.cfg.risk_seed_pending_opp_credit,
                        "risk_seed_pending_opp_qty": pending_opp_qty,
                        "risk_seed_credited_opp_qty": credited_opp_qty,
                        "closeability_comp_ask": closeability_comp_ask,
                        "closeability_net_pair_cost": closeability_net_pair_cost,
                        "risk_seed_closeability_net_cap": self.cfg.risk_seed_closeability_net_cap,
                        "strict_rescue_close_ask_slip": self.cfg.strict_rescue_close_ask_slip,
                        **closeability_audit,
                        **fair_price_audit,
                        **surplus_audit,
                    }
                )
                self.record_activation_seen(side, ts_ms)
                return
            self.adjust_closeability_debt(closeability_debt)
            closeability_audit["closeability_debt_post_open"] = round(self.closeability_debt_open, 12)

        quote_intent_id = self.quote_intent_id(self.next_order_id)
        opposite_trigger_ts_ms = (
            ts_ms - activation_opp_age_ms if activation_opp_age_ms is not None else opposite_seen_ms
        )
        order = VirtualOrder(id=self.next_order_id, quote_intent_id=quote_intent_id, condition_id=self.condition_id, side=side, px=seed_px, qty=qty, created_ms=ts_ms, accepted_ms=ts_ms, offset_s=float(offset), trigger_px=px, trigger_size=size, trigger_ts_ms=ts_ms, trigger_source_sequence_id=trigger_source_sequence_id, opposite_trigger_ts_ms=opposite_trigger_ts_ms, closeability_comp_ask=closeability_comp_ask, closeability_net_pair_cost=closeability_net_pair_cost, risk_seed_closeability_soft_net_cap=self.cfg.risk_seed_closeability_soft_net_cap, risk_seed_closeability_debt_floor=closeability_audit["risk_seed_closeability_debt_floor"], risk_seed_closeability_debt_budget=self.cfg.risk_seed_closeability_debt_budget, closeability_debt_per_share=closeability_audit["closeability_debt_per_share"], closeability_debt=closeability_audit["closeability_debt"])
        self.next_order_id += 1
        self.pending.append(order)
        self.last_seed_ms = ts_ms
        self.metrics.candidates += 1
        self.metrics.seed_qty += qty
        self.metrics.seed_cost += qty * seed_px
        self.emit({"kind": "candidate", "slug": self.slug, "condition_id": self.condition_id, "ts_ms": ts_ms, "placed_ts_ms": ts_ms, "accepted_ts_ms": ts_ms, "order_id": order.id, "quote_intent_id": order.quote_intent_id, "side": side, "price": seed_px, "size": qty, "offset_s": offset, "public_trade_px": px, "public_trade_size": size, "trigger_ts_ms": ts_ms, "trigger_event_time_ms": trigger_event_time_ms, "source": "no_order_public_trade_candidate", "source_sequence_id": trigger_source_sequence_id, "market_md_source_sequence_id": trigger_source_sequence_id, "opposite_trigger_ts_ms": opposite_trigger_ts_ms, "seed_px": seed_px, "qty": qty, "edge": self.cfg.edge, "queue_share": self.cfg.queue_share, "l1_pair_ask": l1_pair, "same_exposure_qty": same_qty, "opp_exposure_qty": opp_qty, "risk_seed_pending_opp_credit": self.cfg.risk_seed_pending_opp_credit, "risk_seed_pending_opp_qty": pending_opp_qty, "risk_seed_credited_opp_qty": credited_opp_qty, "target_qty": target_qty, "base_target_qty": self.cfg.target_qty, "late_target_active": self.cfg.late_target_active(offset), "late_repair_active": self.cfg.late_repair_active(offset), "high_price_mode": self.cfg.high_price_mode, "high_price_normal_px_hi": self.cfg.high_price_normal_px_hi, "high_price_close_only_active": high_price_close_only_active, "activation_mode": self.cfg.activation_mode, "activation_window_s": self.cfg.activation_window_s, "activation_required": risk_increasing_seed and self.cfg.activation_mode != "none", "risk_increasing_seed": risk_increasing_seed, "activation_opp_age_ms": activation_opp_age_ms, "risk_seed_closeability_net_cap": self.cfg.risk_seed_closeability_net_cap, "closeability_comp_ask": closeability_comp_ask, "closeability_net_pair_cost": closeability_net_pair_cost, "open_cost": self.total_open_cost(), "yes_bid": self.book.get("yes_bid"), "yes_ask": yes_ask, "no_bid": self.book.get("no_bid"), "no_ask": no_ask, **pair_completion_audit, **closeability_audit, **fair_price_audit, **source_quality_audit, **surplus_audit})
        self.record_activation_seen(side, ts_ms)

    def read_events(self) -> list[dict[str, Any]]:
        if not self.events_path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line in self.events_path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return events

    def write_normalized_lifecycle_exports(self, residual_lots: list[Lot]) -> None:
        events = self.read_events()
        action_rows: list[dict[str, Any]] = []
        order_rows: list[dict[str, Any]] = []
        fill_rows: list[dict[str, Any]] = []
        inventory_rows: list[dict[str, Any]] = []
        rescue_rows: list[dict[str, Any]] = []
        for idx, event in enumerate(events, start=1):
            kind = event.get("kind")
            common = {
                "event_id": idx,
                "kind": kind,
                "slug": event.get("slug"),
                "condition_id": event.get("condition_id"),
                "ts_ms": event.get("ts_ms"),
                "quote_intent_id": event.get("quote_intent_id"),
                "side": event.get("side"),
                "source_sequence_id": event.get("source_sequence_id"),
                "market_md_source_sequence_id": event.get("market_md_source_sequence_id"),
            }
            if kind == "candidate":
                action_rows.append({
                    **common,
                    "decision": "accept",
                    "block_reason": "",
                    "price": event.get("price"),
                    "qty": event.get("qty") or event.get("size"),
                    "source_quality_decision": event.get("source_quality_decision"),
                    "source_quality_l1_age_ms": event.get("source_quality_l1_age_ms"),
                    "source_quality_l2_source_sequence_id": event.get("source_quality_l2_source_sequence_id"),
                    "surplus_budget_decision": event.get("surplus_budget_decision"),
                    "surplus_budget_projected_unpaired_cost": event.get("surplus_budget_projected_unpaired_cost"),
                    "risk_seed_closeability_net_cap": event.get("risk_seed_closeability_net_cap"),
                    "risk_seed_closeability_soft_net_cap": event.get("risk_seed_closeability_soft_net_cap"),
                    "risk_seed_closeability_debt_floor": event.get("risk_seed_closeability_debt_floor"),
                    "risk_seed_closeability_debt_budget": event.get("risk_seed_closeability_debt_budget"),
                    "risk_seed_closeability_soft_decision": event.get("risk_seed_closeability_soft_decision"),
                    "risk_seed_pending_opp_credit": event.get("risk_seed_pending_opp_credit"),
                    "risk_seed_pending_opp_qty": event.get("risk_seed_pending_opp_qty"),
                    "risk_seed_credited_opp_qty": event.get("risk_seed_credited_opp_qty"),
                    "pair_completion_net_cap": event.get("pair_completion_net_cap"),
                    "pair_completion_min_pair_pnl_after": event.get("pair_completion_min_pair_pnl_after"),
                    "pair_completion_qty": event.get("pair_completion_qty"),
                    "pair_completion_avg_net_pair_cost": event.get("pair_completion_avg_net_pair_cost"),
                    "pair_completion_worst_net_pair_cost": event.get("pair_completion_worst_net_pair_cost"),
                    "pair_completion_pair_pnl_delta": event.get("pair_completion_pair_pnl_delta"),
                    "pair_completion_projected_pair_pnl_after": event.get("pair_completion_projected_pair_pnl_after"),
                    "pair_completion_decision": event.get("pair_completion_decision"),
                    "closeability_net_pair_cost": event.get("closeability_net_pair_cost"),
                    "closeability_debt_per_share": event.get("closeability_debt_per_share"),
                    "closeability_debt": event.get("closeability_debt"),
                    "closeability_debt_pre_open": event.get("closeability_debt_pre_open"),
                    "closeability_debt_post_open": event.get("closeability_debt_post_open"),
                    "fair_price_admission_decision": event.get("fair_price_admission_decision"),
                    "fair_price_admission_block_reason": event.get("fair_price_admission_block_reason"),
                    "fair_price_side_probability": event.get("fair_price_side_probability"),
                    "fair_price_edge_after_fee": event.get("fair_price_edge_after_fee"),
                    "fair_price_pair_cost_after_fee": event.get("fair_price_pair_cost_after_fee"),
                    "fair_price_seconds_to_close": event.get("fair_price_seconds_to_close"),
                    "fair_price_row_id": event.get("fair_price_row_id"),
                })
                order_rows.append({
                    **common,
                    "order_event_type": "virtual_order_accepted",
                    "order_id": event.get("order_id"),
                    "price": event.get("price"),
                    "qty": event.get("qty") or event.get("size"),
                    "placed_ts_ms": event.get("placed_ts_ms"),
                    "accepted_ts_ms": event.get("accepted_ts_ms"),
                    "cancel_reason": "",
                    "closeability_debt": event.get("closeability_debt"),
                    "closeability_debt_per_share": event.get("closeability_debt_per_share"),
                })
            elif kind in {"source_quality_block", "activation_block", "surplus_budget_block", "risk_seed_closeability_block", "pair_completion_block", "fair_price_admission_block"}:
                reason = (
                    event.get("source_quality_block_reason")
                    or event.get("block_reason")
                    or event.get("fair_price_admission_block_reason")
                    or event.get("surplus_budget_decision")
                    or ("risk_seed_closeability_net_cap" if kind == "risk_seed_closeability_block" else kind)
                )
                action_rows.append({
                    **common,
                    "decision": "block",
                    "block_reason": reason,
                    "price": event.get("price") or event.get("seed_px"),
                    "qty": event.get("qty") or event.get("size"),
                    "source_quality_decision": event.get("source_quality_decision"),
                    "source_quality_l1_age_ms": event.get("source_quality_l1_age_ms"),
                    "source_quality_l2_source_sequence_id": event.get("source_quality_l2_source_sequence_id"),
                    "surplus_budget_decision": event.get("surplus_budget_decision"),
                    "surplus_budget_projected_unpaired_cost": event.get("surplus_budget_projected_unpaired_cost"),
                    "risk_seed_closeability_net_cap": event.get("risk_seed_closeability_net_cap"),
                    "risk_seed_closeability_soft_net_cap": event.get("risk_seed_closeability_soft_net_cap"),
                    "risk_seed_closeability_debt_floor": event.get("risk_seed_closeability_debt_floor"),
                    "risk_seed_closeability_debt_budget": event.get("risk_seed_closeability_debt_budget"),
                    "risk_seed_closeability_soft_decision": event.get("risk_seed_closeability_soft_decision"),
                    "risk_seed_pending_opp_credit": event.get("risk_seed_pending_opp_credit"),
                    "risk_seed_pending_opp_qty": event.get("risk_seed_pending_opp_qty"),
                    "risk_seed_credited_opp_qty": event.get("risk_seed_credited_opp_qty"),
                    "pair_completion_net_cap": event.get("pair_completion_net_cap"),
                    "pair_completion_min_pair_pnl_after": event.get("pair_completion_min_pair_pnl_after"),
                    "pair_completion_qty": event.get("pair_completion_qty"),
                    "pair_completion_avg_net_pair_cost": event.get("pair_completion_avg_net_pair_cost"),
                    "pair_completion_worst_net_pair_cost": event.get("pair_completion_worst_net_pair_cost"),
                    "pair_completion_pair_pnl_delta": event.get("pair_completion_pair_pnl_delta"),
                    "pair_completion_projected_pair_pnl_after": event.get("pair_completion_projected_pair_pnl_after"),
                    "pair_completion_decision": event.get("pair_completion_decision"),
                    "closeability_net_pair_cost": event.get("closeability_net_pair_cost"),
                    "closeability_debt_per_share": event.get("closeability_debt_per_share"),
                    "closeability_debt": event.get("closeability_debt"),
                    "closeability_debt_pre_open": event.get("closeability_debt_pre_open"),
                    "closeability_debt_post_open": event.get("closeability_debt_post_open"),
                    "fair_price_admission_decision": event.get("fair_price_admission_decision"),
                    "fair_price_admission_block_reason": event.get("fair_price_admission_block_reason"),
                    "fair_price_side_probability": event.get("fair_price_side_probability"),
                    "fair_price_edge_after_fee": event.get("fair_price_edge_after_fee"),
                    "fair_price_pair_cost_after_fee": event.get("fair_price_pair_cost_after_fee"),
                    "fair_price_seconds_to_close": event.get("fair_price_seconds_to_close"),
                    "fair_price_row_id": event.get("fair_price_row_id"),
                })
            elif kind == "cancel":
                order_rows.append({
                    **common,
                    "order_event_type": "virtual_order_cancelled",
                    "order_id": event.get("order_id"),
                    "price": event.get("price"),
                    "qty": event.get("qty") or event.get("size"),
                    "placed_ts_ms": event.get("placed_ts_ms"),
                    "accepted_ts_ms": event.get("accepted_ts_ms"),
                    "cancel_reason": event.get("reason"),
                    "closeability_debt": event.get("closeability_debt"),
                    "closeability_debt_per_share": event.get("closeability_debt_per_share"),
                })
            elif kind == "queue_supported_fill":
                fill_rows.append({
                    **common,
                    "fill_event_type": "would_queue_supported_fill",
                    "fill_id": event.get("lot_id"),
                    "order_id": event.get("order_id"),
                    "price": event.get("price"),
                    "qty": event.get("qty") or event.get("size"),
                    "fee": 0.0,
                    "fill_ts_ms": event.get("fill_ts_ms"),
                    "source_lot_id": "",
                    "closeability_debt": event.get("closeability_debt"),
                    "closeability_debt_per_share": event.get("closeability_debt_per_share"),
                })
                inventory_rows.append({
                    **common,
                    "inventory_event_type": "would_fill_add",
                    "lot_id": event.get("lot_id"),
                    "side": event.get("side"),
                    "qty_delta": event.get("qty") or event.get("size"),
                    "cost_delta": (event.get("qty") or event.get("size") or 0.0) * (event.get("price") or 0.0),
                    "source_lot_id": "",
                    "matched_pair_id": "",
                })
            elif kind == "internal_pair":
                qty = event.get("qty") or 0.0
                inventory_rows.append({
                    **common,
                    "inventory_event_type": "internal_pair_redeem",
                    "lot_id": "",
                    "side": "YES+NO",
                    "qty_delta": -qty,
                    "cost_delta": "",
                    "source_lot_id": "",
                    "matched_pair_id": event.get("matched_pair_id"),
                })
            elif kind == "fak_salvage":
                fill_rows.append({
                    **common,
                    "fill_event_type": "would_strict_rescue_close",
                    "fill_id": event.get("matched_pair_id"),
                    "order_id": "",
                    "price": event.get("comp_ask"),
                    "qty": event.get("qty"),
                    "fee": (event.get("qty") or 0.0) * (event.get("fee_per_share") or 0.0),
                    "fill_ts_ms": event.get("ts_ms"),
                    "source_lot_id": event.get("source_lot_id"),
                })
                inventory_rows.append({
                    **common,
                    "inventory_event_type": "strict_rescue_redeem",
                    "lot_id": "",
                    "side": event.get("held_side"),
                    "qty_delta": -(event.get("qty") or 0.0),
                    "cost_delta": -(event.get("qty") or 0.0) * (event.get("held_px") or 0.0),
                    "source_lot_id": event.get("source_lot_id"),
                    "matched_pair_id": event.get("matched_pair_id"),
                })
                rescue_rows.append({
                    **common,
                    "matched_pair_id": event.get("matched_pair_id"),
                    "held_side": event.get("held_side"),
                    "close_side": event.get("comp_side"),
                    "held_px": event.get("held_px"),
                    "close_ask": event.get("comp_ask"),
                    "qty": event.get("qty"),
                    "fee_per_share": event.get("fee_per_share"),
                    "net_pair_cost": event.get("net_pair_cost"),
                    "age_ms": event.get("age_ms"),
                    "strict_rescue_l1_age_ms": event.get("strict_rescue_l1_age_ms"),
                    "strict_rescue_l2_source_sequence_id": event.get("strict_rescue_l2_source_sequence_id"),
                    "strict_rescue_l2_age_ms": event.get("strict_rescue_l2_age_ms"),
                    "strict_rescue_surplus_net_cap": event.get("strict_rescue_surplus_net_cap"),
                    "strict_rescue_min_pair_pnl_after": event.get("strict_rescue_min_pair_pnl_after"),
                    "strict_rescue_pair_pnl_delta": event.get("strict_rescue_pair_pnl_delta"),
                    "strict_rescue_projected_pair_pnl_after": event.get("strict_rescue_projected_pair_pnl_after"),
                    "strict_rescue_surplus_decision": event.get("strict_rescue_surplus_decision"),
                    "source_lot_id": event.get("source_lot_id"),
                    "source_lot_sequence_id": event.get("source_lot_sequence_id"),
                })

        residual_rows = [
            {
                "residual_lot_id": lot.id,
                "quote_intent_id": lot.quote_intent_id,
                "slug": self.slug,
                "condition_id": self.condition_id,
                "side": lot.side,
                "qty": round(lot.qty, 12),
                "px": lot.px,
                "cost": round(lot.cost, 12),
                "fill_ms": lot.fill_ms,
                "source_order_id": lot.source_order_id,
                "source_sequence_id": lot.source_sequence_id,
                "source_event_time_ms": lot.source_event_time_ms,
                "closeability_debt_per_share": lot.closeability_debt_per_share,
                "closeability_debt": round(lot.qty * lot.closeability_debt_per_share, 12),
            }
            for lot in residual_lots
            if lot.qty > DUST
        ]
        write_csv(
            self.out_dir / f"{self.slug}.would_action_decisions.csv",
            [
                "event_id", "kind", "slug", "condition_id", "ts_ms", "quote_intent_id", "side",
                "decision", "block_reason", "price", "qty", "source_sequence_id",
                "market_md_source_sequence_id", "source_quality_decision",
                "source_quality_l1_age_ms", "source_quality_l2_source_sequence_id",
                "surplus_budget_decision", "surplus_budget_projected_unpaired_cost",
                "risk_seed_closeability_net_cap", "risk_seed_closeability_soft_net_cap",
                "risk_seed_closeability_debt_floor", "risk_seed_closeability_debt_budget",
                "risk_seed_closeability_soft_decision", "risk_seed_pending_opp_credit",
                "risk_seed_pending_opp_qty", "risk_seed_credited_opp_qty",
                "pair_completion_net_cap", "pair_completion_min_pair_pnl_after",
                "pair_completion_qty", "pair_completion_avg_net_pair_cost",
                "pair_completion_worst_net_pair_cost", "pair_completion_pair_pnl_delta",
                "pair_completion_projected_pair_pnl_after", "pair_completion_decision",
                "closeability_net_pair_cost",
                "closeability_debt_per_share", "closeability_debt", "closeability_debt_pre_open",
                "closeability_debt_post_open", "fair_price_admission_decision",
                "fair_price_admission_block_reason", "fair_price_side_probability",
                "fair_price_edge_after_fee", "fair_price_pair_cost_after_fee",
                "fair_price_seconds_to_close", "fair_price_row_id",
            ],
            action_rows,
        )
        write_csv(
            self.out_dir / f"{self.slug}.would_order_events.csv",
            [
                "event_id", "kind", "slug", "condition_id", "ts_ms", "quote_intent_id", "side",
                "order_event_type", "order_id", "price", "qty", "placed_ts_ms", "accepted_ts_ms",
                "cancel_reason", "source_sequence_id", "market_md_source_sequence_id",
                "closeability_debt_per_share", "closeability_debt",
            ],
            order_rows,
        )
        write_csv(
            self.out_dir / f"{self.slug}.would_fill_events.csv",
            [
                "event_id", "kind", "slug", "condition_id", "ts_ms", "quote_intent_id", "side",
                "fill_event_type", "fill_id", "order_id", "price", "qty", "fee", "fill_ts_ms",
                "source_lot_id", "source_sequence_id", "market_md_source_sequence_id",
                "closeability_debt_per_share", "closeability_debt",
            ],
            fill_rows,
        )
        write_csv(
            self.out_dir / f"{self.slug}.simulated_inventory_events.csv",
            [
                "event_id", "kind", "slug", "condition_id", "ts_ms", "quote_intent_id", "side",
                "inventory_event_type", "lot_id", "qty_delta", "cost_delta", "source_lot_id",
                "matched_pair_id", "source_sequence_id", "market_md_source_sequence_id",
            ],
            inventory_rows,
        )
        write_csv(
            self.out_dir / f"{self.slug}.residual_fifo_lots.csv",
            [
                "residual_lot_id", "quote_intent_id", "slug", "condition_id", "side", "qty",
                "px", "cost", "fill_ms", "source_order_id", "source_sequence_id",
                "source_event_time_ms", "closeability_debt_per_share", "closeability_debt",
            ],
            residual_rows,
        )
        write_csv(
            self.out_dir / f"{self.slug}.strict_rescue_closes.csv",
            [
                "event_id", "kind", "slug", "condition_id", "ts_ms", "quote_intent_id",
                "matched_pair_id", "held_side", "close_side", "held_px", "close_ask", "qty",
                "fee_per_share", "net_pair_cost", "age_ms", "strict_rescue_l1_age_ms",
                "strict_rescue_l2_source_sequence_id", "strict_rescue_l2_age_ms",
                "strict_rescue_surplus_net_cap", "strict_rescue_min_pair_pnl_after",
                "strict_rescue_pair_pnl_delta", "strict_rescue_projected_pair_pnl_after",
                "strict_rescue_surplus_decision",
                "source_lot_id", "source_lot_sequence_id", "source_sequence_id",
                "market_md_source_sequence_id",
            ],
            rescue_rows,
        )
        manifest = {
            "kind": "normalized_no_order_lifecycle_manifest",
            "slug": self.slug,
            "condition_id": self.condition_id,
            "orders_sent": False,
            "files": {
                "would_action_decisions": f"{self.slug}.would_action_decisions.csv",
                "would_order_events": f"{self.slug}.would_order_events.csv",
                "would_fill_events": f"{self.slug}.would_fill_events.csv",
                "simulated_inventory_events": f"{self.slug}.simulated_inventory_events.csv",
                "residual_fifo_lots": f"{self.slug}.residual_fifo_lots.csv",
                "strict_rescue_closes": f"{self.slug}.strict_rescue_closes.csv",
            },
            "row_counts": {
                "would_action_decisions": len(action_rows),
                "would_order_events": len(order_rows),
                "would_fill_events": len(fill_rows),
                "simulated_inventory_events": len(inventory_rows),
                "residual_fifo_lots": len(residual_rows),
                "strict_rescue_closes": len(rescue_rows),
            },
        }
        (self.out_dir / f"{self.slug}.normalized_lifecycle_manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )

    def write_summary(self, final: bool = False) -> None:
        if final:
            ts_ms = now_ms()
            for order in self.pending_orders():
                self.adjust_closeability_debt(-order.closeability_debt)
                order.cancel_ms = ts_ms
                order.cancel_reason = "final"
                self.metrics.cancelled_orders += 1
                if order.first_bid_touch_ms or order.first_trade_touch_ms:
                    self.metrics.touch_only_orders += 1
        residual_lots = [lot for side in ("YES", "NO") for lot in self.lots[side] if lot.qty > DUST]
        residual_qty = sum(lot.qty for lot in residual_lots)
        residual_cost = sum(lot.cost for lot in residual_lots)
        material = [lot for lot in residual_lots if lot.qty > 6 or lot.cost > 6]
        self.metrics.residual_qty = residual_qty
        self.metrics.residual_cost = residual_cost
        m = self.metrics
        summary = {
            "kind": "summary",
            "script": "xuan_dplus_passive_passive_shadow_runner.py",
            "slug": self.slug,
            "final": final,
            "ts_ms": now_ms(),
            "book_ticks": self.book_ticks,
            "trade_ticks": self.trade_ticks,
            "sell_triggers": self.sell_triggers,
            "source_linkage": self.source_linkage,
            "blocked": self.blocked,
            "config": self.cfg.__dict__,
            "metrics": {
                "candidates": m.candidates,
                "queue_supported_fills": m.queue_supported_fills,
                "fill_rate": m.queue_supported_fills / m.candidates if m.candidates else 0.0,
                "touch_only_orders": m.touch_only_orders,
                "cancelled_orders": m.cancelled_orders,
                "pair_actions": m.pair_actions,
                "salvage_actions": m.salvage_actions,
                "strict_rescue_actions": m.strict_rescue_actions,
                "completion_per_fill": m.pair_actions / m.queue_supported_fills if m.queue_supported_fills else 0.0,
                "seed_qty": round(m.seed_qty, 6),
                "seed_cost": round(m.seed_cost, 6),
                "filled_qty": round(m.filled_qty, 6),
                "filled_cost": round(m.filled_cost, 6),
                "pair_qty": round(m.pair_qty, 6),
                "qty_pair_share_of_filled": (2 * m.pair_qty / m.filled_qty) if m.filled_qty else 0.0,
                "pair_pnl": round(m.pair_pnl, 6),
                "taker_fee": round(m.taker_fee, 6),
                "completion_cost": round(m.completion_cost, 6),
                "strict_rescue_qty": round(m.strict_rescue_qty, 6),
                "surplus_budget_blocks": m.surplus_budget_blocks,
                "surplus_budget_capped_qty": round(m.surplus_budget_capped_qty, 6),
                "strict_rescue_source_blocks": m.strict_rescue_source_blocks,
                "surplus_bank": round(self.surplus_bank, 6),
                "closeability_debt_open": round(self.closeability_debt_open, 6),
                "closeability_debt_reserved": round(m.closeability_debt_reserved, 6),
                "closeability_debt_released": round(m.closeability_debt_released, 6),
                "closeability_debt_max_open": round(m.closeability_debt_max_open, 6),
                "roi_on_seed_cost": m.pair_pnl / m.seed_cost if m.seed_cost else 0.0,
                "roi_on_filled_cost": m.pair_pnl / m.filled_cost if m.filled_cost else 0.0,
                "residual_qty": round(residual_qty, 6),
                "residual_cost": round(residual_cost, 6),
                "material_residual_lots": len(material),
                "pair_cost_p50": pct(m.pair_costs, 0.5),
                "pair_cost_p90": pct(m.pair_costs, 0.9),
                "net_pair_cost_p50": pct(m.net_pair_costs, 0.5),
                "net_pair_cost_p90": pct(m.net_pair_costs, 0.9),
                "fill_wait_p50_ms": pct(m.fill_wait_ms, 0.5),
                "fill_wait_p90_ms": pct(m.fill_wait_ms, 0.9),
                "pair_wait_p50_ms": pct(m.pair_wait_ms, 0.5),
                "pair_wait_p90_ms": pct(m.pair_wait_ms, 0.9),
                "salvage_wait_p50_ms": pct(m.salvage_wait_ms, 0.5),
                "salvage_wait_p90_ms": pct(m.salvage_wait_ms, 0.9),
            },
            "top_residual_lots": [
                {"lot_id": lot.id, "quote_intent_id": lot.quote_intent_id, "side": lot.side, "qty": round(lot.qty, 6), "cost": round(lot.cost, 6), "px": lot.px, "age_ms": now_ms() - lot.fill_ms, "source_order_id": lot.source_order_id}
                for lot in sorted(residual_lots, key=lambda x: x.cost, reverse=True)[:10]
            ],
        }
        self.summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        if final and self.cfg.write_normalized_lifecycle:
            self.write_normalized_lifecycle_exports(residual_lots)


async def run_market(
    market: dict[str, str],
    socket_path: str,
    out: Path,
    duration_s: int,
    cfg: RunnerConfig,
    fair_price_gate: FairPriceAdmissionGate | None = None,
) -> None:
    slug = market["POLYMARKET_MARKET_SLUG"]
    runner = DPlusRunner(
        slug,
        out,
        cfg,
        condition_id=market.get("POLYMARKET_MARKET_ID"),
        fair_price_gate=fair_price_gate,
    )
    reader, writer = await asyncio.open_unix_connection(socket_path)
    req = {
        "stream": "market",
        "symbols": [],
        "market_slug": slug,
        "market_id": market["POLYMARKET_MARKET_ID"],
        "yes_asset_id": market["POLYMARKET_YES_ASSET_ID"],
        "no_asset_id": market["POLYMARKET_NO_ASSET_ID"],
        "ws_base_url": "wss://ws-subscriptions-clob.polymarket.com/ws",
        "custom_feature_enabled": False,
    }
    writer.write((json.dumps(req, separators=(",", ":")) + "\n").encode())
    await writer.drain()
    deadline = time.monotonic() + duration_s
    last_summary = 0.0
    try:
        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(reader.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                raw = b""
            if raw:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                kind = msg.get("kind")
                if kind == "market_book_tick":
                    runner.on_book(msg)
                elif kind == "market_trade_tick":
                    runner.on_trade(msg)
            if time.monotonic() - last_summary >= 30:
                runner.write_summary(False)
                last_summary = time.monotonic()
    finally:
        runner.write_summary(True)
        writer.close()
        await writer.wait_closed()


def resolve_market_slug(repo: Path, slug: str) -> dict[str, str]:
    proc = subprocess.run(
        ["/usr/bin/python3", str(repo / "scripts/resolve_market_ids.py"), "--slug", slug, "--format", "env"],
        cwd=repo,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    env = parse_env_exports(proc.stdout)
    if not env:
        raise RuntimeError(f"empty market resolution env for slug={slug}")
    return env


def split_csv(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def resolve_markets(repo: Path, prefix: str, offsets: str, market_slugs: str | None = None) -> list[dict[str, str]]:
    exact_slugs = split_csv(market_slugs)
    if exact_slugs:
        return [resolve_market_slug(repo, slug) for slug in exact_slugs]

    markets: list[dict[str, str]] = []
    for raw_offset in offsets.split(","):
        offset = raw_offset.strip()
        if not offset:
            continue
        proc = subprocess.run(
            ["/usr/bin/python3", str(repo / "scripts/resolve_market_ids.py"), "--prefix", prefix, "--round-offset", offset, "--format", "env"],
            cwd=repo,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        env = parse_env_exports(proc.stdout)
        if env:
            markets.append(env)
    return markets


def aggregate(out: Path) -> dict[str, Any]:
    summaries = []
    for path in sorted(out.glob("*.summary.json")):
        try:
            summaries.append(json.loads(path.read_text()))
        except Exception:
            continue
    totals: dict[str, float] = {}
    blocked: dict[str, int] = {}
    pair_costs: list[float] = []
    net_pair_costs: list[float] = []
    fill_waits: list[float] = []
    pair_waits: list[float] = []
    top_residual: list[dict[str, Any]] = []
    source_linkage_totals: dict[str, Any] = {}
    for s in summaries:
        for k, v in s.get("blocked", {}).items():
            blocked[k] = blocked.get(k, 0) + int(v)
        m = s.get("metrics", {})
        for k, v in m.items():
            if isinstance(v, (int, float)) and not k.endswith(("_p50", "_p90", "_avg", "_rate")):
                totals[k] = totals.get(k, 0.0) + float(v)
        for k, dest in [("pair_cost_p50", pair_costs), ("pair_cost_p90", pair_costs), ("net_pair_cost_p50", net_pair_costs), ("net_pair_cost_p90", net_pair_costs), ("fill_wait_p50_ms", fill_waits), ("fill_wait_p90_ms", fill_waits), ("pair_wait_p50_ms", pair_waits), ("pair_wait_p90_ms", pair_waits)]:
            if m.get(k) is not None:
                dest.append(float(m[k]))
        for lot in s.get("top_residual_lots", []):
            lot["slug"] = s.get("slug")
            top_residual.append(lot)
        for k, v in s.get("source_linkage", {}).items():
            if isinstance(v, (int, float)):
                source_linkage_totals[k] = source_linkage_totals.get(k, 0) + v
            elif k.endswith("_first_source_sequence_id"):
                if source_linkage_totals.get(k) is None and v is not None:
                    source_linkage_totals[k] = v
            elif k.endswith("_last_source_sequence_id") and v is not None:
                source_linkage_totals[k] = v
            elif k.endswith("_time_min_ms") and v is not None:
                source_linkage_totals[k] = (
                    v if source_linkage_totals.get(k) is None else min(source_linkage_totals[k], v)
                )
            elif k.endswith("_time_max_ms") and v is not None:
                source_linkage_totals[k] = (
                    v if source_linkage_totals.get(k) is None else max(source_linkage_totals[k], v)
                )
    candidates = totals.get("candidates", 0.0)
    fills = totals.get("queue_supported_fills", 0.0)
    filled_qty = totals.get("filled_qty", 0.0)
    seed_cost = totals.get("seed_cost", 0.0)
    filled_cost = totals.get("filled_cost", 0.0)
    pair_qty = totals.get("pair_qty", 0.0)
    pair_pnl = totals.get("pair_pnl", 0.0)
    aggregate_report = {
        "kind": "aggregate_report",
        "script": "xuan_dplus_passive_passive_shadow_runner.py",
        "slugs": len(summaries),
        "blocked": blocked,
        "metrics": {
            **{k: round(v, 6) for k, v in totals.items()},
            "fill_rate": fills / candidates if candidates else 0.0,
            "completion_per_fill": totals.get("pair_actions", 0.0) / fills if fills else 0.0,
            "qty_pair_share_of_filled": (2 * pair_qty / filled_qty) if filled_qty else 0.0,
            "roi_on_seed_cost": pair_pnl / seed_cost if seed_cost else 0.0,
            "roi_on_filled_cost": pair_pnl / filled_cost if filled_cost else 0.0,
            "pair_cost_proxy_p50": pct(pair_costs, 0.5),
            "pair_cost_proxy_p90": pct(pair_costs, 0.9),
            "net_pair_cost_proxy_p50": pct(net_pair_costs, 0.5),
            "net_pair_cost_proxy_p90": pct(net_pair_costs, 0.9),
            "fill_wait_proxy_p50_ms": pct(fill_waits, 0.5),
            "fill_wait_proxy_p90_ms": pct(fill_waits, 0.9),
            "pair_wait_proxy_p50_ms": pct(pair_waits, 0.5),
            "pair_wait_proxy_p90_ms": pct(pair_waits, 0.9),
        },
        "source_linkage": source_linkage_totals,
        "top_residual_lots": sorted(top_residual, key=lambda x: x.get("cost", 0), reverse=True)[:20],
    }
    (out / "aggregate_report.json").write_text(json.dumps(aggregate_report, indent=2, sort_keys=True) + "\n")
    return aggregate_report


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="/srv/pm_as_ofi/repo")
    ap.add_argument("--shared-ingress-root", default="/srv/pm_as_ofi/shared-ingress-main")
    ap.add_argument("--prefix", default="btc-updown-5m")
    ap.add_argument("--round-offsets", default="0,1,2,3")
    ap.add_argument(
        "--market-slugs",
        default=None,
        help="Comma-separated exact market slugs to resolve. When set, this bypasses --prefix/--round-offsets generation.",
    )
    ap.add_argument("--duration-s", type=int, default=1800)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--edge", type=float, default=0.040)
    ap.add_argument("--queue-share", type=float, default=0.50)
    ap.add_argument("--target-qty", type=float, default=5.0)
    ap.add_argument("--fill-haircut", type=float, default=0.25)
    ap.add_argument("--max-seed-qty", type=float, default=60.0)
    ap.add_argument("--max-open-cost", type=float, default=80.0)
    ap.add_argument("--seed-l1-cap", type=float, default=1.02)
    ap.add_argument("--seed-px-lo", type=float, default=0.05)
    ap.add_argument("--seed-px-hi", type=float, default=0.90)
    ap.add_argument("--seed-offset-max-s", type=float, default=120.0)
    ap.add_argument("--late-target-after-s", type=float, default=None)
    ap.add_argument("--late-target-qty", type=float, default=None)
    ap.add_argument("--late-repair-after-s", type=float, default=None)
    ap.add_argument("--order-ttl-s", type=float, default=120.0)
    ap.add_argument("--imbalance-qty-cap", type=float, default=2.0)
    ap.add_argument("--imbalance-cost-cap", type=float, default=1_000_000_000.0)
    ap.add_argument("--pairing-only-when-residual", action="store_true")
    ap.add_argument("--high-price-mode", choices=["none", "close_only"], default="none")
    ap.add_argument("--high-price-normal-px-hi", type=float, default=None)
    ap.add_argument("--activation-mode", choices=["none", "opp_seen"], default="none")
    ap.add_argument("--activation-window-s", type=float, default=60.0)
    ap.add_argument("--risk-seed-closeability-net-cap", type=float, default=None)
    ap.add_argument("--risk-seed-closeability-soft-net-cap", type=float, default=None)
    ap.add_argument("--risk-seed-closeability-debt-floor", type=float, default=None)
    ap.add_argument("--risk-seed-closeability-debt-budget", type=float, default=0.0)
    ap.add_argument(
        "--risk-seed-pending-opp-credit",
        type=float,
        default=1.0,
        help="Credit pending opposite-side orders as protective exposure for risk-increasing seed gates; 1.0 preserves legacy behavior, 0.0 credits only filled opposite lots.",
    )
    ap.add_argument(
        "--pair-completion-net-cap",
        type=float,
        default=None,
        help="Block admissions that would pair against existing opposite lots above this actual FIFO pair-cost cap.",
    )
    ap.add_argument(
        "--pair-completion-min-pair-pnl-after",
        type=float,
        default=None,
        help="Block loss-making pair completions only when projected realized pair PnL for this slug would fall below this floor.",
    )
    ap.add_argument("--taker-fee-rate", type=float, default=0.07)
    ap.add_argument("--salvage-net-cap", type=float, default=0.95)
    ap.add_argument("--salvage-age-s", type=float, default=30.0)
    ap.add_argument("--salvage-min-lot-cost", type=float, default=0.25)
    ap.add_argument("--max-salvage-qty", type=float, default=250.0)
    ap.add_argument(
        "--strict-rescue-skip-low-cost-lots",
        action="store_true",
        help="Allow strict rescue to scan past FIFO lots below --salvage-min-lot-cost while preserving age ordering.",
    )
    ap.add_argument("--surplus-budget-mode", choices=["none", "block", "cap"], default="none")
    ap.add_argument("--surplus-budget-bootstrap", type=float, default=0.0)
    ap.add_argument("--surplus-budget-mult", type=float, default=0.0)
    ap.add_argument("--surplus-budget-max-abs-unpaired-cost", type=float, default=None)
    ap.add_argument("--strict-rescue-mode", choices=["none", "source_audit"], default="none")
    ap.add_argument("--strict-rescue-l1-age-max-ms", type=int, default=None)
    ap.add_argument("--strict-rescue-max-wait-s", type=float, default=None)
    ap.add_argument("--strict-rescue-close-size-haircut", type=float, default=1.0)
    ap.add_argument("--strict-rescue-close-ask-slip", type=float, default=0.0)
    ap.add_argument(
        "--strict-rescue-surplus-net-cap",
        type=float,
        default=None,
        help="Optional surplus-backed strict-rescue cap above --salvage-net-cap; requires --strict-rescue-min-pair-pnl-after.",
    )
    ap.add_argument(
        "--strict-rescue-min-pair-pnl-after",
        type=float,
        default=None,
        help="Allow strict rescue above --salvage-net-cap only if projected realized pair PnL stays at or above this floor.",
    )
    ap.add_argument("--strict-rescue-require-book-source", action="store_true")
    ap.add_argument("--strict-rescue-require-l2-source", action="store_true")
    ap.add_argument("--source-quality-require-trade-source", action="store_true")
    ap.add_argument("--source-quality-require-l1-source", action="store_true")
    ap.add_argument("--source-quality-l1-age-max-ms", type=int, default=None)
    ap.add_argument("--source-quality-require-l2-source", action="store_true")
    ap.add_argument(
        "--fair-price-admission-jsonl",
        default=None,
        help="Default-off b55/ce25-style fair-price admission rows keyed by market_slug and side.",
    )
    ap.add_argument("--fair-price-min-edge", type=float, default=0.015)
    ap.add_argument("--fair-price-max-pair-cost", type=float, default=0.975)
    ap.add_argument("--fair-price-min-seconds-to-close", type=float, default=None)
    ap.add_argument("--fair-price-max-seconds-to-close", type=float, default=None)
    ap.add_argument("--fair-price-max-row-age-ms", type=int, default=None)
    ap.add_argument("--write-normalized-lifecycle", action="store_true")
    ap.add_argument("--write-rescue-block-diagnostics", action="store_true")
    ap.add_argument(
        "--allow-concurrent-shared-ingress-readers",
        action="store_true",
        help="Record explicit audit metadata for running as one of multiple read-only shared-ingress clients.",
    )
    args = ap.parse_args()

    if (args.late_target_after_s is None) != (args.late_target_qty is None):
        ap.error("--late-target-after-s and --late-target-qty must be provided together")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    cfg = RunnerConfig(
        edge=args.edge,
        queue_share=args.queue_share,
        target_qty=args.target_qty,
        fill_haircut=args.fill_haircut,
        max_seed_qty=args.max_seed_qty,
        max_open_cost=args.max_open_cost,
        seed_l1_cap=args.seed_l1_cap,
        seed_px_lo=args.seed_px_lo,
        seed_px_hi=args.seed_px_hi,
        seed_offset_max_s=args.seed_offset_max_s,
        late_target_after_s=args.late_target_after_s,
        late_target_qty=args.late_target_qty,
        late_repair_after_s=args.late_repair_after_s,
        order_ttl_ms=int(args.order_ttl_s * 1000),
        imbalance_qty_cap=args.imbalance_qty_cap,
        imbalance_cost_cap=args.imbalance_cost_cap,
        pairing_only_when_residual=args.pairing_only_when_residual,
        high_price_mode=args.high_price_mode,
        high_price_normal_px_hi=args.high_price_normal_px_hi,
        activation_mode=args.activation_mode,
        activation_window_s=args.activation_window_s,
        risk_seed_closeability_net_cap=args.risk_seed_closeability_net_cap,
        risk_seed_closeability_soft_net_cap=args.risk_seed_closeability_soft_net_cap,
        risk_seed_closeability_debt_floor=args.risk_seed_closeability_debt_floor,
        risk_seed_closeability_debt_budget=args.risk_seed_closeability_debt_budget,
        risk_seed_pending_opp_credit=args.risk_seed_pending_opp_credit,
        pair_completion_net_cap=args.pair_completion_net_cap,
        pair_completion_min_pair_pnl_after=args.pair_completion_min_pair_pnl_after,
        taker_fee_rate=args.taker_fee_rate,
        salvage_net_cap=args.salvage_net_cap,
        salvage_age_ms=int(args.salvage_age_s * 1000),
        salvage_min_lot_cost=args.salvage_min_lot_cost,
        max_salvage_qty=args.max_salvage_qty,
        strict_rescue_skip_low_cost_lots=args.strict_rescue_skip_low_cost_lots,
        surplus_budget_mode=args.surplus_budget_mode,
        surplus_budget_bootstrap=args.surplus_budget_bootstrap,
        surplus_budget_mult=args.surplus_budget_mult,
        surplus_budget_max_abs_unpaired_cost=args.surplus_budget_max_abs_unpaired_cost,
        strict_rescue_mode=args.strict_rescue_mode,
        strict_rescue_l1_age_max_ms=args.strict_rescue_l1_age_max_ms,
        strict_rescue_max_wait_ms=(
            int(args.strict_rescue_max_wait_s * 1000)
            if args.strict_rescue_max_wait_s is not None
            else None
        ),
        strict_rescue_close_size_haircut=args.strict_rescue_close_size_haircut,
        strict_rescue_close_ask_slip=args.strict_rescue_close_ask_slip,
        strict_rescue_surplus_net_cap=args.strict_rescue_surplus_net_cap,
        strict_rescue_min_pair_pnl_after=args.strict_rescue_min_pair_pnl_after,
        strict_rescue_require_book_source=args.strict_rescue_require_book_source,
        strict_rescue_require_l2_source=args.strict_rescue_require_l2_source,
        source_quality_require_trade_source=args.source_quality_require_trade_source,
        source_quality_require_l1_source=args.source_quality_require_l1_source,
        source_quality_l1_age_max_ms=args.source_quality_l1_age_max_ms,
        source_quality_require_l2_source=args.source_quality_require_l2_source,
        fair_price_admission_path=args.fair_price_admission_jsonl,
        fair_price_min_edge=args.fair_price_min_edge,
        fair_price_max_pair_cost=args.fair_price_max_pair_cost,
        fair_price_min_seconds_to_close=args.fair_price_min_seconds_to_close,
        fair_price_max_seconds_to_close=args.fair_price_max_seconds_to_close,
        fair_price_max_row_age_ms=args.fair_price_max_row_age_ms,
        write_normalized_lifecycle=args.write_normalized_lifecycle,
        write_rescue_block_diagnostics=args.write_rescue_block_diagnostics,
        allow_concurrent_shared_ingress_readers=args.allow_concurrent_shared_ingress_readers,
    )
    if cfg.imbalance_qty_cap <= cfg.dust_qty:
        raise SystemExit(f"--imbalance-qty-cap must exceed dust_qty={cfg.dust_qty}; otherwise every seed is blocked")
    if cfg.late_target_qty is not None and cfg.late_target_qty <= cfg.dust_qty:
        raise SystemExit(f"--late-target-qty must exceed dust_qty={cfg.dust_qty}; otherwise the late window cannot seed")
    if cfg.activation_window_s <= 0:
        raise SystemExit("--activation-window-s must be positive")
    if cfg.risk_seed_closeability_net_cap is not None and cfg.risk_seed_closeability_net_cap <= 0:
        raise SystemExit("--risk-seed-closeability-net-cap must be positive")
    if cfg.risk_seed_closeability_soft_net_cap is not None and cfg.risk_seed_closeability_soft_net_cap <= 0:
        raise SystemExit("--risk-seed-closeability-soft-net-cap must be positive")
    if cfg.risk_seed_closeability_debt_floor is not None and cfg.risk_seed_closeability_debt_floor <= 0:
        raise SystemExit("--risk-seed-closeability-debt-floor must be positive")
    if cfg.risk_seed_closeability_debt_budget < 0:
        raise SystemExit("--risk-seed-closeability-debt-budget must be non-negative")
    if not (0.0 <= cfg.risk_seed_pending_opp_credit <= 1.0):
        raise SystemExit("--risk-seed-pending-opp-credit must be in [0, 1]")
    if cfg.pair_completion_net_cap is not None and cfg.pair_completion_net_cap <= 0:
        raise SystemExit("--pair-completion-net-cap must be positive")
    if (
        cfg.risk_seed_closeability_soft_net_cap is not None
        and cfg.risk_seed_closeability_debt_floor is not None
        and cfg.risk_seed_closeability_debt_floor > cfg.risk_seed_closeability_soft_net_cap + 1e-12
    ):
        raise SystemExit("--risk-seed-closeability-debt-floor cannot exceed --risk-seed-closeability-soft-net-cap")
    if cfg.taker_fee_rate < 0:
        raise SystemExit("--taker-fee-rate must be non-negative")
    if cfg.high_price_mode == "close_only":
        if cfg.high_price_normal_px_hi is None:
            raise SystemExit("--high-price-normal-px-hi is required when --high-price-mode=close_only")
        if not (0.0 < cfg.high_price_normal_px_hi < cfg.seed_px_hi):
            raise SystemExit("--high-price-normal-px-hi must be between 0 and --seed-px-hi")
    if cfg.surplus_budget_bootstrap < 0 or cfg.surplus_budget_mult < 0:
        raise SystemExit("--surplus-budget-bootstrap and --surplus-budget-mult must be non-negative")
    if cfg.surplus_budget_max_abs_unpaired_cost is not None and cfg.surplus_budget_max_abs_unpaired_cost < 0:
        raise SystemExit("--surplus-budget-max-abs-unpaired-cost must be non-negative")
    if cfg.max_salvage_qty <= 0:
        raise SystemExit("--max-salvage-qty must be positive")
    if not (0.0 < cfg.strict_rescue_close_size_haircut <= 1.0):
        raise SystemExit("--strict-rescue-close-size-haircut must be in (0, 1]")
    if cfg.strict_rescue_close_ask_slip < 0:
        raise SystemExit("--strict-rescue-close-ask-slip must be non-negative")
    if (cfg.strict_rescue_surplus_net_cap is None) != (cfg.strict_rescue_min_pair_pnl_after is None):
        raise SystemExit("--strict-rescue-surplus-net-cap and --strict-rescue-min-pair-pnl-after must be provided together")
    if cfg.strict_rescue_surplus_net_cap is not None and cfg.strict_rescue_surplus_net_cap <= cfg.salvage_net_cap + 1e-12:
        raise SystemExit("--strict-rescue-surplus-net-cap must exceed --salvage-net-cap")
    if cfg.strict_rescue_l1_age_max_ms is not None and cfg.strict_rescue_l1_age_max_ms < 0:
        raise SystemExit("--strict-rescue-l1-age-max-ms must be non-negative")
    if cfg.source_quality_l1_age_max_ms is not None and cfg.source_quality_l1_age_max_ms < 0:
        raise SystemExit("--source-quality-l1-age-max-ms must be non-negative")
    if cfg.strict_rescue_max_wait_ms is not None and cfg.strict_rescue_max_wait_ms <= cfg.salvage_age_ms:
        raise SystemExit("--strict-rescue-max-wait-s must exceed --salvage-age-s")
    if cfg.fair_price_min_edge < 0:
        raise SystemExit("--fair-price-min-edge must be non-negative")
    if cfg.fair_price_max_pair_cost <= 0:
        raise SystemExit("--fair-price-max-pair-cost must be positive")
    if cfg.fair_price_max_row_age_ms is not None and cfg.fair_price_max_row_age_ms < 0:
        raise SystemExit("--fair-price-max-row-age-ms must be non-negative")
    if (
        cfg.fair_price_min_seconds_to_close is not None
        and cfg.fair_price_max_seconds_to_close is not None
        and cfg.fair_price_min_seconds_to_close > cfg.fair_price_max_seconds_to_close + 1e-12
    ):
        raise SystemExit("--fair-price-min-seconds-to-close cannot exceed --fair-price-max-seconds-to-close")
    fair_price_gate = None
    fair_price_rows = 0
    if args.fair_price_admission_jsonl:
        fair_price_path = Path(args.fair_price_admission_jsonl).expanduser().resolve()
        if not fair_price_path.exists():
            raise SystemExit(f"--fair-price-admission-jsonl does not exist: {fair_price_path}")
        fair_price_gate = FairPriceAdmissionGate.from_path(
            fair_price_path,
            min_edge=cfg.fair_price_min_edge,
            max_pair_cost=cfg.fair_price_max_pair_cost,
            min_seconds_to_close=cfg.fair_price_min_seconds_to_close,
            max_seconds_to_close=cfg.fair_price_max_seconds_to_close,
            max_row_age_ms=cfg.fair_price_max_row_age_ms,
        )
        fair_price_rows = len(fair_price_gate.rows)
    socket_path = str(Path(args.shared_ingress_root) / "market.sock")
    process_snapshot_start = collect_runner_process_snapshot()
    markets = resolve_markets(Path(args.repo), args.prefix, args.round_offsets, args.market_slugs)
    manifest = {
        "kind": "manifest",
        "created_ms": now_ms(),
        "script": "xuan_dplus_passive_passive_shadow_runner.py",
        "duration_s": args.duration_s,
        "markets": markets,
        "market_resolution": {
            "mode": "exact_slugs" if split_csv(args.market_slugs) else "prefix_offsets",
            "prefix": args.prefix,
            "round_offsets": args.round_offsets,
            "market_slugs": split_csv(args.market_slugs),
        },
        "config": cfg.__dict__,
        "fair_price_admission": {
            "enabled": fair_price_gate is not None,
            "path": str(Path(args.fair_price_admission_jsonl).expanduser().resolve()) if args.fair_price_admission_jsonl else None,
            "rows": fair_price_rows,
            "min_edge": cfg.fair_price_min_edge,
            "max_pair_cost": cfg.fair_price_max_pair_cost,
            "min_seconds_to_close": cfg.fair_price_min_seconds_to_close,
            "max_seconds_to_close": cfg.fair_price_max_seconds_to_close,
            "max_row_age_ms": cfg.fair_price_max_row_age_ms,
        },
        "safety": {
            "orders_sent": False,
            "dry_run": os.environ.get("PM_DRY_RUN"),
            "shared_ingress_role": os.environ.get("PM_SHARED_INGRESS_ROLE"),
            "shared_ingress_root": args.shared_ingress_root,
            "instance_id": os.environ.get("PM_INSTANCE_ID"),
        },
        "concurrency": {
            "allow_concurrent_shared_ingress_readers": args.allow_concurrent_shared_ingress_readers,
            "mode": (
                "shared_ingress_read_only_concurrent_reader"
                if args.allow_concurrent_shared_ingress_readers
                else "single_reader_or_unrecorded_concurrency"
            ),
            "socket_path": socket_path,
            "runner_pid": os.getpid(),
            "output_dir": str(out.resolve()),
            "process_snapshot_start": process_snapshot_start,
            "concurrent_runner_count_start": len(process_snapshot_start),
            "evidence_claim": (
                "clean_concurrent_shared_ingress_reader"
                if args.allow_concurrent_shared_ingress_readers
                else "no_concurrent_reader_claim"
            ),
            "read_only_contract": {
                "uses_shared_ingress_as_client": True,
                "mutates_shared_ingress": False,
                "sends_orders": False,
                "writes_only_output_dir": str(out.resolve()),
            },
        },
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    await asyncio.gather(*(run_market(m, socket_path, out, args.duration_s, cfg, fair_price_gate) for m in markets))
    aggregate_report = aggregate(out)
    manifest["completed_ms"] = now_ms()
    manifest["concurrency"]["process_snapshot_end"] = collect_runner_process_snapshot()
    manifest["concurrency"]["concurrent_runner_count_end"] = len(manifest["concurrency"]["process_snapshot_end"])
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(aggregate_report, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
