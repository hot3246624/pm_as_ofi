#!/usr/bin/env python3
"""Rerun D+ candidate-pipeline V1 state machine from candidate_base.

This script reads only the derived candidate_base DuckDB plus the existing
candidate-pipeline result manifests for baseline comparison. It does not read
raw/replay/collector stores, the full completion event store, sockets, SSH, or
event JSONL.
"""

from __future__ import annotations

import argparse
import csv
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
ARTIFACT = "xuan_b27_dplus_candidate_pipeline_state_machine_fill_to_balance_rerun"
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
    risk_increasing_imbalance_qty_cap: float | None = None
    risk_increasing_pair_fill_cap: bool = False
    complete_set_dust_opp_late_repair_sizer: bool = False
    complete_set_dust_opp_late_repair_after_s: float = 90.0
    complete_set_dust_opp_late_repair_qty_buffer: float = 1.0
    late_repair_fill_to_balance_after_s: float | None = None
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
    pair_inventory_priority: str = "fifo"


@dataclass
class Lot:
    qty: float
    px: float
    ts_ms: int
    side: str
    source_candidate_row_id: str
    lot_id: str = ""
    original_qty: float = 0.0
    source_label: str = ""
    offset_s: float = 0.0
    trigger_px: float = 0.0
    trigger_size: float = 0.0
    source_risk_direction: str = "unknown"
    pre_seed_same_qty: float = 0.0
    pre_seed_opp_qty: float = 0.0
    pre_seed_same_cost: float = 0.0
    pre_seed_opp_cost: float = 0.0
    ledger_proxy_before: float = 0.0
    ledger_proxy_after: float = 0.0
    source_pair_qty: float = 0.0
    source_pair_cost: float = 0.0
    source_pair_pnl: float = 0.0


@dataclass
class State:
    day: str
    slug: str
    winner_side: str | None
    lots: dict[str, deque[Lot]]
    condition_id: str = ""
    last_seed_ts_ms: int = -(10**18)
    last_ts_ms: int = 0
    active: bool = False


SELECTED_CLOSE_ACTION_FIELDS = [
    "schema_version",
    "profile_name",
    "validation_request_id",
    "day",
    "condition_id",
    "slug",
    "close_action_id",
    "close_sequence",
    "source_seed_action_id",
    "source_seed_candidate_row_id",
    "source_seed_ts_ms",
    "source_seed_side",
    "source_seed_qty",
    "source_seed_px",
    "residual_lot_id",
    "residual_lot_source_seed_action_id",
    "residual_lot_source_candidate_row_id",
    "residual_lot_side",
    "residual_lot_remaining_qty_before_close",
    "residual_lot_remaining_cost_before_close",
    "close_ts_ms",
    "close_side",
    "close_qty",
    "close_px",
    "close_cost",
    "close_fee_rate",
    "close_fee_rate_source",
    "close_official_fee",
    "expected_close_decision",
    "expected_close_status",
    "close_reason",
    "paired_qty",
    "paired_yes_source_action_id",
    "paired_no_source_action_id",
    "paired_yes_px",
    "paired_no_px",
    "paired_cost",
    "pair_pnl_delta",
    "lookup_day",
    "lookup_condition_id",
    "lookup_close_ts_ms",
    "lookup_close_side",
    "close_book_l1_ref",
    "close_book_l2_ref",
    "close_trade_before_ref",
    "close_trade_after_ref",
]

SELECTED_SEED_LEDGER_CONTEXT_FIELDS = [
    "schema_version",
    "profile_name",
    "validation_request_id",
    "day",
    "condition_id",
    "slug",
    "source_seed_action_id",
    "source_seed_candidate_row_id",
    "source_label",
    "ts_ms",
    "ts_iso",
    "side",
    "opposite_side",
    "offset_s",
    "trigger_px",
    "trigger_size",
    "trigger_ts_ms",
    "seed_px",
    "seed_qty",
    "seed_cost",
    "official_fee_rate",
    "official_fee",
    "source_risk_direction",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_s",
    "quote_intent_id",
    "source_order_id",
    "source_sequence_id",
    "external_shadow_ids_available",
    "external_shadow_id_policy",
    "selected_context_reason",
]


@dataclass
class SelectedCloseActionRecorder:
    profile_name: str
    fee_rate_source: str
    sample_cap: int = 30
    per_day_cap: int = 2
    selection_mode: str = "first"
    rows: list[dict[str, Any]] | None = None
    candidate_rows: list[dict[str, Any]] | None = None
    sequence_by_key: dict[tuple[str, str, str], int] | None = None
    selected_by_day: dict[str, int] | None = None
    eligible_count: int = 0
    skipped_by_cap: int = 0
    selection_finalized: bool = False

    def __post_init__(self) -> None:
        self.selection_mode = str(self.selection_mode or "first").strip().lower()
        if self.selection_mode not in {"first", "diversified"}:
            raise ValueError(f"Unsupported selected close-action selection mode: {self.selection_mode}")
        if self.rows is None:
            self.rows = []
        if self.candidate_rows is None:
            self.candidate_rows = []
        if self.sequence_by_key is None:
            self.sequence_by_key = {}
        if self.selected_by_day is None:
            self.selected_by_day = defaultdict(int)

    def should_record_profile(self, profile: Profile) -> bool:
        return self.profile_name == "all" or profile.name == self.profile_name

    def record(
        self,
        *,
        profile: Profile,
        state: State,
        ts_ms: int,
        trigger_lot: Lot,
        residual_lot: Lot,
        yes_lot: Lot,
        no_lot: Lot,
        take: float,
        pair_cost: float,
    ) -> None:
        if not self.should_record_profile(profile):
            return
        self.eligible_count += 1
        condition_id = state.condition_id or "unknown_condition"
        key = (profile.name, state.day, condition_id)
        assert self.sequence_by_key is not None
        close_sequence = self.sequence_by_key.get(key, 0) + 1
        self.sequence_by_key[key] = close_sequence
        assert self.rows is not None
        assert self.selected_by_day is not None
        if (
            self.selection_mode == "first"
            and (len(self.rows) >= self.sample_cap or self.selected_by_day[state.day] >= self.per_day_cap)
        ):
            self.skipped_by_cap += 1
            return

        trigger_qty_before = trigger_lot.qty
        residual_qty_before = residual_lot.qty
        residual_cost_before = residual_lot.qty * residual_lot.px
        close_cost = take * trigger_lot.px
        close_fee = official_taker_fee(take, trigger_lot.px, profile.official_fee_rate)
        yes_source = yes_lot.source_candidate_row_id
        no_source = no_lot.source_candidate_row_id
        row = {
            "schema_version": "selected_close_action_export_v1",
            "profile_name": profile.name,
            "validation_request_id": f"strict_rescue_close_selected_request_v1:{profile.name}",
            "day": state.day,
            "condition_id": condition_id,
            "slug": state.slug,
            "close_action_id": f"close:{profile.name}:{state.day}:{condition_id}:{close_sequence}",
            "close_sequence": close_sequence,
            "source_seed_action_id": trigger_lot.source_candidate_row_id,
            "source_seed_candidate_row_id": trigger_lot.source_candidate_row_id,
            "source_seed_ts_ms": trigger_lot.ts_ms,
            "source_seed_side": trigger_lot.side,
            "source_seed_qty": round(trigger_lot.original_qty or trigger_qty_before, 12),
            "source_seed_px": round(trigger_lot.px, 12),
            "residual_lot_id": residual_lot.lot_id or f"residual_lot:{state.day}:{residual_lot.source_candidate_row_id}",
            "residual_lot_source_seed_action_id": residual_lot.source_candidate_row_id,
            "residual_lot_source_candidate_row_id": residual_lot.source_candidate_row_id,
            "residual_lot_side": residual_lot.side,
            "residual_lot_remaining_qty_before_close": round(residual_qty_before, 12),
            "residual_lot_remaining_cost_before_close": round(residual_cost_before, 12),
            "close_ts_ms": ts_ms,
            "close_side": trigger_lot.side,
            "close_qty": round(take, 12),
            "close_px": round(trigger_lot.px, 12),
            "close_cost": round(close_cost, 12),
            "close_fee_rate": profile.official_fee_rate,
            "close_fee_rate_source": self.fee_rate_source,
            "close_official_fee": round(close_fee, 12),
            "expected_close_decision": "ACCEPTED_SELECTED_CLOSE_ACTION",
            "expected_close_status": "SOURCE_TRUTH_RESEARCH_ONLY_PENDING",
            "close_reason": "pair_inventory_new_seed_offsets_residual",
            "paired_qty": round(take, 12),
            "paired_yes_source_action_id": yes_source,
            "paired_no_source_action_id": no_source,
            "paired_yes_px": round(yes_lot.px, 12),
            "paired_no_px": round(no_lot.px, 12),
            "paired_cost": round(pair_cost, 12),
            "pair_pnl_delta": round(take * (1.0 - pair_cost), 12),
            "lookup_day": state.day,
            "lookup_condition_id": condition_id,
            "lookup_close_ts_ms": ts_ms,
            "lookup_close_side": trigger_lot.side,
            "close_book_l1_ref": "",
            "close_book_l2_ref": "",
            "close_trade_before_ref": "",
            "close_trade_after_ref": "",
        }
        if self.selection_mode == "diversified":
            assert self.candidate_rows is not None
            self.candidate_rows.append(row)
            self.selection_finalized = False
            return
        self.rows.append(row)
        self.selected_by_day[state.day] += 1

    @staticmethod
    def _row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            str(row.get("day") or ""),
            int(float(row.get("close_ts_ms") or 0)),
            str(row.get("condition_id") or ""),
            str(row.get("close_side") or ""),
            int(float(row.get("close_sequence") or 0)),
            str(row.get("close_action_id") or ""),
        )

    def finalize_selection(self) -> None:
        if self.selection_mode != "diversified" or self.selection_finalized:
            return
        assert self.candidate_rows is not None
        self.rows = []
        self.selected_by_day = defaultdict(int)
        selected_ids: set[str] = set()

        if self.sample_cap <= 0 or self.per_day_cap <= 0:
            self.skipped_by_cap = self.eligible_count
            self.selection_finalized = True
            return

        rows_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in self.candidate_rows:
            rows_by_day[str(row.get("day") or "")].append(row)

        def add_row(row: dict[str, Any], day_conditions: set[str], day_sides: set[str]) -> bool:
            assert self.rows is not None
            assert self.selected_by_day is not None
            if len(self.rows) >= self.sample_cap:
                return False
            day = str(row.get("day") or "")
            if self.selected_by_day[day] >= self.per_day_cap:
                return False
            close_action_id = str(row.get("close_action_id") or "")
            if close_action_id in selected_ids:
                return False
            self.rows.append(row)
            self.selected_by_day[day] += 1
            selected_ids.add(close_action_id)
            day_conditions.add(str(row.get("condition_id") or ""))
            day_sides.add(str(row.get("close_side") or ""))
            return True

        for day in sorted(rows_by_day):
            if len(self.rows) >= self.sample_cap:
                break
            day_rows = sorted(rows_by_day[day], key=self._row_sort_key)
            day_conditions: set[str] = set()
            day_sides: set[str] = set()
            for row in day_rows:
                if str(row.get("condition_id") or "") in day_conditions:
                    continue
                if str(row.get("close_side") or "") in day_sides:
                    continue
                add_row(row, day_conditions, day_sides)
                if self.selected_by_day[day] >= self.per_day_cap or len(self.rows) >= self.sample_cap:
                    break
            for row in day_rows:
                if self.selected_by_day[day] >= self.per_day_cap or len(self.rows) >= self.sample_cap:
                    break
                if str(row.get("condition_id") or "") in day_conditions:
                    continue
                add_row(row, day_conditions, day_sides)
            for row in day_rows:
                if self.selected_by_day[day] >= self.per_day_cap or len(self.rows) >= self.sample_cap:
                    break
                add_row(row, day_conditions, day_sides)

        self.skipped_by_cap = max(0, self.eligible_count - len(self.rows or []))
        self.selection_finalized = True

    def summary(self) -> dict[str, Any]:
        self.finalize_selection()
        assert self.rows is not None
        assert self.selected_by_day is not None
        selected_conditions = {str(row.get("condition_id") or "") for row in self.rows}
        selected_side_counts: dict[str, int] = defaultdict(int)
        for row in self.rows:
            selected_side_counts[str(row.get("close_side") or "")] += 1
        return {
            "schema_version": "selected_close_action_export_v1",
            "profile_name": self.profile_name,
            "eligible_close_actions": self.eligible_count,
            "selected_close_actions": len(self.rows),
            "skipped_by_sample_or_day_cap": self.skipped_by_cap,
            "sample_cap": self.sample_cap,
            "per_day_cap": self.per_day_cap,
            "selection_mode": self.selection_mode,
            "candidate_pool_rows": len(self.candidate_rows or []),
            "selected_condition_count": len(selected_conditions),
            "selected_side_counts": dict(sorted(selected_side_counts.items())),
            "selected_by_day": dict(sorted(self.selected_by_day.items())),
            "fee_rate_source": self.fee_rate_source,
        }


@dataclass
class SelectedSeedLedgerContextRecorder:
    profile_name: str
    sample_cap: int = 60
    per_day_cap: int = 4
    selection_mode: str = "residual_tail"
    rows_by_source: dict[str, dict[str, Any]] | None = None
    rows: list[dict[str, Any]] | None = None
    selected_by_day: dict[str, int] | None = None
    eligible_count: int = 0
    selected_count: int = 0
    skipped_by_cap: int = 0
    selection_finalized: bool = False

    def __post_init__(self) -> None:
        self.selection_mode = str(self.selection_mode or "residual_tail").strip().lower()
        if self.selection_mode not in {"first", "residual_tail"}:
            raise ValueError(f"Unsupported selected seed ledger-context selection mode: {self.selection_mode}")
        if self.rows_by_source is None:
            self.rows_by_source = {}
        if self.rows is None:
            self.rows = []
        if self.selected_by_day is None:
            self.selected_by_day = defaultdict(int)

    def should_record_profile(self, profile: Profile) -> bool:
        return self.profile_name == "all" or profile.name == self.profile_name

    @staticmethod
    def _source_key(profile: Profile, source_candidate_row_id: str) -> str:
        return f"{profile.name}\x00{source_candidate_row_id}"

    def record_seed(
        self,
        *,
        profile: Profile,
        state: State,
        row: dict[str, Any],
        seed_lot: Lot,
        official_fee: float,
    ) -> None:
        if not self.should_record_profile(profile):
            return
        self.eligible_count += 1
        condition_id = state.condition_id or str(row.get("condition_id") or "unknown_condition")
        source_candidate_row_id = str(row["candidate_row_id"])
        assert self.rows_by_source is not None
        self.rows_by_source[self._source_key(profile, source_candidate_row_id)] = {
            "schema_version": "selected_seed_ledger_context_export_v1",
            "profile_name": profile.name,
            "validation_request_id": f"residual_tail_ledger_context_verifier_v1:{profile.name}",
            "day": str(row["day"]),
            "condition_id": condition_id,
            "slug": str(row["slug"]),
            "source_seed_action_id": source_candidate_row_id,
            "source_seed_candidate_row_id": source_candidate_row_id,
            "source_label": str(row.get("source_label") or ""),
            "ts_ms": int(row["ts_ms"]),
            "ts_iso": str(row.get("ts_iso") or ""),
            "side": seed_lot.side,
            "opposite_side": other(seed_lot.side),
            "offset_s": round(seed_lot.offset_s, 6),
            "trigger_px": round(seed_lot.trigger_px, 12),
            "trigger_size": round(seed_lot.trigger_size, 12),
            "trigger_ts_ms": int(seed_lot.ts_ms),
            "seed_px": round(seed_lot.px, 12),
            "seed_qty": round(seed_lot.original_qty or seed_lot.qty, 12),
            "seed_cost": round((seed_lot.original_qty or seed_lot.qty) * seed_lot.px, 12),
            "official_fee_rate": profile.official_fee_rate,
            "official_fee": round(official_fee, 12),
            "source_risk_direction": seed_lot.source_risk_direction,
            "pre_seed_same_qty": round(seed_lot.pre_seed_same_qty, 12),
            "pre_seed_opp_qty": round(seed_lot.pre_seed_opp_qty, 12),
            "pre_seed_same_cost": round(seed_lot.pre_seed_same_cost, 12),
            "pre_seed_opp_cost": round(seed_lot.pre_seed_opp_cost, 12),
            "ledger_proxy_before": round(seed_lot.ledger_proxy_before, 12),
            "ledger_proxy_after": round(seed_lot.ledger_proxy_after, 12),
            "source_pair_qty": 0.0,
            "source_pair_cost": 0.0,
            "source_pair_pnl": 0.0,
            "source_residual_qty": 0.0,
            "source_residual_cost": 0.0,
            "source_residual_age_s": 0.0,
            "quote_intent_id": "",
            "source_order_id": "",
            "source_sequence_id": "",
            "external_shadow_ids_available": False,
            "external_shadow_id_policy": (
                "candidate_base/state_machine export has no quote_intent_id, source_order_id, or "
                "source_sequence_id; future shadow exemplar joins must align by candidate_row_id/ts/condition/side "
                "and must not fabricate external ids."
            ),
            "selected_context_reason": "selected_seed_ledger_context_export_default_off",
        }

    def record_pair(self, profile: Profile, lot: Lot, take: float, pair_cost: float) -> None:
        if not self.should_record_profile(profile):
            return
        assert self.rows_by_source is not None
        row = self.rows_by_source.get(self._source_key(profile, lot.source_candidate_row_id))
        if row is None:
            return
        row["source_pair_qty"] = round(float(row.get("source_pair_qty") or 0.0) + take, 12)
        row["source_pair_cost"] = round(float(row.get("source_pair_cost") or 0.0) + take * pair_cost, 12)
        row["source_pair_pnl"] = round(float(row.get("source_pair_pnl") or 0.0) + take * (1.0 - pair_cost), 12)

    def record_residual(self, profile: Profile, lot: Lot, settle_ts_ms: int) -> None:
        if not self.should_record_profile(profile):
            return
        assert self.rows_by_source is not None
        row = self.rows_by_source.get(self._source_key(profile, lot.source_candidate_row_id))
        if row is None:
            return
        residual_cost = lot.qty * lot.px
        residual_age_s = max(0.0, (settle_ts_ms - lot.ts_ms) / 1000.0)
        row["source_residual_qty"] = round(float(row.get("source_residual_qty") or 0.0) + lot.qty, 12)
        row["source_residual_cost"] = round(float(row.get("source_residual_cost") or 0.0) + residual_cost, 12)
        row["source_residual_age_s"] = round(max(float(row.get("source_residual_age_s") or 0.0), residual_age_s), 6)

    @staticmethod
    def _first_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            str(row.get("day") or ""),
            int(float(row.get("ts_ms") or 0)),
            str(row.get("condition_id") or ""),
            str(row.get("side") or ""),
            str(row.get("source_seed_candidate_row_id") or ""),
        )

    @staticmethod
    def _residual_tail_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            -float(row.get("source_residual_cost") or 0.0),
            -float(row.get("source_residual_qty") or 0.0),
            str(row.get("day") or ""),
            int(float(row.get("ts_ms") or 0)),
            str(row.get("condition_id") or ""),
            str(row.get("side") or ""),
            str(row.get("source_seed_candidate_row_id") or ""),
        )

    def finalize_selection(self) -> None:
        if self.selection_finalized:
            return
        assert self.rows_by_source is not None
        assert self.rows is not None
        assert self.selected_by_day is not None
        sort_key = self._residual_tail_sort_key if self.selection_mode == "residual_tail" else self._first_sort_key
        candidates = sorted(self.rows_by_source.values(), key=sort_key)
        self.rows = []
        self.selected_by_day = defaultdict(int)
        if self.sample_cap <= 0 or self.per_day_cap <= 0:
            self.skipped_by_cap = len(candidates)
            self.selection_finalized = True
            return
        for row in candidates:
            if len(self.rows) >= self.sample_cap:
                break
            day = str(row.get("day") or "")
            if self.selected_by_day[day] >= self.per_day_cap:
                continue
            self.rows.append(row)
            self.selected_by_day[day] += 1
        self.selected_count = len(self.rows)
        self.skipped_by_cap = max(0, len(candidates) - len(self.rows))
        self.selection_finalized = True

    def summary(self) -> dict[str, Any]:
        self.finalize_selection()
        assert self.rows is not None
        assert self.selected_by_day is not None
        selected_conditions = {str(row.get("condition_id") or "") for row in self.rows}
        selected_side_counts: dict[str, int] = defaultdict(int)
        residual_rows = 0
        pair_context_rows = 0
        for row in self.rows:
            selected_side_counts[str(row.get("side") or "")] += 1
            if float(row.get("source_residual_qty") or 0.0) > DUST:
                residual_rows += 1
            if float(row.get("source_pair_qty") or 0.0) > DUST:
                pair_context_rows += 1
        return {
            "schema_version": "selected_seed_ledger_context_export_v1",
            "profile_name": self.profile_name,
            "eligible_seed_contexts": self.eligible_count,
            "selected_seed_contexts": len(self.rows),
            "skipped_by_sample_or_day_cap": self.skipped_by_cap,
            "sample_cap": self.sample_cap,
            "per_day_cap": self.per_day_cap,
            "selection_mode": self.selection_mode,
            "selected_condition_count": len(selected_conditions),
            "selected_side_counts": dict(sorted(selected_side_counts.items())),
            "selected_by_day": dict(sorted(self.selected_by_day.items())),
            "selected_rows_with_residual": residual_rows,
            "selected_rows_with_pair_context": pair_context_rows,
            "external_shadow_ids_available": False,
        }


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


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


def ledger_proxy(metrics: defaultdict[str, float], lots: dict[str, deque[Lot]]) -> float:
    exposure_qty = lot_qty(lots["YES"]) + lot_qty(lots["NO"])
    open_cost = lot_cost(lots["YES"]) + lot_cost(lots["NO"])
    pair_qty = float(metrics["pair_qty"])
    return float(metrics["pair_pnl"]) - float(metrics["official_taker_fee"]) - open_cost - 0.01 * (
        2.0 * pair_qty + exposure_qty
    )


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


def pair_priority_index(lots: deque[Lot], profile: Profile) -> int:
    if profile.pair_inventory_priority == "fifo" or len(lots) <= 1:
        return 0
    if profile.pair_inventory_priority != "ledger_tail_first":
        raise ValueError(f"unsupported pair_inventory_priority={profile.pair_inventory_priority!r}")

    def score(item: tuple[int, Lot]) -> tuple[float, float, float, float, float, int]:
        idx, lot = item
        risk_direction_score = 1.0 if lot.source_risk_direction == "repair_or_pairing_improving" else 0.0
        late_offset_score = 1.0 if lot.offset_s >= 90.0 else 0.0
        deficit_qty = max(0.0, lot.pre_seed_opp_qty - lot.pre_seed_same_qty)
        ledger_after_stress = -lot.ledger_proxy_after
        return (
            risk_direction_score,
            late_offset_score,
            deficit_qty,
            ledger_after_stress,
            lot.offset_s,
            -idx,
        )

    return max(enumerate(lots), key=score)[0]


def pair_inventory(
    profile: Profile,
    state: State,
    metrics: defaultdict[str, float],
    ts_ms: int,
    close_recorder: SelectedCloseActionRecorder | None = None,
    trigger_lot: Lot | None = None,
    seed_ledger_context_recorder: SelectedSeedLedgerContextRecorder | None = None,
) -> None:
    yes = state.lots["YES"]
    no = state.lots["NO"]
    while yes and no:
        yes_idx = pair_priority_index(yes, profile)
        no_idx = pair_priority_index(no, profile)
        yes_lot = yes[yes_idx]
        no_lot = no[no_idx]
        take = min(yes_lot.qty, no_lot.qty)
        if take <= DUST:
            break
        pair_cost = yes_lot.px + no_lot.px
        older_ts = min(yes_lot.ts_ms, no_lot.ts_ms)
        if close_recorder is not None and trigger_lot is not None:
            if yes_lot is trigger_lot:
                close_recorder.record(
                    profile=profile,
                    state=state,
                    ts_ms=ts_ms,
                    trigger_lot=yes_lot,
                    residual_lot=no_lot,
                    yes_lot=yes_lot,
                    no_lot=no_lot,
                    take=take,
                    pair_cost=pair_cost,
                )
            elif no_lot is trigger_lot:
                close_recorder.record(
                    profile=profile,
                    state=state,
                    ts_ms=ts_ms,
                    trigger_lot=no_lot,
                    residual_lot=yes_lot,
                    yes_lot=yes_lot,
                    no_lot=no_lot,
                    take=take,
                    pair_cost=pair_cost,
                )
        add_count(metrics, state.day, "pair_actions")
        add_metric(metrics, state.day, "pair_qty", take)
        add_metric(metrics, state.day, "pair_cost_sum", take * pair_cost)
        add_metric(metrics, state.day, "pair_pnl", take * (1.0 - pair_cost))
        add_metric(metrics, state.day, "pair_delay_ms", take * max(0, ts_ms - older_ts))
        pair_pnl = take * (1.0 - pair_cost)
        for source_lot in (yes_lot, no_lot):
            source_lot.source_pair_qty += take
            source_lot.source_pair_cost += take * pair_cost
            source_lot.source_pair_pnl += pair_pnl
            if seed_ledger_context_recorder is not None:
                seed_ledger_context_recorder.record_pair(profile, source_lot, take, pair_cost)
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
            condition_id=condition_id,
        )
        states[condition_id] = state
    elif state.winner_side is None and row.get("winner_side"):
        state.winner_side = str(row["winner_side"])
    state.last_ts_ms = max(state.last_ts_ms, int(row["ts_ms"]))
    return state


def maybe_seed(
    row: dict[str, Any],
    profile: Profile,
    state: State,
    metrics: defaultdict[str, float],
    close_recorder: SelectedCloseActionRecorder | None = None,
    seed_ledger_context_recorder: SelectedSeedLedgerContextRecorder | None = None,
) -> None:
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
    risk_increasing_seed = same_qty >= opp_qty
    if (
        profile.risk_increasing_public_trade_px_hi is not None
        and trade_px > profile.risk_increasing_public_trade_px_hi
        and (
            profile.risk_increasing_public_trade_px_hi_side is None
            or side == profile.risk_increasing_public_trade_px_hi_side
        )
        and risk_increasing_seed
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
    ledger_proxy_before = ledger_proxy(metrics, state.lots)
    effective_imbalance_qty_cap = (
        profile.risk_increasing_imbalance_qty_cap
        if risk_increasing_seed and profile.risk_increasing_imbalance_qty_cap is not None
        else profile.imbalance_qty_cap
    )
    imbalance_room = effective_imbalance_qty_cap - max(0.0, same_qty - opp_qty)
    if imbalance_room <= profile.dust_qty:
        if risk_increasing_seed and profile.risk_increasing_imbalance_qty_cap is not None:
            add_count(metrics, day, "seed_block_dynamic_imbalance_qty")
        else:
            add_count(metrics, day, "seed_block_imbalance_qty")
        return
    base_qty = min(
        profile.max_seed_qty,
        trade_size * profile.fill_haircut,
        profile.target_qty - same_qty,
        (profile.max_open_cost - open_cost) / max(seed_px, 1e-9),
        imbalance_room,
    )
    late_fill_to_balance_active = (
        profile.late_repair_fill_to_balance_after_s is not None
        and offset_s >= profile.late_repair_fill_to_balance_after_s
        and same_qty < opp_qty
    )
    qty = base_qty
    risk_increasing_pair_fill_cap_active = (
        profile.risk_increasing_pair_fill_cap and same_qty > opp_qty + DUST
    )
    if risk_increasing_pair_fill_cap_active:
        capped_qty = min(qty, max(0.0, opp_qty))
        add_count(metrics, day, "seed_risk_increasing_pair_fill_cap_candidate")
        if capped_qty < qty - DUST:
            add_count(metrics, day, "seed_qty_cap_risk_increasing_pair_fill")
            add_metric(metrics, day, "seed_risk_increasing_pair_fill_qty_reduction", qty - capped_qty)
        qty = capped_qty
    complete_set_dust_opp_late_repair_sizer_active = (
        profile.complete_set_dust_opp_late_repair_sizer
        and offset_s >= profile.complete_set_dust_opp_late_repair_after_s
        and not risk_increasing_seed
        and 0.0 < opp_qty < profile.dust_qty
    )
    if complete_set_dust_opp_late_repair_sizer_active:
        capped_qty = min(qty, opp_qty + profile.complete_set_dust_opp_late_repair_qty_buffer)
        add_count(metrics, day, "seed_complete_set_dust_opp_late_repair_sizer_candidate")
        if capped_qty < qty - DUST:
            add_count(metrics, day, "seed_qty_cap_complete_set_dust_opp_late_repair_sizer")
            add_metric(metrics, day, "seed_complete_set_dust_opp_late_repair_qty_reduction", qty - capped_qty)
        qty = capped_qty
    if late_fill_to_balance_active:
        deficit_qty = max(0.0, opp_qty - same_qty)
        capped_qty = min(qty, deficit_qty)
        add_count(metrics, day, "seed_late_fill_to_balance_candidate")
        if capped_qty < qty - DUST:
            add_count(metrics, day, "seed_qty_cap_late_fill_to_balance")
            add_metric(metrics, day, "seed_late_fill_to_balance_qty_reduction", qty - capped_qty)
        qty = capped_qty
    if qty <= profile.dust_qty:
        if risk_increasing_pair_fill_cap_active:
            add_count(metrics, day, "seed_block_risk_increasing_pair_fill_cap_qty")
        if late_fill_to_balance_active:
            add_count(metrics, day, "seed_block_late_fill_to_balance_qty")
        if complete_set_dust_opp_late_repair_sizer_active:
            add_count(metrics, day, "seed_block_complete_set_dust_opp_late_repair_sizer_qty")
        return

    seed_lot = Lot(
        qty=qty,
        px=seed_px,
        ts_ms=ts_ms,
        side=side,
        source_candidate_row_id=str(row["candidate_row_id"]),
        lot_id=f"residual_lot:{day}:{row['candidate_row_id']}",
        original_qty=qty,
        source_label=str(row.get("source_label") or ""),
        offset_s=offset_s,
        trigger_px=trade_px,
        trigger_size=trade_size,
        source_risk_direction="risk_increasing" if risk_increasing_seed else "repair_or_pairing_improving",
        pre_seed_same_qty=same_qty,
        pre_seed_opp_qty=opp_qty,
        pre_seed_same_cost=same_cost,
        pre_seed_opp_cost=opp_cost,
        ledger_proxy_before=ledger_proxy_before,
    )
    state.lots[side].append(seed_lot)
    state.last_seed_ts_ms = ts_ms
    state.active = True
    fee = official_taker_fee(qty, seed_px, profile.official_fee_rate)
    add_count(metrics, day, "seed_actions")
    add_metric(metrics, day, "gross_buy_qty", qty)
    add_metric(metrics, day, "gross_buy_cost", qty * seed_px)
    add_metric(metrics, day, "official_taker_fee", fee)
    seed_lot.ledger_proxy_after = ledger_proxy(metrics, state.lots)
    if seed_ledger_context_recorder is not None:
        seed_ledger_context_recorder.record_seed(
            profile=profile,
            state=state,
            row=row,
            seed_lot=seed_lot,
            official_fee=fee,
        )
    pair_inventory(
        profile,
        state,
        metrics,
        ts_ms,
        close_recorder=close_recorder,
        trigger_lot=seed_lot,
        seed_ledger_context_recorder=seed_ledger_context_recorder,
    )


def settle(
    states: dict[str, State],
    profile: Profile,
    metrics: defaultdict[str, float],
    seed_ledger_context_recorder: SelectedSeedLedgerContextRecorder | None = None,
) -> None:
    for state in states.values():
        if not state.active:
            continue
        pair_inventory(
            profile,
            state,
            metrics,
            state.last_ts_ms,
            seed_ledger_context_recorder=seed_ledger_context_recorder,
        )
        add_count(metrics, state.day, "active_markets")
        winner = state.winner_side
        for side in ("YES", "NO"):
            for lot in state.lots[side]:
                if lot.qty <= DUST:
                    continue
                if seed_ledger_context_recorder is not None:
                    seed_ledger_context_recorder.record_residual(profile, lot, state.last_ts_ms)
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
        "seed_block_dynamic_imbalance_qty": int(metrics["seed_block_dynamic_imbalance_qty"]),
        "seed_risk_increasing_pair_fill_cap_candidate": int(
            metrics["seed_risk_increasing_pair_fill_cap_candidate"]
        ),
        "seed_qty_cap_risk_increasing_pair_fill": int(metrics["seed_qty_cap_risk_increasing_pair_fill"]),
        "seed_block_risk_increasing_pair_fill_cap_qty": int(
            metrics["seed_block_risk_increasing_pair_fill_cap_qty"]
        ),
        "seed_risk_increasing_pair_fill_qty_reduction": round(
            float(metrics["seed_risk_increasing_pair_fill_qty_reduction"]), 6
        ),
        "seed_late_fill_to_balance_candidate": int(metrics["seed_late_fill_to_balance_candidate"]),
        "seed_qty_cap_late_fill_to_balance": int(metrics["seed_qty_cap_late_fill_to_balance"]),
        "seed_block_late_fill_to_balance_qty": int(metrics["seed_block_late_fill_to_balance_qty"]),
        "seed_late_fill_to_balance_qty_reduction": round(
            float(metrics["seed_late_fill_to_balance_qty_reduction"]), 6
        ),
        "seed_complete_set_dust_opp_late_repair_sizer_candidate": int(
            metrics["seed_complete_set_dust_opp_late_repair_sizer_candidate"]
        ),
        "seed_qty_cap_complete_set_dust_opp_late_repair_sizer": int(
            metrics["seed_qty_cap_complete_set_dust_opp_late_repair_sizer"]
        ),
        "seed_block_complete_set_dust_opp_late_repair_sizer_qty": int(
            metrics["seed_block_complete_set_dust_opp_late_repair_sizer_qty"]
        ),
        "seed_complete_set_dust_opp_late_repair_qty_reduction": round(
            float(metrics["seed_complete_set_dust_opp_late_repair_qty_reduction"]), 6
        ),
        "seed_block_imbalance_cost": int(metrics["seed_block_imbalance_cost"]),
        "summary_by_day": summary_by_day,
    }


def run_profiles(
    candidate_base_db: Path,
    profiles: list[Profile],
    base_day_counts: dict[str, int],
    close_recorder: SelectedCloseActionRecorder | None = None,
    seed_ledger_context_recorder: SelectedSeedLedgerContextRecorder | None = None,
) -> dict[str, Any]:
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
                maybe_seed(
                    row,
                    profile,
                    state,
                    metrics_by_profile[profile.name],
                    close_recorder=close_recorder,
                    seed_ledger_context_recorder=seed_ledger_context_recorder,
                )
    for profile in profiles:
        settle(
            states_by_profile[profile.name],
            profile,
            metrics_by_profile[profile.name],
            seed_ledger_context_recorder=seed_ledger_context_recorder,
        )
    con.close()
    return {
        "processed_public_sell_rows": row_count,
        "profiles": {
            profile.name: finish_metrics(profile, metrics_by_profile[profile.name], base_day_counts)
            for profile in profiles
        },
        "selected_close_actions": close_recorder.summary() if close_recorder is not None else None,
        "selected_seed_ledger_contexts": (
            seed_ledger_context_recorder.summary() if seed_ledger_context_recorder is not None else None
        ),
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


def research_ranking_from(control: dict[str, Any], variant: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    fee_positive = float(variant["fee_after_pnl"]) > 0.0
    stress_positive = float(variant["stress100_worst_pnl"]) > 0.0
    worst_day_positive = float(variant["worst_day_fee_after_pnl"]) > 0.0
    retention = float(delta["seed_action_retention"] or 0.0)
    pair_retention = float(delta["pair_qty_retention"] or 0.0)
    fee_delta = float(delta["fee_after_pnl"]["absolute_delta"])
    stress_delta = float(delta["stress100_worst_pnl"]["absolute_delta"])
    worst_day_delta = float(delta["worst_day_fee_after_pnl"]["absolute_delta"])
    residual_reduction = float(delta["residual_qty_rate_reduction"] or 0.0)
    residual_cost_reduction = float(delta["residual_cost_rate_reduction"] or 0.0)
    pair_cost_reduction = float(delta["weighted_pair_cost_reduction"] or 0.0)
    risk_adjusted_score_delta = (
        fee_delta
        + stress_delta
        + worst_day_delta
        + 1000.0 * residual_reduction
        + 1000.0 * residual_cost_reduction
    )

    if not (fee_positive and stress_positive and worst_day_positive):
        label = "DISCARD_ECONOMIC_NEGATIVE"
        decision = "DISCARD"
    elif (
        retention >= 0.75
        and pair_retention >= 0.75
        and risk_adjusted_score_delta > 0.0
        and (
            residual_reduction > 0.0
            or residual_cost_reduction > 0.0
            or pair_cost_reduction > 0.0
        )
    ):
        label = "KEEP_ECONOMIC_RESEARCH_ONLY"
        decision = "KEEP"
    elif retention < 0.65 or pair_retention < 0.65:
        label = "TRADEOFF_ECON_POSITIVE_CAPACITY_DROP"
        decision = "UNKNOWN"
    elif residual_reduction < 0.0 or residual_cost_reduction < 0.0:
        label = "TRADEOFF_ECON_POSITIVE_RISK_WORSE"
        decision = "UNKNOWN"
    else:
        label = "TRADEOFF_ECON_POSITIVE_WEAK_INCREMENT"
        decision = "UNKNOWN"

    return {
        "decision_label": decision,
        "label": label,
        "fee_after_pnl_positive": fee_positive,
        "stress100_worst_pnl_positive": stress_positive,
        "worst_day_fee_after_pnl_positive": worst_day_positive,
        "seed_action_retention": retention,
        "pair_qty_retention": pair_retention,
        "fee_after_pnl_delta": round(fee_delta, 6),
        "stress100_worst_pnl_delta": round(stress_delta, 6),
        "worst_day_fee_after_pnl_delta": round(worst_day_delta, 6),
        "residual_qty_rate_reduction": residual_reduction,
        "residual_cost_rate_reduction": residual_cost_reduction,
        "weighted_pair_cost_reduction": pair_cost_reduction,
        "risk_adjusted_score_delta_proxy": round(risk_adjusted_score_delta, 6),
    }


def fill_to_balance_ranking(
    late_repair: dict[str, Any],
    dynamic: dict[str, Any],
    fill: dict[str, Any],
    fill_vs_late: dict[str, Any],
    fill_vs_dynamic: dict[str, Any],
) -> dict[str, Any]:
    fee_positive = float(fill["fee_after_pnl"]) > 0.0
    stress_positive = float(fill["stress100_worst_pnl"]) > 0.0
    worst_day_positive = float(fill["worst_day_fee_after_pnl"]) > 0.0
    added_seed_total = float(dynamic["seed_actions"]) - float(late_repair["seed_actions"])
    added_pair_total = float(dynamic["pair_qty"]) - float(late_repair["pair_qty"])
    added_fee_total = float(dynamic["fee_after_pnl"]) - float(late_repair["fee_after_pnl"])
    added_seed_kept = float(fill["seed_actions"]) - float(late_repair["seed_actions"])
    added_pair_kept = float(fill["pair_qty"]) - float(late_repair["pair_qty"])
    added_fee_kept = float(fill["fee_after_pnl"]) - float(late_repair["fee_after_pnl"])
    seed_added_retention = added_seed_kept / added_seed_total if added_seed_total > 0.0 else None
    pair_added_retention = added_pair_kept / added_pair_total if added_pair_total > 0.0 else None
    fee_added_retention = added_fee_kept / added_fee_total if added_fee_total > 0.0 else None
    dynamic_qty_excess = float(dynamic["residual_qty_rate"]) - float(late_repair["residual_qty_rate"])
    fill_qty_excess = float(fill["residual_qty_rate"]) - float(late_repair["residual_qty_rate"])
    dynamic_cost_excess = float(dynamic["residual_cost_rate"]) - float(late_repair["residual_cost_rate"])
    fill_cost_excess = float(fill["residual_cost_rate"]) - float(late_repair["residual_cost_rate"])
    residual_excess_reduction = (
        1.0 - fill_qty_excess / dynamic_qty_excess if dynamic_qty_excess > 0.0 else None
    )
    residual_cost_excess_reduction = (
        1.0 - fill_cost_excess / dynamic_cost_excess if dynamic_cost_excess > 0.0 else None
    )
    dynamic_stress_gap = float(late_repair["stress100_worst_pnl"]) - float(dynamic["stress100_worst_pnl"])
    fill_stress_gap = float(late_repair["stress100_worst_pnl"]) - float(fill["stress100_worst_pnl"])
    stress_gap_reduction = 1.0 - fill_stress_gap / dynamic_stress_gap if dynamic_stress_gap > 0.0 else None

    if not (fee_positive and stress_positive and worst_day_positive):
        decision = "DISCARD"
        label = "DISCARD_ECONOMIC_NEGATIVE"
    elif (
        (seed_added_retention is not None and seed_added_retention >= 0.50)
        and (pair_added_retention is not None and pair_added_retention >= 0.50)
        and (fee_added_retention is not None and fee_added_retention >= 0.50)
        and (residual_excess_reduction is not None and residual_excess_reduction >= 0.50)
        and (residual_cost_excess_reduction is not None and residual_cost_excess_reduction >= 0.50)
        and (stress_gap_reduction is not None and stress_gap_reduction >= 0.25)
    ):
        decision = "KEEP"
        label = "KEEP_FILL_TO_BALANCE_RESEARCH_ONLY"
    elif (
        float(fill["fee_after_pnl"]) > float(late_repair["fee_after_pnl"])
        and float(fill["residual_qty_rate"]) <= float(dynamic["residual_qty_rate"])
        and float(fill["residual_cost_rate"]) <= float(dynamic["residual_cost_rate"])
    ):
        decision = "UNKNOWN"
        label = "TRADEOFF_ECON_POSITIVE_RISK_IMPROVED_BUT_NOT_KEEP"
    else:
        decision = "UNKNOWN"
        label = "TRADEOFF_ECON_POSITIVE_WEAK_OR_RISK_WORSE"

    return {
        "decision_label": decision,
        "label": label,
        "fee_after_pnl_positive": fee_positive,
        "stress100_worst_pnl_positive": stress_positive,
        "worst_day_fee_after_pnl_positive": worst_day_positive,
        "seed_added_retention_vs_dynamic": seed_added_retention,
        "pair_qty_added_retention_vs_dynamic": pair_added_retention,
        "fee_after_pnl_added_retention_vs_dynamic": fee_added_retention,
        "residual_qty_excess_reduction_vs_dynamic": residual_excess_reduction,
        "residual_cost_excess_reduction_vs_dynamic": residual_cost_excess_reduction,
        "stress_gap_reduction_vs_dynamic": stress_gap_reduction,
        "fill_vs_late_seed_action_retention": fill_vs_late.get("seed_action_retention"),
        "fill_vs_late_pair_qty_retention": fill_vs_late.get("pair_qty_retention"),
        "fill_vs_late_residual_qty_rate_reduction": fill_vs_late.get("residual_qty_rate_reduction"),
        "fill_vs_late_residual_cost_rate_reduction": fill_vs_late.get("residual_cost_rate_reduction"),
        "fill_vs_dynamic_seed_action_retention": fill_vs_dynamic.get("seed_action_retention"),
        "fill_vs_dynamic_pair_qty_retention": fill_vs_dynamic.get("pair_qty_retention"),
        "fill_vs_dynamic_residual_qty_rate_reduction": fill_vs_dynamic.get("residual_qty_rate_reduction"),
        "fill_vs_dynamic_residual_cost_rate_reduction": fill_vs_dynamic.get("residual_cost_rate_reduction"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", default=str(DEFAULT_CANDIDATE_BASE_DIR))
    parser.add_argument("--baseline-result-dir", default=str(DEFAULT_BASELINE_RESULT_DIR))
    parser.add_argument("--output-dir")
    parser.add_argument(
        "--selected-close-actions-output",
        help="Default-off CSV export path for selected_close_action_export_v1 rows.",
    )
    parser.add_argument(
        "--selected-close-actions-profile",
        default="variant_late_repair_only_after_90",
        help="Profile to sample for selected close-action export, or 'all'.",
    )
    parser.add_argument("--selected-close-actions-sample-cap", type=int, default=30)
    parser.add_argument("--selected-close-actions-per-day-cap", type=int, default=2)
    parser.add_argument(
        "--selected-close-actions-selection-mode",
        choices=("first", "diversified"),
        default="first",
        help=(
            "Default 'first' preserves the legacy first-N export. 'diversified' collects eligible close rows "
            "and deterministically samples distinct condition_id and close_side within day caps."
        ),
    )
    parser.add_argument(
        "--selected-close-actions-fee-rate-source",
        default=(
            "state_machine_result_config:"
            "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
            "20260502_20260518_publicfull_v2:official_fee_rate=0.07"
        ),
    )
    parser.add_argument(
        "--selected-seed-ledger-context-output",
        help="Default-off CSV export path for selected_seed_ledger_context_export_v1 rows.",
    )
    parser.add_argument(
        "--selected-seed-ledger-context-profile",
        default="variant_late_repair_only_after_90",
        help="Profile to sample for selected seed ledger-context export, or 'all'.",
    )
    parser.add_argument("--selected-seed-ledger-context-sample-cap", type=int, default=60)
    parser.add_argument("--selected-seed-ledger-context-per-day-cap", type=int, default=4)
    parser.add_argument(
        "--selected-seed-ledger-context-selection-mode",
        choices=("first", "residual_tail"),
        default="residual_tail",
        help=(
            "Default 'residual_tail' selects highest residual-cost seed contexts after settlement. "
            "'first' keeps deterministic first rows by day/time."
        ),
    )
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

    selected_close_actions_path = Path(args.selected_close_actions_output) if args.selected_close_actions_output else None
    close_recorder = (
        SelectedCloseActionRecorder(
            profile_name=args.selected_close_actions_profile,
            fee_rate_source=args.selected_close_actions_fee_rate_source,
            sample_cap=max(0, int(args.selected_close_actions_sample_cap)),
            per_day_cap=max(0, int(args.selected_close_actions_per_day_cap)),
            selection_mode=args.selected_close_actions_selection_mode,
        )
        if selected_close_actions_path is not None
        else None
    )
    selected_seed_ledger_context_path = (
        Path(args.selected_seed_ledger_context_output) if args.selected_seed_ledger_context_output else None
    )
    seed_ledger_context_recorder = (
        SelectedSeedLedgerContextRecorder(
            profile_name=args.selected_seed_ledger_context_profile,
            sample_cap=max(0, int(args.selected_seed_ledger_context_sample_cap)),
            per_day_cap=max(0, int(args.selected_seed_ledger_context_per_day_cap)),
            selection_mode=args.selected_seed_ledger_context_selection_mode,
        )
        if selected_seed_ledger_context_path is not None
        else None
    )

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
        Profile(
            name="variant_late_repair90_dynamic_risk_imbalance_cap_1p10",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            risk_increasing_imbalance_qty_cap=1.10,
        ),
        Profile(
            name="variant_late_repair90_fill_to_balance_after_90",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            late_repair_fill_to_balance_after_s=90.0,
        ),
        Profile(
            name="variant_late_repair90_dynamic_imbalance_fill_to_balance_after_90",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            risk_increasing_imbalance_qty_cap=1.10,
            late_repair_fill_to_balance_after_s=90.0,
        ),
    ]
    run_result = run_profiles(
        candidate_db_path,
        profiles,
        base_day_counts,
        close_recorder=close_recorder,
        seed_ledger_context_recorder=seed_ledger_context_recorder,
    )
    if selected_close_actions_path is not None:
        assert close_recorder is not None
        close_recorder.finalize_selection()
        write_csv(selected_close_actions_path, close_recorder.rows or [], SELECTED_CLOSE_ACTION_FIELDS)
    if selected_seed_ledger_context_path is not None:
        assert seed_ledger_context_recorder is not None
        seed_ledger_context_recorder.finalize_selection()
        write_csv(
            selected_seed_ledger_context_path,
            seed_ledger_context_recorder.rows or [],
            SELECTED_SEED_LEDGER_CONTEXT_FIELDS,
        )
    control = run_result["profiles"]["control_seed_offset_max_120"]
    hard_offset90 = run_result["profiles"]["discarded_hard_seed_offset_max_90"]
    variant = run_result["profiles"]["variant_late_repair_only_after_90"]
    public_px_variant = run_result["profiles"]["variant_late_repair90_public_trade_px_hi_0p55"]
    risk_px_variant = run_result["profiles"]["variant_late_repair90_risk_public_trade_px_hi_0p55"]
    no_side_risk_px_variant = run_result["profiles"]["variant_late_repair90_no_side_risk_public_trade_px_hi_0p55"]
    dynamic_imbalance_variant = run_result["profiles"]["variant_late_repair90_dynamic_risk_imbalance_cap_1p10"]
    fill_to_balance_variant = run_result["profiles"]["variant_late_repair90_fill_to_balance_after_90"]
    dynamic_fill_to_balance_variant = run_result["profiles"][
        "variant_late_repair90_dynamic_imbalance_fill_to_balance_after_90"
    ]
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
    dynamic_imbalance_delta = build_direct_delta(variant, dynamic_imbalance_variant)
    fill_to_balance_delta = build_direct_delta(variant, fill_to_balance_variant)
    dynamic_fill_to_balance_delta = build_direct_delta(variant, dynamic_fill_to_balance_variant)
    dynamic_fill_vs_dynamic_delta = build_direct_delta(dynamic_imbalance_variant, dynamic_fill_to_balance_variant)
    hard_cap_decision_label, hard_cap_status = decision_public_px_cap(variant, public_px_variant, public_px_delta)
    all_side_risk_decision_label, all_side_risk_status = decision_risk_px_cap(
        variant, risk_px_variant, risk_px_delta
    )
    side_risk_decision_label, side_risk_status = decision_side_risk_px_cap(
        variant, no_side_risk_px_variant, no_side_risk_px_delta
    )
    dynamic_imbalance_research_ranking = research_ranking_from(
        variant, dynamic_imbalance_variant, dynamic_imbalance_delta
    )
    fill_to_balance_research_ranking = research_ranking_from(variant, fill_to_balance_variant, fill_to_balance_delta)
    dynamic_fill_to_balance_research_ranking = fill_to_balance_ranking(
        variant,
        dynamic_imbalance_variant,
        dynamic_fill_to_balance_variant,
        dynamic_fill_to_balance_delta,
        dynamic_fill_vs_dynamic_delta,
    )
    research_ranking = fill_to_balance_research_ranking
    decision_label = research_ranking["decision_label"]
    status = research_ranking["label"]
    manifest = {
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "causal_verifier",
        "decision_label": decision_label,
        "status": status,
        "hypothesis": (
            "Dynamic imbalance added sample and fee-after PnL but attributed residual to late small-deficit repair "
            "seeds overfilling at full size. A fill-to-balance rule should cap late repair seed qty to the current "
            "opposite_qty-same_qty deficit; the primary test applies it to late_repair90, with dynamic+fill kept as "
            "a contrast for whether the earlier dynamic capacity can be salvaged."
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
            "dynamic_risk_imbalance_cap_variant": asdict(profiles[6]),
            "late_repair90_fill_to_balance_variant": asdict(profiles[7]),
            "dynamic_imbalance_fill_to_balance_variant": asdict(profiles[8]),
            "candidate_source": "candidate_base table, public_sell rows with 0 <= offset_s < 300",
            "official_fee_formula": "fee = shares * fee_rate * price * (1 - price)",
            "official_fee_rate": profiles[0].official_fee_rate,
            "selected_close_actions_export_enabled": selected_close_actions_path is not None,
            "selected_close_actions_profile": args.selected_close_actions_profile,
            "selected_close_actions_sample_cap": int(args.selected_close_actions_sample_cap),
            "selected_close_actions_per_day_cap": int(args.selected_close_actions_per_day_cap),
            "selected_close_actions_selection_mode": args.selected_close_actions_selection_mode,
            "selected_close_actions_fee_rate_source": args.selected_close_actions_fee_rate_source,
            "selected_seed_ledger_context_export_enabled": selected_seed_ledger_context_path is not None,
            "selected_seed_ledger_context_profile": args.selected_seed_ledger_context_profile,
            "selected_seed_ledger_context_sample_cap": int(args.selected_seed_ledger_context_sample_cap),
            "selected_seed_ledger_context_per_day_cap": int(args.selected_seed_ledger_context_per_day_cap),
            "selected_seed_ledger_context_selection_mode": args.selected_seed_ledger_context_selection_mode,
        },
        "selected_close_actions_export": {
            "enabled": selected_close_actions_path is not None,
            "path": str(selected_close_actions_path) if selected_close_actions_path is not None else None,
            "fields": SELECTED_CLOSE_ACTION_FIELDS if selected_close_actions_path is not None else [],
            "summary": run_result.get("selected_close_actions"),
            "default_behavior_unchanged_when_unset": True,
            "source_truth_boundary": (
                "selected_close_actions rows are only selected candidate close actions for future bounded "
                "replay_store_v2 lookup; this export is not replay discovery, private truth, deployable, "
                "shadow-ready, canary, or promotion evidence."
            ),
        },
        "selected_seed_ledger_context_export": {
            "enabled": selected_seed_ledger_context_path is not None,
            "path": str(selected_seed_ledger_context_path) if selected_seed_ledger_context_path is not None else None,
            "fields": SELECTED_SEED_LEDGER_CONTEXT_FIELDS if selected_seed_ledger_context_path is not None else [],
            "summary": run_result.get("selected_seed_ledger_contexts"),
            "default_behavior_unchanged_when_unset": True,
            "external_shadow_ids_available": False,
            "source_truth_boundary": (
                "selected_seed_ledger_context rows are local candidate-base/state-machine seed context exports for "
                "future residual_tail_ledger_context_verifier_v1 joins. They do not fabricate quote/order/source "
                "sequence ids, do not query replay_store_v2, and are not private truth, deployable, shadow-ready, "
                "canary, or promotion evidence."
            ),
        },
        "processed_public_sell_rows": run_result["processed_public_sell_rows"],
        "baseline_reported": baseline,
        "control_rerun": control,
        "discarded_hard_offset90_rerun": hard_offset90,
        "variant_late_repair90_rerun": variant,
        "variant_late_repair90_public_trade_px_hi_0p55_rerun": public_px_variant,
        "variant_late_repair90_risk_public_trade_px_hi_0p55_rerun": risk_px_variant,
        "variant_late_repair90_no_side_risk_public_trade_px_hi_0p55_rerun": no_side_risk_px_variant,
        "variant_late_repair90_dynamic_risk_imbalance_cap_1p10_rerun": dynamic_imbalance_variant,
        "variant_late_repair90_fill_to_balance_after_90_rerun": fill_to_balance_variant,
        "variant_late_repair90_dynamic_imbalance_fill_to_balance_after_90_rerun": dynamic_fill_to_balance_variant,
        "hard_offset90_comparison": hard_offset90_comparison,
        "late_repair90_comparison": comparison,
        "late_repair90_decision_label": late_repair_decision_label,
        "late_repair90_status": late_repair_status,
        "hard_public_px_cap_decision_label": hard_cap_decision_label,
        "hard_public_px_cap_status": hard_cap_status,
        "all_side_risk_public_px_cap_decision_label": all_side_risk_decision_label,
        "all_side_risk_public_px_cap_status": all_side_risk_status,
        "no_side_risk_public_px_cap_decision_label": side_risk_decision_label,
        "no_side_risk_public_px_cap_status": side_risk_status,
        "public_px_cap_delta_vs_late_repair90": public_px_delta,
        "risk_public_px_cap_delta_vs_late_repair90": risk_px_delta,
        "no_side_risk_public_px_cap_delta_vs_late_repair90": no_side_risk_px_delta,
        "dynamic_imbalance_delta_vs_late_repair90": dynamic_imbalance_delta,
        "fill_to_balance_delta_vs_late_repair90": fill_to_balance_delta,
        "dynamic_imbalance_fill_to_balance_delta_vs_late_repair90": dynamic_fill_to_balance_delta,
        "dynamic_imbalance_fill_to_balance_delta_vs_dynamic_imbalance": dynamic_fill_vs_dynamic_delta,
        "dynamic_imbalance_research_ranking": dynamic_imbalance_research_ranking,
        "fill_to_balance_research_ranking": fill_to_balance_research_ranking,
        "dynamic_imbalance_fill_to_balance_research_ranking": dynamic_fill_to_balance_research_ranking,
        "research_ranking": research_ranking,
        "promotion_gate": {
            "passed": False,
            "required_before_g2_canary": True,
            "reason": "local completion candidate-pipeline state-machine research only",
            "deployable": False,
            "can_support_strategy_promotion": False,
            "requires_no_order_shadow": decision_label == "KEEP",
            "requires_source_of_truth_replay": True,
        },
        "scoreboard_delta": {
            "control_seed_actions": control["seed_actions"],
            "late_repair90_seed_actions": variant["seed_actions"],
            "dynamic_imbalance_seed_actions": dynamic_imbalance_variant["seed_actions"],
            "fill_to_balance_seed_actions": fill_to_balance_variant["seed_actions"],
            "dynamic_fill_to_balance_seed_actions": dynamic_fill_to_balance_variant["seed_actions"],
            "dynamic_imbalance_seed_action_retention_vs_late_repair90": dynamic_imbalance_delta[
                "seed_action_retention"
            ],
            "late_repair90_fee_after_pnl": variant["fee_after_pnl"],
            "dynamic_imbalance_fee_after_pnl": dynamic_imbalance_variant["fee_after_pnl"],
            "fill_to_balance_fee_after_pnl": fill_to_balance_variant["fee_after_pnl"],
            "dynamic_fill_to_balance_fee_after_pnl": dynamic_fill_to_balance_variant["fee_after_pnl"],
            "late_repair90_stress100_worst_pnl": variant["stress100_worst_pnl"],
            "dynamic_imbalance_stress100_worst_pnl": dynamic_imbalance_variant["stress100_worst_pnl"],
            "fill_to_balance_stress100_worst_pnl": fill_to_balance_variant["stress100_worst_pnl"],
            "dynamic_fill_to_balance_stress100_worst_pnl": dynamic_fill_to_balance_variant[
                "stress100_worst_pnl"
            ],
            "late_repair90_worst_day_fee_after_pnl": variant["worst_day_fee_after_pnl"],
            "dynamic_imbalance_worst_day_fee_after_pnl": dynamic_imbalance_variant[
                "worst_day_fee_after_pnl"
            ],
            "fill_to_balance_worst_day_fee_after_pnl": fill_to_balance_variant["worst_day_fee_after_pnl"],
            "dynamic_fill_to_balance_worst_day_fee_after_pnl": dynamic_fill_to_balance_variant[
                "worst_day_fee_after_pnl"
            ],
            "late_repair90_weighted_pair_cost": variant["weighted_pair_cost"],
            "dynamic_imbalance_weighted_pair_cost": dynamic_imbalance_variant["weighted_pair_cost"],
            "dynamic_fill_to_balance_weighted_pair_cost": dynamic_fill_to_balance_variant["weighted_pair_cost"],
            "weighted_pair_cost_reduction": dynamic_imbalance_delta["weighted_pair_cost_reduction"],
            "late_repair90_residual_qty_rate": variant["residual_qty_rate"],
            "dynamic_imbalance_residual_qty_rate": dynamic_imbalance_variant["residual_qty_rate"],
            "fill_to_balance_residual_qty_rate": fill_to_balance_variant["residual_qty_rate"],
            "dynamic_fill_to_balance_residual_qty_rate": dynamic_fill_to_balance_variant["residual_qty_rate"],
            "residual_qty_rate_reduction": dynamic_imbalance_delta["residual_qty_rate_reduction"],
            "late_repair90_residual_cost_rate": variant["residual_cost_rate"],
            "dynamic_imbalance_residual_cost_rate": dynamic_imbalance_variant["residual_cost_rate"],
            "fill_to_balance_residual_cost_rate": fill_to_balance_variant["residual_cost_rate"],
            "dynamic_fill_to_balance_residual_cost_rate": dynamic_fill_to_balance_variant["residual_cost_rate"],
            "residual_cost_rate_reduction": dynamic_imbalance_delta["residual_cost_rate_reduction"],
            "dynamic_imbalance_block_count": dynamic_imbalance_variant["seed_block_dynamic_imbalance_qty"],
            "fill_to_balance_qty_cap_count": fill_to_balance_variant["seed_qty_cap_late_fill_to_balance"],
            "fill_to_balance_block_count": fill_to_balance_variant["seed_block_late_fill_to_balance_qty"],
            "fill_to_balance_qty_reduction": fill_to_balance_variant["seed_late_fill_to_balance_qty_reduction"],
            "dynamic_fill_to_balance_qty_cap_count": dynamic_fill_to_balance_variant[
                "seed_qty_cap_late_fill_to_balance"
            ],
            "dynamic_fill_to_balance_block_count": dynamic_fill_to_balance_variant[
                "seed_block_late_fill_to_balance_qty"
            ],
            "dynamic_fill_to_balance_qty_reduction": dynamic_fill_to_balance_variant[
                "seed_late_fill_to_balance_qty_reduction"
            ],
            "discarded_all_side_risk_public_px_cap_seed_actions": risk_px_variant["seed_actions"],
            "discarded_all_side_risk_public_px_cap_residual_qty_rate": risk_px_variant["residual_qty_rate"],
            "discarded_hard_public_px_cap_seed_actions": public_px_variant["seed_actions"],
            "discarded_hard_public_px_cap_residual_qty_rate": public_px_variant["residual_qty_rate"],
            "discarded_no_side_risk_public_px_cap_seed_actions": no_side_risk_px_variant["seed_actions"],
            "discarded_no_side_risk_public_px_cap_residual_qty_rate": no_side_risk_px_variant["residual_qty_rate"],
        },
        "interpretation": [
            "This is a local candidate-base state-machine rerun, not source-of-truth replay and not deployable evidence.",
            "The control profile is included to expose implementation drift against the official pipeline result.",
            "The discarded hard offset90 profile is retained only as a contrast against the repair-only late gate.",
            "The late-repair90 variant is retained as the control for the new source-public-price cap.",
            "The hard public price cap is retained only as a discarded contrast.",
            "The all-side risk-increasing public price cap is retained only as a discarded contrast.",
            "The NO-side-only risk-increasing public price cap is retained only as a discarded contrast.",
            "The dynamic imbalance variant caps only risk-increasing seed qty; underweight/repair seeds still use full target_qty=5.",
            "The primary fill-to-balance variant leaves late_repair90 otherwise unchanged and caps late underweight repair seed qty to the live deficit.",
            "The dynamic+fill variant tests whether the prior dynamic imbalance capacity gain can be salvaged; it is a contrast, not the primary decision.",
        ],
        "next_action": (
            "If KEEP, add default-off fill-to-balance support to the no-order shadow runner and local smoke it before "
            "spending another EC2 shadow; if TRADEOFF/UNKNOWN, inspect whether the retained extra sample is too small "
            "or whether another non-price inventory mechanism is needed."
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
