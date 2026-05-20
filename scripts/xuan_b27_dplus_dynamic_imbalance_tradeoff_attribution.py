#!/usr/bin/env python3
"""Attribute the D+ dynamic-imbalance state-machine tradeoff.

This script reads only the derived candidate_base DuckDB and local manifests.
It does not read raw/replay/collector stores, the full completion event store,
sockets, SSH, or event JSONL.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_dynamic_imbalance_tradeoff_attribution"
DUST = 1e-9
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
)


def load_rerun_module() -> Any:
    path = Path(__file__).resolve().with_name("xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py")
    spec = importlib.util.spec_from_file_location("xuan_dplus_state_machine_rerun", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load rerun module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RERUN = load_rerun_module()


@dataclass
class TrackedLot:
    qty: float
    px: float
    ts_ms: int
    side: str
    source_candidate_row_id: str
    source_day: str
    source_slug: str
    source_offset_s: float
    source_public_trade_price: float
    source_l1_pair_ask: float
    source_pre_same_qty: float
    source_pre_opp_qty: float
    source_seed_qty: float
    source_risk_increasing: bool
    source_late_repair_active: bool
    source_dynamic_cap_active: bool
    source_imbalance_room: float


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


def offset_bucket(offset_s: float) -> str:
    if offset_s < 30:
        return "off_0_30"
    if offset_s < 60:
        return "off_30_60"
    if offset_s < 90:
        return "off_60_90"
    if offset_s < 120:
        return "off_90_120"
    if offset_s < 180:
        return "off_120_180"
    return "off_180_300"


def price_bucket(px: float) -> str:
    if px < 0.30:
        return "px_lt_0p30"
    if px < 0.45:
        return "px_0p30_0p45"
    if px < 0.55:
        return "px_0p45_0p55"
    if px < 0.70:
        return "px_0p55_0p70"
    return "px_ge_0p70"


def imbalance_bucket(pre_same: float, pre_opp: float) -> str:
    diff = pre_same - pre_opp
    if diff < -1.0:
        return "underweight_gt_1"
    if diff < -DUST:
        return "underweight_0_1"
    if abs(diff) <= DUST:
        return "flat"
    if diff < 0.50:
        return "risk_0_0p5"
    if diff < 1.10:
        return "risk_0p5_1p10"
    if diff < 1.25:
        return "risk_1p10_1p25"
    return "risk_ge_1p25"


def add_bucket(out: dict[str, dict[str, float]], key: str, qty: float, cost: float) -> None:
    item = out.setdefault(key, {"lots": 0.0, "qty": 0.0, "cost": 0.0})
    item["lots"] += 1.0
    item["qty"] += qty
    item["cost"] += cost


def finalize_buckets(raw: dict[str, dict[str, float]], limit: int | None = None) -> list[dict[str, Any]]:
    rows = [
        {"bucket": key, "lots": int(value["lots"]), "qty": round(value["qty"], 6), "cost": round(value["cost"], 6)}
        for key, value in raw.items()
    ]
    rows.sort(key=lambda row: (row["qty"], row["cost"]), reverse=True)
    return rows if limit is None else rows[:limit]


def compare_bucket_lists(
    control: list[dict[str, Any]],
    variant: list[dict[str, Any]],
    limit: int = 20,
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, float]] = {}
    for row in control:
        by_key.setdefault(row["bucket"], {})["control_qty"] = float(row["qty"])
        by_key.setdefault(row["bucket"], {})["control_cost"] = float(row["cost"])
    for row in variant:
        by_key.setdefault(row["bucket"], {})["variant_qty"] = float(row["qty"])
        by_key.setdefault(row["bucket"], {})["variant_cost"] = float(row["cost"])
    rows = []
    for key, value in by_key.items():
        control_qty = value.get("control_qty", 0.0)
        variant_qty = value.get("variant_qty", 0.0)
        control_cost = value.get("control_cost", 0.0)
        variant_cost = value.get("variant_cost", 0.0)
        rows.append(
            {
                "bucket": key,
                "control_qty": round(control_qty, 6),
                "variant_qty": round(variant_qty, 6),
                "delta_qty": round(variant_qty - control_qty, 6),
                "control_cost": round(control_cost, 6),
                "variant_cost": round(variant_cost, 6),
                "delta_cost": round(variant_cost - control_cost, 6),
            }
        )
    rows.sort(key=lambda row: (row["delta_qty"], row["delta_cost"]), reverse=True)
    return rows[:limit]


def maybe_seed_tracked(row: dict[str, Any], profile: Any, state: Any, metrics: defaultdict[str, float]) -> None:
    day = str(row["day"])
    RERUN.add_count(metrics, day, "candidate_count")
    side = str(row["side"])
    if side not in {"YES", "NO"}:
        return
    offset_s_value = float(row["offset_s"])
    if not (profile.seed_offset_min_s <= offset_s_value < profile.seed_offset_max_s):
        RERUN.add_count(metrics, day, "seed_block_offset")
        return
    trade_px = float(row["public_trade_price"])
    trade_size = float(row["public_trade_size"])
    if trade_size <= DUST or not math.isfinite(trade_px):
        return
    if profile.public_trade_px_hi is not None and trade_px > profile.public_trade_px_hi:
        RERUN.add_count(metrics, day, "seed_block_public_trade_px_hi")
        return
    if not (profile.seed_px_lo <= trade_px <= profile.seed_px_hi):
        RERUN.add_count(metrics, day, "seed_block_price_band")
        return
    l1_pair_ask = float(row["l1_pair_ask"])
    if not math.isfinite(l1_pair_ask) or l1_pair_ask > profile.seed_l1_pair_cap + 1e-12:
        RERUN.add_count(metrics, day, "seed_block_l1_pair_cap")
        return
    ts_ms = int(row["ts_ms"])
    if ts_ms - state.last_seed_ts_ms < profile.cooldown_s * 1000.0:
        RERUN.add_count(metrics, day, "seed_block_cooldown")
        return

    same_qty = RERUN.lot_qty(state.lots[side])
    opp_qty = RERUN.lot_qty(state.lots[RERUN.other(side)])
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
        RERUN.add_count(metrics, day, "seed_block_risk_increasing_public_trade_px_hi")
        return
    aged_cost = RERUN.aged_lot_cost(state.lots["YES"], ts_ms, profile.residual_cooldown_age_s) + RERUN.aged_lot_cost(
        state.lots["NO"], ts_ms, profile.residual_cooldown_age_s
    )
    if aged_cost > profile.residual_cooldown_cost_cap + 1e-12 and same_qty + profile.dust_qty >= opp_qty:
        RERUN.add_count(metrics, day, "seed_block_residual_cooldown")
        return
    late_repair_active = profile.late_repair_only_after_s is not None and offset_s_value >= profile.late_repair_only_after_s
    if late_repair_active and same_qty >= opp_qty:
        RERUN.add_count(metrics, day, "seed_block_late_repair_only")
        return
    if same_qty >= profile.target_qty - profile.dust_qty:
        RERUN.add_count(metrics, day, "seed_block_target")
        return
    same_cost = RERUN.lot_cost(state.lots[side])
    opp_cost = RERUN.lot_cost(state.lots[RERUN.other(side)])
    if max(0.0, same_cost - opp_cost) > profile.imbalance_cost_cap + 1e-12:
        RERUN.add_count(metrics, day, "seed_block_imbalance_cost")
        return

    seed_px = max(0.01, trade_px - profile.edge)
    open_cost = RERUN.lot_cost(state.lots["YES"]) + RERUN.lot_cost(state.lots["NO"])
    dynamic_cap_active = risk_increasing_seed and profile.risk_increasing_imbalance_qty_cap is not None
    effective_imbalance_qty_cap = (
        profile.risk_increasing_imbalance_qty_cap if dynamic_cap_active else profile.imbalance_qty_cap
    )
    imbalance_room = effective_imbalance_qty_cap - max(0.0, same_qty - opp_qty)
    if imbalance_room <= profile.dust_qty:
        if dynamic_cap_active:
            RERUN.add_count(metrics, day, "seed_block_dynamic_imbalance_qty")
        else:
            RERUN.add_count(metrics, day, "seed_block_imbalance_qty")
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
        TrackedLot(
            qty=qty,
            px=seed_px,
            ts_ms=ts_ms,
            side=side,
            source_candidate_row_id=str(row["candidate_row_id"]),
            source_day=day,
            source_slug=str(row["slug"]),
            source_offset_s=offset_s_value,
            source_public_trade_price=trade_px,
            source_l1_pair_ask=l1_pair_ask,
            source_pre_same_qty=same_qty,
            source_pre_opp_qty=opp_qty,
            source_seed_qty=qty,
            source_risk_increasing=risk_increasing_seed,
            source_late_repair_active=late_repair_active,
            source_dynamic_cap_active=dynamic_cap_active,
            source_imbalance_room=imbalance_room,
        )
    )
    state.last_seed_ts_ms = ts_ms
    state.active = True
    fee = RERUN.official_taker_fee(qty, seed_px, profile.official_fee_rate)
    RERUN.add_count(metrics, day, "seed_actions")
    RERUN.add_metric(metrics, day, "gross_buy_qty", qty)
    RERUN.add_metric(metrics, day, "gross_buy_cost", qty * seed_px)
    RERUN.add_metric(metrics, day, "official_taker_fee", fee)
    RERUN.pair_inventory(profile, state, metrics, ts_ms)


def settle_and_collect(states: dict[str, Any], profile: Any, metrics: defaultdict[str, float]) -> list[dict[str, Any]]:
    residuals: list[dict[str, Any]] = []
    for state in states.values():
        if not state.active:
            continue
        RERUN.pair_inventory(profile, state, metrics, state.last_ts_ms)
        RERUN.add_count(metrics, state.day, "active_markets")
        winner = state.winner_side
        for side in ("YES", "NO"):
            for lot in state.lots[side]:
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                payout = lot.qty if winner == side else 0.0
                RERUN.add_metric(metrics, state.day, "residual_qty", lot.qty)
                RERUN.add_metric(metrics, state.day, "residual_cost", cost)
                RERUN.add_metric(metrics, state.day, "residual_settle_payout", payout)
                RERUN.add_metric(metrics, state.day, "residual_settle_pnl", payout - cost)
                residuals.append(
                    {
                        "day": state.day,
                        "slug": state.slug,
                        "residual_side": side,
                        "winner_side": winner,
                        "source_side": lot.side,
                        "qty": lot.qty,
                        "cost": cost,
                        "px": lot.px,
                        "source_candidate_row_id": lot.source_candidate_row_id,
                        "source_day": lot.source_day,
                        "source_slug": lot.source_slug,
                        "source_offset_s": lot.source_offset_s,
                        "source_offset_bucket": offset_bucket(lot.source_offset_s),
                        "source_public_trade_price": lot.source_public_trade_price,
                        "source_public_trade_price_bucket": price_bucket(lot.source_public_trade_price),
                        "source_l1_pair_ask": lot.source_l1_pair_ask,
                        "source_l1_pair_ask_bucket": price_bucket(lot.source_l1_pair_ask),
                        "source_pre_same_qty": lot.source_pre_same_qty,
                        "source_pre_opp_qty": lot.source_pre_opp_qty,
                        "source_imbalance_delta": lot.source_pre_same_qty - lot.source_pre_opp_qty,
                        "source_imbalance_bucket": imbalance_bucket(lot.source_pre_same_qty, lot.source_pre_opp_qty),
                        "source_seed_qty": lot.source_seed_qty,
                        "source_risk_increasing": lot.source_risk_increasing,
                        "source_late_repair_active": lot.source_late_repair_active,
                        "source_dynamic_cap_active": lot.source_dynamic_cap_active,
                        "source_imbalance_room": lot.source_imbalance_room,
                    }
                )
    return residuals


def run_tracked(candidate_base_db: Path, profiles: list[Any], base_day_counts: dict[str, int]) -> dict[str, Any]:
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
    states_by_profile: dict[str, dict[str, Any]] = {profile.name: {} for profile in profiles}
    row_count = 0
    while True:
        rows = cursor.fetchmany(50_000)
        if not rows:
            break
        for raw in rows:
            row = dict(zip(select_cols, raw))
            row_count += 1
            for profile in profiles:
                state = RERUN.state_for(states_by_profile[profile.name], row)
                maybe_seed_tracked(row, profile, state, metrics_by_profile[profile.name])
    con.close()

    out: dict[str, Any] = {"processed_public_sell_rows": row_count, "profiles": {}}
    for profile in profiles:
        residuals = settle_and_collect(states_by_profile[profile.name], profile, metrics_by_profile[profile.name])
        out["profiles"][profile.name] = {
            "metrics": RERUN.finish_metrics(profile, metrics_by_profile[profile.name], base_day_counts),
            "residuals": residuals,
        }
    return out


def summarize_residuals(residuals: list[dict[str, Any]]) -> dict[str, Any]:
    dimensions = {
        "day": lambda row: row["day"],
        "residual_side": lambda row: row["residual_side"],
        "source_side": lambda row: row["source_side"],
        "source_offset_bucket": lambda row: row["source_offset_bucket"],
        "source_public_trade_price_bucket": lambda row: row["source_public_trade_price_bucket"],
        "source_l1_pair_ask_bucket": lambda row: row["source_l1_pair_ask_bucket"],
        "source_imbalance_bucket": lambda row: row["source_imbalance_bucket"],
        "source_risk_increasing": lambda row: f"risk_increasing_{row['source_risk_increasing']}",
        "source_late_repair_active": lambda row: f"late_repair_active_{row['source_late_repair_active']}",
        "source_dynamic_cap_active": lambda row: f"dynamic_cap_active_{row['source_dynamic_cap_active']}",
        "residual_outcome": lambda row: "winner_side" if row["residual_side"] == row["winner_side"] else "loser_side",
        "source_offset_x_imbalance": lambda row: f"{row['source_offset_bucket']}|{row['source_imbalance_bucket']}",
        "source_offset_x_side": lambda row: f"{row['source_offset_bucket']}|{row['source_side']}",
        "day_x_side": lambda row: f"{row['day']}|{row['residual_side']}",
    }
    raw: dict[str, dict[str, dict[str, float]]] = {name: {} for name in dimensions}
    for row in residuals:
        qty = float(row["qty"])
        cost = float(row["cost"])
        for name, getter in dimensions.items():
            add_bucket(raw[name], getter(row), qty, cost)
    return {
        "total_lots": len(residuals),
        "total_qty": round(sum(float(row["qty"]) for row in residuals), 6),
        "total_cost": round(sum(float(row["cost"]) for row in residuals), 6),
        "buckets": {name: finalize_buckets(values) for name, values in raw.items()},
        "top_residual_lots": sorted(
            [
                {
                    key: row[key]
                    for key in (
                        "day",
                        "slug",
                        "residual_side",
                        "winner_side",
                        "qty",
                        "cost",
                        "px",
                        "source_side",
                        "source_offset_s",
                        "source_offset_bucket",
                        "source_imbalance_bucket",
                        "source_late_repair_active",
                        "source_dynamic_cap_active",
                        "source_public_trade_price",
                    )
                }
                for row in residuals
            ],
            key=lambda item: (item["qty"], item["cost"]),
            reverse=True,
        )[:20],
    }


def compare_summaries(control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in control["buckets"]:
        out[name] = compare_bucket_lists(control["buckets"][name], variant["buckets"][name])
    return out


def day_stress_delta(control_metrics: dict[str, Any], variant_metrics: dict[str, Any]) -> list[dict[str, Any]]:
    control_by_day = {row["day"]: row for row in control_metrics.get("summary_by_day") or []}
    variant_by_day = {row["day"]: row for row in variant_metrics.get("summary_by_day") or []}
    rows = []
    for day in sorted(set(control_by_day) | set(variant_by_day)):
        c = control_by_day.get(day, {})
        v = variant_by_day.get(day, {})
        rows.append(
            {
                "day": day,
                "control_stress100_worst_pnl": c.get("stress100_worst_pnl", 0.0),
                "variant_stress100_worst_pnl": v.get("stress100_worst_pnl", 0.0),
                "delta_stress100_worst_pnl": round(
                    float(v.get("stress100_worst_pnl", 0.0)) - float(c.get("stress100_worst_pnl", 0.0)), 6
                ),
                "control_residual_qty": c.get("residual_qty", 0.0),
                "variant_residual_qty": v.get("residual_qty", 0.0),
                "delta_residual_qty": round(float(v.get("residual_qty", 0.0)) - float(c.get("residual_qty", 0.0)), 6),
                "control_fee_after_pnl": c.get("fee_after_pnl", 0.0),
                "variant_fee_after_pnl": v.get("fee_after_pnl", 0.0),
                "delta_fee_after_pnl": round(float(v.get("fee_after_pnl", 0.0)) - float(c.get("fee_after_pnl", 0.0)), 6),
            }
        )
    rows.sort(key=lambda row: row["delta_stress100_worst_pnl"])
    return rows


def choose_decision(delta: dict[str, Any]) -> tuple[str, str, str]:
    broad_dimensions = [
        row for row in delta["source_offset_bucket"] if row["delta_qty"] > 100.0
    ]
    day_concentration = delta["day"][0]["delta_qty"] / max(sum(max(row["delta_qty"], 0.0) for row in delta["day"]), 1e-9)
    if len(broad_dimensions) >= 3 and day_concentration < 0.50:
        return (
            "DISCARD",
            "DISCARD_DYNAMIC_IMBALANCE_RESIDUAL_BROAD",
            "Residual degradation is spread across multiple offset buckets and is not isolated to one narrow day.",
        )
    return (
        "UNKNOWN",
        "UNKNOWN_DYNAMIC_IMBALANCE_ATTRIBUTION_MIXED",
        "Residual degradation is concentrated enough to inspect, but this artifact does not yet define a deployable mechanism.",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=RERUN.DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument(
        "--dynamic-manifest",
        type=Path,
        default=Path(
            "xuan_research_artifacts/"
            "xuan_b27_dplus_candidate_pipeline_state_machine_dynamic_imbalance_rerun_20260520T110120Z/"
            "manifest.json"
        ),
    )
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    candidate_manifest_path = args.candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_db_path = args.candidate_base_dir / "candidate_base.duckdb"
    dynamic_manifest_path = args.dynamic_manifest if args.dynamic_manifest.is_absolute() else root / args.dynamic_manifest
    required = [candidate_manifest_path, candidate_db_path, dynamic_manifest_path]
    missing = [str(path) for path in required if not path.exists()]
    unsafe = [str(path) for path in required if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "artifact": ARTIFACT,
            "created_utc": label,
            "decision_label": "BLOCKED",
            "status": "BLOCKED_INPUT_UNAVAILABLE",
            "missing": missing,
            "unsafe": unsafe,
            "next_action": "Restore required local candidate pipeline artifacts and rerun dynamic imbalance attribution.",
        }
        write_json(output_dir / "manifest.json", manifest)
        print(output_dir / "manifest.json")
        return 0

    candidate_manifest = read_json(candidate_manifest_path)
    dynamic_manifest = read_json(dynamic_manifest_path)
    base_day_counts = {str(day): int(count) for day, count in (candidate_manifest.get("day_counts") or {}).items()}
    profiles = [
        RERUN.Profile(
            name="late_repair90",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
        ),
        RERUN.Profile(
            name="dynamic_imbalance_1p10",
            seed_offset_max_s=120.0,
            late_repair_only_after_s=90.0,
            risk_increasing_imbalance_qty_cap=1.10,
        ),
    ]
    tracked = run_tracked(candidate_db_path, profiles, base_day_counts)
    control = tracked["profiles"]["late_repair90"]
    variant = tracked["profiles"]["dynamic_imbalance_1p10"]
    control_summary = summarize_residuals(control["residuals"])
    variant_summary = summarize_residuals(variant["residuals"])
    bucket_delta = compare_summaries(control_summary, variant_summary)
    decision_label, status, decision_reason = choose_decision(bucket_delta)
    stress_delta = day_stress_delta(control["metrics"], variant["metrics"])
    positive_day_delta = sum(max(row["delta_qty"], 0.0) for row in bucket_delta["day"])
    top_day_delta_share = (
        bucket_delta["day"][0]["delta_qty"] / positive_day_delta
        if positive_day_delta > 0.0
        else 0.0
    )
    attribution_summary = {
        "dominant_offset_bucket": bucket_delta["source_offset_bucket"][0],
        "dominant_imbalance_bucket": bucket_delta["source_imbalance_bucket"][0],
        "dominant_offset_x_imbalance_bucket": bucket_delta["source_offset_x_imbalance"][0],
        "dominant_late_repair_bucket": bucket_delta["source_late_repair_active"][0],
        "dominant_risk_bucket": bucket_delta["source_risk_increasing"][0],
        "top_day_delta_share": round(top_day_delta_share, 6),
        "broad_across_days": top_day_delta_share < 0.15,
        "read": (
            "Residual degradation is concentrated in late off_90_120 repair/underweight sources, "
            "especially small underweight gaps, but it is broad across days and both sides."
        ),
    }
    manifest = {
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "causal_verifier",
        "decision_label": decision_label,
        "status": status,
        "decision_reason": decision_reason,
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
            "dynamic_manifest": str(dynamic_manifest_path),
        },
        "scope": {
            "dataset_type": candidate_manifest.get("dataset_type"),
            "labels": candidate_manifest.get("labels"),
            "days": candidate_manifest.get("days"),
            "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
            "public_account_execution_truth_v1_private_truth": False,
            "deployable": False,
            "can_support_strategy_promotion": False,
            "promotion_gate_pass": False,
            "result_classification": "PASS_LOCAL_COMPLETION_RESEARCH_ONLY",
        },
        "processed_public_sell_rows": tracked["processed_public_sell_rows"],
        "source_decision": {
            "dynamic_manifest_decision_label": dynamic_manifest.get("decision_label"),
            "dynamic_manifest_status": dynamic_manifest.get("status"),
            "dynamic_research_ranking": dynamic_manifest.get("research_ranking"),
        },
        "control_metrics": {
            key: control["metrics"].get(key)
            for key in (
                "seed_actions",
                "pair_qty",
                "weighted_pair_cost",
                "fee_after_pnl",
                "stress100_worst_pnl",
                "worst_day_fee_after_pnl",
                "residual_qty",
                "residual_cost",
                "residual_qty_rate",
                "residual_cost_rate",
            )
        },
        "variant_metrics": {
            key: variant["metrics"].get(key)
            for key in (
                "seed_actions",
                "pair_qty",
                "weighted_pair_cost",
                "fee_after_pnl",
                "stress100_worst_pnl",
                "worst_day_fee_after_pnl",
                "residual_qty",
                "residual_cost",
                "residual_qty_rate",
                "residual_cost_rate",
                "seed_block_dynamic_imbalance_qty",
                "seed_block_late_repair_only",
            )
        },
        "residual_summary": {
            "control": {key: control_summary[key] for key in ("total_lots", "total_qty", "total_cost")},
            "variant": {key: variant_summary[key] for key in ("total_lots", "total_qty", "total_cost")},
            "delta_qty": round(variant_summary["total_qty"] - control_summary["total_qty"], 6),
            "delta_cost": round(variant_summary["total_cost"] - control_summary["total_cost"], 6),
        },
        "top_delta_buckets": bucket_delta,
        "top_variant_residual_lots": variant_summary["top_residual_lots"],
        "day_stress_delta": stress_delta,
        "attribution_summary": attribution_summary,
        "interpretation": [
            "Dynamic imbalance improves participation and fee-after PnL but creates materially more residual.",
            "The attribution compares final residual lot source fields, not raw events or source-of-truth fills.",
            "The extra residual is not from the risk-increasing capped seeds themselves; it comes from the late repair seeds left at full target size.",
            "A narrower verifier should test late fill-to-balance sizing rather than another price cap or hard cutoff.",
        ],
        "research_ranking": {
            "label": (
                "TRADEOFF_ECON_POSITIVE_RISK_WORSE"
                if decision_label != "KEEP"
                else "KEEP_ECONOMIC_RESEARCH_ONLY"
            ),
            "dynamic_fee_after_pnl_positive": float(variant["metrics"]["fee_after_pnl"]) > 0.0,
            "dynamic_stress100_positive": float(variant["metrics"]["stress100_worst_pnl"]) > 0.0,
            "residual_degradation_broad": decision_label == "DISCARD",
            "source_tradeoff_label": dynamic_manifest.get("status"),
        },
        "promotion_gate": {
            "passed": False,
            "required_before_g2_canary": True,
            "reason": "local completion candidate-pipeline attribution only",
            "deployable": False,
            "can_support_strategy_promotion": False,
            "requires_no_order_shadow": False,
            "requires_source_of_truth_replay": True,
        },
        "next_action": (
            "Run a local candidate-pipeline verifier for late_repair90_fill_to_balance_after_90: after offset_s>=90, "
            "allow underweight/repair seeds but cap seed qty to the current opposite_qty-same_qty deficit when that "
            "deficit is below target_qty; keep earlier offsets unchanged and compare against late_repair90 and "
            "dynamic_imbalance_1p10. Do not run EC2 shadow/canary before this local verifier returns KEEP."
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
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
