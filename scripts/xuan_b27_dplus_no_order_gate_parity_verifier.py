#!/usr/bin/env python3
"""Replay D+ fill-to-balance with runner-like no-order gates.

This is a local-only causal verifier. It reads candidate_base, local manifests,
and pulled diagnostic shadow summaries. It does not read raw/replay/collector
stores, full completion stores, event JSONL, sockets, SSH, or remote state.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_no_order_gate_parity_verifier"
SIZE_VERIFIER = "xuan_b27_dplus_size_aware_fill_to_balance_transfer_verifier.py"
DUST = 1e-9


@dataclass(frozen=True)
class Mode:
    name: str
    fill_to_balance: str
    activation_opp_seen: bool
    pairing_only_when_residual: bool


MODES = [
    Mode("no_gate_late_repair90_control", "off", False, False),
    Mode("no_gate_fill_to_balance_all", "all", False, False),
    Mode("gate_parity_late_repair90_control", "off", True, True),
    Mode("gate_parity_fill_to_balance_all", "all", True, True),
    Mode("gate_parity_fill_to_balance_shadow_observed_size_bucket", "shadow_observed", True, True),
]


def load_size_module() -> Any:
    path = Path(__file__).resolve().with_name(SIZE_VERIFIER)
    spec = importlib.util.spec_from_file_location("xuan_b27_dplus_size_verifier", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SV = load_size_module()


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def other(side: str) -> str:
    return "NO" if side == "YES" else "YES"


def add_count(bucket: dict[str, float], key: str, value: float = 1.0) -> None:
    bucket[key] = round(float(bucket.get(key, 0.0)) + float(value), 6)


def add_nested(bucket: dict[str, dict[str, float]], key: str, subkey: str, value: float = 1.0) -> None:
    child = bucket.setdefault(key, {})
    add_count(child, subkey, value)


def mode_profile(sm: Any, mode: Mode) -> Any:
    return sm.Profile(
        name=mode.name,
        seed_offset_max_s=120.0,
        late_repair_only_after_s=90.0,
        late_repair_fill_to_balance_after_s=90.0 if mode.fill_to_balance != "off" else None,
        edge=0.07,
        target_qty=5.0,
        max_open_cost=80.0,
        imbalance_qty_cap=1.25,
    )


def new_trace() -> dict[str, Any]:
    return {
        "gate_config": {},
        "activation_seen_updates": 0,
        "activation_seen_updates_by_side": {},
        "activation_block_count": 0,
        "activation_block_by_side": {},
        "activation_block_by_offset": {},
        "pairing_only_when_residual_block_count": 0,
        "pairing_only_when_residual_block_by_side": {},
        "pairing_only_when_residual_block_by_offset": {},
        "late_repair_only_block_count": 0,
        "late_repair_only_block_by_offset": {},
        "target_block_count": 0,
        "imbalance_qty_block_count": 0,
        "candidate_success_count": 0,
        "candidate_success_by_side": {},
        "late_underweight_candidates": 0,
        "late_underweight_by_base_seed_qty_bucket": {},
        "late_underweight_by_deficit_bucket": {},
        "late_underweight_by_shadow_observed_bucket_match": {},
        "f2b_candidates": 0,
        "f2b_caps": 0,
        "f2b_blocks": 0,
        "f2b_qty_reduction": 0.0,
        "f2b_candidate_by_offset": {},
        "f2b_candidate_by_side": {},
        "f2b_candidate_by_deficit_bucket": {},
        "f2b_candidate_by_base_seed_qty_bucket": {},
        "f2b_cap_by_deficit_bucket": {},
        "f2b_cap_by_base_seed_qty_bucket": {},
        "f2b_qty_reduction_by_deficit_bucket": {},
        "base_seed_qty_limiter": {},
    }


def ratio(num: float, den: float) -> float | None:
    return round(float(num) / float(den), 8) if den else None


def run_modes(candidate_base_db: Path, base_day_counts: dict[str, int]) -> dict[str, Any]:
    sm = SV.load_state_machine_module()
    profiles = {mode.name: mode_profile(sm, mode) for mode in MODES}
    metrics_by_mode: dict[str, defaultdict[str, float]] = {mode.name: defaultdict(float) for mode in MODES}
    states_by_mode: dict[str, dict[str, Any]] = {mode.name: {} for mode in MODES}
    activation_by_mode: dict[str, dict[str, dict[str, int | None]]] = {mode.name: {} for mode in MODES}
    trace_by_mode: dict[str, dict[str, Any]] = {mode.name: new_trace() for mode in MODES}
    for mode in MODES:
        trace_by_mode[mode.name]["gate_config"] = {
            "activation_mode": "opp_seen" if mode.activation_opp_seen else "none",
            "activation_window_s": 15 if mode.activation_opp_seen else None,
            "pairing_only_when_residual": mode.pairing_only_when_residual,
            "fill_to_balance": mode.fill_to_balance,
        }

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

    def activation_state(mode: Mode, condition_id: str) -> dict[str, int | None]:
        return activation_by_mode[mode.name].setdefault(condition_id, {"YES": None, "NO": None})

    def record_activation_seen(mode: Mode, condition_id: str, side: str, ts_ms: int) -> None:
        state = activation_state(mode, condition_id)
        state[side] = ts_ms
        trace = trace_by_mode[mode.name]
        trace["activation_seen_updates"] += 1
        add_count(trace["activation_seen_updates_by_side"], side)

    def activation_allows_seed(mode: Mode, condition_id: str, side: str, ts_ms: int) -> tuple[bool, int | None]:
        if not mode.activation_opp_seen:
            return True, None
        opp_seen_ms = activation_state(mode, condition_id).get(other(side))
        if opp_seen_ms is None:
            return False, None
        age_ms = ts_ms - opp_seen_ms
        return age_ms >= 0 and age_ms <= 15_000 + 1e-9, int(age_ms)

    def record_late_underweight(trace: dict[str, Any], offset_s: float, side: str, base_qty: float, deficit_qty: float) -> None:
        trace["late_underweight_candidates"] += 1
        add_count(trace["late_underweight_by_base_seed_qty_bucket"], SV.qty_bucket("base_seed_qty", base_qty))
        add_count(trace["late_underweight_by_deficit_bucket"], SV.deficit_bucket(deficit_qty))
        shadow_bucket_match = base_qty <= 2.0 + DUST and 1.0 + DUST < deficit_qty <= 2.0 + DUST
        add_count(trace["late_underweight_by_shadow_observed_bucket_match"], "match" if shadow_bucket_match else "not_match")

    def maybe_seed(row: dict[str, Any], mode: Mode) -> None:
        profile = profiles[mode.name]
        metrics = metrics_by_mode[mode.name]
        trace = trace_by_mode[mode.name]
        day = str(row["day"])
        sm.add_count(metrics, day, "candidate_count")
        side = str(row["side"])
        if side not in {"YES", "NO"}:
            return
        condition_id = str(row["condition_id"])
        offset_s = float(row["offset_s"])
        if not (profile.seed_offset_min_s <= offset_s < profile.seed_offset_max_s):
            sm.add_count(metrics, day, "seed_block_offset")
            return
        trade_px = float(row["public_trade_price"])
        trade_size = float(row["public_trade_size"])
        if trade_size <= DUST or not math.isfinite(trade_px):
            return
        if not (profile.seed_px_lo <= trade_px <= profile.seed_px_hi):
            sm.add_count(metrics, day, "seed_block_price_band")
            return
        l1_pair_ask = float(row["l1_pair_ask"])
        if not math.isfinite(l1_pair_ask) or l1_pair_ask > profile.seed_l1_pair_cap + 1e-12:
            sm.add_count(metrics, day, "seed_block_l1_pair_cap")
            return
        ts_ms = int(row["ts_ms"])
        state = sm.state_for(states_by_mode[mode.name], row)
        if ts_ms - state.last_seed_ts_ms < profile.cooldown_s * 1000.0:
            sm.add_count(metrics, day, "seed_block_cooldown")
            return

        same_qty = sm.lot_qty(state.lots[side])
        opp_side = sm.other(side)
        opp_qty = sm.lot_qty(state.lots[opp_side])
        offset_key = SV.offset_bucket(offset_s)
        if mode.pairing_only_when_residual and same_qty > opp_qty + profile.dust_qty:
            sm.add_count(metrics, day, "seed_block_pairing_only_when_residual")
            trace["pairing_only_when_residual_block_count"] += 1
            add_count(trace["pairing_only_when_residual_block_by_side"], side)
            add_count(trace["pairing_only_when_residual_block_by_offset"], offset_key)
            record_activation_seen(mode, condition_id, side, ts_ms)
            return
        if (
            profile.late_repair_only_after_s is not None
            and offset_s >= profile.late_repair_only_after_s
            and same_qty >= opp_qty
        ):
            sm.add_count(metrics, day, "seed_block_late_repair_only")
            trace["late_repair_only_block_count"] += 1
            add_count(trace["late_repair_only_block_by_offset"], offset_key)
            record_activation_seen(mode, condition_id, side, ts_ms)
            return
        risk_increasing_seed = same_qty + profile.dust_qty >= opp_qty
        activation_ok, _activation_opp_age_ms = activation_allows_seed(mode, condition_id, side, ts_ms)
        if risk_increasing_seed and not activation_ok:
            sm.add_count(metrics, day, "seed_block_activation_opp_seen")
            trace["activation_block_count"] += 1
            add_count(trace["activation_block_by_side"], side)
            add_count(trace["activation_block_by_offset"], offset_key)
            record_activation_seen(mode, condition_id, side, ts_ms)
            return
        if same_qty >= profile.target_qty - profile.dust_qty:
            sm.add_count(metrics, day, "seed_block_target")
            trace["target_block_count"] += 1
            record_activation_seen(mode, condition_id, side, ts_ms)
            return
        same_cost = sm.lot_cost(state.lots[side])
        opp_cost = sm.lot_cost(state.lots[opp_side])
        if max(0.0, same_cost - opp_cost) > profile.imbalance_cost_cap + 1e-12:
            sm.add_count(metrics, day, "seed_block_imbalance_cost")
            record_activation_seen(mode, condition_id, side, ts_ms)
            return

        seed_px = max(0.01, trade_px - profile.edge)
        open_cost = sm.lot_cost(state.lots["YES"]) + sm.lot_cost(state.lots["NO"])
        target_room = profile.target_qty - same_qty
        open_cost_room = (profile.max_open_cost - open_cost) / max(seed_px, 1e-9)
        imbalance_room = profile.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
        if imbalance_room <= profile.dust_qty:
            sm.add_count(metrics, day, "seed_block_imbalance_qty")
            trace["imbalance_qty_block_count"] += 1
            record_activation_seen(mode, condition_id, side, ts_ms)
            return
        base_components = {
            "max_seed_qty": profile.max_seed_qty,
            "public_trade_size_fill_haircut": trade_size * profile.fill_haircut,
            "target_room": target_room,
            "open_cost_room": open_cost_room,
            "imbalance_room": imbalance_room,
        }
        base_qty = min(base_components.values())
        limiter = SV.min_limiter(base_components)
        late_underweight = (
            profile.late_repair_fill_to_balance_after_s is not None
            and offset_s >= profile.late_repair_fill_to_balance_after_s
            and same_qty < opp_qty
        )
        if late_underweight:
            record_late_underweight(trace, offset_s, side, base_qty, max(0.0, opp_qty - same_qty))

        fill_to_balance_active = False
        if late_underweight:
            fill_to_balance_active = SV.f2b_allowed(mode, base_qty, max(0.0, opp_qty - same_qty))
        qty = base_qty
        if fill_to_balance_active:
            deficit_qty = max(0.0, opp_qty - same_qty)
            capped_qty = min(qty, deficit_qty)
            deficit_key = SV.deficit_bucket(deficit_qty)
            base_key = SV.qty_bucket("base_seed_qty", base_qty)
            trace["f2b_candidates"] += 1
            add_count(trace["f2b_candidate_by_offset"], offset_key)
            add_count(trace["f2b_candidate_by_side"], side)
            add_count(trace["f2b_candidate_by_deficit_bucket"], deficit_key)
            add_count(trace["f2b_candidate_by_base_seed_qty_bucket"], base_key)
            add_count(trace["base_seed_qty_limiter"], limiter)
            sm.add_count(metrics, day, "seed_late_fill_to_balance_candidate")
            if capped_qty < qty - DUST:
                reduction = qty - capped_qty
                trace["f2b_caps"] += 1
                trace["f2b_qty_reduction"] = round(float(trace["f2b_qty_reduction"]) + reduction, 6)
                add_count(trace["f2b_cap_by_deficit_bucket"], deficit_key)
                add_count(trace["f2b_cap_by_base_seed_qty_bucket"], base_key)
                add_count(trace["f2b_qty_reduction_by_deficit_bucket"], deficit_key, reduction)
                sm.add_count(metrics, day, "seed_qty_cap_late_fill_to_balance")
                sm.add_metric(metrics, day, "seed_late_fill_to_balance_qty_reduction", reduction)
            qty = capped_qty
        if qty <= profile.dust_qty:
            if fill_to_balance_active:
                trace["f2b_blocks"] += 1
                sm.add_count(metrics, day, "seed_block_late_fill_to_balance_qty")
            record_activation_seen(mode, condition_id, side, ts_ms)
            return

        state.lots[side].append(
            sm.Lot(qty=qty, px=seed_px, ts_ms=ts_ms, side=side, source_candidate_row_id=str(row["candidate_row_id"]))
        )
        state.last_seed_ts_ms = ts_ms
        state.active = True
        fee = sm.official_taker_fee(qty, seed_px, profile.official_fee_rate)
        sm.add_count(metrics, day, "seed_actions")
        sm.add_metric(metrics, day, "gross_buy_qty", qty)
        sm.add_metric(metrics, day, "gross_buy_cost", qty * seed_px)
        sm.add_metric(metrics, day, "official_taker_fee", fee)
        trace["candidate_success_count"] += 1
        add_count(trace["candidate_success_by_side"], side)
        sm.pair_inventory(profile, state, metrics, ts_ms)
        record_activation_seen(mode, condition_id, side, ts_ms)

    con = duckdb.connect(str(candidate_base_db), read_only=True)
    cursor = con.execute(query)
    row_count = 0
    while True:
        rows = cursor.fetchmany(50_000)
        if not rows:
            break
        for raw in rows:
            row_count += 1
            row = dict(zip(select_cols, raw))
            for mode in MODES:
                maybe_seed(row, mode)
    con.close()

    profiles_out: dict[str, Any] = {}
    for mode in MODES:
        profile = profiles[mode.name]
        metrics = metrics_by_mode[mode.name]
        sm.settle(states_by_mode[mode.name], profile, metrics)
        out_metrics = sm.finish_metrics(profile, metrics, base_day_counts)
        out_metrics["seed_block_activation_opp_seen"] = int(metrics["seed_block_activation_opp_seen"])
        out_metrics["seed_block_pairing_only_when_residual"] = int(metrics["seed_block_pairing_only_when_residual"])
        profiles_out[mode.name] = {
            "metrics": out_metrics,
            "trace": trace_by_mode[mode.name],
        }
    return {
        "processed_public_sell_rows": row_count,
        "profiles": profiles_out,
    }


def metric_delta(control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
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
        "residual_qty",
        "residual_cost",
        "residual_qty_rate",
        "residual_cost_rate",
        "seed_late_fill_to_balance_candidate",
        "seed_qty_cap_late_fill_to_balance",
        "seed_block_late_fill_to_balance_qty",
        "seed_late_fill_to_balance_qty_reduction",
        "seed_block_activation_opp_seen",
        "seed_block_pairing_only_when_residual",
    ]
    out: dict[str, Any] = {}
    for field in fields:
        c = SV.as_float(control.get(field))
        v = SV.as_float(variant.get(field))
        out[field] = {
            "control": SV.round6(c),
            "variant": SV.round6(v),
            "absolute_delta": SV.round6(v - c),
            "relative_delta": ratio(v - c, c),
        }
    out["seed_action_retention"] = ratio(SV.as_float(variant.get("seed_actions")), SV.as_float(control.get("seed_actions")))
    out["pair_qty_retention"] = ratio(SV.as_float(variant.get("pair_qty")), SV.as_float(control.get("pair_qty")))
    out["f2b_candidate_retention"] = ratio(
        SV.as_float(variant.get("seed_late_fill_to_balance_candidate")),
        SV.as_float(control.get("seed_late_fill_to_balance_candidate")),
    )
    out["f2b_cap_retention"] = ratio(
        SV.as_float(variant.get("seed_qty_cap_late_fill_to_balance")),
        SV.as_float(control.get("seed_qty_cap_late_fill_to_balance")),
    )
    out["f2b_qty_reduction_retention"] = ratio(
        SV.as_float(variant.get("seed_late_fill_to_balance_qty_reduction")),
        SV.as_float(control.get("seed_late_fill_to_balance_qty_reduction")),
    )
    out["residual_qty_rate_reduction"] = (
        None
        if SV.as_float(control.get("residual_qty_rate")) == 0.0
        else round(1.0 - SV.as_float(variant.get("residual_qty_rate")) / SV.as_float(control.get("residual_qty_rate")), 8)
    )
    out["residual_cost_rate_reduction"] = (
        None
        if SV.as_float(control.get("residual_cost_rate")) == 0.0
        else round(1.0 - SV.as_float(variant.get("residual_cost_rate")) / SV.as_float(control.get("residual_cost_rate")), 8)
    )
    return out


def classify(
    no_gate_f2b: dict[str, Any],
    gate_control: dict[str, Any],
    gate_f2b: dict[str, Any],
    delta_gate_vs_no_gate: dict[str, Any],
    delta_gate_f2b_vs_gate_control: dict[str, Any],
    shadow_summary: dict[str, Any],
) -> dict[str, Any]:
    f2b_candidate_retention = delta_gate_vs_no_gate.get("f2b_candidate_retention") or 0.0
    f2b_cap_retention = delta_gate_vs_no_gate.get("f2b_cap_retention") or 0.0
    f2b_qty_reduction_retention = delta_gate_vs_no_gate.get("f2b_qty_reduction_retention") or 0.0
    residual_improves_vs_gate_control = (delta_gate_f2b_vs_gate_control.get("residual_qty_rate_reduction") or 0.0) > 0
    cost_improves_vs_gate_control = (delta_gate_f2b_vs_gate_control.get("residual_cost_rate_reduction") or 0.0) > 0
    economics_positive = (
        SV.as_float(gate_f2b.get("fee_after_pnl")) > 0
        and SV.as_float(gate_f2b.get("stress100_worst_pnl")) > 0
        and SV.as_float(gate_f2b.get("worst_day_fee_after_pnl")) > 0
    )
    shadow_f2b_candidates = SV.as_float(shadow_summary["metrics"].get("late_repair_fill_to_balance_candidates"))
    gate_f2b_candidates = SV.as_float(gate_f2b.get("seed_late_fill_to_balance_candidate"))
    shadow_to_gate_candidate_ratio = ratio(shadow_f2b_candidates, gate_f2b_candidates) or 0.0
    if not economics_positive:
        decision = "DISCARD"
        label = "DISCARD_NO_ORDER_GATE_PARITY_ECONOMIC_NEGATIVE"
    elif f2b_qty_reduction_retention < 0.05 and shadow_to_gate_candidate_ratio < 0.05:
        decision = "DISCARD"
        label = "DISCARD_NO_ORDER_GATE_PARITY_F2B_TRANSFER_COLLAPSE"
    elif residual_improves_vs_gate_control and cost_improves_vs_gate_control and shadow_to_gate_candidate_ratio >= 0.25:
        decision = "KEEP"
        label = "KEEP_NO_ORDER_GATE_PARITY_FILL_TO_BALANCE_RESEARCH_ONLY"
    else:
        decision = "UNKNOWN"
        label = "UNKNOWN_NO_ORDER_GATE_PARITY_MIXED_OR_FIELD_GAP"
    return {
        "decision_label": decision,
        "label": label,
        "gate_parity_fee_stress_worst_day_positive": economics_positive,
        "gate_parity_residual_qty_improves_vs_gate_control": residual_improves_vs_gate_control,
        "gate_parity_residual_cost_improves_vs_gate_control": cost_improves_vs_gate_control,
        "gate_vs_no_gate_f2b_candidate_retention": f2b_candidate_retention,
        "gate_vs_no_gate_f2b_cap_retention": f2b_cap_retention,
        "gate_vs_no_gate_f2b_qty_reduction_retention": f2b_qty_reduction_retention,
        "diagnostic_shadow_f2b_candidates": shadow_f2b_candidates,
        "gate_parity_local_f2b_candidates": gate_f2b_candidates,
        "diagnostic_shadow_to_gate_parity_f2b_candidate_ratio": shadow_to_gate_candidate_ratio,
        "notes": [
            "Activation is approximated from candidate_base public_sell side sequence after runner-like early gates.",
            "Candidate_base lacks exact live market source_sequence_id, pending-order queue state, and full book/trade interleaving; this verifier is parity evidence, not promotion evidence.",
            "If diagnostic shadow opportunity volume remains far below gate-parity local replay, do not run another fill-to-balance shadow without new source fields or mechanism.",
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=SV.DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument("--baseline-result-dir", type=Path, default=SV.DEFAULT_BASELINE_RESULT_DIR)
    parser.add_argument("--local-lead-manifest", type=Path, default=SV.DEFAULT_LOCAL_LEAD)
    parser.add_argument("--diagnostic-shadow-artifact", type=Path, default=SV.DEFAULT_DIAGNOSTIC_SHADOW)
    parser.add_argument("--size-aware-manifest", type=Path, default=Path("xuan_research_artifacts/xuan_b27_dplus_size_aware_fill_to_balance_transfer_verifier_20260520T205314Z/manifest.json"))
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"

    candidate_manifest = args.candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_db = args.candidate_base_dir / "candidate_base.duckdb"
    result_manifest = args.baseline_result_dir / "RESULT_SUMMARY_MANIFEST.json"
    compliance_manifest = args.baseline_result_dir / "COMPLIANCE_MANIFEST.json"
    required_paths = [
        candidate_manifest,
        candidate_db,
        result_manifest,
        compliance_manifest,
        args.local_lead_manifest,
        args.diagnostic_shadow_artifact / "remote" / "output" / "aggregate_report.json",
        args.diagnostic_shadow_artifact / "audit_manifest.json",
        args.size_aware_manifest,
    ]
    missing = [str(path) for path in required_paths if not path.exists()]
    unsafe = [str(path) for path in required_paths if path.exists() and not SV.path_safe(path)]
    if missing or unsafe:
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": label,
            "decision_label": "BLOCKED",
            "status": "BLOCKED_CANDIDATE_PIPELINE_INPUT_UNAVAILABLE",
            "missing_paths": missing,
            "unsafe_paths": unsafe,
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps({"status": manifest["status"], "manifest": str(output_dir / "manifest.json")}, indent=2))
        return 2

    candidate_manifest_json = SV.read_json(candidate_manifest)
    result_manifest_json = SV.read_json(result_manifest)
    compliance_manifest_json = SV.read_json(compliance_manifest)
    local_lead = SV.read_json(args.local_lead_manifest)
    size_aware = SV.read_json(args.size_aware_manifest)
    shadow_summary = SV.summarize_shadow(args.diagnostic_shadow_artifact)

    con = duckdb.connect(str(candidate_db), read_only=True)
    base_day_counts = {
        row[0]: int(row[1])
        for row in con.execute("select day, count(*) from candidate_base group by 1 order by 1").fetchall()
    }
    con.close()

    run = run_modes(candidate_db, base_day_counts)
    no_gate_control = run["profiles"]["no_gate_late_repair90_control"]["metrics"]
    no_gate_f2b = run["profiles"]["no_gate_fill_to_balance_all"]["metrics"]
    gate_control = run["profiles"]["gate_parity_late_repair90_control"]["metrics"]
    gate_f2b = run["profiles"]["gate_parity_fill_to_balance_all"]["metrics"]
    gate_shadow_bucket = run["profiles"]["gate_parity_fill_to_balance_shadow_observed_size_bucket"]["metrics"]
    no_gate_f2b_delta = metric_delta(no_gate_control, no_gate_f2b)
    gate_f2b_delta = metric_delta(gate_control, gate_f2b)
    gate_shadow_bucket_delta = metric_delta(gate_control, gate_shadow_bucket)
    gate_vs_no_gate_f2b = metric_delta(no_gate_f2b, gate_f2b)
    gate_control_vs_no_gate_control = metric_delta(no_gate_control, gate_control)
    ranking = classify(no_gate_f2b, gate_control, gate_f2b, gate_vs_no_gate_f2b, gate_f2b_delta, shadow_summary)

    status = ranking["label"]
    next_action = (
        "Do not run another fill-to-balance shadow. Freeze fill-to-balance90 as a shadow candidate until source "
        "fields can prove live/no-order F2B opportunity parity; choose a different local non-price mechanism or add "
        "default-off no-order parity diagnostics."
        if ranking["decision_label"] == "DISCARD"
        else "Run one more local-only attribution step naming exact missing source fields before considering any EC2 shadow."
    )
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "causal_verifier",
        "decision_label": ranking["decision_label"],
        "status": status,
        "scope": {
            "local_only": True,
            "candidate_base_duckdb_read_only": True,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_collector_scanned": False,
            "full_completion_store_scanned": False,
            "runner_or_trading_paths_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest),
            "candidate_base_duckdb": str(candidate_db),
            "baseline_result_manifest": str(result_manifest),
            "compliance_manifest": str(compliance_manifest),
            "local_lead_manifest": str(args.local_lead_manifest),
            "size_aware_manifest": str(args.size_aware_manifest),
            "diagnostic_shadow_artifact": str(args.diagnostic_shadow_artifact),
        },
        "coverage": {
            "candidate_base_labels": candidate_manifest_json.get("labels", []),
            "candidate_base_excluded_labels_or_days": candidate_manifest_json.get("excluded_labels_or_days", []),
            "candidate_base_row_count": candidate_manifest_json.get("row_count"),
            "result_labels": result_manifest_json.get("labels", []),
            "strict_cache": compliance_manifest_json.get("strict_cache", {}),
            "public_account_audit": compliance_manifest_json.get("public_account_audit", {}),
            "public_account_execution_truth_v1_private_truth": compliance_manifest_json.get(
                "public_account_execution_truth_v1_private_truth"
            ),
        },
        "config": {
            "edge": 0.07,
            "target_qty": 5.0,
            "max_open_cost": 80.0,
            "imbalance_qty_cap": 1.25,
            "fill_haircut": 0.25,
            "late_repair_only_after_s": 90.0,
            "late_repair_fill_to_balance_after_s": 90.0,
            "seed_offset_max_s": 120.0,
            "activation_mode": "opp_seen",
            "activation_window_s": 15,
            "pairing_only_when_residual": True,
            "activation_approximation": "candidate_base public_sell side sequence after offset/price/l1/cooldown gates",
        },
        "processed_public_sell_rows": run["processed_public_sell_rows"],
        "profiles": run["profiles"],
        "deltas": {
            "no_gate_fill_to_balance_vs_no_gate_late_repair90": no_gate_f2b_delta,
            "gate_parity_fill_to_balance_vs_gate_parity_late_repair90": gate_f2b_delta,
            "gate_parity_shadow_bucket_fill_to_balance_vs_gate_parity_late_repair90": gate_shadow_bucket_delta,
            "gate_parity_fill_to_balance_vs_no_gate_fill_to_balance": gate_vs_no_gate_f2b,
            "gate_parity_control_vs_no_gate_control": gate_control_vs_no_gate_control,
        },
        "references": {
            "local_lead_status": local_lead.get("status"),
            "size_aware_status": size_aware.get("status"),
            "diagnostic_shadow_status": shadow_summary.get("status"),
            "diagnostic_shadow_metrics": shadow_summary.get("metrics", {}),
        },
        "research_ranking": ranking,
        "promotion_gate": {
            "passed": False,
            "required_before_g2_canary": True,
            "status": "LOCAL_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "notes": [
                "This artifact is local causal verification only.",
                "No promotion can use it without a later no-order shadow and source-of-truth replay.",
            ],
        },
        "missing_or_approximate_fields": [
            "same-window candidate_base/candidate_registry for the 2026-05-20 diagnostic shadow window; Candidate Pipeline V1 currently allows labels through 20260518 and excludes later labels",
            "exact live market source_sequence_id ordering across book and trade events",
            "pending-order queue state and touch/fill timing from the no-order runner",
            "true shared-ingress trade/book interleaving for the diagnostic shadow window",
            "per-candidate event JSONL source links, intentionally not pulled for this workflow",
        ],
        "interpretation": [
            "This verifier approximates activation_opp_seen15 from candidate_base side sequence and applies pairing_only_when_residual before late-repair and activation gates.",
            "Because the diagnostic shadow is a 2026-05-20 live window and the local candidate-pipeline V1 data stops at 2026-05-18, this is gate-logic parity evidence rather than same-window transfer evidence.",
            "If gate parity collapses F2B opportunity volume or still diverges sharply from the diagnostic shadow, the transfer gap remains in live/no-order trigger parity rather than in fill-to-balance sizing arithmetic.",
        ],
        "next_executable_action": next_action,
        "deployable": False,
        "can_support_strategy_promotion": False,
        "g2_canary_ready": False,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "status": status,
                "decision_label": ranking["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "gate_f2b_candidates": gate_f2b.get("seed_late_fill_to_balance_candidate"),
                "gate_f2b_caps": gate_f2b.get("seed_qty_cap_late_fill_to_balance"),
                "gate_f2b_qty_reduction": gate_f2b.get("seed_late_fill_to_balance_qty_reduction"),
                "diagnostic_shadow_to_gate_f2b_candidate_ratio": ranking[
                    "diagnostic_shadow_to_gate_parity_f2b_candidate_ratio"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
