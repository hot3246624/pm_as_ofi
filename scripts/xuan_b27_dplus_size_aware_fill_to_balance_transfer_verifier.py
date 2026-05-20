#!/usr/bin/env python3
"""Verify whether local fill-to-balance survives shadow-like seed sizing.

This is a local-only causal verifier. It reads the derived candidate_base
DuckDB, local manifests, and pulled no-order shadow summary/audit JSON. It does
not read raw/replay/collector stores, full completion stores, event JSONL,
sockets, SSH, or remote state.
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


ARTIFACT = "xuan_b27_dplus_size_aware_fill_to_balance_transfer_verifier"
DEFAULT_CANDIDATE_BASE_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "local_20260502_20260518_paircap102"
)
DEFAULT_BASELINE_RESULT_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
    "20260502_20260518_publicfull_v2"
)
DEFAULT_LOCAL_LEAD = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_pipeline_shadow_aligned_fill_rerun_"
    "20260520T143629Z/manifest.json"
)
DEFAULT_DIAGNOSTIC_SHADOW = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_fill_to_balance_diagnostic_driver_"
    "20260520T191701Z"
)
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
class Mode:
    name: str
    fill_to_balance: str


MODES = [
    Mode("late_repair90_control", "off"),
    Mode("late_repair90_fill_to_balance_all_size_aware", "all"),
    Mode("late_repair90_fill_to_balance_shadow_observed_size_bucket", "shadow_observed"),
]


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


def load_state_machine_module() -> Any:
    script = Path(__file__).resolve().parent / "xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py"
    spec = importlib.util.spec_from_file_location("xuan_b27_dplus_candidate_pipeline_state_machine_rerun", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {script}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def as_float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def round6(value: Any) -> float:
    return round(as_float(value), 6)


def ratio(num: float, den: float) -> float | None:
    return round(float(num) / float(den), 8) if den else None


def offset_bucket(offset_s: float) -> str:
    if offset_s < 30:
        return "offset_0_30"
    if offset_s < 60:
        return "offset_30_60"
    if offset_s < 90:
        return "offset_60_90"
    if offset_s < 120:
        return "offset_90_120"
    return "offset_ge_120"


def qty_bucket(prefix: str, qty: float) -> str:
    if qty <= DUST:
        return f"{prefix}_zero"
    if qty <= 1.0 + DUST:
        return f"{prefix}_le_1"
    if qty <= 2.0 + DUST:
        return f"{prefix}_1_2"
    if qty < 5.0 - DUST:
        return f"{prefix}_2_5"
    if abs(qty - 5.0) <= 1e-9:
        return f"{prefix}_eq_5"
    return f"{prefix}_gt_5"


def deficit_bucket(deficit: float) -> str:
    return qty_bucket("deficit", deficit)


def add_count(bucket: dict[str, float], key: str, value: float = 1.0) -> None:
    bucket[key] = round(float(bucket.get(key, 0.0)) + float(value), 6)


def add_nested(bucket: dict[str, dict[str, float]], key: str, subkey: str, value: float = 1.0) -> None:
    child = bucket.setdefault(key, {})
    add_count(child, subkey, value)


def min_limiter(values: dict[str, float]) -> str:
    valid = {key: value for key, value in values.items() if math.isfinite(value)}
    if not valid:
        return "unknown"
    best = min(valid.values())
    tied = sorted(key for key, value in valid.items() if abs(value - best) <= 1e-9)
    return "+".join(tied)


def f2b_allowed(mode: Mode, base_qty: float, deficit_qty: float) -> bool:
    if mode.fill_to_balance == "off":
        return False
    if mode.fill_to_balance == "all":
        return True
    if mode.fill_to_balance == "shadow_observed":
        return base_qty <= 2.0 + DUST and 1.0 + DUST < deficit_qty <= 2.0 + DUST
    raise ValueError(f"unknown fill_to_balance mode {mode.fill_to_balance}")


def profile_for(sm: Any, mode: Mode) -> Any:
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
        "late_underweight_candidates": 0,
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
        "base_seed_qty_limiter_by_mode": {},
        "late_underweight_by_shadow_observed_bucket_match": {},
        "late_underweight_by_base_seed_qty_bucket": {},
        "late_underweight_by_deficit_bucket": {},
        "late_underweight_by_offset_side": {},
    }


def run_modes(candidate_base_db: Path, base_day_counts: dict[str, int]) -> dict[str, Any]:
    sm = load_state_machine_module()
    profiles = {mode.name: profile_for(sm, mode) for mode in MODES}
    metrics_by_mode: dict[str, defaultdict[str, float]] = {mode.name: defaultdict(float) for mode in MODES}
    states_by_mode: dict[str, dict[str, Any]] = {mode.name: {} for mode in MODES}
    trace_by_mode: dict[str, dict[str, Any]] = {mode.name: new_trace() for mode in MODES}

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

    def maybe_seed(row: dict[str, Any], mode: Mode) -> None:
        profile = profiles[mode.name]
        metrics = metrics_by_mode[mode.name]
        trace = trace_by_mode[mode.name]
        day = str(row["day"])
        sm.add_count(metrics, day, "candidate_count")
        side = str(row["side"])
        if side not in {"YES", "NO"}:
            return
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
        aged_cost = sm.aged_lot_cost(state.lots["YES"], ts_ms, profile.residual_cooldown_age_s) + sm.aged_lot_cost(
            state.lots["NO"], ts_ms, profile.residual_cooldown_age_s
        )
        if aged_cost > profile.residual_cooldown_cost_cap + 1e-12 and same_qty + profile.dust_qty >= opp_qty:
            sm.add_count(metrics, day, "seed_block_residual_cooldown")
            return
        if (
            profile.late_repair_only_after_s is not None
            and offset_s >= profile.late_repair_only_after_s
            and same_qty >= opp_qty
        ):
            sm.add_count(metrics, day, "seed_block_late_repair_only")
            return
        if same_qty >= profile.target_qty - profile.dust_qty:
            sm.add_count(metrics, day, "seed_block_target")
            return
        same_cost = sm.lot_cost(state.lots[side])
        opp_cost = sm.lot_cost(state.lots[opp_side])
        if max(0.0, same_cost - opp_cost) > profile.imbalance_cost_cap + 1e-12:
            sm.add_count(metrics, day, "seed_block_imbalance_cost")
            return

        seed_px = max(0.01, trade_px - profile.edge)
        open_cost = sm.lot_cost(state.lots["YES"]) + sm.lot_cost(state.lots["NO"])
        target_room = profile.target_qty - same_qty
        open_cost_room = (profile.max_open_cost - open_cost) / max(seed_px, 1e-9)
        imbalance_room = profile.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
        if imbalance_room <= profile.dust_qty:
            sm.add_count(metrics, day, "seed_block_imbalance_qty")
            return
        base_components = {
            "max_seed_qty": profile.max_seed_qty,
            "public_trade_size_fill_haircut": trade_size * profile.fill_haircut,
            "target_room": target_room,
            "open_cost_room": open_cost_room,
            "imbalance_room": imbalance_room,
        }
        base_qty = min(base_components.values())
        limiter = min_limiter(base_components)

        offset_key = offset_bucket(offset_s)
        side_offset_key = f"{side}|{offset_key}"
        late_underweight = (
            profile.late_repair_fill_to_balance_after_s is not None
            and offset_s >= profile.late_repair_fill_to_balance_after_s
            and same_qty < opp_qty
        )
        if late_underweight:
            deficit_qty = max(0.0, opp_qty - same_qty)
            trace["late_underweight_candidates"] += 1
            add_count(trace["late_underweight_by_base_seed_qty_bucket"], qty_bucket("base_seed_qty", base_qty))
            add_count(trace["late_underweight_by_deficit_bucket"], deficit_bucket(deficit_qty))
            add_nested(trace["late_underweight_by_offset_side"], side_offset_key, limiter)
            shadow_bucket_match = base_qty <= 2.0 + DUST and 1.0 + DUST < deficit_qty <= 2.0 + DUST
            add_count(
                trace["late_underweight_by_shadow_observed_bucket_match"],
                "match" if shadow_bucket_match else "not_match",
            )

        fill_to_balance_active = False
        if late_underweight:
            fill_to_balance_active = f2b_allowed(mode, base_qty, max(0.0, opp_qty - same_qty))
        qty = base_qty
        if fill_to_balance_active:
            deficit_qty = max(0.0, opp_qty - same_qty)
            capped_qty = min(qty, deficit_qty)
            deficit_key = deficit_bucket(deficit_qty)
            base_key = qty_bucket("base_seed_qty", base_qty)
            trace["f2b_candidates"] += 1
            add_count(trace["f2b_candidate_by_offset"], offset_key)
            add_count(trace["f2b_candidate_by_side"], side)
            add_count(trace["f2b_candidate_by_deficit_bucket"], deficit_key)
            add_count(trace["f2b_candidate_by_base_seed_qty_bucket"], base_key)
            add_count(trace["base_seed_qty_limiter_by_mode"], limiter)
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
        sm.pair_inventory(profile, state, metrics, ts_ms)

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
        profiles_out[mode.name] = {
            "metrics": sm.finish_metrics(profile, metrics, base_day_counts),
            "trace": trace_by_mode[mode.name],
        }
    return {
        "processed_public_sell_rows": row_count,
        "profiles": profiles_out,
    }


def delta(control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
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
    ]
    out: dict[str, Any] = {}
    for field in fields:
        c = as_float(control.get(field))
        v = as_float(variant.get(field))
        out[field] = {
            "control": round6(c),
            "variant": round6(v),
            "absolute_delta": round6(v - c),
            "relative_delta": ratio(v - c, c),
        }
    out["seed_action_retention"] = ratio(as_float(variant.get("seed_actions")), as_float(control.get("seed_actions")))
    out["pair_qty_retention"] = ratio(as_float(variant.get("pair_qty")), as_float(control.get("pair_qty")))
    out["fee_after_pnl_retention"] = ratio(
        as_float(variant.get("fee_after_pnl")), as_float(control.get("fee_after_pnl"))
    )
    out["stress100_worst_pnl_retention"] = ratio(
        as_float(variant.get("stress100_worst_pnl")), as_float(control.get("stress100_worst_pnl"))
    )
    out["residual_qty_rate_reduction"] = (
        None
        if as_float(control.get("residual_qty_rate")) == 0.0
        else round(1.0 - as_float(variant.get("residual_qty_rate")) / as_float(control.get("residual_qty_rate")), 8)
    )
    out["residual_cost_rate_reduction"] = (
        None
        if as_float(control.get("residual_cost_rate")) == 0.0
        else round(1.0 - as_float(variant.get("residual_cost_rate")) / as_float(control.get("residual_cost_rate")), 8)
    )
    return out


def summarize_shadow(shadow_artifact: Path) -> dict[str, Any]:
    aggregate = read_json(shadow_artifact / "remote" / "output" / "aggregate_report.json")
    audit = read_json(shadow_artifact / "audit_manifest.json")
    acceptance = read_json(
        Path("xuan_research_artifacts")
        / "xuan_b27_dplus_shadow_trading_acceptance_fill_to_balance_diagnostic_20260520T191701Z"
        / "manifest.json"
    )
    metrics = aggregate.get("metrics", {})
    diag = (aggregate.get("event_lite") or {}).get("late_repair_fill_to_balance_diagnostics") or {}
    return {
        "status": audit.get("status"),
        "metrics": {
            "candidates": metrics.get("candidates", 0),
            "queue_supported_fills": metrics.get("queue_supported_fills", 0),
            "pair_actions": metrics.get("pair_actions", 0),
            "pair_qty": metrics.get("pair_qty", 0),
            "pair_pnl": metrics.get("pair_pnl", 0),
            "net_pair_cost_p90": metrics.get("net_pair_cost_proxy_p90", 0),
            "residual_qty": metrics.get("residual_qty", 0),
            "residual_cost": metrics.get("residual_cost", 0),
            "late_repair_fill_to_balance_candidates": metrics.get("late_repair_fill_to_balance_candidates", 0),
            "late_repair_fill_to_balance_caps": metrics.get("late_repair_fill_to_balance_caps", 0),
            "late_repair_fill_to_balance_blocks": metrics.get("late_repair_fill_to_balance_blocks", 0),
            "late_repair_fill_to_balance_qty_reduction": metrics.get(
                "late_repair_fill_to_balance_qty_reduction", 0
            ),
        },
        "diagnostics": diag,
        "research_ranking": acceptance.get("research_ranking", {}),
        "promotion_gate": acceptance.get("promotion_gate", {}),
    }


def classify(
    all_f2b: dict[str, Any],
    shadow_bucket: dict[str, Any],
    all_delta: dict[str, Any],
    shadow_delta: dict[str, Any],
    shadow_summary: dict[str, Any],
) -> dict[str, Any]:
    all_caps = as_float(all_f2b.get("seed_qty_cap_late_fill_to_balance"))
    shadow_caps = as_float(shadow_bucket.get("seed_qty_cap_late_fill_to_balance"))
    all_reduction = as_float(all_f2b.get("seed_late_fill_to_balance_qty_reduction"))
    shadow_reduction = as_float(shadow_bucket.get("seed_late_fill_to_balance_qty_reduction"))
    all_residual_reduction = as_float(all_delta.get("residual_qty_rate_reduction"))
    shadow_residual_reduction = as_float(shadow_delta.get("residual_qty_rate_reduction"))
    shadow_economics_positive = (
        as_float(shadow_bucket.get("fee_after_pnl")) > 0
        and as_float(shadow_bucket.get("stress100_worst_pnl")) > 0
        and as_float(shadow_bucket.get("worst_day_fee_after_pnl")) > 0
    )
    cap_retention = ratio(shadow_caps, all_caps) or 0.0
    qty_reduction_retention = ratio(shadow_reduction, all_reduction) or 0.0
    risk_adjusted_proxy = (
        as_float(shadow_delta["fee_after_pnl"]["absolute_delta"])
        - as_float(shadow_delta["residual_cost"]["absolute_delta"])
        + as_float(shadow_delta["stress100_worst_pnl"]["absolute_delta"])
    )
    if not shadow_economics_positive:
        decision = "DISCARD"
        label = "DISCARD_SIZE_AWARE_FILL_TO_BALANCE_ECONOMIC_NEGATIVE"
    elif cap_retention < 0.05 and qty_reduction_retention < 0.05 and shadow_residual_reduction < max(0.10, all_residual_reduction * 0.25):
        decision = "DISCARD"
        label = "DISCARD_SIZE_AWARE_FILL_TO_BALANCE_BENEFIT_COLLAPSES_TO_SHADOW_BUCKETS"
    else:
        decision = "UNKNOWN"
        label = "UNKNOWN_SIZE_AWARE_FILL_TO_BALANCE_MIXED"
    return {
        "decision_label": decision,
        "label": label,
        "shadow_bucket_fee_stress_worst_day_positive": shadow_economics_positive,
        "all_f2b_residual_qty_rate_reduction": all_residual_reduction,
        "shadow_bucket_residual_qty_rate_reduction": shadow_residual_reduction,
        "shadow_bucket_cap_retention_vs_all_f2b": cap_retention,
        "shadow_bucket_qty_reduction_retention_vs_all_f2b": qty_reduction_retention,
        "shadow_bucket_delta_risk_adjusted_proxy": round6(risk_adjusted_proxy),
        "shadow_observed_no_order_caps": shadow_summary["metrics"].get("late_repair_fill_to_balance_caps", 0),
        "shadow_observed_no_order_qty_reduction": shadow_summary["metrics"].get(
            "late_repair_fill_to_balance_qty_reduction", 0
        ),
        "notes": [
            "Current candidate-pipeline state machine already uses the same base seed qty formula available locally: min(max_seed_qty, public_trade_size*fill_haircut, target_room, open_cost_room, imbalance_room).",
            "The all-fill-to-balance local KEEP is dominated by size buckets that did not appear in the diagnostic no-order shadow.",
            "The shadow-observed-size-bucket variant tests only the base_qty<=2 and deficit_1_2 opportunity surface seen in pulled diagnostic summaries.",
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument("--baseline-result-dir", type=Path, default=DEFAULT_BASELINE_RESULT_DIR)
    parser.add_argument("--local-lead-manifest", type=Path, default=DEFAULT_LOCAL_LEAD)
    parser.add_argument("--diagnostic-shadow-artifact", type=Path, default=DEFAULT_DIAGNOSTIC_SHADOW)
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
    ]
    missing = [str(path) for path in required_paths if not path.exists()]
    unsafe = [str(path) for path in required_paths if path.exists() and not path_safe(path)]
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

    candidate_manifest_json = read_json(candidate_manifest)
    result_manifest_json = read_json(result_manifest)
    compliance_manifest_json = read_json(compliance_manifest)
    local_lead = read_json(args.local_lead_manifest)
    shadow_summary = summarize_shadow(args.diagnostic_shadow_artifact)

    con = duckdb.connect(str(candidate_db), read_only=True)
    base_day_counts = {
        row[0]: int(row[1])
        for row in con.execute("select day, count(*) from candidate_base group by 1 order by 1").fetchall()
    }
    con.close()

    run = run_modes(candidate_db, base_day_counts)
    control = run["profiles"]["late_repair90_control"]["metrics"]
    all_f2b = run["profiles"]["late_repair90_fill_to_balance_all_size_aware"]["metrics"]
    shadow_bucket = run["profiles"]["late_repair90_fill_to_balance_shadow_observed_size_bucket"]["metrics"]
    all_delta = delta(control, all_f2b)
    shadow_delta = delta(control, shadow_bucket)
    ranking = classify(all_f2b, shadow_bucket, all_delta, shadow_delta, shadow_summary)

    status = ranking["label"]
    next_action = (
        "Freeze fill-to-balance90 as a shadow candidate and choose a new local-only causal verifier that addresses "
        "the no-order gate parity gap, especially activation_opp_seen/pairing_only_when_residual versus the "
        "candidate-pipeline state machine; do not run EC2 shadow/canary before a new local verifier returns KEEP."
        if ranking["decision_label"] == "DISCARD"
        else "Name the remaining missing fields and run only a local no-network parity verifier before any EC2 shadow."
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
            "shadow_observed_size_bucket": {
                "base_seed_qty_max": 2.0,
                "deficit_min_exclusive": 1.0,
                "deficit_max_inclusive": 2.0,
            },
        },
        "processed_public_sell_rows": run["processed_public_sell_rows"],
        "profiles": {
            name: {
                "metrics": data["metrics"],
                "trace": data["trace"],
            }
            for name, data in run["profiles"].items()
        },
        "deltas": {
            "all_size_aware_fill_to_balance_vs_late_repair90": all_delta,
            "shadow_observed_size_bucket_fill_to_balance_vs_late_repair90": shadow_delta,
        },
        "local_lead_reference": {
            "status": local_lead.get("status"),
            "research_ranking": local_lead.get("research_ranking"),
            "summary": local_lead.get("summary"),
        },
        "diagnostic_shadow_summary": shadow_summary,
        "research_ranking": ranking,
        "promotion_gate": {
            "passed": False,
            "required_before_g2_canary": True,
            "status": "LOCAL_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "notes": [
                "This artifact is local causal verification only.",
                "No shadow/canary promotion can use this without a later no-order shadow and source-of-truth replay.",
            ],
        },
        "interpretation": [
            "The local state-machine already applies size-aware seed qty constraints available in candidate_base.",
            "When restricted to the shadow-observed base_qty<=2 and deficit_1_2 surface, fill-to-balance no longer represents the broad local cap mechanism.",
            "The diagnostic no-order shadow showed only one cap and 0.0425 qty reduction, so the prior local KEEP does not currently transfer to the live/no-order path.",
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
                "shadow_bucket_cap_retention": ranking["shadow_bucket_cap_retention_vs_all_f2b"],
                "shadow_bucket_qty_reduction_retention": ranking[
                    "shadow_bucket_qty_reduction_retention_vs_all_f2b"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
