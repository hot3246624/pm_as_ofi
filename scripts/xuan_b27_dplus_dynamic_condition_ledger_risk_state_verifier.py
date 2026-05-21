#!/usr/bin/env python3
"""Verify a dynamic condition-ledger risk-state gate locally.

This verifier reads only derived candidate_base and local manifests. It replays
the shadow-aligned late_repair90 state machine and compares it with an online
condition-ledger gate that blocks only risk-increasing seeds after that
condition's conservative ledger proxy turns negative. It does not read
raw/replay/collector stores, full completion stores, event JSONL, sockets, SSH,
or remote state, and it does not modify runner/trading paths.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ARTIFACT = "xuan_b27_dplus_dynamic_condition_ledger_risk_state_verifier"
STATE_MACHINE_SCRIPT = "xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py"
DEFAULT_CANDIDATE_BASE_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "local_20260502_20260518_paircap102"
)
DEFAULT_BASELINE_RESULT_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_"
    "20260502_20260518_publicfull_v2"
)
DEFAULT_STATUS_FREEZE = Path(
    "xuan_research_artifacts/xuan_b27_dplus_status_hygiene_portfolio_ledger_freeze_"
    "20260521T034715Z/manifest.json"
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


@dataclass
class ConditionLedger:
    condition_id: str
    day: str
    slug: str
    winner_side: str | None
    seed_actions: int = 0
    gross_buy_qty: float = 0.0
    gross_buy_cost: float = 0.0
    official_taker_fee: float = 0.0
    pair_actions: int = 0
    pair_qty: float = 0.0
    pair_cost_sum: float = 0.0
    pair_pnl: float = 0.0
    pair_delay_ms_qty_weighted: float = 0.0
    residual_qty: float = 0.0
    residual_cost: float = 0.0
    residual_settle_payout: float = 0.0
    residual_settle_pnl: float = 0.0
    residual_by_side: dict[str, float] = field(default_factory=lambda: {"YES": 0.0, "NO": 0.0})
    residual_cost_by_side: dict[str, float] = field(default_factory=lambda: {"YES": 0.0, "NO": 0.0})

    def avg_pair_cost(self) -> float:
        return self.pair_cost_sum / self.pair_qty if self.pair_qty > DUST else 0.0

    def conservative_final_proxy(self) -> float:
        return self.pair_pnl - self.official_taker_fee - self.residual_cost - 0.01 * (
            2.0 * self.pair_qty + self.residual_qty
        )


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
    path = Path(__file__).resolve().with_name(STATE_MACHINE_SCRIPT)
    spec = importlib.util.spec_from_file_location("xuan_b27_dplus_state_machine", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def round6(value: float) -> float:
    return round(float(value), 6)


def ratio(num: float, den: float) -> float | None:
    return round(float(num) / float(den), 8) if den else None


def rel_delta(new: float, old: float) -> float | None:
    return round((float(new) - float(old)) / float(old), 8) if old else None


def profile_for(sm: Any, name: str) -> Any:
    return sm.Profile(
        name=name,
        seed_offset_max_s=120.0,
        late_repair_only_after_s=90.0,
        edge=0.07,
        target_qty=5.0,
        max_open_cost=80.0,
        imbalance_qty_cap=1.25,
    )


def ledger_for(ledgers: dict[str, ConditionLedger], state: Any) -> ConditionLedger:
    condition_id = str(getattr(state, "condition_id"))
    ledger = ledgers.get(condition_id)
    if ledger is None:
        ledger = ConditionLedger(
            condition_id=condition_id,
            day=str(state.day),
            slug=str(state.slug),
            winner_side=str(state.winner_side) if state.winner_side else None,
        )
        ledgers[condition_id] = ledger
    return ledger


def current_unpaired_qty(sm: Any, state: Any) -> float:
    return sm.lot_qty(state.lots["YES"]) + sm.lot_qty(state.lots["NO"])


def current_unpaired_cost(sm: Any, state: Any) -> float:
    return sm.lot_cost(state.lots["YES"]) + sm.lot_cost(state.lots["NO"])


def online_risk_adjusted_proxy(sm: Any, state: Any, ledger: ConditionLedger) -> float:
    residual_qty_proxy = current_unpaired_qty(sm, state)
    return ledger.pair_pnl - ledger.official_taker_fee - current_unpaired_cost(sm, state) - 0.01 * (
        2.0 * ledger.pair_qty + residual_qty_proxy
    )


def pair_inventory_traced(sm: Any, profile: Any, state: Any, metrics: Any, ledgers: dict[str, ConditionLedger], ts_ms: int) -> None:
    yes = state.lots["YES"]
    no = state.lots["NO"]
    ledger = ledger_for(ledgers, state)
    while yes and no:
        yes_lot = yes[0]
        no_lot = no[0]
        take = min(yes_lot.qty, no_lot.qty)
        if take <= DUST:
            break
        pair_cost = yes_lot.px + no_lot.px
        older_ts = min(yes_lot.ts_ms, no_lot.ts_ms)
        sm.add_count(metrics, state.day, "pair_actions")
        sm.add_metric(metrics, state.day, "pair_qty", take)
        sm.add_metric(metrics, state.day, "pair_cost_sum", take * pair_cost)
        sm.add_metric(metrics, state.day, "pair_pnl", take * (1.0 - pair_cost))
        sm.add_metric(metrics, state.day, "pair_delay_ms", take * max(0, ts_ms - older_ts))
        ledger.pair_actions += 1
        ledger.pair_qty += take
        ledger.pair_cost_sum += take * pair_cost
        ledger.pair_pnl += take * (1.0 - pair_cost)
        ledger.pair_delay_ms_qty_weighted += take * max(0, ts_ms - older_ts)
        yes_lot.qty -= take
        no_lot.qty -= take
        if yes_lot.qty <= DUST:
            sm.pop_lot(yes, 0)
        if no_lot.qty <= DUST:
            sm.pop_lot(no, 0)


def maybe_seed_with_dynamic_gate(
    sm: Any,
    row: dict[str, Any],
    profile: Any,
    state: Any,
    metrics: Any,
    ledgers: dict[str, ConditionLedger],
    gate_enabled: bool,
) -> None:
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
    if profile.public_trade_px_hi is not None and trade_px > profile.public_trade_px_hi:
        sm.add_count(metrics, day, "seed_block_public_trade_px_hi")
        return
    if not (profile.seed_px_lo <= trade_px <= profile.seed_px_hi):
        sm.add_count(metrics, day, "seed_block_price_band")
        return
    l1_pair_ask = float(row["l1_pair_ask"])
    if not math.isfinite(l1_pair_ask) or l1_pair_ask > profile.seed_l1_pair_cap + 1e-12:
        sm.add_count(metrics, day, "seed_block_l1_pair_cap")
        return
    ts_ms = int(row["ts_ms"])
    if ts_ms - state.last_seed_ts_ms < profile.cooldown_s * 1000.0:
        sm.add_count(metrics, day, "seed_block_cooldown")
        return

    same_qty = sm.lot_qty(state.lots[side])
    opp_qty = sm.lot_qty(state.lots[sm.other(side)])
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
        sm.add_count(metrics, day, "seed_block_risk_increasing_public_trade_px_hi")
        return
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
    ledger = ledger_for(ledgers, state)
    if gate_enabled:
        proxy = online_risk_adjusted_proxy(sm, state, ledger)
        if proxy < -1e-12:
            sm.add_count(metrics, day, "condition_ledger_risk_negative_candidates")
            if risk_increasing_seed:
                sm.add_count(metrics, day, "seed_block_condition_ledger_risk_state")
                sm.add_count(metrics, day, f"seed_block_condition_ledger_risk_state_{side.lower()}")
                return
            sm.add_count(metrics, day, "condition_ledger_risk_negative_repair_allowed")
    if same_qty >= profile.target_qty - profile.dust_qty:
        sm.add_count(metrics, day, "seed_block_target")
        return
    same_cost = sm.lot_cost(state.lots[side])
    opp_cost = sm.lot_cost(state.lots[sm.other(side)])
    if max(0.0, same_cost - opp_cost) > profile.imbalance_cost_cap + 1e-12:
        sm.add_count(metrics, day, "seed_block_imbalance_cost")
        return

    seed_px = max(0.01, trade_px - profile.edge)
    open_cost = sm.lot_cost(state.lots["YES"]) + sm.lot_cost(state.lots["NO"])
    effective_imbalance_qty_cap = (
        profile.risk_increasing_imbalance_qty_cap
        if risk_increasing_seed and profile.risk_increasing_imbalance_qty_cap is not None
        else profile.imbalance_qty_cap
    )
    imbalance_room = effective_imbalance_qty_cap - max(0.0, same_qty - opp_qty)
    if imbalance_room <= profile.dust_qty:
        if risk_increasing_seed and profile.risk_increasing_imbalance_qty_cap is not None:
            sm.add_count(metrics, day, "seed_block_dynamic_imbalance_qty")
        else:
            sm.add_count(metrics, day, "seed_block_imbalance_qty")
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
        sm.Lot(
            qty=qty,
            px=seed_px,
            ts_ms=ts_ms,
            side=side,
            source_candidate_row_id=str(row["candidate_row_id"]),
        )
    )
    state.last_seed_ts_ms = ts_ms
    state.active = True
    fee = sm.official_taker_fee(qty, seed_px, profile.official_fee_rate)
    sm.add_count(metrics, day, "seed_actions")
    sm.add_metric(metrics, day, "gross_buy_qty", qty)
    sm.add_metric(metrics, day, "gross_buy_cost", qty * seed_px)
    sm.add_metric(metrics, day, "official_taker_fee", fee)
    ledger.seed_actions += 1
    ledger.gross_buy_qty += qty
    ledger.gross_buy_cost += qty * seed_px
    ledger.official_taker_fee += fee
    pair_inventory_traced(sm, profile, state, metrics, ledgers, ts_ms)


def settle_traced(sm: Any, states: dict[str, Any], profile: Any, metrics: Any, ledgers: dict[str, ConditionLedger]) -> None:
    for state in states.values():
        if not state.active:
            continue
        pair_inventory_traced(sm, profile, state, metrics, ledgers, state.last_ts_ms)
        sm.add_count(metrics, state.day, "active_markets")
        ledger = ledger_for(ledgers, state)
        winner = state.winner_side
        for side in ("YES", "NO"):
            for lot in state.lots[side]:
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                payout = lot.qty if winner == side else 0.0
                sm.add_metric(metrics, state.day, "residual_qty", lot.qty)
                sm.add_metric(metrics, state.day, "residual_cost", cost)
                sm.add_metric(metrics, state.day, "residual_settle_payout", payout)
                sm.add_metric(metrics, state.day, "residual_settle_pnl", payout - cost)
                ledger.residual_qty += lot.qty
                ledger.residual_cost += cost
                ledger.residual_settle_payout += payout
                ledger.residual_settle_pnl += payout - cost
                ledger.residual_by_side[side] += lot.qty
                ledger.residual_cost_by_side[side] += cost


def run_profile(candidate_base_db: Path, base_day_counts: dict[str, int], gate_enabled: bool) -> dict[str, Any]:
    sm = load_state_machine_module()
    profile = profile_for(
        sm,
        "dynamic_condition_ledger_risk_state_gate" if gate_enabled else "late_repair90_control",
    )
    metrics = defaultdict(float)
    states: dict[str, Any] = {}
    ledgers: dict[str, ConditionLedger] = {}
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
    processed = 0
    con = duckdb.connect(str(candidate_base_db), read_only=True)
    try:
        cursor = con.execute(query)
        while True:
            rows = cursor.fetchmany(50_000)
            if not rows:
                break
            for raw in rows:
                processed += 1
                row = dict(zip(select_cols, raw))
                state = sm.state_for(states, row)
                setattr(state, "condition_id", str(row["condition_id"]))
                maybe_seed_with_dynamic_gate(sm, row, profile, state, metrics, ledgers, gate_enabled)
        settle_traced(sm, states, profile, metrics, ledgers)
    finally:
        con.close()
    profile_metrics = sm.finish_metrics(profile, metrics, base_day_counts)
    profile_metrics.update(
        {
            "seed_block_condition_ledger_risk_state": int(metrics["seed_block_condition_ledger_risk_state"]),
            "seed_block_condition_ledger_risk_state_yes": int(metrics["seed_block_condition_ledger_risk_state_yes"]),
            "seed_block_condition_ledger_risk_state_no": int(metrics["seed_block_condition_ledger_risk_state_no"]),
            "condition_ledger_risk_negative_candidates": int(metrics["condition_ledger_risk_negative_candidates"]),
            "condition_ledger_risk_negative_repair_allowed": int(
                metrics["condition_ledger_risk_negative_repair_allowed"]
            ),
        }
    )
    return {
        "processed_public_sell_rows": processed,
        "profile": profile_metrics,
        "ledgers": ledgers,
    }


def ledgers_summary(ledgers: dict[str, ConditionLedger]) -> dict[str, Any]:
    rows = [ledger for ledger in ledgers.values() if ledger.seed_actions > 0]
    nonnegative = [ledger for ledger in rows if ledger.conservative_final_proxy() >= 0.0]
    negative = [ledger for ledger in rows if ledger.conservative_final_proxy() < 0.0]
    top_negative = sorted(negative, key=lambda item: item.conservative_final_proxy())[:20]
    return {
        "condition_count": len(rows),
        "risk_adjusted_nonnegative_conditions": len(nonnegative),
        "risk_adjusted_negative_conditions": len(negative),
        "top_negative_conditions": [
            {
                "condition_id": ledger.condition_id,
                "day": ledger.day,
                "slug": ledger.slug,
                "seed_actions": ledger.seed_actions,
                "pair_qty": round6(ledger.pair_qty),
                "avg_pair_cost": round6(ledger.avg_pair_cost()),
                "pair_pnl": round6(ledger.pair_pnl),
                "official_taker_fee": round6(ledger.official_taker_fee),
                "residual_qty": round6(ledger.residual_qty),
                "residual_cost": round6(ledger.residual_cost),
                "conservative_risk_adjusted_proxy": round6(ledger.conservative_final_proxy()),
                "residual_by_side": {key: round6(value) for key, value in ledger.residual_by_side.items()},
                "residual_cost_by_side": {
                    key: round6(value) for key, value in ledger.residual_cost_by_side.items()
                },
            }
            for ledger in top_negative
        ],
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
        "residual_qty_rate",
        "residual_cost_rate",
        "worst_residual_net_pnl",
        "seed_block_condition_ledger_risk_state",
    ]
    out: dict[str, Any] = {}
    for field_name in fields:
        c = float(control.get(field_name) or 0.0)
        v = float(variant.get(field_name) or 0.0)
        out[field_name] = {
            "control": round6(c),
            "variant": round6(v),
            "absolute_delta": round6(v - c),
            "relative_delta": rel_delta(v, c),
        }
    out["seed_action_retention"] = ratio(float(variant["seed_actions"]), float(control["seed_actions"]))
    out["pair_qty_retention"] = ratio(float(variant["pair_qty"]), float(control["pair_qty"]))
    out["active_market_retention"] = ratio(float(variant["active_markets"]), float(control["active_markets"]))
    out["residual_qty_rate_reduction"] = (
        round6(1.0 - float(variant["residual_qty_rate"]) / float(control["residual_qty_rate"]))
        if control.get("residual_qty_rate")
        else None
    )
    out["residual_cost_rate_reduction"] = (
        round6(1.0 - float(variant["residual_cost_rate"]) / float(control["residual_cost_rate"]))
        if control.get("residual_cost_rate")
        else None
    )
    out["risk_adjusted_proxy_delta"] = round6(
        (float(variant["fee_after_pnl"]) + float(variant["stress100_worst_pnl"]))
        - (float(control["fee_after_pnl"]) + float(control["stress100_worst_pnl"]))
    )
    return out


def classify(control: dict[str, Any], variant: dict[str, Any], d: dict[str, Any]) -> tuple[str, str]:
    fee_positive = float(variant["fee_after_pnl"]) > 0.0
    stress_positive = float(variant["stress100_worst_pnl"]) > 0.0
    worst_day_positive = float(variant["worst_day_fee_after_pnl"]) > 0.0
    seed_retention = float(d["seed_action_retention"] or 0.0)
    pair_retention = float(d["pair_qty_retention"] or 0.0)
    residual_qty_reduction = float(d["residual_qty_rate_reduction"] or 0.0)
    residual_cost_reduction = float(d["residual_cost_rate_reduction"] or 0.0)
    stress_delta = float(d["stress100_worst_pnl"]["absolute_delta"])
    fee_delta = float(d["fee_after_pnl"]["absolute_delta"])
    block_count = int(variant.get("seed_block_condition_ledger_risk_state") or 0)
    if not (fee_positive and stress_positive and worst_day_positive):
        return "DISCARD", "DISCARD_DYNAMIC_CONDITION_LEDGER_ECONOMIC_NEGATIVE"
    if seed_retention < 0.65 or pair_retention < 0.65:
        return "DISCARD", "DISCARD_DYNAMIC_CONDITION_LEDGER_SAMPLE_DESTROYED"
    if block_count <= 0:
        return "UNKNOWN", "UNKNOWN_DYNAMIC_CONDITION_LEDGER_NO_ONLINE_EFFECT"
    if (
        seed_retention >= 0.80
        and pair_retention >= 0.80
        and (residual_qty_reduction > 0.0 or residual_cost_reduction > 0.0 or stress_delta > 0.0)
        and (fee_delta > -150.0)
        and float(d["risk_adjusted_proxy_delta"]) > 0.0
    ):
        return "KEEP", "KEEP_DYNAMIC_CONDITION_LEDGER_RISK_STATE_LOCAL_RESEARCH_ONLY"
    if residual_qty_reduction < 0.0 and residual_cost_reduction < 0.0 and stress_delta < 0.0:
        return "DISCARD", "DISCARD_DYNAMIC_CONDITION_LEDGER_RISK_WORSE"
    return "UNKNOWN", "UNKNOWN_DYNAMIC_CONDITION_LEDGER_MIXED_LOCAL_RESULT"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument("--baseline-result-dir", type=Path, default=DEFAULT_BASELINE_RESULT_DIR)
    parser.add_argument("--status-freeze-manifest", type=Path, default=DEFAULT_STATUS_FREEZE)
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
    required = [candidate_manifest, candidate_db, result_manifest, compliance_manifest, args.status_freeze_manifest]
    missing = [str(path) for path in required if not path.exists()]
    unsafe = [str(path) for path in required if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": label,
            "lane": "causal_verifier",
            "decision_label": "BLOCKED",
            "status": "BLOCKED_DYNAMIC_CONDITION_LEDGER_INPUT_UNAVAILABLE",
            "missing_paths": missing,
            "unsafe_paths": unsafe,
            "next_executable_action": "Restore required local candidate-pipeline/status artifacts and rerun locally.",
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps({"status": manifest["status"], "manifest": str(output_dir / "manifest.json")}, indent=2))
        return 2

    candidate_manifest_json = read_json(candidate_manifest)
    result_manifest_json = read_json(result_manifest)
    compliance_manifest_json = read_json(compliance_manifest)
    status_freeze_json = read_json(args.status_freeze_manifest)
    con = duckdb.connect(str(candidate_db), read_only=True)
    try:
        base_day_counts = {
            str(row[0]): int(row[1])
            for row in con.execute("select day, count(*) from candidate_base group by 1 order by 1").fetchall()
        }
    finally:
        con.close()

    control_replay = run_profile(candidate_db, base_day_counts, gate_enabled=False)
    variant_replay = run_profile(candidate_db, base_day_counts, gate_enabled=True)
    control = control_replay["profile"]
    variant = variant_replay["profile"]
    d = delta(control, variant)
    decision, status = classify(control, variant, d)
    control_ledgers_summary = ledgers_summary(control_replay["ledgers"])
    variant_ledgers_summary = ledgers_summary(variant_replay["ledgers"])
    result_core = result_manifest_json.get("core_metrics") or {}
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "lane": "causal_verifier",
        "decision_label": decision,
        "status": status,
        "scope": {
            "local_only": True,
            "candidate_base_duckdb_read_only": True,
            "candidate_registry_compatible": True,
            "private_owner_trade_truth_used": False,
            "ssh_used": False,
            "shared_ingress_connected": False,
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
            "status_freeze_manifest": str(args.status_freeze_manifest),
        },
        "coverage": {
            "candidate_base_labels": candidate_manifest_json.get("labels", []),
            "candidate_base_days": candidate_manifest_json.get("days", []),
            "candidate_base_excluded_labels_or_days": candidate_manifest_json.get("excluded_labels_or_days", []),
            "candidate_base_row_count": candidate_manifest_json.get("row_count"),
            "baseline_core_metrics": {
                "seed_actions": result_core.get("seed_actions"),
                "pair_qty": result_core.get("pair_qty"),
                "fee_after_pnl": result_core.get("fee_after_pnl"),
                "stress100_worst_pnl": result_core.get("stress100_worst_pnl"),
                "residual_qty_rate": result_core.get("residual_qty_rate"),
                "residual_cost_rate": result_core.get("residual_cost_rate"),
            },
            "public_account_audit": compliance_manifest_json.get("public_account_audit", {}),
        },
        "frozen_context": {
            "fill_to_balance90_frozen": True,
            "portfolio_ledger_diagnostic_3600s_shadow_frozen": True,
            "status_freeze": status_freeze_json.get("status"),
            "notes": [
                "The verifier does not use fill-to-balance90, public/source price caps, static target/cap sweeps, residual cooldown/admission caps, or activation window widening.",
                "The gate is online per seed and blocks only risk-increasing seeds when the current condition ledger proxy is negative.",
            ],
        },
        "mechanism": {
            "name": "dynamic_condition_ledger_risk_state_gate",
            "control": "shadow-aligned late_repair90, edge=0.07, target_qty=5, max_open_cost=80, imbalance_qty_cap=1.25",
            "variant": "same control plus online condition-ledger risk-state gate",
            "online_risk_proxy": "pair_pnl - official_taker_fee - current_unpaired_cost - 0.01 * (2*pair_qty + current_unpaired_qty)",
            "blocked_seed_rule": "if proxy < 0 and same_side_unpaired_qty >= opposite_side_unpaired_qty, block seed",
            "allowed_seed_rule": "underweight/repair or pairing-improving seeds remain allowed even when proxy is negative",
            "summary_only_removal": False,
        },
        "processed_public_sell_rows": {
            "control": control_replay["processed_public_sell_rows"],
            "variant": variant_replay["processed_public_sell_rows"],
        },
        "control": control,
        "variant": variant,
        "delta_vs_control": d,
        "control_condition_ledger_summary": control_ledgers_summary,
        "variant_condition_ledger_summary": variant_ledgers_summary,
        "research_ranking": {
            "decision": decision,
            "label": status,
            "seed_action_retention": d["seed_action_retention"],
            "pair_qty_retention": d["pair_qty_retention"],
            "residual_qty_rate_reduction": d["residual_qty_rate_reduction"],
            "residual_cost_rate_reduction": d["residual_cost_rate_reduction"],
            "risk_state_block_count": variant.get("seed_block_condition_ledger_risk_state"),
            "risk_adjusted_proxy_delta": d["risk_adjusted_proxy_delta"],
            "notes": [
                "This is local candidate-pipeline research evidence only.",
                "A KEEP result would still need default-off implementability review before any no-order shadow.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_CANDIDATE_PIPELINE_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "can_support_strategy_promotion": False,
            "g2_canary_ready": False,
            "notes": [
                "No EC2 shadow/canary, source-of-truth replay, or live/no-order validation was run.",
                "Promotion remains blocked until separate no-order and source-of-truth validation pass.",
            ],
        },
        "next_executable_action": (
            "Run implementability_review for a default-off dynamic condition-ledger diagnostic/gate spec and local smoke only; do not start EC2 shadow/canary."
            if decision == "KEEP"
            else "Do not start EC2 shadow/canary; freeze or revise this local mechanism based on the verifier result."
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    print(json.dumps({"status": status, "decision": decision, "manifest": str(output_dir / "manifest.json")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
