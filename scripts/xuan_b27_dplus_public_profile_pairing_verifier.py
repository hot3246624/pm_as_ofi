#!/usr/bin/env python3
"""Verify the public-profile two-sided inventory pairing hypothesis locally.

This verifier reads only derived candidate_base and local manifests. It replays
the late_repair90 state machine and evaluates portfolio-level paired-inventory
ranking rules over condition-level ledgers. It does not read raw/replay/
collector stores, full completion stores, event JSONL, sockets, SSH, or remote
state, and it does not modify runner/trading paths.
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


ARTIFACT = "xuan_b27_dplus_public_profile_pairing_verifier"
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
DEFAULT_PUBLIC_PROFILE_REVIEW = Path(
    "xuan_research_artifacts/xuan_b27_dplus_public_profile_strategy_review_"
    "20260520T225109Z/manifest.json"
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

    def fee_after_pnl(self) -> float:
        return self.pair_pnl + self.residual_settle_pnl - self.official_taker_fee

    def conservative_risk_adjusted(self) -> float:
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


def profile_for(sm: Any) -> Any:
    return sm.Profile(
        name="shadow_aligned_late_repair90_control",
        seed_offset_max_s=120.0,
        late_repair_only_after_s=90.0,
        edge=0.07,
        target_qty=5.0,
        max_open_cost=80.0,
        imbalance_qty_cap=1.25,
    )


def new_metrics(sm: Any) -> Any:
    return defaultdict(float)


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


def run_late_repair90(candidate_base_db: Path, base_day_counts: dict[str, int]) -> dict[str, Any]:
    sm = load_state_machine_module()
    profile = profile_for(sm)
    metrics = new_metrics(sm)
    states: dict[str, Any] = {}
    ledgers: dict[str, ConditionLedger] = {}

    def traced_pair_inventory(profile: Any, state: Any, metrics: Any, ts_ms: int, **_: Any) -> None:
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

    original_pair_inventory = sm.pair_inventory
    sm.pair_inventory = traced_pair_inventory
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
    con = duckdb.connect(str(candidate_base_db), read_only=True)
    try:
        cursor = con.execute(query)
        processed = 0
        while True:
            rows = cursor.fetchmany(50_000)
            if not rows:
                break
            for raw in rows:
                processed += 1
                row = dict(zip(select_cols, raw))
                state = sm.state_for(states, row)
                setattr(state, "condition_id", str(row["condition_id"]))
                before = {
                    "seed_actions": float(metrics["seed_actions"]),
                    "gross_buy_qty": float(metrics["gross_buy_qty"]),
                    "gross_buy_cost": float(metrics["gross_buy_cost"]),
                    "official_taker_fee": float(metrics["official_taker_fee"]),
                }
                sm.maybe_seed(row, profile, state, metrics)
                after_seed_actions = float(metrics["seed_actions"]) - before["seed_actions"]
                if after_seed_actions > 0:
                    ledger = ledger_for(ledgers, state)
                    ledger.seed_actions += int(after_seed_actions)
                    ledger.gross_buy_qty += float(metrics["gross_buy_qty"]) - before["gross_buy_qty"]
                    ledger.gross_buy_cost += float(metrics["gross_buy_cost"]) - before["gross_buy_cost"]
                    ledger.official_taker_fee += float(metrics["official_taker_fee"]) - before["official_taker_fee"]
        sm.settle(states, profile, metrics)
    finally:
        sm.pair_inventory = original_pair_inventory
        con.close()

    for state in states.values():
        if not state.active:
            continue
        ledger = ledger_for(ledgers, state)
        winner = state.winner_side
        for side in ("YES", "NO"):
            for lot in state.lots[side]:
                if lot.qty <= DUST:
                    continue
                cost = lot.qty * lot.px
                payout = lot.qty if winner == side else 0.0
                ledger.residual_qty += lot.qty
                ledger.residual_cost += cost
                ledger.residual_settle_payout += payout
                ledger.residual_settle_pnl += payout - cost
                ledger.residual_by_side[side] += lot.qty
                ledger.residual_cost_by_side[side] += cost

    metrics_out = sm.finish_metrics(profile, metrics, base_day_counts)
    return {
        "processed_public_sell_rows": processed,
        "profile": metrics_out,
        "ledgers": ledgers,
    }


def aggregate_ledgers(name: str, ledgers: list[ConditionLedger], base_day_counts: dict[str, int]) -> dict[str, Any]:
    day_rows: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))  # type: ignore[arg-type]
    totals = defaultdict(float)
    for ledger in ledgers:
        day = ledger.day
        totals["active_markets"] += 1
        totals["seed_actions"] += ledger.seed_actions
        totals["gross_buy_qty"] += ledger.gross_buy_qty
        totals["gross_buy_cost"] += ledger.gross_buy_cost
        totals["official_taker_fee"] += ledger.official_taker_fee
        totals["pair_actions"] += ledger.pair_actions
        totals["pair_qty"] += ledger.pair_qty
        totals["pair_cost_sum"] += ledger.pair_cost_sum
        totals["pair_pnl"] += ledger.pair_pnl
        totals["residual_qty"] += ledger.residual_qty
        totals["residual_cost"] += ledger.residual_cost
        totals["residual_settle_pnl"] += ledger.residual_settle_pnl
        dm = day_rows[day]
        dm["active_markets"] += 1
        dm["seed_actions"] += ledger.seed_actions
        dm["gross_buy_qty"] += ledger.gross_buy_qty
        dm["gross_buy_cost"] += ledger.gross_buy_cost
        dm["official_taker_fee"] += ledger.official_taker_fee
        dm["pair_actions"] += ledger.pair_actions
        dm["pair_qty"] += ledger.pair_qty
        dm["pair_cost_sum"] += ledger.pair_cost_sum
        dm["pair_pnl"] += ledger.pair_pnl
        dm["residual_qty"] += ledger.residual_qty
        dm["residual_cost"] += ledger.residual_cost
        dm["residual_settle_pnl"] += ledger.residual_settle_pnl

    pair_qty = totals["pair_qty"]
    buy_qty = totals["gross_buy_qty"]
    buy_cost = totals["gross_buy_cost"]
    pair_pnl = totals["pair_pnl"]
    residual_settle_pnl = totals["residual_settle_pnl"]
    actual_settle_pnl = pair_pnl + residual_settle_pnl
    fee_after = actual_settle_pnl - totals["official_taker_fee"]
    worst = pair_pnl - totals["residual_cost"]
    stress = worst - 0.01 * (2.0 * pair_qty + totals["residual_qty"])
    summary_by_day = []
    for day in sorted(base_day_counts):
        dm = day_rows[day]
        day_actual = dm["pair_pnl"] + dm["residual_settle_pnl"]
        day_fee_after = day_actual - dm["official_taker_fee"]
        day_worst = dm["pair_pnl"] - dm["residual_cost"]
        day_stress = day_worst - 0.01 * (2.0 * dm["pair_qty"] + dm["residual_qty"])
        summary_by_day.append(
            {
                "day": day,
                "active_markets": int(dm["active_markets"]),
                "seed_actions": int(dm["seed_actions"]),
                "pair_actions": int(dm["pair_actions"]),
                "pair_qty": round6(dm["pair_qty"]),
                "pair_cost_wavg": round6(dm["pair_cost_sum"] / dm["pair_qty"]) if dm["pair_qty"] else 0.0,
                "pair_pnl": round6(dm["pair_pnl"]),
                "official_taker_fee": round6(dm["official_taker_fee"]),
                "fee_after_pnl": round6(day_fee_after),
                "stress100_worst_pnl": round6(day_stress),
                "residual_qty": round6(dm["residual_qty"]),
                "residual_cost": round6(dm["residual_cost"]),
            }
        )
    return {
        "name": name,
        "active_markets": int(totals["active_markets"]),
        "seed_actions": int(totals["seed_actions"]),
        "pair_actions": int(totals["pair_actions"]),
        "pair_qty": round6(pair_qty),
        "weighted_pair_cost": round6(totals["pair_cost_sum"] / pair_qty) if pair_qty else 0.0,
        "gross_buy_qty": round6(buy_qty),
        "gross_buy_cost": round6(buy_cost),
        "pair_pnl": round6(pair_pnl),
        "gross_pnl": round6(actual_settle_pnl),
        "actual_settle_pnl": round6(actual_settle_pnl),
        "official_taker_fee": round6(totals["official_taker_fee"]),
        "fee_after_pnl": round6(fee_after),
        "residual_settle_pnl": round6(residual_settle_pnl),
        "residual_qty": round6(totals["residual_qty"]),
        "residual_cost": round6(totals["residual_cost"]),
        "residual_qty_rate": round6(totals["residual_qty"] / buy_qty) if buy_qty else 0.0,
        "residual_cost_rate": round6(totals["residual_cost"] / buy_cost) if buy_cost else 0.0,
        "worst_residual_net_pnl": round6(worst),
        "stress100_worst_pnl": round6(stress),
        "worst_day_fee_after_pnl": round6(min((row["fee_after_pnl"] for row in summary_by_day), default=0.0)),
        "summary_by_day": summary_by_day,
    }


def ledger_rule_sets(ledgers: list[ConditionLedger]) -> dict[str, list[ConditionLedger]]:
    return {
        "portfolio_pair_cost_lt_1": [
            ledger for ledger in ledgers if ledger.pair_qty > DUST and ledger.avg_pair_cost() < 1.0 - 1e-12
        ],
        "portfolio_risk_adjusted_nonnegative": [
            ledger for ledger in ledgers if ledger.pair_qty > DUST and ledger.conservative_risk_adjusted() >= 0.0
        ],
        "portfolio_pair_cost_lt_1_and_risk_adjusted_nonnegative": [
            ledger
            for ledger in ledgers
            if ledger.pair_qty > DUST and ledger.avg_pair_cost() < 1.0 - 1e-12 and ledger.conservative_risk_adjusted() >= 0.0
        ],
    }


def delta(control: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "active_markets",
        "seed_actions",
        "pair_actions",
        "pair_qty",
        "weighted_pair_cost",
        "pair_pnl",
        "fee_after_pnl",
        "stress100_worst_pnl",
        "worst_day_fee_after_pnl",
        "residual_qty_rate",
        "residual_cost_rate",
        "worst_residual_net_pnl",
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
    return out


def classify(control: dict[str, Any], variants: dict[str, dict[str, Any]]) -> tuple[str, str, str, dict[str, Any]]:
    ranked = []
    for name, variant in variants.items():
        d = delta(control, variant)
        score_delta = float(variant.get("stress100_worst_pnl") or 0.0) - float(control.get("stress100_worst_pnl") or 0.0)
        ranked.append((score_delta, name, variant, d))
    ranked.sort(reverse=True, key=lambda item: item[0])
    best_score_delta, best_name, best_variant, best_delta = ranked[0]
    economics_positive = (
        float(best_variant.get("fee_after_pnl") or 0.0) > 0.0
        and float(best_variant.get("stress100_worst_pnl") or 0.0) > 0.0
        and float(best_variant.get("worst_day_fee_after_pnl") or 0.0) > 0.0
    )
    sample_retention = best_delta.get("seed_action_retention") or 0.0
    pair_retention = best_delta.get("pair_qty_retention") or 0.0
    residual_improves = (best_delta.get("residual_qty_rate_reduction") or 0.0) > 0.0
    stress_improves = best_score_delta > 0.0
    if economics_positive and sample_retention >= 0.65 and pair_retention >= 0.65 and residual_improves and stress_improves:
        return "KEEP", "KEEP_PUBLIC_PROFILE_PAIRED_INVENTORY_LOCAL_RANKER", best_name, best_delta
    if not economics_positive or sample_retention < 0.35 or pair_retention < 0.35:
        return "DISCARD", "DISCARD_PUBLIC_PROFILE_PAIRED_INVENTORY_SAMPLE_OR_STRESS_COLLAPSE", best_name, best_delta
    return "UNKNOWN", "UNKNOWN_PUBLIC_PROFILE_PAIRED_INVENTORY_TRADEOFF", best_name, best_delta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument("--baseline-result-dir", type=Path, default=DEFAULT_BASELINE_RESULT_DIR)
    parser.add_argument("--public-profile-review", type=Path, default=DEFAULT_PUBLIC_PROFILE_REVIEW)
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
    required = [candidate_manifest, candidate_db, result_manifest, compliance_manifest, args.public_profile_review]
    missing = [str(path) for path in required if not path.exists()]
    unsafe = [str(path) for path in required if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "created_utc": label,
            "lane": "causal_verifier",
            "decision_label": "BLOCKED",
            "status": "BLOCKED_PUBLIC_PROFILE_PAIRING_INPUT_UNAVAILABLE",
            "missing_paths": missing,
            "unsafe_paths": unsafe,
        }
        write_json(output_dir / "manifest.json", manifest)
        print(json.dumps({"status": manifest["status"], "manifest": str(output_dir / "manifest.json")}, indent=2))
        return 2

    candidate_manifest_json = read_json(candidate_manifest)
    result_manifest_json = read_json(result_manifest)
    compliance_manifest_json = read_json(compliance_manifest)
    public_review = read_json(args.public_profile_review)
    con = duckdb.connect(str(candidate_db), read_only=True)
    try:
        base_day_counts = {
            str(row[0]): int(row[1])
            for row in con.execute("select day, count(*) from candidate_base group by 1 order by 1").fetchall()
        }
    finally:
        con.close()

    replay = run_late_repair90(candidate_db, base_day_counts)
    ledgers = [ledger for ledger in replay["ledgers"].values() if ledger.seed_actions > 0]
    control = aggregate_ledgers("late_repair90_control_condition_ledger", ledgers, base_day_counts)
    rule_ledgers = ledger_rule_sets(ledgers)
    variants = {name: aggregate_ledgers(name, rows, base_day_counts) for name, rows in rule_ledgers.items()}
    variant_deltas = {name: delta(control, variant) for name, variant in variants.items()}
    decision, status, best_rule, best_delta = classify(control, variants)
    top_ledgers = sorted(
        ledgers,
        key=lambda item: abs(item.conservative_risk_adjusted()),
        reverse=True,
    )[:25]
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
            "public_profile_proxy_used": True,
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
            "public_profile_review": str(args.public_profile_review),
        },
        "coverage": {
            "candidate_base_labels": candidate_manifest_json.get("labels", []),
            "candidate_base_days": candidate_manifest_json.get("days", []),
            "candidate_base_excluded_labels_or_days": candidate_manifest_json.get("excluded_labels_or_days", []),
            "candidate_base_row_count": candidate_manifest_json.get("row_count"),
            "result_labels": result_manifest_json.get("labels", []),
            "public_account_audit": compliance_manifest_json.get("public_account_audit", {}),
        },
        "hypothesis": public_review.get("research_ranking", {}).get("hypothesis", {}),
        "config": {
            "control_profile": {
                "name": "shadow_aligned_late_repair90_control",
                "edge": 0.07,
                "target_qty": 5.0,
                "max_open_cost": 80.0,
                "imbalance_qty_cap": 1.25,
                "late_repair_only_after_s": 90.0,
                "fill_to_balance": False,
                "public_source_price_caps": False,
                "static_target_or_cap_sweep": False,
                "cooldown_or_admission_cap_added": False,
            },
            "portfolio_rules": {
                "portfolio_pair_cost_lt_1": "condition avg_pair_cost < 1.0 and pair_qty > 0",
                "portfolio_risk_adjusted_nonnegative": "condition conservative pair_pnl - fee - residual_cost - stress >= 0",
                "portfolio_pair_cost_lt_1_and_risk_adjusted_nonnegative": "intersection of both rules",
            },
            "note": "Rules are local research ranking over condition ledgers, not runner/trading behavior.",
        },
        "processed_public_sell_rows": replay["processed_public_sell_rows"],
        "control_profile_metrics": replay["profile"],
        "condition_ledger_control": control,
        "portfolio_rule_variants": variants,
        "portfolio_rule_deltas_vs_control": variant_deltas,
        "best_rule": best_rule,
        "best_rule_delta_vs_control": best_delta,
        "top_abs_risk_ledgers": [
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
                "residual_settle_pnl": round6(ledger.residual_settle_pnl),
                "fee_after_pnl": round6(ledger.fee_after_pnl()),
                "conservative_risk_adjusted": round6(ledger.conservative_risk_adjusted()),
                "residual_by_side": {key: round6(value) for key, value in ledger.residual_by_side.items()},
            }
            for ledger in top_ledgers
        ],
        "research_ranking": {
            "label": status,
            "decision": decision,
            "best_rule": best_rule,
            "best_rule_delta_vs_control": best_delta,
            "notes": [
                "This is a local condition-ledger ranking verifier, not a trading-path implementation.",
                "It does not use fill-to-balance90, public/source price caps, static target/cap sweeps, or cooldown/admission caps.",
                "The public profile supports the account-level paired inventory hypothesis only as proxy research input.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_CANDIDATE_PIPELINE_RANKING_ONLY_NOT_PROMOTION_EVIDENCE",
            "notes": [
                "No EC2 shadow/canary or source-of-truth replay was run.",
                "Any KEEP result requires a separate default-off implementability review and later no-order/source-of-truth validation.",
            ],
        },
        "next_executable_action": (
            "Run implementability_review only: add no trading-path change yet; draft a default-off portfolio-ledger diagnostic/"
            "summary spec for the shadow runner and local-smoke it before any EC2 shadow."
            if decision == "KEEP"
            else "Do not promote this profile-derived mechanism; choose a different local non-price verifier."
        ),
        "deployable": False,
        "can_support_strategy_promotion": False,
        "g2_canary_ready": False,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "status": status,
                "decision_label": decision,
                "manifest": str(output_dir / "manifest.json"),
                "best_rule": best_rule,
                "control_seed_actions": control["seed_actions"],
                "best_seed_actions": variants[best_rule]["seed_actions"],
                "control_stress100": control["stress100_worst_pnl"],
                "best_stress100": variants[best_rule]["stress100_worst_pnl"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
