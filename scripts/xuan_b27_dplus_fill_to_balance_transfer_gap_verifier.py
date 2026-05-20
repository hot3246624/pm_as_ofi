#!/usr/bin/env python3
"""Compare local fill-to-balance activation with pulled no-order shadow summaries.

This is a local-only causal verifier. It reads the derived candidate_base
DuckDB, local candidate-pipeline manifests, and pulled shadow summary/aggregate
JSON. It does not read raw/replay/collector stores, full completion stores,
event JSONL, sockets, or remote state.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_CANDIDATE_BASE_DIR = Path(
    "/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/"
    "local_20260502_20260518_paircap102"
)
DEFAULT_SHADOW_ARTIFACT = Path(
    "xuan_research_artifacts/xuan_b27_dplus_shadow_fill_to_balance_sample_scale_driver_20260520T145943Z"
)
DEFAULT_LOCAL_LEAD = Path(
    "xuan_research_artifacts/xuan_b27_dplus_candidate_pipeline_shadow_aligned_fill_rerun_20260520T143629Z/manifest.json"
)
ARTIFACT_PREFIX = "xuan_b27_dplus_fill_to_balance_transfer_gap_verifier"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
)
DUST = 1e-9


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


def deficit_bucket(deficit: float) -> str:
    if deficit <= 1.0 + DUST:
        return "deficit_le_1"
    if deficit <= 2.0 + DUST:
        return "deficit_1_2"
    if deficit < 5.0 - DUST:
        return "deficit_2_5"
    if deficit <= 5.0 + DUST:
        return "deficit_eq_5"
    return "deficit_gt_5"


def base_qty_bucket(qty: float) -> str:
    if qty <= 1.0 + DUST:
        return "base_qty_le_1"
    if qty < 5.0 - DUST:
        return "base_qty_1_5"
    if qty <= 5.0 + DUST:
        return "base_qty_eq_5"
    return "base_qty_gt_5"


def add_count(bucket: dict[str, float], key: str, value: float = 1.0) -> None:
    bucket[key] = round(float(bucket.get(key, 0.0)) + float(value), 6)


def add_nested(bucket: dict[str, dict[str, float]], key: str, subkey: str, value: float = 1.0) -> None:
    child = bucket.setdefault(key, {})
    add_count(child, subkey, value)


def summarize_shadow(shadow_artifact: Path) -> dict[str, Any]:
    output = shadow_artifact / "remote" / "output"
    aggregate = read_json(output / "aggregate_report.json")
    audit = read_json(shadow_artifact / "audit_manifest.json")
    metrics = aggregate.get("metrics", {})
    event_lite = aggregate.get("event_lite", {})
    summaries: list[dict[str, Any]] = []
    for path in sorted(output.glob("*.summary.json")):
        item = read_json(path)
        m = item.get("metrics", {})
        summaries.append(
            {
                "slug": item.get("slug") or path.name.replace(".summary.json", ""),
                "candidates": m.get("candidates", 0),
                "queue_supported_fills": m.get("queue_supported_fills", 0),
                "pair_actions": m.get("pair_actions", 0),
                "pair_pnl": m.get("pair_pnl", 0),
                "residual_qty": m.get("residual_qty", 0),
                "residual_cost": m.get("residual_cost", 0),
                "late_repair_fill_to_balance_candidates": m.get("late_repair_fill_to_balance_candidates", 0),
                "late_repair_fill_to_balance_caps": m.get("late_repair_fill_to_balance_caps", 0),
                "late_repair_fill_to_balance_blocks": m.get("late_repair_fill_to_balance_blocks", 0),
                "late_repair_fill_to_balance_qty_reduction": m.get(
                    "late_repair_fill_to_balance_qty_reduction", 0
                ),
            }
        )
    f2b_rows = [row for row in summaries if row["late_repair_fill_to_balance_candidates"]]
    residual_without_f2b = [
        row
        for row in summaries
        if row["residual_qty"] and not row["late_repair_fill_to_balance_candidates"]
    ]
    return {
        "aggregate_metrics": {
            "candidates": metrics.get("candidates", 0),
            "queue_supported_fills": metrics.get("queue_supported_fills", 0),
            "pair_actions": metrics.get("pair_actions", 0),
            "pair_qty": metrics.get("pair_qty", 0),
            "pair_pnl": metrics.get("pair_pnl", 0),
            "net_pair_cost_p90": metrics.get("net_pair_cost_proxy_p90", 0),
            "residual_qty": metrics.get("residual_qty", 0),
            "residual_cost": metrics.get("residual_cost", 0),
            "late_repair_fill_to_balance_candidates": metrics.get(
                "late_repair_fill_to_balance_candidates", 0
            ),
            "late_repair_fill_to_balance_caps": metrics.get("late_repair_fill_to_balance_caps", 0),
            "late_repair_fill_to_balance_blocks": metrics.get("late_repair_fill_to_balance_blocks", 0),
            "late_repair_fill_to_balance_qty_reduction": metrics.get(
                "late_repair_fill_to_balance_qty_reduction", 0
            ),
        },
        "risk_adjusted_pnl_proxy": audit.get("metrics", {}).get("risk_adjusted_pnl_proxy"),
        "f2b_slug_rows": f2b_rows,
        "residual_without_f2b_top": sorted(
            residual_without_f2b, key=lambda row: (row["residual_cost"], row["residual_qty"]), reverse=True
        )[:12],
        "event_lite": {
            "f2b_block_by_side": (event_lite.get("block_by_reason_side") or {}).get(
                "late_repair_fill_to_balance_qty", {}
            ),
            "f2b_block_by_offset": (event_lite.get("block_by_reason_offset_bucket") or {}).get(
                "late_repair_fill_to_balance_qty", {}
            ),
            "f2b_block_by_public_px": (event_lite.get("block_by_reason_public_px_bucket") or {}).get(
                "late_repair_fill_to_balance_qty", {}
            ),
            "residual_lot_side_qty": event_lite.get("residual_lot_side_qty", {}),
            "residual_lot_px_qty_buckets": event_lite.get("residual_lot_px_qty_buckets", {}),
        },
        "summary_file_count": len(summaries),
    }


def trace_candidate_pipeline(candidate_base_db: Path) -> dict[str, Any]:
    sm = load_state_machine_module()
    profile = sm.Profile(
        name="shadow_aligned_late_repair90_fill_to_balance_after_90_trace",
        seed_offset_max_s=120.0,
        late_repair_only_after_s=90.0,
        late_repair_fill_to_balance_after_s=90.0,
        edge=0.07,
        target_qty=5.0,
        max_open_cost=80.0,
        imbalance_qty_cap=1.25,
    )
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
    metrics: defaultdict[str, float] = defaultdict(float)
    states: dict[str, Any] = {}
    trace: dict[str, Any] = {
        "f2b_by_offset": {},
        "f2b_by_side": {},
        "f2b_by_deficit": {},
        "f2b_caps_by_deficit": {},
        "f2b_blocks_by_deficit": {},
        "f2b_qty_reduction_by_deficit": {},
        "f2b_by_base_qty": {},
        "f2b_caps_by_offset_side": {},
        "f2b_blocks_by_offset_side": {},
        "late_underweight_pre_target_by_offset": {},
        "late_underweight_blocked_before_f2b": {},
    }

    def trace_maybe_seed(row: dict[str, Any]) -> None:
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
        state = sm.state_for(states, row)
        if ts_ms - state.last_seed_ts_ms < profile.cooldown_s * 1000.0:
            sm.add_count(metrics, day, "seed_block_cooldown")
            return

        same_qty = sm.lot_qty(state.lots[side])
        opp_qty = sm.lot_qty(state.lots[sm.other(side)])
        offset_key = offset_bucket(offset_s)
        if offset_s >= 90.0 and same_qty < opp_qty:
            add_count(trace["late_underweight_pre_target_by_offset"], offset_key)
        aged_cost = sm.aged_lot_cost(state.lots["YES"], ts_ms, profile.residual_cooldown_age_s) + sm.aged_lot_cost(
            state.lots["NO"], ts_ms, profile.residual_cooldown_age_s
        )
        if aged_cost > profile.residual_cooldown_cost_cap + 1e-12 and same_qty + profile.dust_qty >= opp_qty:
            sm.add_count(metrics, day, "seed_block_residual_cooldown")
            return
        if offset_s >= float(profile.late_repair_only_after_s or 1e18) and same_qty >= opp_qty:
            sm.add_count(metrics, day, "seed_block_late_repair_only")
            return
        if same_qty >= profile.target_qty - profile.dust_qty:
            if offset_s >= 90.0 and same_qty < opp_qty:
                add_count(trace["late_underweight_blocked_before_f2b"], "target")
            sm.add_count(metrics, day, "seed_block_target")
            return
        same_cost = sm.lot_cost(state.lots[side])
        opp_cost = sm.lot_cost(state.lots[sm.other(side)])
        if max(0.0, same_cost - opp_cost) > profile.imbalance_cost_cap + 1e-12:
            sm.add_count(metrics, day, "seed_block_imbalance_cost")
            return

        seed_px = max(0.01, trade_px - profile.edge)
        open_cost = sm.lot_cost(state.lots["YES"]) + sm.lot_cost(state.lots["NO"])
        imbalance_room = profile.imbalance_qty_cap - max(0.0, same_qty - opp_qty)
        if imbalance_room <= profile.dust_qty:
            sm.add_count(metrics, day, "seed_block_imbalance_qty")
            return
        base_qty = min(
            profile.max_seed_qty,
            trade_size * profile.fill_haircut,
            profile.target_qty - same_qty,
            (profile.max_open_cost - open_cost) / max(seed_px, 1e-9),
            imbalance_room,
        )
        late_fill_to_balance_active = offset_s >= float(profile.late_repair_fill_to_balance_after_s or 1e18) and same_qty < opp_qty
        qty = base_qty
        if late_fill_to_balance_active:
            deficit_qty = max(0.0, opp_qty - same_qty)
            capped_qty = min(qty, deficit_qty)
            deficit_key = deficit_bucket(deficit_qty)
            base_key = base_qty_bucket(base_qty)
            side_key = side
            add_count(trace["f2b_by_offset"], offset_key)
            add_count(trace["f2b_by_side"], side_key)
            add_count(trace["f2b_by_deficit"], deficit_key)
            add_count(trace["f2b_by_base_qty"], base_key)
            sm.add_count(metrics, day, "seed_late_fill_to_balance_candidate")
            if capped_qty < qty - DUST:
                reduction = qty - capped_qty
                add_count(trace["f2b_caps_by_deficit"], deficit_key)
                add_nested(trace["f2b_caps_by_offset_side"], offset_key, side_key)
                add_count(trace["f2b_qty_reduction_by_deficit"], deficit_key, reduction)
                sm.add_count(metrics, day, "seed_qty_cap_late_fill_to_balance")
                sm.add_metric(metrics, day, "seed_late_fill_to_balance_qty_reduction", reduction)
            qty = capped_qty
        if qty <= profile.dust_qty:
            if late_fill_to_balance_active:
                add_count(trace["f2b_blocks_by_deficit"], deficit_bucket(max(0.0, opp_qty - same_qty)))
                add_nested(trace["f2b_blocks_by_offset_side"], offset_key, side)
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
            trace_maybe_seed(dict(zip(select_cols, raw)))
    sm.settle(states, profile, metrics)
    con.close()
    base_day_counts = {row[0]: int(row[1]) for row in duckdb.connect(str(candidate_base_db), read_only=True).execute(
        "select day, count(*) from candidate_base group by 1 order by 1"
    ).fetchall()}
    profile_metrics = sm.finish_metrics(profile, metrics, base_day_counts)
    return {
        "processed_public_sell_rows": row_count,
        "profile_metrics": profile_metrics,
        "trace": trace,
    }


def ratio(num: float, den: float) -> float | None:
    return round(float(num) / float(den), 6) if den else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidate-base-dir", type=Path, default=DEFAULT_CANDIDATE_BASE_DIR)
    ap.add_argument("--shadow-artifact", type=Path, default=DEFAULT_SHADOW_ARTIFACT)
    ap.add_argument("--local-lead-manifest", type=Path, default=DEFAULT_LOCAL_LEAD)
    ap.add_argument("--output-dir", type=Path, default=None)
    args = ap.parse_args()

    candidate_db = args.candidate_base_dir / "candidate_base.duckdb"
    candidate_manifest = args.candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    required = [
        candidate_db,
        candidate_manifest,
        args.shadow_artifact / "remote" / "output" / "aggregate_report.json",
        args.shadow_artifact / "audit_manifest.json",
        args.local_lead_manifest,
    ]
    for path in required:
        if not path.exists():
            raise SystemExit(f"missing required local input: {path}")
        if not path_safe(path):
            raise SystemExit(f"refusing forbidden input path: {path}")

    out = args.output_dir or Path("xuan_research_artifacts") / f"{ARTIFACT_PREFIX}_{utc_label()}"
    if not path_safe(out):
        raise SystemExit(f"refusing forbidden output path: {out}")

    candidate_trace = trace_candidate_pipeline(candidate_db)
    shadow = summarize_shadow(args.shadow_artifact)
    local_lead = read_json(args.local_lead_manifest)
    local_metrics = candidate_trace["profile_metrics"]
    shadow_metrics = shadow["aggregate_metrics"]

    local_f2b = float(local_metrics["seed_late_fill_to_balance_candidate"])
    shadow_f2b = float(shadow_metrics["late_repair_fill_to_balance_candidates"])
    comparison = {
        "local_f2b_candidates": local_f2b,
        "shadow_f2b_candidates": shadow_f2b,
        "local_f2b_candidate_share_of_seed_actions": ratio(local_f2b, local_metrics["seed_actions"]),
        "shadow_f2b_candidate_share_of_candidates": ratio(shadow_f2b, shadow_metrics["candidates"]),
        "local_f2b_caps": local_metrics["seed_qty_cap_late_fill_to_balance"],
        "shadow_f2b_caps": shadow_metrics["late_repair_fill_to_balance_caps"],
        "local_f2b_cap_share": ratio(local_metrics["seed_qty_cap_late_fill_to_balance"], local_f2b),
        "shadow_f2b_cap_share": ratio(shadow_metrics["late_repair_fill_to_balance_caps"], shadow_f2b),
        "local_f2b_blocks": local_metrics["seed_block_late_fill_to_balance_qty"],
        "shadow_f2b_blocks": shadow_metrics["late_repair_fill_to_balance_blocks"],
        "local_qty_reduction": local_metrics["seed_late_fill_to_balance_qty_reduction"],
        "shadow_qty_reduction": shadow_metrics["late_repair_fill_to_balance_qty_reduction"],
        "local_residual_qty": local_metrics["residual_qty"],
        "shadow_residual_qty": shadow_metrics["residual_qty"],
        "shadow_risk_adjusted_pnl_proxy": shadow.get("risk_adjusted_pnl_proxy"),
    }
    decision = "UNKNOWN_FILL_TO_BALANCE_TRANSFER_GAP_NEEDS_SHADOW_DEFICIT_BUCKETS"
    status = decision
    if shadow_metrics["late_repair_fill_to_balance_caps"] == 0 and shadow_metrics["residual_qty"] > 10:
        status = "UNKNOWN_FILL_TO_BALANCE_TRANSFER_GAP_SHADOW_CAPS_ABSENT"

    manifest = {
        "schema_version": 1,
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lane": "causal_verifier",
        "status": status,
        "decision": "UNKNOWN",
        "scope": {
            "local_only": True,
            "candidate_base_duckdb_read_only": True,
            "pulled_shadow_summary_json_only": True,
            "events_jsonl_read": False,
            "raw_replay_collector_scanned": False,
            "full_completion_store_scanned": False,
            "ssh_used": False,
            "shadow_started": False,
            "orders_cancels_redeems_sent": False,
            "runner_modified": False,
        },
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest),
            "candidate_base_duckdb": str(candidate_db),
            "shadow_artifact": str(args.shadow_artifact),
            "local_lead_manifest": str(args.local_lead_manifest),
        },
        "candidate_pipeline_trace": {
            "processed_public_sell_rows": candidate_trace["processed_public_sell_rows"],
            "metrics": {
                "seed_actions": local_metrics["seed_actions"],
                "pair_actions": local_metrics["pair_actions"],
                "pair_qty": local_metrics["pair_qty"],
                "fee_after_pnl": local_metrics["fee_after_pnl"],
                "stress100_worst_pnl": local_metrics["stress100_worst_pnl"],
                "residual_qty": local_metrics["residual_qty"],
                "residual_cost": local_metrics["residual_cost"],
                "residual_qty_rate": local_metrics["residual_qty_rate"],
                "residual_cost_rate": local_metrics["residual_cost_rate"],
                "seed_late_fill_to_balance_candidate": local_metrics["seed_late_fill_to_balance_candidate"],
                "seed_qty_cap_late_fill_to_balance": local_metrics["seed_qty_cap_late_fill_to_balance"],
                "seed_block_late_fill_to_balance_qty": local_metrics["seed_block_late_fill_to_balance_qty"],
                "seed_late_fill_to_balance_qty_reduction": local_metrics[
                    "seed_late_fill_to_balance_qty_reduction"
                ],
            },
            "trace": candidate_trace["trace"],
        },
        "shadow_summary_trace": shadow,
        "local_lead_scoreboard_delta": local_lead.get("scoreboard_delta"),
        "comparison": comparison,
        "research_ranking": {
            "label": "UNKNOWN_TRANSFER_GAP_SOURCE_FIELDS_INSUFFICIENT",
            "economic_pnl_positive_in_shadow": shadow_metrics["pair_pnl"] > 0,
            "risk_adjusted_proxy_positive_in_shadow": (shadow.get("risk_adjusted_pnl_proxy") or 0) > 0,
            "notes": [
                "Candidate-pipeline fill-to-balance caps are common in the historical state machine.",
                "The same no-order shadow surface reached sample size but produced zero fill-to-balance caps and zero qty reduction.",
                "Pulled summaries do not expose per-candidate deficit/base_qty/capped_qty for the shadow, so the exact absence of cap opportunities cannot be proven without new summary buckets or events JSONL.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "deployable": False,
            "can_support_strategy_promotion": False,
            "required_before_g2_canary": True,
            "reason": "no-order shadow risk budget failed and transfer gap is not resolved",
        },
        "missing_shadow_fields": [
            "late_repair_fill_to_balance_deficit bucket by side/offset/slug",
            "base_seed_qty and capped_seed_qty bucket by side/offset/slug",
            "late_repair_fill_to_balance_qty_reduction bucket by side/offset/slug",
            "residual lot source link to fill_to_balance_active/capped seed",
            "pair action source link to fill_to_balance_active/capped seed",
        ],
        "interpretation": [
            "The local state-machine KEEP relies on many late underweight repair seeds being capped; the shadow window had almost no cap opportunities under the current summary evidence.",
            "This is a transfer-gap result, not a bare PnL failure: pair PnL is positive, but residual budget failed.",
            "Do not run another EC2 shadow until the shadow runner can summarize the missing deficit/cap buckets or a new local mechanism returns KEEP.",
        ],
        "next_executable_action": (
            "Implement default-off event-lite fill-to-balance diagnostic buckets in the shadow runner "
            "and local smoke them: deficit/base_qty/capped_qty/qty_reduction by side, offset, and slug; "
            "do not start EC2 shadow/canary in that heartbeat."
        ),
    }
    write_json(out / "manifest.json", manifest)
    print(out / "manifest.json")


if __name__ == "__main__":
    main()
