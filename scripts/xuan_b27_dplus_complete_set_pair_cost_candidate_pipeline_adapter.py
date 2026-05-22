#!/usr/bin/env python3
"""Adapt complete-set pair-cost context to the local D+ candidate pipeline.

This local-only adapter reruns the allowed candidate_base state machine with
the default-off selected_seed_ledger_context export enabled in memory. It then
enriches seed rows with candidate_base L1 pair context and quantifies whether
complete-set/pair-cost structure exists in our 5m D+ universe.

It is an adapter/scorer, not a trading mechanism. It does not start SSH/shadow,
read events JSONL, query replay/raw stores, or make promotion claims.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb


SCRIPT_DIR = Path(__file__).resolve().parent
STATE_MACHINE_PATH = SCRIPT_DIR / "xuan_b27_dplus_candidate_pipeline_state_machine_rerun.py"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)

REQUIRED_SEED_CONTEXT_FIELDS = (
    "source_seed_candidate_row_id",
    "day",
    "condition_id",
    "slug",
    "ts_ms",
    "side",
    "opposite_side",
    "offset_s",
    "trigger_px",
    "trigger_size",
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
)

REQUIRED_CANDIDATE_FIELDS = (
    "candidate_row_id",
    "l1_pair_ask",
    "public_trade_price",
    "public_trade_size",
)


def load_state_machine() -> Any:
    spec = importlib.util.spec_from_file_location("xuan_dplus_state_machine", STATE_MACHINE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {STATE_MACHINE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_ratio(num: float, den: float) -> float:
    return num / den if den else 0.0


def fee_per_share(price: float, fee_rate: float) -> float:
    return fee_rate * price * (1.0 - price)


def weighted_avg(num: float, den: float) -> float | None:
    return num / den if den else None


def bucket_l1_edge(edge: float | None) -> str:
    if edge is None or not math.isfinite(edge):
        return "l1_edge_missing"
    if edge < -0.10:
        return "l1_edge_lt_-0.10"
    if edge < -0.05:
        return "l1_edge_-0.10_-0.05"
    if edge < 0.0:
        return "l1_edge_-0.05_0"
    if edge < 0.02:
        return "l1_edge_0_0.02"
    if edge < 0.05:
        return "l1_edge_0.02_0.05"
    return "l1_edge_ge_0.05"


def bucket_realized_pair_cost(cost: float | None) -> str:
    if cost is None or not math.isfinite(cost):
        return "unpaired_only"
    if cost < 0.80:
        return "realized_pair_cost_lt_0.80"
    if cost < 0.90:
        return "realized_pair_cost_0.80_0.90"
    if cost < 0.95:
        return "realized_pair_cost_0.90_0.95"
    if cost < 1.00:
        return "realized_pair_cost_0.95_1.00"
    return "realized_pair_cost_ge_1.00"


def load_l1_context(candidate_db: Path) -> dict[str, dict[str, Any]]:
    con = duckdb.connect(str(candidate_db), read_only=True)
    try:
        rows = con.execute(
            """
            select candidate_row_id, l1_pair_ask, public_trade_price, public_trade_size
            from candidate_base
            where candidate_reason = 'public_sell'
            """
        ).fetchall()
    finally:
        con.close()
    return {
        str(candidate_row_id): {
            "l1_pair_ask": as_float(l1_pair_ask, math.nan),
            "public_trade_price": as_float(public_trade_price, math.nan),
            "public_trade_size": as_float(public_trade_size, math.nan),
        }
        for candidate_row_id, l1_pair_ask, public_trade_price, public_trade_size in rows
    }


def field_coverage(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> dict[str, Any]:
    if not rows:
        return {"overall": 0.0, "by_field": {field: 0.0 for field in fields}, "missing_fields": list(fields)}
    by_field = {
        field: safe_ratio(sum(1 for row in rows if row.get(field) not in (None, "")), len(rows))
        for field in fields
    }
    present = sum(1 for row in rows for field in fields if row.get(field) not in (None, ""))
    return {
        "overall": safe_ratio(present, len(rows) * len(fields)),
        "by_field": {field: round(value, 8) for field, value in by_field.items()},
        "missing_fields": [field for field, value in by_field.items() if value < 1.0],
    }


def group_metrics(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
    totals: defaultdict[str, float] = defaultdict(float)
    for row in rows:
        group = str(row.get(key) or "missing")
        for field in (
            "seed_qty",
            "seed_cost",
            "source_pair_qty",
            "source_pair_cost",
            "source_pair_pnl",
            "source_residual_qty",
            "source_residual_cost",
        ):
            value = as_float(row.get(field))
            grouped[group][field] += value
            totals[field] += value
        grouped[group]["row_count"] += 1
        totals["row_count"] += 1
        grouped[group]["l1_edge_qty_weighted_sum"] += as_float(row.get("l1_complete_set_edge"), 0.0) * as_float(
            row.get("seed_qty")
        )
        grouped[group]["l1_edge_weight_qty"] += as_float(row.get("seed_qty"))
    output = []
    for group, values in grouped.items():
        pair_qty = values["source_pair_qty"]
        seed_qty = values["seed_qty"]
        residual_cost = values["source_residual_cost"]
        output.append(
            {
                "group": group,
                "row_count": int(values["row_count"]),
                "seed_qty": round(seed_qty, 8),
                "seed_qty_share": round(safe_ratio(seed_qty, totals["seed_qty"]), 8),
                "source_pair_qty": round(pair_qty, 8),
                "source_pair_qty_share": round(safe_ratio(pair_qty, totals["source_pair_qty"]), 8),
                "source_pair_cost_wavg": round(values["source_pair_cost"] / pair_qty, 8) if pair_qty else None,
                "source_pair_pnl": round(values["source_pair_pnl"], 8),
                "source_residual_qty": round(values["source_residual_qty"], 8),
                "source_residual_cost": round(residual_cost, 8),
                "source_residual_cost_share": round(safe_ratio(residual_cost, totals["source_residual_cost"]), 8),
                "l1_complete_set_edge_wavg": round(
                    values["l1_edge_qty_weighted_sum"] / values["l1_edge_weight_qty"], 8
                )
                if values["l1_edge_weight_qty"]
                else None,
            }
        )
    output.sort(key=lambda row: (as_float(row["source_residual_cost"]), as_float(row["seed_qty"])), reverse=True)
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def enrich_rows(seed_rows: list[dict[str, Any]], l1_context: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for row in seed_rows:
        out = dict(row)
        context = l1_context.get(str(row.get("source_seed_candidate_row_id") or ""))
        if context:
            out.update(context)
        seed_px = as_float(out.get("seed_px"), math.nan)
        l1_pair_ask = as_float(out.get("l1_pair_ask"), math.nan)
        fee_rate = as_float(out.get("official_fee_rate"))
        if math.isfinite(seed_px) and math.isfinite(l1_pair_ask):
            l1_pair_fee = fee_per_share(seed_px, fee_rate) + fee_per_share(l1_pair_ask, fee_rate)
            out["l1_complete_set_pair_cost_sum"] = seed_px + l1_pair_ask
            out["l1_complete_set_pair_fee_per_share"] = l1_pair_fee
            out["l1_complete_set_edge"] = 1.0 - seed_px - l1_pair_ask - l1_pair_fee
            out["l1_complete_set_edge_bucket"] = bucket_l1_edge(out["l1_complete_set_edge"])
        else:
            out["l1_complete_set_pair_cost_sum"] = None
            out["l1_complete_set_pair_fee_per_share"] = None
            out["l1_complete_set_edge"] = None
            out["l1_complete_set_edge_bucket"] = "l1_edge_missing"
        pair_qty = as_float(out.get("source_pair_qty"))
        if pair_qty > 1e-12:
            out["realized_complete_set_pair_cost_wavg"] = as_float(out.get("source_pair_cost")) / pair_qty
            out["realized_complete_set_pair_edge_wavg"] = as_float(out.get("source_pair_pnl")) / pair_qty
        else:
            out["realized_complete_set_pair_cost_wavg"] = None
            out["realized_complete_set_pair_edge_wavg"] = None
        out["realized_pair_cost_bucket"] = bucket_realized_pair_cost(out["realized_complete_set_pair_cost_wavg"])
        enriched.append(out)
    return enriched


def review(*, candidate_base_dir: Path, output_dir: Path) -> dict[str, Any]:
    sm = load_state_machine()
    candidate_manifest_path = candidate_base_dir / "CANDIDATE_BASE_MANIFEST.json"
    candidate_db_path = candidate_base_dir / "candidate_base.duckdb"
    required_paths = [candidate_manifest_path, candidate_db_path]
    missing_paths = [str(path) for path in required_paths if not path.exists()]
    unsafe_paths = [str(path) for path in required_paths if path.exists() and not path_safe(path)]
    if missing_paths:
        raise SystemExit(f"missing required files: {missing_paths}")
    if unsafe_paths:
        raise SystemExit(f"unsafe input paths rejected: {unsafe_paths}")

    candidate_manifest = sm.read_json(candidate_manifest_path)
    base_day_counts = {str(day): int(count) for day, count in (candidate_manifest.get("day_counts") or {}).items()}
    profile = sm.Profile(
        name="variant_late_repair_only_after_90",
        seed_offset_max_s=120.0,
        late_repair_only_after_s=90.0,
    )
    recorder = sm.SelectedSeedLedgerContextRecorder(
        profile_name=profile.name,
        sample_cap=1_000_000,
        per_day_cap=1_000_000,
        selection_mode="first",
    )
    run_result = sm.run_profiles(candidate_db_path, [profile], base_day_counts, seed_ledger_context_recorder=recorder)
    recorder.finalize_selection()
    seed_rows = recorder.rows or []
    l1_context = load_l1_context(candidate_db_path)
    enriched_rows = enrich_rows(seed_rows, l1_context)
    selected_coverage = field_coverage(enriched_rows, REQUIRED_SEED_CONTEXT_FIELDS)
    l1_field_coverage = field_coverage(
        enriched_rows,
        ("l1_pair_ask", "public_trade_price", "public_trade_size", "l1_complete_set_edge"),
    )
    core = run_result["profiles"][profile.name]
    total_seed_qty = sum(as_float(row.get("seed_qty")) for row in enriched_rows)
    total_pair_qty = sum(as_float(row.get("source_pair_qty")) for row in enriched_rows)
    total_pair_cost = sum(as_float(row.get("source_pair_cost")) for row in enriched_rows)
    total_pair_pnl = sum(as_float(row.get("source_pair_pnl")) for row in enriched_rows)
    total_residual_qty = sum(as_float(row.get("source_residual_qty")) for row in enriched_rows)
    total_residual_cost = sum(as_float(row.get("source_residual_cost")) for row in enriched_rows)
    l1_edge_qty = sum(
        as_float(row.get("seed_qty"))
        for row in enriched_rows
        if row.get("l1_complete_set_edge") is not None
    )
    l1_edge_weighted = sum(
        as_float(row.get("seed_qty")) * as_float(row.get("l1_complete_set_edge"))
        for row in enriched_rows
        if row.get("l1_complete_set_edge") is not None
    )
    unpaired_only_rows = [row for row in enriched_rows if as_float(row.get("source_pair_qty")) <= 1e-12]
    top_unpaired_residual_cost_share = safe_ratio(
        sum(as_float(row.get("source_residual_cost")) for row in unpaired_only_rows),
        total_residual_cost,
    )
    realized_groups = group_metrics(enriched_rows, "realized_pair_cost_bucket")
    l1_groups = group_metrics(enriched_rows, "l1_complete_set_edge_bucket")
    reason = (
        "The adapter can quantify complete-set context, but it should not become a direct mechanism yet: "
        "realized pair-cost context is positive in our 5m state-machine, while conservative L1 pair context is "
        "mostly negative and would collapse into a price/cap family if used as an admission rule."
    )
    decision = "KEEP"
    label = "KEEP_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_ADAPTER_READY"
    blockers: list[str] = []
    if selected_coverage["missing_fields"] or l1_field_coverage["missing_fields"]:
        decision = "UNKNOWN"
        label = "UNKNOWN_COMPLETE_SET_PAIR_COST_CANDIDATE_PIPELINE_FIELD_GAP"
        blockers.append("required_candidate_or_seed_context_fields_missing")
    score = {
        "artifact": "xuan_b27_dplus_complete_set_pair_cost_candidate_pipeline_adapter",
        "created_utc": sm.utc_label(),
        "decision": decision,
        "decision_label": label,
        "scope": "local_candidate_pipeline_adapter_scorer",
        "inputs": {
            "candidate_base_manifest": str(candidate_manifest_path),
            "candidate_base_duckdb": str(candidate_db_path),
        },
        "blockers": blockers,
        "dataset_scope": {
            "dataset_type": candidate_manifest.get("dataset_type"),
            "labels": candidate_manifest.get("labels"),
            "days": candidate_manifest.get("days"),
            "excluded_labels_or_days": candidate_manifest.get("excluded_labels_or_days"),
            "allowed_candidate_pipeline_v1_only": True,
        },
        "control_profile": {
            "name": profile.name,
            "seed_offset_max_s": profile.seed_offset_max_s,
            "late_repair_only_after_s": profile.late_repair_only_after_s,
            "official_fee_rate": profile.official_fee_rate,
            "fee_rate_source": (
                "state_machine_profile:variant_late_repair_only_after_90:"
                f"official_fee_rate={profile.official_fee_rate}"
            ),
        },
        "field_coverage": {
            "selected_seed_context": selected_coverage,
            "candidate_l1_context": l1_field_coverage,
        },
        "state_machine_core_metrics": {
            key: core.get(key)
            for key in (
                "seed_actions",
                "pair_actions",
                "pair_qty",
                "weighted_pair_cost",
                "gross_buy_qty",
                "gross_buy_cost",
                "fee_after_pnl",
                "stress100_worst_pnl",
                "worst_day_fee_after_pnl",
                "residual_qty",
                "residual_cost",
                "residual_qty_rate",
                "residual_cost_rate",
            )
        },
        "complete_set_pair_cost_context": {
            "seed_context_rows": len(enriched_rows),
            "seed_qty": round(total_seed_qty, 8),
            "realized_source_pair_qty": round(total_pair_qty, 8),
            "realized_source_pair_qty_share_of_seed_qty": round(safe_ratio(total_pair_qty, total_seed_qty), 8),
            "realized_source_pair_cost_wavg": round(total_pair_cost / total_pair_qty, 8) if total_pair_qty else None,
            "realized_source_pair_edge_wavg": round(total_pair_pnl / total_pair_qty, 8) if total_pair_qty else None,
            "source_residual_qty": round(total_residual_qty, 8),
            "source_residual_cost": round(total_residual_cost, 8),
            "source_residual_qty_share_of_seed_qty": round(safe_ratio(total_residual_qty, total_seed_qty), 8),
            "l1_complete_set_edge_wavg": round(weighted_avg(l1_edge_weighted, l1_edge_qty), 8)
            if weighted_avg(l1_edge_weighted, l1_edge_qty) is not None
            else None,
            "unpaired_only_seed_row_count": len(unpaired_only_rows),
            "unpaired_only_residual_cost_share": round(top_unpaired_residual_cost_share, 8),
        },
        "ranked_groups": {
            "by_realized_pair_cost_bucket": realized_groups,
            "by_l1_complete_set_edge_bucket": l1_groups,
        },
        "adapter_interpretation": {
            "direct_public_profile_transfer": False,
            "public_profile_timeframe_mismatch": "public profile proxy sample is mostly 15m; this adapter scores local 5m D+",
            "realized_pair_cost_context_is_useful_for_diagnostics": total_pair_qty > 0.0,
            "l1_context_should_not_be_used_as_static_price_cap": True,
            "new_mechanism_selected": False,
            "reason": reason,
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "interpretation": (
                "Adapter/scorer readiness only. A KEEP here means D+ is still alive as a research line and has "
                "quantified complete-set pair-cost context; it does not mean deployable strategy economics."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "LOCAL_ADAPTER_ONLY_NOT_PROMOTION_EVIDENCE",
            "deployable": False,
            "private_truth_ready": False,
            "g2_canary_ready": False,
        },
        "side_effects": {
            "candidate_base_duckdb_read_only": True,
            "ssh_started": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_executable_action": (
            "Use the adapter output to select one non-price, sample-preserving local mechanism only if it can be "
            "expressed without static l1/public price caps or summary-only deletion; otherwise return UNKNOWN with "
            "the exact missing live-action fields."
        ),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "selected_seed_complete_set_context.csv", enriched_rows)
    write_csv(output_dir / "realized_pair_cost_bucket_groups.csv", realized_groups)
    write_csv(output_dir / "l1_complete_set_edge_bucket_groups.csv", l1_groups)
    return score


def parse_args() -> argparse.Namespace:
    sm = load_state_machine()
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-base-dir", type=Path, default=sm.DEFAULT_CANDIDATE_BASE_DIR)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = review(candidate_base_dir=args.candidate_base_dir, output_dir=args.output_dir)
    score_path = args.output_dir / "complete_set_pair_cost_candidate_pipeline_adapter_score.json"
    sm = load_state_machine()
    sm.write_json(score_path, report)
    print(score_path)
    return 0 if report["decision"] == "KEEP" else 1


if __name__ == "__main__":
    raise SystemExit(main())
