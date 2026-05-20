#!/usr/bin/env python3
"""Attribute the D+ fill-to-balance pipeline/shadow mismatch.

This script reads only local candidate-pipeline manifests and pulled shadow
summary/aggregate JSON. It does not read raw/replay/collector stores, the full
completion event store, sockets, SSH, or event JSONL.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_fill_to_balance_shadow_mismatch_attribution"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
    "collector",
)

DEFAULT_PIPELINE_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_candidate_pipeline_state_machine_fill_to_balance_rerun_20260520T120527Z/"
    "manifest.json"
)
DEFAULT_FILL_SHADOW = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_passive_passive_shadow_runner_fill_to_balance_driver_20260520T130047Z"
)
DEFAULT_LATE_REPAIR_SHADOW = Path(
    "xuan_research_artifacts/"
    "xuan_b27_dplus_passive_passive_shadow_runner_late_repair_only_driver_20260520T055412Z"
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    resolved = str(path.resolve())
    return not any(fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


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


def rounded(value: Any, digits: int = 6) -> float:
    return round(as_float(value), digits)


def safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return numerator / denominator


def metric_delta(current: dict[str, Any], prior: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in fields:
        c = as_float(current.get(field))
        p = as_float(prior.get(field))
        out[field] = {
            "current": rounded(c),
            "prior": rounded(p),
            "absolute_delta": rounded(c - p),
            "relative_delta": None if p == 0.0 else round((c - p) / p, 8),
        }
    return out


def compact_profile(profile: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "edge",
        "target_qty",
        "max_open_cost",
        "max_seed_qty",
        "seed_px_lo",
        "seed_px_hi",
        "seed_l1_pair_cap",
        "seed_l1_cap",
        "imbalance_qty_cap",
        "late_repair_only_after_s",
        "late_repair_fill_to_balance_after_s",
        "fill_haircut",
        "official_fee_rate",
        "taker_fee_rate",
    ]
    return {key: profile.get(key) for key in keys if key in profile}


def config_delta(pipeline_profile: dict[str, Any], shadow_config: dict[str, Any]) -> dict[str, Any]:
    pairs = {
        "edge": ("edge", "edge"),
        "target_qty": ("target_qty", "target_qty"),
        "max_open_cost": ("max_open_cost", "max_open_cost"),
        "max_seed_qty": ("max_seed_qty", "max_seed_qty"),
        "seed_px_lo": ("seed_px_lo", "seed_px_lo"),
        "seed_px_hi": ("seed_px_hi", "seed_px_hi"),
        "l1_pair_cap": ("seed_l1_pair_cap", "seed_l1_cap"),
        "imbalance_qty_cap": ("imbalance_qty_cap", "imbalance_qty_cap"),
        "late_repair_only_after_s": ("late_repair_only_after_s", "late_repair_only_after_s"),
        "late_repair_fill_to_balance_after_s": (
            "late_repair_fill_to_balance_after_s",
            "late_repair_fill_to_balance_after_s",
        ),
        "fill_haircut": ("fill_haircut", "fill_haircut"),
        "fee_rate": ("official_fee_rate", "taker_fee_rate"),
    }
    out: dict[str, Any] = {}
    for label, (pipeline_key, shadow_key) in pairs.items():
        pipeline_value = pipeline_profile.get(pipeline_key)
        shadow_value = shadow_config.get(shadow_key)
        item: dict[str, Any] = {"pipeline": pipeline_value, "shadow": shadow_value}
        if isinstance(pipeline_value, (int, float)) and isinstance(shadow_value, (int, float)):
            item["absolute_delta"] = round(float(shadow_value) - float(pipeline_value), 8)
        item["matches"] = pipeline_value == shadow_value
        out[label] = item
    return out


def aggregate_day_delta(control: dict[str, Any], variant: dict[str, Any]) -> list[dict[str, Any]]:
    control_days = {row["day"]: row for row in control.get("summary_by_day") or []}
    variant_days = {row["day"]: row for row in variant.get("summary_by_day") or []}
    rows: list[dict[str, Any]] = []
    for day in sorted(set(control_days) | set(variant_days)):
        c = control_days.get(day, {})
        v = variant_days.get(day, {})
        row = {
            "day": day,
            "seed_actions_delta": rounded(as_float(v.get("seed_actions")) - as_float(c.get("seed_actions"))),
            "pair_actions_delta": rounded(as_float(v.get("pair_actions")) - as_float(c.get("pair_actions"))),
            "pair_qty_delta": rounded(as_float(v.get("pair_qty")) - as_float(c.get("pair_qty"))),
            "fee_after_pnl_delta": rounded(as_float(v.get("fee_after_pnl")) - as_float(c.get("fee_after_pnl"))),
            "stress100_worst_pnl_delta": rounded(
                as_float(v.get("stress100_worst_pnl")) - as_float(c.get("stress100_worst_pnl"))
            ),
            "residual_qty_delta": rounded(as_float(v.get("residual_qty")) - as_float(c.get("residual_qty"))),
            "residual_cost_delta": rounded(as_float(v.get("residual_cost")) - as_float(c.get("residual_cost"))),
            "control_residual_qty_rate": rounded(c.get("qty_residual_rate")),
            "variant_residual_qty_rate": rounded(v.get("qty_residual_rate")),
        }
        rows.append(row)
    rows.sort(key=lambda item: (item["residual_qty_delta"], item["residual_cost_delta"]))
    return rows


def load_shadow(shadow_dir: Path) -> dict[str, Any]:
    output = shadow_dir / "remote" / "output"
    aggregate = read_json(output / "aggregate_report.json")
    manifest = read_json(output / "manifest.json")
    summaries = []
    for path in sorted(output.glob("*.summary.json")):
        summary = read_json(path)
        metrics = summary.get("metrics") or {}
        event_lite = summary.get("event_lite") or {}
        summaries.append(
            {
                "slug": summary.get("slug"),
                "metrics": metrics,
                "event_lite": event_lite,
                "residual_qty": as_float(metrics.get("residual_qty")),
                "residual_cost": as_float(metrics.get("residual_cost")),
                "pair_actions": as_float(metrics.get("pair_actions")),
                "pair_qty": as_float(metrics.get("pair_qty")),
                "pair_pnl": as_float(metrics.get("pair_pnl")),
                "candidates": as_float(metrics.get("candidates")),
                "fills": as_float(metrics.get("queue_supported_fills")),
                "fill_to_balance_candidates": as_float(metrics.get("late_repair_fill_to_balance_candidates")),
                "fill_to_balance_caps": as_float(metrics.get("late_repair_fill_to_balance_caps")),
                "fill_to_balance_blocks": as_float(metrics.get("late_repair_fill_to_balance_blocks")),
                "fill_to_balance_qty_reduction": as_float(
                    metrics.get("late_repair_fill_to_balance_qty_reduction")
                ),
            }
        )
    return {"aggregate": aggregate, "manifest": manifest, "summaries": summaries, "output": output}


def sum_summaries(rows: list[dict[str, Any]], predicate: Any) -> dict[str, Any]:
    selected = [row for row in rows if predicate(row)]
    return {
        "slugs": len(selected),
        "candidates": rounded(sum(row["candidates"] for row in selected)),
        "fills": rounded(sum(row["fills"] for row in selected)),
        "pair_actions": rounded(sum(row["pair_actions"] for row in selected)),
        "pair_qty": rounded(sum(row["pair_qty"] for row in selected)),
        "pair_pnl": rounded(sum(row["pair_pnl"] for row in selected)),
        "residual_qty": rounded(sum(row["residual_qty"] for row in selected)),
        "residual_cost": rounded(sum(row["residual_cost"] for row in selected)),
        "fill_to_balance_candidates": rounded(sum(row["fill_to_balance_candidates"] for row in selected)),
        "fill_to_balance_caps": rounded(sum(row["fill_to_balance_caps"] for row in selected)),
        "fill_to_balance_blocks": rounded(sum(row["fill_to_balance_blocks"] for row in selected)),
        "fill_to_balance_qty_reduction": rounded(sum(row["fill_to_balance_qty_reduction"] for row in selected)),
    }


def top_slug_rows(rows: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    out = []
    for row in sorted(rows, key=lambda item: (item["residual_qty"], item["residual_cost"]), reverse=True)[:limit]:
        event_lite = row["event_lite"]
        out.append(
            {
                "slug": row["slug"],
                "candidates": int(row["candidates"]),
                "fills": int(row["fills"]),
                "pair_actions": int(row["pair_actions"]),
                "pair_qty": rounded(row["pair_qty"]),
                "pair_pnl": rounded(row["pair_pnl"]),
                "residual_qty": rounded(row["residual_qty"]),
                "residual_cost": rounded(row["residual_cost"]),
                "fill_to_balance_candidates": int(row["fill_to_balance_candidates"]),
                "fill_to_balance_caps": int(row["fill_to_balance_caps"]),
                "fill_to_balance_blocks": int(row["fill_to_balance_blocks"]),
                "fill_to_balance_qty_reduction": rounded(row["fill_to_balance_qty_reduction"]),
                "candidate_offset_buckets": event_lite.get("candidate_offset_buckets"),
                "residual_lot_side_qty": event_lite.get("residual_lot_side_qty"),
                "residual_lot_px_qty_buckets": event_lite.get("residual_lot_px_qty_buckets"),
                "block_by_reason_offset_bucket": event_lite.get("block_by_reason_offset_bucket"),
            }
        )
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-manifest", type=Path, default=DEFAULT_PIPELINE_MANIFEST)
    parser.add_argument("--fill-shadow-dir", type=Path, default=DEFAULT_FILL_SHADOW)
    parser.add_argument("--late-repair-shadow-dir", type=Path, default=DEFAULT_LATE_REPAIR_SHADOW)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"

    required_paths = [args.pipeline_manifest, args.fill_shadow_dir, args.late_repair_shadow_dir]
    missing = [str(path) for path in required_paths if not path.exists()]
    unsafe = [str(path) for path in required_paths if path.exists() and not path_safe(path)]
    if missing or unsafe:
        manifest = {
            "schema_version": 1,
            "artifact": ARTIFACT,
            "status": "BLOCKED_REQUIRED_LOCAL_ARTIFACT_MISSING_OR_UNSAFE",
            "decision": "BLOCKED",
            "created_utc": label,
            "missing_paths": missing,
            "unsafe_paths": unsafe,
            "side_effects": {"ssh": False, "network": False, "raw_replay_scanned": False, "shadow_started": False},
        }
        write_json(out_dir / "manifest.json", manifest)
        print(out_dir / "manifest.json")
        return 2

    pipeline = read_json(args.pipeline_manifest)
    fill_shadow = load_shadow(args.fill_shadow_dir)
    late_shadow = load_shadow(args.late_repair_shadow_dir)

    pipeline_control = pipeline["variant_late_repair90_rerun"]
    pipeline_fill = pipeline["variant_late_repair90_fill_to_balance_after_90_rerun"]
    fill_agg_metrics = fill_shadow["aggregate"].get("metrics") or {}
    late_agg_metrics = late_shadow["aggregate"].get("metrics") or {}
    fill_manifest_config = fill_shadow["manifest"].get("config") or {}

    shadow_fields = [
        "candidates",
        "queue_supported_fills",
        "pair_actions",
        "pair_qty",
        "pair_pnl",
        "roi_on_filled_cost",
        "net_pair_cost_proxy_p90",
        "residual_qty",
        "residual_cost",
        "material_residual_lots",
    ]
    shadow_delta = metric_delta(fill_agg_metrics, late_agg_metrics, shadow_fields)

    fill_summaries = fill_shadow["summaries"]
    slugs_with_fill_to_balance = sum_summaries(fill_summaries, lambda row: row["fill_to_balance_candidates"] > 0)
    slugs_without_fill_to_balance = sum_summaries(fill_summaries, lambda row: row["fill_to_balance_candidates"] <= 0)
    residual_slugs_with_fill_to_balance = sum_summaries(
        fill_summaries, lambda row: row["fill_to_balance_candidates"] > 0 and row["residual_qty"] > 0
    )
    residual_slugs_without_fill_to_balance = sum_summaries(
        fill_summaries, lambda row: row["fill_to_balance_candidates"] <= 0 and row["residual_qty"] > 0
    )

    fee_adjusted_pair_pnl_proxy = as_float(fill_agg_metrics.get("pair_pnl")) - as_float(fill_agg_metrics.get("taker_fee"))
    conservative_proxy = fee_adjusted_pair_pnl_proxy - as_float(fill_agg_metrics.get("residual_cost"))
    fill_agg_event_lite = fill_shadow["aggregate"].get("event_lite") or {}

    config_mismatch = config_delta(pipeline_fill, fill_manifest_config)
    hard_config_mismatches = {
        key: value
        for key, value in config_mismatch.items()
        if not value["matches"] and key in {"edge", "max_open_cost"}
    }
    missing_source_fields = [
        "per-candidate late_repair_fill_to_balance_deficit bucket in summary/aggregate",
        "per-candidate base_seed_qty/capped_seed_qty/qty_reduction buckets by slug/side/offset",
        "residual lot source link to fill_to_balance_active seed without reading events JSONL",
        "pair action source link to fill_to_balance_active seed without reading events JSONL",
        "same-window candidate-pipeline replay with shadow-aligned edge=0.07 and max_open_cost=80",
    ]

    pipeline_day_delta = aggregate_day_delta(pipeline_control, pipeline_fill)
    pipeline_days_with_residual_improvement = sum(1 for row in pipeline_day_delta if row["residual_qty_delta"] < 0)
    pipeline_days_with_stress_improvement = sum(1 for row in pipeline_day_delta if row["stress100_worst_pnl_delta"] > 0)
    shadow_residual_in_slugs_without_ftb = residual_slugs_without_fill_to_balance["residual_qty"]
    shadow_total_residual = as_float(fill_agg_metrics.get("residual_qty"))
    shadow_residual_without_ftb_share = safe_ratio(shadow_residual_in_slugs_without_ftb, shadow_total_residual)

    decision = "UNKNOWN"
    status = "UNKNOWN_FILL_TO_BALANCE_MISMATCH_CONFIG_AND_SOURCE_FIELDS"
    if conservative_proxy < 0 and shadow_residual_without_ftb_share is not None and shadow_residual_without_ftb_share > 0.5:
        status = "UNKNOWN_FILL_TO_BALANCE_SHADOW_STRESS_NEGATIVE_BUT_SOURCE_FIELDS_INSUFFICIENT"

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "status": status,
        "decision": decision,
        "created_utc": label,
        "scope": "local_no_network_mismatch_attribution",
        "inputs": {
            "pipeline_manifest": str(args.pipeline_manifest),
            "fill_shadow_dir": str(args.fill_shadow_dir),
            "late_repair_shadow_dir": str(args.late_repair_shadow_dir),
        },
        "pipeline_fill_to_balance_evidence": {
            "control": {
                "seed_actions": pipeline_control["seed_actions"],
                "pair_actions": pipeline_control["pair_actions"],
                "pair_qty": pipeline_control["pair_qty"],
                "fee_after_pnl": pipeline_control["fee_after_pnl"],
                "stress100_worst_pnl": pipeline_control["stress100_worst_pnl"],
                "residual_qty_rate": pipeline_control["residual_qty_rate"],
                "residual_cost_rate": pipeline_control["residual_cost_rate"],
            },
            "fill_to_balance": {
                "seed_actions": pipeline_fill["seed_actions"],
                "pair_actions": pipeline_fill["pair_actions"],
                "pair_qty": pipeline_fill["pair_qty"],
                "fee_after_pnl": pipeline_fill["fee_after_pnl"],
                "stress100_worst_pnl": pipeline_fill["stress100_worst_pnl"],
                "residual_qty_rate": pipeline_fill["residual_qty_rate"],
                "residual_cost_rate": pipeline_fill["residual_cost_rate"],
                "seed_qty_cap_late_fill_to_balance": pipeline_fill["seed_qty_cap_late_fill_to_balance"],
                "seed_block_late_fill_to_balance_qty": pipeline_fill["seed_block_late_fill_to_balance_qty"],
                "seed_late_fill_to_balance_qty_reduction": pipeline_fill[
                    "seed_late_fill_to_balance_qty_reduction"
                ],
            },
            "delta_vs_late_repair90": pipeline.get("fill_to_balance_delta_vs_late_repair90"),
            "research_ranking": pipeline.get("fill_to_balance_research_ranking"),
            "days_with_residual_improvement": pipeline_days_with_residual_improvement,
            "days_with_stress_improvement": pipeline_days_with_stress_improvement,
            "worst_residual_delta_days": pipeline_day_delta[:5],
            "best_residual_delta_days": list(reversed(pipeline_day_delta[-5:])),
        },
        "shadow_fill_to_balance_evidence": {
            "metrics": {
                "candidates": fill_agg_metrics.get("candidates"),
                "queue_supported_fills": fill_agg_metrics.get("queue_supported_fills"),
                "pair_actions": fill_agg_metrics.get("pair_actions"),
                "pair_qty": fill_agg_metrics.get("pair_qty"),
                "pair_pnl": fill_agg_metrics.get("pair_pnl"),
                "taker_fee": fill_agg_metrics.get("taker_fee"),
                "fee_adjusted_pair_pnl_proxy": round(fee_adjusted_pair_pnl_proxy, 6),
                "residual_qty": fill_agg_metrics.get("residual_qty"),
                "residual_cost": fill_agg_metrics.get("residual_cost"),
                "conservative_risk_adjusted_pnl_proxy": round(conservative_proxy, 6),
                "net_pair_cost_proxy_p90": fill_agg_metrics.get("net_pair_cost_proxy_p90"),
                "material_residual_lots": fill_agg_metrics.get("material_residual_lots"),
                "late_repair_fill_to_balance_candidates": fill_agg_metrics.get(
                    "late_repair_fill_to_balance_candidates"
                ),
                "late_repair_fill_to_balance_caps": fill_agg_metrics.get("late_repair_fill_to_balance_caps"),
                "late_repair_fill_to_balance_blocks": fill_agg_metrics.get("late_repair_fill_to_balance_blocks"),
                "late_repair_fill_to_balance_qty_reduction": fill_agg_metrics.get(
                    "late_repair_fill_to_balance_qty_reduction"
                ),
            },
            "delta_vs_prior_late_repair_only90_shadow": shadow_delta,
            "slugs_with_fill_to_balance_activity": slugs_with_fill_to_balance,
            "slugs_without_fill_to_balance_activity": slugs_without_fill_to_balance,
            "residual_slugs_with_fill_to_balance_activity": residual_slugs_with_fill_to_balance,
            "residual_slugs_without_fill_to_balance_activity": residual_slugs_without_fill_to_balance,
            "residual_without_fill_to_balance_activity_share": (
                None if shadow_residual_without_ftb_share is None else round(shadow_residual_without_ftb_share, 8)
            ),
            "top_residual_slugs": top_slug_rows(fill_summaries),
            "aggregate_event_lite_focus": {
                "candidate_offset_buckets": fill_agg_event_lite.get("candidate_offset_buckets"),
                "candidate_side_counts": fill_agg_event_lite.get("candidate_side_counts"),
                "block_by_reason_offset_bucket": fill_agg_event_lite.get("block_by_reason_offset_bucket"),
                "residual_lot_side_qty": fill_agg_event_lite.get("residual_lot_side_qty"),
                "residual_lot_side_cost": fill_agg_event_lite.get("residual_lot_side_cost"),
                "residual_lot_px_qty_buckets": fill_agg_event_lite.get("residual_lot_px_qty_buckets"),
                "pair_cost_buckets": fill_agg_event_lite.get("pair_cost_buckets"),
                "pair_cost_by_source_offset_bucket": fill_agg_event_lite.get("pair_cost_by_source_offset_bucket"),
                "pair_cost_by_source_side": fill_agg_event_lite.get("pair_cost_by_source_side"),
            },
        },
        "config_alignment": {
            "pipeline_profile": compact_profile(pipeline_fill),
            "shadow_config": compact_profile(fill_manifest_config),
            "field_comparison": config_mismatch,
            "hard_config_mismatches": hard_config_mismatches,
        },
        "interpretation": [
            "The local candidate-pipeline KEEP was measured at edge=0.055 and max_open_cost=250, while the same-host shadow used edge=0.07 and max_open_cost=80.",
            "In the same-host shadow, most residual came from slugs without fill-to-balance activity, so the pulled summary cannot prove that fill-to-balance caps directly caused the residual tail.",
            "The shadow did show a real economic stress failure: fee-adjusted pair PnL was positive, but subtracting full residual_cost made the conservative proxy negative.",
            "Current summary/event-lite fields are insufficient to join residual lots and pair actions back to individual fill-to-balance cap/block candidates without events JSONL, which was intentionally not pulled.",
        ],
        "missing_fields_for_keep_decision": missing_source_fields,
        "research_ranking": {
            "label": "UNKNOWN_SHADOW_SUMMARY_SOURCE_FIELDS_INSUFFICIENT",
            "economic_pnl_positive": as_float(fill_agg_metrics.get("pair_pnl")) >= 0.0,
            "conservative_stress_proxy_positive": conservative_proxy >= 0.0,
            "risk_adjusted_pnl_proxy": round(conservative_proxy, 6),
            "sample_size_ok": as_float(fill_agg_metrics.get("candidates")) >= 100.0,
            "reason": "config mismatch plus missing per-candidate source linkage prevents a deployable mismatch attribution",
        },
        "promotion_gate": {
            "passed": False,
            "required_before_g2_canary": True,
            "reason": "local mismatch attribution only; prior shadow acceptance failed sample and normalized residual budget",
        },
        "side_effects": {
            "ssh": False,
            "network": False,
            "shadow_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_touched": False,
            "raw_replay_scanned": False,
            "events_jsonl_read": False,
            "full_completion_store_scanned": False,
        },
        "next_executable_action": (
            "Run a local candidate-pipeline state-machine rerun with shadow-aligned config "
            "(edge=0.07, max_open_cost=80) comparing late_repair90 control vs "
            "late_repair90_fill_to_balance_after_90 before any further EC2 shadow."
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
