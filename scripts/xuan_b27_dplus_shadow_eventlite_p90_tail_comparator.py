#!/usr/bin/env python3
"""Compare local event-lite D+ shadow summaries for p90-tail attribution.

This script reads only pulled manifest, aggregate_report, and *.summary.json
files. It does not read event JSONL, raw/replay data, sockets, SSH, or network
paths.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path("xuan_research_artifacts")

DEFAULT_LATE_REPAIR_ONLY = (
    ROOT / "xuan_b27_dplus_passive_passive_shadow_runner_late_repair_only_driver_20260520T055412Z"
)
DEFAULT_EVENTLITE = (
    ROOT / "xuan_b27_dplus_passive_passive_shadow_runner_eventlite_driver_20260520T022345Z"
)
DEFAULT_SAMPLE_SCALE = (
    ROOT / "xuan_b27_dplus_passive_passive_shadow_runner_sample_scale_multprofile_driver_20260519T232703Z"
)

FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
)

METRIC_KEYS = (
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
    "filled_qty",
    "seed_qty",
    "fill_rate",
)

SUMMARY_METRIC_KEYS = (
    "candidates",
    "queue_supported_fills",
    "pair_actions",
    "pair_qty",
    "pair_pnl",
    "roi_on_filled_cost",
    "net_pair_cost_p90",
    "net_pair_cost_proxy_p90",
    "residual_qty",
    "residual_cost",
    "material_residual_lots",
    "filled_qty",
    "seed_qty",
    "fill_rate",
)

PAIR_COST_BUCKET_ORDER = (
    "pair_cost_lt_0p95",
    "pair_cost_0p95_1p00",
    "pair_cost_1p00_1p05",
    "pair_cost_gt_1p05",
    "pair_cost_ge_1p05",
)

HIGH_PAIR_COST_BUCKETS = (
    "pair_cost_1p00_1p05",
    "pair_cost_gt_1p05",
    "pair_cost_ge_1p05",
)

EVENT_LITE_KEYS = (
    "candidate_offset_buckets",
    "candidate_public_trade_px_buckets",
    "candidate_seed_px_buckets",
    "candidate_qty_by_seed_px_bucket",
    "candidate_side_counts",
    "fill_seed_px_buckets",
    "fill_side_counts",
    "net_pair_cost_buckets",
    "pair_cost_buckets",
    "residual_lot_px_qty_buckets",
    "residual_lot_side_qty",
    "residual_lot_side_cost",
    "residual_lot_side_count",
    "block_by_reason_offset_bucket",
    "block_by_reason_public_px_bucket",
    "block_by_reason_side",
)


@dataclass
class ProfileSpec:
    group: str
    profile: str
    output_dir: Path


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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


def ratio(numerator: float, denominator: float) -> float | None:
    return None if denominator == 0 else numerator / denominator


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path) -> dict[str, Any]:
    if not path_safe(path):
        raise SystemExit(f"unsafe input path: {path}")
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def scalar_map(raw: dict[str, Any] | None) -> dict[str, float]:
    return {str(k): as_float(v) for k, v in (raw or {}).items()}


def nested_scalar_map(raw: dict[str, Any] | None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for key, value in (raw or {}).items():
        if isinstance(value, dict):
            out[str(key)] = scalar_map(value)
    return out


def add_scalar_maps(left: dict[str, float], right: dict[str, float], scale: float = 1.0) -> dict[str, float]:
    out = dict(left)
    for key, value in right.items():
        out[key] = out.get(key, 0.0) + value * scale
    return {key: value for key, value in out.items() if abs(value) > 1e-12}


def event_lite_extract(raw: dict[str, Any]) -> dict[str, Any]:
    lite = raw.get("event_lite") or {}
    out: dict[str, Any] = {}
    for key in EVENT_LITE_KEYS:
        value = lite.get(key) or {}
        if key.startswith("block_by_reason_"):
            out[key] = nested_scalar_map(value)
        else:
            out[key] = scalar_map(value)
    return out


def metric_extract(raw: dict[str, Any], keys: tuple[str, ...]) -> dict[str, float]:
    metrics = raw.get("metrics") or {}
    out = {key: as_float(metrics.get(key)) for key in keys}
    if "net_pair_cost_proxy_p90" in out and not out["net_pair_cost_proxy_p90"]:
        out["net_pair_cost_proxy_p90"] = as_float(metrics.get("net_pair_cost_p90"))
    return out


def high_pair_cost_count(event_lite: dict[str, Any]) -> float:
    buckets = event_lite.get("net_pair_cost_buckets") or event_lite.get("pair_cost_buckets") or {}
    return sum(as_float(buckets.get(key)) for key in HIGH_PAIR_COST_BUCKETS)


def p90_bucket_from_buckets(buckets: dict[str, float]) -> dict[str, Any]:
    total = sum(as_float(v) for v in buckets.values())
    if total <= 0:
        return {"total": 0.0, "bucket": None, "pass_proxy": None, "high_cost_count": 0.0, "high_cost_share": None}
    cumulative = 0.0
    selected = None
    for bucket in PAIR_COST_BUCKET_ORDER:
        cumulative += as_float(buckets.get(bucket))
        if cumulative / total >= 0.9:
            selected = bucket
            break
    high = sum(as_float(buckets.get(key)) for key in HIGH_PAIR_COST_BUCKETS)
    return {
        "total": total,
        "bucket": selected,
        "pass_proxy": selected in ("pair_cost_lt_0p95", "pair_cost_0p95_1p00"),
        "high_cost_count": high,
        "high_cost_share": high / total,
    }


def load_profile(spec: ProfileSpec) -> dict[str, Any]:
    out = spec.output_dir
    aggregate_path = out / "aggregate_report.json"
    if not aggregate_path.exists():
        raise SystemExit(f"missing aggregate report: {aggregate_path}")
    aggregate = read_json(aggregate_path)
    summaries = []
    for path in sorted(out.glob("*.summary.json")):
        data = read_json(path)
        lite = event_lite_extract(data)
        sm = metric_extract(data, SUMMARY_METRIC_KEYS)
        if not sm.get("net_pair_cost_proxy_p90"):
            sm["net_pair_cost_proxy_p90"] = sm.get("net_pair_cost_p90", 0.0)
        summaries.append(
            {
                "path": str(path),
                "slug": str(data.get("slug") or path.name.replace(".summary.json", "")),
                "metrics": sm,
                "blocked": scalar_map(data.get("blocked")),
                "event_lite": lite,
                "top_residual_lots": data.get("top_residual_lots") or [],
            }
        )
    event_lite = event_lite_extract(aggregate)
    metrics = metric_extract(aggregate, METRIC_KEYS)
    buckets = event_lite.get("net_pair_cost_buckets") or event_lite.get("pair_cost_buckets") or {}
    p90_bucket = p90_bucket_from_buckets(buckets)
    return {
        "name": f"{spec.group}:{spec.profile}",
        "group": spec.group,
        "profile": spec.profile,
        "output_dir": str(out),
        "aggregate_path": str(aggregate_path),
        "summary_count": len(summaries),
        "metrics": metrics,
        "blocked": scalar_map(aggregate.get("blocked")),
        "event_lite": event_lite,
        "top_residual_lots": aggregate.get("top_residual_lots") or [],
        "p90_bucket_proxy": p90_bucket,
        "high_pair_cost_count": high_pair_cost_count(event_lite),
        "high_pair_cost_share": ratio(high_pair_cost_count(event_lite), metrics.get("pair_actions", 0.0)),
        "summaries": summaries,
    }


def discover_profiles(
    late_repair_only_dir: Path,
    eventlite_dir: Path,
    sample_scale_dir: Path,
) -> list[ProfileSpec]:
    specs = [
        ProfileSpec("late_repair_only90_1800s_offsets0_25", "late_repair_only90", late_repair_only_dir / "remote" / "output"),
    ]
    for group, root in (
        ("eventlite_1800s_offsets0_25", eventlite_dir),
        ("sample_scale_3600s_offsets0_25", sample_scale_dir),
    ):
        out = root / "remote" / "output"
        for profile in ("repair60", "repair75"):
            specs.append(ProfileSpec(group, profile, out / profile))
    return specs


def top_slug_rows(profile: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in profile["summaries"]:
        lite = item["event_lite"]
        metrics = item["metrics"]
        buckets = lite.get("net_pair_cost_buckets") or lite.get("pair_cost_buckets") or {}
        rows.append(
            {
                "slug": item["slug"],
                "candidates": metrics.get("candidates", 0.0),
                "pair_actions": metrics.get("pair_actions", 0.0),
                "pair_pnl": metrics.get("pair_pnl", 0.0),
                "net_pair_cost_p90": metrics.get("net_pair_cost_proxy_p90", 0.0),
                "residual_qty": metrics.get("residual_qty", 0.0),
                "residual_cost": metrics.get("residual_cost", 0.0),
                "high_pair_cost_count": sum(as_float(buckets.get(key)) for key in HIGH_PAIR_COST_BUCKETS),
                "high_pair_cost_share": ratio(
                    sum(as_float(buckets.get(key)) for key in HIGH_PAIR_COST_BUCKETS),
                    metrics.get("pair_actions", 0.0),
                ),
                "net_pair_cost_buckets": buckets,
                "candidate_seed_px_buckets": lite.get("candidate_seed_px_buckets") or {},
                "candidate_public_trade_px_buckets": lite.get("candidate_public_trade_px_buckets") or {},
                "candidate_offset_buckets": lite.get("candidate_offset_buckets") or {},
                "residual_lot_side_qty": lite.get("residual_lot_side_qty") or {},
                "residual_lot_px_qty_buckets": lite.get("residual_lot_px_qty_buckets") or {},
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["high_pair_cost_count"],
            row["net_pair_cost_p90"],
            row["residual_qty"],
            row["candidates"],
        ),
        reverse=True,
    )


def subtract_summary_metrics(profile: dict[str, Any], removed: list[dict[str, Any]]) -> dict[str, float]:
    metrics = dict(profile["metrics"])
    for item in removed:
        for key in METRIC_KEYS:
            if key == "net_pair_cost_proxy_p90":
                continue
            if key in item["metrics"]:
                metrics[key] = metrics.get(key, 0.0) - item["metrics"].get(key, 0.0)
    return metrics


def subtract_buckets(profile: dict[str, Any], removed: list[dict[str, Any]], bucket_key: str) -> dict[str, float]:
    buckets = dict(profile["event_lite"].get(bucket_key) or {})
    for item in removed:
        buckets = add_scalar_maps(buckets, item["event_lite"].get(bucket_key) or {}, scale=-1.0)
    return buckets


def removal_proxy(profile: dict[str, Any]) -> list[dict[str, Any]]:
    summary_rows = profile["summaries"]
    variants = {
        "drop_slugs_with_pair_cost_gt_1p00": [
            item
            for item in summary_rows
            if high_pair_cost_count(item["event_lite"]) > 0
            or item["metrics"].get("net_pair_cost_proxy_p90", 0.0) > 1.0
        ],
        "drop_slugs_with_residual_qty_ge_2p5": [
            item for item in summary_rows if item["metrics"].get("residual_qty", 0.0) >= 2.5
        ],
        "drop_slugs_with_no_residual_px_lt_0p30": [
            item
            for item in summary_rows
            if (item["event_lite"].get("residual_lot_side_qty") or {}).get("NO", 0.0) > 0
            and (item["event_lite"].get("residual_lot_px_qty_buckets") or {}).get("px_lt_0p30", 0.0) > 0
        ],
    }
    out = []
    base_candidates = profile["metrics"].get("candidates", 0.0)
    for name, removed in variants.items():
        retained = subtract_summary_metrics(profile, removed)
        cost_buckets = subtract_buckets(profile, removed, "net_pair_cost_buckets")
        if not cost_buckets:
            cost_buckets = subtract_buckets(profile, removed, "pair_cost_buckets")
        p90_proxy = p90_bucket_from_buckets(cost_buckets)
        retained_candidates = retained.get("candidates", 0.0)
        out.append(
            {
                "rule": name,
                "removed_slug_count": len(removed),
                "removed_slugs": [item["slug"] for item in removed[:12]],
                "retained_candidates": retained_candidates,
                "retained_candidate_ratio": ratio(retained_candidates, base_candidates),
                "retained_pair_actions": retained.get("pair_actions", 0.0),
                "retained_pair_pnl": retained.get("pair_pnl", 0.0),
                "retained_residual_qty": retained.get("residual_qty", 0.0),
                "retained_residual_cost": retained.get("residual_cost", 0.0),
                "retained_material_residual_lots": retained.get("material_residual_lots", 0.0),
                "p90_bucket_proxy_after": p90_proxy,
                "passes_core_proxy": bool(
                    retained_candidates >= 100
                    and (p90_proxy.get("pass_proxy") is True)
                    and retained.get("residual_qty", 0.0) <= 10
                    and retained.get("pair_pnl", 0.0) > 0
                    and retained.get("material_residual_lots", 0.0) <= 0
                ),
            }
        )
    return out


def profile_summary(profile: dict[str, Any]) -> dict[str, Any]:
    m = profile["metrics"]
    return {
        "name": profile["name"],
        "group": profile["group"],
        "profile": profile["profile"],
        "summary_count": profile["summary_count"],
        "metrics": {key: m.get(key, 0.0) for key in METRIC_KEYS},
        "blocked": profile["blocked"],
        "p90_bucket_proxy": profile["p90_bucket_proxy"],
        "high_pair_cost_count": profile["high_pair_cost_count"],
        "high_pair_cost_share": profile["high_pair_cost_share"],
        "candidate_offset_buckets": profile["event_lite"].get("candidate_offset_buckets") or {},
        "candidate_seed_px_buckets": profile["event_lite"].get("candidate_seed_px_buckets") or {},
        "candidate_public_trade_px_buckets": profile["event_lite"].get("candidate_public_trade_px_buckets") or {},
        "candidate_side_counts": profile["event_lite"].get("candidate_side_counts") or {},
        "net_pair_cost_buckets": profile["event_lite"].get("net_pair_cost_buckets") or {},
        "residual_lot_side_qty": profile["event_lite"].get("residual_lot_side_qty") or {},
        "residual_lot_px_qty_buckets": profile["event_lite"].get("residual_lot_px_qty_buckets") or {},
        "block_by_reason_offset_bucket": profile["event_lite"].get("block_by_reason_offset_bucket") or {},
        "block_by_reason_public_px_bucket": profile["event_lite"].get("block_by_reason_public_px_bucket") or {},
        "block_by_reason_side": profile["event_lite"].get("block_by_reason_side") or {},
        "top_p90_cost_slugs": top_slug_rows(profile)[:8],
        "top_residual_lots": profile["top_residual_lots"][:8],
        "removal_proxy": removal_proxy(profile),
    }


def delta(new: dict[str, Any], old: dict[str, Any]) -> dict[str, Any]:
    nm = new["metrics"]
    om = old["metrics"]
    return {
        "new": new["name"],
        "old": old["name"],
        "metric_delta": {
            key: {
                "old": om.get(key, 0.0),
                "new": nm.get(key, 0.0),
                "delta": nm.get(key, 0.0) - om.get(key, 0.0),
                "ratio": ratio(nm.get(key, 0.0), om.get(key, 0.0)),
            }
            for key in METRIC_KEYS
        },
        "high_pair_cost_delta": {
            "old": old["high_pair_cost_count"],
            "new": new["high_pair_cost_count"],
            "delta": new["high_pair_cost_count"] - old["high_pair_cost_count"],
        },
    }


def decide(profile_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    qualified = []
    p90_residual_pass_sample_fail = []
    for profile in profile_summaries:
        m = profile["metrics"]
        p90 = m.get("net_pair_cost_proxy_p90", 0.0)
        core_pass = (
            p90 <= 1.0
            and m.get("residual_qty", 0.0) <= 10
            and m.get("pair_pnl", 0.0) > 0
            and m.get("material_residual_lots", 0.0) <= 0
        )
        if core_pass and m.get("candidates", 0.0) >= 100:
            qualified.append(profile["name"])
        elif core_pass:
            p90_residual_pass_sample_fail.append(profile["name"])

    removal_passes = [
        (profile["name"], rule["rule"])
        for profile in profile_summaries
        for rule in profile["removal_proxy"]
        if rule["passes_core_proxy"]
    ]
    if qualified:
        return {
            "decision_label": "KEEP",
            "status": "KEEP_PROFILE_ALREADY_MEETS_CORE_GATES",
            "reason": "At least one local shadow profile already meets p90/residual/PnL/sample gates.",
            "next_action": "Run bounded no-order acceptance audit for the qualified profile only; do not promote to G2 canary.",
        }
    if removal_passes:
        return {
            "decision_label": "KEEP",
            "status": "KEEP_BOUNDED_REMOVAL_PROXY_P90_TAIL",
            "reason": "A summary-level removal proxy preserves sample, p90 bucket, residual, PnL, and material-residual gates.",
            "next_action": "Implement the bounded rule as a default-off local verifier and smoke it before any EC2 shadow.",
            "supporting_rules": removal_passes,
        }
    return {
        "decision_label": "UNKNOWN",
        "status": "UNKNOWN_P90_TAIL_SOURCE_FIELDS_INSUFFICIENT",
        "reason": (
            "No profile reaches sample>=100 with p90<=1.0 and residual<=10. "
            "Summary/event-lite bucket removal either leaves sample below the gate or cannot join pair-cost tail "
            "back to source seed/public/offset fields, so a deployable p90-tail admission rule is not identifiable yet."
        ),
        "next_action": (
            "Implement a default-off pair-source event-lite attribution path in the no-order runner and smoke it locally: "
            "for each pair action, summarize net_pair_cost bucket by source seed_px bucket, public_trade_px bucket, "
            "offset bucket, side, and quote_intent/source_order id. Then rerun one bounded event-lite shadow only after "
            "the local smoke proves default behavior is unchanged."
        ),
        "positive_context": p90_residual_pass_sample_fail,
        "missing_fields": [
            "pair action net_pair_cost bucket joined to the seed quote_intent/source_order id",
            "pair action source seed_px bucket",
            "pair action source public_trade_price bucket",
            "pair action source offset bucket",
            "pair action source side",
            "aggregate pair_cost_by_source_bucket tables for summary-only audit without events JSONL",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--late-repair-only-dir", type=Path, default=DEFAULT_LATE_REPAIR_ONLY)
    parser.add_argument("--eventlite-dir", type=Path, default=DEFAULT_EVENTLITE)
    parser.add_argument("--sample-scale-dir", type=Path, default=DEFAULT_SAMPLE_SCALE)
    parser.add_argument("--output-root", type=Path, default=ROOT)
    args = parser.parse_args()

    for root in (args.late_repair_only_dir, args.eventlite_dir, args.sample_scale_dir):
        if not path_safe(root):
            raise SystemExit(f"unsafe input root: {root}")
        if not root.exists():
            raise SystemExit(f"missing input root: {root}")

    profiles = [load_profile(spec) for spec in discover_profiles(args.late_repair_only_dir, args.eventlite_dir, args.sample_scale_dir)]
    summaries = [profile_summary(profile) for profile in profiles]
    by_name = {profile["name"]: profile for profile in profiles}

    scoreboard_delta = [
        delta(by_name["late_repair_only90_1800s_offsets0_25:late_repair_only90"], by_name["eventlite_1800s_offsets0_25:repair60"]),
        delta(by_name["late_repair_only90_1800s_offsets0_25:late_repair_only90"], by_name["eventlite_1800s_offsets0_25:repair75"]),
        delta(by_name["late_repair_only90_1800s_offsets0_25:late_repair_only90"], by_name["sample_scale_3600s_offsets0_25:repair60"]),
        delta(by_name["late_repair_only90_1800s_offsets0_25:late_repair_only90"], by_name["sample_scale_3600s_offsets0_25:repair75"]),
    ]
    decision = decide(summaries)

    artifact_dir = args.output_root / f"xuan_b27_dplus_shadow_eventlite_p90_tail_comparator_{utc_label()}"
    manifest = {
        "schema_version": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "lane": "causal_verifier",
        "hypothesis": (
            "If the late-repair-only90 p90 miss is concentrated in deployable seed/public-price, offset, or side buckets, "
            "then a bounded p90-tail gate should preserve residual_qty<=10 and positive PnL without destroying sample."
        ),
        "decision_label": decision["decision_label"],
        "status": decision["status"],
        "decision": decision,
        "input_safety": {
            "read_only_local_json": True,
            "read_events_jsonl": False,
            "ssh": False,
            "network": False,
            "shared_ingress_socket": False,
            "raw_replay_collector_scan": False,
            "full_completion_store_scan": False,
            "orders_cancels_redeems": False,
        },
        "inputs": {
            "late_repair_only_dir": str(args.late_repair_only_dir),
            "eventlite_dir": str(args.eventlite_dir),
            "sample_scale_dir": str(args.sample_scale_dir),
        },
        "profiles": summaries,
        "scoreboard_delta": scoreboard_delta,
        "scoreboard_interpretation": (
            "late-repair-only90 improves residual against sample-scale repair60/75 and keeps PnL positive, "
            "but sample remains below 100 and p90 remains above 1.0. Event-lite repair60 is the only p90-pass profile, "
            "yet it has only 30 candidates."
        ),
        "bounded_artifact": str(artifact_dir / "manifest.json"),
        "next_action": decision["next_action"],
        "promotion": {
            "deployable": False,
            "can_support_strategy_promotion": False,
            "g2_canary_ready": False,
            "reason": "Summary/event-lite comparator is research-only and does not meet shadow acceptance gates.",
        },
    }
    write_json(artifact_dir / "manifest.json", manifest)
    print(json.dumps({"artifact": str(artifact_dir / "manifest.json"), "status": decision["status"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
