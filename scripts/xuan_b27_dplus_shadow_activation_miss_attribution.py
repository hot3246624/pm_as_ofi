#!/usr/bin/env python3
"""Attribute an activation-gated D+ no-order shadow miss from summary reports.

This reads only local runner aggregate/summary JSON artifacts. It does not read
events JSONL, raw/replay data, collector stores, sockets, or network paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    ".events.jsonl",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


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


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def load_run(run_dir: Path) -> dict[str, Any]:
    output = run_dir / "remote" / "output"
    if not output.exists():
        output = run_dir
    manifest_path = output / "manifest.json"
    aggregate_path = output / "aggregate_report.json"
    if not path_safe(run_dir) or not path_safe(output) or not path_safe(manifest_path) or not path_safe(aggregate_path):
        raise SystemExit(f"unsafe input path: {run_dir}")
    manifest = read_json(manifest_path)
    aggregate = read_json(aggregate_path)
    summaries: list[dict[str, Any]] = []
    for path in sorted(output.glob("*.summary.json")):
        if not path_safe(path):
            raise SystemExit(f"unsafe summary path: {path}")
        summaries.append(read_json(path))
    return {
        "run_dir": str(run_dir),
        "output_dir": str(output),
        "manifest_path": str(manifest_path),
        "aggregate_path": str(aggregate_path),
        "manifest": manifest,
        "aggregate": aggregate,
        "summaries": summaries,
    }


def metric(summary: dict[str, Any], name: str) -> float:
    return as_float((summary.get("metrics") or {}).get(name))


def summarize_run(run: dict[str, Any]) -> dict[str, Any]:
    manifest = run["manifest"]
    aggregate = run["aggregate"]
    metrics = aggregate.get("metrics") or {}
    blocked = aggregate.get("blocked") or {}
    duration = as_float(manifest.get("duration_s"))
    candidates = as_float(metrics.get("candidates"))
    residual_qty = as_float(metrics.get("residual_qty"))
    residual_cost = as_float(metrics.get("residual_cost"))
    pair_pnl = as_float(metrics.get("pair_pnl"))
    net_pair_cost_p90 = metrics.get("net_pair_cost_proxy_p90", metrics.get("net_pair_cost_p90"))
    total_blocked = sum(as_float(v) for v in blocked.values())
    activation_blocks = as_float(blocked.get("activation_opp_seen"))
    top_lots = aggregate.get("top_residual_lots") or []
    top_qty = sum(as_float(lot.get("qty")) for lot in top_lots)
    top_cost = sum(as_float(lot.get("cost")) for lot in top_lots)
    top_by_slug: dict[str, dict[str, Any]] = {}
    top_by_side: dict[str, float] = {}
    for lot in top_lots:
        slug = str(lot.get("slug") or "")
        side = str(lot.get("side") or "")
        bucket = top_by_slug.setdefault(slug, {"slug": slug, "top_residual_qty": 0.0, "top_residual_cost": 0.0, "lots": 0})
        bucket["top_residual_qty"] += as_float(lot.get("qty"))
        bucket["top_residual_cost"] += as_float(lot.get("cost"))
        bucket["lots"] += 1
        top_by_side[side] = top_by_side.get(side, 0.0) + as_float(lot.get("qty"))

    per_market: list[dict[str, Any]] = []
    for summary in run["summaries"]:
        sm = summary.get("metrics") or {}
        sb = summary.get("blocked") or {}
        slug = str(summary.get("slug") or "")
        lots = summary.get("top_residual_lots") or []
        residual_side_qty: dict[str, float] = {}
        for lot in lots:
            side = str(lot.get("side") or "")
            residual_side_qty[side] = residual_side_qty.get(side, 0.0) + as_float(lot.get("qty"))
        per_market.append(
            {
                "slug": slug,
                "candidates": as_float(sm.get("candidates")),
                "queue_supported_fills": as_float(sm.get("queue_supported_fills")),
                "pair_actions": as_float(sm.get("pair_actions")),
                "pair_qty": as_float(sm.get("pair_qty")),
                "pair_pnl": as_float(sm.get("pair_pnl")),
                "net_pair_cost_p90": sm.get("net_pair_cost_p90"),
                "residual_qty": as_float(sm.get("residual_qty")),
                "residual_cost": as_float(sm.get("residual_cost")),
                "fill_rate": as_float(sm.get("fill_rate")),
                "activation_blocks": as_float(sb.get("activation_opp_seen")),
                "late_repair_only_blocks": as_float(sb.get("late_repair_only")),
                "l1_pair_ask_gt_cap_blocks": as_float(sb.get("l1_pair_ask_gt_cap")),
                "pairing_only_when_residual_blocks": as_float(sb.get("pairing_only_when_residual")),
                "target_blocks": as_float(sb.get("target")),
                "residual_side_qty": residual_side_qty,
            }
        )
    per_market_by_residual = sorted(per_market, key=lambda row: (row["residual_qty"], row["residual_cost"]), reverse=True)
    per_market_by_candidates = sorted(per_market, key=lambda row: row["candidates"], reverse=True)
    residual_markets = [row for row in per_market_by_residual if row["residual_qty"] > 0]
    residual_top2_qty = sum(row["residual_qty"] for row in per_market_by_residual[:2])
    residual_top3_qty = sum(row["residual_qty"] for row in per_market_by_residual[:3])
    return {
        "run_dir": run["run_dir"],
        "manifest_path": run["manifest_path"],
        "aggregate_path": run["aggregate_path"],
        "summary_count": len(run["summaries"]),
        "duration_s": duration,
        "config": manifest.get("config") or {},
        "metrics": {
            "candidates": candidates,
            "queue_supported_fills": as_float(metrics.get("queue_supported_fills")),
            "pair_actions": as_float(metrics.get("pair_actions")),
            "pair_qty": as_float(metrics.get("pair_qty")),
            "pair_pnl": pair_pnl,
            "roi_on_filled_cost": as_float(metrics.get("roi_on_filled_cost")),
            "net_pair_cost_p90": net_pair_cost_p90,
            "residual_qty": residual_qty,
            "residual_cost": residual_cost,
            "material_residual_lots": as_float(metrics.get("material_residual_lots")),
            "fill_rate": as_float(metrics.get("fill_rate")),
            "filled_qty": as_float(metrics.get("filled_qty")),
            "seed_qty": as_float(metrics.get("seed_qty")),
        },
        "duration_normalized_3600s": {
            "candidates": candidates / duration * 3600.0 if duration else 0.0,
            "pair_pnl": pair_pnl / duration * 3600.0 if duration else 0.0,
            "residual_qty": residual_qty / duration * 3600.0 if duration else 0.0,
            "pair_qty": as_float(metrics.get("pair_qty")) / duration * 3600.0 if duration else 0.0,
        },
        "blocked": blocked,
        "blocked_summary": {
            "total_blocked": total_blocked,
            "activation_opp_seen": activation_blocks,
            "activation_blocks_per_candidate": activation_blocks / candidates if candidates else 0.0,
            "activation_blocks_share_of_blocked": activation_blocks / total_blocked if total_blocked else 0.0,
        },
        "residual_concentration": {
            "residual_markets": len(residual_markets),
            "top2_market_residual_qty": residual_top2_qty,
            "top2_market_residual_share": residual_top2_qty / residual_qty if residual_qty else 0.0,
            "top3_market_residual_qty": residual_top3_qty,
            "top3_market_residual_share": residual_top3_qty / residual_qty if residual_qty else 0.0,
            "aggregate_top_lots_qty": top_qty,
            "aggregate_top_lots_cost": top_cost,
            "aggregate_top_lots_qty_share": top_qty / residual_qty if residual_qty else 0.0,
            "top_residual_by_side_qty": top_by_side,
            "top_residual_by_slug": sorted(top_by_slug.values(), key=lambda row: row["top_residual_qty"], reverse=True),
        },
        "top_markets_by_residual": per_market_by_residual[:8],
        "top_markets_by_candidates": per_market_by_candidates[:8],
        "per_market": per_market,
    }


def ratio(new: float, old: float) -> float | None:
    if old == 0:
        return None
    return new / old


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--activation-run-dir", type=Path, required=True)
    parser.add_argument("--baseline-run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_shadow_activation_miss_attribution_{label}"
    activation = summarize_run(load_run(args.activation_run_dir))
    baseline = summarize_run(load_run(args.baseline_run_dir))

    am = activation["metrics"]
    bm = baseline["metrics"]
    an = activation["duration_normalized_3600s"]
    bn = baseline["duration_normalized_3600s"]
    p90_passed = as_float(am.get("net_pair_cost_p90")) <= 1.0
    residual_failed = as_float(am.get("residual_qty")) > 10.0
    sample_failed = as_float(am.get("candidates")) < 100.0
    activation_blocks_small = activation["blocked_summary"]["activation_blocks_per_candidate"] < 0.25
    residual_concentrated = activation["residual_concentration"]["top2_market_residual_share"] >= 0.5

    if p90_passed and residual_failed and sample_failed and activation_blocks_small and residual_concentrated:
        status = "KEEP_P90_LEAD_RESIDUAL_CONCENTRATED_RESEARCH_ONLY"
    elif p90_passed and residual_failed:
        status = "KEEP_P90_LEAD_RESIDUAL_STILL_FAILS"
    else:
        status = "DISCARD_ACTIVATION_SHADOW_PROXY"

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shadow_activation_miss_attribution",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_summary_aggregate_attribution",
        "hypothesis": (
            "activation15 miss is primarily a duration/residual-concentration problem, not an activation gate "
            "sample-collapse problem, because activation blocks are small while p90 improves"
        ),
        "data_declaration": {
            "data_root": "xuan_research_artifacts",
            "dataset_type": "no_order_shadow_runner_aggregate_and_summary_json",
            "labels": [
                "activation15_edge007_20260519T144649Z",
                "repair_only_edge0055_20260518T174308Z",
            ],
            "days": ["2026-05-18", "2026-05-19"],
            "market_prefix": "btc-updown-5m",
            "assets": ["BTC 5m Polymarket up/down"],
            "row_count": {
                "activation_summary_files": activation["summary_count"],
                "baseline_summary_files": baseline["summary_count"],
            },
            "excluded_20260514_20260515": True,
            "contains_20260518": True,
            "uses_20260518_local_backtest_data": False,
            "public_account_execution_truth_v1_included": False,
            "events_jsonl_read": False,
            "raw_replay_scanned": False,
        },
        "safety": {
            "network_started": False,
            "ssh_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_or_shared_ingress_modified": False,
            "service_control_called": False,
        },
        "activation_run": {
            "run_dir": activation["run_dir"],
            "manifest_path": activation["manifest_path"],
            "aggregate_path": activation["aggregate_path"],
            "duration_s": activation["duration_s"],
            "config": {
                key: activation["config"].get(key)
                for key in (
                    "edge",
                    "target_qty",
                    "late_repair_after_s",
                    "imbalance_qty_cap",
                    "pairing_only_when_residual",
                    "activation_mode",
                    "activation_window_s",
                )
            },
            "metrics": activation["metrics"],
            "duration_normalized_3600s": activation["duration_normalized_3600s"],
            "blocked_summary": activation["blocked_summary"],
            "residual_concentration": activation["residual_concentration"],
            "top_markets_by_residual": activation["top_markets_by_residual"],
        },
        "baseline_run": {
            "run_dir": baseline["run_dir"],
            "duration_s": baseline["duration_s"],
            "config": {
                key: baseline["config"].get(key)
                for key in (
                    "edge",
                    "target_qty",
                    "late_repair_after_s",
                    "imbalance_qty_cap",
                    "pairing_only_when_residual",
                    "activation_mode",
                    "activation_window_s",
                )
            },
            "metrics": baseline["metrics"],
            "duration_normalized_3600s": baseline["duration_normalized_3600s"],
            "blocked_summary": baseline["blocked_summary"],
            "residual_concentration": baseline["residual_concentration"],
            "top_markets_by_residual": baseline["top_markets_by_residual"],
        },
        "delta_vs_baseline": {
            "raw": {
                "candidates": am["candidates"] - bm["candidates"],
                "pair_pnl": am["pair_pnl"] - bm["pair_pnl"],
                "net_pair_cost_p90": as_float(am["net_pair_cost_p90"]) - as_float(bm["net_pair_cost_p90"]),
                "residual_qty": am["residual_qty"] - bm["residual_qty"],
                "residual_cost": am["residual_cost"] - bm["residual_cost"],
            },
            "normalized_3600s": {
                "candidates": an["candidates"] - bn["candidates"],
                "pair_pnl": an["pair_pnl"] - bn["pair_pnl"],
                "residual_qty": an["residual_qty"] - bn["residual_qty"],
                "pair_qty": an["pair_qty"] - bn["pair_qty"],
            },
            "ratios": {
                "candidate_rate_vs_baseline": ratio(an["candidates"], bn["candidates"]),
                "residual_rate_vs_baseline": ratio(an["residual_qty"], bn["residual_qty"]),
                "pair_pnl_rate_vs_baseline": ratio(an["pair_pnl"], bn["pair_pnl"]),
            },
        },
        "decision": {
            "p90_mechanism_validated": p90_passed,
            "sample_gate_failed": sample_failed,
            "residual_gate_failed": residual_failed,
            "activation_blocks_small_relative_to_candidates": activation_blocks_small,
            "residual_is_market_concentrated": residual_concentrated,
            "do_not_promote_to_g2_canary": True,
            "next_action": (
                "Do not run a plain 3600s activation15 as a promotion attempt; if spending EC2, run it as a "
                "residual-scaling falsification or pair it with a residual-tail tweak. Locally inspect the two "
                "dominant residual slugs and test activation-window/late-repair variants that keep p90 <= 1.0."
            ),
        },
    }
    write_json(out_dir / "manifest.json", manifest)
    write_json(out_dir / "activation_per_market.json", activation["per_market"])
    write_json(out_dir / "baseline_per_market.json", baseline["per_market"])
    print(out_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
