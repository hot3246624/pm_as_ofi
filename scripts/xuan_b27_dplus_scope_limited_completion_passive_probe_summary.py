#!/usr/bin/env python3
"""Summarize scope-limited completion-store passive/passive probe output.

The underlying runner is research-only. This summarizer adds the required data
declaration and explicitly blocks promotion/canary interpretation.
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_scope_limited_completion_passive_probe_summary"
SCHEMA_PROBE_ARTIFACT = "xuan_b27_dplus_completion_store_schema_probe"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path | None) -> Any:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def latest_manifest(root: Path, pattern: str, artifact: str) -> Path | None:
    matches = [Path(p) for p in glob.glob(str(root / "xuan_research_artifacts" / pattern / "manifest.json"))]
    matches = [path for path in matches if read_json(path).get("artifact") == artifact]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime_ns)


def latest_runner_dir(root: Path) -> Path | None:
    matches = [
        Path(p)
        for p in glob.glob(
            str(root / "xuan_research_artifacts" / "xuan_b27_dplus_scope_limited_completion_passive_probe_*" / "runner")
        )
    ]
    matches = [path for path in matches if (path / "combined_results.json").exists()]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime_ns)


def label_from_store(store: str) -> str:
    name = Path(store).parent.name
    return name.replace("-", "")


def normalize_day(day: str) -> str:
    return day.replace("-", "")


def safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def pick_top(results: list[dict[str, Any]], key: str, count: int = 5, reverse: bool = True) -> list[dict[str, Any]]:
    return sorted(results, key=lambda item: safe_float(item.get(key)), reverse=reverse)[:count]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner-dir")
    parser.add_argument("--schema-probe")
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    runner_dir = Path(args.runner_dir) if args.runner_dir else latest_runner_dir(root)
    if runner_dir and not runner_dir.is_absolute():
        runner_dir = root / runner_dir
    schema_probe_path = (
        Path(args.schema_probe)
        if args.schema_probe
        else latest_manifest(root, "xuan_b27_dplus_completion_store_schema_probe_*", SCHEMA_PROBE_ARTIFACT)
    )
    if schema_probe_path and not schema_probe_path.is_absolute():
        schema_probe_path = root / schema_probe_path
    runner_manifest = read_json(runner_dir / "manifest.json" if runner_dir else None)
    results = read_json(runner_dir / "combined_results.json" if runner_dir else None)
    schema_probe = read_json(schema_probe_path)
    if not isinstance(results, list):
        results = []
    stores = runner_manifest.get("stores") or []
    labels = [label_from_store(str(item[0])) for item in stores if item]
    days = [normalize_day(day) for item in stores for day in (item[1] if len(item) > 1 else [])]
    schema_rows = {
        str(item.get("label")): int(item.get("row_count") or 0)
        for item in (schema_probe.get("probes") or [])
    }
    row_count = sum(schema_rows.get(item, 0) for item in labels)
    if not row_count:
        row_count = int(schema_probe.get("row_count") or 0)

    nonzero_seed = [item for item in results if safe_float(item.get("seed_actions")) > 0]
    positive_net = [item for item in nonzero_seed if safe_float(item.get("net_pnl")) > 0]
    positive_stress100 = [item for item in nonzero_seed if safe_float(item.get("stress100_actual_pnl")) > 0]
    positive_worst_residual = [
        item for item in nonzero_seed if safe_float(item.get("worst_residual_net_pnl")) > 0
    ]
    positive_stress100_worst = [
        item for item in nonzero_seed if safe_float(item.get("stress100_worst_pnl")) > 0
    ]
    ranked_results = nonzero_seed if nonzero_seed else results
    best_net = pick_top(ranked_results, "net_pnl", 1)[0] if ranked_results else {}
    best_stress100 = pick_top(ranked_results, "stress100_actual_pnl", 1)[0] if ranked_results else {}
    best_worst = pick_top(ranked_results, "worst_residual_net_pnl", 1)[0] if ranked_results else {}
    best_stress100_worst = pick_top(ranked_results, "stress100_worst_pnl", 1)[0] if ranked_results else {}
    summary_passed = bool(results) and bool(nonzero_seed)
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{label}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_scope_limited_completion_passive_passive_research_probe_summary",
        "status": (
            "PASS_SCOPE_LIMITED_COMPLETION_PASSIVE_PROBE_SUMMARY"
            if summary_passed
            else "FAIL_SCOPE_LIMITED_COMPLETION_PASSIVE_PROBE_SUMMARY"
        ),
        "summary_passed": summary_passed,
        "runner_dir": str(runner_dir) if runner_dir else None,
        "runner_manifest": str(runner_dir / "manifest.json") if runner_dir else None,
        "combined_results": str(runner_dir / "combined_results.json") if runner_dir else None,
        "schema_probe": str(schema_probe_path) if schema_probe_path else None,
        "data_root": str(Path(stores[0][0]).parent.parent) if stores else None,
        "dataset_type": "scope_limited_completion_unwind_event_store_v2",
        "labels": labels,
        "days": days,
        "market_prefix": "btc-updown-5m",
        "assets": ["BTC"],
        "row_count": row_count,
        "excluded_20260514_20260515": True,
        "contains_20260518": False,
        "includes_public_account_execution_truth_v1": False,
        "can_support_strategy_promotion": False,
        "requires_compliant_backtest_dataset_for_promotion": True,
        "conclusion_scope": "BTC strict-V2 scope-limited completion-store research only; not full-data, multi-asset, account-truth, deployable, or canary-ready evidence",
        "run_count": len(results),
        "nonzero_seed_run_count": len(nonzero_seed),
        "zero_seed_run_count": max(0, len(results) - len(nonzero_seed)),
        "positive_net_pnl_run_count": len(positive_net),
        "positive_stress100_run_count": len(positive_stress100),
        "positive_worst_residual_run_count": len(positive_worst_residual),
        "positive_stress100_worst_run_count": len(positive_stress100_worst),
        "best_net_pnl": safe_float(best_net.get("net_pnl")),
        "best_net_pnl_config": best_net,
        "best_stress100_actual_pnl": safe_float(best_stress100.get("stress100_actual_pnl")),
        "best_stress100_config": best_stress100,
        "best_worst_residual_net_pnl": safe_float(best_worst.get("worst_residual_net_pnl")),
        "best_worst_residual_config": best_worst,
        "best_stress100_worst_pnl": safe_float(best_stress100_worst.get("stress100_worst_pnl")),
        "best_stress100_worst_config": best_stress100_worst,
        "top_by_net_pnl": pick_top(ranked_results, "net_pnl"),
        "top_by_stress100_actual_pnl": pick_top(ranked_results, "stress100_actual_pnl"),
        "top_by_stress100_worst_pnl": pick_top(ranked_results, "stress100_worst_pnl"),
        "research_interpretation": (
            "positive nominal net PnL is research-only; stress and worst-residual metrics must remain blocking signals"
            if positive_net
            else "no positive nominal net PnL in this scope-limited probe"
        ),
        "promotion_blockers": [
            "scope_limited_completion_store_only",
            "no_public_account_execution_truth_v1",
            "not_current_full_strict_cache_completion_dataset",
            "not_large_no_order_shadow_acceptance",
            "stress_or_worst_residual_risk_not_cleared",
        ],
        "raw_replay_scanned": False,
        "duckdb_tables_read": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "duckdb_tables_read": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
        "next_gate": "use result to focus scope-limited adapter research; promotion still requires compliant data or accepted larger no-order shadow",
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(output_dir / "manifest.json")
    return 0 if summary_passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
