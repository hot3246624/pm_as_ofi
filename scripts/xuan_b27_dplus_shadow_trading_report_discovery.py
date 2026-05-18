#!/usr/bin/env python3
"""Discover real D+ passive/passive shadow-trading reports.

This is an inventory gate for the existing
``tools/xuan_dplus_passive_passive_shadow_runner.py`` output shape. It scans
only local ``xuan_research_artifacts`` paths for ``aggregate_report.json`` plus
the sibling runner ``manifest.json``. It does not start the runner, connect to
shared-ingress, read recorder/raw/replay stores, or touch order paths.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


READY_STATUS = "READY_FOR_SHADOW_TRADING_ACCEPTANCE"
BLOCKED_NO_REPORTS_STATUS = "BLOCKED_NO_REAL_SHADOW_TRADING_REPORTS"
BLOCKED_INVALID_REPORTS_STATUS = "BLOCKED_SHADOW_TRADING_REPORTS_INVALID"
FAIL_UNSAFE_SCAN_ROOT = "FAIL_UNSAFE_SCAN_ROOT"
RUNNER_SCRIPT = "xuan_dplus_passive_passive_shadow_runner.py"
FORBIDDEN_DIR_NAMES = {"raw", "replay", "replay_published"}
FORBIDDEN_PATH_FRAGMENTS = ("/mnt/poly-replay",)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}
    return value if isinstance(value, dict) else {"_read_error": "json_not_object"}


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def as_int(value: Any, default: int = 0) -> int:
    return int(as_float(value, default))


def path_is_under(path: Path, root: Path) -> bool:
    resolved = path.resolve()
    resolved_root = root.resolve()
    return resolved == resolved_root or resolved_root in resolved.parents


def path_is_safe(path: Path, artifacts_root: Path) -> bool:
    resolved = path.resolve()
    if not path_is_under(resolved, artifacts_root):
        return False
    parts = {part.lower() for part in resolved.parts}
    text = str(resolved)
    return not (parts & FORBIDDEN_DIR_NAMES) and not any(
        fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS
    )


def path_is_fixture_or_smoke(path: Path) -> bool:
    return any(
        part.lower() == "fixtures"
        or "fixture" in part.lower()
        or "_smoke_" in part.lower()
        for part in path.resolve().parts
    )


def market_scope_ok(markets: Any, prefix: str) -> bool:
    if not isinstance(markets, list) or not markets:
        return False
    for market in markets:
        if isinstance(market, str):
            slug = market
        elif isinstance(market, dict):
            slug = str(market.get("POLYMARKET_MARKET_SLUG") or market.get("market_slug") or "")
        else:
            return False
        if not (slug == prefix or slug.startswith(f"{prefix}-")):
            return False
    return True


def side_effects_false(manifest: dict[str, Any]) -> bool:
    safety = manifest.get("safety") if isinstance(manifest.get("safety"), dict) else {}
    side_effects = manifest.get("side_effects") if isinstance(manifest.get("side_effects"), dict) else {}
    for key in (
        "orders_sent",
        "cancels_sent",
        "redeems_sent",
        "auth_network_started",
        "started_canary",
        "started_observer",
        "started_user_ws",
    ):
        if manifest.get(key) is True or safety.get(key) is True or side_effects.get(key) is True:
            return False
    return safety.get("orders_sent") is False


def classify_report(path: Path, artifacts_root: Path, market_prefix: str) -> dict[str, Any]:
    run_dir = path.parent
    manifest_path = run_dir / "manifest.json"
    aggregate = read_json(path)
    manifest = read_json(manifest_path)
    metrics = aggregate.get("metrics") if isinstance(aggregate.get("metrics"), dict) else {}

    failures: list[str] = []
    if not path_is_safe(path, artifacts_root) or not path_is_safe(manifest_path, artifacts_root):
        failures.append("unsafe_path")
    if not manifest_path.exists():
        failures.append("missing_runner_manifest")
    if aggregate.get("_read_error"):
        failures.append("aggregate_read_error")
    if manifest.get("_read_error"):
        failures.append("manifest_read_error")
    if aggregate.get("kind") != "aggregate_report":
        failures.append("aggregate_kind_mismatch")
    if aggregate.get("script") != RUNNER_SCRIPT:
        failures.append("aggregate_script_mismatch")
    if manifest.get("script") != RUNNER_SCRIPT:
        failures.append("manifest_script_mismatch")
    if not side_effects_false(manifest):
        failures.append("runner_manifest_side_effects_not_false")
    if not market_scope_ok(manifest.get("markets"), market_prefix):
        failures.append("market_scope_mismatch")

    required_metric_keys = (
        "candidates",
        "queue_supported_fills",
        "pair_actions",
        "filled_qty",
        "filled_cost",
        "pair_qty",
        "pair_pnl",
        "roi_on_filled_cost",
        "residual_qty",
        "residual_cost",
        "material_residual_lots",
    )
    missing_metrics = [key for key in required_metric_keys if key not in metrics]
    if missing_metrics:
        failures.append("missing_required_trading_metrics")

    summary_metrics = {
        "markets": as_int(aggregate.get("slugs")),
        "candidates": as_int(metrics.get("candidates")),
        "queue_supported_fills": as_int(metrics.get("queue_supported_fills")),
        "pair_actions": as_int(metrics.get("pair_actions")),
        "filled_qty": as_float(metrics.get("filled_qty")),
        "pair_qty": as_float(metrics.get("pair_qty")),
        "pair_pnl": as_float(metrics.get("pair_pnl")),
        "roi_on_filled_cost": as_float(metrics.get("roi_on_filled_cost")),
        "residual_qty": as_float(metrics.get("residual_qty")),
        "residual_cost": as_float(metrics.get("residual_cost")),
        "material_residual_lots": as_int(metrics.get("material_residual_lots")),
    }
    return {
        "run_dir": str(run_dir),
        "aggregate_report": str(path),
        "runner_manifest": str(manifest_path),
        "fixture_or_smoke": path_is_fixture_or_smoke(path),
        "valid_shape": not failures,
        "failures": failures,
        "trading_metrics": summary_metrics,
    }


def iter_aggregate_reports(scan_root: Path) -> list[Path]:
    paths: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(scan_root):
        dirnames[:] = [item for item in dirnames if item.lower() not in FORBIDDEN_DIR_NAMES]
        if "aggregate_report.json" in filenames:
            paths.append(Path(dirpath) / "aggregate_report.json")
    return sorted(paths)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scan-root", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--include-fixtures", action="store_true")
    parser.add_argument("--duration-s", type=int, default=1800)
    parser.add_argument("--round-offsets", default="0,1,2,3")
    parser.add_argument("--edge", type=float, default=0.04)
    parser.add_argument("--queue-share", type=float, default=0.50)
    parser.add_argument("--target-qty", type=float, default=5.0)
    parser.add_argument("--fill-haircut", type=float, default=0.25)
    parser.add_argument("--max-open-cost", type=float, default=80.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    artifacts_root = (root / "xuan_research_artifacts").resolve()
    scan_root = (args.scan_root or artifacts_root).resolve()
    label = utc_label()
    out_dir = args.output_dir or artifacts_root / f"xuan_b27_dplus_shadow_trading_report_discovery_{label}"

    scan_root_safe = path_is_under(scan_root, artifacts_root) and path_is_safe(scan_root, artifacts_root)
    report_paths = iter_aggregate_reports(scan_root) if scan_root_safe else []
    candidates = [classify_report(path, artifacts_root, args.market_prefix) for path in report_paths]
    fixture_candidates = [item for item in candidates if item["fixture_or_smoke"]]
    real_candidates = [
        item for item in candidates if args.include_fixtures or not item["fixture_or_smoke"]
    ]
    valid_real_candidates = [item for item in real_candidates if item["valid_shape"]]

    if not scan_root_safe:
        status = FAIL_UNSAFE_SCAN_ROOT
    elif valid_real_candidates:
        status = READY_STATUS
    elif real_candidates:
        status = BLOCKED_INVALID_REPORTS_STATUS
    else:
        status = BLOCKED_NO_REPORTS_STATUS

    proposed_output_dir = (
        artifacts_root / f"xuan_b27_dplus_passive_passive_shadow_run_{label}"
    )
    runner_argv = [
        "python3",
        "tools/xuan_dplus_passive_passive_shadow_runner.py",
        "--repo",
        "/srv/pm_as_ofi/repo",
        "--shared-ingress-root",
        "/srv/pm_as_ofi/shared-ingress-main",
        "--prefix",
        args.market_prefix,
        "--round-offsets",
        args.round_offsets,
        "--duration-s",
        str(args.duration_s),
        "--output-dir",
        str(proposed_output_dir),
        "--edge",
        str(args.edge),
        "--queue-share",
        str(args.queue_share),
        "--target-qty",
        str(args.target_qty),
        "--fill-haircut",
        str(args.fill_haircut),
        "--max-open-cost",
        str(args.max_open_cost),
    ]
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shadow_trading_report_discovery",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_shadow_trading_report_discovery",
        "scan_root": str(scan_root),
        "scan_root_safe": scan_root_safe,
        "include_fixtures": args.include_fixtures,
        "candidate_count": len(candidates),
        "fixture_or_smoke_candidate_count": len(fixture_candidates),
        "real_candidate_count": len(real_candidates),
        "valid_real_candidate_count": len(valid_real_candidates),
        "best_candidate": valid_real_candidates[0] if valid_real_candidates else None,
        "candidate_summaries": candidates[:50],
        "exact_shadow_run_plan": {
            "status": "PLAN_ONLY_NOT_EXECUTED",
            "requires_current_exact_main_thread_approval": True,
            "heartbeat_or_generic_approval_is_not_enough": True,
            "runner": "tools/xuan_dplus_passive_passive_shadow_runner.py",
            "argv": runner_argv,
            "env_contract": {
                "PM_DRY_RUN": "true",
                "PM_SHARED_INGRESS_ROLE": "client",
                "PM_INSTANCE_ID": "xuan-research-dplus-shadow",
            },
            "will_connect_to_shared_ingress_market_socket_if_executed": True,
            "will_send_orders_if_executed": False,
            "expected_outputs": [
                "manifest.json",
                "aggregate_report.json",
                "per-market shadow runner artifacts",
            ],
            "post_run_acceptance_command": [
                "python3",
                "scripts/xuan_b27_dplus_shadow_trading_acceptance.py",
                "--shadow-run-dir",
                str(proposed_output_dir),
            ],
        },
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_observer": False,
        "started_user_ws": False,
        "started_canary": False,
        "side_effects": {
            "shadow_runner_started": False,
            "shared_ingress_connected": False,
            "network_started": False,
            "raw_replay_scanned": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "started_canary": False,
        },
        "next_gate": (
            "run local shadow trading acceptance on the discovered aggregate report"
            if status == READY_STATUS
            else "obtain exact approval for the planned no-order D+ passive/passive shadow runner or locate an existing real aggregate_report.json"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status == READY_STATUS else 1


if __name__ == "__main__":
    raise SystemExit(main())
