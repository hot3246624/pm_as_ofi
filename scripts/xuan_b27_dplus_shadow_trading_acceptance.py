#!/usr/bin/env python3
"""Evaluate D+ shadow-trading/backtest acceptance from existing runner reports.

This gate does not run a shadow process, connect to shared-ingress, inspect
raw/replay stores, or touch order/cancel/redeem paths. It only reads a local
``xuan_dplus_passive_passive_shadow_runner.py`` output directory and turns the
existing simulated trading metrics into a canary-facing acceptance artifact.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_STATUS = "PASS_SHADOW_TRADING_ACCEPTANCE"
PROMOTION_RISK_STATUS = "FAIL_SHADOW_TRADING_PROMOTION_RISK_BUDGET"
RUNNER_SCRIPT = "xuan_dplus_passive_passive_shadow_runner.py"
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


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


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return numerator / denominator


def path_is_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def market_scope_ok(markets: list[Any], prefix: str) -> bool:
    if not markets:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shadow-run-dir", type=Path, required=True)
    parser.add_argument("--aggregate-report", type=Path)
    parser.add_argument("--runner-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--min-markets", type=int, default=1)
    parser.add_argument("--min-candidates", type=int, default=100)
    parser.add_argument("--min-queue-supported-fills", type=int, default=1)
    parser.add_argument("--min-pair-actions", type=int, default=1)
    parser.add_argument("--min-pair-qty", type=float, default=1.0)
    parser.add_argument("--min-qty-pair-share-of-filled", type=float, default=0.5)
    parser.add_argument("--min-pair-pnl", type=float, default=0.0)
    parser.add_argument("--min-roi-on-filled-cost", type=float, default=0.0)
    parser.add_argument("--max-residual-qty-share-of-filled", type=float, default=0.15)
    parser.add_argument("--max-residual-cost-share-of-filled-cost", type=float, default=0.20)
    parser.add_argument("--max-pair-tail-loss-share-of-pair-pnl", type=float, default=0.05)
    parser.add_argument("--pair-tail-loss-fraction", type=float, default=0.10)
    parser.add_argument("--max-residual-qty", type=float, default=10.0)
    parser.add_argument("--max-residual-cost", type=float, default=5.0)
    parser.add_argument("--max-material-residual-lots", type=int, default=0)
    parser.add_argument("--max-net-pair-cost-p90", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_shadow_trading_acceptance_{label}"

    shadow_run_dir = args.shadow_run_dir
    aggregate_path = args.aggregate_report or shadow_run_dir / "aggregate_report.json"
    runner_manifest_path = args.runner_manifest or shadow_run_dir / "manifest.json"
    aggregate = read_json(aggregate_path)
    runner_manifest = read_json(runner_manifest_path)
    metrics = aggregate.get("metrics") or {}
    safety = runner_manifest.get("safety") or {}

    paths_safe = path_is_safe(shadow_run_dir) and path_is_safe(aggregate_path) and path_is_safe(runner_manifest_path)
    report_present = aggregate_path.exists() and runner_manifest_path.exists()
    report_shape_ok = (
        aggregate.get("kind") == "aggregate_report"
        and aggregate.get("script") == RUNNER_SCRIPT
        and runner_manifest.get("script") == RUNNER_SCRIPT
        and "_read_error" not in aggregate
        and "_read_error" not in runner_manifest
    )
    no_order_safety_ok = (
        safety.get("orders_sent") is False
        and runner_manifest.get("orders_sent", False) is False
        and runner_manifest.get("cancels_sent", False) is False
        and runner_manifest.get("redeems_sent", False) is False
        and runner_manifest.get("started_canary", False) is False
    )
    market_ok = (
        as_int(aggregate.get("slugs")) >= args.min_markets
        and market_scope_ok(runner_manifest.get("markets") or [], args.market_prefix)
    )

    candidates = as_int(metrics.get("candidates"))
    queue_supported_fills = as_int(metrics.get("queue_supported_fills"))
    pair_actions = as_int(metrics.get("pair_actions"))
    pair_qty = as_float(metrics.get("pair_qty"))
    filled_qty = as_float(metrics.get("filled_qty"))
    filled_cost = as_float(metrics.get("filled_cost"))
    pair_pnl = as_float(metrics.get("pair_pnl"))
    taker_fee = as_float(metrics.get("taker_fee"))
    residual_qty = as_float(metrics.get("residual_qty"))
    residual_cost = as_float(metrics.get("residual_cost"))
    material_residual_lots = as_int(metrics.get("material_residual_lots"))
    qty_pair_share = as_float(metrics.get("qty_pair_share_of_filled"))
    roi_on_filled_cost = as_float(metrics.get("roi_on_filled_cost"))
    net_pair_cost_p90 = as_float(
        metrics.get("net_pair_cost_proxy_p90", metrics.get("net_pair_cost_p90")),
        default=0.0,
    )
    residual_qty_share_of_filled = safe_ratio(residual_qty, filled_qty)
    residual_cost_share_of_filled_cost = safe_ratio(residual_cost, filled_cost)
    fee_adjusted_pair_pnl_proxy = pair_pnl - taker_fee
    pair_tail_loss_proxy = max(net_pair_cost_p90 - 1.0, 0.0) * max(pair_qty, 0.0) * args.pair_tail_loss_fraction
    pair_tail_loss_share_of_pair_pnl = (
        float("inf") if pair_tail_loss_proxy > 0.0 and fee_adjusted_pair_pnl_proxy <= 0.0
        else safe_ratio(pair_tail_loss_proxy, fee_adjusted_pair_pnl_proxy)
    )
    conservative_risk_adjusted_pnl_proxy = fee_adjusted_pair_pnl_proxy - residual_cost - pair_tail_loss_proxy

    sample_metric_failures: list[dict[str, Any]] = []
    if candidates < args.min_candidates:
        sample_metric_failures.append({"metric": "candidates", "actual": candidates, "required_min": args.min_candidates})
    if queue_supported_fills < args.min_queue_supported_fills:
        sample_metric_failures.append(
            {
                "metric": "queue_supported_fills",
                "actual": queue_supported_fills,
                "required_min": args.min_queue_supported_fills,
            }
        )
    if pair_actions < args.min_pair_actions:
        sample_metric_failures.append({"metric": "pair_actions", "actual": pair_actions, "required_min": args.min_pair_actions})
    if pair_qty < args.min_pair_qty:
        sample_metric_failures.append({"metric": "pair_qty", "actual": pair_qty, "required_min": args.min_pair_qty})
    if filled_qty <= 0.0:
        sample_metric_failures.append({"metric": "filled_qty", "actual": filled_qty, "required_min_exclusive": 0.0})

    pnl_metric_failures: list[dict[str, Any]] = []
    if pair_pnl < args.min_pair_pnl:
        pnl_metric_failures.append({"metric": "pair_pnl", "actual": pair_pnl, "required_min": args.min_pair_pnl})
    if roi_on_filled_cost < args.min_roi_on_filled_cost:
        pnl_metric_failures.append(
            {"metric": "roi_on_filled_cost", "actual": roi_on_filled_cost, "required_min": args.min_roi_on_filled_cost}
        )
    if taker_fee > 0.0 and fee_adjusted_pair_pnl_proxy < args.min_pair_pnl:
        pnl_metric_failures.append(
            {
                "metric": "fee_adjusted_pair_pnl_proxy",
                "actual": round(fee_adjusted_pair_pnl_proxy, 6),
                "required_min": args.min_pair_pnl,
            }
        )

    promotion_risk_budget_failures: list[dict[str, Any]] = []
    residual_metric_failures: list[dict[str, Any]] = []
    if qty_pair_share < args.min_qty_pair_share_of_filled:
        promotion_risk_budget_failures.append(
            {
                "metric": "qty_pair_share_of_filled",
                "actual": qty_pair_share,
                "required_min": args.min_qty_pair_share_of_filled,
                "budget_type": "normalized_inventory_conversion",
            }
        )
    if residual_qty_share_of_filled > args.max_residual_qty_share_of_filled:
        failure = {
            "metric": "residual_qty_share_of_filled",
            "actual": round(residual_qty_share_of_filled, 8),
            "required_max": args.max_residual_qty_share_of_filled,
            "numerator": round(residual_qty, 6),
            "denominator": round(filled_qty, 6),
            "budget_type": "normalized_residual_qty",
        }
        promotion_risk_budget_failures.append(failure)
        residual_metric_failures.append(failure)
    if residual_cost_share_of_filled_cost > args.max_residual_cost_share_of_filled_cost:
        failure = {
            "metric": "residual_cost_share_of_filled_cost",
            "actual": round(residual_cost_share_of_filled_cost, 8),
            "required_max": args.max_residual_cost_share_of_filled_cost,
            "numerator": round(residual_cost, 6),
            "denominator": round(filled_cost, 6),
            "budget_type": "normalized_residual_cost",
        }
        promotion_risk_budget_failures.append(failure)
        residual_metric_failures.append(failure)
    if pair_tail_loss_share_of_pair_pnl > args.max_pair_tail_loss_share_of_pair_pnl:
        promotion_risk_budget_failures.append(
            {
                "metric": "pair_tail_loss_share_of_pair_pnl",
                "actual": (
                    "inf"
                    if pair_tail_loss_share_of_pair_pnl == float("inf")
                    else round(pair_tail_loss_share_of_pair_pnl, 8)
                ),
                "required_max": args.max_pair_tail_loss_share_of_pair_pnl,
                "pair_tail_loss_proxy": round(pair_tail_loss_proxy, 6),
                "fee_adjusted_pair_pnl_proxy": round(fee_adjusted_pair_pnl_proxy, 6),
                "net_pair_cost_p90": net_pair_cost_p90,
                "tail_fraction_assumption": args.pair_tail_loss_fraction,
                "budget_type": "normalized_pair_tail_loss",
            }
        )
    if material_residual_lots > args.max_material_residual_lots:
        failure = {
            "metric": "material_residual_lots",
            "actual": material_residual_lots,
            "required_max": args.max_material_residual_lots,
            "budget_type": "hard_material_lot_limit",
        }
        promotion_risk_budget_failures.append(failure)
        residual_metric_failures.append(failure)

    legacy_absolute_reference_breaches: list[dict[str, Any]] = []
    if residual_qty > args.max_residual_qty:
        legacy_absolute_reference_breaches.append(
            {"metric": "residual_qty", "actual": residual_qty, "legacy_reference_max": args.max_residual_qty}
        )
    if residual_cost > args.max_residual_cost:
        legacy_absolute_reference_breaches.append(
            {"metric": "residual_cost", "actual": residual_cost, "legacy_reference_max": args.max_residual_cost}
        )
    if net_pair_cost_p90 != 0.0 and net_pair_cost_p90 > args.max_net_pair_cost_p90:
        legacy_absolute_reference_breaches.append(
            {"metric": "net_pair_cost_p90", "actual": net_pair_cost_p90, "legacy_reference_max": args.max_net_pair_cost_p90}
        )

    sample_size_ok = not sample_metric_failures
    pnl_metrics_ok = not pnl_metric_failures
    promotion_risk_budget_ok = not promotion_risk_budget_failures
    residual_risk_ok = not residual_metric_failures

    failure_statuses: list[tuple[str, str]] = []
    if not paths_safe:
        failure_statuses.append(("unsafe_report_path", "FAIL_UNSAFE_REPORT_PATH"))
    if not report_present:
        failure_statuses.append(("missing_shadow_trading_report", "FAIL_MISSING_SHADOW_TRADING_REPORT"))
    if not report_shape_ok:
        failure_statuses.append(("shadow_trading_report_shape_failed", "FAIL_SHADOW_TRADING_REPORT_SHAPE"))
    if not no_order_safety_ok:
        failure_statuses.append(("shadow_trading_report_safety_failed", "FAIL_SHADOW_TRADING_REPORT_SAFETY"))
    if not market_ok:
        failure_statuses.append(("shadow_trading_market_scope_failed", "FAIL_SHADOW_TRADING_MARKET_SCOPE"))
    if not sample_size_ok:
        failure_statuses.append(("shadow_trading_sample_size_failed", "FAIL_SHADOW_TRADING_SAMPLE_SIZE"))
    if not pnl_metrics_ok:
        failure_statuses.append(("shadow_trading_pnl_metrics_failed", "FAIL_SHADOW_TRADING_PNL_METRICS"))
    if not promotion_risk_budget_ok:
        failure_statuses.append(("shadow_trading_promotion_risk_budget_failed", PROMOTION_RISK_STATUS))

    failures = [name for name, _ in failure_statuses]
    status = failure_statuses[0][1] if failure_statuses else PASS_STATUS

    thresholds = {
        "market_prefix": args.market_prefix,
        "min_markets": args.min_markets,
        "min_candidates": args.min_candidates,
        "min_queue_supported_fills": args.min_queue_supported_fills,
        "min_pair_actions": args.min_pair_actions,
        "min_pair_qty": args.min_pair_qty,
        "min_qty_pair_share_of_filled": args.min_qty_pair_share_of_filled,
        "min_pair_pnl": args.min_pair_pnl,
        "min_roi_on_filled_cost": args.min_roi_on_filled_cost,
        "max_residual_qty_share_of_filled": args.max_residual_qty_share_of_filled,
        "max_residual_cost_share_of_filled_cost": args.max_residual_cost_share_of_filled_cost,
        "max_pair_tail_loss_share_of_pair_pnl": args.max_pair_tail_loss_share_of_pair_pnl,
        "pair_tail_loss_fraction": args.pair_tail_loss_fraction,
        "max_residual_qty": args.max_residual_qty,
        "max_residual_cost": args.max_residual_cost,
        "max_material_residual_lots": args.max_material_residual_lots,
        "max_net_pair_cost_p90": args.max_net_pair_cost_p90,
        "promotion_risk_budget": {
            "max_residual_qty_share_of_filled": args.max_residual_qty_share_of_filled,
            "max_residual_cost_share_of_filled_cost": args.max_residual_cost_share_of_filled_cost,
            "max_pair_tail_loss_share_of_pair_pnl": args.max_pair_tail_loss_share_of_pair_pnl,
            "max_material_residual_lots": args.max_material_residual_lots,
            "min_qty_pair_share_of_filled": args.min_qty_pair_share_of_filled,
            "pair_tail_loss_fraction": args.pair_tail_loss_fraction,
        },
        "legacy_absolute_reference": {
            "max_residual_qty": args.max_residual_qty,
            "max_residual_cost": args.max_residual_cost,
            "max_net_pair_cost_p90": args.max_net_pair_cost_p90,
            "used_as_primary_promotion_gate": False,
        },
    }
    acceptance_passed = status == PASS_STATUS
    economic_pnl_positive = pnl_metrics_ok
    conservative_stress_proxy_positive = conservative_risk_adjusted_pnl_proxy >= args.min_pair_pnl
    if not report_present or not report_shape_ok or not no_order_safety_ok:
        research_label = "UNKNOWN_REPORT_OR_SAFETY_INVALID"
    elif not economic_pnl_positive:
        research_label = "DISCARD_ECONOMIC_NEGATIVE"
    elif not conservative_stress_proxy_positive:
        research_label = "TRADEOFF_ECON_POSITIVE_STRESS_PROXY_NEGATIVE"
    elif not promotion_risk_budget_ok:
        research_label = "TRADEOFF_ECON_POSITIVE_RISK_BUDGET"
    elif not sample_size_ok:
        research_label = "UNKNOWN_SAMPLE_THIN_ECON_POSITIVE"
    else:
        research_label = "KEEP_ECONOMIC_RESEARCH_ONLY"
    research_notes = [
        "research ranking uses economic proxies first; sample and risk gates are reported separately",
        "residual or pair-cost tail budget failures are TRADEOFF/BLOCKER evidence unless economic PnL turns negative",
    ]
    if conservative_risk_adjusted_pnl_proxy < 0.0:
        research_notes.append("conservative stress proxy subtracts full residual_cost and pair-tail-loss proxy; source-of-truth settlement may differ")
    proves = [
        "existing D+ passive/passive shadow runner produced simulated trading metrics",
        "virtual fills/pairing were evaluated without sending orders",
    ]
    if acceptance_passed:
        proves.append("simulated pair PnL and residual risk passed configured thresholds")
    does_not_prove = [
        "live authenticated exchange fills",
        "actual exchange queue position",
        "real wallet/cashflow PnL",
        "maker fee/rebate settlement",
    ]
    if not acceptance_passed:
        does_not_prove.insert(0, "shadow trading acceptance")
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shadow_trading_acceptance",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_shadow_trading_acceptance",
        "source_tool": f"tools/{RUNNER_SCRIPT}",
        "shadow_run_dir": str(shadow_run_dir),
        "aggregate_report": str(aggregate_path),
        "runner_manifest": str(runner_manifest_path),
        "acceptance_passed": acceptance_passed,
        "failures": failures,
        "checks": {
            "paths_safe": paths_safe,
            "report_present": report_present,
            "report_shape_ok": report_shape_ok,
            "no_order_safety_ok": no_order_safety_ok,
            "market_scope_ok": market_ok,
            "sample_size_ok": sample_size_ok,
            "pnl_metrics_ok": pnl_metrics_ok,
            "promotion_risk_budget_ok": promotion_risk_budget_ok,
            "residual_risk_ok": residual_risk_ok,
        },
        "thresholds": thresholds,
        "metric_failures": {
            "sample_size": sample_metric_failures,
            "pnl_metrics": pnl_metric_failures,
            "promotion_risk_budget": promotion_risk_budget_failures,
            "residual_risk": residual_metric_failures,
            "legacy_absolute_reference": legacy_absolute_reference_breaches,
        },
        "trading_metrics": {
            "markets": as_int(aggregate.get("slugs")),
            "candidates": candidates,
            "queue_supported_fills": queue_supported_fills,
            "fill_rate": as_float(metrics.get("fill_rate")),
            "pair_actions": pair_actions,
            "filled_qty": round(filled_qty, 6),
            "filled_cost": filled_cost,
            "pair_qty": round(pair_qty, 6),
            "qty_pair_share_of_filled": qty_pair_share,
            "pair_pnl": round(pair_pnl, 6),
            "taker_fee": round(taker_fee, 6),
            "fee_adjusted_pair_pnl_proxy": round(fee_adjusted_pair_pnl_proxy, 6),
            "roi_on_seed_cost": as_float(metrics.get("roi_on_seed_cost")),
            "roi_on_filled_cost": roi_on_filled_cost,
            "residual_qty": round(residual_qty, 6),
            "residual_cost": round(residual_cost, 6),
            "residual_qty_share_of_filled": round(residual_qty_share_of_filled, 8),
            "residual_cost_share_of_filled_cost": round(residual_cost_share_of_filled_cost, 8),
            "material_residual_lots": material_residual_lots,
            "pair_tail_loss_proxy": round(pair_tail_loss_proxy, 6),
            "pair_tail_loss_share_of_pair_pnl": (
                "inf"
                if pair_tail_loss_share_of_pair_pnl == float("inf")
                else round(pair_tail_loss_share_of_pair_pnl, 8)
            ),
            "conservative_risk_adjusted_pnl_proxy": round(conservative_risk_adjusted_pnl_proxy, 6),
            "pair_cost_p50": metrics.get("pair_cost_proxy_p50", metrics.get("pair_cost_p50")),
            "pair_cost_p90": metrics.get("pair_cost_proxy_p90", metrics.get("pair_cost_p90")),
            "net_pair_cost_p50": metrics.get("net_pair_cost_proxy_p50", metrics.get("net_pair_cost_p50")),
            "net_pair_cost_p90": metrics.get("net_pair_cost_proxy_p90", metrics.get("net_pair_cost_p90")),
            "fill_wait_p50_ms": metrics.get("fill_wait_proxy_p50_ms", metrics.get("fill_wait_p50_ms")),
            "fill_wait_p90_ms": metrics.get("fill_wait_proxy_p90_ms", metrics.get("fill_wait_p90_ms")),
            "pair_wait_p50_ms": metrics.get("pair_wait_proxy_p50_ms", metrics.get("pair_wait_p50_ms")),
            "pair_wait_p90_ms": metrics.get("pair_wait_proxy_p90_ms", metrics.get("pair_wait_p90_ms")),
        },
        "research_ranking": {
            "label": research_label,
            "economic_pnl_positive": economic_pnl_positive,
            "conservative_stress_proxy_positive": conservative_stress_proxy_positive,
            "risk_adjusted_pnl_proxy": round(conservative_risk_adjusted_pnl_proxy, 6),
            "fee_adjusted_pair_pnl_proxy": round(fee_adjusted_pair_pnl_proxy, 6),
            "pair_tail_loss_proxy": round(pair_tail_loss_proxy, 6),
            "residual_loss_proxy": round(residual_cost, 6),
            "sample_size_ok": sample_size_ok,
            "promotion_risk_budget_ok": promotion_risk_budget_ok,
            "legacy_absolute_reference_breaches": legacy_absolute_reference_breaches,
            "notes": research_notes,
        },
        "promotion_gate": {
            "passed": acceptance_passed,
            "status": status,
            "required_before_g2_canary": True,
            "hard_blockers": failures,
            "sample_size_ok": sample_size_ok,
            "economic_pnl_ok": pnl_metrics_ok,
            "promotion_risk_budget_ok": promotion_risk_budget_ok,
            "normalized_risk_budget": {
                "residual_qty_share_of_filled": round(residual_qty_share_of_filled, 8),
                "max_residual_qty_share_of_filled": args.max_residual_qty_share_of_filled,
                "residual_cost_share_of_filled_cost": round(residual_cost_share_of_filled_cost, 8),
                "max_residual_cost_share_of_filled_cost": args.max_residual_cost_share_of_filled_cost,
                "pair_tail_loss_share_of_pair_pnl": (
                    "inf"
                    if pair_tail_loss_share_of_pair_pnl == float("inf")
                    else round(pair_tail_loss_share_of_pair_pnl, 8)
                ),
                "max_pair_tail_loss_share_of_pair_pnl": args.max_pair_tail_loss_share_of_pair_pnl,
                "material_residual_lots": material_residual_lots,
                "max_material_residual_lots": args.max_material_residual_lots,
            },
            "legacy_absolute_reference": {
                "breaches": legacy_absolute_reference_breaches,
                "used_as_primary_promotion_gate": False,
            },
        },
        "report_safety": {
            "orders_sent": safety.get("orders_sent"),
            "dry_run": safety.get("dry_run"),
            "shared_ingress_role": safety.get("shared_ingress_role"),
            "shared_ingress_root": safety.get("shared_ingress_root"),
        },
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "shadow_runner_started": False,
            "network_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "started_canary": False,
            "raw_replay_scanned": False,
        },
        "evidence_interpretation": {
            "proves": proves,
            "does_not_prove": does_not_prove,
        },
        "next_gate": (
            "review effectful G2 executor and obtain explicit exact G2 canary approval"
            if acceptance_passed
            else (
                "do not promote; choose a new economic mechanism because PnL/ROI are negative"
                if not economic_pnl_positive
                else (
                    "do not promote; gather enough no-order shadow sample while preserving normalized risk budget"
                    if not sample_size_ok
                    else "do not promote; resolve normalized promotion risk budget before any G2 canary"
                )
            )
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if acceptance_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
