#!/usr/bin/env python3
"""Compare two local no-order pair-arb backtest JSONL grids for OOS stability."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_pair_arb_oos_compare"
PNL_EPS = 1e-9
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
)
REQUIRED_DECLARATION_FIELDS = (
    "data_root",
    "dataset_type",
    "labels",
    "days",
    "market_prefix",
    "assets",
    "row_count",
    "excluded_20260514_20260515",
    "includes_public_account_execution_truth_v1",
)
CONFIG_FIELDS = (
    "max_net_diff",
    "pair_target",
    "bid_size",
    "tier_1_mult",
    "tier_2_mult",
    "tier_mode",
    "fill_model",
    "risk_open_cutoff_secs",
    "pair_cost_safety_margin",
    "salvage_net_cap",
    "salvage_start_remaining_secs",
    "taker_fee_rate",
    "directional_risk_filter_bps",
    "directional_entry_min_bps",
    "directional_price_source",
    "reject_stale",
    "require_ws_fresh",
    "max_quote_age_secs",
    "min_ask_depth",
    "entry_pair_max_ask_sum",
    "require_two_sided_entry",
    "pairing_only_when_residual",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def unique_output_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for idx in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{idx:03d}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"failed to allocate unique output dir under {path.parent}")


def path_is_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}


def companion_manifest(jsonl_path: Path) -> Path:
    return jsonl_path.parent / "manifest.json"


def declaration_for_jsonl(jsonl_path: Path) -> dict[str, Any]:
    manifest_path = companion_manifest(jsonl_path)
    manifest = read_json(manifest_path)
    declaration = manifest.get("backtest_data_declaration")
    if not isinstance(declaration, dict):
        declaration = {}
    missing_fields = [
        field
        for field in REQUIRED_DECLARATION_FIELDS
        if field not in declaration
    ]
    out = dict(declaration)
    out["source_manifest"] = str(manifest_path)
    out["source_manifest_exists"] = manifest_path.exists()
    out["required_declaration_fields_present"] = not missing_fields
    out["missing_declaration_fields"] = missing_fields
    out["can_support_strategy_promotion"] = (
        out.get("can_support_strategy_promotion") is True
        and not missing_fields
    )
    if missing_fields:
        out.setdefault("status", "MISSING_BACKTEST_DATA_DECLARATION")
        out.setdefault(
            "reporting_rule",
            "Conclusion scope is local research only until a complete backtest data declaration is present.",
        )
    return out


def combined_data_declaration(
    first_jsonl: Path,
    second_jsonl: Path,
) -> dict[str, Any]:
    first = declaration_for_jsonl(first_jsonl)
    second = declaration_for_jsonl(second_jsonl)
    compatible = (
        first.get("dataset_type") == second.get("dataset_type")
        and first.get("market_prefix") == second.get("market_prefix")
        and first.get("assets") == second.get("assets")
        and first.get("excluded_20260514_20260515") is True
        and second.get("excluded_20260514_20260515") is True
    )
    promotion_eligible = (
        compatible
        and first.get("can_support_strategy_promotion") is True
        and second.get("can_support_strategy_promotion") is True
    )
    days = sorted(set(first.get("days") or []) | set(second.get("days") or []))
    return {
        "data_root": [first.get("data_root"), second.get("data_root")],
        "dataset_type": first.get("dataset_type") if compatible else "MIXED_OR_UNKNOWN",
        "labels": sorted(set(first.get("labels") or []) | set(second.get("labels") or [])),
        "days": days,
        "market_prefix": first.get("market_prefix") if compatible else None,
        "assets": first.get("assets") if compatible else [],
        "row_count": (first.get("row_count") or 0) + (second.get("row_count") or 0),
        "excluded_20260514_20260515": (
            first.get("excluded_20260514_20260515") is True
            and second.get("excluded_20260514_20260515") is True
        ),
        "contains_20260514_20260515": (
            first.get("contains_20260514_20260515") is True
            or second.get("contains_20260514_20260515") is True
        ),
        "contains_20260518": (
            first.get("contains_20260518") is True
            or second.get("contains_20260518") is True
        ),
        "includes_public_account_execution_truth_v1": (
            first.get("includes_public_account_execution_truth_v1") is True
            or second.get("includes_public_account_execution_truth_v1") is True
        ),
        "input_declarations": [first, second],
        "input_declarations_compatible": compatible,
        "can_support_strategy_promotion": promotion_eligible,
        "dataset_scope_blocks_strategy_promotion": not promotion_eligible,
        "conclusion_scope": (
            "Combined OOS compare is research-only unless every input declaration is promotion-eligible."
        ),
        "reporting_rule": (
            "Numeric OOS results are research-only unless all input declarations are complete "
            "and can_support_strategy_promotion=true."
        ),
    }


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text().splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_no}: invalid json: {exc}") from exc
        if isinstance(value, dict) and value.get("kind") == "pair_arb_backtest_run":
            rows.append(value)
    return rows


def config_key(row: dict[str, Any]) -> tuple[Any, ...]:
    cfg = row.get("config") or {}
    return tuple(cfg.get(field) for field in CONFIG_FIELDS)


def metrics(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("metrics") or {}
    return value if isinstance(value, dict) else {}


def metric(row: dict[str, Any], name: str) -> float:
    try:
        return float(metrics(row).get(name) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def summarize(row: dict[str, Any]) -> dict[str, Any]:
    cfg = row.get("config") or {}
    m = metrics(row)
    return {
        "run": row.get("run"),
        "net": cfg.get("max_net_diff"),
        "target": cfg.get("pair_target"),
        "bid": cfg.get("bid_size"),
        "tier": f"{cfg.get('tier_1_mult')}/{cfg.get('tier_2_mult')}",
        "mode": cfg.get("tier_mode"),
        "fill": cfg.get("fill_model"),
        "cutoff": cfg.get("risk_open_cutoff_secs"),
        "margin": cfg.get("pair_cost_safety_margin"),
        "salvage_cap": cfg.get("salvage_net_cap"),
        "salvage_start": cfg.get("salvage_start_remaining_secs"),
        "taker_fee": cfg.get("taker_fee_rate"),
        "directional_risk_filter_bps": cfg.get("directional_risk_filter_bps"),
        "directional_entry_min_bps": cfg.get("directional_entry_min_bps"),
        "directional_price_source": cfg.get("directional_price_source"),
        "reject_stale": cfg.get("reject_stale"),
        "require_ws_fresh": cfg.get("require_ws_fresh"),
        "max_quote_age_secs": cfg.get("max_quote_age_secs"),
        "min_ask_depth": cfg.get("min_ask_depth"),
        "entry_pair_max_ask_sum": cfg.get("entry_pair_max_ask_sum"),
        "require_two_sided_entry": cfg.get("require_two_sided_entry"),
        "pairing_only_when_residual": cfg.get("pairing_only_when_residual"),
        "windows": m.get("windows"),
        "filled_windows": m.get("filled_windows"),
        "paired_windows": m.get("paired_windows"),
        "fills": m.get("fills"),
        "paired_qty": m.get("paired_qty"),
        "paired_pnl": m.get("paired_pnl"),
        "residual_qty": m.get("residual_qty"),
        "residual_cost": m.get("residual_cost"),
        "residual_pnl": m.get("residual_pnl"),
        "pnl": m.get("total_pnl"),
        "avg": m.get("avg_pnl_per_window"),
        "weighted_avg_pair_cost": m.get("weighted_avg_pair_cost"),
        "fill_window_rate": m.get("fill_window_rate"),
        "paired_window_rate": m.get("paired_window_rate"),
        "residual_window_rate": m.get("residual_window_rate"),
        "residual_loss_rate": m.get("residual_loss_rate"),
        "avg_win": m.get("avg_win"),
        "avg_loss": m.get("avg_loss"),
    }


def pair_summary(first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    first_pnl = metric(first, "total_pnl")
    second_pnl = metric(second, "total_pnl")
    first_residual = metric(first, "residual_loss_rate")
    second_residual = metric(second, "residual_loss_rate")
    first_residual_window = metric(first, "residual_window_rate")
    second_residual_window = metric(second, "residual_window_rate")
    first_pair_cost = metric(first, "weighted_avg_pair_cost")
    second_pair_cost = metric(second, "weighted_avg_pair_cost")
    first_avg_residual_cost = metric(first, "avg_residual_cost_per_window")
    second_avg_residual_cost = metric(second, "avg_residual_cost_per_window")
    return {
        "min_pnl": min(first_pnl, second_pnl),
        "sum_pnl": first_pnl + second_pnl,
        "first": summarize(first),
        "second": summarize(second),
        "max_residual_loss_rate": max(first_residual, second_residual),
        "max_residual_window_rate": max(first_residual_window, second_residual_window),
        "max_weighted_avg_pair_cost": max(first_pair_cost, second_pair_cost),
        "max_avg_residual_cost_per_window": max(
            first_avg_residual_cost,
            second_avg_residual_cost,
        ),
        "sum_residual_qty": metric(first, "residual_qty") + metric(second, "residual_qty"),
        "sum_paired_qty": metric(first, "paired_qty") + metric(second, "paired_qty"),
    }


def qualifies(summary: dict[str, Any], args: argparse.Namespace) -> bool:
    first = summary["first"]
    second = summary["second"]
    return (
        float(first.get("pnl") or 0.0) >= args.min_pnl_per_split
        and float(second.get("pnl") or 0.0) >= args.min_pnl_per_split
        and float(first.get("filled_windows") or 0.0) >= args.min_filled_windows_per_split
        and float(second.get("filled_windows") or 0.0) >= args.min_filled_windows_per_split
        and float(first.get("paired_qty") or 0.0) >= args.min_paired_qty_per_split
        and float(second.get("paired_qty") or 0.0) >= args.min_paired_qty_per_split
        and float(summary.get("max_residual_loss_rate") or 0.0)
        <= args.max_residual_loss_rate
        and float(summary.get("max_residual_window_rate") or 0.0)
        <= args.max_residual_window_rate
        and float(summary.get("max_weighted_avg_pair_cost") or 0.0)
        <= args.max_weighted_avg_pair_cost
        and float(summary.get("max_avg_residual_cost_per_window") or 0.0)
        <= args.max_avg_residual_cost_per_window
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--first-jsonl", required=True, type=Path)
    parser.add_argument("--second-jsonl", required=True, type=Path)
    parser.add_argument("--first-label", default="first_split")
    parser.add_argument("--second-label", default="second_split")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--min-pnl-per-split", type=float, default=0.05)
    parser.add_argument("--min-filled-windows-per-split", type=float, default=50.0)
    parser.add_argument("--min-paired-qty-per-split", type=float, default=1.0)
    parser.add_argument("--max-residual-loss-rate", type=float, default=0.70)
    parser.add_argument("--max-residual-window-rate", type=float, default=1.0)
    parser.add_argument("--max-weighted-avg-pair-cost", type=float, default=1.0)
    parser.add_argument("--max-avg-residual-cost-per-window", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    out_dir = args.output_dir or unique_output_dir(
        root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    first_safe = path_is_safe(args.first_jsonl)
    second_safe = path_is_safe(args.second_jsonl)
    first_rows = load_jsonl(args.first_jsonl) if first_safe and args.first_jsonl.exists() else []
    second_rows = load_jsonl(args.second_jsonl) if second_safe and args.second_jsonl.exists() else []
    data_declaration = combined_data_declaration(args.first_jsonl, args.second_jsonl)
    first_by_key = {config_key(row): row for row in first_rows}
    second_by_key = {config_key(row): row for row in second_rows}
    matched_keys = sorted(set(first_by_key) & set(second_by_key), key=repr)

    both_positive: list[dict[str, Any]] = []
    qualified_positive: list[dict[str, Any]] = []
    best_min_candidates: list[dict[str, Any]] = []
    for key in matched_keys:
        first = first_by_key[key]
        second = second_by_key[key]
        summary = pair_summary(first, second)
        best_min_candidates.append(summary)
        if metric(first, "total_pnl") > PNL_EPS and metric(second, "total_pnl") > PNL_EPS:
            both_positive.append(summary)
            if qualifies(summary, args):
                qualified_positive.append(summary)

    both_positive.sort(key=lambda row: (row["min_pnl"], row["sum_pnl"]), reverse=True)
    qualified_positive.sort(key=lambda row: (row["min_pnl"], row["sum_pnl"]), reverse=True)
    best_min_candidates.sort(key=lambda row: (row["min_pnl"], row["sum_pnl"]), reverse=True)

    numeric_oos_passed = bool(qualified_positive)
    promotion_eligible = data_declaration.get("can_support_strategy_promotion") is True
    if not first_safe or not second_safe:
        status = "REFUSED_UNSAFE_INPUT_PATH"
    elif not first_rows or not second_rows:
        status = "FAIL_MISSING_BACKTEST_ROWS"
    elif qualified_positive and promotion_eligible:
        status = "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS"
    elif qualified_positive:
        status = "PASS_OOS_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
    elif both_positive:
        status = "FAIL_OOS_POSITIVE_CONFIGS_BELOW_SAMPLE_OR_SIZE"
    else:
        status = "FAIL_NO_CONFIG_POSITIVE_IN_BOTH_SPLITS"

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "status": status,
        "created_utc": utc_label(),
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_pair_arb_oos_compare",
        "first_label": args.first_label,
        "second_label": args.second_label,
        "backtest_data_declaration": data_declaration,
        "data_root": data_declaration.get("data_root"),
        "dataset_type": data_declaration.get("dataset_type"),
        "labels": data_declaration.get("labels"),
        "days": data_declaration.get("days"),
        "market_prefix": data_declaration.get("market_prefix"),
        "assets": data_declaration.get("assets"),
        "row_count": data_declaration.get("row_count"),
        "excluded_20260514_20260515": data_declaration.get("excluded_20260514_20260515"),
        "contains_20260514_20260515": data_declaration.get("contains_20260514_20260515"),
        "contains_20260518": data_declaration.get("contains_20260518"),
        "includes_public_account_execution_truth_v1": data_declaration.get(
            "includes_public_account_execution_truth_v1"
        ),
        "can_support_strategy_promotion": data_declaration.get(
            "can_support_strategy_promotion"
        ),
        "dataset_scope_blocks_strategy_promotion": data_declaration.get(
            "dataset_scope_blocks_strategy_promotion"
        ),
        "conclusion_scope": data_declaration.get("conclusion_scope"),
        "reporting_rule": data_declaration.get("reporting_rule"),
        "first_jsonl": str(args.first_jsonl),
        "second_jsonl": str(args.second_jsonl),
        "input_paths_safe": first_safe and second_safe,
        "first_run_count": len(first_rows),
        "second_run_count": len(second_rows),
        "matched_config_count": len(matched_keys),
        "qualification_thresholds": {
            "min_pnl_per_split": args.min_pnl_per_split,
            "min_filled_windows_per_split": args.min_filled_windows_per_split,
            "min_paired_qty_per_split": args.min_paired_qty_per_split,
            "max_residual_loss_rate": args.max_residual_loss_rate,
            "max_residual_window_rate": args.max_residual_window_rate,
            "max_weighted_avg_pair_cost": args.max_weighted_avg_pair_cost,
            "max_avg_residual_cost_per_window": args.max_avg_residual_cost_per_window,
        },
        "first_positive_nonzero_run_count": sum(
            1 for row in first_rows if metric(row, "fills") > 0 and metric(row, "total_pnl") > PNL_EPS
        ),
        "second_positive_nonzero_run_count": sum(
            1 for row in second_rows if metric(row, "fills") > 0 and metric(row, "total_pnl") > PNL_EPS
        ),
        "both_positive_config_count": len(both_positive),
        "qualified_both_positive_config_count": len(qualified_positive),
        "best_qualified_both_positive_config": (
            qualified_positive[0] if qualified_positive else None
        ),
        "top_qualified_both_positive_configs": qualified_positive[: args.top],
        "best_both_positive_config": both_positive[0] if both_positive else None,
        "top_both_positive_configs": both_positive[: args.top],
        "best_by_min_pnl": best_min_candidates[0] if best_min_candidates else None,
        "top_by_min_pnl": best_min_candidates[: args.top],
        "numeric_oos_validation_passed": numeric_oos_passed,
        "oos_validation_passed": bool(qualified_positive and promotion_eligible),
        "requires_strategy_retrain_or_positive_oos_config": not bool(
            qualified_positive and promotion_eligible
        ),
        "requires_compliant_backtest_dataset_for_promotion": (
            bool(qualified_positive) and not promotion_eligible
        ),
        "raw_replay_scanned": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "network_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "env_written": False,
            "live_path_modified": False,
            "raw_replay_scanned": False,
        },
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    print(out_dir / "manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
