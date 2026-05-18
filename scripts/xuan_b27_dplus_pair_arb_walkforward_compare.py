#!/usr/bin/env python3
"""Compare local no-order pair-arb backtest JSONL grids across time slices."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_pair_arb_walkforward_compare"
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


def combined_data_declaration(paths: list[Path]) -> dict[str, Any]:
    declarations = [declaration_for_jsonl(path) for path in paths]
    dataset_types = {declaration.get("dataset_type") for declaration in declarations}
    market_prefixes = {declaration.get("market_prefix") for declaration in declarations}
    assets_values = {json.dumps(declaration.get("assets") or []) for declaration in declarations}
    compatible = (
        len(dataset_types) == 1
        and len(market_prefixes) == 1
        and len(assets_values) == 1
        and all(declaration.get("excluded_20260514_20260515") is True for declaration in declarations)
    )
    promotion_eligible = compatible and all(
        declaration.get("can_support_strategy_promotion") is True
        for declaration in declarations
    )
    days = sorted(
        {
            day
            for declaration in declarations
            for day in (declaration.get("days") or [])
        }
    )
    labels = sorted(
        {
            label
            for declaration in declarations
            for label in (declaration.get("labels") or [])
        }
    )
    return {
        "data_root": [declaration.get("data_root") for declaration in declarations],
        "dataset_type": next(iter(dataset_types)) if compatible and dataset_types else "MIXED_OR_UNKNOWN",
        "labels": labels,
        "days": days,
        "market_prefix": next(iter(market_prefixes)) if compatible and market_prefixes else None,
        "assets": declarations[0].get("assets") if compatible and declarations else [],
        "row_count": sum(int(declaration.get("row_count") or 0) for declaration in declarations),
        "excluded_20260514_20260515": all(
            declaration.get("excluded_20260514_20260515") is True
            for declaration in declarations
        ),
        "contains_20260514_20260515": any(
            declaration.get("contains_20260514_20260515") is True
            for declaration in declarations
        ),
        "contains_20260518": any(
            declaration.get("contains_20260518") is True
            for declaration in declarations
        ),
        "includes_public_account_execution_truth_v1": any(
            declaration.get("includes_public_account_execution_truth_v1") is True
            for declaration in declarations
        ),
        "input_declarations": declarations,
        "input_declarations_compatible": compatible,
        "can_support_strategy_promotion": promotion_eligible,
        "dataset_scope_blocks_strategy_promotion": not promotion_eligible,
        "conclusion_scope": (
            "Combined walk-forward compare is research-only unless every input declaration is promotion-eligible."
        ),
        "reporting_rule": (
            "Numeric walk-forward results are research-only unless all input declarations are complete "
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


def parse_split(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("split must be label=/path/to/backtest_stdout.jsonl")
    label, path_text = value.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError("split label must be non-empty")
    return label, Path(path_text)


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
        "avg_residual_cost_per_window": m.get("avg_residual_cost_per_window"),
        "avg_win": m.get("avg_win"),
        "avg_loss": m.get("avg_loss"),
    }


def split_index(rows: list[dict[str, Any]]) -> tuple[dict[tuple[Any, ...], dict[str, Any]], int]:
    indexed: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicate_count = 0
    for row in rows:
        key = config_key(row)
        if key in indexed:
            duplicate_count += 1
        indexed[key] = row
    return indexed, duplicate_count


def walk_summary(labels: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    per_split = {
        label: summarize(row)
        for label, row in zip(labels, rows)
    }
    pnls = [metric(row, "total_pnl") for row in rows]
    filled_windows = [metric(row, "filled_windows") for row in rows]
    paired_qty = [metric(row, "paired_qty") for row in rows]
    residual_loss_rates = [metric(row, "residual_loss_rate") for row in rows]
    residual_window_rates = [metric(row, "residual_window_rate") for row in rows]
    pair_costs = [metric(row, "weighted_avg_pair_cost") for row in rows]
    residual_costs = [metric(row, "avg_residual_cost_per_window") for row in rows]
    return {
        "min_pnl": min(pnls),
        "sum_pnl": sum(pnls),
        "min_filled_windows": min(filled_windows),
        "sum_filled_windows": sum(filled_windows),
        "min_paired_qty": min(paired_qty),
        "sum_paired_qty": sum(paired_qty),
        "sum_residual_qty": sum(metric(row, "residual_qty") for row in rows),
        "sum_residual_cost": sum(metric(row, "residual_cost") for row in rows),
        "max_residual_loss_rate": max(residual_loss_rates),
        "max_residual_window_rate": max(residual_window_rates),
        "max_weighted_avg_pair_cost": max(pair_costs),
        "max_avg_residual_cost_per_window": max(residual_costs),
        "per_split": per_split,
    }


def qualifies(summary: dict[str, Any], args: argparse.Namespace) -> bool:
    return (
        float(summary.get("min_pnl") or 0.0) >= args.min_pnl_per_split
        and float(summary.get("min_filled_windows") or 0.0)
        >= args.min_filled_windows_per_split
        and float(summary.get("min_paired_qty") or 0.0) >= args.min_paired_qty_per_split
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
    parser.add_argument(
        "--split",
        action="append",
        required=True,
        type=parse_split,
        help="Time split in label=/path/to/backtest_stdout.jsonl form. Repeat 3+ times.",
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--min-pnl-per-split", type=float, default=0.02)
    parser.add_argument("--min-filled-windows-per-split", type=float, default=20.0)
    parser.add_argument("--min-paired-qty-per-split", type=float, default=0.5)
    parser.add_argument("--max-residual-loss-rate", type=float, default=0.70)
    parser.add_argument("--max-residual-window-rate", type=float, default=0.10)
    parser.add_argument("--max-weighted-avg-pair-cost", type=float, default=0.70)
    parser.add_argument("--max-avg-residual-cost-per-window", type=float, default=0.03)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    out_dir = args.output_dir or unique_output_dir(
        root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    labels = [label for label, _ in args.split]
    paths = [path for _, path in args.split]
    unique_labels = len(set(labels)) == len(labels)
    input_paths_safe = all(path_is_safe(path) for path in paths)
    all_paths_exist = all(path.exists() for path in paths)
    split_count_ok = len(paths) >= 3

    split_rows: list[list[dict[str, Any]]] = []
    split_indexes: list[dict[tuple[Any, ...], dict[str, Any]]] = []
    duplicate_counts: dict[str, int] = {}
    data_declaration = combined_data_declaration(paths)
    if input_paths_safe and all_paths_exist and split_count_ok and unique_labels:
        for label, path in args.split:
            rows = load_jsonl(path)
            indexed, duplicate_count = split_index(rows)
            split_rows.append(rows)
            split_indexes.append(indexed)
            duplicate_counts[label] = duplicate_count

    matched_keys: set[tuple[Any, ...]] = set()
    if split_indexes:
        matched_keys = set(split_indexes[0])
        for indexed in split_indexes[1:]:
            matched_keys &= set(indexed)

    all_positive: list[dict[str, Any]] = []
    qualified_positive: list[dict[str, Any]] = []
    best_min_candidates: list[dict[str, Any]] = []
    for key in sorted(matched_keys, key=repr):
        rows = [indexed[key] for indexed in split_indexes]
        summary = walk_summary(labels, rows)
        best_min_candidates.append(summary)
        if all(metric(row, "total_pnl") > PNL_EPS for row in rows):
            all_positive.append(summary)
            if qualifies(summary, args):
                qualified_positive.append(summary)

    all_positive.sort(key=lambda row: (row["min_pnl"], row["sum_pnl"]), reverse=True)
    qualified_positive.sort(
        key=lambda row: (row["min_pnl"], row["sum_pnl"]),
        reverse=True,
    )
    best_min_candidates.sort(
        key=lambda row: (row["min_pnl"], row["sum_pnl"]),
        reverse=True,
    )

    numeric_walkforward_passed = bool(qualified_positive)
    promotion_eligible = data_declaration.get("can_support_strategy_promotion") is True
    if not split_count_ok:
        status = "FAIL_TOO_FEW_SPLITS"
    elif not unique_labels:
        status = "FAIL_DUPLICATE_SPLIT_LABELS"
    elif not input_paths_safe:
        status = "REFUSED_UNSAFE_INPUT_PATH"
    elif not all_paths_exist:
        status = "FAIL_MISSING_INPUT_PATH"
    elif not all(split_rows):
        status = "FAIL_MISSING_BACKTEST_ROWS"
    elif any(count > 0 for count in duplicate_counts.values()):
        status = "FAIL_DUPLICATE_CONFIG_KEYS"
    elif qualified_positive and promotion_eligible:
        status = "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS"
    elif qualified_positive:
        status = "PASS_WALKFORWARD_QUALIFIED_POSITIVE_CONFIGS_DATASET_SCOPE_LIMITED"
    elif all_positive:
        status = "FAIL_WALKFORWARD_POSITIVE_CONFIGS_BELOW_SAMPLE_OR_RISK"
    else:
        status = "FAIL_NO_CONFIG_POSITIVE_IN_ALL_SPLITS"

    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "status": status,
        "created_utc": utc_label(),
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_pair_arb_walkforward_compare",
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
        "split_count": len(paths),
        "split_count_ok": split_count_ok,
        "split_labels_unique": unique_labels,
        "input_paths_safe": input_paths_safe,
        "all_paths_exist": all_paths_exist,
        "splits": [
            {
                "label": label,
                "jsonl": str(path),
                "path_safe": path_is_safe(path),
                "exists": path.exists(),
                "run_count": len(rows) if idx < len(split_rows) else 0,
                "positive_nonzero_run_count": (
                    sum(
                        1
                        for row in rows
                        if metric(row, "fills") > 0
                        and metric(row, "total_pnl") > PNL_EPS
                    )
                    if idx < len(split_rows)
                    else 0
                ),
                "duplicate_config_count": duplicate_counts.get(label, 0),
            }
            for idx, ((label, path), rows) in enumerate(zip(args.split, split_rows))
        ],
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
        "all_positive_config_count": len(all_positive),
        "qualified_all_positive_config_count": len(qualified_positive),
        "best_qualified_all_positive_config": (
            qualified_positive[0] if qualified_positive else None
        ),
        "top_qualified_all_positive_configs": qualified_positive[: args.top],
        "best_all_positive_config": all_positive[0] if all_positive else None,
        "top_all_positive_configs": all_positive[: args.top],
        "best_by_min_pnl": best_min_candidates[0] if best_min_candidates else None,
        "top_by_min_pnl": best_min_candidates[: args.top],
        "numeric_walkforward_validation_passed": numeric_walkforward_passed,
        "walkforward_validation_passed": bool(qualified_positive and promotion_eligible),
        "requires_strategy_retrain_or_positive_walkforward_config": not bool(
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
