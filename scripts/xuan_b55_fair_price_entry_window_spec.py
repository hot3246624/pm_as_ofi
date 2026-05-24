#!/usr/bin/env python3
"""Define the b55 fair-price entry-window research contract.

This is a local-only specification artifact. It deliberately fails closed when
an explicit fair-price source is absent, because b55-style replication depends
on fair win probability rather than a simple Polymarket price cap.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_fair_price_entry_window_spec"
DEFAULT_B55_AUDIT = Path(
    "xuan_research_artifacts/"
    "xuan_public_b55_strategy_replicability_audit_20260524T050730Z/"
    "manifest.json"
)
FORBIDDEN_OUTPUT_FRAGMENTS = (
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
REQUIRED_FAIR_PRICE_FIELDS = (
    "asset",
    "market_slug",
    "condition_id",
    "market_start_ts",
    "market_end_ts",
    "quote_ts",
    "entry_delta_s",
    "outcome",
    "polymarket_price",
    "fair_probability",
    "fair_probability_uncertainty",
    "edge_after_fee_and_uncertainty",
    "boundary_price",
    "fair_spot_mid",
    "source_count",
    "source_names",
    "source_dispersion_bps",
    "max_source_age_ms",
    "model_name",
    "model_version",
)
REQUIRED_FEE_FIELDS = (
    "trade_id",
    "condition_id",
    "market_slug",
    "side",
    "outcome",
    "price",
    "size",
    "gross_usdc",
    "fee_usdc",
    "actual_usdc",
    "liquidity_role",
    "timestamp",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_OUTPUT_FRAGMENTS)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def fair_source_status(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {
            "available": False,
            "status": "MISSING_FAIR_PRICE_SOURCE_MANIFEST",
            "path": None,
            "required_fields": list(REQUIRED_FAIR_PRICE_FIELDS),
            "notes": [
                "A b55-style entry cannot be validated from Polymarket price bands alone.",
                "The source must provide fair win probability, not just spot price.",
            ],
        }
    if not path_safe(path):
        return {
            "available": False,
            "status": "UNSAFE_FAIR_PRICE_SOURCE_PATH",
            "path": str(path),
            "required_fields": list(REQUIRED_FAIR_PRICE_FIELDS),
        }
    try:
        manifest = load_json(path)
    except Exception as exc:
        return {
            "available": False,
            "status": "FAIR_PRICE_SOURCE_MANIFEST_READ_FAILED",
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "required_fields": list(REQUIRED_FAIR_PRICE_FIELDS),
        }
    declared = manifest.get("provided_fields")
    if not isinstance(declared, list):
        declared = manifest.get("schema_fields") if isinstance(manifest.get("schema_fields"), list) else []
    missing = [field for field in REQUIRED_FAIR_PRICE_FIELDS if field not in declared]
    return {
        "available": not missing,
        "status": "FAIR_PRICE_SOURCE_READY" if not missing else "FAIR_PRICE_SOURCE_FIELDS_MISSING",
        "path": str(path),
        "provided_fields": declared,
        "missing_fields": missing,
        "required_fields": list(REQUIRED_FAIR_PRICE_FIELDS),
        "source_manifest_summary": {
            "artifact": manifest.get("artifact"),
            "decision_label": manifest.get("decision_label"),
            "created_utc": manifest.get("created_utc"),
        },
    }


def extract_previous_b55(audit: dict[str, Any]) -> dict[str, Any]:
    observed = audit.get("observed_b55_24h") if isinstance(audit.get("observed_b55_24h"), dict) else {}
    ranking = audit.get("research_ranking") if isinstance(audit.get("research_ranking"), dict) else {}
    gate = ranking.get("replicability_gate") if isinstance(ranking.get("replicability_gate"), dict) else {}
    cashflow = observed.get("cashflow") if isinstance(observed.get("cashflow"), dict) else {}
    pair = observed.get("pair_metrics_lifetime") if isinstance(observed.get("pair_metrics_lifetime"), dict) else {}
    current = observed.get("current_positions") if isinstance(observed.get("current_positions"), dict) else {}
    groups = observed.get("asset_timeframe_cash_pair_residual")
    if not isinstance(groups, list):
        groups = []
    safer = gate.get("safer_first_pass_groups") if isinstance(gate.get("safer_first_pass_groups"), list) else []
    return {
        "source_decision_label": audit.get("decision_label"),
        "trades": (observed.get("row_counts") or {}).get("activity_unique_rows"),
        "buy_actual_cost": cashflow.get("buy_actual_cost"),
        "fee_rate_on_gross": cashflow.get("fee_rate_on_gross"),
        "cash_pnl": cashflow.get("cash_pnl"),
        "cash_plus_current": (observed.get("mark_to_market") or {}).get("pnl_with_current_value"),
        "actual_pair_cost": pair.get("actual_pair_cost"),
        "paired_actual_profit": pair.get("paired_actual_profit"),
        "residual_rate": pair.get("lifetime_residual_rate_on_bought_qty"),
        "per_market_pair_cost_p90": (pair.get("per_market_actual_pair_cost") or {}).get("p90"),
        "current_residual_rate": current.get("current_residual_rate_on_position_qty"),
        "safer_first_pass_groups": safer,
        "asset_timeframe_groups": groups[:8],
        "hard_blockers": gate.get("hard_blockers_before_shadow_or_deployable_claim"),
        "cautions": gate.get("cautions"),
    }


def build_contract(args: argparse.Namespace, fair_status: dict[str, Any]) -> dict[str, Any]:
    expected_edge = args.min_edge_after_fee_uncertainty
    return {
        "contract_name": "b55_fair_price_entry_window_spec_v1",
        "objective": (
            "Buy BTC/ETH short-horizon Polymarket outcomes only when independent fair win probability exceeds "
            "Polymarket price after explicit fee, uncertainty, pair-cost tail, and residual-risk buffers."
        ),
        "allowed_universe": {
            "assets": ["BTC", "ETH"],
            "timeframes": ["15m", "1h_or_named"],
            "first_pass_priority": ["BTC 1h_or_named", "ETH 1h_or_named", "BTC/ETH 15m after fair-price gate"],
            "excluded_initially": [
                "5m hard chase",
                "SOL/XRP auxiliary markets",
                "pure tail-ticket 0c-20c or 97c-100c strategy",
            ],
        },
        "entry_filter": {
            "entry_delta_s_min": -900,
            "entry_delta_s_max": -60,
            "polymarket_price_min": args.price_min,
            "polymarket_price_max": args.price_max,
            "last_60_seconds_allowed": False,
            "last_60_seconds_role": "auxiliary_diagnostic_only",
        },
        "fair_price_source_contract": {
            "required": True,
            "source_status": fair_status,
            "minimum_source_count": args.min_fair_source_count,
            "max_source_age_ms": args.max_source_age_ms,
            "max_source_dispersion_bps": args.max_source_dispersion_bps,
            "required_model_outputs": [
                "fair_probability",
                "fair_probability_uncertainty",
                "edge_after_fee_and_uncertainty",
            ],
            "required_model_inputs": [
                "boundary_price",
                "fair_spot_mid",
                "time_to_expiry_s",
                "short_horizon_volatility_or_uncertainty_proxy",
            ],
            "disallowed_substitutes": [
                "Polymarket price band alone",
                "single exchange last trade without source-age/dispersion checks",
                "RDTS or any one-point source without uncertainty buffer",
            ],
        },
        "fee_execution_contract": {
            "max_realized_fee_rate_on_gross": args.max_fee_rate_on_gross,
            "required_fields": list(REQUIRED_FEE_FIELDS),
            "allow_aggregate_fee_proxy_for_research_only": True,
            "require_per_trade_fee_and_liquidity_role_before_shadow": True,
        },
        "pair_and_residual_gates": {
            "max_average_actual_pair_cost": args.max_average_pair_cost,
            "max_market_pair_cost_for_arb_leg": 1.0,
            "max_pair_cost_p90": args.max_pair_cost_p90,
            "max_residual_rate": args.max_residual_rate,
            "max_current_residual_rate": args.max_current_residual_rate,
            "min_edge_after_fee_uncertainty_per_share": expected_edge,
            "residual_stress_rule": (
                "Expected edge from paired inventory plus fair-price surplus must cover residual mark-to-zero stress; "
                "otherwise classify as direction exposure, not arbitrage."
            ),
        },
        "candidate_row_schema": {
            "required_public_trade_fields": [
                "condition_id",
                "market_slug",
                "asset",
                "timeframe",
                "market_end_ts",
                "trade_ts",
                "entry_delta_s",
                "outcome",
                "polymarket_price",
                "size",
                "gross_usdc",
                "fee_usdc",
                "actual_usdc",
                "liquidity_role",
            ],
            "required_fair_price_fields": list(REQUIRED_FAIR_PRICE_FIELDS),
            "required_market_aggregate_fields": [
                "paired_qty",
                "actual_pair_cost",
                "pair_cost_p90_bucket",
                "residual_qty",
                "residual_rate",
                "residual_side",
                "cash_pnl",
                "residual_pnl_estimate",
            ],
        },
        "pass_fail_policy": {
            "keep_research_lead": [
                "fair-price source contract present",
                "fee/gross <= threshold",
                "average pair cost <= threshold",
                "residual <= threshold",
                "edge_after_fee_and_uncertainty positive after buffers",
            ],
            "unknown": [
                "fair-price source absent",
                "per-trade fee/liquidity role unavailable",
                "residual or pair-cost tail cannot be measured",
            ],
            "discard": [
                "strategy reduces to Polymarket price cap without independent fair probability",
                "requires static side/timeframe deletion",
                "requires failed D+ micro-deficit/ledger/tiny-deficit/closed-cycle/cooldown/fill-to-balance families",
                "requires private/deployable/canary/promotion claim from public data",
            ],
        },
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    audit_path = args.b55_audit_manifest
    audit = load_json(audit_path)
    fair_status = fair_source_status(args.fair_price_source_manifest)
    previous = extract_previous_b55(audit)
    contract = build_contract(args, fair_status)
    fair_ready = bool(fair_status.get("available"))
    per_trade_fee_ready = False
    blockers = []
    if not fair_ready:
        blockers.append("fair_price_source_missing_or_incomplete")
    if not per_trade_fee_ready:
        blockers.append("per_trade_fee_and_liquidity_role_source_missing")
    if as_float(previous.get("residual_rate")) > args.max_residual_rate:
        blockers.append("observed_b55_residual_above_replication_target")
    if as_float(previous.get("per_market_pair_cost_p90")) > args.max_pair_cost_p90:
        blockers.append("observed_b55_pair_cost_tail_above_replication_target")

    decision = "KEEP" if fair_ready and not blockers else "UNKNOWN"
    label = (
        "KEEP_B55_FAIR_PRICE_ENTRY_WINDOW_SPEC_EXECUTABLE"
        if decision == "KEEP"
        else "UNKNOWN_B55_FAIR_PRICE_ENTRY_WINDOW_SPEC_SOURCE_AND_TAIL_BLOCKED"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_fair_price_entry_window_spec",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "public_profile_export_only": True,
            "private_owner_trade_truth_used": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "dplus_failed_families_enabled_or_swept": False,
        },
        "source_inputs": {
            "b55_audit_manifest": str(audit_path),
            "fair_price_source_manifest": str(args.fair_price_source_manifest)
            if args.fair_price_source_manifest
            else None,
        },
        "previous_b55_evidence": previous,
        "spec_contract": contract,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "status": (
                "Spec is exact, but fair-price/per-trade fee sources and residual/pair-tail blockers must be resolved "
                "before any no-order diagnostic."
                if decision == "UNKNOWN"
                else "Spec has required source contract and can proceed to bounded local verifier design."
            ),
            "blockers": blockers,
            "why_this_is_not_a_price_cap": [
                "The gate requires independent fair_probability and uncertainty, not only a 35c..90c Polymarket price band.",
                "The price band only defines b55-like trade habitat; fair edge decides admissibility.",
                "Pair-cost and residual gates remain mandatory even when fair edge is positive.",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "status": "B55_FAIR_PRICE_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Build or locate a local-only fair-price source manifest for BTC/ETH 15m/1h with the required fields, "
            "then run this spec again; do not start shadow until fair-price, per-trade fee/liquidity role, residual, "
            "and pair-tail blockers are all explicit."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--b55-audit-manifest", type=Path, default=DEFAULT_B55_AUDIT)
    parser.add_argument("--fair-price-source-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--price-min", type=float, default=0.35)
    parser.add_argument("--price-max", type=float, default=0.90)
    parser.add_argument("--max-fee-rate-on-gross", type=float, default=0.012)
    parser.add_argument("--max-average-pair-cost", type=float, default=0.97)
    parser.add_argument("--max-pair-cost-p90", type=float, default=1.0)
    parser.add_argument("--max-residual-rate", type=float, default=0.10)
    parser.add_argument("--max-current-residual-rate", type=float, default=0.10)
    parser.add_argument("--min-edge-after-fee-uncertainty", type=float, default=0.02)
    parser.add_argument("--min-fair-source-count", type=int, default=3)
    parser.add_argument("--max-source-age-ms", type=int, default=1500)
    parser.add_argument("--max-source-dispersion-bps", type=float, default=8.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    manifest = build_manifest(args, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "blockers": manifest["research_ranking"]["blockers"],
                "fair_price_source_status": manifest["spec_contract"]["fair_price_source_contract"][
                    "source_status"
                ]["status"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
