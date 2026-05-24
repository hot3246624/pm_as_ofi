#!/usr/bin/env python3
"""Specify a fail-closed b55/ce25 public activity entry exporter.

This is a local-only spec. It defines the exact non-trading export contract
needed before b55/ce25 fair-price research can evaluate real public entries.
It does not fetch data, start services, connect shared ingress, run shadow, or
touch live/trading paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_public_activity_entry_export_spec"
DEFAULT_GAP_AUDIT = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_entry_row_source_gap_audit_20260524T065245Z/"
    "manifest.json"
)
DEFAULT_PROBABILITY_SPEC = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_fair_probability_model_spec_20260524T055426Z/"
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
REQUIRED_ACCOUNTS = ("b55", "ce25")
REQUIRED_ASSETS = ("BTC", "ETH")
REQUIRED_TIMEFRAMES = ("15m", "1h_or_named")
PUBLIC_ACTIVITY_RAW_FIELDS = (
    "transactionHash",
    "conditionId",
    "slug",
    "eventSlug",
    "timestamp",
    "price",
    "size",
    "usdcSize",
    "outcome",
    "outcomeIndex",
    "side",
    "type",
)
REQUIRED_JOINS = (
    "public_activity_trade_rows",
    "market_metadata_boundary",
    "fee_usdc_derivation",
    "liquidity_role_truth_source",
    "fair_source_snapshot_rows",
    "volatility_rows",
    "probability_model_output",
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


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def required_candidate_fields(probability_spec: dict[str, Any]) -> list[str]:
    contract = probability_spec.get("probability_model_contract")
    if not isinstance(contract, dict):
        return []
    fields = contract.get("required_candidate_fields")
    return list(fields) if isinstance(fields, list) else []


def build_export_contract(args: argparse.Namespace, probability_spec: dict[str, Any]) -> dict[str, Any]:
    required_fields = required_candidate_fields(probability_spec)
    return {
        "contract_name": "b55_ce25_public_activity_entry_export_spec_v1",
        "objective": (
            "Create a non-trading research export that joins b55/ce25 public BUY entries to market metadata, "
            "fee/liquidity-role evidence, safe fair-source snapshots, volatility, and purchased-outcome fair "
            "probability. The export is input to research scorers only."
        ),
        "allowed_universe": {
            "accounts": list(REQUIRED_ACCOUNTS),
            "assets": list(REQUIRED_ASSETS),
            "timeframes": list(REQUIRED_TIMEFRAMES),
            "entry_delta_s_min": args.entry_delta_s_min,
            "entry_delta_s_max": args.entry_delta_s_max,
            "polymarket_price_min": args.price_min,
            "polymarket_price_max": args.price_max,
            "activity_type": "TRADE",
            "entry_side": "BUY",
            "excluded": [
                "5m hard chase",
                "pure Polymarket price cap",
                "static asset/timeframe deletion",
                "D+ micro-deficit/ledger/tiny-deficit/closed-cycle/cooldown/fill-to-balance families",
            ],
        },
        "required_candidate_fields": required_fields,
        "public_activity_trade_rows_v1": {
            "source": "Polymarket public data-api activity endpoint or already exported public activity rows",
            "raw_required_fields": list(PUBLIC_ACTIVITY_RAW_FIELDS),
            "row_filter": {
                "type": "TRADE",
                "side": "BUY",
                "accounts_required": list(REQUIRED_ACCOUNTS),
            },
            "dedup_key": [
                "account",
                "transactionHash",
                "conditionId",
                "asset",
                "side",
                "outcome",
                "price",
                "size",
                "timestamp",
                "row_index_within_transaction_hash_when_needed",
            ],
            "field_mapping": {
                "trade_id": "transactionHash plus stable row disambiguator",
                "condition_id": "conditionId",
                "market_slug": "slug if present else eventSlug",
                "quote_ts": "timestamp",
                "outcome": "outcome normalized to YES/NO or Up/Down mapping",
                "polymarket_price": "price",
                "size": "size",
                "actual_usdc": "usdcSize",
            },
            "fail_closed_if": [
                "missing raw_required_fields except eventSlug fallback",
                "not type=TRADE",
                "not side=BUY",
                "duplicate key collision remains after row disambiguator",
                "account is not b55 or ce25",
            ],
        },
        "fee_usdc_derivation_policy_v1": {
            "actual_usdc_source": "usdcSize",
            "gross_usdc_formula": "polymarket_price * size",
            "fee_usdc_formula": "actual_usdc - gross_usdc",
            "fee_per_share_formula": "fee_usdc / size",
            "required_basis_declaration": "usdcSize is actual BUY cost including fee for public activity rows",
            "negative_fee_tolerance_usdc": args.negative_fee_tolerance_usdc,
            "fail_closed_if": [
                "usdcSize basis is not declared",
                "side is not BUY",
                "size <= 0",
                "actual_usdc < gross_usdc - negative_fee_tolerance_usdc",
                "fee_usdc cannot be separated from rebate or old-position redeem effects",
            ],
        },
        "market_metadata_boundary_v1": {
            "exact_updown_slug_policy": {
                "market_start_ts": "parse final slug timestamp",
                "market_end_ts": "market_start_ts + interval for -updown-15m/-updown-1h",
            },
            "named_market_policy": {
                "required_join": "public market metadata with condition_id or slug keyed start/end timestamps",
                "no_guessing_from_title": True,
            },
            "derived_fields": ["asset", "timeframe", "entry_delta_s", "time_to_expiry_s"],
            "fail_closed_if": [
                "named 1h market has no metadata join",
                "entry_delta_s is outside allowed window",
                "asset/timeframe is not BTC/ETH 15m/1h_or_named",
            ],
        },
        "liquidity_role_truth_source_v1": {
            "accepted_sources": [
                "explicit public activity field if Polymarket exposes maker/taker role",
                "documented takerOnly field in a public export",
                "verified fill metadata with non-private role field",
            ],
            "rejected_sources": [
                "maker rebate presence as per-trade role",
                "fee magnitude guessed into maker/taker",
                "price improvement or queue behavior inference",
                "account-level takerOnly share without per-trade join",
            ],
            "required_values": ["maker", "taker", "unknown"],
            "fail_closed_if": [
                "all rows are unknown",
                "role is inferred from fee or rebate instead of sourced",
                "role field cannot be joined to trade_id/condition_id",
            ],
        },
        "fair_source_snapshot_rows_v1": {
            "source": "safe exported source tape or pre-existing local source snapshot artifact; no service start",
            "join_key": ["asset", "quote_ts nearest within tolerance"],
            "max_join_abs_delta_ms": args.max_source_join_abs_delta_ms,
            "required_fields": [
                "fair_spot_mid",
                "source_count",
                "source_names",
                "source_dispersion_bps",
                "max_source_age_ms",
                "boundary_price",
            ],
            "quality_gates": {
                "min_source_count": args.min_source_count,
                "max_source_age_ms": args.max_source_age_ms,
                "max_source_dispersion_bps": args.max_source_dispersion_bps,
            },
            "fail_closed_if": [
                "source snapshot was produced by starting local agg/shared WS for this audit",
                "source_count below gate",
                "source age/spread gates absent or failing",
                "snapshot timestamp cannot be joined to quote_ts",
            ],
        },
        "probability_model_output_v1": {
            "model_input_fields": [
                "boundary_price",
                "fair_spot_mid",
                "time_to_expiry_s",
                "volatility_lookback_s",
                "volatility_bps",
                "source quality fields",
            ],
            "output_fields": [
                "fair_probability",
                "fair_probability_uncertainty",
                "edge_after_fee_and_uncertainty",
                "model_name",
                "model_version",
            ],
            "edge_formula": "fair_probability - polymarket_price - fee_usdc / size - fair_probability_uncertainty",
            "quality_gates": {
                "max_probability_uncertainty": args.max_probability_uncertainty,
                "min_edge_after_fee_and_uncertainty": args.min_edge_after_fee_uncertainty,
            },
            "fail_closed_if": [
                "probability is a direction label rather than purchased-outcome probability",
                "uncertainty is missing",
                "model_name/model_version missing",
                "edge formula differs from contract",
            ],
        },
        "validation_policy": {
            "manifest_level_keep": [
                "all required fields listed",
                "all required joins declared",
                "fee policy and role truth source explicitly declared",
                "source snapshot tolerance and quality gates declared",
                "promotion gate remains false",
            ],
            "real_export_ready": [
                "non-fixture manifest covers b55 and ce25",
                "non-fixture manifest declares all joins",
                "non-fixture rows can be validated by a scorer",
            ],
            "unknown": [
                "liquidity role truth unavailable",
                "fair-source snapshot unavailable without service start",
                "named market metadata unavailable",
            ],
            "discard": [
                "strategy degenerates into price band/static asset-timeframe filter",
                "uses D+ failed families",
                "requires private/deployable/canary/promotion claim",
            ],
        },
    }


def candidate_export_manifest_status(path: Path | None, required_fields: list[str]) -> dict[str, Any]:
    if path is None:
        return {
            "status": "MISSING_CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST",
            "ready": False,
            "fixture": False,
            "path": None,
            "missing_fields": required_fields,
            "blockers": ["candidate_export_manifest_absent"],
        }
    if not path_safe(path):
        return {
            "status": "UNSAFE_CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_PATH",
            "ready": False,
            "fixture": False,
            "path": str(path),
            "missing_fields": required_fields,
            "blockers": ["unsafe_candidate_export_manifest_path"],
        }
    try:
        manifest = load_json(path)
    except Exception as exc:
        return {
            "status": "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_READ_FAILED",
            "ready": False,
            "fixture": False,
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "missing_fields": required_fields,
            "blockers": ["candidate_export_manifest_read_failed"],
        }

    provided = set(str(item) for item in as_list(manifest.get("provided_fields") or manifest.get("schema_fields")))
    missing = [field for field in required_fields if field not in provided]
    accounts = set(str(item) for item in as_list(manifest.get("accounts_covered")))
    joins = manifest.get("joins_declared") if isinstance(manifest.get("joins_declared"), dict) else {}
    fee_policy = manifest.get("fee_usdc_derivation_policy") if isinstance(manifest.get("fee_usdc_derivation_policy"), dict) else {}
    role_policy = manifest.get("liquidity_role_truth_source") if isinstance(manifest.get("liquidity_role_truth_source"), dict) else {}
    source_policy = manifest.get("fair_source_snapshot_policy") if isinstance(manifest.get("fair_source_snapshot_policy"), dict) else {}
    model_policy = manifest.get("probability_model_output") if isinstance(manifest.get("probability_model_output"), dict) else {}

    blockers: list[str] = []
    if missing:
        blockers.append("required_fields_missing")
    if not set(REQUIRED_ACCOUNTS).issubset(accounts):
        blockers.append("b55_and_ce25_not_both_covered")
    for join in REQUIRED_JOINS:
        if not joins.get(join):
            blockers.append(f"{join}_join_missing")
    if fee_policy.get("actual_usdc_source") != "usdcSize":
        blockers.append("actual_usdc_source_not_usdcSize")
    if fee_policy.get("gross_usdc_formula") != "polymarket_price * size":
        blockers.append("gross_usdc_formula_mismatch")
    if fee_policy.get("fee_usdc_formula") != "actual_usdc - gross_usdc":
        blockers.append("fee_usdc_formula_mismatch")
    if not bool(fee_policy.get("usdc_size_basis_declared")):
        blockers.append("usdc_size_basis_not_declared")
    if not bool(role_policy.get("per_trade_role_join_declared")):
        blockers.append("per_trade_liquidity_role_join_not_declared")
    if bool(role_policy.get("uses_rebate_or_fee_inference")):
        blockers.append("liquidity_role_uses_forbidden_rebate_or_fee_inference")
    if as_float(source_policy.get("max_join_abs_delta_ms"), 10**9) > 1500.0:
        blockers.append("source_join_tolerance_too_wide")
    if as_float(source_policy.get("min_source_count")) < 3:
        blockers.append("min_source_count_too_low")
    if not bool(source_policy.get("safe_export_only")):
        blockers.append("fair_source_snapshot_not_declared_safe_export_only")
    if str(model_policy.get("probability_output_basis") or "") != "purchased_outcome_win_probability":
        blockers.append("probability_output_basis_not_purchased_outcome")
    if not bool(model_policy.get("uncertainty_declared")):
        blockers.append("probability_uncertainty_not_declared")

    ready = not blockers
    return {
        "status": "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_READY" if ready else "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_FAIL_CLOSED",
        "ready": ready,
        "fixture": bool(manifest.get("fixture")),
        "path": str(path),
        "provided_fields": sorted(provided),
        "missing_fields": missing,
        "accounts_covered": sorted(accounts),
        "joins_declared": joins,
        "blockers": sorted(set(blockers)),
        "candidate_summary": {
            "artifact": manifest.get("artifact"),
            "decision_label": manifest.get("decision_label"),
            "created_utc": manifest.get("created_utc"),
        },
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    gap = load_json(args.entry_source_gap_manifest)
    probability_spec = load_json(args.probability_spec_manifest)
    required_fields = required_candidate_fields(probability_spec)
    contract = build_export_contract(args, probability_spec)
    candidate_status = candidate_export_manifest_status(args.candidate_export_manifest, required_fields)
    real_export_ready = bool(candidate_status.get("ready")) and not bool(candidate_status.get("fixture"))
    decision = "KEEP"
    label = "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORT_SPEC_READY"
    real_status = (
        "READY_WITH_NON_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT"
        if real_export_ready
        else "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_MISSING"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_public_activity_entry_export_spec",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "spec_only": True,
            "public_profile_exports_only": True,
            "private_owner_trade_truth_used": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "local_agg_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "dplus_failed_families_enabled_or_swept": False,
        },
        "source_inputs": {
            "entry_source_gap_manifest": str(args.entry_source_gap_manifest),
            "probability_spec_manifest": str(args.probability_spec_manifest),
            "candidate_export_manifest": str(args.candidate_export_manifest) if args.candidate_export_manifest else None,
        },
        "entry_source_gap_summary": {
            "decision_label": gap.get("decision_label"),
            "hard_missing_fields": (gap.get("research_ranking") or {}).get("hard_missing_fields"),
            "critical_blockers": (gap.get("research_ranking") or {}).get("critical_blockers"),
        },
        "public_activity_entry_export_contract": contract,
        "candidate_export_manifest_status": candidate_status,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "spec_ready": True,
            "real_export_ready": real_export_ready,
            "real_export_status": real_status,
            "b55_role": "main fair-price/pair-edge template",
            "ce25_role": "clean low-residual control",
            "blockers_before_any_shadow_or_deployable_claim": [
                "non_fixture_public_activity_entry_export_missing"
                if not real_export_ready
                else "non_fixture_public_activity_entry_export_available_but_still_proxy_only",
                "liquidity_role_truth_must_be_explicit_per_trade",
                "fair_source_snapshot_must_be_safe_export_not_service_start",
                "private_truth_not_available",
            ],
            "interpretation": (
                "The exporter spec is ready and prevents b55/ce25 from collapsing into a price-band filter. Real "
                "evaluation remains UNKNOWN until non-fixture rows provide per-trade role and fair-probability joins."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_ACTIVITY_ENTRY_EXPORT_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement b55_ce25_public_activity_entry_exporter_fixture_v1 locally: convert already available public "
            "activity rows into the export schema for fixtures, prove fee derivation/metadata parsing fail closed, "
            "and keep real export UNKNOWN until liquidity_role and fair-source probability joins are non-fixture."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry-source-gap-manifest", type=Path, default=DEFAULT_GAP_AUDIT)
    parser.add_argument("--probability-spec-manifest", type=Path, default=DEFAULT_PROBABILITY_SPEC)
    parser.add_argument("--candidate-export-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--entry-delta-s-min", type=float, default=-900.0)
    parser.add_argument("--entry-delta-s-max", type=float, default=-60.0)
    parser.add_argument("--price-min", type=float, default=0.35)
    parser.add_argument("--price-max", type=float, default=0.90)
    parser.add_argument("--negative-fee-tolerance-usdc", type=float, default=0.000001)
    parser.add_argument("--max-source-join-abs-delta-ms", type=float, default=1500.0)
    parser.add_argument("--min-source-count", type=int, default=3)
    parser.add_argument("--max-source-age-ms", type=float, default=1500.0)
    parser.add_argument("--max-source-dispersion-bps", type=float, default=8.0)
    parser.add_argument("--max-probability-uncertainty", type=float, default=0.03)
    parser.add_argument("--min-edge-after-fee-uncertainty", type=float, default=0.02)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    manifest = build_manifest(args, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "real_export_status": manifest["research_ranking"]["real_export_status"],
                "candidate_export_manifest_status": manifest["candidate_export_manifest_status"]["status"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
