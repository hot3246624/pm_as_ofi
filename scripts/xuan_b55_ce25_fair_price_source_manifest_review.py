#!/usr/bin/env python3
"""Review whether b55/ce25 fair-price source manifest is implementable now.

This is a local-only source capability review. It inspects allowlisted repo
source files and the corrected public-profile ranking artifact. It does not
start local aggregation, shared ingress, broker code, live paths, SSH, shadow,
or any trading action.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_fair_price_source_manifest_review"
DEFAULT_RANKING = Path(
    "xuan_research_artifacts/"
    "xuan_public_leaderboard_corrected_new_mtm_audit_20260524T054249Z/"
    "manifest.json"
)
SOURCE_FILES = (
    Path("src/bin/polymarket_v2.rs"),
    Path("scripts/search_local_price_agg_models.py"),
    Path("scripts/tune_local_price_agg.py"),
    Path("scripts/build_local_agg_boundary_dataset.py"),
    Path("scripts/evaluate_local_agg_boundary_router.py"),
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


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def source_evidence(root: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for rel in SOURCE_FILES:
        path = root / rel
        text = read_text(path)
        evidence[str(rel)] = {
            "exists": path.exists(),
            "symbols": {
                "local_price_agg_probe": "LocalPriceAggProbe" in text,
                "local_price_agg_sources_probe": "LocalPriceAggSourcesProbe" in text,
                "local_price_agg_boundary_probe": "LocalPriceAggBoundaryProbe" in text,
                "known_sources": all(token in text.lower() for token in ("binance", "bybit", "okx", "coinbase")),
                "hyperliquid_source": "hyperliquid" in text.lower(),
                "source_count": "source_count" in text,
                "source_spread_bps": "source_spread_bps" in text or "source_dispersion" in text,
                "source_age_or_delta_ms": "close_abs_delta_ms" in text or "max_source_age" in text,
                "boundary_or_rtds_open": "rtds_open" in text or "boundary" in text,
                "uncertainty_gate": "uncertainty_gate" in text,
                "fair_probability": "fair_probability" in text or "fair_prob" in text,
                "edge_after_fee": "edge_after_fee" in text,
            },
        }
    return evidence


def supported_fields(evidence: dict[str, Any]) -> dict[str, Any]:
    combined = {
        key: any((file_info.get("symbols") or {}).get(key) for file_info in evidence.values())
        for key in (
            "local_price_agg_probe",
            "local_price_agg_sources_probe",
            "local_price_agg_boundary_probe",
            "known_sources",
            "hyperliquid_source",
            "source_count",
            "source_spread_bps",
            "source_age_or_delta_ms",
            "boundary_or_rtds_open",
            "uncertainty_gate",
            "fair_probability",
            "edge_after_fee",
        )
    }
    return {
        "asset": "public_profile_export",
        "market_slug": "public_profile_export",
        "condition_id": "public_profile_export",
        "market_start_ts": "slug_or_market_metadata_required",
        "market_end_ts": "slug_or_market_metadata_required",
        "quote_ts": "public_profile_export",
        "entry_delta_s": "derivable_if_market_end_ts_and_quote_ts_joined",
        "outcome": "public_profile_export",
        "polymarket_price": "public_profile_export",
        "fair_spot_mid": "source_code_available_not_manifest_ready" if combined["source_count"] else "missing",
        "source_count": "source_code_available_not_manifest_ready" if combined["source_count"] else "missing",
        "source_names": "source_code_available_not_manifest_ready" if combined["known_sources"] else "missing",
        "source_dispersion_bps": "source_code_available_not_manifest_ready" if combined["source_spread_bps"] else "missing",
        "max_source_age_ms": "source_code_available_not_manifest_ready" if combined["source_age_or_delta_ms"] else "missing",
        "boundary_price": "partial_boundary_or_rtds_open_available_for_round_close_not_entry_probability"
        if combined["boundary_or_rtds_open"]
        else "missing",
        "fair_probability": "missing_probability_model" if not combined["fair_probability"] else "source_code_token_present",
        "fair_probability_uncertainty": "missing_probability_uncertainty_model",
        "edge_after_fee_and_uncertainty": "missing_until_fair_probability_and_per_trade_fee_join",
        "model_name": "missing_probability_model",
        "model_version": "missing_probability_model",
    }


def missing_hard_fields(field_status: dict[str, Any]) -> list[str]:
    hard_missing = []
    for field in REQUIRED_FAIR_PRICE_FIELDS:
        status = str(field_status.get(field, "missing"))
        if status.startswith("missing"):
            hard_missing.append(field)
    return hard_missing


def extract_profiles(ranking: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for profile in ranking.get("profiles") or []:
        account = profile.get("account")
        if account in {"b55", "ce25"}:
            out[account] = {
                "role": profile.get("role"),
                "observed_public_metrics": profile.get("observed_public_metrics"),
                "user_corrected_overlay": profile.get("user_corrected_overlay"),
            }
    return out


def build_manifest(args: argparse.Namespace, root: Path, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    ranking = load_json(args.corrected_ranking_manifest)
    evidence = source_evidence(root)
    field_status = supported_fields(evidence)
    hard_missing = missing_hard_fields(field_status)
    probability_blockers = [
        "fair_probability_model_missing",
        "fair_probability_uncertainty_model_missing",
        "edge_after_fee_and_uncertainty_missing",
        "per_trade_fee_and_liquidity_role_join_missing",
        "entry_window_trade_to_fair_source_join_missing",
    ]
    decision = "UNKNOWN"
    label = "UNKNOWN_B55_CE25_FAIR_PRICE_SOURCE_MANIFEST_PROBABILITY_BLOCKED"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_fair_price_source_manifest_review",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "source_code_review_only": True,
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
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "dplus_failed_families_enabled_or_swept": False,
        },
        "source_inputs": {
            "corrected_ranking_manifest": str(args.corrected_ranking_manifest),
            "source_files_reviewed": [str(path) for path in SOURCE_FILES],
        },
        "b55_ce25_context": extract_profiles(ranking),
        "required_fair_price_fields": list(REQUIRED_FAIR_PRICE_FIELDS),
        "field_status": field_status,
        "source_code_evidence": evidence,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "b55_role": "main fair-price/pair-edge template",
            "ce25_role": "clean low-residual pair-arb control",
            "what_exists": [
                "Rust local_price_agg/source probes can represent multi-source spot, source count, spread, and source timing.",
                "Boundary/router scripts can parse local aggregate close/source probes and RTDS-open comparison rows.",
                "The repo has an uncertainty-gate hook, but it is not the same as an entry fair-probability model.",
            ],
            "what_is_missing_for_b55_style_entry": probability_blockers,
            "hard_missing_required_fields": hard_missing,
            "interpretation": (
                "Existing local aggregation infrastructure is useful input plumbing, but it does not yet produce "
                "b55-style fair win probability for -15m..-1m entries. Without fair_probability and uncertainty, "
                "the strategy would collapse into a Polymarket price-band or direction heuristic."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "SOURCE_CAPABILITY_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement b55_ce25_fair_probability_model_spec_v1 locally: define a default-off probability model "
            "contract that consumes market boundary, time-to-expiry, multi-source fair_spot_mid, source age/spread, "
            "and an explicit volatility/uncertainty input; then fail closed unless those inputs are present for both "
            "b55 and ce25 entry windows. Do not start collection, shared WS, shadow, canary, or live trading."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corrected-ranking-manifest", type=Path, default=DEFAULT_RANKING)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    manifest = build_manifest(args, root, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "hard_missing_required_fields": manifest["research_ranking"]["hard_missing_required_fields"],
                "what_is_missing_for_b55_style_entry": manifest["research_ranking"][
                    "what_is_missing_for_b55_style_entry"
                ],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
