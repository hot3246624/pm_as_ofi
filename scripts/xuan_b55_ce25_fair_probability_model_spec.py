#!/usr/bin/env python3
"""Define the b55/ce25 fair-probability model contract.

This is a local-only specification artifact. It does not fetch data, start
local aggregators, connect shared ingress, run a shadow, or place trades. Its
job is to make b55-style entry research fail closed unless the required
probability-model inputs are present for both the b55 main lead and the ce25
clean low-residual control.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_fair_probability_model_spec"
DEFAULT_SOURCE_REVIEW = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_fair_price_source_manifest_review_20260524T054828Z/"
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
REQUIRED_FIELDS = (
    "account",
    "trade_id",
    "condition_id",
    "market_slug",
    "asset",
    "timeframe",
    "market_start_ts",
    "market_end_ts",
    "quote_ts",
    "entry_delta_s",
    "time_to_expiry_s",
    "outcome",
    "polymarket_price",
    "size",
    "gross_usdc",
    "fee_usdc",
    "actual_usdc",
    "liquidity_role",
    "boundary_price",
    "fair_spot_mid",
    "source_count",
    "source_names",
    "source_dispersion_bps",
    "max_source_age_ms",
    "volatility_lookback_s",
    "volatility_bps",
    "fair_probability",
    "fair_probability_uncertainty",
    "edge_after_fee_and_uncertainty",
    "model_name",
    "model_version",
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


def candidate_manifest_status(path: Path | None, args: argparse.Namespace) -> dict[str, Any]:
    if path is None:
        return {
            "status": "MISSING_CANDIDATE_FAIR_PROBABILITY_MANIFEST",
            "ready": False,
            "path": None,
            "missing_fields": list(REQUIRED_FIELDS),
            "blockers": [
                "candidate_manifest_absent",
                "fair_probability_model_not_bound_to_entry_rows",
                "per_trade_fee_liquidity_role_join_absent",
            ],
        }
    if not path_safe(path):
        return {
            "status": "UNSAFE_CANDIDATE_MANIFEST_PATH",
            "ready": False,
            "path": str(path),
            "missing_fields": list(REQUIRED_FIELDS),
            "blockers": ["unsafe_candidate_manifest_path"],
        }
    try:
        manifest = load_json(path)
    except Exception as exc:
        return {
            "status": "CANDIDATE_MANIFEST_READ_FAILED",
            "ready": False,
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "missing_fields": list(REQUIRED_FIELDS),
            "blockers": ["candidate_manifest_read_failed"],
        }
    provided_fields = as_list(manifest.get("provided_fields") or manifest.get("schema_fields"))
    missing = [field for field in REQUIRED_FIELDS if field not in provided_fields]
    accounts = set(str(item) for item in as_list(manifest.get("accounts_covered")))
    assets = set(str(item) for item in as_list(manifest.get("assets_covered")))
    timeframes = set(str(item) for item in as_list(manifest.get("timeframes_covered")))
    blockers: list[str] = []
    if missing:
        blockers.append("required_fields_missing")
    if not set(REQUIRED_ACCOUNTS).issubset(accounts):
        blockers.append("b55_and_ce25_not_both_covered")
    if not set(REQUIRED_ASSETS).issubset(assets):
        blockers.append("btc_and_eth_not_both_covered")
    if not set(REQUIRED_TIMEFRAMES).issubset(timeframes):
        blockers.append("timeframe_15m_and_1h_not_both_covered")
    sample_policy = manifest.get("sample_policy") if isinstance(manifest.get("sample_policy"), dict) else {}
    if as_float(sample_policy.get("entry_delta_s_min")) > args.entry_delta_s_min:
        blockers.append("entry_window_starts_too_late")
    if as_float(sample_policy.get("entry_delta_s_max")) < args.entry_delta_s_max:
        blockers.append("entry_window_ends_too_early")
    if as_float(sample_policy.get("polymarket_price_min")) > args.price_min:
        blockers.append("price_floor_too_high")
    if as_float(sample_policy.get("polymarket_price_max")) < args.price_max:
        blockers.append("price_ceiling_too_low")
    model = manifest.get("model") if isinstance(manifest.get("model"), dict) else {}
    if not model.get("model_name") or not model.get("model_version"):
        blockers.append("model_identity_missing")
    if str(model.get("probability_output_basis") or "") != "entry_win_probability":
        blockers.append("probability_output_basis_not_entry_win_probability")
    if not bool(model.get("uses_volatility_or_uncertainty_input")):
        blockers.append("volatility_or_uncertainty_input_not_declared")
    quality = manifest.get("quality_gates") if isinstance(manifest.get("quality_gates"), dict) else {}
    if as_float(quality.get("min_source_count")) < args.min_source_count:
        blockers.append("min_source_count_too_low")
    if as_float(quality.get("max_source_age_ms"), 10**9) > args.max_source_age_ms:
        blockers.append("max_source_age_ms_too_high")
    if as_float(quality.get("max_source_dispersion_bps"), 10**9) > args.max_source_dispersion_bps:
        blockers.append("max_source_dispersion_bps_too_high")
    if as_float(quality.get("max_probability_uncertainty"), 10**9) > args.max_probability_uncertainty:
        blockers.append("max_probability_uncertainty_too_high")
    if as_float(quality.get("min_edge_after_fee_and_uncertainty")) < args.min_edge_after_fee_uncertainty:
        blockers.append("min_edge_after_fee_and_uncertainty_too_low")
    ready = not blockers
    return {
        "status": "CANDIDATE_FAIR_PROBABILITY_MANIFEST_READY" if ready else "CANDIDATE_MANIFEST_FAIL_CLOSED",
        "ready": ready,
        "path": str(path),
        "fixture": bool(manifest.get("fixture")),
        "provided_fields": provided_fields,
        "missing_fields": missing,
        "accounts_covered": sorted(accounts),
        "assets_covered": sorted(assets),
        "timeframes_covered": sorted(timeframes),
        "blockers": blockers,
        "candidate_manifest_summary": {
            "artifact": manifest.get("artifact"),
            "decision_label": manifest.get("decision_label"),
            "created_utc": manifest.get("created_utc"),
        },
    }


def build_contract(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "contract_name": "b55_ce25_fair_probability_model_spec_v1",
        "objective": (
            "Estimate entry-time win probability for BTC/ETH 15m and 1h Polymarket outcomes, then admit only rows "
            "where fair_probability exceeds Polymarket price after explicit fee, model uncertainty, source-quality, "
            "pair-cost-tail, and residual-stress buffers."
        ),
        "allowed_universe": {
            "accounts_required_for_research_gate": list(REQUIRED_ACCOUNTS),
            "assets": list(REQUIRED_ASSETS),
            "timeframes": list(REQUIRED_TIMEFRAMES),
            "entry_delta_s_min": args.entry_delta_s_min,
            "entry_delta_s_max": args.entry_delta_s_max,
            "polymarket_price_min": args.price_min,
            "polymarket_price_max": args.price_max,
            "excluded": [
                "5m hard chase",
                "pure price-band strategy",
                "single-source last-trade proxy",
                "static asset/timeframe deletion as a strategy",
                "D+ micro-deficit/ledger/tiny-deficit/closed-cycle/cooldown/fill-to-balance families",
            ],
        },
        "required_candidate_fields": list(REQUIRED_FIELDS),
        "model_inputs": {
            "market_boundary": ["boundary_price", "market_start_ts", "market_end_ts", "time_to_expiry_s"],
            "fair_spot": [
                "fair_spot_mid",
                "source_count",
                "source_names",
                "source_dispersion_bps",
                "max_source_age_ms",
            ],
            "uncertainty": ["volatility_lookback_s", "volatility_bps", "fair_probability_uncertainty"],
            "execution_cost": ["fee_usdc", "gross_usdc", "actual_usdc", "liquidity_role"],
            "polymarket_entry": ["polymarket_price", "outcome", "size", "entry_delta_s"],
        },
        "quality_gates": {
            "min_source_count": args.min_source_count,
            "max_source_age_ms": args.max_source_age_ms,
            "max_source_dispersion_bps": args.max_source_dispersion_bps,
            "max_probability_uncertainty": args.max_probability_uncertainty,
            "min_edge_after_fee_and_uncertainty": args.min_edge_after_fee_uncertainty,
            "max_average_actual_pair_cost": args.max_average_pair_cost,
            "max_pair_cost_p90": args.max_pair_cost_p90,
            "max_residual_rate": args.max_residual_rate,
        },
        "edge_formula_contract": {
            "fair_probability": "model output in [0,1] for the purchased outcome at quote_ts",
            "fee_per_share": "fee_usdc / size when size > 0",
            "edge_after_fee_and_uncertainty": (
                "fair_probability - polymarket_price - fee_per_share - fair_probability_uncertainty"
            ),
            "admit_if": "edge_after_fee_and_uncertainty >= min_edge_after_fee_and_uncertainty and all quality gates pass",
        },
        "pass_fail_policy": {
            "keep_spec_ready": [
                "contract has explicit fields, gates, and fail-closed validation",
                "fixture smoke covers missing and complete candidate manifests",
            ],
            "unknown_real_inputs": [
                "candidate fair-probability manifest absent",
                "volatility/uncertainty input absent",
                "per-trade fee/liquidity role join absent",
                "entry-window trade-to-fair-source join absent",
            ],
            "discard": [
                "Polymarket price band becomes the signal",
                "single exchange or RDTS point replaces fair_probability",
                "strategy depends on forbidden D+ families or live/private promotion claims",
            ],
        },
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    source_review = load_json(args.source_review_manifest)
    candidate_status = candidate_manifest_status(args.candidate_fair_probability_manifest, args)
    spec_ready = True
    real_inputs_ready = bool(candidate_status.get("ready")) and not bool(candidate_status.get("fixture"))
    decision = "KEEP"
    label = "KEEP_B55_CE25_FAIR_PROBABILITY_MODEL_SPEC_READY"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_fair_probability_model_spec",
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
            "source_review_manifest": str(args.source_review_manifest),
            "candidate_fair_probability_manifest": str(args.candidate_fair_probability_manifest)
            if args.candidate_fair_probability_manifest
            else None,
        },
        "source_review_summary": {
            "decision_label": source_review.get("decision_label"),
            "hard_missing_required_fields": (
                (source_review.get("research_ranking") or {}).get("hard_missing_required_fields")
            ),
        },
        "probability_model_contract": build_contract(args),
        "candidate_manifest_status": candidate_status,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "spec_ready": spec_ready,
            "real_inputs_ready": real_inputs_ready,
            "real_input_status": (
                "READY_FOR_LOCAL_RESEARCH_VERIFIER" if real_inputs_ready else "UNKNOWN_REAL_FAIR_PROBABILITY_INPUTS_MISSING"
            ),
            "b55_role": "main fair-price/pair-edge template",
            "ce25_role": "clean low-residual pair-arb control",
            "blockers_before_shadow_or_deployable_claim": [
                "real_candidate_fair_probability_manifest_not_ready"
                if not real_inputs_ready
                else "candidate_manifest_ready_but_still_public_proxy_only",
                "private_truth_not_available",
                "promotion_gate_false_by_design",
            ],
            "interpretation": (
                "The probability model contract is ready as tooling. Real b55/ce25 evaluation remains UNKNOWN until "
                "a non-fixture manifest supplies fair_probability, uncertainty, fee/liquidity role, and entry joins."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "FAIR_PROBABILITY_SPEC_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement b55_ce25_public_entry_join_audit_v1 locally: join the public b55/ce25 entry rows to the "
            "probability-model contract fields, first with fixtures and then with any existing safe exported "
            "source tapes if available. Fail closed if per-trade fee/liquidity role or volatility/uncertainty inputs "
            "are absent; do not start collection, shared WS, shadow, canary, or live trading."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-review-manifest", type=Path, default=DEFAULT_SOURCE_REVIEW)
    parser.add_argument("--candidate-fair-probability-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--entry-delta-s-min", type=float, default=-900.0)
    parser.add_argument("--entry-delta-s-max", type=float, default=-60.0)
    parser.add_argument("--price-min", type=float, default=0.35)
    parser.add_argument("--price-max", type=float, default=0.90)
    parser.add_argument("--min-source-count", type=int, default=3)
    parser.add_argument("--max-source-age-ms", type=float, default=1500.0)
    parser.add_argument("--max-source-dispersion-bps", type=float, default=8.0)
    parser.add_argument("--max-probability-uncertainty", type=float, default=0.03)
    parser.add_argument("--min-edge-after-fee-uncertainty", type=float, default=0.02)
    parser.add_argument("--max-average-pair-cost", type=float, default=0.97)
    parser.add_argument("--max-pair-cost-p90", type=float, default=1.0)
    parser.add_argument("--max-residual-rate", type=float, default=0.10)
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
                "candidate_manifest_status": manifest["candidate_manifest_status"]["status"],
                "real_input_status": manifest["research_ranking"]["real_input_status"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
