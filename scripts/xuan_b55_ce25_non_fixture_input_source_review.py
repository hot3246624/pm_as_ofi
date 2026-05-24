#!/usr/bin/env python3
"""Review whether non-fixture b55/ce25 fair-price inputs exist locally.

This local-only review inspects already committed artifacts, existing public
export schemas, and allowlisted repo source files. It separates source-code
capability from actual non-service exports. It does not fetch data, start local
aggregation/shared WS/services, use SSH, run shadow, or touch trading paths.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_non_fixture_input_source_review"
DEFAULT_SCORER = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_public_activity_entry_scorer_20260524T083746Z/"
    "manifest.json"
)
DEFAULT_ENTRY_SOURCE_GAP = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_entry_row_source_gap_audit_20260524T065245Z/"
    "manifest.json"
)
DEFAULT_FAIR_SOURCE_REVIEW = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_fair_price_source_manifest_review_20260524T054828Z/"
    "manifest.json"
)
DEFAULT_PUBLIC_INPUT_ROOT = Path(
    "xuan_research_artifacts/"
    "xuan_public_leaderboard_candidate_fast_screen_20260524T035500Z/"
    "public_inputs"
)
DEFAULT_EXPORT_ROOT = Path("/Users/hot/web3Scientist/poly_trans_research/data/exports")
PROFILE_EXPORTS = {
    "b55": "leaderboard_b55_recent24h_20260523_103000_to_20260524_103000_bjt",
    "ce25": "leaderboard_ce25_recent24h_20260523_103000_to_20260524_103000_bjt",
}
PUBLIC_INPUT_WALLETS = {
    "b55": "0xb55fa1296e6ec55d0ce53d93b9237389f11764d4",
    "ce25": "0xce25e214d5cfe4f459cf67f08df581885aae7fdc",
}
ALLOWLISTED_SOURCE_FILES = (
    Path("scripts/xuan_public_profile_reset_review.py"),
    Path("scripts/xuan_b55_ce25_public_entry_join_audit.py"),
    Path("scripts/xuan_b55_ce25_entry_row_source_gap_audit.py"),
    Path("scripts/xuan_b55_ce25_public_activity_entry_export_spec.py"),
    Path("scripts/xuan_b55_ce25_public_activity_entry_exporter_fixture.py"),
    Path("scripts/xuan_b55_ce25_public_activity_entry_scorer.py"),
    Path("scripts/xuan_b55_ce25_fair_probability_model_spec.py"),
    Path("scripts/xuan_b55_ce25_fair_price_source_manifest_review.py"),
    Path("scripts/build_local_agg_boundary_dataset.py"),
    Path("scripts/evaluate_local_agg_boundary_router.py"),
    Path("scripts/search_local_price_agg_models.py"),
    Path("scripts/tune_local_price_agg.py"),
)
FORBIDDEN_PATH_FRAGMENTS = (
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
PUBLIC_ACTIVITY_FIELDS = (
    "transactionHash",
    "conditionId",
    "eventSlug",
    "slug",
    "timestamp",
    "price",
    "size",
    "usdcSize",
    "outcome",
    "outcomeIndex",
    "side",
    "type",
)
REQUIRED_NON_FIXTURE_INPUTS = (
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
REQUIRED_JOINS = (
    "public_activity_entry_rows",
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
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def read_csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        return next(reader, [])


def json_keys(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        obj = load_json(path)
    except Exception:
        return []
    return sorted(obj.keys()) if isinstance(obj, dict) else []


def public_input_observation(root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for account, wallet in PUBLIC_INPUT_WALLETS.items():
        path = root / wallet / "activity_trade_rows.json"
        rows = load_json(path) if path.exists() else []
        keys = sorted({key for row in rows for key in row}) if isinstance(rows, list) else []
        out[account] = {
            "path": str(path),
            "exists": path.exists(),
            "row_count": len(rows) if isinstance(rows, list) else 0,
            "keys": keys,
            "public_activity_fields_complete": all(field in keys for field in PUBLIC_ACTIVITY_FIELDS),
            "has_liquidity_role_truth": any("role" in key.lower() or "taker" in key.lower() for key in keys),
            "has_fair_source_fields": any(key in keys for key in ("fair_spot_mid", "source_count", "source_names")),
            "has_probability_fields": any("prob" in key.lower() or "edge_after_fee" in key for key in keys),
            "interpretation": "public activity rows exist but contain no per-trade liquidity role or fair-probability join",
        }
    return out


def public_export_observation(export_root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for account, dirname in PROFILE_EXPORTS.items():
        export_dir = export_root / dirname
        files: dict[str, Any] = {}
        for name in (
            "summary.json",
            "market_trade_metrics.csv",
            "per_market_cash_pnl.csv",
            "current_positions.csv",
            "daily_cashflow.csv",
            "deep_raw_analysis.json",
        ):
            path = export_dir / name
            if name.endswith(".csv"):
                header = read_csv_header(path)
                files[name] = {
                    "exists": path.exists(),
                    "header": header,
                    "has_liquidity_role_truth": any("role" in h.lower() or "taker" in h.lower() for h in header),
                    "has_probability_fields": any("prob" in h.lower() or "edge_after_fee" in h for h in header),
                    "has_fair_source_fields": any(h in header for h in ("fair_spot_mid", "source_count", "source_names")),
                }
            else:
                keys = json_keys(path)
                files[name] = {
                    "exists": path.exists(),
                    "top_level_keys": keys,
                    "has_liquidity_role_truth": any("role" in k.lower() or "taker" in k.lower() for k in keys),
                    "has_probability_fields": any("prob" in k.lower() or "edge_after_fee" in k for k in keys),
                    "has_fair_source_fields": any(k in keys for k in ("fair_spot_mid", "source_count", "source_names")),
                }
        out[account] = {
            "export_dir": str(export_dir),
            "files": files,
            "has_per_trade_entry_rows": False,
            "has_non_fixture_liquidity_role_source": any(
                info["has_liquidity_role_truth"] for info in files.values()
            ),
            "has_non_fixture_fair_probability_source": any(info["has_probability_fields"] for info in files.values()),
            "has_non_fixture_fair_source_snapshot": any(info["has_fair_source_fields"] for info in files.values()),
            "rebate_fields_are_account_level_only": True,
        }
    return out


def source_code_observation(root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for rel in ALLOWLISTED_SOURCE_FILES:
        text = read_text(root / rel)
        lower = text.lower()
        out[str(rel)] = {
            "exists": bool(text),
            "symbols": {
                "writes_public_activity_rows": "activity_trade_rows.json" in text,
                "public_activity_data_api": "data-api.polymarket.com" in lower and "activity" in lower,
                "mentions_liquidity_role": "liquidity_role" in text,
                "mentions_taker_only": "takeronly" in lower or "taker_only" in lower,
                "mentions_fair_probability": "fair_probability" in text,
                "mentions_probability_uncertainty": "fair_probability_uncertainty" in text,
                "mentions_edge_after_fee": "edge_after_fee_and_uncertainty" in text or "edge_after_fee" in text,
                "mentions_source_snapshot": "source_count" in text and ("source_names" in text or "source_spread" in text),
                "mentions_source_age": "max_source_age" in text or "close_abs_delta_ms" in text,
                "mentions_volatility_bps": "volatility_bps" in text,
                "local_agg_code": "local_agg" in lower or "LocalPriceAgg" in text,
            },
        }
    return out


def combined_code_capability(obs: dict[str, Any]) -> dict[str, bool]:
    symbols = [item.get("symbols") or {} for item in obs.values()]

    def any_symbol(name: str) -> bool:
        return any(bool(item.get(name)) for item in symbols)

    return {
        "public_activity_rows_code_exists": any_symbol("writes_public_activity_rows")
        or any_symbol("public_activity_data_api"),
        "liquidity_role_terms_exist": any_symbol("mentions_liquidity_role") or any_symbol("mentions_taker_only"),
        "fair_probability_terms_exist": any_symbol("mentions_fair_probability"),
        "probability_uncertainty_terms_exist": any_symbol("mentions_probability_uncertainty"),
        "edge_after_fee_terms_exist": any_symbol("mentions_edge_after_fee"),
        "source_snapshot_terms_exist": any_symbol("mentions_source_snapshot"),
        "source_age_terms_exist": any_symbol("mentions_source_age"),
        "volatility_terms_exist": any_symbol("mentions_volatility_bps"),
        "local_agg_code_exists": any_symbol("local_agg_code"),
    }


def scorer_summary(scorer: dict[str, Any]) -> dict[str, Any]:
    score = scorer.get("score") if isinstance(scorer.get("score"), dict) else {}
    real = score.get("real_score") if isinstance(score.get("real_score"), dict) else {}
    fixture = score.get("fixture_score") if isinstance(score.get("fixture_score"), dict) else {}
    return {
        "decision_label": scorer.get("decision_label"),
        "real_status": real.get("status"),
        "fixture_status": fixture.get("status"),
        "real_missing_fields": real.get("missing_fields") or [],
        "required_non_fixture_inputs": score.get("required_non_fixture_inputs_before_no_order_diagnostic") or [],
    }


def candidate_input_source_status(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {
            "status": "MISSING_CANDIDATE_NON_FIXTURE_INPUT_SOURCE_MANIFEST",
            "ready": False,
            "path": None,
            "blockers": ["candidate_non_fixture_input_source_manifest_absent"],
            "missing_fields": list(REQUIRED_NON_FIXTURE_INPUTS),
        }
    if not path_safe(path):
        return {
            "status": "UNSAFE_CANDIDATE_NON_FIXTURE_INPUT_SOURCE_MANIFEST_PATH",
            "ready": False,
            "path": str(path),
            "blockers": ["unsafe_candidate_non_fixture_input_source_manifest_path"],
            "missing_fields": list(REQUIRED_NON_FIXTURE_INPUTS),
        }
    try:
        manifest = load_json(path)
    except Exception as exc:
        return {
            "status": "CANDIDATE_NON_FIXTURE_INPUT_SOURCE_MANIFEST_READ_FAILED",
            "ready": False,
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "blockers": ["candidate_non_fixture_input_source_manifest_read_failed"],
            "missing_fields": list(REQUIRED_NON_FIXTURE_INPUTS),
        }
    provided = set(str(field) for field in manifest.get("provided_fields") or manifest.get("schema_fields") or [])
    accounts = set(str(account) for account in manifest.get("accounts_covered") or [])
    joins = manifest.get("joins_declared") if isinstance(manifest.get("joins_declared"), dict) else {}
    source_paths = [Path(str(item)) for item in manifest.get("source_paths") or []]
    missing = [field for field in REQUIRED_NON_FIXTURE_INPUTS if field not in provided]
    blockers: list[str] = []
    if bool(manifest.get("fixture")):
        blockers.append("candidate_is_fixture")
    if not bool(manifest.get("safe_non_service_export")):
        blockers.append("safe_non_service_export_not_declared")
    if bool(manifest.get("fetches_new_data")):
        blockers.append("fetches_new_data")
    if bool(manifest.get("starts_service")):
        blockers.append("starts_service")
    if bool(manifest.get("uses_shared_ws")):
        blockers.append("uses_shared_ws")
    if bool(manifest.get("uses_ssh")):
        blockers.append("uses_ssh")
    if bool(manifest.get("reads_forbidden_store")):
        blockers.append("reads_forbidden_store")
    if missing:
        blockers.append("required_non_fixture_fields_missing")
    if not {"b55", "ce25"}.issubset(accounts):
        blockers.append("b55_and_ce25_not_both_covered")
    for join in REQUIRED_JOINS:
        if not joins.get(join):
            blockers.append(f"{join}_join_missing")
    if not source_paths:
        blockers.append("source_paths_missing")
    for source_path in source_paths:
        if not path_safe(source_path):
            blockers.append("unsafe_source_path")
            break
    ready = not blockers
    return {
        "status": "CANDIDATE_NON_FIXTURE_INPUT_SOURCE_READY" if ready else "CANDIDATE_NON_FIXTURE_INPUT_SOURCE_FAIL_CLOSED",
        "ready": ready,
        "path": str(path),
        "provided_fields": sorted(provided),
        "missing_fields": missing,
        "accounts_covered": sorted(accounts),
        "joins_declared": joins,
        "source_paths": [str(item) for item in source_paths],
        "blockers": sorted(set(blockers)),
        "candidate_summary": {
            "artifact": manifest.get("artifact"),
            "decision_label": manifest.get("decision_label"),
            "created_utc": manifest.get("created_utc"),
        },
    }


def non_fixture_input_contract() -> dict[str, Any]:
    return {
        "contract_name": "b55_ce25_non_fixture_input_source_v1",
        "purpose": "Allow b55/ce25 public entry research to proceed beyond fixture-only schema tests.",
        "required_fields": list(REQUIRED_NON_FIXTURE_INPUTS),
        "required_joins": list(REQUIRED_JOINS),
        "accepted_sources": {
            "liquidity_role": [
                "explicit per-trade public maker/taker role",
                "documented per-trade takerOnly field",
                "verified non-private fill metadata keyed to trade_id or transactionHash",
            ],
            "fair_source_snapshot": [
                "safe pre-existing export with quote_ts-aligned source_count/source_names/source dispersion/source age",
                "no local agg/shared WS/service start for this review",
            ],
            "probability_model": [
                "offline purchased-outcome probability output with uncertainty and model version",
                "edge formula equals fair_probability - polymarket_price - fee_usdc / size - uncertainty",
            ],
        },
        "rejected_sources": [
            "maker rebate as per-trade maker proof",
            "fee magnitude as maker/taker inference",
            "account-level takerOnly share as per-trade role",
            "pure Polymarket price band",
            "local source code token without exported time-aligned rows",
            "fixture/synthetic fair probability",
        ],
        "stop_conditions": [
            "liquidity_role remains absent",
            "fair_probability or uncertainty remains fixture-only",
            "source snapshot requires service/shared-WS/local-agg start",
            "candidate source would require raw/replay/full-store scan",
        ],
    }


def build_manifest(args: argparse.Namespace, root: Path, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    scorer = load_json(args.scorer_manifest)
    entry_gap = load_json(args.entry_source_gap_manifest)
    fair_review = load_json(args.fair_source_review_manifest)
    public_inputs = public_input_observation(args.public_input_root)
    exports = public_export_observation(args.export_root)
    source_obs = source_code_observation(root)
    code_caps = combined_code_capability(source_obs)
    candidate_status = candidate_input_source_status(args.candidate_input_source_manifest)
    candidate_ready = bool(candidate_status.get("ready"))
    decision = "KEEP" if candidate_ready else "UNKNOWN"
    label = (
        "KEEP_B55_CE25_NON_FIXTURE_INPUT_SOURCE_READY"
        if candidate_ready
        else "UNKNOWN_B55_CE25_NON_FIXTURE_INPUT_SOURCE_MISSING"
    )
    exact_missing = list(REQUIRED_NON_FIXTURE_INPUTS)
    if candidate_ready:
        exact_missing = []
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_non_fixture_input_source_review",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "public_export_schema_review_only": True,
            "allowlisted_source_files_only": True,
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
            "scorer_manifest": str(args.scorer_manifest),
            "entry_source_gap_manifest": str(args.entry_source_gap_manifest),
            "fair_source_review_manifest": str(args.fair_source_review_manifest),
            "public_input_root": str(args.public_input_root),
            "export_root": str(args.export_root),
            "allowlisted_source_files": [str(path) for path in ALLOWLISTED_SOURCE_FILES],
            "candidate_input_source_manifest": str(args.candidate_input_source_manifest)
            if args.candidate_input_source_manifest
            else None,
        },
        "prior_scorer_summary": scorer_summary(scorer),
        "prior_gap_summary": {
            "decision_label": entry_gap.get("decision_label"),
            "critical_blockers": (entry_gap.get("research_ranking") or {}).get("critical_blockers"),
            "hard_missing_fields": (entry_gap.get("research_ranking") or {}).get("hard_missing_fields"),
        },
        "prior_fair_source_summary": {
            "decision_label": fair_review.get("decision_label"),
            "hard_missing_required_fields": (fair_review.get("research_ranking") or {}).get(
                "hard_missing_required_fields"
            ),
            "what_exists": (fair_review.get("research_ranking") or {}).get("what_exists"),
        },
        "public_input_observation": public_inputs,
        "public_export_observation": exports,
        "source_code_observation": source_obs,
        "combined_code_capability": code_caps,
        "candidate_input_source_status": candidate_status,
        "non_fixture_input_contract": non_fixture_input_contract(),
        "research_ranking": {
            "decision": decision,
            "label": label,
            "real_non_fixture_source_ready": candidate_ready,
            "exact_missing_inputs": exact_missing,
            "stop_before_no_order_diagnostic": not candidate_ready,
            "b55_role": "main fair-price/pair-edge template",
            "ce25_role": "clean low-residual control",
            "interpretation": (
                "Existing local rows and exports are enough to normalize public entries, but they do not contain "
                "non-fixture per-trade liquidity role or fair-probability joins. Source code has useful plumbing, "
                "yet no safe pre-existing export currently binds those fields to b55/ce25 entry rows."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "NON_FIXTURE_INPUT_SOURCE_REVIEW_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Either prepare an explicitly approved safe offline exporter for non-service liquidity_role/fair-source "
            "probability joins, or pivot to a different public-profile lead that has observable non-fixture evidence; "
            "do not start no-order diagnostics for b55/ce25 until real_non_fixture_source_ready=true."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scorer-manifest", type=Path, default=DEFAULT_SCORER)
    parser.add_argument("--entry-source-gap-manifest", type=Path, default=DEFAULT_ENTRY_SOURCE_GAP)
    parser.add_argument("--fair-source-review-manifest", type=Path, default=DEFAULT_FAIR_SOURCE_REVIEW)
    parser.add_argument("--public-input-root", type=Path, default=DEFAULT_PUBLIC_INPUT_ROOT)
    parser.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--candidate-input-source-manifest", type=Path)
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
                "real_non_fixture_source_ready": manifest["research_ranking"]["real_non_fixture_source_ready"],
                "exact_missing_inputs": manifest["research_ranking"]["exact_missing_inputs"],
                "candidate_input_source_status": manifest["candidate_input_source_status"]["status"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
