#!/usr/bin/env python3
"""Audit the remaining b55/ce25 entry-row and fair-source export gap.

This is a local-only schema/source review. It reads public export schemas and
allowlisted repo source files, then names the smallest fail-closed export
needed before b55/ce25 fair-price research can be evaluated. It does not fetch
new web/API data, start local aggregation, connect shared ingress, run a
shadow, or touch trading paths.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_entry_row_source_gap_audit"
DEFAULT_ENTRY_JOIN = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_public_entry_join_audit_20260524T062245Z/"
    "manifest.json"
)
DEFAULT_PROBABILITY_SPEC = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_fair_probability_model_spec_20260524T055426Z/"
    "manifest.json"
)
DEFAULT_EXPORT_ROOT = Path("/Users/hot/web3Scientist/poly_trans_research/data/exports")
PROFILE_EXPORTS = {
    "b55": "leaderboard_b55_recent24h_20260523_103000_to_20260524_103000_bjt",
    "ce25": "leaderboard_ce25_recent24h_20260523_103000_to_20260524_103000_bjt",
}
ALLOWLISTED_SOURCE_FILES = (
    Path("scripts/xuan_public_profile_reset_review.py"),
    Path("scripts/xuan_b55_ce25_public_entry_join_audit.py"),
    Path("scripts/xuan_b55_ce25_fair_probability_model_spec.py"),
    Path("scripts/xuan_b55_ce25_fair_price_source_manifest_review.py"),
    Path("scripts/build_local_agg_boundary_dataset.py"),
    Path("scripts/evaluate_local_agg_boundary_router.py"),
    Path("scripts/search_local_price_agg_models.py"),
    Path("scripts/tune_local_price_agg.py"),
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
PUBLIC_ACTIVITY_FIELDS = (
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
PUBLIC_ACTIVITY_CORE_FIELDS = (
    "transactionHash",
    "conditionId",
    "slug",
    "timestamp",
    "price",
    "size",
    "usdcSize",
    "outcome",
    "side",
    "type",
)
HARD_MISSING_FIELDS = (
    "liquidity_role",
    "fair_probability",
    "fair_probability_uncertainty",
    "edge_after_fee_and_uncertainty",
    "source_quality_time_join",
    "volatility_bps",
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


def read_csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        return next(reader, [])


def export_schema_observation(export_root: Path) -> dict[str, Any]:
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
                files[name] = {"exists": path.exists(), "header": read_csv_header(path)}
            elif path.exists():
                obj = load_json(path)
                files[name] = {"exists": True, "top_level_keys": list(obj.keys()) if isinstance(obj, dict) else []}
            else:
                files[name] = {"exists": False, "top_level_keys": []}
        market_header = files["market_trade_metrics.csv"]["header"]
        per_market_header = files["per_market_cash_pnl.csv"]["header"]
        out[account] = {
            "export_dir": str(export_dir),
            "files": files,
            "can_identify_market_summary_rows": bool(market_header),
            "can_identify_condition_slug_first_last_trade": all(
                field in market_header for field in ("condition_id", "slug", "first_trade_s", "last_trade_s")
            ),
            "has_per_market_cash_pnl": bool(per_market_header),
            "has_per_trade_activity_rows": False,
            "has_per_trade_fee_or_liquidity_role": False,
            "has_fair_source_join_rows": False,
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
                "fetches_public_activity": "data-api.polymarket.com" in lower and "activity" in lower,
                "writes_public_activity_trade_rows": "activity_trade_rows.json" in text,
                "public_activity_trade_fields": {field: field in text for field in PUBLIC_ACTIVITY_FIELDS},
                "local_price_agg_sources": all(token in lower for token in ("binance", "bybit", "okx", "coinbase")),
                "source_count_or_names": "source_count" in text or "source_names" in text,
                "source_dispersion_or_spread": "source_dispersion" in text or "source_spread_bps" in text,
                "source_age_or_timing": "max_source_age" in text or "close_abs_delta_ms" in text,
                "boundary_or_round_end": "boundary" in lower or "round_end" in lower,
                "volatility_bps": "volatility_bps" in text,
                "fair_probability": "fair_probability" in text,
                "liquidity_role": "liquidity_role" in text,
                "taker_only": "takeronly" in lower or "taker_only" in lower,
            },
        }
    return out


def combined_source_capability(source_obs: dict[str, Any]) -> dict[str, bool]:
    symbols = [info.get("symbols") or {} for info in source_obs.values()]

    def any_symbol(name: str) -> bool:
        return any(bool(item.get(name)) for item in symbols)

    public_fields: dict[str, bool] = {}
    for item in symbols:
        for field, present in (item.get("public_activity_trade_fields") or {}).items():
            public_fields[field] = public_fields.get(field, False) or bool(present)
    return {
        "public_activity_fetcher_exists": any_symbol("fetches_public_activity"),
        "public_activity_raw_rows_writer_exists": any_symbol("writes_public_activity_trade_rows"),
        "public_activity_core_fields_declared": all(public_fields.get(field) for field in PUBLIC_ACTIVITY_CORE_FIELDS),
        "local_source_plumbing_exists": any_symbol("local_price_agg_sources") and any_symbol("source_count_or_names"),
        "source_quality_terms_exist_in_code": any_symbol("source_dispersion_or_spread") and any_symbol("source_age_or_timing"),
        "boundary_terms_exist_in_code": any_symbol("boundary_or_round_end"),
        "volatility_terms_exist_in_code": any_symbol("volatility_bps"),
        "fair_probability_terms_exist_in_code": any_symbol("fair_probability"),
        "liquidity_role_terms_exist_in_code": any_symbol("liquidity_role") or any_symbol("taker_only"),
    }


def required_fields(spec: dict[str, Any]) -> list[str]:
    contract = spec.get("probability_model_contract") if isinstance(spec.get("probability_model_contract"), dict) else {}
    fields = contract.get("required_candidate_fields")
    return list(fields) if isinstance(fields, list) else []


def field_gap_table(required: list[str], caps: dict[str, bool]) -> dict[str, dict[str, Any]]:
    public_activity_available = caps["public_activity_fetcher_exists"] and caps["public_activity_core_fields_declared"]
    fair_source_available = caps["local_source_plumbing_exists"] and caps["source_quality_terms_exist_in_code"]
    table: dict[str, dict[str, Any]] = {}
    for field in required:
        if field in {"account"}:
            status = "available_from_profile_context"
            blocker = False
        elif field in {"trade_id"}:
            status = "available_from_public_activity_transactionHash_if_raw_rows_exported"
            blocker = not public_activity_available
        elif field in {"condition_id"}:
            status = "available_from_public_activity_conditionId_or_market_summary_condition_id"
            blocker = False
        elif field in {"market_slug", "asset", "timeframe"}:
            status = "available_or_derivable_from_public_activity_slug_if_raw_rows_exported"
            blocker = not public_activity_available
        elif field in {"market_start_ts", "market_end_ts"}:
            status = "derivable_for_exact_updown_slugs_but_named_markets_need_market_metadata_join"
            blocker = not public_activity_available
        elif field in {"quote_ts", "entry_delta_s", "time_to_expiry_s"}:
            status = "derivable_from_public_activity_timestamp_plus_market_end_if_raw_rows_exported"
            blocker = not public_activity_available
        elif field in {"outcome", "polymarket_price", "size"}:
            status = "available_from_public_activity_raw_rows"
            blocker = not public_activity_available
        elif field == "gross_usdc":
            status = "derivable_from_public_activity_price_times_size"
            blocker = not public_activity_available
        elif field == "actual_usdc":
            status = "available_from_public_activity_usdcSize_if_raw_rows_exported"
            blocker = not public_activity_available
        elif field == "fee_usdc":
            status = "inferable_only_if_usdcSize_is_actual_cost; direct_fee_field_absent"
            blocker = True
        elif field == "liquidity_role":
            status = "missing_public_activity_taker_or_maker_role"
            blocker = True
        elif field in {"boundary_price", "fair_spot_mid", "source_count", "source_names", "source_dispersion_bps", "max_source_age_ms"}:
            status = "source_plumbing_exists_but_no_entry_time_public_trade_join_manifest" if fair_source_available else "missing_fair_source_plumbing"
            blocker = True
        elif field in {"volatility_lookback_s", "volatility_bps"}:
            status = "model_spec_mentions_volatility_but_no_safe_entry_time_volatility_tape"
            blocker = True
        elif field in {"fair_probability", "fair_probability_uncertainty", "edge_after_fee_and_uncertainty"}:
            status = "missing_entry_probability_model_output"
            blocker = True
        elif field in {"model_name", "model_version"}:
            status = "missing_non_fixture_probability_model_identity"
            blocker = True
        else:
            status = "unknown_field_status"
            blocker = True
        table[field] = {"status": status, "hard_blocker": blocker}
    return table


def candidate_source_manifest_status(path: Path | None, required: list[str]) -> dict[str, Any]:
    if path is None:
        return {
            "status": "MISSING_CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST",
            "ready": False,
            "fixture": False,
            "path": None,
            "missing_fields": required,
            "blockers": ["candidate_source_manifest_absent"],
        }
    if not path_safe(path):
        return {
            "status": "UNSAFE_CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST_PATH",
            "ready": False,
            "fixture": False,
            "path": str(path),
            "missing_fields": required,
            "blockers": ["unsafe_candidate_source_manifest_path"],
        }
    try:
        manifest = load_json(path)
    except Exception as exc:
        return {
            "status": "CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST_READ_FAILED",
            "ready": False,
            "fixture": False,
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "missing_fields": required,
            "blockers": ["candidate_source_manifest_read_failed"],
        }
    provided = set(str(item) for item in (manifest.get("provided_fields") or manifest.get("schema_fields") or []))
    missing = [field for field in required if field not in provided]
    accounts = set(str(item) for item in manifest.get("accounts_covered") or [])
    joins = manifest.get("joins_declared") if isinstance(manifest.get("joins_declared"), dict) else {}
    blockers: list[str] = []
    if missing:
        blockers.append("required_fields_missing")
    if not {"b55", "ce25"}.issubset(accounts):
        blockers.append("b55_and_ce25_not_both_covered")
    for join_name in (
        "public_activity_trade_rows",
        "market_metadata_boundary",
        "fair_source_snapshot_rows",
        "volatility_rows",
        "liquidity_role_truth_source",
        "probability_model_output",
    ):
        if not joins.get(join_name):
            blockers.append(f"{join_name}_join_missing")
    ready = not blockers
    return {
        "status": "CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST_READY" if ready else "CANDIDATE_ENTRY_ROW_SOURCE_MANIFEST_FAIL_CLOSED",
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


def minimal_exporter_contract(required: list[str]) -> dict[str, Any]:
    return {
        "contract_name": "b55_ce25_entry_row_source_export_v1",
        "purpose": (
            "Produce non-trading research rows that join public b55/ce25 activity entries to market metadata, "
            "fee/liquidity-role evidence, source-quality snapshots, volatility inputs, and fair-probability outputs."
        ),
        "required_candidate_fields": required,
        "stages": [
            {
                "name": "public_activity_trade_rows_v1",
                "source": "Polymarket public data-api activity endpoint, type=TRADE, b55 and ce25 only",
                "fields": [
                    "account",
                    "trade_id from transactionHash plus row disambiguator",
                    "condition_id from conditionId",
                    "market_slug from slug/eventSlug",
                    "quote_ts from timestamp",
                    "outcome/outcomeIndex",
                    "polymarket_price from price",
                    "size",
                    "actual_usdc from usdcSize",
                    "gross_usdc = price * size",
                    "fee_usdc only if an explicit or defensible fee decomposition is declared",
                ],
                "fail_closed_if": [
                    "raw trade rows are unavailable",
                    "condition_id/slug/timestamp/price/size/usdcSize missing",
                    "fee_usdc is inferred without stating whether usdcSize is actual cost",
                ],
            },
            {
                "name": "market_metadata_boundary_v1",
                "source": "public market metadata or slug parser for exact updown slugs",
                "fields": ["market_start_ts", "market_end_ts", "asset", "timeframe", "time_to_expiry_s", "entry_delta_s"],
                "fail_closed_if": ["named 1h market lacks start/end metadata", "entry row is outside -900..-60s"],
            },
            {
                "name": "liquidity_role_truth_source_v1",
                "source": "explicit public field, verified fill metadata, or documented takerOnly role source",
                "fields": ["liquidity_role"],
                "fail_closed_if": ["only maker rebate/account-level inference is available", "role is guessed from price/fee"],
            },
            {
                "name": "fair_source_snapshot_rows_v1",
                "source": "safe exported source tape, not live service startup",
                "fields": [
                    "fair_spot_mid",
                    "source_count",
                    "source_names",
                    "source_dispersion_bps",
                    "max_source_age_ms",
                    "boundary_price",
                ],
                "fail_closed_if": ["source tape is not aligned to quote_ts", "source_count < 3", "source age/spread gates absent"],
            },
            {
                "name": "entry_probability_model_output_v1",
                "source": "offline model over boundary/time-to-expiry/fair_spot/volatility",
                "fields": [
                    "volatility_lookback_s",
                    "volatility_bps",
                    "fair_probability",
                    "fair_probability_uncertainty",
                    "edge_after_fee_and_uncertainty",
                    "model_name",
                    "model_version",
                ],
                "fail_closed_if": [
                    "model output is a direction label instead of purchased-outcome probability",
                    "uncertainty is missing",
                    "edge formula differs from fair_probability - polymarket_price - fee_per_share - uncertainty",
                ],
            },
        ],
        "explicit_non_goals": [
            "no shadow",
            "no local agg/shared WS/service start",
            "no live/canary/orders",
            "no raw/replay/full-store scan",
            "no pure Polymarket price cap",
            "no static asset/timeframe deletion as strategy",
        ],
    }


def build_manifest(args: argparse.Namespace, root: Path, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    entry_join = load_json(args.entry_join_manifest)
    spec = load_json(args.probability_spec_manifest)
    required = required_fields(spec)
    exports = export_schema_observation(args.export_root)
    source_obs = source_code_observation(root)
    caps = combined_source_capability(source_obs)
    gaps = field_gap_table(required, caps)
    candidate_status = candidate_source_manifest_status(args.candidate_source_manifest, required)
    real_source_ready = bool(candidate_status.get("ready")) and not bool(candidate_status.get("fixture"))
    hard_blockers = sorted(
        field for field, info in gaps.items() if info.get("hard_blocker") and field in required
    )
    decision = "UNKNOWN"
    label = "UNKNOWN_B55_CE25_ENTRY_ROW_SOURCE_GAP_REAL_EXPORT_FIELDS_MISSING"
    if real_source_ready:
        decision = "KEEP"
        label = "KEEP_B55_CE25_ENTRY_ROW_SOURCE_EXPORT_READY_FOR_LOCAL_RESEARCH"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_entry_row_source_gap_audit",
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
            "entry_join_manifest": str(args.entry_join_manifest),
            "probability_spec_manifest": str(args.probability_spec_manifest),
            "export_root": str(args.export_root),
            "allowlisted_source_files": [str(path) for path in ALLOWLISTED_SOURCE_FILES],
            "candidate_source_manifest": str(args.candidate_source_manifest) if args.candidate_source_manifest else None,
        },
        "entry_join_summary": {
            "decision_label": entry_join.get("decision_label"),
            "public_real_status": (entry_join.get("research_ranking") or {}).get("public_real_status"),
        },
        "export_schema_observation": exports,
        "source_code_observation": source_obs,
        "combined_source_capability": caps,
        "required_field_gap_table": gaps,
        "candidate_source_manifest_status": candidate_status,
        "minimal_safe_exporter_contract": minimal_exporter_contract(required),
        "research_ranking": {
            "decision": decision,
            "label": label,
            "b55_role": "main fair-price/pair-edge template",
            "ce25_role": "clean low-residual control",
            "real_source_ready": real_source_ready,
            "hard_missing_fields": hard_blockers,
            "critical_blockers": list(HARD_MISSING_FIELDS),
            "interpretation": (
                "Existing public export summaries are too aggregated for b55/ce25 replication research. The repo has "
                "a public activity fetcher and local source plumbing, but no current safe export joins per-trade "
                "fee/liquidity role, source-quality snapshots, volatility, and fair-probability output at entry time."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "ENTRY_ROW_SOURCE_GAP_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement b55_ce25_public_activity_entry_export_spec_v1 locally: define a fail-closed exporter contract "
            "for public activity trade rows plus market metadata, explicit fee/liquidity-role evidence, and optional "
            "safe fair-source snapshot manifest. Return UNKNOWN until non-fixture rows provide liquidity_role and "
            "fair_probability/uncertainty joins for both b55 and ce25."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry-join-manifest", type=Path, default=DEFAULT_ENTRY_JOIN)
    parser.add_argument("--probability-spec-manifest", type=Path, default=DEFAULT_PROBABILITY_SPEC)
    parser.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--candidate-source-manifest", type=Path)
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
                "critical_blockers": manifest["research_ranking"]["critical_blockers"],
                "hard_missing_fields": manifest["research_ranking"]["hard_missing_fields"],
                "candidate_source_manifest_status": manifest["candidate_source_manifest_status"]["status"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
