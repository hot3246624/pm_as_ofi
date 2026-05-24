#!/usr/bin/env python3
"""Join public b55/ce25 entry proxies to the fair-probability contract.

This audit is intentionally fail-closed. Public profile exports currently
provide market-level summaries, not per-trade entry rows. A real b55/ce25
fair-price evaluation must join entry rows to fair-probability rows with
per-trade fee/liquidity role and source-quality fields. This script proves the
join contract with fixtures and reports UNKNOWN for current public exports
when those real fields are absent.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_public_entry_join_audit"
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
REQUIRED_ACCOUNTS = ("b55", "ce25")
REQUIRED_ASSETS = ("BTC", "ETH")
REQUIRED_TIMEFRAMES = ("15m", "1h_or_named")
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


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def parse_slug_anchor_ts(slug: str) -> int | None:
    try:
        return int(slug.rsplit("-", 1)[1])
    except Exception:
        return None


def detect_interval_secs_from_slug(slug: str) -> int | None:
    match = re.search(r"-updown-(\d+)([mh])(?:-|$)", slug)
    if not match:
        return None
    n = int(match.group(1))
    unit = match.group(2)
    if unit == "m":
        return n * 60
    if unit == "h":
        return n * 3600
    return None


def parse_slug_start_end(slug: str) -> tuple[int | None, int | None]:
    anchor = parse_slug_anchor_ts(slug)
    interval = detect_interval_secs_from_slug(slug)
    if anchor is None or interval is None:
        return None, None
    return anchor, anchor + interval


def merge_market_rows(export_dir: Path) -> list[dict[str, Any]]:
    metrics = {
        row.get("condition_id"): row
        for row in read_csv_rows(export_dir / "market_trade_metrics.csv")
        if row.get("condition_id")
    }
    cash_rows = read_csv_rows(export_dir / "per_market_cash_pnl.csv")
    if not cash_rows:
        cash_rows = []
        # ce25 has no per_market_cash_pnl.csv in the provided export. Build
        # a reduced market-level view from metrics so missing field coverage is
        # still explicit.
        for metric in metrics.values():
            cash_rows.append(
                {
                    "condition_id": metric.get("condition_id"),
                    "slug": metric.get("slug"),
                    "title": metric.get("title"),
                    "asset": infer_asset(metric.get("slug", ""), metric.get("title", "")),
                    "tf": infer_timeframe(metric.get("slug", ""), metric.get("title", "")),
                    "trade_count": metric.get("trade_count"),
                    "buy_actual": "",
                    "buy_gross": "",
                    "paired_qty": metric.get("paired_qty"),
                    "resid_qty": metric.get("lifetime_residual_qty"),
                    "resid_rate": "",
                    "actual_pair_cost": metric.get("actual_pair_cost"),
                    "pair_pnl": metric.get("paired_actual_profit"),
                }
            )
    out: list[dict[str, Any]] = []
    for cash in cash_rows:
        cid = cash.get("condition_id")
        metric = metrics.get(cid) or {}
        slug = str(cash.get("slug") or metric.get("slug") or "")
        start_ts, end_ts = parse_slug_start_end(slug)
        first_trade = as_int(metric.get("first_trade_s"), default=0) or None
        last_trade = as_int(metric.get("last_trade_s"), default=0) or None
        first_delta = first_trade - end_ts if first_trade is not None and end_ts is not None else None
        last_delta = last_trade - end_ts if last_trade is not None and end_ts is not None else None
        out.append(
            {
                "condition_id": cid,
                "market_slug": slug,
                "title": cash.get("title") or metric.get("title"),
                "asset": cash.get("asset") or infer_asset(slug, str(cash.get("title") or "")),
                "timeframe": cash.get("tf") or infer_timeframe(slug, str(cash.get("title") or "")),
                "market_start_ts": start_ts,
                "market_end_ts": end_ts,
                "first_trade_s": first_trade,
                "last_trade_s": last_trade,
                "first_entry_delta_s": first_delta,
                "last_entry_delta_s": last_delta,
                "trade_count": as_int(cash.get("trade_count") or metric.get("trade_count")),
                "paired_qty": as_float(cash.get("paired_qty") or metric.get("paired_qty")),
                "residual_qty": as_float(cash.get("resid_qty") or metric.get("lifetime_residual_qty")),
                "residual_rate": as_float(cash.get("resid_rate"), default=0.0),
                "actual_pair_cost": as_float(cash.get("actual_pair_cost") or metric.get("actual_pair_cost")),
                "pair_pnl": as_float(cash.get("pair_pnl") or metric.get("paired_actual_profit")),
                "yes_actual_avg": as_float(metric.get("yes_actual_avg"), default=0.0),
                "no_actual_avg": as_float(metric.get("no_actual_avg"), default=0.0),
                "public_row_granularity": "market_summary_not_entry_trade",
            }
        )
    return out


def infer_asset(slug: str, title: str) -> str:
    text = f"{slug} {title}".lower()
    if "bitcoin" in text or "btc-" in text:
        return "BTC"
    if "ethereum" in text or "eth-" in text:
        return "ETH"
    if "solana" in text or "sol-" in text:
        return "SOL"
    if "xrp" in text:
        return "XRP"
    return "unknown"


def infer_timeframe(slug: str, title: str) -> str:
    if "-updown-15m-" in slug:
        return "15m"
    if "-updown-5m-" in slug:
        return "5m"
    if "-updown-1h-" in slug or "1:00" in title:
        return "1h_or_named"
    if "bitcoin up or down" in title.lower() or "ethereum up or down" in title.lower():
        return "1h_or_named"
    match = re.search(r"-updown-(\d+[mh])-", slug)
    return match.group(1) if match else "unknown"


def entry_window_intersects(row: dict[str, Any], min_delta: float, max_delta: float) -> bool:
    first_delta = row.get("first_entry_delta_s")
    last_delta = row.get("last_entry_delta_s")
    if first_delta is None or last_delta is None:
        return False
    return float(first_delta) <= max_delta and float(last_delta) >= min_delta


def load_public_profiles(export_root: Path, args: argparse.Namespace) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for account, dirname in PROFILE_EXPORTS.items():
        export_dir = export_root / dirname
        rows = merge_market_rows(export_dir)
        allowed = [
            row
            for row in rows
            if row.get("asset") in REQUIRED_ASSETS
            and row.get("timeframe") in REQUIRED_TIMEFRAMES
            and as_int(row.get("trade_count")) > 0
        ]
        parseable = [row for row in allowed if row.get("market_end_ts") is not None]
        entry_window_rows = [
            row
            for row in parseable
            if entry_window_intersects(row, args.entry_delta_s_min, args.entry_delta_s_max)
        ]
        out[account] = {
            "export_dir": str(export_dir),
            "market_summary_rows": len(rows),
            "allowed_btc_eth_15m_1h_rows": len(allowed),
            "market_boundary_parseable_rows": len(parseable),
            "entry_window_intersection_rows": len(entry_window_rows),
            "sample_rows": entry_window_rows[:5] or allowed[:5],
            "public_export_field_status": {
                "condition_id": bool(rows),
                "market_slug": bool(rows),
                "asset_timeframe": bool(allowed),
                "market_start_end_ts": bool(parseable),
                "entry_delta_s": bool(parseable),
                "outcome": False,
                "per_trade_size": False,
                "per_trade_fee_usdc": False,
                "liquidity_role": False,
                "fair_probability": False,
                "fair_probability_uncertainty": False,
                "edge_after_fee_and_uncertainty": False,
                "volatility_bps": False,
                "source_tapes_join": False,
            },
        }
    return out


def public_condition_keys(export_root: Path) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for account, dirname in PROFILE_EXPORTS.items():
        for row in merge_market_rows(export_root / dirname):
            cid = row.get("condition_id")
            if cid:
                keys.add((account, str(cid)))
    return keys


def required_fields_from_spec(spec: dict[str, Any]) -> list[str]:
    contract = spec.get("probability_model_contract") if isinstance(spec.get("probability_model_contract"), dict) else {}
    fields = contract.get("required_candidate_fields")
    return list(fields) if isinstance(fields, list) else []


def validate_candidate_rows(
    path: Path | None, required_fields: list[str], public_keys: set[tuple[str, str]]
) -> dict[str, Any]:
    if path is None:
        return {
            "status": "MISSING_CANDIDATE_ENTRY_JOIN_ROWS",
            "ready": False,
            "fixture": False,
            "path": None,
            "missing_fields": required_fields,
            "joined_accounts": [],
            "row_count": 0,
            "blockers": ["candidate_entry_join_rows_absent"],
        }
    if not path_safe(path):
        return {
            "status": "UNSAFE_CANDIDATE_ENTRY_JOIN_ROWS_PATH",
            "ready": False,
            "fixture": False,
            "path": str(path),
            "missing_fields": required_fields,
            "joined_accounts": [],
            "row_count": 0,
            "blockers": ["unsafe_candidate_entry_join_rows_path"],
        }
    try:
        obj = load_json(path)
    except Exception as exc:
        return {
            "status": "CANDIDATE_ENTRY_JOIN_ROWS_READ_FAILED",
            "ready": False,
            "fixture": False,
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "missing_fields": required_fields,
            "joined_accounts": [],
            "row_count": 0,
            "blockers": ["candidate_entry_join_rows_read_failed"],
        }
    rows = obj.get("rows") if isinstance(obj.get("rows"), list) else []
    accounts = sorted({str(row.get("account")) for row in rows if row.get("account")})
    missing = sorted({field for row in rows for field in required_fields if field not in row})
    blockers: list[str] = []
    if not rows:
        blockers.append("candidate_entry_join_rows_empty")
    if missing:
        blockers.append("required_entry_join_fields_missing")
    if not set(REQUIRED_ACCOUNTS).issubset(set(accounts)):
        blockers.append("b55_and_ce25_not_both_joined")
    joined_public_rows = 0
    for row in rows:
        account = str(row.get("account") or "")
        condition_id = str(row.get("condition_id") or "")
        if not account or not condition_id:
            blockers.append("candidate_row_missing_join_key")
        elif (account, condition_id) in public_keys:
            joined_public_rows += 1
        else:
            blockers.append("candidate_rows_not_joined_to_public_export")
        edge = as_float(row.get("edge_after_fee_and_uncertainty"), default=-10**9)
        if edge < 0.02:
            blockers.append("edge_after_fee_and_uncertainty_below_contract")
            break
    if rows and joined_public_rows != len(rows):
        blockers.append("not_all_candidate_rows_joined_to_public_export")
    ready = not blockers
    return {
        "status": "CANDIDATE_ENTRY_JOIN_ROWS_READY" if ready else "CANDIDATE_ENTRY_JOIN_ROWS_FAIL_CLOSED",
        "ready": ready,
        "fixture": bool(obj.get("fixture")),
        "path": str(path),
        "row_count": len(rows),
        "joined_public_rows": joined_public_rows,
        "joined_accounts": accounts,
        "missing_fields": missing,
        "blockers": sorted(set(blockers)),
        "candidate_summary": {
            "artifact": obj.get("artifact"),
            "decision_label": obj.get("decision_label"),
            "created_utc": obj.get("created_utc"),
        },
    }


def public_missing_fields() -> list[str]:
    return [
        "per_trade_outcome",
        "per_trade_size",
        "per_trade_fee_usdc",
        "liquidity_role",
        "fair_probability",
        "fair_probability_uncertainty",
        "edge_after_fee_and_uncertainty",
        "source_count/source_names/source_dispersion_bps/max_source_age_ms_join",
        "volatility_lookback_s/volatility_bps",
    ]


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    spec = load_json(args.probability_spec_manifest)
    required_fields = required_fields_from_spec(spec)
    public_profiles = load_public_profiles(args.export_root, args)
    candidate_status = validate_candidate_rows(
        args.candidate_entry_join_rows,
        required_fields,
        public_condition_keys(args.export_root),
    )
    real_inputs_ready = bool(candidate_status.get("ready")) and not bool(candidate_status.get("fixture"))
    public_real_status = (
        "READY_WITH_NON_FIXTURE_ENTRY_JOIN_ROWS"
        if real_inputs_ready
        else "UNKNOWN_PUBLIC_ENTRY_JOIN_FIELDS_MISSING"
    )
    decision = "KEEP"
    label = "KEEP_B55_CE25_PUBLIC_ENTRY_JOIN_AUDIT_READY_REAL_INPUTS_UNKNOWN"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_public_entry_join_audit",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "public_profile_exports_only": True,
            "fixture_join_supported": True,
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
            "probability_spec_manifest": str(args.probability_spec_manifest),
            "export_root": str(args.export_root),
            "candidate_entry_join_rows": str(args.candidate_entry_join_rows)
            if args.candidate_entry_join_rows
            else None,
        },
        "probability_spec_summary": {
            "decision_label": spec.get("decision_label"),
            "real_input_status": (spec.get("research_ranking") or {}).get("real_input_status"),
        },
        "public_profile_joinability": public_profiles,
        "candidate_entry_join_status": candidate_status,
        "research_ranking": {
            "decision": decision,
            "label": label,
            "public_real_status": public_real_status,
            "real_inputs_ready": real_inputs_ready,
            "b55_role": "main fair-price/pair-edge template",
            "ce25_role": "clean low-residual control",
            "public_export_missing_fields": public_missing_fields(),
            "interpretation": (
                "Current public exports can identify BTC/ETH 15m/1h market-level entry proxies, but they cannot "
                "prove b55/ce25 fair-price entry edge because per-trade fee/liquidity role, volatility, and "
                "fair-probability source joins are absent. Fixture rows validate the join contract only."
            ),
            "blockers_before_shadow_or_deployable_claim": [
                "real_entry_join_rows_not_available" if not real_inputs_ready else "real_join_rows_available_but_public_proxy_only",
                "per_trade_fee_liquidity_role_missing_in_public_export",
                "fair_probability_source_join_missing",
                "volatility_uncertainty_input_missing",
                "private_truth_not_available",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_ENTRY_JOIN_AUDIT_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement b55_ce25_entry_row_source_gap_audit_v1: determine the smallest safe local export needed to "
            "produce per-trade public entry rows with fee/liquidity role and joinable fair source fields, without "
            "starting local agg/shared WS/shadow/live. If no existing safe export exists, return UNKNOWN and name "
            "the exact required exporter fields."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probability-spec-manifest", type=Path, default=DEFAULT_PROBABILITY_SPEC)
    parser.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--candidate-entry-join-rows", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--entry-delta-s-min", type=float, default=-900.0)
    parser.add_argument("--entry-delta-s-max", type=float, default=-60.0)
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
                "public_real_status": manifest["research_ranking"]["public_real_status"],
                "candidate_entry_join_status": manifest["candidate_entry_join_status"]["status"],
                "public_export_missing_fields": manifest["research_ranking"]["public_export_missing_fields"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
