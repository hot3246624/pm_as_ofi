#!/usr/bin/env python3
"""Build a fail-closed fixture exporter for b55/ce25 public activity rows.

This local-only tool converts already available public activity rows into the
entry export schema where fields are observable, then emits a fixture-only
candidate export with synthetic liquidity/fair-probability fields. It does not
fetch data, start services, connect shared ingress, run shadow, or touch
trading paths.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_public_activity_entry_exporter_fixture"
DEFAULT_PUBLIC_INPUT_ROOT = Path(
    "xuan_research_artifacts/"
    "xuan_public_leaderboard_candidate_fast_screen_20260524T035500Z/"
    "public_inputs"
)
DEFAULT_EXPORT_SPEC_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_public_activity_entry_export_spec_20260524T072245Z/"
    "manifest.json"
)
ACCOUNTS = {
    "b55": "0xb55fa1296e6ec55d0ce53d93b9237389f11764d4",
    "ce25": "0xce25e214d5cfe4f459cf67f08df581885aae7fdc",
}
REQUIRED_ASSETS = ("BTC", "ETH")
REQUIRED_TIMEFRAMES = ("15m", "1h_or_named")
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
REQUIRED_PUBLIC_FIELDS = (
    "transactionHash",
    "conditionId",
    "timestamp",
    "price",
    "size",
    "usdcSize",
    "outcome",
    "side",
    "type",
)
REAL_OBSERVABLE_FIELDS = (
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
)
SYNTHETIC_FIXTURE_FIELDS = (
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
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def as_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def as_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def required_fields_from_spec(spec: dict[str, Any]) -> list[str]:
    contract = spec.get("public_activity_entry_export_contract")
    if not isinstance(contract, dict):
        return []
    fields = contract.get("required_candidate_fields")
    return list(fields) if isinstance(fields, list) else []


def parse_interval_seconds(slug: str) -> int | None:
    match = re.search(r"-updown-(\d+)([mh])(?:-|$)", slug)
    if not match:
        return None
    n = int(match.group(1))
    return n * 60 if match.group(2) == "m" else n * 3600


def parse_market_start_end(slug: str) -> tuple[int | None, int | None]:
    interval = parse_interval_seconds(slug)
    if interval is None:
        return None, None
    try:
        start = int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None, None
    return start, start + interval


def infer_asset(slug: str, title: str) -> str:
    text = f"{slug} {title}".lower()
    if "btc-" in text or "bitcoin" in text:
        return "BTC"
    if "eth-" in text or "ethereum" in text:
        return "ETH"
    if "sol-" in text or "solana" in text:
        return "SOL"
    if "xrp" in text:
        return "XRP"
    return "unknown"


def infer_timeframe(slug: str, title: str) -> str:
    if "-updown-5m-" in slug:
        return "5m"
    if "-updown-15m-" in slug:
        return "15m"
    if "-updown-1h-" in slug:
        return "1h_or_named"
    lowered = title.lower()
    if "bitcoin up or down" in lowered or "ethereum up or down" in lowered:
        return "1h_or_named"
    match = re.search(r"-updown-(\d+[mh])-", slug)
    return match.group(1) if match else "unknown"


def normalize_outcome(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"up", "yes"}:
        return "YES"
    if text.lower() in {"down", "no"}:
        return "NO"
    return text or "unknown"


def conversion_rejection(reason: str, extra: dict[str, Any] | None = None) -> tuple[None, list[str], dict[str, Any]]:
    return None, [reason], extra or {}


def convert_public_activity_row(
    account: str,
    row: dict[str, Any],
    row_index: int,
    args: argparse.Namespace,
) -> tuple[dict[str, Any] | None, list[str], dict[str, Any]]:
    missing = [field for field in REQUIRED_PUBLIC_FIELDS if field not in row]
    if missing:
        return conversion_rejection("missing_public_activity_required_fields", {"missing_fields": missing})
    if str(row.get("type")) != "TRADE":
        return conversion_rejection("not_trade_activity")
    if str(row.get("side")) != "BUY":
        return conversion_rejection("not_buy_side")

    slug = str(row.get("slug") or row.get("eventSlug") or "")
    title = str(row.get("title") or "")
    asset = infer_asset(slug, title)
    timeframe = infer_timeframe(slug, title)
    if asset not in REQUIRED_ASSETS:
        return conversion_rejection("asset_not_in_b55_ce25_contract", {"asset": asset})
    if timeframe not in REQUIRED_TIMEFRAMES:
        return conversion_rejection("timeframe_not_in_b55_ce25_contract", {"timeframe": timeframe})

    market_start_ts, market_end_ts = parse_market_start_end(slug)
    if market_start_ts is None or market_end_ts is None:
        return conversion_rejection("market_metadata_unparseable", {"slug": slug})
    quote_ts = as_int(row.get("timestamp"))
    price = as_float(row.get("price"))
    size = as_float(row.get("size"))
    actual_usdc = as_float(row.get("usdcSize"))
    if quote_ts is None:
        return conversion_rejection("timestamp_unparseable")
    if price is None:
        return conversion_rejection("price_unparseable")
    if size is None or size <= 0:
        return conversion_rejection("size_nonpositive_or_unparseable")
    if actual_usdc is None:
        return conversion_rejection("usdc_size_unparseable")

    entry_delta_s = quote_ts - market_end_ts
    if not (args.entry_delta_s_min <= entry_delta_s <= args.entry_delta_s_max):
        return conversion_rejection("outside_entry_window", {"entry_delta_s": entry_delta_s})
    if not (args.price_min <= price <= args.price_max):
        return conversion_rejection("outside_price_habitat", {"price": price})

    gross_usdc = price * size
    fee_usdc = actual_usdc - gross_usdc
    if fee_usdc < -args.negative_fee_tolerance_usdc:
        return conversion_rejection(
            "fee_usdc_negative_beyond_tolerance",
            {"actual_usdc": actual_usdc, "gross_usdc": gross_usdc, "fee_usdc": fee_usdc},
        )

    tx = str(row.get("transactionHash") or "")
    condition_id = str(row.get("conditionId") or "")
    if not tx or not condition_id:
        return conversion_rejection("missing_trade_or_condition_id")

    out = {
        "account": account,
        "trade_id": f"{tx}#{row_index}",
        "condition_id": condition_id,
        "market_slug": slug,
        "asset": asset,
        "timeframe": timeframe,
        "market_start_ts": market_start_ts,
        "market_end_ts": market_end_ts,
        "quote_ts": quote_ts,
        "entry_delta_s": entry_delta_s,
        "time_to_expiry_s": market_end_ts - quote_ts,
        "outcome": normalize_outcome(row.get("outcome")),
        "polymarket_price": price,
        "size": size,
        "gross_usdc": round(gross_usdc, 12),
        "fee_usdc": round(fee_usdc, 12),
        "fee_per_share": round(fee_usdc / size, 12),
        "actual_usdc": actual_usdc,
        "source_public_row_index": row_index,
        "source_transaction_hash": tx,
        "source_proxy_wallet": row.get("proxyWallet"),
        "source_type": row.get("type"),
        "source_side": row.get("side"),
    }
    return out, [], {}


def load_public_rows(input_root: Path, args: argparse.Namespace) -> dict[str, Any]:
    if not path_safe(input_root):
        raise RuntimeError(f"unsafe public input root: {input_root}")
    results: dict[str, Any] = {}
    for account, wallet in ACCOUNTS.items():
        path = input_root / wallet / "activity_trade_rows.json"
        raw_rows = load_json(path) if path.exists() else []
        accepted: list[dict[str, Any]] = []
        rejections: Counter[str] = Counter()
        rejection_examples: dict[str, Any] = {}
        for idx, row in enumerate(raw_rows if isinstance(raw_rows, list) else []):
            converted, reasons, extra = convert_public_activity_row(account, row, idx, args)
            if converted is not None:
                accepted.append(converted)
                continue
            for reason in reasons:
                rejections[reason] += 1
                rejection_examples.setdefault(reason, extra)
        results[account] = {
            "path": str(path),
            "raw_count": len(raw_rows) if isinstance(raw_rows, list) else 0,
            "accepted_rows": accepted,
            "accepted_count": len(accepted),
            "rejections": dict(sorted(rejections.items())),
            "rejection_examples": rejection_examples,
        }
    return results


def account_balanced_sample(rows_by_account: dict[str, Any], max_rows_per_account: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for account in ACCOUNTS:
        accepted = rows_by_account.get(account, {}).get("accepted_rows") or []
        out.extend(accepted[:max_rows_per_account])
    return out


def synthetic_fixture_row(row: dict[str, Any], idx: int) -> dict[str, Any]:
    price = float(row["polymarket_price"])
    fee_per_share = max(0.0, float(row.get("fee_per_share") or 0.0))
    uncertainty = 0.015
    edge_target = 0.04 + (idx % 3) * 0.003
    fair_probability = min(0.99, price + fee_per_share + uncertainty + edge_target)
    edge = fair_probability - price - fee_per_share - uncertainty
    boundary_price = 100000.0 if row.get("asset") == "BTC" else 3500.0
    direction = 1.0 if row.get("outcome") == "YES" else -1.0
    out = dict(row)
    out.update(
        {
            "liquidity_role": "maker",
            "boundary_price": boundary_price,
            "fair_spot_mid": round(boundary_price * (1.0 + direction * 0.0008), 6),
            "source_count": 4,
            "source_names": ["binance", "coinbase", "okx", "bybit"],
            "source_dispersion_bps": 2.5 + (idx % 2) * 0.5,
            "max_source_age_ms": 450 + (idx % 5) * 20,
            "volatility_lookback_s": 300 if row.get("timeframe") == "15m" else 900,
            "volatility_bps": 18.0 if row.get("asset") == "BTC" else 22.0,
            "fair_probability": round(fair_probability, 12),
            "fair_probability_uncertainty": uncertainty,
            "edge_after_fee_and_uncertainty": round(edge, 12),
            "model_name": "fixture_entry_boundary_normal_probability",
            "model_version": "fixture-v1",
            "fixture_synthetic_fair_source": True,
        }
    )
    return out


def fixture_rows_from_public_rows(real_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [synthetic_fixture_row(row, idx) for idx, row in enumerate(real_rows)]


def candidate_export_manifest(
    *,
    path: Path,
    rows_path: Path,
    rows: list[dict[str, Any]],
    provided_fields: list[str],
    fixture: bool,
    ready: bool,
) -> dict[str, Any]:
    accounts = sorted({str(row.get("account")) for row in rows if row.get("account")})
    joins = {
        "public_activity_trade_rows": True,
        "market_metadata_boundary": True,
        "fee_usdc_derivation": True,
        "liquidity_role_truth_source": bool(fixture and ready),
        "fair_source_snapshot_rows": bool(fixture and ready),
        "volatility_rows": bool(fixture and ready),
        "probability_model_output": bool(fixture and ready),
    }
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b55_ce25_public_activity_entry_exporter_candidate_export",
        "created_utc": utc_label(),
        "decision_label": (
            "KEEP_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT_READY"
            if fixture and ready
            else "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING"
        ),
        "fixture": fixture,
        "real_input": not fixture,
        "row_count": len(rows),
        "rows_path": str(rows_path),
        "provided_fields": sorted(provided_fields),
        "accounts_covered": accounts,
        "joins_declared": joins,
        "fee_usdc_derivation_policy": {
            "actual_usdc_source": "usdcSize",
            "gross_usdc_formula": "polymarket_price * size",
            "fee_usdc_formula": "actual_usdc - gross_usdc",
            "fee_per_share_formula": "fee_usdc / size",
            "usdc_size_basis_declared": True,
        },
        "liquidity_role_truth_source": {
            "per_trade_role_join_declared": bool(fixture and ready),
            "uses_rebate_or_fee_inference": False,
            "fixture_synthetic_role": bool(fixture),
        },
        "fair_source_snapshot_policy": {
            "safe_export_only": True,
            "max_join_abs_delta_ms": 1000,
            "min_source_count": 3,
            "fixture_synthetic_source": bool(fixture),
        },
        "probability_model_output": {
            "probability_output_basis": "purchased_outcome_win_probability" if fixture else "missing_in_real_public_rows",
            "uncertainty_declared": bool(fixture and ready),
            "fixture_synthetic_probability": bool(fixture),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
        },
    }
    write_json(path, manifest)
    return manifest


def rows_file(path: Path, artifact: str, fixture: bool, rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_version": 1,
        "artifact": artifact,
        "created_utc": utc_label(),
        "fixture": fixture,
        "row_count": len(rows),
        "rows": rows,
    }
    write_json(path, payload)
    return payload


def fail_closed_probe_rows(sample: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    base = {
        "transactionHash": sample["source_transaction_hash"],
        "conditionId": sample["condition_id"],
        "slug": sample["market_slug"],
        "eventSlug": sample["market_slug"],
        "timestamp": sample["quote_ts"],
        "price": sample["polymarket_price"],
        "size": sample["size"],
        "usdcSize": sample["actual_usdc"],
        "outcome": sample["outcome"],
        "side": "BUY",
        "type": "TRADE",
        "title": "",
    }
    probes = {
        "negative_fee": dict(base, usdcSize=max(0.0, float(sample["gross_usdc"]) - 1.0)),
        "bad_slug_metadata": dict(base, slug="btc-updown-15m-notatime", eventSlug="btc-updown-15m-notatime"),
        "sell_side": dict(base, side="SELL"),
    }
    out: dict[str, Any] = {}
    for name, row in probes.items():
        converted, reasons, extra = convert_public_activity_row("b55", row, 0, args)
        out[name] = {
            "converted": converted is not None,
            "reasons": reasons,
            "extra": extra,
        }
    return out


def field_presence(rows: list[dict[str, Any]]) -> list[str]:
    fields: set[str] = set()
    for row in rows:
        fields.update(row)
    return sorted(fields)


def account_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get("account"))] += 1
    return dict(sorted(counts.items()))


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    spec = load_json(args.export_spec_manifest)
    required_fields = required_fields_from_spec(spec)
    rows_by_account = load_public_rows(args.public_input_root, args)
    real_rows = account_balanced_sample(rows_by_account, args.max_rows_per_account)
    fixture_rows = fixture_rows_from_public_rows(real_rows)

    real_rows_path = output_dir / "real_public_activity_entry_rows.json"
    fixture_rows_path = output_dir / "fixture_public_activity_entry_rows.json"
    real_manifest_path = output_dir / "candidate_real_export_manifest.json"
    fixture_manifest_path = output_dir / "candidate_fixture_export_manifest.json"
    rows_file(real_rows_path, "xuan_b55_ce25_public_activity_entry_exporter_real_rows", False, real_rows)
    rows_file(fixture_rows_path, "xuan_b55_ce25_public_activity_entry_exporter_fixture_rows", True, fixture_rows)
    real_provided = [field for field in required_fields if field in set(field_presence(real_rows))]
    fixture_provided = [field for field in required_fields if field in set(field_presence(fixture_rows))]
    real_manifest = candidate_export_manifest(
        path=real_manifest_path,
        rows_path=real_rows_path,
        rows=real_rows,
        provided_fields=real_provided,
        fixture=False,
        ready=False,
    )
    fixture_manifest = candidate_export_manifest(
        path=fixture_manifest_path,
        rows_path=fixture_rows_path,
        rows=fixture_rows,
        provided_fields=fixture_provided,
        fixture=True,
        ready=bool(fixture_rows) and set(required_fields).issubset(set(fixture_provided)),
    )

    conversion_summary = {
        account: {
            "public_input_path": info["path"],
            "raw_count": info["raw_count"],
            "accepted_count": info["accepted_count"],
            "rejections": info["rejections"],
            "top_rejection_examples": info["rejection_examples"],
        }
        for account, info in rows_by_account.items()
    }
    fail_closed = fail_closed_probe_rows(real_rows[0], args) if real_rows else {}
    fail_closed_ok = {
        "negative_fee_rejected": "fee_usdc_negative_beyond_tolerance" in fail_closed.get("negative_fee", {}).get("reasons", []),
        "bad_slug_rejected": "market_metadata_unparseable" in fail_closed.get("bad_slug_metadata", {}).get("reasons", []),
        "sell_side_rejected": "not_buy_side" in fail_closed.get("sell_side", {}).get("reasons", []),
    }
    fixture_ready = bool(fixture_manifest.get("fixture")) and set(required_fields).issubset(set(fixture_provided))
    real_missing = [field for field in required_fields if field not in set(real_provided)]
    decision = "KEEP" if fixture_ready and all(fail_closed_ok.values()) else "UNKNOWN"
    label = (
        "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_READY_REAL_STILL_UNKNOWN"
        if decision == "KEEP"
        else "UNKNOWN_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_INSUFFICIENT"
    )
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_public_activity_entry_exporter_fixture",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "public_profile_exports_only": True,
            "fixture_only_for_complete_export": True,
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
            "public_input_root": str(args.public_input_root),
            "export_spec_manifest": str(args.export_spec_manifest),
        },
        "conversion_summary": conversion_summary,
        "real_export": {
            "status": "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING",
            "row_count": len(real_rows),
            "account_counts": account_counts(real_rows),
            "provided_fields": real_provided,
            "missing_fields": real_missing,
            "rows_path": str(real_rows_path),
            "candidate_manifest_path": str(real_manifest_path),
            "candidate_manifest_decision_label": real_manifest.get("decision_label"),
        },
        "fixture_export": {
            "status": "READY_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT",
            "row_count": len(fixture_rows),
            "account_counts": account_counts(fixture_rows),
            "provided_fields": fixture_provided,
            "rows_path": str(fixture_rows_path),
            "candidate_manifest_path": str(fixture_manifest_path),
            "candidate_manifest_decision_label": fixture_manifest.get("decision_label"),
            "synthetic_fields": list(SYNTHETIC_FIXTURE_FIELDS),
        },
        "fail_closed_probes": {
            "checks": fail_closed_ok,
            "cases": fail_closed,
        },
        "research_ranking": {
            "decision": decision,
            "label": label,
            "real_export_status": "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING",
            "fixture_export_ready": fixture_ready,
            "real_rows_are_strategy_evidence": False,
            "fixture_rows_are_strategy_evidence": False,
            "interpretation": (
                "Already-available public activity rows can be normalized into b55/ce25 entry rows with fee "
                "derivation and market timing, but real evaluation remains blocked by liquidity_role truth and "
                "fair-source probability joins. Fixture rows only validate the schema and scorer contract."
            ),
            "blockers_before_shadow_or_deployable_claim": [
                "non_fixture_liquidity_role_truth_missing",
                "non_fixture_fair_source_snapshot_join_missing",
                "non_fixture_probability_model_output_missing",
                "private_truth_not_available",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement b55_ce25_public_activity_entry_scorer_v1 locally: run the export spec validator over the "
            "real and fixture manifests, report real UNKNOWN versus fixture READY, and define the exact non-fixture "
            "liquidity_role/fair-source inputs needed before any no-order diagnostic."
        ),
    }
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-input-root", type=Path, default=DEFAULT_PUBLIC_INPUT_ROOT)
    parser.add_argument("--export-spec-manifest", type=Path, default=DEFAULT_EXPORT_SPEC_MANIFEST)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--entry-delta-s-min", type=float, default=-900.0)
    parser.add_argument("--entry-delta-s-max", type=float, default=-60.0)
    parser.add_argument("--price-min", type=float, default=0.35)
    parser.add_argument("--price-max", type=float, default=0.90)
    parser.add_argument("--negative-fee-tolerance-usdc", type=float, default=0.000001)
    parser.add_argument("--max-rows-per-account", type=int, default=12)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    manifest = build_manifest(args, output_dir)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "real_export_status": manifest["research_ranking"]["real_export_status"],
                "real_row_count": manifest["real_export"]["row_count"],
                "fixture_row_count": manifest["fixture_export"]["row_count"],
                "fail_closed_checks": manifest["fail_closed_probes"]["checks"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
