#!/usr/bin/env python3
"""Score b55/ce25 public activity entry exports against the fail-closed contract.

This local-only scorer consumes committed exporter/spec artifacts. It reports
the current real public export as UNKNOWN and fixture export as READY, while
making the missing non-fixture inputs explicit before any diagnostic run can be
considered. It does not fetch data, start services, run shadow, or touch live
trading paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b55_ce25_public_activity_entry_scorer"
DEFAULT_EXPORTER_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_public_activity_entry_exporter_fixture_20260524T075245Z/"
    "manifest.json"
)
DEFAULT_SPEC_REAL_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_public_activity_entry_exporter_fixture_smoke_20260524T075245Z/"
    "spec_real/manifest.json"
)
DEFAULT_SPEC_FIXTURE_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_public_activity_entry_exporter_fixture_smoke_20260524T075245Z/"
    "spec_fixture/manifest.json"
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
REQUIRED_ACCOUNTS = ("b55", "ce25")
CRITICAL_NON_FIXTURE_INPUTS = (
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
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def safe_load(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path_safe(path):
        return None, "unsafe_path"
    try:
        obj = load_json(path)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def candidate_status(spec_manifest: dict[str, Any]) -> dict[str, Any]:
    status = spec_manifest.get("candidate_export_manifest_status")
    return status if isinstance(status, dict) else {}


def research_status(spec_manifest: dict[str, Any]) -> dict[str, Any]:
    status = spec_manifest.get("research_ranking")
    return status if isinstance(status, dict) else {}


def account_counts_ok(counts: dict[str, Any]) -> bool:
    return all(int(counts.get(account, 0)) > 0 for account in REQUIRED_ACCOUNTS)


def build_score(
    exporter: dict[str, Any],
    spec_real: dict[str, Any],
    spec_fixture: dict[str, Any],
) -> dict[str, Any]:
    real_export = exporter.get("real_export") if isinstance(exporter.get("real_export"), dict) else {}
    fixture_export = exporter.get("fixture_export") if isinstance(exporter.get("fixture_export"), dict) else {}
    real_candidate = candidate_status(spec_real)
    fixture_candidate = candidate_status(spec_fixture)
    real_missing = list(real_export.get("missing_fields") or [])
    spec_real_missing = list(real_candidate.get("missing_fields") or [])
    fixture_missing = list(fixture_candidate.get("missing_fields") or [])

    checks = {
        "exporter_fixture_keep": exporter.get("decision_label")
        == "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_EXPORTER_FIXTURE_READY_REAL_STILL_UNKNOWN",
        "real_status_unknown": real_export.get("status")
        == "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING",
        "real_rows_present": int(real_export.get("row_count") or 0) > 0,
        "real_rows_cover_b55_ce25": account_counts_ok(real_export.get("account_counts") or {}),
        "real_spec_fail_closed": real_candidate.get("status")
        == "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_FAIL_CLOSED",
        "real_spec_missing_critical_inputs": set(CRITICAL_NON_FIXTURE_INPUTS).issubset(set(spec_real_missing)),
        "fixture_status_ready": fixture_export.get("status") == "READY_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT",
        "fixture_rows_present": int(fixture_export.get("row_count") or 0) > 0,
        "fixture_rows_cover_b55_ce25": account_counts_ok(fixture_export.get("account_counts") or {}),
        "fixture_spec_ready": fixture_candidate.get("status") == "CANDIDATE_PUBLIC_ACTIVITY_ENTRY_EXPORT_MANIFEST_READY",
        "fixture_spec_missing_no_fields": not fixture_missing,
        "fixture_not_real_input": research_status(spec_fixture).get("real_export_status")
        == "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_MISSING",
        "promotion_never_passes": not bool(exporter.get("promotion_gate", {}).get("passed"))
        and not bool(spec_real.get("promotion_gate", {}).get("passed"))
        and not bool(spec_fixture.get("promotion_gate", {}).get("passed")),
        "no_side_effect_scope": not bool(exporter.get("scope", {}).get("shadow_started"))
        and not bool(exporter.get("scope", {}).get("orders_sent")),
    }
    blockers = [name for name, ok in checks.items() if not ok]
    decision = "KEEP" if not blockers else "UNKNOWN"
    label = (
        "KEEP_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_READY_REAL_UNKNOWN_FIXTURE_READY"
        if decision == "KEEP"
        else "UNKNOWN_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_INPUTS_INSUFFICIENT"
    )
    return {
        "decision": decision,
        "decision_label": label,
        "checks": checks,
        "blockers": blockers,
        "real_score": {
            "status": "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_REQUIRED_JOINS_MISSING",
            "row_count": int(real_export.get("row_count") or 0),
            "account_counts": real_export.get("account_counts") or {},
            "provided_fields": real_export.get("provided_fields") or [],
            "missing_fields": real_missing,
            "spec_validator_status": real_candidate.get("status"),
            "spec_validator_missing_fields": spec_real_missing,
            "strategy_evidence": False,
        },
        "fixture_score": {
            "status": "READY_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT_NOT_REAL_INPUT",
            "row_count": int(fixture_export.get("row_count") or 0),
            "account_counts": fixture_export.get("account_counts") or {},
            "provided_fields": fixture_export.get("provided_fields") or [],
            "spec_validator_status": fixture_candidate.get("status"),
            "spec_validator_missing_fields": fixture_missing,
            "strategy_evidence": False,
        },
        "required_non_fixture_inputs_before_no_order_diagnostic": [
            {
                "field": "liquidity_role",
                "accepted_sources": [
                    "explicit public maker/taker field",
                    "documented per-trade takerOnly field",
                    "verified non-private fill metadata",
                ],
                "rejected_sources": ["maker rebate", "fee magnitude", "price improvement", "account-level taker share"],
            },
            {
                "field": "fair_source_snapshot_join",
                "required_fields": [
                    "boundary_price",
                    "fair_spot_mid",
                    "source_count",
                    "source_names",
                    "source_dispersion_bps",
                    "max_source_age_ms",
                ],
                "join_tolerance_ms": 1500,
                "source_policy": "safe pre-existing export only; no service/shared-WS/local-agg start",
            },
            {
                "field": "volatility_and_probability_model",
                "required_fields": [
                    "volatility_lookback_s",
                    "volatility_bps",
                    "fair_probability",
                    "fair_probability_uncertainty",
                    "edge_after_fee_and_uncertainty",
                    "model_name",
                    "model_version",
                ],
                "edge_formula": "fair_probability - polymarket_price - fee_usdc / size - fair_probability_uncertainty",
            },
        ],
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    loaded: dict[str, Any] = {}
    load_errors: dict[str, str] = {}
    for name, path in (
        ("exporter_manifest", args.exporter_manifest),
        ("spec_real_manifest", args.spec_real_manifest),
        ("spec_fixture_manifest", args.spec_fixture_manifest),
    ):
        obj, err = safe_load(path)
        if err:
            load_errors[name] = err
        else:
            loaded[name] = obj
    if load_errors:
        score = {
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_B55_CE25_PUBLIC_ACTIVITY_ENTRY_SCORER_INPUTS_INSUFFICIENT",
            "checks": {},
            "blockers": sorted(load_errors),
            "real_score": {},
            "fixture_score": {},
            "required_non_fixture_inputs_before_no_order_diagnostic": [],
        }
    else:
        score = build_score(
            loaded["exporter_manifest"],
            loaded["spec_real_manifest"],
            loaded["spec_fixture_manifest"],
        )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "b55_ce25_public_activity_entry_scorer",
        "decision": score["decision"],
        "decision_label": score["decision_label"],
        "scope": {
            "local_only": True,
            "public_profile_exports_only": True,
            "fixture_inputs_allowed_for_schema_only": True,
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
            "exporter_manifest": str(args.exporter_manifest),
            "spec_real_manifest": str(args.spec_real_manifest),
            "spec_fixture_manifest": str(args.spec_fixture_manifest),
            "load_errors": load_errors,
        },
        "score": score,
        "research_ranking": {
            "decision": score["decision"],
            "label": score["decision_label"],
            "real_export_status": score.get("real_score", {}).get(
                "status", "UNKNOWN_REAL_PUBLIC_ACTIVITY_ENTRY_EXPORT_NOT_SCORED"
            ),
            "fixture_export_status": score.get("fixture_score", {}).get(
                "status", "UNKNOWN_FIXTURE_PUBLIC_ACTIVITY_ENTRY_EXPORT_NOT_SCORED"
            ),
            "b55_role": "main fair-price/pair-edge template",
            "ce25_role": "clean low-residual control",
            "interpretation": (
                "The public activity entry pipeline is mechanically ready for fixtures and fail-closes real rows. "
                "It is not strategy evidence until non-fixture liquidity role and fair-probability source joins exist."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_ACTIVITY_ENTRY_SCORER_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Implement b55_ce25_non_fixture_input_source_review_v1 locally: determine whether any safe existing "
            "non-service export can provide per-trade liquidity_role and fair-source probability joins; otherwise "
            "return UNKNOWN and stop before any no-order diagnostic."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exporter-manifest", type=Path, default=DEFAULT_EXPORTER_MANIFEST)
    parser.add_argument("--spec-real-manifest", type=Path, default=DEFAULT_SPEC_REAL_MANIFEST)
    parser.add_argument("--spec-fixture-manifest", type=Path, default=DEFAULT_SPEC_FIXTURE_MANIFEST)
    parser.add_argument("--output-dir", type=Path)
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
                "fixture_export_status": manifest["research_ranking"]["fixture_export_status"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
