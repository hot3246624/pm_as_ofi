#!/usr/bin/env python3
"""Emit a disabled BTC import-contract schema stub and preflight checklist.

BTC low_residual_core_pair_v1 is research-positive, but it is still not an
import package.  This script records the exact missing import, source,
capital-ledger, private-truth, and runner preflight fields in a local-only
artifact.  It intentionally writes no importable candidate rows and authorizes
nothing beyond colleague/schema review.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any


STAMP = "20260528T0934Z"

DEFAULT_BTC_CANARY_GATE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_btc_canary_readiness_gate_20260528T0922Z.json"
)
DEFAULT_SOURCE_CONTRACT = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_source_parity_private_truth_contract_20260528T0904Z.json"
)
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_btc_import_contract_stub_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_btc_import_contract_stub_{STAMP}"
)

FILTER_NAME = "low_residual_core_pair_v1"
FILTER_VERSION = "backtest_v1_20260528_btc_research_only"

RESEARCH_CANDIDATE_FIELDS = [
    "asset",
    "day",
    "condition_id",
    "slug",
    "first_action_ts_ms",
    "selected_seed_actions",
    "gross_buy_cost",
    "pair_pnl",
    "official_taker_fee",
    "core_pair_after_fee_pnl",
    "actual_settlement_residual_pnl_posthoc",
    "xuan_after_fee_pnl_including_residual_settlement",
    "market_end_residual_cost",
    "market_end_residual_qty",
    "residual_cost_share",
    "zero_stress_after_fee_pnl",
    "merge_recovered_capital",
    "capital_turnover",
    "avg_residual_age_s",
]

IMPORT_METADATA_FIELDS = [
    "filter_name",
    "filter_version",
    "source_dataset_fingerprint",
    "source_semantics_contract_id",
    "l2_top_overlay_contract_id",
    "rescore_manifest_fingerprint",
    "candidate_csv_sha256",
    "deterministic_candidate_rank",
    "runner_profile_id",
    "max_notional_cap",
    "dry_run_only",
    "import_enabled",
]

OWNER_PRIVATE_TRUTH_FIELDS = [
    "owner_wallet",
    "order_id",
    "client_order_id",
    "submitted_order_side",
    "submitted_order_price",
    "submitted_order_size",
    "fill_id",
    "fill_ts_ms",
    "fill_price",
    "fill_size",
    "fee_paid",
    "inventory_before",
    "inventory_after",
    "merge_or_redeem_tx",
    "settlement_or_redeem_payout",
    "owner_reconciled_pnl",
]


def fnum(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def clean(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: clean(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = fieldnames or (list(rows[0].keys()) if rows else [])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: clean(row.get(key)) for key in fields})


def field_rows(header: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in RESEARCH_CANDIDATE_FIELDS:
        rows.append(
            {
                "field": field,
                "field_group": "research_candidate",
                "required_before_import": True,
                "status": "AVAILABLE_RESEARCH_ONLY" if field in header else "MISSING_BLOCKER",
                "source": "btc_low_residual_candidate_csv",
                "notes": "Research metric only; does not authorize candidate import.",
            }
        )
    for field in IMPORT_METADATA_FIELDS:
        status = "REQUIRED_CONSTANT_TRUE_MISSING" if field == "dry_run_only" else "MISSING_BLOCKER"
        if field == "import_enabled":
            status = "REQUIRED_CONSTANT_FALSE_MISSING"
        rows.append(
            {
                "field": field,
                "field_group": "import_metadata",
                "required_before_import": True,
                "status": status,
                "source": "backtest_colleague_or_runner_preflight",
                "notes": "Must be populated before any import dry-run table exists.",
            }
        )
    for field in OWNER_PRIVATE_TRUTH_FIELDS:
        rows.append(
            {
                "field": field,
                "field_group": "future_owner_private_truth",
                "required_before_live_canary": True,
                "status": "FUTURE_SCHEMA_MISSING_BLOCKER",
                "source": "future_owner_canary_collector",
                "notes": "Historical V1/shadow data cannot populate owner private truth.",
            }
        )
    return rows


def preflight_rows(canary: dict[str, Any], source_contract: dict[str, Any], candidate_csv: Path) -> list[dict[str, Any]]:
    decision = body(canary, "decision")
    summary = body(canary, "btc_filter_summary")
    outputs = body(canary, "outputs")
    expected_sha = outputs.get("btc_candidates_csv_sha256")
    actual_sha = sha256(candidate_csv)
    source_decision = body(source_contract, "decision")
    return [
        {
            "gate": "btc_candidate_csv_fingerprint_fixed",
            "status": "PASS" if actual_sha and actual_sha == expected_sha else "BLOCKED",
            "evidence": actual_sha,
            "required_before_import": True,
        },
        {
            "gate": "btc_research_edge_ready",
            "status": "PASS" if decision.get("btc_research_edge_ready") else "BLOCKED",
            "evidence": {
                "core_pair_after_fee_pnl": summary.get("core_pair_after_fee_pnl"),
                "zero_stress_after_fee_pnl": summary.get("zero_stress_after_fee_pnl"),
                "residual_cost_share": summary.get("residual_cost_share"),
            },
            "required_before_import": True,
        },
        {
            "gate": "residual_settlement_excluded_as_strategy_edge",
            "status": "PASS",
            "evidence": "Use pair_pnl - official_taker_fee as edge; market_end_residual_cost is single-leg risk.",
            "required_before_import": True,
        },
        {
            "gate": "source_semantics_contract_id_present",
            "status": "BLOCKED",
            "evidence": {
                "source_contract_status": source_contract.get("status"),
                "recommended_direction": decision.get("recommended_source_semantics_direction"),
            },
            "required_before_import": True,
        },
        {
            "gate": "source_dataset_fingerprint_present",
            "status": "BLOCKED",
            "evidence": "No source_dataset_fingerprint is attached to the BTC candidate set.",
            "required_before_import": True,
        },
        {
            "gate": "l2_top_overlay_contract_id_present",
            "status": "BLOCKED",
            "evidence": "No L2 top-overlay contract id is attached to the BTC candidate set.",
            "required_before_import": True,
        },
        {
            "gate": "filter_specific_btc_capital_ledger_present",
            "status": "BLOCKED",
            "evidence": body(canary, "btc_capital_context"),
            "required_before_import": True,
        },
        {
            "gate": "import_contract_table_fields_complete",
            "status": "BLOCKED",
            "evidence": "Current BTC CSV has research fields only; import metadata fields are absent.",
            "required_before_import": True,
        },
        {
            "gate": "deterministic_candidate_rank_defined",
            "status": "BLOCKED",
            "evidence": "Rank order must be deterministic and tied to source/rescore fingerprints.",
            "required_before_import": True,
        },
        {
            "gate": "runner_profile_id_defined",
            "status": "BLOCKED",
            "evidence": "No runner/import profile exists for low_residual_core_pair_v1.",
            "required_before_import": True,
        },
        {
            "gate": "max_notional_cap_defined_from_filter_capital_ledger",
            "status": "BLOCKED",
            "evidence": "Sizing cannot use full-rescore BTC capital ledger.",
            "required_before_import": True,
        },
        {
            "gate": "dry_run_only_true_and_import_enabled_false",
            "status": "BLOCKED",
            "evidence": "Required constants are defined in this stub but no import table is emitted.",
            "required_before_import": True,
        },
        {
            "gate": "owner_private_truth_schema_ready",
            "status": "BLOCKED",
            "evidence": OWNER_PRIVATE_TRUTH_FIELDS,
            "required_before_live_canary": True,
        },
        {
            "gate": "runner_import_preflight_ready",
            "status": "BLOCKED",
            "evidence": "No active-runner conflict check, manifest, preauthorization, or orders_sent=false verification exists.",
            "required_before_import": True,
        },
        {
            "gate": "source_contract_research_csv_ready",
            "status": "PASS" if source_decision.get("research_csv_ready") else "BLOCKED",
            "evidence": source_decision.get("research_csv_ready"),
            "required_before_import": False,
        },
    ]


def schema_stub_header() -> list[str]:
    return RESEARCH_CANDIDATE_FIELDS + IMPORT_METADATA_FIELDS


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    missing_inputs = [str(path) for path in (args.btc_canary_gate, args.source_contract) if not path.exists()]
    if missing_inputs:
        return {
            "artifact": "xuan_shadow_review_backtest_v1_btc_import_contract_stub",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_BTC_IMPORT_CONTRACT_STUB_INPUTS_MISSING_LOCAL_ONLY",
            "created_utc": STAMP,
            "missing_inputs": missing_inputs,
            "decision": {
                "schema_stub_ready": False,
                "candidate_import_allowed": False,
                "remote_runner_allowed": False,
                "private_truth_ready": False,
            },
        }

    canary = load_json(args.btc_canary_gate)
    source_contract = load_json(args.source_contract)
    canary_outputs = body(canary, "outputs")
    candidate_csv = Path(str(canary_outputs.get("btc_candidates_csv") or ""))
    header = read_csv_header(candidate_csv)
    field_map = field_rows(header)
    preflight = preflight_rows(canary, source_contract, candidate_csv)
    hard_blockers = [row["gate"] for row in preflight if row.get("status") == "BLOCKED"]

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    schema_csv = args.artifact_dir / "BTC_IMPORT_CONTRACT_SCHEMA_STUB.csv"
    field_map_csv = args.artifact_dir / "BTC_IMPORT_CONTRACT_FIELD_MAP.csv"
    preflight_csv = args.artifact_dir / "BTC_IMPORT_PREFLIGHT_CHECKLIST.csv"
    write_csv(schema_csv, [], schema_stub_header())
    write_csv(field_map_csv, field_map)
    write_csv(preflight_csv, preflight)

    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_btc_import_contract_stub",
            "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_BTC_IMPORT_CONTRACT_STUB_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_btc_import_contract_stub.py",
            "inputs": {
                "btc_canary_gate": str(args.btc_canary_gate),
                "source_contract": str(args.source_contract),
                "btc_candidates_csv": str(candidate_csv),
                "btc_candidates_csv_sha256": sha256(candidate_csv),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "schema_stub_csv": str(schema_csv),
                "schema_stub_csv_sha256": sha256(schema_csv),
                "field_map_csv": str(field_map_csv),
                "field_map_csv_sha256": sha256(field_map_csv),
                "preflight_checklist_csv": str(preflight_csv),
                "preflight_checklist_csv_sha256": sha256(preflight_csv),
                "markdown": str(args.artifact_dir / "BTC_IMPORT_CONTRACT_STUB.md"),
            },
            "decision": {
                "schema_stub_ready": True,
                "preflight_checklist_ready": True,
                "btc_research_edge_ready": body(canary, "decision").get("btc_research_edge_ready"),
                "allowed_import_rows": 0,
                "candidate_import_allowed": False,
                "runner_support_ready": False,
                "manifest_or_preauthorization_ready": False,
                "future_remote_allowed_by_this_stub": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "next_lane": "wait_for_backtest_colleague_source_capital_import_truth_deliverables",
            },
            "contract_identity": {
                "filter_name": FILTER_NAME,
                "filter_version": FILTER_VERSION,
                "asset_scope": "BTC",
                "import_enabled_required_value": False,
                "dry_run_only_required_value": True,
            },
            "candidate_set": {
                "row_count": count_csv_rows(candidate_csv),
                "header": header,
                "candidate_csv_sha256": sha256(candidate_csv),
            },
            "schema": {
                "research_candidate_fields": RESEARCH_CANDIDATE_FIELDS,
                "import_metadata_fields": IMPORT_METADATA_FIELDS,
                "owner_private_truth_fields": OWNER_PRIVATE_TRUTH_FIELDS,
                "field_map": field_map,
            },
            "preflight_checklist": preflight,
            "hard_blockers_before_any_import": hard_blockers,
            "colleague_p0_deliverables": [
                "source_semantics_contract_id_and_source_dataset_fingerprint",
                "filter_specific_btc_capital_ledger_for_low_residual_core_pair_v1",
                "research_only_import_contract_table_with_schema_fields_and_dry_run_only_true",
                "owner_private_truth_future_canary_schema",
            ],
            "warnings": [
                "This artifact writes a schema stub only; it is not an importable candidate table.",
                "Do not use residual settlement PnL as strategy edge.",
                "Do not use historical V1/shadow outputs as owner private truth.",
                "Do not run remote, deploy, or send live orders from this stub.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    outputs = body(card, "outputs")
    candidate_set = body(card, "candidate_set")
    identity = body(card, "contract_identity")
    lines = [
        "# BTC Import Contract Stub",
        "",
        f"- status: `{card.get('status')}`",
        f"- schema_stub_ready: `{decision.get('schema_stub_ready')}`",
        f"- allowed_import_rows: `{decision.get('allowed_import_rows')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## Contract Identity",
        "",
        f"- filter_name: `{identity.get('filter_name')}`",
        f"- filter_version: `{identity.get('filter_version')}`",
        f"- asset_scope: `{identity.get('asset_scope')}`",
        f"- dry_run_only_required_value: `{identity.get('dry_run_only_required_value')}`",
        f"- import_enabled_required_value: `{identity.get('import_enabled_required_value')}`",
        "",
        "## Candidate Set",
        "",
        f"- row_count: `{candidate_set.get('row_count')}`",
        f"- candidate_csv_sha256: `{candidate_set.get('candidate_csv_sha256')}`",
        "",
        "## Outputs",
        "",
        f"- schema_stub_csv: `{outputs.get('schema_stub_csv')}`",
        f"- field_map_csv: `{outputs.get('field_map_csv')}`",
        f"- preflight_checklist_csv: `{outputs.get('preflight_checklist_csv')}`",
        "",
        "## Preflight Checklist",
        "",
        "| gate | status |",
        "| --- | --- |",
    ]
    for row in card.get("preflight_checklist", []):
        lines.append(f"| `{row.get('gate')}` | `{row.get('status')}` |")
    lines.extend(
        [
            "",
            "## Hard Blockers Before Any Import",
            "",
        ]
    )
    for blocker in card.get("hard_blockers_before_any_import", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Colleague P0 Deliverables",
            "",
        ]
    )
    for item in card.get("colleague_p0_deliverables", []):
        lines.append(f"- `{item}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- BTC research edge is strong enough to keep pushing, but this is still a disabled contract stub.",
            "- The next work item is to fill source, capital ledger, import metadata, and private-truth fields without changing safety boundaries.",
            "- No candidate import, runner profile, manifest, preauthorization, remote, deploy, or live order is authorized.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--btc-canary-gate", type=Path, default=DEFAULT_BTC_CANARY_GATE)
    parser.add_argument("--source-contract", type=Path, default=DEFAULT_SOURCE_CONTRACT)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if card.get("outputs"):
        markdown = Path(str(body(card, "outputs").get("markdown")))
        markdown.parent.mkdir(parents=True, exist_ok=True)
        markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
