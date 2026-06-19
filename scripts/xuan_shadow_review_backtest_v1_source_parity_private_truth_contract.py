#!/usr/bin/env python3
"""Define the gate contract before Backtest V1 filter candidates can advance.

The low-residual Backtest V1 filter package is useful for research, but it is
not an import, runner, or live-trading artifact.  This script records the exact
source/parity/private-truth and runner/import requirements that must clear
before any such escalation.  It only reads local artifacts and writes a local
scorecard/markdown report.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


STAMP = "20260528T0904Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"

DEFAULT_STRATEGY_REVIEW = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_strategy_readiness_review_20260528T0754Z.json"
)
DEFAULT_FILTER_PACKAGE = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_low_residual_filter_package_20260528T0834Z.json"
)
DEFAULT_BTC_PARITY = CONTRACT_ROOT / "backtest_v1_btc_parity_latest/BACKTEST_V1_BTC_PARITY_GATE.json"
DEFAULT_XUAN_BRIDGE = CONTRACT_ROOT / "xuan_bridge_scorecard_latest/XUAN_BRIDGE_SCORECARD_MANIFEST.json"
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_source_parity_private_truth_contract_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_source_parity_private_truth_contract_{STAMP}/"
    "BACKTEST_V1_SOURCE_PARITY_PRIVATE_TRUTH_CONTRACT.md"
)


RESEARCH_CSV_REQUIRED_FIELDS = [
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
    "market_end_residual_cost",
    "market_end_residual_qty",
    "residual_cost_share",
    "zero_stress_after_fee_pnl",
    "merge_recovered_capital",
    "capital_turnover",
]

IMPORT_CONTRACT_REQUIRED_FIELDS = RESEARCH_CSV_REQUIRED_FIELDS + [
    "filter_name",
    "filter_version",
    "source_dataset_fingerprint",
    "source_semantics_contract_id",
    "l2_top_overlay_contract_id",
    "rescore_manifest_fingerprint",
    "deterministic_candidate_rank",
    "runner_profile_id",
    "max_notional_cap",
    "dry_run_only",
]

OWNER_PRIVATE_TRUTH_REQUIRED_FIELDS = [
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


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


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


def field_status(header: list[str], fields: list[str]) -> dict[str, Any]:
    present = [field for field in fields if field in header]
    missing = [field for field in fields if field not in header]
    return {
        "required_count": len(fields),
        "present_count": len(present),
        "missing_count": len(missing),
        "present": present,
        "missing": missing,
        "complete": not missing,
    }


def checklist(strategy: dict[str, Any], package: dict[str, Any], parity: dict[str, Any], bridge: dict[str, Any], import_fields: dict[str, Any]) -> list[dict[str, Any]]:
    strategy_decision = body(strategy, "decision")
    package_decision = body(package, "decision")
    bridge_summary = body(bridge, "summary")
    parity_summary = body(parity, "summary")
    return [
        {
            "gate": "filter_package_ready_for_local_review",
            "status": "PASS" if package_decision.get("filter_package_ready_for_local_review") else "BLOCKED",
            "evidence": package.get("status"),
            "required_to_advance": True,
        },
        {
            "gate": "metric_boundaries_enforced",
            "status": "PASS" if strategy_decision.get("residual_settlement_not_used_as_strategy_edge") else "BLOCKED",
            "evidence": "core edge is pair_pnl - official_taker_fee; residual settlement is posthoc",
            "required_to_advance": True,
        },
        {
            "gate": "research_csv_has_filter_fields",
            "status": "PASS" if import_fields["research_fields"]["complete"] else "BLOCKED",
            "evidence": import_fields["research_fields"],
            "required_to_advance": True,
        },
        {
            "gate": "btc_source_parity_or_canonical_source_semantics",
            "status": "BLOCKED",
            "evidence": {
                "btc_parity_status": parity.get("status"),
                "blockers": parity.get("blockers") or [],
                "source_semantics": parity.get("source_semantics_explanation") or {},
            },
            "required_to_advance": True,
        },
        {
            "gate": "xuan_bridge_complete_for_import_contract",
            "status": "BLOCKED",
            "evidence": {
                "xuan_bridge_status": bridge.get("status"),
                "xuan_compatible_bridge_count": bridge_summary.get("xuan_compatible_bridge_count"),
                "private_truth_ready": bridge_summary.get("private_truth_ready"),
                "queue_pnl_is_strategy_pnl": bridge_summary.get("queue_pnl_is_strategy_pnl"),
            },
            "required_to_advance": True,
        },
        {
            "gate": "import_contract_fields_complete",
            "status": "BLOCKED" if not import_fields["import_fields"]["complete"] else "PASS",
            "evidence": import_fields["import_fields"],
            "required_to_advance": True,
        },
        {
            "gate": "owner_private_truth_handoff_defined_and_populated",
            "status": "BLOCKED",
            "evidence": {
                "private_truth_ready": parity_summary.get("private_truth_ready"),
                "required_future_fields": OWNER_PRIVATE_TRUTH_REQUIRED_FIELDS,
            },
            "required_to_advance": True,
        },
        {
            "gate": "runner_import_contract_dry_run_only",
            "status": "BLOCKED",
            "evidence": "No runner profile, import contract, active-runner conflict gate, or manifest/preauthorization exists for this filter.",
            "required_to_advance": True,
        },
    ]


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    inputs = {
        "strategy_review": args.strategy_review,
        "filter_package": args.filter_package,
        "btc_parity": args.btc_parity,
        "xuan_bridge": args.xuan_bridge,
    }
    missing = [name for name, path in inputs.items() if not path.exists()]
    if missing:
        return {
            "artifact": "xuan_shadow_review_backtest_v1_source_parity_private_truth_contract",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_SOURCE_PARITY_PRIVATE_TRUTH_CONTRACT_INPUTS_MISSING_LOCAL_ONLY",
            "created_utc": STAMP,
            "missing_inputs": missing,
            "inputs": {name: str(path) for name, path in inputs.items()},
            "decision": {
                "contract_ready_for_local_review": False,
                "candidate_import_allowed": False,
                "remote_runner_allowed": False,
                "private_truth_ready": False,
            },
        }

    strategy = load_json(args.strategy_review)
    package = load_json(args.filter_package)
    parity = load_json(args.btc_parity)
    bridge = load_json(args.xuan_bridge)
    outputs = body(package, "outputs")
    primary_csv = Path(str(outputs.get("primary_btc_eth_candidates_csv") or ""))
    holdout_csv = Path(str(outputs.get("other_asset_holdout_candidates_csv") or ""))
    header = read_csv_header(primary_csv)
    import_fields = {
        "research_fields": field_status(header, RESEARCH_CSV_REQUIRED_FIELDS),
        "import_fields": field_status(header, IMPORT_CONTRACT_REQUIRED_FIELDS),
        "owner_private_truth_fields": {
            "required_count": len(OWNER_PRIVATE_TRUTH_REQUIRED_FIELDS),
            "present_count": 0,
            "missing_count": len(OWNER_PRIVATE_TRUTH_REQUIRED_FIELDS),
            "present": [],
            "missing": OWNER_PRIVATE_TRUTH_REQUIRED_FIELDS,
            "complete": False,
        },
    }
    checks = checklist(strategy, package, parity, bridge, import_fields)
    hard_blockers = [item["gate"] for item in checks if item["status"] == "BLOCKED"]

    return {
        "artifact": "xuan_shadow_review_backtest_v1_source_parity_private_truth_contract",
        "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_SOURCE_PARITY_PRIVATE_TRUTH_CONTRACT_READY_LOCAL_ONLY",
        "created_utc": STAMP,
        "script": "scripts/xuan_shadow_review_backtest_v1_source_parity_private_truth_contract.py",
        "inputs": {name: str(path) for name, path in inputs.items()},
        "candidate_sources": {
            "primary_btc_eth_csv": str(primary_csv),
            "primary_btc_eth_row_count": count_csv_rows(primary_csv),
            "other_asset_holdout_csv": str(holdout_csv),
            "other_asset_holdout_row_count": count_csv_rows(holdout_csv),
        },
        "decision": {
            "contract_ready_for_local_review": True,
            "filter_package_ready_for_local_review": body(package, "decision").get("filter_package_ready_for_local_review"),
            "research_csv_ready": import_fields["research_fields"]["complete"],
            "candidate_import_allowed": False,
            "runner_support_ready": False,
            "manifest_or_preauthorization_ready": False,
            "future_remote_allowed_by_this_contract": False,
            "remote_runner_allowed": False,
            "strategy_promotion_ready": False,
            "private_truth_ready": False,
            "deployable": False,
            "live_orders_allowed": False,
            "next_lane": "resolve_btc_source_semantics_or_accept_canonical_v1_then_define_import_contract_schema",
        },
        "field_contract": {
            "research_csv_header": header,
            "research_csv_required_fields": RESEARCH_CSV_REQUIRED_FIELDS,
            "import_contract_required_fields": IMPORT_CONTRACT_REQUIRED_FIELDS,
            "owner_private_truth_required_fields": OWNER_PRIVATE_TRUTH_REQUIRED_FIELDS,
            "coverage": import_fields,
        },
        "source_parity_private_truth_checklist": checks,
        "hard_blockers_before_import_or_remote": hard_blockers,
        "minimum_deliverables_before_import_or_remote": [
            {
                "deliverable": "btc_source_semantics_resolution",
                "acceptable_outcomes": [
                    "prove parity between old BTC baseline and V1 normalized source contract",
                    "or explicitly accept V1 normalized BUY adapter as the new canonical source contract and retire old-baseline parity as a promotion blocker",
                ],
            },
            {
                "deliverable": "xuan_bridge_import_contract",
                "required_content": [
                    "deterministic candidate identity and rank",
                    "filter/version/source fingerprints",
                    "L2 top overlay contract id",
                    "metric boundary policy",
                    "runner dry-run/notional caps",
                    "no live-order path",
                ],
            },
            {
                "deliverable": "owner_private_truth_canary_schema",
                "required_content": OWNER_PRIVATE_TRUTH_REQUIRED_FIELDS,
            },
            {
                "deliverable": "runner_or_import_preflight_gate",
                "required_content": [
                    "candidate CSV checksum",
                    "active-runner conflict check",
                    "PM_DRY_RUN enforcement",
                    "orders_sent=false verification",
                    "manifest/preauthorization artifact",
                ],
            },
        ],
        "source_status": {
            "strategy_review_status": strategy.get("status"),
            "filter_package_status": package.get("status"),
            "btc_parity_status": parity.get("status"),
            "xuan_bridge_status": bridge.get("status"),
        },
        "warnings": [
            "research_csv_is_not_candidate_import_authorization",
            "owner_private_truth_cannot_be_backfilled_from_historical_shadow_or_v1",
            "btc_parity_or_canonical_source_semantics_must_be_resolved_before_promotion",
            "do_not_use_residual_settlement_pnl_as_strategy_edge",
        ],
    }


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    sources = body(card, "candidate_sources")
    lines = [
        "# Backtest V1 Source/Parity/Private-Truth Contract",
        "",
        f"- status: `{card.get('status')}`",
        f"- created_utc: `{card.get('created_utc')}`",
        f"- contract_ready_for_local_review: `{decision.get('contract_ready_for_local_review')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- remote_runner_allowed: `{decision.get('remote_runner_allowed')}`",
        "",
        "## Candidate Sources",
        "",
        f"- primary_btc_eth_csv: `{sources.get('primary_btc_eth_csv')}`",
        f"- primary_btc_eth_row_count: `{sources.get('primary_btc_eth_row_count')}`",
        f"- other_asset_holdout_csv: `{sources.get('other_asset_holdout_csv')}`",
        f"- other_asset_holdout_row_count: `{sources.get('other_asset_holdout_row_count')}`",
        "",
        "## Checklist",
        "",
        "| gate | status | required |",
        "| --- | --- | --- |",
    ]
    for item in card.get("source_parity_private_truth_checklist", []):
        lines.append(
            f"| `{item.get('gate')}` | `{item.get('status')}` | `{item.get('required_to_advance')}` |"
        )
    lines.extend(
        [
            "",
            "## Hard Blockers",
            "",
        ]
    )
    for blocker in card.get("hard_blockers_before_import_or_remote", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Minimum Deliverables",
            "",
        ]
    )
    for item in card.get("minimum_deliverables_before_import_or_remote", []):
        lines.append(f"- `{item.get('deliverable')}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The low-residual BTC/ETH CSV is a research candidate set, not an import package.",
            "- Current CSV fields cover research review, but not runner/import provenance or owner private truth.",
            "- The next blocker to resolve is source semantics/parity or explicit acceptance of V1 normalized source semantics as canonical.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-review", type=Path, default=DEFAULT_STRATEGY_REVIEW)
    parser.add_argument("--filter-package", type=Path, default=DEFAULT_FILTER_PACKAGE)
    parser.add_argument("--btc-parity", type=Path, default=DEFAULT_BTC_PARITY)
    parser.add_argument("--xuan-bridge", type=Path, default=DEFAULT_XUAN_BRIDGE)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()

    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
