#!/usr/bin/env python3
"""Review the BTC tiny-canary preflight package from Backtest V1.

This is a xuan-side local acceptance review.  It verifies that the colleague
deliverables satisfy the research preflight contract for
btc_same_window_residual_share_le_3pct_v1 while preserving the hard boundary:
no import, no executable runner, no remote, no deploy, no live orders, and no
owner private truth data are authorized by this artifact.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any


STAMP = "20260528T1609Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"
PREFLIGHT_ROOT = CONTRACT_ROOT / "btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest"

DEFAULT_PREFLIGHT_MANIFEST = (
    PREFLIGHT_ROOT / "BTC_SAME_WINDOW_RESIDUAL_SHARE_LE_3PCT_V1_CANARY_PREFLIGHT_MANIFEST.json"
)
DEFAULT_PREFLIGHT_CHECKLIST = PREFLIGHT_ROOT / "preflight_checklist.json"
DEFAULT_SOURCE_CONTRACT = PREFLIGHT_ROOT / "source_semantics_contract.json"
DEFAULT_CAPITAL_LEDGER = PREFLIGHT_ROOT / "filter_capital_ledger.json"
DEFAULT_IMPORT_CONTRACT = PREFLIGHT_ROOT / "research_only_import_contract.csv"
DEFAULT_OWNER_TRUTH_SCHEMA = PREFLIGHT_ROOT / "owner_private_truth_schema.json"
DEFAULT_MICROSTRUCTURE = PREFLIGHT_ROOT / "microstructure_feasibility.csv"
DEFAULT_ACCEPTANCE_AUDIT = (
    CONTRACT_ROOT
    / "xuan_backtest_v1_btc_tiny_canary_acceptance_audit_latest"
    / "XUAN_BACKTEST_V1_BTC_TINY_CANARY_ACCEPTANCE_AUDIT.json"
)
DEFAULT_SHADOW_READINESS = (
    CONTRACT_ROOT
    / "xuan_same_window_shadow_readiness_gate_latest"
    / "XUAN_SAME_WINDOW_SHADOW_READINESS_GATE.json"
)
DEFAULT_SHADOW_START_SPEC = (
    CONTRACT_ROOT
    / "xuan_same_window_shadow_start_preflight_spec_latest"
    / "XUAN_SAME_WINDOW_SHADOW_START_PREFLIGHT_SPEC.json"
)
DEFAULT_RUNNER_DRAFT = (
    CONTRACT_ROOT
    / "xuan_same_window_no_order_shadow_runner_config_draft_latest"
    / "XUAN_SAME_WINDOW_NO_ORDER_SHADOW_RUNNER_CONFIG_DRAFT_VALIDATION.json"
)
DEFAULT_PRODUCTIZATION_REQUEST = (
    CONTRACT_ROOT
    / "xuan_backtest_v1_shadow_design_productization_request_latest"
    / "XUAN_BACKTEST_V1_SHADOW_DESIGN_PRODUCTIZATION_REQUEST.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/"
    f"xuan_shadow_review_backtest_v1_btc_tiny_canary_preflight_acceptance_review_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_btc_tiny_canary_preflight_acceptance_review_{STAMP}"
)

FILTER_NAME = "btc_same_window_residual_share_le_3pct_v1"
FILTER_VERSION = "v1"
EXPECTED_ASSET = "BTC"
EXPECTED_COUNT = 52
EXPECTED_MAX_CAP = 25.0

REQUIRED_IMPORT_FIELDS = [
    "filter_name",
    "filter_version",
    "asset",
    "day",
    "condition_id",
    "slug",
    "candidate_rank",
    "deterministic_candidate_id",
    "source_semantics_contract_id",
    "source_dataset_fingerprint",
    "l2_top_overlay_contract_id",
    "rescore_manifest_fingerprint",
    "capital_ledger_fingerprint",
    "runner_profile_id",
    "max_notional_cap",
    "dry_run_only",
    "import_enabled",
    "candidate_import_allowed",
    "live_orders_allowed",
    "deployable",
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


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def bool_text(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def all_equal(values: list[Any], expected: Any) -> bool:
    return all(str(value) == str(expected) for value in values)


def import_contract_review(
    path: Path,
    source: dict[str, Any],
    ledger: dict[str, Any],
) -> dict[str, Any]:
    rows = read_csv(path)
    header = list(rows[0].keys()) if rows else []
    missing = [field for field in REQUIRED_IMPORT_FIELDS if field not in header]
    ranks = sorted(int(fnum(row.get("candidate_rank"))) for row in rows)
    unique_ids = {row.get("deterministic_candidate_id") for row in rows}
    runner_ids = {row.get("runner_profile_id") for row in rows}
    source_id = source.get("source_semantics_contract_id")
    source_fingerprint = source.get("source_dataset_fingerprint")
    overlay_id = source.get("l2_top_overlay_contract_id")
    ledger_fingerprint = ledger.get("capital_ledger_fingerprint")
    max_caps = [fnum(row.get("max_notional_cap")) for row in rows]
    return clean(
        {
            "path": str(path),
            "sha256": sha256(path),
            "row_count": len(rows),
            "header": header,
            "missing_fields": missing,
            "filter_name_all_match": all_equal([row.get("filter_name") for row in rows], FILTER_NAME),
            "filter_version_all_match": all_equal([row.get("filter_version") for row in rows], FILTER_VERSION),
            "asset_all_btc": all_equal([row.get("asset") for row in rows], EXPECTED_ASSET),
            "candidate_rank_contiguous": ranks == list(range(1, len(rows) + 1)),
            "deterministic_candidate_id_unique": len(unique_ids) == len(rows),
            "runner_profile_ids": sorted(runner_ids),
            "runner_profile_id_present": bool(runner_ids and "" not in runner_ids),
            "source_semantics_contract_id_all_match": all_equal(
                [row.get("source_semantics_contract_id") for row in rows], source_id
            ),
            "source_dataset_fingerprint_all_match": all_equal(
                [row.get("source_dataset_fingerprint") for row in rows], source_fingerprint
            ),
            "l2_top_overlay_contract_id_all_match": all_equal(
                [row.get("l2_top_overlay_contract_id") for row in rows], overlay_id
            ),
            "capital_ledger_fingerprint_all_match": all_equal(
                [row.get("capital_ledger_fingerprint") for row in rows], ledger_fingerprint
            ),
            "max_notional_cap_all_match": all(abs(value - EXPECTED_MAX_CAP) < 1e-9 for value in max_caps),
            "dry_run_only_all_true": all(bool_text(row.get("dry_run_only")) for row in rows),
            "import_enabled_all_false": all(not bool_text(row.get("import_enabled")) for row in rows),
            "candidate_import_allowed_all_false": all(
                not bool_text(row.get("candidate_import_allowed")) for row in rows
            ),
            "live_orders_allowed_all_false": all(not bool_text(row.get("live_orders_allowed")) for row in rows),
            "deployable_all_false": all(not bool_text(row.get("deployable")) for row in rows),
        }
    )


def microstructure_review(path: Path) -> dict[str, Any]:
    rows = read_csv(path)
    l2_available = [bool_text(row.get("l2_top_depth_available")) for row in rows]
    bad_tail = [bool_text(row.get("bad_residual_tail_case")) for row in rows]
    residual_zero = [fnum(row.get("residual_zero_after_fee_pnl")) for row in rows]
    fee_150 = [fnum(row.get("pair_after_fee_pnl_fee_1_50x")) for row in rows]
    top5_support = [fnum(row.get("top5_supports_seed_qty_rate")) for row in rows]
    return clean(
        {
            "path": str(path),
            "sha256": sha256(path),
            "row_count": len(rows),
            "l2_top_depth_available_all_true": all(l2_available),
            "bad_residual_tail_case_count": sum(1 for value in bad_tail if value),
            "residual_zero_after_fee_positive_all": all(value > 0 for value in residual_zero),
            "fee_1_50x_pair_after_fee_positive_all": all(value > 0 for value in fee_150),
            "min_top5_supports_seed_qty_rate": min(top5_support) if top5_support else 0.0,
            "min_residual_zero_after_fee_pnl": min(residual_zero) if residual_zero else 0.0,
            "min_pair_after_fee_pnl_fee_1_50x": min(fee_150) if fee_150 else 0.0,
        }
    )


def checklist_review(checklist: dict[str, Any]) -> dict[str, Any]:
    checks = checklist.get("checks") if isinstance(checklist.get("checks"), list) else []
    failed = [item for item in checks if item.get("status") != "PASS"]
    return {
        "check_count": len(checks),
        "failed_checks": failed,
        "all_checks_pass": len(checks) > 0 and not failed,
        "canary_preflight_ready": checklist.get("canary_preflight_ready") is True,
        "tiny_canary_start_ready": checklist.get("tiny_canary_start_ready") is True,
        "manual_approval_required_before_start": checklist.get("manual_approval_required_before_start") is True,
    }


def build_gates(
    manifest: dict[str, Any],
    checklist: dict[str, Any],
    source: dict[str, Any],
    ledger: dict[str, Any],
    import_review: dict[str, Any],
    owner_schema: dict[str, Any],
    micro_review: dict[str, Any],
    acceptance: dict[str, Any],
    readiness: dict[str, Any],
    start_spec: dict[str, Any],
    runner_draft: dict[str, Any],
    productization: dict[str, Any],
) -> list[dict[str, Any]]:
    checklist_status = checklist_review(checklist)
    source_ready = all(
        source.get(key)
        for key in (
            "source_semantics_contract_id",
            "source_dataset_fingerprint",
            "l2_top_overlay_contract_id",
        )
    )
    ledger_ready = (
        ledger.get("filter_name") == FILTER_NAME
        and ledger.get("filter_version") == FILTER_VERSION
        and ledger.get("capital_ledger_fingerprint")
        and int(ledger.get("candidate_count") or 0) == EXPECTED_COUNT
        and fnum(ledger.get("recommended_max_notional_cap")) == EXPECTED_MAX_CAP
        and fnum(ledger.get("residual_cost_share")) <= 0.03
        and fnum(ledger.get("zero_stress_after_fee_pnl")) > 0
    )
    import_ready = all(
        [
            import_review["row_count"] == EXPECTED_COUNT,
            not import_review["missing_fields"],
            import_review["filter_name_all_match"],
            import_review["filter_version_all_match"],
            import_review["asset_all_btc"],
            import_review["candidate_rank_contiguous"],
            import_review["deterministic_candidate_id_unique"],
            import_review["runner_profile_id_present"],
            import_review["source_semantics_contract_id_all_match"],
            import_review["source_dataset_fingerprint_all_match"],
            import_review["l2_top_overlay_contract_id_all_match"],
            import_review["capital_ledger_fingerprint_all_match"],
            import_review["max_notional_cap_all_match"],
            import_review["dry_run_only_all_true"],
            import_review["import_enabled_all_false"],
            import_review["candidate_import_allowed_all_false"],
            import_review["live_orders_allowed_all_false"],
            import_review["deployable_all_false"],
        ]
    )
    owner_schema_ready = (
        owner_schema.get("owner_private_truth_schema_ready") is True
        and owner_schema.get("owner_private_truth_data_ready") is False
        and owner_schema.get("historical_shadow_or_v1_is_private_truth") is False
        and bool(owner_schema.get("tables"))
        and bool(owner_schema.get("reconciliation_requirements"))
    )
    micro_ready = all(
        [
            micro_review["row_count"] == EXPECTED_COUNT,
            micro_review["l2_top_depth_available_all_true"],
            micro_review["bad_residual_tail_case_count"] == 0,
            micro_review["residual_zero_after_fee_positive_all"],
            micro_review["fee_1_50x_pair_after_fee_positive_all"],
        ]
    )
    acceptance_body = body(acceptance, "acceptance")
    acceptance_ready = (
        acceptance.get("status") == "KEEP_BTC_TINY_CANARY_PREFLIGHT_ACCEPTED_RESEARCH_ONLY_START_NOT_READY"
        and acceptance_body.get("all_checklist_pass") is True
        and acceptance_body.get("preflight_ready") is True
        and acceptance_body.get("start_ready") is False
        and acceptance_body.get("owner_truth_data_claimed") is False
    )
    readiness_start = body(readiness, "shadow_start_gate")
    start_gate = body(start_spec, "shadow_start_gate")
    draft_ready = (
        runner_draft.get("status") == "KEEP_XUAN_SAME_WINDOW_NO_ORDER_SHADOW_CONFIG_DRAFT_BOUND_LOCAL_ONLY"
        and runner_draft.get("validation_errors") == []
        and runner_draft.get("shadow_start_ready") is False
    )
    return [
        {
            "gate": "preflight_manifest_ready_not_start_ready",
            "status": "PASS"
            if manifest.get("status") == "OK_BTC_TINY_CANARY_PREFLIGHT_REVIEW_READY_NOT_START_READY"
            and manifest.get("canary_preflight_ready") is True
            and manifest.get("tiny_canary_start_ready") is False
            else "BLOCKED",
            "evidence": {
                "status": manifest.get("status"),
                "canary_preflight_ready": manifest.get("canary_preflight_ready"),
                "tiny_canary_start_ready": manifest.get("tiny_canary_start_ready"),
                "summary": manifest.get("summary"),
            },
        },
        {
            "gate": "preflight_checklist_all_pass_manual_approval_required",
            "status": "PASS"
            if checklist_status["all_checks_pass"]
            and checklist_status["canary_preflight_ready"]
            and not checklist_status["tiny_canary_start_ready"]
            and checklist_status["manual_approval_required_before_start"]
            else "BLOCKED",
            "evidence": checklist_status,
        },
        {
            "gate": "source_semantics_contract_ready_for_research_canary",
            "status": "PASS" if source_ready else "BLOCKED",
            "evidence": {
                "source_semantics_contract_id": source.get("source_semantics_contract_id"),
                "source_dataset_fingerprint": source.get("source_dataset_fingerprint"),
                "l2_top_overlay_contract_id": source.get("l2_top_overlay_contract_id"),
                "source_semantics_policy": source.get("source_semantics_policy"),
                "old_parity_status": source.get("old_parity_status"),
                "known_non_equivalence_to_old_baseline": source.get("known_non_equivalence_to_old_baseline"),
                "canary_preflight_blocker_if_old_parity_unproven": source.get(
                    "canary_preflight_blocker_if_old_parity_unproven"
                ),
            },
        },
        {
            "gate": "filter_specific_capital_ledger_ready",
            "status": "PASS" if ledger_ready else "BLOCKED",
            "evidence": {
                "filter_name": ledger.get("filter_name"),
                "filter_version": ledger.get("filter_version"),
                "capital_ledger_fingerprint": ledger.get("capital_ledger_fingerprint"),
                "candidate_count": ledger.get("candidate_count"),
                "gross_buy_cost": ledger.get("gross_buy_cost"),
                "core_pair_after_fee_pnl": ledger.get("core_pair_after_fee_pnl"),
                "market_end_residual_cost": ledger.get("market_end_residual_cost"),
                "residual_cost_share": ledger.get("residual_cost_share"),
                "zero_stress_after_fee_pnl": ledger.get("zero_stress_after_fee_pnl"),
                "max_capital_tied": ledger.get("max_capital_tied"),
                "recommended_max_notional_cap": ledger.get("recommended_max_notional_cap"),
            },
        },
        {
            "gate": "research_only_import_contract_ready_disabled",
            "status": "PASS" if import_ready else "BLOCKED",
            "evidence": import_review,
        },
        {
            "gate": "owner_private_truth_schema_ready_no_data_claim",
            "status": "PASS" if owner_schema_ready else "BLOCKED",
            "evidence": {
                "owner_private_truth_schema_ready": owner_schema.get("owner_private_truth_schema_ready"),
                "owner_private_truth_data_ready": owner_schema.get("owner_private_truth_data_ready"),
                "historical_shadow_or_v1_is_private_truth": owner_schema.get(
                    "historical_shadow_or_v1_is_private_truth"
                ),
                "schema_version": owner_schema.get("schema_version"),
                "table_names": sorted((owner_schema.get("tables") or {}).keys())
                if isinstance(owner_schema.get("tables"), dict)
                else owner_schema.get("tables"),
            },
        },
        {
            "gate": "microstructure_feasibility_ready",
            "status": "PASS" if micro_ready else "BLOCKED",
            "evidence": micro_review,
        },
        {
            "gate": "colleague_acceptance_audit_research_accepts_start_blocks",
            "status": "PASS" if acceptance_ready else "BLOCKED",
            "evidence": {
                "status": acceptance.get("status"),
                "acceptance": acceptance_body,
                "promotion_gate": body(acceptance, "promotion_gate"),
                "remaining_work": acceptance.get("remaining_work"),
            },
        },
        {
            "gate": "same_window_shadow_design_ready_but_manual_start_blocked",
            "status": "PASS"
            if readiness.get("status") == "KEEP_XUAN_SAME_WINDOW_SHADOW_DESIGN_READY_START_APPROVAL_REQUIRED"
            and readiness_start.get("requires_manual_approval") is True
            and readiness_start.get("shadow_start_ready") is False
            else "BLOCKED",
            "evidence": {
                "status": readiness.get("status"),
                "shadow_design_gate": body(readiness, "shadow_design_gate"),
                "shadow_start_gate": readiness_start,
            },
        },
        {
            "gate": "shadow_start_spec_defines_truth_and_risk_but_does_not_start",
            "status": "PASS"
            if start_spec.get("status") == "KEEP_XUAN_SAME_WINDOW_SHADOW_START_PREFLIGHT_SPEC_READY_APPROVAL_REQUIRED"
            and start_gate.get("shadow_start_ready") is False
            and start_gate.get("requires_manual_approval") is True
            else "BLOCKED",
            "evidence": {
                "status": start_spec.get("status"),
                "shadow_start_gate": start_gate,
                "owner_execution_truth_schema": body(start_spec, "owner_execution_truth_schema"),
            },
        },
        {
            "gate": "runner_config_draft_validated_not_executable",
            "status": "PASS" if draft_ready else "BLOCKED",
            "evidence": {
                "status": runner_draft.get("status"),
                "validation_errors": runner_draft.get("validation_errors"),
                "shadow_start_ready": runner_draft.get("shadow_start_ready"),
                "remaining_blockers": runner_draft.get("remaining_blockers"),
                "next_executable_action": runner_draft.get("next_executable_action"),
            },
        },
        {
            "gate": "productization_request_present_local_only",
            "status": "PASS"
            if productization.get("status") == "KEEP_BACKTEST_V1_SHADOW_DESIGN_PRODUCTIZATION_REQUEST_READY_LOCAL_ONLY"
            else "BLOCKED",
            "evidence": {
                "status": productization.get("status"),
                "summary": body(productization, "summary"),
            },
        },
    ]


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    manifest = load_json(args.preflight_manifest)
    checklist = load_json(args.preflight_checklist)
    source = load_json(args.source_contract)
    ledger = load_json(args.capital_ledger)
    owner_schema = load_json(args.owner_truth_schema)
    acceptance = load_json(args.acceptance_audit)
    readiness = load_json(args.shadow_readiness)
    start_spec = load_json(args.shadow_start_spec)
    runner_draft = load_json(args.runner_draft)
    productization = load_json(args.productization_request)
    import_review = import_contract_review(args.import_contract, source, ledger)
    micro_review = microstructure_review(args.microstructure)
    gates = build_gates(
        manifest,
        checklist,
        source,
        ledger,
        import_review,
        owner_schema,
        micro_review,
        acceptance,
        readiness,
        start_spec,
        runner_draft,
        productization,
    )
    blockers = [item["gate"] for item in gates if item.get("status") != "PASS"]
    all_gates_pass = not blockers
    start_blockers = [
        "explicit_user_manual_start_approval_missing",
        "active_runner_conflict_check_not_run_for_start",
        "runner_config_is_draft_not_executable",
        "no_manifest_or_preauthorization_for_remote_or_live",
        "owner_private_truth_data_only_after_future_owner_execution",
    ]
    status = (
        "KEEP_SHADOW_REVIEW_BACKTEST_V1_BTC_TINY_CANARY_PREFLIGHT_ACCEPTED_LOCAL_ONLY_START_BLOCKED"
        if all_gates_pass
        else "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_BTC_TINY_CANARY_PREFLIGHT_ACCEPTANCE_GAP_LOCAL_ONLY"
    )
    summary = body(manifest, "summary")
    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_btc_tiny_canary_preflight_acceptance_review",
            "status": status,
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_btc_tiny_canary_preflight_acceptance_review.py",
            "inputs": {
                "preflight_manifest": str(args.preflight_manifest),
                "preflight_checklist": str(args.preflight_checklist),
                "source_contract": str(args.source_contract),
                "capital_ledger": str(args.capital_ledger),
                "import_contract": str(args.import_contract),
                "owner_truth_schema": str(args.owner_truth_schema),
                "microstructure": str(args.microstructure),
                "acceptance_audit": str(args.acceptance_audit),
                "shadow_readiness": str(args.shadow_readiness),
                "shadow_start_spec": str(args.shadow_start_spec),
                "runner_draft": str(args.runner_draft),
                "productization_request": str(args.productization_request),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "markdown": str(args.artifact_dir / "BTC_TINY_CANARY_PREFLIGHT_ACCEPTANCE_REVIEW.md"),
            },
            "decision": {
                "colleague_p0_deliverables_reviewed": True,
                "preflight_acceptance_review_pass": all_gates_pass,
                "canary_preflight_review_ready": all_gates_pass,
                "source_semantics_contract_ready": all_gates_pass,
                "filter_specific_capital_ledger_ready": all_gates_pass,
                "research_only_import_contract_ready": all_gates_pass,
                "owner_private_truth_schema_ready": owner_schema.get("owner_private_truth_schema_ready") is True,
                "owner_private_truth_data_ready": False,
                "microstructure_feasibility_ready": all_gates_pass,
                "same_window_shadow_design_ready": body(readiness, "shadow_design_gate").get("shadow_design_ready")
                is True,
                "runner_config_draft_validated": runner_draft.get("validation_errors") == [],
                "tiny_canary_start_ready": False,
                "manual_start_approval_required": True,
                "candidate_import_allowed": False,
                "runner_support_ready": False,
                "manifest_or_preauthorization_ready": False,
                "future_remote_allowed_by_this_review": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "next_lane": "productize_no_order_shadow_runner_config_and_collect_explicit_user_start_approval_separately",
            },
            "btc_tiny_canary_research_summary": {
                "filter_name": FILTER_NAME,
                "filter_version": FILTER_VERSION,
                "candidate_count": summary.get("candidate_count"),
                "valid_day_count": summary.get("valid_day_count"),
                "gross_buy_cost": summary.get("gross_buy_cost"),
                "core_pair_after_fee_pnl": summary.get("core_pair_after_fee_pnl"),
                "market_end_residual_cost": summary.get("market_end_residual_cost"),
                "residual_cost_share": summary.get("residual_cost_share"),
                "zero_stress_after_fee_pnl": summary.get("zero_stress_after_fee_pnl"),
                "max_capital_tied": summary.get("max_capital_tied"),
                "recommended_max_notional_cap": summary.get("recommended_max_notional_cap"),
                "microstructure_status": summary.get("microstructure_status"),
            },
            "acceptance_gates": gates,
            "hard_blockers_if_acceptance_failed": blockers,
            "remaining_blockers_before_any_start": start_blockers,
            "warnings": [
                "This accepts the research preflight package only; it is not a start approval.",
                "Owner private truth schema is ready, but owner private truth data remains false until future owner execution and reconciliation.",
                "V1 normalized BUY is accepted as a new canonical research canary source; this is not proof of old baseline parity.",
                "The import contract is intentionally disabled: dry_run_only=true, import_enabled=false, candidate_import_allowed=false.",
                "No remote, deploy, live order, or private-truth promotion is authorized.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    summary = body(card, "btc_tiny_canary_research_summary")
    lines = [
        "# BTC Tiny Canary Preflight Acceptance Review",
        "",
        f"- status: `{card.get('status')}`",
        f"- preflight_acceptance_review_pass: `{decision.get('preflight_acceptance_review_pass')}`",
        f"- tiny_canary_start_ready: `{decision.get('tiny_canary_start_ready')}`",
        f"- manual_start_approval_required: `{decision.get('manual_start_approval_required')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## BTC Research Summary",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Acceptance Gates",
            "",
            "| gate | status |",
            "| --- | --- |",
        ]
    )
    for item in card.get("acceptance_gates", []):
        lines.append(f"| `{item.get('gate')}` | `{item.get('status')}` |")
    lines.extend(["", "## Remaining Start Blockers", ""])
    for blocker in card.get("remaining_blockers_before_any_start", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The colleague P0 package now satisfies the local research preflight contract for the BTC `le_3pct` canary.",
            "- The owner private truth requirement is schema-only at this stage; actual owner truth data remains impossible until future owner execution.",
            "- Start/live remains blocked by explicit manual approval, active-runner conflict check, and executable runner binding.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight-manifest", type=Path, default=DEFAULT_PREFLIGHT_MANIFEST)
    parser.add_argument("--preflight-checklist", type=Path, default=DEFAULT_PREFLIGHT_CHECKLIST)
    parser.add_argument("--source-contract", type=Path, default=DEFAULT_SOURCE_CONTRACT)
    parser.add_argument("--capital-ledger", type=Path, default=DEFAULT_CAPITAL_LEDGER)
    parser.add_argument("--import-contract", type=Path, default=DEFAULT_IMPORT_CONTRACT)
    parser.add_argument("--owner-truth-schema", type=Path, default=DEFAULT_OWNER_TRUTH_SCHEMA)
    parser.add_argument("--microstructure", type=Path, default=DEFAULT_MICROSTRUCTURE)
    parser.add_argument("--acceptance-audit", type=Path, default=DEFAULT_ACCEPTANCE_AUDIT)
    parser.add_argument("--shadow-readiness", type=Path, default=DEFAULT_SHADOW_READINESS)
    parser.add_argument("--shadow-start-spec", type=Path, default=DEFAULT_SHADOW_START_SPEC)
    parser.add_argument("--runner-draft", type=Path, default=DEFAULT_RUNNER_DRAFT)
    parser.add_argument("--productization-request", type=Path, default=DEFAULT_PRODUCTIZATION_REQUEST)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "BTC_TINY_CANARY_PREFLIGHT_ACCEPTANCE_REVIEW.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
