#!/usr/bin/env python3
"""Gate BTC tiny-canary start readiness from local Backtest V1 artifacts.

This script is intentionally conservative.  It can accept research preflight,
sizing, and schema deliverables, but it cannot authorize a start.  Starting any
shadow/canary still requires a separate manual approval, active-runner conflict
check, and an executable no-order runner binding outside these backtest files.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any


STAMP = "20260528T1626Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"
PREFLIGHT_ROOT = CONTRACT_ROOT / "btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest"

DEFAULT_PREFLIGHT_MANIFEST = (
    PREFLIGHT_ROOT / "BTC_SAME_WINDOW_RESIDUAL_SHARE_LE_3PCT_V1_CANARY_PREFLIGHT_MANIFEST.json"
)
DEFAULT_PREFLIGHT_CHECKLIST = PREFLIGHT_ROOT / "preflight_checklist.json"
DEFAULT_IMPORT_CONTRACT = PREFLIGHT_ROOT / "research_only_import_contract.csv"
DEFAULT_OWNER_SCHEMA = PREFLIGHT_ROOT / "owner_private_truth_schema.json"
DEFAULT_SIZING = (
    CONTRACT_ROOT
    / "xuan_btc_tiny_canary_sizing_sensitivity_latest"
    / "XUAN_BTC_TINY_CANARY_SIZING_SENSITIVITY.json"
)
DEFAULT_RUNNER_CONFIG = (
    CONTRACT_ROOT
    / "xuan_same_window_no_order_shadow_runner_config_draft_latest"
    / "xuan_same_window_no_order_shadow_runner_config_draft.json"
)
DEFAULT_RUNNER_VALIDATION = (
    CONTRACT_ROOT
    / "xuan_same_window_no_order_shadow_runner_config_draft_latest"
    / "XUAN_SAME_WINDOW_NO_ORDER_SHADOW_RUNNER_CONFIG_DRAFT_VALIDATION.json"
)
DEFAULT_START_SPEC = (
    CONTRACT_ROOT
    / "xuan_same_window_shadow_start_preflight_spec_latest"
    / "XUAN_SAME_WINDOW_SHADOW_START_PREFLIGHT_SPEC.json"
)
DEFAULT_READINESS = (
    CONTRACT_ROOT
    / "xuan_same_window_shadow_readiness_gate_latest"
    / "XUAN_SAME_WINDOW_SHADOW_READINESS_GATE.json"
)
DEFAULT_ACCEPTANCE_AUDIT = (
    CONTRACT_ROOT
    / "xuan_backtest_v1_btc_tiny_canary_acceptance_audit_latest"
    / "XUAN_BACKTEST_V1_BTC_TINY_CANARY_ACCEPTANCE_AUDIT.json"
)
DEFAULT_PRODUCTIZATION = (
    CONTRACT_ROOT
    / "xuan_backtest_v1_shadow_design_productization_request_latest"
    / "XUAN_BACKTEST_V1_SHADOW_DESIGN_PRODUCTIZATION_REQUEST.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/"
    f"xuan_shadow_review_backtest_v1_btc_tiny_canary_start_readiness_gate_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_btc_tiny_canary_start_readiness_gate_{STAMP}"
)

FILTER_NAME = "btc_same_window_residual_share_le_3pct_v1"
RUNNER_PROFILE_ID = "btc_same_window_tiny_canary_dryrun_v1"
EXPECTED_COUNT = 52
RECOMMENDED_CAP = 25.0


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


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def bool_text(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def checklist_all_pass(checklist: dict[str, Any]) -> bool:
    checks = checklist.get("checks") if isinstance(checklist.get("checks"), list) else []
    return bool(checks) and all(item.get("status") == "PASS" for item in checks)


def import_contract_summary(path: Path) -> dict[str, Any]:
    rows = read_csv(path)
    caps = [fnum(row.get("max_notional_cap")) for row in rows]
    return clean(
        {
            "row_count": len(rows),
            "runner_profile_ids": sorted({row.get("runner_profile_id") for row in rows}),
            "all_filter_match": all(row.get("filter_name") == FILTER_NAME for row in rows),
            "all_runner_profile_match": all(row.get("runner_profile_id") == RUNNER_PROFILE_ID for row in rows),
            "all_cap_match": all(abs(value - RECOMMENDED_CAP) < 1e-9 for value in caps),
            "dry_run_only_all_true": all(bool_text(row.get("dry_run_only")) for row in rows),
            "import_enabled_all_false": all(not bool_text(row.get("import_enabled")) for row in rows),
            "candidate_import_allowed_all_false": all(
                not bool_text(row.get("candidate_import_allowed")) for row in rows
            ),
            "live_orders_allowed_all_false": all(not bool_text(row.get("live_orders_allowed")) for row in rows),
            "deployable_all_false": all(not bool_text(row.get("deployable")) for row in rows),
        }
    )


def sizing_cap_row(sizing: dict[str, Any], cap: float) -> dict[str, Any]:
    rows = sizing.get("sizing_sensitivity") if isinstance(sizing.get("sizing_sensitivity"), list) else []
    for row in rows:
        if abs(fnum(row.get("notional_cap_usd")) - cap) < 1e-9:
            return row
    return {}


def build_gates(
    manifest: dict[str, Any],
    checklist: dict[str, Any],
    import_summary: dict[str, Any],
    owner_schema: dict[str, Any],
    sizing: dict[str, Any],
    runner_config: dict[str, Any],
    runner_validation: dict[str, Any],
    start_spec: dict[str, Any],
    readiness: dict[str, Any],
    acceptance: dict[str, Any],
    productization: dict[str, Any],
) -> list[dict[str, Any]]:
    cap25 = sizing_cap_row(sizing, RECOMMENDED_CAP)
    observed = body(sizing, "observed_summary")
    runner_guards = body(runner_config, "execution_guards_for_future_approval")
    shadow_start = body(start_spec, "shadow_start_gate")
    readiness_start = body(readiness, "shadow_start_gate")
    return [
        {
            "gate": "btc_tiny_preflight_ready_not_start_ready",
            "status": "PASS"
            if manifest.get("status") == "OK_BTC_TINY_CANARY_PREFLIGHT_REVIEW_READY_NOT_START_READY"
            and manifest.get("canary_preflight_ready") is True
            and manifest.get("tiny_canary_start_ready") is False
            and checklist_all_pass(checklist)
            else "BLOCKED",
            "evidence": {
                "manifest_status": manifest.get("status"),
                "canary_preflight_ready": manifest.get("canary_preflight_ready"),
                "tiny_canary_start_ready": manifest.get("tiny_canary_start_ready"),
                "checklist_all_pass": checklist_all_pass(checklist),
            },
        },
        {
            "gate": "research_only_import_contract_disabled_and_complete",
            "status": "PASS"
            if import_summary["row_count"] == EXPECTED_COUNT
            and import_summary["all_filter_match"]
            and import_summary["all_runner_profile_match"]
            and import_summary["all_cap_match"]
            and import_summary["dry_run_only_all_true"]
            and import_summary["import_enabled_all_false"]
            and import_summary["candidate_import_allowed_all_false"]
            and import_summary["live_orders_allowed_all_false"]
            and import_summary["deployable_all_false"]
            else "BLOCKED",
            "evidence": import_summary,
        },
        {
            "gate": "owner_truth_schema_ready_without_data_claim",
            "status": "PASS"
            if owner_schema.get("owner_private_truth_schema_ready") is True
            and owner_schema.get("owner_private_truth_data_ready") is False
            and owner_schema.get("historical_shadow_or_v1_is_private_truth") is False
            else "BLOCKED",
            "evidence": {
                "owner_private_truth_schema_ready": owner_schema.get("owner_private_truth_schema_ready"),
                "owner_private_truth_data_ready": owner_schema.get("owner_private_truth_data_ready"),
                "historical_shadow_or_v1_is_private_truth": owner_schema.get(
                    "historical_shadow_or_v1_is_private_truth"
                ),
                "schema_version": owner_schema.get("schema_version"),
            },
        },
        {
            "gate": "sizing_sensitivity_ready_research_only",
            "status": "PASS"
            if sizing.get("status") == "KEEP_BTC_TINY_CANARY_SIZING_SENSITIVITY_READY_RESEARCH_ONLY"
            and observed.get("candidate_count") == EXPECTED_COUNT
            and fnum(observed.get("recommended_max_notional_cap")) == RECOMMENDED_CAP
            and cap25
            and cap25.get("is_research_capacity_proxy") is True
            and cap25.get("is_live_profit_claim") is False
            else "BLOCKED",
            "evidence": {
                "status": sizing.get("status"),
                "observed_summary": observed,
                "cap25_proxy": cap25,
                "risk_notes": sizing.get("risk_notes"),
            },
        },
        {
            "gate": "shadow_design_ready_but_start_requires_approval",
            "status": "PASS"
            if body(readiness, "shadow_design_gate").get("shadow_design_ready") is True
            and readiness_start.get("requires_manual_approval") is True
            and readiness_start.get("shadow_start_ready") is False
            else "BLOCKED",
            "evidence": {
                "readiness_status": readiness.get("status"),
                "shadow_design_gate": body(readiness, "shadow_design_gate"),
                "shadow_start_gate": readiness_start,
            },
        },
        {
            "gate": "shadow_start_spec_present_but_not_start_ready",
            "status": "PASS"
            if start_spec.get("status") == "KEEP_XUAN_SAME_WINDOW_SHADOW_START_PREFLIGHT_SPEC_READY_APPROVAL_REQUIRED"
            and shadow_start.get("shadow_start_ready") is False
            and shadow_start.get("requires_manual_approval") is True
            else "BLOCKED",
            "evidence": {
                "status": start_spec.get("status"),
                "shadow_start_gate": shadow_start,
                "promotion_gate": body(start_spec, "promotion_gate"),
            },
        },
        {
            "gate": "runner_config_bound_as_draft_not_executable",
            "status": "PASS"
            if runner_config.get("status") == "DRAFT_NOT_APPROVED_NOT_EXECUTABLE"
            and runner_config.get("start_allowed") is False
            and runner_validation.get("validation_errors") == []
            and runner_validation.get("shadow_start_ready") is False
            and runner_guards.get("orders_allowed") is False
            and runner_guards.get("live_orders_allowed") is False
            and runner_guards.get("remote_runner_allowed") is False
            else "BLOCKED",
            "evidence": {
                "config_status": runner_config.get("status"),
                "mode": runner_config.get("mode"),
                "start_allowed": runner_config.get("start_allowed"),
                "execution_guards": runner_guards,
                "validation_status": runner_validation.get("status"),
                "validation_errors": runner_validation.get("validation_errors"),
                "remaining_blockers": runner_validation.get("remaining_blockers"),
            },
        },
        {
            "gate": "acceptance_audit_accepts_research_blocks_start",
            "status": "PASS"
            if acceptance.get("status") == "KEEP_BTC_TINY_CANARY_PREFLIGHT_ACCEPTED_RESEARCH_ONLY_START_NOT_READY"
            and body(acceptance, "acceptance").get("preflight_ready") is True
            and body(acceptance, "acceptance").get("start_ready") is False
            and body(acceptance, "acceptance").get("owner_truth_data_claimed") is False
            else "BLOCKED",
            "evidence": {
                "status": acceptance.get("status"),
                "acceptance": body(acceptance, "acceptance"),
                "remaining_work": acceptance.get("remaining_work"),
            },
        },
        {
            "gate": "productization_request_completed_for_backtest_side_not_runner_start",
            "status": "PASS"
            if productization.get("status") == "KEEP_BACKTEST_V1_SHADOW_DESIGN_PRODUCTIZATION_REQUEST_READY_LOCAL_ONLY"
            else "BLOCKED",
            "evidence": {
                "status": productization.get("status"),
                "summary": body(productization, "summary"),
                "next_executable_action": productization.get("next_executable_action"),
            },
        },
    ]


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    manifest = load_json(args.preflight_manifest)
    checklist = load_json(args.preflight_checklist)
    owner_schema = load_json(args.owner_schema)
    sizing = load_json(args.sizing)
    runner_config = load_json(args.runner_config)
    runner_validation = load_json(args.runner_validation)
    start_spec = load_json(args.start_spec)
    readiness = load_json(args.readiness)
    acceptance = load_json(args.acceptance_audit)
    productization = load_json(args.productization)
    import_summary = import_contract_summary(args.import_contract)
    gates = build_gates(
        manifest,
        checklist,
        import_summary,
        owner_schema,
        sizing,
        runner_config,
        runner_validation,
        start_spec,
        readiness,
        acceptance,
        productization,
    )
    failed = [gate["gate"] for gate in gates if gate.get("status") != "PASS"]
    observed = body(sizing, "observed_summary")
    cap25 = sizing_cap_row(sizing, RECOMMENDED_CAP)
    start_blockers = [
        "explicit_user_manual_start_approval_missing",
        "active_runner_conflict_check_not_run_for_start",
        "runner_config_is_draft_not_executable",
        "no_start_manifest_or_runtime_binding_created_by_this_gate",
        "owner_private_truth_data_only_after_future_owner_execution",
    ]
    research_ready = not failed
    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_btc_tiny_canary_start_readiness_gate",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_BTC_TINY_CANARY_START_NOT_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_btc_tiny_canary_start_readiness_gate.py",
            "inputs": {
                "preflight_manifest": str(args.preflight_manifest),
                "preflight_checklist": str(args.preflight_checklist),
                "import_contract": str(args.import_contract),
                "owner_schema": str(args.owner_schema),
                "sizing": str(args.sizing),
                "runner_config": str(args.runner_config),
                "runner_validation": str(args.runner_validation),
                "start_spec": str(args.start_spec),
                "readiness": str(args.readiness),
                "acceptance_audit": str(args.acceptance_audit),
                "productization": str(args.productization),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "markdown": str(args.artifact_dir / "BTC_TINY_CANARY_START_READINESS_GATE.md"),
            },
            "decision": {
                "colleague_latest_deliverables_reviewed": True,
                "research_preflight_complete": research_ready,
                "sizing_sensitivity_ready": research_ready,
                "backtest_side_p0_complete_for_research_preflight": research_ready,
                "start_readiness_gate_pass": False,
                "tiny_canary_start_ready": False,
                "explicit_user_approval_required_before_start": True,
                "active_runner_conflict_check_required_before_start": True,
                "runner_config_executable": False,
                "runner_config_draft_validated": runner_validation.get("validation_errors") == [],
                "candidate_import_allowed": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "owner_private_truth_schema_ready": owner_schema.get("owner_private_truth_schema_ready") is True,
                "owner_private_truth_data_ready": False,
                "private_truth_ready": False,
                "next_lane": "productize_executable_no_order_shadow_runner_binding_or_get_manual_start_approval_then_conflict_check",
            },
            "btc_tiny_canary_research_summary": {
                "filter_name": FILTER_NAME,
                "runner_profile_id": RUNNER_PROFILE_ID,
                "candidate_count": observed.get("candidate_count"),
                "valid_day_count": observed.get("valid_day_count"),
                "gross_buy_cost": observed.get("gross_buy_cost"),
                "core_pair_after_fee_pnl": observed.get("core_pair_after_fee_pnl"),
                "market_end_residual_cost": observed.get("market_end_residual_cost"),
                "residual_cost_share": observed.get("residual_cost_share"),
                "zero_stress_after_fee_pnl": observed.get("zero_stress_after_fee_pnl"),
                "max_capital_tied": observed.get("max_capital_tied"),
                "recommended_max_notional_cap": observed.get("recommended_max_notional_cap"),
                "cap25_zero_stress_daily_proxy": cap25.get("zero_stress_daily_proxy"),
                "cap25_is_live_profit_claim": cap25.get("is_live_profit_claim"),
            },
            "start_readiness_gates": gates,
            "failed_research_gates": failed,
            "remaining_blockers_before_any_start": start_blockers,
            "warnings": [
                "Sizing sensitivity is a research capacity proxy, not a live profit claim.",
                "The runner config is validated as a draft but explicitly not executable.",
                "Owner private truth schema is ready; owner private truth data remains false until future owner execution.",
                "No import, remote runner, deploy, live order, or promotion is authorized by this gate.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    summary = body(card, "btc_tiny_canary_research_summary")
    lines = [
        "# BTC Tiny Canary Start Readiness Gate",
        "",
        f"- status: `{card.get('status')}`",
        f"- research_preflight_complete: `{decision.get('research_preflight_complete')}`",
        f"- start_readiness_gate_pass: `{decision.get('start_readiness_gate_pass')}`",
        f"- tiny_canary_start_ready: `{decision.get('tiny_canary_start_ready')}`",
        f"- runner_config_executable: `{decision.get('runner_config_executable')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## Research Summary",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Gates",
            "",
            "| gate | status |",
            "| --- | --- |",
        ]
    )
    for gate in card.get("start_readiness_gates", []):
        lines.append(f"| `{gate.get('gate')}` | `{gate.get('status')}` |")
    lines.extend(["", "## Remaining Start Blockers", ""])
    for blocker in card.get("remaining_blockers_before_any_start", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Backtest-side P0 is complete enough for research preflight and sizing review.",
            "- The next boundary is operational, not backtest: executable no-order runner binding, explicit approval, and conflict check.",
            "- This artifact intentionally keeps start/import/live false.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight-manifest", type=Path, default=DEFAULT_PREFLIGHT_MANIFEST)
    parser.add_argument("--preflight-checklist", type=Path, default=DEFAULT_PREFLIGHT_CHECKLIST)
    parser.add_argument("--import-contract", type=Path, default=DEFAULT_IMPORT_CONTRACT)
    parser.add_argument("--owner-schema", type=Path, default=DEFAULT_OWNER_SCHEMA)
    parser.add_argument("--sizing", type=Path, default=DEFAULT_SIZING)
    parser.add_argument("--runner-config", type=Path, default=DEFAULT_RUNNER_CONFIG)
    parser.add_argument("--runner-validation", type=Path, default=DEFAULT_RUNNER_VALIDATION)
    parser.add_argument("--start-spec", type=Path, default=DEFAULT_START_SPEC)
    parser.add_argument("--readiness", type=Path, default=DEFAULT_READINESS)
    parser.add_argument("--acceptance-audit", type=Path, default=DEFAULT_ACCEPTANCE_AUDIT)
    parser.add_argument("--productization", type=Path, default=DEFAULT_PRODUCTIZATION)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "BTC_TINY_CANARY_START_READINESS_GATE.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
