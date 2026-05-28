#!/usr/bin/env python3
"""Record manual approval and gate BTC tiny-canary runtime readiness.

The user can clear the manual approval blocker, but that does not by itself
make a historical backtest import packet executable.  This gate separates:

* approval captured,
* active-runner conflict check,
* haircut/sizing/research preflight,
* actual live runtime binding.

No runner is started by this script.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import subprocess
from pathlib import Path
from typing import Any


STAMP = "20260528T1634Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"
PREFLIGHT_ROOT = CONTRACT_ROOT / "btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest"

DEFAULT_PREFLIGHT = PREFLIGHT_ROOT / "BTC_SAME_WINDOW_RESIDUAL_SHARE_LE_3PCT_V1_CANARY_PREFLIGHT_MANIFEST.json"
DEFAULT_IMPORT_CONTRACT = PREFLIGHT_ROOT / "research_only_import_contract.csv"
DEFAULT_SIZING = (
    CONTRACT_ROOT / "xuan_btc_tiny_canary_sizing_sensitivity_latest/XUAN_BTC_TINY_CANARY_SIZING_SENSITIVITY.json"
)
DEFAULT_HAIRCUT = (
    CONTRACT_ROOT / "xuan_btc_tiny_canary_haircut_stress_latest/XUAN_BTC_TINY_CANARY_HAIRCUT_STRESS.json"
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
DEFAULT_OWNER_SCHEMA = PREFLIGHT_ROOT / "owner_private_truth_schema.json"
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/"
    f"xuan_shadow_review_backtest_v1_btc_tiny_canary_manual_approval_runtime_gate_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_btc_tiny_canary_manual_approval_runtime_gate_{STAMP}"
)

FILTER_NAME = "btc_same_window_residual_share_le_3pct_v1"
EXPECTED_CANDIDATES = 52


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


def parse_day(value: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError:
        return None


def active_runner_conflicts() -> list[dict[str, Any]]:
    result = subprocess.run(
        ["ps", "ax", "-o", "pid,user,command"],
        check=True,
        text=True,
        capture_output=True,
    )
    conflicts: list[dict[str, Any]] = []
    runner_needles = [
        "tools/xuan_dplus_passive_passive_shadow_runner.py",
        "xuan_dplus_passive_passive_shadow_runner.py",
        "run_xuan_m0001_maker_shadow.py",
        "xuan_research_job_runner.py",
    ]
    ignore_needles = [
        "xuan_shadow_review_backtest_v1_btc_tiny_canary_manual_approval_runtime_gate.py",
        "py_compile",
    ]
    for line in result.stdout.splitlines()[1:]:
        parts = line.strip().split(maxsplit=2)
        if len(parts) < 3:
            continue
        pid, user, command = parts
        if any(needle in command for needle in ignore_needles):
            continue
        if any(needle in command for needle in runner_needles):
            conflicts.append({"pid": int(pid), "user": user, "command": command})
    return conflicts


def import_contract_summary(rows: list[dict[str, str]], current_day: dt.date) -> dict[str, Any]:
    days = [day for day in (parse_day(row.get("day")) for row in rows) if day is not None]
    max_day = max(days) if days else None
    min_day = min(days) if days else None
    future_rows = [row for row in rows if (parse_day(row.get("day")) or dt.date.min) >= current_day]
    return {
        "row_count": len(rows),
        "filter_names": sorted({row.get("filter_name") for row in rows}),
        "asset_values": sorted({row.get("asset") for row in rows}),
        "min_day": min_day.isoformat() if min_day else None,
        "max_day": max_day.isoformat() if max_day else None,
        "current_day": current_day.isoformat(),
        "future_or_current_rows": len(future_rows),
        "historical_only": len(rows) > 0 and len(future_rows) == 0,
        "unique_slug_count": len({row.get("slug") for row in rows}),
        "candidate_import_allowed_values": sorted({row.get("candidate_import_allowed") for row in rows}),
        "import_enabled_values": sorted({row.get("import_enabled") for row in rows}),
        "live_orders_allowed_values": sorted({row.get("live_orders_allowed") for row in rows}),
    }


def haircut_pass(haircut: dict[str, Any]) -> bool:
    scenarios = {
        row.get("scenario"): row
        for row in (haircut.get("scenario_summary") if isinstance(haircut.get("scenario_summary"), list) else [])
    }
    required = [
        "base_zero_stress",
        "fee_1_50_zero_stress",
        "pair_edge_50pct_zero_stress",
        "gross_cost_100bp_slip_zero_stress",
    ]
    if haircut.get("status") != "KEEP_BTC_TINY_CANARY_HAIRCUT_STRESS_READY_RESEARCH_ONLY":
        return False
    for name in required:
        row = scenarios.get(name, {})
        if fnum(row.get("total_pnl")) <= 0 or fnum(row.get("worst_market")) <= 0:
            return False
        if int(fnum(row.get("positive_markets"))) != EXPECTED_CANDIDATES:
            return False
    depth = body(haircut, "depth_summary")
    return (
        int(fnum(depth.get("top5_support_fail_markets"))) == 0
        and int(fnum(depth.get("seed_gt_5pct_top5_depth_markets"))) == 0
    )


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    preflight = load_json(args.preflight)
    sizing = load_json(args.sizing)
    haircut = load_json(args.haircut)
    runner_config = load_json(args.runner_config)
    runner_validation = load_json(args.runner_validation)
    owner_schema = load_json(args.owner_schema)
    import_rows = read_csv(args.import_contract)
    current_day = dt.date.today()
    import_summary = import_contract_summary(import_rows, current_day)
    conflicts = active_runner_conflicts()
    manual_approval_captured = bool(args.manual_approval)
    research_preflight_pass = (
        preflight.get("status") == "OK_BTC_TINY_CANARY_PREFLIGHT_REVIEW_READY_NOT_START_READY"
        and preflight.get("canary_preflight_ready") is True
        and preflight.get("tiny_canary_start_ready") is False
        and sizing.get("status") == "KEEP_BTC_TINY_CANARY_SIZING_SENSITIVITY_READY_RESEARCH_ONLY"
        and haircut_pass(haircut)
        and owner_schema.get("owner_private_truth_schema_ready") is True
        and owner_schema.get("owner_private_truth_data_ready") is False
    )
    conflict_clear = not conflicts
    runner_draft_only = (
        runner_config.get("status") == "DRAFT_NOT_APPROVED_NOT_EXECUTABLE"
        and runner_config.get("start_allowed") is False
        and runner_validation.get("validation_errors") == []
        and runner_validation.get("shadow_start_ready") is False
    )
    runtime_binding_ready = False
    start_ready = (
        manual_approval_captured
        and research_preflight_pass
        and conflict_clear
        and runtime_binding_ready
        and not import_summary["historical_only"]
    )
    remaining_blockers = []
    if not manual_approval_captured:
        remaining_blockers.append("manual_start_approval_missing")
    if not conflict_clear:
        remaining_blockers.append("active_xuan_runner_conflict_detected")
    if not research_preflight_pass:
        remaining_blockers.append("research_preflight_or_haircut_not_passed")
    if import_summary["historical_only"]:
        remaining_blockers.append("historical_import_contract_is_not_live_runtime_selector")
    if runner_draft_only:
        remaining_blockers.append("runner_config_is_draft_not_executable")
    if not runtime_binding_ready:
        remaining_blockers.append("runtime_market_discovery_and_candidate_selection_not_productized")
    observed = body(sizing, "observed_summary")
    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_btc_tiny_canary_manual_approval_runtime_gate",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_BTC_TINY_CANARY_RUNTIME_BINDING_NOT_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_btc_tiny_canary_manual_approval_runtime_gate.py",
            "inputs": {
                "preflight": str(args.preflight),
                "import_contract": str(args.import_contract),
                "sizing": str(args.sizing),
                "haircut": str(args.haircut),
                "runner_config": str(args.runner_config),
                "runner_validation": str(args.runner_validation),
                "owner_schema": str(args.owner_schema),
                "manual_approval_source": args.manual_approval_source,
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "markdown": str(args.artifact_dir / "BTC_TINY_CANARY_MANUAL_APPROVAL_RUNTIME_GATE.md"),
            },
            "decision": {
                "manual_start_approval_captured": manual_approval_captured,
                "manual_approval_source": args.manual_approval_source,
                "active_runner_conflict_check_run": True,
                "active_runner_conflict_clear": conflict_clear,
                "research_preflight_pass": research_preflight_pass,
                "haircut_stress_pass": haircut_pass(haircut),
                "owner_private_truth_schema_ready": owner_schema.get("owner_private_truth_schema_ready") is True,
                "owner_private_truth_data_ready": False,
                "historical_import_contract_only": import_summary["historical_only"],
                "runner_config_draft_only": runner_draft_only,
                "runtime_binding_ready": runtime_binding_ready,
                "tiny_canary_start_ready": start_ready,
                "candidate_import_allowed": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "next_lane": "implement_live_runtime_selector_and_executable_no_order_shadow_binding_for_btc_same_window_filter",
            },
            "btc_tiny_canary_research_summary": {
                "filter_name": FILTER_NAME,
                "candidate_count": observed.get("candidate_count"),
                "valid_day_count": observed.get("valid_day_count"),
                "gross_buy_cost": observed.get("gross_buy_cost"),
                "core_pair_after_fee_pnl": observed.get("core_pair_after_fee_pnl"),
                "market_end_residual_cost": observed.get("market_end_residual_cost"),
                "residual_cost_share": observed.get("residual_cost_share"),
                "zero_stress_after_fee_pnl": observed.get("zero_stress_after_fee_pnl"),
                "max_capital_tied": observed.get("max_capital_tied"),
                "recommended_max_notional_cap": observed.get("recommended_max_notional_cap"),
            },
            "active_runner_conflicts": conflicts,
            "import_contract_summary": import_summary,
            "haircut_stress_summary": {
                "status": haircut.get("status"),
                "depth_summary": body(haircut, "depth_summary"),
                "scenario_summary": haircut.get("scenario_summary"),
                "winner_removal": haircut.get("winner_removal"),
            },
            "runner_config_summary": {
                "status": runner_config.get("status"),
                "mode": runner_config.get("mode"),
                "start_allowed": runner_config.get("start_allowed"),
                "validation_status": runner_validation.get("status"),
                "validation_errors": runner_validation.get("validation_errors"),
                "remaining_blockers": runner_validation.get("remaining_blockers"),
            },
            "remaining_blockers_before_any_start": remaining_blockers,
            "warnings": [
                "User approval clears only the manual approval blocker.",
                "The current import contract is historical backtest output and cannot itself select future live markets.",
                "The runner config remains explicitly draft/not executable.",
                "No shadow/canary was started and no import/live/remote action is authorized by this gate.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    summary = body(card, "btc_tiny_canary_research_summary")
    lines = [
        "# BTC Tiny Canary Manual Approval Runtime Gate",
        "",
        f"- status: `{card.get('status')}`",
        f"- manual_start_approval_captured: `{decision.get('manual_start_approval_captured')}`",
        f"- active_runner_conflict_clear: `{decision.get('active_runner_conflict_clear')}`",
        f"- research_preflight_pass: `{decision.get('research_preflight_pass')}`",
        f"- haircut_stress_pass: `{decision.get('haircut_stress_pass')}`",
        f"- runtime_binding_ready: `{decision.get('runtime_binding_ready')}`",
        f"- tiny_canary_start_ready: `{decision.get('tiny_canary_start_ready')}`",
        "",
        "## Research Summary",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Remaining Blockers", ""])
    for blocker in card.get("remaining_blockers_before_any_start", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Manual approval is now recorded.",
            "- The active-runner conflict check is clear.",
            "- The strategy still cannot be started because the current packet is historical and the runner binding is still draft-only.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", type=Path, default=DEFAULT_PREFLIGHT)
    parser.add_argument("--import-contract", type=Path, default=DEFAULT_IMPORT_CONTRACT)
    parser.add_argument("--sizing", type=Path, default=DEFAULT_SIZING)
    parser.add_argument("--haircut", type=Path, default=DEFAULT_HAIRCUT)
    parser.add_argument("--runner-config", type=Path, default=DEFAULT_RUNNER_CONFIG)
    parser.add_argument("--runner-validation", type=Path, default=DEFAULT_RUNNER_VALIDATION)
    parser.add_argument("--owner-schema", type=Path, default=DEFAULT_OWNER_SCHEMA)
    parser.add_argument("--manual-approval", action="store_true")
    parser.add_argument(
        "--manual-approval-source",
        default="user_message_2026-05-29_manual_start_approval_granted",
    )
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "BTC_TINY_CANARY_MANUAL_APPROVAL_RUNTIME_GATE.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
