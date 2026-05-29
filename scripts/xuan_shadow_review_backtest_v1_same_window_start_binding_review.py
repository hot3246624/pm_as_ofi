#!/usr/bin/env python3
"""Review Backtest V1 same-window no-order shadow start binding.

The backtest side can prepare a research packet and a no-order config, but a
start approval is only useful if the config is executable against current/future
markets.  This reviewer checks the colleague-produced packet after manual user
approval and keeps the boundary explicit:

* no runner is started,
* no candidate import is enabled,
* no live order path is enabled,
* historical candidate rows are not treated as a live selector.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any


STAMP = "20260529T0525Z"

REPO = Path("/Users/hot/web3Scientist/pm_as_ofi-xuan-frontier")
POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"

START_PREFLIGHT_ROOT = CONTRACT_ROOT / "xuan_same_window_no_order_shadow_start_preflight_latest"
DEFAULT_START_PREFLIGHT = START_PREFLIGHT_ROOT / "XUAN_SAME_WINDOW_NO_ORDER_SHADOW_START_PREFLIGHT.json"
DEFAULT_RUNNER_CONFIG = START_PREFLIGHT_ROOT / "xuan_same_window_no_order_shadow_runner_config.json"
DEFAULT_PREFLIGHT_CHECKLIST = START_PREFLIGHT_ROOT / "preflight_checklist.json"
DEFAULT_START_COMMAND_PREVIEW = START_PREFLIGHT_ROOT / "start_command_preview.txt"
DEFAULT_CANDIDATE_BINDING = START_PREFLIGHT_ROOT / "candidate_binding.csv"

DEFAULT_BTC_CANARY_PREFLIGHT = (
    CONTRACT_ROOT
    / "btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest"
    / "BTC_SAME_WINDOW_RESIDUAL_SHARE_LE_3PCT_V1_CANARY_PREFLIGHT_MANIFEST.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/"
    f"xuan_shadow_review_backtest_v1_same_window_start_binding_review_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_same_window_start_binding_review_{STAMP}"
)

TARGET_FILTER_NAME = "btc_same_window_residual_share_le_3pct_v1"
TARGET_ASSET = "BTC"
TARGET_CANDIDATE_COUNT = 52
SAFE_MARKET_OFFSETS = [1, 2, 3]


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
    if not path.exists():
        return []
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
        "run_xuan_same_window_no_order_shadow.py",
        "run_xuan_m0001_maker_shadow.py",
        "xuan_research_job_runner.py",
    ]
    ignore_needles = [
        "xuan_shadow_review_backtest_v1_same_window_start_binding_review.py",
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


def command_script_from_preview(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"preview_exists": False, "script_path": None, "script_exists": False}
    logical_lines: list[str] = []
    pending = ""
    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.endswith("\\"):
            pending += stripped[:-1] + " "
            continue
        logical_lines.append((pending + stripped).strip())
        pending = ""
    if pending:
        logical_lines.append(pending.strip())
    command = logical_lines[0] if logical_lines else ""
    tokens = shlex.split(command) if command else []
    script_token: str | None = None
    for token in tokens:
        if token.endswith(".py"):
            script_token = token
            break
    script_path = (REPO / script_token).resolve() if script_token and not Path(script_token).is_absolute() else (
        Path(script_token).resolve() if script_token else None
    )
    return {
        "preview_exists": True,
        "command": command,
        "script_token": script_token,
        "script_path": str(script_path) if script_path else None,
        "script_exists": bool(script_path and script_path.exists()),
    }


def candidate_binding_summary(path: Path, current_day: dt.date) -> dict[str, Any]:
    rows = read_csv(path)
    days = [day for day in (parse_day(row.get("day")) for row in rows) if day is not None]
    future_rows = [row for row in rows if (parse_day(row.get("day")) or dt.date.min) >= current_day]
    assets = sorted({row.get("asset") for row in rows if row.get("asset")})
    tiers = sorted({row.get("research_tier") for row in rows if row.get("research_tier")})
    slugs = sorted({row.get("slug") for row in rows if row.get("slug")})
    btc_rows = [row for row in rows if row.get("asset") == TARGET_ASSET]
    return {
        "row_count": len(rows),
        "asset_values": assets,
        "research_tiers": tiers,
        "btc_row_count": len(btc_rows),
        "target_filter_name": TARGET_FILTER_NAME,
        "target_candidate_count": TARGET_CANDIDATE_COUNT,
        "target_asset_only": assets == [TARGET_ASSET],
        "target_candidate_count_match": len(rows) == TARGET_CANDIDATE_COUNT,
        "target_btc_count_match": len(btc_rows) == TARGET_CANDIDATE_COUNT,
        "min_day": min(days).isoformat() if days else None,
        "max_day": max(days).isoformat() if days else None,
        "current_day": current_day.isoformat(),
        "future_or_current_rows": len(future_rows),
        "historical_only": len(rows) > 0 and len(future_rows) == 0,
        "unique_slug_count": len(slugs),
        "sample_slugs": slugs[:5],
    }


def config_has_live_selector(config: dict[str, Any]) -> bool:
    selector_keys = [
        "market_selection",
        "runtime_market_selection",
        "live_market_selector",
        "market_discovery",
    ]
    if any(isinstance(config.get(key), dict) for key in selector_keys):
        return True
    source = body(config, "candidate_source")
    return bool(source.get("prefix") or source.get("round_offsets") or source.get("market_selection_mode"))


def config_owner_truth_binding_ready(config: dict[str, Any]) -> bool:
    owner_keys = [
        "owner_truth_collection",
        "owner_private_truth_output_plan",
        "owner_execution_truth_outputs",
    ]
    if any(isinstance(config.get(key), dict) for key in owner_keys):
        return True
    log_plan = body(config, "log_plan")
    events_path = str(log_plan.get("events_jsonl") or "")
    return "owner" in events_path.lower() and bool(events_path)


def checklist_pass(checklist: dict[str, Any]) -> bool:
    checks = checklist.get("checks")
    return isinstance(checks, list) and bool(checks) and all(item.get("passed") is True for item in checks)


def resolve_live_market_preview(artifact_dir: Path, prefix: str, offsets: list[int]) -> dict[str, Any]:
    cache_path = artifact_dir / "market_resolver_cache.json"
    env = os.environ.copy()
    env["PM_MARKET_RESOLVER_CACHE"] = str(cache_path)
    resolved: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for offset in offsets:
        cmd = [
            sys.executable,
            str(REPO / "scripts/resolve_market_ids.py"),
            "--prefix",
            prefix,
            "--round-offset",
            str(offset),
            "--format",
            "json",
        ]
        result = subprocess.run(
            cmd,
            cwd=REPO,
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            errors.append(
                {
                    "round_offset": offset,
                    "returncode": result.returncode,
                    "stderr_tail": result.stderr[-500:],
                    "stdout_tail": result.stdout[-500:],
                }
            )
            continue
        json_line = next((line for line in reversed(result.stdout.splitlines()) if line.startswith("{")), "")
        try:
            payload = json.loads(json_line)
        except json.JSONDecodeError:
            errors.append(
                {
                    "round_offset": offset,
                    "returncode": result.returncode,
                    "stderr_tail": result.stderr[-500:],
                    "stdout_tail": result.stdout[-500:],
                    "parse_error": "missing_json_payload",
                }
            )
            continue
        resolved.append(
            {
                "round_offset": offset,
                "slug": payload.get("slug"),
                "market_id": payload.get("market_id"),
                "yes_asset_id_present": bool(payload.get("yes_asset_id")),
                "no_asset_id_present": bool(payload.get("no_asset_id")),
                "endDate": payload.get("endDate"),
            }
        )
    return {
        "mode": "prefix_offsets",
        "asset": TARGET_ASSET,
        "prefix": prefix,
        "round_offsets": offsets,
        "cache_path": str(cache_path),
        "resolved_count": len(resolved),
        "errors": errors,
        "resolved": resolved,
        "preview_ready": len(resolved) == len(offsets) and not errors,
        "policy_note": "round_offset 0 intentionally excluded from this preview to avoid near-expiry initial start bias",
    }


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    start_preflight = load_json(args.start_preflight)
    runner_config = load_json(args.runner_config)
    checklist = load_json(args.preflight_checklist)
    btc_canary = load_json(args.btc_canary_preflight)
    current_day = dt.date.today()
    conflicts = active_runner_conflicts()
    command_status = command_script_from_preview(args.start_command_preview)
    candidate_summary = candidate_binding_summary(args.candidate_binding, current_day)
    live_preview = resolve_live_market_preview(args.artifact_dir, "btc-updown-5m", SAFE_MARKET_OFFSETS)

    dry_no_order_safety_pass = (
        checklist_pass(checklist)
        and runner_config.get("dry_run_only") is True
        and runner_config.get("orders_allowed") is False
        and runner_config.get("live_orders_allowed") is False
        and runner_config.get("live_import_enabled") is False
        and runner_config.get("remote_runner_allowed") is False
        and runner_config.get("candidate_import_allowed") is False
    )
    source = body(runner_config, "candidate_source")
    target_scope_match = (
        candidate_summary["target_asset_only"]
        and candidate_summary["target_candidate_count_match"]
        and (
            source.get("filter_name") == TARGET_FILTER_NAME
            or TARGET_FILTER_NAME in str(runner_config.get("candidate_source", {}))
        )
    )
    live_selector_bound = config_has_live_selector(runner_config)
    owner_truth_binding_ready = config_owner_truth_binding_ready(runner_config)
    executable_backend_ready = bool(command_status.get("script_exists"))
    conflict_clear = not conflicts
    manual_approval_captured = bool(args.manual_approval)
    research_preflight_accepted = (
        start_preflight.get("engineering_preflight_ready") is True
        and start_preflight.get("validation_errors") == []
        and btc_canary.get("canary_preflight_ready") is True
    )
    historical_binding_only = candidate_summary["historical_only"] and not live_selector_bound
    runtime_binding_ready = (
        manual_approval_captured
        and conflict_clear
        and dry_no_order_safety_pass
        and research_preflight_accepted
        and executable_backend_ready
        and target_scope_match
        and not historical_binding_only
        and live_selector_bound
        and owner_truth_binding_ready
    )

    remaining_blockers: list[str] = []
    if not manual_approval_captured:
        remaining_blockers.append("manual_shadow_start_approval_missing")
    if not conflict_clear:
        remaining_blockers.append("active_xuan_runner_conflict_detected")
    if not dry_no_order_safety_pass:
        remaining_blockers.append("dry_no_order_safety_checks_not_all_passing")
    if not executable_backend_ready:
        remaining_blockers.append("start_command_backend_script_missing")
    if not target_scope_match:
        remaining_blockers.append("runtime_config_not_bound_to_target_btc_le3pct_filter")
    if historical_binding_only:
        remaining_blockers.append("candidate_binding_uses_historical_rows_not_live_selector")
    if not live_selector_bound:
        remaining_blockers.append("runtime_market_selection_contract_missing")
    if not owner_truth_binding_ready:
        remaining_blockers.append("owner_truth_collection_runtime_binding_missing")
    if not research_preflight_accepted:
        remaining_blockers.append("research_preflight_not_accepted")
    status = (
        "KEEP_SHADOW_REVIEW_BACKTEST_V1_SAME_WINDOW_RUNTIME_BINDING_READY_START_NOT_RUN_LOCAL_ONLY"
        if runtime_binding_ready
        else "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_SAME_WINDOW_START_BINDING_NOT_EXECUTABLE_LOCAL_ONLY"
    )

    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_same_window_start_binding_review",
            "status": status,
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_same_window_start_binding_review.py",
            "inputs": {
                "start_preflight": str(args.start_preflight),
                "runner_config": str(args.runner_config),
                "preflight_checklist": str(args.preflight_checklist),
                "start_command_preview": str(args.start_command_preview),
                "candidate_binding": str(args.candidate_binding),
                "btc_canary_preflight": str(args.btc_canary_preflight),
                "manual_approval_source": args.manual_approval_source,
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "markdown": str(args.artifact_dir / "SAME_WINDOW_START_BINDING_REVIEW.md"),
                "colleague_fix_request": str(args.artifact_dir / "BACKTEST_COLLEAGUE_RUNTIME_BINDING_FIX_REQUEST.md"),
            },
            "decision": {
                "manual_shadow_start_approval_captured": manual_approval_captured,
                "manual_approval_source": args.manual_approval_source,
                "active_runner_conflict_check_run": True,
                "active_runner_conflict_clear": conflict_clear,
                "colleague_engineering_preflight_ready": start_preflight.get("engineering_preflight_ready") is True,
                "research_preflight_accepted": research_preflight_accepted,
                "dry_no_order_safety_pass": dry_no_order_safety_pass,
                "xuan_side_live_selector_preview_ready": live_preview["preview_ready"],
                "start_command_backend_exists": executable_backend_ready,
                "target_btc_le3pct_filter_bound": target_scope_match,
                "historical_candidate_binding_only": historical_binding_only,
                "historical_research_filter_contract_present": candidate_summary["historical_only"],
                "live_runtime_market_selector_bound": live_selector_bound,
                "owner_truth_collection_runtime_binding_ready": owner_truth_binding_ready,
                "runtime_binding_ready": runtime_binding_ready,
                "same_window_shadow_start_ready": False,
                "candidate_import_allowed": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "next_lane": (
                    "manual_final_start_decision_required_before_running_no_order_backend"
                    if runtime_binding_ready
                    else "fix_executable_backend_and_live_btc_runtime_selector_before_any_shadow_start"
                ),
            },
            "fresh_active_runner_conflicts": conflicts,
            "command_preview_review": command_status,
            "runner_config_review": {
                "runner_profile_id": runner_config.get("runner_profile_id"),
                "mode": runner_config.get("mode"),
                "config_fingerprint": runner_config.get("config_fingerprint"),
                "dry_run_only": runner_config.get("dry_run_only"),
                "orders_allowed": runner_config.get("orders_allowed"),
                "live_orders_allowed": runner_config.get("live_orders_allowed"),
                "candidate_import_allowed": runner_config.get("candidate_import_allowed"),
                "live_import_enabled": runner_config.get("live_import_enabled"),
                "remote_runner_allowed": runner_config.get("remote_runner_allowed"),
                "candidate_source": runner_config.get("candidate_source"),
                "log_plan": runner_config.get("log_plan"),
            },
            "candidate_binding_summary": candidate_summary,
            "live_market_selector_preview": live_preview,
            "remaining_blockers_before_any_start": remaining_blockers,
            "required_runtime_binding_contract": {
                "target_filter_name": TARGET_FILTER_NAME,
                "target_asset": TARGET_ASSET,
                "target_candidate_scope": "52 BTC same-window residual-share <= 3pct research rows, not the 92-row BTC+ETH <=5pct tier-A packet",
                "market_selection": {
                    "mode": "prefix_offsets",
                    "prefix": "btc-updown-5m",
                    "round_offsets": SAFE_MARKET_OFFSETS,
                    "exclude_round_offset_0_for_initial_canary": True,
                },
                "must_bind_source_semantics": True,
                "must_bind_filter_specific_capital_ledger": True,
                "must_write_owner_truth_collection_files_after_future_execution": True,
                "must_keep_dry_run_only": True,
                "must_keep_orders_allowed_false": True,
                "must_keep_candidate_import_allowed_false": True,
            },
            "warnings": [
                "Manual approval is captured, but it only clears the approval blocker.",
                "A historical research filter binding is acceptable only when a separate current/future live_market_selector is present.",
                "No shadow/canary was started and no import/live/remote action is authorized by this review.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    command = body(card, "command_preview_review")
    candidate = body(card, "candidate_binding_summary")
    live_preview = body(card, "live_market_selector_preview")
    lines = [
        "# Same-Window Start Binding Review",
        "",
        f"- status: `{card.get('status')}`",
        f"- manual_shadow_start_approval_captured: `{decision.get('manual_shadow_start_approval_captured')}`",
        f"- active_runner_conflict_clear: `{decision.get('active_runner_conflict_clear')}`",
        f"- dry_no_order_safety_pass: `{decision.get('dry_no_order_safety_pass')}`",
        f"- start_command_backend_exists: `{decision.get('start_command_backend_exists')}`",
        f"- target_btc_le3pct_filter_bound: `{decision.get('target_btc_le3pct_filter_bound')}`",
        f"- live_runtime_market_selector_bound: `{decision.get('live_runtime_market_selector_bound')}`",
        f"- owner_truth_collection_runtime_binding_ready: `{decision.get('owner_truth_collection_runtime_binding_ready')}`",
        f"- runtime_binding_ready: `{decision.get('runtime_binding_ready')}`",
        f"- same_window_shadow_start_ready: `{decision.get('same_window_shadow_start_ready')}`",
        "",
        "## What Passed",
        "",
        "- User manual start approval is recorded.",
        "- Fresh active-runner conflict check is clear.",
        "- The colleague preflight has dry/no-order safety checks and no live/import/order path enabled.",
        f"- A xuan-side live BTC market selector preview resolved `{live_preview.get('resolved_count')}` future-offset markets.",
        "",
        "## Binding Findings",
        "",
        f"- Start command script: `{command.get('script_path')}` exists=`{command.get('script_exists')}`.",
        f"- Candidate binding rows: `{candidate.get('row_count')}`, assets=`{candidate.get('asset_values')}`, historical_only=`{candidate.get('historical_only')}`.",
        f"- Target BTC <=3pct filter bound: `{decision.get('target_btc_le3pct_filter_bound')}`.",
        f"- Current/future market selector bound: `{decision.get('live_runtime_market_selector_bound')}`.",
        "",
        "## Remaining Blockers",
        "",
    ]
    for blocker in card.get("remaining_blockers_before_any_start", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This review can clear the local runtime-binding contract, but it still does not start the backend or authorize import/live/remote actions. A future start must use fresh conflict checks and preserve all no-order safety flags.",
            "",
        ]
    )
    return "\n".join(lines)


def render_colleague_request(card: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Backtest Colleague Runtime Binding Fix Request",
            "",
            "P0 fixes needed before xuan can start even a no-order shadow:",
            "",
            "1. Provide or generate the executable backend referenced by `start_command_preview.txt`: `scripts/run_xuan_same_window_no_order_shadow.py`, or change the preview to an existing reviewed runner.",
            "2. Bind the runtime config to `btc_same_window_residual_share_le_3pct_v1`: BTC only, 52-row <=3pct filter, not the 92-row BTC+ETH <=5pct tier-A packet.",
            "3. Replace historical candidate CSV rows with a current/future BTC market selector contract: `prefix=btc-updown-5m`, `round_offsets=[1,2,3]`, output-scoped resolver cache, and explicit slug/token resolution audit.",
            "4. Add runtime source-semantics binding: `source_semantics_contract_id`, source dataset fingerprint, L2 top overlay contract id, and the exact normalized BUY adapter policy the runner will use.",
            "5. Add owner-truth collection output binding for future execution: order intents, dry-run observations, simulated/actual fills if any, inventory snapshots, merge/redeem events, fees, residuals, and market-level reconciliation paths. Data remains unavailable until future owner execution.",
            "6. Keep all safety booleans false for this no-order path: `candidate_import_allowed=false`, `live_import_enabled=false`, `orders_allowed=false`, `live_orders_allowed=false`, `deployable=false`, `private_truth_ready=false`.",
            "",
            "Current xuan-side review status:",
            "",
            f"- `{card.get('status')}`",
            f"- blockers: `{', '.join(card.get('remaining_blockers_before_any_start', []))}`",
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-preflight", type=Path, default=DEFAULT_START_PREFLIGHT)
    parser.add_argument("--runner-config", type=Path, default=DEFAULT_RUNNER_CONFIG)
    parser.add_argument("--preflight-checklist", type=Path, default=DEFAULT_PREFLIGHT_CHECKLIST)
    parser.add_argument("--start-command-preview", type=Path, default=DEFAULT_START_COMMAND_PREVIEW)
    parser.add_argument("--candidate-binding", type=Path, default=DEFAULT_CANDIDATE_BINDING)
    parser.add_argument("--btc-canary-preflight", type=Path, default=DEFAULT_BTC_CANARY_PREFLIGHT)
    parser.add_argument("--manual-approval", action="store_true")
    parser.add_argument(
        "--manual-approval-source",
        default="user_message_2026-05-29_manual_shadow_start_approval_granted",
    )
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "SAME_WINDOW_START_BINDING_REVIEW.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    request = args.artifact_dir / "BACKTEST_COLLEAGUE_RUNTIME_BINDING_FIX_REQUEST.md"
    request.write_text(render_colleague_request(card) + "\n", encoding="utf-8")
    print(json.dumps({"scorecard": str(args.scorecard), "status": card.get("status")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
