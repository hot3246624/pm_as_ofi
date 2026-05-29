#!/usr/bin/env python3
"""Build a local BTC <=3pct no-order runtime binding package.

The package splits the historical research filter contract from the live market
selector. It does not start a shadow runner, import candidates, deploy, or send
orders. The output is intended for the xuan start-binding reviewer.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


STAMP = "20260529T0520Z"
REPO = Path("/Users/hot/web3Scientist/pm_as_ofi-xuan-frontier")
POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"
CANARY_ROOT = CONTRACT_ROOT / "btc_same_window_residual_share_le_3pct_v1_canary_preflight_latest"
DEFAULT_IMPORT_CONTRACT = CANARY_ROOT / "research_only_import_contract.csv"
DEFAULT_CANARY_MANIFEST = CANARY_ROOT / "manifest.json"
DEFAULT_SOURCE_SEMANTICS = CANARY_ROOT / "source_semantics_contract.json"
DEFAULT_CAPITAL_LEDGER = CANARY_ROOT / "filter_capital_ledger.json"
DEFAULT_OWNER_TRUTH_SCHEMA = CANARY_ROOT / "owner_private_truth_schema.json"
DEFAULT_NO_ORDER_EVAL = (
    CONTRACT_ROOT
    / "xuan_btc_tiny_canary_no_order_shadow_eval_latest"
    / "XUAN_BTC_TINY_CANARY_NO_ORDER_SHADOW_EVAL.json"
)
DEFAULT_SCORECARD = Path(
    ".tmp_xuan/scorecards/"
    f"xuan_shadow_review_backtest_v1_same_window_runtime_binding_package_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_same_window_runtime_binding_package_{STAMP}"
)

TARGET_FILTER_NAME = "btc_same_window_residual_share_le_3pct_v1"
TARGET_FILTER_VERSION = "v1"
TARGET_ASSET = "BTC"
SAFE_OFFSETS = [1, 2, 3]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def clean(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: clean(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.expanduser().resolve().open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.expanduser().resolve().open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_markets(artifact_dir: Path, offsets: list[int]) -> dict[str, Any]:
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
            "btc-updown-5m",
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
                    "parse_error": "missing_json_payload",
                    "stderr_tail": result.stderr[-500:],
                    "stdout_tail": result.stdout[-500:],
                }
            )
            continue
        resolved.append(
            {
                "round_offset": offset,
                "slug": payload.get("slug"),
                "market_id": payload.get("market_id"),
                "yes_asset_id": payload.get("yes_asset_id"),
                "no_asset_id": payload.get("no_asset_id"),
                "endDate": payload.get("endDate"),
                "outcomes": payload.get("outcomes"),
            }
        )
    return {
        "mode": "prefix_offsets",
        "prefix": "btc-updown-5m",
        "round_offsets": offsets,
        "exclude_round_offset_0_for_initial_canary": True,
        "cache_path": str(cache_path),
        "resolved_markets": resolved,
        "resolved_count": len(resolved),
        "errors": errors,
        "preview_ready": len(resolved) == len(offsets) and not errors,
    }


def build_candidate_binding(rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[str]]:
    fields = [
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
        "binding_role",
        "live_selector_policy",
    ]
    out: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: int(item.get("candidate_rank") or 0)):
        item = dict(row)
        item["binding_role"] = "historical_research_filter_contract_not_live_market_selector"
        item["live_selector_policy"] = "btc-updown-5m round_offsets [1,2,3]"
        out.append(item)
    return out, fields


def checklist(config: dict[str, Any], selector: dict[str, Any], backend_exists: bool) -> dict[str, Any]:
    checks = [
        ("backend_script_exists", backend_exists, "scripts/run_xuan_same_window_no_order_shadow.py"),
        ("dry_run_only_true", config.get("dry_run_only") is True, "dry_run_only=true"),
        ("no_order_true", config.get("no_order") is True, "no_order=true"),
        ("orders_allowed_false", config.get("orders_allowed") is False, "orders_allowed=false"),
        ("live_orders_allowed_false", config.get("live_orders_allowed") is False, "live_orders_allowed=false"),
        (
            "candidate_import_allowed_false",
            config.get("candidate_import_allowed") is False,
            "candidate_import_allowed=false",
        ),
        ("live_selector_preview_ready", selector.get("preview_ready") is True, "3 future-offset BTC markets"),
        (
            "owner_truth_output_binding_ready",
            isinstance(config.get("owner_truth_collection"), dict),
            "future owner truth output binding present",
        ),
    ]
    return {
        "checks": [
            {"name": name, "passed": bool(passed), "evidence": evidence}
            for name, passed, evidence in checks
        ],
        "all_passed": all(bool(passed) for name, passed, evidence in checks),
    }


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    import_rows = read_csv_rows(args.import_contract)
    canary_manifest = load_json(args.canary_manifest)
    source_semantics = load_json(args.source_semantics)
    capital_ledger = load_json(args.capital_ledger)
    owner_schema = load_json(args.owner_truth_schema)
    no_order_eval = load_json(args.no_order_eval)
    selector = resolve_markets(args.artifact_dir, SAFE_OFFSETS)

    candidate_binding_rows, candidate_fields = build_candidate_binding(import_rows)
    candidate_binding_path = args.artifact_dir / "runtime_candidate_binding.csv"
    write_csv_rows(candidate_binding_path, candidate_binding_rows, candidate_fields)

    runtime_root = args.artifact_dir / "runtime_logs"
    owner_truth_root = args.artifact_dir / "future_owner_truth_outputs"
    config_path = args.artifact_dir / "xuan_same_window_no_order_shadow_runtime_config.json"
    backend_script = REPO / "scripts/run_xuan_same_window_no_order_shadow.py"
    start_preview = args.artifact_dir / "start_command_preview.txt"

    config = {
        "schema_version": "xuan_same_window_no_order_shadow_runtime_binding_v1",
        "created_utc": utc_now(),
        "mode": "no_order_shadow_runtime_binding_ready_not_started",
        "runner_profile_id": "btc_same_window_tiny_canary_dryrun_v1",
        "dry_run_only": True,
        "no_order": True,
        "orders_allowed": False,
        "live_orders_allowed": False,
        "candidate_import_allowed": False,
        "live_import_enabled": False,
        "remote_runner_allowed": False,
        "deployable": False,
        "private_truth_ready": False,
        "owner_private_truth_ready": False,
        "candidate_source": {
            "filter_name": TARGET_FILTER_NAME,
            "filter_version": TARGET_FILTER_VERSION,
            "assets": [TARGET_ASSET],
            "candidate_count": len(import_rows),
            "csv": str(candidate_binding_path),
            "sha256": sha256_file(candidate_binding_path),
            "research_filter_contract_only": True,
            "historical_rows_are_not_live_selector": True,
            "source_import_contract": str(args.import_contract),
            "source_import_contract_sha256": sha256_file(args.import_contract),
        },
        "live_market_selector": selector,
        "source_semantics": {
            "source_semantics_contract_id": source_semantics.get("source_semantics_contract_id"),
            "source_dataset_fingerprint": source_semantics.get("source_dataset_fingerprint"),
            "l2_top_overlay_contract_id": source_semantics.get("l2_top_overlay_contract_id"),
            "normalized_buy_adapter_policy": source_semantics.get("source_semantics_policy"),
            "source_semantics_contract_path": str(args.source_semantics),
            "source_semantics_contract_sha256": sha256_file(args.source_semantics),
        },
        "capital_policy": {
            "capital_ledger_path": str(args.capital_ledger),
            "capital_ledger_sha256": sha256_file(args.capital_ledger),
            "recommended_max_notional_cap": capital_ledger.get("recommended_max_notional_cap"),
            "do_not_size_from_residual_settlement_pnl": True,
            "residual_settlement_pnl_is_strategy_edge": False,
        },
        "owner_truth_collection": {
            "schema_path": str(args.owner_truth_schema),
            "schema_sha256": sha256_file(args.owner_truth_schema),
            "output_root": str(owner_truth_root),
            "future_owner_execution_required": True,
            "owner_private_truth_schema_ready": owner_schema.get("owner_private_truth_schema_ready") is True,
            "owner_private_truth_data_ready": False,
            "tables": owner_schema.get("tables"),
        },
        "log_plan": {
            "root": str(runtime_root),
            "events_jsonl": str(runtime_root / "shadow_events.jsonl"),
            "status_json": str(runtime_root / "shadow_status.json"),
            "orders_sent_initially": False,
        },
        "kill_switch": {
            "manual_stop_required": True,
            "panic_on_any_order_send_attempt": True,
            "panic_on_live_import_enabled": True,
            "stop_file_path": str(args.artifact_dir / "STOP_REQUESTED"),
        },
        "stop_conditions": [
            {"name": "any_order_path_enabled", "action": "stop_and_report"},
            {"name": "live_market_selector_resolution_failure", "action": "stop_and_report"},
            {"name": "private_truth_or_promotion_claim_before_owner_execution", "action": "stop_and_report"},
        ],
        "upstream_no_order_proxy_eval": {
            "path": str(args.no_order_eval),
            "status": no_order_eval.get("status"),
            "evaluation_passed": (no_order_eval.get("summary") or {}).get("evaluation_passed") is True,
        },
    }
    write_json(config_path, config)

    start_preview.write_text(
        "\n".join(
            [
                "# Preview only. Do not run from this artifact without a fresh start gate.",
                "# This backend is dry-run/no-order and refuses private-key env vars.",
                f"uv run python scripts/run_xuan_same_window_no_order_shadow.py --config {config_path}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    checklist_body = checklist(config, selector, backend_script.exists())
    checklist_json = args.artifact_dir / "runtime_preflight_checklist.json"
    checklist_csv = args.artifact_dir / "runtime_preflight_checklist.csv"
    write_json(checklist_json, checklist_body)
    write_csv_rows(checklist_csv, checklist_body["checks"], ["name", "passed", "evidence"])

    decision = {
        "runtime_binding_package_ready": checklist_body["all_passed"],
        "backend_script_exists": backend_script.exists(),
        "target_btc_le3pct_filter_bound": len(import_rows) == 52
        and {row.get("asset") for row in import_rows} == {TARGET_ASSET},
        "live_market_selector_bound": selector.get("preview_ready") is True,
        "owner_truth_collection_runtime_binding_ready": True,
        "candidate_import_allowed": False,
        "remote_runner_allowed": False,
        "deployable": False,
        "live_orders_allowed": False,
        "private_truth_ready": False,
        "shadow_started_by_this_package": False,
    }
    blockers: list[str] = []
    if not decision["backend_script_exists"]:
        blockers.append("backend_script_missing")
    if not decision["target_btc_le3pct_filter_bound"]:
        blockers.append("target_btc_le3pct_filter_not_bound")
    if not decision["live_market_selector_bound"]:
        blockers.append("live_market_selector_not_ready")
    if not checklist_body["all_passed"]:
        blockers.append("runtime_preflight_checklist_failed")

    card = clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_same_window_runtime_binding_package",
            "status": (
                "KEEP_SHADOW_REVIEW_BACKTEST_V1_SAME_WINDOW_RUNTIME_BINDING_PACKAGE_READY_LOCAL_ONLY"
                if not blockers
                else "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_SAME_WINDOW_RUNTIME_BINDING_PACKAGE_NOT_READY_LOCAL_ONLY"
            ),
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_same_window_runtime_binding_package.py",
            "inputs": {
                "import_contract": str(args.import_contract),
                "canary_manifest": str(args.canary_manifest),
                "source_semantics": str(args.source_semantics),
                "capital_ledger": str(args.capital_ledger),
                "owner_truth_schema": str(args.owner_truth_schema),
                "no_order_eval": str(args.no_order_eval),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "runner_config": str(config_path),
                "candidate_binding": str(candidate_binding_path),
                "start_command_preview": str(start_preview),
                "preflight_checklist_json": str(checklist_json),
                "preflight_checklist_csv": str(checklist_csv),
                "markdown": str(args.artifact_dir / "SAME_WINDOW_RUNTIME_BINDING_PACKAGE.md"),
            },
            "decision": decision,
            "canary_summary": canary_manifest.get("summary"),
            "live_market_selector": selector,
            "remaining_blockers": blockers,
            "warnings": [
                "This package does not start a runner.",
                "Historical research filter rows are a filter contract, not a live market selector.",
                "Owner private truth data remains unavailable until future owner execution.",
                "No import/live/remote/order path is enabled.",
            ],
        }
    )
    return card


def render_markdown(card: dict[str, Any]) -> str:
    decision = card.get("decision", {})
    outputs = card.get("outputs", {})
    lines = [
        "# Same-Window Runtime Binding Package",
        "",
        f"- status: `{card.get('status')}`",
        f"- runtime_binding_package_ready: `{decision.get('runtime_binding_package_ready')}`",
        f"- backend_script_exists: `{decision.get('backend_script_exists')}`",
        f"- target_btc_le3pct_filter_bound: `{decision.get('target_btc_le3pct_filter_bound')}`",
        f"- live_market_selector_bound: `{decision.get('live_market_selector_bound')}`",
        f"- owner_truth_collection_runtime_binding_ready: `{decision.get('owner_truth_collection_runtime_binding_ready')}`",
        f"- shadow_started_by_this_package: `{decision.get('shadow_started_by_this_package')}`",
        "",
        "## Outputs",
        "",
        f"- runner_config: `{outputs.get('runner_config')}`",
        f"- candidate_binding: `{outputs.get('candidate_binding')}`",
        f"- start_command_preview: `{outputs.get('start_command_preview')}`",
        "",
        "## Remaining Blockers",
        "",
    ]
    blockers = card.get("remaining_blockers", [])
    if blockers:
        lines.extend(f"- `{blocker}`" for blocker in blockers)
    else:
        lines.append("- none for local runtime-binding package construction")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This is a dry/no-order runtime binding package only. It does not authorize start, candidate import, remote execution, deployment, live orders, or promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--import-contract", type=Path, default=DEFAULT_IMPORT_CONTRACT)
    parser.add_argument("--canary-manifest", type=Path, default=DEFAULT_CANARY_MANIFEST)
    parser.add_argument("--source-semantics", type=Path, default=DEFAULT_SOURCE_SEMANTICS)
    parser.add_argument("--capital-ledger", type=Path, default=DEFAULT_CAPITAL_LEDGER)
    parser.add_argument("--owner-truth-schema", type=Path, default=DEFAULT_OWNER_TRUTH_SCHEMA)
    parser.add_argument("--no-order-eval", type=Path, default=DEFAULT_NO_ORDER_EVAL)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.scorecard, card)
    markdown = args.artifact_dir / "SAME_WINDOW_RUNTIME_BINDING_PACKAGE.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"scorecard": str(args.scorecard), "status": card.get("status")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
