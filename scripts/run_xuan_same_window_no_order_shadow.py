#!/usr/bin/env python3
"""Executable no-order backend for the BTC same-window runtime binding.

This script is intentionally conservative. It validates a runtime binding and,
unless run with --validate-only, writes a local status/event stub. It does not
load private keys, import candidates, create orders, cancel, redeem, deploy, or
start any remote process.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any


TARGET_FILTER_NAME = "btc_same_window_residual_share_le_3pct_v1"
TARGET_ASSET = "BTC"
PRIVATE_KEY_ENV_NAMES = (
    "POLYMARKET_PRIVATE_KEY",
    "PK",
    "PRIVATE_KEY",
    "CLOB_PRIVATE_KEY",
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True) + "\n")


def body(config: dict[str, Any], key: str) -> dict[str, Any]:
    value = config.get(key)
    return value if isinstance(value, dict) else {}


def private_key_env_present() -> list[str]:
    return [name for name in PRIVATE_KEY_ENV_NAMES if os.environ.get(name)]


def validate_config(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required_false = [
        "orders_allowed",
        "live_orders_allowed",
        "candidate_import_allowed",
        "live_import_enabled",
        "remote_runner_allowed",
        "deployable",
        "private_truth_ready",
        "owner_private_truth_ready",
    ]
    for key in required_false:
        if config.get(key) is not False:
            errors.append(f"{key}_must_be_false")
    if config.get("dry_run_only") is not True:
        errors.append("dry_run_only_must_be_true")
    if config.get("no_order") is not True:
        errors.append("no_order_must_be_true")

    source = body(config, "candidate_source")
    if source.get("filter_name") != TARGET_FILTER_NAME:
        errors.append("candidate_source_filter_name_mismatch")
    if source.get("assets") != [TARGET_ASSET]:
        errors.append("candidate_source_asset_scope_mismatch")
    if source.get("candidate_count") != 52:
        errors.append("candidate_source_candidate_count_mismatch")

    selector = body(config, "live_market_selector")
    if selector.get("mode") != "prefix_offsets":
        errors.append("live_market_selector_mode_missing")
    if selector.get("prefix") != "btc-updown-5m":
        errors.append("live_market_selector_prefix_mismatch")
    if selector.get("round_offsets") != [1, 2, 3]:
        errors.append("live_market_selector_offsets_mismatch")
    resolved = selector.get("resolved_markets")
    if not isinstance(resolved, list) or len(resolved) != 3:
        errors.append("live_market_selector_resolved_market_count_mismatch")
    else:
        for idx, market in enumerate(resolved):
            if not isinstance(market, dict):
                errors.append(f"resolved_market_{idx}_not_object")
                continue
            if not all(market.get(key) for key in ("slug", "market_id", "yes_asset_id", "no_asset_id")):
                errors.append(f"resolved_market_{idx}_missing_ids")

    source_semantics = body(config, "source_semantics")
    if not source_semantics.get("source_semantics_contract_id"):
        errors.append("source_semantics_contract_id_missing")
    if not source_semantics.get("source_dataset_fingerprint"):
        errors.append("source_dataset_fingerprint_missing")
    if not source_semantics.get("l2_top_overlay_contract_id"):
        errors.append("l2_top_overlay_contract_id_missing")

    owner_truth = body(config, "owner_truth_collection")
    if not owner_truth.get("output_root"):
        errors.append("owner_truth_output_root_missing")
    if not owner_truth.get("schema_path"):
        errors.append("owner_truth_schema_path_missing")
    if owner_truth.get("future_owner_execution_required") is not True:
        errors.append("owner_truth_future_execution_requirement_missing")

    if private_key_env_present():
        errors.append("private_key_env_present_refusing_even_no_order_backend")
    return errors


def build_status(config_path: Path, config: dict[str, Any], errors: list[str]) -> dict[str, Any]:
    selector = body(config, "live_market_selector")
    return {
        "status": (
            "KEEP_XUAN_SAME_WINDOW_NO_ORDER_SHADOW_BACKEND_VALIDATED_LOCAL_ONLY"
            if not errors
            else "BLOCKED_XUAN_SAME_WINDOW_NO_ORDER_SHADOW_BACKEND_CONFIG_INVALID_LOCAL_ONLY"
        ),
        "created_utc": utc_now(),
        "config_path": str(config_path.expanduser().resolve()),
        "runner_profile_id": config.get("runner_profile_id"),
        "filter_name": body(config, "candidate_source").get("filter_name"),
        "dry_run_only": config.get("dry_run_only"),
        "no_order": config.get("no_order"),
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "candidate_import_allowed": False,
        "live_orders_allowed": False,
        "private_key_loaded": False,
        "resolved_market_count": len(selector.get("resolved_markets") or []),
        "validation_errors": errors,
        "policy": {
            "continuous_shadow_started": False,
            "this_backend_is_no_order_only": True,
            "owner_private_truth_data_ready": False,
            "residual_settlement_pnl_is_strategy_edge": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the runtime binding without writing runtime status/events.",
    )
    args = parser.parse_args()

    config_path = args.config.expanduser().resolve()
    config = load_json(config_path)
    errors = validate_config(config)
    status = build_status(config_path, config, errors)

    if not args.validate_only:
        log_plan = body(config, "log_plan")
        status_path = Path(str(log_plan.get("status_json") or "")).expanduser()
        events_path = Path(str(log_plan.get("events_jsonl") or "")).expanduser()
        if not status_path:
            raise SystemExit("log_plan.status_json missing")
        if not events_path:
            raise SystemExit("log_plan.events_jsonl missing")
        write_json(status_path, status)
        append_jsonl(
            events_path,
            {
                "event": "no_order_runtime_binding_validated",
                "ts": utc_now(),
                "status": status["status"],
                "orders_sent": False,
                "live_orders_allowed": False,
                "candidate_import_allowed": False,
            },
        )

    print(json.dumps({"status": status["status"], "validation_errors": errors}, sort_keys=True))
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
