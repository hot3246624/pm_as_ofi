#!/usr/bin/env python3
"""Build the canonical strategy decision register update packet.

This packet consolidates the current NAGI + CE25 + B27BC research state after
public-proxy exhaustion, source-truth ingestion contract readiness, and the own
maker telemetry validator fail-closed packet. It reads local artifacts only and
does not fetch data, import credentials, connect to Polymarket, or execute
orders.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATUS = (
    "KEEP_NAGI_CE25_B27BC_STRATEGY_DECISION_REGISTER_UPDATED_"
    "SOURCE_TRUTH_OR_OWN_TELEMETRY_ONLY_NOT_READY"
)
DEFAULT_PUBLIC_PROXY_DECISION = (
    "data/exports/nagi_ce25_b27bc_public_proxy_decision_packet_20260609T0113Z_public_fetch/"
    "decision_register.json"
)
DEFAULT_PREOPEN_STABILITY = (
    "data/exports/nagi_ce25_b27bc_preopen_candidate_stability_audit_20260609T0113Z/"
    "decision_register.json"
)
DEFAULT_INGESTION_CONTRACT = (
    "data/exports/nagi_ce25_b27bc_source_truth_telemetry_ingestion_contract_packet_20260609/"
    "decision_register.json"
)
DEFAULT_TELEMETRY_VALIDATOR = (
    "data/exports/nagi_ce25_b27bc_own_maker_telemetry_validator_packet_20260609/"
    "decision_register.json"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def non_claims_false(name: str, item: dict[str, Any]) -> bool:
    non_claims = item.get("non_claims") or {}
    flipped = [key for key, value in non_claims.items() if bool(value)]
    if flipped:
        raise RuntimeError(f"{name} has non_claims set true: {flipped}")
    return True


def build_decision(args: argparse.Namespace) -> dict[str, Any]:
    paths = {
        "public_proxy_decision": Path(args.public_proxy_decision),
        "preopen_stability": Path(args.preopen_stability),
        "ingestion_contract": Path(args.ingestion_contract),
        "telemetry_validator": Path(args.telemetry_validator),
    }
    artifacts = {name: read_json(path) for name, path in paths.items()}
    for name, artifact in artifacts.items():
        non_claims_false(name, artifact)

    public_proxy = artifacts["public_proxy_decision"]
    preopen = artifacts["preopen_stability"]
    ingestion = artifacts["ingestion_contract"]
    telemetry = artifacts["telemetry_validator"]

    public_proxy_exhausted = bool(public_proxy.get("public_proxy_families_exhausted"))
    telemetry_fail_closed = bool(telemetry.get("passed") is False)
    telemetry_validator_ready = "OWN_MAKER_TELEMETRY_VALIDATOR" in str(telemetry.get("status", ""))
    ingestion_contract_ready = "SOURCE_TRUTH_TELEMETRY_INGESTION_CONTRACT" in str(ingestion.get("status", ""))
    no_passing_preopen = int(preopen.get("passing_grid_row_count") or 0) == 0

    return {
        "generated_at": utc_now(),
        "status": STATUS,
        "evidence_level": "canonical_research_decision_register",
        "canonical_inputs": {name: str(path) for name, path in paths.items()},
        "go_no_go_register": {
            "ce25_taker_seed_px_mainline": {
                "decision": "NO_GO",
                "reason": "Executable-price replay inverted the seed_px/taker edge; retain CE25 only as coverage/gating context.",
            },
            "nagi_ce25_b27bc_public_proxy_mainline": {
                "decision": "NO_GO",
                "public_proxy_families_exhausted": public_proxy_exhausted,
                "best_residual_rate": public_proxy.get("best_residual_rate"),
                "residual_target_miss_bps": public_proxy.get("residual_target_miss_bps"),
                "reason": "Current source set misses residual target after B27BC public-account pooling.",
            },
            "b27bc_direct_public_pooling": {
                "decision": "NO_GO",
                "preopen_passing_grid_row_count": preopen.get("passing_grid_row_count"),
                "reason": "B27BC remains a residual-control archetype, but pooled public rows invalidated the narrow pre-open pass.",
            },
            "source_truth_ingestion_contract": {
                "decision": "KEEP_TOOLING",
                "ready": ingestion_contract_ready,
                "next_action": ingestion.get("next_executable_action"),
            },
            "own_maker_telemetry_validator": {
                "decision": "KEEP_TOOLING_FAIL_CLOSED",
                "ready": telemetry_validator_ready,
                "passed": telemetry.get("passed"),
                "blockers": telemetry.get("blockers"),
                "fail_closed_without_sample": telemetry_fail_closed,
            },
        },
        "highest_allowed_status": "local_research_no_order_maker_shadow_proxy_not_oos_ready_not_live_ready",
        "next_material_paths": [
            "drop_new_non_smoke_source_truth_or_public_event_rows_into_data_inputs_or_evidence",
            "drop_own_maker_telemetry_sample_csv_into_data_inputs_or_evidence",
        ],
        "rejected_next_paths": [
            "ce25_taker_seed_px_tuning",
            "public_profile_direct_copy",
            "blind_public_proxy_parameter_grids_without_new_source_truth",
            "b27bc_direct_public_row_pooling_as_rescue",
        ],
        "blockers": [
            "own_maker_telemetry_sample_missing",
            "maker_fill_truth_not_proven",
            "private_queue_priority_not_observed",
            "new_source_truth_input_missing",
        ],
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "next_executable_action": "sentinel_wait_for_real_source_truth_or_own_maker_telemetry_input",
    }


def write_report(path: Path, decision: dict[str, Any]) -> None:
    register = decision["go_no_go_register"]
    lines = [
        "# NAGI + CE25 + B27BC Strategy Decision Register Update",
        "",
        f"Generated at: `{decision['generated_at']}`",
        "",
        "## Decision",
        "",
        f"- Status: `{decision['status']}`",
        f"- Highest allowed status: `{decision['highest_allowed_status']}`",
        "- This is local research/no-order evidence only.",
        "",
        "## GO / NO-GO Register",
        "",
    ]
    for name, entry in register.items():
        lines.append(f"- `{name}`: `{entry['decision']}`")
        reason = entry.get("reason")
        if reason:
            lines.append(f"  Reason: {reason}")
    lines.extend(["", "## Next Material Paths", ""])
    lines.extend(f"- `{item}`" for item in decision["next_material_paths"])
    lines.extend(["", "## Rejected Paths", ""])
    lines.extend(f"- `{item}`" for item in decision["rejected_next_paths"])
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{item}`" for item in decision["blockers"])
    lines.extend(
        [
            "",
            "## Non-Claims",
            "",
            "No readiness, private truth, maker-fill truth, order execution, canary, or live claims are made.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--public-proxy-decision", default=DEFAULT_PUBLIC_PROXY_DECISION)
    parser.add_argument("--preopen-stability", default=DEFAULT_PREOPEN_STABILITY)
    parser.add_argument("--ingestion-contract", default=DEFAULT_INGESTION_CONTRACT)
    parser.add_argument("--telemetry-validator", default=DEFAULT_TELEMETRY_VALIDATOR)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    decision = build_decision(args)
    write_json(out / "decision_register.json", decision)
    write_report(out / "STRATEGY_DECISION_REGISTER_UPDATE_REPORT.md", decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
