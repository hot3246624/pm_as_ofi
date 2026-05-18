#!/usr/bin/env python3
"""Create a local allowlist manifest for a future G2 canary executor payload.

This does not package, sync, deploy, SSH, build, or run anything. It records
the source files a separately reviewed executor may sync after exact approval.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CORE_SOURCE_FILES = [
    "Cargo.toml",
    "Cargo.lock",
    "src/bin/polymarket_v2.rs",
    "src/polymarket/mod.rs",
    "src/polymarket/messages.rs",
    "src/polymarket/coordinator.rs",
    "src/polymarket/coordinator_xuan_b27_dplus.rs",
    "src/polymarket/coordinator_tests.rs",
    "src/polymarket/executor.rs",
    "src/polymarket/order_manager.rs",
    "src/polymarket/recorder.rs",
    "src/polymarket/strategy.rs",
    "src/polymarket/strategy/xuan_b27_dplus.rs",
    "src/polymarket/user_ws.rs",
    "src/polymarket/claims.rs",
    "src/polymarket/xuan_b27_dplus_correlation.rs",
    "src/polymarket/xuan_b27_dplus_execution_controller.rs",
    "src/polymarket/xuan_b27_dplus_ledger.rs",
    "src/polymarket/xuan_b27_dplus_oms_adapter.rs",
    "src/polymarket/xuan_b27_dplus_order_plan.rs",
]

EXECUTOR_GATE_FILES = [
    "scripts/xuan_b27_dplus_g2_canary_approval_envelope.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_contract.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_contract_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py",
    "scripts/xuan_b27_dplus_g2_canary_effectful_executor_review_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_executor.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_dry_run_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_executor_payload_manifest.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_launcher_refusal.py",
    "scripts/xuan_b27_dplus_g2_canary_launcher_refusal_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_launch_plan.py",
    "scripts/xuan_b27_dplus_g2_canary_launch_plan_smoke.sh",
    "scripts/xuan_b27_dplus_summarize_g2_canary_run.py",
    "scripts/xuan_b27_dplus_g2_canary_review_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_runbook_smoke.sh",
    "scripts/xuan_b27_dplus_g2_canary_acceptance_smoke.sh",
    "scripts/xuan_b27_dplus_canary_readiness_plan.py",
    "scripts/xuan_b27_dplus_canary_readiness_plan_smoke.sh",
    "scripts/xuan_b27_dplus_extract_shadow_edge_samples.py",
    "scripts/xuan_b27_dplus_extract_shadow_edge_samples_smoke.sh",
    "scripts/xuan_b27_dplus_l1_dry_run_outcome_labels.py",
    "scripts/xuan_b27_dplus_l1_dry_run_outcome_labels_smoke.sh",
    "scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py",
    "scripts/xuan_b27_dplus_no_order_shadow_run_artifact_smoke.sh",
    "scripts/xuan_b27_dplus_shadow_acceptance_input_discovery.py",
    "scripts/xuan_b27_dplus_shadow_acceptance_input_discovery_smoke.sh",
    "scripts/xuan_b27_dplus_realized_outcome_labels.py",
    "scripts/xuan_b27_dplus_realized_outcome_labels_smoke.sh",
    "scripts/xuan_b27_dplus_outcome_label_bridge.py",
    "scripts/xuan_b27_dplus_outcome_label_bridge_smoke.sh",
    "scripts/xuan_b27_dplus_shadow_performance_evidence.py",
    "scripts/xuan_b27_dplus_shadow_performance_evidence_smoke.sh",
    "scripts/xuan_b27_dplus_shadow_trading_acceptance.py",
    "scripts/xuan_b27_dplus_shadow_trading_acceptance_smoke.sh",
    "scripts/xuan_b27_dplus_shadow_trading_report_discovery.py",
    "scripts/xuan_b27_dplus_shadow_trading_report_discovery_smoke.sh",
    "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py",
    "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_smoke.sh",
    "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py",
    "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner_smoke.sh",
    "scripts/xuan_b27_dplus_local_status_bundle.py",
    "scripts/xuan_b27_dplus_local_status_bundle_smoke.sh",
    "scripts/xuan_b27_dplus_auth_observer_readiness_check.sh",
    "scripts/xuan_b27_dplus_shared_ingress_preflight.py",
    "scripts/xuan_b27_dplus_auth_source_preflight.py",
    "scripts/xuan_b27_dplus_source_truth_runtime_gate_smoke.sh",
    "scripts/xuan_b27_dplus_runtime_wiring_smoke.sh",
    "scripts/xuan_b27_dplus_oms_adapter_smoke.sh",
    "scripts/xuan_b27_dplus_execution_controller_smoke.sh",
    "scripts/xuan_b27_dplus_canary_order_plan_smoke.sh",
    "scripts/xuan_b27_dplus_source_of_truth_schema_smoke.sh",
]

DOC_REVIEW_FILES = [
    "docs/research/xuan/DPLUS_G2_CANARY_RUNBOOK_ZH.md",
    "docs/research/xuan/DPLUS_RUNTIME_MODULE_READINESS_ZH.md",
    "docs/research/xuan/DPLUS_SOURCE_OF_TRUTH_SCHEMA_ZH.md",
    "docs/reference/CONFIG_REFERENCE_ZH.md",
]

FORBIDDEN_PATH_FRAGMENTS = (
    ".env",
    "xuan_research_artifacts/",
    "target/",
    ".git/",
    "/raw/",
    "raw/",
    "replay",
    "/mnt/poly-replay",
    "/srv/pm_as_ofi",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def first_line(path: Path) -> str:
    with path.open("rb") as fh:
        return fh.readline(256).decode("utf-8", errors="replace").rstrip("\r\n")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def file_record(root: Path, rel: str, category: str) -> dict[str, Any]:
    path = root / rel
    record: dict[str, Any] = {
        "path": rel,
        "category": category,
        "expected_executable": rel.startswith("scripts/"),
        "exists": path.exists(),
        "is_file": path.is_file(),
    }
    if path.is_file():
        mode = path.stat().st_mode
        line = first_line(path)
        record.update(
            {
                "size_bytes": path.stat().st_size,
                "sha256": sha256(path),
                "mode_octal": f"{mode & 0o777:o}",
                "is_executable": bool(mode & 0o111),
                "has_shebang": line.startswith("#!"),
            }
        )
        if line.startswith("#!"):
            record["shebang"] = line
    return record


def forbidden_hits(paths: list[str]) -> list[str]:
    hits: list[str] = []
    for rel in paths:
        normalized = rel.replace("\\", "/")
        for fragment in FORBIDDEN_PATH_FRAGMENTS:
            if fragment in normalized:
                hits.append(rel)
                break
    return hits


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_g2_canary_executor_payload_manifest_{label}"

    records = (
        [file_record(root, rel, "core_source") for rel in CORE_SOURCE_FILES]
        + [file_record(root, rel, "executor_gate") for rel in EXECUTOR_GATE_FILES]
        + [file_record(root, rel, "doc_review") for rel in DOC_REVIEW_FILES]
    )
    paths = [record["path"] for record in records]
    missing_required = [
        record["path"]
        for record in records
        if not (record.get("exists") is True and record.get("is_file") is True)
    ]
    missing_executable = [
        record["path"]
        for record in records
        if record.get("expected_executable") is True
        and not (
            record.get("exists") is True
            and record.get("is_file") is True
            and record.get("is_executable") is True
            and record.get("has_shebang") is True
        )
    ]
    forbidden = forbidden_hits(paths)
    duplicate_paths = sorted({path for path in paths if paths.count(path) > 1})

    status = "PASS" if not missing_required and not missing_executable and not forbidden and not duplicate_paths else "FAIL"
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_g2_canary_executor_payload_manifest",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_g2_canary_executor_payload_manifest",
        "payload_records": records,
        "payload_file_count": len(records),
        "core_source_count": len(CORE_SOURCE_FILES),
        "executor_gate_count": len(EXECUTOR_GATE_FILES),
        "doc_review_count": len(DOC_REVIEW_FILES),
        "expected_executable_files": [record["path"] for record in records if record["expected_executable"]],
        "missing_required_files": missing_required,
        "missing_executable_files": missing_executable,
        "forbidden_path_hits": forbidden,
        "duplicate_paths": duplicate_paths,
        "requires_reviewed_executor_implementation": True,
        "requires_exact_g2_canary_approval_before_sync": True,
        "orders_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "packaged_files": False,
            "synced_files": False,
            "deployed_code": False,
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "started_broker": False,
            "connected_to_broker": False,
            "remote_files_written": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
        },
        "next_gate": "review and implement an executor that syncs only this allowlisted payload after exact approval",
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
