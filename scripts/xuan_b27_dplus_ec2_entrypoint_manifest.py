#!/usr/bin/env python3
"""Create a local, no-network EC2 entrypoint requirement manifest.

This script does not package, sync, deploy, or contact EC2. It only records
which local files must be present in an EC2 worktree before the read-only User
WS observer exact run can be attempted there.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


REQUIRED_ENTRYPOINT_FILES = [
    "Cargo.toml",
    "Cargo.lock",
    "src/bin/xuan_b27_dplus_user_ws_observer.rs",
    "scripts/xuan_b27_dplus_readonly_user_ws_observer.py",
    "scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py",
    "scripts/xuan_b27_dplus_shared_ingress_preflight.py",
    "scripts/xuan_b27_dplus_auth_source_preflight.py",
]

SUPPORTING_GATE_FILES = [
    "scripts/xuan_b27_dplus_readonly_user_ws_static_smoke.sh",
    "scripts/xuan_b27_dplus_readonly_user_ws_summary_gate_smoke.sh",
    "scripts/xuan_b27_dplus_readonly_user_ws_wrapper_refusal_smoke.sh",
    "scripts/xuan_b27_dplus_ec2_readonly_attempt_smoke.sh",
    "scripts/xuan_b27_dplus_auth_observer_readiness_check.sh",
    "scripts/xuan_b27_dplus_local_status_bundle.py",
    "scripts/xuan_b27_dplus_local_status_bundle_smoke.sh",
]

EXPECTED_EXECUTABLE_FILES = tuple(
    rel for rel in REQUIRED_ENTRYPOINT_FILES + SUPPORTING_GATE_FILES if rel.startswith("scripts/")
)

REQUIRED_RUST_MODULE_HINTS = [
    "src/polymarket/user_ws.rs",
    "src/polymarket/recorder.rs",
    "src/polymarket/executor.rs",
    "src/polymarket/messages.rs",
    "src/polymarket/mod.rs",
]

FORBIDDEN_PATH_FRAGMENTS = (
    ".env",
    "xuan_research_artifacts/",
    "/raw/",
    "raw/",
    "replay",
    "/mnt/poly-replay",
    "target/",
    ".git/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def first_line(path: Path) -> str:
    with path.open("rb") as fh:
        return fh.readline(256).decode("utf-8", errors="replace").rstrip("\r\n")


def file_record(root: Path, rel: str, required: bool) -> dict:
    path = root / rel
    record: dict[str, object] = {
        "path": rel,
        "required": required,
        "expected_executable": rel in EXPECTED_EXECUTABLE_FILES,
        "exists": path.exists(),
        "is_file": path.is_file(),
    }
    if path.is_file():
        mode = path.stat().st_mode
        line = first_line(path)
        record["size_bytes"] = path.stat().st_size
        record["sha256"] = sha256(path)
        record["mode_octal"] = f"{mode & 0o777:o}"
        record["is_executable"] = bool(mode & 0o111)
        record["has_shebang"] = line.startswith("#!")
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
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_ec2_entrypoint_manifest_{label}"

    required_records = [file_record(root, rel, True) for rel in REQUIRED_ENTRYPOINT_FILES]
    support_records = [file_record(root, rel, False) for rel in SUPPORTING_GATE_FILES]
    rust_hint_records = [file_record(root, rel, False) for rel in REQUIRED_RUST_MODULE_HINTS]
    all_records = required_records + support_records + rust_hint_records
    all_paths = [str(record["path"]) for record in all_records]
    missing_required = [
        str(record["path"])
        for record in required_records
        if not (record.get("exists") is True and record.get("is_file") is True)
    ]
    missing_executable = [
        str(record["path"])
        for record in all_records
        if record.get("expected_executable") is True
        and not (
            record.get("exists") is True
            and record.get("is_file") is True
            and record.get("is_executable") is True
            and record.get("has_shebang") is True
        )
    ]
    forbidden = forbidden_hits(all_paths)

    status = "PASS" if not missing_required and not missing_executable and not forbidden else "FAIL"
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_ec2_entrypoint_manifest",
        "status": status,
        "created_utc": label,
        "scope": "local_no_network_ec2_entrypoint_requirement_manifest",
        "strategy": "xuan_b27_dplus",
        "required_entrypoint_files": required_records,
        "supporting_gate_files": support_records,
        "required_rust_module_hints": rust_hint_records,
        "missing_required_files": missing_required,
        "expected_executable_files": list(EXPECTED_EXECUTABLE_FILES),
        "missing_executable_files": missing_executable,
        "forbidden_path_hits": forbidden,
        "recommended_remote_root": "/srv/pm_as_ofi-xuan-research",
        "required_remote_shared_ingress_root": "/srv/pm_as_ofi/shared-ingress-main",
        "requires_explicit_deployment_or_sync_approval": True,
        "orders_sent": False,
        "auth_network_started": False,
        "side_effects": {
            "packaged_files": False,
            "synced_files": False,
            "deployed_code": False,
            "started_observer": False,
            "started_broker": False,
            "connected_to_broker": False,
            "remote_files_written": False,
        },
        "next_gate": "Use this manifest only as a review checklist; deployment/sync needs separate explicit approval.",
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
