#!/usr/bin/env python3
"""Read-only shared-ingress broker preflight for xuan_b27_dplus.

This script never starts, stops, repairs, or connects to the broker. It only
checks the existing broker manifest and advertised socket paths.
"""

from __future__ import annotations

import argparse
import json
import stat
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path("/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main")
EXPECTED_PROTOCOL_VERSION = 1
EXPECTED_SCHEMA_VERSION = 1
DEFAULT_MAX_HEARTBEAT_AGE_MS = 10_000


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def now_ms() -> int:
    return int(time.time() * 1000)


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def socket_state(raw_path: str) -> dict:
    path = Path(raw_path)
    out = {
        "path": raw_path,
        "exists": path.exists(),
        "is_socket": False,
        "ok": False,
    }
    if not path.exists():
        out["reason"] = "missing"
        return out
    try:
        mode = path.stat().st_mode
        out["is_socket"] = stat.S_ISSOCK(mode)
        out["ok"] = bool(out["is_socket"])
        if not out["ok"]:
            out["reason"] = "not_socket"
    except OSError as exc:
        out["reason"] = f"stat_failed: {exc}"
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--max-heartbeat-age-ms", type=int, default=DEFAULT_MAX_HEARTBEAT_AGE_MS)
    parser.add_argument(
        "--soft",
        action="store_true",
        help="Return exit 0 even when the broker is unavailable; still records status.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    label = utc_label()
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = args.output_dir or (
        repo_root / "xuan_research_artifacts" / f"xuan_b27_dplus_shared_ingress_preflight_{label}"
    )
    manifest_path = args.root / "broker_manifest.json"
    checks: list[dict] = []
    status = "OK"
    reason = "broker_manifest_and_sockets_ok"
    broker_manifest: dict = {}

    if not manifest_path.exists():
        status = "UNAVAILABLE"
        reason = "broker_manifest_missing"
    else:
        try:
            broker_manifest = json.loads(manifest_path.read_text())
        except Exception as exc:
            status = "UNAVAILABLE"
            reason = f"broker_manifest_parse_failed: {exc}"

    heartbeat_age_ms = None
    if broker_manifest:
        protocol = broker_manifest.get("protocol_version")
        schema = broker_manifest.get("schema_version")
        if protocol != EXPECTED_PROTOCOL_VERSION or schema != EXPECTED_SCHEMA_VERSION:
            status = "UNAVAILABLE"
            reason = f"broker_protocol_mismatch: protocol={protocol} schema={schema}"
        last_heartbeat_ms = broker_manifest.get("last_heartbeat_ms")
        if isinstance(last_heartbeat_ms, int):
            heartbeat_age_ms = max(0, now_ms() - last_heartbeat_ms)
            if heartbeat_age_ms > args.max_heartbeat_age_ms:
                status = "UNAVAILABLE"
                reason = f"broker_heartbeat_stale_ms={heartbeat_age_ms}"
        else:
            status = "UNAVAILABLE"
            reason = "broker_heartbeat_missing"

        for key in ("chainlink_socket", "local_price_socket", "market_socket"):
            raw_path = str(broker_manifest.get(key) or "")
            state = socket_state(raw_path) if raw_path else {
                "path": raw_path,
                "exists": False,
                "is_socket": False,
                "ok": False,
                "reason": "manifest_field_missing",
            }
            state["field"] = key
            checks.append(state)
        missing = [c for c in checks if not c.get("ok")]
        if missing:
            status = "UNAVAILABLE"
            reason = "broker_socket_unavailable"

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_shared_ingress_preflight",
        "status": status,
        "reason": reason,
        "created_utc": label,
        "root": str(args.root),
        "manifest_path": str(manifest_path),
        "expected_protocol_version": EXPECTED_PROTOCOL_VERSION,
        "expected_schema_version": EXPECTED_SCHEMA_VERSION,
        "max_heartbeat_age_ms": args.max_heartbeat_age_ms,
        "heartbeat_age_ms": heartbeat_age_ms,
        "pid": broker_manifest.get("pid"),
        "build_id": broker_manifest.get("build_id"),
        "socket_checks": checks,
        "side_effects": {
            "started_broker": False,
            "stopped_broker": False,
            "connected_to_broker": False,
            "modified_shared_ingress": False,
        },
        "orders_sent": False,
        "auth_network_started": False,
    }
    out_path = out_dir / "manifest.json"
    write_json(out_path, manifest)
    print(out_path)
    if status == "OK" or args.soft:
        return 0
    return 69


if __name__ == "__main__":
    raise SystemExit(main())
