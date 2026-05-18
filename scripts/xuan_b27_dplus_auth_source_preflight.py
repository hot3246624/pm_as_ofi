#!/usr/bin/env python3
"""Local auth-source preflight for read-only xuan_b27_dplus User WS.

This checks only whether the process environment or repo `.env` has a complete
source for CLOB/User WS credentials. It never prints secret values and never
starts a network client.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


API_KEYS = (
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
)
PRIVATE_KEY = "POLYMARKET_PRIVATE_KEY"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_dotenv(path: Path, env: dict[str, str]) -> list[str]:
    if not path.exists():
        return []
    loaded: list[str] = []
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        if not key or key in env:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        env[key] = value
        loaded.append(key)
    return loaded


def present(env: dict[str, str], key: str) -> bool:
    return bool((env.get(key) or "").strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dotenv", type=Path)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    dotenv = args.dotenv or repo_root / ".env"
    label = utc_label()
    out_dir = (
        args.output_dir
        or repo_root / "xuan_research_artifacts" / f"xuan_b27_dplus_auth_source_preflight_{label}"
    )

    env = os.environ.copy()
    loaded_keys = load_dotenv(dotenv, env)
    api_presence = {key: present(env, key) for key in API_KEYS}
    api_count = sum(1 for value in api_presence.values() if value)
    private_key_present = present(env, PRIVATE_KEY)

    if api_count == len(API_KEYS):
        status = "OK"
        auth_source = "explicit_api_credentials"
        reason = "complete_POLYMARKET_API_credentials_present"
    elif api_count > 0:
        status = "UNAVAILABLE"
        auth_source = "partial_api_credentials"
        reason = "POLYMARKET_API_credentials_partial"
    elif private_key_present:
        status = "OK"
        auth_source = "derive_from_private_key"
        reason = "POLYMARKET_PRIVATE_KEY_present"
    else:
        status = "UNAVAILABLE"
        auth_source = "missing"
        reason = "missing_complete_POLYMARKET_API_credentials_or_POLYMARKET_PRIVATE_KEY"

    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_auth_source_preflight",
        "status": status,
        "reason": reason,
        "created_utc": label,
        "dotenv_path": str(dotenv),
        "dotenv_exists": dotenv.exists(),
        "dotenv_is_symlink": dotenv.is_symlink(),
        "dotenv_target": str(dotenv.resolve()) if dotenv.exists() else None,
        "loaded_relevant_keys": sorted(k for k in loaded_keys if k in set(API_KEYS + (PRIVATE_KEY,))),
        "auth_source": auth_source,
        "api_credentials_present": api_presence,
        "private_key_present": private_key_present,
        "secrets_redacted": True,
        "orders_sent": False,
        "auth_network_started": False,
        "side_effects": {
            "started_observer": False,
            "started_broker": False,
            "connected_to_broker": False,
            "derived_credentials": False,
            "printed_secret_values": False,
        },
    }
    out_path = out_dir / "manifest.json"
    write_json(out_path, manifest)
    print(out_path)
    return 0 if status == "OK" else 67


if __name__ == "__main__":
    raise SystemExit(main())
