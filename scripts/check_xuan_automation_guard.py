#!/usr/bin/env python3
"""Check xuan automation ownership and drift.

This is intentionally read-only. It helps catch accidental sibling loops or
prompt drift in the shared Codex automation directory.
"""

from __future__ import annotations

import ast
import json
import os
import re
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback unavailable.
    tomllib = None  # type: ignore[assignment]


AUTOMATION_ROOT = Path(os.environ.get("CODEX_AUTOMATIONS_DIR", "~/.codex/automations")).expanduser()
OWNER_ID = "xuan-frontier-research-loop"
ALLOWED_ACTIVE_XUAN = {OWNER_ID}
REQUIRED_PROMPT_MARKERS = [
    "LOCAL-ONLY",
    "do not SSH",
    "do not create/update/delete other automations",
    "do not create sibling xuan heartbeats/crons",
    "::archive{reason=\"routine xuan frontier checkpoint\"}",
]


def is_xuanish(item: dict[str, object]) -> bool:
    text = " ".join(
        str(item.get(key, ""))
        for key in ("id", "name", "prompt", "cwds")
    ).lower()
    return any(token in text for token in ("xuan", "frontier", "b27", "rwo", "d+"))


def _minimal_load_automation(path: Path) -> dict[str, object]:
    data: dict[str, object] = {}
    for line in path.read_text().splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            continue
        try:
            parsed = ast.literal_eval(value)
        except Exception:
            parsed = value.strip('"')
        data[key] = parsed
    return data


def load_automation(path: Path) -> dict[str, object]:
    if tomllib is None:
        data = _minimal_load_automation(path)
        data["_path"] = str(path)
        return data
    with path.open("rb") as handle:
        data = tomllib.load(handle)
    data["_path"] = str(path)
    return data


def main() -> int:
    issues: list[str] = []
    automations: list[dict[str, object]] = []
    for path in sorted(AUTOMATION_ROOT.glob("*/automation.toml")):
        try:
            item = load_automation(path)
        except Exception as exc:  # noqa: BLE001 - report all parse issues.
            issues.append(f"cannot parse {path}: {exc}")
            continue
        if is_xuanish(item):
            automations.append(item)

    active = [
        str(item.get("id"))
        for item in automations
        if str(item.get("status", "")).upper() == "ACTIVE"
        and str(item.get("id")) != "auto-reply-pm-as-ofi-issues"
    ]
    unexpected = sorted(set(active) - ALLOWED_ACTIVE_XUAN)
    if unexpected:
        issues.append(f"unexpected active xuan automations: {unexpected}")

    owner = next((item for item in automations if item.get("id") == OWNER_ID), None)
    if owner is None:
        issues.append(f"missing owner automation: {OWNER_ID}")
    else:
        if str(owner.get("status", "")).upper() != "ACTIVE":
            issues.append(f"{OWNER_ID} is not ACTIVE")
        prompt = str(owner.get("prompt", ""))
        for marker in REQUIRED_PROMPT_MARKERS:
            if marker not in prompt:
                issues.append(f"{OWNER_ID} missing prompt marker: {marker}")
        cwd_text = json.dumps(owner.get("cwds", []), ensure_ascii=True)
        if "pm_as_ofi-xuan-frontier" not in cwd_text:
            issues.append(f"{OWNER_ID} cwd does not point at xuan-frontier: {cwd_text}")

    report = {
        "automation_root": str(AUTOMATION_ROOT),
        "active_xuan_ids": active,
        "xuan_automation_ids": [str(item.get("id")) for item in automations],
        "issues": issues,
        "ok": not issues,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not issues else 1


if __name__ == "__main__":
    raise SystemExit(main())
