#!/usr/bin/env python3
"""Check xuan-frontier automation ownership and drift.

This is intentionally read-only. It helps catch accidental sibling loops or
prompt drift in the shared Codex automation directory without policing other
worktree owners.
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
LOCAL_ARCHIVE_ID = "xuan-frontier-research-loop"
REMOTE_VERIFIER_ID = "xuan-frontier-remote-verifier-loop"
FRONTIER_PREFIX = "xuan-frontier-"
ALLOWED_ACTIVE_FRONTIER = {REMOTE_VERIFIER_ID}
REQUIRED_LOCAL_PROMPT_MARKERS = [
    "local-only",
    "intentionally paused",
    "do not ssh",
    "create/update/delete other automations",
    "create sibling xuan heartbeats/crons",
    "xuan frontier owns xuan-frontier-* only",
    "strict auto-archive rule",
    "::archive{reason=\"routine xuan frontier checkpoint\"}",
]
REQUIRED_REMOTE_PROMPT_MARKERS = [
    "remote-verifier",
    "ssh preflight",
    "-i ~/.ssh/polymarket-Ireland.pem",
    "IdentitiesOnly=yes",
    "/tmp/xuan_duckdb_venv/bin/python",
    "event_store.duckdb",
    "outputs.row_count",
    "do not rsync",
    "do not scp",
    "manifest",
    "keep/discard/unknown",
    "strict auto-archive rule",
    "::archive{reason=\"routine xuan frontier remote verifier checkpoint\"}",
    "do not modify collector/raw/replay",
]


def is_frontier_owned(item: dict[str, object]) -> bool:
    automation_id = str(item.get("id", ""))
    return automation_id in ALLOWED_ACTIVE_FRONTIER or automation_id.startswith(FRONTIER_PREFIX)


def is_related_non_frontier(item: dict[str, object]) -> bool:
    automation_id = str(item.get("id", ""))
    if is_frontier_owned(item):
        return False
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
    frontier_automations: list[dict[str, object]] = []
    related_non_frontier: list[dict[str, object]] = []
    for path in sorted(AUTOMATION_ROOT.glob("*/automation.toml")):
        try:
            item = load_automation(path)
        except Exception as exc:  # noqa: BLE001 - report all parse issues.
            issues.append(f"cannot parse {path}: {exc}")
            continue
        if is_frontier_owned(item):
            frontier_automations.append(item)
        elif is_related_non_frontier(item):
            related_non_frontier.append(item)

    active = [
        str(item.get("id"))
        for item in frontier_automations
        if str(item.get("status", "")).upper() == "ACTIVE"
        and str(item.get("id")) != "auto-reply-pm-as-ofi-issues"
    ]
    unexpected = sorted(set(active) - ALLOWED_ACTIVE_FRONTIER)
    if unexpected:
        issues.append(f"unexpected active xuan-frontier automations: {unexpected}")

    local_owner = next((item for item in frontier_automations if item.get("id") == LOCAL_ARCHIVE_ID), None)
    if local_owner is None:
        issues.append(f"missing owner automation: {LOCAL_ARCHIVE_ID}")
    else:
        if str(local_owner.get("status", "")).upper() != "PAUSED":
            issues.append(f"{LOCAL_ARCHIVE_ID} should be PAUSED until cron archive is reliable")
        prompt = str(local_owner.get("prompt", ""))
        prompt_lower = prompt.lower()
        for marker in REQUIRED_LOCAL_PROMPT_MARKERS:
            if marker.lower() not in prompt_lower:
                issues.append(f"{LOCAL_ARCHIVE_ID} missing prompt marker: {marker}")
        cwd_text = json.dumps(local_owner.get("cwds", []), ensure_ascii=True)
        if "pm_as_ofi-xuan-frontier" not in cwd_text:
            issues.append(f"{LOCAL_ARCHIVE_ID} cwd does not point at xuan-frontier: {cwd_text}")

    remote_owner = next((item for item in frontier_automations if item.get("id") == REMOTE_VERIFIER_ID), None)
    if remote_owner is None:
        issues.append(f"missing owner automation: {REMOTE_VERIFIER_ID}")
    else:
        if str(remote_owner.get("status", "")).upper() != "ACTIVE":
            issues.append(f"{REMOTE_VERIFIER_ID} is not ACTIVE")
        prompt = str(remote_owner.get("prompt", ""))
        prompt_lower = prompt.lower()
        for marker in REQUIRED_REMOTE_PROMPT_MARKERS:
            if marker.lower() not in prompt_lower:
                issues.append(f"{REMOTE_VERIFIER_ID} missing prompt marker: {marker}")
        cwd_text = json.dumps(remote_owner.get("cwds", []), ensure_ascii=True)
        if "pm_as_ofi-xuan-frontier" not in cwd_text:
            issues.append(f"{REMOTE_VERIFIER_ID} cwd does not point at xuan-frontier: {cwd_text}")

    report = {
        "automation_root": str(AUTOMATION_ROOT),
        "active_xuan_frontier_ids": active,
        "related_non_frontier_ids": [
            str(item.get("id")) for item in related_non_frontier
        ],
        "xuan_frontier_automation_ids": [
            str(item.get("id")) for item in frontier_automations
        ],
        "issues": issues,
        "ok": not issues,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not issues else 1


if __name__ == "__main__":
    raise SystemExit(main())
