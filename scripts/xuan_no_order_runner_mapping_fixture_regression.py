#!/usr/bin/env python3
"""Run a local no-network fixture regression for the xuan no-order runner mapping.

The fixture is a small JSON contract for replay-validated runtime mapping checks.
It feeds synthetic book/trade/lot steps into the runner and verifies expected
events, source gates, L2 rescue behavior, and normalized lifecycle outputs.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.xuan_dplus_passive_passive_shadow_runner import DPlusRunner, Lot, RunnerConfig


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def runner_config(raw: dict[str, Any]) -> RunnerConfig:
    allowed = {field.name for field in fields(RunnerConfig)}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"unknown RunnerConfig keys: {unknown}")
    return RunnerConfig(**raw)


def read_events(out_dir: Path, slug: str) -> list[dict[str, Any]]:
    path = out_dir / f"{slug}.events.jsonl"
    if not path.exists():
        return []
    events = []
    for line in path.read_text().splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def fields_match(event: dict[str, Any], expected: dict[str, Any]) -> bool:
    return all(event.get(key) == value for key, value in expected.items())


def run_fixture(fixture: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    case_name = fixture.get("case_name") or fixture.get("name")
    slug = fixture.get("slug", "xuan-fixture-market-100")
    out_dir.mkdir(parents=True, exist_ok=True)
    runner = DPlusRunner(
        slug,
        out_dir,
        runner_config(fixture.get("runner_config", {})),
        condition_id=fixture.get("condition_id"),
    )
    for step in fixture.get("steps", []):
        op = step.get("op")
        if op == "book":
            runner.on_book(step["msg"])
        elif op == "trade":
            runner.on_trade(step["msg"])
        elif op == "lot":
            lot = step["lot"]
            side = lot["side"]
            runner.lots[side].append(Lot(**lot))
        elif op == "summary":
            runner.write_summary(final=bool(step.get("final", False)))
        else:
            raise ValueError(f"unsupported fixture op={op!r}")
    events = read_events(out_dir, slug)
    expectations = fixture.get("expectations", {})
    checks: list[dict[str, Any]] = []

    for key, minimum in expectations.get("blocked_min", {}).items():
        actual = runner.blocked.get(key, 0)
        checks.append(
            {
                "name": f"blocked_min:{key}",
                "status": "PASS" if actual >= minimum else "FAIL",
                "actual": actual,
                "expected_min": minimum,
            }
        )

    for spec in expectations.get("event_checks", []):
        kind = spec.get("kind")
        field_equals = spec.get("field_equals", {})
        matching = [
            event
            for event in events
            if (kind is None or event.get("kind") == kind)
            and fields_match(event, field_equals)
        ]
        expected_min = int(spec.get("min_count", 1))
        checks.append(
            {
                "name": spec.get("name") or f"event:{kind}",
                "status": "PASS" if len(matching) >= expected_min else "FAIL",
                "actual_count": len(matching),
                "expected_min": expected_min,
                "field_equals": field_equals,
            }
        )

    if expectations.get("normalized_lifecycle"):
        manifest_path = out_dir / f"{slug}.normalized_lifecycle_manifest.json"
        lifecycle_passed = manifest_path.exists()
        manifest: dict[str, Any] = {}
        missing_files: list[str] = []
        if lifecycle_passed:
            manifest = load_json(manifest_path)
            for rel in manifest.get("files", {}).values():
                if not (out_dir / rel).exists():
                    missing_files.append(rel)
            lifecycle_passed = not missing_files and manifest.get("orders_sent") is False
        checks.append(
            {
                "name": "normalized_lifecycle_files",
                "status": "PASS" if lifecycle_passed else "FAIL",
                "manifest": manifest,
                "missing_files": missing_files,
            }
        )

    status = "PASS" if checks and all(check["status"] == "PASS" for check in checks) else "FAIL"
    return {
        "artifact": "xuan_no_order_runner_mapping_fixture_regression",
        "schema_version": fixture.get("schema_version"),
        "case_name": case_name,
        "status": status,
        "raw_replay_collector_scanned": False,
        "remote_runner_started": False,
        "orders_sent": False,
        "slug": slug,
        "blocked": runner.blocked,
        "event_count": len(events),
        "checks": checks,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    fixture_path = Path(args.fixture)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    fixture = load_json(fixture_path)
    if "cases" in fixture:
        reports = []
        for idx, case in enumerate(fixture["cases"], start=1):
            name = case.get("case_name") or case.get("name") or f"case_{idx:03d}"
            safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)
            reports.append(run_fixture(case, out_dir / safe_name))
        report = {
            "artifact": "xuan_no_order_runner_mapping_fixture_regression_suite",
            "schema_version": fixture.get("schema_version"),
            "status": "PASS" if reports and all(r["status"] == "PASS" for r in reports) else "FAIL",
            "raw_replay_collector_scanned": False,
            "remote_runner_started": False,
            "orders_sent": False,
            "case_count": len(reports),
            "pass_count": sum(1 for r in reports if r["status"] == "PASS"),
            "fail_count": sum(1 for r in reports if r["status"] != "PASS"),
            "reports": reports,
        }
    else:
        report = run_fixture(fixture, out_dir)
    (out_dir / "fixture_regression_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
