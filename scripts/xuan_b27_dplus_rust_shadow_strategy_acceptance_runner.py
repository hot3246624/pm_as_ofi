#!/usr/bin/env python3
"""Run the local D+ no-order shadow acceptance chain.

The runner consumes an already-local no-order shadow/dry-run run artifact and
executes the existing local gates in-process:

run artifact -> realized labels -> outcome bridge -> performance evidence ->
Rust shadow strategy acceptance.

It does not start observers, connect to network, read raw/replay stores, or
touch order/cancel/redeem paths. Fixture/smoke inputs can prove the chain, but
they cannot publish a real ``PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE`` artifact.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS_FIXTURE_STATUS = "PASS_RUNNER_FIXTURE_CHAIN"
PASS_PUBLISHED_STATUS = "PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE_PUBLISHED"

RC_BY_STATUS = {
    PASS_FIXTURE_STATUS: 0,
    PASS_PUBLISHED_STATUS: 0,
    "FAIL_UNSAFE_INPUT_PATH": 2,
    "FAIL_RUN_MANIFEST_NOT_SAFE": 3,
    "FAIL_REFUSED_FIXTURE_PUBLISH": 4,
    "FAIL_REALIZED_LABELS_GATE": 5,
    "FAIL_OUTCOME_BRIDGE_GATE": 6,
    "FAIL_PERFORMANCE_EVIDENCE_GATE": 7,
    "FAIL_STRATEGY_ACCEPTANCE_GATE": 8,
}

ACCEPTED_RUN_STATUSES = {
    "PASS_NO_ORDER_SHADOW_DRY_RUN",
    "PASS_RUST_NO_ORDER_SHADOW_DRY_RUN",
}

ACCEPTED_RUN_SCOPES = {
    "local_no_order_shadow_or_dry_run",
    "rust_no_order_shadow_or_dry_run",
}

FORBIDDEN_FIELDS = (
    "orders_sent",
    "cancels_sent",
    "redeems_sent",
    "auth_network_started",
    "started_canary",
    "started_observer",
    "started_user_ws",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def read_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        value = json.loads(path.read_text())
    except Exception as exc:
        return {"_read_error": str(exc)}
    return value if isinstance(value, dict) else {"_read_error": "json_not_object"}


def safe_local_artifact_path(root: Path, path: Path) -> bool:
    resolved = path.resolve()
    artifacts = (root / "xuan_research_artifacts").resolve()
    if artifacts not in resolved.parents and resolved != artifacts:
        return False
    parts = {part.lower() for part in resolved.parts}
    forbidden = {"raw", "replay", "replay_published"}
    return not (parts & forbidden) and "/mnt/poly-replay" not in str(resolved)


def path_is_fixture_or_smoke(path: Path) -> bool:
    text_parts = [part.lower() for part in path.resolve().parts]
    return any("fixture" in part or "_smoke_" in part or part == "fixtures" for part in text_parts)


def false_side_effects(data: dict[str, Any]) -> bool:
    side_effects = data.get("side_effects") or {}
    return all(data.get(field, False) is False for field in FORBIDDEN_FIELDS) and all(
        side_effects.get(field, False) is False for field in FORBIDDEN_FIELDS
    )


def run_manifest_safe(manifest: dict[str, Any]) -> bool:
    return (
        manifest.get("artifact")
        in {
            "xuan_b27_dplus_no_order_shadow_run_artifact",
            "xuan_b27_dplus_rust_no_order_shadow_run",
        }
        and manifest.get("status") in ACCEPTED_RUN_STATUSES
        and manifest.get("strategy") == "xuan_b27_dplus"
        and manifest.get("scope") in ACCEPTED_RUN_SCOPES
        and manifest.get("no_order") is True
        and manifest.get("orders_allowed") is False
        and manifest.get("cancels_allowed") is False
        and manifest.get("redeems_allowed") is False
        and isinstance(manifest.get("observer_summary"), str)
        and isinstance(manifest.get("edge_samples"), str)
        and false_side_effects(manifest)
    )


def load_script(root: Path, rel: str) -> Any:
    path = root / rel
    name = f"_xuan_runner_{path.stem}_{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {rel}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_script(root: Path, rel: str, argv: list[str]) -> int:
    module = load_script(root, rel)
    old_argv = sys.argv[:]
    sys.argv = [rel] + argv
    try:
        return int(module.main())
    finally:
        sys.argv = old_argv


def resolve_manifest_path(root: Path, run_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    root_relative = root / path
    if root_relative.exists():
        return root_relative
    return run_dir / path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    parser.add_argument("--allow-fixture", action="store_true")
    parser.add_argument("--publish-real-acceptance", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_rust_shadow_strategy_acceptance_runner_{label}"
    labels_dir = out_dir / "realized_labels"
    bridge_dir = out_dir / "outcome_bridge"
    performance_dir = out_dir / "performance_evidence"
    acceptance_dir = (
        root / "xuan_research_artifacts" / f"xuan_b27_dplus_rust_shadow_strategy_acceptance_{label}"
        if args.publish_real_acceptance
        else out_dir / "strategy_acceptance"
    )

    path_safe = safe_local_artifact_path(root, args.run_dir) and safe_local_artifact_path(root, out_dir)
    manifest_path = args.run_dir / "manifest.json"
    run_manifest = read_json(manifest_path) if path_safe else {}
    manifest_safe = path_safe and run_manifest_safe(run_manifest)
    fixture_or_smoke = path_is_fixture_or_smoke(args.run_dir)

    failures: list[str] = []
    if not path_safe:
        failures.append("unsafe_input_path")
        status = "FAIL_UNSAFE_INPUT_PATH"
    elif not manifest_safe:
        failures.append("run_manifest_not_safe")
        status = "FAIL_RUN_MANIFEST_NOT_SAFE"
    elif args.publish_real_acceptance and (fixture_or_smoke or args.allow_fixture):
        failures.append("refused_fixture_publish")
        status = "FAIL_REFUSED_FIXTURE_PUBLISH"
    else:
        status = "RUNNING"

    label_rc = bridge_rc = performance_rc = acceptance_rc = None
    if status == "RUNNING":
        observer_summary = resolve_manifest_path(root, args.run_dir, str(run_manifest["observer_summary"]))
        edge_samples = resolve_manifest_path(root, args.run_dir, str(run_manifest["edge_samples"]))
        if not (
            safe_local_artifact_path(root, observer_summary)
            and safe_local_artifact_path(root, edge_samples)
        ):
            failures.append("unsafe_chained_input_path")
            status = "FAIL_UNSAFE_INPUT_PATH"

    if status == "RUNNING":
        label_rc = run_script(
            root,
            "scripts/xuan_b27_dplus_realized_outcome_labels.py",
            [
                "--run-dir",
                str(args.run_dir),
                "--output-dir",
                str(labels_dir),
                "--market-prefix",
                args.market_prefix,
            ],
        )
        if label_rc != 0:
            failures.append("realized_labels_gate_failed")
            status = "FAIL_REALIZED_LABELS_GATE"

    if status == "RUNNING":
        bridge_rc = run_script(
            root,
            "scripts/xuan_b27_dplus_outcome_label_bridge.py",
            [
                "--edge-samples",
                str(edge_samples),
                "--outcome-labels",
                str(labels_dir / "outcome_labels.jsonl"),
                "--output-dir",
                str(bridge_dir),
                "--market-prefix",
                args.market_prefix,
            ],
        )
        if bridge_rc != 0:
            failures.append("outcome_bridge_gate_failed")
            status = "FAIL_OUTCOME_BRIDGE_GATE"

    if status == "RUNNING":
        performance_rc = run_script(
            root,
            "scripts/xuan_b27_dplus_shadow_performance_evidence.py",
            [
                "--observer-summary",
                str(observer_summary),
                "--edge-samples",
                str(bridge_dir / "realized_edge_samples.jsonl"),
                "--output-dir",
                str(performance_dir),
                "--market-prefix",
                args.market_prefix,
            ],
        )
        if performance_rc != 0:
            failures.append("performance_evidence_gate_failed")
            status = "FAIL_PERFORMANCE_EVIDENCE_GATE"

    if status == "RUNNING":
        acceptance_rc = run_script(
            root,
            "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py",
            [
                "--observer-summary",
                str(observer_summary),
                "--performance-evidence",
                str(performance_dir / "manifest.json"),
                "--output-dir",
                str(acceptance_dir),
                "--market-prefix",
                args.market_prefix,
            ],
        )
        if acceptance_rc != 0:
            failures.append("strategy_acceptance_gate_failed")
            status = "FAIL_STRATEGY_ACCEPTANCE_GATE"
        elif args.publish_real_acceptance:
            status = PASS_PUBLISHED_STATUS
        else:
            status = PASS_FIXTURE_STATUS

    acceptance_manifest = read_json(acceptance_dir / "manifest.json")
    manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_rust_shadow_strategy_acceptance_runner",
        "status": status,
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "scope": "local_no_network_rust_shadow_strategy_acceptance_runner",
        "failures": failures,
        "run_dir": str(args.run_dir),
        "run_manifest": str(manifest_path),
        "run_manifest_status": run_manifest.get("status"),
        "run_manifest_safe": manifest_safe,
        "input_path_safe": path_safe,
        "fixture_or_smoke_input": fixture_or_smoke,
        "allow_fixture": args.allow_fixture,
        "publish_real_acceptance_requested": args.publish_real_acceptance,
        "realized_labels_manifest": str(labels_dir / "manifest.json"),
        "outcome_bridge_manifest": str(bridge_dir / "manifest.json"),
        "performance_evidence_manifest": str(performance_dir / "manifest.json"),
        "strategy_acceptance_manifest": str(acceptance_dir / "manifest.json"),
        "strategy_acceptance_status": acceptance_manifest.get("status"),
        "strategy_acceptance_passed": acceptance_manifest.get("strategy_acceptance_passed"),
        "return_codes": {
            "realized_labels": label_rc,
            "outcome_bridge": bridge_rc,
            "performance_evidence": performance_rc,
            "strategy_acceptance": acceptance_rc,
        },
        "real_acceptance_artifact_published": status == PASS_PUBLISHED_STATUS,
        "acceptance_artifact_published": status == PASS_PUBLISHED_STATUS,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "network_started": False,
            "ssh_started": False,
            "started_observer": False,
            "started_user_ws": False,
            "started_canary": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "broker_modified": False,
        },
        "next_gate": (
            "runner published real Rust shadow strategy acceptance"
            if status == PASS_PUBLISHED_STATUS
            else "provide non-fixture no-order shadow/dry-run run artifact and publish real acceptance"
            if status == PASS_FIXTURE_STATUS
            else "fix local no-order shadow/dry-run acceptance inputs"
        ),
    }
    write_json(out_dir / "manifest.json", manifest)
    print(out_dir / "manifest.json")
    return RC_BY_STATUS.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
