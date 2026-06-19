#!/usr/bin/env python3
"""Local pre-authorization gate for residual-guard no-order runtime profile."""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        val = float(value)
        return val if math.isfinite(val) else default
    except (TypeError, ValueError):
        return default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def score(args: argparse.Namespace) -> dict[str, Any]:
    diagnostic = read_json(Path(args.residual_diagnostic).expanduser().resolve())
    profile = read_json(Path(args.profile_scorecard).expanduser().resolve())
    remote_manifest = read_json(Path(args.remote_manifest).expanduser().resolve())
    manifest_verifier = read_json(Path(args.manifest_verifier).expanduser().resolve())
    hard_blockers: list[str] = []
    warnings: list[str] = []

    profile_body = profile.get("profile", {})
    if diagnostic.get("status") != "UNKNOWN_RESIDUAL_REGIME_ECONOMIC_ADMISSION_RESCUE_MISMATCH":
        hard_blockers.append("residual_diagnostic_status_unexpected")
    if profile.get("status") != "RESIDUAL_GUARD_PROFILE_READY_REMOTE_AUTH_REQUIRED":
        hard_blockers.append("profile_not_residual_guard_ready")
    if profile.get("safety", {}).get("remote_runner_allowed") is not False:
        hard_blockers.append("profile_remote_runner_allowed")
    if remote_manifest.get("status") != "THIRD_WINDOW_REMOTE_STAGING_MANIFEST_READY_LOCAL_ONLY":
        hard_blockers.append("remote_manifest_not_ready")
    if remote_manifest.get("decision", {}).get("remote_runner_allowed") is not False:
        hard_blockers.append("remote_manifest_remote_runner_allowed")
    if manifest_verifier.get("status") != "KEEP_THIRD_WINDOW_MANIFEST_VERIFIER_PASS_LOCAL_ONLY":
        hard_blockers.append("manifest_verifier_not_pass")
    if manifest_verifier.get("hard_blockers"):
        hard_blockers.append("manifest_verifier_has_hard_blockers")

    expected_profile = {
        "profile_family": "residual_guard",
        "duration_s": args.expected_duration_s,
        "round_offsets": "0,1,2,3,4,5,6,7,8,9,10,11,12",
        "soft_cap": 0.98,
        "debt_floor": 0.95,
        "debt_budget": 1.0,
        "target_rescue_net_cap": 0.95,
        "strict_rescue_salvage_net_cap": args.expected_salvage_net_cap,
        "strict_rescue_l1_age_max_ms": 50,
        "activation_mode": "opp_seen",
        "activation_window_s": 15.0,
        "late_repair_after_s": 90.0,
        "imbalance_qty_cap": 1.25,
        "allow_concurrent_shared_ingress_readers": True,
    }
    for key, expected in expected_profile.items():
        if profile_body.get(key) != expected:
            hard_blockers.append(f"profile_{key}_unexpected")

    metrics = diagnostic.get("metrics", {})
    if as_float(metrics.get("residual_qty_share")) <= as_float(profile.get("minimum_window_acceptance", {}).get("max_residual_qty_share")):
        warnings.append("prior_residual_qty_share_not_above_new_gate")
    if as_float(metrics.get("residual_cost_share")) <= as_float(profile.get("minimum_window_acceptance", {}).get("max_residual_cost_share")):
        warnings.append("prior_residual_cost_share_not_above_new_gate")

    status = "KEEP_RESIDUAL_GUARD_PRE_AUTHORIZATION_GATE_PASS_LOCAL_ONLY" if not hard_blockers else "BLOCKED_RESIDUAL_GUARD_PRE_AUTHORIZATION_GATE"
    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "warnings": sorted(set(warnings)),
        "inputs": {
            "residual_diagnostic": str(Path(args.residual_diagnostic).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
            "remote_manifest": str(Path(args.remote_manifest).expanduser().resolve()),
            "manifest_verifier": str(Path(args.manifest_verifier).expanduser().resolve()),
        },
        "target": {
            "instance_prefix": remote_manifest.get("instance", {}).get("instance_id_template"),
            "minimum_window_acceptance": profile.get("minimum_window_acceptance", {}),
            "profile": profile_body,
        },
        "decision": {
            "pre_authorization_local_gate_pass": not hard_blockers,
            "remote_runner_allowed": False,
            "deployable": False,
            "requires_explicit_user_authorization": True,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--residual-diagnostic", required=True)
    parser.add_argument("--profile-scorecard", required=True)
    parser.add_argument("--remote-manifest", required=True)
    parser.add_argument("--manifest-verifier", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--expected-duration-s", type=int, default=1800)
    parser.add_argument("--expected-salvage-net-cap", type=float, default=0.98)
    args = parser.parse_args()

    started = time.time()
    result = score(args)
    scorecard = {
        "artifact": "xuan_no_order_residual_guard_pre_authorization_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_residual_guard_pre_authorization_gate.py",
        **result,
    }
    path = Path(args.scorecard_json).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))
    if result["hard_blockers"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
