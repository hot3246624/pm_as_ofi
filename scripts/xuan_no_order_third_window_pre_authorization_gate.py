#!/usr/bin/env python3
"""Local pre-authorization gate for the xuan-frontier third no-order window.

This is a review gate, not an authorization. It confirms that the current local
artifacts are internally consistent and that the only remaining shadow-review
blocker is the planned repeat-window strict-rescue close gap.
"""

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
    manifest = read_json(Path(args.manifest_verifier).expanduser().resolve())
    packet = read_json(Path(args.shadow_packet).expanduser().resolve())
    gap = read_json(Path(args.gap_plan).expanduser().resolve())
    profile = read_json(Path(args.profile_scorecard).expanduser().resolve())
    remote_manifest = read_json(Path(args.remote_manifest).expanduser().resolve())

    hard_blockers: list[str] = []
    warnings: list[str] = []
    manifest_decision = manifest.get("decision", {})
    packet_summary = packet.get("summary", {})
    packet_decision = packet.get("decision", {})
    remaining_gaps = gap.get("remaining_gaps", {})
    next_floor = gap.get("next_window_floor", {})
    profile_body = profile.get("profile", {})
    safety = profile.get("safety", {})

    if manifest.get("status") != "KEEP_THIRD_WINDOW_MANIFEST_VERIFIER_PASS_LOCAL_ONLY":
        hard_blockers.append("manifest_verifier_not_pass")
    if manifest.get("hard_blockers"):
        hard_blockers.append("manifest_verifier_has_hard_blockers")
    if manifest_decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("manifest_verifier_remote_allowed")
    if remote_manifest.get("status") != "THIRD_WINDOW_REMOTE_STAGING_MANIFEST_READY_LOCAL_ONLY":
        hard_blockers.append("remote_staging_manifest_not_ready")
    if remote_manifest.get("decision", {}).get("remote_runner_allowed") is not False:
        hard_blockers.append("remote_staging_manifest_remote_allowed")
    if profile.get("status") != "THIRD_WINDOW_PROFILE_READY_REMOTE_AUTH_REQUIRED":
        hard_blockers.append("third_window_profile_not_ready")
    if safety.get("remote_runner_allowed") is not False:
        hard_blockers.append("profile_remote_runner_allowed")
    if packet_decision.get("remote_runner_allowed") is not False:
        hard_blockers.append("packet_remote_runner_allowed")
    if packet_decision.get("shadow_review_ready") is True:
        warnings.append("packet_already_shadow_review_ready")
    if packet_summary.get("hard_blockers") != ["total_strict_rescue_closes_below_min"]:
        hard_blockers.append("packet_has_unexpected_hard_blockers")
    if remaining_gaps.get("total_strict_rescue_closes") != 5:
        hard_blockers.append("unexpected_rescue_close_gap")
    for key in ("total_accepted_actions", "total_fills"):
        if remaining_gaps.get(key) != 0:
            hard_blockers.append(f"unexpected_{key}_gap")
    if as_float(remaining_gaps.get("total_pair_pnl")) != 0.0:
        hard_blockers.append("unexpected_pair_pnl_gap")
    if next_floor.get("min_strict_rescue_closes_to_clear_aggregate") != 5:
        hard_blockers.append("next_window_floor_rescue_gap_not_5")
    expected_profile = {
        "duration_s": 1800,
        "round_offsets": "0,1,2,3,4,5,6,7,8,9,10,11,12",
        "soft_cap": 0.98,
        "debt_floor": 0.95,
        "debt_budget": 1.0,
        "strict_rescue_salvage_net_cap": 0.95,
        "strict_rescue_l1_age_max_ms": 50,
    }
    for key, expected in expected_profile.items():
        if profile_body.get(key) != expected:
            hard_blockers.append(f"profile_{key}_unexpected")

    status = (
        "KEEP_THIRD_WINDOW_PRE_AUTHORIZATION_GATE_PASS_LOCAL_ONLY"
        if not hard_blockers
        else "BLOCKED_THIRD_WINDOW_PRE_AUTHORIZATION_GATE"
    )
    return {
        "status": status,
        "hard_blockers": sorted(set(hard_blockers)),
        "warnings": sorted(set(warnings)),
        "inputs": {
            "manifest_verifier": str(Path(args.manifest_verifier).expanduser().resolve()),
            "shadow_packet": str(Path(args.shadow_packet).expanduser().resolve()),
            "gap_plan": str(Path(args.gap_plan).expanduser().resolve()),
            "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
            "remote_manifest": str(Path(args.remote_manifest).expanduser().resolve()),
        },
        "target": {
            "additional_clean_strict_rescue_closes_needed": remaining_gaps.get("total_strict_rescue_closes"),
            "min_accepted_actions": next_floor.get("min_accepted_actions"),
            "min_fills": next_floor.get("min_fills"),
            "min_pair_pnl": next_floor.get("min_pair_pnl"),
            "max_residual_qty_share": next_floor.get("max_residual_qty_share"),
            "max_residual_cost_share": next_floor.get("max_residual_cost_share"),
            "max_rescue_net_pair_cost": next_floor.get("max_rescue_net_pair_cost"),
            "max_rescue_l1_age_ms": next_floor.get("max_rescue_l1_age_ms"),
        },
        "profile": profile_body,
        "decision": {
            "pre_authorization_local_gate_pass": not hard_blockers,
            "remote_runner_allowed": False,
            "deployable": False,
            "requires_explicit_user_authorization": True,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-verifier", default=".tmp_xuan/scorecards/no_order_third_window_manifest_verifier_20260522T2119Z.json")
    parser.add_argument("--shadow-packet", default=".tmp_xuan/scorecards/no_order_shadow_review_packet_20260522T1949Z.json")
    parser.add_argument("--gap-plan", default=".tmp_xuan/scorecards/no_order_soft_closeability_repeat_window_gap_plan_20260522T1919Z.json")
    parser.add_argument("--profile-scorecard", default=".tmp_xuan/scorecards/no_order_soft_closeability_third_window_profile_20260522T1849Z.json")
    parser.add_argument("--remote-manifest", default=".tmp_xuan/scorecards/no_order_third_window_remote_manifest_20260522T2049Z.json")
    parser.add_argument("--scorecard-json", required=True)
    args = parser.parse_args()

    started = time.time()
    result = score(args)
    scorecard = {
        "artifact": "xuan_no_order_third_window_pre_authorization_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_third_window_pre_authorization_gate.py",
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
