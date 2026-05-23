#!/usr/bin/env python3
"""Build a local residual-guard runtime profile for xuan-frontier.

The profile is a local planning artifact only. It records the next bounded
no-order dry-run candidate after residual diagnostics showed the prior soft
closeability profile can leave too much unmatched inventory.
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


def render_markdown(card: dict[str, Any]) -> str:
    lines = [
        "# Residual Guard Runtime Profile",
        "",
        "This is a local profile only. It does not authorize or start remote execution.",
        "",
        "## Status",
        f"- status: `{card['status']}`",
        f"- remote_runner_allowed: `{card['safety']['remote_runner_allowed']}`",
        f"- deployable: `{card['safety']['deployable']}`",
        "",
        "## Profile",
    ]
    for key, value in card["profile"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Acceptance Floor",
        ]
    )
    for key, value in card["minimum_window_acceptance"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            card["interpretation"],
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    diagnostic = read_json(Path(args.residual_diagnostic).expanduser().resolve())
    runtime = read_json(Path(args.runtime_summary).expanduser().resolve())
    metrics = diagnostic.get("metrics", {})
    profile = {
        "profile_family": "residual_guard",
        "duration_s": args.duration_s,
        "round_offsets": args.round_offsets,
        "edge": 0.040,
        "queue_share": 0.50,
        "target_qty": 5.0,
        "fill_haircut": 0.25,
        "max_seed_qty": 60.0,
        "max_open_cost": 80.0,
        "seed_l1_cap": 1.02,
        "seed_px_lo": 0.05,
        "seed_px_hi": 0.90,
        "seed_offset_max_s": 120,
        "order_ttl_s": 120,
        "imbalance_qty_cap": args.imbalance_qty_cap,
        "imbalance_cost_cap": 1000000000.0,
        "activation_mode": args.activation_mode,
        "activation_window_s": args.activation_window_s,
        "late_repair_after_s": args.late_repair_after_s,
        "surplus_budget_mode": "block",
        "surplus_budget_bootstrap": 1.0,
        "surplus_budget_mult": 0.5,
        "surplus_budget_max_abs_unpaired_cost": 2.0,
        "taker_fee_rate": 0.07,
        "soft_cap": args.soft_cap,
        "debt_floor": args.debt_floor,
        "debt_budget": args.debt_budget,
        "risk_seed_pending_opp_credit": args.risk_seed_pending_opp_credit,
        "target_rescue_net_cap": args.target_rescue_net_cap,
        "strict_rescue_salvage_net_cap": args.salvage_net_cap,
        "strict_rescue_skip_low_cost_lots": args.strict_rescue_skip_low_cost_lots,
        "strict_rescue_l1_age_max_ms": 50,
        "strict_rescue_close_size_haircut": 1.0,
        "source_quality_require_trade_source": True,
        "source_quality_require_l1_source": True,
        "source_quality_require_l2_source": True,
        "strict_rescue_require_book_source": True,
        "strict_rescue_require_l2_source": True,
        "write_normalized_lifecycle": True,
        "write_rescue_block_diagnostics": True,
        "allow_concurrent_shared_ingress_readers": True,
    }
    acceptance = {
        "accepted_actions": args.min_accepted_actions,
        "queue_supported_fills": args.min_fills,
        "strict_rescue_closes": args.min_strict_rescue_closes,
        "pair_pnl": 0.0,
        "max_residual_qty_share": args.max_residual_qty_share,
        "max_residual_cost_share": args.max_residual_cost_share,
        "max_rescue_net_pair_cost": args.salvage_net_cap,
        "target_rescue_net_pair_cost": args.target_rescue_net_cap,
        "max_accepted_l1_age_ms": 1000,
        "max_rescue_l1_age_ms": 50,
        "strict_rescue_source_blocks": 0,
        "orders_sent": False,
        "stderr_bytes": 0,
    }
    return {
        "schema_version": "xuan_frontier_scorecard_v1",
        "status": "RESIDUAL_GUARD_PROFILE_READY_REMOTE_AUTH_REQUIRED",
        "scope": "xuan-frontier local residual-guard profile planning only",
        "candidate": "risk-refined source-quality + market-budget + strict rescue + residual guard",
        "basis": {
            "residual_diagnostic": str(Path(args.residual_diagnostic).expanduser().resolve()),
            "runtime_summary": str(Path(args.runtime_summary).expanduser().resolve()),
            "runtime_status": runtime.get("status"),
            "prior_profile_problem": "soft_closeability_same_profile_window_failed_residual_gates",
            "prior_metrics": {
                "accepted_actions": metrics.get("accepted_actions"),
                "fills": metrics.get("fills"),
                "strict_rescue_closes": metrics.get("strict_rescue_closes"),
                "pair_pnl": metrics.get("pair_pnl"),
                "residual_qty_share": metrics.get("residual_qty_share"),
                "residual_cost_share": metrics.get("residual_cost_share"),
            },
        },
        "profile": profile,
        "minimum_window_acceptance": acceptance,
        "safety": {
            "remote_runner_allowed": False,
            "remote_runner_started": False,
            "orders_sent": False,
            "deployable": False,
            "live_trading_allowed": False,
            "shared_service_mutation": False,
            "remote_repo_mutation": False,
            "deploy_or_restart": False,
            "collector_rebuild": False,
            "cron_loop": False,
        },
        "interpretation": (
            "This changes the controller profile, so prior same-profile repeat-window evidence should not be "
            "counted as a completed residual-guard repeat. The next remote sample, if explicitly authorized, "
            "should test whether opp-seen activation, late repair, and a tighter imbalance cap preserve scale "
            "while keeping residual shares under the per-window gates. The 0.95 rescue net cap is a quality "
            "target, not the hard floor; this probe allows a bounded higher rescue cap while requiring "
            "positive pair PnL and controlled residual."
        ),
        "decision": {
            "remote_runner_allowed": False,
            "deployable": False,
            "requires_explicit_user_authorization": True,
            "shadow_review_ready": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--residual-diagnostic", required=True)
    parser.add_argument("--runtime-summary", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--duration-s", type=int, default=1800)
    parser.add_argument("--round-offsets", default="0,1,2,3,4,5,6,7,8,9,10,11,12")
    parser.add_argument("--activation-mode", choices=["none", "opp_seen"], default="opp_seen")
    parser.add_argument("--activation-window-s", type=float, default=15.0)
    parser.add_argument("--late-repair-after-s", type=float, default=90.0)
    parser.add_argument("--imbalance-qty-cap", type=float, default=1.25)
    parser.add_argument("--soft-cap", type=float, default=0.98)
    parser.add_argument("--debt-floor", type=float, default=0.95)
    parser.add_argument("--debt-budget", type=float, default=1.0)
    parser.add_argument("--risk-seed-pending-opp-credit", type=float, default=1.0)
    parser.add_argument("--target-rescue-net-cap", type=float, default=0.95)
    parser.add_argument("--salvage-net-cap", type=float, default=0.98)
    parser.add_argument("--strict-rescue-skip-low-cost-lots", action="store_true")
    parser.add_argument("--min-accepted-actions", type=int, default=25)
    parser.add_argument("--min-fills", type=int, default=18)
    parser.add_argument("--min-strict-rescue-closes", type=int, default=3)
    parser.add_argument("--max-residual-qty-share", type=float, default=0.35)
    parser.add_argument("--max-residual-cost-share", type=float, default=0.30)
    args = parser.parse_args()
    if not (0.0 <= args.risk_seed_pending_opp_credit <= 1.0):
        raise SystemExit("--risk-seed-pending-opp-credit must be in [0, 1]")

    started = time.time()
    card = build(args)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / "RESIDUAL_GUARD_RUNTIME_PROFILE.md"
    card["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    card["runtime_s"] = round(time.time() - started, 3)
    card["script"] = "scripts/xuan_no_order_residual_guard_profile_builder.py"
    card["profile_doc"] = str(md_path)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(rounded(card), indent=2, sort_keys=True) + "\n")
    md_path.write_text(render_markdown(rounded(card)) + "\n")
    print(json.dumps(rounded(card), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
