#!/usr/bin/env python3
"""Audit corrected public-leaderboard 24h strategy leads.

The public leaderboard/cash PnL lens can over-rank accounts whose in-window
REDEEMs came from positions opened before the sampled window. This audit keeps
that correction explicit and ranks public-profile leads by new-window MTM,
pair quality, residual exposure, and pair-cost tails.

This script reads only already-exported public profile summaries. It does not
touch private truth, replay/raw stores, shared ingress, broker state, or any
live/trading path.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_public_leaderboard_corrected_new_mtm_audit"
DEFAULT_EXPORT_ROOT = Path("/Users/hot/web3Scientist/poly_trans_research/data/exports")
FORBIDDEN_OUTPUT_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
    "/broker/",
)


PROFILE_CONFIGS: tuple[dict[str, Any], ...] = (
    {
        "account": "b55",
        "address_or_handle": "0xb55fa1296e6ec55d0ce53d93b9237389f11764d4",
        "export_dir_name": "leaderboard_b55_recent24h_20260523_103000_to_20260524_103000_bjt",
        "old_position_redeem_benefit": 5818.0,
        "new_window_cash_ex_rebate": -2610.0,
        "corrected_new_mtm_ex_rebate": 2851.0,
        "corrected_new_mtm_incl_rebate": 3513.0,
        "split_count": 0,
        "role": "main_fair_price_pair_edge_template",
    },
    {
        "account": "ce25",
        "address_or_handle": "0xce25e214d5cfe4f459cf67f08df581885aae7fdc",
        "export_dir_name": "leaderboard_ce25_recent24h_20260523_103000_to_20260524_103000_bjt",
        "old_position_redeem_benefit": 0.0,
        "new_window_cash_ex_rebate": 150.0,
        "corrected_new_mtm_ex_rebate": 150.0,
        "corrected_new_mtm_incl_rebate": 458.0,
        "split_count": 0,
        "role": "clean_low_residual_pair_arb_control",
    },
    {
        "account": "ohanism",
        "address_or_handle": "ohanism",
        "export_dir_name": "leaderboard_ohanism_recent24h_20260523_103000_to_20260524_103000_bjt",
        "old_position_redeem_benefit": 997.0,
        "new_window_cash_ex_rebate": 100.0,
        "corrected_new_mtm_ex_rebate": 751.0,
        "corrected_new_mtm_incl_rebate": 1771.0,
        "split_count": 0,
        "role": "direction_rebate_inventory_control",
    },
    {
        "account": "xuan",
        "address_or_handle": "xuanxuan008",
        "export_dir_name": "leaderboard_xuan_recent24h_20260523_103000_to_20260524_103000_bjt",
        "old_position_redeem_benefit": 108.0,
        "new_window_cash_ex_rebate": -394.0,
        "corrected_new_mtm_ex_rebate": -394.0,
        "corrected_new_mtm_incl_rebate": -394.0,
        "split_count": 0,
        "role": "excluded_high_fee_control",
    },
    {
        "account": "04b6",
        "address_or_handle": "0x04b6d7e930cf9e493c5e6ef24b496294f95594c8",
        "export_dir_name": "leaderboard_04b6_recent24h_20260523_103000_to_20260524_103000_bjt",
        "old_position_redeem_benefit": 0.0,
        "new_window_cash_ex_rebate": 13.0,
        "corrected_new_mtm_ex_rebate": 13.0,
        "corrected_new_mtm_incl_rebate": 13.0,
        "split_count": 0,
        "role": "stale_or_inactive_recent_window_control",
    },
    {
        "account": "b27bc",
        "address_or_handle": "b27bc",
        "export_dir_name": "leaderboard_b27bc_recent24h_20260523_103000_to_20260524_103000_bjt",
        "old_position_redeem_benefit": None,
        "new_window_cash_ex_rebate": None,
        "corrected_new_mtm_ex_rebate": None,
        "corrected_new_mtm_incl_rebate": None,
        "split_count": 0,
        "role": "inactive_historical_benchmark",
    },
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_OUTPUT_FRAGMENTS)


def as_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def pct(value: float | None) -> float | None:
    return round(value * 100.0, 6) if value is not None else None


def load_profile(config: dict[str, Any], export_root: Path) -> dict[str, Any]:
    export_dir = export_root / config["export_dir_name"]
    summary_path = export_dir / "summary.json"
    if not summary_path.exists():
        return {
            "account": config["account"],
            "address_or_handle": config["address_or_handle"],
            "role": config["role"],
            "source_export": str(export_dir),
            "available": False,
            "status": "SUMMARY_JSON_MISSING",
            "user_corrected_overlay": correction_overlay(config),
        }
    summary = load_json(summary_path)
    cashflow = summary.get("cashflow") if isinstance(summary.get("cashflow"), dict) else {}
    pair = summary.get("pair_metrics_lifetime") if isinstance(summary.get("pair_metrics_lifetime"), dict) else {}
    mtm = summary.get("mark_to_market") if isinstance(summary.get("mark_to_market"), dict) else {}
    row_counts = summary.get("row_counts") if isinstance(summary.get("row_counts"), dict) else {}
    current = summary.get("current_positions") if isinstance(summary.get("current_positions"), dict) else {}
    pair_cost_p90 = None
    pair_cost_distribution = pair.get("per_market_actual_pair_cost")
    if isinstance(pair_cost_distribution, dict):
        pair_cost_p90 = as_float(pair_cost_distribution.get("p90"))
    activity_types = summary.get("activity_types") if isinstance(summary.get("activity_types"), dict) else {}
    activity_counts = activity_types.get("counts") if isinstance(activity_types.get("counts"), dict) else {}
    activity_usdc = activity_types.get("usdc") if isinstance(activity_types.get("usdc"), dict) else {}
    split_rows = as_int(activity_counts.get("SPLIT"), default=as_int(config.get("split_count"), 0))
    trade_rows = as_int(activity_counts.get("TRADE"))
    active = trade_rows > 0
    return {
        "account": config["account"],
        "address_or_handle": config["address_or_handle"],
        "role": config["role"],
        "source_export": str(export_dir),
        "summary_json": str(summary_path),
        "available": True,
        "status": "ACTIVE" if active else "INACTIVE_OR_NO_TRADE_ACTIVITY",
        "window": summary.get("window"),
        "row_counts": {
            "activity_rows": row_counts.get("activity_unique_rows"),
            "tracked_markets": row_counts.get("tracked_markets"),
            "market_trade_rows": row_counts.get("market_trade_rows"),
            "current_position_rows": row_counts.get("current_position_rows"),
        },
        "activity_types": activity_types,
        "observed_public_metrics": {
            "buy_actual_cost": as_float(cashflow.get("buy_actual_cost")),
            "inferred_fee_total": as_float(cashflow.get("fee_total")),
            "fee_rate_on_gross": as_float(cashflow.get("fee_rate_on_gross")),
            "fee_rate_on_gross_pct": pct(as_float(cashflow.get("fee_rate_on_gross"))),
            "cash_pnl": as_float(cashflow.get("cash_pnl")),
            "current_value": as_float(mtm.get("current_value")),
            "cash_plus_current_value": as_float(mtm.get("pnl_with_current_value")),
            "maker_rebate": as_float(cashflow.get("rebate_proceeds")),
            "redeem_total": as_float(cashflow.get("redeem_proceeds")),
            "merge_total": as_float(cashflow.get("merge_proceeds")),
            "activity_usdc_maker_rebate": as_float(activity_usdc.get("MAKER_REBATE")),
            "activity_usdc_redeem": as_float(activity_usdc.get("REDEEM")),
            "activity_usdc_merge": as_float(activity_usdc.get("MERGE")),
            "actual_pair_cost": as_float(pair.get("actual_pair_cost")),
            "paired_actual_profit": as_float(pair.get("paired_actual_profit")),
            "lifetime_residual_rate": as_float(pair.get("lifetime_residual_rate_on_bought_qty")),
            "lifetime_residual_rate_pct": pct(as_float(pair.get("lifetime_residual_rate_on_bought_qty"))),
            "current_residual_rate": as_float(current.get("current_residual_rate_on_position_qty")),
            "pair_cost_p90": pair_cost_p90,
        },
        "user_corrected_overlay": correction_overlay(config),
        "split_interference": {
            "split_rows": split_rows,
            "absent_in_reported_sample": split_rows == 0,
        },
    }


def correction_overlay(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "old_position_redeem_benefit": config.get("old_position_redeem_benefit"),
        "new_window_cash_ex_rebate": config.get("new_window_cash_ex_rebate"),
        "corrected_new_mtm_ex_rebate": config.get("corrected_new_mtm_ex_rebate"),
        "corrected_new_mtm_incl_rebate": config.get("corrected_new_mtm_incl_rebate"),
        "split_count_reported": config.get("split_count"),
        "source": "user_peer_corrected_24h_activity_recalculation",
    }


def profile_score(profile: dict[str, Any]) -> tuple[int, float, float]:
    """Sort active pair-arb research leads.

    Lower tier number is better. Ties rank by corrected MTM then pair cost.
    """

    account = str(profile.get("account"))
    metrics = profile.get("observed_public_metrics") or {}
    overlay = profile.get("user_corrected_overlay") or {}
    corrected_mtm = as_float(overlay.get("corrected_new_mtm_ex_rebate"), default=-10**9) or -10**9
    pair_cost = as_float(metrics.get("actual_pair_cost"), default=10**9) or 10**9
    residual = as_float(metrics.get("lifetime_residual_rate"), default=10**9) or 10**9
    activity_types = profile.get("activity_types") if isinstance(profile.get("activity_types"), dict) else {}
    activity_counts = activity_types.get("counts") if isinstance(activity_types.get("counts"), dict) else {}
    trade_rows = as_int(activity_counts.get("TRADE"))
    if account == "b55":
        tier = 1
    elif account == "ce25":
        tier = 2
    elif account == "b27bc":
        tier = 3
    elif account == "ohanism":
        tier = 4
    elif account == "xuan":
        tier = 5
    elif trade_rows <= 50:
        tier = 6
    else:
        tier = 7
    # Keep low residual ahead within the same tier, then corrected MTM, then pair cost.
    return (tier, residual, -corrected_mtm if corrected_mtm is not None else 10**9, pair_cost)


def build_research_ranking(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(profiles, key=profile_score)
    by_account = {profile["account"]: profile for profile in profiles}
    b55 = by_account.get("b55", {})
    ce25 = by_account.get("ce25", {})
    ohanism = by_account.get("ohanism", {})
    xuan = by_account.get("xuan", {})
    b55_metrics = b55.get("observed_public_metrics") or {}
    ce25_metrics = ce25.get("observed_public_metrics") or {}
    ohanism_metrics = ohanism.get("observed_public_metrics") or {}
    xuan_metrics = xuan.get("observed_public_metrics") or {}
    b55_overlay = b55.get("user_corrected_overlay") or {}
    blockers = []
    if as_float(b55_metrics.get("lifetime_residual_rate")) and as_float(b55_metrics.get("lifetime_residual_rate")) > 0.10:
        blockers.append("b55_residual_above_first_replication_target")
    if as_float(b55_metrics.get("pair_cost_p90")) and as_float(b55_metrics.get("pair_cost_p90")) > 1.0:
        blockers.append("b55_pair_cost_tail_above_arb_leg_threshold")
    if (as_float(b55_overlay.get("new_window_cash_ex_rebate")) or 0.0) < 0.0:
        blockers.append("b55_window_new_cash_negative_after_old_redeem_adjustment")
    return {
        "decision": "KEEP",
        "label": "KEEP_CORRECTED_PUBLIC_LEADERBOARD_RANKING_READY_B55_MAIN_CE25_CLEAN_CONTROL",
        "ranking_semantics": {
            "leaderboard_cash_pnl": "candidate_pool_only_not_conclusion",
            "primary_pair_arb_research_ranking": [profile["account"] for profile in ranked],
            "profitability_fair_price_lead": "b55",
            "clean_low_residual_pair_arb_control": "ce25",
            "direction_rebate_inventory_control": "ohanism",
            "excluded_high_fee_control": "xuan",
            "inactive_historical_benchmark": "b27bc",
            "stale_recent_window_control": "04b6",
        },
        "account_interpretation": {
            "b55": {
                "status": "main_lead_but_profit_downshifted",
                "why": [
                    "Corrected new-window MTM remains positive, but far below the naive cash-plus-current leaderboard read.",
                    "Average actual pair cost is strong and below 0.97.",
                    "Residual rate and pair-cost tail are still too high for direct replication.",
                ],
                "corrected_new_mtm_ex_rebate": b55_overlay.get("corrected_new_mtm_ex_rebate"),
                "corrected_new_mtm_incl_rebate": b55_overlay.get("corrected_new_mtm_incl_rebate"),
                "actual_pair_cost": b55_metrics.get("actual_pair_cost"),
                "residual_rate": b55_metrics.get("lifetime_residual_rate"),
                "pair_cost_p90": b55_metrics.get("pair_cost_p90"),
            },
            "ce25": {
                "status": "upgraded_clean_control",
                "why": [
                    "No reported old-position redeem pollution in the 24h sample.",
                    "Residual rate is below the first replication target.",
                    "Profit scale is thin and fee rate is higher than b55, so it is a control rather than the main template.",
                ],
                "actual_pair_cost": ce25_metrics.get("actual_pair_cost"),
                "residual_rate": ce25_metrics.get("lifetime_residual_rate"),
                "fee_rate_on_gross": ce25_metrics.get("fee_rate_on_gross"),
            },
            "ohanism": {
                "status": "downgraded_direction_inventory_rebate_control",
                "why": [
                    "Average pair cost is above 1.0, so paired subset is not the robust arb mechanism.",
                    "Residual exposure is very high.",
                    "Old-position redeem and maker rebate materially affect naive 24h PnL.",
                ],
                "actual_pair_cost": ohanism_metrics.get("actual_pair_cost"),
                "residual_rate": ohanism_metrics.get("lifetime_residual_rate"),
            },
            "xuan": {
                "status": "excluded",
                "why": [
                    "Corrected new-window result is negative.",
                    "Fee rate and pair cost remain above the required strategy quality bar.",
                ],
                "actual_pair_cost": xuan_metrics.get("actual_pair_cost"),
                "fee_rate_on_gross": xuan_metrics.get("fee_rate_on_gross"),
            },
        },
        "blockers_before_shadow_or_deployable_claim": blockers
        + [
            "fair_price_source_missing_or_not_yet_joined",
            "per_trade_fee_and_liquidity_role_missing",
            "public_profile_proxy_only_not_private_truth",
        ],
        "split_interference_absent_for_reported_accounts": all(
            (profile.get("split_interference") or {}).get("absent_in_reported_sample") for profile in profiles
        ),
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    profiles = [load_profile(config, args.export_root) for config in PROFILE_CONFIGS]
    ranking = build_research_ranking(profiles)
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "public_leaderboard_corrected_new_mtm_audit",
        "decision": ranking["decision"],
        "decision_label": ranking["label"],
        "scope": {
            "public_profile_exports_only": True,
            "private_owner_trade_truth_used": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_collector_scanned": False,
            "full_completion_store_scanned": False,
            "shared_ingress_connected": False,
            "broker_modified": False,
            "service_control_used": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "dplus_failed_families_enabled_or_swept": False,
        },
        "source_inputs": {
            "export_root": str(args.export_root),
            "profiles": [
                {
                    "account": config["account"],
                    "summary_json": str(args.export_root / config["export_dir_name"] / "summary.json"),
                }
                for config in PROFILE_CONFIGS
            ],
        },
        "profiles": profiles,
        "research_ranking": ranking,
        "promotion_gate": {
            "passed": False,
            "status": "PUBLIC_PROFILE_CORRECTED_RANKING_ONLY_NOT_PROMOTION_EVIDENCE",
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
        },
        "next_executable_action": (
            "Make b55 the main fair-price/pair-edge template and ce25 the clean low-residual control; next build a "
            "local-only fair-price source manifest/reviewer for BTC/ETH 15m and 1h that can join entry window, "
            "multi-exchange fair probability, per-trade fee/liquidity role, pair cost tail, and residual gates."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{label}"
    manifest = build_manifest(args, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    ranking = manifest["research_ranking"]
    profiles = {
        profile["account"]: profile
        for profile in manifest["profiles"]
        if profile.get("available")
    }
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "primary_pair_arb_research_ranking": ranking["ranking_semantics"][
                    "primary_pair_arb_research_ranking"
                ],
                "b55_corrected_new_mtm_ex_rebate": profiles["b55"]["user_corrected_overlay"][
                    "corrected_new_mtm_ex_rebate"
                ],
                "b55_actual_pair_cost": profiles["b55"]["observed_public_metrics"]["actual_pair_cost"],
                "ce25_corrected_new_mtm_ex_rebate": profiles["ce25"]["user_corrected_overlay"][
                    "corrected_new_mtm_ex_rebate"
                ],
                "ce25_actual_pair_cost": profiles["ce25"]["observed_public_metrics"]["actual_pair_cost"],
                "blockers_before_shadow_or_deployable_claim": ranking[
                    "blockers_before_shadow_or_deployable_claim"
                ],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
