#!/usr/bin/env python3
"""Score ce25 public market-sequence rows against reproducible rule profiles."""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


PROFILES = {
    "ce25_starter": {
        "assets": {"SOL", "ETH"},
        "timeframes": {"5m", "15m"},
        "pair_cost_lt": 0.90,
        "residual_rate_lt": 0.15,
    },
    "ce25_core": {
        "assets": {"BTC", "ETH", "SOL"},
        "timeframes": {"5m", "15m"},
        "pair_cost_lt": 0.95,
        "residual_rate_lt": 0.10,
    },
}


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value or default)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def pct(value: float | None, digits: int = 4) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value * 100.0, digits)


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def row_passes(row: dict[str, Any], profile: dict[str, Any]) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    if row.get("asset") not in profile["assets"]:
        blockers.append("asset_not_in_profile")
    if row.get("tf") not in profile["timeframes"]:
        blockers.append("timeframe_not_in_profile")
    if fnum(row.get("pair_cost"), 99.0) >= profile["pair_cost_lt"]:
        blockers.append("pair_cost_above_profile")
    if fnum(row.get("resid_rate"), 99.0) >= profile["residual_rate_lt"]:
        blockers.append("residual_rate_above_profile")
    return not blockers, blockers


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    markets = len(rows)
    buy_actual = sum(fnum(row.get("buy_actual")) for row in rows)
    cohort_pnl = sum(fnum(row.get("cohort_pnl")) for row in rows)
    pair_pnl = sum(fnum(row.get("pair_pnl")) for row in rows)
    residual_pnl = sum(fnum(row.get("residual_pnl_est")) for row in rows)
    paired_qty = sum(fnum(row.get("paired_qty")) for row in rows)
    pair_cost_num = sum(fnum(row.get("pair_cost")) * fnum(row.get("paired_qty")) for row in rows)
    buy_qty = sum(fnum(row.get("buy_qty")) for row in rows)
    resid_qty = sum(fnum(row.get("resid_qty")) for row in rows)
    wins = sum(1 for row in rows if fnum(row.get("cohort_pnl")) > 0)
    losses = sum(1 for row in rows if fnum(row.get("cohort_pnl")) < 0)
    return {
        "markets": markets,
        "buy_actual": round_opt(buy_actual),
        "cohort_pnl": round_opt(cohort_pnl),
        "roi": round_opt(cohort_pnl / buy_actual if buy_actual else None),
        "roi_pct": pct(cohort_pnl / buy_actual if buy_actual else None),
        "pair_pnl": round_opt(pair_pnl),
        "residual_pnl_est": round_opt(residual_pnl),
        "weighted_pair_cost": round_opt(pair_cost_num / paired_qty if paired_qty else None),
        "residual_rate": round_opt(resid_qty / buy_qty if buy_qty else None),
        "residual_rate_pct": pct(resid_qty / buy_qty if buy_qty else None),
        "wins": wins,
        "losses": losses,
    }


def profile_score(rows: list[dict[str, Any]], profile_name: str) -> dict[str, Any]:
    profile = PROFILES[profile_name]
    accepted: list[dict[str, Any]] = []
    blocker_counts: dict[str, int] = {}
    for row in rows:
        passed, blockers = row_passes(row, profile)
        if passed:
            accepted.append(row)
        for blocker in blockers:
            blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
    pair_ge_1 = [row for row in rows if fnum(row.get("pair_cost")) >= 1.0]
    pair_095_1 = [row for row in rows if 0.95 <= fnum(row.get("pair_cost")) < 1.0]
    last_60 = [row for row in accepted if row.get("last_delta_bucket") == "last_60s"]
    one_sided = [row for row in accepted if row.get("pair_delay_bucket") == "one_sided"]
    return {
        "profile": profile_name,
        "rules": {
            "assets": sorted(profile["assets"]),
            "timeframes": sorted(profile["timeframes"]),
            "pair_cost_lt": profile["pair_cost_lt"],
            "residual_rate_lt": profile["residual_rate_lt"],
            "pair_cost_hard_stop_gte": 1.0,
        },
        "accepted": summarize(accepted),
        "accepted_share_of_markets": round_opt(len(accepted) / len(rows) if rows else None),
        "accepted_share_of_markets_pct": pct(len(accepted) / len(rows) if rows else None),
        "accepted_last_60s_cleanup": summarize(last_60),
        "accepted_one_sided": summarize(one_sided),
        "rejected_pair_cost_095_to_1": summarize(pair_095_1),
        "rejected_pair_cost_gte_1": summarize(pair_ge_1),
        "blocker_counts": dict(sorted(blocker_counts.items())),
    }


def render_markdown(card: dict[str, Any]) -> str:
    lines = [
        "# Xuan Ce25 Public Profile Rule Scorer",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- remote_runner_allowed: `{card['decision']['remote_runner_allowed']}`",
        f"- deployable: `{card['decision']['deployable']}`",
        "",
        "## Profiles",
        "",
    ]
    for profile in card["profiles"]:
        accepted = profile["accepted"]
        rejected = profile["rejected_pair_cost_gte_1"]
        lines.extend(
            [
                f"### {profile['profile']}",
                "",
                f"- accepted_markets: `{accepted['markets']}`",
                f"- accepted_buy_actual: `{accepted['buy_actual']}`",
                f"- accepted_cohort_pnl: `{accepted['cohort_pnl']}`",
                f"- accepted_roi_pct: `{accepted['roi_pct']}`",
                f"- weighted_pair_cost: `{accepted['weighted_pair_cost']}`",
                f"- residual_rate_pct: `{accepted['residual_rate_pct']}`",
                f"- rejected_pair_cost_gte_1_pnl: `{rejected['cohort_pnl']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Boundary",
            "",
            "- Reads public exported ce25 market sequence only.",
            "- Local/research-only; no remote run, live order, deploy, restart, shared-service mutation, localagg mutation, or raw/replay mutation.",
        ]
    )
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    rows = read_csv(Path(args.market_sequence_csv).expanduser().resolve())
    profiles = [profile_score(rows, name) for name in ("ce25_starter", "ce25_core")]
    hard_blockers: list[str] = []
    if not rows:
        hard_blockers.append("market_sequence_empty")
    status = "KEEP_CE25_PUBLIC_PROFILE_RULE_SCORER_PASS_RESEARCH_ONLY" if not hard_blockers else "UNKNOWN_CE25_PUBLIC_PROFILE_RULE_SCORER_BLOCKED"
    return {
        "artifact": "xuan_ce25_public_profile_rule_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_ce25_public_profile_rule_scorer.py",
        "status": status,
        "inputs": {
            "market_sequence_csv": str(Path(args.market_sequence_csv).expanduser().resolve()),
        },
        "source_report": "/Users/hot/web3Scientist/poly_trans_research/data/exports/ce25_execution_profile_20260523_103000_to_20260524_103000_bjt/ce25_deep_report.md",
        "profiles": profiles,
        "decision": {
            "hard_blockers": hard_blockers,
            "research_only": True,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "shared_service_mutation_allowed": False,
            "localagg_mutation_allowed": False,
            "raw_replay_mutation_allowed": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market-sequence-csv", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    args = parser.parse_args()

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if not card["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
