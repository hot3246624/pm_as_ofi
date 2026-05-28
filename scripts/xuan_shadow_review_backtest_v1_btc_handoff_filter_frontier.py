#!/usr/bin/env python3
"""Build a BTC same-window handoff filter frontier.

This script turns the BTC same-window research shortlist into explicit residual
tiers and behavior diagnostics.  It is research-only: no candidate import,
runner profile, manifest, preauthorization, remote run, deploy, or live order is
created or authorized.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable


STAMP = "20260528T1240Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
RESCORE_ROOT = POLY_BT_ROOT / "derived/contract_examples/xuan_completion_candidate_rescore_latest"
DEFAULT_SHORTLIST = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    "xuan_shadow_review_backtest_v1_same_window_handoff_behavior_review_20260528T1231Z/"
    "BTC_SAME_WINDOW_RESEARCH_SHORTLIST.csv"
)
DEFAULT_ACTIONS = RESCORE_ROOT / "xuan_completion_candidate_same_window_handoff_actions.csv"
DEFAULT_RESIDUAL_LOTS = RESCORE_ROOT / "xuan_completion_candidate_same_window_handoff_residual_lots.csv"
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_btc_handoff_filter_frontier_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_btc_handoff_filter_frontier_{STAMP}"
)


def fnum(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def clean(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: clean(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: clean(value) for key, value in row.items()})


def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(max(math.ceil(len(ordered) * q) - 1, 0), len(ordered) - 1)
    return ordered[idx]


def dist(values: list[float]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "p50": 0.0, "p90": 0.0, "max": 0.0, "avg": 0.0}
    return {
        "min": min(values),
        "p50": statistics.median(values),
        "p90": percentile(values, 0.90),
        "max": max(values),
        "avg": sum(values) / len(values),
    }


def group_by_rank(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("handoff_rank"))].append(row)
    return grouped


def switch_ratio(actions: list[dict[str, str]]) -> float:
    ordered = sorted(actions, key=lambda row: int(fnum(row.get("same_window_action_seq"))))
    if len(ordered) <= 1:
        return 0.0
    switches = sum(1 for idx in range(1, len(ordered)) if ordered[idx].get("side") != ordered[idx - 1].get("side"))
    return switches / (len(ordered) - 1)


def side_imbalance(actions: list[dict[str, str]]) -> float:
    if not actions:
        return 0.0
    counts = Counter(row.get("side") for row in actions)
    return abs(counts.get("YES", 0) - counts.get("NO", 0)) / len(actions)


def summarize_variant(
    name: str,
    rows: list[dict[str, str]],
    actions_by_rank: dict[str, list[dict[str, str]]],
    lots_by_rank: dict[str, list[dict[str, str]]],
) -> dict[str, Any]:
    ranks = {str(row.get("handoff_rank")) for row in rows}
    actions = [action for rank in ranks for action in actions_by_rank.get(rank, [])]
    lots = [lot for rank in ranks for lot in lots_by_rank.get(rank, [])]
    gross = sum(fnum(row.get("gross_buy_cost")) for row in rows)
    core = sum(fnum(row.get("core_pair_after_fee_pnl")) for row in rows)
    residual_cost = sum(fnum(row.get("market_end_residual_cost")) for row in rows)
    zero_stress = sum(fnum(row.get("zero_stress_after_fee_pnl")) for row in rows)
    day_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        day_rows[str(row.get("day"))].append(row)
    day_zero = [
        {
            "day": day,
            "market_count": len(day_group),
            "zero_stress_after_fee_pnl": sum(fnum(row.get("zero_stress_after_fee_pnl")) for row in day_group),
        }
        for day, day_group in sorted(day_rows.items())
    ]
    return clean(
        {
            "variant": name,
            "market_count": len(rows),
            "day_count": len(day_rows),
            "all_days_in_full_window": len(day_rows) == 15,
            "all_present_days_zero_stress_positive": all(
                fnum(row.get("zero_stress_after_fee_pnl")) > 0 for row in day_zero
            ),
            "gross_buy_cost": gross,
            "core_pair_after_fee_pnl": core,
            "market_end_residual_cost": residual_cost,
            "zero_stress_after_fee_pnl": zero_stress,
            "residual_cost_share": residual_cost / gross if gross else 0.0,
            "action_rows": len(actions),
            "residual_lot_rows": len(lots),
            "capital_turnover": dist([fnum(row.get("capital_turnover")) for row in rows]),
            "same_window_duration_s": dist([fnum(row.get("same_window_duration_s")) for row in rows]),
            "same_window_action_rows": dist([fnum(row.get("same_window_action_rows")) for row in rows]),
            "max_residual_age_s": dist([fnum(row.get("max_residual_age_s")) for row in rows]),
            "switch_ratio": dist([switch_ratio(actions_by_rank.get(rank, [])) for rank in ranks]),
            "side_imbalance": dist([side_imbalance(actions_by_rank.get(rank, [])) for rank in ranks]),
            "action_side_counts": dict(Counter(action.get("side") for action in actions)),
            "action_alignment_counts": dict(Counter(action.get("side_alignment") for action in actions)),
            "residual_side_counts": dict(Counter(lot.get("side") for lot in lots)),
            "day_zero_stress": day_zero,
        }
    )


def variant_rows(rows: list[dict[str, str]]) -> list[tuple[str, Callable[[dict[str, str]], bool]]]:
    return [
        ("zero_residual", lambda row: fnum(row.get("market_end_residual_cost")) == 0),
        ("residual_share_le_1pct", lambda row: fnum(row.get("residual_cost_share")) <= 0.01),
        ("residual_share_le_2pct", lambda row: fnum(row.get("residual_cost_share")) <= 0.02),
        ("residual_share_le_3pct", lambda row: fnum(row.get("residual_cost_share")) <= 0.03),
        ("residual_share_le_5pct", lambda row: fnum(row.get("residual_cost_share")) <= 0.05),
    ]


def write_variant_csv(path: Path, variants: list[dict[str, Any]]) -> None:
    rows = [
        {
            "variant": item["variant"],
            "market_count": item["market_count"],
            "day_count": item["day_count"],
            "all_days_in_full_window": item["all_days_in_full_window"],
            "gross_buy_cost": item["gross_buy_cost"],
            "core_pair_after_fee_pnl": item["core_pair_after_fee_pnl"],
            "market_end_residual_cost": item["market_end_residual_cost"],
            "zero_stress_after_fee_pnl": item["zero_stress_after_fee_pnl"],
            "residual_cost_share": item["residual_cost_share"],
            "action_rows": item["action_rows"],
            "switch_ratio_p50": item["switch_ratio"]["p50"],
            "side_imbalance_p50": item["side_imbalance"]["p50"],
            "capital_turnover_p50": item["capital_turnover"]["p50"],
            "max_residual_age_s_p90": item["max_residual_age_s"]["p90"],
        }
        for item in variants
    ]
    write_csv(path, rows)


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    shortlist = read_csv(args.shortlist)
    actions_by_rank = group_by_rank(read_csv(args.actions))
    lots_by_rank = group_by_rank(read_csv(args.residual_lots))
    variants = [
        summarize_variant(name, [row for row in shortlist if pred(row)], actions_by_rank, lots_by_rank)
        for name, pred in variant_rows(shortlist)
    ]
    by_name = {variant["variant"]: variant for variant in variants}
    selected = by_name["residual_share_le_3pct"]

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    variant_csv = args.artifact_dir / "BTC_HANDOFF_FILTER_FRONTIER.csv"
    write_variant_csv(variant_csv, variants)
    selected_csv = args.artifact_dir / "BTC_HANDOFF_RESEARCH_FILTER_LE3PCT.csv"
    write_csv(
        selected_csv,
        [
            dict(row, research_filter="btc_same_window_residual_share_le_3pct_v1", candidate_import_allowed=False)
            for row in shortlist
            if fnum(row.get("residual_cost_share")) <= 0.03
        ],
    )

    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_btc_handoff_filter_frontier",
            "status": "KEEP_SHADOW_REVIEW_BACKTEST_V1_BTC_HANDOFF_FILTER_FRONTIER_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_btc_handoff_filter_frontier.py",
            "inputs": {
                "shortlist": str(args.shortlist),
                "actions": str(args.actions),
                "residual_lots": str(args.residual_lots),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "frontier_csv": str(variant_csv),
                "frontier_csv_sha256": sha256(variant_csv),
                "selected_research_filter_csv": str(selected_csv),
                "selected_research_filter_csv_sha256": sha256(selected_csv),
                "markdown": str(args.artifact_dir / "BTC_HANDOFF_FILTER_FRONTIER.md"),
            },
            "decision": {
                "handoff_filter_frontier_ready": True,
                "recommended_research_filter": "btc_same_window_residual_share_le_3pct_v1",
                "recommended_filter_reason": "covers all 15 valid days while cutting residual cost share materially versus the <=5% shortlist",
                "recommended_filter_market_count": selected["market_count"],
                "recommended_filter_day_count": selected["day_count"],
                "recommended_filter_zero_stress_after_fee_pnl": selected["zero_stress_after_fee_pnl"],
                "recommended_filter_residual_cost_share": selected["residual_cost_share"],
                "zero_residual_filter_too_narrow": by_name["zero_residual"]["day_count"] < 15,
                "two_pct_filter_nearly_complete_but_misses_day": by_name["residual_share_le_2pct"]["day_count"] < 15,
                "same_window_handoff_is_owner_private_truth": False,
                "candidate_import_allowed": False,
                "runner_support_ready": False,
                "manifest_or_preauthorization_ready": False,
                "future_remote_allowed_by_this_frontier": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "next_lane": "request_filter_specific_capital_ledger_and_import_contract_for_selected_research_filter",
            },
            "variant_frontier": variants,
            "selected_research_filter_contract": {
                "filter_name": "btc_same_window_residual_share_le_3pct_v1",
                "filter_source": "same_window_handoff_research_only",
                "asset": "BTC",
                "requires_core_pair_after_fee_positive": True,
                "requires_zero_stress_after_fee_positive": True,
                "max_residual_cost_share": 0.03,
                "requires_side_count_two": True,
                "requires_research_only_true": True,
                "candidate_import_allowed": False,
            },
            "required_preflight_before_any_canary": [
                "source_semantics_contract_id",
                "source_dataset_fingerprint",
                "l2_top_overlay_contract_id",
                "filter_specific_capital_ledger_for_btc_same_window_residual_share_le_3pct_v1",
                "research_only_import_contract_table_with_deterministic_rank",
                "runner_profile_id_and_max_notional_cap",
                "dry_run_only_true_and_import_enabled_false",
                "owner_private_truth_schema",
                "runner_import_preflight_gate",
            ],
            "warnings": [
                "The selected filter is a research filter, not an import contract.",
                "Same-window handoff is not owner private truth.",
                "Residual settlement PnL remains posthoc and is not strategy edge.",
                "No import, runner, remote, deploy, or live order is authorized.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = card.get("decision", {})
    lines = [
        "# BTC Handoff Filter Frontier",
        "",
        f"- status: `{card.get('status')}`",
        f"- recommended_research_filter: `{decision.get('recommended_research_filter')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## Frontier",
        "",
        "| variant | markets | days | zero-stress | residual share | p50 switch |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in card.get("variant_frontier", []):
        lines.append(
            "| `{variant}` | `{markets}` | `{days}` | `{zero}` | `{share}` | `{switch}` |".format(
                variant=item.get("variant"),
                markets=item.get("market_count"),
                days=item.get("day_count"),
                zero=item.get("zero_stress_after_fee_pnl"),
                share=item.get("residual_cost_share"),
                switch=item.get("switch_ratio", {}).get("p50"),
            )
        )
    lines.extend(
        [
            "",
            "## Selected Research Filter",
            "",
            "- `btc_same_window_residual_share_le_3pct_v1` is the current research choice because it keeps all 15 days while reducing residual exposure versus the broader 5% shortlist.",
            "- It is not a live canary and not an import list.",
            "",
            "## Required Before Any Canary",
            "",
        ]
    )
    for item in card.get("required_preflight_before_any_canary", []):
        lines.append(f"- `{item}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Zero-residual rows are clean but too sparse.",
            "- The 2% tier is strong but misses one valid day.",
            "- The 3% tier is the best current research compromise between coverage and residual control.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shortlist", type=Path, default=DEFAULT_SHORTLIST)
    parser.add_argument("--actions", type=Path, default=DEFAULT_ACTIONS)
    parser.add_argument("--residual-lots", type=Path, default=DEFAULT_RESIDUAL_LOTS)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "BTC_HANDOFF_FILTER_FRONTIER.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
