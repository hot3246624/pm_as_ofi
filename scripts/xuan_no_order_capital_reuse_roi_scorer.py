#!/usr/bin/env python3
"""Score no-order runtime ROI using merge/redeem capital reuse accounting."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any


def as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path) -> dict[str, Any]:
    with path.open() as handle:
        return json.load(handle)


def parse_time(path: Path) -> dt.datetime | None:
    if not path.exists():
        return None
    text = path.read_text().strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def pct(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value * 100.0


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def run_id(root: Path) -> str:
    return root.parent.name if root.name == "remote_outputs" else root.name


def remove_from_lots(
    lots: list[dict[str, float | str]],
    qty: float,
    source_lot_id: str | None = None,
) -> tuple[float, float]:
    """Remove qty from FIFO lots, preferring source_lot_id when provided."""

    remaining = qty
    removed_cost = 0.0
    if source_lot_id:
        for lot in list(lots):
            if lot.get("id") != source_lot_id or remaining <= 1e-12:
                continue
            take = min(remaining, float(lot["qty"]))
            removed_cost += take * float(lot["px"])
            lot["qty"] = float(lot["qty"]) - take
            remaining -= take
            if float(lot["qty"]) <= 1e-12:
                lots.remove(lot)
            break

    idx = 0
    while remaining > 1e-12 and idx < len(lots):
        lot = lots[idx]
        take = min(remaining, float(lot["qty"]))
        removed_cost += take * float(lot["px"])
        lot["qty"] = float(lot["qty"]) - take
        remaining -= take
        if float(lot["qty"]) <= 1e-12:
            lots.pop(idx)
        else:
            idx += 1
    return removed_cost, remaining


def analyze_root(root: Path) -> dict[str, Any]:
    aggregate = read_json(root / "aggregate_report.json")
    metrics = aggregate.get("metrics", {})

    rescue_rows: dict[tuple[str, str], dict[str, str]] = {}
    for path in root.glob("*.strict_rescue_closes.csv"):
        for row in read_csv(path):
            rescue_rows[(row.get("slug", ""), row.get("matched_pair_id", ""))] = row
            rescue_rows[(row.get("slug", ""), row.get("event_id", ""))] = row

    events: list[dict[str, str]] = []
    for path in root.glob("*.simulated_inventory_events.csv"):
        events.extend(read_csv(path))
    events.sort(key=lambda row: (as_int(row.get("ts_ms")), as_int(row.get("event_id"))))

    lots: dict[tuple[str, str], list[dict[str, float | str]]] = {}
    open_seed_cost = 0.0
    max_open_seed_cost = 0.0
    max_gross_cash_need = 0.0
    internal_qty = 0.0
    internal_cost = 0.0
    internal_pnl = 0.0
    rescue_qty = 0.0
    rescue_close_cash = 0.0
    warnings: list[str] = []

    def record_open() -> None:
        nonlocal max_open_seed_cost, max_gross_cash_need
        max_open_seed_cost = max(max_open_seed_cost, open_seed_cost)
        max_gross_cash_need = max(max_gross_cash_need, open_seed_cost)

    for row in events:
        event_type = row.get("inventory_event_type", "")
        slug = row.get("slug", "")
        side = row.get("side", "")

        if event_type == "would_fill_add":
            qty = as_float(row.get("qty_delta"))
            cost = as_float(row.get("cost_delta"))
            px = cost / qty if qty else 0.0
            lots.setdefault((slug, side), []).append(
                {"id": row.get("lot_id", ""), "qty": qty, "px": px}
            )
            open_seed_cost += cost
            record_open()
            continue

        if event_type == "internal_pair_redeem":
            remaining = -as_float(row.get("qty_delta"))
            yes_lots = lots.setdefault((slug, "YES"), [])
            no_lots = lots.setdefault((slug, "NO"), [])
            while remaining > 1e-12 and yes_lots and no_lots:
                take = min(remaining, float(yes_lots[0]["qty"]), float(no_lots[0]["qty"]))
                cost = take * (float(yes_lots[0]["px"]) + float(no_lots[0]["px"]))
                internal_qty += take
                internal_cost += cost
                internal_pnl += take - cost
                open_seed_cost -= cost
                yes_lots[0]["qty"] = float(yes_lots[0]["qty"]) - take
                no_lots[0]["qty"] = float(no_lots[0]["qty"]) - take
                remaining -= take
                if float(yes_lots[0]["qty"]) <= 1e-12:
                    yes_lots.pop(0)
                if float(no_lots[0]["qty"]) <= 1e-12:
                    no_lots.pop(0)
            if remaining > 1e-9:
                warnings.append(f"internal_pair_unmatched slug={slug} qty={remaining:.12f}")
            record_open()
            continue

        if event_type == "strict_rescue_redeem":
            qty = -as_float(row.get("qty_delta"))
            held_cost = -as_float(row.get("cost_delta"))
            rescue = rescue_rows.get((slug, row.get("matched_pair_id", ""))) or rescue_rows.get(
                (slug, row.get("event_id", ""))
            ) or {}
            close_cash = qty * (
                as_float(rescue.get("close_ask")) + as_float(rescue.get("fee_per_share"))
            )
            max_gross_cash_need = max(max_gross_cash_need, open_seed_cost + close_cash)
            rescue_qty += qty
            rescue_close_cash += close_cash
            removed_cost, remaining = remove_from_lots(
                lots.setdefault((slug, side), []),
                qty,
                row.get("source_lot_id"),
            )
            if remaining > 1e-9:
                warnings.append(
                    f"strict_rescue_unmatched slug={slug} side={side} qty={remaining:.12f}"
                )
            if abs(removed_cost - held_cost) > 1e-6:
                warnings.append(
                    "strict_rescue_removed_cost_mismatch "
                    f"slug={slug} removed={removed_cost:.12f} held_cost={held_cost:.12f}"
                )
            open_seed_cost -= held_cost
            record_open()

    residual_from_lots = sum(
        float(lot["qty"]) * float(lot["px"]) for lot_list in lots.values() for lot in lot_list
    )
    residual_cost = as_float(metrics.get("residual_cost"))
    if abs(residual_from_lots - residual_cost) > 1e-6 or abs(open_seed_cost - residual_cost) > 1e-6:
        warnings.append(
            "residual_reconstruction_mismatch "
            f"lots={residual_from_lots:.12f} open={open_seed_cost:.12f} metrics={residual_cost:.12f}"
        )

    started = parse_time(root / "run_started_utc.txt")
    finished = parse_time(root / "run_finished_utc.txt")
    duration_minutes = (
        (finished - started).total_seconds() / 60.0 if started and finished and finished > started else None
    )

    pair_pnl = as_float(metrics.get("pair_pnl"))
    pair_qty = as_float(metrics.get("pair_qty"))
    pair_cost = pair_qty - pair_pnl
    filled_cost = as_float(metrics.get("filled_cost"))
    completion_cost = as_float(metrics.get("completion_cost"))
    taker_fee = as_float(metrics.get("taker_fee"))
    total_cash_spend = filled_cost + completion_cost + taker_fee

    return {
        "run_id": run_id(root),
        "output_root": str(root),
        "duration_minutes": round_opt(duration_minutes, 4),
        "metrics": {
            "accepted_actions": as_float(metrics.get("candidates")),
            "queue_supported_fills": as_float(metrics.get("queue_supported_fills")),
            "strict_rescue_closes": as_float(metrics.get("strict_rescue_actions")),
            "pair_qty": pair_qty,
            "pair_pnl": pair_pnl,
            "filled_cost": filled_cost,
            "completion_cost": completion_cost,
            "taker_fee": taker_fee,
            "total_cash_spend": total_cash_spend,
            "residual_cost": residual_cost,
            "residual_qty": as_float(metrics.get("residual_qty")),
        },
        "capital_reuse": {
            "internal_pair_qty": internal_qty,
            "internal_pair_cost": internal_cost,
            "internal_pair_pnl": internal_pnl,
            "strict_rescue_qty": rescue_qty,
            "strict_rescue_close_cash": rescue_close_cash,
            "max_open_seed_cost": max_open_seed_cost,
            "max_gross_cash_need": max_gross_cash_need,
            "turnover_filled_cost_on_max_open_seed_cost": filled_cost / max_open_seed_cost
            if max_open_seed_cost
            else None,
            "capital_roi_on_max_open_seed_cost": pair_pnl / max_open_seed_cost
            if max_open_seed_cost
            else None,
            "capital_roi_on_max_gross_cash_need": pair_pnl / max_gross_cash_need
            if max_gross_cash_need
            else None,
        },
        "round_roi": {
            "pair_cost": pair_cost,
            "roi_on_pair_cost": pair_pnl / pair_cost if pair_cost else None,
            "edge_on_redeem_notional": pair_pnl / pair_qty if pair_qty else None,
            "roi_on_filled_seed_cost": pair_pnl / filled_cost if filled_cost else None,
            "roi_on_total_cash_spend": pair_pnl / total_cash_spend if total_cash_spend else None,
            "residual_cost_to_pair_pnl": residual_cost / pair_pnl if pair_pnl else None,
            "residual_cost_to_pair_qty": residual_cost / pair_qty if pair_qty else None,
        },
        "warnings": warnings,
    }


def aggregate_runs(
    runs: list[dict[str, Any]], round_notional: float, round_minutes: float
) -> dict[str, Any]:
    totals: dict[str, float] = {}
    for key in (
        "accepted_actions",
        "queue_supported_fills",
        "strict_rescue_closes",
        "pair_qty",
        "pair_pnl",
        "filled_cost",
        "completion_cost",
        "taker_fee",
        "total_cash_spend",
        "residual_cost",
        "residual_qty",
    ):
        totals[key] = sum(as_float(run["metrics"].get(key)) for run in runs)
    for key in ("internal_pair_qty", "internal_pair_cost", "internal_pair_pnl", "strict_rescue_qty", "strict_rescue_close_cash"):
        totals[key] = sum(as_float(run["capital_reuse"].get(key)) for run in runs)

    total_duration_minutes = sum(
        as_float(run.get("duration_minutes")) for run in runs if run.get("duration_minutes") is not None
    )
    pair_cost = totals["pair_qty"] - totals["pair_pnl"]
    max_open = max(as_float(run["capital_reuse"].get("max_open_seed_cost")) for run in runs)
    max_gross = max(as_float(run["capital_reuse"].get("max_gross_cash_need")) for run in runs)
    observed_pnl_per_day = (
        totals["pair_pnl"] * (1440.0 / total_duration_minutes) if total_duration_minutes else None
    )
    rounds_per_day = 1440.0 / round_minutes if round_minutes else None
    edge_on_redeem = totals["pair_pnl"] / totals["pair_qty"] if totals["pair_qty"] else None
    roi_on_pair_cost = totals["pair_pnl"] / pair_cost if pair_cost else None
    profit_per_round_on_redeem_notional = (
        round_notional * edge_on_redeem if edge_on_redeem is not None else None
    )
    profit_per_round_on_pair_cost = (
        round_notional * roi_on_pair_cost if roi_on_pair_cost is not None else None
    )
    profit_per_day_on_redeem_notional = (
        profit_per_round_on_redeem_notional * rounds_per_day
        if profit_per_round_on_redeem_notional is not None and rounds_per_day is not None
        else None
    )
    profit_per_day_on_pair_cost = (
        profit_per_round_on_pair_cost * rounds_per_day
        if profit_per_round_on_pair_cost is not None and rounds_per_day is not None
        else None
    )
    capital_roi_on_peak_gross = totals["pair_pnl"] / max_gross if max_gross else None
    profit_per_day_on_reused_capital = (
        round_notional * capital_roi_on_peak_gross * (1440.0 / total_duration_minutes)
        if capital_roi_on_peak_gross is not None and total_duration_minutes
        else None
    )

    return {
        "window_count": len(runs),
        "total_duration_minutes": round_opt(total_duration_minutes, 4),
        "totals": totals,
        "capital_reuse": {
            "max_window_open_seed_cost": max_open,
            "max_window_gross_cash_need": max_gross,
            "turnover_filled_cost_on_max_window_open_seed_cost": totals["filled_cost"] / max_open
            if max_open
            else None,
            "capital_roi_on_max_window_open_seed_cost": totals["pair_pnl"] / max_open
            if max_open
            else None,
            "capital_roi_on_max_window_gross_cash_need": capital_roi_on_peak_gross,
        },
        "round_roi": {
            "pair_cost": pair_cost,
            "roi_on_pair_cost": roi_on_pair_cost,
            "edge_on_redeem_notional": edge_on_redeem,
            "roi_on_filled_seed_cost": totals["pair_pnl"] / totals["filled_cost"]
            if totals["filled_cost"]
            else None,
            "roi_on_total_cash_spend": totals["pair_pnl"] / totals["total_cash_spend"]
            if totals["total_cash_spend"]
            else None,
            "residual_cost_to_pair_pnl": totals["residual_cost"] / totals["pair_pnl"]
            if totals["pair_pnl"]
            else None,
            "residual_cost_to_pair_qty": totals["residual_cost"] / totals["pair_qty"]
            if totals["pair_qty"]
            else None,
            "worst_case_pair_pnl_if_residual_zero": totals["pair_pnl"] - totals["residual_cost"],
        },
        "projection": {
            "round_minutes": round_minutes,
            "rounds_per_day": rounds_per_day,
            "round_notional": round_notional,
            "observed_current_scale_pair_pnl_per_day": observed_pnl_per_day,
            "profit_per_round_if_redeem_notional_filled": profit_per_round_on_redeem_notional,
            "profit_per_day_if_redeem_notional_filled_every_round": profit_per_day_on_redeem_notional,
            "profit_per_round_if_pair_cost_filled": profit_per_round_on_pair_cost,
            "profit_per_day_if_pair_cost_filled_every_round": profit_per_day_on_pair_cost,
            "profit_per_day_if_reused_capital_scales_linearly": profit_per_day_on_reused_capital,
        },
    }


def write_markdown(path: Path, scorecard: dict[str, Any]) -> None:
    agg = scorecard["aggregate"]
    projection = agg["projection"]
    round_roi = agg["round_roi"]
    capital = agg["capital_reuse"]
    totals = agg["totals"]
    lines = [
        "# Xuan No-Order Capital Reuse ROI Review",
        "",
        "## Status",
        "",
        f"- status: `{scorecard['status']}`",
        f"- windows: `{agg['window_count']}`",
        f"- total_duration_minutes: `{agg['total_duration_minutes']}`",
        f"- research_only: `{scorecard['decision']['research_only']}`",
        f"- deployable: `{scorecard['decision']['deployable']}`",
        "",
        "## Correct ROI Denominators",
        "",
        f"- pair_pnl: `{round_opt(totals['pair_pnl'], 6)}`",
        f"- pair_qty/redeem_notional: `{round_opt(totals['pair_qty'], 6)}`",
        f"- pair_cost: `{round_opt(round_roi['pair_cost'], 6)}`",
        f"- edge_on_redeem_notional: `{round_opt(pct(round_roi['edge_on_redeem_notional']), 4)}%`",
        f"- roi_on_pair_cost: `{round_opt(pct(round_roi['roi_on_pair_cost']), 4)}%`",
        f"- roi_on_filled_seed_cost_legacy: `{round_opt(pct(round_roi['roi_on_filled_seed_cost']), 4)}%`",
        f"- roi_on_total_cash_spend: `{round_opt(pct(round_roi['roi_on_total_cash_spend']), 4)}%`",
        "",
        "## Merge/Reuse Capital",
        "",
        f"- max_window_open_seed_cost: `{round_opt(capital['max_window_open_seed_cost'], 6)}`",
        f"- max_window_gross_cash_need: `{round_opt(capital['max_window_gross_cash_need'], 6)}`",
        f"- filled_cost_turnover_on_max_open: `{round_opt(capital['turnover_filled_cost_on_max_window_open_seed_cost'], 4)}x`",
        f"- capital_roi_on_max_window_gross_cash_need: `{round_opt(pct(capital['capital_roi_on_max_window_gross_cash_need']), 4)}%`",
        "",
        "## Residual Risk",
        "",
        f"- residual_cost: `{round_opt(totals['residual_cost'], 6)}`",
        f"- residual_cost_to_pair_qty: `{round_opt(pct(round_roi['residual_cost_to_pair_qty']), 4)}%`",
        f"- residual_cost_to_pair_pnl: `{round_opt(round_roi['residual_cost_to_pair_pnl'], 4)}x`",
        f"- worst_case_pair_pnl_if_residual_zero: `{round_opt(round_roi['worst_case_pair_pnl_if_residual_zero'], 6)}`",
        "",
        "## 300 USD Per-Round Projection",
        "",
        f"- round_minutes: `{projection['round_minutes']}`",
        f"- rounds_per_day: `{round_opt(projection['rounds_per_day'], 4)}`",
        f"- round_notional: `{projection['round_notional']}`",
        f"- profit_per_round_if_300_redeem_notional_filled: `{round_opt(projection['profit_per_round_if_redeem_notional_filled'], 6)}`",
        f"- profit_per_day_if_300_redeem_notional_filled_every_round: `{round_opt(projection['profit_per_day_if_redeem_notional_filled_every_round'], 6)}`",
        f"- profit_per_round_if_300_pair_cost_filled: `{round_opt(projection['profit_per_round_if_pair_cost_filled'], 6)}`",
        f"- profit_per_day_if_300_pair_cost_filled_every_round: `{round_opt(projection['profit_per_day_if_pair_cost_filled_every_round'], 6)}`",
        f"- observed_current_scale_pair_pnl_per_day: `{round_opt(projection['observed_current_scale_pair_pnl_per_day'], 6)}`",
        "",
        "## Caveats",
        "",
        "- The 300 USD projection is a linear capacity hypothetical, not runtime evidence at 300 USD size.",
        "- The scorer accounts for merge/redeem reuse by releasing FIFO YES/NO lot cost on internal pairs and held-lot cost on strict rescue closes.",
        "- Residual cost is still open tail risk. The worst-case residual-zero line is intentionally conservative.",
        "- Existing `roi_on_filled_cost` is retained only as a legacy turnover metric and should not be compared directly with per-round trader ROI.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", action="append", required=True, help="Runtime output root")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown")
    parser.add_argument("--round-notional", type=float, default=300.0)
    parser.add_argument("--round-minutes", type=float, default=5.0)
    args = parser.parse_args()

    roots = [Path(path) for path in args.output_root]
    runs = [analyze_root(root) for root in roots]
    aggregate = aggregate_runs(runs, args.round_notional, args.round_minutes)
    hard_blockers: list[str] = []
    warnings = [warning for run in runs for warning in run.get("warnings", [])]
    if warnings:
        hard_blockers.append("capital_reuse_reconstruction_warnings")
    if aggregate["round_roi"]["edge_on_redeem_notional"] is None:
        hard_blockers.append("missing_pair_redeem_notional")
    if aggregate["capital_reuse"]["max_window_gross_cash_need"] <= 0:
        hard_blockers.append("missing_peak_capital")

    status = (
        "KEEP_CAPITAL_REUSE_ROI_SCORER_PASS_RESEARCH_ONLY"
        if not hard_blockers
        else "UNKNOWN_CAPITAL_REUSE_ROI_SCORER_BLOCKED"
    )
    scorecard = {
        "kind": "xuan_no_order_capital_reuse_roi_scorer",
        "status": status,
        "inputs": {
            "output_roots": [str(root) for root in roots],
            "round_notional": args.round_notional,
            "round_minutes": args.round_minutes,
        },
        "assumptions": {
            "merge_reuse_accounted": True,
            "internal_pair_release": "fifo_yes_no_lot_cost",
            "strict_rescue_release": "held_lot_cost_delta",
            "strict_rescue_close_cash_in_peak_gross_need": True,
            "projection_is_linear_capacity_hypothesis": True,
        },
        "decision": {
            "research_only": True,
            "deployable": False,
            "shadow_review_ready_metric_addendum": not hard_blockers,
            "hard_blockers": hard_blockers,
        },
        "runs": runs,
        "aggregate": aggregate,
        "warnings": warnings,
    }
    scorecard_path = Path(args.scorecard_json)
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    if args.markdown:
        write_markdown(Path(args.markdown), scorecard)
    return 0 if not hard_blockers else 2


if __name__ == "__main__":
    raise SystemExit(main())
