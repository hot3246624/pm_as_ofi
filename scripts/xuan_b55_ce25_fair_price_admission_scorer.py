#!/usr/bin/env python3
"""Score b55/ce25-inspired fair-price admission rows.

This is a local/read-only scorer. It does not read shared sockets, mutate
localagg, authorize remote execution, or place orders. It turns public
benchmark lessons into an explicit candidate-row contract:

* BTC/ETH only
* 15m or named-hour markets
* entry window mostly end-15m to end-1m
* fee-aware fair-price edge
* fee-included pair-cost discipline
* residual budget labels kept separate from pair-arb evidence
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


ASSET_SLUG_HINTS = {
    "BTC": ("btc-", "bitcoin-"),
    "ETH": ("eth-", "ethereum-"),
}
SUPPORTED_TIMEFRAMES = {"15m", "1h", "1h_or_named", "named_hour"}


def as_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def pct(value: float | None, digits: int = 4) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value * 100.0, digits)


def get_first(row: dict[str, Any], names: Iterable[str], default: Any = None) -> Any:
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    return default


def normalize_side(value: Any) -> str:
    side = str(value or "YES").strip().upper()
    if side in {"UP", "YES", "Y"}:
        return "YES"
    if side in {"DOWN", "NO", "N"}:
        return "NO"
    return side


def infer_asset(row: dict[str, Any]) -> str | None:
    raw = get_first(row, ["asset", "symbol", "underlying", "coin"])
    if raw:
        asset = str(raw).strip().upper()
        if asset in ASSET_SLUG_HINTS:
            return asset
    slug = str(get_first(row, ["market_slug", "slug"], "")).lower()
    for asset, hints in ASSET_SLUG_HINTS.items():
        if any(hint in slug for hint in hints):
            return asset
    return None


def infer_timeframe(row: dict[str, Any]) -> str | None:
    raw = get_first(row, ["timeframe", "market_timeframe", "duration"])
    if raw:
        value = str(raw).strip().lower()
        if value in SUPPORTED_TIMEFRAMES:
            return "1h_or_named" if value in {"1h", "named_hour"} else value
    slug = str(get_first(row, ["market_slug", "slug"], "")).lower()
    if "15m" in slug:
        return "15m"
    if "up-or-down" in slug:
        return "1h_or_named"
    return None


def official_fee_per_share(px: float, fee_rate: float) -> float:
    return fee_rate * px * (1.0 - px)


def fair_side_probability(row: dict[str, Any], side: str) -> float | None:
    direct = as_float(
        get_first(
            row,
            [
                "fair_side_probability",
                "side_fair_probability",
                "fair_probability_side",
                "fair_prob_side",
            ],
        )
    )
    if direct is not None:
        return direct
    yes_prob = as_float(
        get_first(
            row,
            [
                "fair_probability_yes",
                "aggregate_fair_probability_yes",
                "fair_probability",
                "aggregate_fair_probability",
                "fair_prob",
                "p_yes",
            ],
        )
    )
    if yes_prob is None:
        return None
    return yes_prob if side == "YES" else 1.0 - yes_prob


def read_rows(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if suffix == ".json":
        raw = json.loads(text)
        if isinstance(raw, list):
            return [dict(row) for row in raw]
        if isinstance(raw, dict) and isinstance(raw.get("rows"), list):
            return [dict(row) for row in raw["rows"]]
        raise SystemExit(f"unsupported JSON shape in {path}; expected list or {{rows: [...]}}")
    rows = []
    for lineno, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(dict(json.loads(line)))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid JSONL at {path}:{lineno}: {exc}") from exc
    return rows


def source_missing(row: dict[str, Any]) -> bool:
    trade = get_first(
        row,
        [
            "source_sequence_id",
            "trade_source_sequence_id",
            "source_quality_trade_source_sequence_id",
        ],
    )
    l1 = get_first(
        row,
        [
            "l1_source_sequence_id",
            "book_l1_source_sequence_id",
            "source_quality_l1_source_sequence_id",
        ],
    )
    l2 = get_first(
        row,
        [
            "l2_source_sequence_id",
            "book_l2_source_sequence_id",
            "source_quality_l2_source_sequence_id",
        ],
    )
    return not (trade and l1 and l2)


def score_row(row: dict[str, Any], args: argparse.Namespace, row_index: int) -> dict[str, Any]:
    side = normalize_side(get_first(row, ["side", "outcome", "token_side"], "YES"))
    asset = infer_asset(row)
    timeframe = infer_timeframe(row)
    slug = str(get_first(row, ["market_slug", "slug"], ""))
    seconds_to_close = as_float(get_first(row, ["seconds_to_close", "secs_to_close", "time_to_close_s"]))
    ask = as_float(get_first(row, ["polymarket_best_ask", "best_ask", "ask", "ask_price", "px", "price"]))
    opposite_ask = as_float(
        get_first(row, ["opposite_best_ask", "opposite_polymarket_best_ask", "pair_opposite_ask", "other_side_ask"])
    )
    fair_prob = fair_side_probability(row, side)
    effective_fee_rate = as_float(get_first(row, ["effective_fee_rate", "fee_rate_on_gross", "execution_fee_rate"]))
    quantity = as_float(get_first(row, ["qty", "quantity", "target_qty", "size"]), 1.0) or 1.0
    residual_budget_share = as_float(
        get_first(row, ["residual_budget_share", "projected_residual_share", "residual_share_after"]),
        0.0,
    )

    blockers: list[str] = []
    warnings: list[str] = []
    if asset not in args.assets:
        blockers.append("asset_not_in_scope")
    if timeframe not in args.timeframes:
        blockers.append("timeframe_not_in_scope")
    if seconds_to_close is None:
        blockers.append("seconds_to_close_missing")
    else:
        if seconds_to_close < args.min_seconds_to_close:
            blockers.append("too_close_to_end")
        if seconds_to_close > args.max_seconds_to_close:
            blockers.append("too_far_from_end")
    if ask is None:
        blockers.append("ask_missing")
    else:
        if ask < args.min_price:
            blockers.append("price_below_b55_band")
        if ask > args.max_price:
            blockers.append("price_above_b55_band")
        if not (args.preferred_min_price <= ask <= args.preferred_max_price):
            warnings.append("outside_preferred_50c_80c_band")
    if fair_prob is None:
        blockers.append("fair_probability_missing")
    elif not (0.0 <= fair_prob <= 1.0):
        blockers.append("fair_probability_invalid")
    if effective_fee_rate is not None and effective_fee_rate > args.max_effective_fee_rate:
        blockers.append("effective_fee_rate_above_hard")
    if args.require_source_fields and source_missing(row):
        blockers.append("source_linkage_missing")

    fee_per_share = official_fee_per_share(ask, args.official_fee_rate) if ask is not None else None
    fair_edge_after_fee = (
        fair_prob - ask - fee_per_share
        if fair_prob is not None and ask is not None and fee_per_share is not None
        else None
    )
    if fair_edge_after_fee is None:
        blockers.append("fair_edge_unavailable")
    elif fair_edge_after_fee < args.min_fair_edge:
        blockers.append("fair_edge_after_fee_below_min")

    opposite_fee_per_share = (
        official_fee_per_share(opposite_ask, args.official_fee_rate) if opposite_ask is not None else None
    )
    pair_cost_after_fee = (
        ask + opposite_ask + fee_per_share + opposite_fee_per_share
        if ask is not None and opposite_ask is not None and fee_per_share is not None and opposite_fee_per_share is not None
        else None
    )
    pair_edge_after_fee = 1.0 - pair_cost_after_fee if pair_cost_after_fee is not None else None
    if args.require_pair_cost and pair_cost_after_fee is None:
        blockers.append("pair_cost_unavailable")
    if pair_cost_after_fee is not None:
        if pair_cost_after_fee > args.max_pair_cost:
            blockers.append("pair_cost_after_fee_above_hard")
        elif pair_cost_after_fee > args.target_pair_cost:
            warnings.append("pair_cost_above_b55_target_below_hard")
    if residual_budget_share is not None and residual_budget_share > args.max_residual_budget_share:
        blockers.append("residual_budget_share_above_hard")
    elif residual_budget_share is not None and residual_budget_share > args.target_residual_budget_share:
        warnings.append("residual_budget_share_above_target")

    accepted = not blockers
    return {
        "row_index": row_index,
        "accepted": accepted,
        "blockers": blockers,
        "warnings": warnings,
        "market_slug": slug,
        "asset": asset,
        "timeframe": timeframe,
        "side": side,
        "seconds_to_close": round_opt(seconds_to_close),
        "quantity": round_opt(quantity),
        "polymarket_best_ask": round_opt(ask),
        "opposite_best_ask": round_opt(opposite_ask),
        "fair_side_probability": round_opt(fair_prob),
        "official_fee_per_share": round_opt(fee_per_share),
        "fair_edge_after_fee": round_opt(fair_edge_after_fee),
        "fair_edge_after_fee_pct": pct(fair_edge_after_fee),
        "pair_cost_after_fee": round_opt(pair_cost_after_fee),
        "pair_edge_after_fee": round_opt(pair_edge_after_fee),
        "pair_edge_after_fee_pct": pct(pair_edge_after_fee),
        "residual_budget_share": round_opt(residual_budget_share),
        "effective_fee_rate": round_opt(effective_fee_rate),
        "source_linkage_present": not source_missing(row),
        "raw": row if args.include_raw_rows else None,
    }


def render_markdown(card: dict[str, Any]) -> str:
    metrics = card["metrics"]
    decision = card["decision"]
    lines = [
        "# Xuan B55/Ce25 Fair-Price Admission Scorer",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- admission_ready: `{decision['admission_ready']}`",
        f"- remote_runner_allowed: `{decision['remote_runner_allowed']}`",
        f"- deployable: `{decision['deployable']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers']) or 'none'}`",
        "",
        "## Metrics",
        "",
        f"- rows: `{metrics['rows']}`",
        f"- accepted_rows: `{metrics['accepted_rows']}`",
        f"- accepted_notional: `{metrics['accepted_notional']}`",
        f"- accepted_pair_cost_after_fee_avg: `{metrics['accepted_pair_cost_after_fee_avg']}`",
        f"- accepted_fair_edge_after_fee_avg_pct: `{metrics['accepted_fair_edge_after_fee_avg_pct']}`",
        f"- accepted_pair_edge_after_fee_avg_pct: `{metrics['accepted_pair_edge_after_fee_avg_pct']}`",
        f"- accepted_residual_budget_share_max_pct: `{metrics['accepted_residual_budget_share_max_pct']}`",
        "",
        "## Blockers",
        "",
    ]
    for name, count in card["blocker_counts"].items():
        lines.append(f"- {name}: `{count}`")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Local/read-only scorer only.",
            "- No remote run, live order, deploy, restart, shared-service mutation, localagg mutation, or raw/replay mutation is authorized.",
            "- Public b55/ce25 metrics are benchmarks for shape and gates, not private truth.",
        ]
    )
    return "\n".join(lines) + "\n"


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def build(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = read_rows(Path(args.input).expanduser().resolve())
    scored = [score_row(row, args, idx) for idx, row in enumerate(rows)]
    accepted = [row for row in scored if row["accepted"]]
    blocker_counts = Counter(blocker for row in scored for blocker in row["blockers"])
    warning_counts = Counter(warning for row in scored for warning in row["warnings"])
    accepted_notional = sum(
        (row.get("quantity") or 0.0) * (row.get("polymarket_best_ask") or 0.0) for row in accepted
    )
    accepted_pair_costs = [row["pair_cost_after_fee"] for row in accepted if row["pair_cost_after_fee"] is not None]
    accepted_pair_edges = [row["pair_edge_after_fee"] for row in accepted if row["pair_edge_after_fee"] is not None]
    accepted_fair_edges = [row["fair_edge_after_fee"] for row in accepted if row["fair_edge_after_fee"] is not None]
    accepted_residual_budget = [
        row["residual_budget_share"] for row in accepted if row["residual_budget_share"] is not None
    ]

    hard_blockers: list[str] = []
    if len(accepted) < args.min_accepted_rows:
        hard_blockers.append("accepted_rows_below_min")
    if accepted_notional < args.min_accepted_notional:
        hard_blockers.append("accepted_notional_below_min")
    avg_pair_cost = avg(accepted_pair_costs)
    if avg_pair_cost is not None and avg_pair_cost > args.max_pair_cost:
        hard_blockers.append("accepted_pair_cost_avg_above_hard")
    avg_fair_edge = avg(accepted_fair_edges)
    if avg_fair_edge is not None and avg_fair_edge < args.min_fair_edge:
        hard_blockers.append("accepted_fair_edge_avg_below_min")

    status = (
        "KEEP_B55_CE25_FAIR_PRICE_ADMISSION_PASS_RESEARCH_ONLY"
        if not hard_blockers
        else "UNKNOWN_B55_CE25_FAIR_PRICE_ADMISSION_BLOCKED"
    )
    card = {
        "artifact": "xuan_b55_ce25_fair_price_admission_scorer",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_b55_ce25_fair_price_admission_scorer.py",
        "status": status,
        "inputs": {
            "input": str(Path(args.input).expanduser().resolve()),
            "assets": args.assets,
            "timeframes": args.timeframes,
            "min_seconds_to_close": args.min_seconds_to_close,
            "max_seconds_to_close": args.max_seconds_to_close,
            "min_price": args.min_price,
            "max_price": args.max_price,
            "preferred_min_price": args.preferred_min_price,
            "preferred_max_price": args.preferred_max_price,
            "min_fair_edge": args.min_fair_edge,
            "target_pair_cost": args.target_pair_cost,
            "max_pair_cost": args.max_pair_cost,
            "target_residual_budget_share": args.target_residual_budget_share,
            "max_residual_budget_share": args.max_residual_budget_share,
            "max_effective_fee_rate": args.max_effective_fee_rate,
            "require_pair_cost": args.require_pair_cost,
            "require_source_fields": args.require_source_fields,
            "official_fee_rate": args.official_fee_rate,
        },
        "benchmark_contract": {
            "basis": "public_b55_ce25_corrected_review_20260524",
            "b55_actual_pair_cost": 0.9592,
            "b55_pair_edge": 0.0408,
            "b55_residual_rate": 0.1495,
            "ce25_actual_pair_cost": 0.9742,
            "ce25_residual_rate": 0.0871,
            "target_actual_pair_cost_lte": args.target_pair_cost,
            "review_actual_pair_cost_lte": args.max_pair_cost,
            "residual_target_lte": args.target_residual_budget_share,
            "residual_hard_lte": args.max_residual_budget_share,
        },
        "metrics": {
            "rows": len(scored),
            "accepted_rows": len(accepted),
            "accepted_notional": round_opt(accepted_notional),
            "accepted_pair_cost_after_fee_avg": round_opt(avg_pair_cost),
            "accepted_pair_cost_after_fee_min": round_opt(min(accepted_pair_costs) if accepted_pair_costs else None),
            "accepted_pair_cost_after_fee_max": round_opt(max(accepted_pair_costs) if accepted_pair_costs else None),
            "accepted_fair_edge_after_fee_avg": round_opt(avg_fair_edge),
            "accepted_fair_edge_after_fee_avg_pct": pct(avg_fair_edge),
            "accepted_pair_edge_after_fee_avg": round_opt(avg(accepted_pair_edges)),
            "accepted_pair_edge_after_fee_avg_pct": pct(avg(accepted_pair_edges)),
            "accepted_residual_budget_share_max": round_opt(max(accepted_residual_budget) if accepted_residual_budget else None),
            "accepted_residual_budget_share_max_pct": pct(max(accepted_residual_budget) if accepted_residual_budget else None),
        },
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "warning_counts": dict(sorted(warning_counts.items())),
        "sample_accepted_rows": accepted[: args.sample_rows],
        "sample_blocked_rows": [row for row in scored if not row["accepted"]][: args.sample_rows],
        "decision": {
            "admission_ready": not hard_blockers,
            "hard_blockers": hard_blockers,
            "research_only": True,
            "remote_runner_allowed": False,
            "requires_separate_bounded_no_order_authorization": True,
            "deployable": False,
            "live_orders_allowed": False,
            "shared_service_mutation_allowed": False,
            "localagg_mutation_allowed": False,
            "raw_replay_mutation_allowed": False,
        },
    }
    return card, accepted


def split_csv(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Candidate rows as JSONL, JSON list, or CSV")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--accepted-jsonl")
    parser.add_argument("--markdown")
    parser.add_argument("--assets", type=split_csv, default=["BTC", "ETH"])
    parser.add_argument("--timeframes", type=split_csv, default=["15m", "1h_or_named"])
    parser.add_argument("--min-seconds-to-close", type=float, default=60.0)
    parser.add_argument("--max-seconds-to-close", type=float, default=900.0)
    parser.add_argument("--min-price", type=float, default=0.35)
    parser.add_argument("--max-price", type=float, default=0.90)
    parser.add_argument("--preferred-min-price", type=float, default=0.50)
    parser.add_argument("--preferred-max-price", type=float, default=0.80)
    parser.add_argument("--official-fee-rate", type=float, default=0.07)
    parser.add_argument("--min-fair-edge", type=float, default=0.015)
    parser.add_argument("--target-pair-cost", type=float, default=0.965)
    parser.add_argument("--max-pair-cost", type=float, default=0.975)
    parser.add_argument("--target-residual-budget-share", type=float, default=0.10)
    parser.add_argument("--max-residual-budget-share", type=float, default=0.20)
    parser.add_argument("--max-effective-fee-rate", type=float, default=0.0175)
    parser.add_argument("--min-accepted-rows", type=int, default=1)
    parser.add_argument("--min-accepted-notional", type=float, default=0.0)
    parser.add_argument("--sample-rows", type=int, default=5)
    parser.add_argument("--no-require-pair-cost", dest="require_pair_cost", action="store_false")
    parser.add_argument("--require-source-fields", action="store_true")
    parser.add_argument("--include-raw-rows", action="store_true")
    parser.set_defaults(require_pair_cost=True)
    args = parser.parse_args()

    card, accepted = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.accepted_jsonl:
        accepted_path = Path(args.accepted_jsonl).expanduser().resolve()
        accepted_path.parent.mkdir(parents=True, exist_ok=True)
        with accepted_path.open("w", encoding="utf-8") as handle:
            for row in accepted:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if card["decision"]["admission_ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
