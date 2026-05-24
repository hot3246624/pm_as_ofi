#!/usr/bin/env python3
"""Build ce25-style pair-cost-only admission rows from read-only book snapshots."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from pathlib import Path
from typing import Any


PROFILE_DEFAULTS = {
    "ce25_starter": {
        "assets": {"SOL", "ETH"},
        "timeframes": {"5m", "15m"},
        "pair_cost_cap": 0.90,
        "residual_budget_share": 0.15,
    },
    "ce25_core": {
        "assets": {"BTC", "ETH", "SOL"},
        "timeframes": {"5m", "15m"},
        "pair_cost_cap": 0.95,
        "residual_budget_share": 0.10,
    },
}


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def round_opt(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def fee_per_share(px: float, rate: float) -> float:
    price = min(max(px, 0.0), 1.0)
    return rate * price * (1.0 - price)


def get_first(row: dict[str, Any], names: tuple[str, ...], default: Any = None) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None and value != "":
            return value
    return default


def infer_asset(row: dict[str, Any]) -> str | None:
    raw = get_first(row, ("asset", "symbol", "underlying", "coin"))
    if raw:
        return str(raw).strip().upper()
    slug = str(get_first(row, ("market_slug", "slug"), "")).lower()
    if "btc-" in slug or "bitcoin-" in slug:
        return "BTC"
    if "eth-" in slug or "ethereum-" in slug:
        return "ETH"
    if "sol-" in slug or "solana-" in slug:
        return "SOL"
    if "xrp-" in slug:
        return "XRP"
    return None


def infer_timeframe(row: dict[str, Any]) -> str | None:
    raw = get_first(row, ("timeframe", "tf", "market_timeframe"))
    if raw:
        return str(raw).strip().lower()
    slug = str(get_first(row, ("market_slug", "slug"), "")).lower()
    if "updown-5m-" in slug:
        return "5m"
    if "updown-15m-" in slug:
        return "15m"
    if "up-or-down" in slug:
        return "1h_or_named"
    return None


def market_duration_s(slug: str) -> int | None:
    if "updown-5m-" in slug:
        return 300
    if "updown-15m-" in slug:
        return 900
    return None


def round_start_s(slug: str) -> int | None:
    match = re.search(r"-(\d{10})$", slug)
    return int(match.group(1)) if match else None


def infer_seconds_to_close(row: dict[str, Any]) -> float | None:
    explicit = fnum(get_first(row, ("seconds_to_close", "secs_to_close", "time_to_close_s")))
    if explicit is not None:
        return explicit
    slug = str(get_first(row, ("market_slug", "slug"), ""))
    start = round_start_s(slug)
    duration = market_duration_s(slug)
    ts_ms = fnum(get_first(row, ("ts_ms", "recv_ts_ms", "book_ts_ms")))
    ts_s = fnum(get_first(row, ("ts_s", "timestamp_s")))
    if start is None or duration is None:
        return None
    if ts_s is None and ts_ms is not None:
        ts_s = ts_ms / 1000.0
    if ts_s is None:
        return None
    return max(0.0, start + duration - ts_s)


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
    out = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            out.append(dict(json.loads(line)))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid JSONL at {path}:{lineno}: {exc}") from exc
    return out


def read_slug_rows(raw: str) -> list[dict[str, Any]]:
    rows = []
    for slug in [part.strip() for part in raw.split(",") if part.strip()]:
        rows.append({"market_slug": slug})
    return rows


def build(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    profile = PROFILE_DEFAULTS[args.profile]
    cap = args.pair_cost_cap if args.pair_cost_cap is not None else profile["pair_cost_cap"]
    residual_budget_share = (
        args.residual_budget_share
        if args.residual_budget_share is not None
        else profile["residual_budget_share"]
    )
    input_mode = "book_snapshot"
    if args.market_slugs:
        input_mode = "runtime_slug_universe"
        rows = read_slug_rows(args.market_slugs)
    else:
        rows = read_rows(Path(args.book_snapshots).expanduser().resolve())
    admissions: list[dict[str, Any]] = []
    blockers: dict[str, int] = {}

    def block(name: str) -> None:
        blockers[name] = blockers.get(name, 0) + 1

    for idx, row in enumerate(rows):
        slug = str(get_first(row, ("market_slug", "slug"), ""))
        asset = infer_asset(row)
        timeframe = infer_timeframe(row)
        yes_ask = fnum(get_first(row, ("yes_ask", "best_yes_ask", "up_ask")))
        no_ask = fnum(get_first(row, ("no_ask", "best_no_ask", "down_ask")))
        seconds_to_close = infer_seconds_to_close(row)
        if not slug:
            block("missing_slug")
            continue
        if asset not in profile["assets"]:
            block("asset_not_in_profile")
            continue
        if timeframe not in profile["timeframes"]:
            block("timeframe_not_in_profile")
            continue
        if seconds_to_close is None and input_mode != "runtime_slug_universe":
            block("seconds_to_close_missing")
            continue
        if seconds_to_close is not None and seconds_to_close < args.min_seconds_to_close:
            block("too_close_to_end")
            continue
        if seconds_to_close is not None and seconds_to_close > args.max_seconds_to_close:
            block("too_far_from_end")
            continue
        if (
            (yes_ask is None or no_ask is None or yes_ask <= 0 or no_ask <= 0)
            and input_mode != "runtime_slug_universe"
        ):
            block("missing_or_invalid_asks")
            continue
        pair_cost = (
            yes_ask + no_ask + fee_per_share(yes_ask, args.official_fee_rate) + fee_per_share(no_ask, args.official_fee_rate)
            if yes_ask is not None and no_ask is not None and yes_ask > 0 and no_ask > 0
            else None
        )
        if pair_cost is not None and pair_cost > cap + 1e-12:
            block("pair_cost_above_cap")
            continue
        common = {
            "row_id": f"{args.profile}:{slug}:{idx}",
            "market_slug": slug,
            "asset": asset,
            "timeframe": timeframe,
            "admission_mode": "pair_cost_only",
            "pair_cost_only": True,
            "pair_cost_after_fee_snapshot": round(pair_cost, 12) if pair_cost is not None else None,
            "yes_ask_snapshot": yes_ask,
            "no_ask_snapshot": no_ask,
            "min_seconds_to_close": args.min_seconds_to_close,
            "max_seconds_to_close": args.max_seconds_to_close,
            "seconds_to_close_snapshot": round(seconds_to_close, 6) if seconds_to_close is not None else None,
            "residual_budget_share": residual_budget_share,
            "source_sequence_id": get_first(row, ("source_sequence_id", "book_l1_source_sequence_id", "l1_source_sequence_id")),
            "l1_source_sequence_id": get_first(row, ("l1_source_sequence_id", "book_l1_source_sequence_id", "source_sequence_id")),
            "l2_source_sequence_id": get_first(row, ("l2_source_sequence_id", "book_l2_source_sequence_id")),
        }
        admissions.append({**common, "row_id": f"{common['row_id']}:YES", "side": "YES"})
        admissions.append({**common, "row_id": f"{common['row_id']}:NO", "side": "NO"})

    scorecard = {
        "artifact": "xuan_ce25_pair_cost_admission_builder",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_ce25_pair_cost_admission_builder.py",
            "status": "KEEP_CE25_PAIR_COST_ADMISSION_BUILDER_PASS_RESEARCH_ONLY" if admissions else "UNKNOWN_CE25_PAIR_COST_ADMISSION_BUILDER_NO_ADMISSIONS",
            "inputs": {
            "input_mode": input_mode,
            "book_snapshots": str(Path(args.book_snapshots).expanduser().resolve()) if args.book_snapshots else None,
            "market_slugs_count": len(rows) if args.market_slugs else 0,
            "profile": args.profile,
            "pair_cost_cap": cap,
            "residual_budget_share": residual_budget_share,
            "min_seconds_to_close": args.min_seconds_to_close,
            "max_seconds_to_close": args.max_seconds_to_close,
            "official_fee_rate": args.official_fee_rate,
        },
        "metrics": {
            "snapshot_rows": len(rows),
            "admission_rows": len(admissions),
            "admission_markets": len({row["market_slug"] for row in admissions}),
            "blocker_counts": dict(sorted(blockers.items())),
            "pair_cost_min": round_opt(min((row["pair_cost_after_fee_snapshot"] for row in admissions if row["pair_cost_after_fee_snapshot"] is not None), default=math.nan)),
            "pair_cost_max": round_opt(max((row["pair_cost_after_fee_snapshot"] for row in admissions if row["pair_cost_after_fee_snapshot"] is not None), default=math.nan)),
        },
        "sample_admissions": admissions[: args.sample_rows],
        "decision": {
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
    return scorecard, admissions


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--book-snapshots")
    parser.add_argument("--market-slugs", help="Comma-separated exact slugs. Emits runtime pair-cost-only rows without requiring snapshot asks.")
    parser.add_argument("--admission-jsonl", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--profile", choices=sorted(PROFILE_DEFAULTS), default="ce25_core")
    parser.add_argument("--pair-cost-cap", type=float, default=None)
    parser.add_argument("--residual-budget-share", type=float, default=None)
    parser.add_argument("--min-seconds-to-close", type=float, default=60.0)
    parser.add_argument("--max-seconds-to-close", type=float, default=900.0)
    parser.add_argument("--official-fee-rate", type=float, default=0.07)
    parser.add_argument("--sample-rows", type=int, default=6)
    args = parser.parse_args()
    if bool(args.book_snapshots) == bool(args.market_slugs):
        raise SystemExit("provide exactly one of --book-snapshots or --market-slugs")

    scorecard, admissions = build(args)
    admission_path = Path(args.admission_jsonl).expanduser().resolve()
    admission_path.parent.mkdir(parents=True, exist_ok=True)
    with admission_path.open("w", encoding="utf-8") as handle:
        for row in admissions:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0 if admissions else 2


if __name__ == "__main__":
    raise SystemExit(main())
