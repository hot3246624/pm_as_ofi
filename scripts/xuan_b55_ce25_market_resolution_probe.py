#!/usr/bin/env python3
"""Resolve b55/ce25-inspired BTC/ETH 15m and named hourly markets.

This is a local, read-only planning probe. It deliberately writes to a
temporary resolver cache instead of the shared resolver cache, and it does not
authorize remote execution.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
MONTH_NAMES = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]
ASSETS = {
    "BTC": {
        "prefix_15m": "btc-updown-15m",
        "named_hour_asset": "bitcoin",
    },
    "ETH": {
        "prefix_15m": "eth-updown-15m",
        "named_hour_asset": "ethereum",
    },
}


def load_resolver():
    module_path = ROOT / "scripts" / "resolve_market_ids.py"
    spec = importlib.util.spec_from_file_location("xuan_resolve_market_ids", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load resolver from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def split_csv(raw: str) -> list[str]:
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


def round_opt(value: Any, digits: int = 6) -> Any:
    if isinstance(value, float):
        return None if not math.isfinite(value) else round(value, digits)
    if isinstance(value, list):
        return [round_opt(item, digits) for item in value]
    if isinstance(value, dict):
        return {key: round_opt(val, digits) for key, val in value.items()}
    return value


def resolve_slug(resolver: Any, slug: str, cache_path: Path, args: argparse.Namespace) -> dict[str, Any]:
    started = time.time()
    try:
        market = resolver.resolve_market(
            slug,
            cache_path,
            retries=args.retries,
            timeout_sec=args.timeout_sec,
            backoff_ms=args.backoff_ms,
            prefer_cache=True,
            quiet=True,
        )
        return {
            "slug": slug,
            "resolved": True,
            "market_id": market.market_id,
            "yes_asset_id": market.yes_asset_id,
            "no_asset_id": market.no_asset_id,
            "end_date": market.end_date,
            "outcomes": market.outcomes,
            "runtime_s": round(time.time() - started, 3),
        }
    except Exception as exc:
        return {
            "slug": slug,
            "resolved": False,
            "error": str(exc),
            "runtime_s": round(time.time() - started, 3),
        }


def epoch_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fifteen_min_slugs(asset: str, start_offset: int, rounds: int, now_ts: int) -> list[dict[str, Any]]:
    prefix = ASSETS[asset]["prefix_15m"]
    base = (now_ts // 900) * 900
    rows = []
    for offset in range(start_offset, start_offset + rounds):
        ts = base + 900 * offset
        rows.append(
            {
                "asset": asset,
                "timeframe": "15m",
                "offset": offset,
                "start_ts": ts,
                "start_utc": epoch_to_iso(ts),
                "slug": f"{prefix}-{ts}",
            }
        )
    return rows


def named_hour_candidates(asset: str, start_offset: int, rounds: int, now_ts: int) -> list[dict[str, Any]]:
    asset_name = ASSETS[asset]["named_hour_asset"]
    base = (now_ts // 3600) * 3600
    et = ZoneInfo("America/New_York")
    rows = []
    for offset in range(start_offset, start_offset + rounds):
        ts = base + 3600 * offset
        dt_et = datetime.fromtimestamp(ts, timezone.utc).astimezone(et)
        month = MONTH_NAMES[dt_et.month - 1]
        hour = dt_et.hour % 12 or 12
        ampm = "am" if dt_et.hour < 12 else "pm"
        with_year = f"{asset_name}-up-or-down-{month}-{dt_et.day}-{dt_et.year}-{hour}{ampm}-et"
        no_year = f"{asset_name}-up-or-down-{month}-{dt_et.day}-{hour}{ampm}-et"
        rows.append(
            {
                "asset": asset,
                "timeframe": "1h_or_named",
                "offset": offset,
                "start_ts": ts,
                "start_utc": epoch_to_iso(ts),
                "start_et": dt_et.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "candidate_slugs": [with_year, no_year],
            }
        )
    return rows


def resolve_named_group(resolver: Any, group: dict[str, Any], cache_path: Path, args: argparse.Namespace) -> dict[str, Any]:
    attempts = []
    for slug in group["candidate_slugs"]:
        resolved = resolve_slug(resolver, slug, cache_path, args)
        attempts.append(resolved)
        if resolved["resolved"]:
            return {**group, "resolved": True, "selected": resolved, "attempts": attempts}
    return {**group, "resolved": False, "selected": None, "attempts": attempts}


def render_markdown(card: dict[str, Any]) -> str:
    summary = card["summary"]
    lines = [
        "# Xuan B55/Ce25 Market Resolution Probe",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- remote_runner_allowed: `{card['decision']['remote_runner_allowed']}`",
        f"- deployable: `{card['decision']['deployable']}`",
        f"- hard_blockers: `{', '.join(card['decision']['hard_blockers']) or 'none'}`",
        "",
        "## Summary",
        "",
        f"- btc_eth_15m_resolved: `{summary['resolved_15m']}/{summary['total_15m']}`",
        f"- btc_eth_named_hour_resolved: `{summary['resolved_named_hour']}/{summary['total_named_hour']}`",
        f"- runner_market_slugs_csv: `{summary['runner_market_slugs_csv']}`",
        "",
        "## Resolved 15m Markets",
        "",
    ]
    for row in card["resolved_15m_markets"]:
        lines.append(f"- `{row['slug']}` asset=`{row['asset']}` start_utc=`{row['start_utc']}` end_date=`{row.get('end_date')}`")
    lines.extend(["", "## Named Hour Discovery", ""])
    for row in card["named_hour_markets"]:
        selected = row.get("selected")
        if selected:
            lines.append(f"- `{selected['slug']}` asset=`{row['asset']}` start_et=`{row['start_et']}` resolved=`true`")
        else:
            lines.append(
                f"- asset=`{row['asset']}` start_et=`{row['start_et']}` resolved=`false` candidates=`{', '.join(row['candidate_slugs'])}`"
            )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Local/read-only only; no remote run is started.",
            "- Uses a `.tmp_xuan` resolver cache, not the default shared resolver cache.",
            "- Exact 15m slugs can be passed to the runner with `--market-slugs` after a separate bounded no-order authorization.",
            "- Named hourly markets remain discovery-only unless the exact slug resolves.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    started = time.time()
    resolver = load_resolver()
    cache_path = Path(args.cache_path).expanduser().resolve()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    assets = split_csv(args.assets)
    unknown_assets = [asset for asset in assets if asset not in ASSETS]
    if unknown_assets:
        raise SystemExit(f"unsupported assets: {','.join(unknown_assets)}")
    now_ts = int(args.now_ts if args.now_ts is not None else time.time())

    fifteen_raw = []
    for asset in assets:
        fifteen_raw.extend(fifteen_min_slugs(asset, args.start_offset, args.rounds_15m, now_ts))
    resolved_15m = []
    for row in fifteen_raw:
        resolved = resolve_slug(resolver, row["slug"], cache_path, args)
        resolved_15m.append({**row, **resolved})

    named_groups = []
    for asset in assets:
        named_groups.extend(named_hour_candidates(asset, args.hour_start_offset, args.rounds_named_hour, now_ts))
    named_resolved = [resolve_named_group(resolver, row, cache_path, args) for row in named_groups]

    ok_15m = [row for row in resolved_15m if row.get("resolved")]
    ok_named = [row for row in named_resolved if row.get("resolved")]
    hard_blockers = []
    min_required_15m = len(assets) * max(1, args.min_rounds_15m)
    if len(ok_15m) < min_required_15m:
        hard_blockers.append("insufficient_15m_markets_resolved")
    status = (
        "KEEP_B55_CE25_MARKET_RESOLUTION_PROBE_READY_LOCAL_ONLY"
        if not hard_blockers and ok_named
        else "KEEP_B55_CE25_MARKET_RESOLUTION_PROBE_15M_READY_NAMED_HOUR_UNRESOLVED_LOCAL_ONLY"
        if not hard_blockers
        else "UNKNOWN_B55_CE25_MARKET_RESOLUTION_PROBE_BLOCKED"
    )
    runner_slugs = [row["slug"] for row in ok_15m]
    card = {
        "artifact": "xuan_b55_ce25_market_resolution_probe",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_b55_ce25_market_resolution_probe.py",
        "status": status,
        "inputs": {
            "assets": assets,
            "rounds_15m": args.rounds_15m,
            "rounds_named_hour": args.rounds_named_hour,
            "start_offset": args.start_offset,
            "hour_start_offset": args.hour_start_offset,
            "now_ts": now_ts,
            "now_utc": epoch_to_iso(now_ts),
            "cache_path": str(cache_path),
        },
        "summary": {
            "total_15m": len(resolved_15m),
            "resolved_15m": len(ok_15m),
            "total_named_hour": len(named_resolved),
            "resolved_named_hour": len(ok_named),
            "runner_market_slugs_csv": ",".join(runner_slugs),
        },
        "resolved_15m_markets": resolved_15m,
        "named_hour_markets": named_resolved,
        "decision": {
            "local_probe_ready": not hard_blockers,
            "hard_blockers": hard_blockers,
            "research_only": True,
            "remote_runner_allowed": False,
            "requires_explicit_bounded_no_order_authorization": True,
            "deployable": False,
            "orders_allowed": False,
            "shared_service_mutation_allowed": False,
            "localagg_mutation_allowed": False,
            "raw_replay_mutation_allowed": False,
        },
    }
    return round_opt(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--assets", default="BTC,ETH")
    parser.add_argument("--rounds-15m", type=int, default=4)
    parser.add_argument("--min-rounds-15m", type=int, default=2)
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--rounds-named-hour", type=int, default=2)
    parser.add_argument("--hour-start-offset", type=int, default=0)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--cache-path", default=".tmp_xuan/market_resolver_cache/b55_ce25_probe.json")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--timeout-sec", type=float, default=4.0)
    parser.add_argument("--backoff-ms", type=int, default=250)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    if args.rounds_15m <= 0:
        raise SystemExit("--rounds-15m must be positive")
    if args.min_rounds_15m <= 0:
        raise SystemExit("--min-rounds-15m must be positive")
    if args.rounds_named_hour < 0:
        raise SystemExit("--rounds-named-hour must be non-negative")

    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path = Path(args.markdown).expanduser().resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(card) + "\n", encoding="utf-8")
    card["markdown_path"] = str(markdown_path)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0 if not card["decision"]["hard_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
