#!/usr/bin/env python3
"""Resolve Polymarket market ids / clob token ids for an exact slug or next round.

Examples:
  python3 scripts/resolve_market_ids.py --slug btc-updown-5m-1777280100
  python3 scripts/resolve_market_ids.py --prefix btc-updown-5m --round-offset 1 --format env
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import fcntl
except ImportError:  # pragma: no cover - this script runs on POSIX in prod.
    fcntl = None


GAMMA_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CACHE_SCHEMA = 1
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_PATH = ROOT / "run" / "market_resolver_cache.json"


class ResolveError(RuntimeError):
    pass


@dataclass
class ResolvedMarket:
    slug: str
    market_id: str
    yes_asset_id: str
    no_asset_id: str
    end_date: str | None
    outcomes: list[str]


def env_int(name: str, default: int, min_value: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(min_value, value)


def env_float(name: str, default: float, min_value: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(min_value, value)


def slug_from_prefix(prefix: str, round_offset: int) -> str:
    now = int(time.time())
    base = (now // 300) * 300
    target = base + (300 * round_offset)
    return f"{prefix}-{target}"


def parse_market(payload: dict[str, Any], slug: str) -> ResolvedMarket:
    try:
        market_id = payload.get("conditionId") or payload["condition_id"]
    except KeyError as exc:
        raise ResolveError(f"invalid market payload for slug={slug}: missing {exc.args[0]}") from exc
    raw_ids = payload.get("clobTokenIds") or payload.get("clob_token_ids")
    ids: list[Any] | None = None
    if raw_ids is not None:
        try:
            ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        except json.JSONDecodeError as exc:
            raise ResolveError(f"invalid clobTokenIds json for slug={slug}") from exc
    outcomes_raw = payload.get("outcomes", [])
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
    except json.JSONDecodeError:
        outcomes = []
    if not isinstance(ids, list) or len(ids) < 2:
        ids = token_ids_from_tokens(payload)
    if not isinstance(ids, list) or len(ids) < 2:
        raise ResolveError(f"invalid clobTokenIds/tokens for slug={slug}")
    return ResolvedMarket(
        slug=slug,
        market_id=str(market_id),
        yes_asset_id=str(ids[0]),
        no_asset_id=str(ids[1]),
        end_date=payload.get("endDate"),
        outcomes=[str(x) for x in outcomes] if isinstance(outcomes, list) else [],
    )


def token_ids_from_tokens(payload: dict[str, Any]) -> list[str] | None:
    tokens = payload.get("tokens")
    if not isinstance(tokens, list):
        return None

    def token_id(token: dict[str, Any]) -> str:
        return str(token.get("token_id") or token.get("tokenId") or "")

    def outcome(token: dict[str, Any]) -> str:
        return str(token.get("outcome") or token.get("name") or "").strip().lower()

    yes_token = None
    no_token = None
    for token in tokens:
        if not isinstance(token, dict):
            continue
        out = outcome(token)
        if out in {"yes", "up"} and yes_token is None:
            yes_token = token
        elif out in {"no", "down"} and no_token is None:
            no_token = token
    if yes_token is None or no_token is None:
        return None
    yes_id = token_id(yes_token)
    no_id = token_id(no_token)
    if not yes_id or not no_id:
        return None
    return [yes_id, no_id]


def cache_path_from_args(raw: str | None) -> Path:
    if raw:
        return Path(raw).expanduser()
    return Path(os.environ.get("PM_MARKET_RESOLVER_CACHE", DEFAULT_CACHE_PATH)).expanduser()


def load_cache(path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        return {}
    if payload.get("schema") != CACHE_SCHEMA:
        return {}
    markets = payload.get("markets")
    return markets if isinstance(markets, dict) else {}


def save_cache(path: Path, markets: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": CACHE_SCHEMA,
        "updated_at": int(time.time()),
        "markets": markets,
    }
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def market_from_cache_record(slug: str, record: dict[str, Any]) -> ResolvedMarket | None:
    try:
        return ResolvedMarket(
            slug=str(record.get("slug") or slug),
            market_id=str(record["market_id"]),
            yes_asset_id=str(record["yes_asset_id"]),
            no_asset_id=str(record["no_asset_id"]),
            end_date=record.get("end_date"),
            outcomes=[str(x) for x in record.get("outcomes", [])],
        )
    except (KeyError, TypeError, ValueError):
        return None


def cache_get(path: Path, slug: str) -> ResolvedMarket | None:
    record = load_cache(path).get(slug)
    if not isinstance(record, dict):
        return None
    return market_from_cache_record(slug, record)


def cache_put(path: Path, market: ResolvedMarket) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    if fcntl is None:
        markets = load_cache(path)
        record = asdict(market)
        record["cached_at"] = int(time.time())
        markets[market.slug] = record
        save_cache(path, markets)
        return
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            markets = load_cache(path)
            record = asdict(market)
            record["cached_at"] = int(time.time())
            markets[market.slug] = record
            save_cache(path, markets)
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def fetch_json(url: str, slug: str, source: str, timeout_sec: float) -> Any:
    req = Request(url, headers={"User-Agent": "pm-as-ofi/resolve-market-ids"})
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            status = getattr(resp, "status", 200)
            if status >= 400:
                raise ResolveError(f"gamma {source} request failed for slug={slug} status={status}")
            return json.load(resp)
    except HTTPError as exc:
        raise ResolveError(f"gamma {source} request failed for slug={slug} status={exc.code}") from exc
    except URLError as exc:
        raise ResolveError(f"gamma {source} request failed for slug={slug}: {exc.reason}") from exc
    except OSError as exc:
        raise ResolveError(f"gamma {source} request failed for slug={slug}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ResolveError(f"gamma {source} request failed for slug={slug}: invalid json") from exc


def fetch_market_from_markets(slug: str, timeout_sec: float) -> ResolvedMarket:
    query = urlencode({"slug": slug, "limit": 1})
    arr = fetch_json(f"{GAMMA_URL}?{query}", slug, "/markets", timeout_sec)
    if not isinstance(arr, list) or not arr:
        raise ResolveError(f"no /markets market found for slug={slug}")
    return parse_market(arr[0], slug)


def fetch_market_from_events(slug: str, timeout_sec: float) -> ResolvedMarket:
    query = urlencode({"slug": slug})
    events = fetch_json(f"{GAMMA_EVENTS_URL}?{query}", slug, "/events", timeout_sec)
    if not isinstance(events, list):
        raise ResolveError(f"gamma /events response is not array for slug={slug}")
    parse_errors: list[str] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        markets = event.get("markets")
        if not isinstance(markets, list):
            continue
        exact = [
            market
            for market in markets
            if isinstance(market, dict) and str(market.get("slug") or "") == slug
        ]
        candidates = exact or [market for market in markets if isinstance(market, dict)]
        for market in candidates:
            try:
                return parse_market(market, slug)
            except ResolveError as exc:
                parse_errors.append(str(exc))
                continue
    suffix = f"; parse_errors={parse_errors[:2]}" if parse_errors else ""
    raise ResolveError(f"no /events market found for slug={slug}{suffix}")


def fetch_market(slug: str, timeout_sec: float) -> ResolvedMarket:
    markets_error: ResolveError | None = None
    try:
        return fetch_market_from_markets(slug, timeout_sec)
    except ResolveError as exc:
        markets_error = exc
    try:
        return fetch_market_from_events(slug, timeout_sec)
    except ResolveError as events_error:
        raise ResolveError(f"{markets_error}; {events_error}") from events_error


def resolve_market(
    slug: str,
    cache_path: Path,
    *,
    retries: int,
    timeout_sec: float,
    backoff_ms: int,
    prefer_cache: bool = True,
    quiet: bool = False,
) -> ResolvedMarket:
    if prefer_cache:
        cached = cache_get(cache_path, slug)
        if cached is not None:
            if not quiet:
                print(f"market_resolve_cache_hit slug={slug}", file=sys.stderr)
            return cached

    attempts = max(1, retries)
    last_error: ResolveError | None = None
    for attempt in range(1, attempts + 1):
        try:
            market = fetch_market(slug, timeout_sec)
            cache_put(cache_path, market)
            return market
        except ResolveError as exc:
            last_error = exc
            cached = cache_get(cache_path, slug)
            if cached is not None:
                if not quiet:
                    print(
                        f"market_resolve_cache_fallback slug={slug} attempt={attempt} err={exc}",
                        file=sys.stderr,
                    )
                return cached
            if attempt < attempts:
                sleep_ms = min(backoff_ms * (2 ** (attempt - 1)), 2_000)
                if not quiet:
                    print(
                        f"market_resolve_retry slug={slug} attempt={attempt}/{attempts} "
                        f"sleep_ms={sleep_ms} err={exc}",
                        file=sys.stderr,
                    )
                time.sleep(sleep_ms / 1000.0)

    assert last_error is not None
    raise last_error


def prefetch_markets(
    *,
    prefix: str,
    start_offset: int,
    rounds: int,
    cache_path: Path,
    retries: int,
    timeout_sec: float,
    backoff_ms: int,
) -> int:
    ok = 0
    for offset in range(start_offset, start_offset + max(0, rounds)):
        slug = slug_from_prefix(prefix, offset)
        if cache_get(cache_path, slug) is not None:
            ok += 1
            continue
        try:
            resolve_market(
                slug,
                cache_path,
                retries=retries,
                timeout_sec=timeout_sec,
                backoff_ms=backoff_ms,
                prefer_cache=True,
                quiet=True,
            )
            ok += 1
            print(f"market_resolve_prefetch_ok slug={slug} offset={offset}", file=sys.stderr)
        except ResolveError as exc:
            print(
                f"market_resolve_prefetch_failed slug={slug} offset={offset} err={exc}",
                file=sys.stderr,
            )
    return ok


def print_market(market: ResolvedMarket, fmt: str) -> None:
    if fmt == "env":
        print(f"export POLYMARKET_MARKET_SLUG={market.slug}")
        print(f"export POLYMARKET_MARKET_ID={market.market_id}")
        print(f"export POLYMARKET_YES_ASSET_ID={market.yes_asset_id}")
        print(f"export POLYMARKET_NO_ASSET_ID={market.no_asset_id}")
    else:
        print(
            json.dumps(
                {
                    "slug": market.slug,
                    "market_id": market.market_id,
                    "yes_asset_id": market.yes_asset_id,
                    "no_asset_id": market.no_asset_id,
                    "endDate": market.end_date,
                    "outcomes": market.outcomes,
                },
                ensure_ascii=False,
            )
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--slug")
    g.add_argument("--prefix")
    ap.add_argument("--round-offset", type=int, default=1, help="1=next 5m round, 0=current")
    ap.add_argument("--format", choices=("json", "env"), default="json")
    ap.add_argument("--cache-path")
    ap.add_argument("--retries", type=int, default=env_int("PM_MARKET_RESOLVE_RETRIES", 6, 1))
    ap.add_argument(
        "--timeout-sec",
        type=float,
        default=env_float("PM_MARKET_RESOLVE_TIMEOUT_SEC", 4.0, 0.1),
    )
    ap.add_argument(
        "--backoff-ms",
        type=int,
        default=env_int("PM_MARKET_RESOLVE_BACKOFF_MS", 250, 0),
    )
    ap.add_argument(
        "--prefetch-only",
        action="store_true",
        help="Resolve/cache a prefix range without printing env/json.",
    )
    ap.add_argument(
        "--prefetch-rounds",
        type=int,
        default=env_int("PM_MARKET_RESOLVE_PREFETCH_ROUNDS", 0, 0),
    )
    ns = ap.parse_args()

    cache_path = cache_path_from_args(ns.cache_path)
    if ns.prefetch_only:
        if not ns.prefix:
            raise SystemExit("--prefetch-only requires --prefix")
        prefetch_markets(
            prefix=ns.prefix,
            start_offset=ns.round_offset,
            rounds=ns.prefetch_rounds,
            cache_path=cache_path,
            retries=ns.retries,
            timeout_sec=ns.timeout_sec,
            backoff_ms=ns.backoff_ms,
        )
        return 0

    slug = ns.slug or slug_from_prefix(ns.prefix, ns.round_offset)
    try:
        market = resolve_market(
            slug,
            cache_path,
            retries=ns.retries,
            timeout_sec=ns.timeout_sec,
            backoff_ms=ns.backoff_ms,
            prefer_cache=True,
        )
    except ResolveError as exc:
        raise SystemExit(str(exc)) from exc
    print_market(market, ns.format)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
