import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "resolve_market_ids.py"
    spec = importlib.util.spec_from_file_location("resolve_market_ids", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


resolver = load_module()


class FakeResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def sample_payload(slug="btc-updown-5m-1"):
    return [
        {
            "conditionId": f"0x{slug.rsplit('-', 1)[-1]}",
            "clobTokenIds": json.dumps(["111", "222"]),
            "outcomes": json.dumps(["Up", "Down"]),
            "endDate": "2026-05-02T00:05:00Z",
        }
    ]


class ResolveMarketIdsTests(unittest.TestCase):
    def test_resolve_retries_then_writes_cache(self):
        with tempfile.TemporaryDirectory(prefix="pm_as_ofi_resolver_") as tmp:
            cache = Path(tmp) / "cache.json"
            calls = []
            old_urlopen = resolver.urlopen

            def fake_urlopen(req, timeout):
                calls.append((req.full_url, timeout))
                if len(calls) <= 2:
                    raise resolver.URLError("EOF occurred in violation of protocol")
                return FakeResponse(sample_payload())

            resolver.urlopen = fake_urlopen
            try:
                market = resolver.resolve_market(
                    "btc-updown-5m-1",
                    cache,
                    retries=2,
                    timeout_sec=1.0,
                    backoff_ms=0,
                    prefer_cache=False,
                    quiet=True,
                )
            finally:
                resolver.urlopen = old_urlopen

            self.assertEqual(market.market_id, "0x1")
            self.assertEqual(len(calls), 3)
            cached = resolver.cache_get(cache, "btc-updown-5m-1")
            self.assertIsNotNone(cached)
            self.assertEqual(cached.yes_asset_id, "111")

    def test_cache_hit_avoids_gamma(self):
        with tempfile.TemporaryDirectory(prefix="pm_as_ofi_resolver_cache_") as tmp:
            cache = Path(tmp) / "cache.json"
            resolver.cache_put(
                cache,
                resolver.ResolvedMarket(
                    slug="btc-updown-5m-1",
                    market_id="0xabc",
                    yes_asset_id="yes",
                    no_asset_id="no",
                    end_date=None,
                    outcomes=[],
                ),
            )
            old_urlopen = resolver.urlopen

            def fake_urlopen(req, timeout):
                raise AssertionError("urlopen should not be called on cache hit")

            resolver.urlopen = fake_urlopen
            try:
                market = resolver.resolve_market(
                    "btc-updown-5m-1",
                    cache,
                    retries=1,
                    timeout_sec=1.0,
                    backoff_ms=0,
                    prefer_cache=True,
                    quiet=True,
                )
            finally:
                resolver.urlopen = old_urlopen

            self.assertEqual(market.market_id, "0xabc")
            self.assertEqual(market.no_asset_id, "no")

    def test_prefetch_continues_after_failed_slug(self):
        with tempfile.TemporaryDirectory(prefix="pm_as_ofi_resolver_prefetch_") as tmp:
            cache = Path(tmp) / "cache.json"
            old_urlopen = resolver.urlopen
            old_time = resolver.time.time
            calls = []

            def fake_time():
                return 1_000

            def fake_urlopen(req, timeout):
                calls.append(req.full_url)
                if "btc-updown-5m-900" in req.full_url:
                    raise resolver.URLError("temporary eof")
                return FakeResponse(sample_payload(req.full_url))

            resolver.time.time = fake_time
            resolver.urlopen = fake_urlopen
            try:
                ok = resolver.prefetch_markets(
                    prefix="btc-updown-5m",
                    start_offset=0,
                    rounds=2,
                    cache_path=cache,
                    retries=1,
                    timeout_sec=1.0,
                    backoff_ms=0,
                )
            finally:
                resolver.urlopen = old_urlopen
                resolver.time.time = old_time

            self.assertEqual(ok, 1)
            self.assertIsNone(resolver.cache_get(cache, "btc-updown-5m-900"))
            self.assertIsNotNone(resolver.cache_get(cache, "btc-updown-5m-1200"))
            self.assertEqual(len(calls), 3)


if __name__ == "__main__":
    unittest.main()
