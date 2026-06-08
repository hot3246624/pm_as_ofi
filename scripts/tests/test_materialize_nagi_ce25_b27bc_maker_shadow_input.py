import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    scripts_dir = Path(__file__).resolve().parents[1]
    module_path = scripts_dir / "materialize_nagi_ce25_b27bc_maker_shadow_input.py"
    spec = importlib.util.spec_from_file_location(
        "materialize_nagi_ce25_b27bc_maker_shadow_input", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


materializer_mod = load_module()


class NagiCe25B27bcMakerShadowMaterializerTests(unittest.TestCase):
    def test_materializes_btc5m_public_trade_event(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_") as tmp:
            path = Path(tmp) / "btc-updown-5m-1900000000.events.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "kind": "candidate",
                        "source": "no_order_public_trade_candidate",
                        "slug": "btc-updown-5m-1900000000",
                        "condition_id": "cond",
                        "ts_ms": 1900000250000,
                        "offset_s": 250,
                        "side": "YES",
                        "price": 0.42,
                        "no_bid": 0.56,
                        "public_trade_px": 0.43,
                        "public_trade_size": 20,
                        "source_sequence_id": "seq-1",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            rows, summary = materializer_mod.materialize_file(path, max_rows_per_file=100)
            self.assertEqual(summary["materialized_rows"], 1)
            self.assertEqual(rows[0]["slug"], "btc-updown-5m-1900000000")
            self.assertEqual(rows[0]["remaining_s"], 50)
            self.assertEqual(rows[0]["yes_bid"], 0.42)
            self.assertEqual(rows[0]["public_trade_qty"], 20.0)
            self.assertEqual(rows[0]["visible_depth_qty"], 20.0)
            self.assertEqual(rows[0]["materialized_depth_source"], "public_trade_size_proxy_not_l2_depth")

    def test_default_path_filter_excludes_smoke_and_fixture(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_filter_") as tmp:
            root = Path(tmp)
            smoke = root / "some_smoke_dir"
            real = root / "real_dir"
            smoke.mkdir()
            real.mkdir()
            (smoke / "a.events.jsonl").write_text("{}\n", encoding="utf-8")
            (real / "b.events.jsonl").write_text("{}\n", encoding="utf-8")
            paths = materializer_mod.iter_event_paths([root], include_smoke=False, max_files=10)
            self.assertEqual(paths, [real / "b.events.jsonl"])
            paths = materializer_mod.iter_event_paths([root], include_smoke=True, max_files=10)
            self.assertEqual(paths, [real / "b.events.jsonl", smoke / "a.events.jsonl"])

    def test_materializes_btc5m_public_activity_sell_touch(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_activity_") as tmp:
            path = Path(tmp) / "real_public_activity_entry_rows.json"
            path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "asset": "BTC",
                                "timeframe": "5m",
                                "market_slug": "btc-updown-5m-1900000000",
                                "condition_id": "cond",
                                "quote_ts": 1900000250,
                                "time_to_expiry_s": 45,
                                "outcome": "YES",
                                "source_side": "SELL",
                                "source_type": "public_profile",
                                "trade_id": "trade-1",
                                "polymarket_price": 0.42,
                                "size": 12,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            rows, summary = materializer_mod.materialize_public_activity_file(
                path,
                max_rows_per_file=100,
            )
            self.assertEqual(summary["source_format"], "public_activity_entry_rows")
            self.assertEqual(summary["materialized_rows"], 1)
            self.assertEqual(rows[0]["slug"], "btc-updown-5m-1900000000")
            self.assertEqual(rows[0]["remaining_s"], 45.0)
            self.assertEqual(rows[0]["public_taker_side"], "SELL")
            self.assertEqual(rows[0]["yes_bid"], 0.42)
            self.assertEqual(rows[0]["public_trade_qty"], 12.0)
            self.assertEqual(rows[0]["visible_depth_qty"], 12.0)
            self.assertEqual(
                rows[0]["materialized_depth_source"],
                "public_activity_sell_size_proxy_not_l2_depth",
            )
            self.assertEqual(rows[0]["maker_truth"], "public_activity_queue_proxy_only")

    def test_public_activity_buy_rows_are_not_queue_touch(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_activity_buy_") as tmp:
            path = Path(tmp) / "real_public_activity_entry_rows.json"
            path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "asset": "BTC",
                                "timeframe": "5m",
                                "market_slug": "btc-updown-5m-1900000000",
                                "quote_ts": 1900000250,
                                "outcome": "YES",
                                "source_side": "BUY",
                                "polymarket_price": 0.42,
                                "size": 12,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            rows, summary = materializer_mod.materialize_public_activity_file(
                path,
                max_rows_per_file=100,
            )
            self.assertEqual(summary["materialized_rows"], 0)
            self.assertEqual(rows, [])

    def test_public_activity_rows_require_market_slug(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_activity_slug_") as tmp:
            path = Path(tmp) / "real_public_activity_entry_rows.json"
            path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "asset": "BTC",
                                "timeframe": "5m",
                                "quote_ts": 1900000250,
                                "outcome": "YES",
                                "source_side": "SELL",
                                "polymarket_price": 0.42,
                                "size": 12,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            rows, summary = materializer_mod.materialize_public_activity_file(
                path,
                max_rows_per_file=100,
            )
            self.assertEqual(summary["materialized_rows"], 0)
            self.assertEqual(rows, [])

    def test_materializes_raw_activity_trade_rows_sell_touch(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_raw_activity_") as tmp:
            path = Path(tmp) / "activity_trade_rows.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "type": "TRADE",
                            "transactionHash": "tx-1",
                            "conditionId": "cond",
                            "slug": "btc-updown-5m-1900000000",
                            "timestamp": 1900000250,
                            "outcome": "Up",
                            "side": "SELL",
                            "price": 0.41,
                            "size": 8,
                            "usdcSize": 3.28,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            paths = materializer_mod.iter_public_activity_paths(
                [Path(tmp)],
                include_smoke=False,
                max_files=10,
            )
            self.assertEqual(paths, [path])
            rows, summary = materializer_mod.materialize_public_activity_file(
                path,
                max_rows_per_file=100,
            )
            self.assertEqual(summary["materialized_rows"], 1)
            self.assertEqual(rows[0]["source_sequence_id"], "tx-1")
            self.assertEqual(rows[0]["condition_id"], "cond")
            self.assertEqual(rows[0]["side"], "YES")
            self.assertEqual(rows[0]["public_taker_side"], "SELL")
            self.assertEqual(rows[0]["public_trade_px"], 0.41)
            self.assertEqual(rows[0]["public_trade_qty"], 8.0)

    def test_materializes_raw_activity_trade_rows_account_buy_proxy(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_raw_activity_buy_") as tmp:
            path = Path(tmp) / "activity_trade_rows.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "type": "TRADE",
                            "transactionHash": "tx-2",
                            "conditionId": "cond",
                            "slug": "btc-updown-5m-1900000000",
                            "timestamp": 1900000250,
                            "outcome": "Down",
                            "side": "BUY",
                            "price": 0.59,
                            "size": 11,
                            "proxyWallet": "0xbf337426aa856996b8bb79b238345dd1a0276bf7",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            rows, summary = materializer_mod.materialize_public_activity_file(
                path,
                max_rows_per_file=100,
            )
            self.assertEqual(summary["materialized_rows"], 1)
            self.assertEqual(rows[0]["side"], "NO")
            self.assertEqual(rows[0]["public_account_side"], "BUY")
            self.assertEqual(rows[0]["public_taker_side"], "")
            self.assertEqual(rows[0]["public_trade_px"], 0.59)
            self.assertEqual(rows[0]["public_trade_qty"], 11.0)
            self.assertEqual(
                rows[0]["materialized_depth_source"],
                "public_account_buy_size_proxy_not_l2_depth",
            )
            self.assertEqual(rows[0]["maker_truth"], "public_account_buy_proxy_only")

    def test_raw_activity_non_trade_rows_are_ignored(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_raw_activity_type_") as tmp:
            path = Path(tmp) / "activity_trade_rows.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "type": "MERGE",
                            "conditionId": "cond",
                            "slug": "btc-updown-5m-1900000000",
                            "timestamp": 1900000250,
                            "outcome": "Up",
                            "side": "SELL",
                            "price": 0.41,
                            "size": 8,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            rows, summary = materializer_mod.materialize_public_activity_file(
                path,
                max_rows_per_file=100,
            )
            self.assertEqual(summary["materialized_rows"], 0)
            self.assertEqual(rows, [])

    def test_cli_writes_csv_and_manifest(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_cli_") as tmp:
            root = Path(tmp)
            input_dir = root / "events"
            input_dir.mkdir()
            event_path = input_dir / "btc-updown-5m-1900000000.events.jsonl"
            event_path.write_text(
                json.dumps(
                    {
                        "kind": "candidate",
                        "slug": "btc-updown-5m-1900000000",
                        "ts_ms": 1900000250000,
                        "offset_s": 250,
                        "side": "NO",
                        "price": 0.56,
                        "yes_bid": 0.42,
                        "public_trade_px": 0.57,
                        "public_trade_size": 30,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            out = root / "out"
            old_argv = sys.argv
            sys.argv = [
                "materialize_nagi_ce25_b27bc_maker_shadow_input.py",
                "--root",
                str(input_dir),
                "--output-dir",
                str(out),
            ]
            try:
                rc = materializer_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            manifest = json.loads((out / "manifest.json").read_text())
            self.assertEqual(manifest["materialized_rows"], 1)
            self.assertFalse(manifest["non_claims"]["order_execution"])
            with (out / "nagi_ce25_b27bc_maker_shadow_input.csv").open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["side"], "NO")

    def test_cli_writes_public_activity_manifest_counts(self):
        with tempfile.TemporaryDirectory(prefix="nagi_materializer_cli_activity_") as tmp:
            root = Path(tmp)
            input_dir = root / "activity"
            input_dir.mkdir()
            activity_path = input_dir / "real_public_activity_entry_rows.json"
            activity_path.write_text(
                json.dumps(
                    {
                        "rows": [
                            {
                                "asset": "BTC",
                                "timeframe": "5m",
                                "market_slug": "btc-updown-5m-1900000000",
                                "quote_ts": 1900000250,
                                "outcome": "DOWN",
                                "source_side": "SELL",
                                "polymarket_price": 0.58,
                                "size": 9,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            out = root / "out"
            old_argv = sys.argv
            sys.argv = [
                "materialize_nagi_ce25_b27bc_maker_shadow_input.py",
                "--root",
                str(input_dir),
                "--output-dir",
                str(out),
            ]
            try:
                rc = materializer_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            manifest = json.loads((out / "manifest.json").read_text())
            self.assertEqual(manifest["source_files_seen"], 1)
            self.assertEqual(manifest["event_files_seen"], 0)
            self.assertEqual(manifest["public_activity_files_seen"], 1)
            self.assertEqual(manifest["materialized_rows"], 1)
            with (out / "nagi_ce25_b27bc_maker_shadow_input.csv").open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(rows[0]["side"], "NO")
            self.assertEqual(rows[0]["public_taker_side"], "SELL")


if __name__ == "__main__":
    unittest.main()
