import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "infer_xuan_maker_taker.py"
    spec = importlib.util.spec_from_file_location("infer_xuan_maker_taker", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


infer_mod = load_module()


class InferXuanMakerTakerTests(unittest.TestCase):
    def test_classify_trade_paths(self):
        self.assertEqual(
            infer_mod.classify_trade(0.51, 0.48, 0.51, 10, 1500, 0.005),
            "taker_at_ask",
        )
        self.assertEqual(
            infer_mod.classify_trade(0.48, 0.48, 0.51, 10, 1500, 0.005),
            "maker_at_bid",
        )
        self.assertEqual(
            infer_mod.classify_trade(0.50, 0.48, 0.51, 10, 1500, 0.005),
            "inside_spread",
        )
        self.assertEqual(
            infer_mod.classify_trade(0.51, 0.48, 0.51, 3000, 1500, 0.005),
            "stale_book",
        )

    def test_end_to_end_inference_uses_latest_book_before_trade(self):
        with tempfile.TemporaryDirectory(prefix="pm_as_ofi_infer_xuan_") as tmp:
            root = Path(tmp)
            db_path = root / "test.sqlite"
            trades_path = root / "trades.json"
            out_csv = root / "out.csv"
            out_json = root / "out.json"

            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE market_meta (
                    condition_id TEXT,
                    slug TEXT,
                    tick_size REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE md_book_l1 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition_id TEXT NOT NULL,
                    recv_ms INTEGER NOT NULL,
                    recv_monotonic_ns INTEGER NOT NULL,
                    capture_seq INTEGER NOT NULL,
                    source_ts_ms INTEGER,
                    yes_bid_px REAL,
                    yes_ask_px REAL,
                    no_bid_px REAL,
                    no_ask_px REAL,
                    yes_bid_sz REAL,
                    yes_ask_sz REAL,
                    no_bid_sz REAL,
                    no_ask_sz REAL,
                    source_kind TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO market_meta(condition_id, slug, tick_size) VALUES(?,?,?)",
                ("0xcond", "btc-updown-5m-1", 0.01),
            )
            conn.execute(
                """
                INSERT INTO md_book_l1(
                    condition_id, recv_ms, recv_monotonic_ns, capture_seq, source_ts_ms,
                    yes_bid_px, yes_ask_px, no_bid_px, no_ask_px, source_kind
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("0xcond", 1_000, 1, 1, 1_000, 0.48, 0.52, 0.47, 0.53, "test"),
            )
            conn.execute(
                """
                INSERT INTO md_book_l1(
                    condition_id, recv_ms, recv_monotonic_ns, capture_seq, source_ts_ms,
                    yes_bid_px, yes_ask_px, no_bid_px, no_ask_px, source_kind
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("0xcond", 1_200, 2, 2, 1_200, 0.50, 0.55, 0.45, 0.49, "test"),
            )
            conn.commit()
            conn.close()

            trades_path.write_text(
                json.dumps(
                    [
                        {
                            "conditionId": "0xcond",
                            "slug": "btc-updown-5m-1",
                            "side": "BUY",
                            "outcome": "Up",
                            "price": 0.55,
                            "size": 100.0,
                            "timestamp": 1.25,
                            "transactionHash": "0xtx",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            old_argv = sys.argv[:]
            try:
                sys.argv = [
                    "infer_xuan_maker_taker.py",
                    "--trades",
                    str(trades_path),
                    "--db",
                    str(db_path),
                    "--out-csv",
                    str(out_csv),
                    "--out-json",
                    str(out_json),
                ]
                infer_mod.main()
            finally:
                sys.argv = old_argv

            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["classification_counts"]["taker_at_ask"], 1)
            rows = out_csv.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(rows), 2)
            self.assertIn("taker_at_ask", rows[1])

    def test_rowwise_recorder_schema_matches_by_slug_and_asset(self):
        with tempfile.TemporaryDirectory(prefix="pm_as_ofi_infer_xuan_rowwise_") as tmp:
            root = Path(tmp)
            db_path = root / "test.sqlite"
            trades_path = root / "trades.json"
            out_csv = root / "out.csv"
            out_json = root / "out.json"

            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE md_book_l1 (
                    date TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    recv_unix_ms INTEGER,
                    capture_seq INTEGER,
                    side TEXT,
                    bid REAL,
                    ask REAL,
                    source TEXT,
                    raw_json TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO md_book_l1(date, slug, recv_unix_ms, capture_seq, side, bid, ask, source, raw_json)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                ("2026-04-26", "btc-updown-5m-1", 1_200, 2, "0xasset", 0.44, 0.47, "test", "{}"),
            )
            conn.commit()
            conn.close()

            trades_path.write_text(
                json.dumps(
                    [
                        {
                            "conditionId": "0xcond",
                            "asset": "0xasset",
                            "slug": "btc-updown-5m-1",
                            "side": "BUY",
                            "outcome": "Down",
                            "price": 0.44,
                            "size": 80.0,
                            "timestamp": 1.25,
                            "transactionHash": "0xtx2",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            old_argv = sys.argv[:]
            try:
                sys.argv = [
                    "infer_xuan_maker_taker.py",
                    "--trades",
                    str(trades_path),
                    "--db",
                    str(db_path),
                    "--out-csv",
                    str(out_csv),
                    "--out-json",
                    str(out_json),
                ]
                infer_mod.main()
            finally:
                sys.argv = old_argv

            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["classification_counts"]["maker_at_bid"], 1)
            rows = out_csv.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(rows), 2)
            self.assertIn("maker_at_bid", rows[1])


if __name__ == "__main__":
    unittest.main()
