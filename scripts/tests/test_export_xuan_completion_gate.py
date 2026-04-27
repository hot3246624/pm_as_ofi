import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "export_xuan_completion_gate.py"
    spec = importlib.util.spec_from_file_location(
        "export_xuan_completion_gate", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate_mod = load_module()


class ExportXuanCompletionGateTests(unittest.TestCase):
    def test_build_episodes_handles_overshoot_as_new_episode(self):
        rows = [
            {
                "slug": "btc-updown-5m-1000",
                "conditionId": "0xcond",
                "timestamp": 1000,
                "outcome": "Up",
                "size": 100.0,
                "price": 0.45,
                "asset": "yes_asset",
            },
            {
                "slug": "btc-updown-5m-1000",
                "conditionId": "0xcond",
                "timestamp": 1010,
                "outcome": "Down",
                "size": 130.0,
                "price": 0.54,
                "asset": "no_asset",
            },
        ]
        episodes = gate_mod.build_episodes(rows)
        self.assertEqual(len(episodes), 2)
        self.assertTrue(episodes[0]["label_complete_30s"])
        self.assertEqual(episodes[1]["first_leg_side"], "NO")
        self.assertAlmostEqual(episodes[1]["first_leg_clip"], 30.0)

    def test_main_exports_defaults_and_summary(self):
        with tempfile.TemporaryDirectory(prefix="xuan_completion_gate_") as tmp:
            root = Path(tmp)
            trades_path = root / "trades.json"
            activity_path = root / "activity.json"
            db_path = root / "replay.sqlite"
            defaults_path = root / "defaults.json"
            summary_path = root / "summary.json"
            episodes_csv = root / "episodes.csv"

            trades = [
                {
                    "slug": "btc-updown-5m-1000",
                    "conditionId": "0xcond",
                    "timestamp": 1000,
                    "outcome": "Up",
                    "size": 100.0,
                    "price": 0.45,
                    "asset": "yes_asset",
                    "side": "BUY",
                },
                {
                    "slug": "btc-updown-5m-1000",
                    "conditionId": "0xcond",
                    "timestamp": 1010,
                    "outcome": "Down",
                    "size": 100.0,
                    "price": 0.54,
                    "asset": "no_asset",
                    "side": "BUY",
                },
            ]
            trades_path.write_text(json.dumps(trades), encoding="utf-8")
            activity_path.write_text(json.dumps(trades), encoding="utf-8")

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
                CREATE TABLE md_trades (
                    date TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    recv_unix_ms INTEGER,
                    capture_seq INTEGER,
                    asset_id TEXT,
                    side TEXT,
                    price REAL,
                    size REAL,
                    trade_id TEXT,
                    raw_json TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO md_book_l1(date, slug, recv_unix_ms, capture_seq, side, bid, ask, source, raw_json) VALUES(?,?,?,?,?,?,?,?,?)",
                ("2026-04-26", "btc-updown-5m-1000", 999_500, 1, "yes_asset", 0.44, 0.45, "test", "{}"),
            )
            conn.execute(
                "INSERT INTO md_book_l1(date, slug, recv_unix_ms, capture_seq, side, bid, ask, source, raw_json) VALUES(?,?,?,?,?,?,?,?,?)",
                ("2026-04-26", "btc-updown-5m-1000", 999_500, 2, "no_asset", 0.54, 0.55, "test", "{}"),
            )
            conn.execute(
                "INSERT INTO md_trades(date, slug, recv_unix_ms, capture_seq, asset_id, side, price, size, trade_id, raw_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("2026-04-26", "btc-updown-5m-1000", 999_000, 1, "no_asset", "BUY", 0.54, 10.0, "t1", "{}"),
            )
            conn.commit()
            conn.close()

            old_argv = sys.argv[:]
            try:
                sys.argv = [
                    "export_xuan_completion_gate.py",
                    "--trades",
                    str(trades_path),
                    "--activity",
                    str(activity_path),
                    "--db",
                    str(db_path),
                    "--out-summary-json",
                    str(summary_path),
                    "--out-defaults-json",
                    str(defaults_path),
                    "--out-episodes-csv",
                    str(episodes_csv),
                ]
                gate_mod.main()
            finally:
                sys.argv = old_argv

            defaults = json.loads(defaults_path.read_text(encoding="utf-8"))
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertIn("feature_bucket_defs", defaults)
            self.assertIn("coverage_stats", defaults)
            self.assertIn("xuan_targets", summary)
            self.assertTrue(episodes_csv.exists())


if __name__ == "__main__":
    unittest.main()
