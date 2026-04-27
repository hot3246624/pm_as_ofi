import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "export_completion_first_shadow_report.py"
    spec = importlib.util.spec_from_file_location(
        "export_completion_first_shadow_report", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


report_mod = load_module()


class ExportCompletionFirstShadowReportTests(unittest.TestCase):
    def test_build_rows_includes_30s_completion_metrics(self):
        with tempfile.TemporaryDirectory(prefix="completion_first_report_") as tmp:
            db_path = Path(tmp) / "test.sqlite"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE market_meta (
                    slug TEXT,
                    strategy TEXT,
                    recv_unix_ms INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE own_inventory_events (
                    slug TEXT,
                    event TEXT,
                    recv_unix_ms INTEGER,
                    capture_seq INTEGER,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO market_meta(slug, strategy, recv_unix_ms) VALUES(?,?,?)",
                ("btc-updown-5m-1", "completion_first", 1_000),
            )
            events = [
                (
                    "completion_first_open_gate_decision",
                    1_000,
                    1,
                    {
                        "side": "YES",
                        "allowed": True,
                        "score": 4,
                        "score_bucket": "full_clip",
                        "block_reason": "allowed",
                        "utc_hour_bucket": 12,
                    },
                ),
                (
                    "pair_tranche_events",
                    1_001,
                    2,
                    {
                        "active_tranche_id": 1,
                        "active_state": "FirstLegPending",
                        "first_side": "YES",
                        "same_side_add_count": 0,
                        "residual_qty": 100.0,
                        "pairable_qty": 0.0,
                        "clean_closed_episode_ratio": 1.0,
                        "same_side_add_qty_ratio": 0.0,
                        "episode_close_delay_p50": 5.0,
                        "episode_close_delay_p90": 5.0,
                    },
                ),
                (
                    "pair_tranche_events",
                    6_000,
                    3,
                    {
                        "active_tranche_id": 1,
                        "active_state": "CompletionOnly",
                        "first_side": "YES",
                        "same_side_add_count": 0,
                        "residual_qty": 20.0,
                        "pairable_qty": 80.0,
                    },
                ),
                (
                    "pair_tranche_events",
                    9_000,
                    4,
                    {
                        "active_tranche_id": None,
                        "active_state": None,
                        "first_side": None,
                        "same_side_add_count": 0,
                        "residual_qty": 0.0,
                        "pairable_qty": 0.0,
                    },
                ),
                (
                    "completion_first_shadow_summary",
                    10_000,
                    5,
                    {
                        "clean_closed_episode_ratio": 1.0,
                        "same_side_add_qty_ratio": 0.0,
                        "episode_close_delay_p50": 8.0,
                        "episode_close_delay_p90": 8.0,
                    },
                ),
            ]
            for event, recv_unix_ms, capture_seq, payload in events:
                conn.execute(
                    "INSERT INTO own_inventory_events(slug, event, recv_unix_ms, capture_seq, payload_json) VALUES(?,?,?,?,?)",
                    ("btc-updown-5m-1", event, recv_unix_ms, capture_seq, json.dumps(payload)),
                )
            conn.commit()

            rows = report_mod.build_rows(conn)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["open_candidate_count"], 1)
            self.assertEqual(row["open_allowed_count"], 1)
            self.assertAlmostEqual(row["30s_completion_hit_rate"], 1.0)
            self.assertAlmostEqual(row["30s_completion_hit_rate_when_gate_on"], 1.0)
            payload = report_mod.build_summary(rows)
            self.assertAlmostEqual(payload["summary"]["30s_completion_hit_rate"], 1.0)
            conn.close()


if __name__ == "__main__":
    unittest.main()
