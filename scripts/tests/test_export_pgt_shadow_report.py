import importlib.util
import sqlite3
import sys
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "export_pgt_shadow_report.py"
    spec = importlib.util.spec_from_file_location("export_pgt_shadow_report", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


report_mod = load_module()


class ExportPgtShadowReportTests(unittest.TestCase):
    def test_build_rows_reconstructs_seed_lifecycle_from_events(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE market_meta (
                    date TEXT,
                    slug TEXT,
                    condition_id TEXT,
                    market_id TEXT,
                    strategy TEXT,
                    dry_run INTEGER,
                    recv_unix_ms INTEGER,
                    event TEXT,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE capital_state_events (
                    date TEXT,
                    slug TEXT,
                    recv_unix_ms INTEGER,
                    capture_seq INTEGER,
                    event TEXT,
                    working_capital REAL,
                    locked_in_active_tranches REAL,
                    locked_in_pair_covered REAL,
                    mergeable_full_sets REAL,
                    locked_capital_ratio REAL,
                    would_block_new_open_due_to_capital INTEGER,
                    would_trigger_merge_due_to_capital INTEGER,
                    capital_pressure_merge_batch_shadow REAL,
                    clean_closed_episode_ratio REAL,
                    same_side_add_qty_ratio REAL,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE own_inventory_events (
                    date TEXT,
                    slug TEXT,
                    recv_unix_ms INTEGER,
                    capture_seq INTEGER,
                    event TEXT,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE pair_tranche_events (
                    date TEXT,
                    slug TEXT,
                    recv_unix_ms INTEGER,
                    capture_seq INTEGER,
                    event TEXT,
                    tranche_id INTEGER,
                    state TEXT,
                    first_side TEXT,
                    pairable_qty REAL,
                    residual_qty REAL,
                    pair_cost_tranche REAL,
                    pair_cost_fifo_ref REAL,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE own_order_lifecycle (
                    date TEXT,
                    slug TEXT,
                    recv_unix_ms INTEGER,
                    capture_seq INTEGER,
                    event TEXT,
                    slot TEXT,
                    side TEXT,
                    direction TEXT,
                    order_id TEXT,
                    reason TEXT,
                    purpose TEXT,
                    reject_kind TEXT,
                    retry TEXT,
                    cooldown_ms INTEGER,
                    price REAL,
                    size REAL,
                    exec_path TEXT,
                    payload_json TEXT
                )
                """
            )

            slug = "btc-updown-5m-1000"
            end_ms = 1_000_000
            conn.execute(
                "INSERT INTO market_meta VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    "2026-04-29",
                    slug,
                    "0xcond",
                    "0xmarket",
                    "pair_gated_tranche_arb",
                    1,
                    end_ms - 300_000,
                    "session_start",
                    '{"round_end_ts":1000}',
                ),
            )
            order_rows = [
                (end_ms - 200_000, 1, "order_accepted", "YES_BUY", "YES", "BUY", "Provide", 0.50, 57.6),
                (end_ms - 100_000, 2, "order_accepted", "NO_BUY", "NO", "BUY", "Provide", 0.49, 57.6),
                (end_ms - 79_999, 5, "order_accepted", "NO_BUY", "NO", "BUY", "Hedge", 0.49, 57.6),
                (end_ms - 70_000, 6, "order_accepted", "YES_BUY", "YES", "BUY", "Provide", 0.49, 6.0),
                (end_ms - 25_000, 8, "cancel_ack", "YES_BUY", "YES", "BUY", "EndgameRiskGate", None, None),
            ]
            for recv_ms, seq, event, slot, side, direction, reason, price, size in order_rows:
                conn.execute(
                    "INSERT INTO own_order_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "2026-04-29",
                        slug,
                        recv_ms,
                        seq,
                        event,
                        slot,
                        side,
                        direction,
                        "",
                        reason,
                        "",
                        "",
                        "",
                        None,
                        price,
                        size,
                        "MAKER_ACCEPTED" if event == "order_accepted" else "",
                        "{}",
                    ),
                )
            inventory_rows = [
                (
                    end_ms - 80_000,
                    3,
                    "fill_snapshot",
                    '{"direction":"Buy","side":"Yes","size":57.6,"price":0.5}',
                ),
                (
                    end_ms - 10_000,
                    7,
                    "fill_snapshot",
                    '{"direction":"Buy","side":"No","size":57.6,"price":0.49}',
                ),
                (
                    end_ms + 45_000,
                    9,
                    "pgt_shadow_summary",
                    '{"paired_qty":57.6,"pair_cost":0.99,"residual_qty":0.0}',
                ),
            ]
            for recv_ms, seq, event, payload in inventory_rows:
                conn.execute(
                    "INSERT INTO own_inventory_events VALUES (?,?,?,?,?,?)",
                    ("2026-04-29", slug, recv_ms, seq, event, payload),
                )
            conn.commit()

            row = report_mod.build_rows(conn)[0]
            summary = report_mod.build_summary([row])
        finally:
            conn.close()

        self.assertEqual(row["round_complete"], 1.0)
        self.assertAlmostEqual(row["first_seed_accept_rel_s"], -200.0)
        self.assertAlmostEqual(row["dual_seed_accept_rel_s"], -100.0)
        self.assertAlmostEqual(row["first_buy_fill_rel_s"], -80.0)
        self.assertAlmostEqual(row["first_cover_fill_rel_s"], -10.0)
        self.assertAlmostEqual(row["first_completion_accept_rel_s"], -79.999)
        self.assertAlmostEqual(row["first_same_side_add_accept_rel_s"], -70.0)
        self.assertAlmostEqual(row["first_seed_to_first_fill_s"], 120.0)
        self.assertAlmostEqual(row["first_completion_delay_s"], 70.0)
        self.assertAlmostEqual(row["seed_live_before_first_fill_or_cancel_s"], 120.0)
        self.assertEqual(row["initial_seed_accept_count"], 2)
        self.assertEqual(row["initial_seed_side_count"], 2)
        self.assertEqual(row["completion_accept_count"], 1)
        self.assertEqual(row["same_side_add_accept_count_before_cover"], 1)
        self.assertAlmostEqual(row["same_side_add_accept_qty_before_cover"], 6.0)
        self.assertAlmostEqual(row["same_side_add_qty_ratio"], 6.0 / 57.6)
        self.assertEqual(summary["seed_exposed_rounds"], 1)
        self.assertEqual(summary["seed_exposed_fill_rounds"], 1)
        self.assertAlmostEqual(summary["seed_exposed_fill_ratio"], 1.0)
        self.assertAlmostEqual(
            summary["median_same_side_add_qty_ratio"], 6.0 / 57.6
        )
        self.assertAlmostEqual(summary["median_first_completion_delay_s"], 70.0)
        self.assertAlmostEqual(summary["p90_first_completion_delay_s"], 70.0)


if __name__ == "__main__":
    unittest.main()
