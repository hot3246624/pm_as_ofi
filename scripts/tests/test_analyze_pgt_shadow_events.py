import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "analyze_pgt_shadow_events.py"
    spec = importlib.util.spec_from_file_location("analyze_pgt_shadow_events", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyze_mod = load_module()


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def event_row(event: str, data: dict, *, recv_unix_ms: int = 1_800_000_000_000):
    return {
        "recv_unix_ms": recv_unix_ms,
        "payload": {
            "event": event,
            "data": data,
        },
    }


class AnalyzePgtShadowEventsTests(unittest.TestCase):
    def test_dplus_minorder_no_seed_counts_are_aggregated(self):
        with tempfile.TemporaryDirectory(prefix="pgt_shadow_events_") as tmp:
            root = Path(tmp)
            instance = "dplus_minorder_v1"
            day = "2026-05-15"
            write_jsonl(
                root / instance / day / "btc-updown-5m-1000" / "events.jsonl",
                [
                    event_row(
                        "pgt_shadow_summary",
                        {
                            "paired_qty": 0.0,
                            "pair_cost": 0.0,
                            "residual_qty": 0.0,
                            "market_trade_ticks": 9,
                            "market_sell_trade_ticks": 4,
                            "pgt_dplus_minorder_no_seed": {
                                "no_recent_sell_trade": 3,
                                "price_band": 1,
                            },
                        },
                    ),
                ],
            )
            write_jsonl(
                root / instance / day / "btc-updown-5m-1001" / "events.jsonl",
                [
                    event_row(
                        "pgt_shadow_summary",
                        {
                            "paired_qty": 0.0,
                            "pair_cost": 0.0,
                            "residual_qty": 0.0,
                            "market_trade_ticks": 5,
                            "market_sell_trade_ticks": 2,
                            "pgt_dplus_minorder_no_seed": {
                                "no_recent_sell_trade": 2,
                                "imbalance": 1,
                            },
                        },
                    ),
                ],
            )

            rows = analyze_mod.collect_rows(root, instance, day)
            summary = analyze_mod.summarize(rows)
            details = analyze_mod.round_details(rows)

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            summary["dplus_minorder_no_seed"],
            {
                "imbalance": 1,
                "no_recent_sell_trade": 5,
                "price_band": 1,
            },
        )
        self.assertEqual(summary["market_trade_ticks"], 14)
        self.assertEqual(summary["market_sell_trade_ticks"], 6)
        self.assertEqual(
            details[0]["dplus_minorder_no_seed"],
            {
                "no_recent_sell_trade": 3,
                "price_band": 1,
            },
        )

    def test_high_pressure_summary_uses_settlement_alpha_fields(self):
        with tempfile.TemporaryDirectory(prefix="pgt_shadow_events_") as tmp:
            root = Path(tmp)
            instance = "xuan_high_pressure_v1"
            day = "2026-05-16"
            write_jsonl(
                root / instance / day / "btc-updown-5m-2000" / "events.jsonl",
                [
                    event_row(
                        "pgt_shadow_summary",
                        {
                            "pgt_shadow_profile": "xuan_high_pressure_v1",
                            "paired_qty": 0.0,
                            "pair_cost": 0.0,
                            "residual_qty": 10.0,
                            "yes_qty": 10.0,
                            "yes_avg_cost": 0.60,
                            "no_qty": 0.0,
                            "no_avg_cost": 0.0,
                            "market_trade_ticks": 9,
                            "market_sell_trade_ticks": 4,
                            "pgt_entry_pressure_sides": 2,
                            "pgt_entry_pressure_extra_ticks": 3,
                            "pgt_high_pressure_no_seed": {
                                "weak_pressure": 5,
                                "price_band": 1,
                            },
                        },
                    ),
                    event_row(
                        "market_resolved",
                        {
                            "winner_side": "YES",
                        },
                    ),
                ],
            )

            rows = analyze_mod.collect_rows(root, instance, day)
            summary = analyze_mod.summarize(rows)
            details = analyze_mod.round_details(rows)

        self.assertEqual(summary["profiles"], {"xuan_high_pressure_v1": 1})
        self.assertEqual(summary["market_buy_trade_ticks"], 5)
        self.assertEqual(summary["pgt_entry_pressure_sides"], 2)
        self.assertEqual(summary["pgt_entry_pressure_extra_ticks"], 3)
        self.assertEqual(
            summary["high_pressure_no_seed"],
            {
                "price_band": 1,
                "weak_pressure": 5,
            },
        )
        self.assertAlmostEqual(summary["settlement_alpha_pnl"], 4.0)
        self.assertAlmostEqual(summary["settlement_alpha_roi"], 4.0 / 6.0)
        self.assertAlmostEqual(summary["settlement_alpha_fee50_pnl"], 3.97)
        self.assertAlmostEqual(summary["settlement_alpha_fee50_roi"], 3.97 / 6.0)
        self.assertAlmostEqual(summary["settlement_alpha_fee100_pnl"], 3.94)
        self.assertAlmostEqual(summary["settlement_alpha_fee100_roi"], 3.94 / 6.0)
        self.assertAlmostEqual(details[0]["settlement_pnl"], 4.0)
        self.assertAlmostEqual(details[0]["settlement_fee50_pnl"], 3.97)
        self.assertAlmostEqual(details[0]["settlement_fee100_pnl"], 3.94)

    def test_gamma_winner_backfill_populates_missing_winner(self):
        with tempfile.TemporaryDirectory(prefix="pgt_shadow_events_") as tmp:
            root = Path(tmp)
            instance = "xuan_high_pressure_v1"
            day = "2026-05-16"
            write_jsonl(
                root / instance / day / "btc-updown-5m-3000" / "events.jsonl",
                [
                    event_row(
                        "pgt_shadow_summary",
                        {
                            "pgt_shadow_profile": "xuan_high_pressure_v1",
                            "paired_qty": 0.0,
                            "pair_cost": 0.0,
                            "residual_qty": 10.0,
                            "yes_qty": 10.0,
                            "yes_avg_cost": 0.60,
                            "no_qty": 0.0,
                            "no_avg_cost": 0.0,
                        },
                    ),
                ],
            )
            rows = analyze_mod.collect_rows(root, instance, day)
            old_fetch = analyze_mod.fetch_gamma_winner_side
            analyze_mod.fetch_gamma_winner_side = lambda slug: "YES"
            try:
                analyze_mod.backfill_missing_winners(rows, True)
            finally:
                analyze_mod.fetch_gamma_winner_side = old_fetch
            summary = analyze_mod.summarize(rows)

        self.assertEqual(rows[0].winner_side, "YES")
        self.assertAlmostEqual(summary["settlement_alpha_pnl"], 4.0)


if __name__ == "__main__":
    unittest.main()
