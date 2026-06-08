import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "run_nagi_ce25_b27bc_maker_shadow.py"
    spec = importlib.util.spec_from_file_location("run_nagi_ce25_b27bc_maker_shadow", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


shadow_mod = load_module()


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class NagiCe25B27bcMakerShadowTests(unittest.TestCase):
    def test_coverage_gate_accepts_last60_bands_and_rejects_negative_bucket(self):
        yes_last60 = {
            "slug": "btc-updown-5m-1800000000",
            "remaining_s": "45",
            "side": "YES",
            "yes_bid": "0.42",
        }
        yes_early = dict(yes_last60)
        yes_early["remaining_s"] = "180"
        sol = dict(yes_last60)
        sol["slug"] = "sol-updown-5m-1800000000"
        decision = shadow_mod.coverage_decision(yes_last60, 1, "YES", 0.42)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.gate_id, "ce25_last60_35_50_primary")
        decision = shadow_mod.coverage_decision(yes_early, 1, "YES", 0.42)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.gate_id, "ce25_1_5m_35_50_negative")
        decision = shadow_mod.coverage_decision(sol, 1, "YES", 0.42)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.gate_id, "excluded_non_btc")

    def test_public_sell_touch_opens_and_closes_queue_proxy_without_maker_truth_claim(self):
        rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "condition_id": "cond-1",
                "ts_ms": "1000",
                "remaining_s": "45",
                "side": "YES",
                "yes_bid": "0.42",
                "public_taker_side": "SELL",
                "yes_bid_top5_size": "100",
                "public_trade_qty": "40",
                "l2_age_ms": "10",
                "align_lag_ms": "20",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "condition_id": "cond-1",
                "ts_ms": "2000",
                "remaining_s": "44",
                "side": "NO",
                "no_bid": "0.56",
                "public_taker_side": "SELL",
                "no_bid_top5_size": "100",
                "public_trade_qty": "40",
                "l2_age_ms": "10",
                "align_lag_ms": "20",
            },
        ]
        pipeline = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(queue_conversion=0.5, pair_cost_cap=0.995)
        )
        pipeline.run(rows)
        events = [event["event"] for event in pipeline.events]
        self.assertIn("queue_proxy_open", events)
        self.assertIn("queue_proxy_close", events)
        self.assertTrue(all(event.get("maker_truth") == "public_queue_proxy_only" for event in pipeline.events))
        overall = pipeline.summary_rows()[0]
        self.assertAlmostEqual(overall["pair_cost"], 0.98)
        self.assertEqual(overall["bad_pc_ge_100_share"], 0.0)
        self.assertEqual(overall["resid_rate"], 0.0)

    def test_residual_closer_flags_discount_and_hard_timeout(self):
        rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "0",
                "remaining_s": "60",
                "side": "YES",
                "yes_bid": "0.42",
                "public_taker_side": "SELL",
                "yes_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "31000",
                "remaining_s": "29",
                "side": "YES",
                "yes_bid": "0.43",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "181000",
                "remaining_s": "0",
                "side": "YES",
                "yes_bid": "0.43",
            },
        ]
        pipeline = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(
                queue_conversion=1.0,
                residual_discount_s=30.0,
                hard_timeout_s=180.0,
                min_markets_for_review=1,
            )
        )
        pipeline.run(rows)
        events = [event["event"] for event in pipeline.events]
        self.assertIn("residual_discount", events)
        self.assertIn("residual_finalized", events)
        summary = pipeline.summary_rows()[0]
        self.assertEqual(summary["residual_timeouts"], 1)
        self.assertGreater(summary["up_first_down_residual_risk_events"], 0)
        self.assertGreater(summary["resid_rate"], 0.0)

    def test_yes_first_suppressor_rejects_yes_open_without_changing_default(self):
        rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "1000",
                "remaining_s": "45",
                "side": "YES",
                "yes_bid": "0.42",
                "public_taker_side": "SELL",
                "yes_bid_top5_size": "100",
                "public_trade_qty": "10",
            }
        ]
        default_pipeline = shadow_mod.MakerShadowPipeline(shadow_mod.PipelineConfig())
        default_pipeline.run(rows)
        self.assertIn("queue_proxy_open", [event["event"] for event in default_pipeline.events])

        guarded = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(suppress_yes_first_open=True)
        )
        guarded.run(rows)
        events = [event["event"] for event in guarded.events]
        self.assertIn("open_rejected_yes_first_suppressor", events)
        self.assertNotIn("queue_proxy_open", events)

    def test_quarantine_after_residual_discount_blocks_reopen_after_timeout(self):
        rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "0",
                "remaining_s": "60",
                "side": "NO",
                "no_bid": "0.42",
                "public_taker_side": "SELL",
                "no_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "31000",
                "remaining_s": "29",
                "side": "NO",
                "no_bid": "0.43",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "61000",
                "remaining_s": "0",
                "side": "NO",
                "no_bid": "0.43",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "62000",
                "remaining_s": "0",
                "side": "NO",
                "no_bid": "0.44",
                "public_taker_side": "SELL",
                "no_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
        ]
        pipeline = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(
                quarantine_after_residual_discount=True,
                residual_discount_s=30.0,
                hard_timeout_s=60.0,
            )
        )
        pipeline.run(rows)
        events = [event["event"] for event in pipeline.events]
        self.assertIn("residual_discount", events)
        self.assertIn("residual_finalized", events)
        self.assertIn("open_rejected_residual_quarantine", events)
        self.assertEqual(events.count("queue_proxy_open"), 1)

    def test_coverage_and_side_profile_filters_reject_open_candidates(self):
        rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "1000",
                "remaining_s": "45",
                "side": "YES",
                "yes_bid": "0.42",
                "public_taker_side": "SELL",
                "yes_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000001",
                "ts_ms": "1000",
                "remaining_s": "45",
                "side": "NO",
                "no_bid": "0.55",
                "public_taker_side": "SELL",
                "no_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
        ]
        gate_filtered = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(allowed_coverage_gates=("ce25_last60_50_65_primary",))
        )
        gate_filtered.run(rows)
        events = [event["event"] for event in gate_filtered.events]
        self.assertIn("open_rejected_coverage_profile_filter", events)
        self.assertIn("queue_proxy_open", events)

        side_filtered = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(allowed_open_sides=("NO",))
        )
        side_filtered.run(rows)
        events = [event["event"] for event in side_filtered.events]
        self.assertIn("open_rejected_side_profile_filter", events)
        self.assertIn("queue_proxy_open", events)

    def test_pre_open_opposite_support_requires_prior_supported_opposite_touch(self):
        rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "1000",
                "remaining_s": "45",
                "side": "NO",
                "no_bid": "0.50",
                "public_taker_side": "SELL",
                "no_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "2000",
                "remaining_s": "44",
                "side": "YES",
                "yes_bid": "0.42",
                "public_taker_side": "SELL",
                "yes_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
        ]
        pipeline = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(
                require_opposite_support_before_open=True,
                opposite_support_lookback_s=15.0,
                opposite_support_min_qty=5.0,
                opposite_support_pair_cost_cap=0.95,
            )
        )
        pipeline.run(rows)
        events = [event["event"] for event in pipeline.events]
        self.assertEqual(events.count("open_rejected_no_recent_opposite_support"), 1)
        self.assertEqual(events.count("queue_proxy_open"), 1)
        open_event = next(event for event in pipeline.events if event["event"] == "queue_proxy_open")
        self.assertEqual(open_event["opposite_support_side"], "NO")
        self.assertAlmostEqual(open_event["opposite_support_pair_cost"], 0.92)

    def test_pre_open_opposite_support_rejects_stale_or_expensive_support(self):
        stale_rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "1000",
                "remaining_s": "45",
                "side": "NO",
                "no_bid": "0.50",
                "public_taker_side": "SELL",
                "no_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "70000",
                "remaining_s": "0",
                "side": "YES",
                "yes_bid": "0.42",
                "public_taker_side": "SELL",
                "yes_bid_top5_size": "100",
                "public_trade_qty": "10",
            },
        ]
        pipeline = shadow_mod.MakerShadowPipeline(
            shadow_mod.PipelineConfig(
                require_opposite_support_before_open=True,
                opposite_support_lookback_s=15.0,
                opposite_support_min_qty=5.0,
                opposite_support_pair_cost_cap=0.95,
            )
        )
        pipeline.run(stale_rows)
        events = [event["event"] for event in pipeline.events]
        self.assertEqual(events.count("queue_proxy_open"), 0)
        self.assertGreaterEqual(events.count("open_rejected_no_recent_opposite_support"), 2)

    def test_limit_order_minimum_requires_five_shares(self):
        rows = [
            {
                "window_id": "w1",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": "1000",
                "remaining_s": "45",
                "side": "YES",
                "yes_bid": "0.42",
                "public_taker_side": "SELL",
                "yes_bid_top5_size": "4.99",
                "public_trade_qty": "4.99",
            }
        ]
        pipeline = shadow_mod.MakerShadowPipeline(shadow_mod.PipelineConfig())
        pipeline.run(rows)
        events = [event["event"] for event in pipeline.events]
        self.assertIn("queue_proxy_touch_insufficient_depth", events)
        self.assertNotIn("queue_proxy_open", events)
        summary = pipeline.summary_rows()[0]
        self.assertEqual(summary["limit_order_min_shares"], 5.0)
        self.assertEqual(summary["market_order_min_usdc"], 1.0)

    def test_cli_writes_decision_register_and_rolling_summary(self):
        with tempfile.TemporaryDirectory(prefix="nagi_ce25_b27bc_shadow_") as tmp:
            root = Path(tmp)
            input_csv = root / "input.csv"
            output_dir = root / "out"
            write_csv(
                input_csv,
                [
                    {
                        "window_id": "w1",
                        "slug": "btc-updown-5m-1800000000",
                        "ts_ms": "1000",
                        "remaining_s": "45",
                        "side": "YES",
                        "yes_bid": "0.42",
                        "public_taker_side": "SELL",
                        "yes_bid_top5_size": "100",
                        "public_trade_qty": "10",
                    }
                ],
            )
            old_argv = sys.argv
            sys.argv = [
                "run_nagi_ce25_b27bc_maker_shadow.py",
                "--input-csv",
                str(input_csv),
                "--output-dir",
                str(output_dir),
                "--min-markets-for-review",
                "1",
            ]
            try:
                rc = shadow_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((output_dir / "decision_register.json").read_text())
            self.assertEqual(decision["status"], shadow_mod.RESEARCH_STATUS)
            self.assertFalse(decision["non_claims"]["maker_fill_truth"])
            self.assertEqual(decision["config"]["min_shadow_qty"], 5.0)
            self.assertEqual(decision["config"]["market_order_min_usdc"], 1.0)
            self.assertTrue((output_dir / "rolling_24h_summary.csv").exists())
            self.assertTrue((output_dir / "nagi_ce25_b27bc_maker_shadow_events.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
