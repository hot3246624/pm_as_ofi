import importlib.util
import sys
import unittest
from pathlib import Path


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


runner_mod = load_module(
    "run_xuan_m0001_maker_shadow",
    (Path(__file__).resolve().parents[1] / "run_xuan_m0001_maker_shadow.py"),
)


class XuanM0001MakerShadowRunnerTests(unittest.TestCase):
    def test_seed_price_uses_public_trade_minus_edge(self):
        cfg = runner_mod.RunnerConfig(seed_edge=0.022, tick_size=0.001)
        self.assertAlmostEqual(runner_mod.seed_price(0.5339, cfg), 0.511)
        self.assertAlmostEqual(runner_mod.seed_price(0.0100, cfg), 0.001)

    def test_fill_classifier_separates_real_queue_and_touch_only(self):
        cfg = runner_mod.RunnerConfig(queue_share=0.25, queue_haircut=0.5)
        real, real_qty, _ = runner_mod.classify_fill(
            {"real_fill": "true"},
            quote_px=0.50,
            reference_px=0.50,
            requested_qty=10.0,
            action="seed",
            cfg=cfg,
        )
        self.assertEqual(real, runner_mod.FillClass.REAL_FILL)
        self.assertEqual(real_qty, 10.0)

        queue, queue_qty, _ = runner_mod.classify_fill(
            {"queue_supported_qty": "100"},
            quote_px=0.50,
            reference_px=0.50,
            requested_qty=10.0,
            action="seed",
            cfg=cfg,
        )
        self.assertEqual(queue, runner_mod.FillClass.QUEUE_SUPPORTED_FILL)
        self.assertEqual(queue_qty, 10.0)

        touch, touch_qty, _ = runner_mod.classify_fill(
            {"queue_supported_qty": "10"},
            quote_px=0.50,
            reference_px=0.50,
            requested_qty=10.0,
            action="seed",
            cfg=cfg,
        )
        self.assertEqual(touch, runner_mod.FillClass.TOUCH_ONLY)
        self.assertEqual(touch_qty, 0.0)

    def test_completion_priority_closes_active_lot_before_new_seed(self):
        cfg = runner_mod.RunnerConfig(
            queue_share=1.0,
            queue_haircut=1.0,
            completion_pair_cap=0.990,
            max_seed_qty=10.0,
        )
        rows = [
            {
                "ts_ms": "1000",
                "side": "YES",
                "public_trade_px": "0.522",
                "public_trade_size": "10",
                "no_ask": "0.480",
                "queue_supported_qty": "10",
            },
            {
                "ts_ms": "2000",
                "side": "NO",
                "public_trade_px": "0.520",
                "public_trade_size": "10",
                "no_ask": "0.480",
                "queue_supported_qty": "10",
            },
        ]
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        events = [e["event"] for e in r.events]
        self.assertEqual(summary["completed_cycles"], 1)
        self.assertIn("completion_evaluated", events)
        self.assertIn("cycle_completed", events)
        self.assertEqual(summary["seed_fills"], 1)

    def test_completion_caps_stage_by_remaining_time(self):
        cfg = runner_mod.RunnerConfig(
            queue_share=1.0,
            queue_haircut=1.0,
            completion_pair_cap=0.990,
            late_completion_pair_cap=0.995,
            final_completion_pair_cap=1.010,
            max_seed_qty=10.0,
        )
        seed = {
            "ts_ms": "1000",
            "side": "YES",
            "public_trade_px": "0.522",
            "public_trade_size": "10",
            "no_ask": "0.480",
            "queue_supported_qty": "10",
        }
        late_completion = {
            "ts_ms": "2000",
            "remaining_s": "60",
            "no_ask": "0.494",
            "queue_supported_qty": "10",
        }
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run([dict(seed), dict(late_completion)]).to_dict()
        self.assertEqual(summary["completed_cycles"], 1)
        self.assertAlmostEqual(summary["pair_cost_avg"], 0.994)

        no_stage_completion = dict(late_completion)
        no_stage_completion.pop("remaining_s")
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run([dict(seed), no_stage_completion]).to_dict()
        self.assertEqual(summary["completed_cycles"], 0)
        self.assertIn("completion_observed", [e["event"] for e in r.events])

    def test_aged_blocked_lot_skip_preserves_residual(self):
        cfg = runner_mod.RunnerConfig(
            queue_share=1.0,
            queue_haircut=1.0,
            blocked_skip_age_s=120.0,
            blocked_skip_margin=0.001,
            aged_unwind_pair_cap=1.000,
            max_seed_qty=10.0,
        )
        rows = [
            {
                "ts_ms": "0",
                "side": "YES",
                "public_trade_px": "0.522",
                "public_trade_size": "10",
                "no_ask": "0.480",
                "queue_supported_qty": "10",
            },
            {
                "ts_ms": "120000",
                "side": "NO",
                "public_trade_px": "0.540",
                "public_trade_size": "10",
                "no_ask": "0.502",
                "queue_supported_qty": "10",
            },
        ]
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        self.assertEqual(summary["blocked_lot_skips"], 1)
        self.assertEqual(summary["completed_cycles"], 0)
        self.assertAlmostEqual(summary["residual_qty"], 10.0)

    def test_touch_only_seed_does_not_create_accounting_fill(self):
        cfg = runner_mod.RunnerConfig(queue_share=0.1, queue_haircut=0.1, max_seed_qty=10.0)
        rows = [
            {
                "ts_ms": "1000",
                "side": "YES",
                "public_trade_px": "0.522",
                "public_trade_size": "10",
                "no_ask": "0.480",
                "queue_supported_qty": "1",
            }
        ]
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        self.assertEqual(summary["touch_only"], 1)
        self.assertEqual(summary["seed_fills"], 0)
        self.assertEqual(summary["residual_qty"], 0.0)

    def test_default_material_lockout_qty_boundary_matches_m0001_guard(self):
        cfg = runner_mod.RunnerConfig(queue_share=1.0, queue_haircut=1.0, max_seed_qty=6.0)
        rows = [
            {
                "ts_ms": "1000",
                "side": "YES",
                "public_trade_px": "0.522",
                "public_trade_size": "6",
                "no_ask": "0.480",
                "queue_supported_qty": "6",
            },
            {
                "ts_ms": "2000",
                "side": "NO",
                "public_trade_px": "0.520",
                "public_trade_size": "6",
                "no_ask": "0.600",
                "queue_supported_qty": "6",
            },
        ]
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        self.assertEqual(summary["material_lockouts"], 0)
        self.assertAlmostEqual(summary["residual_qty"], 6.0)

        cfg = runner_mod.RunnerConfig(queue_share=1.0, queue_haircut=1.0, max_seed_qty=6.1)
        rows[0]["public_trade_size"] = "6.1"
        rows[0]["queue_supported_qty"] = "6.1"
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        self.assertEqual(summary["material_lockouts"], 1)
        self.assertAlmostEqual(summary["residual_qty"], 6.1)

    def test_default_material_lockout_cost_matches_m0001_guard(self):
        cfg = runner_mod.RunnerConfig(material_residual_cost=6.0)
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        r.active = runner_mod.ShadowLot(
            side="YES",
            qty=5.0,
            first_px=1.20,
            opened_ts_ms=0,
            cycle_id=1,
            fill_class=runner_mod.FillClass.QUEUE_SUPPORTED_FILL,
        )
        self.assertFalse(r._active_material_residual_blocks_new_seed())

        r.active.first_px = 1.21
        self.assertTrue(r._active_material_residual_blocks_new_seed())

    def test_seed_quote_sizes_to_public_trade_fraction_before_support_check(self):
        cfg = runner_mod.RunnerConfig(queue_share=0.25, queue_haircut=1.0)
        rows = [
            {
                "ts_ms": "1000",
                "side": "YES",
                "public_trade_px": "0.522",
                "public_trade_size": "80",
                "no_ask": "0.480",
                "queue_supported_qty": "80",
            }
        ]
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        self.assertEqual(summary["seed_fills"], 1)
        self.assertAlmostEqual(summary["residual_qty"], 20.0)
        self.assertAlmostEqual(r.events[0]["requested_qty"], 20.0)

    def test_seed_admission_requires_m0001_public_pair_gate(self):
        cfg = runner_mod.RunnerConfig(queue_share=1.0, queue_haircut=1.0)
        rows = [
            {
                "ts_ms": "1000",
                "side": "YES",
                "public_trade_px": "0.522",
                "public_trade_size": "10",
                "no_ask": "0.4995",
                "queue_supported_qty": "10",
            }
        ]
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        self.assertEqual(summary["seed_attempts"], 0)

    def test_seed_requires_explicit_public_sell_tick_when_taker_side_present(self):
        cfg = runner_mod.RunnerConfig(queue_share=1.0, queue_haircut=1.0)
        base = {
            "ts_ms": "1000",
            "side": "YES",
            "public_trade_px": "0.522",
            "public_trade_size": "10",
            "no_ask": "0.480",
            "queue_supported_qty": "10",
        }

        buy_row = dict(base, taker_side="BUY")
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run([buy_row]).to_dict()
        self.assertEqual(summary["seed_attempts"], 0)
        self.assertIn("seed_skipped_non_sell_public_trade", [e["event"] for e in r.events])

        sell_row = dict(base, taker_side="SELL")
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run([sell_row]).to_dict()
        self.assertEqual(summary["seed_attempts"], 1)
        self.assertEqual(summary["seed_fills"], 1)
        self.assertEqual(r.events[0]["taker_side"], "SELL")

    def test_cycle_cap_blocks_new_seed_after_six_cycles(self):
        cfg = runner_mod.RunnerConfig(
            queue_share=1.0,
            queue_haircut=1.0,
            max_cycles=6,
            max_seed_qty=10.0,
            completion_pair_cap=0.990,
        )
        rows = []
        ts = 0
        for _ in range(7):
            rows.append(
                {
                    "ts_ms": str(ts),
                    "side": "YES",
                    "public_trade_px": "0.522",
                    "public_trade_size": "10",
                    "no_ask": "0.480",
                    "queue_supported_qty": "10",
                }
            )
            ts += 1000
            rows.append(
                {
                    "ts_ms": str(ts),
                    "side": "NO",
                    "public_trade_px": "0.520",
                    "public_trade_size": "10",
                    "no_ask": "0.480",
                    "queue_supported_qty": "10",
                }
            )
            ts += 1000
        r = runner_mod.XuanM0001MakerShadowRunner(cfg)
        summary = r.run(rows).to_dict()
        self.assertEqual(summary["completed_cycles"], 6)
        self.assertIn("seed_blocked_cycle_cap", [e["event"] for e in r.events])


if __name__ == "__main__":
    unittest.main()
