import importlib.util
import sys
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "export_xuan_pgt_gap_report.py"
    spec = importlib.util.spec_from_file_location("export_xuan_pgt_gap_report", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gap_mod = load_module()


class ExportXuanPgtGapReportTests(unittest.TestCase):
    def test_build_gap_compares_to_xuan_targets_and_shadow_gates(self):
        payload = {
            "rows": [
                {
                    "slug": "btc-updown-5m-100",
                    "clean_closed_episode_ratio": 0.92,
                    "same_side_add_qty_ratio": 0.08,
                    "episode_close_delay_p90": 80.0,
                    "merge_requested_first_rel_s": -22.0,
                    "redeem_requested_first_rel_s": 38.0,
                    "summary_pair_cost": 0.99,
                    "single_seed_flip_count": 1.0,
                    "taker_shadow_would_open": 5.0,
                    "taker_shadow_would_close": 4.0,
                    "dispatch_taker_open": 0.0,
                    "dispatch_taker_close": 2.0,
                    "first_seed_accept_rel_s": -360.0,
                    "dual_seed_accept_rel_s": -270.0,
                    "first_buy_fill_rel_s": -180.0,
                    "first_seed_to_first_fill_s": 180.0,
                    "first_completion_delay_s": 70.0,
                    "seed_live_before_first_fill_or_cancel_s": 180.0,
                    "has_active_episode": 1.0,
                    "residual_round": 1.0,
                    "maker_only_missed_open_round": 1.0,
                },
                {
                    "slug": "btc-updown-5m-200",
                    "clean_closed_episode_ratio": 0.90,
                    "same_side_add_qty_ratio": 0.12,
                    "episode_close_delay_p90": 90.0,
                    "merge_requested_first_rel_s": -20.0,
                    "redeem_requested_first_rel_s": 40.0,
                    "summary_pair_cost": 1.01,
                    "single_seed_flip_count": 0.0,
                    "taker_shadow_would_open": 1.0,
                    "taker_shadow_would_close": 0.0,
                    "dispatch_taker_open": 0.0,
                    "dispatch_taker_close": 0.0,
                    "first_seed_accept_rel_s": -350.0,
                    "dual_seed_accept_rel_s": -260.0,
                    "first_buy_fill_rel_s": None,
                    "first_seed_to_first_fill_s": None,
                    "first_completion_delay_s": None,
                    "seed_live_before_first_fill_or_cancel_s": 325.0,
                    "has_active_episode": 0.0,
                    "residual_round": 0.0,
                    "maker_only_missed_open_round": 0.0,
                },
            ]
        }
        out = gap_mod.build_gap(payload)
        self.assertEqual(out["markets"], 2)
        self.assertIsNone(out["filters"]["min_end_ts"])
        self.assertIsNone(out["filters"]["max_end_ts"])
        self.assertAlmostEqual(out["pgt_medians"]["clean_closed_episode_ratio"], 0.91)
        self.assertAlmostEqual(out["pgt_medians"]["same_side_add_qty_ratio"], 0.10)
        self.assertAlmostEqual(out["pgt_medians"]["summary_pair_cost"], 0.99)
        self.assertEqual(out["pgt_distributions"]["summary_pair_cost_rounds"], 1)
        self.assertAlmostEqual(out["pgt_distributions"]["summary_pair_cost_p90"], 0.99)
        self.assertAlmostEqual(out["pgt_distributions"]["summary_pair_cost_max"], 0.99)
        self.assertEqual(out["pgt_distributions"]["summary_pair_cost_gt_1_02_rounds"], 0)
        self.assertEqual(out["pgt_distributions"]["summary_pair_cost_gt_1_03_rounds"], 0)
        self.assertAlmostEqual(out["gap_vs_xuan"]["summary_pair_cost_median"], 0.99 - 0.9755)
        self.assertAlmostEqual(out["pgt_medians"]["single_seed_flip_count"], 0.5)
        self.assertAlmostEqual(out["pgt_medians"]["first_seed_accept_rel_s"], -355.0)
        self.assertAlmostEqual(out["pgt_medians"]["dual_seed_accept_rel_s"], -265.0)
        self.assertAlmostEqual(out["pgt_medians"]["first_buy_fill_rel_s"], -180.0)
        self.assertAlmostEqual(out["pgt_medians"]["first_completion_delay_s"], 70.0)
        self.assertAlmostEqual(out["pgt_medians"]["p90_first_completion_delay_s"], 70.0)
        self.assertAlmostEqual(out["pgt_medians"]["seed_live_before_first_fill_or_cancel_s"], 252.5)
        self.assertTrue(out["within_xuan_windows"]["merge_requested_first_rel_s"])
        self.assertTrue(out["within_xuan_windows"]["redeem_requested_first_rel_s"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["clean_closed_episode_ratio"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["same_side_add_qty_ratio"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["episode_close_delay_p90"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["p90_first_completion_delay_s"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["summary_pair_cost_median"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["summary_pair_cost_p90"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["summary_pair_cost_tail"])
        self.assertEqual(out["pgt_rates"]["active_episode_rounds"], 1)
        self.assertAlmostEqual(out["pgt_rates"]["active_episode_ratio"], 0.5)
        self.assertEqual(out["pgt_rates"]["residual_rounds"], 1)
        self.assertAlmostEqual(out["pgt_rates"]["residual_round_ratio"], 0.5)
        self.assertEqual(out["pgt_rates"]["taker_close_opportunity_rounds"], 1)
        self.assertEqual(out["pgt_rates"]["taker_close_dispatched_rounds"], 1)
        self.assertAlmostEqual(out["pgt_rates"]["taker_close_dispatch_round_ratio"], 1.0)
        self.assertAlmostEqual(out["pgt_rates"]["total_taker_shadow_would_close"], 4.0)
        self.assertAlmostEqual(out["pgt_rates"]["total_dispatch_taker_close"], 2.0)
        self.assertEqual(out["pgt_rates"]["seed_exposed_rounds"], 2)
        self.assertEqual(out["pgt_rates"]["seed_exposed_fill_rounds"], 1)
        self.assertAlmostEqual(out["pgt_rates"]["seed_exposed_fill_ratio"], 0.5)
        self.assertEqual(out["pgt_rates"]["dual_seed_rounds"], 2)
        self.assertEqual(out["counterfactual_readout"]["maker_only_missed_open_rounds"], 1)
        self.assertEqual(out["counterfactual_readout"]["single_seed_flip_rounds"], 1)
        self.assertAlmostEqual(out["counterfactual_readout"]["median_taker_shadow_open_gap"], 3.0)

    def test_build_gap_can_filter_by_slug_end_ts(self):
        payload = {
            "rows": [
                {
                    "slug": "btc-updown-5m-100",
                    "has_active_episode": 1.0,
                    "taker_shadow_would_close": 3.0,
                    "dispatch_taker_close": 0.0,
                },
                {
                    "slug": "btc-updown-5m-200",
                    "has_active_episode": 1.0,
                    "taker_shadow_would_close": 1.0,
                    "dispatch_taker_close": 1.0,
                },
            ]
        }
        out = gap_mod.build_gap(payload, min_end_ts=200)
        self.assertEqual(out["markets"], 1)
        self.assertEqual(out["filters"]["min_end_ts"], 200)
        self.assertEqual(out["pgt_rates"]["taker_close_opportunity_rounds"], 1)
        self.assertEqual(out["pgt_rates"]["taker_close_dispatched_rounds"], 1)

    def test_same_side_gate_matches_xuan_shadow_band(self):
        payload = {
            "rows": [
                {
                    "slug": "btc-updown-5m-100",
                    "same_side_add_qty_ratio": 0.14,
                    "has_active_episode": 1.0,
                }
            ]
        }
        out = gap_mod.build_gap(payload)
        self.assertEqual(out["pgt_shadow_gates"]["same_side_add_qty_ratio_max"], 0.15)
        self.assertTrue(out["passes_pgt_shadow_gate"]["same_side_add_qty_ratio"])

        payload["rows"][0]["same_side_add_qty_ratio"] = 0.16
        out = gap_mod.build_gap(payload)
        self.assertFalse(out["passes_pgt_shadow_gate"]["same_side_add_qty_ratio"])

    def test_pair_cost_gate_ignores_zero_qty_rows_and_flags_expensive_tail(self):
        payload = {
            "rows": [
                {
                    "slug": "btc-updown-5m-100",
                    "has_active_episode": 1.0,
                    "summary_paired_qty": 0.0,
                    "summary_pair_cost": 0.0,
                    "first_completion_delay_s": 10.0,
                },
                {
                    "slug": "btc-updown-5m-200",
                    "has_active_episode": 1.0,
                    "summary_paired_qty": 10.0,
                    "summary_pair_cost": 0.99,
                    "first_completion_delay_s": 20.0,
                },
                {
                    "slug": "btc-updown-5m-300",
                    "has_active_episode": 1.0,
                    "summary_paired_qty": 10.0,
                    "summary_pair_cost": 1.029,
                    "first_completion_delay_s": 150.0,
                },
            ]
        }
        out = gap_mod.build_gap(payload)

        self.assertEqual(out["pgt_distributions"]["summary_pair_cost_rounds"], 2)
        self.assertAlmostEqual(out["pgt_medians"]["summary_pair_cost"], 1.0095)
        self.assertEqual(out["pgt_distributions"]["summary_pair_cost_gt_1_02_rounds"], 1)
        self.assertEqual(out["pgt_distributions"]["summary_pair_cost_gt_1_03_rounds"], 0)
        self.assertFalse(out["passes_pgt_shadow_gate"]["summary_pair_cost_median"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["summary_pair_cost_tail"])
        self.assertFalse(out["passes_pgt_shadow_gate"]["p90_first_completion_delay_s"])

    def test_pair_cost_tail_gate_tracks_xuan_price_cost_p90(self):
        payload = {
            "rows": [
                {
                    "slug": f"btc-updown-5m-{100 + idx}",
                    "has_active_episode": 1.0,
                    "summary_paired_qty": 10.0,
                    "summary_pair_cost": cost,
                }
                for idx, cost in enumerate([0.97, 0.98, 0.99, 1.00, 1.01, 1.02, 1.025, 1.029, 1.031, 1.04])
            ]
        }
        out = gap_mod.build_gap(payload)
        self.assertAlmostEqual(out["pgt_distributions"]["summary_pair_cost_gt_1_03_ratio"], 0.2)
        self.assertFalse(out["passes_pgt_shadow_gate"]["summary_pair_cost_tail"])

        payload["rows"][-1]["summary_pair_cost"] = 1.029
        out = gap_mod.build_gap(payload)
        self.assertAlmostEqual(out["pgt_distributions"]["summary_pair_cost_gt_1_03_ratio"], 0.1)
        self.assertTrue(out["passes_pgt_shadow_gate"]["summary_pair_cost_tail"])


if __name__ == "__main__":
    unittest.main()
