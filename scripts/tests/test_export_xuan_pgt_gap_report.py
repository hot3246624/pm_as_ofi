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
                    "clean_closed_episode_ratio": 0.92,
                    "same_side_add_qty_ratio": 0.08,
                    "episode_close_delay_p90": 80.0,
                    "merge_requested_first_rel_s": -22.0,
                    "redeem_requested_first_rel_s": 38.0,
                    "summary_pair_cost": 0.99,
                    "single_seed_flip_count": 1.0,
                    "taker_shadow_would_open": 5.0,
                    "dispatch_taker_open": 0.0,
                    "maker_only_missed_open_round": 1.0,
                },
                {
                    "clean_closed_episode_ratio": 0.90,
                    "same_side_add_qty_ratio": 0.12,
                    "episode_close_delay_p90": 90.0,
                    "merge_requested_first_rel_s": -20.0,
                    "redeem_requested_first_rel_s": 40.0,
                    "summary_pair_cost": 1.01,
                    "single_seed_flip_count": 0.0,
                    "taker_shadow_would_open": 1.0,
                    "dispatch_taker_open": 0.0,
                    "maker_only_missed_open_round": 0.0,
                },
            ]
        }
        out = gap_mod.build_gap(payload)
        self.assertEqual(out["markets"], 2)
        self.assertAlmostEqual(out["pgt_medians"]["clean_closed_episode_ratio"], 0.91)
        self.assertAlmostEqual(out["pgt_medians"]["same_side_add_qty_ratio"], 0.10)
        self.assertAlmostEqual(out["pgt_medians"]["summary_pair_cost"], 1.0)
        self.assertAlmostEqual(out["pgt_medians"]["single_seed_flip_count"], 0.5)
        self.assertTrue(out["within_xuan_windows"]["merge_requested_first_rel_s"])
        self.assertTrue(out["within_xuan_windows"]["redeem_requested_first_rel_s"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["clean_closed_episode_ratio"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["same_side_add_qty_ratio"])
        self.assertTrue(out["passes_pgt_shadow_gate"]["episode_close_delay_p90"])
        self.assertEqual(out["counterfactual_readout"]["maker_only_missed_open_rounds"], 1)
        self.assertEqual(out["counterfactual_readout"]["single_seed_flip_rounds"], 1)
        self.assertAlmostEqual(out["counterfactual_readout"]["median_taker_shadow_open_gap"], 3.0)


if __name__ == "__main__":
    unittest.main()
