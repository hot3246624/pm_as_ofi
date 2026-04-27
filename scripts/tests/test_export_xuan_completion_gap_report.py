import importlib.util
import sys
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "export_xuan_completion_gap_report.py"
    spec = importlib.util.spec_from_file_location(
        "export_xuan_completion_gap_report", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gap_mod = load_module()


class ExportXuanCompletionGapReportTests(unittest.TestCase):
    def test_build_gap_uses_new_30s_targets(self):
        report = {
            "summary": {
                "market_count": 2,
                "open_candidate_total": 10,
                "open_allowed_total": 4,
                "open_blocked_total": 6,
                "30s_completion_hit_rate": 0.6,
                "30s_completion_hit_rate_when_gate_on": 0.8,
                "30s_completion_hit_rate_when_gate_off": 0.2,
                "median_first_opposite_delay_s": 12.0,
                "score_bucket_distribution": {"full_clip": 4},
                "session_bucket_distribution": {"12": 4},
            }
        }
        xuan_summary = {
            "xuan_targets": {
                "xuan_30s_completion_hit_rate": 0.7,
                "xuan_median_first_opposite_delay_s": 10.0,
                "xuan_score_bucket_distribution": {"full_clip": 8},
                "xuan_session_distribution": {"12": 6},
                "xuan_maker_proxy_ratio": 0.55,
            },
            "provisional": True,
        }
        out = gap_mod.build_gap(report, xuan_summary)
        self.assertEqual(out["shadow_summary"]["markets"], 2)
        self.assertAlmostEqual(out["gap_vs_xuan"]["completion_30s_hit_rate"], -0.1)
        self.assertTrue(out["provisional"])


if __name__ == "__main__":
    unittest.main()
