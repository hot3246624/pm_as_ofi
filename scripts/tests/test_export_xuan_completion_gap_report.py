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
            "coverage_stats": {
                "recent_overlap_episode_count": 320,
                "holdout_lift_pct": 0.12,
            },
            "xuan_targets": {
                "xuan_30s_completion_hit_rate": 0.7,
                "xuan_median_first_opposite_delay_s": 10.0,
                "xuan_score_bucket_distribution": {"full_clip": 8},
                "xuan_session_distribution": {"12": 6},
                "xuan_maker_proxy_ratio": 0.55,
            },
            "provisional": True,
        }
        contract = {
            "thresholds": {
                "data_usable": {
                    "recent_overlap_episode_min": 300,
                    "field": "coverage_stats.recent_overlap_episode_count",
                },
                "research_effective": {
                    "holdout_lift_min_abs": 0.10,
                    "field": "coverage_stats.holdout_lift_pct",
                },
                "enforce_discussion_ready": {
                    "baseline_report_required": True,
                    "shadow_vs_baseline_completion_lift_min_abs": 0.10,
                    "shadow_vs_xuan_completion_gap_max_abs": 0.05,
                    "shadow_vs_xuan_delay_gap_max_s": 5.0,
                },
            },
            "question_priority": [{"rank": 1, "module": "Open Gate"}],
            "shadow_gap_question_order": ["why blocked", "why no opposite", "why no clean close"],
        }
        baseline_report = {
            "summary": {
                "30s_completion_hit_rate": 0.45,
            }
        }
        out = gap_mod.build_gap(report, xuan_summary, contract, baseline_report)
        self.assertEqual(out["shadow_summary"]["markets"], 2)
        self.assertAlmostEqual(out["gap_vs_xuan"]["completion_30s_hit_rate"], -0.1)
        self.assertTrue(out["provisional"])
        self.assertEqual(out["decision_summary"]["verdict"], "Shadow Gap Actionable")
        self.assertTrue(out["decision_summary"]["data_usable"]["pass"])


if __name__ == "__main__":
    unittest.main()
