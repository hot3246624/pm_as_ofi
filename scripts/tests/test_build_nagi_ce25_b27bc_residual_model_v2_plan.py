import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    scripts_dir = Path(__file__).resolve().parents[1]
    module_path = scripts_dir / "build_nagi_ce25_b27bc_residual_model_v2_plan.py"
    spec = importlib.util.spec_from_file_location(
        "build_nagi_ce25_b27bc_residual_model_v2_plan", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


plan_mod = load_module()


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


class NagiCe25B27bcResidualModelV2PlanTests(unittest.TestCase):
    def test_plan_ranks_pre_open_opposite_side_support_first(self):
        with tempfile.TemporaryDirectory(prefix="nagi_residual_v2_plan_") as tmp:
            root = Path(tmp)
            grid_csv = root / "grid_results.csv"
            write_csv(
                grid_csv,
                [
                    {
                        "variant_id": "best_roi",
                        "passes_gate": "False",
                        "roi_proxy_conservative": "0.05",
                        "resid_rate": "0.16",
                        "queue_proxy_opens": "50",
                        "observed_markets": "40",
                    },
                    {
                        "variant_id": "best_resid",
                        "passes_gate": "False",
                        "roi_proxy_conservative": "0.04",
                        "resid_rate": "0.15",
                        "queue_proxy_opens": "45",
                        "observed_markets": "40",
                    },
                ],
            )
            run_dir = root / "best_run"
            run_dir.mkdir()
            (run_dir / "decision_register.json").write_text(
                json.dumps({"status": "KEEP", "overall": {}}),
                encoding="utf-8",
            )
            (run_dir / "nagi_ce25_b27bc_maker_shadow_events.jsonl").write_text(
                "\n".join(
                    json.dumps(row)
                    for row in [
                        {"event": "completion_rejected_pair_cost"},
                        {"event": "completion_queue_proxy_not_supported"},
                        {
                            "event": "residual_discount",
                            "risk_flag": "up_first_down_residual",
                            "first_side": "YES",
                        },
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            out = root / "out"
            old_argv = sys.argv
            sys.argv = [
                "build_nagi_ce25_b27bc_residual_model_v2_plan.py",
                "--grid-results-csv",
                str(grid_csv),
                "--best-run-dir",
                str(run_dir),
                "--output-dir",
                str(out),
            ]
            try:
                rc = plan_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(decision["status"], plan_mod.STATUS)
            self.assertFalse(decision["non_claims"]["order_execution"])
            self.assertEqual(
                decision["model_family_rank"][0]["model_family"],
                "pre_open_opposite_side_support_score",
            )
            self.assertTrue((out / "model_family_rank.csv").exists())
            self.assertTrue((out / "RESIDUAL_MODEL_V2_PLAN.md").exists())


if __name__ == "__main__":
    unittest.main()
