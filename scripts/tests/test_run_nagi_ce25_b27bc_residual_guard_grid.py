import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    scripts_dir = Path(__file__).resolve().parents[1]
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    module_path = scripts_dir / "run_nagi_ce25_b27bc_residual_guard_grid.py"
    spec = importlib.util.spec_from_file_location(
        "run_nagi_ce25_b27bc_residual_guard_grid", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


grid_mod = load_module()


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


class NagiCe25B27bcResidualGuardGridTests(unittest.TestCase):
    def test_grid_cli_writes_decision_register_without_claims(self):
        with tempfile.TemporaryDirectory(prefix="nagi_residual_grid_") as tmp:
            root = Path(tmp)
            input_csv = root / "input.csv"
            out = root / "out"
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
                    },
                    {
                        "window_id": "w1",
                        "slug": "btc-updown-5m-1800000000",
                        "ts_ms": "2000",
                        "remaining_s": "44",
                        "side": "NO",
                        "no_bid": "0.50",
                        "public_taker_side": "SELL",
                        "no_bid_top5_size": "100",
                        "public_trade_qty": "10",
                    },
                ],
            )
            old_argv = sys.argv
            sys.argv = [
                "run_nagi_ce25_b27bc_residual_guard_grid.py",
                "--input-csv",
                str(input_csv),
                "--output-dir",
                str(out),
                "--residual-discount-s",
                "30",
                "--hard-timeout-s",
                "60",
                "--pair-cost-cap",
                "0.995",
                "--include-yes-first-suppressor",
                "--include-residual-quarantine",
                "--include-coverage-profiles",
                "--include-side-profiles",
                "--min-markets-for-review",
                "1",
            ]
            try:
                rc = grid_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertIn("status", decision)
            self.assertFalse(decision["non_claims"]["order_execution"])
            self.assertEqual(decision["variant_count"], 72)
            self.assertTrue((out / "grid_results.csv").exists())
            self.assertTrue((out / "top10_by_roi.csv").exists())


if __name__ == "__main__":
    unittest.main()
