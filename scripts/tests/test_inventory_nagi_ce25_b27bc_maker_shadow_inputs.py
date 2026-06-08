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
    module_path = scripts_dir / "inventory_nagi_ce25_b27bc_maker_shadow_inputs.py"
    spec = importlib.util.spec_from_file_location(
        "inventory_nagi_ce25_b27bc_maker_shadow_inputs", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


inventory_mod = load_module()


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


class NagiCe25B27bcMakerShadowInputInventoryTests(unittest.TestCase):
    def test_ready_input_requires_ce25_touch_and_five_share_limit_floor(self):
        with tempfile.TemporaryDirectory(prefix="nagi_inventory_ready_") as tmp:
            path = Path(tmp) / "ready.csv"
            write_csv(
                path,
                [
                    {
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
            cfg = inventory_mod.shadow.PipelineConfig()
            assessment = inventory_mod.assess_csv(
                path,
                cfg=cfg,
                sample_limit=100,
                max_size_bytes=1024 * 1024,
            )
            self.assertEqual(assessment.status, inventory_mod.READY_STATUS)
            self.assertTrue(assessment.ready_for_shadow_run)
            self.assertEqual(assessment.order_min_ready_rows, 1)
            self.assertEqual(assessment.insufficient_depth_rows, 0)

    def test_below_five_share_touch_is_compatible_but_low_signal(self):
        with tempfile.TemporaryDirectory(prefix="nagi_inventory_small_") as tmp:
            path = Path(tmp) / "small.csv"
            write_csv(
                path,
                [
                    {
                        "slug": "btc-updown-5m-1800000000",
                        "ts_ms": "1000",
                        "remaining_s": "45",
                        "side": "YES",
                        "yes_bid": "0.42",
                        "public_taker_side": "SELL",
                        "yes_bid_top5_size": "4.99",
                        "public_trade_qty": "4.99",
                    }
                ],
            )
            assessment = inventory_mod.assess_csv(
                path,
                cfg=inventory_mod.shadow.PipelineConfig(),
                sample_limit=100,
                max_size_bytes=1024 * 1024,
            )
            self.assertEqual(assessment.status, "UNKNOWN_COMPATIBLE_LOW_SIGNAL_MAKER_SHADOW_INPUT")
            self.assertFalse(assessment.ready_for_shadow_run)
            self.assertEqual(assessment.insufficient_depth_rows, 1)
            self.assertIn("no_ge_5_share_maker_shadow_rows_in_sample", assessment.blockers)

    def test_cli_writes_inventory_decision_register(self):
        with tempfile.TemporaryDirectory(prefix="nagi_inventory_cli_") as tmp:
            root = Path(tmp)
            input_dir = root / "inputs"
            input_dir.mkdir()
            write_csv(
                input_dir / "ready.csv",
                [
                    {
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
            write_csv(input_dir / "bad.csv", [{"name": "not_strategy_input"}])
            output_dir = root / "out"
            old_argv = sys.argv
            sys.argv = [
                "inventory_nagi_ce25_b27bc_maker_shadow_inputs.py",
                "--root",
                str(input_dir),
                "--output-dir",
                str(output_dir),
            ]
            try:
                rc = inventory_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((output_dir / "decision_register.json").read_text())
            self.assertEqual(decision["ready_input_count"], 1)
            self.assertTrue(decision["preferred_input_csv"].endswith("ready.csv"))
            self.assertFalse(decision["non_claims"]["order_execution"])
            self.assertTrue((output_dir / "input_inventory.csv").exists())


if __name__ == "__main__":
    unittest.main()
