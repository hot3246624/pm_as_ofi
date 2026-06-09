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
    module_path = scripts_dir / "build_nagi_ce25_b27bc_own_maker_telemetry_validator_packet.py"
    spec = importlib.util.spec_from_file_location(
        "build_nagi_ce25_b27bc_own_maker_telemetry_validator_packet", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


validator_mod = load_module()


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(validator_mod.REQUIRED_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def valid_row(market_index: int, action_index: int) -> dict[str, object]:
    return {
        "market_slug": f"btc-updown-5m-{market_index}",
        "order_id": f"order-{action_index}",
        "client_order_id": f"client-{action_index}",
        "decision_ts_ms": 1780000000000 + action_index,
        "submitted_ts_ms": 1780000000100 + action_index,
        "side": "YES",
        "limit_price": 0.45,
        "order_qty": 5.0,
        "filled_qty": 5.0,
        "fill_ts_ms": 1780000000200 + action_index,
        "maker_or_taker": "maker",
        "fee_rate": 0.0,
        "fee_paid": 0.0,
        "queue_proxy_open": "true",
        "public_touch_seen": "true",
        "public_touch_to_own_fill_conversion": 0.35,
        "pair_cost_at_decision": 0.99,
        "pair_cost_realized": 0.99,
        "residual_cost": 0.01,
        "residual_cost_rate": 0.10,
        "realized_maker_edge_after_fees": 0.05,
    }


class OwnMakerTelemetryValidatorPacketTests(unittest.TestCase):
    def test_no_private_sample_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix="telemetry_no_sample_") as tmp:
            root = Path(tmp)
            out = root / "out"
            rc = validator_mod.main(["--root", str(root / "empty"), "--output-dir", str(out)])
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(decision["status"], validator_mod.NO_SAMPLE_STATUS)
            self.assertFalse(decision["passed"])
            self.assertIn("own_maker_telemetry_sample_missing", decision["blockers"])
            self.assertFalse(decision["non_claims"]["private_truth"])
            self.assertFalse(decision["non_claims"]["order_execution"])

    def test_bad_fixture_fails_on_taker_fee_and_order_minimum(self):
        with tempfile.TemporaryDirectory(prefix="telemetry_bad_") as tmp:
            root = Path(tmp)
            sample = root / "data" / "own_maker_telemetry_bad.csv"
            row = valid_row(1, 1)
            row["maker_or_taker"] = "taker"
            row["fee_rate"] = 0.07
            row["fee_paid"] = 0.01
            row["order_qty"] = 4.0
            write_rows(sample, [row])
            out = root / "out"
            rc = validator_mod.main(["--input-csv", str(sample), "--output-dir", str(out)])
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(decision["status"], validator_mod.FAILED_STATUS)
            issue_names = {item["issue"] for item in decision["row_issues_sample"]}
            self.assertIn("non_maker_fill", issue_names)
            self.assertIn("maker_fee_not_zero", issue_names)
            self.assertIn("limit_order_minimum_not_met", issue_names)

    def test_large_fixture_can_pass_validator_without_ready_claims(self):
        with tempfile.TemporaryDirectory(prefix="telemetry_good_") as tmp:
            root = Path(tmp)
            sample = root / "evidence" / "own_maker_telemetry_good.csv"
            rows = [valid_row((idx % 100) + 1, idx) for idx in range(500)]
            write_rows(sample, rows)
            out = root / "out"
            rc = validator_mod.main(["--input-csv", str(sample), "--output-dir", str(out)])
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(decision["status"], validator_mod.KEEP_STATUS)
            self.assertTrue(decision["passed"])
            self.assertEqual(decision["aggregate"]["own_maker_filled_markets"], 100)
            self.assertEqual(decision["aggregate"]["own_maker_filled_actions"], 500)
            self.assertFalse(decision["non_claims"]["ready"])
            self.assertFalse(decision["non_claims"]["private_truth"])
            self.assertFalse(decision["non_claims"]["maker_fill_truth"])


if __name__ == "__main__":
    unittest.main()
