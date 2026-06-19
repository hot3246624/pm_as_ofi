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
    module_path = scripts_dir / "build_nagi_ce25_b27bc_source_truth_telemetry_ingestion_contract_packet.py"
    spec = importlib.util.spec_from_file_location(
        "build_nagi_ce25_b27bc_source_truth_telemetry_ingestion_contract_packet", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


contract_mod = load_module()


def write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def non_claims() -> dict[str, bool]:
    return {
        "ready": False,
        "private_truth": False,
        "maker_fill_truth": False,
        "order_execution": False,
        "canary": False,
        "live": False,
    }


class SourceTruthTelemetryContractPacketTests(unittest.TestCase):
    def test_contract_packet_writes_ingestion_schema_and_report(self):
        with tempfile.TemporaryDirectory(prefix="ingestion_contract_") as tmp:
            root = Path(tmp)
            public_proxy = root / "public_proxy" / "decision_register.json"
            preopen = root / "preopen" / "decision_register.json"
            out = root / "out"
            write_json(
                public_proxy,
                {
                    "status": "BLOCKED_PUBLIC_PROXY",
                    "public_proxy_families_exhausted": True,
                    "best_residual_rate": 0.126,
                    "residual_target_miss_bps": 60.0,
                    "non_claims": non_claims(),
                },
            )
            write_json(
                preopen,
                {
                    "status": "BLOCKED_PREOPEN",
                    "passing_grid_row_count": 0,
                    "non_claims": non_claims(),
                },
            )
            old_argv = sys.argv
            sys.argv = [
                "build_nagi_ce25_b27bc_source_truth_telemetry_ingestion_contract_packet.py",
                "--output-dir",
                str(out),
                "--public-proxy-decision",
                str(public_proxy),
                "--preopen-stability",
                str(preopen),
            ]
            try:
                rc = contract_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(decision["status"], contract_mod.STATUS)
            self.assertTrue(decision["current_blocked_state"]["public_proxy_families_exhausted"])
            self.assertTrue(decision["current_blocked_state"]["b27bc_direct_pooling_rejected"])
            self.assertEqual(decision["order_minimum_contract"]["limit_post_only_maker_min_shares"], 5.0)
            self.assertFalse(decision["non_claims"]["order_execution"])
            self.assertIn("market_slug or slug", decision["source_truth_input_schema"]["required_fields_any_schema"])
            self.assertIn("maker_or_taker", decision["own_maker_telemetry_schema"]["required_fields"])
            self.assertEqual(
                decision["next_executable_action"],
                "build_local_only_own_maker_telemetry_validator_packet_with_fixtures",
            )
            report = (out / "SOURCE_TRUTH_TELEMETRY_INGESTION_CONTRACT_REPORT.md").read_text()
            self.assertIn("Accepted Source Truth Inputs", report)
            self.assertIn("Limit/post-only maker minimum shares", report)


if __name__ == "__main__":
    unittest.main()
