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
    module_path = scripts_dir / "build_nagi_ce25_b27bc_strategy_decision_register_update_packet.py"
    spec = importlib.util.spec_from_file_location(
        "build_nagi_ce25_b27bc_strategy_decision_register_update_packet", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


packet_mod = load_module()


def non_claims(**overrides):
    base = {
        "ready": False,
        "private_truth": False,
        "maker_fill_truth": False,
        "order_execution": False,
        "canary": False,
        "live": False,
    }
    base.update(overrides)
    return base


def write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def write_inputs(root: Path, *, flipped_non_claim: bool = False) -> dict[str, Path]:
    paths = {
        "public": root / "public_proxy" / "decision_register.json",
        "preopen": root / "preopen" / "decision_register.json",
        "ingestion": root / "ingestion" / "decision_register.json",
        "telemetry": root / "telemetry" / "decision_register.json",
    }
    write_json(
        paths["public"],
        {
            "status": "BLOCKED_NAGI_CE25_B27BC_PUBLIC_PROXY_EXHAUSTION_PRIVATE_TELEMETRY_OR_SOURCE_TRUTH_REQUIRED",
            "public_proxy_families_exhausted": True,
            "best_residual_rate": 0.126151,
            "residual_target_miss_bps": 61.5,
            "non_claims": non_claims(private_truth=flipped_non_claim),
        },
    )
    write_json(
        paths["preopen"],
        {
            "status": "BLOCKED_NAGI_CE25_B27BC_PREOPEN_CANDIDATE_STABILITY_AUDIT_NO_PASSING_PROXY_CANDIDATE",
            "passing_grid_row_count": 0,
            "non_claims": non_claims(),
        },
    )
    write_json(
        paths["ingestion"],
        {
            "status": "KEEP_NAGI_CE25_B27BC_SOURCE_TRUTH_TELEMETRY_INGESTION_CONTRACT_REVIEWED_PUBLIC_PROXY_EXHAUSTED_SOURCE_TRUTH_OR_OWN_TELEMETRY_REQUIRED_NOT_READY",
            "next_executable_action": "build_local_only_own_maker_telemetry_validator_packet_with_fixtures",
            "non_claims": non_claims(),
        },
    )
    write_json(
        paths["telemetry"],
        {
            "status": "BLOCKED_NAGI_CE25_B27BC_OWN_MAKER_TELEMETRY_VALIDATOR_NO_PRIVATE_SAMPLE_FAIL_CLOSED_NOT_READY",
            "passed": False,
            "blockers": ["own_maker_telemetry_sample_missing"],
            "non_claims": non_claims(),
        },
    )
    return paths


class StrategyDecisionRegisterUpdatePacketTests(unittest.TestCase):
    def test_packet_writes_go_no_go_register(self):
        with tempfile.TemporaryDirectory(prefix="strategy_register_") as tmp:
            root = Path(tmp)
            paths = write_inputs(root)
            out = root / "out"
            rc = packet_mod.main(
                [
                    "--output-dir",
                    str(out),
                    "--public-proxy-decision",
                    str(paths["public"]),
                    "--preopen-stability",
                    str(paths["preopen"]),
                    "--ingestion-contract",
                    str(paths["ingestion"]),
                    "--telemetry-validator",
                    str(paths["telemetry"]),
                ]
            )
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(decision["status"], packet_mod.STATUS)
            self.assertEqual(decision["go_no_go_register"]["ce25_taker_seed_px_mainline"]["decision"], "NO_GO")
            self.assertEqual(
                decision["go_no_go_register"]["own_maker_telemetry_validator"]["decision"],
                "KEEP_TOOLING_FAIL_CLOSED",
            )
            self.assertFalse(decision["non_claims"]["ready"])
            self.assertIn("drop_own_maker_telemetry_sample_csv_into_data_inputs_or_evidence", decision["next_material_paths"])
            report = (out / "STRATEGY_DECISION_REGISTER_UPDATE_REPORT.md").read_text()
            self.assertIn("GO / NO-GO Register", report)
            self.assertIn("blind_public_proxy_parameter_grids_without_new_source_truth", report)

    def test_packet_rejects_non_claim_flip(self):
        with tempfile.TemporaryDirectory(prefix="strategy_register_bad_") as tmp:
            root = Path(tmp)
            paths = write_inputs(root, flipped_non_claim=True)
            with self.assertRaises(RuntimeError):
                packet_mod.main(
                    [
                        "--output-dir",
                        str(root / "out"),
                        "--public-proxy-decision",
                        str(paths["public"]),
                        "--preopen-stability",
                        str(paths["preopen"]),
                        "--ingestion-contract",
                        str(paths["ingestion"]),
                        "--telemetry-validator",
                        str(paths["telemetry"]),
                    ]
                )


if __name__ == "__main__":
    unittest.main()
