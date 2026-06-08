import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    scripts_dir = Path(__file__).resolve().parents[1]
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    import importlib.util

    module_path = scripts_dir / "build_nagi_ce25_b27bc_public_proxy_exhaustion_decision_packet.py"
    spec = importlib.util.spec_from_file_location(
        "build_nagi_ce25_b27bc_public_proxy_exhaustion_decision_packet", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


packet_mod = load_module()


def write_family(root: Path, *, variant_id: str, resid: float, roi: float, pass_count: int = 0) -> None:
    root.mkdir(parents=True)
    row = {
        "variant_id": variant_id,
        "resid_rate": resid,
        "roi_proxy_conservative": roi,
        "queue_proxy_open_markets": 10,
        "queue_proxy_opens": 12,
        "queue_proxy_closes": 9,
        "bad_pc_ge_100_share": 0.0,
    }
    decision = {
        "status": "BLOCKED_TEST",
        "variant_count": 1,
        "pass_count": pass_count,
        "best_by_residual": row,
        "best_by_roi": row,
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
    }
    (root / "decision_register.json").write_text(json.dumps(decision), encoding="utf-8")
    (root / "grid_results.csv").write_text(
        "variant_id,resid_rate,roi_proxy_conservative,queue_proxy_open_markets,queue_proxy_opens,queue_proxy_closes,bad_pc_ge_100_share\n"
        f"{variant_id},{resid},{roi},10,12,9,0\n",
        encoding="utf-8",
    )


class PublicProxyExhaustionPacketTests(unittest.TestCase):
    def test_packet_builder_writes_json_and_report(self):
        with tempfile.TemporaryDirectory(prefix="proxy_exhaustion_") as tmp:
            root = Path(tmp)
            family_a = root / "family_a"
            family_b = root / "family_b"
            out = root / "out"
            write_family(family_a, variant_id="a", resid=0.14, roi=0.05)
            write_family(family_b, variant_id="b", resid=0.20, roi=0.09)
            old_argv = sys.argv
            sys.argv = [
                "build_nagi_ce25_b27bc_public_proxy_exhaustion_decision_packet.py",
                "--output-dir",
                str(out),
                "--family-dir",
                f"family_a={family_a}",
                "--family-dir",
                f"family_b={family_b}",
            ]
            old_defaults = packet_mod.DEFAULT_FAMILY_DIRS
            packet_mod.DEFAULT_FAMILY_DIRS = {}
            try:
                rc = packet_mod.main()
            finally:
                packet_mod.DEFAULT_FAMILY_DIRS = old_defaults
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertTrue(decision["public_proxy_families_exhausted"])
            self.assertTrue(decision["no_live_or_private_claims"])
            self.assertFalse(decision["non_claims"]["order_execution"])
            self.assertEqual(decision["best_public_proxy_candidate"]["variant_id"], "a")
            self.assertEqual(decision["best_roi_tradeoff"]["variant_id"], "b")
            self.assertGreater(decision["residual_target_miss_bps"], 0)
            report = (out / "PUBLIC_PROXY_EXHAUSTION_DECISION_REPORT.md").read_text()
            self.assertIn("Limit/post-only maker shadow requires", report)
            self.assertIn("collect_own_maker_telemetry_sample", report)


if __name__ == "__main__":
    unittest.main()
