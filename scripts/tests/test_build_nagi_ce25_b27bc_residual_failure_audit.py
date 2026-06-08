import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_module():
    scripts_dir = Path(__file__).resolve().parents[1]
    module_path = scripts_dir / "build_nagi_ce25_b27bc_residual_failure_audit.py"
    spec = importlib.util.spec_from_file_location(
        "build_nagi_ce25_b27bc_residual_failure_audit", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


audit_mod = load_module()


def write_run(root: Path, label: str, roi: float, resid_rate: float, residual_cost: float) -> Path:
    run_dir = root / f"nagi_ce25_b27bc_maker_shadow_run_{label}"
    run_dir.mkdir()
    decision = {
        "status": "KEEP_NAGI_CE25_B27BC_MAKER_SHADOW_RESEARCH_PROXY_PRIVATE_TELEMETRY_REQUIRED_NOT_READY",
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "blockers": ["residual_rate_above_target"],
        "overall": {
            "observed_markets": 10,
            "events": 100,
            "opportunities": 50,
            "queue_proxy_opens": 10,
            "queue_proxy_closes": 8,
            "pair_cost": 0.91,
            "pair_pnl_proxy": 25.0,
            "residual_cost": residual_cost,
            "cash_pnl_proxy_conservative": 25.0 - residual_cost,
            "roi_proxy_conservative": roi,
            "bad_pc_ge_100_share": 0.0,
            "resid_rate": resid_rate,
            "residual_timeouts": 2,
            "residual_discount_events": 11,
            "up_first_down_residual_risk_events": 5 if label == "old" else 15,
        },
    }
    (run_dir / "decision_register.json").write_text(json.dumps(decision), encoding="utf-8")
    events = [
        {
            "event": "residual_discount",
            "risk_flag": "up_first_down_residual",
            "first_side": "YES",
            "residual_cost": residual_cost / 2,
            "residual_qty": 3,
        },
        {
            "event": "completion_rejected_pair_cost",
            "pair_cost": 1.05,
            "coverage_gate_id": "ce25_last60_35_50_primary",
        },
        {"event": "queue_proxy_touch_insufficient_depth"},
    ]
    (run_dir / "nagi_ce25_b27bc_maker_shadow_events.jsonl").write_text(
        "\n".join(json.dumps(row) for row in events) + "\n",
        encoding="utf-8",
    )
    return run_dir


class NagiCe25B27bcResidualFailureAuditTests(unittest.TestCase):
    def test_audit_blocks_when_residual_dominates_latest_run(self):
        with tempfile.TemporaryDirectory(prefix="nagi_residual_audit_") as tmp:
            root = Path(tmp)
            old = write_run(root, "old", roi=0.02, resid_rate=0.10, residual_cost=10.0)
            new = write_run(root, "new", roi=-0.01, resid_rate=0.26, residual_cost=30.0)
            out = root / "out"
            old_argv = sys.argv
            sys.argv = [
                "build_nagi_ce25_b27bc_residual_failure_audit.py",
                "--run-dir",
                str(old),
                "--run-dir",
                str(new),
                "--output-dir",
                str(out),
            ]
            try:
                rc = audit_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(decision["status"], audit_mod.BLOCKED_STATUS)
            causes = {row["cause"] for row in decision["failure_register"]}
            self.assertIn("residual_cost_overwhelms_pair_edge", causes)
            self.assertIn("residual_rate_above_target", causes)
            self.assertFalse(decision["non_claims"]["order_execution"])
            self.assertTrue((out / "run_trend_summary.csv").exists())
            self.assertTrue((out / "event_breakdown.csv").exists())
            self.assertTrue((out / "RESIDUAL_FAILURE_AUDIT.md").exists())


if __name__ == "__main__":
    unittest.main()
