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
    module_path = scripts_dir / "build_nagi_ce25_b27bc_preopen_candidate_stability_audit.py"
    spec = importlib.util.spec_from_file_location(
        "build_nagi_ce25_b27bc_preopen_candidate_stability_audit", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


audit_mod = load_module()


WALLET_A = "0x1111111111111111111111111111111111111111"
WALLET_B = "0x2222222222222222222222222222222222222222"


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")


def write_run(root: Path, *, input_csv: Path, lookback: int, resid_rate: float, roi: float) -> None:
    root.mkdir(parents=True)
    decision = {
        "input_csv": str(input_csv),
        "config": {
            "min_shadow_qty": 5.0,
            "market_order_min_usdc": 1.0,
            "opposite_support_lookback_s": lookback,
        },
        "status": "KEEP_NAGI_CE25_B27BC_MAKER_SHADOW_RESEARCH_PROXY_PRIVATE_TELEMETRY_REQUIRED_NOT_READY",
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "blockers": ["own_maker_telemetry_missing"],
        "overall": {
            "observed_markets": 3,
            "queue_proxy_open_markets": 2,
            "queue_proxy_opens": 2,
            "queue_proxy_closes": 2,
            "roi_proxy_conservative": roi,
            "resid_rate": resid_rate,
            "pair_cost": 0.91,
            "limit_order_min_shares": 5.0,
            "market_order_min_usdc": 1.0,
        },
    }
    (root / "decision_register.json").write_text(json.dumps(decision), encoding="utf-8")
    write_csv(
        root / "rolling_24h_summary.csv",
        [
            {
                "window_key": "2026-06-08",
                "queue_proxy_open_markets": 2,
                "roi_proxy_conservative": roi,
                "resid_rate": resid_rate,
            },
            {
                "window_key": "ALL",
                "queue_proxy_open_markets": 2,
                "roi_proxy_conservative": roi,
                "resid_rate": resid_rate,
            },
        ],
    )
    write_jsonl(
        root / "nagi_ce25_b27bc_maker_shadow_events.jsonl",
        [
            {
                "event": "queue_proxy_open",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": 1000,
                "side": "YES",
                "maker_bid_px": 0.42,
                "shadow_qty": 5.0,
            },
            {
                "event": "queue_proxy_open",
                "slug": "btc-updown-5m-1800000300",
                "ts_ms": 2000,
                "side": "NO",
                "maker_bid_px": 0.51,
                "shadow_qty": 6.0,
            },
            {
                "event": "queue_proxy_close",
                "slug": "btc-updown-5m-1800000000",
                "ts_ms": 3000,
                "side": "NO",
                "maker_bid_px": 0.48,
                "shadow_qty": 5.0,
            },
            {
                "event": "residual_finalized",
                "slug": "btc-updown-5m-1800000300",
                "first_side": "NO",
                "residual_cost": 2.5,
            },
        ],
    )


class PreopenCandidateStabilityAuditTests(unittest.TestCase):
    def test_audit_marks_passing_proxy_as_source_expansion_only(self):
        with tempfile.TemporaryDirectory(prefix="preopen_stability_") as tmp:
            root = Path(tmp)
            input_csv = root / "input.csv"
            write_csv(
                input_csv,
                [
                    {
                        "source_path": f"public_inputs/{WALLET_A}/activity_trade_rows.json",
                        "slug": "btc-updown-5m-1800000000",
                        "ts_ms": 1000,
                        "side": "YES",
                        "yes_bid": 0.42,
                    },
                    {
                        "source_path": f"public_inputs/{WALLET_B}/activity_trade_rows.json",
                        "slug": "btc-updown-5m-1800000300",
                        "ts_ms": 2000,
                        "side": "NO",
                        "no_bid": 0.51,
                    },
                ],
            )
            grid = root / "grid_results.csv"
            write_csv(
                grid,
                [
                    {
                        "variant_id": "lb15_oq20_opc0.950_q1_primary_35_65",
                        "passes_gate": "True",
                        "resid_rate": 0.08,
                        "roi_proxy_conservative": 0.04,
                    }
                ],
            )
            best_roi = root / "best_roi"
            best_residual = root / "best_residual"
            write_run(best_roi, input_csv=input_csv, lookback=15, resid_rate=0.086, roi=0.044)
            write_run(best_residual, input_csv=input_csv, lookback=60, resid_rate=0.081, roi=0.039)
            out = root / "out"
            old_argv = sys.argv
            sys.argv = [
                "build_nagi_ce25_b27bc_preopen_candidate_stability_audit.py",
                "--grid-results-csv",
                str(grid),
                "--best-roi-run-dir",
                str(best_roi),
                "--best-residual-run-dir",
                str(best_residual),
                "--output-dir",
                str(out),
                "--min-queue-proxy-open-markets",
                "25",
                "--min-rolling-windows",
                "3",
            ]
            try:
                rc = audit_mod.main()
            finally:
                sys.argv = old_argv
            self.assertEqual(rc, 0)
            decision = json.loads((out / "decision_register.json").read_text())
            self.assertEqual(
                decision["status"],
                audit_mod.STATUS,
            )
            self.assertTrue(decision["candidate_narrow_public_proxy_artifact"])
            self.assertTrue(decision["worth_another_public_or_source_truth_expansion"])
            self.assertFalse(decision["non_claims"]["order_execution"])
            self.assertTrue(decision["order_minimum_ok"])
            self.assertIn("queue_proxy_open_market_sample_below_review_floor", decision["blockers"])
            split = decision["runs"][0]["source_wallet_split_queue_proxy_open"]
            self.assertEqual(split[WALLET_A], 1)
            self.assertEqual(split[WALLET_B], 1)
            report = (out / "PREOPEN_CANDIDATE_STABILITY_AUDIT_REPORT.md").read_text()
            self.assertIn("Worth another public/source-truth expansion", report)


if __name__ == "__main__":
    unittest.main()
