#!/usr/bin/env python3
"""Compare completion_first shadow report against xuan 30s completion targets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_XUAN_SUMMARY = REPO_ROOT / "docs" / "xuan_completion_gate_summary.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--report-json", required=True, help="completion_first_shadow_summary.json")
    p.add_argument("--xuan-summary-json", default=str(DEFAULT_XUAN_SUMMARY))
    p.add_argument("--output-json", help="Optional explicit output path")
    return p.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing json file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"expected dict json: {path}")
    return payload


def build_gap(report: dict[str, Any], xuan_summary: dict[str, Any]) -> dict[str, Any]:
    report_summary = report.get("summary") or {}
    xuan_targets = xuan_summary.get("xuan_targets") or {}
    return {
        "shadow_summary": {
            "markets": report_summary.get("market_count", 0),
            "open_candidate_total": report_summary.get("open_candidate_total", 0),
            "open_allowed_total": report_summary.get("open_allowed_total", 0),
            "open_blocked_total": report_summary.get("open_blocked_total", 0),
            "completion_30s_hit_rate": report_summary.get("30s_completion_hit_rate"),
            "completion_30s_hit_rate_when_gate_on": report_summary.get("30s_completion_hit_rate_when_gate_on"),
            "completion_30s_hit_rate_when_gate_off": report_summary.get("30s_completion_hit_rate_when_gate_off"),
            "median_first_opposite_delay_s": report_summary.get("median_first_opposite_delay_s"),
            "score_bucket_distribution": report_summary.get("score_bucket_distribution", {}),
            "session_bucket_distribution": report_summary.get("session_bucket_distribution", {}),
        },
        "xuan_targets": xuan_targets,
        "gap_vs_xuan": {
            "completion_30s_hit_rate": (
                report_summary.get("30s_completion_hit_rate", 0.0)
                - xuan_targets.get("xuan_30s_completion_hit_rate", 0.0)
            ),
            "median_first_opposite_delay_s": (
                report_summary.get("median_first_opposite_delay_s", 0.0)
                - xuan_targets.get("xuan_median_first_opposite_delay_s", 0.0)
            ),
            "maker_proxy_ratio": None,
        },
        "provisional": bool(xuan_summary.get("provisional")),
    }


def main() -> None:
    args = parse_args()
    report_path = Path(args.report_json)
    xuan_summary_path = Path(args.xuan_summary_json)
    payload = build_gap(load_json(report_path), load_json(xuan_summary_path))
    if args.output_json:
        output_path = Path(args.output_json)
    else:
        output_path = report_path.with_name(f"xuan_completion_gap_{report_path.stem}.json")
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
