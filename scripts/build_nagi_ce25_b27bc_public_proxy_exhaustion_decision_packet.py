#!/usr/bin/env python3
"""Build a local public-proxy exhaustion decision packet.

The packet consolidates bounded no-order maker-shadow proxy families and marks
the public-proxy tuning lane as exhausted when all local families miss the
residual target.  It reads existing local artifacts only; it does not fetch
data, import credentials, connect to Polymarket, or execute orders.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATUS = "BLOCKED_NAGI_CE25_B27BC_PUBLIC_PROXY_EXHAUSTION_PRIVATE_TELEMETRY_OR_SOURCE_TRUTH_REQUIRED"
NEXT_ALLOWED_ACTIONS = [
    "collect_own_maker_telemetry_sample",
    "add_new_source_truth_input",
    "stop_public_proxy_tuning",
]
DEFAULT_FAMILY_DIRS = {
    "residual_failure_audit": [
        "data/exports/nagi_ce25_b27bc_residual_failure_audit_20260608T1120Z"
    ],
    "residual_guard_grid": [
        "data/exports/nagi_ce25_b27bc_residual_guard_grid_20260608T1135Z_profiles"
    ],
    "pre_open_support_grid": [
        "data/exports/nagi_ce25_b27bc_pre_open_support_grid_20260608T1148Z"
    ],
    "per_market_budget_grid": [
        "data/exports/nagi_ce25_b27bc_per_market_residual_budget_grid_20260608T1220Z",
        "data/exports/nagi_ce25_b27bc_per_market_residual_budget_grid_20260608T1228Z_lowcost",
        "data/exports/nagi_ce25_b27bc_per_market_residual_budget_grid_20260608T1235Z_ultralowcost",
    ],
    "first_side_hazard_grid": [
        "data/exports/nagi_ce25_b27bc_first_side_hazard_grid_20260608T121603Z"
    ],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return [dict(row) for row in csv.DictReader(f)]


def number(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def compact_candidate(row: dict[str, Any] | None, *, family: str, artifact_dir: Path) -> dict[str, Any] | None:
    if not row:
        return None
    keys = [
        "variant_id",
        "status",
        "roi_proxy_conservative",
        "resid_rate",
        "bad_pc_ge_100_share",
        "observed_markets",
        "queue_proxy_open_markets",
        "queue_proxy_market_coverage",
        "queue_proxy_opens",
        "queue_proxy_closes",
        "buy_actual_proxy",
        "pair_cost",
        "pair_pnl_proxy",
        "residual_cost",
        "residual_timeouts",
        "max_open_cost_usdc",
        "pair_cost_cap",
        "coverage_profile",
        "side_profile",
        "passes_gate",
    ]
    compact = {key: row.get(key) for key in keys if key in row}
    compact["family"] = family
    compact["artifact_dir"] = str(artifact_dir)
    return compact


def family_summary(family: str, artifact_dir: Path) -> dict[str, Any]:
    decision_path = artifact_dir / "decision_register.json"
    grid_path = artifact_dir / "grid_results.csv"
    decision = read_json(decision_path)
    grid_rows = read_csv_rows(grid_path)
    best_by_residual = decision.get("best_by_residual")
    best_by_roi = decision.get("best_by_roi")
    if grid_rows:
        numeric_residual_rows = [row for row in grid_rows if number(row.get("resid_rate")) is not None]
        numeric_roi_rows = [row for row in grid_rows if number(row.get("roi_proxy_conservative")) is not None]
        if numeric_residual_rows:
            best_by_residual = min(numeric_residual_rows, key=lambda row: number(row.get("resid_rate"), 999.0) or 999.0)
        if numeric_roi_rows:
            best_by_roi = max(
                numeric_roi_rows,
                key=lambda row: number(row.get("roi_proxy_conservative"), -999.0) or -999.0,
            )
    return {
        "family": family,
        "artifact_dir": str(artifact_dir),
        "status": decision.get("status"),
        "variant_count": decision.get("variant_count"),
        "pass_count": decision.get("pass_count"),
        "non_claims": decision.get("non_claims"),
        "best_by_residual": compact_candidate(best_by_residual, family=family, artifact_dir=artifact_dir),
        "best_by_roi": compact_candidate(best_by_roi, family=family, artifact_dir=artifact_dir),
        "grid_rows": len(grid_rows),
    }


def load_families(family_dirs: dict[str, list[str]]) -> list[dict[str, Any]]:
    summaries = []
    for family, dirs in family_dirs.items():
        for directory in dirs:
            path = Path(directory)
            if not (path / "decision_register.json").exists():
                raise FileNotFoundError(f"missing decision_register.json for {family}: {path}")
            summaries.append(family_summary(family, path))
    return summaries


def candidate_metric(candidate: dict[str, Any] | None, key: str, default: float) -> float:
    if not candidate:
        return default
    value = number(candidate.get(key))
    return default if value is None else value


def best_candidates(summaries: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    residual_candidates = [summary.get("best_by_residual") for summary in summaries if summary.get("best_by_residual")]
    roi_candidates = [summary.get("best_by_roi") for summary in summaries if summary.get("best_by_roi")]
    best_residual = min(
        residual_candidates,
        key=lambda row: candidate_metric(row, "resid_rate", 999.0),
        default=None,
    )
    best_roi = max(
        roi_candidates,
        key=lambda row: candidate_metric(row, "roi_proxy_conservative", -999.0),
        default=None,
    )
    return best_residual, best_roi


def format_pct(value: Any) -> str:
    numeric = number(value)
    if numeric is None:
        return "n/a"
    return f"{numeric * 100:.4f}%"


def write_report(path: Path, decision: dict[str, Any]) -> None:
    best = decision["best_public_proxy_candidate"]
    roi = decision["best_roi_tradeoff"]
    lines = [
        "# NAGI + CE25 + B27BC Public Proxy Exhaustion Decision",
        "",
        f"Generated at: `{decision['generated_at']}`",
        "",
        "## Decision",
        "",
        f"- Status: `{decision['status']}`",
        f"- Public proxy families exhausted: `{decision['public_proxy_families_exhausted']}`",
        f"- Evidence level: `{decision['evidence_level']}`",
        "- No live/private/order claims are made.",
        "",
        "## Best Residual Candidate",
        "",
        f"- Family: `{best.get('family')}`",
        f"- Variant: `{best.get('variant_id')}`",
        f"- Residual rate: `{format_pct(best.get('resid_rate'))}`",
        f"- ROI proxy: `{format_pct(best.get('roi_proxy_conservative'))}`",
        f"- Residual target miss: `{decision['residual_target_miss_bps']:.2f}` bps",
        f"- Queue proxy markets: `{best.get('queue_proxy_open_markets')}`",
        f"- Artifact: `{best.get('artifact_dir')}`",
        "",
        "## Best ROI Tradeoff",
        "",
        f"- Family: `{roi.get('family')}`",
        f"- Variant: `{roi.get('variant_id')}`",
        f"- ROI proxy: `{format_pct(roi.get('roi_proxy_conservative'))}`",
        f"- Residual rate: `{format_pct(roi.get('resid_rate'))}`",
        f"- Artifact: `{roi.get('artifact_dir')}`",
        "",
        "## Exhausted Public Proxy Families",
        "",
    ]
    for summary in decision["family_summaries"]:
        lines.append(
            f"- `{summary['family']}` from `{summary['artifact_dir']}`: "
            f"status `{summary.get('status')}`, variants `{summary.get('variant_count')}`, "
            f"pass_count `{summary.get('pass_count')}`"
        )
    lines.extend(
        [
            "",
            "## Order Minimum Boundary",
            "",
            "- Limit/post-only maker shadow requires at least `5` shares.",
            "- Market order minimum is tracked as `1` USDC, but market orders are not used by this maker-only proxy.",
            "- Public SELL touch or visible/proxy depth below 5 shares is insufficient depth, not queue proxy open.",
            "",
            "## Next Allowed Actions",
            "",
        ]
    )
    for action in decision["next_allowed_actions"]:
        lines.append(f"- `{action}`")
    lines.extend(
        [
            "",
            "## Non-Claims",
            "",
            "The packet remains local research only. It does not prove own maker fill, queue priority, private execution truth, OOS readiness, canary readiness, or live readiness.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--residual-rate-target", type=float, default=0.12)
    p.add_argument("--family-dir", action="append", default=[], help="family_name=path")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    family_dirs = {family: list(paths) for family, paths in DEFAULT_FAMILY_DIRS.items()}
    for item in args.family_dir:
        if "=" not in item:
            raise ValueError("--family-dir must be family_name=path")
        family, path = item.split("=", 1)
        family_dirs.setdefault(family, []).append(path)
    summaries = load_families(family_dirs)
    best_residual, best_roi = best_candidates(summaries)
    if best_residual is None:
        raise RuntimeError("no residual candidate found")
    best_resid_rate = candidate_metric(best_residual, "resid_rate", 999.0)
    best_roi_rate = candidate_metric(best_roi, "roi_proxy_conservative", -999.0)
    residual_target_miss_bps = max(0.0, (best_resid_rate - args.residual_rate_target) * 10000.0)
    any_pass = any(int(summary.get("pass_count") or 0) > 0 for summary in summaries)
    decision = {
        "generated_at": utc_now(),
        "status": STATUS,
        "evidence_level": "research_proxy",
        "public_proxy_families_exhausted": not any_pass and best_resid_rate > args.residual_rate_target,
        "no_live_or_private_claims": True,
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "residual_rate_target": args.residual_rate_target,
        "best_public_proxy_candidate": best_residual,
        "best_residual_rate": best_resid_rate,
        "best_roi_tradeoff": best_roi,
        "best_roi_proxy": best_roi_rate,
        "residual_target_miss_bps": residual_target_miss_bps,
        "next_allowed_actions": NEXT_ALLOWED_ACTIONS,
        "order_minimum_boundary": {
            "limit_post_only_maker_min_shares": 5.0,
            "market_order_min_usdc": 1.0,
            "market_orders_used": False,
        },
        "family_summaries": summaries,
    }
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "decision_register.json").write_text(
        json.dumps(decision, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_report(out / "PUBLIC_PROXY_EXHAUSTION_DECISION_REPORT.md", decision)
    print(
        json.dumps(
            {
                "status": STATUS,
                "output_dir": str(out),
                "public_proxy_families_exhausted": decision["public_proxy_families_exhausted"],
                "best_residual_rate": best_resid_rate,
                "best_roi_proxy": best_roi_rate,
                "residual_target_miss_bps": residual_target_miss_bps,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
