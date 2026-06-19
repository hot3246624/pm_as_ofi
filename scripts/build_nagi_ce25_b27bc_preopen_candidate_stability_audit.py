#!/usr/bin/env python3
"""Build a local-only stability audit for pre-open support candidates.

The audit reads existing no-order maker-shadow artifacts. It does not fetch
data, import credentials, connect to Polymarket, or execute orders. Public
activity remains a queue proxy only; own maker fill truth is not inferred.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


STATUS = (
    "KEEP_NAGI_CE25_B27BC_PREOPEN_CANDIDATE_STABILITY_AUDIT_REVIEWED_"
    "SOURCE_EXPANSION_REQUIRED_PRIVATE_TELEMETRY_REQUIRED_NOT_READY"
)
BLOCKED_STATUS = (
    "BLOCKED_NAGI_CE25_B27BC_PREOPEN_CANDIDATE_STABILITY_AUDIT_NO_PASSING_PROXY_CANDIDATE"
)
WALLET_RE = re.compile(r"0x[a-fA-F0-9]{40}")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return [dict(row) for row in csv.DictReader(f)]


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def number(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def event_path(run_dir: Path) -> Path:
    return run_dir / "nagi_ce25_b27bc_maker_shadow_events.jsonl"


def summary_path(run_dir: Path) -> Path:
    return run_dir / "rolling_24h_summary.csv"


def decision_path(run_dir: Path) -> Path:
    return run_dir / "decision_register.json"


def non_claims_clean(*items: dict[str, Any]) -> bool:
    for item in items:
        non_claims = item.get("non_claims") or {}
        if any(bool(value) for value in non_claims.values()):
            return False
    return True


def wallet_from_source_path(path: str) -> str:
    match = WALLET_RE.search(path or "")
    return match.group(0).lower() if match else "unknown"


def row_side(row: dict[str, str]) -> str:
    value = str(row.get("side") or row.get("outcome") or "").upper()
    if value in {"YES", "UP"}:
        return "YES"
    if value in {"NO", "DOWN"}:
        return "NO"
    return ""


def row_price(row: dict[str, str], side: str) -> float | None:
    names = ["yes_bid", "public_trade_px", "price"] if side == "YES" else ["no_bid", "public_trade_px", "price"]
    for name in names:
        value = number(row.get(name))
        if value is not None:
            return value
    return None


def price_key(value: Any) -> str:
    numeric = number(value)
    return "" if numeric is None else f"{numeric:.8f}"


def build_input_index(input_csv: Path | None) -> tuple[dict[tuple[str, str, str, str], Counter[str]], dict[str, Counter[str]]]:
    exact: dict[tuple[str, str, str, str], Counter[str]] = defaultdict(Counter)
    by_slug: dict[str, Counter[str]] = defaultdict(Counter)
    if input_csv is None or not input_csv.exists():
        return exact, by_slug
    for row in read_csv(input_csv):
        slug = str(row.get("slug") or row.get("market_slug") or "")
        side = row_side(row)
        ts_ms = str(row.get("ts_ms") or row.get("timestamp_ms") or "")
        px = row_price(row, side)
        wallet = wallet_from_source_path(str(row.get("source_path") or ""))
        if slug:
            by_slug[slug][wallet] += 1
        if slug and side and ts_ms and px is not None:
            exact[(slug, ts_ms, side, price_key(px))][wallet] += 1
    return exact, by_slug


def dominant_wallet(counter: Counter[str]) -> str:
    if not counter:
        return "unknown"
    return counter.most_common(1)[0][0]


def event_wallet(event: dict[str, Any], exact: dict[tuple[str, str, str, str], Counter[str]], by_slug: dict[str, Counter[str]]) -> str:
    slug = str(event.get("slug") or "")
    side = str(event.get("side") or "")
    ts_ms = str(event.get("ts_ms") or "")
    px = price_key(event.get("maker_bid_px"))
    key = (slug, ts_ms, side, px)
    if key in exact:
        return dominant_wallet(exact[key])
    return dominant_wallet(by_slug.get(slug, Counter()))


def pct(value: float | None) -> float | None:
    return None if value is None else round(value * 100.0, 6)


def summarize_run(run_dir: Path, *, label: str, min_open_markets: int, min_windows: int) -> dict[str, Any]:
    decision = read_json(decision_path(run_dir))
    events = read_jsonl(event_path(run_dir))
    summaries = read_csv(summary_path(run_dir))
    input_csv_raw = decision.get("input_csv")
    input_csv = Path(input_csv_raw) if input_csv_raw else None
    exact, by_slug = build_input_index(input_csv)

    event_counts = Counter(str(event.get("event") or "unknown") for event in events)
    source_wallet_split: Counter[str] = Counter()
    open_events = [event for event in events if event.get("event") == "queue_proxy_open"]
    for event in open_events:
        source_wallet_split[event_wallet(event, exact, by_slug)] += 1

    residual_by_slug: Counter[str] = Counter()
    residual_by_side: Counter[str] = Counter()
    for event in events:
        if event.get("event") != "residual_finalized":
            continue
        cost = number(event.get("residual_cost"), 0.0) or 0.0
        if cost <= 0:
            continue
        residual_by_slug[str(event.get("slug") or "unknown")] += cost
        residual_by_side[str(event.get("first_side") or "unknown")] += cost

    total_residual = sum(residual_by_slug.values())
    top_residual = residual_by_slug.most_common(5)
    top1_residual_share = (top_residual[0][1] / total_residual) if total_residual else None
    rolling_rows = [row for row in summaries if row.get("window_key") != "ALL"]
    overall = decision.get("overall") or {}
    limit_min = number(overall.get("limit_order_min_shares"), number((decision.get("config") or {}).get("min_shadow_qty"), 5.0)) or 5.0
    open_qty_violations = [
        event
        for event in open_events
        if (number(event.get("shadow_qty"), 0.0) or 0.0) < limit_min
    ]
    market_orders_used = False
    queue_open_markets = int(number(overall.get("queue_proxy_open_markets"), 0.0) or 0)
    rolling_window_count = len(rolling_rows)
    sample_width_ok = queue_open_markets >= min_open_markets and rolling_window_count >= min_windows

    return {
        "label": label,
        "run_dir": str(run_dir),
        "status": decision.get("status"),
        "input_csv": str(input_csv) if input_csv else None,
        "overall": overall,
        "config": decision.get("config") or {},
        "blockers": decision.get("blockers") or [],
        "non_claims": decision.get("non_claims") or {},
        "event_counts": dict(event_counts.most_common()),
        "source_wallet_split_queue_proxy_open": dict(source_wallet_split.most_common()),
        "rolling_window_count": rolling_window_count,
        "sample_width_ok": sample_width_ok,
        "sample_width_thresholds": {
            "min_queue_proxy_open_markets": min_open_markets,
            "min_rolling_windows": min_windows,
        },
        "residual_concentration": {
            "total_residual_cost": round(total_residual, 8),
            "top1_residual_cost_share": top1_residual_share,
            "top_residual_markets": [
                {"slug": slug, "residual_cost": round(cost, 8), "share": (cost / total_residual if total_residual else None)}
                for slug, cost in top_residual
            ],
            "residual_cost_by_first_side": {side: round(cost, 8) for side, cost in residual_by_side.most_common()},
        },
        "order_minimum_compliance": {
            "limit_order_min_shares": limit_min,
            "market_order_min_usdc": number(overall.get("market_order_min_usdc"), 1.0),
            "market_orders_used": market_orders_used,
            "queue_proxy_open_count": len(open_events),
            "open_qty_below_limit_count": len(open_qty_violations),
            "insufficient_depth_events": event_counts.get("queue_proxy_touch_insufficient_depth", 0),
            "passes": len(open_qty_violations) == 0 and not market_orders_used,
        },
    }


def passing_grid_rows(grid_results_csv: Path) -> list[dict[str, str]]:
    return [row for row in read_csv(grid_results_csv) if truthy(row.get("passes_gate"))]


def build_decision(args: argparse.Namespace) -> dict[str, Any]:
    grid_results = Path(args.grid_results_csv)
    best_roi_dir = Path(args.best_roi_run_dir)
    best_residual_dir = Path(args.best_residual_run_dir)
    passing = passing_grid_rows(grid_results)
    runs = [
        summarize_run(
            best_roi_dir,
            label="best_roi",
            min_open_markets=args.min_queue_proxy_open_markets,
            min_windows=args.min_rolling_windows,
        ),
        summarize_run(
            best_residual_dir,
            label="best_residual",
            min_open_markets=args.min_queue_proxy_open_markets,
            min_windows=args.min_rolling_windows,
        ),
    ]
    clean_non_claims = non_claims_clean(*runs)
    best_residual_rate = min(
        (number((run.get("overall") or {}).get("resid_rate"), 999.0) or 999.0 for run in runs),
        default=999.0,
    )
    best_roi = max(
        (number((run.get("overall") or {}).get("roi_proxy_conservative"), -999.0) or -999.0 for run in runs),
        default=-999.0,
    )
    order_minimum_ok = all(run["order_minimum_compliance"]["passes"] for run in runs)
    sample_width_ok = all(run["sample_width_ok"] for run in runs)
    has_positive_proxy = bool(passing) and best_residual_rate <= args.residual_rate_target and best_roi > 0
    worth_source_expansion = has_positive_proxy and order_minimum_ok and clean_non_claims and not sample_width_ok
    candidate_narrow = has_positive_proxy and not sample_width_ok
    blockers = [
        "own_maker_telemetry_missing",
        "maker_fill_truth_not_proven",
        "private_queue_priority_not_observed",
    ]
    if not sample_width_ok:
        blockers.extend(["queue_proxy_open_market_sample_below_review_floor", "rolling_window_count_below_review_floor"])
    if not order_minimum_ok:
        blockers.append("order_minimum_violation")
    if not clean_non_claims:
        blockers.append("non_claims_flipped_true")
    if not has_positive_proxy:
        status = BLOCKED_STATUS
        next_action = "stop_preopen_public_proxy_or_add_new_source_truth_input"
    else:
        status = STATUS
        next_action = (
            "expand_public_or_source_truth_input_for_preopen_support_candidate"
            if worth_source_expansion
            else "collect_own_maker_telemetry_sample_before_any_private_truth_claim"
        )
    return {
        "generated_at": utc_now(),
        "status": status,
        "evidence_level": "research_proxy",
        "strategy_family": "NAGI-style maker queue + CE25 primary 35-65 coverage + pre-open opposite-side support",
        "source_artifacts": {
            "grid_results_csv": str(grid_results),
            "best_roi_run_dir": str(best_roi_dir),
            "best_residual_run_dir": str(best_residual_dir),
        },
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "passing_grid_row_count": len(passing),
        "best_residual_rate": best_residual_rate,
        "best_roi_proxy": best_roi,
        "candidate_narrow_public_proxy_artifact": candidate_narrow,
        "worth_another_public_or_source_truth_expansion": worth_source_expansion,
        "sample_width_ok": sample_width_ok,
        "order_minimum_ok": order_minimum_ok,
        "blockers": blockers,
        "runs": runs,
        "decision": {
            "next_executable_action": next_action,
            "highest_allowed_status": "local research/no-order maker-shadow proxy, not OOS-ready or live-ready",
        },
    }


def format_pct(value: Any) -> str:
    numeric = number(value)
    return "n/a" if numeric is None else f"{numeric * 100:.4f}%"


def write_report(path: Path, decision: dict[str, Any]) -> None:
    lines = [
        "# NAGI + CE25 + B27BC Pre-Open Candidate Stability Audit",
        "",
        f"Generated at: `{decision['generated_at']}`",
        "",
        "## Decision",
        "",
        f"- Status: `{decision['status']}`",
        f"- Evidence level: `{decision['evidence_level']}`",
        f"- Best residual rate: `{format_pct(decision['best_residual_rate'])}`",
        f"- Best ROI proxy: `{format_pct(decision['best_roi_proxy'])}`",
        f"- Passing grid rows: `{decision['passing_grid_row_count']}`",
        f"- Narrow public proxy artifact: `{decision['candidate_narrow_public_proxy_artifact']}`",
        f"- Worth another public/source-truth expansion: `{decision['worth_another_public_or_source_truth_expansion']}`",
        "",
        "## Candidate Runs",
        "",
    ]
    for run in decision["runs"]:
        overall = run["overall"]
        residual = run["residual_concentration"]
        order_min = run["order_minimum_compliance"]
        lines.extend(
            [
                f"### `{run['label']}`",
                "",
                f"- Run: `{run['run_dir']}`",
                f"- ROI proxy: `{format_pct(overall.get('roi_proxy_conservative'))}`",
                f"- Residual rate: `{format_pct(overall.get('resid_rate'))}`",
                f"- Pair cost: `{overall.get('pair_cost')}`",
                f"- Queue proxy open markets: `{overall.get('queue_proxy_open_markets')}`",
                f"- Queue proxy opens/closes: `{overall.get('queue_proxy_opens')}` / `{overall.get('queue_proxy_closes')}`",
                f"- Rolling windows: `{run['rolling_window_count']}`",
                f"- Source wallet split on opens: `{run['source_wallet_split_queue_proxy_open']}`",
                f"- Residual top1 cost share: `{format_pct(residual.get('top1_residual_cost_share'))}`",
                f"- Order minimum passes: `{order_min['passes']}`; open qty below 5 shares: `{order_min['open_qty_below_limit_count']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Blockers",
            "",
        ]
    )
    for blocker in decision["blockers"]:
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Non-Claims",
            "",
            "This packet is local research only. It does not prove own maker fill, queue priority, private execution truth, OOS readiness, canary readiness, or live readiness.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--grid-results-csv", required=True)
    p.add_argument("--best-roi-run-dir", required=True)
    p.add_argument("--best-residual-run-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--residual-rate-target", type=float, default=0.12)
    p.add_argument("--min-queue-proxy-open-markets", type=int, default=25)
    p.add_argument("--min-rolling-windows", type=int, default=3)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    decision = build_decision(args)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_json(out / "decision_register.json", decision)
    write_report(out / "PREOPEN_CANDIDATE_STABILITY_AUDIT_REPORT.md", decision)
    print(
        json.dumps(
            {
                "status": decision["status"],
                "output_dir": str(out),
                "passing_grid_row_count": decision["passing_grid_row_count"],
                "best_residual_rate": decision["best_residual_rate"],
                "best_roi_proxy": decision["best_roi_proxy"],
                "worth_another_public_or_source_truth_expansion": decision[
                    "worth_another_public_or_source_truth_expansion"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
