#!/usr/bin/env python3
"""Build a local-only residual failure audit for maker-shadow proxy runs.

The audit reads existing no-order maker-shadow artifacts, compares recent
decision registers and event logs, and writes a compact failure register.  It
does not fetch data, import credentials, connect to Polymarket, or execute any
orders.  Public account activity remains a research proxy only.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


REVIEW_STATUS = (
    "KEEP_NAGI_CE25_B27BC_RESIDUAL_FAILURE_AUDIT_REVIEWED_RESEARCH_PROXY_NOT_READY"
)
BLOCKED_STATUS = (
    "BLOCKED_NAGI_CE25_B27BC_RESIDUAL_FAILURE_AUDIT_RESIDUAL_DOMINATES_NOT_READY"
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def to_float(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def discover_run_dirs(limit: int) -> list[Path]:
    root = Path("data/exports")
    paths = sorted(root.glob("nagi_ce25_b27bc_maker_shadow_run_*/decision_register.json"))
    return [p.parent for p in paths[-limit:]]


def parse_event_log(path: Path) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    coverage_counts: dict[str, Counter[str]] = defaultdict(Counter)
    residual_by_risk: dict[str, dict[str, float]] = defaultdict(
        lambda: {"count": 0.0, "residual_cost": 0.0, "residual_qty": 0.0}
    )
    completion_pair_costs: list[float] = []
    first_side_residual_counts: Counter[str] = Counter()
    residual_cost_total = 0.0
    residual_qty_total = 0.0

    if not path.exists():
        return {
            "event_counts": {},
            "coverage_event_counts": {},
            "residual_by_risk": {},
            "completion_rejected_pair_cost_avg": None,
            "completion_rejected_pair_cost_max": None,
            "first_side_residual_counts": {},
            "residual_event_cost_total": 0.0,
            "residual_event_qty_total": 0.0,
        }

    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            event = str(row.get("event") or "unknown")
            counts[event] += 1
            coverage_gate = str(row.get("coverage_gate_id") or "")
            if coverage_gate:
                coverage_counts[coverage_gate][event] += 1

            if event in {"residual_discount", "residual_finalized"}:
                risk = str(row.get("risk_flag") or "unknown")
                cost = to_float(row.get("residual_cost")) or 0.0
                qty = to_float(row.get("residual_qty")) or 0.0
                residual_by_risk[risk]["count"] += 1.0
                residual_by_risk[risk]["residual_cost"] += cost
                residual_by_risk[risk]["residual_qty"] += qty
                residual_cost_total += cost
                residual_qty_total += qty
                first_side = str(row.get("first_side") or "unknown")
                first_side_residual_counts[first_side] += 1

            if event == "completion_rejected_pair_cost":
                pair_cost = to_float(row.get("pair_cost"))
                if pair_cost is not None:
                    completion_pair_costs.append(pair_cost)

    return {
        "event_counts": dict(counts),
        "coverage_event_counts": {
            gate: dict(counter) for gate, counter in sorted(coverage_counts.items())
        },
        "residual_by_risk": {
            risk: {
                "count": int(values["count"]),
                "residual_cost": values["residual_cost"],
                "residual_qty": values["residual_qty"],
            }
            for risk, values in sorted(residual_by_risk.items())
        },
        "completion_rejected_pair_cost_avg": (
            sum(completion_pair_costs) / len(completion_pair_costs)
            if completion_pair_costs
            else None
        ),
        "completion_rejected_pair_cost_max": max(completion_pair_costs)
        if completion_pair_costs
        else None,
        "first_side_residual_counts": dict(first_side_residual_counts),
        "residual_event_cost_total": residual_cost_total,
        "residual_event_qty_total": residual_qty_total,
    }


def load_run(run_dir: Path) -> dict[str, Any]:
    decision_path = run_dir / "decision_register.json"
    if not decision_path.exists():
        raise FileNotFoundError(f"missing decision_register.json under {run_dir}")
    decision = load_json(decision_path)
    overall = decision.get("overall") or {}
    events = parse_event_log(run_dir / "nagi_ce25_b27bc_maker_shadow_events.jsonl")
    label = run_dir.name.removeprefix("nagi_ce25_b27bc_maker_shadow_run_")
    return {
        "run_label": label,
        "run_dir": str(run_dir),
        "decision_path": str(decision_path),
        "status": decision.get("status"),
        "non_claims": decision.get("non_claims") or {},
        "blockers": decision.get("blockers") or [],
        "overall": overall,
        "events": events,
    }


def summary_row(run: dict[str, Any]) -> dict[str, Any]:
    overall = run["overall"]
    events = run["events"]
    event_counts = events["event_counts"]
    return {
        "run_label": run["run_label"],
        "observed_markets": overall.get("observed_markets"),
        "events": overall.get("events"),
        "opportunities": overall.get("opportunities"),
        "queue_proxy_opens": overall.get("queue_proxy_opens"),
        "queue_proxy_closes": overall.get("queue_proxy_closes"),
        "pair_cost": overall.get("pair_cost"),
        "pair_pnl_proxy": overall.get("pair_pnl_proxy"),
        "residual_cost": overall.get("residual_cost"),
        "cash_pnl_proxy_conservative": overall.get("cash_pnl_proxy_conservative"),
        "roi_proxy_conservative": overall.get("roi_proxy_conservative"),
        "bad_pc_ge_100_share": overall.get("bad_pc_ge_100_share"),
        "resid_rate": overall.get("resid_rate"),
        "residual_timeouts": overall.get("residual_timeouts"),
        "residual_discount_events": overall.get("residual_discount_events"),
        "up_first_down_residual_risk_events": overall.get("up_first_down_residual_risk_events"),
        "completion_rejected_pair_cost": event_counts.get("completion_rejected_pair_cost", 0),
        "queue_proxy_touch_insufficient_depth": event_counts.get(
            "queue_proxy_touch_insufficient_depth", 0
        ),
        "open_skipped_active_residual": event_counts.get("open_skipped_active_residual", 0),
        "residual_event_cost_total": events.get("residual_event_cost_total"),
        "residual_event_qty_total": events.get("residual_event_qty_total"),
    }


def delta(latest: dict[str, Any], first: dict[str, Any], key: str) -> float | None:
    a = to_float(latest.get(key))
    b = to_float(first.get(key))
    if a is None or b is None:
        return None
    return a - b


def build_failure_register(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    first = rows[0]
    latest = rows[-1]
    pair_pnl = to_float(latest.get("pair_pnl_proxy")) or 0.0
    residual_cost = to_float(latest.get("residual_cost")) or 0.0
    roi = to_float(latest.get("roi_proxy_conservative")) or 0.0
    resid_rate = to_float(latest.get("resid_rate")) or 0.0
    causes: list[dict[str, Any]] = []

    if residual_cost >= pair_pnl and pair_pnl > 0:
        causes.append(
            {
                "cause": "residual_cost_overwhelms_pair_edge",
                "severity": "critical",
                "evidence": (
                    f"latest residual_cost={residual_cost:.6f} >= "
                    f"pair_pnl_proxy={pair_pnl:.6f}"
                ),
                "action": "prioritize residual suppression before any coverage expansion",
            }
        )
    elif residual_cost >= 0.9 * pair_pnl and pair_pnl > 0:
        causes.append(
            {
                "cause": "residual_cost_consumes_most_pair_edge",
                "severity": "high",
                "evidence": (
                    f"latest residual_cost={residual_cost:.6f} is "
                    f"{residual_cost / pair_pnl:.2%} of pair_pnl_proxy"
                ),
                "action": "audit residual origins and timeout/discount rules",
            }
        )

    if roi <= 0:
        causes.append(
            {
                "cause": "conservative_proxy_pnl_negative",
                "severity": "critical",
                "evidence": f"latest roi_proxy_conservative={roi:.6%}",
                "action": "block promotion and run only local no-order failure experiments",
            }
        )

    if resid_rate > 0.12:
        causes.append(
            {
                "cause": "residual_rate_above_target",
                "severity": "critical" if resid_rate >= 0.20 else "high",
                "evidence": f"latest resid_rate={resid_rate:.6%} target<=12%",
                "action": "test stricter residual timeout and first-side risk controls",
            }
        )

    resid_rate_delta = delta(latest, first, "resid_rate")
    if resid_rate_delta is not None and resid_rate_delta > 0:
        causes.append(
            {
                "cause": "residual_rate_worsening_across_runs",
                "severity": "high",
                "evidence": f"resid_rate delta first_to_latest={resid_rate_delta:.6%}",
                "action": "compare event mix drift before accepting new public-profile fetches",
            }
        )

    up_risk_delta = delta(latest, first, "up_first_down_residual_risk_events")
    if up_risk_delta is not None and up_risk_delta > 0:
        causes.append(
            {
                "cause": "up_first_down_residual_risk_accelerating",
                "severity": "high",
                "evidence": (
                    "up_first_down_residual_risk_events "
                    f"{first.get('up_first_down_residual_risk_events')} -> "
                    f"{latest.get('up_first_down_residual_risk_events')}"
                ),
                "action": "evaluate a no-order UP-first suppressor or DOWN-first priority guard",
            }
        )

    pair_rejects = to_float(latest.get("completion_rejected_pair_cost")) or 0.0
    closes = to_float(latest.get("queue_proxy_closes")) or 0.0
    if closes > 0 and pair_rejects > 2 * closes:
        causes.append(
            {
                "cause": "pair_cost_cap_rejects_many_repair_rows",
                "severity": "medium",
                "evidence": (
                    f"completion_rejected_pair_cost={int(pair_rejects)} "
                    f"vs queue_proxy_closes={int(closes)}"
                ),
                "action": "audit whether rejected repair rows are protecting PnL or trapping residual",
            }
        )

    insufficient_depth = to_float(latest.get("queue_proxy_touch_insufficient_depth")) or 0.0
    opens = to_float(latest.get("queue_proxy_opens")) or 0.0
    if opens > 0 and insufficient_depth > opens:
        causes.append(
            {
                "cause": "five_share_depth_floor_blocks_many_queue_touches",
                "severity": "medium",
                "evidence": (
                    f"insufficient_depth_touches={int(insufficient_depth)} "
                    f"vs queue_proxy_opens={int(opens)}"
                ),
                "action": "keep 5-share limit floor; do not count sub-floor touches as queue opens",
            }
        )

    return causes


def build_decision_register(runs: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [summary_row(run) for run in runs]
    failures = build_failure_register(rows)
    latest = rows[-1] if rows else {}
    status = BLOCKED_STATUS if any(f["severity"] == "critical" for f in failures) else REVIEW_STATUS
    latest_non_claims = runs[-1].get("non_claims", {}) if runs else {}
    non_claims_safe = {
        "ready": False,
        "private_truth": False,
        "maker_fill_truth": False,
        "order_execution": False,
        "canary": False,
        "live": False,
    }
    non_claim_violations = [
        key for key, expected in non_claims_safe.items() if latest_non_claims.get(key) != expected
    ]
    next_action = (
        "run_local_no_order_residual_guard_grid_on_existing_materialized_inputs"
        if status == BLOCKED_STATUS
        else "continue_public_proxy_monitoring_with_failure_audit"
    )
    return {
        "generated_at": utc_now(),
        "status": status,
        "evidence_level": "research_proxy",
        "strategy_label": "NAGI-style no-order maker queue shadow + CE25 coverage gate + B27BC residual closer",
        "runs_compared": len(runs),
        "run_labels": [run["run_label"] for run in runs],
        "latest_run": runs[-1]["run_dir"] if runs else None,
        "non_claims": non_claims_safe,
        "non_claim_violations": non_claim_violations,
        "latest_metrics": latest,
        "failure_register": failures,
        "source_boundaries": {
            "public_account_activity": "research_proxy_only",
            "maker_fill_truth": "not_observed",
            "private_queue_priority": "not_observed",
            "orders_or_cancels": "not_used",
        },
        "next_executable_action": next_action,
        "recommended_no_order_experiment": {
            "name": "residual_guard_grid_v1",
            "scope": "local-only existing materialized inputs",
            "candidate_controls": [
                "UP-first suppressor / DOWN-first priority guard",
                "residual_discount_s in {15, 30}",
                "hard_timeout_s in {60, 90, 120, 180}",
                "pair_cost_cap in {0.95, 0.975, 0.995}",
                "block additional opens after residual_discount in the same market",
            ],
            "pass_conditions": [
                "resid_rate <= 0.12",
                "roi_proxy_conservative > 0",
                "bad_pc_ge_100_share <= 0.35",
                "no non_claims become true",
            ],
        },
    }


def write_markdown(path: Path, decision: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    latest = decision.get("latest_metrics") or {}
    failures = decision.get("failure_register") or []
    lines = [
        "# NAGI/CE25/B27BC Residual Failure Audit",
        "",
        f"Status: `{decision['status']}`",
        "",
        "This is a local no-order research-proxy audit. It does not prove maker fills, private queue priority, or readiness.",
        "",
        "## Latest Metrics",
        "",
        f"- ROI proxy conservative: `{latest.get('roi_proxy_conservative')}`",
        f"- Residual rate: `{latest.get('resid_rate')}`",
        f"- Pair PnL proxy: `{latest.get('pair_pnl_proxy')}`",
        f"- Residual cost: `{latest.get('residual_cost')}`",
        f"- UP-first/DOWN-residual risk events: `{latest.get('up_first_down_residual_risk_events')}`",
        "",
        "## Run Trend",
        "",
        "| run | ROI | residual rate | pair PnL | residual cost | UP-first risk |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {run_label} | {roi_proxy_conservative} | {resid_rate} | "
            "{pair_pnl_proxy} | {residual_cost} | {up_first_down_residual_risk_events} |".format(
                **row
            )
        )
    lines.extend(["", "## Failure Register", ""])
    for failure in failures:
        lines.append(
            f"- `{failure['severity']}` `{failure['cause']}`: {failure['evidence']}. "
            f"Next: {failure['action']}."
        )
    lines.extend(
        [
            "",
            "## Next Executable Review-Only Action",
            "",
            f"`{decision['next_executable_action']}`",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-dir", action="append", default=[], help="Maker-shadow run directory")
    p.add_argument("--latest", type=int, default=3, help="Default number of latest runs to audit")
    p.add_argument("--output-dir", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    run_dirs = [Path(p) for p in args.run_dir] if args.run_dir else discover_run_dirs(args.latest)
    if not run_dirs:
        raise SystemExit("no maker-shadow run directories found")
    runs = [load_run(path) for path in run_dirs]
    rows = [summary_row(run) for run in runs]
    decision = build_decision_register(runs)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(out / "run_trend_summary.csv", rows)
    event_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []
    for run in runs:
        for event, count in sorted(run["events"]["event_counts"].items()):
            event_rows.append({"run_label": run["run_label"], "event": event, "count": count})
        for risk, values in sorted(run["events"]["residual_by_risk"].items()):
            residual_rows.append({"run_label": run["run_label"], "risk_flag": risk, **values})
    write_csv(out / "event_breakdown.csv", event_rows)
    write_csv(out / "residual_risk_breakdown.csv", residual_rows)
    write_json(out / "decision_register.json", decision)
    write_markdown(out / "RESIDUAL_FAILURE_AUDIT.md", decision, rows)
    print(
        json.dumps(
            {
                "status": decision["status"],
                "output_dir": str(out),
                "decision_register": str(out / "decision_register.json"),
                "runs_compared": len(runs),
                "next_executable_action": decision["next_executable_action"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
