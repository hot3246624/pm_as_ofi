#!/usr/bin/env python3
"""Score/spec D+ portfolio-ledger + source-link + residual-tail intersection.

This is a local-only reader for allowlisted no-order shadow summary artifacts.
It consumes aggregate_report.json, *.summary.json, source-link score JSON, and
residual-tail exemplar score JSON. It does not read events JSONL, raw/replay
stores, sockets, SSH targets, or live order paths.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    "/raw/",
    "raw/replay",
    "/collector/",
    "collector/raw",
    ".events.jsonl",
    "shared-ingress",
)

PORTFOLIO_LEDGER_REQUIRED_FIELDS = (
    "condition_id",
    "slug",
    "seed_actions",
    "queue_supported_fills",
    "pair_actions",
    "pair_qty",
    "pair_cost_sum",
    "net_pair_cost_sum",
    "pair_pnl",
    "official_taker_fee_proxy",
    "residual_qty",
    "residual_cost",
    "residual_qty_by_side",
    "residual_cost_by_side",
    "residual_lot_count_by_side",
    "stress_cost_proxy",
    "conservative_risk_adjusted_proxy",
    "risk_adjusted_nonnegative",
    "risk_adjusted_bucket",
)

EXEMPLAR_REQUIRED_METRICS = (
    "required_field_coverage",
    "source_sequence_coverage",
    "quote_intent_coverage",
    "source_order_coverage",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    with path.open() as fh:
        obj = json.load(fh)
    if not isinstance(obj, dict):
        raise ValueError(f"{path} is not a JSON object")
    return obj


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def safe_ratio(num: float, den: float) -> float:
    if den <= 0.0:
        return 0.0
    return num / den


def path_is_safe(path: Path | None) -> bool:
    if path is None:
        return True
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def expand_summary_paths(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        if path.is_dir():
            out.extend(sorted(path.glob("*.summary.json")))
        else:
            out.append(path)
    return out


def event_lite(obj: dict[str, Any]) -> dict[str, Any]:
    value = obj.get("event_lite")
    return value if isinstance(value, dict) else {}


def portfolio_ledger(obj: dict[str, Any]) -> dict[str, Any] | None:
    value = event_lite(obj).get("portfolio_ledger_diagnostics")
    return value if isinstance(value, dict) else None


def offset_bucket(value: Any) -> str:
    offset = as_float(value, -1.0)
    if offset < 0.0:
        return "offset_unknown"
    if offset < 30.0:
        return "offset_0_30"
    if offset < 60.0:
        return "offset_30_60"
    if offset < 90.0:
        return "offset_60_90"
    if offset < 120.0:
        return "offset_90_120"
    return "offset_gte_120"


def ledger_bucket(value: dict[str, Any] | None) -> str:
    if not value:
        return "ledger_unknown"
    bucket = value.get("risk_adjusted_bucket")
    if bucket:
        return str(bucket)
    return "risk_adjusted_nonnegative" if bool(value.get("risk_adjusted_nonnegative")) else "risk_adjusted_negative"


def top_rows(rows: list[dict[str, Any]], key: str, limit: int = 8) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (-as_float(row.get(key)), str(row)))[:limit]


def load_portfolio_summaries(summary_paths: list[Path]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    by_condition: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    for path in summary_paths:
        obj = load_json(path)
        ledger = portfolio_ledger(obj)
        if ledger is None:
            failures.append({"path": str(path), "missing": "event_lite.portfolio_ledger_diagnostics"})
            continue
        missing = [field for field in PORTFOLIO_LEDGER_REQUIRED_FIELDS if field not in ledger]
        condition_id = str(ledger.get("condition_id") or obj.get("condition_id") or obj.get("slug") or path.stem)
        if missing:
            failures.append({"path": str(path), "condition_id": condition_id, "missing_fields": missing})
            continue
        by_condition[condition_id] = ledger
    return by_condition, failures


def source_link_ok(score: dict[str, Any]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if not score:
        return False, ["source_link_score file missing or empty"]
    if score.get("status") != "KEEP_SOURCE_LINK_TRANSITION_CROSS_BUCKET_SCORER_READY":
        failures.append(f"source_link_score.status={score.get('status')}")
    parity = score.get("aggregate_parity")
    if not isinstance(parity, dict) or not parity.get("passed"):
        failures.append("source_link_score.aggregate_parity.passed is not true")
    guardrails = score.get("guardrails")
    missing_cross = guardrails.get("missing_cross_bucket_fields") if isinstance(guardrails, dict) else None
    if missing_cross:
        failures.append(f"source_link missing_cross_bucket_fields={missing_cross}")
    return not failures, failures


def residual_tail_ok(score: dict[str, Any]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if not score:
        return False, ["residual_tail_score file missing or empty"]
    metrics = score.get("exemplar_metrics")
    if not isinstance(metrics, dict):
        return False, ["residual_tail_score.exemplar_metrics missing"]
    for field in EXEMPLAR_REQUIRED_METRICS:
        if as_float(metrics.get(field)) < 1.0:
            failures.append(f"residual_tail_score.exemplar_metrics.{field}={metrics.get(field)}")
    rankings = score.get("rankings")
    if not isinstance(rankings, dict) or not isinstance(rankings.get("top_residual_tail_exemplars"), list):
        failures.append("residual_tail_score.rankings.top_residual_tail_exemplars missing")
    return not failures, failures


def build_intersection(
    portfolio_by_condition: dict[str, dict[str, Any]],
    residual_tail_score: dict[str, Any],
) -> dict[str, Any]:
    rankings = residual_tail_score.get("rankings", {})
    exemplars = rankings.get("top_residual_tail_exemplars", []) if isinstance(rankings, dict) else []
    exemplar_rows = [row for row in exemplars if isinstance(row, dict)]
    total_residual_cost = sum(as_float(row.get("source_residual_cost")) for row in exemplar_rows)
    joined: list[dict[str, Any]] = []
    groups: dict[str, dict[str, Any]] = {}
    for row in exemplar_rows:
        condition_id = str(row.get("condition_id") or "")
        ledger = portfolio_by_condition.get(condition_id)
        if not ledger:
            continue
        residual_cost = as_float(row.get("source_residual_cost"))
        group_key = "|".join(
            [
                str(row.get("side", "UNKNOWN") or "UNKNOWN"),
                offset_bucket(row.get("offset_s")),
                str(row.get("source_risk_direction", "UNKNOWN") or "UNKNOWN"),
                ledger_bucket(ledger),
            ]
        )
        joined_row = {
            "condition_id": condition_id,
            "slug": row.get("slug"),
            "side": row.get("side"),
            "offset_bucket": offset_bucket(row.get("offset_s")),
            "source_risk_direction": row.get("source_risk_direction"),
            "source_residual_cost": round(residual_cost, 6),
            "source_pair_qty": round(as_float(row.get("source_pair_qty")), 6),
            "quote_intent_id": row.get("quote_intent_id"),
            "source_order_id": row.get("source_order_id"),
            "source_sequence_id": row.get("source_sequence_id"),
            "portfolio_risk_adjusted_bucket": ledger_bucket(ledger),
            "portfolio_conservative_risk_adjusted_proxy": round(
                as_float(ledger.get("conservative_risk_adjusted_proxy")), 6
            ),
            "portfolio_residual_cost": round(as_float(ledger.get("residual_cost")), 6),
            "portfolio_pair_pnl": round(as_float(ledger.get("pair_pnl")), 6),
        }
        joined.append(joined_row)
        group = groups.setdefault(
            group_key,
            {
                "source_side_offset_risk_direction_ledger": group_key,
                "row_count": 0,
                "source_residual_cost": 0.0,
                "source_pair_qty": 0.0,
                "conditions": set(),
            },
        )
        group["row_count"] += 1
        group["source_residual_cost"] += residual_cost
        group["source_pair_qty"] += as_float(row.get("source_pair_qty"))
        group["conditions"].add(condition_id)

    negative_joined_cost = sum(
        row["source_residual_cost"]
        for row in joined
        if row.get("portfolio_risk_adjusted_bucket") == "risk_adjusted_negative"
    )
    group_rows: list[dict[str, Any]] = []
    for group in groups.values():
        resid = as_float(group["source_residual_cost"])
        group_rows.append(
            {
                "source_side_offset_risk_direction_ledger": group["source_side_offset_risk_direction_ledger"],
                "row_count": group["row_count"],
                "condition_count": len(group["conditions"]),
                "source_residual_cost": round(resid, 6),
                "source_residual_cost_share": round(safe_ratio(resid, total_residual_cost), 6),
                "source_pair_qty": round(as_float(group["source_pair_qty"]), 6),
            }
        )
    group_rows.sort(key=lambda row: (-row["source_residual_cost"], row["source_side_offset_risk_direction_ledger"]))
    return {
        "exemplar_rows_considered": len(exemplar_rows),
        "portfolio_condition_count": len(portfolio_by_condition),
        "joined_exemplar_rows": len(joined),
        "joined_exemplar_share": round(safe_ratio(len(joined), len(exemplar_rows)), 6),
        "total_exemplar_source_residual_cost": round(total_residual_cost, 6),
        "risk_adjusted_negative_joined_residual_cost": round(negative_joined_cost, 6),
        "risk_adjusted_negative_joined_residual_cost_share": round(
            safe_ratio(negative_joined_cost, total_residual_cost), 6
        ),
        "top_intersection_groups": group_rows[:8],
        "top_joined_exemplars": top_rows(joined, "source_residual_cost", 8),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregate-report", type=Path, required=True)
    parser.add_argument("--summary", type=Path, action="append", default=[])
    parser.add_argument("--source-link-score", type=Path, required=True)
    parser.add_argument("--residual-tail-score", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    paths = [args.aggregate_report, args.source_link_score, args.residual_tail_score, args.output_dir] + args.summary
    unsafe = [str(path) for path in paths if not path_is_safe(path)]
    if unsafe:
        raise SystemExit(f"refusing forbidden input/output path(s): {unsafe}")

    aggregate = load_json(args.aggregate_report)
    source_score = load_json(args.source_link_score)
    residual_score = load_json(args.residual_tail_score)
    summary_paths = expand_summary_paths(args.summary)
    portfolio_by_condition, portfolio_failures = load_portfolio_summaries(summary_paths)
    aggregate_portfolio_present = portfolio_ledger(aggregate) is not None
    source_ok, source_failures = source_link_ok(source_score)
    residual_ok, residual_failures = residual_tail_ok(residual_score)

    missing: list[str] = []
    if not aggregate_portfolio_present:
        missing.append("aggregate_report.event_lite.portfolio_ledger_diagnostics")
    if not portfolio_by_condition:
        missing.append("summary.event_lite.portfolio_ledger_diagnostics for every supplied summary")
    if source_failures:
        missing.extend(source_failures)
    if residual_failures:
        missing.extend(residual_failures)

    intersection = build_intersection(portfolio_by_condition, residual_score) if residual_ok else {}
    top_group_share = 0.0
    if intersection.get("top_intersection_groups"):
        top_group_share = as_float(intersection["top_intersection_groups"][0].get("source_residual_cost_share"))

    if missing:
        status = "UNKNOWN_PORTFOLIO_LEDGER_SOURCE_LINK_INTERSECTION_INPUTS_MISSING"
        decision_label = "UNKNOWN"
        next_action = (
            "Run only the already selected exact diagnostic question after main-thread approval, with "
            "--portfolio-ledger-event-lite-summary, --source-link-transition-event-lite-summary, and "
            "--source-link-residual-tail-exemplars-event-lite-summary enabled together; then rescore this intersection."
        )
    elif as_float(intersection.get("risk_adjusted_negative_joined_residual_cost_share")) >= 0.20 and top_group_share >= 0.10:
        status = "KEEP_PORTFOLIO_LEDGER_SOURCE_LINK_INTERSECTION_SCORER_READY"
        decision_label = "KEEP"
        next_action = (
            "Use the top risk-adjusted-negative source-link/exemplar intersection group as a local-only verifier target; "
            "do not treat it as a trading rule without a separate candidate-pipeline causal verifier."
        )
    else:
        status = "UNKNOWN_PORTFOLIO_LEDGER_SOURCE_LINK_INTERSECTION_SIGNAL_INSUFFICIENT"
        decision_label = "UNKNOWN"
        next_action = (
            "Keep diagnostics as research-only; do not create summary-only removals, static cutoffs, price caps, "
            "or promotion claims from weak intersection concentration."
        )

    manifest = {
        "artifact": "xuan_b27_dplus_portfolio_ledger_source_link_intersection_spec",
        "created_utc": utc_now(),
        "schema_version": "portfolio_ledger_source_link_intersection_spec_v1",
        "status": status,
        "decision_label": decision_label,
        "scope": "local_only_summary_scorer_spec",
        "inputs": {
            "aggregate_report": str(args.aggregate_report),
            "summary_count": len(summary_paths),
            "source_link_score": str(args.source_link_score),
            "residual_tail_score": str(args.residual_tail_score),
        },
        "surface_checks": {
            "aggregate_portfolio_ledger_present": aggregate_portfolio_present,
            "summary_portfolio_ledger_count": len(portfolio_by_condition),
            "summary_portfolio_ledger_failures_sample": portfolio_failures[:8],
            "source_link_score_ok": source_ok,
            "source_link_failures": source_failures,
            "residual_tail_exemplar_score_ok": residual_ok,
            "residual_tail_failures": residual_failures,
        },
        "missing_files_or_fields": missing,
        "intersection_metrics": intersection,
        "scoring_contract": {
            "required_surfaces": [
                "aggregate_report.event_lite.portfolio_ledger_diagnostics",
                "summary.event_lite.portfolio_ledger_diagnostics",
                "source_link_transition_bucket_score.aggregate_parity.passed=true",
                "source_link_transition_bucket_score.guardrails.missing_cross_bucket_fields=[]",
                "residual_tail_exemplar_score.exemplar_metrics required/source_sequence/quote/order coverage=1.0",
            ],
            "join_keys": [
                "condition_id",
                "slug as fallback label only",
                "residual-tail exemplar source_sequence_id/quote_intent_id/source_order_id for action traceability",
            ],
            "keep_signal": {
                "risk_adjusted_negative_joined_residual_cost_share_min": 0.20,
                "top_intersection_group_residual_cost_share_min": 0.10,
                "must_nominate_local_verifier_only": True,
            },
        },
        "guardrails": {
            "no_events_jsonl_read": True,
            "no_raw_replay_or_full_store_scan": True,
            "no_ssh_or_remote_start": True,
            "no_summary_only_removal": True,
            "no_static_side_offset_cutoff": True,
            "no_price_or_cap_family": True,
            "no_cooldown_or_admission_cap": True,
            "no_fill_to_balance_revival": True,
            "no_portfolio_ledger_trading_rule": True,
            "public_profiles_are_proxy_only": True,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "canary_ready": False,
        },
        "side_effects": {
            "ssh_started": False,
            "network_started": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
        "next_executable_action": next_action,
    }
    write_json(args.output_dir / "manifest.json", manifest)
    write_json(args.output_dir / "score.json", manifest)
    print(json.dumps({"status": status, "decision_label": decision_label, "missing": missing}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
