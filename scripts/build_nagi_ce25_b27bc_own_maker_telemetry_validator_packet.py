#!/usr/bin/env python3
"""Validate own maker telemetry samples for the NAGI/CE25/B27BC research gate.

This is a local-only packet builder. It reads CSV samples only, never imports
credentials, never calls APIs, and never executes orders. Without a private
sample it fails closed by design.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


NO_SAMPLE_STATUS = (
    "BLOCKED_NAGI_CE25_B27BC_OWN_MAKER_TELEMETRY_VALIDATOR_NO_PRIVATE_SAMPLE_"
    "FAIL_CLOSED_NOT_READY"
)
FAILED_STATUS = (
    "BLOCKED_NAGI_CE25_B27BC_OWN_MAKER_TELEMETRY_VALIDATOR_SAMPLE_FAILED_"
    "NOT_READY"
)
KEEP_STATUS = (
    "KEEP_NAGI_CE25_B27BC_OWN_MAKER_TELEMETRY_VALIDATOR_SAMPLE_PASSED_"
    "RESEARCH_ONLY_PRIVATE_TRUTH_REVIEW_REQUIRED_NOT_READY"
)

DEFAULT_OUTPUT_DIR = "data/exports/nagi_ce25_b27bc_own_maker_telemetry_validator_packet_20260609"
DEFAULT_ROOTS = ("data/inputs", "evidence")
EXCLUDED_PATH_TOKENS = ("smoke", "fixture", "testdata", "__pycache__")
ACCEPTED_NAME_TOKENS = (
    "own_maker_telemetry",
    "maker_fill_telemetry",
    "own-maker-telemetry",
    "maker-fill-telemetry",
)

REQUIRED_COLUMNS = (
    "market_slug",
    "order_id",
    "client_order_id",
    "decision_ts_ms",
    "submitted_ts_ms",
    "side",
    "limit_price",
    "order_qty",
    "filled_qty",
    "fill_ts_ms",
    "maker_or_taker",
    "fee_rate",
    "fee_paid",
    "queue_proxy_open",
    "public_touch_seen",
    "public_touch_to_own_fill_conversion",
    "pair_cost_at_decision",
    "pair_cost_realized",
    "residual_cost",
    "residual_cost_rate",
    "realized_maker_edge_after_fees",
)

MIN_ORDER_QTY = 5.0
MIN_MARKETS = 100
MIN_ACTIONS = 500
MAX_RESIDUAL_COST_RATE = 0.20
MAX_PAIR_COST_P50 = 0.995


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def parse_float(value: str | None) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def p50(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def is_candidate_path(path: Path) -> bool:
    lower = str(path).lower()
    if path.suffix.lower() != ".csv":
        return False
    if any(token in lower for token in EXCLUDED_PATH_TOKENS):
        return False
    name = path.name.lower()
    return any(token in name for token in ACCEPTED_NAME_TOKENS)


def discover_inputs(roots: Iterable[Path]) -> list[Path]:
    found: list[Path] = []
    for root in roots:
        if root.is_file() and is_candidate_path(root):
            found.append(root)
            continue
        if not root.exists() or not root.is_dir():
            continue
        for path in root.rglob("*.csv"):
            if is_candidate_path(path):
                found.append(path)
    return sorted(set(found), key=lambda item: str(item))


@dataclass
class RowIssue:
    row_number: int
    issue: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {"row_number": self.row_number, "issue": self.issue, "detail": self.detail}


def read_rows(paths: list[Path]) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    schema_blockers: list[str] = []
    for path in paths:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = tuple(reader.fieldnames or ())
            missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
            if missing:
                schema_blockers.append(f"{path}:missing_columns:{','.join(missing)}")
                continue
            for row in reader:
                item = dict(row)
                item["_source_path"] = str(path)
                rows.append(item)
    return rows, schema_blockers


def validate_rows(rows: list[dict[str, str]], schema_blockers: list[str]) -> dict[str, Any]:
    issues: list[RowIssue] = []
    markets: set[str] = set()
    action_count = 0
    pair_costs: list[float] = []
    residual_rates: list[float] = []
    conversions: list[float] = []
    total_edge = 0.0
    queue_open_count = 0
    public_touch_count = 0

    for index, row in enumerate(rows, start=2):
        market_slug = (row.get("market_slug") or "").strip()
        if market_slug:
            markets.add(market_slug)
        else:
            issues.append(RowIssue(index, "missing_market_slug", "market_slug is required"))

        maker_or_taker = (row.get("maker_or_taker") or "").strip().lower()
        if maker_or_taker != "maker":
            issues.append(RowIssue(index, "non_maker_fill", f"maker_or_taker={maker_or_taker or '<blank>'}"))

        side = (row.get("side") or "").strip().upper()
        if side not in {"YES", "NO", "UP", "DOWN"}:
            issues.append(RowIssue(index, "invalid_side", f"side={side or '<blank>'}"))

        fee_rate = parse_float(row.get("fee_rate"))
        fee_paid = parse_float(row.get("fee_paid"))
        if fee_rate is None or abs(fee_rate) > 1e-12:
            issues.append(RowIssue(index, "maker_fee_not_zero", f"fee_rate={row.get('fee_rate')}"))
        if fee_paid is None or abs(fee_paid) > 1e-9:
            issues.append(RowIssue(index, "fee_paid_not_zero", f"fee_paid={row.get('fee_paid')}"))

        order_qty = parse_float(row.get("order_qty"))
        filled_qty = parse_float(row.get("filled_qty"))
        if order_qty is None or order_qty < MIN_ORDER_QTY:
            issues.append(RowIssue(index, "limit_order_minimum_not_met", f"order_qty={row.get('order_qty')}"))
        if filled_qty is None or filled_qty <= 0:
            issues.append(RowIssue(index, "filled_qty_not_positive", f"filled_qty={row.get('filled_qty')}"))
        else:
            action_count += 1

        queue_proxy_open = parse_bool(row.get("queue_proxy_open"))
        public_touch_seen = parse_bool(row.get("public_touch_seen"))
        if queue_proxy_open is not True:
            issues.append(RowIssue(index, "queue_open_evidence_missing", f"queue_proxy_open={row.get('queue_proxy_open')}"))
        else:
            queue_open_count += 1
        if public_touch_seen is not True:
            issues.append(RowIssue(index, "public_touch_evidence_missing", f"public_touch_seen={row.get('public_touch_seen')}"))
        else:
            public_touch_count += 1

        conversion = parse_float(row.get("public_touch_to_own_fill_conversion"))
        if conversion is None or conversion < 0 or conversion > 1:
            issues.append(
                RowIssue(
                    index,
                    "own_fill_conversion_invalid",
                    f"public_touch_to_own_fill_conversion={row.get('public_touch_to_own_fill_conversion')}",
                )
            )
        else:
            conversions.append(conversion)

        pair_cost_at_decision = parse_float(row.get("pair_cost_at_decision"))
        pair_cost_realized = parse_float(row.get("pair_cost_realized"))
        if pair_cost_at_decision is None or pair_cost_at_decision <= 0:
            issues.append(
                RowIssue(index, "pair_cost_at_decision_invalid", f"pair_cost_at_decision={row.get('pair_cost_at_decision')}")
            )
        if pair_cost_realized is None or pair_cost_realized <= 0:
            issues.append(RowIssue(index, "pair_cost_realized_invalid", f"pair_cost_realized={row.get('pair_cost_realized')}"))
        else:
            pair_costs.append(pair_cost_realized)

        residual_cost = parse_float(row.get("residual_cost"))
        residual_rate = parse_float(row.get("residual_cost_rate"))
        if residual_cost is None or residual_cost < 0:
            issues.append(RowIssue(index, "residual_cost_invalid", f"residual_cost={row.get('residual_cost')}"))
        if residual_rate is None or residual_rate < 0:
            issues.append(RowIssue(index, "residual_cost_rate_invalid", f"residual_cost_rate={row.get('residual_cost_rate')}"))
        else:
            residual_rates.append(residual_rate)

        edge = parse_float(row.get("realized_maker_edge_after_fees"))
        if edge is None:
            issues.append(
                RowIssue(
                    index,
                    "realized_maker_edge_after_fees_missing",
                    f"realized_maker_edge_after_fees={row.get('realized_maker_edge_after_fees')}",
                )
            )
        else:
            total_edge += edge

    aggregate = {
        "row_count": len(rows),
        "own_maker_filled_markets": len(markets),
        "own_maker_filled_actions": action_count,
        "queue_proxy_open_rows": queue_open_count,
        "public_touch_seen_rows": public_touch_count,
        "pair_cost_p50": p50(pair_costs),
        "residual_cost_rate_max": max(residual_rates) if residual_rates else None,
        "residual_cost_rate_p50": p50(residual_rates),
        "own_fill_conversion_p50": p50(conversions),
        "realized_maker_edge_after_fees_sum": total_edge,
    }

    gate_blockers = list(schema_blockers)
    if len(markets) < MIN_MARKETS:
        gate_blockers.append(f"own_maker_filled_markets_below_{MIN_MARKETS}")
    if action_count < MIN_ACTIONS:
        gate_blockers.append(f"own_maker_filled_actions_below_{MIN_ACTIONS}")
    if aggregate["pair_cost_p50"] is None or aggregate["pair_cost_p50"] > MAX_PAIR_COST_P50:
        gate_blockers.append(f"pair_cost_p50_above_{MAX_PAIR_COST_P50}")
    if aggregate["residual_cost_rate_max"] is None or aggregate["residual_cost_rate_max"] > MAX_RESIDUAL_COST_RATE:
        gate_blockers.append(f"residual_cost_rate_above_{MAX_RESIDUAL_COST_RATE}")
    if total_edge <= 0:
        gate_blockers.append("realized_maker_edge_after_fees_not_positive")
    if issues:
        gate_blockers.append("row_level_telemetry_issues_present")

    passed = not gate_blockers
    return {
        "passed": passed,
        "status": KEEP_STATUS if passed else FAILED_STATUS,
        "aggregate": aggregate,
        "blockers": gate_blockers,
        "row_issue_count": len(issues),
        "row_issues_sample": [issue.to_dict() for issue in issues[:50]],
    }


def non_claims() -> dict[str, bool]:
    return {
        "ready": False,
        "private_truth": False,
        "maker_fill_truth": False,
        "order_execution": False,
        "canary": False,
        "live": False,
    }


def build_decision(args: argparse.Namespace) -> dict[str, Any]:
    explicit_inputs = [Path(item) for item in args.input_csv]
    roots = [Path(item) for item in args.root]
    input_files = explicit_inputs if explicit_inputs else discover_inputs(roots)

    base: dict[str, Any] = {
        "generated_at": utc_now(),
        "evidence_level": "local_validator_packet",
        "input_files": [str(path) for path in input_files],
        "order_minimum_contract": {
            "limit_post_only_maker_min_shares": MIN_ORDER_QTY,
            "market_order_min_usdc": 1.0,
            "market_orders_used_by_this_pipeline": False,
        },
        "required_columns": list(REQUIRED_COLUMNS),
        "thresholds": {
            "own_maker_filled_markets": MIN_MARKETS,
            "own_maker_filled_actions": MIN_ACTIONS,
            "pair_cost_p50_max": MAX_PAIR_COST_P50,
            "residual_cost_rate_max": MAX_RESIDUAL_COST_RATE,
            "maker_fee_rate_required": 0.0,
        },
        "non_claims": non_claims(),
    }
    if not input_files:
        base.update(
            {
                "status": NO_SAMPLE_STATUS,
                "passed": False,
                "aggregate": {
                    "row_count": 0,
                    "own_maker_filled_markets": 0,
                    "own_maker_filled_actions": 0,
                },
                "blockers": ["own_maker_telemetry_sample_missing"],
                "next_executable_action": "wait_for_own_maker_telemetry_sample_or_new_source_truth_input",
            }
        )
        return base

    rows, schema_blockers = read_rows(input_files)
    validation = validate_rows(rows, schema_blockers)
    base.update(validation)
    base["next_executable_action"] = (
        "review_private_maker_truth_manually_before_any_status_promotion"
        if validation["passed"]
        else "fix_or_replace_own_maker_telemetry_sample_then_rerun_validator"
    )
    return base


def write_report(path: Path, decision: dict[str, Any]) -> None:
    lines = [
        "# NAGI + CE25 + B27BC Own Maker Telemetry Validator",
        "",
        f"Generated at: `{decision['generated_at']}`",
        "",
        "## Decision",
        "",
        f"- Status: `{decision['status']}`",
        f"- Passed validator: `{decision['passed']}`",
        f"- Input files: `{len(decision['input_files'])}`",
        f"- Row count: `{decision['aggregate'].get('row_count')}`",
        f"- Own maker-filled markets: `{decision['aggregate'].get('own_maker_filled_markets')}`",
        f"- Own maker-filled actions: `{decision['aggregate'].get('own_maker_filled_actions')}`",
        f"- Pair-cost p50: `{decision['aggregate'].get('pair_cost_p50')}`",
        f"- Residual cost-rate max: `{decision['aggregate'].get('residual_cost_rate_max')}`",
        f"- Realized maker edge after fees sum: `{decision['aggregate'].get('realized_maker_edge_after_fees_sum')}`",
        "",
        "## Blockers",
        "",
    ]
    blockers = decision.get("blockers") or []
    if blockers:
        lines.extend(f"- `{item}`" for item in blockers)
    else:
        lines.append("- None from this validator, but this is still review-only and not OOS/live ready.")
    lines.extend(
        [
            "",
            "## Hard Guards",
            "",
            "- Limit/post-only maker order quantity must be at least `5` shares.",
            "- Market-order minimum is `$1` USDC, but market orders are not used by this pipeline.",
            "- All counted rows must be maker fills with fee rate `0` and fee paid `0`.",
            "- Taker or ambiguous fills fail closed.",
            "- Queue/open evidence and public-touch-to-own-fill conversion must be reported.",
            "- `private_truth`, `maker_fill_truth`, `ready`, `canary`, and `live` claims remain false in this packet.",
            "",
        ]
    )
    if decision.get("row_issues_sample"):
        lines.extend(["## Row Issue Sample", ""])
        for issue in decision["row_issues_sample"][:20]:
            lines.append(f"- row `{issue['row_number']}` `{issue['issue']}`: {issue['detail']}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--input-csv", action="append", default=[])
    parser.add_argument("--root", action="append", default=list(DEFAULT_ROOTS))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    decision = build_decision(args)
    write_json(out / "decision_register.json", decision)
    write_report(out / "OWN_MAKER_TELEMETRY_VALIDATOR_REPORT.md", decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
