#!/usr/bin/env python3
"""Validate an in-worktree multi-day public-profile source.

This scorer is still public-proxy only. It converts public activity rows into
daily buy-side complete-set pairing diagnostics and fails closed on sparse days,
SELL-heavy samples, broad residual, or weak pair cost. It does not fetch data,
start services, use SSH/shadow/live, or scan forbidden stores.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_public_profile_multiday_validation_spec"
CONTRACT_NAME = "xuan_public_profile_multiday_validation_v1"
DEFAULT_PUBLIC_INPUT = Path(
    "xuan_research_artifacts/"
    "xuan_public_profile_reset_review_20260524T022800Z/"
    "public_inputs/0x63ce342161250d705dc0b16df89036c8e5f9ba9a/"
    "activity_trade_rows.json"
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)
MIN_DAYS = 3
MIN_ROWS_PER_DAY = 100
MIN_BUY_ROWS_PER_DAY = 80
MAX_SELL_SHARE = 0.10
MAX_WEIGHTED_PAIR_COST = 0.98
MAX_RESIDUAL_SHARE = 0.25
MIN_BOTH_OUTCOME_RATIO = 0.50


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_is_forbidden(path: Path) -> bool:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    return any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def path_safe(path: Path, root: Path) -> tuple[bool, str | None]:
    if path_is_forbidden(path):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def normalize_ts(value: Any) -> int | None:
    try:
        ts = int(float(value))
    except (TypeError, ValueError):
        return None
    if ts > 10_000_000_000:
        ts //= 1000
    if ts <= 0:
        return None
    return ts


def bjt_day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(timezone(timedelta(hours=8))).date().isoformat()


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def read_public_rows(path: Path, root: Path) -> tuple[list[dict[str, Any]], str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return [], reason
    try:
        rows = load_json(path)
    except FileNotFoundError:
        return [], "missing"
    except Exception as exc:
        return [], f"{type(exc).__name__}: {exc}"
    if not isinstance(rows, list):
        return [], "not_json_list"
    return [row for row in rows if isinstance(row, dict)], None


def row_day(row: dict[str, Any]) -> str | None:
    ts = normalize_ts(row.get("timestamp"))
    return bjt_day(ts) if ts is not None else None


def complete_set_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    buy_rows = [row for row in rows if str(row.get("side", "")).upper() == "BUY" and str(row.get("type", "")).upper() == "TRADE"]
    by_condition: dict[str, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"qty": 0.0, "cost": 0.0})
    )
    for row in buy_rows:
        condition = str(row.get("conditionId") or row.get("condition_id") or "")
        outcome = str(row.get("outcome") or row.get("outcomeIndex") or "")
        if not condition or not outcome:
            continue
        qty = as_float(row.get("size"))
        cost = as_float(row.get("usdcSize"), as_float(row.get("price")) * qty)
        if qty <= 0:
            continue
        by_condition[condition][outcome]["qty"] += qty
        by_condition[condition][outcome]["cost"] += cost
    matched_qty_total = 0.0
    weighted_pair_cost_sum = 0.0
    residual_qty_total = 0.0
    residual_cost_total = 0.0
    condition_count = 0
    both_outcome_count = 0
    condition_summaries: list[dict[str, Any]] = []
    for condition, outcomes in by_condition.items():
        condition_count += 1
        outcome_items = sorted(outcomes.items())
        total_qty = sum(item["qty"] for _, item in outcome_items)
        total_cost = sum(item["cost"] for _, item in outcome_items)
        if len(outcome_items) >= 2:
            both_outcome_count += 1
            first, second = outcome_items[0][1], outcome_items[1][1]
            matched_qty = min(first["qty"], second["qty"])
            if matched_qty > 0:
                first_px = first["cost"] / first["qty"] if first["qty"] else 0.0
                second_px = second["cost"] / second["qty"] if second["qty"] else 0.0
                pair_cost = first_px + second_px
                matched_qty_total += matched_qty
                weighted_pair_cost_sum += pair_cost * matched_qty
        else:
            matched_qty = 0.0
        residual_qty = max(total_qty - (2.0 * matched_qty), 0.0)
        residual_cost = total_cost * (residual_qty / total_qty) if total_qty > 0 else 0.0
        residual_qty_total += residual_qty
        residual_cost_total += residual_cost
        condition_summaries.append(
            {
                "condition_id": condition,
                "outcome_count": len(outcome_items),
                "total_qty": round(total_qty, 8),
                "matched_qty": round(matched_qty, 8),
                "residual_qty": round(residual_qty, 8),
            }
        )
    bought_qty_total = matched_qty_total * 2.0 + residual_qty_total
    weighted_pair_cost = weighted_pair_cost_sum / matched_qty_total if matched_qty_total > 0 else None
    residual_share = residual_qty_total / bought_qty_total if bought_qty_total > 0 else None
    both_outcome_ratio = both_outcome_count / condition_count if condition_count else 0.0
    return {
        "buy_rows": len(buy_rows),
        "condition_count": condition_count,
        "both_outcome_condition_count": both_outcome_count,
        "both_outcome_condition_ratio": round(both_outcome_ratio, 8),
        "matched_qty": round(matched_qty_total, 8),
        "bought_qty": round(bought_qty_total, 8),
        "weighted_pair_cost": round(weighted_pair_cost, 8) if weighted_pair_cost is not None else None,
        "weighted_pair_edge": round(1.0 - weighted_pair_cost, 8) if weighted_pair_cost is not None else None,
        "residual_qty": round(residual_qty_total, 8),
        "residual_cost_proxy": round(residual_cost_total, 8),
        "residual_share": round(residual_share, 8) if residual_share is not None else None,
        "top_conditions_by_residual_qty": sorted(
            condition_summaries,
            key=lambda item: item["residual_qty"],
            reverse=True,
        )[:5],
    }


def daily_validation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = row_day(row)
        if day is not None:
            by_day[day].append(row)
    daily: dict[str, Any] = {}
    valid_days = []
    blockers: list[str] = []
    for day in sorted(by_day):
        day_rows = by_day[day]
        trade_rows = [row for row in day_rows if str(row.get("type", "")).upper() == "TRADE"]
        sell_rows = [row for row in trade_rows if str(row.get("side", "")).upper() == "SELL"]
        sell_share = len(sell_rows) / len(trade_rows) if trade_rows else 0.0
        metrics = complete_set_metrics(day_rows)
        day_blockers: list[str] = []
        if len(trade_rows) < MIN_ROWS_PER_DAY:
            day_blockers.append("trade_rows_below_min")
        if metrics["buy_rows"] < MIN_BUY_ROWS_PER_DAY:
            day_blockers.append("buy_rows_below_min")
        if sell_share > MAX_SELL_SHARE:
            day_blockers.append("sell_share_above_max")
        pair_cost = metrics["weighted_pair_cost"]
        residual = metrics["residual_share"]
        if pair_cost is None:
            day_blockers.append("no_matched_pair_cost")
        elif pair_cost > MAX_WEIGHTED_PAIR_COST:
            day_blockers.append("weighted_pair_cost_above_max")
        if residual is None:
            day_blockers.append("no_bought_qty")
        elif residual > MAX_RESIDUAL_SHARE:
            day_blockers.append("residual_share_above_max")
        if metrics["both_outcome_condition_ratio"] < MIN_BOTH_OUTCOME_RATIO:
            day_blockers.append("both_outcome_ratio_below_min")
        day_passed = not day_blockers
        if day_passed:
            valid_days.append(day)
        else:
            blockers.extend(f"{day}_{blocker}" for blocker in day_blockers)
        daily[day] = {
            "trade_rows": len(trade_rows),
            "sell_rows": len(sell_rows),
            "sell_share": round(sell_share, 8),
            "passed": day_passed,
            "blockers": day_blockers,
            "complete_set_metrics": metrics,
        }
    if len(by_day) < MIN_DAYS:
        blockers.append("fewer_than_3_bjt_days")
    if len(valid_days) < MIN_DAYS:
        blockers.append("fewer_than_3_valid_days")
    return {
        "bjt_days": sorted(by_day),
        "bjt_day_count": len(by_day),
        "valid_days": valid_days,
        "valid_day_count": len(valid_days),
        "daily_results": daily,
        "blockers": sorted(set(blockers)),
        "candidate_status": "READY" if len(valid_days) >= MIN_DAYS and not blockers else "UNKNOWN",
    }


def validation_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "minimum_days": MIN_DAYS,
        "minimum_rows_per_day": MIN_ROWS_PER_DAY,
        "minimum_buy_rows_per_day": MIN_BUY_ROWS_PER_DAY,
        "max_sell_share": MAX_SELL_SHARE,
        "max_weighted_pair_cost": MAX_WEIGHTED_PAIR_COST,
        "max_residual_share": MAX_RESIDUAL_SHARE,
        "min_both_outcome_condition_ratio": MIN_BOTH_OUTCOME_RATIO,
        "retrospective_public_proxy_only": True,
        "rejected_interpretations": [
            "deployable or canary-ready evidence",
            "private truth",
            "directional settlement skill proof",
            "realized pair_cost as live criterion",
            "price cap/static filter",
            "D+ failed families",
        ],
    }


def build_manifest(args: argparse.Namespace, root: Path, output_dir: Path) -> dict[str, Any]:
    ok_output, output_reason = path_safe(output_dir, root)
    if not ok_output:
        raise RuntimeError(f"unsafe output dir: {output_reason}: {output_dir}")
    rows, input_error = read_public_rows(args.public_input, root)
    validation = daily_validation(rows) if input_error is None else {
        "candidate_status": "UNKNOWN",
        "bjt_days": [],
        "bjt_day_count": 0,
        "valid_days": [],
        "valid_day_count": 0,
        "daily_results": {},
        "blockers": [input_error or "input_unreadable"],
    }
    ready = validation["candidate_status"] == "READY"
    label = (
        "KEEP_PUBLIC_PROFILE_MULTIDAY_VALIDATION_PROXY_READY"
        if ready
        else "UNKNOWN_PUBLIC_PROFILE_MULTIDAY_VALIDATION_SAMPLE_INSUFFICIENT"
    )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "public_profile_multiday_validation_spec",
        "decision": "KEEP" if ready else "UNKNOWN",
        "decision_label": label,
        "scope": {
            "local_only": True,
            "current_worktree_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "trading_behavior_changed": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "public_input": str(args.public_input),
            "public_input_error": input_error,
            "row_count": len(rows),
        },
        "validation_contract": validation_contract(),
        "public_profile_multiday_validation": validation,
        "research_ranking": {
            "status": label,
            "strategy_evidence": False,
            "public_proxy_ready": ready,
            "no_order_diagnostic_allowed": False,
            "blockers": validation["blockers"],
            "interpretation": (
                "The selected public rows must pass daily sample, buy-side, complete-set pair-cost, and residual checks "
                "before they become a useful proxy lead. Passing still would not prove private truth or deployability."
            ),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "PUBLIC_PROFILE_MULTIDAY_VALIDATION_ONLY_NOT_PROMOTION_EVIDENCE",
        },
        "next_executable_action": (
            "If validation is UNKNOWN, do not continue this source as mimicability evidence; provide a safer 72h source "
            "or pivot to another line. If validation is KEEP, implement only a proxy scorer/spec next; do not start no-order diagnostics."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-input", type=Path, default=DEFAULT_PUBLIC_INPUT)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    manifest = build_manifest(args, root, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "bjt_day_count": manifest["public_profile_multiday_validation"]["bjt_day_count"],
                "valid_day_count": manifest["public_profile_multiday_validation"]["valid_day_count"],
                "blockers": manifest["research_ranking"]["blockers"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
