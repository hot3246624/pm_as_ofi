#!/usr/bin/env python3
"""Audit whether xuan artifacts contain quote-lifecycle evidence.

This is an implementability/data-contract check, not a strategy backtest.  It
only scans xuan research artifact logs for structured lifecycle events and
reports whether they can support promotion claims such as first-leg leakage and
realized maker price improvement.  It must not read collector, raw, or replay
data.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_quote_lifecycle_evidence_audit"
DEFAULT_MARKET_PREFIX = "btc-updown-5m-"
EVENT_LOG_NAMES = {
    "events.jsonl",
    "orders.jsonl",
    "market_md.jsonl",
    "order_events.jsonl",
    "shadow_events.jsonl",
}
EVENT_LOG_SUFFIXES = (".events.jsonl",)
LIFECYCLE_EVENTS = {
    "activation_block",
    "candidate",
    "cancel",
    "quote_plan",
    "quote_intent",
    "order_plan",
    "order_sent",
    "order_accepted",
    "cancel_sent",
    "cancel_ack",
    "order_cancelled",
    "quote_expired",
    "order_fill",
    "fill",
    "queue_supported_fill",
    "dry_run_touch_fill_confirmed",
}
QUOTE_STAGE_EVENTS = {
    "activation_block",
    "candidate",
    "cancel",
    "quote_plan",
    "quote_intent",
    "order_plan",
    "order_sent",
    "order_accepted",
    "cancel_sent",
    "cancel_ack",
    "order_cancelled",
    "quote_expired",
    "dry_run_touch_fill_confirmed",
}
FILL_EVENTS = {"order_fill", "fill", "queue_supported_fill", "dry_run_touch_fill_confirmed"}
CORE_QUOTE_FIELDS = ("quote_intent_id", "condition_id", "side", "price", "size")
PAIR_LINK_FIELDS = ("matched_pair_id", "opposite_trigger_ts_ms")
FILL_PROVENANCE_FIELDS = (
    "source",
    "depth_source_sequence_id",
    "depth_event_time_ms",
    "depth_best_bid",
    "depth_best_bid_drop_qty",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-root",
        default="xuan_research_artifacts",
        help="Root containing xuan research artifacts. Must not point at raw/replay/collector.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for manifest.json. Defaults under artifact root with timestamp.",
    )
    parser.add_argument("--market-prefix", default=DEFAULT_MARKET_PREFIX)
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--max-events", type=int, default=1_000_000)
    parser.add_argument("--min-core-field-coverage", type=float, default=0.99)
    parser.add_argument("--min-fill-provenance-coverage", type=float, default=0.99)
    return parser.parse_args()


def utc_now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def reject_forbidden_root(path: Path) -> None:
    parts = {part.lower() for part in path.resolve().parts}
    forbidden = {"raw", "replay", "collector"}
    hit = sorted(parts & forbidden)
    if hit:
        raise SystemExit(f"Refusing to scan forbidden data root components: {hit}")


def nested_get(record: dict[str, Any], key: str) -> Any:
    if key in record:
        return record.get(key)
    payload = record.get("payload")
    if isinstance(payload, dict):
        if key in payload:
            return payload.get(key)
        data = payload.get("data")
        if isinstance(data, dict) and key in data:
            return data.get(key)
    data = record.get("data")
    if isinstance(data, dict) and key in data:
        return data.get(key)
    return None


def event_name(record: dict[str, Any]) -> str:
    for key in ("event", "event_type", "kind", "type", "name"):
        value = nested_get(record, key)
        if value is not None:
            return str(value)
    return ""


def truthy_field(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value:
        return False
    return True


def discover_event_logs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*.jsonl")
        if (
            path.name in EVENT_LOG_NAMES
            or any(path.name.endswith(suffix) for suffix in EVENT_LOG_SUFFIXES)
        )
        and path.is_file()
    )


def inc_field_counts(
    record: dict[str, Any],
    fields: tuple[str, ...],
    counts: Counter[str],
) -> None:
    for field in fields:
        if truthy_field(nested_get(record, field)):
            counts[field] += 1


def coverage(count: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(count / denominator, 6)


def audit_logs(paths: list[Path], max_events: int) -> dict[str, Any]:
    event_counts: Counter[str] = Counter()
    file_counts: Counter[str] = Counter()
    core_quote_field_counts: Counter[str] = Counter()
    pair_link_field_counts: Counter[str] = Counter()
    fill_provenance_field_counts: Counter[str] = Counter()
    fill_source_counts: Counter[str] = Counter()
    path_errors: list[dict[str, str]] = []
    quote_event_count = 0
    fill_event_count = 0
    parsed_row_count = 0
    lifecycle_event_count = 0
    pair_link_any_count = 0

    for path in paths:
        if parsed_row_count >= max_events:
            break
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line_no, line in enumerate(handle, start=1):
                    if parsed_row_count >= max_events:
                        break
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        record = json.loads(text)
                    except json.JSONDecodeError as exc:
                        path_errors.append(
                            {
                                "path": str(path),
                                "line": str(line_no),
                                "error": f"json_decode_error: {exc}",
                            }
                        )
                        continue
                    if not isinstance(record, dict):
                        continue
                    parsed_row_count += 1
                    file_counts[path.name] += 1
                    name = event_name(record)
                    event_counts[name or "<missing>"] += 1
                    if name not in LIFECYCLE_EVENTS:
                        continue
                    lifecycle_event_count += 1
                    if name in QUOTE_STAGE_EVENTS:
                        quote_event_count += 1
                        inc_field_counts(record, CORE_QUOTE_FIELDS, core_quote_field_counts)
                        inc_field_counts(record, PAIR_LINK_FIELDS, pair_link_field_counts)
                        if any(truthy_field(nested_get(record, field)) for field in PAIR_LINK_FIELDS):
                            pair_link_any_count += 1
                    if name in FILL_EVENTS:
                        fill_event_count += 1
                        inc_field_counts(
                            record,
                            FILL_PROVENANCE_FIELDS,
                            fill_provenance_field_counts,
                        )
                        source = nested_get(record, "source")
                        if source is not None:
                            fill_source_counts[str(source)] += 1
        except OSError as exc:
            path_errors.append({"path": str(path), "error": str(exc)})

    return {
        "event_log_count": len(paths),
        "event_log_paths": [str(path) for path in paths],
        "parsed_row_count": parsed_row_count,
        "max_events_reached": parsed_row_count >= max_events,
        "file_row_counts": dict(file_counts),
        "event_counts": dict(event_counts.most_common()),
        "lifecycle_event_count": lifecycle_event_count,
        "quote_event_count": quote_event_count,
        "fill_event_count": fill_event_count,
        "core_quote_field_counts": dict(core_quote_field_counts),
        "core_quote_field_coverage": {
            field: coverage(core_quote_field_counts[field], quote_event_count)
            for field in CORE_QUOTE_FIELDS
        },
        "pair_link_field_counts": dict(pair_link_field_counts),
        "pair_link_any_count": pair_link_any_count,
        "pair_link_any_coverage": coverage(pair_link_any_count, quote_event_count),
        "fill_provenance_field_counts": dict(fill_provenance_field_counts),
        "fill_provenance_field_coverage": {
            field: coverage(fill_provenance_field_counts[field], fill_event_count)
            for field in FILL_PROVENANCE_FIELDS
        },
        "fill_source_counts": dict(fill_source_counts),
        "path_errors": path_errors[:50],
        "path_error_count": len(path_errors),
    }


def compute_status(audit: dict[str, Any], args: argparse.Namespace) -> tuple[str, list[str]]:
    blockers: list[str] = []
    if audit["event_log_count"] == 0:
        return "BLOCKED_NO_QUOTE_LIFECYCLE_EVENT_LOGS", [
            "No events.jsonl/orders.jsonl/market_md.jsonl logs were found under artifact_root."
        ]
    if audit["lifecycle_event_count"] == 0:
        return "BLOCKED_NO_QUOTE_LIFECYCLE_EVENTS", [
            "Event logs exist, but no quote/order/fill lifecycle events were found."
        ]

    quote_count = int(audit["quote_event_count"])
    fill_count = int(audit["fill_event_count"])
    if quote_count == 0:
        blockers.append("No quote-stage events found for leakage denominator.")
    for field, value in audit["core_quote_field_coverage"].items():
        if quote_count > 0 and float(value) < args.min_core_field_coverage:
            blockers.append(
                f"quote field {field} coverage {value} < {args.min_core_field_coverage}"
            )

    pair_link_coverage = float(audit["pair_link_any_coverage"])
    audit["pair_link_any_coverage"] = pair_link_coverage
    if quote_count > 0 and pair_link_coverage < args.min_core_field_coverage:
        blockers.append(
            "pair linkage coverage "
            f"{pair_link_coverage} < {args.min_core_field_coverage}; "
            "need matched_pair_id or opposite_trigger_ts_ms"
        )

    if fill_count == 0:
        blockers.append("No fill events found for realized source/provenance audit.")
    for field, value in audit["fill_provenance_field_coverage"].items():
        if fill_count > 0 and float(value) < args.min_fill_provenance_coverage:
            blockers.append(
                f"fill provenance field {field} coverage {value} "
                f"< {args.min_fill_provenance_coverage}"
            )

    if blockers:
        return "BLOCKED_QUOTE_LIFECYCLE_FIELDS_MISSING", blockers
    return "PASS_QUOTE_LIFECYCLE_EVIDENCE_READY", []


def load_manifest_scope(root: Path) -> dict[str, Any]:
    labels: set[str] = set()
    days: set[str] = set()
    statuses: Counter[str] = Counter()
    for manifest_path in sorted(root.glob("*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        status = manifest.get("status")
        if status:
            statuses[str(status)] += 1
        for key in ("labels", "covered_labels", "holdout_labels"):
            value = manifest.get(key)
            if isinstance(value, list):
                labels.update(str(item) for item in value)
        for key in ("days", "covered_days", "holdout_days"):
            value = manifest.get(key)
            if isinstance(value, list):
                days.update(str(item) for item in value)
    return {
        "labels": sorted(labels),
        "days": sorted(days),
        "artifact_status_counts": dict(statuses),
    }


def main() -> int:
    args = parse_args()
    root = Path(args.artifact_root).expanduser().resolve()
    reject_forbidden_root(root)
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else root / f"{ARTIFACT}_{utc_now_tag()}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = discover_event_logs(root)
    audit = audit_logs(paths, args.max_events)
    status, blockers = compute_status(audit, args)
    scope = load_manifest_scope(root)

    manifest = {
        "artifact": ARTIFACT,
        "status": status,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_declaration": {
            "data_root": str(root),
            "dataset_type": "xuan_artifact_quote_lifecycle_evidence_audit",
            "labels": scope["labels"],
            "days": scope["days"],
            "market_prefix": args.market_prefix,
            "assets": [args.asset],
            "row_count": audit["parsed_row_count"],
            "excluded_20260514_20260515": True,
            "public_account_execution_truth_v1_included": False,
            "raw_replay_scanned": False,
            "collector_scanned": False,
        },
        "safety": {
            "read_only": True,
            "services_touched": False,
            "raw_replay_scanned": False,
            "collector_scanned": False,
            "xuan_shadow_deployed_or_restarted": False,
        },
        "gate": {
            "can_support_strategy_promotion": status == "PASS_QUOTE_LIFECYCLE_EVIDENCE_READY",
            "blocks_shadow_deploy": status != "PASS_QUOTE_LIFECYCLE_EVIDENCE_READY",
            "blockers": blockers,
            "next_action": (
                "add quote-lifecycle event logging or run a xuan-owned no-order shadow "
                "that emits quote_intent_id, lifecycle timestamps, pair linkage, "
                "leaked fill denominator, and depth source_sequence_id linkage"
                if status != "PASS_QUOTE_LIFECYCLE_EVIDENCE_READY"
                else "use this evidence artifact as an input to a separate strategy promotion review"
            ),
        },
        "audit": audit,
        "artifact_scope": scope,
    }

    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"status": status, "output_dir": str(output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
