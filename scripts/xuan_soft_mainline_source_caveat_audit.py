#!/usr/bin/env python3
"""Audit whether soft-mainline source blocks were safely absorbed.

Some otherwise-good dry-runs can contain a small number of source_quality_block
rows. This local-only audit distinguishes a working source gate from a source
quality defect: stale rows must be blocked before admission, while accepted and
rescue paths must remain source-clean. It never approves shadow, remote runs, or
deployment by itself.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path).expanduser().resolve()
    if not p.exists():
        return {}
    raw = json.loads(p.read_text())
    return raw if isinstance(raw, dict) else {}


def fnum(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def inum(value: Any, default: int = 0) -> int:
    out = fnum(value)
    return int(out) if out is not None else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    raw = card.get(key)
    return raw if isinstance(raw, dict) else {}


def status(card: dict[str, Any]) -> str:
    return str(card.get("status") or "")


def source_block_reason(row: dict[str, Any]) -> str:
    return str(
        row.get("source_quality_block_reason")
        or row.get("block_reason")
        or row.get("reason")
        or "<missing>"
    )


def iter_source_rows(output_root: str | None, sample_limit: int) -> tuple[list[dict[str, Any]], int, int]:
    if not output_root:
        return [], 0, 0
    root = Path(output_root).expanduser().resolve()
    if not root.exists():
        return [], 0, 0
    sample: list[dict[str, Any]] = []
    total = 0
    bad_json = 0
    for path in sorted(root.glob("*.events.jsonl")):
        with path.open() as handle:
            for lineno, line in enumerate(handle, 1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue
                kind = row.get("kind") or row.get("event") or row.get("event_type") or row.get("type")
                if kind != "source_quality_block" and not row.get("source_quality_block_reason"):
                    continue
                total += 1
                if len(sample) < sample_limit:
                    sample.append(
                        {
                            "file": path.name,
                            "line": lineno,
                            "slug": row.get("slug"),
                            "side": row.get("side"),
                            "price": fnum(row.get("price")),
                            "size": fnum(row.get("size")),
                            "accepted_ts_ms": row.get("accepted_ts_ms"),
                            "reason": source_block_reason(row),
                            "source_quality_l1_age_ms": fnum(row.get("source_quality_l1_age_ms")),
                            "source_quality_l2_age_ms": fnum(row.get("source_quality_l2_age_ms")),
                            "source_quality_l1_age_max_ms": fnum(row.get("source_quality_l1_age_max_ms")),
                            "trigger_ts_ms": row.get("trigger_ts_ms"),
                            "trigger_event_time_ms": row.get("trigger_event_time_ms"),
                            "source_sequence_id": row.get("source_sequence_id"),
                        }
                    )
    return sample, total, bad_json


def build_markdown(card: dict[str, Any]) -> str:
    obs = card["observed"]
    decision = card["decision"]
    lines = [
        "# Source Caveat Audit",
        "",
        f"Status: `{card['status']}`",
        f"Next action: `{decision['next_action']}`",
        "",
        "## Observed",
        "",
        f"- source blocks: `{obs['source_block_rows']}`",
        f"- accepted/rescue L1 max ms: `{obs['accepted_l1_age_ms_max']}` / `{obs['rescue_l1_age_ms_max']}`",
        f"- strict rescue source blocks: `{obs['strict_rescue_source_blocks']}`",
        f"- source block reasons: `{obs['source_block_reasons']}`",
        "",
        "## Guardrails",
        "",
        "- This is diagnostic evidence only; density_preflight may still remain blocked.",
        "- Absorbed source blocks mean stale rows were rejected before admission, not that source checks are relaxed.",
        "- Accepted and rescue paths must remain source-clean before any future shadow review.",
        "",
    ]
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    event = load_json(args.event_diagnostics)
    runtime = load_json(args.runtime_summary)
    density = load_json(args.density_preflight)
    metrics = body(runtime, "metrics")
    density_observed = body(density, "observed")
    event_counts = body(event, "event_counts")
    block_reasons = body(event, "block_reason_counts")
    rows, raw_source_count, bad_json = iter_source_rows(args.output_root, args.sample_limit)

    source_blocks_event = inum(event_counts.get("source_quality_block"))
    source_blocks_density = inum(density_observed.get("source_blocks"))
    source_rows = raw_source_count or source_blocks_event or source_blocks_density
    reason_counts = {
        key: inum(value)
        for key, value in block_reasons.items()
        if str(key).startswith("source_quality") or str(key).startswith("missing_source")
    }
    if rows and not reason_counts:
        for row in rows:
            reason = str(row.get("reason") or "<missing>")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    accepted_l1 = fnum(metrics.get("accepted_l1_age_ms_max"), 0.0) or 0.0
    rescue_l1 = fnum(metrics.get("rescue_l1_age_ms_max"), 0.0) or 0.0
    strict_rescue_source_blocks = fnum(metrics.get("strict_rescue_source_blocks"), 0.0) or 0.0
    source_row_l1_ages = [fnum(row.get("source_quality_l1_age_ms")) for row in rows]
    source_row_l1_ages = [age for age in source_row_l1_ages if age is not None]

    blockers: list[str] = []
    warnings: list[str] = []
    allowed_reasons = set(args.allowed_source_block_reasons)
    if source_rows > args.max_absorbed_source_blocks:
        blockers.append("source_block_rows_above_absorbed_max")
    if accepted_l1 > args.max_accepted_l1_age_ms:
        blockers.append("accepted_l1_age_above_max")
    if rescue_l1 > args.max_rescue_l1_age_ms:
        blockers.append("rescue_l1_age_above_max")
    if strict_rescue_source_blocks > args.max_strict_rescue_source_blocks:
        blockers.append("strict_rescue_source_blocks_present")
    if bad_json:
        blockers.append("bad_event_json_present")
    if rows and any(row.get("accepted_ts_ms") is not None for row in rows):
        blockers.append("source_block_row_has_accepted_ts")
    unknown_reasons = sorted(set(reason_counts) - allowed_reasons)
    if unknown_reasons:
        blockers.append("unexpected_source_block_reason")
    if source_row_l1_ages and max(source_row_l1_ages) > args.max_absorbed_source_block_l1_age_ms:
        blockers.append("source_block_l1_age_above_absorbed_max")
    if source_rows and args.output_root and raw_source_count != source_blocks_event:
        warnings.append("raw_source_count_differs_from_event_diagnostics")
    if source_blocks_density and source_blocks_density != source_rows:
        warnings.append("density_source_count_differs_from_audit_count")
    if source_rows and not args.output_root:
        warnings.append("raw_event_rows_not_available")

    if not source_rows and not blockers:
        card_status = "KEEP_SOFT_MAINLINE_SOURCE_CAVEAT_AUDIT_NO_SOURCE_BLOCKS_LOCAL_ONLY"
        next_action = "continue_standard_source_clean_interpretation"
        absorbed = True
    elif source_rows and not blockers:
        card_status = "KEEP_SOFT_MAINLINE_SOURCE_CAVEAT_AUDIT_ABSORBED_BY_GATE_LOCAL_ONLY"
        next_action = "treat_source_blocks_as_absorbed_caveat_not_admission_contamination"
        absorbed = True
    else:
        card_status = "BLOCKED_SOFT_MAINLINE_SOURCE_CAVEAT_AUDIT_SOURCE_DEFECT_LOCAL_ONLY"
        next_action = "inspect_source_rows_before_any_further_bridge_or_remote_interpretation"
        absorbed = False

    card = {
        "artifact": "xuan_soft_mainline_source_caveat_audit",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_soft_mainline_source_caveat_audit.py",
        "status": card_status,
        "inputs": {
            "output_root": str(Path(args.output_root).expanduser()) if args.output_root else None,
            "event_diagnostics": str(Path(args.event_diagnostics).expanduser()) if args.event_diagnostics else None,
            "runtime_summary": str(Path(args.runtime_summary).expanduser()) if args.runtime_summary else None,
            "density_preflight": str(Path(args.density_preflight).expanduser()) if args.density_preflight else None,
        },
        "source_statuses": {
            "event_diagnostics": status(event) or None,
            "runtime_summary": status(runtime) or None,
            "density_preflight": status(density) or None,
        },
        "observed": {
            "source_block_rows": source_rows,
            "source_block_rows_from_raw_events": raw_source_count,
            "source_block_rows_from_event_diagnostics": source_blocks_event,
            "source_block_rows_from_density_preflight": source_blocks_density,
            "source_block_reasons": reason_counts,
            "source_block_l1_age_ms_max": max(source_row_l1_ages) if source_row_l1_ages else None,
            "accepted_l1_age_ms_max": accepted_l1,
            "rescue_l1_age_ms_max": rescue_l1,
            "strict_rescue_source_blocks": strict_rescue_source_blocks,
            "bad_event_json": bad_json,
            "sample_source_block_rows": rows,
        },
        "thresholds": {
            "max_absorbed_source_blocks": args.max_absorbed_source_blocks,
            "max_absorbed_source_block_l1_age_ms": args.max_absorbed_source_block_l1_age_ms,
            "max_accepted_l1_age_ms": args.max_accepted_l1_age_ms,
            "max_rescue_l1_age_ms": args.max_rescue_l1_age_ms,
            "max_strict_rescue_source_blocks": args.max_strict_rescue_source_blocks,
            "allowed_source_block_reasons": sorted(allowed_reasons),
        },
        "decision": {
            "deployable": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "research_only": True,
            "source_caveat_absorbed": absorbed,
            "accepted_or_rescue_source_contamination": not absorbed,
            "hard_blockers": blockers,
            "warnings": warnings,
            "next_action": next_action,
        },
        "guardrails": [
            "Local audit only; does not relax source gates or approve shadow.",
            "Source blocks are acceptable only when rejected before admission and accepted/rescue paths stay source-clean.",
            "Any future remote remains PM_DRY_RUN=1 with strict source and rescue L1 gates.",
        ],
    }
    return rounded(card)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root")
    parser.add_argument("--event-diagnostics", required=True)
    parser.add_argument("--runtime-summary", required=True)
    parser.add_argument("--density-preflight")
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--markdown-output")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--max-absorbed-source-blocks", type=int, default=5)
    parser.add_argument("--max-absorbed-source-block-l1-age-ms", type=float, default=5000.0)
    parser.add_argument("--max-accepted-l1-age-ms", type=float, default=1000.0)
    parser.add_argument("--max-rescue-l1-age-ms", type=float, default=50.0)
    parser.add_argument("--max-strict-rescue-source-blocks", type=float, default=0.0)
    parser.add_argument(
        "--allowed-source-block-reasons",
        nargs="*",
        default=["source_quality_l1_age"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    card = build(args)
    out = Path(args.scorecard_json).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        md = Path(args.markdown_output).expanduser().resolve()
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(build_markdown(card) + "\n")
    print(json.dumps(card, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
