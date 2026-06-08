#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any


def to_float(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def to_int(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    values = sorted(values)
    pos = max(0.0, min(1.0, q)) * (len(values) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return values[int(lo)]
    weight = pos - lo
    return values[int(lo)] * (1.0 - weight) + values[int(hi)] * weight


def load_rows(root: Path) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    bad_rows = 0
    for raw_path in sorted(glob.glob(str(root / "**" / "round_evidence.jsonl"), recursive=True)):
        path = Path(raw_path)
        with path.open(errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    bad_rows += 1
                    continue
                row.setdefault("_source_path", str(path))
                rows.append(row)
    return rows, bad_rows


def filter_rows(
    rows: list[dict[str, Any]],
    min_round_end_ts: int | None,
    phase: str,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if phase and str(row.get("phase", "")) != phase:
            continue
        round_end_ts = to_int(row.get("round_end_ts"))
        if min_round_end_ts is not None and (round_end_ts is None or round_end_ts < min_round_end_ts):
            continue
        filtered.append(row)
    return filtered


def dedupe_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicates = 0
    for row in rows:
        key = (
            row.get("slug"),
            row.get("round_end_ts"),
            row.get("source_subset"),
            row.get("rule"),
            row.get("local_close_ts_ms"),
        )
        if key in by_key:
            duplicates += 1
        by_key[key] = row
    return list(by_key.values()), duplicates


def summarize(rows: list[dict[str, Any]], bad_rows: int, duplicates: int) -> dict[str, Any]:
    accepted = [row for row in rows if row.get("gate_status") == "accepted"]
    gated = [row for row in rows if row.get("gate_status") == "gated"]
    quarantine = [row for row in rows if row.get("gate_reason") == "residual_family_quarantine"]
    accepted_plus_quarantine = len(accepted) + len(quarantine)
    errors = [v for row in accepted if (v := to_float(row.get("close_diff_bps"))) is not None]
    latencies = [v for row in accepted if (v := to_float(row.get("ready_delay_ms"))) is not None]
    accepted_side_errors = sum(
        1 for row in accepted if row.get("side_match_vs_rtds_open") is False
    )
    accepted_hard_count = sum(
        1 for row in accepted if (to_float(row.get("close_diff_bps")) or 0.0) >= 5.0
    )
    latest_round_end_ts = max(
        (to_int(row.get("round_end_ts")) or 0 for row in rows),
        default=0,
    )
    return {
        "rows": len(rows),
        "bad_json_rows": bad_rows,
        "duplicate_rows": duplicates,
        "latest_round_end_ts": latest_round_end_ts,
        "accepted": len(accepted),
        "gated": len(gated),
        "quarantine": len(quarantine),
        "accepted_plus_quarantine": accepted_plus_quarantine,
        "quarantine_share": (
            len(quarantine) / accepted_plus_quarantine if accepted_plus_quarantine else None
        ),
        "accepted_side_errors": accepted_side_errors,
        "accepted_hard_count": accepted_hard_count,
        "accepted_mean_bps": (sum(errors) / len(errors)) if errors else None,
        "accepted_max_bps": max(errors) if errors else 0.0,
        "accepted_p95_bps": percentile(errors, 0.95),
        "accepted_latency_p95_ms": percentile(latencies, 0.95),
        "accepted_latency_max_ms": max(latencies) if latencies else None,
        "status_counts": dict(Counter(str(row.get("gate_status", "")) for row in rows)),
        "gated_reasons": dict(Counter(str(row.get("gate_reason") or "<empty>") for row in gated)),
        "accepted_levels": dict(Counter(str(row.get("gate_key_level") or "<empty>") for row in accepted)),
        "quarantine_families": dict(
            Counter(
                "|".join(
                    [
                        str(row.get("symbol", "")),
                        str(row.get("source_subset", "")),
                        str(row.get("rule", "")),
                    ]
                )
                for row in quarantine
            )
        ),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)

    def csv_value(value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))
        return value

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_value(row.get(key, "")) for key in fields})


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize compact local-agg round evidence JSONL files.")
    ap.add_argument("--root", default="data/compact_evidence/local-agg-challenger")
    ap.add_argument("--min-round-end-ts", type=int, default=None)
    ap.add_argument("--phase", default="final")
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    rows, bad_rows = load_rows(Path(args.root))
    rows = filter_rows(rows, args.min_round_end_ts, args.phase)
    rows, duplicates = dedupe_rows(rows)
    summary = summarize(rows, bad_rows, duplicates)

    if args.summary_json:
        path = Path(args.summary_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.out_csv:
        write_csv(Path(args.out_csv), rows)

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
