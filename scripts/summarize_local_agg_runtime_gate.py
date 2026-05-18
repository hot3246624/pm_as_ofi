#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import re
import statistics
from collections import Counter
from pathlib import Path


FIELD_RE = re.compile(r"(?P<key>[A-Za-z0-9_]+)=(?P<value>[^ ]*)")


def parse_line(line: str) -> dict[str, str]:
    return {m.group("key"): m.group("value") for m in FIELD_RE.finditer(line)}


def to_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


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


def latest_run_dir(logs_root: Path) -> Path:
    runs = sorted(path for path in logs_root.glob("20*") if path.is_dir())
    if not runs:
        raise SystemExit(f"no run directories under {logs_root}")
    return runs[-1]


def load_final_rows(run_dir: Path) -> tuple[list[dict[str, str]], int]:
    by_key: dict[tuple[str, str, str], dict[str, str]] = {}
    duplicate_rows = 0
    for raw_path in glob.glob(str(run_dir / "*.log*")):
        path = Path(raw_path)
        with path.open(errors="ignore") as f:
            for line in f:
                if "local_price_agg_uncertainty_gate_shadow" not in line:
                    continue
                row = parse_line(line)
                if row.get("phase") != "final":
                    continue
                key = (
                    row.get("slug", ""),
                    row.get("round_end_ts", ""),
                    row.get("local_close", ""),
                )
                if key in by_key:
                    duplicate_rows += 1
                by_key[key] = row
    return list(by_key.values()), duplicate_rows


def summarize(run_dir: Path) -> dict:
    rows, duplicate_rows = load_final_rows(run_dir)
    accepted = [row for row in rows if row.get("gate_status") == "accepted"]
    gated = [row for row in rows if row.get("gate_status") == "gated"]
    errors = [
        value
        for value in (to_float(row.get("close_diff_bps")) for row in accepted)
        if value is not None
    ]
    latencies = []
    for row in accepted:
        ready_ms = to_float(row.get("local_ready_ms"))
        round_end_ts = to_float(row.get("round_end_ts"))
        if ready_ms is not None and round_end_ts is not None:
            latencies.append(ready_ms - round_end_ts * 1000.0)
    accepted_side_errors = sum(
        1 for row in accepted if str(row.get("side_match_vs_rtds_open", "")).lower() == "false"
    )
    return {
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "final_rows": len(rows),
        "duplicate_log_rows": duplicate_rows,
        "accepted": len(accepted),
        "gated": len(gated),
        "accepted_side_errors": accepted_side_errors,
        "accepted_mean_bps": (sum(errors) / len(errors)) if errors else None,
        "accepted_max_bps": max(errors) if errors else 0.0,
        "accepted_p95_bps": percentile(errors, 0.95),
        "latency_p95_ms": percentile(latencies, 0.95),
        "latency_max_ms": max(latencies) if latencies else None,
        "status_counts": dict(Counter(row.get("gate_status", "") for row in rows)),
        "gated_reasons": dict(Counter(row.get("gate_reason") or "<empty>" for row in gated)),
        "accepted_levels": dict(Counter(row.get("gate_key_level") or "<empty>" for row in accepted)),
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize runtime local agg uncertainty-gate shadow logs.")
    ap.add_argument("--logs-root", default="logs/local-agg-challenger/runs")
    ap.add_argument("--run-id", default="", help="Run directory name; defaults to latest 20* run")
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--out-csv", default="", help="Optional deduped final shadow rows CSV")
    args = ap.parse_args()

    logs_root = Path(args.logs_root)
    run_dir = logs_root / args.run_id if args.run_id else latest_run_dir(logs_root)
    if not run_dir.is_dir():
        raise SystemExit(f"run directory not found: {run_dir}")
    rows, _ = load_final_rows(run_dir)
    summary = summarize(run_dir)

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
