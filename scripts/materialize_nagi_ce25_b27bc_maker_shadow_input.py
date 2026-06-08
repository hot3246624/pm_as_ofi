#!/usr/bin/env python3
"""Materialize local event JSONL into maker-shadow input CSV.

This converter is local-only and review-only. It reads already-local
``*.events.jsonl`` files, extracts public-trade candidate rows, and writes a
CSV that can be checked by ``inventory_nagi_ce25_b27bc_maker_shadow_inputs.py``
and then consumed by ``run_nagi_ce25_b27bc_maker_shadow.py``.

By default, paths containing smoke/fixture are excluded so synthetic artifacts
cannot become strategy evidence accidentally.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


MATERIALIZER_STATUS = (
    "KEEP_NAGI_CE25_B27BC_MAKER_SHADOW_INPUT_MATERIALIZER_REVIEWED_LOCAL_ONLY_NOT_READY"
)

OUTPUT_FIELDS = [
    "source_path",
    "source_line",
    "source_kind",
    "source",
    "source_sequence_id",
    "window_id",
    "slug",
    "condition_id",
    "ts_ms",
    "remaining_s",
    "side",
    "yes_bid",
    "no_bid",
    "public_trade_px",
    "public_trade_qty",
    "queue_visible_qty",
    "visible_depth_qty",
    "l2_age_ms",
    "align_lag_ms",
    "materialized_depth_source",
    "maker_truth",
    "own_telemetry",
]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def to_float(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value == value and abs(value) != float("inf") else None


def to_int(raw: Any) -> int | None:
    value = to_float(raw)
    return int(value) if value is not None else None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_excluded_path(path: Path, *, include_smoke: bool) -> bool:
    if include_smoke:
        return False
    lowered = {part.lower() for part in path.parts}
    return any("smoke" in part or "fixture" in part for part in lowered)


def iter_event_paths(roots: Iterable[Path], *, include_smoke: bool, max_files: int) -> list[Path]:
    paths: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        candidates: list[Path]
        if root.is_file():
            candidates = [root] if root.name.endswith(".jsonl") else []
        else:
            candidates = sorted(root.rglob("*.jsonl"))
        for path in candidates:
            name = path.name.lower()
            if "event" not in name:
                continue
            if is_excluded_path(path, include_smoke=include_smoke):
                continue
            paths.append(path)
            if len(paths) >= max_files:
                return paths
    return paths


def slug_round_start(slug: str) -> int | None:
    tail = slug.rsplit("-", 1)[-1] if slug else ""
    return int(tail) if tail.isdigit() else None


def remaining_seconds(obj: dict[str, Any]) -> float | None:
    for name in ("remaining_s", "remaining_secs", "seconds_to_end", "time_to_expiry_s"):
        value = to_float(obj.get(name))
        if value is not None:
            return max(0.0, value)
    offset = to_float(obj.get("offset_s"))
    if offset is not None:
        return max(0.0, 300.0 - offset)
    slug = str(obj.get("slug") or "")
    start = slug_round_start(slug)
    ts_ms = to_int(obj.get("ts_ms") or obj.get("accepted_ts_ms") or obj.get("placed_ts_ms"))
    if start is not None and ts_ms is not None:
        return max(0.0, start + 300.0 - ts_ms / 1000.0)
    return None


def side_bid_field(side: str) -> str:
    return "yes_bid" if side.upper() in {"YES", "UP"} else "no_bid"


def materialize_event(obj: dict[str, Any], *, source_path: Path, line_no: int) -> dict[str, Any] | None:
    slug = str(obj.get("slug") or "")
    side = str(obj.get("side") or "").upper()
    if not slug or side not in {"YES", "NO", "UP", "DOWN"}:
        return None
    if "btc-updown-5m" not in slug.lower():
        return None
    public_trade_size = to_float(obj.get("public_trade_size") or obj.get("public_trade_qty"))
    public_trade_px = to_float(obj.get("public_trade_px") or obj.get("public_trade_price"))
    if public_trade_size is None or public_trade_size <= 0:
        return None
    bid_px = to_float(obj.get("price") or obj.get("seed_px") or obj.get(side_bid_field(side)))
    if bid_px is None or bid_px <= 0:
        return None
    ts_ms = to_int(obj.get("ts_ms") or obj.get("accepted_ts_ms") or obj.get("placed_ts_ms"))
    if ts_ms is None:
        return None

    out = {field: "" for field in OUTPUT_FIELDS}
    out.update(
        {
            "source_path": str(source_path),
            "source_line": line_no,
            "source_kind": obj.get("kind") or "",
            "source": obj.get("source") or "",
            "source_sequence_id": obj.get("source_sequence_id") or obj.get("market_md_source_sequence_id") or "",
            "window_id": dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d"),
            "slug": slug,
            "condition_id": obj.get("condition_id") or slug,
            "ts_ms": ts_ms,
            "remaining_s": remaining_seconds(obj),
            "side": "YES" if side in {"YES", "UP"} else "NO",
            "public_trade_px": public_trade_px if public_trade_px is not None else "",
            "public_trade_qty": public_trade_size,
            # This is a public-trade-size proxy, not L2 truth. It lets the
            # downstream no-order pipeline test queue-haircut hypotheses while
            # keeping maker/private truth claims false.
            "queue_visible_qty": public_trade_size,
            "visible_depth_qty": public_trade_size,
            "l2_age_ms": obj.get("l2_age_ms") or "",
            "align_lag_ms": obj.get("align_lag_ms") or "",
            "materialized_depth_source": "public_trade_size_proxy_not_l2_depth",
            "maker_truth": "public_trade_queue_proxy_only",
            "own_telemetry": False,
        }
    )
    out[side_bid_field(side)] = bid_px
    other_bid = to_float(obj.get("no_bid" if side_bid_field(side) == "yes_bid" else "yes_bid"))
    if other_bid is not None:
        out["no_bid" if side_bid_field(side) == "yes_bid" else "yes_bid"] = other_bid
    return out


def materialize_file(path: Path, *, max_rows_per_file: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    parsed = 0
    invalid_json = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            if len(rows) >= max_rows_per_file:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                invalid_json += 1
                continue
            if not isinstance(obj, dict):
                continue
            parsed += 1
            row = materialize_event(obj, source_path=path, line_no=line_no)
            if row is not None:
                rows.append(row)
    return rows, {
        "path": str(path),
        "sha256": sha256_file(path),
        "parsed_json_rows": parsed,
        "materialized_rows": len(rows),
        "invalid_json_rows": invalid_json,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def default_output_dir() -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("data/exports") / f"nagi_ce25_b27bc_maker_shadow_materialized_input_{stamp}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--root",
        action="append",
        default=None,
        help="Event JSONL file or directory root. Defaults to xuan_research_artifacts and logs.",
    )
    p.add_argument("--output-dir", default=None)
    p.add_argument("--include-smoke", action="store_true")
    p.add_argument("--max-files", type=int, default=500)
    p.add_argument("--max-rows-per-file", type=int, default=100_000)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    roots = [Path(p) for p in (args.root or ["xuan_research_artifacts", "logs"])]
    out = Path(args.output_dir) if args.output_dir else default_output_dir()
    paths = iter_event_paths(roots, include_smoke=args.include_smoke, max_files=args.max_files)
    all_rows: list[dict[str, Any]] = []
    source_summaries: list[dict[str, Any]] = []
    for path in paths:
        rows, summary = materialize_file(path, max_rows_per_file=max(1, args.max_rows_per_file))
        all_rows.extend(rows)
        source_summaries.append(summary)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "nagi_ce25_b27bc_maker_shadow_input.csv"
    write_csv(csv_path, all_rows)
    manifest = {
        "generated_at": utc_now(),
        "status": MATERIALIZER_STATUS,
        "evidence_level": "local_public_event_materialization_only",
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "roots": [str(root) for root in roots],
        "include_smoke": bool(args.include_smoke),
        "event_files_seen": len(paths),
        "materialized_rows": len(all_rows),
        "output_csv": str(csv_path),
        "depth_source": "public_trade_size_proxy_not_l2_depth",
        "order_minimum_guard": {
            "limit_order_min_shares": 5.0,
            "market_order_min_usdc": 1.0,
            "market_orders_used": False,
        },
        "source_summaries": source_summaries,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "status": MATERIALIZER_STATUS,
                "output_dir": str(out),
                "output_csv": str(csv_path),
                "event_files_seen": len(paths),
                "materialized_rows": len(all_rows),
                "include_smoke": bool(args.include_smoke),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
