#!/usr/bin/env python3
"""Inventory local CSV inputs for the NAGI/CE25/B27BC maker-shadow pipeline.

This is a local-only contract checker. It scans bounded CSV samples, classifies
whether each file can feed the no-order maker-shadow pipeline, and reports
whether sampled rows contain CE25-gated, public-touch, >=5-share limit-maker
opportunities. It does not connect to any network and does not execute orders.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import run_nagi_ce25_b27bc_maker_shadow as shadow


INVENTORY_STATUS = (
    "KEEP_NAGI_CE25_B27BC_MAKER_SHADOW_INPUT_INVENTORY_REVIEWED_LOCAL_ONLY_NOT_READY"
)

READY_STATUS = "READY_MAKER_SHADOW_INPUT"


HEADER_GROUPS: dict[str, tuple[str, ...]] = {
    "market_or_slug": ("slug", "market_slug", "market_id"),
    "side": ("side", "market_side", "outcome", "public_trade_side", "first_side"),
    "timing": (
        "remaining_s",
        "remaining_secs",
        "seconds_to_end",
        "secs_to_end",
        "ts_ms",
        "timestamp_ms",
        "market_end_ts_ms",
        "end_ts_ms",
        "round_end_ts_ms",
        "market_end_ts",
        "end_ts",
        "round_end_ts",
    ),
    "bid_or_price": (
        "yes_bid",
        "no_bid",
        "up_bid",
        "down_bid",
        "bid_px",
        "best_bid",
        "maker_bid_px",
        "public_trade_px",
        "public_trade_price",
        "trade_px",
        "trade_price",
        "price",
        "first_price",
    ),
    "public_touch": (
        "public_sell_touch",
        "sell_touch",
        "touch",
        "touched",
        "maker_bid_touched",
        "public_taker_side",
        "public_trade_taker_side",
        "trade_taker_side",
        "taker_side",
        "aggressor_side",
        "public_taker_sell",
        "trade_taker_sell",
        "taker_sell",
    ),
    "visible_depth_or_qty": (
        "yes_bid_top5_size",
        "no_bid_top5_size",
        "yes_top5_bid_size",
        "no_top5_bid_size",
        "bid_top5_size",
        "top5_bid_size",
        "visible_depth_qty",
        "queue_visible_qty",
        "public_trade_qty",
        "public_trade_size",
        "trade_qty",
        "trade_size",
        "size",
        "qty",
    ),
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def norm_header(header: Iterable[str]) -> set[str]:
    return {str(name or "").strip() for name in header if str(name or "").strip()}


def header_has_any(header: set[str], aliases: Iterable[str]) -> bool:
    return any(alias in header for alias in aliases)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_csv_paths(roots: Iterable[Path], *, max_files: int) -> list[Path]:
    paths: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        if root.is_file() and root.suffix.lower() == ".csv":
            paths.append(root)
        elif root.is_dir():
            paths.extend(sorted(root.rglob("*.csv")))
        if len(paths) >= max_files:
            break
    return paths[:max_files]


@dataclass(frozen=True)
class FileAssessment:
    path: str
    status: str
    pipeline_contract: str
    ready_for_shadow_run: bool
    blockers: list[str]
    header_missing_groups: list[str]
    file_size_bytes: int
    sha256: str | None
    sampled_rows: int
    parseable_rows: int
    btc5m_rows: int
    ce25_allowed_rows: int
    public_touch_rows: int
    order_min_ready_rows: int
    insufficient_depth_rows: int
    gate_counts: dict[str, int]
    sample_limit: int

    def to_row(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "status": self.status,
            "pipeline_contract": self.pipeline_contract,
            "ready_for_shadow_run": self.ready_for_shadow_run,
            "blockers": ";".join(self.blockers),
            "header_missing_groups": ";".join(self.header_missing_groups),
            "file_size_bytes": self.file_size_bytes,
            "sha256": self.sha256 or "",
            "sampled_rows": self.sampled_rows,
            "parseable_rows": self.parseable_rows,
            "btc5m_rows": self.btc5m_rows,
            "ce25_allowed_rows": self.ce25_allowed_rows,
            "public_touch_rows": self.public_touch_rows,
            "order_min_ready_rows": self.order_min_ready_rows,
            "insufficient_depth_rows": self.insufficient_depth_rows,
            "gate_counts": json.dumps(self.gate_counts, sort_keys=True),
            "sample_limit": self.sample_limit,
        }


def assess_csv(path: Path, *, cfg: shadow.PipelineConfig, sample_limit: int, max_size_bytes: int) -> FileAssessment:
    file_size = path.stat().st_size
    if file_size > max_size_bytes:
        return FileAssessment(
            path=str(path),
            status="BLOCKED_CSV_TOO_LARGE_FOR_AUTOMATION_SAMPLE",
            pipeline_contract="blocked",
            ready_for_shadow_run=False,
            blockers=["csv_size_exceeds_limit"],
            header_missing_groups=[],
            file_size_bytes=file_size,
            sha256=None,
            sampled_rows=0,
            parseable_rows=0,
            btc5m_rows=0,
            ce25_allowed_rows=0,
            public_touch_rows=0,
            order_min_ready_rows=0,
            insufficient_depth_rows=0,
            gate_counts={},
            sample_limit=sample_limit,
        )

    blockers: list[str] = []
    gate_counts: dict[str, int] = {}
    sampled_rows = 0
    parseable_rows = 0
    btc5m_rows = 0
    ce25_allowed_rows = 0
    public_touch_rows = 0
    order_min_ready_rows = 0
    insufficient_depth_rows = 0

    try:
        with path.open(newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            header = norm_header(reader.fieldnames or [])
            missing_groups = [
                group for group, aliases in HEADER_GROUPS.items() if not header_has_any(header, aliases)
            ]
            for idx, row in enumerate(reader):
                if idx >= sample_limit:
                    break
                sampled_rows += 1
                ts_ms = shadow.infer_ts_ms(row, idx)
                side = shadow.infer_row_side(row)
                bid_px = shadow.infer_bid_px(row, side) if side else None
                if shadow.infer_slug(row) and side is not None and bid_px is not None:
                    parseable_rows += 1
                if shadow.infer_asset(row) == "BTC" and shadow.infer_timeframe(row) == "5m":
                    btc5m_rows += 1
                decision = shadow.coverage_decision(row, ts_ms, side, bid_px)
                gate_counts[decision.gate_id] = gate_counts.get(decision.gate_id, 0) + 1
                if not decision.allowed or side is None or bid_px is None:
                    continue
                ce25_allowed_rows += 1
                visible_depth = shadow.infer_visible_depth_qty(row, side)
                public_qty = shadow.infer_trade_qty(row)
                requested_qty = min(cfg.max_shadow_qty, max(public_qty, visible_depth * cfg.queue_conversion))
                fillable_qty = (
                    visible_depth * cfg.queue_conversion / max(cfg.queue_ahead_multiplier, 1e-9)
                )
                touched, _touch_reason = shadow.infer_public_sell_touch(row, bid_px)
                if touched:
                    public_touch_rows += 1
                    if fillable_qty >= cfg.min_shadow_qty and requested_qty >= cfg.min_shadow_qty:
                        order_min_ready_rows += 1
                    else:
                        insufficient_depth_rows += 1
    except OSError as exc:
        return FileAssessment(
            path=str(path),
            status="BLOCKED_CSV_READ_ERROR",
            pipeline_contract="blocked",
            ready_for_shadow_run=False,
            blockers=[f"read_error:{exc.__class__.__name__}"],
            header_missing_groups=[],
            file_size_bytes=file_size,
            sha256=None,
            sampled_rows=0,
            parseable_rows=0,
            btc5m_rows=0,
            ce25_allowed_rows=0,
            public_touch_rows=0,
            order_min_ready_rows=0,
            insufficient_depth_rows=0,
            gate_counts={},
            sample_limit=sample_limit,
        )

    if sampled_rows == 0:
        blockers.append("empty_or_header_only_csv")
    if parseable_rows == 0:
        blockers.append("no_parseable_market_side_price_rows")
    if btc5m_rows == 0:
        blockers.append("no_btc5m_rows_in_sample")
    if ce25_allowed_rows == 0:
        blockers.append("no_ce25_coverage_rows_in_sample")
    if public_touch_rows == 0:
        blockers.append("no_public_sell_touch_rows_in_sample")
    if order_min_ready_rows == 0:
        blockers.append("no_ge_5_share_maker_shadow_rows_in_sample")

    pipeline_contract = "compatible" if sampled_rows > 0 and parseable_rows > 0 else "blocked"
    ready = order_min_ready_rows > 0
    if ready:
        status = READY_STATUS
    elif pipeline_contract == "compatible":
        status = "UNKNOWN_COMPATIBLE_LOW_SIGNAL_MAKER_SHADOW_INPUT"
    else:
        status = "BLOCKED_NOT_MAKER_SHADOW_INPUT"

    return FileAssessment(
        path=str(path),
        status=status,
        pipeline_contract=pipeline_contract,
        ready_for_shadow_run=ready,
        blockers=blockers,
        header_missing_groups=missing_groups,
        file_size_bytes=file_size,
        sha256=sha256_file(path),
        sampled_rows=sampled_rows,
        parseable_rows=parseable_rows,
        btc5m_rows=btc5m_rows,
        ce25_allowed_rows=ce25_allowed_rows,
        public_touch_rows=public_touch_rows,
        order_min_ready_rows=order_min_ready_rows,
        insufficient_depth_rows=insufficient_depth_rows,
        gate_counts=gate_counts,
        sample_limit=sample_limit,
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def default_output_dir() -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("data/exports") / f"nagi_ce25_b27bc_maker_shadow_input_inventory_{stamp}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--root",
        action="append",
        default=None,
        help="CSV file or directory root to scan. Defaults to data/inputs, evidence, xuan_research_artifacts.",
    )
    p.add_argument("--output-dir", default=None)
    p.add_argument("--sample-limit", type=int, default=500)
    p.add_argument("--max-files", type=int, default=2500)
    p.add_argument("--max-size-mb", type=float, default=128.0)
    p.add_argument("--queue-conversion", type=float, default=shadow.PipelineConfig.queue_conversion)
    p.add_argument("--queue-ahead-multiplier", type=float, default=shadow.PipelineConfig.queue_ahead_multiplier)
    p.add_argument("--min-shadow-qty", type=float, default=shadow.PipelineConfig.min_shadow_qty)
    p.add_argument("--max-shadow-qty", type=float, default=shadow.PipelineConfig.max_shadow_qty)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    roots = [Path(p) for p in (args.root or ["data/inputs", "evidence", "xuan_research_artifacts"])]
    out = Path(args.output_dir) if args.output_dir else default_output_dir()
    cfg = shadow.PipelineConfig(
        queue_conversion=args.queue_conversion,
        queue_ahead_multiplier=args.queue_ahead_multiplier,
        min_shadow_qty=args.min_shadow_qty,
        max_shadow_qty=args.max_shadow_qty,
    )
    paths = iter_csv_paths(roots, max_files=args.max_files)
    assessments = [
        assess_csv(
            path,
            cfg=cfg,
            sample_limit=max(1, args.sample_limit),
            max_size_bytes=max(1, int(args.max_size_mb * 1024 * 1024)),
        )
        for path in paths
    ]
    ready = [item for item in assessments if item.ready_for_shadow_run]
    compatible = [item for item in assessments if item.pipeline_contract == "compatible"]
    decision = {
        "generated_at": utc_now(),
        "status": INVENTORY_STATUS,
        "evidence_level": "local_input_inventory_only",
        "non_claims": {
            "ready": False,
            "private_truth": False,
            "maker_fill_truth": False,
            "order_execution": False,
            "canary": False,
            "live": False,
        },
        "roots": [str(root) for root in roots],
        "csv_files_seen": len(paths),
        "compatible_input_count": len(compatible),
        "ready_input_count": len(ready),
        "preferred_input_csv": ready[0].path if ready else None,
        "next_step": (
            "run_maker_shadow_pipeline_on_preferred_input_csv"
            if ready
            else "provide_or_materialize_local_csv_matching_maker_shadow_contract"
        ),
        "order_minimum_guard": {
            "limit_order_min_shares": cfg.min_shadow_qty,
            "market_order_min_usdc": cfg.market_order_min_usdc,
            "market_orders_used": False,
        },
        "assessments": [item.to_row() for item in assessments],
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "decision_register.json").write_text(
        json.dumps(decision, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_csv(out / "input_inventory.csv", [item.to_row() for item in assessments])
    print(
        json.dumps(
            {
                "status": INVENTORY_STATUS,
                "output_dir": str(out),
                "csv_files_seen": len(paths),
                "compatible_input_count": len(compatible),
                "ready_input_count": len(ready),
                "preferred_input_csv": ready[0].path if ready else None,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
