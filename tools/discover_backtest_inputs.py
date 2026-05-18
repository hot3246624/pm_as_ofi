#!/usr/bin/env python3
"""Discover allowed backtest input labels without touching raw/replay data.

The discovery contract is intentionally narrow:
- strict V2 taker-buy cache dirs must have CACHE_MANIFEST.json.
- completion_unwind_event_store_v2 dirs must have EVENT_STORE_MANIFEST.json
  and event_store.duckdb.
- labels containing 20260514 or 20260515 are excluded from normal research
  because public market_ws L1/L2 capture was degraded on those days.
- public_account_execution_truth_v1 is discovered separately for B27/RWO
  public-account audit/proxy truth.

This script only inspects manifests and file existence. It never scans raw,
replay, replay_published, or large SQLite contents.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CACHE_ROOT = Path("/mnt/poly-cache/taker_buy_signal_core_v2_strict_l1")
DEFAULT_STORE_ROOTS = [
    Path("/mnt/poly-verification-store/completion_unwind_event_store_v2"),
    Path("/mnt/poly-verification/completion_unwind_event_store_v2"),
]
DEFAULT_PUBLIC_ACCOUNT_ROOT = Path(
    "/mnt/poly-verification-store/public_account_execution_truth_v1/20260502_20260513"
)
FORBIDDEN_LABEL_PARTS = ("20260514", "20260515")


def label_allowed(label: str) -> bool:
    return not any(part in label for part in FORBIDDEN_LABEL_PARTS)


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def discover_cache(root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not root.exists():
        return out
    for p in sorted(x for x in root.iterdir() if x.is_dir()):
        if not label_allowed(p.name):
            continue
        manifest = p / "CACHE_MANIFEST.json"
        db = p / "cache.duckdb"
        if not manifest.exists():
            continue
        out.append(
            {
                "label": p.name,
                "path": str(p),
                "manifest": str(manifest),
                "cache_duckdb": str(db) if db.exists() else None,
                "cache_duckdb_exists": db.exists(),
            }
        )
    return out


def discover_store_roots(roots: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    root_status: list[dict[str, Any]] = []
    by_label: dict[str, dict[str, Any]] = {}
    for root in roots:
        status = {"root": str(root), "exists": root.exists(), "labels": []}
        if not root.exists():
            root_status.append(status)
            continue
        for p in sorted(x for x in root.iterdir() if x.is_dir()):
            if not label_allowed(p.name):
                continue
            manifest = p / "EVENT_STORE_MANIFEST.json"
            db = p / "event_store.duckdb"
            dataset = p / "dataset"
            if not (manifest.exists() and db.exists()):
                continue
            payload = load_json(manifest)
            rows = (payload or {}).get("outputs", {}).get("row_count", 0)
            try:
                rows_int = int(rows)
            except Exception:
                rows_int = 0
            label = p.name
            status["labels"].append(label)
            # Preserve the first valid root in the configured priority order.
            by_label.setdefault(
                label,
                {
                    "label": label,
                    "path": str(p),
                    "root": str(root),
                    "manifest": str(manifest),
                    "event_store_duckdb": str(db),
                    "dataset": str(dataset) if dataset.exists() else None,
                    "dataset_exists": dataset.exists(),
                    "row_count": rows_int,
                },
            )
        root_status.append(status)
    return sorted(by_label.values(), key=lambda item: item["label"]), root_status


def discover_public_account(root: Path) -> dict[str, Any]:
    db = root / "event_store.duckdb"
    manifest = root / "EVENT_STORE_MANIFEST.json"
    return {
        "label": root.name,
        "path": str(root),
        "exists": root.exists(),
        "event_store_duckdb": str(db) if db.exists() else None,
        "event_store_duckdb_exists": db.exists(),
        "manifest": str(manifest) if manifest.exists() else None,
        "manifest_exists": manifest.exists(),
        "table": "public_account_execution_events",
        "truth_level": "public_account_audit_proxy_truth_not_private_owner_trade_truth",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    parser.add_argument("--store-root", action="append", type=Path, dest="store_roots")
    parser.add_argument("--public-account-root", type=Path, default=DEFAULT_PUBLIC_ACCOUNT_ROOT)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    store_roots = args.store_roots or DEFAULT_STORE_ROOTS
    cache = discover_cache(args.cache_root)
    stores, store_root_status = discover_store_roots(store_roots)
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rules": {
            "raw_replay_scanned": False,
            "normal_research_complete_dates": "2026-05-02..2026-05-13",
            "forbidden_full_day_backtest_dates": list(FORBIDDEN_LABEL_PARTS),
            "cache_requires": ["CACHE_MANIFEST.json", "label excludes 20260514/20260515"],
            "store_requires": [
                "EVENT_STORE_MANIFEST.json",
                "event_store.duckdb",
                "label excludes 20260514/20260515",
            ],
            "public_account_audit_requires": ["event_store.duckdb"],
            "public_account_audit_limit": "proxy truth only; not private order/cancel/queue truth",
        },
        "cache_root": str(args.cache_root),
        "strict_v2_cache": cache,
        "strict_v2_cache_labels": [item["label"] for item in cache],
        "store_roots_checked": store_root_status,
        "completion_unwind_event_store_v2": stores,
        "completion_unwind_event_store_v2_labels": [item["label"] for item in stores],
        "public_account_execution_truth_v1": discover_public_account(args.public_account_root),
    }
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
