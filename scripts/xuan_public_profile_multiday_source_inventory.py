#!/usr/bin/env python3
"""Inventory in-worktree public-profile sources for multi-day validation.

This local-only tool only inspects current-worktree artifacts and allowlisted
source files. It does not fetch public data, read external worktrees, start
services/shared WS/local agg, use SSH, run shadows, or scan raw/replay stores.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_public_profile_multiday_source_inventory"
CONTRACT_NAME = "xuan_public_profile_multiday_source_inventory_v1"
DEFAULT_PUBLIC_INPUT_ROOTS = (
    Path("xuan_research_artifacts/xuan_public_leaderboard_candidate_fast_screen_20260524T035500Z/public_inputs"),
    Path("xuan_research_artifacts/xuan_public_profile_reset_review_20260524T022800Z/public_inputs"),
    Path("xuan_research_artifacts/xuan_public_profile_reset_review_20260524T021417Z/public_inputs"),
    Path(
        "xuan_research_artifacts/"
        "xuan_b27_dplus_public_profile_two_account_complete_set_review_20260522T113113Z/"
        "public_inputs"
    ),
)
DEFAULT_MANIFESTS = (
    Path("xuan_research_artifacts/xuan_public_profile_reset_review_20260524T022800Z/manifest.json"),
    Path("xuan_research_artifacts/xuan_public_leaderboard_candidate_fast_screen_20260524T035500Z/manifest.json"),
    Path("xuan_research_artifacts/xuan_public_profile_next_lead_gate_20260524T091916Z/manifest.json"),
    Path("xuan_research_artifacts/xuan_ce25_three_day_input_source_readiness_20260524T115616Z/manifest.json"),
    Path("xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_spec_20260524T122500Z/manifest.json"),
    Path("xuan_research_artifacts/xuan_b55_ce25_non_fixture_input_source_review_20260524T090916Z/manifest.json"),
)
ALLOWLISTED_SOURCE_FILES = (
    Path("scripts/xuan_public_profile_reset_review.py"),
    Path("scripts/xuan_public_leaderboard_corrected_new_mtm_audit.py"),
    Path("scripts/xuan_public_profile_next_lead_gate.py"),
    Path("scripts/xuan_ce25_three_day_input_source_readiness.py"),
    Path("scripts/xuan_ce25_three_day_pair_cost_validation_spec.py"),
    Path("scripts/xuan_b55_ce25_public_activity_entry_exporter_fixture.py"),
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
WALLET_LABELS = {
    "0xce25e214d5cfe4f459cf67f08df581885aae7fdc": "ce25",
    "0xb55fa1296e6ec55d0ce53d93b9237389f11764d4": "b55",
    "0x04b6d7e930cf9e493c5e6ef24b496294f95594c8": "04b6",
    "0x9f5ffe76a818dce37c70f947998b52b70671a008": "9f5",
    "0x89b5cdaaa4866c1e738406712012a630b4078beb": "ohanism",
    "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d": "gabagool22",
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a": "0x8dxd",
}
MIN_MULTIDAY_PUBLIC_ROWS = 500
MIN_MULTIDAY_DAYS = 3


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


def safe_load_dict(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    ok, reason = path_safe(path, root)
    if not ok:
        return None, reason
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


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


def utc_day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()


def bjt_day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(timezone(timedelta(hours=8))).date().isoformat()


def wallet_label(wallet: str) -> str:
    return WALLET_LABELS.get(wallet.lower(), wallet.lower()[:10])


def read_rows(path: Path) -> list[dict[str, Any]]:
    try:
        rows = load_json(path)
    except FileNotFoundError:
        return []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def public_source_summary(root: Path, wallet_dir: Path, root_dir: Path) -> dict[str, Any]:
    wallet = wallet_dir.name.lower()
    trade_path = wallet_dir / "activity_trade_rows.json"
    all_path = wallet_dir / "activity_all_rows.json"
    position_path = wallet_dir / "positions_rows.json"
    trade_rows = read_rows(trade_path)
    all_rows = read_rows(all_path)
    position_rows = read_rows(position_path)
    rows = trade_rows or all_rows
    timestamps = [ts for ts in (normalize_ts(row.get("timestamp")) for row in rows) if ts is not None]
    keys = sorted({str(key) for row in rows for key in row.keys()})
    utc_days = sorted({utc_day(ts) for ts in timestamps})
    bjt_days = sorted({bjt_day(ts) for ts in timestamps})
    has_required_public_fields = all(
        field in keys
        for field in ("conditionId", "slug", "timestamp", "price", "size", "usdcSize", "side", "type")
    )
    multiday_ready = (
        len(bjt_days) >= MIN_MULTIDAY_DAYS
        and len(rows) >= MIN_MULTIDAY_PUBLIC_ROWS
        and has_required_public_fields
    )
    validation_command = (
        "python3 scripts/xuan_public_profile_multiday_validation_spec.py "
        f"--public-input {trade_path} "
        "--output-dir xuan_research_artifacts/xuan_public_profile_multiday_validation_score_<UTC>"
    )
    return {
        "account": wallet_label(wallet),
        "wallet": wallet,
        "source_root": str(root_dir),
        "activity_trade_rows_path": str(trade_path),
        "activity_trade_rows_exists": trade_path.exists(),
        "activity_all_rows_path": str(all_path),
        "activity_all_rows_exists": all_path.exists(),
        "positions_rows_path": str(position_path),
        "positions_rows_exists": position_path.exists(),
        "trade_row_count": len(trade_rows),
        "all_row_count": len(all_rows),
        "position_row_count": len(position_rows),
        "row_count_used": len(rows),
        "timestamp_min": min(timestamps) if timestamps else None,
        "timestamp_max": max(timestamps) if timestamps else None,
        "utc_days": utc_days,
        "utc_day_count": len(utc_days),
        "bjt_days": bjt_days,
        "bjt_day_count": len(bjt_days),
        "keys": keys,
        "has_required_public_fields": has_required_public_fields,
        "source_is_multiday_candidate": multiday_ready,
        "validation_command": validation_command if multiday_ready else None,
        "blockers": []
        if multiday_ready
        else [
            blocker
            for blocker, active in (
                ("fewer_than_3_bjt_days", len(bjt_days) < MIN_MULTIDAY_DAYS),
                ("insufficient_public_rows", len(rows) < MIN_MULTIDAY_PUBLIC_ROWS),
                ("required_public_activity_fields_missing", not has_required_public_fields),
            )
            if active
        ],
    }


def inventory_public_inputs(roots: list[Path], root: Path) -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    rejected_roots: list[dict[str, str]] = []
    for input_root in roots:
        ok, reason = path_safe(input_root, root)
        if not ok:
            rejected_roots.append({"path": str(input_root), "reason": reason or "unsafe"})
            continue
        if not input_root.exists():
            rejected_roots.append({"path": str(input_root), "reason": "missing"})
            continue
        for wallet_dir in sorted(path for path in input_root.iterdir() if path.is_dir()):
            sources.append(public_source_summary(input_root, wallet_dir, input_root))
    by_account: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in sources:
        by_account[source["account"]].append(source)
    account_rollup: dict[str, Any] = {}
    for account, items in sorted(by_account.items()):
        best = max(items, key=lambda item: (item["bjt_day_count"], item["row_count_used"]))
        account_rollup[account] = {
            "source_count": len(items),
            "best_source_path": best["activity_trade_rows_path"],
            "best_bjt_day_count": best["bjt_day_count"],
            "best_utc_day_count": best["utc_day_count"],
            "best_row_count": best["row_count_used"],
            "best_bjt_days": best["bjt_days"],
            "has_multiday_candidate": any(item["source_is_multiday_candidate"] for item in items),
            "candidate_sources": [
                {
                    "path": item["activity_trade_rows_path"],
                    "bjt_day_count": item["bjt_day_count"],
                    "row_count": item["row_count_used"],
                    "validation_command": item["validation_command"],
                }
                for item in items
                if item["source_is_multiday_candidate"]
            ],
            "blockers": sorted({blocker for item in items for blocker in item["blockers"]}),
        }
    return {
        "sources": sources,
        "rejected_roots": rejected_roots,
        "account_rollup": account_rollup,
    }


def manifest_summary(path: Path, root: Path) -> dict[str, Any]:
    manifest, error = safe_load_dict(path, root)
    if error or manifest is None:
        return {"path": str(path), "exists": False, "error": error}
    ranking = manifest.get("research_ranking") if isinstance(manifest.get("research_ranking"), dict) else {}
    promotion = manifest.get("promotion_gate") if isinstance(manifest.get("promotion_gate"), dict) else {}
    return {
        "path": str(path),
        "exists": True,
        "artifact": manifest.get("artifact"),
        "decision_label": manifest.get("decision_label"),
        "next_executable_action": manifest.get("next_executable_action"),
        "research_status": ranking.get("status") or ranking.get("label") or ranking.get("decision"),
        "strategy_evidence": bool(ranking.get("strategy_evidence", False)),
        "promotion_gate_passed": bool(promotion.get("passed", False)),
    }


def source_code_summary(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe(path, root)
    if not ok:
        return {"path": str(path), "exists": False, "error": reason}
    if not path.exists():
        return {"path": str(path), "exists": False, "error": "missing"}
    text = path.read_text(encoding="utf-8", errors="replace")
    lower = text.lower()
    return {
        "path": str(path),
        "exists": True,
        "mentions_public_activity_rows": "activity_trade_rows" in text,
        "mentions_three_day": "three_day" in lower or "72h" in lower,
        "mentions_validation_command": "validation" in lower and "manifest" in lower,
        "mentions_external_export_root": "poly_trans_research" in text,
        "mentions_fetch": "requests." in text or "urllib" in text or "data-api.polymarket" in lower,
    }


def inventory_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "purpose": "Find an in-worktree multi-day public-profile source that can justify the next validation contract.",
        "minimum_ready_source": {
            "bjt_day_count_gte": MIN_MULTIDAY_DAYS,
            "row_count_gte": MIN_MULTIDAY_PUBLIC_ROWS,
            "required_public_activity_fields": [
                "conditionId",
                "slug",
                "timestamp",
                "price",
                "size",
                "usdcSize",
                "side",
                "type",
            ],
            "source_path_policy": "must be inside the current pm_as_ofi-xuan-research worktree",
        },
        "rejected_substitutes": [
            "external worktree exports",
            "single-day or two-day public rows",
            "fixtures/synthetic rows",
            "price cap/static asset-timeframe filters",
            "realized pair_cost as live criterion",
            "D+ failed micro-deficit/ledger/tiny-deficit/closed-cycle families",
            "unobservable direction/fair-probability claims",
        ],
    }


def build_manifest(args: argparse.Namespace, root: Path, output_dir: Path) -> dict[str, Any]:
    ok_output, output_reason = path_safe(output_dir, root)
    if not ok_output:
        raise RuntimeError(f"unsafe output dir: {output_reason}: {output_dir}")
    input_roots = list(args.public_input_root or DEFAULT_PUBLIC_INPUT_ROOTS)
    manifests = list(args.manifest or DEFAULT_MANIFESTS)
    public_inventory = inventory_public_inputs(input_roots, root)
    ready_sources = [
        source for source in public_inventory["sources"] if source.get("source_is_multiday_candidate")
    ]
    best_ready = max(ready_sources, key=lambda item: (item["bjt_day_count"], item["row_count_used"]), default=None)
    decision = "KEEP" if best_ready else "UNKNOWN"
    label = (
        "KEEP_PUBLIC_PROFILE_MULTIDAY_SOURCE_READY"
        if best_ready
        else "UNKNOWN_PUBLIC_PROFILE_MULTIDAY_SOURCE_NOT_IN_CURRENT_WORKTREE"
    )
    manifest_summaries = [manifest_summary(path, root) for path in manifests]
    source_summaries = [source_code_summary(path, root) for path in ALLOWLISTED_SOURCE_FILES]
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "public_profile_multiday_source_inventory",
        "decision": decision,
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
            "public_input_roots": [str(path) for path in input_roots],
            "manifests": [str(path) for path in manifests],
            "allowlisted_source_files": [str(path) for path in ALLOWLISTED_SOURCE_FILES],
        },
        "inventory_contract": inventory_contract(),
        "public_input_inventory": public_inventory,
        "manifest_summaries": manifest_summaries,
        "source_code_summaries": source_summaries,
        "selected_multiday_source": best_ready
        and {
            "account": best_ready["account"],
            "wallet": best_ready["wallet"],
            "source_path": best_ready["activity_trade_rows_path"],
            "bjt_days": best_ready["bjt_days"],
            "bjt_day_count": best_ready["bjt_day_count"],
            "row_count": best_ready["row_count_used"],
            "validation_command": best_ready["validation_command"],
        },
        "research_ranking": {
            "status": label,
            "strategy_evidence": False,
            "safe_in_worktree_multiday_source_ready": bool(best_ready),
            "no_order_diagnostic_allowed": False,
            "accounts_reviewed": sorted(public_inventory["account_rollup"].keys()),
            "account_day_counts": {
                account: {
                    "best_bjt_day_count": info["best_bjt_day_count"],
                    "best_row_count": info["best_row_count"],
                    "has_multiday_candidate": info["has_multiday_candidate"],
                }
                for account, info in public_inventory["account_rollup"].items()
            },
            "exact_blockers": []
            if best_ready
            else [
                "no_current_worktree_public_profile_source_with_at_least_3_bjt_days",
                "ce25_three_day_source_absent",
                "external_or_single_day_evidence_must_not_be_substituted",
            ],
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "PUBLIC_PROFILE_SOURCE_INVENTORY_ONLY_NOT_STRATEGY_OR_PROMOTION_EVIDENCE",
        },
        "next_executable_action": (
            (
                "Implement a local-only validation contract for the selected in-worktree multi-day public source "
                f"{best_ready['activity_trade_rows_path']} and keep it proxy-only; do not start no-order diagnostics."
            )
            if best_ready
            else (
                "Mark public-profile mimicability input-blocked until a safe in-worktree 72h public export is available, "
                "or pivot to another research line with current-worktree multi-day evidence; do not start no-order diagnostics."
            )
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--public-input-root", type=Path, action="append")
    parser.add_argument("--manifest", type=Path, action="append")
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
                "safe_in_worktree_multiday_source_ready": manifest["research_ranking"][
                    "safe_in_worktree_multiday_source_ready"
                ],
                "account_day_counts": manifest["research_ranking"]["account_day_counts"],
                "selected_multiday_source": manifest["selected_multiday_source"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
