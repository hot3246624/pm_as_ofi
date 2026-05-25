#!/usr/bin/env python3
"""Review local strict-cache strategy discovery grid expansion results."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_strategy_discovery_grid_expansion_review"
DEFAULT_SELECTOR = Path(
    "xuan_research_artifacts/"
    "xuan_strategy_discovery_train_holdout_selector_20260525T061500Z/"
    "manifest.json"
)
DEFAULT_CAP100 = Path(
    "xuan_research_artifacts/"
    "xuan_strategy_discovery_symmetric_activation_grid_expansion_20260525T062000Z/"
    "manifest.json"
)
DEFAULT_CAP102 = Path(
    "xuan_research_artifacts/"
    "xuan_strategy_discovery_symmetric_activation_grid_expansion_cap102_20260525T062500Z/"
    "manifest.json"
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


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    if path_is_forbidden(path):
        return None, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return None, "outside_current_worktree"
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def qualify_count(manifest: dict[str, Any] | None) -> int:
    if not isinstance(manifest, dict):
        return 0
    qualified = manifest.get("qualified")
    return len(qualified) if isinstance(qualified, list) else 0


def grid_config(manifest: dict[str, Any] | None) -> dict[str, Any]:
    return manifest.get("config", {}) if isinstance(manifest, dict) else {}


def selected_candidate(manifest: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(manifest, dict):
        return None
    rows = [row for row in manifest.get("qualified", []) if isinstance(row, dict)]
    if not rows:
        return None
    rows.sort(
        key=lambda row: (
            as_float(row.get("holdout_worst_net_fee_after")),
            as_float(row.get("holdout_worst_day")),
            as_int(row.get("holdout_pair_actions")),
            -as_float(row.get("holdout_residual_qty_rate"), 1.0),
        ),
        reverse=True,
    )
    return rows[0]


def low_residual_candidate(manifest: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(manifest, dict):
        return None
    rows = [row for row in manifest.get("qualified", []) if isinstance(row, dict)]
    if not rows:
        return None
    rows.sort(
        key=lambda row: (
            -as_float(row.get("holdout_residual_qty_rate"), 1.0),
            as_float(row.get("holdout_worst_net_fee_after")),
            as_float(row.get("holdout_worst_day")),
        ),
        reverse=True,
    )
    return rows[0]


def summarize_top_rows(manifest: dict[str, Any] | None, limit: int = 8) -> list[dict[str, Any]]:
    if not isinstance(manifest, dict):
        return []
    rows = [row for row in manifest.get("qualified", []) if isinstance(row, dict)]
    rows.sort(
        key=lambda row: (
            as_float(row.get("holdout_worst_net_fee_after")),
            as_float(row.get("holdout_worst_day")),
            as_int(row.get("holdout_pair_actions")),
            -as_float(row.get("holdout_residual_qty_rate"), 1.0),
        ),
        reverse=True,
    )
    return rows[:limit]


def explain_cap100(manifest: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        return {"status": "UNKNOWN_CAP100_MISSING"}
    return {
        "status": "DISCARD_CAP100_OVERSTRICT_EMPTY_INPUT" if qualify_count(manifest) == 0 else "CAP100_HAS_QUALIFIED_ROWS",
        "qualified_count": qualify_count(manifest),
        "filtered_row_count": manifest.get("filtered_row_count"),
        "row_count": manifest.get("row_count"),
        "config": grid_config(manifest),
        "interpretation": (
            "strict_l1_pair_cap=1.00 removes the usable strict-cache opportunity surface in this probe; "
            "it should not be used as the next training entry point."
        ),
    }


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    root = Path.cwd()
    selector, selector_error = safe_load(args.selector_manifest, root)
    cap100, cap100_error = safe_load(args.cap100_manifest, root)
    cap102, cap102_error = safe_load(args.cap102_manifest, root)
    selected = selected_candidate(cap102)
    low_residual = low_residual_candidate(cap102)
    cap102_count = qualify_count(cap102)

    selector_ready = (
        isinstance(selector, dict)
        and selector.get("decision_label") == "KEEP_STRATEGY_DISCOVERY_TRAIN_HOLDOUT_SELECTOR_READY"
        and selector.get("selected_next_lane", {}).get("lane") == "symmetric_activation_strict_cache_grid_expansion"
    )
    cap102_ready = (
        isinstance(cap102, dict)
        and cap102.get("status") == "UNKNOWN_ACTIVATION_REPAIR_LEAD"
        and cap102_count >= 20
        and selected is not None
    )

    blockers: list[str] = []
    if selector_error:
        blockers.append(f"selector:{selector_error}")
    if cap100_error:
        blockers.append(f"cap100:{cap100_error}")
    if cap102_error:
        blockers.append(f"cap102:{cap102_error}")
    if not selector_ready:
        blockers.append("selector_not_ready_for_grid_expansion")
    if not cap102_ready:
        blockers.append("cap102_grid_has_no_sufficient_qualified_rows")

    decision = "KEEP" if not blockers else "UNKNOWN"
    label = (
        "KEEP_STRATEGY_DISCOVERY_GRID_EXPANSION_CAP102_LEAD_READY"
        if decision == "KEEP"
        else "UNKNOWN_STRATEGY_DISCOVERY_GRID_EXPANSION_REVIEW_BLOCKED"
    )

    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "decision": decision,
        "decision_label": label,
        "source_inputs": {
            "selector_manifest": str(args.selector_manifest),
            "selector_error": selector_error,
            "cap100_manifest": str(args.cap100_manifest),
            "cap100_error": cap100_error,
            "cap102_manifest": str(args.cap102_manifest),
            "cap102_error": cap102_error,
        },
        "cap100_result": explain_cap100(cap100),
        "cap102_result": {
            "status": cap102.get("status") if isinstance(cap102, dict) else None,
            "qualified_count": cap102_count,
            "config": grid_config(cap102),
            "selected_by_holdout_worst_net": selected,
            "selected_by_lowest_holdout_residual": low_residual,
            "top_rows_by_holdout_worst_net": summarize_top_rows(cap102),
        },
        "interpretation": {
            "cap100": "discard as over-strict empty training surface",
            "cap102": (
                "keep as local train/holdout research lead; this is not no-order acceptance because the last "
                "fresh no-order run still failed normalized risk budget"
            ),
            "ce25_prior_role": (
                "use ce25 projected pair-cost/residual concepts as feature priors for future predicates, but "
                "do not use public-profile realized pair_cost as a live gate"
            ),
        },
        "blockers": blockers,
        "next_executable_action": (
            "Prepare an exact bounded no-order diagnostic proposal for the selected cap102 candidate only if "
            "the user approves it later; until then run local-only review comparing selected cap102 candidate "
            "against previous edge=0.07/window=7.5 no-order failure and do not start another shadow."
        ),
        "research_ranking": {
            "status": label,
            "strategy_evidence": False,
            "selected_local_candidate": selected,
            "no_order_diagnostic_allowed": False,
            "cap100_discarded": qualify_count(cap100) == 0,
            "cap102_qualified_count": cap102_count,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "GRID_EXPANSION_RESEARCH_LEAD_NOT_PROMOTION_EVIDENCE",
        },
        "scope": {
            "local_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "orders_cancels_redeems_sent": False,
            "trading_behavior_changed": False,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selector-manifest", type=Path, default=DEFAULT_SELECTOR)
    parser.add_argument("--cap100-manifest", type=Path, default=DEFAULT_CAP100)
    parser.add_argument("--cap102-manifest", type=Path, default=DEFAULT_CAP102)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
