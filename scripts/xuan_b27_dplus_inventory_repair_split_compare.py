#!/usr/bin/env python3
"""Compare public-audit-covered and uncovered inventory repair probe splits.

This is an offline metadata/artifact reducer. It only reads existing
`xuan_b27_dplus_compliant_inventory_repair_probe` manifests and writes a
small comparison manifest under `xuan_research_artifacts`.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_b27_dplus_inventory_repair_split_compare"


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def f(row: dict[str, Any], key: str) -> float:
    value = row.get(key)
    if value is None:
        return 0.0
    return float(value)


def i(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if value is None:
        return 0
    return int(value)


def compact_split(name: str, manifest: dict[str, Any], path: Path) -> dict[str, Any]:
    best = dict(manifest.get("best_result") or {})
    baseline = dict(manifest.get("baseline_result") or {})
    delta = dict(manifest.get("scoreboard_delta_vs_baseline") or {})
    return {
        "name": name,
        "manifest_path": str(path),
        "status": manifest.get("status"),
        "data_root": manifest.get("data_root"),
        "dataset_type": manifest.get("dataset_type"),
        "labels": manifest.get("labels"),
        "days": manifest.get("days"),
        "market_prefix": manifest.get("market_prefix"),
        "assets": manifest.get("assets"),
        "row_count": manifest.get("row_count"),
        "excluded_20260514_20260515": bool(manifest.get("excluded_20260514_20260515")),
        "contains_20260518": bool(manifest.get("contains_20260518")),
        "includes_public_account_execution_truth_v1": bool(
            manifest.get("includes_public_account_execution_truth_v1")
        ),
        "public_account_truth_missing_days": manifest.get("public_account_truth_missing_days") or [],
        "public_account_truth_level": manifest.get("public_account_truth_level"),
        "can_support_strategy_promotion": bool(manifest.get("can_support_strategy_promotion")),
        "requires_source_of_truth_replay_for_promotion": bool(
            manifest.get("requires_source_of_truth_replay_for_promotion")
        ),
        "best_result": {
            "name": best.get("name"),
            "seed_actions": i(best, "seed_actions"),
            "repair_actions": i(best, "repair_actions"),
            "pair_actions": i(best, "pair_actions"),
            "pair_qty": f(best, "pair_qty"),
            "pair_cost_wavg": f(best, "pair_cost_wavg"),
            "residual_qty": f(best, "residual_qty"),
            "qty_residual_rate": f(best, "qty_residual_rate"),
            "pair_pnl": f(best, "pair_pnl"),
            "net_pnl": f(best, "net_pnl"),
            "worst_residual_net_pnl": f(best, "worst_residual_net_pnl"),
            "stress100_worst_pnl": f(best, "stress100_worst_pnl"),
        },
        "baseline_result": {
            "name": baseline.get("name"),
            "seed_actions": i(baseline, "seed_actions"),
            "pair_actions": i(baseline, "pair_actions"),
            "pair_qty": f(baseline, "pair_qty"),
            "pair_cost_wavg": f(baseline, "pair_cost_wavg"),
            "residual_qty": f(baseline, "residual_qty"),
            "qty_residual_rate": f(baseline, "qty_residual_rate"),
            "pair_pnl": f(baseline, "pair_pnl"),
            "net_pnl": f(baseline, "net_pnl"),
            "worst_residual_net_pnl": f(baseline, "worst_residual_net_pnl"),
            "stress100_worst_pnl": f(baseline, "stress100_worst_pnl"),
        },
        "scoreboard_delta_vs_baseline": delta,
    }


def lead_preserved(split: dict[str, Any]) -> bool:
    delta = split["scoreboard_delta_vs_baseline"]
    best = split["best_result"]
    missing_days = split["public_account_truth_missing_days"]
    return (
        str(split["status"]).startswith("KEEP_")
        and not missing_days
        and int(split["row_count"] or 0) >= 10_000
        and float(delta.get("qty_residual_rate_improvement") or 0.0) >= 0.10
        and float(delta.get("stress100_worst_pnl_delta") or 0.0) > 0.0
        and float(best.get("qty_residual_rate") or 1.0) <= 0.10
        and float(best.get("pair_pnl") or 0.0) > 0.0
    )


def compare_splits(
    covered: dict[str, Any],
    uncovered: dict[str, Any],
    full: dict[str, Any] | None,
) -> tuple[str, dict[str, Any], str]:
    covered_ok = lead_preserved(covered)
    covered_best = covered["best_result"]
    uncovered_best = uncovered["best_result"]
    full_best = (full or {}).get("best_result") or {}
    scoreboard = {
        "covered_lead_preserved": covered_ok,
        "covered_residual_rate": covered_best.get("qty_residual_rate"),
        "covered_stress100_worst_pnl": covered_best.get("stress100_worst_pnl"),
        "covered_worst_residual_net_pnl": covered_best.get("worst_residual_net_pnl"),
        "covered_pair_pnl": covered_best.get("pair_pnl"),
        "covered_net_pnl": covered_best.get("net_pnl"),
        "uncovered_residual_rate": uncovered_best.get("qty_residual_rate"),
        "uncovered_stress100_worst_pnl": uncovered_best.get("stress100_worst_pnl"),
        "uncovered_worst_residual_net_pnl": uncovered_best.get("worst_residual_net_pnl"),
        "uncovered_pair_pnl": uncovered_best.get("pair_pnl"),
        "uncovered_net_pnl": uncovered_best.get("net_pnl"),
    }
    if full_best:
        scoreboard.update(
            {
                "full_residual_rate": full_best.get("qty_residual_rate"),
                "full_stress100_worst_pnl": full_best.get("stress100_worst_pnl"),
                "full_worst_residual_net_pnl": full_best.get("worst_residual_net_pnl"),
                "covered_minus_full_stress100_worst_pnl": round(
                    f(covered_best, "stress100_worst_pnl") - f(full_best, "stress100_worst_pnl"),
                    6,
                ),
                "covered_minus_full_residual_rate": round(
                    f(covered_best, "qty_residual_rate") - f(full_best, "qty_residual_rate"),
                    6,
                ),
            }
        )
    if not covered_ok:
        return (
            "DISCARD_REPAIR_LEAD_NOT_PUBLIC_AUDIT_COVERED",
            scoreboard,
            "covered public-audit split did not preserve the repair lead; kill or redesign before shadow",
        )
    if f(covered_best, "stress100_worst_pnl") >= 0.0:
        return (
            "KEEP_PUBLIC_COVERED_REPAIR_CANDIDATE_NEEDS_SOURCE_TRUTH",
            scoreboard,
            "covered split is stress-positive; test source-of-truth replay and no-order shadow before promotion",
        )
    return (
        "KEEP_PUBLIC_COVERED_REPAIR_LEAD_RESEARCH_ONLY",
        scoreboard,
        "covered split preserves the repair lead but stress100_worst remains negative; test surplus-funded/high-cost repair variants locally next",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare D+ inventory repair split manifests.")
    parser.add_argument("--covered-manifest", required=True)
    parser.add_argument("--uncovered-manifest", required=True)
    parser.add_argument("--full-manifest")
    parser.add_argument("--output-dir")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    covered_path = Path(args.covered_manifest)
    uncovered_path = Path(args.uncovered_manifest)
    full_path = Path(args.full_manifest) if args.full_manifest else None
    covered = compact_split("public_audit_covered_20260502_20260513", load_json(covered_path), covered_path)
    uncovered = compact_split("public_audit_uncovered_20260516_20260517", load_json(uncovered_path), uncovered_path)
    full = (
        compact_split("full_allowed_labels", load_json(full_path), full_path)
        if full_path is not None
        else None
    )
    status, scoreboard_delta, next_action = compare_splits(covered, uncovered, full)
    output_dir = Path(args.output_dir or f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}")
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "strategy": "xuan_b27_dplus",
        "status": status,
        "hypothesis": (
            "the inventory repair lead should persist on public-audit-covered 2026-05-02..13 "
            "rather than depending on uncovered 2026-05-16/17"
        ),
        "data_root": covered["data_root"],
        "dataset_type": "metadata_compare_of_local_poly_backtest_strict_completion_inventory_repair_splits",
        "splits": {
            "public_audit_covered": covered,
            "public_audit_uncovered": uncovered,
            "full_allowed_labels": full,
        },
        "labels": {
            "strict_covered": (covered.get("labels") or {}).get("strict"),
            "completion_covered": (covered.get("labels") or {}).get("completion"),
            "strict_uncovered": (uncovered.get("labels") or {}).get("strict"),
            "completion_uncovered": (uncovered.get("labels") or {}).get("completion"),
        },
        "days": {
            "covered": covered.get("days"),
            "uncovered": uncovered.get("days"),
        },
        "market_prefix": covered.get("market_prefix"),
        "assets": covered.get("assets"),
        "row_count": {
            "covered": covered.get("row_count"),
            "uncovered": uncovered.get("row_count"),
            "full": (full or {}).get("row_count"),
        },
        "excluded_20260514_20260515": bool(
            covered.get("excluded_20260514_20260515")
            and uncovered.get("excluded_20260514_20260515")
        ),
        "contains_20260518": bool(covered.get("contains_20260518") or uncovered.get("contains_20260518")),
        "includes_public_account_execution_truth_v1": bool(
            covered.get("includes_public_account_execution_truth_v1")
        ),
        "public_account_truth_level": covered.get("public_account_truth_level"),
        "public_account_truth_missing_days": {
            "covered": covered.get("public_account_truth_missing_days"),
            "uncovered": uncovered.get("public_account_truth_missing_days"),
        },
        "scoreboard_delta": scoreboard_delta,
        "can_support_strategy_promotion": False,
        "requires_source_of_truth_replay_for_promotion": True,
        "conclusion_scope": (
            "manifest-only split comparison over local strict/cache plus completion-store inventory repair metrics; "
            "not private owner truth, not source-of-truth replay, not deployable as-is"
        ),
        "next_action": next_action,
        "raw_replay_scanned": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "auth_network_started": False,
        "started_canary": False,
        "side_effects": {
            "raw_replay_scanned": False,
            "raw_replay_written": False,
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "auth_network_started": False,
            "started_canary": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
        },
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(manifest_path)
    print(status)
    print(next_action)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
