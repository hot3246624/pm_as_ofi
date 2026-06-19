#!/usr/bin/env python3
"""Review current L2/V1 bridge outputs against the xuan bridge spec.

The report is deliberately local-only.  It reads the latest local backtest V1
and L2 bridge artifacts, compares them with the xuan bridge validation spec,
and records whether the rebuilt data is sufficient for xuan strategy
evaluation.  It does not import candidates, build a runner profile, or
authorize remote execution.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path
from typing import Any


STAMP = "20260527T0727Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"

DEFAULT_SPEC = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_l2_v1_age_fee_bridge_validation_spec_20260527T0657Z.json"
)
DEFAULT_BRIDGE_MANIFEST = (
    CONTRACT_ROOT / "xuan_bridge_scorecard_latest/XUAN_BRIDGE_SCORECARD_MANIFEST.json"
)
DEFAULT_BRIDGE_SCORECARD = CONTRACT_ROOT / "xuan_bridge_scorecard_latest/xuan_bridge_scorecard.csv"
DEFAULT_BRIDGE_MISSING_FIELDS = CONTRACT_ROOT / "xuan_bridge_scorecard_latest/xuan_bridge_missing_fields.csv"
DEFAULT_L1_L2_PARITY = CONTRACT_ROOT / "l1_from_l2_parity_latest/L1_FROM_L2_PARITY_REPORT.json"
DEFAULT_L1_EVENT_DB = (
    POLY_BT_ROOT
    / "derived/multiasset_l1_flow_event_store_v1/20260502_20260518_minsz10/event_store.duckdb"
)
DEFAULT_L2_DUCKDB = (
    POLY_BT_ROOT / "verification_store/replay_store_multiasset_l2_v1/smoke_20260517_l2/store.duckdb"
)
DEFAULT_AUDIT_DUCKDB = (
    CONTRACT_ROOT / "backtest_candidate_audit_pack_latest/backtest_candidate_audit_pack.duckdb"
)

DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_l2_v1_bridge_field_gap_report_{STAMP}.json"
)
DEFAULT_MARKDOWN = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_l2_v1_bridge_field_gap_report_{STAMP}/"
    "L2_V1_BRIDGE_FIELD_GAP_REPORT.md"
)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def fnum(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def inspect_duckdb(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "tables": {}}
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency path
        return {
            "path": str(path),
            "exists": True,
            "duckdb_import_ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "tables": {},
        }
    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute(
            """
            select table_name, table_type
            from information_schema.tables
            where table_schema = 'main'
            order by table_name
            """
        ).fetchall()
        tables: dict[str, Any] = {}
        for table_name, table_type in rows:
            columns = [
                row[0]
                for row in con.execute(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema = 'main' and table_name = ?
                    order by ordinal_position
                    """,
                    [table_name],
                ).fetchall()
            ]
            tables[str(table_name)] = {
                "table_type": table_type,
                "column_count": len(columns),
                "columns": columns,
            }
    finally:
        con.close()
    return {
        "path": str(path),
        "exists": True,
        "duckdb_import_ok": True,
        "tables": tables,
    }


def semantic_schema_coverage(db_cards: dict[str, dict[str, Any]]) -> dict[str, Any]:
    all_columns: set[str] = set()
    by_db: dict[str, list[str]] = {}
    for key, card in db_cards.items():
        columns: set[str] = set()
        for table in body(card, "tables").values():
            if isinstance(table, dict):
                columns.update(str(col) for col in table.get("columns", []))
        by_db[key] = sorted(columns)
        all_columns.update(columns)

    def has_any(*names: str) -> bool:
        return any(name in all_columns for name in names)

    semantic_groups = {
        "public_trade_and_l1_context": has_any("public_trade_price", "price")
        and has_any("l1_age_ms", "recv_ms")
        and has_any("yes_bid_px", "bid1_px"),
        "l2_top_depth_available": has_any("bid1_px") and has_any("ask1_px") and has_any("depth"),
        "settlement_available": has_any("winner_side", "official_outcome", "settle_ms"),
        "v1_queue_audit_available": has_any("best_queue_pnl", "queue_best_pnl"),
        "xuan_owner_activity_available": has_any("xuan_activity", "proxy_wallet", "activity_type"),
        "pair_completion_state_machine_outputs": has_any("pair_completion_pnl", "pair_pnl", "pair_completion_qty"),
        "strict_rescue_outputs": has_any("strict_rescue_pnl", "strict_rescue_closes"),
        "residual_fifo_lots": has_any("residual_lot_id", "residual_qty", "residual_cost"),
        "mature_mark_recovery_outputs": has_any(
            "mature_after_fee_recovery_rate",
            "mature_after_fee_mark_value",
            "marked_pair_pnl_after_residual",
        ),
        "merge_capital_reuse_outputs": has_any("merge_reuse_turnover_count", "capital_turnover"),
        "v2_feature_direct_outputs": has_any("risk_increasing_seed")
        and has_any("surplus_budget_projected_unpaired_cost")
        and has_any("same_exposure_qty")
        and has_any("opp_exposure_qty")
        and has_any("source_quality_decision"),
    }
    missing_groups = [key for key, ok in semantic_groups.items() if not ok]
    return {
        "semantic_groups": semantic_groups,
        "missing_semantic_groups": missing_groups,
        "db_column_counts": {key: len(cols) for key, cols in by_db.items()},
    }


def bridge_scorecard_summary(rows: list[dict[str, str]]) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    assets: set[str] = set()
    sources: set[str] = set()
    positive_pair_rows = 0
    positive_queue_rows = 0
    v1_rows = 0
    old_rows = 0
    for row in rows:
        statuses[row.get("status", "")] = statuses.get(row.get("status", ""), 0) + 1
        if row.get("asset"):
            assets.add(row["asset"])
        if row.get("source"):
            sources.add(row["source"])
        if row.get("source") == "multiasset_v1_audit_pack_search_safe_screener":
            v1_rows += 1
        if row.get("source") == "old_btc_completion_residual_baseline":
            old_rows += 1
        if (fnum(row.get("pair_pnl"), 0.0) or 0.0) > 0:
            positive_pair_rows += 1
        if (fnum(row.get("queue_pnl"), 0.0) or 0.0) > 0:
            positive_queue_rows += 1
    return {
        "row_count": len(rows),
        "assets": sorted(assets),
        "sources": sorted(sources),
        "statuses": statuses,
        "v1_candidate_rows": v1_rows,
        "old_baseline_rows": old_rows,
        "positive_pair_pnl_rows": positive_pair_rows,
        "positive_queue_pnl_rows": positive_queue_rows,
    }


def missing_fields_summary(rows: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "row_count": len(rows),
        "fields": [
            {
                "field": row.get("field"),
                "old_baseline": row.get("old_baseline"),
                "multiasset_v1": row.get("multiasset_v1"),
                "unlock": row.get("unlock"),
            }
            for row in rows
        ],
    }


def parity_summary(report: dict[str, Any]) -> dict[str, Any]:
    by_asset = report.get("by_asset") if isinstance(report.get("by_asset"), list) else []
    max_pure_mismatch = max((fnum(row.get("bid_ask_mismatch_rate"), 0.0) or 0.0 for row in by_asset), default=0.0)
    min_overlay_match = min((fnum(row.get("overlay_match_rate"), 0.0) or 0.0 for row in by_asset), default=0.0)
    max_overlay_mismatch = max(
        (fnum(row.get("overlay_bid_ask_mismatch_rate"), 0.0) or 0.0 for row in by_asset), default=0.0
    )
    max_stale = max((fnum(row.get("stale_rate"), 0.0) or 0.0 for row in by_asset), default=0.0)
    return rounded(
        {
            "status": report.get("status"),
            "sample_per_asset": report.get("sample_per_asset"),
            "failed_assets": report.get("failed_assets") or [],
            "overlay_failed_assets": report.get("overlay_failed_assets") or [],
            "max_pure_l2_bid_ask_mismatch_rate": max_pure_mismatch,
            "min_overlay_match_rate": min_overlay_match,
            "max_overlay_bid_ask_mismatch_rate": max_overlay_mismatch,
            "max_stale_rate": max_stale,
            "parity_model_read": (
                "pure_l2_top_of_book_does_not_pass_current_thresholds; l1_top_overlay_model_has_no_overlay_mismatches"
                if report.get("status") == "OK_L1_TOP_OVERLAY_REQUIRED"
                else "inspect_report_status"
            ),
        }
    )


def deliverable_matrix(
    spec: dict[str, Any],
    bridge_manifest: dict[str, Any],
    parity: dict[str, Any],
    bridge_rows: list[dict[str, str]],
    missing_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    bridge_status = bridge_manifest.get("status")
    scorecard = bridge_scorecard_summary(bridge_rows)
    missing = {row.get("field") for row in missing_rows}
    parity_status = parity.get("status")
    requested = spec.get("requested_deliverables_from_l2_v1_rebuild") or []
    rows = []
    for item in requested:
        if item == "L1_FROM_L2_PARITY_REPORT.json":
            status = "PARTIAL_OVERLAY_REQUIRED" if parity_status == "OK_L1_TOP_OVERLAY_REQUIRED" else str(parity_status or "MISSING")
            blockers = [] if status == "OK" else ["pure_l2_parity_failed_or_overlay_model_requires_explicit_contract"]
        elif item == "XUAN_OLD_ANCHOR_REPLAY_EXPLANATION.json":
            status = "PARTIAL_OLD_BASELINE_ONLY" if scorecard["old_baseline_rows"] else "MISSING"
            blockers = ["four_old_anchor_windows_not_replayed_or_explained"]
        elif item == "XUAN_AGE_FEE_V2_HOLDOUT_SCORECARD.json":
            status = "MISSING"
            blockers = ["refined_v2_age_fee_feature_not_scored_on_l2_v1_holdout"]
        elif item == "RESIDUAL_DYNAMIC_MARK_RECOVERY_REPORT.json":
            status = "PARTIAL_OLD_BASELINE_ONLY" if "residual_dynamic_recovery" in missing else "UNKNOWN"
            blockers = ["multiasset_candidate_residual_fifo_and_mature_mark_recovery_missing"]
        elif item == "PRIVATE_TRUTH_BOUNDARY_REPORT.json":
            status = "PARTIAL_POLICY_TEXT_ONLY"
            blockers = ["formal_private_truth_boundary_report_missing"]
        elif item.startswith("schema manifest"):
            refs = json.dumps(bridge_manifest, sort_keys=True)
            status = "PASS_LOCAL_ROOT" if "/Volumes/PolyData" not in refs and str(POLY_BT_ROOT) in refs else "CHECK_ROOT_REFERENCES"
            blockers = [] if status == "PASS_LOCAL_ROOT" else ["schema_or_manifest_path_root_not_clean"]
        else:
            status = "UNKNOWN"
            blockers = ["unrecognized_deliverable"]
        rows.append({"deliverable": item, "status": status, "blockers": blockers})
    if bridge_status == "BLOCKED_XUAN_BRIDGE_NOT_COMPLETE":
        rows.append(
            {
                "deliverable": "XUAN_BRIDGE_SCORECARD_MANIFEST.json",
                "status": bridge_status,
                "blockers": ["bridge_scorecard_self_reports_not_complete"],
            }
        )
    return rows


def build(args: argparse.Namespace) -> dict[str, Any]:
    spec = load_json(args.spec)
    bridge_manifest = load_json(args.bridge_manifest)
    parity = load_json(args.l1_l2_parity)
    bridge_rows = read_csv(args.bridge_scorecard)
    missing_rows = read_csv(args.bridge_missing_fields)
    db_cards = {
        "l1_event_store": inspect_duckdb(args.l1_event_db),
        "l2_replay_store": inspect_duckdb(args.l2_duckdb),
        "candidate_audit_pack": inspect_duckdb(args.audit_duckdb),
    }
    schema_coverage = semantic_schema_coverage(db_cards)
    deliverables = deliverable_matrix(spec, bridge_manifest, parity, bridge_rows, missing_rows)
    bridge_summary = bridge_scorecard_summary(bridge_rows)
    parity_read = parity_summary(parity)
    missing_summary = missing_fields_summary(missing_rows)

    deliverables_pass = all(row["status"] in {"PASS_LOCAL_ROOT", "OK"} for row in deliverables)
    bridge_complete = bridge_manifest.get("status") != "BLOCKED_XUAN_BRIDGE_NOT_COMPLETE"
    semantic_required_missing = [
        key
        for key in [
            "pair_completion_state_machine_outputs",
            "strict_rescue_outputs",
            "residual_fifo_lots",
            "mature_mark_recovery_outputs",
            "merge_capital_reuse_outputs",
            "v2_feature_direct_outputs",
        ]
        if key in schema_coverage["missing_semantic_groups"]
    ]

    hard_blockers = [
        "owner_private_truth_not_ready",
        "independent_l2_v1_holdout_not_yet_run",
        "runner_support_not_implemented",
    ]
    if not bridge_complete:
        hard_blockers.append("xuan_bridge_scorecard_self_reports_blocked")
    if parity_read["status"] == "OK_L1_TOP_OVERLAY_REQUIRED":
        hard_blockers.append("pure_l2_parity_failed_l1_top_overlay_contract_required")
    if semantic_required_missing:
        hard_blockers.append("xuan_required_semantic_fields_missing")
    if bridge_summary["positive_queue_pnl_rows"] == 0:
        hard_blockers.append("v1_candidate_audit_has_no_positive_queue_pnl_for_direct_import")

    status = (
        "KEEP_SHADOW_REVIEW_L2_V1_BRIDGE_FIELD_GAP_REPORT_READY_LOCAL_ONLY"
        if not deliverables_pass or hard_blockers
        else "KEEP_SHADOW_REVIEW_L2_V1_BRIDGE_FIELD_GAP_CLEAR_LOCAL_ONLY"
    )

    return rounded(
        {
            "artifact": "xuan_shadow_review_l2_v1_bridge_field_gap_report",
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "scripts/xuan_shadow_review_l2_v1_bridge_field_gap_report.py",
            "status": status,
            "inputs": {
                "spec": str(args.spec),
                "bridge_manifest": str(args.bridge_manifest),
                "bridge_scorecard": str(args.bridge_scorecard),
                "bridge_missing_fields": str(args.bridge_missing_fields),
                "l1_l2_parity": str(args.l1_l2_parity),
                "l1_event_db": str(args.l1_event_db),
                "l2_duckdb": str(args.l2_duckdb),
                "audit_duckdb": str(args.audit_duckdb),
            },
            "source_statuses": {
                "bridge_manifest": bridge_manifest.get("status"),
                "l1_l2_parity": parity.get("status"),
                "bridge_spec": spec.get("status"),
            },
            "bridge_scorecard_summary": bridge_summary,
            "bridge_missing_fields_summary": missing_summary,
            "l1_l2_parity_summary": parity_read,
            "deliverable_matrix": deliverables,
            "duckdb_schema_coverage": schema_coverage,
            "db_introspection": {
                key: {
                    "path": card.get("path"),
                    "exists": card.get("exists"),
                    "duckdb_import_ok": card.get("duckdb_import_ok"),
                    "table_count": len(body(card, "tables")),
                    "tables": {
                        name: {
                            "table_type": table.get("table_type"),
                            "column_count": table.get("column_count"),
                            "columns_sample": table.get("columns", [])[:40],
                        }
                        for name, table in body(card, "tables").items()
                    },
                }
                for key, card in db_cards.items()
            },
            "decision": {
                "l2_v1_bridge_field_gap_report_ready": True,
                "current_l2_v1_bridge_sufficient_for_xuan": False,
                "l1_from_l2_pure_parity_pass": parity_read["status"] == "OK",
                "l1_top_overlay_contract_required": parity_read["status"] == "OK_L1_TOP_OVERLAY_REQUIRED",
                "xuan_bridge_scorecard_complete": bridge_complete,
                "requested_deliverables_all_clear": deliverables_pass,
                "semantic_xuan_fields_all_present": not semantic_required_missing,
                "v2_age_fee_holdout_ready": False,
                "direct_v1_candidate_import_ready": False,
                "candidate_profile_ready": False,
                "bounded_cap25_remote_rationale_ready": False,
                "future_remote_allowed_by_this_report": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "promotion_ready": False,
                "research_only": True,
                "paper_shadow_only": True,
                "hard_blockers": hard_blockers,
                "next_action": (
                    "ask the L2/V1 rebuild to clear the bridge scorecard gaps: formalize the L1-top-overlay contract or "
                    "fix pure L2 parity, add xuan pair-completion/rescue/residual FIFO/mature mark/merge turnover outputs, "
                    "and produce the v2 age-fee holdout scorecard before any runner-support proposal"
                ),
            },
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = card["decision"]
    parity = card["l1_l2_parity_summary"]
    bridge = card["bridge_scorecard_summary"]
    missing = card["bridge_missing_fields_summary"]["fields"]
    lines = [
        "# Xuan L2/V1 Bridge Field Gap Report",
        "",
        f"Status: `{card['status']}`",
        "",
        "## Decision",
        "",
        f"- current_l2_v1_bridge_sufficient_for_xuan: `{decision['current_l2_v1_bridge_sufficient_for_xuan']}`",
        f"- l1_top_overlay_contract_required: `{decision['l1_top_overlay_contract_required']}`",
        f"- xuan_bridge_scorecard_complete: `{decision['xuan_bridge_scorecard_complete']}`",
        f"- semantic_xuan_fields_all_present: `{decision['semantic_xuan_fields_all_present']}`",
        f"- v2_age_fee_holdout_ready: `{decision['v2_age_fee_holdout_ready']}`",
        f"- future_remote_allowed_by_this_report: `{decision['future_remote_allowed_by_this_report']}`",
        f"- hard_blockers: `{', '.join(decision['hard_blockers'])}`",
        "",
        "## Current Bridge Read",
        "",
        f"- bridge rows: `{bridge['row_count']}`",
        f"- bridge statuses: `{json.dumps(bridge['statuses'], sort_keys=True)}`",
        f"- bridge assets: `{', '.join(bridge['assets'])}`",
        f"- positive queue pnl rows: `{bridge['positive_queue_pnl_rows']}`",
        f"- positive pair pnl rows: `{bridge['positive_pair_pnl_rows']}`",
        "",
        "## L1/L2 Parity Read",
        "",
        f"- parity status: `{parity['status']}`",
        f"- failed pure-L2 assets: `{', '.join(parity['failed_assets'])}`",
        f"- overlay failed assets: `{', '.join(parity['overlay_failed_assets'])}`",
        f"- max pure-L2 mismatch rate: `{parity['max_pure_l2_bid_ask_mismatch_rate']}`",
        f"- min overlay match rate: `{parity['min_overlay_match_rate']}`",
        f"- read: `{parity['parity_model_read']}`",
        "",
        "## Missing Bridge Fields",
        "",
    ]
    for row in missing:
        lines.append(f"- `{row['field']}`: old=`{row['old_baseline']}`, multiasset=`{row['multiasset_v1']}`; unlock={row['unlock']}")
    lines.extend(["", "## Deliverable Matrix", ""])
    for row in card["deliverable_matrix"]:
        lines.append(f"- `{row['deliverable']}`: `{row['status']}`")
        if row["blockers"]:
            lines.append(f"  blockers: `{', '.join(row['blockers'])}`")
    lines.extend(["", "## Missing Semantic Groups", ""])
    for group in card["duckdb_schema_coverage"]["missing_semantic_groups"]:
        lines.append(f"- `{group}`")
    lines.extend(["", "## Next Action", "", decision["next_action"], ""])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--bridge-manifest", type=Path, default=DEFAULT_BRIDGE_MANIFEST)
    parser.add_argument("--bridge-scorecard", type=Path, default=DEFAULT_BRIDGE_SCORECARD)
    parser.add_argument("--bridge-missing-fields", type=Path, default=DEFAULT_BRIDGE_MISSING_FIELDS)
    parser.add_argument("--l1-l2-parity", type=Path, default=DEFAULT_L1_L2_PARITY)
    parser.add_argument("--l1-event-db", type=Path, default=DEFAULT_L1_EVENT_DB)
    parser.add_argument("--l2-duckdb", type=Path, default=DEFAULT_L2_DUCKDB)
    parser.add_argument("--audit-duckdb", type=Path, default=DEFAULT_AUDIT_DUCKDB)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    card = build(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps({"status": card["status"], "scorecard": str(args.scorecard), "markdown": str(args.markdown)}, indent=2))


if __name__ == "__main__":
    main()
