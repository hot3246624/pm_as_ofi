#!/usr/bin/env python3
"""Xuan-side integration gate for the multi-asset backtest V1 outputs.

This gate treats the backtest suite as an upstream search-safe infrastructure
signal only. It verifies that the required manifests and audit pack are present,
that forbidden search columns remain absent, and that no candidate is promoted
past the owner-private-truth boundary.

It is local/read-only and does not authorize remote runs or live deployment.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any


DEFAULT_SUITE_MANIFEST = Path(
    "/Volumes/PolyData/poly_backtest_data/derived/contract_examples/"
    "backtest_experiment_suite_deep_v1/BACKTEST_EXPERIMENT_SUITE_MANIFEST.json"
)
DEFAULT_READINESS_REPORT = Path(
    "/Volumes/PolyData/poly_backtest_data/derived/contract_examples/"
    "backtest_readiness_deep_with_experiment_v1/BACKTEST_READINESS_REPORT.json"
)
DEFAULT_AUDIT_MANIFEST = Path(
    "/Volumes/PolyData/poly_backtest_data/derived/contract_examples/"
    "backtest_candidate_audit_pack_latest/BACKTEST_CANDIDATE_AUDIT_PACK_MANIFEST.json"
)
DEFAULT_AUDIT_DUCKDB = Path(
    "/Volumes/PolyData/poly_backtest_data/derived/contract_examples/"
    "backtest_candidate_audit_pack_latest/backtest_candidate_audit_pack.duckdb"
)
DEFAULT_SCORECARD = Path(".tmp_xuan/scorecards/xuan_backtest_v1_integration_gate_20260527T0144Z.json")


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    path = path.expanduser()
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(item) for item in value]
    return value


def recursive_find(data: Any, key: str) -> Any:
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for value in data.values():
            found = recursive_find(value, key)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = recursive_find(item, key)
            if found is not None:
                return found
    return None


def forbidden_columns_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return all(forbidden_columns_empty(item) for item in value.values())
    if isinstance(value, list):
        return len(value) == 0
    return False


def query_audit_pack(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"duckdb_present": False, "duckdb_query_ok": False, "rows": []}
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover - environment dependent
        return {
            "duckdb_present": True,
            "duckdb_query_ok": False,
            "duckdb_error": f"duckdb_import_failed:{exc!r}",
            "rows": [],
        }
    try:
        con = duckdb.connect(str(path), read_only=True)
        rows = con.execute(
            """
            select
              audit_rank,
              asset,
              candidate_key,
              cast(best_queue_pnl as double) as best_queue_pnl,
              audit_status,
              promotion_blockers
            from search_safe_private_blocked
            order by audit_rank
            """
        ).fetchall()
        by_asset = con.execute(
            """
            select asset, count(*) as n, min(cast(best_queue_pnl as double)), max(cast(best_queue_pnl as double))
            from search_safe_private_blocked
            group by asset
            order by asset
            """
        ).fetchall()
        con.close()
    except Exception as exc:  # pragma: no cover - environment dependent
        return {
            "duckdb_present": True,
            "duckdb_query_ok": False,
            "duckdb_error": repr(exc),
            "rows": [],
        }
    return {
        "duckdb_present": True,
        "duckdb_query_ok": True,
        "row_count": len(rows),
        "rows": [
            {
                "audit_rank": int(row[0]),
                "asset": row[1],
                "candidate_key": row[2],
                "best_queue_pnl": fnum(row[3]),
                "audit_status": row[4],
                "promotion_blockers": row[5],
            }
            for row in rows
        ],
        "by_asset": [
            {"asset": row[0], "count": int(row[1]), "min_best_queue_pnl": fnum(row[2]), "max_best_queue_pnl": fnum(row[3])}
            for row in by_asset
        ],
    }


def render_markdown(card: dict[str, Any]) -> str:
    d = card["decision"]
    s = card["summary"]
    lines = [
        "# Xuan Backtest V1 Integration Gate",
        "",
        "## Status",
        "",
        f"- status: `{card['status']}`",
        f"- backtest_v1_inputs_present: `{d['backtest_v1_inputs_present']}`",
        f"- upstream_search_safe_infra_ready: `{d['upstream_search_safe_infra_ready']}`",
        f"- direct_candidate_import_ready: `{d['direct_candidate_import_ready']}`",
        f"- future_remote_allowed_by_this_gate: `{d['future_remote_allowed_by_this_gate']}`",
        "",
        "## Summary",
        "",
        f"- suite ok: `{s['suite_ok']}`",
        f"- readiness ok: `{s['readiness_ok']}`",
        f"- candidate audit ok: `{s['candidate_audit_ok']}`",
        f"- selected candidates: `{s['selected_candidate_count']}`",
        f"- private promotion ready: `{s['private_promotion_ready_count']}`",
        f"- audit assets: `{s['audit_assets']}`",
        "",
        "## Boundary",
        "",
        "- Local/read-only.",
        "- Search-safe evidence is not owner private truth.",
        "- Does not authorize remote runs, cap75, deploy, restart, or live orders.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> dict[str, Any]:
    suite_path = Path(args.suite_manifest).expanduser()
    readiness_path = Path(args.readiness_report).expanduser()
    audit_manifest_path = Path(args.audit_manifest).expanduser()
    audit_duckdb_path = Path(args.audit_duckdb).expanduser()
    suite = load_json_if_exists(suite_path)
    readiness = load_json_if_exists(readiness_path)
    audit = load_json_if_exists(audit_manifest_path)
    audit_query = query_audit_pack(audit_duckdb_path)

    missing_inputs = [
        str(path)
        for path, data in [
            (suite_path, suite),
            (readiness_path, readiness),
            (audit_manifest_path, audit),
        ]
        if data is None
    ]
    if not audit_duckdb_path.exists():
        missing_inputs.append(str(audit_duckdb_path))

    suite_ok = bool(recursive_find(suite, "ok")) if suite else False
    readiness_ok = bool(recursive_find(readiness, "ok")) if readiness else False
    audit_ok = bool(recursive_find(audit, "ok")) if audit else False
    forbidden_empty = forbidden_columns_empty(recursive_find(audit, "forbidden_result_columns")) if audit else False
    selected_count = int(fnum(recursive_find(audit, "selected_candidate_count")))
    evidence_count = int(fnum(recursive_find(audit, "evidence_row_count")))
    blocked_count = int(fnum(recursive_find(audit, "search_safe_private_blocked_count")))
    private_ready_count = int(fnum(recursive_find(audit, "private_promotion_ready_count")))
    rows = audit_query.get("rows", [])
    audit_assets = sorted({row.get("asset") for row in rows if row.get("asset")})
    positive_queue_rows = [row for row in rows if fnum(row.get("best_queue_pnl")) > 0.0]
    all_rows_private_blocked = bool(rows) and all(
        row.get("audit_status") == "SEARCH_SAFE_READY_PRIVATE_BLOCKED" for row in rows
    )
    all_rows_owner_truth_blocked = bool(rows) and all(
        row.get("promotion_blockers") == "owner_private_truth_missing_for_deployable_promotion" for row in rows
    )

    inputs_present = not missing_inputs and bool(audit_query.get("duckdb_query_ok"))
    upstream_ready = (
        inputs_present
        and suite_ok
        and readiness_ok
        and audit_ok
        and forbidden_empty
        and private_ready_count == 0
        and blocked_count == selected_count
        and all_rows_private_blocked
        and all_rows_owner_truth_blocked
    )
    direct_candidate_import_ready = upstream_ready and bool(positive_queue_rows)

    hard_blockers = []
    if missing_inputs:
        hard_blockers.append("backtest_v1_inputs_missing")
    if audit_query.get("duckdb_query_ok") is not True:
        hard_blockers.append("backtest_candidate_audit_pack_duckdb_unavailable")
    if not suite_ok:
        hard_blockers.append("suite_not_ok_or_unavailable")
    if not readiness_ok:
        hard_blockers.append("readiness_not_ok_or_unavailable")
    if not audit_ok:
        hard_blockers.append("candidate_audit_pack_not_ok_or_unavailable")
    if not forbidden_empty:
        hard_blockers.append("forbidden_result_columns_not_confirmed_empty")
    if private_ready_count != 0:
        hard_blockers.append("private_promotion_ready_count_nonzero")
    if not positive_queue_rows:
        hard_blockers.append("no_positive_queue_pnl_search_safe_candidate_for_direct_import")
    hard_blockers.append("owner_private_truth_not_ready")

    status = (
        "KEEP_XUAN_BACKTEST_V1_INTEGRATION_GATE_READY_LOCAL_ONLY"
        if inputs_present
        else "BLOCKED_XUAN_BACKTEST_V1_INTEGRATION_GATE_INPUTS_MISSING_LOCAL_ONLY"
    )
    card = {
        "artifact": "xuan_backtest_v1_integration_gate",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "script": "scripts/xuan_shadow_review_backtest_v1_integration_gate.py",
        "status": status,
        "inputs": {
            "suite_manifest": str(suite_path),
            "readiness_report": str(readiness_path),
            "audit_manifest": str(audit_manifest_path),
            "audit_duckdb": str(audit_duckdb_path),
            "missing_inputs": missing_inputs,
        },
        "fingerprints": {
            "suite_fingerprint": recursive_find(suite, "suite_fingerprint") if suite else None,
            "readiness_fingerprint": recursive_find(readiness, "readiness_fingerprint") if readiness else None,
            "candidate_audit_pack_fingerprint": recursive_find(audit, "candidate_audit_pack_fingerprint")
            if audit
            else None,
        },
        "summary": {
            "suite_ok": suite_ok,
            "readiness_ok": readiness_ok,
            "candidate_audit_ok": audit_ok,
            "forbidden_result_columns_empty": forbidden_empty,
            "selected_candidate_count": selected_count,
            "evidence_row_count": evidence_count,
            "search_safe_private_blocked_count": blocked_count,
            "private_promotion_ready_count": private_ready_count,
            "audit_duckdb_query_ok": audit_query.get("duckdb_query_ok"),
            "audit_assets": audit_assets,
            "positive_queue_candidate_count": len(positive_queue_rows),
        },
        "candidate_audit_rows": rows,
        "candidate_audit_by_asset": audit_query.get("by_asset", []),
        "interpretation": {
            "core_read": (
                "backtest V1 is usable only as an upstream search-safe screening layer when all inputs are present "
                "and readiness gates pass"
            ),
            "private_truth_boundary": "search-safe/public/proxy evidence does not satisfy owner private truth",
            "xuan_integration_read": (
                "do not import candidates directly into xuan promotion unless a local scorer also proves positive "
                "queue evidence, residual/density/source gates, and private-truth boundary handling"
            ),
        },
        "decision": {
            "backtest_v1_inputs_present": inputs_present,
            "upstream_search_safe_infra_ready": upstream_ready,
            "direct_candidate_import_ready": direct_candidate_import_ready,
            "hard_blockers": hard_blockers,
            "bounded_cap25_remote_rationale_ready": False,
            "future_remote_allowed_by_this_gate": False,
            "same_profile_repeat_allowed": False,
            "cap75_remote_rationale_ready": False,
            "capacity_expansion_allowed": False,
            "private_truth_ready": False,
            "promotion_ready": False,
            "shadow_review_ready": False,
            "remote_runner_allowed": False,
            "deployable": False,
            "live_orders_allowed": False,
            "research_only": True,
            "paper_shadow_only": True,
            "next_action": (
                "remount_or_restore_backtest_v1_artifacts_then_use_as_search_safe_filter"
                if not inputs_present
                else "xuan_specific_backtest_v1_candidate_filter_before_any_remote"
            ),
        },
    }
    return rounded(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite-manifest", type=Path, default=DEFAULT_SUITE_MANIFEST)
    parser.add_argument("--readiness-report", type=Path, default=DEFAULT_READINESS_REPORT)
    parser.add_argument("--audit-manifest", type=Path, default=DEFAULT_AUDIT_MANIFEST)
    parser.add_argument("--audit-duckdb", type=Path, default=DEFAULT_AUDIT_DUCKDB)
    parser.add_argument("--scorecard-json", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--markdown", type=Path, default=None)
    args = parser.parse_args()
    card = build(args)
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        markdown_path = Path(args.markdown).expanduser().resolve()
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(render_markdown(card), encoding="utf-8")
    print(json.dumps(card, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
