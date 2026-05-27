#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_backtest_v1_candidate_filter_handoff_plan_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-${TMPDIR:-/tmp}/xuan_backtest_v1_candidate_filter_pycache_$ts}"
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT

python3 -m py_compile scripts/xuan_backtest_v1_candidate_filter_handoff_plan.py >> "$log" 2>&1

python3 - "$out_dir" "$ts" <<'PY' >> "$log" 2>&1
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
created_utc = sys.argv[2]
fixture_root = out_dir / "fixtures"
script = Path("scripts/xuan_backtest_v1_candidate_filter_handoff_plan.py")


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def validation_manifest(path, *, private_truth=False, requires_raw=False, requires_remote=False):
    write_json(
        path,
        {
            "schema_version": "backtest_validation_manifest_v1",
            "ok": True,
            "promotion": {
                "deployable_ready": False,
                "private_truth_ready": private_truth,
                "search_safe_validation_ready": True,
            },
            "validation_scope": {
                "allowed_layers": ["replay_source_truth", "public_proxy_evidence", "search_safe_lineage"],
                "private_truth_ready_on_completion": False,
                "requires_owner_private_truth": False,
                "requires_raw": requires_raw,
                "requires_remote": requires_remote,
            },
        },
    )


def candidate_row(key, asset, rank, *, status="SEARCH_SAFE_READY_PRIVATE_BLOCKED", private_count=0, scope_unsafe=0, deployable=False):
    return {
        "candidate_key": key,
        "asset": asset,
        "experiment_count": "2",
        "experiment_labels": "deep_v1;formal_latest",
        "row_count": "2",
        "ok_count": "2",
        "search_safe_ready_count": "2",
        "private_truth_ready_count": str(private_count),
        "scope_unsafe_count": str(scope_unsafe),
        "promotion_blocker_count": "4",
        "best_queue_pnl": "-0.05",
        "avg_queue_pnl": "-0.05",
        "min_queue_pnl": "-0.05",
        "queue_pnl_range": "0.0",
        "max_seen_batch_count": "2",
        "max_seen_matrix_count": "3",
        "search_safe_gate_pass": "True",
        "historical_private_boundary_ok": "True",
        "private_promotion_gate_pass": "False",
        "promotion_blockers": "owner_private_truth_missing_for_deployable_promotion",
        "audit_rank": str(rank),
        "audit_status": status,
        "deployable_ready": "True" if deployable else "False",
    }


def evidence_row(case_dir, key, asset, label, idx, *, private_truth=False, requires_raw=False, requires_remote=False):
    manifest = case_dir / f"{key}_{label}_{idx}_VALIDATION_MANIFEST.json"
    validation_manifest(manifest, private_truth=private_truth, requires_raw=requires_raw, requires_remote=requires_remote)
    return {
        "job_id": f"validate_{idx:04d}_{asset.lower()}_{key[:12]}",
        "candidate_key": key,
        "asset": asset,
        "ok": "True",
        "validation_fingerprint": f"validation-{key[:8]}-{label}",
        "search_safe_validation_ready": "True",
        "deployable_ready": "False",
        "private_truth_ready": "True" if private_truth else "False",
        "source_truth_ready": "True",
        "public_or_proxy_truth_only": "False" if private_truth else "True",
        "requires_raw": "True" if requires_raw else "False",
        "requires_remote": "True" if requires_remote else "False",
        "requires_owner_private_truth": "False",
        "private_truth_ready_on_completion": "False",
        "catalog_row_count": "3",
        "catalog_rows_sum": "3",
        "catalog_best_pnl": "-0.05",
        "queue_best_pnl": "-0.05",
        "queue_avg_pnl": "-0.05",
        "seen_batch_count": "2",
        "seen_matrix_count": "3",
        "pnl_range": "0.0",
        "pnl_stddev": "0.0",
        "promotion_blocker_count": "2",
        "error_count": "0",
        "validation_manifest": str(manifest),
        "validation_manifest_semantic_hash": f"hash-{key[:8]}-{label}",
        "experiment_label": label,
        "experiment_candidate_index": str(idx),
        "post_validation_pipeline_fingerprint": "pipeline",
        "validation_result_catalog_fingerprint": "catalog",
        "readiness_fingerprint": "readiness",
        "validation_manifest_sha256": "",
        "audit_status": "EVIDENCE_ONLY",
    }


def make_case(name, *, private_ready=False, forbidden_columns=False, no_candidates=False, raw_evidence=False):
    case_dir = fixture_root / name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    candidate_csv = case_dir / "backtest_candidate_audit_pack.csv"
    evidence_csv = case_dir / "backtest_candidate_audit_pack_evidence.csv"
    duckdb_path = case_dir / "backtest_candidate_audit_pack.duckdb"
    duckdb_path.write_text("placeholder\n")
    if no_candidates:
        rows = [candidate_row("blocked", "BTC", 1, status="BLOCKED")]
        evidence = [evidence_row(case_dir, "blocked", "BTC", "deep_v1", 1)]
    else:
        rows = [
            candidate_row("leadkey", "HYPE", 1, private_count=1 if private_ready else 0, deployable=private_ready),
            candidate_row("secondkey", "DOGE", 2),
        ]
        evidence = [
            evidence_row(case_dir, "leadkey", "HYPE", "deep_v1", 1, private_truth=private_ready, requires_raw=raw_evidence),
            evidence_row(case_dir, "leadkey", "HYPE", "formal_latest", 2, private_truth=private_ready, requires_raw=raw_evidence),
            evidence_row(case_dir, "secondkey", "DOGE", "deep_v1", 3),
            evidence_row(case_dir, "secondkey", "DOGE", "formal_latest", 4),
        ]
    write_csv(candidate_csv, rows)
    write_csv(evidence_csv, evidence)
    audit_manifest = {
        "schema_version": "backtest_candidate_audit_pack_manifest_v1",
        "ok": True,
        "candidate_audit_pack_csv": str(candidate_csv),
        "candidate_audit_pack_evidence_csv": str(evidence_csv),
        "candidate_audit_pack_duckdb": str(duckdb_path),
        "candidate_audit_pack_fingerprint": f"fingerprint-{name}",
        "duckdb_tables": ["audit_candidates", "audit_candidate_evidence"],
        "duckdb_views": ["search_safe_private_blocked", "candidate_evidence_by_experiment"],
        "errors": [],
        "evidence_row_count": len(evidence),
        "filters": {"min_experiment_count": 2, "require_search_safe_gate": True},
        "forbidden_result_columns": {"candidates": ["winner_side"] if forbidden_columns else [], "evidence": []},
        "input_candidate_count": len(rows),
        "private_promotion_ready_count": 1 if private_ready else 0,
        "search_safe_private_blocked_count": 0 if no_candidates else 2,
        "selected_candidate_count": 0 if no_candidates else 2,
    }
    suite_manifest = {
        "schema_version": "backtest_experiment_suite_manifest_v1",
        "ok": True,
        "candidate_audit_pack": {
            "candidate_audit_pack_fingerprint": f"fingerprint-{name}",
            "private_promotion_ready_count": 1 if private_ready else 0,
            "selected_candidate_count": 0 if no_candidates else 2,
        },
        "errors": [],
        "input_hashes": {str(candidate_csv): "hash"},
        "steps": [{"name": "readiness_report", "ok": True, "readiness_fingerprint": f"readiness-{name}"}],
        "suite_fingerprint": f"suite-{name}",
    }
    write_json(case_dir / "BACKTEST_CANDIDATE_AUDIT_PACK_MANIFEST.json", audit_manifest)
    write_json(case_dir / "BACKTEST_EXPERIMENT_SUITE_MANIFEST.json", suite_manifest)
    return case_dir / "BACKTEST_CANDIDATE_AUDIT_PACK_MANIFEST.json", case_dir / "BACKTEST_EXPERIMENT_SUITE_MANIFEST.json"


def run_case(name, audit, suite):
    output = out_dir / name
    subprocess.run(
        [
            "python3",
            str(script),
            "--audit-manifest",
            str(audit),
            "--suite-manifest",
            str(suite),
            "--created-utc",
            created_utc,
            "--output-dir",
            str(output),
        ],
        check=True,
    )
    return json.loads((output / "manifest.json").read_text())


complete = make_case("complete")
private_ready = make_case("private_ready", private_ready=True)
forbidden_columns = make_case("forbidden_columns", forbidden_columns=True)
no_candidates = make_case("no_candidates", no_candidates=True)
raw_evidence = make_case("raw_evidence", raw_evidence=True)

cases = {
    "complete": run_case("complete", *complete),
    "private_ready": run_case("private_ready", *private_ready),
    "forbidden_columns": run_case("forbidden_columns", *forbidden_columns),
    "no_candidates": run_case("no_candidates", *no_candidates),
    "raw_evidence": run_case("raw_evidence", *raw_evidence),
}

checks = {
    "complete_keep": cases["complete"]["decision_label"] == "KEEP_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_READY_PRIVATE_BLOCKED",
    "complete_lead_hype": cases["complete"]["lead_candidate"]["asset"] == "HYPE",
    "complete_private_blocked": cases["complete"]["private_truth_ready"] is False
    and cases["complete"]["promotion_gate"]["passed"] is False,
    "private_ready_discard": cases["private_ready"]["decision_label"]
    == "DISCARD_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_FORBIDDEN_INPUT",
    "forbidden_columns_discard": cases["forbidden_columns"]["decision_label"]
    == "DISCARD_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_FORBIDDEN_INPUT",
    "no_candidates_unknown": cases["no_candidates"]["decision_label"]
    == "UNKNOWN_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_GAPS",
    "raw_evidence_discard": cases["raw_evidence"]["decision_label"]
    == "DISCARD_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_FORBIDDEN_INPUT",
    "no_deployable_claim": all(case["deployable"] is False and case["strategy_evidence"] is False for case in cases.values()),
}
manifest = {
    "artifact": "xuan_backtest_v1_candidate_filter_handoff_plan_smoke",
    "case_decisions": {name: case["decision_label"] for name, case in cases.items()},
    "checks": checks,
    "decision": "KEEP" if all(checks.values()) else "BLOCKED",
    "decision_label": "KEEP_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_SMOKE_PASS"
    if all(checks.values())
    else "BLOCKED_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_SMOKE_FAIL",
    "strategy_evidence": False,
    "private_truth_ready": False,
    "deployable": False,
    "promotion_gate": {"passed": False},
}
write_json(out_dir / "manifest.json", manifest)
if not all(checks.values()):
    raise SystemExit(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" > /dev/null
echo "smoke ok: $out_dir"
