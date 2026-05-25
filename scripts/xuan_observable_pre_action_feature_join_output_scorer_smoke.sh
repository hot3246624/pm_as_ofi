#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_observable_pre_action_feature_join_output_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-$out_dir/.pycache}"

python3 -m py_compile scripts/xuan_observable_pre_action_feature_join_output_scorer.py >> "$log" 2>&1

python3 - "$ROOT" "$out_dir" <<'PY' >> "$log" 2>&1
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
script = root / "scripts/xuan_observable_pre_action_feature_join_output_scorer.py"
spec = importlib.util.spec_from_file_location("feature_join_scorer", script)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows))


def field_contract(**overrides: object) -> dict[str, object]:
    data = {
        "schema_version": "observable_pre_action_feature_join_field_contract_v1",
        "default_off": True,
        "writes_events_jsonl": False,
        "reads_events_jsonl": False,
        "raw_replay_or_full_store_scan": False,
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
        "strategy_evidence": False,
        "private_truth_ready": False,
        "deployable": False,
        "promotion_gate_passed": False,
    }
    data.update(overrides)
    return data


def row(status: str, reason: str, *, side: str, seq: bool, quote: bool, order: bool) -> dict[str, object]:
    return {
        "schema_version": "observable_pre_action_candidate_row_v1",
        "condition_id": "condition-fixture",
        "market_slug": "btc-updown-5m-1900000000",
        "day_id": "2030-03-17",
        "quote_ts_ms": 1900000020000,
        "side": side,
        "offset_s": 20.0,
        "offset_bucket": "offset_0_30",
        "status_before_action": status,
        "reason": reason,
        "block_reason": reason if status == "blocked" else None,
        "source_sequence_present": seq,
        "quote_intent_present": quote,
        "source_order_present": order,
        "source_sequence_id_policy": "present_redacted" if seq else "missing",
        "quote_intent_id_policy": "present_redacted" if quote else "missing",
        "source_order_id_policy": "present_redacted" if order else "missing",
        "pre_seed_same_qty": 0.0,
        "pre_seed_opp_qty": 5.0 if side == "NO" else 0.0,
        "pre_seed_same_cost": 0.0,
        "pre_seed_opp_cost": 2.0 if side == "NO" else 0.0,
        "pre_seed_open_qty": 5.0 if side == "NO" else 0.0,
        "pre_seed_open_cost": 2.0 if side == "NO" else 0.0,
        "pre_seed_deficit_qty": 5.0 if side == "NO" else 0.0,
        "candidate_qty": 5.0 if status == "admitted" else None,
        "pre_seed_same_qty_bucket": "same_qty_zero",
        "pre_seed_opp_qty_bucket": "opp_qty_eq_5" if side == "NO" else "opp_qty_zero",
        "pre_seed_open_qty_bucket": "open_qty_eq_5" if side == "NO" else "open_qty_zero",
        "pre_seed_deficit_qty_bucket": "deficit_eq_5" if side == "NO" else "deficit_zero",
        "candidate_qty_bucket": "candidate_qty_eq_5" if status == "admitted" else "candidate_qty_unknown",
        "source_risk_direction": "repair_or_pairing_improving" if side == "NO" else "risk_increasing",
        "post_action_outcome_labels_included": False,
        "realized_pair_cost_used_as_live_criteria": False,
        "trading_behavior_changed": False,
    }


def materialize_case(case_dir: Path, rows: list[dict[str, object]], contract=None) -> None:
    contract = contract or field_contract()
    rows_path = case_dir / "observable_pre_action_candidate_rows.jsonl"
    summary_path = case_dir / "observable_pre_action_source_link_summary.json"
    manifest_path = case_dir / "observable_pre_action_feature_join_manifest.json"
    write_jsonl(rows_path, rows)
    summary = {
        "schema_version": "observable_pre_action_source_link_summary_v1",
        "row_count": len(rows),
        "slug_count": 1,
        **mod.row_counts(rows),
        "field_contract": contract,
    }
    write_json(summary_path, summary)
    manifest = {
        "schema_version": "observable_pre_action_feature_join_manifest_v1",
        "candidate_rows_path": rows_path.name,
        "source_link_summary_path": summary_path.name,
        "candidate_row_count": len(rows),
        "slug_manifest_count": 1,
        "slug_count_with_rows": 1,
        "field_contract": contract,
        "safety": {
            "orders_sent": False,
            "cancels_sent": False,
            "redeems_sent": False,
            "shared_ingress_modified": False,
            "broker_modified": False,
            "service_control_used": False,
            "events_jsonl_read": False,
            "events_jsonl_pulled": False,
            "raw_replay_or_full_store_scan": False,
        },
    }
    write_json(manifest_path, manifest)


good_rows = [
    row("admitted", "candidate", side="YES", seq=True, quote=True, order=True),
    row("blocked", "offset", side="NO", seq=True, quote=False, order=False),
]
materialize_case(out_dir / "good", good_rows)

bad_missing_field = [dict(good_rows[0]), dict(good_rows[1])]
bad_missing_field[0].pop("source_sequence_present")
materialize_case(out_dir / "bad_missing_field", bad_missing_field)

materialize_case(out_dir / "bad_private_claim", good_rows, field_contract(deployable=True))


def run(case: str) -> dict[str, object]:
    case_dir = out_dir / case
    result_dir = out_dir / f"{case}_scored"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--candidate-rows",
            str(case_dir / "observable_pre_action_candidate_rows.jsonl"),
            "--source-link-summary",
            str(case_dir / "observable_pre_action_source_link_summary.json"),
            "--feature-join-manifest",
            str(case_dir / "observable_pre_action_feature_join_manifest.json"),
            "--output-dir",
            str(result_dir),
        ],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return json.loads((result_dir / "manifest.json").read_text())


good = run("good")
missing = run("bad_missing_field")
private_claim = run("bad_private_claim")
missing_summary = subprocess.run(
    [
        sys.executable,
        str(script),
        "--candidate-rows",
        str(out_dir / "good/observable_pre_action_candidate_rows.jsonl"),
        "--source-link-summary",
        str(out_dir / "missing_source_summary.json"),
        "--feature-join-manifest",
        str(out_dir / "good/observable_pre_action_feature_join_manifest.json"),
        "--output-dir",
        str(out_dir / "missing_summary_scored"),
    ],
    cwd=root,
    check=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)
missing_summary_manifest = json.loads((out_dir / "missing_summary_scored/manifest.json").read_text())

assert good["decision_label"] == "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_READY"
assert good["score"]["row_count"] == 2
assert good["promotion_gate"]["passed"] is False
assert good["private_truth_ready"] is False
assert good["deployable"] is False
assert missing["decision"] == "UNKNOWN"
assert any("missing_required_fields" in blocker for blocker in missing["blockers"])
assert private_claim["decision"] == "UNKNOWN"
assert "feature_manifest_field_contract_deployable_not_false" in private_claim["blockers"]
assert missing_summary_manifest["decision"] == "UNKNOWN"
assert "source_link_summary_missing" in missing_summary_manifest["blockers"]

manifest = {
    "artifact": "xuan_observable_pre_action_feature_join_output_scorer_smoke",
    "schema_version": 1,
    "decision": "KEEP",
    "decision_label": "KEEP_OBSERVABLE_PRE_ACTION_FEATURE_JOIN_OUTPUT_SCORER_SMOKE_PASS",
    "checks": {
        "good_fixture_keep": True,
        "missing_source_field_fail_closed": True,
        "private_or_deployable_claim_fail_closed": True,
        "missing_source_summary_fail_closed": True,
        "promotion_gate_separated": True,
        "private_truth_ready_false": True,
        "deployable_false": True,
    },
}
write_json(out_dir / "manifest.json", manifest)
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/good_scored/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/bad_missing_field_scored/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/bad_private_claim_scored/manifest.json" >/dev/null
python3 -m json.tool "$out_dir/missing_summary_scored/manifest.json" >/dev/null

echo "smoke output: $out_dir" >> "$log"
