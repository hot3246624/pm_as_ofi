#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT="${ARTIFACT:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_truth_validation_response_action_scorer_smoke_${TS}}"
STATE_MACHINE="/Users/hot/web3Scientist/poly_backtest_data/derived/completion_candidate_pipeline_v1/pass_local_completion_residual_cooldown_officialfee_e055_t5_imb125_rc30_050_20260502_20260518_publicfull_v2/state_machine_results.duckdb"
SINGLE_DIR="$ROOT/xuan_research_artifacts/xuan_b27_dplus_replay_store_selected_candidate_truth_response_20260521T131256Z"
MULTI_DIR="$ROOT/xuan_research_artifacts/xuan_b27_dplus_replay_store_multiday_selected_candidate_truth_response_20260521T141153Z"

mkdir -p "$ARTIFACT/single" "$ARTIFACT/multiday"

python3 -m py_compile scripts/xuan_b27_dplus_truth_validation_response_action_scorer.py

python3 scripts/xuan_b27_dplus_truth_validation_response_action_scorer.py \
  --response-manifest "$SINGLE_DIR/response/VALIDATION_RESPONSE_MANIFEST.json" \
  --selected-actions "$SINGLE_DIR/selected_actions.json" \
  --state-machine-duckdb "$STATE_MACHINE" \
  --output-dir "$ARTIFACT/single"

python3 scripts/xuan_b27_dplus_truth_validation_response_action_scorer.py \
  --response-manifest "$MULTI_DIR/response/VALIDATION_RESPONSE_MANIFEST.json" \
  --selected-actions "$MULTI_DIR/selected_actions.json" \
  --state-machine-duckdb "$STATE_MACHINE" \
  --output-dir "$ARTIFACT/multiday"

python3 - "$ARTIFACT" <<'PY'
import json
import sys
from pathlib import Path

artifact = Path(sys.argv[1])
single = json.loads((artifact / "single" / "score.json").read_text())
multi = json.loads((artifact / "multiday" / "score.json").read_text())


def all_required_refs(score):
    return all(v["coverage"] == 1.0 for v in score["required_source_refs"].values())


def guards_false(score):
    return score["private_guard"]["all_false"] and not score["promotion_gate"]["passed"]


def close_gap_named(score):
    missing = set(score["strict_rescue_closes"]["missing_fields"])
    return {
        "close_action_id",
        "close_ts_ms",
        "close_side",
        "close_qty",
        "close_px",
        "fee_rate_source",
        "book_l1_ref",
        "book_l2_ref",
        "trade_before_ref",
        "trade_after_ref",
    }.issubset(missing)


checks = {
    "py_compile_passed": True,
    "single_score_keep": single["label"] == "KEEP_TRUTH_VALIDATION_RESPONSE_ACTION_SCORE_READY",
    "multiday_score_keep": multi["label"] == "KEEP_TRUTH_VALIDATION_RESPONSE_ACTION_SCORE_READY",
    "single_required_refs_100pct": all_required_refs(single),
    "multiday_required_refs_100pct": all_required_refs(multi),
    "single_fee_source_100pct": single["fee_metrics"]["fee_rate_source_coverage"] == 1.0,
    "multiday_fee_source_100pct": multi["fee_metrics"]["fee_rate_source_coverage"] == 1.0,
    "single_fee_delta_joined": single["fee_metrics"]["expected_fee_join_count"] == 10,
    "multiday_fee_delta_joined": multi["fee_metrics"]["expected_fee_join_count"] == 30,
    "single_private_promotion_guards_false": guards_false(single),
    "multiday_private_promotion_guards_false": guards_false(multi),
    "single_close_gap_named": close_gap_named(single),
    "multiday_close_gap_named": close_gap_named(multi),
    "multiday_breadth_preserved": multi["row_count"] == 30 and multi["day_count"] == 15 and multi["condition_count"] == 30,
    "no_side_effect_flags": not any(multi["side_effects"].values()) and not any(single["side_effects"].values()),
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise SystemExit("smoke checks failed: " + ",".join(failed))

manifest = {
    "artifact": "xuan_b27_dplus_truth_validation_response_action_scorer_smoke",
    "schema_version": "xuan_research_smoke_v1",
    "created_utc": artifact.name.rsplit("_", 1)[-1] if "_" in artifact.name else "",
    "status": "PASS",
    "decision_label": "KEEP_TRUTH_VALIDATION_RESPONSE_ACTION_SCORER_READY",
    "checks": checks,
    "scores": {
        "single": str(artifact / "single" / "score.json"),
        "multiday": str(artifact / "multiday" / "score.json"),
    },
    "summary": {
        "single": {
            "row_count": single["row_count"],
            "day_count": single["day_count"],
            "condition_count": single["condition_count"],
            "label": single["label"],
            "max_fee_delta": single["fee_metrics"]["max_abs_delta_vs_selected_expected_official_taker_fee"],
        },
        "multiday": {
            "row_count": multi["row_count"],
            "day_count": multi["day_count"],
            "condition_count": multi["condition_count"],
            "label": multi["label"],
            "max_fee_delta": multi["fee_metrics"]["max_abs_delta_vs_selected_expected_official_taker_fee"],
        },
    },
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_TRUTH_VALIDATION_RESPONSE_ACTION_SCORER_READY",
        "interpretation": "Scorer can audit replay-derived truth_validation_response_v1 outputs and name FIFO/close gaps. This is tooling readiness only.",
    },
    "promotion_gate": {
        "passed": False,
        "status": "SCORER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
        "private_truth_ready": False,
        "deployable": False,
        "g2_canary_ready": False,
    },
    "side_effects": {
        "ssh_started": False,
        "network_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "broad_strategy_search": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
    },
    "next_action": "Local status_hygiene: use scorer output to decide whether to implement selected-action residual FIFO verifier or require explicit strict rescue close request fields first.",
}
(artifact / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(artifact / "manifest.json")
PY

python3 -m json.tool "$ARTIFACT/manifest.json" >/dev/null
python3 -m json.tool "$ARTIFACT/single/score.json" >/dev/null
python3 -m json.tool "$ARTIFACT/multiday/score.json" >/dev/null
echo "PASS $ARTIFACT/manifest.json"
