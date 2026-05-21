#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_source_link_transition_bucket_scorer_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

fixture_root="${SOURCE_LINK_FIXTURE_ROOT:-}"
if [[ -z "$fixture_root" ]]; then
  fixture_root="$(find "$ROOT/xuan_research_artifacts" -maxdepth 1 -type d -name 'xuan_b27_dplus_passive_passive_shadow_runner_source_link_transition_smoke_*' | sort | tail -1)"
fi
if [[ -z "$fixture_root" || ! -d "$fixture_root" ]]; then
  echo "missing source-link transition smoke fixture root" >&2
  exit 2
fi
enabled_summary="$fixture_root/source_link_enabled/btc-updown-5m-1900000000.summary.json"
enabled_aggregate="$fixture_root/source_link_enabled/aggregate_report.json"
residual_summary="$fixture_root/source_link_residual_enabled/btc-updown-5m-1900000000.summary.json"
default_summary="$fixture_root/source_link_default_off/btc-updown-5m-1900000000.summary.json"

python3 -m py_compile scripts/xuan_b27_dplus_source_link_transition_bucket_scorer.py >> "$log" 2>&1

python3 scripts/xuan_b27_dplus_source_link_transition_bucket_scorer.py \
  --summary "$enabled_summary" \
  --aggregate "$enabled_aggregate" \
  --output "$out_dir/enabled_score.json" >> "$log" 2>&1

python3 scripts/xuan_b27_dplus_source_link_transition_bucket_scorer.py \
  --summary "$residual_summary" \
  --output "$out_dir/residual_score.json" >> "$log" 2>&1

set +e
python3 scripts/xuan_b27_dplus_source_link_transition_bucket_scorer.py \
  --summary "$default_summary" \
  --output "$out_dir/default_off_score_should_fail.json" >> "$log" 2>&1
missing_diag_rc=$?
set -e

python3 - "$out_dir" "$missing_diag_rc" "$enabled_summary" "$enabled_aggregate" "$residual_summary" "$default_summary" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
missing_diag_rc = int(sys.argv[2])
enabled_summary = sys.argv[3]
enabled_aggregate = sys.argv[4]
residual_summary = sys.argv[5]
default_summary = sys.argv[6]

enabled = json.loads((out_dir / "enabled_score.json").read_text())
residual = json.loads((out_dir / "residual_score.json").read_text())

checks = {
    "py_compile": True,
    "enabled_score_keep": enabled["research_ranking"]["decision"] == "KEEP",
    "enabled_aggregate_parity_passed": enabled["aggregate_parity"]["checked"] is True and enabled["aggregate_parity"]["passed"] is True,
    "enabled_immediate_pair_buckets_ranked": bool(enabled["rankings"]["top_immediate_pair_source_buckets"]),
    "enabled_cross_pair_buckets_ranked": bool(enabled["rankings"]["top_immediate_pair_source_cross_buckets"]),
    "enabled_cross_risk_direction_available": bool(enabled["rankings"]["risk_direction_by_status_side_offset"]),
    "residual_score_keep_without_aggregate": residual["research_ranking"]["decision"] == "KEEP" and residual["aggregate_parity"]["checked"] is False,
    "residual_buckets_ranked": bool(residual["rankings"]["top_residual_source_buckets"]) and residual["rankings"]["top_residual_source_buckets"][0]["residual_cost"] > 0,
    "residual_cross_buckets_ranked": bool(residual["rankings"]["top_residual_source_cross_buckets"]) and residual["rankings"]["top_residual_source_cross_buckets"][0]["residual_cost"] > 0,
    "summary_only_removal_refused": residual["guardrails"]["summary_only_removal_recommended"] is False,
    "missing_diagnostic_rejected": missing_diag_rc != 0,
    "promotion_gate_separated": enabled["promotion_gate"]["passed"] is False and residual["promotion_gate"]["passed"] is False,
}

if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"source-link scorer smoke failed checks: {failed}")

manifest = {
    "artifact": "xuan_b27_dplus_source_link_transition_bucket_scorer_smoke",
    "status": "PASS",
    "decision_label": "KEEP_SOURCE_LINK_TRANSITION_CROSS_BUCKET_SCORER_READY",
    "hypothesis": "A local no-network scorer can consume source-link transition summary/aggregate shape, validate aggregate parity, rank immediate-pair and residual source buckets including side+offset+risk-direction cross buckets, and refuse summary-only removal.",
    "inputs": {
        "enabled_summary": enabled_summary,
        "enabled_aggregate": enabled_aggregate,
        "residual_summary": residual_summary,
        "default_summary": default_summary,
    },
    "outputs": {
        "enabled_score": str(out_dir / "enabled_score.json"),
        "residual_score": str(out_dir / "residual_score.json"),
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_SOURCE_LINK_TRANSITION_CROSS_BUCKET_SCORER_READY",
        "interpretation": "The scorer is ready as local attribution tooling over source-link diagnostic summaries, including side+offset+risk-direction cross buckets. It is not strategy economics."
    },
    "promotion_gate": {
        "passed": False,
        "status": "SCORER_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
        "deployable": False,
        "can_support_strategy_promotion": False,
        "g2_canary_ready": False
    },
    "guardrails": {
        "future_bucket_rule_min_seed_or_pair_qty_retention": 0.85,
        "future_bucket_rule_max_residual_cost_share": 0.20,
        "future_bucket_rule_min_immediate_pair_qty_share_for_context": 0.10,
        "summary_only_removal_recommended": False,
        "diagnostic_shadow_policy": "Do not run EC2 shadow/canary from this smoke alone; a future diagnostic-only no-order shadow still needs a concrete bucket question and bounded preflight."
    },
    "side_effects": {
        "network_started": False,
        "ssh_started": False,
        "events_jsonl_read": False,
        "raw_replay_or_collector_scanned": False,
        "shared_ingress_connected": False,
        "broker_modified": False,
        "service_control_used": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False
    },
    "next_action": "Local status_hygiene: decide whether source-link scorer readiness is enough to justify a future diagnostic-only no-order shadow plan, or whether another local verifier should run first."
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(out_dir / "manifest.json")
PY

printf 'PASS D+ source-link transition bucket scorer smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
