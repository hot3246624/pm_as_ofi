#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_complete_set_pair_cost_context_verifier_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_complete_set_pair_cost_context_verifier_spec.py"
python3 -m py_compile "$script" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
fixtures = out / "fixtures"
fixtures.mkdir(parents=True, exist_ok=True)

positive_public = {
    "counts": {"trades": 1000, "activity": 1000, "positions": 4},
    "by_side": {"BUY": 1000},
    "by_outcome": {"Up": 510, "Down": 490},
    "by_slug": [["btc-updown-15m-1779439500", 1000]],
    "conditions_total": 25,
    "both_outcome_conditions": 23,
    "top_conditions": [
        {
            "conditionId": "0xabc",
            "slug": "btc-updown-15m-1779439500",
            "n": 100,
            "outcomes": {"Up": 52, "Down": 48},
            "sides": {"BUY": 100},
            "notional": 50.0,
            "size": 100.0,
            "min_ts": 1779439501,
            "max_ts": 1779440300,
            "span_s": 799,
            "both_outcomes": True,
        }
    ],
}
weak_public = dict(positive_public)
weak_public["conditions_total"] = 25
weak_public["both_outcome_conditions"] = 4
weak_public["by_outcome"] = {"Up": 1000}

required_fields = [
    "condition_id",
    "slug",
    "side",
    "offset_s",
    "source_risk_direction",
    "trigger_px",
    "trigger_size",
    "pre_seed_same_qty",
    "pre_seed_opp_qty",
    "pre_seed_same_cost",
    "pre_seed_opp_cost",
    "ledger_proxy_before",
    "ledger_proxy_after",
    "source_pair_qty",
    "source_pair_cost",
    "source_pair_pnl",
    "source_residual_qty",
    "source_residual_cost",
    "source_residual_age_ms",
    "source_sequence_id",
    "quote_intent_id",
    "source_order_id",
]
score = {
    "artifact": "xuan_b27_dplus_residual_tail_exemplar_shadow_scorer",
    "status": "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF",
    "checks": {
        "report_shape_ok": True,
        "required_field_coverage_ok": True,
        "source_sequence_coverage_ok": True,
        "metric_budgets_ok": False,
    },
    "exemplar_metrics": {
        "aggregate_exemplar_row_count": 8,
        "required_field_coverage": 1.0,
        "source_sequence_coverage": 1.0,
        "quote_intent_coverage": 1.0,
        "source_order_coverage": 1.0,
        "required_field_coverage_by_field": {field: 1.0 for field in required_fields},
    },
    "shadow_gate_failures": ["net_pair_cost_p90_above_1.0"],
}
completion = {
    "residual_tail_exemplar_score": {
        "status": "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF",
        "scorer_ready": True,
    },
    "promotion_gate": {"passed": False},
}
aggregate = {
    "event_lite": {
        "source_link": {"present": True},
        "residual_tail_exemplars": {"present": True},
        "source_pair": {"present": True},
    }
}
files = {
    "positive_public.json": positive_public,
    "weak_public.json": weak_public,
    "score.json": score,
    "completion.json": completion,
    "aggregate.json": aggregate,
}
for name, value in files.items():
    (fixtures / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
PY

python3 "$script" \
  --public-profile-summary "$out_dir/fixtures/positive_public.json" \
  --completion-manifest "$out_dir/fixtures/completion.json" \
  --exemplar-score "$out_dir/fixtures/score.json" \
  --aggregate-report "$out_dir/fixtures/aggregate.json" \
  --output "$out_dir/positive_spec.json" >> "$log" 2>&1

if python3 "$script" \
  --public-profile-summary "$out_dir/fixtures/weak_public.json" \
  --completion-manifest "$out_dir/fixtures/completion.json" \
  --exemplar-score "$out_dir/fixtures/score.json" \
  --aggregate-report "$out_dir/fixtures/aggregate.json" \
  --output "$out_dir/weak_spec.json" >> "$log" 2>&1; then
  echo "weak fixture unexpectedly returned KEEP" >> "$log"
  exit 1
fi

python3 - "$out_dir/positive_spec.json" "$out_dir/weak_spec.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

positive = json.loads(Path(sys.argv[1]).read_text())
weak = json.loads(Path(sys.argv[2]).read_text())
checks = {
    "positive_keep": positive["decision_label"] == "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_SPEC_READY",
    "selected_target_named": positive["verifier_spec"]["name"] == "complete_set_pair_cost_context_verifier_v1",
    "public_proxy_boundary": positive["public_proxy_boundary"]["public_profile_is_proxy_inspiration_only"] is True,
    "no_order_contract_ok": positive["no_order_residual_tail_field_contract"]["field_contract_ok"] is True,
    "pair_cost_fields_named": {
        "complete_set_pair_cost_sum",
        "complete_set_pair_cost_edge",
        "unpaired_residual_qty",
        "near_dual_outcome_window_s",
    }.issubset(set(positive["verifier_spec"]["complete_set_pair_cost_context_fields"])),
    "weak_unknown": weak["decision_label"] == "UNKNOWN_COMPLETE_SET_PAIR_COST_CONTEXT_INPUT_GAP",
    "weak_blocker_named": "public_profile_complete_set_proxy_signal_insufficient" in weak["blockers"],
    "promotion_gate_false": positive["promotion_gate"]["passed"] is False and weak["promotion_gate"]["passed"] is False,
    "remote_not_approved": positive["guardrails"]["remote_run_approved"] is False,
    "no_side_effects": positive["side_effects"]["ssh_started"] is False
    and positive["side_effects"]["shadow_started"] is False
    and positive["side_effects"]["events_jsonl_read"] is False
    and positive["side_effects"]["orders_sent"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"complete-set pair-cost spec smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_complete_set_pair_cost_context_verifier_spec_smoke",
    "status": "PASS",
    "decision_label": "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_SPEC_READY",
    "hypothesis": "A public/proxy complete-set profile signal plus no-order residual-tail exemplar field coverage can select a local pair-cost context verifier without promotion or remote execution.",
    "outputs": {
        "positive_spec": str(Path(sys.argv[1])),
        "weak_spec": str(Path(sys.argv[2])),
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_SPEC_READY",
        "interpretation": "Spec readiness only. Public profile evidence remains proxy inspiration and requires a local verifier before any new shadow question."
    },
    "promotion_gate": {
        "passed": False,
        "status": "SPEC_TOOLING_ONLY_NOT_PROMOTION_EVIDENCE",
        "deployable": False,
        "private_truth_ready": False,
        "g2_canary_ready": False
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
    "next_action": "Implement local complete_set_pair_cost_context_verifier_v1 scorer over public profile summary and no-order exemplar score; do not start remote shadow/canary from this spec alone."
}
(Path(sys.argv[1]).parent / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(Path(sys.argv[1]).parent / "manifest.json")
PY

printf 'PASS D+ complete-set pair-cost verifier spec smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
