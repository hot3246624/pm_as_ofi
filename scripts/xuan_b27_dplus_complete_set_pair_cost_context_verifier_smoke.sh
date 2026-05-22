#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_complete_set_pair_cost_context_verifier_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_complete_set_pair_cost_context_verifier.py"
python3 -m py_compile "$script" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
fixtures = out / "fixtures"
fixtures.mkdir(parents=True, exist_ok=True)

trades = []
for idx in range(30):
    condition = f"0x{idx:064x}"
    slug = f"btc-updown-15m-{1779430000 + idx * 900}"
    trades.extend(
        [
            {
                "conditionId": condition,
                "slug": slug,
                "outcome": "Up",
                "side": "BUY",
                "timestamp": 1779430000 + idx * 900 + 10,
                "price": 0.42,
                "size": 10,
                "transactionHash": f"0xup{idx}",
            },
            {
                "conditionId": condition,
                "slug": slug,
                "outcome": "Down",
                "side": "BUY",
                "timestamp": 1779430000 + idx * 900 + 12,
                "price": 0.53,
                "size": 8,
                "transactionHash": f"0xdown{idx}",
            },
        ]
    )
for idx in range(470):
    condition = f"0x{idx % 30:064x}"
    slug = f"btc-updown-15m-{1779430000 + (idx % 30) * 900}"
    trades.append(
        {
            "conditionId": condition,
            "slug": slug,
            "outcome": "Up" if idx % 2 == 0 else "Down",
            "side": "BUY",
            "timestamp": 1779430000 + idx,
            "price": 0.46 if idx % 2 == 0 else 0.49,
            "size": 1,
            "transactionHash": f"0xextra{idx}",
        }
    )

weak_trades = trades[:20]
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
    "status": "UNKNOWN_RESIDUAL_TAIL_EXEMPLAR_SHADOW_REVIEW_TRADEOFF",
    "checks": {"report_shape_ok": True, "metric_budgets_ok": False},
    "exemplar_metrics": {
        "required_field_coverage": 1.0,
        "source_sequence_coverage": 1.0,
        "quote_intent_coverage": 1.0,
        "source_order_coverage": 1.0,
        "required_field_coverage_by_field": {field: 1.0 for field in required_fields},
    },
}
(fixtures / "trades.json").write_text(json.dumps(trades, indent=2, sort_keys=True) + "\n")
(fixtures / "weak_trades.json").write_text(json.dumps(weak_trades, indent=2, sort_keys=True) + "\n")
(fixtures / "score.json").write_text(json.dumps(score, indent=2, sort_keys=True) + "\n")
PY

python3 "$script" \
  --public-trades "$out_dir/fixtures/trades.json" \
  --exemplar-score "$out_dir/fixtures/score.json" \
  --output-dir "$out_dir/positive" >> "$log" 2>&1

if python3 "$script" \
  --public-trades "$out_dir/fixtures/weak_trades.json" \
  --exemplar-score "$out_dir/fixtures/score.json" \
  --output-dir "$out_dir/weak" >> "$log" 2>&1; then
  echo "weak fixture unexpectedly returned KEEP" >> "$log"
  exit 1
fi

python3 - "$out_dir/positive/complete_set_pair_cost_context_score.json" "$out_dir/weak/complete_set_pair_cost_context_score.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

positive = json.loads(Path(sys.argv[1]).read_text())
weak = json.loads(Path(sys.argv[2]).read_text())
checks = {
    "positive_keep": positive["decision_label"] == "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_READY",
    "pair_cost_quantified": positive["public_complete_set_pair_cost_context"]["weighted_pair_cost_sum"] is not None,
    "positive_edge": positive["public_complete_set_pair_cost_context"]["weighted_pair_edge"] > 0,
    "condition_csv_written": Path(positive["outputs"]["condition_pair_cost_groups_csv"]).exists(),
    "fee_source_explicit": positive["fee_policy"]["account_average_fee_rate_used"] is False,
    "proxy_boundary": positive["public_proxy_boundary"]["public_profile_is_proxy_inspiration_only"] is True,
    "weak_unknown": weak["decision_label"] == "UNKNOWN_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_INPUT_GAP",
    "weak_blocker": "public_trade_pair_cost_proxy_signal_insufficient" in weak["blockers"],
    "promotion_gate_false": positive["promotion_gate"]["passed"] is False and weak["promotion_gate"]["passed"] is False,
    "no_side_effects": positive["side_effects"]["ssh_started"] is False
    and positive["side_effects"]["shadow_started"] is False
    and positive["side_effects"]["orders_sent"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"complete-set pair-cost verifier smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_complete_set_pair_cost_context_verifier_smoke",
    "status": "PASS",
    "decision_label": "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_READY",
    "hypothesis": "The verifier quantifies same-condition public/proxy complete-set pair-cost context while preserving private/promotion guards.",
    "outputs": {
        "positive_score": str(Path(sys.argv[1])),
        "weak_score": str(Path(sys.argv[2])),
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_COMPLETE_SET_PAIR_COST_CONTEXT_VERIFIER_READY",
        "interpretation": "Verifier readiness and proxy signal quantification only, not strategy promotion."
    },
    "promotion_gate": {
        "passed": False,
        "status": "PUBLIC_PROXY_VERIFIER_ONLY_NOT_PROMOTION_EVIDENCE",
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
    "next_action": "Use the verifier output to design a local candidate-pipeline complete-set pair-cost adapter; do not start remote shadow from proxy evidence alone."
}
(Path(sys.argv[1]).parent.parent / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(Path(sys.argv[1]).parent.parent / "manifest.json")
PY

printf 'PASS D+ complete-set pair-cost verifier smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
