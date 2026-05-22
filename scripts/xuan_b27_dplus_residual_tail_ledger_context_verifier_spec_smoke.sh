#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_residual_tail_ledger_context_verifier_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_residual_tail_ledger_context_verifier_spec.py"
python3 -m py_compile "$script" >> "$log" 2>&1

python3 "$script" --output "$out_dir/spec.json" >> "$log" 2>&1

python3 - "$out_dir/spec.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

spec = json.loads(Path(sys.argv[1]).read_text())
needed = set(spec["verifier_spec"]["producer_export_needed"]["fields"])
checks = {
    "decision_keep": spec["decision_label"] == "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_SPEC_READY",
    "candidate_schema_present": spec["schema_checks"]["candidate_base_required_present"] is True,
    "actions_schema_present": spec["schema_checks"]["actions_required_present"] is True,
    "residual_lots_schema_present": spec["schema_checks"]["residual_lots_required_present"] is True,
    "producer_export_needed": spec["verifier_spec"]["producer_export_needed"]["name"] == "selected_seed_ledger_context_export_v1",
    "pre_seed_fields_named": {"pre_seed_same_qty", "pre_seed_opp_qty", "ledger_proxy_before", "ledger_proxy_after"}.issubset(needed),
    "source_attribution_fields_named": {"source_pair_qty", "source_pair_pnl", "source_residual_qty", "source_residual_cost"}.issubset(needed),
    "external_ids_named": {"source_sequence_id", "quote_intent_id", "source_order_id"}.issubset(needed),
    "remote_not_approved": spec["guardrails"]["remote_run_approved"] is False,
    "promotion_gate_false": spec["promotion_gate"]["passed"] is False,
    "no_side_effects": spec["side_effects"]["ssh_started"] is False
    and spec["side_effects"]["events_jsonl_read"] is False
    and spec["side_effects"]["orders_sent"] is False,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise AssertionError(f"residual-tail ledger-context verifier spec smoke failed checks: {failed}")
manifest = {
    "artifact": "xuan_b27_dplus_residual_tail_ledger_context_verifier_spec_smoke",
    "status": "PASS",
    "decision_label": "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_SPEC_READY",
    "hypothesis": "A local schema/spec review can prove the ledger-context verifier is implementable only after adding a default-off producer export, while naming exact missing fields from current outputs.",
    "outputs": {
        "spec": str(Path(sys.argv[1]))
    },
    "checks": checks,
    "research_ranking": {
        "decision": "KEEP",
        "label": "KEEP_RESIDUAL_TAIL_LEDGER_CONTEXT_VERIFIER_SPEC_READY",
        "interpretation": "Spec readiness only. The verifier still needs producer export implementation and a future real exemplar score."
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
    "next_action": "Implement default-off selected_seed_ledger_context_export_v1 in the local state-machine and smoke it before any mechanism verifier or remote run."
}
(Path(sys.argv[1]).parent / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(Path(sys.argv[1]).parent / "manifest.json")
PY

printf 'PASS D+ residual-tail ledger-context verifier spec smoke: %s\n' "$out_dir/manifest.json" | tee -a "$log"
