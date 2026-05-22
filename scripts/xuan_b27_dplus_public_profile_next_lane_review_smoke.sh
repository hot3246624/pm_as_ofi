#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_public_profile_next_lane_review_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

script="scripts/xuan_b27_dplus_public_profile_next_lane_review.py"
python3 -m py_compile "$script" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
fixture = out_dir / "fixtures"
fixture.mkdir(parents=True, exist_ok=True)

public_profile = {
    "cross_profile_summary": {
        "best_positive_profile_wallet": "0xpositive",
        "positive_complete_set_proxy_profiles": 1,
        "negative_control_profiles": 1,
    },
    "profiles_reviewed": [
        {"wallet": "0xpositive", "classification": {"profile_label": "STRONG_PUBLIC_PROXY_POSITIVE_COMPLETE_SET_PAIRING"}},
        {"wallet": "0xnegative", "classification": {"profile_label": "NEGATIVE_CONTROL_HIGH_TURNOVER_RESIDUAL_HEAVY"}},
    ],
}
pairing = {
    "best_rule": "portfolio_risk_adjusted_nonnegative",
    "best_rule_delta_vs_control": {
        "seed_action_retention": 0.952,
        "pair_qty_retention": 0.956,
        "residual_qty_rate_reduction": 0.274,
        "residual_cost_rate_reduction": 0.466,
        "stress100_worst_pnl": {"absolute_delta": 10.34},
    },
    "promotion_gate": {"passed": False},
}
intersection_missing = {
    "decision_label": "UNKNOWN",
    "status": "UNKNOWN_PORTFOLIO_LEDGER_SOURCE_LINK_INTERSECTION_INPUTS_MISSING",
    "missing_files_or_fields": [
        "aggregate_report.event_lite.portfolio_ledger_diagnostics",
        "summary.event_lite.portfolio_ledger_diagnostics for every supplied summary",
    ],
}
post_sizer = {"decision_label": "UNKNOWN_COMPLETE_SET_POST_SIZER_NO_SAFE_LOCAL_MECHANISM"}
for name, obj in {
    "public_profile.json": public_profile,
    "pairing.json": pairing,
    "intersection_missing.json": intersection_missing,
    "post_sizer.json": post_sizer,
}.items():
    (fixture / name).write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
PY

fixture="$out_dir/fixtures"
python3 "$script" \
  --public-profile-manifest "$fixture/public_profile.json" \
  --pairing-verifier-manifest "$fixture/pairing.json" \
  --intersection-manifest "$fixture/intersection_missing.json" \
  --post-sizer-gap-manifest "$fixture/post_sizer.json" \
  --output-dir "$out_dir/review" >> "$log" 2>&1

python3 - "$out_dir" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
review = json.loads((out_dir / "review" / "manifest.json").read_text())
checks = {
    "decision_unknown": review["decision_label"] == "UNKNOWN_PUBLIC_PROFILE_LEAD_ACTIVE_BUT_NO_NEW_LOCAL_LANE",
    "profile_lead_active": review["public_profile_summary"]["profile_lead_active"] is True,
    "missing_intersection_named": "aggregate_report.event_lite.portfolio_ledger_diagnostics"
    in review["current_intersection_status"]["missing_files_or_fields"],
    "discarded_lane_present": any(
        row["lane"] == "complete_set_buffer_or_target_sizer" and row["decision"] == "DISCARD"
        for row in review["candidate_lane_reviews"]
    ),
    "promotion_false": review["promotion_gate"]["passed"] is False,
    "no_side_effects": review["side_effects"]["ssh_started"] is False
    and review["side_effects"]["orders_sent"] is False
    and review["guardrails"]["no_events_jsonl_read"] is True,
}
if not all(checks.values()):
    failed = [key for key, value in checks.items() if not value]
    raise SystemExit(f"failed checks: {failed}\nreview={review}")
manifest = {
    "artifact": "xuan_b27_dplus_public_profile_next_lane_review_smoke",
    "status": "PASS",
    "decision_label": "KEEP",
    "checks": checks,
    "outputs": {"review_manifest": str(out_dir / "review" / "manifest.json")},
    "side_effects": {
        "ssh_started": False,
        "network_started": False,
        "shadow_started": False,
        "orders_sent": False,
        "cancels_sent": False,
        "redeems_sent": False,
        "shared_ingress_modified": False,
        "broker_modified": False,
        "service_control_used": False,
    },
}
(out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps({"status": "PASS", "checks": checks}, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "PASS $out_dir"
