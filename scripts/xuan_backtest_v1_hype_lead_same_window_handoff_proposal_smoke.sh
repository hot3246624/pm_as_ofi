#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_backtest_v1_hype_lead_same_window_handoff_proposal_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-${TMPDIR:-/tmp}/xuan_backtest_v1_hype_lead_proposal_pycache_$ts}"
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT

python3 -m py_compile scripts/xuan_backtest_v1_hype_lead_same_window_handoff_proposal.py >> "$log" 2>&1

python3 - "$out_dir" "$ts" <<'PY' >> "$log" 2>&1
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
created_utc = sys.argv[2]
fixture_root = out_dir / "fixtures"
script = Path("scripts/xuan_backtest_v1_hype_lead_same_window_handoff_proposal.py")


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def params(**overrides):
    base = {
        "asset": "HYPE",
        "max_l1_immediate_pair": "2.0",
        "max_l1_pair_ask": "1.1",
        "offset_hi": "30.0",
        "offset_lo": "0.0",
        "pnl_cost_source": "pair_ask",
        "price_hi": "0.55",
        "price_lo": "0.5",
        "side_alignment": "high",
        "size_hi": "150.0",
        "size_lo": "50.0",
    }
    base.update(overrides)
    return base


def validation_manifest(path, *, candidate_params=None, private_truth=False, requires_raw=False, requires_remote=False):
    write_json(
        path,
        {
            "asset": candidate_params.get("asset", "HYPE") if candidate_params else "HYPE",
            "candidate_key": "30cfee296e1d0912f78109dd",
            "candidate_params": candidate_params if candidate_params is not None else params(),
            "ok": True,
            "promotion": {
                "deployable_ready": False,
                "private_truth_ready": private_truth,
                "search_safe_validation_ready": True,
            },
            "schema_version": "backtest_validation_manifest_v1",
            "validation_scope": {
                "private_truth_ready_on_completion": False,
                "requires_raw": requires_raw,
                "requires_remote": requires_remote,
            },
        },
    )


def plan_manifest(case_dir, *, decision="KEEP", decision_label=None, asset="HYPE", private_claim=False, raw=False, remote=False, inconsistent=False, missing_params=False):
    if decision_label is None:
        decision_label = "KEEP_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_READY_PRIVATE_BLOCKED" if decision == "KEEP" else "UNKNOWN_BACKTEST_V1_CANDIDATE_FILTER_HANDOFF_PLAN_GAPS"
    deep = case_dir / "validation_deep.json"
    formal = case_dir / "validation_formal.json"
    p1 = params(asset=asset)
    p2 = params(asset=asset, price_hi="0.57") if inconsistent else params(asset=asset)
    if missing_params:
        p1.pop("price_hi", None)
    validation_manifest(deep, candidate_params=p1, private_truth=private_claim, requires_raw=raw, requires_remote=remote)
    validation_manifest(formal, candidate_params=p2, private_truth=private_claim, requires_raw=raw, requires_remote=remote)
    return {
        "contract_name": "xuan_backtest_v1_candidate_filter_handoff_plan_v1",
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "forbidden_blockers": [],
        "lead_candidate": {
            "asset": asset,
            "audit_rank": 1,
            "audit_status": "SEARCH_SAFE_READY_PRIVATE_BLOCKED",
            "best_queue_pnl": "-0.05",
            "candidate_key": "30cfee296e1d0912f78109dd",
            "deployable_ready": False,
            "experiment_labels": "deep_v1;formal_latest",
            "private_promotion_gate_pass": False,
            "private_truth_ready_count": 0,
            "promotion_blockers": "owner_private_truth_missing_for_deployable_promotion",
        },
        "lead_candidate_evidence": [
            {
                "experiment_label": "deep_v1",
                "requires_raw": raw,
                "requires_remote": remote,
                "validation_manifest": str(deep),
            },
            {
                "experiment_label": "formal_latest",
                "requires_raw": raw,
                "requires_remote": remote,
                "validation_manifest": str(formal),
            },
        ],
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": private_claim,
        "promotion_gate": {"passed": private_claim},
        "research_ranking": {"real_miner_input_ready": False, "strategy_evidence": False},
        "safety": {
            "events_jsonl_read": False,
            "new_no_order_diagnostic_started": False,
            "raw_replay_or_full_store_scan": False,
        },
        "strategy_evidence": False,
        "suite": {
            "candidate_audit_pack_fingerprint": "pack",
            "readiness_fingerprint": "readiness",
            "suite_fingerprint": "suite",
        },
    }


def make_case(name, **kwargs):
    case_dir = fixture_root / name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    plan = plan_manifest(case_dir, **kwargs)
    plan_path = case_dir / "plan_manifest.json"
    write_json(plan_path, plan)
    return plan_path


def run_case(name, plan_path):
    output = out_dir / name
    subprocess.run(
        [
            "python3",
            str(script),
            "--plan-manifest",
            str(plan_path),
            "--created-utc",
            created_utc,
            "--output-dir",
            str(output),
        ],
        check=True,
    )
    return json.loads((output / "manifest.json").read_text())


cases = {
    "complete": make_case("complete"),
    "plan_not_keep": make_case("plan_not_keep", decision="UNKNOWN"),
    "private_claim": make_case("private_claim", private_claim=True),
    "raw_remote": make_case("raw_remote", raw=True, remote=True),
    "inconsistent_params": make_case("inconsistent_params", inconsistent=True),
    "wrong_asset": make_case("wrong_asset", asset="DOGE"),
    "missing_params": make_case("missing_params", missing_params=True),
}
results = {name: run_case(name, path) for name, path in cases.items()}

assert results["complete"]["decision_label"] == "KEEP_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_READY_APPROVAL_REQUIRED"
proposal = results["complete"]["proposal"]
assert proposal["asset"] == "HYPE"
assert proposal["candidate_key"] == "30cfee296e1d0912f78109dd"
assert proposal["market_prefix"] == "hype-updown-5m"
assert proposal["minimum_multiday_window"]["train_days"] == 3
assert proposal["minimum_multiday_window"]["holdout_days"] == 2
assert proposal["train_holdout_label_policy"]["labels_allowed_in_live_predicate"] is False
assert proposal["diagnostic_allowed_now"] is False
assert results["complete"]["promotion_gate"]["passed"] is False
assert results["complete"]["research_ranking"]["strategy_evidence"] is False

assert results["plan_not_keep"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_GAPS"
assert results["private_claim"]["decision_label"] == "DISCARD_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_FORBIDDEN_INPUT"
assert results["raw_remote"]["decision_label"] == "DISCARD_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_FORBIDDEN_INPUT"
assert results["inconsistent_params"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_GAPS"
assert results["wrong_asset"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_GAPS"
assert results["missing_params"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_GAPS"

manifest = {
    "artifact": "xuan_backtest_v1_hype_lead_same_window_handoff_proposal_smoke",
    "case_decisions": {name: result["decision_label"] for name, result in sorted(results.items())},
    "contract_name": "xuan_backtest_v1_hype_lead_same_window_handoff_proposal_smoke_v1",
    "created_utc": created_utc,
    "decision": "KEEP",
    "decision_label": "KEEP_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_SMOKE_PASS",
    "promotion_gate": {"passed": False},
    "safety": {
        "events_jsonl_read": False,
        "new_no_order_diagnostic_started": False,
        "raw_replay_or_full_store_scan": False,
    },
}
write_json(out_dir / "manifest.json", manifest)
print(json.dumps({"decision_label": manifest["decision_label"], "output": str(out_dir)}, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "smoke ok: $out_dir/manifest.json"
