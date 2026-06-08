#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_backtest_v1_hype_lead_prefix_source_preflight_review_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-${TMPDIR:-/tmp}/xuan_backtest_v1_hype_preflight_pycache_$ts}"
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT

python3 -m py_compile scripts/xuan_backtest_v1_hype_lead_prefix_source_preflight_review.py >> "$log" 2>&1

python3 - "$out_dir" "$ts" <<'PY' >> "$log" 2>&1
import json
import shutil
import subprocess
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
created_utc = sys.argv[2]
fixture_root = out_dir / "fixtures"
script = Path("scripts/xuan_backtest_v1_hype_lead_prefix_source_preflight_review.py")


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def proposal_manifest(path, *, private_claim=False, decision="KEEP"):
    decision_label = (
        "KEEP_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_READY_APPROVAL_REQUIRED"
        if decision == "KEEP"
        else "UNKNOWN_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_GAPS"
    )
    write_json(
        path,
        {
            "contract_name": "xuan_backtest_v1_hype_lead_same_window_handoff_proposal_v1",
            "decision": decision,
            "decision_label": decision_label,
            "deployable": False,
            "forbidden_blockers": [],
            "no_order_diagnostic_allowed": False,
            "private_truth_ready": private_claim,
            "promotion_gate": {"passed": private_claim},
            "proposal": {
                "approval_required_in_thread": True,
                "asset": "HYPE",
                "candidate_filter": {
                    "asset": "HYPE",
                    "max_l1_immediate_pair": "2.0",
                    "max_l1_pair_ask": "1.1",
                    "offset_s": {"high": "30.0", "low": "0.0"},
                    "pnl_cost_source": "pair_ask",
                    "price": {"high": "0.55", "low": "0.5"},
                    "side_alignment": "high",
                    "size": {"high": "150.0", "low": "50.0"},
                },
                "candidate_key": "30cfee296e1d0912f78109dd",
                "diagnostic_allowed_now": False,
                "market_prefix": "hype-updown-5m",
                "no_order_safety_boundary": {
                    "canary_or_live": False,
                    "events_jsonl_pull_or_read": False,
                    "orders_cancels_redeems": False,
                    "raw_replay_or_full_store_scan": False,
                },
                "train_holdout_label_policy": {
                    "labels_allowed_in_live_predicate": False,
                    "realized_pair_cost_allowed_as_live_criteria": False,
                },
            },
            "strategy_evidence": False,
        },
    )


RESOLVER_FULL = '''
def slug_from_prefix(prefix: str, round_offset: int):
    target = 1
    return f"{prefix}-{target}"
def parse_market(payload, slug): pass
g.add_argument("--prefix")
slug = ns.slug or slug_from_prefix(ns.prefix, ns.round_offset)
def print_market(market: ResolvedMarket, fmt: str):
    print(f"export POLYMARKET_MARKET_SLUG={market.slug}")
    print(f"export POLYMARKET_YES_ASSET_ID={market.yes_asset_id}")
    print(f"export POLYMARKET_NO_ASSET_ID={market.no_asset_id}")
cache_get(cache_path, slug)
'''

RESOLVER_MISSING = '''
def slug_from_prefix(prefix, round_offset):
    return "btc-updown-5m-1"
def parse_market(payload, slug): pass
'''

RUNNER_BASE = '''
seed_offset_min_s: float = 0.0
ap.add_argument("--prefix", default="btc-updown-5m")
ap.add_argument("--seed-px-lo")
ap.add_argument("--seed-px-hi")
ap.add_argument("--seed-offset-max-s")
ap.add_argument("--seed-l1-cap")
markets = resolve_markets(Path(args.repo), args.prefix, args.round_offsets)
--observable-pre-action-rule-miner-feature-join-output
--observable-pre-action-same-window-offline-label-handoff-output
--observable-pre-action-same-window-offline-label-handoff-output requires --observable-pre-action-rule-miner-feature-join-output
source_seed_candidate_row_id
source_seed_action_id
self.cfg.observable_pre_action_same_window_offline_label_handoff_output
row["source_seed_candidate_row_id"]
row["source_seed_action_id"]
def aggregate_observable_pre_action_same_window_label_handoff_outputs(): pass
"observable_pre_action_same_window_offline_label_handoff.json"
"observable_pre_action_candidate_rows.jsonl"
"observable_pre_action_same_window_offline_labels.csv"
"events_jsonl_read": False
"events_jsonl_pulled": False
"raw_replay_or_full_store_scan": False
"trading_behavior_changed": False
"promotion_gate": {"passed": False
'''

RUNNER_FULL = RUNNER_BASE + '''
--public-trade-size-lo
--public-trade-size-hi
--max-l1-immediate-pair
--side-alignment
--pnl-cost-source
--candidate-filter-manifest
'''

RUNNER_MISSING_HANDOFF = '''
seed_offset_min_s: float = 0.0
ap.add_argument("--prefix", default="btc-updown-5m")
markets = resolve_markets(Path(args.repo), args.prefix, args.round_offsets)
'''

DOWNSTREAM = {
    "contract.py": '''
HANDOFF_SCHEMA = "observable_pre_action_same_window_offline_label_handoff_v1"
"source_seed_candidate_row_id"
labels_allowed_in_live_predicate
''',
    "materializer.py": '''
HANDOFF_SCHEMA = "observable_pre_action_same_window_offline_label_handoff_v1"
"exact_id:source_seed_candidate_row_id"
feature_row_
source_seed_candidate_row_id
''',
    "arrival.py": '''
source_handoff_id
source_seed_candidate_row_id
duplicate_candidate_exact_id_count
accepted_exact_id_ready_bridge_output_count
''',
    "accumulator.py": '''
source_handoff_id
duplicate_handoff_id
duplicate_candidate_exact_id
accumulation_source_handoff_id
''',
    "training.py": '''
promotion_gate
private_truth_ready
deployable
''',
}


def make_case(name, *, runner=RUNNER_BASE, resolver=RESOLVER_FULL, private_claim=False, decision="KEEP"):
    case = fixture_root / name
    if case.exists():
        shutil.rmtree(case)
    case.mkdir(parents=True)
    proposal = case / "proposal.json"
    proposal_manifest(proposal, private_claim=private_claim, decision=decision)
    runner_path = case / "runner.py"
    resolver_path = case / "resolver.py"
    write(runner_path, runner)
    write(resolver_path, resolver)
    downstream_paths = {}
    for filename, text in DOWNSTREAM.items():
        path = case / filename
        write(path, text)
        downstream_paths[filename] = path
    return proposal, runner_path, resolver_path, downstream_paths


def run_case(name, fixture):
    proposal, runner, resolver, downstream = fixture
    output = out_dir / name
    subprocess.run(
        [
            "python3",
            str(script),
            "--proposal-manifest",
            str(proposal),
            "--runner-source",
            str(runner),
            "--resolver-source",
            str(resolver),
            "--contract-source",
            str(downstream["contract.py"]),
            "--materializer-source",
            str(downstream["materializer.py"]),
            "--arrival-source",
            str(downstream["arrival.py"]),
            "--accumulator-source",
            str(downstream["accumulator.py"]),
            "--training-source",
            str(downstream["training.py"]),
            "--created-utc",
            created_utc,
            "--output-dir",
            str(output),
        ],
        check=True,
    )
    return json.loads((output / "manifest.json").read_text())


cases = {
    "full_support": make_case("full_support", runner=RUNNER_FULL),
    "current_like_candidate_filter_gaps": make_case("current_like_candidate_filter_gaps"),
    "private_claim": make_case("private_claim", runner=RUNNER_FULL, private_claim=True),
    "proposal_not_keep": make_case("proposal_not_keep", runner=RUNNER_FULL, decision="UNKNOWN"),
    "resolver_missing_prefix": make_case("resolver_missing_prefix", runner=RUNNER_FULL, resolver=RESOLVER_MISSING),
    "runner_missing_handoff": make_case("runner_missing_handoff", runner=RUNNER_MISSING_HANDOFF),
}
results = {name: run_case(name, fixture) for name, fixture in cases.items()}

assert results["full_support"]["decision_label"] == "KEEP_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_READY_APPROVAL_REQUIRED"
assert results["full_support"]["proposal_approval_required_in_thread"] is True
assert results["full_support"]["no_order_diagnostic_allowed"] is False
assert results["full_support"]["research_ranking"]["candidate_filter_exact_runtime_supported"] is True
assert results["current_like_candidate_filter_gaps"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_CANDIDATE_FILTER_GAPS"
assert "candidate_filter_exact_runtime_adapter_absent" in results["current_like_candidate_filter_gaps"]["blockers"]
assert results["private_claim"]["decision_label"] == "DISCARD_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_FORBIDDEN_INPUT"
assert results["proposal_not_keep"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_CANDIDATE_FILTER_GAPS"
assert results["resolver_missing_prefix"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_CANDIDATE_FILTER_GAPS"
assert results["runner_missing_handoff"]["decision_label"] == "UNKNOWN_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_CANDIDATE_FILTER_GAPS"

manifest = {
    "artifact": "xuan_backtest_v1_hype_lead_prefix_source_preflight_review_smoke",
    "case_decisions": {name: result["decision_label"] for name, result in sorted(results.items())},
    "contract_name": "xuan_backtest_v1_hype_lead_prefix_source_preflight_review_smoke_v1",
    "created_utc": created_utc,
    "decision": "KEEP",
    "decision_label": "KEEP_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_REVIEW_SMOKE_PASS",
    "promotion_gate": {"passed": False},
    "safety": {
        "events_jsonl_read": False,
        "market_resolver_executed": False,
        "new_no_order_diagnostic_started": False,
        "raw_replay_or_full_store_scan": False,
    },
}
write_json(out_dir / "manifest.json", manifest)
print(json.dumps({"decision_label": manifest["decision_label"], "output": str(out_dir)}, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "smoke ok: $out_dir/manifest.json"
