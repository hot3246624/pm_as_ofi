#!/usr/bin/env python3
"""Review local source support for the proposed HYPE same-window handoff path.

This local-only preflight reads the committed HYPE proposal manifest and current
worktree source/spec files. It checks whether the proposed `hype-updown-5m`
handoff path is source-supported, names exact future preflight commands, and
keeps the request approval-gated. It does not resolve markets, SSH, start a
runner, read events JSONL, or scan raw/replay stores.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_backtest_v1_hype_lead_prefix_source_preflight_review"
CONTRACT_NAME = "xuan_backtest_v1_hype_lead_prefix_source_preflight_review_v1"

READY_DECISION = "KEEP_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_READY_APPROVAL_REQUIRED"
UNKNOWN_DECISION = "UNKNOWN_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_CANDIDATE_FILTER_GAPS"
DISCARD_DECISION = "DISCARD_BACKTEST_V1_HYPE_LEAD_PREFIX_SOURCE_PREFLIGHT_FORBIDDEN_INPUT"

DEFAULT_PROPOSAL_MANIFEST = Path(
    "xuan_research_artifacts/"
    "xuan_backtest_v1_hype_lead_same_window_handoff_proposal_20260527T011815Z/"
    "manifest.json"
)
DEFAULT_RUNNER_SOURCE = Path("tools/xuan_dplus_passive_passive_shadow_runner.py")
DEFAULT_RESOLVER_SOURCE = Path("scripts/resolve_market_ids.py")
DEFAULT_CONTRACT_SOURCE = Path("scripts/xuan_observable_pre_action_same_window_offline_label_handoff_contract.py")
DEFAULT_MATERIALIZER_SOURCE = Path("scripts/xuan_observable_pre_action_non_fixture_training_bridge_materializer.py")
DEFAULT_ARRIVAL_SOURCE = Path("scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_arrival_inventory.py")
DEFAULT_ACCUMULATOR_SOURCE = Path("scripts/xuan_observable_pre_action_multiday_same_window_label_handoff_accumulator.py")
DEFAULT_TRAINING_SOURCE = Path("scripts/xuan_observable_pre_action_rule_miner_training_fixture_scorer.py")

EXPECTED_PROPOSAL_DECISION = "KEEP_BACKTEST_V1_HYPE_LEAD_SAME_WINDOW_HANDOFF_PROPOSAL_READY_APPROVAL_REQUIRED"
EXPECTED_ASSET = "HYPE"
EXPECTED_CANDIDATE_KEY = "30cfee296e1d0912f78109dd"
EXPECTED_PREFIX = "hype-updown-5m"

FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/replay",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def path_safe(path: Path) -> tuple[bool, str | None]:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    if any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS):
        return False, "forbidden_path_fragment"
    if path.suffix == ".jsonl":
        return False, "jsonl_input_not_allowed_for_source_preflight"
    return True, None


def read_source(path: Path, label: str, blockers: list[str], forbidden: list[str]) -> str:
    ok, reason = path_safe(path)
    if not ok:
        forbidden.append(f"{label}_{reason}:{path}")
        return ""
    if not path.exists():
        blockers.append(f"{label}_missing:{path}")
        return ""
    try:
        return path.read_text()
    except OSError as exc:
        blockers.append(f"{label}_read_failed:{exc}")
        return ""


def has_all(text: str, patterns: list[str]) -> tuple[bool, list[str]]:
    missing = [pattern for pattern in patterns if pattern not in text]
    return not missing, missing


def validate_proposal(proposal: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    blockers: list[str] = []
    forbidden: list[str] = []
    if proposal.get("contract_name") != "xuan_backtest_v1_hype_lead_same_window_handoff_proposal_v1":
        blockers.append("proposal_contract_name_mismatch")
    if proposal.get("decision_label") != EXPECTED_PROPOSAL_DECISION:
        blockers.append("proposal_decision_not_keep_approval_required")
    if proposal.get("decision") != "KEEP":
        blockers.append("proposal_decision_not_keep")
    if as_bool(proposal.get("private_truth_ready")):
        forbidden.append("proposal_claims_private_truth_ready")
    if as_bool(proposal.get("deployable")):
        forbidden.append("proposal_claims_deployable")
    if as_bool(proposal.get("strategy_evidence")):
        forbidden.append("proposal_claims_strategy_evidence")
    if as_bool(proposal.get("no_order_diagnostic_allowed")):
        forbidden.append("proposal_allows_no_order_without_approval")
    if as_dict(proposal.get("promotion_gate")).get("passed") is True:
        forbidden.append("proposal_promotion_gate_passed")
    if as_list(proposal.get("forbidden_blockers")):
        forbidden.append("proposal_forbidden_blockers_present")

    request = as_dict(proposal.get("proposal"))
    if not request:
        blockers.append("proposal_request_missing")
    if request.get("asset") != EXPECTED_ASSET:
        blockers.append("proposal_asset_mismatch")
    if request.get("candidate_key") != EXPECTED_CANDIDATE_KEY:
        blockers.append("proposal_candidate_key_mismatch")
    if request.get("market_prefix") != EXPECTED_PREFIX:
        blockers.append("proposal_market_prefix_mismatch")
    if request.get("approval_required_in_thread") is not True:
        blockers.append("proposal_approval_required_missing")
    if request.get("diagnostic_allowed_now") is not False:
        forbidden.append("proposal_diagnostic_allowed_now")
    labels = as_dict(request.get("train_holdout_label_policy"))
    if labels.get("labels_allowed_in_live_predicate") is not False:
        forbidden.append("proposal_labels_allowed_in_live_predicate")
    if labels.get("realized_pair_cost_allowed_as_live_criteria") is not False:
        forbidden.append("proposal_realized_pair_cost_live_criteria")
    safety = as_dict(request.get("no_order_safety_boundary"))
    for key, value in safety.items():
        if value is True:
            forbidden.append(f"proposal_safety_boundary_{key}_true")
    return blockers, forbidden, request


def review_resolver_source(text: str) -> dict[str, Any]:
    required = [
        "def slug_from_prefix(prefix: str, round_offset: int)",
        "g.add_argument(\"--prefix\")",
        "slug = ns.slug or slug_from_prefix(ns.prefix, ns.round_offset)",
        "def print_market(market: ResolvedMarket, fmt: str)",
        "export POLYMARKET_MARKET_SLUG=",
        "export POLYMARKET_YES_ASSET_ID=",
        "export POLYMARKET_NO_ASSET_ID=",
        "cache_get(cache_path, slug)",
    ]
    ok, missing = has_all(text, required)
    generic_prefix = "return f\"{prefix}-{target}\"" in text and "btc-updown" not in text.split("def slug_from_prefix", 1)[1].split("def parse_market", 1)[0]
    return {
        "generic_prefix_source_supported": generic_prefix,
        "missing_patterns": missing,
        "resolver_source_supported": ok and generic_prefix,
        "required_patterns": required,
    }


def review_runner_source(text: str) -> dict[str, Any]:
    checks = {
        "generic_prefix_cli": [
            "ap.add_argument(\"--prefix\", default=\"btc-updown-5m\")",
            "markets = resolve_markets(Path(args.repo), args.prefix, args.round_offsets)",
        ],
        "same_window_output_flags": [
            "--observable-pre-action-rule-miner-feature-join-output",
            "--observable-pre-action-same-window-offline-label-handoff-output",
            "--observable-pre-action-same-window-offline-label-handoff-output requires --observable-pre-action-rule-miner-feature-join-output",
        ],
        "row_level_exact_ids": [
            "source_seed_candidate_row_id",
            "source_seed_action_id",
            "self.cfg.observable_pre_action_same_window_offline_label_handoff_output",
            "row[\"source_seed_candidate_row_id\"]",
            "row[\"source_seed_action_id\"]",
        ],
        "aggregate_outputs": [
            "def aggregate_observable_pre_action_same_window_label_handoff_outputs",
            "\"observable_pre_action_same_window_offline_label_handoff.json\"",
            "\"observable_pre_action_candidate_rows.jsonl\"",
            "\"observable_pre_action_same_window_offline_labels.csv\"",
        ],
        "safety_contract": [
            "\"events_jsonl_read\": False",
            "\"events_jsonl_pulled\": False",
            "\"raw_replay_or_full_store_scan\": False",
            "\"trading_behavior_changed\": False",
            "\"promotion_gate\": {\"passed\": False",
        ],
    }
    results: dict[str, Any] = {}
    for name, patterns in checks.items():
        ok, missing = has_all(text, patterns)
        results[name] = {"ok": ok, "missing_patterns": missing}
    results["runner_handoff_source_supported"] = all(item["ok"] for item in results.values())
    return results


def review_downstream_sources(sources: dict[str, str]) -> dict[str, Any]:
    patterns = {
        "same_window_contract": [
            "HANDOFF_SCHEMA = \"observable_pre_action_same_window_offline_label_handoff_v1\"",
            "\"source_seed_candidate_row_id\"",
            "labels_allowed_in_live_predicate",
        ],
        "materializer": [
            "HANDOFF_SCHEMA = \"observable_pre_action_same_window_offline_label_handoff_v1\"",
            "\"exact_id:source_seed_candidate_row_id\"",
            "feature_row_",
            "source_seed_candidate_row_id",
        ],
        "arrival_inventory": [
            "source_handoff_id",
            "source_seed_candidate_row_id",
            "duplicate_candidate_exact_id_count",
            "accepted_exact_id_ready_bridge_output_count",
        ],
        "accumulator": [
            "source_handoff_id",
            "duplicate_handoff_id",
            "duplicate_candidate_exact_id",
            "accumulation_source_handoff_id",
        ],
        "training_scorer": [
            "promotion_gate",
            "private_truth_ready",
            "deployable",
        ],
    }
    results: dict[str, Any] = {}
    for name, required in patterns.items():
        ok, missing = has_all(sources.get(name, ""), required)
        results[name] = {"ok": ok, "missing_patterns": missing}
    results["downstream_contracts_supported"] = all(item["ok"] for item in results.values())
    return results


def candidate_filter_mapping(request: dict[str, Any], runner_text: str) -> dict[str, Any]:
    candidate_filter = as_dict(request.get("candidate_filter"))
    size = as_dict(candidate_filter.get("size"))
    offset = as_dict(candidate_filter.get("offset_s"))
    price = as_dict(candidate_filter.get("price"))
    size_support = "--public-trade-size-lo" in runner_text and "--public-trade-size-hi" in runner_text
    immediate_pair_support = "--max-l1-immediate-pair" in runner_text
    side_alignment_support = "--side-alignment" in runner_text
    pnl_cost_source_support = "--pnl-cost-source" in runner_text
    candidate_key_support = "--candidate-filter-manifest" in runner_text or "--backtest-candidate-key" in runner_text
    mappings = [
        {
            "backtest_param": "price_lo/price_hi",
            "candidate_value": f"{price.get('low')}..{price.get('high')}",
            "runner_support": "--seed-px-lo" in runner_text and "--seed-px-hi" in runner_text,
            "runner_surface": "--seed-px-lo/--seed-px-hi",
            "status": "exact" if "--seed-px-lo" in runner_text and "--seed-px-hi" in runner_text else "missing",
        },
        {
            "backtest_param": "offset_lo/offset_hi",
            "candidate_value": f"{offset.get('low')}..{offset.get('high')}",
            "runner_support": "--seed-offset-max-s" in runner_text and "seed_offset_min_s: float = 0.0" in runner_text,
            "runner_surface": "seed_offset_min_s default plus --seed-offset-max-s",
            "status": "exact" if "--seed-offset-max-s" in runner_text and str(offset.get("low")) in {"0", "0.0"} else "partial",
        },
        {
            "backtest_param": "max_l1_pair_ask",
            "candidate_value": candidate_filter.get("max_l1_pair_ask"),
            "runner_support": "--seed-l1-cap" in runner_text,
            "runner_surface": "--seed-l1-cap",
            "status": "exact" if "--seed-l1-cap" in runner_text else "missing",
        },
        {
            "backtest_param": "size_lo/size_hi",
            "candidate_value": f"{size.get('low')}..{size.get('high')}",
            "runner_support": size_support,
            "runner_surface": "--public-trade-size-lo/--public-trade-size-hi" if size_support else "missing exact public trade size range CLI",
            "status": "exact" if size_support else "missing",
        },
        {
            "backtest_param": "max_l1_immediate_pair",
            "candidate_value": candidate_filter.get("max_l1_immediate_pair"),
            "runner_support": immediate_pair_support,
            "runner_surface": "--max-l1-immediate-pair" if immediate_pair_support else "missing exact immediate pair cap CLI",
            "status": "exact" if immediate_pair_support else "missing",
        },
        {
            "backtest_param": "side_alignment",
            "candidate_value": candidate_filter.get("side_alignment"),
            "runner_support": side_alignment_support,
            "runner_surface": "--side-alignment" if side_alignment_support else "missing exact side alignment CLI",
            "status": "exact" if side_alignment_support else "missing",
        },
        {
            "backtest_param": "pnl_cost_source",
            "candidate_value": candidate_filter.get("pnl_cost_source"),
            "runner_support": pnl_cost_source_support,
            "runner_surface": "--pnl-cost-source" if pnl_cost_source_support else "missing explicit pnl cost source CLI",
            "status": "exact" if pnl_cost_source_support else "missing",
        },
        {
            "backtest_param": "candidate_key/filter manifest",
            "candidate_value": request.get("candidate_key"),
            "runner_support": candidate_key_support,
            "runner_surface": "--candidate-filter-manifest/--backtest-candidate-key"
            if candidate_key_support
            else "missing exact backtest V1 candidate filter manifest/key CLI",
            "status": "exact" if candidate_key_support else "missing",
        },
    ]
    missing = [item["backtest_param"] for item in mappings if item["status"] != "exact"]
    exact = [item["backtest_param"] for item in mappings if item["status"] == "exact"]
    return {
        "exact_supported_params": exact,
        "mapping": mappings,
        "missing_or_partial_params": missing,
        "candidate_filter_exact_runtime_supported": not missing,
    }


def future_preflight_commands(prefix: str) -> list[str]:
    return [
        "python3 -m py_compile scripts/resolve_market_ids.py tools/xuan_dplus_passive_passive_shadow_runner.py",
        (
            "python3 scripts/xuan_backtest_v1_hype_lead_prefix_source_preflight_review.py "
            "--proposal-manifest xuan_research_artifacts/xuan_backtest_v1_hype_lead_same_window_handoff_proposal_20260527T011815Z/manifest.json "
            "--output-dir ${preflight_output_dir}"
        ),
        (
            f"python3 scripts/resolve_market_ids.py --prefix {prefix} --round-offset 0 --format env "
            "--cache-path ${market_resolver_cache}"
        ),
        (
            f"python3 scripts/resolve_market_ids.py --prefix {prefix} --round-offset 1 --format env "
            "--cache-path ${market_resolver_cache}"
        ),
        (
            "Only after exact approval and successful preflight: bounded no-order runner with "
            "--event-lite-summary --source-link-transition-event-lite-summary "
            "--observable-pre-action-rule-miner-feature-join-output "
            "--observable-pre-action-same-window-offline-label-handoff-output"
        ),
    ]


def classify(forbidden: list[str], blockers: list[str]) -> tuple[str, str, str]:
    if forbidden:
        return (
            "DISCARD",
            DISCARD_DECISION,
            "Discard this preflight input because it crosses forbidden raw/remote/private/live/promotion boundaries.",
        )
    if blockers:
        return (
            "UNKNOWN",
            UNKNOWN_DECISION,
            "Specify or implement the missing exact candidate-filter runtime adapter before any HYPE diagnostic; do not start no-order automatically.",
        )
    return (
        "KEEP",
        READY_DECISION,
        "Wait for explicit approval of the exact bounded HYPE same-window handoff diagnostic; do not start it automatically.",
    )


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    blockers: list[str] = []
    forbidden: list[str] = []
    ok, reason = path_safe(args.proposal_manifest)
    if not ok:
        forbidden.append(f"proposal_manifest_{reason}")
    if not args.proposal_manifest.exists():
        blockers.append("proposal_manifest_missing")
        proposal_manifest: dict[str, Any] = {}
    else:
        raw = read_json(args.proposal_manifest)
        proposal_manifest = raw if isinstance(raw, dict) else {}
        if not proposal_manifest:
            blockers.append("proposal_manifest_not_json_object")

    proposal_blockers, proposal_forbidden, request = validate_proposal(proposal_manifest)
    blockers.extend(proposal_blockers)
    forbidden.extend(proposal_forbidden)

    runner_text = read_source(args.runner_source, "runner_source", blockers, forbidden)
    resolver_text = read_source(args.resolver_source, "resolver_source", blockers, forbidden)
    downstream_texts = {
        "same_window_contract": read_source(args.contract_source, "same_window_contract_source", blockers, forbidden),
        "materializer": read_source(args.materializer_source, "materializer_source", blockers, forbidden),
        "arrival_inventory": read_source(args.arrival_source, "arrival_inventory_source", blockers, forbidden),
        "accumulator": read_source(args.accumulator_source, "accumulator_source", blockers, forbidden),
        "training_scorer": read_source(args.training_source, "training_source", blockers, forbidden),
    }

    resolver_review = review_resolver_source(resolver_text)
    runner_review = review_runner_source(runner_text)
    downstream_review = review_downstream_sources(downstream_texts)
    mapping = candidate_filter_mapping(request, runner_text)

    if not resolver_review["resolver_source_supported"]:
        blockers.append("resolver_prefix_source_support_missing")
    if not runner_review["runner_handoff_source_supported"]:
        blockers.append("runner_same_window_handoff_source_support_missing")
    if not downstream_review["downstream_contracts_supported"]:
        blockers.append("downstream_contract_source_support_missing")
    if not mapping["candidate_filter_exact_runtime_supported"]:
        blockers.append("candidate_filter_exact_runtime_adapter_absent")

    decision, decision_label, next_action = classify(sorted(set(forbidden)), sorted(set(blockers)))
    prefix = str(request.get("market_prefix") or EXPECTED_PREFIX)
    return {
        "artifact": ARTIFACT,
        "blockers": sorted(set(blockers)),
        "candidate_filter_runtime_mapping": mapping,
        "contract_name": CONTRACT_NAME,
        "created_utc": args.created_utc,
        "decision": decision,
        "decision_label": decision_label,
        "deployable": False,
        "downstream_contract_review": downstream_review,
        "forbidden_blockers": sorted(set(forbidden)),
        "future_preflight_commands_required_before_approved_diagnostic": future_preflight_commands(prefix),
        "inputs": {
            "accumulator_source": str(args.accumulator_source),
            "arrival_inventory_source": str(args.arrival_source),
            "contract_source": str(args.contract_source),
            "materializer_source": str(args.materializer_source),
            "proposal_manifest": str(args.proposal_manifest),
            "resolver_source": str(args.resolver_source),
            "runner_source": str(args.runner_source),
            "training_source": str(args.training_source),
        },
        "lead": {
            "asset": request.get("asset"),
            "candidate_key": request.get("candidate_key"),
            "market_prefix": prefix,
            "proposal_decision_label": proposal_manifest.get("decision_label"),
        },
        "next_executable_action": next_action,
        "no_order_diagnostic_allowed": False,
        "private_truth_ready": False,
        "promotion_gate": {
            "passed": False,
            "reason": "prefix_source_preflight_only_candidate_filter_exact_runtime_gaps",
        },
        "proposal_approval_required_in_thread": request.get("approval_required_in_thread") is True,
        "resolver_review": resolver_review,
        "research_ranking": {
            "candidate_filter_exact_runtime_supported": mapping["candidate_filter_exact_runtime_supported"],
            "downstream_contracts_supported": downstream_review["downstream_contracts_supported"],
            "handoff_proposal_ready": proposal_manifest.get("decision") == "KEEP",
            "prefix_source_preflight_review_complete": True,
            "real_miner_input_ready": False,
            "resolver_prefix_source_supported": resolver_review["resolver_source_supported"],
            "runner_same_window_handoff_source_supported": runner_review["runner_handoff_source_supported"],
            "status": decision_label,
            "strategy_evidence": False,
        },
        "runner_review": runner_review,
        "safety": {
            "aws_or_remote_read": False,
            "broker_modified": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "market_resolver_executed": False,
            "new_no_order_diagnostic_started": False,
            "orders_cancels_redeems_sent": False,
            "raw_replay_or_full_store_scan": False,
            "shared_ingress_modified": False,
            "systemd_or_service_control_used": False,
            "trading_behavior_changed": False,
        },
        "strategy_evidence": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proposal-manifest", type=Path, default=DEFAULT_PROPOSAL_MANIFEST)
    parser.add_argument("--runner-source", type=Path, default=DEFAULT_RUNNER_SOURCE)
    parser.add_argument("--resolver-source", type=Path, default=DEFAULT_RESOLVER_SOURCE)
    parser.add_argument("--contract-source", type=Path, default=DEFAULT_CONTRACT_SOURCE)
    parser.add_argument("--materializer-source", type=Path, default=DEFAULT_MATERIALIZER_SOURCE)
    parser.add_argument("--arrival-source", type=Path, default=DEFAULT_ARRIVAL_SOURCE)
    parser.add_argument("--accumulator-source", type=Path, default=DEFAULT_ACCUMULATOR_SOURCE)
    parser.add_argument("--training-source", type=Path, default=DEFAULT_TRAINING_SOURCE)
    parser.add_argument("--created-utc", default=utc_label())
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("xuan_research_artifacts") / f"{ARTIFACT}_{utc_label()}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest(args)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps({"decision_label": manifest["decision_label"], "output": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
