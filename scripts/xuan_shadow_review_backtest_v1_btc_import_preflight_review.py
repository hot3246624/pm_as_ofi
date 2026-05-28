#!/usr/bin/env python3
"""Review refreshed Backtest V1 BTC outputs against the import preflight stub.

This is a local-only reviewer.  It checks whether colleague-generated BTC/V1
artifacts now satisfy the disabled import-contract stub.  It does not create an
import table, runner profile, manifest, preauthorization, remote run, deploy, or
live-order path.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any


STAMP = "20260528T1133Z"

POLY_BT_ROOT = Path("/Users/hot/web3Scientist/poly_backtest_data")
CONTRACT_ROOT = POLY_BT_ROOT / "derived/contract_examples"

DEFAULT_IMPORT_STUB = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_btc_import_contract_stub_20260528T0934Z.json"
)
DEFAULT_BTC_CANARY = Path(
    ".tmp_xuan/scorecards/"
    "xuan_shadow_review_backtest_v1_btc_canary_readiness_gate_20260528T0922Z.json"
)
DEFAULT_BTC_PARITY = CONTRACT_ROOT / "backtest_v1_btc_parity_latest/BACKTEST_V1_BTC_PARITY_GATE.json"
DEFAULT_STRATEGY_READINESS = (
    CONTRACT_ROOT / "xuan_backtest_v1_strategy_readiness_latest/XUAN_BACKTEST_V1_STRATEGY_READINESS_GATE.json"
)
DEFAULT_REFRESH_SUMMARY = (
    CONTRACT_ROOT / "xuan_backtest_v1_refresh_latest/XUAN_BACKTEST_V1_RESEARCH_REFRESH_SUMMARY.json"
)
DEFAULT_CAPITAL_LEDGER = CONTRACT_ROOT / "xuan_capital_ledger_latest/XUAN_CAPITAL_LEDGER_REPORT.json"
DEFAULT_COMPLETION_RESCORE = (
    CONTRACT_ROOT / "xuan_completion_candidate_rescore_latest/XUAN_COMPLETION_CANDIDATE_RESCORE_MANIFEST.json"
)
DEFAULT_XUAN_BRIDGE = CONTRACT_ROOT / "xuan_bridge_scorecard_latest/XUAN_BRIDGE_SCORECARD_MANIFEST.json"
DEFAULT_SCORECARD = Path(
    f".tmp_xuan/scorecards/xuan_shadow_review_backtest_v1_btc_import_preflight_review_{STAMP}.json"
)
DEFAULT_ARTIFACT_DIR = Path(
    ".tmp_xuan/local_verifier_artifacts/"
    f"xuan_shadow_review_backtest_v1_btc_import_preflight_review_{STAMP}"
)

REQUIRED_IMPORT_FIELDS = [
    "filter_name",
    "filter_version",
    "source_dataset_fingerprint",
    "source_semantics_contract_id",
    "l2_top_overlay_contract_id",
    "rescore_manifest_fingerprint",
    "candidate_csv_sha256",
    "deterministic_candidate_rank",
    "runner_profile_id",
    "max_notional_cap",
    "dry_run_only",
    "import_enabled",
]


def fnum(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def clean(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {key: clean(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean(item) for item in value]
    return value


def load_json(path: Path) -> dict[str, Any]:
    with path.expanduser().resolve().open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} did not contain a JSON object")
    return raw


def maybe_json(path: Path) -> dict[str, Any]:
    return load_json(path) if path.exists() else {}


def body(card: dict[str, Any], key: str) -> dict[str, Any]:
    value = card.get(key)
    return value if isinstance(value, dict) else {}


def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def path_from_outputs(card: dict[str, Any], key: str) -> Path:
    value = body(card, "outputs").get(key)
    return Path(str(value)) if value else Path("")


def has_top_level_id(card: dict[str, Any], key: str) -> bool:
    value = card.get(key)
    return bool(value and str(value).strip())


def capital_ledger_is_filter_specific(report: dict[str, Any], by_asset_day_csv: Path) -> bool:
    header = read_csv_header(by_asset_day_csv)
    summary = body(report, "summary")
    return (
        report.get("filter_name") == "low_residual_core_pair_v1"
        and report.get("filter_version")
        and "filter_name" in header
        and "filter_version" in header
        and summary.get("asset_scope") == "BTC"
    )


def import_table_status(rescore: dict[str, Any]) -> dict[str, Any]:
    top_csv = path_from_outputs(rescore, "top_csv")
    header = read_csv_header(top_csv)
    missing = [field for field in REQUIRED_IMPORT_FIELDS if field not in header]
    btc_rows = 0
    if top_csv.exists():
        with top_csv.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("asset") == "BTC":
                    btc_rows += 1
    return {
        "top_csv": str(top_csv) if str(top_csv) else None,
        "top_csv_exists": top_csv.exists(),
        "top_csv_sha256": sha256(top_csv),
        "top_csv_row_count": count_csv_rows(top_csv),
        "btc_row_count": btc_rows,
        "required_import_fields": REQUIRED_IMPORT_FIELDS,
        "missing_import_fields": missing,
        "import_fields_complete": not missing,
    }


def build_preflight(
    import_stub: dict[str, Any],
    btc_canary: dict[str, Any],
    btc_parity: dict[str, Any],
    strategy: dict[str, Any],
    refresh: dict[str, Any],
    capital: dict[str, Any],
    rescore: dict[str, Any],
    bridge: dict[str, Any],
) -> list[dict[str, Any]]:
    stub_inputs = body(import_stub, "inputs")
    candidate_csv = Path(str(stub_inputs.get("btc_candidates_csv") or ""))
    expected_sha = stub_inputs.get("btc_candidates_csv_sha256")
    actual_sha = sha256(candidate_csv)
    capital_csv = path_from_outputs(capital, "by_asset_day_csv")
    import_status = import_table_status(rescore)
    return [
        {
            "gate": "btc_candidate_csv_fingerprint_fixed",
            "status": "PASS" if actual_sha and actual_sha == expected_sha else "BLOCKED",
            "evidence": {"expected": expected_sha, "actual": actual_sha},
        },
        {
            "gate": "btc_research_edge_ready",
            "status": "PASS" if body(btc_canary, "decision").get("btc_research_edge_ready") else "BLOCKED",
            "evidence": body(btc_canary, "btc_filter_summary"),
        },
        {
            "gate": "strategy_refresh_research_ready",
            "status": "PASS"
            if refresh.get("status") == "OK_XUAN_BACKTEST_V1_RESEARCH_REFRESH"
            and body(refresh, "summary").get("strategy_research_ready")
            else "BLOCKED",
            "evidence": body(refresh, "summary"),
        },
        {
            "gate": "btc_source_parity_or_canonical_source_semantics",
            "status": "BLOCKED",
            "evidence": {
                "btc_parity_status": btc_parity.get("status"),
                "blockers": btc_parity.get("blockers") or [],
                "source_semantics_contract_id_present": has_top_level_id(btc_parity, "source_semantics_contract_id"),
                "source_dataset_fingerprint_present": has_top_level_id(btc_parity, "source_dataset_fingerprint"),
            },
        },
        {
            "gate": "source_semantics_contract_id_present",
            "status": "PASS" if has_top_level_id(btc_parity, "source_semantics_contract_id") else "BLOCKED",
            "evidence": btc_parity.get("source_semantics_contract_id"),
        },
        {
            "gate": "source_dataset_fingerprint_present",
            "status": "PASS" if has_top_level_id(btc_parity, "source_dataset_fingerprint") else "BLOCKED",
            "evidence": btc_parity.get("source_dataset_fingerprint"),
        },
        {
            "gate": "l2_top_overlay_contract_id_present",
            "status": "PASS" if has_top_level_id(btc_parity, "l2_top_overlay_contract_id") else "BLOCKED",
            "evidence": btc_parity.get("l2_top_overlay_contract_id"),
        },
        {
            "gate": "filter_specific_btc_capital_ledger_present",
            "status": "PASS" if capital_ledger_is_filter_specific(capital, capital_csv) else "BLOCKED",
            "evidence": {
                "capital_status": capital.get("status"),
                "capital_summary": body(capital, "summary"),
                "by_asset_day_csv": str(capital_csv) if str(capital_csv) else None,
                "by_asset_day_header": read_csv_header(capital_csv),
                "filter_name": capital.get("filter_name"),
                "filter_version": capital.get("filter_version"),
            },
        },
        {
            "gate": "import_contract_table_fields_complete",
            "status": "PASS" if import_status["import_fields_complete"] else "BLOCKED",
            "evidence": import_status,
        },
        {
            "gate": "deterministic_candidate_rank_defined",
            "status": "PASS"
            if "deterministic_candidate_rank" not in import_status["missing_import_fields"]
            else "BLOCKED",
            "evidence": import_status,
        },
        {
            "gate": "runner_profile_id_defined",
            "status": "PASS" if "runner_profile_id" not in import_status["missing_import_fields"] else "BLOCKED",
            "evidence": import_status,
        },
        {
            "gate": "max_notional_cap_defined_from_filter_capital_ledger",
            "status": "PASS" if "max_notional_cap" not in import_status["missing_import_fields"] else "BLOCKED",
            "evidence": import_status,
        },
        {
            "gate": "dry_run_only_true_and_import_enabled_false",
            "status": "PASS"
            if "dry_run_only" not in import_status["missing_import_fields"]
            and "import_enabled" not in import_status["missing_import_fields"]
            else "BLOCKED",
            "evidence": import_status,
        },
        {
            "gate": "owner_private_truth_schema_ready",
            "status": "BLOCKED",
            "evidence": {
                "strategy_private_truth_ready": body(strategy, "summary").get("private_truth_ready")
                or body(strategy, "decision").get("private_truth_ready"),
                "bridge_private_truth_ready": body(bridge, "summary").get("private_truth_ready"),
                "private_promotion_ready_count": body(bridge, "summary").get("private_promotion_ready_count"),
            },
        },
        {
            "gate": "runner_import_preflight_ready",
            "status": "BLOCKED",
            "evidence": "No runner/import preflight artifact, active-runner conflict check, manifest, or preauthorization exists.",
        },
    ]


def build_card(args: argparse.Namespace) -> dict[str, Any]:
    import_stub = load_json(args.import_stub)
    btc_canary = load_json(args.btc_canary)
    btc_parity = maybe_json(args.btc_parity)
    strategy = maybe_json(args.strategy_readiness)
    refresh = maybe_json(args.refresh_summary)
    capital = maybe_json(args.capital_ledger)
    rescore = maybe_json(args.completion_rescore)
    bridge = maybe_json(args.xuan_bridge)
    preflight = build_preflight(
        import_stub, btc_canary, btc_parity, strategy, refresh, capital, rescore, bridge
    )
    hard_blockers = [row["gate"] for row in preflight if row.get("status") == "BLOCKED"]
    new_useful_outputs = [
        {
            "artifact": "backtest_v1_btc_parity_latest",
            "status": btc_parity.get("status"),
            "useful": True,
            "clears_import_preflight": False,
        },
        {
            "artifact": "xuan_capital_ledger_latest",
            "status": capital.get("status"),
            "useful": True,
            "clears_import_preflight": False,
            "reason": "global/all-candidate ledger; not BTC low_residual_core_pair_v1 filter-specific",
        },
        {
            "artifact": "xuan_completion_candidate_rescore_latest",
            "status": rescore.get("status"),
            "useful": True,
            "clears_import_preflight": False,
            "reason": "research rescore; lacks required import metadata fields",
        },
        {
            "artifact": "xuan_bridge_scorecard_latest",
            "status": bridge.get("status"),
            "useful": True,
            "clears_import_preflight": False,
            "reason": "partial research bridge; private truth remains false",
        },
    ]
    return clean(
        {
            "artifact": "xuan_shadow_review_backtest_v1_btc_import_preflight_review",
            "status": "BLOCKED_SHADOW_REVIEW_BACKTEST_V1_BTC_IMPORT_PREFLIGHT_NOT_READY_LOCAL_ONLY",
            "created_utc": STAMP,
            "script": "scripts/xuan_shadow_review_backtest_v1_btc_import_preflight_review.py",
            "inputs": {
                "import_stub": str(args.import_stub),
                "btc_canary": str(args.btc_canary),
                "btc_parity": str(args.btc_parity),
                "strategy_readiness": str(args.strategy_readiness),
                "refresh_summary": str(args.refresh_summary),
                "capital_ledger": str(args.capital_ledger),
                "completion_rescore": str(args.completion_rescore),
                "xuan_bridge": str(args.xuan_bridge),
            },
            "outputs": {
                "artifact_dir": str(args.artifact_dir),
                "markdown": str(args.artifact_dir / "BTC_IMPORT_PREFLIGHT_REVIEW.md"),
            },
            "decision": {
                "new_colleague_outputs_reviewed": True,
                "btc_research_edge_ready": body(btc_canary, "decision").get("btc_research_edge_ready"),
                "strategy_research_ready": body(refresh, "summary").get("strategy_research_ready"),
                "btc_import_preflight_ready": False,
                "candidate_import_allowed": False,
                "runner_support_ready": False,
                "manifest_or_preauthorization_ready": False,
                "future_remote_allowed_by_this_review": False,
                "remote_runner_allowed": False,
                "deployable": False,
                "live_orders_allowed": False,
                "private_truth_ready": False,
                "next_lane": "backtest_colleague_fill_remaining_import_contract_preflight_fields",
            },
            "new_colleague_outputs": new_useful_outputs,
            "preflight_review": preflight,
            "hard_blockers_before_any_import": hard_blockers,
            "status_summary": {
                "btc_parity_status": btc_parity.get("status"),
                "strategy_readiness_status": strategy.get("status"),
                "refresh_status": refresh.get("status"),
                "capital_ledger_status": capital.get("status"),
                "completion_rescore_status": rescore.get("status"),
                "xuan_bridge_status": bridge.get("status"),
            },
            "warnings": [
                "Refreshed research artifacts are useful but not import authorization.",
                "Global capital ledger cannot size BTC low_residual_core_pair_v1 canary.",
                "Research rescore top CSV lacks import metadata fields.",
                "No owner private truth exists in historical V1/L2 outputs.",
            ],
        }
    )


def render_markdown(card: dict[str, Any]) -> str:
    decision = body(card, "decision")
    status = body(card, "status_summary")
    lines = [
        "# BTC Import Preflight Review",
        "",
        f"- status: `{card.get('status')}`",
        f"- new_colleague_outputs_reviewed: `{decision.get('new_colleague_outputs_reviewed')}`",
        f"- btc_import_preflight_ready: `{decision.get('btc_import_preflight_ready')}`",
        f"- candidate_import_allowed: `{decision.get('candidate_import_allowed')}`",
        f"- live_orders_allowed: `{decision.get('live_orders_allowed')}`",
        "",
        "## Source Statuses",
        "",
    ]
    for key, value in status.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Preflight Gates",
            "",
            "| gate | status |",
            "| --- | --- |",
        ]
    )
    for row in card.get("preflight_review", []):
        lines.append(f"| `{row.get('gate')}` | `{row.get('status')}` |")
    lines.extend(["", "## Hard Blockers", ""])
    for blocker in card.get("hard_blockers_before_any_import", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The refresh is materially useful for research: strategy readiness, BTC parity, capital ledger, rescore, and bridge were regenerated.",
            "- It still does not clear BTC import/canary preflight because the outputs are not filter-specific/import-contract/private-truth complete.",
            "- No candidate import, runner profile, manifest, preauthorization, remote, deploy, or live order is authorized.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--import-stub", type=Path, default=DEFAULT_IMPORT_STUB)
    parser.add_argument("--btc-canary", type=Path, default=DEFAULT_BTC_CANARY)
    parser.add_argument("--btc-parity", type=Path, default=DEFAULT_BTC_PARITY)
    parser.add_argument("--strategy-readiness", type=Path, default=DEFAULT_STRATEGY_READINESS)
    parser.add_argument("--refresh-summary", type=Path, default=DEFAULT_REFRESH_SUMMARY)
    parser.add_argument("--capital-ledger", type=Path, default=DEFAULT_CAPITAL_LEDGER)
    parser.add_argument("--completion-rescore", type=Path, default=DEFAULT_COMPLETION_RESCORE)
    parser.add_argument("--xuan-bridge", type=Path, default=DEFAULT_XUAN_BRIDGE)
    parser.add_argument("--scorecard", type=Path, default=DEFAULT_SCORECARD)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    card = build_card(args)
    args.scorecard.parent.mkdir(parents=True, exist_ok=True)
    args.scorecard.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = args.artifact_dir / "BTC_IMPORT_PREFLIGHT_REVIEW.md"
    markdown.write_text(render_markdown(card) + "\n", encoding="utf-8")
    print(json.dumps({"status": card.get("status"), "scorecard": str(args.scorecard)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
