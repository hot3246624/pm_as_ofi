#!/usr/bin/env python3
"""Review whether a safe in-worktree ce25 72h validation source exists.

This local-only readiness check does not fetch data, enter external worktrees,
start services, use SSH, scan replay/raw/full stores, or change trading
behavior. It only reads committed/current-worktree artifacts and optionally a
candidate source manifest supplied by the caller.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_ce25_three_day_input_source_readiness"
CONTRACT_NAME = "ce25_three_day_input_source_readiness_v1"
THREE_DAY_CONTRACT = "ce25_three_day_pair_cost_validation_v1"
DEFAULT_THREE_DAY_SPEC = Path(
    "xuan_research_artifacts/"
    "xuan_ce25_three_day_pair_cost_validation_spec_20260524T122500Z/"
    "manifest.json"
)
DEFAULT_EXPORTER_FIXTURE = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_public_activity_entry_exporter_fixture_20260524T075245Z/"
    "manifest.json"
)
DEFAULT_NON_FIXTURE_REVIEW = Path(
    "xuan_research_artifacts/"
    "xuan_b55_ce25_non_fixture_input_source_review_20260524T090916Z/"
    "manifest.json"
)
DEFAULT_CE25_PUBLIC_INPUT = Path(
    "xuan_research_artifacts/"
    "xuan_public_leaderboard_candidate_fast_screen_20260524T035500Z/"
    "public_inputs/0xce25e214d5cfe4f459cf67f08df581885aae7fdc/"
    "activity_trade_rows.json"
)
REQUIRED_DAILY_FIELDS = (
    "day_id",
    "account",
    "cohort",
    "markets",
    "buy_actual",
    "cohort_pnl_ex_rebate",
    "cohort_pnl_including_rebate",
    "pair_pnl",
    "residual_pnl_est",
    "wins",
    "losses",
    "avg_pair_cost",
    "residual_rate",
    "old_redeem_contamination",
    "post_window_same_condition_buy",
    "rebate",
)
REQUIRED_COHORTS = (
    "starter_eth_sol_5m15m",
    "core_btc_eth_sol_5m15m",
    "hard_loss_pair_cost_ge_1",
)
FORBIDDEN_PATH_FRAGMENTS = (
    "/mnt/poly-replay",
    "replay_published",
    ".events.jsonl",
    "/raw/",
    "raw/",
    "/collector/",
    "collector/raw",
    "shared-ingress",
    "/broker/",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_is_forbidden(path: Path) -> bool:
    text = str(path)
    resolved = str(path.resolve(strict=False))
    return any(fragment in text or fragment in resolved for fragment in FORBIDDEN_PATH_FRAGMENTS)


def path_in_worktree(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def path_safe_in_worktree(path: Path, root: Path) -> tuple[bool, str | None]:
    if path_is_forbidden(path):
        return False, "forbidden_path_fragment"
    if not path_in_worktree(path, root):
        return False, "outside_current_worktree"
    return True, None


def safe_load(path: Path, root: Path) -> tuple[dict[str, Any] | None, str | None]:
    ok, reason = path_safe_in_worktree(path, root)
    if not ok:
        return None, reason
    try:
        obj = load_json(path)
    except FileNotFoundError:
        return None, "missing"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def public_input_observation(path: Path, root: Path) -> dict[str, Any]:
    ok, reason = path_safe_in_worktree(path, root)
    if not ok:
        return {
            "path": str(path),
            "exists": False,
            "safe": False,
            "error": reason,
            "row_count": 0,
            "distinct_utc_days": [],
            "has_public_activity_rows": False,
            "has_daily_cohort_manifest": False,
        }
    try:
        rows = load_json(path)
    except FileNotFoundError:
        return {
            "path": str(path),
            "exists": False,
            "safe": True,
            "row_count": 0,
            "distinct_utc_days": [],
            "has_public_activity_rows": False,
            "has_daily_cohort_manifest": False,
        }
    if not isinstance(rows, list):
        return {
            "path": str(path),
            "exists": True,
            "safe": True,
            "row_count": 0,
            "distinct_utc_days": [],
            "has_public_activity_rows": False,
            "has_daily_cohort_manifest": False,
            "error": "not_json_list",
        }
    timestamps: list[int] = []
    keys: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        keys.update(str(key) for key in row)
        ts = row.get("timestamp")
        try:
            timestamps.append(int(ts))
        except (TypeError, ValueError):
            continue
    days = sorted(
        {
            datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            for ts in timestamps
            if ts > 0
        }
    )
    return {
        "path": str(path),
        "exists": True,
        "safe": True,
        "row_count": len(rows),
        "keys": sorted(keys),
        "timestamp_min": min(timestamps) if timestamps else None,
        "timestamp_max": max(timestamps) if timestamps else None,
        "distinct_utc_days": days,
        "has_public_activity_rows": all(
            field in keys for field in ("conditionId", "slug", "timestamp", "price", "size", "usdcSize", "side", "type")
        ),
        "has_daily_cohort_manifest": False,
        "interpretation": (
            "Public activity rows are useful raw material, but they are not a 72h daily cohort validation manifest."
        ),
    }


def summarize_three_day_spec(spec: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    if error or spec is None:
        return {"exists": False, "error": error, "contract_ready": False}
    score = spec.get("candidate_three_day_score") if isinstance(spec.get("candidate_three_day_score"), dict) else {}
    return {
        "exists": True,
        "decision_label": spec.get("decision_label"),
        "contract_name": ((spec.get("validation_contract") or {}).get("contract_name")),
        "contract_ready": ((spec.get("validation_contract") or {}).get("contract_name")) == THREE_DAY_CONTRACT,
        "current_real_input_status": score.get("evidence_status"),
        "current_candidate_blockers": score.get("blockers") or [],
        "validation_command_template": (
            "python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py "
            "--candidate-three-day-manifest <manifest.json> "
            "--output-dir xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_score_<UTC>"
        ),
    }


def summarize_exporter_fixture(exporter: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    if error or exporter is None:
        return {"exists": False, "error": error}
    conversion = exporter.get("conversion_summary") if isinstance(exporter.get("conversion_summary"), dict) else {}
    ce25 = conversion.get("ce25") if isinstance(conversion.get("ce25"), dict) else {}
    real_export = exporter.get("real_export") if isinstance(exporter.get("real_export"), dict) else {}
    fixture_export = exporter.get("fixture_export") if isinstance(exporter.get("fixture_export"), dict) else {}
    return {
        "exists": True,
        "decision_label": exporter.get("decision_label"),
        "ce25_raw_public_rows": ce25.get("raw_count"),
        "ce25_entry_rows_old_contract": ce25.get("accepted_count"),
        "real_export_rows": real_export.get("row_count"),
        "real_export_status": real_export.get("status"),
        "real_export_missing_fields": real_export.get("missing_fields") or [],
        "fixture_export_status": fixture_export.get("status"),
        "interpretation": (
            "Existing normalized real rows are a small public-entry sample, not the required three-day daily cohort source."
        ),
    }


def summarize_non_fixture_review(review: dict[str, Any] | None, error: str | None) -> dict[str, Any]:
    if error or review is None:
        return {"exists": False, "error": error}
    status = review.get("candidate_input_source_status") if isinstance(review.get("candidate_input_source_status"), dict) else {}
    ranking = review.get("research_ranking") if isinstance(review.get("research_ranking"), dict) else {}
    return {
        "exists": True,
        "decision_label": review.get("decision_label"),
        "real_non_fixture_source_ready": ranking.get("real_non_fixture_source_ready"),
        "candidate_input_source_status": status.get("status"),
        "exact_missing_inputs": ranking.get("exact_missing_inputs") or status.get("missing_fields") or [],
        "stop_before_no_order_diagnostic": ranking.get("stop_before_no_order_diagnostic"),
    }


def daily_manifest_shape(path: Path, root: Path) -> dict[str, Any]:
    obj, error = safe_load(path, root)
    if error or obj is None:
        return {
            "path": str(path),
            "exists": False,
            "read_error": error,
            "shape_ready": False,
            "day_count": 0,
            "blockers": ["candidate_three_day_manifest_unreadable"],
        }
    rows = obj.get("daily_cohort_rows")
    if not isinstance(rows, list):
        return {
            "path": str(path),
            "exists": True,
            "shape_ready": False,
            "day_count": 0,
            "blockers": ["daily_cohort_rows_missing"],
        }
    blockers: list[str] = []
    days = sorted({str(row.get("day_id")) for row in rows if isinstance(row, dict) and row.get("day_id") is not None})
    cohorts_by_day = {
        (str(row.get("day_id")), str(row.get("cohort")))
        for row in rows
        if isinstance(row, dict) and row.get("day_id") is not None
    }
    if len(days) < 3:
        blockers.append("fewer_than_3_days")
    for day in days:
        for cohort in REQUIRED_COHORTS:
            if (day, cohort) not in cohorts_by_day:
                blockers.append(f"missing_{cohort}_for_{day}")
    missing_fields = sorted(
        {
            field
            for row in rows
            if isinstance(row, dict)
            for field in REQUIRED_DAILY_FIELDS
            if field not in row
        }
    )
    if missing_fields:
        blockers.append("required_daily_fields_missing")
    return {
        "path": str(path),
        "exists": True,
        "schema_version": obj.get("schema_version"),
        "fixture": bool(obj.get("fixture")),
        "row_count": len(rows),
        "days": days,
        "day_count": len(days),
        "missing_fields": missing_fields,
        "shape_ready": not blockers,
        "blockers": sorted(set(blockers)),
    }


def candidate_source_status(path: Path | None, root: Path) -> dict[str, Any]:
    if path is None:
        return {
            "status": "MISSING_CE25_THREE_DAY_INPUT_SOURCE_MANIFEST",
            "ready": False,
            "path": None,
            "blockers": ["candidate_source_manifest_absent"],
        }
    ok, reason = path_safe_in_worktree(path, root)
    if not ok:
        return {
            "status": "UNSAFE_OR_EXTERNAL_CE25_THREE_DAY_INPUT_SOURCE_MANIFEST",
            "ready": False,
            "path": str(path),
            "blockers": [reason or "unsafe_candidate_source_manifest_path"],
        }
    try:
        manifest = load_json(path)
    except Exception as exc:
        return {
            "status": "CANDIDATE_CE25_THREE_DAY_INPUT_SOURCE_READ_FAILED",
            "ready": False,
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "blockers": ["candidate_source_manifest_read_failed"],
        }
    if not isinstance(manifest, dict):
        return {
            "status": "CANDIDATE_CE25_THREE_DAY_INPUT_SOURCE_NOT_OBJECT",
            "ready": False,
            "path": str(path),
            "blockers": ["candidate_source_manifest_not_json_object"],
        }
    blockers: list[str] = []
    if bool(manifest.get("fixture")):
        blockers.append("candidate_source_is_fixture")
    if not bool(manifest.get("safe_offline_72h_export")):
        blockers.append("safe_offline_72h_export_not_declared")
    if str(manifest.get("account")) != "ce25":
        blockers.append("account_not_ce25")
    if str(manifest.get("contract_name")) != CONTRACT_NAME:
        blockers.append("contract_name_mismatch")
    forbidden_flags = {
        "fetches_new_data": "fetches_new_data",
        "enters_external_worktree": "enters_external_worktree",
        "starts_service": "starts_service",
        "uses_local_agg_service": "uses_local_agg_service",
        "uses_shared_ws": "uses_shared_ws",
        "uses_ssh": "uses_ssh",
        "starts_shadow": "starts_shadow",
        "starts_canary_or_live": "starts_canary_or_live",
        "reads_raw_replay_or_full_store": "reads_raw_replay_or_full_store",
        "changes_trading_behavior": "changes_trading_behavior",
    }
    for key, blocker in forbidden_flags.items():
        if bool(manifest.get(key)):
            blockers.append(blocker)
    source_paths = [Path(str(item)) for item in manifest.get("source_paths") or []]
    if not source_paths:
        blockers.append("source_paths_missing")
    for source_path in source_paths:
        ok_source, reason_source = path_safe_in_worktree(source_path, root)
        if not ok_source:
            blockers.append(f"unsafe_source_path_{reason_source}")
            break
    candidate_three_day = manifest.get("candidate_three_day_manifest_path")
    if not candidate_three_day:
        blockers.append("candidate_three_day_manifest_path_missing")
        daily_shape = {
            "exists": False,
            "shape_ready": False,
            "blockers": ["candidate_three_day_manifest_path_missing"],
        }
    else:
        daily_shape = daily_manifest_shape(Path(str(candidate_three_day)), root)
        if not daily_shape.get("shape_ready"):
            blockers.append("candidate_three_day_manifest_shape_not_ready")
        if bool(daily_shape.get("fixture")):
            blockers.append("candidate_three_day_manifest_is_fixture")
    validation_command = str(manifest.get("validation_command") or "")
    if "scripts/xuan_ce25_three_day_pair_cost_validation_spec.py" not in validation_command:
        blockers.append("validation_command_missing_three_day_scorer")
    if "--candidate-three-day-manifest" not in validation_command:
        blockers.append("validation_command_missing_candidate_arg")
    ready = not blockers
    return {
        "status": "CE25_THREE_DAY_INPUT_SOURCE_READY" if ready else "CE25_THREE_DAY_INPUT_SOURCE_FAIL_CLOSED",
        "ready": ready,
        "path": str(path),
        "blockers": sorted(set(blockers)),
        "source_paths": [str(item) for item in source_paths],
        "candidate_three_day_manifest_shape": daily_shape,
        "validation_command": validation_command,
        "candidate_summary": {
            "artifact": manifest.get("artifact"),
            "created_utc": manifest.get("created_utc"),
            "decision_label": manifest.get("decision_label"),
        },
    }


def source_contract() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "account": "ce25",
        "purpose": "Provide a safe current-worktree source for the 72h ce25 pair-cost validation contract.",
        "required_source_properties": [
            "safe_offline_72h_export=true",
            "source_paths are inside /Users/hot/web3Scientist/pm_as_ofi-xuan-research",
            "no new fetch, external worktree read, service/local-agg/shared-WS start, SSH, shadow, canary, live, or raw/replay/full-store scan",
            "candidate_three_day_manifest_path points to a JSON manifest with daily_cohort_rows",
            "daily_cohort_rows cover at least three day_id values and all required ce25 cohorts",
            "validation_command invokes scripts/xuan_ce25_three_day_pair_cost_validation_spec.py with --candidate-three-day-manifest",
        ],
        "required_daily_fields": list(REQUIRED_DAILY_FIELDS),
        "required_cohorts": list(REQUIRED_COHORTS),
        "rejected_substitutes": [
            "single-day public proxy buckets",
            "price cap or static asset/timeframe deletion",
            "realized pair_cost as a live criterion",
            "D+ micro-deficit/ledger/tiny-deficit/closed-cycle families",
            "fixture or synthetic daily cohort rows as strategy evidence",
            "external poly_trans_research path read from this worktree",
        ],
    }


def build_manifest(args: argparse.Namespace, root: Path, output_dir: Path) -> dict[str, Any]:
    ok_output, output_reason = path_safe_in_worktree(output_dir, root)
    if not ok_output:
        raise RuntimeError(f"unsafe output dir: {output_reason}: {output_dir}")
    spec, spec_error = safe_load(args.three_day_spec_manifest, root)
    exporter, exporter_error = safe_load(args.exporter_fixture_manifest, root)
    non_fixture, non_fixture_error = safe_load(args.non_fixture_review_manifest, root)
    public_input = public_input_observation(args.ce25_public_input, root)
    candidate = candidate_source_status(args.candidate_source_manifest, root)
    candidate_ready = bool(candidate.get("ready"))
    label = (
        "KEEP_CE25_THREE_DAY_INPUT_SOURCE_READY"
        if candidate_ready
        else "UNKNOWN_CE25_THREE_DAY_INPUT_SOURCE_NOT_IN_CURRENT_WORKTREE"
    )
    decision = "KEEP" if candidate_ready else "UNKNOWN"
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_three_day_input_source_readiness",
        "decision": decision,
        "decision_label": label,
        "scope": {
            "local_only": True,
            "current_worktree_only": True,
            "new_data_fetched": False,
            "external_worktree_read": False,
            "ssh_used": False,
            "shadow_started": False,
            "canary_or_live_started": False,
            "events_jsonl_read": False,
            "raw_replay_or_full_store_scanned": False,
            "shared_ws_or_local_agg_or_service_started": False,
            "shared_ingress_or_broker_or_live_modified": False,
            "trading_behavior_changed": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "three_day_spec_manifest": str(args.three_day_spec_manifest),
            "exporter_fixture_manifest": str(args.exporter_fixture_manifest),
            "non_fixture_review_manifest": str(args.non_fixture_review_manifest),
            "ce25_public_input": str(args.ce25_public_input),
            "candidate_source_manifest": str(args.candidate_source_manifest) if args.candidate_source_manifest else None,
        },
        "current_worktree_source_observation": {
            "three_day_validation_spec": summarize_three_day_spec(spec, spec_error),
            "public_activity_entry_exporter": summarize_exporter_fixture(exporter, exporter_error),
            "non_fixture_input_review": summarize_non_fixture_review(non_fixture, non_fixture_error),
            "ce25_public_input": public_input,
            "interpretation": (
                "The current worktree contains a single-day ce25 public proxy and small normalized entry samples, "
                "but no non-fixture 72h daily cohort source manifest that can satisfy the validation contract."
            ),
        },
        "candidate_source_status": candidate,
        "source_contract": source_contract(),
        "validation_command_template": (
            "python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py "
            "--candidate-three-day-manifest <manifest.json> "
            "--output-dir xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_score_<UTC>"
        ),
        "research_ranking": {
            "status": label,
            "ce25_line_status": "INPUT_BLOCKED_UNTIL_SAFE_72H_MANIFEST" if not candidate_ready else "INPUT_SOURCE_READY_FOR_VALIDATION",
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
            "exact_blockers": candidate.get("blockers") or [],
            "safe_current_worktree_source_ready": candidate_ready,
            "three_day_validation_contract_ready": summarize_three_day_spec(spec, spec_error).get("contract_ready", False),
            "single_day_public_proxy_available": bool(public_input.get("has_public_activity_rows")),
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "status": "SOURCE_READINESS_ONLY_NOT_STRATEGY_OR_PROMOTION_EVIDENCE",
            "blockers": [
                "no private truth",
                "no no-order diagnostic",
                "no deployable runner behavior",
                "safe 72h public proxy source missing" if not candidate_ready else "72h source still requires validation scorer",
            ],
        },
        "next_executable_action": (
            "Provide or create inside this worktree a safe offline ce25 72h daily-cohort manifest matching "
            "ce25_three_day_pair_cost_validation_v1, then run the validation command; otherwise mark ce25 "
            "mimicability input-blocked and pivot to another lead with in-worktree multi-day evidence."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--three-day-spec-manifest", type=Path, default=DEFAULT_THREE_DAY_SPEC)
    parser.add_argument("--exporter-fixture-manifest", type=Path, default=DEFAULT_EXPORTER_FIXTURE)
    parser.add_argument("--non-fixture-review-manifest", type=Path, default=DEFAULT_NON_FIXTURE_REVIEW)
    parser.add_argument("--ce25-public-input", type=Path, default=DEFAULT_CE25_PUBLIC_INPUT)
    parser.add_argument("--candidate-source-manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir or root / "xuan_research_artifacts" / f"{ARTIFACT}_{utc_label()}"
    manifest = build_manifest(args, root, output_dir)
    write_json(output_dir / "manifest.json", manifest)
    print(
        json.dumps(
            {
                "decision_label": manifest["decision_label"],
                "manifest": str(output_dir / "manifest.json"),
                "safe_current_worktree_source_ready": manifest["research_ranking"][
                    "safe_current_worktree_source_ready"
                ],
                "candidate_source_status": manifest["candidate_source_status"]["status"],
                "exact_blockers": manifest["research_ranking"]["exact_blockers"],
                "next_executable_action": manifest["next_executable_action"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
