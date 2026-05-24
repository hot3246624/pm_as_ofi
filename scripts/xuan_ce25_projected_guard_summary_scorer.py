#!/usr/bin/env python3
"""Score ce25 projected guard runner summaries.

This local-only scorer reads committed summary/aggregate artifacts produced by
the default-off runner instrumentation smoke. It validates that projected guard
diagnostics remain summary-only, aggregate cleanly, and fail closed when the
schema or safety contract is absent or behavior-changing.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARTIFACT = "xuan_ce25_projected_guard_summary_scorer"
DEFAULT_ROOT = Path(
    "xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_20260524T113800Z"
)
DEFAULT_RUNNER_SMOKE_MANIFEST = DEFAULT_ROOT / "manifest.json"
DEFAULT_DEFAULT_AGGREGATE = DEFAULT_ROOT / "default_off" / "aggregate_report.json"
DEFAULT_ENABLED_AGGREGATE = DEFAULT_ROOT / "enabled" / "aggregate_report.json"
DEFAULT_ENABLED_SUMMARY_DIR = DEFAULT_ROOT / "enabled"
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
SUMMARY_SCHEMA = "ce25_projected_guard_summary_v1"
EXPECTED_FIELD_CONTRACT = {
    "default_off": True,
    "post_action_outcome_labels_included": False,
    "realized_pair_cost_used_as_live_criteria": False,
    "trading_behavior_changed": False,
    "private_truth_ready": False,
    "deployable": False,
    "promotion_gate_passed": False,
}
AGGREGATED_KEYS = (
    "decision_count_by_status",
    "decision_count_by_guard",
    "decision_count_by_asset_timeframe_guard_status",
    "projected_pair_cost_bucket_by_guard",
    "projected_residual_bucket_by_guard",
    "final_window_policy_count",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> Any:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def path_safe(path: Path) -> bool:
    text = str(path.resolve())
    return not any(fragment in text for fragment in FORBIDDEN_PATH_FRAGMENTS)


def safe_load(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path_safe(path):
        return None, "unsafe_path"
    try:
        obj = load_json(path)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(obj, dict):
        return None, "not_json_object"
    return obj, None


def add_recursive(dest: dict[str, Any], src: dict[str, Any]) -> None:
    for key, value in src.items():
        if isinstance(value, dict):
            child = dest.setdefault(key, {})
            if isinstance(child, dict):
                add_recursive(child, value)
            else:
                dest[key] = value
        elif isinstance(value, (int, float)):
            dest[key] = float(dest.get(key, 0.0) or 0.0) + float(value)
        else:
            dest[key] = value


def normalize_numbers(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): normalize_numbers(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [normalize_numbers(v) for v in value]
    if isinstance(value, (int, float)):
        return round(float(value), 9)
    return value


def ce25_summary(report: dict[str, Any]) -> dict[str, Any] | None:
    lite = report.get("event_lite")
    if not isinstance(lite, dict):
        return None
    diag = lite.get("ce25_projected_guard_summary")
    return diag if isinstance(diag, dict) else None


def load_summaries(summary_dir: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path_safe(summary_dir):
        return [], ["unsafe_summary_dir"]
    summaries: list[dict[str, Any]] = []
    errors: list[str] = []
    for path in sorted(summary_dir.glob("*.summary.json")):
        obj, err = safe_load(path)
        if err:
            errors.append(f"{path.name}:{err}")
        elif obj is not None:
            summaries.append(obj)
    if not summaries:
        errors.append("no_summary_json_files")
    return summaries, errors


def reconstructed_summary(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for summary in summaries:
        diag = ce25_summary(summary)
        if not isinstance(diag, dict):
            continue
        for key in ("schema_version", "field_contract"):
            if key in diag and key not in out:
                out[key] = diag[key]
        for key in AGGREGATED_KEYS:
            source = diag.get(key)
            if isinstance(source, dict):
                add_recursive(out.setdefault(key, {}), source)
    return out


def build_score(
    *,
    runner_smoke: dict[str, Any],
    default_aggregate: dict[str, Any],
    enabled_aggregate: dict[str, Any],
    enabled_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    default_diag = ce25_summary(default_aggregate)
    enabled_diag = ce25_summary(enabled_aggregate)
    reconstructed = reconstructed_summary(enabled_summaries)
    field_contract = enabled_diag.get("field_contract") if isinstance(enabled_diag, dict) else {}
    checks = {
        "runner_smoke_keep": runner_smoke.get("decision_label")
        == "KEEP_CE25_PROJECTED_GUARD_RUNNER_INSTRUMENTATION_DEFAULT_OFF_READY",
        "default_off_summary_absent": default_diag is None,
        "enabled_summary_present": isinstance(enabled_diag, dict),
        "schema_version_ok": isinstance(enabled_diag, dict)
        and enabled_diag.get("schema_version") == SUMMARY_SCHEMA,
        "field_contract_ok": isinstance(field_contract, dict)
        and all(field_contract.get(key) is expected for key, expected in EXPECTED_FIELD_CONTRACT.items()),
        "starter_denominator_present": isinstance(enabled_diag, dict)
        and enabled_diag.get("decision_count_by_guard", {}).get("starter", {}).get("would_allow_projected_guard", 0)
        >= 1,
        "core_denominator_present": isinstance(enabled_diag, dict)
        and enabled_diag.get("decision_count_by_guard", {}).get("core", {}).get("would_allow_projected_guard", 0)
        >= 1,
        "hard_kill_denominator_present": isinstance(enabled_diag, dict)
        and enabled_diag.get("decision_count_by_guard", {}).get("hard_kill", {}).get("would_block_pair_cost_gte_1_00", 0)
        >= 1,
        "projected_pair_cost_buckets_present": isinstance(enabled_diag, dict)
        and enabled_diag.get("projected_pair_cost_bucket_by_guard", {}).get("starter", {}).get(
            "projected_pair_cost_lt_0p90",
            0,
        )
        >= 1
        and enabled_diag.get("projected_pair_cost_bucket_by_guard", {}).get("core", {}).get(
            "projected_pair_cost_0p90_0p95",
            0,
        )
        >= 1
        and enabled_diag.get("projected_pair_cost_bucket_by_guard", {}).get("hard_kill", {}).get(
            "projected_pair_cost_gte_1p00",
            0,
        )
        >= 1,
        "projected_residual_buckets_present": isinstance(enabled_diag, dict)
        and enabled_diag.get("projected_residual_bucket_by_guard", {}).get("starter", {}).get(
            "projected_residual_lt_10pct",
            0,
        )
        >= 1
        and enabled_diag.get("projected_residual_bucket_by_guard", {}).get("core", {}).get(
            "projected_residual_lt_10pct",
            0,
        )
        >= 1,
        "aggregate_parity": isinstance(enabled_diag, dict)
        and all(
            normalize_numbers(enabled_diag.get(key, {})) == normalize_numbers(reconstructed.get(key, {}))
            for key in AGGREGATED_KEYS
        ),
        "promotion_never_passes": not bool(runner_smoke.get("promotion_gate", {}).get("passed")),
        "private_truth_false": not bool(runner_smoke.get("promotion_gate", {}).get("private_truth_ready")),
        "deployable_false": not bool(runner_smoke.get("promotion_gate", {}).get("deployable")),
        "no_side_effect_scope": not bool(runner_smoke.get("scope", {}).get("shadow_started"))
        and not bool(runner_smoke.get("scope", {}).get("orders_cancels_redeems_sent"))
        and not bool(runner_smoke.get("scope", {}).get("shared_ingress_or_live_modified")),
    }
    blockers = [name for name, ok in checks.items() if not ok]
    decision = "KEEP" if not blockers else "UNKNOWN"
    label = (
        "KEEP_CE25_PROJECTED_GUARD_SUMMARY_SCORER_READY"
        if decision == "KEEP"
        else "UNKNOWN_CE25_PROJECTED_GUARD_SUMMARY_SCORER_INPUTS_INSUFFICIENT"
    )
    return {
        "decision": decision,
        "decision_label": label,
        "checks": checks,
        "blockers": blockers,
        "summary": {
            "schema_version": enabled_diag.get("schema_version") if isinstance(enabled_diag, dict) else None,
            "starter_allow_count": enabled_diag.get("decision_count_by_guard", {})
            .get("starter", {})
            .get("would_allow_projected_guard", 0)
            if isinstance(enabled_diag, dict)
            else 0,
            "core_allow_count": enabled_diag.get("decision_count_by_guard", {})
            .get("core", {})
            .get("would_allow_projected_guard", 0)
            if isinstance(enabled_diag, dict)
            else 0,
            "hard_kill_pair_cost_count": enabled_diag.get("decision_count_by_guard", {})
            .get("hard_kill", {})
            .get("would_block_pair_cost_gte_1_00", 0)
            if isinstance(enabled_diag, dict)
            else 0,
            "enabled_summary_files": len(enabled_summaries),
        },
        "research_ranking": {
            "status": label,
            "diagnostic_ready": decision == "KEEP",
            "strategy_evidence": False,
            "no_order_diagnostic_allowed": False,
        },
        "promotion_gate": {
            "passed": False,
            "private_truth_ready": False,
            "deployable": False,
            "g2_canary_ready": False,
            "blockers": [
                "summary_only_diagnostics_not_strategy_evidence",
                "no_private_truth",
                "no_non_fixture_live_profitability_validation",
            ],
        },
    }


def build_manifest(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    if not path_safe(output_dir):
        raise RuntimeError(f"unsafe output path: {output_dir}")
    loaded: dict[str, dict[str, Any]] = {}
    load_errors: dict[str, str] = {}
    for name, path in (
        ("runner_smoke_manifest", args.runner_smoke_manifest),
        ("default_aggregate", args.default_aggregate),
        ("enabled_aggregate", args.enabled_aggregate),
    ):
        obj, err = safe_load(path)
        if err:
            load_errors[name] = err
        elif obj is not None:
            loaded[name] = obj
    enabled_summaries, summary_errors = load_summaries(args.enabled_summary_dir)
    if summary_errors:
        load_errors["enabled_summary_dir"] = ";".join(summary_errors)
    if load_errors:
        score = {
            "decision": "UNKNOWN",
            "decision_label": "UNKNOWN_CE25_PROJECTED_GUARD_SUMMARY_SCORER_INPUTS_INSUFFICIENT",
            "checks": {},
            "blockers": sorted(load_errors),
            "summary": {},
            "research_ranking": {
                "status": "UNKNOWN_CE25_PROJECTED_GUARD_SUMMARY_SCORER_INPUTS_INSUFFICIENT",
                "diagnostic_ready": False,
                "strategy_evidence": False,
                "no_order_diagnostic_allowed": False,
            },
            "promotion_gate": {
                "passed": False,
                "private_truth_ready": False,
                "deployable": False,
                "g2_canary_ready": False,
                "blockers": sorted(load_errors),
            },
        }
    else:
        score = build_score(
            runner_smoke=loaded["runner_smoke_manifest"],
            default_aggregate=loaded["default_aggregate"],
            enabled_aggregate=loaded["enabled_aggregate"],
            enabled_summaries=enabled_summaries,
        )
    return {
        "schema_version": 1,
        "artifact": ARTIFACT,
        "created_utc": utc_label(),
        "lane": "ce25_projected_guard_summary_scorer",
        "decision": score["decision"],
        "decision_label": score["decision_label"],
        "scope": {
            "local_only": True,
            "summary_aggregate_only": True,
            "new_data_fetched": False,
            "ssh_used": False,
            "shadow_started": False,
            "events_jsonl_read": False,
            "raw_replay_full_store_scanned": False,
            "shared_ingress_or_live_modified": False,
            "orders_cancels_redeems_sent": False,
        },
        "inputs": {
            "runner_smoke_manifest": str(args.runner_smoke_manifest),
            "default_aggregate": str(args.default_aggregate),
            "enabled_aggregate": str(args.enabled_aggregate),
            "enabled_summary_dir": str(args.enabled_summary_dir),
        },
        "score": {
            "checks": score["checks"],
            "blockers": score["blockers"],
            "summary": score["summary"],
        },
        "research_ranking": score["research_ranking"],
        "promotion_gate": score["promotion_gate"],
        "next_executable_action": (
            "implement ce25_projected_guard_summary_real_pullback_scorer_adapter local-only only after a future "
            "allowlisted no-order pullback contains ce25_projected_guard_summary_v1; until then continue local "
            "public-profile search or non-fixture input preparation, not no-order diagnostics"
        ),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runner-smoke-manifest", type=Path, default=DEFAULT_RUNNER_SMOKE_MANIFEST)
    ap.add_argument("--default-aggregate", type=Path, default=DEFAULT_DEFAULT_AGGREGATE)
    ap.add_argument("--enabled-aggregate", type=Path, default=DEFAULT_ENABLED_AGGREGATE)
    ap.add_argument("--enabled-summary-dir", type=Path, default=DEFAULT_ENABLED_SUMMARY_DIR)
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path(f"xuan_research_artifacts/{ARTIFACT}_{utc_label()}"),
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    manifest = build_manifest(args, args.output_dir)
    write_json(args.output_dir / "manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
