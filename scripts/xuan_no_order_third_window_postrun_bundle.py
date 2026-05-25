#!/usr/bin/env python3
"""Bundle post-run scoring for the next xuan-frontier no-order window.

Given a new bounded no-order dry-run output root, this local wrapper runs the
same scorers used for the soft-closeability shadow-review lane and rebuilds the
evidence packet. It intentionally tolerates UNKNOWN/BLOCKED scorer exits so a
partial or failing window still leaves a complete review trail.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        val = float(value)
        return val if math.isfinite(val) else default
    except (TypeError, ValueError):
        return default


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def run_command(cmd: list[str], cwd: Path) -> dict[str, Any]:
    started = time.time()
    proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False)
    return {
        "cmd": cmd,
        "exit_code": proc.returncode,
        "runtime_s": round(time.time() - started, 3),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }


def load_lifecycle_rows(root: Path) -> dict[str, list[dict[str, str]]]:
    rows = {"actions": [], "rescues": []}
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        manifest = read_json(manifest_path)
        files = manifest.get("files", {})
        rows["actions"].extend(read_csv(root / files.get("would_action_decisions", "")))
        rows["rescues"].extend(read_csv(root / files.get("strict_rescue_closes", "")))
    return rows


def max_float(rows: list[dict[str, str]], field: str) -> float | None:
    vals = [as_float(row.get(field), float("nan")) for row in rows]
    nums = [val for val in vals if math.isfinite(val)]
    return max(nums) if nums else None


def count_active_slugs(root: Path) -> tuple[int, int]:
    active = 0
    rescue = 0
    for manifest_path in sorted(root.glob("*.normalized_lifecycle_manifest.json")):
        counts = read_json(manifest_path).get("row_counts", {})
        if sum(counts.values()) > 0:
            active += 1
        if counts.get("strict_rescue_closes", 0) > 0:
            rescue += 1
    return active, rescue


def write_runtime_summary(root: Path, output_path: Path) -> dict[str, Any]:
    required = ["manifest.json", "aggregate_report.json", "run_exit_code.txt", "run_stderr.log"]
    missing = [name for name in required if not (root / name).exists()]
    if missing:
        summary = {
            "status": "UNKNOWN_THIRD_WINDOW_RUNTIME_SUMMARY_MISSING_FILES",
            "missing_files": missing,
            "output_root": str(root),
            "metrics": {},
            "profile": {},
            "safety": {"remote_runner_allowed": False, "orders_sent": False},
            "decision": {
                "deployable": False,
                "remote_runner_allowed": False,
                "shadow_review_ready": False,
            },
        }
    else:
        manifest = read_json(root / "manifest.json")
        aggregate = read_json(root / "aggregate_report.json")
        metrics = aggregate.get("metrics", {})
        blocked = aggregate.get("blocked", {})
        rows = load_lifecycle_rows(root)
        accepted = [row for row in rows["actions"] if row.get("decision") == "accept"]
        rescues = rows["rescues"]
        active_slugs, rescue_slugs = count_active_slugs(root)
        filled_qty = as_float(metrics.get("filled_qty"))
        filled_cost = as_float(metrics.get("filled_cost"))
        residual_qty = as_float(metrics.get("residual_qty"))
        residual_cost = as_float(metrics.get("residual_cost"))
        strict_rescue_source_blocks = as_float(metrics.get("strict_rescue_source_blocks"))
        run_exit = (root / "run_exit_code.txt").read_text().strip()
        stderr_empty = (root / "run_stderr.log").read_text() == ""
        safety = manifest.get("safety", {})
        safe = (
            run_exit == "0"
            and stderr_empty
            and safety.get("orders_sent") is False
            and str(safety.get("dry_run")) == "1"
        )
        summary = {
            "status": (
                "KEEP_THIRD_WINDOW_RUNTIME_SUMMARY_SAFE_RESEARCH_ONLY"
                if safe
                else "UNKNOWN_THIRD_WINDOW_RUNTIME_SUMMARY_SAFETY_OR_EXIT_BLOCKED"
            ),
            "output_root": str(root),
            "metrics": {
                "accepted_actions": len(accepted),
                "queue_supported_fills": int(as_float(metrics.get("queue_supported_fills"))),
                "strict_rescue_closes": len(rescues),
                "strict_rescue_qty": as_float(metrics.get("strict_rescue_qty")),
                "active_slugs": active_slugs,
                "rescue_slugs": rescue_slugs,
                "filled_qty": filled_qty,
                "filled_cost": filled_cost,
                "pair_pnl": as_float(metrics.get("pair_pnl")),
                "roi_on_filled_cost": as_float(metrics.get("roi_on_filled_cost")),
                "residual_qty": residual_qty,
                "residual_cost": residual_cost,
                "residual_qty_share": residual_qty / filled_qty if filled_qty else None,
                "residual_cost_share": residual_cost / filled_cost if filled_cost else None,
                "accepted_l1_age_ms_max": max_float(accepted, "source_quality_l1_age_ms"),
                "rescue_l1_age_ms_max": max_float(rescues, "strict_rescue_l1_age_ms"),
                "rescue_net_pair_cost_max": max_float(rescues, "net_pair_cost"),
                "strict_rescue_source_blocks": strict_rescue_source_blocks,
                "strict_rescue_l1_age_blocks": int(blocked.get("strict_rescue_l1_age") or 0),
                "closeability_debt_open": as_float(metrics.get("closeability_debt_open")),
                "closeability_debt_max_open": as_float(metrics.get("closeability_debt_max_open")),
            },
            "profile": manifest.get("config", {}),
            "safety": {
                "pm_dry_run": str(safety.get("dry_run")) == "1",
                "orders_sent": safety.get("orders_sent"),
                "deployable": False,
                "live_trading_allowed": False,
                "shared_service_mutation": False,
                "remote_repo_mutation": False,
                "deploy_or_restart": False,
                "collector_rebuild": False,
            },
            "concurrency": manifest.get("concurrency", {}),
            "decision": {
                "deployable": False,
                "remote_runner_allowed": False,
                "shadow_review_ready": False,
            },
        }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rounded(summary), indent=2, sort_keys=True) + "\n")
    return summary


def status_of(path: Path) -> str | None:
    if not path.exists():
        return None
    return read_json(path).get("status") or "KEEP_SCORECARD_PRESENT_NO_STATUS_FIELD_LOCAL_ONLY"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--third-output-root", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--replay-scorecard", default=".tmp_xuan/scorecards/source_quality_market_budget_rescue_replay_validation_20260521T1133Z.json")
    parser.add_argument("--baseline-root", default=".tmp_xuan/local_verifier_artifacts/no_order_replay_aligned_rescue_diagnostic_runtime_20260522T0924Z/remote_outputs")
    parser.add_argument("--prior-output-roots", nargs="*", default=None)
    parser.add_argument("--no-default-prior-output-roots", action="store_true")
    parser.add_argument("--profile-scorecard", default=".tmp_xuan/scorecards/no_order_soft_closeability_third_window_profile_20260522T1849Z.json")
    parser.add_argument("--public-benchmark-scorecard", default=None)
    parser.add_argument("--capacity-plan-scorecard", default=None)
    parser.add_argument("--capacity-stage", default="cap_25")
    parser.add_argument("--max-closeability-debt-per-slug", type=float, default=1.0)
    parser.add_argument(
        "--contrast-reference-runtime-summary",
        default=".tmp_xuan/scorecards/no_order_xuan-frontier-soft-mainline-explicit-surplus-no-cancel-20260525T0510Z_runtime_summary.json",
    )
    parser.add_argument(
        "--contrast-reference-event-diagnostics",
        default=".tmp_xuan/scorecards/xuan-frontier-soft-mainline-explicit-surplus-no-cancel-20260525T0510Z_event_diagnostics.json",
    )
    parser.add_argument(
        "--contrast-reference-prefix-scorecard",
        default=".tmp_xuan/scorecards/xuan_soft_mainline_density_prefix_scorer_0510Z_duration_guard_regression_20260525T0932Z.json",
    )
    parser.add_argument("--disable-window-contrast", action="store_true")
    args = parser.parse_args()
    if args.prior_output_roots is None:
        args.prior_output_roots = [] if args.no_default_prior_output_roots else [
            ".tmp_xuan/local_verifier_artifacts/no_order_soft_closeability_balanced_runtime_20260522T1546Z/remote_outputs",
            ".tmp_xuan/local_verifier_artifacts/no_order_soft_closeability_comparable_runtime_20260522T1705Z/remote_outputs",
        ]

    started = time.time()
    cwd = Path.cwd()
    third_root = Path(args.third_output_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    scorecard_dir = Path(args.scorecard_json).expanduser().resolve().parent
    scorecard_dir.mkdir(parents=True, exist_ok=True)
    profile_card = read_json(Path(args.profile_scorecard).expanduser().resolve())
    profile_body = profile_card.get("profile", {})
    max_rescue_net_pair_cost = as_float(profile_body.get("strict_rescue_salvage_net_cap"), 0.95)

    paths = {
        "runtime_summary": scorecard_dir / f"no_order_{args.tag}_runtime_summary.json",
        "event_diagnostics": scorecard_dir / f"no_order_{args.tag}_event_diagnostics.json",
        "density_preflight": scorecard_dir / f"no_order_{args.tag}_density_preflight_gate.json",
        "density_prefix": scorecard_dir / f"no_order_{args.tag}_density_prefix_scorer.json",
        "next_run_decision": scorecard_dir / f"no_order_{args.tag}_next_run_decision.json",
        "lifecycle": scorecard_dir / f"no_order_{args.tag}_lifecycle_scorer.json",
        "comparison": scorecard_dir / f"no_order_{args.tag}_comparison_scorer.json",
        "concurrency": scorecard_dir / f"no_order_{args.tag}_concurrent_shared_ingress_scorer.json",
        "young_tiny_residual": scorecard_dir / f"no_order_{args.tag}_young_tiny_residual_scorer.json",
        "shadow": scorecard_dir / f"no_order_{args.tag}_shadow_readiness.json",
        "repeat": scorecard_dir / f"no_order_{args.tag}_repeat_window_scorer.json",
        "gap": scorecard_dir / f"no_order_{args.tag}_repeat_window_gap_plan.json",
        "capital": scorecard_dir / f"no_order_{args.tag}_capital_reuse_roi.json",
        "packet": scorecard_dir / f"no_order_{args.tag}_shadow_review_packet.json",
    }
    contrast_reference_paths = {
        "runtime_summary": Path(args.contrast_reference_runtime_summary).expanduser().resolve(),
        "event_diagnostics": Path(args.contrast_reference_event_diagnostics).expanduser().resolve(),
        "prefix_scorecard": Path(args.contrast_reference_prefix_scorecard).expanduser().resolve(),
    }
    contrast_reference_available = (
        not args.disable_window_contrast and all(path.exists() for path in contrast_reference_paths.values())
    )
    if contrast_reference_available:
        paths["window_contrast"] = scorecard_dir / f"no_order_{args.tag}_window_contrast.json"
        paths["density_history"] = scorecard_dir / f"no_order_{args.tag}_density_history.json"
    public_benchmark_path = (
        Path(args.public_benchmark_scorecard).expanduser().resolve()
        if args.public_benchmark_scorecard
        else None
    )
    if public_benchmark_path and public_benchmark_path.exists():
        paths["public_benchmark"] = scorecard_dir / f"no_order_{args.tag}_public_benchmark_comparison.json"
    capacity_plan_path = (
        Path(args.capacity_plan_scorecard).expanduser().resolve()
        if args.capacity_plan_scorecard
        else None
    )
    if public_benchmark_path and capacity_plan_path and public_benchmark_path.exists() and capacity_plan_path.exists():
        paths["capacity_stage_public_benchmark"] = (
            scorecard_dir / f"no_order_{args.tag}_capacity_stage_public_benchmark_gate.json"
        )

    runtime_summary = write_runtime_summary(third_root, paths["runtime_summary"])
    event_diagnostics_command = [
        sys.executable,
        "scripts/xuan_no_order_event_diagnostics_from_jsonl.py",
        "--output-root",
        str(third_root),
        "--output-json",
        str(paths["event_diagnostics"]),
    ]
    if profile_body.get("risk_seed_pair_completion_required_above_net_cap") is not None:
        event_diagnostics_command.extend(
            [
                "--risk-seed-pair-completion-required-above-net-cap",
                str(profile_body.get("risk_seed_pair_completion_required_above_net_cap")),
            ]
        )
    if profile_body.get("risk_seed_pair_completion_required_above_fair_price_pair_cost") is not None:
        event_diagnostics_command.extend(
            [
                "--risk-seed-pair-completion-required-above-fair-price-pair-cost",
                str(profile_body.get("risk_seed_pair_completion_required_above_fair_price_pair_cost")),
            ]
        )
    if profile_body.get("risk_seed_pair_completion_min_qty") is not None:
        event_diagnostics_command.extend(
            [
                "--risk-seed-pair-completion-min-qty",
                str(profile_body.get("risk_seed_pair_completion_min_qty")),
            ]
        )
    commands = [
        event_diagnostics_command,
        [
            sys.executable,
            "scripts/xuan_soft_mainline_density_preflight_gate.py",
            "--event-diagnostics",
            str(paths["event_diagnostics"]),
            "--runtime-summary",
            str(paths["runtime_summary"]),
            "--scorecard-json",
            str(paths["density_preflight"]),
        ],
        [
            sys.executable,
            "scripts/xuan_soft_mainline_density_prefix_scorer.py",
            "--output-root",
            str(third_root),
            "--scorecard-json",
            str(paths["density_prefix"]),
        ],
        [
            sys.executable,
            "scripts/xuan_soft_mainline_next_run_decision_scorer.py",
            "--latest-prefix-scorecard",
            str(paths["density_prefix"]),
            "--latest-density-scorecard",
            str(paths["density_preflight"]),
            "--scorecard-json",
            str(paths["next_run_decision"]),
        ],
    ]
    if contrast_reference_available:
        commands.append(
            [
                sys.executable,
                "scripts/xuan_soft_mainline_window_contrast_scorer.py",
                "--reference-runtime-summary",
                str(contrast_reference_paths["runtime_summary"]),
                "--reference-event-diagnostics",
                str(contrast_reference_paths["event_diagnostics"]),
                "--reference-prefix-scorecard",
                str(contrast_reference_paths["prefix_scorecard"]),
                "--latest-runtime-summary",
                str(paths["runtime_summary"]),
                "--latest-event-diagnostics",
                str(paths["event_diagnostics"]),
                "--latest-prefix-scorecard",
                str(paths["density_prefix"]),
                "--reference-label",
                "soft_mainline_reference_good_window",
                "--latest-label",
                args.tag,
                "--scorecard-json",
                str(paths["window_contrast"]),
            ]
        )
        commands.append(
            [
                sys.executable,
                "scripts/xuan_soft_mainline_density_history_scorer.py",
                "--window",
                "soft_mainline_reference_good_window",
                str(contrast_reference_paths["runtime_summary"]),
                str(contrast_reference_paths["event_diagnostics"]),
                str(contrast_reference_paths["prefix_scorecard"]),
                "--window",
                args.tag,
                str(paths["runtime_summary"]),
                str(paths["event_diagnostics"]),
                str(paths["density_prefix"]),
                "--scorecard-json",
                str(paths["density_history"]),
            ]
        )
    commands.extend(
        [
            [
                sys.executable,
                "scripts/xuan_no_order_strict_rescue_lifecycle_scorer.py",
                "--output-root",
                str(third_root),
                "--scorecard-json",
                str(paths["lifecycle"]),
                "--min-rescue-rows",
                "1",
                "--max-action-l1-age-ms",
                "1000",
                "--max-rescue-l1-age-ms",
                "50",
            ],
            [
                sys.executable,
                "scripts/xuan_no_order_closeability_gate_comparison_scorer.py",
                "--baseline-root",
                str(Path(args.baseline_root).expanduser().resolve()),
                "--candidate-root",
                str(third_root),
                "--scorecard-json",
                str(paths["comparison"]),
            ],
            [
                sys.executable,
                "scripts/xuan_no_order_young_tiny_residual_scorer.py",
                "--output-root",
                str(third_root),
                "--scorecard-json",
                str(paths["young_tiny_residual"]),
            ],
            [
                sys.executable,
                "scripts/xuan_no_order_runtime_shadow_readiness_scorer.py",
                "--output-root",
                str(third_root),
                "--scorecard-json",
                str(paths["shadow"]),
                "--young-tiny-residual-scorecard",
                str(paths["young_tiny_residual"]),
                "--max-closeability-debt-open-per-slug",
                str(args.max_closeability_debt_per_slug),
                "--max-closeability-debt-max-open-per-slug",
                str(args.max_closeability_debt_per_slug),
                "--max-rescue-net-pair-cost",
                str(max_rescue_net_pair_cost),
            ],
            [
                sys.executable,
                "scripts/xuan_no_order_concurrent_shared_ingress_scorer.py",
                "--output-root",
                str(third_root),
                "--scorecard-json",
                str(paths["concurrency"]),
            ],
            [
                sys.executable,
                "scripts/xuan_no_order_runtime_repeat_window_scorer.py",
                "--output-roots",
                *[str(Path(root).expanduser().resolve()) for root in args.prior_output_roots],
                str(third_root),
                "--scorecard-json",
                str(paths["repeat"]),
                "--max-rescue-net-pair-cost",
                str(max_rescue_net_pair_cost),
            ],
            [
                sys.executable,
                "scripts/xuan_no_order_runtime_repeat_window_gap_planner.py",
                "--repeat-scorecard",
                str(paths["repeat"]),
                "--scorecard-json",
                str(paths["gap"]),
            ],
            [
                sys.executable,
                "scripts/xuan_no_order_capital_reuse_roi_scorer.py",
                *[
                    item
                    for root in [
                        *[str(Path(root).expanduser().resolve()) for root in args.prior_output_roots],
                        str(third_root),
                    ]
                    for item in ("--output-root", root)
                ],
                "--scorecard-json",
                str(paths["capital"]),
                "--round-notional",
                str(profile_body.get("target_qty") or 300.0),
                "--round-minutes",
                "5",
            ],
        ]
    )
    if public_benchmark_path and "public_benchmark" in paths:
        commands.append(
            [
                sys.executable,
                "scripts/xuan_no_order_public_benchmark_comparison_scorer.py",
                "--public-benchmark-scorecard",
                str(public_benchmark_path),
                "--capital-roi-scorecard",
                str(paths["capital"]),
                "--runtime-scorecard",
                str(paths["repeat"]),
                "--scorecard-json",
                str(paths["public_benchmark"]),
            ]
        )
    if public_benchmark_path and capacity_plan_path and "capacity_stage_public_benchmark" in paths:
        commands.append(
            [
                sys.executable,
                "scripts/xuan_capacity_stage_public_benchmark_gate.py",
                "--runtime-summary",
                str(paths["runtime_summary"]),
                "--capital-roi-scorecard",
                str(paths["capital"]),
                "--public-benchmark-scorecard",
                str(public_benchmark_path),
                "--capacity-plan",
                str(capacity_plan_path),
                "--stage",
                str(args.capacity_stage),
                "--scorecard-json",
                str(paths["capacity_stage_public_benchmark"]),
                "--markdown",
                str(output_dir / "CAPACITY_STAGE_PUBLIC_BENCHMARK_GATE.md"),
            ]
        )
    packet_command = [
        sys.executable,
        "scripts/xuan_no_order_shadow_review_packet_builder.py",
        "--replay-scorecard",
        str(Path(args.replay_scorecard).expanduser().resolve()),
        "--runtime-scorecard",
        str(paths["runtime_summary"]),
        "--repeat-scorecard",
        str(paths["repeat"]),
        "--gap-plan-scorecard",
        str(paths["gap"]),
        "--profile-scorecard",
        str(Path(args.profile_scorecard).expanduser().resolve()),
        "--concurrency-scorecard",
        str(paths["concurrency"]),
        "--capital-roi-scorecard",
        str(paths["capital"]),
        "--output-dir",
        str(output_dir),
        "--scorecard-json",
        str(paths["packet"]),
    ]
    if "public_benchmark" in paths:
        packet_command.extend(["--public-benchmark-comparison-scorecard", str(paths["public_benchmark"])])
    commands.append(packet_command)

    command_results = [run_command(command, cwd) for command in commands]
    statuses = {name: status_of(path) for name, path in paths.items()}
    all_outputs_present = all(path.exists() for path in paths.values())
    concurrency_clean = statuses.get("concurrency") == "KEEP_NO_ORDER_CONCURRENT_SHARED_INGRESS_EVIDENCE_PASS_RESEARCH_ONLY"
    if not all_outputs_present:
        bundle_status = "UNKNOWN_THIRD_WINDOW_POSTRUN_BUNDLE_INCOMPLETE"
    elif not concurrency_clean:
        bundle_status = "UNKNOWN_THIRD_WINDOW_POSTRUN_BUNDLE_CONCURRENCY_EVIDENCE_BLOCKED"
    else:
        bundle_status = "KEEP_THIRD_WINDOW_POSTRUN_BUNDLE_READY_LOCAL_ONLY"
    scorecard = {
        "artifact": "xuan_no_order_third_window_postrun_bundle",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_third_window_postrun_bundle.py",
        "status": bundle_status,
        "third_output_root": str(third_root),
        "output_dir": str(output_dir),
        "scorecards": {key: str(path) for key, path in paths.items()},
        "scorecard_statuses": statuses,
        "commands": command_results,
        "runtime_summary": runtime_summary,
        "profile_scorecard": str(Path(args.profile_scorecard).expanduser().resolve()),
        "window_contrast_reference": {
            "enabled": contrast_reference_available,
            "disabled_by_arg": args.disable_window_contrast,
            "paths": {key: str(path) for key, path in contrast_reference_paths.items()},
        },
        "profile_cap_policy": {
            "target_rescue_net_cap": profile_body.get("target_rescue_net_cap"),
            "max_rescue_net_pair_cost": max_rescue_net_pair_cost,
        },
        "decision": {
            "local_bundle_ready": all_outputs_present,
            "concurrent_shared_ingress_evidence_clean": concurrency_clean,
            "deployable": False,
            "remote_runner_allowed": False,
        },
    }
    bundle_path = Path(args.scorecard_json).expanduser().resolve()
    bundle_path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
