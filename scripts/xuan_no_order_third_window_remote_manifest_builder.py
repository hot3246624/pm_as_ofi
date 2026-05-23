#!/usr/bin/env python3
"""Build a local staging manifest for the third xuan-frontier no-order window.

This script does not connect to any server. It records the exact local files,
hashes, profile parameters, remote path template, and post-run bundle command
needed if a user later authorizes the bounded dry-run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shlex
import time
from pathlib import Path
from typing import Any


DEFAULT_STAGE_FILES = [
    "tools/xuan_dplus_passive_passive_shadow_runner.py",
    "scripts/xuan_no_order_third_window_postrun_bundle.py",
    "scripts/xuan_no_order_strict_rescue_lifecycle_scorer.py",
    "scripts/xuan_no_order_closeability_gate_comparison_scorer.py",
    "scripts/xuan_no_order_runtime_shadow_readiness_scorer.py",
    "scripts/xuan_no_order_runtime_repeat_window_scorer.py",
    "scripts/xuan_no_order_runtime_repeat_window_gap_planner.py",
    "scripts/xuan_no_order_shadow_review_packet_builder.py",
    "scripts/xuan_no_order_concurrent_shared_ingress_scorer.py",
]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 6)
    if isinstance(value, dict):
        return {key: rounded(val) for key, val in value.items()}
    if isinstance(value, list):
        return [rounded(val) for val in value]
    return value


def shell_quote_part(part: str) -> str:
    if "=" in part:
        name, value = part.split("=", 1)
        if name and (name[0].isalpha() or name[0] == "_") and all(ch.isalnum() or ch == "_" for ch in name):
            return f"{name}={shlex.quote(value)}"
    return shlex.quote(part)


def shell_join(parts: list[str]) -> str:
    return " ".join(shell_quote_part(part) for part in parts)


def profile_value(profile: dict[str, Any], key: str, default: Any) -> Any:
    value = profile.get(key)
    return default if value is None else value


def append_option(parts: list[str], option: str, value: Any) -> None:
    if value is None:
        return
    parts.extend([option, str(value)])


def render_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        "# Third Window Remote Staging Manifest",
        "",
        "This is a local manifest only. It does not authorize or start remote execution.",
        "",
        "## Status",
        f"- status: `{manifest['status']}`",
        f"- remote_runner_allowed: `{manifest['decision']['remote_runner_allowed']}`",
        f"- deployable: `{manifest['decision']['deployable']}`",
        f"- orders_sent_allowed: `{manifest['safety']['orders_sent_allowed']}`",
        "",
        "## Instance",
        f"- instance_id_template: `{manifest['instance']['instance_id_template']}`",
        f"- remote_output_dir_template: `{manifest['instance']['remote_output_dir_template']}`",
        f"- local_output_root_template: `{manifest['instance']['local_output_root_template']}`",
        "",
        "## Profile",
    ]
    for key, value in manifest["profile"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Files To Stage"])
    for item in manifest["files_to_stage"]:
        lines.append(f"- `{item['path']}` sha256=`{item['sha256']}` bytes=`{item['bytes']}`")
    lines.extend(
        [
            "",
            "## Remote Command Template",
            "```bash",
            manifest["remote_command_template"],
            "```",
            "",
            "## Local Post-Run Bundle Command Template",
            "```bash",
            manifest["postrun_bundle_command_template"],
            "```",
            "",
            "## Guardrails",
            "- Requires explicit user authorization before remote execution.",
            "- Dry-run only: `PM_DRY_RUN=1`.",
            "- Shared-ingress concurrency is allowed only as a read-only client and must be scored from runner manifest evidence.",
            "- No deploy, restart, shared-service mutation, remote repo mutation, collector rebuild, raw/replay scan, or cron loop.",
            "- This manifest is research-only and not promotion evidence by itself.",
            "",
        ]
    )
    return "\n".join(lines)


def build(args: argparse.Namespace) -> dict[str, Any]:
    cwd = Path.cwd()
    profile_path = Path(args.profile_scorecard).expanduser().resolve()
    profile_card = read_json(profile_path)
    profile = profile_card.get("profile", {})
    profile_status = str(profile_card.get("status") or "")
    profile_family = str(profile.get("profile_family") or "")
    reset_repeat_roots = profile_status.startswith("RESIDUAL_GUARD") or profile_family == "residual_guard"
    files = []
    for rel in args.stage_files:
        path = (cwd / rel).resolve()
        files.append(
            {
                "path": rel,
                "abs_path": str(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    instance_template = f"{args.instance_prefix}-<YYYYMMDDTHHMMZ>"
    remote_runner_template = f"{args.remote_tool_dir}/xuan_dplus_passive_passive_shadow_runner_<YYYYMMDDTHHMMZ>.py"
    remote_output_template = f"{args.remote_output_base}/{instance_template}"
    local_output_template = f".tmp_xuan/local_verifier_artifacts/{instance_template}/remote_outputs"
    remote_command = [
        "PM_DRY_RUN=1",
        f"PM_SHARED_INGRESS_ROLE={args.shared_ingress_role}",
        f"PM_INSTANCE_ID={instance_template}",
        "python3",
        remote_runner_template,
        "--repo",
        args.remote_repo,
        "--shared-ingress-root",
        args.shared_ingress_root,
        "--prefix",
        args.market_prefix,
        "--round-offsets",
        str(profile["round_offsets"]),
        "--duration-s",
        str(profile["duration_s"]),
        "--output-dir",
        remote_output_template,
        "--edge",
        str(profile_value(profile, "edge", 0.040)),
        "--queue-share",
        str(profile_value(profile, "queue_share", 0.50)),
        "--target-qty",
        str(profile_value(profile, "target_qty", 5.0)),
        "--fill-haircut",
        str(profile_value(profile, "fill_haircut", 0.25)),
        "--max-seed-qty",
        str(profile_value(profile, "max_seed_qty", 60.0)),
        "--max-open-cost",
        str(profile_value(profile, "max_open_cost", 80.0)),
        "--seed-l1-cap",
        str(profile_value(profile, "seed_l1_cap", 1.02)),
        "--seed-px-lo",
        str(profile_value(profile, "seed_px_lo", 0.05)),
        "--seed-px-hi",
        str(profile_value(profile, "seed_px_hi", 0.90)),
        "--seed-offset-max-s",
        str(profile_value(profile, "seed_offset_max_s", 120)),
        "--order-ttl-s",
        str(profile_value(profile, "order_ttl_s", 120)),
        "--imbalance-qty-cap",
        str(profile_value(profile, "imbalance_qty_cap", 2.0)),
        "--imbalance-cost-cap",
        str(profile_value(profile, "imbalance_cost_cap", 1000000000.0)),
        "--surplus-budget-mode",
        str(profile_value(profile, "surplus_budget_mode", "block")),
        "--surplus-budget-bootstrap",
        str(profile_value(profile, "surplus_budget_bootstrap", 1.0)),
        "--surplus-budget-mult",
        str(profile_value(profile, "surplus_budget_mult", 0.5)),
        "--surplus-budget-max-abs-unpaired-cost",
        str(profile_value(profile, "surplus_budget_max_abs_unpaired_cost", 2.0)),
        "--taker-fee-rate",
        str(profile_value(profile, "taker_fee_rate", 0.07)),
        "--salvage-net-cap",
        str(profile["strict_rescue_salvage_net_cap"]),
        "--salvage-age-s",
        "30",
        "--salvage-min-lot-cost",
        "0.25",
        "--strict-rescue-mode",
        "source_audit",
        "--strict-rescue-l1-age-max-ms",
        str(profile["strict_rescue_l1_age_max_ms"]),
        "--strict-rescue-close-size-haircut",
        "1.0",
        "--strict-rescue-require-book-source",
        "--strict-rescue-require-l2-source",
        "--source-quality-require-trade-source",
        "--source-quality-require-l1-source",
        "--source-quality-l1-age-max-ms",
        "1000",
        "--source-quality-require-l2-source",
        "--risk-seed-closeability-soft-net-cap",
        str(profile["soft_cap"]),
        "--risk-seed-closeability-debt-floor",
        str(profile["debt_floor"]),
        "--risk-seed-closeability-debt-budget",
        str(profile["debt_budget"]),
        "--risk-seed-pending-opp-credit",
        str(profile_value(profile, "risk_seed_pending_opp_credit", 1.0)),
        "--write-normalized-lifecycle",
        "--write-rescue-block-diagnostics",
        "--allow-concurrent-shared-ingress-readers",
    ]
    append_option(remote_command, "--activation-mode", profile.get("activation_mode"))
    append_option(remote_command, "--activation-window-s", profile.get("activation_window_s"))
    append_option(remote_command, "--late-repair-after-s", profile.get("late_repair_after_s"))
    append_option(remote_command, "--pair-completion-net-cap", profile.get("pair_completion_net_cap"))
    append_option(
        remote_command,
        "--pair-completion-min-pair-pnl-after",
        profile.get("pair_completion_min_pair_pnl_after"),
    )
    if profile.get("pairing_only_when_residual") is True:
        remote_command.append("--pairing-only-when-residual")
    if profile.get("strict_rescue_skip_low_cost_lots") is True:
        remote_command.append("--strict-rescue-skip-low-cost-lots")
    postrun_command = [
        "python3",
        "scripts/xuan_no_order_third_window_postrun_bundle.py",
        "--third-output-root",
        local_output_template,
        "--tag",
        "<instance_id>",
        "--output-dir",
        f".tmp_xuan/local_verifier_artifacts/{instance_template}/postrun_bundle",
        "--scorecard-json",
        f".tmp_xuan/scorecards/{instance_template}_postrun_bundle.json",
        "--profile-scorecard",
        str(profile_path),
    ]
    if reset_repeat_roots:
        postrun_command.append("--no-default-prior-output-roots")

    return {
        "status": "THIRD_WINDOW_REMOTE_STAGING_MANIFEST_READY_LOCAL_ONLY",
        "profile_scorecard": str(profile_path),
        "profile_status": profile_status,
        "profile": profile,
        "target": {
            "ssh_host": args.ssh_host,
            "fixed_ip": args.fixed_ip,
            "remote_user": args.remote_user,
            "remote_repo": args.remote_repo,
            "shared_ingress_root": args.shared_ingress_root,
            "remote_tool_dir": args.remote_tool_dir,
            "remote_output_base": args.remote_output_base,
        },
        "instance": {
            "instance_id_template": instance_template,
            "remote_runner_template": remote_runner_template,
            "remote_output_dir_template": remote_output_template,
            "local_output_root_template": local_output_template,
        },
        "files_to_stage": files,
        "remote_command_template": shell_join(remote_command),
        "postrun_bundle_command_template": shell_join(postrun_command),
        "safety": {
            "orders_sent_allowed": False,
            "pm_dry_run_required": True,
            "deployable": False,
            "live_trading_allowed": False,
            "shared_service_mutation_allowed": False,
            "remote_repo_mutation_allowed": False,
            "deploy_or_restart_allowed": False,
            "collector_rebuild_allowed": False,
            "cron_loop_allowed": False,
        },
        "decision": {
            "local_manifest_ready": True,
            "remote_runner_allowed": False,
            "deployable": False,
            "requires_explicit_user_authorization": True,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-scorecard", default=".tmp_xuan/scorecards/no_order_soft_closeability_third_window_profile_20260522T1849Z.json")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--scorecard-json", required=True)
    parser.add_argument("--stage-files", nargs="+", default=DEFAULT_STAGE_FILES)
    parser.add_argument("--instance-prefix", default="xuan-frontier-soft-closeability-third-window")
    parser.add_argument("--ssh-host", default="ec2-52-209-13-135.eu-west-1.compute.amazonaws.com")
    parser.add_argument("--fixed-ip", default="52.209.13.135")
    parser.add_argument("--remote-user", default="ubuntu")
    parser.add_argument("--remote-repo", default="/srv/pm_as_ofi/repo")
    parser.add_argument("--shared-ingress-root", default="/srv/pm_as_ofi/shared-ingress-main")
    parser.add_argument("--shared-ingress-role", default="xuan-frontier-soft-closeability-runtime")
    parser.add_argument("--remote-tool-dir", default="/srv/pm_as_ofi/xuan-frontier-no-order-smoke-tools")
    parser.add_argument("--remote-output-base", default="/srv/pm_as_ofi/xuan-frontier-no-order-smoke")
    parser.add_argument("--market-prefix", default="btc-updown-5m")
    args = parser.parse_args()

    started = time.time()
    manifest = build(args)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / "THIRD_WINDOW_REMOTE_STAGING_MANIFEST.md"
    md_path.write_text(render_markdown(manifest) + "\n")
    scorecard = {
        "artifact": "xuan_no_order_third_window_remote_manifest_builder",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runtime_s": round(time.time() - started, 3),
        "script": "scripts/xuan_no_order_third_window_remote_manifest_builder.py",
        "markdown_path": str(md_path),
        **manifest,
    }
    scorecard_path = Path(args.scorecard_json).expanduser().resolve()
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_path.write_text(json.dumps(rounded(scorecard), indent=2, sort_keys=True) + "\n")
    print(json.dumps(rounded(scorecard), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
