#!/usr/bin/env python3
"""Build and launch the standalone read-only xuan_b27_dplus User WS observer."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SENSITIVE_KEY_FRAGMENTS = (
    "KEY",
    "SECRET",
    "TOKEN",
    "PRIVATE",
    "PASSWORD",
    "AUTH",
    "COOKIE",
    "CREDENTIAL",
    "PASSPHRASE",
)


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def wrapper_exit_code(status: str, process_return_code: int, summary_return_code: int) -> int:
    if status == "SHARED_INGRESS_BROKER_UNAVAILABLE":
        return 69
    if status == "FAIL_READONLY_VIOLATION":
        return 2
    if status == "FAIL_NO_USER_WS_RECORDS":
        return 3
    if status == "FAIL_NO_USER_WS_CONNECTION":
        return 4
    if status == "FAIL_SUMMARY_GATE":
        return summary_return_code if summary_return_code != 0 else 70
    if status == "FAIL_PROCESS_EXIT_NONZERO":
        return process_return_code if process_return_code != 0 else 1
    return process_return_code


def load_dotenv_into_env(path: Path, env: dict[str, str]) -> list[str]:
    if not path.exists():
        return []
    loaded: list[str] = []
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        if not key or key in env:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        env[key] = value
        loaded.append(key)
    return loaded


def is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(fragment in upper for fragment in SENSITIVE_KEY_FRAGMENTS)


def dotenv_summary(loaded_keys: list[str]) -> dict[str, object]:
    return {
        "loaded": bool(loaded_keys),
        "loaded_key_count": len(loaded_keys),
        "loaded_sensitive_key_count": sum(1 for key in loaded_keys if is_sensitive_key(key)),
        "key_names_recorded": False,
        "secret_values_recorded": False,
    }


def env_summary(env: dict[str, str]) -> dict[str, object]:
    visible_keys = [
        key
        for key in env
        if key.startswith("PM_") or key == "POLYMARKET_MARKET_SLUG" or key == "RUST_LOG"
    ]
    return {
        "visible_key_count": len(visible_keys),
        "visible_sensitive_key_count": sum(1 for key in visible_keys if is_sensitive_key(key)),
        "key_names_recorded": False,
        "secret_values_recorded": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--approved-readonly-user-ws",
        action="store_true",
        help="Required. Confirms this exact read-only User WS observer run was approved.",
    )
    parser.add_argument("--duration-seconds", type=int, default=1800)
    parser.add_argument("--market-slug", default="btc-updown-5m")
    parser.add_argument(
        "--shared-ingress-root",
        type=Path,
        default=Path("/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main"),
    )
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    label = utc_label()
    out_dir = (
        args.output_dir
        or root / "xuan_research_artifacts" / f"xuan_b27_dplus_user_ws_observer_{label}"
    )
    logs_dir = out_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    wrapper_manifest = out_dir / "wrapper_manifest.json"

    if not args.approved_readonly_user_ws:
        write_json(
            wrapper_manifest,
            {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_readonly_user_ws_observer_wrapper",
                "status": "REFUSED_NO_EXPLICIT_APPROVAL",
                "created_utc": label,
                "orders_allowed": False,
                "reason": "missing --approved-readonly-user-ws",
            },
        )
        print("Refusing to start: --approved-readonly-user-ws is required.", file=sys.stderr)
        return 64

    if args.duration_seconds <= 0 or args.duration_seconds > 3600:
        write_json(
            wrapper_manifest,
            {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_readonly_user_ws_observer_wrapper",
                "status": "REFUSED_INVALID_DURATION",
                "created_utc": label,
                "orders_allowed": False,
                "duration_seconds": args.duration_seconds,
            },
        )
        print("Refusing to start: duration must be in 1..3600 seconds.", file=sys.stderr)
        return 64

    env = os.environ.copy()
    dotenv_loaded_entries = load_dotenv_into_env(root / ".env", env)
    dotenv_loaded_summary = dotenv_summary(dotenv_loaded_entries)

    broker_preflight_dir = out_dir / "broker_preflight"
    broker_preflight_cmd = [
        sys.executable,
        str(root / "scripts" / "xuan_b27_dplus_shared_ingress_preflight.py"),
        "--root",
        str(args.shared_ingress_root),
        "--output-dir",
        str(broker_preflight_dir),
    ]
    broker_preflight = subprocess.run(broker_preflight_cmd, cwd=root, env=env)
    broker_preflight_manifest = broker_preflight_dir / "manifest.json"
    broker_preflight_status = "UNKNOWN"
    broker_preflight_reason = "manifest_missing"
    if broker_preflight_manifest.exists():
        try:
            broker_preflight_data = json.loads(broker_preflight_manifest.read_text())
            broker_preflight_status = str(broker_preflight_data.get("status") or "UNKNOWN")
            broker_preflight_reason = str(broker_preflight_data.get("reason") or "")
        except Exception as exc:
            broker_preflight_reason = f"manifest_parse_failed: {exc}"
    if broker_preflight.returncode != 0 or broker_preflight_status != "OK":
        write_json(
            wrapper_manifest,
            {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_readonly_user_ws_observer_wrapper",
                "status": "SHARED_INGRESS_BROKER_UNAVAILABLE",
                "created_utc": label,
                "strategy": "xuan_b27_dplus",
                "mode": "readonly_user_ws_observer",
                "orders_allowed": False,
                "cancels_allowed": False,
                "redeems_allowed": False,
                "shared_ingress_role": "client",
                "shared_ingress_root": str(args.shared_ingress_root),
                "broker_preflight_cmd": broker_preflight_cmd,
                "broker_preflight_return_code": broker_preflight.returncode,
                "broker_preflight_manifest": str(broker_preflight_manifest),
                "broker_preflight_status": broker_preflight_status,
                "broker_preflight_reason": broker_preflight_reason,
                "dotenv_loaded": dotenv_loaded_summary,
            },
        )
        print(out_dir / "wrapper_manifest.json")
        return 69

    build_log = logs_dir / "cargo_build_xuan_b27_dplus_user_ws_observer.log"
    build_cmd = ["cargo", "build", "--bin", "xuan_b27_dplus_user_ws_observer"]
    with build_log.open("w") as fh:
        build = subprocess.run(build_cmd, cwd=root, env=env, stdout=fh, stderr=subprocess.STDOUT)
    if build.returncode != 0:
        write_json(
            wrapper_manifest,
            {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_readonly_user_ws_observer_wrapper",
                "status": "BUILD_FAILED",
                "created_utc": label,
                "orders_allowed": False,
                "build_log": str(build_log),
            },
        )
        return 65

    binary = root / "target" / "debug" / "xuan_b27_dplus_user_ws_observer"
    if not binary.exists():
        write_json(
            wrapper_manifest,
            {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_readonly_user_ws_observer_wrapper",
                "status": "BINARY_MISSING_AFTER_BUILD",
                "created_utc": label,
                "orders_allowed": False,
                "binary": str(binary),
                "build_log": str(build_log),
            },
        )
        return 66

    run_cmd = [
        str(binary),
        "--approved-readonly-user-ws",
        "--duration-secs",
        str(args.duration_seconds),
        "--market-slug",
        args.market_slug,
        "--shared-ingress-root",
        str(args.shared_ingress_root),
        "--output-dir",
        str(out_dir),
    ]
    env.update(
        {
            "PM_DRY_RUN": "true",
            "PM_STRATEGY": "xuan_b27_dplus",
            "PM_XUAN_B27_DPLUS_MODE": "auth_observer",
            "PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS": "0",
            "PM_SHARED_INGRESS_ROLE": "client",
            "PM_SHARED_INGRESS_ROOT": str(args.shared_ingress_root),
            "PM_RECORDER_ENABLED": "true",
            "PM_MULTI_MARKET_PREFIXES": args.market_slug,
            "PM_ORACLE_LAG_SYMBOL_UNIVERSE": args.market_slug.split("-updown-", 1)[0],
        }
    )

    write_json(
        wrapper_manifest,
        {
            "schema_version": 1,
            "artifact": "xuan_b27_dplus_readonly_user_ws_observer_wrapper",
            "status": "STARTING",
            "created_utc": label,
            "strategy": "xuan_b27_dplus",
            "mode": "readonly_user_ws_observer",
            "orders_allowed": False,
            "cancels_allowed": False,
            "redeems_allowed": False,
            "build_log": str(build_log),
            "binary": str(binary),
            "binary_mtime_ns": binary.stat().st_mtime_ns,
            "binary_size": binary.stat().st_size,
            "shared_ingress_role": "client",
            "shared_ingress_root": str(args.shared_ingress_root),
            "broker_preflight_manifest": str(broker_preflight_manifest),
            "dotenv_loaded": dotenv_loaded_summary,
            "env_snapshot": env_summary(env),
            "run_cmd": run_cmd,
        },
    )

    stdout_log = logs_dir / "readonly_user_ws_stdout.log"
    stderr_log = logs_dir / "readonly_user_ws_stderr.log"
    with stdout_log.open("w") as out_fh, stderr_log.open("w") as err_fh:
        proc = subprocess.run(run_cmd, cwd=root, env=env, stdout=out_fh, stderr=err_fh)

    status = "PASS_READONLY_USER_WS_OBSERVER" if proc.returncode == 0 else "FAIL_PROCESS_EXIT_NONZERO"
    run_manifest = out_dir / "run_manifest.json"
    if run_manifest.exists():
        try:
            run_status = json.loads(run_manifest.read_text()).get("status")
            if run_status == "SHARED_INGRESS_BROKER_UNAVAILABLE":
                status = "SHARED_INGRESS_BROKER_UNAVAILABLE"
        except Exception:
            pass
    summary_path = out_dir / "readonly_user_ws_summary.json"
    summary_cmd = [
        sys.executable,
        str(root / "scripts" / "xuan_b27_dplus_summarize_readonly_user_ws_run.py"),
        str(out_dir),
        "--output",
        str(summary_path),
        "--check-readonly",
        "--require-user-ws-connection",
    ]
    summary = subprocess.run(summary_cmd, cwd=root, env=env)
    if summary.returncode == 2:
        status = "FAIL_READONLY_VIOLATION"
    elif summary.returncode == 3:
        status = "FAIL_NO_USER_WS_RECORDS"
    elif summary.returncode == 4:
        status = "FAIL_NO_USER_WS_CONNECTION"
    elif summary.returncode != 0 and status == "PASS_READONLY_USER_WS_OBSERVER":
        status = "FAIL_SUMMARY_GATE"
    write_json(
        wrapper_manifest,
        {
            "schema_version": 1,
            "artifact": "xuan_b27_dplus_readonly_user_ws_observer_wrapper",
            "status": status,
            "created_utc": label,
            "completed_utc": utc_label(),
            "strategy": "xuan_b27_dplus",
            "mode": "readonly_user_ws_observer",
            "orders_allowed": False,
            "cancels_allowed": False,
            "redeems_allowed": False,
            "return_code": proc.returncode,
            "build_log": str(build_log),
            "stdout_log": str(stdout_log),
            "stderr_log": str(stderr_log),
            "run_manifest": str(run_manifest),
            "summary_cmd": summary_cmd,
            "summary_return_code": summary.returncode,
            "readonly_user_ws_summary": str(summary_path),
        },
    )
    if status == "SHARED_INGRESS_BROKER_UNAVAILABLE":
        return 69
    return wrapper_exit_code(status, proc.returncode, summary.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
