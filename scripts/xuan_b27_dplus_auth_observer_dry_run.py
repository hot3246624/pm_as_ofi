#!/usr/bin/env python3
"""Launch an explicitly approved no-order xuan_b27_dplus observer dry-run."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def utc_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_dotenv_into_env(path: Path, env: dict[str, str]) -> list[str]:
    if not path.exists():
        return []
    loaded = []
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


def safe_env(env: dict[str, str]) -> dict[str, str]:
    allowed_prefixes = (
        "PM_",
        "POLYMARKET_MARKET_SLUG",
        "RUST_LOG",
    )
    redacted_fragments = (
        "KEY",
        "SECRET",
        "TOKEN",
        "PRIVATE",
        "PASSWORD",
        "AUTH",
        "COOKIE",
        "CREDENTIAL",
    )
    out = {}
    for key, value in sorted(env.items()):
        if not (key.startswith(allowed_prefixes) or key == "POLYMARKET_MARKET_SLUG"):
            continue
        if any(fragment in key.upper() for fragment in redacted_fragments):
            out[key] = "<redacted>"
        else:
            out[key] = value
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--approved-no-order-observer",
        action="store_true",
        help="Required. Confirms the main thread explicitly approved this exact observer run.",
    )
    parser.add_argument("--duration-seconds", type=int, default=1800)
    parser.add_argument("--market-slug", default="btc-updown-5m")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--shared-ingress-root",
        type=Path,
        default=Path("/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main"),
        help="Existing shared-ingress broker root to use as client.",
    )
    parser.add_argument(
        "--allow-standalone-market-ws",
        action="store_true",
        help="Explicitly allow a standalone public market WS instead of requiring an existing shared-ingress broker.",
    )
    parser.add_argument(
        "--approved-readonly-user-ws",
        action="store_true",
        help="Optional. Enables authenticated User WS in dry-run observer mode; still sends no orders/cancels/redeems.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.approved_no_order_observer:
        print("Refusing to start: --approved-no-order-observer is required.", file=sys.stderr)
        return 64
    if args.duration_seconds <= 0 or args.duration_seconds > 3600:
        print("Refusing to start: duration must be in 1..3600 seconds.", file=sys.stderr)
        return 64

    label = utc_label()
    root = Path(__file__).resolve().parents[1]
    binary = root / "target" / "debug" / "polymarket_v2"
    env = os.environ.copy()
    dotenv_path = root / ".env"
    dotenv_loaded_keys = load_dotenv_into_env(dotenv_path, env)
    out_dir = args.output_dir or root / "xuan_research_artifacts" / f"xuan_b27_dplus_auth_observer_{label}"
    logs_dir = out_dir / "logs"
    recorder_root = out_dir / "recorder"
    logs_dir.mkdir(parents=True, exist_ok=True)
    recorder_root.mkdir(parents=True, exist_ok=True)
    shared_ingress_root = args.shared_ingress_root
    shared_ingress_role = "standalone" if args.allow_standalone_market_ws else "client"

    if not args.allow_standalone_market_ws:
        broker_manifest = shared_ingress_root / "broker_manifest.json"
        market_socket = shared_ingress_root / "market.sock"
        shared_ingress_ready = False
        shared_ingress_reason = "broker_manifest_missing"
        if broker_manifest.exists():
            try:
                manifest = json.loads(broker_manifest.read_text())
                age_ms = int(time.time() * 1000) - int(manifest.get("last_heartbeat_ms", 0))
                market_socket = Path(manifest.get("market_socket") or market_socket)
                if age_ms <= 10_000 and market_socket.exists():
                    shared_ingress_ready = True
                    shared_ingress_reason = "healthy"
                else:
                    shared_ingress_reason = f"stale_or_socket_missing age_ms={age_ms}"
            except Exception as exc:
                shared_ingress_reason = f"broker_manifest_invalid {exc}"
        if not shared_ingress_ready:
            run_manifest = {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_auth_observer_dry_run",
                "status": "SHARED_INGRESS_BROKER_UNAVAILABLE",
                "created_utc": label,
                "completed_utc": utc_label(),
                "strategy": "xuan_b27_dplus",
                "mode": "auth_observer",
                "dry_run": True,
                "readonly_user_ws_requested": args.approved_readonly_user_ws,
                "orders_allowed": False,
                "cancels_allowed": False,
                "redeems_allowed": False,
                "shared_ingress_role": shared_ingress_role,
                "shared_ingress_root": str(shared_ingress_root),
                "shared_ingress_reason": shared_ingress_reason,
                "standalone_market_ws_allowed": False,
                "output_dir": str(out_dir),
            }
            write_json(out_dir / "run_manifest.json", run_manifest)
            print(out_dir / "run_manifest.json")
            return 69

    if args.approved_readonly_user_ws:
        required_user_ws_env = (
            "POLYMARKET_API_KEY",
            "POLYMARKET_API_SECRET",
            "POLYMARKET_API_PASSPHRASE",
        )
        present_user_ws_env = [key for key in required_user_ws_env if env.get(key, "").strip()]
        if present_user_ws_env and len(present_user_ws_env) != len(required_user_ws_env):
            run_manifest = {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_auth_observer_dry_run",
                "status": "READONLY_USER_WS_CREDENTIALS_PARTIAL",
                "created_utc": label,
                "completed_utc": utc_label(),
                "strategy": "xuan_b27_dplus",
                "mode": "auth_observer",
                "dry_run": True,
                "readonly_user_ws_requested": True,
                "orders_allowed": False,
                "cancels_allowed": False,
                "redeems_allowed": False,
                "present_env": present_user_ws_env,
                "missing_env": [
                    key for key in required_user_ws_env if not env.get(key, "").strip()
                ],
                "dotenv_path": str(dotenv_path),
                "dotenv_loaded": bool(dotenv_loaded_keys),
                "output_dir": str(out_dir),
            }
            write_json(out_dir / "run_manifest.json", run_manifest)
            print(out_dir / "run_manifest.json")
            return 67
        if not present_user_ws_env and not env.get("POLYMARKET_PRIVATE_KEY", "").strip():
            run_manifest = {
                "schema_version": 1,
                "artifact": "xuan_b27_dplus_auth_observer_dry_run",
                "status": "READONLY_USER_WS_AUTH_SOURCE_MISSING",
                "created_utc": label,
                "completed_utc": utc_label(),
                "strategy": "xuan_b27_dplus",
                "mode": "auth_observer",
                "dry_run": True,
                "readonly_user_ws_requested": True,
                "orders_allowed": False,
                "cancels_allowed": False,
                "redeems_allowed": False,
                "missing_env": ["POLYMARKET_PRIVATE_KEY"],
                "api_credentials": "absent",
                "credential_derivation": "not_possible_without_private_key",
                "dotenv_path": str(dotenv_path),
                "dotenv_loaded": bool(dotenv_loaded_keys),
                "output_dir": str(out_dir),
            }
            write_json(out_dir / "run_manifest.json", run_manifest)
            print(out_dir / "run_manifest.json")
            return 67

    build_cmd = ["cargo", "build", "--bin", "polymarket_v2"]
    build = subprocess.run(build_cmd, cwd=str(root), text=True, capture_output=True)
    (logs_dir / "cargo_build.stdout").write_text(build.stdout)
    (logs_dir / "cargo_build.stderr").write_text(build.stderr)
    if build.returncode != 0:
        run_manifest = {
            "schema_version": 1,
            "artifact": "xuan_b27_dplus_auth_observer_dry_run",
            "status": "BUILD_FAILED",
            "created_utc": label,
            "completed_utc": utc_label(),
            "strategy": "xuan_b27_dplus",
            "mode": "auth_observer",
            "dry_run": True,
            "orders_allowed": False,
            "cancels_allowed": False,
            "redeems_allowed": False,
            "build_command": build_cmd,
            "build_return_code": build.returncode,
            "output_dir": str(out_dir),
        }
        write_json(out_dir / "run_manifest.json", run_manifest)
        print(out_dir / "run_manifest.json")
        return 65
    if not binary.exists():
        run_manifest = {
            "schema_version": 1,
            "artifact": "xuan_b27_dplus_auth_observer_dry_run",
            "status": "BINARY_MISSING_AFTER_BUILD",
            "created_utc": label,
            "completed_utc": utc_label(),
            "strategy": "xuan_b27_dplus",
            "mode": "auth_observer",
            "dry_run": True,
            "orders_allowed": False,
            "cancels_allowed": False,
            "redeems_allowed": False,
            "build_command": build_cmd,
            "build_return_code": build.returncode,
            "output_dir": str(out_dir),
        }
        write_json(out_dir / "run_manifest.json", run_manifest)
        print(out_dir / "run_manifest.json")
        return 66
    binary_stat = binary.stat()

    env.update(
        {
            "PM_DRY_RUN": "true",
            "PM_STRATEGY": "xuan_b27_dplus",
            "PM_XUAN_B27_DPLUS_MODE": "auth_observer",
            "PM_XUAN_B27_DPLUS_MARKET_SLUG": args.market_slug,
            "POLYMARKET_MARKET_SLUG": args.market_slug,
            "PM_MULTI_MARKET_PREFIXES": args.market_slug,
            "PM_ORACLE_LAG_SYMBOL_UNIVERSE": args.market_slug.split("-updown-", 1)[0],
            "PM_XUAN_B27_DPLUS_EDGE": "0.040",
            "PM_XUAN_B27_DPLUS_TARGET_QTY": "5",
            "PM_XUAN_B27_DPLUS_SEED_PX_LO": "0.010",
            "PM_XUAN_B27_DPLUS_SEED_PX_HI": "0.990",
            "PM_XUAN_B27_DPLUS_IMBALANCE_QTY_CAP": "2",
            "PM_XUAN_B27_DPLUS_SALVAGE_NET_CAP": "0.950",
            "PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC": "50",
            "PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC": "100",
            "PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS": "0",
            "PM_XUAN_B27_DPLUS_POST_ONLY": "true",
            "PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER": "false",
            "PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN": "true",
            "PM_XUAN_B27_DPLUS_USER_WS_IN_DRY_RUN": "true"
            if args.approved_readonly_user_ws
            else "false",
            "PM_RECORDER_ENABLED": "true",
            "PM_RECORDER_MARKET_MODE": "structured",
            "PM_RECORDER_ROOT": str(recorder_root),
            "PM_LOG_ROOT": str(logs_dir),
            "PM_INSTANCE_ID": f"xuan-b27-dplus-auth-observer-{label}",
            "PM_SHARED_INGRESS_ROLE": shared_ingress_role,
            "PM_SHARED_INGRESS_ROOT": str(shared_ingress_root),
            "PM_AUTO_CLAIM": "false",
            "PM_AUTO_CLAIM_DRY_RUN": "true",
            "PM_CLAIM_MONITOR": "false",
            "PM_PGT_SHADOW_REDEEM_LIFECYCLE_ENABLED": "false",
            "PM_DRY_RUN_FILL_PROBABILITY": "0",
            "RUST_LOG": env.get("RUST_LOG", "info"),
        }
    )

    log_file = logs_dir / "polymarket_v2.log"
    start_manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_auth_observer_dry_run_start",
        "status": "STARTED",
        "created_utc": label,
        "strategy": "xuan_b27_dplus",
        "mode": "auth_observer",
        "dry_run": True,
        "readonly_user_ws_requested": args.approved_readonly_user_ws,
        "auth_user_ws_expected": args.approved_readonly_user_ws,
        "orders_allowed": False,
        "cancels_allowed": False,
        "redeems_allowed": False,
        "duration_seconds": args.duration_seconds,
        "market_slug_filter": args.market_slug,
        "multi_market_prefixes": args.market_slug,
        "shared_ingress_role": shared_ingress_role,
        "shared_ingress_root": str(shared_ingress_root),
        "standalone_market_ws_allowed": args.allow_standalone_market_ws,
        "command": [str(binary)],
        "build_command": build_cmd,
        "build_return_code": build.returncode,
        "binary_mtime_ns": binary_stat.st_mtime_ns,
        "binary_size_bytes": binary_stat.st_size,
        "dotenv_path": str(dotenv_path),
        "dotenv_loaded": bool(dotenv_loaded_keys),
        "output_dir": str(out_dir),
        "recorder_root": str(recorder_root),
        "log_file": str(log_file),
        "dotenv_path": str(dotenv_path),
        "dotenv_loaded": bool(dotenv_loaded_keys),
        "safe_env": safe_env(env),
    }
    write_json(out_dir / "start_manifest.json", start_manifest)

    started = time.monotonic()
    proc = subprocess.Popen(
        [str(binary)],
        cwd=str(root),
        env=env,
        stdout=log_file.open("ab"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    (out_dir / "pid.txt").write_text(str(proc.pid) + "\n")

    timed_out = False
    try:
        proc.wait(timeout=args.duration_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)

    elapsed = round(time.monotonic() - started, 3)
    completed = utc_label()
    run_manifest = {
        "schema_version": 1,
        "artifact": "xuan_b27_dplus_auth_observer_dry_run",
        "status": "TIMEOUT_TERMINATED" if timed_out else "PROCESS_EXITED",
        "created_utc": label,
        "completed_utc": completed,
        "duration_seconds_requested": args.duration_seconds,
        "elapsed_seconds": elapsed,
        "return_code": proc.returncode,
        "timed_out": timed_out,
        "strategy": "xuan_b27_dplus",
        "mode": "auth_observer",
        "dry_run": True,
        "readonly_user_ws_requested": args.approved_readonly_user_ws,
        "auth_user_ws_expected": args.approved_readonly_user_ws,
        "auth_user_ws_note": (
            "Read-only User WS requested in dry-run observer mode; orders/cancels/redeems remain disabled."
            if args.approved_readonly_user_ws
            else "User WS disabled; this run validates observer/recorder/no-order behavior only."
        ),
        "orders_allowed": False,
        "cancels_allowed": False,
        "redeems_allowed": False,
        "shared_ingress_role": shared_ingress_role,
        "shared_ingress_root": str(shared_ingress_root),
        "standalone_market_ws_allowed": args.allow_standalone_market_ws,
        "output_dir": str(out_dir),
        "recorder_root": str(recorder_root),
        "log_file": str(log_file),
    }
    write_json(out_dir / "run_manifest.json", run_manifest)

    summary_cmd = [
        sys.executable,
        str(root / "scripts" / "xuan_b27_dplus_summarize_observer_run.py"),
        str(out_dir),
        "--output",
        str(out_dir / "observer_summary.json"),
        "--check-no-order",
        "--require-tracked-candidates",
        "--require-market-prefix",
        args.market_slug,
    ]
    summary = subprocess.run(summary_cmd, cwd=str(root), text=True, capture_output=True)
    (out_dir / "observer_summary.stdout").write_text(summary.stdout)
    (out_dir / "observer_summary.stderr").write_text(summary.stderr)
    run_manifest["observer_summary_return_code"] = summary.returncode
    run_manifest["observer_summary_path"] = str(out_dir / "observer_summary.json")
    run_manifest["process_return_code_gate"] = (
        "PASS_TIMEOUT_OR_ZERO" if timed_out or proc.returncode == 0 else "FAIL_PROCESS_EXIT_NONZERO"
    )
    write_json(out_dir / "run_manifest.json", run_manifest)

    print(out_dir / "run_manifest.json")
    if proc.returncode not in (0, -signal.SIGTERM) and not timed_out:
        return 68
    return summary.returncode


if __name__ == "__main__":
    raise SystemExit(main())
