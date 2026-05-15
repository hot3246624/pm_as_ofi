#!/usr/bin/env python3
"""Small whitelist runner for xuan frontier research jobs.

This runner intentionally does not accept shell commands.  A job JSON selects a
known `kind`, the runner writes a dedicated output directory, and every artifact
lands under the configured runs root.  It is meant for the research server, not
for production service control.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any


DEFAULT_RUNS_ROOT = Path("/home/ubuntu/xuan_frontier_runs")
DEFAULT_JOB_RUNS_DIR = DEFAULT_RUNS_ROOT / "job_runs"
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{2,96}$")


class JobError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text())
    except Exception as exc:  # noqa: BLE001 - report compactly in status.
        raise JobError(f"cannot read json {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise JobError(f"job json must be an object: {path}")
    return data


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def validate_job_id(value: Any) -> str:
    if not isinstance(value, str) or not JOB_ID_RE.match(value):
        raise JobError("job_id must match ^[A-Za-z0-9][A-Za-z0-9_.-]{2,96}$")
    return value


def within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def require_within(path_s: str, root: Path) -> Path:
    path = Path(path_s).expanduser()
    if not path.is_absolute():
        path = root / path
    path = path.resolve()
    if not within(path, root):
        raise JobError(f"path escapes allowed root {root}: {path}")
    return path


def summarize_json(path: Path) -> dict[str, Any]:
    data = load_json(path)
    summary: dict[str, Any] = {
        "path": str(path),
        "top_level_keys": sorted(data.keys())[:50],
    }
    if isinstance(data.get("candidate_specs"), list):
        summary["candidate_count"] = len(data["candidate_specs"])
        summary["candidate_names"] = [
            item.get("name")
            for item in data["candidate_specs"][:20]
            if isinstance(item, dict)
        ]
    if isinstance(data.get("top_rows"), list):
        summary["top_rows_count"] = len(data["top_rows"])
        summary["top_rows_preview"] = data["top_rows"][:5]
    if isinstance(data.get("out"), str):
        summary["out"] = data["out"]
    return summary


def run_inspect_artifacts(job: dict[str, Any], runs_root: Path, out_dir: Path) -> dict[str, Any]:
    params = job.get("params", {})
    if not isinstance(params, dict):
        raise JobError("params must be an object")
    paths = params.get("paths", [])
    if not isinstance(paths, list) or not paths:
        raise JobError("inspect_artifacts requires params.paths")

    inspected = []
    for item in paths:
        if not isinstance(item, str):
            raise JobError("params.paths items must be strings")
        path = require_within(item, runs_root)
        entry: dict[str, Any] = {
            "path": str(path),
            "exists": path.exists(),
            "kind": "dir" if path.is_dir() else "file" if path.is_file() else "missing",
        }
        if path.exists():
            entry["size_bytes"] = path.stat().st_size
        if path.is_file() and path.suffix == ".json":
            entry["json_summary"] = summarize_json(path)
        elif path.is_dir():
            files = sorted(p for p in path.rglob("*") if p.is_file())[:80]
            entry["file_count_sample"] = len(files)
            entry["files_sample"] = [str(p.relative_to(path)) for p in files[:40]]
        inspected.append(entry)
    return {"inspected": inspected}


def run_verifier_spec_request(job: dict[str, Any], runs_root: Path, out_dir: Path) -> dict[str, Any]:
    params = job.get("params", {})
    if not isinstance(params, dict):
        raise JobError("params must be an object")
    spec_path_s = params.get("spec_path")
    if not isinstance(spec_path_s, str):
        raise JobError("verifier_spec_request requires params.spec_path")
    spec_path = require_within(spec_path_s, runs_root)
    if not spec_path.is_file():
        raise JobError(f"spec_path not found: {spec_path}")
    spec = load_json(spec_path)
    dest = out_dir / spec_path.name
    shutil.copy2(spec_path, dest)
    result = {
        "state": "waiting_external_verifier",
        "spec_path": str(spec_path),
        "copied_spec": str(dest),
        "candidate_count": len(spec.get("candidate_specs", []))
        if isinstance(spec.get("candidate_specs"), list)
        else 0,
        "candidate_names": [
            item.get("name")
            for item in spec.get("candidate_specs", [])[:20]
            if isinstance(item, dict)
        ]
        if isinstance(spec.get("candidate_specs"), list)
        else [],
        "note": "This job records a verifier request only. Source-of-truth verification must be run by the authorized verifier queue.",
    }
    md_path_s = params.get("markdown_path")
    if isinstance(md_path_s, str):
        md_path = require_within(md_path_s, runs_root)
        if md_path.is_file():
            shutil.copy2(md_path, out_dir / md_path.name)
            result["copied_markdown"] = str(out_dir / md_path.name)
    return result


def run_dplus_minorder_fillhaircut(job: dict[str, Any], runs_root: Path, out_dir: Path) -> dict[str, Any]:
    params = job.get("params", {})
    if not isinstance(params, dict):
        raise JobError("params must be an object")
    script = runs_root / "run_xuan_dplus_minorder_fillhaircut_dayshard_remote.sh"
    if not script.is_file():
        raise JobError(f"missing whitelist script: {script}")
    result_dir_s = params.get("result_dir", str(out_dir / "run"))
    result_dir = require_within(result_dir_s, runs_root)
    result_dir.mkdir(parents=True, exist_ok=True)

    stdout_path = out_dir / "subprocess.stdout.log"
    stderr_path = out_dir / "subprocess.stderr.log"
    with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
        proc = subprocess.run(
            ["bash", str(script), str(result_dir)],
            cwd=str(runs_root),
            stdout=stdout,
            stderr=stderr,
            text=True,
            check=False,
        )
    if proc.returncode != 0:
        raise JobError(f"whitelist script failed with code {proc.returncode}; see {stderr_path}")

    summary = result_dir / "minorder_fillhaircut_full_summary.json"
    return {
        "state": "completed",
        "result_dir": str(result_dir),
        "summary_path": str(summary) if summary.exists() else None,
        "summary_preview": json.loads(summary.read_text())[:12] if summary.exists() else [],
    }


HANDLERS = {
    "inspect_artifacts": run_inspect_artifacts,
    "verifier_spec_request": run_verifier_spec_request,
    "dplus_minorder_fillhaircut_full": run_dplus_minorder_fillhaircut,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one xuan research job from a whitelist.")
    parser.add_argument("--job", required=True, help="Path to job JSON.")
    parser.add_argument("--runs-root", default=str(DEFAULT_RUNS_ROOT))
    parser.add_argument("--job-runs-dir", default=str(DEFAULT_JOB_RUNS_DIR))
    parser.add_argument("--force", action="store_true", help="Replace an existing job run dir.")
    args = parser.parse_args()

    runs_root = Path(args.runs_root).resolve()
    job_runs_dir = Path(args.job_runs_dir).resolve()
    if not within(job_runs_dir, runs_root):
        raise SystemExit(f"job-runs-dir must be inside runs-root: {job_runs_dir}")

    job_path = Path(args.job).expanduser().resolve()
    job = load_json(job_path)
    job_id = validate_job_id(job.get("job_id"))
    kind = job.get("kind")
    if kind not in HANDLERS:
        raise SystemExit(f"unsupported job kind {kind!r}; supported={sorted(HANDLERS)}")

    out_dir = job_runs_dir / job_id
    if out_dir.exists() and args.force:
        shutil.rmtree(out_dir)
    if out_dir.exists():
        raise SystemExit(f"job run already exists: {out_dir}")
    out_dir.mkdir(parents=True)

    status = {
        "job_id": job_id,
        "kind": kind,
        "job_path": str(job_path),
        "out_dir": str(out_dir),
        "started_at_utc": utc_now(),
        "state": "running",
    }
    write_json(out_dir / "status.json", status)
    shutil.copy2(job_path, out_dir / "job.json")

    try:
        result = HANDLERS[kind](job, runs_root, out_dir)
        write_json(out_dir / "result.json", result)
        status.update(
            {
                "state": result.get("state", "succeeded"),
                "finished_at_utc": utc_now(),
                "result_path": str(out_dir / "result.json"),
            }
        )
        write_json(out_dir / "status.json", status)
        print(json.dumps(status, indent=2, sort_keys=True))
        return 0
    except Exception as exc:  # noqa: BLE001 - status must capture all failures.
        (out_dir / "error.txt").write_text("".join(traceback.format_exception(exc)))
        status.update(
            {
                "state": "failed",
                "finished_at_utc": utc_now(),
                "error": str(exc),
                "error_path": str(out_dir / "error.txt"),
            }
        )
        write_json(out_dir / "status.json", status)
        print(json.dumps(status, indent=2, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
