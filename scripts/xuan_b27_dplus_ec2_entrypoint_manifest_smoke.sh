#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_ec2_entrypoint_manifest_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
manifest_dir="$out_dir/entrypoint_manifest_run"
manifest="$manifest_dir/manifest.json"
: > "$log"

run_check() {
  local name="$1"
  shift
  {
    printf '== %s ==\n' "$name"
    "$@"
  } >> "$log" 2>&1 || {
    status="FAIL"
    printf 'CHECK_FAILED %s\n' "$name" >> "$log"
  }
}

script="scripts/xuan_b27_dplus_ec2_entrypoint_manifest.py"

run_check "entrypoint_manifest_script_exists" test -x "$script"
run_check "entrypoint_manifest_py_compile" python3 -m py_compile "$script"
run_check "entrypoint_manifest_has_no_network_or_sync_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+socket|from[[:space:]]+socket[[:space:]]+import|subprocess|requests|websocket|systemctl|ssh|rsync|scp|urllib|http|tarfile|zipfile|shutil' '$script'"
run_check "entrypoint_manifest_run" "$script" --output-dir "$manifest_dir"
run_check "entrypoint_manifest_exists" test -f "$manifest"
run_check "entrypoint_manifest_safe" python3 - "$manifest" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
side_effects = data.get("side_effects", {})
required = data.get("required_entrypoint_files") or []
support = data.get("supporting_gate_files") or []
rust_hints = data.get("required_rust_module_hints") or []
all_records = required + support + rust_hints
required_paths = {record.get("path") for record in required}
expected_executable_files = set(data.get("expected_executable_files") or [])

must_have = {
    "Cargo.toml",
    "Cargo.lock",
    "src/bin/xuan_b27_dplus_user_ws_observer.rs",
    "scripts/xuan_b27_dplus_readonly_user_ws_observer.py",
    "scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py",
    "scripts/xuan_b27_dplus_shared_ingress_preflight.py",
    "scripts/xuan_b27_dplus_auth_source_preflight.py",
}

ok = (
    data.get("artifact") == "xuan_b27_dplus_ec2_entrypoint_manifest"
    and data.get("status") == "PASS"
    and data.get("scope") == "local_no_network_ec2_entrypoint_requirement_manifest"
    and data.get("requires_explicit_deployment_or_sync_approval") is True
    and data.get("required_remote_shared_ingress_root") == "/srv/pm_as_ofi/shared-ingress-main"
    and data.get("missing_required_files") == []
    and data.get("missing_executable_files") == []
    and data.get("forbidden_path_hits") == []
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and all(value is False for value in side_effects.values())
    and must_have.issubset(required_paths)
    and expected_executable_files
    and all(path.startswith("scripts/") for path in expected_executable_files)
    and all(
        record.get("exists") is True
        and record.get("is_file") is True
        and isinstance(record.get("sha256"), str)
        and len(record.get("sha256")) == 64
        for record in required
    )
    and all(
        record.get("expected_executable") is not True
        or (
            record.get("is_executable") is True
            and record.get("has_shebang") is True
            and isinstance(record.get("shebang"), str)
            and record.get("shebang", "").startswith("#!/usr/bin/env ")
        )
        for record in all_records
    )
    and all("xuan_research_artifacts/" not in str(record.get("path")) for record in all_records)
    and all(".env" not in str(record.get("path")) for record in all_records)
    and all("raw" not in str(record.get("path")) for record in all_records)
    and all("replay" not in str(record.get("path")) for record in all_records)
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_ec2_entrypoint_manifest_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_ec2_entrypoint_manifest_gate",
  "entrypoint_manifest": "$manifest",
  "orders_sent": false,
  "auth_network_started": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
