#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke_$ts}"
mkdir -p "$out_dir"

status="PASS"
log="$out_dir/checks.log"
manifest_dir="$out_dir/payload_manifest_run"
payload_manifest="$manifest_dir/manifest.json"
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

script="scripts/xuan_b27_dplus_g2_canary_executor_payload_manifest.py"

run_check "payload_manifest_script_exists" test -x "$script"
run_check "payload_manifest_py_compile" python3 -m py_compile "$script"
run_check "payload_manifest_has_no_network_process_or_archive_imports" \
  bash -c "! rg -q '(^|[[:space:]])import[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)|from[[:space:]]+(socket|subprocess|requests|websocket|urllib|http|tarfile|zipfile|shutil)[[:space:]]+import|systemctl|run_shared_ingress|os\\.system|Popen|check_call|check_output|(^|[^[:alnum:]_])(ssh|rsync|scp)([[:space:]/-]|$)' '$script'"
run_check "payload_manifest_run" "$script" --output-dir "$manifest_dir"
run_check "payload_manifest_exists" test -f "$payload_manifest"
run_check "payload_manifest_safe" python3 - "$payload_manifest" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
records = data.get("payload_records") or []
paths = {record.get("path") for record in records}
side_effects = data.get("side_effects") or {}
must_have = {
    "Cargo.toml",
    "Cargo.lock",
    "src/bin/polymarket_v2.rs",
    "src/polymarket/coordinator_xuan_b27_dplus.rs",
    "src/polymarket/xuan_b27_dplus_oms_adapter.rs",
    "scripts/xuan_b27_dplus_g2_canary_executor.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_contract.py",
    "scripts/xuan_b27_dplus_g2_canary_executor_payload_manifest.py",
    "scripts/xuan_b27_dplus_g2_canary_approval_envelope.py",
    "scripts/xuan_b27_dplus_summarize_g2_canary_run.py",
    "scripts/xuan_b27_dplus_extract_shadow_edge_samples.py",
    "scripts/xuan_b27_dplus_l1_dry_run_outcome_labels.py",
    "scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py",
    "scripts/xuan_b27_dplus_shadow_acceptance_input_discovery.py",
    "scripts/xuan_b27_dplus_realized_outcome_labels.py",
    "scripts/xuan_b27_dplus_outcome_label_bridge.py",
    "scripts/xuan_b27_dplus_shadow_performance_evidence.py",
    "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py",
    "scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py",
    "docs/research/xuan/DPLUS_G2_CANARY_RUNBOOK_ZH.md",
}
forbidden_fragments = (
    ".env",
    "xuan_research_artifacts/",
    "target/",
    ".git/",
    "/raw/",
    "raw/",
    "replay",
    "/mnt/poly-replay",
    "/srv/pm_as_ofi",
)
ok = (
    data.get("artifact") == "xuan_b27_dplus_g2_canary_executor_payload_manifest"
    and data.get("status") == "PASS"
    and data.get("scope") == "local_no_network_g2_canary_executor_payload_manifest"
    and data.get("strategy") == "xuan_b27_dplus"
    and data.get("payload_file_count") == len(records)
    and data.get("payload_file_count", 0) >= 40
    and data.get("missing_required_files") == []
    and data.get("missing_executable_files") == []
    and data.get("forbidden_path_hits") == []
    and data.get("duplicate_paths") == []
    and data.get("requires_reviewed_executor_implementation") is True
    and data.get("requires_exact_g2_canary_approval_before_sync") is True
    and data.get("orders_sent") is False
    and data.get("auth_network_started") is False
    and data.get("started_canary") is False
    and all(value is False for value in side_effects.values())
    and must_have.issubset(paths)
    and all(
        record.get("exists") is True
        and record.get("is_file") is True
        and isinstance(record.get("sha256"), str)
        and len(record.get("sha256")) == 64
        for record in records
    )
    and all(
        record.get("expected_executable") is not True
        or (
            record.get("is_executable") is True
            and record.get("has_shebang") is True
            and isinstance(record.get("shebang"), str)
            and record.get("shebang", "").startswith("#!")
        )
        for record in records
    )
    and all(
        not any(fragment in str(record.get("path")) for fragment in forbidden_fragments)
        for record in records
    )
)
raise SystemExit(0 if ok else 1)
PY

cat > "$out_dir/manifest.json" <<JSON
{
  "artifact": "xuan_b27_dplus_g2_canary_executor_payload_manifest_smoke",
  "status": "$status",
  "strategy": "xuan_b27_dplus",
  "scope": "local_no_network_g2_canary_executor_payload_manifest_gate",
  "payload_manifest": "$payload_manifest",
  "orders_sent": false,
  "auth_network_started": false,
  "started_canary": false,
  "checks_log": "$log",
  "generated_at_utc": "$ts"
}
JSON

if [[ "$status" != "PASS" ]]; then
  cat "$log" >&2
  exit 1
fi

printf '%s\n' "$out_dir/manifest.json"
