#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESEARCH_ROOT_DEFAULT="${ROOT_DIR}/../poly_trans_research"
RESEARCH_ROOT="${RESEARCH_ROOT:-$RESEARCH_ROOT_DEFAULT}"
DAY_UTC="${1:-$(date -u +%F)}"
SHADOW_DB="${SHADOW_DB:-${ROOT_DIR}/data/replay/${DAY_UTC}/crypto_5m.sqlite}"
BASELINE_REPORT_JSON="${BASELINE_REPORT_JSON:-}"

AUDIT_JSON="${RESEARCH_ROOT}/data/replay/${DAY_UTC}/startup_audit.json"
XUAN_SUMMARY_JSON="${ROOT_DIR}/docs/xuan_completion_gate_summary.json"
XUAN_DEFAULTS_JSON="${ROOT_DIR}/configs/xuan_completion_gate_defaults.json"
SHADOW_SUMMARY_JSON="${ROOT_DIR}/data/replay/${DAY_UTC}/completion_first_shadow_summary.json"

echo "[1/5] verify startup audit: ${AUDIT_JSON}"
python3 - <<'PY' "${AUDIT_JSON}"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(f"missing startup audit: {path}")
payload = json.loads(path.read_text(encoding="utf-8"))
if not bool(payload.get("all_passed")):
    raise SystemExit(f"startup audit failed: {path}")
print(json.dumps({
    "all_passed": payload.get("all_passed"),
    "recent_note": "audit only; overlap/data-usable is checked after xuan summary refresh"
}, ensure_ascii=False))
PY

echo "[2/5] refresh xuan completion gate summary/defaults"
python3 "${ROOT_DIR}/scripts/export_xuan_completion_gate.py" \
  --out-summary-json "${XUAN_SUMMARY_JSON}" \
  --out-defaults-json "${XUAN_DEFAULTS_JSON}"

echo "[3/5] export completion_first shadow report from ${SHADOW_DB}"
python3 "${ROOT_DIR}/scripts/export_completion_first_shadow_report.py" --db "${SHADOW_DB}"

echo "[4/5] export xuan completion gap report"
if [[ -n "${BASELINE_REPORT_JSON}" ]]; then
  python3 "${ROOT_DIR}/scripts/export_xuan_completion_gap_report.py" \
    --report-json "${SHADOW_SUMMARY_JSON}" \
    --baseline-report-json "${BASELINE_REPORT_JSON}"
else
  python3 "${ROOT_DIR}/scripts/export_xuan_completion_gap_report.py" \
    --report-json "${SHADOW_SUMMARY_JSON}"
fi

echo "[5/5] finished T+72h cycle prep for ${DAY_UTC}"
