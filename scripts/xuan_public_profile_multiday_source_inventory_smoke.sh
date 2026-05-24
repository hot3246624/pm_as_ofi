#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_public_profile_multiday_source_inventory_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_public_profile_multiday_source_inventory.py >> "$log" 2>&1

python3 scripts/xuan_public_profile_multiday_source_inventory.py \
  --output-dir "$out_dir/default" \
  >> "$log" 2>&1

python3 - "$out_dir/good_inputs/0xce25e214d5cfe4f459cf67f08df581885aae7fdc/activity_trade_rows.json" "$out_dir/bad_inputs/0xce25e214d5cfe4f459cf67f08df581885aae7fdc/activity_trade_rows.json" <<'PY' >> "$log" 2>&1
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

def ts(day):
    return int(datetime.fromisoformat(day + "T12:00:00+00:00").timestamp())

def rows(days):
    out = []
    idx = 0
    for day in days:
        for _ in range(220):
            idx += 1
            out.append(
                {
                    "transactionHash": f"0x{idx:064x}",
                    "conditionId": f"cond-{day}-{idx % 5}",
                    "eventSlug": "btc-updown-15m",
                    "slug": f"btc-updown-15m-{ts(day)}",
                    "timestamp": ts(day),
                    "price": 0.6,
                    "size": 10,
                    "usdcSize": 6.03,
                    "outcome": "Up",
                    "outcomeIndex": 0,
                    "side": "BUY",
                    "type": "TRADE",
                }
            )
    return out

good_path = Path(sys.argv[1])
bad_path = Path(sys.argv[2])
good_path.parent.mkdir(parents=True, exist_ok=True)
bad_path.parent.mkdir(parents=True, exist_ok=True)
good_path.write_text(json.dumps(rows(["2026-05-21", "2026-05-22", "2026-05-23"]), indent=2) + "\n")
bad_path.write_text(json.dumps(rows(["2026-05-23"]), indent=2) + "\n")
PY

python3 scripts/xuan_public_profile_multiday_source_inventory.py \
  --public-input-root "$out_dir/good_inputs" \
  --output-dir "$out_dir/good" \
  >> "$log" 2>&1

python3 scripts/xuan_public_profile_multiday_source_inventory.py \
  --public-input-root "$out_dir/bad_inputs" \
  --output-dir "$out_dir/bad_single_day" \
  >> "$log" 2>&1

python3 scripts/xuan_public_profile_multiday_source_inventory.py \
  --public-input-root "/Users/hot/web3Scientist/poly_trans_research/data/exports/public_inputs" \
  --output-dir "$out_dir/external_fail" \
  >> "$log" 2>&1

python3 - "$out_dir/default/manifest.json" "$out_dir/good/manifest.json" "$out_dir/bad_single_day/manifest.json" "$out_dir/external_fail/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

default = json.loads(Path(sys.argv[1]).read_text())
good = json.loads(Path(sys.argv[2]).read_text())
bad = json.loads(Path(sys.argv[3]).read_text())
external = json.loads(Path(sys.argv[4]).read_text())
checks = {
    "default_fail_closed_or_ready": default["decision"] in {"UNKNOWN", "KEEP"},
    "default_no_promotion": not default["promotion_gate"]["passed"],
    "good_ready": good["decision_label"] == "KEEP_PUBLIC_PROFILE_MULTIDAY_SOURCE_READY",
    "good_selected_source": good["selected_multiday_source"]["bjt_day_count"] >= 3,
    "good_validation_command_named": "xuan_public_profile_multiday_validation_spec.py" in good["selected_multiday_source"]["validation_command"],
    "bad_single_day_unknown": bad["decision_label"] == "UNKNOWN_PUBLIC_PROFILE_MULTIDAY_SOURCE_NOT_IN_CURRENT_WORKTREE",
    "bad_single_day_blocker": "no_current_worktree_public_profile_source_with_at_least_3_bjt_days" in bad["research_ranking"]["exact_blockers"],
    "external_fail_closed": external["decision_label"] == "UNKNOWN_PUBLIC_PROFILE_MULTIDAY_SOURCE_NOT_IN_CURRENT_WORKTREE",
    "external_rejected": external["public_input_inventory"]["rejected_roots"][0]["reason"] == "outside_current_worktree",
    "no_no_order_allowed": not default["research_ranking"]["no_order_diagnostic_allowed"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"public profile multiday source inventory smoke failed: {failed}")
manifest = {
    "artifact": "xuan_public_profile_multiday_source_inventory_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_PUBLIC_PROFILE_MULTIDAY_SOURCE_INVENTORY_SMOKE_PASS",
    "checks": checks,
    "default_manifest": sys.argv[1],
    "good_manifest": sys.argv[2],
    "bad_single_day_manifest": sys.argv[3],
    "external_fail_manifest": sys.argv[4],
}
Path(sys.argv[5]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
