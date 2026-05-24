#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_public_profile_multiday_validation_spec_smoke_$ts}"
mkdir -p "$out_dir"
log="$out_dir/smoke.log"
: > "$log"

python3 -m py_compile scripts/xuan_public_profile_multiday_validation_spec.py >> "$log" 2>&1

python3 scripts/xuan_public_profile_multiday_validation_spec.py \
  --output-dir "$out_dir/default" \
  >> "$log" 2>&1

python3 - "$out_dir/good/activity_trade_rows.json" "$out_dir/bad/activity_trade_rows.json" <<'PY' >> "$log" 2>&1
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

def ts(day):
    return int(datetime.fromisoformat(day + "T12:00:00+00:00").timestamp())

def make_rows(days, bad=False):
    rows = []
    idx = 0
    for day in days:
        for condition in range(60):
            for outcome, price in (("Up", 0.42), ("Down", 0.44)):
                idx += 1
                rows.append(
                    {
                        "transactionHash": f"0x{idx:064x}",
                        "conditionId": f"cond-{day}-{condition}",
                        "eventSlug": "eth-updown-15m",
                        "slug": f"eth-updown-15m-{ts(day)}",
                        "timestamp": ts(day),
                        "price": price,
                        "size": 10,
                        "usdcSize": price * 10,
                        "outcome": outcome,
                        "outcomeIndex": 0 if outcome == "Up" else 1,
                        "side": "BUY",
                        "type": "TRADE",
                    }
                )
        if bad:
            for extra in range(20):
                idx += 1
                rows.append(
                    {
                        "transactionHash": f"0x{idx:064x}",
                        "conditionId": f"sell-{day}-{extra}",
                        "eventSlug": "eth-updown-15m",
                        "slug": f"eth-updown-15m-{ts(day)}",
                        "timestamp": ts(day),
                        "price": 0.5,
                        "size": 10,
                        "usdcSize": 5,
                        "outcome": "Up",
                        "outcomeIndex": 0,
                        "side": "SELL",
                        "type": "TRADE",
                    }
                )
    return rows

good_path = Path(sys.argv[1])
bad_path = Path(sys.argv[2])
good_path.parent.mkdir(parents=True, exist_ok=True)
bad_path.parent.mkdir(parents=True, exist_ok=True)
good_path.write_text(json.dumps(make_rows(["2026-05-21", "2026-05-22", "2026-05-23"]), indent=2) + "\n")
bad_path.write_text(json.dumps(make_rows(["2026-05-21", "2026-05-22", "2026-05-23"], bad=True), indent=2) + "\n")
PY

python3 scripts/xuan_public_profile_multiday_validation_spec.py \
  --public-input "$out_dir/good/activity_trade_rows.json" \
  --output-dir "$out_dir/good_score" \
  >> "$log" 2>&1

python3 scripts/xuan_public_profile_multiday_validation_spec.py \
  --public-input "$out_dir/bad/activity_trade_rows.json" \
  --output-dir "$out_dir/bad_score" \
  >> "$log" 2>&1

python3 scripts/xuan_public_profile_multiday_validation_spec.py \
  --public-input "/Users/hot/web3Scientist/poly_trans_research/data/exports/activity_trade_rows.json" \
  --output-dir "$out_dir/external_fail" \
  >> "$log" 2>&1

python3 - "$out_dir/default/manifest.json" "$out_dir/good_score/manifest.json" "$out_dir/bad_score/manifest.json" "$out_dir/external_fail/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

default = json.loads(Path(sys.argv[1]).read_text())
good = json.loads(Path(sys.argv[2]).read_text())
bad = json.loads(Path(sys.argv[3]).read_text())
external = json.loads(Path(sys.argv[4]).read_text())
checks = {
    "default_unknown_for_0x8dxd": default["decision_label"] == "UNKNOWN_PUBLIC_PROFILE_MULTIDAY_VALIDATION_SAMPLE_INSUFFICIENT",
    "default_valid_days_below_3": default["public_profile_multiday_validation"]["valid_day_count"] < 3,
    "good_keep": good["decision_label"] == "KEEP_PUBLIC_PROFILE_MULTIDAY_VALIDATION_PROXY_READY",
    "good_three_valid_days": good["public_profile_multiday_validation"]["valid_day_count"] == 3,
    "bad_unknown": bad["decision_label"] == "UNKNOWN_PUBLIC_PROFILE_MULTIDAY_VALIDATION_SAMPLE_INSUFFICIENT",
    "bad_sell_blocker": any("sell_share_above_max" in b for b in bad["research_ranking"]["blockers"]),
    "external_fail_closed": external["decision_label"] == "UNKNOWN_PUBLIC_PROFILE_MULTIDAY_VALIDATION_SAMPLE_INSUFFICIENT",
    "external_blocker": "outside_current_worktree" in external["research_ranking"]["blockers"],
    "no_promotion": not default["promotion_gate"]["passed"] and not good["promotion_gate"]["passed"],
    "no_no_order_allowed": not default["research_ranking"]["no_order_diagnostic_allowed"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"public profile multiday validation smoke failed: {failed}")
manifest = {
    "artifact": "xuan_public_profile_multiday_validation_spec_smoke",
    "decision": "KEEP",
    "decision_label": "KEEP_PUBLIC_PROFILE_MULTIDAY_VALIDATION_SPEC_SMOKE_PASS",
    "checks": checks,
    "default_manifest": sys.argv[1],
    "good_manifest": sys.argv[2],
    "bad_manifest": sys.argv[3],
    "external_fail_manifest": sys.argv[4],
}
Path(sys.argv[5]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

python3 -m json.tool "$out_dir/manifest.json" >/dev/null
echo "$out_dir/manifest.json"
