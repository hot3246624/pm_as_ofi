#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b27_dplus_residual_cooldown_compliant_metrics_runner_smoke_$ts}"
fixture="$out_dir/fixture"
run_out="$out_dir/run"
log="$out_dir/smoke.log"
mkdir -p "$fixture" "$run_out"
: > "$log"

script="scripts/xuan_b27_dplus_residual_cooldown_compliant_metrics_runner.py"
python3 -m py_compile "$script" >> "$log" 2>&1

env FIXTURE="$fixture" uv run --with duckdb python - <<'PY' >> "$log" 2>&1
import json
import os
from pathlib import Path

import duckdb

fixture = Path(os.environ["FIXTURE"])
strict_dir = fixture / "strict" / "20260513"
completion_dir = fixture / "completion" / "20260513"
public_dir = fixture / "public_account_execution_truth_v1" / "20260502_20260513"
for path in (strict_dir, completion_dir, public_dir):
    path.mkdir(parents=True, exist_ok=True)

(strict_dir / "CACHE_MANIFEST.json").write_text(json.dumps({"days": ["2026-05-13"]}) + "\n")
(completion_dir / "EVENT_STORE_MANIFEST.json").write_text(json.dumps({"days": ["2026-05-13"]}) + "\n")
(public_dir / "EVENT_STORE_MANIFEST.json").write_text(json.dumps({"days": ["2026-05-13"]}) + "\n")

con = duckdb.connect(str(strict_dir / "cache.duckdb"))
con.execute(
    """
    create table taker_buy_signal_candidates(
        day varchar,
        slug varchar,
        condition_id varchar,
        winner_side varchar,
        trigger_ts_ms bigint,
        offset_s double,
        first_side varchar,
        side_alignment varchar,
        public_trade_price double,
        public_trade_size double,
        first_l2_vwap double,
        l1_immediate_pair double,
        strict_l1_immediate_pair double
    )
    """
)
con.execute(
    """
    insert into taker_buy_signal_candidates values
    ('2026-05-13', 'btc-updown-5m-1', 'cond1', 'YES', 1000000, 10.0, 'YES', 'all', 0.35, 20.0, 0.31, 0.95, 0.95),
    ('2026-05-13', 'btc-updown-5m-1', 'cond1', 'YES', 1007000, 17.0, 'NO', 'all', 0.32, 20.0, 0.28, 0.95, 0.95),
    ('2026-05-13', 'btc-updown-5m-2', 'cond2', 'NO', 2000000, 8.0, 'YES', 'all', 0.40, 20.0, 0.36, 0.97, 0.97)
    """
)
con.close()

con = duckdb.connect(str(completion_dir / "event_store.duckdb"))
con.execute(
    """
    create table completion_unwind_events(
        event_id varchar,
        day varchar,
        condition_id varchar,
        ts_ms bigint,
        side varchar,
        side_ask double
    )
    """
)
con.execute(
    """
    insert into completion_unwind_events values
    ('e1', '2026-05-13', 'cond1', 1005000, 'NO', 0.45),
    ('e2', '2026-05-13', 'cond1', 1010000, 'YES', 0.44),
    ('e3', '2026-05-13', 'cond2', 2005000, 'NO', 0.50)
    """
)
con.close()

con = duckdb.connect(str(public_dir / "event_store.duckdb"))
con.execute(
    """
    create table public_account_execution_events(
        day varchar,
        condition_id varchar,
        event_kind varchar,
        fill_price double,
        fill_qty double
    )
    """
)
con.execute(
    """
    insert into public_account_execution_events values
    ('2026-05-13', 'cond1', 'fill', 0.35, 20.0),
    ('2026-05-13', 'cond2', 'fill', 0.40, 20.0)
    """
)
con.close()
PY

uv run --with duckdb python "$script" \
  --strict-root "$fixture/strict" \
  --completion-root "$fixture/completion" \
  --public-audit-db "$fixture/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb" \
  --strict-labels 20260513 \
  --completion-labels 20260513 \
  --max-strict-candidates-per-plan 0 \
  --output-dir "$run_out" >> "$log" 2>&1

python3 - "$run_out/manifest.json" <<'PY' >> "$log" 2>&1
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text())
assert manifest["artifact"] == "xuan_b27_dplus_residual_cooldown_compliant_metrics_runner"
assert manifest["status"] == "PASS_COMPLIANT_METRICS_RUNNER"
assert manifest["raw_replay_scanned"] is False
assert manifest["orders_sent"] is False
assert manifest["side_effects"]["service_control_used"] is False
assert manifest["profile_count"] >= 1
assert any(
    profile["name"] == "dpass_all_e055_t5_px050_900_imb125_rc30_050"
    for profile in manifest["profiles"]
)
PY

printf 'PASS residual cooldown compliant metrics runner smoke: %s\n' "$run_out/manifest.json" | tee -a "$log"
