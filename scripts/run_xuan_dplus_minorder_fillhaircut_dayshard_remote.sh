#!/usr/bin/env bash
set -euo pipefail

BASE="${1:-/home/ubuntu/xuan_frontier_runs/d_branch_minorder_fillhaircut_full_0502_0512_20260514_0310}"
PY="${PY:-/tmp/xuan_duckdb_venv/bin/python}"
SCRIPT="${SCRIPT:-/home/ubuntu/xuan_frontier_runs/xuan_d_branch_passive_passive_redeem.py}"

mkdir -p "$BASE"

COMMON_STORES=(
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb:2026-05-02
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb:2026-05-03
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb:2026-05-04
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb:2026-05-05
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb:2026-05-06
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb:2026-05-07
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260502_20260508/event_store.duckdb:2026-05-08
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260509/event_store.duckdb:2026-05-09
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260510/event_store.duckdb:2026-05-10
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260511/event_store.duckdb:2026-05-11
  --store /mnt/poly-verification-store/completion_unwind_event_store_v2/20260512/event_store.duckdb:2026-05-12
)

for FH in 0.20 0.15 0.10; do
  TAG=$("$PY" - <<PY
print("fh%03d" % int(round($FH * 100)))
PY
)
  OUT="$BASE/$TAG"
  if [ -f "$OUT/combined_results.json" ]; then
    echo "skip existing $OUT"
    continue
  fi
  rm -rf "$OUT"
  mkdir -p "$OUT" "/home/ubuntu/duckdb_tmp_xuan_d_minorder_$TAG"
  echo "$(date -Is) running fill_haircut=$FH out=$OUT"
  nice -n 5 "$PY" "$SCRIPT" \
    "${COMMON_STORES[@]}" \
    --out-dir "$OUT" \
    --tmp-dir "/home/ubuntu/duckdb_tmp_xuan_d_minorder_$TAG" \
    --edges 0.04 \
    --targets 10 \
    --px-bands 0.010:0.990 \
    --alignments all \
    --fill-haircut "$FH" \
    --max-seed-qty 60 \
    --max-open-cost 160 \
    --seed-l1-pair-cap 1.02 \
    --imbalance-qty-caps 6 8 \
    --salvage-net-caps 0.95 0.96 \
    --salvage-age-s 30 \
    --salvage-min-lot-cost 0.25 \
    > "$BASE/$TAG.log" 2>&1
  tail -20 "$BASE/$TAG.log"
done

"$PY" - "$BASE" <<'PY'
import csv
import json
import pathlib
import sys

base = pathlib.Path(sys.argv[1])
rows = []
for result_path in sorted(base.glob("fh*/combined_results.json")):
    data = json.loads(result_path.read_text())
    for row in data:
        rows.append(
            {
                "fh": result_path.parent.name,
                **{
                    key: row.get(key)
                    for key in [
                        "name",
                        "target_qty",
                        "imbalance_qty_cap",
                        "salvage_net_cap",
                        "active_markets",
                        "pair_actions",
                        "gross_buy_cost",
                        "pair_cost_wavg",
                        "net_pair_cost_wavg",
                        "pair_delay_wavg_s",
                        "rounds_per_market",
                        "qty_residual_rate",
                        "cost_residual_rate",
                        "actual_settle_pnl",
                        "worst_residual_net_pnl",
                        "actual_settle_roi",
                        "stress100_actual_pnl",
                        "stress100_worst_pnl",
                    ]
                },
            }
        )

rows.sort(key=lambda item: (item["stress100_worst_pnl"], item["actual_settle_pnl"]), reverse=True)
(base / "minorder_fillhaircut_full_summary.json").write_text(json.dumps(rows, indent=2))
with (base / "minorder_fillhaircut_full_summary.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
print(json.dumps({"out": str(base), "top": rows[:12]}, indent=2))
PY
