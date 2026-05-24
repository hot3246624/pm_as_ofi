#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ts="${XUAN_B55_CE25_PUBLIC_ENTRY_JOIN_AUDIT_SMOKE_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
out_dir="${1:-$ROOT/xuan_research_artifacts/xuan_b55_ce25_public_entry_join_audit_smoke_$ts}"
mkdir -p "$out_dir"

missing_out="$out_dir/missing"
complete_fixture="$out_dir/complete_fixture_rows.json"
complete_out="$out_dir/complete"
bad_fixture="$out_dir/bad_fixture_rows.json"
bad_out="$out_dir/bad"

python3 "$ROOT/scripts/xuan_b55_ce25_public_entry_join_audit.py" \
  --output-dir "$missing_out" \
  >"$out_dir/missing.log"

python3 - "$ROOT" "$complete_fixture" "$bad_fixture" <<'PY'
import csv
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
complete_path = Path(sys.argv[2])
bad_path = Path(sys.argv[3])
spec = json.loads(
    (root / "xuan_research_artifacts/xuan_b55_ce25_fair_probability_model_spec_20260524T055426Z/manifest.json").read_text()
)
required_fields = spec["probability_model_contract"]["required_candidate_fields"]
export_root = Path("/Users/hot/web3Scientist/poly_trans_research/data/exports")
exports = {
    "b55": export_root / "leaderboard_b55_recent24h_20260523_103000_to_20260524_103000_bjt",
    "ce25": export_root / "leaderboard_ce25_recent24h_20260523_103000_to_20260524_103000_bjt",
}


def parse_start_end(slug):
    try:
        anchor = int(slug.rsplit("-", 1)[1])
    except Exception as exc:
        raise AssertionError(f"fixture slug has no anchor timestamp: {slug}") from exc
    if "-updown-15m-" in slug:
        interval = 900
        timeframe = "15m"
    elif "-updown-1h-" in slug:
        interval = 3600
        timeframe = "1h_or_named"
    else:
        raise AssertionError(f"fixture slug is not allowed 15m/1h: {slug}")
    return anchor, anchor + interval, timeframe


def infer_asset(slug, title):
    text = f"{slug} {title}".lower()
    if "btc-" in text or "bitcoin" in text:
        return "BTC"
    if "eth-" in text or "ethereum" in text:
        return "ETH"
    return "unknown"


def choose_row(account, export_dir):
    with (export_dir / "market_trade_metrics.csv").open(newline="") as fh:
        for row in csv.DictReader(fh):
            slug = row["slug"]
            title = row["title"]
            asset = infer_asset(slug, title)
            if asset not in {"BTC", "ETH"}:
                continue
            if "-updown-15m-" not in slug and "-updown-1h-" not in slug:
                continue
            market_start, market_end, timeframe = parse_start_end(slug)
            quote_ts = market_end - 300
            polymarket_price = 0.62 if account == "b55" else 0.58
            fee_per_share = 0.002
            fee_usdc = 0.02
            size = 10.0
            fair_probability = 0.68 if account == "b55" else 0.64
            uncertainty = 0.015
            edge = fair_probability - polymarket_price - fee_per_share - uncertainty
            return {
                "account": account,
                "trade_id": f"fixture-{account}-{row['condition_id'][:10]}",
                "condition_id": row["condition_id"],
                "market_slug": slug,
                "asset": asset,
                "timeframe": timeframe,
                "market_start_ts": market_start,
                "market_end_ts": market_end,
                "quote_ts": quote_ts,
                "entry_delta_s": quote_ts - market_end,
                "time_to_expiry_s": market_end - quote_ts,
                "outcome": "YES",
                "polymarket_price": polymarket_price,
                "size": size,
                "gross_usdc": round(polymarket_price * size, 6),
                "fee_usdc": fee_usdc,
                "actual_usdc": round(polymarket_price * size + fee_usdc, 6),
                "liquidity_role": "maker",
                "boundary_price": 100000.0,
                "fair_spot_mid": 100012.5,
                "source_count": 4,
                "source_names": ["binance", "coinbase", "okx", "bybit"],
                "source_dispersion_bps": 2.5,
                "max_source_age_ms": 450,
                "volatility_lookback_s": 300,
                "volatility_bps": 18.0,
                "fair_probability": fair_probability,
                "fair_probability_uncertainty": uncertainty,
                "edge_after_fee_and_uncertainty": round(edge, 6),
                "model_name": "fixture_entry_boundary_normal_probability",
                "model_version": "fixture-v1",
            }
    raise AssertionError(f"no fixture-eligible public row found for {account}")


rows = [choose_row(account, export_dir) for account, export_dir in exports.items()]
for row in rows:
    missing = [field for field in required_fields if field not in row]
    if missing:
        raise AssertionError(f"fixture row missing required fields: {missing}")
complete = {
    "artifact": "xuan_b55_ce25_public_entry_join_fixture",
    "decision_label": "KEEP_FIXTURE_ENTRY_JOIN_ROWS_READY",
    "fixture": True,
    "rows": rows,
}
bad_rows = []
for row in rows:
    bad_row = dict(row)
    bad_row.pop("fair_probability", None)
    bad_row.pop("fair_probability_uncertainty", None)
    bad_rows.append(bad_row)
bad = {
    "artifact": "xuan_b55_ce25_public_entry_join_bad_fixture",
    "decision_label": "UNKNOWN_BAD_FIXTURE_MISSING_PROBABILITY",
    "fixture": True,
    "rows": bad_rows,
}
complete_path.write_text(json.dumps(complete, indent=2, sort_keys=True) + "\n")
bad_path.write_text(json.dumps(bad, indent=2, sort_keys=True) + "\n")
PY

python3 "$ROOT/scripts/xuan_b55_ce25_public_entry_join_audit.py" \
  --candidate-entry-join-rows "$complete_fixture" \
  --output-dir "$complete_out" \
  >"$out_dir/complete.log"

python3 "$ROOT/scripts/xuan_b55_ce25_public_entry_join_audit.py" \
  --candidate-entry-join-rows "$bad_fixture" \
  --output-dir "$bad_out" \
  >"$out_dir/bad.log"

python3 - "$missing_out/manifest.json" "$complete_out/manifest.json" "$bad_out/manifest.json" "$out_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

missing = json.loads(Path(sys.argv[1]).read_text())
complete = json.loads(Path(sys.argv[2]).read_text())
bad = json.loads(Path(sys.argv[3]).read_text())
checks = {
    "missing_public_real_unknown": missing["research_ranking"]["public_real_status"] == "UNKNOWN_PUBLIC_ENTRY_JOIN_FIELDS_MISSING",
    "missing_candidate_rows_absent": missing["candidate_entry_join_status"]["status"] == "MISSING_CANDIDATE_ENTRY_JOIN_ROWS",
    "missing_fields_include_fair_probability": "fair_probability" in missing["research_ranking"]["public_export_missing_fields"],
    "complete_fixture_ready": complete["candidate_entry_join_status"]["status"] == "CANDIDATE_ENTRY_JOIN_ROWS_READY",
    "complete_fixture_joined_to_public": complete["candidate_entry_join_status"]["joined_public_rows"] == complete["candidate_entry_join_status"]["row_count"] == 2,
    "complete_fixture_not_real_inputs": complete["research_ranking"]["public_real_status"] == "UNKNOWN_PUBLIC_ENTRY_JOIN_FIELDS_MISSING",
    "bad_fixture_fail_closed": bad["candidate_entry_join_status"]["status"] == "CANDIDATE_ENTRY_JOIN_ROWS_FAIL_CLOSED",
    "bad_fixture_missing_probability": "fair_probability" in bad["candidate_entry_join_status"]["missing_fields"],
    "promotion_never_passes": not missing["promotion_gate"]["passed"] and not complete["promotion_gate"]["passed"],
    "no_side_effect_scope": not missing["scope"]["shadow_started"] and not missing["scope"]["orders_sent"],
}
failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise AssertionError(f"b55/ce25 public entry-join audit smoke failed: {failed}")
manifest = {
    "artifact": "xuan_b55_ce25_public_entry_join_audit_smoke",
    "decision_label": "KEEP_B55_CE25_PUBLIC_ENTRY_JOIN_AUDIT_SMOKE_PASS",
    "checks": checks,
    "missing_manifest": sys.argv[1],
    "complete_manifest": sys.argv[2],
    "bad_manifest": sys.argv[3],
}
Path(sys.argv[4]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY
