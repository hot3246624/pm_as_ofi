#!/usr/bin/env python3
import subprocess
import json
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path("/Users/hot/web3Scientist/poly_backtest_data/verification_store/replay_store_v2/20260502_20260518/store.duckdb")
FIXED_DB_PATH = REPO_ROOT / "xuan_research_artifacts" / "eval_compat_store.sqlite"

# Champion config for 612728605018
CHAMPION_CONFIG = {
    "max_net_diff": 0.5,
    "pair_target": 0.85,
    "bid_size": 0.2,
    "tier_1_mult": 0.4,
    "tier_2_mult": 0.12,
    "tier_mode": "disabled",
    "fill_model": "conservative",
    "risk_open_cutoff_secs": 210.0,
    "pair_cost_safety_margin": 0.02,
    "salvage_net_cap": 1.0,
    "salvage_start_remaining_secs": 280.0,
    "taker_fee_rate": 0.07,
    "directional_risk_filter_bps": 0.0,
    "directional_entry_min_bps": 0.0,
    "directional_price_source": "price",
    "entry_pair_max_ask_sum": 0.0,
    "reject_stale": False,
    "require_ws_fresh": False,
    "max_quote_age_secs": 0.0,
    "min_ask_depth": 0.0,
    "require_two_sided_entry": False,
    "pairing_only_when_residual": False,
    "initial_balance": "10000",
    "stop_loss_threshold": 1.0,
    "exit_window_secs": 10.0,
    "exit_loss_limit": 0.05
}

ITERATION_WEIGHTS = {
    "net_pnl": 1000.0,
    "pair_cost": 260.0,
    "participation": 300.0,
    "fill_count": 14.0,
    "residual_rate": 900.0,
    "residual_cost": 1200.0,
    "residual_qty": 80.0,
    "fee": 100.0,
}

MODE_WEIGHT_BOOST = {
    "nagi": {
        "residual_rate": 1.15,
        "residual_cost": 1.25,
        "pair_cost": 0.95,
    }
}

def materialize_db():
    if FIXED_DB_PATH.exists():
        print(f"Reusing existing compatibility DB at: {FIXED_DB_PATH}")
        return FIXED_DB_PATH

    print("Materializing compatibility database from DuckDB...")
    sys.path.append(str(REPO_ROOT / "scripts"))
    from xuan_b27_dplus_pair_arb_autoresearch import resolve_backtest_db
    
    temp_dir = REPO_ROOT / "xuan_research_artifacts" / "eval_temp_build"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    compat_db, info = resolve_backtest_db(DEFAULT_DB, temp_dir, force=True, infer_settlement_outcome=True)
    # Locate files in eval_temp_build
    resolved_paths = list(temp_dir.glob("**/*.compat_market_ticks.sqlite"))
    if not resolved_paths:
        raise RuntimeError(f"Failed to find materialized db under eval_temp_build")
    
    # Move to FIXED_DB_PATH
    FIXED_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    resolved_paths[0].rename(FIXED_DB_PATH)
    
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"Compatibility DB materialized and saved to: {FIXED_DB_PATH}")
    return FIXED_DB_PATH

def run_backtest(db_path, limit=300):
    print("Building pair_arb_backtest in release mode...")
    subprocess.run(["cargo", "build", "--release", "--bin", "pair_arb_backtest"], cwd=REPO_ROOT, check=True)

    cmd = [
        "./target/release/pair_arb_backtest",
        "--jsonl",
        "--db", str(db_path),
        "--limit", str(limit),
        "--skip", "0",
        "--max-net-diff", str(CHAMPION_CONFIG["max_net_diff"]),
        "--pair-target", str(CHAMPION_CONFIG["pair_target"]),
        "--bid-size", str(CHAMPION_CONFIG["bid_size"]),
        "--tier1", str(CHAMPION_CONFIG["tier_1_mult"]),
        "--tier2", str(CHAMPION_CONFIG["tier_2_mult"]),
        "--tier-mode", CHAMPION_CONFIG["tier_mode"],
        "--fill-model", CHAMPION_CONFIG["fill_model"],
        "--cutoff", str(CHAMPION_CONFIG["risk_open_cutoff_secs"]),
        "--margin", str(CHAMPION_CONFIG["pair_cost_safety_margin"]),
        "--salvage-net-cap", str(CHAMPION_CONFIG["salvage_net_cap"]),
        "--salvage-start-remaining", str(CHAMPION_CONFIG["salvage_start_remaining_secs"]),
        "--taker-fee-rate", str(CHAMPION_CONFIG["taker_fee_rate"]),
        "--directional-risk-filter-bps", str(CHAMPION_CONFIG["directional_risk_filter_bps"]),
        "--directional-entry-min-bps", str(CHAMPION_CONFIG["directional_entry_min_bps"]),
        "--directional-price-source", CHAMPION_CONFIG["directional_price_source"],
        "--entry-pair-max-ask-sum", str(CHAMPION_CONFIG["entry_pair_max_ask_sum"]),
        "--max-quote-age-sec", str(CHAMPION_CONFIG["max_quote_age_secs"]),
        "--min-ask-depth", str(CHAMPION_CONFIG["min_ask_depth"]),
        "--initial-balance", CHAMPION_CONFIG["initial_balance"],
        "--stop-loss-threshold", str(CHAMPION_CONFIG["stop_loss_threshold"]),
        "--exit-window-secs", str(CHAMPION_CONFIG["exit_window_secs"]),
        "--exit-loss-limit", str(CHAMPION_CONFIG["exit_loss_limit"]),
        "--salvage-failure-probability", "0.10"
    ]
    if CHAMPION_CONFIG["reject_stale"]:
        cmd.append("--reject-stale")
    if CHAMPION_CONFIG["require_ws_fresh"]:
        cmd.append("--require-ws-fresh")
    if CHAMPION_CONFIG["pairing_only_when_residual"]:
        cmd.append("--pairing-only-when-residual")
    if CHAMPION_CONFIG["require_two_sided_entry"]:
        cmd.append("--require-two-sided-entry")

    print(f"Running backtest with command: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    return proc.stdout

def compute_objective(metrics):
    mode_weights = dict(ITERATION_WEIGHTS)
    multiplier = MODE_WEIGHT_BOOST["nagi"]
    for key, mul in multiplier.items():
        if key in mode_weights:
            mode_weights[key] = mode_weights[key] * float(mul)

    total_pnl = metrics.get("total_pnl", 0.0)
    paired_pnl = metrics.get("paired_pnl", 0.0)
    residual_pnl = metrics.get("residual_pnl", 0.0)
    residual_loss = max(0.0, -residual_pnl)
    fee = metrics.get("completion_fee", 0.0)
    net_pnl = max(0.0, paired_pnl) - residual_loss - max(0.0, fee)

    pair_cost = metrics.get("weighted_avg_pair_cost", 1.0)
    if pair_cost == 0.0:
        pair_cost = 1.0
    fill_window_rate = metrics.get("fill_window_rate", 0.0)
    participation_rate = max(0.0, min(1.0, fill_window_rate))
    residual_loss_rate = max(0.0, min(1.0, metrics.get("residual_loss_rate", 0.0)))
    residual_qty = metrics.get("residual_qty", 0.0)
    fill_count = max(0.0, metrics.get("fills", 0.0))

    net_pnl_term = net_pnl * mode_weights["net_pnl"]
    pair_cost_term = (1.0 - max(0.0, min(2.0, pair_cost))) * mode_weights["pair_cost"]
    participation_term = participation_rate * mode_weights["participation"]
    fill_count_term = math.sqrt(fill_count) * mode_weights["fill_count"]
    residual_rate_term = (1.0 - residual_loss_rate) * mode_weights["residual_rate"]
    residual_cost_term = -residual_loss * mode_weights["residual_cost"]
    residual_qty_term = -math.log1p(max(0.0, residual_qty)) * mode_weights["residual_qty"]
    fee_term = -fee * mode_weights["fee"]

    objective = (
        net_pnl_term
        + pair_cost_term
        + participation_term
        + fill_count_term
        + residual_rate_term
        + residual_cost_term
        + residual_qty_term
        + fee_term
    )
    return objective, {
        "net_pnl_term": net_pnl_term,
        "pair_cost_term": pair_cost_term,
        "participation_term": participation_term,
        "fill_count_term": fill_count_term,
        "residual_rate_term": residual_rate_term,
        "residual_cost_term": residual_cost_term,
        "residual_qty_term": residual_qty_term,
        "fee_term": fee_term,
        "total_objective": objective,
    }

def main():
    db_path = materialize_db()
    stdout_text = run_backtest(db_path, limit=1000)
    rows = []
    for line in stdout_text.strip().split("\n"):
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    
    if not rows:
        print("Error: No backtest rows outputted by the binary!")
        sys.exit(1)
    
    last_row = rows[-1]
    raw_metrics = last_row.get("metrics") or {}
    
    metrics = {
        "total_pnl": raw_metrics.get("total_pnl", 0.0),
        "weighted_avg_pair_cost": raw_metrics.get("weighted_avg_pair_cost", 1.0),
        "paired_window_rate": raw_metrics.get("paired_window_rate", 0.0),
        "fill_window_rate": raw_metrics.get("fill_window_rate", 0.0),
        "residual_loss_rate": raw_metrics.get("residual_loss_rate", 0.0),
        "residual_cost": raw_metrics.get("residual_cost", 0.0),
        "completion_cost": raw_metrics.get("completion_cost", 0.0),
        "completion_fee": raw_metrics.get("completion_fee", 0.0),
        "fills": raw_metrics.get("fills", 0.0),
        "paired_qty": raw_metrics.get("paired_qty", 0.0),
        "residual_qty": raw_metrics.get("residual_qty", 0.0),
        "completion_fills": raw_metrics.get("completion_fills", 0.0),
        "completion_qty": raw_metrics.get("completion_qty", 0.0),
        "paired_pnl": raw_metrics.get("paired_pnl", 0.0),
        "residual_pnl": raw_metrics.get("residual_pnl", 0.0),
        "rer": raw_metrics.get("rer", 0.0),
        "total_bad_pair_qty": raw_metrics.get("total_bad_pair_qty", 0.0),
        "positive_days": raw_metrics.get("positive_days", 0),
        "worst_day_pnl": raw_metrics.get("worst_day_pnl", 0.0),
    }
    
    bad_pc_pct = (metrics["total_bad_pair_qty"] / metrics["paired_qty"] * 100.0) if metrics["paired_qty"] > 0 else 0.0
    objective, components = compute_objective(metrics)
    print("\n=== EVALUATION RESULTS ===")
    print(f"Total PnL:     {metrics['total_pnl']:.4f}")
    print(f"Paired PnL:    {metrics['paired_pnl']:.4f}")
    print(f"Residual PnL:  {metrics['residual_pnl']:.4f}")
    print(f"Pair Cost:     {metrics['weighted_avg_pair_cost']:.4f}")
    print(f"Fills Count:   {metrics['fills']}")
    print(f"Residual Qty:  {metrics['residual_qty']:.4f}")
    print(f"Residual Loss Rate: {metrics['residual_loss_rate']*100:.1f}%")
    print(f"Fill Window Rate:   {metrics['fill_window_rate']*100:.1f}%")
    print(f"Residual Exposure Ratio (RER): {metrics['rer']*100:.4f}%")
    print(f"Bad Pair Cost Ratio (>= 1.0):  {bad_pc_pct:.4f}% ({metrics['total_bad_pair_qty']:.2f} / {metrics['paired_qty']:.2f} shares)")
    print(f"Positive Days:                 {metrics['positive_days']}")
    print(f"Worst Day PnL:                 {metrics['worst_day_pnl']:.4f}")
    print("\n--- Objective Components ---")
    for k, v in components.items():
        print(f"{k:20}: {v:+.4f}")

if __name__ == "__main__":
    main()
