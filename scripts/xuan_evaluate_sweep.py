#!/usr/bin/env python3
import subprocess
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "xuan_research_artifacts" / "eval_compat_store.sqlite"

sys.path.append(str(REPO_ROOT / "scripts"))
from xuan_evaluate_candidate import CHAMPION_CONFIG, compute_objective

def main():
    print("Building pair_arb_backtest...")
    subprocess.run(["cargo", "build", "--release", "--bin", "pair_arb_backtest"], cwd=REPO_ROOT, check=True)

    # Let's define the sweep parameters
    exit_window_secs_grid = "2.0,4.0,6.0,8.0,10.0,12.0,14.0,16.0,18.0,20.0"
    exit_loss_limit_grid = "0.0,0.01,0.02,0.03,0.04,0.05,0.06,0.08,0.10,0.12,0.15"

    cmd = [
        "./target/release/pair_arb_backtest",
        "--jsonl",
        "--db", str(DEFAULT_DB),
        "--limit", "300",
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
        "--exit-window-secs", exit_window_secs_grid,
        "--exit-loss-limit", exit_loss_limit_grid,
        "--salvage-failure-probability", "0.10",
    ]
    if CHAMPION_CONFIG["reject_stale"]:
        cmd.append("--reject-stale")
    if CHAMPION_CONFIG["require_ws_fresh"]:
        cmd.append("--require-ws-fresh")
    if CHAMPION_CONFIG["pairing_only_when_residual"]:
        cmd.append("--pairing-only-when-residual")
    if CHAMPION_CONFIG["require_two_sided_entry"]:
        cmd.append("--require-two-sided-entry")

    print(f"Running sweep backtest...")
    proc = subprocess.run(cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    
    results = []
    for line in proc.stdout.strip().split("\n"):
        if not line:
            continue
        try:
            row = json.loads(line)
            cfg = row.get("config") or {}
            metrics = row.get("metrics") or {}
            obj_score, components = compute_objective(metrics)
            
            results.append({
                "exit_window_secs": cfg.get("exit_window_secs"),
                "exit_loss_limit": cfg.get("exit_loss_limit"),
                "objective": obj_score,
                "pnl": metrics.get("total_pnl"),
                "paired_pnl": metrics.get("paired_pnl"),
                "residual_pnl": metrics.get("residual_pnl"),
                "fills": metrics.get("fills"),
                "completion_fills": metrics.get("completion_fills"),
            })
        except Exception as e:
            print(f"Failed to parse line: {e}")
            
    # Sort results by objective descending
    results.sort(key=lambda x: x["objective"], reverse=True)
    
    print("\n=== TOP 25 SWEEP RESULTS ===")
    print(f"{'Rank':<5} | {'Window (s)':<10} | {'Loss Limit':<10} | {'Objective':<12} | {'Net PnL':<10} | {'Residual PnL':<12} | {'Fills':<6} | {'Completions':<11}")
    print("-" * 90)
    for i, res in enumerate(results[:25]):
        print(f"{i+1:<5} | {res['exit_window_secs']:<10.1f} | {res['exit_loss_limit']:<10.3f} | {res['objective']:<12.4f} | {res['pnl']:<10.4f} | {res['residual_pnl']:<12.4f} | {res['fills']:<6} | {res['completion_fills']:<11}")

if __name__ == "__main__":
    main()
