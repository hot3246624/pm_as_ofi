#!/usr/bin/env python3
import subprocess
import json
import sys
import numpy as np
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "xuan_research_artifacts" / "eval_compat_store.sqlite"

sys.path.append(str(REPO_ROOT / "scripts"))
from xuan_evaluate_candidate import CHAMPION_CONFIG

def get_window_data():
    print("Building pair_arb_backtest...")
    subprocess.run(["cargo", "build", "--release", "--bin", "pair_arb_backtest"], cwd=REPO_ROOT, check=True)

    print("Extracting window-by-window metrics from backtester...")
    window_records = []
    
    cmd_base = [
        "./target/release/pair_arb_backtest",
        "--jsonl",
        "--db", str(DEFAULT_DB),
        "--limit", "1",
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
        "--salvage-failure-probability", "0.10",
    ]
    if CHAMPION_CONFIG["reject_stale"]:
        cmd_base.append("--reject-stale")
    if CHAMPION_CONFIG["require_ws_fresh"]:
        cmd_base.append("--require-ws-fresh")
    if CHAMPION_CONFIG["pairing_only_when_residual"]:
        cmd_base.append("--pairing-only-when-residual")
    if CHAMPION_CONFIG["require_two_sided_entry"]:
        cmd_base.append("--require-two-sided-entry")

    for s in range(300):
        cmd = cmd_base + ["--skip", str(s)]
        proc = subprocess.run(cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        for line in proc.stdout.strip().split("\n"):
            if not line:
                continue
            try:
                row = json.loads(line)
                metrics = row.get("metrics") or {}
                window_records.append({
                    "total_pnl": metrics.get("total_pnl", 0.0),
                    "fills": metrics.get("fills", 0.0),
                    "paired_qty": metrics.get("paired_qty", 0.0),
                    "residual_qty": metrics.get("residual_qty", 0.0),
                    "residual_pnl": metrics.get("residual_pnl", 0.0),
                    "completion_qty": metrics.get("completion_qty", 0.0),
                })
            except Exception as e:
                print(f"Failed to parse skip {s}: {e}")
                
    print(f"Loaded {len(window_records)} windows successfully.")
    return window_records

def run_portfolio_sim(window_records, num_categories, num_trials=1000, initial_capital=300.0, risk_fraction=0.10):
    # max_net_diff is 0.5 shares per 0.2 clip size.
    # Capital required for a share is at most $1.00.
    # Peak capital locked per category = 0.5 * clip_size.
    max_net_diff = 0.5
    
    # We will simulate and compare:
    # 1. Siloed Allocation: Peak position limit per category is restricted to (risk_fraction * Capital / N).
    #    Clip size = (risk_fraction * Capital / N) / max_net_diff.
    # 2. Shared (Dynamic) Allocation with sqrt(N) scaling:
    #    Clip size = (risk_fraction * Capital / sqrt(N)) / max_net_diff.
    
    results = {
        "siloed": {
            "ending_capital": [],
            "total_roi": [],
            "bankruptcies": 0,
        },
        "shared": {
            "ending_capital": [],
            "total_roi": [],
            "bankruptcies": 0,
            "capital_breach_count": 0, # Times worst-case peak capital exceeds actual available capital
        }
    }
    
    np.random.seed(42)
    
    for trial in range(num_trials):
        cap_siloed = initial_capital
        cap_shared = initial_capital
        
        for t in range(300):
            indices = np.random.randint(0, len(window_records), size=num_categories)
            
            # --- SILOED POLICY ---
            if cap_siloed > 0.01:
                clip_size_siloed = (risk_fraction * cap_siloed / num_categories) / max_net_diff
                scale_siloed = clip_size_siloed / 0.2
                pnl_siloed = 0.0
                for idx in indices:
                    pnl_siloed += window_records[idx]["total_pnl"] * scale_siloed
                cap_siloed = max(0.0, cap_siloed + pnl_siloed)
            else:
                cap_siloed = 0.0
                
            # --- SHARED POLICY ---
            if cap_shared > 0.01:
                clip_size_shared = (risk_fraction * cap_shared / np.sqrt(num_categories)) / max_net_diff
                scale_shared = clip_size_shared / 0.2
                
                # Check potential over-exposure breach
                worst_case_peak_cap = num_categories * (max_net_diff * clip_size_shared)
                if worst_case_peak_cap > cap_shared:
                    results["shared"]["capital_breach_count"] += 1
                    
                pnl_shared = 0.0
                for idx in indices:
                    pnl_shared += window_records[idx]["total_pnl"] * scale_shared
                cap_shared = max(0.0, cap_shared + pnl_shared)
            else:
                cap_shared = 0.0
                
        results["siloed"]["ending_capital"].append(cap_siloed)
        results["siloed"]["total_roi"].append((cap_siloed - initial_capital) / initial_capital * 100)
        if cap_siloed <= 0.01:
            results["siloed"]["bankruptcies"] += 1
            
        results["shared"]["ending_capital"].append(cap_shared)
        results["shared"]["total_roi"].append((cap_shared - initial_capital) / initial_capital * 100)
        if cap_shared <= 0.01:
            results["shared"]["bankruptcies"] += 1
            
    return results

def main():
    window_records = get_window_data()
    
    categories = [1, 2, 3, 5, 10]
    initial_cap = 300.0
    
    # We will run simulations with a conservative 10% risk fraction, and a moderate 20% risk fraction
    for risk in [0.10, 0.20]:
        print("\n" + "="*80)
        print(f"MULTI-CATEGORY CAPITAL SHARING & COMPOUNDING SIMULATION")
        print(f"Risk Fraction: {risk*100:.1f}% (Peak margin limit per asset = {risk*100:.1f}% of capital)")
        print(f"Initial Capital: {initial_cap} USDC | Period: 16 days (300 epochs)")
        print("="*80)
        
        for N in categories:
            res = run_portfolio_sim(window_records, num_categories=N, num_trials=1000, initial_capital=initial_cap, risk_fraction=risk)
            
            siloed_caps = np.array(res["siloed"]["ending_capital"])
            siloed_rois = np.array(res["siloed"]["total_roi"])
            siloed_bankrupt = res["siloed"]["bankruptcies"]
            
            shared_caps = np.array(res["shared"]["ending_capital"])
            shared_rois = np.array(res["shared"]["total_roi"])
            shared_bankrupt = res["shared"]["bankruptcies"]
            
            breach_rate = (res["shared"]["capital_breach_count"] / (1000 * 300)) * 100
            
            print(f"\n--- concurrent Categories: N = {N} ---")
            print(f"Siloed Capital Policy (Conservative, 100% Capital Safe):")
            print(f"  Clip Size Limit (Start): {(risk * initial_cap / N) / 0.5:.2f} shares")
            print(f"  Mean Ending Cap:         {np.mean(siloed_caps):.2f} USDC")
            print(f"  Mean Total ROI:          {np.mean(siloed_rois):.2f}% (Std: {np.std(siloed_rois):.2f}%)")
            print(f"  Bankruptcy Rate:         {siloed_bankrupt/10.0:.1f}% ({siloed_bankrupt}/1000)")
            print(f"  Daily Net Profit:        {(np.mean(siloed_caps) - initial_cap)/16.0:.2f} USDC/day")
            print(f"Shared Capital Policy (Dynamic, Over-allocated by sqrt(N)):")
            print(f"  Clip Size Limit (Start): {(risk * initial_cap / np.sqrt(N)) / 0.5:.2f} shares")
            print(f"  Mean Ending Cap:         {np.mean(shared_caps):.2f} USDC")
            print(f"  Mean Total ROI:          {np.mean(shared_rois):.2f}% (Std: {np.std(shared_rois):.2f}%)")
            print(f"  Bankruptcy Rate:         {shared_bankrupt/10.0:.1f}% ({shared_bankrupt}/1000)")
            print(f"  Daily Net Profit:        {(np.mean(shared_caps) - initial_cap)/16.0:.2f} USDC/day")
            print(f"  Worst-Case Capital Over-exposure Rate: {breach_rate:.2f}%")
        
    print("="*80)

if __name__ == "__main__":
    main()
