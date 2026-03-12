# Mastering the Polymarket V2 Maker-Only Strategy (Core Guide)

This document serves as the single source of truth for the `pm_as_ofi` trading engine. It covers the mathematical models, decision loops, and risk management systems that power the bot.

---

## 1. Core Architecture & Unit Definition

### Unit Definition (Units)
In the Polymarket binary option market, **1 Share = $1 of Max Potential Risk**.
- **PM_BID_SIZE** (Shares): Determines the maximum risk per transaction.
- **PM_MAX_NET_DIFF** (Shares): Determines the maximum directional net risk allowed by the system.
- **Dynamic Sizing**: Dynamic amounts ($) calculated from your balance are automatically mapped 1:1 to **Shares**. This ensures your risk-to-equity ratio remains constant across different price points.

### The Decision Loop
The bot operates on a high-frequency event loop. Every order book update or trade execution triggers a re-evaluation of the strategy's desired state.

```mermaid
graph TD
    A[Market Data Update] --> B{Sequential Health Check}
    B -- Toxic/Stale/Expired --> C[Health Override]
    B -- Healthy --> D[Unified Pricing Mode]
    
    C --> G[Set Price to 0.0]
    G --> H[Auto-Trigger Cancellation]
    
    D --> I[state_unified]
    I --> J[Apply A-S Skew + Gabagool Averaging]
    J --> K{Inventory Gap?}
    K -- Yes --> L[Place Dynamic Hedge sz=abs_net]
    K -- No --> M[Place Standard Provide sz=bid_size]
```

---

## 2. Dynamic Pricing Engine

The bot uses three distinct pricing regimes to balance profit, inventory, and safety.

### A. The "Provide" Regime (Balanced Market Making)
When inventory is within limits (`net_diff < max_net_diff`), the bot provides a two-sided market.
- **Base Price**: `mid_market - profit_margin`.
- **Inventory Skew (A-S Model)**: Bids are shifted downward (for the overweight side) or upward (for the underweight side) based on the `PM_AS_SKEW_FACTOR`.
- **Time Decay**: As the market approaches expiry, the skew urgency increases linearly (up to 3x by default).

### B. The "Hedge" Regime (Profit-Linked Unwinding)
When an imbalance exists, the bot prioritizes filling the "missing" side of a pair to lock in profit.
- **Ceiling**: `PM_PAIR_TARGET - current_avg_cost`.
- **Goal**: Complete a pair while staying within the target cost (e.g., $0.985).

### C. The "Emergency Rescue" Regime (Risk Minimization)
When inventory reaches the hard limit (`net_diff >= max_net_diff`), the bot enters "rescue" mode.
- **Ceiling**: `PM_MAX_PORTFOLIO_COST - current_avg_cost`.
- **Goal**: Close directional risk even at a breakeven or slight loss (e.g., $1.02) to prevent being stuck in a directional crash.

---

## 3. Risk Hardening & Protection

### Toxic Flow Protection (Lead-Lag Kill Switch)
The bot monitors the Order Flow Imbalance (OFI) on a 3-second sliding window.
- **Lead-Lag Logic**: If YES or NO becomes toxic, the system sets "Provide" prices on BOTH sides to 0.0 to flush out risk.
- **Hedge Exception**: If we need a hedge and the hedge side is healthy, the system **preserves** the hedge order while killing the risky side.

### Stale Book Protection (Configurable TTL)
To prevent "Blind Crossing" based on stale data, the system enforces a strict lifecyle check.
- **PM_STALE_TTL_MS**: Default 3000ms. If data is older than this threshold, the side is shut down and canceled.

### "Blind Cross" Prevention
Even with fresh data, the bot detects if its calculated Bid would cross the current Ask. Since the bot is **Maker-Only**, it will automatically clamp the price to **one tick below the Ask** instead of crossing.

---

## 4. Configuration Reference

| Parameter | Purpose | Critical Interaction |
| :--- | :--- | :--- |
| `PM_MAX_NET_DIFF` | Max allowed difference between Yes/No qty. | **Dynamic Sizing** may lower this for small accounts. |
| `PM_PAIR_TARGET` | Target cost for a Y+N pair. | Directly controls your profit margin. |
| `PM_AS_SKEW_FACTOR` | Aggressiveness of inventory-based pricing. | 0.00 = pure grid; 0.03 = standard A-S skew. |
| `PM_MAX_PORTFOLIO_COST`| Absolute survival cost ceiling. | Used ONLY for emergency inventory rescue. |
| `PM_STALE_TTL_MS` | Data Freshness Threshold | Default 3000ms. Shutdown if exceeded. |

## 5. Hedge Logic & Dynamic Sizing

In V2, hedging and rescue operations have the highest priority.
- **Dynamic Sizing**: `size = net_diff.abs()`.
- **Principle**: We no longer use a fixed `PM_BID_SIZE` for rescue. We neutralize the exact inventory gap in a single step to return to neutral as quickly as possible.
- **Polymarket Constraint**: Minimum order size still applies (typically $1-$5 nominal value).
