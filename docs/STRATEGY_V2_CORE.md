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
    L --> N{Inventory Gate}
    M --> N
    N -- can_buy=false --> G
    N -- can_buy=true --> O[Send SetTarget]
```

---

## 2. Dynamic Pricing Engine

The bot uses three distinct pricing regimes to balance profit, inventory, and safety.

### A. The "Provide" Regime (Balanced Market Making)
When inventory is within limits (`net_diff < max_net_diff`), the bot provides a two-sided market.
- **Base Price**: `mid_market - profit_margin`.
- **Inventory Skew (A-S Model)**: Bids are shifted downward (for the overweight side) or upward (for the underweight side) based on the `PM_AS_SKEW_FACTOR`.
- **Time Decay**: As the market approaches expiry, the skew urgency increases linearly (up to 3x by default).
- **Inventory Gate**: Provide orders are sent only if `can_buy_*` is true for that side.

### B. The "Hedge" Regime (Profit-Linked Unwinding)
When an imbalance exists, the bot prioritizes filling the "missing" side of a pair to lock in profit.
- **Ceiling**: `hedge_target - current_avg_cost`, where `hedge_target` ramps from `PM_PAIR_TARGET` to `PM_MAX_PORTFOLIO_COST` as `|net_diff|` approaches `PM_MAX_NET_DIFF`.
- **Goal**: Complete a pair while staying within the target cost (e.g., $0.985).
- **Inventory Gate**: Hedge orders are also subject to `can_buy_*`. There is no special privilege.

### C. The "Emergency Rescue" Regime (Risk Minimization)
When inventory reaches the hard limit (`net_diff >= max_net_diff`), the bot enters "rescue" mode.
- **Ceiling**: `PM_MAX_PORTFOLIO_COST - current_avg_cost` (only triggered once `|net_diff| >= PM_MAX_NET_DIFF`).
- **Goal**: Close directional risk even at a breakeven or slight loss (e.g., $1.02) to prevent being stuck in a directional crash.

### D. Inventory Gate (System-Wide, Non-Privileged)
All order types (Provide and Hedge) are gated by `can_buy_*`.
- **Rule**: If `can_buy_*` is false, price is forced to `0.0` and the target is cleared.
- **Rationale**: Prevents any privileged path from bypassing risk limits.

---

## 3. Risk Hardening & Protection

### Toxic Flow Protection (Lead-Lag Kill Switch)
The bot monitors the Order Flow Imbalance (OFI) on a 3-second sliding window.
- **Lead-Lag Logic**: If YES or NO becomes toxic, the system sets "Provide" prices on BOTH sides to 0.0 to flush out risk.
- **Hedge Allowance (Non-Privileged)**: Hedge orders may still be placed only if the hedge side itself is healthy and `can_buy_*` is true.

### Stale Book Protection (Configurable TTL)
To prevent "Blind Crossing" based on stale data, the system enforces a strict lifecyle check.
- **PM_STALE_TTL_MS**: Default 3000ms. If data is older than this threshold, the side is shut down and canceled.
- **Critical Expiry**: If either side is stale for 30s, the system clears both sides and refuses to quote.

### Empty Book Handling
If usable book data is unavailable, the bot will not place new orders. If the system is toxic or stale while the book is empty, existing targets are cleared to avoid dangling exposure.

### "Blind Cross" Prevention
Even with fresh data, the bot detects if its calculated Bid would cross the current Ask. Since the bot is **Maker-Only**, it will automatically clamp the price to **one tick below the Ask** instead of crossing.

---

## 4. Configuration Reference

| Parameter | Purpose | Critical Interaction |
| :--- | :--- | :--- |
| `PM_MAX_NET_DIFF` | Max allowed difference between Yes/No qty. | **Dynamic Sizing** may lower this for small accounts. |
| `PM_MAX_SIDE_SHARES` | Max per-side gross exposure (shares). | Independent cap to prevent large gross positions. |
| `PM_PAIR_TARGET` | Target cost for a Y+N pair. | Directly controls your profit margin. |
| `PM_AS_SKEW_FACTOR` | Aggressiveness of inventory-based pricing. | 0.00 = pure grid; 0.03 = standard A-S skew. |
| `PM_MAX_PORTFOLIO_COST`| Absolute survival cost ceiling. | Used ONLY for emergency inventory rescue. |
| `PM_MAX_LOSS_PCT`| Max allowable loss percent. | Clamps `PM_MAX_PORTFOLIO_COST` to `1 + max_loss_pct`. |
| `PM_STALE_TTL_MS` | Data Freshness Threshold | Default 3000ms. Shutdown if exceeded. |

## 5. Hedge Logic & Dynamic Sizing

In V2, hedging and rescue operations have the highest priority.
- **Dynamic Sizing**: `size = net_diff.abs()`.
- **Size Updates**: A size-only change is treated as a reprice and triggers `SetTarget`.
- **Principle**: We no longer use a fixed `PM_BID_SIZE` for rescue. We neutralize the exact inventory gap in a single step to return to neutral as quickly as possible.
- **Polymarket Constraint**: Minimum order size still applies (typically $1-$5 nominal value).

Hedge ceiling step:
```
hedge_target = if abs(net_diff) >= max_net_diff {
                 max_portfolio_cost
               } else {
                 pair_target
               }
```
`max_portfolio_cost` is clamped by `PM_MAX_LOSS_PCT` (default 0.02).
