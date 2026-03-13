# Mastering the Polymarket V2 Maker-Only Strategy (Core Guide)

This document is the single source of truth for the `pm_as_ofi` trading engine.

---

## 1. Core Architecture & Unit Definition

### Unit Definition
In Polymarket binary option markets, **1 Share = $1 Max Potential Risk**.
- **PM_BID_SIZE** (Shares): Maximum risk per provide order.
- **PM_MAX_NET_DIFF** (Shares): Maximum directional net risk (|YES_qty - NO_qty|).
- **PM_MAX_SIDE_SHARES** (Shares): Max per-side gross exposure (independent cap).
- **Dynamic Sizing**: Balance × PCT auto-maps to Shares, keeping risk-to-equity constant.

### Channel Architecture

| Channel | Type | Rationale |
|:---|:---|:---|
| Market Data → Coordinator | `watch::channel` | Only latest tick kept; eliminates backlog automatically |
| Market Data → OFI | `mpsc(512)` | OFI needs full trade history for window calculation |
| Coordinator → OMS | `mpsc(64)` | ≤2 msgs/tick; ample capacity |
| OFI Kill → Coordinator | `mpsc(4)` | `biased select!` gives absolute priority |
| Fill fanout → IM / Executor | `mpsc(64)` | Splitter task broadcasts fills |

### Decision Loop (Priority Order)

```
tick():
  P1: >30s global stale guard  → clear both sides, return
  P2: Toxic / 3s-stale         → pre-emptive cancel (even on empty book), stats counted here
  P3: Empty book               → skip new orders, return
  P4: state_unified()          → price + hedge + dispatch
```

---

## 2. Dynamic Pricing Engine

### A. "Provide" Regime (Balanced Market Making)
When `|net_diff| < max_net_diff`, the bot provides a two-sided market.
- **Price**: `mid - (excess / 2) ± A-S skew`. Clamped below `best_ask - tick`.
- **Time Decay**: Skew urgency grows linearly (up to 3x at expiry with default `k=2.0`).
- **Gate**: Only dispatched if `can_buy_yes/no(bid_size)` is true.

### B. "Hedge" Regime (Profit-Linked Unwinding)
With existing imbalance, prioritizes filling the missing pair side.
- **Ceiling**: `hedge_target - avg_cost_held_side`.
- **Step function**:
  - `|net_diff| < max_net_diff` → `hedge_target = pair_target`
  - `|net_diff| >= max_net_diff` → `hedge_target = max_portfolio_cost`
- **Gate**: Same `can_buy_*` rules. No privileged path.

### C. Emergency Rescue (Risk Minimization)
Triggered only when `|net_diff| >= max_net_diff`.
- Accept breakeven or slight loss (≤ `PM_MAX_LOSS_PCT` = 2%) to close directional risk.
- Hard cap: `max_portfolio_cost <= 1 + PM_MAX_LOSS_PCT` enforced at startup.

### D. Inventory Gate (System-Wide, Dual-Lock)

```rust
// Both conditions must pass for any order (Provide or Hedge)
net_ok  = (inv.net_diff +/- size).abs() <= max_net_diff    // directional risk
side_ok = (inv.yes/no_qty + size)       <= max_side_shares  // gross exposure
allow   = net_ok && side_ok
```

---

## 3. Risk Hardening & Protection

### Toxic Flow Protection (Lead-Lag Kill Switch)
OFI engine monitors a 3s sliding window. On toxicity:
- **P2 pre-emptive cancel**: Fires immediately via `biased select!` even on empty book.
- Kill signal bypasses md_rx latency via dedicated `mpsc(4)` channel.

### Stale Book Protection
- **PM_STALE_TTL_MS** (default 3000ms): Per-side TTL. Exceeded → cancel that side.
- **30s global guard**: Either side stale >30s → clear both sides, stop quoting.

### Maker-Only Enforcement
All bid prices are clamped to `best_ask - tick_size`. Post-Only orders refused if book is empty or crossed.

---

## 4. Key Configuration Reference

| Parameter | Unit | Purpose | Key Interaction |
| :--- | :--- | :--- | :--- |
| `PM_BID_SIZE` | Shares | Provide order size | Dynamic: `balance × PM_BID_PCT` |
| `PM_MAX_NET_DIFF` | Shares | Max directional risk | Dynamic: `balance × PM_NET_DIFF_PCT` |
| `PM_MAX_SIDE_SHARES` | Shares | Max per-side gross exposure | Default = max_net_diff if unset |
| `PM_PAIR_TARGET` | Cost | Target Y+N pair cost | Profit margin = `1.00 - pair_target` |
| `PM_AS_SKEW_FACTOR` | Factor | A-S skew aggressiveness | 0.00 = pure grid; 0.03 = standard A-S |
| `PM_AS_TIME_DECAY_K` | Factor | Time decay amplifier | 0.0 = disabled; 2.0 = 3x skew at expiry |
| `PM_MAX_PORTFOLIO_COST` | Cost | Emergency rescue ceiling | Clamped to `1 + PM_MAX_LOSS_PCT` |
| `PM_MAX_LOSS_PCT` | Decimal | Max acceptable loss in rescue | Clamps max_portfolio_cost at startup |
| `PM_STALE_TTL_MS` | ms | Per-side freshness TTL | Side shutdown if exceeded |
| `PM_DEBOUNCE_MS` | ms | Provide order anti-thrash | Prevents rapid re-quoting |
| `PM_HEDGE_DEBOUNCE_MS` | ms | Hedge order anti-thrash | Lower (100ms) for urgency |

## 5. Hedge Logic & Sizing

- **Size**: `net_diff.abs()` — exact 1:1 gap neutralization.
- **Principle**: Fixed `PM_BID_SIZE` is NOT used for hedging.

```
hedge_target = if abs(net_diff) >= max_net_diff {
                 max_portfolio_cost   // rescue ceiling (capped at 1 + loss_pct)
               } else {
                 pair_target          // normal profit line
               }
ceiling = hedge_target - avg_cost_of_imbalanced_side
```
