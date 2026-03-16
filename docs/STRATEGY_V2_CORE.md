# Mastering the Polymarket V2 Maker-Only Strategy (Core Guide)

This document is the single source of truth for the `pm_as_ofi` trading engine.

---

## 1. Core Architecture & Unit Definition

### Unit Definition
In Polymarket binary option markets, **1 Share = $1 Max Potential Risk**.
- **PM_BID_SIZE** (Shares): Maximum risk per provide order.
- **PM_MAX_NET_DIFF** (Shares): Maximum directional net risk (|YES_qty - NO_qty|).
- **PM_MAX_SIDE_SHARES** (Shares): Max per-side gross exposure (independent cap).
- **Dynamic Sizing**: Balance × PCT computes live risk caps (`PM_BID_PCT`, `PM_NET_DIFF_PCT`) and runtime parameters are clamped under those caps.

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
- **Ceiling (incremental budget)**:
  - `net_diff > 0` (buy NO hedge):
    - `target_no_avg = hedge_target - yes_avg_cost`
    - `no_ceiling = (target_no_avg * (no_qty + size) - no_qty * no_avg_cost) / size`
  - `net_diff < 0` (buy YES hedge):
    - `target_yes_avg = hedge_target - no_avg_cost`
    - `yes_ceiling = (target_yes_avg * (yes_qty + size) - yes_qty * yes_avg_cost) / size`
  - If hedge-side qty is zero, this collapses to the old static form `ceiling = hedge_target - held_avg`.
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

### Marketable-BUY Minimum Notional (Optional)
Some venue-side validations can reject very small **marketable BUY** orders (< `$1` notional), while passive maker fills at low prices can still happen. These two facts are not contradictory.
- Optional hedge-only guard:
  - `PM_HEDGE_MIN_MARKETABLE_NOTIONAL` (default `0`, disabled)
  - `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA`
  - `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT`
- Behavior: when enabled, hedge size can be bumped just enough to pass the notional floor, but only within strict extra-size caps and still under `can_buy_*` inventory gates.

### Balance-Stress Recycle (Batch Merge)
When placement rejects indicate collateral stress, the bot uses **batch recycle** instead of frequent tiny merges:
- **Trigger**: balance/allowance rejects within `PM_RECYCLE_TRIGGER_WINDOW_SECS` reach `PM_RECYCLE_TRIGGER_REJECTS`.
- **Low-water gate**: run only when `free_balance < PM_RECYCLE_LOW_WATER_USDC`.
- **High-water refill target**:
  - `shortage = max(0, target_free - free_balance)`
  - `batch = clamp(max(shortage * shortfall_mult, min_batch), min_batch, max_batch)`
- **Anti-thrash**: `PM_RECYCLE_COOLDOWN_SECS` + `PM_RECYCLE_MAX_MERGES_PER_ROUND`.
- **Affordability precheck**: Executor checks free collateral before submit and locally rejects clearly unaffordable orders.

Notes:
- `allowance=0` is an auth/approval issue; merge cannot fix it.
- SAFE mode uses relayer merge (requires `POLYMARKET_BUILDER_*`), EOA mode uses direct on-chain merge.

---

## 4. Key Configuration Reference

| Parameter | Unit | Purpose | Key Interaction |
| :--- | :--- | :--- | :--- |
| `PM_BID_SIZE` | Shares | Provide order size | Dynamic: `balance × PM_BID_PCT` |
| `PM_MIN_ORDER_SIZE` | Shares | Minimum order size | Auto-detected from order_book if unset; orders below are skipped |
| `PM_MIN_HEDGE_SIZE` | Shares | Hedge trigger threshold | Hedges below are skipped |
| `PM_HEDGE_ROUND_UP` | bool | Hedge rounding | Round up small hedges to min size |
| `PM_HEDGE_MIN_MARKETABLE_NOTIONAL` | USDC | Optional marketable-BUY floor for hedges | `0` disables; avoids tiny-notional reject bursts |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA` | Shares | Absolute extra-size cap for hedge bump | Limits risk increase when floor is enabled |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT` | Decimal | Relative extra-size cap for hedge bump | `extra <= size * pct` must hold |
| `PM_MAX_NET_DIFF` | Shares | Max directional risk | Dynamic: `balance × PM_NET_DIFF_PCT` |
| `PM_MAX_SIDE_SHARES` | Shares | Max per-side gross exposure | Default = max_net_diff if unset |
| `PM_MAX_POS_PCT` | Decimal | Target gross utilization | Dynamic: `balance × pct / pair_target` |
| `PM_PAIR_TARGET` | Cost | Target Y+N pair cost | Profit margin = `1.00 - pair_target` |
| `PM_AS_SKEW_FACTOR` | Factor | A-S skew aggressiveness | 0.00 = pure grid; 0.03 = standard A-S |
| `PM_AS_TIME_DECAY_K` | Factor | Time decay amplifier | 0.0 = disabled; 2.0 = 3x skew at expiry |
| `PM_MAX_PORTFOLIO_COST` | Cost | Emergency rescue ceiling | Clamped to `1 + PM_MAX_LOSS_PCT` |
| `PM_MAX_LOSS_PCT` | Decimal | Max acceptable loss in rescue | Clamps max_portfolio_cost at startup |
| `PM_STALE_TTL_MS` | ms | Per-side freshness TTL | Side shutdown if exceeded |
| `PM_DEBOUNCE_MS` | ms | Provide order anti-thrash | Prevents rapid re-quoting |
| `PM_HEDGE_DEBOUNCE_MS` | ms | Hedge order anti-thrash | Lower (100ms) for urgency |
| `PM_RECONCILE_INTERVAL_SECS` | sec | REST order reconciliation | Detects WS blind spots |
| `PM_RECYCLE_TRIGGER_REJECTS` | count | Reject threshold for recycle trigger | Used with `PM_RECYCLE_TRIGGER_WINDOW_SECS` |
| `PM_RECYCLE_LOW_WATER_USDC` | USDC | Recycle low-water gate | No recycle above this free balance |
| `PM_RECYCLE_TARGET_FREE_USDC` | USDC | Recycle high-water target | Batch refill objective |
| `PM_RECYCLE_MIN_BATCH_USDC` | USDC | Minimum recycle batch | Avoid tiny frequent merges |
| `PM_RECYCLE_MAX_BATCH_USDC` | USDC | Maximum recycle batch | Caps single-merge gas/risk |
| `PM_BALANCE_CACHE_TTL_MS` | ms | Precheck balance cache TTL | Reduces repeated balance RPC calls |

## 5. Hedge Logic & Sizing

- **Size**: `net_diff.abs()` — exact 1:1 gap neutralization.
- **Principle**: Fixed `PM_BID_SIZE` is NOT used for hedging.

```
hedge_target = if abs(net_diff) >= max_net_diff {
                 max_portfolio_cost   // rescue ceiling (capped at 1 + loss_pct)
               } else {
                 pair_target          // normal profit line
               }

if net_diff > 0 { // buy NO
  target_no_avg = hedge_target - yes_avg_cost
  ceiling_no = (target_no_avg * (no_qty + size) - no_qty * no_avg_cost) / size
}
if net_diff < 0 { // buy YES
  target_yes_avg = hedge_target - no_avg_cost
  ceiling_yes = (target_yes_avg * (yes_qty + size) - yes_qty * yes_avg_cost) / size
}
```

Live debugging note:
- If logs show `not enough balance / allowance`, that side enters OMS cooldown (default 30s). Hedge intent may continue to be computed, but placements are suppressed until cooldown expiry.
