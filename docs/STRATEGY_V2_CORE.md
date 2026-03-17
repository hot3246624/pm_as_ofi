# Mastering the Polymarket V2 Maker-Only Strategy (Core Guide)

This document is the single source of truth for the `pm_as_ofi` trading engine.

---

## 1. Core Architecture & Unit Definition

### Unit Definition
In Polymarket binary option markets, **1 Share = $1 Max Potential Risk**.
- **PM_BID_SIZE** (Shares): Maximum risk per provide order.
- **PM_MAX_NET_DIFF** (Shares): Maximum directional net risk (|YES_qty - NO_qty|).
- **PM_MAX_SIDE_SHARES** (Shares): Max per-side gross exposure (independent cap).
- **Dynamic Sizing (Explicit Opt-In)**: `PM_BID_SIZE` / `PM_MAX_NET_DIFF` are hard floors; when `PM_BID_PCT` / `PM_NET_DIFF_PCT` are configured, runtime targets are `balance*pct`, and effective values are `max(target, floor)`.

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
- **Gate**: Hedge path uses `can_hedge_buy_*` (net-only) to prioritize directional de-risking; provide path remains `can_buy_*` (net+side).

### C. Emergency Rescue (Risk Minimization)
Triggered only when `|net_diff| >= max_net_diff`.
- Accept breakeven or slight loss (≤ `PM_MAX_LOSS_PCT` = 2%) to close directional risk.
- Hard cap: `max_portfolio_cost <= 1 + PM_MAX_LOSS_PCT` enforced at startup.

### D. Inventory Gate (System-Wide, Layered)

```rust
// Provide path: net + side dual gate
net_ok  = (inv.net_diff +/- size).abs() <= max_net_diff    // directional risk
side_ok = (inv.yes/no_qty + size)       <= max_side_shares  // gross exposure
allow   = net_ok && side_ok
```

```rust
// Hedge path: net-only gate (de-risk priority)
allow_hedge_yes = inv.net_diff + size <=  max_net_diff
allow_hedge_no  = inv.net_diff - size >= -max_net_diff
```

Provide and hedge now use split gating:
- Provide uses `can_buy_*` (`net + side`) for gross risk control.
- Hedge uses `can_hedge_buy_*` (`net-only`) to prioritize directional de-risking even when dynamic side caps shrink.

---

## 3. Risk Hardening & Protection

### Toxic Flow Protection (Lead-Lag Kill Switch)
OFI engine monitors a 3s sliding window. On toxicity:
- **P2 pre-emptive cancel**: Fires immediately via `biased select!` even on empty book.
- Kill signal bypasses md_rx latency via dedicated `mpsc(4)` channel.
- Toxic exit threshold is **frozen at entry threshold** (`entry_threshold * PM_OFI_EXIT_RATIO`) to avoid premature recovery when adaptive threshold rises.
- `PM_OFI_ADAPTIVE_RISE_CAP_PCT` limits per-heartbeat adaptive threshold growth to prevent moving-target drift.

### Stale Book Protection
- **PM_STALE_TTL_MS** (default 3000ms): Per-side TTL. Exceeded → cancel that side.
- **30s global guard**: Either side stale >30s → clear both sides, stop quoting.
- **PM_COORD_WATCHDOG_MS**: coordinator periodic watchdog tick; stale/toxic guards still run when WS stream goes silent.

### Maker-Only Enforcement
All bid prices are clamped to `best_ask - tick_size`. Post-Only orders refused if book is empty or crossed.

### Marketable-BUY Minimum Notional & Reject-Storm Control
Some venue-side validations can reject very small **marketable BUY** orders (< `$1` notional), while passive maker fills at low prices can still happen. These two facts are not contradictory.

Static-first defaults:
- `PM_MIN_MARKETABLE_NOTIONAL_FLOOR=0`
- `PM_MIN_MARKETABLE_AUTO_DETECT=false`
- This means global min-notional precheck is off by default to preserve low-price opportunities.

- Optional hedge-only guard:
  - `PM_HEDGE_MIN_MARKETABLE_NOTIONAL` (default `0`, disabled)
  - `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA`
  - `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT`
- Global anti-storm guard:
  - `PM_MIN_MARKETABLE_NOTIONAL_FLOOR` (default `0`, disabled)
  - `PM_MIN_MARKETABLE_AUTO_DETECT` (default `false`, static-first; optional learning from rejects)
  - `PM_MIN_MARKETABLE_COOLDOWN_MS` (default `10000`)
- Behavior:
  - Even with precheck disabled, executor classifies marketable-min rejects and applies side cooldown to avoid retry storms.
  - If precheck is enabled, orders below floor are rejected locally with cooldown.
  - Hedge bump remains minimal and bounded by absolute/relative extra-size caps plus net-only hedge gates.

### Balance-Stress Recycle (Batch Merge)
When placement rejects indicate collateral stress, the bot uses **batch recycle** instead of frequent tiny merges:
- **Trigger**: balance/allowance rejects within `PM_RECYCLE_TRIGGER_WINDOW_SECS` reach `PM_RECYCLE_TRIGGER_REJECTS`.
  - Default `PM_RECYCLE_ONLY_HEDGE=false`: both `Hedge` and `Provide` balance rejects can trigger recycle.
  - Set `PM_RECYCLE_ONLY_HEDGE=true` to count hedge rejects only.
- **Low-water gate**: run only when `free_balance < PM_RECYCLE_LOW_WATER_USDC`.
- **High-water refill target**:
  - `shortage = max(0, target_free - free_balance)`
  - `batch = clamp(max(shortage * shortfall_mult, min_batch), min_batch, max_batch)`
- **Anti-thrash**: `PM_RECYCLE_COOLDOWN_SECS` + `PM_RECYCLE_MAX_MERGES_PER_ROUND`.
- **Affordability precheck**: Executor checks free collateral before submit and locally rejects clearly unaffordable orders.

Notes:
- Allowance parsing is unified with startup preflight under `U256` semantics, preventing false zero on very large allowances.
- Recycler skip logs now include `skip_reason`, `free_balance`, and `allowance_ok`.
- SAFE mode uses relayer merge (requires `POLYMARKET_BUILDER_*`), EOA mode uses direct on-chain merge.

### Round-Scoped Claim 30s SLA
After each `Market ended`, a dedicated claim runner executes within a 30s SLA window:
- **Window**: `PM_AUTO_CLAIM_ROUND_WINDOW_SECS=30`.
- **Retry cadence**: default `0,2,5,9,14,20,27` (overridable by `PM_AUTO_CLAIM_ROUND_RETRY_SCHEDULE`).
- **Scope order**: default `PM_AUTO_CLAIM_ROUND_SCOPE=ended_then_global`, i.e. ended condition first, then global fallback.
- **Interval gate bypass**: this round runner bypasses `PM_AUTO_CLAIM_INTERVAL_SECONDS`; startup/periodic claim checks remain as fallback.
- **Expected logs**: `start -> retry -> result -> success|SLA exhausted`.

---

## 4. Key Configuration Reference

| Parameter | Unit | Purpose | Key Interaction |
| :--- | :--- | :--- | :--- |
| `PM_BID_SIZE` | Shares | Provide order size floor | Used directly when `PM_BID_PCT` is unset |
| `PM_MIN_ORDER_SIZE` | Shares | Minimum order size | Auto-detected from order_book if unset; orders below are skipped |
| `PM_MIN_HEDGE_SIZE` | Shares | Hedge trigger threshold | Hedges below are skipped |
| `PM_HEDGE_ROUND_UP` | bool | Hedge rounding | Round up small hedges to min size |
| `PM_HEDGE_MIN_MARKETABLE_NOTIONAL` | USDC | Optional marketable-BUY floor for hedges | `0` disables; avoids tiny-notional reject bursts |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA` | Shares | Absolute extra-size cap for hedge bump | Limits risk increase when floor is enabled |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT` | Decimal | Relative extra-size cap for hedge bump | `extra <= size * pct` must hold |
| `PM_MIN_MARKETABLE_NOTIONAL_FLOOR` | USDC | Global marketable-BUY precheck floor | `0` disables |
| `PM_MIN_MARKETABLE_AUTO_DETECT` | bool | Auto-learn floor from exchange rejects | Static-first recommended `false` |
| `PM_MIN_MARKETABLE_COOLDOWN_MS` | ms | Cooldown after min-notional reject | Suppresses reject storms |
| `PM_MAX_NET_DIFF` | Shares | Max directional risk floor | Used directly when `PM_NET_DIFF_PCT` is unset; dynamic mode keeps this as minimum |
| `PM_MAX_SIDE_SHARES` | Shares | Max per-side gross exposure | Default = max_net_diff if unset |
| `PM_MAX_POS_PCT` | Decimal | Target gross utilization | `0` disables dynamic gross; `>0` enables `balance × pct / pair_target` |
| `PM_PAIR_TARGET` | Cost | Target Y+N pair cost | Profit margin = `1.00 - pair_target` |
| `PM_AS_SKEW_FACTOR` | Factor | A-S skew aggressiveness | 0.00 = pure grid; 0.03 = standard A-S |
| `PM_OFI_ADAPTIVE` | bool | Enable adaptive OFI threshold | Off = static threshold |
| `PM_OFI_ADAPTIVE_K` | Factor | Adaptive threshold amplifier | `threshold = mean + k*sigma` |
| `PM_OFI_ADAPTIVE_MIN` | Shares | Adaptive floor | Avoid false positives in thin flow |
| `PM_OFI_ADAPTIVE_MAX` | Shares | Adaptive hard cap | `0` disables hard upper cap |
| `PM_OFI_ADAPTIVE_RISE_CAP_PCT` | Decimal | Adaptive threshold rise cap per heartbeat | `0` disables; default `0.20` |
| `PM_OFI_RATIO_ENTER` | Decimal | Toxicity entry ratio gate | `|buy-sell|/(buy+sell)` |
| `PM_OFI_RATIO_EXIT` | Decimal | Toxicity exit ratio gate | Should be <= entry ratio |
| `PM_AS_TIME_DECAY_K` | Factor | Time decay amplifier | 0.0 = disabled; 2.0 = 3x skew at expiry |
| `PM_MAX_PORTFOLIO_COST` | Cost | Emergency rescue ceiling | Clamped to `1 + PM_MAX_LOSS_PCT` |
| `PM_MAX_LOSS_PCT` | Decimal | Max acceptable loss in rescue | Clamps max_portfolio_cost at startup |
| `PM_STALE_TTL_MS` | ms | Per-side freshness TTL | Side shutdown if exceeded |
| `PM_COORD_WATCHDOG_MS` | ms | Coordinator watchdog tick | Enforces stale/toxic checks without new md events |
| `PM_DEBOUNCE_MS` | ms | Provide order anti-thrash | Prevents rapid re-quoting |
| `PM_HEDGE_DEBOUNCE_MS` | ms | Hedge order anti-thrash | Lower (100ms) for urgency |
| `PM_RECONCILE_INTERVAL_SECS` | sec | REST order reconciliation | Detects WS blind spots |
| `PM_WS_CONNECT_TIMEOUT_MS` | ms | Market WS connect timeout | Fail-fast reconnect on TLS/network stalls |
| `PM_RESOLVE_TIMEOUT_MS` | ms | Gamma resolve request timeout | Limits single resolve stall impact |
| `PM_RESOLVE_RETRY_ATTEMPTS` | count | Resolve retries per round | Exponential backoff before giving up |
| `PM_RECYCLE_TRIGGER_REJECTS` | count | Reject threshold for recycle trigger | Used with `PM_RECYCLE_TRIGGER_WINDOW_SECS` |
| `PM_RECYCLE_ONLY_HEDGE` | bool | Recycle trigger scope | `false`=Hedge+Provide, `true`=Hedge only |
| `PM_RECYCLE_LOW_WATER_USDC` | USDC | Recycle low-water gate | No recycle above this free balance |
| `PM_RECYCLE_TARGET_FREE_USDC` | USDC | Recycle high-water target | Batch refill objective |
| `PM_RECYCLE_MIN_BATCH_USDC` | USDC | Minimum recycle batch | Avoid tiny frequent merges |
| `PM_RECYCLE_MAX_BATCH_USDC` | USDC | Maximum recycle batch | Caps single-merge gas/risk |
| `PM_BALANCE_CACHE_TTL_MS` | ms | Precheck balance cache TTL | Reduces repeated balance RPC calls |
| `PM_AUTO_CLAIM_ROUND_WINDOW_SECS` | sec | Per-round claim SLA window | Default 30s after market end |
| `PM_AUTO_CLAIM_ROUND_RETRY_MODE` | enum | Round claim retry mode | Default `exponential` |
| `PM_AUTO_CLAIM_ROUND_SCOPE` | enum | Round claim scope order | `ended_then_global/ended_only/global_only` |
| `PM_AUTO_CLAIM_ROUND_RETRY_SCHEDULE` | sec list | Round claim retry offsets | Default `0,2,5,9,14,20,27` |

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
