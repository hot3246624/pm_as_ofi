# Fix: Eliminate Regime-Transition Cancel Cascades that Inflate replace_ratio

## Context

We've been adding increasingly complex governance layers (debt, shadow targets, policy buckets, dwells, forced realign) to reduce replace_ratio, but the metric isn't converging. Deep analysis of `logs/polymarket.log.2026-04-01` reveals the root cause is **not** in the policy publish system — it's in the **structural cancel→reinitial cycle** caused by a bug in the absent-intent defer logic.

### Root Cause: Two distinct churn sources

**Source A — Regime-transition cancel cascades (the bug, primary):**

When GLFT regime transitions (e.g., Aligned→Guarded), `shape_glft_quotes` suppresses the dominant side's BUY intent (`dominant_buy_suppressed = true` for Guarded/Blocked). This causes `execute_slot_market_making` → `slot_quote_allowed(intent=None)` → false → cancel.

The critical bug is in `should_defer_absent_reprice_clear` (coordinator_execution.rs:592): the **distance check executes BEFORE the warmup dwell**, so an order gets evicted **instantly on the very first absent-intent tick** if it exceeds the regime's hard_stale threshold — completely bypassing the dwell protection that was designed to prevent exactly this.

Traced from the log:
```
04:51:24.016  GLFT regime → Guarded (drift=9.9)
04:51:24.018  CANCEL YES_BUY (Reprice)          ← 2ms later! Dwell bypassed.
...11 seconds of NO order on book...
04:51:35     GLFT regime → Tracking (bounce back)
...8 seconds rebuilding policy state...
04:51:43     Initial YES_BUY @0.700              ← 19s total gap
```

The cancel wipes ALL policy state (`reset_slot_publish_state`), forcing a full commit_dwell + policy_dwell rebuild (10+ seconds) before a new order can be placed.

**Source B — Policy replaces in volatile markets (correct behavior):**

When BTC moves 16+ ticks (normal for 5-min binary), the policy system correctly reprices. This is well-governed (5.4s dwell minimum between publishes) and cannot be reduced further without making quotes stale.

### Why previous fixes didn't converge

Every commit in the chain (debt thresholds, debounce, forced_realign narrowing, etc.) targeted Source B — the policy publish governance. But the dominant churn comes from Source A, which lives in a completely different code path (`execute_slot_market_making` → `should_defer_absent_reprice_clear`). No amount of policy tuning can fix a structural cancel in the execution layer.

### Evidence from logs (4 rounds)

| Round | placed | replace | cancel | initial | policy | replace_ratio | cancel→reinitial cycles |
|-------|--------|---------|--------|---------|--------|---------------|------------------------|
| 1     | 9      | 2       | 7      | 7       | 2      | 0.22          | ~5 cycles               |
| 2     | 6      | 4       | 2      | 2       | 4      | 0.67          | 0 (all endgame cancel)  |
| 3     | 5      | 2       | 3      | 3       | 2      | 0.40          | ~1 cycle                |
| 4     | (partial) | 1    | 1      | 3       | 1      | ~0.25         | ~1 cycle                |

Round 1 has 7 Reprice cancels — 5 are structural cancel→reinitial cycles that inflate `initial` count and deflate `replace_ratio`. The TRUE churn (counting cancel→reinitial as effective replace) is 7/9 = 78%, masked to 22%.

## Plan

### Step 1: Fix the dwell-bypass bug in `should_defer_absent_reprice_clear`

**File:** `src/polymarket/coordinator_execution.rs:547-631`

Swap the distance check and the warmup dwell check so that the **dwell always applies first**, regardless of distance:

```
Current order (BROKEN):
  1. Check distance → if too far, return false (instant eviction, dwell bypassed!)
  2. Check warmup dwell → if within dwell, defer

Fixed order:
  1. Check warmup dwell → if within dwell, ALWAYS defer (minimum grace period)
  2. Check distance → if too far AND dwell elapsed, return false
```

This ensures every absent-intent event gets at least `warmup_dwell` seconds (1.2-2.6s) of grace before any distance-based eviction, giving brief regime spikes time to resolve.

### Step 2: Widen hard_stale thresholds to match force_realign safety levels

**File:** `src/polymarket/coordinator_execution.rs:586-591`

Current thresholds tighten with regime volatility (backwards — more volatile = tighter = more evictions):
```
Aligned: 12, Tracking: 10, Guarded: 8, Blocked: 0
```

Change to use a **uniform generous threshold** that matches the force_realign trusted thresholds (coordinator_order_io.rs:1770):
```
Aligned: 16, Tracking: 14, Guarded: 14, Blocked: 0
```

Rationale: An order placed during Aligned (where 14-tick force_realign is the safety boundary) should survive a brief Guarded transition without eviction. The force_realign thresholds represent the actual safety boundary — absent-intent defer should be at least as permissive.

Also widen the warmup_dwell for Guarded from 2.6s to 4.0s to cover typical regime spike durations observed in the log (~11 seconds at the extreme, but most regime bounces are 3-5 seconds).

### Step 3: Preserve partial policy state across slot clears

**File:** `src/polymarket/coordinator_pricing.rs:207-223`

Currently `reset_slot_publish_state` wipes everything. When a slot IS cleared for legitimate reasons (Safety, StaleData), preserve the policy candidate tracking so re-entry doesn't pay the full commit_dwell + policy_dwell penalty:

- Keep `slot_policy_candidates` and `slot_policy_candidate_since` (candidate tracking)
- Reset `slot_policy_states` and `slot_policy_since` (committed state — must re-commit)
- Keep `slot_last_regime_seen` and `slot_regime_changed_at` (regime continuity)

This cuts re-entry time from ~10s (commit_dwell + policy_dwell) to ~6.6s (policy_dwell only, since candidate is already tracking).

### Step 4: Update tests

**File:** `src/polymarket/coordinator_tests.rs`

- Update existing `should_defer_absent_reprice_clear` tests to verify dwell-first behavior
- Add test: order survives regime transition from Aligned→Guarded→Tracking
- Add test: order at 14 ticks from trusted_mid is NOT evicted during Guarded
- Add test: order at 20+ ticks IS evicted after warmup_dwell elapses

## Files to modify

1. `src/polymarket/coordinator_execution.rs` — Steps 1 & 2 (should_defer_absent_reprice_clear)
2. `src/polymarket/coordinator_pricing.rs` — Step 3 (reset_slot_publish_state)
3. `src/polymarket/coordinator_tests.rs` — Step 4 (tests)

## Verification

1. `cargo test` — all existing tests pass + new tests pass
2. `cargo build` — compiles cleanly
3. Re-run dry-run with same market config, check:
   - `cancel_reprice` count drops significantly (target: <3 per round vs current 5-7)
   - `replace_ratio` improves (target: <0.40 across rounds)
   - No new `WARN` or `ERROR` log entries
   - Time-on-book increases (fewer/shorter gaps between orders)
