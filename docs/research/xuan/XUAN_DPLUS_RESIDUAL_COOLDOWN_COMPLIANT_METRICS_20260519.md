# Xuan D+ Residual Cooldown Probe And Compliant Metrics Runner

This note makes the residual cooldown probe reproducible and separates the old
completion-only result from the compliant metrics path.

## Source Files

- Residual cooldown runner: `scripts/xuan_b27_dplus_residual_cooldown_compliant_metrics_runner.py`
- Smoke: `scripts/xuan_b27_dplus_residual_cooldown_compliant_metrics_runner_smoke.sh`
- Broad compliant metrics runner: `scripts/xuan_b27_dplus_compliant_metrics_runner.py`
- Core profile config: `Profile` and `build_profiles()`
- Admission rule: `maybe_seed()`
- Pairing rule: `pair_inventory()` and `choose_pair_indices()`
- Residual cooldown rule: `maybe_seed()` block using `residual_cooldown_age_s` and `residual_cooldown_cost_cap`
- Residual settlement metrics: `settle()` and `finish()`

## Profile: dpass_all_e055_t5_px050_900_imb125_rc30_050

This is the profile requested by the residual cooldown probe.

- Edge: `0.055`
- Target qty: `5`
- Price band: `0.05 <= public_trade_price <= 0.90`
- Alignment: `all`
- Fill haircut: `0.25`
- Max seed qty: `60`
- Max open cost: `250`
- Seed offset window: `0s <= offset_s < 120s`
- Seed L1 pair cap: `strict_l1_immediate_pair <= 1.02`
- Per-market seed cooldown: `5000ms`
- Imbalance qty cap: `1.25`
- Imbalance cost cap: effectively unbounded
- Residual cooldown age: `30s`
- Residual cooldown cost cap: `0.50`
- Pair select: `fifo`
- Salvage/repair: disabled in this profile

## Entry Rule

The compliant runner uses strict/cache rows as the admission universe. A seed is
eligible when the strict candidate has a YES/NO first side, the configured side
alignment matches, the offset and price band match, and the strict L1 immediate
pair is no wider than the profile cap.

The seed price is:

```text
seed_px = max(0.01, public_trade_price - edge)
```

The seed quantity is:

```text
min(
  max_seed_qty,
  public_trade_size * fill_haircut,
  target_qty - same_side_qty,
  (max_open_cost - open_cost) / seed_px,
  imbalance_qty_cap - max(0, same_side_qty - opposite_side_qty)
)
```

## Pairing Rule

YES and NO lots are paired internally. The default `fifo` mode consumes the
oldest YES lot and oldest NO lot. Pair PnL is:

```text
paired_qty * (1 - yes_px - no_px)
```

## Residual Cooldown Rule

For each candidate seed, the runner computes aged residual cost across both
sides:

```text
aged_cost = sum(qty * px for lots with ts age >= residual_cooldown_age_s)
```

It blocks same-side seed admission when:

```text
aged_cost > residual_cooldown_cost_cap
and same_side_qty + dust_qty >= opposite_side_qty
```

This is intentionally a seed admission brake. It is not a repair or unwind
mechanism.

## Residual Settlement Rule

At the end of each market, remaining lots are settled with `winner_side`.
The runner reports actual settlement PnL and also conservative residual stress:

- `worst_residual_net_pnl = pair_pnl - residual_cost`
- `stress100_worst_pnl = worst_residual_net_pnl - 0.01 * (2 * pair_qty + residual_qty)`

## Compliant Data Path

The residual cooldown runner is not the old completion-only probe. It reads:

- strict/cache: `$POLY_BT_ROOT/backtest_cache/taker_buy_signal_core_v2_strict_l1`
- completion store: `$POLY_BT_ROOT/verification_store/completion_unwind_event_store_v2`
- public audit: `$POLY_BT_ROOT/verification_store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb`

It excludes `20260514`, `20260515`, and not-ready `20260518`. It does not scan
raw replay paths, does not start auth/network services, and does not place,
cancel, redeem, or control services.

Residual cooldown smoke run:

```bash
scripts/xuan_b27_dplus_residual_cooldown_compliant_metrics_runner_smoke.sh
```

Residual cooldown full local run:

```bash
python3 scripts/xuan_b27_dplus_residual_cooldown_compliant_metrics_runner.py
```

## Evidence Boundary

The earlier residual cooldown result remains useful as a falsification hint:
the completion-only model showed no metric delta even when the strictest
cooldown cap added residual-cooldown blocks. It should not be promoted as
source-of-truth or canary evidence.

The compliant runner upgrades the evidence chain to strict/cache admission plus
completion-store metrics plus public-audit coverage segmentation. It still does
not claim private queue truth or source-of-truth replay validation.
