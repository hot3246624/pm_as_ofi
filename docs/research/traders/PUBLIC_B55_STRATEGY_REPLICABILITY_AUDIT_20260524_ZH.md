# Public B55 Strategy Replicability Audit - 2026-05-24

## Decision

`KEEP_B55_TEMPLATE_LEAD_NOT_DEPLOYABLE_FAIR_PRICE_AND_RESIDUAL_GATE_REQUIRED`.

b55 仍然值得猛攻，但定位要修正：它不是已经可复制的低风险套利策略，而是一个低费率、低平均 pair cost、到期前 fair-price 判断、允许少量方向残仓的公开模板。公开 profile 只能作为研究输入，不是 private truth、deployable、canary 或 promotion 证据。

Source export:

`/Users/hot/web3Scientist/poly_trans_research/data/exports/leaderboard_b55_recent24h_20260523_103000_to_20260524_103000_bjt/summary.json`

Artifact:

`xuan_research_artifacts/xuan_public_b55_strategy_replicability_audit_20260524T050730Z/manifest.json`

## Key Read

| metric | value |
| --- | ---: |
| trades | 22,749 |
| buy actual cost | $289,123.99 |
| inferred fee | $3,346.47 |
| fee / gross | 1.171% |
| cash PnL | +$3,870.06 |
| current value | $5,460.78 |
| cash + current value | +$9,330.84 |
| actual pair cost | 0.959218 |
| paired actual profit | +$11,159.82 |
| lifetime residual rate | 14.9474% |
| current residual rate | 18.3211% |

The correction matters: part of the positive realized cash came from positions opened before the 24h window and redeemed inside it. On the same-window-new-position lens, realized cash is negative, while cash plus current open value stays positive. That makes b55 a strong template, not a proof that this exact 24h new-entry strategy was locked-in profitable.

## What To Copy

Do not start from 5m high-frequency chasing. The first replicable hypothesis should be:

- Asset/timeframe: BTC and ETH, first `1h_or_named`, then `15m`; keep 5m as later stress case. In this export, BTC 1h and ETH 1h are the safer first pass because both cash PnL and residual PnL estimates are positive. BTC/ETH 15m have strong pair edge but negative residual estimates.
- Entry timing: end-minus 15 minutes to 1 minute; last 60 seconds is auxiliary.
- Price band: 35c to 90c core, especially 50c to 80c.
- Pair cost: average actual pair cost must stay <= 0.97; market-level pair cost > 1.00 is not an arbitrage leg.
- Residual: first xuan replication target must be <= 10%, stricter than b55's 14.95%.
- Fee: if realized fee/gross is near xuan's old 2.5%-3% range, discard this family.
- Fair price: require independent BTC/ETH fair-probability from multi-exchange pricing before any shadow.

## Why This Is Not D+

This does not reuse the failed D+ micro-deficit, ledger-after/open<=1, ledger-before/delta tiny-deficit, closed-cycle marker, static deletion, cooldown/admission cap, or fill-to-balance families. The objective is different: determine whether Polymarket's 35c-90c quote is below true win probability after fees, then control pair cost and residual.

## Next Action

Implement local-only `b55_fair_price_entry_window_spec_v1`: define the exact data contract and pass/fail gates for BTC/ETH 15m/1h, entry -15m..-1m, price 35c..90c, fee/gross <=1.2%, average pair cost <=0.97, residual <=10%, and fair-price source availability. No shadow until the fair-price source and fee/residual gates are explicit.

Hard blockers before any shadow/deployable claim:

- Observed residual rate is `14.9474%`, above the first replication target `10%`.
- Per-market actual pair-cost p90 is `1.164791`, so the average pair edge hides a bad tail.
- Same-window-new cash estimate is negative after correcting for old-position redeem benefit.
