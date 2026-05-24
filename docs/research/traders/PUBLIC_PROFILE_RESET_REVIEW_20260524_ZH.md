# Public Profile Reset Review - 2026-05-24

## Decision

`KEEP_PUBLIC_PROFILE_RESET_RESEARCH_READY`.

D+ 当前可部署路径直接冻结/丢弃：micro-deficit、ledger-after/open<=1、ledger-before/delta tiny-deficit、target/cooldown/static deletion 都不再作为可部署或 shadow 推进对象。失败的核心不是写代码慢，而是研究 funnel 把 gross pair PnL 误当成了策略强度，尾部 pair cost 和 residual inventory risk 没有被提前作为硬目标函数。

本轮从四个公开 Polymarket profile 重新开始，只使用公开 profile 页面、Gamma public-search 和 data-api activity/positions。公开 profile 只用于 proxy inspiration；不等于 private truth、deployable、canary 或 promotion 证据。

## Sources

- https://polymarket.com/zh/@0x04b6d7e930cf9e493c5e6ef24b496294f95594c8-1774448369789
- https://polymarket.com/zh/@0x9f5ffe76a818dce37c70f947998b52b70671a008-1772728605528
- https://polymarket.com/zh/@0x8dxd
- https://polymarket.com/zh/@gabagool22
- Public API pulls: `https://data-api.polymarket.com/activity`, `https://data-api.polymarket.com/positions`
- Handle resolution: `https://gamma-api.polymarket.com/public-search`

Artifact: `xuan_research_artifacts/xuan_public_profile_reset_review_20260524T022800Z/manifest.json`.

## Profile Read

| profile | wallet | decision | recent public trade rows | motif | key metric |
| --- | --- | --- | ---: | --- | --- |
| `0x04b6...` | `0x04b6d7e930cf9e493c5e6ef24b496294f95594c8` | KEEP | 3500 | short-horizon complete-set pairing | 75.0% BTC 5m, 20.9% BTC 15m, pair cost 0.9932, residual share 15.9% |
| `0x9f5...` | `0x9f5ffe76a818dce37c70f947998b52b70671a008` | KEEP as profit/settlement lead, not robust-pairing lead | 3426 | high turnover, strong directional/settlement exposure | sampled pair cost 1.0405, residual share 40.0%, current paired position proxy -86.25 |
| `0x8dxd` | `0x63ce342161250d705dc0b16df89036c8e5f9ba9a` | KEEP as lead/control | 3500 | strong entry edge but residual-heavy | pair cost 0.9278, weighted edge +7.22%, residual share 83.9%, current positions 0 |
| `gabagool22` | `0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d` | UNKNOWN / mixed | 3426 | closed-cycle low residual, timeframe mixed | pair cost 0.9957, residual share 4.46%, 50% of rows are 10-share clips |

Important caveat: the public activity endpoint returned HTTP 400 after offset 3500 for these profiles, so this is a capped recent-window review, not a full 72h or lifetime reconstruction. It is enough to classify sampled motifs, but not enough to rank 72h fee-after account PnL. In particular, `0x9f5...` should not be read as a weak account: the sampled pair-cost lens says its paired subset is expensive/residual-heavy, while a 72h realized/settlement PnL lens can still rank it as the strongest earner.

## Methodology Correction

There are two different questions:

1. Strongest money machine over a realized public window.
2. Most robust and copyable infrastructure pattern.

Under the first question, `0x9f5...` can legitimately rank first if 72h fee-after PnL is +$43k and all days are positive. Its edge appears to come from direction/settlement selection rather than low-risk complete-set pairing. That is valuable and should be studied, but it is not the same object as a low-residual pairing engine.

Under the second question, `0x04b6...` remains the priority because it has lower residual, more concentrated BTC short-horizon activity, low takerOnly sample, positive paired-cost structure, and material maker/rebate support. It is not necessarily the highest PnL account, but it is the cleaner strategy-structure template.

Therefore the updated ranking is:

- Profitability lead: `0x9f5...`.
- Robust/copyable infrastructure lead: `0x04b6...`.
- Directional/lifecycle lead or control: `0x8dxd` and any equivalent high-residual zero-position account.
- Mixed closed-cycle clip lead: `gabagool22`.

## What This Changes

The next research object should not be another D+ marker sweep. The profiles point to a different objective:

`closed_cycle_pairing_objective_audit_v1`

Required target function:

- Optimize risk-adjusted closed-cycle PnL after fees and residual stress, not gross pair PnL.
- Separate average pair edge from tail pair-cost and residual inventory exposure.
- Treat zero-current-position accounts (`0x8dxd`, `gabagool22`) as evidence that lifecycle closure matters more than a single admitted trade marker.
- Treat `0x9f5...` as the warning case: many trades and visible PnL can still have negative pair-cost structure and large residuals.
- Require train/holdout stability and a same-window no-order marker before any shadow.

## Next Action

Implement local-only `closed_cycle_pairing_objective_audit_v1` against allowed candidate/state-machine exports. It must explicitly reject candidates that show positive gross pair PnL while residual-stress-adjusted PnL is negative. Keep `private_truth_ready=false`, `deployable=false`, and `promotion_gate.passed=false`.

## First Local Objective Audit

Artifact: `xuan_research_artifacts/xuan_closed_cycle_pairing_objective_audit_20260524T030800Z/manifest.json`.

Decision: `KEEP_CLOSED_CYCLE_PAIRING_OBJECTIVE_AUDIT_READY_NO_ORDER_MARKER_BLOCKED`.

The audit found that the objective is expressible in the historical separator export, but not yet safe to shadow. Selected local historical family:

`balance=deficit_0_25_1_25&opp_qty=opp_le_5`

Train:

- rows=11008, seed_qty=13701.4125, pair_qty=13670.0225
- weighted_pair_cost=0.88736382
- closed_cycle_pnl_proxy=1320.43955482
- stress100_closed_cycle_pnl_proxy=1046.72520482
- residual_qty_share_of_pair_plus_residual=0.2291%

Holdout:

- rows=9596, seed_qty=11940.58, pair_qty=11910.3175
- weighted_pair_cost=0.88715578
- closed_cycle_pnl_proxy=1152.73574123
- stress100_closed_cycle_pnl_proxy=914.22676623
- residual_qty_share_of_pair_plus_residual=0.2534%

This is useful because gross pair PnL is now explicitly subordinated to residual-stress-adjusted closed-cycle PnL. It is not deployable because there is no same-window no-order closed-cycle marker yet, and the historical export itself is too optimistic versus fresh no-order evidence.

Next executable action: implement a default-off closed-cycle no-order marker summary/scorer only if the runner can expose pre-action cycle denominators without changing behavior. Do not start a shadow from this local audit.
