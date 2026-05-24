# Public Leaderboard Corrected New MTM Audit - 2026-05-24

## Decision

`KEEP_CORRECTED_PUBLIC_LEADERBOARD_RANKING_READY_B55_MAIN_CE25_CLEAN_CONTROL`.

这轮把 leaderboard/cash PnL 口径降级为候选池，不再当结论。核心修正是：窗口内 `REDEEM` 可能来自窗口前建仓，因此必须把旧仓 redeem、maker rebate、新开仓 cash、当前仓位价值分开看。

Artifact:

`xuan_research_artifacts/xuan_public_leaderboard_corrected_new_mtm_audit_20260524T054249Z/manifest.json`

## Corrected Ranking

| rank | account | corrected read |
| --- | --- | --- |
| 1 | `b55` | 主线继续保留：pair edge 真强，但 24h 新策略收益要从 naive `+$9.33k` 下修到新仓 MTM 约 `+$2.85k`，含 rebate 约 `+$3.51k`。 |
| 2 | `ce25` | 升级为干净低残仓 pair-arb 对照组：无旧仓 redeem 污染，残仓低，但规模和利润薄。 |
| 3 | `b27bc` | 历史 maker benchmark，当前窗口无活动，只能当历史参考。 |
| 4 | `ohanism` | 降级为方向/返点/库存暴露样本：赚钱能力可能强，但不是稳健 pair-arb 模板。 |
| 5 | `xuan` | 继续排除：fee 和 pair cost 都过高，修正后新仓仍亏。 |
| stale | `04b6` | 历史强，但当前 24h 几乎停跑，不能当现役可复制样本。 |

## Key Metrics

| account | old redeem | rebate | new cash | current value | corrected new MTM | pair cost | residual |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `b55` | +$5,818 | +$662 | -$2,610 | +$5,461 | +$2,851 / incl rebate +$3,513 | 0.959218 | 14.9474% |
| `ce25` | $0 | +$308 | +$150 | $0 | +$150 / incl rebate +$458 | 0.974217 | 8.7091% |
| `ohanism` | +$997 | +$1,020 | +$100 | +$650 | +$751 / incl rebate +$1,771 | 1.036747 | 76.9792% |
| `xuan` | +$108 | $0 | -$394 | $0 | -$394 | 1.006930 | 0.4266% |
| `04b6` | $0 | $0 | +$13 | $0 | +$13 | 1.011359 | 13.9233% |
| `b27bc` | no activity | no activity | no activity | no activity | not evaluable | - | - |

Reported `SPLIT` count is `0` for this sample, so this pass does not need split-then-sell adjustment.

## Interpretation

b55 仍然是主攻对象，但它不是已经可部署的低风险套利。它的优势是 `actual_pair_cost=0.959218`，这是可学习的真 edge；它的风险是新仓 realized cash 为负、残仓 `14.95%` 高于第一版复制目标 `10%`、pair-cost p90 `1.164791` 说明坏尾部仍重。

ce25 被低估了。它不如 b55 有爆发力，但窗口更干净：旧仓 redeem 为 0，残仓率 `8.71%`，pair cost 低于 1。下一步应该把 ce25 作为 b55 fair-price 线的低残仓对照组，而不是只盯 b55。

ohanism 不适合作为稳健 pair-arb 模板。它的 pair cost `1.036747` 且残仓接近 `77%`，更像方向判断、库存暴露和 rebate 叠加。

## Blockers

本轮仍不能启动 shadow/canary 或声称 deployable。硬 blocker 是：

- b55 新仓 cash 在旧仓 redeem 修正后为负。
- b55 残仓高于第一复制目标。
- b55 和 ce25 的 per-market pair-cost tail 都高于 `1.00`，说明平均 edge 下面仍有坏市场。
- fair-price source 还没有接入。
- public profile 只有 proxy evidence，不是 private truth。

## Next Action

下一步不再按榜单收益追人，而是做 `b55 + ce25` 双样本 fair-price source manifest/reviewer：BTC/ETH `15m` 和 `1h`，入场 `-15m..-1m`，多交易所 fair probability，per-trade fee/liquidity role，pair-cost tail，残仓 gate。只有 b55 主线和 ce25 对照组都能解释，才继续推进到下一层 no-order diagnostic。
