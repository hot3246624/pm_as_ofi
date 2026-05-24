# ce25 三天 pair-cost 验证合同

时间：2026-05-24

## 结论

已把 ce25 的新画像固化成 72h 验证合同：

```text
KEEP_CE25_THREE_DAY_PAIR_COST_VALIDATION_SPEC_READY_REAL_INPUT_UNKNOWN
```

这一步不启动 no-order、不启动 shadow、不读取 raw/replay，也不把单日结果当成策略证据。当前真实状态是：合同 ready，但 repo 内还没有安全的 3 天非 fixture 输入 manifest。

## 单日观察

来自已提交的 ce25 projected guard spec artifact：

| 桶 | markets | PnL | wins/losses | avg pair cost | residual |
|---|---:|---:|---:|---:|---:|
| starter: ETH/SOL 5m+15m, pair_cost<0.90, residual<15% | 61 | +1752.797631 | 61/0 | 0.796786 | 7.0275% |
| core: BTC/ETH/SOL 5m+15m, pair_cost<0.95, residual<10% | 119 | +5078.222860 | 118/1 | 0.861041 | 4.6593% |
| hard-loss: BTC/ETH/SOL 5m+15m, pair_cost>=1.00 | 163 | -5589.455319 | 18/145 | 1.089349 | 13.5101% |

这说明规则值得验证，但仍只是单日 public proxy。

## 三天合同

合同名：

```text
ce25_three_day_pair_cost_validation_v1
```

每日必须同时存在三类 cohort：

- starter：ETH/SOL，5m/15m，pair cost `<0.90`，residual `<15%`，每日 markets `>=20`，每日 PnL 必须为正。
- core：BTC/ETH/SOL，5m/15m，排除 XRP，pair cost `<0.95`，residual `<10%`，每日 markets `>=40`，每日 PnL 必须为正。
- hard-loss：BTC/ETH/SOL，5m/15m，pair cost `>=1.00`，每日 PnL 必须为负，用来证明 stop condition 不是偶然。

每个 daily cohort 必须包含：

- `day_id`
- `account`
- `cohort`
- `markets`
- `buy_actual`
- `cohort_pnl_ex_rebate`
- `cohort_pnl_including_rebate`
- `pair_pnl`
- `residual_pnl_est`
- `wins`
- `losses`
- `avg_pair_cost`
- `residual_rate`
- `old_redeem_contamination`
- `post_window_same_condition_buy`
- `rebate`

可选但重要：

- takerOnly sample count/share
- maker/taker role source
- per-trade liquidity role truth coverage

## Fail-Closed 条件

以下任一成立即 UNKNOWN：

- 少于 3 个 distinct `day_id`
- 任一天缺 starter/core/hard-loss 任一 cohort
- starter/core 任一天 PnL 非正
- starter/core 任一天 pair cost 或 residual 超阈值
- hard-loss 任一天不是负 PnL
- ce25 出现 old redeem contamination 或 post-window same-condition buy contamination
- fixture 被当成真实策略证据
- 没有非 fixture liquidity role 与 fair-source probability join 却宣称可部署

## Smoke

Artifact：

```text
xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_spec_smoke_20260524T122500Z/manifest.json
```

Smoke 覆盖：

- 默认无 3 天输入时：spec KEEP，但 real input UNKNOWN
- 3 天完整 fixture：candidate READY，但不算 strategy evidence
- 单日 fixture：UNKNOWN，阻塞 `fewer_than_3_days`
- hard-loss 不亏 fixture：UNKNOWN，阻塞 `hard_loss_pnl_not_negative`

## 下一步

准备安全 offline 72h ce25 public-export manifest，匹配 `ce25_three_day_pair_cost_validation_v1`。在它存在之前，不启动 no-order diagnostic；即使 72h public proxy 通过，也仍需要 liquidity role、fair-source probability 和 no-order pullback 才能继续。
