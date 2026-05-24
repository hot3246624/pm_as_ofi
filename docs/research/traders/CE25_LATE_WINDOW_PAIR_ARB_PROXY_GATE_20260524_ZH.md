# ce25 Late-Window Pair-Arb Proxy Gate - 2026-05-24

## 结论

本轮只使用已存在的 b55/ce25 deep-compare public export 和已提交的 lead-selection artifact。

decision:
`KEEP_CE25_LATE_WINDOW_PAIR_ARB_PROXY_GATE_READY_TAIL_RESIDUAL_BLOCKED`

artifact:
`xuan_research_artifacts/xuan_ce25_late_window_pair_arb_proxy_gate_20260524T094516Z/manifest.json`

含义：

- ce25 BTC/ETH/SOL 15m late-window 子组值得继续本地拆解。
- BTC 5m 是负控制，不应先复制。
- b55 继续作为强度/规模/pair-engine control。
- 不允许 no-order diagnostic、shadow、canary、live 或 deploy。

## ce25 15m 主子组

| 指标 | 数值 |
|---|---:|
| markets | 229 |
| buy + post-buy | 86726.035130 |
| conservative cohort PnL ex rebate | +813.385850 |
| ROI ex rebate | 0.9379% |
| pair PnL base | +2487.942550 |
| residual PnL est | -2112.977989 |
| residual drag / pair PnL | 84.9287% |
| market actual pair cost p50 | 0.954724 |
| market actual pair cost p75 | 1.049775 |
| market actual pair cost p90 | 1.113350 |
| pair-cost > 1.0 market share | 39.2857% |

分资产：

| asset | markets | cohort PnL | pair PnL | residual PnL est |
|---|---:|---:|---:|---:|
| BTC 15m | 77 | +367.821063 | +1620.973492 | -1455.883016 |
| SOL 15m | 76 | +226.193089 | +266.651911 | -88.759524 |
| ETH 15m | 76 | +219.371698 | +600.317147 | -568.335449 |

这个子组符合 public-proxy 继续拆解的条件：保守 cohort PnL 为正、pair PnL 为正、账户层 old redeem 和 post-window same-condition buy 都为 0，账户残仓率低于 10%。

但它不是策略通过：market-level pair-cost tail 明显，residual drag 仍很重。

## 负控制

BTC 5m 明确作为负控制：

| 指标 | 数值 |
|---|---:|
| markets | 105 |
| buy + post-buy | 39082.568927 |
| cohort PnL ex rebate | -341.020183 |
| pair PnL base | +340.843284 |
| residual PnL est | -681.863467 |
| residual drag / pair PnL | 200.0519% |
| market actual pair cost p90 | 1.085093 |

这说明 ce25 的 edge 不是“任意 late-window + 任意 5m/15m 都可复制”。5m，尤其 BTC 5m，不能作为第一复制目标。

## 阻塞项

promotion gate 仍失败：

- primary 15m market actual_pair_cost_p90 = 1.113350 > 1.0。
- primary 15m residual_pnl_est_base = -2112.977989。
- 真实逐笔 liquidity_role 缺失。
- 非 fixture fair_probability / uncertainty / edge join 缺失。
- public proxy 不能证明 private truth、deployable、canary 或 promotion。

## 下一步

实现 `ce25_15m_market_tail_gap_audit_v1`，仍然 local-only，只用现有 `market_cash_pnl` 和 public rows。

目标：

- 判断 15m p90 pair-cost tail 是否集中在少数可观察 market contexts。
- 判断 residual drag 是否集中在少数可观察 contexts。
- 不使用纯 price-band、静态 asset/timeframe 删除、fixture probability 或 D+ 失败族。
- 如果只能靠这些 forbidden families 才能解释 tail，则直接 DISCARD ce25 复制线。
