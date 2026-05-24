# Public Profile 下一研究对象选择门 - 2026-05-24

## 结论

本轮 local-only gate 采用同事的 b55/ce25 deep-compare 导出，并接入上一轮
`b55_ce25_non_fixture_input_source_review_v1` 的阻塞状态。结论是：

- `b55` 仍是强度/规模/配对引擎第一。
- `ce25` 是下一步优先模仿和工程化拆解的对象。
- `b55` 应保留为强度控制组，不作为第一条复制线。
- 不允许启动 b55/ce25 no-order diagnostic、shadow、canary 或 live。真实逐笔
  `liquidity_role` 与非 fixture fair-probability join 仍缺失。

artifact:
`xuan_research_artifacts/xuan_public_profile_next_lead_gate_20260524T091916Z/manifest.json`

decision:
`KEEP_PUBLIC_PROFILE_NEXT_LEAD_GATE_CE25_MIMICABILITY_SELECTED_B55_CONTROL`

## 为什么不是继续先攻 b55

按强度口径，b55 更强：

| 指标 | b55 | ce25 |
|---|---:|---:|
| buy actual | 289123.992505 | 144400.833610 |
| actual pair cost | 0.959218 | 0.974217 |
| paired actual profit | 11159.822900 | 3605.251327 |
| conservative cohort PnL ex rebate | 2346.354635 | 644.529813 |
| conservative cohort PnL incl rebate | 3008.413735 | 952.877413 |

但 b55 的复制难度更高：

- old redeem contamination = 5818.087877。
- post-window same-condition buy = 4664.419153。
- residual rate = 14.9474%，高于第一复制目标 10%。
- timeframe 混合 `15m/5m/1h/4h/other`，并且 b55 的 BTC 5m、BTC 15m 是拖累项。

所以 b55 适合作为“强度控制组”和后续增强模板，不适合作为第一条窄策略线。

## 为什么 ce25 先拆

ce25 的规模较小、pair edge 不如 b55，但它更干净：

- old redeem contamination = 0。
- post-window same-condition buy = 0。
- residual rate = 8.7091%，低于 10% 第一复制目标。
- actual pair cost = 0.974217，低于 0.98。
- conservative cohort PnL ex rebate = +644.529813。
- 主要只有 `15m/5m` 两类周期，工程拆解面更窄。

因此下一步应从 `ce25_late_window_pair_arb_proxy_gate_v1` 开始。

## 下一条本地 proxy 线

优先测试：

- ce25 BTC/ETH/SOL 15m late-window cohort。
- b55 作为 strength control。
- BTC 5m 作为负控制，不先复制。

当前 ce25 子组 public-export proxy：

| 子组 | markets | buy+post-buy | cohort PnL ex rebate | pair PnL base | residual PnL est |
|---|---:|---:|---:|---:|---:|
| BTC/ETH/SOL 15m | 229 | 86726.035130 | +813.385850 | +2487.942550 | -2112.977989 |
| all 15m | 305 | 91591.014601 | +861.539779 | +2496.614069 | -2129.583297 |
| all 5m control | 335 | 52809.819009 | -217.009966 | +1108.637258 | -1325.647224 |
| BTC 5m negative control | 105 | 39082.568927 | -341.020183 | +340.843284 | -681.863467 |

这个 proxy 只能说明 ce25 15m 是更干净的研究对象，不能证明策略可部署。核心风险仍是 residual drag 和 pair-cost tail。

## 禁止误用

该 KEEP 是 lead-selection readiness，不是 strategy profitability truth。

禁止：

- 用 public price band 替代 fair probability。
- 用 maker rebate、fee magnitude、account-level taker share 推断逐笔 liquidity role。
- 重启 D+ 失败的 micro-deficit/ledger/tiny-deficit/closed-cycle/static/cooldown/fill-to-balance 分支。
- 启动 SSH、shadow、canary、local agg/shared WS/service 或 live。
- 扫 raw/replay/collector/full completion store。
- 声称 private_truth_ready、deployable 或 promotion_gate_pass。

下一步 local-only：

实现 `ce25_late_window_pair_arb_proxy_gate_v1`，只用现有 deep-compare/public export rows，固化：

- ce25 15m BTC/ETH/SOL 为第一复制候选。
- BTC 5m 为负控制。
- b55 为强度控制。
- 缺逐笔 liquidity role 或非 fixture fair-probability join 时 fail closed。
