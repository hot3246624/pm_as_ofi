# ce25 15m Market Tail Gap Audit - 2026-05-24

## 结论

本轮只使用现有 ce25 `market_cash_pnl`、`post_window_by_condition`、public activity rows 和已提交 proxy gate artifact。

decision:
`KEEP_CE25_15M_MARKET_TAIL_GAP_AUDIT_FINAL_TOUCH_TARGET_PAIR_TAIL_BROAD`

artifact:
`xuan_research_artifacts/xuan_ce25_15m_market_tail_gap_audit_20260524T101516Z/manifest.json`

含义：

- 找到一个非价格带、非静态 asset/timeframe 的本地优化目标：`last_touch_to_expiry_bucket=last_touch_30_60s`。
- 该上下文解释了最大 cash/residual drag，可继续做 final-touch coverage 诊断。
- 但 pair-cost tail 仍是 broad blocker；剔除该上下文后 p90 仍高于 1.0。
- 不允许 no-order diagnostic、shadow、canary、live 或 deploy。

## 基准组

ce25 BTC/ETH/SOL 15m：

| 指标 | 数值 |
|---|---:|
| markets | 229 |
| buy_actual | 86726.035130 |
| cash PnL | +374.964561 |
| pair PnL | +2487.942550 |
| residual PnL est | -2112.977989 |
| residual drag / pair PnL | 84.9287% |
| actual_pair_cost p50 | 0.954724 |
| actual_pair_cost p75 | 1.049775 |
| actual_pair_cost p90 | 1.113350 |
| pair-cost > 1.0 market share | 39.2857% |

## 最强合法上下文

`last_touch_30_60s`：ce25 在这些市场的最后一次公开触达发生在到期前 30-60 秒。

| 指标 | last_touch_30_60s |
|---|---:|
| markets | 69 |
| buy_actual | 34540.288419 |
| cash PnL | -1177.577256 |
| pair PnL | -87.372650 |
| residual PnL est | -1090.204606 |
| actual_pair_cost p90 | 1.112906 |
| pair-cost > 1.0 share | 39.7059% |

剔除该上下文后的剩余组：

| 指标 | kept |
|---|---:|
| markets | 160 |
| cash PnL | +1552.541817 |
| pair PnL | +2575.315200 |
| residual PnL est | -1022.773383 |
| residual drag / pair PnL | 39.7145% |
| actual_pair_cost p90 | 1.112572 |

解释：

- final-touch gap 对 cash/residual drag 有明显解释力。
- 但它不能解决 pair-cost tail；pair-cost p90 基本没有下降。
- 因此下一步只能做 final-touch diagnostics/spec，不能直接转策略。

## Public Rows 覆盖

现有 public activity rows 是 capped export：

- raw rows: 3497。
- primary condition coverage: 43 / 229。
- selected-context condition coverage: 13 / 69。
- selected-context trade rows: 269。

这足够验证上下文存在，但不足以证明完整策略 truth。

缺失仍包括：

- `liquidity_role`
- `fair_probability`
- `fair_probability_uncertainty`
- `edge_after_fee_and_uncertainty`
- `model_name`
- `model_version`

## Promotion Gate

promotion 继续失败：

- selected context 不解决 pair-cost p90。
- 剔除 selected context 后 residual PnL 仍为负。
- 真实逐笔 liquidity role 缺失。
- 非 fixture fair-probability/uncertainty/edge 缺失。
- public final-touch context 只是 proxy evidence。

## 下一步

实现 `ce25_15m_final_touch_context_spec_v1`，local-only：

- 定义 default-off final-touch coverage diagnostics。
- 报告 market-level pair-cost/residual accounting。
- 继续禁止纯 price band、静态 asset/timeframe 删除、fixture probability、D+ 失败族。
- 若 final-touch diagnostics 不能给出非价格、非 fixture 的 tail 改善机制，则 DISCARD ce25 复制线。
