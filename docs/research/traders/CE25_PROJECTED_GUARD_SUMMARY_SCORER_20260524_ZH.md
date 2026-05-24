# CE25 projected guard summary scorer

时间：2026-05-24

## 结论

`ce25_projected_guard_summary_scorer_v1` 已完成，决策为：

```text
KEEP_CE25_PROJECTED_GUARD_SUMMARY_SCORER_READY
```

它只读取已提交的 runner smoke summary/aggregate artifacts，不读取 events JSONL，不启动 shadow/canary/local agg/shared WS，也不改变任何 trading behavior。

## 验证内容

输入：

- `xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_20260524T113800Z/manifest.json`
- `xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_20260524T113800Z/default_off/aggregate_report.json`
- `xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_20260524T113800Z/enabled/aggregate_report.json`
- `xuan_research_artifacts/xuan_ce25_projected_guard_runner_instrumentation_smoke_20260524T113800Z/enabled/*.summary.json`

通过项：

- default-off aggregate 不含 `ce25_projected_guard_summary`
- enabled aggregate 含 schema `ce25_projected_guard_summary_v1`
- field contract 保持 `trading_behavior_changed=false`
- field contract 保持 `private_truth_ready=false`、`deployable=false`、`promotion_gate_passed=false`
- starter/core/hard-kill denominators 非零
- projected pair-cost buckets 非零
- projected residual buckets 非零
- 从 3 个 enabled summary 重建后的计数与 aggregate 完全一致

核心计数：

| 指标 | 数值 |
|---|---:|
| enabled summary files | 3 |
| starter allow count | 1 |
| core allow count | 1 |
| hard-kill pair-cost count | 1 |

## Fail-Closed Smoke

Smoke artifact：

```text
xuan_research_artifacts/xuan_ce25_projected_guard_summary_scorer_smoke_20260524T115000Z/manifest.json
```

覆盖：

- good path 返回 `KEEP_CE25_PROJECTED_GUARD_SUMMARY_SCORER_READY`
- bad path 将 aggregate field contract 改成 `trading_behavior_changed=true`，scorer 必须返回 UNKNOWN
- missing path 缺 enabled summary dir，scorer 必须返回 UNKNOWN

## 研究边界

该 scorer 证明的是 runner 诊断字段可观测、默认关闭、可聚合、可 fail-closed。

它不证明：

- ce25 策略可盈利
- no-order diagnostic 可以启动
- private truth ready
- deployable
- promotion/canary ready

下一步只有两类安全方向：

- 等未来 allowlisted no-order pullback 中自然包含 `ce25_projected_guard_summary_v1` 后，做真实 pullback scorer adapter。
- 在本地继续准备 non-fixture input，例如逐笔 liquidity role 和 fair-source probability join。
