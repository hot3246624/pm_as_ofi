# Strategy Reset Next Lane Selector

时间：2026-05-25 BJT

结论：`KEEP_SYMMETRIC_ACTIVATION_NEXT_STRATEGY_SELECTED_RESEARCH_ONLY`

## 为什么不继续 public-profile

用户要求“原来的策略不行就直接放弃，开始研究新的策略”。因此本轮把 ce25/b55/04b6/9f5/ohanism/gabagool 这条 public-profile mimicability 线从“等待输入”改成冻结/丢弃：

- 当前 worktree 没有安全 72h public export manifest。
- ce25/b55/04b6/9f5/ohanism/gabagool 现有样本都只有 1 个 BJT day。
- 唯一浅层多日样本 `0x8dxd` 的 validation 失败。
- 继续挖当前 public-profile artifacts 只能把单日 bucket 或 fixture 误当策略证据。

因此 public-profile mimicability 线的状态是：

`DISCARD_PUBLIC_PROFILE_MIMICABILITY_INPUT_BLOCKED`

这不是说 b55/ce25 公开画像一定亏，而是当前 worktree 证据不足，不能继续自动推进，更不能启动 no-order diagnostic。

## 新选择的策略线

新线：`symmetric_activation_residual_stress`

依据：当前 worktree 中已有多日 covered+holdout strict-cache artifact：

- `xuan_research_artifacts/xuan_symmetric_activation_residual_stress_strict_count_boundary_20260519T1431Z/manifest.json`
- `xuan_research_artifacts/xuan_symmetric_activation_residual_stress_20260519T1231Z/manifest.json`

该线不是 public-profile 模仿，也不是 D+ 失败的 micro-deficit/ledger/tiny-deficit/closed-cycle 族。它的机制是：

> 只有最近 activation window 内出现反向 strict flow，才允许当前方向激活；用 residual/leak stress 检验 unmatched first-leg 暴露。

## 选中的起点

从 strict-count-boundary residual-stress manifest 中选择的当前最佳 research-only 起点：

| 字段 | 数值 |
|---|---:|
| edge | 0.07 |
| leak_rate | 0.02 |
| activation_window_s | 7.5 |
| min_opp_count | 1 |
| covered_pair_actions | 3079 |
| covered_residual_qty_rate | 0.025476 |
| covered_worst_day | 20.588189 |
| covered_worst_net_fee_after | 778.801385 |
| holdout_pair_actions | 594 |
| holdout_residual_qty_rate | 0.027760 |
| holdout_worst_day | 17.860534 |
| holdout_worst_net_fee_after | 151.002955 |

这个选择只说明“下一条研究线值得本地合同化”，不说明可部署。

## 边界

本轮没有：

- fetch 新数据
- 读取外部 worktree
- SSH
- shadow/canary/live
- observer dry-run
- local agg/service/shared WS
- raw/replay/full-store scan
- shared-ingress/broker/env/live 修改
- order/cancel/redeem

`private_truth_ready=false`，`deployable=false`，`promotion_gate.passed=false`，`no_order_diagnostic_allowed=false`。

## 下一步

执行 `symmetric_activation_contract_v1`，仅本地：

- 定义 default-off pre-action fields。
- 字段包括 opposite-side strict-flow activation count、activation window、projected pair cost、residual/leak stress。
- 证明缺字段 fail closed。
- 继续保持 research_ranking 和 promotion_gate 分离。
- 不启动 no-order diagnostic，不碰 live/canary/order。
