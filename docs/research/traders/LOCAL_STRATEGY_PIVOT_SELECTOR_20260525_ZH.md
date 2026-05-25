# Local Strategy Pivot Selector 2026-05-25

## 结论

`local_strategy_pivot_selector_v1` 的结论是：

```text
KEEP_LOCAL_STRATEGY_PIVOT_OBSERVABLE_PRE_ACTION_RULE_MINER_SELECTED
```

这不是新策略已经可用。含义是：已有自动推进线里，public-profile 模仿、symmetric activation、closed-cycle、complete-set sizer、micro-deficit/source-opportunity marker、residual-tail exemplar 和 dynamic/ledger 旧族都不能继续自动开 no-order；下一步只能转向一个新的 local-only 规则发现 spec。

被提名的新方向是：

```text
observable_pre_action_rule_miner_spec_v1
```

## 为什么要丢弃旧线

- public-profile / ce25-b55：当前 worktree 没有 safe 72h manifest；单日公开画像只能作为 feature prior。
- symmetric activation edge0.075/window20：最新 projected-tail no-order 失败，`net_pair_cost_p90=1.11`、`residual_qty_share=31.36%`、`residual_cost_share=25.97%`、`pair_tail_loss_share=27.74%`，且最大 residual tail 来自 projected low-residual bucket。
- closed-cycle marker：denominator 有，但样本不足且 residual/risk-adjusted proxy 失败。
- complete-set dust-opposite sizer：精确 sizer 不能有效降 residual，继续加 buffer 会退化成 forbidden qty cap。
- old separator / micro-deficit / source-opportunity marker：离线 train/holdout 稳定，但 no-order marker denominator 不复现。
- residual-tail exemplar：字段完整，但 tail 不集中，不能安全提取 sample-preserving local mechanism。
- dynamic/ledger/fill-to-balance 旧族：已冻结或 mixed，不作为自动候选。

## 新方向合同

`observable_pre_action_rule_miner_spec_v1` 只允许 local-only spec/scorer：

- 输入只能来自当前 worktree 的安全 materialized exports 和 summary manifests。
- 训练阶段可以用 post-action labels 做离线筛选，但冻结后的候选只能包含 pre-action observable fields。
- 任何候选必须先证明 same-window no-order denominator reproduction，再谈经济诊断。
- 不允许把 realized pair_cost、future residual/pair labels、public-profile single-day buckets 当作 live rule。
- 不允许静态 side/offset/timeframe 删除、public price cap、D+ 失败族复活。
- 不允许 SSH、shadow、canary、live、shared WS/local agg/service、raw/replay/full-store scan。

## 下一步

实现 `observable_pre_action_rule_miner_spec_v1`：

1. 定义安全输入 manifest。
2. 定义 train/holdout/no-order denominator 三层 gate。
3. 写 fixture scorer，验证完整输入 pass、缺 source/denominator fail closed。
4. 继续保持 `promotion_gate.passed=false`、`private_truth_ready=false`、`deployable=false`。
