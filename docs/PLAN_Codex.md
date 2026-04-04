# GLFT 重心切换计划：从“程序收敛”转向“策略 Edge 验证”

## Summary
当前仓库已经基本证明了两件事：

- `glft_mm` 的执行链路、参考链路、发布链路已经能跑，最近 dry-run 主要暴露的是“发布制度质量”，不是系统失控。
- 但现有证据仍然几乎全部是 **dry-run / 无 fill** 证据，不能回答最关键的问题：`glft_mm` 在真实成交后到底有没有正期望。

因此下一阶段的唯一主目标改为：

- **`GLFT-only + 小额实盘` 做真实 edge 验证**
- 暂停把 `replace/placed` 当作头号目标；只保留 correctness 级 bug 修复
- 不再继续大规模优化执行层，除非真实成交样本表明它直接损害 edge

默认结论：
- `BTC 5m` 这类活跃市场下，`replace/placed < 0.35` 不是合理生产门槛；当前更合理的执行门槛应按 “不触发 API/执行风险 + 不出现结构性 churn” 来看
- 当前真正未被证明的是 **策略 edge**，不是系统是否还能继续写得更“平滑”

## Key Changes
### 1. 冻结当前程序主线，只允许两类改动
- 允许：
  - 真实成交后暴露出的 correctness bug
  - 为 edge 验证补充观测/归因数据
- 暂停：
  - 单纯为了压 `replace_ratio` 的制度微调
  - 新策略分叉
  - 大型架构重写

这一步的目标是防止“边验证边改模型”，导致样本失真、结论不可归因。

### 2. 把验证单位从“tick/round 日志”改成“回合级交易归因”
基于现有 fill、inventory、metrics 链路，新增一个**回合级验证视图**。每个 market round 至少要输出一份可机读 summary，字段固定为：

- 基础信息：
  - `market_slug`
  - `round_start_ts`
  - `round_end_ts`
  - `strategy=glft_mm`
- 成交信息：
  - `buy_fill_count`
  - `sell_fill_count`
  - `yes_bought_qty / yes_sold_qty`
  - `no_bought_qty / no_sold_qty`
  - `avg_yes_buy / avg_yes_sell`
  - `avg_no_buy / avg_no_sell`
- 风险与库存：
  - `max_abs_net_diff`
  - `time_weighted_abs_net_diff`
  - `max_inventory_value`
  - `time_in_guarded_ms`
  - `time_in_blocked_ms`
- 策略质量：
  - `paired_locked_pnl_end`
  - `worst_case_outcome_pnl_end`
  - `realized_cash_pnl`
  - `mark_to_mid_pnl_end`
  - `fees_paid_est`
  - `maker_rebate_est`（如果当前市场可得）
- 微观质量：
  - `replace_events`
  - `cancel_events`
  - `publish_events`
  - `fill_to_adverse_move_3s`
  - `fill_to_adverse_move_10s`
  - `mean_entry_edge_vs_trusted_mid`
  - `mean_fill_slippage_vs_posted`

核心要求：
- 评价对象从“程序每轮挂了多少次单”改成“每轮真实成交后赚了什么/亏了什么，为什么”
- 同时保留 `trusted_mid` 作为内部参考，但盈利结论不能只基于内部模型，要基于真实成交与市场后续路径

### 3. 建立固定验证协议，不再凭单轮印象决策
固定验证协议如下：

1. 验证范围：
- 只跑 `GLFT-only`
- 只跑 `BTC 5m`
- 单市场串行，不并行多市场

2. 风险预算：
- 使用当前最小可执行风险档：
  - `PM_BID_SIZE=5`
  - `PM_MAX_NET_DIFF=5`
- 不在验证期内调整 `gamma/xi/ofi/publish` 参数

3. 样本要求：
- 第一阶段至少 `20` 个完整实盘 round
- 第二阶段扩展到 `50` 个完整实盘 round
- 中途网络断连 round 单独标记，不直接纳入主统计

4. 决策门槛：
- `20 round` 后，如果以下任一成立，则暂停 `glft_mm` 主线并复盘：
  - `realized_cash_pnl + mark_to_mid_pnl_end` 显著为负
  - `fill_to_adverse_move_3s` 明显为负，说明持续逆选
  - 盈亏主要由单边库存伤害导致，而不是偶发执行错误
- `50 round` 后，如果：
  - 回合级中位数 edge 仍不显著为正
  - 或正收益完全依赖极少数 round
  则结论应是“当前 GLFT 主线 edge 未被证明”，而不是继续磨程序

### 4. 用“归因树”判断该优化哪里
每个亏损 round 必须进入统一归因树，不再凭感觉判断：

- A 类：模型方向错
  - `trusted_mid` 持续偏离真实后续价格
  - fill 后短时间 adverse move 明显不利
- B 类：报价过激
  - fill 价格太靠近不利一侧，entry edge 太薄
- C 类：库存治理错
  - 单边库存积累后无法靠自然对冲消化
- D 类：执行/时序错
  - cross reject、position lag、source block、reconcile 错误导致本可避免损失
- E 类：市场本身不适合
  - 该市场时段长期 trend/流动性结构不支持这类 maker edge

只有当亏损主要集中在 `D 类` 时，才继续优先改程序。
如果主要是 `A/B/C/E`，就应改策略或停掉该策略，不再继续优化执行层。

### 5. 文档与上线标准同步改写
更新正式文档时，结论必须改成：

- `glft_mm` 现在是“可运行并可验证”的主线，不是“已被证明盈利”的主线
- `GO_LIVE` 标准分成两层：
  - **系统可运行**：源健康、发布稳定、无结构性 churn
  - **策略可上线**：有足够真实 fill 样本证明 edge 为正
- 把旧的“已具备实盘条件”改写成：
  - “已具备进入小额验证实盘条件”
  - 不是“大额生产上线条件”

## Public Interfaces / Types
最小新增接口即可，不改策略哲学：

- 新增一个回合级验证 summary 结构，例如：
  - `RoundValidationSummary`
- 新增一个持久化输出：
  - JSONL 或 CSV，按 round 追加
- 现有 `InventoryState`、`GlftSignalSnapshot`、`StrategyMetrics` 不需要改语义，只作为输入源
- 不新增新的交易参数面
- 不调整默认策略选择；只是调整“上线结论”的定义

## Test Plan
1. 数据正确性
- 真实 fill 数量与 round summary 聚合值一致
- `realized_cash_pnl / mark_to_mid_pnl_end / worst_case_outcome_pnl_end` 计算可回放验证
- 网络断连/半轮数据要能被正确标记为 `partial_round`

2. 归因正确性
- 能从单个亏损 round 追溯到：
  - fill 时的 `trusted_mid`
  - fill 后 3s/10s 的价格方向
  - 当时 `quote_regime`
  - 当时 `net_diff`
- 不允许再出现“亏了，但不知道是模型错、库存错还是执行错”

3. 验证协议
- 固定参数跑满 `20` 个完整小额实盘 round
- 输出一份聚合报告，至少包含：
  - 总体 PnL
  - 中位数 round PnL
  - fill 后 adverse selection 指标
  - 库存暴露分布
  - 亏损 round 归因分布

## Assumptions
- 主线继续是 `glft_mm`
- 下一阶段不做 `GLFT vs pair_arb` 并行对比；先把 `glft_mm` 的真实 edge 问题答清楚
- 当前市场以 `BTC 5m` 为验证对象
- 允许继续小修 correctness bug，但不再把“继续压 replace ratio”作为主目标
- 评价标准默认改为：
  - 系统门槛：当前基本已过
  - 策略门槛：尚未被证明，需要真实 fill 样本
