**核心判断**
这两份更新很有价值，但它们现在不是同一层级的文档。

[STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:1) 是“风险补丁 / 假设裁决层”：它把 V1 里最危险的隐含假设拆出来，要求先用数据、双口径、资本模型验证。

[PLAN_Codex.md](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:1) 是“工程主线 / 实施收敛层”：它把新策略压成一个可实现版本，明确 `maker-first`、`single-leg open`、`CompletionOnly`、旧 `pair_arb` 保留 baseline。

我建议：**PLAN_Codex 继续做实现主计划，V1.1 作为 PLAN_Codex 的 gate/风险附录吸收进去**。不要让 V1.1 的所有 override 直接改变 V1 默认行为，否则实现面会从“可落地”一下跳到“研究系统 + 双路径策略 + 资本模型 + 时间衰减”四件事同时开干，复杂度会炸。

**V1.1 强项**
V1.1 最强的贡献是把三个隐藏假设显式化：

- `A1`：xuan 约 `4000 trades / 33h / 367 markets` 是否能代表常态。
- `A2`：我方是否能像 xuan 一样 maker 到 first leg。
- `A3`：我方 7-market 环境下是否能累积足够 surplus bank。

这三个都是真问题。尤其是 [V1.1 §2.1](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:32) 把 H-0 从单窗口聚合升级为 `>=20000 trades`、`>=5` 周、market 分桶、4h 分桶，这是研究纪律上的明显进步。否则我们可能拿一个顺风 session 的 `clean_closed=95%` 去指导实盘，然后在低流动/高波动时段被打穿。

[V1.1 §2.2](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:54) 的 `pair_cost_tranche` 与 `pair_cost_fifo` 并行也非常关键。FIFO 是研究近似，行为层真正消费的是 tranche-explicit 账本。如果二者 `delta_p50 > 0.05`，说明我们的 surplus/repair 预算可能是错的。这个应该马上吸收到 PLAN_Codex。

[V1.1 §2.4](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:117) 的 Capital Pressure Gate 方向也对。xuan 的 edge 依赖 rolling merge 和资金周转；我方只有有限 market/资金时，不建模 `locked_capital_ratio`，会出现“策略逻辑允许开仓，但资金实际被 covered/unmerged 库存锁死”的假阳性。

**主要冲突**
第一个冲突是 H-0 门槛。V1.1 说任一 H-0 升级条件未达标，V1 不允许进入 shadow；PLAN_Codex 则把 `H-0` 通过条件固定回 `clean_closed >= 90%`、`same_side_add <= 10%`、`p90 <= 60s`，并把实现列为“立即做”。见 [V1.1](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:39) 和 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:36)。

我的建议是拆成两层：**工程 skeleton 可以立即做，enforce 不允许；shadow 也可以跑我方，但研究结论不得宣称通过 H-0 升级门槛**。也就是说，V1.1 的 H-0 应阻塞 enforce，不应阻塞基础账本/回放/指标建设。

第二个冲突是 maker/taker。V1.1 要 maker/taker 双路径并行 shadow，见 [V1.1 §2.6](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:221)。PLAN_Codex 则明确 `maker-first` 是默认执行边界，`bounded taker completion` 只预留接口，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:8) 和 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:46)。

我站 PLAN_Codex 这一边：**先 maker-only 实现，taker 路径做 replay/simulation shadow，不要一开始做 live taker runner**。原因很朴素：taker first leg 的成交率当然高，但 adverse selection、滑点和资金锁定完全不同；它不是 xuan 的等价替代路径。双路径应该用于裁决，不应该同时进入行为执行。

第三个冲突是 same-side run。V1.1 把默认从 `MAX=1` 改成条件化 `MAX=2`，见 [V1.1](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:84)。PLAN_Codex 仍固定 `max_same_side_run=1`，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:59)。

这里我建议保守：**实现上保留 `MAX=1`，但指标层加 `conditional_second_same_side_would_allow` shadow 计数**。等我们看到第二笔同侧是否真的提高 open opportunity 且不污染 repair budget，再改默认。否则一开口子，最容易从“pair-gated”滑回“越不平越加仓”。

第四个冲突是 completion ceiling。V1.1 要时间衰减公式，见 [V1.1 §2.5](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:166)。PLAN_Codex 则固定 `urgency_budget_per_share=0`，只在后续 bounded taker shadow 启用，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:26)。

V1.1 的方向对，但公式有一个硬问题：当前初始交易域是 `BTC 5m`，而公式里有 `t > 600s`、`60s < t <= 600s`。5 分钟 round 的 `T=300s`，所以 `t > 600s` 永远不会发生，`60s < t <= 600s` 从开盘就成立。这会让 urgency ramp 在全 round 生效，和“close 前 10min 才启用”的解释不一致。这个公式要按 5m 重写，不能原样进实现。

第五个冲突是 CapitalModel。V1.1 引入 `locked_capital_ratio` 和 `harvest_min_dynamic`，见 [V1.1](D:/web3work/pm_as_ofi/docs/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md:124)。PLAN_Codex 默认仍是 `harvest_min_full_set=10`，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:59)。

这个我建议立即吸收 telemetry，但暂不改变行为。还有一个小逻辑需要澄清：`locked_capital_ratio > 0.50` 已经会 `trigger_merge=true`，同时 `harvest_min_dynamic = max(10, round(ratio * 50))` 在高压力时变大。如果这个值是“触发阈值”，变大反而可能降低 merge 频率；如果它是“目标 batch size”，那文档应改名为 `harvest_batch_dynamic`。这块需要改清楚，否则实现者会误读。

**PLAN_Codex 评价**
PLAN_Codex 是目前最适合作为工程入口的文档。它做了几个很好的收敛：

- 新增 `StrategyKind::PairGatedTrancheArb`，不污染旧 `pair_arb`，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:13)。
- `InventoryManager` 成为账本真相源，coordinator 只消费快照，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:14)。
- 明确 `CompletionOnly` 后同侧风险全部抑制，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:25)。
- 明确 `PairCovered != Merged`，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:30)。
- 回测器独立为 `backtest_pair_gated_tranche.py`，不塞进旧 pair arb 回测，见 [PLAN_Codex](D:/web3work/pm_as_ofi/docs/PLAN_Codex.md:41)。

我比较担心的是 PLAN_Codex 现在太“决策完成”了，但没有吸收 V1.1 的关键风险门。它适合开工，但在开工前应补三行硬规则：

- `pair_cost` 字段必须拆成 `pair_cost_tranche` 与 `pair_cost_fifo`，enforce 使用 tranche-explicit。
- `H-0 upgraded sample gate` 阻塞 enforce，不阻塞 skeleton/shadow 工具建设。
- `CapitalState` 先作为 telemetry 加入 ledger snapshot，行为 gate 先 shadow-only。

**实施建议**
我建议把两份文档关系定成这样：

`PLAN_Codex` 是主计划，描述“我们现在怎么实现”。

`STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md` 是风险门，描述“哪些实现结果不能直接上线”。

下一步最稳的路线：

1. 更新 `PLAN_Codex.md`，吸收 V1.1 的非争议项：tranche/FIFO 双 pair cost、CapitalState telemetry、H-0 upgraded gate 作为 enforce 前置。
2. 保持 PLAN_Codex 的 maker-only V1 默认，不采纳 V1.1 的 live maker/taker 双 runner，只做 replay/simulation 双口径。
3. 保持 `max_same_side_run=1` 行为默认，把 V1.1 的 `MAX=2` 条件化放行降级为 shadow counter。
4. 修正 V1.1 的 5m 时间衰减公式，把 `600s` 改成相对 `T` 的比例或 5m 专用窗口。
5. 澄清 `harvest_min_dynamic` 是触发阈值还是 batch size；我倾向改为 `harvest_batch_dynamic`。

**底线判断**
V1.1 是必要的“刹车系统”，PLAN_Codex 是必要的“发动机图纸”。现在最好的形态不是二选一，而是：**PLAN_Codex 负责推进，V1.1 负责防止我们把未裁决假设误上线**。

我建议下一步直接做一次文档对齐 patch：更新 `PLAN_Codex.md` 为 V1.1-aware 主计划，同时在 `STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md` 修正 5m 时间衰减和 harvest 动态命名。