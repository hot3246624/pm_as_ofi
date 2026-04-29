# `XUANXUAN008_STRATEGY_V2_ZH.md` 重大更新评估

生成时间：`2026-04-28`

---

## 1. 摘要

`docs/research/xuan/XUANXUAN008_STRATEGY_V2_ZH.md` 当前最大的变化，不是 A2 cash-flow ledger 草稿本身，而是 `§0.5 修正 5c` 已经把研究主叙事从：

- `loser-bias / 无方向 alpha / 中性 pair-merger`

切换成：

- `MERGE 前存在显著 winner-bias 的 winner-overweight maker`

这条更新足以改变后续研究优先级，也足以推翻沿旧 `5b` 路线直接推进 `PGT` 复刻的合理性。

但当前 V2 还不能直接作为实现依据，原因不是 `5c` 太弱，而是 **V2 的“观察事实 / 因果解释 / 实现含义”三层仍然混在一起**：

1. `5c` 已经推翻 `5b`，但正文后续多个章节仍沿用 `5b`。
2. `winner-bias` 的“存在”已很强，但“来源”尚未裁决。
3. `realizedPnl=-$23k` 的口径冲突仍未解开，不能拿来支持或否定 `5c`。

当前最稳的研究立场是：

- **把 `5c` 视为当前 canonical working thesis**
- **把 `winner-bias 来源` 视为最高优先级未解问题**
- **在 A2 会计口径收敛前，不把 `winner-bias` 直接写进策略实现**

---

## 2. 当前已经成立的结论

### 2.1 `5c` 已经实质性推翻 `5b`

当前本地结果 `data/xuan/pre_merge_bias_full.json` 显示：

- `MERGE events = 497`
- `winner mapping coverage = 497/497 = 100%`
- `winner-heavier = 373`
- `loser-heavier = 124`
- `winner_qty - loser_qty median = +11.27 shares`
- `delta_norm median = +1.51%`

并且在 `278s` 主峰桶内：

- `W:L = 223:54`

这与 `5b` 的 `44/497` 子样本：

- `W:L = 2:42`
- `median delta = -8.4`

形成方向相反的结果。`5b` 只能再被视为 **selection-biased 历史误判**，不能再参与当前主叙事。

### 2.2 `5c` 的强度足以重写 `§2.2`

`538 loser / 0 winner residual` 现在更合理的解释是：

- `MERGE` 前已经存在 winner-heavier 的瞬时库存
- `MERGE` 后 pairable 部分被等额扣减
- `settlement + REDEEM` 把 winner residual 带走
- `snapshot` 中只剩 loser residual

因此：

- `loser-only snapshot` 是 **结算后稳态**
- 不是 `MERGE 前无方向性 alpha` 的直接证据

### 2.3 `278s MERGE pulse` 的含义被强化

在 `5c` 框架下，`278s` 的主峰不再只是“收水 timer”，而更像：

- `capital recycling`
- `winner-side inventory flattening`

的联合动作。

`278s` 这一点在研究上的含义已经比旧版更强：它不是简单的“经验节律”，而是 **xuan 如何把 winner-overweight 风险压平并现金化** 的关键节点。

### 2.4 `5c` 不是“纯解释层故事”，而是强观察事实

`5c` 目前最大的优点是：它不是只来自 `positions snapshot` 的静态推断，而是来自：

- `trades_long.json`
- `activity_long.json`
- `slug_winner_map_full.json`

三者联动下的时序 replay。

就当前可见证据而言，`winner-bias observed before MERGE` 已经是 **强观察事实**，不再只是合理假设。

---

## 3. `5c` 的边界：为什么它可以 canonical，但还不能直接变成实现规则

### 3.1 `100% 全样本` 的表述必须收紧

`5c` 当前的 `100%`，更准确地说是：

- `100% of 497 captured MERGE events in the current long-window dataset`

而不是：

- `100% of xuan lifetime MERGE history`

这不是吹毛求疵，而是研究边界问题。当前窗口仍然是：

- `trades_long window ≈ 37.05h`
- `activity_long window ≈ 29.6h`

所以 `5c` 目前是：

- **窗口内全覆盖**
- 不是 **生涯全覆盖**

### 3.2 但 `5c` 的窗口内稳健性是足够强的

当前我专门核对了一个最容易出错的地方：是否存在大量 `MERGE size > 窗口内累计 pairable inventory` 的事件。

结果：

- `497` 个 MERGE 中，只有 `1` 个明显 undercovered
- 去掉这 `1` 个事件后：
  - `winner-heavier` 仍是 `373`
  - `loser-heavier` 变成 `123`
  - `median delta` 保持 `+11.27`

也就是说，`5c` 的方向结论不是由窗口尾部一个坏样本撑起来的。

### 3.3 `5c` 证明了“存在 winner-bias”，没有证明“winner-bias 来源”

当前仍不能从 `5c` 直接推出：

- xuan 有明确 directional alpha
- xuan 的 first leg 有可交易的预测信号
- xuan 的 winner-overweight 一定来自主动择边

`winner-bias` 仍可能来自多种机制：

1. `first-leg side selection`
2. `被动 maker fill geometry`
3. `MERGE timing / 只在某些状态触发`
4. 上述因素叠加

所以当前最准确的话术不是“xuan 已被证明有方向性 alpha”，而是：

- **xuan 在 MERGE 前表现出稳定 winner-overweight**
- **但其来源尚未裁决**

---

## 4. 当前 V2 的内部冲突

`§0.5 修正 5c` 已经发生，但正文没有同步完成。当前主要冲突至少有 5 处。

### 4.1 `§2.2` 仍停留在旧世界观

当前 `§2.2` 仍写有：

- `没有方向性 alpha`
- `MERGE 前 95.5% loser-heavier`
- `A级（升回，§0.5 修正 5b）`

这与 `5c` 已经正面冲突。

### 4.2 `§2.4` 仍把 PnL 口径冲突当成开放 TODO，但没有吸收 `5c` 的新解释

`§2.4` 现在还只是列出：

- `realizedPnl=-$23k`
- FIFO `+$5k`
- “可能是 redeem / lifetime / 口径差”

但还没有把 `winner residual after settlement gets redeemed away` 这条新叙事纳入。

### 4.3 `§0.6 修正 8` 仍按 `5b` 解释 xuan

`修正 8` 当前把 xuan 写成：

- `pair-MERGE surplus + barely cover residual cost`
- `loser-heavy residual = ... maker geometry 自然累积`

这已经落后于 `5c`。在 `5c` 框架下，xuan 更像：

- `pair surplus + winner residual redeem / settlement cashout`

至少不能再写成“只是 barely cover loser cost”。

### 4.4 `§10` 证据等级表没有完成叙事切换

`§10` 目前对 `Winner residual 倾向` 的描述仍停留在：

- `B（降级）`
- `未证 MERGE 前瞬时无 winner-bias`

但 `5c` 实际上已经把这条从“待裁决”推进到“已观察到 winner-bias”。当前更需要区分的是：

- `winner-bias existence`
- `winner-bias source`

而不是继续把二者混成一个等级。

### 4.5 `§11` 路线图仍把 `5b` 当成已完成主线

`§11` 当前仍写：

- `第 5 条已完成：95% loser-heavier`
- `第 7 条：继续补 full sample 看是否仍 loser-bias`

但从当前状态看：

- `第 5 条` 应降级为历史误判记录
- `第 7 条` 已完成，且结果是反向推翻
- 下一步第一优先级应切到 `winner-bias 来源裁决`

---

## 5. A2 / PnL 部分的研究评估

### 5.1 A2 的问题意识是对的

A2 真正要解决的，不是“做个 ledger 就完”，而是解释：

- 为什么 `data-api realizedPnl ≈ -$23.4k`
- 与旧 FIFO `+$5.4k`
- 以及新 `winner-bias` 叙事

看上去互相矛盾。

这个问题必须研究，因为它决定我们到底在复刻：

- `pair surplus model`
- `winner redeem model`
- 还是一个被字段定义误导的伪利润故事

### 5.2 但当前 A2 草稿还不能裁决任何大结论

当前数据域明显分裂：

- `trade slugs = 425`
- `activity slugs = 359`
- `positions snapshot slugs = 540`

其中：

- `positions only vs trades = 498 rows`
- 这批 alone 贡献：
  - `realizedPnl ≈ -20.9k`
  - `totalBought ≈ 233.6k`

而 `positions in trade window` 只有：

- `43 rows`
- `realizedPnl ≈ -2.5k`
- `totalBought ≈ 28.8k`
- `currentValue ≈ 579.6`

这说明当前 `realizedPnl` 的主体，来自 **37h 交易窗口之外的 lifetime 历史**。因此在 cohort 没拆开之前：

- 不能用 A2 去否定 `5c`
- 也不能用 A2 去证明 xuan 的 lifetime alpha

### 5.3 当前最稳的 A2 研究定位

A2 目前应该只回答两类问题：

1. `37h` 窗口内的 cash-flow 能否相对闭合
2. `realizedPnl` 里有多少其实来自窗口外历史

在这之前，不应把任何 ledger 数字直接上升为：

- `xuan lifetime PnL truth`

---

## 6. 现在最重要的未解问题

### 6.1 `winner-bias` 的来源到底是什么

这是当前第一优先级问题。

它直接决定：

- 我们要不要把 xuan 当成“弱方向性 / winner-overweight maker”
- `PGT` 是否需要 winner-side overweight 监控
- `open gate` 是看价差，还是也要看择边条件

### 6.2 `winner-bias` 与 `30s completion` 是否是同一条 edge

这也是关键分叉。

两种完全不同的可能：

1. xuan 只是因为更会选 first-leg，所以更容易进到 winner side，同时也更容易在 30s 内配对
2. xuan 的 `winner-bias` 和 `30s completion` 是两件独立事情：
   - 前者来自 fill geometry
   - 后者来自 open gate / sizing / cooldown

这个问题不澄清，后续实现很容易把“能配对”和“能选中 winner”混成一个信号。

### 6.3 loser-only snapshot 还有没有研究价值

有，但它的角色必须降级。

它现在更适合做：

- `settlement-state residual accounting`
- `redeem/merge 后稳态解释`

而不再适合做：

- `pre-merge directional neutrality proof`

### 6.4 `realizedPnl` 字段到底记录了什么

当前最需要澄清的不是“xuan 到底赚没赚”，而是：

- `realizedPnl` 是否把 winner-redeem 算进去了
- `cashPnl / realizedPnl / totalBought / currentValue` 的边界到底是什么

在这个字段没澄清前，不宜用 data-api 的单一字段去判断策略经济性。

---

## 7. 对实现工作的约束

在当前研究状态下，以下事情还不能做：

### 7.1 不能再把 xuan 当成“中性 pair-merger”

这条已经过时。

### 7.2 不能把 `5c` 直接写成 live 策略规则

因为当前还不知道：

- `winner-bias` 是主动择边
- 还是被动成交几何
- 还是 MERGE 时点筛选结果

### 7.3 不能继续用 loser-only snapshot 证明“无方向 alpha”

这条已经被推翻。

### 7.4 可以做的只有研究型约束

当前实现层最多只该吸收两类东西作为研究 guardrail：

- `MERGE trigger` 仍然是 P0
- `winner-side overweight monitoring` 可以进入 shadow / 研究报表

但它们都不该在现阶段被升为 live 行为逻辑。

---

## 8. 当前研究裁决

### 8.1 工作性主结论

当前最稳的主结论应当是：

> xuan 在当前长窗口中，并不是“MERGE 前 loser-bias / 无方向 alpha”的中性 maker；相反，它在 MERGE 前表现出稳定的 winner-overweight。`538 loser / 0 winner` 只说明 settlement + redeem 后的稳态，不再说明 pre-MERGE 的库存结构。

### 8.2 证据等级建议

把 `5c` 拆开看更合适：

- `MERGE 前 observed winner-bias exists`
  - **A（窗口内强观察事实）**
- `loser-only snapshot is settlement-state consequence`
  - **A/B**
- `winner-bias 来源`
  - **B/C**
- `xuan 存在可交易 directional alpha`
  - **C**
- `lifetime PnL truth`
  - **C**

### 8.3 下一步研究优先级

当前最优顺序应当是：

1. **统一 V2 叙事口径**
2. **A2 会计口径拆 cohort**
3. **winner-bias 来源分解**
4. **winner-bias 与 30s completion 的关系**
5. **再讨论 PGT 是否吸收 winner-side 逻辑**

---

## 9. 最终结论

`V2` 当前不是“没研究透”，而是已经到了一个非常关键的新阶段：

- 旧版 `loser-bias / neutral maker` 理解已经不能继续支撑实现
- `5c` 已经足够把研究主轴改写成 `winner-overweight maker`
- 但这还只是 **观察层胜利**，不是 **因果层闭环**

所以现在最正确的动作不是继续改策略代码，而是：

- **先把文档主叙事统一**
- **再把 winner-bias 的来源研究透**
- **最后才决定 `PGT` 该不该带上 winner-side 监控甚至择边逻辑**

在这之前，任何“已经可以复刻 xuan”的说法都还过早。

---

## 10. 证据需求矩阵

| 问题 | 当前最优答案 | 还缺什么证据 | 为什么阻塞实现 |
|------|-------------|-------------|---------------|
| `winner-bias` 是否真实存在 | **是**。当前窗口 `497/497` MERGE replay 已观察到 `373:124` winner-heavier | 更长窗口复核，确认不是短窗口特异性 | 决定我们是否还把 xuan 当成“中性 pair-merger” |
| `winner-bias` 来自哪里 | 未解。可能是 `first-leg selection`、`maker geometry`、`MERGE timing` 的组合 | 更长窗口的 first-leg side / L1 / fillability 联动分析 | 不澄清来源，就不能决定是否把 winner-side 信息变成 live 规则 |
| `winner-bias` 与 `30s completion` 是否同源 | 未解 | `open gate`、`first opposite delay`、`winner-side overweight` 的联合 episode 研究 | 决定我们优化的是择边、配对，还是两者同时 |
| `realizedPnl=-$23k` 为什么与 FIFO/ledger 冲突 | 目前只能确认大部分负值来自窗口外历史，而非 37h 交易窗口本身 | cohort 拆分后的 A2 ledger、字段定义核对 | 不澄清口径，就无法判断 xuan 的真实经济来源 |
| `538 loser / 0 winner` 的研究角色是什么 | 仅能证明 settlement 后稳态 | 无需更多数据；需要文档口径统一 | 若继续误用，会把旧 `5b` 世界观带回实现层 |
