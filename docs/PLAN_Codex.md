# Pair_Arb 升级计划 v3.5（以 `v3.3` 为基线，吸收 `v3.4` 的有效增强）

## 摘要
`v3.5` 的定位是：**保留 `v3.3` 的研究纪律与可证伪框架，吸收 `v3.4` 的操作纪律增强，明确拆分“研究北极星”和“工程上线目标”。**

固定原则：
- **研究北极星**：尽最大可能还原 `xuanxuan008` 的 edge 结构与策略机制。
- **工程上线目标**：量化我方差距，优先上线已被裁决、且对我方有显著 gap 的部分。
- **研究与会计层并行推进**，不串行。
- **任何未裁决机制只能 shadow，不能 enforce。**

## 关键改动
### 1. 保留 `v3.3`，不接受 `v3.4` 的回退
- 保留 `v3.3` 的目标主轴：
  - 会计层立即开干
  - 研究层用 `L1/L2` 分层裁决
  - 行为层按裁决推进
  - 安全层不删代码
- 保留 `v3.3` 的四个硬阈值，不再改成“稳定足够高”这类定性表述：
  - `H-1`：末段折扣贡献 `> 30%` 的 round 占比 `>= 50%`
  - `H-2`：首两笔时间差 `<= 1s` 的 round 占比 `>= 70%`
  - `H-3`：`clip_size ~ best_depth` 回归 `R² >= 0.4`
  - `H-4`：`worst MTM <= -$50` 且终局 `PnL > 0` 的 round 占比 `>= 60%`
- 保留 `v3.3` 的 `PnL` 归因 bar：
  - 至少 `70%` 的盈利 round，能把 `TOP-2` 利润来源归到：
    - `pair_discount`
    - `merge_turnover`
    - `final_clip_discount`
    - `residual_payout`
- 保留 `v3.3` 的 fallback 矩阵与时间盒：
  - `S0=1w`
  - `S1=2w`
  - 每个 `H-x <= 1w`
  - `A0/B0/D0 = 2w` 并行推进

### 2. 吸收 `v3.4` 的四条操作纪律增强
- **我方 baseline 报告前置**
  - `C1-single tranche_arb` 进入 enforce 前，必须已有：
    - `A0 + B0`
    - 至少 `2` 周我方 baseline 报告
    - 与 `xuan` 的 gap 表
  - 行为层 patch 优先级由 gap 大小决定，不按假设编号顺序决定。
- **“永远不做” 清单**
  - 不恢复 `dynamic pair_target` 作为主模块
  - 不再把旧 `pair_arb` 当成最终承载体
  - 不删除 `FAK/SELL` 代码
  - 不把单 round 现象直接升格成 enforce 规则
- **两周 shadow + PR 引证据**
  - 所有依赖 `BS-x / H-x` 的模块：
    - shadow 至少 `2` 周
    - PR 描述必须引用对应裁决结果与证据
- **D0 定位降级**
  - `PairArbHarvester` 先明确为：
    - 观测工具
    - dry-run / shadow 工具
    - turnover 对照工具
  - 不是默认盈利主模块；参数只算 `initial defaults`

### 3. 会计层与研究层并行，仍是主干
- **会计层**
  - `A0 TrancheLedger`
  - `B0 CadenceTracker`
  - `D0 PairArbHarvester`
- **研究层**
  - 统一研究库三对象：
    - `xuan_round_summary`
    - `xuan_tranche_ledger`
    - `xuan_round_path_features`
  - 两类报告并行：
    - 长窗报告：判定哪些现象稳定存在
    - 单 round 深描：解释路径机制
- `A0/B0/D0` 不被任何 `BS-x/H-x` 阻塞。
- 研究层失败时，行为层只维持 shadow，不阻塞会计层。

### 4. 发布矩阵
- **可立即实施**
  - `A0 TrancheLedger`
  - `B0 CadenceTracker`
  - `D0 Harvester` shadow / dry-run
  - `C1-stub tranche_arb` 骨架
- **需裁决后才能 enforce**
  - `C1-single tranche_arb`
    - 前置：`A0 + B0 + 我方 baseline 2 周 + BS-1 已解`
  - `C1-twoside (round-open)`
    - 前置：`H-2` 通过
  - `Phase H final-clip`
    - 前置：`H-1` 通过
  - `Depth-aware sizing`
    - 前置：`H-3` 通过
  - `Abandon-sell` 放宽
    - 前置：`H-4` 通过
- **永久规则**
  - 不删除 `FAK/SELL` 代码
  - 不把 `dynamic target` 重新拉回主线
  - 不用单 round 证据直接写死参数

## 测试与验收
### 会计层
- `A0`
  - 单测：多 tranche 并发、partial merge、abandon 精准归因
- `B0`
  - 单测：run length、half-life、cover delay、MTM history
- `D0`
  - dry-run `7d`
  - 参数扫描 `min_full_set × interval`
  - 不以盈利为第一目标，以 turnover/触发合理性为目标

### 研究层
- `S0`
  - 现有 `95` 个连续 round 自动重建完成
- `S1`
  - `2-4` 周长窗样本落库
- `S2`
  - 每个 `H-x` 都必须产出：
    - `通过`
    - `不通过`
    - `无法裁决`
- `PnL` 归因报告必须达到：
  - `70%` 盈利 round 的 `TOP-2` 来源可解释

### 行为层
- 任何 enforce 模块：
  - shadow `>= 2w`
  - PR 必须引用对应 `BS-x/H-x` 裁决证据
  - enforce 后 `1w` 对比 baseline，恶化即回退 shadow

## 假设与默认
- 默认 `xuan` 研究仍然尽量往“完整机制还原”推进，但工程不上不可裁决的东西。
- 默认 `v3.2` 的单 round 发现全部继续是 `L2` 候选，不直接升格。
- 默认 baseline 观察窗口从 `1` 周提升到 `>= 2` 周，避免长尾恢复分布失真。
- 默认 `v3.3` 是结构基线，`v3.4` 只提供操作纪律补丁，不提供新的研究主轴。
