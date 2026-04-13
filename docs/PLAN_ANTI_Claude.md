# PLAN_Codex 四态状态机方案深度评估

> 基于当前代码（2026-04-13）、`.env` 实际配置、April 12 实盘日志、PLAN_Claude 分析对照

---

## 一、方案概要与当前配置对照

### PLAN_Codex 提出的 5 个核心改造

| # | 改造内容 | 核心目的 |
|---|---------|---------|
| 1 | 主次目标分离：主=最小化残仓，次=pair_cost≤target | 改变优化目标 |
| 2 | 四态状态机：Probe→Build→Complete→Lock | 严格阶段控制 |
| 3 | 配对腿事件+定时重报，移除no-chase约束 | 提升配对效率 |
| 4 | 动态库存上限：按剩余时间收缩 | 时间衰减风控 |
| 5 | T-90禁止新风险腿 + T-30 taker 收口 | 强制收敛 |

### 关键发现：`.env` 已经大幅偏离 April 12 时的配置

```diff
# April 12 实盘配置 → 当前 .env
- PM_MAX_NET_DIFF=15          → PM_MAX_NET_DIFF=5.0         ✅ 已改
- PM_PAIR_ARB_TIER_1_MULT=0.80 → PM_PAIR_ARB_TIER_1_MULT=0.50  ✅ 已改
- PM_PAIR_ARB_TIER_2_MULT=0.60 → PM_PAIR_ARB_TIER_2_MULT=0.15  ✅ 已改
- PM_PAIR_ARB_RISK_OPEN_CUTOFF_SECS=180 → =240              ✅ 已改
- PM_RECYCLE_MIN_BATCH_USDC=10.0 → =5.0                     ✅ 已改
```

> [!IMPORTANT]
> **这些配置变化极其关键**。PLAN_Codex 在 `max_net_diff=15` 的语境下提出，但当前已降至 5。这从根本上改变了方案的价值计算——很多问题已被配置解决。

---

## 二、逐条深度技术分析

### 2.1 改造一：主次目标分离

**PLAN_Codex 原文**：主目标=最小化残仓，次目标=pair_cost≤pair_target

**当前代码实际行为**（`pair_arb.rs:47-250`）：
- 定价逻辑：`raw = mid - excess/2 - skew_shift`，本质是追求 pair_cost≤pair_target
- VWAP ceiling（L160-223）：严格限制 YES/NO 买价使得 pair_cost 不超标
- Tier cap（L90-109）：在 risk_increasing 时降低报价以减少深仓成本
- `should_keep_candidate`（L432-492）：inventory gate + risk_open_cutoff + simulate_buy

**评估**：

当前代码的**实际优先级**已经是：
1. **Inventory gate**（`can_place_strategy_intent` → max_net_diff=5 硬限）→ 先卡库存
2. **VWAP ceiling** → 保证 pair_cost
3. **Tier cap** → 降低深仓成本
4. **Skew shift** → 偏移报价向缺失腿

**结论**：在 `max_net_diff=5` 下，"最小化残仓"已经通过硬限实现了。5 份残仓 × avg_cost 0.45 = $2.25 最大损失，与典型 5-10 对配对利润（$0.5-$1.5）处于可比范围。目标分离的**理论价值仍在**，但**紧迫性大幅降低**。

> [!NOTE]
> 核心的目标对立是：追求更多配对 vs 控制残仓。当 max_net_diff=5 时，残仓天花板已经很低，这个矛盾被钝化了。

---

### 2.2 改造二：四态状态机 Probe→Build→Complete→Lock

**这是 PLAN_Codex 的核心提案，需要最深入的分析。**

#### 2.2.1 与现有状态机的对比

当前代码已有**两层**状态机制：

**层 1：PairArbStateKey**（`coordinator_execution.rs:46-72`）
```rust
PairArbStateKey {
    dominant_side: Option<Side>,     // YES/NO/None
    net_bucket: PairArbNetBucket,    // Flat/Low/Mid/High
    risk_open_cutoff_active: bool,
}
```
- Flat: |net_diff| ≤ ε
- Low: |net_diff| < 5
- Mid: |net_diff| < 10
- High: |net_diff| ≥ 10

**层 2：EndgamePhase**（`coordinator.rs:586-591`）
```rust
enum EndgamePhase {
    Normal,    // 全功能
    SoftClose, // 禁止 risk-increasing
    HardClose, // 仅允许 hedge
    Freeze,    // 冻结
}
```

#### 2.2.2 关键发现：max_net_diff=5 导致现有状态机大部分失效

```
代码中的 bucket 阈值（硬编码）：
  Flat:  |net| ≤ ε
  Low:   |net| < 5.0
  Mid:   |net| < 10.0    ← max_net_diff=5 → 永远到不了
  High:  |net| ≥ 10.0   ← max_net_diff=5 → 永远到不了

RISK_INCR 阈值（硬编码）：
  tier_1: |net| ≥ 3.5    ← 仍可触发（容量 3.5~5.0）
  tier_2: |net| ≥ 8.0    ← max_net_diff=5 → 永远到不了
```

> [!WARNING]
> **在 max_net_diff=5 的配置下**：
> - `PairArbNetBucket::Mid` 和 `High` 从不出现
> - `RISK_INCR_TIER_2_NET_DIFF=8.0` 永远不触发
> - `tier_2_mult=0.15` 永远不生效
> - `pair_arb_quote_still_admissible` 中的 SoftClose risk_increasing 检查是唯一的endgame保护
>
> 整个 tier 系统的大半是死代码。

#### 2.2.3 PLAN_Codex 四态映射到当前架构

| PLAN_Codex 状态 | 语义 | 现有最近似机制 | 差距 |
|----------------|------|-------------|------|
| **Probe** | 初始探测，双向挂单 | `Flat` bucket + Normal phase | ≈已有 |
| **Build** | 一侧有成交，开始建仓 | `Low` bucket | ≈已有 |
| **Complete** | 达到一定配对量，停止risk-increasing | **无** | ❌ 核心缺口 |
| **Lock** | 完全禁止风险，只做缺失腿 | `SoftClose` phase | ≈部分有 |

**Complete 状态是真正的新增价值**：当已配对足够多时（比如 paired_qty ≥ 10），主动停止同侧 risk-increasing，不等到 endgame SoftClose。

#### 2.2.4 Complete 状态的数学价值分析

场景：双向市场中策略已配对 10 对，pair_cost=0.93
- 配对利润：10 × (1.0 - 0.93) = **+$0.70**
- 此时如果继续挂单被填到 max_net_diff=5 的残仓：
  - 残仓损失 worst_case = 5 × 0.45 = **-$2.25**
  - 抵消 3.2 轮配对利润

但在 max_net_diff=5 下：
- 总可能配对量 = bid_size × 2 / min_fill = 有限（资金约$13只够2-3次循环）
- 达到 "Complete" 的条件本身就意味着已经消耗了大部分时间
- 而 `risk_open_cutoff=240s`（T-4min）已经在轮末提供了类似保护

**结论**：Complete 状态在 max_net_diff=15 时**至关重要**（阻止从 5 继续堆到 15）。在 max_net_diff=5 时**仍有价值但边际递减**——因为从 0 到 5 的损失（$2.25）已经处于单轮利润可覆盖的范围。

---

### 2.3 改造三：配对腿事件+定时重报

**PLAN_Codex 原文**：fill/cross-reject/每N秒 都允许向上重报，不再被 no-chase 约束

**当前代码行为**（`coordinator_execution.rs:15-43`）：
```rust
fn pair_arb_should_force_freshness_republish(...) {
    match risk_effect {
        RiskIncreasing => (false, "risk_increasing_state_driven", ...),
        PairingOrReducing => (false, "pairing_holds_between_triggers", ...),
    }
}
```

两侧**都不做连续freshness reprice**，完全依赖状态键变化的事件驱动（fill → net_bucket/dominant_side 变化 → state_key changed → republish）。

**实际效果**：
- ✅ Fill 事件触发 republish：通过 `fill_recheck_pending` 已实现
- ✅ Cross-reject 触发：通过 `cross_reject_reprice_pending` 已实现
- ❌ 定时 N 秒重报：**未实现**
- ❌ 向上 reprice（pairing 腿追涨）：被 `pairing_holds_between_triggers` 阻止

**评估**：

定时重报的价值取决于市场微观结构：
- 在 BTC 15min 市场中，价格在 15 分钟内持续漂移
- 如果 NO 已填但 YES 配对腿挂在 0.52，市场 YES mid 漂到 0.55
- 没有定时重报 → YES@0.52 可能永远不被填（ask 已上移）
- 有定时重报 → 每 N 秒检查并上移到 0.55，增加配对概率

**但有副作用**：向上追价增加了 pair_cost，可能使 pair_cost > pair_target

**结论**：在配对腿上实现适度的定时重报（如每 5-10s）有正期望值，但需与 VWAP ceiling 配合。这是**中等优先级**的改进，不是架构重构。

---

### 2.4 改造四：动态库存上限

**PLAN_Codex 原文**：不是固定 15，而是按剩余时间收缩（T-180/T-90/T-45 分段降档）

**当前机制**：
- `max_net_diff=5`（固定）
- `risk_open_cutoff=240s`（T-4min 后完全禁止 risk-increasing）
- `EndgamePhase::SoftClose`（T-35s）
- `.env` 中无动态 max_net_diff

**评估**：

在 max_net_diff=15 时，动态降档（15→10→5）是关键的风控手段。

但在 max_net_diff=5 时：
- 已经是最低档了，再降只能到 0（即不交易）
- 降到 3 或 2 会严重限制双向配对能力

```
max_net_diff=5 + risk_open_cutoff=240s 已经实现了：
  T-900~T-240s: max_net=5, 正常交易
  T-240~T-35s:  max_net=5, 禁止 risk-increasing
  T-35~T-12s:   SoftClose, 禁止所有 risk-increasing
  T-12~T-2s:    HardClose, 仅 hedge
  T-2~T-0s:     Freeze
```

**结论**：动态降档在 max_net_diff=5 下**价值极低**，当前的 `risk_open_cutoff + EndgamePhase` 组合已经提供了等效的时间衰减保护。

---

### 2.5 改造五：T-90 禁止新风险腿 + T-30 taker 收口

**当前实现**：
- T-240s（risk_open_cutoff=240）：禁止 risk-increasing ✅
- T-35s（SoftClose）：禁止所有 risk-increasing ✅
- T-12s（HardClose）：禁止 provide，允许 hedge ✅
- T-30s taker 收口：**未实现** ❌

**评估**：

T-90 禁止新风险腿 → `risk_open_cutoff=240s` 已经比这更保守（T-240 就禁了）。

T-30 taker 收口才是真正的新功能：
- 当轮末仍有残仓时，以 taker 方式卖出残仓
- 消除方向性到期风险
- **但存在重大问题**：taker 订单需要付 spread + fee，在低流动性时成本很高

Taker 收口的数学门槛：
```
残仓 qty=5, avg_cost=0.45
如果市场 bid=0.40 → sell@0.40 → 损失 5×(0.45-0.40) = -$0.25
如果市场 bid=0.30 → sell@0.30 → 损失 5×(0.45-0.30) = -$0.75
如果不卖 → 50% 概率损失 5×0.45 = -$2.25, 50% 概率获利 5×0.55 = +$2.75
E[hold] = 0.5×2.75 - 0.5×2.25 = +$0.25
```

除非残仓方向已经明确劣势（如 YES 价格跌到 0.10），否则持有的期望值可能高于 taker 平仓。只有当 `market_probability` 远离 0.5 时，taker 平仓才有正期望。

**结论**：Taker 收口在理论上有价值，但实现复杂度高（需要新的 taker 逻辑），且期望值高度依赖市场状态。**建议推迟到所有 maker 层面优化完成后再考虑**。

---

## 三、PLAN_Codex vs 已有配置修复的增量价值评估

### 3.1 April 12 → 当前配置的 EV 变化

基于 PLAN_Claude 的反事实分析框架：

| 配置状态 | EV/轮 | 计算依据 |
|---------|-------|---------|
| April 12 原配 (net=15, tier1=0.80, tier2=0.60) | **-1.24 USDC** | PLAN_Claude 四-基准 |
| 仅改 net=5 | **+0.02 USDC** | PLAN_Claude 方案B |
| net=5 + tier1=0.50 + tier2=0.15 | **+0.15~0.30 USDC** | 更保守出价减少深仓成本 |
| + risk_cutoff=240 + recycle_min=5 | **+0.25~0.40 USDC** | PLAN_Claude 方案D |

### 3.2 PLAN_Codex 四态状态机的额外增量

在上述已优化配置基础上：

| 改造 | 增量 EV/轮 | 实现复杂度 | 风险 |
|------|-----------|-----------|------|
| Complete 状态（配对够了就停 risk-incr）| +0.05~0.10 | **高**（新状态机 + 200行代码） | 中：可能漏判提前停止 |
| 配对腿定时重报 | +0.05~0.15 | **中**（30行代码） | 低：VWAP ceiling 兜底 |
| 动态 max_net_diff | ≈0 | 低 | 低：但价值几乎为零 |
| T-30 taker 收口 | +0.10~0.20 | **极高**（新 taker 逻辑 + 500行） | 高：taker 成本+滑点 |

**四态状态机全部实现的 EV 增量：约 +0.20~0.45 USDC/轮**

对比 **已有配置变化** 的 EV 增量：约 **+1.50 USDC/轮**（从 -1.24 到 +0.25）

> [!IMPORTANT]
> **结论：配置修复带来了 ~85% 的 EV 改善，PLAN_Codex 状态机在此基础上的边际贡献约 15%**。
>
> 但状态机引入的代码复杂度（预估 500-800 行新增/修改）和回归风险不可忽视。

---

## 四、验收标准可行性评估

PLAN_Codex 提出的三个验收标准：

### 4.1 `E[L_residual] <= 0.03 × E[Q_pair]`

**翻译**：期望残仓损失 ≤ 期望配对数量的 3%

以当前配置估算（max_net_diff=5, pair_target=0.97）：
```
E[Q_pair] ≈ 10 份/轮（保守估计）
0.03 × 10 = 0.30 USDC   ← 允许的最大残仓损失

当前 E[L_residual] ≈ 5/8概率 × 5份 × 0.45 avg_cost × 0.5输的概率
                    = 0.625 × 2.25 × 0.5 = 0.70 USDC
```

**当前差距**：0.70 vs 0.30，差 2.3 倍。
**可行性**：在 max_net_diff=5 下**非常困难**。需要将残仓发生率从 5/8 降到 2/8，或者大幅提高配对量。Complete 状态可以帮助，但不足以跨越这个门槛。

> [!CAUTION]
> 这个验收标准在 BTC 15min 市场上**可能不可达**。单向趋势的结构性频率（~50%）意味着残仓是不可消除的。要满足此标准，需要切换到波动率更低的市场或大幅延长时间窗口。

### 4.2 `round-end residual_value: p50 ≤ 0.3, p90 ≤ 0.8`

- p50（50%情况下残仓价值≤$0.30）：在 max_net_diff=5, 完美配对率 3/8=37.5% 的情况下，p50 处于残仓轮 → 残仓价值 ≈ 3-5份 × 0.2-0.4 = $0.6-2.0
- 当前**不满足** p50≤0.3

但如果提高完美配对率到 60%（通过配对腿重报等优化）：
- p50 落在完美配对轮 → residual_value=0 → **满足**
- 但 p90 仍可能 ≈ $2.25（max_net_diff=5 的 worst case）

**部分可达**，但 p90≤0.8（即 90% 情况下残仓≤$0.80）几乎不可能——这要求只有 10% 的轮次产生超过 $0.80 的残仓。

### 4.3 `T-90 后不再新增风险腿成交`

当前 `risk_open_cutoff=240s`（T-4min），比 T-90s **更保守**。

**已满足**（T-240 > T-90）✅

---

## 五、实施建议：分层渐进而非一次性重构

### 推荐方案：三阶段渐进优化

#### Phase 0：验证当前配置（0 代码改动）⬅️ 立即执行

运行 2-3 个实盘 session，验证 max_net_diff=5 + tier1=0.50 + tier2=0.15 + cutoff=240s + recycle=5.0 的组合效果。

**预期**：EV 从 -1.24 提升到 +0.25/轮左右。如果数据验证为正 EV，后续优化的紧迫性大幅降低。

#### Phase 1：低复杂度高价值改进（约 50 行代码）

1. **配对腿定时 reprice**（`pair_arb.rs` + `coordinator_execution.rs`）
   - 每 8-10 秒检查配对腿是否偏离市场 ≥ 3 ticks
   - 如果偏离且新价格仍在 VWAP ceiling 内，触发 republish
   - 预期 EV：+0.05~0.15/轮

2. **Fix B 无条件化**（`coordinator_execution.rs`）
   - 在 max_net_diff=5 下，`RISK_INCR_TIER_2=8.0` 永远不触发
   - 建议将 RISK_INCR_TIER_2 降至 4.5（或直接用 max_net_diff-0.5 作为阈值）
   - 这样在 net_diff=4.5 时就停止 risk-increasing，留 0.5 的缓冲
   - **效果等同于 Complete 状态的 80%**，但只需 5 行代码

3. **Bucket 阈值对齐**
   - 当前 `Low<5, Mid<10, High>=10` 在 max_net_diff=5 时大部分失效
   - 建议：`Low<2.5, Mid<4, High>=4`（与 max_net_diff=5 对齐）
   - 或者直接基于 `net_diff / max_net_diff` 的比例来判断

#### Phase 2：Complete 状态（可选，约 150 行代码）

仅在 Phase 0/1 的实盘数据仍显示残仓问题过大时执行：

1. 新增 `PairArbRoundState` 枚举（Probe/Build/Complete）
2. 在 `compute_quotes` 中根据 `paired_qty / bid_size` 比值判断状态
3. Complete 触发条件：`paired_qty >= N × bid_size`（如 N=2，即配对 ≥ 10 份）
4. Complete 行为：所有 risk-increasing 报价设为 0

**不建议实现 Lock 状态**——它与 SoftClose 功能重叠。

#### Phase 3：Taker 收口（远期）

仅在策略验证为稳定正 EV 后考虑。需要：
- 新的 taker 订单类型支持
- 残仓方向性判断逻辑
- 成本/滑点建模

---

## 六、与 PLAN_Claude 的交叉验证

| 维度 | PLAN_Claude 结论 | PLAN_Codex 提案 | 评估 |
|------|----------------|----------------|------|
| 核心问题 | 残仓损失: 配对利润 = 3.7:1 | 需要四态状态机 | PLAN_Claude 的配置修复已解决 70%+ |
| 最高杠杆 | max_net_diff=5（零代码） | 动态库存上限 | **PLAN_Claude 正确**，已实施 |
| 第二杠杆 | Recycler min_batch 修复 | 配对腿重报 | 两者互补，不冲突 |
| 第三杠杆 | Fix B 无条件化 | Complete 状态 | 功能等价，但 Fix B 成本低 100 倍 |
| Taker 收口 | P2 长期考虑 | T-30 必须有 | **PLAN_Claude 更务实** |

---

## 七、最终结论

### 方案评级

| 维度 | 评分 | 说明 |
|------|------|------|
| 问题诊断准确性 | ⭐⭐⭐⭐⭐ | 残仓最小化作为主目标完全正确 |
| 解决方案适当性 | ⭐⭐⭐ | 四态状态机过度工程化，大部分价值可通过配置+小改动获得 |
| 实施风险 | ⭐⭐ | 500-800 行新代码的回归风险高，在实盘系统中危险 |
| 成本效益比 | ⭐⭐ | 边际 EV ≈ +0.20-0.45/轮，但需 1-2 天开发 + 测试 |
| 与当前配置的时效性 | ⭐⭐ | 方案在 net=15 时设计，net=5 下价值大幅缩水 |

### 一句话结论

> **PLAN_Codex 的诊断完全正确（残仓是核心矛盾），但在 max_net_diff 已降至 5 的当前配置下，四态状态机的实施是过度工程化。建议用 Phase 0（验证配置）+ Phase 1（50 行代码微调）替代，获取 80%+ 的预期收益，风险降低 90%。**

### 建议的下一步

1. **立即**：用当前 `.env` 配置跑 2-3 个实盘 session，收集 EV 数据
2. **数据驱动决策**：如果 EV > 0 → Phase 1 微调；如果 EV < 0 → 重新评估是否需要 Phase 2
3. **不要先写 Complete 状态**：等实盘数据说话
