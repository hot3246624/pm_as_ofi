# PLAN_Codex 深度评估报告

## 一、总体判断

**这是迄今为止最精准、最克制的一版改造计划。** 它找到了当前 `pair_arb` 三个月实盘迭代中最核心的低效根因——不是 tier 参数、不是 OFI 灵敏度、不是 settled/fragile 状态机复杂度——而是 **执行语义没有区分两类本质不同的买单**。

从 `STRATEGY_PAIR_ARB_ZH.md` 的策略哲学出发，这版计划做到了"改得少、改得准"。

---

## 二、与策略哲学的对齐度分析

### 2.1 策略核心哲学回顾

根据 [STRATEGY_PAIR_ARB_ZH.md](file:///Users/hot/web3Scientist/pm_as_ofi/docs/STRATEGY_PAIR_ARB_ZH.md) 定义的四条核心：

1. **Maker-only / Buy-only / Pair-cost-first**
2. **单一策略状态脑**（不用 settled/fragile 分裂视图）
3. **5/10 阶梯即时切换**
4. **尾 45s 只做配对，不扩风险**

### 2.2 逐项对齐

| 哲学要求 | PLAN_Codex 覆盖 | 评判 |
| :--- | :--- | :--- |
| maker-only | ✅ 不引入 taker/HardClose | 完美对齐 |
| pair-cost-first | ✅ VWAP ceiling 仍是价格链终极约束 | 完美对齐 |
| 单一状态脑 | ✅ 明确放弃 fragile/settled/pending，保留 working-only | 完美对齐 |
| 5/10 阶梯 | ✅ 不改 tier 参数，不改切换逻辑 | 完美对齐 |
| SoftClose | ✅ 配对腿继续允许，risk-increasing 阻断 | 完美对齐 |
| OFI 从属 | ✅ 不改 OFI 数学，配对腿忽略 OFI | 完美对齐 |

> [!TIP]
> 这版计划的最大美德是 **"不碰不该碰的东西"**。相比前几版对 settled/fragile/timeout-promotion 的反复折腾，这版的克制程度本身就是一种架构品味。

---

## 三、架构优雅度评估

### 3.1 核心设计原则："按角色拆执行语义"

计划的整个改造只围绕一个命题：

> **同一个 PairArb BUY，根据它是 `risk-increasing` 还是 `pairing/reducing`，应该有不同的执行行为。**

这个拆分点选得极好，原因是：

1. **`candidate_risk_effect()` 已经存在**（[pair_arb.rs:285-299](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy/pair_arb.rs#L285-L299)）。策略层已经在计算每个候选 BUY 是 `PairingOrReducing` 还是 `RiskIncreasing`，只是之前没有把这个信息传递到执行层。
2. **不需要新增状态**。`candidate_risk_effect` 是纯函数，输入是当前 `inv.net_diff + side + size`，不依赖历史、不引入新状态变量。
3. **不需要修改价格链数学**。A-S、skew、tier cap、VWAP ceiling 全部不动。唯一变化是 maker clamp 和 retain 的适用条件。

### 3.2 三层修改的内聚性

| 修改 | 层级 | 改什么 | 不改什么 |
| :--- | :--- | :--- | :--- |
| Fix 1: maker clamp 按角色拆 | 策略定价层 (`pair_arb.rs`) | 配对腿不持续跟 ask 下拉 | A-S/skew/tier cap/VWAP ceiling |
| Fix 2: retain 按角色拆 | 发布执行层 (`coordinator_order_io.rs`) | 配对腿允许向上 reprice | state_key 切换逻辑、fill recheck |
| Fix 3: post-only 改为动作约束 | 发布执行层 | 初次放单保持 post-only，但不持续拖价 | order_manager 协议 |

这三层的内聚性非常高——每层只改一个条件判断，且三个改动之间没有交叉依赖（任何一个单独上线都不会破坏另外两个）。

> [!NOTE]
> 相比之下，之前的 V5 计划需要同时修改 `InventorySnapshot` 数据结构、`pair_arb` 定价逻辑、`coordinator_execution` 的状态机、和 `coordinator_order_io` 的发布逻辑——四个层面耦合在一起。这版只动两个文件的条件分支。

---

## 四、执行效率评估

### 4.1 Fix 1（配对腿去 maker clamp）的效率

**问题定位精确。** 从 [pair_arb.rs:192-197](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy/pair_arb.rs#L192-L197) 可以看到，当前的 maker clamp 对 YES 和 NO **无差别施加**：

```rust
if ub.yes_ask > 0.0 {
    raw_yes = f64::min(raw_yes, ub.yes_ask - yes_safety_margin);
}
if ub.no_ask > 0.0 {
    raw_no = f64::min(raw_no, ub.no_ask - no_safety_margin);
}
```

计划提议的修改方式也很直接——在 clamp 前增加 `is_pairing` 判断。这只需要在现有代码的 L192 前插入 2 行变量声明，然后在 L192 和 L195 各加一个 `&& !xxx_is_pairing` 条件。

**代码改动量：约 6 行。**

### 4.2 Fix 2（retain 按角色拆）的效率

**问题定位精确。** 从 [coordinator_order_io.rs:356-371](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_order_io.rs#L356-L371) 可以看到，当前的 retain 只看 `price > slot_price`，不区分角色：

```rust
if needs_reprice
    && self.cfg.strategy == StrategyKind::PairArb
    && ...
    && price > slot_price + reprice_eps  // ← 所有向上都 retain
{
    retain_hits++;
    return;
}
```

计划提议在此处增加 `candidate_risk_effect` 判断：
- 如果是 `RiskIncreasing`：保持现有 retain（不追更高价）
- 如果是 `PairingOrReducing`：只 retain ≤ 2 ticks 的小漂移，> 2 ticks 放行 reprice

这需要在 L356 前获取当前 `inv`、计算 `risk_effect`，然后修改条件。新增约 10 行代码。

> [!IMPORTANT]
> 这里有一个实现细节需要注意：`slot_place_or_reprice` 当前用 `self.current_working_inventory()` 获取库存（L58）。这个函数已经在上下文中被调用了，所以获取 `risk_effect` 的成本几乎为零——不需要额外的 I/O 或状态查询。

### 4.3 Fix 3（post-only 改为动作约束）

这一条在计划中描述得最清楚，本质是：

- **不改 `pair_arb.rs` 的价格输出**（策略层输出的是 strategic target）
- **在 `slot_place_or_reprice` 的 `!active`（新单初放）分支里做 clamp**
- **在 `active`（已有挂单）路径上，不再为配对腿做持续 clamp**

这实际上是 Fix 1 的自然推论：策略层不再 clamp → 执行层只在初次放置时做安全检查。

---

## 五、风险分析

### 5.1 风险 1：配对腿 strategic target 高于 best ask 时的行为

**场景**：配对腿（比如 NO）的 VWAP ceiling = 0.66，当前 NO best_ask = 0.64。策略输出 `raw_no = 0.66`（因为不再 clamp）。

**当前代码行为**：在 `slot_place_or_reprice` 的 `!active` 分支，如果按计划在初次放置时做 post-only clamp，则实际下单价 = `min(0.66, 0.64 - safety_margin) ≈ 0.63`。这是正确的。

**但如果市场随后反弹**（ask 从 0.64 涨到 0.70），live 挂单在 0.63。按 Fix 2 的逻辑，fresh target 0.66 > live 0.63 = +3 ticks > 2 ticks 阈值 → 触发 Republish。新单再次初放 clamp → `min(0.66, 0.70 - margin) = 0.66`。

**这是完全正确的行为**：配对腿终于能追上市场上行了。✅

### 5.2 风险 2：配对腿 strategic target 高于 best ask 且 ask 极窄时的 post-only 循环

**场景**：VWAP ceiling = 0.66，NO ask = 0.66（刚好等于）。clamp 后 = `0.66 - 0.01 = 0.65`。下单 0.65。ask 保持 0.66。fresh target 仍然 0.66，但 live 是 0.65。差 1 tick ≤ 2 ticks → retain。

**几秒后 ask 微跌到 0.65**。CLOB 自动成交 live 挂单。成交价 0.65。

**这就是 Fix 1 + Fix 3 的设计效果**：配对腿在 VWAP ceiling 附近"等鱼上钩"，而不是追到底部。与之前在 0.55 成交相比，改善了 0.10 的成交价。✅

### 5.3 ⚠️ 风险 3（唯一存疑）：`net_diff` 恰好在 ±5 边界时的角色判定晃动

当 `inv.net_diff` 恰好在 ±5 附近（比如 4.9 或 5.1）时：

- `candidate_risk_effect(inv, Side::Yes, 5.0)` 的结果取决于 `(4.9 + 5.0).abs() = 9.9` vs `4.9.abs() = 4.9` → 9.9 > 4.9 → `RiskIncreasing`
- 但如果 `net_diff = 5.1`：`(5.1 + 5.0).abs() = 10.1` vs `5.1.abs() = 5.1` → 10.1 > 5.1 → 仍然 `RiskIncreasing`
- 如果 `net_diff = -5.1`（YES 是配对腿）：`(-5.1 + 5.0).abs() = 0.1` vs `5.1` → 0.1 < 5.1 → `PairingOrReducing` ✅

**边界行为正确。** `candidate_risk_effect` 的数学定义（projected `|net_diff|` vs current `|net_diff|`）天然处理了边界情况。在 `net_diff` 正负翻转时，同侧永远是 RiskIncreasing（因为加仓只会远离 0),对侧永远是 PairingOrReducing（加仓靠近 0）。

但有一个微妙的 **tick-by-tick 晃动风险**：如果 `net_diff` 在 `4.99` 和 `5.01` 之间高频振荡（因为市场微结构导致的小幅 fill），两侧的角色可能在 `RiskIncreasing` 和 `PairingOrReducing` 之间反复切换。这会导致 maker clamp 一会儿施加、一会儿不施加→价格一会儿被压低、一会儿抬回→触发频繁的 Republish。

**缓解**：当前的 `pair_arb_effective_reprice_band` 已经在 `|net_diff| < bid_size` 时放宽了 reprice band（+2 ticks），所以在 flat/low 区间，微小价格变化不会触发 reprice。但在 `|net_diff| ≈ 5` 时，reprice band 回到 base，角色切换造成的价格跳变可能超出 band。

> [!WARNING]
> **建议**：在 Fix 1 中，将 `is_pairing` 的判定从 `net_diff >= TIER_1_NET_DIFF` 改为带有小幅迟滞的 `net_diff >= TIER_1_NET_DIFF + 0.5 * bid_size` 或者直接复用 `candidate_risk_effect` 而不用硬编码 threshold。后者更优雅，因为它自动包含了 projected net_diff 的方向判断。

---

## 六、与 PLAN_Claude 修复方案的对比

| 维度 | PLAN_Claude 原版 Fix 1-3 | PLAN_Codex 新版 |
| :--- | :--- | :--- |
| 改动范围 | 2 个文件 | 2 个文件（相同） |
| 核心思想 | retain 放宽 + 配对腿去 clamp + pairing floor | retain 按角色拆 + 配对腿去 clamp + post-only 动作化 |
| 是否引入新参数 | `PAIR_ARB_MAX_RETAIN_UP_TICKS` | 同（2 ticks） |
| 是否引入新状态 | 否 | 否 |
| pairing floor | 有（90% VWAP ceiling） | 无（认为不需要） |
| 文档和日志 | 无规划 | 有完整的可观测性设计（Section 6） |

**PLAN_Codex 的改进**：
1. 更清晰地表述了"为什么不要 pairing floor"——配对腿由 VWAP ceiling 自然约束，额外的 floor 会在低迷市场中强行挂出不符合市场价的订单。
2. 增加了 Section 6 可观测性设计，这在 PLAN_Claude 中完全缺失。`candidate_role`、`strategic_target_price` vs `action_price`、`retain_block_reason` 这几个日志字段对后续排障极其关键。
3. Section 3 "post-only 是动作约束，不是长期定价目标"的表述非常精确，这是 PLAN_Claude 没有明确说出来的设计哲学。

**PLAN_Claude 的优势**：
1. `pairing_floor` 在极端情况下（A-S skew 和 VWAP ceiling 都给出很低价格时）能保证配对腿不会报出"明明有利润空间但报价远低于 breakeven"的价格。PLAN_Codex 放弃了这层保护，需要依赖 VWAP ceiling 本身的正确性。

---

## 七、最终评分

| 维度 | 评分 | 说明 |
| :--- | :--- | :--- |
| 策略哲学对齐 | **10/10** | 完美遵守"单一状态脑、pair-cost-first、maker-only"三大原则 |
| 架构优雅度 | **9/10** | 改动极少、内聚极高、无耦合、无新状态。扣 1 分因为 `is_pairing` 判定建议用 `candidate_risk_effect` 而非硬编码 threshold |
| 执行效率 | **9/10** | 代码改动量约 20 行，零运行时开销。扣 1 分因为缺少边界迟滞设计 |
| 风险控制 | **8/10** | 扣 2 分因为放弃了 `pairing_floor` 且存在 `net_diff≈5` 边界晃动风险 |
| 可观测性 | **9/10** | Section 6 设计完整实用，但缺少 `pairing_upward_reprice_count` 的 shutdown metrics |
| 完整性 | **9/10** | Test Plan 完整覆盖 4 个场景，有 04-08 回放验收。扣 1 分因为没有提及 merge sync 期间的行为 |

**总分：9.0/10**

> [!TIP]
> **结论**：这版计划可以直接执行。它用最小的代码改动解决了最核心的问题（配对腿成交价劣化和挂单僵死），同时完美保持了策略哲学的一致性。建议在实施时注意上述两个边界风险点（Section 5.3 的迟滞设计和 `pairing_floor` 是否需要保留一个轻量版本）。
