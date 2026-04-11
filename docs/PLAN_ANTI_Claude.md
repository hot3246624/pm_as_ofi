# PLAN_Codex 最终版深度评估（纯离散状态机版）

## 总体判断

**这是迄今写得最干净、最诚实的一份计划。评分：9/10。**

它的核心美德是**克制**——在经历了多轮越加越复杂的修改后，这份计划终于做了减法而不是加法。它的每一项 Key Change 都是在删除一层不属于 `pair_arb` 核心语义的叠加逻辑，而不是在已有的复杂度上再堆一层。

---

## 一、逐项评估

### 1. 移除 Round Suitability Gate ✅ **完全正确，最高优先级**

> [!IMPORTANT]
> 这是本计划中**最重要的改动**，且代码里确认了问题的严重性。

代码现状（[coordinator.rs:1955-2016](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator.rs#L1955-L2016)）：

```rust
fn evaluate_pair_arb_round_suitability(...) -> Option<RoundSuitability> {
    // 前 60s 返回 None → tick() 中直接 return，策略完全沉默
    if now < self.pair_arb_round_gate_until {
        return None;  // ← 整整 60 秒不挂任何单
    }
    // 如果 min_mid < 0.35 且 sum_mid >= pair_target + gap → SkippedImbalanced
    // → 整轮沉默，永不恢复
}
```

然后在 `tick()` 主循环中（[coordinator.rs:2186-2206](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator.rs#L2186-L2206)）：

```rust
match self.evaluate_pair_arb_round_suitability(&ub, now) {
    None => { return; }                    // 前60s: 完全不做任何事
    Some(SkippedImbalanced) => {           // 判定不适合: 整轮永久沉默
        self.clear_target(Side::Yes, ...).await;
        self.clear_target(Side::No, ...).await;
        return;
    }
    Some(Eligible) => {}                   // 通过了才继续
}
```

**问题分析**：

1. **15 分钟市场浪费 4 分钟**：前 60s 观察 + 判定后整轮沉默 = 一个 15min 窗口里最多损失 25% 的交易时间。对于利润微薄的 pair_arb，这是致命的机会成本。
2. **判定标准脆弱**：`min_mid < 0.35` 这个阈值是固定的魔法数字。BTC 15m 市场在开盘时有明显的价格发散（YES/NO 之和偏离 1.0），这个判定很容易误触发。
3. **不可恢复**：一旦标记为 `SkippedImbalanced`，该轮永远不会恢复。即使市场在第 2 分钟就恢复均衡，策略也坐在场外 13 分钟。

**结论**：必须删除。`pair_arb` 的 tier cap + VWAP ceiling + SoftClose 已经足够保护高失衡场景，不需要一个全局的"这轮不玩"开关。

---

### 2. 把 `should_keep_candidate()` 收缩成纯硬约束 ✅ **方向完全正确**

代码现状（[pair_arb.rs:439-540](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/strategy/pair_arb.rs#L439-L540)）：

```rust
fn should_keep_candidate(...) -> bool {
    // 第一关：inventory gate + simulate_buy → 硬约束，正确 ✓
    
    // 第二关：improves_locked_pnl || improves_pair_cost || reaches_target_pair
    //   → 如果满足任一，直接通过。这是"收益过滤的快速通道"。
    
    // 第三关：如果是 risk_increasing 且不满足第二关：
    //   → 计算 utility_delta，与 min_utility_delta 比较
    //   → 如果不满足 → skip_utility_delta（被计数器记录）
    //   → 计算 open_edge_improvement，与 min_open_edge 比较
    //   → 如果不满足 → skip_open_edge（被计数器记录）
}
```

**问题分析**：

这段逻辑的本质是：策略层（价格链）已经算好了一个报价，但 `should_keep_candidate` 又引入了一套**独立的收益评估函数**来二次否决策略的决策。这相当于策略层说"该挂 YES@0.45"，但收益过滤层说"不行，utility_delta 太小，不够格"。

从 [coordinator.rs:1689](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator.rs#L1689) 可以看到 `pair_arb_skip_utility_delta` 确实被累加到了 shutdown 统计中，而且干跑日志中 `skip(util/edge)=0/0` 的事实说明**这些过滤器在当前参数下几乎从不触发**——但一旦触发，它们会在策略已经认为应该挂单的情况下，默默吞掉那笔单子，且理由隐藏在 `trace!` 级别日志中，极难追踪。

> [!WARNING]
> **注意**：`min_utility_delta_for_risk_increasing` 在 High bucket 时返回 `HIGH_IMBALANCE_UTILITY_MULT * base`，这是一个更高的门槛。如果 PLAN_Claude 的 Fix B（High bucket 禁止 RiskIncreasing）被实施，这个路径理论上永远不会被走到——但如果不实施 Fix B 而只删除 utility check，High bucket 的 RiskIncreasing 将失去最后一道防线。
>
> **因此：Key Change 2 和 PLAN_Claude 的 Fix B 之间存在顺序依赖。必须先实施 Fix B（或类似的 High bucket 硬约束），才能安全删除 utility_delta 过滤。**

---

### 3. 保留并锁死状态驱动执行语义 ✅ **已验证，代码与计划一致**

代码验证：
- `pair_arb_should_force_freshness_republish` 对两种角色都返回 `(false, ..., delta)`（[coordinator_execution.rs:30-41](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_execution.rs#L30-L41)）→ 没有连续 freshness reprice。
- `pair_arb_should_retain_existing` 在 `state_changed && !state_aligned` 时返回 `false`（[coordinator_execution.rs:307-318](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/coordinator_execution.rs#L307-L318)）→ 状态变化时强制重评。

**唯一问题**：计划说"旧单不得跨状态 retain"，但代码事实上是 `state_changed && !state_aligned` 才 republish。如果碰巧状态变了但新旧价格差异 < 1 tick，仍然会 retain。这在大多数场景下是安全优化（避免无效的 API churn），但在 PLAN_Claude 描述的 Bug A 场景中——YES@0.52 vs fresh=0.62，gap=10 ticks——`state_aligned` 为 false，所以**会触发 republish**。

等等，让我再仔细看一下。回到 Bug A 的链路：

```
state_changed=true
state_aligned = (0.52 - 0.62).abs() = 0.10 > tick=0.01 → state_aligned=false
→ 行 307-318：return false（不 retain）→ 重发 ✓
```

**所以 PLAN_Claude 描述的 Bug A 似乎已经被当前代码的 `state_aligned` 检查覆盖了？** 但 PLAN_Claude 声称 YES@0.52 被 retained——这意味着根因可能不在 `state_changed && !state_aligned` 这个分支，而是在它之后的 `still_admissible` 检查中。让我重新看代码流程：

```rust
// 行 272-280：
let still_admissible = !fill_recheck
    || self.pair_arb_quote_still_admissible(...);

// 行 307-318：
if state_changed && !state_aligned {
    return false;  // 状态变+价格偏离 → 重发
}

// 行 320-322：
if !still_admissible || force_freshness_republish {
    return false;  // admissible 失败 → 重发
}
```

关键发现：**`state_changed` 和 `fill_recheck` 是独立的**。行 272 中 `still_admissible = !fill_recheck || admissible_check`。如果 `fill_recheck=false`，则 `still_admissible=true`，跳过 admissible 检查。

但行 262-263 说：`state_changed = prev_state != Some(state_key)`，`fill_recheck = self.slot_pair_arb_fill_recheck_pending[idx]`。

在 Bug A 的场景中：NO 填单后 → `fill_recheck` 被设为 true（通过 `rescind_with_fill_recheck`），同时 `state_key` 变化。因此 `state_changed=true` 且 `fill_recheck=true`。

行 307 `state_changed && !state_aligned` → YES@0.52 vs fresh=0.62 → `state_aligned=false` → `return false` → **应该重发**。

**这说明 PLAN_Claude 的 Bug A 分析可能有误，或者实际 live 中的 state_key 并没有真正变化**（可能因为 YES 侧的 state_key 更新时机问题）。这需要通过日志确认。

> [!CAUTION]
> PLAN_Claude 的 Fix A（upward_stale 检查）本身是无害的加固，但如果 Bug A 的真正根因是 state_key 变化的时序问题（YES slot 的 state_key 在 NO fill 后是否真的更新了），那么 Fix A 是在正确的位置加了一道保险，但可能不是对根因的修复。建议在实施前先确认 April 10 日志中 YES slot 的 `state_changed` 字段实际值。

---

### 4. 保留"单账本策略脑" ✅ **务实选择**

计划明确承认了 `working inventory` 策略脑的 venue rollback race 风险，但选择不在本轮增加复杂度来消除它。这是正确的优先级排序——先把控制流问题修干净，再讨论最终性语义。

`PairProgressState / PairProgressRegime` 降级为 diagnostics-only 是正确决定。当前它们通过 `fragile` 间接影响 `fill_recheck_pending`，但一旦 `fill_recheck` 完全由 `Matched/Failed` 事件直接驱动（而不是通过 fragile 中介），这些状态就只有观测价值。

---

### 5. 不改当前 tier 和 safety margin ⚠️ **与 PLAN_Claude 存在分歧**

这是本计划中最值得商榷的决定。计划明确说"不采纳 High bucket hard ban"，但 PLAN_Claude 的 Bug B 分析清楚地证明了 `tier_2_mult=0.30` 在 High bucket 中创造了价格悬崖（YES 从 0.35 瞬间跌到 0.14）。

PLAN_Codex 的立场是：
> "先把状态机和策略真相统一，tier 是否过深留给下一轮 live 样本判断"

PLAN_Claude 的立场是：
> "当 |net_diff| >= 10 时，继续向主仓侧增加 RiskIncreasing 订单根本就是错误的。正确做法是完全停止 RiskIncreasing。"

**我的评估**：

PLAN_Claude 在这个问题上是对的。`tier_2_mult=0.30` 把主仓侧的买价打到真实市价的 ~30%，这不是"策略参数需要调优"的问题，而是**在已知高风险区域继续加仓本身就是策略逻辑错误**。即使你把 `tier_2_mult` 调到 0.50 或 0.60，High bucket 继续 RiskIncreasing 仍然是在错误的方向上积累风险。

但 PLAN_Codex 把这个推迟到"下一层策略哲学决策"也有其合理性——如果本轮的目标严格限定为"控制流 correctness 收口"，那么参数和策略哲学确实可以分离。

> [!WARNING]
> **风险提示**：如果在删除 `utility_delta` 过滤（Key Change 2）的同时不实施 High bucket hard ban，系统在 High bucket 将同时失去两道防线。建议至少将 Fix B 的 admissibility 版本（5 行代码）作为**本轮必须项**保留。

---

### 6. 文档同步 ✅ **必要且正确**

无特别评论。唯一建议：在文档中明确写清 `state_aligned` 优化的语义——即"状态变了但新旧价格差 < 1 tick 时仍可 retain"——这是一个非显然的实现细节。

---

## 二、计划与 PLAN_Claude / PLAN_Claude_temp 的交叉评估

| 议题 | PLAN_Codex 立场 | PLAN_Claude 立场 | 评估 |
|------|----------------|-----------------|------|
| Round Suitability | 删除 | 未提及 | **一致**（PLAN_Claude 未反对） |
| utility/open_edge | 删除 | 未提及 | **一致**（PLAN_Claude 分析的 skip=0/0 说明不影响） |
| High bucket ban | 推迟 | P0 紧急修复 | **分歧**。建议采纳 PLAN_Claude 的 Fix B |
| upward stale 检查 | 未提及 | P0 紧急修复 | **遗漏**。建议采纳 PLAN_Claude 的 Fix A |
| VWAP safety margin | Assumptions 中保留 | P1 | **一致** |
| PairingOrReducing 连续 band | 已删除 (commit 45fdad8) | 已过时 | **一致** |

> [!IMPORTANT]
> **最关键的遗漏**：PLAN_Codex 完全没有提及 PLAN_Claude 的 Fix A（upward stale 检查）和 Fix B（High bucket RiskIncreasing ban）。这两个是 April 10 实盘灾难的直接修复，应该被整合进 PLAN_Codex 作为 P0 前置条件，而不是分属两个独立文档。

---

## 三、Test Plan 评估

计划的 6 项测试覆盖了核心场景，但有几处需要补充：

1. **测试 3（无第二层收益过滤）**中说"shutdown / 日志中的 `skip(util/edge)` 应归零或消失"——但如果只是移除了 `should_keep_candidate` 中的 utility check，计数器变量本身仍然在 `strategy.rs` 和 `coordinator.rs` 中声明。需要明确是删除整个计数通路还是只置零。

2. **缺少 High bucket 场景测试**——即使本轮推迟 High bucket ban，也应该有一个回归测试验证 `tier_2_mult=0.30` 在 High bucket 下的实际报价行为，作为已知风险的基线记录。

3. **live canary（测试 6）是最重要的**——计划正确地强调了"先跑 1-2 轮小额 live，不靠 dry-run"。Dry-run 由于 `net_diff` 始终为 0，无法验证状态切换、fill_recheck、和 tier cap 行为。

---

## 四、总结

| 维度 | 评分 | 说明 |
|------|------|------|
| 架构方向 | **10/10** | 纯离散状态机是正确的终局设计 |
| 问题诊断精度 | **9/10** | Round Suitability 和 utility/open_edge 的定位极准 |
| 与 PLAN_Claude 对齐 | **6/10** | 缺失 Fix A/Fix B 整合，存在 High bucket ban 分歧 |
| 实施可行性 | **9/10** | 每项改动独立、可测、可回滚 |
| 风险管理 | **7/10** | 推迟 High bucket ban 有真实风险，需要 Fix B 兜底 |

### 建议的最终实施顺序

1. **P0-a**：PLAN_Claude Fix A（upward stale 检查，5 行）+ Fix B admissibility 版本（5 行）
2. **P0-b**：PLAN_Codex Key Change 1（删除 Round Suitability Gate）
3. **P0-c**：PLAN_Codex Key Change 2（收缩 `should_keep_candidate`）—— **必须在 Fix B 之后**
4. **P1**：PLAN_Codex Key Change 4（PairProgressState 降级）
5. **P1**：PLAN_Claude_temp VWAP safety margin
6. **P2**：文档同步
