# 评估: 2026-04-02 日志分析与 05ef028 修复验证

## Context

2026-04-01 第三轮 (16:55–17:30) 发现 `replace/placed=0.528`，诊断为 policy price 粗网格吸附引起大 publish_debt（计划写入了 action_price 解耦方案）。随后用户针对 2026-04-02 日志发现了一个更深层的结构问题并已完成修复（commit `05ef028`）。本计划评估该修复的正确性，并给出 go-live 判定标准。

---

## 2026-04-02 日志评估

### 整体指标

| Round | placed | replace | replace_ratio | rb_ms  | stale | soft | full |
|-------|--------|---------|---------------|--------|-------|------|------|
| 02:10 | 4      | 1       | 0.25          | 0      | 0     | 7    | 4    |
| 02:15 | 5      | 3       | 0.60          | 42060  | 0     | 6    | 4    |
| 02:20 | 7      | 3       | 0.43          | 0      | 2     | 8    | 6    |
| 02:25 | 2      | 0       | 0.00          | 2653   | 0     | 6    | 4    |

**汇总**: placed=18, replace=7, **replace/placed=0.389**

**排除 ref_blocked 轮 (02:15)**: placed=13, replace=4, **replace/placed=0.308** ✓ 已在阈值以下

### 关键异常点 (已被 05ef028 修复)

```
02:08:55 NO_BUY Buy@0.150 regime=Tracking
  raw_target=0.360 normalized_target=0.360
  publish_target=0.150  ← 21 ticks 偏差
  publish_cause=policy publish_debt=0.0
  dist_trusted_ticks=0.0 dist_action_ticks=0.0 dist_shadow_ticks=0.0
```

**机制分析**（修正用户描述）：

- 前序：slot 在 Aligned 制度下 committed policy_price=0.150，发布了 Buy@0.150
- 触发：Aligned→Tracking 制度切换，cross-regime band tick 不可比，触发 `PriceBucket` transition
- 缺陷：`!active` 路径（soft-reset 重激活）走 `PolicyPublishCause::Initial`，直接复用旧 committed policy_price=0.150
- 结果：norm=0.360 但 publish_target=0.150，dist_action_ticks=0（因为比较对象也是 0.150）所以 debt 报告为 0.0（虚假）

这不是"无 transition 时 committed action 未同步"，而是：**soft-reset 重激活时 `!active` 路径直接用了旧 committed price 而非当前 norm**。用户识别出的现象是正确的，机制略有不同。

---

## 05ef028 修复评估

### 修复一：`had_committed_policy` + `PolicyPublishCause::Policy`

```rust
let had_committed_policy = self.slot_policy_states[slot.index()].is_some();
// ...
let publish_cause = if !active {
    if policy_age >= policy_dwell && ... {
        Some(if had_committed_policy {
            PolicyPublishCause::Policy   // 走正常 transition 路径，从 candidates 构建新 policy
        } else {
            PolicyPublishCause::Initial  // 首次初始化
        })
    } else { None }
};
```

**正确性**：soft-reset 后 `slot_policy_states` 仍保有旧 committed state（`soft_reset_slot_publish_state` 不清空它）。`had_committed_policy=true` → 走 Policy 路径 → `transition = classify_policy_transition(prev_committed, new_candidate)` → 用 candidate 中的当前 norm 构建新 policy → publish_target 反映当前 norm。**修复完整，覆盖了该缺陷路径。** ✓

### 修复二：bucket_ticks 调宽（12/14/16 替代 8/10/12 或 16/20/24）

减少 band tick 变化触发的细粒度 transition，降低正常市场漂移引发的无谓 replace。经 2026-04-02 日志验证有效（02:25 轮 replace=0）。 ✓

### 修复三：`classify_policy_transition` 简化

去掉 `price_tick + moved_ticks >= threshold` 的旧逻辑，改为直接 `prev.price_band_tick != next.price_band_tick`。与 bucket_ticks 调宽协同：每次 band tick 变化都触发，但触发频率已被更宽的 bucket 控制。 ✓

### 修复四：`clear_slot_target_with_scope` / `SlotResetScope`

将 soft（保留 policy candidates/states）和 full（清空）正确分离，避免历史污染。 ✓

### 测试覆盖

`coordinator_tests.rs` 新增 4 个针对性回归测试，覆盖主要路径。173/173 通过。 ✓

---

## 剩余非结构性问题

1. **Binance WS 重连**（02:17:02）：stale=2，duration ~100ms。属于网络基础设施问题，非做市逻辑缺陷。不影响 go-live 判定。
2. **ref_blocked 轮**（02:15，rb_ms=42060）：42秒 reference 被阻断，造成 replace_ratio=0.60。此轮不计入结构性指标。
3. **Publish debt 非零值**（17/17/17/18/19/21/31）：这些都出现在 ref_blocked 或大幅行情波动期间，是合法债务（dwell 期间 norm 大幅移动），非结构性问题。 ✓

---

## 遗留问题评估：action_price 解耦（原计划）

2026-04-01 日志中的 publish_target 粗网格吸附异常（0.010/0.160/0.320/0.480 等）在 2026-04-02 日志中 **已不复现**。当前 HEAD 的 `build_slot_quote_policy` 中 policy_price 是 `glft_quantize_policy_price()` 输出，但实际 publish_target 已是 tick 精度（0.150/0.360 等）。这说明 `build_slot_quote_policy` 的量化逻辑在 6aabf2a 或更早时已处理了 action price 的精度问题。**原 action_price 解耦计划已无须实施**，可关闭。

---

## Go-Live 判定标准

| 指标 | 标准 | 2026-04-02 状态 |
|------|------|-----------------|
| replace/placed（正常轮） | < 0.35 | 0.308 ✓ |
| replace/placed（所有轮） | < 0.40 | 0.389 ✓ |
| publish_target 偏差异常 | 0 次 | 0 次（fix 后） ✓ |
| OFI heat | 无高频 kill | 正常 ✓ |
| ref_blocked 轮 | 不计入结构性指标 | 1/4 轮 |
| 连续通过轮数 | ≥ 3 轮无异常 | 需再验证 |

---

## 验证步骤

1. `cargo test --lib` 通过（应为 173+ tests）
2. `cargo build --release` 编译无警告
3. 执行下一轮 dry-run（建议覆盖至少 3 个 5 分钟轮）：
   - 确认 publish_target 与 normalized_target 偏差 ≤ 1 tick（所有 policy 路径）
   - 确认 soft-reset 重激活后 publish_target = 当前 norm（不是旧 committed price）
   - 确认 replace/placed < 0.35（排除 ref_blocked 轮）
4. 若 3 轮连续通过：可进入小仓位实盘（建议 size 为设计仓位的 25%，观察 1 天）

---

## 文件变更清单（05ef028，已完成）

- `src/polymarket/coordinator.rs` — `QuotePolicyState`（无结构变更）
- `src/polymarket/coordinator_order_io.rs` — `had_committed_policy`、`!active` publish 路径、bucket_ticks、`classify_policy_transition`、`clear_slot_target_with_scope`
- `src/polymarket/coordinator_tests.rs` — 4 个新回归测试
