# PLAN_Codex 深度分析评估报告

## 一、事故复盘：实盘日志验证

通过 04-08 日志，精确还原了 PLAN_Codex 描述的场景：

关键时序（每行对应日志行号）：

- **03:47:06**  NO 5.00@0.59 Matched (96fef2) → net=0.0  ← 看起来平衡
- **03:47:18**  metrics: net_diff=0.00 Healthy           ← 系统以为已配对
- **03:47:34**  NO 5.00@0.54 Matched (39dc67) → YES=5, NO=10, net=-5
- **03:47:34**  YES 5.00@0.37 Matched (a05526) → YES=10, NO=10, net=0    ← 双侧同时匹配
- **03:47:34**  Reconcile: NO_BUY 39dc67 missing on exchange → slot freed ← reconcile 抢先释放槽位
- **03:47:34**  OMS: YES_BUY OrderFilled → Slot freed
- **03:47:38**  ← 仅 4 秒后 → coordinator 下单 YES@0.32 (基于 net=0 的 working 视图)
- **03:47:43**  NO 5.00@0.54 Failed (39dc67) → YES=10, NO=5, net=5.0
- **03:47:43**  YES 5.00@0.32 Matched (06a67b) → YES=15, NO=5, net=10.0  ← 131ms 内填入！取消令在 03:47:43.829 发出，fill 在 03:47:43.960 到达

此后系统陷入 net=10 / Stalled，反复出现 NO 的 Matched→Failed 循环：

| 订单 | Matched | Failed | 间隔 |
| :--- | :--- | :--- | :--- |
| 0xead99f | 03:46:13 | 03:46:17 | 4.4s |
| 0xb35d85 | 03:46:49 | 03:46:53 | 4.0s |
| 0x39dc67 | 03:47:34 | 03:47:43 | 9.7s ← 致命窗口 |
| 0x0827a5 | 03:49:47 | 03:49:54 | 7.6s |
| 0xd6137e | 03:50:22 | 03:50:27 | 5.0s |
| 0x46551f | 03:50:56 | 03:51:02 | 6.3s |

PLAN_Codex 的问题定位完全准确。

---

## 二、计划各部分逐项评分

### 变更 1：双视图库存（settled + working）

- **核心逻辑**：正确 ✅
- **实现假设**：存在重大缺陷 ❌

**致命发现**：Polymarket User WS 从未发送 Confirmed 事件。

# 跨所有真实交易日志的 status=Confirmed 计数
- polymarket.log.2026-04-07-real: 0
- polymarket.log.2026-04-03-real: 0
- polymarket.log.2026-04-02-real: 0
- polymarket.log.2026-04-08:      0

`user_ws.rs:324` 映射了 `"MINED" | "CONFIRMED" → FillStatus::Confirmed`，但这两种 event type 在实盘中从未出现。Polymarket 的实际协议是：Matched 即生效，Failed 才是异常事件。没有独立的 Confirmed 流。

**后果**：如果严格按计划实现，settled 视图将永远为空，系统将永久拒绝所有"激进"方向的操作（relief、reanchor、新的 dominant-side 买单），等同于完全停止做市。

**必须修正**：将 Confirmed 触发替换为超时自动晋升（基于观测数据，15–20s 未见 Failed 则晋升为 settled）。

---

### 变更 2：非对称权限模型

逻辑正确 ✅，是计划中最有价值的设计原则。

**具体映射**：
- 放松约束（dominant relief、bucket 改善、risk fill anchor reset） → 仅读 settled ✅
- 增加保守性（missing-leg pairing、invalidation、stalled 判断） → 读 working ✅

一个需要细化的点：当 settled 长期滞后（超时晋升前），系统会在较长时间内拒绝补充 missing leg 的 NO 单。由于 NO 单本身是"保守行动"（降低 YES-heavy 敞口），应该用 working 视图决策，这点计划说对了。

---

### 变更 3：状态机双视图拆分

正确 ✅
- `StateImprovementReanchor` → settled improvement：防止 provisional relief 触发 anchor reset
- `FillTriggeredInvalidationCheck` → working worsening：匹配 NO 的 Matched→Failed 后要立即触发

04-08 事故中，如果 `StateImprovementReanchor` 只接受 settled（而非 working net=0），YES@0.32 就不会被允许下单。

---

### 变更 4：槽位生命周期修正（Slot Soft Release）

必要但非根因 ⚠️

04-08 事故中，槽位释放实际是 reconcile 触发的，不是 CONFIRMED：
- 03:47:34.390  Reconcile: NO_BUY 39dc67 missing on exchange → releasing slot
- 03:47:34.394  OMS: NO_BUY OrderFilled -> Slot freed
- 03:47:34.394  OMS: YES_BUY OrderFilled -> Slot freed

Reconcile 判定"exchange 无单"后主动释放，与此同时 NO fill 仍处于 provisional Matched 状态。槽位释放本身不是错误（确实需要重建报价），但释放后立刻下单 YES@0.32 才是错误。

根因仍是视图问题：槽位空闲 + working 视图显示 net=0 → 系统认为可以重新下 YES 单。若改为 settled 视图（YES=5, NO=0, net=5.0 → YES-heavy），则拒绝下新的 YES 买单。

Soft Release 机制改进的真正价值在于：防止 slot_active 状态与 exchange 状态出现 split-brain（exchange 无单但 Coordinator 认为 active），这会导致缺失腿无法及时重建。这个修正有独立价值。

---

### 变更 5：Matched→Failed 不再"先放松再反悔"

正确 ✅，且包含了一个很好的操作改进：

> **NO failed → working worsening invalidation → Republish**（而不是普通撤单慢路径）

这个即时 Republish 机制可以将 YES@0.32 这类危险单在 Failed 到达时立刻撤销，缩短暴露窗口。不过在 04-08 事故中，131ms 的竞态条件（取消令 03:47:43.829，fill 03:47:43.960）仍然无法被软件层完全消除。计划的价值在于防止 YES@0.32 被下单，而不是加速撤单。

---

### 变更 6：可观测性

优秀 ✅，特别是：
- `decision_view={settled|working}`    ← 让每个决策可追溯
- `slot_live_on_exchange vs slot_target_present_local`  ← split-brain 诊断
- `fill_status_conflict(order_id, first=Matched, later=Failed)`  ← 异常模式追踪

这类可观测性在事后分析中价值极高，也方便验证计划实施是否生效。

---

## 三、计划遗漏/需补充的内容

### 遗漏 1（严重）：Confirmed 事件不存在
如上分析，计划必须加入超时晋升机制：
Matched 后 T=15–20s 内无 Failed → 自动晋升为 settled。基于实测数据，所有 Failed 事件均在 10s 内到达，因此 15s 超时安全。

### 遗漏 2：Reconcile 侧的 provisional 感知
Reconcile 在 "missing on exchange" 时释放槽位是正确的，但释放前应该检查该槽位是否有尚未 settle 的 pending fill。如果 NO 槽位有 pending fill (Matched，未超时)，reconcile "missing" 发现可能是交易所状态还未刷新，不应立即释放为 Idle。

### 遗漏 3：重复 Matched→Failed 的市场微结构问题
04-08 的 6 次连续 NO Matched→Failed 并非随机，而是系统性问题（链上流动性或价格水平导致 NO 在该价位无法真实成交）。计划不处理这个底层问题，但加入一个计数器（同一 slot 在 N 次 Matched→Failed 后触发 admission 降级或价格收缩）会有帮助。

---

## 四、总体评估

| 维度 | 评分 | 说明 |
| :--- | :--- | :--- |
| 问题诊断准确性 | 9/10 | 完全复现日志事故，根因正确 |
| 架构设计合理性 | 8/10 | 非对称权限模型正确，slot 治理有价值 |
| 实现可行性 | 5/10 | Confirmed 假设在 Polymarket 上不成立 |
| 完整性 | 7/10 | 遗漏 timeout-promotion 机制，reconcile 交互 |
| 测试计划质量 | 7/10 | 覆盖核心场景，但 Test 4 依赖不存在的 Confirmed |
| 可观测性 | 9/10 | 全面，实用 |

**结论**：计划框架正确，核心设计原则（settled 只允许激进操作、working 允许保守操作）是解决这类问题的正确方向。但在 Polymarket 环境下，Confirmed 事件永远不会来这一实证发现让计划的整个 settled 层无法运作。

**最重要的修订项**：在实现前，将 settled 的推进触发从 `FillStatus::Confirmed` 改为：
> **"Matched fill 存在超过 T=15s 且未被 Failed 撤销" → 自动晋升为 settled。**

这一改动不影响计划的其他部分，但是它是整个双视图体系能否真正工作的前提。