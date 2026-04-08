## Pair_arb 两阶段成交语义收敛计划（修复 MATCHED/FAILED 状态不同步）

### Summary
当前主问题不是 `pair_arb` 数学本身，而是单一库存真相把 `MATCHED` 和 `FAILED` 混成了一条即时事实，导致：

- `MATCHED` 过早放松风险约束
- `FAILED` 迟到后又把库存拉回去
- 旧单在错误状态下被保留或仅靠普通撤单路径失效
- `OMS` 与 `Coordinator` 的槽位活跃状态出现 split-brain

本轮固定采用 **两阶段成交语义**，但不是“等 `CONFIRMED` 才行动”。  
核心原则锁死为：

- `MATCHED` 可以立即让系统 **更保守**
- `MATCHED` 不可以立即让系统 **更激进/更放松**
- `CONFIRMED` 才允许正式释放风险约束
- `FAILED` 只回滚 provisional 状态，不再让错误真相先污染整套决策

### Key Changes
#### 1. Inventory 改为 `settled + provisional` 双视图
把当前单一 [InventoryState](/Users/hot/web3Scientist/pm_as_ofi/src/polymarket/messages.rs:219) 升级为一个统一快照，例如：

- `settled`: 最终可信库存
- `working`: `settled + unresolved matched fills`
- `pending_fills`: 仅内部追踪，按 `order_id + side + direction + size` 维护 unresolved matched

固定语义：
- `Matched`
  - 只进入 `pending_fills`
  - 更新 `working`
  - 不直接并入 `settled`
- `Confirmed`
  - 若存在同 key pending：从 pending 提升到 settled
  - 若无 pending：按 confirmed-first 容错直接并入 settled
- `Failed`
  - 只撤销 pending 对 `working` 的影响
  - 不再从 `settled` 里“反删除”已最终确认的库存
- `Merge`
  - 同步作用于 `settled` 与 `working`
  - merge 后 pending 仍保留，但要重新映射剩余可解释库存

#### 2. 策略与执行改成“非对称权限”
`pair_arb` 不等待 `CONFIRMED` 才工作，但不同动作读取不同库存视图。

固定规则：
- **会增加风险 / 放松约束** 的动作，只能基于 `settled`
  - dominant-side relief
  - net bucket 改善
  - risk-fill anchor reset
  - 主仓侧旧单继续 retain
  - 解除 stalled / 高失衡 admission
  - 上调主仓侧报价
- **会降低风险 / 增强保守性** 的动作，可以基于 `working`
  - 缺失侧 pairing quote
  - 状态恶化后的旧单 invalidation
  - 进入高失衡 / stalled
  - 继续压低主仓侧价格
  - SoftClose 阻断
- 直接结果：
  - `NO matched` 不能立刻让 `YES` 旧危险单获得保留资格
  - 但 `YES matched` 可以立刻促使系统去补 `NO`

#### 3. `pair_arb` 状态机按双视图拆分
当前 `pair_arb_state_key / PairProgressState / StateImprovementReanchor / invalidation check` 统一吃单一 `InventoryState`。本轮收敛为：

- `Risk state key` 用 `settled`
  - `dominant_side`
  - `net_bucket`
  - `pair progress improvement`
  - risk-fill anchor reset
- `Safety/invalidation state` 用 `working`
  - 当前 live quote 是否因更坏库存已不合法
  - 是否要立刻撤掉/重发主仓侧旧单
  - 是否进入 `Stalled`

固定要求：
- `StateImprovementReanchor` 只接受 **settled improvement**
- `FillTriggeredInvalidationCheck` 接受 **working worsening**
- `MATCHED` 只能触发保守方向的重检，不能触发 relief

#### 4. 槽位生命周期改成单一真相，消除 OMS/Coordinator split-brain
当前 `OMS` 在 `OrderFilled` 时会把 slot 置 `Idle`，但 `Coordinator` 仍可能保留 `slot_targets`。  
这轮必须引入 Coordinator 侧的 **slot soft release**。

固定行为：
- 以下事件一旦发生，Coordinator 必须同步 soft release 对应槽位：
  - `OrderFilled`
  - reconcile missing-on-exchange release
  - 明确 slot no-longer-live 的 terminal event
- `soft release` 只清：
  - 当前已发布 live target
  - 当前 slot active 标记
- `soft release` 不清：
  - `pair_arb` policy continuity
  - recheck pending
  - risk/progress state
- `clear/full reset` 仍只保留给：
  - source blocked
  - invalid state
  - round cleanup

结果：
- 交易所无单时，Coordinator 不再误以为 slot 仍 active
- 缺失腿不会因为本地旧 target 残留而错过立即重建

#### 5. `Matched -> Failed` 不再走“先放松、后反悔”
对于本次 `04-08` 场景，系统需要保证：

- `NO matched`
  - 可以触发缺失腿/风险重检
  - 不能让主仓侧 `YES@0.32` 因“看起来 net=0”而继续被视为安全
- `NO failed`
  - 只移除 provisional relief
  - 立即触发 working worsening invalidation
  - 若 live order 已无效，走 `Republish`，不是普通慢路径等待

也就是说，本轮不追求把 `t2` 再压到极限，而是先把 `t1` 对策略的伤害从结构上消掉。

#### 6. 可观测性补齐
新增明确指标与日志，避免以后再靠猜：

- `matched_pending_events`
- `confirmed_promotions`
- `failed_pending_reverts`
- `slot_soft_release_events`
- `risk_relief_blocked_by_provisional`
- `working_worsening_invalidation`
- `fill_status_conflict(order_id, first=Matched, later=Failed)`

日志每次打印：
- `settled_net_diff`
- `working_net_diff`
- `pending_yes_qty`
- `pending_no_qty`
- `decision_view={settled|working}`
- `slot_live_on_exchange`
- `slot_target_present_local`

### Public Interfaces / Types
新增或升级最小接口：

- `InventorySnapshot`
  - `settled: InventoryState`
  - `working: InventoryState`
- `PendingFillRecord`
- `InventoryViewKind::{Settled, Working}`
- `soft_release_slot_target(...)`
- `FillLifecycleMetrics`
  - `matched_pending`
  - `confirmed`
  - `failed_reverted`

策略输入从单一 `&InventoryState` 升级为 `&InventorySnapshot` 或等价的双视图访问接口。  
不新增 `.env` 参数，不修改 `pair_arb` 价格链、tier 参数、OFI、SoftClose 哲学。

### Test Plan
1. **Matched 不再立即放松风险**
- 初始 `YES-heavy`
- 缺失侧 `NO matched` 到来但未 confirmed
- 主仓侧旧 `YES` 单不得因 provisional relief 被 retain
- dominant-side relief / bucket improvement 不得触发

2. **Matched 仍允许快速保守动作**
- `YES matched` 让 `working` 风险更高
- 缺失侧 `NO` pairing quote 仍可立即生成
- stalled / invalidation 可立即生效

3. **Failed 只回滚 provisional**
- `Matched -> Failed` 同 order id
- `working` 回退，`settled` 不被错误污染
- 若该回退使 live quote 不合法，立即 `Republish`

4. **Confirmed promotion**
- `Matched -> Confirmed`
- pending 正确提升到 settled
- 之后才允许 relief/reanchor/reset risk anchors

5. **Slot soft release**
- `OrderFilled` 后 OMS `Idle`
- Coordinator 同步 soft release
- 同价或近价缺失腿下一 tick 可立即重建
- 不再出现“前端没单，Coordinator 还认为 active”的状态

6. **04-08 回放验收**
- `NO matched` 后 `YES@0.32` 不能再因为 provisional `net=0` 获得保留
- 即便 `NO failed` 迟到约 9.7s，系统也不会在这段时间里把 risk relief 当真
- 收盘前不再因为同类 provisional relief 把残仓放大到 `net=10`

### Assumptions
- 主线继续 `pair_arb + BTC 15m`
- 本轮不重写 `pair_arb` 数学，不讨论 tier 参数是否再压低
- 当前第一矛盾是成交状态语义和槽位生命周期，不是撤单速度本身
- `MATCHED` 仍然有价值，但只能用于“更保守”，不能用于“更放松”
- 若这轮完成后 live 仍明显亏损，再评估是否是 `pair_arb + BTC 15m` 的策略边界，而不是继续修 correctness
