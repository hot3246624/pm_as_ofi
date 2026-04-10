     │ PLAN_Codex V2 深度评估与实施计划                                        │
     │                                                                         │
     │ Context                                                                 │
     │                                                                         │
     │ 用户要求对 docs/PLAN_Codex.md（V2                                       │
     │ 版本）进行深度分析评估。本次评估基于：                                  │
     │ - 代码探索（pair_arb.rs、coordinator_order_io.rs、coordinator_execution │
     │ .rs、executor.rs、coordinator_tests.rs）                                │
     │ - 2026-04-09 实盘日志暴露的三个根本问题（Ghost Order / RETAIN /         │
     │ 未对冲单侧积累）                                                        │
     │ - 2026-04-08 实盘分析的已有结论                                         │
     │                                                                         │
     │ ---                                                                     │
     │ 代码现状与 V2 计划对照                                                  │
     │                                                                         │
     │ 1. 已经正确实现的部分（V2 确认即可）                                    │
     │                                                                         │
     │ V2 节: 状态层三字段                                                     │
     │ 代码现状: PairArbStateKey { dominant_side, net_bucket,                  │
     │ soft_close_active } 已在                                                │
     │   coordinator_execution.rs:105-132 存在                                 │
     │ 结论: ✅ 无需改动结构                                                   │
     │ ────────────────────────────────────────                                │
     │ V2 节: PairingOrReducing 2-tick retain                                  │
     │ 代码现状: coordinator_order_io.rs ~410: upward_ticks <= 2.0 + 1e-9 →    │
     │ retain                                                                  │
     │ 结论: ✅ 基础逻辑已有，但阈值 V2 改成 3 ticks                           │
     │ ────────────────────────────────────────                                │
     │ V2 节: strategic_target 不受 action_price 污染                          │
     │ 代码现状: pair_arb.rs:186-201 risk-increasing 侧才做 post-only          │
     │ clamp，pairing                                                          │
     │   侧保留 strategic target                                               │
     │ 结论: ✅ 设计意图已存在，只需规范化                                     │
     │ ────────────────────────────────────────                                │
     │ V2 节: 状态变化触发强制重评                                             │
     │ 代码现状: pair_arb_state_changed = true → RETAIN 条件不满足             │
     │ 结论: ✅ 已实现                                                         │
     │                                                                         │
     │ ---                                                                     │
     │ 2. V2 核心修改项（有代码基础，需要定向改动）                            │
     │                                                                         │
     │ 2a. RiskIncreasing 侧缺失 downward republish（最高价值单点修改）        │
     │                                                                         │
     │ 现状（coordinator_order_io.rs ~410）：                                  │
     │ Some(PairArbRiskEffect::RiskIncreasing) => (true,                       │
     │ "same_side_no_chase"),                                                  │
     │ // ← 无论 upward 还是 downward，一律 retain                             │
     │                                                                         │
     │ 问题：当网络故障期间市场大幅下行，risk-increasing 腿仍挂着 stale        │
     │ 高价单（因为 price > slot_price 是 upward，触发                         │
     │ RETAIN），导致成交价远离当前市场。                                      │
     │                                                                         │
     │ V2 期望：                                                               │
     │ - upward：retain                                                        │
     │ - downward >= 2 ticks：Republish                                        │
     │                                                                         │
     │ 所需修改：coordinator_order_io.rs ~410，约 8 行，增加 downward_ticks    │
     │ 分支。                                                                  │
     │                                                                         │
     │ ---                                                                     │
     │ 2b. PairingOrReducing band 阈值从 2 → 3 ticks                           │
     │                                                                         │
     │ 现状：upward_ticks <= 2.0 + 1e-9 → retain（单向，只管 upward）          │
     │                                                                         │
     │ V2 期望：双向 3-tick band（|strategic_target - live_target| > 3 ticks   │
     │ 才 Republish）                                                          │
     │                                                                         │
     │ 所需修改：coordinator_order_io.rs ~410，将单向 upward                   │
     │ 判断改为双向绝对值 > 3 ticks。                                          │
     │                                                                         │
     │ ---                                                                     │
     │ 2c. action_price clamp 显式分离                                         │
     │                                                                         │
     │ 现状：risk-increasing 侧在 pair_arb.rs:186-201 已有 maker               │
     │ clamp，但命名不清晰，pairing 侧也可能被意外 clamp。                     │
     │                                                                         │
     │ V2 期望：action_price 只在 place/reprice 动作时才计算，不反向污染       │
     │ strategic_target 存储值。                                               │
     │                                                                         │
     │ 所需修改：pair_arb.rs ~186-201，确保 maker clamp                        │
     │ 只在"真实下单路径"执行，strategic_target 字段不被覆写。                 │
     │                                                                         │
     │ ---                                                                     │
     │ 2d. admissible 收缩为纯硬约束                                           │
     │                                                                         │
     │ 现状：pair_arb_quote_still_admissible() 中包含                          │
     │ utility_delta、open_edge_improvement 等第二套策略逻辑影响 retain 决策。 │
     │                                                                         │
     │ V2 期望：这些移出 admissible，只留：inventory limit、SoftClose、OFI     │
     │ suppress、tier cap、VWAP、simulate_buy。                                │
     │                                                                         │
     │ 所需修改：coordinator_execution.rs admissible 检查路径，剥离            │
     │ utility/open_edge 逻辑。                                                │
     │                                                                         │
     │ ---                                                                     │
     │ 2e. fragile / PairProgressState 从主报价路径退出                        │
     │                                                                         │
     │ 现状：                                                                  │
     │ - fragile 变化 → fill_recheck_pending = true → RETAIN 条件不满足 →      │
     │ 触发重评                                                                │
     │ - PairProgressRegime::Stalled 可能影响某些路径                          │
     │                                                                         │
     │ V2 期望：这些保留在 accounting/diagnostics，不进入主报价决策。          │
     │                                                                         │
     │ 风险点：fragile 目前是 fill_recheck 的主要触发源。移除后，fill          │
     │ 事件必须通过 state_key 变化来触发重评，需验证 fill → state_key          │
     │ 变化链路是否覆盖所有场景。                                              │
     │                                                                         │
     │ 所需修改：coordinator_metrics.rs 中 fragile 触发 fill_recheck           │
     │ 的路径，需先验证 fill → net_diff → state_key 变化能否完全替代。         │
     │                                                                         │
     │ ---                                                                     │
     │ 3. V2 中存在分歧的一项（建议调整）                                      │
     │                                                                         │
     │ Section 6：移除 opposite-slot fuse                                      │
     │                                                                         │
     │ V2 立场：pair_arb_opposite_slot_blocked() 是"执行层 emergency           │
     │ patch"，从主决策链移除，只保留 metrics/debug。                          │
     │                                                                         │
     │ 代码现状（coordinator_execution.rs ~1062-1079）：                       │
     │ - 仅在 High bucket（net_diff >= 10）+ risk-increasing 时触发            │
     │ - 30s 阻断窗口，10s TTL                                                 │
     │ - 触发源：executor 发出 ExecutionFeedback::SlotBlocked                  │
     │                                                                         │
     │ 问题：April 9 的 NO slot 被 Ghost Order 封锁 7 分钟，期间 YES 在        │
     │ 0.30、0.15 继续累积，形成 net_diff=10 的单侧风险。当前 fuse 在 net_diff │
     │  已经 >=10 时才触发，相当于亡羊补牢。                                   │
     │                                                                         │
     │ 分歧不在于"fuse 好不好"，而在于"用什么替代"：                           │
     │                                                                         │
     │ ┌─────────────┬──────────────────────┬───────────────────────────────── │
     │ ───┐                                                                    │
     │ │    方案     │         描述         │                风险              │
     │    │                                                                    │
     │ ├─────────────┼──────────────────────┼───────────────────────────────── │
     │ ───┤                                                                    │
     │ │ V2：直接移  │ tier cap + VWAP +    │ 若 slot                          │
     │ 被物理封锁，这三者都是价格 │                                            │
     │ │ 除          │ SoftClose 兜底       │ 端约束，对执行端无效             │
     │    │                                                                    │
     │ ├─────────────┼──────────────────────┼───────────────────────────────── │
     │ ───┤                                                                    │
     │ │ 保留（改为  │ 继续记录 SlotBlocked │                                  │
     │    │                                                                    │
     │ │ 纯          │ ，但不影响主路径     │ 等于移除，不解决问题             │
     │    │                                                                    │
     │ │ metrics）   │                      │                                  │
     │    │                                                                    │
     │ ├─────────────┼──────────────────────┼───────────────────────────────── │
     │ ───┤                                                                    │
     │ │             │ coordinator 感知     │                                  │
     │    │                                                                    │
     │ │ 升级替代（  │ SlotBusy 持续时长 →  │ 策略层语义，不是 executor        │
     │    │                                                                    │
     │ │ 推荐）      │ 主动降低对侧         │ emergency patch                  │
     │    │                                                                    │
     │ │             │ risk-increasing 意愿 │                                  │
     │    │                                                                    │
     │ └─────────────┴──────────────────────┴───────────────────────────────── │
     │ ───┘                                                                    │
     │                                                                         │
     │ 推荐方案：coordinator 在收到连续 SlotBusy { slot } 时，记录             │
     │ slot_persistently_busy_since，当 busy_duration > 15s 时，对侧           │
     │ risk-increasing 候选的 strategic_target 设为                            │
     │ None（不下单），pairing/reducing 继续允许。这是策略层语义，不依赖       │
     │ executor 的 emergency flag。                                            │
     │                                                                         │
     │ ---                                                                     │
     │ 必须先修复的 Executor 层 Bug（V2 前提）                                 │
     │                                                                         │
     │ Ghost Order Root Cause（reconcile_open_orders ~436-439）                │
     │                                                                         │
     │ 代码现状：                                                              │
     │ // reconcile 时，无论 dust cancel 是否成功，都从 remote mapping 删除    │
     │ if let Some(remote) = remote_by_slot.get_mut(&slot) {                   │
     │     for (id, _) in dust_orders {                                        │
     │         remote.remove(&id);   // ← cancel 失败时也删！                  │
     │     }                                                                   │
     │ }                                                                       │
     │                                                                         │
     │ 后果：cancel 失败 → dust 从 remote_by_slot 删除 →                       │
     │ 本地/远端同步逻辑误以为订单已消失 → 本地 tracked 删除 → 下一轮          │
     │ reconcile 重新发现 → 永无止境循环，且 slot 时而释放时而再锁。           │
     │                                                                         │
     │ 注意：handle_fill_notification 和 handle_cancel_slot                    │
     │ 本身的逻辑已经是正确的（cancel 失败 → SlotBusy，fill + dust →           │
     │ 自动删除）。问题只在 reconcile 的这个分支。                             │
     │                                                                         │
     │ 修复：只有 cancel 成功后才从 remote_by_slot 移除 dust order。cancel     │
     │ 失败时保留在 remote，让后续的本地/远端同步逻辑保留本地 tracked 状态。   │
     │                                                                         │
     │ 文件：executor.rs，约 5 行修改（reconcile_open_orders 的 dust cancel    │
     │ 循环）。                                                                │
     │                                                                         │
     │ ---                                                                     │
     │ 推荐实施顺序                                                            │
     │                                                                         │
     │ Phase 0（前提，应先于 V2 主体）                                         │
     │                                                                         │
     │ E1. executor.rs reconcile dust cancel 修复                              │
     │ - 文件：src/polymarket/executor.rs                                      │
     │ - 位置：reconcile_open_orders ~436-439                                  │
     │ - 改动：cancel 失败时不从 remote_by_slot 删除 dust order                │
     │ - 约 5 行                                                               │
     │                                                                         │
     │ ---                                                                     │
     │ Phase 1（V2 核心，高价值低风险）                                        │
     │                                                                         │
     │ C1. RiskIncreasing downward republish                                   │
     │ - 文件：src/polymarket/coordinator_order_io.rs                          │
     │ - 位置：~410，RETAIN match 逻辑                                         │
     │ - 改动：RiskIncreasing 分支增加 downward >= 2 ticks 的 Republish 路径   │
     │ - 约 8 行                                                               │
     │                                                                         │
     │ C2. PairingOrReducing band 扩展为双向 3-tick                            │
     │ - 文件：src/polymarket/coordinator_order_io.rs                          │
     │ - 位置：~410，PairingOrReducing 分支                                    │
     │ - 改动：单向 upward 2-tick → 双向 |delta| > 3 ticks                     │
     │ - 约 5 行                                                               │
     │                                                                         │
     │ ---                                                                     │
     │ Phase 2（中等复杂度，独立可验证）                                       │
     │                                                                         │
     │ C3. action_price / strategic_target 显式分离                            │
     │ - 文件：src/polymarket/strategy/pair_arb.rs                             │
     │ - 位置：~186-201，role-aware maker clamp                                │
     │ - 确保 clamp 只在 place 动作时计算，不写回 strategic_target 字段        │
     │                                                                         │
     │ C4. admissible 剥离 utility/open_edge                                   │
     │ - 文件：src/polymarket/coordinator_execution.rs                         │
     │ - 位置：admissible 检查路径                                             │
     │ - 约 10-15 行，需要确认 utility_delta 移除后 skip_util 计数仍保留       │
     │                                                                         │
     │ ---                                                                     │
     │ Phase 3（最高风险，需验证 fill_recheck 替代链）                         │
     │                                                                         │
     │ C5. fragile 退出主报价路径                                              │
     │ - 先验证：fill → net_diff 变化 → state_key 变化 →                       │
     │ pair_arb_state_changed 是否覆盖所有 fragile 触发的场景                  │
     │ - 若覆盖完整：coordinator_metrics.rs 中 fragile 触发                    │
     │ fill_recheck_pending 的路径可以移除                                     │
     │ - 若不完整：需要补充 fill event 直接触发 state_key 重算的路径           │
     │                                                                         │
     │ C6. opposite-slot fuse 升级为策略层 SlotBusy 感知                       │
     │ - 文件：src/polymarket/coordinator_execution.rs 或 coordinator.rs       │
     │ - 新增：slot_persistently_busy_since[2]: Option<Instant>                │
     │ - 当 SlotBusy 持续 > 15s，对侧 risk-increasing 的 strategic_target →    │
     │ None                                                                    │
     │ - 替代现有 pair_arb_opposite_slot_blocked() 并移除旧逻辑                │
     │                                                                         │
     │ ---                                                                     │
     │ 验证步骤                                                                │
     │                                                                         │
     │ Phase 0 验证                                                            │
     │                                                                         │
     │ 1. cargo check                                                          │
     │ 2. dry-run：reconcile 不再循环发现同一 dust order                       │
     │                                                                         │
     │ Phase 1 验证                                                            │
     │                                                                         │
     │ 1. cargo test --lib pair_arb（目标：148 / 148 通过）                    │
     │ 2. 新增测试：                                                           │
     │   - test_risk_increasing_republishes_on_downward_2tick_drift            │
     │   - test_pairing_retains_within_3tick_band_both_directions              │
     │ 3. dry-run：log 中不再出现 stale price 下 RiskIncreasing 单长期挂账     │
     │                                                                         │
     │ Phase 3 验证                                                            │
     │                                                                         │
     │ 1. 回放 April 9 场景：Ghost Order → SlotBusy 持续 > 15s → 对侧停止      │
     │ risk-increasing                                                         │
     │ 2. 确认 net_diff 不再继续单侧累积                                       │
     │                                                                         │
     │ ---                                                                     │
     │ 与上轮计划（April 8）的关系                                             │
     │                                                                         │
     │ ┌──────────────────────────┬──────────────────────────────────────┐     │
     │ │       April 8 Bug        │                 状态                 │     │
     │ ├──────────────────────────┼──────────────────────────────────────┤     │
     │ │ Bug #1 skip_inv 误计     │ 独立于 V2，可在 Phase 1 之外单独修复 │     │
     │ ├──────────────────────────┼──────────────────────────────────────┤     │
     │ │ 观察 #3 pair_cost 超成本 │ 待确认，不阻塞 V2                    │     │
     │ └──────────────────────────┴──────────────────────────────────────┘   