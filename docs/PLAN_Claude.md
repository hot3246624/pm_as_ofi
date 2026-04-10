重构后策略全面评估

  执行摘要

  重构整体方向正确，Phase 0（Executor 层）修复完整，策略层核心收敛大幅推进。存在
  两处设计层面的偏差需要明确，以及一个未覆盖的场景需要确认。

  ---
  一、Executor 层（Phase 0）：完整修复 ✅

  所有 April 9 Ghost Order 根因已修复：

  ┌────────────────────────────┬─────────┬──────────────────────────────────┐
  │           修复项           │ 实现状  │             关键代码             │
  │                            │   态    │                                  │
  ├────────────────────────────┼─────────┼──────────────────────────────────┤
  │ Dust Cancel 失败不从       │ ✅      │ cancel_remote_dust_orders_for_sl │
  │ remote_by_slot 删除        │ 已修复  │ ot() 只在 cancel 成功时          │
  │                            │         │ remote.remove(&id)               │
  ├────────────────────────────┼─────────┼──────────────────────────────────┤
  │ is_residual_dust 门控 fill │ ✅      │ DUST_REMAINING_SHARES =          │
  │  notification              │ 已实现  │ 0.10，handle_fill_notification   │
  │                            │         │ 双条件判断                       │
  ├────────────────────────────┼─────────┼──────────────────────────────────┤
  │ handle_cancel_slot         │ ✅      │ cancel 有一个失败 →              │
  │ 失败返回 SlotBusy          │ 已修复  │ SlotBusy，全部成功才发 CancelAck │
  ├────────────────────────────┼─────────┼──────────────────────────────────┤
  │ Slot Lock Recovery         │ ✅ 新增 │ 阻塞 8s 后强制 cancel，5s        │
  │                            │         │ 重试间隔                         │
  ├────────────────────────────┼─────────┼──────────────────────────────────┤
  │ ExecutionFeedback::SlotBlo │ ✅ 新增 │ 让 Coordinator                   │
  │ cked                       │         │ 感知持续阻塞，有频率限制防止风暴 │
  └────────────────────────────┴─────────┴──────────────────────────────────┘

  Ghost Order 场景在下次 reconcile 时可被 cancel_remote_dust_orders_for_slot
  清理，即使 cancel 失败也保留在 remote_by_slot
  中以供下轮重试。这个设计是正确的。

  ---
  二、策略层核心收敛：基本达成，有两处偏差

  2.1 PairArbStateKey 三字段结构 ✅

  PairArbStateKey { dominant_side, net_bucket, soft_close_active }

  与 PLAN_Codex V2 Section 2 完全一致。Bucket 边界
  Flat/Low(<5)/Mid(<10)/High(≥10) 正确。

  ---
  2.2 Band-driven 发布制度 ⚠️  实现与计划存在偏差

  PairingOrReducing（coordinator_order_io.rs ~404）：

  if delta_ticks > 0.0 + 1e-9 {
      (delta_ticks <= 2.0 + 1e-9, "small_pairing_up_drift")   // 上行: > 2 tick
  → Republish
  } else {
      (delta_ticks >= -3.0 - 1e-9, "small_pairing_down_drift") // 下行: < -3
  tick → Republish
  }

  实现：不对称双向带宽（上 2 tick / 下 3 tick）。PLAN_Codex V2 Section 3
  写的是对称 3-tick band。这个偏差看起来是故意的设计决策（配对腿更容易向上追、更
  难向下追），但需要你明确确认，因为文档和代码不一致。

  RiskIncreasing（coordinator_order_io.rs ~399）：

  Some(PairArbRiskEffect::RiskIncreasing) => (true, "same_side_state_driven"),
  // 无论 upward 还是 downward 均 retain

  同时 pair_arb_should_force_freshness_republish 对 RiskIncreasing 也返回
  false。

  实现选择：RiskIncreasing 完全由状态机驱动，不响应价格漂移。这与 PLAN_Codex V2
  Section 3 的说法矛盾：

  ▎ "downward：仅当 strategic_target <= live_target - 2 ticks 时 Republish"

  代码里有一个测试明确验证了这个行为：test_pair_arb_risk_increasing_retains_when
  _fresh_target_moves_down_two_ticks — 即 fresh_target 下移 2 tick 时
  RiskIncreasing 仍然 retain。这是有意为之的。

  这个设计选择的影响分析：

  场景: market 下行 20 tick，YES 单还挂着
  当前行为: 保留原价，直到状态 bucket 变
  风险评估: Post-only 单不会成交（市场离太远），实际无害
  ────────────────────────────────────────
  场景: market 上行后 state bucket 变（如 net_diff 穿 5），YES 单在旧低价
  当前行为: 状态触发重评，fresh target 重新计算
  风险评估: ✅ 覆盖
  ────────────────────────────────────────
  场景: market 小幅震荡，state 不变，YES 单偏离 < bucket 边界
  当前行为: 始终 retain
  风险评估: 可能挂着偏离市场的单无法成交，但也不会成交亏损

  结论：状态驱动策略对 RiskIncreasing 更为保守，可以接受。但需要在
  STRATEGY_PAIR_ARB_ZH.md 中明确写清这个选择，而不是沿用 V2 文档的"下行 2 tick
  Republish"说法。

  ---
  2.3 strategic_target vs action_price 分离 ✅ 基本实现

  pair_arb.rs:186-201 中：
  - RiskIncreasing：raw_yes/raw_no = min(raw_yes, ask - margin)（在计算时就
  clamp）
  - PairingOrReducing：保留 strategic target，post-only safety 在 order IO
  阶段应用

  设计意图已实现。但术语层面：代码注释用的是 "strategic_target" 和
  "action_price" 概念，STRATEGY_PAIR_ARB_ZH.md 仍用"fresh
  target"和"真实发单价"——文档未正式定义这两个术语，会造成阅读混淆。

  ---
  2.4 opposite-slot fuse 移除 ✅

  pair_arb_opposite_slot_blocked() 已标记
  #[allow(dead_code)]，PAIR_ARB_OPPOSITE_SLOT_BLOCK_MS / TTL_MS 常量也标记
  #[allow(dead_code)]，未在 slot_quote_allowed() 或主决策链中调用。

  同时新增了 ExecutionFeedback::SlotBlocked 作为观测替代。测试
  test_pair_arb_opposite_slot_blocked_does_not_gate_pair_arb_quotes
  验证了这一点。

  ---
  2.5 admissible 收缩为纯硬约束

  Agent 未明确报告 utility_delta / open_edge_improvement 是否从 retain
  路径移除。这是 V2 Section 5 的要求，需要直接确认：当前
  pair_arb_quote_still_admissible() 或等价路径里是否还有 utility_delta
  检查会影响 retain 决策？

  ---
  2.6 fragile / PairProgressState 状态

  fragile 仍然通过 rescind_with_fill_recheck() 设置 fill_recheck_pending =
  true，绕过 RETAIN 逻辑触发重评。这在 5
  个触发器（Matched/Failed/Merge/SoftClose/Round reset）框架内是合理的，属于
  fill 事件触发机制，不是第二套策略逻辑。但 PairProgressRegime::Stalled
  的路径是否还影响报价需要确认。

  ---
  三、测试覆盖评估

  ┌─────────────┬───────┬───────────────────────────────────────────────────┐
  │    场景     │ 覆盖  │                      测试名                       │
  │             │ 状态  │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ 双向小漂移  │       │ test_pair_arb_pairing_side_retains_small_*_drift_ │
  │ 不 churn    │ ✅    │ * (4个)                                           │
  │ (pairing)   │       │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ 5/10/0      │       │                                                   │
  │ bucket 切换 │ ✅    │ test_pair_arb_state_bucket_change_*               │
  │ 强制重评    │       │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ RiskIncreas │       │                                                   │
  │ ing 不追价  │ ✅    │ test_pair_arb_risk_increasing_retains_when_fresh_ │
  │ （含下行    │       │ target_moves_down_two_ticks                       │
  │ retain）    │       │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ pairing 腿  │       │ test_pair_arb_retention_republishes_pairing_side_ │
  │ 下行大漂移  │ ✅    │ on_large_downward_drift                           │
  │ Republish   │       │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ opposite-sl │       │ test_pair_arb_opposite_slot_blocked_does_not_gate │
  │ ot fuse 不  │ ✅    │ _pair_arb_quotes                                  │
  │ 再影响主链  │       │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ SoftClose   │ ✅    │ test_pair_arb_soft_close_deadband_* (多个)        │
  │ deadband    │       │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ Ghost Order │       │                                                   │
  │  / Slot     │ ⚠️     │ 无单元测试验证 PendingCancel → CancelSlot →       │
  │ Busy        │ 缺失  │ CancelAck 完整链路                                │
  │ 恢复流程    │       │                                                   │
  ├─────────────┼───────┼───────────────────────────────────────────────────┤
  │ Merge sync  │       │                                                   │
  │ 不衍生额外  │ ⚠️  弱 │ 有正向 accounting 测试，缺 negative case          │
  │ 策略模式    │       │                                                   │
  └─────────────┴───────┴───────────────────────────────────────────────────┘

  ---
  四、待确认事项（3 项）
                                                                                
  Q1（设计澄清）：RiskIncreasing 的 "state-driven only"
  策略是有意为之吗？如果是，应该更新 STRATEGY_PAIR_ARB_ZH.md                    
  将其明确写入，并删除 V2 文档中"下行 2 tick Republish"的说法。
                                                                                
  Q2（设计澄清）：PairingOrReducing 的不对称带宽（上 2 tick / 下 3              
  tick）是有意为之吗？还是应该统一为 V2 的对称 3 tick？
                                                                                
  Q3（代码确认）：utility_delta 和 open_edge_improvement 是否已从 retain        
  决策路径移除（V2 Section 5 要求）？
                                                                                
  ---                                                   
  五、遗留的已知问题
                                                                                
  ┌────────────────────────────────────────┬───────────┬────────────────────┐
  │                  问题                  │   来源    │        状态        │   
  ├────────────────────────────────────────┼───────────┼────────────────────┤
  │ skip_inv 误计 EndgameRiskGate（Bug     │ April 8   │ 未修复，仅统计误报 │   
  │ #1）                                   │ 分析      │                    │
  ├────────────────────────────────────────┼───────────┼────────────────────┤   
  │ PairProgressRegime::Stalled            │ April 9   │ 需确认             │
  │ 是否仍影响报价                         │ 分析      │                    │   
  ├────────────────────────────────────────┼───────────┼────────────────────┤
  │ 文档术语不一致（strategic_target /     │ V2        │ 未同步             │   
  │ action_price）                         │ Section 7 │                    │   
  └────────────────────────────────────────┴───────────┴────────────────────┘
                                                                                
  ---                                                   
  六、总结

  ┌────────────────────┬──────┬─────────────────────────────────────────────┐
  │        层级        │ 评分 │                    说明                     │
  ├────────────────────┼──────┼─────────────────────────────────────────────┤   
  │ Executor 层（Phase │ 9/10 │ Ghost order 根因修复完整，机制健壮          │
  │  0）               │      │                                             │   
  ├────────────────────┼──────┼─────────────────────────────────────────────┤   
  │ 策略层核心收敛     │ 8/10 │ 三字段状态机、band-driven 发布、fuse        │   
  │                    │      │ 移除均已完成                                │   
  ├────────────────────┼──────┼─────────────────────────────────────────────┤   
  │ 设计与文档一致性   │ 6/10 │ RiskIncreasing 和 PairingOrReducing 带宽与  │
  │                    │      │ V2 计划有偏差，需同步文档                   │   
  ├────────────────────┼──────┼─────────────────────────────────────────────┤
  │ 测试覆盖           │ 8/10 │ 策略场景覆盖完整，Executor 链路测试缺失     │   
  └────────────────────┴──────┴─────────────────────────────────────────────┘   
  
  请先确认 Q1/Q2 两个设计问题，然后我可以帮你同步文档或补充缺失的测试。