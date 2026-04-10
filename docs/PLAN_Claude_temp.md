  核心问题：pairing/reducing 腿的 strategic_target 实际上在 fill 之间是常数
                                                                                
  VWAP ceiling 的关键性质：
                                                                                
  NO_BUY 的 strategic_target 上限 = pair_target - YES_avg_cost
                                                                                
  YES_avg_cost 只在 Matched fill 事件时更新。所以 fill 事件之间，VWAP ceiling   
  不变。
                                                                                
  当 |net_diff| > 5（Mid/High bucket，VWAP ceiling 起作用时）：                 
  
  YES@50 成交后：NO 的 ceiling = 0.97 - 0.50 = 0.47（常数，直到下一笔 fill）    
                                                                                
  这时候市场 NO 在 45-49 间震荡，A-S baseline 跟着动，但 strategic_target 始终被
   cap 在 0.47。strategic_target 并没有真正在 2/3 tick 内漂移——漂移的只是 A-S   
  baseline，而它被 ceiling 截断了。                                             
                                                        
  所以 2/3 tick band 追踪的是噪声，不是信号。

  ---
  你描述的异常场景分析
                                                                                
  YES@35 部分成交（成交了 3/5）
  → 触发 Matched → fill_recheck_pending = true → 正确重评一次                   
  → 之后 market YES 在 33-38 间震荡                                             
  → A-S baseline for NO 随之在某个范围内漂移                                    
  → 但 NO 的 VWAP ceiling = 0.97 - 0.35 = 0.62（不变）                          
  → 如果 baseline < ceiling，strategic_target 随 A-S 漂移 ±2-3 tick             
  → 当前实现：每次穿越带宽边界就 Republish                                      
  → 结果：市场震荡驱动了 NO 单的连续调价，不合理                                
                                                                                
  正确行为应该是：那次 Matched 事件触发了唯一一次重评，之后 NO                  
  单保持不动，直到下一个触发事件（再次 fill / bucket 变化 / SoftClose）。       
                                                                                
  ---                                                   
  对"部分成交多少才进入监控"的回答
                                                                                
  这个问题本身就说明了问题：不应该进入连续监控。
                                                                                
  每次 Matched 已经是一个离散触发点：                                           
  - 部分成交 3 份 → Matched → 重评一次 → 更新 NO 的 fresh_target → 放单         
  - 再次部分成交 1 份 → 再一次 Matched → 再次重评                               
  - 每笔 fill 都是一个精确的重评节点                    
                                                                                
  在两次 fill 之间，VWAP ceiling 不变，strategic_target 基本不变（仅 A-S        
  baseline 噪声），应该 retain。                                                
                                                                                
  ---                                                                           
  正确的 pairing/reducing 重评制度                      
                                  
  ┌─────────────────────────────┬──────────┬────────────────────────────────┐
  │           触发源            │ 是否正确 │            当前实现            │   
  ├─────────────────────────────┼──────────┼────────────────────────────────┤
  │ Matched fill 事件           │ ✅ 正确  │ ✅ 通过 fill_recheck_pending   │   
  │                             │          │ 实现                           │
  ├─────────────────────────────┼──────────┼────────────────────────────────┤   
  │ net_diff bucket             │ ✅ 正确  │ ✅ 通过 state_key 变化实现     │   
  │ 穿越（0/5/10）              │          │                                │   
  ├─────────────────────────────┼──────────┼────────────────────────────────┤   
  │ SoftClose 进入              │ ✅ 正确  │ ✅ 通过 endgame phase 变化实现 │
  ├─────────────────────────────┼──────────┼────────────────────────────────┤   
  │ Failed 回滚                 │ ✅ 正确  │ ✅ 通过 rollback 路径实现      │
  ├─────────────────────────────┼──────────┼────────────────────────────────┤   
  │ A-S baseline 振荡 2/3 tick  │ ❌ 噪声  │ ❌ 当前实现误加入了            │
  └─────────────────────────────┴──────────┴────────────────────────────────┘   
                                                        
  ---                                                                           
  建议的修改                                            
            
  coordinator_order_io.rs 约 404 行，移除 PairingOrReducing 的 band 监控：
                                                                                
  // 当前（有问题）
  Some(PairArbRiskEffect::PairingOrReducing) => {                               
      if delta_ticks > 0.0 + 1e-9 {                     
          (delta_ticks <= 2.0 + 1e-9, "small_pairing_up_drift")                 
      } else {                                                                  
          (delta_ticks >= -3.0 - 1e-9, "small_pairing_down_drift")              
      }                                                                         
  }                                                     
                                                                                
  // 修改后：pairing/reducing 在两个 state 触发之间始终 retain                  
  Some(PairArbRiskEffect::PairingOrReducing) => {
      (true, "pairing_holds_between_state_triggers")                            
  }                                                     

  同时，pair_arb_should_force_freshness_republish 中 PairingOrReducing          
  分支也改为：
                                                                                
  PairingOrReducing => (false, "pairing_holds_between_triggers", 0.0)
                                                                                
  这样 pairing/reducing 的重评完全由 5 个离散触发器驱动，RiskIncreasing         
  也是状态驱动——两侧都统一为"状态机驱动"语义，没有连续 band 追踪。              
                                                                                
  ---                                                   
  边界情况：如果 VWAP ceiling 不 binding
                                                                                
  当 |net_diff| 很小（Low bucket）时，A-S baseline < VWAP
  ceiling，strategic_target 跟市场走。但此时两侧都是                            
  RiskIncreasing（开仓期），pairing/reducing 概念不适用，不触发这个路径。
                                                                                
  当且仅当 |net_diff| > 5 进入 Mid/High bucket 时，pairing/reducing             
  角色才有意义，此时 VWAP ceiling 几乎必然 binding。所以上述设计是安全的。