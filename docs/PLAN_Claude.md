  实盘执行语义严格验证报告（2026-04-11）                                                                                                                             
                                                                                                                                                                     
  轮次基本情况                                                                                                                                                       
                                                                                                                                                                     
  日志实际记录了 4 个轮次（非之前 plan 分析的 3 个）：                                                                                                               
                                                                                                                                                                     
  ┌─────────┬─────────────┬───────────────────────────────────────────────────────┬──────────────┐                                                                   
  │  轮次   │ 时间（UTC） │                     FinalMetrics                      │   P&L 风险   │
  ├─────────┼─────────────┼───────────────────────────────────────────────────────┼──────────────┤                                                                   
  │ Round 1 │ 16:15–16:30 │ realized_pnl=+0.70, residual=5 NO, worst_case=-1.82   │ 中等残仓风险 │
  ├─────────┼─────────────┼───────────────────────────────────────────────────────┼──────────────┤
  │ Round 2 │ 16:30–16:45 │ realized_pnl=+0.35, residual=10 NO, worst_case=-4.45  │ 高危         │                                                                   
  ├─────────┼─────────────┼───────────────────────────────────────────────────────┼──────────────┤                                                                   
  │ Round 3 │ 16:45–17:00 │ realized_pnl=+0.62, residual=10 YES, worst_case=-2.31 │ 中等残仓风险 │                                                                   
  ├─────────┼─────────────┼───────────────────────────────────────────────────────┼──────────────┤                                                                   
  │ Round 4 │ 17:00–      │ 日志截断                                              │ 不完整       │
  └─────────┴─────────────┴───────────────────────────────────────────────────────┴──────────────┘                                                                   
                                                        
  注：之前 plan 中分析的 "Round 1 pair_cost=0.36" 等数据来自更早的日志文件，并非本次 April 11 日志。                                                                 
                                                        
  ---                                                                                                                                                                
  各设计语义验证结果                                    
                                                                                                                                                                     
  ---                                                   
  ✅ Fix A（stale-low 强制重发）— 已正确实现
                                                                                                                                                                     
  验证案例（line 4058）：
                                                                                                                                                                     
  16:27:05 — NO 3.18@0.330 fills → working net=-8.2     
  pair_arb_state_forced_republish:                                                                                                                                   
    slot=NO_BUY  prev: {Low, No dominant}  current: {Mid, No dominant}
    live=0.3300@5.00  fresh=0.0900@5.00                                                                                                                              
    → Cancel YES_BUY (reason=Reprice)  ← stale YES 被正确取消                                                                                                        
                                                                                                                                                                     
  当 NO partial fill 使 net_diff 从 Low → Mid bucket 时，YES slot 的 state_changed=true 触发了 pair_arb_state_forced_republish。由于 fresh=0.09 远低于 live=0.33，该 
  YES 单被正确取消重发。                                                                                                                                             
                                                                                                                                                                     
  第二案例（lines 16466-16475）：                                                                                                                                    
  
  16:37:29 — NO 0.38@0.330 completes → net=-10 (High)                                                                                                                
  Cancel YES@0.460 (stale-low → fresh=0.500, gap=4ticks > 3tick threshold)                                                                                           
  → PROVIDE Buy Yes@0.500  ← 正确升价重发                                                                                                                            
                                                                                                                                                                     
  Fix A 表现符合设计语义。✅                                                                                                                                         
                                                                                                                                                                     
  ---                                                                                                                                                                
  ⚠️  Fix B（High bucket 无条件禁止 RiskIncreasing）— 条件性，存在实际违规
                                                                                                                                                                     
  Round 1 违规（lines 4063-4079）：
                                                                                                                                                                     
  16:27:05 — NO 1.82@0.330 fills → working net=-10.0 (High bucket 入场)
  上次 NO fill：16:26:12（53秒前）< PAIR_ARB_STALLED_SECS=60s                                                                                                        
                                                                                                                                                                     
  inventory: working NO=10.0 net=-10.0 pending_no=5.00                                                                                                               
  → PROVIDE PostOnly Buy Yes@0.480   ← PairingOrReducing ✓                                                                                                           
  → PROVIDE PostOnly Buy No@0.080    ← ⚠️  RiskIncreasing at High bucket! VIOLATION!                                                                                  
                                                                                                                                                                     
  16:27:19 StrategyMetrics: net_diff=-10.00 pair_progress_regime=Healthy                                                                                             
  16:27:34 PairArbGate:   keep_rate=100.0%  skip(inv/sim/util/edge)=0/0/0/0                                                                                          
                                                                                                                                                                     
  Fix B 失效原因：NOcascade 在 53s 内完成（< 60s stall 阈值），此时 pair_progress_regime=Healthy，Fix B 的 pair_arb_should_block_stalled_risk_increasing 因 Healthy  
  条件不满足而跳过。结果 NO@0.080 和 YES@0.480 同时挂出。                                                                                                            
                                                                                                                                                                     
  Round 2 / Round 3 Fix B 工作（对比案例）：            

  Round 2（lines 16478-16489）— 上次 NO fill 在 16:35:38（>90s 前）：                                                                                                
  16:37:29 — net_diff=-10, pair_progress_regime=Stalled（立即触发）
  16:37:30 — PROVIDE Yes@0.460 ONLY（PairingOrReducing）                                                                                                             
             NO bid 未出现                                                                                                                                           
  16:38:04 PairArbGate: keep_rate=50.0%  skip_inv=3056/6112                                                                                                          
                                                                                                                                                                     
  Round 3（line 16713-16724）— 上次 fill 在 16:45:16（>60s 前）：                                                                                                    
  16:47:00 — YES 5@0.240 fills → net_diff=+10, Stalled 立即                                                                                                          
  16:47:01 — PROVIDE No@0.620 ONLY（PairingOrReducing）                                                                                                              
             YES bid 未出现                                                                                                                                          
                                                        
  结论：Fix B 依赖 Stalled≥60s 条件，只在 stall 计时器已满的情况下生效。当 cascade 填入发生在上次配对事件 60s 内时，Fix B 完全失效，RiskIncreasing 单会被挂出。      
                                                                                                                                                                     
  ---
  ✅ 90s 风险开窗（Issue 3）— 已正确实现                                                                                                                             
                                                                                                                                                                     
  16:28:30 UTC — pair_arb risk_open_cutoff_changed=true (t-90s)
  state_key 从 {Mid, risk_open_cutoff_active: false}                                                                                                                 
         → {Mid, risk_open_cutoff_active: true}                                                                                                                      
  → 触发 pair_arb_state_forced_republish（state key 变化）                                                                                                           
                                                                                                                                                                     
  该机制阻止了新的 RiskIncreasing 订单在 16:28:30 后开仓。✅                                                                                                         
                                                                                                                                                                     
  ---                                                                                                                                                                
  ✅ SoftClose / HardClose / Freeze — 时序正确          
                                                                                                                                                                     
  Round 1:
    16:29:15 SoftClose (t-45s) ✓                                                                                                                                     
    16:29:30 HardClose (t-30s) ✓                                                                                                                                     
    16:29:58 Freeze (t-2s)     ✓                                                                                                                                     
                                                                                                                                                                     
  Round 2:                                                                                                                                                           
    16:44:15 SoftClose ✓                                
    16:44:30 HardClose ✓                                                                                                                                             
    16:44:58 Freeze    ✓
                                                                                                                                                                     
  Round 3:                                              
    16:59:15 SoftClose ✓
    16:59:30 HardClose ✓
    16:59:58 Freeze    ✓                                                                                                                                             
  
  所有轮次均在正确时间点触发。✅                                                                                                                                     
                                                        
  ---                                                                                                                                                                
  🚨 Cross-reject 无步降（Issue 1）— 实际违规，效率损失 
                                                                                                                                                                     
  证据（lines 5240-5269，16:28:55 – 16:29:01）：
                                                                                                                                                                     
  16:28:55 YES@0.460 — ❌ crosses book (400)            
  16:28:56 YES@0.460 — ❌ crosses book (相同价格)                                                                                                                    
  16:28:57 YES@0.460 — ❌ crosses book (相同价格)                                                                                                                    
  16:28:58 YES@0.460 — ❌ crosses book (相同价格)                                                                                                                    
  16:29:00 YES@0.460 — ❌ crosses book (相同价格)                                                                                                                    
  16:29:01 YES@0.460 — ❌ crosses book (相同价格)       
                                                                                                                                                                     
  连续 6 次使用同一价格 0.460 提交，全部 cross-reject。在 Round 1 最后 30 秒的临界时间点（SoftClose 将在 16:29:15 触发），浪费了约 6 秒窗口期。                      
                                                                                                                                                                     
  extra_safety_ticks 机制存在，但 last_cross_rejected_action_price 步降未生效——每次重试前 fresh_target 被重新计算为 0.460，覆盖了步降。                              
                                                        
  ---                                                                                                                                                                
  ⚠️  TIER_EPS 边界（Fix E）— 存在但此次无实际影响       
                                                                                                                                                                     
  证据（lines 16283, 16372）：
                                                                                                                                                                     
  16:34:51 YES 4.89@0.400 fills（应为 5.00）            
    → working net_diff = 4.89（Low bucket，非 Mid）                                                                                                                  
  16:34:52 YES 0.11@0.400 fills（1.3s 后完成）                                                                                                                       
    → working net_diff = 5.00（Mid bucket）                                                                                                                          
                                                                                                                                                                     
  Round 3（lines 16670-16676）：                                                                                                                                     
                                                                                                                                                                     
  16:45:16.496 YES 1.74@0.420 fills（分次）             
    → net_diff = 1.74，Low bucket                                                                                                                                    
  16:45:16.510 YES 3.26@0.420 fills（14ms 后）                                                                                                                       
    → net_diff = 5.00，Mid bucket                                                                                                                                    
                                                                                                                                                                     
  在 14ms 窗口内，tier1 cap 未生效（net_diff=1.74 < 5.0）。但由于：                                                                                                  
  1. 窗口极短（14ms < 700ms debounce）                                                                                                                               
  2. 同一 fill 订单的下一档 fresh_target = 0.390（低于 tier1_cap = 0.294），价格已在 cap 以下                                                                        
  3. 即使有 fresh 报价触发，admissibility check 也会在 net_diff=5.0 时用 tier1 cap 重新过滤  
                                                                                                                                                                     
  此次日志中 Fix E 无实际 P&L 损失，但理论漏洞存在。                                                                                                                 
                                                                                                                                                                     
  ---                                                                                                                                                                
  🚨 pair_arb_state_forced_republish 日志风暴 — 新发现                                                                                                               
                                                                                                                                                                     
  证据（lines 4097-4124）：
                                                                                                                                                                     
  16:28:30.116 risk_open_cutoff_changed → state_key 变化
  16:28:30.116 ~ 16:28:31.115：同一 slot、同一 state_key 变化记录了 ~30 次 pair_arb_state_forced_republish                                                           
                                                                                                                                                                     
  每个 WS tick（约每 10-30ms）都触发一次评估日志。原因：state_changed=true 标志在 cancel 确认（16:28:30.422）前一直保持，导致每个 tick 都重新评估并记录。            
                                                                                                                                                                     
  实际影响：日志噪音严重，在关键窗口期可能淹没真实的操作日志。cancel 本身只触发一次，无重复下单，但诊断可读性极差。                                                  
                                                        
  ---                                                                                                                                                                
  设计语义符合性总结                                    
                                                                                                                                                                     
  ┌─────────────────────────────────┬─────────────────────────────┬────────┬──────────────────┐
  │              机制               │            状态             │ 严重度 │      发现行      │                                                                      
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤
  │ Fix A (stale-low 重发)          │ ✅ 符合                     │ —      │ 4058, 16466      │                                                                      
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤
  │ Fix B (High bucket 无条件)      │ ⚠️  有条件，Healthy 时失效   │ P0     │ 4072, 4079       │                                                                      
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤                                                                      
  │ 90s 风险开窗                    │ ✅ 符合                     │ —      │ 4096             │                                                                      
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤                                                                      
  │ SoftClose/HardClose/Freeze      │ ✅ 符合                     │ —      │ 5336, 5349, 5356 │
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤                                                                      
  │ Cross-reject 步降 (Issue 1)     │ ❌ 不工作                   │ P1     │ 5240-5269        │
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤                                                                      
  │ Fix E (TIER_EPS)                │ ⚠️  理论漏洞，本次无实际影响 │ P1     │ 16283, 16670     │
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤                                                                      
  │ Fix C (flat 冷却)               │ ❌ 未实现                   │ P0     │ 无               │
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤                                                                      
  │ Fix D (同侧步降)                │ ❌ 未实现                   │ P0     │ 无               │
  ├─────────────────────────────────┼─────────────────────────────┼────────┼──────────────────┤                                                                      
  │ state_forced_republish 日志风暴 │ ⚠️  日志噪音                 │ P2     │ 4097-4124        │
  └─────────────────────────────────┴─────────────────────────────┴────────┴──────────────────┘                                                                      
                                                        
  ---                                                                                                                                                                
  Fix B 根本性缺陷总结                                  
                      
  Fix B 当前行为完全依赖 last_pair_progress_at 的历史时长：
                                                                                                                                                                     
  - 上次配对 > 60s 前进入 High：Stalled 立即 → Fix B 工作 ✓（Round 2, Round 3）                                                                                      
  - 上次配对 < 60s 前进入 High：Healthy → Fix B 失效 → RiskIncreasing NO@0.080 挂出 ✗（Round 1, line 4072）                                                          
                                                                                                                                                                     
  Round 1 的 NO@0.080 挂出后最终在 16:28:55+ 区间是否填单影响了 P&L？                                                                                                
  - Round 1 FinalMetrics（16:30:00）: worst_case=-1.82，residual=5 NO                                                                                                
  - 该 NO@0.080 极低价未被填单（市场NO价约 0.40+），Round 1 结束时被 HardClose/Freeze 取消                                                                           
                                                                                          
  此次没有造成实际损失，但 Fix B 的条件性脆弱性已被实证。   