  结论验证结果                                                      
                                                                                
  你的结论大体正确，但有一处机制细节需要更精确的定位，以及一个未提及的结构性发现
  。                                                                            
                                                                                
  ---                                                                           
  一、完整验证    
                                                                                
  结论 1：merge/pending 正确 ✓
                                                                                
  第一轮（行 243-244）、第二轮（行 1191）均确认：merge 前已将 pending 促进为    
  settled，merge 后 pending=0/fragile=false，残仓是真实库存，不是 phantom。     
                                                                                
  结论 2：配对后重新开成大残仓 ✓（且比你描述的更清晰）                          
  
  第二轮时间线精确还原：                                                        
                  
  12:53:19  merge sync → settled net=0.0, pending=0, fragile=false              
  12:53:33  residual_qty=0.01, net_diff=0.01（几乎完全平仓）                    
            realized_pair_locked_pnl=1.9320                                     
                                                                                
  12:53:34  YES@0.310 新单挂出  ← 系统自动再开仓                                
  12:54:02  YES 5@0.310 成交                                                    
  12:54:03  YES@0.210 挂出（基于新状态）                                        
  12:54:21  YES 2.59@0.210 + 2.40@0.210 成交                                    
                                                                                
  12:59:49  residual_qty=10, worst_case=-2.6033                                 
                                                                                
  从 net_diff≈0 到重新累积 10 单 YES 仓位，只用了不到 7 分钟。                  
                  
  结论 3：balance/allowance 真实存在 ✓                                          
                  
  行 222-234 确认两次连续的 balance 拒绝（YES 和 NO 均被拒），且拒绝原因是"sum  
  of matched orders = 11000000"——是未释放的已成交资金占用造成的。
                                                                                
  结论 4：OFI/churn/utility-edge 不是主凶 ✓                                     
  
  两轮 toxic_events=0 / kill_events=0 / skip_util=0 /                           
  skip_edge=0，这三条路径均未干扰结果。
                                                                                
  ---             
  二、cross-reject storm 的精确根因（补充你的分析）
                                                   
  你的结论说"执行层有 cross-book
  问题"是对的，但现在可以更精确地说明为什么它持续了 6 次：                      
  
  观测（行 1275-1326）：YES@0.430 被原样重发 6 次，价格从未改变：               
                  
  12:52:54 → YES@0.430 → cross reject                                           
  12:52:55 → YES@0.430 → cross reject                                           
  12:52:57 → YES@0.430 → cross reject
  12:52:59 → YES@0.430 → cross reject                                           
  12:53:01 → YES@0.430 → cross reject                                           
  12:53:02 → YES@0.430 → cross reject
                                                                                
  根因：extra_safety_ticks 机制（每次 reject +1 tick margin）本应让 action_price
   逐步下降，但它失效了。                                                       
                                                                                
  失效条件：action_price = min(strategic_target, best_ask - margin)。如果       
  coordinator 的 self.book.yes_ask 显示的是 ~0.475（而实际 exchange ask 已是
  0.430），那么：                                                               
                  
  - extra_ticks=3：cap = 0.475 - 0.035 = 0.440 > strategic_target(0.430)        
  - 故 action_price = 0.430，始终由 strategic_target 决定，margin 不生效
                                                                                
  真正需要的修复：不是"用 fresh book"（coordinator                              
  认为自己在用），而是基于拒绝价格本身步降：                                    
                                                                                
  ▎ 当 YES@P 被 cross-reject 后，下一次 action_price 必须 ≤ P - 1tick，无论 book
  ▎  怎么说
                                                                                
  即：if recent_cross_reject: action_price = min(action_price,                  
  last_rejected_price - tick_size)
                                                                                
  这条逻辑比"fresh book"更可靠，因为它不依赖 WS 延迟状态。                      
  
  ---                                                                           
  三、最核心的结构问题（你的判断完全正确）
                                                                                
  经济性验证（对两轮数据的精确核算）：
                                                                                
  ┌──────────────────────────┬─────────┬─────────┐
  │                          │ Round 1 │ Round 2 │                              
  ├──────────────────────────┼─────────┼─────────┤
  │ realized_pair_locked_pnl │ +2.273  │ +1.932  │
  ├──────────────────────────┼─────────┼─────────┤
  │ 残仓 worst_case          │ -2.818  │ -2.603  │                              
  ├──────────────────────────┼─────────┼─────────┤                              
  │ 净期望（假设 YES 输）    │ -0.545  │ -0.671  │                              
  └──────────────────────────┴─────────┴─────────┘                              
                  
  在 max_net_diff=15, bid_size=5 下，每次单侧填满的暴露量是 3 轮 × 5 = 15       
  份。以第二轮为例：
                                                                                
  YES avg_cost = 0.2602（10份）
  如果市场收 0：每份损失 = 0.2602 USDC                                          
  总损失 = 10 × 0.2602 = 2.60 USDC                                              
  已锁利润 = 1.93 USDC                                                          
  净损失 = 0.67 USDC                                                            
                                                                                
  这个结构下，一次"系统在已配对后重新开出 10 份单腿"就能吃掉整轮收益并超出。    
  
  为什么系统在 12:53:34 重新开仓？                                              
                  
  状态分析：                                                                    
  - net_diff = 0.01（dust，理论上 flat）
  - net_bucket = Low（0 < 0.01 < 5）                                            
  - YES 侧为 RiskIncreasing（买 YES 会使 net_diff 从 0.01 → 5.01）
  - no_qty ≈ 0 → VWAP ceiling for YES 不生效（no_qty <= EPSILON 跳过）          
  - YES_ask 大约在 0.31-0.32 范围 → A-S 定价：bid ≈ 0.31                        
                                                                                
  没有任何机制阻止这个新开仓。策略不知道自己刚刚完成了一轮配对、刚锁住了 1.93   
  USDC。                                                                        
                                                                                
  ---                                                                           
  四、关于参数经济性的补充判断
                                                                                
  你的结论"在 pair_target=0.97 / max_net_diff=15
  下策略经济性危险"是正确的，但可以更精确：                                     
                  
  每次 pair 赚多少 vs 允许的单腿风险是多少：                                    
                  
  单次配对利润 = (1.0 - pair_cost) × bid_size ≈ (1.0 - 0.93) × 5 = 0.35         
  USDC（每对）                                                                  
  
  max_net_diff=15 时允许的最大单腿暴露：                                        
  如果以 avg_cost=0.26 买入 10 份 YES
  worst_case = 10 × 0.26 = 2.6 USDC（全损）                                     
                                                                                
  需要多少对子才能覆盖：2.6 / 0.35 ≈ 7.4 对子                                   
  但每轮最多只配 ~6 对（bid_size=5, max_net_diff=15 → max pairs/round ≈ 6×5=30  
  份, locked=~2 USDC）                                                          
                  
  即使整轮全部配完，单次不好的残仓就能吃掉整轮利润，这说明参数组合下 risk/reward
   是不利的。     
                                                                                
  ---             
  五、总结：认可程度与一处精确化
                                                                                
  ┌────────────────────────┬────────┬──────────────────────────────────────┐
  │          结论          │  认可  │                 备注                 │    
  ├────────────────────────┼────────┼──────────────────────────────────────┤
  │ merge/pending          │ ✅ 完  │ 日志逐行确认                         │    
  │ 正确，不是幽灵仓       │ 全认可 │                                      │
  ├────────────────────────┼────────┼──────────────────────────────────────┤    
  │ 配对后重新开成大残仓是 │ ✅ 完  │ 第二轮时间线极清晰                   │    
  │ 主凶                   │ 全认可 │                                      │    
  ├────────────────────────┼────────┼──────────────────────────────────────┤    
  │ balance/allowance      │ ✅ 完  │ 行 222-237                           │
  │ 是真实执行伤害         │ 全认可 │                                      │    
  ├────────────────────────┼────────┼──────────────────────────────────────┤
  │ cross-reject storm     │ ✅ 完  │ 但根因更精确：WS book stale 导致     │    
  │ 是真实执行伤害         │ 全认可 │ extra_safety_ticks 失效，修复要用"基 │
  │                        │        │ 于拒绝价格步降"而非"fresh book"      │
  ├────────────────────────┼────────┼──────────────────────────────────────┤
  │ 不是                   │ ✅ 完  │ shutdown 数据明确                    │
  │ OFI/churn/utility-edge │ 全认可 │                                      │    
  ├────────────────────────┼────────┼──────────────────────────────────────┤
  │ 策略经济性不成立       │ ✅ 完  │ 两轮数据都支持，risk/reward 倒置     │    
  │                        │ 全认可 │                                      │    
  └────────────────────────┴────────┴──────────────────────────────────────┘
                                                                                
  最重要的一句话：策略的核心矛盾是——它没有"已完成本轮配对目标后停止再开仓"的语义
  。在市场临近结束时，任何新的 RiskIncreasing
  开仓都是用有限的剩余时间赌一个无上限的方向性风险。