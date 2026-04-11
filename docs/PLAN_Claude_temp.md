  当前问题：增量 VWAP 公式把早期便宜成交的优势当成"预算"花掉，最终 pair_cost →  
  pair_target，零利润。                                 
                                                                                
  修复方案：新增 pair_cost_safety_margin 参数（推荐 0.02），让所有 ceiling      
  计算以 pair_target - safety_margin 为基准。早期低价成交 =                     
  利润锁定，而不是后续高价的许可证。                                            
                                                        
  改动点：
  - vwap_ceiling 函数签名加一个 safety_margin 参数（4个调用点同步）
  - StrategyConfig 新增字段 + 环境变量解析                                      
  - 约 20 行代码，改动面小但策略意义显著  
                                                                                
  这个优化列为 P1，在 Bug A/B 修复稳定后实施。