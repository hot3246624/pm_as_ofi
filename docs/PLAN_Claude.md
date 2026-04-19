  升级到 7 币种 · 落地计划                                                                                                                                           
                                                                                                                                                                     
  目标宇宙：{btc, eth, sol, hype, doge, xrp, link}-updown-5m         
                                                                                                                                                                     
  0. 预研（写代码前必做，~1 天）                                                                                                                                     
                                                                                                                                                                     
  ┌──────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────┬────────────────────────────────┐   
  │        未知项        │                                               试探方法                                               │             决策点             │   
  ├──────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────┤   
  │ Chainlink RTDS       │ 写 ~50 行 src/bin/chainlink_probe.rs，分别测三种：a) 单连接发 7 个 subscribe frame；b) 一帧          │ 选出唯一能工作的模式，决定 Hub │   
  │ 是否接受多 symbol    │ filters:"{\"symbol\":\"HYPE/USD\"}" ×7；c) filters:"{\"symbol\":[\"HYPE/USD\",\"BTC/USD\"]}"         │  结构                          │   
  │ 订阅                 │                                                                                                      │                                │   
  ├──────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────┤   
  │ UserWs 多 market_id  │ 在 src/polymarket/user_ws.rs 找当前 filter 结构；尝试 subscribe payload 传数组                       │ 若不支持则需 per-symbol        │   
  │ 过滤                 │                                                                                                      │ UserWs（7 条）                 │   
  ├──────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────┤
  │ 并发 FAK 下单速率限  │ 本地写测试，单账户 1s 内 POST /order 40 次看 429                                                     │ 决定 Executor 内 semaphore     │   
  │                      │                                                                                                      │ 上限                           │   
  ├──────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────┤
  │ 每币 tick 率         │ probe 运行时统计 7 币 15 分钟 msg/s，看 peak                                                         │ 决定 broadcast channel buffer  │   
  │                      │                                                                                                      │ 大小                           │   
  └──────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────┘
                                                                                                                                                                     
  3 个预研都得清楚后才进 Stage 2a。                                                                                                                                  
  
  ---                                                                                                                                                                
  Stage 0 · 日志 slug 化（前置 · ~200 LoC · 1 天）         
                                                                                                                                                                     
  问题：并发写同一个 logs/polymarket.log / chainlink_round_alignment.jsonl / round_validation_*.jsonl 会串。虽然 writeln! 对单行 append
  是原子的，但人读事后日志如果不能按 slug 过滤就废了。                                                                                                               
                                                           
  改动：                                                                                                                                                             
  - src/bin/polymarket_v2.rs:5327 把每轮工作包进 tracing::info_span!("round", slug=%slug, round_id=%round_id)，所有子 info/warn 自动带 slug 字段。
  - 所有 info! 宏无需改写（tracing 继承 span 字段）。                                                                                                                
  - 结构化 JSONL 记录（probe、validation）已有 symbol 字段，抽查补齐。
  - logs/polymarket.log 维持单文件 daily rolling；按 slug 过滤靠 grep "slug=hype-updown-5m"。                                                                        
                                                                                                                                                                     
  验证：1 币种 dry-run log diff，字段多了 slug=…，其他格式不动。                                                                                                     
                                                                                                                                                                     
  跳过代价：Stage 2b 并发跑 7 币，日志完全串烂。                                                                                                                     
                                                                                                                                                                     
  ---                                                                                                                                                                
  Stage 1 · 抽出 run_round()（机械重构 · ~350 LoC · 1 天） 
                                                                                                                                                                     
  问题：当前 main() 是一个 ~800 行的 loop，per-round 状态全部 inline，无法被 JoinSet 并发调用。
                                                                                                                                                                     
  改动：                                                   
  - 新建 src/bin/polymarket_v2/market_worker.rs（或保持单文件加 fn）：                                                                                               
  pub struct RoundSlugInfo { pub slug: String, pub round_end_ts: u64, pub round_id: String, ... }                                                                    
  pub async fn run_round(                                                                        
      base_settings: &Settings,                                                                                                                                      
      signer_bundle: &SignerBundle,                        
      hub: &Arc<ChainlinkHub>,      // Stage 2a 之后才存在；先 stub                                                                                                  
      user_ws_router: &UserWsRouter, // Stage 2b 之后；先 stub                                                                                                       
      slug_info: RoundSlugInfo,                                                                                                                                      
  ) -> RoundOutcome                                                                                                                                                  
  - main() 调用 run_round(base, &signer, &hub_of_one, &user_router_of_one, info).await。                                                                             
  - 所有 per-round 资源（channels, actors, JoinHandle）限定在这个函数 scope；函数返回即 drop。                                                                       
  - 行为零变更，单测继续过。                                                                                                                                         
                                                                                                                                                                     
  验证：1 币 dry-run 和 Stage 0 版本 JSONL/metrics byte-level 对齐（去掉时间戳）。                                                                                   
                                                                                                                                                                     
  跳过代价：Stage 2b 无法开工。                                                                                                                                      
                                                                                                                                                                     
  ---                                                                                                                                                                
  Stage 2a · ChainlinkHub actor（~400 LoC · 2 天）         
                                                                                                                                                                     
  问题：14 条 Chainlink WS (7 币 × 2 任务) 早晚被限速。
                                                                                                                                                                     
  改动：                                                   
  - 新建 src/polymarket/chainlink_hub.rs：                                                                                                                           
  pub struct ChainlinkHub { /* 1 WS, reconnect loop, symbol registry */ }
  impl ChainlinkHub {                                                    
      pub async fn spawn(symbols: Vec<String>) -> Arc<Self>;                                                                                                         
      pub fn subscribe(&self, symbol: &str) -> broadcast::Receiver<(f64, u64)>;
      pub fn peek_prewarmed_tick(&self, symbol: &str, ts_ms: u64) -> Option<(f64, u64)>;                                                                             
      pub fn last_close(&self, symbol: &str) -> Option<(u64, f64)>;                                                                                                  
  }                                                                                                                                                                  
  - Hub 内部维护：HashMap<symbol, broadcast::Sender>、CHAINLINK_PREWARMED_OPEN（搬进来）、CHAINLINK_LAST_CLOSE（搬进来）。预研决定的订阅形态（a/b/c 三选一）。       
  - 重连策略：指数退避 300ms→2s；重连后自动重新订阅全部 symbol；重连期间消费方 recv() 会 Lagged，策略侧要兼容。                                                      
  - 安全带：保留 per-round prewarm 作为冗余，只在 [round_start-5s, round_start+5s] 区间活着，命中后立即 drop WS。覆盖 Hub 单点故障场景。                             
  - run_chainlink_winner_hint 从直开 WS 改为 hub.subscribe(symbol) 拿 Receiver + hub.peek_prewarmed_tick 查兜底。                                                    
                                                                                                                                                                     
  验证：                                                                                                                                                             
  1. 1 币 dry-run：Hub-of-1，对比 Stage 1 的 round alignment jsonl 无退化。                                                                                          
  2. 2 币 dry-run：HYPE + BTC，两轮内都拿到准确 open/close tick。                                                                                                    
  3. Kill Hub 连接（tcpkill/网络切断 5s），看 Hub 自愈 + 冗余 prewarm 救场。
                                                                                                                                                                     
  跳过代价：14 条 WS 硬上；一次 server-side rate-limit 全挂 7 币。                                                                                                   
                                                                                                                                                                     
  ---                                                                                                                                                                
  Stage 2b · 7 并发 MarketWorker + CapitalAllocator（~600 LoC · 3 天）                                                                                               
                                                                                                                                                                     
  改动清单：
  1. main() 用 tokio::task::JoinSet 起 7 个 market_worker_task(slug)，每个 task 自己 loop 调 run_round()。                                                           
  2. CapitalAllocator（新 actor）：                                                                                                                                  
  pub struct CapitalAllocator { pool_usdc: f64, per_symbol_reserved: HashMap<String, f64> }
  // API: reserve(slug, amount) -> Result<(), Denied>; release(slug, amount)                                                                                         
  // 请求通道: mpsc::Sender<AllocRequest>                                                                                                                            
  2. 每个 MarketWorker 在 run_round 开头 reserve = bid_size × FAK_MAX_SHOTS × 1.2（5 × 3 × 1.2 = 18 USDC/币），round 结束 release。                                  
  v1 策略：简单先到先得；预留超限时返回 Denied → 该币本轮跳过。                                                                                                      
  3. UserWsRouter：一条 UserWs 连接，filter 扩成 set<market_id>（预研结果如果支持则 1 条；不支持就 7 条）。收到 fill 后按 market_id 路由到对应 worker 的             
  inv_event_tx。                                                                                                                                                     
  4. Executor 内加 Semaphore::new(10)，包住 REST post_order。防 21 单并发 burst。                                                                                    
  5. Nonce/Signer：当前 signer 是全局共享 CPU-bound；加 Arc<Signer>，EIP-712 签名方法内 &self（应该已是）。如实测 signing p99>2ms/单，改为预签池。                   
  6. Book WS：每币仍独立（wss://ws-subscriptions-clob.polymarket.com/ws，filter asset_id 对）。7 条 Book WS 成本可接受；收敛到单条 WS 属于 Stage 3+ 的事。           
  7. Gamma resolve_market 调用：7 个 worker 同时轮询会把 Gamma 打挂。加共享 GammaResolver（Arc<Mutex<LruCache>>）+ 请求去重。                                        
                                                                                                                                                                     
  配置：.env 新增：                                                                                                                                                  
  PM_MULTI_MARKET_SLUGS="btc-updown-5m,eth-updown-5m,sol-updown-5m,hype-updown-5m,doge-updown-5m,xrp-updown-5m,link-updown-5m"                                       
  PM_CAPITAL_POOL_USDC=150    # 7 × 18 + 缓冲                                                                                                                        
  PM_EXECUTOR_CONCURRENT_SUBMITS=10                                                                                                                                  
  保持向后兼容：如果只设置 POLYMARKET_MARKET_SLUG（旧单币），走 1-worker 路径。                                                                                      
                                                                                                                                                                     
  验证（渐进灰度）：                                                                                                                                                 
                                                                                                                                                                     
  ┌──────┬───────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────┐                                        
  │ 步骤 │     币数      │                                             关注指标                                             │
  ├──────┼───────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤                                        
  │ a    │ 1 (hype)      │ Stage 2a 等价，无回归                                                                            │
  ├──────┼───────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ b    │ 2 (hype+btc)  │ 两币 round alignment jsonl 正确；FAK 方向正确                                                    │                                        
  ├──────┼───────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤                                        
  │ c    │ 4 (+eth+sol)  │ CPU < 30%；Chainlink hub 无 lag；无 429                                                          │                                        
  ├──────┼───────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤                                        
  │ d    │ 7             │ Round end 1.5s 窗口内所有 FAK/maker 按时发出；capital allocator 无 starvation；log slug 字段齐全 │
  ├──────┼───────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────┤                                        
  │ e    │ 7 live canary │ 1 轮实盘，金额 0.5× 平时，看实际下单和预期是否一致                                               │
  └──────┴───────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────┘                                        
                                                           
  跳过代价：这就是"上 7 币"本身。                                                                                                                                    
                                                           
  ---                                                                                                                                                                
  里程碑时间表（纯重构部分）                               
                                                                                                                                                                     
  ┌───────┬──────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────┐
  │ 周次  │               交付               │                                      风险                                      │                                      
  ├───────┼──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────┤
  │ W1    │ 预研脚本 + 决策文档              │ Chainlink 多 symbol 订阅不支持 → Hub 退化到 per-symbol WS，但仍保持 actor 边界 │
  ├───────┼──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────┤
  │ W1    │ Stage 0（slug span）             │ 低                                                                             │                                      
  ├───────┼──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────┤                                      
  │ W1–W2 │ Stage 1（run_round 抽出）        │ 低                                                                             │                                      
  ├───────┼──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────┤                                      
  │ W2–W3 │ Stage 2a（Hub）                  │ 中（重连 edge cases）                                                          │
  ├───────┼──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────┤                                      
  │ W3–W4 │ Stage 2b（7 worker + Allocator） │ 中（burst rate limit）                                                         │
  ├───────┼──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────┤                                      
  │ W4    │ 渐进灰度 a→e                     │ 高（首次多币实盘）                                                             │
  └───────┴──────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────┘                                      
                                                           
  可逆性：每个 Stage 后旧单币路径仍能跑（通过配置回落）。Stage 2b 之前任何时候可以停。                                                                               
                                                           
  ---                                                                                                                                                                
  关键保留 / 不改动                                        
                                                                                                                                                                     
  - 策略逻辑：coordinator.rs 的 pricing / A-S skew / OFI kill / FAK decision 全部不动。
  - 安全带：per-round prewarm 仍在（Hub 冗余）。                                                                                                                     
  - FAK 上限：保留当前 per-round 3 shot cap，7 币 = 21 shot/round。                                                                                                  
  - Endgame soft-close / OFI kill：per-symbol 独立，本质上它已经是 coordinator 状态。