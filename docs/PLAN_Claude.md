 Cross-Market Best-Ask Arbiter for Oracle-Lag Sniping   

 Context

 当前7个市场的 coordinator 完全独立：Chainlink hint 到达时，每个 coordinator 各自用固定 bid_size 同时发单。资金利用率低、无法集中在最优机会上。

 用户要求：每轮只在一个市场发单（最低 ask 的胜方），用全部可用 USDC，规则：
 - winner ask < 0.99 → FAK at 0.99 limit（主动吃单）
 - winner ask ≥ 0.99 → GTC at 0.99（挂单等成交）
 - winner ask = 0   → GTC at min(bid+tick, 0.991)（挂单高于最优买价）
 其余6个市场本轮跳过。

 数据依据

 - 831轮中位时延 1,376ms；7市场均值相差 ≤ 24ms → 全部 hint 在 300ms 内到齐
 - FAK触发频率：HYPE 60.5%，其余40-49%；ask几乎非0即0.99，ask<0.99 的轮次极少（每币约 1-9轮）
 - 资金收益优先级：ask 越低 → 以越低价格买入胜方 token → 期望利润越高

 ---
 架构变化

 新增一个 run_cross_market_hint_arbiter 任务，插在 hint listener 和 coordinator 之间：

 ChainlinkHub
     ↓ (per-market)
 run_post_close_winner_hint_listener ×7
     ↓  ArbiterPacket（含 winner_ask_raw、winner_bid、per-coord hint_tx）
 run_cross_market_hint_arbiter（NEW）
     ↓  WinnerHint { selected: bool, use_max_size: bool }（每个 coord 都收到）
 coordinator handle_market_data ×7
     ↓
 selected=true  → FAK / GTC / maker（按 ask 规则）
 selected=false → 本轮直接跳过

 ---
 详细修改

 1. src/polymarket/messages.rs

 WinnerHint 枚举增加两个字段：
 WinnerHint {
     // ... existing fields ...
     /// Set by cross-market arbiter. false = skip this round.
     cross_market_selected: bool,
     /// When true, coordinator sizes order using all free USDC instead of cfg.bid_size.
     use_max_size: bool,
 }
 无 arbiter 时（单市场模式）两者默认 true / false，保持向后兼容。

 ---
 2. src/bin/polymarket_v2.rs

 新增 ArbiterPacket struct（文件内部，不需要 pub）：
 struct ArbiterPacket {
     round_end_ts: u64,
     slug: String,
     winner_ask_raw: f64,   // 0.0 = no ask
     winner_bid: f64,
     hint_tx: mpsc::Sender<MarketDataMsg>,
     hint_msg: MarketDataMsg,   // 原始 WinnerHint（selected/use_max_size 未设）
 }

 新增 run_cross_market_hint_arbiter（参考现有 run_post_close_winner_hint_listener 风格）：
 - 参数：arbiter_rx: mpsc::Receiver<ArbiterPacket>, collection_window_ms: u64（推荐 300）
 - 内部按 round_end_ts 分批，首包到达后等待 collection_window_ms
 - 评分规则：
   a. ask ∈ (0, 0.99) → score = ask（越低越好，负向排序）
   b. ask = 0.99 → score = 0.99
   c. ask ≥ 0.993 → score = ask
   d. ask = 0.0 → score = 2.0（兜底，最低优先级）
   - 同分时按 winner_bid 降序（spread 最小的优先）
 - 选出 rank=1 的市场：selected=true, use_max_size=true；其余：selected=false, use_max_size=false
 - 将修改后的 WinnerHint 发给各市场 hint_tx

 改造 hint listener 发送路径（约在 run_post_close_winner_hint_listener 末尾发送处）：
 - 若 oracle_lag_sniping 启用了 arbiter（cross_market_arbiter_tx: Option<mpsc::Sender<ArbiterPacket>>），则发 ArbiterPacket 到共享 arbiter channel
 - 否则发原始 WinnerHint（兜底，向后兼容）

 supervisor 启动时（run_inproc_supervisor，约 line 5003）：
 - 创建 let (arbiter_tx, arbiter_rx) = mpsc::channel::<ArbiterPacket>(64);
 - 启动一个 run_cross_market_hint_arbiter 任务
 - 每个市场的 run_post_close_winner_hint_listener 都拿到 arbiter_tx.clone()

 ---
 3. src/polymarket/coordinator_order_io.rs

 handle_market_data(WinnerHint { ..., cross_market_selected, use_max_size }) 修改：

 if !cross_market_selected {
     // 本轮被 arbiter 淘汰，设内部 suppress flag，直接返回
     self.cross_market_suppressed = true;
     return;
 }
 self.cross_market_suppressed = false;

 FAK/GTC 路径修改（替换当前 ORACLE_LAG_NO_TAKER_ABOVE_PRICE = 0.993 的判断）：

 let order_action = if winner_ask > 0.0 && winner_ask < 0.990 {
     OrderAction::ImmediateFak           // ask < 99c → taker FAK at 0.99 limit
 } else if winner_ask >= 0.990 {
     OrderAction::GtcAt(0.990)           // ask ≥ 99c → maker GTC at 0.99
 } else {
     OrderAction::DeferToComputeQuotes   // no ask → post_close_hype 处理
 };

 use_max_size 处理（在 FAK dispatch 和 GTC place 路径）：
 let size = if use_max_size {
     // 向 executor 查询 free USDC，除以 limit price，下取整到 0.01 lot
     let free = self.executor_free_usdc_cached();
     let price = match order_action { ... };
     (free / price * 100.0).floor() / 100.0
 } else {
     self.cfg.bid_size
 };
 executor_free_usdc_cached()：从已有的 PlacementObserver / balance cache 读取，无需新增网络调用。

 GTC at 0.990 路径（复用现有 slot_place_or_reprice，BidReason::Provide，reason=OracleLagSnipeMaker）。

 ---
 4. src/polymarket/coordinator.rs

 新增字段：
 cross_market_suppressed: bool,  // set by WinnerHint(selected=false); cleared each new round hint

 post_close_chainlink_winner() 增加判断：若 cross_market_suppressed 则返回 None（让 compute_quotes 也跳过）。

 ---
 5. src/polymarket/strategy/post_close_hype.rs

 - no-ask GTC ceiling 从 (1.0 - tick).max(tick) 改为 0.991_f64.min(1.0 - tick)（cap at 99.1c）
 - 函数开头加守卫：if coordinator.is_cross_market_suppressed() { return StrategyQuotes::default(); }

 ---
 新增配置参数（OracleLagSnipingStrategyConfig）

 cross_market_arbiter = true          # 启用跨市场仲裁（默认 true）
 arbiter_collection_window_ms = 300   # 等待所有 hint 到齐的时间窗口

 ---
 关键文件汇总

 ┌────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────┐
 │                    文件                    │                                改动类型                                │
 ├────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
 │ src/polymarket/messages.rs                 │ 新增2字段                                                              │
 ├────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
 │ src/bin/polymarket_v2.rs                   │ 新增 ArbiterPacket + run_cross_market_hint_arbiter，改造 hint 发送路径 │
 ├────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
 │ src/polymarket/coordinator_order_io.rs     │ 新增 suppress 逻辑，修改 FAK/GTC 阈值，use_max_size sizing             │
 ├────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
 │ src/polymarket/coordinator.rs              │ 新增 cross_market_suppressed 字段 + getter                             │
 ├────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
 │ src/polymarket/strategy/post_close_hype.rs │ cap at 0.991，suppression 守卫                                         │
 └────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────┘

 ---
 验证方式

 1. cargo build --release 无报错
 2. 启动 dry-run，观察每轮只有 1 个市场出现 oracle_lag_submit_latency 或 dry_taker_preview，其余 6 个出现 cross_market_skip | round_end_ts=... 日志
 3. 验证选中市场的 ask 确实是当轮最低的
 4. 验证 ask<0.99 → type=FAK，ask=0.99 → type=Limit（GTC），ask=0 → type=Limit price≤0.991
 5. 连续跑 5 轮确认行为稳定