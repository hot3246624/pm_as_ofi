# Bug Fix & Optimization Report

**日期**: 2026-03-09
**版本**: Post-Audit Patch v1
**审计范围**: 全部 Rust 源码（coordinator.rs / order_manager.rs / strategy.rs / inventory.rs / executor.rs / ofi.rs / user_ws.rs / polymarket_v2.rs）
**测试结果**: 30/30 单元测试通过，编译零警告

---

## 🔴 Critical Bug 修复（直接影响资金安全）

---

### Bug 1：对冲单立即自我取消

**文件**: `src/polymarket/coordinator.rs`
**函数**: `state_unified()`

**根本原因**：

当库存偏斜（`net_diff > 0`）时，策略调用 `place_or_reprice(Side::No, ...)` 挂出对冲单，随后用 `bid_no = 0.0` 标记"已消费"。但底部的统一清理代码逻辑为：

```rust
// 修复前（有 bug）：
if bid_no > 0.0 {
    place_or_reprice(...)
} else if self.no_target.is_some() {  // ← 刚刚被对冲路径设为 Some！
    self.cancel(Side::No, ...)         // ← 立即取消了刚挂的对冲单
}
```

结果：每次 tick，对冲单先被 place，再被 cancel，**对冲永远无法成功执行**。有库存时程序处于死锁状态——既不能对冲，又继续持仓累积风险。

**修复方案**：引入 `hedge_dispatched_yes` / `hedge_dispatched_no` 布尔标志。对冲路径标记后，底部清理代码检查该标志并跳过取消操作。

```rust
// 修复后：
let mut hedge_dispatched_no = false;

if agg_no > 0.0 {
    self.place_or_reprice(Side::No, bid_no, BidReason::Hedge, ...).await;
    hedge_dispatched_no = true;  // ← 标记已通过对冲路径处理
    bid_no = 0.0;
}

// 底部清理：
} else if !hedge_dispatched_no && self.no_target.is_some() {  // ← 加了保护
    self.cancel(Side::No, CancelReason::InventoryLimit).await;
}
```

**影响级别**: 🔴 Critical — 修复前有库存时对冲功能完全失效，是最严重的资金安全漏洞。

---

### Bug 2：OrderManager 完全忽略 cooldown_ms

**文件**: `src/polymarket/order_manager.rs`
**函数**: `handle_failed()`, `pump()`

**根本原因**：

Executor 在余额/授权不足时发送 `OrderResult::OrderFailed { cooldown_ms: 30_000 }`，但 `handle_failed` 函数签名中不存在该参数，导致 30 秒冷却时间被完全丢弃：

```rust
// 修复前（有 bug）：
Some(OrderResult::OrderFailed { side, .. }) => {  // ← ".." 丢弃了 cooldown_ms
    self.handle_failed(side).await;
}

async fn handle_failed(&mut self, side: Side) {
    tracker.state = OrderState::Idle;
    self.pump(side).await;  // ← 立即重试，导致密集 API 调用
}
```

**后果**：余额不足时每个 book tick 都触发一次 API 下单请求，数秒内就会打爆 rate limit，造成账户 429 封禁风险。

**修复方案**：在 `SideTracker` 增加 `cooldown_until: Option<Instant>`，`handle_failed` 接收 `cooldown_ms`，`pump()` 在冷却期内直接返回。

```rust
// 修复后：
pub struct SideTracker {
    // ...
    pub cooldown_until: Option<Instant>,  // 新字段
}

async fn handle_failed(&mut self, side: Side, cooldown_ms: u64) {
    tracker.state = OrderState::Idle;
    if cooldown_ms > 0 {
        tracker.cooldown_until = Some(Instant::now() + Duration::from_millis(cooldown_ms));
        warn!("⏳ OMS: {:?} 冷却 {}s", side, cooldown_ms / 1000);
    }
    self.pump(side).await;
}

async fn pump(&mut self, side: Side) {
    // 冷却期检查（新增）
    if let Some(until) = tracker.cooldown_until {
        if Instant::now() < until { return; }
        tracker.cooldown_until = None;
    }
    // ...
}
```

**影响级别**: 🔴 Critical — 修复前余额不足会触发密集重试，可导致 API 封禁。

---

### Bug 3：calc_kelly_qty 对 NO 侧使用 YES 盘口

**文件**: `src/polymarket/strategy.rs`
**函数**: `calc_kelly_qty()`

**根本原因**：

```rust
// 修复前（有 bug）：
fn calc_kelly_qty(&self, price: f64, book: &OrderBook) -> f64 {
    let mid_price = (book.yes_bid + book.yes_ask) / 2.0;  // ← 始终用 YES 盘口
    let edge = (mid_price - price).abs() / mid_price;
    // ...
}
```

计算 NO 侧订单的 Kelly 数量时，`mid_price` 使用的是 YES 侧的 bid/ask 均值。当 YES 价格 = 0.45，NO 价格 = 0.52 时，对 NO 侧计算 edge = |0.45 - 0.50| / 0.45 ≈ 11%，严重高估，导致 NO 侧下单量虚高。

**修复方案**：增加 `side` 参数，按侧选择正确盘口，并加入 `ask <= bid` 的防御检查：

```rust
// 修复后：
fn calc_kelly_qty(&self, side: Side, price: f64, book: &OrderBook) -> f64 {
    let (bid, ask) = match side {
        Side::Yes => (book.yes_bid, book.yes_ask),
        Side::No  => (book.no_bid, book.no_ask),
    };
    if bid <= 0.0 || ask <= 0.0 || ask <= bid {
        return self.cfg.qty_per_level;  // 盘口异常时退化为默认数量
    }
    let mid_price = (bid + ask) / 2.0;
    // ...
}
```

**影响级别**: 🔴 Critical — 修复前 NO 侧定量计算使用错误的价格参考。

---

## 🟠 High Risk 修复（严重影响策略执行）

---

### Issue 4：BookAssembler 跨 WS 重连不重置

**文件**: `src/bin/polymarket_v2.rs`
**函数**: `run_market_ws()`

**根本原因**：`BookAssembler` 在 reconnect 循环**外**声明，WS 重连后保留旧连接的盘口数据。重连后第一条 YES 更新会与旧 NO 数据合并发出 BookTick，产生混合新旧数据的错误信号。

**修复**：将 `book_asm` 移入 reconnect 循环内，每次重连重置。

```rust
// 修复后：
loop {
    let mut book_asm = BookAssembler::default();  // ← 每次重连重置
    // ...
}
```

**影响级别**: 🟠 High — 修复前重连瞬间可能产生错误定价信号。

---

### Issue 5：last_valid_ts 双侧共享，无法检测单侧过期

**文件**: `src/polymarket/coordinator.rs`
**函数**: `update_book()`, `is_book_stale()`

**根本原因**：YES 和 NO 共用一个 `last_valid_ts`。若 YES 每秒更新但 NO 5 分钟没有数据，`is_book_stale()` 不会触发，程序继续用陈旧 NO 数据定价和对冲。

**修复**：拆分为 `last_valid_ts_yes` 和 `last_valid_ts_no`，`is_book_stale()` 改为任一侧超时即判定过期。

```rust
// 修复后：
fn is_book_stale(&self) -> bool {
    let limit = Duration::from_secs(30);
    self.last_valid_ts_yes.elapsed() > limit || self.last_valid_ts_no.elapsed() > limit
}
```

**影响级别**: 🟠 High — 修复前单侧盘口可能在无感知情况下长期过期。

---

### Issue 6：CancelSide 无论取消是否成功都发 CancelAck

**文件**: `src/polymarket/executor.rs`
**函数**: `handle_cancel_side()`

**根本原因**：原代码无论 `handle_cancel_order` 是否返回 `false`，总是无条件发送 `CancelAck`。

**修复**：统计失败次数，记录明确警告日志。`open_orders` 中保留失败订单作为"安全网"（阻止 Executor 替换下单），CancelAck 仍然发送以防 OMS 状态机死锁。

```rust
// 修复后：
let mut failed_count = 0usize;
for id in &order_ids {
    if !self.handle_cancel_order(id, reason).await { failed_count += 1; }
}
if failed_count > 0 {
    warn!("⚠️ CancelSide {:?}: {}/{} 取消失败，open_orders 保留（下单保护激活）", ...);
}
let _ = self.result_tx.send(OrderResult::CancelAck { side }).await;
```

**影响级别**: 🟠 High — 修复后失败的取消操作有明确可见性，不再静默忽略。

---

### Issue 7：can_buy_yes/no 初次买入时 value_ok 恒为真

**文件**: `src/polymarket/inventory.rs`
**函数**: `can_buy_yes()`, `can_buy_no()`

**根本原因**：

```rust
// 修复前（有 bug）：
let projected_yes_value = (self.state.yes_qty + self.cfg.bid_size) * self.state.yes_avg_cost;
// 当 yes_qty=0, yes_avg_cost=0 时：projected_value = bid_size * 0 = 0 → 永远通过
```

首次买入时 `avg_cost = 0`，`projected_value` 恒为 0，`max_position_value` 限制形同虚设。

**修复**：无持仓时使用保守的最坏情况价格 `1.0`（二元期权最大价值）：

```rust
// 修复后：
let price_est = if self.state.yes_avg_cost > f64::EPSILON {
    self.state.yes_avg_cost
} else {
    1.0  // 保守估价：首次买入按最高可能价格计算敞口
};
let projected_yes_value = (self.state.yes_qty + self.cfg.bid_size) * price_est;
```

**影响级别**: 🟠 High — 修复前 `max_position_value` 对首次买入完全无效。

---

## 🟡 Medium Risk 修复（性能与可靠性）

---

### Issue 9：OFI SideWindow 无容量上限

**文件**: `src/polymarket/ofi.rs`
**函数**: `SideWindow::push()`

**修复**：增加 `MAX_WINDOW_TICKS = 4096` 硬上限，超出时淘汰最旧 tick：

```rust
const MAX_WINDOW_TICKS: usize = 4096;

fn push(&mut self, ...) {
    if self.ticks.len() >= MAX_WINDOW_TICKS {
        self.ticks.pop_front();  // O(1) 淘汰
    }
    self.ticks.push_back(...);
}
```

容量评估：4096 tick @ 3 秒窗口 = 约 1300 trade/s 才触发，远超 Polymarket 实际流量。

---

### Issue 10：DedupCache O(n) 淘汰改为 O(1)

**文件**: `src/polymarket/user_ws.rs`
**结构**: `DedupCache`

**根本原因**：原代码 `evict_oldest_if_needed()` 每次调用 `HashMap::iter().min_by_key()`，在 max_entries=50000 时是 O(50000) 操作，每次插入都触发。

**修复**：加入 `insertion_order: VecDeque<String>` 追踪插入顺序，O(1) pop_front 即可淘汰最旧条目：

```rust
struct DedupCache {
    seen_at: HashMap<String, Instant>,
    order: VecDeque<String>,   // ← 新增：O(1) LRU 队列
    ttl: Duration,
    max_entries: usize,
}

fn evict_oldest_if_needed(&mut self) {
    while self.seen_at.len() > self.max_entries {
        if let Some(k) = self.order.pop_front() {  // O(1)
            self.seen_at.remove(&k);
        }
    }
}
```

`evict_expired` 也改为从队列前端线性扫描（利用时间单调性），比随机 HashMap retain 更高效。

---

### Issue 11：parse_price_value 允许 100 以内的异常价格

**文件**: `src/bin/polymarket_v2.rs`
**函数**: `parse_price_value()`, `parse_price_str()`

**修复**：将范围从 `(0.0, 100.0)` 收窄为 `(0.0, 1.0)`，杜绝百分制格式价格污染：

```rust
// 修复后：
.filter(|p| *p > 0.0 && *p < 1.0)  // Polymarket 价格严格在 (0,1) 内
```

---

## 📊 修改文件清单

| 文件 | 改动类型 | 涉及 Bug/Issue |
|------|----------|----------------|
| `coordinator.rs` | Bug 修复 + 架构改进 | Bug 1, Issue 5 |
| `order_manager.rs` | Bug 修复 | Bug 2 |
| `strategy.rs` | Bug 修复 + 清理 | Bug 3 |
| `polymarket_v2.rs` | Bug 修复 | Issue 4, Issue 11 |
| `inventory.rs` | 风控加固 | Issue 7 |
| `executor.rs` | 可观测性改进 | Issue 6 |
| `ofi.rs` | 性能保护 | Issue 9 |
| `user_ws.rs` | 性能优化 | Issue 10 |
| `docs/STRATEGY_V2_CORE_ZH.md` | 文档更新 | 安全机制清单同步 |

---

## ✅ 测试验证

```
cargo test
running 30 tests

test polymarket::coordinator::tests::test_balanced_mid_pricing ... ok
test polymarket::coordinator::tests::test_balanced_excess_mid_capped ... ok
test polymarket::coordinator::tests::test_debounce_skips_rapid_reprice ... ok
test polymarket::coordinator::tests::test_empty_book_skipped ... ok
test polymarket::coordinator::tests::test_global_kill_blocks_new_orders ... ok
test polymarket::coordinator::tests::test_global_kill_cancels_both_sides ... ok
test polymarket::coordinator::tests::test_safe_price_clamps_negative ... ok
test polymarket::coordinator::tests::test_safe_price_clamps_over_one ... ok
test polymarket::coordinator::tests::test_safe_price_normal ... ok
test polymarket::coordinator::tests::test_aggressive_ceiling_wins ... ok
test polymarket::coordinator::tests::test_aggressive_ask_wins ... ok
test polymarket::inventory::tests::test_confirmed_after_matched_is_noop ... ok
test polymarket::inventory::tests::test_confirmed_first_records_fill ... ok
test polymarket::inventory::tests::test_failed_fill_reversal ... ok
test polymarket::inventory::tests::test_inventory_constraint ... ok
test polymarket::inventory::tests::test_pair_fill ... ok
test polymarket::inventory::tests::test_single_side_fill ... ok
test polymarket::inventory::tests::test_vwap_averaging ... ok
test polymarket::ofi::tests::test_empty_windows ... ok
test polymarket::ofi::tests::test_independent_toxicity ... ok
test polymarket::ofi::tests::test_per_side_tracking ... ok
test polymarket::ofi::tests::test_sell_pressure_toxic ... ok
test polymarket::ofi::tests::test_toxicity_timeout_recovery ... ok
test polymarket::ofi::tests::test_window_eviction_per_side ... ok
test polymarket::user_ws::tests::test_dedup_cache_blocks_replay ... ok
test polymarket::user_ws::tests::test_taker_dedup_does_not_merge_distinct_partial_fills_without_trade_id ... ok
[polymarket_v2 binary tests]
test tests::test_should_skip_entry_window ... ok
test tests::test_last_trade_price_missing_side_parsing ... ok
test tests::test_ping_task_cleanup_pattern ... ok
test tests::test_executor_timeout_abort_pattern ... ok

test result: ok. 30 passed; 0 failed
```

---

## ✅ 第二轮优化（全部已实现）

---

### Opt 1：A-S 时间衰减项

**文件**: `src/polymarket/coordinator.rs`

**实现**：新增 `market_end_ts: Option<u64>` 和 `as_time_decay_k: f64`（默认 2.0）配置项，以及 `market_start: Instant` 字段。

```rust
fn compute_time_decay_factor(&self) -> f64 {
    // 1.0 at open → (1 + k) at close
    let elapsed_frac = (elapsed / total).clamp(0.0, 1.0);
    1.0 + k * elapsed_frac
}
// state_unified():
let effective_skew_factor = self.cfg.as_skew_factor * self.compute_time_decay_factor();
let skew_shift = skew * effective_skew_factor;
```

- `polymarket_v2.rs` 每轮自动设置 `coord_cfg.market_end_ts = Some(effective_end_ts)`
- 默认 k=2.0：开盘时 1× skew，临期时 3× skew，加速库存消化
- 环境变量 `PM_AS_TIME_DECAY_K` 可覆盖

---

### Opt 2：OFI 自适应阈值

**文件**: `src/polymarket/ofi.rs`

**实现**：新增 5 个配置项（`adaptive_threshold`, `adaptive_k`, `adaptive_min`, `adaptive_max`, `adaptive_window`），`OfiEngine` 维护滚动 `score_history: VecDeque<f64>`。

```rust
fn update_adaptive_threshold(&mut self, yes_score: f64, no_score: f64) {
    // 记录每次 heartbeat 的最大绝对 OFI 值
    let mean = history.mean();
    let std_dev = history.std_dev();
    self.effective_threshold = (mean + k * std_dev).clamp(adaptive_min, adaptive_max);
}
```

- 默认关闭（`adaptive_threshold=false`），兼容原固定阈值行为
- 启用后自动适配市场流动性变化（厚市场阈值自动上调，避免误杀）
- 环境变量 `PM_OFI_ADAPTIVE=true` 启用

---

### Opt 3：Hedge 防抖绕过

**文件**: `src/polymarket/coordinator.rs`

**实现**：新增 `hedge_debounce_ms: u64`（默认 100ms），`place_or_reprice()` 根据 `BidReason` 选择防抖时间。

```rust
let debounce_ms = match reason {
    BidReason::Hedge => self.cfg.hedge_debounce_ms,   // 100ms — urgent
    BidReason::Provide => self.cfg.debounce_ms,        // 500ms — normal
};
```

- 对冲是紧急清仓操作，不应被 500ms 正常防抖拦截
- 环境变量 `PM_HEDGE_DEBOUNCE_MS` 可覆盖

---

### Opt 4：OFI Kill Switch 直通路径

**文件**: `src/polymarket/ofi.rs`, `src/polymarket/coordinator.rs`, `src/bin/polymarket_v2.rs`

**实现**：在 OFI Engine→Coordinator 之间建立 `mpsc` 直通信道，coordinator 使用 `biased select!` 优先处理。

```rust
// OFI Engine（边沿触发，毒性首次出现时发送）:
if let Some(ref tx) = self.kill_tx {
    let _ = tx.try_send(KillSwitchSignal { side, ofi_score, ts });
}

// Coordinator run loop:
tokio::select! {
    biased;
    Some(sig) = self.kill_rx.recv() => {
        // 立即触发 tick() 无需等待下一个 BookTick
        self.tick().await;
    }
    msg = self.md_rx.recv() => { ... }
}
```

- 毒性检测到取消命令的延迟从「下一个 book tick 到达时」降至「OFI heartbeat（200ms）触发时」
- 使用 `Some(sig) = recv()` pattern，当信道关闭时优雅降级（arm 自动禁用），不影响 md_rx 正常处理

---

## 📊 第二轮优化修改文件清单

| 文件 | 改动类型 | 涉及 Opt |
|------|----------|----------|
| `coordinator.rs` | 功能扩展 | Opt 1, Opt 3, Opt 4, Emergency Hedge |
| `ofi.rs` | 功能扩展 | Opt 2, Opt 4 |
| `polymarket_v2.rs` | 配置注入 | Opt 1, Opt 4 |

---

### Opt 5：紧急对冲价格天花板 (Emergency Hedge Ceiling)

**文件**: `src/polymarket/coordinator.rs`

**实现**：新增 `max_portfolio_cost` 到 `CoordinatorConfig`。在 `state_unified()` 中判断库存状态。

```rust
// state_unified():
let hedge_target = if net_diff.abs() >= self.cfg.max_net_diff {
    self.cfg.max_portfolio_cost // 救火模式：允许微亏平掉单边敞口
} else {
    self.cfg.pair_target        // 盈利模式：死守利润线
};
let ceiling = hedge_target - avg_cost;
```

- 解决了"跳空后无法平仓"的僵局，确保即便不赚钱也要先关掉风险。
- `PM_MAX_PORTFOLIO_COST` 默认 1.02，允许 2% 的亏损缓冲区。

## ✅ 第二轮测试验证

```
cargo test
running 30 tests

... (all coordinator, inventory, ofi, user_ws, polymarket_v2 tests)

test result: ok. 30 passed; 0 failed
```
