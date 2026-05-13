# Local Agg Diagnostic Tick / Wire Schema Plan

更新时间：2026-05-13 22:16Z

本文是研究设计，不是实现补丁。目标是给 BNB residual max 与后续 microstructure attribution 准备一个诊断专用的数据通道，同时保证现有 local-agg selector、gate、price 行为完全不读这些字段。

## 当前运行背景

22:16Z safety check：

- service：`pm-local-agg-challenger.service` active/running，`NRestarts=0`
- server HEAD：`08f6dc6`
- run id：`20260513_045906`
- accepted / gated：266 / 329
- accepted side errors：0
- accepted max / p95：6.893443bps / 3.663586bps
- latency p95 / max：49.75ms / 279.0ms

官方 accepted hard tails 仍是 side-matched DOGE/HYPE：

- DOGE 6.893443bps，`drop_binance`，`last_before`，`bybit;coinbase;okx`
- DOGE 5.368432bps，`drop_binance`，`last_before`，`okx`
- HYPE 5.349395bps，`drop_binance`，`after_then_before`，`hyperliquid;okx`

当前 deterministic HYPE+DOGE-only runtime proposal 仍然需要用户显式批准后才能实现/重启；本文不改变这个状态。

## 设计原则

1. 保留当前行为价格：`price` 继续表示现有 selector 使用的价格，不改语义。
2. 诊断字段全部 optional：缺失字段不得影响解析、replay、selector 或 gate。
3. 先并行记录，不替换老通道：第一阶段不要把核心 broadcast tuple 改成新 struct，以降低行为回归风险。
4. 默认关闭：使用显式 env flag，例如 `PM_LOCAL_AGG_MICROSTRUCTURE_DIAG=1`。
5. 只做 top-of-book/summary：不在 challenger 主日志里写全量 order book。

## 当前代码约束

`src/bin/polymarket_v2.rs` 当前核心通道是 price-tick only：

- `SharedIngressWireMsg::LocalPriceSnapshot/Tick`：`symbol, price, ts_ms, source`
- `LocalPriceHub::senders`：`broadcast::Sender<(f64, u64, LocalPriceSource)>`
- `LocalPriceHub::recent_ticks`：`VecDeque<(f64, u64, LocalPriceSource)>`
- Coinbase ticker parser 当前只返回 `(symbol, price, ts_ms)`
- boundary tape 当前只保存 `Vec<(u64, f64)>`

结论：microstructure enrichment 不能只是 parser-only change。需要一个兼容的新诊断结构，并且必须避免 selector 路径误用它。

## Proposed Structs

建议先增加诊断 struct，而不是替换现有 tuple。

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LocalPriceKind {
    Trade,
    Ticker,
    BookTicker,
    Mid,
    Mark,
    Index,
    AllMids,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LocalPriceMicrostructureDiag {
    bid: Option<f64>,
    ask: Option<f64>,
    bid_size: Option<f64>,
    ask_size: Option<f64>,
    mid: Option<f64>,
    microprice: Option<f64>,
    spread_bps: Option<f64>,
    last_size: Option<f64>,
    trade_side: Option<String>,
    trade_id: Option<String>,
    update_id: Option<String>,
    mark_price: Option<f64>,
    index_price: Option<f64>,
    exchange_ts_ms: Option<u64>,
    received_ts_ms: Option<u64>,
    source_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalPriceTickDiag {
    symbol: String,
    source: String,
    price: f64,
    ts_ms: u64,
    #[serde(default)]
    price_kind: Option<LocalPriceKind>,
    #[serde(default)]
    microstructure: Option<LocalPriceMicrostructureDiag>,
}
```

派生字段建议只在 parser/helper 内计算：

- `mid = (bid + ask) / 2`
- `microprice = (ask * bid_size + bid * ask_size) / (bid_size + ask_size)`
- `spread_bps = 10000 * (ask / bid - 1)`，要求 `bid > 0` 且 `ask >= bid`
- `source_latency_ms = received_ts_ms - exchange_ts_ms`

所有计算失败时返回 `None`，不使用 sentinel 值。

## Wire Compatibility Plan

### Phase 0: no behavior change baseline

保留：

- `publish_tick(symbol, price, ts_ms, source)`
- `subscribe() -> Receiver<(f64, u64, LocalPriceSource)>`
- `snapshot_recent_ticks() -> Vec<(f64, u64, LocalPriceSource)>`
- `LocalSourceBoundaryTapeState { Vec<(u64, f64)> }`

这保证现有 aggregation、HYPE selector、gate 与 replay helper 不需要迁移。

### Phase 1: add diagnostic side store

在 `LocalPriceHub` 增加独立、bounded 的 diagnostic cache，例如：

```rust
recent_diag_ticks: Arc<Mutex<HashMap<String, VecDeque<LocalPriceTickDiag>>>>,
```

新增 helper：

```rust
fn publish_diag_tick(&self, diag: LocalPriceTickDiag)
fn snapshot_recent_diag_ticks(&self, symbol: &str) -> Vec<LocalPriceTickDiag>
```

`publish_diag_tick` 可以同时调用 `publish_tick`，但 selector 仍只消费原有 `publish_tick` 路径。

### Phase 2: shared ingress optional fields

给 `SharedIngressWireMsg::LocalPriceSnapshot/Tick` 增加 optional 字段时必须使用 `#[serde(default)]`，保持旧消息兼容：

```rust
LocalPriceTick {
    symbol: String,
    price: f64,
    ts_ms: u64,
    source: String,
    #[serde(default)]
    price_kind: Option<LocalPriceKind>,
    #[serde(default)]
    microstructure: Option<LocalPriceMicrostructureDiag>,
}
```

接收端逻辑：

- 无 diag 字段：继续 `publish_tick`
- 有 diag 字段且 env flag 开启：`publish_diag_tick`
- 有 diag 字段但 env flag 关闭：只保留旧 `publish_tick`

### Phase 3: Coinbase ticker parser expansion

Coinbase 是最低风险第一步：

- 当前已经订阅 `ticker`
- 官方 ticker payload 已包含 best bid/ask 与 size
- 不需要新增连接或上游负载

解析新增字段时：

- behavior `price` 仍使用当前 ticker `price`
- optional diag fields 只写入 `LocalPriceMicrostructureDiag`
- 不改变 Coinbase source eligibility、price pick 或 gate

### Phase 4: boundary diagnostic tape

如果需要把 microstructure 对齐到 round boundary，新增并行结构，而不是替换 `LocalSourceBoundaryTapeState`：

```rust
struct LocalSourceBoundaryDiagTapeState {
    open_window_ticks: Vec<LocalPriceTickDiag>,
    close_window_ticks: Vec<LocalPriceTickDiag>,
}
```

同样保留 `LOCAL_PRICE_AGG_BOUNDARY_TAPE_MAX_TICKS_PER_SIDE` 限制，且默认不开启。

### Phase 5: Binance bookTicker diagnostic stream

Binance top-of-book 需要新订阅，建议单独排在 Coinbase 之后：

- 只订阅 watched local-agg symbols 的 `<symbol>@bookTicker`
- 只保存 best bid/ask、size、update id、received time
- 不订阅 full depth
- 先小样本观察 log volume 与 latency，再决定是否扩大

## Validation Commands

实现前无需运行这些命令；未来实现补丁后建议固定使用：

```bash
cargo check --bin polymarket_v2 --target-dir target/local_agg_check
python3 -m py_compile scripts/summarize_local_agg_runtime_gate.py
python3 -m py_compile scripts/research_local_agg_combined_source_regimes.py
```

如果新增 parser unit test，应覆盖：

- Coinbase ticker 只有 `price/time` 时仍能解析老路径
- Coinbase ticker 含 bid/ask/size 时 microstructure fields 正确填充
- bid/ask 缺失、非数值、ask < bid 时 derived fields 为 `None`
- shared ingress 老 JSON 不含 optional 字段时仍能反序列化

## Runtime Acceptance Criteria

任何未来诊断实现都必须满足：

- dry-run 仍开启，live trading 不开启
- 不重启 `pm-shared-ingress-broker.service`
- 不改变 7-market topology
- flag off 时 selector/gate 行为与当前版本一致
- flag on 时 latency p95 仍低于 300ms
- no accepted side errors
- 新字段只进入 diagnostics/replay，不进入 model selection

## Research Use

诊断字段主要服务 BNB residual max 与 source-quality attribution：

- BNB tail 是否发生在 trade price 偏离 mid/microprice 时？
- selected source 是否 wide-spread 或 book imbalance？
- Coinbase/Binance excluded source 的 BBO 是否更接近 RTDS？
- exchange timestamp 与 received timestamp 是否解释 source staleness？
- visible `(timestamp, price)` tape 无法修复的 4.984909bps 是否其实是 price-kind 问题？

只有这些问题有 matched replay evidence 后，才考虑 BNB selector/model 变更。

## Decision

当前状态：research plan only。

不建议把该诊断 schema 与 deterministic HYPE+DOGE-only runtime proposal 捆绑部署。前者是观测能力建设，后者是已验证的低复杂度 dry-run checkpoint；两者应分开审批、分开重启时钟、分开评估。
