# Local Agg Microstructure Schema Enrichment Proposal

更新时间：2026-05-13 20:46Z

本文是非行为变更提案。目标不是改变 aggregation、gate、source selection 或 live posture，而是补齐当前 local boundary 数据里缺失的微观结构字段，用于解释 BNB residual max 和后续 per-symbol source-regime 研究。

## 背景

当前 deterministic HYPE+DOGE-only 候选已经把固定 replay 的 current-run max 从 6.893443bps 压到 4.531900bps，all-run max 压到 4.984909bps，side_errors=0，BTC unchanged。

剩余 limiter 是 BNB historical max 4.984909bps。已有 visible boundary tape attribution 没有找到更好的 close price candidate，说明继续只在 `(timestamp, price)` tape 上做 lag/window 搜索，很可能无法解释这条尾部。

BNB 当前 run 也呈现偏高尾部：

- 20:46Z runtime BNB accepted n=9，max=3.895720bps，p95=3.878288bps。
- BNB has-Bybit debiased trigger 能改善 p95，但依赖 selector-history/debias 语义，且不能修复 20260511 的 4.984909bps historical max。

判断：BNB 下一步更像是数据可观测性问题，而不是继续加一个窄 lag 规则。

## 当前 Schema 缺口

`src/bin/polymarket_v2.rs` 里的 local boundary 诊断结构目前主要保留价格和时间：

- `LocalBoundaryTickProbe`：`ts_ms`、`offset_ms`、`price`。
- `LocalSourceBoundaryTapeState`：`open_window_ticks: Vec<(u64, f64)>`、`close_window_ticks: Vec<(u64, f64)>`。
- `LocalPriceAggSourcePointProbe`：source、open/close price、timestamp、exact 标记、pick kind。
- `LocalSourceLagCloseCandidate`：source、raw/adjusted price、timestamp、offset、kind。

这些字段足够做 source-lag/window replay，但不足以回答：

- 该 source 的 bid/ask 是否很宽？
- mid 与 microprice 是否不同？
- 单源 price 是 last、mid、index 还是 mark？
- BNB tail 是否发生在低深度、单边薄 book 或 stale quote 下？
- exchange timestamp 与本地 receive timestamp 是否有系统性 skew？
- 某个 source 被选中时，另一个 source 的 book quality 是否更好但只因 timestamp/eligibility 被排除？

## 建议新增字段

新增字段应全部是 optional diagnostic fields，不参与 runtime selection/gate，先只写入 local-agg lab/shadow 日志。

### Per-source quote/tick fields

- `instrument_type`: `spot` / `perp` / `index` / `mark` / `dex_pool`
- `price_kind`: `mid` / `last` / `index` / `mark` / `twap` / `unknown`
- `exchange_ts_ms`
- `received_ts_ms`
- `source_latency_ms`
- `bid`
- `ask`
- `mid`
- `microprice`
- `bid_size`
- `ask_size`
- `spread_bps`
- `depth_1bps`
- `depth_5bps`
- `depth_10bps`
- `rolling_volume_1s`
- `rolling_volume_5s`
- `mark_price`
- `index_price`

### Per-boundary derived fields

- `source_age_ms_at_boundary`
- `source_return_1s_bps`
- `source_return_3s_bps`
- `source_return_10s_bps`
- `source_vs_cross_median_bps`
- `source_vs_microprice_bps`
- `source_book_imbalance`
- `is_stale_by_exchange_ts`
- `is_wide_spread`
- `is_low_depth`

### Polymarket opportunity fields, kept separate

This is for the opportunity metric thread, not oracle proxy fitting:

- accepted signal time
- predicted side
- pre-RDTS `ask0`
- pre-RDTS bid/ask/depth
- ask disappearance/repricing time
- executable size at `ask0`

Do not use close_diff_bps as a profit proxy.

## Proposed Implementation Path

### Phase 1: Adapter audit

Audit existing source adapters and raw exchange payloads for Binance, Bybit, Coinbase, OKX, and Hyperliquid:

- Which sources already expose top-of-book bid/ask?
- Which expose bid/ask size?
- Which expose mark/index price?
- Which timestamps are exchange timestamps versus local receive timestamps?
- Are BNB and DOGE currently using spot, perp, or mixed source semantics?

Output should be a source capability table before code changes.

### Phase 2: Diagnostic-only structs

Add optional diagnostic structs in `src/bin/polymarket_v2.rs`, for example:

- `LocalSourceMicrostructureProbe`
- `LocalBoundaryMicrostructureTickProbe`
- optional `microstructure` field on source point or boundary tick probes

Rules:

- All fields `Option<T>` where a source cannot provide them.
- No selection/gate code reads these fields.
- Existing logs remain backward compatible.
- Feature is guarded by an env flag, e.g. `PM_LOCAL_AGG_MICROSTRUCTURE_DIAG=1`.

### Phase 3: Bounded capture

Keep the existing bounded tape discipline:

- Preserve max ticks per side.
- Avoid full-depth book dumps by default.
- Store top-of-book plus compact depth summaries only.
- Log payload size and per-round write latency.

Acceptance criteria:

- `pm-local-agg-challenger.service` remains dry-run.
- 7-market topology remains `bnb,btc,doge,eth,hype,sol,xrp`.
- latency p95 remains below 300ms.
- No accepted/gated logic changes.
- Logs contain microstructure fields for at least BNB Binance/Bybit/OKX rows.

### Phase 4: Offline BNB analysis

After enough diagnostic samples:

- Compare selected close price vs mid, microprice, mark, index where available.
- Bucket BNB errors by spread, book imbalance, depth, source age, exchange/receive skew, source set, and open/close target.
- Re-run BNB tail attribution with microstructure fields.
- Decide whether the 4.984909bps residual is better explained by:
  - bad price kind,
  - stale/freshness issue,
  - microprice vs mid,
  - source eligibility,
  - target time alignment,
  - or unavailable oracle-internal source.

## Safety Position

This proposal should not be bundled with deterministic HYPE/DOGE runtime implementation. It is a separate diagnostics-only capture improvement.

Implementation or deployment still needs explicit approval if it requires restarting `pm-local-agg-challenger.service`. It must not restart `pm-shared-ingress-broker.service`, must not enable live trading, and must not relax thresholds.

## Decision

Recommended next research action: keep deterministic HYPE+DOGE-only as the current runtime proposal, and pursue BNB via microstructure schema enrichment rather than another lag-only selector.

Current decision status: research proposal only; no runtime change.
