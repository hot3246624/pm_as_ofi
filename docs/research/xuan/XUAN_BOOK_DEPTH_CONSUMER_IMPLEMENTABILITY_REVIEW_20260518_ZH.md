# Xuan book-depth consumer implementability review

日期: 2026-05-18

结论: `BOOK_DEPTH_EVIDENCE_AVAILABLE_CONSUMER_PATCH_LOCAL_ONLY`

## 远端状态

- 远端 repo: `/srv/pm_as_ofi/repo`
- HEAD: `15b4917 Merge localagg recorder depth evidence`
- `pm-xuan-shadow.service`: active
- 当前 profile: recorder summary 显示仍为 `xuan_pair_ask_rescue_v1`
- 服务未切到新的 maker-depth profile

重启/子进程观察:

- `systemctl` active timestamp 仍显示 2026-05-18 00:04:39 UTC，说明外层 service loop 没重建 active timestamp。
- 但新 `target/debug/polymarket_v2` 子进程已在 2026-05-18 11:10 UTC 对齐新轮次运行。
- 这足以让新轮次消费新二进制；不需要为观察目的再次 restart。

## Recorder smoke

从 2026-05-18 11:10 UTC 对齐轮次 `1779102600` 起，新 recorder 已持续写入 depth/depletion evidence:

| round | book_l1 | depth | size | event_time | source_seq | bid_drop_ticks | ask_drop_ticks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1779102600 | 139701 | 139701 | 46825 | 139701 | 134677 | 2423 | 2422 |
| 1779102900 | 147325 | 147325 | 54711 | 147325 | 143139 | 3216 | 3216 |

两侧都有 side-tagged drop qty:

- `1779102600`: YES bid drop 41633.51, NO bid drop 19740.86, YES ask drop 19740.86, NO ask drop 41627.87
- `1779102900`: YES bid drop 25945.42, NO bid drop 18236.09, YES ask drop 18236.09, NO ask drop 25945.42

这说明 shared-ingress/localagg 侧已经提供了 live book-depth/depletion evidence，且进入 `market_md.jsonl`。

## Focused validation

远端合并后 focused tests:

- `cargo test book_depth --bin polymarket_v2`: PASS
- `cargo test recorder_market_modes_split_raw_and_structured_streams --lib`: PASS

仅有既有 warnings。

## 当前缺口

Depth evidence 现在到达 `SharedIngressWireMsg::MarketBookTick.depth` 并进入 recorder，但还没有进入 dry-run fill 语义。

当前 shared-ingress client path:

1. 收到 `SharedIngressWireMsg::MarketBookTick { ..., depth }`
2. 构造 `MarketDataMsg::BookTick { yes_bid, yes_ask, no_bid, no_ask, ts }`
3. 把无 depth 的 `BookTick` 发给:
   - `dry_run_touch_md_tx`
   - coordinator
   - GLFT/其他消费者
4. 仅在 recorder 写 `record_market_book_l1(..., depth.as_ref())`

当前 executor dry-run touch taxonomy:

- `book_touch`: crossed-book / ask touch 证据
- `trade_sell_touch`: public SELL trade touch 证据
- `dry_run_taker`: 主动 taker 纸面成交

还没有:

- `book_depth_touch`
- side-specific depth/depletion event entering executor
- source_sequence_id/event_time 透传到 fill diagnostics
- analyzer 对 `book_depth_touch` 与旧 `book_touch` 的分离统计

因此，不能把 recorder 里的 drop qty 直接解释成当前 shadow 的 maker fill。现在只能说 evidence channel 已可观测，不能说 maker-depth profile 已可部署。

## 本地消费侧 patch 状态

本地分支 `codex/xuan-frontier-depth-consumer` 已实现最小 dry-run-only consumer patch，但尚未部署/重启:

- `MarketDataMsg::BookDepthTick` 只进入 dry-run touch channel，不进入 coordinator/OFI/GLFT 策略准入。
- `FillSource::DryRunBookDepthTouch` 与旧 `book_touch` / `trade_sell_touch` 分离。
- shared-ingress client 和 direct WS path 会把 `MarketBookTick.depth` / `book_depth_tracker.annotate(...)` 结果转成 depth tick。
- executor 仅在 live BUY maker order 位于同侧 best bid、`best_bid_drop_qty > 0`、且未 crossed ask 时，按 `min(remaining, best_bid_drop_qty * fraction)` 产生 `book_depth_touch` dry-run fill。
- analyzer 输出 `touch(book/depth/trade/other)`，并汇总 `market_md.jsonl` 的 depth coverage/drop qty。

本地验证:

- `cargo fmt --check`: PASS
- `cargo test dry_run_market_book_depth --lib`: PASS
- `cargo test book_depth --bin polymarket_v2`: PASS
- `cargo test recorder_market_modes_split_raw_and_structured_streams --lib`: PASS
- `python3 -m unittest scripts.tests.test_analyze_pgt_shadow_events`: PASS
- `python3 -m py_compile scripts/analyze_pgt_shadow_events.py scripts/tests/test_analyze_pgt_shadow_events.py`: PASS
- `cargo build --bin polymarket_v2`: PASS

注意: 这仍然不是策略 KEEP，也不是部署许可。下一步需要合并/部署后做 5-6 / 10 / 20 round smoke，确认 `book_depth_touch` 独立出现、source taxonomy 不混淆、residual/pair cost/fee-after PnL 通过门槛。

## 最小消费侧 patch 边界

建议新增一个 dry-run-only depth touch channel，而不是复用旧 `book_touch`。

代码边界:

1. `src/polymarket/messages.rs`
   - 新增 `MarketDataMsg::BookDepthTick`，或给 `BookTick` 增加 `depth: Option<MarketBookDepthEvidence>`。
   - 新增 `FillSource::DryRunBookDepthTouch`，`as_str()` 返回 `dry_run_book_depth_touch`。

2. `src/bin/polymarket_v2.rs`
   - shared-ingress client 收到 `MarketBookTick.depth` 后，把 depth-bearing msg 发给 `dry_run_touch_md_tx`。
   - direct market WS path 在 `book_depth_tracker.annotate(...)` 后也要把 depth-bearing msg 发给 dry-run touch。
   - recorder 继续保持现在的 `market_md.jsonl` 写法。

3. `src/polymarket/executor.rs`
   - 在 `process_dry_run_market_touch` 里新增 depth 分支。
   - 仅对 live BUY maker order 生效。
   - 必须要求:
     - `market_side == order.side`
     - `best_bid_drop_qty > 0`
     - `best_bid` 有效
     - order price 与 best bid 同价或非常接近；不要把低于 best bid 的订单算成交
     - order price 未 crossed ask；crossed ask 仍归旧 `book_touch`
     - fill size = `min(remaining, best_bid_drop_qty * fraction)`，并经过 material/min-fill gate
   - emit `dry_run_touch_fill_confirmed` 时 source 必须是 `book_depth_touch`，并带上:
     - `market_side`
     - `best_bid`
     - `best_bid_size`
     - `best_bid_drop_qty`
     - `event_time_ms`
     - `source_sequence_id`

4. `scripts/analyze_pgt_shadow_events.py`
   - 分离统计 `touch(book/depth/trade/other)`。
   - 汇总 `depth_evidence_ticks/size/event/seq/drop_qty`。
   - 明确 `book_touch` 和 `book_depth_touch` 不可混用。

5. Tests
   - parser/tracker tests 已有，应保留。
   - 新增 executor unit test:
     - same-side best-bid drop triggers `DryRunBookDepthTouch`
     - lower-than-best-bid order does not fill
     - crossed ask still maps to `DryRunBookTouch`
     - missing depth/source_sequence does not crash
   - 新增 analyzer unit test:
     - `book_depth_touch` 独立计数
     - depth coverage fields 从 `market_md.jsonl` 正确汇总

## 策略推进含义

L1 depletion lane 从 `INFRA_BLOCKED` 变成:

`EVIDENCE_AVAILABLE -> CONSUMER_PATCH_REQUIRED -> SHADOW_SMOKE_REQUIRED`

下一步不是调参，而是实现最小 depth consumer patch。只有当 `book_depth_touch` 在 shadow 中独立出现，并且 5-6 / 10 / 20 round smoke 证明:

- fill source 不混淆
- residual 没有回到单边失控
- pair cost 仍低
- fee-after PnL 有正 edge
- depth touch 与 recorder drop qty 对得上

才可以恢复 maker-depth profile 候选。
