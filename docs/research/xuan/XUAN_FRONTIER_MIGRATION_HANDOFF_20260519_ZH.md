# Xuan frontier migration handoff

日期: 2026-05-19

## 收尾状态

- 最新已合入主线提交: `ab610ce6 Add xuan book depth touch consumer`
- GitHub `origin/main`: `ab610ce6`
- GitHub `origin/codex/xuan-frontier-depth-consumer`: `ab610ce6`
- 本地验证已完成:
  - `cargo fmt --check`
  - `cargo test dry_run_market_book_depth --lib`
  - `cargo test book_depth --bin polymarket_v2`
  - `cargo test recorder_market_modes_split_raw_and_structured_streams --lib`
  - `python3 -m unittest scripts.tests.test_analyze_pgt_shadow_events`
  - `python3 -m py_compile scripts/analyze_pgt_shadow_events.py scripts/tests/test_analyze_pgt_shadow_events.py`
  - `cargo build --bin polymarket_v2`

## 已落地代码能力

`ab610ce6` 合并了两部分:

1. `15b4917` 的 live recorder depth evidence:
   - top-of-book bid/ask size
   - best-level size delta/drop qty
   - event_time
   - market/side
   - source_sequence_id
   - recorder `market_md.jsonl`

2. xuan-frontier dry-run-only consumer:
   - `MarketDataMsg::BookDepthTick`
   - `FillSource::DryRunBookDepthTouch`
   - shared-ingress client/direct WS depth evidence 进入 dry-run touch channel
   - executor 只在同侧 BUY maker order 位于 best bid、`best_bid_drop_qty > 0`、且未 crossed ask 时生成 `book_depth_touch`
   - analyzer 输出 `touch(book/depth/trade/other)` 并汇总 depth coverage/drop qty

## 旧服务器状态

截至最后只读检查:

- 旧服务器 repo: `/srv/pm_as_ofi/repo`
- 旧服务器 HEAD: `15b4917`
- 旧服务器尚未包含 `book_depth_touch` consumer
- `pm-xuan-shadow.service`: active
- 未执行 pull/build/deploy/restart

因此旧服务器不是最终迁移基准；新服务器应从 GitHub `origin/main@ab610ce6` 或更新提交初始化。

## 新服务器迁移建议

新服务器应从干净源码开始:

1. clone/pull `origin/main`
2. 确认 HEAD 至少为 `ab610ce6`
3. 本地 build `polymarket_v2`
4. 只恢复 xuan-shadow dry-run 所需 env/profile
5. 不复制旧服务器 stale debug binary
6. 不把 `xuan_pair_ask_rescue_v1` 当主线策略，它只是 `AUXILIARY_PROBE_ONLY`

迁移后的第一轮验证:

- repo HEAD/binary 中能找到 `DryRunBookDepthTouch` / `BookDepthTick` / `book_depth_touch`
- recorder 继续写入 depth coverage/drop qty
- analyzer 能区分 `touch(book/depth/trade/other)`
- 若启用 maker-depth shadow profile，先跑 5-6 / 10 / 20 complete rounds smoke

## 策略状态

当前没有 deployable mainline champion。

- `xuan_pair_ask_rescue_v1`: 技术上干净，但双 taker 费后边际和参与率太薄，只能作为辅助探针。
- L1 depletion/maker-depth lane: `EVIDENCE_AVAILABLE + CONSUMER_PATCH_READY`，但需要迁移后 smoke 才能继续判断。
- 不应重复已经冻结的 public-flow/offset/simple B27 filter 方向，除非有新 feature source。

核心目标保持不变: 以低 pair cost、低 residual、真实手续费后正 PnL 为门槛，再用更高参与率和配对数放大收益。
