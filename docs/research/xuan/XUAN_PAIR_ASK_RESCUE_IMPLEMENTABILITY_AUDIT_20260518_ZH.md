# Xuan Pair-Ask Rescue 实现审计

日期: 2026-05-18

结论: `LOCAL_PATCH_READY_FOR_REVIEW_NOT_DEPLOYED`

`strict_l1_pair_ask_opportunity_scan` 找到的是一个低残仓 rescue/opportunity 模块，不是主策略 backbone。它的核心条件是同一时刻可以买入 YES 和 NO，且 `yes_ask + no_ask <= cap`，因此 residual 应该天然为 0。当前 Rust PGT/shadow 路径不能安全直接表达这个模块，因为现有 taker-open 执行路径是单腿优先，会把本应成对成交的机会拆成一条 first leg，从而重新制造 residual。

## 最新研究输入

远端 artifact:

`/home/ubuntu/xuan_frontier_runs/xuan-frontier-remote-verifier-heartbeat/20260517T222853Z_strict_l1_pair_ask_opportunity_scan`

代表性 2026-05-02..13 结果:

- `cap980_gap3_clip100`: `fee100_pnl=+3993.70`, `pair_qty=42000`, `weighted_pair_cost=0.89595`, `active_rate=11.02%`, `worst_day_fee100=+43.59`
- `cap990_gap3_clip100`: `fee100_pnl=+3990.69`, `pair_qty=94700`, `weighted_pair_cost=0.94838`, `active_rate=22.71%`, `worst_day_fee100=+43.97`
- `cap980_gap3_clip10`: `fee100_pnl=+832.48`, `pair_qty=5290`, `weighted_pair_cost=0.83429`, `active_rate=13.72%`

解释: fee 后正、pair cost 很好、residual=0 by construction，但活跃率低。因此它适合作为 rescue/opportunity 模块，不适合作为全天候 backbone。

## 当前代码能表达什么

策略报价容器本身可以同时持有 YES/NO 两侧买单:

- `src/polymarket/strategy.rs`: `StrategyQuotes` 有 `yes_buy` 和 `no_buy`

dry-run executor 也可以模拟 PGT 的 BUY taker fill:

- `src/polymarket/coordinator_order_io.rs`: `pgt_shadow_taker_dryrun_execute_enabled()` 对 PGT dry-run 的 `Provide | Hedge` BUY 返回 true
- `src/polymarket/executor.rs`: dry-run taker 使用 `FillSource::DryRunTaker`

所以底层 dry-run taker fill 不是 blocker。

## 当前 blocker

### 1. PGT taker-open candidate 是单腿选择器

`src/polymarket/coordinator_execution.rs::pgt_shadow_taker_open_candidate()` 会分别计算 YES/NO candidate，但如果两边都可行，会按 slack/ask 只返回其中一个。

这对 tail-taker first-leg profile 是合理的，但对 pair-ask rescue 不合理。pair-ask rescue 的单位是一个 paired cycle，不是一条 first leg。

### 2. 同一 decision epoch 有单次 fired guard

`pgt_shadow_taker_open_candidate_for_side()` 和 `decide_provide_side_action()` 都会检查 `pgt_shadow_taker_open_fired_epoch == Some(pgt_decision_epoch)`。

`apply_provide_side_action(ShadowTaker)` 一旦发送第一腿，就把 `pgt_shadow_taker_open_fired_epoch` 设为当前 epoch。结果是第二腿会被同 epoch guard 挡掉。

### 3. active-tranche/residual lifecycle 会误解这个模块

现有 PGT lifecycle 假设:

1. 先有一条 open/seed leg
2. 然后 completion/hedge 补另一条腿
3. residual 是待修复状态

pair-ask rescue 的假设不同:

1. 同一 tick 同时买 YES/NO
2. 成交后立即形成 locked pair
3. 不应产生 active residual

如果沿用单腿 first-leg path，即便研究模块本身 residual=0，shadow 也会记录出 residual。

### 4. recorder/analyzer 命名会混淆

dry-run taker 当前会 emit `taker_repair_sent` 事件，`FillSource` 只有 `dry_run_taker`。这足够统计 fill source，但不够区分:

- tail repair
- settlement-alpha taker open
- pair-ask rescue paired open

如果不补诊断字段，后续分析容易把 paired rescue 误看成 repair 或残仓补腿。

### 5. live depth 缺失限制 clip

研究 scan 用 `min_buy_available_qty >= clip` 确认两侧 marketable depth。当前 live/shared-ingress `BookTick` 只有 top-of-book price，没有 ask size/depth。

因此在没有 book-depth evidence channel 前，paper-shadow 只能用 `yes_ask + no_ask <= cap` 加小 clip 做保守验证。`clip100` 不应直接部署为 shadow profile；优先 `clip10` 或 `clip20`。

## 最小可行 patch plan

### A. 新增 profile

新增 dry-run-only profile:

`xuan_pair_ask_rescue_v1`

建议初始 envelope:

- `yes_ask + no_ask <= 0.98` 起步；`0.99` 只作为后续扩展
- fixed clip `10`，最多再测试 `20`
- per-market gap cooldown `3s`
- 只在 flat 或无 material active residual 时触发
- 不使用 winner/residual direction
- 不依赖 B27 fills、future completion、book-depletion fill
- 不接入 settlement-alpha lifecycle

### B. 新增原子双腿 dispatch path

不要复用 `pgt_shadow_taker_open_candidate()` 的单腿返回值。

建议新增一个独立判断:

`pgt_pair_ask_rescue_candidate(quotes, book, inv) -> Option<PairAskRescueCandidate>`

候选必须同时包含:

- YES ask price
- NO ask price
- clip size
- pair cost
- cap
- reason/profile label

然后在 coordinator 执行层新增一个原子路径:

1. 同一 decision epoch 内先校验 YES/NO 都仍然可 taker BUY
2. 连续 dispatch 两条 taker intent
3. 只在两条 dispatch 都发出后设置 pair-rescue fired/cooldown
4. 不经过现有单腿 `pgt_shadow_taker_open_fired_epoch` guard
5. 不把第一腿写成需要 completion 的 active tranche 状态

如果短期不想新增 executor command，可以继续调用两次 `dispatch_taker_intent(... TradePurpose::Provide ...)`，但 coordinator 必须用新的 pair-rescue guard，而不是复用单腿 taker-open guard。

### C. 明确 recorder/analyzer 诊断

最少需要在 recorder/analyzer 中能看到:

- `pgt_shadow_profile=xuan_pair_ask_rescue_v1`
- pair ask cap
- observed `yes_ask`, `no_ask`, `pair_ask`
- clip
- pair-rescue sent count
- `dry_run_taker` fill count split by YES/NO
- paired qty
- residual qty/rate
- weighted pair cost
- fee50/fee100 locked or worst-case PnL

建议新增事件名或字段，避免只显示 `taker_repair_sent`。

### D. Focused tests

代码 patch 之前必须至少覆盖:

1. `pair_ask_rescue` profile parsing 和 dry-run-only gate
2. pair ask `<= cap` 时产生双腿 candidate，`> cap` 时不产生
3. 同一 decision epoch 可以 dispatch YES/NO 两条 dry-run taker
4. 单腿 `xuan_tail_taker_v1` / `xuan_high_pressure_v1` 行为不变
5. 成对 fill 后 analyzer 统计 residual 为 0 或接近 0，不把它当 completion residual failure

## Paper-shadow 验证建议

仅在上述 patch 和测试通过后才考虑 shadow:

```text
PM_PGT_SHADOW_PROFILE=xuan_pair_ask_rescue_v1
PM_DRY_RUN=true
PM_SHARED_INGRESS_ROLE=client
```

不建议在没有 depth channel 前使用大 clip。第一轮用 `clip10`，按 30 分钟 cadence:

1. 5-6 rounds smoke: 确认 profile/env、双腿 dry_run_taker、pair cost、无单腿 residual
2. 10 rounds: 看 participation 和 fee100/worst-case 方向
3. 20 rounds: 再判断是否扩大 cap 或 clip

## 当前不应该做的事

- 不直接把 `strict_l1_pair_ask_opportunity_scan` 的 `clip100` 映射进 shadow
- 不用现有单腿 PGT taker-open path 试跑
- 不把这个模块接到 settlement-alpha / winner-direction lifecycle
- 不依赖 current `BookTick` 推断 depth
- 不碰 shared ingress/broker/systemd/env

## 本地补丁状态

已在本地实现最小 shadow patch，未提交、未推送、未部署、未重启服务。

变更范围:

- `src/polymarket/strategy/pair_gated_tranche.rs`
  - 新增 dry-run-only profile `xuan_pair_ask_rescue_v1`
  - 新 profile 在 `yes_ask + no_ask <= 0.980` 时生成 YES/NO 两条 `Provide` BUY intent
  - 初始 clip 固定为 `10`
- `src/polymarket/coordinator_execution.rs`
  - 新增 `PgtPairAskRescueCandidate`
  - 新增独立 pair-ask rescue candidate 检查
  - 新增原子双腿 dispatch path，在同一 decision epoch 发送 YES/NO 两条 `OneShotTakerHedge`
  - 不复用旧的单腿 `pgt_shadow_taker_open_fired_epoch`
- `src/polymarket/coordinator.rs`
  - 新增 pair-ask rescue 自己的 fired epoch guard
- `src/polymarket/executor.rs`
  - dry-run taker order event 增加 `purpose` 和 `expected_fill_price`
- `scripts/analyze_pgt_shadow_events.py`
  - analyzer 增加 `taker(open/hedge/all)` 诊断
- `src/polymarket/coordinator_tests.rs`
  - 增加同 epoch 双腿 taker dispatch 单测

已通过本地验证:

- `cargo fmt --check`
- `cargo test xuan_pair_ask_rescue --lib`
- `cargo test pair_gated_tranche --lib`
- `python3 -m unittest scripts.tests.test_analyze_pgt_shadow_events`
- `python3 -m py_compile scripts/analyze_pgt_shadow_events.py`
- `cargo build --bin polymarket_v2`

剩余部署前 caveat:

- 仍然没有 live ask size/depth；因此只能从 `clip10` 做 paper-shadow smoke，不应直接用研究里更强的 `clip100`
- 当前 analyzer 只能区分 taker open/hedge purpose，尚未新增专门的 `pair_ask_rescue_sent` 事件名
- 需要人工 review diff 后，才适合 commit/push/deploy paper-shadow

## 下一步

推荐下一步是 review 本地补丁。如果认可，commit/push 后才考虑把 `pm-xuan-shadow.service` 从 negative-control `xuan_prepositioned_live_v1` 切到 `xuan_pair_ask_rescue_v1` 纸面 shadow。首次部署只用 `clip10`，按 5-6 / 10 / 20 round 快速窗口验证双腿 dry-run taker、pair cost、residual 和 fee-after worst-case。
