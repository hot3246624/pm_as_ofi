# Completion First 策略说明

## 目标
- 复刻 `xuanxuan008` 的核心做法，而不是继续扩写 `pair_arb`
- 第一阶段只支持 `BTC 5m`
- 第一阶段默认 `shadow`，只输出决策与生命周期事件，不真实下单
- 当前入口：
  - `PM_STRATEGY=completion_first`
  - 兼容别名：`xuan_clone`

## 策略语义
- `FlatSeed`
  - 同时生成 `YES_BUY` 与 `NO_BUY`
  - seed 定价复用 `pair_arb` 的 buy-side 底座
  - seed 数量不再沿用 `bid_size`，而是直接使用 `completion_first clip`
- `CompletionOnly`
  - 一旦出现 active tranche，默认优先推动对侧 completion
  - 同侧加仓最多允许 1 次
  - same-side add 也使用 `completion_first clip`
- `HarvestWindow`
  - 收盘前 `t-25s` 进入
  - 只做 completion 评估与 merge 请求评估
- `PostResolve`
  - 结算后不再开新仓
  - 只做 winner-side redeem 请求评估

## Clip 规则
- 内置常量，不开放 env：
  - `BASE_CLIP = 150`
  - `MAX_CLIP = 250`
  - `MIN_CLIP = 45`
- 适用范围：
  - `FlatSeed` 双边 seed
  - `CompletionOnly` 下的 same-side add
  - completion 腿（上限为 `min(residual_qty, clip)`）
- 乘子：
  - `session_mult`
  - `imbalance_mult`
  - `trade_index_mult`
  - 尾部 `30s` 再乘 `1.16`
- 最终数量四舍五入到 `0.1 share`

## 生命周期
- `merge`
  - `t-25s` 首次检查
  - `pairable_full_sets >= 10` 才请求
  - `t-18s` 最多 retry 一次
  - `shadow` 模式下会同时写 `completion_first_merge_requested` 与 `completion_first_merge_executed`
- `redeem`
  - 只针对 winner-side residual
  - `+35s` 首次请求
  - `+50s` 第二次请求

## Shadow 事件
- `completion_first_seed_built`
- `completion_first_completion_built`
- `completion_first_same_side_add_blocked`
- `completion_first_merge_requested`
- `completion_first_merge_executed`
- `completion_first_redeem_requested`

## 观察重点
- `pair_tranche_events`
- `pair_budget_events`
- `capital_state_events`
- `clean_closed_episode_ratio`
- `same_side_add_qty_ratio`
- `episode_close_delay_p50/p90`
- replay/report：
  - `python scripts/build_replay_db.py --date YYYY-MM-DD`
  - `python scripts/export_completion_first_shadow_report.py --date YYYY-MM-DD`
