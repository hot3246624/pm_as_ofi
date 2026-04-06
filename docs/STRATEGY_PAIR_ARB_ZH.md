# `pair_arb` 策略说明（2026-04 版）

本文档描述当前主线 `pair_arb` 的最终语义与参数基线。

## 1. 定位

`pair_arb` 是一个：
- maker-only
- buy-only
- 双边报价
- 以 `pair_cost / paired_locked_pnl` 为主目标
- 以库存偏置限制单边风险

它不是 `glft_mm` 的外锚真双边做市，也不包含尾盘市价对冲或方向性 overlay。

## 2. 一句话策略意图

只有当新的买入符合阈值要求，最好能服务于更低对子成本，且不把库存推入不可接受的单边敞口时，才继续挂单。

## 3. Tick 级决策流程

每个 tick：

1. 计算盘口中间价：
- `mid_yes = (yes_bid + yes_ask) / 2`
- `mid_no = (no_bid + no_ask) / 2`

2. 基于 `pair_target` 和库存偏置得到 `raw_yes/raw_no`：
- `excess = max(0, mid_yes + mid_no - pair_target)`
- `raw_yes/raw_no` 先做对称下压，再做库存 skew 偏移

3. 应用三段库存 skew（固定边界 `5 / 10 / 15`）：
- `|net_diff| < 5`：`0.35 * as_skew_factor`，不叠加 time decay
- `5 <= |net_diff| < 10`：从 `35% -> 100%` 线性抬升，叠加 time decay
- `10 <= |net_diff| < 15`：`100% * as_skew_factor`，叠加 time decay
- `|net_diff| >= 15`：由库存上限逻辑阻止继续加主仓侧风险

4. 应用主仓侧梯度 avg-cost 封顶：
- Tier 1：`|net_diff| >= 5`，主仓侧 `bid <= avg_cost * 0.85`
- Tier 2：`|net_diff| >= 10`，主仓侧 `bid <= avg_cost * 0.70`

5. 应用 OFI 软塑形（仅同侧风险增加单）：
- `pairing / risk-reducing buy`：完全忽略 OFI
- `same-side risk-increasing buy`：
  - `is_hot=true`：额外下调 `1 tick`
  - `is_toxic=true`：额外下调 `2 ticks`
  - `is_toxic && saturated`：直接 suppress，不挂这一侧

6. 应用 `pair_cost ceiling`（硬保护）：
- `yes_ceiling = pair_target - no_avg_cost`
- `no_ceiling = pair_target - yes_avg_cost`

7. 应用 strict maker clamp 与 tick 对齐。

8. 对候选买单执行 `simulate_buy` 过滤：
- 优先：提升 `paired_locked_pnl` / 降低 `pair_cost` / 让 `pair_cost` 达到目标线
- 次级：`utility_delta >= bid_size * tick_size`
- 若是风险增加单：还要求 `projected_open_edge > current_open_edge`

## 4. Round Suitability Gate（15m）

`pair_arb` 对每轮先做 `60s` 观察窗口：

- 观察期内只收集 `mid_yes/mid_no` 样本，不发单。
- 60 秒后用中位数判定：
  - `min(median_mid_yes, median_mid_no) < 0.20`
  - 且 `median_mid_yes + median_mid_no >= pair_target + 0.01`
- 同时满足则本轮标记 `SkippedImbalanced`，整轮静默，不中途恢复。

用途：跳过开盘就严重单边、几乎没有 pair edge 的 round（例如 `12/88`）。

## 5. `locked_pnl > 0` 的语义

`locked_pnl > 0` 不等于停手。

策略在已锁利后仍可继续买入，只要新买入仍通过：
- `pair_cost ceiling`
- `max_net_diff`
- `simulate_buy` 的改善条件

停止由策略自然触发：
- 没有候选通过过滤
- 命中库存上限
- 命中 `SkippedImbalanced`

## 6. OFI 与执行层边界

当前主线中：
- OFI 由 `pair_arb` 策略层自己消费，不再由执行层对 `pair_arb` 做常态硬拦截
- OFI 只塑形同侧风险增加单，不阻断 pairing buy
- `PM_OFI_*` 继续复用现有全局 OFI 引擎参数，`pair_arb` 不新增专属参数
- 执行层节奏参数（`debounce/reprice_threshold` 等）本轮不作为策略调参对象

## 7. 推荐验证参数（当前默认）

- `PM_STRATEGY=pair_arb`
- `POLYMARKET_MARKET_SLUG=btc-updown-15m`
- `PM_BID_SIZE=5`
- `PM_MAX_NET_DIFF=15`
- `PM_PAIR_TARGET=0.98`
- `PM_AS_SKEW_FACTOR=0.06`
- `PM_AS_TIME_DECAY_K=1.0`

## 8. 验证顺序

1. `dry-run` 5 个完整 round：
- 核对 `SkippedImbalanced` 是否只在失衡 round 触发
- 核对前段 skew 是否明显低于旧版本
- 核对 Tier 1/2 封顶是否生效

2. 小额 live 10 个 round：
- 观察 `locked_pnl` 中位数
- 观察是否仍出现主仓侧高位加仓
- 观察残仓损失是否改善
