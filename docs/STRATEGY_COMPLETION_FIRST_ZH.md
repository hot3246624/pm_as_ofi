# Completion-First 策略设计

## 1. 目标

这不是 `pair_target-first` 策略，而是 `completion-first` 策略。

主目标按优先级排序：

1. 在 `30s` 内把首腿库存转成可 `MERGE` 的 matched pair
2. 配对完成后尽快 `MERGE` 回收 USDC
3. 只在 `completion probability` 足够高的市场状态里参与
4. 在上述约束下，再优化 realized `pair_cost`

这套设计是对当前 [STRATEGY_PAIR_ARB_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/STRATEGY_PAIR_ARB_ZH.md:1) 的刻意反转：

- 现有 `pair_arb`：`pair_cost-first`
- 新设计：`completion hazard-first`

## 2. 可实现前提

策略只使用当前系统已经具备或可直接补采的数据：

- 双边盘口：`best bid/ask`、spread、top-of-book depth、book freshness
- 最近成交流：两边 recent trades、方向翻转频率、成交密度
- 当前库存：`yes_qty / no_qty / avg_cost / net_diff`
- round 时钟：距开盘/收盘时间、是否在 cooldown
- merge/claim 状态

不依赖任何隐藏权限、特殊撮合接口或私有对手方流。

## 3. 核心假设

Polymarket 的补腿概率，不主要来自“方向判断”，而主要来自以下三件事：

1. 当前 round 是否处于高双边成交流 regime
2. 首腿成交后，是否立刻切到 `opposite-completion mode`
3. 是否把已配对 tranche 先 `MERGE`，而不是继续叠成大库存

所以核心预测对象不是：

- `E[pair_cost]`

而是：

- `Pr(opposite fill within 30s | current microstructure, first-leg state)`

## 4. 状态机

### 4.1 `Idle`

没有 live round，或 round gate 未通过。

进入条件：

- 没有 live unmatched inventory
- 不在 cooldown
- `completion_score >= enter_threshold`

退出条件：

- 发出 seed 双边挂单，进入 `Seed`

### 4.2 `Seed`

目标：用小 tranche 测试这一轮是否值得进入。

动作：

- 双边各挂 `2-3` 档 maker bid
- 总 tranche notional 小，默认只允许 `seed_clip_notional`
- 两边 price band 满足：
  - `bid_yes + bid_no <= dynamic_max_pair_cost`
  - `bid_yes + bid_no >= dynamic_min_pair_cost`
- 若 book 只剩单边、另一边 stale，则不 seed

退出条件：

- 任一侧首腿成交，进入 `Completion`
- round gate 失效且无成交，撤单回 `Idle`

### 4.3 `Completion`

目标：把首腿库存在 `30s` 内转成 matched pair。

动作：

- 首腿一成交，立即取消所有 same-side risk-increasing live orders
- 只保留 opposite side 的 completion ladder
- 允许每 `4-6s` 最多一次向上 reprice
- reprice 永不突破：
  - `dynamic_completion_ceiling = dynamic_max_pair_cost - first_leg_avg`
- 不继续同侧摊成本，除非启用单次 tiny add 例外

硬约束：

- `completion_ttl_secs = 30`
- `max_same_side_adds_before_completion = 0`
- `max_unmatched_notional` 不能继续增长

退出条件：

- opposite fill 到足够份额，进入 `Harvest`
- 超时未配对，进入 `ResidualHalt`

### 4.4 `Harvest`

目标：把足够大的 matched tranche 尽快转回现金。

动作：

- 若 `paired_qty >= merge_min_qty`，发 `MERGE`
- `MERGE` 完成前禁止新 seed
- 剩余极小尾巴允许留待 `REDEEM`

退出条件：

- `MERGE` 完成，进入 `Cooldown`

### 4.5 `Cooldown`

目标：避免连续开新一轮导致库存串扰。

动作：

- merge 后等待一个短冷却窗口
- 冷却结束后重新评估 round gate

默认：

- `post_merge_cooldown_secs = 10`

退出条件：

- round gate 仍通过，回 `Idle`
- 否则继续空仓等待

### 4.6 `ResidualHalt`

目标：首腿没能在 TTL 内配平时，阻断继续放大。

动作：

- 取消未成交 same-side live orders
- 停止新一轮 seed
- TTL 内只允许 opposite completion maker repair
- TTL 超时后升级为价格受限的 opposite-side `FAK` repair
- `FAK` 价格上限仍受动态 pair band 约束，不做无上限市价追单

## 5. Completion Score

进入 `Seed` 前，每轮先算一个 `completion_score in [0, 1]`。

建议特征：

1. `flip_rate_10s`
   - 最近 `10s` 成交方向翻转频率
   - 两边都有流时更高

2. `two_sided_trade_density_10s`
   - 最近 `10s` 内两边 outcome 的成交笔数/成交量

3. `book_health`
   - 双边 top ask/bid 都存在
   - freshness 未过期
   - spread 不过宽

4. `inside_depth_symmetry`
   - 两边 top-of-book depth 都不太薄
   - 不能出现一边常驻空簿

5. `queue_turnover`
   - 顶层价位在最近几秒是否持续被刷新/吃掉

6. `clock_penalty`
   - 刚开盘 `0-5s` 与临近收盘尾端可以额外加权
   - 但若处于差 regime，时钟不单独放行

建议分数形式：

```text
completion_score =
    0.30 * flip_rate_10s
  + 0.25 * two_sided_trade_density_10s
  + 0.20 * book_health
  + 0.15 * inside_depth_symmetry
  + 0.10 * queue_turnover
  - penalties
```

默认：

- `enter_threshold = 0.70`
- `hold_threshold = 0.55`

## 6. 动态 pair band

这里不再使用单点 `pair_target`。

改为：

- `dynamic_max_pair_cost`
- `dynamic_min_pair_cost`

它们由 `completion_score` 驱动：

```text
if score in [0.70, 0.80): max_pair_cost = 0.970
if score in [0.80, 0.90): max_pair_cost = 0.980
if score >= 0.90:         max_pair_cost = 0.990
```

而 `dynamic_min_pair_cost` 只用于避免挂得过低、导致很久都不成交：

```text
dynamic_min_pair_cost = max_pair_cost - 0.05
```

这意味着：

- 高分 regime 可以接受更高 pair cost，以换取更高 completion probability
- 实现出来的 realized pair cost 可以落在 band 内任意位置
- 所以 pair cost 看起来“不连续”是正常现象

## 7. 下单几何

### 7.1 Seed 阶段

建议双边各挂三档：

- `L1`: `best_bid`
- `L2`: `best_bid - 1 tick`
- `L3`: `best_bid - 2 ticks`

但双边每一档组合必须满足：

- `yes_bid_i + no_bid_j <= dynamic_max_pair_cost`

优先保留更靠里的档位，减少首腿之后的补腿难度。

### 7.2 Completion 阶段

一旦 YES 首腿成交：

- 撤掉 YES 侧剩余 risk-increasing 档位
- 只保留 NO completion 梯子
- completion 梯子可以每 `4-6s` 向上抬 `1 tick`

NO 首腿成交时对称处理。

## 8. 风控

### 8.1 不再允许的行为

- 首腿成交后继续无限制同侧摊低成本
- merge 前立刻开始下一轮 seed
- 把大 residual 长时间留在账上

### 8.2 关键参数

- `seed_clip_notional_usdc`
- `merge_min_qty`
- `completion_ttl_secs`
- `post_merge_cooldown_secs`
- `max_unmatched_notional_usdc`
- `max_same_side_adds_before_completion`
- `hedge_debounce_ms`

推荐初始值：

```text
seed_clip_notional_usdc         = 25
merge_min_qty                   = 20 shares
completion_ttl_secs             = 30
post_merge_cooldown_secs        = 10
max_unmatched_notional_usdc     = 35
max_same_side_adds_before_completion = 0
hedge_debounce_ms               = 100
```

## 9. 接入现有系统的实现建议

这套策略不适合继续塞进现有 `pair_arb` 的 `pair_target-first` 语义里。

建议新建：

- `StrategyKind::CompletionFirst`
- `src/polymarket/strategy/completion_first.rs`

需要复用的现有能力：

- inventory/working inventory 更新
- `Merge sync`
- 现有 OMS / post-only 安全夹层
- stale/toxic gating

需要新增的状态：

- `phase = Idle | Seed | Completion | Harvest | Cooldown | ResidualHalt`
- `first_leg_side`
- `first_leg_avg_cost`
- `first_leg_fill_ts`
- `matched_qty_ready_to_merge`
- `cooldown_until_ts`
- `completion_score`

## 10. 自检流程

先不用 live book 回放，先做结构自检：

1. round selection 是否存在
2. active session 内是否高参与
3. 首腿成交后 opposite 是否通常为下一笔
4. `30s` 内配对率是否足够高
5. merge 是否在 round 结束前发生，且不是拖到结算后才处理
6. merge 后是否存在短 cooldown，再进入下一轮
7. unmatched cost 是否维持很低

仓库里的自检脚本：

- [scripts/self_check_completion_strategy.py](/Users/hot/web3Scientist/pm_as_ofi/scripts/self_check_completion_strategy.py:1)

## 11. 当前结论

这套策略的可行性，不建立在“预测方向”上，而建立在：

- `BTC 5m` 这类高双边成交流 market
- completion-oriented state machine
- merge-first inventory discipline

因此它最值得验证的，不是更低 `pair_target`，而是更高：

- `completion probability`
- `merge velocity`
- `inventory half-life`
