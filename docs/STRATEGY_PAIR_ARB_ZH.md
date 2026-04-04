# `pair_arb` 策略说明

本文档描述当前仓库中旧 `pair cost + A-S + OFI` 主线的恢复版本：`pair_arb`。

## 1. 定位

`pair_arb` 是一个：
- buy-only
- 双边 maker
- 以 `pair_target` 为核心目标
- 以库存偏置做风险控制

它不是 `glft_mm` 那种外锚驱动的真双边做市，也不是 `gabagool_grid` 的 utility 模型。
当前版本已经显式去掉 `pair_arb` 的方向对冲 overlay、尾盘强制去风险和市价对冲链路。

如果你的目标是继续走“尽量把 YES/NO 的组合成本压到目标线以下”的老路线，当前仓库里最直接的入口就是 `pair_arb`。

## 2. 策略在做什么

每个 tick，它会同时看 YES 和 NO 的盘口中间价：
- `mid_yes = (yes_bid + yes_ask) / 2`
- `mid_no = (no_bid + no_ask) / 2`

然后做三件事：

1. 看当前 `mid_yes + mid_no` 是否高于 `PM_PAIR_TARGET`
2. 看当前库存 `net_diff` 是否偏向某一边
3. 基于上述两点，把 YES 和 NO 的目标买价一起往下压，或向某一侧倾斜

结果是：
- 如果组合价格太贵，就双边一起压低
- 如果库存偏 YES，就压低 YES 买价、相对抬高 NO 买价
- 如果库存偏 NO，则反过来

## 3. 最简单的理解

可以把 `pair_arb` 理解成：

“围绕 `pair_target` 做一个带库存偏置的双边买入器。只有当新的买入通过阈值检查，并且满足『改善已配对收益/对子成本』或『在可接受风险内增加正 open edge』时，才继续挂单。”

它的核心不是预测方向，而是：
- 尽量低价收集 YES
- 尽量低价收集 NO
- 让组合成本尽量不要超过目标值

## 4. 每个 Tick 的动作流程（当前实现）

1. 读取 YES/NO 盘口
2. 计算 `mid_yes` 和 `mid_no`
3. 计算 `excess = max(0, mid_yes + mid_no - pair_target)`
4. 计算库存偏置 `skew = net_diff / max_net_diff`
5. 用 `as_skew_factor * time_decay` 生成库存偏移
6. 得到 `raw_yes/raw_no`
7. 如果已有对侧库存，再用 `Inventory Cost Clamp` 把该侧买价钳住
8. 再经过 strict maker clamp，确保不撞穿盘口
9. 对每个候选买单执行 `simulate_buy`，只在通过以下语义门时保留：
   - 优先：提升 `paired_locked_pnl` / 降低 `pair_cost` / 使 `pair_cost` 达到 `pair_target`
   - 次级：`utility_delta >= bid_size * tick_size`
   - 若是风险增加单：还要求 `open_edge` 比当前更好
10. 最终生成：
   - `YesBuy`
   - `NoBuy`

它不会生成正常盘中的 `Sell`。

## 5. 一个直观例子

假设：
- `pair_target = 0.98`
- 当前盘口：
  - `YES = 0.49 / 0.50`
  - `NO = 0.50 / 0.51`
- 当前没有持仓，`net_diff = 0`

这时：
- `mid_yes + mid_no = 1.00`
- 高于 `pair_target`

系统会做的不是“直接买”，而是：
- 同时把 YES 和 NO 的目标买价往下压
- 只在足够便宜的位置挂双边买单

如果随后只买到了 YES，导致 `net_diff > 0`：
- 下一轮会进一步压低 YES 的买价
- 同时相对提高 NO 的买价
- 目的是更偏向补齐 NO，而不是继续无脑买 YES

## 6. 当前真正重要的参数

- `PM_PAIR_TARGET`
  - 组合成本目标线，也是最核心参数
- `PM_MAX_NET_DIFF`
  - 盘中净仓硬上限
- `PM_BID_SIZE`
  - 单次挂单份额
- `PM_AS_SKEW_FACTOR`
  - 库存偏置强度
- `PM_AS_TIME_DECAY_K`
  - 时间衰减强度

共享层仍然会继续作用：
- OFI 风险抑制
- outcome floor
- stale gate

不再作为 `pair_arb` 正常盘中主逻辑的机制：
- 方向对冲 overlay（`Hedge`）
- 尾盘强制市价去风险
- maker sell alpha harvesting

## 7. 它和 `glft_mm` 的区别

`pair_arb`：
- 不依赖 Binance 外锚
- 不做真双边 `Buy + Sell`
- 主目标是 `pair_target / pair cost`
- 风格更接近你之前长期开发的老主线

`glft_mm`：
- 依赖 Binance 外锚
- 正常盘中走四槽位真双边做市
- 主目标是 GLFT reservation price + spread
- `pair_cost` 不再是主驱动

## 8. 测试建议

推荐测试顺序：
1. 先 `5m dry-run` 做机制冒烟（2-3 轮，主要看生命周期、挂撤和库存约束）
2. 再 `15m` 做收益验证（10 轮起步，参数冻结）

推荐验证配置：
- `PM_STRATEGY=pair_arb`
- `POLYMARKET_MARKET_SLUG=btc-updown-15m`
- `PM_BID_SIZE=5`
- `PM_MAX_NET_DIFF=5`
- `PM_DRY_RUN=true`（先）

在 `15m` 验证完成前，不建议把 `glft_mm` 作为生产主线。
