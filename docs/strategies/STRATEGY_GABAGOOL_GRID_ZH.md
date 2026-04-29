# `gabagool_grid` 策略说明

本文档描述当前仓库里的 `gabagool_grid`。

## 1. 定位

`gabagool_grid` 是一个：
- buy-only
- 双边候选
- 以 utility 改善为判断标准
- 比 `pair_arb` 更保守、更离散的库存构建策略

它不是原始 `pair_arb` 的简单重命名，也不是 `glft_mm` 的前身。

## 2. 策略在做什么

它每次只问一个问题：

“如果我现在再买一笔 YES 或 NO，这笔买入是否值得？”

这里的“值得”不是看方向，而是看：
- 是否让当前库存的综合效用提升
- 是否改善已有同侧成本
- 是否仍然处在允许的 open-pair 带宽内

## 3. 核心思路

对 YES/NO 两侧分别做一遍模拟：

1. 先看对侧 ask，得到当前允许的进入上限：
   - `entry_ceiling = open_pair_band - opposite_best_ask`
2. 如果这侧已经有库存：
   - 继续加仓必须比本侧平均成本更便宜至少 `1 tick`
3. 用 `aggressive_price_for()` 生成一个 maker 买价
4. 模拟这笔买入后的库存状态
5. 比较买入前后的 utility
6. 只有当效用改善足够大，才保留这张买单

所以它不是一直双边挂，也不是一直追着市场买。

## 4. 一个简单例子

假设：
- `open_pair_band = 0.99`
- 当前：
  - `YES ask = 0.48`
  - `NO ask = 0.50`

那么对于 YES：
- `entry_ceiling = 0.99 - 0.50 = 0.49`
- YES 当前 ask 在允许范围内
- 系统会尝试挂一个 YES maker buy

如果后来又已经持有 YES，平均成本是 `0.48`，那么下一笔 YES：
- 必须比 `0.48 - 1 tick` 更便宜
- 否则就不继续加 YES

这就是它和 `pair_arb` 的最大区别：
- `pair_arb` 是连续型压价
- `gabagool_grid` 是离散型“值得才买”

## 5. 当前重要参数

- `PM_OPEN_PAIR_BAND`
  - 允许进入的组合带宽
- `PM_BID_SIZE`
  - 单次份额
- `PM_MAX_NET_DIFF`
  - 净仓硬上限
- `PM_TICK_SIZE`
  - 同侧成本改善的最小步长

此外，它会继续使用共享层：
- outcome floor
- OFI gate
- stale gate
- endgame

## 6. 它和 `pair_arb` 的区别

`gabagool_grid`：
- 先做模拟，再决定买不买
- 更强调 utility 改善
- 更像一个“离散下单选择器”

`pair_arb`：
- 更接近传统 `pair target + skew` 的连续报价
- 风格更像老的 A-S / pair-cost 主线

## 7. 当前结论

如果你要一个：
- 更像“买入选择器”的 buy-only 策略

可以用：
- `PM_STRATEGY=gabagool_grid`

如果你要一个：
- 更像你原来长期开发的 pair-cost / A-S 路线

应该优先看：
- `PM_STRATEGY=pair_arb`
