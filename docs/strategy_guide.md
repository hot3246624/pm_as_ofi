# V2 策略说明（Maker-Only + 库存约束 + OFI 熔断）

本文档描述当前 `polymarket_v2` 实现的真实逻辑，不是理想化版本。

## 1. 设计目标

- 仅做 Maker：所有挂单 `post_only=true`
- 把库存变化源头收敛到 User WS 成交事件
- 用简单、可解释、可验证的状态机代替黑箱预测

## 2. 三个核心模块

### 2.1 InventoryManager

维护：

- `yes_qty`, `no_qty`
- `yes_avg_cost`, `no_avg_cost`
- `net_diff = yes_qty - no_qty`
- `portfolio_cost = yes_avg_cost + no_avg_cost`（双腿都 > 0 时）
- `can_open`

`can_open` 判定为三重与条件：

1. `abs(net_diff) < PM_MAX_NET_DIFF`
2. `portfolio_cost < PM_MAX_PORTFOLIO_COST`（或 `portfolio_cost == 0`）
3. 单侧价值限制：
   - `yes_qty * yes_avg_cost < PM_MAX_POSITION_VALUE`
   - `no_qty * no_avg_cost < PM_MAX_POSITION_VALUE`

### 2.2 OFI Engine

对 YES/NO 两侧分别维护滑动窗口（默认 3 秒）：

- `ofi_score = buy_volume - sell_volume`
- `is_toxic = abs(ofi_score) > PM_OFI_TOXICITY_THRESHOLD`

只要任一侧 toxic，就触发全局熔断。

### 2.3 StrategyCoordinator

输入：Book + Inventory + OFI
输出：`ExecutionCmd`

状态：

- `Balanced`（净仓接近 0）
- `Hedge`（净仓不为 0）
- `Global Kill`（任一侧 OFI toxic）

## 3. 定价与下单逻辑

## 3.1 Balanced（双边提供流动性）

给定盘口中点：

- `mid_yes = (yes_bid + yes_ask) / 2`
- `mid_no = (no_bid + no_ask) / 2`

若 `mid_yes + mid_no <= pair_target`：

- `bid_yes = mid_yes`
- `bid_no = mid_no`

否则等量下调两侧，直到和为 `pair_target`。

最后做边界钳制：`clamp(0.001, 0.999)`。

## 3.2 Hedge（只补缺腿）

当 `net_diff > 0`（YES 偏多）：

1. 先撤 YES 买单（避免继续加偏）
2. 计算 NO 对冲上限：`ceiling = pair_target - yes_avg_cost`
3. 用激进 maker 价挂 NO：`min(ceiling, no_ask - tick)`

当 `net_diff < 0`（NO 偏多）同理对称。

## 3.3 Global Kill（全局熔断）

若 `ofi.yes.is_toxic || ofi.no.is_toxic`：

- 撤 YES、NO 两侧所有活跃单
- 暂停新挂单，直到毒性恢复

## 4. `!can_open` 时为什么撤单

这是库存硬风控，不是价格预测：

- `net_diff≈0`：不需要对冲，撤掉双边 Provide 单，避免继续扩仓
- `net_diff>0`：只撤 YES（风险侧），保留 NO（对冲侧）
- `net_diff<0`：只撤 NO（风险侧），保留 YES（对冲侧）

这可以解释你提到的“不是总是 CancelAll，而是保留对冲方向”的行为。

## 5. 订单生命周期（防重复、可回滚）

1. Coordinator 发 `PlacePostOnlyBid`
2. Executor 下单（REST）
3. 订单成交后，由 User WS 推送 `trade` 事件
4. User WS 解析为 `FillEvent` 发给 InventoryManager（和 Executor 生命周期清理）
5. `MATCHED` 计入库存，`CONFIRMED` 跳过（避免重复），`FAILED` 反向回滚

去重基于 trade id / maker order id 组合键，防止重连或重复消息导致重复记账。

## 6. 关键伪代码

```text
on_tick(book, ofi, inv):
  if ofi_yes_toxic or ofi_no_toxic:
    cancel_side(YES)
    cancel_side(NO)
    return

  if book not usable:
    return

  if inv.net_diff == 0:
    if not inv.can_open:
      cancel_provide_orders_by_rule(inv.net_diff)
      return
    place_or_reprice_balanced_yes_no()
  else:
    place_or_reprice_hedge_side_only(inv.net_diff)
```

## 7. 小资金参数建议（如本金 $250）

若你只愿承担约 `$5` 单侧风险：

```bash
PM_MAX_POSITION_VALUE=5
PM_MAX_NET_DIFF=5
PM_BID_SIZE=1
PM_PAIR_TARGET=0.99
PM_TICK_SIZE=0.001
PM_DRY_RUN=true   # 先观测
```

再根据成交密度和撤单率小步调整，不建议直接放大 `PM_BID_SIZE`。

## 8. 与“预测未来价格”的关系

当前实现并不依赖 CEX 价格领先预测，也不做方向判断。看起来像“提前撤单/改价”的动作，主要来自：

- OFI 毒性阈值触发
- 重报价阈值触发
- `can_open` 风控门控
- 对冲优先级高于双边提供

这是一套可解释的库存做市系统，不是纯冲量预测模型。
