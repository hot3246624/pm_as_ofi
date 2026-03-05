# 策略详解：Polymarket V2 Maker-Only Engine

## 1. 利润模型

```
价格假设：YES mid ≈ 0.50, NO mid ≈ 0.50
我方出价：bid_yes + bid_no ≤ pair_target (0.985)
结算收入：resolution 无论方向必定支付 1.00

profit_per_pair = 1.00 - (bid_yes + bid_no)
               ≥ 1.00 - 0.985 = $0.015 per share pair
```

**只要双边都成交，无论 YES 对还是 NO 对最终胜出，都能确保每 pair $0.015+ 的利润。**

## 2. 策略状态机

### State A: BALANCED（`net_diff ≈ 0` + `can_open = true`）

- 计算 `mid_yes = (yes_bid + yes_ask) / 2`
- 计算 `mid_no = (no_bid + no_ask) / 2`
- 如果 `mid_yes + mid_no > pair_target`，对称扣除超额
- 双边同时挂 Post-Only Bid

### State B: HEDGE（`net_diff ≠ 0`）

- 如果 `net_diff > 0`（持有过多 YES）：
  - 撤销 YES 侧挂单
  - `ceiling = pair_target - yes_avg_cost`（确保总成本不超标）
  - `price = min(ceiling, no_ask - tick)`（不会溢价追单）
  - Aggressive bid NO

- 如果 `net_diff < 0`（持有过多 NO）：
  - 同理反方向

### Priority 0: GLOBAL KILL SWITCH

- 任一侧 `|OFI| > toxicity_threshold` → 双边全部撤单
- 基于 Lead-Lag 理论：套利者在 YES 和 NO 间传导信息流

### Priority Gate: `!can_open`

三重检查全部通过才允许开仓：
1. `|net_diff| < max_net_diff`（仓位偏差 < 10）
2. `portfolio_cost < max_portfolio_cost`（组合成本 < 1.02）
3. `single_side_value < max_position_value`（单侧 < $50）

## 3. 成交生命周期

```
Coordinator.place()  →  set slot.active=true
  │
Executor.handle_place_bid()  →  SDK post_order(post_only=true)
  │
  ├─ 成功 → open_orders[side].insert(order_id, size)
  │   │
  │   └─ User WS: FillEvent(MATCHED)
  │       ├─ Executor: remaining -= fill_size
  │       │   └─ remaining ≤ 0 → remove + send OrderFilled(side)
  │       ├─ InventoryManager: ledger.push() + recompute_from_ledger()
  │       └─ Coordinator: slot.active = false (可立刻重新挂单)
  │
  │   └─ User WS: FillEvent(CONFIRMED)
  │       └─ InventoryManager: no-op（幂等，不重复入账）
  │
  └─ 失败 → send OrderFailed(side)
      └─ Coordinator: slot.active = false（重置 ghost slot）
```

## 4. OFI 毒性引擎

- **独立双窗口**：YES 和 NO 各自维护一个 3 秒滑窗
- **OFI Score**：`|buy_volume - sell_volume|` 超阈值 → toxic
- **Heartbeat**：每 200ms 强制驱逐过期 tick，即使无新 trade 到达
- **edge-triggered 日志**：只在 toxic 边沿触发日志，避免刷屏

## 5. Fill Ledger（VWAP 零漂移）

```
Matched → ledger.push(FillRecord { order_id, side, size, price })
Confirmed → no-op（幂等，已由 Matched 记录）
Failed → ledger.remove(匹配 order_id + side)

Every change → recompute_from_ledger():
  yes_qty = Σ ledger[side=Yes].size
  yes_avg = Σ(size * price) / yes_qty
  ...同理 NO 侧...
```

## 6. 安全机制清单

| 机制 | 位置 | 说明 |
|------|------|------|
| post_only=true | executor.rs | 100% 不会变 taker |
| 启动 CancelAll | polymarket_v2.rs | 清除历史残单 |
| 关机 CancelAll | polymarket_v2.rs | MarketExpired 触发 |
| Owner 地址匹配 | user_ws.rs | 只处理自己钱包的 fill |
| 陈旧盘口 TTL | coordinator.rs | 30s 无数据 → 全撤 |
| Confirmed 幂等 | inventory.rs | 防止库存翻倍 |
| OrderFilled 反馈 | executor→coordinator | 成交后释放 slot |
| 3重 can_open | inventory.rs | 净仓 + 成本 + 敞口 |
| Cancel fallback | executor.rs | CancelAll 失败 → 逐单撤 |
| Price clamp | coordinator.rs | 0.001 ≤ price ≤ 0.999 |
| OFI global kill | coordinator.rs | 双侧联动撤单 |

## 7. $100 资金参数计算

```
资金总额: $100 USDC.e
单侧最大: $50 (MAX_POSITION_VALUE)
每笔大小: $5 (BID_SIZE)
最大偏差: 10 股 (MAX_NET_DIFF = BID_SIZE × 2)
每对利润: ≈ $0.015 × 5 = $0.075
回本轮数: 100 / 0.075 ≈ 1,334 对
5分钟市场: 每轮 5-15 分钟可完成多对
```
