# Polymarket 做市策略说明

## 📋 系统定位

**此系统是高度优化的 Maker-Only 纯做市场商，核心基于 Gabagool22 组合成本摊薄风控，叠加了 Avellaneda-Stoikov (A-S) 纯正库存偏移（Skew）模型以及 OFI 流动性防身。**

- **做市（Market Making）**: 在市场上同时挂买单和卖单，赚取 bid-ask spread
- **Maker-Only**: 所有订单都是 **Post-Only** 限价单，确保赚取 maker rebate  
- **网格摊薄体系**: 不盲目止损抛售。下跌时开启网格买入，通过摊平成本拉起对冲天花板。
- **A-S 偏移**: 仓位越重，挂单越偏。自然消化风险。

## 🎯 核心架构（Gabagool22 + A-S 综合体）

**只要双边都成交，无论 YES 对还是 NO 对最终胜出，都能确保每 pair $0.015+ 的利润。**

### 2. 阶段化做市引擎 (Unified Pricing Engine)

**核心思想**: **消灭死扛，永远保持活跃性。**系统有三种自然演化的做市状态。

#### 第一道防线：被动防守 (库存越界与盘口存活控制)
1. **盘口过期撤单**: 如果连续 30 秒没有任何 Trade 被 Polymarket 推送，撤销挂单以防盲狙。
2. **库存越界撤单**: 如果此时的持仓触及了 `max_net_diff` 上限阈值（即单边仓位满了 10 份），系统会自动触发硬阻断——**主动 Cancel 撤销该持有方向的所有买单**，并在另一侧（对冲端）以 Gabagool22 兜底公式挂出新单。这保证了即时 OFI 没触发时你的仓位也不会无限套牢。

#### 第二道防线：基于 OFI 的毒性流避险 (Toxic Hedge Override)
当市场出现狂跌/暴涨的单边集中换手砸盘时（如实盘校准后的阈值 `OFI > 300` 级别，过滤了前 10% 的极值数据）。
系统触发 **选择性防飞刀**：
- 如果没有敞口，全面撤单，防空接飞刀。
- 如果有被套敞口（例如拿着 YES），立刻撤销 YES（防接盘），并直接向上顶格挂出最高额度的对冲单（NO），力求在巨震中脱身。

#### 核心输出：常规做市区间（A-S 网格做市）
只要还在安全线内（未触及 `max_net_diff` 限制），系统永远在双边挂单，同时利用 **纯正 Avellaneda-Stoikov** 存货倾斜模型给出最优价：
- 如果没有仓位：买卖分布非常匀称（Mid ± 半差价）。
- 如果手中有过量 YES 被套：
  1. A-S 惩罚机制启动：YES 挂单自动打骨折，向下压价 `skew * PM_AS_SKEW_FACTOR`。默认 SKEW 系数为 `0.03`，表现为强烈的库存厌恶。如果你想变成纯正 Gabagool22 的激进底部网格大吃货，可在 `.env` 中把 `PM_AS_SKEW_FACTOR` 改成 `0.00`。
  2. A-S 奖励机制启动：NO 挂单给出溢价（迎合对手盘，提升撮合几率）。
  3. 通过网格一路向下接：随着你继续以更低阶买入 YES，你的 `yes_avg_cost` 持续降低。

#### 绝境防守区间：触及风控天花板（Gabagool22 硬兜底）
当你积累单侧仓位触发了 `net_diff >= max_net_diff`。
- 此时执行铁律：坚决停止买入持有侧。
- 对手端对冲公式启动：`NO bid = min(pair_target - yes_avg_cost, 市场 NO 卖首 - tick)`
- 由于你前面的 A-S 网格一路成功把 `yes_avg_cost` 从 0.5 摊薄到了极低水平，此时对冲天花板将大幅跃升贴近最新市价。
- 只要稍微回调或填档，立刻零风险完美对冲离场。

## 4. OFI 毒性引擎

- **独立双窗口**：YES 和 NO 各自维护一个 3 秒滑窗
- **OFI Score**：`buy_volume - sell_volume` 绝对值超阈值 → toxic
- **Heartbeat**：每 200ms 强制驱逐过期 tick，即使无新 trade 到达
- **edge-triggered 日志**：只在 toxic 边沿触发日志，避免刷屏
- **容量保护**：每侧最多保留 4 096 个 tick，超出时自动淘汰最旧数据
- **自适应阈值（可选）**：`PM_OFI_ADAPTIVE=true` 启用。基于 200 次滚动均值 + 3σ 自动计算阈值，自动适配高/低流动性市场，范围钳位 [50, 1000]
- **直通 Kill 信道**：OFI → Coordinator 独立 mpsc 信道，边沿触发（首次变 toxic 时）。Coordinator 的 `biased select!` 优先处理，绕过等待下一个 book tick 的延迟

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
| **per-side 盘口 TTL** | coordinator.rs | **YES/NO 各自独立 30s 过期检测（已修复双侧共享问题）** |
| Confirmed 幂等 | inventory.rs | 防止库存翻倍 |
| OrderFilled 反馈 | executor→coordinator | 成交后释放 slot |
| **3重 can_open（修复版）** | inventory.rs | 净仓 + 成本 + 敞口（初次买入现用保守估价 1.0） |
| Cancel fallback | executor.rs | CancelAll 失败 → 逐单撤（失败计数明确日志） |
| Price clamp | coordinator.rs | 0.001 ≤ price ≤ 0.999 |
| OFI global kill | coordinator.rs | 双侧联动撤单 |
| **对冲单保护标志** | coordinator.rs | **hedge_dispatched 防止对冲单被底部清理代码立即取消** |
| **OMS Cooldown** | order_manager.rs | **余额不足后遵守 Executor 发送的冷却时间（30s）** |
| **OFI 窗口容量限制** | ofi.rs | **SideWindow 最大 4096 tick，防止高频市场内存无限增长** |
| **O(1) Dedup 淘汰** | user_ws.rs | **DedupCache 改用 VecDeque 实现 O(1) LRU，替换原 O(n) 扫描** |
| **价格范围收窄** | polymarket_v2.rs | **parse_price_value 限制 (0,1)，杜绝百分制价格污染** |
| **WS 重连盘口重置** | polymarket_v2.rs | **BookAssembler 每次重连归零，防止旧数据跨连接污染** |
| **A-S 时间衰减** | coordinator.rs | **skew_factor 随市场临期线性增大（默认最高 3×），加速临期清仓** |
| **OFI 自适应阈值** | ofi.rs | **滚动均值+3σ 自动计算毒性阈值，PM_OFI_ADAPTIVE=true 启用** |
| **Hedge 防抖绕过** | coordinator.rs | **对冲单使用独立 100ms 防抖，不受正常 500ms 防抖限制** |
| **OFI 直通 Kill 信道** | ofi.rs + coordinator.rs | **毒性首次触发时立即通知 Coordinator，biased select! 零等待响应** |

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
