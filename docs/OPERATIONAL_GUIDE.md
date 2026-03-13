# 深度分析与最佳操作实践

**日期**: 2026-03-09
**版本**: Post-Optimization v2
**适用**: Polymarket A-S + OFI + Gabagool22 做市机器人

---

## 一、系统深度分析

### 1.1 架构全景

```
WebSocket (Market Feed)
    │  BookTick (YES/NO 4-price)
    ▼
BookAssembler ──► OFI Engine ──► OfiSnapshot (watch)
    │                  │
    │                  │ KillSwitchSignal (mpsc, biased)
    ▼                  ▼
StrategyCoordinator ◄──────────────────────────────
    │  (reads OfiSnapshot + InventoryState via watch)
    │  OrderManagerCmd::SetTarget
    ▼
OrderManager (OMS) ──► Executor ──► Polymarket REST API
    ▲                      │
    │   OrderResult        │  FillEvent
    └──────────────────────┘
                           │ FillEvent (fan-out)
               ┌───────────┴───────────┐
               ▼                       ▼
       InventoryManager         (User WS / Executor state)
       InventoryState (watch)
```

**关键设计决策**：
- **Actor 模型**：所有组件通过 `mpsc` / `watch` 通信，无共享可变状态，天然线程安全
- **Watch 通道用于读密写疏的状态**（OFI 快照、库存状态）——每次写覆盖，读方随时可读最新值
- **Mpsc 通道用于命令流**——保证顺序，背压天然限流
- **BookAssembler 双侧合并**——等 YES 和 NO 都更新后才发出 BookTick，保证定价使用同批次盘口

### 1.2 定价逻辑深度解析

#### 三层状态机

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 1: Environmental Health Check (Sequential in V2)        │
│  - Stale Book Guard (per-side TTL, default 3s; 30s clears all)│
│  - OFI Toxicity (Lead-Lag: provide both sides = 0.0)          │
│  - ACTION: If unhealthy, set target price to 0.0 (Cancel)     │
├─────────────────────────────────────────────────────────────┤
│ Layer 2: Inventory & Hedge Control (state_unified)            │
│  - net_diff != 0 -> Calculate dynamic hedge (size=abs_net)    │
│  - Apply Pair Target or Emergency Max Cost ceilings           │
│  - Inventory gate: can_buy_* applies to hedge and provide     │
├─────────────────────────────────────────────────────────────┤
│ Layer 3: Passive Liquidity (Provide)                          │
│  - Apply A-S Skew + Gabagool Cost Averaging                   │
│  - bid_yes = mid_yes - excess/2 - skew_shift                  │
│  - bid_no  = mid_no  - excess/2 + skew_shift                  │
└─────────────────────────────────────────────────────────────┘
```

#### A-S 报价公式详解

```
mid_yes = (yes_bid + yes_ask) / 2
mid_no  = (no_bid + no_ask) / 2
excess  = max(0, mid_yes + mid_no - pair_target)   # 超出配对目标的部分

skew = net_diff / max_net_diff  ∈ [-1, 1]          # 归一化库存偏斜

# 时间衰减（Opt-1）：
decay = 1 + as_time_decay_k * elapsed_fraction     # 1.0~3.0 随临期增大

effective_as_factor = as_skew_factor * decay
skew_shift = skew * effective_as_factor

bid_yes = mid_yes - excess/2 - skew_shift
bid_no  = mid_no  - excess/2 + skew_shift

# 约束：
bid_yes = min(bid_yes, yes_ask - tick)   # 严格 Maker 约束
bid_no  = min(bid_no,  no_ask  - tick)   # 严格 Maker 约束
bid_yes, bid_no ∈ [tick, 1-2*tick]      # 边界安全钳位
```

**盈利条件**: `bid_yes + bid_no ≤ pair_target < 1.0`，成交后锁定利润。

#### Gabagool22 对冲公式

```
#### Gabagool22 对冲公式 (含紧急救火模式)

```
# 1. 确定对冲天花板 (Hedge/Rescue)
# 正常风险区间内不引入损失；超出最大净仓差时直接进入救火线
hedge_target = if abs(net_diff) >= max_net_diff {
                 max_portfolio_cost
               } else {
                 pair_target
               }

# 2. 动态下单规模 (Hedge Sizing)
size = net_diff.abs()

# 3. 计算对冲价格
no_ceiling  = hedge_target - yes_avg_cost
bid_no      = min(no_ceiling, no_ask - tick) 

# 3. 盈亏评估：
# 盈利模式 (target=0.99)：profit ≥ 1.0 - 0.99 = +$0.01/股 (保证盈利)
# 救火模式 (target=1.02)：profit ≥ 1.0 - 1.02 = -$0.02/股 (允许微亏以平仓)
```

> **注意**：`pair_target`（默认 0.99）是正常状态下的利润线。
> `max_portfolio_cost`（默认 1.02）是库存满载时的紧急救火线下。
> `max_portfolio_cost` 会被 `PM_MAX_LOSS_PCT`（默认 0.02）自动钳制。
> 这种设计保证了：在正常波动中我们只做赚钱的买卖；在极端行情中，我们愿意付出极小代价 (2%) 来关掉面临 100% 归零风险的大额单边敞口。

**核心洞察**：A-S 网格一路摊薄 `yes_avg_cost`，越低则 `no_ceiling` 越高（越贴近市价），即便在救火模式下，由于成本已被摊薄，往往最终依然能实现保本或微利离场。

### 1.3 OFI 引擎分析

#### OFI 计算与市场解读

```
OFI_YES = buy_volume_YES - sell_volume_YES  (3秒窗口内)

OFI_YES > threshold (+200) → 大量买入 YES → 市场预期价格上涨
    策略：YES 价格可能被推高，挂的 YES 买单风险增加 (对手盘知情)
    → 全部撤单 (防止以过低价格卖给知情者)

OFI_YES < -threshold (-200) → 大量抛售 YES → 价格下跌信号
    若无仓位：撤单防空接飞刀
    若有 YES 仓位：仅当 NO 侧健康且 can_buy_no=true 时挂对冲单；否则清空提供单并等待恢复
```

**Lead-Lag 逻辑**：YES 和 NO 是互补的（YES + NO = $1），两者并非独立市场。YES 的毒性流必然预示 NO 的反方向压力，因此任何一侧毒性触发全局 Kill。

#### 自适应阈值（Opt-2）的价值

| 市场状态 | 固定阈值行为 | 自适应阈值行为 |
|---------|------------|--------------|
| 低流动性（每分钟几笔） | 偶发小波动触发误 Kill | 阈值自动下调，更敏感 |
| 高流动性（每秒数十笔） | 正常流量触发频繁 Kill | 阈值自动上调，减少误 Kill |
| 重大事件冲击 | 正确触发 | 阈值已预热，正确触发 |

---

## 二、风险矩阵

### 2.1 主要风险类型

| 风险类型 | 触发条件 | 保护机制 | 残余风险 |
|---------|---------|---------|---------|
| 知情交易 | 大户掌握非公开信息 | OFI Strategy-First Kill Switch | 毫秒级延迟内 |
| 单侧库存积累 | 市场单向波动 | A-S Skew + abs_net 对冲（受 can_buy_* 约束） | 流动性枯竭时 |
| 盘口数据过期 | WS 断连或流动性极低 | Configurable TTL (default 3s) | 频繁重连期间 |
| 重连重放填单 | WS 断线重连 | DedupCache TTL 去重 | TTL 外的极端延迟 |

### 2.2 最坏情况分析

**场景：市场临期前大幅单边跳空**

1. YES 价格从 0.50 骤跌至 0.20（坏消息突然公布）
2. 系统已持有 10 股 YES @ avg_cost=0.45（过去通过 A-S 网格积累）
3. `no_ceiling = pair_target - yes_avg_cost = 0.99 - 0.45 = 0.54`
4. 市场 NO 价格同步跳涨：NO ask = 0.75（行情反转，NO 变贵）
5. 系统实际挂单：`aggressive_price(ceiling=0.54, ask=0.75) = min(0.54, 0.74) = 0.54`
   → **系统绝不会挂 NO@0.74**，天花板 0.54 是硬限制

**真正的最坏情况：对冲单无法成交**

| 情形 | 结果 |
|-----|------|
| NO@0.54 成功对冲 | 利润 = 1.0 - (0.45 + 0.54) = **+$0.01/股** ✓ |
| NO ask 跳到 0.75，没人接 0.54 | 对冲单**挂在那里但无法成交** |
| 市场到期，YES 结果为 NO 获胜 | YES token 价值归零，NO 获 $1 但我们没有 NO |
| 最终损失 | 10 × $0.45（买 YES 成本）= **-$4.50**（全亏） |

对冲天花板 0.54 本身**不会**造成亏损——问题是当 NO 市价（0.75）远高于天花板时，系统的 post-only 属性决定了**不能主动 taker 买 NO 平仓**，只能等待市价回落到 0.54 以下。若市场到期前未能回归，则面临全额亏损。

**防御机制**：
- A-S 网格从更低价位积累 YES，`yes_avg_cost` 越低 → `no_ceiling` 越高 → 对冲成功概率越大
- OFI Kill Switch 在单边跳空前撤单（前提是流量异常已被检测到）
- `max_net_diff` 限制最大 YES 仓位（默认 10 股），限制了最大亏损额
- 时间衰减（Opt-1）在临期前加大 skew 惩罚，减少 YES 积累

---

## 三、最佳操作实践

### 3.1 市场选择

**优先选择**：
- 活跃市场（每 5-15 分钟至少 10-20 笔成交）
- Spread 合理：`(yes_ask - yes_bid)` 和 `(no_ask - no_bid)` 各自 ≥ 2 ticks
- 明确到期时间（`btc-updown-5m`, `btc-updown-15m` 等周期性市场）
- 双边均有流动性（避免单边盘口枯竭的市场）

**避免**：
- 结果即将公布（最后 2 分钟停止入场，见 `PM_ENTRY_DEADLINE`）
- 极低流动性（单次成交 < 1 USDC）
- 政治类/主观事件市场（知情交易风险高，OFI 不稳定）

### 3.2 参数配置指南

#### 资金规模对应配置

| 资金规模 | BID_SIZE | MAX_NET_DIFF | MAX_SIDE_SHARES | PAIR_TARGET |
|--------|----------|-------------|-------------------|-------------|
| $50    | 2           | 5           | $20               | 0.98        |
| $100   | 5           | 10          | $50               | 0.98        |
| $500   | 10          | 20          | $250              | 0.985       |
| $2000  | 20          | 40          | $1000             | 0.99        |

> 注：动态计算公式 `BID_SIZE = balance * PM_BID_PCT (default 2%)`, `MAX_NET_DIFF = balance * PM_NET_DIFF_PCT (default 10%)`

#### 关键参数调优

```bash
# 激进模式（高流动性市场，追求更高成交率）
PM_AS_SKEW_FACTOR=0.01      # 减小 A-S 惩罚，双边价格更有竞争力
PM_DEBOUNCE_MS=200          # 缩短防抖，更快跟随盘口变化
PM_OFI_TOXICITY_THRESHOLD=300  # 提高阈值，减少误 Kill

# 保守模式（低流动性市场，优先保护本金）
PM_AS_SKEW_FACTOR=0.05      # 加大 A-S 惩罚，库存倾斜时报价快速收紧
PM_DEBOUNCE_MS=800          # 延长防抖，减少 API 调用
PM_OFI_TOXICITY_THRESHOLD=100  # 降低阈值，更敏感的毒性检测

# 高频市场（使用自适应阈值）
PM_OFI_ADAPTIVE=true        # 启用自适应
PM_OFI_ADAPTIVE_K=2.5       # 均值+2.5σ，比默认3σ稍激进

# 临期加速清仓
PM_AS_TIME_DECAY_K=3.0      # 最后时刻达到4×基础skew（比默认2.0更激进）
PM_HEDGE_DEBOUNCE_MS=50     # 极端情况减少对冲等待时间
```

### 3.3 启动前检查清单

```
□ 确认 .env 文件存在且包含正确的 API 密钥
□ 确认余额充足（建议至少 5× BID_SIZE 的 USDC.e）
□ 确认 PM_DRY_RUN=false（非演习时）
□ 目标市场已解析（slug 格式正确）
□ ENTRY_DEADLINE 配置合理（建议 2-5 分钟）
□ 检查最近运行的 stale orders（系统启动时自动 CancelAll）
□ 在 dry-run 模式下确认日志输出正常，再切换实盘
```

### 3.4 运行中监控

#### 关键日志信号

| 日志关键词 | 含义 | 应对 |
|----------|------|------|
| `☠️ YES/NO entered toxicity` | OFI 触发，正在撤单 | 正常保护，观察恢复 |
| `✅ YES/NO flow recovered` | OFI 恢复正常 | 系统自动恢复报价 |
| `⚡ DIRECT KILL from OFI` | 直通 Kill 信道触发 | 表示毒性发生在两个 book tick 之间 |
| `🔧 HEDGE NO/YES@` | 对冲单已发出 | 正常流程，注意价格是否合理 |
| `⏳ OMS: 冷却 30s` | 余额/授权不足 | 检查账户余额和授权额度 |
| `⚠️ Stale book (>30s)` | 盘口数据超时 | 可能是 WS 问题，检查网络 |
| `⚠️ CancelSide: N/M 取消失败` | 撤单部分失败 | 检查 API 限速或网络问题 |
| `📦 Fill: Yes/No` | 成交确认 | 确认金额和方向符合预期 |

#### 健康指标

```bash
# 良好运行时：
- Placed/Tick 比率 < 0.3（每个 tick 不频繁重新报价）
- cancel_toxic/placed 比率 < 0.1（偶发毒性 Kill）
- skipped_debounce/ticks 比率 0.3-0.7（正常防抖工作中）
- 0 个 skipped_empty_book（盘口正常）

# 警示信号：
- cancel_toxic/placed > 0.3 → OFI 阈值可能过低，考虑调高或启用自适应
- skipped_inv_limit 持续增加 → 库存积压，考虑降低 max_net_diff 或等市场改善
- placed 长期为 0 → 盘口异常或撤单循环，检查日志
```

### 3.5 市场轮换策略（prefix mode）

```bash
# 设置 slug 为前缀模式：
PM_MARKET_SLUG=btc-updown-5m  # 自动轮换 5 分钟 BTC 涨跌市场

# 系统在当前市场结束前30秒预加载下一个市场
# 轮换间隙 < 1 秒，最大程度减少空档期
```

**最佳实践**：
- 使用 5m 或 15m 的周期性市场，流动性稳定、轮换规律
- 避免同时运行多个 slug 实例（API 限速）
- 在市场活跃时段运行（美股交易时间 UTC 14:30-21:00 for 加密预测市场）

### 3.6 资金安全最后防线

1. **不要用不能接受亏损的资金**：在极端市场（单边走势 + 低流动性）下系统仍可能亏损
2. **定期检查 claimable 仓位**：`PM_AUTO_CLAIM=true` 自动领取已结算市场盈利
3. **监控 portfolio_cost**：若 `yes_avg_cost + no_avg_cost > 0.99`，配对必然亏损（检查参数）
4. **干演习（dry-run）新市场**：每次进入新类型市场，先跑 1-2 轮 dry-run 确认定价逻辑
5. **设置系统监控**：进程意外退出时发送告警（系统关机前已自动 CancelAll）

---

## 四、Post-only 与 A-S 模型的设计张力

### 4.1 经典 A-S 的核心假设

标准 Avellaneda-Stoikov 模型建立在三个前提之上：

1. **对称双边报价**：做市商同时维护 bid 和 ask，从两侧成交循环捕获价差
2. **连续库存管理**：库存过重时可通过调整 ask 报价，等待客户方自然消化
3. **Taker 兜底**：理论上当库存极端时，做市商可主动 taker 平仓（接受滑点换取确定性）

### 4.2 本系统的三处偏差

| 维度 | 经典 A-S | 本系统 | 影响 |
|-----|---------|--------|------|
| 报单方向 | bid + ask 双边 | 只挂 bid（两侧都是买入） | 无法通过挂 ask 卖出库存 |
| 平仓路径 | 挂 ask 让对手接走 | 必须买入对面配对（YES↔NO） | 对冲依赖对手方愿意接 |
| 紧急出逃 | 可主动 taker 市价平仓 | 严格 post-only，**无强制平仓路径** | 跳空时只能等待，无法止损 |

### 4.3 额外风险来源

**风险 1：对冲不可强制执行**

经典 A-S 理论保证：在足够时间内，偏斜的报价必然吸引对手盘成交。这依赖"只要价格足够有吸引力，就有人来"——即 Poisson 到达过程的基本假设。

但在 Polymarket 跳空场景中：
- YES 从 0.50 跳到 0.20，NO 从 0.50 跳到 0.75
- 我们的 NO 天花板 0.54 远低于市场（0.75）
- 没有人愿意以 0.54 卖 NO——合理价格对对手方根本没有吸引力
- **Poisson 到达假设在跳空时失效**

经典 A-S 的应对是：`以市价买入 NO`（taker），以确定性换滑点。我们做不到。

**风险 2：积累模式的非对称性**

经典做市商的 P&L 来自双边价差循环：买@bid→持仓→卖@ask，每个周期锁定一个 spread。不持有单方向敞口。

本系统是**单向积累 + 配对退出**：
- 积累阶段：持续买入 YES（或 NO），承担单边敞口
- 退出阶段：等待对面配对成交，然后整对锁定利润

在退出之前，仓位始终是单向的——这比经典做市商承受更高的方向性风险。

**风险 3：Post-only 被对手方利用**

我们的挂单价格对所有人可见。知情交易者（informed traders）可以：
- 看到我们的 YES bid@0.47
- 在掌握坏消息后，立即以市价卖出 YES，恰好成交我们的单子
- 我们被迫以 0.47 接了明知会跌的 YES

OFI Kill Switch 正是为此设计：**在知情交易流出现时，提前撤单**。但它能检测到的是"已经发生的流量异常"，对于突然的新闻事件（跳空开盘）仍有盲区。

### 4.4 Post-only 在 Polymarket 语境下的合理性

尽管存在上述偏差，post-only 在这个特定场景中仍是合理选择：

**结构性优势**：
- Polymarket maker fee = 0%（有时有 rebate），taker 则支付费用。挂单成本为零
- 市场本身流动性较薄，taker 会显著推高自己的成交成本
- 二元期权不存在"卖出 YES"赚价差的机制——到期 YES 必然价值 $1 或 $0，中间挂 ask 卖出意味着我们主动放弃 YES 敞口，这不是价差套利而是方向性交易

**A-S 精神的延续**：

尽管形式不同，A-S 的核心思想仍然被保留：
- **库存倾斜**：YES 持仓重时，降低 YES bid（减少积累），提高 NO bid（加速配对）→ 这正是 A-S skew 的直接应用 ✓
- **价差保护**：`bid_yes + bid_no ≤ pair_target < 1.0` 保证每对利润，等价于经典 A-S 的 spread capture ✓
- **时间紧迫性**：临期加大 skew 惩罚，等价于 A-S 模型中 `γσ²(T-t)` 项在 T→0 时的增大 ✓

**更准确的定位**：本系统是 **"成本摊薄型配对套利"**，借用了 A-S 的库存管理思想，但本质上是一种受 OFI 和库存限制约束的**条件性套利策略**，而非经典做市。理解这一区别有助于合理设置预期：在市场跳空且无法对冲时，接受单次市场的亏损是策略正常风险的一部分，通过大量成功配对的积累来正期望。

---

## 六、策略局限性与未来改进方向

### 6.1 已知局限

1. **无跨市场对冲**：仅在单一市场内做市，无法利用关联市场（如同时做多个 BTC 区间市场）
2. **OFI 窗口固定**：3 秒窗口对快速反转市场可能过短，对慢速市场可能过敏
3. **无成交量预测**：不知道每轮市场预期成交量，无法动态调整 bid_size
4. **单线程报价**：同一时刻只处理一个 BookTick，高频市场下可能堆积延迟

### 6.2 潜在改进

1. **多市场并行**：并行运行多个不相关市场，分散单市场风险
2. **动态 bid_size**：基于市场成交量动态调整，高流量时加大，低流量时减小
3. **OFI 预测**：记录历史 OFI 模式，在毒性爆发前提前收紧报价
4. **Gas/Fee 感知**：考虑 Polymarket 的 maker fee 结构，在薄利市场自动降低风险

---

## 七、环境变量完整参考

```bash
# 必填
PM_PRIVATE_KEY=0x...          # 签名私钥
PM_FUNDER_ADDRESS=0x...        # 资金地址（可与签名地址相同）
PM_MARKET_SLUG=btc-updown-5m  # 市场前缀或完整 slug

# 运行模式
PM_DRY_RUN=false              # false=实盘, true=演习（默认 true）
PM_AUTO_CLAIM=true            # 自动领取结算仓位

# 核心策略参数
PM_PAIR_TARGET=0.98           # 配对成本上限（利润空间 = 1 - pair_target）
PM_BID_SIZE=5.0               # Size per bid (Shares)
PM_MAX_NET_DIFF=10.0          # 最大净仓量（单股）
PM_MAX_SIDE_SHARES=50.0       # 单侧最大持仓股数上限
PM_MAX_PORTFOLIO_COST=1.02    # 组合成本上限
PM_MAX_LOSS_PCT=0.02          # 最大可接受亏损比例（2%）
PM_AS_SKEW_FACTOR=0.03        # A-S 库存倾斜系数（0=纯 Gabagool22）
PM_AS_TIME_DECAY_K=2.0        # 时间衰减系数（0=禁用）
PM_DEBOUNCE_MS=500            # 正常防抖时间
PM_HEDGE_DEBOUNCE_MS=100      # 对冲防抖时间（建议 ≤ 200ms）
PM_REPRICE_THRESHOLD=0.010    # 触发重新报价的价格偏差阈值
PM_STALE_TTL_MS=3000          # 盘口数据 TTL（毫秒，单侧）

> 注：`PM_MAX_POSITION_VALUE` 已弃用，若仍设置将被视为 `PM_MAX_SIDE_SHARES`（单位为 shares）。

# 动态规模（基于账户余额自动计算）
PM_BID_PCT=0.02               # bid_size = balance * 2%
PM_NET_DIFF_PCT=0.10          # max_net_diff = balance * 10%

# OFI 参数
PM_OFI_WINDOW_MS=3000         # 滑动窗口长度（毫秒）
PM_OFI_TOXICITY_THRESHOLD=200 # 毒性阈值（未启用自适应时使用）
PM_OFI_HEARTBEAT_MS=200       # OFI 心跳间隔
PM_OFI_ADAPTIVE=false         # 启用自适应阈值
PM_OFI_ADAPTIVE_K=3.0         # 自适应系数（均值+Kσ）
PM_OFI_ADAPTIVE_MIN=50.0      # 自适应阈值下限
PM_OFI_ADAPTIVE_MAX=1000.0    # 自适应阈值上限

# 市场控制
PM_TICK_SIZE=0.01             # 报价最小单位
PM_ENTRY_DEADLINE=120         # 距到期多少秒内停止新建仓（建议 60-300）
```
