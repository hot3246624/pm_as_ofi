# 策略核心手册（当前主线）

本文档定义当前仓库的正式运行主线。

## 1. 当前结论

当前推荐实盘主线是：
- `PM_STRATEGY=glft_mm`
- 仅运行在 `btc/eth/xrp/sol` 的 `*-updown-5m`
- 正常盘中走真双边 maker
- 共享尾盘、安全阀、回收和 claim，不再让尾盘逻辑承担主策略修复职责

其它策略仍可运行，但不再作为默认实盘路线。

## 2. 系统分层

### 2.1 策略层
负责回答一件事：
- 当前 tick 想挂哪些 maker 单

当前统一输出四个槽位：
- `YesBuy`
- `YesSell`
- `NoBuy`
- `NoSell`

### 2.2 执行层
负责回答另一件事：
- 这些单现在能不能挂、要不要保留、要不要撤、要不要改

执行层统一处理：
- post-only 安全垫
- keep-if-safe
- reprice/cancel
- OMS / Executor 生命周期
- reconcile

### 2.3 风控层
统一处理：
- `PM_MAX_NET_DIFF`
- outcome floor
- OFI slot-level kill-switch
- stale book
- SoftClose / HardClose / Freeze
- recycle / claim

## 3. 共享系统边界

本手册不再承担 `glft_mm` 独立策略规格书的角色。  
`glft_mm` 的专属说明见：
- `docs/STRATEGY_GLFT_MM_ZH.md`

这里仅保留所有策略共享的系统边界。

### 3.1 共享库存与风险单位

- `PM_BID_SIZE`：单次基础下单份额
- `PM_MAX_NET_DIFF`：盘中净仓硬上限
- `PM_PAIR_TARGET`：共享 outcome-floor 参考线

说明：
- `PM_MAX_PORTFOLIO_COST` 是旧 hedge / rescue 路径的兼容参数，不属于当前 `glft_mm` 正常盘中主逻辑。

### 3.2 共享执行治理

所有策略统一经过：
- tick 对齐
- post-only 安全垫
- keep-if-safe
- reprice/cancel
- OMS / Executor / reconcile

### 3.3 共享风控

共享风控层统一处理：
- `PM_MAX_NET_DIFF`
- outcome floor
- OFI slot-level / side-level 抑制
- stale book
- endgame phase gate
- recycle / claim

## 4. OFI 的共享角色
OFI 现在不是主策略本身，而是共享执行保护层。

它做三件事：
- 进入 `alpha_prob`
- 进入 `spread_mult`
- 作为 slot-level kill-switch 抑制最危险的槽位

语义：
- `YES` 买压强：优先抑制 `YesSell`
- `YES` 卖压强：优先抑制 `YesBuy`
- `NO` 对称处理

为避免误杀，当前实现还有：
- 2-heartbeat 才正式进入 toxic
- adaptive threshold 心跳更新
- rise cap
- robust clipping + mean reversion

## 5. 盘中硬风险边界

盘中唯一核心硬风险上限仍是：
- `PM_MAX_NET_DIFF`

在此之上，还有两个共享门：
- outcome floor：禁止新增 Buy 把最差结算结果继续挖深
- stale / toxic：禁止继续在坏数据或坏流里冒险

这意味着当前系统的风险哲学是：
- 正常盘中靠库存偏移和价差控制风险
- 极端情况下靠统一门禁和尾盘安全阀止血

## 6. 尾盘是共享安全阀，不是主策略

尾盘逻辑在系统层保留，但不同执行模式接入深度不同：
- buy-only / legacy hedge 路径：仍可进入更完整的 `Keep / MakerRepair / ForceTaker`
- `glft_mm` 当前 slot 路径：只把 `SoftClose/HardClose/Freeze` 作为 phase gate 使用，实际效果是“非 Normal 阶段禁止风险增加型 slot 意图”

因此：
- `glft_mm` 不能靠尾盘逻辑赚钱
- 当前 `glft_mm` 并没有独立的 HardClose maker-repair / taker-de-risk 数学分支
- 若未来要做更强尾盘处理，应作为 `glft_mm` 的专门增强项，而不是继续沿用旧 hedge 叙事

## 7. 共享资本循环

### recycle / merge
在余额或 allowance 压力下，系统走 batch merge 回收资金，而不是无限重试下单。

### auto-claim
回合结束后，系统按 `30s` SLA 窗口在后台重试 claim：
- 默认重试节奏：`0,2,5,9,14,20,27`
- 先本轮市场，再全局兜底
- 不阻塞下一轮开盘

## 8. 其它内置策略的定位

- `gabagool_grid`
  - buy-only utility 基线
  - 可直接运行，适合做 buy-only 对照
- `gabagool_corridor`
  - corridor 变体
  - 当前仍是 research，不作为默认 live 主线
- `pair_arb`
  - 当前最接近旧 `pair cost + A-S` 主线的可运行策略
  - 可直接运行，但不是当前主线
- `dip_buy` / `phase_builder`
  - 实验策略
  - 不纳入当前正式上线文档主线

## 9. 当前实盘结论

如果你的目标是“现在就准备 5m 实盘测试”，当前仓库应按下面的理解使用：

1. 主策略选 `glft_mm`
2. `.env.example` 直接作为模板起点
3. 优先关注执行稳定性、OFI 阈值行为、Binance 外锚稳定性，以及 slot 路径尾盘行为是否满足预期
4. 其它策略不要再和当前主线混用
