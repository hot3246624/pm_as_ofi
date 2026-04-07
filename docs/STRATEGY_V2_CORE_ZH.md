# 策略核心手册（当前主线）

本文档定义当前仓库的正式运行主线。

## 1. 当前结论

当前推荐实盘主线是：
- `PM_STRATEGY=pair_arb`
- 收益验证优先 `btc-updown-15m`（`5m` 仅用于机制冒烟）
- 正常盘中走双边 maker buy-only
- 去掉方向对冲 overlay 与尾盘强制市价去风险

其它策略仍可运行，但不再作为默认实盘路线。

## 2. 系统分层

### 2.1 策略层
负责回答一件事：
- 当前 tick 想挂哪些 maker 单

`pair_arb` 主线只输出两个买入槽位：
- `YesBuy`
- `NoBuy`

执行层统一仍支持四槽位，这是共享基础设施能力，不代表当前主线策略会用到四槽位。

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
- 共享 OFI 引擎
- stale book
- SoftClose / HardClose / Freeze
- recycle / claim

## 3. 共享系统边界

本手册不承担 `glft_mm` 独立策略规格书的角色。  
`glft_mm` 的专属说明见：
- `docs/STRATEGY_GLFT_MM_ZH.md`

这里仅保留所有策略共享的系统边界。

### 3.1 共享库存与风险单位

- `PM_BID_SIZE`：单次基础下单份额
- `PM_MAX_NET_DIFF`：盘中净仓硬上限
- `PM_PAIR_TARGET`：共享 outcome-floor 参考线

说明：
- `PM_MAX_PORTFOLIO_COST` 是旧 hedge / rescue 路径兼容参数，不属于当前 `pair_arb` 主路径。

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
- 共享 OFI 信号引擎
- stale book
- endgame phase gate
- recycle / claim

## 4. OFI 的共享角色
OFI 现在分成两层：

1. 共享层只负责维护 OFI 引擎本身
- `heat`
- `toxicity`
- `saturated`
- regime-aware baseline 与恢复语义

2. 各策略自己决定如何消费 OFI

当前主线 `pair_arb` 的语义是：
- OFI 不再由执行层对 normal-phase buy 做常态硬拦截
- OFI 只在策略层塑形 same-side risk-increasing buy
- pairing / risk-reducing buy 完全忽略 OFI

这意味着：
- 共享层提供的是统一信号，不是统一的交易裁决
- `pair_arb` 的主目标仍然是 `pair_cost / paired_locked_pnl`
- OFI 只是 subordinate 风险塑形器，不覆盖主目标函数

## 5. 盘中硬风险边界

盘中唯一核心硬风险上限仍是：
- `PM_MAX_NET_DIFF`

在此之上，还有两个共享门：
- outcome floor：禁止新增 Buy 把最差结算结果继续挖深
- stale / source gate：禁止继续在坏数据里冒险

这意味着当前系统的风险哲学是：
- 正常盘中靠库存偏移和价差控制风险
- 极端情况下靠统一门禁和共享安全阀止血

## 6. 尾盘是共享能力，不是 `pair_arb` 主路径

系统层仍保留尾盘阶段，但 `pair_arb` 当前主线路径不再依赖：
- HardClose maker-repair
- ForceTaker
- 市价强制对冲

`pair_arb` 当前核心是盘中 pair-cost + inventory 语义，不靠尾盘补丁修收益。

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
  - 当前验证主线
- `dip_buy` / `phase_builder`
  - 实验策略
  - 不纳入当前正式上线文档主线

## 9. 当前实盘结论

如果你的目标是“现在进入收益验证”，当前仓库应按下面的理解使用：

1. 主策略选 `pair_arb`
2. 先 `5m` 冒烟（机制），再 `15m` 验证（收益）
3. 参数冻结跑满样本轮次，再做 go/no-go，不中途调参
4. `glft_mm` 留作 challenger，不和主线混跑
