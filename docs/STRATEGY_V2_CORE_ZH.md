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

## 3. 主策略 `glft_mm`

### 3.1 适用边界

`glft_mm` 只服务以下市场：
- `btc-updown-5m`
- `eth-updown-5m`
- `xrp-updown-5m`
- `sol-updown-5m`

原因很直接：
- 它强依赖 Binance `aggTrade` 外部锚
- 需要有足够密集的 Polymarket trade-flow 才能做 `A/k` 拟合

Binance stale 时：
- `glft_mm` 直接静默
- 清空四槽位
- 不回退到纯 Polymarket 内生报价

### 3.2 快变量

`GlftSignalEngine` 持续产出一个快照，至少包含：
- `anchor_prob`
- `basis_prob`
- `alpha_flow`
- `sigma_prob`
- `tau_norm`
- `fit_a`
- `fit_k`
- `fit_quality`
- `stale`

定义：
- `anchor_prob`：Binance 相对本轮开盘价的概率锚
- `basis_prob`：`poly_mid - anchor_prob` 的 EWMA 偏差修正
- `alpha_flow`：Polymarket 本地 trade-flow OFI
- `sigma_prob`：概率空间波动率慢变量
- `tau_norm`：剩余时间占比

### 3.3 慢变量拟合

`IntensityEstimator` 使用市场逐笔成交代理拟合：
- 周期：`10s`
- 窗口：`30s`
- bucket：`0..20` ticks
- 模型：`lambda(delta) = A * exp(-k * delta)`

质量门控：
- 相关 trade 数 `>= 20`
- 非空 bucket 数 `>= 3`
- `A > 0`
- `k > 0`
- `R² >= 0.60`

只有 `Ready` 拟合才会覆盖 `last_good_fit`。
新拟合不合格时：
- 继续沿用 `last_good_fit`
- 没有历史有效值时，使用 bootstrap / warm-start

### 3.4 报价数学

库存先做标准化：
- `q_norm = clamp(net_diff / max_net_diff, -1, 1)`

然后用：
- `p_anchor = clamp(anchor_prob + basis_prob, tick, 1-tick)`
- `alpha_prob = PM_GLFT_OFI_ALPHA * alpha_flow`
- `compute_optimal_offsets(...) -> inventory_shift + half_spread_base`

最终：
- `spread_mult = 1 + PM_GLFT_OFI_SPREAD_BETA * |alpha_flow|^2`
- `half_spread = max(tick, half_spread_base * spread_mult)`
- `r_yes = clamp(p_anchor + alpha_prob - inventory_shift, tick, 1-tick)`
- `r_no = 1 - r_yes`

四槽位原始中心：
- `YesBuy = r_yes - half_spread`
- `YesSell = r_yes + half_spread`
- `NoBuy = r_no - half_spread`
- `NoSell = r_no + half_spread`

之后统一经过：
- tick 对齐
- post-only 安全垫
- keep-if-safe / governor
- inventory gate
- OFI / stale / endgame

### 3.5 Quote Governor

`glft_mm` 额外加了一层专属 governor，避免参数快跳直接放大成撤改单风暴：
- reservation price 单次最多移动 `1 tick`
- half spread 单次最多移动 `1 tick`
- 超出的变化要分多个 tick 逐步靠拢

它和 keep-if-safe 配合，目标不是“反应最快”，而是“快且不抖”。

## 4. OFI 的最终角色

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

## 6. 尾盘不是主策略

尾盘逻辑保留，但它只是 safety valve：
- `SoftClose`：停风险增加型意图，允许风险不增的修复
- `HardClose`：优先 maker repair，不足时 taker de-risk
- `Freeze`：禁止新增风险，允许去风险

因此：
- `glft_mm` 不能靠尾盘逻辑赚钱
- 尾盘逻辑只负责在主策略失效时收口风险

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
  - buy-only 基线
  - 适合回放、对照和研究
- `gabagool_corridor`
  - corridor 变体
  - 当前仍是 research，不作为默认 live 主线
- `pair_arb`
  - 旧 fair-value maker
  - 仅保留历史比较价值
- `dip_buy` / `phase_builder`
  - 实验策略
  - 不纳入当前正式上线文档主线

## 9. 当前实盘结论

如果你的目标是“现在就准备 5m 实盘测试”，当前仓库应按下面的理解使用：

1. 主策略选 `glft_mm`
2. `.env.example` 直接作为模板起点
3. 优先关注执行稳定性、OFI 阈值行为和 Binance 外锚稳定性
4. 其它策略不要再和当前主线混用
