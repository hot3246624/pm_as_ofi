# `glft_mm` 策略规格书

本文档只描述当前 `glft_mm` 策略本身，不重复共享执行层和共享风控细节。

## 1. 定位

`glft_mm` 是当前仓库唯一推荐 live 主线：
- 真双边
- 四槽位
- maker-first
- 仅服务 `btc/eth/xrp/sol` 的 `*-updown-5m`

它不是 buy-only 策略，也不是旧的 `A-S + hedge` 拼接器。

## 2. 输入信号

### 2.1 外部锚

来自 Binance `aggTrade`：
- `anchor_prob`
- 本轮开盘价 `S0`
- 当前价格 `St`

当 Binance stale 时：
- `glft_mm` 直接静默
- 清空四槽位
- 不回退到纯 Polymarket 定价

### 2.2 内部慢变量

来自 `GlftSignalEngine` / `IntensityEstimator`：
- `basis_prob`
- `sigma_prob`
- `tau_norm`
- `fit_a`
- `fit_k`
- `fit_quality`

拟合节奏：
- `10s refit`
- `30s window`

只有 `Ready` 拟合才会覆盖 `last_good_fit`。

### 2.3 本地 OFI

`alpha_flow` 来自 Polymarket trade-flow OFI。

它在 `glft_mm` 中做两件事：
- 进入 reservation price 偏移
- 进入价差放大

它不直接改写 `fit_k`。

## 3. 报价数学

### 3.1 库存标准化

`q_norm = clamp(net_diff / max_net_diff, -1, 1)`

### 3.2 中心价

- `p_anchor = clamp(anchor_prob + basis_prob, tick, 1-tick)`
- `alpha_prob = PM_GLFT_OFI_ALPHA * alpha_flow`

### 3.3 偏移与基础半价差

调用：
- `compute_optimal_offsets(q_norm, sigma_prob, tau_norm, fit, gamma, xi, bid_size, max_net_diff, tick_size)`

输出：
- `inventory_shift`
- `half_spread_base`

### 3.4 OFI 价差放大

- `spread_mult = 1 + PM_GLFT_OFI_SPREAD_BETA * |alpha_flow|^2`
- `half_spread = max(tick, half_spread_base * spread_mult)`

### 3.5 四槽位报价

- `r_yes = clamp(p_anchor + alpha_prob - inventory_shift, tick, 1-tick)`
- `r_no = 1 - r_yes`

然后：
- `YesBuy = r_yes - half_spread`
- `YesSell = r_yes + half_spread`
- `NoBuy = r_no - half_spread`
- `NoSell = r_no + half_spread`

最后统一经过：
- tick 对齐
- aggressive post-only 安全垫
- keep-if-safe
- governor
- inventory gate
- OFI gate

## 4. 真实执行行为

这是当前代码行为，不是理想蓝图。

### 4.1 正常盘中

`glft_mm` 输出四槽位 `Provide` 意图：
- `YesBuy`
- `YesSell`
- `NoBuy`
- `NoSell`

没有旧的 directional hedge overlay。

### 4.2 净仓门

硬约束仍是：
- `PM_MAX_NET_DIFF`

对应效果：
- 正净仓过大时，风险增加方向的槽位会被门禁挡掉
- 负净仓同理

### 4.3 Outcome floor

当前 `glft_mm` 仍共享 buy-side outcome floor：
- 所有新的 Buy 都要通过 floor
- Sell 不受该门限制

这就是为什么 `PM_PAIR_TARGET` 仍然有效：
- 它不再代表旧 hedge ceiling
- 但仍影响共享 outcome-floor

## 5. 尾盘真实行为

这里要和旧文档明确切开。

当前 `glft_mm` 的 slot 路径：
- **没有**接入旧的完整 `Keep / MakerRepair / ForceTaker` 尾盘状态机
- **没有**接入旧的 directional hedge rescue 路径

当前真实效果是：
- 一旦进入非 `Normal` phase
- 只允许“风险不增加”的 slot 意图继续存在
- 风险增加的 slot 会被清掉

因此：
- `PM_ENDGAME_SOFT_CLOSE_SECS` 对 `glft_mm` 有实效
- `PM_ENDGAME_HARD_CLOSE_SECS` / `PM_ENDGAME_FREEZE_SECS` 会改变 phase 名称，但当前 slot 路径没有更细的差异化行为
- `PM_ENDGAME_MAKER_REPAIR_MIN_SECS`
- `PM_ENDGAME_EDGE_KEEP_MULT`
- `PM_ENDGAME_EDGE_EXIT_MULT`
  
以上三个参数当前对 `glft_mm` 基本不生效

## 6. 当前真正生效的主参数

### 6.1 策略参数

- `PM_STRATEGY=glft_mm`
- `PM_BID_SIZE`
- `PM_MAX_NET_DIFF`
- `PM_PAIR_TARGET`
- `PM_TICK_SIZE`

### 6.2 GLFT 参数

- `PM_GLFT_GAMMA`
- `PM_GLFT_XI`
- `PM_GLFT_OFI_ALPHA`
- `PM_GLFT_OFI_SPREAD_BETA`
- `PM_GLFT_INTENSITY_WINDOW_SECS`
- `PM_GLFT_REFIT_SECS`

### 6.3 执行治理参数

- `PM_POST_ONLY_SAFETY_TICKS`
- `PM_POST_ONLY_TIGHT_SPREAD_TICKS`
- `PM_POST_ONLY_EXTRA_TIGHT_TICKS`
- `PM_REPRICE_THRESHOLD`
- `PM_DEBOUNCE_MS`

### 6.4 OFI 参数

- `PM_OFI_WINDOW_MS`
- `PM_OFI_TOXICITY_THRESHOLD`
- `PM_OFI_ADAPTIVE`
- `PM_OFI_ADAPTIVE_K`
- `PM_OFI_ADAPTIVE_MIN`
- `PM_OFI_ADAPTIVE_MAX`
- `PM_OFI_ADAPTIVE_RISE_CAP_PCT`
- `PM_OFI_ADAPTIVE_WINDOW`
- `PM_OFI_RATIO_ENTER`
- `PM_OFI_RATIO_EXIT`
- `PM_OFI_HEARTBEAT_MS`
- `PM_OFI_EXIT_RATIO`
- `PM_OFI_MIN_TOXIC_MS`
- `PM_TOXIC_RECOVERY_HOLD_MS`

### 6.5 共享运营参数

- `PM_STALE_TTL_MS`
- `PM_ENTRY_GRACE_SECONDS`
- `PM_RECONCILE_INTERVAL_SECS`
- `PM_COORD_WATCHDOG_MS`
- recycle / claim 相关参数

## 7. 当前不应误认为是 `glft_mm` 主参数的项

这些参数目前仍被代码保留，但不应写成 `glft_mm` 核心：

- `PM_MAX_PORTFOLIO_COST`
  - 属于旧 hedge / rescue ceiling 语义
  - 当前 `glft_mm` 正常盘中不走这条路
- `PM_HEDGE_DEBOUNCE_MS`
  - `glft_mm` 正常盘中不走 hedge path
- `PM_MIN_HEDGE_SIZE`
- `PM_HEDGE_ROUND_UP`
- `PM_HEDGE_MIN_MARKETABLE_*`
  - 都属于旧 hedge/taker 辅助参数
- `PM_ENDGAME_MAKER_REPAIR_MIN_SECS`
- `PM_ENDGAME_EDGE_KEEP_MULT`
- `PM_ENDGAME_EDGE_EXIT_MULT`
  - 当前 slot 路径没有真正用到它们

## 8. 当前结论

如果你问“现在的 `glft_mm` 到底是什么”：

答案是：
- 一个以 Binance 为外锚
- 以 GLFT 为中心报价模型
- 以 OFI 为 alpha/spread 调制和 kill-switch
- 以四槽位 maker 执行为主体
- 但尾盘仍只接了最小 phase gate 的系统

这比旧买入型主线更像一个真正的做市策略，但尾盘与去风险链路仍有继续收敛空间。
