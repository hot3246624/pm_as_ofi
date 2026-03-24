# 配置参数手册

本文档描述当前 `.env.example` 的推荐模板值。  
注意：代码内部 fallback 默认值仍然偏保守，但实盘建议以模板为准。

## 1. 市场与认证

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `POLYMARKET_MARKET_SLUG` | `btc-updown-5m` | 当前推荐主战场 |
| `PM_BINANCE_SYMBOL_OVERRIDE` | unset | `glft_mm` 可选覆盖；默认按 slug 自动映射 |
| `POLYMARKET_PRIVATE_KEY` | empty | 实盘必填 |
| `POLYMARKET_FUNDER_ADDRESS` | empty | 实盘必填 |
| `POLYMARKET_API_KEY/SECRET/PASSPHRASE` | unset | 可选，留空则尝试派生 |
| `POLYMARKET_BUILDER_API_KEY/SECRET/PASSPHRASE` | unset | Safe claim / merge 需要 |
| `PM_SIGNATURE_TYPE` | `2` | Safe 模式推荐值 |

## 2. 运行控制

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_DRY_RUN` | `true` | 模板默认先演习 |
| `PM_ENTRY_GRACE_SECONDS` | `30` | 新市场开盘后的可入场窗口 |
| `PM_WS_CONNECT_TIMEOUT_MS` | `6000` | Market WS 连接超时 |
| `PM_WS_DEGRADE_MAX_FAILURES` | `12` | 连续失败后提前结束本轮 |
| `PM_RESOLVE_TIMEOUT_MS` | `4000` | Gamma 解析超时 |
| `PM_RESOLVE_RETRY_ATTEMPTS` | `4` | 解析重试次数 |
| `PM_RECONCILE_INTERVAL_SECS` | `30` | REST 对账周期 |
| `PM_COORD_WATCHDOG_MS` | `500` | 无行情时的风控心跳 |
| `PM_STRATEGY_METRICS_LOG_SECS` | `15` | 指标日志周期 |

## 3. 当前推荐策略模板（5m live 基线）

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_STRATEGY` | `glft_mm` | 当前唯一推荐 live 主线 |
| `PM_BID_SIZE` | `5.0` | 单槽位 clip |
| `PM_MAX_NET_DIFF` | `15.0` | 盘中净仓硬上限 |
| `PM_PAIR_TARGET` | `0.985` | 共享配对/尾盘安全阀参考成本线 |
| `PM_MAX_PORTFOLIO_COST` | `1.02` | 救火成本上限 |
| `PM_TICK_SIZE` | `0.01` | 价格粒度 |
| `PM_POST_ONLY_SAFETY_TICKS` | `2.0` | maker 安全垫基础退让 |
| `PM_POST_ONLY_TIGHT_SPREAD_TICKS` | `3.0` | 紧价差额外退让触发线 |
| `PM_POST_ONLY_EXTRA_TIGHT_TICKS` | `1.0` | 紧价差额外退让 |
| `PM_REPRICE_THRESHOLD` | `0.020` | 更保守的重报价阈值 |
| `PM_DEBOUNCE_MS` | `700` | provide 防抖 |
| `PM_HEDGE_DEBOUNCE_MS` | `100` | safety-valve hedge 防抖 |
| `PM_STALE_TTL_MS` | `3000` | 单侧 stale TTL |
| `PM_TOXIC_RECOVERY_HOLD_MS` | `1200` | toxic 恢复冷却 |

## 4. `glft_mm` 专属参数

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_GLFT_GAMMA` | `0.10` | inventory shift 风险厌恶系数 |
| `PM_GLFT_XI` | `0.10` | 终端惩罚；V1 推荐与 `gamma` 相同 |
| `PM_GLFT_OFI_ALPHA` | `0.30` | OFI 对 reservation price 的偏移系数 |
| `PM_GLFT_OFI_SPREAD_BETA` | `1.00` | OFI 对价差扩张的非线性乘子 |
| `PM_GLFT_INTENSITY_WINDOW_SECS` | `30` | 强度拟合窗口 |
| `PM_GLFT_REFIT_SECS` | `10` | 强度拟合周期 |

固定实现，不额外开放参数：
- warm-start TTL = `6h`
- bootstrap = `A=0.20, k=0.50, sigma=0.02, basis=0.0`
- `sigma_prob` 半衰期 = `20s`
- `basis_prob` 半衰期 = `30s`
- governor 步长 = `1 tick`

## 5. OFI 推荐值（当前 5m live 基线）

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_OFI_WINDOW_MS` | `3000` | 订单流窗口 |
| `PM_OFI_TOXICITY_THRESHOLD` | `300.0` | 自适应基准阈值 |
| `PM_OFI_ADAPTIVE` | `true` | 开启自适应 |
| `PM_OFI_ADAPTIVE_K` | `4.2` | `mean + k*sigma` 的 `k` |
| `PM_OFI_ADAPTIVE_MIN` | `120.0` | 自适应下限 |
| `PM_OFI_ADAPTIVE_MAX` | `1800.0` | 自适应上限；防阈值漂到失真区 |
| `PM_OFI_ADAPTIVE_RISE_CAP_PCT` | `0.20` | 每次心跳最大上升比例 |
| `PM_OFI_ADAPTIVE_WINDOW` | `200` | 自适应样本窗口 |
| `PM_OFI_RATIO_ENTER` | `0.70` | 进入 toxic 的比例门槛 |
| `PM_OFI_RATIO_EXIT` | `0.40` | 退出比例门槛 |
| `PM_OFI_HEARTBEAT_MS` | `200` | OFI 心跳 |
| `PM_OFI_EXIT_RATIO` | `0.85` | 滞回退出比 |
| `PM_OFI_MIN_TOXIC_MS` | `800` | 单次 toxic 最短持续时间 |

## 6. Endgame

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_ENDGAME_SOFT_CLOSE_SECS` | `60` | 停风险增加型意图 |
| `PM_ENDGAME_HARD_CLOSE_SECS` | `30` | 优先 maker repair，不足时 taker de-risk |
| `PM_ENDGAME_FREEZE_SECS` | `2` | 禁止新增风险 |
| `PM_ENDGAME_MAKER_REPAIR_MIN_SECS` | `8` | HardClose 内继续 maker repair 的最小时间预算 |
| `PM_ENDGAME_EDGE_KEEP_MULT` | `1.5` | keep-mode 阈值 |
| `PM_ENDGAME_EDGE_EXIT_MULT` | `1.25` | keep-mode 退出阈值 |

## 7. recycle / claim

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_RECYCLE_ENABLED` | `true` | 启用 batch merge |
| `PM_RECYCLE_ONLY_HEDGE` | `false` | Hedge + Provide 拒单都可触发 |
| `PM_RECYCLE_TRIGGER_REJECTS` | `2` | 窗口内触发阈值 |
| `PM_RECYCLE_TRIGGER_WINDOW_SECS` | `90` | 统计窗口 |
| `PM_RECYCLE_PROACTIVE` | `true` | 启用低水位主动探测 |
| `PM_RECYCLE_POLL_SECS` | `5` | 主动探测周期 |
| `PM_RECYCLE_COOLDOWN_SECS` | `120` | 回收冷却 |
| `PM_RECYCLE_MAX_MERGES_PER_ROUND` | `2` | 单轮最大 merge 次数 |
| `PM_RECYCLE_LOW_WATER_USDC` | `6.0` | 低水位门槛 |
| `PM_RECYCLE_TARGET_FREE_USDC` | `18.0` | 回补目标 |
| `PM_RECYCLE_MIN_BATCH_USDC` | `10.0` | 最小批量 |
| `PM_RECYCLE_MAX_BATCH_USDC` | `30.0` | 最大批量 |
| `PM_RECYCLE_SHORTFALL_MULT` | `1.2` | 缺口放大倍率 |
| `PM_RECYCLE_MIN_EXECUTABLE_USDC` | `5.0` | 低于该金额不执行 |
| `PM_BALANCE_CACHE_TTL_MS` | `2000` | 余额缓存 TTL |
| `PM_AUTO_CLAIM` | `true` | 开启回合 claim |
| `PM_AUTO_CLAIM_DRY_RUN` | `false` | live 才执行 |
| `PM_AUTO_CLAIM_ROUND_WINDOW_SECS` | `30` | claim SLA 窗口 |
| `PM_AUTO_CLAIM_ROUND_RETRY_SCHEDULE` | `0,2,5,9,14,20,27` | 重试节奏 |
| `PM_AUTO_CLAIM_ROUND_SCOPE` | `ended_then_global` | 先本轮后全局 |

## 8. 非主线策略参数

以下参数仍被代码支持，但不属于当前推荐 live 主线：
- `PM_OPEN_PAIR_BAND`
- `PM_DIP_BUY_MAX_ENTRY_PRICE`
- `PM_AS_SKEW_FACTOR`
- `PM_AS_TIME_DECAY_K`
- `PM_BID_PCT`
- `PM_NET_DIFF_PCT`

原则：
- 当前先把 `glft_mm` 跑稳
- 非主线参数默认不作为 live 模板的激活项
