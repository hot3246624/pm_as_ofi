# 配置参数手册（`.env` / `.env.example`）

本文是参数语义的统一说明，和 `docs/STRATEGY_V2_CORE_ZH.md` 配套使用。  
默认值以当前 `.env.example` 为准。

## 1. 市场选择

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `POLYMARKET_MARKET_SLUG` | `btc-updown-5m` | 最高优先级；可填完整 slug（单场）或前缀（轮转） |
| `POLYMARKET_MARKET_PREFIX` | unset | 前缀模式别名，和 `SLUG` 二选一 |
| `POLYMARKET_MARKET_SYMBOL` | unset | 与 `TIMEFRAME` 组合自动拼接 `<symbol>-updown-<tf>` |
| `POLYMARKET_MARKET_TIMEFRAME` | unset | 支持 `1m/5m/15m/30m/1h/4h/1d` |

## 2. 连接与认证

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `POLYMARKET_WS_BASE_URL` | `wss://.../ws` | 基础 WS 地址，程序自动拼接 `/market`/`/user` |
| `POLYMARKET_REST_URL` | `https://clob.polymarket.com` | CLOB REST 基址 |
| `POLYMARKET_PRIVATE_KEY` | empty | 实盘必填，签名私钥（不带 `0x`） |
| `POLYMARKET_FUNDER_ADDRESS` | empty | 实盘必填，资金地址（余额/授权/代理认证） |
| `POLYMARKET_API_KEY/SECRET/PASSPHRASE` | unset | 可选；留空会尝试自动派生 |
| `POLYMARKET_BUILDER_API_KEY/SECRET/PASSPHRASE` | unset | 仅 Safe 自动 Claim/merge 需要 |
| `POLYMARKET_RELAYER_URL` | `https://relayer-v2.polymarket.com` | Builder/Relayer 地址 |

## 3. Claim（可选）

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `PM_CLAIM_MONITOR` | `true` | 输出可领取仓位监控日志 |
| `PM_AUTO_CLAIM` | `false` | 自动领取开关 |
| `PM_AUTO_CLAIM_DRY_RUN` | `false` | `true` 时仅打印不执行 |
| `PM_AUTO_CLAIM_MIN_VALUE` | `0` | 单个 condition 最小价值阈值（美元） |
| `PM_AUTO_CLAIM_MAX_CONDITIONS` | `5` | 单轮最多处理 condition 数量 |
| `PM_AUTO_CLAIM_INTERVAL_SECONDS` | `300` | 两次自动领取最小间隔 |
| `PM_AUTO_CLAIM_ROUND_WINDOW_SECS` | `30` | 每轮结束后的 Claim SLA 窗口（秒） |
| `PM_AUTO_CLAIM_ROUND_RETRY_MODE` | `exponential` | 回合 Claim 重试模式（默认指数退避） |
| `PM_AUTO_CLAIM_ROUND_SCOPE` | `ended_then_global` | 先尝试刚结束市场，再全局兜底 |
| `PM_AUTO_CLAIM_ROUND_RETRY_SCHEDULE` | `0,2,5,9,14,20,27` | 回合窗口内重试偏移秒（需 ≤ window） |
| `PM_AUTO_CLAIM_WAIT_CONFIRM` | `false` | Safe 模式是否等待 relayer confirm |
| `PM_AUTO_CLAIM_WAIT_TIMEOUT_SECONDS` | `20` | `WAIT_CONFIRM=true` 时最大等待秒数 |

## 4. 运行时控制

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `PM_DRY_RUN` | `true` | 演习模式（不真实下单） |
| `PM_ENTRY_GRACE_SECONDS` | `30` | 新市场开盘后可入场窗口（秒） |
| `PM_WS_CONNECT_TIMEOUT_MS` | `6000` | Market WS 单次连接超时 |
| `PM_RESOLVE_TIMEOUT_MS` | `4000` | Gamma 市场解析超时 |
| `PM_RESOLVE_RETRY_ATTEMPTS` | `4` | 解析重试次数（指数退避） |
| `PM_RECONCILE_INTERVAL_SECS` | `30` | REST 对账周期 |
| `PM_COORD_WATCHDOG_MS` | `500` | 无行情事件时的风控心跳 |

## 5. 核心策略参数

| 参数 | 默认值 | 单位 | 说明 |
|---|---:|---|---|
| `PM_PAIR_TARGET` | `0.985` | 成本 | 一对 `YES+NO` 目标成本；利润约为 `1-pair_target` |
| `PM_BID_SIZE` | `5.0` | shares | Provide 单次规模 |
| `PM_MIN_ORDER_SIZE` | auto | shares | 最小订单数量；未设置时从 order_book 探测 |
| `PM_MIN_HEDGE_SIZE` | `0.0` | shares | 对冲触发最小阈值（`0`=关闭） |
| `PM_HEDGE_ROUND_UP` | `false` | bool | 小额对冲是否向上取整到最小订单 |
| `PM_TICK_SIZE` | `0.01` | price | 价格粒度 |
| `PM_POST_ONLY_SAFETY_TICKS` | `2.0` | ticks | maker 安全垫基础退让 |
| `PM_POST_ONLY_TIGHT_SPREAD_TICKS` | `3.0` | ticks | 价差小于该值时触发额外退让 |
| `PM_POST_ONLY_EXTRA_TIGHT_TICKS` | `1.0` | ticks | 紧价差额外退让 |
| `PM_REPRICE_THRESHOLD` | `0.010` | price | 重报价阈值 |
| `PM_DEBOUNCE_MS` | `500` | ms | Provide 防抖 |
| `PM_HEDGE_DEBOUNCE_MS` | `100` | ms | Hedge 防抖 |
| `PM_AS_SKEW_FACTOR` | `0.03` | factor | 库存偏斜系数 |
| `PM_AS_TIME_DECAY_K` | `2.0` | factor | 临期 skew 放大系数 |
| `PM_STALE_TTL_MS` | `3000` | ms | 单侧 stale 熔断阈值 |

## 6. 风险约束

| 参数 | 默认值 | 单位 | 说明 |
|---|---:|---|---|
| `PM_MAX_NET_DIFF` | `10.0` | shares | 最大净敞口（方向性风险） |
| `PM_MAX_SIDE_SHARES` | `50.0` | shares | 单侧总持仓上限（绝对暴露） |
| `PM_MAX_PORTFOLIO_COST` | `1.02` | 成本 | 救火成本上限 |
| `PM_MAX_LOSS_PCT` | `0.02` | pct | 最大容忍亏损比例；启动时钳制 `MAX_PORTFOLIO_COST` |

## 7. 动态资金管理（显式启用）

默认静态优先：只有显式配置才启用动态覆盖。

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `PM_MAX_POS_PCT` | `0.0` | `0`=关闭动态 gross；`>0` 启用动态 side cap |
| `PM_BID_PCT` | unset | 显式启用后，按余额下调 `PM_BID_SIZE` |
| `PM_NET_DIFF_PCT` | unset | 显式启用后，按余额下调 `PM_MAX_NET_DIFF` |
| `PM_DYNAMIC_GROSS_REFRESH_SECS` | `10`（建议注释） | 仅 `PM_MAX_POS_PCT>0` 时生效 |

## 8. OFI 毒性参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `PM_OFI_WINDOW_MS` | `3000` | OFI 滑窗长度 |
| `PM_OFI_TOXICITY_THRESHOLD` | `300` | 基准阈值（adaptive=true 时作为初始/回退值） |
| `PM_OFI_ADAPTIVE` | `true` | 自适应阈值开关 |
| `PM_OFI_ADAPTIVE_K` | `3.0` | `mean + k*sigma` 的 `k` |
| `PM_OFI_ADAPTIVE_MIN` | `120` | 自适应阈值下限 |
| `PM_OFI_ADAPTIVE_MAX` | `0.0` | 自适应阈值硬上限（`0`=关闭） |
| `PM_OFI_ADAPTIVE_RISE_CAP_PCT` | `0.20` | 每次心跳阈值最大上升比例 |
| `PM_OFI_ADAPTIVE_WINDOW` | `200` | 自适应滚动样本窗口 |
| `PM_OFI_RATIO_ENTER` | `0.45` | 毒性进入比例门槛 |
| `PM_OFI_RATIO_EXIT` | `0.30` | 毒性退出比例门槛（应 ≤ enter） |
| `PM_OFI_HEARTBEAT_MS` | `200` | OFI 心跳 |
| `PM_OFI_EXIT_RATIO` | `0.85` | 滞回退出比例 |
| `PM_OFI_MIN_TOXIC_MS` | `800` | 单次 toxic 最短保持时间 |
| `PM_TOXIC_RECOVERY_HOLD_MS` | `1200` | 恢复后冷却，避免撤挂抖动 |

## 9. Marketable-BUY 防拒单参数

默认静态优先：关闭全局最小名义预检，保留“拒单分类 + 冷却”。

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `PM_MIN_MARKETABLE_NOTIONAL_FLOOR` | `0.0` | 全局最小名义预检（`0`=关闭） |
| `PM_MIN_MARKETABLE_AUTO_DETECT` | `false` | 是否从拒单自动学习 `min size:$X` |
| `PM_MIN_MARKETABLE_COOLDOWN_MS` | `10000` | 命中 marketable-min 后侧边冷却 |
| `PM_HEDGE_MIN_MARKETABLE_NOTIONAL` | `0.0` | 仅对冲路径的最小名义兜底 |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA` | `0.5` | 触发兜底时的绝对额外份额上限 |
| `PM_HEDGE_MIN_MARKETABLE_MAX_EXTRA_PCT` | `0.15` | 触发兜底时的相对额外比例上限 |

## 10. 资金回收（Batch Merge）

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `PM_RECYCLE_ENABLED` | `true` | 回收器开关 |
| `PM_RECYCLE_ONLY_HEDGE` | `false` | `false`=Hedge+Provide 都可触发；`true`=仅 Hedge |
| `PM_RECYCLE_TRIGGER_REJECTS` | `2` | 窗口内拒单触发阈值 |
| `PM_RECYCLE_TRIGGER_WINDOW_SECS` | `90` | 拒单统计窗口 |
| `PM_RECYCLE_PROACTIVE` | `true` | 主动低水位探测 |
| `PM_RECYCLE_POLL_SECS` | `5` | 主动探测周期 |
| `PM_RECYCLE_COOLDOWN_SECS` | `120` | 两次回收最小间隔 |
| `PM_RECYCLE_MAX_MERGES_PER_ROUND` | `2` | 单轮最大 merge 次数 |
| `PM_RECYCLE_LOW_WATER_USDC` | `6.0` | 低水位门槛 |
| `PM_RECYCLE_TARGET_FREE_USDC` | `18.0` | 回补目标余额 |
| `PM_RECYCLE_MIN_BATCH_USDC` | `10.0` | 单次最小回收量 |
| `PM_RECYCLE_MAX_BATCH_USDC` | `30.0` | 单次最大回收量 |
| `PM_RECYCLE_SHORTFALL_MULT` | `1.2` | 缺口放大倍率（批处理） |
| `PM_RECYCLE_MIN_EXECUTABLE_USDC` | `5.0` | 低于该金额不执行 |
| `PM_BALANCE_CACHE_TTL_MS` | `2000` | 执行层余额缓存 TTL |

## 11. 签名模式

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `PM_SIGNATURE_TYPE` | `2` | SAFE 模式（merge/claim 常用） |

---

## 12. 建议配置原则

1. 静态优先：动态覆盖项 (`PM_BID_PCT/PM_NET_DIFF_PCT/PM_MAX_POS_PCT`) 默认关闭或注释。  
2. 分层门禁：Provide 看“净仓+单侧”，Hedge 看“净仓优先去风险”。  
3. 防风暴优先级：先保证拒单冷却与状态自愈，再考虑开启全局名义预检。  
4. 所有调参以日志验证为准：先 dry-run，再小仓 live。
