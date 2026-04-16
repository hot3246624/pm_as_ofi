# 配置参数手册

本文档描述当前 `.env.example` 的推荐模板值。  
注意：代码内部 fallback 默认值仍然偏保守，但实盘建议以模板为准。

## 1. 市场与认证

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `POLYMARKET_MARKET_SLUG` | `btc-updown-15m` | 当前推荐收益验证市场（`5m` 仅用于机制冒烟） |
| `PM_BINANCE_SYMBOL_OVERRIDE` | unset | 仅 `glft_mm` 使用；`pair_arb` 主线不需要 |
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
| `PM_POST_CLOSE_WINDOW_SECS` | `120` | 仅 `oracle_lag_sniping` 使用：收盘后继续运行的窗口（秒） |
| `PM_POST_CLOSE_CHAINLINK_WS_URL` | `wss://ws-live-data.polymarket.com` | 仅 `oracle_lag_sniping` 使用：Chainlink RTDS 地址 |
| `PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS` | `8` | 仅 `oracle_lag_sniping` 使用：收盘后等待 Chainlink 胜负提示的最大秒数 |
| `PM_POST_CLOSE_GAMMA_POLL_MS` | `300` | 仅 `oracle_lag_sniping` 使用：Chainlink 未命中时 Gamma 兜底轮询间隔 |

## 3. 当前推荐策略模板（pair_arb 验证基线）

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_STRATEGY` | `pair_arb` | 当前验证主线 |
| `PM_BID_SIZE` | `5.0` | 单次挂单份额 |
| `PM_MAX_NET_DIFF` | `5.0` | 盘中净仓硬上限（当前 15m canary 基线） |
| `PM_PAIR_TARGET` | `0.97` | 组合成本目标线（pair_arb 核心参数） |
| `PM_PAIR_ARB_PAIR_COST_SAFETY_MARGIN` | `0.02` | `VWAP ceiling` 使用的安全边际，真实上限基于 `pair_target - margin` |
| `PM_TICK_SIZE` | `0.01` | 价格粒度 |
| `PM_POST_ONLY_SAFETY_TICKS` | `2.0` | maker 安全垫基础退让 |
| `PM_POST_ONLY_TIGHT_SPREAD_TICKS` | `3.0` | 紧价差额外退让触发线 |
| `PM_POST_ONLY_EXTRA_TIGHT_TICKS` | `1.0` | 紧价差额外退让 |
| `PM_REPRICE_THRESHOLD` | `0.020` | 更保守的重报价阈值 |
| `PM_DEBOUNCE_MS` | `700` | provide 防抖 |
| `PM_STALE_TTL_MS` | `3000` | 单侧 stale TTL |
| `PM_TOXIC_RECOVERY_HOLD_MS` | `1200` | toxic 恢复冷却 |
| `PM_AS_SKEW_FACTOR` | `0.06` | 三段库存 skew 的基础强度（pair_arb） |
| `PM_AS_TIME_DECAY_K` | `1.0` | 后半段库存叠加的时间衰减（pair_arb） |
| `PM_PAIR_ARB_TIER_MODE` | `discrete` | 分段库存模式：`disabled`（关闭 tier cap，并回退非分段库存曲线）、`discrete`（阶梯）或 `continuous`（平滑） |
| `PM_PAIR_ARB_TIER_1_MULT` | `0.60` | 仅 risk-increasing：`|net_diff| >= 3.5` 的主仓侧 avg-cost cap |
| `PM_PAIR_ARB_TIER_2_MULT` | `0.20` | 仅 risk-increasing：`|net_diff| >= 8` 的主仓侧 avg-cost cap |

验证时建议同时观察两组日志：
- `PairArbGate(30s)`：候选保留/跳过/OFI 软塑形
- `LIVE_OBS`：执行稳定性与 `pair_arb_softened_ratio`

`pair_arb` 当前报价主语义（固定内部行为，不开放 env）：
- 策略主脑只读单一实时库存（`Matched` 即时生效，`Failed` 立即回滚，`Merge` 做校正）
- 状态切换仍以 `dominant_side / net_bucket / risk_open_cutoff_active` 为骨架
- live quote 保留语义统一为离散状态驱动（`Matched/Failed/Merge/SoftClose/round reset` + `state_key` 变化）；不再基于连续 `up/down ticks` 漂移重发
- `state_key` 变化时强制重评；旧状态 target 不得跨状态继续 place/reprice
- `pair_progress_regime` 只保留为观测指标，不参与运行时报价阻断
- 已移除 `Round Suitability` 与 `utility/open_edge` 二次收益过滤，候选保留只看硬约束链路
- 成交最终性相关统计仍保留在 accounting / diagnostics，不再直接驱动 `pair_arb` 报价

补充：`oracle_lag_sniping`（HYPE-only 试验策略）
- 建议单独设置：
  - `PM_STRATEGY=oracle_lag_sniping`（兼容旧别名 `post_close_hype`）
  - `POLYMARKET_MARKET_SLUG=hype-updown-5m`
  - `PM_POST_CLOSE_WINDOW_SECS=120`
  - `PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS=8`
  - `PM_POST_CLOSE_GAMMA_POLL_MS=300`
- 运行语义：
  - 先监听 Chainlink RTDS（`crypto_prices_chainlink`）判定胜方并发送 WinnerHint；
  - 若 `+N` 秒内未拿到 Chainlink 胜方提示，则自动切到 Gamma 兜底；
  - 仅在市场结束后窗口内尝试胜方 BUY 报价；非 HYPE 5m 轮次保持静默。
  - 胜方价格 `>0.993` 时不做 taker；maker 仍允许挂单。
- 实盘前操作清单见：
  - [ORACLE_LAG_SNIPING_LIVE_CHECKLIST_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/ORACLE_LAG_SNIPING_LIVE_CHECKLIST_ZH.md)

## 4. `glft_mm` 专属参数（仅 challenger 使用）

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
- `sigma_prob` 半衰期 = `20s`（由 Polymarket 概率中价变化在线估计）
- `basis_prob` 半衰期 = `30s`
- cold-ramp = `8s`（basis 限幅 `±0.08`）
- cold-ramp reservation corridor = `synthetic_mid_yes ± 15*tick`
- governor 步长 = `1 tick`
- post-fill sell warmup = `1500ms`
- drift guard（Safe/Aligned）= `ColdRamp 1*tick` / `Live 2*tick`，并带 `>=1500ms` age 门控
- post-only `crosses book` 短冷却 = `1000ms`（独立于通用 validation 冷却）

运行解读：
- `pair_arb` 主线不读取这组参数
- 仅在切换 `PM_STRATEGY=glft_mm` 时才需要启用

## 5. OFI 推荐值（当前 pair_arb 验证基线）

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_OFI_WINDOW_MS` | `3000` | 订单流窗口 |
| `PM_OFI_TOXICITY_THRESHOLD` | `300.0` | 冷启动阈值；warm-up 前的回退锚点 |
| `PM_OFI_ADAPTIVE` | `true` | 开启自适应 |
| `PM_OFI_ADAPTIVE_K` | `4.2` | 旧 mean+sigma 兼容参数；当前 regime-normalized 模式不使用 |
| `PM_OFI_ADAPTIVE_MIN` | `120.0` | regime baseline 下限护栏 |
| `PM_OFI_ADAPTIVE_MAX` | `1800.0` | regime baseline 上限护栏（命中会打 `saturated` 日志） |
| `PM_OFI_ADAPTIVE_RISE_CAP_PCT` | `0.20` | 旧 rise-cap 兼容参数；当前 regime-normalized 模式不使用 |
| `PM_OFI_ADAPTIVE_WINDOW` | `200` | 自适应样本窗口，用于 rolling Q50/Q99/Q95 |
| `PM_OFI_RATIO_ENTER` | `0.70` | 进入 toxic 的比例门槛 |
| `PM_OFI_RATIO_EXIT` | `0.40` | 退出比例门槛 |
| `PM_OFI_HEARTBEAT_MS` | `200` | OFI 心跳 |
| `PM_OFI_EXIT_RATIO` | `0.85` | 滞回退出比 |
| `PM_OFI_MIN_TOXIC_MS` | `800` | 单次 toxic 最短持续时间 |

运行解读：
- 当前 OFI 是“连续信号 + regime-aware tail kill”双层结构
- kill 主判定基于 `normalized_score = |OFI| / baseline`，其中 baseline 来自 rolling `Q50`
- 进入/恢复阈值由 rolling `Q99/Q95` 映射到 score 空间，再叠加 ratio gate 与最小毒性保持时间
- `PM_OFI_ADAPTIVE_MIN/MAX` 仅作 baseline 护栏；高热时若触及上限会输出 `saturated` 可观测日志
- `pair_arb` 当前只把这套 OFI 用于 same-side risk-increasing buy 的软塑形：
  - `hot` 额外退让 `1 tick`
  - `toxic` 额外退让 `2 ticks`
  - `toxic + saturated` 直接 suppress 同侧加仓
- `pairing / risk-reducing buy` 不受 OFI 影响；`pair_arb` 也不新增专属 `PM_OFI_*` 参数

## 6. Endgame（当前 `pair_arb` 主线语义）

| 参数 | 模板值 | 说明 |
| --- | --- | --- |
| `PM_ENDGAME_SOFT_CLOSE_SECS` | `45` | 共享阶段参数；`pair_arb` 在 SoftClose 下阻断 risk-increasing，且 `|net_diff|<=bid_size/2` 时停止新开买单 |
| `PM_ENDGAME_HARD_CLOSE_SECS` | `30` | 共享阶段参数 |
| `PM_ENDGAME_FREEZE_SECS` | `2` | 共享阶段参数 |
| `PM_PAIR_ARB_RISK_OPEN_CUTOFF_SECS` | `240` | `pair_arb` 独立开窗参数；当剩余时间 `<= cutoff` 时阻断新的 risk-increasing |

说明：
- `pair_arb` 当前已去掉方向对冲 overlay 与尾盘强制市价去风险路径。
- `PM_ENDGAME_MAKER_REPAIR_MIN_SECS` / `PM_ENDGAME_EDGE_KEEP_MULT` / `PM_ENDGAME_EDGE_EXIT_MULT` 对当前 `pair_arb` 主路径不生效。

## 7. 兼容保留参数（当前 `pair_arb` 主路径不主用）

| 参数 | 模板建议 | 说明 |
| --- | --- | --- |
| `PM_MAX_PORTFOLIO_COST` | 注释保留 | 旧 hedge / rescue ceiling 兼容参数，当前主路径不使用 |
| `PM_HEDGE_DEBOUNCE_MS` | 注释保留 | 旧 hedge 兼容参数 |
| `PM_MIN_HEDGE_SIZE` | 注释保留 | 旧 hedge/taker 兼容参数 |
| `PM_HEDGE_ROUND_UP` | 注释保留 | 旧 hedge/taker 兼容参数 |
| `PM_HEDGE_MIN_MARKETABLE_*` | 注释保留 | 旧 hedge/taker 兼容参数 |

## 8. recycle / claim

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
| `PM_RECYCLE_MIN_BATCH_USDC` | `5.0` | 最小批量（降低 4.99-lot 回收死锁概率） |
| `PM_RECYCLE_MAX_BATCH_USDC` | `30.0` | 最大批量 |
| `PM_RECYCLE_SHORTFALL_MULT` | `1.2` | 缺口放大倍率 |
| `PM_RECYCLE_MIN_EXECUTABLE_USDC` | `5.0` | 低于该金额不执行 |
| `PM_BALANCE_CACHE_TTL_MS` | `2000` | 余额缓存 TTL |
| `PM_AUTO_CLAIM` | `true` | 开启回合 claim |
| `PM_AUTO_CLAIM_DRY_RUN` | `false` | live 才执行 |
| `PM_AUTO_CLAIM_ROUND_WINDOW_SECS` | `30` | claim SLA 窗口 |
| `PM_AUTO_CLAIM_ROUND_RETRY_SCHEDULE` | `0,2,5,9,14,20,27` | 重试节奏 |
| `PM_AUTO_CLAIM_ROUND_SCOPE` | `ended_then_global` | 先本轮后全局 |

## 9. 非主线策略参数

以下参数仍被代码支持，但不属于当前推荐 live 主线：
- `PM_OPEN_PAIR_BAND`
- `PM_DIP_BUY_MAX_ENTRY_PRICE`
- `PM_BID_PCT`
- `PM_NET_DIFF_PCT`

原则：
- 当前先把 `pair_arb` 跑稳
- 非主线参数默认不作为验证模板激活项

补充说明：
- `PM_OPEN_PAIR_BAND` 主要服务 `gabagool_grid`
- `PM_AS_SKEW_FACTOR` / `PM_AS_TIME_DECAY_K` 是 `pair_arb` 核心参数
