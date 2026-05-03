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
| `PM_INSTANCE_ID` | unset | 进程实例标识。建议每个独立进程都设置唯一值，避免日志、replay、recorder 路径互相覆盖 |
| `PM_LOG_ROOT` | auto | 显式覆盖 runtime 日志根目录；默认写入 `logs/<instance_id>/runs/<timestamp>` |
| `PM_RECORDER_ROOT` | `data/recorder` | 显式覆盖 recorder 根目录；多实例场景建议使用 `data/recorder/<instance_id>` |
| `PM_SHARED_INGRESS_ROLE` | `standalone` | 跨进程共享公共数据平面的角色：`standalone / broker / client / auto`。二进制默认 `standalone`；策略启动脚本通常会切到 `auto` |
| `PM_SHARED_INGRESS_ROOT` | `run/shared-ingress-main` | broker 与所有 client 共享的 Unix socket 根目录，必须完全一致 |
| `PM_SHARED_INGRESS_MARKET_CONNECT_PERMITS` | `2` | shared-ingress prefix market feed 的并发连接握手上限 |
| `PM_SHARED_INGRESS_MARKET_CONNECT_JITTER_MS` | `1200` | shared-ingress prefix market feed 启动连接抖动，避免同时打爆 WS |
| `PM_SHARED_INGRESS_FIXED_MARKET_CONNECT_PERMITS` | `2` | shared-ingress fixed round market feed 的独立并发连接握手上限 |
| `PM_SHARED_INGRESS_FIXED_MARKET_CONNECT_JITTER_MS` | `0` | shared-ingress fixed round market feed 启动连接抖动；默认 0，优先服务单轮研究/交易 |
| `PM_ENTRY_GRACE_SECONDS` | `30` | 新市场开盘后的可入场窗口 |
| `PM_WS_CONNECT_TIMEOUT_MS` | `6000` | Market WS 连接超时 |
| `PM_WS_DEGRADE_MAX_FAILURES` | `12` | 连续失败后提前结束本轮 |
| `PM_RESOLVE_TIMEOUT_MS` | `4000` | Gamma 解析超时 |
| `PM_RESOLVE_RETRY_ATTEMPTS` | `4` | 解析重试次数 |
| `PM_RECONCILE_INTERVAL_SECS` | `30` | REST 对账周期 |
| `PM_COORD_WATCHDOG_MS` | `500` | 无行情时的风控心跳 |
| `PM_STRATEGY_METRICS_LOG_SECS` | `15` | 指标日志周期 |

### Live 执行前置条件

CLOB V2 live maker/taker 现在走本地 V2 签名与 `POST /order`，不再依赖旧的 V1 下单 builder。恢复真钱交易前必须满足：

1. 钱包里已有 `pUSD` collateral。
2. `pUSD / CTF / V2 exchange` allowance 已就绪。
3. `POLYMARKET_FUNDER_ADDRESS` 与 `PM_SIGNATURE_TYPE` 匹配当前账户形态：
   - `0 = EOA`
   - `1 = Proxy`
   - `2 = GnosisSafe`
4. `POLYMARKET_API_KEY / SECRET / PASSPHRASE` 如显式填写，必须与当前 signer/funder/signature_type 对应；否则请留空，让程序自动派生。
| `PM_POST_CLOSE_WINDOW_SECS` | `105` | 仅 `oracle_lag_sniping` 使用：收盘后继续运行的窗口（秒） |
| `PM_POST_CLOSE_CHAINLINK_WS_URL` | `wss://ws-live-data.polymarket.com` | 仅 `oracle_lag_sniping` 使用：Chainlink RTDS 地址 |
| `PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS` | `8` | 仅 `oracle_lag_sniping` 使用：收盘后等待 Chainlink 胜负提示的最大秒数 |
| `PM_SELF_BUILT_PRICE_AGG_ENABLED` | `true` | 仅 `oracle_lag_sniping` 使用：启用自建价格聚合器（失败自动回退 legacy exact） |
| `PM_SELF_BUILT_PRICE_AGG_OPEN_TOLERANCE_MS` | `1200` | 仅 `oracle_lag_sniping` 使用：open 候选与 round_start 的最大容忍偏差（毫秒） |
| `PM_SELF_BUILT_PRICE_AGG_CLOSE_TOLERANCE_MS` | `1500` | 仅 `oracle_lag_sniping` 使用：close 候选与 round_end 的最大容忍偏差（毫秒） |
| `PM_SELF_BUILT_PRICE_AGG_MIN_CONFIDENCE` | `0.80` | 仅 `oracle_lag_sniping` 使用：聚合器最小置信度阈值 |
| `PM_LOCAL_PRICE_AGG_ENABLED` | `false` | 仅 `oracle_lag_sniping` 使用：启用本地多源价格聚合器（Binance/Bybit/OKX/Coinbase），默认 shadow |
| `PM_LOCAL_PRICE_AGG_DECISION_ENABLED` | `false` | 仅 `oracle_lag_sniping` 使用：允许 `LocalAgg` 直接驱动 winner hint 决策 |
| `PM_LOCAL_PRICE_AGG_OPEN_TOLERANCE_MS` | `600` | 本地聚合器 open 边界容忍偏差（毫秒） |
| `PM_LOCAL_PRICE_AGG_CLOSE_TOLERANCE_MS` | `2500` | 本地聚合器 close 边界容忍偏差（毫秒） |
| `PM_LOCAL_PRICE_AGG_MIN_CONFIDENCE` | `0.85` | 本地聚合器最小置信度阈值 |
| `PM_LOCAL_PRICE_AGG_MIN_SOURCES` | `1` | 本地聚合器最少有效源数量 |
| `PM_LOCAL_PRICE_AGG_MAX_SOURCE_SPREAD_BPS` | `12` | 本地聚合器源间最大允许价差（bps） |
| `PM_LOCAL_PRICE_AGG_WEIGHT_BINANCE` | `1.0` | 本地聚合器 Binance 基础权重 |
| `PM_LOCAL_PRICE_AGG_WEIGHT_BYBIT` | `1.0` | 本地聚合器 Bybit 基础权重 |
| `PM_LOCAL_PRICE_AGG_WEIGHT_OKX` | `1.0` | 本地聚合器 OKX 基础权重 |
| `PM_LOCAL_PRICE_AGG_WEIGHT_COINBASE` | `1.0` | 本地聚合器 Coinbase 基础权重 |
| `PM_LOCAL_PRICE_AGG_CLOSE_TIME_DECAY_MS` | `900` | 本地聚合器 close 融合的时间衰减常数（毫秒） |
| `PM_LOCAL_PRICE_AGG_EXACT_BOOST` | `1.25` | 本地聚合器对 exact close 点的额外权重倍数 |
| `PM_POST_CLOSE_GAMMA_POLL_MS` | `300` | 仅 `oracle_lag_sniping` 使用：Chainlink 未命中时 Gamma 兜底轮询间隔 |
| `PM_ORACLE_LAG_CROSS_MARKET_ARBITER_ENABLED` | `false` | 仅 `oracle_lag_sniping` 使用：是否启用跨市场仲裁（默认关闭，推荐每市场独立 single-shot） |
| `PM_ORACLE_LAG_ARBITER_COLLECTION_WINDOW_MS` | `200` | 仅在启用跨市场仲裁时有效：仲裁收集窗口（毫秒） |
| `PM_ORACLE_LAG_ARBITER_BOOK_MAX_AGE_MS` | `250` | 仅在启用跨市场仲裁时有效：盘口新鲜度阈值（毫秒） |
| `PM_ORACLE_LAG_MAX_ORDER_NOTIONAL_USDC` | `0` | 仅 `oracle_lag_sniping` 使用：单笔名义上限（USDC），`0` 表示关闭；例如 `1` 表示每次最多买 1 USDC |
| `PM_ORACLE_LAG_LAB_ONLY` | `false` | 仅 `oracle_lag_sniping` 使用：实验模式。开启后保留 RTDS/本地聚合判定与对照日志，但阻断 taker/maker 下单路径 |
| `PM_ORACLE_LAG_DRYRUN_EXECUTE` | `false` | 仅 `oracle_lag_sniping` dry-run 使用：默认仅 `dry_taker_preview`；设 `true` 时允许 taker 意图继续进入 OMS/Executor，走 simulated fill 链路 |
| `PM_ORACLE_LAG_SYMBOL_UNIVERSE` | `hype,btc,eth,sol,bnb,doge,xrp` | 仅 `oracle_lag_sniping` 使用：允许激活的 5m symbol 列表；`*` 表示全部 |
| `PM_MULTI_MARKET_PREFIXES` | unset | 逗号分隔的市场前缀并发列表；设置后主进程进入 supervisor 模式并拉起多个子进程（每个前缀一个） |
| `PM_INPROC_SUPERVISOR` | auto | 多市场模式下是否使用 in-proc 单进程多worker；`oracle_lag_sniping` 默认为 `true`，设 `0/false` 可强制回退多进程 |

### 跨进程共享数据平面说明

- `PM_SHARED_INGRESS_ROLE=auto`
  - 推荐模式。
  - 启动时先检查共享 broker 是否健康、是否兼容。
  - 若不存在健康 broker，则当前实例自动拉起 sidecar broker。
  - 若已存在健康 broker，则当前实例直接作为 client 接入。
  - broker 在最后一个 client 离开后会按 idle grace 自动退出。
- 共享兼容性按 shared-ingress ABI 判定，而不是按整个二进制 build 判定：
  - `protocol_version`
  - `schema_version`
- 因此，不同 PR / 不同 build 只要没有修改 shared-ingress control/wire/schema，就可以共用同一个 `PM_SHARED_INGRESS_ROOT`。
- `build_id` 仅用于观测和调试，不再作为默认拒绝复用 broker 的条件。
- `PM_SHARED_INGRESS_ROLE=broker`
  - 当前进程只持有公共上游连接：
    - market WS
    - Chainlink RTDS
    - Local Price feeds
  - 不应持有钱包私钥执行真实交易。
- `PM_SHARED_INGRESS_ROLE=client`
  - 当前进程不再自行建立以上公共行情连接。
  - 它只通过 `PM_SHARED_INGRESS_ROOT` 下的 Unix socket 订阅 broker 广播出来的标准化事件。
- `PM_SHARED_INGRESS_ROLE=standalone`
  - 默认模式，当前进程自行建立所有连接。
- market feed 调度：
  - prefix feed（如 `btc-updown-5m`）继续走普通连接通道和 jitter。
  - fixed round feed（如 `btc-updown-5m-1777442700`）走独立连接通道，默认不做启动 jitter，避免被 prefix feed 重连风暴饿死。
  - broker 会在订阅者登记后才启动 fixed feed；最后一个订阅者离开后，feed 会释放并停止重连。

运行约束：

- 同一组 `broker + clients` 必须共享同一个 `PM_SHARED_INGRESS_ROOT`。
- 多个 client 可以共用一个 broker。
- `auto` 模式只在**启动期**做单机自动选主；运行中 client 不会自我晋升为 broker，避免 split-brain。
- 若使用手工 `broker/client`，则必须**显式指定**哪个进程是 `broker`，其余进程是 `client`。
- 若多个进程使用**同一个钱包**，共享数据平面并不能解决执行冲突；仍然需要单执行 authority。
- 若多个进程使用**不同钱包**，可安全共用同一个 broker，因为 broker 不持有任何钱包密钥。

### 策略启动入口

- [run_local_agg_lab.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_local_agg_lab.sh)
  - 仅适用于 `PM_STRATEGY=oracle_lag_sniping`
- [run_pgt_fixed_shadow_next.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_pgt_fixed_shadow_next.sh)
  - 仅适用于 `PM_STRATEGY=pair_gated_tranche_arb`
- [run_strategy_instance.sh](/Users/hot/web3Scientist/pm_as_ofi/scripts/run_strategy_instance.sh)
  - 统一入口；按 `PM_STRATEGY` 分流到上述专用脚本

示例：

```bash
PM_INSTANCE_ID=agent-a \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
PM_STRATEGY=oracle_lag_sniping \
./scripts/run_strategy_instance.sh
```

```bash
PM_INSTANCE_ID=xuanxuan008_research \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
PM_STRATEGY=pair_gated_tranche_arb \
./scripts/run_strategy_instance.sh btc-updown-5m
```

### PGT Shadow Replay Profile

`run_pgt_fixed_shadow_next.sh` 默认启用：

```bash
PM_PGT_SHADOW_PROFILE=replay_focused_v1
PM_PAIR_TARGET=0.975
PM_OPEN_PAIR_BAND=0.98
```

profile 说明：

| profile | 用途 | 关键参数 |
| --- | --- | --- |
| `legacy` | 原始 PGT 行为 | 不改硬编码参数 |
| `replay_focused_v1` | 当前 BTC 5m replay 最优 shadow 候选 | 跳过开盘前 75s；seed pair cap `0.980`；early completion cap `0.975`；late completion cap `0.995`；fixed seed clip `57.6` |
| `replay_lower_clip_v1` | 更保守低 clip 候选 | 跳过开盘前 60s；seed pair cap `0.970`；early completion cap `0.975`；late completion cap `1.000`；fixed seed clip `30.0` |
| `xuan_ladder_v1` | 最新 xuan public 行为 shadow 近似 + profit guard | 开盘后 4s 开始；收盘前 25s 停新 first leg；seed pair cap `1.040`；completion cap 按 residual age 分层；fresh residual 不花 repair budget；age >= 90s 可用 `1.010` stale exposure insurance；age >= 45s 或 remaining <= 45s 才允许真实 surplus repair budget，且总 pair cost 仍封顶 `1.030`；亏损闭合后新 first leg 启用 breakeven-path brake；clip 随轮内时间梯度变化 |

Replay profile 下的 `fixed seed clip` 是 replay 搜索里的实际 seed clip；`xuan_ladder_v1` 使用内置时间梯度 clip。它们都会绕过 legacy seed 缩量逻辑，例如 “no immediate completion 时乘 `0.60`” 和 thin-slack clip haircut；否则 shadow 样本会变成不同策略，不能直接验证 replay/xuan 候选。

注意：这些都是 public market-side replay / xuan public trade 行为得出的 shadow profile，不是实盘盈利证明。promotion 到 enforce 前需要用小额实盘验证 maker queue / completion fill truth。

### PGT Shadow Loop 启动控制

连续 fixed BTC PGT shadow 建议使用：

```bash
PM_INSTANCE_ID=xuan_ladder_v1_brake_full \
PM_SHARED_INGRESS_ROOT=/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main \
PM_SHARED_INGRESS_ROLE=auto \
./scripts/run_pgt_shadow_loop.sh btc-updown-5m
```

关键启动参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `PM_PGT_SHADOW_BUILD_ONCE` | `true` | `run_pgt_shadow_loop.sh` 启动时最多 build 一次，后续轮次不在热路径重复 build |
| `PM_PGT_FIXED_AUTO_BUILD` | fixed 单轮默认 `true`；shadow loop 默认 `false` | 单轮 `run_pgt_fixed_shadow_next.sh` 仍会按需 build；连续 loop 内禁用每轮 build，避免外部源码 mtime 变化导致错过开盘 |
| `PM_PGT_FIXED_PRESTART_SECS` | `10` | fixed worker 在目标 round start 前约 10 秒启动；覆盖 Rust 进程启动、shared-ingress client 握手与 feed attach 的热路径开销 |
| `PM_PGT_SHADOW_LOOP_OVERLAP` | `true` | shadow loop 使用 overlapping scheduler，每轮提前调度下一个 fixed worker，避免当前 worker 到 end+grace 后才串行启动下一轮 |
| `PM_PGT_FIXED_INSTANCE_PER_ROUND` | overlap loop 默认 `true` | overlapping worker 按 round timestamp 拆分 `PM_INSTANCE_ID`、log root 与 recorder root，避免前后轮短暂重叠时互相覆盖 |
| `PM_PGT_SHADOW_LOOP_BACKOFF_SEC` | `1` | 正常轮转退出后的重启间隔 |
| `PM_MARKET_WS_HARD_CUTOFF_GRACE_SECS` | shadow loop 默认 `2` | fixed shadow round 收盘后快速退出，避免下一轮迟到 |
| `PM_PGT_SHADOW_REDEEM_LIFECYCLE_ENABLED` | shadow loop 默认 `false` | PGT shadow 轮转默认不跑 post-close redeem lifecycle，redeem 行为单独验证 |
| `PM_CLAIM_MONITOR` | fixed shadow 默认 `false` | PGT shadow 热启动不跑 claim monitor，避免非交易 HTTP 阻塞下一轮开盘 |
| `PM_MIN_ORDER_SIZE` | fixed shadow 默认 `5` | PGT shadow 固定最低下单量，避免每轮启动同步探测 `/books` 拖慢开盘 |

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
| `PM_PAIR_ARB_TIER_1_MULT` | `0.60` | `5 <= |net_diff| < 10` 时主仓侧 avg-cost cap |
| `PM_PAIR_ARB_TIER_2_MULT` | `0.20` | `|net_diff| >= 10` 时主仓侧 avg-cost cap |

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

补充：`oracle_lag_sniping`（5m symbol-universe 试验策略）
- 建议单独设置：
  - `PM_STRATEGY=oracle_lag_sniping`（兼容旧别名 `post_close_hype`）
  - 单市场：`POLYMARKET_MARKET_SLUG=btc-updown-5m`（或其他 `*-updown-5m`）
  - 多市场并发：`PM_MULTI_MARKET_PREFIXES=hype-updown-5m,btc-updown-5m,eth-updown-5m,sol-updown-5m,bnb-updown-5m,doge-updown-5m,xrp-updown-5m`
  - `PM_ORACLE_LAG_SYMBOL_UNIVERSE=hype,btc,eth,sol,bnb,doge,xrp`（或 `*`）
  - `PM_POST_CLOSE_WINDOW_SECS=105`
  - `PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS=8`
  - `PM_POST_CLOSE_GAMMA_POLL_MS=300`
- 运行语义：
  - 先监听 Chainlink RTDS（`crypto_prices_chainlink`）判定胜方并发送 WinnerHint；
  - 若 `+N` 秒内未拿到 Chainlink 胜方提示，则自动切到 Gamma 兜底；
  - 仅在市场结束后窗口内尝试胜方 BUY 报价；仅激活 `PM_ORACLE_LAG_SYMBOL_UNIVERSE` 中的 `*-updown-5m`。
  - 胜方价格 `>0.993` 时不做 taker；maker 仍允许挂单。
- 实盘前操作清单见：
  - [ORACLE_LAG_SNIPING_LIVE_CHECKLIST_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/runbooks/ORACLE_LAG_SNIPING_LIVE_CHECKLIST_ZH.md)

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
