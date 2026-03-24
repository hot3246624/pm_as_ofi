# 5m 实盘验收清单（当前主线）

适用范围：
- `PM_STRATEGY=glft_mm`
- `btc/eth/xrp/sol` 的 `*-updown-5m`

## 1. 推荐参数快照

```env
PM_DRY_RUN=false
POLYMARKET_MARKET_SLUG="btc-updown-5m"

PM_STRATEGY=glft_mm
PM_BID_SIZE=5.0
PM_MAX_NET_DIFF=15.0
PM_PAIR_TARGET=0.985

PM_TICK_SIZE=0.01
PM_POST_ONLY_SAFETY_TICKS=2.0
PM_POST_ONLY_TIGHT_SPREAD_TICKS=3.0
PM_POST_ONLY_EXTRA_TIGHT_TICKS=1.0
PM_REPRICE_THRESHOLD=0.020
PM_DEBOUNCE_MS=700

PM_GLFT_GAMMA=0.10
PM_GLFT_XI=0.10
PM_GLFT_OFI_ALPHA=0.30
PM_GLFT_OFI_SPREAD_BETA=1.00
PM_GLFT_INTENSITY_WINDOW_SECS=30
PM_GLFT_REFIT_SECS=10

PM_OFI_WINDOW_MS=3000
PM_OFI_TOXICITY_THRESHOLD=300.0
PM_OFI_ADAPTIVE=true
PM_OFI_ADAPTIVE_K=4.2
PM_OFI_ADAPTIVE_MIN=120.0
PM_OFI_ADAPTIVE_MAX=1800.0
PM_OFI_ADAPTIVE_RISE_CAP_PCT=0.20
PM_OFI_ADAPTIVE_WINDOW=200
PM_OFI_RATIO_ENTER=0.70
PM_OFI_RATIO_EXIT=0.40
PM_OFI_HEARTBEAT_MS=200
PM_OFI_EXIT_RATIO=0.85
PM_OFI_MIN_TOXIC_MS=800
PM_TOXIC_RECOVERY_HOLD_MS=1200

PM_ENDGAME_SOFT_CLOSE_SECS=60
PM_ENDGAME_HARD_CLOSE_SECS=30
PM_ENDGAME_FREEZE_SECS=2

PM_RECYCLE_ENABLED=true
PM_RECYCLE_ONLY_HEDGE=false
PM_RECYCLE_TRIGGER_REJECTS=2
PM_RECYCLE_TRIGGER_WINDOW_SECS=90
PM_RECYCLE_PROACTIVE=true
PM_RECYCLE_POLL_SECS=5
PM_RECYCLE_COOLDOWN_SECS=120
PM_RECYCLE_MAX_MERGES_PER_ROUND=2
PM_RECYCLE_LOW_WATER_USDC=6.0
PM_RECYCLE_TARGET_FREE_USDC=18.0
PM_RECYCLE_MIN_BATCH_USDC=10.0
PM_RECYCLE_MAX_BATCH_USDC=30.0
PM_RECYCLE_SHORTFALL_MULT=1.2
PM_RECYCLE_MIN_EXECUTABLE_USDC=5.0

PM_AUTO_CLAIM=true
PM_AUTO_CLAIM_DRY_RUN=false
PM_AUTO_CLAIM_ROUND_WINDOW_SECS=30
PM_AUTO_CLAIM_ROUND_RETRY_SCHEDULE=0,2,5,9,14,20,27
PM_AUTO_CLAIM_ROUND_SCOPE=ended_then_global
PM_AUTO_CLAIM_WAIT_CONFIRM=false
```

## 2. 上线前硬验收

必须全部满足：

1. `cargo check` 通过。
2. `cargo test --lib` 通过。
3. `cargo test --bin polymarket_v2` 通过。
4. `PM_STRATEGY=glft_mm` 已显式写入 `.env`，不要依赖代码 fallback。
5. `POLYMARKET_PRIVATE_KEY`、`POLYMARKET_FUNDER_ADDRESS` 正确。
6. 若启用自动 claim / merge，Builder 三件套完整。

## 3. Dry-run 验收

建议至少 `20-30` 分钟。

必须看到：
- Binance 外锚连接日志
- `GLFT signal engine active`
- `fit_quality=Warm/Ready` 的正常推进
- 没有持续 cancel/place 风暴
- OFI threshold 不再无限飘升
- 市场轮转后仍能继续初始化 GLFT

重点看：
- `reprice` 是否显著低于旧版本
- `crosses book` 是否不再连发
- `No User WS — net_diff stays 0` 仅出现在 dry-run

## 4. 首轮实盘验收

先跑 `3-5` 轮，小仓。

必须检查：
1. `abs(net_diff)` 不持续越过 `15`。
2. 四槽位有正常挂撤，不出现单侧永久卡死。
3. Binance stale 时，系统应静默，而不是胡乱回退定价。
4. 余额不足时 recycle 有完整闭环日志。
5. 每轮结束后 claim runner 有 `start/retry/result` 闭环。
6. 尾盘阶段当前应理解为“风险增加型 slot 被关掉”，不要按旧 hedge-repair / taker rescue 预期去验收。

## 5. 立即停机条件

满足任一条，直接回到 dry-run：

1. 同一轮出现异常连续 taker 去风险。
2. OFI 长时间把系统锁死，且阈值明显失真。
3. Binance 外锚失效后系统没有静默。
4. recycle / claim 连续失败但系统继续盲跑。
5. 新市场切换后 actor 停摆或日志明显中断。
