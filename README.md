# pm_as_ofi

Polymarket crypto up/down 市场做市与库存管理引擎。

当前仓库当前验证主线：
- 推荐测试主策略：`pair_arb`
- 推荐收益验证市场：`btc-updown-15m`
- `glft_mm` 保留为 challenger / research 线

## 当前共享能力

- `OrderSlot(side,direction)` 四槽位执行层：`YesBuy / YesSell / NoBuy / NoSell`
- 共享 OFI 热度/毒性引擎、stale gate、生命周期去抖
- 共享执行治理：`keep-if-safe`、post-only 安全垫、slot 级 cancel/reprice
- 共享风控：`PM_MAX_NET_DIFF`、outcome floor、endgame、recycle、claim

## 内置策略定位

| 策略 | 定位 | 状态 |
| --- | --- | --- |
| `pair_arb` | pair cost + A-S 风格双边买入策略 | 推荐（当前验证主线） |
| `glft_mm` | 真双边、slot-keyed、外锚驱动 | challenger |
| `gabagool_grid` | buy-only utility 基线 | 可用 |
| `gabagool_corridor` | `gabagool_grid` 的 corridor 变体 | research |
| `dip_buy` | 单边抄底 | 实验 |
| `phase_builder` | 分阶段单边建仓 | 实验 |

## 快速开始

1. 复制模板并填写凭证

```bash
cp .env.example .env
```

2. 先跑 dry-run

```bash
cargo run --bin polymarket_v2
```

3. 做静态校验

```bash
cargo check
cargo test --lib
cargo test --bin polymarket_v2
```

4. 小仓 live

```bash
PM_DRY_RUN=false cargo run --bin polymarket_v2 --release
```

## Live Truth Capture

- 默认关闭：只有显式设置 `PM_RECORDER_ENABLED=true` 才会落盘。
- 默认 market 口径是 `structured`，也就是写 `market_md.jsonl`，记录完整 `L1 book + trades`。
- `user_ws.jsonl`、`events.jsonl`、`meta.jsonl` 会继续保留，用于回放真实 order/fill/inventory/tranche/capital 语义。
- `market_ws.jsonl` 只在 `PM_RECORDER_MARKET_MODE=raw` 或 `hybrid` 时写入，作为 parser/debug 取证，不再是默认主路径。

推荐 live 配置：

```bash
PM_DRY_RUN=false \
PM_RECORDER_ENABLED=true \
PM_RECORDER_MARKET_MODE=structured \
cargo run --bin polymarket_v2 --release
```

推荐 dry-run 录制（用于链路冒烟，不发真实 REST 下单）：

```bash
PM_DRY_RUN=true \
PM_RECORDER_ENABLED=true \
PM_RECORDER_MARKET_MODE=structured \
PM_DRY_RUN_FILL_PROBABILITY=1.0 \
cargo run --bin polymarket_v2 --release
```

排查 market parser 或 replay 偏差时再切到：

```bash
PM_RECORDER_MARKET_MODE=hybrid
```

会后离线构建 replay：

```bash
python3 scripts/build_replay_db.py --input-root data/recorder --output-root data/replay --date 2026-04-26
```

dry-run 录制后可做最小验收：

```bash
sqlite3 data/replay/2026-04-26/crypto_5m.sqlite "SELECT count(*) FROM pair_tranche_events;"
sqlite3 data/replay/2026-04-26/crypto_5m.sqlite "SELECT count(*) FROM pair_budget_events;"
sqlite3 data/replay/2026-04-26/crypto_5m.sqlite "SELECT count(*) FROM own_inventory_events;"
```

## 推荐阅读顺序

1. `docs/STRATEGY_V2_CORE_ZH.md`
2. `docs/STRATEGY_PAIR_ARB_ZH.md`
3. `docs/STRATEGY_GLFT_MM_ZH.md`
4. `docs/STRATEGY_GABAGOOL_GRID_ZH.md`
5. `docs/CONFIG_REFERENCE_ZH.md`
6. `docs/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md`
7. `docs/TESTING.md`
8. `docs/ADDING_STRATEGY_ZH.md`

## 当前推荐验证参数基线（pair_arb）

以 `.env.example` 为准，核心值如下：

```env
PM_STRATEGY=pair_arb
POLYMARKET_MARKET_SLUG="btc-updown-15m"
PM_BID_SIZE=5.0
PM_MAX_NET_DIFF=15.0
PM_PAIR_TARGET=0.98
PM_AS_SKEW_FACTOR=0.06
PM_AS_TIME_DECAY_K=1.0
PM_REPRICE_THRESHOLD=0.020
PM_DEBOUNCE_MS=700
PM_OFI_ADAPTIVE=true
PM_OFI_ADAPTIVE_MIN=120.0
PM_OFI_ADAPTIVE_MAX=1800.0
PM_OFI_RATIO_ENTER=0.70
PM_OFI_RATIO_EXIT=0.40
PM_AUTO_CLAIM=true
```

说明：
- 建议先 `5m` 做机制冒烟，再用 `15m` 做收益验证。
- 这是当前准备验证的保守配置，不是利润最大化配置。
- 当前 `pair_arb` 已去掉方向对冲 overlay 和尾盘强制市价对冲路径，运行形态是 `UnifiedBuys`。
- `pair_arb` 的 dry-run 重点看两类日志：
  - `PairArbGate(30s)`：候选保留/跳过/OFI 软塑形
  - `LIVE_OBS`：执行稳定性与 `pair_arb_softened_ratio`
- `glft_mm` 仍可运行，但不建议作为当前生产主线。
