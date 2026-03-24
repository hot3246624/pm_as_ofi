# pm_as_ofi

Polymarket crypto up/down 市场做市与库存管理引擎。

当前仓库已经收敛到一条明确主线：
- 推荐实盘主策略：`glft_mm`
- 推荐市场：`btc/eth/xrp/sol` 的 `*-updown-5m`
- 其它内置策略仅保留为基线、回放或实验，不再作为默认上线方案

## 当前主线能力

- `OrderSlot(side,direction)` 四槽位执行层：`YesBuy / YesSell / NoBuy / NoSell`
- Binance `aggTrade` 外部锚 + Polymarket 本地盘口/成交融合
- GLFT 强度拟合：`10s refit / 30s window`
- OFI 同时进入 `alpha + spread`，并保留共享 kill-switch
- 共享执行治理：`keep-if-safe`、post-only 安全垫、slot 级 cancel/reprice
- 共享风控：`PM_MAX_NET_DIFF`、outcome floor、endgame、recycle、claim

## 内置策略定位

| 策略 | 定位 | 状态 |
| --- | --- | --- |
| `glft_mm` | 真双边、slot-keyed、5m crypto 主线 | 推荐 |
| `gabagool_grid` | buy-only 基线 | parked |
| `gabagool_corridor` | `gabagool_grid` 的 corridor 变体 | research |
| `pair_arb` | 旧 fair-value maker | 软弃用 |
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

## 推荐阅读顺序

1. `docs/STRATEGY_V2_CORE_ZH.md`
2. `docs/CONFIG_REFERENCE_ZH.md`
3. `docs/GO_LIVE_5M_CHECKLIST_ZH.md`
4. `docs/TESTING.md`
5. `docs/ADDING_STRATEGY_ZH.md`

## 当前推荐 5m 参数基线

以 `.env.example` 为准，核心值如下：

```env
PM_STRATEGY=glft_mm
POLYMARKET_MARKET_SLUG="btc-updown-5m"
PM_BID_SIZE=5.0
PM_MAX_NET_DIFF=15.0
PM_PAIR_TARGET=0.985
PM_MAX_PORTFOLIO_COST=1.02
PM_REPRICE_THRESHOLD=0.020
PM_DEBOUNCE_MS=700
PM_HEDGE_DEBOUNCE_MS=100
PM_GLFT_GAMMA=0.10
PM_GLFT_XI=0.10
PM_GLFT_OFI_ALPHA=0.30
PM_GLFT_OFI_SPREAD_BETA=1.00
PM_OFI_ADAPTIVE=true
PM_OFI_ADAPTIVE_K=4.2
PM_OFI_ADAPTIVE_MIN=120.0
PM_OFI_ADAPTIVE_MAX=1800.0
PM_OFI_RATIO_ENTER=0.70
PM_OFI_RATIO_EXIT=0.40
PM_AUTO_CLAIM=true
```

说明：
- 这是当前准备实盘测试的保守配置，不是利润最大化配置。
- 代码 fallback 默认策略仍是 `gabagool_grid`；但模板和实盘建议统一使用 `glft_mm`。
- 如果 Binance 外锚失效，`glft_mm` 会主动静默并清空四槽位，不会回退到纯 Poly 报价。
