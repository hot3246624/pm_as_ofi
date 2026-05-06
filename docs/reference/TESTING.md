# 测试手册

## 1. 静态检查

```bash
cargo check
cargo test --lib
cargo test --bin polymarket_v2
```

这是当前提交前的最低门槛。

## 2. Dry-run

```bash
cargo run --bin polymarket_v2
```

期望：
- 市场解析成功
- Market WS 正常
- `pair_arb` 下正常输出双边 `Buy` 意图
- 不发真实订单
- 日志里能看到 keep-if-safe / OFI / stale 的自然行为
- `15m` 轮次里能看到 `PairArbGate(30s)` 与 `LIVE_OBS`

推荐顺序：
1. `btc-updown-5m` 跑 2-3 轮机制冒烟
2. 切到 `btc-updown-15m` 跑收益验证样本（参数冻结）

## 3. Live 冒烟

```bash
PM_DRY_RUN=false cargo run --bin polymarket_v2 --release
```

期望：
- REST / User WS 认证成功
- 有 slot 级别的下单与状态回传
- `REAL FILL` 后库存与净仓变化正确
- 拒单有明确分类，不应悄悄吞掉

## 4. 重点回归场景

每次改下面这些模块，都应至少重跑一轮 dry-run：
- `src/polymarket/strategy/pair_arb.rs`
- `src/polymarket/coordinator_execution.rs`
- `src/polymarket/ofi.rs`
- `src/bin/polymarket_v2.rs`

## 5. 关键观测项

### 执行层
- `placed / cancel / reprice` 比例
- 是否仍有撤改单风暴
- `crosses book` 是否成串出现

### pair_arb
- 是否始终保持 buy-only（无 `Hedge` / `OneShotTakerHedge`）
- 双边报价是否符合 `pair_target + tier avg-cost cap + VWAP ceiling` 语义
- `residual_inventory_cost_end` 是否可控
- `PairArbGate(30s)` 是否合理：
  - `attempts / keep / keep_rate`
  - `skip(inv/sim/util/edge)`
  - `ofi(softened/suppressed)`
- `LIVE_OBS` 是否合理：
  - `replace_ratio`
  - `reprice_ratio`
  - `ref_blocked_ms`
  - `heat_events`
  - `pair_arb_softened_ratio`

### OFI
- 对 `pair_arb`，重点不是“有没有热度”，而是：
  - 是否只塑形 same-side risk-increasing buy
  - 是否没有误伤 pairing buy
  - `softened/suppressed` 是否与实际盘口环境相符

### 资本循环
- recycle 是否只在真正余额压力下触发
- claim 是否在回合窗口内完成

## 6. dry-run 与收益判断的边界

`dry-run` 只能验证：
- 生命周期是否稳定
- 策略是否按预期出意图
- 执行治理是否没有失控

`dry-run` 不能验证：
- 真实 fill edge
- 残仓损失
- 真实盈利能力

所以：
- `5m dry-run` 主要看机制
- `15m dry-run` 主要看策略行为是否合理
- 真正的 go/no-go 仍要靠小额 live 样本
