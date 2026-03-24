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
- `glft_mm` 时 Binance 外锚正常连接
- 不发真实订单
- 日志里能看到 keep-if-safe / OFI / stale / endgame 的自然行为

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
- `src/polymarket/glft.rs`
- `src/polymarket/strategy/glft_mm.rs`
- `src/polymarket/coordinator_execution.rs`
- `src/polymarket/ofi.rs`
- `src/bin/polymarket_v2.rs`

## 5. 关键观测项

### 执行层
- `placed / cancel / reprice` 比例
- 是否仍有撤改单风暴
- `crosses book` 是否成串出现

### GLFT
- `fit_quality` 是否从 `Warm` 进入 `Ready`
- Binance stale 时是否静默
- `A/k` 是否异常跳变

### OFI
- threshold 是否过快上升
- toxic 是否长时间不恢复
- recovery 后是否立即再次 kill

### 资本循环
- recycle 是否只在真正余额压力下触发
- claim 是否在回合窗口内完成
