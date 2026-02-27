# 价格与数量精度（V2）

## 1. 当前实现

`src/polymarket/executor.rs` 下单前会主动舍入：

- 价格：3 位小数（`0.001`）
- 数量：6 位小数

实现逻辑：

```rust
let price_rounded = (price * 1000.0).round() / 1000.0;
let size_rounded = (size * 1_000_000.0).round() / 1_000_000.0;
```

## 2. 为什么这样做

- 避免浮点尾差导致订单被拒
- 与 CLOB 常见精度约束对齐
- 保持 maker 重报价在可预测的网格上

## 3. 相关参数

- `PM_TICK_SIZE`：策略报价步长（默认 `0.001`）
- `PM_REPRICE_THRESHOLD`：触发重报价的偏移阈值（默认 `0.005`）

说明：

- `PM_TICK_SIZE` 影响“目标价怎么计算”
- 实际发单仍会经过最终舍入

## 4. 建议

- 不要把 `PM_TICK_SIZE` 设得比市场最小精度更细
- 如果频繁出现最小 tick 相关拒单，先检查：
  - 市场是否发生 `tick_size_change`
  - 你的 `PM_TICK_SIZE` 是否需要动态调整

## 5. 排查命令

```bash
rg -n "INVALID_ORDER_MIN_TICK_SIZE|invalid.*tick" logs/*.log
```
