# 价格与数量精度（V2，当前实现）

## 1. 当前实现（以代码为准）

执行层下单前会进行硬性对齐：

- 价格：按 `PM_TICK_SIZE` **向下取整**（floor 到 tick 网格）
- 数量：固定 **2 位小数**（向下取整）
- 若数量取整后 `< 0.01`，直接拒绝发送

对应逻辑（`src/polymarket/executor.rs`）：

```rust
let inv_tick = (1.0 / tick_size).round();
let price_rounded = (price * inv_tick).floor() / inv_tick;
let size_rounded = (size * 100.0).floor() / 100.0;
if size_rounded < 0.01 {
    bail!("size rounds to 0 at 2dp");
}
```

同时，策略层 `safe_price()` 会将目标价钳制在 `[tick, 1-tick]` 合法区间，避免负价或 `>=1` 的非法报价。

## 2. 自愈机制

当交易所返回最小 tick 相关拒单时，执行层会：

1. 从错误文本提取 `minimum tick size`
2. 运行时更新 tick
3. 立即重试一次下单

该机制可降低“配置 tick 与交易所实际 tick 不一致”导致的持续拒单。

## 3. 相关参数

- `PM_TICK_SIZE`：价格粒度（当前模板默认 `0.01`）
- `PM_REPRICE_THRESHOLD`：重报价触发阈值（当前模板默认 `0.010`）

## 4. 建议

1. `PM_TICK_SIZE` 不要设置得比交易所最小粒度更细。
2. 若频繁出现 `order crosses book`，优先调 `PM_POST_ONLY_*` 安全垫，而不是盲目放宽 reprice。
3. 低价区调参时，先确认是“tick/精度拒单”还是“marketable-min 拒单”，两者处理路径不同。

## 5. 排查命令

```bash
rg -n "minimum tick size|invalid.*tick|order crosses book|min size: \\$" logs/*.log
```
