# 订单簿数据检查（Book / Price Change）

V2 主流程主要使用 WebSocket 订单簿事件，不依赖高频 REST 轮询。

## 1. 关键点

- `book` 事件：代码会扫描所有档位取真正 `best bid/ask`
- `price_change` 事件：增量更新 top-of-book
- 若某侧暂时空档，会回退到 `last_valid_book`

## 2. 验证命令

```bash
PM_DRY_RUN=true cargo run --bin polymarket_v2
```

检查日志是否持续出现：

- `BookTick`
- 有效 bid/ask（非全 0）

## 3. REST 辅助检查（可选）

```bash
curl "https://clob.polymarket.com/book?token_id=<token_id>"
```

用于交叉验证当前盘口，不建议用于高频轮询策略。
