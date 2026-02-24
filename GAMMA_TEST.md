# Gamma 市场解析测试

`polymarket_v2` 会在每轮启动时通过 slug 解析市场（condition id + YES/NO token ids）。

## 1. 手工检查 slug 是否可用

```bash
curl -s "https://gamma-api.polymarket.com/markets?slug=btc-updown-15m-<end_ts>"
```

若返回为空数组，说明该具体窗口还未生成或已过期。

## 2. 程序内验证

```bash
PM_DRY_RUN=true cargo run --bin polymarket_v2
```

日志应包含：

- `Resolving market`
- `Market resolved`
- `YES=... NO=...`

## 3. 轮转验证（prefix）

配置：

```bash
POLYMARKET_MARKET_SLUG="btc-updown-15m"
```

观察到：

- 自动拼接 `btc-updown-15m-<unix>`
- 窗口到期后自动解析下一轮

## 4. 常见问题

- `Failed to resolve market from slug`
  - 前缀写错
  - 当前时间窗口无市场
  - 网络访问 Gamma 失败
