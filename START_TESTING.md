# 快速测试（5-10 分钟）

目标：确认 `polymarket_v2` 在你的环境里可稳定接收行情、正确订阅、并按预期进入 dry/live 模式。

## 1. 准备 `.env`

```bash
cp .env.example .env
```

最小 dry 配置：

```bash
POLYMARKET_MARKET_SLUG="btc-updown-15m"
PM_DRY_RUN=true
```

live 额外配置：

```bash
POLYMARKET_PRIVATE_KEY="..."
POLYMARKET_FUNDER_ADDRESS="0x..."
# 可选：POLYMARKET_API_KEY/SECRET/PASSPHRASE
```

注意：

- `POLYMARKET_WS_BASE_URL` 必须是 `.../ws`（程序会再拼 `/market` 或 `/user`）

## 2. Dry Run 启动

```bash
PM_DRY_RUN=true cargo run --bin polymarket_v2
```

预期日志关键字：

- `PREFIX mode` 或 `FIXED mode`
- `Market resolved`
- `Actors spawned`
- `DRY-RUN mode — no orders`

## 3. Live 冒烟测试（小仓）

```bash
PM_DRY_RUN=false cargo run --bin polymarket_v2 --release
```

预期日志关键字：

- `CLOB client authenticated`
- `User WS Listener spawned`
- `REAL FILL`（有成交时）

如果认证失败，程序应直接退出（fail-fast），不会盲跑。

## 4. 多市场脚本

```bash
./start_markets.sh       # dry
./start_markets.sh live  # live
./stop_markets.sh
```

- 日志在 `logs/*.log`
- PID 在 `pids/*.pid`

## 5. 常见错误

- `failed to derive API key`：live 认证链路不通，先回到 dry 检查
- `ws connect timeout`：网络或端点问题
- `owner mismatch`：User WS owner 与 API key 格式不一致

## 6. 下一步

通过冒烟后再进入完整测试清单：`TESTING.md`。
