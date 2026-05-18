# Backtest Data Rules 2026-05-15

本文件是 `xuan-research-*` 的本地硬约束。任何后续 D+/B27/RWO 搜索、验证、public account audit 都必须先遵守这些数据边界。

## 可用完整日期

常规研究完整日只使用：

```text
2026-05-02..2026-05-13
```

禁止自动纳入完整日回测：

```text
2026-05-14
2026-05-15
```

原因：这两天 public market_ws L1/L2 capture 曾降级。除非明确声明做故障取证或局部区间分析，否则不得把 20260514/20260515 加入训练、搜索或 OOS 评估。

## 数据层

### Strict V2 Taker-Buy Cache

搜索根：

```text
/mnt/poly-cache/taker_buy_signal_core_v2_strict_l1
```

label 可用条件：

- `<label>/CACHE_MANIFEST.json` 存在。
- label 不包含 `20260514` 或 `20260515`。

当前远端 discovery 已确认可用：

```text
20260502_20260507
20260508
20260509
20260510
20260511
20260512
20260513
```

### Completion/Unwind Event Store V2

研究根：

```text
/mnt/poly-verification-store/completion_unwind_event_store_v2
```

label 可用条件：

- `<label>/EVENT_STORE_MANIFEST.json` 存在。
- `<label>/event_store.duckdb` 存在。
- label 不包含 `20260514` 或 `20260515`。

当前远端 discovery 已确认可用：

```text
20260502_20260508
20260509
20260510
20260511
20260512
20260513
```

### Public Account Execution Truth V1

B27/RWO public-account audit 使用：

```text
/mnt/poly-verification-store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb
```

table：

```text
public_account_execution_events
```

用途：

- B27/RWO public fill、merge、redeem、settle 行为审计。
- public maker/taker inference。
- strict L1/L2 上下文交叉检查。
- 与 D+/completion/inventory 候选同向性检查。

限制：

- 这是 public-account proxy truth。
- 不能证明私有下单、撤单、真实 queue ahead、真实 maker resting lifetime。
- 不能替代我们的 authenticated order/fill/wallet/redeem/cashflow source-of-truth。

## 禁止项

- 不使用旧 `taker_buy_signal_core_v2` non-strict cache。
- 不扫 `/mnt/poly-replay`、`replay_published`、`raw` 或 raw/replay SQLite 做宽搜索。
- 不因看到 20260514/20260515 raw/replay 目录就自动纳入研究。
- 不让多个 agent 共用 output dir。
- 不把 public-account proxy truth 当作私有成交真相。

## 当前 Discovery Artifacts

本地规则 smoke：

```text
/Users/hot/web3Scientist/pm_as_ofi-xuan-research/xuan_research_artifacts/discovery/backtest_input_discovery_local_20260515.json
```

远端 manifest discovery：

```text
/home/ubuntu/xuan_research_runs/backtest_input_discovery_20260515_0906/discovery.json
```

远端 discovery 只读取 manifest 和文件存在性，没有读取 DuckDB 表内容，也没有扫描 replay/raw。
