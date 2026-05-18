# Backtest Data Rules 2026-05-15

本文件是 `xuan-research-*` 的本地硬约束。任何后续 D+/B27/RWO 搜索、验证、public account audit 都必须先遵守这些数据边界。

## 2026-05-19 本地数据根

当前回测数据已改为本地已下载 cache/store。默认根目录：

```bash
export POLY_BT_ROOT=/Users/hot/web3Scientist/poly_backtest_data
```

所有 agent 应从 manifest 自动发现可用 label，不要手写日期集合：

```bash
find "$POLY_BT_ROOT/backtest_cache/taker_buy_signal_core_v2_strict_l1" \
  -maxdepth 2 -name CACHE_MANIFEST.json \
  | sed 's#/CACHE_MANIFEST.json##' | sort

find "$POLY_BT_ROOT/verification_store/completion_unwind_event_store_v2" \
  -maxdepth 2 -name EVENT_STORE_MANIFEST.json \
  | sed 's#/EVENT_STORE_MANIFEST.json##' | sort
```

只能使用本地已下载且有 manifest 的 cache/store。不要扫 collector，不要扫 raw，不要扫 replay。`20260518` 目录未出现完整 manifest 前不得纳入样本。

## 可用完整日期

常规研究完整日只使用：

```text
2026-05-02..2026-05-13
2026-05-16
2026-05-17
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
$POLY_BT_ROOT/backtest_cache/taker_buy_signal_core_v2_strict_l1
```

label 可用条件：

- `<label>/CACHE_MANIFEST.json` 存在。
- label 不包含 `20260514` 或 `20260515`。

当前本地 manifest discovery 已确认可用：

```text
20260502_20260507
20260508
20260509
20260510
20260511
20260512
20260513
20260516
20260517
```

参数搜索示例：

```bash
cd /Users/hot/web3Scientist/poly_trans_research

LABEL=20260513
CACHE="$POLY_BT_ROOT/backtest_cache/taker_buy_signal_core_v2_strict_l1/$LABEL"
OUT="/tmp/${USER}_taker_buy_search_$LABEL"

uv run --with duckdb python scripts/search_taker_buy_signal_candidate_cache_v2.py \
  --cache-dir "$CACHE" \
  --output-dir "$OUT" \
  --top-n 100 \
  --min-rows 50
```

每个 agent 必须使用自己的 `OUT`，不要共用输出目录。

### Completion/Unwind Event Store V2

研究根：

```text
$POLY_BT_ROOT/verification_store/completion_unwind_event_store_v2
```

label 可用条件：

- `<label>/EVENT_STORE_MANIFEST.json` 存在。
- `<label>/event_store.duckdb` 存在。
- label 不包含 `20260514` 或 `20260515`。

当前本地 manifest discovery 已确认可用：

```text
20260502_20260508
20260509
20260510
20260511
20260512
20260513
20260516
20260517
```

DuckDB 读取示例：

```bash
export STORE="$POLY_BT_ROOT/verification_store/completion_unwind_event_store_v2/20260513/event_store.duckdb"

uv run --with duckdb python - <<'PY'
import duckdb, os
store = os.environ["STORE"]
con = duckdb.connect(store, read_only=True)
print(con.execute("select count(*) from events").fetchall())
print(con.execute("select day, count(*) from events group by day order by day").fetchall())
PY
```

### Public Account Execution Truth V1

B27/RWO public-account audit 使用：

```text
$POLY_BT_ROOT/verification_store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb
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
- 不是 private owner-trade truth。
- 不能证明私有下单、撤单、真实 queue ahead、真实 maker resting lifetime。
- 不能替代我们的 authenticated order/fill/wallet/redeem/cashflow source-of-truth。

## 禁止项

- 不使用旧 `taker_buy_signal_core_v2` non-strict cache。
- 不扫 collector。
- 不扫 `/mnt/poly-replay`、`replay_published`、`raw` 或 raw/replay SQLite 做宽搜索。
- 不因看到 20260514/20260515 raw/replay 目录就自动纳入研究。
- 不把 `20260518` 纳入样本，除非本地对应目录已经出现完整 manifest 且主线程确认可用。
- 不让多个 agent 共用 output dir。
- 不把 public-account proxy truth 当作私有成交真相。
- 不把当前本地 cache/store 结论写成最终 source-of-truth replay 验证结论；最终 replay 验证需要等 replay archive 下载后单独排队。

## 当前 Discovery Artifacts

本地规则 smoke / preflight：

```text
/Users/hot/web3Scientist/pm_as_ofi-xuan-research/xuan_research_artifacts/discovery/backtest_input_discovery_local_20260515.json
```

历史远端 manifest discovery：

```text
/home/ubuntu/xuan_research_runs/backtest_input_discovery_20260515_0906/discovery.json
```

所有 discovery/preflight 只读取 manifest 和文件存在性；除明确的 DuckDB 研究脚本外，不读取 DuckDB 表内容，也不扫描 collector/replay/raw。
