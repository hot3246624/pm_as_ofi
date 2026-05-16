# Xuan Frontier 回测数据源规则 2026-05-15

本规则给 xuan-frontier 研究、回测、automation/heartbeat 使用。目标是避免重复扫大 replay、避免误用降级日期，并统一搜索、验证、公开账户审计三层数据源。

## 日期窗口

当前完整研究窗口：

```text
2026-05-02..2026-05-13
```

禁止自动纳入完整日回测：

```text
2026-05-14
2026-05-15
```

原因：这两天 public market_ws L1/L2 capture 曾降级。除非用户明确声明只做故障取证或局部区间分析，否则不要因为看到 raw/replay/cache/store 目录就使用这两天。

## 数据分层

### 1. Taker-buy strict V2 cache

用途：public/taker-buy 参数搜索。

路径：

```text
/mnt/poly-cache/taker_buy_signal_core_v2_strict_l1/<label>
```

可用条件：

```text
CACHE_MANIFEST.json 存在
label 不包含 20260514 或 20260515
```

当前已核对 label：

```text
20260502_20260507
20260508
20260509
20260510
20260511
20260512
20260513
```

发现命令：

```bash
find /mnt/poly-cache/taker_buy_signal_core_v2_strict_l1 \
  -mindepth 2 -maxdepth 2 -name CACHE_MANIFEST.json \
  -printf '%h\n' | sort | grep -Ev '20260514|20260515'
```

### 2. Completion/unwind event store V2

用途：maker / inventory / completion-unwind 研究；D+/B27/RWO/xuan lifecycle 模型验证。

路径：

```text
/mnt/poly-verification-store/completion_unwind_event_store_v2/<label>/event_store.duckdb
```

表：

```text
completion_unwind_events
```

可用条件：

```text
EVENT_STORE_MANIFEST.json 存在
event_store.duckdb 存在
label 不包含 20260514 或 20260515
```

当前已核对 label：

```text
20260502_20260508
20260509
20260510
20260511
20260512
20260513
```

发现命令：

```bash
find /mnt/poly-verification-store/completion_unwind_event_store_v2 \
  -mindepth 2 -maxdepth 2 -name EVENT_STORE_MANIFEST.json \
  -printf '%h\n' | sort | grep -Ev '20260514|20260515'
```

跨天评估优先使用：

```text
20260502_20260508
20260509
20260510
20260511
20260512
20260513
```

### 3. B27/RWO public account audit

用途：公开成交行为、公开推断 maker/taker 角色、严格 L1/L2 上下文、merge/redeem/settlement 结果。用于校准 B27/RWO 行为和候选模型，不是私有 queue truth。

路径：

```text
/mnt/poly-verification-store/public_account_execution_truth_v1/20260502_20260513/event_store.duckdb
```

表：

```text
public_account_execution_events
```

当前已核对：

```text
row_count = 562087
b27bc fill       = 547345
b27bc merge      = 143
b27bc redeem     = 1936
b27bc settlement = 1968
rwo fill         = 9563
rwo merge        = 56
rwo redeem       = 574
rwo settlement   = 502
```

限制：

```text
public account audit/proxy truth 不能证明私有挂单、私有撤单时间、真实 queue ahead 或真实 maker resting lifetime。
```

## xuan-frontier 使用原则

- 搜索用 strict V2 cache。
- maker/inventory/completion-unwind 研究用 completion_unwind_event_store_v2。
- B27/RWO 行为校准用 public_account_execution_truth_v1。
- 不宽扫 raw/replay/replay_published。
- replay_published 只做少量 finalist audit，不能多个 agent 并发扫。
- 每个 agent/automation 输出到自己的 xuan-frontier 目录。
- 05-14/05-15 默认排除，除非用户明确说修复完成或要求故障取证。

## 当前策略研究影响

当前 D+ reactive-after-public-SELL Rust 映射已被 shadow-realistic verifier 判定为不合格。下一步应把 public account audit 纳入 B27/RWO 行为校准，并把 completion_unwind_event_store_v2 用于 pre-positioned resting two-sided inventory 模型，而不是继续重复 reactive-after-sell D+ 网格。
