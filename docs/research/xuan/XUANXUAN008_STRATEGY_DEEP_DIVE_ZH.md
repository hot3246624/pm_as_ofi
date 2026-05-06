# `xuanxuan008` 策略深度解析：Pair-Gated Tranche 视角

生成时间：`2026-04-25`
研究对象：`xuanxuan008` / `Little-Fraction`
公开钱包：`0xcfb103c37c0234f524c632d964ed31f117b5f694`

本文只做策略还原与证据分级，不设计我方新策略，不评价 `v3.5` 工程方案。

---

## 1. 研究口径

### 1.1 数据来源

- Polymarket profile: `https://polymarket.com/zh/@xuanxuan008`
- Public search: `https://gamma-api.polymarket.com/public-search?q=xuanxuan008&search_profiles=true&limit_per_type=10`
- Public trades: `https://data-api.polymarket.com/trades?user=0xcfb103c37c0234f524c632d964ed31f117b5f694&limit=1000`
- Public activity: `https://data-api.polymarket.com/activity?user=0xcfb103c37c0234f524c632d964ed31f117b5f694&limit=1000`
- Public positions / closed positions

公开 `data-api` 对历史 offset 有限制；本次能稳定重建的是最近约 `4000` 笔公开 trades，不代表完整生涯。

### 1.2 最新可拉窗口

本次刷新时间：`2026-04-25 09:30:09 UTC`

| 指标 | 数值 |
|------|------|
| trades | `4000` |
| markets | `367` |
| 时间窗 UTC | `2026-04-24 00:27:22` 到 `2026-04-25 09:29:46` |
| side | `BUY = 4000/4000` |
| outcome | `Up = 1999`, `Down = 2001` |

### 1.3 证据等级

- `A`：公开数据直接确认。
- `B`：公开数据强支持，但仍有替代解释。
- `C`：当前只是合理假设，需要 maker/taker、order timeline 或同步 book 数据裁决。

---

## 2. 最核心结论

`xuanxuan008` 看起来“总能配对成功”，不是因为每一对都严格用固定 `UP + Down < 1` 的价格完成，也不是因为能预测未来盘口。

更可信的机制是：

```text
低残差状态开一条 first leg
进入 completion-only
只允许 opposite leg 修复
残差回到阈值内后，才允许开启下一条 tranche
折扣 pair 积累 surplus
高成本 repair pair 消耗 surplus
盘中滚动 MERGE 回收资金
```

也就是说，它是 **Pair-Gated Tranche Automaton**：

- `pair-gated`：active tranche 没被 opposite 覆盖前，基本不继续开同侧风险。
- `tranche`：单个 round 内不是一个大 cycle，而是很多小 episode。
- `automaton`：行为像状态机，不像单个固定 `pair_target` 公式。

---

## 3. “一对一推进”的真实形态

### 3.1 它不是数学精确 1:1

公开成交的数量不会刚好相等。比如：

```text
BUY Up   121.85
BUY Down 118.72
```

这不应被判定为“未完成配对”。真正需要看的不是精确归零，而是：

```text
abs(live_up - live_down) <= residual_eps
```

因此本文用两个口径重建 episode：

- 严格口径：`residual_eps = 10` shares
- 宽松敏感性口径：`residual_eps = 25` shares

### 3.2 Episode 定义

```text
Flat / Residual:
  abs(net_diff) <= residual_eps
  下一笔 BUY 视为开启新 episode

Active First Leg:
  已有未覆盖 first leg

Completion:
  opposite BUY 逐步覆盖 first leg

Closed:
  abs(net_diff) <= residual_eps
```

如果在 `Active First Leg` 状态下继续买同侧，则记为：

```text
same_side_add_before_covered
```

这就是判断“一对一推进”是否成立的核心指标。

### 3.3 Episode 重建结果

| 指标 | `eps=10` | `eps=25` |
|------|----------|----------|
| open episode 数 | `1488` | `1822` |
| closed episode 数 | `1249` | `1742` |
| clean closed episode 数 | `1190` | `1671` |
| clean closed ratio | `95.28%` | `95.92%` |
| same-side add trade 数 | `409` | `132` |
| same-side add qty ratio | `9.52%` | `3.08%` |
| close delay 中位数 | `12s` | `12s` |
| close delay p90 | `56s` | `55.8s` |

结论等级：`A/B`

解释：

- 如果按 `10` 份额残差容忍，已闭合 episode 中约 `95%` 是“没有同侧追加就完成覆盖”。
- 如果按 `25` 份额残差容忍，同侧追加成交量只占约 `3%`。
- 所以它不是精确的“买一笔 Up，再买完全等量 Down”，而是“允许小尾差的一对一推进”。

---

## 4. Pair Cost 为什么变化无穷

用户实践中最难的点是：`UP + Down` 不能固定。公开数据也支持这一点。

FIFO 近似配对结果：

| 指标 | 数值 |
|------|------|
| matched pair 数 | `3629` |
| pair cost p10 | `0.8100` |
| pair cost median | `0.9865` |
| pair cost p90 | `1.1200` |
| FIFO 估算 pair PnL | `+5422.02` |

按 pair cost 分桶：

| pair cost | qty | profit |
|----------|-----|--------|
| `<= 0.95` | `95167.51` | `+13890.56` |
| `0.95 - 1.00` | `57038.96` | `+1254.43` |
| `1.00 - 1.04` | `51322.02` | `-1017.88` |
| `> 1.04` | `68483.22` | `-8705.10` |

结论等级：`A/B`

这说明：

- 他确实有很多高成本补腿。
- 高成本补腿不是错误本身，而是 repair cost。
- 只要低成本 pair 的 surplus 足以覆盖 repair loss，整体仍然赚钱。

因此把策略理解成：

```text
每一对都必须 UP + Down <= 固定 pair_target
```

会跑偏。

更贴近公开路径的是：

```text
cohort_net_pair_pnl = discount_pair_surplus - repair_pair_loss
```

`pair_target` 不是每一对的硬阈值，而是 cohort 层的预算纪律。

---

## 5. 它为什么能“继续开新仓”

从公开成交顺序看，它大概率满足下面的推进纪律：

```text
1. 只有 residual 很小时，才允许新 first leg。
2. 一旦 first leg 出现，进入 completion-only。
3. completion-only 期间只买 opposite。
4. opposite 覆盖到 residual_eps 内后，才进入下一 episode。
5. 如果补腿成本偏高，则消耗 surplus bank，而不是无限追价。
```

这解释了两个表面矛盾：

### 5.1 为什么它能几乎配上

因为它不让 first leg 累积成大单腿。

它控制的是：

- `same_side_add_qty_ratio`
- `episode_close_delay`
- `residual_before_new_open`
- `live_unpaired_qty`

而不是单看 round 结束时的 `up_qty == down_qty`。

### 5.2 为什么它有时 `UP + Down = 1.04+` 还能赚钱

因为高成本补腿是局部修复；它必须由之前的低成本 pair surplus 支付。

也就是说：

```text
低价 first leg 或低价 completion leg 形成 surplus
后续高价 completion leg 消耗 surplus
最终 cohort 净收益为正
```

---

## 6. 滚动 MERGE 的角色

公开 activity 显示它大量使用：

- `TRADE`
- `MERGE`
- `REDEEM`

这支持：

```text
BUY Up
BUY Down
MERGE pairable full sets
REDEEM residual / winning payout
```

`MERGE` 的核心作用不是证明 tranche 已闭合，而是：

- 回收抵押资金。
- 降低后续 capital pressure。
- 把已配对部分从风险库存中移除。

所以必须区分：

```text
tranche close: 会计意义上的 pair-covered
merge: 链上 full-set 回收动作
```

这点对复刻非常关键。把 `merge` 当成 `close` 会导致归因错误。

---

## 7. 最可信策略还原

```text
Universe:
  BTC 5m, only in selected high-liquidity sessions

Entry:
  when round is fresh and residual inventory is small
  open small first leg

Completion:
  while active tranche exists:
    suppress same-side risk-increasing orders
    prioritize opposite leg
    allow variable completion price within repair budget

Budget:
  cheap pairs create surplus
  expensive completion consumes surplus
  no unlimited rescue

Harvest:
  when enough pairable full sets exist:
    MERGE
  after resolution:
    REDEEM leftover
```

---

## 8. 当前未裁决问题

### 8.1 Maker 还是 taker

等级：`C`

公开 `data-api` 的 `side=BUY` 只说明买卖方向，不说明 maker/taker。

需要：

- authenticated CLOB `/data/trades`
- `trader_side`
- `maker_orders`
- `taker_order_id`
- own order lifecycle

### 8.2 预埋双边，还是触发后补腿

等级：`C`

公开成交时间不是挂单时间。高度交替可以由两种机制产生：

- 预先挂双边小 ladder。
- 一侧成交后快速补 opposite。

需要：

- open order timeline
- user channel replay
- 同步 book + trade tape

### 8.3 Clip size 是否 depth-aware

等级：`C`

公开数据确认 clip size 不固定，但不能证明它按盘口深度调节。

需要：

- fill 前后的 book depth
- best bid/ask queue
- same-second tape

---

## 9. 最终判断

`xuanxuan008` 的 edge 最像：

```text
Pair-gated small-tranche completion
+ repair-budgeted variable pair cost
+ rolling merge capital recycling
```

不是：

```text
固定 pair_target
固定 30 秒必配平
大 first leg 后神奇救腿
明确主动保留赢家残差
```

工程上如果要学习它，第一优先级不是调一个更聪明的 `pair_target`，而是建立：

- tranche 账本
- completion-only 状态机
- surplus / repair budget
- residual-gated new open
- episode-level metrics
