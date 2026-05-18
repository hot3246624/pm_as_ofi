# Xuan-Like M0001 Maker Shadow Runner Spec

更新时间：2026-05-12

## 目的

把当前最强研究候选 `c6_e022 + conditional blocked-lot skip age>=120s margin=0.001`
从离线/P2/只读 queue proxy 推进到可验证的 maker-like shadow runner。

这个 runner 仍然不是 live 策略，不允许真实下单。第一版只做 dry-run / shadow
核对，目标是确认三件事：

1. 真实挂单/队列语境下能否拿到接近 `2.0-2.2c` 的 first-leg improvement。
2. fill 之后 completion 是否能保持 xuan-like pair_cost、pair delay、残余结构。
3. 未 fill、部分 fill、completion 失败时是否能避免残仓尾部扩大。

## 当前证据

离线 P2 05-02..05-08：

- ROI `2.0599%`，约为 xuan ROI `93%`
- pair_cost `0.978100`，接近 xuan `0.978292`
- rounds/market `5.945`
- qty residual `0.1665%`
- pair delay `1.65s`
- stress100 `+10.25`

P2 future-fill 证据：

- trigger 当下 touch support 很低：`1.10%` actions / `2.17%` qty
- seed 后 120s 内 same-side bid touch/cross 虚拟价：`83.43%` actions / `82.12%` qty
- seed 后 120s 内 public SELL price touch/cross 虚拟价：`77.96%` actions / `76.40%` qty

Queue-aware 只读 probe 合并到 v5：

- `e0.022 q0.5`: candidates `222`, fill_rate `68.0%`, filled-cost proxy ROI `2.33%`,
  pair_cost p50/p90 `0.98323/0.98660`, residual qty/cost `15.63/8.58`
- `e0.022 q1.0`: candidates `220`, fill_rate `69.5%`, filled-cost proxy ROI `2.31%`,
  pair_cost p50/p90 `0.98190/0.98695`, residual qty/cost `15.63/8.58`

结论：edge 质量仍然成立，但实盘差距主要来自 fill participation、completion cost、
completion_per_fill 和残余尾部，不是单纯缺少 edge。

## Runner 状态机

### Seed lane

只在以下条件同时满足时生成虚拟 maker seed：

- `public_trade_taker_side == SELL`
- seed side 是 high side
- `trade_px` 在 `[0.05, 0.90]`
- `trade_px + opposite_ask <= 1.02`
- offset `< 240s`
- market cycle count `< 6`
- 距离上次 seed 至少 `5000ms`
- opposite-side inventory 不超过 dust
- material residual 未解决时禁止新 seed
- open cost 不超过 `$250`

虚拟 seed price：

```text
seed_px = max(0.01, public_trade_px - 0.022)
```

目标 size：

```text
target = 115 shares when offset < 180s
target = 57.5 shares when 180s <= offset < 240s
target = 0 when offset >= 240s
qty = min(60, public_trade_size * 0.25, target-current_qty, remaining_open_cost/seed_px)
```

### Fill accounting

第一版 shadow 必须区分三种 fill：

- `real_fill`: 真实 dry-run/order-manager 回报的 fill。
- `queue_supported_fill`: 后续 same-side SELL 在/穿过虚拟价，并按 queue_share 计入的 proxy fill。
- `touch_only`: 盘口或成交价触到，但无队列/真实 fill 证据。

策略指标只允许用 `real_fill` 或 `queue_supported_fill` 计算，不允许用
`touch_only` 当成交。

必须记录：

- seed event id、market、side、offset、trade_px、seed_px、qty、edge
- queue wait、first touch wait、first trade-through wait
- fill qty、partial fill qty、unfilled qty
- cancel reason：TTL、offset cutoff、adverse move、material residual lockout

### Completion lane

completion 优先级高于新 seed。

对当前 row side 的 buy opportunity，优先配对 opposite-side inventory。

cap：

```text
base_cap = 0.990
late_cap = 0.995 when offset >= 240s
final_cap = 1.010 when offset >= 270s
```

conditional blocked-lot skip：

- FIFO lot 如果 pair_cost 超 cap
- blocked lot age `>=120s`
- younger lot pair_cost `<= cap - 0.001`
- 允许跳过 blocked lot 先配 younger lot
- 被跳过 lot 必须保留并进入 residual classification

必须记录：

- completion event id、buy_px、paired_qty、pair_cost、pair_delay
- 是否发生 skip、blocked lot age/cost、younger lot margin
- completion_per_fill、qty_pair_share_of_filled、pair_cost p50/p90

### Residual control lane

不追求零残仓，但必须追 xuan-like residual profile。

Material residual 定义：

```text
residual_qty > 6 shares or residual_cost > 6 USD
```

一旦出现 material residual：

- 禁止同 market 新 seed
- 继续允许 completion priority
- 进入 aged residual unwind 观察态
- 记录是否有 affordable completion opportunity

第一版不使用全局加 cap 的 salvage，因为离线测试显示 +0.004/+0.008/+0.012
只触发 1-3 次，未降低残余尾部，还略伤 PnL/stress。

可测试的 residual repair 分支：

- `weak_fill_small_target`: 若最近 N 个 candidate fill_rate 低于阈值，target 降为 `57.5/30`
- `short_ttl_weak_queue`: 若 queue touch p90 变差，TTL 从 `120s` 降至 `60s`
- `material_residual_lockout`: material residual 未解决时禁止任何新 seed
- `aged_unwind_observe`: 只记录 aged unwind opportunity，不先执行强制 unwind

## 必报指标

每轮 shadow 汇总必须报告：

- candidates、fill_rate、filled_qty、unfilled_qty
- completion_per_candidate、completion_per_fill
- qty_pair_share_of_filled
- pair_cost p50/p90、pair_delay p50/p90
- ROI_on_seed_cost、ROI_on_filled_cost
- residual qty/cost、material residual market count
- top residual markets/lots
- P2 delta context：book_update_reason、bid/ask delta、level drop/lift
- 与 xuan benchmark 的 XEI/XS 粗略估计

## 当前推进判断

现阶段不要继续只堆被动 probe。下一步应该实现/接入 runner spec，
让 dry-run 日志具备真实 fill / queue-supported fill / touch-only 的三分法，
再用同一套 ledger 评估。否则离线 ROI 和 queue proxy 之间会一直有解释缺口。
