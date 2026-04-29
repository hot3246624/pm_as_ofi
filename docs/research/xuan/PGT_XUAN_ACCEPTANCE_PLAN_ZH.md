# PGT/xuan Shadow 验收计划

更新时间：2026-04-29 21:27 CST

目标：在不影响 `oracle_lag_sniping` / `pair_arb` / `glft_mm` 的前提下，将 `pair_gated_tranche_arb` 的 BTC 5m shadow 行为收敛到接近 xuanxuan008 的 completion-first 特征，并确认是否具备进入更长期 shadow soak 的条件。

## 验收表

| 序号 | 验收项 | 门槛 | 当前进度 | 状态 | 下一步 |
| --- | --- | --- | --- | --- | --- |
| A1 | shared-ingress fixed BTC book lane | `wire_book_ticks > 0` 且 `coord_partial_forwards > 0`，无 fixed feed connect/shutdown 风暴 | 已由最新 fixed-mode 样本验证可正常出 book/seed | 通过 | 持续观察 broker/client 日志 |
| A2 | seed 初始挂单稳定性 | 每 30s `dispatch_place` 应接近 0-2，`retain` 为主；不得出现数百次撤挂 | `1777465200` 修复后多窗口 `place=0/1`、`retain` 为主 | 通过 | 长样本确认 |
| A3 | seed latch release | 单边 seed 最迟约 30s 释放到双边，避免 90s 单边暴露 | 已从样本看到约 30s 后释放双边 | 通过 | 继续累计样本 |
| A4 | first fill 捕获 | `seed_exposed_fill_ratio` 持续提升；短样本不作为硬门槛 | 近期 5 个完整样本约 40%，样本少 | 观察中 | 至少再收集 10-20 轮 |
| A5 | same-side add | `same_side_add_qty_ratio` 目标约 0.05-0.15，且 `MAX_SAME_SIDE_RUN=1` | `1777465200` 出现 6/57.6=10.4%，符合 | 通过 | 防止多次 same-side add 回归 |
| A6 | completion maker 稳定性 | completion 阶段不得出现数百次真实 maker reprice/cancel | `1777468200` 实际 `placed=8`、`cancel=4`、`replace_per_min=0.22`，无真实挂撤风暴；已修正 `PGTGate.dispatch_place` 口径，只统计真实 maker SetTarget | 通过 | 下一轮确认新口径 |
| A7 | taker-close SLA | shadow-only；触发后 `dispatch_taker_close > 0`，first fill 到 cover p90 <= 100s | `1777468200` `dispatch_taker_close=2`，`first_completion_delay_s=90.128`，`episode_close_delay_p90=90.128` | 通过 | 继续累计样本 |
| A8 | taker-close 成本边界 | 75s 只 breakeven；90s 后最多 1.01；120s 1.015；tail 最高 1.03 | `1777468200` 最终 `summary_pair_cost=1.0100`，刚好落在 90s repair 上限 | 通过 | 继续看是否系统性贴上限 |
| A9 | pair cost | 中位 `summary_pair_cost <= 1.00`，理想接近 0.99；不得系统性 >1.01 | `1777468200` 单轮 `summary_pair_cost=1.0100`、`paired_locked_pnl=-0.7950`，闭合质量偏保守 | 观察中 | 累计样本后决定是否放慢/收紧 taker-close |
| A10 | merge/redeem 生命周期 | merge 主要在 t-25 到 t-18；redeem 在 +35/+50；无 residual 积压 | 近期报告显示 merge/redeem 窗口符合 | 通过 | 长样本确认 |
| A11 | replay/report 可观测性 | 报告包含 seed/cover delay、dispatch_taker_close、pair_cost、same-side 指标 | 已新增 `first_completion_delay_s` / p90 | 通过 | 每轮重建 gap report |
| A12 | 回归测试 | PGT Rust 单测、replay/report Python 单测全过 | `cargo test -q pair_gated_tranche --lib` 77 passed；Python 9 passed | 通过 | 每次策略改动后必跑 |

## 当前硬阻塞

1. `completion` maker 阶段实际订单生命周期已稳定；`PGTGate.dispatch_place` 误报问题已修复，下一轮需确认新口径下 `place` 接近真实 `placed`。
2. `taker-close` 已能在收紧后的 repair band 下闭环，但 `summary_pair_cost=1.0100` 处于边界；需要确认长期不是用负 EV 换 clean close。
3. first fill 成交率仍样本不足，不能只靠一两轮判断策略质量。

## 最新样本：`btc-updown-5m-1777468200`

运行实例：`xuanxuan008_research`

结构化报告：

- `clean_closed_episode_ratio=1.0`
- `summary_paired_qty=79.5`
- `summary_pair_cost=1.0100`
- `summary_residual_qty=0.0`
- `first_seed_accept_rel_s=-501.603`
- `dual_seed_accept_rel_s=-471.214`
- `first_buy_fill_rel_s=-299.537`
- `first_seed_to_first_fill_s=202.066`
- `first_completion_delay_s=90.128`
- `episode_close_delay_p90=90.128`
- `dispatch_taker_close=2`
- `taker_close_dispatch_gap=0`
- `merge_requested_first_rel_s=-24.968`
- `redeem_requested_first_rel_s=45.838`

真实订单生命周期：

- `placed=8`
- `cancel=4`
- `replace=2`
- `replace_per_min=0.22`
- `LIVE_OBS[OK]`

判断：

- shared-ingress/fixed BTC book lane 正常。
- seed latch 正常：单边 seed 约 30s 后释放到双边。
- taker-close 通路正常：两次机会都发出并闭合。
- 没有真实挂撤风暴。
- 当前主要问题从“能不能闭合”转为“闭合是否足够赚钱”。
- 后续修复已完成：`PGTGate.dispatch_place` 不再统计被下层 retain 的内部 intent，只统计真实 maker SetTarget；被保留的 PGT buy intent 计入 `dispatch_retain`。

## 推进顺序

1. 继续跑 fixed BTC PGT shadow，累计至少 10 个 active episode。
2. 用 replay/gap report 每轮追踪 `summary_pair_cost`、`first_completion_delay_s`、`dispatch_taker_close`、`clean_closed_episode_ratio`。
3. 若 `summary_pair_cost` 长期贴近或超过 1.01，优先收紧 taker-close 或延后 repair，而不是放宽成交。
4. 下一轮重点确认修正后的 `PGTGate.dispatch_place` 与真实订单生命周期一致。
