# PGT/xuan Shadow 验收计划

更新时间：2026-04-30 09:45 CST

目标：在不影响 `oracle_lag_sniping` / `pair_arb` / `glft_mm` 的前提下，将 `pair_gated_tranche_arb` 的 BTC 5m shadow 行为收敛到接近 xuanxuan008 的 completion-first 特征，并确认是否具备进入更长期 shadow soak 的条件。

## 验收表

| 序号 | 验收项 | 门槛 | 当前进度 | 状态 | 下一步 |
| --- | --- | --- | --- | --- | --- |
| A1 | shared-ingress fixed BTC book lane | `wire_book_ticks > 0` 且 `coord_partial_forwards > 0`，无 fixed feed connect/shutdown 风暴 | 已由最新 fixed-mode 样本验证可正常出 book/seed | 通过 | 持续观察 broker/client 日志 |
| A2 | seed 初始挂单稳定性 | 每 30s `dispatch_place` 应接近 0-2，`retain` 为主；不得出现数百次撤挂 | `1777465200` 修复后多窗口 `place=0/1`、`retain` 为主 | 通过 | 长样本确认 |
| A3 | seed latch release | 单边 seed 最迟约 30s 释放到双边，避免 90s 单边暴露 | 已从样本看到约 30s 后释放双边 | 通过 | 继续累计样本 |
| A4 | first fill 捕获 | `seed_exposed_fill_ratio` 持续提升；短样本不作为硬门槛 | 最新连续 5 个 active episode 均有 first fill | 观察中 | 至少再收集 10-20 轮 |
| A5 | same-side add | `same_side_add_qty_ratio` 目标约 0.05-0.15，且 `MAX_SAME_SIDE_RUN=1` | 最新 5 个 active episode 中位 `0.0941`，接近 xuan 目标 `0.105`；gap gate 已统一为 `<=0.15` | 通过 | 防止多次 same-side add 回归 |
| A6 | completion maker 稳定性 | completion 阶段不得出现数百次真实 maker reprice/cancel | `1777468800` 新口径复验通过：真实 `placed=3`、`cancel=2`、`replace=0`；PGTGate 全轮以 `retain` 为主 | 通过 | 继续累计样本 |
| A7 | taker-close SLA | shadow-only；触发后 `dispatch_taker_close > 0`，first fill 到 cover p90 <= 100s | 过夜样本 `p90_first_completion_delay_s=103.233`，尾部最长 `209.550s` | 未通过 | 已把 p90 first-completion 加入硬 gate |
| A8 | taker-close 成本边界 | 75s 只 breakeven；90s 只允许半 tick repair；150s 才允许 1c repair；极端老 episode 才允许 1.015 | 旧口径过夜样本 `pair_cost_p90=1.0191`，`max=1.0291`，有 2 轮 `>1.02`；新 cap 首轮仍因 t-45 时间兜底打到 `1.0100`，已继续移除该兜底 | 未通过 | 用新 cap 继续验证 clean close / pair cost 权衡 |
| A9 | pair cost | 中位 `summary_pair_cost <= 1.00`，理想接近 0.99；不得系统性 >1.01 | 过夜 paired 样本中位 `1.0100`；60 轮 paired 中 53 轮 `>1.00`、12 轮 `>1.01` | 未通过 | 优先降低负 EV completion，不再用宽 tail 换 clean close |
| A10 | merge/redeem 生命周期 | merge 主要在 t-25 到 t-18；redeem 在 +35/+50；无 residual 积压 | 近期报告显示 merge/redeem 窗口符合 | 通过 | 长样本确认 |
| A11 | replay/report 可观测性 | 报告包含 seed/cover delay、dispatch_taker_close、pair_cost、same-side 指标 | 已新增 `first_completion_delay_s` / p90 | 通过 | 每轮重建 gap report |
| A12 | 回归测试 | PGT Rust 单测、replay/report Python 单测全过 | `cargo test -q pair_gated_tranche --lib` 77 passed；Python 3.12 replay/report 11 passed | 通过 | 每次策略改动后必跑 |

## 当前硬阻塞

1. `completion` maker 阶段实际订单生命周期已稳定；`PGTGate.dispatch_place` 误报问题已修复，下一轮需确认新口径下 `place` 接近真实 `placed`。
2. `taker-close` 已能闭环，但过夜样本证明旧 repair band 偏宽：中位成本贴 `1.01`，尾部到 `1.0291`。
3. 报表旧 gate 偏松：只看 median `episode_close_delay_p90`，没有拦截 `p90_first_completion_delay_s` 与 pair-cost tail。

## 最新聚合：过夜样本 `btc-updown-5m-1777484100` 到 `btc-updown-5m-1777506900`

样本数：77 行，其中 63 个 active episode，60 个有实际 paired cost 的 episode

- `clean_closed_episode_ratio=1.0`
- `summary_pair_cost_median=1.0100`
- `summary_pair_cost_p90=1.0191`
- `summary_pair_cost_max=1.0291`
- `summary_pair_cost_gt_1.02=2/60`
- `same_side_add_qty_ratio_median=0.1039`
- `same_side_add_qty_ratio_p90=0.1041`
- `first_completion_delay_s_median=37.278`
- `p90_first_completion_delay_s=103.233`
- `first_completion_delay_s_max=209.550`
- `episode_close_delay_p90_median=5.569`
- `episode_close_delay_p90_p90=90.010`
- `summary_paired_qty_median=82.8`

判断：

- shared-ingress、fixed BTC book lane、seed latch、same-side add、merge/redeem 已具备长样本稳定性。
- 旧 shadow repair band 过宽，已经从“可闭合验证”进入“负 EV 控制”阶段。
- 报表已补 `p90_first_completion_delay_s`、`summary_pair_cost_p90`、`summary_pair_cost_tail` gate。
- pair-cost 统计现在忽略 `summary_paired_qty=0` 的伪 active 行，避免 `pair_cost=0` 污染验收。
- 策略已收紧：90s 只允许半 tick repair，150s 才允许 1c repair，极端老 episode 才允许 `1.015`；不再允许 t-45 纯时间兜底触发 1c repair。

## 样本明细：`btc-updown-5m-1777468200`

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

## 样本明细：`btc-updown-5m-1777468800`

结构化报告：

- `clean_closed_episode_ratio=1.0`
- `summary_paired_qty=72.0`
- `summary_pair_cost=1.0100`
- `summary_residual_qty=0.0`
- `first_seed_accept_rel_s=-234.362`
- `dual_seed_accept_rel_s=-204.154`
- `first_buy_fill_rel_s=-30.107`
- `first_seed_to_first_fill_s=204.255`
- `first_completion_delay_s=0.002`
- `episode_close_delay_p90=0.001`
- `dispatch_taker_close=1`
- `taker_shadow_would_close=1`
- `merge_requested_first_rel_s=-24.818`
- `redeem_requested_first_rel_s=47.238`

真实订单生命周期：

- `placed=3`
- `cancel=2`
- `replace=0`
- `replace_per_min=0.00`
- `LIVE_OBS[OK]`

## 样本明细：`btc-updown-5m-1777479000`

结构化报告：

- `clean_closed_episode_ratio=1.0`
- `summary_paired_qty=91.4`
- `summary_pair_cost=1.0091`
- `summary_residual_qty=0.0`
- `same_side_add_qty_ratio=0.0941`
- `first_seed_accept_rel_s=-415.504`
- `dual_seed_accept_rel_s=-415.504`
- `first_buy_fill_rel_s=-259.335`
- `first_seed_to_first_fill_s=156.169`
- `first_completion_delay_s=90.125`
- `episode_close_delay_p90=90.126`
- `dispatch_taker_close=2`
- `taker_shadow_would_close=3`
- `merge_requested_first_rel_s=-24.878`
- `redeem_requested_first_rel_s=46.906`

真实订单生命周期：

- `placed=8`
- `cancel=5`
- `replace=1`
- `replace_per_min=0.13`
- `LIVE_OBS[OK]`

备注：

- 本轮 first fill 后出现 same-side add：`8.6 / 91.4 = 0.0941`。
- 报表已修复：当 ledger ratio 缺失或为 0，但 lifecycle 能识别 same-side add 时，用 same-side add 接受数量 / `summary_paired_qty` 回填。

## 样本明细：`btc-updown-5m-1777479600`

结构化报告：

- `clean_closed_episode_ratio=1.0`
- `summary_paired_qty=82.8`
- `summary_pair_cost=1.0100`
- `summary_residual_qty=0.0`
- `same_side_add_qty_ratio=0.1039`
- `first_seed_accept_rel_s=-427.519`
- `dual_seed_accept_rel_s=-385.026`
- `first_buy_fill_rel_s=-299.304`
- `first_seed_to_first_fill_s=128.215`
- `first_completion_delay_s=90.021`
- `episode_close_delay_p90=90.023`
- `dispatch_taker_close=1`
- `taker_shadow_would_close=2`
- `merge_requested_first_rel_s=-24.985`
- `redeem_requested_first_rel_s=46.188`

真实订单生命周期：

- `placed=6`
- `cancel=3`
- `replace=2`
- `replace_per_min=0.25`
- `LIVE_OBS[OK]`

备注：

- first fill 后通过 taker-close 在约 90s 内配平，最终 residual 为 0。
- clean close 后同轮又出现一个新的 YES seed intent，但在 endgame 前未成交，并在 t-25s 由 `EndgameRiskGate` 撤掉；这说明当前仍允许同轮二次 seed，需要继续观察是否应增加 round-level episode cap。

## 样本明细：`btc-updown-5m-1777480200`

结构化报告：

- `clean_closed_episode_ratio=1.0`
- `summary_paired_qty=91.4`
- `summary_pair_cost=1.0009`
- `summary_residual_qty=0.0`
- `same_side_add_qty_ratio=0.0941`
- `first_seed_accept_rel_s=-434.140`
- `dual_seed_accept_rel_s=-434.140`
- `first_buy_fill_rel_s=-138.043`
- `first_seed_to_first_fill_s=296.097`
- `first_completion_delay_s=30.350`
- `episode_close_delay_p90=30.351`
- `dispatch_taker_close=1`
- `taker_shadow_would_close=1`
- `merge_requested_first_rel_s=-24.953`
- `redeem_requested_first_rel_s=47.084`

真实订单生命周期：

- `placed=8`
- `cancel=3`
- `replace=2`
- `replace_per_min=0.25`
- `LIVE_OBS[OK]`

备注：

- 本轮 first fill 较晚，但 completion 在约 30s 内完成，pair cost 接近 breakeven。
- first-leg 后挂出的 same-side add 在后段成交，并被 taker-close 立即配平；这符合 `MAX_SAME_SIDE_RUN=1` 的复刻结构。
- clean close 后同轮继续 seed 与 xuan 的多 episode/round 结构并不冲突；验收重点是 endgame 风险门槛、挂撤频率、same-side add 上限继续有效。

## 推进顺序

1. 继续跑 fixed BTC PGT shadow，累计至少 10 个 active episode。
2. 用 replay/gap report 每轮追踪 `summary_pair_cost`、`first_completion_delay_s`、`dispatch_taker_close`、`clean_closed_episode_ratio`。
3. 若新 cap 后 `clean_closed_episode_ratio` 大幅下降，再考虑使用真实 surplus/repair budget，而不是重新放宽无资金来源的 tail repair。
4. 下一轮重点确认修正后的 `PGTGate.dispatch_place` 与真实订单生命周期一致。
