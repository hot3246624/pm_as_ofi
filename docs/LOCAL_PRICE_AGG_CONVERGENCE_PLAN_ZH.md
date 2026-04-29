# 本地价格聚合器收敛计划

更新时间：2026-04-29

## 目标

自建价格聚合器的目标不是复刻 RTDS 的传输链路，而是在本地用多交易所数据源快速估算比赛结算价，并在 Polymarket 5 分钟市场收盘后尽快给出可执行方向。

核心目标：

- 决策可用延迟：本地 final 候选在收盘后 p95 <= 500ms，最终目标 p95 <= 300ms。
- 方向安全性：以 RTDS/Data Streams final/open 对比为验证基准，accepted 决策方向误判必须为 0。
- 覆盖率：在方向安全的前提下逐步提高 accepted 覆盖率，不能为了覆盖率接受 near-flat 风险。
- 生产状态：当前只允许 shadow/dry-run 验证；未达到验收门槛前不得接入 live 下单路径。

## 验收门槛

| Gate | 指标 | 验收标准 | 当前状态 |
| --- | --- | --- | --- |
| G0 | Accepted side mismatch | rolling 100 accepted = 0，且最近 24h = 0 | 进行中 |
| G1 | 本地 final 产出延迟 | p95 <= 500ms，目标 p95 <= 300ms | 进行中 |
| G2 | 覆盖率 | 总 accepted coverage >= 85%，单币种 >= 75% | 进行中 |
| G3 | Accepted 价格偏差 | mean <= 1.5bps，p95 <= 3bps，max <= 5bps | 进行中 |
| G4 | 稳定性 | train/test/unseen 三段均无 accepted side mismatch | 进行中 |

说明：

- “12 位以上价格精度”不应按绝对小数位理解，因为交易所报价粒度、成交簿更新时间、Chainlink 聚合窗口都会引入不可消除差异；验收应以方向误判、bps 偏差和延迟为主。
- 任何 accepted side mismatch 都是阻断项，必须先过滤或降级为 missing，再考虑提高覆盖率。

## 当前基线

最新 close-only boundary 数据集评估基线，已包含 HYPE/XRP wide-spread near-flat guardrail，并已修正 evaluator，使其与运行时的 HYPE `close_only_fallback`、BNB/SOL close-only filter reason 保持一致：

| 样本段 | ok | side | filtered | missing | mean_bps | max_bps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| latest test | 115 | 0 | 15 | 9 | 1.155327 | 6.801403 |
| latest train | 762 | 0 | 73 | 83 | 0.898039 | 5.440670 |
| current unseen `20260429_210600` | 12 | 0 | 1 | 2 | 0.685041 | 1.927020 |

已修复的 accepted side mismatch：

- HYPE：`after_then_before`、2 source、0 exact、source spread >= 2bps、direction margin < 1.5bps。
- XRP：`nearest_abs`、>=2 source、0 exact、source spread >= 2bps、direction margin < 2.0bps。

当前剩余瓶颈：

- G0/G4：latest test、latest train、当前 unseen 都已经是 `side=0`，但 rolling 100 accepted 还需要继续采样确认。
- G2：missing 主要集中在 SOL，其次是 HYPE 的极少数 primary/fallback 都不可用样本。
- G3：latest test `p95` 已接近目标，但 `max_bps=6.801403` 仍超出 5bps，主要来自 HYPE outlier；DOGE train 也有 `5.440670bps` outlier。

结论：当前优先级已经从“清零 accepted side mismatch”切换为“继续采样确认 rolling 0 side，同时针对 HYPE/SOL coverage 和 HYPE/DOGE outlier 做定向优化”。

## 推进路线

| 阶段 | 目标 | 输出 | 状态 |
| --- | --- | --- | --- |
| P0 | shared-ingress 与多实例隔离稳定 | broker/client 稳定，日志按 instance 隔离 | 已完成基础版 |
| P1 | boundary tape 与评估器稳定 | 可重复生成 close-only dataset 和 router eval | 已完成，evaluator 已对齐运行时 fallback/filter |
| P2 | router v1 清零 accepted side mismatch | 最新 test/train/unseen 均 side=0 | 已达到当前样本，继续 rolling 验证 |
| P3 | 覆盖率提升 | 在 side=0 前提下减少 filtered/missing | 进行中，优先 SOL/HYPE |
| P4 | 延迟验收 | 本地 final ready p95 <= 500ms，冲刺 <= 300ms | 进行中 |
| P5 | 生产集成评估 | shadow -> dry-run -> limited enable | 阻塞于 P2/P4 |

## 执行节奏

| 时间点 | 动作 | 验收 |
| --- | --- | --- |
| T+0 | 修复当前 HYPE/XRP 两个 guardrail，重建 release，重启 challenger | 已完成，离线 latest test/train side=0 |
| T+30m | 检查新增 unseen rounds | 已开始，`20260429_210600` 当前 side=0 |
| T+2h | 汇总 20+ 新轮次 | side=0，记录覆盖率、filtered/missing、latency |
| T+12h | 过夜 shadow/dry-run | rolling 100 accepted side=0 |
| T+24h | 冻结候选 router 或继续 shadow | 满足 G0/G1/G4 才进入生产集成评估 |

## 调参原则

- 先保方向，再保覆盖，最后优化 bps。
- 不做全局强行替换；任何规则必须通过 train/test/unseen。
- per-symbol guardrail 可以接受，但必须有可解释的市场结构原因，例如 source spread、source count、exact source、close rule、near-flat margin。
- 对新增 fallback 只允许在历史和 unseen 都无 side mismatch 后启用。
- 如果某一币种长期依赖单一来源且 near-flat 风险高，宁可 missing，不接受方向赌错。

## 下一步

1. 继续采样到 current unseen 至少 20+ 行，再判断 rolling 稳定性。
2. 定向分析 HYPE `max_bps=6.801403` outlier，优先比较 fallback、source spread、source timestamp offset，而不是扩大无约束全局搜索。
3. 定向分析 SOL missing，确认是 source availability、`only_okx_coinbase` min_sources 约束，还是 close-only filter 导致。
4. 维持 `pm_local_agg_challenger` 和 shared-ingress broker 运行；下一次建议在 30 分钟后复盘新增 unseen rounds。
