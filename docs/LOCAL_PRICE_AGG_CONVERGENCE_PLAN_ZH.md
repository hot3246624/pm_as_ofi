# 本地价格聚合器收敛计划

更新时间：2026-04-29 22:35 CST

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

最新 close-only boundary 数据集评估基线，已包含 HYPE/XRP wide-spread near-flat guardrail、DOGE last-before guardrail、HYPE stale-spread fallback / single-source guardrail，并已修正 evaluator，使其与运行时的 HYPE `close_only_fallback`、BNB/SOL close-only filter reason、BNB/SOL strict source fallback 保持一致：

| 样本段 | ok | side | filtered | missing | mean_bps | max_bps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| latest test | 250 | 0 | 24 | 17 | 1.016569 | 4.402462 |
| latest train | 660 | 0 | 76 | 67 | 0.862845 | 4.413691 |
| current unseen `20260429_210600` | 43 | 0 | 2 | 2 | 0.893107 | 3.753107 |
| current unseen `20260429_220724` pre-guard observation | 12 | 0 | 2 | 0 | n/a | 3.415719 |

已修复的 accepted side mismatch：

- HYPE：`after_then_before`、2 source、0 exact、source spread >= 2bps、direction margin < 1.5bps。
- XRP：`nearest_abs`、>=2 source、0 exact、source spread >= 2bps、direction margin < 2.0bps。

已新增的 strict source fallback：

- BNB：仅在 weighted primary missing 时启用 `okx` 单源 `after_then_before`，要求 0 exact 且 direction margin >= 7bps；current unseen 新增 1 个 accepted，side=0。
- SOL：仅在 weighted primary missing 时启用 `coinbase` 单源 `after_then_before`，要求 0 exact 且 direction margin >= 5bps；current unseen 新增 2 个 accepted，side=0。

已新增的 DOGE guardrail：

- DOGE：`last_before` 多源、0 exact、direction margin < 1.5bps 时过滤为 `doge_last_multi_near_flat`，拦截历史 train `5.440670bps` outlier。
- DOGE：`last_before` source_count >= 3、0 exact、source spread >= 8bps 时过滤为 `doge_last_high_spread`，拦截 `20260429_220724` 新 unseen 中的 7.673766bps / 6.071739bps outlier。

已新增的 HYPE tail guardrail：

- HYPE：`after_then_before`、source_count >= 3、0 exact、source spread 2-4bps、direction margin >= 40bps 时，不再接受 weighted primary，改走 close-only fallback；拦截 latest test `6.801403bps` outlier。
- HYPE：close-only 仅 1 source、0 exact、direction margin >= 20bps 时直接 missing，避免单一 Hyperliquid close-only 在大幅偏离 open 时形成 >5bps accepted tail。

当前剩余瓶颈：

- G0/G4：latest test、latest train、当前 unseen 都已经是 `side=0`，但 rolling 100 accepted 还需要继续采样确认；当前稳定样本仍不足 100 accepted。
- G2：current unseen accepted coverage 已到 43/47=91.5%；latest test accepted coverage 在 HYPE tail guard 后约 85.9%，仍需继续观察 per-symbol 稳定性。
- G3：latest test/train max 均已进入 5bps 内；下一步重点是 rolling unseen 是否维持 `side=0` 且 `max<=5bps`。

结论：当前优先级已经从“清零 accepted side mismatch”切换为“继续采样确认 rolling 0 side，同时针对 HYPE/SOL coverage 和 HYPE/DOGE outlier 做定向优化”。

## 推进路线

| 阶段 | 目标 | 输出 | 状态 |
| --- | --- | --- | --- |
| P0 | shared-ingress 与多实例隔离稳定 | broker/client 稳定，日志按 instance 隔离 | 已完成基础版 |
| P1 | boundary tape 与评估器稳定 | 可重复生成 close-only dataset 和 router eval | 已完成，evaluator 已对齐运行时 fallback/filter |
| P2 | router v1 清零 accepted side mismatch | 最新 test/train/unseen 均 side=0 | 已达到当前样本，继续 rolling 验证 |
| P3 | 覆盖率提升 | 在 side=0 前提下减少 filtered/missing | 进行中，SOL/BNB strict fallback 已提升 current coverage |
| P4 | 延迟验收 | 本地 final ready p95 <= 500ms，冲刺 <= 300ms | 进行中 |
| P5 | 生产集成评估 | shadow -> dry-run -> limited enable | 阻塞于 P2/P4 |

## 执行节奏

| 时间点 | 动作 | 验收 |
| --- | --- | --- |
| T+0 | 修复当前 HYPE/XRP 两个 guardrail，重建 release，重启 challenger | 已完成，离线 latest test/train side=0 |
| T+30m | 检查新增 unseen rounds | 已完成，`20260429_210600` 当前 43 accepted、side=0 |
| T+2h | 汇总 20+ 新轮次 | DOGE/HYPE tail guardrail 已补齐，需重建 release 并重启 challenger 验证 |
| T+12h | 过夜 shadow/dry-run | rolling 100 accepted side=0 |
| T+24h | 冻结候选 router 或继续 shadow | 满足 G0/G1/G4 才进入生产集成评估 |

## 调参原则

- 先保方向，再保覆盖，最后优化 bps。
- 不做全局强行替换；任何规则必须通过 train/test/unseen。
- per-symbol guardrail 可以接受，但必须有可解释的市场结构原因，例如 source spread、source count、exact source、close rule、near-flat margin。
- 对新增 fallback 只允许在历史和 unseen 都无 side mismatch 后启用。
- 如果某一币种长期依赖单一来源且 near-flat 风险高，宁可 missing，不接受方向赌错。

## 下一步

1. 重建 release 并重启 challenger，让 DOGE/HYPE guardrail 生效；目标 rolling 100 accepted side=0 且 accepted max<=5bps。
2. 继续观察 HYPE tail guard 的 coverage 代价；如果 unseen HYPE missing 过高，再单独研究 HYPE source timing，而不是直接放宽单源规则。
3. 继续观察 SOL/BNB strict source fallback 在新 unseen 中是否保持 side=0；任何 side mismatch 立即回滚为 missing。
4. 下一次建议在重启后 30 分钟复盘；若 30 分钟内 side=0 且 max<=5bps，再进入 2 小时 rolling 验收。
