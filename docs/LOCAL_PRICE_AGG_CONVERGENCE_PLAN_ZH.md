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

最新 close-only boundary 数据集评估基线：

| 样本段 | ok | side | filtered | missing | mean_bps | max_bps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| latest test | 115 | 2 | 9 | 14 | 1.180373 | 6.801403 |
| latest train | 733 | 0 | 49 | 116 | 0.868501 | 5.440670 |

当前 test 的 2 个 accepted side mismatch 已定位：

- HYPE：`after_then_before`、2 source、0 exact、source spread >= 2bps、direction margin < 1.5bps。
- XRP：`nearest_abs`、>=2 source、0 exact、source spread >= 2bps、direction margin < 2.0bps。

对应 guardrail 的离线模拟结果：

| 样本段 | ok | side | filtered | missing |
| --- | ---: | ---: | ---: | ---: |
| latest test | 112 | 0 | 12 | 14 |
| latest train | 727 | 0 | 55 | 116 |

结论：当前优先级是先把 accepted side mismatch 清零，再继续做覆盖率和偏差优化。

## 推进路线

| 阶段 | 目标 | 输出 | 状态 |
| --- | --- | --- | --- |
| P0 | shared-ingress 与多实例隔离稳定 | broker/client 稳定，日志按 instance 隔离 | 已完成基础版 |
| P1 | boundary tape 与评估器稳定 | 可重复生成 close-only dataset 和 router eval | 已完成基础版 |
| P2 | router v1 清零 accepted side mismatch | 最新 test/train/unseen 均 side=0 | 进行中 |
| P3 | 覆盖率提升 | 在 side=0 前提下减少 filtered/missing | 待 P2 稳定 |
| P4 | 延迟验收 | 本地 final ready p95 <= 500ms，冲刺 <= 300ms | 进行中 |
| P5 | 生产集成评估 | shadow -> dry-run -> limited enable | 阻塞于 P2/P4 |

## 执行节奏

| 时间点 | 动作 | 验收 |
| --- | --- | --- |
| T+0 | 修复当前 HYPE/XRP 两个 guardrail，重建 release，重启 challenger | 离线 latest test/train side=0 |
| T+30m | 检查新增 unseen rounds | 新增 accepted side=0；若有误判，立即降级过滤 |
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

1. 落地 HYPE/XRP wide-spread near-flat guardrail。
2. 重新生成 dataset/eval，确认 latest test/train accepted side=0。
3. release build 后重启 `pm_local_agg_challenger`，不重启 shared-ingress broker。
4. 30 分钟后复盘新增 unseen rounds，重点看 accepted side、filtered reason、local ready latency。
