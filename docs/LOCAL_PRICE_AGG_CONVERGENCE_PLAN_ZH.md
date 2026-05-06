# 本地价格聚合器收敛计划

更新时间：2026-05-05 12:05 CST

## 2026-05-05 11:40 状态更新：切到真实 T+300ms replay / dry-run

本轮重点从“默认 1.5s 等待窗口”切到真实目标窗口：本地 final 必须在 `T+300ms` 内可用。

已完成：

- `scripts/build_local_agg_boundary_dataset.py` 新增 `--cap-local-ready-lag-ms`，离线 replay 可以把每轮可见 tick 截断到 `round_end + 300ms`，避免继续用旧等待窗口误判。
- `scripts/run_local_agg_lab.sh` 默认设置 `PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS=50`。原因是 shadow compare lane 在 Rust 中额外加 `250ms` diagnostic grace，因此 dry-run 的有效 deadline 为 `T+300ms`；生产 decision lane 没有这段 compare grace，生产配置仍应按 `300ms` 单独评估。
- hourly automation 默认输出 `replay_cap_ready_lag_ms=300`，并修正“当前 run 没有样本却回退 all-runs 后 pass=true”的误判；当前 run 无足够样本时只能是 `insufficient_sample`。
- 已重启 `local_agg_challenger` 到 `20260505_113223`，确认进程环境为 `PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS=50`，仍通过 shared-ingress client 复用 broker，不重启 broker。

cap-300 all-runs replay 结果：

- uncertainty gate：`eval_rows=10582`、`accepted=6569`、`coverage=81.5215%`、`side_errors=0`、`mean=1.076508bps`、`p95=3.312290bps`、`p99=4.436076bps`、`max=4.987616bps`。
- cap 后 router raw 仍有 side errors，说明 raw router 不能作为验收口径；gate 后 side=0，max<5bps 仍成立。
- 分币种 accepted coverage：BNB `34.3%`、BTC `75.0%`、DOGE `60.4%`、ETH `74.6%`、HYPE `54.7%`、SOL `68.5%`、XRP `70.8%`。BNB/HYPE/DOGE 是覆盖率瓶颈。

第一轮真实 T+300ms live shadow：

- 当前 run `20260505_113223` 第 1 轮：router eval `ok=5`、`filtered=2`、`missing=0`、`side=0`、`mean=1.413501bps`、`max=3.011878bps`。
- BNB 与 SOL raw 方向不一致，但均被 router/gate 过滤，没有进入 accepted。
- 延迟：local p50 `42ms`、p95/max `215ms`；RTDS p50 `1312ms`、p95 `1342ms`；worker late start `0`。
- 当前样本量只有 5 accepted，不足以验收；automation action 正确为 `insufficient_sample`。

12:03 复盘补充：

- 当前 run `20260505_113223` 已累计 `eval_rows=14`、`ok=12`、`filtered=2`、`missing=0`、`accepted_side_errors=0`、`mean=1.481515bps`、`max=3.427845bps`，仍因 accepted 样本 <20 标记为 `insufficient_sample`。
- runtime gate shadow：`final_rows=19`、`accepted=17`、`gated=2`、`accepted_side_errors=0`、`accepted_mean=1.461326bps`、`accepted_max=3.427845bps`。
- latency：local p50 `25ms`、p95/max `273ms`；RTDS p50 `1415ms`、p95 `6581ms`、max `6610ms`；worker late start `0`。这说明当前 T+300ms 本地产出有效，且本轮 RTDS 发生 6.5s 级延迟时，本地路线仍保持 <300ms。

VW-Median-lite 结论：

- 已新增 `scripts/evaluate_local_agg_vw_median_candidates.py`，用于在现有 boundary tape 上测试加权中位数/加权均值候选。
- 该脚本当前只使用 trade/ticker/mid tape，不是 L2 orderbook depth tape，因此只能验证“现有数据上直接套 VW-Median 是否可行”。
- 在 cap-300 all-runs、MedMAD-lite 50bps outlier filter、`min_margin=1bps` 下，最佳中位数候选仍有 `side_errors>500`，max 约 `14.8bps`；提高 `min_margin=12bps` 才能 side=0，但 coverage 降到约 `22-23%` 且 p95 已超过 5bps。
- 结论：用户提出的 VW-Median/L2 深度权重方向值得作为下一条 shadow research lane，但当前 trade/ticker tape 上的 median-lite 不能替代现有 router+uncertainty gate。要继续这条路，必须先采集 L2 best bid/ask + depth_0.1% tape，再重新评估。

## 2026-05-05 状态更新：从硬编码规则切到统计 gate

过去几轮 live/dry-run 证明，继续对 `boundary_symbol_router_v1` 追加 per-symbol 硬编码过滤规则已经进入尾部追逐：all-history replay 可以通过，但新 unseen rounds 仍会出现 accepted side error 或 `>5bps` tail。当前主线已经切换为“router 候选 + walk-forward uncertainty gate”：

- 原始 router 仍不是验收口径：all-runs raw router `rows=10825`、`ok=8249`、`side=12`、`filtered=1951`、`missing=625`、`mean=1.118494bps`、`max=12.922542bps`。
- uncertainty gate walk-forward 是当前硬验收口径：`eval_rows=10475`、`accepted=6569`、`coverage=82.3699%`、`side_errors=0`、`mean=1.076806bps`、`p95=3.319512bps`、`p99=4.444944bps`、`max=4.987616bps`。
- 当前候选配置：`min_samples=20`、`min_margin=1bps`、`train_q95<=5bps`、`safety=0.5bps`、`train_side_rate=0`、`round_max_margin<=37bps`、`shape_history_rescue(min_samples=30,max_train_max<=4.5bps)`。
- 模型产物：`logs/local-agg-boundary-challenger-lab/monitor_reports/local_agg_uncertainty_gate_model.latest.json`，会由 hourly check 按当前配置重建。
- monitor 口径已经改为同时输出 raw router、uncertainty-gated acceptance、runtime gate shadow；硬门槛以后以 gated accepted 为准。
- runtime shadow 已接入并升级到 v1.1：`20260505_100833` 首轮 7 个 symbol 全部产出 `phase=final`，`accepted=7`、`gated=0`、`side_errors=0`、`mean=0.828407bps`、`max=2.514868bps`、本地 ready p95 `29ms`、RTDS p95 `1550ms`。
- 2026-05-05 10:08 已重启 challenger 到 `20260505_100833`，加载最新 release binary 和 v1.1 uncertainty gate model。日志确认 `buckets=787`、`rescue_buckets=8`、`max_round_max_margin_bps=37`、`rescue_min_samples=30`、`rescue_max_train_max_bps=4.5`。当前 automation action 为 `clean_keep_monitoring`。

解释与口径修正：

- `round_max_margin` 是当前最关键的新特征。定点实验显示：阈值 34bps 时 coverage 74.67%、side=0、max=4.987616bps；阈值 37bps 时 coverage 76.76%、side=0、max=4.987616bps；阈值 38bps 开始会放入 8.144533bps tail。因此当前 dry-run 候选上界提升到 37bps，但生产启用前仍需 rolling unseen 验证。
- v1.1 新增的 `shape_history_max_rescue` 只 rescue 原本因为 `margin_below_trained_error` 被拒绝的候选，并要求同 shape 历史样本数 >=30、历史 side error=0、历史 max <=4.5bps。它把 all-runs gate coverage 从约 76.78% 提升到 82.37%，同时保持 `side_errors=0`、`max=4.987616bps` 不变；不会 rescue `train_side_rate`、`below_min_margin` 或 `round_max_margin_too_wide`。
- 生产验收不能把“方向安全”和“足够像 RDTS final”割裂。正确口径是：`accepted side_errors=0` 是最低门槛，`mean/p95/p99/max bps` 接近 RDTS final 是证明方向判断可泛化的强约束；任何一项不达标都不能进入生产。
- 离线分析时二者会表现为不同失败形态：near-flat 样本可能 `close_diff_bps` 很小但方向错；大波动样本可能方向对但 `close_diff_bps` 很大。前者说明不能下单，后者说明还不够像 RDTS final，也不能作为生产级 final estimator。
- 这不是生产启用结论。当前 gate 已经接入 runtime shadow，但尚未驱动任何下单/提示；必须继续跑 clean rolling dry-run，确认 runtime accepted `side_errors=0`、`max<5bps`、本地 p95 稳定小于 300-500ms 后，才评估生产路径。
- 下一阶段不再优先新增 symbol-specific tail 规则，除非是明确重复的 parser/source 故障；优先推进可解释、可导出的统计 gate 和运行时加载模型。

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
| G2 | 覆盖率 | 总 accepted coverage >= 85%，单币种 >= 75% | 进行中：v1.1 gate all-runs coverage 82.37%，仍略低于 85% |
| G3 | Accepted 价格偏差 | mean <= 1.5bps，p95 <= 3bps，max <= 5bps | mean/max 达标，p95=3.319512bps 仍略高 |
| G4 | 稳定性 | train/test/unseen 三段均无 accepted side mismatch | 进行中 |

说明：

- “12 位以上价格精度”不应按绝对小数位理解，因为交易所报价粒度、成交簿更新时间、Chainlink 聚合窗口都会引入不可消除差异；验收应以方向误判、bps 偏差和延迟为主。
- 任何 accepted side mismatch 都是阻断项，必须先过滤或降级为 missing，再考虑提高覆盖率。

## 当前基线

最新 expanded close-only boundary 数据集评估基线已递归纳入 `logs/local-agg-boundary-challenger-lab/runs/*`。这是当前可信口径；此前 80.42% 覆盖率漏扫了 run 目录，不能作为收敛依据。当前 evaluator 与运行时的 HYPE `close_only_fallback`、BNB/SOL/DOGE strict source fallback、以及高风险单源 fallback 禁用逻辑保持一致。2026-05-01 09:50 已完成第二轮 coverage rescue，并同步到 Rust 运行时：

| 样本段 | ok | side | filtered | missing | mean_bps | max_bps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| expanded all test post-rescue | 76 | 0 | 28 | 8 | 1.479053 | 4.977554 |
| expanded all train post-rescue | 2842 | 0 | 795 | 241 | 1.142890 | 4.961959 |
| expanded all total post-rescue | 2918 | 0 | 823 | 249 | 1.151646 | 4.977554 |
| expanded all test rescue-batch2 | 84 | 0 | 20 | 8 | 1.578156 | 4.878025 |
| expanded all train rescue-batch2 | 3100 | 0 | 537 | 241 | 1.193002 | 4.961959 |
| expanded all total rescue-batch2 | 3184 | 0 | 557 | 249 | 1.203163 | 4.961959 |
| expanded all test post-09:55-tail-fix | 79 | 0 | 19 | 8 | 1.491095 | 4.856099 |
| expanded all train post-09:55-tail-fix | 3054 | 0 | 530 | 240 | 1.196907 | 4.961959 |
| expanded all total post-09:55-tail-fix | 3133 | 0 | 549 | 248 | 1.204325 | 4.961959 |
| current unseen `20260429_210600` | 43 | 0 | 2 | 2 | 0.893107 | 3.753107 |
| current unseen `20260429_220724` pre-guard observation | 12 | 0 | 2 | 0 | n/a | 3.415719 |
| current unseen `20260429_223509` raw router_v1 | 44 | 1 | 0 | n/a | 1.495656 | 8.264534 |
| current unseen `20260429_223509` post-guard replay | 42 | 0 | 2 | n/a | 1.333700 | 4.098987 |
| current unseen `20260429_232345` raw router_v1 | 54 | 1 | 8 | 1 | 1.426143 | 5.274113 |
| current unseen `20260429_232345` post-guard replay | 50 | 0 | 12 | 1 | 1.231579 | 4.011575 |
| current unseen `20260430_000805` raw router_v1 snapshot | 87 | 2 | 10 | 1 | 1.547337 | 5.968113 |
| current unseen `20260430_000805` post-guard replay | 82 | 0 | 15 | 1 | 1.318078 | 4.534162 |
| overnight challenger `20260430_012719` post-guard replay | 502 | 0 | 89 | 41 | 1.551156 | 4.898166 |
| rolling challenger `20260430_153546` post-guard replay | 35 | 0 | 20 | 0 | 1.364049 | 4.729164 |
| rolling challenger `20260430_162025` post-guard replay | 8 | 0 | 5 | 0 | 1.752813 | 3.728363 |
| rolling challenger `20260430_163618` post-guard replay | 2 | 0 | 3 | 0 | 1.342939 | 2.255855 |
| rolling challenger `20260430_164633` post-guard replay | 1 | 0 | 5 | 0 | 1.941681 | 1.941681 |
| rolling challenger `20260430_165604` post-guard replay | 4 | 0 | 2 | 0 | 1.001461 | 1.643077 |
| rolling challenger `20260430_170724` post-guard replay | 4 | 0 | 3 | 0 | 1.604277 | 3.422953 |
| rolling challenger `20260430_172434` post-guard replay | 2 | 0 | 4 | 0 | 1.293762 | 2.349764 |
| rolling challenger `20260430_173344` post-guard replay | 5 | 0 | 2 | 0 | 1.697209 | 3.901577 |
| rolling challenger `20260430_173742` post-guard replay | 5 | 0 | 2 | 0 | 0.715287 | 1.271986 |
| rolling challenger `20260430_173742` 17:45 post-guard replay | 6 | 0 | 1 | 0 | 1.162194 | 2.705623 |
| rolling challenger `20260430_173742` 17:40-18:20 post-guard replay | 46 | 0 | 13 | 0 | 1.241366 | 4.569195 |
| rolling challenger `20260430_173742` 17:40-18:50 post-guard replay | 73 | 0 | 26 | 0 | 1.236085 | 4.571303 |
| rolling challenger `20260430_185622` 19:00-20:05 post-fix replay | 72 | 0 | 20 | 0 | 1.286641 | 4.434810 |
| rolling challenger `20260430_185622` 19:00-20:35 post-fix replay | 106 | 0 | 27 | 0 | 1.375527 | 4.724149 |
| rolling challenger `20260430_185622` 19:00-22:05 post-fix replay | 177 | 0 | 56 | 0 | 1.464679 | 4.733137 |
| rolling challenger `20260430_220844` 22:10-22:40 post-guard replay | 33 | 0 | 14 | 0 | 1.374806 | 4.745928 |
| rolling challenger `20260430_224806` 22:50-23:35 post-guard replay | 46 | 0 | 21 | 0 | 1.300208 | 4.230635 |
| rolling challenger `20260430_234139` 23:45-00:45 post-guard replay | 62 | 0 | 33 | 0 | 1.286922 | 4.600896 |
| rolling challenger `20260501_005117` 00:55-01:25 post-guard replay | 37 | 0 | 14 | 0 | 1.478531 | 4.977554 |

已修复的 accepted side mismatch：

- HYPE：`after_then_before`、2 source、0 exact、source spread >= 2bps、direction margin < 2.0bps。
- XRP：`nearest_abs`、>=2 source、0 exact、source spread >= 2bps、direction margin < 2.0bps。
- XRP：`nearest_abs`、单源、0 exact、direction margin < 1.3bps 时过滤为 `xrp_single_nearest_near_flat`，拦截旧 challenger 汇总中的 near-boundary side mismatch。
- XRP：`nearest_abs`、单源 Binance、0 exact、close 点距边界 <= 1000ms、direction margin < 4.5bps 时过滤为 `xrp_single_binance_fast_mid_margin`，拦截 15:30 live run 与 `20260430_163618` 中的单源 Binance fast-mid side mismatch。

已新增的 strict source fallback：

- BNB：仅在 weighted primary missing 时启用 `okx` 单源 `after_then_before`，要求 0 exact 且 direction margin >= 7bps；current unseen 新增 1 个 accepted，side=0。
- SOL：优先启用 `binance+coinbase` 双源 `nearest_abs`，要求 2 source、0 exact、direction margin >= 2bps、spread <= 7bps；expanded dataset 中 accepted=63、side=0、max=2.121579bps。
- SOL：仅在双源 fallback 不可用时启用 `coinbase` 单源 `after_then_before`，要求 0 exact 且 direction margin >= 5bps；expanded dataset 中 accepted=1、side=0、max=1.336739bps。
- DOGE：`binance` 单源 `after_then_before` 只允许 5bps <= direction margin <= 15bps，且 close 点距边界 <= 4000ms；expanded dataset 中 accepted=54、side=0、max=4.349657bps。
- HYPE：`hyperliquid` 单源 fallback 已从 router 接收路径禁用；expanded run 证明它会产生多个 >5bps tail 和 side mismatch，只保留 shadow 观察价值。
- SOL：`binance` 单源 fallback 已从 router 接收路径禁用；expanded run 证明它会产生 side mismatch 和 >5bps tail。

已新增的 DOGE guardrail：

- DOGE：`last_before` 多源、0 exact、direction margin < 1.5bps 时过滤为 `doge_last_multi_near_flat`，拦截历史 train `5.440670bps` outlier。
- DOGE：`last_before` source_count >= 3、0 exact、source spread >= 8bps 时过滤为 `doge_last_high_spread`，拦截 `20260429_220724` 新 unseen 中的 7.673766bps / 6.071739bps outlier。
- DOGE：`last_before` source_count >= 2、0 exact、source spread >= 8bps 时同样过滤为 `doge_last_high_spread`，拦截 `20260429_223509` 新 unseen 中的 8.264534bps accepted tail。
- DOGE：`last_before` 单源、0 exact、close 点距边界 <= 1000ms、direction margin >= 4bps 的 fast/far 样本已从一刀切过滤改为可接受；expanded dataset 净增 12 个 accepted、side=0、max 仍为 4.898172bps。
- DOGE：`last_before` 单源、0 exact、close 点距边界 <= 1000ms、direction margin < 4bps 时过滤为 `doge_single_last_fast_mid_margin`，拦截 15:30 live run 中的单源 Bybit fast-mid side mismatch / 5.142434bps tail。
- DOGE：`last_before` 单源 OKX、0 exact、close 点距边界 900-1000ms、8bps <= direction margin < 15bps 时过滤为 `doge_single_okx_late_fast_mid_margin_tail`，拦截放开 fast/far 后暴露的 5.316074bps tail。
- DOGE：`last_before` 单源、0 exact、close 点距边界 >= 1500ms、13bps <= direction margin < 15bps 时过滤为 `doge_single_last_stale_tail`；`8-13bps` 与 `margin>=15bps` 子区间已释放为 coverage rescue，其中 `8-13bps` 历史 13 样本 side=0、max=3.036bps。
- DOGE：`last_before` >=3 source、0 exact、source spread >= 3.8bps、direction margin >= 7bps、close 点距边界 <= 500ms 时过滤为 `doge_three_last_fast_spread_tail`。
- DOGE：`last_before` >=3 source、0 exact、source spread >= 4bps、direction margin < 3.5bps、close 点距边界 <= 800ms 时过滤为 `doge_three_last_fast_spread_near_flat`，拦截 overnight 新增的 7.331641bps side mismatch。
- DOGE：`last_before` >=3 source、0 exact、source spread <= 1.1bps、direction margin >= 15bps、close 点距边界 <= 800ms 时过滤为 `doge_three_last_fast_tightspread_tail`，拦截 rebuilt 数据中的 5.816445bps tail。
- DOGE：`last_before` >=2 source、0 exact、source spread >= 1.5bps、direction margin < 3bps、close 点距边界 <= 1000ms 时过滤为 `doge_multi_last_fast_midspread_near_flat`，拦截 `20260430_131908` live 中的 DOGE near-flat side mismatch / 5.293068bps tail。
- DOGE：`last_before`、2 source、source set 为 Bybit/Coinbase、0 exact、local side Yes、source spread >= 5bps、2bps <= direction margin < 3bps、close 点距边界 1000-1800ms 时过滤为 `doge_two_bybit_coinbase_last_stale_widespread_mid_margin`，拦截 `20260430_234139` 00:25 的 DOGE accepted side mismatch。
- DOGE：`last_before` >=2 source、0 exact、source spread >= 1.5bps、direction margin >= 5bps、close 点距边界 <= 700ms 时过滤为 `doge_multi_last_fast_midspread_tail`，并允许 `doge_binance_fallback` 接管；拦截 `20260430_122616` live 中的 DOGE accepted >5bps tail。
- DOGE：`last_before` >=2 source、0 exact、source spread >= 1.5bps、5bps <= direction margin < 10bps、close 点距边界在 1000-1800ms 时过滤为 `doge_multi_last_midspread_tail`；`margin>=10bps` 子区间历史 23 样本 side=0、max=4.358bps，已释放为 coverage rescue。
- DOGE：`last_before` >=3 source、0 exact、source spread >= 1.5bps、direction margin >= 15bps、close 点距边界 >= 2500ms 时过滤为 `doge_multi_last_stale_midspread_tail`，拦截 `20260430_153546` rolling run 中 2911ms stale 多源 tail / 5.436082bps。
- DOGE：`last_before` >=2 source、0 exact、source spread >= 1.5bps、direction margin < 3.5bps、close 点距边界 >= 1800ms 时过滤为 `doge_multi_last_stale_near_flat`。
- DOGE：`doge_binance_fallback` 增加 close 点距边界 <= 4000ms，过滤 `20260430_124933` 中 4.1s stale Binance fallback 导致的 5.052314bps tail。
- DOGE：`doge_binance_fallback` 单源 Binance、`after_then_before`、0 exact、direction margin < 8bps 时过滤为 `doge_binance_fallback_mid_margin`；同时 Rust source fallback 会二次调用同一套 filter，避免 fallback 绕过 router guard。

已新增的 BNB guardrail：

- BNB：`after_then_before`、2 source、0 exact、local side Yes、source spread >= 1.5bps、direction margin < 1.5bps 时过滤为 `bnb_mid_spread_yes_near_flat`，拦截 `20260429_223509` 新 unseen 中的 accepted side mismatch。
- BNB：`after_then_before`、单源、0 exact、local side Yes、direction margin < 4.5bps 时过滤为 `bnb_single_yes_near_flat`，拦截 `20260430_000805`、overnight 和 `20260430_164633` 中的 accepted side mismatch。
- BNB：`after_then_before`、单源、0 exact、local side Yes、direction margin >= 7bps、close 点距边界 <= 1200ms 时过滤为 `bnb_single_yes_fast_tail`，拦截 `20260430_095416` 新 live run 中的 BNB >5bps tail。
- BNB：`after_then_before`、<=2 source、0 exact、local side No、direction margin < 2.0bps 时过滤为 `bnb_no_near_flat`，拦截旧 challenger 汇总与最新 rolling 中的 near-boundary side mismatch。
- BNB：`after_then_before`、单源 Binance、0 exact、local side No、direction margin >= 5bps、close 点距边界 <= 1000ms 时过滤为 `bnb_single_binance_no_fast_tail`，拦截 `20260430_132428` 中的 5.072600bps tail。
- BNB：`after_then_before`、单源 Bybit、0 exact、local side No、direction margin >= 10bps 时过滤为 `bnb_single_bybit_no_tail`，拦截旧 challenger 汇总中 `5.011356bps` tail。
- BNB：`after_then_before`、>=3 source、0 exact、local side Yes、direction margin < 2.5bps 时过滤为 `bnb_three_yes_near_flat`。
- BNB：`after_then_before`、>=3 source、0 exact、local side Yes、source spread >= 3bps、direction margin < 3.5bps 时过滤为 `bnb_three_wide_spread_yes_near_flat`，拦截 `20260430_122616` live 中的 BNB accepted side mismatch。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source spread <= 0.55bps、direction margin >= 3bps 时过滤为 `bnb_two_tight_spread_yes_tail`。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source spread >= 1.5bps、direction margin < 5bps 时过滤为 `bnb_two_wide_spread_yes_near_flat`。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source spread <= 1.3bps、direction margin < 2.2bps 时过滤为 `bnb_tight_spread_yes_near_flat`，拦截 `20260430_130911` live 中的 near-flat side mismatch。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source spread <= 1bps、direction margin >= 15bps 时过滤为 `bnb_two_tight_spread_yes_far_margin`，拦截 `20260430_000805` 中的 >5bps tail。
- BNB：`after_then_before`、2 source、0 exact、local side No、source spread 1.0-2.0bps、direction margin < 2.2bps 时过滤为 `bnb_two_no_midspread_near_flat`，拦截 `20260430_165604` 中的 5.960428bps no-side midspread tail。
- BNB：`after_then_before`、单源 Binance、0 exact、local side Yes、5.0bps <= direction margin < 5.5bps、close 点距边界 >= 1800ms 时过滤为 `bnb_single_binance_yes_stale_mid_margin`，拦截 `20260430_164633` 中的 5.090907bps stale mid-margin side mismatch。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source set 为 Binance/Bybit、source spread <= 1.2bps、2bps <= direction margin < 3bps、close 点距边界 <= 800ms 时过滤为 `bnb_two_binance_bybit_yes_midspread_near_flat`，拦截 `20260430_173742` 18:30/18:50 两个 BNB accepted side mismatch。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source set 为 Binance/Bybit、source spread <= 1.3bps、2bps <= direction margin < 3bps 时过滤为 `bnb_two_binance_bybit_yes_lowspread_mid_margin`，拦截 `20260430_234139` 23:50 和 00:45 两个 BNB accepted side mismatch。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source set 为 Binance/Bybit、source spread <= 1.0bps、5.5bps <= direction margin < 7bps、close 点距边界 <= 500ms 时过滤为 `bnb_two_binance_bybit_yes_lowspread_fast_upper_mid_tail`，拦截 `20260430_234139` 00:50 的 5.098729bps accepted tail。
- BNB：`after_then_before`、2 source、0 exact、local side Yes、source set 为 Binance/Bybit、source spread <= 1.3bps、7bps <= direction margin < 8bps、close 点距边界 >= 2500ms 时过滤为 `bnb_two_binance_bybit_yes_stale_mid_margin_tail`，拦截 `20260430_185622` 22:05 中 5.281092bps stale tail。
- BNB：`bnb_okx_fallback` 单源 `after_then_before` 只保留 7-8bps relief；direction margin >= 8bps 过滤为 `bnb_okx_fallback_far_margin`。

已新增的 BTC guardrail：

- BTC：`after_then_before`、单 Coinbase source、0 exact、local side Yes、direction margin < 0.451bps 时过滤为 `btc_single_near_flat`；`0.451-1.25bps` 子区间释放后 expanded accepted +10，side=0、max 仍 <= 5bps。

已新增的 XRP guardrail：

- XRP：`nearest_abs`、单源 Binance、0 exact、local side Yes、2.5bps <= direction margin < 3.5bps、close 点距边界 >= 2000ms 时过滤为 `xrp_single_binance_yes_stale_mid_margin`，拦截 `20260430_185622` 22:00 中的 accepted side mismatch。

已新增的 HYPE tail guardrail：

- HYPE：`after_then_before`、source_count >= 3、0 exact、source spread 2-4bps、direction margin >= 40bps 时，不再接受 weighted primary，改走 close-only fallback；拦截 latest test `6.801403bps` outlier。
- HYPE：`after_then_before`、source_count >= 3、0 exact、source spread >= 8bps、direction margin < 8bps、close 点距边界 <= 500ms 时过滤为 `hype_three_after_high_spread_fast_mid_margin`，拦截 `20260430_173742` 18:25 的 5.614233bps accepted tail。
- HYPE：`after_then_before`、source_count >= 4、0 exact、source spread >= 10bps、direction margin < 8bps 时过滤为 `hype_four_after_high_spread_mid_near_flat`，拦截 `20260430_124041` live 中的 6.175301bps tail。
- HYPE：`after_then_before`、source_count >= 4、0 exact、source spread >= 10bps、direction margin < 20bps 时过滤为 `hype_four_after_high_spread_mid_margin`，拦截 `20260430_162025` rolling run 中的 6.285415bps 4-source high-spread tail。
- HYPE：`after_then_before`、source_count >= 2、0 exact、source spread >= 8bps、direction margin >= 30bps 时过滤为 `hype_after_high_spread_margin`，拦截 `20260429_232345` 新 unseen 中的 5.274113bps / 5.079526bps tail。
- HYPE：`after_then_before`、>=3 source、0 exact、direction margin < 1.2bps 时过滤为 `hype_three_after_near_flat`，拦截旧 challenger 汇总中的 near-boundary side mismatch。
- HYPE：`after_then_before`、2 source、0 exact、source spread 1.0-1.5bps、direction margin < 1.8bps、close 点距边界 <= 1000ms 时过滤为 `hype_two_after_mid_spread_fast_near_flat`。
- HYPE：`after_then_before`、>=3 source、0 exact、local side Yes、source set 为 Coinbase/Hyperliquid/OKX 且无 Bybit、source spread >= 4bps、direction margin < 4bps 时过滤为 `hype_three_coinbase_hl_okx_yes_near_flat`。
- HYPE：`after_then_before`、2 source、0 exact、source spread >= 10bps 时过滤为 `hype_two_after_very_high_spread`，拦截 `20260430_000805` 中的 5.968113bps / 5.922938bps tail。
- HYPE：`after_then_before`、2 source、0 exact、source spread >= 6bps、direction margin >= 15bps 时过滤为 `hype_two_after_high_spread_mid_margin`。
- HYPE：`after_then_before`、2 source、source set 为 Bybit/Hyperliquid、0 exact、local side Yes、2bps <= source spread < 3bps、3bps <= direction margin < 5bps、close 点距边界 <= 500ms 时过滤为 `hype_two_bybit_hyperliquid_fast_midspread_yes_mid_margin`，拦截 `20260430_220844` 22:25 accepted side mismatch。
- HYPE：`after_then_before`、2 source、source set 为 Bybit/Hyperliquid、0 exact、local side Yes、3bps <= source spread < 4bps、2bps <= direction margin < 3bps、close 点距边界 <= 500ms 时过滤为 `hype_two_bybit_hyperliquid_fast_widespread_yes_near_margin`，拦截 `20260430_234139` 00:35 的 HYPE accepted side mismatch。
- HYPE：`after_then_before`、3 source、source set 为 Bybit/Hyperliquid/OKX、0 exact、local side Yes、3bps <= source spread < 5bps、10bps <= direction margin < 15bps、close 点距边界 >= 1500ms 时过滤为 `hype_three_bybit_hyperliquid_okx_stale_midspread_mid_margin_tail`，拦截 `20260430_220844` 22:35 的 >5bps tail。
- HYPE：`after_then_before`、>=2 source、0 exact、local side Yes、source spread >= 6bps、direction margin < 3.5bps 时过滤为 `hype_after_high_spread_yes_near_flat`，拦截 `20260430_095416` 与 `20260430_153546` rolling run 中的 HYPE high-spread yes near-flat side mismatch。
- HYPE：`after_then_before`、2 source、0 exact、source spread >= 1.5bps、direction margin < 1.8bps 时过滤为 `hype_two_after_wide_spread_near_flat`。
- HYPE：`close_only_fallback` 单源、0 exact、direction margin < 3bps 时不再允许 fallback accepted，拦截 overnight close-only side mismatch。
- HYPE：`close_only_fallback` 单源、0 exact、local side Yes、direction margin < 5.5bps 时过滤为 `hype_close_only_single_yes_near_flat`。
- HYPE：`close_only_fallback` 单源、0 exact、local side Yes、direction margin < 7bps、close 点距边界 >= 500ms 时过滤为 `hype_close_only_single_yes_stale_near_flat`。
- HYPE：`close_only_fallback` 单源 Hyperliquid、0 exact、local side No、4bps <= direction margin < 10bps、close 点距边界 500-1000ms 时过滤为 `hype_close_only_single_hyperliquid_no_stale_mid_margin_tail`，拦截 `20260430_220844` 22:15/22:20 两个 >5bps tail。
- HYPE：`close_only_fallback` Bybit/Hyperliquid 双源、0 exact、local side Yes、1bps <= source spread < 2.5bps、5bps <= direction margin < 7bps、close 点距边界 >= 1000ms 时过滤为 `hype_close_only_bybit_hyperliquid_yes_stale_midspread_mid_margin`，拦截 `20260430_220844` 22:25 fallback side mismatch。
- HYPE：`close_only_fallback` >=2 source、0 exact、source spread <= 1bps、direction margin >= 40bps 时过滤为 `hype_close_only_tight_far_margin_tail`。
- HYPE：`close_only_fallback` >=2 source、0 exact、source spread >= 4bps、direction margin >= 8bps 时过滤为 `hype_close_only_multi_wide_spread_margin`，阻止 weighted high-spread 被过滤后继续降级为 close-only accepted tail；拦截 `20260430_162025` 中同一轮的 5.993419bps fallback tail。
- HYPE：close-only 仅 1 source、0 exact、direction margin >= 20bps 时直接 missing，避免单一 Hyperliquid close-only 在大幅偏离 open 时形成 >5bps accepted tail。

已新增的 SOL guardrail：

- SOL：`after_then_before`、2 source、0 exact、direction margin < 1.0bps 时过滤为 `sol_after_near_flat`；原 1.7bps 阈值中的 `margin>=1.0bps` 子区间历史 25 样本 side=0、max=1.553bps，已释放为 coverage rescue。
- SOL：`only_okx_coinbase`、`after_then_before`、2 source、0 exact、local side Yes、source set 为 Coinbase/OKX、source spread >= 7bps、direction margin < 1.5bps、close 点距边界 <= 600ms 时过滤为 `sol_okx_coinbase_high_spread_yes_near_flat`，拦截 `20260501_005117` 01:25 的 SOL accepted side mismatch。

已新增的 ETH guardrail：

- ETH：`last_before`、单 Coinbase source、0 exact、direction margin < 1.0bps 时过滤为 `eth_single_last_near_flat`；原 1.5bps 阈值中的 `1.0-1.5bps` 子区间历史 14 样本 side=0、max=1.045bps，已释放为 coverage rescue。
- ETH：`last_before`、单 Coinbase source、0 exact、local side Yes、1.0bps <= direction margin < 2.2bps、close 点距边界 <= 500ms 时过滤为 `eth_single_coinbase_yes_fast_mid_margin`，拦截 `20260430_224806` 23:15 的 ETH accepted side mismatch。

当前剩余瓶颈：

- G0/G4：v1.1 gate all-runs accepted side=0；`20260505_100833` 首轮 runtime shadow accepted side=0。下一步需要 rolling unseen 继续累计到至少 100 accepted clean。
- G1：`20260505_100833` 首轮 runtime accepted-ready p95=29ms、max=29ms，明显满足 300ms 目标；仍需长样本确认没有 worker late-start 或 missing deadline tail 污染可执行路径。
- G2：v1.1 gate accepted=6569、filtered=3307、missing=599，覆盖率 82.37%；安全性达标但仍低于 85%，BNB 单币种 coverage 仍是主要瓶颈。
- G3：v1.1 gate mean=1.076806bps、p95=3.319512bps、p99=4.444944bps、max=4.987616bps；mean/max 达标，p95 仍高于 3bps，top accepted tail 贴近 5bps，需要继续压缩。
- 数据质量：`20260505_100833` 已确认 v1.1 模型加载和 runtime shadow 正常；后续仍需观察长时间运行是否还有坏行、source missing 或 shared-ingress 退化。

结论：当前已经从“硬编码规则追尾部”进入“统计 gate + 运行时 shadow 验证”阶段。v1.1 把安全 coverage 提升到 82.37%，首轮 runtime 延迟和误差都干净，但 G2/G3 还没完全达标，尚不能进入生产集成。

## 推进路线

| 阶段 | 目标 | 输出 | 状态 |
| --- | --- | --- | --- |
| P0 | shared-ingress 与多实例隔离稳定 | broker/client 稳定，日志按 instance 隔离 | 已完成基础版 |
| P1 | boundary tape 与评估器稳定 | 可重复生成 close-only dataset 和 router eval | 已完成，evaluator 已对齐运行时 fallback/filter；boundary tape 写入已加互斥锁 |
| P2 | router v1 清零 accepted side mismatch | 最新 test/train/unseen 均 side=0 | 已达到当前样本，继续 rolling 验证 |
| P3 | 覆盖率提升 | 在 side=0 前提下减少 filtered/missing | 进行中，v1.1 gate coverage=82.37%，仍需继续 rescue，BNB 是最大瓶颈 |
| P4 | 延迟验收 | 本地 final ready p95 <= 500ms，冲刺 <= 300ms | 进行中 |
| P5 | 生产集成评估 | shadow -> dry-run -> limited enable | 阻塞于 P2/P4 |

## 执行节奏

| 时间点 | 动作 | 验收 |
| --- | --- | --- |
| T+0 | 修复当前 HYPE/XRP 两个 guardrail，重建 release，重启 challenger | 已完成，离线 latest test/train side=0 |
| T+30m | 检查新增 unseen rounds | 已完成，`20260429_210600` 当前 43 accepted、side=0 |
| T+2h | 汇总 20+ 新轮次 | 22:35 run 暴露 BNB side mismatch / DOGE high-spread tail，guardrail 已补齐 |
| T+3h | 汇总下一批 unseen | 23:23 run 暴露 BTC single-source near-flat side mismatch 和 HYPE high-spread tail，guardrail 已补齐，需重建 release 并重启 challenger 验证 |
| T+4h | 汇总新 challenger run | 00:08 run 暴露 HYPE wide-spread near-flat、HYPE two-source very-high-spread、BNB single-source near-flat 和 BNB tight-spread far-margin 风险，guardrail 已补齐，需重建 release 并重启 challenger 验证 |
| T+overnight | 复盘 overnight challenger | `20260430_012719` post-guard replay accepted=502、side=0、max=4.898166bps；需重建 release 并重启 challenger 验证真实运行时 |
| T+current | 修复 expanded dataset 漏扫、12:45/12:55/13:05/13:15/13:20/15:30/15:35/16:20/16:40/16:50/17:00/17:10/17:25/17:35/17:40-01:25 rolling case，并新增第二轮保守 coverage rescue 与 09:55 BNB/HYPE tail fix | expanded all accepted=3133、side=0、filtered=549、missing=248、mean=1.204325bps、p95=3.778491bps、p99=4.558736bps、max=4.961959bps；09:59 已重建 release 并重启 challenger，10:00 贴边轮次 router_v1 全部 side=0/max=4.231604bps |
| T+12h | 过夜 shadow/dry-run | rolling 100 accepted side=0 |
| T+24h | 冻结候选 router 或继续 shadow | 满足 G0/G1/G4 才进入生产集成评估 |

## 调参原则

- 先保方向，再保覆盖，最后优化 bps。
- 不做全局强行替换；任何规则必须通过 train/test/unseen。
- per-symbol guardrail 可以接受，但必须有可解释的市场结构原因，例如 source spread、source count、exact source、close rule、near-flat margin。
- 对新增 fallback 只允许在历史和 unseen 都无 side mismatch 后启用。
- 如果某一币种长期依赖单一来源且 near-flat 风险高，宁可 missing，不接受方向赌错。

## 下一步

1. 下次重启 `pm_shared_ingress_broker` 和 `pm_local_agg_challenger` 后继续观察；目标 rolling accepted side=0、accepted max<=5bps、accepted local_ready p95<=500ms。
2. 若下一批仍有 side mismatch 或 >5bps accepted tail，继续优先过滤为 missing，不先追 coverage。
3. 若连续 30 分钟新样本 side=0/max<=5bps，再重新做 coverage rescue 搜索，重点从 source timing 和 multi-source consensus 下手，不再放宽危险单源 fallback。
4. 下一次建议在 20-30 分钟后复盘；如果届时仍无 accepted bad case，再进入 2 小时 rolling 验收。
