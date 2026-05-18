# Local Agg Selector-History Debias Safety Plan

更新时间：2026-05-13 22:46Z

本文是研究设计，不是 runtime 实现。目标是把当前 SOL/BNB offline debiased selector 的上线前安全语义写清楚，避免把一个看起来有效的 replay helper 直接搬进 runtime。

## 当前结论

当前 strongest full offline candidate 的 p95 改善主要来自两类逻辑：

- deterministic source-window selector：HYPE mid-spread、DOGE shallow-window、DOGE OKX-only deeper-window。
- selector-history/debias selector：SOL `same_shallower + Coinbase`，BNB `local_sources contains Bybit`。

复杂度调整后的 runtime 推荐仍是 Option B1：只实现 deterministic HYPE+DOGE。SOL/BNB 不建议一起上线，因为它们需要历史 selector state、signed-error debias、参数冻结、降级和审计机制。

## 为什么 SOL/BNB 不能直接部署

当前 offline helper `scripts/research_local_agg_lag_selector_models.py` 做了这些事：

- 用历史 accepted rows 训练 source/kind/lag/depth key 的 signed error 分布。
- 每个 candidate 根据 p90、median abs、support penalty、recency penalty、after penalty 排序。
- 用历史 median signed error 做 debias。
- 选择当前 round 的最低 score candidate。

这在 replay 中是 causal-ish，但 runtime 仍缺几件关键设施：

- 固定训练集边界：不能让当前 run 的未来 row 影响当前 row。
- 参数版本：必须知道当前决策用的是哪一版历史表。
- warmup 降级：history 支持不足时不能用弱 fallback 悄悄接管价格。
- row-level regression guard：不能只看总体 p95，必须限制单行恶化。
- audit log：每次 fire 必须可还原 score、key、train_n、bias、offset。
- rollback/disable：必须能用 env/config 关闭，不重启共享 ingress。

## Runtime State Model

如果未来实现 SOL/BNB debias，建议不要在线学习。第一版只允许 frozen model：

```text
offline replay -> frozen selector table -> runtime read-only use -> post-run audit
```

禁止第一版 runtime 在服务运行中把新 accepted rows 追加到 selector history 并影响后续 round。原因是：

- 当前 gate 本身会选择样本，在线追加会形成选择偏差。
- 一旦出现 tail，后续参数会被污染，难以复盘。
- 重启后状态恢复和 exactly-once append 都会增加不必要复杂度。

## Frozen Table Shape

建议 future model artifact 使用 JSON，独立于 Rust binary，带 schema version：

```json
{
  "schema_version": 1,
  "candidate_id": "sol_same_shallower_coinbase_trigger_mt8_20260512_13",
  "created_at": "2026-05-13T22:46:00Z",
  "train_runs": ["20260511_083910", "20260512_014312"],
  "symbols": ["sol/usd"],
  "min_train": 8,
  "recency_penalty_bps_per_sec": 0.02,
  "after_penalty_bps": 0.75,
  "max_allowed_row_worsen_bps": 0.75,
  "keys": [
    {
      "key": ["symbol_source_kind_lag_depth", "sol/usd", "coinbase", "tape", "pre_250ms_1s", "same_shallower"],
      "n": 12,
      "median_signed_bps": -0.18,
      "median_abs_bps": 0.64,
      "p90_abs_bps": 1.42,
      "p95_abs_bps": 1.88
    }
  ]
}
```

Runtime 应只读取这个 frozen table，不在内存中改变它。

## Candidate Eligibility Guard

即使 frozen table 中存在 key，runtime 也必须先通过决策时可见的 eligibility guard。

### SOL candidate guard

当前 research-only trigger：

- symbol = `sol/usd`
- selected source = `coinbase`
- selected depth = `same_shallower`
- key train support >= 8
- debiased selected candidate 不产生 side flip

未来上线前还应增加：

- candidate score 必须比当前 runtime close 的 conservative score 至少好 `min_score_improve_bps`
- candidate 不能把 abs error proxy 估计推高超过 `max_allowed_row_worsen_bps`
- candidate source age 不超过 frozen model 对应 bucket 上限
- selected source 必须在 boundary tape 可见，且不是 after-boundary，除非该 artifact 明确允许 after

### BNB candidate guard

当前 research-only trigger：

- symbol = `bnb/usd`
- runtime local source set contains `bybit`
- key train support >= 20
- debiased selected candidate 不产生 side flip

未来上线前还应增加：

- 不使用 row-level diagnostic-only gated tails 训练 accepted model，除非明确分层。
- 对 source spread / microstructure fields 缺失保持保守；缺失不应触发更大胆选择。
- 在 BNB 4.984909bps historical residual 未能被 visible tape 修复前，不把该 selector 描述为 max repair，只能描述为 p95 improvement。

## Runtime Decision Flow

建议顺序：

1. 先计算现有 weighted boundary hit。
2. 先应用 deterministic source-regime selector：HYPE/DOGE。
3. 若 deterministic 未 fire，才考虑 frozen debias selector。
4. 对每个 candidate 计算：
   - key
   - train_n
   - score_bps
   - median_signed_bps
   - debiased_price
   - side check
   - row-worsen guard
5. 只有所有 guard 通过才替换 weighted boundary hit。
6. 否则保留原 runtime hit，并记录 `debias_skip_reason`。

不要把 SOL/BNB debias 放在 deterministic HYPE/DOGE 之前。

## Required Logs

每次 selector-history/debias fire 必须写入结构化字段：

- `policy_name`
- `model_artifact_id`
- `symbol`
- `round_end_ts`
- `source_subset`
- `local_sources`
- `selected_source`
- `selected_kind`
- `selected_lag_bucket`
- `selected_offset_ms`
- `selected_key`
- `selected_train_n`
- `selected_score_bps`
- `selected_median_signed_bps`
- `selected_debiased_price`
- `current_price`
- `direction_margin_bps`
- `side_check_passed`
- `row_worsen_guard_passed`

每次 skip 也应记录 compact reason，例如：

- `no_model`
- `insufficient_train_n`
- `source_guard_failed`
- `depth_guard_failed`
- `side_flip`
- `row_worsen_guard_failed`
- `candidate_after_boundary_disallowed`
- `missing_boundary_candidate`

## Replay Requirements Before Runtime Proposal

任何 SOL/BNB debias artifact 提交 Decision Needed 前，至少要通过：

- fixed accepted replay：`20260511_083910`, `20260512_014312`, `20260513_045906`
- train/test 方向：
  - train 20260511 -> test 20260512
  - train 20260511+20260512 -> test 20260513
- HYPE/DOGE deterministic normalization 同时开启，避免旧 HYPE/DOGE max 污染判断。
- BTC p95/max unchanged。
- side_errors 不增加。
- coverage/accepted count 不减少。
- 全局 p95/p99 改善，或 per-symbol p95 明确改善且 global max 不恶化。
- row-level regression table：列出所有 candidate_error - base_error > 0.5bps 的行。

如果某个 selector 只在当前 run 生效、历史 run 触发为 0，应标记为 weak/current-only，不能进 runtime。

## Build / Deploy Separation

不要把 debias model 与 deterministic HYPE/DOGE runtime patch 捆绑。

推荐两阶段：

1. Option B1：deterministic HYPE+DOGE-only dry-run checkpoint。
2. 后续独立 Decision Needed：frozen SOL/BNB debias artifact，带模型文件、replay、row regression review、rollback flag。

这样每次 checkpoint clock 都能清楚归因。

## Safety Position

当前状态：research plan only。

未修改 `src/bin/polymarket_v2.rs`，未部署，未重启。若未来实现，本方案仍要求用户显式批准 challenger restart，并且不得重启 `pm-shared-ingress-broker.service`、不得开启 live trading、不得放宽任何安全阈值。
