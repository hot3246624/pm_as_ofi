# Public Profile 输入阻塞交接

时间：2026-05-24
结论：`UNKNOWN_PUBLIC_PROFILE_INPUT_BLOCKED_SAFE_72H_SOURCE_REQUIRED`

## 当前状态

当前 worktree 已经完成三层收口：

1. ce25/b55 单日 public-profile 规则拆解完成，但只能作为 proxy。
2. ce25 72h 验证合同已定义，但当前 worktree 内没有 ce25 72h daily-cohort source。
3. 全部当前 worktree public-profile 输入已盘点：ce25、b55、04b6、9f5、ohanism、gabagool22 都只有 1 个 BJT day；唯一表面 3 天的 `0x8dxd` 已被 validation 拒绝。

因此不能继续从当前 public artifacts 里强挖策略，也不能启动 no-order diagnostic。

## 安全 72h manifest 要求

若要继续 ce25/b55/04b6/9f5/ohanism/gabagool 任一 public-profile 线，必须先把安全离线导出的 manifest 放进当前 worktree，例如：

```text
xuan_research_artifacts/<lead>_safe_72h_public_export_<UTC>/manifest.json
```

最低要求：

- 位于 `/Users/hot/web3Scientist/pm_as_ofi-xuan-research` 当前 worktree 内。
- `safe_offline_72h_export=true`。
- 不 fetch 新数据、不读外部 worktree、不启动 service/local agg/shared WS、不 SSH、不 shadow/canary/live、不读 raw/replay/full-store。
- 至少 3 个 BJT day，每天样本数足够。
- 每日 cohort rows 至少包含：
  - `day_id`
  - `account`
  - `cohort`
  - `markets`
  - `buy_actual`
  - `cohort_pnl_ex_rebate`
  - `cohort_pnl_including_rebate`
  - `pair_pnl`
  - `residual_pnl_est`
  - `wins`
  - `losses`
  - `avg_pair_cost`
  - `residual_rate`
  - `old_redeem_contamination`
  - `post_window_same_condition_buy`
  - `rebate`
- 明确 source paths，且所有 source paths 都在当前 worktree 内。

## 推荐验证命令

ce25 线：

```bash
python3 scripts/xuan_ce25_three_day_pair_cost_validation_spec.py \
  --candidate-three-day-manifest <manifest.json> \
  --output-dir xuan_research_artifacts/xuan_ce25_three_day_pair_cost_validation_score_<UTC>
```

任意 public-profile 多日行级 source：

```bash
python3 scripts/xuan_public_profile_multiday_source_inventory.py \
  --output-dir xuan_research_artifacts/xuan_public_profile_multiday_source_inventory_<UTC>

python3 scripts/xuan_public_profile_multiday_validation_spec.py \
  --public-input <activity_trade_rows.json> \
  --output-dir xuan_research_artifacts/xuan_public_profile_multiday_validation_score_<UTC>
```

## 明确禁止

- 用单日桶继续冒充 72h 策略证据。
- 用 price cap/static asset-timeframe filter 继续拟合。
- 把 realized `pair_cost` 当 live criterion。
- 复用 D+ 已失败的 micro-deficit / ledger / tiny-deficit / closed-cycle 家族。
- 用 fixture/synthetic rows 当策略证据。
- 读取外部 `poly_trans_research` worktree 或 forbidden raw/replay/full store。
- 启动 SSH、shadow、canary、live、local agg/shared WS/service 或 order/cancel/redeem。

## 决策

`strategy_evidence=false`
`no_order_diagnostic_allowed=false`
`private_truth_ready=false`
`deployable=false`
`promotion_gate.passed=false`

下一步只有两种合法路径：

1. 用户或离线流程把安全 72h manifest 放入当前 worktree 后，运行对应 scorer。
2. 新研究线必须自带当前 worktree 内已有多日证据；否则先做输入源准备，不做策略判断。
