# Completion First 策略说明

## 目标
- 复刻 `xuanxuan008` 的核心做法，而不是继续扩写 `pair_arb`
- 第一阶段只支持 `BTC 5m`
- 第一阶段默认 `shadow`，只输出决策与生命周期事件，不真实下单
- 当前入口：
  - `PM_STRATEGY=completion_first`
  - 兼容别名：`xuan_clone`

## 研究裁决资产

- 机器可读 contract：`configs/xuan_research_contract.json`
- 问题树与证据矩阵：[docs/XUAN_RESEARCH_EVIDENCE_MATRIX_ZH.md](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/docs/XUAN_RESEARCH_EVIDENCE_MATRIX_ZH.md)
- Xuan 不变量矩阵：[docs/XUAN_ARCHETYPE_INVARIANTS_ZH.md](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/docs/XUAN_ARCHETYPE_INVARIANTS_ZH.md)
- Shadow 控制组筛查：[docs/XUAN_SHADOW_CONTROL_SCREEN_ZH.md](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/docs/XUAN_SHADOW_CONTROL_SCREEN_ZH.md)
- 假设分级表：[docs/XUAN_HYPOTHESIS_GRADING_ZH.md](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/docs/XUAN_HYPOTHESIS_GRADING_ZH.md)
- 真值到位后 `72h` 冲刺流程：[docs/XUAN_TRUTH_TPLUS72_RUNBOOK_ZH.md](/Users/hot/web3Scientist/pm_as_ofi_completion_first_v2_shadow/docs/XUAN_TRUTH_TPLUS72_RUNBOOK_ZH.md)

当前研究默认只回答 5 个模块：

1. `Open Gate`
2. `Completion Controller`
3. `First-Leg Sizing`
4. `Re-entry / Cooldown`
5. `Abort / Repair`

如果新的分析不能帮助裁决这 5 个模块，就不进入主研究链。

## 策略语义
- `FlatSeed`
  - 先生成 `pair_arb` 风格的 `YES_BUY` / `NO_BUY` 候选
  - 再走 `xuan completion gate`：
    - 只影响 `first leg open allow/block`
    - 只影响 `first leg clip multiplier`
    - 不改变 `completion-only`、`repair budget`、`merge/redeem` 语义
  - gate 默认读取 `configs/xuan_completion_gate_defaults.json`
  - 每个候选都会写一条 `completion_first_open_gate_decision`
- `CompletionOnly`
  - 一旦出现 active tranche，默认优先推动对侧 completion
  - 同侧加仓最多允许 1 次
  - same-side add 也使用 `completion_first clip`
- `HarvestWindow`
  - 收盘前 `t-25s` 进入
  - 只做 completion 评估与 merge 请求评估
- `PostResolve`
  - 结算后不再开新仓
  - 只做 winner-side redeem 请求评估

## Clip 规则
- 内置常量，不开放 env：
  - `BASE_CLIP = 150`
  - `MAX_CLIP = 250`
  - `MIN_CLIP = 45`
- 适用范围：
  - `FlatSeed` 候选 seed
  - `CompletionOnly` 下的 same-side add
  - completion 腿（上限为 `min(residual_qty, clip)`）
- 乘子：
  - `FlatSeed`：
    - 先算 `completion_first base clip`
    - 再乘 `gate score bucket clip_mult`
    - 再乘 `session_mult_by_utc_hour`
  - `CompletionOnly` / same-side add：
    - 继续使用原 `completion_first clip`
    - 不吃 `xuan gate`
- 最终数量四舍五入到 `0.1 share`

## 生命周期
- `merge`
  - `t-25s` 首次检查
  - `pairable_full_sets >= 10` 才请求
  - `t-18s` 最多 retry 一次
  - `shadow` 模式下会同时写 `completion_first_merge_requested` 与 `completion_first_merge_executed`
- `redeem`
  - 只针对 winner-side residual
  - `+35s` 首次请求
  - `+50s` 第二次请求

## Shadow 事件
- `completion_first_open_gate_decision`
- `completion_first_seed_built`
- `completion_first_completion_built`
- `completion_first_same_side_add_blocked`
- `completion_first_merge_requested`
- `completion_first_merge_executed`
- `completion_first_redeem_requested`

## 观察重点
- `pair_tranche_events`
- `pair_budget_events`
- `capital_state_events`
- `completion_first_open_gate_decision`
- `clean_closed_episode_ratio`
- `same_side_add_qty_ratio`
- `episode_close_delay_p50/p90`
- `open_candidate_count`
- `open_allowed_count`
- `open_blocked_count`
- `30s_completion_hit_rate`
- `30s_completion_hit_rate_when_gate_on`
- `30s_completion_hit_rate_when_gate_off`
- `median_first_opposite_delay_s`
- `gate_block_reason_counts`
- `score_bucket_distribution`
- `session_bucket_distribution`
- gap 默认只按以下顺序解释：
  - 为什么候选没被放行
  - 为什么放行后没有在 `30s` 内出现 opposite
  - 为什么出现 opposite 后仍未 clean close
- replay/report：
  - `python scripts/build_replay_db.py --date YYYY-MM-DD`
  - `python scripts/export_completion_first_shadow_report.py --date YYYY-MM-DD`
  - `python scripts/export_xuan_completion_gate.py`
  - `python scripts/export_xuan_completion_gap_report.py --report-json .../completion_first_shadow_summary.json`
  - `bash scripts/run_xuan_truth_t72_cycle.sh <UTC-DAY>`
