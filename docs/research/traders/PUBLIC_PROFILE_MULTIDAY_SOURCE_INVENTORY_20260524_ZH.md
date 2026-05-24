# Public Profile 多日输入盘点与 0x8dxd 验证

时间：2026-05-24
范围：只读当前 worktree 中已有 public input、artifact manifest 和 allowlisted source files。

## 结论

最终决策：`UNKNOWN_PUBLIC_PROFILE_MULTIDAY_VALIDATION_SAMPLE_INSUFFICIENT`

这不是 ce25/b55 规则失败，而是当前 worktree 的 public-profile 输入不足以继续做 72h mimicability 证明。盘点发现只有 `0x8dxd` 有 3 个 BJT 日期的公开行，但验证后发现它不是稳定多日样本：

| 账户 | 最多 BJT 日数 | 最多 rows | 结果 |
|---|---:|---:|---|
| ce25 | 1 | 3497 | 不足 3 天 |
| b55 | 1 | 3492 | 不足 3 天 |
| 04b6 | 1 | 3500 | 不足 3 天 |
| 9f5 | 1 | 3426 | 不足 3 天 |
| ohanism | 1 | 3490 | 不足 3 天 |
| gabagool22 | 1 | 3426 | 不足 3 天 |
| 0x8dxd | 3 | 3500 | 日样本极不均衡，验证失败 |

## 新增工具

- `scripts/xuan_public_profile_multiday_source_inventory.py`
- `scripts/xuan_public_profile_multiday_source_inventory_smoke.sh`
- `scripts/xuan_public_profile_multiday_validation_spec.py`
- `scripts/xuan_public_profile_multiday_validation_spec_smoke.sh`

新增 artifacts：

- `xuan_research_artifacts/xuan_public_profile_multiday_source_inventory_20260524T122616Z/manifest.json`
- `xuan_research_artifacts/xuan_public_profile_multiday_source_inventory_smoke_20260524T122616Z/manifest.json`
- `xuan_research_artifacts/xuan_public_profile_multiday_validation_spec_20260524T122616Z/manifest.json`
- `xuan_research_artifacts/xuan_public_profile_multiday_validation_spec_smoke_20260524T122616Z/manifest.json`

## 盘点结果

`xuan_public_profile_multiday_source_inventory_v1` 的最低门槛：

- public input 必须在当前 worktree 内
- 至少 3 个 BJT day
- 至少 500 行
- 必须包含 `conditionId/slug/timestamp/price/size/usdcSize/side/type`
- 不接受外部 worktree、single-day、fixture、price cap/static filter、realized pair_cost live criterion、D+ 失败族

盘点结果给出 `KEEP_PUBLIC_PROFILE_MULTIDAY_SOURCE_READY`，但只因为 `0x8dxd` 的 source path 有 3 个 BJT day：

```text
xuan_research_artifacts/xuan_public_profile_reset_review_20260524T022800Z/public_inputs/0x63ce342161250d705dc0b16df89036c8e5f9ba9a/activity_trade_rows.json
```

因此我没有停止在“有 3 天”这个浅层条件，而是立即实现并运行 validation。

## 0x8dxd 验证结果

`xuan_public_profile_multiday_validation_v1` 要求每天都满足：

- trade rows `>=100`
- BUY rows `>=80`
- SELL share `<=10%`
- buy-side complete-set weighted pair cost `<=0.98`
- residual share `<=25%`
- both-outcome condition ratio `>=50%`

0x8dxd 失败点：

| BJT 日期 | trade rows | BUY rows | pair cost | residual share | 结论 |
|---|---:|---:|---:|---:|---|
| 2026-03-24 | 3469 | 3173 | 0.8791 | 26.8868% | residual 超阈值 |
| 2026-04-29 | 30 | 28 | 无 matched pair | 100% | 样本太小且单边 |
| 2026-05-12 | 1 | 0 | 无 matched pair | 无 | 样本太小且 SELL-only |

所以 `valid_day_count=0`，阻塞项包括 `fewer_than_3_valid_days`。

## 决策

`public_profile_multiday_source_inventory` 找到了一个表面多日源，但 `public_profile_multiday_validation_spec` 证明它不能作为 mimicability 证据。

因此：

- `strategy_evidence=false`
- `no_order_diagnostic_allowed=false`
- `promotion_gate.passed=false`
- `private_truth_ready=false`
- `deployable=false`

下一步不应继续从当前 worktree public-profile 样本里强挖策略。要么提供/生成安全的 72h public export manifest 放进当前 worktree，要么切换到另一个已经有当前 worktree 多日证据的数据线。
