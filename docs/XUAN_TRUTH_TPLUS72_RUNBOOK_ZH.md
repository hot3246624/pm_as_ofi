# Xuan 真值数据到位后 T+72h 冲刺 Runbook

生成时间：`2026-04-27`

目标：在新 truth 数据到位后 `72h` 内输出第一版非 `provisional` 的 `xuan completion gate` 结论。

## 0. 输入前提

必须同时具备：

- `poly_trans_research` 的 startup audit 通过
- 新 `xuan episode truth` 可导出
- 当前分支的 shadow replay 可生成 `completion_first_shadow_report`

未满足上述前提时，不进入冲刺阶段。

## 1. T+0h 到 T+8h：先裁决数据本身

顺序固定为：

1. 读取 `startup_audit.json`
2. 检查 `all_passed`
3. 检查 `recent_overlap_episode_count`
4. 检查 `holdout_lift_pct`

停止条件：

- `recent_overlap_episode_count < 300`
- 或 audit 未通过
- 或 `holdout_lift_pct < 0.10`

一旦命中停止条件，本轮只允许输出“数据尚不可裁决”，不允许继续推进到策略结论。

## 2. T+8h 到 T+24h：导出 xuan 真值与 gate

顺序固定为：

1. 重导 `xuan_completion_gate_summary.json`
2. 重导 `xuan_completion_gate_defaults.json`
3. 检查 `provisional` 是否仍为 `true`

必须回答：

- `30s completion` lift 是否已经出现
- lift 主要落在哪些 `score bucket / session bucket`
- `maker_proxy_ratio` 是否仍稳定在既有叙事内

## 3. T+24h 到 T+48h：导出我方 shadow 与 gap

顺序固定为：

1. 导出 `completion_first_shadow_summary.json`
2. 导出 `xuan_completion_gap_*.json`
3. 如 baseline 报告可用，再做 baseline 对比

解释顺序固定为：

1. 先看 `control_screening`
2. 再看 `gap_vs_xuan`
3. 最后才看单市场 / 单轮 episode drill-down

只回答三件事：

- 哪些候选被 block
- 哪些候选被放行后未在 `30s` 内出现 opposite
- 哪些已经出现 opposite 但仍未 clean close

## 4. T+48h 到 T+72h：输出裁决

最终裁决只允许三种：

- `Data Not Usable`
- `Research Not Effective`
- `Shadow Gap Actionable`

含义固定为：

- `Data Not Usable`：truth 覆盖和 startup audit 不足
- `Research Not Effective`：truth 覆盖足够，但 gate 对 `30s completion` 没有形成足够 lift
- `Shadow Gap Actionable`：truth 足够，gate 有效，我方 shadow 与 xuan 的差距可定位到 timing / clip / fillability 之一

## 5. 必出文件

冲刺完成后必须得到：

- `docs/xuan_completion_gate_summary.json`
- `configs/xuan_completion_gate_defaults.json`
- `completion_first_shadow_summary.json`
- `xuan_completion_gap_*.json`

如 baseline 可用，再额外产出 baseline compare json。

## 6. 禁止事项

- 不允许边看边改指标定义
- 不允许在数据不足时口头宣布“已逼近 xuan”
- 不允许跳过 startup audit 直接看策略结果
- 不允许拿单个漂亮 round 代替 cohort 裁决
