# Symmetric Activation Summary Scorer

时间：2026-05-25 BJT

结论：`KEEP_SYMMETRIC_ACTIVATION_SUMMARY_SCORER_READY`

## 本轮做了什么

新增本地 scorer：

`scripts/xuan_symmetric_activation_summary_scorer.py`

它只读 runner `summary.json` / `aggregate_report.json`，验证上一轮默认关闭 instrumentation 的输出合同。

## 验证项

scorer 检查：

- default-off aggregate 不含 `event_lite.symmetric_activation_summary`。
- enabled aggregate 含 `symmetric_activation_contract_summary_v1`。
- `field_contract` 保持 default-off、无 post-action labels、无 realized pair-cost live criterion、无 trading behavior change。
- blocked activation denominator 非零。
- admitted activation denominator 非零。
- source_sequence coverage present。
- projected pair-cost bucket present。
- summary files 与 aggregate parity 通过。

真实本地 fixture scorer 结果：

| 指标 | 数值 |
|---|---:|
| blocked_activation_denominator | 1 |
| admitted_activation_denominator | 1 |
| aggregate_total | 2 |
| summary_file_total | 2 |
| summary_file_count | 1 |

## 边界

本轮没有：

- fetch 新数据
- 读取外部 worktree
- SSH
- shadow/canary/live
- observer dry-run
- local agg/service/shared WS
- raw/replay/full-store scan
- shared-ingress/broker/env/live 修改
- order/cancel/redeem

这是 scorer/tooling readiness，不是 strategy evidence、private truth、deployable、canary 或 promotion evidence。

## 下一步

不要因为 scorer ready 直接启动 no-order diagnostic。下一步只能：

- 等未来 allowlisted no-order pullback 自然包含 `symmetric_activation_summary_v1` 后跑该 scorer；或
- 本地继续做 scorer adapter/spec，定义未来 pullback scorer 如何与 acceptance gate 串联。
