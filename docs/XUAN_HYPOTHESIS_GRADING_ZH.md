# Xuan 假设分级表

生成时间：`2026-04-27`

本文只保留四类结论：`已证伪`、`弱证据`、`强待验证`、`不可回答`。

## 1. 已证伪

- `固定 pair target`：不能成立。公开窗口里 `pair cost` 分布过宽，且 cohort 盈亏结构明显不是固定阈值可解释。
- `只靠单一尾盘公式赚钱`：不能成立。尾段/MERGE/REDEEM 重要，但不是解释 `30s completion` 的主模块。
- `只要进场就能神奇 100% 30s 配对`：不能成立。当前 overlap 样本里 `30s completion` 远未达到这个水平。
- `只靠 maker` 或 `只靠 taker`：不能成立。当前只能支持 `maker-leaning mixed execution`。

## 2. 弱证据

- `xuan` 的 edge 主要来自某个固定价格公式：弱证据，不作为主假设。
- `xuan` 主要靠 repair 才赚到钱：弱证据。repair 更像尾部处理，不像主 alpha。
- `xuan` 在所有时段都同样强：弱证据。当前 session 分布明显不均匀。

## 3. 强待验证

- `xuan` 的核心 edge 是 `state selection`：当前主假设，等待更厚 overlap truth。
- `Open Gate` 比 `dynamic pair target` 更重要：当前主假设。
- `30s completion` lift 来自 `对侧 spread / 近期成交节律 / 时段 / clip 质量` 的组合，而不是单一特征：当前主假设。
- `Re-entry / Cooldown` 是离散状态机，不是随机等待：当前主假设。

这些假设一律要用新 truth 数据裁决，不能靠口头升级为结论。

## 4. 不可回答

- `xuan` 的真实 resting order timeline
- `xuan` 的 queue position
- `xuan` 的精确 fee / rebate 优势
- `xuan` 的底层网络与托管延迟优势

这些项最多写成风险提醒，不能写成“待补一个接口就能解决”的待办。

## 5. 当前默认叙事

在新数据到位前，默认叙事固定为：

> `xuan` 更像一台基于状态筛选的 `completion-first` 自动机，而不是一个固定 `pair target` 公式或单一尾盘套利器。

如果后续分析不能直接帮助裁决这个叙事，就不再进入主研究链。
