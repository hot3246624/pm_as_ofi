# `xuan` 新真值到来后的证伪矩阵

生成时间：`2026-04-28`

---

## 1. 为什么必须先写证伪矩阵

两天后 `poly_trans_research` 的新数据一到，如果没有预先写死裁决条件，我们很容易犯 3 个错误：

1. 看到更高精度数据后继续扩展叙事，而不是缩小假设
2. 看到局部支持就兴奋，却没有检查是否同时推翻别的假设
3. 把“更接近某个好看的指标”误判成“更接近 xuan”

所以这份矩阵的目标很简单：

- 在新数据来之前，先把 **什么算支持、什么算推翻、推翻后意味着什么** 写死

这样数据一到，我们不是继续讨论，而是直接裁决。

---

## 2. 统一前提

### 2.1 本矩阵要裁决的，不是全部策略问题

当前优先级只围绕这 4 个研究模块：

1. `Winner-Bias 是否真实稳定存在`
2. `Winner-Bias 来源是什么`
3. `Winner-Bias` 与 `30s completion` 是否同源
4. `Open gate family` 到底是哪一类最像核心主线

### 2.2 当前不能靠新公开数据解决的问题

即使两天后数据更好，以下问题仍不能直接回答：

- xuan 的私有挂单时间线
- xuan 的撤单轨迹
- xuan 的 queue position
- xuan 的 fee / rebate 等级
- xuan 的真实 infra latency

这意味着：

- 新公开数据可以裁决 `state selection`
- 不能单独裁决 xuan 的完整执行优势

---

## 3. 数据可用门槛

在进入任何研究裁决前，先过 3 道门槛。

### 3.1 覆盖门槛

至少要满足：

- `book-aligned first-leg observations >= 200`
- `recent overlap episodes >= 300`
- `valid winner mapping coverage >= 95%`

### 3.2 时间精度门槛

至少要满足：

- 首腿对齐使用的 book age：`p50 <= 100ms`
- `p90 <= 500ms`

如果显著差于这个口径：

- 允许做 context 分析
- 不允许做强 microstructure 因果归因

### 3.3 窗口完整性门槛

至少要满足：

- 首腿后 `30s` 公共 trades 基本连续
- `MERGE` 前序列缺口不系统偏向某一类状态

否则：

- 只能讨论 `winner-bias existence`
- 暂不讨论 `completion mechanism`

---

## 4. 证伪矩阵

## H1. `MERGE` 前 `winner-bias` 是真实稳定现象，不是窗口错觉

当前工作性主张：

- 成立

当前证据：

- `pre_merge_bias_full`：`497/497` captured MERGE events
- `winner_heavier = 373`
- `loser_heavier = 124`
- `median delta = +11.27 shares`

新数据支持条件：

- `recent-only` 重新计算后，`winner-heavier ratio > 60%`
- `median delta` 仍为正
- 去除 undercovered MERGE 后方向不翻转

新数据推翻条件：

- recent window 中 `winner-heavier ratio <= 55%`
- 或 `median delta <= 0`
- 或结果只在极少数时间桶成立

若被推翻，意味着：

- `5c` 只能降级为窗口幻觉
- 当前 `winner-bias` 主叙事需要重开

---

## H2. 第一腿存在弱 winner-side 选择，但它不是全部来源

当前工作性主张：

- 成立，但仅是部分来源

当前证据：

- 全体首腿 `first winner ratio = 58.8%`
- 有 MERGE 样本首腿 `≈ 58.9%`
- 明显高于 50%，但远低于 `pre-MERGE winner-heavier ≈ 75.1%`

新数据支持条件：

- `book-aligned` 与 `all-public` 两个样本中，`first winner ratio > 55%`
- 但仍明显低于 `first5 winner-net ratio`

新数据推翻条件：

- `first winner ratio` 回落到 `50% ± 2pct`
- 或一旦只看高精度样本就失效

若被推翻，意味着：

- `winner-bias` 的主来源不在首腿择边
- 后续研究重心应进一步压向 fill geometry / merge timing

---

## H3. `winner-bias` 的主要放大器是高交替成交中的不对称 fill geometry

当前工作性主张：

- 成立，且是当前最强来源候选

当前证据：

- `first5 winner-net ratio = 72.7%`，显著高于 `first winner ratio = 58.8%`
- 早期最常见模式是：
  - `WLWLW`
  - `WLWL`
  - `LWLWL`
- `1/3/5` 更偏 winner，`2/4` 更偏 loser

新数据支持条件：

- 高交替结构仍主导早期路径
- `odd-position winner ratio - even-position winner ratio >= 10pct`
- `first5 winner-net ratio - first winner ratio >= 10pct`

新数据推翻条件：

- 真实高精度样本显示早期主要是同侧连续加仓
- 或者 `first5 winner-net` 与 `first winner` 差距基本消失

若被推翻，意味着：

- 当前“pair-gated geometry”解释过强
- xuan 可能比我们以为的更 directional

---

## H4. `winner-bias` 与 `30s completion` 不是同一个 edge，而是存在张力

当前工作性主张：

- 成立

当前证据：

- `W_first` 的 `<=30s opposite` 低于 `L_first`
- provisional L1 overlap 中：
  - 买更高价侧更偏 `winner-overweight`
  - 但更窄价差、更弱高价侧倾向更偏 `hit30`

新数据支持条件：

- 高价侧 / 更强一侧状态继续对应更高 `winner-quality`
- 但不自动对应更高 `<=30s` completion
- `Completion-First` / `Winner-Quality` 两端仍明显分离

新数据推翻条件：

- 更高价侧状态在高精度数据里同时稳定支配：
  - `winner-quality`
  - `30s completion`
- 或两者差异在 richer L1/L2 下完全消失

若被推翻，意味着：

- 当前 frontier/tradeoff 叙事太复杂了
- xuan 可能就是在找单一强状态，而不是折中状态

---

## H5. `Hybrid Frontier` 是当前最像核心 `open gate` 的主候选

当前工作性主张：

- 成立，但仍是工作性结论

当前证据：

- `Hybrid-Early`：
  - `65.1 / 74.6 / 63.5`
- `Hybrid-Mid`：
  - `76.5 / 82.4 / 64.7`
- 它们都在 baseline 之上，且没有像 `Winner-Quality` 那样明显牺牲 completion

新数据支持条件：

- 在更长窗口里，`0.6-0.7` 带的 early/mid family 仍同时满足：
  - `first winner lift > 0`
  - `first5 winner-net lift > 0`
  - `hit30 lift > 0`
- 至少有一个 family 的样本数上升到：
  - `n >= 40` 且方向不翻

新数据推翻条件：

- `0.6-0.7` 带只是短窗口噪音
- 或一扩容后只有 winner-quality 保留，completion 优势消失
- 或相反只剩 completion 优势，winner-quality 退化

若被推翻，意味着：

- 当前“frontier 主线”并不稳定
- 需要回到多 family 并存，而不是寻找单一主线

---

## H6. `Completion-First` 只解释 completion，不解释 xuan 的核心 edge

当前工作性主张：

- 更像边界 family，不像核心 family

当前证据：

- `Completion-First`：
  - `first winner = 47.4%`
  - `first5 winner-net = 71.8%`
  - `hit30 = 71.8%`
- completion 强，但 winner-quality 弱

新数据支持条件：

- 继续表现为：
  - `hit30 lift` 明显为正
  - `first winner lift` 不为正或明显偏弱

新数据推翻条件：

- richer L1/L2 下它同时成为最强 winner-quality family
- 或它占据大部分高质量轮次，而非 `Hybrid Frontier` family

若被推翻，意味着：

- 当前对“低价腿只偏 completion”的理解不成立

---

## H7. `Winner-Quality` 解释了 bias，但不能解释 30s 内高成功配对

当前工作性主张：

- 成立

当前证据：

- `Winner-Quality`：
  - `first winner = 64.0%`
  - `first5 winner-net = 88.0%`
  - `hit30 = 44.0%`

新数据支持条件：

- 继续表现为 winner 强、completion 弱
- 对应更宽价差 / 更强 side-location

新数据推翻条件：

- 在高精度样本里它同时拥有稳定高 `hit30`
- 且不再明显慢于 `Completion-First` / `Hybrid`

若被推翻，意味着：

- winner-bias 与 completion 的张力被夸大了

---

## H8. 微观结构上，“更高价侧 + 更宽价差”偏 winner，“更窄价差”偏 completion

当前工作性主张：

- provisional 成立

当前证据：

- 当前可对齐样本 `n = 69`
- 买在更高价侧的比例约 `76.8%`
- `hit30=yes` 样本平均价差更窄
- 高价侧买入更偏 `first5 winner-net`

新数据支持条件：

- 在更高精度 book 上，关系方向仍保持：
  - 高价侧 / 宽 gap -> 更强 winner-quality
  - 窄 gap -> 更强 quick completion

新数据推翻条件：

- 高价侧和价差变量在 richer book 中与两个目标都无稳定关系
- 或方向全面反转

若被推翻，意味着：

- 当前 microstructure context 只是一层伪相关
- 需要回到更上层的时机/节奏解释

---

## 5. 新数据到来后的建议裁决顺序

固定顺序如下：

1. 先过 `数据可用门槛`
2. 先裁 `H1/H2/H3`
   - 因为这三条决定 `winner-bias` 是否仍是主叙事
3. 再裁 `H4/H5/H6/H7`
   - 因为这四条决定 `open gate family` 的真正结构
4. 最后裁 `H8`
   - 因为这条最依赖新 L1/L2 的质量

这样做的原因是：

- 先确定大的世界观
- 再决定 family
- 最后才做微观结构归因

---

## 6. 当前最重要的“不能偷跑到实现”的红线

在以下任一问题没有被新数据裁决前，都不应该把对应结论直接写进 live 策略：

- `winner-bias` 是否稳定而非窗口错觉
- `Hybrid Frontier` 是否真是主线
- `Winner-Quality` 与 `Completion-First` 的张力是否真实存在
- 高价侧 / 价差关系是否只是 provisional 伪相关

换句话说：

当前所有这些结论，最多只配进入：

- shadow gate
- 研究解释层
- replay 裁决层

还不配直接进入：

- enforce 行为规则
- 真实开仓择边逻辑

---

## 7. 这份矩阵真正要保护什么

它真正要保护的，不是“让某个假设赢”，而是：

- 防止我们把 `winner-bias` 过早写成方向信号
- 防止我们把 `completion` 误当成唯一目标
- 防止我们因为看见少量高光样本，就把某个 family 升格为主线

只要这份矩阵被严格执行，两天后新数据一到，我们就能更快地知道：

- 什么是真的
- 什么只是暂时看起来像真的
- 什么应当被彻底丢弃
