# `xuan` 开仓范式族谱：在等待新真值前先压缩假设空间

生成时间：`2026-04-28`

---

## 1. 为什么现在要做 `gate family`

到目前为止，我们已经得到一组彼此并不完全一致的观察事实：

- `MERGE` 前存在显著 `winner-bias`
- 第一腿更偏向 eventual winner side，但强度只有中等
- `30s opposite completion` 和 `winner-overweight` 不是同一个单调目标
- 首腿更常买在当前更高价的一侧，但这不一定对应更快的 opposite fill

如果继续把这些事实直接堆在一起，很容易得出错误实现结论：

- 要么误以为 xuan 是 `completion-first`
- 要么误以为 xuan 是 `direction-first`
- 要么误以为它有一个固定的 pair target / 固定公式

更合理的做法，是先把当前证据压成少数几类 **候选开仓范式**，然后让新数据去裁决：

- 哪一类是真核心
- 哪一类只是局部边缘状态
- 哪一类看起来很像 edge，实际只是样本幻觉

---

## 2. 当前统一基准口径

样本：

- `data/xuan/trades_long.json`
- `data/xuan/slug_winner_map_full.json`

当前窗口内可用首腿样本：

- `n = 425`

统一 baseline：

| 指标 | 当前 baseline |
|---|---:|
| `first winner ratio` | `58.8%` |
| `first5 winner-net ratio` | `72.7%` |
| `opposite <=30s ratio` | `60.9%` |

这里的三个指标分别代表：

- `first winner ratio`
  - 第一腿是否更常落在 eventual winner side
- `first5 winner-net ratio`
  - 前五笔后，winner side 是否已净占优
- `opposite <=30s ratio`
  - 首腿后 30 秒内是否出现 opposite fill

当前所有 `gate family` 都只是在这个 baseline 上比较 **lift / tradeoff**。

---

## 3. 当前最像的 5 类 `open gate family`

### 3.1 `Completion-First` 家族

定义：

- `first offset = 0-30s`
- `first price < 0.5`

当前指标：

| 指标 | 数值 | 相对 baseline |
|---|---:|---:|
| 样本数 | `78` | - |
| `first winner ratio` | `47.4%` | `-11.4pct` |
| `first5 winner-net ratio` | `71.8%` | `-0.9pct` |
| `opposite <=30s ratio` | `71.8%` | `+10.9pct` |

当前画像：

- 很像 `completion-quality` 状态
- 不像 `winner-quality` 状态
- 第一腿对 eventual winner 的命中反而偏弱

它当前最像什么：

- 首腿开在更容易补 opposite 的位置
- 并不追求一开始就站在更强的一侧

它当前不支持什么：

- 不支持“xuan 核心就是纯 direction/winner 选择”
- 也不支持“低价腿天然会带来更强的 winner-bias”

新数据到来后，这一类如果要继续成立，应该看到：

- 更窄的双边价差
- 更高的对侧近期成交节奏
- 更高的 `<=30s` completion，但仍低于 hybrid family 的 winner-quality

---

### 3.2 `Baseline-Core` 家族

定义：

- `first offset = 0-30s`
- `first price = 0.5-0.6`

当前指标：

| 指标 | 数值 | 相对 baseline |
|---|---:|---:|
| 样本数 | `164` | - |
| `first winner ratio` | `59.1%` | `+0.3pct` |
| `first5 winner-net ratio` | `69.5%` | `-3.2pct` |
| `opposite <=30s ratio` | `55.5%` | `-5.5pct` |

当前画像：

- 样本最多
- 三项表现都不极端
- 更像“常态流量区”，不像“核心 alpha 区”

它的研究价值主要是：

- 作为参照组
- 防止我们把“最常见状态”误判成“最有 edge 的状态”

当前判断：

- `Baseline-Core` 不是 xuan 的解释答案
- 但很可能是它大量普通轮次经过的默认分布区

---

### 3.3 `Winner-Quality` 家族

定义：

- `first offset = 30-60s`
- `first price = 0.5-0.6`

当前指标：

| 指标 | 数值 | 相对 baseline |
|---|---:|---:|
| 样本数 | `25` | - |
| `first winner ratio` | `64.0%` | `+5.2pct` |
| `first5 winner-net ratio` | `88.0%` | `+15.3pct` |
| `opposite <=30s ratio` | `44.0%` | `-16.9pct` |

当前画像：

- `winner-overweight` 很强
- `30s completion` 明显更慢
- 这是当前最清楚的“偏 winner-quality、但不偏 quick completion”的家族

它意味着什么：

- xuan 的 `winner-bias` 不能被简单等同成 “更快 opposite fill”
- 说明它在某些状态下确实愿意牺牲短时 completion，换更强的 winner-side 库存质量

当前最像的机制：

- 稍晚开仓
- 更偏高质量 price-location
- 不一定在最容易配对的时点进场

---

### 3.4 `Hybrid Frontier` 家族

这是当前最值得盯的主候选，因为它看起来最像同时兼顾：

- `winner-quality`
- `completion-quality`

为了避免把两个不同强度的桶混成一个故事，这里拆成两个变体。

#### 3.4.1 `Hybrid-Early`

定义：

- `first offset = 0-30s`
- `first price = 0.6-0.7`

当前指标：

| 指标 | 数值 | 相对 baseline |
|---|---:|---:|
| 样本数 | `63` | - |
| `first winner ratio` | `65.1%` | `+6.3pct` |
| `first5 winner-net ratio` | `74.6%` | `+1.9pct` |
| `opposite <=30s ratio` | `63.5%` | `+2.6pct` |

当前画像：

- 三项都在 baseline 之上
- 没有像 `Winner-Quality` 那样用 completion 去换 winner-quality

这是当前最稳的 frontier 候选之一。

#### 3.4.2 `Hybrid-Mid`

定义：

- `first offset = 30-60s`
- `first price = 0.6-0.7`

当前指标：

| 指标 | 数值 | 相对 baseline |
|---|---:|---:|
| 样本数 | `17` | - |
| `first winner ratio` | `76.5%` | `+17.6pct` |
| `first5 winner-net ratio` | `82.4%` | `+9.6pct` |
| `opposite <=30s ratio` | `64.7%` | `+3.8pct` |

当前画像：

- 当前所有主候选里最亮眼
- 但样本还太少，不能直接升格为主规则

#### 当前对 `Hybrid Frontier` 的工作性判断

如果 xuan 真有一条最值得复刻的 `open gate` 主线，它当前最像：

- 不是最便宜的一腿
- 也不是最极端的 winner-quality 状态
- 而是 `0.6-0.7` 这一类既有 winner-overweight 倾向、又不明显牺牲 opposite fill 的状态

---

### 3.5 `Late Extreme Probe` 家族

定义：

- `first offset = 60-120s`
- `first price >= 0.7`

当前指标：

| 指标 | 数值 | 相对 baseline |
|---|---:|---:|
| 样本数 | `9` | - |
| `first winner ratio` | `77.8%` | `+19.0pct` |
| `first5 winner-net ratio` | `88.9%` | `+16.2pct` |
| `opposite <=30s ratio` | `77.8%` | `+16.8pct` |

当前画像：

- 看上去几乎全优
- 但样本太小

当前裁决：

- 只能作为 `watchlist`
- 不能作为实现参考

它最大的风险是：

- 新数据一扩容，这类状态很可能直接均值回归

---

## 4. 这些 family 之间的关系

### 4.1 它们不是互斥的“策略”，而是候选开仓状态簇

当前最合理的理解，不是 xuan 只用其中一种 family，而是：

- 在不同市场状态下，进入不同的开仓簇

也就是说：

- `Completion-First` 更像 completion 端的极值
- `Winner-Quality` 更像 winner-overweight 端的极值
- `Hybrid Frontier` 更像两者折中的主战场

### 4.2 当前最值得怀疑的不是“哪一类存在”，而是“哪一类是核心”

当前最重要的未解问题，不是 family 有没有，而是：

- xuan 的大部分高质量轮次，主要来自哪一类 family
- 这些 family 的选择，是由什么微观状态决定的

### 4.3 当前最像伪目标的 family

有两类状态目前最容易误导实现：

#### `Baseline-Core`

- 因为样本最多，看起来“像常态”
- 但三项都不够亮，不能解释 xuan 的 edge

#### `Late Extreme Probe`

- 因为三项都太强，看起来“像最终答案”
- 但样本过小，极容易是窗口幻觉

---

## 5. 当前最稳的工作性主张

当前最稳的主张不是：

- “xuan 是 completion-first”
- “xuan 是 winner-first”

而是：

> xuan 更像在少数 `gate family` 之间做状态筛选，其中真正最值得复刻的主线候选，不是两端的极值家族，而是能同时让 `winner-overweight` 与 `30s completion` 保持高于 baseline 的 `Hybrid Frontier` 家族。

这也是为什么当前最值得重点追的新真值，不是简单的更多 trades，而是：

- 更准的首腿时点盘口
- 对侧近期成交节奏
- 双边价差与 size
- 首腿后 30 秒内的更完整公开成交路径

---

## 6. 这份族谱的用途

这份文档的作用不是替代策略设计，而是：

1. 先把假设空间从“无数可能的状态”压成少数几类 family
2. 让两天后新数据到来时，我们不再继续泛泛描述
3. 直接裁决：
   - 哪一类是 xuan 的核心 family
   - 哪一类只是边缘状态
   - 哪一类应该被排除出实现目标

---

## 7. 当前临时排序

按“最值得继续验证”的优先级，当前排序是：

1. `Hybrid Frontier`
2. `Winner-Quality`
3. `Completion-First`
4. `Baseline-Core`
5. `Late Extreme Probe`

读法不是“第 1 名最好”，而是：

- 第 1 名最像主线
- 第 2/3 名最像边界极值
- 第 4 名更像参照组
- 第 5 名更像高风险观察样本

