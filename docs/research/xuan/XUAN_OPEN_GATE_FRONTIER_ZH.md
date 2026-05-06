# `xuan` 开仓状态前沿：winner-overweight vs 30s completion

生成时间：`2026-04-28`

---

## 1. 为什么要单独看这件事

前面的研究已经得到两个看似冲突的事实：

1. `xuan` 的早期路径会形成 `winner-overweight`
2. 第一腿如果一开始就在 eventual winner side，`opposite fill` 反而不一定更快

这意味着：

- `winner-bias`
- `30s completion`

并不是同一个单调目标。

所以更合理的问题不是：

- “哪种状态最好”

而是：

- “哪些开仓状态更偏向 winner-overweight”
- “哪些开仓状态更偏向快速 completion”
- “哪些状态能兼顾二者”

这份笔记把这些状态看成一个 **frontier**。

---

## 2. 口径

只用当前窗口内数据：

- `data/xuan/trades_long.json`
- `data/xuan/slug_winner_map_full.json`

对每个 slug 提取第一笔特征：

- `first offset`：第一笔距 round open 的秒数
- `first price`

并计算三个结果指标：

- `first winner ratio`
- `first5 winner-net ratio`
- `opposite fill <= 30s ratio`

这里的 `first5 winner-net ratio` 代表：

- 前五笔累计后，winner side size 是否大于 loser side size

它可以近似看成：

- 早期路径是否更容易形成后续的 winner-overweight

---

## 3. 一维结论先回顾

### 3.1 按开仓时机看

| first offset | n | first winner ratio | first5 winner-net ratio | opposite <=30s |
|------|---:|---:|---:|---:|
| `0-30s` | 306 | 58.5% | 69.3% | 61.4% |
| `30-60s` | 55 | 65.5% | 81.8% | 54.5% |
| `60-120s` | 42 | 59.5% | 78.6% | 54.8% |
| `120s+` | 22 | 样本偏少 | 样本偏少 | 样本偏少 |

**读法**：

- `30-60s` 比 `0-30s` 更偏 winner-overweight
- 但 `0-30s` 的短时 opposite fill 更快

这已经显示出：

- `winner-bias` 与 `completion` 有 tradeoff

### 3.2 按第一笔价格看

| first price | n | first winner ratio | first5 winner-net ratio | opposite <=30s |
|------|---:|---:|---:|---:|
| `<0.5` | 97 | 偏低 | 中高 | **偏高** |
| `0.5-0.6` | 205 | 中高 | 中高 | 偏低 |
| `0.6-0.7` | 101 | **更高** | **更高** | 中高 |
| `>=0.7` | 22 | 高 | 高 | 高（但样本少） |

**读法**：

- 买在更“贵”的第一腿，更容易押中 eventual winner
- 但更“贵”不自动意味着更快 opposite fill

---

## 4. 二维前沿：时机 × 价格

下面只看样本相对可用的桶。

### 4.1 `0-30s | <0.5`

- `n = 84`
- `first winner ratio = 50.0%`
- `first5 winner-net ratio = 69.0%`
- `opposite <=30s = 73.8%`

**画像**：

- completion-friendly
- winner selection 较弱

这更像：

- “快速补腿”状态
- 不像“方向质量高”的状态

### 4.2 `0-30s | 0.5-0.6`

- `n = 159`
- `first winner ratio = 60.4%`
- `first5 winner-net ratio = 68.6%`
- `opposite <=30s = 54.1%`

**画像**：

- 样本最多
- 三项都中等

这更像：

- xuan 的常态基础状态

### 4.3 `0-30s | 0.6-0.7`

- `n = 62`
- `first winner ratio = 66.1%`
- `first5 winner-net ratio = 72.6%`
- `opposite <=30s = 62.9%`

**画像**：

- 样本量足够
- winner-side 质量更高
- opposite fill 也不差

这是当前最像：

- **兼顾 directional-quality 与 completion-quality 的可疑前沿状态**

### 4.4 `30-60s | 0.5-0.6`

- `n = 25`
- `first winner ratio = 64.0%`
- `first5 winner-net ratio = 88.0%`
- `opposite <=30s = 44.0%`

**画像**：

- 很强的 early winner-overweight
- 但短时 opposite fill 明显更慢

这更像：

- “偏 winner quality”的状态
- 不像“偏 completion quality”的状态

### 4.5 `30-60s | 0.6-0.7`

- `n = 18`
- `first winner ratio = 77.8%`
- `first5 winner-net ratio = 77.8%`
- `opposite <=30s = 66.7%`

**画像**：

- winner quality 很高
- completion 也不差
- 样本仍偏小，但已经非常值得重点盯

如果未来更长窗口里它仍成立，这会是非常强的：

- `open gate candidate`

### 4.6 `60-120s | >=0.7`

- `n = 9`
- `first winner ratio = 77.8%`
- `first5 winner-net ratio = 88.9%`
- `opposite <=30s = 77.8%`

**画像**：

- 三项都很强
- 但样本太少

当前只能算：

- 高价值待验证状态

---

## 5. 当前最重要的观察

### 5.1 不是所有“更 winner”的状态都更 completion-friendly

这点在：

- `30-60s | 0.5-0.6`

最明显。

它的 early winner-overweight 很强，但 `opposite <=30s` 明显偏弱。

这说明：

- xuan 的 `winner-quality`
- 与 `hedge immediacy`

至少部分分离。

### 5.2 但也存在兼顾两者的前沿状态

当前最像前沿的状态是：

- `0-30s | 0.6-0.7`
- `30-60s | 0.6-0.7`

它们同时具备：

- 较高的 first winner ratio
- 较高的 first5 winner-net ratio
- 不差的 `<=30s` opposite fill

这意味着 xuan 可能并不是在单目标优化，而是在挑选：

- **能够兼顾 winner-overweight 与 completion 的稀缺状态**

### 5.3 `<0.5` 价位更像“completion 状态”，不像“winner 质量状态”

`0-30s | <0.5` 很有代表性：

- first winner ratio 只有 `50%`
- 但 opposite fill 很快

这更像：

- 价差/对侧 liquidity 容易补
- 但并没有明显方向质量

如果把这种状态机械学进 `PGT`，可能会得到：

- completion 指标变好
- 但不一定更像 xuan

---

## 6. 当前对 xuan open gate 的最新理解

到这一轮为止，最稳的说法已经不再是：

- “xuan 会开在某个固定 pair target”

也不再只是：

- “xuan 可能有一点 winner-bias”

而更像：

> xuan 的第一腿可能是在一个 **二维状态空间** 里挑点：既看 `price location / stronger side leaning`，也看 `对侧在短时间内是否仍有较高 fillability`。它真正要找的，可能不是最 directional 的点，也不是最好补腿的点，而是两者兼顾的局部前沿。

---

## 7. 对后续研究的直接含义

### 7.1 下一步最值得验证的状态

优先级从高到低：

1. `0-30s | 0.6-0.7`
2. `30-60s | 0.6-0.7`
3. `60-120s | >=0.7`（样本不足，作为待验证）

### 7.2 需要补的数据

要确认这些状态是不是“真前沿”，下一步最需要的是：

- L1 book spread / size
- 该时刻对侧 recent trade cadence
- 我方 own fill truth

因为当前公开 trades 只能告诉我们：

- 它买了什么

还不能直接告诉我们：

- 那一刻为什么 opposite side 更容易在 30s 内也成交

### 7.3 对 PGT 的当前约束

现在仍然不能把这些桶直接写成 live 规则。

但已经可以把它们写成：

- shadow monitor buckets
- gap report 重点关注状态

也就是：

- 我方 first leg 是否出现在这些“xuan-like frontier states”
- 如果出现了，为什么 opposite fill 仍然更差

---

## 8. 最终裁决

当前最稳的 open-gate 研究结论是：

> xuan 并不是在“越 directional 越好”与“越快 completion 越好”之间只选一边。它更像是在找一类稀缺的开仓状态：**第一腿已经带有一定 winner 倾向，但对侧在短时间内仍有足够 fillability**。这类状态当前最可疑的前沿，是 `0-30s | 0.6-0.7` 与 `30-60s | 0.6-0.7`。

这离“可直接实现”还差一步，但比单纯说“xuan 有 winner-bias”已经更接近真正的 open gate 了。
