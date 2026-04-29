# `xuan` frontier state 的微观结构补充笔记（provisional）

生成时间：`2026-04-28`

---

## 1. 目标

在 [XUAN_OPEN_GATE_FRONTIER_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/research/xuan/XUAN_OPEN_GATE_FRONTIER_ZH.md) 中，我们已经从公开 trades 序列得到一个研究性判断：

- xuan 可能在寻找一类能兼顾
  - `winner-overweight`
  - `short-horizon opposite fillability`

的 `open gate frontier state`。

这份补充笔记尝试把这个结论推进到更接近盘口层：

- xuan 的首腿当时更常买在高价侧还是低价侧？
- 双边价格差与 `winner-overweight` / `30s completion` 的关系是什么？

但要先说明结论边界：

- 本文只使用 `pm_as_ofi` 当前仓库里的 `replay_recorder`
- 这里只能给出 **microstructure context**
- 不能给出 **execution truth**

---

## 2. 数据与边界

使用的数据：

- `data/xuan/trades_long.json`
- `data/xuan/slug_winner_map_full.json`
- `data/replay_recorder/2026-04-25/crypto_5m.sqlite`
- `data/replay_recorder/2026-04-26/crypto_5m.sqlite`

### 2.1 覆盖

- `xuan` 首腿 slug 总数：`425`
- 能对上本地 `replay_recorder` 首腿附近 book 的 slug：`69`

### 2.2 好消息

`xuan` 公开 trades 里的 `asset` 字段，可以直接对上 `md_book_l1.side`，所以：

- 能知道他买的是 book 里的哪一侧

### 2.3 坏消息

当前本地 `replay_recorder` 的 book 不能拿来做精确 maker/taker 判定：

- 只有 `69` 个 slug 能稳定对上 book
- 在这些样本里，首腿成交价落在最近快照 `bid-ask` 区间内的只占少数

具体地：

- `overlap ≈ 79`
- `within spread = 16`
- `below bid = 48`
- `above ask = 15`

而最近 book snapshot 的时间偏差中位数仍约：

- `46ms ~ 118ms`

这说明当前公开 trade 秒级时间戳与本地毫秒级 book 快照之间存在显著对齐误差，或者两者观察口径不同。

**因此：本文所有结果只能解释“当时的 book 形状”，不能解释“xuan 的真实执行方式”。**

---

## 3. 当前能提取的微观特征

在可对齐样本上，我提取了：

- `sel_is_high_ask`
  - xuan 首腿买的是否是当时更高价的一侧
- `price_gap`
  - 两侧 ask 差值
- `pair_ask_sum`
  - 双边 ask 和
- `hit30`
  - 公开 trades 中首腿后 30s 内是否出现 opposite fill
- `first5_win_net`
  - 前 5 笔后 winner-side 是否净占优

---

## 4. 关键结果

### 4.1 xuan 首腿大多数时候买在“更高价的一侧”

在 `69` 个可对齐样本上：

- `sel_is_high_ask ratio = 76.8%`

这说明：

- xuan 的首腿通常不是去买更便宜的一侧
- 它更常去买当前 book 上更“强”的一侧

这与前面公开 trade 里看到的：

- 更高 first price
- 更高 eventual winner ratio

是一致的。

### 4.2 但“买在高价侧”不等于“更快完成 opposite fill”

按 `hit30` 拆：

#### `hit30 = yes`

- `n = 47`
- `first_is_winner ratio = 53.2%`
- `sel_is_high_ask ratio = 72.3%`
- `avg price_gap = 0.121`

#### `hit30 = no`

- `n = 22`
- `first_is_winner ratio = 68.2%`
- `sel_is_high_ask ratio = 86.4%`
- `avg price_gap = 0.173`

这说明：

- 更偏向高价侧、更大的双边价格差
- 更容易对应 `winner-overweight`
- 但不一定更容易对应 `30s completion`

换句话说：

- `winner-quality` 强
- 不代表 `hedge immediacy` 强

### 4.3 低价侧更 completion-friendly，高价侧更 winner-friendly

按是否买在高价侧拆：

#### 买在高价侧

- `n = 53`
- `first5_win_net ratio = 71.7%`
- `hit30 ratio = 64.2%`
- `avg price_gap = 0.209`

#### 买在低价侧

- `n = 16`
- `first5_win_net ratio = 50.0%`
- `hit30 ratio = 81.3%`
- `avg price_gap = -0.098`

这条关系非常值得重视：

- **高价侧**更像 `winner-overweight` 状态
- **低价侧**更像 `completion-friendly` 状态

这与公开 trade 层面的结论高度一致：

- `winner-bias`
- 与 `completion edge`

并非完全同源。

### 4.4 pair ask 和整体接近 1.01，说明不是极端深折扣状态

全体可对齐样本：

- `avg pair_ask_sum ≈ 1.011`

这至少说明：

- xuan 的首腿前沿状态，不像是“总在非常深的 pair discount”上开第一腿
- 更像是在一个不算特别便宜、但可能更有质量/更有流动性的状态下开仓

这也和之前的研究一致：

- xuan 不是固定 pair_target 玩家
- 它更像是在做状态筛选

---

## 5. 与前面 frontier 研究如何拼起来

把这份微观笔记和 [XUAN_OPEN_GATE_FRONTIER_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/research/xuan/XUAN_OPEN_GATE_FRONTIER_ZH.md) 合起来，当前可以得到一个更清晰的工作性画像：

### 5.1 winner-overweight 方向

更像：

- 买在当前更高价的一侧
- 双边价格差更大
- 第一腿价格更高

### 5.2 quick-completion 方向

更像：

- 双边价格差更窄
- 不必总买在更高价的一侧
- 对侧更容易在 30s 内也成交

### 5.3 xuan 真正可能在做的事

不是：

- 无脑追更强的一侧

也不是：

- 无脑挑最容易补腿的一侧

而更像：

- 在两者之间寻找少量兼顾状态

也就是：

- 既不能太“偏 directional”，否则 opposite fill 慢
- 也不能太“偏 completion”，否则 winner-overweight 不强

---

## 6. 当前最稳的微观结论

在本仓库这批 `provisional` L1 overlap 上，最稳的结论是：

> xuan 的首腿确实更常买在当前更高价的一侧，但真正短时间内容易完成 opposite fill 的状态，反而表现为更窄的双边价格差、稍弱的高价侧倾向。这强化了一个核心判断：`winner-overweight` 与 `completion` 是两个相关但并不相同的目标。

---

## 7. 仍然不能下的结论

### 7.1 不能据此判 maker/taker

当前本地 `replay_recorder` 的时间对齐不够硬，不能拿这批样本做：

- maker/taker A 级判定
- 成交价与盘口价的精确归因

### 7.2 不能把这 69 个样本直接当成完整真相

这是：

- `69 / 425` 首腿 slug 的局部 overlap

只能做：

- 支撑性微观证据

不能做：

- 主结论唯一来源

---

## 8. 对下一步研究的意义

这份笔记最大的价值，不是给出了最终答案，而是明确了下一步高价值数据要回答什么：

1. 更高精度 L1/L2 下，`high-price-side` 与 `hit30` 的 tradeoff 是否仍成立
2. 当 xuan 买在高价侧但仍能很快 completion 时，当时对侧的成交节律/流动性条件是什么
3. frontier state 是否真的集中在：
   - `0-30s | 0.6-0.7`
   - `30-60s | 0.6-0.7`
   这类状态上

这也是为什么：

- `pm_as_ofi` 当前仓库里的 replay 只能做 `provisional microstructure note`
- 真正要把它打磨成更硬的研究结论，仍要依赖 `poly_trans_research` 的高精度公开侧采集

---

## 9. 最终裁决

当前这份微观补充并没有推翻前面的研究结论，反而让轮廓更清晰：

- xuan 的首腿经常偏向当前更强的一侧
- 但快速 opposite fill 更喜欢“没那么偏”的状态
- 所以 xuan 的 open gate 很可能不是单指标，而是在 `winner-overweight` 与 `completion` 之间做 frontier 搜索

这离可直接实现还差一步，但已经比“xuan 有 winner-bias”更接近真正可复刻的开仓资格审查了。
