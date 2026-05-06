# Xuan 独有不变量矩阵

生成时间：`2026-04-27`

本文的目标不是再写一份 trader 画像，而是回答一个更硬的问题：

> 在 `late_grammar / quietb27 / gabagool22 / silent_d189` 都已知存在的前提下，`xuan` 到底有哪些东西是**必须逼近**的，哪些只是“Polymarket 双边 BUY 玩家”的共性，哪些是我们一旦出现就说明走偏了？

## 1. 控制组结论

当前控制组：

- [xuan](/Users/hot/web3Scientist/pm_as_ofi/docs/XUANXUAN008_STRATEGY_V2_ZH.md)
- [late_grammar](/Users/hot/web3Scientist/pm_as_ofi/docs/LATE_GRAMMAR_0x7347d3_STRATEGY_DEEP_DIVE_ZH.md)
- [quietb27](/Users/hot/web3Scientist/pm_as_ofi/docs/QUIETB27_0xb27bc_STRATEGY_DEEP_DIVE_ZH.md)
- [gabagool22](/Users/hot/web3Scientist/pm_as_ofi/docs/GABAGOOL22_GROWN_CANTALOUPE_STRATEGY_DEEP_DIVE_ZH.md)
- [silent_d189](/Users/hot/web3Scientist/pm_as_ofi/docs/SILENT_D189_0xd189664c_STRATEGY_DEEP_DIVE_ZH.md)
- 以及已有总览：[POLYMARKET_BILATERAL_BUY_ARCHETYPES_ZH.md](/Users/hot/web3Scientist/pm_as_ofi/docs/POLYMARKET_BILATERAL_BUY_ARCHETYPES_ZH.md)

这几个控制组足以说明：

- `BTC 5m + BUY only` 不是 xuan 独有
- `MERGE / REDEEM` 不是 xuan 独有
- `maker 几何` 也不是 xuan 独有

真正稀缺的，是这些特征的**组合**。

## 2. 必须逼近的 Xuan 不变量

### 2.1 单 venue `BTC 5m` 专精

这不是表层偏好，而是 archetype 选择。

- `xuan`：单 venue，几乎全在 `BTC 5m`
- `late_grammar / gabagool22`：多 venue 平铺
- `quietb27 / silent_d189`：虽也重仓 `BTC 5m`，但走的是 directional selector 路线

结论：

- 我们当前主线继续锁定 `BTC 5m` 是对的
- 在 `completion-first` 还没打透前，不应该把多 venue 扩展误判为“更接近 xuan”

### 2.2 高 round coverage

这是 `xuan` 最容易被误读的一点。

- `xuan`：高覆盖，接近 every-round maker
- `quietb27 / silent_d189`：强 selective，覆盖率低

所以：

- `xuan` 的 edge 不是“只挑最确定的 round”
- 如果我们后续为了提高 hit rate 而把参与率降得很低，那更像 quietb27 / silent_d189，不像 xuan

### 2.3 低 directional share

`xuan` 的核心不是押方向，而是尽快把 first leg 变成可回收的 pair-covered inventory。

对照组里：

- `quietb27`：大部分 round 单方向
- `silent_d189`：near-resolution 单边低风险 ITM sniper
- `gabagool22`：部分 directional + 部分 paired
- `late_grammar`：venue-dependent，5m 上也会失真

所以：

- 看到 high directional skew，不应兴奋，而应警惕我们在滑向别的 archetype
- `same_side_add` 与 `pure directional share` 应继续被当作负向指标

### 2.4 `in-round completion` 比 `post-close cleanup` 更重要

这是一条真正的主线不变量。

- `xuan`：强项在 round 内快速形成 opposite completion
- `late_grammar`：更多靠慢收敛
- `gabagool22`：大量 post-close batch merge
- `silent_d189`：根本不是 completion-first

所以：

- 我们后续任何 improvement，必须优先提升 `30s completion` 和 `60s clean close`
- 不能靠“事后 merge/redeem 很漂亮”掩盖 round 内配不上

### 2.5 clip 必须保持状态感知

`gabagool22` 证明固定 10 股 clip 也能跑通 maker 路线，但那不是 xuan。

对 xuan 更像真的不变量是：

- clip 不是常数
- clip 受状态质量影响
- clip 服务的是 completion-first，而不是 throughput-first

所以：

- fixed-clip 可以作为对照试验
- 但不能升格成主假设

## 3. 只属于“大类玩家”的共性，不应误判为 xuan 独有

这些东西在多类玩家身上都出现过：

- `BUY only`
- `MERGE / REDEEM` 存在
- `BTC 5m` 出现频繁
- 看起来“好像是 maker”

它们只能说明“属于 Polymarket crypto round 双边 BUY 大类”，不能说明“接近 xuan”。

## 4. 明确的 Anti-target

### 4.1 低 coverage + 高 directional

这更像：

- `quietb27`
- `silent_d189`

如果我们后面看到：

- round 覆盖率明显下降
- directional share 升高
- 大单单边押注增多

那不是逼近 xuan，而是滑向 `round selector / directional bettor`。

### 4.2 多 venue 平铺 + 慢 merge

这更像：

- `late_grammar`

如果我们后面靠“跨 venue 分散”把报表做漂亮，那是另外一类策略，不是 xuan 复制。

### 4.3 固定小 clip + post-close batch merge

这更像：

- `gabagool22`

它有研究价值，但不应作为“接近 xuan”的证据。

## 5. 对我们最重要的翻译

后续所有优化，只按下面三档归类：

### 5.1 `must-match`

- `BTC 5m` 单 venue 主战场
- 高 round coverage
- 低 directional share
- `30s completion` 优先
- 状态感知 clip

### 5.2 `nice-to-have`

- maker-leaning mixed execution
- in-round capital recycling
- 更低 same-side drift

### 5.3 `anti-target`

- selective directional 低覆盖打法
- 多 venue 慢收敛 spread 囤仓
- 固定 clip + post-close batch merge 主导

## 6. 结论

`xuan` 真正稀缺的不是某个单点特征，而是一组同时成立的不变量：

1. 单 venue `BTC 5m`
2. 高 coverage
3. completion-first
4. 低 directional
5. 状态感知 clip

这 5 条同时成立，才值得叫“逼近 xuan”。只满足其中一两条，都可能只是撞上了别的 archetype。
