# `xuan` 研究栈 `post-5c` 口径统一清单

生成时间：`2026-04-28`

---

## 1. 为什么需要这份清单

`docs/research/xuan/XUANXUAN008_STRATEGY_V2_ZH.md` 的 `§0.5 修正 5c` 已经把主叙事改写成：

- `MERGE 前 observed winner-bias`
- `loser-only residual = settlement + redeem 后稳态`

但当前问题不是 `V2` 某一段没更新，而是 **整套支撑文档中仍然分布着旧 `5b` 口径**。如果不先统一这些研究资产，后续讨论很容易出现：

- 文档 A 用 `5c`
- 文档 B 仍把 xuan 当作 `loser-bias / 无方向 alpha`
- 文档 C 又继续把 `538 loser / 0 winner` 当作 neutral-maker 证明

结果是：

- 研究结论互相打架
- `PGT` 实现会拿到互相矛盾的上游约束

这份清单的目标不是立即重写所有文档，而是先明确：

1. 哪些冲突是 **硬冲突**
2. 哪些只是 **表述落后**
3. 哪些结论在 `5c` 后仍然成立

---

## 2. `post-5c` canonical 口径

后续所有文档如引用 xuan，默认应服从以下口径：

### 2.1 已成立

- `MERGE 前 winner-bias 在当前长窗口中已被观察到`
- `538 loser / 0 winner residual` 只能说明结算后稳态
- `278s MERGE pulse` 至少兼具：
  - `capital recycling`
  - `winner-side overweight realization / flattening`

### 2.2 未成立

- `xuan = loser-bias neutral maker`
- `loser-only snapshot => 无方向 alpha`
- `5b 仍是当前工作性结论`

### 2.3 仍未裁决

- `winner-bias` 是否来自主动择边
- `winner-bias` 是否主要来自被动 fill geometry
- `winner-bias` 与 `30s completion` 是否同源
- `winner-bias` 是否应进入 live 策略规则

---

## 3. 硬冲突文档

这些文档中的相关段落已经与 `5c` 正面冲突，后续如作为实现依据，必须先修。

### 3.1 `XUANXUAN008_STRATEGY_V2_ZH.md`

当前仍残留旧 `5b` 口径的关键位置：

- `§2.2`
  - 仍写 `没有方向性 alpha`
  - 仍写 `95.5% loser-heavier`
  - 仍给 `A级（升回，§0.5 修正 5b）`
- `§0.6 修正 8`
  - 仍把 xuan 写成 `pair-MERGE surplus + barely cover residual cost`
  - 仍把 `loser-heavy residual` 当成 xuan 的主要解释
- `§10`
  - `Winner residual 倾向` 等级表仍停留在 `5b/待裁决` 中间态
- `§11`
  - 仍把 `第 5 条已完成 = loser-bias`
  - 仍把 `第 7 条` 写成“继续补全 full sample 看是否仍 loser-bias”

**裁决**：`V2` 正文必须作为第一优先级统一对象。

### 3.2 `POLYMARKET_BILATERAL_BUY_ARCHETYPES_ZH.md`

这是当前最危险的“旧口径扩音器”。

问题不在于它引用了 `V2`，而在于它把旧结论升成了更上层框架：

- `§2.4`
  - 仍写 `xuan MERGE 前 loser-bias`
  - 仍试图把 xuan 与 late_grammar 解释成“不同时点观测下的同一 loser-bias 路径”
- `§3.1`
  - 仍写 `xuan 实证 MERGE 前 loser-bias，「无方向 alpha」假设成立`
- 文中表格 / 证据等级 / 待办
  - 仍把 `44/497` 的 `5b` 当成 active evidence

**裁决**：这份文档在 `post-5c` 后不能继续作为 PGT 研究框架的主上位表，除非先回写。

---

## 4. 软冲突文档

这些文档的主要结论仍成立，但有局部表述需要换口径。

### 4.1 `CANDID_CLOSURE_0x93c22116_STRATEGY_DEEP_DIVE_ZH.md`

这份文档整体并没有被 `5c` 推翻，反而强化了：

- `loser-only residual` 是一种 **settlement-state residual pattern**
- 它既可以出现在 xuan，也可以出现在注册 maker / 长 tenor / post-close MERGE 路线

但文中个别段落仍写：

- `机理（与 V2 §0.5 修正 5b 同）`

这类表述在 `post-5c` 后应改成：

- `机理与 V2 loser-only settlement-state 解释一致`

而不应继续直接绑定 `5b`。

### 4.2 `MAYBE_5_QUICK_SURVEY_ZH.md`

总体结论依然成立：

- 没有新 archetype
- 研究该从横向扩张转向纵向深化

但下一步建议仍写着：

- `验证 §0.5 修正 5b 的 95% loser-bias 是否被 selection bias 影响`

这条在 `post-5c` 后已经完成，而且答案是：

- `是，且已被 5c 反向推翻`

因此这份文档只需要更新路线图，不需要重写主体。

### 4.3 `LATE_GRAMMAR_...`

`late_grammar` 自身结论仍然成立：

- 它是 `winner-bias` 微利 / 无 MERGE / margin-thin / 多 venue passive maker

需要调整的是对 xuan 的映射关系：

- 过去它被拿来支持 `xuan 也许 MERGE 前 loser-bias`
- 现在它更适合作为：
  - `被动 maker 几何可能自然形成 winner-bias`
  - `xuan 的 278s MERGE 可能是在兑现而不是预防这种偏差`

它仍有参考价值，但方向要改。

---

## 5. 仍然成立、无需因 `5c` 回滚的上层结论

以下几类结论，在 `5c` 之后依然成立。

### 5.1 Archetype 谱系“封盘”仍成立

`MAYBE_5_QUICK_SURVEY` 的核心判断仍有效：

- 当前已知 archetype 足够覆盖主要双边 BUY 玩家
- 研究重心应转向纵向深化，而不是继续找新 archetype

### 5.2 注册 maker 路线与 xuan 路线分离，仍成立

来自：

- `gabagool22`
- `silent_d189`
- `candid_closure`

的共同约束仍然有效：

- registered maker 的主要 archetype，并不长得像 xuan
- 如果目标是逼近 xuan，就不能把 registered-maker 的行为模式硬混进来

### 5.3 `loser-only residual` 仍有研究价值，但角色变了

它仍然可以用来做：

- `settlement-state accounting`
- `REDEEM / MERGE 后稳态解释`
- `不同 archetype 的尾部库存形态对比`

但不再适合做：

- `pre-MERGE neutrality proof`

---

## 6. `post-5c` 后应优先修订的顺序

### P0

- `XUANXUAN008_STRATEGY_V2_ZH.md`

原因：

- 它是主参考文档
- 冲突最集中
- 也是其他文档引用源

### P1

- `POLYMARKET_BILATERAL_BUY_ARCHETYPES_ZH.md`

原因：

- 它是研究框架总表
- 当前会把旧 `5b` 误升级为全局框架

### P2

- `MAYBE_5_QUICK_SURVEY_ZH.md`
- `CANDID_CLOSURE_...`
- `LATE_GRAMMAR_...`

原因：

- 主要是口径替换与引用更新
- 不是主框架阻塞项

---

## 7. 对后续研究工作的含义

`post-5c` reconciliation 的意义，不是把所有文档都写得一致，而是防止我们在下一步研究中犯两个错误：

### 7.1 错误一：继续把 `5b` 当作还活着的待选答案

它不是待选答案，而是已被更强样本推翻的历史误判。

### 7.2 错误二：因为 `5c` 成立，就直接跳到实现

`5c` 证明的是：

- `winner-bias exists`

还没有证明：

- `winner-bias source`
- `winner-bias tradability`
- `winner-bias should be coded`

所以 `post-5c` 之后的正确顺序是：

1. 先统一研究文档口径
2. 再拆 `winner-bias` 的来源
3. 最后才讨论实现层吸收多少

---

## 8. 最终裁决

当前这套文档群对 `xuan` 的认知已经发生实质性换轨，但还没有完成一次系统的 `post-5c` 清账。

因此现在最重要的不是继续扩展策略设计，而是先把以下一句话变成整个研究栈的统一前提：

> `xuan` 当前最稳的工作性画像，不再是 `loser-bias / neutral pair-merger`，而是 `MERGE 前 observed winner-overweight，但来源未裁决`。

只要这句话还没有在所有上层研究文档里生效，任何实现讨论都仍然带着旧世界观的污染。
