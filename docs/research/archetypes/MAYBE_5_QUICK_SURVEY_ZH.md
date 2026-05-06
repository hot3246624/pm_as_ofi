# 5 个 MAYBE 候选钱包快速 survey

生成时间：`2026-04-27`
研究对象：用户提供的 10 个候选钱包中标记为 "MAYBE" 的 5 个。这份 quick-survey 用于一次性给出 archetype 归类与是否值得 deep dive 的判断，不再为每个钱包出独立深度文档。

证据等级：`A`/`B`/`C` 同前。数据来源：500-trade quick probe + positions 全量 + activity 500 sample。

---

## 0. TL;DR — 5 个钱包归类

| tag | wallet | 已知最相似 archetype | 关键签名 | 建议 |
|-----|--------|------|---------|------|
| **c01 Idolized-Scallops** | `0xe1d6...` | **C' (late_grammar)** | 10 venue + 86% 双侧 + 0 MERGE | 继 late_grammar 路线，**不 deep dive** |
| **c02** `0xeebde7a0...` | (无 pseudonym) | **xuan + late_grammar 混合** | btc/eth 5m + 16 loser-only + 0 MERGE | **新发现：5m 双侧 BUY 但不 MERGE**，值得 mini deep dive |
| **c05** `0xf6963d4c...` | (无 pseudonym) | **D++ (quietb27 极端版)** | **500/500 single-side ALL LOSER** + imb=1.0 | 极端 directional，与 quietb27 同类但更大资金 |
| **c08** `0x45bc74ef...` | (无 pseudonym) | **F variant (silent_d189)** | btc 5m + 4% SELL + 1 REBATE + 484 loser | **类 silent_d189 但有 SELL**，不是纯 sniper |
| **c10 Twin-Driving-Shower** | `0x3a847382...` | **C' (late_grammar)** | 4 venue 15m + 100% 双侧 + 0 MERGE | 极小样本，跳过 |

**核心发现**：5 个 MAYBE 候选**全部是 5 已知 archetype 的 variant，无新 archetype**。这巩固了我们目前的 6-archetype 分类体系（A/B/C/C'/D/E/E'/F）。

---

## 1. 详细数据

### 1.1 c01 — Idolized-Scallops `0xe1d6b515...`

```text
positions: 39 / 21 conditions   (2-side: 18, 1-side: 3 — 86% 双侧持仓率)
trades sample: 500   SELL: 0%
venues (10): btc-15m:193, eth-15m:71, btc-daily:58, xrp-15m:52, eth-daily:51, ...
imb_median: 0.370 (mid)
pure_directional: 7/27 rounds
clip p50=33  max=2042
activity: MERGE=0  REDEEM=3  REBATE=0
```

**归类：C' (late_grammar) variant**——multi-venue 15m+daily passive maker，几乎不 MERGE，靠 winner-side redeem 收水。86% 双侧持仓率与 late_grammar 89% 接近。

vs late_grammar 的差异：
- venue 数 10 vs late_grammar 6
- 包含 4h tenor，更靠近 candid_closure 的 12-venue 长 tenor 风格
- 但**未注册 maker**（0 REBATE）

可视为「late_grammar + candid_closure 的 mid-point」——都是 multi-venue 但 c01 不主动 MERGE。

### 1.2 c02 — `0xeebde7a0...` (无 pseudonym)

```text
positions: 28 / 22 conditions   (2-side: 6, 1-side: 16 — 73% 单侧, 全 loser)
trades sample: 500   SELL: 0%
venues (5): btc-5m:203, eth-5m:184, btc-15m:62, btc-daily:50, eth-15m:1
imb_median: 0.131 (低)
pure_directional: 1/14 rounds
clip p50=18  max=2445
activity: MERGE=0  REDEEM=8  REBATE=0
```

**归类：xuan/late_grammar 混合 — 5m 双侧 BUY 但 0 MERGE**。

这是个有意思的 case：
- 主要做 5m（btc-5m + eth-5m 共 387/500 = 77%）—— 与 xuan 同 short tenor
- 73% 单侧残差全 loser-only —— 与 xuan 同模式
- imb_median=0.13（低）—— 与 xuan 平衡度近
- **但 0 MERGE**—— 与 late_grammar 同
- max clip 2445 —— 中等

**机理推测**：该 wallet 像 xuan 一样做 5m 双边 BUY，但**让 winner 自然 redeem 而非主动 MERGE**——相当于 xuan 模式的「懒人 maker」。

vs xuan：xuan 是 in-round 紧密 MERGE pulse，c02 是「躺着等 redeem」。但因为 5m round 短，c02 这种风格收水比 xuan 慢得多。

vs gabagool22/candid_closure：它们是注册 maker，c02 不是。

**值得 mini deep dive 吗？** 对 V2 修订 marginal value——它揭示了「5m 但不 MERGE」是可能的，但与 xuan 主路径正交，不影响我们 PGT 蓝图。**跳过 deep dive，记入 V2 archetype 表作为 C-variant**。

### 1.3 c05 — `0xf6963d4c...` (无 pseudonym)

```text
positions: 500 / 500 conditions   (2-side: 0, 1-side: 500 — 100% 单侧, ALL LOSER)
trades sample: 500   SELL: 0%
venues (3): btc-5m:251, sol-15m:150, xrp-15m:99 — sample 较窄
imb_median: 1.000 (极端 directional)
pure_directional: 36/59 rounds
clip p50=14  max=191
activity: MERGE=0  REDEEM=50  REBATE=0
```

**归类：D++ (quietb27 极端版)**——超极端 directional：

- **500 conditions, 全部 single-side, 全部 loser**——意味着此 wallet **每个 round 都全单方向押注，且方向都错了**
- imb_median=1.0 —— 100% 的 round 完全单方向
- 36/59 = 61% 的 sample round 单方向交易
- clip p50=14（小，比 silent_d189 199 / quietb27 60 都小）
- max=191（远比 quietb27 max 9392 / silent_d189 max 115k 小）

这是个 **「小资金 directional bettor」**——全部押注 + 全部押错方向 + 留下 500 个 loser-side 残差等不到 redeem（因为 loser 价值 0）。

可能是新手 / 失败 trader / 测试账号——profile 上无 pseudonym 也支持这点。

**复刻判断：明显不推荐**（500 个失败押注 = 失败策略）。**跳过 deep dive**。

### 1.4 c08 — `0x45bc74ef...` (无 pseudonym)

```text
positions: 500 / 499 conditions   (2-side: 1, 1-side: 488 — 484 loser + 4 winner)
trades sample: 500   SELL: 4.4% (22 SELL)
venues (8): btc-5m:400, btc-15m:61, btc-daily:16, eth-5m:16, ...
imb_median: 1.000
pure_directional: 127/170 rounds (75%)
clip p50=97  max=3469
activity: MERGE=0  REDEEM=30  REBATE=1
```

**归类：F variant (类 silent_d189 但有 SELL)**——大资金 directional + maker rebate，但与 silent_d189 不同：

| 维度 | silent_d189 (F) | c08 |
|------|-----------------|-----|
| Universe | btc-5m + nba | btc-5m 主导 |
| SELL % | 0.6% | **4.4%（高）** |
| MAKER_REBATE | 4 | 1 |
| Single-side residual | 5 winner / 0 loser | **484 loser / 4 winner** |
| Pure directional | 80% | 75% |
| Clip max | 115k | 3469 |

c08 的 484 loser-side residual 与 silent_d189 的 5 winner-side residual **完全反向**——意味着 c08 的方向选择**经常错**（loser-heavy 残差 = 押错方向居多）。

而且 SELL 4.4% 不是 noise——它是**主动平仓行为**（silent_d189 仅 0.6%）。

**机理推测**：c08 是个**注册 maker 但没找到稳定 alpha 的 trader**——挂单被打中后 4.4% 用 SELL 平仓止损，剩下的留成残差。注册 maker 拿 1 次 rebate（$1xx 量级）。

vs silent_d189：silent_d189 是稳定 ITM sniper（5/0 winner-only 残差证明方向几乎不错）；c08 是更激进、更不准的同类。

**复刻判断：不推荐**（方向准确率不够，且与 V1.1 PGT 蓝图正交）。**跳过 deep dive**。

### 1.5 c10 — Twin-Driving-Shower `0x3a847382...`

```text
positions: 8 / 4 conditions   (2-side: 4, 1-side: 0 — 100% 双侧, 但样本极小)
trades sample: 500   SELL: 0%
venues (4): btc-15m:265, eth-15m:142, xrp-15m:50, sol-15m:43
imb_median: 0.189 (低，平衡度好)
pure_directional: 1/20 rounds
clip p50=27  max=638
activity: MERGE=0  REDEEM=8  REBATE=0
```

**归类：C' (late_grammar) variant**——multi-venue 15m, 100% 双侧持仓 (4/4)，0 MERGE，0 REBATE。

但 sample 极小（4 conditions），无法判定长期模式。可能：
- (a) 新账号，刚开始多 venue 双边囤
- (b) 选择性参与，仅 4 round 都双边（少见）

vs late_grammar 的差异：
- late_grammar 跑 5m + 15m + daily, c10 仅 15m
- late_grammar 6 venue, c10 4 venue
- late_grammar 95 conditions, c10 仅 4

**值得 deep dive？不**——样本太小，结构与 late_grammar 高度重合，新增信息少。

---

## 2. 整体洞察

### 2.1 5 个 MAYBE 候选印证 archetype 谱系完整性

5 个候选全部归入已发现的 archetype（C/C'/D/E/E'/F），无新 archetype 出现。这意味着：

- **现有 6+ archetype 分类大概率覆盖了 Polymarket 双边 BUY 玩家的主要类型**
- 进一步研究新钱包的 marginal value 在递减
- 应该把研究重心**从"找新 archetype"转向"深化已有 archetype 的 alpha 来源理解"**

### 2.2 注册 maker 的 single-side residual 模式

注册 maker（有 MAKER_REBATE）的钱包呈现**完全不同**的 residual 模式：
- gabagool22: 0 visible (完全清干净)
- silent_d189: 5 winner / 0 loser
- candid_closure: 281 loser / 0 winner
- c08: 484 loser / 4 winner
- xuan (未注册): 538 loser / 0 winner

**没有简单规律**——注册 maker 可能 winner-heavy 也可能 loser-heavy。猜测原因是 alpha 来源不同：
- silent_d189 (winner-heavy): ITM sniper，方向几乎不错，winner 累积
- candid_closure (loser-heavy): 双边 maker，loser side 沉淀
- c08 (loser-heavy): 方向不准的 maker，亏损沉淀

这暗示 **residual 偏向是 alpha 质量的间接指标**：
- winner-heavy = 方向准确率高
- loser-heavy = 中性 maker 或方向不准

### 2.3 5m vs 15m+ 选择是 alpha 边界

观察所有 archetype 的 tenor 偏好：
- xuan (C): 5m only — 紧密执行 + 91% clean
- late_grammar (C'): 5m + 15m + daily mix — 不 MERGE
- quietb27 (D): 5m only — directional taker
- gabagool22 (E): 5m + 15m + daily — 注册 maker
- silent_d189 (F): 5m only — ITM sniper
- candid_closure (E'): **15m + 4h + daily（无 5m）** — 注册 maker
- c01 / c10: 15m only — passive maker no MERGE
- c02: 5m + 15m + daily — xuan-like 但 no MERGE
- c08: 5m focused — directional maker

**5m 是高 churn 战场**——绝大多数 archetype 都做。**只跳过 5m 的是 candid_closure**（注册 maker 长 tenor 偏好）。

V2 §0.5 修正 3 中提出的「**xuan 5m 专精暗示执行栈优势**」得到进一步支持——5m 是大家都想做的市场，但每人做法不同；xuan 在 5m 上达到 91% clean_ratio 是少有的执行成就。

### 2.4 数据 API 截断的实际影响

5 个候选都遇到 data-api 3500 条 cap：
- c01: 500 trades sample 已包含 10 venue
- c05: 500 positions 全部 loser-only — snapshot 仅显示「未 redeem 的 loser 尾部」，无法知道 winner 总量
- c08: 500 positions 接近 cap，可能更多

**含义**：positions 数远大于 cap 的 trader（c05/c08），实际 active 程度可能远高于 quick probe 显示。

---

## 3. 与 6 archetype 谱系映射

```
                              MERGE 频率
                   高 (in-round)│
                                │
              Archetype C ●──────●  Archetype E (gabagool22)
              (xuan)             │  multi-venue tenor-adaptive
                                 │
              ●(c02)             ● Archetype E' (candid_closure)
              5m no MERGE        │  long-tenor multi-venue
              "lazy xuan"        │
                                 │
                  ┌─────────────┼─────────────┐
                  │             │             │
        单 venue ←┤             │             ├→ 多 venue
                  │             │             │
        ●(c08)   ●(quietb27)    │     ●(c01,c10) ●(late_grammar)
        F variant Archetype D    │     mid-late_grammar Archetype C'
        Maker dir taker dir      │     (10 venue)
                                 │
        ●(silent_d189)          ●(c05)
        Archetype F             D++ (失败 directional)
                                │
                  低 (post-close 或 0)
```

---

## 4. 总建议

1. ✅ **不再 deep dive 这 5 个 MAYBE**——它们都是已知 archetype 的 variant，无新信息
2. ✅ **重点回到 V2 综合修订**（基于 6 个深度研究 + 5 个 quick survey 共 11 个数据点）
3. ✅ **archetype 谱系封盘**——除非用户提供新的"特别"钱包，否则暂停横向扩展
4. ✅ **下一步研究方向**：
   - **注册 maker 流程实务**（gabagool22 / silent_d189 / candid_closure 都是注册 maker，我们 PGT 想拿 rebate 必须了解注册流程）
   - **xuan §11 第 7 条**（补全 cleaned slug 的 winner side，验证 §0.5 修正 5b 的 95% loser-bias 是否被 selection bias 影响）

---

## 附录 A：数据文件

```
data/maybe_5_quicksurvey.json    # 5 个候选 quick probe 汇总数据
```

5 个候选未单独建 data/ 目录（不进 deep dive）。如未来需要其中任一深挖，可参考 `pull_silent_d189_long_window.py` 模板，仅替换 WALLET + OUT_DIR 即可启动。
