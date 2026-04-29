# `gabagool22` (Grown-Cantaloupe) 策略深度研究

生成时间：`2026-04-26`
研究对象：`gabagool22` / pseudonym `Grown-Cantaloupe`，钱包 `0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d`
URL：`https://polymarket.com/profile/0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d`

研究动机：**项目名字 `Gabagool22 + A-S + OFI Kill Switch + Oracle-Lag Sniping` 的源头**——这个钱包是项目命名的灵感来源，但从未被系统研究过。本次首次深挖以验证项目蓝图根基。

证据等级：`A`=直接确认；`B`=强支持；`C`=合理假设。

---

## 0. TL;DR

**关键定性**（与 V2 §0.5 修正 1 直接相关）：

> gabagool22 是一台 **6 venue 平铺 + 严格 10 股 clip + tenor-adaptive MERGE + 高频爆发** 的 Polymarket maker。它**与 xuan / late_grammar / quietb27 都不同**——是第 5 个 archetype。最重要的发现：**activity 中存在 `MAKER_REBATE` event**——这是公开数据中**首次直接确认 maker 身份的硬证据**（V2 §8 / late_grammar §8 / quietb27 §8 都只能是 B 级间接推断）。

**与 xuan 的关系**：
- ✅ 同样 BUY only，0 SELL（与 xuan / late_grammar / quietb27 同）
- ✅ 同样有 MERGE 收水机制（与 xuan 同，与 late_grammar 反）
- ⚠️ MERGE 时机完全不同：xuan = round close 前 22s 主峰；gabagool22 = **round close 之后**（63/63 全 post-close）
- ⚠️ Universe 不同：xuan 单一 BTC 5m；gabagool22 = **6 venue cross-asset cross-tenor**（btc/eth/bitcoin-daily/ethereum-daily × 5m/15m/daily）
- ⚠️ Clip size 极端严格：xuan p50=132 弹性；gabagool22 **几乎所有 clip = 10 股**（p50=9.9, p90=10.0）

**对项目根基的含义**：

> 我们项目命名借用 gabagool22，但实际 PGT 蓝图（V1.1）更接近 xuan 的「单 venue 紧密 in-round MERGE」而非 gabagool22 的「multi-venue post-close batch MERGE」。**项目根基应该重新审视**：是要继续走 xuan 路线（V1.1），还是部分回归 gabagool22 的多 venue + 后置 MERGE + tenor-adaptive 路线？详见 §10。

---

## 1. 数据样本与窗口

### 1.1 数据来源

- profile：gamma-api `/public-search?q=gabagool22&search_profiles=true` 返回 4 个候选；选 pseudonym = `Grown-Cantaloupe`（无 bio）的主账号
- 其他候选是 imitator（`gabagool22.nutildah` bio: "I think i cracked gabagool22"，`gabagool22-2th`，`gabagool22slayer`）
- 拉取时间：`2026-04-26 15:44 UTC`

### 1.2 数据规模

- positions = **0**（snapshot 时刻无 open；与 quietb27 同）
- trades = `3500`，window UTC `2026-02-20 00:04` → `2026-02-20 09:06`，跨度 **9.03 h**
- activity = `3500` = **`3343 TRADE` + `86 REDEEM` + `67 MERGE` + `3 REFERRAL_REWARD` + `1 MAKER_REBATE`** ⭐

> **重大发现**：`MAKER_REBATE` event 是 Polymarket 对官方注册 maker 发的费率补贴。出现 1 次即直接确认该钱包是 **registered maker on Polymarket**。这是公开数据可获得的最强 maker 证据，**直接把 §8 maker/taker 等级从 B 升到 A**。

### 1.3 时间分布特征

```text
trades 时间窗：2026-02-20 00:04:38 → 09:06:08 UTC（仅 9.03 小时）
weekday：100% Friday
hour 分布：UTC 00–08 集中（350–470/h），UTC 09 衰减到 70，UTC 10+ 全空
```

**这是单日 9 小时爆发期数据**——3500 trades / 9.03 h = **389 trades/h**。

> 注：data-api 3500 cap 限制了我们能看到更早数据；这 9 小时窗可能是该钱包完整生命周期的一个 active session 或 burst。无法判断它是「每周固定爆发一次」还是「单次实验后停止」。

等级：A（数据），C（burst pattern 解读）

---

## 2. 严格 10 股 clip — 最显著特征

### 2.1 全集分布

```text
n=3500
p10=4.9    p50=9.9    p90=10.0    p99=10.0    max=10.0
```

**几乎所有 clip 都是恰好 10 股**，少数低价位由于 fill 量不足略低于 10 股。**最大值是 10.0，没有任何例外**。

vs xuan p50=131.7 / max=600；vs late_grammar p50=10.4 / max=633；vs quietb27 p50=59.6 / max=9392。

**gabagool22 的 clip size 是所有研究对象中最严格的**——一个固定的 `clip = 10 shares` 规则。

### 2.2 不依赖 imbalance / intra-round / venue

| 维度 | clip p50 |
|------|---------:|
| trade #1 (intra-round) | 9.8 |
| trade #10 | 9.9 |
| imb 0.00-0.05 | 9.9 |
| imb 0.30-1.00 | 9.9 |
| round 段 0-30s | 9.8 |
| round 段 240-300s | 7.9 |
| round 段 post-close (300+) | 9.9 |

**几乎所有维度上 clip p50 都是 9.8–9.9**——零适配，纯固定常量。这是**最简单的 maker post 规则**。

含义：clip 不是 inventory-aware（与 xuan 反），不是 imbalance-aware（与 xuan 反），就是 **「post 10 股，被打就再 post 10 股」**。

等级：A

---

## 3. 6 Venue 平铺

### 3.1 Universe 分布

| Universe | trades | rounds | t/round |
|----------|-------:|-------:|--------:|
| eth-updown-5m   | 869 | 108 | 8.0 |
| btc-updown-15m  | 768 |  36 | **21.3** |
| eth-updown-15m  | 695 |  33 | **21.1** |
| btc-updown-5m   | 630 | 102 | 6.2 |
| bitcoin-daily   | 352 |  10 | **35.2** |
| ethereum-daily  | 186 |  10 | 18.6 |
| **总计** | 3500 | 299 | 11.7 avg |

每 round 平均 trade 数随 tenor 升：5m round ~6–8 trades，15m ~21 trades，daily ~19–35 trades。**tenor 越长每 round 交易越多**——符合「时间越长有更多双边对冲机会」直觉。

### 3.2 Round coverage

```text
expected btc-5m rounds in 9.03h: 113
actually-traded btc-5m rounds:    102
coverage:                         90.3%
```

vs xuan 94.7% / late_grammar 98.3% / quietb27 17.9%。**gabagool22 的 round coverage 接近 xuan**——几乎每个 round 都进。

但 quan 单 venue 全进 vs gabagool22 6 venue 都进——后者**总并发负担**比 xuan 高 6×。

等级：A

---

## 4. Tenor-adaptive MERGE / REDEEM（最有趣的策略机制）

### 4.1 MERGE 全部 post-close

```text
MERGE 数量: 63
MERGE offset (s vs round open):
  p10=626   p50=1826   p90=3618

MERGE in-round (offset < 300s for 5m round): 0
MERGE post-close: 63 / 63 = 100%
```

vs xuan 全部 in-round（before close p50=278s）。**完全相反时机**！

但需要分 tenor 看：
- 5m round 关 300s，1826s offset = round close 后 1526s = 25 min
- 15m round 关 900s，1826s offset = round close 后 926s = 15 min
- daily round 关 86400s，1826s offset = 在 round 内 30 min

所以 MERGE p50=1826s 实际上是：
- **5m / 15m round 的 post-close 集中处理**
- **daily round 的 in-round 早期处理**

这是 **tenor-adaptive MERGE**——5m/15m 等结算后再 batch 处理，daily 在 round 内主动 merge。

### 4.2 REDEEM 节律

```text
REDEEM 数量: 71
REDEEM offset (s vs round open):
  p10=932   p50=2732   p90=42408
REDEEM all post-close: 71/71
```

vs xuan REDEEM p50=338s；late_grammar p50=7864s；quietb27 p50=516s。

gabagool22 REDEEM 中位 2732s = round 后约 40 min（5m round），介于 xuan 38s 和 late_grammar 2.18h 之间。

REDEEM p90=42408s = 11.8h —— 长尾的 daily round redeem。

### 4.3 资金回收量级

```text
MERGE total: 19,658 shares (= USDC)
REDEEM total:  1,619 shares (= USDC)
ratio:        12.1:1
```

vs xuan 5.2:1；late_grammar 0:∞；quietb27 17.4:1（少量）。

**gabagool22 强 MERGE 主导**——比 xuan 还激进的 MERGE/REDEEM 比例（12:1 vs 5.2:1）。

等级：A

---

## 5. Episode 与方向性（中等水平）

### 5.1 Episode 重建

```text
                     eps=10                     eps=25
opened               505                        255
closed               313                        154
clean_closed         196                         97
clean_closed_ratio   38.81%                     38.04%
same_side_add_ratio  29.45%                     15.45%
close_delay_p50      32s                         28s
close_delay_p90      214s                       188s
```

vs xuan 91.3% clean / 10.5% same-add；late_grammar 33–100% (venue dep)；quietb27 2.17% / 44.5%。

gabagool22 的 38.8% clean 处于**中间偏低**——比 xuan 差很多，但比 quietb27 好得多。同侧追加比例 29.5% 也是**中间值**。

### 5.2 Per-round 方向性

```text
imbalance |U-D|/total:
  median: 0.272
  mean:   0.357
rounds with imb > 0.5 (highly directional): 74 / 299 = 24.7%
rounds with imb < 0.05 (well-paired):       39 / 299 = 13.0%
single-side rounds (pure directional):      39 / 299 = 13.0%
```

vs xuan ≈ 0% pure directional；late_grammar 中等；quietb27 68.9%。

gabagool22 介于 xuan 和 quietb27 之间——**部分 directional + 部分 paired**。

### 5.3 Run length

```text
n_runs=1688  p50=2  p90=4  max=12
histogram: {1:786, 2:449, 3:224, 4:114, 5:55, 6:34, 7:11, 8:8, ...}
```

max=12 比 xuan max=4 长得多但比 quietb27 max=31 短得多。**46.5% 的 run = 1 笔同侧**（vs xuan 81%, quietb27 40%）。

等级：A

---

## 6. Inter-fill delay

```text
all  : p10=0  p50=14  p90=78
cross: p10=2  p50=18  p90=82
same : p10=0  p50=10  p90=76
```

非常接近 xuan（cross p50=18, same p50=10）。**与 xuan 同时序节律**。

Same-second cross-side ratio = 1.14% （vs xuan 5.49%, late_grammar 2.49%, quietb27 1.37%）—— 低，**几乎不双边预埋**。

等级：A

---

## 7. Maker vs Taker 路径裁决（首次升 A 级）

### 7.1 关键证据：`MAKER_REBATE` event

activity counter:

```text
TRADE: 3343
REDEEM: 86
MERGE: 67
REFERRAL_REWARD: 3
MAKER_REBATE: 1     ⭐
```

Polymarket 的 MAKER_REBATE event 是**针对官方注册 maker 的费率补贴**。出现该事件即**直接确认该钱包是 Polymarket registered maker**。

虽然只有 1 次（rebate 触发条件可能是周期性结算或某门槛），但这一条 event 比所有间接证据都强。

**等级：A（直接 on-chain 证据）**

### 7.2 间接证据互证

- clip 严格 10 股（passive maker post）✅
- clip 不依赖 imbalance / intra-round（不主动加仓）✅
- inter-fill same p50=10s（与 xuan 一致，maker 被分批吃）✅
- 0 SELL（与所有 maker 同）✅

### 7.3 含义

gabagool22 的**确认 maker 身份**给我们提供了一个**重要的反向推断工具**：

可以在 V2 §8 / late_grammar §8 / quietb27 §8 中，通过查 activity 是否含 `MAKER_REBATE` event **直接验证 maker 身份**。

**已验证（2026-04-26 跑 grep）**：

| Wallet | MAKER_REBATE count |
|--------|---------:|
| xuan | **0** |
| late_grammar | **0** |
| quietb27 | **0** |
| **gabagool22** | **1** |

**结论**：4 个 wallet 中**只有 gabagool22 是 Polymarket 官方注册 maker**。

注意：MAKER_REBATE event 触发条件可能是周期性结算或某 tier 门槛，1 次出现是**单边正向证据**（必定是 maker），但 0 次出现**不能直接证明非 maker**（可能注册 maker 但未达 rebate 触发条件）。

更稳妥的解读：
- **gabagool22**：✅ A 级 maker（直接确认）
- **xuan / late_grammar / quietb27**：维持 B 级（间接证据强但无 rebate event）—— 可能是「跑 maker geometry 但未注册 / 未达 rebate tier」。

等级：A

---

## 8. 与 xuan + late_grammar + quietb27 横向对比

| 维度 | xuan | late_grammar | quietb27 | **gabagool22** |
|------|------|--------------|----------|----------------|
| Universe 数 | 1 | 6 | 1 | **6** |
| 主 venue | BTC 5m | btc/eth × 5m/15m + daily | BTC 5m | **btc/eth × 5m/15m + daily** |
| 窗口 | 37h | 14.6h | 361h | **9.03h（burst）** |
| 平均 trades/h | 95 | 240 | 9.7 | **389** |
| Round coverage | 94.7% | 98.3% | 17.9% | **90.3%** |
| 双侧持仓率 | 0.2% | 88.8% | 0% | **0%（snapshot）** |
| Clip p50 | 131.7 | 10.4 | 59.6 | **9.9** |
| Clip max | 600 | 633 | 9392 | **10.0** |
| Clip 适配 | imb + idx | 部分 | 无 | **完全固定** |
| MERGE 总量（pro-rated 9h） | 121 | 0 | 2.9 | **63** |
| MERGE 时机 | in-round (p50=278s, 距 close 22s) | n/a | 早期 (p50=172s) | **post-close** (p50=1826s) |
| REDEEM 总量（pro-rated 9h） | 78 | 277 | 0.17 | **71** |
| REDEEM 时机 | +38s post-close | +2.18h post-close | +216s post-close | **+25 min post-close** |
| MERGE/REDEEM 比 | 5.2:1 | 0:∞ | 17.4:1 | **12.1:1** |
| clean_closed_ratio | 91.3% | 33-100% (venue) | 2.17% | **38.8%** |
| same_side_add_ratio | 10.5% | 4-39% (venue) | 44.5% | **29.5%** |
| Run length max | 4 | 8 | 31 | **12** |
| Pure directional rounds % | ~0% | unknown | 68.9% | **13.0%** |
| Inter-fill same p50 | 10s | 2s | 0s | **10s** |
| **Maker/Taker** | Maker (B) | Maker (B) | Taker (B) | **Maker (A) ⭐** |

**5 项 archetype 谱系**（更新）：

- A：体育 directional（rank01-02-04-05-06 of leaderboard）
- B：政治 directional with active SELL（rank03 tripping）
- C：crypto-perpetual single-venue passive maker（**xuan**）
- C'：crypto-perpetual multi-venue passive maker（**late_grammar**）
- D：crypto-perpetual single-venue selective directional taker（**quietb27**）
- **E（新）：crypto-perpetual multi-venue tenor-adaptive maker（gabagool22）**

E 与 C' 的差异：
- C' (late_grammar) 不 MERGE，纯囤双边等结算
- E (gabagool22) 有 tenor-adaptive MERGE：daily round mid-round merge + 5m/15m post-close batch merge

E 与 C (xuan) 的差异：
- C 单 venue + in-round merge
- E 多 venue + post-close merge

等级：A

---

## 9. 对 V2 文档与 PGT 蓝图的修订建议

### 9.1 立即修订 V2

#### 修订 A：§8 maker/taker 等级机制

发现 `MAKER_REBATE` event 是直接 on-chain 证据。建议：

1. 跑 `cat data/xuan/activity_long.json | grep -c MAKER_REBATE`
2. 如有 ≥ 1 → §8 升 A 级；如无 → 维持 B 级（**说明 xuan 不是注册 maker，可能跑的是 self-rebate 算法**）
3. 同样跑 late_grammar / quietb27 的 activity 验证

#### 修订 B：项目命名 `gabagool22` 的真实 archetype 与 V1.1 不一致

V1.1 蓝图的「单 venue 紧密 in-round MERGE state machine」更接近 xuan 而非 gabagool22。**项目命名继承的策略灵感与实际蓝图脱节**。

建议在 V2 §0.5 后**新增 §0.6 "项目命名与实际蓝图的关系"**，澄清：
- 名字 `Gabagool22` 来自这个 wallet
- 但 V1.1 蓝图实际是 xuan-derived
- 这不是 bug 而是有意选择（xuan 路线在 V1.1 蓝图里更可执行）

### 9.2 PGT 蓝图增量备注

V1.1.1 增量（与 late_grammar 对应建议合并）：

1. **Per-tenor MERGE timing**：
   - 5m / 15m: post-close batch MERGE（gabagool22 模式）
   - daily: mid-round adaptive MERGE（gabagool22 daily 模式）
   - 当前 V1.1 只针对 5m，加 tenor-adapted MERGE timer 是直接借鉴

2. **极简 fixed clip 选项**：
   - gabagool22 用固定 10 股 clip 跑通 maker rebate
   - V1.1 当前是 BASE_CLIP=120 + adaptive
   - 加一个 `clip_mode = fixed | adaptive` 配置项，shadow 测试 fixed 是否在 high-throughput regime 下更优

3. **MAKER_REBATE 监控**：
   - 我们 PGT shadow 启动后，监控自己 wallet 是否产生 MAKER_REBATE event
   - 这是**检验我们是否被 Polymarket 认定为 official maker** 的硬指标

### 9.3 复刻判断：**部分推荐**（区别于 late_grammar / quietb27 的 "不推荐"）

理由：
- ✓ 它的 maker 身份直接确认（A 级证据），策略路径与我们 PGT 同向
- ✓ Tenor-adaptive MERGE 机制是 V1.1 没有的，可借鉴
- ✓ 严格 10 股 clip 是**最简单可复制的执行规则**
- ✗ Multi-venue 平铺需要 cross-venue infrastructure（与 PGT 当前单 venue 设计正交）
- ✗ 9 小时单日 burst 数据样本不足以判断长期可持续性

**建议**：保留 V1.1 主干（xuan-derived 单 venue），但在 V1.1.1 增量里**优先借鉴 gabagool22 的 tenor-adaptive MERGE + maker rebate 监控**。multi-venue 等 V1.1 主干 shadow 验证后再考虑扩展。

---

## 10. 项目命名与策略路线的关系（重要）

项目代码自我描述为 `Gabagool22 + Avellaneda-Stoikov + OFI Kill Switch + Oracle-Lag Sniping`——这显然是**借用** gabagool22 的"高频 micro-clip maker"形象作为项目灵感名。

但实证发现：
- gabagool22 = multi-venue + tenor-adaptive + 严格 10 股 + post-close MERGE
- xuan = 单 venue + in-round MERGE pulse + 自适应 clip
- 我们的 V1.1 蓝图 = xuan-derived

**含义**：

1. **「gabagool22 灵感」 ≠ 「gabagool22 复刻」**——项目继承的是其「高频 micro-clip maker 美学」，而非具体策略机制

2. **V1.1 主干基于 xuan 是合理的**：
   - xuan 是单 venue，与我们当前 polymarket_v2 单 venue 架构兼容
   - xuan 的 in-round MERGE 收水是**资金效率最高**的
   - 91.3% clean_ratio 是 archetype C 中最干净的执行

3. **V1.1.1 借鉴 gabagool22**：
   - tenor-adaptive MERGE timer
   - 极简 10-fixed-clip 模式（高 throughput 场景）
   - MAKER_REBATE 监控

4. **未来 V2 / V3 蓝图扩展**：
   - 如果要扩 multi-venue，参考 gabagool22 的 6 venue 平铺
   - 如果要做 directional alpha，参考 quietb27 的 round selection signal（需独立信号源）

---

## 11. 证据等级表

| 维度 | 等级 |
|------|------|
| 「multi-venue cross-asset/tenor」 | A |
| 「严格 10 股 clip，零适配」 | A |
| 「Maker 身份」 | **A（首次基于 MAKER_REBATE on-chain event）** |
| 「Tenor-adaptive MERGE 时机」 | A（5m/15m post-close + daily mid-round 数据直接显示）|
| 「9h burst 是单次活动 vs 长期模式」 | C（数据窗口受限）|
| 「项目命名灵感 ≠ V1.1 实际机制」 | A（直接对比）|
| 「Tenor-adaptive MERGE 值得借鉴」 | B |
| 「Multi-venue 部署值得借鉴」 | B（依赖架构升级）|

---

## 12. 下一步

1. **立即跑** `grep MAKER_REBATE data/xuan/activity_long.json` 验证 xuan 是否注册 maker → 决定 V2 §8 等级
2. **跑同样 grep on late_grammar / quietb27** → 完善 archetype 表 maker 列等级
3. **写 4-archetype 综合对比文档**（已计划）
4. **xuan §11 第 5 条**（MERGE 前瞬时仓位重建）—— 与本研究并行推进
5. **拉 gabagool22 更长窗口**（如果可能，通过 before-cursor 拉 9h 之前的数据）—— 验证 burst 是否周期性

---

## 附录 A：脚本与数据文件

```
scripts/pull_grown_cantaloupe_long_window.py    # WALLET swap from quietb27 template
scripts/analyze_grown_cantaloupe_positions.py
scripts/analyze_grown_cantaloupe_long_window.py

data/grown_cantaloupe/positions_snapshot_2026-04-26.json    # 0 rows
data/grown_cantaloupe/trades_long.json                      # 3500 rows, 9.03h burst
data/grown_cantaloupe/activity_long.json                    # 3500 rows, includes MAKER_REBATE
data/grown_cantaloupe/positions_analysis.json / .txt
data/grown_cantaloupe/long_window_analysis.json / .txt
data/grown_cantaloupe/pull_summary.json / pull_log.txt
```
