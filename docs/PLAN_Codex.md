## Pair_arb 实盘修正版计划 V5（简化状态签名 + 可配置 tier cap）

### Summary
基于你刚才的三点，计划需要继续收敛：

- **不采用 `last_risk_fill_price` 路径依赖**
- **不采用硬性的 `pair_progress_brake`**
- **Republish 改为简化状态签名驱动**
- **`tier avg-cost cap` 收紧到 `0.80 / 0.60`，并做成可配置参数**
- **OFI 继续复用当前 GLFT 引擎，但在 `pair_arb` 中只做 subordinate shaping，不再追求“常态毒性撤单”**

这版的核心哲学是：
- `pair_arb` 继续只看当前状态，不看历史路径
- 主仓侧不是“禁止继续买”，而是“高失衡时更难通过准入”
- 只有真正重要的状态变化才触发 republish，不跟着每个 partial fill 跑

### Key Changes
#### 1. `tier avg-cost cap` 收紧并参数化
把当前 `pair_arb` 的 dominant-side cap 从 `0.85 / 0.70` 收紧到：

- `5 <= |net_diff| < 10`
  - 主仓侧 `bid <= avg_cost * 0.80`
- `|net_diff| >= 10`
  - 主仓侧 `bid <= avg_cost * 0.60`

同时新增两项配置参数：
- `PM_PAIR_ARB_TIER_1_MULT=0.80`
- `PM_PAIR_ARB_TIER_2_MULT=0.60`

固定约束：
- 这两个参数只影响 `pair_arb`
- 依然位于价格链中的 `VWAP ceiling` 之前
- 依然只约束主仓侧 `same-side risk-increasing buy`
- 不引入 `last fill` 路径依赖，不要求“价格必须低于上一笔成交价”

价格链固定为：
1. A-S 基础价
2. 三段 skew
3. **tier avg-cost cap**
4. same-side OFI soft shaping
5. `VWAP ceiling`
6. maker clamp
7. `safe_price`
8. `simulate_buy`

#### 2. 用“高失衡准入收紧”替代硬 brake
不再做“30 秒无进展就冻结主仓侧”的 `pair_progress_brake`。  
改为只在候选准入层收紧 `same-side risk-increasing buy`：

- `pairing / risk-reducing buy` 完全不受影响
- `same-side risk-increasing buy`：
  - `abs(net_diff) < 10`：保持现有 `utility / open_edge` 规则
  - `abs(net_diff) >= 10`：提高准入门槛
    - `min_utility_delta` 提高到 `2.0 * size * tick`
    - 且 `projected_open_edge` 必须至少改善 `0.5 * size * tick`
- 任一不满足则 suppress 该候选

这一步的作用是：
- 不硬冻结主仓侧继续平均成本的能力
- 但在高失衡区，只允许“明显更值得”的同侧 build 继续存在
- 这比硬 brake 更符合 `pair_cost-first` 的策略思想

#### 3. Pair_arb 专属最小 SoftClose
保留最小 endgame 接入：

- 仅对 `15m` 启用
- 固定窗口：最后 `45s`
- 只阻断：
  - `same-side risk-increasing buy`
- 继续允许：
  - `pairing / risk-reducing buy`
- 继续跳过：
  - `HardClose`
  - `ForceTaker`
  - 市价去风险

目标：
- 防止最后几十秒继续把单边残仓从 `10 -> 15`
- 不误伤真正的补配对机会

#### 4. `Republish` 改成简化状态签名驱动
`pair_arb` 的状态变化 republish 不再看每次 fill 的细粒度数字，也不看 `paired_qty` 桶。  
固定采用简化状态签名：

- `dominant_side`：`Yes | No | Flat`
- `net_bucket`：
  - `Flat` (`|net_diff| == 0`)
  - `Low` (`0 < |net_diff| < 5`)
  - `Mid` (`5 <= |net_diff| < 10`)
  - `High` (`|net_diff| >= 10`)
- `soft_close_active`：`true | false`

规则固定：
- 只有当这个状态签名发生变化时，才做一次 `state-change admissibility recheck`
- 若当前 live quote 因新状态已不再满足：
  - `tier avg-cost cap`
  - 高失衡准入
  - `VWAP ceiling`
  - `SoftClose`
  - OFI suppress
- 则走：
  - `RetentionDecision::Republish`
- 若状态签名未变化：
  - partial fill
  - `net_diff +1/+2/+3`
  - 小规模 `paired_qty` 增加
  都**不触发 republish**
- 只有：
  - source blocked
  - invalid state
  - round cleanup
  才允许 `Clear`

这一步的目标是：
- 避免 1 秒内多次 partial fill 导致重复 republish
- 又保留真正关键状态变化的响应能力

#### 5. OFI 边界明确：继承 GLFT 引擎，但在 Pair_arb 中降级使用
当前 OFI **确实继承的是 GLFT 时代的引擎**，包括：
- `heat`
- `toxicity`
- `saturated`
- regime-aware baseline
- adverse-selection 确认

但在 `pair_arb` 中，它的职责已经被刻意降级：

- `pairing / risk-reducing buy`
  - 忽略 OFI
- `same-side risk-increasing buy`
  - `hot`：下调 1 tick
  - `toxic`：下调 2 ticks
  - `toxic + saturated`：suppress
- 执行层对 `pair_arb` 不再做第二套常态 OFI 硬拦截

所以“最近似乎没有毒性撤单”并不代表 OFI 没用了，而是：
- 引擎还在跑
- 统计仍会记 `heat_events / toxic_events / kill_events`
- 但 `pair_arb` 的主语义已经不是“常态毒性撤单”，而是“高热/毒性下收紧同侧加仓”

这条边界在文档里要明确写死，避免继续误解。

#### 6. 观测与文档同步
同步更新：
- `STRATEGY_PAIR_ARB_ZH.md`
- `CONFIG_REFERENCE_ZH.md`
- `README` / checklist 中当前 pair_arb 部分

日志新增：
- `candidate_role={pairing|risk_increasing}`
- `high_imbalance_admission={pass|blocked}`
- `state_key_changed=true|false`
- `state_change_republish=true|false`
- `softclose_blocked=true|false`

同时处理死统计项：
- `cancel_toxic`
- `cancel_inv`
- `skipped_inv_limit`

要求二选一：
- 真接线并持续计数
- 或删除，不再保留假可观测性

### Public Interfaces / Config
新增或调整最小接口：

- 新增配置：
  - `PM_PAIR_ARB_TIER_1_MULT`
  - `PM_PAIR_ARB_TIER_2_MULT`
- 新增内部状态：
  - `PairArbStateKey`
    - `dominant_side`
    - `net_bucket`
    - `soft_close_active`
- 不新增新的 OFI 参数
- 不新增 `pair_progress_brake` 参数
- 不改变 `PM_STRATEGY=pair_arb` 外部入口

### Test Plan
1. **tier cap 配置**
- 默认值读取为 `0.80 / 0.60`
- 自定义配置可覆盖
- `net_diff=10, yes_avg=0.60`：
  - YES cap = `0.36`
- `net_diff=5, yes_avg=0.60`：
  - YES cap = `0.48`
- `net_diff<5`：
  - dominant-side cap 消失

2. **高失衡准入**
- `abs(net_diff) >= 10` 时：
  - utility/open-edge 门槛更高
  - 但不是硬冻结
- pairing buy 不受影响
- 验证状态改善后仍可合法再报价

3. **SoftClose**
- `15m` 最后 `45s`
  - risk-increasing buy 阻断
  - pairing / risk-reducing buy 保留

4. **简化状态签名 Republish**
- partial fill 但签名未变化：
  - 不 republish
- `|net_diff|` 跨越 `0/5/10`：
  - 触发一次状态重检
- `dominant_side` 翻转：
  - 触发一次状态重检
- 进入 `SoftClose`：
  - 触发一次状态重检
- 不再因为每个 small fill 重复 churn

5. **OFI 边界**
- same-side risk-increasing + `hot`：
  - 报价软化 1 tick
- same-side risk-increasing + `toxic + saturated`：
  - suppress
- pairing buy + toxic：
  - 不得被误伤
- 证明 `pair_arb` 的 OFI 语义是“soft shaping”，不是“常态毒性撤单”

6. **针对 2026-04-07 live 的回放验收**
- Round #1：允许 state-driven 的正常再报价，但不应出现尾段危险 build
- Round #2：高失衡区同侧 build 必须更难通过，不再一路轻松买满
- Round #3：配对成功后，旧单只在关键状态变化时 republish，不再长期滞留，也不因每个 partial fill 反复重算

### Assumptions
- 主线继续 `pair_arb + BTC 15m`
- 采用更简单的 republish 签名，不引入 `paired_qty` 桶
- 不采用路径依赖的 `last_risk_fill_price`
- 不采用硬 `pair_progress_brake`
- OFI 继续复用当前 GLFT 引擎，但在 `pair_arb` 中只保留 subordinate shaping 角色
- 不采纳绝对价格底限
- 不引入 taker / HardClose / 市价去风险
