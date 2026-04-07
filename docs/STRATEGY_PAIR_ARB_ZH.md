# `pair_arb` 策略说明（2026-04 版）

本文档描述当前主线 `pair_arb` 的运行语义、价格链、风险边界和验证口径。

## 1. 定位

`pair_arb` 是当前仓库的主验证策略。它是一个：
- maker-only
- buy-only
- 双边报价
- `UnifiedBuys` 执行模式
- 以 `pair_cost / paired_locked_pnl` 为主目标
- 以库存偏置限制单边风险

它不是：
- `glft_mm` 那种外锚真双边做市
- `dip_buy / phase_builder` 那种 directional hedge overlay
- 尾盘市价对冲或方向性主动平仓策略

当前策略只输出两个买入槽位：
- `YesBuy`
- `NoBuy`

共享执行层仍支持四槽位，但 `pair_arb` 主路径不会主动输出 `Sell`。

## 2. 一句话策略意图

只有当新的买入符合阈值要求，最好能服务于更低对子成本，且不把库存推入不可接受的单边敞口时，才继续挂单。

这里“最好能服务于更低对子成本”的含义是：
- 优先鼓励能直接改善 `pair_cost / paired_locked_pnl` 的买入
- 对同侧继续加仓则要求更严格：必须更便宜，且要对 open edge / utility 有改善

## 3. 运行形态

`pair_arb` 的控制流分成两层：

### 3.1 策略层
每个 tick 只负责回答：
- 现在 `YesBuy` 该不该挂
- 现在 `NoBuy` 该不该挂

### 3.2 执行层
统一负责：
- keep-if-safe
- retain / republish / clear
- post-only 安全垫
- slot 生命周期
- reprice / cancel / reconcile

这意味着：
- `pair_arb` 不直接控制 OMS / Executor
- 已经挂在书上的旧单，不会因为策略层重新算出了新价格就必然立刻撤掉
- 旧单是否继续保留，取决于共享执行治理是否认为它仍然安全、仍然对齐当前目标

## 4. Tick 级价格链

每个 tick，`pair_arb` 的价格链固定为：

1. A-S 基础价格
2. 三段库存 skew
3. 主仓侧梯度 avg-cost cap
4. same-side OFI soft shaping
5. `VWAP ceiling`
6. strict maker clamp
7. `safe_price`
8. `simulate_buy`

只有最后通过整条链的候选，才会真正变成 `YesBuy / NoBuy` 意图。

### 4.1 A-S 基础价格

先计算盘口中间价：
- `mid_yes = (yes_bid + yes_ask) / 2`
- `mid_no = (no_bid + no_ask) / 2`

再基于 `pair_target` 计算双边的基础下压：
- `excess = max(0, mid_yes + mid_no - pair_target)`

含义：
- 当 `YES + NO` 的中间价和高于 `pair_target` 时，双边基础报价都会往下压
- 这样系统天然优先在更便宜的对子成本区间挂单

### 4.2 三段库存 skew（固定边界 `5 / 10 / 15`）

- `|net_diff| < 5`
  - `0.35 * PM_AS_SKEW_FACTOR`
  - 不叠加 time decay
- `5 <= |net_diff| < 10`
  - skew 从 `35% -> 100%` 线性抬升
  - 开始叠加 `PM_AS_TIME_DECAY_K`
- `10 <= |net_diff| < 15`
  - `100% * PM_AS_SKEW_FACTOR`
  - 继续叠加 time decay
- `|net_diff| >= 15`
  - 由库存硬上限阻止继续增加主仓侧风险

这套 skew 只看当前状态，不保留历史记忆。

也就是说：
- 当 `|net_diff|` 升到 `10`，主仓侧偏置会明显增强
- 当后续缺失侧成交，把 `|net_diff|` 拉回 `4`，策略会在下一次 tick 重新回到低强度 skew 区间
- 它不是“进入某档后永久停留”，而是纯粹由当前库存状态驱动

### 4.3 主仓侧 avg-cost cap（可配置）

这一步只限制主仓侧继续 build，不限制缺失侧 pairing。

当前默认规则：
- Tier 1：`5 <= |net_diff| < 10`
  - 主仓侧 `bid <= avg_cost * PM_PAIR_ARB_TIER_1_MULT`
  - 当前默认：`0.80`
- Tier 2：`|net_diff| >= 10`
  - 主仓侧 `bid <= avg_cost * PM_PAIR_ARB_TIER_2_MULT`
  - 当前默认：`0.60`

直观理解：
- 仓位越偏，主仓侧继续买就必须越便宜
- 缺失侧补配对不受这条约束，仍可相对积极
- 这条约束只看**当前状态**，不看“上一笔 risk fill 的价格路径”

### 4.4 OFI 软塑形（仅同侧风险增加单）

OFI 在 `pair_arb` 中不是主目标函数，而是 subordinate 风险塑形器。

先判断候选买单的风险效果：
- `projected_abs_net_diff <= current_abs_net_diff`
  - 视为 `pairing / risk-reducing`
- `projected_abs_net_diff > current_abs_net_diff`
  - 视为 `same-side risk-increasing`

规则固定：
- `pairing / risk-reducing buy`
  - 完全忽略 OFI
- `same-side risk-increasing buy`
  - `is_hot=true`：额外下调 `1 tick`
  - `is_toxic=true`：额外下调 `2 ticks`
  - `is_toxic && saturated`：直接 suppress，该侧本 tick 不下单

所以当前 OFI 的角色是：
- 让同侧加仓“更便宜才买”
- 不允许覆盖 `pair_cost-first` 主目标
- 不误伤真正的 pairing buy

### 4.5 高失衡准入收紧（不是硬 brake）

当 `abs(net_diff) >= 10` 时，`pair_arb` 不会硬性冻结主仓侧 build。  
它做的是更严格的候选准入：

- 仅对 `same-side risk-increasing buy` 生效
- `pairing / risk-reducing buy` 完全不受影响
- 当前默认门槛：
  - `min_utility_delta >= 2.0 * bid_size * tick`
  - 且 `projected_open_edge` 至少改善 `0.5 * bid_size * tick`

语义：
- 不是“不许继续均价下修”
- 而是“高失衡区只允许明显更值得的同侧 build 存在”

### 4.6 `VWAP ceiling`

这是当前 `pair_arb` 的硬价格保护。

它不再使用旧的静态 ceiling：
- `pair_target - opposite_avg`

而是用“本次成交后投影均价”反推最高允许 bid：
- `yes_ceiling = vwap_ceiling(pair_target, no_avg, yes_qty, yes_avg, bid_size)`
- `no_ceiling = vwap_ceiling(pair_target, yes_avg, no_qty, no_avg, bid_size)`

语义：
- 若同侧当前无仓位（`same_qty <= 0`），自动退化回旧公式
- 若同侧已有更低成本仓位，`VWAP ceiling` 会比旧公式更宽，释放原本被白白浪费的 pair-cost 空间
- 若 `vwap_ceiling <= tick`，该侧本 tick 禁挂

重要优先级：
- `tier avg-cost cap` 先于 `VWAP ceiling`
- `VWAP ceiling` 先于 maker clamp

所以：
- 主仓侧高位继续加仓，仍会先被 tier cap 压住
- 缺失侧补配对，则可以吃到 `VWAP ceiling` 带来的更宽空间

### 4.7 `simulate_buy`

最终候选还要经过投影过滤。

优先改善项：
- 提升 `paired_locked_pnl`
- 降低 `pair_cost`
- 让投影 `pair_cost` 达到目标线

风险增加单还要额外满足：
- `utility_delta >= bid_size * tick_size`
- `projected_open_edge > current_open_edge`

这一步确保策略不会为了“还没坏到爆”就继续盲目抬仓。

## 5. 一个具体例子

下面用一个简化例子说明它怎么工作。

### 5.1 开始时库存平衡

假设观察期结束后：
- `mid_yes = 0.47`
- `mid_no = 0.50`
- `pair_target = 0.98`
- `net_diff = 0`

此时：
- 双边都可能产生 `Buy` 意图
- skew 很轻
- 没有主仓侧 avg-cost cap
- 只要候选通过 `VWAP ceiling + simulate_buy`，就会挂 `YesBuy` 和/或 `NoBuy`

### 5.2 先成交一笔 YES

假设先成交：
- `YES 5 @ 0.46`

此时：
- `yes_qty = 5`
- `yes_avg = 0.46`
- `net_diff = +5`

下一次 tick：
- 策略进入 `YES-heavy` 的 Tier 1
- 同侧 `YesBuy` 会被 `yes_avg * PM_PAIR_ARB_TIER_1_MULT` 约束
- 在当前默认值下，就是 `0.46 * 0.80 = 0.368`
- 缺失侧 `NoBuy` 不受这个 cap 约束，更可能成为优先候选

如果这时盘口让 `NO` 很便宜，系统会倾向继续挂 `NoBuy` 去补配对。

### 5.3 后面又成交一笔 NO

假设又成交：
- `NO 5 @ 0.49`

此时：
- `net_diff` 回到接近 `0`

接下来会发生两件事：

1. 策略层重新按当前状态计算
- skew 回到低强度区间
- 主仓侧 tier cap 也会自然放松

2. 执行层重新治理书上的旧单
- 已经挂着的 `YesBuy` 不会因为“历史上你曾经 `YES-heavy`”而被永久压制
- 但它也不会无条件保留
- 执行层会根据当前新的 intent、走廊和 keep-if-safe 规则决定：
  - 继续留着
  - 重报价
  - 或清掉

所以当前系统不是“配平后重新开一个新阶段状态机”，而是：
- 策略状态完全由当前库存决定
- 订单状态由共享执行层持续治理

## 6. Round Suitability Gate（15m）

`pair_arb` 在 `15m` 主验证市场上，先做 `60s` 观察窗口：

- 观察期内只收集 `mid_yes / mid_no` 样本，不发单
- 60 秒后用中位数判定

触发 `SkippedImbalanced` 的条件：
- `min(median_mid_yes, median_mid_no) < 0.20`
- 且 `median_mid_yes + median_mid_no >= pair_target + 0.01`

触发后：
- 本轮整轮静默
- 不中途恢复

这里要特别强调：
- 这不是一个“证明本轮不可能做出低 pair_cost”的数学定理
- 也不是在说 `YES + NO > 1` 就一定没有 edge

它只是一个当前版本的**极端轮次 veto 启发式**：
- 如果开盘后整整 `60s`，一侧长期低于 `0.20`
- 同时双边中位价和仍然偏高
- 那么系统把这种 round 视为“早期就强单边、对子成本恢复概率差”的轮次，直接跳过

换句话说，`pair_arb` 的真正盈利方式仍然是：
- 允许先后腿成交
- 通过时间上的错位去做更低的 pair cost

当前这个 gate 只是为了避免在“开盘就极端失衡”的轮次里过早背上单腿风险，不是为了否定时序配对本身。

## 7. `locked_pnl > 0` 的语义

`locked_pnl > 0` 不等于停手。

自动化系统在已锁利后仍可继续买入，只要新买入仍通过：
- `VWAP ceiling`
- `max_net_diff`
- `simulate_buy` 的改善条件

真正让策略停止的是：
- 没有候选通过过滤
- 命中库存上限
- 命中 `SkippedImbalanced`

## 8. Endgame 与执行层边界

当前 `pair_arb` 不再完全绕开 endgame。

它接入的是**最小 SoftClose**：
- 仅阻断 `same-side risk-increasing buy`
- 继续允许 `pairing / risk-reducing buy`
- 不启用：
  - `HardClose`
  - `ForceTaker`
  - 市价去风险

当前验证基线里：
- `PM_ENDGAME_SOFT_CLOSE_SECS=45`
- 含义是最后 `45s` 停止危险加仓，但继续允许补配对

## 9. OFI 与执行层边界

当前主线中：
- OFI 由 `pair_arb` 策略层自己消费
- 执行层不再对 `pair_arb` 的 normal-phase buy 做常态 OFI 硬拦截
- stale / inventory / outcome floor / round suitability / endgame 仍由共享层处理

也就是说：
- OFI 在 `pair_arb` 里是价格塑形器，不是常态一刀切 gate
- `pairing / risk-reducing buy` 不会因为 OFI 被误伤

但这里还有一个容易混淆的边界：
- “pairing buy 不受 OFI 抑制”
- 不等于
- “已经挂在书上的 pairing 买单永远不需要下调”

如果市场继续朝有利于该侧买入的方向走：
- 更低的价格意味着更好的 pair cost
- 这时是否保留旧单、还是按更低目标重报价，不由 OFI 决定
- 它属于共享执行层的 retain / republish 问题

所以正确理解是：
- OFI 不负责阻断 pairing side
- 但执行层仍然可以在目标明显下移时，把旧的 pairing 买单下调到更合理的位置
- 这两件事不能混为一谈

## 10. 关键日志怎么读

### 10.1 `PairArbGate(30s)`

它是当前 `pair_arb` 的主要策略观测窗口，核心字段：
- `attempts`
- `keep`
- `skip(inv/sim/util/edge)`
- `ofi(softened/suppressed)`

解释：
- `attempts`：候选数量
- `keep`：真正通过整条价格链与 `simulate_buy` 的数量
- `skip_inv`：被库存硬门槛挡掉
- `skip_sim`：`simulate_buy` 没通过
- `skip_util`：utility 改善不够
- `skip_edge`：open edge 没改善
- `ofi_softened/suppressed`：OFI 只对同侧风险增加单产生了多少软塑形/抑制

### 9.2 `LIVE_OBS`

当前 `pair_arb` 的 `LIVE_OBS` 不再把“热”简单等价成“危险”。

现阶段重点看：
- `replace_ratio`
- `reprice_ratio`
- `ref_blocked_ms`
- `heat_events`
- `pair_arb_softened_ratio`

当前语义下：
- `heat_events` 高，不代表系统必须停
- 更值得看的是 `pair_arb_softened_ratio`
  - 它表示 OFI 实际有多大比例真的在塑形 `pair_arb`

### 10.3 `state-change republish`

`pair_arb` 的旧单重检不再跟着每个 partial fill 跑。  
当前只看一个简化状态签名：

- `dominant_side`
- `|net_diff|` 所在 bucket：
  - `Flat`
  - `Low (<5)`
  - `Mid (5~10)`
  - `High (>=10)`
- `soft_close_active`

只有这个状态签名变化时，系统才会重新判断当前 live quote 是否还满足：
- tier cap
- 高失衡准入
- `VWAP ceiling`
- `SoftClose`
- OFI suppress

如果不再满足，执行层会优先走 `Republish`，而不是让旧单长期按旧状态滞留。

## 11. 推荐验证参数（当前默认）

- `PM_STRATEGY=pair_arb`
- `POLYMARKET_MARKET_SLUG=btc-updown-15m`
- `PM_BID_SIZE=5`
- `PM_MAX_NET_DIFF=15`
- `PM_PAIR_TARGET=0.98`
- `PM_AS_SKEW_FACTOR=0.06`
- `PM_AS_TIME_DECAY_K=1.0`
- `PM_PAIR_ARB_TIER_1_MULT=0.80`
- `PM_PAIR_ARB_TIER_2_MULT=0.60`
- `PM_ENDGAME_SOFT_CLOSE_SECS=45`

## 12. 验证顺序

1. `5m` 冒烟 2-3 轮
- 只验证机制：无 ghost order、无异常拒单连锁、无生命周期失控

2. `15m dry-run` 5 个完整 round
- 核对 `SkippedImbalanced` 是否只在真正失衡 round 触发
- 核对前段 skew 是否明显低于旧版本
- 核对 Tier 1/2 封顶是否生效
- 核对 `PairArbGate(30s)` 与 `LIVE_OBS` 是否稳定

3. 小额 `15m live` 10 个 round
- 观察 `locked_pnl` 中位数
- 观察是否仍出现主仓侧高位加仓
- 观察残仓损失是否改善
