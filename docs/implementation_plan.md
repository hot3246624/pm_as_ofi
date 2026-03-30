# 消除高频发单与修复启动异常延迟 (Round #1~#4 Review)

根据最新 2026-03-30 的四轮日志分析，`replace/placed = 0.56` 有显著好转，且导致换单的原因完全收敛到了 `debt_sync`（14 次）。同时发现 R2 的准备阶段极其缓慢（23.7s）。这两个问题都可以通过极其精简的架构参数和逻辑调整解决。

## Proposed Changes

### 1. 修复 Ready 异常慢路径 (23.7s 延迟)
**根因分析:** 
`ready_elapsed_ms=23773` 的根本原因在于当前系统硬性要求 Polymarket WS 必须收到 **3 个 Book Ticks** (`GLFT_WARM_MIN_BOOK_TICKS = 3`) 才允许进入 Ready 状态。
Polymarket 的机制是典型的静默订单簿（不同于 Binance），如果当前标的（如 ETH-Updown）没有发生挂单撤单，WS 就完全不会推数据。因此，系统就像傻子一样干等了 23.7 秒，直到有 3 个真实用户在盘口发生动作。
**修复方案:**
- 在 `src/polymarket/glft.rs` 中，将 `GLFT_WARM_MIN_BOOK_TICKS` 从 `3` 改为 `1`。
- **效果**: 只要启动时成功通过 REST 抓取并组装了 Initial Snapshot (算作第一次 BookTick 注水)，且 WS 通道处于存活状态，系统就能瞬间进入 Ready。从而把准备时间从 23s 直接压缩到 1~2s 以内。

### 2. 将 Debt 发布从“连续追踪”改成“批量收敛” (Batch Settle)
**根因分析:**
尽管前几版我们限制了在趋势市（Tracking/Guarded）中的追踪，但在 `Aligned` 状态下，`target_follow_threshold = 7.0` 依然在生效。这就造成了行情如果在小范围内缓慢蠕动，一旦达到 7 Ticks，系统就在 `Aligned` 下忠实地追一下。这叫做“连续追踪”(Continuous Tracking)，是产生 `debt_sync=14` 几乎全部来源的原因。
要在 MM 中做到真正的“批量收敛”，我们实际上应该**彻底放弃单纯的目标价追逐**，转而完全交给“结构安全（Structural Debt）”来兜底。
**修复方案:**
- 在 `src/polymarket/coordinator_order_io.rs` 中：
  1. 将 `glft_publish_target_debt_threshold` 函数的阈值统统抬高到极大的值（例如 `99.0`），等同于物理“阉割”日常小步追逐。
  2. 让 `structural_debt_threshold` 成为唯一的触发机关（当前 Aligned 为 `6.0`，Tracking 为 `7.0`）。由于结构债衡量的是**真实挂单与 Trusted Mid 的偏移**或者**危险深度区**，这意味着：无论目标怎么变，只要我当前的挂单没有偏离宏观价值太远，也没有陷入危险流动性，我就**绝对不换单**（赚取被动滑点）。
  3. 一旦偏离过度（例如超过 6 Ticks），触发一次 `Debt` 发布，并直接一次性**批量收敛**到最新的模型目标价 (`shadow_target`)。
- **效果**: 用一个动作斩断所有的微调换单（Micro-Replace）。把 Replace 次数压缩到只在结构性防线被击穿时才发生。

## User Review Required

> [!IMPORTANT]
> - `GLFT_WARM_MIN_BOOK_TICKS` 降为 1 是否符合你的预期？这是彻底解决冷启动读不到行情的标准解法。
> - 直接把 `target_debt_threshold` 设为 `99.0`（废除连续追踪）是一个做市逻辑的根本性转变：我们不再“跟单”，而是“守区间，破区间再跳跃”。请确认这符合您对“批量收敛”的定义。

## Verification Plan
1. `cargo test` 验证状态与阈值更改。
2. 实盘验证（使用 log），查看 `ready_elapsed_ms` 是否稳定回到 < 2500ms 内。
3. 跑 4 轮 5m 观察 `replace/placed`，因为切断了 `target_follow`，该比率预计会直接跌破 0.40 甚至更低。
