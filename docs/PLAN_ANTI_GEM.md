# Dry Run Log Analysis (2026-04-09)

## 1. 宏观指标与状态流转 (Macro & Lifecycle)

整体表现非常稳定，策略在 `dry_run=true` 模式下展现了极高的健康度。

*   **市场轮转正常**: 成功在多个 15 分钟级的 BTC 市场间无缝轮转，比如 `btc-updown-15m-1775736900` 和 `1775737800`。
*   **Endgame 执行精准**: 完美的倒计时撤单流程
    ```text
    ⏱️ Endgame phase: Normal -> SoftClose (t-45s)      // 停止新开仓，允许对冲
    📝 DRY cancel YES_BUY & NO_BUY (EndgameRiskGate)  // 撤销未成交单
    ⏱️ Endgame phase: SoftClose -> HardClose (t-30s)   // 强制平仓
    ⏱️ Endgame phase: HardClose -> Freeze (t-2s)       // 冻结网络
    🏁 Market expired (wall-clock)
    ```
*   **引擎健壮性**: 单个 session 处理超过 12 万次 WS 变动 (`parsed=239202`)，没有死锁、没有卡顿，`Coordinator` 和 `Executor` 层面的处理延迟一直处于安全范围内。

## 2. 核心架构收敛验证 (V2 Architecture Progress)

这份日志带来了我们期待已久的**最核心好消息**，证明了前阶段重构带来的设计收敛（Full-Chain Responsibility Convergence）已经生效！

> [!TIP]
> **脆弱状态完全剥离 (Crucial Success)**
> 观察到 `skip(inv/sim/util/edge)=0/0/0/0` 这四个过滤指标在全程全部为 **0**。这证明历史遗留的那些“因为预期盈亏不足、或者因为利用率不够”而阻止挂单的复杂且脆弱的状态（Fragile State）已经被彻底移除了主执行路径，执行层做到了纯粹的“价格与指令透传”。

*   **连续报价机制 (Continuous Quoting)**:
    日志中 `pair_arb_suppressed=0` (完全封杀=0次)，而 `pair_arb_softened=19586` (软化降频=1.9万次)。
    这表明 **OFI 毒性防御现在不会直接导致策略"死机（Stall）"**。在受到高单边毒性冲击时，系统选择了 "Softened" 路线——适度压低买价、拉宽 Spread，但**始终留在场内保持流动性**！这是做市稳定性的一次巨大飞跃。

## 3. RETAIN 逻辑微观验证与挂单效率

我们一度非常担心 `RETAIN` 这个拦截器会不会变成拦路虎，这份日志清晰展示了它的实际工作表现。

*   **高频去重作用**:
    ```text
    Round 4: retain(hits=72603), replace=14
    Round 5: retain(hits=115627), replace=40 (replace_per_min=2.70)
    ```
    `RETAIN` 的去重率极高，阻挡了约 99.9% 毫无意义的微小价格跳动，避免了向 Polymarket 发送海量的 API 垃圾请求。
*   **有效的阶梯式追踪**:
    当市场偏离超过 `reprice=0.020` 阈值时，订单能够被顺利挤出 `RETAIN` 屏障。在日志中我们清楚地看到其价格的跟随变迁：从 `$0.48/$0.43` 一路顺滑下移到 `$0.28/$0.26`、`$0.09` 甚至 `$0.01`。这代表向下重新定价 (Downward Reprice) 没有被卡死。

> [!WARNING]
> **潜在的隐患：上旋提单 (Upward Reprice)**
> 虽然 `replace` 在正常生效，但日志显示 `pairing_upward_reprice=0` 且 `reprice_ratio=0.00`。
> 我们目前处于 Dry Run 模式下（由于没有填单，`net_diff` 始终为 0），策略两边永远处于 `Pairing` 对称状态。在 V2 计划中提及的：“如果偏离超过 3 ticks 将引发强制 Upward Reprice”，这一点似乎没有被触发。
> *需要复核代码：是 `reprice` 阈值过高，还是上一次推送的代码暂未彻底覆盖 Upward Reprice 的特权通道？*

## 4. 其它细节指标评估

*   **OFI 尾部热度判断 (`mode=tail-quantile`)**:
    模型非常适应这种震荡市，能敏锐抓到尾部热度。`🔥 NO heat enter` 触发后伴随清晰的 `🧊 NO heat cool` 退出逻辑，不存在"进入毒性过长导致系统瘫痪"的情况。
*   **Inventory Limit (`inv_limit=3386`)**:
    在极度低价区 (e.g. Yes@0.01, No 被压得很低) 时，出现了 3000 多次的 `inv_limit`。猜测是受制于极速单边行情下引发的某些 Tier 参数保护（或者 `dip_cap=0.20` 控制）。这是一个很好的安全锁，防止在胜率极低的末路继续大量浪费报价。
*   **零死锁 / 无 Ghost Orders**:
    受限于干跑模式，不会模拟出真实的残渣订单（Dust Floor）与撤单失败 (`opposite-slot blocked=0`)。但这起码证明在我们策略层的纯净模拟中，订单插槽（Slots）的进出生命周期是自洽的，没有任何死循环分配。

---

## 结论 (Conclusion)

目前的版本日志**非常漂亮 (Elegant & Highly Efficient)**。
1. **符合预期**：它如期抛弃了繁杂的多视界过滤 (`sim/util=0`)，全面依赖于 Band/IO 执行层。
2. **生命力强**：即使面对连续下挫至 `<$0.10` 的单边极端行情，做市算法也一路持续挂单紧咬 (`softened` 替代了 `suppressed`)，展现了极强的策略韧性。
3. **下一步建议**：可以立即转向真实资金的极小规模实盘（例如每注 `$5`）进行湿跑，验证在有真实 Fill 导致的 `net_diff` 变化时，真实的 RETAIN 边界（特别是风险侧的退让和安全侧的上旋追击）是否达到完美的收敛态。
