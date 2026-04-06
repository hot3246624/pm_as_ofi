评估总结
PLAN_Codex 的方向和核心机制设计都非常好，三大改造（分段 skew / 梯度封顶 / Round Gate）和 pair_arb 代码已有实现可以很好地对接。发现 6 个具体问题：

需立即处理（P0）
.env 参数完全没同步 — 当前 skew=0.15 / max_net_diff=5 / time_decay_k=2.0，但 PLAN_Codex 要求 0.06 / 15 / 1.0。不改就跑等于在旧参数上测试新策略。
需决定（实施前）
三段 skew 的边界定义 — PLAN_Codex 用固定 5/10/15，PLAN_Claude 用 frac=0.5 比例。两者在 max_net_diff=15 时分界点不同（Codex: net_diff=5 开始爬升 vs Claude: net_diff=7.5 开始爬升）。你倾向哪个？

Round Gate 行为 — 首 tick 检测即判定，还是观测 60s 后再判定？一旦触发是整轮静默还是可恢复？

文档清理
4-6. PLAN_Claude Part 4 的 skew=0.20 建议已被 Part 5 的 0.06 取代但未标注；tier cap 与 simulate_buy 的交互虽然正确但需声明；执行层参数（debounce/reprice_threshold）需要明确保持不变或调整。

详细分析请查看 evaluation artifact。请先在上述 2-3 的决策点给出你的倾向，我再进行代码实施。