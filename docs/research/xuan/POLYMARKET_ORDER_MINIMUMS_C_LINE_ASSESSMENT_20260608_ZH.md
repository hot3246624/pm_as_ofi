# Polymarket Order Minimums - C-line assessment - 2026-06-08

## 结论

本轮把用户同步的订单最小额规则纳入 C-line 风控解释：

- Market/FOK/FAK BUY 按 dollar amount 表达；Market/FOK/FAK SELL 按 shares 表达。官方 CLOB order docs 明确 FOK/FAK 是 market order types，BUY 指定美元金额、SELL 指定卖出份数。
- Limit/GTC/GTD 是 resting limit order；官方 orderbook/public methods 文档暴露 `min_order_size` 字段，示例为 `"5"`，含义是该市场最小订单大小。
- 用户同步的 operational rule `market order minimum = 1 USDC` 应作为执行侧硬 guard：任何 market/FOK/FAK BUY 的 quote amount 必须 `>= 1.00`，否则 fail closed；market/FOK/FAK SELL 也必须按 notional/venue validation 做 preflight，不得把小额 dust close 当作一定可成交。
- `limit order minimum = 5 shares` 与 C139/C169 的 sub-5 residual 问题一致：4.999-ish position 不能用常规 limit close-only SELL 证明闭环。

## 对 C139/C169 的解释

C139 与 C169 都出现了接近但小于 5 shares 的 partial fill：

- C139 filled_qty=4.999022，剩余 losing/current_value=0 dust 无法证明 close-only SELL。
- C169 filled_qty=4.999229，open remainder cancel not confirmed，状态机在未完成 cancel/open-order reconciliation 前停止 top-up/SELL。

这不是单纯策略预测错误，而是 execution-chain canary 暴露的最小订单/partial-fill 闭环风险。只要 residual < 5，limit close path 不应被视为可用；若要通过 market/FOK/FAK close 或 top-up 处理，必须额外满足 market minimum quote/notional guard，且要在 live in-play 时完成，不能依赖赛后 losing dust redeem。

## 对下一版 canary 的要求

C175 或后续 successor 必须显式加入：

1. `limit_min_shares = book.min_order_size`，通常为 5；所有 GTC/GTD/post-only limit BUY/SELL size 必须 `>= limit_min_shares`。
2. `market_min_quote_usdc = 1.00`，所有 market/FOK/FAK BUY quote amount 必须 `>= 1.00`。
3. 对 market/FOK/FAK SELL，必须在 preflight 中计算 `sell_size * worst_price` 与 venue min validation；不满足时 classify `PLUMBING_ONLY_NO_CLOSE` 或 fail closed，不得 claim closed-loop.
4. 如果 initial limit BUY partial fill 后 `0 < filled_qty < 5`，top-up 只能在 market/FAK minimum、spend cap、position cap、order-count cap 都满足时执行；否则停止，不得尝试不可验证 close。
5. 任何 residual cleanup/redeem 不得替代 live in-play close proof。

## Sources

- Official order docs: https://docs.polymarket.com/trading/orders/overview
- Official create-order docs: https://docs.polymarket.com/trading/orders/create
- Official orderbook docs: https://docs.polymarket.com/trading/orderbook
- Official public methods docs: https://docs.polymarket.com/trading/clients/public

## Readiness status

该规则评估不是 live readiness、strategy promotion readiness、deployable claim，也不是 canary approval。它只是把 C139/C169 暴露的问题固化为后续 execution guard 与 scorecard 解释。
