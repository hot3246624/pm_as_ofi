# Polymarket CLOB V2 升级计划（2026-04-28 Cutover）

## Summary
目标是在 **2026-04-28 19:00 Asia/Shanghai** 前，把仓库从当前 V1 执行语义切到 **CLOB V2 可运行状态**，并在 cutover 当天以最小风险完成切换。

当前判断：
- **真实交易执行链路：高风险，必须迁移**
  - 当前仓库仍依赖 `polymarket-client-sdk v0.4.3` 的 V1 风格订单、fee、nonce、taker 语义。
- **本地价格聚合器 / shadow 研究链：低风险**
  - 主要依赖 RTDS、market WS、公开 book/trade 数据，不直接依赖 V1 order struct。
- **2026-04-21 relayer 变更：低风险**
  - 当前 safe/claim 路径本来就是 `transactionID -> /transaction` 轮询模式，不依赖同步返回 `transactionHash`。

默认策略：
- cutover 前不恢复任何真钱交易优化，先完成 V2 迁移与 `clob-v2.polymarket.com` smoke test。
- 若 V2 交易链任一核心链路未通过，则 **live 保持关闭**，只保留数据链与 shadow。

## Key Changes
### 1. CLOB V2 执行迁移
- 升级到官方 **Rust V2 SDK**，不再继续使用当前 V1 语义执行路径。
- 订单构造统一迁到 V2：
  - 移除对 `feeRateBps / nonce / taker` 的依赖
  - 切到 V2 exchange domain/version
  - builder 路径统一改为 `builderCode`
- 交易执行只保留一条 V2 主路径，不长期维护 V1/V2 双栈。
- 在 smoke 阶段支持切 host 到 `https://clob-v2.polymarket.com`，cutover 后回到 `https://clob.polymarket.com`。
- 主要受影响路径：
  - `src/polymarket/executor.rs`
  - `src/bin/polymarket_v2.rs`

### 2. pUSD / Allowance / Claim / Merge 迁移
- 把交易 preflight 从“USDC/USDC.e”语义全面切到 **pUSD collateral**：
  - balance / allowance 日志、fatal guard、错误文案、headroom 命名全部更新
  - 不再保留“approve USDC”之类 V1 文案
- 校验并更新 `contract_config(...)` 驱动的：
  - exchange
  - neg-risk exchange
  - collateral
  - conditional tokens / adapter
- `claim / merge / redeem` 全部按 V2 合约与 pUSD collateral 路径校验。
- 若 API-only trader 需要 wrap，加入独立 **wrap readiness preflight**，不把问题留到 live 失败后才发现。
- 主要受影响路径：
  - `src/bin/polymarket_v2.rs`
  - `src/polymarket/claims.rs`

### 3. 2026-04-21 Relayer 变更对齐
- 保持当前 safe/claim 提交流程：
  - `POST /submit` 只取 `transactionID`
  - 后续统一轮询 `GET /transaction`
- 不再假设 `/submit` 会同步返回 `transactionHash`
- 仅补充回归验证与日志确认，不做架构改写

### 4. Cutover Day 运行策略
- **18:30 Asia/Shanghai** 进入冻结窗口：
  - 停止 live maker/taker 发单
  - 不保留任何期待跨 cutover 持续有效的 open order
  - 数据采集、RTDS、local-agg shadow 可继续运行
- **19:00~20:00 Asia/Shanghai**
  - 默认视为交易暂停期
  - 不做自动恢复 live
- **维护结束后恢复顺序固定**
  1. `clob-v2` / production host 连通性与 auth smoke
  2. pUSD balance/allowance preflight
  3. maker 下单 smoke
  4. taker/FAK smoke
  5. claim/merge/redeem smoke
  6. 最后恢复 live 策略
- 任一步失败：
  - live 保持关闭
  - 仅保留数据链与 shadow
  - 不边跑边修

## Test Plan
1. **V2 SDK / 交易执行**
- `cargo check` 与核心测试通过
- 在 `https://clob-v2.polymarket.com` 上完成：
  - `balance_allowance`
  - order book fetch
  - maker post-only order
  - taker/FAK order
  - cancel / cancel-all
- 验证订单构造不再依赖 `feeRateBps / nonce / taker`

### 恢复后最小 smoke 顺序

先跑只读 smoke，不直接碰主策略：

```bash
cargo run --quiet --bin probe_clob_v2
```

期望先看到：

1. `auth_ok`
2. `allowance_update_ok` 或明确的 allowance 更新告警
3. `collateral balance_raw=...`
4. 如已提供 `POLYMARKET_YES_ASSET_ID / POLYMARKET_NO_ASSET_ID`，还能看到两侧 `book ... best_bid / best_ask`

只有在以下条件同时满足时，才允许最小真实单 smoke：

1. `pUSD` balance 非零
2. exchange / adapter allowance 非零
3. 明确知道当前 `PM_SIGNATURE_TYPE` 与 `POLYMARKET_FUNDER_ADDRESS` 匹配

最小真实单 smoke 通过独立 probe 控制，不走主策略：

```bash
PM_V2_SMOKE_PLACE_ORDER=true \
PM_V2_SMOKE_SIDE=yes \
PM_V2_SMOKE_DIRECTION=buy \
PM_V2_SMOKE_PRICE=0.01 \
PM_V2_SMOKE_SIZE=5 \
PM_V2_SMOKE_ORDER_TYPE=GTC \
PM_V2_SMOKE_POST_ONLY=true \
cargo run --quiet --bin probe_clob_v2
```

若只是验证下单链路，建议继续保持：

- 极小名义金额
- `post_only=true`
- 成功后立即 `cancel_all`

2. **Collateral / claim / merge**
- pUSD collateral 日志、fatal guard、allowance 语义正确
- allowance 缺失时提示 wrap/pUSD，而不是 USDC.e 旧文案
- merge / redeem / safe relayer 至少完成一轮 dry smoke
- 4/21 relayer 变更回归：
  - `/submit` 只返回 `transactionID` 时仍能正常轮询完成

3. **Cutover 后验收**
- cutover 后第一轮 smoke：
  - live 执行路径通过
  - local-agg / RTDS / challenger lab 不回退
- 若 live fail 但数据链成功：
  - 允许继续 shadow
  - 禁止恢复真钱交易

## Assumptions
- 本计划中的 cutover 时间以 **2026-04-28 19:00 Asia/Shanghai** 为基准，对应官方约 `11:00 UTC`。
- 官方 Rust V2 SDK 可用；若实际升级阻塞，则执行 fallback：
  - 暂停 live
  - 保留 shadow
  - 不在 cutover 当天强行恢复真钱交易
- 2026-04-21 relayer 变更不构成主要阻塞，因为当前代码已按 `transactionID -> /transaction` 模式工作。
