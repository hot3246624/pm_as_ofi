# Oracle-Lag Sniping 实盘就绪评估

> 评估时间: 2026-04-21 10:30 BJT  
> 代码版本: `v1.1.0-stable` (commit `11141f4`)  
> 数据来源: `docs/stage_g_multi_dryrun.csv` (831 轮) + `docs/oracle_lag_dryrun_2026-04-21.csv` (28 轮)  

---

## 1. Oracle 准确性 ✅ PASS

| 指标 | 数值 | 判定 |
|------|------|------|
| 总轮次 | 831 | — |
| 数据源 | 100% Chainlink RTDS | ✅ 无 Gamma fallback |
| `winner_match_logged` = true | **831/831 (100%)** | ✅ 完美 |
| `precision_source` = `chainlink_result_ready` | 816/831 (98.2%) | ✅ |
| `precision_source` = `post_close_emit_winner_hint` | 15/831 (1.8%) | ⚠️ 可接受（偶发缓存命中） |
| 涵盖币种 | 7 (bnb, btc, doge, eth, hype, sol, xrp) | ✅ |
| 每币轮次 | 118-119（均匀） | ✅ 无市场偏斜 |
| Yes/No 分布 | Yes 431 / No 400 (52%/48%) | ✅ 近似均匀 |

> [!TIP]
> **核心结论**：Chainlink RTDS 在连续 831 轮中, winner 判定与链上价格 delta 100% 吻合。这是 oracle_lag_sniping 策略最关键的前提假设，已被充分验证。

---

## 2. 延迟分析 ✅ PASS

### 2.1 `latency_from_end_ms`（收盘→hint 到达）

| 百分位 | 毫秒 | 判定 |
|--------|------|------|
| p5 | 1,052 | — |
| p25 | 1,186 | — |
| **p50** | **1,376** | ✅ 远低于 1.5s 目标 |
| p75 | 1,569 | ⚠️ 略超 1.5s |
| **p95** | **2,007** | ⚠️ 需关注 |
| max | 4,236 | ⚠️ 极端值（1 轮） |

### 2.2 延迟分桶

| 区间 | 轮次 | 占比 |
|------|------|------|
| ≤ 1.0s | 15 | 1.8% |
| 1.0 - 1.2s | 216 | 26.0% |
| **1.2 - 1.5s** | **340** | **40.9%** |
| 1.5 - 2.0s | 216 | 26.0% |
| > 2.0s | 44 | 5.3% |

> [!NOTE]
> **68.7% 的轮次在 1.5s 内完成 hint 检测**。考虑到 Polymarket 盘后窗口长达 105s，且竞争对手（如 goyslop1）的成交记录显示他们在关盘后 4-10 秒才开始吃单，我们的 1.4s 中位延迟具有显著的时间优势。
>
> **p95 = 2.0s** 的长尾主要来自 Chainlink WS 的推送间隔波动，不影响策略安全性（最多让该轮慢 0.5s 进场）。

---

## 3. 执行安全机制 ✅ PASS

### 3.1 关键安全带

| 机制 | 实现状态 | 代码位置 |
|------|----------|----------|
| **Slug Lock（TCP loopback）** | ✅ 已实现 | `polymarket_v2.rs:6449-6469` |
| **FAK 单轮 1-shot cap** | ✅ `>= 1` 即停 | `coordinator_order_io.rs:1247` |
| **Taker 价格门槛** | ✅ `ORACLE_LAG_NO_TAKER_ABOVE_PRICE = 0.990` | `coordinator.rs:55` |
| **Dry-run 模式守卫** | ✅ `dry_taker_preview` 日志确认 | 最新日志验证 |
| **Endgame SoftClose** | ✅ 收盘前 45s 阻断 risk-increasing | `coordinator_endgame.rs` |
| **Post-close window** | ✅ 105s 硬限 | `.env: PM_POST_CLOSE_WINDOW_SECS=105` |

### 3.2 FAK 执行路径审查

```
WinnerHint 到达
  → winner_bid <= 0.990? 
    → YES: FAK at 0.99 limit, size=5.00, 单轮最多 1 shot
    → NO (bid > 0.990): 转 Maker 路径（GTC at 0.991）
  → Dry-run? 
    → YES: 输出 dry_taker_preview, 不真实下单
    → NO: POST /order 真实提交
```

> [!IMPORTANT]
> **ORACLE_LAG_NO_TAKER_ABOVE_PRICE = 0.990**：当 winner_bid > 0.990 时（即盘口已经很贵），系统**不会发 FAK**，而是退回 Maker 路径挂单。这是防止在高价位白付 taker fee 的关键保护。
>
> 从 goyslop1 的交易分析中我们确认：99¢ 的 GTC 挂单在 Polymarket 是 **Maker 免手续费**的，而 FAK 吃 99¢ 的单子需要付 0.7% taker fee（净利润仅约 0.3%/份）。当前阈值设计合理。

### 3.3 最新 Dry-Run 行为验证

从 `supervisor-20260421-093236.log` 确认：
- ✅ 42 个 Round 正常运行
- ✅ 5 次 `dry_taker_preview` 日志（FAK 路径触发正确）
- ✅ `WinnerHint` 全部来自 Chainlink
- ✅ 仲裁器已**关闭**（`PM_ORACLE_LAG_CROSS_MARKET_ARBITER_ENABLED=false`）
- ✅ 每市场独立 single-shot 模式

---

## 4. 配置审计 ✅ PASS

### 4.1 当前 `.env` 关键参数

| 参数 | 当前值 | 评价 |
|------|--------|------|
| `PM_STRATEGY` | `oracle_lag_sniping` | ✅ |
| `PM_DRY_RUN` | `true` | ✅ 安全状态 |
| `PM_BID_SIZE` | `5.0` | ✅ 保守（$5/shot） |
| `PM_INPROC_SUPERVISOR` | `1`（通过脚本） | ✅ 单进程多任务 |
| `PM_MULTI_MARKET_PREFIXES` | 7 币种 | ✅ |
| `PM_POST_CLOSE_WINDOW_SECS` | `105` | ✅ 充足窗口 |
| `PM_POST_CLOSE_CHAINLINK_WS_URL` | `wss://ws-live-data.polymarket.com` | ✅ |
| `PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS` | `8` | ✅ |
| `PM_ORACLE_LAG_CROSS_MARKET_ARBITER_ENABLED` | `false` | ✅ 保守默认 |
| `PM_ORACLE_LAG_DRYRUN_ALLOW_FALLBACK_OPEN` | `false` | ✅ 不依赖前端数据 |

### 4.2 建议实盘参数

```bash
# 仅需修改这一行
PM_DRY_RUN=false

# 其余参数保持不变
PM_BID_SIZE=5.0          # 保守起步
PM_STRATEGY=oracle_lag_sniping
```

---

## 5. 跨市场仲裁器 (Arbiter) 状态

| 维度 | 状态 |
|------|------|
| 代码实现 | ✅ 完成（`run_cross_market_hint_arbiter`） |
| Dry-run 验证 | ✅ 7 轮全部通过（来自 PLAN_Claude_temp.md 记录） |
| 当前启用状态 | ❌ **已关闭**（`cross_market_arbiter_enabled=false`） |
| 推荐 | 先以 single-shot 模式上线，稳定后再启用仲裁 |

> [!NOTE]
> 仲裁器已在之前的干跑中验证了 7 轮（btc/bnb/hype 交替选中），行为正确。但为降低首次实盘风险，建议先在**每市场独立模式**下运行。

---

## 6. 收益预估与风险分析

### 6.1 单轮收益模型

| 场景 | 买入价 | 赔付价 | Taker Fee | 净利润/份 | 5份净利润 |
|------|--------|--------|-----------|-----------|-----------|
| **FAK @ 0.99**（ask < 0.99） | 0.990 | 1.000 | 0.7% (≈0.007) | **$0.003** | **$0.015** |
| **GTC Maker @ 0.99**（ask ≥ 0.99） | 0.990 | 1.000 | 0% (Maker) | **$0.010** | **$0.050** |
| **GTC Maker @ 0.991**（no ask） | 0.991 | 1.000 | 0% (Maker) | **$0.009** | **$0.045** |

### 6.2 预期轮次频率（从 dry-run 推算）

- 每 5 分钟一轮，每天约 **288 轮/市场**
- 7 市场并行 → **~2,016 轮/天**
- 从 dry-run 日志看，`dry_taker_preview` 在 42 轮中触发了 5 次 → 约 **12%** 的轮次有可执行机会
- 保守估计：每天 **~240 次** 下单机会

### 6.3 日收益预估（保守）

| 模式 | 每日机会 | 成交率 | 净利润/笔 | 日收益 |
|------|----------|--------|-----------|--------|
| FAK taker | 60 | 30% | $0.015 | $0.27 |
| GTC maker | 180 | 15% | $0.050 | $1.35 |
| **合计** | — | — | — | **~$1.6/天** |

> [!WARNING]
> **这是极保守的估算**（PM_BID_SIZE=5, 单轮 1 shot）。goyslop1 的数据显示，他单轮投入 $1,700+，一笔赚 $17-$241。规模化是后续优化方向，不影响当前 GO/NO-GO 判断。

### 6.4 风险项

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| Chainlink RTDS 延迟突增 | 低 | 错过窗口 | 105s post-close window 兜底 |
| Winner hint 判定错误 | **极低**（0/831） | 买错方向 | 链上 delta 验证 100% 吻合 |
| FAK 未成交（盘口空） | 中 | 无损失（FAK 自动取消） | 已有 Maker 兜底路径 |
| 双边下单 | **零**（代码保证） | — | 单侧 winner hint 机制 |
| 进程重复 | **零** | — | Slug lock (TCP loopback) |
| API 429 限速 | 低 | 延迟 | 单轮 1 shot cap |

---

## 7. 综合评定

### 检查清单

| # | 检查项 | 状态 | 备注 |
|---|--------|------|------|
| 1 | Oracle 准确性 100% | ✅ | 831/831 |
| 2 | 延迟 p50 < 1.5s | ✅ | 1,376ms |
| 3 | 无异常双边下单 | ✅ | 代码保证 |
| 4 | FAK shot cap 生效 | ✅ | >= 1 即停 |
| 5 | Taker 价格门槛合理 | ✅ | 0.990 |
| 6 | Slug lock 防重复 | ✅ | TCP loopback |
| 7 | Dry-run 连续稳定 | ✅ | 42+ 轮无中断 |
| 8 | 配置参数保守合理 | ✅ | BID_SIZE=5 |
| 9 | 回退策略明确 | ✅ | 改回 DRY_RUN=true |
| 10 | Winner hint 来源可靠 | ✅ | 100% Chainlink |

### 最终判定

> [!IMPORTANT]
> ## 🟢 GO — 具备实盘条件
>
> Oracle-Lag Sniping 策略在经过 **831 轮 × 7 币种** 的 Dry-Run 验证后，所有关键指标均达标：
> - Oracle 准确率 **100%**
> - 延迟中位数 **1.4s**（远优于竞争对手的 4-10s 进场窗口）
> - 执行安全带完备（1 shot cap、0.990 价格门槛、slug lock）
> - 无系统性风险暴露
>
> **建议切实盘的步骤**：
> 1. 将 `.env` 中 `PM_DRY_RUN=true` 改为 `PM_DRY_RUN=false`
> 2. 保持 `PM_BID_SIZE=5.0`（首次实盘保守）
> 3. 保持仲裁器关闭（`PM_ORACLE_LAG_CROSS_MARKET_ARBITER_ENABLED=false`）
> 4. 运行 `bash stop_markets.sh && sleep 2 && bash start_markets.sh`
> 5. 观察首轮 `oracle_lag_submit_latency` 日志确认真实下单
> 6. 若出现异常，立即 `PM_DRY_RUN=true` 回退

---

## 附录：数据源文件

- [stage_g_multi_dryrun.csv](file:///Users/hot/web3Scientist/pm_as_ofi/docs/stage_g_multi_dryrun.csv) — 831 轮核心验证数据
- [oracle_lag_dryrun_2026-04-21.csv](file:///Users/hot/web3Scientist/pm_as_ofi/docs/oracle_lag_dryrun_2026-04-21.csv) — 最新 28 轮细粒度数据
- [ORACLE_LAG_SNIPING_LIVE_CHECKLIST_ZH.md](file:///Users/hot/web3Scientist/pm_as_ofi/docs/ORACLE_LAG_SNIPING_LIVE_CHECKLIST_ZH.md) — 实盘检查清单
