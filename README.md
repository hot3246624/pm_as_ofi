# pm_as_ofi

Polymarket crypto up/down 市场做市与库存双边配对套利管理引擎。

---

## 1. 核心策略与状态定位

目前仓库的主要验证与研究主线已升级为高频双边配对套利架构：

| 策略标识 | 核心定位 | 状态 | 备注 |
| --- | --- | --- | --- |
| `xuan_b27_dplus` | 优势侧 Maker 建仓 + 限制价差双边对齐配平 + 双阶段早退 | **验证主线** | 通过实盘只读干跑验证 |
| `nagi777_v1` | 完成感知入场 (CAE) + 动态 Clip 分散 + 兜底风控对齐 | **优化主线** | 扫参优化版 (Clip=80, Cap=0.980) |
| `pair_arb` | 基础双边对冲套利（A-S 风格） | 基线 / 备用 | 历史经典款 |
| `glft_mm` | 真双边做市、slot-keyed 挂单、外锚驱动 | 实验 / Challenger | 队列研究线 |

---

## 2. 最近研发成果与就绪状态

我们近期完成了对 `xuan_b27_dplus` 和 `nagi777` 的核心安全和数学对齐升级：

1. **手续费率数学偏差修正**：
   - 将 Rust 回测引擎 ([pair_arb_backtest.rs](file:///Users/hot/web3Scientist/pm_as_ofi/src/bin/pair_arb_backtest.rs)) 与 Python 验证脚本的手续费计算公式完全对齐为官方类目费率口径，即：
     $$\text{fee\_per\_share} = \text{rate} \times \min(p, 1.0 - p)$$
     彻底消除了原先在 $p=0.5$ 附近存在的 2 倍费率少算漏洞。

2. **内置三维物理熔断器 (Circuit Breaker)**：
   - 保护策略在网络拥堵、连接退化或交易所响应滞后时及时避险。在 [xuan_b27_dplus_order_plan.rs](file:///Users/hot/web3Scientist/pm_as_ofi/src/polymarket/xuan_b27_dplus_order_plan.rs) 中实现以下自动熔断：
     * **滚动 Taker 交易失败率** $\ge 5\%$
     * **WebSocket 连接延迟 (Lag)** $\ge 1000\text{ms}$
     * **订单确认 (ACK) 延迟** $\ge 2000\text{ms}$

3. **L2 深度数据验证**：
   - 基于 3.5 天 L2 真实深度的 Unified Fee-Inclusive ledger 回测取得 **+4.21 USDC** 净收益，残仓敞口比 (RER) 严格控制在 **0.3004%**。

---

## 3. 实盘只读干跑 (Dry-Run) 验证指南

本引擎支持以完全安全的**只读/只观测 (Auth-Observer)** 模式运行。全程在 `PM_DRY_RUN=true` 约束下挂载，绝对不提交任何真实订单。

### 步骤 1：启动 Shared Ingress Broker
拉起后台常驻数据网关服务：
```bash
PM_SHARED_INGRESS_IDLE_EXIT_ENABLED=false ./scripts/run_shared_ingress_broker.sh
```
通过 preflight 脚本检查 sockets 状态：
```bash
python3 scripts/xuan_b27_dplus_shared_ingress_preflight.py --root /Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main
```

### 步骤 2：启动 Market WS 只读观测器
```bash
python3 scripts/xuan_b27_dplus_auth_observer_dry_run.py \
  --approved-no-order-observer \
  --duration-seconds 120 \
  --market-slug btc-updown-5m
```

### 步骤 3：启动 User WS 专有链路观测器
```bash
python3 scripts/xuan_b27_dplus_readonly_user_ws_observer.py \
  --approved-readonly-user-ws \
  --duration-seconds 120 \
  --market-slug btc-updown-5m
```

### 步骤 4：运行只读审计检查
运行结束后，对输出文件夹执行 summary 门禁验证，确保未发生任何写动作违规：
```bash
# 审计 Market 观测输出
python3 scripts/xuan_b27_dplus_summarize_observer_run.py xuan_research_artifacts/xuan_b27_dplus_auth_observer_<timestamp>

# 审计 User WS 观测输出
python3 scripts/xuan_b27_dplus_summarize_readonly_user_ws_run.py xuan_research_artifacts/xuan_b27_dplus_user_ws_observer_<timestamp>
```

---

## 4. 项目瘦身与规范声明

为了保持代码库的轻量级和清晰的历史记录，我们在仓库中作了以下清理和优化：
- **移除临时运行文件夹**：所有本地生成的 `temp_*` 临时测试与状态诊断目录已被清理并加入 `.gitignore`，不再提交至版本库。
- **排除海量历史日志/数据库**：将 `xuan_research_artifacts/` 目录从 Git 版本跟踪中移除（本地保留），避免 7GB+ 级别的 SQLite 文件和数千个 JSON 文件污染代码库。

---

## 5. 开发人员建议阅读顺序

1. `docs/README.md`
2. `docs/strategies/XUAN_M0001_MAKER_SHADOW_RUNNER_SPEC_ZH.md` (Maker 队列 shadow 详解)
3. `docs/architecture/STRATEGY_V2_CORE_ZH.md`
4. `docs/strategies/STRATEGY_PAIR_GATED_TRANCHE_V1_1_ZH.md`
5. `docs/runbooks/GO_LIVE_PAIR_ARB_CHECKLIST_ZH.md`
