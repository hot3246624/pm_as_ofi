# API 频率与运行建议（V2）

> 本文关注工程侧“如何不打爆接口”，不是官方配额声明页。
> 实际限额请以 Polymarket 官方文档当前值为准。

## 1. 调用面划分

本项目的数据路径：

- 行情：`Market WS`（持续推送）
- 成交：`User WS`（认证推送）
- 交易动作：REST `post_order / cancel_*`

因此常态下 REST 压力主要来自：

- 重报价
- 对冲撤单
- 到期 `CancelAll`

## 2. 减压机制（代码已实现）

- 同侧防抖：`PM_DEBOUNCE_MS`（默认 200ms）
- 重报价阈值：`PM_REPRICE_THRESHOLD`（默认 0.005）
- OFI 毒性熔断：毒性时先撤再等待，不盲目追单
- 市场轮转：先 `CancelAll`，再清理 session

## 3. 实战建议阈值（内部运营）

可先按以下告警线管理：

- 预警：`> 20` 次下单/秒（持续 10 秒）
- 强告警：`> 35` 次下单/秒（持续 10 秒）
- 预警：`> 60` 次撤单/秒（持续 10 秒）

若触发，优先动作：

1. 提高 `PM_DEBOUNCE_MS`（如 200 → 350）
2. 提高 `PM_REPRICE_THRESHOLD`（如 0.005 → 0.008）
3. 降低 `PM_BID_SIZE`

## 4. 观测方式

```bash
# 最近日志
tail -n 200 logs/*.log

# 观察下单/撤单关键字
rg -n "PostOnlyBid|Cancel order|CancelAll|OrderFailed" logs/*.log
```

## 5. 常见误区

- 误区1：只看 REST，不看 WS
  - 真实库存变化来自 User WS，WS异常时 live 应视为不可交易状态
- 误区2：高频重挂一定更赚钱
  - 过低防抖常导致撤单风暴与噪声成交
- 误区3：只看成交率
  - 还要看净仓偏离、熔断频率、失败单占比
