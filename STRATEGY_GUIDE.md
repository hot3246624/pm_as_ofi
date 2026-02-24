# 策略文档迁移说明

本文件已改为入口页，避免出现多份策略文档长期漂移。

请使用以下主文档：

- `docs/strategy_guide.md`

该文档已与当前 `src/bin/polymarket_v2.rs`、`src/polymarket/*` 的实际实现对齐，包含：

- 状态机（Balanced / Hedge / Global Kill）
- `can_open` 风控门控
- Maker-only 下单与成交生命周期
- 小资金参数建议
