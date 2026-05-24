# B55 / CE25 Fair Price Source Manifest Review - 2026-05-24

## Decision

`UNKNOWN_B55_CE25_FAIR_PRICE_SOURCE_MANIFEST_PROBABILITY_BLOCKED`.

本轮只做本地源码能力审查，不启动 local agg、shared-ingress、shadow、canary 或任何交易路径。结论很直接：现有 local price aggregation 是有价值的行情底座，但还不是 b55/ce25 可验证策略所需的 fair-probability source manifest。

Artifact:

`xuan_research_artifacts/xuan_b55_ce25_fair_price_source_manifest_review_20260524T054828Z/manifest.json`

## What Exists

仓库里已有这些可复用能力：

- `src/bin/polymarket_v2.rs` 有 `LocalPriceAggProbe`、`LocalPriceAggSourcesProbe`、`LocalPriceAggBoundaryProbe`。
- 支持的 source 包括 `binance`、`bybit`、`okx`、`coinbase`、`hyperliquid`。
- probe/schema 能表达 `source_count`、source spread、source timing / age、open/close price、boundary tape。
- `scripts/search_local_price_agg_models.py`、`scripts/tune_local_price_agg.py`、`scripts/build_local_agg_boundary_dataset.py`、`scripts/evaluate_local_agg_boundary_router.py` 可以解析 local agg / RTDS 对比和 boundary 相关数据。

这些能力足够支撑 `fair_spot_mid`、`source_count`、`source_names`、`source_dispersion_bps`、`max_source_age_ms` 的来源设计。

## What Is Missing

缺的是 b55 风格入场的核心，不是行情字段：

- `fair_probability`
- `fair_probability_uncertainty`
- `edge_after_fee_and_uncertainty`
- 明确的 `model_name` / `model_version`
- per-trade fee/liquidity role join
- entry window trade-to-fair-source join

现有 uncertainty gate 更像 source quality / decision filter，不等于 `-15m..-1m` 入场时的 Polymarket outcome fair win probability。

## Interpretation

b55/ce25 线不能退化成 `35c..90c` price cap，也不能只看 BTC/ETH 方向。必须回答的是：当前 Polymarket 60c/70c/85c 是否低于真实胜率，且扣除 fee、source uncertainty、pair-cost tail 和 residual stress 后仍有 edge。

所以当前状态是 UNKNOWN，不是 DISCARD。底层 multi-source spot 能力存在，但 fair-probability model 还没有被定义或产出。

## Next Action

实现 local-only `b55_ce25_fair_probability_model_spec_v1`：定义一个 default-off 概率模型合同，输入必须包括 market boundary、time-to-expiry、多源 `fair_spot_mid`、source age/spread、显式 volatility/uncertainty、per-trade fee/liquidity role，并要求在 b55 主线和 ce25 对照组上都能 fail-closed 评估。没有这些输入之前，不启动 shadow。
