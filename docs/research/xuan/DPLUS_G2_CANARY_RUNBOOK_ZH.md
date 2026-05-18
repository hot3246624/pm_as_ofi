# DPLUS_G2_CANARY_RUNBOOK_ZH

本文定义 `xuan_b27_dplus` G2 smoke canary 的 exact-run 审批边界、风险上限、停止条件和验收 artifact。本文不是授权；任何 canary、live、order、cancel、redeem、broker、service/systemd、shared-ingress 或 env 修改都必须由主线程再次明确授权 exact run。

## 当前前置证据

- EC2 30m read-only User WS acceptance 已通过：`xuan_research_artifacts/xuan_b27_dplus_ec2_readonly_user_ws_acceptance_20260517T031459Z/local_acceptance_review.json`。
- 该 read-only run 只作为连接、订阅、recorder 和只读安全证据；它不授权 G2 canary。
- G2 canary 前必须先通过 Rust no-order shadow/dry-run 策略验收，再保持本地 source-of-truth、runtime gate、OMS adapter、readiness/status bundle、G2 runbook smoke 和 G2 canary acceptance smoke 全部 PASS。read-only User WS acceptance 不是策略表现验收。
- Rust no-order shadow/dry-run 策略验收由 `scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance.py` 生成真实 artifact，并由 `scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_smoke.sh` 只用本地 fixtures 验证 gate 行为。真实 PASS 必须同时满足 no-order safety、recorder completeness、candidate quality、source-truth UNKNOWN handling 和独立 shadow/replay performance evidence；缺少 performance evidence 时必须 fail closed，不能把 read-only User WS acceptance 或 observer candidate tracking 当作策略表现通过。
- 独立 shadow/replay performance evidence 由 `scripts/xuan_b27_dplus_shadow_performance_evidence.py` 生成，并由 `scripts/xuan_b27_dplus_shadow_performance_evidence_smoke.sh` 覆盖本地 fixtures。它要求安全 observer summary、BTC 5m market scope、足够 edge samples、正向 edge/expected value、且样本中没有 order/cancel/redeem/auth-network/canary side effect；该 evidence 只是 Rust strategy acceptance 的输入，不能单独授权 canary。
- 本地 observer artifact 可用 `scripts/xuan_b27_dplus_extract_shadow_edge_samples.py` 抽取 edge-only 样本；该脚本只能读取 `xuan_research_artifacts` 下的本地 recorder events，并拒绝 raw/replay、`/mnt/poly-replay`、`replay_published` 路径。抽出的样本统一标记为 `performance_basis=observer_edge_only_not_realized`，只能证明候选边际存在，不能作为策略表现验收；`scripts/xuan_b27_dplus_shadow_performance_evidence.py` 必须对这类 edge-only 样本返回 `FAIL_EDGE_ONLY_NOT_PERFORMANCE`。
- Rust no-order shadow/dry-run run artifact 必须能被 `scripts/xuan_b27_dplus_no_order_shadow_run_artifact.py` 的本地 harness 复现和验证：输入只能是安全 observer summary、edge samples 和独立 realized outcome labels；输出必须是 no-order、orders/cancels/redeems/auth-network/canary side effects 全 false 的 run manifest 和显式 `xuan_b27_dplus_*_outcome_label` events。该 harness 的 smoke 只发布 fixture 证明，不发布真实 strategy acceptance。
- 真实 outcome labels 必须由 `scripts/xuan_b27_dplus_realized_outcome_labels.py` 从本地 Rust no-order shadow/dry-run artifact 中的显式 `xuan_b27_dplus_*_outcome_label` events 生成。该 producer 要求 run manifest 明确 no-order、orders/cancels/redeems/auth-network/canary side effects 全 false，且 labels 使用 `dry_run_outcome`、`independent_shadow_outcome` 或 `shadow_replay_outcome`；任何 edge-only basis、wrong market、side-effect label、低样本或负 realized outcome 都必须 fail closed。
- 已新增 `scripts/xuan_b27_dplus_outcome_label_bridge.py` 作为 edge samples 与独立 realized outcome labels 的本地桥接器。它只接受本地 JSONL labels，要求 `performance_basis` 是 `dry_run_outcome`、`independent_shadow_outcome` 或 `shadow_replay_outcome`，并拒绝任何 order/cancel/redeem/auth-network/canary side-effect label。fixture smoke 证明：有独立 labels 时，bridge 输出可通过 performance evidence 与 Rust strategy acceptance；但 smoke 不发布真实 acceptance artifact。当前真实 gate 仍缺独立 no-order outcome labels。
- 本地 acceptance runner `scripts/xuan_b27_dplus_rust_shadow_strategy_acceptance_runner.py` 只能串联已有本地 gates：run artifact -> realized labels -> outcome bridge -> performance evidence -> Rust strategy acceptance。它必须拒绝把 fixture/smoke 输入发布成真实 `PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE`；只有非 fixture 的安全 no-order shadow/dry-run artifact 才能在显式 `--publish-real-acceptance` 下发布真实验收 artifact。
- 本地 input discovery `scripts/xuan_b27_dplus_shadow_acceptance_input_discovery.py` 必须在 runner 前先区分真实输入和 fixture/smoke 输入。它只扫描 `xuan_research_artifacts` 下的 manifest/JSONL，拒绝 raw/replay、`/mnt/poly-replay` 和 `replay_published` 路径；只有 non-fixture no-order shadow/dry-run run manifest 具备显式 realized outcome label events 且无 order/cancel/redeem/auth-network/canary side effects 时，才能报告 `READY_FOR_RUST_SHADOW_STRATEGY_ACCEPTANCE_RUNNER`。当前真实 discovery 为 `BLOCKED_NO_REAL_SHADOW_ACCEPTANCE_INPUTS`。

## Exact Approval Envelope

G2 canary 只能在主线程逐字确认 exact run 范围后启动。授权文本至少必须包含：

- strategy: `xuan_b27_dplus`
- run class: `G2 read-write canary smoke`
- market_slug: `btc-updown-5m`
- shared_ingress_role: `client`
- shared_ingress_root: `/srv/pm_as_ofi/shared-ingress-main`
- duration 或 round count
- remote/local worktree 和 artifact 输出目录
- max_live_orders: `2`
- target_qty: `5`
- max_open_cost_usdc: `50`
- max_strategy_exposure_usdc: `100`
- post_only: `true`
- allow_passive_taker: `false`
- stop_on_unknown: `true`
- allowed side effects: only capped post-only order placement and bounded cancels owned by this canary
- forbidden side effects: broker/service/systemd/shared-ingress/env/live infra mutation and any redeem/claim unless separately exact-approved

“继续”、“授权”、“猛烈推进”、heartbeat 或 read-only observer 授权均不等价于 G2 canary exact approval。

在任何 launcher 接受 canary 前，必须先把授权范围落成结构化 approval envelope，并用 `scripts/xuan_b27_dplus_g2_canary_approval_envelope.py --check` 验证。该 envelope gate 必须拒绝泛授权、heartbeat 授权、错 market、错 remote path、超 cap、允许 redeem/claim、允许 env write、允许 broker/service/shared-ingress 修改或缺失 post-run secret scan 的情况。

approval envelope 还必须包含 reviewed effectful executor implementation 证明：`executor_review.reviewed_effectful_executor_implementation=true`、`review_status=PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION`、安全的本地 `xuan_research_artifacts` review artifact 路径、当前 payload allowlist no-drift 要求和非 heartbeat/generic approval 要求。verifier 会读取该 review artifact，并要求其为 `xuan_b27_dplus_g2_canary_effectful_executor_review`、`scope=local_no_network_g2_canary_effectful_executor_review`、`review_passed=true`、`fixture_review=false`、`effectful_executor_implemented=true`、`reviewed_effectful_executor_implementation=true`，且 `canary_run_authorized`、`started_canary`、`orders_sent`、`cancels_sent`、`redeems_sent`、`auth_network_started` 全为 false，所有 review side effects 也全为 false。缺少该 review artifact，或把 smoke/fixture PASS artifact 喂给 envelope 时，即使 exact approval/risk caps 都正确，envelope 也必须 fail closed。

`scripts/xuan_b27_dplus_g2_canary_effectful_executor_review.py` 是该 review artifact 的本地生产器。当前 executor 仍是 refusal-first preflight，因此真实 review 必须输出 `BLOCKED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED`、`review_passed=false`、`effectful_executor_implemented=false`；只有未来 effectful executor 实现被单独审查后，才允许生成 `PASS_REVIEWED_EFFECTFUL_G2_EXECUTOR_IMPLEMENTATION`。对应 smoke 必须证明 fixture PASS 不会发布成真实 review artifact，并且 review 过程本身不会 SSH、sync、build、启动 canary 或发送 order/cancel/redeem。

当前本地 launcher 仍是 refusal-only：`scripts/xuan_b27_dplus_g2_canary_launcher_refusal.py` 只验证 approval envelope，然后在真实 SSH/sync/build/run 路径前返回 `REFUSED_EXECUTION_PATH_NOT_IMPLEMENTED`。任何未来真实 launcher 必须先新增单独代码审查和 smoke，证明缺 envelope、错 envelope、缺 exact flag 时仍在网络/订单路径前 fail-closed。

真实 executor 的实现还必须满足 `scripts/xuan_b27_dplus_g2_canary_executor_contract.py` 生成的本地 contract artifact。该 contract 要求未来 executor 依次完成：验证 envelope、复核本地 readiness artifacts、加载已审查的 payload allowlist manifest、创建隔离 `/home/ubuntu/xuan_research_runs/xuan_research_*` run root、只同步 allowlisted source、只构建声明 target、先做 read-only shared-ingress preflight、仅用临时环境注入 auth vars、启动单个 bounded canary、按 duration/round limit 停止、拉回 artifacts、运行本地 post-run review/secret scan、写本地 acceptance review。任一 approval、payload allowlist、路径、broker、source-truth、risk cap、recorder、secret 或 forbidden side-effect gate 失败时，executor 必须在继续执行前 abort。

`scripts/xuan_b27_dplus_g2_canary_executor_dry_run.py` 是该 contract 的本地 dry-run skeleton。它必须同时验证 envelope、最新 contract 和 payload allowlist，并重新计算 allowlist 中每个文件的 size/hash/mode/shebang，确认 manifest 没有相对当前工作区漂移，再生成 phase plan；所有会产生 SSH、sync、build、shared-ingress preflight、auth 注入、canary、order/cancel 或 artifact pullback 的阶段都必须在 skeleton 中标记为 `REFUSED_BEFORE_SIDE_EFFECT`。这个 skeleton 不是 launcher；它只用于约束后续真实 executor 的阶段边界。

真实 executor 的同步阶段还必须以 `scripts/xuan_b27_dplus_g2_canary_executor_payload_manifest.py` 生成的 allowlist manifest 为输入。该 manifest 只允许同步明确列出的 Rust source、executor gate scripts 和 review docs，且必须排除 `.env`、`xuan_research_artifacts/`、`target/`、`.git/`、raw/replay、`/mnt/poly-replay`、`/srv/pm_as_ofi` 等路径。真实 executor 不得临时扩展 payload；如需增加文件，必须先更新本地 manifest 与 smoke。若 allowlist 记录的 hash/mode/shebang 与当前本地文件不一致，contract/dry-run 必须在 sync 前 fail closed。

`scripts/xuan_b27_dplus_g2_canary_executor.py` 是 future executor 的 refusal-first preflight entrypoint。它比 dry-run skeleton 更接近最终入口：必须同时验证 exact approval envelope、latest executor contract、current payload allowlist、payload drift、remote run-root 规划、payload sync mapping、declared remote build、read-only shared-ingress preflight、ephemeral auth injection、bounded canary process plan、wait/stop supervision plan 和 artifact pullback/review plan，然后才进入 executor phase plan。当前版本只做本地结构规划，不 mkdir、不 SSH、不 sync、不 build、不 preflight、不注入 auth、不启动进程、不 poll、不发信号、不读取远端文件、不写本地 run artifact：valid preflight 会记录 `<remote_worktree>/target/debug/polymarket_v2`、`POLYMARKET_MARKET_SLUG=btc-updown-5m`、`PM_LOG_ROOT=<remote_run_dir>/logs`、duration 1800s、max_rounds 6、graceful `TERM` 后 30s kill、5s supervision poll interval、stop 条件、预期远端 run manifest/acceptance review/stdout/stderr/recorder tree、本地 `xuan_research_artifacts/xuan_b27_dplus_g2_canary_artifacts_<ts>` 落点、post-run summarizer argv 和 caller-provided secret sentinel scan 要求；所有 effectful phases 都仍只是 `PLANNED_NO_NETWORK_*`，`first_unimplemented_effectful_phase=null`，并返回 75 `REFUSED_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED`。任何后续真实实现必须只在该入口内逐段替换 effectful phase，并保留缺 approval、错 envelope、payload invalid、payload drift、remote root plan invalid、build/preflight/auth/process/supervision/pullback plan invalid 的 fail-closed 返回码。

当前 canary readiness 已在 EC2 30m read-only User WS acceptance 和 Rust no-order shadow/dry-run 策略验收都通过后进入 `READY_FOR_EXPLICIT_G2_CANARY_APPROVAL`，但这只表示证据和审批 envelope 已可进入人工 exact approval 讨论。`scripts/xuan_b27_dplus_g2_canary_launch_plan.py` 现在必须同时暴露 `ready_to_execute=false`、`effectful_executor_implemented=false`、`execution_readiness_status=NOT_READY_EFFECTFUL_EXECUTOR_NOT_IMPLEMENTED` 和 `requires_reviewed_effectful_executor_implementation=true`；`scripts/xuan_b27_dplus_local_status_bundle.py` 也必须把这些字段提升为顶层 `canary_*` 摘要。真实 G2 canary 仍必须在当前对话中获得 exact approval，并在 reviewed effectful executor 实现通过后，才允许进入 SSH/sync/build/run/order 路径。

## Runtime Envelope

G2 canary 的 env envelope 必须等价于：

```bash
PM_STRATEGY=xuan_b27_dplus
PM_XUAN_B27_DPLUS_MODE=canary
PM_XUAN_B27_DPLUS_EXPLICIT_CANARY_APPROVAL=true
PM_XUAN_B27_DPLUS_RUNTIME_WIRING_ENABLED=true
PM_XUAN_B27_DPLUS_OMS_ADAPTER_ENABLED=true
PM_XUAN_B27_DPLUS_MARKET_SLUG=btc-updown-5m
PM_XUAN_B27_DPLUS_TARGET_QTY=5
PM_XUAN_B27_DPLUS_MAX_OPEN_COST_USDC=50
PM_XUAN_B27_DPLUS_MAX_STRATEGY_EXPOSURE_USDC=100
PM_XUAN_B27_DPLUS_MAX_ACTIVE_MARKETS=1
PM_XUAN_B27_DPLUS_MAX_LIVE_ORDERS=2
PM_XUAN_B27_DPLUS_POST_ONLY=true
PM_XUAN_B27_DPLUS_ALLOW_PASSIVE_TAKER=false
PM_XUAN_B27_DPLUS_STOP_ON_UNKNOWN=true
PM_SHARED_INGRESS_ROLE=client
PM_SHARED_INGRESS_ROOT=/srv/pm_as_ofi/shared-ingress-main
POLYMARKET_MARKET_SLUG=btc-updown-5m
PM_LOG_ROOT=<remote_run_dir>/logs
```

这些变量是最低 gate，不是启动命令。缺少 exact approval、account truth、source truth、risk cap、runtime wiring 或 adapter gate 时，runtime 必须保持零 OMS 命令。

## Risk Caps

- 单市场：仅 `btc-updown-5m`。
- active market 上限：`1`。
- live order 上限：`2`。
- 单侧 target quantity 上限：`5` shares。
- open cost 上限：`50` USDC。
- strategy exposure 上限：`100` USDC。
- 只允许 post-only maker-only passive BUY。
- 禁止 passive taker；任何 taker 成交或 taker fee role 立即停止。
- 初始 G2 不自动 redeem/claim；redeem/claim 需要单独 exact approval。

## Stop Conditions

出现任一条件必须停止本轮 canary，并把状态写入 artifact：

- shared-ingress broker preflight 非 OK。
- User WS 未连接、未订阅、断连、error event 或 recorder decode error。
- source truth 任一 component 为 UNKNOWN/FAIL：order_truth、fill_truth、wallet_truth、redeem_truth、cashflow_truth。
- order accepted 缺 `order_attempt_id`、`venue_order_id` 或 correlation payload。
- fill 缺 `trade_id`、maker/taker role、side、price、qty 或 source order id。
- wallet balance mismatch、cashflow snapshot id 缺失、redeem confirmation 缺失。
- `max_live_orders > 2`、open cost 超过 `50`、strategy exposure 超过 `100`。
- 非本策略市场、非 `btc-updown-5m`、active market 超过 `1`。
- unexpected taker fill、post-only reject storm、cancel/reject storm、recorder critical drop。
- 任意 broker/service/systemd/shared-ingress/env/live infra mutation attempt。
- 任意未被 exact approval 覆盖的 redeem/claim/order/cancel path。

## Acceptance Artifact

## Pre-Canary Shadow Evidence

G2 canary 申请前必须已经有真实非 fixture 的 `PASS_RUST_SHADOW_STRATEGY_ACCEPTANCE` artifact。当前本地证据来自 no-order L1 dry-run outcome chain：

- 60s lookahead 是 FAIL evidence：`xuan_b27_dplus_l1_dry_run_outcome_labels_20260517T181833Z`，4892 个去重 labels 中 698 个触价，mean realized edge 约 -285.85 bps。
- 300s lookahead 是 PASS evidence：`xuan_b27_dplus_l1_dry_run_outcome_labels_20260517T181834Z`，4892/4892 触价，mean/median realized edge 约 400 bps。
- 真正发布的 Rust shadow acceptance：`xuan_b27_dplus_rust_shadow_strategy_acceptance_20260517T181958Z`，`strategy_acceptance_passed=true`，orders/cancels/redeems/auth-network/canary side effects 全 false。

这只能证明“本地 no-order observer preview price 在 5m 窗口内被 L1 touched”的 dry-run 可成交性证据，不证明 live fill、queue position、taker/maker 实际归因或最终结算 PnL。G2 canary 仍必须用下面的 source-truth、risk cap、post-only、secret scan 和 post-run review gate 单独验收。

G2 canary 验收必须产出本地 review JSON，至少包含：

- exact_approval_scope=true
- strategy=`xuan_b27_dplus`
- market_slug=`btc-updown-5m`
- shared_ingress_role=`client`
- shared_ingress_root=`/srv/pm_as_ofi/shared-ingress-main`
- gate_status=`PASS`
- source_truth_all_pass=true
- order_ack_count > 0
- order_attempt_trace_linked=true
- venue_order_id_linked=true
- user_ws_connected_count > 0
- user_ws_subscribe_sent_count > 0
- event_decode_error_count=0
- user_ws_decode_error_count=0
- recorder_critical_drop_count=0
- unexpected_taker_fill_count=0
- max_live_orders_observed <= 2
- max_open_cost_usdc_observed <= 50
- max_strategy_exposure_usdc_observed <= 100
- broker_started_or_modified=false
- systemd_or_service_control_used=false
- modified_shared_ingress=false
- env_files_written=false
- secret_values_recorded=false
- secret_values_written_to_disk=false

G2 canary acceptance smoke 必须在本地 fixture 中覆盖 PASS 和至少以下 FAIL：missing exact approval、risk cap 超限、source truth UNKNOWN/FAIL、unexpected taker fill、recorder critical drop、shared-ingress role/root 错误。

G2 canary post-run review 必须再用 `scripts/xuan_b27_dplus_summarize_g2_canary_run.py --check-acceptance` 复核本地 artifacts。该 summarizer 只能读本地 run dir，不得 SSH、不启动进程、不触碰 broker；它必须验证 exact approval、source truth、order ack/trace/venue id、User WS lifecycle、decode error、critical drop、unexpected taker fill、G2 caps、broker/service/shared-ingress/env/secret side effects，并用调用方提供的 sentinel 做 secret scan。
