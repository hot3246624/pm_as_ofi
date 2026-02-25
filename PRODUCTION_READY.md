# 🚀 PRODUCTION READY CHECKLIST 🚀

## 1. Environment Variables (`.env`)
- [ ] `POLYMARKET_PRIVATE_KEY` 必须填写（有资金操作权限的钱包私钥）。
- [ ] `POLYMARKET_FUNDER_ADDRESS` （可选）如果使用 Proxy 账户需填写，否则留空。
- [ ] `PM_DRY_RUN` 必须设置为 `false`。
- [ ] `PM_PAIR_TARGET` 建议第一次跑设为 `0.985`（保守防滑点）。
- [ ] `PM_BID_SIZE` 建议第一次跑设为 `5.0`（单侧 5 股进行微型压力测试）。
- [ ] `PM_MAX_NET_DIFF` 建议设为 `15.0`（单侧倾斜上限，超额拒单）。

## 2. Funding & Approval
- [ ] 确保在 Polygon 链上的该钱包有足够的 `USDC.e` 用于双边双排做市。
- [ ] 确保钱包有少量 `MATIC/POL` 用于支付 Gas 费。
- [ ] 确保已经在前端授权过了 CTF 交易所的 Token Approvals。

## 3. Network & Deployment
- [ ] 确保服务器时钟已经同步 (`ntpdate`)，误差 < 1s 会影响 5 分钟市场切场倒计时。
- [ ] 建议使用 `screen` 或 `tmux` 维持进程，防止 SSH 掉线导致死机。
- [ ] 启动命令：`cargo run --bin polymarket_v2 --release`

## 4. Emergency Procedures
- 如果发现吃单剧烈亏损，或者日志疯狂刷错误（比如余额不足、限速 429）：
  1. 立刻按 `Ctrl+C`：收到 SIGINT 信号后，由于我们在底层绑定了 `CancelAll` 清盘撤单逻辑，系统会在销毁进程前安全撤销盘口所有的 Maker 挂单。
  2. 如果由于网络原因 Crash 导致 `CancelAll` 发送失败，立刻打开网页版 Polymarket 进行撤单和手动平仓对冲。
