use crate::polymarket::types::{DesiredOrder, OrderBook, Side};

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    // 风控约束（核心指标）
    pub max_pair_cost: f64,      // 最大 Pair Cost（平均成本和），默认 1.0112
    pub max_diff_value: f64,     // 最大 Diff Value（净头寸美元值），默认 5.0

    // 订单参数
    pub tick: f64,               // 价格步长，默认 0.001（0.1美分）
    pub levels: usize,           // 挂单层数，默认 3
    pub qty_per_level: f64,      // 每层基础数量，默认 5.0
    pub qty_cap: f64,            // 单笔最大数量，默认 10.0
    pub min_order_size: f64,     // 最小订单数量，默认 1.0（建议≥5）
    pub ttl_secs: u64,           // GTD 订单 TTL（秒），默认 60

    // Kelly 仓位管理
    pub kelly_enabled: bool,     // 是否启用 Kelly，默认 true
    pub kelly_fraction: f64,     // Kelly 比例（0-1），默认 0.5（半凯利）
    pub edge_ref: f64,           // 参考 edge，默认 0.01
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            max_pair_cost: 1.0112,
            max_diff_value: 5.0,
            tick: 0.001,             // ✅ 0.1美分精度（官方文档确认）
            levels: 3,
            qty_per_level: 5.0,      // 提高到5（实际建议）
            qty_cap: 10.0,
            min_order_size: 1.0,     // 最小1份，实际建议≥5
            ttl_secs: 60,  // GTD 订单标准 TTL
            kelly_enabled: true,
            kelly_fraction: 0.5,
            edge_ref: 0.01,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Position {
    pub yes_qty: f64,
    pub no_qty: f64,
    pub yes_avg: f64,
    pub no_avg: f64,
}

impl Position {
    /// 净头寸份额（YES - NO）
    pub fn net_diff(&self) -> f64 {
        self.yes_qty - self.no_qty
    }

    /// Pair Cost = 持仓平均成本之和（核心风控指标1）
    /// 确保配对头寸总成本 < $1，保证利润空间
    pub fn pair_cost(&self) -> f64 {
        if self.yes_qty == 0.0 && self.no_qty == 0.0 {
            0.0
        } else {
            self.yes_avg + self.no_avg
        }
    }

    /// Diff Value = 净头寸的美元价值（核心风控指标2）
    /// 衡量单向敞口风险
    ///
    /// # Arguments
    /// * `yes_price` - YES 侧价格（通常用 bid 价）
    /// * `no_price` - NO 侧价格（通常用 bid 价）
    pub fn diff_value(&self, yes_price: f64, no_price: f64) -> f64 {
        let net_diff = self.net_diff();
        if net_diff > 0.0 {
            // 多 YES，用 YES 价格计算风险
            net_diff * yes_price
        } else {
            // 多 NO，用 NO 价格计算风险
            net_diff.abs() * no_price
        }
    }

    /// 应用成交更新持仓（成交后调用）
    pub fn apply_fill(&mut self, side: Side, qty: f64, price: f64) {
        match side {
            Side::Yes => {
                let old_qty = self.yes_qty;
                let old_avg = self.yes_avg;
                self.yes_qty += qty;

                if self.yes_qty > 0.0 {
                    self.yes_avg = (old_qty * old_avg + qty * price) / self.yes_qty;
                } else {
                    self.yes_avg = 0.0;
                }
            }
            Side::No => {
                let old_qty = self.no_qty;
                let old_avg = self.no_avg;
                self.no_qty += qty;

                if self.no_qty > 0.0 {
                    self.no_avg = (old_qty * old_avg + qty * price) / self.no_qty;
                } else {
                    self.no_avg = 0.0;
                }
            }
        }
    }

    /// 模拟成交后的新状态（用于风控预检查）
    pub fn simulate_fill(&self, side: Side, qty: f64, price: f64) -> Position {
        let mut new_pos = *self;
        new_pos.apply_fill(side, qty, price);
        new_pos
    }
}

pub struct Strategy {
    cfg: StrategyConfig,
}

impl Strategy {
    pub fn new(cfg: StrategyConfig) -> Self {
        Self { cfg }
    }

    pub fn config(&self) -> &StrategyConfig {
        &self.cfg
    }

    /// 生成当前应挂的 maker-only 订单
    /// 每个订单下单前都检查 Pair Cost 和 Diff Value 约束
    pub fn compute_quotes(&self, book: &OrderBook, pos: &Position) -> Vec<DesiredOrder> {
        if !book.is_ready() {
            return Vec::new();
        }

        let mut orders = Vec::new();

        // 为每一层生成 YES 和 NO 订单
        for level in 0..self.cfg.levels {
            let offset = self.cfg.tick * (level as f64 + 1.0);

            // YES bid（我们买入 YES 的价格）
            let yes_price = (book.yes_bid - offset).max(0.01).min(0.99);
            if let Some(yes_qty) = self.calc_safe_qty(Side::Yes, yes_price, pos, book) {
                orders.push(DesiredOrder {
                    side: Side::Yes,
                    price: yes_price,
                    qty: yes_qty,
                });
            }

            // NO bid（我们买入 NO 的价格）
            let no_price = (book.no_bid - offset).max(0.01).min(0.99);
            if let Some(no_qty) = self.calc_safe_qty(Side::No, no_price, pos, book) {
                orders.push(DesiredOrder {
                    side: Side::No,
                    price: no_price,
                    qty: no_qty,
                });
            }
        }

        orders
    }

    /// 计算安全的下单数量（考虑 Pair Cost 和 Diff Value 约束）
    /// 返回 None 表示不应下单
    fn calc_safe_qty(
        &self,
        side: Side,
        price: f64,
        pos: &Position,
        book: &OrderBook,
    ) -> Option<f64> {
        // BUG 3 FIX: 传入 side 参数，按侧选择正确的盘口中价
        let base_qty = self.calc_kelly_qty(side, price, book);

        // 模拟成交后的状态
        let future_pos = pos.simulate_fill(side, base_qty, price);

        // 1. 检查 Pair Cost 约束
        if future_pos.pair_cost() > self.cfg.max_pair_cost {
            return None;
        }

        // 2. 检查 Diff Value 约束（使用 bid 价格）
        let future_diff = future_pos.diff_value(book.yes_bid, book.no_bid);
        if future_diff > self.cfg.max_diff_value {
            // 尝试减半数量
            let half_qty = base_qty / 2.0;
            if half_qty < self.cfg.min_order_size {
                return None;
            }

            let future_pos_half = pos.simulate_fill(side, half_qty, price);
            let future_diff_half = future_pos_half.diff_value(book.yes_bid, book.no_bid);

            if future_diff_half > self.cfg.max_diff_value {
                return None;
            }

            return Some(half_qty.min(self.cfg.qty_cap));
        }

        // 3. 检查最小订单约束
        if base_qty < self.cfg.min_order_size {
            return None;
        }

        Some(base_qty.min(self.cfg.qty_cap))
    }

    /// BUG 3 FIX: 使用 Kelly 公式计算下单数量。
    ///
    /// 原实现对所有侧都用 `yes_bid/yes_ask` 计算 mid_price，导致 NO 侧
    /// edge 计算使用了错误的参考价格。现在根据 `side` 选择对应侧的盘口。
    fn calc_kelly_qty(&self, side: Side, price: f64, book: &OrderBook) -> f64 {
        if !self.cfg.kelly_enabled {
            return self.cfg.qty_per_level;
        }

        // 按侧选择正确盘口计算 mid_price
        let (bid, ask) = match side {
            Side::Yes => (book.yes_bid, book.yes_ask),
            Side::No => (book.no_bid, book.no_ask),
        };

        if bid <= 0.0 || ask <= 0.0 || ask <= bid {
            return self.cfg.qty_per_level;
        }

        let mid_price = (bid + ask) / 2.0;
        let edge = (mid_price - price).abs() / mid_price;

        // Kelly 公式: qty = base * edge/edge_ref * kelly_fraction
        let edge_mult = (edge / self.cfg.edge_ref).max(0.5);
        let kelly_qty = self.cfg.qty_per_level * edge_mult * self.cfg.kelly_fraction;

        // 限制在合理范围
        kelly_qty.max(self.cfg.qty_per_level * 0.5).min(self.cfg.qty_cap)
    }

    fn floor_to_tick(&self, p: f64) -> f64 {
        if self.cfg.tick <= 0.0 {
            return p;
        }
        (p / self.cfg.tick).floor() * self.cfg.tick
    }
}
