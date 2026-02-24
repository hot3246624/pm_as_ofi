use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Yes,
    No,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Yes => "YES",
            Side::No => "NO",
        }
    }
}

/// 订单簿快照（仅保存最优 bid/ask）
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub yes_bid: f64,
    pub yes_ask: f64,
    pub no_bid: f64,
    pub no_ask: f64,
    pub updated_at: Instant,
}

impl OrderBook {
    pub fn is_ready(&self) -> bool {
        self.yes_bid > 0.0 && self.yes_ask > 0.0 && self.no_bid > 0.0 && self.no_ask > 0.0
    }
}

/// 行情更新（最优价）
#[derive(Debug, Clone)]
pub struct BookUpdate {
    pub asset_id: String,
    pub side: Option<Side>,
    pub best_bid: f64,
    pub best_ask: f64,
    pub ts: Instant,
}

/// 订单状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    PendingNew,
    Open,
    PendingCancel,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

/// 内部订单结构（用于状态机）
#[derive(Debug, Clone)]
pub struct Order {
    pub id: String,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub remaining_qty: f64,
    pub status: OrderStatus,
    pub created_at: Instant,
    pub ttl: Duration,
}

impl Order {
    pub fn is_expired(&self, now: Instant) -> bool {
        now.duration_since(self.created_at) >= self.ttl
    }
}

/// 目标订单（策略输出）
#[derive(Debug, Clone)]
pub struct DesiredOrder {
    pub side: Side,
    pub price: f64,
    pub qty: f64,
}

/// 订单事件（来自交易所 WS）
#[derive(Debug, Clone)]
pub struct OrderEvent {
    pub id: String,
    pub side: Option<Side>,
    pub event_type: Option<String>,
    pub status: OrderStatus,
    pub raw_status: Option<String>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub filled_qty: f64,
    pub avg_fill_price: Option<f64>,
    pub remaining_qty: Option<f64>,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone)]
pub enum OrderAction {
    Place {
        client_id: String,
        order: DesiredOrder,
    },
    Cancel {
        id: String,
    },
}

// ===== REST API Types =====

use serde::{Deserialize, Serialize};

/// 订单参数（用于签名）
#[derive(Debug, Clone)]
pub struct OrderParams {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub expiration: u64,
    pub nonce: u64,
    pub fee_rate_bps: u64,
    pub side: u8, // 0=BUY, 1=SELL
}

/// 签名后的订单对象
#[derive(Debug, Clone, Serialize)]
pub struct SignedOrder {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    pub side: u8,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

/// POST /order 请求体
#[derive(Debug, Clone, Serialize)]
pub struct PostOrderRequest {
    pub order: SignedOrder,
    pub owner: String,
    #[serde(rename = "orderType")]
    pub order_type: String, // "GTC"
    #[serde(rename = "postOnly")]
    pub post_only: bool,
}

/// POST /order 响应
#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderID")]
    pub order_id: String,
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub error_msg: Option<String>,
}

/// DELETE /order 请求体
#[derive(Debug, Clone, Serialize)]
pub struct CancelOrderRequest {
    #[serde(rename = "orderID")]
    pub order_id: String,
}
