use polymarket_client_sdk::clob::types::{OrderType, Side};
use polymarket_client_sdk::ClobClient;

#[tokio::main]
async fn main() {
    let client = ClobClient::new("https://clob.polymarket.com", 137);
    let order = client.limit_order()
        .token_id(alloy::primitives::U256::from(1))
        .size(rust_decimal::Decimal::new(100, 2))
        .price(rust_decimal::Decimal::new(99, 2))
        .side(Side::Buy)
        .order_type(OrderType::FAK);
    println!("Type exists on limit_order");
}
