// 简单的WebSocket连接测试
use tokio_tungstenite::connect_async;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("Testing WebSocket connection to Polymarket...");
    
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    println!("URL: {}", url);
    
    println!("Attempting connection with 10s timeout...");
    match tokio::time::timeout(
        Duration::from_secs(10),
        connect_async(url)
    ).await {
        Ok(Ok((ws, response))) => {
            println!("✅ SUCCESS! Connected to WebSocket");
            println!("Response status: {:?}", response.status());
            println!("Response headers: {:?}", response.headers());
        }
        Ok(Err(e)) => {
            println!("❌ Connection error: {}", e);
            println!("Error type: {:?}", e);
        }
        Err(_) => {
            println!("⏱️ TIMEOUT after 10 seconds");
            println!("This suggests TLS handshake is hanging");
        }
    }
}
