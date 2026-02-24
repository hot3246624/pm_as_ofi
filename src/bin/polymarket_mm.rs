use anyhow::Context;
use futures::{SinkExt, StreamExt};
use mev_backrun_rs_cu::polymarket::legacy::order_manager::OrderManager;
use mev_backrun_rs_cu::polymarket::legacy::strategy::{Position, Strategy, StrategyConfig};
use mev_backrun_rs_cu::polymarket::types::{BookUpdate, OrderAction, OrderBook, OrderEvent, OrderStatus, Side};

// Polymarket official SDK
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::Decimal;
use polymarket_client_sdk::auth::state::Authenticated;
use rust_decimal::prelude::FromPrimitive;  // For from_f64
use alloy::signers::{Signer as _, local::LocalSigner};
use std::str::FromStr;

use serde_json::{json, Value};
use std::env;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
struct ApiCreds {
    api_key: String,
    api_secret: String,
    api_passphrase: String,
}

#[derive(Debug, Clone)]
struct Settings {
    ws_base_url: String,
    rest_url: String,
    market_slug: Option<String>,
    market_id: String,
    yes_asset_id: String,
    no_asset_id: String,
    maker_only: bool,
    custom_feature: bool,
    creds: Option<ApiCreds>,
    // L1 signing (for orders)
    private_key: Option<String>,
    funder_address: Option<String>,
    signature_type: u8,
}

impl Settings {
    fn from_env() -> anyhow::Result<Self> {
        let ws_base_url = env::var("POLYMARKET_WS_BASE_URL")
            .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com".to_string());
        let rest_url = env::var("POLYMARKET_REST_URL")
            .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());
        let market_slug = env::var("POLYMARKET_MARKET_SLUG").ok();
        
        // 如果有 slug，则 market ID 可选；否则必填
        let market_id = if market_slug.is_some() {
            env::var("POLYMARKET_MARKET_ID").unwrap_or_default()
        } else {
            env::var("POLYMARKET_MARKET_ID")
                .context("POLYMARKET_MARKET_ID or POLYMARKET_MARKET_SLUG is required")?
        };
        
        let yes_asset_id = env::var("POLYMARKET_YES_ASSET_ID").unwrap_or_default();
        let no_asset_id = env::var("POLYMARKET_NO_ASSET_ID").unwrap_or_default();
        let maker_only = env::var("POLYMARKET_MAKER_ONLY")
            .map(|v| v != "0")
            .unwrap_or(true);
        let custom_feature = env::var("POLYMARKET_WSS_CUSTOM_FEATURE")
            .map(|v| v != "0")
            .unwrap_or(false);

        let creds = match (
            env::var("POLYMARKET_API_KEY"),
            env::var("POLYMARKET_API_SECRET"),
            env::var("POLYMARKET_API_PASSPHRASE"),
        ) {
            (Ok(key), Ok(secret), Ok(passphrase)) => Some(ApiCreds {
                api_key: key,
                api_secret: secret,
                api_passphrase: passphrase,
            }),
            _ => None,
        };

        let private_key = env::var("POLYMARKET_PRIVATE_KEY").ok();
        let funder_address = env::var("POLYMARKET_FUNDER_ADDRESS").ok();
        let signature_type = env::var("POLYMARKET_SIGNATURE_TYPE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        Ok(Self {
            ws_base_url,
            rest_url,
            market_slug,
            market_id,
            yes_asset_id,
            no_asset_id,
            maker_only,
            custom_feature,
            creds,
            private_key,
            funder_address,
            signature_type,
        })
    }

    fn ws_url(&self, channel: &str) -> String {
        format!("{}/ws/{}", self.ws_base_url.trim_end_matches('/'), channel)
    }

    fn market_assets(&self) -> Vec<String> {
        vec![self.yes_asset_id.clone(), self.no_asset_id.clone()]
    }

    fn market_ids(&self) -> Vec<String> {
        vec![self.market_id.clone()]
    }
}

// 市场解析：优先使用 slug，回退到手动配置
async fn resolve_market(settings: &Settings) -> anyhow::Result<(String, String, String)> {
    use mev_backrun_rs_cu::gamma_http::GammaClient;
    
    // 1. 优先：使用 slug 自动发现
    if let Some(slug) = &settings.market_slug {
        info!("🔍 Fetching latest market for: {}", slug);
        
        let gamma = GammaClient::new();
        let event = gamma.get_event_by_slug(slug).await?;
        
        info!("   Event: {}", event.title.as_deref().unwrap_or("N/A"));
        info!("   Active: {}, Closed: {}", 
            event.active.unwrap_or(false),
            event.closed.unwrap_or(false));
        
        let market = GammaClient::extract_latest_market(&event)?;
        let (yes_id, no_id) = GammaClient::extract_tokens(market)?;
        
        info!("✅ Auto-discovered:");
        info!("   Market ID: {}", market.condition_id);
        info!("   YES token: {}", yes_id);
        info!("   NO token:  {}", no_id);
        
        return Ok((market.condition_id.clone(), yes_id, no_id));
    }
    
    // 2. 回退：手动配置
    info!("📌 Using manual market configuration");
    info!("   Market ID: {}", settings.market_id);
    info!("   YES token: {}", settings.yes_asset_id);
    info!("   NO token:  {}", settings.no_asset_id);
    
    Ok((
        settings.market_id.clone(),
        settings.yes_asset_id.clone(),
        settings.no_asset_id.clone(),
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter("info").init();

    let mut settings = Settings::from_env()?;
    
    // 解析市场信息
    let (market_id, yes_asset_id, no_asset_id) = resolve_market(&settings).await?;
    
    // 更新settings中的token IDs
    settings.market_id = market_id.clone();
    settings.yes_asset_id = yes_asset_id;
    settings.no_asset_id = no_asset_id;
    
    info!("starting polymarket maker for {}", market_id);

    let (md_tx, mut md_rx) = mpsc::channel::<BookUpdate>(1024);
    let (oe_tx, mut oe_rx) = mpsc::channel::<OrderEvent>(1024);

    let market_settings = settings.clone();
    tokio::spawn(async move {
        if let Err(err) = run_market_ws(market_settings, md_tx).await {
            error!("market ws stopped: {err:?}");
        }
    });

    if settings.creds.is_some() {
        let user_settings = settings.clone();
        tokio::spawn(async move {
            if let Err(err) = run_user_ws(user_settings, oe_tx).await {
                error!("user ws stopped: {err:?}");
            }
        });
    } else {
        warn!("POLYMARKET_API_KEY/SECRET/PASSPHRASE 未设置，跳过 user channel");
    }

    // Initialize CLOB client using official Polymarket SDK
    let (clob_client, signer) = if let (Some(_creds), Some(pk), Some(_funder)) = 
        (&settings.creds, &settings.private_key, &settings.funder_address) 
    {
        match LocalSigner::from_str(pk) {
            Ok(signer) => {
                let signer = signer.with_chain_id(Some(137)); // Polygon
                
                match ClobClient::new(&settings.rest_url, ClobConfig::default()) {
                    Ok(client) => {
                        let auth_builder = client.authentication_builder(&signer);
                        match auth_builder.authenticate().await {
                            Ok(client) => {
                                info!("✅ Polymarket CLOB client authenticated");
                                (Some(client), Some(signer))
                            }
                            Err(e) => {
                                warn!("⚠️ Failed to authenticate CLOB client: {:?}, 将仅监听不下单", e);
                                (None, Some(signer))
                            }
                        }
                    }
                    Err(e) => {
                        warn!("⚠️ Failed to create CLOB client: {:?}, 将仅监听不下单", e);
                        (None, None)
                    }
                }
            }
            Err(e) => {
                warn!("⚠️ Invalid private key: {:?}, 将仅监听不下单", e);
                (None, None)
            }
        }
    } else {
        warn!("⚠️ 缺少私钥或 API 凭证，将仅监听不下单");
        (None, None)
    };

    let mut book = OrderBook {
        yes_bid: 0.0,
        yes_ask: 0.0,
        no_bid: 0.0,
        no_ask: 0.0,
        updated_at: Instant::now(),
    };
    let mut position = Position {
        yes_qty: 0.0,
        no_qty: 0.0,
        yes_avg: 0.0,
        no_avg: 0.0,
    };

    let strat_cfg = StrategyConfig::default();
    let strategy = Strategy::new(strat_cfg.clone());
    let mut orders = OrderManager::new(Duration::from_secs(strat_cfg.ttl_secs));
    let mut tick = interval(Duration::from_millis(250));

    loop {
        let mut changed = false;
        tokio::select! {
            Some(update) = md_rx.recv() => {
                apply_book_update(&settings, &mut book, update);
                changed = true;
            }
            Some(event) = oe_rx.recv() => {
                if event.filled_qty > 0.0 {
                    if let (Some(side), Some(price)) = (event.side, event.avg_fill_price) {
                        position.apply_fill(side, event.filled_qty, price);
                    }
                }
                orders.on_order_event(event);
                changed = true;
            }
            _ = tick.tick() => {
                changed = true;
            }
        }

        if !changed || !book.is_ready() {
            continue;
        }

        if orders.has_pending() {
            continue;
        }

        // 计算并记录当前关键指标
        let pair_cost = position.pair_cost();
        let diff_value = position.diff_value(book.yes_bid, book.no_bid);
        
        info!(
            "Position: YES={:.2}@{:.4}, NO={:.2}@{:.4} | PairCost={:.4} (max={:.4}) | DiffValue=${:.2} (max=${:.2}) | NetDiff={:.2}",
            position.yes_qty,
            position.yes_avg,
            position.no_qty,
            position.no_avg,
            pair_cost,
            strategy.config().max_pair_cost,
            diff_value,
            strategy.config().max_diff_value,
            position.net_diff(),
        );

        let desired = strategy.compute_quotes(&book, &position);
        let actions = orders.sync(&desired, Instant::now(), &book);
        for action in actions {
            if let Err(err) = dispatch_action(&settings, clob_client.as_ref(), signer.as_ref(), action).await {
                warn!("dispatch action failed: {err:?}");
            }
        }
    }
}

async fn run_market_ws(settings: Settings, md_tx: mpsc::Sender<BookUpdate>) -> anyhow::Result<()> {
    loop {
        let url = settings.ws_url("market");
        info!(%url, "connecting market ws");
        
        // Add timeout to prevent hanging
        let connect_result = tokio::time::timeout(
            Duration::from_secs(10),
            connect_async(&url)
        ).await;
        
        match connect_result {
            Ok(Ok((ws, response))) => {
                info!("✅ market ws connected! response status: {:?}", response.status());
                let (mut write, mut read) = ws.split();
                
                let asset_ids = settings.market_assets();
                info!("📡 Subscribing to {} assets", asset_ids.len());
                info!("🔑 Asset IDs: {:?}", asset_ids);
                
                // 正确格式：包含operation, markets, initial_dump
                let subscribe = json!({
                    "type": "market",
                    "operation": "subscribe",
                    "markets": [],  // 空数组
                    "assets_ids": asset_ids,
                    "initial_dump": true,  // 请求初始状态
                    "custom_feature_enabled": settings.custom_feature,
                });
                info!("📤 Subscribe: {}", subscribe.to_string());
                
                if let Err(err) = write.send(Message::Text(subscribe.to_string())).await {
                    warn!("market ws subscribe failed: {err:?}");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
                
                info!("✅ market ws subscribed to market");


                let mut ping_writer = write;
                tokio::spawn(async move {
                    let mut delay = interval(Duration::from_secs(10));
                    loop {
                        delay.tick().await;
                        if ping_writer
                            .send(Message::Text("PING".to_string()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            // Log messages to debug
                            if text.len() < 50 {
                                info!("📩 msg: [{}]", text);
                            } else {
                                info!("📩 msg ({} bytes): {}...", text.len(), &text[..100]);
                            }
                            
                            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                let updates = parse_market_message(&settings, &value);
                                if !updates.is_empty() {
                                    info!("📊 Got {} updates", updates.len());
                                }
                                for update in updates {
                                    let _ = md_tx.send(update).await;
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {}
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Binary(_)) => {}
                        Ok(Message::Close(_)) => {
                            warn!("market ws closed by server");
                            break;
                        }
                        Ok(Message::Frame(_)) => {}
                        Err(err) => {
                            warn!("market ws error: {err:?}");
                            break;
                        }
                    }
                }
            }
            Ok(Err(err)) => {
                warn!("market ws connect error: {err:?}");
                sleep(Duration::from_secs(2)).await;
            }
            Err(_timeout) => {
                warn!("⏱️ market ws connection timeout after 10s");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn run_user_ws(settings: Settings, oe_tx: mpsc::Sender<OrderEvent>) -> anyhow::Result<()> {
    let creds = match settings.creds.clone() {
        Some(c) => c,
        None => return Ok(()),
    };

    loop {
        let url = settings.ws_url("user");
        info!(%url, "connecting user ws");
        match connect_async(&url).await {
            Ok((ws, _)) => {
                let (mut write, mut read) = ws.split();
                let subscribe = json!({
                    "type": "user",
                    "markets": settings.market_ids(),
                    "auth": {
                        "apikey": creds.api_key,
                        "secret": creds.api_secret,
                        "passphrase": creds.api_passphrase,
                    },
                });
                if let Err(err) = write.send(Message::Text(subscribe.to_string())).await {
                    warn!("user ws subscribe failed: {err:?}");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }

                let mut ping_writer = write;
                tokio::spawn(async move {
                    let mut delay = interval(Duration::from_secs(10));
                    loop {
                        delay.tick().await;
                        if ping_writer
                            .send(Message::Text("PING".to_string()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                if let Some(event) = parse_order_event(&value) {
                                    let _ = oe_tx.send(event).await;
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {}
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Binary(_)) => {}
                        Ok(Message::Close(_)) => {
                            warn!("user ws closed by server");
                            break;
                        }
                        Ok(Message::Frame(_)) => {}
                        Err(err) => {
                            warn!("user ws error: {err:?}");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!("user ws connect error: {err:?}");
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}

fn apply_book_update(settings: &Settings, book: &mut OrderBook, update: BookUpdate) {
    let side = update
        .side
        .or_else(|| classify_side(&update.asset_id, settings));
    if let Some(side) = side {
        match side {
            Side::Yes => {
                if update.best_bid > 0.0 {
                    book.yes_bid = update.best_bid;
                }
                if update.best_ask > 0.0 {
                    book.yes_ask = update.best_ask;
                }
            }
            Side::No => {
                if update.best_bid > 0.0 {
                    book.no_bid = update.best_bid;
                }
                if update.best_ask > 0.0 {
                    book.no_ask = update.best_ask;
                }
            }
        }
        book.updated_at = update.ts;
    }
}

fn classify_side(asset_id: &str, settings: &Settings) -> Option<Side> {
    if asset_id == settings.yes_asset_id {
        Some(Side::Yes)
    } else if asset_id == settings.no_asset_id {
        Some(Side::No)
    } else {
        None
    }
}

fn parse_market_message(settings: &Settings, value: &Value) -> Vec<BookUpdate> {
    let mut updates = Vec::new();
    match value.get("event_type").and_then(|v| v.as_str()) {
        Some("book") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                if let Some(update) = build_book_from_levels(asset_id, value) {
                    updates.push(update);
                }
            }
        }
        Some("price_change") => {
            if let Some(changes) = value.get("price_changes").and_then(|v| v.as_array()) {
                for ch in changes {
                    if let Some(asset_id) = ch.get("asset_id").and_then(|v| v.as_str()) {
                        let best_bid = ch
                            .get("best_bid")
                            .and_then(|v| v.as_str())
                            .and_then(parse_price_str)
                            .unwrap_or(0.0);
                        let best_ask = ch
                            .get("best_ask")
                            .and_then(|v| v.as_str())
                            .and_then(parse_price_str)
                            .unwrap_or(0.0);
                        updates.push(BookUpdate {
                            asset_id: asset_id.to_string(),
                            side: classify_side(asset_id, settings),
                            best_bid,
                            best_ask,
                            ts: Instant::now(),
                        });
                    }
                }
            }
        }
        Some("best_bid_ask") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                let best_bid = value
                    .get("best_bid")
                    .and_then(|v| v.as_str())
                    .and_then(parse_price_str)
                    .unwrap_or(0.0);
                let best_ask = value
                    .get("best_ask")
                    .and_then(|v| v.as_str())
                    .and_then(parse_price_str)
                    .unwrap_or(0.0);
                updates.push(BookUpdate {
                    asset_id: asset_id.to_string(),
                    side: classify_side(asset_id, settings),
                    best_bid,
                    best_ask,
                    ts: Instant::now(),
                });
            }
        }
        _ => {}
    }
    updates
}

fn build_book_from_levels(asset_id: &str, value: &Value) -> Option<BookUpdate> {
    let bids = value
        .get("bids")
        .or_else(|| value.get("buys"))
        .and_then(|v| v.as_array());
    let asks = value
        .get("asks")
        .or_else(|| value.get("sells"))
        .and_then(|v| v.as_array());
    let best_bid = bids
        .and_then(|levels| levels.first())
        .and_then(|lvl| lvl.get("price"))
        .and_then(|v| v.as_str())
        .and_then(parse_price_str)
        .unwrap_or(0.0);
    let best_ask = asks
        .and_then(|levels| levels.first())
        .and_then(|lvl| lvl.get("price"))
        .and_then(|v| v.as_str())
        .and_then(parse_price_str)
        .unwrap_or(0.0);
    Some(BookUpdate {
        asset_id: asset_id.to_string(),
        side: None,
        best_bid,
        best_ask,
        ts: Instant::now(),
    })
}

fn parse_order_event(value: &Value) -> Option<OrderEvent> {
    let event_type = value.get("event_type").and_then(|v| v.as_str())?.to_string();
    let id = value
        .get("id")
        .or_else(|| value.get("order_id"))
        .and_then(|v| v.as_str())?
        .to_string();
    let status_text = value
        .get("status")
        .or_else(|| value.get("type"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let status = status_text
        .as_deref()
        .and_then(map_status)
        .unwrap_or(OrderStatus::Open);
    let price = value
        .get("price")
        .and_then(|v| v.as_str())
        .and_then(parse_price_str);
    let size = value
        .get("size")
        .or_else(|| value.get("size_matched"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let filled_qty = value
        .get("size_matched")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let outcome = value.get("outcome").and_then(|v| v.as_str()).map(|s| s.to_string());
    let side = value
        .get("side")
        .and_then(|v| v.as_str())
        .and_then(map_side);
    let avg_fill_price = value
        .get("avg_fill_price")
        .and_then(|v| v.as_str())
        .and_then(parse_price_str)
        .or(price);

    Some(OrderEvent {
        id,
        side,
        event_type: Some(event_type),
        status,
        raw_status: status_text,
        price,
        size,
        filled_qty,
        avg_fill_price,
        remaining_qty: None,
        outcome,
    })
}

fn map_status(status: &str) -> Option<OrderStatus> {
    match status.to_uppercase().as_str() {
        "PLACEMENT" => Some(OrderStatus::PendingNew),
        "UPDATE" => Some(OrderStatus::PartiallyFilled),
        "CANCELLATION" => Some(OrderStatus::Canceled),
        "MATCHED" | "MINED" | "CONFIRMED" => Some(OrderStatus::Filled),
        "FAILED" => Some(OrderStatus::Rejected),
        _ => None,
    }
}

fn map_side(value: &str) -> Option<Side> {
    match value.to_uppercase().as_str() {
        "BUY" | "YES" => Some(Side::Yes),
        "SELL" | "NO" => Some(Side::No),
        _ => None,
    }
}

fn parse_price_str(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('.') {
        format!("0{}", trimmed).parse().ok()
    } else {
        trimmed.parse().ok()
    }
}

// Helper type alias for the authenticated client
type AuthenticatedClient = polymarket_client_sdk::clob::Client<Authenticated<polymarket_client_sdk::auth::Normal>>;

async fn dispatch_action(
    settings: &Settings,
    clob_client: Option<&AuthenticatedClient>,
    signer: Option<&LocalSigner<alloy::signers::k256::ecdsa::SigningKey>>,
    action: OrderAction,
) -> anyhow::Result<()> {
    use polymarket_client_sdk::clob::types::Side as SdkSide;
    
    let client = match (clob_client, signer) {
        (Some(c), Some(s)) => (c, s),
        _ => {
            // DRY-RUN mode
            match &action {
                OrderAction::Place { client_id, order } => {
                    info!(
                        "📝 [DRY-RUN] place {} {} @ {:.4} (client_id={})",
                        order.side.as_str(),
                        order.qty,
                        order.price,
                        client_id
                    );
                }
                OrderAction::Cancel { id } => {
                    info!("📝 [DRY-RUN] cancel order {}", id);
                }
            }
            return Ok(());
        }
    };

    match action {
        OrderAction::Place { client_id, order } => {
            // Determine asset_id/token_id based on side
            let token_id = match order.side {
                Side::Yes => &settings.yes_asset_id,
                Side::No => &settings.no_asset_id,
            };
            
            // ✅ Round price to 0.001 precision (0.1 cent)
            let price_rounded = (order.price * 1000.0).round() / 1000.0;
            
            // ✅ Round size to 6 decimal places (API max precision)
            let size_rounded = (order.qty * 1_000_000.0).round() / 1_000_000.0;
            
            info!(
                "📤 Placing order: {} {:.6} @ {:.3} (client_id={})",
                order.side.as_str(),
                size_rounded,
                price_rounded,
                client_id
            );
            
            // Convert to rust_decimal with proper precision
            let price_decimal = rust_decimal::Decimal::from_f64(price_rounded)
                .ok_or_else(|| anyhow::anyhow!("Invalid price: {}", price_rounded))?;
            
            let size_decimal = rust_decimal::Decimal::from_f64(size_rounded)
                .ok_or_else(|| anyhow::anyhow!("Invalid size: {}", size_rounded))?;
            
            // Parse token_id to Uint256
            let token_id_uint = alloy::primitives::U256::from_str_radix(token_id, 10)
                .context("Invalid token_id")?;
            
            // Build limit order using SDK
            let sdk_order = client.0
                .limit_order()
                .token_id(token_id_uint)
                .size(size_decimal)
                .price(price_decimal)
                .side(match order.side {
                    Side::Yes => SdkSide::Buy,
                    Side::No => SdkSide::Sell,
                })
                .build()
                .await?;
            
            // Sign order using SDK (automatic EIP-712)
            let signed_order = client.0.sign(client.1, sdk_order).await?;
            
            // Post order
            let response = client.0.post_order(signed_order).await?;
            
            info!(
                "✅ Order placed: client_id={}, server_id={:?}, side={:?}, price={}, qty={}",
                client_id, response.order_id, order.side, order.price, order.qty
            );
        }
        OrderAction::Cancel { id } => {
            info!("🗑️  Canceling order: {}", id);
            
            client.0.cancel_order(&id).await?;
            
            info!("✅ Order canceled: {}", id);
        }
    }
    Ok(())
}
