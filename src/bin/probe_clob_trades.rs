use std::collections::HashSet;
use std::env;
use std::str::FromStr;

use alloy::primitives::Address;
use anyhow::{Context, Result};
use pm_as_ofi::polymarket::executor::init_clob_client;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::types::request::TradesRequest;
use polymarket_client_sdk::clob::types::response::Page;
use polymarket_client_sdk::clob::types::response::TradeResponse;
use polymarket_client_sdk::clob::types::TraderSide;
use uuid::Uuid;

const DEFAULT_TARGET: &str = "0xcfb103c37c0234f524c632d964ed31f117b5f694";

#[derive(Debug, Clone, Copy)]
struct ProbeStats {
    count: usize,
    maker_side: usize,
    taker_side: usize,
    unknown_side: usize,
    has_maker_orders: usize,
    maker_addr_match: usize,
    unique_maker_addrs: usize,
}

impl ProbeStats {
    fn from_page(page: &Page<TradeResponse>, target: Address) -> Self {
        let mut out = Self {
            count: page.data.len(),
            maker_side: 0,
            taker_side: 0,
            unknown_side: 0,
            has_maker_orders: 0,
            maker_addr_match: 0,
            unique_maker_addrs: 0,
        };
        let mut maker_set = HashSet::new();
        for trade in &page.data {
            match trade.trader_side {
                TraderSide::Maker => out.maker_side += 1,
                TraderSide::Taker => out.taker_side += 1,
                TraderSide::Unknown(_) => out.unknown_side += 1,
                _ => out.unknown_side += 1,
            }
            if !trade.maker_orders.is_empty() {
                out.has_maker_orders += 1;
            }
            if trade.maker_address == target {
                out.maker_addr_match += 1;
            }
            maker_set.insert(trade.maker_address);
        }
        out.unique_maker_addrs = maker_set.len();
        out
    }
}

fn read_optional_api_credentials() -> Option<Credentials> {
    let key = env::var("POLYMARKET_API_KEY").ok();
    let secret = env::var("POLYMARKET_API_SECRET").ok();
    let passphrase = env::var("POLYMARKET_API_PASSPHRASE").ok();
    match (key, secret, passphrase) {
        (Some(key_raw), Some(secret), Some(passphrase))
            if !key_raw.trim().is_empty()
                && !secret.trim().is_empty()
                && !passphrase.trim().is_empty() =>
        {
            let key = Uuid::from_str(&key_raw).ok()?;
            Some(Credentials::new(key, secret, passphrase))
        }
        _ => None,
    }
}

fn parse_env_address(var: &str) -> Result<Option<Address>> {
    let raw = match env::var(var) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Ok(None),
    };
    let addr = Address::from_str(&raw)
        .with_context(|| format!("{var} is present but not a valid address: {raw}"))?;
    Ok(Some(addr))
}

async fn fetch_once(
    client: &pm_as_ofi::polymarket::executor::AuthClient,
    label: &str,
    target: Address,
    req: &TradesRequest,
    tx_hash_hint: Option<&str>,
) -> Result<()> {
    let page = client
        .trades(req, None)
        .await
        .with_context(|| format!("failed querying /data/trades for {label}"))?;

    let stats = ProbeStats::from_page(&page, target);
    println!(
        "{label}: count={} next_cursor={} maker_side={} taker_side={} unknown_side={} has_maker_orders={} maker_addr_match={} unique_maker_addrs={}",
        stats.count,
        page.next_cursor,
        stats.maker_side,
        stats.taker_side,
        stats.unknown_side,
        stats.has_maker_orders,
        stats.maker_addr_match,
        stats.unique_maker_addrs
    );

    for (idx, trade) in page.data.iter().take(3).enumerate() {
        println!(
            "{label}[{idx}]: id={} side={:?} trader_side={:?} price={} size={} outcome={} maker_orders={} taker_order_id={} tx={} match_time={} maker_address={} owner={}",
            trade.id,
            trade.side,
            trade.trader_side,
            trade.price,
            trade.size,
            trade.outcome,
            trade.maker_orders.len(),
            trade.taker_order_id,
            trade.transaction_hash,
            trade.match_time,
            trade.maker_address,
            trade.owner
        );
    }

    if let Some(tx_hint) = tx_hash_hint {
        let tx_hint = tx_hint.to_ascii_lowercase();
        let hit = page.data.iter().find(|t| {
            format!("{:?}", t.transaction_hash).to_ascii_lowercase() == tx_hint
                || t.transaction_hash.to_string().to_ascii_lowercase() == tx_hint
        });
        if let Some(hit) = hit {
            println!(
                "{label}: tx_hit id={} side={:?} trader_side={:?} price={} size={} outcome={} maker_address={} taker_order_id={}",
                hit.id,
                hit.side,
                hit.trader_side,
                hit.price,
                hit.size,
                hit.outcome,
                hit.maker_address,
                hit.taker_order_id
            );
        } else {
            println!("{label}: tx_hit=NONE");
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let rest_url = env::var("PM_CLOB_URL").unwrap_or_else(|_| "https://clob.polymarket.com".into());
    let private_key = env::var("POLYMARKET_PRIVATE_KEY").ok();
    let funder = parse_env_address("POLYMARKET_FUNDER_ADDRESS")?;
    let api_creds = read_optional_api_credentials();
    let target = env::var("PM_PROBE_TRADER").unwrap_or_else(|_| DEFAULT_TARGET.to_string());
    let target_addr = Address::from_str(&target)
        .with_context(|| format!("PM_PROBE_TRADER is not a valid address: {target}"))?;
    let after = env::var("PM_PROBE_AFTER")
        .ok()
        .and_then(|v| v.parse::<i64>().ok());
    let before = env::var("PM_PROBE_BEFORE")
        .ok()
        .and_then(|v| v.parse::<i64>().ok());
    let tx_hint = env::var("PM_PROBE_TX_HASH").ok();

    println!(
        "probe config: rest_url={} target={} explicit_l2_creds={} funder_set={} after={:?} before={:?}",
        rest_url,
        target_addr,
        api_creds.is_some(),
        funder.is_some(),
        after,
        before
    );

    let (client, _) = init_clob_client(&rest_url, private_key.as_deref(), funder, api_creds).await;
    let client = client.context("authenticated CLOB client unavailable")?;

    let maker_req = TradesRequest::builder().maker_address(target_addr).build();
    let taker_req = TradesRequest::builder().taker_address(target_addr).build();
    let mut window_req = TradesRequest::default();
    window_req.after = after;
    window_req.before = before;

    fetch_once(
        &client,
        "maker",
        target_addr,
        &maker_req,
        tx_hint.as_deref(),
    )
    .await?;
    fetch_once(
        &client,
        "taker",
        target_addr,
        &taker_req,
        tx_hint.as_deref(),
    )
    .await?;
    fetch_once(
        &client,
        "window",
        target_addr,
        &window_req,
        tx_hint.as_deref(),
    )
    .await?;
    Ok(())
}
