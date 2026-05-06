use std::env;
use std::str::FromStr;

use alloy::primitives::Address;
use anyhow::{Context, Result};
use pm_as_ofi::polymarket::clob_v2::{
    build_signed_limit_order_v2, builder_code_from_env, infer_signature_type, post_order_v2,
    v2_contract_config, OrderSizingV2, V2OrderContext,
};
use pm_as_ofi::polymarket::executor::init_clob_client;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::types::request::{
    BalanceAllowanceRequest, OrderBookSummaryRequest,
};
use polymarket_client_sdk::clob::types::{AssetType, OrderType, SignatureType};
use rust_decimal::prelude::ToPrimitive;
use uuid::Uuid;

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

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn env_f64(key: &str) -> Result<f64> {
    env::var(key)
        .with_context(|| format!("missing {key}"))?
        .parse::<f64>()
        .with_context(|| format!("invalid {key}"))
}

fn env_u256_dec(key: &str) -> Result<alloy::primitives::U256> {
    let raw = env::var(key).with_context(|| format!("missing {key}"))?;
    alloy::primitives::U256::from_str_radix(raw.trim(), 10)
        .with_context(|| format!("invalid decimal {key}"))
}

fn signature_type_from_env_or_infer(signer: Address, funder: Address) -> SignatureType {
    let explicit = env::var("PM_SIGNATURE_TYPE")
        .ok()
        .and_then(|v| v.parse::<u8>().ok());
    infer_signature_type(signer, Some(funder), explicit)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let rest_url = env::var("POLYMARKET_REST_URL")
        .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());
    let private_key = env::var("POLYMARKET_PRIVATE_KEY").ok();
    let funder = parse_env_address("POLYMARKET_FUNDER_ADDRESS")?;
    let api_creds = read_optional_api_credentials();

    println!(
        "probe_clob_v2: rest_url={} explicit_l2_creds={} funder_set={}",
        rest_url,
        api_creds.is_some(),
        funder.is_some()
    );

    let (client, signer) =
        init_clob_client(&rest_url, private_key.as_deref(), funder, api_creds).await;
    let client = client.context("authenticated CLOB client unavailable")?;
    let signer = signer.context("local signer unavailable")?;

    println!("auth_ok signer={:?}", signer.address());

    let update_req = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .build();
    match client.update_balance_allowance(update_req.clone()).await {
        Ok(_) => println!("allowance_update_ok"),
        Err(e) => println!("allowance_update_warn err={e:?}"),
    }

    let balance_req = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .build();
    let balance_resp = client.balance_allowance(balance_req).await?;
    let raw_balance = balance_resp.balance.to_f64().unwrap_or(0.0);
    println!(
        "collateral balance_raw={} balance_pusd={:.6} allowance_entries={}",
        balance_resp.balance,
        raw_balance / 1_000_000.0,
        balance_resp.allowances.len()
    );

    for (spender, allowance) in balance_resp.allowances.iter().take(8) {
        println!("allowance spender={spender:?} raw={allowance}");
    }

    for key in ["POLYMARKET_YES_ASSET_ID", "POLYMARKET_NO_ASSET_ID"] {
        if let Ok(raw) = env::var(key) {
            if raw.trim().is_empty() {
                continue;
            }
            let token_id = alloy::primitives::U256::from_str_radix(raw.trim(), 10)
                .with_context(|| format!("invalid token id in {key}"))?;
            let book = client
                .order_book(
                    &OrderBookSummaryRequest::builder()
                        .token_id(token_id)
                        .build(),
                )
                .await
                .with_context(|| format!("order_book failed for {key}"))?;
            println!(
                "book {} token_id={} bids={} asks={} best_bid={:?} best_ask={:?}",
                key,
                raw.trim(),
                book.bids.len(),
                book.asks.len(),
                book.bids.first().map(|l| (l.price, l.size)),
                book.asks.first().map(|l| (l.price, l.size))
            );
        }
    }

    if !env_bool("PM_V2_SMOKE_PLACE_ORDER", false) {
        println!("place_order_skipped set PM_V2_SMOKE_PLACE_ORDER=true to post a minimal V2 order");
        return Ok(());
    }

    let side_raw = env::var("PM_V2_SMOKE_SIDE").unwrap_or_else(|_| "yes".into());
    let direction_raw = env::var("PM_V2_SMOKE_DIRECTION").unwrap_or_else(|_| "buy".into());
    let side_yes = !side_raw.eq_ignore_ascii_case("no");
    let direction = if direction_raw.eq_ignore_ascii_case("sell") {
        pm_as_ofi::polymarket::messages::TradeDirection::Sell
    } else {
        pm_as_ofi::polymarket::messages::TradeDirection::Buy
    };
    let token_id = if side_yes {
        env_u256_dec("POLYMARKET_YES_ASSET_ID")?
    } else {
        env_u256_dec("POLYMARKET_NO_ASSET_ID")?
    };
    let price = env_f64("PM_V2_SMOKE_PRICE")?;
    let size_shares = env_f64("PM_V2_SMOKE_SIZE")?;
    let order_type = match env::var("PM_V2_SMOKE_ORDER_TYPE")
        .unwrap_or_else(|_| "GTC".into())
        .to_ascii_uppercase()
        .as_str()
    {
        "FAK" => OrderType::FAK,
        "FOK" => OrderType::FOK,
        _ => OrderType::GTC,
    };
    let post_only = env_bool(
        "PM_V2_SMOKE_POST_ONLY",
        matches!(order_type, OrderType::GTC),
    );
    let defer_exec = env_bool("PM_V2_SMOKE_DEFER_EXEC", false);
    let funder_addr = funder.unwrap_or(signer.address());
    let sig_type = signature_type_from_env_or_infer(signer.address(), funder_addr);
    let ctx = V2OrderContext {
        exchange: v2_contract_config(false).exchange,
        maker: funder_addr,
        token_id,
        direction,
        signature_type: sig_type,
        expiration: 0,
        metadata: alloy::primitives::B256::ZERO,
        builder: builder_code_from_env(),
    };
    let signed =
        build_signed_limit_order_v2(&signer, 137, ctx, OrderSizingV2 { price, size_shares })
            .await?;
    println!(
        "signed_v2 side={} price={} size={} maker_amount={} taker_amount={} sig_type={} post_only={} order_type={:?}",
        signed.side,
        price,
        size_shares,
        signed.maker_amount,
        signed.taker_amount,
        signed.signature_type,
        post_only,
        order_type
    );

    let resp = post_order_v2(&client, &signed, order_type.clone(), post_only, defer_exec).await?;
    println!(
        "post_order success={} status={:?} order_id={} taking_amount={} making_amount={} error={:?}",
        resp.success,
        resp.status,
        resp.order_id,
        resp.taking_amount,
        resp.making_amount,
        resp.error_msg
    );

    if matches!(order_type, OrderType::GTC) && env_bool("PM_V2_SMOKE_CANCEL_ALL", true) {
        match client.cancel_all_orders().await {
            Ok(_) => println!("cancel_all_ok"),
            Err(e) => println!("cancel_all_warn err={e:?}"),
        }
    }

    Ok(())
}
