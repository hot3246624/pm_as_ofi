use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::hex::ToHexExt as _;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer as _;
use alloy::sol;
use alloy::sol_types::SolStruct as _;
use anyhow::{Context, Result};
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE as BASE64_URL_SAFE};
use base64::Engine as _;
use hmac::{Hmac, Mac as _};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal};
use polymarket_client_sdk::clob::types::response::{OrderBookSummaryResponse, PostOrderResponse};
use polymarket_client_sdk::clob::types::{OrderType, SignatureType};
use polymarket_client_sdk::clob::Client as ClobClient;
use polymarket_client_sdk::types::address;
use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use secrecy::ExposeSecret as _;
use serde::Serialize;
use sha2::Sha256;

use crate::polymarket::messages::TradeDirection;

pub type AuthClientV2 = ClobClient<Authenticated<Normal>>;

const CTF_EXCHANGE_V2_DOMAIN_NAME: &str = "Polymarket CTF Exchange";
const CTF_EXCHANGE_V2_DOMAIN_VERSION: &str = "2";
const V2_OWNER_TAKER: Address = address!("0x0000000000000000000000000000000000000000");
const USDC_SCALE_U128: u128 = 1_000_000;
const IEEE_754_SAFE_INT_MASK: u64 = (1_u64 << 53) - 1;

sol! {
    #[derive(Debug)]
    struct CtfOrderV2Signable {
        uint256 salt;
        address maker;
        address signer;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8 side;
        uint8 signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
    }
}

#[derive(Debug, Clone)]
pub struct SignedOrderV2 {
    pub salt: u64,
    pub maker: Address,
    pub signer: Address,
    pub taker: Address,
    pub token_id: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub side: &'static str,
    pub signature_type: u8,
    pub timestamp_ms: u64,
    pub expiration: u64,
    pub metadata: B256,
    pub builder: B256,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewOrderV2Body {
    pub order: NewOrderV2Payload,
    pub owner: String,
    pub order_type: OrderType,
    pub defer_exec: bool,
    pub post_only: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewOrderV2Payload {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub side: &'static str,
    pub signature_type: u8,
    pub timestamp: String,
    pub expiration: String,
    pub metadata: String,
    pub builder: String,
    pub signature: String,
}

#[derive(Debug, Clone, Copy)]
pub struct OrderSizingV2 {
    pub price: f64,
    pub size_shares: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct V2OrderContext {
    pub exchange: Address,
    pub maker: Address,
    pub token_id: U256,
    pub direction: TradeDirection,
    pub signature_type: SignatureType,
    pub expiration: u64,
    pub metadata: B256,
    pub builder: B256,
}

#[derive(Debug, Clone, Copy)]
pub struct V2ContractConfig {
    pub exchange: Address,
    pub collateral: Address,
    pub conditional_tokens: Address,
    pub neg_risk_adapter: Option<Address>,
}

#[derive(Debug, Clone, Copy)]
pub struct V2CoreContracts {
    pub p_usd: Address,
    pub ctf: Address,
    pub exchange: Address,
    pub neg_risk_exchange: Address,
    pub neg_risk_adapter: Address,
    pub collateral_onramp: Address,
    pub collateral_offramp: Address,
}

pub const V2_CONTRACTS: V2CoreContracts = V2CoreContracts {
    p_usd: address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"),
    ctf: address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"),
    exchange: address!("0xE111180000d2663C0091e4f400237545B87B996B"),
    neg_risk_exchange: address!("0xe2222d279d744050d28e00520010520000310F59"),
    neg_risk_adapter: address!("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),
    collateral_onramp: address!("0x93070a847efEf7F70739046A929D47a521F5B8ee"),
    collateral_offramp: address!("0x2957922Eb93258b93368531d39fAcCA3B4dC5854"),
};

pub const V2_STANDARD_CONFIG: V2ContractConfig = V2ContractConfig {
    exchange: V2_CONTRACTS.exchange,
    collateral: V2_CONTRACTS.p_usd,
    conditional_tokens: V2_CONTRACTS.ctf,
    neg_risk_adapter: None,
};

pub const V2_NEG_RISK_CONFIG: V2ContractConfig = V2ContractConfig {
    exchange: V2_CONTRACTS.neg_risk_exchange,
    collateral: V2_CONTRACTS.p_usd,
    conditional_tokens: V2_CONTRACTS.ctf,
    neg_risk_adapter: Some(V2_CONTRACTS.neg_risk_adapter),
};

#[must_use]
pub fn v2_contract_config(is_neg_risk: bool) -> &'static V2ContractConfig {
    if is_neg_risk {
        &V2_NEG_RISK_CONFIG
    } else {
        &V2_STANDARD_CONFIG
    }
}

#[must_use]
pub fn infer_signature_type(
    signer: Address,
    funder: Option<Address>,
    explicit: Option<u8>,
) -> SignatureType {
    match explicit {
        Some(2) => SignatureType::GnosisSafe,
        Some(1) => SignatureType::Proxy,
        Some(0) => SignatureType::Eoa,
        _ => match funder {
            Some(funder) if funder == signer => SignatureType::Eoa,
            Some(funder)
                if polymarket_client_sdk::derive_safe_wallet(signer, 137) == Some(funder) =>
            {
                SignatureType::GnosisSafe
            }
            Some(_) => SignatureType::Proxy,
            None => SignatureType::Eoa,
        },
    }
}

#[must_use]
pub fn builder_code_from_env() -> B256 {
    std::env::var("PM_CLOB_V2_BUILDER_CODE")
        .ok()
        .and_then(|s| s.parse::<B256>().ok())
        .unwrap_or(B256::ZERO)
}

fn hmac_signature(secret: &str, message: &str) -> Result<String> {
    let decoded_secret = BASE64_URL_SAFE
        .decode(secret.as_bytes())
        .or_else(|_| BASE64_STANDARD.decode(secret.as_bytes()))
        .context("invalid L2 API secret (base64 decode failed)")?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&decoded_secret)
        .context("invalid HMAC key length for L2 API secret")?;
    mac.update(message.as_bytes());
    Ok(BASE64_URL_SAFE.encode(mac.finalize().into_bytes()))
}

fn l2_headers(
    address: Address,
    credentials: &Credentials,
    request_path: &str,
    body: &str,
    timestamp: u64,
) -> Result<HeaderMap> {
    let message = format!("{timestamp}POST{request_path}{body}");
    let signature = hmac_signature(credentials.secret().expose_secret(), &message)?;
    let mut headers = HeaderMap::new();
    headers.insert(
        "POLY_ADDRESS",
        HeaderValue::from_str(&address.encode_hex_with_prefix())?,
    );
    headers.insert(
        "POLY_API_KEY",
        HeaderValue::from_str(&credentials.key().to_string())?,
    );
    headers.insert(
        "POLY_PASSPHRASE",
        HeaderValue::from_str(credentials.passphrase().expose_secret())?,
    );
    headers.insert("POLY_SIGNATURE", HeaderValue::from_str(&signature)?);
    headers.insert(
        "POLY_TIMESTAMP",
        HeaderValue::from_str(&timestamp.to_string())?,
    );
    Ok(headers)
}

fn decimal_to_u256_6dp(value: Decimal) -> Result<U256> {
    let scaled = (value * Decimal::from(USDC_SCALE_U128)).round_dp(0);
    let n = scaled
        .to_u128()
        .with_context(|| format!("value out of range for 6dp conversion: {value}"))?;
    Ok(U256::from(n))
}

fn now_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn generate_v2_salt() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    nanos & IEEE_754_SAFE_INT_MASK
}

fn side_label(direction: TradeDirection) -> &'static str {
    match direction {
        TradeDirection::Buy => "BUY",
        TradeDirection::Sell => "SELL",
    }
}

pub async fn build_signed_limit_order_v2(
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    chain_id: u64,
    ctx: V2OrderContext,
    sizing: OrderSizingV2,
) -> Result<SignedOrderV2> {
    let price = Decimal::from_f64(sizing.price)
        .with_context(|| format!("invalid V2 price {}", sizing.price))?;
    let size = Decimal::from_f64(sizing.size_shares)
        .with_context(|| format!("invalid V2 size {}", sizing.size_shares))?;
    let (maker_amount_dec, taker_amount_dec) = match ctx.direction {
        TradeDirection::Buy => ((size * price).round_dp(6), size.round_dp(6)),
        TradeDirection::Sell => (size.round_dp(6), (size * price).round_dp(6)),
    };
    let maker_amount = decimal_to_u256_6dp(maker_amount_dec)?;
    let taker_amount = decimal_to_u256_6dp(taker_amount_dec)?;
    let timestamp_ms = now_timestamp_ms();
    let salt = generate_v2_salt();

    let signable = CtfOrderV2Signable {
        salt: U256::from(salt),
        maker: ctx.maker,
        signer: signer.address(),
        tokenId: ctx.token_id,
        makerAmount: maker_amount,
        takerAmount: taker_amount,
        side: match ctx.direction {
            TradeDirection::Buy => 0,
            TradeDirection::Sell => 1,
        },
        signatureType: ctx.signature_type as u8,
        timestamp: U256::from(timestamp_ms),
        metadata: ctx.metadata,
        builder: ctx.builder,
    };

    let domain = Eip712Domain {
        name: Some(Cow::Borrowed(CTF_EXCHANGE_V2_DOMAIN_NAME)),
        version: Some(Cow::Borrowed(CTF_EXCHANGE_V2_DOMAIN_VERSION)),
        chain_id: Some(U256::from(chain_id)),
        verifying_contract: Some(ctx.exchange),
        ..Eip712Domain::default()
    };
    let hash = signable.eip712_signing_hash(&domain);
    let signature = signer.sign_hash(&hash).await.map(|s| s.to_string())?;

    Ok(SignedOrderV2 {
        salt,
        maker: ctx.maker,
        signer: signer.address(),
        taker: V2_OWNER_TAKER,
        token_id: ctx.token_id,
        maker_amount,
        taker_amount,
        side: side_label(ctx.direction),
        signature_type: ctx.signature_type as u8,
        timestamp_ms,
        expiration: ctx.expiration,
        metadata: ctx.metadata,
        builder: ctx.builder,
        signature,
    })
}

pub async fn post_order_v2(
    client: &AuthClientV2,
    order: &SignedOrderV2,
    order_type: OrderType,
    post_only: bool,
    defer_exec: bool,
) -> Result<PostOrderResponse> {
    let payload = NewOrderV2Body {
        order: NewOrderV2Payload {
            salt: order.salt,
            maker: order.maker.encode_hex_with_prefix(),
            signer: order.signer.encode_hex_with_prefix(),
            taker: order.taker.encode_hex_with_prefix(),
            token_id: order.token_id.to_string(),
            maker_amount: order.maker_amount.to_string(),
            taker_amount: order.taker_amount.to_string(),
            side: order.side,
            signature_type: order.signature_type,
            timestamp: order.timestamp_ms.to_string(),
            expiration: order.expiration.to_string(),
            metadata: order.metadata.encode_hex_with_prefix(),
            builder: order.builder.encode_hex_with_prefix(),
            signature: order.signature.clone(),
        },
        owner: client.credentials().key().to_string(),
        order_type,
        defer_exec,
        post_only,
    };
    let request_path = "/order";
    let body = serde_json::to_string(&payload)?;
    let url = client.host().join("order")?;
    let timestamp = client
        .server_time()
        .await
        .ok()
        .and_then(|v| u64::try_from(v).ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        });
    let headers = l2_headers(
        client.address(),
        client.credentials(),
        request_path,
        &body,
        timestamp,
    )?;
    let http = reqwest::Client::new();
    let resp = http
        .post(url)
        .headers(headers)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .context("V2 /order request failed")?;
    let status = resp.status();
    let text = resp.text().await.context("V2 /order body read failed")?;
    if !status.is_success() {
        anyhow::bail!("V2 /order HTTP {} {}", status, text);
    }
    let parsed = serde_json::from_str::<PostOrderResponse>(&text)
        .with_context(|| format!("failed to parse V2 /order response: {text}"))?;
    Ok(parsed)
}

pub fn marketable_limit_from_book(
    book: &OrderBookSummaryResponse,
    direction: TradeDirection,
    shares: Decimal,
    order_type: OrderType,
) -> Result<Decimal> {
    let levels = match direction {
        TradeDirection::Buy => &book.asks,
        TradeDirection::Sell => &book.bids,
    };
    let first = levels
        .first()
        .with_context(|| format!("no opposing orders for token {}", book.asset_id))?;
    let mut sum = Decimal::ZERO;
    let cutoff = levels.iter().rev().find_map(|level| {
        sum += level.size;
        (sum >= shares).then_some(level.price)
    });
    match cutoff {
        Some(px) => Ok(px),
        None if matches!(order_type, OrderType::FOK) => {
            anyhow::bail!("insufficient liquidity to fill {} shares", shares)
        }
        None => Ok(first.price),
    }
}
