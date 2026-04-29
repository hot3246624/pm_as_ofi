//! Claim monitoring utilities.
//!
//! This module scans Data API for redeemable positions so the trading loop can
//! surface claimable winnings promptly.

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::U256;
use alloy::providers::ProviderBuilder;
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use alloy::signers::SignerSync;
use alloy::sol;
use alloy::sol_types::SolCall;
use alloy::sol_types::SolStruct;
use anyhow::Context;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE as BASE64_URL_SAFE};
use base64::Engine as _;
use hmac::{Hmac, Mac as _};
use polymarket_client_sdk::ctf::types::{
    MergePositionsRequest, RedeemNegRiskRequest, RedeemPositionsRequest,
};
use polymarket_client_sdk::ctf::Client as CtfClient;
use polymarket_client_sdk::data::types::request::PositionsRequest;
use polymarket_client_sdk::data::types::MarketFilter;
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::types::{Address, Decimal, B256};
use polymarket_client_sdk::POLYGON;
use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::prelude::FromPrimitive;
use serde::Serialize;
use serde_json::Value;
use sha2::Sha256;

use crate::polymarket::clob_v2::v2_contract_config;

sol! {
    /// Minimal Safe EIP-712 transaction payload used by the relayer flow.
    struct SafeTx {
        address to;
        uint256 value;
        bytes data;
        uint8 operation;
        uint256 safeTxGas;
        uint256 baseGas;
        uint256 gasPrice;
        address gasToken;
        address refundReceiver;
        uint256 nonce;
    }

    interface IConditionalTokensAutoClaim {
        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] partition,
            uint256 amount
        );

        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        );
    }

    interface INegRiskAdapterAutoClaim {
        function redeemPositions(bytes32 conditionId, uint256[] amounts);
    }
}

const RELAYER_DEFAULT_URL: &str = "https://relayer-v2.polymarket.com";
const RELAYER_SAFE_TX_TYPE: &str = "SAFE";
const RELAYER_POLL_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct ClaimableCondition {
    pub condition_id: B256,
    pub positions: usize,
    pub total_value: Decimal,
    pub negative_risk: bool,
    pub yes_size: Decimal,
    pub no_size: Decimal,
}

#[derive(Debug, Clone)]
pub struct ClaimableSummary {
    pub positions: usize,
    pub conditions: usize,
    pub total_value: Decimal,
    pub top_conditions: Vec<ClaimableCondition>,
    pub all_conditions: Vec<ClaimableCondition>,
}

#[derive(Debug, Clone)]
pub struct BuilderCredentials {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

#[derive(Debug, Clone)]
pub struct AutoClaimConfig {
    pub enabled: bool,
    pub dry_run: bool,
    pub min_condition_value: Decimal,
    pub max_conditions_per_run: usize,
    pub run_interval: Duration,
    pub rpc_url: String,
    pub data_api_url: String,
    pub relayer_url: String,
    pub builder_credentials: Option<BuilderCredentials>,
    pub builder_credentials_partial: bool,
    pub signature_type: Option<u8>,
    pub relayer_wait_confirm: bool,
    pub relayer_wait_timeout: Duration,
}

#[derive(Debug, Default)]
pub struct AutoClaimState {
    pub last_run: Option<Instant>,
    pub warned_safe_mode: bool,
    pub warned_builder_creds_partial: bool,
    pub warned_builder_creds_missing: bool,
    pub warned_proxy_mode: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AutoClaimRunResult {
    pub positions: usize,
    pub candidates: usize,
    pub claimed: usize,
}

impl AutoClaimRunResult {
    #[must_use]
    pub fn succeeded(self, dry_run: bool) -> bool {
        if dry_run {
            self.candidates > 0
        } else {
            self.claimed > 0
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimExecutionMode {
    EoaOnchain,
    SafeRelayer,
    ProxyRelayerUnsupported,
    UnknownProxyOrSafe,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SafeSignatureParamsPayload {
    gas_price: String,
    operation: String,
    safe_txn_gas: String,
    base_gas: String,
    gas_token: String,
    refund_receiver: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SafeSubmitPayload {
    #[serde(rename = "type")]
    tx_type: String,
    #[serde(rename = "from")]
    from_address: String,
    to: String,
    proxy_wallet: String,
    value: String,
    data: String,
    nonce: String,
    signature: String,
    metadata: String,
    signature_params: SafeSignatureParamsPayload,
}

/// Scan redeemable positions for `user` from Data API.
///
/// Returns a compact summary grouped by condition ID.
pub async fn scan_claimable_positions(
    data_api_url: &str,
    user: Address,
) -> anyhow::Result<ClaimableSummary> {
    let client = DataClient::new(data_api_url)
        .with_context(|| format!("invalid data api url: {data_api_url}"))?;

    let mut offset = 0_i32;
    let page_size = 500_i32;
    let mut total_positions = 0_usize;
    let mut by_condition: BTreeMap<B256, ClaimableCondition> = BTreeMap::new();

    loop {
        let req = PositionsRequest::builder()
            .user(user)
            .redeemable(true)
            .limit(page_size)?
            .offset(offset)?
            .build();

        let rows = client.positions(&req).await?;
        if rows.is_empty() {
            break;
        }

        let row_count = rows.len();
        for row in rows {
            total_positions += 1;
            let e = by_condition
                .entry(row.condition_id)
                .or_insert(ClaimableCondition {
                    condition_id: row.condition_id,
                    positions: 0,
                    total_value: Decimal::ZERO,
                    negative_risk: row.negative_risk,
                    yes_size: Decimal::ZERO,
                    no_size: Decimal::ZERO,
                });
            e.positions += 1;
            e.total_value += row.current_value;
            e.negative_risk = e.negative_risk || row.negative_risk;
            let outcome = row.outcome.trim().to_ascii_lowercase();
            if outcome == "yes" {
                e.yes_size += row.size;
            } else if outcome == "no" {
                e.no_size += row.size;
            }
        }

        if row_count < page_size as usize {
            break;
        }
        offset = offset.saturating_add(page_size);
        if offset >= 10_000 {
            break;
        }
    }

    let mut all_conditions: Vec<ClaimableCondition> = by_condition.into_values().collect();

    all_conditions.sort_by(|a, b| b.total_value.cmp(&a.total_value));
    let total_value = all_conditions
        .iter()
        .fold(Decimal::ZERO, |acc, c| acc + c.total_value);
    let top_conditions = all_conditions.iter().take(5).cloned().collect();
    Ok(ClaimableSummary {
        positions: total_positions,
        conditions: all_conditions.len(),
        total_value,
        top_conditions,
        all_conditions,
    })
}

/// Scan currently mergeable YES/NO inventory for one market and return
/// the maximum full-set amount that can be merged (in pUSD units).
pub async fn scan_mergeable_full_set_usdc(
    data_api_url: &str,
    user: Address,
    condition_id: B256,
) -> anyhow::Result<Decimal> {
    let client = DataClient::new(data_api_url)
        .with_context(|| format!("invalid data api url: {data_api_url}"))?;

    let mut offset = 0_i32;
    let page_size = 500_i32;
    let mut yes_qty = Decimal::ZERO;
    let mut no_qty = Decimal::ZERO;

    loop {
        let req = PositionsRequest::builder()
            .user(user)
            .filter(MarketFilter::markets([condition_id]))
            .mergeable(true)
            .limit(page_size)?
            .offset(offset)?
            .build();

        let rows = client.positions(&req).await?;
        if rows.is_empty() {
            break;
        }

        let row_count = rows.len();
        for row in rows {
            let outcome = row.outcome.trim().to_ascii_lowercase();
            if outcome == "yes" || row.outcome_index == 0 {
                yes_qty += row.size;
            } else if outcome == "no" || row.outcome_index == 1 {
                no_qty += row.size;
            }
        }

        if row_count < page_size as usize {
            break;
        }
        offset = offset.saturating_add(page_size);
        if offset >= 10_000 {
            break;
        }
    }

    Ok(if yes_qty < no_qty { yes_qty } else { no_qty })
}

/// Execute a market-level collateral merge for one condition.
///
/// This supports:
/// - EOA mode: direct on-chain `merge_positions`.
/// - SAFE mode: relayer submit with SAFE signature.
///
/// Proxy mode is currently unsupported.
pub async fn execute_market_merge(
    cfg: &AutoClaimConfig,
    funder_address: Option<&str>,
    signer_address: Option<&str>,
    private_key: Option<&str>,
    condition_id: B256,
    amount_usdc: Decimal,
    dry_run: bool,
) -> anyhow::Result<()> {
    if amount_usdc <= Decimal::ZERO {
        return Ok(());
    }

    let Some(funder_raw) = funder_address else {
        anyhow::bail!("missing funder address for merge execution");
    };
    let funder = funder_raw
        .trim()
        .parse::<Address>()
        .with_context(|| format!("invalid funder address: {funder_raw}"))?;

    let Some(signer_raw) = signer_address else {
        anyhow::bail!("missing signer address for merge execution");
    };
    let signer = signer_raw
        .trim()
        .parse::<Address>()
        .with_context(|| format!("invalid signer address: {signer_raw}"))?;

    let mode = detect_claim_execution_mode(signer, funder, cfg.signature_type);
    let amount_u256 = decimal_to_u256_6(amount_usdc)
        .filter(|v| !v.is_zero())
        .with_context(|| format!("invalid merge amount (usdc): {}", amount_usdc))?;

    if dry_run {
        tracing::info!(
            "📝 [RECYCLE DRY-RUN] merge condition={} amount_usdc={} mode={:?}",
            condition_id,
            amount_usdc,
            mode
        );
        return Ok(());
    }

    let Some(pk) = private_key else {
        anyhow::bail!("POLYMARKET_PRIVATE_KEY is required for merge execution");
    };

    match mode {
        ClaimExecutionMode::EoaOnchain => {
            let signer_wallet: LocalSigner<alloy::signers::k256::ecdsa::SigningKey> = pk.parse()?;
            let signer_wallet = signer_wallet.with_chain_id(Some(POLYGON));
            let provider = ProviderBuilder::new()
                .wallet(signer_wallet)
                .connect(&cfg.rpc_url)
                .await
                .with_context(|| format!("connect rpc failed: {}", cfg.rpc_url))?;
            let standard = CtfClient::new(provider, POLYGON)?;
            let collateral = v2_contract_config(false).collateral;
            let req =
                MergePositionsRequest::for_binary_market(collateral, condition_id, amount_u256);
            let resp = standard.merge_positions(&req).await?;
            tracing::info!(
                "♻️ Merge success (EOA): condition={} amount_usdc={} tx={} block={}",
                condition_id,
                amount_usdc,
                resp.transaction_hash,
                resp.block_number
            );
            Ok(())
        }
        ClaimExecutionMode::SafeRelayer => {
            let creds = cfg
                .builder_credentials
                .as_ref()
                .context("POLYMARKET_BUILDER_* credentials are required for SAFE merge")?;
            let signer_wallet: LocalSigner<alloy::signers::k256::ecdsa::SigningKey> = pk
                .parse()
                .context("invalid POLYMARKET_PRIVATE_KEY for SAFE merge signing")?;
            let standard_cfg = v2_contract_config(false);
            let to = standard_cfg.conditional_tokens;
            let collateral = standard_cfg.collateral;
            let call = IConditionalTokensAutoClaim::mergePositionsCall {
                collateralToken: collateral,
                parentCollectionId: B256::ZERO,
                conditionId: condition_id,
                partition: vec![U256::from(1_u8), U256::from(2_u8)],
                amount: amount_u256,
            };

            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()?;
            let metadata = format!("pm_as_ofi:recycle:merge:{}", condition_id);
            let tx_id = relayer_submit_safe_claim(
                &http,
                cfg,
                creds,
                &signer_wallet,
                signer,
                funder,
                to,
                call.abi_encode(),
                metadata,
            )
            .await
            .with_context(|| {
                format!("relayer submit failed for merge condition {}", condition_id)
            })?;

            if !cfg.relayer_wait_confirm {
                tracing::info!(
                    "♻️ Merge SAFE submitted: condition={} amount_usdc={} tx_id={} (wait_confirm=false)",
                    condition_id,
                    amount_usdc,
                    tx_id
                );
                return Ok(());
            }

            match relayer_wait_transaction(&http, cfg, &tx_id).await? {
                Some(hash) => tracing::info!(
                    "♻️ Merge SAFE confirmed: condition={} amount_usdc={} tx_id={} tx_hash={}",
                    condition_id,
                    amount_usdc,
                    tx_id,
                    hash
                ),
                None => tracing::warn!(
                    "⚠️ Merge SAFE pending timeout after {}s: condition={} amount_usdc={} tx_id={}",
                    cfg.relayer_wait_timeout.as_secs(),
                    condition_id,
                    amount_usdc,
                    tx_id
                ),
            }
            Ok(())
        }
        ClaimExecutionMode::ProxyRelayerUnsupported => {
            anyhow::bail!(
                "merge skipped: proxy execution mode unsupported (signer={} funder={})",
                signer,
                funder
            )
        }
        ClaimExecutionMode::UnknownProxyOrSafe => {
            anyhow::bail!(
                "merge skipped: unknown proxy/safe mode; set PM_SIGNATURE_TYPE explicitly"
            )
        }
    }
}

impl AutoClaimConfig {
    #[must_use]
    pub fn from_env() -> Self {
        let enabled = std::env::var("PM_AUTO_CLAIM")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let dry_run = std::env::var("PM_AUTO_CLAIM_DRY_RUN")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let min_condition_value = std::env::var("PM_AUTO_CLAIM_MIN_VALUE")
            .ok()
            .and_then(|v| v.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        let max_conditions_per_run = std::env::var("PM_AUTO_CLAIM_MAX_CONDITIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(5);
        let run_interval = Duration::from_secs(
            std::env::var("PM_AUTO_CLAIM_INTERVAL_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(300),
        );
        let rpc_url = std::env::var("POLYMARKET_RPC_URL")
            .unwrap_or_else(|_| "https://polygon-rpc.com".to_string());
        let data_api_url = std::env::var("POLYMARKET_DATA_API_URL")
            .unwrap_or_else(|_| "https://data-api.polymarket.com".to_string());
        let relayer_url = std::env::var("POLYMARKET_RELAYER_URL")
            .unwrap_or_else(|_| RELAYER_DEFAULT_URL.to_string());
        let signature_type = std::env::var("PM_SIGNATURE_TYPE")
            .ok()
            .and_then(|v| v.parse::<u8>().ok());
        let relayer_wait_confirm = std::env::var("PM_AUTO_CLAIM_WAIT_CONFIRM")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let relayer_wait_timeout = Duration::from_secs(
            std::env::var("PM_AUTO_CLAIM_WAIT_TIMEOUT_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(20),
        );

        let builder_api_key = std::env::var("POLYMARKET_BUILDER_API_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let builder_secret = std::env::var("POLYMARKET_BUILDER_SECRET")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let builder_passphrase = std::env::var("POLYMARKET_BUILDER_PASSPHRASE")
            .ok()
            .filter(|s| !s.trim().is_empty());

        let (builder_credentials, builder_credentials_partial) =
            match (builder_api_key, builder_secret, builder_passphrase) {
                (Some(api_key), Some(secret), Some(passphrase)) => (
                    Some(BuilderCredentials {
                        api_key,
                        secret,
                        passphrase,
                    }),
                    false,
                ),
                (None, None, None) => (None, false),
                _ => (None, true),
            };

        Self {
            enabled,
            dry_run,
            min_condition_value,
            max_conditions_per_run,
            run_interval,
            rpc_url,
            data_api_url,
            relayer_url,
            builder_credentials,
            builder_credentials_partial,
            signature_type,
            relayer_wait_confirm,
            relayer_wait_timeout,
        }
    }
}

fn decimal_to_u256_6(dec: Decimal) -> Option<U256> {
    if dec <= Decimal::ZERO {
        return Some(U256::ZERO);
    }
    let scale = Decimal::from_u64(1_000_000)?;
    let scaled = (dec * scale).trunc();
    U256::from_str_radix(&scaled.to_string(), 10).ok()
}

fn parse_u256_any(raw: &str) -> Option<U256> {
    let text = raw.trim();
    if text.is_empty() {
        return None;
    }
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        U256::from_str_radix(hex, 16).ok()
    } else {
        U256::from_str_radix(text, 10).ok()
    }
}

fn detect_claim_execution_mode(
    signer: Address,
    funder: Address,
    signature_type: Option<u8>,
) -> ClaimExecutionMode {
    if signer == funder {
        return ClaimExecutionMode::EoaOnchain;
    }

    let derived_safe = polymarket_client_sdk::derive_safe_wallet(signer, POLYGON);
    let derived_proxy = polymarket_client_sdk::derive_proxy_wallet(signer, POLYGON);

    if signature_type == Some(2) || derived_safe == Some(funder) {
        return ClaimExecutionMode::SafeRelayer;
    }
    if signature_type == Some(1) || derived_proxy == Some(funder) {
        return ClaimExecutionMode::ProxyRelayerUnsupported;
    }

    ClaimExecutionMode::UnknownProxyOrSafe
}

fn make_candidates(summary: ClaimableSummary, cfg: &AutoClaimConfig) -> Vec<ClaimableCondition> {
    let mut candidates = summary.all_conditions;
    candidates.retain(|c| c.total_value >= cfg.min_condition_value);
    candidates.truncate(cfg.max_conditions_per_run);
    candidates
}

fn make_builder_headers(
    creds: &BuilderCredentials,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> anyhow::Result<HeaderMap> {
    let secret = BASE64_URL_SAFE
        .decode(creds.secret.as_bytes())
        .or_else(|_| BASE64_STANDARD.decode(creds.secret.as_bytes()))
        .context("invalid POLYMARKET_BUILDER_SECRET (base64 decode failed)")?;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs()
        .to_string();

    let mut message = format!("{timestamp}{method}{path}");
    if let Some(body) = body {
        if !body.is_empty() {
            message.push_str(body);
        }
    }

    let mut mac: Hmac<Sha256> =
        Hmac::new_from_slice(&secret).context("invalid HMAC key length for builder secret")?;
    mac.update(message.as_bytes());
    let signature = BASE64_URL_SAFE.encode(mac.finalize().into_bytes());

    let mut headers = HeaderMap::new();
    headers.insert(
        "POLY_BUILDER_API_KEY",
        HeaderValue::from_str(&creds.api_key).context("invalid POLY_BUILDER_API_KEY header")?,
    );
    headers.insert(
        "POLY_BUILDER_PASSPHRASE",
        HeaderValue::from_str(&creds.passphrase)
            .context("invalid POLYMARKET_BUILDER_PASSPHRASE header")?,
    );
    headers.insert(
        "POLY_BUILDER_TIMESTAMP",
        HeaderValue::from_str(&timestamp).context("invalid builder timestamp header")?,
    );
    headers.insert(
        "POLY_BUILDER_SIGNATURE",
        HeaderValue::from_str(&signature).context("invalid builder signature header")?,
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );

    Ok(headers)
}

async fn relayer_safe_is_deployed(
    http: &reqwest::Client,
    relayer_url: &str,
    safe_wallet: Address,
) -> anyhow::Result<bool> {
    let base = relayer_url.trim_end_matches('/');
    let url = format!("{base}/deployed?address={safe_wallet:#x}");
    let resp = http.get(url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("relayer deployed check failed ({status}): {body}");
    }
    let value: Value = resp.json().await?;
    if let Some(v) = value.as_bool() {
        return Ok(v);
    }
    if let Some(v) = value.get("deployed").and_then(Value::as_bool) {
        return Ok(v);
    }
    anyhow::bail!("unexpected /deployed response: {value}")
}

async fn relayer_safe_nonce(
    http: &reqwest::Client,
    relayer_url: &str,
    signer: Address,
) -> anyhow::Result<String> {
    let base = relayer_url.trim_end_matches('/');
    let url = format!("{base}/nonce?address={signer:#x}&type=SAFE");
    let resp = http.get(url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("relayer nonce failed ({status}): {body}");
    }
    let value: Value = resp.json().await?;
    if let Some(v) = value.get("nonce") {
        if let Some(s) = v.as_str() {
            return Ok(s.to_string());
        }
        if v.is_number() {
            return Ok(v.to_string());
        }
    }
    if let Some(s) = value.as_str() {
        return Ok(s.to_string());
    }
    if value.is_number() {
        return Ok(value.to_string());
    }
    anyhow::bail!("unexpected /nonce response: {value}")
}

fn safe_signature_from_struct_hash(
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    struct_hash: B256,
) -> anyhow::Result<String> {
    let sig = signer.sign_message_sync(struct_hash.as_slice())?;
    let mut raw = sig.as_bytes();

    raw[64] = match raw[64] {
        0 | 1 => raw[64] + 31,
        27 | 28 => raw[64] + 4,
        other => {
            anyhow::bail!("unexpected signature v value for safe relayer signing: {other}");
        }
    };

    Ok(format!("0x{}", hex::encode(raw)))
}

fn safe_struct_hash(proxy_wallet: Address, to: Address, data: Vec<u8>, nonce: U256) -> B256 {
    let tx = SafeTx {
        to,
        value: U256::ZERO,
        data: data.into(),
        operation: 0,
        safeTxGas: U256::ZERO,
        baseGas: U256::ZERO,
        gasPrice: U256::ZERO,
        gasToken: Address::ZERO,
        refundReceiver: Address::ZERO,
        nonce,
    };

    let domain = Eip712Domain {
        chain_id: Some(U256::from(POLYGON)),
        verifying_contract: Some(proxy_wallet),
        ..Eip712Domain::default()
    };

    tx.eip712_signing_hash(&domain)
}

#[allow(clippy::too_many_arguments)]
async fn relayer_submit_safe_claim(
    http: &reqwest::Client,
    cfg: &AutoClaimConfig,
    creds: &BuilderCredentials,
    signer_wallet: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    signer: Address,
    proxy_wallet: Address,
    to: Address,
    data: Vec<u8>,
    metadata: String,
) -> anyhow::Result<String> {
    let nonce_raw = relayer_safe_nonce(http, &cfg.relayer_url, signer).await?;
    let nonce = parse_u256_any(&nonce_raw)
        .with_context(|| format!("invalid relayer nonce '{nonce_raw}'"))?;

    let hash = safe_struct_hash(proxy_wallet, to, data.clone(), nonce);
    let signature = safe_signature_from_struct_hash(signer_wallet, hash)?;

    let payload = SafeSubmitPayload {
        tx_type: RELAYER_SAFE_TX_TYPE.to_string(),
        from_address: format!("{signer:#x}"),
        to: format!("{to:#x}"),
        proxy_wallet: format!("{proxy_wallet:#x}"),
        value: "0".to_string(),
        data: format!("0x{}", hex::encode(data)),
        nonce: nonce_raw,
        signature,
        metadata,
        signature_params: SafeSignatureParamsPayload {
            gas_price: "0".to_string(),
            operation: "0".to_string(),
            safe_txn_gas: "0".to_string(),
            base_gas: "0".to_string(),
            gas_token: format!("{:#x}", Address::ZERO),
            refund_receiver: format!("{:#x}", Address::ZERO),
        },
    };

    let body = serde_json::to_string(&payload)?;
    let headers = make_builder_headers(creds, "POST", "/submit", Some(&body))?;

    let base = cfg.relayer_url.trim_end_matches('/');
    let url = format!("{base}/submit");
    let resp = http.post(url).headers(headers).body(body).send().await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("relayer submit failed ({status}): {body}");
    }

    let value: Value = resp.json().await?;
    if let Some(id) = value
        .get("transactionID")
        .and_then(Value::as_str)
        .or_else(|| value.get("transactionId").and_then(Value::as_str))
        .or_else(|| value.get("id").and_then(Value::as_str))
    {
        return Ok(id.to_string());
    }

    anyhow::bail!("unexpected relayer submit response: {value}")
}

async fn relayer_wait_transaction(
    http: &reqwest::Client,
    cfg: &AutoClaimConfig,
    transaction_id: &str,
) -> anyhow::Result<Option<String>> {
    let base = cfg.relayer_url.trim_end_matches('/');
    let deadline = Instant::now() + cfg.relayer_wait_timeout;
    while Instant::now() < deadline {
        tokio::time::sleep(RELAYER_POLL_DELAY).await;

        let url = format!("{base}/transaction?id={transaction_id}");
        let resp = http.get(url).send().await?;
        if !resp.status().is_success() {
            continue;
        }

        let value: Value = resp.json().await?;
        let tx = if let Some(arr) = value.as_array() {
            arr.first().cloned().unwrap_or(Value::Null)
        } else {
            value.clone()
        };

        if tx.is_null() {
            continue;
        }

        let state = tx
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();

        if state == "FAILED" {
            let msg = tx
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown relayer failure");
            anyhow::bail!("relayer tx failed: {msg} (id={transaction_id})");
        }

        if state == "MINED" || state == "CONFIRMED" {
            let hash = tx
                .get("transactionHash")
                .and_then(Value::as_str)
                .or_else(|| tx.get("txHash").and_then(Value::as_str))
                .map(ToOwned::to_owned);
            return Ok(hash);
        }
    }

    Ok(None)
}

async fn run_eoa_onchain_claims(
    cfg: &AutoClaimConfig,
    private_key: &str,
    candidates: Vec<ClaimableCondition>,
) -> anyhow::Result<usize> {
    let signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey> = private_key.parse()?;
    let signer = signer.with_chain_id(Some(POLYGON));
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect(&cfg.rpc_url)
        .await
        .with_context(|| format!("connect rpc failed: {}", cfg.rpc_url))?;

    let standard = CtfClient::new(provider.clone(), POLYGON)?;
    let neg_risk = CtfClient::with_neg_risk(provider, POLYGON)?;
    let collateral = v2_contract_config(false).collateral;
    let mut claimed = 0_usize;

    for c in candidates {
        if c.negative_risk {
            let yes = decimal_to_u256_6(c.yes_size).unwrap_or(U256::ZERO);
            let no = decimal_to_u256_6(c.no_size).unwrap_or(U256::ZERO);
            if yes.is_zero() && no.is_zero() {
                continue;
            }
            let req = RedeemNegRiskRequest::builder()
                .condition_id(c.condition_id)
                .amounts(vec![yes, no])
                .build();
            match neg_risk.redeem_neg_risk(&req).await {
                Ok(resp) => {
                    claimed += 1;
                    tracing::info!(
                        "✅ AUTO-CLAIM neg-risk success: condition={} tx={} block={}",
                        c.condition_id,
                        resp.transaction_hash,
                        resp.block_number
                    )
                }
                Err(e) => tracing::warn!(
                    "⚠️ AUTO-CLAIM neg-risk failed: condition={} err={:?}",
                    c.condition_id,
                    e
                ),
            }
        } else {
            let req = RedeemPositionsRequest::for_binary_market(collateral, c.condition_id);
            match standard.redeem_positions(&req).await {
                Ok(resp) => {
                    claimed += 1;
                    tracing::info!(
                        "✅ AUTO-CLAIM success: condition={} tx={} block={}",
                        c.condition_id,
                        resp.transaction_hash,
                        resp.block_number
                    )
                }
                Err(e) => tracing::warn!(
                    "⚠️ AUTO-CLAIM failed: condition={} err={:?}",
                    c.condition_id,
                    e
                ),
            }
        }
    }

    Ok(claimed)
}

async fn run_safe_relayer_claims(
    cfg: &AutoClaimConfig,
    signer: Address,
    funder: Address,
    private_key: &str,
    candidates: Vec<ClaimableCondition>,
) -> anyhow::Result<usize> {
    let creds = cfg
        .builder_credentials
        .as_ref()
        .context("POLYMARKET_BUILDER_* credentials are required for SAFE relayer claim")?;

    let standard_cfg = v2_contract_config(false);
    let neg_cfg = v2_contract_config(true);
    let ctf_contract = standard_cfg.conditional_tokens;
    let collateral = standard_cfg.collateral;
    let neg_risk_adapter = neg_cfg
        .neg_risk_adapter
        .context("missing neg-risk adapter contract for polygon")?;

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let signer_wallet: LocalSigner<alloy::signers::k256::ecdsa::SigningKey> =
        private_key
            .parse()
            .context("invalid POLYMARKET_PRIVATE_KEY for safe relayer claim signing")?;

    let deployed = relayer_safe_is_deployed(&http, &cfg.relayer_url, funder).await?;
    if !deployed {
        tracing::warn!(
            "⚠️ Safe relayer claim skipped: safe wallet not deployed yet (safe={:#x})",
            funder
        );
        return Ok(0);
    }

    let mut claimed = 0_usize;
    for c in candidates {
        let (to, data, mode_tag) = if c.negative_risk {
            let yes = decimal_to_u256_6(c.yes_size).unwrap_or(U256::ZERO);
            let no = decimal_to_u256_6(c.no_size).unwrap_or(U256::ZERO);
            if yes.is_zero() && no.is_zero() {
                continue;
            }
            let call = INegRiskAdapterAutoClaim::redeemPositionsCall {
                conditionId: c.condition_id,
                amounts: vec![yes, no],
            };
            (neg_risk_adapter, call.abi_encode(), "neg-risk")
        } else {
            let call = IConditionalTokensAutoClaim::redeemPositionsCall {
                collateralToken: collateral,
                parentCollectionId: B256::ZERO,
                conditionId: c.condition_id,
                indexSets: vec![U256::from(1_u8), U256::from(2_u8)],
            };
            (ctf_contract, call.abi_encode(), "standard")
        };

        let metadata = format!("pm_as_ofi:auto_claim:{}:{}", mode_tag, c.condition_id);
        let tx_id = relayer_submit_safe_claim(
            &http,
            cfg,
            creds,
            &signer_wallet,
            signer,
            funder,
            to,
            data,
            metadata,
        )
        .await
        .with_context(|| format!("relayer submit failed for condition {}", c.condition_id))?;
        claimed += 1;

        if !cfg.relayer_wait_confirm {
            tracing::info!(
                "✅ AUTO-CLAIM SAFE submitted: condition={} tx_id={} (wait_confirm=false)",
                c.condition_id,
                tx_id,
            );
            continue;
        }

        match relayer_wait_transaction(&http, cfg, &tx_id).await {
            Ok(Some(hash)) => tracing::info!(
                "✅ AUTO-CLAIM SAFE confirmed: condition={} tx_id={} tx_hash={}",
                c.condition_id,
                tx_id,
                hash
            ),
            Ok(None) => tracing::warn!(
                "⚠️ AUTO-CLAIM SAFE pending timeout after {}s: condition={} tx_id={}",
                cfg.relayer_wait_timeout.as_secs(),
                c.condition_id,
                tx_id
            ),
            Err(e) => tracing::warn!(
                "⚠️ AUTO-CLAIM SAFE failed: condition={} tx_id={} err={:?}",
                c.condition_id,
                tx_id,
                e
            ),
        }
    }

    Ok(claimed)
}

#[allow(clippy::too_many_arguments)]
pub async fn run_auto_claim_once(
    cfg: &AutoClaimConfig,
    state: &mut AutoClaimState,
    funder_address: Option<&str>,
    signer_address: Option<&str>,
    private_key: Option<&str>,
    preferred_condition: Option<B256>,
    fallback_global: bool,
    bypass_interval: bool,
) -> anyhow::Result<AutoClaimRunResult> {
    let mut result = AutoClaimRunResult::default();
    if !cfg.enabled {
        return Ok(result);
    }
    if !bypass_interval {
        if let Some(last) = state.last_run {
            if last.elapsed() < cfg.run_interval {
                let remain = cfg.run_interval.saturating_sub(last.elapsed()).as_secs();
                tracing::info!(
                    "💸 AUTO-CLAIM skipped: run interval not elapsed (remaining {}s)",
                    remain
                );
                return Ok(result);
            }
        }
        state.last_run = Some(Instant::now());
    }

    let Some(funder_raw) = funder_address else {
        return Ok(result);
    };
    let funder = funder_raw
        .trim()
        .parse::<Address>()
        .with_context(|| format!("invalid funder address: {funder_raw}"))?;

    let signer = match signer_address {
        Some(signer_raw) => signer_raw
            .trim()
            .parse::<Address>()
            .with_context(|| format!("invalid signer address: {signer_raw}"))?,
        None if cfg.dry_run => {
            tracing::info!(
                "💸 AUTO-CLAIM dry-run: signer_address missing; using funder as preview signer"
            );
            funder
        }
        None => return Ok(result),
    };

    let mode = detect_claim_execution_mode(signer, funder, cfg.signature_type);

    if !cfg.dry_run {
        if mode == ClaimExecutionMode::ProxyRelayerUnsupported {
            if !state.warned_proxy_mode {
                state.warned_proxy_mode = true;
                tracing::warn!(
                    "⚠️ PM_AUTO_CLAIM detected Proxy mode (signer={} funder={}). \
                     Current bot only supports EOA or SAFE relayer auto-claim. \
                     Please claim via UI for now.",
                    signer,
                    funder
                );
            }
            return Ok(result);
        }

        if mode == ClaimExecutionMode::UnknownProxyOrSafe {
            if !state.warned_safe_mode {
                state.warned_safe_mode = true;
                tracing::warn!(
                    "⚠️ PM_AUTO_CLAIM could not determine claim mode (signer={} funder={}). \
                     Set PM_SIGNATURE_TYPE=2 for SAFE relayer mode, or claim via UI.",
                    signer,
                    funder
                );
            }
            return Ok(result);
        }

        if mode == ClaimExecutionMode::SafeRelayer {
            if cfg.builder_credentials_partial && !state.warned_builder_creds_partial {
                state.warned_builder_creds_partial = true;
                tracing::warn!(
                    "⚠️ Incomplete POLYMARKET_BUILDER_* env vars. \
                     Set POLYMARKET_BUILDER_API_KEY/SECRET/PASSPHRASE together for SAFE auto-claim."
                );
            }
            if cfg.builder_credentials.is_none() {
                if !state.warned_builder_creds_missing {
                    state.warned_builder_creds_missing = true;
                    tracing::warn!(
                        "⚠️ PM_AUTO_CLAIM SAFE mode needs POLYMARKET_BUILDER_API_KEY / \
                         POLYMARKET_BUILDER_SECRET / POLYMARKET_BUILDER_PASSPHRASE. \
                         Auto-claim execution skipped."
                    );
                }
                return Ok(result);
            }
        }
    }

    let pk = if cfg.dry_run {
        private_key.unwrap_or_default()
    } else {
        let Some(pk) = private_key else {
            tracing::warn!("⚠️ PM_AUTO_CLAIM enabled but POLYMARKET_PRIVATE_KEY is missing");
            return Ok(result);
        };
        pk
    };

    let summary = scan_claimable_positions(&cfg.data_api_url, funder).await?;
    result.positions = summary.positions;
    if summary.positions == 0 {
        tracing::info!("💸 AUTO-CLAIM skipped: no claimable positions");
        return Ok(result);
    }

    let candidates = make_candidates(summary, cfg);
    if candidates.is_empty() {
        tracing::info!(
            "💸 AUTO-CLAIM skipped: no candidates above min_value=${}",
            cfg.min_condition_value
        );
        return Ok(result);
    }

    let mut primary = candidates.clone();
    let mut fallback = Vec::new();
    if let Some(condition_id) = preferred_condition {
        primary.retain(|c| c.condition_id == condition_id);
        if fallback_global {
            fallback = candidates
                .into_iter()
                .filter(|c| c.condition_id != condition_id)
                .collect();
        }
        if primary.is_empty() {
            tracing::info!(
                "💸 AUTO-CLAIM round scope: ended condition {} has no candidates above threshold",
                condition_id
            );
        }
    }

    let execute = |run_candidates: Vec<ClaimableCondition>, scope_label: &'static str| async move {
        if run_candidates.is_empty() {
            return Ok(0_usize);
        }
        tracing::info!(
            "💸 AUTO-CLAIM run: scope={} mode={:?} candidates={} min_value=${}",
            scope_label,
            mode,
            run_candidates.len(),
            cfg.min_condition_value
        );
        if cfg.dry_run {
            for c in &run_candidates {
                tracing::info!(
                    "📝 [AUTO-CLAIM DRY-RUN] scope={} condition={} est_value=${} neg_risk={} yes_size={} no_size={}",
                    scope_label,
                    c.condition_id,
                    c.total_value,
                    c.negative_risk,
                    c.yes_size,
                    c.no_size
                );
            }
            return Ok(0_usize);
        }
        match mode {
            ClaimExecutionMode::EoaOnchain => run_eoa_onchain_claims(cfg, pk, run_candidates).await,
            ClaimExecutionMode::SafeRelayer => {
                run_safe_relayer_claims(cfg, signer, funder, pk, run_candidates).await
            }
            ClaimExecutionMode::ProxyRelayerUnsupported
            | ClaimExecutionMode::UnknownProxyOrSafe => Ok(0_usize),
        }
    };

    if !primary.is_empty() {
        result.candidates += primary.len();
        result.claimed += execute(primary, "primary").await?;
    }
    if result.claimed == 0 && !fallback.is_empty() {
        result.candidates += fallback.len();
        result.claimed += execute(fallback, "fallback_global").await?;
    }

    Ok(result)
}

pub async fn maybe_auto_claim(
    cfg: &AutoClaimConfig,
    state: &mut AutoClaimState,
    funder_address: Option<&str>,
    signer_address: Option<&str>,
    private_key: Option<&str>,
) -> anyhow::Result<()> {
    let _ = run_auto_claim_once(
        cfg,
        state,
        funder_address,
        signer_address,
        private_key,
        None,
        true,
        false,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_claim_run_result_success_live_mode() {
        let outcome = AutoClaimRunResult {
            positions: 3,
            candidates: 2,
            claimed: 1,
        };
        assert!(outcome.succeeded(false));
        assert!(outcome.succeeded(true));
    }

    #[test]
    fn test_auto_claim_run_result_success_dry_run_mode() {
        let outcome = AutoClaimRunResult {
            positions: 3,
            candidates: 2,
            claimed: 0,
        };
        assert!(!outcome.succeeded(false));
        assert!(outcome.succeeded(true));
    }
}
