//! A lightweight, direct-to-RPC client for Flashbots.
// This avoids dependency conflicts from the archived `ethers-flashbots` crate.

use anyhow::{anyhow, Result};
use alloy_primitives::{Bytes, B256 as H256};
use alloy_signer_local::PrivateKeySigner as LocalWallet;
use alloy::signers::utils;
use serde::Serialize;
use serde_json::json;
use url::Url;

/// The Flashbots relay client.
#[derive(Debug, Clone)]
pub struct FlashbotsClient {
    client: reqwest::Client,
    relay_url: Url,
    signer: LocalWallet,
}

/// Represents a single transaction in a Flashbots bundle.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BundleTransaction {
    /// The raw, signed transaction bytes.
    pub tx: Bytes,
    /// Whether this transaction can revert without the entire bundle failing.
    pub can_revert: bool,
}

/// Represents a Flashbots bundle to be sent.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FlashbotsBundle {
    /// The EIP-2718 `TransactionType` byte. Should be `0x02` for EIP-1559.
    pub txs: Vec<Bytes>,
    pub block_number: H256,
    pub min_timestamp: Option<u64>,
    pub max_timestamp: Option<u64>,
    pub reverting_tx_hashes: Vec<H256>,
}

/// The parameters for an `eth_sendBundle` RPC call.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SendBundleParams {
    /// An array of signed transactions to execute in sequence.
    txs: Vec<Bytes>,
    /// The target block number for the bundle.
    block_number: String,
}

impl FlashbotsClient {
    /// Creates a new Flashbots client.
    pub fn new(relay_url: Url, signer: LocalWallet) -> Self {
        Self {
            client: reqwest::Client::new(),
            relay_url,
            signer,
        }
    }

    /// Signs and sends a bundle to the Flashbots relay.
    pub async fn send_bundle(
        &self,
        signed_txs: &[Bytes],
        target_block: u64,
    ) -> Result<H256> {
        // 1. Prepare the RPC parameters
        let params = SendBundleParams {
            txs: signed_txs.to_vec(),
            block_number: format!("0x{:x}", target_block),
        };
        let params_json = json!([params]);

        // 2. Craft the RPC request payload
        let mut request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_sendBundle",
            "params": params_json,
        });

        // 3. Sign the request body
        let signature = self.sign_request_payload(&request).await?;
        
        // 4. Send the request with the X-Flashbots-Signature header
        let res: serde_json::Value = self.client
            .post(self.relay_url.clone())
            .header("X-Flashbots-Signature", signature)
            .json(&mut request)
            .send()
            .await?
            .json()
            .await?;

        // 5. Parse the response
        if let Some(error) = res.get("error") {
            return Err(anyhow!("Flashbots RPC error: {}", error));
        }

        let bundle_hash: H256 = serde_json::from_value(res["result"]["bundleHash"].clone())?;
        Ok(bundle_hash)
    }

    /// Signs a JSON RPC request payload for Flashbots authentication.
    /// The signature is `keccak256(signer_address + H(request_body))`
    async fn sign_request_payload(&self, request: &serde_json::Value) -> Result<String> {
        let body_bytes = serde_json::to_vec(&request)?;
        let body_hash = ethers::utils::keccak256(&body_bytes);
        
        let signature = self.signer.sign_message(&body_hash).await?;
        
        // The signature format is `signer_address:signature`
        let signature_string = format!(
            "{:?}:{}",
            self.signer.address(),
            signature
        );
        
        Ok(signature_string)
    }
}
