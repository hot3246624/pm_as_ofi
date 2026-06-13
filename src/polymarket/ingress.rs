//! Shared Ingress module (extracted from the monolithic bin).
//! Handles multi-process / multi-agent sharing of public data plane:
//! - market WS feeds (prefix + fixed round)
//! - Chainlink RTDS
//! - Local price feeds (for oracle lag / boundary)
//!
//! Roles: standalone / broker / client / auto.
//! Uses Unix sockets + manifest + leases + heartbeats for coordination.
//! This reduces WS connection explosion when running many shadows / agents / strategies.

use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

// Re-export or duplicate small helpers that were in bin (env_flag_or is used widely).
// For now we keep a local copy; a broader refactor can hoist it.
fn env_flag_or(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedIngressRole {
    Standalone,
    Broker,
    Client,
    Auto,
}

static SHARED_INGRESS_ROLE_OVERRIDE: OnceLock<SharedIngressRole> = OnceLock::new();

pub fn shared_ingress_role() -> SharedIngressRole {
    if let Some(role) = SHARED_INGRESS_ROLE_OVERRIDE.get() {
        return *role;
    }
    match env::var("PM_SHARED_INGRESS_ROLE")
        .ok()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "broker" => SharedIngressRole::Broker,
        "client" => SharedIngressRole::Client,
        "auto" => SharedIngressRole::Auto,
        _ => SharedIngressRole::Standalone,
    }
}

pub fn set_shared_ingress_role_override(role: SharedIngressRole) {
    let _ = SHARED_INGRESS_ROLE_OVERRIDE.set(role);
}

pub fn shared_ingress_root() -> PathBuf {
    env::var("PM_SHARED_INGRESS_ROOT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("run/shared-ingress-main"))
}

pub fn shared_ingress_chainlink_socket_path() -> PathBuf {
    shared_ingress_root().join("chainlink.sock")
}

pub fn shared_ingress_local_price_socket_path() -> PathBuf {
    shared_ingress_root().join("local_price.sock")
}

pub const SHARED_INGRESS_PROTOCOL_VERSION: u32 = 1;
pub const SHARED_INGRESS_SCHEMA_VERSION: u32 = 1;
pub const SHARED_INGRESS_HEARTBEAT_INTERVAL_MS: u64 = 1_000;
pub const SHARED_INGRESS_BROKER_STALE_MS: u64 = 5_000;
pub const SHARED_INGRESS_CLIENT_STALE_MS: u64 = 5_000;
pub const SHARED_INGRESS_IDLE_SHUTDOWN_GRACE_MS: u64 = 8_000;
pub const SHARED_INGRESS_BROKER_START_TIMEOUT_MS: u64 = 12_000;
pub const SHARED_INGRESS_LOCK_STALE_MS: u64 = 15_000;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SharedIngressBrokerCapabilities {
    #[serde(default = "shared_ingress_market_enabled_default")]
    pub market_enabled: bool,
    #[serde(default)]
    pub chainlink_symbols: Vec<String>,
    #[serde(default)]
    pub local_price_symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedIngressBrokerManifest {
    pub protocol_version: u32,
    #[serde(default = "shared_ingress_schema_version")]
    pub schema_version: u32,
    pub build_id: String,
    #[serde(default = "shared_ingress_default_capabilities")]
    pub capabilities: SharedIngressBrokerCapabilities,
    pub pid: u32,
    pub started_ms: u64,
    pub last_heartbeat_ms: u64,
    pub chainlink_socket: String,
    pub local_price_socket: String,
    pub market_socket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedIngressClientLease {
    pub protocol_version: u32,
    #[serde(default = "shared_ingress_schema_version")]
    pub schema_version: u32,
    pub build_id: String,
    pub instance_id: String,
    pub pid: u32,
    pub started_ms: u64,
    pub last_heartbeat_ms: u64,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

pub fn shared_ingress_protocol_version() -> u32 {
    SHARED_INGRESS_PROTOCOL_VERSION
}

pub fn shared_ingress_schema_version() -> u32 {
    SHARED_INGRESS_SCHEMA_VERSION
}

fn shared_ingress_market_enabled_default() -> bool {
    true
}

fn shared_ingress_default_capabilities() -> SharedIngressBrokerCapabilities {
    SharedIngressBrokerCapabilities {
        market_enabled: true,
        chainlink_symbols: Vec::new(),
        local_price_symbols: Vec::new(),
    }
}

pub fn shared_ingress_build_id() -> String {
    static BUILD_ID: OnceLock<String> = OnceLock::new();
    BUILD_ID
        .get_or_init(|| {
            let exe = env::current_exe().ok();
            let meta = exe.as_ref().and_then(|p| fs::metadata(p).ok());
            let modified_ms = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(0);
            format!("exe:{}:{}", modified_ms, len)
        })
        .clone()
}

pub fn shared_ingress_idle_exit_enabled() -> bool {
    env_flag_or("PM_SHARED_INGRESS_IDLE_EXIT_ENABLED", false)
}

pub fn shared_ingress_broker_replace_existing() -> bool {
    env_flag_or("PM_SHARED_INGRESS_BROKER_REPLACE_EXISTING", false)
}

// Additional small helpers that are pure (more can be moved iteratively)
pub fn shared_ingress_broker_log_path() -> PathBuf {
    shared_ingress_root().join("broker.log")
}

pub fn shared_ingress_broker_runtime_log_root() -> PathBuf {
    shared_ingress_root().join("broker-runtime")
}

pub fn shared_ingress_broker_bias_cache_path() -> PathBuf {
    shared_ingress_root().join("local_price_agg_bias_cache.broker.json")
}

pub fn shared_ingress_broker_manifest_path() -> PathBuf {
    shared_ingress_root().join("broker_manifest.json")
}

pub fn shared_ingress_broker_lock_path() -> PathBuf {
    shared_ingress_root().join("broker.lock")
}

pub fn shared_ingress_clients_dir() -> PathBuf {
    shared_ingress_root().join("clients")
}

pub fn shared_ingress_client_lease_path(instance: &str, pid: u32) -> PathBuf {
    shared_ingress_clients_dir().join(format!("{}-{}.json", instance, pid))
}

pub fn shared_ingress_market_socket_path() -> PathBuf {
    shared_ingress_root().join("market.sock")
}

pub fn sorted_symbol_vec(symbols: &HashSet<String>) -> Vec<String> {
    let mut out: Vec<String> = symbols.iter().cloned().collect();
    out.sort();
    out
}

// Note: more complex async broker spawn / cleanup / manifest reading / lock logic
// will be moved in follow-up waves to keep each step verifiable.
// The goal of this extraction is to shrink the 20k+ line bin and make ingress testable.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingress_versions_are_stable() {
        assert_eq!(shared_ingress_protocol_version(), 1);
        assert_eq!(shared_ingress_schema_version(), 1);
    }
}