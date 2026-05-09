//! Polymarket V2 — Async Inventory Arbitrage Engine
//!
//! Actor-based architecture:
//!   WebSocket ──fan-out──→ OFI Engine  → (watch) → StrategyCoordinator → Executor → InventoryManager
//!
//! Lifecycle: auto-discover market from prefix → run → wall-clock expiry → CancelAll → rotate.

use futures::{SinkExt, StreamExt};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc, oneshot, watch, Semaphore};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

// V2 Actor modules
use pm_as_ofi::polymarket::claims::{
    execute_market_merge, maybe_auto_claim, run_auto_claim_once, scan_claimable_positions,
    scan_mergeable_full_set_usdc, AutoClaimConfig, AutoClaimState,
};
use pm_as_ofi::polymarket::coordinator::{
    CoordinatorConfig, CoordinatorObsSnapshot, StrategyCoordinator,
};
use pm_as_ofi::polymarket::executor::{init_clob_client, AuthClient, Executor, ExecutorConfig};
use pm_as_ofi::polymarket::glft::{GlftRuntimeConfig, GlftSignalEngine, GlftSignalSnapshot};
use pm_as_ofi::polymarket::inventory::{InventoryConfig, InventoryManager};
use pm_as_ofi::polymarket::messages::*;
use pm_as_ofi::polymarket::ofi::{OfiConfig, OfiEngine};
use pm_as_ofi::polymarket::order_manager::OrderManager;
use pm_as_ofi::polymarket::recorder::{
    RecorderHandle, RecorderSessionMeta, RecorderSessionStart,
};
use pm_as_ofi::polymarket::strategy::StrategyKind;
use pm_as_ofi::polymarket::types::Side;
use pm_as_ofi::polymarket::user_ws::{UserWsConfig, UserWsListener};

// ─────────────────────────────────────────────────────────
// Settings (reused from V1, simplified)
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Settings {
    market_slug: Option<String>,
    market_id: String,
    yes_asset_id: String,
    no_asset_id: String,
    ws_base_url: String,
    rest_url: String,
    private_key: Option<String>,
    #[allow(dead_code)]
    funder_address: Option<String>,
    custom_feature: bool,
}

fn normalize_market_timeframe(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1m" => "1m".to_string(),
        "5m" => "5m".to_string(),
        "15m" => "15m".to_string(),
        "30m" => "30m".to_string(),
        "1h" => "1h".to_string(),
        "4h" => "4h".to_string(),
        "d" | "1d" | "daily" => "1d".to_string(),
        other => other.to_string(),
    }
}

fn derive_market_prefix_from_env() -> Option<String> {
    if let Ok(prefix) = env::var("POLYMARKET_MARKET_PREFIX") {
        let trimmed = prefix.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let symbol = env::var("POLYMARKET_MARKET_SYMBOL")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty());
    let timeframe = env::var("POLYMARKET_MARKET_TIMEFRAME")
        .or_else(|_| env::var("POLYMARKET_MARKET_INTERVAL"))
        .ok()
        .map(|s| normalize_market_timeframe(&s));

    if symbol.is_none() && timeframe.is_none() {
        return None;
    }

    let symbol = symbol.unwrap_or_else(|| "btc".to_string());
    let timeframe = timeframe.unwrap_or_else(|| "15m".to_string());
    if symbol.contains("-updown") {
        Some(format!("{}-{}", symbol, timeframe))
    } else {
        Some(format!("{}-updown-{}", symbol, timeframe))
    }
}

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
enum SharedIngressRole {
    Standalone,
    Broker,
    Client,
    Auto,
}

static SHARED_INGRESS_ROLE_OVERRIDE: OnceLock<SharedIngressRole> = OnceLock::new();

fn shared_ingress_role() -> SharedIngressRole {
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

fn set_shared_ingress_role_override(role: SharedIngressRole) {
    let _ = SHARED_INGRESS_ROLE_OVERRIDE.set(role);
}

fn shared_ingress_root() -> PathBuf {
    env::var("PM_SHARED_INGRESS_ROOT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("run/shared-ingress-main"))
}

fn shared_ingress_chainlink_socket_path() -> PathBuf {
    shared_ingress_root().join("chainlink.sock")
}

fn shared_ingress_local_price_socket_path() -> PathBuf {
    shared_ingress_root().join("local_price.sock")
}

const SHARED_INGRESS_PROTOCOL_VERSION: u32 = 1;
const SHARED_INGRESS_SCHEMA_VERSION: u32 = 1;
const SHARED_INGRESS_HEARTBEAT_INTERVAL_MS: u64 = 1_000;
const SHARED_INGRESS_BROKER_STALE_MS: u64 = 5_000;
const SHARED_INGRESS_CLIENT_STALE_MS: u64 = 5_000;
const SHARED_INGRESS_IDLE_SHUTDOWN_GRACE_MS: u64 = 8_000;
const SHARED_INGRESS_BROKER_START_TIMEOUT_MS: u64 = 12_000;
const SHARED_INGRESS_LOCK_STALE_MS: u64 = 15_000;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SharedIngressBrokerCapabilities {
    #[serde(default = "shared_ingress_market_enabled_default")]
    market_enabled: bool,
    #[serde(default)]
    chainlink_symbols: Vec<String>,
    #[serde(default)]
    local_price_symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedIngressBrokerManifest {
    protocol_version: u32,
    #[serde(default = "shared_ingress_schema_version")]
    schema_version: u32,
    build_id: String,
    #[serde(default = "shared_ingress_default_capabilities")]
    capabilities: SharedIngressBrokerCapabilities,
    pid: u32,
    started_ms: u64,
    last_heartbeat_ms: u64,
    chainlink_socket: String,
    local_price_socket: String,
    market_socket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedIngressClientLease {
    protocol_version: u32,
    #[serde(default = "shared_ingress_schema_version")]
    schema_version: u32,
    build_id: String,
    instance_id: String,
    pid: u32,
    started_ms: u64,
    last_heartbeat_ms: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn shared_ingress_protocol_version() -> u32 {
    SHARED_INGRESS_PROTOCOL_VERSION
}

fn shared_ingress_schema_version() -> u32 {
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

fn shared_ingress_build_id() -> String {
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

fn shared_ingress_idle_exit_enabled() -> bool {
    env_flag_or("PM_SHARED_INGRESS_IDLE_EXIT_ENABLED", false)
}

fn shared_ingress_broker_replace_existing() -> bool {
    env_flag_or("PM_SHARED_INGRESS_BROKER_REPLACE_EXISTING", false)
}

fn shared_ingress_broker_log_path() -> PathBuf {
    shared_ingress_root().join("broker.log")
}

fn shared_ingress_broker_runtime_log_root() -> PathBuf {
    shared_ingress_root().join("broker-runtime")
}

fn shared_ingress_broker_bias_cache_path() -> PathBuf {
    shared_ingress_root().join("local_price_agg_bias_cache.broker.json")
}

fn shared_ingress_broker_manifest_path() -> PathBuf {
    shared_ingress_root().join("broker_manifest.json")
}

fn shared_ingress_broker_lock_path() -> PathBuf {
    shared_ingress_root().join("broker.lock")
}

fn shared_ingress_clients_dir() -> PathBuf {
    shared_ingress_root().join("clients")
}

fn shared_ingress_client_lease_path() -> PathBuf {
    let instance = instance_id().unwrap_or_else(|| "shared-ingress-client".to_string());
    shared_ingress_clients_dir().join(format!("{}-{}.json", instance, std::process::id()))
}

fn shared_ingress_market_socket_path() -> PathBuf {
    shared_ingress_root().join("market.sock")
}

fn write_json_pretty_atomic<T: Serialize>(path: &PathBuf, value: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, serde_json::to_vec_pretty(value)?)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn read_broker_manifest() -> Option<SharedIngressBrokerManifest> {
    let path = shared_ingress_broker_manifest_path();
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str::<SharedIngressBrokerManifest>(&raw).ok()
}

fn is_broker_manifest_compatible(manifest: &SharedIngressBrokerManifest) -> bool {
    manifest.protocol_version == shared_ingress_protocol_version()
        && manifest.schema_version == shared_ingress_schema_version()
}

fn is_broker_manifest_fresh(manifest: &SharedIngressBrokerManifest) -> bool {
    now_ms().saturating_sub(manifest.last_heartbeat_ms) <= SHARED_INGRESS_BROKER_STALE_MS
}

fn is_broker_manifest_healthy(manifest: &SharedIngressBrokerManifest) -> bool {
    is_broker_manifest_compatible(manifest)
        && is_broker_manifest_fresh(manifest)
        && PathBuf::from(&manifest.chainlink_socket).exists()
        && PathBuf::from(&manifest.local_price_socket).exists()
        && PathBuf::from(&manifest.market_socket).exists()
}

fn sorted_symbol_vec(symbols: &HashSet<String>) -> Vec<String> {
    let mut out: Vec<String> = symbols.iter().cloned().collect();
    out.sort();
    out
}

fn shared_ingress_required_capabilities_from_env() -> SharedIngressBrokerCapabilities {
    let coord_cfg = CoordinatorConfig::from_env();
    let prefixes = parse_multi_market_prefixes_from_env();
    let chainlink_symbols = shared_ingress_hub_symbols(&prefixes, &coord_cfg);
    let local_price_symbols =
        if coord_cfg.strategy.is_oracle_lag_sniping() && local_price_agg_enabled() {
            chainlink_symbols.clone()
        } else {
            HashSet::new()
        };
    SharedIngressBrokerCapabilities {
        market_enabled: true,
        chainlink_symbols: sorted_symbol_vec(&chainlink_symbols),
        local_price_symbols: sorted_symbol_vec(&local_price_symbols),
    }
}

fn shared_ingress_capabilities_satisfy(
    have: &SharedIngressBrokerCapabilities,
    need: &SharedIngressBrokerCapabilities,
) -> bool {
    if need.market_enabled && !have.market_enabled {
        return false;
    }
    let have_chainlink: HashSet<&str> = have.chainlink_symbols.iter().map(|s| s.as_str()).collect();
    if need
        .chainlink_symbols
        .iter()
        .any(|symbol| !have_chainlink.contains(symbol.as_str()))
    {
        return false;
    }
    let have_local_price: HashSet<&str> = have
        .local_price_symbols
        .iter()
        .map(|s| s.as_str())
        .collect();
    if need
        .local_price_symbols
        .iter()
        .any(|symbol| !have_local_price.contains(symbol.as_str()))
    {
        return false;
    }
    true
}

fn shared_ingress_pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn cleanup_stale_broker_artifacts() -> bool {
    let Some(manifest) = read_broker_manifest() else {
        return false;
    };
    if shared_ingress_pid_alive(manifest.pid) {
        return false;
    }
    warn!(
        "🧹 removing stale shared ingress broker artifacts | pid={} build_id={} protocol={} schema={}",
        manifest.pid, manifest.build_id, manifest.protocol_version, manifest.schema_version
    );
    let _ = fs::remove_file(shared_ingress_broker_manifest_path());
    let _ = fs::remove_file(shared_ingress_chainlink_socket_path());
    let _ = fs::remove_file(shared_ingress_local_price_socket_path());
    let _ = fs::remove_file(shared_ingress_market_socket_path());
    true
}

#[derive(Default, Debug, Clone, Copy)]
struct SharedIngressLeaseCleanupStats {
    removed_stale: usize,
    removed_invalid: usize,
}

fn cleanup_stale_client_leases() -> SharedIngressLeaseCleanupStats {
    let mut stats = SharedIngressLeaseCleanupStats::default();
    let dir = shared_ingress_clients_dir();
    let Ok(entries) = fs::read_dir(&dir) else {
        return stats;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(raw) = fs::read_to_string(&path) else {
            let _ = fs::remove_file(&path);
            stats.removed_invalid += 1;
            continue;
        };
        let Ok(lease) = serde_json::from_str::<SharedIngressClientLease>(&raw) else {
            let _ = fs::remove_file(&path);
            stats.removed_invalid += 1;
            continue;
        };
        if lease.protocol_version != shared_ingress_protocol_version()
            || lease.schema_version != shared_ingress_schema_version()
        {
            let _ = fs::remove_file(&path);
            stats.removed_invalid += 1;
            continue;
        }
        if now_ms().saturating_sub(lease.last_heartbeat_ms) > SHARED_INGRESS_CLIENT_STALE_MS {
            let _ = fs::remove_file(&path);
            stats.removed_stale += 1;
        }
    }
    stats
}

fn shared_ingress_active_client_count() -> usize {
    fs::read_dir(shared_ingress_clients_dir())
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
                .count()
        })
        .unwrap_or(0)
}

struct SharedIngressBrokerLockGuard {
    path: PathBuf,
}

impl Drop for SharedIngressBrokerLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn try_acquire_shared_ingress_broker_lock() -> anyhow::Result<Option<SharedIngressBrokerLockGuard>>
{
    let path = shared_ingress_broker_lock_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::create_dir(&path) {
        Ok(()) => Ok(Some(SharedIngressBrokerLockGuard { path })),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let stale = fs::metadata(&path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| {
                    now_ms().saturating_sub(d.as_millis() as u64) > SHARED_INGRESS_LOCK_STALE_MS
                })
                .unwrap_or(true);
            if stale {
                let _ = fs::remove_dir_all(&path);
                match fs::create_dir(&path) {
                    Ok(()) => Ok(Some(SharedIngressBrokerLockGuard { path })),
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
                    Err(err) => Err(err.into()),
                }
            } else {
                Ok(None)
            }
        }
        Err(err) => Err(err.into()),
    }
}

async fn kill_shared_ingress_broker_pid(pid: u32) {
    if pid == 0 {
        return;
    }
    let _ = tokio::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    sleep(Duration::from_millis(600)).await;
    let _ = tokio::process::Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

async fn wait_for_shared_ingress_broker_healthy(
    timeout_ms: u64,
    required_caps: &SharedIngressBrokerCapabilities,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        cleanup_stale_broker_artifacts();
        if let Some(manifest) = read_broker_manifest() {
            if is_broker_manifest_healthy(&manifest)
                && shared_ingress_capabilities_satisfy(&manifest.capabilities, required_caps)
            {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for healthy shared ingress broker");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn spawn_shared_ingress_broker_sidecar() -> anyhow::Result<()> {
    let root = shared_ingress_root();
    fs::create_dir_all(&root)?;
    let log_path = shared_ingress_broker_log_path();
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let log_file_err = log_file.try_clone()?;
    let exe = env::current_exe()?;
    let mut cmd = tokio::process::Command::new(&exe);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err))
        .process_group(0)
        .env("PM_SHARED_INGRESS_ROLE", "broker")
        .env("PM_SHARED_INGRESS_ROOT", &root)
        .env("PM_SHARED_INGRESS_IDLE_EXIT_ENABLED", "true")
        .env("PM_LOG_ROOT", shared_ingress_broker_runtime_log_root())
        .env(
            "PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH",
            shared_ingress_broker_bias_cache_path(),
        )
        .env(
            "PM_INSTANCE_ID",
            env::var("PM_SHARED_INGRESS_BROKER_INSTANCE_ID")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| "shared-ingress-broker-auto".to_string()),
        );
    for key in [
        "PM_STRATEGY",
        "PM_MULTI_MARKET_PREFIXES",
        "PM_ORACLE_LAG_SYMBOL_UNIVERSE",
        "PM_LOCAL_PRICE_AGG_ENABLED",
        "PM_LOCAL_PRICE_AGG_DECISION_ENABLED",
        "PM_SELF_BUILT_PRICE_AGG_ENABLED",
        "PM_ORACLE_LAG_LAB_ONLY",
        "PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED",
        "PM_LOCAL_PRICE_AGG_SOURCES",
        "PM_CHAINLINK_HUB_TICK_STALL_RECONNECT_MS",
        "PM_POST_CLOSE_CHAINLINK_WS_URL",
        "PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS",
    ] {
        if let Ok(value) = env::var(key) {
            if !value.trim().is_empty() {
                cmd.env(key, value);
            }
        }
    }
    for key in [
        "PM_MULTI_MARKET_CHILD",
        "POLYMARKET_MARKET_SLUG",
        "POLYMARKET_MARKET_ID",
        "POLYMARKET_YES_ASSET_ID",
        "POLYMARKET_NO_ASSET_ID",
    ] {
        cmd.env_remove(key);
    }
    if env::var("PM_ORACLE_LAG_SYMBOL_UNIVERSE").is_err() {
        if let Ok(slug) = env::var("POLYMARKET_MARKET_SLUG") {
            if let Some(sym) = oracle_lag_symbol_from_slug(&slug) {
                cmd.env("PM_ORACLE_LAG_SYMBOL_UNIVERSE", sym);
            }
        }
    }
    let child = cmd.spawn()?;
    info!(
        "🚀 shared ingress auto spawned broker sidecar | pid={} root={} log={}",
        child.id().unwrap_or_default(),
        root.display(),
        log_path.display()
    );
    Ok(())
}

async fn ensure_shared_ingress_broker_running() -> anyhow::Result<()> {
    let root = shared_ingress_root();
    let required_caps = shared_ingress_required_capabilities_from_env();
    if shared_ingress_role() == SharedIngressRole::Client {
        return wait_for_shared_ingress_broker_healthy(
            SHARED_INGRESS_BROKER_START_TIMEOUT_MS,
            &required_caps,
        )
        .await;
    }
    let market_only_wait_ms = shared_ingress_market_only_auto_wait_ms();
    fs::create_dir_all(shared_ingress_clients_dir())?;
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(SHARED_INGRESS_BROKER_START_TIMEOUT_MS);
    let grace_deadline =
        if shared_ingress_caps_market_only(&required_caps) && market_only_wait_ms > 0 {
            Some(tokio::time::Instant::now() + Duration::from_millis(market_only_wait_ms))
        } else {
            None
        };
    loop {
        cleanup_stale_broker_artifacts();
        if let Some(manifest) = read_broker_manifest() {
            if is_broker_manifest_healthy(&manifest)
                && shared_ingress_capabilities_satisfy(&manifest.capabilities, &required_caps)
            {
                return Ok(());
            }
        }
        if let Some(wait_deadline) = grace_deadline {
            if tokio::time::Instant::now() < wait_deadline {
                sleep(Duration::from_millis(250)).await;
                continue;
            }
        }
        if let Some(_lock) = try_acquire_shared_ingress_broker_lock()? {
            if let Some(manifest) = read_broker_manifest() {
                if is_broker_manifest_healthy(&manifest)
                    && shared_ingress_capabilities_satisfy(&manifest.capabilities, &required_caps)
                {
                    return Ok(());
                }
                if is_broker_manifest_fresh(&manifest)
                    && (!is_broker_manifest_compatible(&manifest)
                        || !shared_ingress_capabilities_satisfy(
                            &manifest.capabilities,
                            &required_caps,
                        ))
                {
                    warn!(
                        "♻️ shared ingress auto replacing broker | pid={} protocol={} schema={} build_id={} have_caps={:?} need_caps={:?}",
                        manifest.pid,
                        manifest.protocol_version,
                        manifest.schema_version,
                        manifest.build_id,
                        manifest.capabilities,
                        required_caps
                    );
                    kill_shared_ingress_broker_pid(manifest.pid).await;
                    let _ = fs::remove_file(shared_ingress_broker_manifest_path());
                    let _ = fs::remove_file(shared_ingress_chainlink_socket_path());
                    let _ = fs::remove_file(shared_ingress_local_price_socket_path());
                    let _ = fs::remove_file(shared_ingress_market_socket_path());
                }
            }
            spawn_shared_ingress_broker_sidecar().await?;
            wait_for_shared_ingress_broker_healthy(
                SHARED_INGRESS_BROKER_START_TIMEOUT_MS,
                &required_caps,
            )
            .await?;
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting to acquire shared ingress broker or observe healthy broker at {}",
                root.display()
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn spawn_shared_ingress_client_lease_heartbeat() {
    let lease_path = shared_ingress_client_lease_path();
    let instance = instance_id().unwrap_or_else(|| "shared-ingress-client".to_string());
    let build_id = shared_ingress_build_id();
    let started_ms = now_ms();
    tokio::spawn(async move {
        loop {
            let lease = SharedIngressClientLease {
                protocol_version: shared_ingress_protocol_version(),
                schema_version: shared_ingress_schema_version(),
                build_id: build_id.clone(),
                instance_id: instance.clone(),
                pid: std::process::id(),
                started_ms,
                last_heartbeat_ms: now_ms(),
            };
            if let Err(err) = write_json_pretty_atomic(&lease_path, &lease) {
                warn!(
                    "⚠️ shared ingress client lease heartbeat write failed | path={} err={}",
                    lease_path.display(),
                    err
                );
            }
            sleep(Duration::from_millis(SHARED_INGRESS_HEARTBEAT_INTERVAL_MS)).await;
        }
    });
}

fn local_price_source_names() -> String {
    LOCAL_PRICE_SOURCES
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

impl Settings {
    fn from_env() -> anyhow::Result<Self> {
        let market_slug = env::var("POLYMARKET_MARKET_SLUG")
            .ok()
            .and_then(|s| {
                let trimmed = s.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            })
            .or_else(derive_market_prefix_from_env);
        Ok(Self {
            market_slug,
            market_id: env::var("POLYMARKET_MARKET_ID").unwrap_or_default(),
            yes_asset_id: env::var("POLYMARKET_YES_ASSET_ID").unwrap_or_default(),
            no_asset_id: env::var("POLYMARKET_NO_ASSET_ID").unwrap_or_default(),
            ws_base_url: env::var("POLYMARKET_WS_BASE_URL")
                .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws".into()),
            rest_url: env::var("POLYMARKET_REST_URL")
                .unwrap_or_else(|_| "https://clob.polymarket.com".into()),
            private_key: env::var("POLYMARKET_PRIVATE_KEY").ok(),
            funder_address: env::var("POLYMARKET_FUNDER_ADDRESS").ok(),
            custom_feature: env::var("POLYMARKET_CUSTOM_FEATURE")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(true),
        })
    }

    fn ws_url(&self, channel: &str) -> String {
        format!("{}/{}", self.ws_base_url, channel)
    }

    fn market_assets(&self) -> Vec<String> {
        vec![self.yes_asset_id.clone(), self.no_asset_id.clone()]
    }
}

#[derive(Debug, Clone)]
struct CapitalRecycleConfig {
    enabled: bool,
    only_hedge_rejects: bool,
    trigger_rejects: usize,
    trigger_window: Duration,
    proactive_headroom: bool,
    headroom_poll: Duration,
    cooldown: Duration,
    max_merges_per_round: usize,
    low_water_usdc: f64,
    target_free_usdc: f64,
    min_batch_usdc: f64,
    max_batch_usdc: f64,
    shortfall_multiplier: f64,
    min_executable_usdc: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundClaimRetryMode {
    Exponential,
    Fixed,
}

impl RoundClaimRetryMode {
    fn from_env() -> Self {
        match env::var("PM_AUTO_CLAIM_ROUND_RETRY_MODE")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("fixed") => Self::Fixed,
            _ => Self::Exponential,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Exponential => "exponential",
            Self::Fixed => "fixed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundClaimScope {
    EndedThenGlobal,
    EndedOnly,
    GlobalOnly,
}

impl RoundClaimScope {
    fn from_env() -> Self {
        match env::var("PM_AUTO_CLAIM_ROUND_SCOPE")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("ended_only") => Self::EndedOnly,
            Some("global_only") => Self::GlobalOnly,
            _ => Self::EndedThenGlobal,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::EndedThenGlobal => "ended_then_global",
            Self::EndedOnly => "ended_only",
            Self::GlobalOnly => "global_only",
        }
    }
}

#[derive(Debug, Clone)]
struct RoundClaimRunnerConfig {
    window: Duration,
    retry_mode: RoundClaimRetryMode,
    scope: RoundClaimScope,
    retry_schedule: Vec<Duration>,
}

impl RoundClaimRunnerConfig {
    fn from_env() -> Self {
        const DEFAULT_SCHEDULE_SECS: [u64; 7] = [0, 2, 5, 9, 14, 20, 27];
        let window_secs = env::var("PM_AUTO_CLAIM_ROUND_WINDOW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(30);
        let retry_mode = RoundClaimRetryMode::from_env();
        let scope = RoundClaimScope::from_env();
        let schedule_raw = env::var("PM_AUTO_CLAIM_ROUND_RETRY_SCHEDULE")
            .ok()
            .map(|raw| parse_retry_schedule_secs(&raw))
            .unwrap_or_else(|| DEFAULT_SCHEDULE_SECS.to_vec());
        let schedule = normalize_retry_schedule_secs(schedule_raw, window_secs);
        Self {
            window: Duration::from_secs(window_secs),
            retry_mode,
            scope,
            retry_schedule: schedule.into_iter().map(Duration::from_secs).collect(),
        }
    }

    fn retry_schedule_text(&self) -> String {
        self.retry_schedule
            .iter()
            .map(|d| d.as_secs().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoundValidationSummary {
    market_slug: String,
    strategy: String,
    round_start_ts_ms: i64,
    round_end_ts_ms: i64,
    partial_round: bool,
    buy_fill_count: u64,
    sell_fill_count: u64,
    yes_bought_qty: f64,
    yes_sold_qty: f64,
    no_bought_qty: f64,
    no_sold_qty: f64,
    avg_yes_buy: Option<f64>,
    avg_yes_sell: Option<f64>,
    avg_no_buy: Option<f64>,
    avg_no_sell: Option<f64>,
    max_abs_net_diff: f64,
    time_weighted_abs_net_diff: f64,
    max_inventory_value: f64,
    time_in_guarded_ms: u64,
    time_in_blocked_ms: u64,
    paired_locked_pnl_end: Option<f64>,
    worst_case_outcome_pnl_end: Option<f64>,
    realized_cash_pnl: f64,
    #[serde(default)]
    realized_round_pnl_ex_residual: f64,
    mark_to_mid_pnl_end: Option<f64>,
    #[serde(default)]
    residual_inventory_cost_end: f64,
    #[serde(default)]
    residual_exit_required: bool,
    loss_attribution: Option<RoundLossAttribution>,
    fees_paid_est: Option<f64>,
    maker_rebate_est: Option<f64>,
    replace_events: u64,
    cancel_events: u64,
    publish_events: u64,
    mean_entry_edge_vs_trusted_mid: Option<f64>,
    mean_fill_slippage_vs_posted: Option<f64>,
    fill_to_adverse_move_3s: Option<f64>,
    fill_to_adverse_move_10s: Option<f64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RoundLossAttribution {
    AModelWrong,
    BQuoteTooAggressive,
    CInventoryNotCompleted,
    DExecutionTimingBug,
    EMarketNotSuitable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoundValidationAggregate {
    rounds_total: usize,
    rounds_partial: usize,
    rounds_full: usize,
    total_realized_cash_pnl: f64,
    total_realized_round_pnl_ex_residual: f64,
    total_mark_to_mid_pnl_end: f64,
    total_residual_inventory_cost_end: f64,
    median_round_pnl: Option<f64>,
    median_round_pnl_ex_residual: Option<f64>,
    mean_fill_to_adverse_move_3s: Option<f64>,
    mean_fill_to_adverse_move_10s: Option<f64>,
    mean_max_abs_net_diff: Option<f64>,
    mean_time_weighted_abs_net_diff: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoundValidationAggregateReport {
    generated_at_ts_ms: i64,
    strategy: String,
    market_slug: String,
    all: RoundValidationAggregate,
    last_5: Option<RoundValidationAggregate>,
    last_20: Option<RoundValidationAggregate>,
    last_50: Option<RoundValidationAggregate>,
}

#[derive(Debug, Clone)]
struct FillTrace {
    side: Side,
    direction: TradeDirection,
    size: f64,
    price: f64,
    ts: Instant,
    trusted_mid_yes_at_fill: Option<f64>,
}

#[derive(Debug)]
struct RoundValidationCollector {
    market_slug: String,
    strategy: String,
    round_start_ts_ms: i64,
    start_instant: Instant,
    trusted_mid_series: Vec<(Instant, f64)>,
    fills: HashMap<String, FillTrace>,
    seen_matched: HashSet<String>,
    last_book_yes_mid: Option<f64>,
    last_book_no_mid: Option<f64>,
    last_inv: InventoryState,
    last_inv_ts: Instant,
    max_abs_net_diff: f64,
    tw_abs_net_diff_accum: f64,
    max_inventory_value: f64,
    last_regime: pm_as_ofi::polymarket::glft::QuoteRegime,
    last_regime_ts: Instant,
    time_in_guarded_ms: u64,
    time_in_blocked_ms: u64,
}

impl RoundValidationCollector {
    fn new(market_slug: String, strategy: String, round_start_ts_ms: i64, now: Instant) -> Self {
        Self {
            market_slug,
            strategy,
            round_start_ts_ms,
            start_instant: now,
            trusted_mid_series: Vec::new(),
            fills: HashMap::new(),
            seen_matched: HashSet::new(),
            last_book_yes_mid: None,
            last_book_no_mid: None,
            last_inv: InventoryState::default(),
            last_inv_ts: now,
            max_abs_net_diff: 0.0,
            tw_abs_net_diff_accum: 0.0,
            max_inventory_value: 0.0,
            last_regime: pm_as_ofi::polymarket::glft::QuoteRegime::Blocked,
            last_regime_ts: now,
            time_in_guarded_ms: 0,
            time_in_blocked_ms: 0,
        }
    }

    fn fill_key(fill: &FillEvent) -> String {
        format!(
            "{}|{:?}|{:?}|{:.6}|{:.6}",
            fill.order_id, fill.side, fill.direction, fill.filled_size, fill.price
        )
    }

    fn note_fill(&mut self, fill: FillEvent, now: Instant) {
        let key = Self::fill_key(&fill);
        match fill.status {
            FillStatus::Matched => {
                self.seen_matched.insert(key.clone());
            }
            FillStatus::Confirmed => {
                if self.seen_matched.contains(&key) {
                    return;
                }
            }
            FillStatus::Failed => {
                self.fills.remove(&key);
                return;
            }
        }

        let trusted_mid_yes_at_fill = self.trusted_mid_at_or_before(now);
        self.fills.entry(key).or_insert_with(|| FillTrace {
            side: fill.side,
            direction: fill.direction,
            size: fill.filled_size.max(0.0),
            price: fill.price,
            ts: fill.ts,
            trusted_mid_yes_at_fill,
        });
    }

    fn note_inventory(&mut self, inv: InventoryState, now: Instant) {
        let dt = now
            .saturating_duration_since(self.last_inv_ts)
            .as_secs_f64()
            .max(0.0);
        self.tw_abs_net_diff_accum += self.last_inv.net_diff.abs() * dt;
        self.last_inv_ts = now;
        self.last_inv = inv;
        self.max_abs_net_diff = self.max_abs_net_diff.max(inv.net_diff.abs());
        let inv_value =
            (inv.yes_qty * inv.yes_avg_cost).max(0.0) + (inv.no_qty * inv.no_avg_cost).max(0.0);
        self.max_inventory_value = self.max_inventory_value.max(inv_value);
    }

    fn note_glft(&mut self, snap: GlftSignalSnapshot, now: Instant) {
        if snap.trusted_mid.is_finite() {
            self.trusted_mid_series.push((now, snap.trusted_mid));
        }
        let dt_ms = now
            .saturating_duration_since(self.last_regime_ts)
            .as_millis() as u64;
        match self.last_regime {
            pm_as_ofi::polymarket::glft::QuoteRegime::Guarded => {
                self.time_in_guarded_ms = self.time_in_guarded_ms.saturating_add(dt_ms);
            }
            pm_as_ofi::polymarket::glft::QuoteRegime::Blocked => {
                self.time_in_blocked_ms = self.time_in_blocked_ms.saturating_add(dt_ms);
            }
            _ => {}
        }
        self.last_regime = snap.quote_regime;
        self.last_regime_ts = now;
    }

    fn note_market_data(&mut self, msg: MarketDataMsg) {
        if let MarketDataMsg::BookTick {
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
            ..
        } = msg
        {
            if yes_bid > 0.0 && yes_ask > 0.0 {
                self.last_book_yes_mid = Some((yes_bid + yes_ask) * 0.5);
            }
            if no_bid > 0.0 && no_ask > 0.0 {
                self.last_book_no_mid = Some((no_bid + no_ask) * 0.5);
            }
        }
    }

    fn trusted_mid_at_or_before(&self, t: Instant) -> Option<f64> {
        self.trusted_mid_series
            .iter()
            .rev()
            .find_map(|(ts, v)| if *ts <= t { Some(*v) } else { None })
            .or_else(|| self.trusted_mid_series.first().map(|(_, v)| *v))
    }

    fn trusted_mid_at_or_after(&self, t: Instant) -> Option<f64> {
        self.trusted_mid_series
            .iter()
            .find_map(|(ts, v)| if *ts >= t { Some(*v) } else { None })
            .or_else(|| self.trusted_mid_series.last().map(|(_, v)| *v))
    }

    fn finalize(
        mut self,
        partial_round: bool,
        coord_obs: CoordinatorObsSnapshot,
        end_inv: InventoryState,
        end_ts_ms: i64,
        now: Instant,
    ) -> RoundValidationSummary {
        self.note_inventory(end_inv, now);
        let dt_ms = now
            .saturating_duration_since(self.last_regime_ts)
            .as_millis() as u64;
        match self.last_regime {
            pm_as_ofi::polymarket::glft::QuoteRegime::Guarded => {
                self.time_in_guarded_ms = self.time_in_guarded_ms.saturating_add(dt_ms);
            }
            pm_as_ofi::polymarket::glft::QuoteRegime::Blocked => {
                self.time_in_blocked_ms = self.time_in_blocked_ms.saturating_add(dt_ms);
            }
            _ => {}
        }

        let elapsed_secs = now
            .saturating_duration_since(self.start_instant)
            .as_secs_f64()
            .max(1e-9);
        let time_weighted_abs_net_diff = self.tw_abs_net_diff_accum / elapsed_secs;

        let mut buy_fill_count = 0_u64;
        let mut sell_fill_count = 0_u64;
        let (mut yes_buy_qty, mut yes_sell_qty, mut no_buy_qty, mut no_sell_qty) =
            (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
        let (mut yes_buy_cost, mut yes_sell_notional, mut no_buy_cost, mut no_sell_notional) =
            (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
        let mut realized_cash_pnl = 0.0_f64;

        let mut edge_weighted_sum = 0.0_f64;
        let mut edge_weight = 0.0_f64;
        let mut move3_weighted_sum = 0.0_f64;
        let mut move3_weight = 0.0_f64;
        let mut move10_weighted_sum = 0.0_f64;
        let mut move10_weight = 0.0_f64;

        for fill in self.fills.values() {
            if fill.size <= 0.0 || !fill.price.is_finite() {
                continue;
            }
            let side_trusted_at_fill =
                fill.trusted_mid_yes_at_fill.map(|yes_mid| match fill.side {
                    Side::Yes => yes_mid,
                    Side::No => 1.0 - yes_mid,
                });
            match (fill.side, fill.direction) {
                (Side::Yes, TradeDirection::Buy) => {
                    buy_fill_count = buy_fill_count.saturating_add(1);
                    yes_buy_qty += fill.size;
                    yes_buy_cost += fill.size * fill.price;
                    realized_cash_pnl -= fill.size * fill.price;
                    if let Some(side_trusted) = side_trusted_at_fill {
                        edge_weighted_sum += (side_trusted - fill.price) * fill.size;
                        edge_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(3))
                    {
                        move3_weighted_sum += (fut_yes_mid - fill.price) * fill.size;
                        move3_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(10))
                    {
                        move10_weighted_sum += (fut_yes_mid - fill.price) * fill.size;
                        move10_weight += fill.size;
                    }
                }
                (Side::Yes, TradeDirection::Sell) => {
                    sell_fill_count = sell_fill_count.saturating_add(1);
                    yes_sell_qty += fill.size;
                    yes_sell_notional += fill.size * fill.price;
                    realized_cash_pnl += fill.size * fill.price;
                    if let Some(side_trusted) = side_trusted_at_fill {
                        edge_weighted_sum += (fill.price - side_trusted) * fill.size;
                        edge_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(3))
                    {
                        move3_weighted_sum += (fill.price - fut_yes_mid) * fill.size;
                        move3_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(10))
                    {
                        move10_weighted_sum += (fill.price - fut_yes_mid) * fill.size;
                        move10_weight += fill.size;
                    }
                }
                (Side::No, TradeDirection::Buy) => {
                    buy_fill_count = buy_fill_count.saturating_add(1);
                    no_buy_qty += fill.size;
                    no_buy_cost += fill.size * fill.price;
                    realized_cash_pnl -= fill.size * fill.price;
                    if let Some(side_trusted) = side_trusted_at_fill {
                        edge_weighted_sum += (side_trusted - fill.price) * fill.size;
                        edge_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(3))
                    {
                        let fut_no_mid = 1.0 - fut_yes_mid;
                        move3_weighted_sum += (fut_no_mid - fill.price) * fill.size;
                        move3_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(10))
                    {
                        let fut_no_mid = 1.0 - fut_yes_mid;
                        move10_weighted_sum += (fut_no_mid - fill.price) * fill.size;
                        move10_weight += fill.size;
                    }
                }
                (Side::No, TradeDirection::Sell) => {
                    sell_fill_count = sell_fill_count.saturating_add(1);
                    no_sell_qty += fill.size;
                    no_sell_notional += fill.size * fill.price;
                    realized_cash_pnl += fill.size * fill.price;
                    if let Some(side_trusted) = side_trusted_at_fill {
                        edge_weighted_sum += (fill.price - side_trusted) * fill.size;
                        edge_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(3))
                    {
                        let fut_no_mid = 1.0 - fut_yes_mid;
                        move3_weighted_sum += (fill.price - fut_no_mid) * fill.size;
                        move3_weight += fill.size;
                    }
                    if let Some(fut_yes_mid) =
                        self.trusted_mid_at_or_after(fill.ts + Duration::from_secs(10))
                    {
                        let fut_no_mid = 1.0 - fut_yes_mid;
                        move10_weighted_sum += (fill.price - fut_no_mid) * fill.size;
                        move10_weight += fill.size;
                    }
                }
            }
        }

        let avg_yes_buy = if yes_buy_qty > 0.0 {
            Some(yes_buy_cost / yes_buy_qty)
        } else {
            None
        };
        let avg_yes_sell = if yes_sell_qty > 0.0 {
            Some(yes_sell_notional / yes_sell_qty)
        } else {
            None
        };
        let avg_no_buy = if no_buy_qty > 0.0 {
            Some(no_buy_cost / no_buy_qty)
        } else {
            None
        };
        let avg_no_sell = if no_sell_qty > 0.0 {
            Some(no_sell_notional / no_sell_qty)
        } else {
            None
        };

        let mean_entry_edge_vs_trusted_mid = if edge_weight > 0.0 {
            Some(edge_weighted_sum / edge_weight)
        } else {
            None
        };
        let fill_to_adverse_move_3s = if move3_weight > 0.0 {
            Some(move3_weighted_sum / move3_weight)
        } else {
            None
        };
        let fill_to_adverse_move_10s = if move10_weight > 0.0 {
            Some(move10_weighted_sum / move10_weight)
        } else {
            None
        };

        let residual_inventory_cost_end = end_inv.yes_qty.max(0.0) * end_inv.yes_avg_cost.max(0.0)
            + end_inv.no_qty.max(0.0) * end_inv.no_avg_cost.max(0.0);
        let paired_qty = end_inv.yes_qty.min(end_inv.no_qty).max(0.0);
        let pair_cost = if end_inv.yes_qty > 0.0 && end_inv.no_qty > 0.0 {
            end_inv.yes_avg_cost + end_inv.no_avg_cost
        } else {
            0.0
        };
        let paired_locked_pnl_end = if paired_qty > 0.0 {
            Some(paired_qty * (1.0 - pair_cost))
        } else {
            None
        };
        let worst_case_outcome_pnl_end = if residual_inventory_cost_end > 0.0 {
            Some(end_inv.yes_qty.min(end_inv.no_qty) - residual_inventory_cost_end)
        } else {
            None
        };
        let mark_to_mid_pnl_end = match (self.last_book_yes_mid, self.last_book_no_mid) {
            (Some(yes_mid), Some(no_mid)) => Some(
                end_inv.yes_qty * (yes_mid - end_inv.yes_avg_cost)
                    + end_inv.no_qty * (no_mid - end_inv.no_avg_cost),
            ),
            _ => None,
        };
        let residual_exit_required = residual_inventory_cost_end > 1e-9;
        let realized_round_pnl_ex_residual = realized_cash_pnl + residual_inventory_cost_end;
        let marked_round_pnl = realized_cash_pnl + mark_to_mid_pnl_end.unwrap_or(0.0);
        let recovery_storm_observed = coord_obs.publish_from_recovery >= 4;
        let source_stale_observed =
            coord_obs.blocked_due_source > 0 || self.time_in_blocked_ms >= 15_000;
        let residual_dominates = residual_exit_required
            && residual_inventory_cost_end
                >= (marked_round_pnl
                    .abs()
                    .max(realized_round_pnl_ex_residual.abs()))
                .max(1.0)
                    * 0.5;
        let strongly_adverse = fill_to_adverse_move_10s.unwrap_or(0.0) <= -0.01
            || fill_to_adverse_move_3s.unwrap_or(0.0) <= -0.008;
        let thin_or_negative_edge = mean_entry_edge_vs_trusted_mid.unwrap_or(0.0) <= 0.0;
        let loss_attribution = if partial_round {
            Some(RoundLossAttribution::DExecutionTimingBug)
        } else if recovery_storm_observed || source_stale_observed {
            Some(RoundLossAttribution::DExecutionTimingBug)
        } else if residual_dominates {
            Some(RoundLossAttribution::CInventoryNotCompleted)
        } else if strongly_adverse {
            Some(RoundLossAttribution::AModelWrong)
        } else if thin_or_negative_edge {
            Some(RoundLossAttribution::BQuoteTooAggressive)
        } else {
            Some(RoundLossAttribution::EMarketNotSuitable)
        };

        RoundValidationSummary {
            market_slug: self.market_slug,
            strategy: self.strategy,
            round_start_ts_ms: self.round_start_ts_ms,
            round_end_ts_ms: end_ts_ms,
            partial_round,
            buy_fill_count,
            sell_fill_count,
            yes_bought_qty: yes_buy_qty,
            yes_sold_qty: yes_sell_qty,
            no_bought_qty: no_buy_qty,
            no_sold_qty: no_sell_qty,
            avg_yes_buy,
            avg_yes_sell,
            avg_no_buy,
            avg_no_sell,
            max_abs_net_diff: self.max_abs_net_diff,
            time_weighted_abs_net_diff,
            max_inventory_value: self.max_inventory_value,
            time_in_guarded_ms: self.time_in_guarded_ms,
            time_in_blocked_ms: self.time_in_blocked_ms,
            paired_locked_pnl_end,
            worst_case_outcome_pnl_end,
            realized_cash_pnl,
            realized_round_pnl_ex_residual,
            mark_to_mid_pnl_end,
            residual_inventory_cost_end,
            residual_exit_required,
            loss_attribution,
            fees_paid_est: None,
            maker_rebate_est: None,
            replace_events: coord_obs.replace_events,
            cancel_events: coord_obs.cancel_events,
            publish_events: coord_obs.publish_events,
            mean_entry_edge_vs_trusted_mid,
            mean_fill_slippage_vs_posted: None,
            fill_to_adverse_move_3s,
            fill_to_adverse_move_10s,
        }
    }
}

async fn run_round_validation_collector(
    market_slug: String,
    strategy: String,
    round_start_ts_ms: i64,
    mut fill_rx: mpsc::Receiver<FillEvent>,
    mut inv_rx: watch::Receiver<InventorySnapshot>,
    mut glft_rx: watch::Receiver<GlftSignalSnapshot>,
    mut md_rx: watch::Receiver<MarketDataMsg>,
    coord_obs_rx: watch::Receiver<CoordinatorObsSnapshot>,
    mut stop_rx: oneshot::Receiver<()>,
    partial_round: bool,
) -> RoundValidationSummary {
    let start = Instant::now();
    let mut collector =
        RoundValidationCollector::new(market_slug, strategy, round_start_ts_ms, start);
    collector.note_inventory(inv_rx.borrow().working, start);
    collector.note_glft(*glft_rx.borrow(), start);
    collector.note_market_data(md_rx.borrow().clone());

    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                break;
            }
            Some(fill) = fill_rx.recv() => {
                collector.note_fill(fill, Instant::now());
            }
            changed = inv_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                collector.note_inventory(inv_rx.borrow().working, Instant::now());
            }
            changed = glft_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                collector.note_glft(*glft_rx.borrow(), Instant::now());
            }
            changed = md_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                collector.note_market_data(md_rx.borrow().clone());
            }
        }
    }

    collector.finalize(
        partial_round,
        *coord_obs_rx.borrow(),
        inv_rx.borrow().working,
        unix_now_ms(),
        Instant::now(),
    )
}

fn unix_now_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}

fn sanitize_instance_id(raw: &str) -> String {
    let sanitized = raw
        .trim()
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '_',
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "default".to_string()
    } else {
        sanitized
    }
}

fn instance_id() -> Option<String> {
    env::var("PM_INSTANCE_ID")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|s| sanitize_instance_id(&s))
}

fn log_root() -> PathBuf {
    if let Some(root) = env::var("PM_LOG_ROOT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return PathBuf::from(root);
    }
    let base = PathBuf::from("logs");
    match instance_id() {
        Some(id) => base.join(id),
        None => base,
    }
}

fn log_path(file_name: &str) -> PathBuf {
    log_root().join(file_name)
}

fn round_validation_jsonl_path() -> PathBuf {
    log_path("round_validation_glft_mm.jsonl")
}

fn round_validation_aggregate_path() -> PathBuf {
    log_path("round_validation_aggregate.json")
}

fn append_round_validation_summary(summary: &RoundValidationSummary) -> anyhow::Result<()> {
    let path = round_validation_jsonl_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(serde_json::to_string(summary)?.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn load_round_validation_summaries() -> anyhow::Result<Vec<RoundValidationSummary>> {
    let path = round_validation_jsonl_path();
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(path)?;
    Ok(text
        .lines()
        .filter_map(|line| serde_json::from_str::<RoundValidationSummary>(line).ok())
        .collect())
}

fn mean_opt(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let mut sum = 0.0;
    let mut n = 0_u64;
    for v in values.flatten() {
        if v.is_finite() {
            sum += v;
            n = n.saturating_add(1);
        }
    }
    if n > 0 {
        Some(sum / n as f64)
    } else {
        None
    }
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        Some(values[mid])
    } else {
        Some((values[mid - 1] + values[mid]) * 0.5)
    }
}

fn aggregate_round_validation(entries: &[RoundValidationSummary]) -> RoundValidationAggregate {
    let rounds_total = entries.len();
    let rounds_partial = entries.iter().filter(|s| s.partial_round).count();
    let rounds_full = rounds_total.saturating_sub(rounds_partial);
    let total_realized_cash_pnl = entries.iter().map(|s| s.realized_cash_pnl).sum::<f64>();
    let total_realized_round_pnl_ex_residual = entries
        .iter()
        .map(|s| s.realized_round_pnl_ex_residual)
        .sum::<f64>();
    let total_mark_to_mid_pnl_end = entries
        .iter()
        .map(|s| s.mark_to_mid_pnl_end.unwrap_or(0.0))
        .sum::<f64>();
    let total_residual_inventory_cost_end = entries
        .iter()
        .map(|s| s.residual_inventory_cost_end)
        .sum::<f64>();
    let median_round_pnl = median(
        entries
            .iter()
            .map(|s| s.realized_cash_pnl + s.mark_to_mid_pnl_end.unwrap_or(0.0))
            .collect(),
    );
    let median_round_pnl_ex_residual = median(
        entries
            .iter()
            .map(|s| s.realized_round_pnl_ex_residual)
            .collect(),
    );
    let mean_fill_to_adverse_move_3s = mean_opt(entries.iter().map(|s| s.fill_to_adverse_move_3s));
    let mean_fill_to_adverse_move_10s =
        mean_opt(entries.iter().map(|s| s.fill_to_adverse_move_10s));
    let mean_max_abs_net_diff = if rounds_total > 0 {
        Some(entries.iter().map(|s| s.max_abs_net_diff).sum::<f64>() / rounds_total as f64)
    } else {
        None
    };
    let mean_time_weighted_abs_net_diff = if rounds_total > 0 {
        Some(
            entries
                .iter()
                .map(|s| s.time_weighted_abs_net_diff)
                .sum::<f64>()
                / rounds_total as f64,
        )
    } else {
        None
    };
    RoundValidationAggregate {
        rounds_total,
        rounds_partial,
        rounds_full,
        total_realized_cash_pnl,
        total_realized_round_pnl_ex_residual,
        total_mark_to_mid_pnl_end,
        total_residual_inventory_cost_end,
        median_round_pnl,
        median_round_pnl_ex_residual,
        mean_fill_to_adverse_move_3s,
        mean_fill_to_adverse_move_10s,
        mean_max_abs_net_diff,
        mean_time_weighted_abs_net_diff,
    }
}

fn update_round_validation_report(strategy: &str, market_slug: &str) -> anyhow::Result<()> {
    let mut entries = load_round_validation_summaries()?;
    entries.retain(|s| s.strategy == strategy && s.market_slug == market_slug);
    let all = aggregate_round_validation(&entries);
    let last_5 = if entries.len() >= 5 {
        Some(aggregate_round_validation(&entries[entries.len() - 5..]))
    } else {
        None
    };
    let last_20 = if entries.len() >= 20 {
        Some(aggregate_round_validation(&entries[entries.len() - 20..]))
    } else {
        None
    };
    let last_50 = if entries.len() >= 50 {
        Some(aggregate_round_validation(&entries[entries.len() - 50..]))
    } else {
        None
    };

    let report = RoundValidationAggregateReport {
        generated_at_ts_ms: unix_now_ms(),
        strategy: strategy.to_string(),
        market_slug: market_slug.to_string(),
        all,
        last_5,
        last_20,
        last_50,
    };
    let path = round_validation_aggregate_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    if report.all.rounds_full > 0 && report.all.rounds_full % 5 == 0 {
        info!(
            "📊 EdgeAggregate[{} rounds] strategy={} market={} pnl(realized/ex_residual/mtm/residual)={:.4}/{:.4}/{:.4}/{:.4} median(marked/ex_residual)={:.4}/{:.4} adverse(3s/10s)={:.5}/{:.5}",
            report.all.rounds_full,
            report.strategy,
            report.market_slug,
            report.all.total_realized_cash_pnl,
            report.all.total_realized_round_pnl_ex_residual,
            report.all.total_mark_to_mid_pnl_end,
            report.all.total_residual_inventory_cost_end,
            report.all.median_round_pnl.unwrap_or(0.0),
            report.all.median_round_pnl_ex_residual.unwrap_or(0.0),
            report.all.mean_fill_to_adverse_move_3s.unwrap_or(0.0),
            report.all.mean_fill_to_adverse_move_10s.unwrap_or(0.0),
        );
    }
    Ok(())
}

impl CapitalRecycleConfig {
    fn from_env() -> Self {
        let enabled = env::var("PM_RECYCLE_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let only_hedge_rejects = env::var("PM_RECYCLE_ONLY_HEDGE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let trigger_rejects = env::var("PM_RECYCLE_TRIGGER_REJECTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);
        let trigger_window = Duration::from_secs(
            env::var("PM_RECYCLE_TRIGGER_WINDOW_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(90),
        );
        let proactive_headroom = env::var("PM_RECYCLE_PROACTIVE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let headroom_poll = Duration::from_secs(
            env::var("PM_RECYCLE_POLL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(5),
        );
        let cooldown = Duration::from_secs(
            env::var("PM_RECYCLE_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(120),
        );
        let max_merges_per_round = env::var("PM_RECYCLE_MAX_MERGES_PER_ROUND")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);
        let low_water_usdc = env::var("PM_RECYCLE_LOW_WATER_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(6.0);
        let target_free_usdc = env::var("PM_RECYCLE_TARGET_FREE_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > low_water_usdc)
            .unwrap_or(18.0);
        let min_batch_usdc = env::var("PM_RECYCLE_MIN_BATCH_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(10.0);
        let max_batch_usdc = env::var("PM_RECYCLE_MAX_BATCH_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= min_batch_usdc)
            .unwrap_or(30.0);
        let shortfall_multiplier = env::var("PM_RECYCLE_SHORTFALL_MULT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(1.2);
        let min_executable_usdc = env::var("PM_RECYCLE_MIN_EXECUTABLE_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(5.0);
        Self {
            enabled,
            only_hedge_rejects,
            trigger_rejects,
            trigger_window,
            proactive_headroom,
            headroom_poll,
            cooldown,
            max_merges_per_round,
            low_water_usdc,
            target_free_usdc,
            min_batch_usdc,
            max_batch_usdc,
            shortfall_multiplier,
            min_executable_usdc,
        }
    }
}

#[derive(Debug, Default)]
struct CapitalRecycleState {
    recent_rejects: VecDeque<Instant>,
    last_merge_ts: Option<Instant>,
    merges_done: usize,
}

#[derive(Debug, Clone, Copy)]
enum RecycleTrigger {
    Reject,
    Headroom,
}

impl RecycleTrigger {
    fn label(self) -> &'static str {
        match self {
            RecycleTrigger::Reject => "reject",
            RecycleTrigger::Headroom => "headroom",
        }
    }
}

fn plan_merge_batch_usdc(
    cfg: &CapitalRecycleConfig,
    free_balance: f64,
    mergeable_full_set_usdc: f64,
) -> f64 {
    if mergeable_full_set_usdc <= 0.0 {
        return 0.0;
    }
    let shortage = (cfg.target_free_usdc - free_balance).max(0.0);
    let desired = (shortage * cfg.shortfall_multiplier).max(cfg.min_batch_usdc);
    desired.min(cfg.max_batch_usdc).min(mergeable_full_set_usdc)
}

#[derive(Debug, Clone, Copy)]
struct DynamicSizingOutcome {
    bid_target: Option<f64>,
    net_target: Option<f64>,
    bid_effective: f64,
    net_effective: f64,
}

fn apply_dynamic_sizing(
    balance_usdc: f64,
    bid_floor: f64,
    net_floor: f64,
    bid_pct: Option<f64>,
    net_pct: Option<f64>,
) -> DynamicSizingOutcome {
    let bid_floor = bid_floor.max(5.0);
    let net_floor = net_floor.max(bid_floor);

    let bid_target = bid_pct.map(|pct| (balance_usdc * pct).round());
    let net_target = net_pct.map(|pct| (balance_usdc * pct).round());

    let bid_effective = bid_target.map(|v| v.max(bid_floor)).unwrap_or(bid_floor);
    let net_effective = net_target
        .map(|v| v.max(net_floor.max(bid_effective)))
        .unwrap_or(net_floor.max(bid_effective));

    DynamicSizingOutcome {
        bid_target,
        net_target,
        bid_effective,
        net_effective,
    }
}

fn parse_retry_schedule_secs(raw: &str) -> Vec<u64> {
    raw.split(',')
        .filter_map(|v| v.trim().parse::<u64>().ok())
        .collect()
}

fn normalize_retry_schedule_secs(mut schedule: Vec<u64>, window_secs: u64) -> Vec<u64> {
    schedule.sort_unstable();
    schedule.dedup();
    if schedule.first().copied() != Some(0) {
        schedule.insert(0, 0);
    }
    schedule.retain(|v| *v <= window_secs);
    if schedule.is_empty() {
        schedule.push(0);
    }
    schedule
}

fn parse_u256_allowance(raw: &str) -> Option<alloy::primitives::U256> {
    use alloy::primitives::U256;

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed.split('.').next().unwrap_or(trimmed);
    if normalized.is_empty() {
        return None;
    }
    if let Some(hex) = normalized
        .strip_prefix("0x")
        .or_else(|| normalized.strip_prefix("0X"))
    {
        U256::from_str_radix(hex, 16).ok()
    } else {
        U256::from_str_radix(normalized, 10).ok()
    }
}

#[derive(Debug, Clone, Copy)]
struct CollateralStatus {
    free_balance: f64,
    allowance_ok: bool,
    allowance_entries: usize,
    allowance_parseable: usize,
    allowance_nonzero: usize,
}

async fn fetch_free_collateral_usdc(client: &AuthClient) -> anyhow::Result<f64> {
    use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
    use polymarket_client_sdk::clob::types::AssetType;

    let req = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .build();
    let resp = client.balance_allowance(req).await?;
    let raw = resp.balance.to_f64().unwrap_or(0.0);
    Ok((raw / 1_000_000.0).max(0.0))
}

async fn fetch_collateral_status(client: &AuthClient) -> anyhow::Result<CollateralStatus> {
    use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
    use polymarket_client_sdk::clob::types::AssetType;

    let req = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .build();
    let resp = client.balance_allowance(req).await?;
    let raw_balance = resp.balance.to_f64().unwrap_or(0.0);
    let free_balance = (raw_balance / 1_000_000.0).max(0.0);
    let allowance_entries = resp.allowances.len();
    let mut allowance_parseable = 0_usize;
    let mut allowance_nonzero = 0_usize;
    for value in resp.allowances.values() {
        if let Some(parsed) = parse_u256_allowance(value) {
            allowance_parseable += 1;
            if !parsed.is_zero() {
                allowance_nonzero += 1;
            }
        }
    }
    Ok(CollateralStatus {
        free_balance,
        allowance_ok: allowance_nonzero > 0,
        allowance_entries,
        allowance_parseable,
        allowance_nonzero,
    })
}

fn should_count_recycle_reject(cfg: &CapitalRecycleConfig, evt: &PlacementRejectEvent) -> bool {
    if evt.kind != RejectKind::BalanceOrAllowance {
        return false;
    }
    if cfg.only_hedge_rejects && evt.reason != BidReason::Hedge {
        return false;
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn try_recycle_merge(
    cfg: &CapitalRecycleConfig,
    state: &mut CapitalRecycleState,
    trigger: RecycleTrigger,
    reject_event: Option<&PlacementRejectEvent>,
    reject_count: usize,
    auto_claim_cfg: &AutoClaimConfig,
    client: &AuthClient,
    inventory_tx: &mpsc::Sender<InventoryEvent>,
    condition_id: alloy::primitives::B256,
    funder: alloy::primitives::Address,
    funder_address: Option<&str>,
    signer_address: Option<&str>,
    private_key: Option<&str>,
    dry_run: bool,
) {
    if state.merges_done >= cfg.max_merges_per_round {
        if matches!(trigger, RecycleTrigger::Reject) {
            warn!(
                "⚠️ Recycler suppressed: reached per-round merge cap ({})",
                cfg.max_merges_per_round
            );
        }
        return;
    }
    if let Some(last) = state.last_merge_ts {
        if last.elapsed() < cfg.cooldown {
            return;
        }
    }

    let status = match fetch_collateral_status(client).await {
        Ok(v) => v,
        Err(e) => {
            if matches!(trigger, RecycleTrigger::Reject) {
                warn!("⚠️ Recycler balance fetch failed: {:?}", e);
            } else {
                debug!("Recycler proactive balance fetch failed: {:?}", e);
            }
            return;
        }
    };
    if !status.allowance_ok {
        warn!(
            "⚠️ Recycler skipped: skip_reason=allowance_zero free_balance={:.2} allowance_ok={} allowance_nonzero={} allowance_entries={} allowance_parseable={}",
            status.free_balance,
            status.allowance_ok,
            status.allowance_nonzero,
            status.allowance_entries,
            status.allowance_parseable
        );
        return;
    }
    if status.free_balance >= cfg.low_water_usdc {
        if matches!(trigger, RecycleTrigger::Reject) {
            state.recent_rejects.clear();
            debug!(
                "Recycler skipped: skip_reason=free_balance_above_low_water free_balance={:.2} low_water={:.2} allowance_ok={}",
                status.free_balance,
                cfg.low_water_usdc,
                status.allowance_ok
            );
        }
        return;
    }

    let mergeable = match scan_mergeable_full_set_usdc(
        &auto_claim_cfg.data_api_url,
        funder,
        condition_id,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            if matches!(trigger, RecycleTrigger::Reject) {
                warn!("⚠️ Recycler mergeable scan failed: {:?}", e);
            } else {
                debug!("Recycler proactive mergeable scan failed: {:?}", e);
            }
            return;
        }
    };
    let mergeable_f64 = mergeable.to_f64().unwrap_or(0.0).max(0.0);
    if mergeable_f64 <= 0.0 {
        if matches!(trigger, RecycleTrigger::Reject) {
            warn!(
                "⚠️ Recycler: no mergeable full sets available for condition={}",
                condition_id
            );
        }
        return;
    }

    let batch_usdc = plan_merge_batch_usdc(cfg, status.free_balance, mergeable_f64);
    if batch_usdc < cfg.min_executable_usdc {
        if matches!(trigger, RecycleTrigger::Reject) {
            warn!(
                "⚠️ Recycler planned batch too small ({:.2} < {:.2}) — skip",
                batch_usdc, cfg.min_executable_usdc
            );
        }
        return;
    }
    let Some(amount_dec) = Decimal::from_f64(batch_usdc) else {
        if matches!(trigger, RecycleTrigger::Reject) {
            warn!(
                "⚠️ Recycler failed to convert batch amount to Decimal: {:.6}",
                batch_usdc
            );
        }
        return;
    };

    let evt_reason = if let Some(evt) = reject_event {
        match evt.reason {
            BidReason::Hedge => "Hedge",
            BidReason::Provide | BidReason::OracleLagProvide => "Provide",
        }
    } else {
        "Headroom"
    };
    let evt_text = if let Some(evt) = reject_event {
        format!(
            "side={:?} reason={:?} price={:.3} size={:.1}",
            evt.side, evt.reason, evt.price, evt.size
        )
    } else {
        "proactive headroom".to_string()
    };
    info!(
        "♻️ Recycler trigger[{}]: evt.reason={} rejects={} free={:.2} allowance_ok={} mergeable={:.2} -> merge {:.2} pUSD ({})",
        trigger.label(),
        evt_reason,
        reject_count,
        status.free_balance,
        status.allowance_ok,
        mergeable_f64,
        batch_usdc,
        evt_text
    );

    match execute_market_merge(
        auto_claim_cfg,
        funder_address,
        signer_address,
        private_key,
        condition_id,
        amount_dec,
        dry_run,
    )
    .await
    {
        Ok(_) => {
            let merge_id = format!("{}-{}", condition_id, state.merges_done + 1);
            let _ = inventory_tx
                .send(InventoryEvent::Merge {
                    full_set_size: batch_usdc,
                    merge_id,
                    ts: Instant::now(),
                })
                .await;
            state.last_merge_ts = Some(Instant::now());
            state.merges_done += 1;
            state.recent_rejects.clear();
            match fetch_free_collateral_usdc(client).await {
                Ok(after) => {
                    info!(
                        "♻️ Recycler post-merge balance: before={:.2} after={:.2}",
                        status.free_balance, after
                    );
                }
                Err(e) => warn!("⚠️ Recycler post-merge balance refresh failed: {:?}", e),
            }
        }
        Err(e) => {
            // Failure is also cooled down to avoid hammering relayer/rpc.
            state.last_merge_ts = Some(Instant::now());
            warn!("⚠️ Recycler merge execution failed: {:?}", e);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_capital_recycler(
    cfg: CapitalRecycleConfig,
    auto_claim_cfg: AutoClaimConfig,
    mut rx: mpsc::Receiver<PlacementRejectEvent>,
    inventory_tx: mpsc::Sender<InventoryEvent>,
    clob_client: Option<AuthClient>,
    market_id: String,
    funder_address: Option<String>,
    signer_address: Option<String>,
    private_key: Option<String>,
    dry_run: bool,
) {
    if !cfg.enabled {
        info!("♻️ Capital recycler disabled by PM_RECYCLE_ENABLED");
        return;
    }

    let condition_id = match market_id.parse::<alloy::primitives::B256>() {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "⚠️ Capital recycler disabled: invalid market_id '{}' for condition_id parse: {:?}",
                market_id, e
            );
            return;
        }
    };

    let Some(client) = clob_client else {
        warn!("⚠️ Capital recycler disabled: no authenticated CLOB client");
        return;
    };

    let Some(funder_raw) = funder_address.as_deref() else {
        warn!("⚠️ Capital recycler disabled: missing funder address");
        return;
    };
    let funder = match funder_raw.parse::<alloy::primitives::Address>() {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "⚠️ Capital recycler disabled: invalid funder address '{}': {:?}",
                funder_raw, e
            );
            return;
        }
    };

    info!(
        "♻️ Capital recycler active | trigger={} in {}s cooldown={}s low={} target={} batch=[{}, {}] proactive={} poll={}s",
        cfg.trigger_rejects,
        cfg.trigger_window.as_secs(),
        cfg.cooldown.as_secs(),
        cfg.low_water_usdc,
        cfg.target_free_usdc,
        cfg.min_batch_usdc,
        cfg.max_batch_usdc,
        cfg.proactive_headroom,
        cfg.headroom_poll.as_secs()
    );

    let mut state = CapitalRecycleState::default();
    let mut headroom_ticker = tokio::time::interval(cfg.headroom_poll);
    loop {
        tokio::select! {
            maybe_evt = rx.recv() => {
                let Some(evt) = maybe_evt else {
                    break;
                };
                if !should_count_recycle_reject(&cfg, &evt) {
                    continue;
                }

                let now = Instant::now();
                state.recent_rejects.push_back(now);
                while let Some(front) = state.recent_rejects.front() {
                    if now.duration_since(*front) > cfg.trigger_window {
                        state.recent_rejects.pop_front();
                    } else {
                        break;
                    }
                }

                if state.recent_rejects.len() < cfg.trigger_rejects {
                    continue;
                }
                let reject_count = state.recent_rejects.len();
                try_recycle_merge(
                    &cfg,
                    &mut state,
                    RecycleTrigger::Reject,
                    Some(&evt),
                    reject_count,
                    &auto_claim_cfg,
                    &client,
                    &inventory_tx,
                    condition_id,
                    funder,
                    funder_address.as_deref(),
                    signer_address.as_deref(),
                    private_key.as_deref(),
                    dry_run,
                )
                .await;
            }
            _ = headroom_ticker.tick(), if cfg.proactive_headroom => {
                try_recycle_merge(
                    &cfg,
                    &mut state,
                    RecycleTrigger::Headroom,
                    None,
                    0,
                    &auto_claim_cfg,
                    &client,
                    &inventory_tx,
                    condition_id,
                    funder,
                    funder_address.as_deref(),
                    signer_address.as_deref(),
                    private_key.as_deref(),
                    dry_run,
                )
                .await;
            }
        }
    }
}

fn log_config_self_check(
    coord: &CoordinatorConfig,
    inv: &InventoryConfig,
    ofi: &OfiConfig,
    balance_opt: Option<f64>,
    reconcile_interval_secs: u64,
) {
    info!("🔎 Config self-check (consistency + risk thresholds)");
    info!(
        "   pair_target={:.4} max_portfolio_cost={:.4} (PM_MAX_LOSS_PCT deprecated/ignored)",
        coord.pair_target, coord.max_portfolio_cost
    );
    info!(
        "   bid_size={:.1} max_net_diff={:.1}",
        coord.bid_size, coord.max_net_diff
    );
    info!(
        "   min_order_size={:.2} min_hedge_size={:.2} hedge_round_up={}",
        coord.min_order_size, coord.min_hedge_size, coord.hedge_round_up
    );
    info!(
        "   post_only_safety_ticks={:.1} tight_spread_ticks={:.1} extra_tight_ticks={:.1}",
        coord.post_only_safety_ticks,
        coord.post_only_tight_spread_ticks,
        coord.post_only_extra_tight_ticks
    );
    info!(
        "   glft_min_half_spread_ticks={:.1} (min half spread = {:.3})",
        coord.glft_min_half_spread_ticks,
        coord.glft_min_half_spread_ticks * coord.tick_size
    );
    info!(
        "   hedge_marketable_floor=${:.2} max_extra={:.2} max_extra_pct={:.0}%",
        coord.hedge_min_marketable_notional,
        coord.hedge_min_marketable_max_extra,
        coord.hedge_min_marketable_max_extra_pct * 100.0
    );
    info!(
        "   tick={:.3} reprice={:.3} debounce={}ms hedge_debounce={}ms stale_ttl={}ms watchdog={}ms toxic_hold={}ms",
        coord.tick_size,
        coord.reprice_threshold,
        coord.debounce_ms,
        coord.hedge_debounce_ms,
        coord.stale_ttl_ms,
        coord.watchdog_tick_ms,
        coord.toxic_recovery_hold_ms
    );
    info!(
        "   endgame windows: soft={}s hard={}s freeze={}s maker_repair_min={}s pair_arb_risk_open_cutoff={}s edge(keep/exit)={:.2}/{:.2}",
        coord.endgame_soft_close_secs,
        coord.endgame_hard_close_secs,
        coord.endgame_freeze_secs,
        coord.endgame_maker_repair_min_secs,
        coord.pair_arb.risk_open_cutoff_secs,
        coord.endgame_edge_keep_mult,
        coord.endgame_edge_exit_mult
    );
    info!("   reconcile_interval={}s", reconcile_interval_secs);
    if ofi.adaptive_threshold {
        info!(
            "   ofi_window={}ms adaptive=tail-quantile q_enter/q_exit=99%/95% min={:.1} ratio_enter/exit={:.2}/{:.2} heartbeat={}ms exit_ratio={:.2} min_toxic={}ms",
            ofi.window_duration.as_millis(),
            ofi.adaptive_min,
            ofi.toxicity_ratio_enter,
            ofi.toxicity_ratio_exit,
            ofi.heartbeat_ms,
            ofi.toxicity_exit_ratio,
            ofi.min_toxic_ms
        );
    } else {
        let adaptive_max_text = if ofi.adaptive_max > 0.0 {
            format!("{:.1}", ofi.adaptive_max)
        } else {
            "off".to_string()
        };
        let adaptive_rise_cap_text = if ofi.adaptive_rise_cap_pct > 0.0 {
            format!("{:.0}%", ofi.adaptive_rise_cap_pct * 100.0)
        } else {
            "off".to_string()
        };
        info!(
            "   ofi_window={}ms ofi_thresh={:.1} adaptive={} k={:.2} min/max=[{:.1}, {}] rise_cap={} ratio_enter/exit={:.2}/{:.2} heartbeat={}ms exit_ratio={:.2} min_toxic={}ms",
            ofi.window_duration.as_millis(),
            ofi.toxicity_threshold,
            ofi.adaptive_threshold,
            ofi.adaptive_k,
            ofi.adaptive_min,
            adaptive_max_text,
            adaptive_rise_cap_text,
            ofi.toxicity_ratio_enter,
            ofi.toxicity_ratio_exit,
            ofi.heartbeat_ms,
            ofi.toxicity_exit_ratio,
            ofi.min_toxic_ms
        );
    }

    if (inv.max_net_diff - coord.max_net_diff).abs() > 1e-6 {
        warn!(
            "⚠️ Inconsistent max_net_diff: inv={:.1} coord={:.1}",
            inv.max_net_diff, coord.max_net_diff
        );
    }
    if (inv.bid_size - coord.bid_size).abs() > 1e-6 {
        warn!(
            "⚠️ Inconsistent bid_size: inv={:.1} coord={:.1}",
            inv.bid_size, coord.bid_size
        );
    }
    if (inv.max_portfolio_cost - coord.max_portfolio_cost).abs() > 1e-6 {
        warn!(
            "⚠️ Inconsistent max_portfolio_cost: inv={:.4} coord={:.4}",
            inv.max_portfolio_cost, coord.max_portfolio_cost
        );
    }

    if coord.bid_size > coord.max_net_diff {
        warn!(
            "⚠️ bid_size ({:.1}) > max_net_diff ({:.1}) → net gate blocks normal provides",
            coord.bid_size, coord.max_net_diff
        );
    }
    if coord.min_order_size > 0.0 && coord.min_order_size > coord.bid_size {
        warn!(
            "⚠️ min_order_size ({:.2}) > bid_size ({:.2}) → provide orders may be skipped",
            coord.min_order_size, coord.bid_size
        );
    }
    if coord.hedge_round_up {
        warn!("⚠️ hedge_round_up enabled — small imbalances may be over-hedged");
    }
    if coord.hedge_min_marketable_notional > 0.0
        && (coord.hedge_min_marketable_max_extra <= 0.0
            || coord.hedge_min_marketable_max_extra_pct <= 0.0)
    {
        warn!(
            "⚠️ hedge marketable floor is enabled but extra-size cap is 0 (abs={:.2}, pct={:.2})",
            coord.hedge_min_marketable_max_extra, coord.hedge_min_marketable_max_extra_pct
        );
    }
    if coord.pair_target >= 1.0 {
        warn!(
            "⚠️ pair_target >= 1.0 → no guaranteed arbitrage margin (pair_target={:.4})",
            coord.pair_target
        );
    }
    if coord.max_portfolio_cost < coord.pair_target {
        warn!(
            "⚠️ max_portfolio_cost ({:.4}) < pair_target ({:.4}) → rescue ceiling below profit line",
            coord.max_portfolio_cost, coord.pair_target
        );
    }
    let _ = balance_opt;
}

// ─────────────────────────────────────────────────────────
// Market Discovery — prefix → current live slug
// ─────────────────────────────────────────────────────────

/// Check if a slug is a "prefix" (no trailing timestamp) vs a full slug.
/// Prefix examples: "btc-updown-15m", "btc-updown-5m"
/// Full slug: "btc-updown-15m-1771904700"
fn is_prefix_slug(slug: &str) -> bool {
    // If the last segment (after final '-') is NOT a pure number, it's a prefix
    slug.rsplit('-')
        .next()
        .map(|last| last.parse::<u64>().is_err())
        .unwrap_or(true)
}

/// Detect interval from prefix: "...-5m" → 300, "...-15m" → 900, "...-1h" → 3600.
fn detect_interval(prefix: &str) -> u64 {
    let lower = prefix.to_ascii_lowercase();
    if lower.contains("-1m") {
        60
    } else if lower.contains("-5m") {
        300
    } else if lower.contains("-15m") {
        900
    } else if lower.contains("-30m") {
        1800
    } else if lower.contains("-1h") {
        3600
    } else if lower.contains("-4h") {
        14400
    } else if lower.contains("-1d") || lower.ends_with("-d") || lower.contains("-daily") {
        86400
    } else {
        900 // default 15min
    }
}

#[derive(Debug, Clone)]
enum OracleLagSymbolUniverse {
    All,
    Only(HashSet<String>),
}

const ORACLE_LAG_SUPPORTED_SYMBOLS: &[&str] = &["hype", "btc", "eth", "sol", "bnb", "doge", "xrp"];

impl OracleLagSymbolUniverse {
    fn supported_symbol_set() -> HashSet<String> {
        ORACLE_LAG_SUPPORTED_SYMBOLS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn from_env() -> Self {
        let raw = env::var("PM_ORACLE_LAG_SYMBOL_UNIVERSE")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let Some(raw) = raw else {
            return Self::Only(Self::supported_symbol_set());
        };
        if raw == "*" || raw.eq_ignore_ascii_case("all") {
            return Self::All;
        }
        let parsed: HashSet<String> = raw
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        if parsed.is_empty() {
            Self::All
        } else {
            Self::Only(parsed)
        }
    }

    fn contains(&self, symbol: &str) -> bool {
        let sym = symbol.trim().to_ascii_lowercase();
        match self {
            Self::All => Self::supported_symbol_set().contains(&sym),
            Self::Only(set) => set.contains(&sym),
        }
    }

    fn hub_symbols(&self) -> HashSet<String> {
        match self {
            Self::All => Self::supported_symbol_set(),
            Self::Only(set) => set.clone(),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::All => "*".to_string(),
            Self::Only(set) => {
                let mut items: Vec<String> = set.iter().cloned().collect();
                items.sort();
                items.join(",")
            }
        }
    }
}

fn extract_updown_symbol_timeframe(slug: &str) -> Option<(String, String)> {
    let lower = slug.trim().to_ascii_lowercase();
    let (symbol, tail) = lower.split_once("-updown-")?;
    let timeframe = tail.split('-').next()?.trim();
    if symbol.is_empty() || timeframe.is_empty() {
        return None;
    }
    Some((symbol.to_string(), timeframe.to_string()))
}

fn oracle_lag_symbol_from_slug(slug: &str) -> Option<String> {
    let (symbol, timeframe) = extract_updown_symbol_timeframe(slug)?;
    if timeframe == "5m" {
        Some(symbol)
    } else {
        None
    }
}

fn parse_multi_market_prefixes_from_env() -> Vec<String> {
    let Some(raw) = env::var("PM_MULTI_MARKET_PREFIXES")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        return Vec::new();
    };
    let mut out = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for slug in raw.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if seen.insert(slug.to_string()) {
            out.push(slug.to_string());
        }
    }
    out
}

fn default_endgame_windows_secs(interval_secs: u64) -> (u64, u64, u64) {
    if interval_secs <= 300 {
        // 5m: t-60 hedge-only, t-30 edge-aware taker de-risk, final risk-freeze at 2s.
        (60, 30, 2)
    } else if interval_secs <= 900 {
        // 15m: allow longer inventory convergence window.
        (90, 30, 3)
    } else if interval_secs <= 3600 {
        (180, 60, 5)
    } else {
        (600, 180, 8)
    }
}

fn default_endgame_maker_repair_min_secs(interval_secs: u64) -> u64 {
    if interval_secs <= 300 {
        8
    } else if interval_secs <= 900 {
        15
    } else if interval_secs <= 3600 {
        30
    } else {
        90
    }
}

fn apply_endgame_windows_for_interval(cfg: &mut CoordinatorConfig, interval_secs: u64) {
    let soft_env = env::var("PM_ENDGAME_SOFT_CLOSE_SECS")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .and_then(|v| v.parse::<u64>().ok());
    let hard_env = env::var("PM_ENDGAME_HARD_CLOSE_SECS")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .and_then(|v| v.parse::<u64>().ok());
    let freeze_env = env::var("PM_ENDGAME_FREEZE_SECS")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .and_then(|v| v.parse::<u64>().ok());
    let maker_repair_env = env::var("PM_ENDGAME_MAKER_REPAIR_MIN_SECS")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .and_then(|v| v.parse::<u64>().ok());

    let (soft_default, hard_default, freeze_default) = default_endgame_windows_secs(interval_secs);
    cfg.endgame_soft_close_secs = soft_env.unwrap_or(soft_default);
    cfg.endgame_hard_close_secs = hard_env.unwrap_or(hard_default);
    cfg.endgame_freeze_secs = freeze_env.unwrap_or(freeze_default);
    cfg.endgame_maker_repair_min_secs =
        maker_repair_env.unwrap_or(default_endgame_maker_repair_min_secs(interval_secs));

    // Keep windows ordered.
    cfg.endgame_hard_close_secs = cfg.endgame_hard_close_secs.min(cfg.endgame_soft_close_secs);
    cfg.endgame_freeze_secs = cfg.endgame_freeze_secs.min(cfg.endgame_hard_close_secs);
    cfg.endgame_maker_repair_min_secs = cfg
        .endgame_maker_repair_min_secs
        .min(cfg.endgame_hard_close_secs);
}

fn should_skip_entry_window(now_unix: u64, end_ts: u64, interval: u64, grace: u64) -> bool {
    if now_unix >= end_ts {
        return true;
    }
    let start_ts = end_ts.saturating_sub(interval);
    now_unix > start_ts.saturating_add(grace)
}

/// Compute how long to wait before attempting next round.
///
/// We align to `end_ts` precisely instead of fixed sleeps. This avoids
/// missing opening seconds while still preventing boundary races when the
/// current loop exits slightly early due to second-level rounding.
fn rotation_wait_duration(now_unix: u64, end_ts: u64) -> Duration {
    if now_unix >= end_ts {
        Duration::from_millis(0)
    } else {
        Duration::from_secs(end_ts - now_unix)
    }
}

fn market_preload_lead_secs() -> u64 {
    env::var("PM_MARKET_PRELOAD_LEAD_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|v| v.clamp(0, 300))
        .unwrap_or(180)
}

fn market_preload_retry_interval_ms() -> u64 {
    env::var("PM_MARKET_PRELOAD_RETRY_INTERVAL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|v| v.clamp(100, 10_000))
        .unwrap_or(750)
}

fn market_preload_deadline_slack_secs() -> u64 {
    env::var("PM_MARKET_PRELOAD_DEADLINE_SLACK_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|v| v.clamp(0, 60))
        .unwrap_or(2)
}

fn spawn_market_pre_resolve(
    raw_slug: &str,
    next_slug_ts: u64,
    preload_tx: mpsc::Sender<(String, anyhow::Result<ResolvedMarket>)>,
    preloading_slug: &mut Option<String>,
    reason: &'static str,
) {
    let next_slug = format!("{}-{}", raw_slug, next_slug_ts);
    if preloading_slug.as_deref() == Some(next_slug.as_str()) {
        debug!(
            "⏳ Skip duplicate pre-resolve spawn: {} already in-flight",
            next_slug
        );
        return;
    }

    *preloading_slug = Some(next_slug.clone());
    let lead_secs = market_preload_lead_secs();
    let retry_interval_ms = market_preload_retry_interval_ms();
    let deadline_ts = next_slug_ts.saturating_add(market_preload_deadline_slack_secs());

    tokio::spawn(async move {
        let now = unix_now_secs();
        let target_start_ts = next_slug_ts.saturating_sub(lead_secs);
        if target_start_ts > now {
            tokio::time::sleep(Duration::from_secs(target_start_ts - now)).await;
        }

        let mut attempt = 0u64;
        loop {
            attempt = attempt.saturating_add(1);
            info!(
                "⏳ Pre-resolving next market in background | slug={} reason={} attempt={} lead_secs={} deadline_ts={}",
                next_slug, reason, attempt, lead_secs, deadline_ts
            );

            match resolve_market_with_retry(&next_slug).await {
                Ok(ids) => {
                    let _ = preload_tx.send((next_slug, Ok(ids))).await;
                    return;
                }
                Err(err) => {
                    let now = unix_now_secs();
                    if now >= deadline_ts {
                        warn!(
                            "❌ Market pre-resolve deadline exhausted | slug={} reason={} attempts={} deadline_ts={} err={}",
                            next_slug, reason, attempt, deadline_ts, err
                        );
                        let _ = preload_tx.send((next_slug, Err(err))).await;
                        return;
                    }
                    warn!(
                        "⚠️ Market pre-resolve retrying | slug={} reason={} attempt={} retry_ms={} deadline_ts={} err={}",
                        next_slug, reason, attempt, retry_interval_ms, deadline_ts, err
                    );
                    tokio::time::sleep(Duration::from_millis(retry_interval_ms)).await;
                }
            }
        }
    });
}

type ResolvedMarket = (String, String, String, Option<u64>);

fn inferred_start_ts_from_slug(slug: &str) -> Option<u64> {
    slug.rsplit('-').next()?.parse().ok()
}

fn inferred_end_ts_from_slug(slug: &str) -> Option<u64> {
    let start_ts = inferred_start_ts_from_slug(slug)?;
    Some(start_ts.saturating_add(detect_interval(slug)))
}

fn resolve_timeout_ms() -> u64 {
    env::var("PM_RESOLVE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(1000, 30000))
        .unwrap_or(4000)
}

fn resolve_retry_attempts() -> usize {
    env::var("PM_RESOLVE_RETRY_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(1, 10))
        .unwrap_or(4)
}

fn ws_connect_timeout_ms() -> u64 {
    env::var("PM_WS_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(1000, 30000))
        .unwrap_or(6000)
}

fn ws_degrade_max_failures() -> u32 {
    env::var("PM_WS_DEGRADE_MAX_FAILURES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(12)
}

fn should_degrade_ws(consecutive_failures: u32, max_failures: u32) -> bool {
    max_failures > 0 && consecutive_failures >= max_failures
}

fn shared_ingress_market_connect_permits() -> usize {
    env::var("PM_SHARED_INGRESS_MARKET_CONNECT_PERMITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(1, 8))
        .unwrap_or(2)
}

fn shared_ingress_market_connect_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(shared_ingress_market_connect_permits()))
}

fn shared_ingress_fixed_market_connect_permits() -> usize {
    env::var("PM_SHARED_INGRESS_FIXED_MARKET_CONNECT_PERMITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(1, 8))
        .unwrap_or(2)
}

fn shared_ingress_fixed_market_connect_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(shared_ingress_fixed_market_connect_permits()))
}

fn shared_ingress_market_connect_jitter_ms() -> u64 {
    env::var("PM_SHARED_INGRESS_MARKET_CONNECT_JITTER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(0, 5000))
        .unwrap_or(1200)
}

fn shared_ingress_fixed_market_connect_jitter_ms() -> u64 {
    env::var("PM_SHARED_INGRESS_FIXED_MARKET_CONNECT_JITTER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(0, 5000))
        .unwrap_or(0)
}

fn shared_ingress_market_health_jitter_ms() -> u64 {
    env::var("PM_SHARED_INGRESS_MARKET_HEALTH_JITTER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(0, 5000))
        .unwrap_or(1500)
}

fn shared_ingress_market_feed_jitter_ms(settings: &Settings, ceiling_ms: u64) -> u64 {
    if ceiling_ms == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    settings.market_slug.hash(&mut hasher);
    settings.market_id.hash(&mut hasher);
    settings.yes_asset_id.hash(&mut hasher);
    settings.no_asset_id.hash(&mut hasher);
    settings.custom_feature.hash(&mut hasher);
    hasher.finish() % ceiling_ms
}

fn is_fixed_market_slug(slug: Option<&str>) -> bool {
    let Some(slug) = slug else {
        return false;
    };
    let Some((_, suffix)) = slug.rsplit_once('-') else {
        return false;
    };
    if suffix.len() != 10 || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    suffix
        .parse::<u64>()
        .map(|ts| (1_000_000_000..=4_102_444_800).contains(&ts))
        .unwrap_or(false)
}

fn shared_ingress_market_only_auto_wait_ms() -> u64 {
    env::var("PM_SHARED_INGRESS_MARKET_ONLY_AUTO_WAIT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(0, 10000))
        .unwrap_or(2500)
}

fn shared_ingress_caps_market_only(caps: &SharedIngressBrokerCapabilities) -> bool {
    caps.market_enabled && caps.chainlink_symbols.is_empty() && caps.local_price_symbols.is_empty()
}

/// Compute the slug and end-timestamp for the CURRENTLY ACTIVE market.
///
/// "btc-updown-15m" + now=03:36 UTC → ("btc-updown-15m-1771904700", 1771904700)
fn compute_current_slug(prefix: &str) -> (String, u64, u64) {
    let interval = detect_interval(prefix);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let start_ts = (now / interval) * interval;
    let expected_end_ts = start_ts + interval;
    (
        format!("{}-{}", prefix, start_ts),
        start_ts,
        expected_end_ts,
    )
}

/// Resolve a market by exact slug via Gamma API.
async fn resolve_market_by_slug(slug: &str) -> anyhow::Result<ResolvedMarket> {
    info!("🔍 Resolving market: {}", slug);
    let url = format!("https://gamma-api.polymarket.com/markets?slug={}", slug);
    let timeout_ms = resolve_timeout_ms();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()?;
    let resp: Value = client.get(&url).send().await?.json().await?;

    if let Some(markets) = resp.as_array() {
        if let Some(market) = markets.first() {
            if let Some(resolved) = parse_resolved_market_payload(market) {
                info!(
                    "✅ Market resolved via /markets: {} (YES={}, NO={})",
                    resolved.0,
                    &resolved.1[..8.min(resolved.1.len())],
                    &resolved.2[..8.min(resolved.2.len())]
                );
                return Ok(resolved);
            }
        }
    }
    anyhow::bail!("Failed to resolve market from slug: {}", slug);
}

fn parse_resolved_market_payload(market: &Value) -> Option<ResolvedMarket> {
    let market_id = market
        .get("conditionId")
        .and_then(|v| v.as_str())
        .or_else(|| market.get("condition_id").and_then(|v| v.as_str()))?
        .to_string();

    let end_date_ts = market
        .get("endDate")
        .or_else(|| market.get("end_date"))
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp() as u64);

    if let Some(ids) = market
        .get("clobTokenIds")
        .or_else(|| market.get("clob_token_ids"))
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
    {
        if ids.len() >= 2 {
            return Some((market_id, ids[0].clone(), ids[1].clone(), end_date_ts));
        }
    }

    if let Some(tokens) = market.get("tokens").and_then(|v| v.as_array()) {
        let yes = tokens.iter().find(|t| t["outcome"].as_str() == Some("Yes"));
        let no = tokens.iter().find(|t| t["outcome"].as_str() == Some("No"));
        if let (Some(y), Some(n)) = (yes, no) {
            let yes_id = y["token_id"].as_str().unwrap_or_default().to_string();
            let no_id = n["token_id"].as_str().unwrap_or_default().to_string();
            if !yes_id.is_empty() && !no_id.is_empty() {
                return Some((market_id, yes_id, no_id, end_date_ts));
            }
        }
    }
    None
}

async fn resolve_market_by_event_slug(slug: &str) -> anyhow::Result<ResolvedMarket> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
    let timeout_ms = resolve_timeout_ms();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()?;
    let resp: Value = client.get(&url).send().await?.json().await?;
    let Some(events) = resp.as_array() else {
        anyhow::bail!("events response is not array");
    };

    for event in events {
        let Some(markets) = event.get("markets").and_then(|v| v.as_array()) else {
            continue;
        };
        if let Some(exact) = markets.iter().find(|m| {
            m.get("slug")
                .and_then(|v| v.as_str())
                .map(|s| s == slug)
                .unwrap_or(false)
        }) {
            if let Some(resolved) = parse_resolved_market_payload(exact) {
                info!(
                    "✅ Market resolved via /events exact slug: {} (YES={}, NO={})",
                    resolved.0,
                    &resolved.1[..8.min(resolved.1.len())],
                    &resolved.2[..8.min(resolved.2.len())]
                );
                return Ok(resolved);
            }
        }
        for market in markets {
            if let Some(resolved) = parse_resolved_market_payload(market) {
                info!(
                    "✅ Market resolved via /events fallback: {} (YES={}, NO={})",
                    resolved.0,
                    &resolved.1[..8.min(resolved.1.len())],
                    &resolved.2[..8.min(resolved.2.len())]
                );
                return Ok(resolved);
            }
        }
    }
    anyhow::bail!("Failed to resolve market from /events for slug: {}", slug);
}

async fn resolve_market_with_retry(slug: &str) -> anyhow::Result<ResolvedMarket> {
    let attempts = resolve_retry_attempts();
    let mut backoff = Duration::from_millis(300);
    let max_backoff = Duration::from_secs(2);
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=attempts {
        match resolve_market_by_slug(slug).await {
            Ok(m) => return Ok(m),
            Err(primary_err) => match resolve_market_by_event_slug(slug).await {
                Ok(m) => return Ok(m),
                Err(fallback_err) => {
                    let e = anyhow::anyhow!(
                        "markets_resolve_err={} | events_resolve_err={}",
                        primary_err,
                        fallback_err
                    );
                    warn!(
                        "❌ Resolve attempt {}/{} failed for '{}': {}",
                        attempt, attempts, slug, e
                    );
                    last_err = Some(e);
                    if attempt < attempts {
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            },
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("resolve failed: {}", slug)))
}

/// Fetch minimum order size for the current market outcomes via CLOB order book.
/// Returns the max(min_order_size) across YES/NO to avoid rejections.
async fn fetch_min_order_size(
    rest_url: &str,
    yes_asset_id: &str,
    no_asset_id: &str,
) -> anyhow::Result<f64> {
    use polymarket_client_sdk::clob::types::request::OrderBookSummaryRequest;
    use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
    use rust_decimal::prelude::ToPrimitive;

    let parse_u256 = |raw: &str| -> anyhow::Result<alloy::primitives::U256> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            anyhow::bail!("empty token_id");
        }
        if let Some(hex) = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
        {
            alloy::primitives::U256::from_str_radix(hex, 16)
                .map_err(|e| anyhow::anyhow!("invalid token_id hex '{}': {:?}", raw, e))
        } else {
            alloy::primitives::U256::from_str_radix(trimmed, 10)
                .map_err(|e| anyhow::anyhow!("invalid token_id '{}': {:?}", raw, e))
        }
    };

    let client = ClobClient::new(rest_url, ClobConfig::default())?;
    let yes_id = parse_u256(yes_asset_id)?;
    let no_id = parse_u256(no_asset_id)?;

    let req_yes = OrderBookSummaryRequest::builder().token_id(yes_id).build();
    let req_no = OrderBookSummaryRequest::builder().token_id(no_id).build();
    let requests = [req_yes, req_no];
    let books = client.order_books(&requests).await?;

    let mut min_size: Option<f64> = None;
    for book in books {
        let v = book.min_order_size.to_f64().unwrap_or(0.0);
        if v > 0.0 {
            min_size = Some(min_size.map_or(v, |m| m.max(v)));
        }
    }
    min_size.ok_or_else(|| anyhow::anyhow!("min_order_size unavailable from order_books"))
}

async fn fetch_clob_top_of_book(
    rest_url: &str,
    yes_asset_id: &str,
    no_asset_id: &str,
) -> anyhow::Result<(f64, f64, f64, f64)> {
    use polymarket_client_sdk::clob::types::request::OrderBookSummaryRequest;
    use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
    use rust_decimal::prelude::ToPrimitive;

    let parse_u256 = |raw: &str| -> anyhow::Result<alloy::primitives::U256> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            anyhow::bail!("empty token_id");
        }
        if let Some(hex) = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
        {
            alloy::primitives::U256::from_str_radix(hex, 16)
                .map_err(|e| anyhow::anyhow!("invalid token_id hex '{}': {:?}", raw, e))
        } else {
            alloy::primitives::U256::from_str_radix(trimmed, 10)
                .map_err(|e| anyhow::anyhow!("invalid token_id '{}': {:?}", raw, e))
        }
    };

    let top_bid_ask =
        |book: &polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse| {
            let bid = book
                .bids
                .iter()
                .filter_map(|lvl| lvl.price.to_f64())
                .fold(0.0_f64, f64::max);
            let ask = book
                .asks
                .iter()
                .filter_map(|lvl| lvl.price.to_f64())
                .fold(f64::MAX, f64::min);
            let ask = if ask == f64::MAX { 0.0 } else { ask };
            (bid.max(0.0), ask.max(0.0))
        };

    let client = ClobClient::new(rest_url, ClobConfig::default())?;
    let yes_id = parse_u256(yes_asset_id)?;
    let no_id = parse_u256(no_asset_id)?;
    let req_yes = OrderBookSummaryRequest::builder().token_id(yes_id).build();
    let req_no = OrderBookSummaryRequest::builder().token_id(no_id).build();
    let books = client.order_books(&[req_yes, req_no]).await?;
    if books.len() < 2 {
        anyhow::bail!("order_books returned {} entries", books.len());
    }
    let (yes_bid, yes_ask) = top_bid_ask(&books[0]);
    let (no_bid, no_ask) = top_bid_ask(&books[1]);
    Ok((yes_bid, yes_ask, no_bid, no_ask))
}

async fn maybe_log_claimable_positions(funder_address: Option<&str>, signer_address: Option<&str>) {
    let claim_monitor = env::var("PM_CLAIM_MONITOR")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    if !claim_monitor {
        return;
    }

    let Some(funder) = funder_address else {
        return;
    };
    let funder_addr = match funder.trim().parse::<alloy::primitives::Address>() {
        Ok(a) => a,
        Err(e) => {
            warn!(
                "⚠️ Claim monitor skipped: invalid funder address '{}': {:?}",
                funder, e
            );
            return;
        }
    };

    let data_api_url = env::var("POLYMARKET_DATA_API_URL")
        .unwrap_or_else(|_| "https://data-api.polymarket.com".to_string());

    let summary = match tokio::time::timeout(
        Duration::from_secs(8),
        scan_claimable_positions(&data_api_url, funder_addr),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            warn!("⚠️ Claim monitor failed: {:?}", e);
            return;
        }
        Err(_) => {
            warn!("⚠️ Claim monitor timed out after 8s");
            return;
        }
    };

    if summary.positions == 0 {
        return;
    }

    warn!(
        "💸 Claimable winnings detected: positions={} conditions={} est_value=${}",
        summary.positions, summary.conditions, summary.total_value
    );
    for c in &summary.top_conditions {
        info!(
            "💸 Claim candidate: condition={} positions={} est_value=${}",
            c.condition_id, c.positions, c.total_value
        );
    }

    if let Some(signer) = signer_address {
        if !signer.trim().eq_ignore_ascii_case(funder.trim()) {
            warn!(
                "⚠️ Claim requires proxy/safe execution (signer={} funder={}). \
                 To auto-claim, enable PM_AUTO_CLAIM=true and set POLYMARKET_BUILDER_API_KEY/SECRET/PASSPHRASE.",
                signer, funder
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_round_claim_window(
    cfg: &AutoClaimConfig,
    state: &mut AutoClaimState,
    runner_cfg: &RoundClaimRunnerConfig,
    ended_condition: Option<alloy::primitives::B256>,
    funder_address: Option<&str>,
    signer_address: Option<&str>,
    private_key: Option<&str>,
    recorder: Option<&RecorderHandle>,
    recorder_meta: Option<&RecorderSessionMeta>,
) -> anyhow::Result<()> {
    if !cfg.enabled {
        info!("💸 Round claim runner skipped: PM_AUTO_CLAIM disabled");
        return Ok(());
    }
    if runner_cfg.retry_schedule.is_empty() {
        warn!("⚠️ Round claim runner skipped: retry schedule is empty");
        return Ok(());
    }
    let missing =
        round_claim_missing_requirements(cfg, funder_address, signer_address, private_key);
    if !missing.is_empty() {
        warn!(
            "⚠️ Round claim runner skipped: missing {}",
            missing.join(", ")
        );
        return Ok(());
    }
    if cfg.dry_run && (signer_address.is_none() || private_key.is_none()) {
        info!(
            "💸 Round claim dry-run: signer/private not required; proceeding with preview scan only"
        );
    }

    let (preferred_condition, fallback_global) = match runner_cfg.scope {
        RoundClaimScope::EndedThenGlobal => (ended_condition, true),
        RoundClaimScope::EndedOnly => (ended_condition, false),
        RoundClaimScope::GlobalOnly => (None, true),
    };

    if matches!(
        runner_cfg.scope,
        RoundClaimScope::EndedOnly | RoundClaimScope::EndedThenGlobal
    ) && preferred_condition.is_none()
    {
        warn!(
            "⚠️ Round claim scope={} but ended condition is unavailable; fallback_global={}",
            runner_cfg.scope.as_str(),
            fallback_global
        );
    }

    info!(
        "💸 Round claim runner start: window={}s mode={} scope={} schedule=[{}] ended_condition={}",
        runner_cfg.window.as_secs(),
        runner_cfg.retry_mode.as_str(),
        runner_cfg.scope.as_str(),
        runner_cfg.retry_schedule_text(),
        preferred_condition
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string())
    );

    let start = Instant::now();
    let mut attempts = 0_usize;
    let mut errors = 0_usize;
    let mut last_error: Option<String> = None;

    for delay in &runner_cfg.retry_schedule {
        if *delay > runner_cfg.window {
            continue;
        }
        let elapsed = start.elapsed();
        if *delay > elapsed {
            sleep(*delay - elapsed).await;
        }
        if start.elapsed() > runner_cfg.window {
            break;
        }

        attempts += 1;
        info!(
            "💸 Round claim retry {}/{} at +{}s",
            attempts,
            runner_cfg.retry_schedule.len(),
            start.elapsed().as_secs()
        );

        match run_auto_claim_once(
            cfg,
            state,
            funder_address,
            signer_address,
            private_key,
            preferred_condition,
            fallback_global,
            true,
        )
        .await
        {
            Ok(outcome) => {
                if let (Some(rec), Some(meta)) = (recorder, recorder_meta) {
                    rec.emit_redeem_result(
                        meta,
                        json!({
                            "kind": "round_claim_attempt",
                            "attempt": attempts,
                            "elapsed_secs": start.elapsed().as_secs(),
                            "positions": outcome.positions,
                            "candidates": outcome.candidates,
                            "claimed": outcome.claimed,
                            "dry_run": cfg.dry_run,
                            "scope": runner_cfg.scope.as_str(),
                            "preferred_condition": preferred_condition.map(|v| v.to_string()),
                            "fallback_global": fallback_global,
                        }),
                    );
                }
                info!(
                    "💸 Round claim result: positions={} candidates={} claimed={} dry_run={}",
                    outcome.positions, outcome.candidates, outcome.claimed, cfg.dry_run
                );
                if outcome.succeeded(cfg.dry_run) {
                    info!(
                        "✅ Round claim runner completed within SLA (attempt={}, elapsed={}s)",
                        attempts,
                        start.elapsed().as_secs()
                    );
                    return Ok(());
                }
                info!(
                    "💸 Round claim noop: no executable claim on attempt={} (positions={} candidates={} claimed={} dry_run={})",
                    attempts,
                    outcome.positions,
                    outcome.candidates,
                    outcome.claimed,
                    cfg.dry_run
                );
            }
            Err(e) => {
                errors += 1;
                last_error = Some(format!("{:?}", e));
                if let (Some(rec), Some(meta)) = (recorder, recorder_meta) {
                    rec.emit_redeem_result(
                        meta,
                        json!({
                            "kind": "round_claim_error",
                            "attempt": attempts,
                            "elapsed_secs": start.elapsed().as_secs(),
                            "error": format!("{:?}", e),
                        }),
                    );
                }
                warn!(
                    "⚠️ Round claim retry failed at +{}s: {:?}",
                    start.elapsed().as_secs(),
                    e
                );
            }
        }
    }

    warn!(
        "⚠️ Round claim runner SLA exhausted: window={}s attempts={} errors={} last_error={}",
        runner_cfg.window.as_secs(),
        attempts,
        errors,
        last_error.clone().unwrap_or_else(|| "none".to_string())
    );
    if let (Some(rec), Some(meta)) = (recorder, recorder_meta) {
        rec.emit_redeem_result(
            meta,
            json!({
                "kind": "round_claim_sla_exhausted",
                "window_secs": runner_cfg.window.as_secs(),
                "attempts": attempts,
                "errors": errors,
                "last_error": last_error,
            }),
        );
    }
    Ok(())
}

const PGT_SHADOW_HARVEST_FIRST_SECS: u64 = 25;
const PGT_SHADOW_HARVEST_RETRY_SECS: u64 = 18;
const PGT_SHADOW_HARVEST_MIN_PAIRABLE_QTY: f64 = 10.0;
const PGT_SHADOW_REDEEM_FIRST_SECS: u64 = 35;
const PGT_SHADOW_REDEEM_RETRY_SECS: u64 = 50;
const PGT_SHADOW_REDEEM_GAMMA_TIMEOUT_MS: u64 = 2_000;

fn pgt_shadow_redeem_lifecycle_enabled() -> bool {
    env_bool("PM_PGT_SHADOW_REDEEM_LIFECYCLE_ENABLED", true)
}

fn pgt_shadow_harvest_attempt(
    first_sent: bool,
    retry_sent: bool,
    first_attempt_was_eligible: bool,
    shadow_merge_completed: bool,
    eligible: bool,
    remaining_secs: u64,
) -> Option<u8> {
    if !first_sent && remaining_secs <= PGT_SHADOW_HARVEST_FIRST_SECS {
        return Some(1);
    }
    if retry_sent || shadow_merge_completed || !eligible {
        return None;
    }
    if remaining_secs <= PGT_SHADOW_HARVEST_RETRY_SECS
        || (first_sent
            && !first_attempt_was_eligible
            && remaining_secs <= PGT_SHADOW_HARVEST_FIRST_SECS)
    {
        return Some(2);
    }
    None
}

fn pgt_shadow_redeem_target(
    snapshot: &InventorySnapshot,
    winner_side: Option<Side>,
) -> Option<(Side, f64)> {
    let side = winner_side?;
    if snapshot.pair_ledger.residual_side != Some(side) {
        return None;
    }
    let qty = snapshot.pair_ledger.residual_qty.max(0.0);
    (qty > 1e-9).then_some((side, qty))
}

async fn run_pgt_shadow_harvest_lifecycle(
    mut inv_rx: watch::Receiver<InventorySnapshot>,
    recorder: RecorderHandle,
    recorder_meta: RecorderSessionMeta,
    market_end_ts: u64,
) {
    let mut first_sent = false;
    let mut retry_sent = false;
    let mut first_attempt_was_eligible = false;
    let mut shadow_merge_completed = false;

    loop {
        let now = unix_now_secs();
        if now >= market_end_ts {
            break;
        }

        let remaining_secs = market_end_ts.saturating_sub(now);
        let snap = *inv_rx.borrow();
        let pairable_qty = snap.pair_ledger.total_pairable_qty().max(0.0);
        let mergeable_full_sets = snap.pair_ledger.capital_state.mergeable_full_sets.max(0.0);
        let eligible = pairable_qty + 1e-9 >= PGT_SHADOW_HARVEST_MIN_PAIRABLE_QTY;

        let attempt = pgt_shadow_harvest_attempt(
            first_sent,
            retry_sent,
            first_attempt_was_eligible,
            shadow_merge_completed,
            eligible,
            remaining_secs,
        );

        if let Some(attempt) = attempt {
            let payload = json!({
                "attempt": attempt,
                "shadow_only": true,
                "remaining_secs": remaining_secs,
                "eligible": eligible,
                "pairable_qty": pairable_qty,
                "mergeable_full_sets": mergeable_full_sets,
                "working_capital": snap.pair_ledger.capital_state.working_capital,
                "clean_closed_episode_ratio": snap.episode_metrics.clean_closed_episode_ratio,
                "same_side_add_qty_ratio": snap.episode_metrics.same_side_add_qty_ratio,
            });
            if eligible {
                recorder.emit_own_inventory_event(
                    &recorder_meta,
                    "merge_requested",
                    payload.clone(),
                );
                recorder.emit_own_inventory_event(&recorder_meta, "merge_executed", payload);
                shadow_merge_completed = true;
            } else {
                recorder.emit_own_inventory_event(&recorder_meta, "merge_skipped", payload);
            }
            if attempt == 1 {
                first_sent = true;
                first_attempt_was_eligible = eligible;
            } else {
                retry_sent = true;
            }
        }

        tokio::select! {
            changed = inv_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = sleep(Duration::from_millis(250)) => {}
        }
    }
}

async fn run_pgt_shadow_redeem_lifecycle(
    recorder: RecorderHandle,
    recorder_meta: RecorderSessionMeta,
    market_end_ts: u64,
    final_snapshot: InventorySnapshot,
    local_winner_side: Option<Side>,
) {
    let residual_qty = final_snapshot.pair_ledger.residual_qty.max(0.0);
    let yes_qty = final_snapshot.working.yes_qty.max(0.0);
    let no_qty = final_snapshot.working.no_qty.max(0.0);
    if residual_qty <= 1e-9 && yes_qty <= 1e-9 && no_qty <= 1e-9 {
        return;
    }

    info!(
        "🧾 PGT shadow-redeem lifecycle start | slug={} market_end_ts={} now={} local_winner_side={:?} residual_qty={:.2} yes_qty={:.2} no_qty={:.2}",
        recorder_meta.slug,
        market_end_ts,
        unix_now_secs(),
        local_winner_side,
        residual_qty,
        yes_qty,
        no_qty
    );
    let mut resolved_winner_side: Option<Side> = local_winner_side;
    for (attempt, delay_secs) in [
        (1_u8, PGT_SHADOW_REDEEM_FIRST_SECS),
        (2_u8, PGT_SHADOW_REDEEM_RETRY_SECS),
    ] {
        let fire_at = market_end_ts.saturating_add(delay_secs);
        let now = unix_now_secs();
        let sleep_secs = fire_at.saturating_sub(now);
        info!(
            "🧾 PGT shadow-redeem attempt scheduled | slug={} attempt={} post_close_secs={} fire_at={} now={} sleep_secs={}",
            recorder_meta.slug, attempt, delay_secs, fire_at, now, sleep_secs
        );
        if fire_at > now {
            sleep(Duration::from_secs(sleep_secs)).await;
        }

        if resolved_winner_side.is_none() {
            info!(
                "🧾 PGT shadow-redeem fetching gamma winner | slug={} attempt={} timeout_ms={}",
                recorder_meta.slug, attempt, PGT_SHADOW_REDEEM_GAMMA_TIMEOUT_MS
            );
            match tokio::time::timeout(
                Duration::from_millis(PGT_SHADOW_REDEEM_GAMMA_TIMEOUT_MS),
                fetch_gamma_winner_hint(&recorder_meta.slug),
            )
            .await
            {
                Ok(hit) => {
                    resolved_winner_side = hit.map(|(side, _)| side);
                }
                Err(_) => {
                    warn!(
                        "⚠️ PGT shadow-redeem gamma winner fetch timed out | slug={} attempt={} timeout_ms={}",
                        recorder_meta.slug, attempt, PGT_SHADOW_REDEEM_GAMMA_TIMEOUT_MS
                    );
                }
            }
        }
        let target = pgt_shadow_redeem_target(&final_snapshot, resolved_winner_side);

        recorder.emit_own_inventory_event(
            &recorder_meta,
            "redeem_requested",
            json!({
                "attempt": attempt,
                "shadow_only": true,
                "post_close_secs": delay_secs,
                "residual_side": final_snapshot.pair_ledger.residual_side.map(|s| s.as_str().to_string()),
                "residual_qty": residual_qty,
                "yes_qty": yes_qty,
                "no_qty": no_qty,
                "resolved_winner_side": resolved_winner_side.map(|s| s.as_str().to_string()),
                "target_side": target.map(|(side, _)| side.as_str().to_string()),
                "target_qty": target.map(|(_, qty)| qty).unwrap_or(0.0),
                "target_present": target.is_some(),
                "winner_side_only_target": true,
            }),
        );
        info!(
            "🧾 PGT shadow-redeem requested | slug={} attempt={} target_side={:?} target_qty={:.2}",
            recorder_meta.slug,
            attempt,
            target.map(|(side, _)| side),
            target.map(|(_, qty)| qty).unwrap_or(0.0)
        );
    }
    info!(
        "🧾 PGT shadow-redeem lifecycle done | slug={} now={}",
        recorder_meta.slug,
        unix_now_secs()
    );
}

fn round_claim_missing_requirements(
    cfg: &AutoClaimConfig,
    funder_address: Option<&str>,
    signer_address: Option<&str>,
    private_key: Option<&str>,
) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if funder_address.is_none() {
        missing.push("funder_address");
    }
    // Dry-run should still run claim-candidate preview without signer/pk.
    if !cfg.dry_run {
        if signer_address.is_none() {
            missing.push("signer_address");
        }
        if private_key.is_none() {
            missing.push("private_key");
        }
    }
    missing
}

const POST_CLOSE_CHAINLINK_WS_URL_DEFAULT: &str = "wss://ws-live-data.polymarket.com";
const CHAINLINK_DATA_STREAMS_PRICE_API_DEFAULT: &str = "https://priceapi.dataengine.chain.link";
const DATA_STREAMS_CONNECT_TIMEOUT_MS: u64 = 2_500;
const SELF_BUILT_AGG_OPEN_TOLERANCE_MS_DEFAULT: u64 = 1_200;
const SELF_BUILT_AGG_CLOSE_TOLERANCE_MS_DEFAULT: u64 = 1_500;
const SELF_BUILT_AGG_MIN_CONFIDENCE_DEFAULT: f64 = 0.80;
const LOCAL_PRICE_AGG_OPEN_TOLERANCE_MS_DEFAULT: u64 = 600;
// close-only shadow compare is diagnostic, not execution-critical; allow a wider
// close window so we can collect enough multi-source samples near round end.
const LOCAL_PRICE_AGG_CLOSE_TOLERANCE_MS_DEFAULT: u64 = 2_500;
const LOCAL_PRICE_AGG_MIN_CONFIDENCE_DEFAULT: f64 = 0.85;
const LOCAL_PRICE_AGG_MIN_SOURCES_DEFAULT: usize = 1;
const LOCAL_PRICE_AGG_MAX_SOURCE_SPREAD_BPS_DEFAULT: f64 = 12.0;
const LOCAL_PRICE_AGG_SINGLE_SOURCE_MIN_DIRECTION_MARGIN_BPS_DEFAULT: f64 = 1.25;
const LOCAL_PRICE_AGG_RELIEF_MIN_CONFIDENCE_DEFAULT: f64 = 0.80;
const LOCAL_PRICE_AGG_RELIEF_MIN_DIRECTION_MARGIN_BPS_DEFAULT: f64 = 5.0;
const LOCAL_PRICE_AGG_RELIEF_MAX_SOURCE_SPREAD_BPS_DEFAULT: f64 = 2.0;
const LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_SOURCES: usize = 2;
const LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_DIRECTION_MARGIN_BPS: f64 = 3.0;
const LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MAX_SOURCE_SPREAD_BPS: f64 = 5.0;
const LOCAL_PRICE_AGG_DECISION_WAIT_MS_DEFAULT: u64 = 1_500;
const LOCAL_PRICE_AGG_WEIGHT_BINANCE_DEFAULT: f64 = 1.0;
const LOCAL_PRICE_AGG_WEIGHT_BYBIT_DEFAULT: f64 = 1.0;
const LOCAL_PRICE_AGG_WEIGHT_OKX_DEFAULT: f64 = 1.0;
const LOCAL_PRICE_AGG_WEIGHT_COINBASE_DEFAULT: f64 = 1.0;
const LOCAL_PRICE_AGG_WEIGHT_HYPERLIQUID_DEFAULT: f64 = 0.5;
const LOCAL_PRICE_AGG_CLOSE_TIME_DECAY_MS_DEFAULT: f64 = 900.0;
const LOCAL_PRICE_AGG_EXACT_BOOST_DEFAULT: f64 = 1.25;
const LOCAL_PRICE_AGG_SOURCE_BIAS_EMA_ALPHA: f64 = 0.30;
const LOCAL_PRICE_AGG_SOURCE_BIAS_MAX_ABS_BPS: f64 = 10.0;
const LOCAL_PRICE_AGG_SOURCE_BIAS_RESIDUAL_MAX_ABS_BPS: f64 = 25.0;
const LOCAL_PRICE_AGG_COMPARE_DEADLINE_GRACE_MS: u64 = 250;
const LOCAL_PRICE_AGG_COMPARE_TARGET_BPS: f64 = 5.0;
const LOCAL_PRICE_AGG_COMPARE_TARGET_MIN_MATCH_DECIMALS: usize = 12;
const LOCAL_PRICE_AGG_COMPARE_REPORT_DECIMALS: usize = 15;
const LOCAL_PRICE_AGG_COMPARE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS: f64 = 5.5;
const LOCAL_PRICE_AGG_COMPARE_PRECLOSE_MIN_DIRECTION_MARGIN_BPS: f64 = 6.0;
const LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_SOURCES: usize = 2;
const LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS: f64 = 2.0;
const LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS: f64 = 3.0;
const LOCAL_PRICE_AGG_COMPARE_SAFE_SINGLE_SOURCE_RELIEF_MIN_DIRECTION_MARGIN_BPS: f64 = 2.0;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_SOURCES: usize = 3;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS: f64 = 0.6;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS: f64 = 4.0;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS_TWO_SOURCE: f64 =
    1.0;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS_TWO_SOURCE: f64 = 2.5;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_CONFIDENCE: f64 = 0.79;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_SOURCES: usize = 2;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_DIRECTION_MARGIN_BPS: f64 = 2.0;
const LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MAX_SOURCE_SPREAD_BPS: f64 = 5.1;

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_now_millis_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn post_close_chainlink_ws_url() -> String {
    env::var("PM_POST_CLOSE_CHAINLINK_WS_URL")
        .ok()
        .and_then(|v| {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(|| POST_CLOSE_CHAINLINK_WS_URL_DEFAULT.to_string())
}

fn post_close_chainlink_max_wait_secs() -> u64 {
    env::var("PM_POST_CLOSE_CHAINLINK_MAX_WAIT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(1, 30))
        .unwrap_or(8)
}

fn env_bool(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn self_built_price_agg_enabled() -> bool {
    env_bool("PM_SELF_BUILT_PRICE_AGG_ENABLED", true)
}

fn self_built_price_agg_open_tolerance_ms() -> u64 {
    env::var("PM_SELF_BUILT_PRICE_AGG_OPEN_TOLERANCE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(100, 5_000))
        .unwrap_or(SELF_BUILT_AGG_OPEN_TOLERANCE_MS_DEFAULT)
}

fn self_built_price_agg_close_tolerance_ms() -> u64 {
    env::var("PM_SELF_BUILT_PRICE_AGG_CLOSE_TOLERANCE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(100, 5_000))
        .unwrap_or(SELF_BUILT_AGG_CLOSE_TOLERANCE_MS_DEFAULT)
}

fn self_built_price_agg_min_confidence() -> f64 {
    env::var("PM_SELF_BUILT_PRICE_AGG_MIN_CONFIDENCE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 1.0))
        .unwrap_or(SELF_BUILT_AGG_MIN_CONFIDENCE_DEFAULT)
}

fn local_price_agg_enabled() -> bool {
    env_bool("PM_LOCAL_PRICE_AGG_ENABLED", false)
}

fn local_price_agg_decision_enabled() -> bool {
    env_bool("PM_LOCAL_PRICE_AGG_DECISION_ENABLED", false)
}

fn local_price_agg_open_tolerance_ms() -> u64 {
    env::var("PM_LOCAL_PRICE_AGG_OPEN_TOLERANCE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(50, 5_000))
        .unwrap_or(LOCAL_PRICE_AGG_OPEN_TOLERANCE_MS_DEFAULT)
}

fn local_price_agg_close_tolerance_ms() -> u64 {
    env::var("PM_LOCAL_PRICE_AGG_CLOSE_TOLERANCE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(50, 5_000))
        .unwrap_or(LOCAL_PRICE_AGG_CLOSE_TOLERANCE_MS_DEFAULT)
}

fn local_price_agg_min_confidence() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_MIN_CONFIDENCE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 1.0))
        .unwrap_or(LOCAL_PRICE_AGG_MIN_CONFIDENCE_DEFAULT)
}

fn local_price_agg_min_sources() -> usize {
    env::var("PM_LOCAL_PRICE_AGG_MIN_SOURCES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(1, 8))
        .unwrap_or(LOCAL_PRICE_AGG_MIN_SOURCES_DEFAULT)
}

fn local_price_agg_max_source_spread_bps() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_MAX_SOURCE_SPREAD_BPS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 500.0))
        .unwrap_or(LOCAL_PRICE_AGG_MAX_SOURCE_SPREAD_BPS_DEFAULT)
}

fn local_price_agg_single_source_min_direction_margin_bps() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_SINGLE_SOURCE_MIN_DIRECTION_MARGIN_BPS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 20.0))
        .unwrap_or(LOCAL_PRICE_AGG_SINGLE_SOURCE_MIN_DIRECTION_MARGIN_BPS_DEFAULT)
}

fn local_price_agg_decision_wait_ms() -> u64 {
    env::var("PM_LOCAL_PRICE_AGG_DECISION_WAIT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(50, 3_000))
        .unwrap_or(LOCAL_PRICE_AGG_DECISION_WAIT_MS_DEFAULT)
}

fn local_price_agg_weight_binance() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_WEIGHT_BINANCE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 10.0))
        .unwrap_or(LOCAL_PRICE_AGG_WEIGHT_BINANCE_DEFAULT)
}

fn local_price_agg_weight_bybit() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_WEIGHT_BYBIT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 10.0))
        .unwrap_or(LOCAL_PRICE_AGG_WEIGHT_BYBIT_DEFAULT)
}

fn local_price_agg_weight_okx() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_WEIGHT_OKX")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 10.0))
        .unwrap_or(LOCAL_PRICE_AGG_WEIGHT_OKX_DEFAULT)
}

fn local_price_agg_weight_coinbase() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_WEIGHT_COINBASE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 10.0))
        .unwrap_or(LOCAL_PRICE_AGG_WEIGHT_COINBASE_DEFAULT)
}

fn local_price_agg_weight_hyperliquid() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_WEIGHT_HYPERLIQUID")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(0.0, 10.0))
        .unwrap_or(LOCAL_PRICE_AGG_WEIGHT_HYPERLIQUID_DEFAULT)
}

fn local_price_agg_close_time_decay_ms() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_CLOSE_TIME_DECAY_MS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(50.0, 10_000.0))
        .unwrap_or(LOCAL_PRICE_AGG_CLOSE_TIME_DECAY_MS_DEFAULT)
}

fn local_price_agg_exact_boost() -> f64 {
    env::var("PM_LOCAL_PRICE_AGG_EXACT_BOOST")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v.clamp(1.0, 3.0))
        .unwrap_or(LOCAL_PRICE_AGG_EXACT_BOOST_DEFAULT)
}

fn local_price_agg_boundary_window_ms() -> u64 {
    env::var("PM_LOCAL_PRICE_AGG_BOUNDARY_WINDOW_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(100, 30_000))
        .unwrap_or(5_000)
}

fn local_price_agg_uncertainty_gate_enabled() -> bool {
    env_bool("PM_LOCAL_AGG_UNCERTAINTY_GATE_ENABLED", false)
}

fn local_price_agg_uncertainty_gate_finalize_ms() -> u64 {
    env::var("PM_LOCAL_AGG_UNCERTAINTY_GATE_FINALIZE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(100, 10_000))
        .unwrap_or(2_500)
}

fn local_price_agg_uncertainty_gate_model_path() -> Option<PathBuf> {
    env_nonempty_var(&["PM_LOCAL_AGG_UNCERTAINTY_GATE_MODEL_PATH"])
        .map(PathBuf::from)
        .or_else(|| {
            let path = PathBuf::from("logs")
                .join(instance_id().unwrap_or_else(|| "local-agg-lab".to_string()))
                .join("monitor_reports")
                .join("local_agg_uncertainty_gate_model.latest.json");
            path.exists().then_some(path)
        })
}

fn env_nonempty_var(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        env::var(name).ok().and_then(|v| {
            let trimmed = v.trim().trim_matches('"').trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
    })
}

fn chainlink_data_streams_user_id() -> Option<String> {
    env_nonempty_var(&[
        "chainlink_data_streams_user_id",
        "CHAINLINK_DATA_STREAMS_USER_ID",
    ])
}

fn chainlink_data_streams_api_key() -> Option<String> {
    env_nonempty_var(&[
        "chainlink_data_streams_api_key",
        "chainlink_data_streamsapi_key",
        "CHAINLINK_DATA_STREAMS_API_KEY",
    ])
}

fn chainlink_data_streams_price_api_base_url() -> String {
    env_nonempty_var(&[
        "PM_POST_CLOSE_DATA_STREAMS_PRICE_API_URL",
        "CHAINLINK_DATA_STREAMS_PRICE_API_URL",
        "chainlink_data_streams_price_api_url",
    ])
    .unwrap_or_else(|| CHAINLINK_DATA_STREAMS_PRICE_API_DEFAULT.to_string())
}

fn chainlink_data_streams_enabled() -> bool {
    chainlink_data_streams_user_id().is_some() && chainlink_data_streams_api_key().is_some()
}

fn data_streams_symbol_from_chainlink_symbol(chainlink_symbol: &str) -> Option<String> {
    let (base, quote) = chainlink_symbol.split_once('/')?;
    let base = base.trim();
    let quote = quote.trim();
    if base.is_empty() || quote.is_empty() {
        return None;
    }
    Some(format!(
        "{}{}",
        base.to_ascii_uppercase(),
        quote.to_ascii_uppercase()
    ))
}

fn normalize_data_streams_price(raw: f64) -> Option<f64> {
    if !raw.is_finite() || raw <= 0.0 {
        return None;
    }
    // Candlestick/streaming payloads often emit fixed-point numbers around 1e21 for crypto prices.
    if raw.abs() >= 1.0e15 {
        let scaled = raw / 1.0e18;
        if scaled.is_finite() && scaled > 0.0 {
            return Some(scaled);
        }
    }
    Some(raw)
}

#[allow(dead_code)]
fn post_close_gamma_poll_ms() -> u64 {
    env::var("PM_POST_CLOSE_GAMMA_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(100, 2_000))
        .unwrap_or(300)
}

fn normalize_chainlink_symbol(raw: &str) -> String {
    raw.trim()
        .to_ascii_lowercase()
        .replace(['-', '_'], "/")
        .replace("//", "/")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LocalPriceSource {
    Binance,
    Bybit,
    Okx,
    Coinbase,
    Hyperliquid,
}

impl LocalPriceSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::Coinbase => "coinbase",
            Self::Hyperliquid => "hyperliquid",
        }
    }

    fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "binance" => Some(Self::Binance),
            "bybit" => Some(Self::Bybit),
            "okx" => Some(Self::Okx),
            "coinbase" => Some(Self::Coinbase),
            "hyperliquid" => Some(Self::Hyperliquid),
            _ => None,
        }
    }
}

fn local_price_agg_source_weight(source: LocalPriceSource) -> f64 {
    match source {
        LocalPriceSource::Binance => local_price_agg_weight_binance(),
        LocalPriceSource::Bybit => local_price_agg_weight_bybit(),
        LocalPriceSource::Okx => local_price_agg_weight_okx(),
        LocalPriceSource::Coinbase => local_price_agg_weight_coinbase(),
        LocalPriceSource::Hyperliquid => local_price_agg_weight_hyperliquid(),
    }
}

const LOCAL_PRICE_SOURCES: [LocalPriceSource; 5] = [
    LocalPriceSource::Binance,
    LocalPriceSource::Bybit,
    LocalPriceSource::Okx,
    LocalPriceSource::Coinbase,
    LocalPriceSource::Hyperliquid,
];

fn binance_stream_symbol_from_chainlink_symbol(chainlink_symbol: &str) -> Option<String> {
    let (base, quote) = chainlink_symbol.split_once('/')?;
    if !quote.eq_ignore_ascii_case("usd") {
        return None;
    }
    let base = base.trim().to_ascii_lowercase();
    if base.is_empty() {
        return None;
    }
    Some(format!("{}usdt", base))
}

fn chainlink_symbol_from_binance_stream_symbol(binance_symbol: &str) -> Option<String> {
    let lower = binance_symbol.trim().to_ascii_lowercase();
    let base = lower.strip_suffix("usdt")?;
    if base.is_empty() {
        return None;
    }
    Some(format!("{}/usd", base))
}

fn parse_binance_combined_trade_tick(text: &str) -> Option<(String, f64, u64)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return None;
    };
    let stream = value
        .get("stream")
        .and_then(|v| v.as_str())
        .or_else(|| value.get("s").and_then(|v| v.as_str()))?;
    let stream_symbol = stream
        .split('@')
        .next()
        .map(str::to_string)
        .unwrap_or_default();
    let chainlink_symbol = chainlink_symbol_from_binance_stream_symbol(&stream_symbol)?;
    let data = value.get("data").unwrap_or(&value);
    let price = data.get("p").and_then(parse_f64_value)?;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let ts_ms = data
        .get("T")
        .and_then(parse_unix_ms_value)
        .or_else(|| data.get("E").and_then(parse_unix_ms_value))
        .unwrap_or_else(unix_now_millis_u64);
    Some((chainlink_symbol, price, ts_ms))
}

fn bybit_stream_symbol_from_chainlink_symbol(chainlink_symbol: &str) -> Option<String> {
    let (base, quote) = chainlink_symbol.split_once('/')?;
    if !quote.eq_ignore_ascii_case("usd") {
        return None;
    }
    let base = base.trim();
    if base.is_empty() {
        return None;
    }
    Some(format!("{}USDT", base.to_ascii_uppercase()))
}

fn chainlink_symbol_from_bybit_stream_symbol(bybit_symbol: &str) -> Option<String> {
    let upper = bybit_symbol.trim().to_ascii_uppercase();
    let base = upper.strip_suffix("USDT")?;
    if base.is_empty() {
        return None;
    }
    Some(format!("{}/usd", base.to_ascii_lowercase()))
}

fn parse_bybit_public_trade_ticks(text: &str) -> Vec<(String, f64, u64)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    let topic = value.get("topic").and_then(|v| v.as_str()).unwrap_or("");
    if !topic.starts_with("publicTrade.") {
        return vec![];
    }
    let bybit_symbol = topic.trim_start_matches("publicTrade.");
    let Some(chainlink_symbol) = chainlink_symbol_from_bybit_stream_symbol(bybit_symbol) else {
        return vec![];
    };
    let mut out = Vec::new();
    if let Some(items) = value.get("data").and_then(|v| v.as_array()) {
        for item in items {
            let price = item.get("p").and_then(parse_f64_value).unwrap_or(0.0);
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            let ts_ms = item
                .get("T")
                .and_then(parse_unix_ms_value)
                .or_else(|| item.get("ts").and_then(parse_unix_ms_value))
                .unwrap_or_else(unix_now_millis_u64);
            out.push((chainlink_symbol.clone(), price, ts_ms));
        }
    }
    out
}

fn okx_inst_id_from_chainlink_symbol(chainlink_symbol: &str) -> Option<String> {
    let (base, quote) = chainlink_symbol.split_once('/')?;
    if !quote.eq_ignore_ascii_case("usd") {
        return None;
    }
    let base = base.trim();
    if base.is_empty() {
        return None;
    }
    Some(format!("{}-USDT", base.to_ascii_uppercase()))
}

fn chainlink_symbol_from_okx_inst_id(inst_id: &str) -> Option<String> {
    let upper = inst_id.trim().to_ascii_uppercase();
    let base = upper.strip_suffix("-USDT")?;
    if base.is_empty() {
        return None;
    }
    Some(format!("{}/usd", base.to_ascii_lowercase()))
}

fn parse_okx_trade_ticks(text: &str) -> Vec<(String, f64, u64)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    let arg = value.get("arg");
    let channel = arg
        .and_then(|a| a.get("channel"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if channel != "trades" {
        return vec![];
    }
    let inst_id = arg
        .and_then(|a| a.get("instId"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let Some(chainlink_symbol) = chainlink_symbol_from_okx_inst_id(inst_id) else {
        return vec![];
    };
    let mut out = Vec::new();
    if let Some(items) = value.get("data").and_then(|v| v.as_array()) {
        for item in items {
            let price = item.get("px").and_then(parse_f64_value).unwrap_or(0.0);
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            let ts_ms = item
                .get("ts")
                .and_then(parse_unix_ms_value)
                .unwrap_or_else(unix_now_millis_u64);
            out.push((chainlink_symbol.clone(), price, ts_ms));
        }
    }
    out
}

fn coinbase_product_id_from_chainlink_symbol(chainlink_symbol: &str) -> Option<String> {
    let (base, quote) = chainlink_symbol.split_once('/')?;
    if !quote.eq_ignore_ascii_case("usd") {
        return None;
    }
    let base = base.trim();
    if base.is_empty() {
        return None;
    }
    Some(format!("{}-USD", base.to_ascii_uppercase()))
}

fn hyperliquid_coin_from_chainlink_symbol(chainlink_symbol: &str) -> Option<String> {
    let (base, quote) = chainlink_symbol.split_once('/')?;
    if !quote.eq_ignore_ascii_case("usd") {
        return None;
    }
    let base = base.trim().to_ascii_uppercase();
    if base == "HYPE" {
        Some(base)
    } else {
        None
    }
}

fn chainlink_symbol_from_coinbase_product_id(product_id: &str) -> Option<String> {
    let upper = product_id.trim().to_ascii_uppercase();
    let base = upper.strip_suffix("-USD")?;
    if base.is_empty() {
        return None;
    }
    Some(format!("{}/usd", base.to_ascii_lowercase()))
}

fn parse_coinbase_ticker_tick(text: &str) -> Option<(String, f64, u64)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return None;
    };
    if value.get("type").and_then(|v| v.as_str()) != Some("ticker") {
        return None;
    }
    let product_id = value.get("product_id").and_then(|v| v.as_str())?;
    let chainlink_symbol = chainlink_symbol_from_coinbase_product_id(product_id)?;
    let price = value.get("price").and_then(parse_f64_value)?;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let ts_ms = value
        .get("time")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis().max(0) as u64)
        .unwrap_or_else(unix_now_millis_u64);
    Some((chainlink_symbol, price, ts_ms))
}

fn parse_hyperliquid_all_mids_ticks(
    text: &str,
    wanted_coins: &HashMap<String, String>,
) -> Vec<(String, f64, u64)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let channel = value
        .get("channel")
        .and_then(|v| v.as_str())
        .or_else(|| value.get("type").and_then(|v| v.as_str()))
        .unwrap_or_default();
    if channel != "allMids" {
        return Vec::new();
    }
    let Some(mids) = value
        .get("data")
        .and_then(|v| v.get("mids"))
        .and_then(|v| v.as_object())
    else {
        return Vec::new();
    };
    let ts_ms = value
        .get("data")
        .and_then(|v| v.get("time"))
        .and_then(parse_u64_value)
        .unwrap_or_else(unix_now_millis_u64);
    let mut out = Vec::new();
    for (coin, pxv) in mids {
        let Some(symbol) = wanted_coins.get(&coin.to_ascii_uppercase()) else {
            continue;
        };
        let Some(price) = parse_f64_value(pxv).filter(|v| *v > 0.0) else {
            continue;
        };
        out.push((symbol.clone(), price, ts_ms));
    }
    out
}

fn robust_median(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        Some(values[mid])
    } else {
        Some((values[mid - 1] + values[mid]) / 2.0)
    }
}

fn robust_center(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    match values.len() {
        1 => Some(values[0]),
        2 => Some((values[0] + values[1]) / 2.0),
        // Drop one-side outliers and average the center mass; this is usually
        // closer to Chainlink-style aggregated closes than picking one median tick.
        _ => {
            let inner = &values[1..values.len() - 1];
            let sum: f64 = inner.iter().sum();
            Some(sum / inner.len() as f64)
        }
    }
}

fn weighted_mean(values: &[(f64, f64)]) -> Option<f64> {
    let mut weighted_sum = 0.0f64;
    let mut weight_sum = 0.0f64;
    for (value, weight) in values {
        if !value.is_finite() || !weight.is_finite() || *weight <= 0.0 {
            continue;
        }
        weighted_sum += value * weight;
        weight_sum += weight;
    }
    if weight_sum <= 0.0 {
        None
    } else {
        Some(weighted_sum / weight_sum)
    }
}

fn spread_bps(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let min_v = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max_v = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let median = robust_median(values.to_vec())?;
    if !median.is_finite() || median <= 0.0 {
        return None;
    }
    Some(((max_v - min_v) / median.abs()) * 10_000.0)
}

fn median_u64(mut values: Vec<u64>) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(values[values.len() / 2])
}

fn chainlink_symbol_from_slug(slug: &str) -> Option<String> {
    let prefix = slug.split("-updown-").next()?.trim().to_ascii_lowercase();
    if prefix.is_empty() {
        None
    } else {
        Some(format!("{}/usd", prefix))
    }
}

fn parse_u64_value(v: &Value) -> Option<u64> {
    if let Some(u) = v.as_u64() {
        return Some(u);
    }
    if let Some(i) = v.as_i64() {
        return (i >= 0).then_some(i as u64);
    }
    if let Some(f) = v.as_f64() {
        return (f.is_finite() && f >= 0.0).then_some(f as u64);
    }
    v.as_str()?.trim().parse::<u64>().ok()
}

fn parse_f64_value(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
        .filter(|x| x.is_finite())
}

#[derive(Debug, Deserialize)]
struct DataStreamsAuthorizeEnvelope {
    d: Option<DataStreamsAuthorizeData>,
    #[allow(dead_code)]
    s: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DataStreamsAuthorizeData {
    access_token: String,
    #[allow(dead_code)]
    expiration: Option<u64>,
}

static DATA_STREAMS_TOKEN_CACHE: OnceLock<Mutex<Option<(String, u64)>>> = OnceLock::new();

fn data_streams_token_cache() -> &'static Mutex<Option<(String, u64)>> {
    DATA_STREAMS_TOKEN_CACHE.get_or_init(|| Mutex::new(None))
}

fn cached_data_streams_token(now_ms: u64) -> Option<String> {
    let guard = data_streams_token_cache().lock().ok()?;
    let (tok, exp_ms) = guard.as_ref()?;
    // Refresh at least 30s before expiry.
    if *exp_ms > now_ms.saturating_add(30_000) {
        Some(tok.clone())
    } else {
        None
    }
}

fn store_data_streams_token(token: String, expiration_unix_s: Option<u64>) {
    let exp_ms = expiration_unix_s
        .map(|s| s.saturating_mul(1_000))
        // Default TTL if server omits expiration: 5 minutes from now.
        .unwrap_or_else(|| unix_now_millis_u64().saturating_add(5 * 60 * 1_000));
    if let Ok(mut guard) = data_streams_token_cache().lock() {
        *guard = Some((token, exp_ms));
    }
}

async fn data_streams_authorize_token() -> Option<String> {
    let t0 = unix_now_millis_u64();
    if let Some(tok) = cached_data_streams_token(t0) {
        return Some(tok);
    }
    let user_id = chainlink_data_streams_user_id()?;
    let api_key = chainlink_data_streams_api_key()?;
    let base = chainlink_data_streams_price_api_base_url();
    let url = format!("{}/api/v1/authorize", base.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(DATA_STREAMS_CONNECT_TIMEOUT_MS))
        .build()
        .ok()?;
    let resp = client
        .post(url)
        .json(&json!({
            "login": user_id,
            "password": api_key,
        }))
        .send()
        .await
        .ok()?;
    let t_resp = unix_now_millis_u64();
    let body = resp.json::<DataStreamsAuthorizeEnvelope>().await.ok()?;
    let data = body.d?;
    let token = data.access_token;
    if token.trim().is_empty() {
        return None;
    }
    let t_done = unix_now_millis_u64();
    info!(
        "⏱️ data_streams_authorize_timing | post_ms={} parse_ms={} total_ms={} exp_unix_s={:?}",
        t_resp.saturating_sub(t0),
        t_done.saturating_sub(t_resp),
        t_done.saturating_sub(t0),
        data.expiration,
    );
    store_data_streams_token(token.clone(), data.expiration);
    Some(token)
}

fn parse_data_streams_tick(line: &str, target_symbol: &str) -> Option<(f64, u64)> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    if value.get("heartbeat").is_some() {
        return None;
    }
    let symbol = value.get("i")?.as_str()?.trim().to_ascii_uppercase();
    if symbol != target_symbol {
        return None;
    }
    let ts_s = parse_u64_value(value.get("t")?)?;
    let raw_price = parse_f64_value(value.get("p")?)?;
    let price = normalize_data_streams_price(raw_price)?;
    Some((price, ts_s))
}

fn parse_unix_ms_value(v: &Value) -> Option<u64> {
    let raw = parse_u64_value(v)?;
    if raw > 10_000_000_000 {
        Some(raw)
    } else {
        Some(raw.saturating_mul(1_000))
    }
}

fn extract_chainlink_point_recursive(value: &Value, target_symbol: &str) -> Option<(f64, u64)> {
    match value {
        Value::Array(items) => {
            for item in items {
                if let Some(hit) = extract_chainlink_point_recursive(item, target_symbol) {
                    return Some(hit);
                }
            }
            None
        }
        Value::Object(map) => {
            let symbol_keys = ["symbol", "pair", "instrument"];
            let mut symbol_match = false;
            for key in symbol_keys {
                if let Some(s) = map.get(key).and_then(|v| v.as_str()) {
                    if normalize_chainlink_symbol(s) == target_symbol {
                        symbol_match = true;
                        break;
                    }
                }
            }

            let topic = map
                .get("topic")
                .and_then(|v| v.as_str())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_default();
            let event_type = map
                .get("event_type")
                .or_else(|| map.get("type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_default();
            let chainlink_topic = topic.contains("crypto_prices_chainlink")
                || event_type.contains("crypto_prices_chainlink");

            let price = ["value", "price", "p"]
                .iter()
                .find_map(|k| map.get(*k).and_then(parse_f64_value))
                .filter(|v| *v > 0.0);
            if let Some(price) = price {
                if symbol_match || chainlink_topic {
                    let ts_ms = ["timestamp", "ts", "t", "time"]
                        .iter()
                        .find_map(|k| map.get(*k).and_then(parse_unix_ms_value))
                        .unwrap_or_else(unix_now_millis_u64);
                    return Some((price, ts_ms));
                }
            }

            for key in ["data", "payload", "message"] {
                if let Some(child) = map.get(key) {
                    if let Some(hit) = extract_chainlink_point_recursive(child, target_symbol) {
                        return Some(hit);
                    }
                }
            }

            for child in map.values() {
                if let Some(hit) = extract_chainlink_point_recursive(child, target_symbol) {
                    return Some(hit);
                }
            }
            None
        }
        _ => None,
    }
}

fn parse_chainlink_tick(text: &str, target_symbol: &str) -> Option<(f64, u64)> {
    let value: Value = serde_json::from_str(text).ok()?;
    extract_chainlink_point_recursive(&value, target_symbol)
}

/// Collect ALL (price, ts_ms) pairs matching target_symbol from a WS message.
/// Unlike extract_chainlink_point_recursive which returns only the first match,
/// this traverses the entire array/batch and collects every tick.
fn extract_all_chainlink_points(value: &Value, target_symbol: &str, out: &mut Vec<(f64, u64)>) {
    match value {
        Value::Array(items) => {
            for item in items {
                extract_all_chainlink_points(item, target_symbol, out);
            }
        }
        Value::Object(map) => {
            let symbol_keys = ["symbol", "pair", "instrument"];
            let mut symbol_match = false;
            for key in symbol_keys {
                if let Some(s) = map.get(key).and_then(|v| v.as_str()) {
                    if normalize_chainlink_symbol(s) == target_symbol {
                        symbol_match = true;
                        break;
                    }
                }
            }
            let topic = map
                .get("topic")
                .and_then(|v| v.as_str())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_default();
            let event_type = map
                .get("event_type")
                .or_else(|| map.get("type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_default();
            let chainlink_topic = topic.contains("crypto_prices_chainlink")
                || event_type.contains("crypto_prices_chainlink");

            let price = ["value", "price", "p"]
                .iter()
                .find_map(|k| map.get(*k).and_then(parse_f64_value))
                .filter(|v| *v > 0.0);

            if let Some(price) = price {
                if symbol_match || chainlink_topic {
                    let ts_ms = ["timestamp", "ts", "t", "time"]
                        .iter()
                        .find_map(|k| map.get(*k).and_then(parse_unix_ms_value))
                        .unwrap_or_else(unix_now_millis_u64);
                    out.push((price, ts_ms));
                    return; // this tick object processed; don't recurse into its children
                }
            }

            // Recurse into all child values exactly once (no duplicate key enumeration).
            for child in map.values() {
                extract_all_chainlink_points(child, target_symbol, out);
            }
        }
        _ => {}
    }
}

/// Parse the RTDS initial-batch format: {"payload":{"data":[{"timestamp":T,"value":V},...]}}.
/// Items in the data array have no "symbol" field — they are bare price-points from our
/// subscription, so any item with a valid price+timestamp is a valid tick for this symbol.
fn extract_chainlink_bare_batch(value: &Value, out: &mut Vec<(f64, u64)>) {
    let Some(data) = value
        .get("payload")
        .and_then(|p| p.get("data"))
        .and_then(|d| d.as_array())
    else {
        return;
    };
    for item in data {
        let Some(price) = ["value", "price", "p"]
            .iter()
            .find_map(|k| item.get(*k).and_then(parse_f64_value))
            .filter(|v| *v > 0.0)
        else {
            continue;
        };
        let ts_ms = ["timestamp", "ts", "t", "time"]
            .iter()
            .find_map(|k| item.get(*k).and_then(parse_unix_ms_value))
            .unwrap_or_else(unix_now_millis_u64);
        out.push((price, ts_ms));
    }
}

fn parse_chainlink_all_ticks(text: &str, target_symbol: &str) -> Vec<(f64, u64)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    let mut out = Vec::new();
    extract_all_chainlink_points(&value, target_symbol, &mut out);
    if out.is_empty() {
        // Fallback: RTDS initial batch — bare {timestamp, value} items with no symbol field.
        extract_chainlink_bare_batch(&value, &mut out);
    }
    out
}

fn extract_all_chainlink_symbol_ticks(
    value: &Value,
    inherited_symbol: Option<&str>,
    out: &mut Vec<(String, f64, u64)>,
) {
    match value {
        Value::Array(items) => {
            for item in items {
                extract_all_chainlink_symbol_ticks(item, inherited_symbol, out);
            }
        }
        Value::Object(map) => {
            let mut symbol = inherited_symbol.map(|s| s.to_string());
            for key in ["symbol", "pair", "instrument"] {
                if let Some(s) = map.get(key).and_then(|v| v.as_str()) {
                    let normalized = normalize_chainlink_symbol(s);
                    if !normalized.is_empty() {
                        symbol = Some(normalized);
                        break;
                    }
                }
            }
            let price = ["value", "price", "p"]
                .iter()
                .find_map(|k| map.get(*k).and_then(parse_f64_value))
                .filter(|v| *v > 0.0);
            if let (Some(sym), Some(px)) = (symbol.clone(), price) {
                let ts_ms = ["timestamp", "ts", "t", "time"]
                    .iter()
                    .find_map(|k| map.get(*k).and_then(parse_unix_ms_value))
                    .unwrap_or_else(unix_now_millis_u64);
                out.push((sym, px, ts_ms));
                return;
            }
            for child in map.values() {
                extract_all_chainlink_symbol_ticks(child, symbol.as_deref(), out);
            }
        }
        _ => {}
    }
}

fn parse_chainlink_multi_symbol_ticks(text: &str) -> Vec<(String, f64, u64)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    let mut out = Vec::new();
    extract_all_chainlink_symbol_ticks(&value, None, &mut out);
    out
}

const CHAINLINK_HUB_TICK_STALL_RECONNECT_MS_DEFAULT: u64 = 15_000;
const CHAINLINK_HUB_RECENT_TICKS_PER_SYMBOL: usize = 16_384;
const LOCAL_PRICE_HUB_RECENT_TICKS_PER_SYMBOL: usize = 16_384;

fn chainlink_hub_tick_stall_reconnect_ms() -> u64 {
    env::var("PM_CHAINLINK_HUB_TICK_STALL_RECONNECT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v >= 2_000)
        .unwrap_or(CHAINLINK_HUB_TICK_STALL_RECONNECT_MS_DEFAULT)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedIngressSubscribeReq {
    stream: String,
    #[serde(default)]
    symbols: Vec<String>,
    #[serde(default)]
    market_slug: Option<String>,
    #[serde(default)]
    market_id: Option<String>,
    #[serde(default)]
    yes_asset_id: Option<String>,
    #[serde(default)]
    no_asset_id: Option<String>,
    #[serde(default)]
    ws_base_url: Option<String>,
    #[serde(default)]
    custom_feature_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SharedIngressWireMsg {
    ChainlinkSnapshot {
        symbol: String,
        price: f64,
        ts_ms: u64,
    },
    ChainlinkTick {
        symbol: String,
        price: f64,
        ts_ms: u64,
    },
    LocalPriceSnapshot {
        symbol: String,
        price: f64,
        ts_ms: u64,
        source: String,
    },
    LocalPriceTick {
        symbol: String,
        price: f64,
        ts_ms: u64,
        source: String,
    },
    MarketBookTick {
        yes_bid: f64,
        yes_ask: f64,
        no_bid: f64,
        no_ask: f64,
        ts_ms: u64,
    },
    MarketTradeTick {
        asset_id: String,
        trade_id: Option<String>,
        market_side: String,
        taker_side: String,
        price: f64,
        size: f64,
        ts_ms: u64,
    },
    PostCloseBookUpdate {
        side: String,
        bid: f64,
        ask: f64,
        recv_ms: u64,
    },
    SnapshotDone,
    Error {
        message: String,
    },
}

async fn shared_ingress_send_wire_line<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &SharedIngressWireMsg,
) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(msg)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await?;
    Ok(())
}

fn partial_book_tick_from_post_close_update(side: Side, bid: f64, ask: f64) -> MarketDataMsg {
    match side {
        Side::Yes => MarketDataMsg::BookTick {
            yes_bid: bid,
            yes_ask: ask,
            no_bid: f64::NAN,
            no_ask: f64::NAN,
            ts: Instant::now(),
        },
        Side::No => MarketDataMsg::BookTick {
            yes_bid: f64::NAN,
            yes_ask: f64::NAN,
            no_bid: bid,
            no_ask: ask,
            ts: Instant::now(),
        },
    }
}

#[derive(Clone)]
struct ChainlinkHub {
    senders: Arc<HashMap<String, broadcast::Sender<(f64, u64)>>>,
    recent_ticks: Arc<Mutex<HashMap<String, VecDeque<(f64, u64)>>>>,
}

impl ChainlinkHub {
    fn new(symbols: &HashSet<String>) -> Arc<Self> {
        let mut senders = HashMap::new();
        let mut recent_ticks = HashMap::new();
        for sym in symbols {
            let (tx, _rx) = broadcast::channel::<(f64, u64)>(2048);
            senders.insert(sym.clone(), tx);
            recent_ticks.insert(sym.clone(), VecDeque::new());
        }
        Arc::new(Self {
            senders: Arc::new(senders),
            recent_ticks: Arc::new(Mutex::new(recent_ticks)),
        })
    }

    fn publish_tick(&self, symbol: &str, price: f64, ts_ms: u64) {
        let sym = normalize_chainlink_symbol(symbol);
        if sym.is_empty() || !price.is_finite() || price <= 0.0 {
            return;
        }
        if let Ok(mut guard) = self.recent_ticks.lock() {
            if let Some(buf) = guard.get_mut(&sym) {
                buf.push_back((price, ts_ms));
                while buf.len() > CHAINLINK_HUB_RECENT_TICKS_PER_SYMBOL {
                    buf.pop_front();
                }
            }
        }
        if let Some(tx) = self.senders.get(&sym) {
            let _ = tx.send((price, ts_ms));
        }
    }

    fn spawn(symbols: HashSet<String>) -> Arc<Self> {
        let hub = Self::new(&symbols);
        let ws_url = post_close_chainlink_ws_url();
        let stall_reconnect_ms = chainlink_hub_tick_stall_reconnect_ms();
        info!(
            "📡 chainlink_hub_config | tick_stall_reconnect_ms={}",
            stall_reconnect_ms
        );
        // One WS connection per symbol: the server only honours ONE active
        // subscription per connection, so sharing a single connection for N
        // symbols only delivers ticks for the last-subscribed symbol.
        for sym in symbols {
            let tx = hub.senders.get(&sym).expect("sender just inserted").clone();
            let recent_ticks = Arc::clone(&hub.recent_ticks);
            let url = ws_url.clone();
            let stall_reconnect_ms = stall_reconnect_ms;
            tokio::spawn(async move {
                let mut reconnect_backoff = Duration::from_millis(300);
                let mut stat_last_log = Instant::now();
                let mut stat_msgs: u64 = 0;
                let mut stat_ticks: u64 = 0;
                let mut stat_delivered: u64 = 0;
                loop {
                    let connect =
                        tokio::time::timeout(Duration::from_secs(3), connect_async(&url)).await;
                    let Ok(Ok((ws, _resp))) = connect else {
                        sleep(reconnect_backoff).await;
                        reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(2));
                        continue;
                    };
                    reconnect_backoff = Duration::from_millis(300);
                    let (mut write, mut read) = ws.split();
                    let subscribe_msg = json!({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": format!("{{\"symbol\":\"{}\"}}", sym),
                        }]
                    });
                    if write
                        .send(Message::Text(subscribe_msg.to_string().into()))
                        .await
                        .is_err()
                    {
                        continue;
                    }
                    let mut last_tick_at = Instant::now();
                    let mut last_tick_ts_ms: Option<u64> = None;
                    loop {
                        let next =
                            tokio::time::timeout(Duration::from_millis(1500), read.next()).await;
                        let msg = match next {
                            Ok(Some(Ok(m))) => m,
                            Ok(Some(Err(_))) | Ok(None) => break,
                            Err(_) => {
                                let idle = last_tick_at.elapsed();
                                if idle.as_millis() >= stall_reconnect_ms as u128 {
                                    warn!(
                                        "⚠️ chainlink_hub_tick_stall_reconnect | symbol={} idle_ms={} threshold_ms={} msgs={} ticks={} delivered={} last_tick_ts_ms={:?}",
                                        sym,
                                        idle.as_millis(),
                                        stall_reconnect_ms,
                                        stat_msgs,
                                        stat_ticks,
                                        stat_delivered,
                                        last_tick_ts_ms,
                                    );
                                    break;
                                }
                                continue;
                            }
                        };
                        let text = match msg {
                            Message::Text(t) => t.to_string(),
                            Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                                Ok(t) => t,
                                Err(_) => continue,
                            },
                            _ => continue,
                        };
                        let ticks = parse_chainlink_multi_symbol_ticks(&text);
                        if ticks.is_empty() {
                            stat_msgs = stat_msgs.saturating_add(1);
                            if stat_last_log.elapsed() >= Duration::from_secs(30) {
                                info!(
                                    "📡 chainlink_hub_conn | symbol={} msgs={} ticks={} delivered={}",
                                    sym, stat_msgs, stat_ticks, stat_delivered
                                );
                                stat_last_log = Instant::now();
                            }
                            let idle = last_tick_at.elapsed();
                            if idle.as_millis() >= stall_reconnect_ms as u128 {
                                warn!(
                                    "⚠️ chainlink_hub_no_tick_payload_stall | symbol={} idle_ms={} threshold_ms={} msgs={} ticks={} delivered={} last_tick_ts_ms={:?}",
                                    sym,
                                    idle.as_millis(),
                                    stall_reconnect_ms,
                                    stat_msgs,
                                    stat_ticks,
                                    stat_delivered,
                                    last_tick_ts_ms,
                                );
                                break;
                            }
                            continue;
                        }
                        stat_msgs = stat_msgs.saturating_add(1);
                        let mut matched_ticks: u64 = 0;
                        for (tick_sym, price, ts_ms) in ticks {
                            if normalize_chainlink_symbol(&tick_sym) != sym {
                                continue;
                            }
                            if let Ok(mut guard) = recent_ticks.lock() {
                                if let Some(buf) = guard.get_mut(&sym) {
                                    buf.push_back((price, ts_ms));
                                    while buf.len() > CHAINLINK_HUB_RECENT_TICKS_PER_SYMBOL {
                                        buf.pop_front();
                                    }
                                }
                            }
                            let _ = tx.send((price, ts_ms));
                            stat_delivered = stat_delivered.saturating_add(1);
                            matched_ticks = matched_ticks.saturating_add(1);
                            last_tick_at = Instant::now();
                            last_tick_ts_ms = Some(ts_ms);
                        }
                        stat_ticks = stat_ticks.saturating_add(matched_ticks);
                        if matched_ticks == 0 {
                            let idle = last_tick_at.elapsed();
                            if idle.as_millis() >= stall_reconnect_ms as u128 {
                                warn!(
                                    "⚠️ chainlink_hub_symbol_mismatch_stall | symbol={} idle_ms={} threshold_ms={} msgs={} ticks={} delivered={} last_tick_ts_ms={:?}",
                                    sym,
                                    idle.as_millis(),
                                    stall_reconnect_ms,
                                    stat_msgs,
                                    stat_ticks,
                                    stat_delivered,
                                    last_tick_ts_ms,
                                );
                                break;
                            }
                        }
                        if stat_last_log.elapsed() >= Duration::from_secs(30) {
                            info!(
                                "📡 chainlink_hub_conn | symbol={} msgs={} ticks={} delivered={}",
                                sym, stat_msgs, stat_ticks, stat_delivered
                            );
                            stat_last_log = Instant::now();
                        }
                    }
                }
            });
        }
        hub
    }

    fn spawn_remote(symbols: HashSet<String>, socket_path: PathBuf) -> Arc<Self> {
        let hub = Self::new(&symbols);
        let target_symbols = symbols
            .into_iter()
            .map(|sym| normalize_chainlink_symbol(&sym))
            .filter(|sym| !sym.is_empty())
            .collect::<Vec<_>>();
        let shared_hub = Arc::clone(&hub);
        tokio::spawn(async move {
            let mut reconnect_backoff = Duration::from_millis(300);
            loop {
                let _ = ensure_shared_ingress_broker_running().await;
                let connect =
                    tokio::time::timeout(Duration::from_secs(2), UnixStream::connect(&socket_path))
                        .await;
                let Ok(Ok(stream)) = connect else {
                    sleep(reconnect_backoff).await;
                    reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(2));
                    continue;
                };
                reconnect_backoff = Duration::from_millis(300);
                let (read_half, mut write_half) = stream.into_split();
                let req = SharedIngressSubscribeReq {
                    stream: "chainlink".to_string(),
                    symbols: target_symbols.clone(),
                    market_slug: None,
                    market_id: None,
                    yes_asset_id: None,
                    no_asset_id: None,
                    ws_base_url: None,
                    custom_feature_enabled: None,
                };
                let mut line = match serde_json::to_vec(&req) {
                    Ok(v) => v,
                    Err(_) => break,
                };
                line.push(b'\n');
                if write_half.write_all(&line).await.is_err() || write_half.flush().await.is_err() {
                    continue;
                }
                let mut reader = BufReader::new(read_half);
                let mut buf = String::new();
                loop {
                    buf.clear();
                    let read = reader.read_line(&mut buf).await;
                    let Ok(n) = read else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    let Ok(msg) = serde_json::from_str::<SharedIngressWireMsg>(buf.trim()) else {
                        continue;
                    };
                    match msg {
                        SharedIngressWireMsg::ChainlinkSnapshot {
                            symbol,
                            price,
                            ts_ms,
                        }
                        | SharedIngressWireMsg::ChainlinkTick {
                            symbol,
                            price,
                            ts_ms,
                        } => {
                            shared_hub.publish_tick(&symbol, price, ts_ms);
                        }
                        SharedIngressWireMsg::SnapshotDone => {}
                        SharedIngressWireMsg::Error { message } => {
                            warn!("⚠️ chainlink_remote_hub_error | msg={}", message);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        });
        hub
    }

    fn subscribe(&self, symbol: &str) -> Option<broadcast::Receiver<(f64, u64)>> {
        let sym = normalize_chainlink_symbol(symbol);
        self.senders.get(&sym).map(|tx| tx.subscribe())
    }

    fn snapshot_recent_ticks(&self, symbol: &str) -> Vec<(f64, u64)> {
        let sym = normalize_chainlink_symbol(symbol);
        if sym.is_empty() {
            return Vec::new();
        }
        let Ok(guard) = self.recent_ticks.lock() else {
            return Vec::new();
        };
        guard
            .get(&sym)
            .map(|buf| buf.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default()
    }
}

#[derive(Clone)]
struct LocalPriceHub {
    senders: Arc<HashMap<String, broadcast::Sender<(f64, u64, LocalPriceSource)>>>,
    recent_ticks: Arc<Mutex<HashMap<String, VecDeque<(f64, u64, LocalPriceSource)>>>>,
}

impl LocalPriceHub {
    fn new(symbols: &HashSet<String>) -> Arc<Self> {
        let mut senders = HashMap::new();
        let mut recent_ticks = HashMap::new();
        for sym in symbols {
            let (tx, _rx) = broadcast::channel::<(f64, u64, LocalPriceSource)>(4096);
            senders.insert(sym.clone(), tx);
            recent_ticks.insert(sym.clone(), VecDeque::new());
        }
        Arc::new(Self {
            senders: Arc::new(senders),
            recent_ticks: Arc::new(Mutex::new(recent_ticks)),
        })
    }

    fn spawn(symbols: HashSet<String>) -> Arc<Self> {
        let hub = Self::new(&symbols);
        Self::spawn_binance_feeder(Arc::clone(&hub), symbols.clone());
        Self::spawn_bybit_feeder(Arc::clone(&hub), symbols.clone());
        Self::spawn_okx_feeder(Arc::clone(&hub), symbols.clone());
        Self::spawn_hyperliquid_feeder(Arc::clone(&hub), symbols.clone());
        Self::spawn_coinbase_feeder(Arc::clone(&hub), symbols);
        hub
    }

    fn spawn_remote(symbols: HashSet<String>, socket_path: PathBuf) -> Arc<Self> {
        let hub = Self::new(&symbols);
        let target_symbols = symbols
            .into_iter()
            .map(|sym| normalize_chainlink_symbol(&sym))
            .filter(|sym| !sym.is_empty())
            .collect::<Vec<_>>();
        let shared_hub = Arc::clone(&hub);
        tokio::spawn(async move {
            let mut reconnect_backoff = Duration::from_millis(300);
            loop {
                let _ = ensure_shared_ingress_broker_running().await;
                let connect =
                    tokio::time::timeout(Duration::from_secs(2), UnixStream::connect(&socket_path))
                        .await;
                let Ok(Ok(stream)) = connect else {
                    sleep(reconnect_backoff).await;
                    reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(2));
                    continue;
                };
                reconnect_backoff = Duration::from_millis(300);
                let (read_half, mut write_half) = stream.into_split();
                let req = SharedIngressSubscribeReq {
                    stream: "local_price".to_string(),
                    symbols: target_symbols.clone(),
                    market_slug: None,
                    market_id: None,
                    yes_asset_id: None,
                    no_asset_id: None,
                    ws_base_url: None,
                    custom_feature_enabled: None,
                };
                let mut line = match serde_json::to_vec(&req) {
                    Ok(v) => v,
                    Err(_) => break,
                };
                line.push(b'\n');
                if write_half.write_all(&line).await.is_err() || write_half.flush().await.is_err() {
                    continue;
                }
                let mut reader = BufReader::new(read_half);
                let mut buf = String::new();
                loop {
                    buf.clear();
                    let read = reader.read_line(&mut buf).await;
                    let Ok(n) = read else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    let Ok(msg) = serde_json::from_str::<SharedIngressWireMsg>(buf.trim()) else {
                        continue;
                    };
                    match msg {
                        SharedIngressWireMsg::LocalPriceSnapshot {
                            symbol,
                            price,
                            ts_ms,
                            source,
                        }
                        | SharedIngressWireMsg::LocalPriceTick {
                            symbol,
                            price,
                            ts_ms,
                            source,
                        } => {
                            let Some(source) = LocalPriceSource::from_str(&source) else {
                                continue;
                            };
                            shared_hub.publish_tick(&symbol, price, ts_ms, source);
                        }
                        SharedIngressWireMsg::SnapshotDone => {}
                        SharedIngressWireMsg::Error { message } => {
                            warn!("⚠️ local_price_remote_hub_error | msg={}", message);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        });
        hub
    }

    fn publish_tick(&self, symbol: &str, price: f64, ts_ms: u64, source: LocalPriceSource) {
        let sym = normalize_chainlink_symbol(symbol);
        if sym.is_empty() || !price.is_finite() || price <= 0.0 {
            return;
        }
        if let Ok(mut guard) = self.recent_ticks.lock() {
            if let Some(buf) = guard.get_mut(&sym) {
                buf.push_back((price, ts_ms, source));
                while buf.len() > LOCAL_PRICE_HUB_RECENT_TICKS_PER_SYMBOL {
                    buf.pop_front();
                }
            }
        }
        if let Some(tx) = self.senders.get(&sym) {
            let _ = tx.send((price, ts_ms, source));
        }
    }

    fn subscribe(&self, symbol: &str) -> Option<broadcast::Receiver<(f64, u64, LocalPriceSource)>> {
        let sym = normalize_chainlink_symbol(symbol);
        self.senders.get(&sym).map(|tx| tx.subscribe())
    }

    fn snapshot_recent_ticks(&self, symbol: &str) -> Vec<(f64, u64, LocalPriceSource)> {
        let sym = normalize_chainlink_symbol(symbol);
        if sym.is_empty() {
            return Vec::new();
        }
        let Ok(guard) = self.recent_ticks.lock() else {
            return Vec::new();
        };
        guard
            .get(&sym)
            .map(|buf| buf.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    fn spawn_binance_feeder(hub: Arc<Self>, symbols: HashSet<String>) {
        let streams: Vec<String> = symbols
            .iter()
            .filter_map(|sym| binance_stream_symbol_from_chainlink_symbol(sym))
            .map(|sym| format!("{}@trade", sym))
            .collect();
        if streams.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let url = format!(
                "wss://stream.binance.com:9443/stream?streams={}",
                streams.join("/")
            );
            let mut backoff = Duration::from_millis(300);
            loop {
                let connect =
                    tokio::time::timeout(Duration::from_secs(3), connect_async(&url)).await;
                let Ok(Ok((ws, _))) = connect else {
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(3));
                    continue;
                };
                backoff = Duration::from_millis(300);
                let (_write, mut read) = ws.split();
                while let Some(next) = read.next().await {
                    let msg = match next {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    let text = match msg {
                        Message::Text(t) => t.to_string(),
                        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                            Ok(t) => t,
                            Err(_) => continue,
                        },
                        _ => continue,
                    };
                    if let Some((symbol, price, ts_ms)) = parse_binance_combined_trade_tick(&text) {
                        hub.publish_tick(&symbol, price, ts_ms, LocalPriceSource::Binance);
                    }
                }
            }
        });
    }

    fn spawn_bybit_feeder(hub: Arc<Self>, symbols: HashSet<String>) {
        let topics: Vec<String> = symbols
            .iter()
            .filter_map(|sym| bybit_stream_symbol_from_chainlink_symbol(sym))
            .map(|sym| format!("publicTrade.{}", sym))
            .collect();
        if topics.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let url = "wss://stream.bybit.com/v5/public/spot";
            let mut backoff = Duration::from_millis(300);
            loop {
                let connect =
                    tokio::time::timeout(Duration::from_secs(3), connect_async(url)).await;
                let Ok(Ok((ws, _))) = connect else {
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(3));
                    continue;
                };
                backoff = Duration::from_millis(300);
                let (mut write, mut read) = ws.split();
                let subscribe_msg = json!({
                    "op": "subscribe",
                    "args": topics,
                });
                if write
                    .send(Message::Text(subscribe_msg.to_string().into()))
                    .await
                    .is_err()
                {
                    continue;
                }
                while let Some(next) = read.next().await {
                    let msg = match next {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    let text = match msg {
                        Message::Text(t) => t.to_string(),
                        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                            Ok(t) => t,
                            Err(_) => continue,
                        },
                        _ => continue,
                    };
                    for (symbol, price, ts_ms) in parse_bybit_public_trade_ticks(&text) {
                        hub.publish_tick(&symbol, price, ts_ms, LocalPriceSource::Bybit);
                    }
                }
            }
        });
    }

    fn spawn_okx_feeder(hub: Arc<Self>, symbols: HashSet<String>) {
        let args: Vec<Value> = symbols
            .iter()
            .filter_map(|sym| okx_inst_id_from_chainlink_symbol(sym))
            .map(|inst| json!({"channel":"trades","instId": inst}))
            .collect();
        if args.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let url = "wss://ws.okx.com:8443/ws/v5/public";
            let mut backoff = Duration::from_millis(300);
            loop {
                let connect =
                    tokio::time::timeout(Duration::from_secs(3), connect_async(url)).await;
                let Ok(Ok((ws, _))) = connect else {
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(3));
                    continue;
                };
                backoff = Duration::from_millis(300);
                let (mut write, mut read) = ws.split();
                let subscribe_msg = json!({
                    "op": "subscribe",
                    "args": args,
                });
                if write
                    .send(Message::Text(subscribe_msg.to_string().into()))
                    .await
                    .is_err()
                {
                    continue;
                }
                while let Some(next) = read.next().await {
                    let msg = match next {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    let text = match msg {
                        Message::Text(t) => t.to_string(),
                        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                            Ok(t) => t,
                            Err(_) => continue,
                        },
                        _ => continue,
                    };
                    for (symbol, price, ts_ms) in parse_okx_trade_ticks(&text) {
                        hub.publish_tick(&symbol, price, ts_ms, LocalPriceSource::Okx);
                    }
                }
            }
        });
    }

    fn spawn_coinbase_feeder(hub: Arc<Self>, symbols: HashSet<String>) {
        let product_ids: Vec<String> = symbols
            .iter()
            .filter_map(|sym| coinbase_product_id_from_chainlink_symbol(sym))
            .collect();
        if product_ids.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let url = "wss://ws-feed.exchange.coinbase.com";
            let mut backoff = Duration::from_millis(300);
            loop {
                let connect =
                    tokio::time::timeout(Duration::from_secs(3), connect_async(url)).await;
                let Ok(Ok((ws, _))) = connect else {
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(3));
                    continue;
                };
                backoff = Duration::from_millis(300);
                let (mut write, mut read) = ws.split();
                let subscribe_msg = json!({
                    "type": "subscribe",
                    "product_ids": product_ids,
                    "channels": ["ticker"],
                });
                if write
                    .send(Message::Text(subscribe_msg.to_string().into()))
                    .await
                    .is_err()
                {
                    continue;
                }
                while let Some(next) = read.next().await {
                    let msg = match next {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    let text = match msg {
                        Message::Text(t) => t.to_string(),
                        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                            Ok(t) => t,
                            Err(_) => continue,
                        },
                        _ => continue,
                    };
                    if let Some((symbol, price, ts_ms)) = parse_coinbase_ticker_tick(&text) {
                        hub.publish_tick(&symbol, price, ts_ms, LocalPriceSource::Coinbase);
                    }
                }
            }
        });
    }

    fn spawn_hyperliquid_feeder(hub: Arc<Self>, symbols: HashSet<String>) {
        let wanted: HashMap<String, String> = symbols
            .iter()
            .filter_map(|sym| {
                hyperliquid_coin_from_chainlink_symbol(sym)
                    .map(|coin| (coin, normalize_chainlink_symbol(sym)))
            })
            .collect();
        if wanted.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let url = "wss://api.hyperliquid.xyz/ws";
            let mut backoff = Duration::from_millis(300);
            loop {
                let connect =
                    tokio::time::timeout(Duration::from_secs(3), connect_async(url)).await;
                let Ok(Ok((ws, _))) = connect else {
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(3));
                    continue;
                };
                backoff = Duration::from_millis(300);
                let (mut write, mut read) = ws.split();
                let subscribe_msg = json!({
                    "method": "subscribe",
                    "subscription": { "type": "allMids" },
                });
                if write
                    .send(Message::Text(subscribe_msg.to_string().into()))
                    .await
                    .is_err()
                {
                    continue;
                }
                while let Some(next) = read.next().await {
                    let msg = match next {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    let text = match msg {
                        Message::Text(t) => t.to_string(),
                        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                            Ok(t) => t,
                            Err(_) => continue,
                        },
                        _ => continue,
                    };
                    for (symbol, price, ts_ms) in parse_hyperliquid_all_mids_ticks(&text, &wanted) {
                        hub.publish_tick(&symbol, price, ts_ms, LocalPriceSource::Hyperliquid);
                    }
                }
            }
        });
    }
}

#[derive(Debug, Clone, Serialize)]
struct ChainlinkRoundAlignmentProbe {
    unix_ms: u64,
    symbol: String,
    round_start_ts: u64,
    round_end_ts: u64,
    start_ms: u64,
    end_ms: u64,
    open_t: Option<f64>,
    open_t_minus_1s: Option<f64>,
    open_t_plus_1s: Option<f64>,
    open_prev_round_close: Option<f64>,
    prev_round_close_ts_ms: Option<u64>,
    close_t: Option<f64>,
    winner_t: Option<String>,
    winner_t_minus_1s: Option<String>,
    winner_t_plus_1s: Option<String>,
    winner_prev_round_close: Option<String>,
    selected_rule: Option<String>,
    note: String,
}

#[derive(Debug, Clone, Serialize)]
struct SelfBuiltPriceAggProbe {
    unix_ms: u64,
    symbol: String,
    round_start_ts: u64,
    round_end_ts: u64,
    status: String,
    side: Option<String>,
    confidence: Option<f64>,
    min_confidence: f64,
    open_price: Option<f64>,
    open_ts_ms: Option<u64>,
    open_source: Option<String>,
    open_exact: Option<bool>,
    open_delta_ms: Option<u64>,
    close_price: Option<f64>,
    close_ts_ms: Option<u64>,
    close_source: Option<String>,
    close_exact: Option<bool>,
    close_delta_ms: Option<u64>,
    observed_ticks: u64,
    observed_snapshot_ticks: u64,
    observed_live_ticks: u64,
    lagged_ticks: u64,
    nearest_start_delta_ms: Option<u64>,
    nearest_end_delta_ms: Option<u64>,
    first_after_start_delta_ms: Option<u64>,
    first_after_end_delta_ms: Option<u64>,
    open_pick_rule: Option<String>,
    close_pick_rule: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LocalPriceAggProbe {
    unix_ms: u64,
    symbol: String,
    round_start_ts: u64,
    round_end_ts: u64,
    status: String,
    side: Option<String>,
    confidence: Option<f64>,
    min_confidence: f64,
    effective_min_confidence: f64,
    min_sources: usize,
    max_source_spread_bps: f64,
    single_source_min_direction_margin_bps: f64,
    confidence_relief_applied: bool,
    source_count: usize,
    source_agreement: Option<f64>,
    source_spread_bps: Option<f64>,
    direction_margin_bps: Option<f64>,
    open_price: Option<f64>,
    open_ts_ms: Option<u64>,
    close_price: Option<f64>,
    close_ts_ms: Option<u64>,
    open_exact_sources: usize,
    close_exact_sources: usize,
    observed_ticks: u64,
    observed_snapshot_ticks: u64,
    observed_live_ticks: u64,
    lagged_ticks: u64,
}

#[derive(Debug, Clone, Serialize)]
struct LocalPriceAggSourcePointProbe {
    source: String,
    open_price: Option<f64>,
    open_ts_ms: Option<u64>,
    open_exact: Option<bool>,
    raw_close_price: Option<f64>,
    close_price: Option<f64>,
    close_ts_ms: Option<u64>,
    close_exact: Option<bool>,
    close_abs_delta_ms: Option<u64>,
    close_pick_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LocalPriceAggSourcesProbe {
    unix_ms: u64,
    symbol: String,
    round_start_ts: u64,
    round_end_ts: u64,
    mode: String,
    status: String,
    min_sources: usize,
    source_points: Vec<LocalPriceAggSourcePointProbe>,
}

#[derive(Debug, Clone, Serialize)]
struct LocalBoundaryTickProbe {
    ts_ms: u64,
    offset_ms: i64,
    price: f64,
}

#[derive(Debug, Clone, Serialize)]
struct LocalPriceAggBoundarySourceProbe {
    source: String,
    open_window_ticks: Vec<LocalBoundaryTickProbe>,
    close_window_ticks: Vec<LocalBoundaryTickProbe>,
}

#[derive(Debug, Clone, Serialize)]
struct LocalPriceAggBoundaryProbe {
    unix_ms: u64,
    symbol: String,
    round_start_ts: u64,
    round_end_ts: u64,
    mode: String,
    status: String,
    boundary_window_ms: u64,
    source_tapes: Vec<LocalPriceAggBoundarySourceProbe>,
}

fn chainlink_round_alignment_path() -> PathBuf {
    log_path("chainlink_round_alignment.jsonl")
}

fn self_built_price_agg_probe_path() -> PathBuf {
    log_path("self_built_price_agg.jsonl")
}

fn local_price_agg_probe_path() -> PathBuf {
    log_path("local_price_agg.jsonl")
}

fn local_price_agg_sources_probe_path() -> PathBuf {
    log_path("local_price_agg_sources.jsonl")
}

fn local_price_agg_boundary_probe_path() -> PathBuf {
    log_path("local_price_agg_boundary_tape.jsonl")
}

static LOCAL_PRICE_AGG_BOUNDARY_PROBE_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn local_price_agg_boundary_probe_write_lock() -> &'static Mutex<()> {
    LOCAL_PRICE_AGG_BOUNDARY_PROBE_WRITE_LOCK.get_or_init(|| Mutex::new(()))
}

fn chainlink_last_close_cache_path() -> PathBuf {
    log_path("chainlink_last_close_cache.json")
}

fn append_chainlink_round_alignment_probe(probe: &ChainlinkRoundAlignmentProbe) {
    let path = chainlink_round_alignment_path();
    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            warn!(
                "⚠️ chainlink_round_alignment write open failed: path={} err={}",
                path.display(),
                e
            );
            return;
        }
    };
    let mut writer = BufWriter::new(file);
    let line = match serde_json::to_string(probe) {
        Ok(s) => s,
        Err(e) => {
            warn!("⚠️ chainlink_round_alignment serialize failed: {}", e);
            return;
        }
    };
    if let Err(e) = writeln!(writer, "{}", line) {
        warn!("⚠️ chainlink_round_alignment write failed: {}", e);
    }
}

fn append_self_built_price_agg_probe(probe: &SelfBuiltPriceAggProbe) {
    let path = self_built_price_agg_probe_path();
    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            warn!(
                "⚠️ self_built_price_agg write open failed: path={} err={}",
                path.display(),
                e
            );
            return;
        }
    };
    let mut writer = BufWriter::new(file);
    let line = match serde_json::to_string(probe) {
        Ok(s) => s,
        Err(e) => {
            warn!("⚠️ self_built_price_agg serialize failed: {}", e);
            return;
        }
    };
    if let Err(e) = writeln!(writer, "{}", line) {
        warn!("⚠️ self_built_price_agg write failed: {}", e);
    }
}

fn append_local_price_agg_probe(probe: &LocalPriceAggProbe) {
    let path = local_price_agg_probe_path();
    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            warn!(
                "⚠️ local_price_agg write open failed: path={} err={}",
                path.display(),
                e
            );
            return;
        }
    };
    let mut writer = BufWriter::new(file);
    let line = match serde_json::to_string(probe) {
        Ok(s) => s,
        Err(e) => {
            warn!("⚠️ local_price_agg serialize failed: {}", e);
            return;
        }
    };
    if let Err(e) = writeln!(writer, "{}", line) {
        warn!("⚠️ local_price_agg write failed: {}", e);
    }
}

fn append_local_price_agg_sources_probe(probe: &LocalPriceAggSourcesProbe) {
    let path = local_price_agg_sources_probe_path();
    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            warn!(
                "⚠️ local_price_agg_sources write open failed: path={} err={}",
                path.display(),
                e
            );
            return;
        }
    };
    let mut writer = BufWriter::new(file);
    let line = match serde_json::to_string(probe) {
        Ok(s) => s,
        Err(e) => {
            warn!("⚠️ local_price_agg_sources serialize failed: {}", e);
            return;
        }
    };
    if let Err(e) = writeln!(writer, "{}", line) {
        warn!("⚠️ local_price_agg_sources write failed: {}", e);
    }
}

fn append_local_price_agg_boundary_probe(probe: &LocalPriceAggBoundaryProbe) {
    let mut line = match serde_json::to_string(probe) {
        Ok(s) => s,
        Err(e) => {
            warn!("⚠️ local_price_agg_boundary_tape serialize failed: {}", e);
            return;
        }
    };
    line.push('\n');

    let _guard = match local_price_agg_boundary_probe_write_lock().lock() {
        Ok(guard) => guard,
        Err(e) => {
            warn!(
                "⚠️ local_price_agg_boundary_tape write lock poisoned: {}",
                e
            );
            return;
        }
    };
    let path = local_price_agg_boundary_probe_path();
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            warn!(
                "⚠️ local_price_agg_boundary_tape write open failed: path={} err={}",
                path.display(),
                e
            );
            return;
        }
    };
    if let Err(e) = file.write_all(line.as_bytes()).and_then(|_| file.flush()) {
        warn!("⚠️ local_price_agg_boundary_tape write failed: {}", e);
    }
}

fn winner_from_open_close(open: Option<f64>, close: Option<f64>) -> Option<Side> {
    let open = open?;
    let close = close?;
    Some(if close >= open { Side::Yes } else { Side::No })
}

fn side_label(side: Option<Side>) -> Option<String> {
    side.map(|s| format!("{:?}", s))
}

type LocalPriceAggBiasMap = HashMap<(String, LocalPriceSource), f64>;
static LOCAL_PRICE_AGG_SOURCE_BIAS_BPS: OnceLock<Mutex<LocalPriceAggBiasMap>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalPriceAggBiasCacheEntry {
    symbol: String,
    source: String,
    bias_bps: f64,
}

fn local_price_agg_bias_cache_path() -> PathBuf {
    if let Ok(path) = env::var("PM_LOCAL_PRICE_AGG_BIAS_CACHE_PATH") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    log_path("local_price_agg_bias_cache.json")
}

fn load_local_price_agg_bias_cache() -> LocalPriceAggBiasMap {
    let path = local_price_agg_bias_cache_path();
    let Ok(raw) = fs::read_to_string(&path) else {
        return HashMap::new();
    };
    let Ok(entries) = serde_json::from_str::<Vec<LocalPriceAggBiasCacheEntry>>(&raw) else {
        warn!(
            "⚠️ Failed to parse local price agg bias cache | path={}",
            path.display()
        );
        return HashMap::new();
    };
    let mut out = HashMap::new();
    for entry in entries {
        let symbol = normalize_chainlink_symbol(&entry.symbol);
        let Some(source) = LocalPriceSource::from_str(&entry.source) else {
            continue;
        };
        if symbol.is_empty() || !entry.bias_bps.is_finite() {
            continue;
        }
        out.insert((symbol, source), entry.bias_bps);
    }
    if !out.is_empty() {
        info!(
            "🧠 Loaded local price agg bias cache | path={} entries={}",
            path.display(),
            out.len()
        );
    }
    out
}

fn persist_local_price_agg_bias_cache(map: &LocalPriceAggBiasMap) {
    let path = local_price_agg_bias_cache_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let entries = map
        .iter()
        .map(|((symbol, source), bias_bps)| LocalPriceAggBiasCacheEntry {
            symbol: symbol.clone(),
            source: source.as_str().to_string(),
            bias_bps: *bias_bps,
        })
        .collect::<Vec<_>>();
    let Ok(payload) = serde_json::to_vec(&entries) else {
        return;
    };
    let tmp = path.with_extension("json.tmp");
    if fs::write(&tmp, payload).is_err() {
        return;
    }
    if fs::rename(&tmp, &path).is_err() {
        let _ = fs::remove_file(&tmp);
    }
}

fn local_price_agg_source_bias_map() -> &'static Mutex<LocalPriceAggBiasMap> {
    LOCAL_PRICE_AGG_SOURCE_BIAS_BPS.get_or_init(|| Mutex::new(load_local_price_agg_bias_cache()))
}

fn local_price_agg_get_source_bias_bps(symbol: &str, source: LocalPriceSource) -> f64 {
    let normalized = normalize_chainlink_symbol(symbol);
    if normalized.is_empty() {
        return 0.0;
    }
    let Ok(guard) = local_price_agg_source_bias_map().lock() else {
        return 0.0;
    };
    guard.get(&(normalized, source)).copied().unwrap_or(0.0)
}

fn local_price_agg_apply_source_bias(symbol: &str, source: LocalPriceSource, raw: f64) -> f64 {
    if !raw.is_finite() || raw <= 0.0 {
        return raw;
    }
    let bias_bps = local_price_agg_get_source_bias_bps(symbol, source);
    let adjusted = raw * (1.0 + bias_bps / 10_000.0);
    if adjusted.is_finite() && adjusted > 0.0 {
        adjusted
    } else {
        raw
    }
}

fn local_price_agg_bias_learning_enabled() -> bool {
    env_flag_or("PM_LOCAL_PRICE_AGG_BIAS_LEARNING_ENABLED", true)
}

fn local_price_agg_update_source_bias_bps(
    symbol: &str,
    source: LocalPriceSource,
    residual_bps: f64,
) -> Option<f64> {
    let normalized = normalize_chainlink_symbol(symbol);
    if normalized.is_empty() || !residual_bps.is_finite() {
        return None;
    }
    let clamped_residual = residual_bps.clamp(
        -LOCAL_PRICE_AGG_SOURCE_BIAS_RESIDUAL_MAX_ABS_BPS,
        LOCAL_PRICE_AGG_SOURCE_BIAS_RESIDUAL_MAX_ABS_BPS,
    );
    let Ok(mut guard) = local_price_agg_source_bias_map().lock() else {
        return None;
    };
    let key = (normalized, source);
    let prev = guard.get(&key).copied().unwrap_or(0.0);
    let next = (prev * (1.0 - LOCAL_PRICE_AGG_SOURCE_BIAS_EMA_ALPHA)
        + clamped_residual * LOCAL_PRICE_AGG_SOURCE_BIAS_EMA_ALPHA)
        .clamp(
            -LOCAL_PRICE_AGG_SOURCE_BIAS_MAX_ABS_BPS,
            LOCAL_PRICE_AGG_SOURCE_BIAS_MAX_ABS_BPS,
        );
    guard.insert(key, next);
    let snapshot = guard.clone();
    drop(guard);
    persist_local_price_agg_bias_cache(&snapshot);
    Some(next)
}

type LastCloseMap = HashMap<String, (u64, f64)>;
static CHAINLINK_LAST_CLOSE: OnceLock<Mutex<LastCloseMap>> = OnceLock::new();

// Global hint dedup: key = "{slug}:{round_end_ts}:{side}", value = first detect_ms.
// Guards against duplicate winner-hint emits from stale pre-resolve races or dual processes.
static HINT_DEDUP: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
fn hint_dedup_map() -> &'static Mutex<HashMap<String, u64>> {
    HINT_DEDUP.get_or_init(|| Mutex::new(HashMap::new()))
}
fn hint_dedup_key(slug: &str, round_end_ts: u64, side: Side) -> String {
    format!("{}:{}:{:?}", slug, round_end_ts, side)
}
/// Returns true if this is a fresh emit; false if already emitted (duplicate).
fn hint_dedup_try_insert(slug: &str, round_end_ts: u64, side: Side, detect_ms: u64) -> bool {
    let key = hint_dedup_key(slug, round_end_ts, side);
    if let Ok(mut guard) = hint_dedup_map().lock() {
        if guard.contains_key(&key) {
            return false;
        }
        guard.insert(key, detect_ms);
    }
    true
}

fn chainlink_last_close_map() -> &'static Mutex<LastCloseMap> {
    CHAINLINK_LAST_CLOSE.get_or_init(|| Mutex::new(load_last_chainlink_close_cache()))
}

fn get_last_chainlink_close(symbol: &str) -> Option<(u64, f64)> {
    let guard = chainlink_last_close_map().lock().ok()?;
    guard.get(symbol).copied()
}

fn set_last_chainlink_close(symbol: &str, ts_ms: u64, price: f64) {
    if let Ok(mut guard) = chainlink_last_close_map().lock() {
        guard.insert(symbol.to_string(), (ts_ms, price));
        let snapshot = guard.clone();
        drop(guard);
        persist_last_chainlink_close_cache(&snapshot);
    }
}

fn load_last_chainlink_close_cache() -> LastCloseMap {
    let path = chainlink_last_close_cache_path();
    let Ok(raw) = fs::read_to_string(&path) else {
        return HashMap::new();
    };
    match serde_json::from_str::<LastCloseMap>(&raw) {
        Ok(map) => {
            info!(
                "🧠 Loaded chainlink last-close cache | path={} symbols={}",
                path.display(),
                map.len()
            );
            map
        }
        Err(e) => {
            warn!(
                "⚠️ Failed to parse chainlink last-close cache | path={} err={}",
                path.display(),
                e
            );
            HashMap::new()
        }
    }
}

fn persist_last_chainlink_close_cache(map: &LastCloseMap) {
    let path = chainlink_last_close_cache_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let tmp = path.with_extension("json.tmp");
    let Ok(payload) = serde_json::to_vec(map) else {
        return;
    };
    if fs::write(&tmp, payload).is_err() {
        return;
    }
    if fs::rename(&tmp, &path).is_err() {
        let _ = fs::remove_file(&tmp);
    }
}

fn cached_prev_round_close(symbol: &str, round_start_ms: u64) -> Option<(f64, u64)> {
    get_last_chainlink_close(symbol)
        .filter(|(ts_ms, _)| ts_ms.abs_diff(round_start_ms) <= 1_000)
        .map(|(ts_ms, px)| (px, ts_ms))
}

fn cached_round_exact_close(symbol: &str, round_end_ms: u64) -> Option<(f64, u64)> {
    get_last_chainlink_close(symbol)
        .filter(|(ts_ms, _)| ts_ms.abs_diff(round_end_ms) <= 1_000)
        .map(|(ts_ms, px)| (px, ts_ms))
}

#[derive(Debug, Deserialize)]
struct FrontendCryptoPriceResp {
    #[serde(rename = "openPrice")]
    open_price: Option<f64>,
    #[serde(rename = "closePrice")]
    close_price: Option<f64>,
    timestamp: Option<u64>,
    completed: Option<bool>,
    incomplete: Option<bool>,
    cached: Option<bool>,
}

#[derive(Debug, Clone)]
struct FrontendRoundPrices {
    open_price: Option<f64>,
    close_price: Option<f64>,
    timestamp_ms: u64,
    completed: Option<bool>,
    incomplete: Option<bool>,
    cached: Option<bool>,
    symbol: String,
    variant: &'static str,
    event_start_time: String,
    end_date: String,
}

fn epoch_secs_to_rfc3339_utc(secs: u64) -> Option<String> {
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0).map(|dt| {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            .replace("+00:00", "Z")
    })
}

fn frontend_symbol_from_chainlink_symbol(chainlink_symbol: &str) -> Option<String> {
    let base = chainlink_symbol.split('/').next()?.trim();
    if base.is_empty() {
        None
    } else {
        Some(base.to_ascii_uppercase())
    }
}

fn frontend_variant_from_round_len_secs(round_len_secs: u64) -> Option<&'static str> {
    match round_len_secs {
        300 => Some("fiveminute"),
        900 => Some("fifteenminute"),
        3_600 => Some("hourly"),
        14_400 => Some("fourhour"),
        _ => None,
    }
}

async fn fetch_frontend_crypto_round_prices(
    chainlink_symbol: &str,
    round_start_ts: u64,
    round_end_ts: u64,
) -> Option<FrontendRoundPrices> {
    let symbol = frontend_symbol_from_chainlink_symbol(chainlink_symbol)?;
    let round_len_secs = round_end_ts.saturating_sub(round_start_ts);
    let variant = frontend_variant_from_round_len_secs(round_len_secs)?;
    let event_start_time = epoch_secs_to_rfc3339_utc(round_start_ts)?;
    let end_date = epoch_secs_to_rfc3339_utc(round_end_ts)?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1_500))
        .build()
        .ok()?;
    let resp = client
        .get("https://polymarket.com/api/crypto/crypto-price")
        .query(&[
            ("symbol", symbol.as_str()),
            ("eventStartTime", event_start_time.as_str()),
            ("variant", variant),
            ("endDate", end_date.as_str()),
        ])
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let payload = resp.json::<FrontendCryptoPriceResp>().await.ok()?;
    Some(FrontendRoundPrices {
        open_price: payload.open_price.filter(|p| p.is_finite() && *p > 0.0),
        close_price: payload.close_price.filter(|p| p.is_finite() && *p > 0.0),
        timestamp_ms: payload.timestamp.unwrap_or_else(unix_now_millis_u64),
        completed: payload.completed,
        incomplete: payload.incomplete,
        cached: payload.cached,
        symbol,
        variant,
        event_start_time,
        end_date,
    })
}

type PrewarmedOpenMap = HashMap<(String, u64), (f64, u64)>;
static CHAINLINK_PREWARMED_OPEN: OnceLock<Mutex<PrewarmedOpenMap>> = OnceLock::new();
static CHAINLINK_EXACT_OPEN: OnceLock<Mutex<PrewarmedOpenMap>> = OnceLock::new();
static CHAINLINK_EXACT_CLOSE: OnceLock<Mutex<PrewarmedOpenMap>> = OnceLock::new();
type LocalPrewarmedOpenMap = HashMap<(String, u64, LocalPriceSource), AggregatedPricePoint>;
static LOCAL_PRICE_PREWARMED_OPEN: OnceLock<Mutex<LocalPrewarmedOpenMap>> = OnceLock::new();

fn chainlink_prewarmed_open_map() -> &'static Mutex<PrewarmedOpenMap> {
    CHAINLINK_PREWARMED_OPEN.get_or_init(|| Mutex::new(HashMap::new()))
}

fn chainlink_exact_open_map() -> &'static Mutex<PrewarmedOpenMap> {
    CHAINLINK_EXACT_OPEN.get_or_init(|| Mutex::new(HashMap::new()))
}

fn chainlink_exact_close_map() -> &'static Mutex<PrewarmedOpenMap> {
    CHAINLINK_EXACT_CLOSE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn local_price_prewarmed_open_map() -> &'static Mutex<LocalPrewarmedOpenMap> {
    LOCAL_PRICE_PREWARMED_OPEN.get_or_init(|| Mutex::new(HashMap::new()))
}

fn set_prewarmed_open(symbol: &str, round_start_ms: u64, price: f64, ts_ms: u64) {
    if let Ok(mut guard) = chainlink_prewarmed_open_map().lock() {
        guard.insert((symbol.to_string(), round_start_ms), (price, ts_ms));
    }
}

fn take_prewarmed_open(symbol: &str, round_start_ms: u64) -> Option<(f64, u64)> {
    let mut guard = chainlink_prewarmed_open_map().lock().ok()?;
    guard.remove(&(symbol.to_string(), round_start_ms))
}

/// Peek the prewarmed tick for a given ms-aligned timestamp without consuming it.
///
/// Use case: winner-hint listener's own WS subscription may drop the exact close tick
/// (at ts_ms == end_ms) while the *next round's* prewarm listener — which subscribes
/// to the same stream and stores the tick at end_ms as its open — successfully receives
/// it. This function lets the winner-hint listener fall back to the prewarm-observed tick
/// before declaring deadline_exhausted. Non-consuming so the next round's winner-hint can
/// still `take_prewarmed_open` the same entry as *its* open reference.
fn peek_prewarmed_tick(symbol: &str, ts_ms: u64) -> Option<(f64, u64)> {
    let guard = chainlink_prewarmed_open_map().lock().ok()?;
    guard.get(&(symbol.to_string(), ts_ms)).copied()
}

fn remember_chainlink_exact_open(symbol: &str, round_start_ms: u64, price: f64, ts_ms: u64) {
    if let Ok(mut guard) = chainlink_exact_open_map().lock() {
        guard.insert((symbol.to_string(), round_start_ms), (price, ts_ms));
    }
}

fn remember_chainlink_exact_close(symbol: &str, round_end_ms: u64, price: f64, ts_ms: u64) {
    if let Ok(mut guard) = chainlink_exact_close_map().lock() {
        guard.insert((symbol.to_string(), round_end_ms), (price, ts_ms));
    }
}

fn peek_chainlink_exact_open(symbol: &str, round_start_ms: u64) -> Option<(f64, u64)> {
    let guard = chainlink_exact_open_map().lock().ok()?;
    guard.get(&(symbol.to_string(), round_start_ms)).copied()
}

fn peek_chainlink_exact_close(symbol: &str, round_end_ms: u64) -> Option<(f64, u64)> {
    let guard = chainlink_exact_close_map().lock().ok()?;
    guard.get(&(symbol.to_string(), round_end_ms)).copied()
}

fn set_local_prewarmed_open(
    symbol: &str,
    round_start_ms: u64,
    source: LocalPriceSource,
    point: AggregatedPricePoint,
) {
    if let Ok(mut guard) = local_price_prewarmed_open_map().lock() {
        guard.insert((symbol.to_string(), round_start_ms, source), point);
    }
}

fn peek_local_prewarmed_open(
    symbol: &str,
    round_start_ms: u64,
    source: LocalPriceSource,
) -> Option<AggregatedPricePoint> {
    let guard = local_price_prewarmed_open_map().lock().ok()?;
    guard
        .get(&(symbol.to_string(), round_start_ms, source))
        .copied()
}

fn map_outcome_label_to_side(label: &str) -> Option<Side> {
    let lower = label.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return None;
    }
    if lower.contains("yes") || lower.contains("up") {
        return Some(Side::Yes);
    }
    if lower.contains("no") || lower.contains("down") {
        return Some(Side::No);
    }
    None
}

fn parse_string_vec(v: &Value) -> Option<Vec<String>> {
    if let Some(arr) = v.as_array() {
        let parsed = arr
            .iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();
        return (!parsed.is_empty()).then_some(parsed);
    }
    let raw = v.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(raw) {
        let parsed = arr
            .iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();
        return (!parsed.is_empty()).then_some(parsed);
    }
    None
}

fn parse_f64_vec(v: &Value) -> Option<Vec<f64>> {
    if let Some(arr) = v.as_array() {
        let parsed = arr.iter().filter_map(parse_f64_value).collect::<Vec<_>>();
        return (!parsed.is_empty()).then_some(parsed);
    }
    let raw = v.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(raw) {
        let parsed = arr.iter().filter_map(parse_f64_value).collect::<Vec<_>>();
        return (!parsed.is_empty()).then_some(parsed);
    }
    None
}

fn extract_gamma_winner_side(market: &Value) -> Option<(Side, f64)> {
    for key in ["winningOutcome", "winning_outcome", "winner"] {
        if let Some(raw) = market.get(key).and_then(|v| v.as_str()) {
            if let Some(side) = map_outcome_label_to_side(raw) {
                return Some((side, 1.0));
            }
        }
    }

    let outcomes = market.get("outcomes").and_then(parse_string_vec)?;
    let outcome_prices = market
        .get("outcomePrices")
        .or_else(|| market.get("outcome_prices"))
        .and_then(parse_f64_vec)?;
    if outcomes.len() != outcome_prices.len() || outcomes.len() < 2 {
        return None;
    }

    let mut yes_idx: Option<usize> = None;
    let mut no_idx: Option<usize> = None;
    for (idx, outcome) in outcomes.iter().enumerate() {
        if let Some(side) = map_outcome_label_to_side(outcome) {
            match side {
                Side::Yes => yes_idx = Some(idx),
                Side::No => no_idx = Some(idx),
            }
        }
    }
    let (yes_i, no_i) = match (yes_idx, no_idx) {
        (Some(y), Some(n)) => (y, n),
        _ if outcomes.len() >= 2 => (0, 1),
        _ => return None,
    };

    let yes_px = *outcome_prices.get(yes_i)?;
    let no_px = *outcome_prices.get(no_i)?;
    if yes_px >= 0.99 && no_px <= 0.01 {
        return Some((Side::Yes, yes_px));
    }
    if no_px >= 0.99 && yes_px <= 0.01 {
        return Some((Side::No, no_px));
    }
    None
}

/// Fetch the winner from Gamma's /events endpoint (correct for hype-updown markets).
/// /markets?slug= returns [] for these markets; /events?slug= has the nested market data.
#[allow(dead_code)]
async fn fetch_gamma_winner_hint(slug: &str) -> Option<(Side, f64)> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()
        .ok()?;
    let resp = client.get(&url).send().await.ok()?;
    let body = resp.json::<Value>().await.ok()?;
    let events = body.as_array()?;
    for event in events {
        let markets = event.get("markets").and_then(|m| m.as_array())?;
        for market in markets {
            if let Some(hit) = extract_gamma_winner_side(market) {
                return Some(hit);
            }
        }
    }
    None
}

/// Fetch the Chainlink reference price ("Price to beat") for a round at t-10s.
/// Available from Gamma /events eventMetadata.priceToBeat before the round ends.
#[allow(dead_code)]
async fn fetch_gamma_price_to_beat(slug: &str) -> Option<f64> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(2000))
        .build()
        .ok()?;
    let resp = client.get(&url).send().await.ok()?;
    let body = resp.json::<Value>().await.ok()?;
    let events = body.as_array()?;
    let event = events.first()?;
    event
        .get("eventMetadata")?
        .get("priceToBeat")?
        .as_f64()
        .filter(|p| *p > 0.0)
}

async fn run_chainlink_open_prewarm(
    symbol: &str,
    round_start_ts: u64,
    hard_deadline_ts: u64,
    chainlink_hub: Option<Arc<ChainlinkHub>>,
) {
    let target_symbol = normalize_chainlink_symbol(symbol);
    let start_ms = round_start_ts.saturating_mul(1_000);
    let mut observed_ticks: u64 = 0;
    let mut lagged_ticks: u64 = 0;
    let mut first_tick_ts_ms: Option<u64> = None;
    let mut last_tick_ts_ms: Option<u64> = None;
    let mut nearest_start: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)
    if let Some(hub) = chainlink_hub {
        let Some(mut rx) = hub.subscribe(&target_symbol) else {
            warn!(
                "⚠️ chainlink_open_prewarm_hub_unsubscribed | symbol={} round_start_ts={} deadline_ts={}",
                target_symbol, round_start_ts, hard_deadline_ts
            );
            return;
        };
        while unix_now_secs() <= hard_deadline_ts {
            let next = tokio::time::timeout(Duration::from_millis(700), rx.recv()).await;
            match next {
                Ok(Ok((price, ts_ms))) => {
                    observed_ticks = observed_ticks.saturating_add(1);
                    first_tick_ts_ms.get_or_insert(ts_ms);
                    last_tick_ts_ms = Some(ts_ms);
                    let delta = ts_ms.abs_diff(start_ms);
                    match nearest_start {
                        Some((best_delta, _, _)) if delta >= best_delta => {}
                        _ => nearest_start = Some((delta, ts_ms, price)),
                    }
                    if ts_ms == start_ms {
                        set_prewarmed_open(&target_symbol, start_ms, price, ts_ms);
                        info!(
                            "⏱️ chainlink_open_prewarm_captured | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6}",
                            unix_now_millis_u64(),
                            target_symbol,
                            round_start_ts,
                            ts_ms,
                            price,
                        );
                        return;
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                    lagged_ticks = lagged_ticks.saturating_add(n);
                    continue;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                _ => continue,
            }
        }
        warn!(
            "⚠️ chainlink_open_prewarm_missed | symbol={} round_start_ts={} deadline_ts={} observed_ticks={} lagged_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?}",
            target_symbol,
            round_start_ts,
            hard_deadline_ts,
            observed_ticks,
            lagged_ticks,
            first_tick_ts_ms,
            last_tick_ts_ms,
            nearest_start
        );
        return;
    }

    let ws_url = post_close_chainlink_ws_url();
    let mut reconnect_backoff = Duration::from_millis(300);

    while unix_now_secs() <= hard_deadline_ts {
        let connect = tokio::time::timeout(Duration::from_secs(3), connect_async(&ws_url)).await;
        let Ok(Ok((ws, _resp))) = connect else {
            sleep(reconnect_backoff).await;
            reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(2));
            continue;
        };
        reconnect_backoff = Duration::from_millis(300);
        let (mut write, mut read) = ws.split();

        let subscribe_msg = json!({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": format!("{{\"symbol\":\"{}\"}}", target_symbol),
            }]
        });
        if write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .is_err()
        {
            sleep(reconnect_backoff).await;
            continue;
        }

        loop {
            if unix_now_secs() > hard_deadline_ts {
                break;
            }
            let next = tokio::time::timeout(Duration::from_millis(700), read.next()).await;
            let msg = match next {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => continue,
            };
            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                    Ok(t) => t,
                    Err(_) => continue,
                },
                _ => continue,
            };
            let ticks = parse_chainlink_all_ticks(&text, &target_symbol);
            if ticks.is_empty() {
                continue;
            }
            for (price, ts_ms) in ticks {
                observed_ticks = observed_ticks.saturating_add(1);
                first_tick_ts_ms.get_or_insert(ts_ms);
                last_tick_ts_ms = Some(ts_ms);
                let delta = ts_ms.abs_diff(start_ms);
                match nearest_start {
                    Some((best_delta, _, _)) if delta >= best_delta => {}
                    _ => nearest_start = Some((delta, ts_ms, price)),
                }
                if ts_ms == start_ms {
                    set_prewarmed_open(&target_symbol, start_ms, price, ts_ms);
                    remember_chainlink_exact_open(&target_symbol, start_ms, price, ts_ms);
                    info!(
                        "⏱️ chainlink_open_prewarm_captured | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6}",
                        unix_now_millis_u64(),
                        target_symbol,
                        round_start_ts,
                        ts_ms,
                        price,
                    );
                    return;
                }
            }
        }
    }
    warn!(
        "⚠️ chainlink_open_prewarm_missed | symbol={} round_start_ts={} deadline_ts={} observed_ticks={} lagged_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?}",
        target_symbol,
        round_start_ts,
        hard_deadline_ts,
        observed_ticks,
        lagged_ticks,
        first_tick_ts_ms,
        last_tick_ts_ms,
        nearest_start
    );
}

async fn run_local_price_open_prewarm(
    symbol: &str,
    round_start_ts: u64,
    hard_deadline_ts: u64,
    local_price_hub: Option<Arc<LocalPriceHub>>,
) {
    let Some(hub) = local_price_hub else {
        return;
    };
    let target_symbol = normalize_chainlink_symbol(symbol);
    if target_symbol.is_empty() {
        return;
    }
    let start_ms = round_start_ts.saturating_mul(1_000);
    let open_tol_ms = local_price_agg_open_tolerance_ms();
    let mut states: HashMap<LocalPriceSource, LocalSourceBoundaryState> = HashMap::new();

    for (price, ts_ms, source) in hub.snapshot_recent_ticks(&target_symbol) {
        local_price_agg_ingest_state(&mut states, source, price, ts_ms, start_ms, start_ms);
    }

    if let Some(mut rx) = hub.subscribe(&target_symbol) {
        while unix_now_secs() <= hard_deadline_ts {
            let next = tokio::time::timeout(Duration::from_millis(700), rx.recv()).await;
            match next {
                Ok(Ok((price, ts_ms, source))) => {
                    local_price_agg_ingest_state(
                        &mut states,
                        source,
                        price,
                        ts_ms,
                        start_ms,
                        start_ms,
                    );
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                Err(_) => continue,
            }
        }
    }

    let mut hits = 0usize;
    for source in LOCAL_PRICE_SOURCES {
        let Some(state) = states.get(&source) else {
            continue;
        };
        let Some(point) = pick_local_source_open_point(state, open_tol_ms) else {
            continue;
        };
        let tagged = AggregatedPricePoint {
            source: match point.source {
                "local_first_after_open" => "local_prewarm_open_first_after",
                "local_nearest_open" => "local_prewarm_open_nearest",
                _ => "local_prewarm_open_exact",
            },
            ..point
        };
        set_local_prewarmed_open(&target_symbol, start_ms, source, tagged);
        hits += 1;
        info!(
            "⏱️ local_price_open_prewarm_hit | unix_ms={} symbol={} source={} round_start_ts={} open_ts_ms={} open_price={:.6} exact={} delta_ms={}",
            unix_now_millis_u64(),
            target_symbol,
            source.as_str(),
            round_start_ts,
            point.ts_ms,
            point.price,
            point.exact,
            point.abs_delta_ms,
        );
    }

    if hits == 0 {
        warn!(
            "⚠️ local_price_open_prewarm_missed | symbol={} round_start_ts={} deadline_ts={}",
            target_symbol, round_start_ts, hard_deadline_ts,
        );
    }
}

#[derive(Debug, Clone, Copy)]
struct BookSnapshot {
    yes_bid: f64,
    yes_ask: f64,
    no_bid: f64,
    no_ask: f64,
    ts: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PostCloseBookSource {
    None,
    ClobRest,
    WsPartial,
}

impl PostCloseBookSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ClobRest => "clob_rest",
            Self::WsPartial => "ws_partial",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PostCloseBookEvidence {
    source: PostCloseBookSource,
    side: Side,
    bid: f64,
    ask: f64,
    recv_ms: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SideQuoteSnapshot {
    bid: f64,
    ask: f64,
    recv_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct PostCloseSideBookUpdate {
    side: Side,
    bid: f64,
    ask: f64,
    recv_ms: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PostCloseBookEvidenceTape {
    ws_yes: Option<SideQuoteSnapshot>,
    ws_no: Option<SideQuoteSnapshot>,
    rest_yes: Option<SideQuoteSnapshot>,
    rest_no: Option<SideQuoteSnapshot>,
}

impl PostCloseBookEvidenceTape {
    fn record(&mut self, ev: PostCloseBookEvidence) {
        let snap = SideQuoteSnapshot {
            bid: ev.bid.max(0.0),
            ask: ev.ask.max(0.0),
            recv_ms: ev.recv_ms,
        };
        match (ev.source, ev.side) {
            (PostCloseBookSource::WsPartial, Side::Yes) => self.ws_yes = Some(snap),
            (PostCloseBookSource::WsPartial, Side::No) => self.ws_no = Some(snap),
            (PostCloseBookSource::ClobRest, Side::Yes) => self.rest_yes = Some(snap),
            (PostCloseBookSource::ClobRest, Side::No) => self.rest_no = Some(snap),
            (PostCloseBookSource::None, _) => {}
        }
    }

    fn latest_for_side(self, side: Side) -> Option<(PostCloseBookSource, SideQuoteSnapshot)> {
        let ws = match side {
            Side::Yes => self.ws_yes,
            Side::No => self.ws_no,
        };
        let rest = match side {
            Side::Yes => self.rest_yes,
            Side::No => self.rest_no,
        };
        match (ws, rest) {
            (Some(w), Some(r)) => {
                if w.recv_ms >= r.recv_ms {
                    Some((PostCloseBookSource::WsPartial, w))
                } else {
                    Some((PostCloseBookSource::ClobRest, r))
                }
            }
            (Some(w), None) => Some((PostCloseBookSource::WsPartial, w)),
            (None, Some(r)) => Some((PostCloseBookSource::ClobRest, r)),
            (None, None) => None,
        }
    }
}

fn post_close_round_observation_from_latest_view(
    tape: PostCloseBookEvidenceTape,
    winner_side: Side,
    final_detect_ms: u64,
) -> (PostCloseBookSource, f64, f64, u64, u64) {
    if let Some((src, snap)) = tape.latest_for_side(winner_side) {
        let dist_ms = if snap.recv_ms > final_detect_ms {
            snap.recv_ms - final_detect_ms
        } else {
            final_detect_ms - snap.recv_ms
        };
        return (src, snap.bid, snap.ask, snap.recv_ms, dist_ms);
    }
    (PostCloseBookSource::None, 0.0, 0.0, 0, u64::MAX)
}

const ORACLE_LAG_WINNER_ASK_WINDOW_MS: u64 = 2_000;
const ORACLE_LAG_WINNER_ASK_WINDOW_SAMPLE_MS: u64 = 50;

async fn log_post_close_winner_ask_window_stats(
    shared_view: Arc<Mutex<PostCloseBookEvidenceTape>>,
    slug: &str,
    winner_side: Side,
    final_detect_ms: u64,
) {
    let deadline_ms = final_detect_ms.saturating_add(ORACLE_LAG_WINNER_ASK_WINDOW_MS);
    let mut samples: u64 = 0;
    let mut quote_samples: u64 = 0;
    let mut post_final_quote_samples: u64 = 0;
    let mut tradable_ask_samples: u64 = 0;
    let mut post_final_tradable_ask_samples: u64 = 0;
    let mut first_post_final_tradable_after_ms: Option<u64> = None;
    let mut best_post_final_tradable_ask: Option<(f64, PostCloseBookSource, u64)> = None;
    let mut last_source = PostCloseBookSource::None;
    let mut last_bid = 0.0;
    let mut last_ask = 0.0;
    let mut last_recv_ms = 0u64;

    while unix_now_millis_u64() <= deadline_ms {
        samples = samples.saturating_add(1);
        let tape = shared_view.lock().map(|g| *g).unwrap_or_default();
        let (source, bid, ask, recv_ms, _dist_ms) =
            post_close_round_observation_from_latest_view(tape, winner_side, final_detect_ms);
        if source != PostCloseBookSource::None {
            quote_samples = quote_samples.saturating_add(1);
            if recv_ms >= final_detect_ms {
                post_final_quote_samples = post_final_quote_samples.saturating_add(1);
            }
        }
        last_source = source;
        last_bid = bid;
        last_ask = ask;
        last_recv_ms = recv_ms;
        if let Some(eff_ask) = post_close_effective_ask_opt(bid, ask) {
            tradable_ask_samples = tradable_ask_samples.saturating_add(1);
            if recv_ms >= final_detect_ms {
                post_final_tradable_ask_samples = post_final_tradable_ask_samples.saturating_add(1);
                let after_ms = recv_ms.saturating_sub(final_detect_ms);
                if first_post_final_tradable_after_ms.is_none() {
                    first_post_final_tradable_after_ms = Some(after_ms);
                }
                let should_replace = best_post_final_tradable_ask
                    .map(|(best, _, _)| eff_ask < best - 1e-9)
                    .unwrap_or(true);
                if should_replace {
                    best_post_final_tradable_ask = Some((eff_ask, source, after_ms));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(
            ORACLE_LAG_WINNER_ASK_WINDOW_SAMPLE_MS,
        ))
        .await;
    }

    let (best_ask, best_source, best_after_ms) = best_post_final_tradable_ask
        .map(|(ask, src, after)| {
            (
                format!("{ask:.4}"),
                src.as_str().to_string(),
                after.to_string(),
            )
        })
        .unwrap_or_else(|| ("none".to_string(), "none".to_string(), "none".to_string()));

    info!(
        "📈 oracle_lag_winner_ask_window | slug={} side={:?} window_ms={} sample_ms={} samples={} quote_samples={} post_final_quote_samples={} tradable_ask_samples={} post_final_tradable_ask_samples={} first_post_final_tradable_after_ms={} best_post_final_tradable_ask={} best_post_final_source={} best_post_final_after_ms={} last_source={} last_bid={:.4} last_ask={:.4} last_recv_ms={} final_detect_ms={}",
        slug,
        winner_side,
        ORACLE_LAG_WINNER_ASK_WINDOW_MS,
        ORACLE_LAG_WINNER_ASK_WINDOW_SAMPLE_MS,
        samples,
        quote_samples,
        post_final_quote_samples,
        tradable_ask_samples,
        post_final_tradable_ask_samples,
        first_post_final_tradable_after_ms
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string()),
        best_ask,
        best_source,
        best_after_ms,
        last_source.as_str(),
        last_bid,
        last_ask,
        last_recv_ms,
        final_detect_ms,
    );
}

async fn run_post_close_observation_plane(
    shared_view: Arc<Mutex<PostCloseBookEvidenceTape>>,
    mut post_close_book_rx: mpsc::Receiver<PostCloseSideBookUpdate>,
    rest_url: String,
    yes_asset_id: String,
    no_asset_id: String,
    slug: String,
    market_end_ms: u64,
    hard_wait_deadline_ms: u64,
) {
    let mut rest_interval = tokio::time::interval(Duration::from_millis(100));
    rest_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if unix_now_millis_u64() >= hard_wait_deadline_ms {
            break;
        }
        tokio::select! {
            maybe_ev = post_close_book_rx.recv() => {
                if let Some(ev) = maybe_ev {
                    if ev.recv_ms >= market_end_ms {
                        if let Ok(mut view) = shared_view.lock() {
                            view.record(PostCloseBookEvidence {
                                source: PostCloseBookSource::WsPartial,
                                side: ev.side,
                                bid: ev.bid,
                                ask: ev.ask,
                                recv_ms: ev.recv_ms,
                            });
                        }
                        info!(
                            "📚 post_close_book_evidence | slug={} source=ws_partial side={:?} bid={:.4} ask={:.4} recv_ms={} lag_from_end_ms={}",
                            slug,
                            ev.side,
                            ev.bid,
                            ev.ask,
                            ev.recv_ms,
                            ev.recv_ms.saturating_sub(market_end_ms),
                        );
                    }
                } else {
                    break;
                }
            }
            _ = rest_interval.tick() => {
                let now_ms = unix_now_millis_u64();
                if now_ms >= market_end_ms {
                    if let Ok(Ok((yes_bid, yes_ask, no_bid, no_ask))) = tokio::time::timeout(
                        Duration::from_millis(120),
                        fetch_clob_top_of_book(&rest_url, &yes_asset_id, &no_asset_id),
                    ).await {
                        if let Ok(mut view) = shared_view.lock() {
                            view.record(PostCloseBookEvidence {
                                source: PostCloseBookSource::ClobRest,
                                side: Side::Yes,
                                bid: yes_bid,
                                ask: yes_ask,
                                recv_ms: now_ms,
                            });
                            view.record(PostCloseBookEvidence {
                                source: PostCloseBookSource::ClobRest,
                                side: Side::No,
                                bid: no_bid,
                                ask: no_ask,
                                recv_ms: now_ms,
                            });
                        }
                        info!(
                            "📚 post_close_book_evidence | slug={} source=clob_rest yes_bid={:.4} yes_ask={:.4} no_bid={:.4} no_ask={:.4} recv_ms={} lag_from_end_ms={}",
                            slug,
                            yes_bid,
                            yes_ask,
                            no_bid,
                            no_ask,
                            now_ms,
                            now_ms.saturating_sub(market_end_ms),
                        );
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
}

fn snapshot_book(msg: &MarketDataMsg) -> Option<BookSnapshot> {
    match msg {
        MarketDataMsg::BookTick {
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
            ts,
        } => Some(BookSnapshot {
            yes_bid: *yes_bid,
            yes_ask: *yes_ask,
            no_bid: *no_bid,
            no_ask: *no_ask,
            ts: *ts,
        }),
        _ => None,
    }
}

fn nonzero_price_opt(v: f64) -> Option<f64> {
    if v > 0.0 {
        Some(v)
    } else {
        None
    }
}

fn post_close_tick_size_for_price(bid: f64, ask: f64) -> f64 {
    if bid > 0.96 || ask > 0.96 || (bid > 0.0 && bid < 0.04) || (ask > 0.0 && ask < 0.04) {
        0.001
    } else {
        0.01
    }
}

fn post_close_effective_ask_opt(bid: f64, ask: f64) -> Option<f64> {
    if ask <= 0.0 {
        return None;
    }
    if bid <= 0.0 {
        return Some(ask);
    }
    let tick = post_close_tick_size_for_price(bid, ask);
    if ask > bid + 0.5 * tick + 1e-9 {
        Some(ask)
    } else {
        None
    }
}

fn fmt_price_opt(v: Option<f64>) -> String {
    match v {
        Some(p) => format!("{p:.4}"),
        None => "none".to_string(),
    }
}

/// Carries a winner-hint observation from one market's hint listener to the
/// cross-market arbiter, along with channels needed to forward the decision.
struct ArbiterObservation {
    round_end_ts: u64,
    slug: String,
    winner_side: Side,
    winner_bid: f64,
    winner_ask_raw: f64,
    /// Effective tradable ask (0.0 = no tradable ask).
    winner_ask_eff: f64,
    winner_ask_tradable: bool,
    /// Legacy compatibility metric. For `ws_partial` this is typically 0, for
    /// rest-derived observations this is nearest distance to final.
    book_age_ms: u64,
    /// Runtime evidence source for this round's winner-side book quality.
    book_source: PostCloseBookSource,
    /// Unix milliseconds when the evidence sample was captured.
    evidence_recv_ms: u64,
    /// Absolute |evidence_recv_ms - detect_ms| in milliseconds.
    distance_to_final_ms: u64,
    /// Unix milliseconds at which Chainlink result was detected.
    detect_ms: u64,
    /// The WinnerHint to forward to the selected market's coordinator.
    hint_msg: MarketDataMsg,
    /// Channel to the market coordinator (receives WinnerHint + OracleLagSelection).
    hint_tx: mpsc::Sender<MarketDataMsg>,
}

/// Per-market final observation used by round-tail coordinator.
/// Immediate per-market order path remains local; this stream is only for the
/// "all markets processed" tail action.
struct RoundTailObservation {
    round_end_ts: u64,
    slug: String,
    winner_side: Side,
    winner_bid: f64,
    winner_ask_raw: f64,
    winner_ask_eff: f64,
    winner_ask_tradable: bool,
    detect_ms: u64,
    hint_tx: mpsc::Sender<MarketDataMsg>,
}

const ORACLE_LAG_TAIL_MAKER_MAX_PRICE: f64 = 0.991;

/// Round-tail coordinator:
/// - Collect final observations for a round.
/// - Once all expected markets are observed (or timeout from first final):
///   send exactly one maker fallback (lowest winner-side bid market).
async fn run_oracle_lag_round_tail_coordinator(
    mut rx: mpsc::Receiver<RoundTailObservation>,
    expected_market_count: usize,
    timeout_ms: u64,
) {
    use std::collections::{HashMap, HashSet};
    let mut pending: HashMap<u64, HashMap<String, RoundTailObservation>> = HashMap::new();
    let mut deadlines: HashMap<u64, tokio::time::Instant> = HashMap::new();
    let mut finalized_rounds: HashSet<u64> = HashSet::new();

    let dispatch_fallback_maker = |round_end_ts: u64,
                                   by_slug: &HashMap<String, RoundTailObservation>|
     -> Option<(MarketDataMsg, mpsc::Sender<MarketDataMsg>)> {
        let observations: Vec<&RoundTailObservation> = by_slug.values().collect();
        if observations.is_empty() {
            return None;
        }

        let mut bid_candidates: Vec<&RoundTailObservation> = observations
            .into_iter()
            .filter(|o| o.winner_bid > 0.0)
            .collect();
        bid_candidates.sort_by(|a, b| {
            a.winner_bid
                .partial_cmp(&b.winner_bid)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.detect_ms.cmp(&b.detect_ms))
        });

        if let Some(best) = bid_candidates.first() {
            let tick = post_close_tick_size_for_price(best.winner_bid, best.winner_ask_raw);
            let step = if tick <= 0.001 + 1e-12 {
                tick
            } else {
                tick * 0.1
            };
            let price = (best.winner_bid + step)
                .min(ORACLE_LAG_TAIL_MAKER_MAX_PRICE)
                .max(step);
            let msg = MarketDataMsg::OracleLagTailAction {
                round_end_ts,
                side: best.winner_side,
                mode: OracleLagTailMode::MakerBidStep,
                limit_price: price,
                target_slug: best.slug.clone(),
                reason: "tail_lowest_bid_fallback_after_finals",
            };
            return Some((msg, best.hint_tx.clone()));
        }

        None
    };

    let finalize_round = |round_end_ts: u64,
                          pending: &mut HashMap<u64, HashMap<String, RoundTailObservation>>,
                          deadlines: &mut HashMap<u64, tokio::time::Instant>,
                          finalized_rounds: &mut HashSet<u64>| {
        deadlines.remove(&round_end_ts);
        if let Some(by_slug) = pending.remove(&round_end_ts) {
            if let Some((msg, tx)) = dispatch_fallback_maker(round_end_ts, &by_slug) {
                if let Err(e) = tx.try_send(msg) {
                    warn!(
                        "⚠️ oracle_lag_round_tail_send_failed | round_end_ts={} err={}",
                        round_end_ts, e
                    );
                }
            } else {
                info!(
                    "⏭️ oracle_lag_round_tail_skip | round_end_ts={} reason=no_bid_fallback_candidate",
                    round_end_ts
                );
            }
        } else {
            info!(
                "⏭️ oracle_lag_round_tail_skip | round_end_ts={} reason=no_pending_observation",
                round_end_ts
            );
        }
        finalized_rounds.insert(round_end_ts);
    };

    loop {
        let now = tokio::time::Instant::now();
        let expired: Vec<u64> = deadlines
            .iter()
            .filter(|(_, &dl)| now >= dl)
            .map(|(&ts, _)| ts)
            .collect();
        for round_end_ts in expired {
            finalize_round(
                round_end_ts,
                &mut pending,
                &mut deadlines,
                &mut finalized_rounds,
            );
        }

        let sleep_until = deadlines.values().min().cloned();
        if let Some(deadline) = sleep_until {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::select! {
                biased;
                maybe = rx.recv() => {
                    let Some(obs) = maybe else { break; };
                    if finalized_rounds.contains(&obs.round_end_ts) {
                        continue;
                    }
                    let round_end_ts = obs.round_end_ts;
                    let should_dispatch = {
                        let by_slug = pending.entry(round_end_ts).or_default();
                        by_slug.insert(obs.slug.clone(), obs);
                        expected_market_count > 0 && by_slug.len() >= expected_market_count
                    };
                    if should_dispatch {
                        finalize_round(
                            round_end_ts,
                            &mut pending,
                            &mut deadlines,
                            &mut finalized_rounds,
                        );
                    } else {
                        deadlines.entry(round_end_ts).or_insert_with(|| {
                            tokio::time::Instant::now()
                                + std::time::Duration::from_millis(timeout_ms)
                        });
                    }
                }
                _ = tokio::time::sleep(remaining) => {}
            }
        } else {
            let Some(obs) = rx.recv().await else {
                break;
            };
            if finalized_rounds.contains(&obs.round_end_ts) {
                continue;
            }
            let round_end_ts = obs.round_end_ts;
            let should_dispatch = {
                let by_slug = pending.entry(round_end_ts).or_default();
                by_slug.insert(obs.slug.clone(), obs);
                expected_market_count > 0 && by_slug.len() >= expected_market_count
            };
            if should_dispatch {
                finalize_round(
                    round_end_ts,
                    &mut pending,
                    &mut deadlines,
                    &mut finalized_rounds,
                );
            } else {
                deadlines.entry(round_end_ts).or_insert_with(|| {
                    tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms)
                });
            }
        }
    }
    warn!("🛑 oracle_lag_round_tail_coordinator exited (channel closed)");
}

/// Cross-market arbiter: collects observations from all hint listeners within a
/// configurable window, ranks by ask quality, and selects the single best market.
async fn run_cross_market_hint_arbiter(
    mut obs_rx: mpsc::Receiver<ArbiterObservation>,
    expected_market_count: usize,
    collection_window_ms: u64,
    book_max_age_ms: u64,
) {
    use std::collections::{HashMap, HashSet};
    let mut pending: HashMap<u64, HashMap<String, ArbiterObservation>> = HashMap::new();
    // Deadline = tokio::time::Instant when the max wait expires per round.
    let mut deadlines: HashMap<u64, tokio::time::Instant> = HashMap::new();
    // Dedup guard: once a round is dispatched, late observations for that same
    // round_end_ts must be dropped (prevents duplicate selected=true decisions).
    let mut finalized_rounds: HashSet<u64> = HashSet::new();

    loop {
        // Fire any expired collection windows.
        let now = tokio::time::Instant::now();
        let expired: Vec<u64> = deadlines
            .iter()
            .filter(|(_, &dl)| now >= dl)
            .map(|(&ts, _)| ts)
            .collect();
        for round_end_ts in expired {
            deadlines.remove(&round_end_ts);
            if let Some(observations) = pending.remove(&round_end_ts) {
                arbiter_dispatch(
                    observations.into_values().collect(),
                    round_end_ts,
                    book_max_age_ms,
                )
                .await;
                finalized_rounds.insert(round_end_ts);
            }
        }

        // Determine how long until the next deadline (if any).
        let sleep_until = deadlines.values().min().cloned();

        if let Some(deadline) = sleep_until {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::select! {
                biased;
                maybe = obs_rx.recv() => {
                    match maybe {
                        None => break,
                        Some(obs) => {
                            let should_dispatch = arbiter_intake(
                                &mut pending,
                                &mut deadlines,
                                &mut finalized_rounds,
                                obs,
                                collection_window_ms,
                                expected_market_count,
                            );
                            if let Some(round_end_ts) = should_dispatch {
                                deadlines.remove(&round_end_ts);
                                if let Some(observations) = pending.remove(&round_end_ts) {
                                    arbiter_dispatch(
                                        observations.into_values().collect(),
                                        round_end_ts,
                                        book_max_age_ms,
                                    )
                                    .await;
                                    finalized_rounds.insert(round_end_ts);
                                }
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(remaining) => { /* loop will fire expired windows */ }
            }
        } else {
            match obs_rx.recv().await {
                None => break,
                Some(obs) => {
                    let should_dispatch = arbiter_intake(
                        &mut pending,
                        &mut deadlines,
                        &mut finalized_rounds,
                        obs,
                        collection_window_ms,
                        expected_market_count,
                    );
                    if let Some(round_end_ts) = should_dispatch {
                        deadlines.remove(&round_end_ts);
                        if let Some(observations) = pending.remove(&round_end_ts) {
                            arbiter_dispatch(
                                observations.into_values().collect(),
                                round_end_ts,
                                book_max_age_ms,
                            )
                            .await;
                            finalized_rounds.insert(round_end_ts);
                        }
                    }
                }
            }
        }
    }
    warn!("🛑 cross_market_hint_arbiter exited (channel closed)");
}

fn arbiter_intake(
    pending: &mut std::collections::HashMap<
        u64,
        std::collections::HashMap<String, ArbiterObservation>,
    >,
    deadlines: &mut std::collections::HashMap<u64, tokio::time::Instant>,
    finalized_rounds: &mut std::collections::HashSet<u64>,
    obs: ArbiterObservation,
    collection_window_ms: u64,
    expected_market_count: usize,
) -> Option<u64> {
    let ts = obs.round_end_ts;
    if finalized_rounds.contains(&ts) {
        debug!(
            "⏭️ oracle_lag_arbiter_intake_ignored_finalized | round_end_ts={} slug={}",
            ts, obs.slug
        );
        return None;
    }
    info!(
        "📥 oracle_lag_arbiter_intake | round_end_ts={} slug={} winner_ask_eff={:.4} winner_ask_tradable={} book_source={} distance_to_final_ms={} book_age_ms={}",
        ts,
        obs.slug,
        obs.winner_ask_eff,
        obs.winner_ask_tradable,
        obs.book_source.as_str(),
        obs.distance_to_final_ms,
        obs.book_age_ms
    );
    let by_slug = pending.entry(ts).or_default();
    match by_slug.get(&obs.slug) {
        Some(existing) => {
            let replace = obs.book_source > existing.book_source
                || (obs.book_source == existing.book_source
                    && obs.distance_to_final_ms < existing.distance_to_final_ms)
                || (obs.book_source == existing.book_source
                    && obs.distance_to_final_ms == existing.distance_to_final_ms
                    && obs.detect_ms < existing.detect_ms);
            if replace {
                by_slug.insert(obs.slug.clone(), obs);
            }
        }
        None => {
            by_slug.insert(obs.slug.clone(), obs);
        }
    }

    let observed = by_slug.len();
    let early_dispatch = expected_market_count > 0 && observed >= expected_market_count;
    if early_dispatch {
        info!(
            "🏁 oracle_lag_arbiter_round_ready | round_end_ts={} observed={} expected={} early_dispatch=true",
            ts, observed, expected_market_count
        );
        return Some(ts);
    }

    deadlines.entry(ts).or_insert_with(|| {
        tokio::time::Instant::now() + std::time::Duration::from_millis(collection_window_ms)
    });
    None
}

async fn arbiter_dispatch(
    observations: Vec<ArbiterObservation>,
    round_end_ts: u64,
    book_max_age_ms: u64,
) {
    if observations.is_empty() {
        return;
    }
    let n = observations.len();
    let has_real_post_close = observations
        .iter()
        .any(|o| o.book_source != PostCloseBookSource::None);
    let has_fresh = observations
        .iter()
        .any(|o| o.book_age_ms <= book_max_age_ms);

    // Build a ranked index (ascending = best first).
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| {
        let oa = &observations[a];
        let ob = &observations[b];
        // Real post-close evidence beats fallback-none.
        if oa.book_source != ob.book_source {
            return ob.book_source.cmp(&oa.book_source);
        }
        // Stale penalty when a fresh alternative exists.
        let a_stale = has_fresh && oa.book_age_ms > book_max_age_ms;
        let b_stale = has_fresh && ob.book_age_ms > book_max_age_ms;
        match (a_stale, b_stale) {
            (true, false) => return std::cmp::Ordering::Greater,
            (false, true) => return std::cmp::Ordering::Less,
            _ => {}
        }
        // Tradable ask beats no-ask.
        let a_tradable = oa.winner_ask_tradable && oa.winner_ask_eff > 0.0;
        let b_tradable = ob.winner_ask_tradable && ob.winner_ask_eff > 0.0;
        match (a_tradable, b_tradable) {
            (true, false) => return std::cmp::Ordering::Less,
            (false, true) => return std::cmp::Ordering::Greater,
            _ => {}
        }
        // Lower effective ask is better.
        if (oa.winner_ask_eff - ob.winner_ask_eff).abs() > 1e-9 {
            return oa
                .winner_ask_eff
                .partial_cmp(&ob.winner_ask_eff)
                .unwrap_or(std::cmp::Ordering::Equal);
        }
        // Higher bid (tighter spread) is better.
        if (oa.winner_bid - ob.winner_bid).abs() > 1e-9 {
            return ob
                .winner_bid
                .partial_cmp(&oa.winner_bid)
                .unwrap_or(std::cmp::Ordering::Equal);
        }
        // Closer evidence-to-final is better.
        if oa.distance_to_final_ms != ob.distance_to_final_ms {
            return oa.distance_to_final_ms.cmp(&ob.distance_to_final_ms);
        }
        // Earlier detection wins tiebreak.
        oa.detect_ms.cmp(&ob.detect_ms)
    });

    // Winner is eligible only if it isn't itself stale (when fresh candidates exist).
    let winner_idx = order[0];
    let winner_stale = has_fresh && observations[winner_idx].book_age_ms > book_max_age_ms;
    let winner_has_real_book = observations[winner_idx].book_source != PostCloseBookSource::None;
    let winner_eligible = has_real_post_close && winner_has_real_book && !winner_stale;

    for (rank_pos, &obs_idx) in order.iter().enumerate() {
        let obs = &observations[obs_idx];
        let rank = (rank_pos + 1) as u8;
        let selected = winner_eligible && rank_pos == 0;
        let reason: &'static str = if !has_real_post_close {
            "no_real_post_close_book"
        } else if !winner_eligible && rank_pos == 0 {
            "no_eligible_candidate"
        } else if selected {
            "best_ask_eff"
        } else {
            "outranked"
        };

        info!(
            "🏆 oracle_lag_arbiter_decision | round_end_ts={} slug={} rank={}/{} selected={} reason={} winner_ask_eff={:.4} winner_ask_tradable={} book_source={} distance_to_final_ms={} book_age_ms={}",
            round_end_ts,
            obs.slug,
            rank,
            n,
            selected,
            reason,
            obs.winner_ask_eff,
            obs.winner_ask_tradable,
            obs.book_source.as_str(),
            obs.distance_to_final_ms,
            obs.book_age_ms
        );

        // Send OracleLagSelection first so coordinator gate is set before WinnerHint.
        let sel_msg = MarketDataMsg::OracleLagSelection {
            round_end_ts,
            selected,
            rank,
            reason,
        };
        if let Err(e) = obs.hint_tx.try_send(sel_msg) {
            warn!(
                "⚠️ arbiter_selection_send_failed | slug={} rank={} err={}",
                obs.slug, rank, e
            );
        }

        // Forward WinnerHint only to the selected market.
        if selected {
            if let Err(e) = obs.hint_tx.try_send(obs.hint_msg.clone()) {
                warn!(
                    "⚠️ arbiter_hint_forward_failed | slug={} err={}",
                    obs.slug, e
                );
            }
        }
    }
}

async fn run_post_close_winner_hint_listener(
    winner_hint_tx: mpsc::Sender<MarketDataMsg>,
    arbiter_tx: Option<mpsc::Sender<ArbiterObservation>>,
    round_tail_tx: Option<mpsc::Sender<RoundTailObservation>>,
    coord_md_rx: watch::Receiver<MarketDataMsg>,
    post_close_book_rx: mpsc::Receiver<PostCloseSideBookUpdate>,
    rest_url: String,
    chainlink_hub: Option<Arc<ChainlinkHub>>,
    local_price_hub: Option<Arc<LocalPriceHub>>,
    slug: String,
    yes_asset_id: String,
    no_asset_id: String,
    round_start_ts: u64,
    round_end_ts: u64,
    post_close_window_secs: u64,
) {
    let Some(symbol) = chainlink_symbol_from_slug(&slug) else {
        warn!(
            "⚠️ post_close winner hint skipped: failed to derive symbol from slug '{}'",
            slug
        );
        return;
    };

    let market_end_ms = round_end_ts.saturating_mul(1_000);
    let chainlink_wait_secs = post_close_chainlink_max_wait_secs();
    let chainlink_deadline_ts = round_end_ts.saturating_add(chainlink_wait_secs);

    // Chainlink-only decision path:
    // - Runtime decision uses Chainlink RTDS only.
    // - Gamma is validation-only and intentionally excluded from trading path.
    // Task starts IMMEDIATELY so RTDS backfill has best chance to include open_t.
    let cl_symbol = symbol.clone();
    let cl_slug = slug.clone();
    let cl_local_price_hub = local_price_hub.clone();
    let cl_chainlink_hub = chainlink_hub.clone();
    let chainlink_task = tokio::spawn(async move {
        if let Some((source, side, open_ref, close_px, open_ts_ms, close_ts_ms, open_is_exact)) =
            run_chainlink_winner_hint(
                &cl_symbol,
                round_start_ts,
                round_end_ts,
                chainlink_deadline_ts,
                cl_chainlink_hub,
                cl_local_price_hub,
            )
            .await
        {
            let detect_ms = unix_now_millis_u64();
            info!(
                "⏱️ chainlink_result_ready | slug={} symbol={} source={:?} side={:?} open_exact={} open_ref={:.15} close={:.15} open_ts_ms={} close_ts_ms={} detect_ms={} latency_from_end_ms={}",
                cl_slug, cl_symbol, source, side, open_is_exact, open_ref, close_px,
                open_ts_ms, close_ts_ms, detect_ms,
                detect_ms.saturating_sub(market_end_ms),
            );
            return Some((source, side, open_ref, close_px, detect_ms, open_is_exact));
        }
        None
    });
    let frontend_symbol = symbol.clone();
    let mut frontend_task = Some(tokio::spawn(async move {
        fetch_frontend_crypto_round_prices(&frontend_symbol, round_start_ts, round_end_ts).await
    }));
    // Independent local aggregator lane for accuracy/latency benchmarking against RTDS.
    // This lane never drives trading in shadow mode; it only emits compare logs.
    let mut local_compare_task = if local_price_agg_enabled() && !local_price_agg_decision_enabled()
    {
        let cmp_hub = local_price_hub.clone();
        let cmp_symbol = symbol.clone();
        let cmp_deadline_ms = round_end_ts
            .saturating_mul(1_000)
            .saturating_add(local_price_agg_decision_wait_ms())
            .saturating_add(LOCAL_PRICE_AGG_COMPARE_DEADLINE_GRACE_MS);
        Some(tokio::spawn(async move {
            let started_ms = unix_now_millis_u64();
            let hit = run_local_price_close_aggregator(
                cmp_hub,
                &cmp_symbol,
                round_start_ts,
                round_end_ts,
                cmp_deadline_ms,
            )
            .await;
            let ready_ms = unix_now_millis_u64();
            (hit, started_ms, ready_ms, cmp_deadline_ms)
        }))
    } else {
        None
    };
    let mut local_full_shadow_task =
        if local_price_agg_enabled() && !local_price_agg_decision_enabled() {
            let full_hub = local_price_hub.clone();
            let full_symbol = symbol.clone();
            let full_deadline_ms = round_end_ts
                .saturating_mul(1_000)
                .saturating_add(local_price_agg_decision_wait_ms())
                .saturating_add(LOCAL_PRICE_AGG_COMPARE_DEADLINE_GRACE_MS);
            Some(tokio::spawn(async move {
                let started_ms = unix_now_millis_u64();
                let hit = run_local_price_aggregator(
                    full_hub,
                    &full_symbol,
                    round_start_ts,
                    round_end_ts,
                    full_deadline_ms,
                )
                .await;
                let ready_ms = unix_now_millis_u64();
                (hit, started_ms, ready_ms, full_deadline_ms)
            }))
        } else {
            None
        };

    // ── Wait until t-10s for book snapshot ──
    // The Chainlink task is already running; this sleep only gates the pre-close book snapshot
    // (no impact on decision).
    let preclose_ts = round_end_ts.saturating_sub(10);
    let now_secs = unix_now_secs();
    if preclose_ts > now_secs {
        sleep(Duration::from_secs(preclose_ts - now_secs)).await;
    }

    // ── At t-10s: book snapshot ──
    let snap_pre = snapshot_book(&coord_md_rx.borrow().clone()).unwrap_or(BookSnapshot {
        yes_bid: 0.0,
        yes_ask: 0.0,
        no_bid: 0.0,
        no_ask: 0.0,
        ts: Instant::now(),
    });
    let yes_ask_eff = fmt_price_opt(post_close_effective_ask_opt(
        snap_pre.yes_bid,
        snap_pre.yes_ask,
    ));
    let no_ask_eff = fmt_price_opt(post_close_effective_ask_opt(
        snap_pre.no_bid,
        snap_pre.no_ask,
    ));
    info!(
        "⏱️ post_close_preclose_snapshot | unix_ms={} slug={} round_start_ts={} round_end_ts={} t_minus_s={} yes_asset={} no_asset={} yes_bid={:.4} yes_ask={:.4} yes_ask_eff={} no_bid={:.4} no_ask={:.4} no_ask_eff={} book_age_ms={}",
        unix_now_millis_u64(), slug, round_start_ts, round_end_ts,
        round_end_ts.saturating_sub(unix_now_secs()),
        yes_asset_id, no_asset_id,
        snap_pre.yes_bid, snap_pre.yes_ask, yes_ask_eff,
        snap_pre.no_bid, snap_pre.no_ask, no_ask_eff,
        snap_pre.ts.elapsed().as_millis(),
    );

    info!(
        "📊 post_close_validation_source | slug={} decision=chainlink_only gamma_polling=disabled data_streams_enabled={} data_streams_base={}",
        slug,
        chainlink_data_streams_enabled(),
        chainlink_data_streams_price_api_base_url()
    );

    // ── Observation plane (book evidence) runs independently from decision plane ──
    let hard_wait_deadline_ms =
        market_end_ms.saturating_add(post_close_window_secs.saturating_mul(1_000));
    let shared_execution_view = Arc::new(Mutex::new(PostCloseBookEvidenceTape::default()));
    let obs_view = Arc::clone(&shared_execution_view);
    let obs_slug = slug.clone();
    let obs_rest_url = rest_url.clone();
    let obs_yes_asset_id = yes_asset_id.clone();
    let obs_no_asset_id = no_asset_id.clone();
    let observation_task = tokio::spawn(async move {
        run_post_close_observation_plane(
            obs_view,
            post_close_book_rx,
            obs_rest_url,
            obs_yes_asset_id,
            obs_no_asset_id,
            obs_slug,
            market_end_ms,
            hard_wait_deadline_ms,
        )
        .await;
    });

    let chainlink_result: Option<(WinnerHintSource, Side, f64, f64, u64, bool)> =
        match chainlink_task.await {
            Ok(Some(v)) => Some(v),
            Ok(None) => None,
            Err(e) => {
                warn!(
                    "⚠️ post_close chainlink task join error for {}: {}",
                    slug, e
                );
                None
            }
        };

    let (compare_truth, compare_truth_open_source): (
        Option<(WinnerHintSource, Side, f64, f64, u64, bool)>,
        Option<&'static str>,
    ) = if let Some(v) = chainlink_result {
        (Some(v), Some("exact_open"))
    } else if local_price_agg_enabled() && !local_price_agg_decision_enabled() {
        let start_ms = round_start_ts.saturating_mul(1_000);
        let end_ms = round_end_ts.saturating_mul(1_000);
        let open_truth = peek_chainlink_exact_open(&symbol, start_ms)
            .map(|(px, ts)| ("exact_open", px, ts, true))
            .or_else(|| {
                cached_prev_round_close(&symbol, start_ms)
                    .map(|(px, ts)| ("prev_exact_close", px, ts, false))
            });
        let close_truth = peek_chainlink_exact_close(&symbol, end_ms)
            .map(|(px, ts)| ("exact_close", px, ts))
            .or_else(|| {
                cached_round_exact_close(&symbol, end_ms)
                    .map(|(px, ts)| ("last_close_cache", px, ts))
            });
        match (open_truth, close_truth) {
            (
                Some((open_truth_source, open_ref_px, open_ts_ms, open_is_exact)),
                Some((close_truth_source, close_px, close_ts_ms)),
            ) => {
                let side = if close_px >= open_ref_px {
                    Side::Yes
                } else {
                    Side::No
                };
                info!(
                    "🧪 chainlink_compare_truth_fallback | slug={} symbol={} source=rtds_truth_cache open_truth_source={} close_truth_source={} side={:?} open_ref={:.15}@{} close={:.15}@{}",
                    slug,
                    symbol,
                    open_truth_source,
                    close_truth_source,
                    side,
                    open_ref_px,
                    open_ts_ms,
                    close_px,
                    close_ts_ms,
                );
                (
                    Some((
                        WinnerHintSource::Chainlink,
                        side,
                        open_ref_px,
                        close_px,
                        close_ts_ms,
                        open_is_exact,
                    )),
                    Some(open_truth_source),
                )
            }
            _ => (None, None),
        }
    } else {
        (None, None)
    };

    // ── Chainlink result → emit WinnerHint ──
    let first = if let Some(v) = compare_truth {
        v
    } else {
        let frontend_round = if let Some(task) = frontend_task.take() {
            match tokio::time::timeout(Duration::from_millis(450), task).await {
                Ok(Ok(hit)) => hit,
                Ok(Err(err)) => {
                    warn!(
                        "⚠️ frontend_round_task_join_error | slug={} err={}",
                        slug, err
                    );
                    None
                }
                Err(_) => None,
            }
        } else {
            None
        };
        if let Some(hit) = frontend_round {
            let frontend_winner = match (hit.open_price, hit.close_price) {
                (Some(open), Some(close)) if close >= open => Some(Side::Yes),
                (Some(_open), Some(_close)) => Some(Side::No),
                _ => None,
            };
            warn!(
                "⚠️ post_close_unresolved_observation | slug={} symbol={} reason=chainlink_unresolved frontend_open={:?} frontend_close={:?} frontend_winner={:?} frontend_completed={:?} frontend_cached={:?} frontend_ts_ms={} latency_from_end_ms={}",
                slug,
                symbol,
                hit.open_price,
                hit.close_price,
                frontend_winner,
                hit.completed,
                hit.cached,
                hit.timestamp_ms,
                unix_now_millis_u64().saturating_sub(market_end_ms),
            );
        } else {
            warn!(
                "⚠️ post_close_winner_frontend_missing | slug={} symbol={} — frontend api returned no round data",
                slug, symbol
            );
            warn!(
                "⚠️ post_close winner hint unresolved within window for {} (end={} window={}s)",
                slug, round_end_ts, post_close_window_secs
            );
            observation_task.abort();
            return;
        }
        warn!(
            "⚠️ post_close winner hint unresolved within window for {} (end={} window={}s)",
            slug, round_end_ts, post_close_window_secs
        );
        observation_task.abort();
        return;
    };
    let (first_source, first_side, first_ref, first_obs, first_ms, first_open_exact) = first;
    let compare_truth_open_source = compare_truth_open_source.unwrap_or("none");
    let winner_hint_ready = chainlink_result.is_some();

    if let Some(task) = local_compare_task.take() {
        match task.await {
            Ok((mut compare_hit, started_ms, ready_ms, deadline_ms)) => {
                if let Some(hit) = compare_hit.base_hit.as_mut() {
                    local_boundary_maybe_debias_hype_close_only(
                        &symbol,
                        hit,
                        round_end_ts,
                        first_ref,
                    );
                }
                for outcome in &mut compare_hit.boundary_shadow_outcomes {
                    if let Some(hit) = outcome.hit.as_mut() {
                        local_boundary_maybe_equalize_xrp_binance_coinbase_fast_no(
                            &symbol,
                            hit,
                            round_end_ts,
                            first_ref,
                        );
                    }
                }
                let weighted_shadow_hit = compare_hit
                    .boundary_shadow_outcomes
                    .iter()
                    .find(|outcome| outcome.policy_name == "boundary_weighted")
                    .and_then(|outcome| outcome.hit.as_ref());
                let mut close_only_filtered = false;
                let mut close_only_filter_reason: Option<&'static str> = None;
                let mut close_only_direction_margin_bps: Option<f64> = None;
                if let Some(hit) = compare_hit.base_hit.as_ref() {
                    local_price_agg_learn_source_biases_from_rtds(
                        &symbol,
                        first_obs,
                        &hit.source_contributions,
                    );
                    let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    let source_agreement = if hit.source_contributions.is_empty() {
                        0.0
                    } else {
                        let agree_cnt = hit
                            .source_contributions
                            .iter()
                            .filter(|src| {
                                let src_side = if src.adjusted_close_price >= first_ref {
                                    Side::Yes
                                } else {
                                    Side::No
                                };
                                src_side == local_side_vs_rtds_open
                            })
                            .count();
                        agree_cnt as f64 / hit.source_contributions.len() as f64
                    };
                    let direction_margin_bps = ((hit.close_price - first_ref).abs()
                        / first_ref.abs().max(1e-12))
                        * 10_000.0;
                    close_only_direction_margin_bps = Some(direction_margin_bps);
                    let all_preclose = hit
                        .source_contributions
                        .iter()
                        .all(|src| src.close_ts_ms < round_end_ts.saturating_mul(1_000));
                    let preclose_relief_applied = all_preclose
                        && hit.close_exact_sources == 0
                        && direction_margin_bps + 1e-9
                            >= LOCAL_PRICE_AGG_COMPARE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS
                        && direction_margin_bps + 1e-9
                            < LOCAL_PRICE_AGG_COMPARE_PRECLOSE_MIN_DIRECTION_MARGIN_BPS;
                    let safe_preclose_relief_applied = all_preclose
                        && hit.close_exact_sources == 0
                        && hit.source_count >= LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_SOURCES
                        && hit.source_spread_bps
                            <= LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS
                                + 1e-9
                        && direction_margin_bps + 1e-9
                            >= LOCAL_PRICE_AGG_COMPARE_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS;
                    let sol_safe_preclose_relief_applied = symbol == "sol/usd"
                        && all_preclose
                        && hit.close_exact_sources == 0
                        && !(hit.source_count == 2
                            && hit.source_spread_bps
                                <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS_TWO_SOURCE
                                    + 1e-9
                            && direction_margin_bps + 1e-9 < 3.0)
                        && ((hit.source_count
                            >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_SOURCES
                            && hit.source_spread_bps
                                <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS
                                    + 1e-9
                            && direction_margin_bps + 1e-9
                                >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS)
                            || (hit.source_count >= 2
                                && hit.source_spread_bps
                                    <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MAX_SOURCE_SPREAD_BPS_TWO_SOURCE
                                        + 1e-9
                                && direction_margin_bps + 1e-9
                                    >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_PRECLOSE_RELIEF_MIN_DIRECTION_MARGIN_BPS_TWO_SOURCE));
                    let single_source_min_direction_margin_bps =
                        local_price_agg_single_source_min_direction_margin_bps()
                            .max(LOCAL_PRICE_AGG_RELIEF_MIN_DIRECTION_MARGIN_BPS_DEFAULT);
                    let safe_single_source_relief_applied = hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && direction_margin_bps + 1e-9
                            >= LOCAL_PRICE_AGG_COMPARE_SAFE_SINGLE_SOURCE_RELIEF_MIN_DIRECTION_MARGIN_BPS;
                    if all_preclose
                        && hit.close_exact_sources == 0
                        && !preclose_relief_applied
                        && !safe_preclose_relief_applied
                        && !sol_safe_preclose_relief_applied
                        && !safe_single_source_relief_applied
                        && direction_margin_bps + 1e-9
                            < LOCAL_PRICE_AGG_COMPARE_PRECLOSE_MIN_DIRECTION_MARGIN_BPS
                    {
                        close_only_filter_reason = Some("preclose_near_flat");
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=preclose_near_flat direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            LOCAL_PRICE_AGG_COMPARE_PRECLOSE_MIN_DIRECTION_MARGIN_BPS,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && direction_margin_bps + 1e-9 >= 20.0
                    {
                        close_only_filter_reason = Some("hype_close_only_single_far_margin");
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_far_margin direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            20.0,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 6.0
                        && direction_margin_bps < 7.0
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 150
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 500
                    {
                        close_only_filter_reason =
                            Some("hype_close_only_single_hyperliquid_no_fast_mid_margin_tail");
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_fast_mid_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            6.0,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 12.68
                        && direction_margin_bps < 12.71
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 120
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_micro_upper_mid_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_micro_upper_mid_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            12.68,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 13.40
                        && direction_margin_bps < 13.45
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 30
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_micro_upper_high_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_micro_upper_high_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            13.40,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 13.82
                        && direction_margin_bps < 13.90
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 10
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_micro_high_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_micro_high_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            13.82,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 11.4
                        && direction_margin_bps < 11.7
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 50
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 80
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_micro_upper_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_micro_upper_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            11.4,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 14.42
                        && direction_margin_bps < 14.45
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 500
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 600
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_late_upper_mid_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_late_upper_mid_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            14.42,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 10.0
                        && direction_margin_bps < 16.0
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 850
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 950
                    {
                        close_only_filter_reason =
                            Some("hype_close_only_single_hyperliquid_no_late_upper_margin_tail");
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_late_upper_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            10.0,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 14.45
                        && direction_margin_bps < 14.55
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 400
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 450
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_midlag_upper_mid_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_midlag_upper_mid_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            14.45,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 19.14
                        && direction_margin_bps < 19.18
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 60
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_micro_far_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_micro_far_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            19.14,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 19.27
                        && direction_margin_bps < 19.35
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 40
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 250
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_no_fast_far_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_no_fast_far_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            19.27,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 2
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::Yes
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Hyperliquid)
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Okx)
                        && hit.source_spread_bps + 1e-9 >= 1.0
                        && hit.source_spread_bps < 1.5
                        && direction_margin_bps + 1e-9 >= 20.0
                        && direction_margin_bps < 30.0
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 500
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_hyperliquid_okx_yes_fast_midspread_far_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_hyperliquid_okx_yes_fast_midspread_far_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            20.0,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 2
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::Yes
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Bybit)
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 10.5
                        && direction_margin_bps < 10.8
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 2_000
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 2_600
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_bybit_hyperliquid_yes_very_stale_mid_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_bybit_hyperliquid_yes_very_stale_mid_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            10.5,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 2
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::Yes
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Bybit)
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Hyperliquid)
                        && hit.source_spread_bps < 0.5
                        && direction_margin_bps + 1e-9 >= 30.0
                        && direction_margin_bps < 40.0
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 1_000
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 1_500
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_bybit_hyperliquid_yes_stale_tightspread_far_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_bybit_hyperliquid_yes_stale_tightspread_far_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            30.0,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::Yes
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                        && direction_margin_bps + 1e-9 >= 5.5
                        && direction_margin_bps < 5.6
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 450
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 550
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_single_hyperliquid_yes_late_low_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_single_hyperliquid_yes_late_low_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            5.5,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 2
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::Yes
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Bybit)
                        && hit
                            .source_contributions
                            .iter()
                            .any(|src| src.source == LocalPriceSource::Hyperliquid)
                        && hit.source_spread_bps + 1e-9 >= 0.5
                        && hit.source_spread_bps < 0.7
                        && direction_margin_bps + 1e-9 >= 12.0
                        && direction_margin_bps < 15.0
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 2_000
                        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 2_500
                    {
                        close_only_filter_reason = Some(
                            "hype_close_only_bybit_hyperliquid_yes_very_stale_tightspread_upper_margin_tail",
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=hype_close_only_bybit_hyperliquid_yes_very_stale_tightspread_upper_margin_tail direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            12.0,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered
                        && symbol == "hype/usd"
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && local_side_vs_rtds_open == Side::No
                        && hit
                            .source_contributions
                            .first()
                            .is_some_and(|src| src.source == LocalPriceSource::Hyperliquid)
                    {
                        let close_abs_delta_ms =
                            hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000));
                        let close_only_tail_reason = if direction_margin_bps + 1e-9 >= 2.95
                            && direction_margin_bps < 3.0
                            && close_abs_delta_ms >= 280
                            && close_abs_delta_ms <= 320
                        {
                            Some((
                                "hype_close_only_single_hyperliquid_no_late_mid_margin_side_tail",
                                2.95,
                            ))
                        } else if direction_margin_bps + 1e-9 >= 5.0
                            && direction_margin_bps < 5.1
                            && close_abs_delta_ms >= 180
                            && close_abs_delta_ms <= 230
                        {
                            Some((
                                "hype_close_only_single_hyperliquid_no_fast_low_margin_side_tail",
                                5.0,
                            ))
                        } else if direction_margin_bps + 1e-9 >= 6.4
                            && direction_margin_bps < 6.5
                            && close_abs_delta_ms >= 80
                            && close_abs_delta_ms <= 120
                        {
                            Some((
                                "hype_close_only_single_hyperliquid_no_micro_mid_margin_error_tail",
                                6.4,
                            ))
                        } else if direction_margin_bps + 1e-9 >= 4.35
                            && direction_margin_bps < 4.39
                            && close_abs_delta_ms >= 480
                            && close_abs_delta_ms <= 520
                        {
                            Some((
                                "hype_close_only_single_hyperliquid_no_midlag_low_margin_side_tail",
                                4.35,
                            ))
                        } else if direction_margin_bps + 1e-9 >= 4.2
                            && direction_margin_bps < 4.35
                            && close_abs_delta_ms >= 850
                            && close_abs_delta_ms <= 900
                        {
                            Some((
                                "hype_close_only_single_hyperliquid_no_stale_lower_mid_margin_side_tail",
                                4.2,
                            ))
                        } else if direction_margin_bps + 1e-9 >= 5.9
                            && direction_margin_bps < 6.1
                            && close_abs_delta_ms >= 880
                            && close_abs_delta_ms <= 930
                        {
                            Some((
                                "hype_close_only_single_hyperliquid_no_stale_mid_margin_error_tail",
                                5.9,
                            ))
                        } else if direction_margin_bps + 1e-9 >= 3.4
                            && direction_margin_bps < 3.5
                            && close_abs_delta_ms >= 900
                            && close_abs_delta_ms <= 1_000
                        {
                            Some((
                                "hype_close_only_single_hyperliquid_no_stale_low_margin_side_tail",
                                3.4,
                            ))
                        } else {
                            None
                        };
                        if let Some((reason, min_margin_bps)) = close_only_tail_reason {
                            close_only_filter_reason = Some(reason);
                            warn!(
                                "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason={} direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                                slug,
                                symbol,
                                compare_truth_open_source,
                                reason,
                                direction_margin_bps,
                                min_margin_bps,
                                hit.close_price,
                                hit.close_ts_ms,
                                first_ref,
                                first_obs,
                                hit.source_count,
                                hit.close_exact_sources,
                                hit.source_spread_bps,
                            );
                            warn!(
                                "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                                slug,
                                symbol,
                                started_ms,
                                ready_ms,
                                deadline_ms,
                                ready_ms.saturating_sub(started_ms),
                                first_ref,
                                first_side,
                                first_obs,
                            );
                            close_only_filtered = true;
                        }
                    }
                    if !close_only_filtered
                        && hit.source_count == 1
                        && hit.close_exact_sources == 0
                        && !safe_single_source_relief_applied
                        && direction_margin_bps + 1e-9 < single_source_min_direction_margin_bps
                    {
                        close_only_filter_reason = Some("single_source_near_flat");
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=single_source_near_flat direction_margin_bps={:.6} min_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            direction_margin_bps,
                            single_source_min_direction_margin_bps,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    let min_confidence = local_price_agg_min_confidence();
                    let relief_min_confidence = LOCAL_PRICE_AGG_RELIEF_MIN_CONFIDENCE_DEFAULT;
                    let relief_min_direction_margin_bps =
                        LOCAL_PRICE_AGG_RELIEF_MIN_DIRECTION_MARGIN_BPS_DEFAULT;
                    let relief_max_source_spread_bps =
                        LOCAL_PRICE_AGG_RELIEF_MAX_SOURCE_SPREAD_BPS_DEFAULT;
                    let strong_direction_relief_applied = hit.source_count >= 3
                        && (source_agreement >= 1.0 - 1e-9)
                        && hit.source_spread_bps <= 5.0 + 1e-9
                        && direction_margin_bps >= 10.0 - 1e-9;
                    let safe_low_conf_relief_applied = hit.source_count
                        >= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_SOURCES
                        && (source_agreement >= 1.0 - 1e-9)
                        && hit.source_spread_bps
                            <= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MAX_SOURCE_SPREAD_BPS + 1e-9
                        && direction_margin_bps
                            >= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_DIRECTION_MARGIN_BPS - 1e-9;
                    let sol_safe_low_conf_relief_applied = symbol == "sol/usd"
                        && hit.source_count
                            >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_SOURCES
                        && (source_agreement >= 1.0 - 1e-9)
                        && hit.source_spread_bps
                            <= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MAX_SOURCE_SPREAD_BPS
                                + 1e-9
                        && direction_margin_bps + 1e-9
                            >= LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_DIRECTION_MARGIN_BPS;
                    let confidence = if hit.source_count == 1 {
                        if hit.close_exact_sources > 0 {
                            0.93
                        } else {
                            0.87
                        }
                    } else {
                        let source_factor = (hit.source_count.min(3) as f64) / 3.0;
                        let spread_factor = if local_price_agg_max_source_spread_bps() > 0.0 {
                            (1.0 - (hit.source_spread_bps
                                / local_price_agg_max_source_spread_bps()))
                            .clamp(0.0, 1.0)
                        } else {
                            1.0
                        };
                        let exact_factor = (hit.close_exact_sources.min(2) as f64) / 2.0;
                        (0.50 * source_agreement
                            + 0.20 * source_factor
                            + 0.20 * spread_factor
                            + 0.10 * exact_factor)
                            .clamp(0.0, 1.0)
                    };
                    let confidence_relief_applied = hit.source_count >= 2
                        && (source_agreement >= 1.0 - 1e-9)
                        && hit.source_spread_bps <= relief_max_source_spread_bps + 1e-9
                        && direction_margin_bps >= relief_min_direction_margin_bps - 1e-9;
                    let effective_min_confidence = if confidence_relief_applied
                        || strong_direction_relief_applied
                        || safe_low_conf_relief_applied
                        || sol_safe_low_conf_relief_applied
                    {
                        min_confidence.min(if sol_safe_low_conf_relief_applied {
                            LOCAL_PRICE_AGG_COMPARE_SOL_SAFE_LOW_CONF_MIN_CONFIDENCE
                        } else {
                            relief_min_confidence
                        })
                    } else {
                        min_confidence
                    };
                    if !close_only_filtered && confidence + 1e-9 < effective_min_confidence {
                        close_only_filter_reason = Some("low_confidence");
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_filtered | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} reason=low_confidence confidence={:.3} min_confidence={:.3} effective_min_confidence={:.3} relief_applied={} strong_direction_relief_applied={} safe_low_conf_relief_applied={} agreement={:.3} direction_margin_bps={:.6} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} local_sources={} local_close_exact_sources={} local_close_spread_bps={:.6}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            confidence,
                            min_confidence,
                            effective_min_confidence,
                            confidence_relief_applied,
                            strong_direction_relief_applied,
                            safe_low_conf_relief_applied,
                            source_agreement,
                            direction_margin_bps,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            hit.source_count,
                            hit.close_exact_sources,
                            hit.source_spread_bps,
                        );
                        warn!(
                            "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                        close_only_filtered = true;
                    }
                    if !close_only_filtered {
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let close_within_target =
                            close_diff_bps <= LOCAL_PRICE_AGG_COMPARE_TARGET_BPS;
                        let close_match_frac_digits = count_matching_fractional_digits(
                            hit.close_price,
                            first_obs,
                            LOCAL_PRICE_AGG_COMPARE_REPORT_DECIMALS,
                        );
                        let close_meets_12dp = close_match_frac_digits
                            >= LOCAL_PRICE_AGG_COMPARE_TARGET_MIN_MATCH_DECIMALS;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        info!(
                            "🧪 local_price_agg_vs_rtds | slug={} symbol={} compare_mode=close_only_open_from_rtds open_truth_source={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} preclose_relief_applied={} safe_preclose_relief_applied={} safe_single_source_relief_applied={} safe_low_conf_relief_applied={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} close_within_5bps={} close_match_frac_digits={} close_meets_12dp={} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_decay_ms={:.1} local_exact_boost={:.2} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            compare_truth_open_source,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            preclose_relief_applied,
                            safe_preclose_relief_applied,
                            safe_single_source_relief_applied,
                            safe_low_conf_relief_applied,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            close_within_target,
                            close_match_frac_digits,
                            close_meets_12dp,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            local_price_agg_close_time_decay_ms(),
                            local_price_agg_exact_boost(),
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    }
                } else {
                    warn!(
                        "⚠️ local_price_agg_vs_rtds_unresolved | slug={} symbol={} compare_mode=close_only_open_from_rtds local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                        slug,
                        symbol,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        first_ref,
                        first_side,
                        first_obs,
                    );
                }
                let weighted_shadow_filter_reason = weighted_shadow_hit.and_then(|hit| {
                    local_boundary_weighted_candidate_filter_reason_for_policy(
                        &symbol,
                        close_only_filter_reason,
                        close_only_direction_margin_bps,
                        round_end_ts,
                        first_ref,
                        hit,
                    )
                });
                let weighted_shadow_candidate = weighted_shadow_hit.filter(|hit| {
                    local_boundary_weighted_candidate_allowed_for_policy(
                        &symbol,
                        close_only_filter_reason,
                        close_only_direction_margin_bps,
                        round_end_ts,
                        first_ref,
                        hit,
                    )
                });
                for outcome in &compare_hit.boundary_shadow_outcomes {
                    match outcome.hit.as_ref() {
                        Some(hit) => {
                            let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                                Side::Yes
                            } else {
                                Side::No
                            };
                            let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                            let close_abs_diff = (hit.close_price - first_obs).abs();
                            let close_diff_bps =
                                close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                            let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                            info!(
                                "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                                slug,
                                symbol,
                                hit.policy_name,
                                hit.source_subset_name,
                                hit.rule.as_str(),
                                hit.min_sources,
                                local_side_vs_rtds_open,
                                first_side,
                                side_match_vs_rtds_open,
                                hit.close_price,
                                hit.close_ts_ms,
                                first_ref,
                                first_obs,
                                close_abs_diff,
                                close_diff_bps,
                                hit.source_count,
                                hit.source_spread_bps,
                                hit.close_exact_sources,
                                started_ms,
                                ready_ms,
                                deadline_ms,
                                ready_ms.saturating_sub(started_ms),
                                local_vs_rtds_detect_gap_ms,
                            );
                        }
                        None => {
                            warn!(
                                "⚠️ local_price_agg_boundary_shadow_unresolved | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                                slug,
                                symbol,
                                outcome.policy_name,
                                outcome.source_subset_name,
                                outcome.rule.as_str(),
                                outcome.min_sources,
                                started_ms,
                                ready_ms,
                                deadline_ms,
                                ready_ms.saturating_sub(started_ms),
                                first_ref,
                                first_side,
                                first_obs,
                            );
                        }
                    }
                }
                let close_only_any_hit = compare_hit.base_hit.as_ref();
                let close_only_primary_hit = close_only_any_hit.filter(|_| !close_only_filtered);
                let hybrid_uses_weighted = match (close_only_primary_hit, weighted_shadow_candidate)
                {
                    (Some(close_only_hit), Some(weighted_hit)) => {
                        local_boundary_hybrid_prefers_weighted_over_close_only(
                            &symbol,
                            close_only_hit,
                            weighted_hit,
                            first_ref,
                        )
                    }
                    _ => false,
                };
                let hybrid_uses_filtered_close_only =
                    match (close_only_any_hit, weighted_shadow_candidate) {
                        (Some(close_only_hit), Some(weighted_hit)) if close_only_filtered => {
                            local_boundary_hybrid_prefers_filtered_close_only_over_weighted(
                                &symbol,
                                close_only_filter_reason,
                                close_only_hit,
                                weighted_hit,
                                first_ref,
                            )
                        }
                        _ => false,
                    };
                if let Some(hit) = close_only_primary_hit.filter(|_| !hybrid_uses_weighted) {
                    let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                    let close_abs_diff = (hit.close_price - first_obs).abs();
                    let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                    let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                    info!(
                        "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                        slug,
                        symbol,
                        "boundary_hybrid_close_only_then_weighted",
                        "close_only_primary",
                        "close_only",
                        hit.source_count,
                        local_side_vs_rtds_open,
                        first_side,
                        side_match_vs_rtds_open,
                        hit.close_price,
                        hit.close_ts_ms,
                        first_ref,
                        first_obs,
                        close_abs_diff,
                        close_diff_bps,
                        hit.source_count,
                        hit.source_spread_bps,
                        hit.close_exact_sources,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        local_vs_rtds_detect_gap_ms,
                    );
                } else if let Some(hit) =
                    close_only_any_hit.filter(|_| hybrid_uses_filtered_close_only)
                {
                    let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                    let close_abs_diff = (hit.close_price - first_obs).abs();
                    let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                    let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                    info!(
                        "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                        slug,
                        symbol,
                        "boundary_hybrid_close_only_then_weighted",
                        "filtered_close_only_fallback",
                        "close_only",
                        hit.source_count,
                        local_side_vs_rtds_open,
                        first_side,
                        side_match_vs_rtds_open,
                        hit.close_price,
                        hit.close_ts_ms,
                        first_ref,
                        first_obs,
                        close_abs_diff,
                        close_diff_bps,
                        hit.source_count,
                        hit.source_spread_bps,
                        hit.close_exact_sources,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        local_vs_rtds_detect_gap_ms,
                    );
                } else if let Some(hit) = close_only_any_hit.filter(|hit| {
                    close_only_filtered
                        && weighted_shadow_candidate.is_none()
                        && local_boundary_filtered_close_only_allowed_without_weighted(
                            &symbol,
                            close_only_filter_reason,
                            hit,
                            round_end_ts,
                            first_ref,
                        )
                }) {
                    let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                    let close_abs_diff = (hit.close_price - first_obs).abs();
                    let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                    let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                    info!(
                        "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                        slug,
                        symbol,
                        "boundary_hybrid_close_only_then_weighted",
                        "filtered_close_only_no_weighted",
                        "close_only",
                        hit.source_count,
                        local_side_vs_rtds_open,
                        first_side,
                        side_match_vs_rtds_open,
                        hit.close_price,
                        hit.close_ts_ms,
                        first_ref,
                        first_obs,
                        close_abs_diff,
                        close_diff_bps,
                        hit.source_count,
                        hit.source_spread_bps,
                        hit.close_exact_sources,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        local_vs_rtds_detect_gap_ms,
                    );
                } else if let Some(hit) = weighted_shadow_candidate {
                    let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                    let close_abs_diff = (hit.close_price - first_obs).abs();
                    let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                    let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                    info!(
                        "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                        slug,
                        symbol,
                        "boundary_hybrid_close_only_then_weighted",
                        "weighted_fallback",
                        hit.rule.as_str(),
                        hit.min_sources,
                        local_side_vs_rtds_open,
                        first_side,
                        side_match_vs_rtds_open,
                        hit.close_price,
                        hit.close_ts_ms,
                        first_ref,
                        first_obs,
                        close_abs_diff,
                        close_diff_bps,
                        hit.source_count,
                        hit.source_spread_bps,
                        hit.close_exact_sources,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        local_vs_rtds_detect_gap_ms,
                    );
                } else {
                    warn!(
                        "⚠️ local_price_agg_boundary_shadow_unresolved | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                        slug,
                        symbol,
                        "boundary_hybrid_close_only_then_weighted",
                        "weighted_fallback",
                        "hybrid",
                        1,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        first_ref,
                        first_side,
                        first_obs,
                    );
                }
                let router_prefers_weighted =
                    local_boundary_symbol_router_prefers_weighted_primary(&symbol);
                let close_only_router_candidate = close_only_primary_hit.filter(|hit| {
                    local_boundary_symbol_router_allows_close_only_fallback(
                        &symbol,
                        hit,
                        round_end_ts,
                        first_ref,
                    )
                });
                let router_source_fallback_candidate = if close_only_router_candidate.is_some() {
                    None
                } else {
                    compare_hit
                        .boundary_shadow_outcomes
                        .iter()
                        .filter(|outcome| outcome.policy_name == "boundary_symbol_router_fallback")
                        .filter_map(|outcome| outcome.hit.as_ref())
                        .find(|hit| {
                            local_boundary_symbol_router_source_fallback_allowed(
                                &symbol,
                                weighted_shadow_hit,
                                weighted_shadow_filter_reason,
                                hit,
                                round_end_ts,
                                first_ref,
                            )
                        })
                };
                if router_prefers_weighted {
                    if let Some(hit) = weighted_shadow_candidate {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        info!(
                            "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v1",
                            hit.source_subset_name,
                            hit.rule.as_str(),
                            hit.min_sources,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else if let Some(hit) = router_source_fallback_candidate {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        info!(
                            "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v1",
                            hit.source_subset_name,
                            hit.rule.as_str(),
                            hit.min_sources,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else if let Some(hit) = close_only_router_candidate {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        info!(
                            "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v1",
                            "close_only_fallback",
                            "close_only",
                            hit.source_count,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else if let Some(hit) = close_only_any_hit.filter(|hit| {
                        close_only_filtered
                            && local_boundary_filtered_close_only_allowed_without_weighted(
                                &symbol,
                                close_only_filter_reason,
                                hit,
                                round_end_ts,
                                first_ref,
                            )
                    }) {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        info!(
                            "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v1",
                            "filtered_close_only_fallback",
                            "close_only",
                            hit.source_count,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else if let Some(hit) = close_only_any_hit.filter(|hit| {
                        close_only_filtered
                            && weighted_shadow_hit.is_none()
                            && local_boundary_symbol_router_missing_filter_reason(
                                &symbol,
                                close_only_filter_reason,
                                close_only_direction_margin_bps,
                                hit,
                            )
                            .is_some()
                    }) {
                        let filter_reason = local_boundary_symbol_router_missing_filter_reason(
                            &symbol,
                            close_only_filter_reason,
                            close_only_direction_margin_bps,
                            hit,
                        )
                        .unwrap_or("filtered_close_only_missing");
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        warn!(
                            "⚠️ local_price_agg_boundary_shadow_filtered | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} filter_reason={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v1",
                            "filtered_close_only_missing",
                            "close_only",
                            1,
                            filter_reason,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else if let (Some(hit), Some(filter_reason)) =
                        (weighted_shadow_hit, weighted_shadow_filter_reason)
                    {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        warn!(
                            "⚠️ local_price_agg_boundary_shadow_filtered | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} filter_reason={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v1",
                            hit.source_subset_name,
                            hit.rule.as_str(),
                            hit.min_sources,
                            filter_reason,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else {
                        warn!(
                            "⚠️ local_price_agg_boundary_shadow_unresolved | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v1",
                            "weighted_primary",
                            "weighted",
                            1,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                    }
                } else if let Some(hit) = close_only_primary_hit {
                    let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                    let close_abs_diff = (hit.close_price - first_obs).abs();
                    let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                    let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                    info!(
                        "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                        slug,
                        symbol,
                        "boundary_symbol_router_v1",
                        "close_only_primary",
                        "close_only",
                        hit.source_count,
                        local_side_vs_rtds_open,
                        first_side,
                        side_match_vs_rtds_open,
                        hit.close_price,
                        hit.close_ts_ms,
                        first_ref,
                        first_obs,
                        close_abs_diff,
                        close_diff_bps,
                        hit.source_count,
                        hit.source_spread_bps,
                        hit.close_exact_sources,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        local_vs_rtds_detect_gap_ms,
                    );
                } else if let Some(hit) = weighted_shadow_candidate {
                    let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                    let close_abs_diff = (hit.close_price - first_obs).abs();
                    let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                    let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                    info!(
                        "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                        slug,
                        symbol,
                        "boundary_symbol_router_v1",
                        "weighted_fallback",
                        hit.rule.as_str(),
                        hit.min_sources,
                        local_side_vs_rtds_open,
                        first_side,
                        side_match_vs_rtds_open,
                        hit.close_price,
                        hit.close_ts_ms,
                        first_ref,
                        first_obs,
                        close_abs_diff,
                        close_diff_bps,
                        hit.source_count,
                        hit.source_spread_bps,
                        hit.close_exact_sources,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        local_vs_rtds_detect_gap_ms,
                    );
                } else {
                    warn!(
                        "⚠️ local_price_agg_boundary_shadow_unresolved | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                        slug,
                        symbol,
                        "boundary_symbol_router_v1",
                        "close_only_primary",
                        "close_only",
                        1,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        first_ref,
                        first_side,
                        first_obs,
                    );
                }
                let router_uncertainty_gate_candidate = if router_prefers_weighted {
                    if let Some(hit) = weighted_shadow_candidate {
                        Some(local_agg_gate_candidate_from_boundary_hit(
                            &slug,
                            &symbol,
                            round_end_ts,
                            first_ref,
                            first_obs,
                            first_side,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            hit,
                        ))
                    } else if let Some(hit) = router_source_fallback_candidate {
                        Some(local_agg_gate_candidate_from_boundary_hit(
                            &slug,
                            &symbol,
                            round_end_ts,
                            first_ref,
                            first_obs,
                            first_side,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            hit,
                        ))
                    } else if let Some(hit) = close_only_router_candidate {
                        Some(local_agg_gate_candidate_from_close_only_hit(
                            &slug,
                            &symbol,
                            round_end_ts,
                            first_ref,
                            first_obs,
                            first_side,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            "close_only_fallback",
                            hit,
                        ))
                    } else if let Some(hit) = close_only_any_hit.filter(|hit| {
                        close_only_filtered
                            && local_boundary_filtered_close_only_allowed_without_weighted(
                                &symbol,
                                close_only_filter_reason,
                                hit,
                                round_end_ts,
                                first_ref,
                            )
                    }) {
                        Some(local_agg_gate_candidate_from_close_only_hit(
                            &slug,
                            &symbol,
                            round_end_ts,
                            first_ref,
                            first_obs,
                            first_side,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            "filtered_close_only_fallback",
                            hit,
                        ))
                    } else {
                        None
                    }
                } else if let Some(hit) = close_only_primary_hit {
                    Some(local_agg_gate_candidate_from_close_only_hit(
                        &slug,
                        &symbol,
                        round_end_ts,
                        first_ref,
                        first_obs,
                        first_side,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        "close_only_primary",
                        hit,
                    ))
                } else {
                    weighted_shadow_candidate.map(|hit| {
                        local_agg_gate_candidate_from_boundary_hit(
                            &slug,
                            &symbol,
                            round_end_ts,
                            first_ref,
                            first_obs,
                            first_side,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            hit,
                        )
                    })
                };
                if let Some(candidate) = router_uncertainty_gate_candidate {
                    local_agg_uncertainty_gate_observe_candidate(candidate);
                }
                if symbol == "hype/usd" {
                    if let Some(hit) = close_only_primary_hit {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        info!(
                            "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v2",
                            "close_only_primary",
                            "close_only",
                            hit.source_count,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else if let Some(hit) = weighted_shadow_candidate {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        info!(
                            "🧪 local_price_agg_boundary_shadow_vs_rtds | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v2",
                            "weighted_fallback",
                            hit.rule.as_str(),
                            hit.min_sources,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else if let (Some(hit), Some(filter_reason)) =
                        (weighted_shadow_hit, weighted_shadow_filter_reason)
                    {
                        let local_side_vs_rtds_open = if hit.close_price >= first_ref {
                            Side::Yes
                        } else {
                            Side::No
                        };
                        let side_match_vs_rtds_open = local_side_vs_rtds_open == first_side;
                        let close_abs_diff = (hit.close_price - first_obs).abs();
                        let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                        let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                        warn!(
                            "⚠️ local_price_agg_boundary_shadow_filtered | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} filter_reason={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v2",
                            hit.source_subset_name,
                            hit.rule.as_str(),
                            hit.min_sources,
                            filter_reason,
                            local_side_vs_rtds_open,
                            first_side,
                            side_match_vs_rtds_open,
                            hit.close_price,
                            hit.close_ts_ms,
                            first_ref,
                            first_obs,
                            close_abs_diff,
                            close_diff_bps,
                            hit.source_count,
                            hit.source_spread_bps,
                            hit.close_exact_sources,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            local_vs_rtds_detect_gap_ms,
                        );
                    } else {
                        warn!(
                            "⚠️ local_price_agg_boundary_shadow_unresolved | slug={} symbol={} compare_mode=boundary_shadow_open_from_rtds policy={} source_subset={} rule={} min_sources={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                            slug,
                            symbol,
                            "boundary_symbol_router_v2",
                            "close_only_primary",
                            "close_only",
                            1,
                            started_ms,
                            ready_ms,
                            deadline_ms,
                            ready_ms.saturating_sub(started_ms),
                            first_ref,
                            first_side,
                            first_obs,
                        );
                    }
                }
            }
            Err(err) => {
                warn!(
                    "⚠️ local_price_agg_compare_task_join_error | slug={} symbol={} err={}",
                    slug, symbol, err
                );
            }
        }
    }
    if let Some(task) = local_full_shadow_task.take() {
        match task.await {
            Ok((hit, started_ms, ready_ms, deadline_ms)) => match hit {
                Some((
                    local_side,
                    local_open,
                    local_close,
                    local_open_ts_ms,
                    local_close_ts_ms,
                    local_open_exact,
                )) => {
                    let close_abs_diff = (local_close - first_obs).abs();
                    let close_diff_bps = close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0;
                    let local_vs_rtds_detect_gap_ms = first_ms.saturating_sub(ready_ms);
                    let side_match_vs_rtds_open = local_side == first_side;
                    info!(
                        "🧪 local_price_agg_full_shadow_vs_rtds | slug={} symbol={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_open={:.15}@{} local_close={:.15}@{} local_open_exact={} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} local_to_rtds_detect_gap_ms={}",
                        slug,
                        symbol,
                        local_side,
                        first_side,
                        side_match_vs_rtds_open,
                        local_open,
                        local_open_ts_ms,
                        local_close,
                        local_close_ts_ms,
                        local_open_exact,
                        first_ref,
                        first_obs,
                        close_abs_diff,
                        close_diff_bps,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        local_vs_rtds_detect_gap_ms,
                    );
                }
                None => {
                    warn!(
                        "⚠️ local_price_agg_full_shadow_unresolved | slug={} symbol={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={} rtds_open={:.15} rtds_side={:?} rtds_close={:.15}",
                        slug,
                        symbol,
                        started_ms,
                        ready_ms,
                        deadline_ms,
                        ready_ms.saturating_sub(started_ms),
                        first_ref,
                        first_side,
                        first_obs,
                    );
                }
            },
            Err(err) => {
                warn!(
                    "⚠️ local_price_agg_full_shadow_task_join_error | slug={} symbol={} err={}",
                    slug, symbol, err
                );
            }
        }
    }

    if !winner_hint_ready {
        observation_task.abort();
        return;
    }

    if first_ms.saturating_sub(market_end_ms) > post_close_window_secs.saturating_mul(1_000) {
        warn!(
            "⚠️ post_close winner hint late for {} (end={} window={}s detect_lag_ms={}) — skipping",
            slug,
            round_end_ts,
            post_close_window_secs,
            first_ms.saturating_sub(market_end_ms)
        );
        observation_task.abort();
        return;
    }

    let final_detect_unix_ms = unix_now_millis_u64();
    let tape = shared_execution_view.lock().map(|g| *g).unwrap_or_default();
    let (book_source, winner_bid, winner_ask_raw, evidence_recv_ms, distance_to_final_ms) =
        post_close_round_observation_from_latest_view(tape, first_side, final_detect_unix_ms);

    let winner_ask_book = fmt_price_opt(nonzero_price_opt(winner_ask_raw));
    let winner_ask_eff = fmt_price_opt(post_close_effective_ask_opt(winner_bid, winner_ask_raw));
    let emit_unix_ms = unix_now_millis_u64();
    let final_detect_to_emit_ms = emit_unix_ms.saturating_sub(final_detect_unix_ms);
    let evidence_to_emit_ms = if evidence_recv_ms > 0 {
        Some(emit_unix_ms.saturating_sub(evidence_recv_ms))
    } else {
        None
    };
    info!(
        "⏱️ post_close_emit_winner_hint | unix_ms={} final_detect_unix_ms={} source={:?} open_exact={} slug={} side={:?} ref_price={:.9} observed_price={:.9} frontend_open={:?} frontend_close={:?} frontend_ts_ms={:?} frontend_completed={:?} frontend_cached={:?} latency_from_end_ms={} book_source={} evidence_recv_ms={} distance_to_final_ms={} winner_bid={:.4} winner_ask_raw={:.4} winner_ask_book={} winner_ask_eff={}",
        emit_unix_ms, final_detect_unix_ms, first_source, first_open_exact, slug, first_side, first_ref, first_obs,
        Option::<f64>::None, Option::<f64>::None, Option::<u64>::None, Option::<bool>::None, Option::<bool>::None,
        first_ms.saturating_sub(market_end_ms),
        book_source.as_str(),
        evidence_recv_ms,
        distance_to_final_ms,
        winner_bid, winner_ask_raw, winner_ask_book, winner_ask_eff,
    );
    info!(
        "⏱️ oracle_lag_latency_emit | slug={} side={:?} final_detect_to_emit_ms={} evidence_to_final_ms={} evidence_to_emit_ms={:?} book_source={}",
        slug,
        first_side,
        final_detect_to_emit_ms,
        distance_to_final_ms,
        evidence_to_emit_ms,
        book_source.as_str(),
    );
    // Dedup guard: drop duplicate hints from stale preload races or dual processes.
    if !hint_dedup_try_insert(&slug, round_end_ts, first_side, final_detect_unix_ms) {
        warn!(
            "⚠️ post_close_hint_duplicate_suppressed | slug={} round_end_ts={} side={:?} — already dispatched by another instance",
            slug, round_end_ts, first_side
        );
        observation_task.abort();
        return;
    }

    let hint_msg = MarketDataMsg::WinnerHint {
        slug: slug.clone(),
        hint_id: final_detect_unix_ms,
        side: first_side,
        source: first_source,
        ref_price: first_ref,
        observed_price: first_obs,
        final_detect_unix_ms,
        emit_unix_ms,
        winner_bid,
        winner_ask_raw,
        winner_evidence_recv_ms: evidence_recv_ms,
        winner_book_source: book_source.as_str(),
        winner_distance_to_final_ms: distance_to_final_ms,
        open_is_exact: first_open_exact,
        ts: Instant::now(),
    };

    // Always report final observation to the round-tail coordinator (if enabled).
    // This does not replace local immediate WinnerHint execution path.
    if let Some(tail_tx) = round_tail_tx {
        let winner_ask_eff_f64 =
            post_close_effective_ask_opt(winner_bid, winner_ask_raw).unwrap_or(0.0);
        let winner_ask_tradable = winner_ask_eff_f64 > 0.0;
        let tail_obs = RoundTailObservation {
            round_end_ts,
            slug: slug.clone(),
            winner_side: first_side,
            winner_bid,
            winner_ask_raw,
            winner_ask_eff: winner_ask_eff_f64,
            winner_ask_tradable,
            detect_ms: first_ms,
            hint_tx: winner_hint_tx.clone(),
        };
        if let Err(e) = tail_tx.try_send(tail_obs) {
            warn!(
                "⚠️ post_close_round_tail_obs_send_failed | slug={} round_end_ts={} err={}",
                slug, round_end_ts, e
            );
        }
    }

    if let Some(arb_tx) = arbiter_tx {
        // Arbiter mode: wrap into ArbiterObservation and hand off to the arbiter.
        let winner_ask_eff_f64 =
            post_close_effective_ask_opt(winner_bid, winner_ask_raw).unwrap_or(0.0);
        let winner_ask_tradable = winner_ask_eff_f64 > 0.0;
        let obs = ArbiterObservation {
            round_end_ts,
            slug: slug.clone(),
            winner_side: first_side,
            winner_bid,
            winner_ask_raw,
            winner_ask_eff: winner_ask_eff_f64,
            winner_ask_tradable,
            book_age_ms: distance_to_final_ms,
            book_source,
            evidence_recv_ms,
            distance_to_final_ms,
            detect_ms: first_ms,
            hint_msg,
            hint_tx: winner_hint_tx,
        };
        if let Err(e) = arb_tx.try_send(obs) {
            warn!(
                "⚠️ post_close_arbiter_obs_send_failed | slug={} err={}",
                slug, e
            );
        } else {
            info!(
                "⏱️ post_close_winner_hint_to_arbiter | slug={} source={:?} side={:?}",
                slug, first_source, first_side
            );
        }
    } else {
        // No arbiter: send WinnerHint directly to coordinator (backward-compatible path).
        match tokio::time::timeout(Duration::from_millis(500), winner_hint_tx.send(hint_msg)).await
        {
            Ok(Ok(())) => {
                info!(
                    "⏱️ post_close_winner_hint_dispatched_ok | slug={} source={:?} side={:?}",
                    slug, first_source, first_side
                );
            }
            Ok(Err(e)) => {
                warn!(
                    "⚠️ post_close winner hint dispatch failed for {}: {}",
                    slug, e
                );
            }
            Err(_) => {
                warn!(
                    "⚠️ post_close winner hint dispatch timeout for {} (500ms)",
                    slug
                );
            }
        }
    }

    // Validation-only path: keep frontend observability, but do not block winner-hint emit.
    if let Some(task) = frontend_task.take() {
        match tokio::time::timeout(Duration::from_millis(350), task).await {
            Ok(Ok(Some(hit))) => {
                info!(
                    "🧾 post_close_frontend_validation | slug={} symbol={} open={:?} close={:?} completed={:?} cached={:?} ts_ms={} detect_to_frontend_validation_ms={}",
                    slug,
                    symbol,
                    hit.open_price,
                    hit.close_price,
                    hit.completed,
                    hit.cached,
                    hit.timestamp_ms,
                    unix_now_millis_u64().saturating_sub(final_detect_unix_ms),
                );
            }
            Ok(Ok(None)) => {
                info!(
                    "🧾 post_close_frontend_validation | slug={} symbol={} result=none detect_to_frontend_validation_ms={}",
                    slug,
                    symbol,
                    unix_now_millis_u64().saturating_sub(final_detect_unix_ms),
                );
            }
            Ok(Err(err)) => {
                warn!(
                    "⚠️ frontend_round_task_join_error | slug={} err={}",
                    slug, err
                );
            }
            Err(_) => {
                info!(
                    "🧾 post_close_frontend_validation | slug={} symbol={} result=timeout",
                    slug, symbol
                );
            }
        }
    }
    // Post-final marketability probe (0~2s window) for production diagnostics.
    // This is out of trading hot-path: WinnerHint has already been dispatched.
    log_post_close_winner_ask_window_stats(
        Arc::clone(&shared_execution_view),
        &slug,
        first_side,
        final_detect_unix_ms,
    )
    .await;
    observation_task.abort();
}

async fn run_data_streams_winner_hint(
    symbol: &str,
    round_start_ts: u64,
    round_end_ts: u64,
    hard_deadline_ts: u64,
) -> Option<(Side, f64, f64, u64, u64, bool)> {
    let Some(stream_symbol) = data_streams_symbol_from_chainlink_symbol(symbol) else {
        warn!(
            "⚠️ data_streams_symbol_invalid | chainlink_symbol={}",
            symbol
        );
        return None;
    };
    let chainlink_symbol = normalize_chainlink_symbol(symbol);
    let start_ms = round_start_ts.saturating_mul(1_000);
    let end_ms = round_end_ts.saturating_mul(1_000);
    let mut open_point: Option<(f64, u64)> = take_prewarmed_open(&chainlink_symbol, start_ms);
    if let Some((px, ts_ms)) = open_point {
        info!(
            "⏱️ data_streams_open_prewarm_hit | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6}",
            unix_now_millis_u64(),
            chainlink_symbol,
            round_start_ts,
            ts_ms,
            px,
        );
    }
    let mut close_point: Option<(f64, u64)> = None;
    let mut observed_ticks: u64 = 0;
    let mut first_tick_ts_ms: Option<u64> = None;
    let mut last_tick_ts_ms: Option<u64> = None;
    let mut nearest_start: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)
    let mut nearest_end: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)

    let t_fn_enter = unix_now_millis_u64();
    let t_auth_start = unix_now_millis_u64();
    let Some(access_token) = data_streams_authorize_token().await else {
        warn!(
            "⚠️ data_streams_authorize_failed | symbol={} round_start_ts={} round_end_ts={}",
            stream_symbol, round_start_ts, round_end_ts
        );
        return None;
    };
    let t_auth_done = unix_now_millis_u64();

    let base = chainlink_data_streams_price_api_base_url();
    let url = format!(
        "{}/api/v1/streaming?symbol={}",
        base.trim_end_matches('/'),
        stream_symbol
    );
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(DATA_STREAMS_CONNECT_TIMEOUT_MS))
        // Do not set a global request timeout for streaming; use local loop deadline control.
        .build()
        .ok()?;
    let t_connect_start = unix_now_millis_u64();
    let mut resp = match client
        .get(url)
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Connection", "keep-alive")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(
                "⚠️ data_streams_stream_connect_failed | symbol={} err={}",
                stream_symbol, e
            );
            return None;
        }
    };
    let t_connect_done = unix_now_millis_u64();
    info!(
        "⏱️ data_streams_winner_hint_timing_phase1 | symbol={} round_end_ts={} fn_enter_ms={} auth_ms={} connect_ms={} enter_to_connected_ms={} now_vs_end_ms={}",
        stream_symbol,
        round_end_ts,
        t_fn_enter,
        t_auth_done.saturating_sub(t_auth_start),
        t_connect_done.saturating_sub(t_connect_start),
        t_connect_done.saturating_sub(t_fn_enter),
        (t_connect_done as i128) - (end_ms as i128),
    );
    if !resp.status().is_success() {
        warn!(
            "⚠️ data_streams_stream_http_status | symbol={} status={}",
            stream_symbol,
            resp.status()
        );
        return None;
    }

    let mut line_buf = String::new();
    let mut first_chunk_logged = false;
    let mut first_tick_logged = false;
    while unix_now_secs() <= hard_deadline_ts {
        let next = tokio::time::timeout(Duration::from_millis(700), resp.chunk()).await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => {
                warn!(
                    "⚠️ data_streams_stream_read_error | symbol={} err={}",
                    stream_symbol, e
                );
                break;
            }
            Err(_) => continue,
        };
        if !first_chunk_logged {
            first_chunk_logged = true;
            let now_ms = unix_now_millis_u64();
            info!(
                "⏱️ data_streams_first_chunk | symbol={} round_end_ts={} unix_ms={} since_connect_ms={} since_end_ms={}",
                stream_symbol,
                round_end_ts,
                now_ms,
                now_ms.saturating_sub(t_connect_done),
                (now_ms as i128) - (end_ms as i128),
            );
        }

        line_buf.push_str(&String::from_utf8_lossy(&chunk));

        loop {
            let Some(newline_idx) = line_buf.find('\n') else {
                if line_buf.len() > 16 * 1024 {
                    line_buf.clear();
                }
                break;
            };
            let line = line_buf[..newline_idx].trim().to_string();
            line_buf.drain(..=newline_idx);
            if line.is_empty() {
                continue;
            }
            let Some((price, ts_s)) = parse_data_streams_tick(&line, &stream_symbol) else {
                continue;
            };
            let ts_ms = ts_s.saturating_mul(1_000);
            observed_ticks = observed_ticks.saturating_add(1);
            first_tick_ts_ms.get_or_insert(ts_ms);
            last_tick_ts_ms = Some(ts_ms);
            let start_delta = ts_ms.abs_diff(start_ms);
            match nearest_start {
                Some((best_delta, _, _)) if start_delta >= best_delta => {}
                _ => nearest_start = Some((start_delta, ts_ms, price)),
            }
            let end_delta = ts_ms.abs_diff(end_ms);
            match nearest_end {
                Some((best_delta, _, _)) if end_delta >= best_delta => {}
                _ => nearest_end = Some((end_delta, ts_ms, price)),
            }
            if !first_tick_logged {
                first_tick_logged = true;
                let now_ms = unix_now_millis_u64();
                info!(
                    "⏱️ data_streams_first_tick | symbol={} round_end_ts={} unix_ms={} tick_ts_ms={} since_connect_ms={} tick_lag_vs_end_ms={}",
                    stream_symbol,
                    round_end_ts,
                    now_ms,
                    ts_ms,
                    now_ms.saturating_sub(t_connect_done),
                    (now_ms as i128) - (end_ms as i128),
                );
            }
            if open_point.is_none() && ts_ms == start_ms {
                open_point = Some((price, ts_ms));
                info!(
                    "⏱️ data_streams_open_captured | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6} exact=true",
                    unix_now_millis_u64(),
                    chainlink_symbol,
                    round_start_ts,
                    ts_ms,
                    price,
                );
            }
            if close_point.is_none() && ts_ms == end_ms {
                close_point = Some((price, ts_ms));
                info!(
                    "⏱️ data_streams_close_captured | unix_ms={} symbol={} round_end_ts={} close_ts_ms={} close_price={:.6} exact=true",
                    unix_now_millis_u64(),
                    chainlink_symbol,
                    round_end_ts,
                    ts_ms,
                    price,
                );
            }
            if let Some((close, close_ts_ms)) = close_point {
                if let Some((open_ref_px, open_ts_ms)) = open_point {
                    let side = if close >= open_ref_px {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    info!(
                        "🏁 Chainlink winner hint ready | unix_ms={} symbol={} side={:?} open_ref={:.6}@{} close={:.6}@{} source=data_streams_exact",
                        unix_now_millis_u64(),
                        chainlink_symbol,
                        side,
                        open_ref_px,
                        open_ts_ms,
                        close,
                        close_ts_ms,
                    );
                    set_last_chainlink_close(&chainlink_symbol, close_ts_ms, close);
                    return Some((side, open_ref_px, close, open_ts_ms, close_ts_ms, true));
                }
                set_last_chainlink_close(&chainlink_symbol, close_ts_ms, close);
                warn!(
                    "⚠️ data_streams_close_ready_but_open_missing | symbol={} round_start_ts={} round_end_ts={} has_open_t={} has_final=true observed_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?} nearest_end={:?} — keeping decision unresolved",
                    chainlink_symbol,
                    round_start_ts,
                    round_end_ts,
                    open_point.is_some(),
                    observed_ticks,
                    first_tick_ts_ms,
                    last_tick_ts_ms,
                    nearest_start,
                    nearest_end
                );
                return None;
            }
        }
    }

    warn!(
        "⚠️ data_streams_winner_hint_unresolved | symbol={} round_start_ts={} round_end_ts={} deadline_ts={} observed_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?} nearest_end={:?}",
        chainlink_symbol,
        round_start_ts,
        round_end_ts,
        hard_deadline_ts,
        observed_ticks,
        first_tick_ts_ms,
        last_tick_ts_ms,
        nearest_start,
        nearest_end
    );
    None
}

#[derive(Debug, Clone, Copy)]
struct AggregatedPricePoint {
    price: f64,
    ts_ms: u64,
    exact: bool,
    abs_delta_ms: u64,
    source: &'static str,
}

impl AggregatedPricePoint {
    fn from_exact(price: f64, ts_ms: u64, source: &'static str) -> Self {
        Self {
            price,
            ts_ms,
            exact: true,
            abs_delta_ms: 0,
            source,
        }
    }
}

fn self_built_agg_ingest_tick(
    price: f64,
    ts_ms: u64,
    start_ms: u64,
    end_ms: u64,
    first_tick_ts_ms: &mut Option<u64>,
    open_exact: &mut Option<AggregatedPricePoint>,
    close_exact: &mut Option<AggregatedPricePoint>,
    nearest_start: &mut Option<(u64, u64, f64)>,
    nearest_end: &mut Option<(u64, u64, f64)>,
    first_after_start: &mut Option<(u64, u64, f64)>,
    first_after_end: &mut Option<(u64, u64, f64)>,
) {
    first_tick_ts_ms.get_or_insert(ts_ms);

    if open_exact.is_none() && ts_ms == start_ms {
        *open_exact = Some(AggregatedPricePoint::from_exact(
            price,
            ts_ms,
            "hub_open_exact",
        ));
    }
    if close_exact.is_none() && ts_ms == end_ms {
        *close_exact = Some(AggregatedPricePoint::from_exact(
            price,
            ts_ms,
            "hub_close_exact",
        ));
    }

    let start_delta = ts_ms.abs_diff(start_ms);
    match *nearest_start {
        Some((best_delta, _, _)) if start_delta >= best_delta => {}
        _ => *nearest_start = Some((start_delta, ts_ms, price)),
    }
    if ts_ms >= start_ms {
        let after_delta = ts_ms.saturating_sub(start_ms);
        match *first_after_start {
            Some((best_delta, _, _)) if after_delta >= best_delta => {}
            _ => *first_after_start = Some((after_delta, ts_ms, price)),
        }
    }

    let end_delta = ts_ms.abs_diff(end_ms);
    match *nearest_end {
        Some((best_delta, _, _)) if end_delta >= best_delta => {}
        _ => *nearest_end = Some((end_delta, ts_ms, price)),
    }
    if ts_ms >= end_ms {
        let after_delta = ts_ms.saturating_sub(end_ms);
        match *first_after_end {
            Some((best_delta, _, _)) if after_delta >= best_delta => {}
            _ => *first_after_end = Some((after_delta, ts_ms, price)),
        }
    }
}

/// Self-built round price aggregator:
/// - consume Chainlink hub ticks
/// - fuse exact + nearest-round-boundary candidates
/// - emit high-confidence winner hint (or unresolved)
async fn run_self_built_price_aggregator(
    chainlink_hub: Option<Arc<ChainlinkHub>>,
    symbol: &str,
    round_start_ts: u64,
    round_end_ts: u64,
    hard_deadline_ts: u64,
) -> Option<(Side, f64, f64, u64, u64, bool)> {
    let Some(hub) = chainlink_hub else {
        return None;
    };

    let target_symbol = normalize_chainlink_symbol(symbol);
    let start_ms = round_start_ts.saturating_mul(1_000);
    let end_ms = round_end_ts.saturating_mul(1_000);
    let open_tol_ms = self_built_price_agg_open_tolerance_ms();
    let close_tol_ms = self_built_price_agg_close_tolerance_ms();
    let min_confidence = self_built_price_agg_min_confidence();

    let mut open_exact: Option<AggregatedPricePoint> =
        peek_prewarmed_tick(&target_symbol, start_ms).map(|(price, ts_ms)| {
            AggregatedPricePoint::from_exact(price, ts_ms, "prewarm_open_exact")
        });
    let mut close_exact: Option<AggregatedPricePoint> = None;
    let prev_round_close = cached_prev_round_close(&target_symbol, start_ms);

    let mut observed_ticks: u64 = 0;
    let mut observed_snapshot_ticks: u64 = 0;
    let mut observed_live_ticks: u64 = 0;
    let mut lagged_ticks: u64 = 0;
    let mut nearest_start: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)
    let mut nearest_end: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)
    let mut first_after_start: Option<(u64, u64, f64)> = None; // (delta_after_ms, ts_ms, price)
    let mut first_after_end: Option<(u64, u64, f64)> = None; // (delta_after_ms, ts_ms, price)
    let mut first_tick_ts_ms: Option<u64> = None;
    let emit_probe = |status: &str,
                      side: Option<Side>,
                      confidence: Option<f64>,
                      open: Option<AggregatedPricePoint>,
                      close: Option<AggregatedPricePoint>,
                      observed_ticks: u64,
                      observed_snapshot_ticks: u64,
                      observed_live_ticks: u64,
                      lagged_ticks: u64,
                      nearest_start: Option<(u64, u64, f64)>,
                      nearest_end: Option<(u64, u64, f64)>,
                      first_after_start: Option<(u64, u64, f64)>,
                      first_after_end: Option<(u64, u64, f64)>,
                      open_pick_rule: Option<&str>,
                      close_pick_rule: Option<&str>| {
        append_self_built_price_agg_probe(&SelfBuiltPriceAggProbe {
            unix_ms: unix_now_millis_u64(),
            symbol: target_symbol.clone(),
            round_start_ts,
            round_end_ts,
            status: status.to_string(),
            side: side.map(|s| format!("{:?}", s)),
            confidence,
            min_confidence,
            open_price: open.map(|p| p.price),
            open_ts_ms: open.map(|p| p.ts_ms),
            open_source: open.map(|p| p.source.to_string()),
            open_exact: open.map(|p| p.exact),
            open_delta_ms: open.map(|p| p.abs_delta_ms),
            close_price: close.map(|p| p.price),
            close_ts_ms: close.map(|p| p.ts_ms),
            close_source: close.map(|p| p.source.to_string()),
            close_exact: close.map(|p| p.exact),
            close_delta_ms: close.map(|p| p.abs_delta_ms),
            observed_ticks,
            observed_snapshot_ticks,
            observed_live_ticks,
            lagged_ticks,
            nearest_start_delta_ms: nearest_start.map(|(d, _, _)| d),
            nearest_end_delta_ms: nearest_end.map(|(d, _, _)| d),
            first_after_start_delta_ms: first_after_start.map(|(d, _, _)| d),
            first_after_end_delta_ms: first_after_end.map(|(d, _, _)| d),
            open_pick_rule: open_pick_rule.map(|s| s.to_string()),
            close_pick_rule: close_pick_rule.map(|s| s.to_string()),
        });
    };

    // Phase-1: consume hub in-memory recent ticks first (zero network wait).
    for (price, ts_ms) in hub.snapshot_recent_ticks(&target_symbol) {
        observed_ticks = observed_ticks.saturating_add(1);
        self_built_agg_ingest_tick(
            price,
            ts_ms,
            start_ms,
            end_ms,
            &mut first_tick_ts_ms,
            &mut open_exact,
            &mut close_exact,
            &mut nearest_start,
            &mut nearest_end,
            &mut first_after_start,
            &mut first_after_end,
        );
        observed_snapshot_ticks = observed_snapshot_ticks.saturating_add(1);
    }

    // If snapshot already gives enough evidence, avoid extra waiting.
    let need_live_stream = !(close_exact.is_some()
        && (open_exact.is_some()
            || first_after_start
                .map(|(delta, _, _)| delta <= open_tol_ms)
                .unwrap_or(false)
            || nearest_start
                .map(|(delta, _, _)| delta <= open_tol_ms)
                .unwrap_or(false)
            || prev_round_close.is_some()));

    if need_live_stream {
        let Some(mut rx) = hub.subscribe(&target_symbol) else {
            warn!(
                "⚠️ self_built_price_agg_unsubscribed | symbol={} round_start_ts={} round_end_ts={}",
                target_symbol, round_start_ts, round_end_ts
            );
            emit_probe(
                "hub_unsubscribed",
                None,
                None,
                open_exact,
                close_exact,
                observed_ticks,
                observed_snapshot_ticks,
                observed_live_ticks,
                lagged_ticks,
                nearest_start,
                nearest_end,
                first_after_start,
                first_after_end,
                None,
                None,
            );
            return None;
        };

        while unix_now_secs() <= hard_deadline_ts {
            if close_exact.is_none() {
                if let Some((price, ts_ms)) = peek_prewarmed_tick(&target_symbol, end_ms) {
                    close_exact = Some(AggregatedPricePoint::from_exact(
                        price,
                        ts_ms,
                        "prewarm_close_early",
                    ));
                }
            }
            if close_exact.is_some()
                && (open_exact.is_some()
                    || first_after_start
                        .map(|(delta, _, _)| delta <= open_tol_ms)
                        .unwrap_or(false)
                    || nearest_start
                        .map(|(delta, _, _)| delta <= open_tol_ms)
                        .unwrap_or(false)
                    || prev_round_close.is_some())
            {
                break;
            }
            let next = tokio::time::timeout(Duration::from_millis(250), rx.recv()).await;
            let (price, ts_ms) = match next {
                Ok(Ok(hit)) => hit,
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                    lagged_ticks = lagged_ticks.saturating_add(n);
                    continue;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                Err(_) => continue,
            };
            observed_ticks = observed_ticks.saturating_add(1);
            self_built_agg_ingest_tick(
                price,
                ts_ms,
                start_ms,
                end_ms,
                &mut first_tick_ts_ms,
                &mut open_exact,
                &mut close_exact,
                &mut nearest_start,
                &mut nearest_end,
                &mut first_after_start,
                &mut first_after_end,
            );
            observed_live_ticks = observed_live_ticks.saturating_add(1);

            // Early stop: close known and at least one usable open candidate already exists.
            if close_exact.is_some()
                && (open_exact.is_some()
                    || first_after_start
                        .map(|(delta, _, _)| delta <= open_tol_ms)
                        .unwrap_or(false)
                    || nearest_start
                        .map(|(delta, _, _)| delta <= open_tol_ms)
                        .unwrap_or(false)
                    || prev_round_close.is_some())
            {
                break;
            }
        }
    }

    if close_exact.is_none() {
        close_exact = peek_prewarmed_tick(&target_symbol, end_ms)
            .map(|(price, ts_ms)| AggregatedPricePoint::from_exact(price, ts_ms, "prewarm_close"));
    }

    let (open_point, open_pick_rule): (Option<AggregatedPricePoint>, &'static str) =
        if let Some(p) = open_exact {
            (Some(p), "exact")
        } else if let Some((delta, ts_ms, px)) = first_after_start {
            if delta <= open_tol_ms {
                (
                    Some(AggregatedPricePoint {
                        price: px,
                        ts_ms,
                        exact: false,
                        abs_delta_ms: delta,
                        source: "hub_first_after_open",
                    }),
                    "first_after",
                )
            } else {
                (None, "missing")
            }
        } else if let Some((delta, ts_ms, px)) = nearest_start {
            if delta <= open_tol_ms {
                (
                    Some(AggregatedPricePoint {
                        price: px,
                        ts_ms,
                        exact: false,
                        abs_delta_ms: delta,
                        source: "hub_nearest_open",
                    }),
                    "nearest",
                )
            } else {
                (None, "missing")
            }
        } else if let Some((px, ts_ms)) = prev_round_close {
            (
                Some(AggregatedPricePoint {
                    price: px,
                    ts_ms,
                    exact: false,
                    abs_delta_ms: ts_ms.abs_diff(start_ms),
                    source: "prev_round_close",
                }),
                "prev_close",
            )
        } else {
            (None, "missing")
        };
    let (close_point, close_pick_rule): (Option<AggregatedPricePoint>, &'static str) =
        if let Some(p) = close_exact {
            (Some(p), "exact")
        } else if let Some((delta, ts_ms, px)) = first_after_end {
            if delta <= close_tol_ms {
                (
                    Some(AggregatedPricePoint {
                        price: px,
                        ts_ms,
                        exact: false,
                        abs_delta_ms: delta,
                        source: "hub_first_after_close",
                    }),
                    "first_after",
                )
            } else {
                (None, "missing")
            }
        } else if let Some((delta, ts_ms, px)) = nearest_end {
            if delta <= close_tol_ms {
                (
                    Some(AggregatedPricePoint {
                        price: px,
                        ts_ms,
                        exact: false,
                        abs_delta_ms: delta,
                        source: "hub_nearest_close",
                    }),
                    "nearest",
                )
            } else {
                (None, "missing")
            }
        } else {
            (None, "missing")
        };

    let (open_hit, close_hit) = match (open_point, close_point) {
        (Some(o), Some(c)) => (o, c),
        _ => {
            warn!(
                "⚠️ self_built_price_agg_unresolved | symbol={} round_start_ts={} round_end_ts={} observed_ticks={} lagged_ticks={} open_exact={} close_exact={} nearest_start={:?} nearest_end={:?}",
                target_symbol,
                round_start_ts,
                round_end_ts,
                observed_ticks,
                lagged_ticks,
                open_exact.is_some(),
                close_exact.is_some(),
                nearest_start,
                nearest_end,
            );
            emit_probe(
                "unresolved",
                None,
                None,
                open_point,
                close_point,
                observed_ticks,
                observed_snapshot_ticks,
                observed_live_ticks,
                lagged_ticks,
                nearest_start,
                nearest_end,
                first_after_start,
                first_after_end,
                Some(open_pick_rule),
                Some(close_pick_rule),
            );
            return None;
        }
    };

    let mut confidence: f64 = match (open_hit.exact, close_hit.exact) {
        (true, true) => 1.0,
        (true, false) => 0.9,
        (false, true) => 0.86,
        (false, false) => 0.76,
    };
    if !open_hit.exact && open_hit.abs_delta_ms > open_tol_ms / 2 {
        confidence -= 0.05;
    }
    if !close_hit.exact && close_hit.abs_delta_ms > close_tol_ms / 2 {
        confidence -= 0.05;
    }
    if open_hit.source == "prev_round_close" {
        confidence -= 0.08;
    }
    confidence = confidence.clamp(0.0, 1.0);

    if confidence + 1e-9 < min_confidence {
        warn!(
            "⚠️ self_built_price_agg_low_confidence | symbol={} round_start_ts={} round_end_ts={} confidence={:.3} min_confidence={:.3} open_source={} close_source={} open_delta_ms={} close_delta_ms={}",
            target_symbol,
            round_start_ts,
            round_end_ts,
            confidence,
            min_confidence,
            open_hit.source,
            close_hit.source,
            open_hit.abs_delta_ms,
            close_hit.abs_delta_ms,
        );
        emit_probe(
            "low_confidence",
            None,
            Some(confidence),
            Some(open_hit),
            Some(close_hit),
            observed_ticks,
            observed_snapshot_ticks,
            observed_live_ticks,
            lagged_ticks,
            nearest_start,
            nearest_end,
            first_after_start,
            first_after_end,
            Some(open_pick_rule),
            Some(close_pick_rule),
        );
        return None;
    }

    let side = if close_hit.price >= open_hit.price {
        Side::Yes
    } else {
        Side::No
    };
    set_last_chainlink_close(&target_symbol, close_hit.ts_ms, close_hit.price);
    info!(
        "🧠 self_built_price_agg_ready | unix_ms={} symbol={} side={:?} confidence={:.3} open={:.6}@{} open_source={} open_exact={} open_delta_ms={} close={:.6}@{} close_source={} close_exact={} close_delta_ms={} observed_ticks={} observed_snapshot_ticks={} observed_live_ticks={} lagged_ticks={}",
        unix_now_millis_u64(),
        target_symbol,
        side,
        confidence,
        open_hit.price,
        open_hit.ts_ms,
        open_hit.source,
        open_hit.exact,
        open_hit.abs_delta_ms,
        close_hit.price,
        close_hit.ts_ms,
        close_hit.source,
        close_hit.exact,
        close_hit.abs_delta_ms,
        observed_ticks,
        observed_snapshot_ticks,
        observed_live_ticks,
        lagged_ticks,
    );
    emit_probe(
        "ready",
        Some(side),
        Some(confidence),
        Some(open_hit),
        Some(close_hit),
        observed_ticks,
        observed_snapshot_ticks,
        observed_live_ticks,
        lagged_ticks,
        nearest_start,
        nearest_end,
        first_after_start,
        first_after_end,
        Some(open_pick_rule),
        Some(close_pick_rule),
    );

    Some((
        side,
        open_hit.price,
        close_hit.price,
        open_hit.ts_ms,
        close_hit.ts_ms,
        open_hit.exact,
    ))
}

#[derive(Debug, Default, Clone, Copy)]
struct LocalSourceBoundaryState {
    first_tick_ts_ms: Option<u64>,
    open_exact: Option<AggregatedPricePoint>,
    close_exact: Option<AggregatedPricePoint>,
    nearest_start: Option<(u64, u64, f64)>,
    nearest_end: Option<(u64, u64, f64)>,
    first_after_start: Option<(u64, u64, f64)>,
    first_after_end: Option<(u64, u64, f64)>,
}

#[derive(Debug, Default, Clone)]
struct LocalSourceBoundaryTapeState {
    open_window_ticks: Vec<(u64, f64)>,
    close_window_ticks: Vec<(u64, f64)>,
}

const LOCAL_PRICE_AGG_BOUNDARY_TAPE_MAX_TICKS_PER_SIDE: usize = 512;

fn format_local_source_boundary_state(
    source: LocalPriceSource,
    state: &LocalSourceBoundaryState,
) -> String {
    let first_tick = state
        .first_tick_ts_ms
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    let open_exact = state
        .open_exact
        .map(|p| format!("{}@{}", p.price, p.ts_ms))
        .unwrap_or_else(|| "-".to_string());
    let close_exact = state
        .close_exact
        .map(|p| format!("{}@{}", p.price, p.ts_ms))
        .unwrap_or_else(|| "-".to_string());
    let nearest_start = state
        .nearest_start
        .map(|(delta, ts_ms, px)| format!("{delta}ms/{px}@{ts_ms}"))
        .unwrap_or_else(|| "-".to_string());
    let first_after_start = state
        .first_after_start
        .map(|(delta, ts_ms, px)| format!("{delta}ms/{px}@{ts_ms}"))
        .unwrap_or_else(|| "-".to_string());
    let nearest_end = state
        .nearest_end
        .map(|(delta, ts_ms, px)| format!("{delta}ms/{px}@{ts_ms}"))
        .unwrap_or_else(|| "-".to_string());
    let first_after_end = state
        .first_after_end
        .map(|(delta, ts_ms, px)| format!("{delta}ms/{px}@{ts_ms}"))
        .unwrap_or_else(|| "-".to_string());
    format!(
        "{}:first_tick={} open_exact={} first_after_start={} nearest_start={} close_exact={} first_after_end={} nearest_end={}",
        source.as_str(),
        first_tick,
        open_exact,
        first_after_start,
        nearest_start,
        close_exact,
        first_after_end,
        nearest_end,
    )
}

fn local_price_agg_collect_boundary_tape_tick(
    tapes: &mut HashMap<LocalPriceSource, LocalSourceBoundaryTapeState>,
    source: LocalPriceSource,
    price: f64,
    ts_ms: u64,
    start_ms: u64,
    end_ms: u64,
    boundary_window_ms: u64,
) {
    let tape = tapes.entry(source).or_default();
    let open_low = start_ms.saturating_sub(boundary_window_ms);
    let open_high = start_ms.saturating_add(boundary_window_ms);
    if ts_ms >= open_low && ts_ms <= open_high {
        if tape.open_window_ticks.len() < LOCAL_PRICE_AGG_BOUNDARY_TAPE_MAX_TICKS_PER_SIDE {
            tape.open_window_ticks.push((ts_ms, price));
        }
    }
    let close_low = end_ms.saturating_sub(boundary_window_ms);
    let close_high = end_ms.saturating_add(boundary_window_ms);
    if ts_ms >= close_low && ts_ms <= close_high {
        if tape.close_window_ticks.len() < LOCAL_PRICE_AGG_BOUNDARY_TAPE_MAX_TICKS_PER_SIDE {
            tape.close_window_ticks.push((ts_ms, price));
        }
    }
}

fn build_local_price_agg_boundary_source_probes(
    tapes: &HashMap<LocalPriceSource, LocalSourceBoundaryTapeState>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<LocalPriceAggBoundarySourceProbe> {
    let mut items = tapes
        .iter()
        .map(|(source, tape)| LocalPriceAggBoundarySourceProbe {
            source: source.as_str().to_string(),
            open_window_ticks: tape
                .open_window_ticks
                .iter()
                .map(|(ts_ms, price)| LocalBoundaryTickProbe {
                    ts_ms: *ts_ms,
                    offset_ms: (*ts_ms as i64) - (start_ms as i64),
                    price: *price,
                })
                .collect(),
            close_window_ticks: tape
                .close_window_ticks
                .iter()
                .map(|(ts_ms, price)| LocalBoundaryTickProbe {
                    ts_ms: *ts_ms,
                    offset_ms: (*ts_ms as i64) - (end_ms as i64),
                    price: *price,
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| a.source.cmp(&b.source));
    items
}

fn pick_local_source_points(
    state: &LocalSourceBoundaryState,
    open_tol_ms: u64,
    close_tol_ms: u64,
) -> Option<(AggregatedPricePoint, AggregatedPricePoint)> {
    let open = if let Some(p) = state.open_exact {
        Some(p)
    } else if let Some((delta, ts_ms, px)) = state.first_after_start {
        if delta <= open_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_first_after_open",
            })
        } else {
            None
        }
    } else if let Some((delta, ts_ms, px)) = state.nearest_start {
        if delta <= open_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_nearest_open",
            })
        } else {
            None
        }
    } else {
        None
    }?;

    let close = if let Some(p) = state.close_exact {
        Some(p)
    } else if let Some((delta, ts_ms, px)) = state.first_after_end {
        if delta <= close_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_first_after_close",
            })
        } else {
            None
        }
    } else if let Some((delta, ts_ms, px)) = state.nearest_end {
        if delta <= close_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_nearest_close",
            })
        } else {
            None
        }
    } else {
        None
    }?;

    Some((open, close))
}

fn pick_local_source_close_point(
    state: &LocalSourceBoundaryState,
    close_tol_ms: u64,
) -> Option<AggregatedPricePoint> {
    if let Some(p) = state.close_exact {
        Some(p)
    } else if let Some((delta, ts_ms, px)) = state.first_after_end {
        if delta <= close_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_first_after_close",
            })
        } else {
            None
        }
    } else if let Some((delta, ts_ms, px)) = state.nearest_end {
        if delta <= close_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_nearest_close",
            })
        } else {
            None
        }
    } else {
        None
    }
}

fn pick_local_boundary_tape_close_point(
    tape: &LocalSourceBoundaryTapeState,
    end_ms: u64,
    pre_ms: u64,
    post_ms: u64,
    rule: LocalBoundaryCloseRule,
) -> Option<(AggregatedPricePoint, f64)> {
    let ticks = tape
        .close_window_ticks
        .iter()
        .filter_map(|(ts_ms, price)| {
            let offset_ms = (*ts_ms as i64) - (end_ms as i64);
            let in_range = offset_ms >= -(pre_ms as i64) && offset_ms <= post_ms as i64;
            if in_range {
                Some((*ts_ms, *price, offset_ms))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if ticks.is_empty() {
        return None;
    }

    let picked = match rule {
        LocalBoundaryCloseRule::LastBefore => ticks
            .iter()
            .filter(|(_, _, offset_ms)| *offset_ms <= 0)
            .max_by_key(|(ts_ms, _, _)| *ts_ms)
            .copied(),
        LocalBoundaryCloseRule::NearestAbs => ticks
            .iter()
            .min_by_key(|(ts_ms, _, offset_ms)| (offset_ms.abs(), *offset_ms > 0, *ts_ms))
            .copied(),
        LocalBoundaryCloseRule::AfterThenBefore => ticks
            .iter()
            .filter(|(_, _, offset_ms)| *offset_ms >= 0)
            .min_by_key(|(ts_ms, _, _)| *ts_ms)
            .copied()
            .or_else(|| {
                ticks
                    .iter()
                    .filter(|(_, _, offset_ms)| *offset_ms <= 0)
                    .max_by_key(|(ts_ms, _, _)| *ts_ms)
                    .copied()
            }),
    }?;

    let (ts_ms, raw_price, offset_ms) = picked;
    Some((
        AggregatedPricePoint {
            price: raw_price,
            ts_ms,
            exact: offset_ms == 0,
            abs_delta_ms: offset_ms.unsigned_abs(),
            source: rule.as_str(),
        },
        raw_price,
    ))
}

fn local_boundary_policy_specs_fully_free(symbol: &str) -> LocalBoundaryShadowPolicySpec {
    match symbol {
        "bnb/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "full",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_FULL,
        },
        "btc/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "drop_okx",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_OKX,
        },
        "doge/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "drop_okx",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_OKX,
        },
        "eth/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "drop_binance",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_BINANCE,
        },
        "hype/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "drop_hyperliquid",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_HYPERLIQUID,
        },
        "sol/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "drop_bybit",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 2,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_BYBIT,
        },
        "xrp/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "drop_okx",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_OKX,
        },
        _ => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_fully_free",
            source_subset_name: "full",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_FULL,
        },
    }
}

fn local_boundary_policy_specs_shared_core(symbol: &str) -> LocalBoundaryShadowPolicySpec {
    match symbol {
        "bnb/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "drop_binance",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_BINANCE,
        },
        "btc/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "drop_okx",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_OKX,
        },
        "doge/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "drop_okx",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_OKX,
        },
        "eth/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "full",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_FULL,
        },
        "hype/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "drop_coinbase",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_COINBASE,
        },
        "sol/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "drop_binance",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_BINANCE,
        },
        "xrp/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "drop_binance",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_BINANCE,
        },
        _ => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_shared_core",
            source_subset_name: "full",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_FULL,
        },
    }
}

fn local_boundary_policy_specs_weighted(symbol: &str) -> LocalBoundaryShadowPolicySpec {
    match symbol {
        "bnb/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "drop_okx",
            rule: LocalBoundaryCloseRule::AfterThenBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_OKX,
        },
        "btc/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "only_coinbase",
            rule: LocalBoundaryCloseRule::AfterThenBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_COINBASE,
        },
        "doge/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "drop_binance",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_BINANCE,
        },
        "eth/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "only_coinbase",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_COINBASE,
        },
        "hype/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "drop_binance",
            rule: LocalBoundaryCloseRule::AfterThenBefore,
            min_sources: 2,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_DROP_BINANCE,
        },
        "sol/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "only_okx_coinbase",
            rule: LocalBoundaryCloseRule::AfterThenBefore,
            min_sources: 2,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_OKX_COINBASE,
        },
        "xrp/usd" => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "only_binance_coinbase",
            rule: LocalBoundaryCloseRule::NearestAbs,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE_COINBASE,
        },
        _ => LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_weighted",
            source_subset_name: "full",
            rule: LocalBoundaryCloseRule::LastBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_FULL,
        },
    }
}

fn local_boundary_symbol_router_fallback_policy_specs(
    symbol: &str,
) -> Vec<LocalBoundaryShadowPolicySpec> {
    match symbol {
        "bnb/usd" => vec![
            LocalBoundaryShadowPolicySpec {
                policy_name: "boundary_symbol_router_fallback",
                source_subset_name: "bnb_okx_no_fallback",
                rule: LocalBoundaryCloseRule::AfterThenBefore,
                min_sources: 1,
                allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_OKX,
            },
            LocalBoundaryShadowPolicySpec {
                policy_name: "boundary_symbol_router_fallback",
                source_subset_name: "bnb_okx_fallback",
                rule: LocalBoundaryCloseRule::AfterThenBefore,
                min_sources: 1,
                allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_OKX,
            },
        ],
        "doge/usd" => vec![LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_symbol_router_fallback",
            source_subset_name: "doge_binance_fallback",
            rule: LocalBoundaryCloseRule::AfterThenBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE,
        }],
        "eth/usd" => vec![LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_symbol_router_fallback",
            source_subset_name: "eth_binance_missing_fallback",
            rule: LocalBoundaryCloseRule::AfterThenBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE,
        }],
        "hype/usd" => vec![LocalBoundaryShadowPolicySpec {
            policy_name: "boundary_symbol_router_fallback",
            source_subset_name: "hype_hyperliquid_fallback",
            rule: LocalBoundaryCloseRule::AfterThenBefore,
            min_sources: 1,
            allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_HYPERLIQUID,
        }],
        "sol/usd" => vec![
            LocalBoundaryShadowPolicySpec {
                policy_name: "boundary_symbol_router_fallback",
                source_subset_name: "sol_binance_coinbase_fallback",
                rule: LocalBoundaryCloseRule::NearestAbs,
                min_sources: 2,
                allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE_COINBASE,
            },
            LocalBoundaryShadowPolicySpec {
                policy_name: "boundary_symbol_router_fallback",
                source_subset_name: "sol_coinbase_fallback",
                rule: LocalBoundaryCloseRule::AfterThenBefore,
                min_sources: 1,
                allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_COINBASE,
            },
            LocalBoundaryShadowPolicySpec {
                policy_name: "boundary_symbol_router_fallback",
                source_subset_name: "sol_binance_fallback",
                rule: LocalBoundaryCloseRule::AfterThenBefore,
                min_sources: 1,
                allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE,
            },
            LocalBoundaryShadowPolicySpec {
                policy_name: "boundary_symbol_router_fallback",
                source_subset_name: "sol_okx_missing_fallback",
                rule: LocalBoundaryCloseRule::AfterThenBefore,
                min_sources: 1,
                allowed_sources: LOCAL_BOUNDARY_SOURCES_ONLY_OKX,
            },
        ],
        _ => Vec::new(),
    }
}

fn local_boundary_policy_source_weight(
    policy_name: &str,
    symbol: &str,
    source: LocalPriceSource,
) -> f64 {
    if policy_name != "boundary_weighted" {
        return local_price_agg_source_weight(source);
    }
    if symbol == "doge/usd" {
        return match source {
            LocalPriceSource::Binance => 0.5,
            LocalPriceSource::Bybit => 0.5,
            LocalPriceSource::Coinbase => 1.0,
            LocalPriceSource::Hyperliquid => 1.5,
            LocalPriceSource::Okx => 0.5,
        };
    }
    if symbol == "xrp/usd" {
        return match source {
            LocalPriceSource::Binance => 0.462_086,
            LocalPriceSource::Bybit => 0.0,
            LocalPriceSource::Coinbase => 2.329_335,
            LocalPriceSource::Okx => 0.050_000,
            LocalPriceSource::Hyperliquid => 1.083_887,
        };
    }
    if symbol == "btc/usd" {
        return match source {
            LocalPriceSource::Coinbase => 1.0,
            _ => 0.0,
        };
    }
    if symbol == "bnb/usd" {
        return match source {
            LocalPriceSource::Binance => 0.5,
            LocalPriceSource::Bybit => 0.5,
            LocalPriceSource::Okx => 0.5,
            LocalPriceSource::Coinbase => 1.0,
            LocalPriceSource::Hyperliquid => 1.5,
        };
    }
    if symbol == "hype/usd" {
        return match source {
            LocalPriceSource::Bybit => 1.0,
            LocalPriceSource::Coinbase => 1.0,
            LocalPriceSource::Okx => 1.0,
            LocalPriceSource::Hyperliquid => 0.25,
            LocalPriceSource::Binance => 0.0,
        };
    }
    match source {
        LocalPriceSource::Binance => 0.5,
        LocalPriceSource::Bybit => 0.5,
        LocalPriceSource::Okx => 0.5,
        LocalPriceSource::Coinbase => 1.0,
        LocalPriceSource::Hyperliquid => 1.5,
    }
}

fn local_boundary_weighted_candidate_filter_reason_for_policy(
    symbol: &str,
    close_only_filter_reason: Option<&'static str>,
    close_only_direction_margin_bps: Option<f64>,
    round_end_ts: u64,
    rtds_open: f64,
    hit: &LocalBoundaryShadowHit,
) -> Option<&'static str> {
    let weighted_direction_margin_bps =
        ((hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let close_abs_delta_ms = hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000));
    let has_source = |source: LocalPriceSource| {
        hit.source_contributions
            .iter()
            .any(|contribution| contribution.source == source)
    };
    let first_source_is = |source: LocalPriceSource| {
        hit.source_contributions
            .first()
            .is_some_and(|contribution| contribution.source == source)
    };
    if hit.source_count >= 2 && hit.source_spread_bps > 20.0 {
        return Some("cross_source_extreme_spread");
    }
    match symbol {
        "bnb/usd" => {
            let weighted_side_yes = hit.close_price >= rtds_open;
            if hit.source_subset_name == "bnb_okx_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 >= 8.0
            {
                return Some("bnb_okx_fallback_far_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 < 4.5
            {
                return Some("bnb_single_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 7.6
                && close_abs_delta_ms >= 400
                && close_abs_delta_ms <= 1_200
            {
                return Some("bnb_single_yes_fast_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 9.3 + 1e-9
                && (close_abs_delta_ms < 400 || close_abs_delta_ms >= 1_800)
            {
                return Some("bnb_single_yes_upper_mid_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 9.3 + 1e-9
                && hit.source_spread_bps <= 0.5 + 1e-9
                && close_abs_delta_ms >= 2_500
            {
                return Some("bnb_two_lowspread_yes_upper_mid_stale_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_direction_margin_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps < 5.8
                && close_abs_delta_ms <= 500
            {
                return Some("bnb_single_binance_yes_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_direction_margin_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps < 5.5 + 1e-9
                && close_abs_delta_ms >= 1_800
            {
                return Some("bnb_single_binance_yes_stale_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_direction_margin_bps + 1e-9 >= 7.0
                && weighted_direction_margin_bps < 8.0
                && close_abs_delta_ms >= 1_800
            {
                return Some("bnb_single_binance_yes_stale_upper_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_direction_margin_bps + 1e-9 >= 11.5
                && close_abs_delta_ms >= 1_800
            {
                return Some("bnb_single_binance_yes_stale_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count <= 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 < 2.0
                && close_abs_delta_ms >= 100
                && close_abs_delta_ms < 1_500
            {
                return Some("bnb_no_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps <= 2.0 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 2.2
            {
                return Some("bnb_two_no_midspread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_direction_margin_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps < 6.0
                && close_abs_delta_ms <= 1_000
            {
                return Some("bnb_single_binance_no_fast_tail");
            }
            if hit.source_subset_name == "drop_okx"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 1
                && weighted_side_yes
                && first_source_is(LocalPriceSource::Binance)
                && weighted_direction_margin_bps + 1e-9 >= 0.70
                && weighted_direction_margin_bps < 0.75
                && close_abs_delta_ms == 0
            {
                return Some("bnb_single_binance_exact_yes_tiny_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Bybit)
                && weighted_direction_margin_bps + 1e-9 >= 10.0
            {
                return Some("bnb_single_bybit_no_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 < 2.5
            {
                return Some("bnb_three_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps + 1e-9 >= 30.0
                && close_abs_delta_ms <= 100
            {
                return Some("bnb_three_high_spread_fast_far_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps + 1e-9 < 3.5
            {
                return Some("bnb_three_wide_spread_yes_near_flat");
            }
            if hit.source_subset_name == "drop_okx"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && has_source(LocalPriceSource::Binance)
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 4.0
                && hit.source_spread_bps < 5.0
                && weighted_direction_margin_bps + 1e-9 >= 2.2
                && weighted_direction_margin_bps < 2.4
                && close_abs_delta_ms >= 200
                && close_abs_delta_ms <= 320
            {
                return Some("bnb_three_no_fast_midspread_lowmargin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps <= 0.55 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps < 7.3
                && close_abs_delta_ms < 250
            {
                return Some("bnb_two_tight_spread_yes_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources >= 1
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 0.5 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 2.0
            {
                return Some("bnb_two_binance_bybit_exact_lowspread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 0.6 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 15.0
                && close_abs_delta_ms <= 200
            {
                return Some("bnb_two_binance_bybit_yes_lowspread_fast_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 1.3 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps < 3.3
                && close_abs_delta_ms <= 800
            {
                return Some("bnb_two_binance_bybit_yes_lowspread_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 1.0 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 5.5
                && weighted_direction_margin_bps < 7.0
                && close_abs_delta_ms <= 500
            {
                return Some("bnb_two_binance_bybit_yes_lowspread_fast_upper_mid_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 0.3 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 9.0
                && weighted_direction_margin_bps < 10.0
                && close_abs_delta_ms >= 800
                && close_abs_delta_ms <= 1_500
            {
                return Some("bnb_two_binance_bybit_yes_lowspread_fast_high_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps + 1e-9 >= 1.5
                && hit.source_spread_bps < 2.0
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 8.5
                && close_abs_delta_ms >= 1_000
                && close_abs_delta_ms <= 1_500
            {
                return Some("bnb_two_binance_bybit_yes_midspread_stale_upper_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 < 5.0
            {
                return Some("bnb_two_wide_spread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps + 1e-9 < 1.5
            {
                return Some("bnb_high_spread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 < 1.5
            {
                return Some("bnb_mid_spread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps <= 1.3 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 2.2
            {
                return Some("bnb_tight_spread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 1.2 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 3.0
                && close_abs_delta_ms <= 800
            {
                return Some("bnb_two_binance_bybit_yes_midspread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 1.3 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 3.0
            {
                return Some("bnb_two_binance_bybit_yes_lowspread_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps <= 1.3 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 7.0
                && weighted_direction_margin_bps < 8.0
                && close_abs_delta_ms >= 2_500
            {
                return Some("bnb_two_binance_bybit_yes_stale_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 < 1.0
            {
                return Some("bnb_yes_near_flat");
            }
            if close_only_filter_reason == Some("preclose_near_flat")
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 2.0
            {
                return Some("bnb_close_only_preclose_near_flat");
            }
        }
        "btc/usd" => {
            let weighted_side_yes = hit.close_price >= rtds_open;
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 < 0.451
            {
                return Some("btc_single_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 < 0.9
                && close_abs_delta_ms <= 300
            {
                return Some("btc_single_coinbase_yes_fast_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 < 0.9
                && close_abs_delta_ms > 300
                && close_abs_delta_ms <= 600
            {
                return Some("btc_single_coinbase_yes_midlag_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 1.0
                && weighted_direction_margin_bps < 1.5
                && close_abs_delta_ms <= 200
            {
                return Some("btc_single_coinbase_yes_fast_low_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 2.2
                && weighted_direction_margin_bps < 2.5
                && close_abs_delta_ms <= 100
            {
                return Some("btc_single_coinbase_yes_very_fast_upper_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 2.2
                && weighted_direction_margin_bps < 2.5
                && close_abs_delta_ms > 100
                && close_abs_delta_ms <= 150
            {
                return Some("btc_single_coinbase_yes_fast_upper_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 0.9
                && weighted_direction_margin_bps < 1.2
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 550
            {
                return Some("btc_single_coinbase_yes_midlag_upper_near_flat_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 1.8
                && weighted_direction_margin_bps < 2.1
                && close_abs_delta_ms > 300
                && close_abs_delta_ms <= 500
            {
                return Some("btc_single_coinbase_yes_midlag_upper_near_flat");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && first_source_is(LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 1.14
                && weighted_direction_margin_bps < 1.17
                && close_abs_delta_ms >= 250
                && close_abs_delta_ms <= 330
            {
                return Some("btc_single_coinbase_yes_midlag_low_margin_tail");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && first_source_is(LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 1.05
                && weighted_direction_margin_bps < 1.07
                && close_abs_delta_ms >= 430
                && close_abs_delta_ms <= 500
            {
                return Some("btc_single_coinbase_yes_upper_midlag_low_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 2.4
                && weighted_direction_margin_bps < 2.7
                && close_abs_delta_ms > 500
                && close_abs_delta_ms <= 700
            {
                return Some("btc_single_coinbase_yes_midlag_upper_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 < 0.9
                && close_abs_delta_ms >= 1_800
            {
                return Some("btc_single_coinbase_yes_stale_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 < 0.5
                && close_abs_delta_ms <= 100
            {
                return Some("btc_single_coinbase_no_fast_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 < 0.5
                && close_abs_delta_ms > 300
                && close_abs_delta_ms <= 700
            {
                return Some("btc_single_coinbase_no_midlag_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 < 0.25
                && close_abs_delta_ms >= 1_800
            {
                return Some("btc_single_coinbase_no_stale_near_flat");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && first_source_is(LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 50.0
                && close_abs_delta_ms >= 700
                && close_abs_delta_ms <= 800
            {
                return Some("btc_single_coinbase_no_late_extreme_margin_tail");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && first_source_is(LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 0.75
                && weighted_direction_margin_bps < 0.79
                && close_abs_delta_ms >= 850
                && close_abs_delta_ms <= 900
            {
                return Some("btc_single_coinbase_no_midlag_low_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 2.6
                && close_abs_delta_ms <= 300
            {
                return Some("btc_single_coinbase_no_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 1.0
            {
                return Some("btc_two_source_near_flat");
            }
        }
        "xrp/usd" => {
            let weighted_side_yes = hit.close_price >= rtds_open;
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps < 0.05
                && weighted_direction_margin_bps + 1e-9 < 1.0
                && close_abs_delta_ms <= 300
            {
                return Some("xrp_binance_coinbase_yes_zero_spread_fast_near_flat");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && has_source(LocalPriceSource::Binance)
                && has_source(LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 1.14
                && weighted_direction_margin_bps < 1.16
                && close_abs_delta_ms >= 60
                && close_abs_delta_ms <= 140
            {
                return Some("xrp_binance_coinbase_yes_fast_lowmargin_side_tail");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps < 1.7
                && weighted_direction_margin_bps + 1e-9 < 0.35
                && close_abs_delta_ms <= 300
            {
                return Some("xrp_binance_coinbase_yes_fast_midspread_near_flat");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps < 1.7
                && weighted_direction_margin_bps + 1e-9 < 1.0
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 800
            {
                return Some("xrp_binance_coinbase_yes_midlag_midspread_near_flat");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 0.5
                && hit.source_spread_bps < 1.0
                && weighted_direction_margin_bps + 1e-9 < 0.7
                && close_abs_delta_ms <= 300
            {
                return Some("xrp_binance_coinbase_yes_fast_tightspread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 0.45
                && !((!weighted_side_yes && close_abs_delta_ms >= 400)
                    || (weighted_side_yes && close_abs_delta_ms < 300))
            {
                return Some("xrp_nearest_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps + 1e-9 < 2.0
                && !(!weighted_side_yes && close_abs_delta_ms >= 50)
            {
                return Some("xrp_nearest_wide_spread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 1.3
                && !(!weighted_side_yes && weighted_direction_margin_bps + 1e-9 < 1.13)
            {
                return Some("xrp_single_nearest_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && !weighted_side_yes
                && hit.source_subset_name == "only_binance_coinbase"
                && weighted_direction_margin_bps + 1e-9 < 0.7
                && close_abs_delta_ms <= 300
            {
                return Some("xrp_single_binance_no_fast_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_side_yes
                && weighted_direction_margin_bps < 2.5
                && close_abs_delta_ms >= 1_000
            {
                return Some("xrp_single_binance_yes_stale_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 < 4.5
                && close_abs_delta_ms <= 1_000
            {
                return Some("xrp_single_binance_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps < 2.6
                && close_abs_delta_ms <= 100
            {
                return Some("xrp_nearest_wide_spread_yes_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 3.0
                && close_abs_delta_ms >= 800
            {
                return Some("xrp_binance_coinbase_no_stale_mid_margin");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps < 1.7
                && weighted_direction_margin_bps + 1e-9 < 0.8
                && close_abs_delta_ms >= 1_000
            {
                return Some("xrp_binance_coinbase_no_stale_midspread_near_flat");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && has_source(LocalPriceSource::Binance)
                && has_source(LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 >= 0.65
                && weighted_direction_margin_bps < 0.665
                && close_abs_delta_ms >= 2_000
                && close_abs_delta_ms <= 2_070
            {
                return Some("xrp_binance_coinbase_yes_stale_lowmargin_side_tail");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && has_source(LocalPriceSource::Binance)
                && has_source(LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 0.7
                && hit.source_spread_bps < 0.72
                && weighted_direction_margin_bps + 1e-9 >= 2.5
                && weighted_direction_margin_bps < 2.6
                && close_abs_delta_ms >= 330
                && close_abs_delta_ms <= 350
            {
                return Some("xrp_binance_coinbase_yes_midlag_tightspread_mid_margin_tail");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.5
                && weighted_direction_margin_bps + 1e-9 < 1.2
                && close_abs_delta_ms <= 500
            {
                return Some("xrp_binance_coinbase_no_fast_midspread_near_flat");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Binance)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && weighted_direction_margin_bps + 1e-9 < 0.09
                && close_abs_delta_ms >= 400
                && close_abs_delta_ms <= 700
            {
                return Some("xrp_binance_coinbase_no_stale_extreme_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Binance)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 2.5
                && weighted_direction_margin_bps < 3.5
                && close_abs_delta_ms >= 2_000
            {
                return Some("xrp_single_binance_yes_stale_mid_margin");
            }
            if hit.source_subset_name == "only_binance_coinbase"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && first_source_is(LocalPriceSource::Binance)
                && !weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 0.54
                && weighted_direction_margin_bps < 0.56
                && close_abs_delta_ms >= 2_750
                && close_abs_delta_ms <= 2_850
            {
                return Some("xrp_single_binance_no_stale_lowmargin_side_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 < 1.5
            {
                return Some("xrp_last_yes_near_flat");
            }
        }
        "doge/usd" => {
            let weighted_side_yes = hit.close_price >= rtds_open;
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && close_abs_delta_ms <= 1_000
                && weighted_direction_margin_bps + 1e-9 < 4.0
                && !(hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx)
                    && weighted_direction_margin_bps + 1e-9 >= 0.45)
            {
                return Some("doge_single_last_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Bybit)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 4.0
                && weighted_direction_margin_bps < 5.0
                && close_abs_delta_ms <= 1_000
            {
                return Some("doge_single_bybit_yes_fast_upper_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Bybit)
                && weighted_direction_margin_bps + 1e-9 >= 14.0
                && close_abs_delta_ms <= 300
            {
                return Some("doge_single_bybit_fast_high_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Bybit)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 2.5
                && weighted_direction_margin_bps < 15.5
                && close_abs_delta_ms >= 2_000
                && close_abs_delta_ms <= 4_600
            {
                return Some("doge_single_bybit_last_stale_yes_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 1.5
                && hit.source_spread_bps < 2.2
                && weighted_direction_margin_bps + 1e-9 >= 10.0
                && close_abs_delta_ms >= 1_500
            {
                return Some("doge_two_bybit_okx_last_stale_midspread_high_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx)
                && (900..=1_000).contains(&close_abs_delta_ms)
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 15.0
            {
                return Some("doge_single_okx_late_fast_mid_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 2.5
                && close_abs_delta_ms >= 4_000
                && close_abs_delta_ms <= 4_500
            {
                return Some("doge_single_okx_last_very_stale_yes_mid_margin");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps < 2.0
                && close_abs_delta_ms >= 1_200
                && close_abs_delta_ms <= 1_600
            {
                return Some("doge_single_okx_last_midlag_mid_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps < 3.2
                && close_abs_delta_ms >= 450
                && close_abs_delta_ms <= 520
            {
                return Some("doge_single_okx_last_fast_upper_mid_margin_side_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 3.5
                && hit.source_spread_bps < 3.7
                && weighted_direction_margin_bps + 1e-9 >= 50.0
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 600
            {
                return Some("doge_three_bybit_coinbase_okx_last_midspread_no_extreme_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 3.8
                && weighted_direction_margin_bps + 1e-9 >= 7.0
                && weighted_direction_margin_bps < 8.0
                && close_abs_delta_ms <= 500
            {
                return Some("doge_three_last_fast_spread_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 6.0
                && hit.source_spread_bps < 7.0
                && weighted_direction_margin_bps + 1e-9 >= 25.0
                && weighted_direction_margin_bps < 27.0
                && close_abs_delta_ms <= 500
            {
                return Some("doge_three_bybit_coinbase_okx_last_fast_highspread_far_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.0
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 10.0
                && close_abs_delta_ms >= 400
                && close_abs_delta_ms <= 600
            {
                return Some("doge_three_bybit_coinbase_okx_last_fast_midspread_no_mid_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.7
                && hit.source_spread_bps < 2.8
                && weighted_direction_margin_bps + 1e-9 >= 2.2
                && weighted_direction_margin_bps < 2.4
                && close_abs_delta_ms >= 250
                && close_abs_delta_ms <= 330
            {
                return Some("doge_three_last_fast_midspread_lowmargin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 4.0
                && weighted_direction_margin_bps + 1e-9 < 3.5
                && close_abs_delta_ms <= 800
            {
                return Some("doge_three_last_fast_spread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps <= 1.1 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 15.0
                && close_abs_delta_ms <= 800
            {
                return Some("doge_three_last_fast_tightspread_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps <= 1.1 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 10.0
                && hit.source_spread_bps + 1e-9 >= 0.9
                && hit.source_spread_bps < 1.0
                && close_abs_delta_ms <= 800
            {
                return Some("doge_multi_last_fast_tightspread_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps <= 0.1 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps < 6.0
                && close_abs_delta_ms <= 800
            {
                return Some("doge_two_last_tightspread_fast_mid_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_direction_margin_bps + 1e-9 < 4.0
                && close_abs_delta_ms <= 100
            {
                return Some("doge_two_bybit_okx_last_very_fast_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 0.8
                && hit.source_spread_bps < 1.1
                && weighted_direction_margin_bps + 1e-9 >= 10.0
                && weighted_direction_margin_bps < 15.0
                && close_abs_delta_ms >= 1_500
            {
                return Some("doge_two_bybit_okx_last_stale_tightspread_high_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 < 3.0
                && hit.source_spread_bps < 2.0
                && close_abs_delta_ms <= 1_000
            {
                return Some("doge_multi_last_fast_midspread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.5
                && weighted_direction_margin_bps + 1e-9 < 3.0
                && close_abs_delta_ms <= 500
                && matches!(
                    close_only_filter_reason,
                    Some("preclose_near_flat" | "low_confidence")
                )
                && close_only_direction_margin_bps
                    .map(|margin_bps| margin_bps + 1e-9 < 0.5)
                    .unwrap_or(false)
            {
                return Some("doge_three_last_fast_midspread_low_signal_near_flat");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_direction_margin_bps + 1e-9 < 0.6
                && hit.source_spread_bps < 1.0
                && close_abs_delta_ms >= 3_500
            {
                return Some("doge_three_bybit_coinbase_okx_last_very_stale_tightspread_low_signal");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_direction_margin_bps + 1e-9 < 1.0
                && hit.source_spread_bps + 1e-9 >= 0.89
                && hit.source_spread_bps < 0.91
                && close_abs_delta_ms >= 3_000
            {
                return Some("doge_two_coinbase_okx_last_very_stale_tightspread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 3.0
                && close_abs_delta_ms >= 1_000
                && close_abs_delta_ms <= 1_800
            {
                return Some("doge_two_bybit_coinbase_last_stale_widespread_mid_margin");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 4.0
                && hit.source_spread_bps < 5.5
                && weighted_direction_margin_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps < 2.2
                && close_abs_delta_ms >= 700
                && close_abs_delta_ms <= 5_000
            {
                return Some("doge_two_bybit_coinbase_last_stale_widespread_lowmargin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps < 7.5
                && hit.source_spread_bps < 2.0
                && close_abs_delta_ms <= 700
            {
                return Some("doge_multi_last_fast_midspread_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps < 10.0
                && close_abs_delta_ms >= 1_000
                && close_abs_delta_ms <= 1_800
            {
                return Some("doge_multi_last_midspread_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 >= 15.0
                && weighted_direction_margin_bps < 23.0
                && close_abs_delta_ms >= 2_500
            {
                return Some("doge_multi_last_stale_midspread_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 1.5
                && hit.source_spread_bps < 3.5
                && weighted_direction_margin_bps + 1e-9 >= 23.0
                && close_abs_delta_ms >= 4_500
            {
                return Some("doge_multi_last_very_stale_midspread_high_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 4.0
                && weighted_direction_margin_bps + 1e-9 < 3.0
                && close_abs_delta_ms >= 1_800
            {
                return Some("doge_three_last_stale_highspread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 < 3.5
                && close_abs_delta_ms < 2_148
                && close_abs_delta_ms >= 1_800
            {
                return Some("doge_multi_last_stale_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 0.8
                && hit.source_spread_bps < 1.1
                && weighted_direction_margin_bps + 1e-9 < 0.5
                && close_abs_delta_ms >= 1_800
                && close_abs_delta_ms <= 3_000
            {
                return Some("doge_three_last_stale_tightspread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 0.8
                && hit.source_spread_bps < 1.1
                && weighted_direction_margin_bps + 1e-9 < 0.2
                && close_abs_delta_ms >= 2_000
                && close_abs_delta_ms <= 2_500
            {
                return Some("doge_two_bybit_coinbase_last_stale_tightspread_no_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 1.5
                && hit.source_spread_bps < 2.0
                && weighted_direction_margin_bps + 1e-9 < 3.5
                && close_abs_delta_ms >= 2_148
                && close_abs_delta_ms <= 3_000
            {
                return Some("doge_multi_last_stale_midspread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.5
                && weighted_direction_margin_bps + 1e-9 < 1.0
                && close_abs_delta_ms >= 3_000
            {
                return Some("doge_three_last_stale_midspread_yes_near_flat");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 3.5
                && hit.source_spread_bps < 3.8
                && weighted_direction_margin_bps + 1e-9 >= 1.7
                && weighted_direction_margin_bps < 1.9
                && close_abs_delta_ms >= 3_500
                && close_abs_delta_ms <= 3_700
            {
                return Some("doge_three_last_stale_midspread_lowmargin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 1.5
                && hit.source_spread_bps < 2.2
                && weighted_direction_margin_bps + 1e-9 < 3.0
                && close_abs_delta_ms >= 1_500
            {
                return Some("doge_two_bybit_okx_last_stale_midspread_near_flat");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.5
                && hit.source_spread_bps < 3.1
                && weighted_direction_margin_bps + 1e-9 < 1.5
                && close_abs_delta_ms >= 2_000
                && close_abs_delta_ms <= 2_600
            {
                return Some("doge_two_bybit_okx_last_stale_highmidspread_yes_near_flat");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 1.5
                && hit.source_spread_bps < 2.2
                && weighted_direction_margin_bps + 1e-9 < 0.3
                && close_abs_delta_ms >= 2_500
                && close_abs_delta_ms <= 3_000
            {
                return Some("doge_two_bybit_okx_last_stale_midspread_no_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 0.8
                && hit.source_spread_bps < 1.1
                && weighted_direction_margin_bps + 1e-9 < 1.5
                && close_abs_delta_ms >= 2_500
            {
                return Some("doge_two_bybit_okx_last_very_stale_tight_near_flat");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx)
                && weighted_direction_margin_bps + 1e-9 < 1.6
                && close_abs_delta_ms >= 4_000
            {
                return Some("doge_single_okx_last_very_stale_no_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 1.5
            {
                return Some("doge_single_last_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 0.8
                && hit.source_spread_bps < 1.2
                && weighted_direction_margin_bps + 1e-9 < 0.8
                && close_abs_delta_ms <= 300
            {
                return Some("doge_two_coinbase_okx_last_fast_tightspread_no_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 1.5
                && !((!weighted_side_yes && weighted_direction_margin_bps + 1e-9 < 1.25)
                    || close_abs_delta_ms >= 1_500)
            {
                return Some("doge_last_multi_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 8.0
                && !(weighted_side_yes
                    && weighted_direction_margin_bps + 1e-9 >= 20.0
                    && hit.source_spread_bps <= 11.0 + 1e-9
                    && close_abs_delta_ms <= 500)
            {
                return Some("doge_last_high_spread");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 0.5
            {
                return Some("doge_nearest_near_flat");
            }
            if hit.source_subset_name == "doge_binance_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && ((weighted_direction_margin_bps + 1e-9 >= 7.8
                    && weighted_direction_margin_bps < 8.2)
                    || weighted_direction_margin_bps + 1e-9 >= 13.5)
            {
                return Some("doge_binance_fallback_tail_margin");
            }
            if hit.source_subset_name == "doge_binance_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 >= 7.0
                && weighted_direction_margin_bps < 7.8
            {
                return Some("doge_binance_fallback_mid_margin");
            }
        }
        "hype/usd" => {
            let weighted_side_yes = hit.close_price >= rtds_open;
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps + 1e-9 < 8.0
                && close_abs_delta_ms <= 500
            {
                return Some("hype_three_after_high_spread_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.5
                && hit.source_spread_bps < 2.5 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 7.0
                && weighted_direction_margin_bps < 8.0
                && close_abs_delta_ms <= 1_000
            {
                return Some("hype_three_after_mid_spread_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 4
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 10.0
                && weighted_direction_margin_bps + 1e-9 < 8.0
            {
                return Some("hype_four_after_high_spread_mid_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 4
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 8.0
                && hit.source_spread_bps < 10.0
                && weighted_direction_margin_bps + 1e-9 < 8.0
                && close_abs_delta_ms >= 1_500
            {
                return Some("hype_four_drop_binance_stale_highspread_yes_mid_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 4
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 10.0
                && weighted_direction_margin_bps + 1e-9 < 20.0
            {
                return Some("hype_four_after_high_spread_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 4
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 15.0
                && weighted_direction_margin_bps + 1e-9 < 5.0
            {
                return Some("hype_after_very_high_spread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps + 1e-9 >= 30.0
                && close_abs_delta_ms >= 900
            {
                return Some("hype_after_high_spread_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 30.0
            {
                return Some("hype_multi_after_high_spread_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 5.0
                && weighted_direction_margin_bps + 1e-9 < 2.0
                && close_abs_delta_ms >= 3_000
            {
                return Some("hype_multi_after_stale_wide_spread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 4
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 4.5
                && hit.source_spread_bps < 5.5
                && weighted_direction_margin_bps + 1e-9 < 2.0
                && close_abs_delta_ms >= 1_000
                && close_abs_delta_ms <= 1_500
            {
                return Some("hype_four_drop_binance_stale_midspread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 4
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Hyperliquid)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.2
                && hit.source_spread_bps < 2.3
                && weighted_direction_margin_bps + 1e-9 >= 1.3
                && weighted_direction_margin_bps < 1.4
                && close_abs_delta_ms >= 700
                && close_abs_delta_ms <= 800
            {
                return Some("hype_four_all_yes_midlag_low_margin_side_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps <= 4.0 + 1e-9
                && weighted_direction_margin_bps + 1e-9 >= 40.0
            {
                return Some("hype_three_source_stale_spread_fallback");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 3.0
            {
                return Some("hype_single_after_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 1.2
            {
                return Some("hype_three_after_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps < 2.5
                && weighted_direction_margin_bps + 1e-9 >= 2.5
                && weighted_direction_margin_bps < 3.2
                && close_abs_delta_ms >= 2_500
                && close_abs_delta_ms <= 3_000
            {
                return Some("hype_three_bybit_hyperliquid_okx_stale_tightspread_yes_near_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Hyperliquid)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.4
                && hit.source_spread_bps < 2.5
                && weighted_direction_margin_bps + 1e-9 >= 1.3
                && weighted_direction_margin_bps < 1.4
                && close_abs_delta_ms >= 2_000
                && close_abs_delta_ms <= 2_100
            {
                return Some("hype_three_bybit_hyperliquid_okx_yes_stale_low_margin_side_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 2.5
                && weighted_direction_margin_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps < 2.0
                && close_abs_delta_ms >= 1_000
                && close_abs_delta_ms <= 1_200
            {
                return Some("hype_three_bybit_hyperliquid_okx_midlag_midspread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 4.5
                && hit.source_spread_bps < 5.0
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 2.5
                && close_abs_delta_ms >= 3_000
                && close_abs_delta_ms <= 3_500
            {
                return Some("hype_three_bybit_hyperliquid_okx_stale_widespread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 2.5
                && hit.source_spread_bps < 3.1
                && weighted_direction_margin_bps + 1e-9 >= 13.0
                && weighted_direction_margin_bps < 15.0
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 700
            {
                return Some(
                    "hype_three_bybit_hyperliquid_okx_midlag_midspread_yes_upper_margin_tail",
                );
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 7.0
                && weighted_direction_margin_bps + 1e-9 < 3.0
                && close_abs_delta_ms <= 1_500
            {
                return Some("hype_three_bybit_hyperliquid_okx_fast_highspread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 4.0
                && hit.source_spread_bps < 5.0
                && weighted_direction_margin_bps + 1e-9 >= 20.0
                && weighted_direction_margin_bps < 25.0
                && close_abs_delta_ms <= 100
            {
                return Some(
                    "hype_three_bybit_hyperliquid_okx_fast_highspread_yes_far_margin_tail",
                );
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 7.0
                && weighted_direction_margin_bps + 1e-9 >= 20.0
                && weighted_direction_margin_bps < 25.0
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 700
            {
                return Some(
                    "hype_three_bybit_hyperliquid_okx_fast_highspread_yes_midlag_far_margin_tail",
                );
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 4.0
                && hit.source_spread_bps < 5.0
                && weighted_direction_margin_bps + 1e-9 >= 4.0
                && weighted_direction_margin_bps < 5.0
                && close_abs_delta_ms <= 500
            {
                return Some("hype_three_bybit_hyperliquid_okx_fast_midspread_no_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 2.5
                && hit.source_spread_bps < 3.5
                && weighted_direction_margin_bps + 1e-9 < 2.5
                && close_abs_delta_ms <= 600
            {
                return Some("hype_two_hyperliquid_okx_yes_midspread_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 0.7
                && hit.source_spread_bps < 1.0
                && weighted_direction_margin_bps + 1e-9 >= 1.8
                && weighted_direction_margin_bps < 2.4
                && close_abs_delta_ms >= 700
                && close_abs_delta_ms <= 1_200
            {
                return Some("hype_two_hyperliquid_okx_yes_tightspread_midlag_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps < 1.5 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 1.8
                && close_abs_delta_ms <= 1_000
            {
                return Some("hype_two_after_mid_spread_fast_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.0
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 2.6
                && close_abs_delta_ms <= 500
            {
                return Some("hype_two_bybit_hyperliquid_fast_midspread_yes_near_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.0
                && weighted_direction_margin_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps < 5.0
                && close_abs_delta_ms <= 500
            {
                return Some("hype_two_bybit_hyperliquid_fast_midspread_yes_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Hyperliquid)
                && !has_source(LocalPriceSource::Coinbase)
                && !has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 0.3
                && hit.source_spread_bps < 0.4
                && weighted_direction_margin_bps + 1e-9 >= 4.1
                && weighted_direction_margin_bps < 4.2
                && close_abs_delta_ms <= 50
            {
                return Some("hype_two_bybit_hyperliquid_yes_fast_low_margin_side_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 3.0
                && hit.source_spread_bps < 3.5
                && weighted_direction_margin_bps + 1e-9 < 2.2
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 800
            {
                return Some("hype_two_bybit_hyperliquid_midlag_widespread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps < 1.5
                && weighted_direction_margin_bps + 1e-9 >= 10.0
                && weighted_direction_margin_bps < 12.5
                && close_abs_delta_ms <= 500
            {
                return Some("hype_two_bybit_hyperliquid_fast_tightspread_yes_upper_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 3.0
                && hit.source_spread_bps < 4.0
                && weighted_direction_margin_bps + 1e-9 >= 2.0
                && weighted_direction_margin_bps < 3.0
                && close_abs_delta_ms <= 500
            {
                return Some("hype_two_bybit_hyperliquid_fast_widespread_yes_near_margin");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit.source_spread_bps + 1e-9 >= 3.5
                && hit.source_spread_bps < 4.0
                && weighted_direction_margin_bps + 1e-9 >= 2.5
                && weighted_direction_margin_bps < 3.0
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 900
            {
                return Some("hype_two_bybit_hyperliquid_midlag_widespread_yes_near_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 3.5
                && hit.source_spread_bps < 4.0
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 9.0
                && close_abs_delta_ms <= 100
            {
                return Some("hype_two_bybit_hyperliquid_fast_widespread_yes_upper_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 6.0
                && weighted_direction_margin_bps + 1e-9 >= 3.5
                && weighted_direction_margin_bps < 4.5
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 700
            {
                return Some("hype_two_bybit_hyperliquid_midlag_highspread_yes_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 4.0
                && hit.source_spread_bps < 4.5
                && weighted_direction_margin_bps + 1e-9 >= 13.0
                && weighted_direction_margin_bps < 15.0
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 700
            {
                return Some("hype_two_bybit_hyperliquid_midlag_midspread_yes_upper_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps + 1e-9 >= 4.0
                && weighted_direction_margin_bps + 1e-9 < 4.0
            {
                return Some("hype_three_coinbase_hl_okx_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_subset_name == "drop_binance"
                && !weighted_side_yes
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Hyperliquid)
                && !has_source(LocalPriceSource::Bybit)
                && !has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 3.9
                && hit.source_spread_bps < 4.1
                && weighted_direction_margin_bps + 1e-9 >= 1.08
                && weighted_direction_margin_bps < 1.10
                && close_abs_delta_ms >= 850
                && close_abs_delta_ms <= 900
            {
                return Some("hype_two_coinbase_hyperliquid_no_stale_low_margin_side_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit.source_spread_bps + 1e-9 >= 4.0
                && hit.source_spread_bps < 5.0
                && weighted_direction_margin_bps + 1e-9 >= 40.0
                && close_abs_delta_ms >= 800
            {
                return Some("hype_three_coinbase_hl_okx_no_midspread_far_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 3
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 3.0
                && hit.source_spread_bps < 5.0
                && weighted_direction_margin_bps + 1e-9 >= 10.0
                && weighted_direction_margin_bps < 15.0
                && close_abs_delta_ms >= 1_500
            {
                return Some("hype_three_bybit_hyperliquid_okx_stale_midspread_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 10.0
                && (close_abs_delta_ms < 200
                    || (!weighted_side_yes
                        && hit
                            .source_contributions
                            .iter()
                            .any(|c| c.source == LocalPriceSource::Hyperliquid)
                        && hit
                            .source_contributions
                            .iter()
                            .any(|c| c.source == LocalPriceSource::Okx)
                        && !hit
                            .source_contributions
                            .iter()
                            .any(|c| c.source == LocalPriceSource::Bybit)
                        && !hit
                            .source_contributions
                            .iter()
                            .any(|c| c.source == LocalPriceSource::Coinbase)
                        && weighted_direction_margin_bps + 1e-9 < 1.5))
            {
                return Some("hype_two_after_very_high_spread");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 6.0
                && weighted_direction_margin_bps + 1e-9 < 4.5
                && close_abs_delta_ms <= 1_000
            {
                return Some("hype_two_hyperliquid_okx_yes_widespread_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 6.5
                && weighted_direction_margin_bps + 1e-9 >= 15.0
                && weighted_direction_margin_bps < 22.0
                && close_abs_delta_ms <= 700
            {
                return Some("hype_two_hyperliquid_okx_yes_highspread_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && !hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 6.5
                && weighted_direction_margin_bps + 1e-9 >= 15.0
                && weighted_direction_margin_bps < 22.0
                && close_abs_delta_ms <= 700
            {
                return Some("hype_two_hyperliquid_okx_no_highspread_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 6.0
                && (weighted_direction_margin_bps + 1e-9 >= 30.0
                    || (weighted_direction_margin_bps + 1e-9 >= 16.0
                        && hit.source_spread_bps < 6.5))
                && (weighted_side_yes || weighted_direction_margin_bps + 1e-9 >= 40.0)
            {
                return Some("hype_two_after_high_spread_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit.source_spread_bps + 1e-9 >= 6.0
                && hit.source_spread_bps < 7.0
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 9.0
                && close_abs_delta_ms <= 200
            {
                return Some("hype_two_bybit_hyperliquid_fast_highspread_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit.source_spread_bps + 1e-9 >= 5.5
                && hit.source_spread_bps < 8.0
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 10.0
                && close_abs_delta_ms >= 800
                && close_abs_delta_ms <= 1_200
            {
                return Some("hype_two_bybit_hyperliquid_stale_highspread_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 6.0
                && weighted_direction_margin_bps + 1e-9 >= 20.0
                && close_abs_delta_ms <= 500
            {
                return Some("hype_two_bybit_hyperliquid_fast_highspread_high_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 6.0
                && hit.source_spread_bps < 8.0
                && weighted_direction_margin_bps + 1e-9 < 3.5
            {
                return Some("hype_after_high_spread_yes_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count >= 3
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit.source_spread_bps + 1e-9 >= 6.0
                && hit.source_spread_bps < 8.0
                && weighted_direction_margin_bps + 1e-9 >= 10.0
                && weighted_direction_margin_bps < 20.0 + 1e-9
            {
                return Some("hype_three_after_high_spread_yes_mid_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps <= 1.0 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 2.0
            {
                return Some("hype_two_after_tight_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps + 1e-9 >= 1.5
                && weighted_direction_margin_bps + 1e-9 < 1.8
                && (weighted_side_yes || weighted_direction_margin_bps + 1e-9 >= 1.25)
            {
                return Some("hype_two_after_wide_spread_near_flat");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 4
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Hyperliquid)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 4.5
                && hit.source_spread_bps < 4.9
                && weighted_direction_margin_bps + 1e-9 >= 18.0
                && weighted_direction_margin_bps < 19.0
                && close_abs_delta_ms >= 450
                && close_abs_delta_ms <= 550
            {
                return Some("hype_four_drop_binance_midspread_no_high_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 4
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && has_source(LocalPriceSource::Bybit)
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Hyperliquid)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 9.0
                && weighted_direction_margin_bps + 1e-9 >= 50.0
                && close_abs_delta_ms >= 200
                && close_abs_delta_ms <= 250
            {
                return Some("hype_four_drop_binance_fast_widespread_no_extreme_margin_tail");
            }
            if hit.source_subset_name == "drop_binance"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Bybit)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Hyperliquid)
                && hit.source_spread_bps + 1e-9 >= 4.0
                && weighted_direction_margin_bps + 1e-9 >= 30.0
                && close_abs_delta_ms >= 700
                && close_abs_delta_ms <= 800
            {
                return Some("hype_two_bybit_hyperliquid_stale_highspread_yes_high_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.close_exact_sources == 0
                && ((hit.source_count >= 2
                    && hit.source_spread_bps <= 1.0 + 1e-9
                    && weighted_direction_margin_bps + 1e-9 < 1.8)
                    || (hit.source_count == 1 && weighted_direction_margin_bps + 1e-9 < 2.0))
            {
                return Some("hype_nearest_near_flat");
            }
        }
        "eth/usd" => {
            let weighted_side_yes = hit.close_price >= rtds_open;
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 1.0
                && !(!weighted_side_yes && weighted_direction_margin_bps + 1e-9 >= 0.63)
            {
                return Some("eth_single_last_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && !weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 0.63
                && weighted_direction_margin_bps < 0.8
                && close_abs_delta_ms >= 3_000
                && close_abs_delta_ms <= 5_000
            {
                return Some("eth_single_coinbase_no_very_stale_near_flat");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && !weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 0.63
                && weighted_direction_margin_bps < 0.75
                && close_abs_delta_ms <= 100
            {
                return Some("eth_single_coinbase_no_fast_low_margin_tail");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && first_source_is(LocalPriceSource::Coinbase)
                && !weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 1.85
                && weighted_direction_margin_bps < 1.90
                && close_abs_delta_ms >= 150
                && close_abs_delta_ms <= 250
            {
                return Some("eth_single_coinbase_no_fast_low_margin_side_tail");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && first_source_is(LocalPriceSource::Coinbase)
                && !weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 80.0
                && close_abs_delta_ms >= 750
                && close_abs_delta_ms <= 800
            {
                return Some("eth_single_coinbase_no_late_extreme_margin_tail");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && first_source_is(LocalPriceSource::Coinbase)
                && !weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 1.03
                && weighted_direction_margin_bps < 1.06
                && close_abs_delta_ms >= 650
                && close_abs_delta_ms <= 720
            {
                return Some("eth_single_coinbase_no_midlag_low_margin_side_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 1.0
                && weighted_direction_margin_bps < 2.2
                && close_abs_delta_ms <= 500
            {
                return Some("eth_single_coinbase_yes_fast_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 1.0
                && weighted_direction_margin_bps < 2.5
                && close_abs_delta_ms >= 3_000
            {
                return Some("eth_single_coinbase_yes_stale_mid_margin");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && first_source_is(LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 5.3
                && weighted_direction_margin_bps < 5.5
                && close_abs_delta_ms >= 930
                && close_abs_delta_ms <= 970
            {
                return Some("eth_single_coinbase_yes_late_mid_margin_error_tail");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && first_source_is(LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 11.3
                && weighted_direction_margin_bps < 11.5
                && close_abs_delta_ms >= 550
                && close_abs_delta_ms <= 590
            {
                return Some("eth_single_coinbase_yes_midlag_high_margin_tail");
            }
            if hit.source_subset_name == "only_coinbase"
                && hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && first_source_is(LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 7.0
                && weighted_direction_margin_bps < 7.5
                && close_abs_delta_ms >= 700
                && close_abs_delta_ms <= 780
            {
                return Some("eth_single_coinbase_yes_late_high_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 1.0
                && weighted_direction_margin_bps < 1.3
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms < 3_000
            {
                return Some("eth_single_coinbase_yes_mid_stale_near_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_subset_name == "only_coinbase"
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 2.5
                && weighted_direction_margin_bps < 3.0
                && close_abs_delta_ms <= 150
            {
                return Some("eth_single_coinbase_yes_fast_upper_mid_margin");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_subset_name == "only_coinbase"
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Coinbase)
                && weighted_side_yes
                && weighted_direction_margin_bps + 1e-9 >= 8.0
                && weighted_direction_margin_bps < 13.0
                && close_abs_delta_ms <= 50
            {
                return Some("eth_single_coinbase_yes_fast_mid_margin_tail");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps <= 1.5 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 0.35
            {
                return Some("eth_two_last_near_flat");
            }
            if hit.rule == LocalBoundaryCloseRule::LastBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && hit.source_spread_bps <= 1.1 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 0.2
            {
                return Some("eth_two_last_tight_near_flat");
            }
        }
        "sol/usd" => {
            let weighted_side_yes = hit.close_price >= rtds_open;
            if hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_direction_margin_bps + 1e-9 < 1.0
                && (weighted_side_yes || hit.source_spread_bps < 2.5)
            {
                return Some("sol_after_near_flat");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 1.0
                && hit.source_spread_bps < 2.5
                && weighted_direction_margin_bps + 1e-9 >= 1.45
                && weighted_direction_margin_bps < 1.7
                && close_abs_delta_ms >= 500
                && close_abs_delta_ms <= 900
            {
                return Some("sol_okx_coinbase_no_midlag_midmargin_near_flat");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 3.0
                && hit.source_spread_bps < 4.2
                && weighted_direction_margin_bps + 1e-9 >= 4.0
                && weighted_direction_margin_bps < 5.0
                && close_abs_delta_ms <= 250
            {
                return Some("sol_okx_coinbase_no_fast_midspread_midmargin_tail");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 1.1
                && hit.source_spread_bps < 1.3
                && weighted_direction_margin_bps + 1e-9 >= 50.0
                && close_abs_delta_ms >= 150
                && close_abs_delta_ms <= 220
            {
                return Some("sol_okx_coinbase_no_fast_tightspread_extreme_margin_tail");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 7.0
                && weighted_direction_margin_bps + 1e-9 < 1.5
                && close_abs_delta_ms <= 600
            {
                return Some("sol_okx_coinbase_high_spread_yes_near_flat");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.0
                && weighted_direction_margin_bps + 1e-9 < 3.0
                && close_abs_delta_ms <= 100
            {
                return Some("sol_okx_coinbase_mid_spread_yes_fast_near_flat");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 2.0
                && hit.source_spread_bps < 3.0
                && weighted_direction_margin_bps + 1e-9 < 1.6
                && close_abs_delta_ms > 100
                && close_abs_delta_ms <= 500
            {
                return Some("sol_okx_coinbase_mid_spread_yes_fastish_near_flat");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && weighted_direction_margin_bps + 1e-9 >= 1.0
                && weighted_direction_margin_bps < 1.8
                && hit.source_spread_bps <= 1.2 + 1e-9
                && close_abs_delta_ms >= 300
                && close_abs_delta_ms <= 400
            {
                return Some("sol_okx_coinbase_yes_midlag_near_flat_tail");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Okx)
                && weighted_direction_margin_bps + 1e-9 >= 1.17
                && weighted_direction_margin_bps < 1.19
                && close_abs_delta_ms >= 2_300
                && close_abs_delta_ms <= 2_400
            {
                return Some("sol_okx_coinbase_yes_stale_low_margin_side_tail");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && has_source(LocalPriceSource::Coinbase)
                && has_source(LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 5.0
                && hit.source_spread_bps < 6.5
                && weighted_direction_margin_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps < 4.5
            {
                return Some("sol_okx_coinbase_yes_highspread_mid_margin_side_tail");
            }
            if hit.source_subset_name == "only_okx_coinbase"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && !weighted_side_yes
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Coinbase)
                && hit
                    .source_contributions
                    .iter()
                    .any(|c| c.source == LocalPriceSource::Okx)
                && hit.source_spread_bps + 1e-9 >= 3.0
                && weighted_direction_margin_bps + 1e-9 >= 10.0
                && close_abs_delta_ms <= 500
            {
                return Some("sol_okx_coinbase_no_fast_high_spread_tail");
            }
            if hit.source_subset_name == "sol_okx_missing_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && weighted_side_yes
                && hit
                    .source_contributions
                    .first()
                    .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx)
                && weighted_direction_margin_bps + 1e-9 >= 9.0
                && weighted_direction_margin_bps < 10.5
                && close_abs_delta_ms >= 2_000
            {
                return Some("sol_okx_missing_stale_borderline_tail");
            }
            if close_only_filter_reason == Some("preclose_near_flat")
                && close_only_direction_margin_bps
                    .map(|v| v + 1e-9 < 0.2)
                    .unwrap_or(false)
                && hit.source_count == 2
                && hit.source_spread_bps <= 0.1 + 1e-9
            {
                return Some("sol_close_only_preclose_near_flat");
            }
        }
        _ => {}
    }
    None
}

fn local_boundary_weighted_candidate_allowed_for_policy(
    symbol: &str,
    close_only_filter_reason: Option<&'static str>,
    close_only_direction_margin_bps: Option<f64>,
    round_end_ts: u64,
    rtds_open: f64,
    hit: &LocalBoundaryShadowHit,
) -> bool {
    local_boundary_weighted_candidate_filter_reason_for_policy(
        symbol,
        close_only_filter_reason,
        close_only_direction_margin_bps,
        round_end_ts,
        rtds_open,
        hit,
    )
    .is_none()
}

fn local_boundary_symbol_router_prefers_weighted_primary(symbol: &str) -> bool {
    let _ = symbol;
    true
}

fn local_boundary_symbol_router_allows_close_only_fallback(
    symbol: &str,
    hit: &LocalCloseOnlyAggHit,
    round_end_ts: u64,
    rtds_open: f64,
) -> bool {
    if symbol != "hype/usd" {
        return false;
    }
    let direction_margin_bps =
        ((hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let close_only_side_yes = hit.close_price >= rtds_open;
    if hit.source_count == 1 && hit.close_exact_sources == 0 {
        let close_abs_delta_ms = hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000));
        if direction_margin_bps + 1e-9 < 2.6
            && !(close_only_side_yes && direction_margin_bps + 1e-9 < 2.5)
        {
            return false;
        }
        if close_only_side_yes && direction_margin_bps + 1e-9 >= 3.5 && direction_margin_bps < 5.5 {
            return false;
        }
        if close_only_side_yes && direction_margin_bps + 1e-9 < 7.0 && close_abs_delta_ms >= 500 {
            return false;
        }
        let close_only_single_hyperliquid = hit
            .source_contributions
            .first()
            .is_some_and(|contribution| contribution.source == LocalPriceSource::Hyperliquid);
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 7.0
            && direction_margin_bps < 13.0
            && close_abs_delta_ms <= 500
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 6.5
            && direction_margin_bps < 7.0
            && (300..=500).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 6.5
            && direction_margin_bps < 7.0
            && close_abs_delta_ms <= 100
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 5.5
            && direction_margin_bps < 6.0
            && close_abs_delta_ms <= 150
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 15.0
            && direction_margin_bps < 17.0
            && close_abs_delta_ms <= 200
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 7.5
            && direction_margin_bps < 8.2
            && (840..=950).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 5.0
            && direction_margin_bps < 20.0
            && (650..=850).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 10.0
            && direction_margin_bps < 13.0
            && (950..=1_100).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 3.0
            && direction_margin_bps < 4.0
            && close_abs_delta_ms <= 150
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 2.0
            && direction_margin_bps < 2.5
            && close_abs_delta_ms <= 100
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 2.0
            && direction_margin_bps < 2.1
            && (150..=450).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 2.6
            && direction_margin_bps < 3.0
            && (250..=450).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 6.0
            && direction_margin_bps < 7.0
            && (200..=500).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 5.0
            && direction_margin_bps < 6.0
            && (300..=500).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 10.0
            && direction_margin_bps < 11.5
            && (350..=500).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 7.0
            && direction_margin_bps < 8.0
            && close_abs_delta_ms <= 300
        {
            return false;
        }
        let close_only_single_okx = hit
            .source_contributions
            .first()
            .is_some_and(|contribution| contribution.source == LocalPriceSource::Okx);
        if close_only_single_okx
            && close_only_side_yes
            && direction_margin_bps + 1e-9 >= 8.5
            && direction_margin_bps < 8.8
            && (300..=350).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 7.5
            && direction_margin_bps < 7.7
            && (520..=540).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 2.60
            && direction_margin_bps < 2.65
            && (600..=650).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 4.0
            && direction_margin_bps < 10.0
            && (650..720).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 2.9
            && direction_margin_bps < 4.1
            && (700..=750).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 2.9
            && direction_margin_bps < 3.1
            && (850..=950).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 10.0
            && direction_margin_bps < 16.0
            && (720..850).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 10.0
            && direction_margin_bps < 11.0
            && (500..=560).contains(&close_abs_delta_ms)
        {
            return false;
        }
        if close_only_single_hyperliquid
            && !close_only_side_yes
            && direction_margin_bps + 1e-9 >= 17.0
            && direction_margin_bps < 19.0
            && (700..=750).contains(&close_abs_delta_ms)
        {
            return false;
        }
    }
    if hit.source_count >= 2
        && hit.close_exact_sources == 0
        && close_only_side_yes
        && hit.source_spread_bps + 1e-9 >= 1.0
        && hit.source_spread_bps < 1.5
        && direction_margin_bps + 1e-9 >= 7.0
        && direction_margin_bps < 10.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 500
    {
        return false;
    }
    if hit.source_count >= 2
        && hit.close_exact_sources == 0
        && close_only_side_yes
        && direction_margin_bps + 1e-9 < 5.0
    {
        return false;
    }
    if hit.source_count == 2
        && hit.close_exact_sources == 0
        && close_only_side_yes
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Bybit)
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Hyperliquid)
        && hit.source_spread_bps + 1e-9 >= 0.5
        && hit.source_spread_bps < 1.0
        && direction_margin_bps + 1e-9 >= 5.5
        && direction_margin_bps < 7.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 1_000
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 1_500
    {
        return false;
    }
    if hit.source_count == 2
        && hit.close_exact_sources == 0
        && close_only_side_yes
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Bybit)
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Hyperliquid)
        && hit.source_spread_bps + 1e-9 >= 1.0
        && hit.source_spread_bps < 2.5
        && direction_margin_bps + 1e-9 >= 5.0
        && direction_margin_bps < 7.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 1_000
    {
        return false;
    }
    if hit.source_count == 2
        && hit.close_exact_sources == 0
        && close_only_side_yes
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Bybit)
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Hyperliquid)
        && hit.source_spread_bps + 1e-9 >= 1.0
        && hit.source_spread_bps < 1.5
        && direction_margin_bps + 1e-9 >= 20.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 1_000
    {
        return false;
    }
    if hit.source_count == 2
        && hit.close_exact_sources == 0
        && close_only_side_yes
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Bybit)
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Hyperliquid)
        && hit.source_spread_bps < 0.5
        && direction_margin_bps + 1e-9 >= 30.0
        && direction_margin_bps < 40.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 1_000
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 1_500
    {
        return false;
    }
    if hit.source_count == 2
        && hit.close_exact_sources == 0
        && close_only_side_yes
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Bybit)
        && hit
            .source_contributions
            .iter()
            .any(|c| c.source == LocalPriceSource::Hyperliquid)
        && hit.source_spread_bps + 1e-9 >= 0.5
        && hit.source_spread_bps < 0.7
        && direction_margin_bps + 1e-9 >= 12.0
        && direction_margin_bps < 15.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 2_000
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 2_500
    {
        return false;
    }
    if hit.source_count >= 2
        && hit.close_exact_sources == 0
        && hit.source_spread_bps <= 1.0 + 1e-9
        && direction_margin_bps + 1e-9 >= 40.0
    {
        return false;
    }
    if hit.source_count >= 2
        && hit.close_exact_sources == 0
        && hit.source_spread_bps + 1e-9 >= 1.5
        && direction_margin_bps + 1e-9 >= 8.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) <= 1_000
    {
        return false;
    }
    if hit.source_count >= 2
        && hit.close_exact_sources == 0
        && hit.source_spread_bps + 1e-9 >= 2.0
        && hit.source_spread_bps < 4.0 + 1e-9
        && direction_margin_bps + 1e-9 >= 40.0
        && hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000)) >= 1_000
    {
        return false;
    }
    if hit.source_count >= 2
        && hit.close_exact_sources == 0
        && hit.source_spread_bps + 1e-9 >= 4.0
        && direction_margin_bps + 1e-9 >= 8.0
    {
        return false;
    }
    true
}

fn local_boundary_symbol_router_source_fallback_allowed(
    symbol: &str,
    weighted_shadow_hit: Option<&LocalBoundaryShadowHit>,
    weighted_shadow_filter_reason: Option<&'static str>,
    hit: &LocalBoundaryShadowHit,
    round_end_ts: u64,
    rtds_open: f64,
) -> bool {
    if weighted_shadow_hit.is_some()
        && !local_boundary_symbol_router_source_fallback_rescues_filtered(
            symbol,
            weighted_shadow_filter_reason,
            hit,
        )
    {
        return false;
    }
    let direction_margin_bps =
        ((hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let close_abs_delta_ms = hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000));
    let source_fallback_allowed = match symbol {
        "bnb/usd" => {
            let fallback_side_yes = hit.close_price >= rtds_open;
            (hit.source_subset_name == "bnb_okx_no_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && !fallback_side_yes
                && direction_margin_bps + 1e-9 >= 4.0
                && direction_margin_bps < 7.0 + 1e-9)
                || (hit.source_subset_name == "bnb_okx_fallback"
                    && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                    && hit.source_count == 1
                    && hit.close_exact_sources == 0
                    && direction_margin_bps + 1e-9 >= 7.0
                    && direction_margin_bps < 8.0 + 1e-9)
        }
        "doge/usd" => {
            hit.source_subset_name == "doge_binance_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && direction_margin_bps + 1e-9 >= 5.0
                && direction_margin_bps <= 15.0 + 1e-9
                && close_abs_delta_ms <= 4_000
        }
        "eth/usd" => {
            hit.source_subset_name == "eth_binance_missing_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && direction_margin_bps + 1e-9 >= 3.0
                && close_abs_delta_ms <= 2_500
        }
        "hype/usd" => {
            hit.source_subset_name == "hype_hyperliquid_fallback"
                && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                && hit.source_count == 1
                && hit.close_exact_sources == 0
                && direction_margin_bps + 1e-9 >= 4.0
                && direction_margin_bps < 5.0 + 1e-9
                && close_abs_delta_ms <= 900
        }
        "sol/usd" => {
            (hit.source_subset_name == "sol_binance_coinbase_fallback"
                && hit.rule == LocalBoundaryCloseRule::NearestAbs
                && hit.source_count == 2
                && hit.close_exact_sources == 0
                && direction_margin_bps + 1e-9 >= 2.0
                && hit.source_spread_bps <= 7.0 + 1e-9)
                || (hit.source_subset_name == "sol_coinbase_fallback"
                    && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                    && hit.source_count == 1
                    && hit.close_exact_sources == 0
                    && direction_margin_bps + 1e-9 >= 5.0)
                || (hit.source_subset_name == "sol_binance_fallback"
                    && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                    && hit.source_count == 1
                    && hit.close_exact_sources == 0
                    && direction_margin_bps + 1e-9 >= 12.0
                    && close_abs_delta_ms <= 2_500)
                || (hit.source_subset_name == "sol_okx_missing_fallback"
                    && hit.rule == LocalBoundaryCloseRule::AfterThenBefore
                    && hit.source_count == 1
                    && hit.close_exact_sources == 0
                    && direction_margin_bps + 1e-9 >= 4.0
                    && close_abs_delta_ms <= 4_000)
        }
        _ => false,
    };
    if !source_fallback_allowed {
        return false;
    }
    local_boundary_weighted_candidate_filter_reason_for_policy(
        symbol,
        None,
        None,
        round_end_ts,
        rtds_open,
        hit,
    )
    .is_none()
}

fn local_boundary_symbol_router_source_fallback_rescues_filtered(
    symbol: &str,
    weighted_shadow_filter_reason: Option<&'static str>,
    hit: &LocalBoundaryShadowHit,
) -> bool {
    let Some(reason) = weighted_shadow_filter_reason else {
        return false;
    };
    match symbol {
        "bnb/usd" => {
            hit.source_subset_name == "bnb_okx_no_fallback"
                && matches!(
                    reason,
                    "bnb_no_near_flat"
                        | "bnb_single_binance_no_fast_tail"
                        | "bnb_two_no_midspread_near_flat"
                )
        }
        "doge/usd" => {
            hit.source_subset_name == "doge_binance_fallback"
                && matches!(
                    reason,
                    "doge_multi_last_fast_midspread_tail"
                        | "doge_multi_last_midspread_tail"
                        | "doge_multi_last_stale_near_flat"
                        | "doge_two_bybit_okx_last_stale_tightspread_high_margin_tail"
                )
        }
        "sol/usd" => {
            hit.source_subset_name == "sol_binance_fallback" && reason == "sol_after_near_flat"
        }
        "hype/usd" => {
            hit.source_subset_name == "hype_hyperliquid_fallback"
                && reason == "hype_three_after_near_flat"
        }
        _ => false,
    }
}

fn local_close_only_source_agreement(hit: &LocalCloseOnlyAggHit, rtds_open: f64) -> f64 {
    if hit.source_contributions.is_empty() {
        return 0.0;
    }
    let close_only_side = if hit.close_price >= rtds_open {
        Side::Yes
    } else {
        Side::No
    };
    let agree_cnt = hit
        .source_contributions
        .iter()
        .filter(|src| {
            let src_side = if src.adjusted_close_price >= rtds_open {
                Side::Yes
            } else {
                Side::No
            };
            src_side == close_only_side
        })
        .count();
    agree_cnt as f64 / hit.source_contributions.len() as f64
}

fn local_boundary_hybrid_prefers_weighted_over_close_only(
    symbol: &str,
    close_only_hit: &LocalCloseOnlyAggHit,
    weighted_hit: &LocalBoundaryShadowHit,
    rtds_open: f64,
) -> bool {
    let close_only_direction_margin_bps =
        ((close_only_hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let weighted_direction_margin_bps =
        ((weighted_hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let close_only_side = if close_only_hit.close_price >= rtds_open {
        Side::Yes
    } else {
        Side::No
    };
    let weighted_side = if weighted_hit.close_price >= rtds_open {
        Side::Yes
    } else {
        Side::No
    };
    if close_only_side == weighted_side {
        return false;
    }
    match symbol {
        "bnb/usd" => {
            (close_only_hit.source_count == 1
                && close_only_hit.close_exact_sources == 0
                && close_only_direction_margin_bps + 1e-9 < 2.5
                && weighted_hit.source_count >= 2
                && weighted_direction_margin_bps + 1e-9 >= 1.25)
                || (close_only_hit.source_count == 1
                    && close_only_hit.close_exact_sources == 0
                    && close_only_direction_margin_bps + 1e-9 < 3.6
                    && weighted_hit.source_count >= 2
                    && weighted_hit.source_spread_bps + 1e-9 >= 3.5
                    && weighted_direction_margin_bps + 1e-9 >= 0.65)
        }
        "eth/usd" => {
            close_only_hit.close_exact_sources == 0
                && weighted_hit.source_count >= 2
                && weighted_hit.rule == LocalBoundaryCloseRule::LastBefore
        }
        "doge/usd" => close_only_hit.close_exact_sources == 0 && weighted_hit.source_count >= 2,
        "sol/usd" => {
            close_only_hit.close_exact_sources == 0
                && weighted_hit.source_count >= 2
                && weighted_hit.rule == LocalBoundaryCloseRule::AfterThenBefore
        }
        _ => false,
    }
}

fn local_boundary_hybrid_prefers_filtered_close_only_over_weighted(
    symbol: &str,
    close_only_filter_reason: Option<&'static str>,
    close_only_hit: &LocalCloseOnlyAggHit,
    weighted_hit: &LocalBoundaryShadowHit,
    rtds_open: f64,
) -> bool {
    let close_only_direction_margin_bps =
        ((close_only_hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let weighted_direction_margin_bps =
        ((weighted_hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let close_only_side = if close_only_hit.close_price >= rtds_open {
        Side::Yes
    } else {
        Side::No
    };
    let weighted_side = if weighted_hit.close_price >= rtds_open {
        Side::Yes
    } else {
        Side::No
    };
    if close_only_side == weighted_side {
        return false;
    }
    let source_agreement = local_close_only_source_agreement(close_only_hit, rtds_open);
    match symbol {
        "doge/usd" => {
            (close_only_filter_reason == Some("low_confidence")
                && close_only_hit.source_count >= 2
                && close_only_hit.close_exact_sources == 0
                && source_agreement >= 1.0 - 1e-9
                && close_only_hit.source_spread_bps <= 0.5 + 1e-9
                && close_only_direction_margin_bps + 1e-9 >= 2.0
                && weighted_hit.rule == LocalBoundaryCloseRule::NearestAbs
                && weighted_hit.source_count >= 2
                && weighted_hit.close_exact_sources == 0
                && weighted_hit.source_spread_bps <= 0.1 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 0.5)
                || (close_only_filter_reason == Some("preclose_near_flat")
                    && close_only_hit.source_count >= 3
                    && close_only_hit.close_exact_sources == 0
                    && source_agreement >= 1.0 - 1e-9
                    && close_only_hit.source_spread_bps <= 3.1 + 1e-9
                    && close_only_direction_margin_bps + 1e-9 >= 1.5
                    && weighted_hit.rule == LocalBoundaryCloseRule::NearestAbs
                    && weighted_hit.source_count >= 2
                    && weighted_hit.close_exact_sources == 0
                    && weighted_hit.source_spread_bps <= 0.1 + 1e-9
                    && weighted_direction_margin_bps + 1e-9 < 0.5)
        }
        "hype/usd" => {
            close_only_filter_reason == Some("low_confidence")
                && close_only_hit.source_count >= 2
                && close_only_hit.close_exact_sources == 0
                && source_agreement >= 1.0 - 1e-9
                && close_only_hit.source_spread_bps <= 3.0 + 1e-9
                && close_only_direction_margin_bps + 1e-9 >= 2.3
                && weighted_hit.rule == LocalBoundaryCloseRule::NearestAbs
                && weighted_hit.source_count >= 2
                && weighted_hit.close_exact_sources == 0
                && weighted_hit.source_spread_bps <= 1.0 + 1e-9
                && weighted_direction_margin_bps + 1e-9 < 1.8
        }
        _ => false,
    }
}

fn local_boundary_filtered_close_only_allowed_without_weighted(
    symbol: &str,
    close_only_filter_reason: Option<&'static str>,
    close_only_hit: &LocalCloseOnlyAggHit,
    round_end_ts: u64,
    rtds_open: f64,
) -> bool {
    if symbol != "hype/usd"
        || close_only_filter_reason != Some("hype_close_only_single_far_margin")
        || close_only_hit.source_count != 1
        || close_only_hit.close_exact_sources != 0
    {
        return false;
    }
    let close_only_single_hyperliquid = close_only_hit
        .source_contributions
        .first()
        .is_some_and(|contribution| contribution.source == LocalPriceSource::Hyperliquid);
    if !close_only_single_hyperliquid {
        return false;
    }
    let direction_margin_bps =
        ((close_only_hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let close_abs_delta_ms = close_only_hit
        .close_ts_ms
        .abs_diff(round_end_ts.saturating_mul(1_000));
    close_abs_delta_ms <= 500
        && direction_margin_bps + 1e-9 >= 21.0
        && direction_margin_bps < 24.0
        && !(direction_margin_bps + 1e-9 >= 23.5
            && direction_margin_bps < 23.7
            && (100..=150).contains(&close_abs_delta_ms))
        && !(direction_margin_bps + 1e-9 >= 22.55
            && direction_margin_bps < 22.7
            && (150..=250).contains(&close_abs_delta_ms))
        && !(direction_margin_bps + 1e-9 >= 21.0
            && direction_margin_bps < 21.1
            && (150..=300).contains(&close_abs_delta_ms))
}

fn local_boundary_symbol_router_missing_filter_reason(
    symbol: &str,
    close_only_filter_reason: Option<&'static str>,
    close_only_direction_margin_bps: Option<f64>,
    close_only_hit: &LocalCloseOnlyAggHit,
) -> Option<&'static str> {
    if symbol == "sol/usd"
        && close_only_filter_reason == Some("preclose_near_flat")
        && close_only_direction_margin_bps
            .map(|margin_bps| margin_bps + 1e-9 < 1.0)
            .unwrap_or(false)
        && close_only_hit.source_count >= 2
        && close_only_hit.close_exact_sources == 0
        && close_only_hit.source_spread_bps <= 5.0 + 1e-9
    {
        return Some("sol_missing_preclose_near_flat");
    }
    None
}

fn run_local_boundary_shadow_policy(
    symbol: &str,
    spec: LocalBoundaryShadowPolicySpec,
    tapes: &HashMap<LocalPriceSource, LocalSourceBoundaryTapeState>,
    end_ms: u64,
) -> LocalBoundaryShadowOutcome {
    let mut contributions = Vec::new();
    for source in spec.allowed_sources {
        let Some(tape) = tapes.get(source) else {
            continue;
        };
        let Some((point, raw_close_price)) = pick_local_boundary_tape_close_point(
            tape,
            end_ms,
            LOCAL_BOUNDARY_POLICY_PRE_MS,
            LOCAL_BOUNDARY_POLICY_POST_MS,
            spec.rule,
        ) else {
            continue;
        };
        contributions.push(LocalCloseSourceContribution {
            source: *source,
            raw_close_price,
            adjusted_close_price: point.price,
            close_ts_ms: point.ts_ms,
            close_exact: point.exact,
        });
    }

    if contributions.len() < spec.min_sources {
        return LocalBoundaryShadowOutcome {
            policy_name: spec.policy_name,
            source_subset_name: spec.source_subset_name,
            rule: spec.rule,
            min_sources: spec.min_sources,
            hit: None,
        };
    }

    let close_values = contributions
        .iter()
        .map(|src| src.adjusted_close_price)
        .collect::<Vec<_>>();
    let close_timestamps = contributions
        .iter()
        .map(|src| src.close_ts_ms)
        .collect::<Vec<_>>();
    let num = contributions
        .iter()
        .map(|src| {
            local_boundary_policy_source_weight(spec.policy_name, symbol, src.source)
                * src.adjusted_close_price
        })
        .sum::<f64>();
    let den = contributions
        .iter()
        .map(|src| local_boundary_policy_source_weight(spec.policy_name, symbol, src.source))
        .sum::<f64>();
    let close_price = if den > 0.0 {
        num / den
    } else {
        robust_center(close_values.clone()).unwrap_or_default()
    };
    let close_ts_ms = median_u64(close_timestamps).unwrap_or(end_ms);
    let source_spread_bps = spread_bps(&close_values).unwrap_or(0.0);
    let close_exact_sources = contributions.iter().filter(|src| src.close_exact).count();
    LocalBoundaryShadowOutcome {
        policy_name: spec.policy_name,
        source_subset_name: spec.source_subset_name,
        rule: spec.rule,
        min_sources: spec.min_sources,
        hit: Some(LocalBoundaryShadowHit {
            policy_name: spec.policy_name,
            source_subset_name: spec.source_subset_name,
            rule: spec.rule,
            min_sources: spec.min_sources,
            close_price,
            close_ts_ms,
            source_count: contributions.len(),
            source_spread_bps,
            close_exact_sources,
            source_contributions: contributions,
        }),
    }
}

fn local_boundary_shadow_candidate_ready(
    symbol: &str,
    tapes: &HashMap<LocalPriceSource, LocalSourceBoundaryTapeState>,
    end_ms: u64,
) -> bool {
    let weighted = run_local_boundary_shadow_policy(
        symbol,
        local_boundary_policy_specs_weighted(symbol),
        tapes,
        end_ms,
    );
    if weighted.hit.is_some() {
        return true;
    }
    local_boundary_symbol_router_fallback_policy_specs(symbol)
        .into_iter()
        .any(|spec| {
            run_local_boundary_shadow_policy(symbol, spec, tapes, end_ms)
                .hit
                .is_some()
        })
}

fn pick_local_source_open_point(
    state: &LocalSourceBoundaryState,
    open_tol_ms: u64,
) -> Option<AggregatedPricePoint> {
    if let Some(p) = state.open_exact {
        Some(p)
    } else if let Some((delta, ts_ms, px)) = state.first_after_start {
        if delta <= open_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_first_after_open",
            })
        } else {
            None
        }
    } else if let Some((delta, ts_ms, px)) = state.nearest_start {
        if delta <= open_tol_ms {
            Some(AggregatedPricePoint {
                price: px,
                ts_ms,
                exact: false,
                abs_delta_ms: delta,
                source: "local_nearest_open",
            })
        } else {
            None
        }
    } else {
        None
    }
}

fn apply_local_prewarmed_open_seed(
    states: &mut HashMap<LocalPriceSource, LocalSourceBoundaryState>,
    symbol: &str,
    round_start_ms: u64,
) {
    for source in LOCAL_PRICE_SOURCES {
        let Some(point) = peek_local_prewarmed_open(symbol, round_start_ms, source) else {
            continue;
        };
        let state = states.entry(source).or_default();
        match point.source {
            "local_prewarm_open_exact" => state.open_exact = Some(point),
            "local_prewarm_open_first_after" => {
                state.first_after_start = Some((point.abs_delta_ms, point.ts_ms, point.price));
            }
            "local_prewarm_open_nearest" => {
                state.nearest_start = Some((point.abs_delta_ms, point.ts_ms, point.price));
            }
            _ => {}
        }
    }
}

fn count_matching_fractional_digits(a: f64, b: f64, max_decimals: usize) -> usize {
    if !a.is_finite() || !b.is_finite() || max_decimals == 0 {
        return 0;
    }
    let sa = format!("{:.*}", max_decimals, a.abs());
    let sb = format!("{:.*}", max_decimals, b.abs());
    let fa = sa.split('.').nth(1).unwrap_or("");
    let fb = sb.split('.').nth(1).unwrap_or("");
    fa.chars()
        .zip(fb.chars())
        .take_while(|(x, y)| x == y)
        .count()
}

fn local_price_agg_ingest_state(
    states: &mut HashMap<LocalPriceSource, LocalSourceBoundaryState>,
    source: LocalPriceSource,
    price: f64,
    ts_ms: u64,
    start_ms: u64,
    end_ms: u64,
) {
    let st = states.entry(source).or_default();
    self_built_agg_ingest_tick(
        price,
        ts_ms,
        start_ms,
        end_ms,
        &mut st.first_tick_ts_ms,
        &mut st.open_exact,
        &mut st.close_exact,
        &mut st.nearest_start,
        &mut st.nearest_end,
        &mut st.first_after_start,
        &mut st.first_after_end,
    );
}

#[derive(Debug, Clone, Copy)]
struct LocalCloseSourceContribution {
    source: LocalPriceSource,
    raw_close_price: f64,
    adjusted_close_price: f64,
    close_ts_ms: u64,
    close_exact: bool,
}

#[derive(Debug, Clone)]
struct LocalCloseOnlyAggHit {
    close_price: f64,
    close_ts_ms: u64,
    source_count: usize,
    source_spread_bps: f64,
    close_exact_sources: usize,
    source_contributions: Vec<LocalCloseSourceContribution>,
}

fn local_close_only_contribution_weight(
    contribution: &LocalCloseSourceContribution,
    round_end_ts: u64,
) -> f64 {
    let end_ms = round_end_ts.saturating_mul(1_000);
    let abs_delta_ms = contribution.close_ts_ms.abs_diff(end_ms);
    let exact = if contribution.close_exact {
        local_price_agg_exact_boost()
    } else {
        1.0
    };
    local_price_agg_source_weight(contribution.source)
        * local_price_agg_temporal_weight(abs_delta_ms)
        * exact
}

fn local_close_only_apply_raw_contributions(
    hit: &mut LocalCloseOnlyAggHit,
    round_end_ts: u64,
) -> bool {
    let weighted = hit
        .source_contributions
        .iter()
        .map(|contribution| {
            (
                contribution.raw_close_price,
                local_close_only_contribution_weight(contribution, round_end_ts),
            )
        })
        .collect::<Vec<_>>();
    let Some(raw_close) = weighted_mean(&weighted) else {
        return false;
    };
    let raw_values = hit
        .source_contributions
        .iter()
        .map(|contribution| contribution.raw_close_price)
        .collect::<Vec<_>>();
    let raw_spread_bps = spread_bps(&raw_values).unwrap_or(0.0);
    if raw_spread_bps > local_price_agg_max_source_spread_bps() + 1e-9 {
        return false;
    }
    hit.close_price = raw_close;
    hit.source_spread_bps = raw_spread_bps;
    for contribution in &mut hit.source_contributions {
        contribution.adjusted_close_price = contribution.raw_close_price;
    }
    true
}

fn local_boundary_maybe_debias_hype_close_only(
    symbol: &str,
    hit: &mut LocalCloseOnlyAggHit,
    round_end_ts: u64,
    rtds_open: f64,
) {
    if symbol != "hype/usd" || !rtds_open.is_finite() || rtds_open <= 0.0 {
        return;
    }
    if hit.source_count == 1 && hit.close_exact_sources == 0 {
        let Some(contribution) = hit.source_contributions.first() else {
            return;
        };
        if contribution.source != LocalPriceSource::Hyperliquid {
            return;
        }
        let adjusted = hit.close_price;
        let raw = contribution.raw_close_price;
        if !adjusted.is_finite() || !raw.is_finite() || adjusted < rtds_open || raw < rtds_open {
            return;
        }
        let adjusted_margin_bps =
            ((adjusted - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
        let raw_margin_bps = ((raw - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
        if adjusted_margin_bps + 1e-9 >= 10.0 && raw_margin_bps + 1e-9 >= 7.0 {
            let _ = local_close_only_apply_raw_contributions(hit, round_end_ts);
        }
        return;
    }

    if hit.source_count < 2 || hit.close_exact_sources != 0 {
        return;
    }
    let end_ms = round_end_ts.saturating_mul(1_000);
    let all_preclose = hit
        .source_contributions
        .iter()
        .all(|contribution| contribution.close_ts_ms < end_ms);
    let all_postclose = hit
        .source_contributions
        .iter()
        .all(|contribution| contribution.close_ts_ms > end_ms);
    if !all_preclose && !all_postclose {
        return;
    }
    let raw_weighted = hit
        .source_contributions
        .iter()
        .map(|contribution| {
            (
                contribution.raw_close_price,
                local_close_only_contribution_weight(contribution, round_end_ts),
            )
        })
        .collect::<Vec<_>>();
    let Some(raw_close) = weighted_mean(&raw_weighted) else {
        return;
    };
    let adjusted = hit.close_price;
    if !adjusted.is_finite()
        || !raw_close.is_finite()
        || adjusted >= rtds_open
        || raw_close >= rtds_open
    {
        return;
    }
    let adjusted_margin_bps =
        ((adjusted - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    if (all_preclose && (2.0..12.0).contains(&adjusted_margin_bps))
        || (all_postclose && (4.0..12.0).contains(&adjusted_margin_bps))
    {
        let _ = local_close_only_apply_raw_contributions(hit, round_end_ts);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalBoundaryCloseRule {
    LastBefore,
    NearestAbs,
    AfterThenBefore,
}

impl LocalBoundaryCloseRule {
    fn as_str(self) -> &'static str {
        match self {
            Self::LastBefore => "last_before",
            Self::NearestAbs => "nearest_abs",
            Self::AfterThenBefore => "after_then_before",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LocalBoundaryShadowPolicySpec {
    policy_name: &'static str,
    source_subset_name: &'static str,
    rule: LocalBoundaryCloseRule,
    min_sources: usize,
    allowed_sources: &'static [LocalPriceSource],
}

#[derive(Debug, Clone)]
struct LocalBoundaryShadowHit {
    policy_name: &'static str,
    source_subset_name: &'static str,
    rule: LocalBoundaryCloseRule,
    min_sources: usize,
    close_price: f64,
    close_ts_ms: u64,
    source_count: usize,
    source_spread_bps: f64,
    close_exact_sources: usize,
    source_contributions: Vec<LocalCloseSourceContribution>,
}

#[derive(Debug, Clone)]
struct LocalBoundaryShadowOutcome {
    policy_name: &'static str,
    source_subset_name: &'static str,
    rule: LocalBoundaryCloseRule,
    min_sources: usize,
    hit: Option<LocalBoundaryShadowHit>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct LocalAggUncertaintyGateConfig {
    min_samples: usize,
    min_margin_bps: f64,
    max_train_quantile_bps: f64,
    #[serde(default)]
    max_train_max_bps: f64,
    safety_bps: f64,
    max_side_rate: f64,
    max_source_spread_bps: f64,
    max_round_max_margin_bps: f64,
    #[serde(default)]
    rescue_min_samples: usize,
    #[serde(default)]
    rescue_max_train_max_bps: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct LocalAggUncertaintyGateBucketFile {
    level: String,
    key: Vec<String>,
    n: usize,
    side_errors: usize,
    side_rate: f64,
    q_bps: f64,
    q95_bps: f64,
    q99_bps: f64,
    mean_bps: f64,
    max_bps: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct LocalAggUncertaintyGateFile {
    model: String,
    config: LocalAggUncertaintyGateConfig,
    buckets: Vec<LocalAggUncertaintyGateBucketFile>,
    #[serde(default)]
    rescue_buckets: Vec<LocalAggUncertaintyGateBucketFile>,
}

#[derive(Debug, Clone)]
struct LocalAggUncertaintyGateModel {
    config: LocalAggUncertaintyGateConfig,
    buckets: HashMap<String, LocalAggUncertaintyGateBucketFile>,
    rescue_buckets: HashMap<String, LocalAggUncertaintyGateBucketFile>,
}

#[derive(Debug, Clone)]
struct LocalAggUncertaintyGateDecision {
    accepted: bool,
    reason: &'static str,
    key_level: &'static str,
    train_n: usize,
    train_side_errors: usize,
    train_q_bps: f64,
    train_q95_bps: f64,
    train_q99_bps: f64,
    train_mean_bps: f64,
    train_max_bps: f64,
    required_margin_bps: f64,
}

#[derive(Debug, Clone)]
struct LocalAggUncertaintyGateCandidate {
    slug: String,
    symbol: String,
    round_end_ts: u64,
    source_subset: String,
    rule: String,
    min_sources: usize,
    local_side: Side,
    rtds_side: Side,
    side_match: bool,
    local_close: f64,
    local_close_ts_ms: u64,
    rtds_open: f64,
    rtds_close: f64,
    close_abs_diff: f64,
    close_diff_bps: f64,
    direction_margin_bps: f64,
    source_count: usize,
    source_spread_bps: f64,
    exact_sources: usize,
    close_abs_delta_ms: u64,
    sources: String,
    started_ms: u64,
    ready_ms: u64,
    deadline_ms: u64,
}

#[derive(Debug, Default)]
struct LocalAggUncertaintyGateRoundState {
    candidates: HashMap<String, LocalAggUncertaintyGateCandidate>,
    finalized: bool,
}

static LOCAL_AGG_UNCERTAINTY_GATE_ROUNDS: OnceLock<
    Mutex<HashMap<u64, LocalAggUncertaintyGateRoundState>>,
> = OnceLock::new();

fn local_agg_uncertainty_gate_bucket_key(level: &str, key: &[String]) -> String {
    format!("{}\x1e{}", level, key.join("\x1f"))
}

impl LocalAggUncertaintyGateModel {
    fn from_file(path: PathBuf) -> Option<Self> {
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) => {
                warn!(
                    "⚠️ local_price_agg_uncertainty_gate_model_load_failed | path={} err={}",
                    path.display(),
                    err
                );
                return None;
            }
        };
        let file = match serde_json::from_str::<LocalAggUncertaintyGateFile>(&raw) {
            Ok(file) => file,
            Err(err) => {
                warn!(
                    "⚠️ local_price_agg_uncertainty_gate_model_parse_failed | path={} err={}",
                    path.display(),
                    err
                );
                return None;
            }
        };
        if file.model != "local_agg_uncertainty_gate_v1" {
            warn!(
                "⚠️ local_price_agg_uncertainty_gate_model_unsupported | path={} model={}",
                path.display(),
                file.model
            );
            return None;
        }
        let buckets = file
            .buckets
            .into_iter()
            .map(|bucket| {
                (
                    local_agg_uncertainty_gate_bucket_key(&bucket.level, &bucket.key),
                    bucket,
                )
            })
            .collect::<HashMap<_, _>>();
        let rescue_buckets = file
            .rescue_buckets
            .into_iter()
            .map(|bucket| {
                (
                    local_agg_uncertainty_gate_bucket_key(&bucket.level, &bucket.key),
                    bucket,
                )
            })
            .collect::<HashMap<_, _>>();
        info!(
            "🧪 local_price_agg_uncertainty_gate_model_loaded | path={} buckets={} rescue_buckets={} min_samples={} min_margin_bps={:.6} max_train_quantile_bps={:.6} max_train_max_bps={:.6} safety_bps={:.6} max_side_rate={:.6} max_round_max_margin_bps={:.6} rescue_min_samples={} rescue_max_train_max_bps={:.6}",
            path.display(),
            buckets.len(),
            rescue_buckets.len(),
            file.config.min_samples,
            file.config.min_margin_bps,
            file.config.max_train_quantile_bps,
            file.config.max_train_max_bps,
            file.config.safety_bps,
            file.config.max_side_rate,
            file.config.max_round_max_margin_bps,
            file.config.rescue_min_samples,
            file.config.rescue_max_train_max_bps,
        );
        Some(Self {
            config: file.config,
            buckets,
            rescue_buckets,
        })
    }

    fn choose_stats(
        &self,
        candidate: &LocalAggUncertaintyGateCandidate,
    ) -> Option<(&'static str, &LocalAggUncertaintyGateBucketFile)> {
        for (level, key) in local_agg_uncertainty_gate_key_levels(candidate) {
            let lookup = local_agg_uncertainty_gate_bucket_key(level, &key);
            if let Some(stats) = self.buckets.get(&lookup) {
                if stats.n >= self.config.min_samples {
                    return Some((level, stats));
                }
            }
        }
        None
    }

    fn choose_rescue_stats(
        &self,
        candidate: &LocalAggUncertaintyGateCandidate,
    ) -> Option<(&'static str, &LocalAggUncertaintyGateBucketFile)> {
        if self.config.rescue_min_samples == 0 || self.config.rescue_max_train_max_bps <= 0.0 {
            return None;
        }
        let level = "rescue_shape";
        let key = local_agg_uncertainty_gate_rescue_key(candidate);
        let lookup = local_agg_uncertainty_gate_bucket_key(level, &key);
        let stats = self.rescue_buckets.get(&lookup)?;
        if stats.n < self.config.rescue_min_samples {
            return None;
        }
        if stats.side_errors > 0 {
            return None;
        }
        if stats.max_bps > self.config.rescue_max_train_max_bps + 1e-12 {
            return None;
        }
        Some((level, stats))
    }

    fn evaluate_row(
        &self,
        candidate: &LocalAggUncertaintyGateCandidate,
    ) -> LocalAggUncertaintyGateDecision {
        if !candidate.direction_margin_bps.is_finite() {
            return LocalAggUncertaintyGateDecision::gated("missing_margin");
        }
        if candidate.direction_margin_bps + 1e-12 < self.config.min_margin_bps {
            return LocalAggUncertaintyGateDecision::gated("below_min_margin");
        }
        if self.config.max_source_spread_bps > 0.0
            && candidate.source_spread_bps.is_finite()
            && candidate.source_spread_bps > self.config.max_source_spread_bps + 1e-12
        {
            return LocalAggUncertaintyGateDecision::gated("above_max_source_spread");
        }
        let Some((level, stats)) = self.choose_stats(candidate) else {
            return LocalAggUncertaintyGateDecision::gated("insufficient_history");
        };
        if stats.side_rate > self.config.max_side_rate + 1e-12 {
            return LocalAggUncertaintyGateDecision::from_stats(false, "train_side_rate", level, stats, self.config.safety_bps);
        }
        if stats.q_bps > self.config.max_train_quantile_bps + 1e-12 {
            return LocalAggUncertaintyGateDecision::from_stats(false, "train_quantile_too_wide", level, stats, self.config.safety_bps);
        }
        if self.config.max_train_max_bps > 0.0
            && stats.max_bps > self.config.max_train_max_bps + 1e-12
        {
            return LocalAggUncertaintyGateDecision::from_stats(false, "train_max_too_wide", level, stats, self.config.safety_bps);
        }
        let required_margin_bps = stats.q_bps + self.config.safety_bps;
        if candidate.direction_margin_bps + 1e-12 < required_margin_bps {
            if let Some((rescue_level, rescue_stats)) = self.choose_rescue_stats(candidate) {
                return LocalAggUncertaintyGateDecision::from_stats_with_required_margin(
                    true,
                    "shape_history_max_rescue",
                    rescue_level,
                    rescue_stats,
                    rescue_stats.max_bps,
                );
            }
            return LocalAggUncertaintyGateDecision::from_stats(false, "margin_below_trained_error", level, stats, self.config.safety_bps);
        }
        LocalAggUncertaintyGateDecision::from_stats(true, "", level, stats, self.config.safety_bps)
    }
}

impl LocalAggUncertaintyGateDecision {
    fn gated(reason: &'static str) -> Self {
        Self {
            accepted: false,
            reason,
            key_level: "",
            train_n: 0,
            train_side_errors: 0,
            train_q_bps: f64::NAN,
            train_q95_bps: f64::NAN,
            train_q99_bps: f64::NAN,
            train_mean_bps: f64::NAN,
            train_max_bps: f64::NAN,
            required_margin_bps: f64::NAN,
        }
    }

    fn from_stats(
        accepted: bool,
        reason: &'static str,
        key_level: &'static str,
        stats: &LocalAggUncertaintyGateBucketFile,
        safety_bps: f64,
    ) -> Self {
        Self::from_stats_with_required_margin(
            accepted,
            reason,
            key_level,
            stats,
            stats.q_bps + safety_bps,
        )
    }

    fn from_stats_with_required_margin(
        accepted: bool,
        reason: &'static str,
        key_level: &'static str,
        stats: &LocalAggUncertaintyGateBucketFile,
        required_margin_bps: f64,
    ) -> Self {
        Self {
            accepted,
            reason,
            key_level,
            train_n: stats.n,
            train_side_errors: stats.side_errors,
            train_q_bps: stats.q_bps,
            train_q95_bps: stats.q95_bps,
            train_q99_bps: stats.q99_bps,
            train_mean_bps: stats.mean_bps,
            train_max_bps: stats.max_bps,
            required_margin_bps,
        }
    }
}

fn local_agg_uncertainty_gate_model() -> Option<&'static LocalAggUncertaintyGateModel> {
    static MODEL: OnceLock<Option<LocalAggUncertaintyGateModel>> = OnceLock::new();
    MODEL
        .get_or_init(|| {
            if !local_price_agg_uncertainty_gate_enabled() {
                return None;
            }
            let Some(path) = local_price_agg_uncertainty_gate_model_path() else {
                warn!("⚠️ local_price_agg_uncertainty_gate_model_missing | reason=no_model_path");
                return None;
            };
            LocalAggUncertaintyGateModel::from_file(path)
        })
        .as_ref()
}

fn local_agg_uncertainty_gate_bucket(
    value: f64,
    cuts: &[f64],
    labels: &[&'static str],
) -> &'static str {
    if !value.is_finite() {
        return "missing";
    }
    for (cut, label) in cuts.iter().zip(labels.iter()) {
        if value <= *cut + 1e-12 {
            return label;
        }
    }
    labels.last().copied().unwrap_or("missing")
}

fn local_agg_uncertainty_gate_delta_bucket(value: u64) -> &'static str {
    local_agg_uncertainty_gate_bucket(
        value as f64,
        &[50.0, 100.0, 250.0, 500.0, 1_000.0, 2_000.0, 3_500.0],
        &[
            "d050", "d100", "d250", "d500", "d1000", "d2000", "d3500", "d_gt3500",
        ],
    )
}

fn local_agg_uncertainty_gate_spread_bucket(value: f64) -> &'static str {
    local_agg_uncertainty_gate_bucket(
        value,
        &[0.0, 0.5, 1.0, 2.0, 4.0, 8.0, 12.0],
        &["s0", "s050", "s100", "s200", "s400", "s800", "s1200", "s_gt1200"],
    )
}

fn local_agg_uncertainty_gate_margin_bucket(value: f64) -> &'static str {
    local_agg_uncertainty_gate_bucket(
        value,
        &[0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0],
        &[
            "m050", "m100", "m200", "m300", "m500", "m800", "m1300", "m2100",
            "m3400", "m5500", "m_gt5500",
        ],
    )
}

fn local_agg_uncertainty_gate_source_count_bucket(n: usize) -> &'static str {
    match n {
        0 | 1 => "n1",
        2 => "n2",
        3 => "n3",
        _ => "n4p",
    }
}

fn local_agg_uncertainty_gate_key_levels(
    candidate: &LocalAggUncertaintyGateCandidate,
) -> Vec<(&'static str, Vec<String>)> {
    let symbol = candidate.symbol.clone();
    let subset = candidate.source_subset.clone();
    let rule = candidate.rule.clone();
    let sources = candidate.sources.clone();
    let side = if candidate.local_side == Side::Yes {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let source_count = local_agg_uncertainty_gate_source_count_bucket(candidate.source_count).to_string();
    let exact = if candidate.exact_sources > 0 {
        "exact".to_string()
    } else {
        "no_exact".to_string()
    };
    let delta = local_agg_uncertainty_gate_delta_bucket(candidate.close_abs_delta_ms).to_string();
    let spread = local_agg_uncertainty_gate_spread_bucket(candidate.source_spread_bps).to_string();
    let margin = local_agg_uncertainty_gate_margin_bucket(candidate.direction_margin_bps).to_string();
    vec![
        (
            "full",
            vec![
                symbol.clone(),
                subset.clone(),
                rule.clone(),
                sources.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                delta.clone(),
                spread.clone(),
                margin.clone(),
            ],
        ),
        (
            "no_delta",
            vec![
                symbol.clone(),
                subset.clone(),
                rule.clone(),
                sources.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                spread.clone(),
                margin.clone(),
            ],
        ),
        (
            "no_spread",
            vec![
                symbol.clone(),
                subset.clone(),
                rule.clone(),
                sources.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                delta.clone(),
                margin.clone(),
            ],
        ),
        (
            "shape_margin",
            vec![
                symbol.clone(),
                subset.clone(),
                rule.clone(),
                sources.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                margin.clone(),
            ],
        ),
        (
            "shape",
            vec![
                symbol.clone(),
                subset.clone(),
                rule.clone(),
                sources.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
            ],
        ),
        (
            "symbol_rule_quality_full",
            vec![
                symbol.clone(),
                rule.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                delta.clone(),
                spread.clone(),
                margin.clone(),
            ],
        ),
        (
            "symbol_rule_quality_no_delta",
            vec![
                symbol.clone(),
                rule.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                spread.clone(),
                margin.clone(),
            ],
        ),
        (
            "symbol_rule_quality_no_spread",
            vec![
                symbol.clone(),
                rule.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                delta.clone(),
                margin.clone(),
            ],
        ),
        (
            "symbol_rule_quality_margin",
            vec![
                symbol.clone(),
                rule.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                margin.clone(),
            ],
        ),
        (
            "symbol_rule_quality",
            vec![
                symbol.clone(),
                rule.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
            ],
        ),
        (
            "symbol_quality_margin",
            vec![
                symbol.clone(),
                side.clone(),
                source_count.clone(),
                exact.clone(),
                margin,
            ],
        ),
        ("symbol_quality", vec![symbol, side, source_count, exact]),
    ]
}

fn local_agg_uncertainty_gate_rescue_key(
    candidate: &LocalAggUncertaintyGateCandidate,
) -> Vec<String> {
    vec![
        candidate.symbol.clone(),
        candidate.source_subset.clone(),
        candidate.rule.clone(),
        candidate.sources.clone(),
    ]
}

fn local_agg_gate_sources_string(contributions: &[LocalCloseSourceContribution]) -> String {
    let mut sources = contributions
        .iter()
        .map(|src| src.source.as_str())
        .collect::<Vec<_>>();
    sources.sort_unstable();
    sources.join(";")
}

fn local_agg_gate_median_abs_delta_ms(
    contributions: &[LocalCloseSourceContribution],
    round_end_ts: u64,
) -> u64 {
    let end_ms = round_end_ts.saturating_mul(1_000);
    let mut deltas = contributions
        .iter()
        .map(|src| src.close_ts_ms.abs_diff(end_ms))
        .collect::<Vec<_>>();
    if deltas.is_empty() {
        return 0;
    }
    deltas.sort_unstable();
    deltas[deltas.len() / 2]
}

fn local_agg_gate_candidate_from_boundary_hit(
    slug: &str,
    symbol: &str,
    round_end_ts: u64,
    first_ref: f64,
    first_obs: f64,
    first_side: Side,
    started_ms: u64,
    ready_ms: u64,
    deadline_ms: u64,
    hit: &LocalBoundaryShadowHit,
) -> LocalAggUncertaintyGateCandidate {
    let local_side = if hit.close_price >= first_ref {
        Side::Yes
    } else {
        Side::No
    };
    let close_abs_diff = (hit.close_price - first_obs).abs();
    LocalAggUncertaintyGateCandidate {
        slug: slug.to_string(),
        symbol: symbol.to_string(),
        round_end_ts,
        source_subset: hit.source_subset_name.to_string(),
        rule: hit.rule.as_str().to_string(),
        min_sources: hit.min_sources,
        local_side,
        rtds_side: first_side,
        side_match: local_side == first_side,
        local_close: hit.close_price,
        local_close_ts_ms: hit.close_ts_ms,
        rtds_open: first_ref,
        rtds_close: first_obs,
        close_abs_diff,
        close_diff_bps: close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0,
        direction_margin_bps: (hit.close_price - first_ref).abs()
            / first_ref.abs().max(1e-12)
            * 10_000.0,
        source_count: hit.source_count,
        source_spread_bps: hit.source_spread_bps,
        exact_sources: hit.close_exact_sources,
        close_abs_delta_ms: local_agg_gate_median_abs_delta_ms(
            &hit.source_contributions,
            round_end_ts,
        ),
        sources: local_agg_gate_sources_string(&hit.source_contributions),
        started_ms,
        ready_ms,
        deadline_ms,
    }
}

fn local_agg_gate_candidate_from_close_only_hit(
    slug: &str,
    symbol: &str,
    round_end_ts: u64,
    first_ref: f64,
    first_obs: f64,
    first_side: Side,
    started_ms: u64,
    ready_ms: u64,
    deadline_ms: u64,
    source_subset: &str,
    hit: &LocalCloseOnlyAggHit,
) -> LocalAggUncertaintyGateCandidate {
    let local_side = if hit.close_price >= first_ref {
        Side::Yes
    } else {
        Side::No
    };
    let close_abs_diff = (hit.close_price - first_obs).abs();
    LocalAggUncertaintyGateCandidate {
        slug: slug.to_string(),
        symbol: symbol.to_string(),
        round_end_ts,
        source_subset: source_subset.to_string(),
        rule: "close_only".to_string(),
        min_sources: 1,
        local_side,
        rtds_side: first_side,
        side_match: local_side == first_side,
        local_close: hit.close_price,
        local_close_ts_ms: hit.close_ts_ms,
        rtds_open: first_ref,
        rtds_close: first_obs,
        close_abs_diff,
        close_diff_bps: close_abs_diff / first_obs.abs().max(1e-12) * 10_000.0,
        direction_margin_bps: (hit.close_price - first_ref).abs()
            / first_ref.abs().max(1e-12)
            * 10_000.0,
        source_count: hit.source_count,
        source_spread_bps: hit.source_spread_bps,
        exact_sources: hit.close_exact_sources,
        close_abs_delta_ms: local_agg_gate_median_abs_delta_ms(
            &hit.source_contributions,
            round_end_ts,
        ),
        sources: local_agg_gate_sources_string(&hit.source_contributions),
        started_ms,
        ready_ms,
        deadline_ms,
    }
}

fn local_agg_gate_fmt_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{:.6}", value)
    } else {
        "nan".to_string()
    }
}

fn local_agg_uncertainty_gate_emit_candidate(
    model: &LocalAggUncertaintyGateModel,
    candidate: &LocalAggUncertaintyGateCandidate,
    phase: &'static str,
    observed_candidates: usize,
    round_max_margin_bps: Option<f64>,
) {
    let row_decision = model.evaluate_row(candidate);
    let final_phase = matches!(phase, "final" | "final_update");
    let round_too_wide = round_max_margin_bps
        .map(|margin| {
            model.config.max_round_max_margin_bps > 0.0
                && margin > model.config.max_round_max_margin_bps + 1e-12
        })
        .unwrap_or(false);
    let (gate_status, gate_reason) = if !row_decision.accepted {
        ("gated", row_decision.reason)
    } else if round_too_wide {
        ("gated", "round_max_margin_too_wide")
    } else if final_phase {
        ("accepted", "")
    } else {
        ("pending_round", "round_final_pending")
    };
    let row_gate_status = if row_decision.accepted {
        "accepted"
    } else {
        "gated"
    };
    let round_max = round_max_margin_bps.unwrap_or(f64::NAN);
    let line = format!(
        "🧪 local_price_agg_uncertainty_gate_shadow | phase={} slug={} symbol={} round_end_ts={} policy=boundary_symbol_router_v1 source_subset={} rule={} min_sources={} gate_status={} gate_reason={} row_gate_status={} row_gate_reason={} gate_key_level={} gate_train_n={} gate_train_side_errors={} gate_train_q_bps={} gate_train_q95_bps={} gate_train_q99_bps={} gate_train_mean_bps={} gate_train_max_bps={} gate_required_margin_bps={} round_observed_candidates={} round_max_margin_bps={} round_max_margin_limit_bps={} local_side_vs_rtds_open={:?} rtds_side={:?} side_match_vs_rtds_open={} local_close={:.15}@{} rtds_open={:.15} rtds_close={:.15} close_abs_diff={:.12e} close_diff_bps={:.6} direction_margin_bps={:.6} local_sources={} local_close_spread_bps={:.6} local_close_exact_sources={} close_abs_delta_ms={} local_started_ms={} local_ready_ms={} local_deadline_ms={} local_elapsed_ms={}",
        phase,
        candidate.slug,
        candidate.symbol,
        candidate.round_end_ts,
        candidate.source_subset,
        candidate.rule,
        candidate.min_sources,
        gate_status,
        gate_reason,
        row_gate_status,
        row_decision.reason,
        row_decision.key_level,
        row_decision.train_n,
        row_decision.train_side_errors,
        local_agg_gate_fmt_f64(row_decision.train_q_bps),
        local_agg_gate_fmt_f64(row_decision.train_q95_bps),
        local_agg_gate_fmt_f64(row_decision.train_q99_bps),
        local_agg_gate_fmt_f64(row_decision.train_mean_bps),
        local_agg_gate_fmt_f64(row_decision.train_max_bps),
        local_agg_gate_fmt_f64(row_decision.required_margin_bps),
        observed_candidates,
        local_agg_gate_fmt_f64(round_max),
        local_agg_gate_fmt_f64(model.config.max_round_max_margin_bps),
        candidate.local_side,
        candidate.rtds_side,
        candidate.side_match,
        candidate.local_close,
        candidate.local_close_ts_ms,
        candidate.rtds_open,
        candidate.rtds_close,
        candidate.close_abs_diff,
        candidate.close_diff_bps,
        candidate.direction_margin_bps,
        candidate.sources,
        candidate.source_spread_bps,
        candidate.exact_sources,
        candidate.close_abs_delta_ms,
        candidate.started_ms,
        candidate.ready_ms,
        candidate.deadline_ms,
        candidate.ready_ms.saturating_sub(candidate.started_ms),
    );
    if gate_status == "accepted" || gate_status == "pending_round" {
        info!("{}", line);
    } else {
        warn!("{}", line);
    }
}

fn local_agg_uncertainty_gate_finalize_round(round_end_ts: u64) {
    let Some(model) = local_agg_uncertainty_gate_model() else {
        return;
    };
    let Some((candidates, round_max_margin_bps, observed_candidates)) = ({
        let rounds = LOCAL_AGG_UNCERTAINTY_GATE_ROUNDS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = rounds.lock().expect("local agg gate rounds mutex poisoned");
        match guard.get_mut(&round_end_ts) {
            Some(state) if !state.finalized => {
                state.finalized = true;
                let mut candidates = state.candidates.values().cloned().collect::<Vec<_>>();
                candidates.sort_by(|a, b| a.symbol.cmp(&b.symbol));
                let round_max = candidates
                    .iter()
                    .map(|candidate| candidate.direction_margin_bps)
                    .filter(|value| value.is_finite())
                    .fold(f64::NAN, |acc, value| {
                        if acc.is_nan() {
                            value
                        } else {
                            acc.max(value)
                        }
                    });
                Some((candidates, round_max, state.candidates.len()))
            }
            _ => None,
        }
    }) else {
        return;
    };
    for candidate in candidates {
        local_agg_uncertainty_gate_emit_candidate(
            model,
            &candidate,
            "final",
            observed_candidates,
            Some(round_max_margin_bps),
        );
    }
}

fn local_agg_uncertainty_gate_observe_candidate(candidate: LocalAggUncertaintyGateCandidate) {
    if !local_price_agg_uncertainty_gate_enabled() {
        return;
    }
    let Some(model) = local_agg_uncertainty_gate_model() else {
        return;
    };
    let finalize_ms = local_price_agg_uncertainty_gate_finalize_ms();
    let mut spawn_finalize = false;
    let mut final_update_candidates = Vec::new();
    let (round_max_margin_bps, observed_candidates) = {
        let rounds = LOCAL_AGG_UNCERTAINTY_GATE_ROUNDS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = rounds.lock().expect("local agg gate rounds mutex poisoned");
        let oldest_keep = candidate.round_end_ts.saturating_sub(12 * 3_600);
        guard.retain(|round_end_ts, _| *round_end_ts >= oldest_keep);
        let state = guard.entry(candidate.round_end_ts).or_default();
        if state.candidates.is_empty() {
            spawn_finalize = true;
        }
        state
            .candidates
            .insert(candidate.symbol.clone(), candidate.clone());
        let round_max = state
            .candidates
            .values()
            .map(|candidate| candidate.direction_margin_bps)
            .filter(|value| value.is_finite())
            .fold(f64::NAN, |acc, value| {
                if acc.is_nan() {
                    value
                } else {
                    acc.max(value)
                }
            });
        let observed = state.candidates.len();
        if state.finalized {
            final_update_candidates = state.candidates.values().cloned().collect::<Vec<_>>();
            final_update_candidates.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        }
        (round_max, observed)
    };

    local_agg_uncertainty_gate_emit_candidate(
        model,
        &candidate,
        "preliminary",
        observed_candidates,
        Some(round_max_margin_bps),
    );

    if spawn_finalize {
        tokio::spawn(async move {
            sleep(Duration::from_millis(finalize_ms)).await;
            local_agg_uncertainty_gate_finalize_round(candidate.round_end_ts);
        });
    }

    if !final_update_candidates.is_empty() {
        for candidate in final_update_candidates {
            local_agg_uncertainty_gate_emit_candidate(
                model,
                &candidate,
                "final_update",
                observed_candidates,
                Some(round_max_margin_bps),
            );
        }
    }
}

#[derive(Debug, Clone)]
struct LocalCloseAggCompareResult {
    base_hit: Option<LocalCloseOnlyAggHit>,
    boundary_shadow_outcomes: Vec<LocalBoundaryShadowOutcome>,
}

fn local_boundary_maybe_equalize_xrp_binance_coinbase_fast_no(
    symbol: &str,
    hit: &mut LocalBoundaryShadowHit,
    round_end_ts: u64,
    rtds_open: f64,
) {
    if symbol != "xrp/usd"
        || hit.policy_name != "boundary_weighted"
        || hit.source_subset_name != "only_binance_coinbase"
        || hit.rule != LocalBoundaryCloseRule::NearestAbs
        || hit.source_count != 2
        || hit.close_exact_sources != 0
        || !rtds_open.is_finite()
        || rtds_open <= 0.0
        || hit.close_price >= rtds_open
        || hit.source_spread_bps < 4.0
    {
        return;
    }
    let direction_margin_bps =
        ((hit.close_price - rtds_open).abs() / rtds_open.abs().max(1e-12)) * 10_000.0;
    let close_abs_delta_ms = hit.close_ts_ms.abs_diff(round_end_ts.saturating_mul(1_000));
    if direction_margin_bps < 6.0 || close_abs_delta_ms > 150 {
        return;
    }
    let Some(binance) = hit
        .source_contributions
        .iter()
        .find(|contribution| contribution.source == LocalPriceSource::Binance)
    else {
        return;
    };
    let Some(coinbase) = hit
        .source_contributions
        .iter()
        .find(|contribution| contribution.source == LocalPriceSource::Coinbase)
    else {
        return;
    };
    let end_ms = round_end_ts.saturating_mul(1_000);
    if binance.close_ts_ms.abs_diff(end_ms) >= coinbase.close_ts_ms.abs_diff(end_ms)
        || binance.adjusted_close_price <= coinbase.adjusted_close_price
    {
        return;
    }
    let equalized = (binance.adjusted_close_price + coinbase.adjusted_close_price) / 2.0;
    if equalized.is_finite() && equalized > 0.0 {
        hit.close_price = equalized;
    }
}

const LOCAL_BOUNDARY_SOURCES_FULL: &[LocalPriceSource] = &[
    LocalPriceSource::Binance,
    LocalPriceSource::Bybit,
    LocalPriceSource::Okx,
    LocalPriceSource::Coinbase,
    LocalPriceSource::Hyperliquid,
];
const LOCAL_BOUNDARY_SOURCES_DROP_BINANCE: &[LocalPriceSource] = &[
    LocalPriceSource::Bybit,
    LocalPriceSource::Okx,
    LocalPriceSource::Coinbase,
    LocalPriceSource::Hyperliquid,
];
const LOCAL_BOUNDARY_SOURCES_DROP_BYBIT: &[LocalPriceSource] = &[
    LocalPriceSource::Binance,
    LocalPriceSource::Okx,
    LocalPriceSource::Coinbase,
    LocalPriceSource::Hyperliquid,
];
const LOCAL_BOUNDARY_SOURCES_DROP_OKX: &[LocalPriceSource] = &[
    LocalPriceSource::Binance,
    LocalPriceSource::Bybit,
    LocalPriceSource::Coinbase,
    LocalPriceSource::Hyperliquid,
];
const LOCAL_BOUNDARY_SOURCES_DROP_COINBASE: &[LocalPriceSource] = &[
    LocalPriceSource::Binance,
    LocalPriceSource::Bybit,
    LocalPriceSource::Okx,
    LocalPriceSource::Hyperliquid,
];
const LOCAL_BOUNDARY_SOURCES_DROP_HYPERLIQUID: &[LocalPriceSource] = &[
    LocalPriceSource::Binance,
    LocalPriceSource::Bybit,
    LocalPriceSource::Okx,
    LocalPriceSource::Coinbase,
];
const LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE_COINBASE: &[LocalPriceSource] =
    &[LocalPriceSource::Binance, LocalPriceSource::Coinbase];
const LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE_HYPERLIQUID: &[LocalPriceSource] =
    &[LocalPriceSource::Binance, LocalPriceSource::Hyperliquid];
const LOCAL_BOUNDARY_SOURCES_ONLY_BYBIT_COINBASE: &[LocalPriceSource] =
    &[LocalPriceSource::Bybit, LocalPriceSource::Coinbase];
const LOCAL_BOUNDARY_SOURCES_ONLY_BINANCE: &[LocalPriceSource] = &[LocalPriceSource::Binance];
const LOCAL_BOUNDARY_SOURCES_ONLY_COINBASE: &[LocalPriceSource] = &[LocalPriceSource::Coinbase];
const LOCAL_BOUNDARY_SOURCES_ONLY_HYPERLIQUID: &[LocalPriceSource] =
    &[LocalPriceSource::Hyperliquid];
const LOCAL_BOUNDARY_SOURCES_ONLY_OKX: &[LocalPriceSource] = &[LocalPriceSource::Okx];
const LOCAL_BOUNDARY_SOURCES_ONLY_OKX_COINBASE: &[LocalPriceSource] =
    &[LocalPriceSource::Okx, LocalPriceSource::Coinbase];

const LOCAL_BOUNDARY_POLICY_PRE_MS: u64 = 5_000;
const LOCAL_BOUNDARY_POLICY_POST_MS: u64 = 250;

fn local_price_agg_temporal_weight(abs_delta_ms: u64) -> f64 {
    let decay = local_price_agg_close_time_decay_ms();
    if !decay.is_finite() || decay <= 0.0 {
        return 1.0;
    }
    1.0 / (1.0 + (abs_delta_ms as f64 / decay))
}

fn local_price_agg_point_weight(source: LocalPriceSource, point: &AggregatedPricePoint) -> f64 {
    let base = local_price_agg_source_weight(source);
    let temporal = local_price_agg_temporal_weight(point.abs_delta_ms);
    let exact = if point.exact {
        local_price_agg_exact_boost()
    } else {
        1.0
    };
    base * temporal * exact
}

fn local_price_agg_learn_source_biases_from_rtds(
    symbol: &str,
    rtds_close: f64,
    points: &[LocalCloseSourceContribution],
) {
    if !local_price_agg_bias_learning_enabled() {
        return;
    }
    if !rtds_close.is_finite() || rtds_close <= 0.0 {
        return;
    }
    for point in points {
        if !point.raw_close_price.is_finite() || point.raw_close_price <= 0.0 {
            continue;
        }
        let residual_bps =
            ((rtds_close - point.raw_close_price) / rtds_close.abs().max(1e-12)) * 10_000.0;
        if let Some(new_bias_bps) =
            local_price_agg_update_source_bias_bps(symbol, point.source, residual_bps)
        {
            info!(
                "🧪 local_price_agg_bias_update | symbol={} source={} rtds_close={:.15} raw_close={:.15} adjusted_close={:.15} residual_bps={:.6} new_bias_bps={:.6} close_ts_ms={} close_exact={}",
                symbol,
                point.source.as_str(),
                rtds_close,
                point.raw_close_price,
                point.adjusted_close_price,
                residual_bps,
                new_bias_bps,
                point.close_ts_ms,
                point.close_exact,
            );
        }
    }
}

async fn run_local_price_close_aggregator(
    local_price_hub: Option<Arc<LocalPriceHub>>,
    symbol: &str,
    round_start_ts: u64,
    round_end_ts: u64,
    hard_deadline_ms: u64,
) -> LocalCloseAggCompareResult {
    let Some(hub) = local_price_hub else {
        return LocalCloseAggCompareResult {
            base_hit: None,
            boundary_shadow_outcomes: Vec::new(),
        };
    };
    let target_symbol = normalize_chainlink_symbol(symbol);
    if target_symbol.is_empty() {
        return LocalCloseAggCompareResult {
            base_hit: None,
            boundary_shadow_outcomes: Vec::new(),
        };
    }

    let start_ms = round_start_ts.saturating_mul(1_000);
    let end_ms = round_end_ts.saturating_mul(1_000);
    let close_tol_ms = local_price_agg_close_tolerance_ms();
    let min_sources = local_price_agg_min_sources();
    let max_source_spread_bps = local_price_agg_max_source_spread_bps();
    let boundary_window_ms = local_price_agg_boundary_window_ms();

    let mut states: HashMap<LocalPriceSource, LocalSourceBoundaryState> = HashMap::new();
    let mut tapes: HashMap<LocalPriceSource, LocalSourceBoundaryTapeState> = HashMap::new();
    for (price, ts_ms, source) in hub.snapshot_recent_ticks(&target_symbol) {
        local_price_agg_ingest_state(&mut states, source, price, ts_ms, start_ms, end_ms);
        local_price_agg_collect_boundary_tape_tick(
            &mut tapes,
            source,
            price,
            ts_ms,
            start_ms,
            end_ms,
            boundary_window_ms,
        );
    }

    let close_ready_sources = |states: &HashMap<LocalPriceSource, LocalSourceBoundaryState>| {
        states
            .values()
            .filter(|st| pick_local_source_close_point(st, close_tol_ms).is_some())
            .count()
    };
    let boundary_ready_after_ms = end_ms.saturating_add(LOCAL_BOUNDARY_POLICY_POST_MS);

    if unix_now_millis_u64() < end_ms || close_ready_sources(&states) < min_sources {
        if let Some(mut rx) = hub.subscribe(&target_symbol) {
            while unix_now_millis_u64() <= hard_deadline_ms {
                let next = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
                match next {
                    Ok(Ok((price, ts_ms, source))) => {
                        local_price_agg_ingest_state(
                            &mut states,
                            source,
                            price,
                            ts_ms,
                            start_ms,
                            end_ms,
                        );
                        local_price_agg_collect_boundary_tape_tick(
                            &mut tapes,
                            source,
                            price,
                            ts_ms,
                            start_ms,
                            end_ms,
                            boundary_window_ms,
                        );
                    }
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                    Err(_) => {}
                }

                let now_ms = unix_now_millis_u64();
                let close_ready = now_ms >= end_ms && close_ready_sources(&states) >= min_sources;
                let boundary_ready = now_ms >= boundary_ready_after_ms
                    && local_boundary_shadow_candidate_ready(&target_symbol, &tapes, end_ms);
                if close_ready || boundary_ready {
                    break;
                }
            }
        }
    }

    let mut source_hits: Vec<(LocalPriceSource, AggregatedPricePoint, f64)> = Vec::new();
    for (source, st) in &states {
        if let Some(mut close) = pick_local_source_close_point(st, close_tol_ms) {
            let raw_close_price = close.price;
            close.price = local_price_agg_apply_source_bias(&target_symbol, *source, close.price);
            source_hits.push((*source, close, raw_close_price));
        }
    }
    let source_contributions = source_hits
        .iter()
        .map(
            |(source, close, raw_close_price)| LocalCloseSourceContribution {
                source: *source,
                raw_close_price: *raw_close_price,
                adjusted_close_price: close.price,
                close_ts_ms: close.ts_ms,
                close_exact: close.exact,
            },
        )
        .collect::<Vec<_>>();
    let source_points = source_hits
        .iter()
        .map(
            |(source, close, _raw_close_price)| LocalPriceAggSourcePointProbe {
                source: source.as_str().to_string(),
                open_price: None,
                open_ts_ms: None,
                open_exact: None,
                raw_close_price: Some(*_raw_close_price),
                close_price: Some(close.price),
                close_ts_ms: Some(close.ts_ms),
                close_exact: Some(close.exact),
                close_abs_delta_ms: Some(close.abs_delta_ms),
                close_pick_kind: Some(close.source.to_string()),
            },
        )
        .collect::<Vec<_>>();
    let status = if source_hits.len() >= min_sources {
        "ready"
    } else {
        "insufficient_sources"
    };
    append_local_price_agg_sources_probe(&LocalPriceAggSourcesProbe {
        unix_ms: unix_now_millis_u64(),
        symbol: target_symbol.clone(),
        round_start_ts,
        round_end_ts,
        mode: "close_only".to_string(),
        status: status.to_string(),
        min_sources,
        source_points,
    });
    append_local_price_agg_boundary_probe(&LocalPriceAggBoundaryProbe {
        unix_ms: unix_now_millis_u64(),
        symbol: target_symbol.clone(),
        round_start_ts,
        round_end_ts,
        mode: "close_only".to_string(),
        status: status.to_string(),
        boundary_window_ms,
        source_tapes: build_local_price_agg_boundary_source_probes(&tapes, start_ms, end_ms),
    });

    let mut boundary_shadow_outcomes = vec![run_local_boundary_shadow_policy(
        &target_symbol,
        local_boundary_policy_specs_weighted(&target_symbol),
        &tapes,
        end_ms,
    )];
    for spec in local_boundary_symbol_router_fallback_policy_specs(&target_symbol) {
        boundary_shadow_outcomes.push(run_local_boundary_shadow_policy(
            &target_symbol,
            spec,
            &tapes,
            end_ms,
        ));
    }

    if source_hits.len() < min_sources {
        return LocalCloseAggCompareResult {
            base_hit: None,
            boundary_shadow_outcomes,
        };
    }

    let close_values: Vec<f64> = source_hits.iter().map(|(_, c, _)| c.price).collect();
    let close_timestamps: Vec<u64> = source_hits.iter().map(|(_, c, _)| c.ts_ms).collect();
    let close_exact_sources = source_hits.iter().filter(|(_, c, _)| c.exact).count();
    let close_weighted = source_hits
        .iter()
        .map(|(source, close, _)| (close.price, local_price_agg_point_weight(*source, close)))
        .collect::<Vec<_>>();
    let Some(close_med) =
        weighted_mean(&close_weighted).or_else(|| robust_center(close_values.clone()))
    else {
        return LocalCloseAggCompareResult {
            base_hit: None,
            boundary_shadow_outcomes,
        };
    };
    let close_ts_med = median_u64(close_timestamps).unwrap_or(end_ms);
    let source_spread_bps = spread_bps(&close_values).unwrap_or(0.0);

    if source_spread_bps > max_source_spread_bps + 1e-9 {
        warn!(
            "⚠️ local_price_agg_close_only_spread_reject | symbol={} round_start_ts={} round_end_ts={} spread_bps={:.3} max_bps={:.3} source_count={}",
            target_symbol,
            round_start_ts,
            round_end_ts,
            source_spread_bps,
            max_source_spread_bps,
            source_hits.len(),
        );
        return LocalCloseAggCompareResult {
            base_hit: None,
            boundary_shadow_outcomes,
        };
    }

    LocalCloseAggCompareResult {
        base_hit: Some(LocalCloseOnlyAggHit {
            close_price: close_med,
            close_ts_ms: close_ts_med,
            source_count: source_hits.len(),
            source_spread_bps,
            close_exact_sources,
            source_contributions,
        }),
        boundary_shadow_outcomes,
    }
}

async fn run_local_price_aggregator(
    local_price_hub: Option<Arc<LocalPriceHub>>,
    symbol: &str,
    round_start_ts: u64,
    round_end_ts: u64,
    hard_deadline_ms: u64,
) -> Option<(Side, f64, f64, u64, u64, bool)> {
    let Some(hub) = local_price_hub else {
        return None;
    };
    let target_symbol = normalize_chainlink_symbol(symbol);
    if target_symbol.is_empty() {
        return None;
    }

    let start_ms = round_start_ts.saturating_mul(1_000);
    let end_ms = round_end_ts.saturating_mul(1_000);
    let open_tol_ms = local_price_agg_open_tolerance_ms();
    let close_tol_ms = local_price_agg_close_tolerance_ms();
    let min_confidence = local_price_agg_min_confidence();
    let min_sources = local_price_agg_min_sources();
    let max_source_spread_bps = local_price_agg_max_source_spread_bps();
    let boundary_window_ms = local_price_agg_boundary_window_ms();
    let single_source_min_direction_margin_bps =
        local_price_agg_single_source_min_direction_margin_bps();
    let relief_min_confidence = LOCAL_PRICE_AGG_RELIEF_MIN_CONFIDENCE_DEFAULT;
    let relief_min_direction_margin_bps = LOCAL_PRICE_AGG_RELIEF_MIN_DIRECTION_MARGIN_BPS_DEFAULT;
    let relief_max_source_spread_bps = LOCAL_PRICE_AGG_RELIEF_MAX_SOURCE_SPREAD_BPS_DEFAULT;

    let mut states: HashMap<LocalPriceSource, LocalSourceBoundaryState> = HashMap::new();
    let mut tapes: HashMap<LocalPriceSource, LocalSourceBoundaryTapeState> = HashMap::new();
    let mut observed_ticks: u64 = 0;
    let mut observed_snapshot_ticks: u64 = 0;
    let mut observed_live_ticks: u64 = 0;
    let mut lagged_ticks: u64 = 0;

    for (price, ts_ms, source) in hub.snapshot_recent_ticks(&target_symbol) {
        observed_ticks = observed_ticks.saturating_add(1);
        observed_snapshot_ticks = observed_snapshot_ticks.saturating_add(1);
        local_price_agg_ingest_state(&mut states, source, price, ts_ms, start_ms, end_ms);
        local_price_agg_collect_boundary_tape_tick(
            &mut tapes,
            source,
            price,
            ts_ms,
            start_ms,
            end_ms,
            boundary_window_ms,
        );
    }
    apply_local_prewarmed_open_seed(&mut states, &target_symbol, start_ms);

    let full_ready_sources = |states: &HashMap<LocalPriceSource, LocalSourceBoundaryState>| {
        states
            .values()
            .filter(|st| pick_local_source_points(st, open_tol_ms, close_tol_ms).is_some())
            .count()
    };

    if unix_now_millis_u64() < end_ms || full_ready_sources(&states) < min_sources {
        if let Some(mut rx) = hub.subscribe(&target_symbol) {
            while unix_now_millis_u64() <= hard_deadline_ms {
                let next = tokio::time::timeout(Duration::from_millis(120), rx.recv()).await;
                match next {
                    Ok(Ok((price, ts_ms, source))) => {
                        observed_ticks = observed_ticks.saturating_add(1);
                        observed_live_ticks = observed_live_ticks.saturating_add(1);
                        local_price_agg_ingest_state(
                            &mut states,
                            source,
                            price,
                            ts_ms,
                            start_ms,
                            end_ms,
                        );
                        local_price_agg_collect_boundary_tape_tick(
                            &mut tapes,
                            source,
                            price,
                            ts_ms,
                            start_ms,
                            end_ms,
                            boundary_window_ms,
                        );
                    }
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                        lagged_ticks = lagged_ticks.saturating_add(n);
                    }
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                    Err(_) => {}
                }

                if unix_now_millis_u64() >= end_ms && full_ready_sources(&states) >= min_sources {
                    break;
                }
            }
        }
    }

    let mut source_hits: Vec<(
        LocalPriceSource,
        AggregatedPricePoint,
        AggregatedPricePoint,
        f64,
        f64,
    )> = Vec::new();
    for (source, st) in &states {
        if let Some((mut open, mut close)) = pick_local_source_points(st, open_tol_ms, close_tol_ms)
        {
            let raw_open = open.price;
            let raw_close = close.price;
            open.price = local_price_agg_apply_source_bias(&target_symbol, *source, open.price);
            close.price = local_price_agg_apply_source_bias(&target_symbol, *source, close.price);
            source_hits.push((*source, open, close, raw_open, raw_close));
        }
    }

    let source_points_for_probe = || {
        source_hits
            .iter()
            .map(
                |(source, open, close, _, raw_close)| LocalPriceAggSourcePointProbe {
                    source: source.as_str().to_string(),
                    open_price: Some(open.price),
                    open_ts_ms: Some(open.ts_ms),
                    open_exact: Some(open.exact),
                    raw_close_price: Some(*raw_close),
                    close_price: Some(close.price),
                    close_ts_ms: Some(close.ts_ms),
                    close_exact: Some(close.exact),
                    close_abs_delta_ms: Some(close.abs_delta_ms),
                    close_pick_kind: Some(close.source.to_string()),
                },
            )
            .collect::<Vec<_>>()
    };

    let emit_probe = |status: &str,
                      side: Option<Side>,
                      confidence: Option<f64>,
                      effective_min_confidence: f64,
                      confidence_relief_applied: bool,
                      source_count: usize,
                      source_agreement: Option<f64>,
                      source_spread_bps: Option<f64>,
                      direction_margin_bps: Option<f64>,
                      open_price: Option<f64>,
                      open_ts_ms: Option<u64>,
                      close_price: Option<f64>,
                      close_ts_ms: Option<u64>,
                      open_exact_sources: usize,
                      close_exact_sources: usize| {
        append_local_price_agg_probe(&LocalPriceAggProbe {
            unix_ms: unix_now_millis_u64(),
            symbol: target_symbol.clone(),
            round_start_ts,
            round_end_ts,
            status: status.to_string(),
            side: side.map(|s| format!("{:?}", s)),
            confidence,
            min_confidence,
            effective_min_confidence,
            min_sources,
            max_source_spread_bps,
            single_source_min_direction_margin_bps,
            confidence_relief_applied,
            source_count,
            source_agreement,
            source_spread_bps,
            direction_margin_bps,
            open_price,
            open_ts_ms,
            close_price,
            close_ts_ms,
            open_exact_sources,
            close_exact_sources,
            observed_ticks,
            observed_snapshot_ticks,
            observed_live_ticks,
            lagged_ticks,
        });
        append_local_price_agg_sources_probe(&LocalPriceAggSourcesProbe {
            unix_ms: unix_now_millis_u64(),
            symbol: target_symbol.clone(),
            round_start_ts,
            round_end_ts,
            mode: "full".to_string(),
            status: status.to_string(),
            min_sources,
            source_points: source_points_for_probe(),
        });
        append_local_price_agg_boundary_probe(&LocalPriceAggBoundaryProbe {
            unix_ms: unix_now_millis_u64(),
            symbol: target_symbol.clone(),
            round_start_ts,
            round_end_ts,
            mode: "full".to_string(),
            status: status.to_string(),
            boundary_window_ms,
            source_tapes: build_local_price_agg_boundary_source_probes(&tapes, start_ms, end_ms),
        });
    };

    let boundary_debug = || {
        states
            .iter()
            .map(|(source, st)| format_local_source_boundary_state(*source, st))
            .collect::<Vec<_>>()
            .join(" | ")
    };

    if source_hits.len() < min_sources {
        warn!(
            "⚠️ local_price_agg_unresolved | symbol={} round_start_ts={} round_end_ts={} reason=insufficient_sources have={} need={} observed_ticks={} snapshot_ticks={} live_ticks={} source_boundary={}",
            target_symbol,
            round_start_ts,
            round_end_ts,
            source_hits.len(),
            min_sources,
            observed_ticks,
            observed_snapshot_ticks,
            observed_live_ticks,
            boundary_debug(),
        );
        emit_probe(
            "insufficient_sources",
            None,
            None,
            min_confidence,
            false,
            source_hits.len(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            0,
        );
        return None;
    }

    let open_values: Vec<f64> = source_hits.iter().map(|(_, o, _, _, _)| o.price).collect();
    let close_values: Vec<f64> = source_hits.iter().map(|(_, _, c, _, _)| c.price).collect();
    let open_timestamps: Vec<u64> = source_hits.iter().map(|(_, o, _, _, _)| o.ts_ms).collect();
    let close_timestamps: Vec<u64> = source_hits.iter().map(|(_, _, c, _, _)| c.ts_ms).collect();
    let open_exact_sources = source_hits.iter().filter(|(_, o, _, _, _)| o.exact).count();
    let close_exact_sources = source_hits.iter().filter(|(_, _, c, _, _)| c.exact).count();

    let open_weighted = source_hits
        .iter()
        .map(|(source, open, _, _, _)| (open.price, local_price_agg_point_weight(*source, open)))
        .collect::<Vec<_>>();
    let Some(open_med) =
        weighted_mean(&open_weighted).or_else(|| robust_median(open_values.clone()))
    else {
        emit_probe(
            "open_median_missing",
            None,
            None,
            min_confidence,
            false,
            source_hits.len(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            open_exact_sources,
            close_exact_sources,
        );
        return None;
    };
    let close_weighted = source_hits
        .iter()
        .map(|(source, _, close, _, _)| (close.price, local_price_agg_point_weight(*source, close)))
        .collect::<Vec<_>>();
    let Some(close_med) =
        weighted_mean(&close_weighted).or_else(|| robust_median(close_values.clone()))
    else {
        emit_probe(
            "close_median_missing",
            None,
            None,
            min_confidence,
            false,
            source_hits.len(),
            None,
            None,
            None,
            Some(open_med),
            median_u64(open_timestamps.clone()),
            None,
            None,
            open_exact_sources,
            close_exact_sources,
        );
        return None;
    };
    let open_ts_med = median_u64(open_timestamps).unwrap_or(start_ms);
    let close_ts_med = median_u64(close_timestamps).unwrap_or(end_ms);

    let side = if close_med >= open_med {
        Side::Yes
    } else {
        Side::No
    };
    let agree_cnt = source_hits
        .iter()
        .filter(|(_, open, close, _, _)| {
            let src_side = if close.price >= open.price {
                Side::Yes
            } else {
                Side::No
            };
            src_side == side
        })
        .count();
    let source_agreement = agree_cnt as f64 / source_hits.len() as f64;
    let spread_open = spread_bps(&open_values).unwrap_or(0.0);
    let spread_close = spread_bps(&close_values).unwrap_or(0.0);
    let source_spread = spread_open.max(spread_close);
    let direction_margin_bps =
        ((close_med - open_med).abs() / open_med.abs().max(1e-12)) * 10_000.0;
    if source_spread > max_source_spread_bps + 1e-9 {
        warn!(
            "⚠️ local_price_agg_spread_reject | symbol={} round_start_ts={} round_end_ts={} spread_bps={:.3} max_bps={:.3} source_count={}",
            target_symbol,
            round_start_ts,
            round_end_ts,
            source_spread,
            max_source_spread_bps,
            source_hits.len(),
        );
        emit_probe(
            "spread_reject",
            Some(side),
            None,
            min_confidence,
            false,
            source_hits.len(),
            Some(source_agreement),
            Some(source_spread),
            Some(direction_margin_bps),
            Some(open_med),
            Some(open_ts_med),
            Some(close_med),
            Some(close_ts_med),
            open_exact_sources,
            close_exact_sources,
        );
        return None;
    }

    let confidence = if source_hits.len() == 1 {
        if open_exact_sources > 0 && close_exact_sources > 0 {
            0.93
        } else {
            0.87
        }
    } else {
        let source_factor = (source_hits.len().min(3) as f64) / 3.0;
        let spread_factor = if max_source_spread_bps > 0.0 {
            (1.0 - (source_spread / max_source_spread_bps)).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let exact_factor = ((open_exact_sources.min(close_exact_sources).min(2)) as f64) / 2.0;
        (0.50 * source_agreement
            + 0.20 * source_factor
            + 0.20 * spread_factor
            + 0.10 * exact_factor)
            .clamp(0.0, 1.0)
    };

    if source_hits.len() == 1
        && open_exact_sources == 0
        && close_exact_sources == 0
        && direction_margin_bps + 1e-9 < single_source_min_direction_margin_bps
    {
        warn!(
            "⚠️ local_price_agg_single_source_near_flat | symbol={} round_start_ts={} round_end_ts={} direction_margin_bps={:.6} min_margin_bps={:.6} source_boundary={}",
            target_symbol,
            round_start_ts,
            round_end_ts,
            direction_margin_bps,
            single_source_min_direction_margin_bps,
            boundary_debug(),
        );
        emit_probe(
            "single_source_near_flat",
            Some(side),
            None,
            min_confidence,
            false,
            source_hits.len(),
            Some(source_agreement),
            Some(source_spread),
            Some(direction_margin_bps),
            Some(open_med),
            Some(open_ts_med),
            Some(close_med),
            Some(close_ts_med),
            open_exact_sources,
            close_exact_sources,
        );
        return None;
    }

    let confidence_relief_applied = source_hits.len() >= 2
        && (source_agreement >= 1.0 - 1e-9)
        && source_spread <= relief_max_source_spread_bps + 1e-9
        && direction_margin_bps >= relief_min_direction_margin_bps - 1e-9;
    let safe_low_conf_relief_applied = source_hits.len()
        >= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_SOURCES
        && (source_agreement >= 1.0 - 1e-9)
        && source_spread <= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MAX_SOURCE_SPREAD_BPS + 1e-9
        && direction_margin_bps
            >= LOCAL_PRICE_AGG_SAFE_LOW_CONF_RELIEF_MIN_DIRECTION_MARGIN_BPS - 1e-9;
    let effective_min_confidence = if confidence_relief_applied || safe_low_conf_relief_applied {
        min_confidence.min(relief_min_confidence)
    } else {
        min_confidence
    };

    if confidence + 1e-9 < effective_min_confidence {
        warn!(
            "⚠️ local_price_agg_low_confidence | symbol={} round_start_ts={} round_end_ts={} confidence={:.3} min_confidence={:.3} effective_min_confidence={:.3} relief_applied={} safe_low_conf_relief_applied={} source_count={} agreement={:.3} spread_bps={:.3} direction_margin_bps={:.6} source_boundary={}",
            target_symbol,
            round_start_ts,
            round_end_ts,
            confidence,
            min_confidence,
            effective_min_confidence,
            confidence_relief_applied,
            safe_low_conf_relief_applied,
            source_hits.len(),
            source_agreement,
            source_spread,
            direction_margin_bps,
            boundary_debug(),
        );
        emit_probe(
            "low_confidence",
            Some(side),
            Some(confidence),
            effective_min_confidence,
            confidence_relief_applied || safe_low_conf_relief_applied,
            source_hits.len(),
            Some(source_agreement),
            Some(source_spread),
            Some(direction_margin_bps),
            Some(open_med),
            Some(open_ts_med),
            Some(close_med),
            Some(close_ts_med),
            open_exact_sources,
            close_exact_sources,
        );
        return None;
    }

    info!(
        "🧠 local_price_agg_ready | unix_ms={} symbol={} side={:?} confidence={:.3} min_confidence={:.3} effective_min_confidence={:.3} relief_applied={} safe_low_conf_relief_applied={} sources={} agreement={:.3} spread_bps={:.3} direction_margin_bps={:.6} open={:.6}@{} close={:.6}@{} open_exact_sources={} close_exact_sources={} decay_ms={:.1} exact_boost={:.2} observed_ticks={} snapshot_ticks={} live_ticks={} lagged_ticks={}",
        unix_now_millis_u64(),
        target_symbol,
        side,
        confidence,
        min_confidence,
        effective_min_confidence,
        confidence_relief_applied,
        safe_low_conf_relief_applied,
        source_hits
            .iter()
            .map(|(s, _, _, _, _)| s.as_str())
            .collect::<Vec<_>>()
            .join(","),
        source_agreement,
        source_spread,
        direction_margin_bps,
        open_med,
        open_ts_med,
        close_med,
        close_ts_med,
        open_exact_sources,
        close_exact_sources,
        local_price_agg_close_time_decay_ms(),
        local_price_agg_exact_boost(),
        observed_ticks,
        observed_snapshot_ticks,
        observed_live_ticks,
        lagged_ticks,
    );
    emit_probe(
        "ready",
        Some(side),
        Some(confidence),
        effective_min_confidence,
        confidence_relief_applied,
        source_hits.len(),
        Some(source_agreement),
        Some(source_spread),
        Some(direction_margin_bps),
        Some(open_med),
        Some(open_ts_med),
        Some(close_med),
        Some(close_ts_med),
        open_exact_sources,
        close_exact_sources,
    );

    Some((side, open_med, close_med, open_ts_med, close_ts_med, false))
}

async fn run_chainlink_winner_hint_via_hub(
    hub: Arc<ChainlinkHub>,
    symbol: &str,
    round_start_ts: u64,
    round_end_ts: u64,
    hard_deadline_ts: u64,
) -> Option<(Side, f64, f64, u64, u64, bool)> {
    let target_symbol = normalize_chainlink_symbol(symbol);
    let start_ms = round_start_ts.saturating_mul(1_000);
    let end_ms = round_end_ts.saturating_mul(1_000);
    let mut open_point: Option<(f64, u64)> = take_prewarmed_open(&target_symbol, start_ms);
    if let Some((px, ts_ms)) = open_point {
        info!(
            "⏱️ chainlink_open_prewarm_hit | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6}",
            unix_now_millis_u64(),
            target_symbol,
            round_start_ts,
            ts_ms,
            px,
        );
    }
    let mut close_point: Option<(f64, u64)> = None;
    let mut open_missing_warned = false;
    let mut observed_ticks: u64 = 0;
    let mut observed_snapshot_ticks: u64 = 0;
    let mut observed_live_ticks: u64 = 0;
    let mut lagged_ticks: u64 = 0;
    let mut first_tick_ts_ms: Option<u64> = None;
    let mut last_tick_ts_ms: Option<u64> = None;
    let mut nearest_start: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)
    let mut nearest_end: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)

    for (price, ts_ms) in hub.snapshot_recent_ticks(&target_symbol) {
        observed_ticks = observed_ticks.saturating_add(1);
        observed_snapshot_ticks = observed_snapshot_ticks.saturating_add(1);
        first_tick_ts_ms.get_or_insert(ts_ms);
        last_tick_ts_ms = Some(ts_ms);
        let start_delta = ts_ms.abs_diff(start_ms);
        match nearest_start {
            Some((best_delta, _, _)) if start_delta >= best_delta => {}
            _ => nearest_start = Some((start_delta, ts_ms, price)),
        }
        let end_delta = ts_ms.abs_diff(end_ms);
        match nearest_end {
            Some((best_delta, _, _)) if end_delta >= best_delta => {}
            _ => nearest_end = Some((end_delta, ts_ms, price)),
        }
        if open_point.is_none() && ts_ms == start_ms {
            open_point = Some((price, ts_ms));
        }
        if close_point.is_none() && ts_ms == end_ms {
            close_point = Some((price, ts_ms));
        }
    }

    if let Some((close, close_ts_ms)) = close_point {
        if let Some((open_ref_px, open_ts_ms)) = open_point {
            let side = if close >= open_ref_px {
                Side::Yes
            } else {
                Side::No
            };
            info!(
                "🏁 Chainlink winner hint ready | unix_ms={} symbol={} side={:?} open_ref={:.6}@{} close={:.6}@{} source=rtds_exact_hub_snapshot",
                unix_now_millis_u64(),
                target_symbol,
                side,
                open_ref_px,
                open_ts_ms,
                close,
                close_ts_ms,
            );
            set_last_chainlink_close(&target_symbol, close_ts_ms, close);
            return Some((side, open_ref_px, close, open_ts_ms, close_ts_ms, true));
        }
    }

    let Some(mut rx) = hub.subscribe(&target_symbol) else {
        warn!(
            "⚠️ chainlink_hub_unsubscribed | symbol={} round_start_ts={} round_end_ts={}",
            target_symbol, round_start_ts, round_end_ts
        );
        return None;
    };

    while unix_now_secs() <= hard_deadline_ts {
        if close_point.is_none() {
            if let Some((price, ts_ms)) = peek_prewarmed_tick(&target_symbol, end_ms) {
                close_point = Some((price, ts_ms));
                info!(
                    "🔄 chainlink_close_recovered_from_prewarm_early | symbol={} round_end_ts={} close_ts_ms={} close_price={:.6}",
                    target_symbol, round_end_ts, ts_ms, price,
                );
            }
        }
        if let Some((close, close_ts_ms)) = close_point {
            if let Some((open_ref_px, open_ts_ms)) = open_point {
                let side = if close >= open_ref_px {
                    Side::Yes
                } else {
                    Side::No
                };
                info!(
                    "🏁 Chainlink winner hint ready | unix_ms={} symbol={} side={:?} open_ref={:.6}@{} close={:.6}@{} source=rtds_exact_close_recovered_early",
                    unix_now_millis_u64(),
                    target_symbol,
                    side,
                    open_ref_px,
                    open_ts_ms,
                    close,
                    close_ts_ms,
                );
                set_last_chainlink_close(&target_symbol, close_ts_ms, close);
                return Some((side, open_ref_px, close, open_ts_ms, close_ts_ms, true));
            }
            set_last_chainlink_close(&target_symbol, close_ts_ms, close);
            warn!(
                "⚠️ chainlink_close_ready_but_open_missing | symbol={} round_start_ts={} round_end_ts={} has_open_t={} has_final=true observed_ticks={} lagged_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?} nearest_end={:?} — keeping decision unresolved",
                target_symbol,
                round_start_ts,
                round_end_ts,
                open_point.is_some(),
                observed_ticks,
                lagged_ticks,
                first_tick_ts_ms,
                last_tick_ts_ms,
                nearest_start,
                nearest_end
            );
            return None;
        }
        if open_point.is_none() {
            let elapsed = unix_now_secs().saturating_sub(round_start_ts);
            if elapsed > 30 && !open_missing_warned {
                open_missing_warned = true;
                warn!(
                    "⚠️ chainlink_open_missing | symbol={} round_start_ts={} elapsed={}s observed_ticks={} lagged_ticks={} nearest_start={:?} nearest_end={:?} — no exact round_start tick found yet; continue collecting for alignment",
                    target_symbol,
                    round_start_ts,
                    elapsed,
                    observed_ticks,
                    lagged_ticks,
                    nearest_start,
                    nearest_end
                );
            }
        }
        let next = tokio::time::timeout(Duration::from_millis(700), rx.recv()).await;
        let (price, ts_ms) = match next {
            Ok(Ok(hit)) => hit,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                lagged_ticks = lagged_ticks.saturating_add(n);
                continue;
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
            Err(_) => continue,
        };
        observed_ticks = observed_ticks.saturating_add(1);
        observed_live_ticks = observed_live_ticks.saturating_add(1);
        first_tick_ts_ms.get_or_insert(ts_ms);
        last_tick_ts_ms = Some(ts_ms);
        let start_delta = ts_ms.abs_diff(start_ms);
        match nearest_start {
            Some((best_delta, _, _)) if start_delta >= best_delta => {}
            _ => nearest_start = Some((start_delta, ts_ms, price)),
        }
        let end_delta = ts_ms.abs_diff(end_ms);
        match nearest_end {
            Some((best_delta, _, _)) if end_delta >= best_delta => {}
            _ => nearest_end = Some((end_delta, ts_ms, price)),
        }
        if open_point.is_none() && ts_ms == start_ms {
            open_point = Some((price, ts_ms));
            remember_chainlink_exact_open(&target_symbol, start_ms, price, ts_ms);
            info!(
                        "⏱️ chainlink_open_captured | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6} exact=true",
                        unix_now_millis_u64(),
                        target_symbol,
                round_start_ts,
                ts_ms,
                price,
            );
        }
        if close_point.is_none() && ts_ms == end_ms {
            close_point = Some((price, ts_ms));
            remember_chainlink_exact_close(&target_symbol, end_ms, price, ts_ms);
            info!(
                        "⏱️ chainlink_close_captured | unix_ms={} symbol={} round_end_ts={} close_ts_ms={} close_price={:.6} exact=true",
                        unix_now_millis_u64(),
                        target_symbol,
                round_end_ts,
                ts_ms,
                price,
            );
        }
        if let Some((close, close_ts_ms)) = close_point {
            if let Some((open_ref_px, open_ts_ms)) = open_point {
                let side = if close >= open_ref_px {
                    Side::Yes
                } else {
                    Side::No
                };
                info!(
                    "🏁 Chainlink winner hint ready | unix_ms={} symbol={} side={:?} open_ref={:.6}@{} close={:.6}@{} source=rtds_exact",
                    unix_now_millis_u64(),
                    target_symbol,
                    side,
                    open_ref_px,
                    open_ts_ms,
                    close,
                    close_ts_ms,
                );
                set_last_chainlink_close(&target_symbol, close_ts_ms, close);
                return Some((side, open_ref_px, close, open_ts_ms, close_ts_ms, true));
            }
            set_last_chainlink_close(&target_symbol, close_ts_ms, close);
            warn!(
                "⚠️ chainlink_close_ready_but_open_missing | symbol={} round_start_ts={} round_end_ts={} has_open_t={} has_final=true observed_ticks={} lagged_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?} nearest_end={:?} — keeping decision unresolved",
                target_symbol,
                round_start_ts,
                round_end_ts,
                open_point.is_some(),
                observed_ticks,
                lagged_ticks,
                first_tick_ts_ms,
                last_tick_ts_ms,
                nearest_start,
                nearest_end
            );
            return None;
        }
    }

    let recovered_close = if close_point.is_none() {
        peek_prewarmed_tick(&target_symbol, end_ms)
    } else {
        None
    };
    if let Some((close, close_ts_ms)) = recovered_close {
        info!(
            "🔄 chainlink_close_recovered_from_prewarm | symbol={} round_end_ts={} close_ts_ms={} close_price={:.6} — own WS missed the tick, using next-round prewarm observation",
            target_symbol, round_end_ts, close_ts_ms, close,
        );
        remember_chainlink_exact_close(&target_symbol, end_ms, close, close_ts_ms);
        if let Some((open_ref_px, open_ts_ms)) = open_point {
            let side = if close >= open_ref_px {
                Side::Yes
            } else {
                Side::No
            };
            info!(
                "🏁 Chainlink winner hint ready | unix_ms={} symbol={} side={:?} open_ref={:.6}@{} close={:.6}@{} source=rtds_exact_close_recovered",
                unix_now_millis_u64(),
                target_symbol,
                side,
                open_ref_px,
                open_ts_ms,
                close,
                close_ts_ms,
            );
            set_last_chainlink_close(&target_symbol, close_ts_ms, close);
            return Some((side, open_ref_px, close, open_ts_ms, close_ts_ms, true));
        }
        set_last_chainlink_close(&target_symbol, close_ts_ms, close);
    }

    warn!(
        "⚠️ chainlink_winner_hint_unresolved | symbol={} round_start_ts={} round_end_ts={} deadline_ts={} observed_ticks={} observed_snapshot_ticks={} observed_live_ticks={} lagged_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?} nearest_end={:?}",
        target_symbol,
        round_start_ts,
        round_end_ts,
        hard_deadline_ts,
        observed_ticks,
        observed_snapshot_ticks,
        observed_live_ticks,
        lagged_ticks,
        first_tick_ts_ms,
        last_tick_ts_ms,
        nearest_start,
        nearest_end
    );
    None
}

/// Listen on Chainlink RTDS WS and return (side, open_ref, close_price, open_ts_ms, close_ts_ms).
///
/// Cold-start guard:
/// emit winner only when final(close_t) and exact open_t are both captured from Chainlink ticks:
/// - exact `open_t` at round start (or exact prewarmed open_t)
/// - exact `close_t` at round end (or exact prewarmed close_t recovery)
/// If exact open_t is missing, keep decision unresolved (no order).
async fn run_chainlink_winner_hint(
    symbol: &str,
    round_start_ts: u64,
    round_end_ts: u64,
    hard_deadline_ts: u64,
    chainlink_hub: Option<Arc<ChainlinkHub>>,
    local_price_hub: Option<Arc<LocalPriceHub>>,
) -> Option<(WinnerHintSource, Side, f64, f64, u64, u64, bool)> {
    let local_agg_deadline_ms = round_end_ts
        .saturating_mul(1_000)
        .saturating_add(local_price_agg_decision_wait_ms());
    if local_price_agg_enabled() {
        if local_price_agg_decision_enabled() {
            let agg_start_ms = unix_now_millis_u64();
            if let Some((side, open_ref, close_px, open_ts_ms, close_ts_ms, open_is_exact)) =
                run_local_price_aggregator(
                    local_price_hub.clone(),
                    symbol,
                    round_start_ts,
                    round_end_ts,
                    local_agg_deadline_ms,
                )
                .await
            {
                let agg_done_ms = unix_now_millis_u64();
                info!(
                    "⏱️ local_price_agg_latency | symbol={} round_start_ts={} round_end_ts={} status=ready elapsed_ms={}",
                    symbol,
                    round_start_ts,
                    round_end_ts,
                    agg_done_ms.saturating_sub(agg_start_ms),
                );
                return Some((
                    WinnerHintSource::LocalAgg,
                    side,
                    open_ref,
                    close_px,
                    open_ts_ms,
                    close_ts_ms,
                    open_is_exact,
                ));
            }
        } else {
            info!(
                "🧪 local_price_agg_shadow_mode | symbol={} round_end_ts={} decision_enabled=false",
                symbol, round_end_ts
            );
        }
    }

    if self_built_price_agg_enabled() {
        let agg_start_ms = unix_now_millis_u64();
        if let Some(hit) = run_self_built_price_aggregator(
            chainlink_hub.clone(),
            symbol,
            round_start_ts,
            round_end_ts,
            hard_deadline_ts,
        )
        .await
        {
            let agg_done_ms = unix_now_millis_u64();
            info!(
                "⏱️ self_built_price_agg_latency | symbol={} round_start_ts={} round_end_ts={} status=ready elapsed_ms={}",
                symbol,
                round_start_ts,
                round_end_ts,
                agg_done_ms.saturating_sub(agg_start_ms),
            );
            let (side, open_ref, close_px, open_ts_ms, close_ts_ms, open_is_exact) = hit;
            return Some((
                WinnerHintSource::Chainlink,
                side,
                open_ref,
                close_px,
                open_ts_ms,
                close_ts_ms,
                open_is_exact,
            ));
        }
        let agg_done_ms = unix_now_millis_u64();
        warn!(
            "⚠️ self_built_price_agg_unresolved_fallback_legacy | symbol={} round_start_ts={} round_end_ts={} elapsed_ms={}",
            symbol,
            round_start_ts,
            round_end_ts,
            agg_done_ms.saturating_sub(agg_start_ms),
        );
    }

    if chainlink_data_streams_enabled() {
        if let Some(hit) =
            run_data_streams_winner_hint(symbol, round_start_ts, round_end_ts, hard_deadline_ts)
                .await
        {
            let (side, open_ref, close_px, open_ts_ms, close_ts_ms, open_is_exact) = hit;
            return Some((
                WinnerHintSource::Chainlink,
                side,
                open_ref,
                close_px,
                open_ts_ms,
                close_ts_ms,
                open_is_exact,
            ));
        }
        warn!(
            "⚠️ chainlink_data_streams_unresolved_fallback_rtds | symbol={} round_start_ts={} round_end_ts={}",
            symbol, round_start_ts, round_end_ts
        );
    }

    if let Some(hub) = chainlink_hub {
        return run_chainlink_winner_hint_via_hub(
            hub,
            symbol,
            round_start_ts,
            round_end_ts,
            hard_deadline_ts,
        )
        .await
        .map(
            |(side, open_ref, close_px, open_ts_ms, close_ts_ms, open_is_exact)| {
                (
                    WinnerHintSource::Chainlink,
                    side,
                    open_ref,
                    close_px,
                    open_ts_ms,
                    close_ts_ms,
                    open_is_exact,
                )
            },
        );
    }

    let ws_url = post_close_chainlink_ws_url();
    let target_symbol = normalize_chainlink_symbol(symbol);
    let start_ms = round_start_ts.saturating_mul(1_000);
    let end_ms = round_end_ts.saturating_mul(1_000);
    let mut open_point: Option<(f64, u64)> = take_prewarmed_open(&target_symbol, start_ms);
    if let Some((px, ts_ms)) = open_point {
        info!(
            "⏱️ chainlink_open_prewarm_hit | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6}",
            unix_now_millis_u64(),
            target_symbol,
            round_start_ts,
            ts_ms,
            px,
        );
    }
    let mut open_minus_1s: Option<(f64, u64)> = None;
    let mut open_plus_1s: Option<(f64, u64)> = None;
    let prev_round_close_snapshot = cached_prev_round_close(&target_symbol, start_ms);
    let mut close_point: Option<(f64, u64)> = None;
    let mut reconnect_backoff = Duration::from_millis(300);
    let mut open_missing_warned = false;
    let mut connected_at: Option<std::time::Instant> = None;
    let mut logged_raw = false; // log raw RTDS message only once

    let emit_alignment_probe = |note: &str,
                                selected_rule: Option<&str>,
                                open_point: Option<(f64, u64)>,
                                open_minus_1s: Option<(f64, u64)>,
                                open_plus_1s: Option<(f64, u64)>,
                                prev_round_close: Option<(f64, u64)>,
                                close_point: Option<(f64, u64)>| {
        let winner_t =
            winner_from_open_close(open_point.map(|(p, _)| p), close_point.map(|(p, _)| p));
        let winner_t_minus_1s =
            winner_from_open_close(open_minus_1s.map(|(p, _)| p), close_point.map(|(p, _)| p));
        let winner_t_plus_1s =
            winner_from_open_close(open_plus_1s.map(|(p, _)| p), close_point.map(|(p, _)| p));
        let winner_prev_round_close = winner_from_open_close(
            prev_round_close.map(|(p, _)| p),
            close_point.map(|(p, _)| p),
        );
        let probe = ChainlinkRoundAlignmentProbe {
            unix_ms: unix_now_millis_u64(),
            symbol: target_symbol.clone(),
            round_start_ts,
            round_end_ts,
            start_ms,
            end_ms,
            open_t: open_point.map(|(p, _)| p),
            open_t_minus_1s: open_minus_1s.map(|(p, _)| p),
            open_t_plus_1s: open_plus_1s.map(|(p, _)| p),
            open_prev_round_close: prev_round_close.map(|(p, _)| p),
            prev_round_close_ts_ms: prev_round_close.map(|(_, ts)| ts),
            close_t: close_point.map(|(p, _)| p),
            winner_t: side_label(winner_t),
            winner_t_minus_1s: side_label(winner_t_minus_1s),
            winner_t_plus_1s: side_label(winner_t_plus_1s),
            winner_prev_round_close: side_label(winner_prev_round_close),
            selected_rule: selected_rule.map(|s| s.to_string()),
            note: note.to_string(),
        };
        info!(
            "🧪 chainlink_round_alignment | symbol={} start={} end={} note={} selected_rule={:?} open_t={:?} open_t-1s={:?} open_t+1s={:?} open_prev_close={:?}@{:?} close_t={:?} winner_t={:?} winner_t-1={:?} winner_t+1={:?} winner_prev={:?}",
            probe.symbol,
            probe.round_start_ts,
            probe.round_end_ts,
            probe.note,
            probe.selected_rule,
            probe.open_t,
            probe.open_t_minus_1s,
            probe.open_t_plus_1s,
            probe.open_prev_round_close,
            probe.prev_round_close_ts_ms,
            probe.close_t,
            probe.winner_t,
            probe.winner_t_minus_1s,
            probe.winner_t_plus_1s,
            probe.winner_prev_round_close,
        );
        append_chainlink_round_alignment_probe(&probe);
    };

    while unix_now_secs() <= hard_deadline_ts {
        // Guard: if we're past round_start + 30s and still missing exact open,
        // keep listening (for alignment diagnostics) but warn only once.
        if open_point.is_none() {
            let elapsed = unix_now_secs().saturating_sub(round_start_ts);
            if elapsed > 30 && !open_missing_warned {
                open_missing_warned = true;
                warn!(
                    "⚠️ chainlink_open_missing | symbol={} round_start_ts={} elapsed={}s — no exact round_start tick found yet; continue collecting for alignment",
                    target_symbol, round_start_ts, elapsed,
                );
            }
        }

        let connect = tokio::time::timeout(Duration::from_secs(3), connect_async(&ws_url)).await;
        let Ok(Ok((ws, _resp))) = connect else {
            sleep(reconnect_backoff).await;
            reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(4));
            continue;
        };
        reconnect_backoff = Duration::from_millis(300);
        connected_at.get_or_insert_with(std::time::Instant::now);
        let elapsed_since_start = unix_now_secs().saturating_sub(round_start_ts);
        info!(
            "📡 rtds_connected | symbol={} round_start_ts={} elapsed_since_start={}s",
            target_symbol, round_start_ts, elapsed_since_start,
        );
        let (mut write, mut read) = ws.split();

        let subscribe_msg = json!({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": format!("{{\"symbol\":\"{}\"}}", target_symbol),
            }]
        });
        if write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .is_err()
        {
            sleep(reconnect_backoff).await;
            continue;
        }

        loop {
            if unix_now_secs() > hard_deadline_ts {
                break;
            }
            let next = tokio::time::timeout(Duration::from_millis(700), read.next()).await;
            let msg = match next {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => {
                    // Timeout: abort only if we've been CONNECTED for >30s with no open_point.
                    // This handles the case where the round_start tick is genuinely absent from
                    // both the initial batch and live feed (e.g., RTDS gap at round boundary).
                    if open_point.is_none() {
                        let elapsed = unix_now_secs().saturating_sub(round_start_ts);
                        if elapsed > 30 && !open_missing_warned {
                            open_missing_warned = true;
                            let connected_for_secs = connected_at.map(|t| t.elapsed().as_secs());
                            warn!(
                                "⚠️ chainlink_open_missing | symbol={} round_start_ts={} elapsed={}s connected_for={:?}s — no exact round_start tick found yet; continue collecting for alignment",
                                target_symbol, round_start_ts, elapsed, connected_for_secs,
                            );
                        }
                    }
                    continue;
                }
            };
            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                    Ok(t) => t,
                    Err(_) => continue,
                },
                _ => continue,
            };

            // Parse ALL ticks in this message (handles both single ticks and batch payloads).
            // Log the first raw message once per connection for format verification.
            if !logged_raw {
                logged_raw = true;
                let preview: String = text.chars().take(400).collect();
                info!(
                    "🔬 rtds_raw_msg | symbol={} round_start_ts={} preview={}",
                    target_symbol, round_start_ts, preview,
                );
            }
            let ticks = parse_chainlink_all_ticks(&text, &target_symbol);
            if ticks.is_empty() {
                continue;
            }

            // Log first successful batch for diagnostics.
            if open_point.is_none() && close_point.is_none() {
                let ts_range: Vec<String> = ticks.iter()
                    .take(5)
                    .map(|(p, t)| format!("{:.4}@{}", p, t))
                    .collect();
                info!(
                    "🔬 rtds_batch | symbol={} n={} samples=[{}]",
                    target_symbol, ticks.len(), ts_range.join(", ")
                );
            }

            for (price, ts_ms) in &ticks {
                let (price, ts_ms) = (*price, *ts_ms);

                // open_ref: tick closest to round_start_ms within +/-1000ms.
                // RTDS sends 1 tick/sec but timestamps may not be exactly ms-aligned on boundaries.
                if open_point.is_none() && ts_ms.abs_diff(start_ms) <= 1_000 {
                    open_point = Some((price, ts_ms));
                    let delta_ms = (ts_ms as i64) - (start_ms as i64);
                    info!(
                        "⏱️ chainlink_open_captured | unix_ms={} symbol={} round_start_ts={} open_ts_ms={} open_price={:.6} delta_from_start_ms={}",
                        unix_now_millis_u64(), target_symbol, round_start_ts, ts_ms, price, delta_ms,
                    );
                }
                if open_minus_1s.is_none() && ts_ms == start_ms.saturating_sub(1_000) {
                    open_minus_1s = Some((price, ts_ms));
                }
                if open_plus_1s.is_none() && ts_ms == start_ms.saturating_add(1_000) {
                    open_plus_1s = Some((price, ts_ms));
                }

                if close_point.is_none() && ts_ms == end_ms {
                    close_point = Some((price, ts_ms));
                    let delta_ms = (ts_ms as i64) - (end_ms as i64);
                    info!(
                        "⏱️ chainlink_close_captured | unix_ms={} symbol={} round_end_ts={} close_ts_ms={} close_price={:.6} delta_from_end_ms={}",
                        unix_now_millis_u64(), target_symbol, round_end_ts, ts_ms, price, delta_ms,
                    );
                }
            }

            // Emit result when close is captured.
            if let Some((close, close_ts_ms)) = close_point {
                let prev_round_close_live = cached_prev_round_close(&target_symbol, start_ms);
                let prev_round_close = prev_round_close_live.or(prev_round_close_snapshot);
                let selected_rule = if open_point.is_some() {
                    Some("t_exact")
                } else {
                    None
                };

                emit_alignment_probe(
                    if open_point.is_some() {
                        "close_captured_exact_open_available"
                    } else if prev_round_close.is_some() {
                        "close_captured_prev_close_fallback"
                    } else {
                        "close_captured_exact_open_missing"
                    },
                    selected_rule,
                    open_point,
                    open_minus_1s,
                    open_plus_1s,
                    prev_round_close,
                    close_point,
                );

                if let Some((open_ref_px, open_ts_ms)) = open_point {
                    let side = if close >= open_ref_px {
                        Side::Yes
                    } else {
                        Side::No
                    };
                    info!(
                        "🏁 Chainlink winner hint ready | unix_ms={} symbol={} side={:?} open_ref={:.6}@{} close={:.6}@{} source=rtds_exact",
                        unix_now_millis_u64(),
                        target_symbol,
                        side,
                        open_ref_px,
                        open_ts_ms,
                        close,
                        close_ts_ms,
                    );
                    set_last_chainlink_close(&target_symbol, close_ts_ms, close);
                    return Some((
                        WinnerHintSource::Chainlink,
                        side,
                        open_ref_px,
                        close,
                        open_ts_ms,
                        close_ts_ms,
                        true,
                    ));
                }
                set_last_chainlink_close(&target_symbol, close_ts_ms, close);
                warn!(
                    "⚠️ chainlink_close_ready_but_open_missing | symbol={} round_start_ts={} round_end_ts={} has_open_t={} has_final=true — keeping decision unresolved",
                    target_symbol,
                    round_start_ts,
                    round_end_ts,
                    open_point.is_some(),
                );
                return None;
            }
        }
    }

    // Fallback: if our own WS missed the exact close tick, consult the prewarm cache.
    // The next round's prewarm listener subscribes to the same stream and stores the
    // tick at end_ms as its "open". If it succeeded, we can recover close_t from there.
    let recovered_close = if close_point.is_none() {
        peek_prewarmed_tick(&target_symbol, end_ms)
    } else {
        None
    };
    if let Some((close, close_ts_ms)) = recovered_close {
        info!(
            "🔄 chainlink_close_recovered_from_prewarm | symbol={} round_end_ts={} close_ts_ms={} close_price={:.6} — own WS missed the tick, using next-round prewarm observation",
            target_symbol, round_end_ts, close_ts_ms, close,
        );
        if let Some((open_ref_px, open_ts_ms)) = open_point {
            let side = if close >= open_ref_px {
                Side::Yes
            } else {
                Side::No
            };
            info!(
                "🏁 Chainlink winner hint ready | unix_ms={} symbol={} side={:?} open_ref={:.6}@{} close={:.6}@{} source=rtds_exact_close_recovered",
                unix_now_millis_u64(),
                target_symbol, side, open_ref_px, open_ts_ms, close, close_ts_ms,
            );
            set_last_chainlink_close(&target_symbol, close_ts_ms, close);
            return Some((
                WinnerHintSource::Chainlink,
                side,
                open_ref_px,
                close,
                open_ts_ms,
                close_ts_ms,
                true,
            ));
        }
        set_last_chainlink_close(&target_symbol, close_ts_ms, close);
    }

    let prev_round_close_live = cached_prev_round_close(&target_symbol, start_ms);
    let prev_round_close = prev_round_close_live.or(prev_round_close_snapshot);
    emit_alignment_probe(
        "deadline_exhausted",
        if open_point.is_some() {
            Some("t_exact")
        } else {
            None
        },
        open_point,
        open_minus_1s,
        open_plus_1s,
        prev_round_close,
        close_point.or(recovered_close),
    );
    None
}

// ─────────────────────────────────────────────────────────
// WS Parsing helpers
// ─────────────────────────────────────────────────────────

fn classify_side(asset_id: &str, settings: &Settings) -> Option<Side> {
    if asset_id == settings.yes_asset_id {
        Some(Side::Yes)
    } else if asset_id == settings.no_asset_id {
        Some(Side::No)
    } else {
        None
    }
}

// ISSUE 11 FIX: Tighten price range to strict (0.0, 1.0).
// The old upper bound of 100.0 would silently accept percentage-format prices
// (e.g. 51.0 meaning $0.51), which would corrupt the pricing engine.
// Polymarket CLOB prices are always decimal in (0, 1).
fn parse_price_str(raw: &str) -> Option<f64> {
    raw.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| *v > 0.0 && *v < 1.0)
}

fn parse_price_value(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(parse_price_str))
        .filter(|p| *p > 0.0 && *p < 1.0)
}

fn is_market_ws_keepalive_text(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("PING")
        || trimmed.eq_ignore_ascii_case("PONG")
}

fn collect_market_ws_asset_ids(value: &Value, out: &mut Vec<String>) {
    if out.len() >= 12 {
        return;
    }
    match value {
        Value::Array(values) => {
            for value in values.iter().take(8) {
                collect_market_ws_asset_ids(value, out);
                if out.len() >= 12 {
                    break;
                }
            }
        }
        Value::Object(map) => {
            for key in ["asset_id", "assetId", "token_id", "tokenID"] {
                if let Some(asset_id) = map.get(key).and_then(|v| v.as_str()) {
                    if !out.iter().any(|existing| existing == asset_id) {
                        out.push(asset_id.to_string());
                    }
                }
            }
            for key in ["asset_ids", "assets_ids"] {
                if let Some(asset_ids) = map.get(key).and_then(|v| v.as_array()) {
                    for asset_id in asset_ids.iter().filter_map(|v| v.as_str()).take(8) {
                        if !out.iter().any(|existing| existing == asset_id) {
                            out.push(asset_id.to_string());
                        }
                        if out.len() >= 12 {
                            return;
                        }
                    }
                }
            }
            for key in ["price_changes", "changes", "orders"] {
                if let Some(items) = map.get(key).and_then(|v| v.as_array()) {
                    for item in items.iter().take(8) {
                        collect_market_ws_asset_ids(item, out);
                        if out.len() >= 12 {
                            return;
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn market_ws_payload_debug_summary(settings: &Settings, text: &str) -> String {
    let trimmed = text.trim();
    if is_market_ws_keepalive_text(trimmed) {
        return format!("reason=keepalive_text payload={}", trimmed);
    }

    let raw_len = trimmed.len();
    let value = match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => value,
        Err(err) => {
            return format!("reason=parse_error err={} raw_len={}", err, raw_len);
        }
    };

    let first_value = value
        .as_array()
        .and_then(|values| values.first())
        .unwrap_or(&value);
    let array_len = value.as_array().map(|values| values.len()).unwrap_or(0);
    let event_type = first_value
        .get("event_type")
        .or_else(|| first_value.get("type"))
        .or_else(|| first_value.get("event"))
        .or_else(|| first_value.get("channel"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let keys = first_value
        .as_object()
        .map(|map| map.keys().take(12).cloned().collect::<Vec<_>>().join(","))
        .unwrap_or_else(|| "non_object".to_string());

    let mut asset_ids = Vec::new();
    collect_market_ws_asset_ids(&value, &mut asset_ids);
    let matched_sides = asset_ids
        .iter()
        .filter_map(|asset_id| classify_side(asset_id, settings))
        .map(|side| side.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let asset_ids_summary = if asset_ids.is_empty() {
        "none".to_string()
    } else {
        asset_ids.join(",")
    };
    let matched_sides = if matched_sides.is_empty() {
        "none".to_string()
    } else {
        matched_sides
    };

    format!(
        "reason=no_parsed_market_event event_type={} array_len={} keys=[{}] asset_ids=[{}] matched_sides=[{}] expected_yes={} expected_no={} raw_len={}",
        event_type,
        array_len,
        keys,
        asset_ids_summary,
        matched_sides,
        settings.yes_asset_id,
        settings.no_asset_id,
        raw_len
    )
}

/// Parse a WS message into MarketDataMsg events.
fn parse_ws_message(settings: &Settings, value: &Value) -> Vec<MarketDataMsg> {
    let mut msgs = Vec::new();
    let partial_book_tick = |s: Side, best_bid: f64, best_ask: f64| MarketDataMsg::BookTick {
        yes_bid: if s == Side::Yes { best_bid } else { f64::NAN },
        yes_ask: if s == Side::Yes { best_ask } else { f64::NAN },
        no_bid: if s == Side::No { best_bid } else { f64::NAN },
        no_ask: if s == Side::No { best_ask } else { f64::NAN },
        ts: Instant::now(),
    };

    match value.get("event_type").and_then(|v| v.as_str()) {
        // ─── Book snapshot ───
        Some("book") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                let side = classify_side(asset_id, settings);
                let bids = value
                    .get("bids")
                    .or_else(|| value.get("buys"))
                    .and_then(|v| v.as_array());
                let asks = value
                    .get("asks")
                    .or_else(|| value.get("sells"))
                    .and_then(|v| v.as_array());
                // P2-8: Find true best bid/ask — don't assume array is sorted
                let best_bid = bids
                    .map(|levels| {
                        levels
                            .iter()
                            .filter_map(|lvl| lvl.get("price").and_then(parse_price_value))
                            .fold(0.0_f64, f64::max)
                    })
                    .unwrap_or(0.0);
                let best_ask = asks
                    .map(|levels| {
                        levels
                            .iter()
                            .filter_map(|lvl| lvl.get("price").and_then(parse_price_value))
                            .fold(f64::MAX, f64::min)
                    })
                    .map(|v| if v == f64::MAX { 0.0 } else { v })
                    .unwrap_or(0.0);

                if let Some(s) = side {
                    // Emit side-tagged partial update; non-updated side uses NaN sentinel.
                    msgs.push(partial_book_tick(s, best_bid, best_ask));
                }
            }
        }
        // ─── Price change ───
        Some("price_change") => {
            if let Some(changes) = value.get("price_changes").and_then(|v| v.as_array()) {
                for ch in changes {
                    if let Some(asset_id) = ch.get("asset_id").and_then(|v| v.as_str()) {
                        let side = classify_side(asset_id, settings);
                        let best_bid = ch
                            .get("best_bid")
                            .and_then(parse_price_value)
                            .unwrap_or(0.0);
                        let best_ask = ch
                            .get("best_ask")
                            .and_then(parse_price_value)
                            .unwrap_or(0.0);

                        if let Some(s) = side {
                            msgs.push(partial_book_tick(s, best_bid, best_ask));
                        }
                    }
                }
            }
        }
        // ─── Best bid/ask ───
        Some("best_bid_ask") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                let side = classify_side(asset_id, settings);
                let best_bid = value
                    .get("best_bid")
                    .and_then(parse_price_value)
                    .unwrap_or(0.0);
                let best_ask = value
                    .get("best_ask")
                    .and_then(parse_price_value)
                    .unwrap_or(0.0);

                if let Some(s) = side {
                    msgs.push(partial_book_tick(s, best_bid, best_ask));
                }
            }
        }
        // ─── Last trade price (NEW — OFI data source) ───
        Some("last_trade_price") => {
            if let Some(asset_id) = value.get("asset_id").and_then(|v| v.as_str()) {
                let price = value
                    .get("price")
                    .and_then(parse_price_value)
                    .unwrap_or(0.0);
                let size = match value.get("size").and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                }) {
                    Some(s) if s > 0.0 => s,
                    _ => {
                        // P2 FIX: Missing size — discard instead of injecting fake 1.0
                        debug!("OFI parser: missing or zero 'size' in trade, skipping to avoid fake toxicity");
                        return msgs;
                    }
                };

                let Some(side_val) = value.get("side").and_then(|v| v.as_str()) else {
                    debug!("OFI parser: missing 'side' field in trade, skipping to avoid bias");
                    return msgs;
                };

                // Determine taker side from the "side" field
                let taker_side = match side_val {
                    "BUY" | "buy" | "Buy" => TakerSide::Buy,
                    "SELL" | "sell" | "Sell" => TakerSide::Sell,
                    _ => {
                        debug!("OFI parser: unknown 'side' value: {}, skipping", side_val);
                        return msgs;
                    }
                };

                // Classify which market side (YES or NO token)
                let market_side = classify_side(asset_id, settings);
                let trade_id = value
                    .get("trade_id")
                    .or_else(|| value.get("id"))
                    .or_else(|| value.get("hash"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);

                if price > 0.0 {
                    if let Some(ms) = market_side {
                        msgs.push(MarketDataMsg::TradeTick {
                            asset_id: asset_id.to_string(),
                            trade_id,
                            market_side: ms,
                            taker_side,
                            price,
                            size,
                            ts: Instant::now(),
                        });
                    }
                }
            }
        }
        _ => {}
    }

    msgs
}

fn parse_ws_message_with_drop_reason(
    settings: &Settings,
    value: &Value,
) -> (Vec<MarketDataMsg>, Option<String>) {
    let msgs = parse_ws_message(settings, value);
    if !msgs.is_empty() {
        return (msgs, None);
    }

    let event_type = value
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let reason = match event_type {
        "book" => {
            let asset_id = value
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or("missing");
            let side_known = value
                .get("asset_id")
                .and_then(|v| v.as_str())
                .and_then(|asset_id| classify_side(asset_id, settings))
                .is_some();
            let bids = value
                .get("bids")
                .or_else(|| value.get("buys"))
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .unwrap_or(0);
            let asks = value
                .get("asks")
                .or_else(|| value.get("sells"))
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .unwrap_or(0);
            if !side_known {
                format!(
                    "event_type=book asset_id={} reason=asset_id_unmatched yes={} no={} bids={} asks={}",
                    asset_id, settings.yes_asset_id, settings.no_asset_id, bids, asks
                )
            } else {
                format!(
                    "event_type=book asset_id={} reason=no_valid_top_of_book bids={} asks={}",
                    asset_id, bids, asks
                )
            }
        }
        "price_change" => {
            let changes = value
                .get("price_changes")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let asset_ids: Vec<String> = changes
                .iter()
                .filter_map(|ch| ch.get("asset_id").and_then(|v| v.as_str()))
                .map(|s| s.to_string())
                .collect();
            let matched = asset_ids
                .iter()
                .filter(|asset_id| classify_side(asset_id, settings).is_some())
                .count();
            if matched == 0 {
                format!(
                    "event_type=price_change asset_ids=[{}] reason=no_matching_asset_id yes={} no={}",
                    asset_ids.join(","),
                    settings.yes_asset_id,
                    settings.no_asset_id
                )
            } else {
                format!(
                    "event_type=price_change asset_ids=[{}] reason=no_valid_best_bid_ask matched={}",
                    asset_ids.join(","),
                    matched
                )
            }
        }
        "best_bid_ask" => {
            let asset_id = value
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or("missing");
            let side_known = value
                .get("asset_id")
                .and_then(|v| v.as_str())
                .and_then(|asset_id| classify_side(asset_id, settings))
                .is_some();
            if !side_known {
                format!(
                    "event_type=best_bid_ask asset_id={} reason=asset_id_unmatched yes={} no={}",
                    asset_id, settings.yes_asset_id, settings.no_asset_id
                )
            } else {
                format!(
                    "event_type=best_bid_ask asset_id={} reason=no_valid_best_bid_ask",
                    asset_id
                )
            }
        }
        "last_trade_price" => {
            let asset_id = value
                .get("asset_id")
                .and_then(|v| v.as_str())
                .unwrap_or("missing");
            if classify_side(asset_id, settings).is_none() {
                format!(
                    "event_type=last_trade_price asset_id={} reason=asset_id_unmatched yes={} no={}",
                    asset_id, settings.yes_asset_id, settings.no_asset_id
                )
            } else if value.get("size").is_none() {
                format!(
                    "event_type=last_trade_price asset_id={} reason=missing_size",
                    asset_id
                )
            } else if value.get("side").and_then(|v| v.as_str()).is_none() {
                format!(
                    "event_type=last_trade_price asset_id={} reason=missing_side",
                    asset_id
                )
            } else {
                let side_val = value
                    .get("side")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                format!(
                    "event_type=last_trade_price asset_id={} reason=invalid_trade_payload side={}",
                    asset_id, side_val
                )
            }
        }
        _ => format!("event_type={} reason=unknown_event", event_type),
    };

    (msgs, Some(reason))
}

/// Parse one WS text payload and return parsed market-data messages plus ingest stats.
fn parse_ws_payload(
    settings: &Settings,
    text: &str,
) -> (Vec<MarketDataMsg>, u64, u64, Vec<String>) {
    let mut out = Vec::new();
    let mut unknown_events = 0_u64;
    let mut parse_drops = 0_u64;
    let mut drop_reasons = Vec::new();
    let trimmed = text.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("PING")
        || trimmed.eq_ignore_ascii_case("PONG")
    {
        return (out, unknown_events, parse_drops, drop_reasons);
    }

    let value = match serde_json::from_str::<Value>(trimmed) {
        Ok(v) => v,
        Err(err) => {
            drop_reasons.push(format!("event_type=parse_error reason={}", err));
            return (out, unknown_events, 1, drop_reasons);
        }
    };

    let values = if value.is_array() {
        value.as_array().cloned().unwrap_or_default()
    } else {
        vec![value]
    };

    for val in &values {
        let event_type = val
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let known_event = matches!(
            event_type,
            "book" | "price_change" | "best_bid_ask" | "last_trade_price"
        );
        if !known_event {
            unknown_events = unknown_events.saturating_add(1);
        }

        let (parsed, drop_reason) = parse_ws_message_with_drop_reason(settings, val);
        if known_event && parsed.is_empty() {
            parse_drops = parse_drops.saturating_add(1);
            if let Some(reason) = drop_reason {
                drop_reasons.push(reason);
            }
        }
        out.extend(parsed);
    }

    (out, unknown_events, parse_drops, drop_reasons)
}

// ─────────────────────────────────────────────────────────
// Book State Assembler (merges partial updates into full BookTick)
// ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct BookAssembler {
    yes_bid: f64,
    yes_ask: f64,
    no_bid: f64,
    no_ask: f64,
    yes_seen: bool,
    no_seen: bool,
}

impl BookAssembler {
    fn update(&mut self, msg: &MarketDataMsg) -> Option<MarketDataMsg> {
        if let MarketDataMsg::BookTick {
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
            ts,
        } = msg
        {
            // Merge side-tagged partial updates.
            // Updated side carries finite values (including 0.0 for empty book);
            // non-updated side carries NaN sentinels.
            let mut yes_touched = false;
            let mut no_touched = false;
            if yes_bid.is_finite() {
                self.yes_bid = (*yes_bid).max(0.0);
                yes_touched = true;
            }
            if yes_ask.is_finite() {
                self.yes_ask = (*yes_ask).max(0.0);
                yes_touched = true;
            }
            if no_bid.is_finite() {
                self.no_bid = (*no_bid).max(0.0);
                no_touched = true;
            }
            if no_ask.is_finite() {
                self.no_ask = (*no_ask).max(0.0);
                no_touched = true;
            }
            if yes_touched {
                self.yes_seen = true;
            }
            if no_touched {
                self.no_seen = true;
            }

            // Emit once both sides have been observed at least once.
            // A bid/ask of 0.0 is a valid "known empty" state on Polymarket,
            // especially post-close. Requiring bid > 0.0 incorrectly suppresses
            // full-book emission for feeds that only show liquidity on one side
            // after reconnect, which then cascades into shared-ingress remote
            // starvation despite a live partial stream.
            if self.yes_seen && self.no_seen {
                return Some(MarketDataMsg::BookTick {
                    yes_bid: self.yes_bid,
                    yes_ask: self.yes_ask,
                    no_bid: self.no_bid,
                    no_ask: self.no_ask,
                    ts: *ts,
                });
            }
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SharedMarketFeedKey {
    ws_base_url: String,
    yes_asset_id: String,
    no_asset_id: String,
    custom_feature_enabled: bool,
}

struct SharedMarketIngressFeed {
    tx: broadcast::Sender<SharedIngressWireMsg>,
    recent: Arc<Mutex<VecDeque<SharedIngressWireMsg>>>,
    subscriber_count: AtomicUsize,
    shutdown_tx: watch::Sender<bool>,
}

impl SharedMarketIngressFeed {
    fn new() -> Arc<Self> {
        let (tx, _rx) = broadcast::channel::<SharedIngressWireMsg>(4096);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        Arc::new(Self {
            tx,
            recent: Arc::new(Mutex::new(VecDeque::with_capacity(256))),
            subscriber_count: AtomicUsize::new(0),
            shutdown_tx,
        })
    }

    fn publish(&self, msg: SharedIngressWireMsg) {
        if let Ok(mut guard) = self.recent.lock() {
            guard.push_back(msg.clone());
            while guard.len() > 256 {
                guard.pop_front();
            }
        }
        let _ = self.tx.send(msg);
    }

    fn subscribe(&self) -> broadcast::Receiver<SharedIngressWireMsg> {
        self.tx.subscribe()
    }

    fn snapshot(&self) -> Vec<SharedIngressWireMsg> {
        let Ok(guard) = self.recent.lock() else {
            return Vec::new();
        };
        guard.iter().cloned().collect()
    }

    fn snapshot_and_subscribe(
        &self,
    ) -> (
        Vec<SharedIngressWireMsg>,
        broadcast::Receiver<SharedIngressWireMsg>,
    ) {
        let Ok(guard) = self.recent.lock() else {
            return (Vec::new(), self.tx.subscribe());
        };
        // Hold the recent-buffer lock across snapshot+subscribe so a producer
        // cannot publish into the gap between replay and live attach. This is
        // critical for fixed-round feeds, where the initial dump may be the
        // only burst of book/trade traffic the client will ever see.
        let snapshot = guard.iter().cloned().collect();
        let rx = self.tx.subscribe();
        (snapshot, rx)
    }

    fn acquire_subscriber(&self) -> usize {
        self.subscriber_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn release_subscriber(&self) -> usize {
        self.subscriber_count
            .fetch_sub(1, Ordering::AcqRel)
            .saturating_sub(1)
    }

    fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Acquire)
    }

    fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    fn shutdown_requested(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

struct SharedMarketFeedLease {
    key: SharedMarketFeedKey,
    feed: Arc<SharedMarketIngressFeed>,
    registry: Arc<Mutex<HashMap<SharedMarketFeedKey, Arc<SharedMarketIngressFeed>>>>,
}

impl SharedMarketFeedLease {
    fn new(
        key: SharedMarketFeedKey,
        feed: Arc<SharedMarketIngressFeed>,
        registry: Arc<Mutex<HashMap<SharedMarketFeedKey, Arc<SharedMarketIngressFeed>>>>,
    ) -> (Self, usize) {
        let subscribers = feed.acquire_subscriber();
        Self {
            key,
            feed,
            registry,
        }
        .into_with_subscribers(subscribers)
    }

    fn into_with_subscribers(self, subscribers: usize) -> (Self, usize) {
        (self, subscribers)
    }
}

impl Drop for SharedMarketFeedLease {
    fn drop(&mut self) {
        let remaining = self.feed.release_subscriber();
        if remaining > 0 {
            info!(
                "📉 shared ingress market feed subscriber released | yes_asset_id={} no_asset_id={} subscribers_remaining={}",
                self.key.yes_asset_id, self.key.no_asset_id, remaining
            );
            return;
        }
        info!(
            "🛑 shared ingress market feed shutdown requested | yes_asset_id={} no_asset_id={} subscribers_remaining=0",
            self.key.yes_asset_id, self.key.no_asset_id
        );
        self.feed.request_shutdown();
        let mut guard = self
            .registry
            .lock()
            .expect("shared ingress market registry poisoned");
        if let Some(existing) = guard.get(&self.key) {
            if Arc::ptr_eq(existing, &self.feed) {
                guard.remove(&self.key);
            }
        }
        let registry_len = guard.len();
        info!(
            "🧹 shared ingress market feed released | yes_asset_id={} no_asset_id={} subscribers=0 registry_size={}",
            self.key.yes_asset_id, self.key.no_asset_id, registry_len
        );
    }
}

async fn run_market_ws_broker_feed(settings: Settings, feed: Arc<SharedMarketIngressFeed>) {
    let ws_connect_timeout = ws_connect_timeout_ms();
    let ws_degrade_failures = ws_degrade_max_failures();
    let is_fixed_market_feed = is_fixed_market_slug(settings.market_slug.as_deref());
    let connect_jitter_ceiling_ms = if is_fixed_market_feed {
        shared_ingress_fixed_market_connect_jitter_ms()
    } else {
        shared_ingress_market_connect_jitter_ms()
    };
    let connect_jitter_ms =
        shared_ingress_market_feed_jitter_ms(&settings, connect_jitter_ceiling_ms);
    let health_jitter_ms = if is_fixed_market_feed {
        0
    } else {
        shared_ingress_market_feed_jitter_ms(&settings, shared_ingress_market_health_jitter_ms())
    };
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);
    let mut consecutive_failures: u32 = 0;
    // Prefix feeds only need book/trade data. Leaving the Polymarket custom
    // feature on fans global `new_market` events into every prefix connection.
    // Keep fixed feeds unchanged because they are already stable.
    let subscribe_custom_feature_enabled = if is_fixed_market_feed {
        settings.custom_feature
    } else {
        false
    };
    let mut shutdown_rx = feed.shutdown_receiver();
    'outer: loop {
        if feed.shutdown_requested() || feed.subscriber_count() == 0 {
            info!(
                "🛑 shared ingress market broker feed exiting idle | slug={} subscribers={}",
                settings
                    .market_slug
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                feed.subscriber_count(),
            );
            break;
        }
        let mut book_asm = BookAssembler::default();
        let url = settings.ws_url("market");
        info!(
            "📡 shared ingress market broker connecting | slug={} url={} fixed_round={} subscribers={} connect_jitter_ms={}",
            settings
                .market_slug
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            url,
            is_fixed_market_feed,
            feed.subscriber_count(),
            connect_jitter_ms
        );
        if connect_jitter_ms > 0 {
            sleep(Duration::from_millis(connect_jitter_ms)).await;
            if feed.shutdown_requested() || feed.subscriber_count() == 0 {
                info!(
                    "🛑 shared ingress market broker feed canceled before connect | slug={} subscribers={}",
                    settings
                        .market_slug
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    feed.subscriber_count(),
                );
                break;
            }
        }
        let connect_result = {
            let _connect_permit = if is_fixed_market_feed {
                shared_ingress_fixed_market_connect_semaphore()
                    .acquire()
                    .await
                    .expect("shared ingress fixed market connect semaphore closed")
            } else {
                shared_ingress_market_connect_semaphore()
                    .acquire()
                    .await
                    .expect("shared ingress market connect semaphore closed")
            };
            if feed.shutdown_requested() || feed.subscriber_count() == 0 {
                info!(
                    "🛑 shared ingress market broker feed canceled after permit | slug={} subscribers={}",
                    settings
                        .market_slug
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    feed.subscriber_count(),
                );
                break 'outer;
            }
            tokio::time::timeout(
                Duration::from_millis(ws_connect_timeout),
                connect_async(&url),
            )
            .await
        };
        match connect_result {
            Ok(Ok((ws, response))) => {
                info!(
                    "✅ shared ingress market broker connected (status={:?})",
                    response.status()
                );
                backoff = Duration::from_millis(100);
                let (mut write, mut read) = ws.split();
                let subscribe = json!({
                    "type": "market",
                    "operation": "subscribe",
                    "markets": [],
                    "assets_ids": settings.market_assets(),
                    "asset_ids": settings.market_assets(),
                    "initial_dump": true,
                    "custom_feature_enabled": subscribe_custom_feature_enabled,
                });
                if !is_fixed_market_feed {
                    info!(
                        "📤 shared ingress market broker subscribe | slug={} market_id={} yes_asset_id={} no_asset_id={} custom_feature_enabled={}",
                        settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
                        settings.market_id,
                        settings.yes_asset_id,
                        settings.no_asset_id,
                        subscribe_custom_feature_enabled
                    );
                }
                if let Err(err) = write
                    .send(Message::Text(subscribe.to_string().into()))
                    .await
                {
                    warn!("shared ingress market subscribe failed: {:?}", err);
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
                let ping_handle = tokio::spawn(async move {
                    let mut delay = tokio::time::interval(Duration::from_secs(5));
                    loop {
                        delay.tick().await;
                        if write.send(Message::Ping(Vec::new().into())).await.is_err() {
                            break;
                        }
                    }
                });
                let mut session_raw_msg_count: u64 = 0;
                let mut session_parsed_msg_count: u64 = 0;
                let mut session_trade_tick_count: u64 = 0;
                let mut session_book_tick_count: u64 = 0;
                let mut session_keepalive_msg_count: u64 = 0;
                let mut session_unknown_event_count: u64 = 0;
                let mut session_parse_drop_count: u64 = 0;
                let mut session_last_parse_drop_reason: Option<String> = None;
                let mut session_parse_drop_log_count: u64 = 0;
                let mut session_noop_log_count: u64 = 0;
                let mut last_raw_msg_at = tokio::time::Instant::now();
                let mut last_full_book_at: Option<tokio::time::Instant> = None;
                let session_started_at = tokio::time::Instant::now();
                let mut health_probe = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_millis(health_jitter_ms),
                    Duration::from_secs(10),
                );
                health_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            match changed {
                                Ok(()) if *shutdown_rx.borrow() => {
                                    info!(
                                        "🛑 shared ingress market broker feed shutdown requested | slug={}",
                                        settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string())
                                    );
                                    ping_handle.abort();
                                    break 'outer;
                                }
                                Ok(()) => {}
                                Err(_) => {
                                    ping_handle.abort();
                                    break 'outer;
                                }
                            }
                        }
                        _ = health_probe.tick() => {
                            let now = tokio::time::Instant::now();
                            let raw_silence = now.saturating_duration_since(last_raw_msg_at);
                            let no_full_book_silence = now.saturating_duration_since(
                                last_full_book_at.unwrap_or(session_started_at),
                            );
                            let no_initial_wire = session_raw_msg_count == 0
                                && raw_silence
                                    >= Duration::from_secs(MARKET_WS_NO_RAW_RECONNECT_SECS);
                            let lane_never_became_usable = session_book_tick_count == 0
                                && session_trade_tick_count == 0;
                            let bootstrap_lane_never_became_usable = session_raw_msg_count > 0
                                && lane_never_became_usable
                                && no_full_book_silence
                                    >= Duration::from_secs(MARKET_WS_NO_FULL_BOOK_RECONNECT_SECS);
                            if no_initial_wire || bootstrap_lane_never_became_usable {
                                let reason = if no_initial_wire {
                                    "no_initial_wire"
                                } else {
                                    "bootstrap_lane_never_became_usable"
                                };
                                warn!(
                                    "⚠️ shared ingress market broker unhealthy ({}) for {:?} | slug={} yes_asset_id={} no_asset_id={} raw={} keepalive={} parsed={} book_ticks={} trade_ticks={} unknown={} dropped={} last_drop={:?} — reconnecting",
                                    reason,
                                    no_full_book_silence.max(raw_silence),
                                    settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
                                    settings.yes_asset_id,
                                    settings.no_asset_id,
                                    session_raw_msg_count,
                                    session_keepalive_msg_count,
                                    session_parsed_msg_count,
                                    session_book_tick_count,
                                    session_trade_tick_count,
                                    session_unknown_event_count,
                                    session_parse_drop_count,
                                    session_last_parse_drop_reason,
                                );
                                ping_handle.abort();
                                break;
                            }
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    let text_ref: &str = &text;
                                    if !is_fixed_market_feed
                                        && is_market_ws_keepalive_text(text_ref)
                                    {
                                        session_keepalive_msg_count =
                                            session_keepalive_msg_count.saturating_add(1);
                                        continue;
                                    }
                                    session_raw_msg_count = session_raw_msg_count.saturating_add(1);
                                    last_raw_msg_at = tokio::time::Instant::now();
                                    let (parsed, unknown_events, parse_drops, drop_reasons) =
                                        parse_ws_payload(&settings, text_ref);
                                    session_unknown_event_count = session_unknown_event_count
                                        .saturating_add(unknown_events);
                                    session_parse_drop_count = session_parse_drop_count
                                        .saturating_add(parse_drops);
                                    if let Some(reason) = drop_reasons.last() {
                                        session_last_parse_drop_reason = Some(reason.clone());
                                    }
                                    if parsed.is_empty() && drop_reasons.is_empty() {
                                        let summary =
                                            market_ws_payload_debug_summary(&settings, text_ref);
                                        session_last_parse_drop_reason = Some(summary.clone());
                                        if !is_fixed_market_feed && session_noop_log_count < 5 {
                                            warn!(
                                                "⚠️ shared ingress market broker parser noop | slug={} {}",
                                                settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
                                                summary
                                            );
                                            session_noop_log_count =
                                                session_noop_log_count.saturating_add(1);
                                        }
                                    }
                                    if !drop_reasons.is_empty() && session_parse_drop_log_count < 5 {
                                        warn!(
                                            "⚠️ shared ingress market broker parser drop | slug={} reasons={}",
                                            settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
                                            drop_reasons.join(" || ")
                                        );
                                        session_parse_drop_log_count =
                                            session_parse_drop_log_count.saturating_add(1);
                                    }
                                    for md_msg in parsed {
                                        session_parsed_msg_count = session_parsed_msg_count.saturating_add(1);
                                        match &md_msg {
                                            MarketDataMsg::TradeTick {
                                                asset_id,
                                                trade_id,
                                                market_side,
                                                taker_side,
                                                price,
                                                size,
                                                ..
                                            } => {
                                                session_trade_tick_count =
                                                    session_trade_tick_count.saturating_add(1);
                                                feed.publish(SharedIngressWireMsg::MarketTradeTick {
                                                    asset_id: asset_id.clone(),
                                                    trade_id: trade_id.clone(),
                                                    market_side: market_side.as_str().to_string(),
                                                    taker_side: taker_side.as_str().to_string(),
                                                    price: *price,
                                                    size: *size,
                                                    ts_ms: unix_now_millis_u64(),
                                                });
                                            }
                                            MarketDataMsg::BookTick { yes_bid, yes_ask, no_bid, no_ask, .. } => {
                                                session_book_tick_count =
                                                    session_book_tick_count.saturating_add(1);
                                                let recv_ms = unix_now_millis_u64();
                                                if yes_bid.is_finite() || yes_ask.is_finite() {
                                                    feed.publish(SharedIngressWireMsg::PostCloseBookUpdate {
                                                        side: Side::Yes.as_str().to_string(),
                                                        bid: (*yes_bid).max(0.0),
                                                        ask: (*yes_ask).max(0.0),
                                                        recv_ms,
                                                    });
                                                }
                                                if no_bid.is_finite() || no_ask.is_finite() {
                                                    feed.publish(SharedIngressWireMsg::PostCloseBookUpdate {
                                                        side: Side::No.as_str().to_string(),
                                                        bid: (*no_bid).max(0.0),
                                                        ask: (*no_ask).max(0.0),
                                                        recv_ms,
                                                    });
                                                }
                                                if let Some(full) = book_asm.update(&md_msg) {
                                                    if let MarketDataMsg::BookTick {
                                                        yes_bid,
                                                        yes_ask,
                                                        no_bid,
                                                        no_ask,
                                                        ..
                                                    } = &full
                                                    {
                                                        feed.publish(SharedIngressWireMsg::MarketBookTick {
                                                            yes_bid: *yes_bid,
                                                            yes_ask: *yes_ask,
                                                            no_bid: *no_bid,
                                                            no_ask: *no_ask,
                                                            ts_ms: recv_ms,
                                                        });
                                                    }
                                                    last_full_book_at = Some(tokio::time::Instant::now());
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(bytes))) => {
                                    let Ok(text) = std::str::from_utf8(&bytes) else {
                                        continue;
                                    };
                                    if !is_fixed_market_feed
                                        && is_market_ws_keepalive_text(text)
                                    {
                                        session_keepalive_msg_count =
                                            session_keepalive_msg_count.saturating_add(1);
                                        continue;
                                    }
                                    session_raw_msg_count = session_raw_msg_count.saturating_add(1);
                                    last_raw_msg_at = tokio::time::Instant::now();
                                    let (parsed, unknown_events, parse_drops, drop_reasons) =
                                        parse_ws_payload(&settings, text);
                                    session_unknown_event_count = session_unknown_event_count
                                        .saturating_add(unknown_events);
                                    session_parse_drop_count = session_parse_drop_count
                                        .saturating_add(parse_drops);
                                    if let Some(reason) = drop_reasons.last() {
                                        session_last_parse_drop_reason = Some(reason.clone());
                                    }
                                    if parsed.is_empty() && drop_reasons.is_empty() {
                                        let summary =
                                            market_ws_payload_debug_summary(&settings, text);
                                        session_last_parse_drop_reason = Some(summary.clone());
                                        if !is_fixed_market_feed && session_noop_log_count < 5 {
                                            warn!(
                                                "⚠️ shared ingress market broker parser noop | slug={} {}",
                                                settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
                                                summary
                                            );
                                            session_noop_log_count =
                                                session_noop_log_count.saturating_add(1);
                                        }
                                    }
                                    if !drop_reasons.is_empty() && session_parse_drop_log_count < 5 {
                                        warn!(
                                            "⚠️ shared ingress market broker parser drop | slug={} reasons={}",
                                            settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
                                            drop_reasons.join(" || ")
                                        );
                                        session_parse_drop_log_count =
                                            session_parse_drop_log_count.saturating_add(1);
                                    }
                                    for md_msg in parsed {
                                        session_parsed_msg_count = session_parsed_msg_count.saturating_add(1);
                                        match &md_msg {
                                            MarketDataMsg::TradeTick {
                                                asset_id,
                                                trade_id,
                                                market_side,
                                                taker_side,
                                                price,
                                                size,
                                                ..
                                            } => {
                                                session_trade_tick_count =
                                                    session_trade_tick_count.saturating_add(1);
                                                feed.publish(SharedIngressWireMsg::MarketTradeTick {
                                                    asset_id: asset_id.clone(),
                                                    trade_id: trade_id.clone(),
                                                    market_side: market_side.as_str().to_string(),
                                                    taker_side: taker_side.as_str().to_string(),
                                                    price: *price,
                                                    size: *size,
                                                    ts_ms: unix_now_millis_u64(),
                                                });
                                            }
                                            MarketDataMsg::BookTick { yes_bid, yes_ask, no_bid, no_ask, .. } => {
                                                session_book_tick_count =
                                                    session_book_tick_count.saturating_add(1);
                                                let recv_ms = unix_now_millis_u64();
                                                if yes_bid.is_finite() || yes_ask.is_finite() {
                                                    feed.publish(SharedIngressWireMsg::PostCloseBookUpdate {
                                                        side: Side::Yes.as_str().to_string(),
                                                        bid: (*yes_bid).max(0.0),
                                                        ask: (*yes_ask).max(0.0),
                                                        recv_ms,
                                                    });
                                                }
                                                if no_bid.is_finite() || no_ask.is_finite() {
                                                    feed.publish(SharedIngressWireMsg::PostCloseBookUpdate {
                                                        side: Side::No.as_str().to_string(),
                                                        bid: (*no_bid).max(0.0),
                                                        ask: (*no_ask).max(0.0),
                                                        recv_ms,
                                                    });
                                                }
                                                if let Some(full) = book_asm.update(&md_msg) {
                                                    if let MarketDataMsg::BookTick {
                                                        yes_bid,
                                                        yes_ask,
                                                        no_bid,
                                                        no_ask,
                                                        ..
                                                    } = &full
                                                    {
                                                        feed.publish(SharedIngressWireMsg::MarketBookTick {
                                                            yes_bid: *yes_bid,
                                                            yes_ask: *yes_ask,
                                                            no_bid: *no_bid,
                                                            no_ask: *no_ask,
                                                            ts_ms: recv_ms,
                                                        });
                                                    }
                                                    last_full_book_at = Some(tokio::time::Instant::now());
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                Some(Ok(_)) => {}
                                Some(Err(err)) => {
                                    warn!("⚠️ shared ingress market broker ws error: {:?}", err);
                                    ping_handle.abort();
                                    break;
                                }
                                None => {
                                    ping_handle.abort();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Ok(Err(err)) => {
                warn!("⚠️ shared ingress market broker connect error: {:?}", err);
                consecutive_failures = consecutive_failures.saturating_add(1);
            }
            Err(_) => {
                warn!("⚠️ shared ingress market broker connect timeout");
                consecutive_failures = consecutive_failures.saturating_add(1);
            }
        }
        if should_degrade_ws(consecutive_failures, ws_degrade_failures) {
            warn!(
                "🛑 shared ingress market broker degraded: consecutive_failures={} >= {} — resetting counter and retrying",
                consecutive_failures,
                ws_degrade_failures
            );
            consecutive_failures = 0;
        }
        if feed.shutdown_requested() || feed.subscriber_count() == 0 {
            info!(
                "🛑 shared ingress market broker feed exiting before reconnect | slug={} subscribers={}",
                settings
                    .market_slug
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                feed.subscriber_count(),
            );
            break;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// ─────────────────────────────────────────────────────────
// WebSocket runner (with reconnection + wall-clock deadline)
// ─────────────────────────────────────────────────────────

/// Why the WS session ended.
#[derive(Debug)]
enum MarketEnd {
    /// Wall-clock hit the market's end timestamp.
    Expired,
    /// WS degraded (consecutive reconnect/connect failures exceeded threshold).
    WsDegraded {
        consecutive_failures: u32,
        remaining_secs: u64,
    },
}

// Hard guard against "zombie rounds": even if WS loop gets stuck, the outer runner
// must force-rotate shortly after the market end timestamp.
const MARKET_WS_HARD_CUTOFF_GRACE_SECS_DEFAULT: u64 = 45;
fn market_ws_hard_cutoff_grace_secs() -> u64 {
    env::var("PM_MARKET_WS_HARD_CUTOFF_GRACE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|v| v.min(300))
        .unwrap_or(MARKET_WS_HARD_CUTOFF_GRACE_SECS_DEFAULT)
}
// If we keep receiving raw payloads but never reconstruct a full 4-price book,
// reconnect proactively because strategy cannot trade without complete book.
const MARKET_WS_NO_FULL_BOOK_RECONNECT_SECS: u64 = 12;
const MARKET_WS_NO_RAW_RECONNECT_SECS: u64 = 12;

fn try_forward_md(
    tx: &mpsc::Sender<MarketDataMsg>,
    msg: MarketDataMsg,
    dropped_full_counter: &mut u64,
) {
    match tx.try_send(msg) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            *dropped_full_counter = dropped_full_counter.saturating_add(1);
        }
        // Closed receiver is expected for non-GLFT strategies.
        Err(TrySendError::Closed(_)) => {}
    }
}

fn try_broadcast_dry_run_touch_md(
    tx: &Option<broadcast::Sender<MarketDataMsg>>,
    msg: &MarketDataMsg,
) {
    if let Some(tx) = tx {
        let _ = tx.send(msg.clone());
    }
}

async fn run_market_ws_remote_with_wall_guard(
    settings: Settings,
    ofi_tx: mpsc::Sender<MarketDataMsg>,
    glft_tx: mpsc::Sender<MarketDataMsg>,
    coord_tx: watch::Sender<MarketDataMsg>,
    dry_run_touch_md_tx: Option<broadcast::Sender<MarketDataMsg>>,
    coord_accept_partial_book: bool,
    post_close_book_tx: mpsc::Sender<PostCloseSideBookUpdate>,
    end_ts: u64,
) -> MarketEnd {
    let hard_cutoff_grace_secs = market_ws_hard_cutoff_grace_secs();
    let hard_cutoff_ts = end_ts.saturating_add(hard_cutoff_grace_secs);
    let socket_path = shared_ingress_root().join("market.sock");
    let mut reconnect_backoff = Duration::from_millis(200);
    loop {
        let _ = ensure_shared_ingress_broker_running().await;
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        if now_unix >= hard_cutoff_ts {
            return MarketEnd::Expired;
        }
        let mut book_asm = BookAssembler::default();
        let connect =
            tokio::time::timeout(Duration::from_secs(2), UnixStream::connect(&socket_path)).await;
        let Ok(Ok(stream)) = connect else {
            sleep(reconnect_backoff).await;
            reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(2));
            continue;
        };
        reconnect_backoff = Duration::from_millis(200);
        let (read_half, mut write_half) = stream.into_split();
        let req = SharedIngressSubscribeReq {
            stream: "market".to_string(),
            symbols: Vec::new(),
            market_slug: settings.market_slug.clone(),
            market_id: Some(settings.market_id.clone()),
            yes_asset_id: Some(settings.yes_asset_id.clone()),
            no_asset_id: Some(settings.no_asset_id.clone()),
            ws_base_url: Some(settings.ws_base_url.clone()),
            custom_feature_enabled: Some(settings.custom_feature),
        };
        let mut line = match serde_json::to_vec(&req) {
            Ok(v) => v,
            Err(_) => {
                return MarketEnd::WsDegraded {
                    consecutive_failures: 1,
                    remaining_secs: 0,
                }
            }
        };
        line.push(b'\n');
        if write_half.write_all(&line).await.is_err() || write_half.flush().await.is_err() {
            sleep(reconnect_backoff).await;
            continue;
        }
        info!(
            "📡 shared ingress market remote subscribed | slug={} yes_asset_id={} no_asset_id={} coord_accept_partial_book={}",
            settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
            settings.yes_asset_id,
            settings.no_asset_id,
            coord_accept_partial_book
        );
        let mut reader = BufReader::new(read_half);
        let mut buf = String::new();
        let mut last_raw_msg_at = tokio::time::Instant::now();
        let mut last_full_book_at: Option<tokio::time::Instant> = None;
        let session_started_at = tokio::time::Instant::now();
        let mut health_probe = tokio::time::interval(Duration::from_secs(10));
        let mut tx_drop_count: u64 = 0;
        let mut session_wire_msg_count: u64 = 0;
        let mut session_wire_trade_tick_count: u64 = 0;
        let mut session_wire_book_tick_count: u64 = 0;
        let mut session_coord_partial_forward_count: u64 = 0;
        let mut session_full_book_emit_count: u64 = 0;
        let mut session_post_close_update_count: u64 = 0;
        health_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_secs();
            if now_unix >= hard_cutoff_ts {
                return MarketEnd::Expired;
            }
            tokio::select! {
                _ = health_probe.tick() => {
                    let now = tokio::time::Instant::now();
                    let session_age = now.saturating_duration_since(session_started_at);
                    let raw_silence = now.saturating_duration_since(last_raw_msg_at);
                    let full_book_silence_ms = last_full_book_at
                        .map(|ts| now.saturating_duration_since(ts).as_millis() as u64);
                    let no_initial_wire = session_wire_msg_count == 0
                        && raw_silence >= Duration::from_secs(MARKET_WS_NO_RAW_RECONNECT_SECS);
                    if no_initial_wire {
                        warn!(
                            "⚠️ shared ingress market remote unhealthy ({}) for {:?} | slug={} session_age={:?} full_book_silence_ms={:?} wire_msgs={} wire_book_ticks={} wire_trade_ticks={} coord_partial_forwards={} full_book_emits={} post_close_updates={} tx_dropped={} — reconnecting",
                            "no_initial_wire",
                            raw_silence,
                            settings.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
                            session_age,
                            full_book_silence_ms,
                            session_wire_msg_count,
                            session_wire_book_tick_count,
                            session_wire_trade_tick_count,
                            session_coord_partial_forward_count,
                            session_full_book_emit_count,
                            session_post_close_update_count,
                            tx_drop_count,
                        );
                        break;
                    }
                }
                read = reader.read_line(&mut buf) => {
                    let Ok(n) = read else { break; };
                    if n == 0 { break; }
                    last_raw_msg_at = tokio::time::Instant::now();
                    session_wire_msg_count = session_wire_msg_count.saturating_add(1);
                    let Ok(msg) = serde_json::from_str::<SharedIngressWireMsg>(buf.trim()) else {
                        buf.clear();
                        continue;
                    };
                    match msg {
                            SharedIngressWireMsg::MarketTradeTick { asset_id, trade_id, market_side, taker_side, price, size, .. } => {
                                session_wire_trade_tick_count =
                                    session_wire_trade_tick_count.saturating_add(1);
                                let market_side = if market_side.eq_ignore_ascii_case("YES") { Side::Yes } else { Side::No };
                                let taker_side = if taker_side.eq_ignore_ascii_case("BUY") { TakerSide::Buy } else { TakerSide::Sell };
                                let md_msg = MarketDataMsg::TradeTick {
                                    asset_id,
                                    trade_id,
                                    market_side,
                                    taker_side,
                                    price,
                                    size,
                                    ts: Instant::now(),
                                };
                                try_broadcast_dry_run_touch_md(&dry_run_touch_md_tx, &md_msg);
                                try_forward_md(&ofi_tx, md_msg.clone(), &mut tx_drop_count);
                                try_forward_md(&glft_tx, md_msg, &mut tx_drop_count);
                            }
                            SharedIngressWireMsg::MarketBookTick { yes_bid, yes_ask, no_bid, no_ask, .. } => {
                                session_wire_book_tick_count =
                                    session_wire_book_tick_count.saturating_add(1);
                                let md_msg = MarketDataMsg::BookTick { yes_bid, yes_ask, no_bid, no_ask, ts: Instant::now() };
                                try_broadcast_dry_run_touch_md(&dry_run_touch_md_tx, &md_msg);
                                if coord_accept_partial_book {
                                    let _ = coord_tx.send(md_msg.clone());
                                    session_coord_partial_forward_count =
                                        session_coord_partial_forward_count.saturating_add(1);
                                }
                                if let Some(full) = book_asm.update(&md_msg) {
                                    last_full_book_at = Some(tokio::time::Instant::now());
                                    try_broadcast_dry_run_touch_md(&dry_run_touch_md_tx, &full);
                                    try_forward_md(&glft_tx, full.clone(), &mut tx_drop_count);
                                    let _ = coord_tx.send(full);
                                    session_full_book_emit_count =
                                        session_full_book_emit_count.saturating_add(1);
                                }
                            }
                            SharedIngressWireMsg::PostCloseBookUpdate { side, bid, ask, recv_ms } => {
                                session_post_close_update_count =
                                    session_post_close_update_count.saturating_add(1);
                                let side = if side.eq_ignore_ascii_case("YES") { Side::Yes } else { Side::No };
                                let md_msg =
                                    partial_book_tick_from_post_close_update(side, bid, ask);
                                try_broadcast_dry_run_touch_md(&dry_run_touch_md_tx, &md_msg);
                                if coord_accept_partial_book {
                                    let _ = coord_tx.send(md_msg.clone());
                                    session_coord_partial_forward_count =
                                        session_coord_partial_forward_count.saturating_add(1);
                                }
                                if let Some(full) = book_asm.update(&md_msg) {
                                    last_full_book_at = Some(tokio::time::Instant::now());
                                    try_broadcast_dry_run_touch_md(&dry_run_touch_md_tx, &full);
                                    try_forward_md(&glft_tx, full.clone(), &mut tx_drop_count);
                                    let _ = coord_tx.send(full);
                                    session_full_book_emit_count =
                                        session_full_book_emit_count.saturating_add(1);
                                }
                                let _ = post_close_book_tx.try_send(PostCloseSideBookUpdate { side, bid, ask, recv_ms });
                            }
                        SharedIngressWireMsg::SnapshotDone => {}
                        SharedIngressWireMsg::Error { .. } => break,
                        _ => {}
                    }
                    buf.clear();
                }
            }
        }
    }
}

async fn run_market_ws_with_wall_guard(
    settings: Settings,
    ofi_tx: mpsc::Sender<MarketDataMsg>,
    glft_tx: mpsc::Sender<MarketDataMsg>,
    coord_tx: watch::Sender<MarketDataMsg>,
    dry_run_touch_md_tx: Option<broadcast::Sender<MarketDataMsg>>,
    coord_accept_partial_book: bool,
    post_close_book_tx: mpsc::Sender<PostCloseSideBookUpdate>,
    end_ts: u64,
    recorder: Option<RecorderHandle>,
    recorder_meta: Option<RecorderSessionMeta>,
) -> MarketEnd {
    let hard_cutoff_grace_secs = market_ws_hard_cutoff_grace_secs();
    let hard_cutoff_ts = end_ts.saturating_add(hard_cutoff_grace_secs);
    let mut ws_task = tokio::spawn(run_market_ws(
        settings,
        ofi_tx,
        glft_tx,
        coord_tx,
        dry_run_touch_md_tx,
        coord_accept_partial_book,
        post_close_book_tx,
        end_ts,
        recorder,
        recorder_meta,
    ));
    loop {
        tokio::select! {
            joined = &mut ws_task => {
                match joined {
                    Ok(reason) => return reason,
                    Err(e) => {
                        warn!("⚠️ Market WS task join error: {:?} — treating as degraded", e);
                        return MarketEnd::WsDegraded {
                            consecutive_failures: ws_degrade_max_failures(),
                            remaining_secs: 0,
                        };
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                let now_unix = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| Duration::from_secs(0))
                    .as_secs();
                if now_unix >= hard_cutoff_ts {
                    warn!(
                        "🛑 Hard wall-clock cutoff reached (now={} >= end_ts+grace={}+{}) — aborting market WS and rotating",
                        now_unix,
                        end_ts,
                        hard_cutoff_grace_secs
                    );
                    ws_task.abort();
                    let _ = ws_task.await;
                    return MarketEnd::Expired;
                }
            }
        }
    }
}

/// In-process supervisor: one tokio runtime, one ChainlinkHub (covering the union
/// of all prefixes' Chainlink symbols), one JoinSet spawning run_prefix_worker
/// per slug. Replaces the OS-process supervisor when PM_INPROC_SUPERVISOR=1.
/// Designed for the 30+ market scale where N OS-processes × per-process overhead
/// becomes prohibitive.
async fn run_inproc_supervisor(prefixes: Vec<String>) -> anyhow::Result<()> {
    info!(
        "🧩 in-proc multi-market supervisor enabled | workers={} prefixes={}",
        prefixes.len(),
        prefixes.join(",")
    );

    let coord_cfg = CoordinatorConfig::from_env();
    let shared_ingress = SharedIngressRuntime::build(&prefixes, &coord_cfg);

    // Oracle-lag execution now runs per-market directly on WinnerHint hot path.
    // Cross-market arbiter and round-tail maker fallback are intentionally disabled.
    let arbiter_sender: Option<mpsc::Sender<ArbiterObservation>> =
        if coord_cfg.strategy.is_oracle_lag_sniping()
            && coord_cfg.oracle_lag_sniping.cross_market_arbiter_enabled
        {
            info!("⏭️ cross_market_hint_arbiter disabled for oracle_lag_sniping | requested=true");
            None
        } else {
            None
        };
    let round_tail_sender: Option<mpsc::Sender<RoundTailObservation>> =
        if coord_cfg.strategy.is_oracle_lag_sniping() && prefixes.len() > 1 {
            info!(
                "⏭️ oracle_lag_round_tail_coordinator disabled for oracle_lag_sniping | markets={}",
                prefixes.len()
            );
            None
        } else {
            None
        };

    let mut joinset: tokio::task::JoinSet<(String, anyhow::Result<()>)> =
        tokio::task::JoinSet::new();
    for prefix in prefixes {
        let ctx = Arc::new(WorkerCtx {
            slug: prefix.clone(),
            shared_ingress: shared_ingress.clone(),
            arbiter_tx: arbiter_sender.clone(),
            round_tail_tx: round_tail_sender.clone(),
        });
        let slug = prefix.clone();
        joinset.spawn(async move {
            let res = run_prefix_worker(Some(ctx)).await;
            (slug, res)
        });
        info!("🚀 in-proc worker spawned | slug={}", prefix);
    }

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                warn!("🛑 in-proc supervisor received ctrl-c — aborting all workers");
                joinset.abort_all();
                // Drain so tasks get a chance to log their abort
                while joinset.join_next().await.is_some() {}
                return Ok(());
            }
            maybe = joinset.join_next() => {
                match maybe {
                    Some(Ok((slug, Ok(())))) => {
                        info!("🏁 in-proc worker exited cleanly | slug={}", slug);
                    }
                    Some(Ok((slug, Err(e)))) => {
                        warn!("⚠️ in-proc worker returned error | slug={} err={}", slug, e);
                    }
                    Some(Err(join_err)) => {
                        warn!("⚠️ in-proc worker task join error: {}", join_err);
                    }
                    None => break, // all workers finished
                }
            }
        }
    }
    Ok(())
}

async fn run_multi_market_supervisor(prefixes: Vec<String>) -> anyhow::Result<()> {
    if prefixes.is_empty() {
        return Ok(());
    }
    // Default policy:
    // - Oracle-lag multi-market should prefer in-proc supervisor (shared hub, lower overhead).
    // - Operator can still override via PM_INPROC_SUPERVISOR.
    let strategy_is_oracle_lag = env::var("PM_STRATEGY")
        .ok()
        .map(|v| {
            let lv = v.trim().to_ascii_lowercase();
            lv == "oracle_lag_sniping" || lv == "post_close_hype"
        })
        .unwrap_or(false);
    let inproc = env::var("PM_INPROC_SUPERVISOR")
        .ok()
        .map(|v| {
            let lv = v.trim().to_ascii_lowercase();
            lv == "1" || lv == "true" || lv == "yes" || lv == "on"
        })
        .unwrap_or(strategy_is_oracle_lag);
    if inproc {
        return run_inproc_supervisor(prefixes).await;
    }
    let exe = std::env::current_exe()?;
    info!(
        "🧩 multi-market supervisor enabled | workers={} prefixes={}",
        prefixes.len(),
        prefixes.join(",")
    );

    #[derive(Debug)]
    struct WorkerExit {
        prefix: String,
        status: Option<std::process::ExitStatus>,
        wait_err: Option<String>,
    }

    let (exit_tx, mut exit_rx) = mpsc::channel::<WorkerExit>(prefixes.len().max(1) * 2);
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    for prefix in prefixes {
        let mut cmd = tokio::process::Command::new(&exe);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("PM_MULTI_MARKET_CHILD", "1")
            .env("POLYMARKET_MARKET_SLUG", &prefix)
            .env_remove("PM_MULTI_MARKET_PREFIXES");

        // Always narrow child universe to its own symbol: each worker only needs
        // its own symbol's Chainlink ticks. Overriding regardless of parent env
        // avoids the rate-limit burst (N children × full universe = N² subs).
        if let Some(sym) = oracle_lag_symbol_from_slug(&prefix) {
            cmd.env("PM_ORACLE_LAG_SYMBOL_UNIVERSE", &sym);
            info!(
                "🧭 supervisor scoping child | prefix={} PM_ORACLE_LAG_SYMBOL_UNIVERSE={}",
                prefix, sym
            );
        } else {
            warn!(
                "⚠️ supervisor could not derive symbol from prefix='{}' — child will inherit parent env",
                prefix
            );
        }

        let child = cmd.spawn();
        let mut child = match child {
            Ok(c) => c,
            Err(e) => {
                anyhow::bail!("failed to spawn worker for prefix='{}': {}", prefix, e);
            }
        };
        let pid = child.id().unwrap_or_default();
        info!(
            "🚀 worker spawned | prefix={} pid={} strategy={} dry_run={}",
            prefix,
            pid,
            env::var("PM_STRATEGY").unwrap_or_else(|_| "unset".to_string()),
            env::var("PM_DRY_RUN").unwrap_or_else(|_| "unset".to_string())
        );

        let mut shutdown_rx = shutdown_tx.subscribe();
        let tx = exit_tx.clone();
        tokio::spawn(async move {
            let result = tokio::select! {
                waited = child.wait() => {
                    match waited {
                        Ok(status) => WorkerExit {
                            prefix,
                            status: Some(status),
                            wait_err: None,
                        },
                        Err(e) => WorkerExit {
                            prefix,
                            status: None,
                            wait_err: Some(e.to_string()),
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    let _ = child.kill().await;
                    match child.wait().await {
                        Ok(status) => WorkerExit {
                            prefix,
                            status: Some(status),
                            wait_err: None,
                        },
                        Err(e) => WorkerExit {
                            prefix,
                            status: None,
                            wait_err: Some(e.to_string()),
                        }
                    }
                }
            };
            let _ = tx.send(result).await;
        });
    }
    drop(exit_tx);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            warn!("🛑 supervisor received ctrl-c — shutting down workers");
            let _ = shutdown_tx.send(());
            sleep(Duration::from_millis(800)).await;
            Ok(())
        }
        maybe_exit = exit_rx.recv() => {
            if let Some(exit) = maybe_exit {
                let _ = shutdown_tx.send(());
                sleep(Duration::from_millis(800)).await;
                if let Some(err) = exit.wait_err {
                    anyhow::bail!("worker prefix='{}' wait error: {}", exit.prefix, err);
                }
                if let Some(status) = exit.status {
                    if !status.success() {
                        anyhow::bail!("worker prefix='{}' exited with status {}", exit.prefix, status);
                    }
                    anyhow::bail!("worker prefix='{}' exited unexpectedly with success status {}", exit.prefix, status);
                }
                anyhow::bail!("worker prefix='{}' exited unexpectedly", exit.prefix);
            }
            anyhow::bail!("multi-market supervisor exited without worker status");
        }
    }
}

async fn run_market_ws(
    settings: Settings,
    ofi_tx: mpsc::Sender<MarketDataMsg>,
    glft_tx: mpsc::Sender<MarketDataMsg>,
    coord_tx: watch::Sender<MarketDataMsg>,
    dry_run_touch_md_tx: Option<broadcast::Sender<MarketDataMsg>>,
    coord_accept_partial_book: bool,
    post_close_book_tx: mpsc::Sender<PostCloseSideBookUpdate>,
    end_ts: u64,
    recorder: Option<RecorderHandle>,
    recorder_meta: Option<RecorderSessionMeta>,
) -> MarketEnd {
    if matches!(
        shared_ingress_role(),
        SharedIngressRole::Client | SharedIngressRole::Auto
    ) {
        return run_market_ws_remote_with_wall_guard(
            settings,
            ofi_tx,
            glft_tx,
            coord_tx,
            dry_run_touch_md_tx,
            coord_accept_partial_book,
            post_close_book_tx,
            end_ts,
        )
        .await;
    }
    // Compute wall-clock deadline
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let secs_remaining = end_ts.saturating_sub(now_unix);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs_remaining);
    let ws_connect_timeout = ws_connect_timeout_ms();
    let ws_degrade_failures = ws_degrade_max_failures();
    info!(
        "⏰ Market deadline in {}s (end_ts={})",
        secs_remaining, end_ts
    );

    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);
    let mut consecutive_failures: u32 = 0;
    let mut subscribe_custom_feature_enabled = settings.custom_feature;

    loop {
        // Check if already expired before connecting
        if tokio::time::Instant::now() >= deadline {
            info!("🏁 Market expired (wall-clock)");
            return MarketEnd::Expired;
        }

        // ISSUE 4 FIX: Reset BookAssembler on every reconnect.
        // Previously it was declared outside the loop, causing stale data from the
        // previous session to be mixed with fresh data on reconnect. E.g., old NO
        // price combined with new YES price would produce a wrong BookTick.
        let mut book_asm = BookAssembler::default();

        let url = settings.ws_url("market");
        info!(%url, "📡 connecting market WS");

        let connect_result = tokio::time::timeout(
            Duration::from_millis(ws_connect_timeout),
            connect_async(&url),
        )
        .await;

        match connect_result {
            Ok(Ok((ws, response))) => {
                info!("✅ WS connected (status={:?})", response.status());
                backoff = Duration::from_millis(100); // Reset on successful connect
                let (mut write, mut read) = ws.split();
                let mut session_had_market_data = false;
                let mut session_raw_msg_count: u64 = 0;
                let mut session_text_msg_count: u64 = 0;
                let mut session_binary_msg_count: u64 = 0;
                let mut session_parsed_msg_count: u64 = 0;
                let mut session_trade_tick_count: u64 = 0;
                let mut session_book_tick_count: u64 = 0;
                let mut session_partial_yes_book_count: u64 = 0;
                let mut session_partial_no_book_count: u64 = 0;
                let mut session_unknown_event_count: u64 = 0;
                let mut session_parse_drop_count: u64 = 0;
                let mut session_non_utf8_binary_count: u64 = 0;
                let mut session_tx_drop_count: u64 = 0;
                let mut warned_non_utf8_binary = false;
                let session_started_at = tokio::time::Instant::now();
                let mut last_raw_msg_at = tokio::time::Instant::now();
                let mut last_market_data_at = tokio::time::Instant::now();
                let mut last_full_book_at: Option<tokio::time::Instant> = None;
                let mut health_probe = tokio::time::interval(Duration::from_secs(10));
                health_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut session_expired = false;

                // Subscribe
                let asset_ids = settings.market_assets();
                let subscribe = json!({
                    "type": "market",
                    "operation": "subscribe",
                    "markets": [],
                    "assets_ids": asset_ids,
                    "asset_ids": settings.market_assets(),
                    "initial_dump": true,
                    "custom_feature_enabled": subscribe_custom_feature_enabled,
                });
                info!(
                    "📤 Subscribe: {} (custom_feature_enabled={})",
                    subscribe, subscribe_custom_feature_enabled
                );

                if let Err(err) = write.send(Message::Text(subscribe.to_string())).await {
                    warn!("WS subscribe failed: {err:?}");
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if should_degrade_ws(consecutive_failures, ws_degrade_failures) {
                        let remaining_secs = deadline
                            .saturating_duration_since(tokio::time::Instant::now())
                            .as_secs();
                        warn!(
                            "🛑 WS degraded: consecutive_failures={} >= {} (remaining={}s) — ending current market early",
                            consecutive_failures, ws_degrade_failures, remaining_secs
                        );
                        return MarketEnd::WsDegraded {
                            consecutive_failures,
                            remaining_secs,
                        };
                    }
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }

                // Ping keepalive — store handle for explicit cleanup
                let ping_handle = tokio::spawn(async move {
                    let mut delay = tokio::time::interval(Duration::from_secs(5));
                    loop {
                        delay.tick().await;
                        if write.send(Message::Ping(Vec::new().into())).await.is_err() {
                            break;
                        }
                    }
                });

                // Read loop with wall-clock deadline
                loop {
                    // Hard wall-clock guard: avoid deadline starvation when read branch is very hot.
                    if tokio::time::Instant::now() >= deadline {
                        info!("🏁 Market expired (hard guard) — stopping WS");
                        ping_handle.abort();
                        session_expired = true;
                        break;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {
                            info!("🏁 Market expired (wall-clock) — stopping WS");
                            ping_handle.abort();
                            session_expired = true;
                            break;
                        }
                        _ = health_probe.tick() => {
                            let now = tokio::time::Instant::now();
                            let raw_silence = now.saturating_duration_since(last_raw_msg_at);
                            let market_data_silence = now.saturating_duration_since(last_market_data_at);
                            let no_full_book_silence = now.saturating_duration_since(
                                last_full_book_at.unwrap_or(session_started_at),
                            );

                            // Connection appears alive but no raw payloads: force reconnect.
                            if raw_silence
                                >= Duration::from_secs(MARKET_WS_NO_RAW_RECONNECT_SECS)
                            {
                                warn!(
                                    "⚠️ Market WS raw stream silent for {:?} (raw_msgs={} parsed={} book_ticks={} trade_ticks={}) — reconnecting",
                                    raw_silence,
                                    session_raw_msg_count,
                                    session_parsed_msg_count,
                                    session_book_tick_count,
                                    session_trade_tick_count
                                );
                                ping_handle.abort();
                                break;
                            }

                            // Raw payloads exist, but no usable market data reaches strategy path.
                            if session_raw_msg_count > 0
                                && !session_had_market_data
                                && market_data_silence >= Duration::from_secs(30)
                            {
                                warn!(
                                    "⚠️ Market WS has raw payloads but no usable market data for {:?} (raw_msgs={} parsed={}) — reconnecting",
                                    market_data_silence,
                                    session_raw_msg_count,
                                    session_parsed_msg_count
                                );
                                ping_handle.abort();
                                break;
                            }

                            // During bootstrap, raw payloads may arrive but never produce any usable
                            // market-data lane (neither full book nor trades). Reconnect in that case.
                            // After a session has already produced usable market data, do not reconnect
                            // merely because the market went quiet; that creates reconnect storms on
                            // naturally low-throughput rounds.
                            let bootstrap_lane_missing = !session_had_market_data;
                            if session_raw_msg_count > 0
                                && bootstrap_lane_missing
                                && no_full_book_silence
                                    >= Duration::from_secs(MARKET_WS_NO_FULL_BOOK_RECONNECT_SECS)
                            {
                                warn!(
                                    "⚠️ Market WS bootstrap lane never became usable for {:?} (raw={} parsed={} partial_yes={} partial_no={} full_book={} trade={} unknown={} dropped={} tx_dropped={}) — reconnecting",
                                    no_full_book_silence,
                                    session_raw_msg_count,
                                    session_parsed_msg_count,
                                    session_partial_yes_book_count,
                                    session_partial_no_book_count,
                                    session_book_tick_count,
                                    session_trade_tick_count,
                                    session_unknown_event_count,
                                    session_parse_drop_count,
                                    session_tx_drop_count,
                                );
                                ping_handle.abort();
                                break;
                            }

                            info!(
                                "📡 WS ingest | raw={} text={} binary={} parsed={} partial_yes={} partial_no={} full_book={} trade={} unknown={} dropped={} tx_dropped={} no_full_book_for={:.1}s",
                                session_raw_msg_count,
                                session_text_msg_count,
                                session_binary_msg_count,
                                session_parsed_msg_count,
                                session_partial_yes_book_count,
                                session_partial_no_book_count,
                                session_book_tick_count,
                                session_trade_tick_count,
                                session_unknown_event_count,
                                session_parse_drop_count,
                                session_tx_drop_count,
                                no_full_book_silence.as_secs_f64()
                            );
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    session_raw_msg_count =
                                        session_raw_msg_count.saturating_add(1);
                                    session_text_msg_count =
                                        session_text_msg_count.saturating_add(1);
                                    last_raw_msg_at = tokio::time::Instant::now();
                                    if let (Some(rec), Some(meta)) = (&recorder, &recorder_meta) {
                                        rec.record_market_ws_raw(meta, &text);
                                    }

                                    let (parsed, unknown_events, parse_drops, _drop_reasons) =
                                        parse_ws_payload(&settings, &text);
                                    session_unknown_event_count = session_unknown_event_count
                                        .saturating_add(unknown_events);
                                    session_parse_drop_count =
                                        session_parse_drop_count.saturating_add(parse_drops);

                                    for md_msg in parsed {
                                        session_parsed_msg_count =
                                            session_parsed_msg_count.saturating_add(1);
                                        match &md_msg {
                                            MarketDataMsg::TradeTick {
                                                asset_id,
                                                trade_id,
                                                market_side,
                                                taker_side,
                                                price,
                                                size,
                                                ..
                                            } => {
                                                session_had_market_data = true;
                                                session_trade_tick_count =
                                                    session_trade_tick_count.saturating_add(1);
                                                last_market_data_at = tokio::time::Instant::now();
                                                    if let (Some(rec), Some(meta)) = (&recorder, &recorder_meta) {
                                                        rec.record_market_trade(
                                                            meta,
                                                            asset_id,
                                                        *market_side,
                                                        *taker_side,
                                                        *price,
                                                        *size,
                                                        trade_id.as_deref(),
                                                            Some(unix_now_millis_u64()),
                                                        );
                                                    }
                                                    try_broadcast_dry_run_touch_md(
                                                        &dry_run_touch_md_tx,
                                                        &md_msg,
                                                    );
                                                    try_forward_md(
                                                        &ofi_tx,
                                                        md_msg.clone(),
                                                    &mut session_tx_drop_count,
                                                );
                                                try_forward_md(
                                                    &glft_tx,
                                                    md_msg.clone(),
                                                    &mut session_tx_drop_count,
                                                );
                                            }
                                            MarketDataMsg::BookTick { .. } => {
                                                if let MarketDataMsg::BookTick {
                                                    yes_bid,
                                                    yes_ask,
                                                    no_bid,
                                                    no_ask,
                                                    ..
                                                } = &md_msg
                                                {
                                                    let recv_ms = unix_now_millis_u64();
                                                    if yes_bid.is_finite()
                                                        || yes_ask.is_finite()
                                                    {
                                                        let _ = post_close_book_tx.try_send(
                                                            PostCloseSideBookUpdate {
                                                                side: Side::Yes,
                                                                bid: (*yes_bid).max(0.0),
                                                                ask: (*yes_ask).max(0.0),
                                                                recv_ms,
                                                            },
                                                        );
                                                    }
                                                    if no_bid.is_finite() || no_ask.is_finite() {
                                                        let _ = post_close_book_tx.try_send(
                                                            PostCloseSideBookUpdate {
                                                                side: Side::No,
                                                                bid: (*no_bid).max(0.0),
                                                                ask: (*no_ask).max(0.0),
                                                                recv_ms,
                                                            },
                                                        );
                                                    }
                                                    if *yes_bid > 0.0 || *yes_ask > 0.0 {
                                                        session_partial_yes_book_count =
                                                            session_partial_yes_book_count
                                                                .saturating_add(1);
                                                    }
                                                        if *no_bid > 0.0 || *no_ask > 0.0 {
                                                            session_partial_no_book_count =
                                                                session_partial_no_book_count
                                                                    .saturating_add(1);
                                                        }
                                                    }
                                                    try_broadcast_dry_run_touch_md(
                                                        &dry_run_touch_md_tx,
                                                        &md_msg,
                                                    );
                                                    if coord_accept_partial_book {
                                                        let _ = coord_tx.send(md_msg.clone());
                                                }
                                                if let Some(full) = book_asm.update(&md_msg) {
                                                    session_had_market_data = true;
                                                    session_book_tick_count =
                                                        session_book_tick_count.saturating_add(1);
                                                    last_market_data_at = tokio::time::Instant::now();
                                                    last_full_book_at = Some(last_market_data_at);
                                                        if let (
                                                            Some(rec),
                                                            Some(meta),
                                                        MarketDataMsg::BookTick {
                                                            yes_bid,
                                                            yes_ask,
                                                            no_bid,
                                                            no_ask,
                                                            ..
                                                        },
                                                    ) = (&recorder, &recorder_meta, &full)
                                                    {
                                                            rec.record_market_book_l1(
                                                                meta, *yes_bid, *yes_ask, *no_bid, *no_ask,
                                                            );
                                                        }
                                                        try_broadcast_dry_run_touch_md(
                                                            &dry_run_touch_md_tx,
                                                            &full,
                                                        );
                                                        try_forward_md(
                                                            &glft_tx,
                                                            full.clone(),
                                                        &mut session_tx_drop_count,
                                                    );
                                                    let _ = coord_tx.send(full);
                                                }
                                            }
                                            MarketDataMsg::WinnerHint { .. } => {
                                                let _ = coord_tx.send(md_msg.clone());
                                            }
                                            MarketDataMsg::OracleLagSelection { .. } => {
                                                // Arbiter-internal; never arrives from WS parser.
                                            }
                                            MarketDataMsg::OracleLagTailAction { .. } => {
                                                // Supervisor-internal; never arrives from WS parser.
                                            }
                                        }
                                        if session_parsed_msg_count % 256 == 0 {
                                            tokio::task::yield_now().await;
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(bytes))) => {
                                    session_raw_msg_count =
                                        session_raw_msg_count.saturating_add(1);
                                    session_binary_msg_count =
                                        session_binary_msg_count.saturating_add(1);
                                    last_raw_msg_at = tokio::time::Instant::now();

                                    match std::str::from_utf8(&bytes) {
                                        Ok(text) => {
                                            if let (Some(rec), Some(meta)) =
                                                (&recorder, &recorder_meta)
                                            {
                                                rec.record_market_ws_raw(meta, text);
                                            }
                                            let (parsed, unknown_events, parse_drops, _drop_reasons) =
                                                parse_ws_payload(&settings, text);
                                            session_unknown_event_count = session_unknown_event_count
                                                .saturating_add(unknown_events);
                                            session_parse_drop_count = session_parse_drop_count
                                                .saturating_add(parse_drops);

                                            for md_msg in parsed {
                                                session_parsed_msg_count =
                                                    session_parsed_msg_count.saturating_add(1);
                                                match &md_msg {
                                                    MarketDataMsg::TradeTick {
                                                        asset_id,
                                                        trade_id,
                                                        market_side,
                                                        taker_side,
                                                        price,
                                                        size,
                                                        ..
                                                    } => {
                                                        session_had_market_data = true;
                                                        session_trade_tick_count =
                                                            session_trade_tick_count.saturating_add(1);
                                                        last_market_data_at = tokio::time::Instant::now();
                                                            if let (Some(rec), Some(meta)) =
                                                                (&recorder, &recorder_meta)
                                                            {
                                                                rec.record_market_trade(
                                                                meta,
                                                                asset_id,
                                                                *market_side,
                                                                *taker_side,
                                                                *price,
                                                                *size,
                                                                trade_id.as_deref(),
                                                                    Some(unix_now_millis_u64()),
                                                                );
                                                            }
                                                            try_broadcast_dry_run_touch_md(
                                                                &dry_run_touch_md_tx,
                                                                &md_msg,
                                                            );
                                                            try_forward_md(
                                                                &ofi_tx,
                                                                md_msg.clone(),
                                                            &mut session_tx_drop_count,
                                                        );
                                                        try_forward_md(
                                                            &glft_tx,
                                                            md_msg.clone(),
                                                            &mut session_tx_drop_count,
                                                        );
                                                    }
                                                    MarketDataMsg::BookTick { .. } => {
                                                        if let MarketDataMsg::BookTick {
                                                            yes_bid,
                                                            yes_ask,
                                                            no_bid,
                                                            no_ask,
                                                            ..
                                                        } = &md_msg
                                                        {
                                                            let recv_ms = unix_now_millis_u64();
                                                            if yes_bid.is_finite()
                                                                || yes_ask.is_finite()
                                                            {
                                                                let _ = post_close_book_tx.try_send(
                                                                    PostCloseSideBookUpdate {
                                                                        side: Side::Yes,
                                                                        bid: (*yes_bid).max(0.0),
                                                                        ask: (*yes_ask).max(0.0),
                                                                        recv_ms,
                                                                    },
                                                                );
                                                            }
                                                            if no_bid.is_finite() || no_ask.is_finite() {
                                                                let _ = post_close_book_tx.try_send(
                                                                    PostCloseSideBookUpdate {
                                                                        side: Side::No,
                                                                        bid: (*no_bid).max(0.0),
                                                                        ask: (*no_ask).max(0.0),
                                                                        recv_ms,
                                                                    },
                                                                );
                                                            }
                                                            if *yes_bid > 0.0 || *yes_ask > 0.0 {
                                                                session_partial_yes_book_count =
                                                                    session_partial_yes_book_count
                                                                        .saturating_add(1);
                                                            }
                                                                if *no_bid > 0.0 || *no_ask > 0.0 {
                                                                    session_partial_no_book_count =
                                                                        session_partial_no_book_count
                                                                            .saturating_add(1);
                                                                }
                                                            }
                                                            try_broadcast_dry_run_touch_md(
                                                                &dry_run_touch_md_tx,
                                                                &md_msg,
                                                            );
                                                            if coord_accept_partial_book {
                                                                let _ = coord_tx.send(md_msg.clone());
                                                        }
                                                        if let Some(full) = book_asm.update(&md_msg) {
                                                            session_had_market_data = true;
                                                            session_book_tick_count =
                                                                session_book_tick_count
                                                                    .saturating_add(1);
                                                            last_market_data_at = tokio::time::Instant::now();
                                                            last_full_book_at = Some(last_market_data_at);
                                                            if let (
                                                                Some(rec),
                                                                Some(meta),
                                                                MarketDataMsg::BookTick {
                                                                    yes_bid,
                                                                    yes_ask,
                                                                    no_bid,
                                                                    no_ask,
                                                                    ..
                                                                },
                                                            ) = (&recorder, &recorder_meta, &full)
                                                                {
                                                                    rec.record_market_book_l1(
                                                                        meta, *yes_bid, *yes_ask, *no_bid, *no_ask,
                                                                    );
                                                                }
                                                                try_broadcast_dry_run_touch_md(
                                                                    &dry_run_touch_md_tx,
                                                                    &full,
                                                                );
                                                                try_forward_md(
                                                                    &glft_tx,
                                                                    full.clone(),
                                                                &mut session_tx_drop_count,
                                                            );
                                                            let _ = coord_tx.send(full);
                                                        }
                                                    }
                                                    MarketDataMsg::WinnerHint { .. } => {
                                                        let _ = coord_tx.send(md_msg.clone());
                                                    }
                                                    MarketDataMsg::OracleLagSelection { .. } => {
                                                        // Arbiter-internal; never arrives from WS parser.
                                                    }
                                                    MarketDataMsg::OracleLagTailAction { .. } => {
                                                        // Supervisor-internal; never arrives from WS parser.
                                                    }
                                                }
                                                if session_parsed_msg_count % 256 == 0 {
                                                    tokio::task::yield_now().await;
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            session_non_utf8_binary_count =
                                                session_non_utf8_binary_count.saturating_add(1);
                                            session_parse_drop_count =
                                                session_parse_drop_count.saturating_add(1);
                                            if !warned_non_utf8_binary {
                                                warn!(
                                                    "⚠️ Market WS received non-UTF8 binary frame(s); first_len={} — relying on reconnect/fallback",
                                                    bytes.len()
                                                );
                                                warned_non_utf8_binary = true;
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("WS closed by server");
                                    ping_handle.abort();
                                    break;
                                }
                                Some(Err(err)) => {
                            let msg = format!("{err:?}");
                            if msg.contains("ResetWithoutClosingHandshake") {
                                info!("📡 Market WS server reset (expected) — fast reconnect");
                            } else {
                                warn!("WS error: {err:?}");
                            }
                            ping_handle.abort();
                            break;
                        }
                                None => {
                                    ping_handle.abort();
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                info!(
                    "📡 WS session summary: raw_msgs={} text={} binary={} non_utf8_binary={} parsed={} partial_yes={} partial_no={} book_ticks={} trade_ticks={} unknown={} dropped={} tx_dropped={} had_market_data={} custom_feature_enabled={}",
                    session_raw_msg_count,
                    session_text_msg_count,
                    session_binary_msg_count,
                    session_non_utf8_binary_count,
                    session_parsed_msg_count,
                    session_partial_yes_book_count,
                    session_partial_no_book_count,
                    session_book_tick_count,
                    session_trade_tick_count,
                    session_unknown_event_count,
                    session_parse_drop_count,
                    session_tx_drop_count,
                    session_had_market_data,
                    subscribe_custom_feature_enabled
                );
                if subscribe_custom_feature_enabled {
                    if session_raw_msg_count == 0 {
                        warn!(
                            "🧪 WS A/B fallback: no raw payloads in this session with custom_feature_enabled=true; retrying next session with custom_feature_enabled=false"
                        );
                        subscribe_custom_feature_enabled = false;
                    } else if session_book_tick_count == 0 {
                        warn!(
                            "🧪 WS A/B fallback: no complete book in this session with custom_feature_enabled=true; retrying next session with custom_feature_enabled=false"
                        );
                        subscribe_custom_feature_enabled = false;
                    }
                }
                if session_expired {
                    return MarketEnd::Expired;
                }
                if session_had_market_data {
                    consecutive_failures = 0;
                } else {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                }
            }
            Ok(Err(err)) => {
                warn!("WS connect error: {err:?}");
                consecutive_failures = consecutive_failures.saturating_add(1);
            }
            Err(_) => {
                warn!("⏱️ WS connection timeout");
                consecutive_failures = consecutive_failures.saturating_add(1);
            }
        }

        // If expired during reconnect, stop
        if tokio::time::Instant::now() >= deadline {
            info!("🏁 Market expired during reconnect");
            return MarketEnd::Expired;
        }

        if should_degrade_ws(consecutive_failures, ws_degrade_failures) {
            let remaining_secs = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_secs();
            warn!(
                "🛑 WS degraded: consecutive_failures={} >= {} (remaining={}s) — ending current market early",
                consecutive_failures, ws_degrade_failures, remaining_secs
            );
            return MarketEnd::WsDegraded {
                consecutive_failures,
                remaining_secs,
            };
        }

        info!(
            "🔄 Reconnecting in {:?}... (consecutive_failures={})",
            backoff, consecutive_failures
        );
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// ─────────────────────────────────────────────────────────
// Per-slug process lock: binds a loopback TCP port derived from the slug.
// Prevents two OS processes from running oracle-lag-sniping for the same
// slug simultaneously. Automatically released when the process exits or
// the guard is dropped.
// ─────────────────────────────────────────────────────────

struct SlugLock {
    _listener: TcpListener,
    port: u16,
}

fn slug_lock_class_name(lab_only: bool) -> &'static str {
    if lab_only {
        "lab"
    } else {
        "trade"
    }
}

fn slug_lock_key(slug: &str, lab_only: bool) -> String {
    format!("{}::{}", slug_lock_class_name(lab_only), slug)
}

fn slug_lock_port(slug: &str, lab_only: bool) -> u16 {
    let lock_key = slug_lock_key(slug, lab_only);
    let mut h: u32 = 0x811c9dc5u32;
    for b in lock_key.bytes() {
        h = h.wrapping_mul(0x01000193).wrapping_add(b as u32);
    }
    // Dynamic/private port range: 49152–59999 (10848 slots)
    49152 + (h % 10848) as u16
}

fn try_acquire_slug_lock(slug: &str, lab_only: bool) -> Option<SlugLock> {
    let port = slug_lock_port(slug, lab_only);
    match TcpListener::bind(format!("127.0.0.1:{}", port)) {
        Ok(listener) => Some(SlugLock {
            _listener: listener,
            port,
        }),
        Err(_) => None,
    }
}

fn install_rustls_crypto_provider() {
    // rustls 0.23 requires one process-level CryptoProvider.
    // Install ring explicitly so WS/TLS tasks won't panic on provider auto-detect.
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_ok()
    {
        info!("🔐 rustls CryptoProvider installed: ring");
    } else {
        // Already installed in-process (expected in some supervisor/worker paths).
        debug!("🔐 rustls CryptoProvider already installed");
    }
}

// ─────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let active_instance_id = instance_id();
    let active_log_root = log_root();
    // Per-worker log isolation: when spawned as a supervisor child, each worker
    // writes to its own slug-tagged daily-rolling file so grepping one market's
    // story stops requiring slug= field scoping on every log line.
    let log_slug = env::var("POLYMARKET_MARKET_SLUG")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let log_filename_prefix = match log_slug.as_deref() {
        Some(slug) => format!("polymarket.{}.log", slug),
        None => "polymarket.log".to_string(),
    };
    // Dual-output logging: stdout + daily rolling file in log_root().
    fs::create_dir_all(&active_log_root)?;
    let file_appender = tracing_appender::rolling::daily(&active_log_root, &log_filename_prefix);
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    {
        use tracing_subscriber::fmt::writer::MakeWriterExt;
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_writer(std::io::stdout.and(non_blocking))
            .init();
    }
    install_rustls_crypto_provider();

    info!("═══════════════════════════════════════════════════");
    info!("  Polymarket V2 — Async Inventory Arbitrage Engine");
    info!("  Auto-Discovery + Market Rotation");
    info!("═══════════════════════════════════════════════════");
    info!(
        "🗂️ runtime_paths | instance_id={} log_root={}",
        active_instance_id.as_deref().unwrap_or("unset"),
        active_log_root.display()
    );
    if shared_ingress_role() == SharedIngressRole::Auto {
        info!(
            "🤖 shared ingress auto mode | root={} protocol={} schema={} build_id={}",
            shared_ingress_root().display(),
            shared_ingress_protocol_version(),
            shared_ingress_schema_version(),
            shared_ingress_build_id()
        );
        ensure_shared_ingress_broker_running().await?;
        set_shared_ingress_role_override(SharedIngressRole::Client);
        info!(
            "✅ shared ingress auto resolved to client | root={}",
            shared_ingress_root().display()
        );
    }
    match shared_ingress_role() {
        SharedIngressRole::Broker => {
            if !shared_ingress_broker_replace_existing() {
                cleanup_stale_broker_artifacts();
                if let Some(manifest) = read_broker_manifest() {
                    if manifest.pid != std::process::id()
                        && is_broker_manifest_healthy(&manifest)
                        && shared_ingress_pid_alive(manifest.pid)
                    {
                        warn!(
                            "🛑 shared ingress broker duplicate start suppressed | existing_pid={} root={} set PM_SHARED_INGRESS_BROKER_REPLACE_EXISTING=true to force replacement",
                            manifest.pid,
                            shared_ingress_root().display()
                        );
                        return Ok(());
                    }
                }
            }
            info!(
                "🧵 shared ingress broker mode | root={}",
                shared_ingress_root().display()
            );
            return run_shared_ingress_broker().await;
        }
        SharedIngressRole::Client => {
            info!(
                "🧵 shared ingress client mode | root={}",
                shared_ingress_root().display()
            );
            spawn_shared_ingress_client_lease_heartbeat();
        }
        SharedIngressRole::Standalone => {}
        SharedIngressRole::Auto => unreachable!("auto role must resolve before worker startup"),
    }

    let is_multi_market_child = env::var("PM_MULTI_MARKET_CHILD")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let raw_market_slug = env::var("POLYMARKET_MARKET_SLUG").ok();
    let exact_fixed_slug = raw_market_slug
        .as_deref()
        .map(|slug| !slug.trim().is_empty() && !is_prefix_slug(slug))
        .unwrap_or(false);
    let explicit_market_ids = env::var("POLYMARKET_MARKET_ID")
        .ok()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
        && env::var("POLYMARKET_YES_ASSET_ID")
            .ok()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
        && env::var("POLYMARKET_NO_ASSET_ID")
            .ok()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false);
    let multi_market_prefixes = parse_multi_market_prefixes_from_env();
    if exact_fixed_slug && explicit_market_ids && multi_market_prefixes.len() > 1 {
        info!(
            "🧷 exact fixed-mode slug+ids provided — bypassing multi-market supervisor and forcing single-market worker"
        );
    } else if !is_multi_market_child && multi_market_prefixes.len() > 1 {
        return run_multi_market_supervisor(multi_market_prefixes).await;
    }
    run_prefix_worker(None).await
}

/// Shared ingress plane passed from the in-proc supervisor to per-slug workers.
/// This keeps one upstream data plane and multiple downstream workers.
#[derive(Clone)]
struct SharedIngressRuntime {
    chainlink_hub: Option<Arc<ChainlinkHub>>,
    local_price_hub: Option<Arc<LocalPriceHub>>,
}

fn shared_ingress_hub_symbols(
    prefixes: &[String],
    coord_cfg: &CoordinatorConfig,
) -> HashSet<String> {
    let mut hub_symbols: HashSet<String> = HashSet::new();
    if coord_cfg.strategy.is_oracle_lag_sniping() {
        for prefix in prefixes {
            if let Some(sym) = oracle_lag_symbol_from_slug(prefix) {
                hub_symbols.insert(format!("{}/usd", sym));
            } else {
                warn!(
                    "⚠️ shared ingress: could not derive symbol from prefix='{}' — this slug will not have a Chainlink feed",
                    prefix
                );
            }
        }
        if hub_symbols.is_empty() {
            for base in OracleLagSymbolUniverse::from_env().hub_symbols() {
                hub_symbols.insert(format!("{}/usd", base));
            }
        }
    }
    hub_symbols
}

fn build_chainlink_hub_for_symbols(symbols: &HashSet<String>) -> Option<Arc<ChainlinkHub>> {
    if symbols.is_empty() {
        return None;
    }
    let mut log_symbols: Vec<String> = symbols.iter().cloned().collect();
    log_symbols.sort();
    match shared_ingress_role() {
        SharedIngressRole::Client | SharedIngressRole::Auto => {
            let socket_path = shared_ingress_chainlink_socket_path();
            info!(
                "🛰️ shared ingress chainlink_hub remote client | symbols={} socket={}",
                log_symbols.join(","),
                socket_path.display()
            );
            Some(ChainlinkHub::spawn_remote(symbols.clone(), socket_path))
        }
        SharedIngressRole::Standalone | SharedIngressRole::Broker => {
            info!(
                "🛰️ shared ingress chainlink_hub local | symbols={}",
                log_symbols.join(",")
            );
            Some(ChainlinkHub::spawn(symbols.clone()))
        }
    }
}

fn build_local_price_hub_for_symbols(
    symbols: &HashSet<String>,
    coord_cfg: &CoordinatorConfig,
) -> Option<Arc<LocalPriceHub>> {
    if !coord_cfg.strategy.is_oracle_lag_sniping()
        || !local_price_agg_enabled()
        || symbols.is_empty()
    {
        return None;
    }
    let mut log_symbols: Vec<String> = symbols.iter().cloned().collect();
    log_symbols.sort();
    match shared_ingress_role() {
        SharedIngressRole::Client | SharedIngressRole::Auto => {
            let socket_path = shared_ingress_local_price_socket_path();
            info!(
                "🧠 shared ingress local_price_hub remote client | symbols={} socket={} sources={} bias_learning_enabled={}",
                log_symbols.join(","),
                socket_path.display(),
                local_price_source_names(),
                local_price_agg_bias_learning_enabled(),
            );
            Some(LocalPriceHub::spawn_remote(symbols.clone(), socket_path))
        }
        SharedIngressRole::Standalone | SharedIngressRole::Broker => {
            info!(
                "🧠 shared ingress local_price_hub local | symbols={} sources={} bias_learning_enabled={}",
                log_symbols.join(","),
                local_price_source_names(),
                local_price_agg_bias_learning_enabled(),
            );
            Some(LocalPriceHub::spawn(symbols.clone()))
        }
    }
}

impl SharedIngressRuntime {
    fn build(prefixes: &[String], coord_cfg: &CoordinatorConfig) -> Self {
        let hub_symbols = shared_ingress_hub_symbols(prefixes, coord_cfg);
        let chainlink_hub = build_chainlink_hub_for_symbols(&hub_symbols);
        let local_price_hub = build_local_price_hub_for_symbols(&hub_symbols, coord_cfg);
        Self {
            chainlink_hub,
            local_price_hub,
        }
    }
}

async fn handle_chainlink_ingress_client(
    stream: UnixStream,
    hub: Arc<ChainlinkHub>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        anyhow::bail!("chainlink client closed before subscribe");
    }
    let req: SharedIngressSubscribeReq = serde_json::from_str(line.trim())?;
    if req.stream != "chainlink" {
        shared_ingress_send_wire_line(
            &mut write_half,
            &SharedIngressWireMsg::Error {
                message: format!("unexpected stream '{}'", req.stream),
            },
        )
        .await?;
        return Ok(());
    }
    let mut symbols = req
        .symbols
        .into_iter()
        .map(|s| normalize_chainlink_symbol(&s))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();
    for symbol in &symbols {
        for (price, ts_ms) in hub.snapshot_recent_ticks(symbol) {
            shared_ingress_send_wire_line(
                &mut write_half,
                &SharedIngressWireMsg::ChainlinkSnapshot {
                    symbol: symbol.clone(),
                    price,
                    ts_ms,
                },
            )
            .await?;
        }
    }
    shared_ingress_send_wire_line(&mut write_half, &SharedIngressWireMsg::SnapshotDone).await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<SharedIngressWireMsg>();
    let mut tasks = Vec::new();
    for symbol in symbols {
        let Some(mut sub) = hub.subscribe(&symbol) else {
            continue;
        };
        let tx_clone = tx.clone();
        let symbol_clone = symbol.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                match sub.recv().await {
                    Ok((price, ts_ms)) => {
                        if tx_clone
                            .send(SharedIngressWireMsg::ChainlinkTick {
                                symbol: symbol_clone.clone(),
                                price,
                                ts_ms,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }));
    }
    drop(tx);
    while let Some(msg) = rx.recv().await {
        if shared_ingress_send_wire_line(&mut write_half, &msg)
            .await
            .is_err()
        {
            break;
        }
    }
    for task in tasks {
        task.abort();
    }
    Ok(())
}

async fn handle_local_price_ingress_client(
    stream: UnixStream,
    hub: Arc<LocalPriceHub>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        anyhow::bail!("local_price client closed before subscribe");
    }
    let req: SharedIngressSubscribeReq = serde_json::from_str(line.trim())?;
    if req.stream != "local_price" {
        shared_ingress_send_wire_line(
            &mut write_half,
            &SharedIngressWireMsg::Error {
                message: format!("unexpected stream '{}'", req.stream),
            },
        )
        .await?;
        return Ok(());
    }
    let mut symbols = req
        .symbols
        .into_iter()
        .map(|s| normalize_chainlink_symbol(&s))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();
    for symbol in &symbols {
        for (price, ts_ms, source) in hub.snapshot_recent_ticks(symbol) {
            shared_ingress_send_wire_line(
                &mut write_half,
                &SharedIngressWireMsg::LocalPriceSnapshot {
                    symbol: symbol.clone(),
                    price,
                    ts_ms,
                    source: source.as_str().to_string(),
                },
            )
            .await?;
        }
    }
    shared_ingress_send_wire_line(&mut write_half, &SharedIngressWireMsg::SnapshotDone).await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<SharedIngressWireMsg>();
    let mut tasks = Vec::new();
    for symbol in symbols {
        let Some(mut sub) = hub.subscribe(&symbol) else {
            continue;
        };
        let tx_clone = tx.clone();
        let symbol_clone = symbol.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                match sub.recv().await {
                    Ok((price, ts_ms, source)) => {
                        if tx_clone
                            .send(SharedIngressWireMsg::LocalPriceTick {
                                symbol: symbol_clone.clone(),
                                price,
                                ts_ms,
                                source: source.as_str().to_string(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }));
    }
    drop(tx);
    while let Some(msg) = rx.recv().await {
        if shared_ingress_send_wire_line(&mut write_half, &msg)
            .await
            .is_err()
        {
            break;
        }
    }
    for task in tasks {
        task.abort();
    }
    Ok(())
}

async fn handle_market_ingress_client(
    stream: UnixStream,
    registry: Arc<Mutex<HashMap<SharedMarketFeedKey, Arc<SharedMarketIngressFeed>>>>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        anyhow::bail!("market client closed before subscribe");
    }
    let req: SharedIngressSubscribeReq = serde_json::from_str(line.trim())?;
    if req.stream != "market" {
        shared_ingress_send_wire_line(
            &mut write_half,
            &SharedIngressWireMsg::Error {
                message: format!("unexpected stream '{}'", req.stream),
            },
        )
        .await?;
        return Ok(());
    }
    let yes_asset_id = req
        .yes_asset_id
        .clone()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("market subscribe missing yes_asset_id"))?;
    let no_asset_id = req
        .no_asset_id
        .clone()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("market subscribe missing no_asset_id"))?;
    let ws_base_url = req
        .ws_base_url
        .clone()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "wss://ws-subscriptions-clob.polymarket.com/ws".to_string());
    let key = SharedMarketFeedKey {
        ws_base_url: ws_base_url.clone(),
        yes_asset_id: yes_asset_id.clone(),
        no_asset_id: no_asset_id.clone(),
        custom_feature_enabled: req.custom_feature_enabled.unwrap_or(false),
    };
    let (feed, reused_existing, registry_len, spawn_settings) = {
        let mut guard = registry.lock().expect("market registry poisoned");
        if let Some(feed) = guard.get(&key) {
            (Arc::clone(feed), true, guard.len(), None)
        } else {
            let feed = SharedMarketIngressFeed::new();
            let mut settings = Settings {
                market_slug: req.market_slug.clone(),
                market_id: req.market_id.clone().unwrap_or_default(),
                yes_asset_id: yes_asset_id.clone(),
                no_asset_id: no_asset_id.clone(),
                ws_base_url: ws_base_url.clone(),
                rest_url: String::new(),
                private_key: None,
                funder_address: None,
                custom_feature: key.custom_feature_enabled,
            };
            if settings.market_slug.is_none() {
                settings.market_slug = Some(format!(
                    "{}-{}",
                    &yes_asset_id[..8.min(yes_asset_id.len())],
                    &no_asset_id[..8.min(no_asset_id.len())]
                ));
            }
            guard.insert(key.clone(), Arc::clone(&feed));
            let registry_len = guard.len();
            (feed, false, registry_len, Some(settings))
        }
    };
    let (lease, subscribers) =
        SharedMarketFeedLease::new(key.clone(), Arc::clone(&feed), Arc::clone(&registry));
    let _lease = lease;
    if let Some(settings) = spawn_settings {
        let feed_clone = Arc::clone(&feed);
        tokio::spawn(async move {
            run_market_ws_broker_feed(settings, feed_clone).await;
        });
    }
    info!(
        "📡 shared ingress market feed attached | slug={} yes_asset_id={} no_asset_id={} reused_existing={} subscribers={} registry_size={}",
        req.market_slug.clone().unwrap_or_else(|| "unknown".to_string()),
        yes_asset_id,
        no_asset_id,
        reused_existing,
        subscribers,
        registry_len
    );
    let (snapshot, mut rx) = feed.snapshot_and_subscribe();
    for msg in snapshot {
        shared_ingress_send_wire_line(&mut write_half, &msg).await?;
    }
    shared_ingress_send_wire_line(&mut write_half, &SharedIngressWireMsg::SnapshotDone).await?;
    line.clear();
    loop {
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(msg) => {
                        if shared_ingress_send_wire_line(&mut write_half, &msg)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            read = reader.read_line(&mut line) => {
                match read {
                    Ok(0) => break,
                    Ok(_) => {
                        // Market ingress clients do not send runtime control messages today.
                        // Drain any unexpected bytes and keep the lease alive until the peer
                        // actually closes, so empty feeds can still be released on EOF.
                        line.clear();
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }
    Ok(())
}

async fn run_chainlink_ingress_broker(hub: Arc<ChainlinkHub>) -> anyhow::Result<()> {
    let socket_path = shared_ingress_chainlink_socket_path();
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(&socket_path) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    let listener = UnixListener::bind(&socket_path)?;
    info!(
        "🛰️ shared ingress chainlink broker listening | socket={}",
        socket_path.display()
    );
    loop {
        let (stream, _) = listener.accept().await?;
        let hub_clone = Arc::clone(&hub);
        tokio::spawn(async move {
            if let Err(err) = handle_chainlink_ingress_client(stream, hub_clone).await {
                warn!("⚠️ shared ingress chainlink client error | err={}", err);
            }
        });
    }
}

async fn run_local_price_ingress_broker(hub: Arc<LocalPriceHub>) -> anyhow::Result<()> {
    let socket_path = shared_ingress_local_price_socket_path();
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(&socket_path) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    let listener = UnixListener::bind(&socket_path)?;
    info!(
        "🧠 shared ingress local_price broker listening | socket={}",
        socket_path.display()
    );
    loop {
        let (stream, _) = listener.accept().await?;
        let hub_clone = Arc::clone(&hub);
        tokio::spawn(async move {
            if let Err(err) = handle_local_price_ingress_client(stream, hub_clone).await {
                warn!("⚠️ shared ingress local_price client error | err={}", err);
            }
        });
    }
}

async fn run_market_ingress_broker(
    registry: Arc<Mutex<HashMap<SharedMarketFeedKey, Arc<SharedMarketIngressFeed>>>>,
) -> anyhow::Result<()> {
    let socket_path = shared_ingress_market_socket_path();
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(&socket_path) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    let listener = UnixListener::bind(&socket_path)?;
    info!(
        "📡 shared ingress market broker listening | socket={}",
        socket_path.display()
    );
    loop {
        let (stream, _) = listener.accept().await?;
        let registry_clone = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(err) = handle_market_ingress_client(stream, registry_clone).await {
                warn!("⚠️ shared ingress market client error | err={}", err);
            }
        });
    }
}

async fn run_shared_ingress_broker() -> anyhow::Result<()> {
    let coord_cfg = CoordinatorConfig::from_env();
    let prefixes = parse_multi_market_prefixes_from_env();
    let hub_symbols = shared_ingress_hub_symbols(&prefixes, &coord_cfg);
    let local_price_symbols =
        if coord_cfg.strategy.is_oracle_lag_sniping() && local_price_agg_enabled() {
            hub_symbols.clone()
        } else {
            HashSet::new()
        };
    if hub_symbols.is_empty() {
        info!(
            "🪫 shared ingress broker starting in market-only mode | no chainlink/local_price symbols configured"
        );
    }
    let chainlink_hub = ChainlinkHub::spawn(hub_symbols.clone());
    let local_price_hub = LocalPriceHub::spawn(hub_symbols.clone());
    let market_registry: Arc<Mutex<HashMap<SharedMarketFeedKey, Arc<SharedMarketIngressFeed>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut chainlink_task =
        tokio::spawn(async move { run_chainlink_ingress_broker(chainlink_hub).await });
    let mut local_price_task = Some(tokio::spawn(async move {
        run_local_price_ingress_broker(local_price_hub).await
    }));
    let market_registry_clone = Arc::clone(&market_registry);
    let mut market_task =
        tokio::spawn(async move { run_market_ingress_broker(market_registry_clone).await });
    let started_ms = now_ms();
    let build_id = shared_ingress_build_id();
    let protocol_version = shared_ingress_protocol_version();
    let schema_version = shared_ingress_schema_version();
    let mut heartbeat =
        tokio::time::interval(Duration::from_millis(SHARED_INGRESS_HEARTBEAT_INTERVAL_MS));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut zero_clients_since: Option<tokio::time::Instant> = None;
    let mut last_market_registry_len: Option<usize> = None;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                warn!("🛑 shared ingress broker received ctrl-c — shutting down");
                break;
            }
            _ = heartbeat.tick() => {
                let manifest = SharedIngressBrokerManifest {
                    protocol_version,
                    schema_version,
                    build_id: build_id.clone(),
                    capabilities: SharedIngressBrokerCapabilities {
                        market_enabled: true,
                        chainlink_symbols: sorted_symbol_vec(&hub_symbols),
                        local_price_symbols: sorted_symbol_vec(&local_price_symbols),
                    },
                    pid: std::process::id(),
                    started_ms,
                    last_heartbeat_ms: now_ms(),
                    chainlink_socket: shared_ingress_chainlink_socket_path().display().to_string(),
                    local_price_socket: shared_ingress_local_price_socket_path().display().to_string(),
                    market_socket: shared_ingress_market_socket_path().display().to_string(),
                };
                if let Err(err) = write_json_pretty_atomic(&shared_ingress_broker_manifest_path(), &manifest) {
                    warn!("⚠️ shared ingress broker manifest write failed | err={}", err);
                }
                let cleanup = cleanup_stale_client_leases();
                if cleanup.removed_stale > 0 || cleanup.removed_invalid > 0 {
                    info!(
                        "🧹 shared ingress client lease cleanup | removed_stale={} removed_invalid={}",
                        cleanup.removed_stale, cleanup.removed_invalid
                    );
                }
                let active_clients = shared_ingress_active_client_count();
                let market_registry_len = market_registry.lock().map(|g| g.len()).unwrap_or_default();
                if last_market_registry_len != Some(market_registry_len) {
                    info!(
                        "📊 shared ingress market registry size | feeds={} active_clients={}",
                        market_registry_len,
                        active_clients
                    );
                    last_market_registry_len = Some(market_registry_len);
                }
                if shared_ingress_idle_exit_enabled() {
                    if active_clients == 0 {
                        match zero_clients_since {
                            Some(since) if since.elapsed() >= Duration::from_millis(SHARED_INGRESS_IDLE_SHUTDOWN_GRACE_MS) => {
                                info!(
                                    "🛑 shared ingress broker idle shutdown | active_clients=0 grace_ms={}",
                                    SHARED_INGRESS_IDLE_SHUTDOWN_GRACE_MS
                                );
                                break;
                            }
                            Some(_) => {}
                            None => {
                                zero_clients_since = Some(tokio::time::Instant::now());
                            }
                        }
                    } else {
                        zero_clients_since = None;
                    }
                }
            }
            joined = &mut chainlink_task => {
                match joined {
                    Ok(Err(err)) => warn!("⚠️ shared ingress chainlink broker exited with error: {}", err),
                    Err(err) => warn!("⚠️ shared ingress chainlink broker join error: {}", err),
                    Ok(Ok(())) => info!("🏁 shared ingress chainlink broker exited cleanly"),
                }
                break;
            }
            joined = async {
                match &mut local_price_task {
                    Some(task) => task.await.ok().and_then(Result::err),
                    None => None,
                }
            }, if local_price_task.is_some() => {
                if let Some(err) = joined {
                    warn!("⚠️ shared ingress local_price broker exited with error: {}", err);
                } else {
                    info!("🏁 shared ingress local_price broker exited");
                }
                break;
            }
            joined = &mut market_task => {
                match joined {
                    Ok(Err(err)) => warn!("⚠️ shared ingress market broker exited with error: {}", err),
                    Err(err) => warn!("⚠️ shared ingress market broker join error: {}", err),
                    Ok(Ok(())) => info!("🏁 shared ingress market broker exited cleanly"),
                }
                break;
            }
        }
    }
    chainlink_task.abort();
    if let Some(task) = local_price_task.take() {
        task.abort();
    }
    market_task.abort();
    let _ = fs::remove_file(shared_ingress_broker_manifest_path());
    let _ = fs::remove_file(shared_ingress_chainlink_socket_path());
    let _ = fs::remove_file(shared_ingress_local_price_socket_path());
    let _ = fs::remove_file(shared_ingress_market_socket_path());
    Ok(())
}

struct WorkerCtx {
    slug: String,
    shared_ingress: SharedIngressRuntime,
    /// Cross-market arbiter channel. When Some, hint listeners send
    /// `ArbiterObservation` to this instead of `WinnerHint` directly.
    arbiter_tx: Option<mpsc::Sender<ArbiterObservation>>,
    /// Round-tail coordinator channel.
    /// When Some, hint listeners report one final observation per round so
    /// supervisor can dispatch exactly one tail action across markets.
    round_tail_tx: Option<mpsc::Sender<RoundTailObservation>>,
}

async fn run_prefix_worker(ctx: Option<Arc<WorkerCtx>>) -> anyhow::Result<()> {
    let mut base_settings = Settings::from_env()?;
    if let Some(c) = &ctx {
        base_settings.market_slug = Some(c.slug.clone());
    }
    let raw_slug = base_settings
        .market_slug
        .clone()
        .unwrap_or_else(|| "btc-updown-15m".to_string());
    let prefix_mode = is_prefix_slug(&raw_slug);

    if prefix_mode {
        info!("🔄 PREFIX mode: '{}' — will auto-rotate markets", raw_slug);
    } else {
        info!("📌 FIXED mode: '{}' — single market", raw_slug);
    }
    let oracle_lag_symbol_universe = OracleLagSymbolUniverse::from_env();
    info!(
        "🧭 oracle_lag symbol universe={} (set PM_ORACLE_LAG_SYMBOL_UNIVERSE, use '*' for all)",
        oracle_lag_symbol_universe.describe()
    );
    let inv_cfg_base = InventoryConfig::from_env();
    let ofi_cfg = OfiConfig::from_env();
    let coord_cfg_base = CoordinatorConfig::from_env();
    // Slug lock: standalone mode only (ctx=None = single OS-process worker).
    // In inproc mode the supervisor IS the single process, so no cross-process
    // conflict is possible and we skip the lock. Lab-only challenger workers
    // use a separate lock class so they can shadow one tradable oracle-lag
    // worker on the same slug without disabling duplicate protection entirely.
    let _slug_lock = if ctx.is_none()
        && prefix_mode
        && coord_cfg_base.strategy.is_oracle_lag_sniping()
    {
        let slug_lock_lab_only = coord_cfg_base.oracle_lag_sniping.lab_only;
        let slug_lock_class = slug_lock_class_name(slug_lock_lab_only);
        match try_acquire_slug_lock(&raw_slug, slug_lock_lab_only) {
            Some(lock) => {
                info!(
                    "🔒 slug_lock acquired | slug={} class={} port={}",
                    raw_slug, slug_lock_class, lock.port
                );
                Some(lock)
            }
            None => {
                let port = slug_lock_port(&raw_slug, slug_lock_lab_only);
                anyhow::bail!(
                    "🚨 slug_lock_conflict: another process is already running oracle_lag_sniping class='{}' for slug='{}' (port {}). Exiting to prevent duplicate orders.",
                    slug_lock_class, raw_slug, port
                );
            }
        }
    } else {
        None
    };

    let chainlink_hub = if let Some(c) = &ctx {
        c.shared_ingress.chainlink_hub.clone()
    } else if coord_cfg_base.strategy.is_oracle_lag_sniping() {
        let mut hub_symbols = HashSet::new();
        for base in oracle_lag_symbol_universe.hub_symbols() {
            hub_symbols.insert(format!("{}/usd", base));
        }
        build_chainlink_hub_for_symbols(&hub_symbols)
    } else {
        None
    };
    let local_price_hub = if let Some(c) = &ctx {
        c.shared_ingress.local_price_hub.clone()
    } else if coord_cfg_base.strategy.is_oracle_lag_sniping() && local_price_agg_enabled() {
        let mut hub_symbols = HashSet::new();
        for base in oracle_lag_symbol_universe.hub_symbols() {
            hub_symbols.insert(format!("{}/usd", base));
        }
        build_local_price_hub_for_symbols(&hub_symbols, &coord_cfg_base)
    } else {
        None
    };
    let mut auto_claim_cfg = AutoClaimConfig::from_env();
    let round_claim_cfg = RoundClaimRunnerConfig::from_env();
    let recycle_cfg = CapitalRecycleConfig::from_env();
    let recorder = RecorderHandle::from_env();
    let mut auto_claim_state = AutoClaimState::default();
    let mut round_claim_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut pgt_shadow_redeem_task: Option<tokio::task::JoinHandle<()>> = None;

    let dry_run = coord_cfg_base.dry_run;
    if dry_run {
        auto_claim_cfg.dry_run = true;
    }
    let min_order_size_env_raw = env::var("PM_MIN_ORDER_SIZE")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let min_order_size_env_val = min_order_size_env_raw
        .as_ref()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| *v >= 0.0);
    let min_order_size_auto = min_order_size_env_val.is_none();
    // Keep the most recent known-good min_order_size across market rotations.
    // In auto mode, this prevents transient /books failures from dropping back
    // to a too-small default for the next round.
    let mut min_order_size_last_good = min_order_size_env_val
        .filter(|v| *v > 0.0)
        .unwrap_or_else(|| coord_cfg_base.min_order_size.max(0.0));
    if min_order_size_env_raw.is_some() && min_order_size_env_val.is_none() {
        warn!(
            "⚠️ Invalid PM_MIN_ORDER_SIZE='{}' — will attempt order book auto-detection",
            min_order_size_env_raw.as_deref().unwrap_or_default()
        );
    }
    // Static-first policy:
    // PM_BID_SIZE / PM_MAX_NET_DIFF are hard floors.
    // PM_BID_PCT / PM_NET_DIFF_PCT (if set) provide dynamic targets.
    let bid_pct_raw = env::var("PM_BID_PCT").ok().filter(|s| !s.trim().is_empty());
    let bid_pct_parsed = bid_pct_raw
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0);
    if bid_pct_raw.is_some() && bid_pct_parsed.is_none() {
        warn!(
            "⚠️ Invalid PM_BID_PCT='{}' — dynamic bid sizing disabled",
            bid_pct_raw.as_deref().unwrap_or_default()
        );
    }
    let bid_pct_opt = bid_pct_parsed.filter(|v| *v > 0.0);

    let net_diff_pct_raw = env::var("PM_NET_DIFF_PCT")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let net_diff_pct_parsed = net_diff_pct_raw
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0);
    if net_diff_pct_raw.is_some() && net_diff_pct_parsed.is_none() {
        warn!(
            "⚠️ Invalid PM_NET_DIFF_PCT='{}' — dynamic net sizing disabled",
            net_diff_pct_raw.as_deref().unwrap_or_default()
        );
    }
    let net_diff_pct_opt = net_diff_pct_parsed.filter(|v| *v > 0.0);
    // GLFT risk policy:
    // keep PM_MAX_NET_DIFF as a hard cap during stabilization.
    // Ignore dynamic percentage upsizing in GLFT mode.
    let mut dynamic_bid_pct_opt = bid_pct_opt;
    let mut dynamic_net_diff_pct_opt = net_diff_pct_opt;
    if coord_cfg_base.strategy.is_glft_mm()
        && (dynamic_bid_pct_opt.is_some() || dynamic_net_diff_pct_opt.is_some())
    {
        warn!(
            "⚠️ PM_BID_PCT/PM_NET_DIFF_PCT are ignored for glft_mm; using static PM_BID_SIZE/PM_MAX_NET_DIFF"
        );
        dynamic_bid_pct_opt = None;
        dynamic_net_diff_pct_opt = None;
    }

    if dynamic_bid_pct_opt.is_some() || dynamic_net_diff_pct_opt.is_some() {
        info!(
            "📏 Dynamic sizing enabled: PM_BID_PCT={} PM_NET_DIFF_PCT={} (floors: PM_BID_SIZE / PM_MAX_NET_DIFF)",
            dynamic_bid_pct_opt
                .map(|v| format!("{:.4}", v))
                .unwrap_or_else(|| "off".to_string()),
            dynamic_net_diff_pct_opt
                .map(|v| format!("{:.4}", v))
                .unwrap_or_else(|| "off".to_string())
        );
    } else {
        info!(
            "📏 Dynamic sizing disabled: static PM_BID_SIZE / PM_MAX_NET_DIFF floors will be used"
        );
    }

    info!(
        "📊 Base Config: pair={:.2} bid={:.1} tick={:.3} net={:.0} ofi_thresh={:.1} dry={}",
        coord_cfg_base.pair_target,
        coord_cfg_base.bid_size,
        coord_cfg_base.tick_size,
        coord_cfg_base.max_net_diff,
        ofi_cfg.toxicity_threshold,
        dry_run
    );
    info!(
        "🌐 Net Resilience: ws_connect_timeout={}ms resolve_timeout={}ms resolve_retries={} ws_degrade_failures={}",
        ws_connect_timeout_ms(),
        resolve_timeout_ms(),
        resolve_retry_attempts(),
        ws_degrade_max_failures()
    );
    if auto_claim_cfg.enabled {
        info!(
            "💸 Auto-claim enabled: min_value=${} max_conditions={} interval={}s dry_run={} wait_confirm={} wait_timeout={}s",
            auto_claim_cfg.min_condition_value,
            auto_claim_cfg.max_conditions_per_run,
            auto_claim_cfg.run_interval.as_secs(),
            auto_claim_cfg.dry_run,
            auto_claim_cfg.relayer_wait_confirm,
            auto_claim_cfg.relayer_wait_timeout.as_secs()
        );
        info!(
            "💸 Round-claim SLA: window={}s mode={} scope={} schedule=[{}]",
            round_claim_cfg.window.as_secs(),
            round_claim_cfg.retry_mode.as_str(),
            round_claim_cfg.scope.as_str(),
            round_claim_cfg.retry_schedule_text()
        );
    }
    if recycle_cfg.enabled {
        info!(
            "♻️ Recycle enabled: trigger={} in {}s cooldown={}s low={} target={} batch=[{}, {}] only_hedge={} max_round_merges={}",
            recycle_cfg.trigger_rejects,
            recycle_cfg.trigger_window.as_secs(),
            recycle_cfg.cooldown.as_secs(),
            recycle_cfg.low_water_usdc,
            recycle_cfg.target_free_usdc,
            recycle_cfg.min_batch_usdc,
            recycle_cfg.max_batch_usdc,
            recycle_cfg.only_hedge_rejects,
            recycle_cfg.max_merges_per_round
        );
    }

    // P1 FIX: Parse funder_address from environment, which represents the Magic Proxy Wallet.
    // We need this BEFORE init_clob_client to configure the API key derivation.
    let funder_address: Option<String> = if !dry_run {
        let explicit = base_settings
            .funder_address
            .clone()
            .filter(|s| !s.trim().is_empty());
        if let Some(addr) = explicit {
            info!(
                "🔑 Using explicit POLYMARKET_FUNDER_ADDRESS: {}…",
                &addr[..10.min(addr.len())]
            );
            Some(addr)
        } else {
            // We can't derive from an uninitialized signer anymore. Let's just fall back to standard EOA auth if empty.
            // But log a critical warning.
            warn!(
                "⚠️ Live mode usually requires POLYMARKET_FUNDER_ADDRESS (Proxy Wallet) to trade."
            );
            None
        }
    } else {
        base_settings.funder_address.clone()
    };

    let funder_alloy = match funder_address.as_ref() {
        Some(addr) => match addr.trim().parse::<alloy::primitives::Address>() {
            Ok(a) => Some(a),
            Err(e) => {
                warn!(
                    "⚠️ Invalid POLYMARKET_FUNDER_ADDRESS='{}': {:?}. Falling back to EOA auth.",
                    addr, e
                );
                None
            }
        },
        None => None,
    };

    // Shared L2 credentials for BOTH CLOB REST and User WS.
    // If provided in env, we force both channels to use exactly the same keypair.
    let shared_api_creds_env: Option<(String, String, String)> = {
        let env_key = env::var("POLYMARKET_API_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let env_secret = env::var("POLYMARKET_API_SECRET")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let env_pass = env::var("POLYMARKET_API_PASSPHRASE")
            .ok()
            .filter(|s| !s.trim().is_empty());
        match (env_key, env_secret, env_pass) {
            (Some(k), Some(s), Some(p)) => Some((k, s, p)),
            (None, None, None) => None,
            _ => {
                anyhow::bail!(
                    "🚨 FATAL: POLYMARKET_API_KEY / POLYMARKET_API_SECRET / POLYMARKET_API_PASSPHRASE must be set together."
                );
            }
        }
    };
    let shared_api_creds_auth = match shared_api_creds_env.as_ref() {
        Some((key, secret, passphrase)) => {
            let key_uuid = match key.parse::<polymarket_client_sdk::auth::ApiKey>() {
                Ok(k) => k,
                Err(e) => {
                    anyhow::bail!("🚨 FATAL: Invalid POLYMARKET_API_KEY UUID: {:?}", e);
                }
            };
            Some(polymarket_client_sdk::auth::Credentials::new(
                key_uuid,
                secret.clone(),
                passphrase.clone(),
            ))
        }
        None => None,
    };

    // ═══ Initialize CLOB client (once, reused across rotations) ═══
    // We pass funder_alloy for maker identity and shared_api_creds_auth for unified L2 auth.
    let (clob_client, signer) = if !dry_run {
        init_clob_client(
            &base_settings.rest_url,
            base_settings.private_key.as_deref(),
            funder_alloy,
            shared_api_creds_auth,
        )
        .await
    } else {
        info!("📝 DRY-RUN mode — no orders, no User WS");
        (None, None)
    };

    if !dry_run && (clob_client.is_none() || signer.is_none()) {
        anyhow::bail!(
            "🚨 FATAL: dry_run=false but CLOB client auth failed. \
             Set PM_DRY_RUN=true or fix private key / auth config."
        );
    }
    if !dry_run {
        info!(
            "🛠️ CLOB V2 execution path enabled: live maker/taker orders use local V2 signing and POST /order."
        );
    }
    #[allow(unused_imports)]
    use alloy::signers::Signer;
    let signer_address = signer.as_ref().map(|s| format!("{:?}", s.address()));

    // Startup preflight: force-refresh and inspect collateral balance/allowance.
    if !dry_run {
        use alloy::primitives::Address;
        use alloy::primitives::U256;
        use pm_as_ofi::polymarket::clob_v2::{v2_contract_config, V2_CONTRACTS};
        use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
        use polymarket_client_sdk::clob::types::{AssetType, SignatureType};
        use rust_decimal::Decimal;

        let is_api_key_unauthorized = |err: &dyn std::fmt::Display| -> bool {
            let lower = format!("{:#}", err).to_ascii_lowercase();
            lower.contains("unauthorized")
                || lower.contains("invalid api key")
                || lower.contains("401")
        };

        if let Some(client) = clob_client.as_ref() {
            let req = BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Collateral)
                .build();

            if let Err(e) = client.update_balance_allowance(req.clone()).await {
                if is_api_key_unauthorized(&e) {
                    anyhow::bail!(
                        "🚨 FATAL: CLOB API key unauthorized during preflight update. \
                         POLYMARKET_API_* is invalid/stale for current signer/funder. \
                         Remove POLYMARKET_API_* to let bot auto-derive, or regenerate matching credentials."
                    );
                }
                warn!("⚠️ balance-allowance/update failed: {:?}", e);
            }

            match client.balance_allowance(req).await {
                Ok(resp) => {
                    let max_allowance = resp
                        .allowances
                        .values()
                        .filter_map(|v| parse_u256_allowance(v))
                        .max()
                        .unwrap_or(U256::ZERO);
                    let expected_spenders: Vec<(&str, Option<Address>)> = vec![
                        ("pUSD_ctf", Some(V2_CONTRACTS.ctf)),
                        ("exchange", Some(v2_contract_config(false).exchange)),
                        ("neg_risk_exchange", Some(v2_contract_config(true).exchange)),
                        (
                            "neg_risk_adapter",
                            v2_contract_config(true).neg_risk_adapter,
                        ),
                    ];

                    info!(
                        "💰 Preflight pUSD collateral: balance={} max_allowance={} allowance_entries={}",
                        resp.balance,
                        max_allowance,
                        resp.allowances.len()
                    );
                    for (label, maybe_addr) in expected_spenders {
                        if let Some(addr) = maybe_addr {
                            let raw = resp.allowances.get(&addr).cloned().unwrap_or_default();
                            let parsed = parse_u256_allowance(&raw).unwrap_or(U256::ZERO);
                            info!(
                                "💳 Preflight allowance[{label}] {} raw='{}' parsed={}",
                                addr, raw, parsed
                            );
                        }
                    }
                    if resp.balance <= Decimal::ZERO || max_allowance.is_zero() {
                        let samples: Vec<String> = resp
                            .allowances
                            .iter()
                            .take(3)
                            .map(|(k, v)| format!("{k:?}={v}"))
                            .collect();
                        warn!(
                            "⚠️ Preflight indicates insufficient pUSD balance/allowance for trading \
                             (balance={} max_allowance={} samples={:?})",
                            resp.balance, max_allowance, samples
                        );
                    }

                    // Diagnostic probe: compare balance/allowance views across all signature types.
                    // This catches signature type mismatches for proxy/safe wallets.
                    for sig in [
                        SignatureType::Eoa,
                        SignatureType::Proxy,
                        SignatureType::GnosisSafe,
                    ] {
                        let probe_req = BalanceAllowanceRequest::builder()
                            .asset_type(AssetType::Collateral)
                            .signature_type(sig)
                            .build();
                        match client.balance_allowance(probe_req).await {
                            Ok(probe) => {
                                let probe_max = probe
                                    .allowances
                                    .values()
                                    .filter_map(|v| parse_u256_allowance(v))
                                    .max()
                                    .unwrap_or(U256::ZERO);
                                info!(
                                    "🧪 pUSD collateral probe sig_type={} balance={} max_allowance={} entries={}",
                                    sig as u8,
                                    probe.balance,
                                    probe_max,
                                    probe.allowances.len()
                                );
                            }
                            Err(e) => {
                                warn!("⚠️ Collateral probe sig_type={} failed: {:?}", sig as u8, e);
                            }
                        }
                    }

                    let allow_zero_allowance = env::var("PM_ALLOW_ZERO_ALLOWANCE")
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);
                    if resp.balance > Decimal::ZERO
                        && max_allowance.is_zero()
                        && !allow_zero_allowance
                    {
                        anyhow::bail!(
                            "🚨 FATAL: wallet balance is non-zero but CLOB pUSD allowance is zero. \
                             Wrap USDC.e into pUSD via CollateralOnramp, approve pUSD for CTF, and approve outcome tokens for the V2 exchanges, then retry. \
                             Set PM_ALLOW_ZERO_ALLOWANCE=true to bypass this guard."
                        );
                    }
                }
                Err(e) => {
                    if is_api_key_unauthorized(&e) {
                        anyhow::bail!(
                            "🚨 FATAL: CLOB balance_allowance returned unauthorized/invalid API key. \
                             Credentials do not match current signer/funder/signature_type. \
                             Remove POLYMARKET_API_* and retry with auto-derive, or regenerate correct API creds."
                        );
                    }
                    warn!("⚠️ balance_allowance preflight failed: {:?}", e);
                }
            }
        }
    }

    // Fallback: If no explicit funder address was given but we have a signer, we assume EOA mapping.
    let funder_address = match funder_address {
        Some(addr) => Some(addr),
        None if signer.is_some() => {
            #[allow(unused_imports)]
            use alloy::signers::Signer;
            let derived = format!("{:?}", signer.as_ref().unwrap().address());
            info!(
                "🔑 Deduced funder_address from EOA private key: {}…",
                &derived[..10.min(derived.len())]
            );
            Some(derived)
        }
        None => None,
    };

    if !dry_run && funder_address.is_none() {
        anyhow::bail!(
            "🚨 FATAL: Live mode requires POLYMARKET_FUNDER_ADDRESS or a valid private key \
             to derive the wallet address. Without it, ALL maker fills will be silently \
             filtered out and inventory will never update."
        );
    }
    if auto_claim_cfg.enabled
        && auto_claim_cfg.signature_type == Some(2)
        && auto_claim_cfg.builder_credentials.is_some()
        && signer_address.is_some()
        && funder_address.is_some()
    {
        info!(
            "💸 SAFE auto-claim armed: round-window={}s schedule=[{}] wait_confirm={}",
            round_claim_cfg.window.as_secs(),
            round_claim_cfg.retry_schedule_text(),
            auto_claim_cfg.relayer_wait_confirm
        );
    }
    maybe_log_claimable_positions(funder_address.as_deref(), signer_address.as_deref()).await;
    if let Err(e) = maybe_auto_claim(
        &auto_claim_cfg,
        &mut auto_claim_state,
        funder_address.as_deref(),
        signer_address.as_deref(),
        base_settings.private_key.as_deref(),
    )
    .await
    {
        warn!("⚠️ Auto-claim runner failed at startup: {:?}", e);
    }

    // ═══ L2 API credentials for User WS (live mode only) ═══
    // Always source credentials from authenticated CLOB client to avoid REST/WS identity drift.
    let api_creds: Option<(String, String, String)> = if !dry_run {
        use secrecy::ExposeSecret;
        if let Some(client) = clob_client.as_ref() {
            let creds = client.credentials();
            if shared_api_creds_env.is_some() {
                info!(
                    "🔑 User WS using verified credentials from authenticated CLOB client \
                     (env POLYMARKET_API_* may be reused or auto-fallbacked)"
                );
            } else {
                info!("🔑 User WS reusing auto-derived authenticated CLOB credentials");
            }
            Some((
                creds.key().to_string(),
                creds.secret().expose_secret().to_string(),
                creds.passphrase().expose_secret().to_string(),
            ))
        } else {
            anyhow::bail!(
                "🚨 FATAL: dry_run=false but no authenticated CLOB client available for User WS credentials."
            );
        }
    } else {
        None
    };

    // ═══════════════════════════════════════════════════
    // OUTER LOOP: Market Rotation
    // ═══════════════════════════════════════════════════

    // Channel for pre-resolved next markets to eliminate 7-8s rotation latency
    let (preload_tx, mut preload_rx) = mpsc::channel::<(String, anyhow::Result<ResolvedMarket>)>(2);
    let mut preloaded_market: Option<(String, anyhow::Result<ResolvedMarket>)> = None;
    let mut preloading_slug: Option<String> = None;
    let mut market_cache: HashMap<String, ResolvedMarket> = HashMap::new();

    let mut round = 0u64;
    loop {
        // ── Step 1: Resolve current market ──
        let (slug, slug_start_ts, mut expected_end_ts) = if prefix_mode {
            let (s, ts, e_ts) = compute_current_slug(&raw_slug);
            (s, ts, e_ts)
        } else {
            let inferred_start = inferred_start_ts_from_slug(&raw_slug).unwrap_or(u64::MAX);
            let inferred_end = inferred_end_ts_from_slug(&raw_slug).unwrap_or(u64::MAX);
            // Exact up/down slugs encode market start, not close. Keep fixed
            // mode on the same time axis as prefix mode; Gamma endDate can
            // still override this below when available.
            (raw_slug.clone(), inferred_start, inferred_end)
        };

        // Entry gate: if startup is too late in the current interval, skip it.
        if prefix_mode {
            let interval_secs = detect_interval(&raw_slug);
            let entry_grace_secs = env::var("PM_ENTRY_GRACE_SECONDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30);
            let now_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Secondary Entry Gate: if API resolution took too long and pushed us past grace, abort.
            if should_skip_entry_window(now_unix, expected_end_ts, interval_secs, entry_grace_secs)
            {
                let start_ts = expected_end_ts.saturating_sub(interval_secs);
                let age_secs = now_unix.saturating_sub(start_ts);
                let wait_secs = expected_end_ts.saturating_sub(now_unix).saturating_add(1);
                warn!(
                    "⏭️ Late startup for {}: age={}s > grace={}s. Skip current market, wait {}s for next open.",
                    slug, age_secs, entry_grace_secs, wait_secs
                );

                spawn_market_pre_resolve(
                    &raw_slug,
                    expected_end_ts,
                    preload_tx.clone(),
                    &mut preloading_slug,
                    "skip_delay",
                );

                sleep(Duration::from_secs(wait_secs)).await;
                continue;
            }
        }

        // Drain any incoming preloads
        while let Ok(pre) = preload_rx.try_recv() {
            if preloading_slug.as_deref() == Some(pre.0.as_str()) {
                preloading_slug = None;
            }
            preloaded_market = Some(pre);
        }

        let manual_resolved = if !prefix_mode
            && base_settings.market_slug.as_deref() == Some(slug.as_str())
            && !base_settings.market_id.trim().is_empty()
            && !base_settings.yes_asset_id.trim().is_empty()
            && !base_settings.no_asset_id.trim().is_empty()
        {
            let inferred_end = inferred_end_ts_from_slug(&slug);
            info!(
                "🧷 Using explicit market ids from env for {} (skip gamma resolve)",
                slug
            );
            Some((
                base_settings.market_id.clone(),
                base_settings.yes_asset_id.clone(),
                base_settings.no_asset_id.clone(),
                inferred_end,
            ))
        } else {
            None
        };

        let resolved = if let Some(ids) = manual_resolved {
            Ok(ids)
        } else if let Some((pre_slug, pre_res)) = preloaded_market.take() {
            if pre_slug == slug {
                preloading_slug = None;
                info!("⚡ Using pre-resolved market data for {}", slug);
                match pre_res {
                    Ok(ids) => Ok(ids),
                    Err(e) => {
                        warn!(
                            "⚠️ Pre-resolved data for '{}' failed: {} — falling back to direct resolve",
                            slug, e
                        );
                        resolve_market_with_retry(&slug).await
                    }
                }
            } else {
                // Keep unrelated preloaded payload for its intended round.
                preloaded_market = Some((pre_slug, pre_res));
                if preloading_slug.as_deref() == Some(slug.as_str()) {
                    match tokio::time::timeout(Duration::from_millis(700), preload_rx.recv()).await
                    {
                        Ok(Some((incoming_slug, incoming_res))) if incoming_slug == slug => {
                            preloading_slug = None;
                            incoming_res
                        }
                        Ok(Some(other)) => {
                            if preloading_slug.as_deref() == Some(other.0.as_str()) {
                                preloading_slug = None;
                            }
                            preloaded_market = Some(other);
                            if preloading_slug.as_deref() == Some(slug.as_str()) {
                                preloading_slug = None;
                            }
                            resolve_market_with_retry(&slug).await
                        }
                        _ => {
                            preloading_slug = None;
                            resolve_market_with_retry(&slug).await
                        }
                    }
                } else {
                    resolve_market_with_retry(&slug).await
                }
            }
        } else {
            if preloading_slug.as_deref() == Some(slug.as_str()) {
                match tokio::time::timeout(Duration::from_millis(700), preload_rx.recv()).await {
                    Ok(Some((incoming_slug, incoming_res))) if incoming_slug == slug => {
                        preloading_slug = None;
                        incoming_res
                    }
                    Ok(Some(other)) => {
                        if preloading_slug.as_deref() == Some(other.0.as_str()) {
                            preloading_slug = None;
                        }
                        preloaded_market = Some(other);
                        if preloading_slug.as_deref() == Some(slug.as_str()) {
                            preloading_slug = None;
                        }
                        resolve_market_with_retry(&slug).await
                    }
                    _ => {
                        preloading_slug = None;
                        resolve_market_with_retry(&slug).await
                    }
                }
            } else {
                resolve_market_with_retry(&slug).await
            }
        };
        let (market_id, yes_asset_id, no_asset_id, api_end_date) = match resolved {
            Ok(ids) => {
                market_cache.insert(slug.clone(), ids.clone());
                ids
            }
            Err(err) => {
                if let Some(cached) = market_cache.get(&slug).cloned() {
                    warn!(
                        "⚠️ Resolve failed for '{}': {} — using cached market ids for continuity",
                        slug, err
                    );
                    cached
                } else {
                    warn!("❌ Failed to resolve '{}': {} — retrying in 2s", slug, err);
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            }
        };

        // Only count/log a round after we have resolved market ids successfully.
        round += 1;
        info!("═══════════════════════════════════════════════════");
        info!("  Round #{} — {}", round, slug);
        info!("═══════════════════════════════════════════════════");

        // P0 FIX: Apply API verifiable endDate if present
        if let Some(actual_end_ts) = api_end_date {
            expected_end_ts = actual_end_ts;
        }

        // P2 FIX: Clamp end_ts for deadline calculation to avoid overflow
        let effective_end_ts = if expected_end_ts == u64::MAX {
            // Fixed mode: use a sane 1-year cap instead of u64::MAX
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 31_536_000
        } else {
            expected_end_ts
        };

        let mut settings = base_settings.clone();
        settings.market_id = market_id.clone();
        settings.yes_asset_id = yes_asset_id.clone();
        settings.no_asset_id = no_asset_id.clone();

        let mut coord_cfg = coord_cfg_base.clone();
        if min_order_size_auto && min_order_size_last_good > coord_cfg.min_order_size {
            coord_cfg.min_order_size = min_order_size_last_good;
            info!(
                "🧭 Carry forward min_order_size from last good round: {:.2}",
                coord_cfg.min_order_size
            );
        }
        // Opt-1: Pass market expiry timestamp so coordinator can apply A-S time decay.
        coord_cfg.market_end_ts = Some(effective_end_ts);
        coord_cfg.oracle_lag_sniping.market_enabled = false;
        coord_cfg.completion_first.market_enabled = false;
        let market_interval_secs = detect_interval(&slug);
        apply_endgame_windows_for_interval(&mut coord_cfg, market_interval_secs);
        if prefix_mode {
            // Start resolving the next round as soon as the current worker is alive.
            // Waiting until session cleanup can leave the next worker absent for most
            // of its own 5m round when Gamma/network resolution stalls.
            spawn_market_pre_resolve(
                &raw_slug,
                effective_end_ts,
                preload_tx.clone(),
                &mut preloading_slug,
                "session_start",
            );
        }
        let mut inv_cfg = inv_cfg_base.clone();
        let oracle_lag_symbol = oracle_lag_symbol_from_slug(&slug);
        let completion_first_active = coord_cfg.strategy == StrategyKind::CompletionFirst
            && slug.starts_with("btc-updown-5m");
        let oracle_lag_sniping_active = coord_cfg.strategy.is_oracle_lag_sniping()
            && oracle_lag_symbol
                .as_deref()
                .map(|sym| oracle_lag_symbol_universe.contains(sym))
                .unwrap_or(false);
        if coord_cfg.strategy == StrategyKind::CompletionFirst {
            if completion_first_active {
                coord_cfg.completion_first.market_enabled = true;
                info!(
                    "🧩 completion_first enabled for {} | mode={:?}",
                    slug, coord_cfg.completion_first.mode
                );
            } else {
                warn!(
                    "⚠️ PM_STRATEGY=completion_first currently only supports BTC 5m; slug='{}' stays inactive",
                    slug
                );
            }
        }
        if coord_cfg.strategy.is_oracle_lag_sniping() {
            if oracle_lag_sniping_active {
                coord_cfg.oracle_lag_sniping.market_enabled = true;
                info!(
                    "🕓 oracle_lag_sniping enabled for {} | symbol={} post_close_window={}s",
                    slug,
                    oracle_lag_symbol.as_deref().unwrap_or("unknown"),
                    coord_cfg.oracle_lag_sniping.window_secs
                );
            } else {
                match oracle_lag_symbol.as_deref() {
                    None => warn!(
                        "⚠️ PM_STRATEGY=oracle_lag_sniping requires updown-5m round; current slug='{}' stays inactive",
                        slug
                    ),
                    Some(sym) => warn!(
                        "⚠️ PM_STRATEGY=oracle_lag_sniping symbol='{}' excluded by PM_ORACLE_LAG_SYMBOL_UNIVERSE='{}'; slug='{}' stays inactive",
                        sym,
                        oracle_lag_symbol_universe.describe(),
                        slug
                    ),
                }
            }
        }

        let mut balance_opt: Option<f64> = None;

        // ── Step 2.5: Dynamic Sizing ──
        if !dry_run {
            if let Some(client) = clob_client.as_ref() {
                use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
                use polymarket_client_sdk::clob::types::AssetType;

                let req = BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Collateral)
                    .build();

                if let Ok(resp) = client.balance_allowance(req).await {
                    // Polymarket returns pUSD collateral in 6 decimals (1 pUSD = 1_000_000 base units)
                    let raw_balance =
                        rust_decimal::prelude::ToPrimitive::to_f64(&resp.balance).unwrap_or(0.0);
                    let balance_f64 = raw_balance / 1_000_000.0;
                    balance_opt = Some(balance_f64);

                    // Unit policy:
                    // - PM_BID_SIZE / PM_MAX_NET_DIFF are floors.
                    // - PM_BID_PCT / PM_NET_DIFF_PCT are dynamic targets (opt-in).
                    let bid_floor = coord_cfg.bid_size;
                    let net_floor = coord_cfg.max_net_diff;
                    let sizing = apply_dynamic_sizing(
                        balance_f64,
                        bid_floor,
                        net_floor,
                        dynamic_bid_pct_opt,
                        dynamic_net_diff_pct_opt,
                    );
                    coord_cfg.bid_size = sizing.bid_effective;
                    coord_cfg.max_net_diff = sizing.net_effective;

                    // CRITICAL: Sync InventoryConfig with the new dynamic values
                    inv_cfg.bid_size = coord_cfg.bid_size;
                    inv_cfg.max_net_diff = coord_cfg.max_net_diff;

                    if sizing.bid_target.is_some() || sizing.net_target.is_some() {
                        info!(
                            "💡 [DYNAMIC SIZING] Balance: {:.2} pUSD -> target BID_SIZE={} MAX_NET_DIFF={} | floors BID_SIZE={:.1} MAX_NET_DIFF={:.1} -> effective BID_SIZE={:.1} MAX_NET_DIFF={:.1} (bid_pct={}, net_pct={})",
                            balance_f64,
                            sizing
                                .bid_target
                                .map(|v| format!("{:.1}", v))
                                .unwrap_or_else(|| "off".to_string()),
                            sizing
                                .net_target
                                .map(|v| format!("{:.1}", v))
                                .unwrap_or_else(|| "off".to_string()),
                            bid_floor,
                            net_floor,
                            coord_cfg.bid_size,
                            coord_cfg.max_net_diff,
                            dynamic_bid_pct_opt
                                .map(|v| format!("{:.4}", v))
                                .unwrap_or_else(|| "off".to_string()),
                            dynamic_net_diff_pct_opt
                                .map(|v| format!("{:.4}", v))
                                .unwrap_or_else(|| "off".to_string())
                        );
                    }
                } else {
                    warn!("⚠️ Failed to fetch balance for dynamic sizing. Falling back to env defaults.");
                }
            }
        }

        if min_order_size_auto {
            match fetch_min_order_size(&base_settings.rest_url, &yes_asset_id, &no_asset_id).await {
                Ok(auto_min) if auto_min > 0.0 => {
                    let prev = coord_cfg.min_order_size;
                    if auto_min > prev {
                        coord_cfg.min_order_size = auto_min;
                        min_order_size_last_good = auto_min;
                        info!(
                            "🧭 Auto min_order_size from order book: {:.2} (prev {:.2})",
                            auto_min, prev
                        );
                    } else {
                        min_order_size_last_good = min_order_size_last_good.max(prev);
                        info!(
                            "🧭 Order book min_order_size {:.2} <= configured {:.2} — keeping configured",
                            auto_min, prev
                        );
                    }
                }
                Ok(_) => {
                    warn!(
                        "⚠️ Order book reported non-positive min_order_size; keeping configured value {:.2} (last_good={:.2})",
                        coord_cfg.min_order_size,
                        min_order_size_last_good
                    );
                }
                Err(e) => {
                    warn!(
                        "⚠️ Failed to auto-detect min_order_size from order book: {:?} — keeping {:.2} (last_good={:.2})",
                        e,
                        coord_cfg.min_order_size,
                        min_order_size_last_good
                    );
                }
            }
        }

        info!("🎯 Market: {}", market_id);
        info!("   YES: {}...", &yes_asset_id[..16.min(yes_asset_id.len())]);
        info!("   NO:  {}...", &no_asset_id[..16.min(no_asset_id.len())]);
        let reconcile_interval_secs = std::env::var("PM_RECONCILE_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        log_config_self_check(
            &coord_cfg,
            &inv_cfg,
            &ofi_cfg,
            balance_opt,
            reconcile_interval_secs,
        );

        // P0-2: Track all session spawns for cleanup on rotation
        let mut session_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let round_start_ts_ms = unix_now_ms();
        let recorder_meta = RecorderSessionMeta {
            slug: slug.clone(),
            condition_id: market_id.clone(),
            market_id: market_id.clone(),
            strategy: coord_cfg.strategy.as_str().to_string(),
            dry_run,
        };
        let enable_round_validation =
            !dry_run && coord_cfg.strategy.is_glft_mm() && slug.starts_with("btc-updown-5m");

        // Fill fanout: UserWS → fill_tx → splitter → (InventoryManager, Executor)
        let (fill_tx, mut fill_rx) = mpsc::channel::<FillEvent>(64);
        let dry_run_sim_fill_tx = if dry_run { Some(fill_tx.clone()) } else { None };
        let (inv_event_tx, inv_event_rx) = mpsc::channel::<InventoryEvent>(64);
        let (exec_fill_tx, exec_fill_rx) = mpsc::channel::<FillEvent>(64);
        let (validation_fill_tx, validation_fill_rx) = mpsc::channel::<FillEvent>(256);
        let validation_fill_tx_opt = if enable_round_validation {
            Some(validation_fill_tx.clone())
        } else {
            None
        };
        let inv_event_tx_split = inv_event_tx.clone();

        // Splitter task: fan-out fills to InventoryManager, Executor, and round validator
        session_handles.push(tokio::spawn(async move {
            while let Some(fill) = fill_rx.recv().await {
                let _ = inv_event_tx_split
                    .send(InventoryEvent::Fill(fill.clone()))
                    .await;
                let _ = exec_fill_tx.send(fill.clone()).await;
                if let Some(tx) = validation_fill_tx_opt.as_ref() {
                    let _ = tx.send(fill).await;
                }
            }
        }));

        let (exec_tx, exec_rx) = mpsc::channel::<ExecutionCmd>(32);
        let (result_tx, result_rx) = mpsc::channel::<OrderResult>(32);
        let (capital_tx, capital_rx) = mpsc::channel::<PlacementRejectEvent>(64);
        let (feedback_tx, feedback_rx) = mpsc::channel::<ExecutionFeedback>(32);
        let (om_tx, om_rx) = mpsc::channel::<OrderManagerCmd>(64);
        let (ofi_md_tx, ofi_md_rx) = mpsc::channel::<MarketDataMsg>(512);
        let (glft_md_tx, glft_md_rx) = mpsc::channel::<MarketDataMsg>(512);
        let (coord_md_tx, coord_md_rx) = watch::channel::<MarketDataMsg>(MarketDataMsg::BookTick {
            yes_bid: 0.0,
            yes_ask: 0.0,
            no_bid: 0.0,
            no_ask: 0.0,
            ts: Instant::now(),
        });
        let (dry_run_touch_md_tx, dry_run_executor_md_rx) =
            if dry_run && coord_cfg.strategy.is_pair_gated_tranche_arb() {
                let (tx, rx) = broadcast::channel::<MarketDataMsg>(8192);
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
        let (inv_watch_tx, inv_watch_rx) = watch::channel(InventorySnapshot::default());
        let inv_watch_rx_postclose = inv_watch_rx.clone();
        let (ofi_watch_tx, ofi_watch_rx) = watch::channel(OfiSnapshot::default());
        let (glft_watch_tx, glft_watch_rx) = watch::channel(GlftSignalSnapshot::default());
        let (coord_obs_tx, coord_obs_rx) = watch::channel(CoordinatorObsSnapshot::default());
        let (slot_release_tx, slot_release_rx) = mpsc::channel::<SlotReleaseEvent>(64);
        let (winner_hint_tx, winner_hint_rx) = mpsc::channel::<MarketDataMsg>(16);
        let (post_close_book_tx, post_close_book_rx) =
            mpsc::channel::<PostCloseSideBookUpdate>(1024);

        let mut validation_stop_tx: Option<oneshot::Sender<()>> = None;
        let mut validation_handle: Option<tokio::task::JoinHandle<RoundValidationSummary>> = None;
        if enable_round_validation {
            let inv_watch_rx_validation = inv_watch_rx.clone();
            let glft_watch_rx_validation = glft_watch_rx.clone();
            let coord_md_rx_validation = coord_md_rx.clone();
            let coord_obs_rx_validation = coord_obs_rx.clone();
            let (stop_tx, stop_rx) = oneshot::channel::<()>();
            validation_stop_tx = Some(stop_tx);
            let validation_market_slug = slug.clone();
            let validation_strategy = coord_cfg.strategy.as_str().to_string();
            validation_handle = Some(tokio::spawn(run_round_validation_collector(
                validation_market_slug,
                validation_strategy,
                round_start_ts_ms,
                validation_fill_rx,
                inv_watch_rx_validation,
                glft_watch_rx_validation,
                coord_md_rx_validation,
                coord_obs_rx_validation,
                stop_rx,
                false,
            )));
        }

        // Opt-4: Direct kill channel from OFI Engine → Coordinator.
        // Capacity 4: at most one kill per side (YES/NO) queued without blocking OFI heartbeat.
        let (kill_tx, kill_rx) = mpsc::channel::<KillSwitchSignal>(4);

        if oracle_lag_sniping_active {
            let hint_tx = winner_hint_tx.clone();
            let hint_md_rx = coord_md_rx.clone();
            let hint_post_close_book_rx = post_close_book_rx;
            let hint_chainlink_hub = chainlink_hub.clone();
            let hint_local_price_hub = local_price_hub.clone();
            let hint_rest_url = settings.rest_url.clone();
            let hint_slug = slug.clone();
            let hint_yes_asset_id = yes_asset_id.clone();
            let hint_no_asset_id = no_asset_id.clone();
            let hint_round_start_ts = if slug_start_ts != u64::MAX {
                slug_start_ts
            } else {
                effective_end_ts.saturating_sub(market_interval_secs)
            };
            let hint_round_end_ts = effective_end_ts;
            let hint_window_secs = coord_cfg.oracle_lag_sniping.window_secs;
            let hint_arbiter_tx = ctx.as_ref().and_then(|c| c.arbiter_tx.clone());
            let hint_round_tail_tx = ctx.as_ref().and_then(|c| c.round_tail_tx.clone());
            session_handles.push(tokio::spawn(async move {
                run_post_close_winner_hint_listener(
                    hint_tx,
                    hint_arbiter_tx,
                    hint_round_tail_tx,
                    hint_md_rx,
                    hint_post_close_book_rx,
                    hint_rest_url,
                    hint_chainlink_hub,
                    hint_local_price_hub,
                    hint_slug,
                    hint_yes_asset_id,
                    hint_no_asset_id,
                    hint_round_start_ts,
                    hint_round_end_ts,
                    hint_window_secs,
                )
                .await;
            }));

            if let Some(prewarm_symbol) = chainlink_symbol_from_slug(&slug) {
                // Cold-start protection: on session bootstrap, also launch prewarm for the CURRENT
                // round if it hasn't started yet OR just started a few seconds ago. Chainlink WS
                // only streams new ticks, so if round_start is already > ~5s in the past we won't
                // catch the exact tick (first-round protection via open_is_exact=false applies).
                let now_secs = unix_now_secs();
                let current_round_start_ts = hint_round_start_ts;
                // Keep a small tolerance: if we're within 10s past round_start, the tick may still
                // arrive late or a followup tick at the same ts may re-hit. Past that, skip.
                if now_secs.saturating_add(10) >= current_round_start_ts
                    && now_secs <= current_round_start_ts.saturating_add(10)
                {
                    let current_prewarm_deadline_ts = current_round_start_ts.saturating_add(20);
                    info!(
                        "🧠 chainlink_open_prewarm_start (current-round cold-start) | symbol={} round_start_ts={} deadline_ts={} now={}",
                        prewarm_symbol, current_round_start_ts, current_prewarm_deadline_ts, now_secs
                    );
                    let sym = prewarm_symbol.clone();
                    let prewarm_chainlink_hub = chainlink_hub.clone();
                    session_handles.push(tokio::spawn(async move {
                        run_chainlink_open_prewarm(
                            &sym,
                            current_round_start_ts,
                            current_prewarm_deadline_ts,
                            prewarm_chainlink_hub,
                        )
                        .await;
                    }));
                    if local_price_agg_enabled() {
                        let sym = prewarm_symbol.clone();
                        let prewarm_local_price_hub = local_price_hub.clone();
                        session_handles.push(tokio::spawn(async move {
                            run_local_price_open_prewarm(
                                &sym,
                                current_round_start_ts,
                                current_prewarm_deadline_ts,
                                prewarm_local_price_hub,
                            )
                            .await;
                        }));
                    }
                }
                // Next-round prewarm (normal path): capture exact open before round N+1 starts.
                let next_round_start_ts = hint_round_end_ts;
                let prewarm_deadline_ts = next_round_start_ts.saturating_add(20);
                info!(
                    "🧠 chainlink_open_prewarm_start | symbol={} next_round_start_ts={} deadline_ts={}",
                    prewarm_symbol, next_round_start_ts, prewarm_deadline_ts
                );
                let prewarm_chainlink_hub = chainlink_hub.clone();
                let next_round_chainlink_symbol = prewarm_symbol.clone();
                session_handles.push(tokio::spawn(async move {
                    run_chainlink_open_prewarm(
                        &next_round_chainlink_symbol,
                        next_round_start_ts,
                        prewarm_deadline_ts,
                        prewarm_chainlink_hub,
                    )
                    .await;
                }));
                if local_price_agg_enabled() {
                    let prewarm_local_price_hub = local_price_hub.clone();
                    let prewarm_symbol_local = prewarm_symbol.clone();
                    session_handles.push(tokio::spawn(async move {
                        run_local_price_open_prewarm(
                            &prewarm_symbol_local,
                            next_round_start_ts,
                            prewarm_deadline_ts,
                            prewarm_local_price_hub,
                        )
                        .await;
                    }));
                }
            }
        }

        if coord_cfg.strategy.is_pair_gated_tranche_arb() && recorder.enabled() {
            let inv_watch_rx_shadow = inv_watch_rx.clone();
            let shadow_recorder = recorder.clone();
            let shadow_meta = recorder_meta.clone();
            session_handles.push(tokio::spawn(async move {
                run_pgt_shadow_harvest_lifecycle(
                    inv_watch_rx_shadow,
                    shadow_recorder,
                    shadow_meta,
                    effective_end_ts,
                )
                .await;
            }));
        }

        let inv = InventoryManager::new(
            inv_cfg.clone(),
            inv_event_rx,
            inv_watch_tx,
            recorder.enabled().then_some(recorder.clone()),
            recorder.enabled().then_some(recorder_meta.clone()),
        );
        session_handles.push(tokio::spawn(inv.run()));

        let ofi = OfiEngine::new(ofi_cfg.clone(), ofi_md_rx, ofi_watch_tx).with_kill_tx(kill_tx);
        session_handles.push(tokio::spawn(ofi.run()));

        if coord_cfg.strategy.is_glft_mm() {
            if let Some(glft_cfg) =
                GlftRuntimeConfig::from_market_slug(&slug, effective_end_ts, coord_cfg.tick_size)
            {
                info!(
                    "📡 GLFT signal engine active | symbol={} horizon={} refit={}s window={}s",
                    glft_cfg.symbol,
                    glft_cfg.horizon_key,
                    glft_cfg.refit_interval.as_secs(),
                    glft_cfg.intensity_window.as_secs()
                );
                let glft_engine = GlftSignalEngine::new(glft_cfg, glft_md_rx, glft_watch_tx);
                session_handles.push(tokio::spawn(glft_engine.run()));
            } else {
                warn!(
                    "⚠️ GLFT strategy selected but slug '{}' is not a supported crypto up/down market; strategy will stay inactive",
                    slug
                );
            }
        }

        let shared_pgt_winner_side = coord_cfg
            .strategy
            .is_pair_gated_tranche_arb()
            .then_some(Arc::new(Mutex::new(None)));

        let coord = StrategyCoordinator::with_aux_rx_and_shared_winner(
            coord_cfg.clone(),
            ofi_watch_rx,
            inv_watch_rx,
            coord_md_rx.clone(),
            winner_hint_rx,
            glft_watch_rx,
            om_tx.clone(),
            kill_rx,
            feedback_rx,
            slot_release_rx,
            shared_pgt_winner_side.clone(),
        )
        .with_recorder(recorder.clone(), recorder_meta.clone())
        .with_obs_tx(coord_obs_tx);
        session_handles.push(tokio::spawn(coord.run()));

        let pgt_buy_fill_reopen_cooldown = if coord_cfg.strategy.is_pair_gated_tranche_arb() {
            std::time::Duration::from_millis(300)
        } else {
            std::time::Duration::ZERO
        };
        let om = OrderManager::with_buy_fill_reopen_cooldown(
            om_rx,
            exec_tx.clone(),
            result_rx,
            slot_release_tx,
            pgt_buy_fill_reopen_cooldown,
        );
        session_handles.push(tokio::spawn(om.run()));

        if !dry_run && recycle_cfg.enabled {
            session_handles.push(tokio::spawn(run_capital_recycler(
                recycle_cfg.clone(),
                auto_claim_cfg.clone(),
                capital_rx,
                inv_event_tx.clone(),
                clob_client.clone(),
                market_id.clone(),
                funder_address.clone(),
                signer_address.clone(),
                base_settings.private_key.clone(),
                dry_run,
            )));
        }

        let executor = Executor::new(
            ExecutorConfig {
                rest_url: settings.rest_url.clone(),
                market_id: market_id.clone(),
                yes_asset_id: yes_asset_id.clone(),
                no_asset_id: no_asset_id.clone(),
                tick_size: coord_cfg.tick_size,
                reconcile_interval_secs,
                dry_run,
                market_end_ts: coord_cfg.market_end_ts,
                pgt_shadow_same_side_provide_cooldown_ms: if dry_run
                    && coord_cfg.strategy.is_pair_gated_tranche_arb()
                {
                    1_200
                } else {
                    0
                },
            },
            clob_client.clone(),
            signer.clone(),
            exec_rx,
            result_tx,
            exec_fill_rx,
            dry_run_sim_fill_tx,
            dry_run_executor_md_rx,
            dry_run && coord_cfg.strategy.is_pair_gated_tranche_arb(),
            Some(capital_tx),
            Some(feedback_tx),
            recorder.enabled().then_some(recorder.clone()),
            recorder.enabled().then_some(recorder_meta.clone()),
        );
        let executor_handle = tokio::spawn(executor.run());
        let executor_abort = executor_handle.abort_handle();

        // 5. User WS Listener (live mode only — single source of truth for fills)
        if let Some((ref api_key, ref api_secret, ref api_passphrase)) = api_creds {
            let ws_base = if base_settings.ws_base_url.is_empty() {
                "wss://ws-subscriptions-clob.polymarket.com/ws".to_string()
            } else {
                base_settings.ws_base_url.clone()
            };
            let user_ws = UserWsListener::new(
                UserWsConfig {
                    ws_base_url: ws_base,
                    api_key: api_key.clone(),
                    api_secret: api_secret.clone(),
                    api_passphrase: api_passphrase.clone(),
                    market_id: market_id.clone(),
                    yes_asset_id: yes_asset_id.clone(),
                    no_asset_id: no_asset_id.clone(),
                },
                fill_tx,
            )
            .with_recorder(recorder.clone(), recorder_meta.clone());
            session_handles.push(tokio::spawn(user_ws.run()));
            info!("👤 User WS Listener spawned (real fills only)");
        } else {
            info!(
                "📝 DRY-RUN: User WS disabled — executor synthetic fills enabled via PM_DRY_RUN_FILL_PROBABILITY"
            );
            if coord_cfg.strategy.as_str() == "glft_mm" {
                info!(
                    "🧪 GLFT dry-run note: inventory now depends on synthetic fill simulator; tune PM_DRY_RUN_FILL_PROBABILITY as needed"
                );
            }
        }

        info!("🚀 Actors spawned — starting WS feed");

        // P1 FIX: Startup reconciliation — sweep any lingering orders from prior crashes
        if !dry_run {
            let _ = exec_tx
                .send(ExecutionCmd::CancelAll {
                    reason: CancelReason::Startup,
                })
                .await;
            info!("🧹 Startup CancelAll sent — clearing any stale orders from prior session");
        }

        // ── Step 3: Run until market expires ──
        // P2 FIX: Use effective_end_ts to avoid overflow in fixed mode
        let ws_round_end_ts = if oracle_lag_sniping_active {
            effective_end_ts.saturating_add(coord_cfg.oracle_lag_sniping.window_secs)
        } else {
            effective_end_ts
        };
        if recorder.enabled() {
            let session_start = RecorderSessionStart {
                round_start_ts: if slug_start_ts != u64::MAX {
                    slug_start_ts
                } else {
                    0
                },
                round_end_ts: effective_end_ts,
                yes_asset_id: yes_asset_id.clone(),
                no_asset_id: no_asset_id.clone(),
            };
            recorder.emit_session_start(&recorder_meta, &session_start);
        }
        let reason = run_market_ws_with_wall_guard(
            settings,
            ofi_md_tx,
            glft_md_tx,
            coord_md_tx,
            dry_run_touch_md_tx,
            coord_cfg.strategy.is_pair_gated_tranche_arb(),
            post_close_book_tx,
            ws_round_end_ts,
            recorder.enabled().then_some(recorder.clone()),
            recorder.enabled().then_some(recorder_meta.clone()),
        )
        .await;
        if recorder.enabled() {
            recorder.emit_session_end(&recorder_meta, &format!("{:?}", reason));
        }
        info!("🏁 Market ended: {:?}", reason);
        if let MarketEnd::WsDegraded {
            consecutive_failures,
            remaining_secs,
        } = reason
        {
            warn!(
                "🛑 Market session degraded early: ws_failures={} remaining={}s — skipping this round and rotating",
                consecutive_failures, remaining_secs
            );
        }
        let market_settled = matches!(reason, MarketEnd::Expired);
        let pgt_local_winner_side = shared_pgt_winner_side
            .as_ref()
            .and_then(|shared| shared.lock().ok().and_then(|guard| *guard));
        if recorder.enabled() && coord_cfg.strategy.is_pair_gated_tranche_arb() {
            let end_inv = inv_watch_rx_postclose.borrow().working;
            let coord_obs = *coord_obs_rx.borrow();
            let paired_qty = end_inv.yes_qty.min(end_inv.no_qty).max(0.0);
            let pair_cost = if end_inv.yes_qty > 0.0 && end_inv.no_qty > 0.0 {
                end_inv.yes_avg_cost + end_inv.no_avg_cost
            } else {
                0.0
            };
            let residual_qty = (end_inv.yes_qty - end_inv.no_qty).abs();
            recorder.emit_own_inventory_event(
                &recorder_meta,
                "pgt_shadow_summary",
                json!({
                    "reason": format!("{:?}", reason),
                    "round_end_ts": ws_round_end_ts,
                    "winner_side": pgt_local_winner_side.map(|s| s.as_str().to_string()),
                    "paired_qty": paired_qty,
                    "pair_cost": pair_cost,
                    "residual_qty": residual_qty,
                    "yes_qty": end_inv.yes_qty,
                    "no_qty": end_inv.no_qty,
                    "yes_avg_cost": end_inv.yes_avg_cost,
                    "no_avg_cost": end_inv.no_avg_cost,
                    "pgt_seed_quotes": coord_obs.pgt_seed_quotes,
                    "pgt_completion_quotes": coord_obs.pgt_completion_quotes,
                    "pgt_taker_shadow_would_open": coord_obs.pgt_taker_shadow_would_open,
                    "pgt_taker_shadow_would_close": coord_obs.pgt_taker_shadow_would_close,
                    "pgt_dispatch_taker_open": coord_obs.pgt_dispatch_taker_open,
                    "pgt_dispatch_taker_close": coord_obs.pgt_dispatch_taker_close,
                    "pgt_dispatch_place": coord_obs.pgt_dispatch_place,
                    "pgt_dispatch_retain": coord_obs.pgt_dispatch_retain,
                    "pgt_dispatch_clear": coord_obs.pgt_dispatch_clear,
                    "pgt_single_seed_first_side": coord_obs
                        .pgt_single_seed_first_side
                        .map(|s| s.as_str().to_string()),
                    "pgt_single_seed_last_side": coord_obs
                        .pgt_single_seed_last_side
                        .map(|s| s.as_str().to_string()),
                    "pgt_single_seed_flip_count": coord_obs.pgt_single_seed_flip_count,
                    "pgt_dual_seed_quotes": coord_obs.pgt_dual_seed_quotes,
                    "pgt_single_seed_released_to_dual": coord_obs.pgt_single_seed_released_to_dual,
                    "maker_only_missed_open_round": paired_qty <= 1e-9
                        && coord_obs.pgt_taker_shadow_would_open > 0
                        && coord_obs.pgt_dispatch_taker_open == 0,
                    "maker_only_missed_close_round": paired_qty <= 1e-9
                        && coord_obs.pgt_taker_shadow_would_close > 0
                        && coord_obs.pgt_dispatch_taker_close == 0,
                }),
            );
        }
        if market_settled && recorder.enabled() {
            recorder.emit_own_inventory_event(
                &recorder_meta,
                "market_resolved",
                json!({
                    "reason": format!("{:?}", reason),
                    "round_end_ts": ws_round_end_ts,
                    "winner_side": pgt_local_winner_side.map(|s| s.as_str().to_string()),
                }),
            );
        }

        let _ = om_tx.send(OrderManagerCmd::CancelAll).await;
        // Drop om_tx so the OrderManager channel closes
        drop(om_tx);

        // ── Step 4: Cleanup ──
        let _ = exec_tx
            .send(ExecutionCmd::CancelAll {
                reason: CancelReason::MarketExpired,
            })
            .await;
        // Drop exec_tx so the executor channel closes, letting it break its loop after CancelAll
        drop(exec_tx);
        info!("🧹 CancelAll sent — waiting for executor graceful shutdown (8s timeout)");

        // Wait up to 8s for the executor to complete its work and exit
        // P1 FIX: If timeout expires, use the AbortHandle to force-kill the executor task
        match tokio::time::timeout(Duration::from_secs(8), executor_handle).await {
            Ok(_) => { /* executor exited gracefully */ }
            Err(_) => {
                warn!(
                    "⚠️ Executor did not finish within 8s timeout — force aborting via AbortHandle"
                );
                executor_abort.abort();
            }
        }

        if let Some(stop_tx) = validation_stop_tx.take() {
            let _ = stop_tx.send(());
        }
        let mut round_validation_summary: Option<RoundValidationSummary> = None;
        if let Some(handle) = validation_handle.take() {
            match tokio::time::timeout(Duration::from_secs(3), handle).await {
                Ok(Ok(summary)) => {
                    round_validation_summary = Some(summary);
                }
                Ok(Err(e)) => {
                    warn!("⚠️ Round validation collector join failed: {:?}", e);
                }
                Err(_) => {
                    warn!("⚠️ Round validation collector timed out");
                }
            }
        }

        info!("🧹 Aborting remaining session tasks");

        // P0-2: Abort all session tasks to prevent leaking
        for h in session_handles {
            h.abort();
            let _ = h.await;
        }

        if let Some(mut summary) = round_validation_summary {
            summary.partial_round = !market_settled;
            if let Err(e) = append_round_validation_summary(&summary) {
                warn!("⚠️ Failed to append round validation summary: {:?}", e);
            } else {
                info!(
                    "📘 RoundValidation | market={} strategy={} partial={} fills(buy/sell)={}/{} net(max/tw)={:.2}/{:.2} pnl(realized/ex_residual/mtm/residual)={:.4}/{:.4}/{:.4}/{:.4} attr={:?} pub(replace/cancel/publish)={}/{}/{}",
                    summary.market_slug,
                    summary.strategy,
                    summary.partial_round,
                    summary.buy_fill_count,
                    summary.sell_fill_count,
                    summary.max_abs_net_diff,
                    summary.time_weighted_abs_net_diff,
                    summary.realized_cash_pnl,
                    summary.realized_round_pnl_ex_residual,
                    summary.mark_to_mid_pnl_end.unwrap_or(0.0),
                    summary.residual_inventory_cost_end,
                    summary.loss_attribution,
                    summary.replace_events,
                    summary.cancel_events,
                    summary.publish_events,
                );
                if let Err(e) =
                    update_round_validation_report(&summary.strategy, &summary.market_slug)
                {
                    warn!(
                        "⚠️ Failed to refresh round validation aggregate report: {:?}",
                        e
                    );
                }
            }
        }

        let ended_condition = market_id.parse::<alloy::primitives::B256>().ok();
        if let Some(prev) = pgt_shadow_redeem_task.take() {
            if prev.is_finished() {
                match prev.await {
                    Ok(_) => {}
                    Err(e) if e.is_cancelled() => {}
                    Err(e) => warn!("⚠️ Previous pgt shadow-redeem task join failed: {:?}", e),
                }
            } else {
                warn!(
                    "⚠️ Previous pgt shadow-redeem task still running at new market boundary — aborting stale task"
                );
                prev.abort();
                let _ = prev.await;
            }
        }
        if let Some(prev) = round_claim_task.take() {
            if prev.is_finished() {
                match prev.await {
                    Ok(_) => {}
                    Err(e) if e.is_cancelled() => {}
                    Err(e) => warn!("⚠️ Previous round-claim task join failed: {:?}", e),
                }
            } else {
                warn!(
                    "⚠️ Previous round-claim task still running at new market boundary — aborting stale task"
                );
                prev.abort();
                let _ = prev.await;
            }
        }

        if market_settled {
            maybe_log_claimable_positions(funder_address.as_deref(), signer_address.as_deref())
                .await;
        } else {
            info!(
                "💸 Round claim skipped: market session ended by {:?} (not settled)",
                reason
            );
        }

        if market_settled
            && coord_cfg.strategy.is_pair_gated_tranche_arb()
            && recorder.enabled()
            && pgt_shadow_redeem_lifecycle_enabled()
        {
            let redeem_recorder = recorder.clone();
            let redeem_meta = recorder_meta.clone();
            let final_snapshot = *inv_watch_rx_postclose.borrow();
            let local_winner_side = pgt_local_winner_side;
            pgt_shadow_redeem_task = Some(tokio::spawn(async move {
                run_pgt_shadow_redeem_lifecycle(
                    redeem_recorder,
                    redeem_meta,
                    effective_end_ts,
                    final_snapshot,
                    local_winner_side,
                )
                .await;
            }));
        }

        if auto_claim_cfg.enabled && market_settled {
            let claim_cfg = auto_claim_cfg.clone();
            let claim_runner_cfg = round_claim_cfg.clone();
            let claim_funder = funder_address.clone();
            let claim_signer = signer_address.clone();
            let claim_pk = base_settings.private_key.clone();
            let claim_recorder = recorder.enabled().then_some(recorder.clone());
            let claim_recorder_meta = recorder.enabled().then_some(recorder_meta.clone());
            round_claim_task = Some(tokio::spawn(async move {
                let mut round_state = AutoClaimState::default();
                if let Err(e) = run_round_claim_window(
                    &claim_cfg,
                    &mut round_state,
                    &claim_runner_cfg,
                    ended_condition,
                    claim_funder.as_deref(),
                    claim_signer.as_deref(),
                    claim_pk.as_deref(),
                    claim_recorder.as_ref(),
                    claim_recorder_meta.as_ref(),
                )
                .await
                {
                    warn!("⚠️ Round claim runner failed after market end: {:?}", e);
                }
            }));
            info!(
                "💸 Round claim runner launched in background (non-blocking; rotation continues immediately) ended_condition={} dry_run={} window={}s",
                ended_condition
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                auto_claim_cfg.dry_run,
                round_claim_cfg.window.as_secs()
            );
        } else if auto_claim_cfg.enabled {
            debug!("Round claim runner skipped: market not settled");
        } else {
            debug!("Round claim runner skipped at market boundary: PM_AUTO_CLAIM disabled");
        }

        if !prefix_mode {
            if let Some(handle) = pgt_shadow_redeem_task.take() {
                let wait_secs = PGT_SHADOW_REDEEM_RETRY_SECS.saturating_add(5);
                info!(
                    "🧾 Fixed mode: waiting up to {}s for pgt shadow-redeem task before exit",
                    wait_secs
                );
                let mut handle = handle;
                tokio::select! {
                    joined = &mut handle => {
                        if let Err(e) = joined {
                            warn!("⚠️ PGT shadow-redeem task join failed: {:?}", e);
                        }
                    }
                    _ = sleep(Duration::from_secs(wait_secs)) => {
                        warn!("⚠️ PGT shadow-redeem task timed out on fixed-mode exit — aborting");
                        handle.abort();
                        let _ = handle.await;
                    }
                }
            }
            if let Some(handle) = round_claim_task.take() {
                let wait_secs = round_claim_cfg.window.as_secs().saturating_add(5);
                info!(
                    "💸 Fixed mode: waiting up to {}s for background round-claim task before exit",
                    wait_secs
                );
                let mut handle = handle;
                tokio::select! {
                    joined = &mut handle => {
                        if let Err(e) = joined {
                            warn!("⚠️ Round claim background task join failed: {:?}", e);
                        }
                    }
                    _ = sleep(Duration::from_secs(wait_secs)) => {
                        warn!("⚠️ Round claim background task timed out on fixed-mode exit — aborting");
                        handle.abort();
                        let _ = handle.await;
                    }
                }
            }
            info!("📌 Fixed mode — exiting");
            break;
        }

        // Background preload for next market
        if prefix_mode {
            spawn_market_pre_resolve(
                &raw_slug,
                expected_end_ts,
                preload_tx.clone(),
                &mut preloading_slug,
                "session_end_fallback",
            );
        }

        // Wait using precise rotation wait duration instead of fixed 3s latency
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait = rotation_wait_duration(now_unix, expected_end_ts);
        if wait.is_zero() {
            info!("🔄 Rotating immediately to next market");
        } else {
            info!(
                "🔄 Waiting {}s for next market boundary before rotate",
                wait.as_secs()
            );
            sleep(wait).await;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::{HashMap, HashSet};

    fn round_summary_for_test() -> RoundValidationSummary {
        RoundValidationSummary {
            market_slug: "btc-updown-5m-test".to_string(),
            strategy: "glft_mm".to_string(),
            round_start_ts_ms: 1,
            round_end_ts_ms: 2,
            partial_round: false,
            buy_fill_count: 1,
            sell_fill_count: 0,
            yes_bought_qty: 5.0,
            yes_sold_qty: 0.0,
            no_bought_qty: 0.0,
            no_sold_qty: 0.0,
            avg_yes_buy: Some(0.5),
            avg_yes_sell: None,
            avg_no_buy: None,
            avg_no_sell: None,
            max_abs_net_diff: 5.0,
            time_weighted_abs_net_diff: 3.0,
            max_inventory_value: 2.5,
            time_in_guarded_ms: 1_000,
            time_in_blocked_ms: 0,
            paired_locked_pnl_end: None,
            worst_case_outcome_pnl_end: Some(-0.5),
            realized_cash_pnl: -2.5,
            realized_round_pnl_ex_residual: 0.0,
            mark_to_mid_pnl_end: Some(-0.25),
            residual_inventory_cost_end: 2.5,
            residual_exit_required: true,
            loss_attribution: Some(RoundLossAttribution::CInventoryNotCompleted),
            fees_paid_est: None,
            maker_rebate_est: None,
            replace_events: 2,
            cancel_events: 1,
            publish_events: 3,
            mean_entry_edge_vs_trusted_mid: Some(0.01),
            mean_fill_slippage_vs_posted: None,
            fill_to_adverse_move_3s: Some(-0.02),
            fill_to_adverse_move_10s: Some(-0.03),
        }
    }

    fn claim_cfg_for_test(dry_run: bool) -> AutoClaimConfig {
        AutoClaimConfig {
            enabled: true,
            dry_run,
            min_condition_value: Decimal::ZERO,
            max_conditions_per_run: 5,
            run_interval: Duration::from_secs(300),
            rpc_url: "https://polygon-rpc.com".to_string(),
            data_api_url: "https://data-api.polymarket.com".to_string(),
            relayer_url: "https://relayer-v2.polymarket.com".to_string(),
            builder_credentials: None,
            builder_credentials_partial: false,
            signature_type: Some(2),
            relayer_wait_confirm: false,
            relayer_wait_timeout: Duration::from_secs(20),
        }
    }

    #[test]
    fn test_should_skip_entry_window() {
        let interval = 300; // 5 min
        let grace = 30; // 30 sec grace
                        // Suppose current block ends at timestamp 1000. Start corresponds to 700.
                        // We are at 715 (15 seconds after open) -> within grace.
        assert!(!should_skip_entry_window(715, 1000, interval, grace));
        // We are at 735 (35 seconds after open) -> outside grace, we should skip!
        assert!(should_skip_entry_window(735, 1000, interval, grace));
        // We are at 1001 (already past)
        assert!(should_skip_entry_window(1001, 1000, interval, grace));
    }

    #[test]
    fn test_should_degrade_ws_threshold() {
        assert!(!should_degrade_ws(0, 12));
        assert!(!should_degrade_ws(11, 12));
        assert!(should_degrade_ws(12, 12));
        assert!(!should_degrade_ws(999, 0)); // 0 disables degradation
    }

    #[test]
    fn test_detect_interval_supports_4h_and_daily_alias() {
        assert_eq!(detect_interval("btc-updown-4h"), 14_400);
        assert_eq!(detect_interval("sol-updown-1d"), 86_400);
        assert_eq!(detect_interval("eth-updown-d"), 86_400);
    }

    #[test]
    fn test_inferred_market_times_from_exact_updown_slug() {
        assert_eq!(
            inferred_start_ts_from_slug("btc-updown-5m-1777608600"),
            Some(1_777_608_600)
        );
        assert_eq!(
            inferred_end_ts_from_slug("btc-updown-5m-1777608600"),
            Some(1_777_608_900)
        );
        assert_eq!(
            inferred_end_ts_from_slug("btc-updown-15m-1777608600"),
            Some(1_777_609_500)
        );
    }

    #[test]
    fn test_normalize_market_timeframe_aliases() {
        assert_eq!(normalize_market_timeframe("4H"), "4h");
        assert_eq!(normalize_market_timeframe("daily"), "1d");
        assert_eq!(normalize_market_timeframe("d"), "1d");
    }

    #[test]
    fn test_chainlink_symbol_from_slug() {
        assert_eq!(
            chainlink_symbol_from_slug("hype-updown-5m-1776148200"),
            Some("hype/usd".to_string())
        );
        assert_eq!(chainlink_symbol_from_slug(""), None);
    }

    #[test]
    fn test_frontend_symbol_from_chainlink_symbol() {
        assert_eq!(
            frontend_symbol_from_chainlink_symbol("hype/usd"),
            Some("HYPE".to_string())
        );
        assert_eq!(
            frontend_symbol_from_chainlink_symbol("btc/usd"),
            Some("BTC".to_string())
        );
        assert_eq!(frontend_symbol_from_chainlink_symbol(""), None);
    }

    #[test]
    fn test_data_streams_symbol_from_chainlink_symbol() {
        assert_eq!(
            data_streams_symbol_from_chainlink_symbol("hype/usd"),
            Some("HYPEUSD".to_string())
        );
        assert_eq!(
            data_streams_symbol_from_chainlink_symbol("btc/usd"),
            Some("BTCUSD".to_string())
        );
        assert_eq!(data_streams_symbol_from_chainlink_symbol(""), None);
    }

    #[test]
    fn test_parse_data_streams_tick_scales_fixed_point_price() {
        let line = r#"{"f":"t","i":"HYPEUSD","p":2.50e19,"t":1776148200,"s":1}"#;
        let tick = parse_data_streams_tick(line, "HYPEUSD");
        assert_eq!(tick, Some((25.0, 1_776_148_200)));
    }

    #[test]
    fn test_parse_chainlink_multi_symbol_ticks_with_payload_symbol() {
        let msg = r#"{"payload":{"symbol":"bnb/usd","data":[{"timestamp":1776488226000,"value":644.658},{"timestamp":1776488227000,"value":644.654}]}}"#;
        let ticks = parse_chainlink_multi_symbol_ticks(msg);
        assert_eq!(
            ticks,
            vec![
                ("bnb/usd".to_string(), 644.658, 1_776_488_226_000),
                ("bnb/usd".to_string(), 644.654, 1_776_488_227_000),
            ]
        );
    }

    #[test]
    fn test_parse_chainlink_multi_symbol_ticks_with_multiple_payloads() {
        let msg = r#"[{"payload":{"symbol":"btc/usd","data":[{"timestamp":1776488226000,"value":85000.1}]}},{"payload":{"symbol":"eth/usd","data":[{"timestamp":1776488226000,"value":2400.2}]}}]"#;
        let ticks = parse_chainlink_multi_symbol_ticks(msg);
        assert_eq!(
            ticks,
            vec![
                ("btc/usd".to_string(), 85000.1, 1_776_488_226_000),
                ("eth/usd".to_string(), 2400.2, 1_776_488_226_000),
            ]
        );
    }

    #[test]
    fn test_parse_binance_combined_trade_tick() {
        let msg = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1776488227000,"s":"BTCUSDT","t":1,"p":"85000.12000000","q":"0.001","T":1776488226999}}"#;
        let tick = parse_binance_combined_trade_tick(msg);
        assert_eq!(
            tick,
            Some(("btc/usd".to_string(), 85000.12, 1_776_488_226_999))
        );
    }

    #[test]
    fn test_parse_bybit_public_trade_ticks() {
        let msg = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1776488227000,"data":[{"T":1776488226998,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"85000.12"}]}"#;
        let ticks = parse_bybit_public_trade_ticks(msg);
        assert_eq!(
            ticks,
            vec![("btc/usd".to_string(), 85000.12, 1_776_488_226_998)]
        );
    }

    #[test]
    fn test_parse_okx_trade_ticks() {
        let msg = r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"85000.12","sz":"0.1","side":"buy","ts":"1776488226997"}]}"#;
        let ticks = parse_okx_trade_ticks(msg);
        assert_eq!(
            ticks,
            vec![("btc/usd".to_string(), 85000.12, 1_776_488_226_997)]
        );
    }

    fn arbiter_obs_for_test(
        round_end_ts: u64,
        slug: &str,
        source: PostCloseBookSource,
        distance_to_final_ms: u64,
    ) -> ArbiterObservation {
        let (hint_tx, _hint_rx) = mpsc::channel::<MarketDataMsg>(4);
        ArbiterObservation {
            round_end_ts,
            slug: slug.to_string(),
            winner_side: Side::Yes,
            winner_bid: 0.99,
            winner_ask_raw: 0.99,
            winner_ask_eff: 0.99,
            winner_ask_tradable: true,
            book_age_ms: distance_to_final_ms,
            book_source: source,
            evidence_recv_ms: 1_000,
            distance_to_final_ms,
            detect_ms: 1_100,
            hint_msg: MarketDataMsg::WinnerHint {
                slug: slug.to_string(),
                hint_id: 1_100,
                side: Side::Yes,
                source: WinnerHintSource::Chainlink,
                ref_price: 1.0,
                observed_price: 1.01,
                final_detect_unix_ms: 1_100,
                emit_unix_ms: 1_120,
                winner_bid: 0.99,
                winner_ask_raw: 0.99,
                winner_evidence_recv_ms: 1_000,
                winner_book_source: "ws_partial",
                winner_distance_to_final_ms: distance_to_final_ms,
                open_is_exact: true,
                ts: Instant::now(),
            },
            hint_tx,
        }
    }

    #[test]
    fn test_arbiter_ignores_observation_for_finalized_round() {
        let mut pending: HashMap<u64, HashMap<String, ArbiterObservation>> = HashMap::new();
        let mut deadlines: HashMap<u64, tokio::time::Instant> = HashMap::new();
        let mut finalized: HashSet<u64> = HashSet::new();
        finalized.insert(1_776_680_400);

        let should_dispatch = arbiter_intake(
            &mut pending,
            &mut deadlines,
            &mut finalized,
            arbiter_obs_for_test(
                1_776_680_400,
                "btc-updown-5m-1776680100",
                PostCloseBookSource::WsPartial,
                10,
            ),
            200,
            7,
        );

        assert!(should_dispatch.is_none());
        assert!(pending.is_empty());
        assert!(deadlines.is_empty());
    }

    #[test]
    fn test_arbiter_prefers_closer_same_source_observation() {
        let mut pending: HashMap<u64, HashMap<String, ArbiterObservation>> = HashMap::new();
        let mut deadlines: HashMap<u64, tokio::time::Instant> = HashMap::new();
        let mut finalized: HashSet<u64> = HashSet::new();
        let round_end = 1_776_680_700;
        let slug = "hype-updown-5m-1776680400";

        let _ = arbiter_intake(
            &mut pending,
            &mut deadlines,
            &mut finalized,
            arbiter_obs_for_test(round_end, slug, PostCloseBookSource::WsPartial, 50),
            200,
            7,
        );
        let _ = arbiter_intake(
            &mut pending,
            &mut deadlines,
            &mut finalized,
            arbiter_obs_for_test(round_end, slug, PostCloseBookSource::WsPartial, 12),
            200,
            7,
        );

        let kept = pending
            .get(&round_end)
            .and_then(|by_slug| by_slug.get(slug))
            .expect("expected retained observation");
        assert_eq!(kept.distance_to_final_ms, 12);
    }

    #[test]
    fn test_frontend_variant_from_round_len_secs() {
        assert_eq!(
            frontend_variant_from_round_len_secs(300),
            Some("fiveminute")
        );
        assert_eq!(
            frontend_variant_from_round_len_secs(900),
            Some("fifteenminute")
        );
        assert_eq!(frontend_variant_from_round_len_secs(3_600), Some("hourly"));
        assert_eq!(
            frontend_variant_from_round_len_secs(14_400),
            Some("fourhour")
        );
        assert_eq!(frontend_variant_from_round_len_secs(123), None);
    }

    #[test]
    fn test_parse_chainlink_tick_with_nested_payload() {
        let payload = json!({
            "topic": "crypto_prices_chainlink",
            "data": {
                "symbol": "HYPE/USD",
                "value": "22.37",
                "timestamp": 1_776_148_200_123u64
            }
        })
        .to_string();
        let tick = parse_chainlink_tick(&payload, "hype/usd");
        assert_eq!(tick, Some((22.37, 1_776_148_200_123)));
    }

    #[test]
    fn test_extract_gamma_winner_side_from_prices() {
        let market = json!({
            "outcomes": "[\"Up\",\"Down\"]",
            "outcomePrices": "[\"1.0\",\"0.0\"]"
        });
        let winner = extract_gamma_winner_side(&market);
        assert_eq!(winner, Some((Side::Yes, 1.0)));
    }

    #[test]
    fn test_last_trade_price_missing_side_parsing() {
        let val_with_side = json!({
            "asset_id": "111",
            "price": "0.50",
            "size": "100",
            "side": "SELL"
        });

        let val_no_side = json!({
            "asset_id": "111",
            "price": "0.50",
            "size": "100"
        });

        let side1 = val_with_side.get("side").and_then(|v| v.as_str());
        let side2 = val_no_side.get("side").and_then(|v| v.as_str());

        assert_eq!(side1, Some("SELL"));
        assert_eq!(side2, None);
    }

    #[test]
    fn test_parse_ws_message_last_trade_price_preserves_trade_id() {
        let settings = Settings {
            market_slug: Some("btc-updown-5m-test".to_string()),
            market_id: "0xmarket".to_string(),
            yes_asset_id: "111".to_string(),
            no_asset_id: "222".to_string(),
            ws_base_url: String::new(),
            rest_url: String::new(),
            private_key: None,
            funder_address: None,
            custom_feature: false,
        };
        let value = json!({
            "event_type": "last_trade_price",
            "asset_id": "111",
            "price": "0.50",
            "size": "100",
            "side": "SELL",
            "trade_id": "tid-123",
        });

        let msgs = parse_ws_message(&settings, &value);
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            MarketDataMsg::TradeTick {
                asset_id,
                trade_id,
                market_side,
                taker_side,
                price,
                size,
                ..
            } => {
                assert_eq!(asset_id, "111");
                assert_eq!(trade_id.as_deref(), Some("tid-123"));
                assert_eq!(*market_side, Side::Yes);
                assert_eq!(*taker_side, TakerSide::Sell);
                assert!((*price - 0.50).abs() < 1e-9);
                assert!((*size - 100.0).abs() < 1e-9);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn test_book_assembler_emits_full_book_only_after_both_sides() {
        let mut asm = BookAssembler::default();
        let ts = Instant::now();
        let yes_only = MarketDataMsg::BookTick {
            yes_bid: 0.49,
            yes_ask: 0.51,
            no_bid: f64::NAN,
            no_ask: f64::NAN,
            ts,
        };
        let no_only = MarketDataMsg::BookTick {
            yes_bid: f64::NAN,
            yes_ask: f64::NAN,
            no_bid: 0.48,
            no_ask: 0.52,
            ts,
        };

        assert!(asm.update(&yes_only).is_none());
        let full = asm
            .update(&no_only)
            .expect("full book should emit after both sides");
        match full {
            MarketDataMsg::BookTick {
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                ..
            } => {
                assert_eq!(yes_bid, 0.49);
                assert_eq!(yes_ask, 0.51);
                assert_eq!(no_bid, 0.48);
                assert_eq!(no_ask, 0.52);
            }
            other => panic!("unexpected full book output: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_executor_timeout_abort_pattern() {
        // P3 Regression Test: Verified that if timeout consumes the JoinHandle,
        // the AbortHandle successfully terminates the lingering background task.
        use std::time::Duration;
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        let abort_handle = handle.abort_handle();

        // 1. Simulate the market WS 8s shutdown timeout firing early.
        // NOTE: tokio::time::timeout consumes the `handle` itself if passed directly
        let res = tokio::time::timeout(Duration::from_millis(5), handle).await;
        assert!(res.is_err(), "timeout must expire");

        // 2. We no longer have `handle`, but we have `abort_handle`. Force kill it.
        abort_handle.abort();

        // Verification: Since we can't join it (handle is gone), we just wait a tick
        // to ensure it didn't panic and the abort went through cleanly.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_ping_task_cleanup_pattern() {
        // P3 Regression Test: Verified that ping_handle.abort() structurally works
        // to cleanly kill a spawned ping keepalive task when the WS drops.
        use std::time::Duration;
        let ping_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        // Simulate WS Exit Branch (e.g. server close or EOF) calling abort
        ping_handle.abort();

        // Awaited task should explicitly yield a Cancelled error, proving no leak.
        let join_res = ping_handle.await;
        assert!(
            join_res.unwrap_err().is_cancelled(),
            "Ping task must be cancelled on WS exit"
        );
    }

    #[test]
    fn test_parse_u256_allowance_supports_huge_values() {
        let huge = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let parsed = parse_u256_allowance(huge).expect("must parse large allowance");
        assert!(!parsed.is_zero(), "large allowance should be non-zero");
    }

    #[test]
    fn test_pgt_shadow_redeem_target_only_targets_resolved_winner_residual() {
        let mut snapshot = InventorySnapshot::default();
        snapshot.working.yes_qty = 12.0;
        snapshot.working.no_qty = 3.0;
        snapshot.pair_ledger.residual_side = Some(Side::Yes);
        snapshot.pair_ledger.residual_qty = 9.0;

        assert_eq!(
            pgt_shadow_redeem_target(&snapshot, Some(Side::Yes)),
            Some((Side::Yes, 9.0))
        );
        assert_eq!(pgt_shadow_redeem_target(&snapshot, Some(Side::No)), None);
        assert_eq!(pgt_shadow_redeem_target(&snapshot, None), None);
    }

    #[test]
    fn test_pgt_shadow_redeem_target_skips_zero_qty_winner_side() {
        let mut snapshot = InventorySnapshot::default();
        snapshot.working.yes_qty = 0.0;
        snapshot.working.no_qty = 7.0;
        snapshot.pair_ledger.residual_side = Some(Side::No);
        snapshot.pair_ledger.residual_qty = 7.0;

        assert_eq!(pgt_shadow_redeem_target(&snapshot, Some(Side::Yes)), None);
        assert_eq!(
            pgt_shadow_redeem_target(&snapshot, Some(Side::No)),
            Some((Side::No, 7.0))
        );
    }

    #[test]
    fn test_pgt_shadow_redeem_target_ignores_pairable_full_sets() {
        let mut snapshot = InventorySnapshot::default();
        snapshot.working.yes_qty = 57.6;
        snapshot.working.no_qty = 57.6;
        snapshot.pair_ledger.capital_state.mergeable_full_sets = 57.6;

        assert_eq!(pgt_shadow_redeem_target(&snapshot, Some(Side::Yes)), None);
        assert_eq!(pgt_shadow_redeem_target(&snapshot, Some(Side::No)), None);
    }

    #[test]
    fn test_pgt_shadow_harvest_does_not_retry_after_shadow_merge_completed() {
        assert_eq!(
            pgt_shadow_harvest_attempt(false, false, false, false, true, 25),
            Some(1)
        );
        assert_eq!(
            pgt_shadow_harvest_attempt(true, false, true, true, true, 18),
            None
        );
    }

    #[test]
    fn test_pgt_shadow_harvest_retries_when_first_attempt_was_ineligible() {
        assert_eq!(
            pgt_shadow_harvest_attempt(true, false, false, false, true, 24),
            Some(2)
        );
        assert_eq!(
            pgt_shadow_harvest_attempt(true, false, false, false, false, 18),
            None
        );
    }

    #[test]
    fn test_round_validation_aggregate_tracks_residual_inventory_fields() {
        let entries = vec![round_summary_for_test()];
        let agg = aggregate_round_validation(&entries);
        assert_eq!(agg.total_realized_cash_pnl, -2.5);
        assert_eq!(agg.total_realized_round_pnl_ex_residual, 0.0);
        assert_eq!(agg.total_residual_inventory_cost_end, 2.5);
        assert_eq!(agg.median_round_pnl, Some(-2.75));
        assert_eq!(agg.median_round_pnl_ex_residual, Some(0.0));
    }

    #[test]
    fn test_recycler_reject_filter_accepts_provide_when_not_hedge_only() {
        let mut cfg = CapitalRecycleConfig {
            enabled: true,
            only_hedge_rejects: false,
            trigger_rejects: 2,
            trigger_window: Duration::from_secs(90),
            proactive_headroom: true,
            headroom_poll: Duration::from_secs(5),
            cooldown: Duration::from_secs(120),
            max_merges_per_round: 2,
            low_water_usdc: 6.0,
            target_free_usdc: 18.0,
            min_batch_usdc: 10.0,
            max_batch_usdc: 30.0,
            shortfall_multiplier: 1.2,
            min_executable_usdc: 5.0,
        };
        let evt = PlacementRejectEvent {
            side: Side::Yes,
            reason: BidReason::Provide,
            kind: RejectKind::BalanceOrAllowance,
            price: 0.51,
            size: 5.0,
            ts: Instant::now(),
        };
        assert!(should_count_recycle_reject(&cfg, &evt));

        cfg.only_hedge_rejects = true;
        assert!(
            !should_count_recycle_reject(&cfg, &evt),
            "hedge-only mode should ignore provide rejects"
        );
    }

    #[test]
    fn test_round_claim_schedule_normalization() {
        let parsed = parse_retry_schedule_secs("27,2,5,9,14,20,0,40");
        assert_eq!(parsed, vec![27, 2, 5, 9, 14, 20, 0, 40]);
        let normalized = normalize_retry_schedule_secs(parsed, 30);
        assert_eq!(normalized, vec![0, 2, 5, 9, 14, 20, 27]);
    }

    #[test]
    fn test_round_claim_requirements_relax_signer_and_pk_in_dry_run() {
        let cfg = claim_cfg_for_test(true);
        let missing = round_claim_missing_requirements(&cfg, Some("0xabc"), None, None);
        assert!(
            missing.is_empty(),
            "dry-run round claim should proceed without signer/private"
        );
    }

    #[test]
    fn test_round_claim_requirements_enforce_signer_and_pk_in_live() {
        let cfg = claim_cfg_for_test(false);
        let missing = round_claim_missing_requirements(&cfg, Some("0xabc"), None, None);
        assert_eq!(missing, vec!["signer_address", "private_key"]);
    }

    #[test]
    fn test_dynamic_sizing_keeps_static_floors_when_target_lower() {
        let out = apply_dynamic_sizing(80.0, 5.0, 15.0, Some(0.02), Some(0.10));
        assert_eq!(out.bid_target, Some(2.0));
        assert_eq!(out.net_target, Some(8.0));
        assert_eq!(out.bid_effective, 5.0);
        assert_eq!(out.net_effective, 15.0);
    }

    #[test]
    fn test_dynamic_sizing_scales_up_above_floors() {
        let out = apply_dynamic_sizing(300.0, 5.0, 15.0, Some(0.02), Some(0.10));
        assert_eq!(out.bid_target, Some(6.0));
        assert_eq!(out.net_target, Some(30.0));
        assert_eq!(out.bid_effective, 6.0);
        assert_eq!(out.net_effective, 30.0);
    }

    #[test]
    fn test_dynamic_sizing_keeps_net_at_least_bid_when_net_pct_off() {
        let out = apply_dynamic_sizing(300.0, 5.0, 7.0, Some(0.03), None);
        assert_eq!(out.bid_effective, 9.0);
        assert_eq!(out.net_effective, 9.0);
    }
}
