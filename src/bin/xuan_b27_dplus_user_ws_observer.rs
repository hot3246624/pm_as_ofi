//! Standalone read-only User WS observer for xuan_b27_dplus.
//!
//! This binary intentionally does not create Executor, OrderManager, InventoryManager,
//! or any order command channel. It only authenticates User WS, records raw/parsed
//! private account events, and counts FillEvent messages in a local sink.

use std::env;
use std::fs;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use anyhow::{Context, Result};
use pm_as_ofi::polymarket::executor::init_clob_client;
use pm_as_ofi::polymarket::messages::FillEvent;
use pm_as_ofi::polymarket::recorder::{
    RecorderConfig, RecorderHandle, RecorderMarketMode, RecorderSessionMeta, RecorderSessionStart,
};
use pm_as_ofi::polymarket::user_ws::{UserWsConfig, UserWsListener};
use polymarket_client_sdk::auth::Credentials;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const DEFAULT_SHARED_INGRESS_ROOT: &str =
    "/Users/hot/web3Scientist/pm_as_ofi/run/shared-ingress-main";
const DEFAULT_MARKET_SLUG: &str = "btc-updown-5m";
const SHARED_INGRESS_PROTOCOL_VERSION: u32 = 1;
const SHARED_INGRESS_SCHEMA_VERSION: u32 = 1;
const SHARED_INGRESS_STALE_MS: u64 = 10_000;

type ResolvedMarket = (String, String, String, Option<u64>);

#[derive(Debug, Clone)]
struct Args {
    approved_readonly_user_ws: bool,
    duration_secs: u64,
    market_slug: String,
    output_dir: PathBuf,
    shared_ingress_root: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedIngressBrokerManifest {
    protocol_version: u32,
    #[serde(default)]
    schema_version: u32,
    #[serde(default)]
    build_id: String,
    pid: u32,
    started_ms: u64,
    last_heartbeat_ms: u64,
    chainlink_socket: String,
    local_price_socket: String,
    market_socket: String,
}

#[derive(Debug, Clone, Serialize)]
struct RunManifest {
    artifact: &'static str,
    status: String,
    strategy: &'static str,
    mode: &'static str,
    dry_run: bool,
    orders_possible: bool,
    market_slug_arg: String,
    resolved_slug: Option<String>,
    market_id: Option<String>,
    yes_asset_id: Option<String>,
    no_asset_id: Option<String>,
    duration_secs: u64,
    output_dir: String,
    recorder_root: String,
    shared_ingress_root: String,
    shared_ingress_status: String,
    user_ws_auth_source: String,
    started_ms: u64,
    finished_ms: Option<u64>,
    fills_seen: u64,
    recorder_md_drop_count: u64,
    recorder_critical_drop_count: u64,
    reason: String,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn install_rustls_crypto_provider() {
    // rustls 0.23 requires one process-level CryptoProvider. Install ring
    // explicitly before any reqwest/tungstenite TLS path can run in a task.
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_ok()
    {
        info!("rustls CryptoProvider installed: ring");
    }
}

fn parse_args() -> Result<Args> {
    let mut args = env::args().skip(1);
    let mut out = Args {
        approved_readonly_user_ws: false,
        duration_secs: 1_800,
        market_slug: DEFAULT_MARKET_SLUG.to_string(),
        output_dir: PathBuf::from(format!(
            "xuan_research_artifacts/xuan_b27_dplus_user_ws_observer_{}",
            now_ms()
        )),
        shared_ingress_root: PathBuf::from(DEFAULT_SHARED_INGRESS_ROOT),
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--approved-readonly-user-ws" => out.approved_readonly_user_ws = true,
            "--duration-secs" => {
                let raw = args.next().context("--duration-secs requires a value")?;
                out.duration_secs = raw
                    .parse::<u64>()
                    .context("--duration-secs must be an integer")?;
            }
            "--market-slug" => {
                out.market_slug = args.next().context("--market-slug requires a value")?;
            }
            "--output-dir" => {
                out.output_dir =
                    PathBuf::from(args.next().context("--output-dir requires a value")?);
            }
            "--shared-ingress-root" => {
                out.shared_ingress_root = PathBuf::from(
                    args.next()
                        .context("--shared-ingress-root requires a value")?,
                );
            }
            "-h" | "--help" => {
                println!(
                    "Usage: xuan_b27_dplus_user_ws_observer \\
  --approved-readonly-user-ws \\
  [--duration-secs 1800] [--market-slug btc-updown-5m] \\
  [--shared-ingress-root /path/to/shared-ingress-main] \\
  [--output-dir xuan_research_artifacts/...]"
                );
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }

    Ok(out)
}

fn write_manifest(path: &Path, manifest: &RunManifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(manifest)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn load_dotenv() {
    let cwd_env = PathBuf::from(".env");
    if cwd_env.exists() {
        let _ = dotenv::from_path(&cwd_env);
    }
    let _ = dotenv::dotenv();
}

fn check_shared_ingress(root: &Path) -> (String, String) {
    let manifest_path = root.join("broker_manifest.json");
    let raw = match fs::read_to_string(&manifest_path) {
        Ok(raw) => raw,
        Err(err) => {
            return (
                "UNAVAILABLE".to_string(),
                format!("broker_manifest_read_failed: {err}"),
            )
        }
    };
    let manifest: SharedIngressBrokerManifest = match serde_json::from_str(&raw) {
        Ok(manifest) => manifest,
        Err(err) => {
            return (
                "UNAVAILABLE".to_string(),
                format!("broker_manifest_parse_failed: {err}"),
            )
        }
    };
    if manifest.protocol_version != SHARED_INGRESS_PROTOCOL_VERSION
        || manifest.schema_version != SHARED_INGRESS_SCHEMA_VERSION
    {
        return (
            "UNAVAILABLE".to_string(),
            format!(
                "broker_protocol_mismatch: protocol={} schema={}",
                manifest.protocol_version, manifest.schema_version
            ),
        );
    }
    let age_ms = now_ms().saturating_sub(manifest.last_heartbeat_ms);
    if age_ms > SHARED_INGRESS_STALE_MS {
        return (
            "UNAVAILABLE".to_string(),
            format!("broker_heartbeat_stale_ms={age_ms}"),
        );
    }
    for path in [
        &manifest.chainlink_socket,
        &manifest.local_price_socket,
        &manifest.market_socket,
    ] {
        let socket_path = Path::new(path);
        let Ok(meta) = fs::metadata(socket_path) else {
            return ("UNAVAILABLE".to_string(), format!("socket_missing: {path}"));
        };
        if !meta.file_type().is_socket() {
            return (
                "UNAVAILABLE".to_string(),
                format!("socket_not_unix_socket: {path}"),
            );
        }
    }
    (
        "OK".to_string(),
        format!("pid={} heartbeat_age_ms={age_ms}", manifest.pid),
    )
}

fn read_explicit_api_credentials() -> Result<Option<(String, String, String, Credentials)>> {
    let key = env::var("POLYMARKET_API_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let secret = env::var("POLYMARKET_API_SECRET")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let passphrase = env::var("POLYMARKET_API_PASSPHRASE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    match (key, secret, passphrase) {
        (Some(key), Some(secret), Some(passphrase)) => {
            let uuid = Uuid::from_str(&key).context("invalid POLYMARKET_API_KEY UUID")?;
            let creds = Credentials::new(uuid, secret.clone(), passphrase.clone());
            Ok(Some((key, secret, passphrase, creds)))
        }
        (None, None, None) => Ok(None),
        _ => anyhow::bail!(
            "POLYMARKET_API_KEY / POLYMARKET_API_SECRET / POLYMARKET_API_PASSPHRASE must be set together"
        ),
    }
}

fn parse_env_address(name: &str) -> Result<Option<Address>> {
    let Some(raw) = env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    else {
        return Ok(None);
    };
    Ok(Some(Address::from_str(&raw).with_context(|| {
        format!("{name} is not a valid address")
    })?))
}

async fn resolve_user_ws_credentials(rest_url: &str) -> Result<((String, String, String), String)> {
    if let Some((key, secret, passphrase, _creds)) = read_explicit_api_credentials()? {
        return Ok((
            (key, secret, passphrase),
            "explicit_POLYMARKET_API".to_string(),
        ));
    }

    let private_key = env::var("POLYMARKET_PRIVATE_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .context("missing POLYMARKET_PRIVATE_KEY and no complete POLYMARKET_API_* credentials")?;
    let funder = parse_env_address("POLYMARKET_FUNDER_ADDRESS")?;
    let (client, _signer) = init_clob_client(rest_url, Some(&private_key), funder, None).await;
    let client = client.context("could not derive read-only User WS credentials")?;
    let creds = client.credentials();
    Ok((
        (
            creds.key().to_string(),
            creds.secret().expose_secret().to_string(),
            creds.passphrase().expose_secret().to_string(),
        ),
        "derived_from_POLYMARKET_PRIVATE_KEY".to_string(),
    ))
}

fn is_prefix_slug(slug: &str) -> bool {
    slug.rsplit('-')
        .next()
        .map(|last| last.parse::<u64>().is_err())
        .unwrap_or(true)
}

fn detect_interval(prefix: &str) -> u64 {
    let lower = prefix.to_ascii_lowercase();
    if lower.contains("-1m") {
        60
    } else if lower.contains("-5m") {
        300
    } else if lower.contains("-15m") {
        900
    } else if lower.contains("-30m") {
        1_800
    } else if lower.contains("-1h") {
        3_600
    } else if lower.contains("-4h") {
        14_400
    } else if lower.contains("-1d") || lower.ends_with("-d") || lower.contains("-daily") {
        86_400
    } else {
        900
    }
}

fn compute_current_slug(prefix: &str) -> (String, u64, u64) {
    let interval = detect_interval(prefix);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let start_ts = (now / interval) * interval;
    let end_ts = start_ts + interval;
    (format!("{prefix}-{start_ts}"), start_ts, end_ts)
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

async fn resolve_market_by_slug(slug: &str) -> Result<ResolvedMarket> {
    let url = format!("https://gamma-api.polymarket.com/markets?slug={slug}");
    let resp: Value = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()?
        .get(url)
        .send()
        .await?
        .json()
        .await?;
    let markets = resp
        .as_array()
        .context("Gamma /markets response is not an array")?;
    let market = markets
        .first()
        .context("Gamma /markets returned no markets")?;
    parse_resolved_market_payload(market).context("Gamma /markets payload missing ids")
}

async fn resolve_market_by_event_slug(slug: &str) -> Result<ResolvedMarket> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    let resp: Value = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()?
        .get(url)
        .send()
        .await?
        .json()
        .await?;
    let events = resp
        .as_array()
        .context("Gamma /events response is not an array")?;
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
                return Ok(resolved);
            }
        }
        for market in markets {
            if let Some(resolved) = parse_resolved_market_payload(market) {
                return Ok(resolved);
            }
        }
    }
    anyhow::bail!("Gamma /events returned no resolvable market for {slug}")
}

async fn resolve_market(slug_arg: &str) -> Result<(String, ResolvedMarket, u64, u64)> {
    if let (Some(market_id), Some(yes), Some(no)) = (
        env::var("POLYMARKET_MARKET_ID")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        env::var("POLYMARKET_YES_ASSET_ID")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        env::var("POLYMARKET_NO_ASSET_ID")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
    ) {
        return Ok((slug_arg.to_string(), (market_id, yes, no, None), 0, 0));
    }

    let (slug, round_start, round_end) = if is_prefix_slug(slug_arg) {
        compute_current_slug(slug_arg)
    } else {
        let interval = detect_interval(slug_arg);
        let start = slug_arg
            .rsplit('-')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        (slug_arg.to_string(), start, start.saturating_add(interval))
    };

    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..3 {
        match resolve_market_by_slug(&slug).await {
            Ok(ids) => return Ok((slug, ids, round_start, round_end)),
            Err(primary_err) => match resolve_market_by_event_slug(&slug).await {
                Ok(ids) => return Ok((slug, ids, round_start, round_end)),
                Err(fallback_err) => {
                    last_err = Some(anyhow::anyhow!(
                        "markets_resolve_err={} | events_resolve_err={}",
                        primary_err,
                        fallback_err
                    ));
                }
            },
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("market resolve failed: {slug}")))
}

#[tokio::main]
async fn main() -> Result<()> {
    load_dotenv();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
    install_rustls_crypto_provider();

    let args = parse_args()?;
    fs::create_dir_all(&args.output_dir)?;
    let manifest_path = args.output_dir.join("run_manifest.json");
    let recorder_root = args.output_dir.join("recorder");
    let started_ms = now_ms();
    let (shared_ingress_status, shared_ingress_reason) =
        check_shared_ingress(&args.shared_ingress_root);

    let mut manifest = RunManifest {
        artifact: "xuan_b27_dplus_user_ws_observer",
        status: "STARTING".to_string(),
        strategy: "xuan_b27_dplus",
        mode: "readonly_user_ws_observer",
        dry_run: true,
        orders_possible: false,
        market_slug_arg: args.market_slug.clone(),
        resolved_slug: None,
        market_id: None,
        yes_asset_id: None,
        no_asset_id: None,
        duration_secs: args.duration_secs,
        output_dir: args.output_dir.display().to_string(),
        recorder_root: recorder_root.display().to_string(),
        shared_ingress_root: args.shared_ingress_root.display().to_string(),
        shared_ingress_status: shared_ingress_status.clone(),
        user_ws_auth_source: "unknown".to_string(),
        started_ms,
        finished_ms: None,
        fills_seen: 0,
        recorder_md_drop_count: 0,
        recorder_critical_drop_count: 0,
        reason: shared_ingress_reason,
    };

    if !args.approved_readonly_user_ws {
        manifest.status = "REFUSED_NO_EXPLICIT_APPROVAL".to_string();
        manifest.reason = "missing --approved-readonly-user-ws".to_string();
        manifest.finished_ms = Some(now_ms());
        write_manifest(&manifest_path, &manifest)?;
        anyhow::bail!("refusing to start without --approved-readonly-user-ws");
    }

    if shared_ingress_status != "OK" {
        manifest.status = "SHARED_INGRESS_BROKER_UNAVAILABLE".to_string();
        manifest.finished_ms = Some(now_ms());
        write_manifest(&manifest_path, &manifest)?;
        anyhow::bail!(
            "shared ingress broker unavailable at {}: {}",
            args.shared_ingress_root.display(),
            manifest.reason
        );
    }

    let rest_url = env::var("POLYMARKET_REST_URL")
        .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());
    let ws_base_url = env::var("POLYMARKET_WS_BASE_URL")
        .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws".to_string());
    let ((api_key, api_secret, api_passphrase), auth_source) =
        resolve_user_ws_credentials(&rest_url).await?;
    manifest.user_ws_auth_source = auth_source;

    let (resolved_slug, (market_id, yes_asset_id, no_asset_id, gamma_end), round_start, round_end) =
        resolve_market(&args.market_slug).await?;
    let round_end = gamma_end.unwrap_or(round_end);
    manifest.resolved_slug = Some(resolved_slug.clone());
    manifest.market_id = Some(market_id.clone());
    manifest.yes_asset_id = Some(yes_asset_id.clone());
    manifest.no_asset_id = Some(no_asset_id.clone());
    manifest.status = "RUNNING".to_string();
    manifest.reason =
        "readonly User WS observer running; no executor/order manager/inventory".to_string();
    write_manifest(&manifest_path, &manifest)?;

    let recorder = RecorderHandle::from_config(&RecorderConfig {
        enabled: true,
        root: recorder_root.clone(),
        md_queue_cap: 16_384,
        ops_queue_cap: 4_096,
        flush_every: Duration::from_millis(250),
        market_mode: RecorderMarketMode::Structured,
    });
    let meta = RecorderSessionMeta {
        slug: resolved_slug.clone(),
        condition_id: market_id.clone(),
        market_id: market_id.clone(),
        strategy: "xuan_b27_dplus".to_string(),
        dry_run: true,
    };
    recorder.emit_session_start(
        &meta,
        &RecorderSessionStart {
            round_start_ts: round_start,
            round_end_ts: round_end,
            yes_asset_id: yes_asset_id.clone(),
            no_asset_id: no_asset_id.clone(),
        },
    );
    recorder.emit_strategy_event(
        &meta,
        "xuan_b27_dplus_readonly_user_ws_observer_start",
        json!({
            "schema_version": 1,
            "orders_possible": false,
            "executor_created": false,
            "order_manager_created": false,
            "inventory_manager_created": false,
            "shared_ingress_root": args.shared_ingress_root,
            "duration_secs": args.duration_secs,
        }),
    );

    let (fill_tx, mut fill_rx) = mpsc::channel::<FillEvent>(256);
    let user_ws = UserWsListener::new(
        UserWsConfig {
            ws_base_url,
            api_key,
            api_secret,
            api_passphrase,
            market_id: market_id.clone(),
            yes_asset_id,
            no_asset_id,
        },
        fill_tx,
    )
    .with_recorder(recorder.clone(), meta.clone());
    let mut user_ws_handle = tokio::spawn(user_ws.run());

    let recorder_for_fills = recorder.clone();
    let meta_for_fills = meta.clone();
    let fill_sink = tokio::spawn(async move {
        let mut fills_seen = 0u64;
        while let Some(fill) = fill_rx.recv().await {
            fills_seen = fills_seen.saturating_add(1);
            recorder_for_fills.emit_strategy_event(
                &meta_for_fills,
                "xuan_b27_dplus_readonly_user_ws_fill_sink",
                json!({
                    "schema_version": 1,
                    "orders_possible": false,
                    "fill_count": fills_seen,
                    "order_id": fill.order_id,
                    "side": fill.side.as_str(),
                    "direction": format!("{:?}", fill.direction),
                    "filled_size": fill.filled_size,
                    "price": fill.price,
                    "status": format!("{:?}", fill.status),
                }),
            );
        }
        fills_seen
    });

    info!(
        "xuan_b27_dplus readonly User WS observer started | slug={} duration_secs={} out={}",
        resolved_slug,
        args.duration_secs,
        args.output_dir.display()
    );
    let duration_sleep = tokio::time::sleep(Duration::from_secs(args.duration_secs));
    tokio::pin!(duration_sleep);
    let mut early_user_ws_exit: Option<String> = None;
    tokio::select! {
        _ = &mut duration_sleep => {
            user_ws_handle.abort();
            let _ = user_ws_handle.await;
        }
        joined = &mut user_ws_handle => {
            early_user_ws_exit = Some(match joined {
                Ok(()) => "user_ws_task_returned_unexpectedly".to_string(),
                Err(err) => format!("user_ws_task_join_error: {err}"),
            });
        }
    }
    drop(recorder.clone());

    // Closing the listener drops fill_tx; allow the sink to drain briefly.
    let fills_seen = tokio::time::timeout(Duration::from_secs(2), fill_sink)
        .await
        .ok()
        .and_then(|join| join.ok())
        .unwrap_or(0);

    let end_reason = early_user_ws_exit
        .clone()
        .unwrap_or_else(|| "duration_elapsed".to_string());
    recorder.emit_strategy_event(
        &meta,
        "xuan_b27_dplus_readonly_user_ws_observer_end",
        json!({
            "schema_version": 1,
            "orders_possible": false,
            "fills_seen": fills_seen,
            "recorder_md_drop_count": recorder.md_drop_count(),
            "recorder_critical_drop_count": recorder.critical_drop_count(),
            "reason": end_reason,
            "user_ws_task_exited_early": early_user_ws_exit.is_some(),
        }),
    );
    recorder.emit_session_end(&meta, &end_reason);
    tokio::time::sleep(Duration::from_millis(500)).await;

    if let Some(reason) = early_user_ws_exit {
        manifest.status = "FAIL_USER_WS_TASK_EXITED".to_string();
        manifest.finished_ms = Some(now_ms());
        manifest.fills_seen = fills_seen;
        manifest.recorder_md_drop_count = recorder.md_drop_count();
        manifest.recorder_critical_drop_count = recorder.critical_drop_count();
        manifest.reason = reason.clone();
        write_manifest(&manifest_path, &manifest)?;
        anyhow::bail!("User WS task exited before duration elapsed: {reason}");
    }

    manifest.status = "PASS_READONLY_USER_WS_OBSERVER".to_string();
    manifest.finished_ms = Some(now_ms());
    manifest.fills_seen = fills_seen;
    manifest.recorder_md_drop_count = recorder.md_drop_count();
    manifest.recorder_critical_drop_count = recorder.critical_drop_count();
    manifest.reason = "duration_elapsed".to_string();
    write_manifest(&manifest_path, &manifest)?;
    warn!(
        "xuan_b27_dplus readonly User WS observer finished | fills_seen={} critical_drops={}",
        fills_seen, manifest.recorder_critical_drop_count
    );
    Ok(())
}
