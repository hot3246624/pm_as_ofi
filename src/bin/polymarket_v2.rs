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
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
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
use pm_as_ofi::polymarket::recorder::{RecorderHandle, RecorderSessionMeta};
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

fn round_validation_jsonl_path() -> PathBuf {
    PathBuf::from("logs/round_validation_glft_mm.jsonl")
}

fn round_validation_aggregate_path() -> PathBuf {
    PathBuf::from("logs/round_validation_aggregate.json")
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
        "♻️ Recycler trigger[{}]: evt.reason={} rejects={} free={:.2} allowance_ok={} mergeable={:.2} -> merge {:.2} USDC ({})",
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

type ResolvedMarket = (String, String, String, Option<u64>);

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

const CHAINLINK_HUB_TICK_STALL_RECONNECT_MS: u64 = 2_000;

#[derive(Clone)]
struct ChainlinkHub {
    senders: Arc<HashMap<String, broadcast::Sender<(f64, u64)>>>,
}

impl ChainlinkHub {
    fn spawn(symbols: HashSet<String>) -> Arc<Self> {
        let mut senders = HashMap::new();
        for sym in &symbols {
            let (tx, _rx) = broadcast::channel::<(f64, u64)>(2048);
            senders.insert(sym.clone(), tx);
        }
        let hub = Arc::new(Self {
            senders: Arc::new(senders),
        });
        let ws_url = post_close_chainlink_ws_url();
        // One WS connection per symbol: the server only honours ONE active
        // subscription per connection, so sharing a single connection for N
        // symbols only delivers ticks for the last-subscribed symbol.
        for sym in symbols {
            let tx = hub.senders.get(&sym).expect("sender just inserted").clone();
            let url = ws_url.clone();
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
                                if idle.as_millis() >= CHAINLINK_HUB_TICK_STALL_RECONNECT_MS as u128
                                {
                                    warn!(
                                        "⚠️ chainlink_hub_tick_stall_reconnect | symbol={} idle_ms={} msgs={} ticks={} delivered={} last_tick_ts_ms={:?}",
                                        sym,
                                        idle.as_millis(),
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
                            if idle.as_millis() >= CHAINLINK_HUB_TICK_STALL_RECONNECT_MS as u128 {
                                warn!(
                                    "⚠️ chainlink_hub_no_tick_payload_stall | symbol={} idle_ms={} msgs={} ticks={} delivered={} last_tick_ts_ms={:?}",
                                    sym,
                                    idle.as_millis(),
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
                            let _ = tx.send((price, ts_ms));
                            stat_delivered = stat_delivered.saturating_add(1);
                            matched_ticks = matched_ticks.saturating_add(1);
                            last_tick_at = Instant::now();
                            last_tick_ts_ms = Some(ts_ms);
                        }
                        stat_ticks = stat_ticks.saturating_add(matched_ticks);
                        if matched_ticks == 0 {
                            let idle = last_tick_at.elapsed();
                            if idle.as_millis() >= CHAINLINK_HUB_TICK_STALL_RECONNECT_MS as u128 {
                                warn!(
                                    "⚠️ chainlink_hub_symbol_mismatch_stall | symbol={} idle_ms={} msgs={} ticks={} delivered={} last_tick_ts_ms={:?}",
                                    sym,
                                    idle.as_millis(),
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

    fn subscribe(&self, symbol: &str) -> Option<broadcast::Receiver<(f64, u64)>> {
        let sym = normalize_chainlink_symbol(symbol);
        self.senders.get(&sym).map(|tx| tx.subscribe())
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

fn chainlink_round_alignment_path() -> PathBuf {
    PathBuf::from("logs/chainlink_round_alignment.jsonl")
}

fn chainlink_last_close_cache_path() -> PathBuf {
    PathBuf::from("logs/chainlink_last_close_cache.json")
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

fn winner_from_open_close(open: Option<f64>, close: Option<f64>) -> Option<Side> {
    let open = open?;
    let close = close?;
    Some(if close >= open { Side::Yes } else { Side::No })
}

fn side_label(side: Option<Side>) -> Option<String> {
    side.map(|s| format!("{:?}", s))
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

fn chainlink_prewarmed_open_map() -> &'static Mutex<PrewarmedOpenMap> {
    CHAINLINK_PREWARMED_OPEN.get_or_init(|| Mutex::new(HashMap::new()))
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
    let chainlink_task = tokio::spawn(async move {
        if let Some((side, open_ref, close_px, open_ts_ms, close_ts_ms, open_is_exact)) =
            run_chainlink_winner_hint(
                &cl_symbol,
                round_start_ts,
                round_end_ts,
                chainlink_deadline_ts,
                chainlink_hub.clone(),
            )
            .await
        {
            let detect_ms = unix_now_millis_u64();
            info!(
                "⏱️ chainlink_result_ready | slug={} symbol={} side={:?} open_exact={} open_ref={:.6} close={:.6} open_ts_ms={} close_ts_ms={} detect_ms={} latency_from_end_ms={}",
                cl_slug, cl_symbol, side, open_is_exact, open_ref, close_px,
                open_ts_ms, close_ts_ms, detect_ms,
                detect_ms.saturating_sub(market_end_ms),
            );
            return Some((
                WinnerHintSource::Chainlink,
                side,
                open_ref,
                close_px,
                detect_ms,
                open_is_exact,
            ));
        }
        None
    });
    let frontend_symbol = symbol.clone();
    let mut frontend_task = Some(tokio::spawn(async move {
        fetch_frontend_crypto_round_prices(&frontend_symbol, round_start_ts, round_end_ts).await
    }));

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

    // ── Chainlink result → emit WinnerHint ──
    let first = if let Some(v) = chainlink_result {
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
    let mut lagged_ticks: u64 = 0;
    let mut first_tick_ts_ms: Option<u64> = None;
    let mut last_tick_ts_ms: Option<u64> = None;
    let mut nearest_start: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)
    let mut nearest_end: Option<(u64, u64, f64)> = None; // (abs_delta_ms, ts_ms, price)
    let Some(mut rx) = hub.subscribe(&target_symbol) else {
        warn!(
            "⚠️ chainlink_hub_unsubscribed | symbol={} round_start_ts={} round_end_ts={}",
            target_symbol, round_start_ts, round_end_ts
        );
        return None;
    };

    while unix_now_secs() <= hard_deadline_ts {
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
        "⚠️ chainlink_winner_hint_unresolved | symbol={} round_start_ts={} round_end_ts={} deadline_ts={} observed_ticks={} lagged_ticks={} first_tick_ts_ms={:?} last_tick_ts_ms={:?} nearest_start={:?} nearest_end={:?}",
        target_symbol,
        round_start_ts,
        round_end_ts,
        hard_deadline_ts,
        observed_ticks,
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
) -> Option<(Side, f64, f64, u64, u64, bool)> {
    if chainlink_data_streams_enabled() {
        if let Some(hit) =
            run_data_streams_winner_hint(symbol, round_start_ts, round_end_ts, hard_deadline_ts)
                .await
        {
            return Some(hit);
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
        .await;
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
                    return Some((side, open_ref_px, close, open_ts_ms, close_ts_ms, true));
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
            return Some((side, open_ref_px, close, open_ts_ms, close_ts_ms, true));
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

                if price > 0.0 {
                    if let Some(ms) = market_side {
                        msgs.push(MarketDataMsg::TradeTick {
                            asset_id: asset_id.to_string(),
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

/// Parse one WS text payload and return parsed market-data messages plus ingest stats.
fn parse_ws_payload(settings: &Settings, text: &str) -> (Vec<MarketDataMsg>, u64, u64) {
    let mut out = Vec::new();
    let mut unknown_events = 0_u64;
    let mut parse_drops = 0_u64;

    let value = match serde_json::from_str::<Value>(text) {
        Ok(v) => v,
        Err(_) => return (out, unknown_events, 1),
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

        let parsed = parse_ws_message(settings, val);
        if known_event && parsed.is_empty() {
            parse_drops = parse_drops.saturating_add(1);
        }
        out.extend(parsed);
    }

    (out, unknown_events, parse_drops)
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

            // Emit once both sides are initialized and both bids are known.
            // Asks may be 0.0 post-close (no liquidity), which is meaningful.
            if self.yes_seen && self.no_seen && self.yes_bid > 0.0 && self.no_bid > 0.0 {
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
const MARKET_WS_HARD_CUTOFF_GRACE_SECS: u64 = 45;
// If we keep receiving raw payloads but never reconstruct a full 4-price book,
// reconnect proactively because strategy cannot trade without complete book.
const MARKET_WS_NO_FULL_BOOK_RECONNECT_SECS: u64 = 30;

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

async fn run_market_ws_with_wall_guard(
    settings: Settings,
    ofi_tx: mpsc::Sender<MarketDataMsg>,
    glft_tx: mpsc::Sender<MarketDataMsg>,
    coord_tx: watch::Sender<MarketDataMsg>,
    post_close_book_tx: mpsc::Sender<PostCloseSideBookUpdate>,
    end_ts: u64,
    recorder: Option<RecorderHandle>,
    recorder_meta: Option<RecorderSessionMeta>,
) -> MarketEnd {
    let hard_cutoff_ts = end_ts.saturating_add(MARKET_WS_HARD_CUTOFF_GRACE_SECS);
    let mut ws_task = tokio::spawn(run_market_ws(
        settings,
        ofi_tx,
        glft_tx,
        coord_tx,
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
                        MARKET_WS_HARD_CUTOFF_GRACE_SECS
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
    let shared_hub = if coord_cfg.strategy.is_oracle_lag_sniping() {
        let mut hub_symbols: HashSet<String> = HashSet::new();
        for prefix in &prefixes {
            if let Some(sym) = oracle_lag_symbol_from_slug(prefix) {
                hub_symbols.insert(format!("{}/usd", sym));
            } else {
                warn!(
                    "⚠️ in-proc supervisor: could not derive symbol from prefix='{}' — this slug will not have a Chainlink feed",
                    prefix
                );
            }
        }
        if hub_symbols.is_empty() {
            None
        } else {
            let mut log_symbols: Vec<String> = hub_symbols.iter().cloned().collect();
            log_symbols.sort();
            info!(
                "🛰️ shared chainlink_hub starting | symbols={}",
                log_symbols.join(",")
            );
            Some(ChainlinkHub::spawn(hub_symbols))
        }
    } else {
        None
    };

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
            chainlink_hub: shared_hub.clone(),
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
    post_close_book_tx: mpsc::Sender<PostCloseSideBookUpdate>,
    end_ts: u64,
    recorder: Option<RecorderHandle>,
    recorder_meta: Option<RecorderSessionMeta>,
) -> MarketEnd {
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
                        if write.send(Message::Text("PING".to_string())).await.is_err() {
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
                            if raw_silence >= Duration::from_secs(30) {
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

                            // Raw payloads exist, but no complete 4-price book ever formed (or has stalled)
                            // for too long. Strategy cannot trade in this state, so reconnect.
                            if session_raw_msg_count > 0
                                && no_full_book_silence
                                    >= Duration::from_secs(MARKET_WS_NO_FULL_BOOK_RECONNECT_SECS)
                            {
                                warn!(
                                    "⚠️ Market WS missing complete book for {:?} (raw={} parsed={} partial_yes={} partial_no={} full_book={} trade={} unknown={} dropped={} tx_dropped={}) — reconnecting",
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

                                    let (parsed, unknown_events, parse_drops) =
                                        parse_ws_payload(&settings, &text);
                                    session_unknown_event_count = session_unknown_event_count
                                        .saturating_add(unknown_events);
                                    session_parse_drop_count =
                                        session_parse_drop_count.saturating_add(parse_drops);

                                    for md_msg in parsed {
                                        session_parsed_msg_count =
                                            session_parsed_msg_count.saturating_add(1);
                                        match &md_msg {
                                            MarketDataMsg::TradeTick { .. } => {
                                                session_had_market_data = true;
                                                session_trade_tick_count =
                                                    session_trade_tick_count.saturating_add(1);
                                                last_market_data_at = tokio::time::Instant::now();
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
                                                if let Some(full) = book_asm.update(&md_msg) {
                                                    session_had_market_data = true;
                                                    session_book_tick_count =
                                                        session_book_tick_count.saturating_add(1);
                                                    last_market_data_at = tokio::time::Instant::now();
                                                    last_full_book_at = Some(last_market_data_at);
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
                                            let (parsed, unknown_events, parse_drops) =
                                                parse_ws_payload(&settings, text);
                                            session_unknown_event_count = session_unknown_event_count
                                                .saturating_add(unknown_events);
                                            session_parse_drop_count = session_parse_drop_count
                                                .saturating_add(parse_drops);

                                            for md_msg in parsed {
                                                session_parsed_msg_count =
                                                    session_parsed_msg_count.saturating_add(1);
                                                match &md_msg {
                                                    MarketDataMsg::TradeTick { .. } => {
                                                        session_had_market_data = true;
                                                        session_trade_tick_count =
                                                            session_trade_tick_count.saturating_add(1);
                                                        last_market_data_at = tokio::time::Instant::now();
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
                                                        if let Some(full) = book_asm.update(&md_msg) {
                                                            session_had_market_data = true;
                                                            session_book_tick_count =
                                                                session_book_tick_count
                                                                    .saturating_add(1);
                                                            last_market_data_at = tokio::time::Instant::now();
                                                            last_full_book_at = Some(last_market_data_at);
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

fn slug_lock_port(slug: &str) -> u16 {
    let mut h: u32 = 0x811c9dc5u32;
    for b in slug.bytes() {
        h = h.wrapping_mul(0x01000193).wrapping_add(b as u32);
    }
    // Dynamic/private port range: 49152–59999 (10848 slots)
    49152 + (h % 10848) as u16
}

fn try_acquire_slug_lock(slug: &str) -> Option<SlugLock> {
    let port = slug_lock_port(slug);
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
    // Dual-output logging: stdout + daily rolling file in logs/
    let file_appender = tracing_appender::rolling::daily("logs", &log_filename_prefix);
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

    let is_multi_market_child = env::var("PM_MULTI_MARKET_CHILD")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let multi_market_prefixes = parse_multi_market_prefixes_from_env();
    if !is_multi_market_child && multi_market_prefixes.len() > 1 {
        return run_multi_market_supervisor(multi_market_prefixes).await;
    }
    run_prefix_worker(None).await
}

/// Shared handles passed from the in-proc supervisor to each per-slug worker.
/// When present, overrides env-based slug resolution and lets every worker
/// share one ChainlinkHub (one WS connection, N symbol subscriptions).
struct WorkerCtx {
    slug: String,
    chainlink_hub: Option<Arc<ChainlinkHub>>,
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
    // conflict is possible and we skip the lock.
    let _slug_lock = if ctx.is_none()
        && prefix_mode
        && coord_cfg_base.strategy.is_oracle_lag_sniping()
    {
        match try_acquire_slug_lock(&raw_slug) {
            Some(lock) => {
                info!(
                    "🔒 slug_lock acquired | slug={} port={}",
                    raw_slug, lock.port
                );
                Some(lock)
            }
            None => {
                let port = slug_lock_port(&raw_slug);
                anyhow::bail!(
                    "🚨 slug_lock_conflict: another process is already running oracle_lag_sniping for slug='{}' (port {}). Exiting to prevent duplicate orders.",
                    raw_slug, port
                );
            }
        }
    } else {
        None
    };

    let chainlink_hub = if let Some(c) = &ctx {
        c.chainlink_hub.clone()
    } else if coord_cfg_base.strategy.is_oracle_lag_sniping() {
        let mut hub_symbols = HashSet::new();
        for base in oracle_lag_symbol_universe.hub_symbols() {
            hub_symbols.insert(format!("{}/usd", base));
        }
        let mut log_symbols: Vec<String> = hub_symbols.iter().cloned().collect();
        log_symbols.sort();
        info!(
            "🛰️ chainlink_hub starting | symbols={}",
            log_symbols.join(",")
        );
        Some(ChainlinkHub::spawn(hub_symbols))
    } else {
        None
    };
    let mut auto_claim_cfg = AutoClaimConfig::from_env();
    let round_claim_cfg = RoundClaimRunnerConfig::from_env();
    let recycle_cfg = CapitalRecycleConfig::from_env();
    let recorder = RecorderHandle::from_env();
    let mut auto_claim_state = AutoClaimState::default();
    let mut round_claim_task: Option<tokio::task::JoinHandle<()>> = None;

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
    #[allow(unused_imports)]
    use alloy::signers::Signer;
    let signer_address = signer.as_ref().map(|s| format!("{:?}", s.address()));

    // Startup preflight: force-refresh and inspect collateral balance/allowance.
    if !dry_run {
        use alloy::primitives::Address;
        use alloy::primitives::U256;
        use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
        use polymarket_client_sdk::clob::types::{AssetType, SignatureType};
        use polymarket_client_sdk::{contract_config, POLYGON};
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
                    let main_cfg = contract_config(POLYGON, false);
                    let neg_cfg = contract_config(POLYGON, true);
                    let expected_spenders: Vec<(&str, Option<Address>)> = vec![
                        ("exchange", main_cfg.map(|c| c.exchange)),
                        ("neg_risk_exchange", neg_cfg.map(|c| c.exchange)),
                        ("neg_risk_adapter", neg_cfg.and_then(|c| c.neg_risk_adapter)),
                    ];

                    info!(
                        "💰 Preflight collateral: balance={} max_allowance={} allowance_entries={}",
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
                            "⚠️ Preflight indicates insufficient balance/allowance for trading \
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
                                    "🧪 Collateral probe sig_type={} balance={} max_allowance={} entries={}",
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
                            "🚨 FATAL: wallet balance is non-zero but CLOB collateral allowance is zero. \
                             Use the same signer/funder to approve USDC for Polymarket contracts, then retry. \
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
            // P2 FIX: Cap secs_remaining to avoid Instant + Duration overflow panics
            (raw_slug.clone(), u64::MAX, u64::MAX) // Fixed mode: no expiry
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

                // Pre-resolve the NEXT market in the background while sleeping
                let next_slug_ts = expected_end_ts;
                let next_slug = format!("{}-{}", raw_slug, next_slug_ts);
                if preloading_slug.as_deref() != Some(next_slug.as_str()) {
                    preloading_slug = Some(next_slug.clone());
                    let p_tx = preload_tx.clone();
                    tokio::spawn(async move {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        let sleep_time = if expected_end_ts > now + 30 {
                            expected_end_ts - now - 30
                        } else {
                            0
                        };
                        if sleep_time > 0 {
                            tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                        }
                        info!(
                            "⏳ Pre-resolving next market in background during skip delay: {}",
                            next_slug
                        );
                        let res = resolve_market_with_retry(&next_slug).await;
                        let _ = p_tx.send((next_slug, res)).await;
                    });
                } else {
                    debug!(
                        "⏳ Skip duplicate pre-resolve spawn during skip-delay: {} already in-flight",
                        next_slug
                    );
                }

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

        let resolved = if let Some((pre_slug, pre_res)) = preloaded_market.take() {
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
                    // Polymarket returns collateral balance in 6 decimals (1 USDC = 1,000,000)
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
                            "💡 [DYNAMIC SIZING] Balance: {:.2} USDC -> target BID_SIZE={} MAX_NET_DIFF={} | floors BID_SIZE={:.1} MAX_NET_DIFF={:.1} -> effective BID_SIZE={:.1} MAX_NET_DIFF={:.1} (bid_pct={}, net_pct={})",
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
        let (inv_watch_tx, inv_watch_rx) = watch::channel(InventorySnapshot::default());
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
                }
                // Next-round prewarm (normal path): capture exact open before round N+1 starts.
                let next_round_start_ts = hint_round_end_ts;
                let prewarm_deadline_ts = next_round_start_ts.saturating_add(20);
                info!(
                    "🧠 chainlink_open_prewarm_start | symbol={} next_round_start_ts={} deadline_ts={}",
                    prewarm_symbol, next_round_start_ts, prewarm_deadline_ts
                );
                let prewarm_chainlink_hub = chainlink_hub.clone();
                session_handles.push(tokio::spawn(async move {
                    run_chainlink_open_prewarm(
                        &prewarm_symbol,
                        next_round_start_ts,
                        prewarm_deadline_ts,
                        prewarm_chainlink_hub,
                    )
                    .await;
                }));
            }
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

        let coord = StrategyCoordinator::with_aux_rx(
            coord_cfg.clone(),
            ofi_watch_rx,
            inv_watch_rx,
            coord_md_rx,
            winner_hint_rx,
            glft_watch_rx,
            om_tx.clone(),
            kill_rx,
            feedback_rx,
            slot_release_rx,
        )
        .with_recorder(recorder.clone(), recorder_meta.clone())
        .with_obs_tx(coord_obs_tx);
        session_handles.push(tokio::spawn(coord.run()));

        let om = OrderManager::new(om_rx, exec_tx.clone(), result_rx, slot_release_tx);
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
            },
            clob_client.clone(),
            signer.clone(),
            exec_rx,
            result_tx,
            exec_fill_rx,
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
            info!("📝 DRY-RUN: No User WS — net_diff stays 0 (no fills)");
            if coord_cfg.strategy.as_str() == "glft_mm" {
                info!(
                    "🧪 GLFT dry-run note: no fills means zero inventory, so SELL slots stay inventory-gated"
                );
            }
            // In DRY-RUN mode, fill_tx is unused, fill_rx sees nothing.
            // InventoryManager stays at default state → Coordinator always Balanced.
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
            recorder.emit_session_start(&recorder_meta, ws_round_end_ts);
        }
        let reason = run_market_ws_with_wall_guard(
            settings,
            ofi_md_tx,
            glft_md_tx,
            coord_md_tx,
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
        if market_settled && recorder.enabled() {
            recorder.emit_own_inventory_event(
                &recorder_meta,
                "market_resolved",
                json!({
                    "reason": format!("{:?}", reason),
                    "round_end_ts": ws_round_end_ts,
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
            if let Some(handle) = round_claim_task.take() {
                let wait_secs = round_claim_cfg.window.as_secs().saturating_add(5);
                info!(
                    "💸 Fixed mode: waiting up to {}s for background round-claim task before exit",
                    wait_secs
                );
                match tokio::time::timeout(Duration::from_secs(wait_secs), handle).await {
                    Ok(joined) => {
                        if let Err(e) = joined {
                            warn!("⚠️ Round claim background task join failed: {:?}", e);
                        }
                    }
                    Err(_) => warn!("⚠️ Round claim background task timed out on fixed-mode exit"),
                }
            }
            info!("📌 Fixed mode — exiting");
            break;
        }

        // Background preload for next market
        if prefix_mode {
            let next_slug_ts = expected_end_ts;
            let next_slug = format!("{}-{}", raw_slug, next_slug_ts);

            if preloading_slug.as_deref() != Some(next_slug.as_str()) {
                preloading_slug = Some(next_slug.clone());
                let p_tx = preload_tx.clone();
                tokio::spawn(async move {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let sleep_time = if expected_end_ts > now + 30 {
                        expected_end_ts - now - 30
                    } else {
                        0
                    };
                    if sleep_time > 0 {
                        tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                    }
                    info!("⏳ Pre-resolving next market in background: {}", next_slug);

                    let res = resolve_market_with_retry(&next_slug).await;
                    let _ = p_tx.send((next_slug, res)).await;
                });
            } else {
                debug!(
                    "⏳ Skip duplicate pre-resolve spawn: {} already in-flight",
                    next_slug
                );
            }
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
