use super::messages::TakerSide;
use super::types::Side;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecorderConfig {
    pub enabled: bool,
    pub root: PathBuf,
    pub md_queue_cap: usize,
    pub ops_queue_cap: usize,
    pub flush_every: Duration,
    pub market_mode: RecorderMarketMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecorderMarketMode {
    Structured,
    Raw,
    Hybrid,
}

impl RecorderMarketMode {
    fn from_env_value(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "raw" => Self::Raw,
            "hybrid" => Self::Hybrid,
            _ => Self::Structured,
        }
    }

    fn captures_raw(self) -> bool {
        matches!(self, Self::Raw | Self::Hybrid)
    }

    fn captures_structured(self) -> bool {
        matches!(self, Self::Structured | Self::Hybrid)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Structured => "structured",
            Self::Raw => "raw",
            Self::Hybrid => "hybrid",
        }
    }
}

impl RecorderConfig {
    pub fn from_env() -> Self {
        let enabled = std::env::var("PM_RECORDER_ENABLED")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let root = std::env::var("PM_RECORDER_ROOT")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("data/recorder"));
        let md_queue_cap = std::env::var("PM_RECORDER_MD_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(16_384);
        let ops_queue_cap = std::env::var("PM_RECORDER_OPS_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2_048);
        let flush_every_ms = std::env::var("PM_RECORDER_FLUSH_EVERY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(250);
        let market_mode = std::env::var("PM_RECORDER_MARKET_MODE")
            .ok()
            .map(|v| RecorderMarketMode::from_env_value(&v))
            .unwrap_or(RecorderMarketMode::Structured);

        Self {
            enabled,
            root,
            md_queue_cap,
            ops_queue_cap,
            flush_every: Duration::from_millis(flush_every_ms),
            market_mode,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecorderSessionMeta {
    pub slug: String,
    pub condition_id: String,
    pub market_id: String,
    pub strategy: String,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecorderSessionStart {
    pub round_start_ts: u64,
    pub round_end_ts: u64,
    pub yes_asset_id: String,
    pub no_asset_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RecorderStream {
    MarketWs,
    MarketMd,
    UserWs,
    Events,
    Meta,
}

impl RecorderStream {
    fn as_str(self) -> &'static str {
        match self {
            Self::MarketWs => "market_ws",
            Self::MarketMd => "market_md",
            Self::UserWs => "user_ws",
            Self::Events => "events",
            Self::Meta => "meta",
        }
    }

    fn file_name(self) -> &'static str {
        match self {
            Self::MarketWs => "market_ws.jsonl",
            Self::MarketMd => "market_md.jsonl",
            Self::UserWs => "user_ws.jsonl",
            Self::Events => "events.jsonl",
            Self::Meta => "meta.jsonl",
        }
    }
}

#[derive(Debug, Clone)]
struct RecorderEnvelope {
    recv_unix_ms: u64,
    recv_monotonic_ns: u64,
    stream: RecorderStream,
    slug: String,
    condition_id: String,
    market_id: String,
    strategy: String,
    dry_run: bool,
    payload: Value,
}

#[derive(Clone)]
pub struct RecorderHandle {
    enabled: bool,
    market_mode: RecorderMarketMode,
    md_tx: Option<mpsc::Sender<RecorderEnvelope>>,
    ops_tx: Option<mpsc::Sender<RecorderEnvelope>>,
    md_drop_count: Arc<AtomicU64>,
    critical_drop_count: Arc<AtomicU64>,
}

impl RecorderHandle {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            market_mode: RecorderMarketMode::Structured,
            md_tx: None,
            ops_tx: None,
            md_drop_count: Arc::new(AtomicU64::new(0)),
            critical_drop_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn from_config(cfg: &RecorderConfig) -> Self {
        if !cfg.enabled {
            return Self::disabled();
        }

        let (md_tx, mut md_rx) = mpsc::channel::<RecorderEnvelope>(cfg.md_queue_cap);
        let (ops_tx, mut ops_rx) = mpsc::channel::<RecorderEnvelope>(cfg.ops_queue_cap);
        let root = cfg.root.clone();
        let flush_every = cfg.flush_every;

        tokio::spawn(async move {
            let mut state = RecorderWriterState::new(root);
            let mut flush_tick = tokio::time::interval(flush_every);
            flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut md_closed = false;
            let mut ops_closed = false;

            loop {
                tokio::select! {
                    biased;

                    maybe_env = ops_rx.recv(), if !ops_closed => {
                        match maybe_env {
                            Some(env) => {
                                if let Err(e) = state.write_envelope(env) {
                                    warn!("⚠️ recorder write(op) failed: {:?}", e);
                                }
                            }
                            None => {
                                ops_closed = true;
                            }
                        }
                    }

                    maybe_env = md_rx.recv(), if !md_closed => {
                        match maybe_env {
                            Some(env) => {
                                if let Err(e) = state.write_envelope(env) {
                                    warn!("⚠️ recorder write(md) failed: {:?}", e);
                                }
                            }
                            None => {
                                md_closed = true;
                            }
                        }
                    }

                    _ = flush_tick.tick() => {
                        if let Err(e) = state.flush_all() {
                            warn!("⚠️ recorder flush failed: {:?}", e);
                        }
                    }
                }

                if md_closed && ops_closed {
                    if let Err(e) = state.flush_all() {
                        warn!("⚠️ recorder final flush failed: {:?}", e);
                    }
                    break;
                }
            }
        });

        info!(
            "📼 recorder enabled | root={} market_mode={} md_queue_cap={} ops_queue_cap={} flush_every_ms={}",
            cfg.root.display(),
            cfg.market_mode.as_str(),
            cfg.md_queue_cap,
            cfg.ops_queue_cap,
            cfg.flush_every.as_millis(),
        );

        Self {
            enabled: true,
            market_mode: cfg.market_mode,
            md_tx: Some(md_tx),
            ops_tx: Some(ops_tx),
            md_drop_count: Arc::new(AtomicU64::new(0)),
            critical_drop_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn from_env() -> Self {
        let cfg = RecorderConfig::from_env();
        Self::from_config(&cfg)
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn md_drop_count(&self) -> u64 {
        self.md_drop_count.load(Ordering::Relaxed)
    }

    pub fn critical_drop_count(&self) -> u64 {
        self.critical_drop_count.load(Ordering::Relaxed)
    }

    pub fn emit_session_start(&self, meta: &RecorderSessionMeta, session: &RecorderSessionStart) {
        self.try_send_ops(
            meta,
            json!({
                "event": "session_start",
                "round_start_ts": session.round_start_ts,
                "round_end_ts": session.round_end_ts,
                "yes_asset_id": session.yes_asset_id,
                "no_asset_id": session.no_asset_id,
            }),
            RecorderStream::Meta,
        );
    }

    pub fn emit_session_end(&self, meta: &RecorderSessionMeta, reason: &str) {
        self.try_send_ops(
            meta,
            json!({
                "event": "session_end",
                "reason": reason,
            }),
            RecorderStream::Meta,
        );
    }

    pub fn record_market_ws_raw(&self, meta: &RecorderSessionMeta, payload_text: &str) {
        if !self.market_mode.captures_raw() {
            return;
        }
        self.try_send_md(
            meta,
            json!({
                "raw_text": payload_text,
            }),
            RecorderStream::MarketWs,
        );
    }

    pub fn record_market_book_l1(
        &self,
        meta: &RecorderSessionMeta,
        yes_bid: f64,
        yes_ask: f64,
        no_bid: f64,
        no_ask: f64,
    ) {
        if !self.market_mode.captures_structured() {
            return;
        }
        self.try_send_md(
            meta,
            json!({
                "kind": "book_l1",
                "yes_bid": yes_bid,
                "yes_ask": yes_ask,
                "no_bid": no_bid,
                "no_ask": no_ask,
            }),
            RecorderStream::MarketMd,
        );
    }

    pub fn record_market_trade(
        &self,
        meta: &RecorderSessionMeta,
        asset_id: &str,
        market_side: Side,
        taker_side: TakerSide,
        price: f64,
        size: f64,
        trade_id: Option<&str>,
        trade_ts_ms: Option<u64>,
    ) {
        if !self.market_mode.captures_structured() {
            return;
        }
        self.try_send_md(
            meta,
            json!({
                "kind": "trade",
                "asset_id": asset_id,
                "market_side": market_side.as_str(),
                "taker_side": taker_side.as_str(),
                "price": price,
                "size": size,
                "trade_id": trade_id.unwrap_or_default(),
                "trade_ts_ms": trade_ts_ms.unwrap_or_default(),
            }),
            RecorderStream::MarketMd,
        );
    }

    pub fn record_user_ws_raw(&self, meta: &RecorderSessionMeta, payload_text: &str) {
        self.try_send_ops(
            meta,
            json!({
                "raw_text": payload_text,
            }),
            RecorderStream::UserWs,
        );
    }

    pub fn emit_own_order_event(&self, meta: &RecorderSessionMeta, event: &str, payload: Value) {
        self.try_send_ops(
            meta,
            json!({
                "event": event,
                "data": payload,
            }),
            RecorderStream::Events,
        );
    }

    pub fn emit_own_inventory_event(
        &self,
        meta: &RecorderSessionMeta,
        event: &str,
        payload: Value,
    ) {
        self.try_send_ops(
            meta,
            json!({
                "event": event,
                "data": payload,
            }),
            RecorderStream::Events,
        );
    }

    pub fn emit_redeem_result(&self, meta: &RecorderSessionMeta, payload: Value) {
        self.try_send_ops(
            meta,
            json!({
                "event": "redeem_result",
                "data": payload,
            }),
            RecorderStream::Events,
        );
    }

    fn try_send_md(&self, meta: &RecorderSessionMeta, payload: Value, stream: RecorderStream) {
        if !self.enabled {
            return;
        }
        let Some(tx) = &self.md_tx else {
            return;
        };
        let env = RecorderEnvelope {
            recv_unix_ms: unix_now_millis(),
            recv_monotonic_ns: monotonic_now_ns(),
            stream,
            slug: meta.slug.clone(),
            condition_id: meta.condition_id.clone(),
            market_id: meta.market_id.clone(),
            strategy: meta.strategy.clone(),
            dry_run: meta.dry_run,
            payload,
        };
        if tx.try_send(env).is_err() {
            let dropped = self.md_drop_count.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped % 100 == 1 {
                warn!("⚠️ recorder md queue full: md_drop_count={}", dropped);
            }
        }
    }

    fn try_send_ops(&self, meta: &RecorderSessionMeta, payload: Value, stream: RecorderStream) {
        if !self.enabled {
            return;
        }
        let Some(tx) = &self.ops_tx else {
            return;
        };
        let env = RecorderEnvelope {
            recv_unix_ms: unix_now_millis(),
            recv_monotonic_ns: monotonic_now_ns(),
            stream,
            slug: meta.slug.clone(),
            condition_id: meta.condition_id.clone(),
            market_id: meta.market_id.clone(),
            strategy: meta.strategy.clone(),
            dry_run: meta.dry_run,
            payload,
        };
        if tx.try_send(env).is_err() {
            let dropped = self.critical_drop_count.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                "⚠️ recorder ops queue full: critical_drop_count={} (this should stay 0)",
                dropped
            );
        }
    }
}

struct RecorderWriterState {
    root: PathBuf,
    seq: u64,
    writers: HashMap<(String, String, RecorderStream), BufWriter<File>>,
}

impl RecorderWriterState {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            seq: 0,
            writers: HashMap::new(),
        }
    }

    fn write_envelope(&mut self, env: RecorderEnvelope) -> anyhow::Result<()> {
        self.seq = self.seq.saturating_add(1);
        let capture_seq = self.seq;
        let date = ymd_utc(env.recv_unix_ms);
        let slug_for_key = env.slug.clone();

        let line = json!({
            "capture_seq": capture_seq,
            "recv_unix_ms": env.recv_unix_ms,
            "recv_monotonic_ns": env.recv_monotonic_ns,
            "stream": env.stream.as_str(),
            "slug": env.slug,
            "condition_id": env.condition_id,
            "market_id": env.market_id,
            "strategy": env.strategy,
            "dry_run": env.dry_run,
            "payload": env.payload,
        });

        let key = (date, slug_for_key, env.stream);
        let writer = self.writer_for(&key)?;
        serde_json::to_writer(&mut *writer, &line)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn writer_for(
        &mut self,
        key: &(String, String, RecorderStream),
    ) -> anyhow::Result<&mut BufWriter<File>> {
        if !self.writers.contains_key(key) {
            let (date, slug, stream) = key;
            let dir = self.root.join(date).join(slug);
            create_dir_all(&dir)?;
            let path = dir.join(stream.file_name());
            let file = open_append(&path)?;
            self.writers
                .insert(key.clone(), BufWriter::with_capacity(256 * 1024, file));
        }
        Ok(self.writers.get_mut(key).expect("writer inserted"))
    }

    fn flush_all(&mut self) -> anyhow::Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

fn open_append(path: &Path) -> anyhow::Result<File> {
    Ok(OpenOptions::new().create(true).append(true).open(path)?)
}

fn ymd_utc(unix_ms: u64) -> String {
    let dt = Utc
        .timestamp_millis_opt(unix_ms as i64)
        .single()
        .unwrap_or_else(Utc::now);
    dt.format("%Y-%m-%d").to_string()
}

fn unix_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn monotonic_now_ns() -> u64 {
    static MONO_START: OnceLock<Instant> = OnceLock::new();
    let start = MONO_START.get_or_init(Instant::now);
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(prefix: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("pm_as_ofi_{}_{}", prefix, ts))
    }

    fn test_meta(slug: &str) -> RecorderSessionMeta {
        RecorderSessionMeta {
            slug: slug.to_string(),
            condition_id: "0xcond".to_string(),
            market_id: "0xmarket".to_string(),
            strategy: "oracle_lag_sniping".to_string(),
            dry_run: true,
        }
    }

    fn read_jsonl(path: &Path) -> Vec<serde_json::Value> {
        let Ok(raw) = fs::read_to_string(path) else {
            return vec![];
        };
        raw.lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .collect()
    }

    fn session_start() -> RecorderSessionStart {
        RecorderSessionStart {
            round_start_ts: 100,
            round_end_ts: 200,
            yes_asset_id: "yes-asset".to_string(),
            no_asset_id: "no-asset".to_string(),
        }
    }

    #[tokio::test]
    async fn recorder_disabled_writes_nothing() {
        let root = temp_root("recorder_disabled");
        let cfg = RecorderConfig {
            enabled: false,
            root: root.clone(),
            md_queue_cap: 8,
            ops_queue_cap: 8,
            flush_every: Duration::from_millis(5),
            market_mode: RecorderMarketMode::Structured,
        };
        let rec = RecorderHandle::from_config(&cfg);
        let meta = test_meta("btc-updown-5m-test");
        rec.emit_session_start(&meta, &session_start());
        rec.record_market_ws_raw(&meta, r#"{"event_type":"book"}"#);
        rec.emit_session_end(&meta, "Expired");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!root.exists(), "disabled recorder should not create files");
    }

    #[tokio::test]
    async fn recorder_enabled_writes_structured_jsonl_with_monotonic_seq() {
        let root = temp_root("recorder_enabled");
        let cfg = RecorderConfig {
            enabled: true,
            root: root.clone(),
            md_queue_cap: 64,
            ops_queue_cap: 64,
            flush_every: Duration::from_millis(5),
            market_mode: RecorderMarketMode::Structured,
        };
        let rec = RecorderHandle::from_config(&cfg);
        let meta = test_meta("btc-updown-5m-test");

        rec.emit_session_start(&meta, &session_start());
        rec.record_market_book_l1(&meta, 0.48, 0.52, 0.47, 0.53);
        rec.record_market_trade(
            &meta,
            "yes-asset",
            Side::Yes,
            TakerSide::Buy,
            0.51,
            10.0,
            None,
            Some(1_746_000_000_000),
        );
        rec.emit_own_order_event(
            &meta,
            "intent_sent",
            json!({"side":"Yes","direction":"Buy","size":5.0,"price":0.5}),
        );
        rec.emit_session_end(&meta, "Expired");
        drop(rec);

        tokio::time::sleep(Duration::from_millis(80)).await;

        let date = ymd_utc(unix_now_millis());
        let dir = root.join(date).join(meta.slug);
        let meta_rows = read_jsonl(&dir.join("meta.jsonl"));
        let market_rows = read_jsonl(&dir.join("market_md.jsonl"));
        let event_rows = read_jsonl(&dir.join("events.jsonl"));

        assert!(meta_rows.len() >= 2, "session start/end should be recorded");
        assert_eq!(
            market_rows.len(),
            2,
            "structured market payloads should be recorded"
        );
        assert_eq!(
            market_rows[1]["payload"]["trade_ts_ms"].as_u64(),
            Some(1_746_000_000_000),
            "structured trade should carry trade_ts_ms"
        );
        assert_eq!(event_rows.len(), 1, "own_order_events should be recorded");
        assert_eq!(
            meta_rows[0]["payload"]["round_start_ts"].as_u64(),
            Some(100),
            "session_start should include round_start_ts"
        );
        assert_eq!(
            meta_rows[0]["payload"]["yes_asset_id"].as_str(),
            Some("yes-asset"),
            "session_start should include asset ids"
        );

        let mut seqs: Vec<u64> = meta_rows
            .iter()
            .chain(market_rows.iter())
            .chain(event_rows.iter())
            .filter_map(|v| v.get("capture_seq").and_then(|x| x.as_u64()))
            .collect();
        assert!(!seqs.is_empty(), "capture_seq should exist");
        seqs.sort_unstable();
        for w in seqs.windows(2) {
            assert!(w[1] > w[0], "capture_seq must be strictly increasing");
        }

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn recorder_market_modes_split_raw_and_structured_streams() {
        let modes = [
            (
                RecorderMarketMode::Structured,
                false,
                true,
                "recorder_mode_structured",
            ),
            (RecorderMarketMode::Raw, true, false, "recorder_mode_raw"),
            (
                RecorderMarketMode::Hybrid,
                true,
                true,
                "recorder_mode_hybrid",
            ),
        ];

        for (market_mode, expect_raw, expect_structured, label) in modes {
            let root = temp_root(label);
            let cfg = RecorderConfig {
                enabled: true,
                root: root.clone(),
                md_queue_cap: 64,
                ops_queue_cap: 64,
                flush_every: Duration::from_millis(5),
                market_mode,
            };
            let rec = RecorderHandle::from_config(&cfg);
            let meta = test_meta(label);

            rec.emit_session_start(&meta, &session_start());
            rec.record_market_ws_raw(&meta, r#"{"event_type":"book"}"#);
            rec.record_market_book_l1(&meta, 0.48, 0.52, 0.47, 0.53);
            rec.record_market_trade(
                &meta,
                "yes-asset",
                Side::Yes,
                TakerSide::Sell,
                0.49,
                2.0,
                Some("tid-1"),
                Some(1_746_000_000_100),
            );
            drop(rec);

            tokio::time::sleep(Duration::from_millis(80)).await;

            let date = ymd_utc(unix_now_millis());
            let dir = root.join(date).join(meta.slug);
            let raw_rows = read_jsonl(&dir.join("market_ws.jsonl"));
            let structured_rows = read_jsonl(&dir.join("market_md.jsonl"));

            assert_eq!(
                !raw_rows.is_empty(),
                expect_raw,
                "raw rows presence should match market mode {}",
                market_mode.as_str()
            );
            assert_eq!(
                !structured_rows.is_empty(),
                expect_structured,
                "structured rows presence should match market mode {}",
                market_mode.as_str()
            );

            let _ = fs::remove_dir_all(root);
        }
    }
}
