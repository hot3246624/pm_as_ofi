use std::env;
use std::fmt;
use std::str::FromStr;

use rusqlite::{params, Connection};

const PAIR_ARB_NET_EPS: f64 = 0.001;
const TIER_1_NET_DIFF: f64 = 5.0;
const TIER_2_NET_DIFF: f64 = 10.0;
const RISK_INCR_TIER_1_NET_DIFF: f64 = 3.5;
const RISK_INCR_TIER_2_NET_DIFF: f64 = 8.0;
const EARLY_SKEW_MULT: f64 = 0.35;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillModel {
    Conservative,
    Aggressive,
}

impl FromStr for FillModel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "conservative" => Ok(Self::Conservative),
            "aggressive" => Ok(Self::Aggressive),
            other => Err(format!("unsupported fill model: {other}")),
        }
    }
}

impl fmt::Display for FillModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FillModel::Conservative => write!(f, "conservative"),
            FillModel::Aggressive => write!(f, "aggressive"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TierMode {
    Disabled,
    Discrete,
    Continuous,
}

impl FromStr for TierMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" | "false" => Ok(Self::Disabled),
            "discrete" | "step" | "bucket" => Ok(Self::Discrete),
            "continuous" | "smooth" | "non_discrete" | "nondiscrete" => Ok(Self::Continuous),
            other => Err(format!("unsupported tier mode: {other}")),
        }
    }
}

impl fmt::Display for TierMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TierMode::Disabled => write!(f, "disabled"),
            TierMode::Discrete => write!(f, "discrete"),
            TierMode::Continuous => write!(f, "continuous"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Config {
    pair_target: f64,
    bid_size: f64,
    max_net_diff: f64,
    tick_size: f64,
    tier_1_mult: f64,
    tier_2_mult: f64,
    tier_mode: TierMode,
    risk_open_cutoff_secs: f64,
    pair_cost_safety_margin: f64,
    as_skew_factor: f64,
    fill_model: FillModel,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pair_target: 0.97,
            bid_size: 5.0,
            max_net_diff: 5.0,
            tick_size: 0.01,
            tier_1_mult: 0.50,
            tier_2_mult: 0.15,
            tier_mode: TierMode::Discrete,
            risk_open_cutoff_secs: 240.0,
            pair_cost_safety_margin: 0.02,
            as_skew_factor: 0.06,
            fill_model: FillModel::Conservative,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Inventory {
    yes_qty: f64,
    yes_avg_cost: f64,
    no_qty: f64,
    no_avg_cost: f64,
    net_diff: f64,
}

impl Inventory {
    fn update_after_fill(&mut self, side_yes: bool, qty: f64, price: f64) {
        if side_yes {
            let new_total = self.yes_qty + qty;
            if new_total > 0.0 {
                self.yes_avg_cost = (self.yes_qty * self.yes_avg_cost + qty * price) / new_total;
            }
            self.yes_qty = new_total;
        } else {
            let new_total = self.no_qty + qty;
            if new_total > 0.0 {
                self.no_avg_cost = (self.no_qty * self.no_avg_cost + qty * price) / new_total;
            }
            self.no_qty = new_total;
        }
        self.net_diff = self.yes_qty - self.no_qty;
    }
}

#[derive(Debug, Clone, Copy)]
struct Tick {
    ts: i64,
    remaining_sec: f64,
    ask_up: f64,
    bid_up: f64,
    ask_down: f64,
    bid_down: f64,
}

#[derive(Debug, Clone)]
struct Settlement {
    condition_id: String,
    outcome: String,
}

#[derive(Debug, Clone, Copy)]
enum RiskEffect {
    Pairing,
    RiskIncreasing,
}

#[derive(Debug, Clone, Copy)]
struct Quotes {
    yes_bid: f64,
    yes_size: f64,
    no_bid: f64,
    no_size: f64,
}

#[derive(Debug, Clone, Copy)]
struct WindowResult {
    paired_qty: f64,
    pair_cost: f64,
    paired_pnl: f64,
    residual_yes: f64,
    residual_no: f64,
    residual_pnl: f64,
    total_pnl: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Aggregate {
    windows_tested: u64,
    total_fills: u64,
    total_paired_qty: f64,
    total_paired_pnl: f64,
    total_residual_pnl: f64,
    total_pnl: f64,
    perfect_pair_windows: u64,
    windows_with_residual: u64,
    residual_wins: u64,
    residual_losses: u64,
    wins: u64,
    losses: u64,
    zeros: u64,
    sum_wins: f64,
    sum_losses: f64,
}

fn risk_effect(inv: Inventory, side_yes: bool, size: f64) -> RiskEffect {
    let projected = if side_yes {
        (inv.net_diff + size).abs()
    } else {
        (inv.net_diff - size).abs()
    };
    if projected <= inv.net_diff.abs() + PAIR_ARB_NET_EPS {
        RiskEffect::Pairing
    } else {
        RiskEffect::RiskIncreasing
    }
}

fn candidate_size(inv: Inventory, side_yes: bool, bid_size: f64) -> f64 {
    if bid_size <= PAIR_ARB_NET_EPS {
        return bid_size.max(0.0);
    }

    let dominant_yes = if inv.net_diff > PAIR_ARB_NET_EPS {
        Some(true)
    } else if inv.net_diff < -PAIR_ARB_NET_EPS {
        Some(false)
    } else {
        None
    };
    let pairing_side_yes = dominant_yes.map(|d| !d);

    if pairing_side_yes == Some(side_yes) {
        let d = inv.net_diff.abs().max(0.0);
        let sized = (d * 100.0).floor() / 100.0;
        if sized >= 0.01 {
            sized
        } else {
            bid_size
        }
    } else {
        bid_size
    }
}

fn continuous_tier_multiplier(abs_net: f64, tier_1_mult: f64, tier_2_mult: f64) -> Option<f64> {
    if abs_net <= PAIR_ARB_NET_EPS {
        return None;
    }
    if abs_net <= RISK_INCR_TIER_1_NET_DIFF {
        let ratio = (abs_net / RISK_INCR_TIER_1_NET_DIFF).clamp(0.0, 1.0);
        return Some(1.0 + (tier_1_mult - 1.0) * ratio);
    }
    if abs_net < RISK_INCR_TIER_2_NET_DIFF {
        let ratio = ((abs_net - RISK_INCR_TIER_1_NET_DIFF)
            / (RISK_INCR_TIER_2_NET_DIFF - RISK_INCR_TIER_1_NET_DIFF))
            .clamp(0.0, 1.0);
        return Some(tier_1_mult + (tier_2_mult - tier_1_mult) * ratio);
    }
    Some(tier_2_mult)
}

fn tier_cap_price(
    inv: Inventory,
    side_yes: bool,
    risk_effect: RiskEffect,
    tier_mode: TierMode,
    tier_1_mult: f64,
    tier_2_mult: f64,
) -> Option<f64> {
    if !matches!(risk_effect, RiskEffect::RiskIncreasing) {
        return None;
    }

    let abs_net = inv.net_diff.abs();
    let mult = match tier_mode {
        TierMode::Disabled => return None,
        TierMode::Discrete => {
            if abs_net + PAIR_ARB_NET_EPS >= RISK_INCR_TIER_2_NET_DIFF {
                Some(tier_2_mult)
            } else if abs_net + PAIR_ARB_NET_EPS >= RISK_INCR_TIER_1_NET_DIFF {
                Some(tier_1_mult)
            } else {
                None
            }
        }
        TierMode::Continuous => continuous_tier_multiplier(abs_net, tier_1_mult, tier_2_mult),
    }?;

    if side_yes {
        if inv.yes_qty > f64::EPSILON && inv.yes_avg_cost > 0.0 {
            Some(inv.yes_avg_cost * mult)
        } else {
            None
        }
    } else if inv.no_qty > f64::EPSILON && inv.no_avg_cost > 0.0 {
        Some(inv.no_avg_cost * mult)
    } else {
        None
    }
}

fn vwap_ceiling(
    pair_target: f64,
    safety_margin: f64,
    opp_avg: f64,
    same_qty: f64,
    same_avg: f64,
    bid_size: f64,
) -> f64 {
    let guarded_target = (pair_target - safety_margin).max(0.0);
    let legacy = guarded_target - opp_avg;
    if same_qty <= PAIR_ARB_NET_EPS || bid_size <= PAIR_ARB_NET_EPS {
        return legacy;
    }

    let numerator = (guarded_target - opp_avg) * (same_qty + bid_size) - same_qty * same_avg;
    let ceiling = numerator / bid_size;
    if ceiling.is_finite() {
        ceiling
    } else {
        legacy
    }
}

fn safe_price(price: f64, tick: f64) -> f64 {
    let p = (price / tick).round() * tick;
    if p < tick {
        return 0.0;
    }
    p.min(0.99)
}

fn effective_skew_factor(
    base: f64,
    abs_net_diff: f64,
    time_decay: f64,
    tier_mode: TierMode,
) -> f64 {
    if matches!(tier_mode, TierMode::Disabled) {
        return base * time_decay;
    }
    if abs_net_diff < TIER_1_NET_DIFF {
        return base * EARLY_SKEW_MULT;
    }
    if abs_net_diff < TIER_2_NET_DIFF {
        let ramp = (abs_net_diff - TIER_1_NET_DIFF) / (TIER_2_NET_DIFF - TIER_1_NET_DIFF);
        return base * (EARLY_SKEW_MULT + (1.0 - EARLY_SKEW_MULT) * ramp) * time_decay;
    }
    base * time_decay
}

fn compute_quotes(cfg: Config, inv: Inventory, tick: Tick, total_window_sec: f64) -> Quotes {
    let mid_yes = (tick.bid_up + tick.ask_up) / 2.0;
    let mid_no = (tick.bid_down + tick.ask_down) / 2.0;

    let excess = (mid_yes + mid_no - cfg.pair_target).max(0.0);
    let skew = if cfg.max_net_diff > 0.0 {
        (inv.net_diff / cfg.max_net_diff).clamp(-1.0, 1.0)
    } else {
        0.0
    };

    let elapsed = total_window_sec - tick.remaining_sec;
    let time_decay = 1.0 + (elapsed / total_window_sec) * 0.6;

    let effective_skew = effective_skew_factor(
        cfg.as_skew_factor,
        inv.net_diff.abs(),
        time_decay,
        cfg.tier_mode,
    );
    let skew_shift = skew * effective_skew;

    let mut raw_yes = mid_yes - (excess / 2.0) - skew_shift;
    let mut raw_no = mid_no - (excess / 2.0) + skew_shift;

    if raw_yes + raw_no > cfg.pair_target {
        let overflow = (raw_yes + raw_no) - cfg.pair_target;
        raw_yes -= overflow / 2.0;
        raw_no -= overflow / 2.0;
    }

    let yes_size = candidate_size(inv, true, cfg.bid_size);
    let no_size = candidate_size(inv, false, cfg.bid_size);
    let yes_re = risk_effect(inv, true, yes_size);
    let no_re = risk_effect(inv, false, no_size);

    if let Some(cap) = tier_cap_price(
        inv,
        true,
        yes_re,
        cfg.tier_mode,
        cfg.tier_1_mult,
        cfg.tier_2_mult,
    ) {
        raw_yes = raw_yes.min(cap);
    }
    if let Some(cap) = tier_cap_price(
        inv,
        false,
        no_re,
        cfg.tier_mode,
        cfg.tier_1_mult,
        cfg.tier_2_mult,
    ) {
        raw_no = raw_no.min(cap);
    }

    let effective_margin = if inv.net_diff.abs() < PAIR_ARB_NET_EPS {
        0.0
    } else {
        cfg.pair_cost_safety_margin
    };

    let mut disable_yes = false;
    let mut disable_no = false;

    if inv.no_qty > 1e-9 && inv.no_avg_cost > 0.0 {
        let ceiling = vwap_ceiling(
            cfg.pair_target,
            effective_margin,
            inv.no_avg_cost,
            inv.yes_qty,
            inv.yes_avg_cost,
            yes_size,
        );
        raw_yes = raw_yes.min(ceiling);
        if ceiling <= cfg.tick_size + 1e-9 {
            disable_yes = true;
        }
    }

    if inv.yes_qty > 1e-9 && inv.yes_avg_cost > 0.0 {
        let ceiling = vwap_ceiling(
            cfg.pair_target,
            effective_margin,
            inv.yes_avg_cost,
            inv.no_qty,
            inv.no_avg_cost,
            no_size,
        );
        raw_no = raw_no.min(ceiling);
        if ceiling <= cfg.tick_size + 1e-9 {
            disable_no = true;
        }
    }

    if tick.ask_up > 0.0 && matches!(yes_re, RiskEffect::RiskIncreasing) {
        raw_yes = raw_yes.min(tick.ask_up - cfg.tick_size);
    }
    if tick.ask_down > 0.0 && matches!(no_re, RiskEffect::RiskIncreasing) {
        raw_no = raw_no.min(tick.ask_down - cfg.tick_size);
    }

    let mut yes_bid = if disable_yes {
        0.0
    } else {
        safe_price(raw_yes, cfg.tick_size)
    };
    let mut no_bid = if disable_no {
        0.0
    } else {
        safe_price(raw_no, cfg.tick_size)
    };

    if yes_bid > 0.0 && matches!(yes_re, RiskEffect::RiskIncreasing) {
        if (inv.net_diff + yes_size).abs() > cfg.max_net_diff + PAIR_ARB_NET_EPS {
            yes_bid = 0.0;
        }
    }
    if no_bid > 0.0 && matches!(no_re, RiskEffect::RiskIncreasing) {
        if (inv.net_diff - no_size).abs() > cfg.max_net_diff + PAIR_ARB_NET_EPS {
            no_bid = 0.0;
        }
    }

    if tick.remaining_sec <= cfg.risk_open_cutoff_secs {
        if matches!(yes_re, RiskEffect::RiskIncreasing) {
            yes_bid = 0.0;
        }
        if matches!(no_re, RiskEffect::RiskIncreasing) {
            no_bid = 0.0;
        }
    }

    Quotes {
        yes_bid,
        yes_size,
        no_bid,
        no_size,
    }
}

fn check_fill(our_bid: f64, market_ask: f64, model: FillModel) -> bool {
    if our_bid <= 0.0 || market_ask <= 0.0 {
        return false;
    }
    match model {
        FillModel::Conservative => market_ask <= our_bid,
        // In aggressive mode we assume top-of-book participation gives slightly easier fills.
        FillModel::Aggressive => market_ask <= our_bid + 0.5 * 0.01,
    }
}

fn compute_settlement(inv: Inventory, outcome: &str) -> WindowResult {
    let paired = inv.yes_qty.min(inv.no_qty);
    let residual_yes = inv.yes_qty - paired;
    let residual_no = inv.no_qty - paired;
    let pair_cost = if paired > 0.0 {
        inv.yes_avg_cost + inv.no_avg_cost
    } else {
        0.0
    };
    let paired_pnl = if paired > 0.0 {
        paired * (1.0 - pair_cost).max(0.0)
    } else {
        0.0
    };

    let residual_pnl = match outcome {
        "UP" => residual_yes * (1.0 - inv.yes_avg_cost) - residual_no * inv.no_avg_cost,
        "DOWN" => residual_no * (1.0 - inv.no_avg_cost) - residual_yes * inv.yes_avg_cost,
        _ => 0.0,
    };

    WindowResult {
        paired_qty: paired,
        pair_cost,
        paired_pnl,
        residual_yes,
        residual_no,
        residual_pnl,
        total_pnl: paired_pnl + residual_pnl,
    }
}

fn run_backtest(conn: &Connection, cfg: Config, limit: usize) -> anyhow::Result<Aggregate> {
    let mut settlements_stmt = if limit > 0 {
        conn.prepare(
            "SELECT condition_id, outcome FROM settlement_records
             WHERE outcome IN ('UP','DOWN') ORDER BY ts_end LIMIT ?1",
        )?
    } else {
        conn.prepare(
            "SELECT condition_id, outcome FROM settlement_records
             WHERE outcome IN ('UP','DOWN') ORDER BY ts_end",
        )?
    };

    let settlements: Vec<Settlement> = if limit > 0 {
        settlements_stmt
            .query_map(params![limit as i64], |row| {
                Ok(Settlement {
                    condition_id: row.get(0)?,
                    outcome: row.get(1)?,
                })
            })?
            .filter_map(Result::ok)
            .collect()
    } else {
        settlements_stmt
            .query_map([], |row| {
                Ok(Settlement {
                    condition_id: row.get(0)?,
                    outcome: row.get(1)?,
                })
            })?
            .filter_map(Result::ok)
            .collect()
    };

    let mut agg = Aggregate::default();

    let mut ticks_stmt = conn.prepare(
        "SELECT ts, remaining_sec, ask_up, bid_up, ask_down, bid_down
         FROM market_ticks WHERE condition_id = ?1 ORDER BY ts ASC",
    )?;

    for settlement in settlements {
        let ticks: Vec<Tick> = ticks_stmt
            .query_map(params![settlement.condition_id], |row| {
                Ok(Tick {
                    ts: row.get::<_, i64>(0)?,
                    remaining_sec: row.get::<_, f64>(1)?,
                    ask_up: row.get::<_, Option<f64>>(2)?.unwrap_or(0.0),
                    bid_up: row.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
                    ask_down: row.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                    bid_down: row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                })
            })?
            .filter_map(Result::ok)
            .collect();

        if ticks.len() < 10 {
            continue;
        }

        let mut inv = Inventory::default();
        let total_window_sec = 300.0;

        let mut active_yes_bid = 0.0;
        let mut active_yes_size = 0.0;
        let mut active_no_bid = 0.0;
        let mut active_no_size = 0.0;
        let mut last_quote_ts: i64 = 0;
        let mut fills = 0u64;

        for tick in &ticks {
            if tick.ask_up <= 0.0
                || tick.bid_up <= 0.0
                || tick.ask_down <= 0.0
                || tick.bid_down <= 0.0
            {
                continue;
            }

            if active_yes_bid > 0.0 && check_fill(active_yes_bid, tick.ask_up, cfg.fill_model) {
                inv.update_after_fill(true, active_yes_size, active_yes_bid);
                active_yes_bid = 0.0;
                active_yes_size = 0.0;
                last_quote_ts = 0;
                fills = fills.saturating_add(1);
            }

            if active_no_bid > 0.0 && check_fill(active_no_bid, tick.ask_down, cfg.fill_model) {
                inv.update_after_fill(false, active_no_size, active_no_bid);
                active_no_bid = 0.0;
                active_no_size = 0.0;
                last_quote_ts = 0;
                fills = fills.saturating_add(1);
            }

            if tick.ts - last_quote_ts >= 2 {
                let q = compute_quotes(cfg, inv, *tick, total_window_sec);
                active_yes_bid = q.yes_bid;
                active_yes_size = q.yes_size;
                active_no_bid = q.no_bid;
                active_no_size = q.no_size;
                last_quote_ts = tick.ts;
            }
        }

        let wr = compute_settlement(inv, &settlement.outcome);

        agg.windows_tested = agg.windows_tested.saturating_add(1);
        agg.total_fills = agg.total_fills.saturating_add(fills);
        agg.total_paired_qty += wr.paired_qty;
        agg.total_paired_pnl += wr.paired_pnl;
        agg.total_residual_pnl += wr.residual_pnl;
        agg.total_pnl += wr.total_pnl;

        let has_residual = wr.residual_yes > 0.01 || wr.residual_no > 0.01;
        if has_residual {
            agg.windows_with_residual = agg.windows_with_residual.saturating_add(1);
            if wr.residual_pnl > 0.0 {
                agg.residual_wins = agg.residual_wins.saturating_add(1);
            } else {
                agg.residual_losses = agg.residual_losses.saturating_add(1);
            }
        } else {
            agg.perfect_pair_windows = agg.perfect_pair_windows.saturating_add(1);
        }

        if wr.total_pnl > 0.0 {
            agg.wins = agg.wins.saturating_add(1);
            agg.sum_wins += wr.total_pnl;
        } else if wr.total_pnl < 0.0 {
            agg.losses = agg.losses.saturating_add(1);
            agg.sum_losses += wr.total_pnl;
        } else {
            agg.zeros = agg.zeros.saturating_add(1);
        }

        let _ = wr.pair_cost;
    }

    Ok(agg)
}

fn parse_vec<T: FromStr<Err = String>>(
    value: Option<String>,
    default: T,
) -> Result<Vec<T>, String> {
    let Some(v) = value else {
        return Ok(vec![default]);
    };
    let mut out = Vec::new();
    for p in v.split(',') {
        let parsed = T::from_str(p.trim())?;
        out.push(parsed);
    }
    if out.is_empty() {
        return Err("empty list provided".to_string());
    }
    Ok(out)
}

fn parse_vec_f64(value: Option<String>, default: f64) -> Result<Vec<f64>, String> {
    let Some(v) = value else {
        return Ok(vec![default]);
    };
    let mut out = Vec::new();
    for p in v.split(',') {
        let parsed = p
            .trim()
            .parse::<f64>()
            .map_err(|e| format!("failed to parse float '{p}': {e}"))?;
        out.push(parsed);
    }
    if out.is_empty() {
        return Err("empty float list provided".to_string());
    }
    Ok(out)
}

fn get_arg(flag: &str) -> Option<String> {
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        if a == flag {
            return args.next();
        }
    }
    None
}

fn has_flag(flag: &str) -> bool {
    env::args().any(|a| a == flag)
}

fn main() -> anyhow::Result<()> {
    if has_flag("--help") || has_flag("-h") {
        println!(
            "Usage: cargo run --bin pair_arb_backtest -- [options]\n\
             --db <path>\n\
             --limit <n>\n\
             --max-net-diff <v[,v2,...]>\n\
             --pair-target <v[,v2,...]>\n\
             --bid-size <v[,v2,...]>\n\
             --tier1 <v[,v2,...]>\n\
             --tier2 <v[,v2,...]>\n\
             --tier-mode <disabled|discrete|continuous[,..]>\n\
             --fill-model <conservative|aggressive[,..]>\n\
             --cutoff <secs[,..]>\n\
             --margin <v[,..]>\n"
        );
        return Ok(());
    }

    let db = get_arg("--db")
        .unwrap_or_else(|| "/Users/hot/web3Scientist/poly_trans_research/btc5m.db".to_string());
    let limit = get_arg("--limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);

    let max_net_diffs =
        parse_vec_f64(get_arg("--max-net-diff"), 5.0).map_err(anyhow::Error::msg)?;
    let pair_targets = parse_vec_f64(get_arg("--pair-target"), 0.97).map_err(anyhow::Error::msg)?;
    let bid_sizes = parse_vec_f64(get_arg("--bid-size"), 5.0).map_err(anyhow::Error::msg)?;
    let tier1s = parse_vec_f64(get_arg("--tier1"), 0.50).map_err(anyhow::Error::msg)?;
    let tier2s = parse_vec_f64(get_arg("--tier2"), 0.15).map_err(anyhow::Error::msg)?;
    let cutoffs = parse_vec_f64(get_arg("--cutoff"), 240.0).map_err(anyhow::Error::msg)?;
    let margins = parse_vec_f64(get_arg("--margin"), 0.02).map_err(anyhow::Error::msg)?;
    let fill_models = parse_vec::<FillModel>(get_arg("--fill-model"), FillModel::Conservative)
        .map_err(anyhow::Error::msg)?;
    let tier_modes = parse_vec::<TierMode>(get_arg("--tier-mode"), TierMode::Discrete)
        .map_err(anyhow::Error::msg)?;

    let conn = Connection::open(&db)?;

    println!(
        "pair_arb_backtest | db={} limit={} grid={}x{}x{}x{}x{}x{}x{}x{}",
        db,
        limit,
        max_net_diffs.len(),
        pair_targets.len(),
        bid_sizes.len(),
        tier1s.len(),
        tier2s.len(),
        cutoffs.len(),
        margins.len(),
        tier_modes.len() * fill_models.len(),
    );

    let mut run_id: u64 = 0;
    for &max_net_diff in &max_net_diffs {
        for &pair_target in &pair_targets {
            for &bid_size in &bid_sizes {
                for &tier1 in &tier1s {
                    for &tier2 in &tier2s {
                        for &cutoff in &cutoffs {
                            for &margin in &margins {
                                for &tier_mode in &tier_modes {
                                    for &fill_model in &fill_models {
                                        run_id = run_id.saturating_add(1);
                                        let cfg = Config {
                                            max_net_diff,
                                            pair_target,
                                            bid_size,
                                            tier_1_mult: tier1,
                                            tier_2_mult: tier2,
                                            risk_open_cutoff_secs: cutoff,
                                            pair_cost_safety_margin: margin,
                                            tier_mode,
                                            fill_model,
                                            ..Config::default()
                                        };

                                        let agg = run_backtest(&conn, cfg, limit)?;
                                        let windows = agg.windows_tested.max(1) as f64;
                                        let avg = agg.total_pnl / windows;
                                        let residual_loss_rate = if agg.windows_with_residual > 0 {
                                            agg.residual_losses as f64
                                                / agg.windows_with_residual as f64
                                        } else {
                                            0.0
                                        };
                                        let avg_win = if agg.wins > 0 {
                                            agg.sum_wins / agg.wins as f64
                                        } else {
                                            0.0
                                        };
                                        let avg_loss = if agg.losses > 0 {
                                            agg.sum_losses / agg.losses as f64
                                        } else {
                                            0.0
                                        };

                                        println!(
                                            "RUN#{run_id:03} net={:.1} target={:.3} bid={:.2} tier={:.2}/{:.2} mode={} cutoff={:.0}s margin={:.3} fill={} | windows={} fills={} pnl={:+.2} avg={:+.4} residual_loss_rate={:.1}% win/loss/zero={}/{}/{} avg_win={:+.3} avg_loss={:+.3}",
                                            cfg.max_net_diff,
                                            cfg.pair_target,
                                            cfg.bid_size,
                                            cfg.tier_1_mult,
                                            cfg.tier_2_mult,
                                            cfg.tier_mode,
                                            cfg.risk_open_cutoff_secs,
                                            cfg.pair_cost_safety_margin,
                                            cfg.fill_model,
                                            agg.windows_tested,
                                            agg.total_fills,
                                            agg.total_pnl,
                                            avg,
                                            residual_loss_rate * 100.0,
                                            agg.wins,
                                            agg.losses,
                                            agg.zeros,
                                            avg_win,
                                            avg_loss,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
