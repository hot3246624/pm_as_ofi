use std::time::{Duration, Instant};

use tracing::debug;

use super::*;

const FLOAT_INV_EPS: f64 = PAIR_ARB_NET_EPS;

impl StrategyCoordinator {
    pub(super) fn observe_completion_first_inventory_transition(
        &mut self,
        snapshot: &InventorySnapshot,
        now: Instant,
    ) {
        let settled = snapshot.settled;
        let working = snapshot.working;
        let prev_settled = self.completion_first_last_settled_inv_snapshot;
        let prev_working = self.completion_first_last_working_inv_snapshot;

        let settled_changed = (prev_settled.yes_qty - settled.yes_qty).abs() > FLOAT_INV_EPS
            || (prev_settled.no_qty - settled.no_qty).abs() > FLOAT_INV_EPS
            || (prev_settled.yes_avg_cost - settled.yes_avg_cost).abs() > FLOAT_INV_EPS
            || (prev_settled.no_avg_cost - settled.no_avg_cost).abs() > FLOAT_INV_EPS;
        let working_changed = (prev_working.yes_qty - working.yes_qty).abs() > FLOAT_INV_EPS
            || (prev_working.no_qty - working.no_qty).abs() > FLOAT_INV_EPS
            || (prev_working.yes_avg_cost - working.yes_avg_cost).abs() > FLOAT_INV_EPS
            || (prev_working.no_avg_cost - working.no_avg_cost).abs() > FLOAT_INV_EPS
            || (prev_working.net_diff - working.net_diff).abs() > FLOAT_INV_EPS;

        if self.cfg.strategy != StrategyKind::CompletionFirst {
            self.completion_first_last_settled_inv_snapshot = settled;
            self.completion_first_last_working_inv_snapshot = working;
            return;
        }

        if !settled_changed && !working_changed {
            return;
        }

        let prev_abs = prev_working.net_diff.abs();
        let curr_abs = working.net_diff.abs();
        if prev_abs <= FLOAT_INV_EPS && curr_abs > FLOAT_INV_EPS {
            self.completion_first_state.active_since = Some(now);
            self.completion_first_state.last_seed_at = Some(now);
        } else if prev_abs > FLOAT_INV_EPS && curr_abs <= FLOAT_INV_EPS {
            self.completion_first_state.active_since = None;
            self.completion_first_state.last_flattened_at = Some(now);
        } else if prev_working.net_diff * working.net_diff < -FLOAT_INV_EPS {
            self.completion_first_state.active_since = Some(now);
            self.completion_first_state.last_seed_at = Some(now);
        }

        if settled_changed {
            let merged_yes = (prev_settled.yes_qty - settled.yes_qty).max(0.0);
            let merged_no = (prev_settled.no_qty - settled.no_qty).max(0.0);
            if merged_yes.min(merged_no) > FLOAT_INV_EPS {
                self.completion_first_state.last_merge_at = Some(now);
            }
        }

        self.completion_first_last_settled_inv_snapshot = settled;
        self.completion_first_last_working_inv_snapshot = working;

        debug!(
            "🧭 completion_first inventory transition | prev_net={:.2} curr_net={:.2} active_since={:?} last_flattened_at={:?} last_merge_at={:?}",
            prev_working.net_diff,
            working.net_diff,
            self.completion_first_state.active_since,
            self.completion_first_state.last_flattened_at,
            self.completion_first_state.last_merge_at,
        );
    }

    pub(super) fn record_completion_first_trade_tick(&mut self, side: Side, ts: Instant) {
        if self.cfg.strategy != StrategyKind::CompletionFirst {
            return;
        }

        let state = &mut self.completion_first_trade_state;
        if let Some(prev_side) = state.last_trade_side {
            if prev_side != side {
                state.alternation_streak = state.alternation_streak.saturating_add(1).min(8);
            } else {
                state.alternation_streak = 0;
            }
        }
        state.last_trade_side = Some(side);
        state.last_trade_ts = Some(ts);
        state.recent_seen = state.recent_seen.saturating_add(1).min(8);
        match side {
            Side::Yes => state.last_yes_trade_ts = Some(ts),
            Side::No => state.last_no_trade_ts = Some(ts),
        }
    }

    pub(crate) fn completion_first_active_age(&self, now: Instant) -> Option<Duration> {
        self.completion_first_state
            .active_since
            .map(|ts| now.saturating_duration_since(ts))
    }

    pub(crate) fn completion_first_reentry_cooldown_active(&self, now: Instant) -> bool {
        let cooldown = Duration::from_secs(self.cfg.completion_first.reentry_cooldown_secs);
        if cooldown.is_zero() {
            return false;
        }

        self.completion_first_state
            .last_merge_at
            .is_some_and(|ts| now.saturating_duration_since(ts) < cooldown)
            || self
                .completion_first_state
                .last_flattened_at
                .is_some_and(|ts| now.saturating_duration_since(ts) < cooldown)
    }

    pub(crate) fn completion_first_score(
        &self,
        book: &Book,
        ofi: Option<&OfiSnapshot>,
        now: Instant,
    ) -> f64 {
        let cfg = &self.cfg.completion_first;
        let tick = self.cfg.tick_size.max(1e-9);
        let trade_window = Duration::from_secs(cfg.trade_recency_secs.max(1));

        let yes_recent = self
            .completion_first_trade_state
            .last_yes_trade_ts
            .is_some_and(|ts| now.saturating_duration_since(ts) <= trade_window);
        let no_recent = self
            .completion_first_trade_state
            .last_no_trade_ts
            .is_some_and(|ts| now.saturating_duration_since(ts) <= trade_window);
        let two_sided_recent = yes_recent && no_recent;
        let alternating_recent = self
            .completion_first_trade_state
            .last_trade_ts
            .is_some_and(|ts| now.saturating_duration_since(ts) <= trade_window)
            && self.completion_first_trade_state.alternation_streak >= 1;

        let both_live =
            book.yes_bid > 0.0 && book.yes_ask > 0.0 && book.no_bid > 0.0 && book.no_ask > 0.0;
        let yes_spread_ticks = if book.yes_bid > 0.0 && book.yes_ask > 0.0 {
            (book.yes_ask - book.yes_bid) / tick
        } else {
            f64::INFINITY
        };
        let no_spread_ticks = if book.no_bid > 0.0 && book.no_ask > 0.0 {
            (book.no_ask - book.no_bid) / tick
        } else {
            f64::INFINITY
        };
        let spread_ok = yes_spread_ticks <= cfg.max_seed_spread_ticks + 1e-9
            && no_spread_ticks <= cfg.max_seed_spread_ticks + 1e-9;

        let mid_yes = if book.yes_bid > 0.0 && book.yes_ask > 0.0 {
            (book.yes_bid + book.yes_ask) / 2.0
        } else {
            0.0
        };
        let mid_no = if book.no_bid > 0.0 && book.no_ask > 0.0 {
            (book.no_bid + book.no_ask) / 2.0
        } else {
            0.0
        };
        let book_centered = both_live && ((mid_yes + mid_no) - 1.0).abs() <= 0.08;

        let healthy_flow = ofi.is_none_or(|ofi| {
            !ofi.yes.is_toxic && !ofi.no.is_toxic && !ofi.yes.saturated && !ofi.no.saturated
        });
        let balanced_heat = ofi.is_some_and(|ofi| {
            let max_heat = ofi.yes.heat_score.max(ofi.no.heat_score);
            max_heat >= 0.4 && (ofi.yes.heat_score - ofi.no.heat_score).abs() <= 1.5
        });
        let toxic_penalty = ofi.is_some_and(|ofi| {
            ofi.yes.is_toxic || ofi.no.is_toxic || ofi.yes.saturated || ofi.no.saturated
        });

        let any_side_stale = now.saturating_duration_since(self.last_valid_ts_yes)
            > Duration::from_millis(self.cfg.stale_ttl_ms)
            || now.saturating_duration_since(self.last_valid_ts_no)
                > Duration::from_millis(self.cfg.stale_ttl_ms);

        let market_age_secs = self.market_start.elapsed().as_secs_f64();
        let early_window = (5.0..=75.0).contains(&market_age_secs);
        let tail_window = self
            .seconds_to_market_end()
            .is_some_and(|remaining| remaining <= 45);

        let mut score: f64 = 0.0;
        if two_sided_recent {
            score += 0.30;
        }
        if alternating_recent {
            score += 0.20;
        }
        if healthy_flow {
            score += 0.18;
        }
        if spread_ok {
            score += 0.14;
        }
        if book_centered {
            score += 0.10;
        }
        if balanced_heat {
            score += 0.08;
        }
        if early_window || tail_window {
            score += 0.08;
        }
        if !both_live {
            score -= 0.15;
        }
        if toxic_penalty {
            score -= 0.18;
        }
        if any_side_stale {
            score -= 0.20;
        }

        score.clamp(0.0, 1.0)
    }
}
