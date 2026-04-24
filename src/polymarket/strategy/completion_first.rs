use std::time::Instant;

use crate::polymarket::coordinator::{Book, StrategyCoordinator};
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::types::Side;

use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct CompletionFirstStrategy;

pub(crate) static COMPLETION_FIRST_STRATEGY: CompletionFirstStrategy = CompletionFirstStrategy;

impl QuoteStrategy for CompletionFirstStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::CompletionFirst
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let now = Instant::now();
        let inv = input.inv;
        let metrics = input.metrics;

        if metrics.residual_qty > f64::EPSILON {
            return self.completion_quotes(coordinator, input, now);
        }

        if coordinator.completion_first_reentry_cooldown_active(now) {
            return StrategyQuotes::default();
        }

        let score = coordinator.completion_first_score(input.book, input.ofi, now);
        if score + 1e-9 < coordinator.cfg().completion_first.score_threshold {
            return StrategyQuotes::default();
        }

        if inv.net_diff.abs() <= f64::EPSILON {
            self.seed_quotes(coordinator, input, score)
        } else {
            StrategyQuotes::default()
        }
    }
}

impl CompletionFirstStrategy {
    fn seed_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        score: f64,
    ) -> StrategyQuotes {
        let cfg = coordinator.cfg();
        let cfcfg = &cfg.completion_first;
        let book = input.book;
        if !(book.yes_bid > 0.0 && book.yes_ask > 0.0 && book.no_bid > 0.0 && book.no_ask > 0.0) {
            return StrategyQuotes::default();
        }

        let seed_size = (cfg.bid_size * cfcfg.seed_size_mult).max(cfg.min_order_size.max(0.25));
        let dynamic_pair_target =
            Self::dynamic_pair_target(cfg.pair_target, cfcfg, score, false, false);
        let mid_yes = (book.yes_bid + book.yes_ask) / 2.0;
        let mid_no = (book.no_bid + book.no_ask) / 2.0;
        let mut raw_yes = mid_yes;
        let mut raw_no = mid_no;
        if raw_yes + raw_no > dynamic_pair_target {
            let overflow = (raw_yes + raw_no) - dynamic_pair_target;
            raw_yes -= overflow / 2.0;
            raw_no -= overflow / 2.0;
        }

        let yes_safety =
            coordinator.post_only_safety_margin_for(Side::Yes, book.yes_bid, book.yes_ask);
        let no_safety = coordinator.post_only_safety_margin_for(Side::No, book.no_bid, book.no_ask);
        let bid_yes = coordinator.safe_price(raw_yes.min(book.yes_ask - yes_safety));
        let bid_no = coordinator.safe_price(raw_no.min(book.no_ask - no_safety));

        let mut quotes = StrategyQuotes::default();
        if bid_yes > 0.0 {
            quotes.set(StrategyIntent {
                side: Side::Yes,
                direction: TradeDirection::Buy,
                price: bid_yes,
                size: seed_size,
                reason: BidReason::Provide,
            });
        }
        if bid_no > 0.0 {
            quotes.set(StrategyIntent {
                side: Side::No,
                direction: TradeDirection::Buy,
                price: bid_no,
                size: seed_size,
                reason: BidReason::Provide,
            });
        }
        quotes
    }

    fn completion_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        now: Instant,
    ) -> StrategyQuotes {
        let cfg = coordinator.cfg();
        let cfcfg = &cfg.completion_first;
        let inv = input.inv;
        let metrics = input.metrics;
        let Some(dominant_side) = metrics.dominant_side else {
            return StrategyQuotes::default();
        };

        let repair_side = Self::opposite(dominant_side);
        let repair_size = metrics
            .residual_qty
            .min((cfg.bid_size * cfcfg.repair_size_mult).max(cfg.min_order_size.max(0.25)));
        if repair_size <= f64::EPSILON {
            return StrategyQuotes::default();
        }

        let completion_timed_out = coordinator
            .completion_first_active_age(now)
            .is_some_and(|age| age.as_secs() >= cfcfg.completion_ttl_secs);
        let score = coordinator.completion_first_score(input.book, input.ofi, now);
        let dynamic_pair_target =
            Self::dynamic_pair_target(cfg.pair_target, cfcfg, score, true, completion_timed_out);
        let held_avg_cost = match dominant_side {
            Side::Yes => inv.yes_avg_cost.max(0.0),
            Side::No => inv.no_avg_cost.max(0.0),
        };
        let ceiling = dynamic_pair_target - held_avg_cost;
        if ceiling <= cfg.tick_size + 1e-9 {
            return StrategyQuotes::default();
        }

        let (best_bid, best_ask) = Self::book_for_side(input.book, repair_side);
        let safety = coordinator.post_only_safety_margin_for(repair_side, best_bid, best_ask);
        let near_touch_post_only = if best_ask > 0.0 {
            (best_ask - safety).max(best_bid)
        } else if best_bid > 0.0 {
            best_bid + cfg.tick_size
        } else {
            ceiling
        };
        let target_price = if completion_timed_out {
            near_touch_post_only.min(ceiling)
        } else if best_bid > 0.0 {
            (best_bid + cfg.tick_size).min(ceiling)
        } else {
            ceiling
        };
        let bid = coordinator.safe_price(target_price);
        if bid <= 0.0 {
            return StrategyQuotes::default();
        }

        let mut quotes = StrategyQuotes::default();
        quotes.set(StrategyIntent {
            side: repair_side,
            direction: TradeDirection::Buy,
            price: bid,
            size: repair_size,
            reason: BidReason::Provide,
        });
        quotes
    }

    pub(crate) fn dynamic_pair_target(
        base_pair_target: f64,
        cfg: &crate::polymarket::coordinator::CompletionFirstStrategyConfig,
        score: f64,
        allow_score_bonus: bool,
        timed_out: bool,
    ) -> f64 {
        let mut target = base_pair_target.max(0.0);
        if allow_score_bonus && score > cfg.score_threshold {
            let unlocked = (score - cfg.score_threshold) / (1.0 - cfg.score_threshold).max(1e-9);
            target += unlocked.clamp(0.0, 1.0) * cfg.score_pair_band_bonus;
        }
        if timed_out {
            target += cfg.timeout_pair_band_bonus;
        }
        target.clamp(0.05, 0.999)
    }

    pub(crate) fn opposite(side: Side) -> Side {
        match side {
            Side::Yes => Side::No,
            Side::No => Side::Yes,
        }
    }

    pub(crate) fn book_for_side(book: &Book, side: Side) -> (f64, f64) {
        match side {
            Side::Yes => (book.yes_bid, book.yes_ask),
            Side::No => (book.no_bid, book.no_ask),
        }
    }
}
