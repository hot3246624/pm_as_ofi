use std::time::{SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::{StrategyCoordinator, PAIR_ARB_NET_EPS};
use crate::polymarket::messages::{BidReason, TradeDirection};
use crate::polymarket::pair_ledger::{urgency_budget_shadow_5m, PairTranche};
use crate::polymarket::types::Side;

use super::pair_arb::PairArbStrategy;
use super::{QuoteStrategy, StrategyIntent, StrategyKind, StrategyQuotes, StrategyTickInput};

const RESIDUAL_EPS: f64 = 10.0;
const MIN_EDGE_PER_PAIR: f64 = 0.005;
const TAIL_COMPLETION_ONLY_SECS: u64 = 45;
const HARVEST_WINDOW_SECS: u64 = 25;
const HARVEST_MIN_PAIRABLE_QTY: f64 = 10.0;
const BASE_CLIP_QTY: f64 = 120.0;
const MAX_CLIP_QTY: f64 = 250.0;
const MIN_CLIP_QTY: f64 = 25.0;
const SEED_NO_IMMEDIATE_COMPLETION_CLIP_MULT: f64 = 0.70;
pub(crate) const PGT_OPEN_PAIR_BAND_WIDE_SECS: u64 = 150;
pub(crate) const PGT_OPEN_PAIR_BAND_MID_SECS: u64 = 90;
pub(crate) const PGT_OPEN_PAIR_BAND_MID_VALUE: f64 = 0.995;

struct CompletionPlan {
    intent: StrategyIntent,
    taker_shadow_would_close: bool,
}

struct SeedPlan {
    intent: StrategyIntent,
    taker_shadow_would_open: bool,
}

pub(crate) struct PairGatedTrancheStrategy;

pub(crate) static PAIR_GATED_TRANCHE_STRATEGY: PairGatedTrancheStrategy = PairGatedTrancheStrategy;

impl QuoteStrategy for PairGatedTrancheStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::PairGatedTrancheArb
    }

    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let mut quotes = StrategyQuotes::default();
        let remaining_secs = coordinator.seconds_to_market_end().unwrap_or(u64::MAX);
        let no_new_open = remaining_secs <= TAIL_COMPLETION_ONLY_SECS;
        let harvest_window_active = self.should_shadow_harvest(input, remaining_secs);

        if let Some(active) = input
            .pair_ledger
            .active_tranche
            .filter(|tranche| tranche.first_side.is_some() && tranche.residual_qty > f64::EPSILON)
        {
            if harvest_window_active {
                quotes.note_pgt_skip_harvest();
                return quotes;
            }
            if let Some(plan) = self.completion_intent(coordinator, input, active, remaining_secs)
            {
                quotes.note_pgt_completion_quote();
                if plan.taker_shadow_would_close {
                    quotes.note_pgt_taker_shadow_would_close();
                }
                quotes.set(plan.intent);
            } else {
                quotes.note_pgt_skip_invalid_book();
            }
            return quotes;
        }

        if harvest_window_active {
            quotes.note_pgt_skip_harvest();
            return quotes;
        }
        if no_new_open {
            quotes.note_pgt_skip_tail_completion_only();
            return quotes;
        }
        if input.pair_ledger.residual_qty.abs() > RESIDUAL_EPS {
            quotes.note_pgt_skip_residual_guard();
            return quotes;
        }
        if input
            .pair_ledger
            .capital_state
            .would_block_new_open_due_to_capital
        {
            quotes.note_pgt_skip_capital_guard();
            return quotes;
        }

        let Some((raw_yes, raw_no)) =
            self.flat_seed_raw_prices(coordinator, input, remaining_secs)
        else {
            quotes.note_pgt_skip_invalid_book();
            return quotes;
        };

        let size = self.adaptive_clip_qty(coordinator, input, None, remaining_secs);
        let size = quantize_tenth(size);
        if size <= 0.0 {
            quotes.note_pgt_skip_no_seed();
            return quotes;
        }

        if let Some(seed) =
            self.flat_seed_intent_for_side(coordinator, input, Side::Yes, raw_yes, size)
        {
            quotes.note_pgt_seed_quote();
            if seed.taker_shadow_would_open {
                quotes.note_pgt_taker_shadow_would_open();
            }
            quotes.set(seed.intent);
        }
        if let Some(seed) =
            self.flat_seed_intent_for_side(coordinator, input, Side::No, raw_no, size)
        {
            quotes.note_pgt_seed_quote();
            if seed.taker_shadow_would_open {
                quotes.note_pgt_taker_shadow_would_open();
            }
            quotes.set(seed.intent);
        }

        if quotes.yes_buy.is_none() && quotes.no_buy.is_none() {
            quotes.note_pgt_skip_no_seed();
        }

        quotes
    }
}

impl PairGatedTrancheStrategy {
    fn should_shadow_harvest(&self, input: StrategyTickInput<'_>, remaining_secs: u64) -> bool {
        remaining_secs <= HARVEST_WINDOW_SECS
            && input.pair_ledger.total_pairable_qty() >= HARVEST_MIN_PAIRABLE_QTY - 1e-9
    }

    fn flat_seed_raw_prices(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        remaining_secs: u64,
    ) -> Option<(f64, f64)> {
        let ub = input.book;
        if ub.yes_bid <= 0.0 || ub.yes_ask <= 0.0 || ub.no_bid <= 0.0 || ub.no_ask <= 0.0 {
            return None;
        }
        // Opening-leg seed is anchored by the broad open_pair_band ceiling.
        // But we also reserve room for the opposite leg to complete at
        // ask-1tick if that ask is already visible now; otherwise we enter
        // first-leg fills that only close after drifting into negative pair
        // cost. Clip haircut still handles the "no immediate completion" case,
        // but price itself must not consume the entire completion budget.
        let open_pair_band =
            pgt_effective_open_pair_band_value(coordinator.cfg().open_pair_band, remaining_secs);
        let tick = coordinator.cfg().tick_size.max(1e-9);
        let yes_bid_cap = pgt_open_leg_ceiling_from_opposite_bid(open_pair_band, ub.no_bid)?;
        let no_bid_cap = pgt_open_leg_ceiling_from_opposite_bid(open_pair_band, ub.yes_bid)?;
        let yes_immediate_completion_cap =
            (open_pair_band - (ub.no_ask - tick).max(0.0)).clamp(0.0, 1.0);
        let no_immediate_completion_cap =
            (open_pair_band - (ub.yes_ask - tick).max(0.0)).clamp(0.0, 1.0);
        let yes_ceiling = yes_bid_cap.min(yes_immediate_completion_cap);
        let no_ceiling = no_bid_cap.min(no_immediate_completion_cap);
        Some((yes_ceiling, no_ceiling))
    }

    fn flat_seed_intent_for_side(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        side: Side,
        raw_price: f64,
        size: f64,
    ) -> Option<SeedPlan> {
        if raw_price <= 0.0 || size <= 0.0 {
            return None;
        }

        let (best_bid, best_ask, opp_avg, same_qty, same_avg) = match side {
            Side::Yes => (
                input.book.yes_bid,
                input.book.yes_ask,
                input.inv.no_avg_cost,
                input.inv.yes_qty,
                input.inv.yes_avg_cost,
            ),
            Side::No => (
                input.book.no_bid,
                input.book.no_ask,
                input.inv.yes_avg_cost,
                input.inv.no_qty,
                input.inv.no_avg_cost,
            ),
        };
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }

        let risk_effect = PairArbStrategy::candidate_risk_effect(input.inv, side, size);
        let mut ceiling = raw_price;
        if let Some(tier_cap) = PairArbStrategy::tier_cap_price_for_candidate(
            input.inv,
            side,
            size,
            risk_effect,
            coordinator.cfg().pair_arb.tier_1_mult,
            coordinator.cfg().pair_arb.tier_2_mult,
        ) {
            ceiling = ceiling.min(tier_cap);
        }

        if opp_avg > 0.0 {
            let effective_pair_cost_margin = if input.inv.net_diff.abs() < PAIR_ARB_NET_EPS {
                0.0
            } else {
                coordinator.cfg().pair_arb.pair_cost_safety_margin
            };
            let vwap_ceiling = PairArbStrategy::vwap_ceiling(
                coordinator.cfg().pair_target,
                effective_pair_cost_margin,
                opp_avg,
                same_qty,
                same_avg,
                size,
            );
            ceiling = ceiling.min(vwap_ceiling);
        }

        let price = self.passive_seed_price(coordinator, side, ceiling, best_bid, best_ask)?;
        if price <= 0.0 {
            return None;
        }
        let taker_shadow_would_open = best_ask <= ceiling + 1e-9 && best_ask > price + 1e-9;
        let size = if taker_shadow_would_open {
            size
        } else {
            quantize_tenth(size * SEED_NO_IMMEDIATE_COMPLETION_CLIP_MULT)
        };
        if size <= 0.0 {
            return None;
        }
        coordinator.simulate_buy(input.inv, side, size, price)?;

        Some(SeedPlan {
            taker_shadow_would_open,
            intent: StrategyIntent {
                side,
                direction: TradeDirection::Buy,
                price,
                size,
                reason: BidReason::Provide,
            },
        })
    }

    fn completion_intent(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: PairTranche,
        remaining_secs: u64,
    ) -> Option<CompletionPlan> {
        let first_side = active.first_side?;
        let hedge_side = opposite_side(first_side);
        let (best_bid, best_ask) = match hedge_side {
            Side::Yes => (input.book.yes_bid, input.book.yes_ask),
            Side::No => (input.book.no_bid, input.book.no_ask),
        };
        if best_ask <= 0.0 || active.first_vwap <= 0.0 || active.residual_qty <= f64::EPSILON {
            return None;
        }

        let repair_budget_per_share =
            input.pair_ledger.repair_budget_available / active.residual_qty.max(1.0);
        let urgency_shadow = urgency_budget_shadow_5m(remaining_secs, true);
        let ceiling =
            1.0 - active.first_vwap - MIN_EDGE_PER_PAIR + repair_budget_per_share + urgency_shadow;
        if ceiling <= 0.0 {
            return None;
        }

        let Some(price) = self.passive_completion_price(
            coordinator,
            hedge_side,
            ceiling,
            best_bid,
            best_ask,
            remaining_secs,
            active
                .last_transition_at
                .map(|ts| ts.elapsed().as_secs_f64())
                .or_else(|| active.opened_at.map(|ts| ts.elapsed().as_secs_f64()))
                .unwrap_or(0.0),
        ) else {
            return None;
        };
        if price <= 0.0 || price > ceiling + 1e-9 {
            return None;
        }

        let size = self
            .adaptive_clip_qty(coordinator, input, Some(active), remaining_secs)
            .min(active.residual_qty.max(0.0));
        let size = quantize_tenth(size);
        if size <= 0.0 {
            return None;
        }

        Some(CompletionPlan {
            taker_shadow_would_close: best_ask <= ceiling + 1e-9 && best_ask > price + 1e-9,
            intent: StrategyIntent {
                side: hedge_side,
                direction: TradeDirection::Buy,
                price,
                size,
                reason: BidReason::Hedge,
            },
        })
    }

    fn passive_seed_price(
        &self,
        coordinator: &StrategyCoordinator,
        _side: Side,
        ceiling: f64,
        best_bid: f64,
        best_ask: f64,
    ) -> Option<f64> {
        if ceiling <= 0.0 || best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }
        let tick = coordinator.cfg().tick_size.max(1e-9);
        // Unlike pair_arb, PGT flat seed is low-cadence and explicitly
        // maker-first. Reusing the shared tight-spread safety margin pushes a
        // one-tick market one full tick below the actual best bid, which kills
        // fills on BTC 5m. The only hard requirement here is "remain below the
        // ask", so use ask-1tick as the maker cap.
        let maker_cap = (best_ask - tick).max(0.0);
        if maker_cap <= 0.0 {
            return None;
        }

        // Flat-state seed should behave like a passive maker: quote at the bid
        // or improve by a single tick when there is enough spread, and treat
        // pair-target / tier / VWAP logic strictly as ceilings rather than as a
        // reason to chase toward the ask.
        let passive_anchor = if best_ask > best_bid + (2.0 * tick) {
            best_bid + tick
        } else {
            best_bid
        };
        let price = coordinator.safe_price(passive_anchor.min(maker_cap).min(ceiling));
        if price > 0.0 {
            Some(price)
        } else {
            None
        }
    }

    fn adaptive_clip_qty(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
        active: Option<PairTranche>,
        remaining_secs: u64,
    ) -> f64 {
        let session_mult = session_clip_mult_utc();
        let imbalance_mult = imbalance_clip_mult(coordinator, input, active);
        let trade_index = input.episode_metrics.round_buy_fill_count.max(1) as f64;
        let trade_index_mult = (1.0 - 0.05 * (trade_index - 1.0)).max(0.70);
        let tail_mult = if remaining_secs <= 30 { 1.16 } else { 1.0 };

        (BASE_CLIP_QTY * session_mult * imbalance_mult * trade_index_mult * tail_mult)
            .clamp(MIN_CLIP_QTY, MAX_CLIP_QTY)
    }

    fn passive_completion_price(
        &self,
        coordinator: &StrategyCoordinator,
        _side: Side,
        ceiling: f64,
        best_bid: f64,
        best_ask: f64,
        remaining_secs: u64,
        completion_age_secs: f64,
    ) -> Option<f64> {
        if ceiling <= 0.0 || best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }
        let tick = coordinator.cfg().tick_size.max(1e-9);
        // PGT completion is a dedicated close-out path, not a generic reprice
        // path like pair_arb. Shadow validation against xuan only starts to make
        // sense if completion is allowed to lean to ask-1tick while remaining
        // maker-only, instead of inheriting the broader shared post-only margin.
        let maker_cap = (best_ask - tick).max(0.0);
        if maker_cap <= 0.0 {
            return None;
        }

        let spread_ticks = ((best_ask - best_bid) / tick).max(0.0);
        let max_passive_ticks = (spread_ticks - 1.0).max(0.0).floor();
        let time_ticks = if remaining_secs <= 25 {
            // Harvest edge: stay maker-only, but move all the way to ask-1tick
            // so any remaining pairable inventory has a realistic chance to close
            // before the merge pulse.
            max_passive_ticks
        } else if remaining_secs <= 45 {
            if spread_ticks >= 5.0 {
                3.0
            } else if spread_ticks >= 3.0 {
                2.0
            } else {
                1.0
            }
        } else if remaining_secs <= 60 {
            if spread_ticks >= 5.0 {
                3.0
            } else if spread_ticks >= 3.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if remaining_secs <= 90 {
            if spread_ticks >= 4.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if remaining_secs <= 120 {
            if spread_ticks >= 4.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if remaining_secs <= 180 {
            if spread_ticks >= 4.0 { 1.0 } else { 0.0 }
        } else if spread_ticks >= 5.0 {
            1.0
        } else {
            0.0
        };

        // Once a residual leg has been sitting for a while, completion should
        // progressively lean further inside the spread even outside tail mode.
        // This keeps the path maker-only, but prevents 60s+ close delays where
        // a tranche is technically closable yet we keep repricing too slowly.
        let age_ticks = if completion_age_secs >= 45.0 {
            max_passive_ticks
        } else if completion_age_secs >= 25.0 {
            if spread_ticks >= 4.0 {
                3.0
            } else if spread_ticks >= 3.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else if completion_age_secs >= 12.0 {
            if spread_ticks >= 3.0 {
                2.0
            } else if spread_ticks >= 2.0 {
                1.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        let improve_ticks = time_ticks.max(age_ticks).min(max_passive_ticks);
        let passive_anchor = best_bid + improve_ticks * tick;
        let price = coordinator.safe_price(passive_anchor.min(maker_cap).min(ceiling));
        if price > 0.0 {
            Some(price)
        } else {
            None
        }
    }

}

pub(crate) fn pgt_effective_open_pair_band_value(base: f64, remaining_secs: u64) -> f64 {
    if remaining_secs == u64::MAX {
        return base;
    }
    if remaining_secs > PGT_OPEN_PAIR_BAND_WIDE_SECS {
        1.0
    } else if remaining_secs > PGT_OPEN_PAIR_BAND_MID_SECS {
        base.max(PGT_OPEN_PAIR_BAND_MID_VALUE)
    } else {
        base
    }
}

pub(crate) fn pgt_open_leg_ceiling_from_opposite_bid(
    open_pair_band: f64,
    opposite_bid: f64,
) -> Option<f64> {
    if opposite_bid <= 0.0 {
        return None;
    }
    let ceiling = (open_pair_band - opposite_bid).clamp(0.0, 1.0);
    if ceiling > 0.0 { Some(ceiling) } else { None }
}

fn imbalance_clip_mult(
    coordinator: &StrategyCoordinator,
    input: StrategyTickInput<'_>,
    active: Option<PairTranche>,
) -> f64 {
    let abs_imb = if let Some(tranche) = active {
        let denom = tranche
            .first_qty
            .max(tranche.hedge_qty)
            .max(tranche.residual_qty)
            .max(1.0);
        (tranche.residual_qty.max(0.0) / denom).clamp(0.0, 1.0)
    } else {
        (input.inv.net_diff.abs() / coordinator.cfg().max_net_diff.max(1.0)).clamp(0.0, 1.0)
    };

    if abs_imb < 0.05 {
        1.00
    } else if abs_imb < 0.15 {
        0.95
    } else if abs_imb < 0.30 {
        1.00
    } else {
        1.20
    }
}

fn session_clip_mult_utc() -> f64 {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let hour = ((now_secs / 3600) % 24) as u8;
    if (16..=22).contains(&hour) {
        1.15
    } else if (3..=12).contains(&hour) {
        0.80
    } else {
        1.00
    }
}

fn quantize_tenth(qty: f64) -> f64 {
    let rounded = (qty * 10.0).floor() / 10.0;
    if rounded >= 0.1 {
        rounded
    } else {
        0.0
    }
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}
