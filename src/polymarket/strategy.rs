use tracing::warn;

use super::coordinator::{Book, StrategyCoordinator, StrategyInventoryMetrics};
use super::glft::GlftSignalSnapshot;
use super::messages::{
    BidReason, InventorySnapshot, InventoryState, OfiSnapshot, OrderSlot, TradeDirection,
};
use super::pair_ledger::{EpisodeMetrics, PairLedgerSnapshot};
use super::types::Side;

pub mod dip_buy;
pub mod gabagool_corridor;
pub mod gabagool_grid;
pub mod glft_mm;
pub mod pair_arb;
pub mod pair_gated_tranche;
pub mod phase_builder;
pub mod post_close_hype;

pub(crate) trait QuoteStrategy: Send + Sync {
    fn kind(&self) -> StrategyKind;
    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyKind {
    GabagoolGrid,
    GabagoolCorridor,
    GlftMm,
    PairArb,
    PairGatedTrancheArb,
    DipBuy,
    PhaseBuilder,
    OracleLagSniping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StrategyExecutionMode {
    UnifiedBuys,
    DirectionalHedgeOverlay,
    SlotMarketMaking,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StrategyIntent {
    pub(crate) side: Side,
    pub(crate) direction: TradeDirection,
    pub(crate) price: f64,
    pub(crate) size: f64,
    pub(crate) reason: BidReason,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StrategyQuoteDiagnostics {
    pub(crate) pair_arb_ofi_softened_quotes: u8,
    pub(crate) pair_arb_ofi_suppressed_quotes: u8,
    pub(crate) pair_arb_keep_candidates: u8,
    pub(crate) pair_arb_skip_inventory_gate: u8,
    pub(crate) pair_arb_skip_simulate_buy_none: u8,
    pub(crate) pgt_seed_quotes: u8,
    pub(crate) pgt_completion_quotes: u8,
    pub(crate) pgt_skip_harvest: u8,
    pub(crate) pgt_skip_tail_completion_only: u8,
    pub(crate) pgt_skip_residual_guard: u8,
    pub(crate) pgt_skip_capital_guard: u8,
    pub(crate) pgt_skip_invalid_book: u8,
    pub(crate) pgt_skip_no_seed: u8,
    pub(crate) pgt_skip_geometry_guard: u8,
    pub(crate) pgt_single_seed_bias: u8,
    pub(crate) pgt_entry_pressure_sides: u8,
    pub(crate) pgt_entry_pressure_extra_ticks: u8,
    pub(crate) pgt_taker_shadow_would_open: u8,
    pub(crate) pgt_taker_shadow_would_close: u8,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StrategyQuotes {
    pub(crate) yes_buy: Option<StrategyIntent>,
    pub(crate) yes_sell: Option<StrategyIntent>,
    pub(crate) no_buy: Option<StrategyIntent>,
    pub(crate) no_sell: Option<StrategyIntent>,
    pub(crate) pgt_taker_close_limit: [Option<f64>; 2],
    pub(crate) diagnostics: StrategyQuoteDiagnostics,
}

impl StrategyQuotes {
    pub(crate) fn set(&mut self, intent: StrategyIntent) {
        match OrderSlot::new(intent.side, intent.direction) {
            OrderSlot::YES_BUY => self.yes_buy = Some(intent),
            OrderSlot::YES_SELL => self.yes_sell = Some(intent),
            OrderSlot::NO_BUY => self.no_buy = Some(intent),
            OrderSlot::NO_SELL => self.no_sell = Some(intent),
        }
    }

    pub(crate) fn get(&self, slot: OrderSlot) -> Option<StrategyIntent> {
        match slot {
            OrderSlot::YES_BUY => self.yes_buy,
            OrderSlot::YES_SELL => self.yes_sell,
            OrderSlot::NO_BUY => self.no_buy,
            OrderSlot::NO_SELL => self.no_sell,
        }
    }

    pub(crate) fn clear(&mut self, slot: OrderSlot) {
        match slot {
            OrderSlot::YES_BUY => self.yes_buy = None,
            OrderSlot::YES_SELL => self.yes_sell = None,
            OrderSlot::NO_BUY => self.no_buy = None,
            OrderSlot::NO_SELL => self.no_sell = None,
        }
    }

    pub(crate) fn buy_for(&self, side: Side) -> Option<StrategyIntent> {
        self.get(OrderSlot::new(side, TradeDirection::Buy))
    }

    pub(crate) fn set_pgt_taker_close_limit(&mut self, side: Side, limit_price: f64) {
        self.pgt_taker_close_limit[side.index()] = Some(limit_price);
    }

    pub(crate) fn pgt_taker_close_limit_for(&self, side: Side) -> Option<f64> {
        self.pgt_taker_close_limit[side.index()]
    }

    pub(crate) fn note_pair_arb_ofi_softened(&mut self) {
        self.diagnostics.pair_arb_ofi_softened_quotes = self
            .diagnostics
            .pair_arb_ofi_softened_quotes
            .saturating_add(1);
    }

    pub(crate) fn note_pair_arb_ofi_suppressed(&mut self) {
        self.diagnostics.pair_arb_ofi_suppressed_quotes = self
            .diagnostics
            .pair_arb_ofi_suppressed_quotes
            .saturating_add(1);
    }

    pub(crate) fn note_pair_arb_keep_candidate(&mut self) {
        self.diagnostics.pair_arb_keep_candidates =
            self.diagnostics.pair_arb_keep_candidates.saturating_add(1);
    }

    pub(crate) fn note_pair_arb_skip_inventory_gate(&mut self) {
        self.diagnostics.pair_arb_skip_inventory_gate = self
            .diagnostics
            .pair_arb_skip_inventory_gate
            .saturating_add(1);
    }

    pub(crate) fn note_pair_arb_skip_simulate_buy_none(&mut self) {
        self.diagnostics.pair_arb_skip_simulate_buy_none = self
            .diagnostics
            .pair_arb_skip_simulate_buy_none
            .saturating_add(1);
    }

    pub(crate) fn note_pgt_seed_quote(&mut self) {
        self.diagnostics.pgt_seed_quotes = self.diagnostics.pgt_seed_quotes.saturating_add(1);
    }

    pub(crate) fn note_pgt_completion_quote(&mut self) {
        self.diagnostics.pgt_completion_quotes =
            self.diagnostics.pgt_completion_quotes.saturating_add(1);
    }

    pub(crate) fn note_pgt_skip_harvest(&mut self) {
        self.diagnostics.pgt_skip_harvest = self.diagnostics.pgt_skip_harvest.saturating_add(1);
    }

    pub(crate) fn note_pgt_skip_tail_completion_only(&mut self) {
        self.diagnostics.pgt_skip_tail_completion_only = self
            .diagnostics
            .pgt_skip_tail_completion_only
            .saturating_add(1);
    }

    pub(crate) fn note_pgt_skip_residual_guard(&mut self) {
        self.diagnostics.pgt_skip_residual_guard =
            self.diagnostics.pgt_skip_residual_guard.saturating_add(1);
    }

    pub(crate) fn note_pgt_skip_capital_guard(&mut self) {
        self.diagnostics.pgt_skip_capital_guard =
            self.diagnostics.pgt_skip_capital_guard.saturating_add(1);
    }

    pub(crate) fn note_pgt_skip_invalid_book(&mut self) {
        self.diagnostics.pgt_skip_invalid_book =
            self.diagnostics.pgt_skip_invalid_book.saturating_add(1);
    }

    pub(crate) fn note_pgt_skip_no_seed(&mut self) {
        self.diagnostics.pgt_skip_no_seed = self.diagnostics.pgt_skip_no_seed.saturating_add(1);
    }

    pub(crate) fn note_pgt_skip_geometry_guard(&mut self) {
        self.diagnostics.pgt_skip_geometry_guard =
            self.diagnostics.pgt_skip_geometry_guard.saturating_add(1);
    }

    pub(crate) fn note_pgt_single_seed_bias(&mut self) {
        self.diagnostics.pgt_single_seed_bias =
            self.diagnostics.pgt_single_seed_bias.saturating_add(1);
    }

    pub(crate) fn note_pgt_entry_pressure(&mut self, extra_ticks: u8) {
        self.diagnostics.pgt_entry_pressure_sides =
            self.diagnostics.pgt_entry_pressure_sides.saturating_add(1);
        self.diagnostics.pgt_entry_pressure_extra_ticks = self
            .diagnostics
            .pgt_entry_pressure_extra_ticks
            .saturating_add(extra_ticks);
    }

    pub(crate) fn note_pgt_taker_shadow_would_close(&mut self) {
        self.diagnostics.pgt_taker_shadow_would_close = self
            .diagnostics
            .pgt_taker_shadow_would_close
            .saturating_add(1);
    }

    pub(crate) fn note_pgt_taker_shadow_would_open(&mut self) {
        self.diagnostics.pgt_taker_shadow_would_open = self
            .diagnostics
            .pgt_taker_shadow_would_open
            .saturating_add(1);
    }
}

impl StrategyKind {
    fn from_str_name(raw: &str) -> Option<Self> {
        match raw {
            "gabagool_grid" | "gabagool-grid" | "gabagoolgrid" | "gabagool" => {
                Some(Self::GabagoolGrid)
            }
            "gabagool_corridor" | "gabagool-corridor" | "gabagoolcorridor" => {
                Some(Self::GabagoolCorridor)
            }
            "glft_mm" | "glft-mm" | "glftmm" => Some(Self::GlftMm),
            "pair_arb" | "pairarb" | "pair-arb" => Some(Self::PairArb),
            "pair_gated_tranche_arb"
            | "pair-gated-tranche-arb"
            | "pairgatedtranchearb"
            | "pgt_arb"
            | "pgt-arb" => Some(Self::PairGatedTrancheArb),
            "dip_buy" | "dipbuy" | "dip-buy" => Some(Self::DipBuy),
            "phase_builder" | "phasebuilder" | "phase-builder" => Some(Self::PhaseBuilder),
            "oracle_lag_sniping" | "oraclelagsniping" | "oracle-lag-sniping"
            | "post_close_hype" | "postclose_hype" | "post-close-hype" | "hype_post_close" => {
                Some(Self::OracleLagSniping)
            }
            _ => None,
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Self::from_str_name(raw.trim().to_ascii_lowercase().as_str())
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::GabagoolGrid => "gabagool_grid",
            Self::GabagoolCorridor => "gabagool_corridor",
            Self::GlftMm => "glft_mm",
            Self::PairArb => "pair_arb",
            Self::PairGatedTrancheArb => "pair_gated_tranche_arb",
            Self::DipBuy => "dip_buy",
            Self::PhaseBuilder => "phase_builder",
            Self::OracleLagSniping => "oracle_lag_sniping",
        }
    }

    #[inline]
    pub fn is_glft_mm(self) -> bool {
        matches!(self, Self::GlftMm)
    }

    #[inline]
    pub fn is_pair_arb(self) -> bool {
        matches!(self, Self::PairArb)
    }

    #[inline]
    pub fn is_pair_gated_tranche_arb(self) -> bool {
        matches!(self, Self::PairGatedTrancheArb)
    }

    #[inline]
    pub fn is_oracle_lag_sniping(self) -> bool {
        matches!(self, Self::OracleLagSniping)
    }

    #[inline]
    pub fn bypasses_stale_market_gate(self) -> bool {
        self.is_oracle_lag_sniping()
    }

    pub(crate) fn execution_mode(self) -> StrategyExecutionMode {
        match self {
            Self::GabagoolGrid
            | Self::GabagoolCorridor
            | Self::PairArb
            | Self::PairGatedTrancheArb => StrategyExecutionMode::UnifiedBuys,
            Self::OracleLagSniping => StrategyExecutionMode::UnifiedBuys,
            Self::GlftMm => StrategyExecutionMode::SlotMarketMaking,
            Self::DipBuy | Self::PhaseBuilder => StrategyExecutionMode::DirectionalHedgeOverlay,
        }
    }

    pub(crate) fn engine(self) -> &'static dyn QuoteStrategy {
        StrategyRegistry::get(self)
    }

    pub fn from_env_or_default(default: Self) -> Self {
        let Ok(raw) = std::env::var("PM_STRATEGY") else {
            return default;
        };
        let normalized = raw.trim().to_ascii_lowercase();
        match Self::parse(&raw) {
            Some(kind) => {
                if normalized == "gabagool" {
                    warn!(
                        "⚠️ PM_STRATEGY=gabagool is a deprecated alias. The parked unified-utility model now lives under gabagool_grid."
                    );
                }
                kind
            }
            None => {
                warn!(
                    "⚠️ Invalid PM_STRATEGY='{}' (supported: {}). Falling back to {}",
                    raw,
                    StrategyRegistry::supported_names().join(", "),
                    default.as_str()
                );
                default
            }
        }
    }

    pub(crate) fn compute_quotes(
        self,
        coordinator: &StrategyCoordinator,
        input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        self.engine().compute_quotes(coordinator, input)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StrategyTickInput<'a> {
    pub(crate) inv: &'a InventoryState,
    pub(crate) settled_inv: &'a InventoryState,
    pub(crate) working_inv: &'a InventoryState,
    pub(crate) inventory: &'a InventorySnapshot,
    pub(crate) pair_ledger: &'a PairLedgerSnapshot,
    pub(crate) episode_metrics: &'a EpisodeMetrics,
    pub(crate) book: &'a Book,
    pub(crate) metrics: &'a StrategyInventoryMetrics,
    pub(crate) ofi: Option<&'a OfiSnapshot>,
    pub(crate) glft: Option<&'a GlftSignalSnapshot>,
}

pub(crate) struct StrategyRegistry;

impl StrategyRegistry {
    fn entries() -> &'static [&'static dyn QuoteStrategy] {
        static ENTRIES: [&'static dyn QuoteStrategy; 8] = [
            &gabagool_grid::GABAGOOL_GRID_STRATEGY,
            &gabagool_corridor::GABAGOOL_CORRIDOR_STRATEGY,
            &glft_mm::GLFT_MM_STRATEGY,
            &pair_arb::PAIR_ARB_STRATEGY,
            &pair_gated_tranche::PAIR_GATED_TRANCHE_STRATEGY,
            &dip_buy::DIP_BUY_STRATEGY,
            &phase_builder::PHASE_BUILDER_STRATEGY,
            &post_close_hype::POST_CLOSE_HYPE_STRATEGY,
        ];
        &ENTRIES
    }

    pub(crate) fn supported_names() -> Vec<&'static str> {
        Self::entries().iter().map(|s| s.kind().as_str()).collect()
    }

    pub(crate) fn get(kind: StrategyKind) -> &'static dyn QuoteStrategy {
        Self::entries()
            .iter()
            .copied()
            .find(|entry| entry.kind() == kind)
            .unwrap_or(&gabagool_grid::GABAGOOL_GRID_STRATEGY)
    }
}

#[cfg(test)]
mod tests {
    use super::{StrategyKind, StrategyRegistry};

    #[test]
    fn parse_strategy_aliases() {
        assert_eq!(
            StrategyKind::parse("gabagool"),
            Some(StrategyKind::GabagoolGrid)
        );
        assert_eq!(
            StrategyKind::parse("gabagool_grid"),
            Some(StrategyKind::GabagoolGrid)
        );
        assert_eq!(
            StrategyKind::parse("gabagool_corridor"),
            Some(StrategyKind::GabagoolCorridor)
        );
        assert_eq!(StrategyKind::parse("glft-mm"), Some(StrategyKind::GlftMm));
        assert_eq!(StrategyKind::parse("pair_arb"), Some(StrategyKind::PairArb));
        assert_eq!(StrategyKind::parse("PAIR-ARB"), Some(StrategyKind::PairArb));
        assert_eq!(
            StrategyKind::parse("pair_gated_tranche_arb"),
            Some(StrategyKind::PairGatedTrancheArb)
        );
        assert_eq!(StrategyKind::parse("dipbuy"), Some(StrategyKind::DipBuy));
        assert_eq!(
            StrategyKind::parse("phase-builder"),
            Some(StrategyKind::PhaseBuilder)
        );
        assert_eq!(
            StrategyKind::parse("post_close_hype"),
            Some(StrategyKind::OracleLagSniping)
        );
        assert_eq!(
            StrategyKind::parse("oracle_lag_sniping"),
            Some(StrategyKind::OracleLagSniping)
        );
        assert_eq!(StrategyKind::parse("unknown"), None);
    }

    #[test]
    fn registry_covers_all_public_kinds() {
        let names = StrategyRegistry::supported_names();
        assert!(names.contains(&"gabagool_grid"));
        assert!(names.contains(&"gabagool_corridor"));
        assert!(names.contains(&"glft_mm"));
        assert!(names.contains(&"pair_arb"));
        assert!(names.contains(&"dip_buy"));
        assert!(names.contains(&"phase_builder"));
        assert!(names.contains(&"oracle_lag_sniping"));
    }
}
