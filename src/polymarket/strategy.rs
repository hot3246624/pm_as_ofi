use tracing::warn;

use super::coordinator::{Book, StrategyCoordinator, StrategyInventoryMetrics};
use super::glft::GlftSignalSnapshot;
use super::messages::{
    BidReason, InventorySnapshot, InventoryState, OfiSnapshot, OrderSlot, TradeDirection,
};
use super::types::Side;

pub mod completion_first;
pub mod dip_buy;
pub mod gabagool_corridor;
pub mod gabagool_grid;
pub mod glft_mm;
pub mod pair_arb;
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
    CompletionFirst,
    PairArb,
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
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StrategyQuotes {
    pub(crate) yes_buy: Option<StrategyIntent>,
    pub(crate) yes_sell: Option<StrategyIntent>,
    pub(crate) no_buy: Option<StrategyIntent>,
    pub(crate) no_sell: Option<StrategyIntent>,
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
            "completion_first" | "completion-first" | "completionfirst" | "xuan_clone"
            | "xuan-clone" | "xuanclone" => Some(Self::CompletionFirst),
            "pair_arb" | "pairarb" | "pair-arb" => Some(Self::PairArb),
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
            Self::CompletionFirst => "completion_first",
            Self::PairArb => "pair_arb",
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
    pub fn is_oracle_lag_sniping(self) -> bool {
        matches!(self, Self::OracleLagSniping)
    }

    #[inline]
    pub fn bypasses_stale_market_gate(self) -> bool {
        self.is_oracle_lag_sniping()
    }

    pub(crate) fn execution_mode(self) -> StrategyExecutionMode {
        match self {
            Self::GabagoolGrid | Self::GabagoolCorridor | Self::PairArb => {
                StrategyExecutionMode::UnifiedBuys
            }
            Self::CompletionFirst => StrategyExecutionMode::UnifiedBuys,
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
            &completion_first::COMPLETION_FIRST_STRATEGY,
            &pair_arb::PAIR_ARB_STRATEGY,
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
        assert_eq!(
            StrategyKind::parse("completion_first"),
            Some(StrategyKind::CompletionFirst)
        );
        assert_eq!(
            StrategyKind::parse("xuan_clone"),
            Some(StrategyKind::CompletionFirst)
        );
        assert_eq!(StrategyKind::parse("pair_arb"), Some(StrategyKind::PairArb));
        assert_eq!(StrategyKind::parse("PAIR-ARB"), Some(StrategyKind::PairArb));
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
        assert!(names.contains(&"completion_first"));
        assert!(names.contains(&"pair_arb"));
        assert!(names.contains(&"dip_buy"));
        assert!(names.contains(&"phase_builder"));
        assert!(names.contains(&"oracle_lag_sniping"));
    }
}
