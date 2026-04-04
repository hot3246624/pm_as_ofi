use tracing::warn;

use super::coordinator::{Book, StrategyCoordinator, StrategyInventoryMetrics};
use super::glft::GlftSignalSnapshot;
use super::messages::{BidReason, InventoryState, OfiSnapshot, OrderSlot, TradeDirection};
use super::types::Side;

pub mod dip_buy;
pub mod gabagool_corridor;
pub mod gabagool_grid;
pub mod glft_mm;
pub mod pair_arb;
pub mod phase_builder;

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
    DipBuy,
    PhaseBuilder,
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
pub(crate) struct StrategyQuotes {
    pub(crate) yes_buy: Option<StrategyIntent>,
    pub(crate) yes_sell: Option<StrategyIntent>,
    pub(crate) no_buy: Option<StrategyIntent>,
    pub(crate) no_sell: Option<StrategyIntent>,
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
            "dip_buy" | "dipbuy" | "dip-buy" => Some(Self::DipBuy),
            "phase_builder" | "phasebuilder" | "phase-builder" => Some(Self::PhaseBuilder),
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
            Self::DipBuy => "dip_buy",
            Self::PhaseBuilder => "phase_builder",
        }
    }

    pub(crate) fn execution_mode(self) -> StrategyExecutionMode {
        match self {
            Self::GabagoolGrid | Self::GabagoolCorridor | Self::PairArb => {
                StrategyExecutionMode::UnifiedBuys
            }
            Self::GlftMm => StrategyExecutionMode::SlotMarketMaking,
            Self::DipBuy | Self::PhaseBuilder => {
                StrategyExecutionMode::DirectionalHedgeOverlay
            }
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
    pub(crate) book: &'a Book,
    pub(crate) metrics: &'a StrategyInventoryMetrics,
    pub(crate) ofi: Option<&'a OfiSnapshot>,
    pub(crate) glft: Option<&'a GlftSignalSnapshot>,
}

pub(crate) struct StrategyRegistry;

impl StrategyRegistry {
    fn entries() -> &'static [&'static dyn QuoteStrategy] {
        static ENTRIES: [&'static dyn QuoteStrategy; 6] = [
            &gabagool_grid::GABAGOOL_GRID_STRATEGY,
            &gabagool_corridor::GABAGOOL_CORRIDOR_STRATEGY,
            &glft_mm::GLFT_MM_STRATEGY,
            &pair_arb::PAIR_ARB_STRATEGY,
            &dip_buy::DIP_BUY_STRATEGY,
            &phase_builder::PHASE_BUILDER_STRATEGY,
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
        assert_eq!(StrategyKind::parse("dipbuy"), Some(StrategyKind::DipBuy));
        assert_eq!(
            StrategyKind::parse("phase-builder"),
            Some(StrategyKind::PhaseBuilder)
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
    }
}
