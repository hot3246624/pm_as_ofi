use super::{QuoteStrategy, StrategyKind, StrategyQuotes, StrategyTickInput};
use crate::polymarket::coordinator::StrategyCoordinator;

pub(crate) struct XuanB27DplusStrategy;
pub(crate) static XUAN_B27_DPLUS_STRATEGY: XuanB27DplusStrategy = XuanB27DplusStrategy;

impl QuoteStrategy for XuanB27DplusStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::XuanB27Dplus
    }

    fn compute_quotes(
        &self,
        _coordinator: &StrategyCoordinator,
        _input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        StrategyQuotes::default()
    }
}
