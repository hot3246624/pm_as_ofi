use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::polymarket::coordinator::StrategyCoordinator;

use super::{QuoteStrategy, StrategyKind, StrategyQuotes, StrategyTickInput};

pub(crate) struct PostCloseHypeStrategy;

pub(crate) static POST_CLOSE_HYPE_STRATEGY: PostCloseHypeStrategy = PostCloseHypeStrategy;

impl QuoteStrategy for PostCloseHypeStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::OracleLagSniping
    }

    /// Oracle-lag execution is WinnerHint-driven and handled in coordinator hot path.
    /// Strategy layer no longer emits maker intents for this mode.
    fn compute_quotes(
        &self,
        coordinator: &StrategyCoordinator,
        _input: StrategyTickInput<'_>,
    ) -> StrategyQuotes {
        let cfg = coordinator.cfg();
        if !cfg.oracle_lag_sniping.market_enabled || cfg.oracle_lag_sniping.window_secs == 0 {
            return StrategyQuotes::default();
        }
        if !is_in_post_close_window(cfg.market_end_ts, cfg.oracle_lag_sniping.window_secs) {
            return StrategyQuotes::default();
        }
        StrategyQuotes::default()
    }
}

fn is_in_post_close_window(market_end_ts: Option<u64>, window_secs: u64) -> bool {
    let Some(end_ts) = market_end_ts else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs();
    now >= end_ts && now < end_ts.saturating_add(window_secs)
}

#[cfg(test)]
mod tests {
    // Intentionally empty: oracle-lag strategy is hot-path driven by WinnerHint in coordinator.
}
