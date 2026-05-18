use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XuanB27DplusTruthVerdict {
    Pass,
    Fail,
    Unknown,
}

impl XuanB27DplusTruthVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct XuanB27DplusCorrelationIds {
    pub run_id: Option<String>,
    pub market_session_id: Option<String>,
    pub candidate_id: Option<String>,
    pub order_attempt_id: Option<String>,
    pub client_order_id: Option<String>,
    pub venue_order_id: Option<String>,
    pub fill_event_id: Option<String>,
    pub trade_id: Option<String>,
    pub lot_id: Option<String>,
    pub pair_id: Option<String>,
    pub residual_lot_id: Option<String>,
    pub salvage_action_id: Option<String>,
    pub redeem_attempt_id: Option<String>,
    pub redeem_tx_hash: Option<String>,
    pub cashflow_snapshot_id: Option<String>,
}

impl XuanB27DplusCorrelationIds {
    pub fn with_candidate(
        run_id: impl Into<String>,
        market_session_id: impl Into<String>,
        candidate_id: impl Into<String>,
    ) -> Self {
        Self {
            run_id: Some(run_id.into()),
            market_session_id: Some(market_session_id.into()),
            candidate_id: Some(candidate_id.into()),
            ..Self::default()
        }
    }

    pub fn missing_order_fill_truth_fields(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        push_missing(&mut missing, "run_id", &self.run_id);
        push_missing(&mut missing, "market_session_id", &self.market_session_id);
        push_missing(&mut missing, "candidate_id", &self.candidate_id);
        push_missing(&mut missing, "order_attempt_id", &self.order_attempt_id);
        push_missing(&mut missing, "client_order_id", &self.client_order_id);
        push_missing(&mut missing, "venue_order_id", &self.venue_order_id);
        push_missing(&mut missing, "fill_event_id", &self.fill_event_id);
        push_missing(&mut missing, "trade_id", &self.trade_id);
        push_missing(&mut missing, "lot_id", &self.lot_id);
        missing
    }

    pub fn missing_redeem_cashflow_truth_fields(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        push_missing(&mut missing, "run_id", &self.run_id);
        push_missing(&mut missing, "market_session_id", &self.market_session_id);
        push_missing(&mut missing, "pair_id", &self.pair_id);
        push_missing(&mut missing, "redeem_attempt_id", &self.redeem_attempt_id);
        push_missing(&mut missing, "redeem_tx_hash", &self.redeem_tx_hash);
        push_missing(
            &mut missing,
            "cashflow_snapshot_id",
            &self.cashflow_snapshot_id,
        );
        missing
    }

    pub fn order_fill_verdict(&self) -> XuanB27DplusTruthVerdict {
        if self.missing_order_fill_truth_fields().is_empty() {
            XuanB27DplusTruthVerdict::Pass
        } else {
            XuanB27DplusTruthVerdict::Unknown
        }
    }

    pub fn redeem_cashflow_verdict(&self) -> XuanB27DplusTruthVerdict {
        if self.missing_redeem_cashflow_truth_fields().is_empty() {
            XuanB27DplusTruthVerdict::Pass
        } else {
            XuanB27DplusTruthVerdict::Unknown
        }
    }
}

fn push_missing<'a>(out: &mut Vec<&'static str>, name: &'static str, value: &'a Option<String>) {
    if value.as_deref().map(str::trim).unwrap_or("").is_empty() {
        out.push(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_only_correlation_is_unknown_for_order_fill_truth() {
        let ids = XuanB27DplusCorrelationIds::with_candidate(
            "run-1",
            "btc-updown-5m:cond",
            "xuan_b27_dplus:btc-updown-5m:17:yes",
        );

        assert_eq!(ids.order_fill_verdict(), XuanB27DplusTruthVerdict::Unknown);
        assert_eq!(
            ids.missing_order_fill_truth_fields(),
            vec![
                "order_attempt_id",
                "client_order_id",
                "venue_order_id",
                "fill_event_id",
                "trade_id",
                "lot_id"
            ]
        );
    }

    #[test]
    fn complete_order_fill_correlation_passes() {
        let ids = XuanB27DplusCorrelationIds {
            run_id: Some("run-1".to_string()),
            market_session_id: Some("btc-updown-5m:cond".to_string()),
            candidate_id: Some("candidate-1".to_string()),
            order_attempt_id: Some("attempt-1".to_string()),
            client_order_id: Some("client-1".to_string()),
            venue_order_id: Some("venue-1".to_string()),
            fill_event_id: Some("fill-1".to_string()),
            trade_id: Some("trade-1".to_string()),
            lot_id: Some("lot-1".to_string()),
            ..Default::default()
        };

        assert_eq!(ids.order_fill_verdict(), XuanB27DplusTruthVerdict::Pass);
        assert!(ids.missing_order_fill_truth_fields().is_empty());
    }

    #[test]
    fn pair_only_correlation_is_unknown_for_redeem_cashflow_truth() {
        let ids = XuanB27DplusCorrelationIds {
            run_id: Some("run-1".to_string()),
            market_session_id: Some("btc-updown-5m:cond".to_string()),
            pair_id: Some("pair-1".to_string()),
            ..Default::default()
        };

        assert_eq!(
            ids.redeem_cashflow_verdict(),
            XuanB27DplusTruthVerdict::Unknown
        );
        assert_eq!(
            ids.missing_redeem_cashflow_truth_fields(),
            vec![
                "redeem_attempt_id",
                "redeem_tx_hash",
                "cashflow_snapshot_id"
            ]
        );
    }

    #[test]
    fn complete_redeem_cashflow_correlation_passes() {
        let ids = XuanB27DplusCorrelationIds {
            run_id: Some("run-1".to_string()),
            market_session_id: Some("btc-updown-5m:cond".to_string()),
            pair_id: Some("pair-1".to_string()),
            redeem_attempt_id: Some("redeem-1".to_string()),
            redeem_tx_hash: Some("0xredeem".to_string()),
            cashflow_snapshot_id: Some("cashflow-1".to_string()),
            ..Default::default()
        };

        assert_eq!(
            ids.redeem_cashflow_verdict(),
            XuanB27DplusTruthVerdict::Pass
        );
        assert!(ids.missing_redeem_cashflow_truth_fields().is_empty());
    }
}
